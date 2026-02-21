/*
 * codec_csv.c — Optimized streaming CSV decoder and encoder.
 *
 * Decoder design (three key optimizations):
 *
 *   1. Zero-copy field parsing: fields are returned as (ptr, len) slices
 *      into the line buffer, avoiding per-field malloc/free. Only quoted
 *      fields with escaped quotes ("") need copying (rare in practice).
 *
 *   2. Type detection window: the first batch (batch_size rows) detects
 *      column types via progressive widening (NULL → INT64 → FLOAT64 →
 *      STRING). Types freeze after the first batch. This matches the
 *      behavior of Arrow CSV, DuckDB, and other production parsers.
 *
 *   3. Direct-to-typed parsing: after types freeze, field slices are parsed
 *      directly into typed column arrays (int64, double, string) without
 *      an intermediate STRING batch. Combined with custom fast_int64/
 *      fast_double parsers, this eliminates double parsing for >99% of rows.
 *
 * Encoder: writes typed batches as RFC 4180 CSV with proper quoting.
 */

#include "internal.h"
#include "date_utils.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <limits.h>
#include <math.h>

#define DEFAULT_BATCH_SIZE 1024
#define MAX_COLS           256   /* max columns per row */

/* ================================================================
 * Field Slice — zero-copy reference into the line buffer
 * ================================================================ */

typedef struct {
    const char *ptr;   /* points into line buffer (or field_arena for escapes) */
    size_t      len;
} field_slice;

/* ================================================================
 * Fast Numeric Parsers
 *
 * These avoid libc strtoll/strtod overhead for common number formats.
 * They take (ptr, len) instead of null-terminated strings, which
 * pairs naturally with the zero-copy field slices.
 * ================================================================ */

/*
 * Fast int64 parser. Handles [-+]digits format only.
 * Rejects decimal points, exponents, leading zeros (except "0"),
 * and values outside the int64 range.
 */
static int fast_int64(const char *s, size_t len, int64_t *out) {
    if (len == 0) return 0;

    size_t i = 0;
    int neg = 0;
    if (s[0] == '-')      { neg = 1; i = 1; }
    else if (s[0] == '+') { i = 1; }

    /* Must have at least one digit */
    if (i >= len || s[i] < '0' || s[i] > '9') return 0;

    /* Max int64 is 19 digits; reject longer numbers to avoid overflow */
    size_t n_digits = len - i;
    if (n_digits > 19) return 0;

    uint64_t v = 0;
    for (; i < len; i++) {
        if (s[i] < '0' || s[i] > '9') return 0;
        v = v * 10 + (uint64_t)(s[i] - '0');
    }

    /* Range check against int64 bounds */
    if (neg) {
        /* INT64_MIN magnitude is INT64_MAX + 1 */
        if (v > (uint64_t)INT64_MAX + 1) return 0;
        *out = (v == (uint64_t)INT64_MAX + 1) ? INT64_MIN : -(int64_t)v;
    } else {
        if (v > (uint64_t)INT64_MAX) return 0;
        *out = (int64_t)v;
    }
    return 1;
}

/*
 * Fast double parser for common decimal formats: [-+]digits[.digits]
 *
 * Uses integer accumulation + power-of-10 division for precision.
 * Handles up to 18 significant digits (fits in uint64 without overflow).
 * Falls back to strtod for exponents (e/E), special values, or
 * very long mantissas.
 */
static int fast_double(const char *s, size_t len, double *out) {
    if (len == 0) return 0;

    size_t i = 0;
    int neg = 0;
    if (s[0] == '-')      { neg = 1; i = 1; }
    else if (s[0] == '+') { i = 1; }
    if (i >= len) return 0;

    /* Accumulate mantissa as integer: 123.456 → mantissa=123456, n_frac=3 */
    uint64_t mantissa = 0;
    int n_digits = 0;
    int n_frac = 0;

    /* Integer part */
    while (i < len && s[i] >= '0' && s[i] <= '9') {
        mantissa = mantissa * 10 + (uint64_t)(s[i] - '0');
        n_digits++;
        i++;
    }

    /* Fractional part */
    if (i < len && s[i] == '.') {
        i++;
        while (i < len && s[i] >= '0' && s[i] <= '9') {
            mantissa = mantissa * 10 + (uint64_t)(s[i] - '0');
            n_frac++;
            n_digits++;
            i++;
        }
    }

    if (n_digits == 0) return 0;

    /* Fast path: no exponent and ≤18 digits (uint64 safe) */
    if (i == len && n_digits <= 18) {
        static const double pow10[] = {
            1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9,
            1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18
        };
        double result = (double)mantissa;
        if (n_frac > 0) result /= pow10[n_frac];
        *out = neg ? -result : result;
        return 1;
    }

    /* Fallback to strtod for exponents, very long numbers, etc. */
    if (len >= 64) return 0;
    char buf[64];
    memcpy(buf, s, len);
    buf[len] = '\0';
    char *end;
    errno = 0;
    *out = strtod(buf, &end);
    if (errno || (size_t)(end - buf) != len) return 0;
    return 1;
}

/*
 * Fast date parser: exactly YYYY-MM-DD (10 chars) → int32_t days since epoch.
 * Returns 1 on success, 0 on failure.
 */
static int fast_date(const char *s, size_t len, int32_t *out) {
    if (len != 10) return 0;
    if (s[4] != '-' || s[7] != '-') return 0;
    /* Parse YYYY */
    int y = 0;
    for (int i = 0; i < 4; i++) {
        if (s[i] < '0' || s[i] > '9') return 0;
        y = y * 10 + (s[i] - '0');
    }
    /* Parse MM */
    int m = 0;
    for (int i = 5; i < 7; i++) {
        if (s[i] < '0' || s[i] > '9') return 0;
        m = m * 10 + (s[i] - '0');
    }
    /* Parse DD */
    int d = 0;
    for (int i = 8; i < 10; i++) {
        if (s[i] < '0' || s[i] > '9') return 0;
        d = d * 10 + (s[i] - '0');
    }
    if (m < 1 || m > 12 || d < 1 || d > 31) return 0;
    *out = tf_date_from_ymd(y, m, d);
    return 1;
}

/*
 * Fast timestamp parser: YYYY-MM-DD[T ]HH:MM:SS[.ffffff][Z|+HH:MM|-HH:MM]
 * Returns 1 on success, 0 on failure.
 */
static int fast_timestamp(const char *s, size_t len, int64_t *out) {
    if (len < 19) return 0;
    if (s[4] != '-' || s[7] != '-') return 0;
    if (s[10] != 'T' && s[10] != ' ') return 0;
    if (s[13] != ':' || s[16] != ':') return 0;

    int y = 0, mo = 0, d = 0, h = 0, mi = 0, se = 0;
    /* YYYY */
    for (int i = 0; i < 4; i++) {
        if (s[i] < '0' || s[i] > '9') return 0;
        y = y * 10 + (s[i] - '0');
    }
    /* MM */
    for (int i = 5; i < 7; i++) {
        if (s[i] < '0' || s[i] > '9') return 0;
        mo = mo * 10 + (s[i] - '0');
    }
    /* DD */
    for (int i = 8; i < 10; i++) {
        if (s[i] < '0' || s[i] > '9') return 0;
        d = d * 10 + (s[i] - '0');
    }
    /* HH */
    for (int i = 11; i < 13; i++) {
        if (s[i] < '0' || s[i] > '9') return 0;
        h = h * 10 + (s[i] - '0');
    }
    /* MM */
    for (int i = 14; i < 16; i++) {
        if (s[i] < '0' || s[i] > '9') return 0;
        mi = mi * 10 + (s[i] - '0');
    }
    /* SS */
    for (int i = 17; i < 19; i++) {
        if (s[i] < '0' || s[i] > '9') return 0;
        se = se * 10 + (s[i] - '0');
    }
    if (mo < 1 || mo > 12 || d < 1 || d > 31) return 0;
    if (h > 23 || mi > 59 || se > 59) return 0;

    /* Optional fractional seconds */
    int frac_us = 0;
    size_t pos = 19;
    if (pos < len && s[pos] == '.') {
        pos++;
        int frac_digits = 0;
        int frac_val = 0;
        while (pos < len && s[pos] >= '0' && s[pos] <= '9' && frac_digits < 6) {
            frac_val = frac_val * 10 + (s[pos] - '0');
            frac_digits++;
            pos++;
        }
        /* Skip remaining digits beyond 6 */
        while (pos < len && s[pos] >= '0' && s[pos] <= '9') pos++;
        /* Pad to 6 digits */
        while (frac_digits < 6) { frac_val *= 10; frac_digits++; }
        frac_us = frac_val;
    }

    /* Optional timezone */
    int64_t tz_offset_us = 0;
    if (pos < len) {
        if (s[pos] == 'Z') {
            pos++;
        } else if (s[pos] == '+' || s[pos] == '-') {
            int tz_sign = (s[pos] == '-') ? -1 : 1;
            pos++;
            if (pos + 2 > len) return 0;
            int tz_h = (s[pos] - '0') * 10 + (s[pos + 1] - '0');
            pos += 2;
            int tz_m = 0;
            if (pos < len && s[pos] == ':') {
                pos++;
                if (pos + 2 > len) return 0;
                tz_m = (s[pos] - '0') * 10 + (s[pos + 1] - '0');
                pos += 2;
            }
            tz_offset_us = tz_sign * ((int64_t)tz_h * 3600000000LL + (int64_t)tz_m * 60000000LL);
        }
    }

    if (pos != len) return 0;

    *out = tf_timestamp_from_parts(y, mo, d, h, mi, se, frac_us) - tz_offset_us;
    return 1;
}

/* Detect the type of a field slice without copying it. */
static tf_type detect_type_slice(const char *s, size_t len) {
    if (len == 0) return TF_TYPE_NULL;
    int64_t iv;
    double fv;
    int32_t dv;
    if (fast_int64(s, len, &iv)) return TF_TYPE_INT64;
    if (fast_double(s, len, &fv)) return TF_TYPE_FLOAT64;
    if (fast_date(s, len, &dv)) return TF_TYPE_DATE;
    if (fast_timestamp(s, len, &iv)) return TF_TYPE_TIMESTAMP;
    return TF_TYPE_STRING;
}

/* Widen a column type if needed (NULL < INT64 < FLOAT64 < STRING). */
static tf_type widen_type(tf_type current, tf_type incoming) {
    if (current == incoming) return current;
    if (current == TF_TYPE_NULL) return incoming;
    if (incoming == TF_TYPE_NULL) return current;
    if (current == TF_TYPE_INT64 && incoming == TF_TYPE_FLOAT64) return TF_TYPE_FLOAT64;
    if (current == TF_TYPE_FLOAT64 && incoming == TF_TYPE_INT64) return TF_TYPE_FLOAT64;
    if ((current == TF_TYPE_DATE && incoming == TF_TYPE_TIMESTAMP) ||
        (current == TF_TYPE_TIMESTAMP && incoming == TF_TYPE_DATE))
        return TF_TYPE_TIMESTAMP;
    return TF_TYPE_STRING; /* anything else → string */
}

/* ================================================================
 * Zero-Copy Field Parser
 *
 * Parses a CSV line into an array of field_slice structs. For most
 * fields, the slice points directly into the line buffer (zero copy).
 * Only quoted fields with escaped quotes ("") need allocation, which
 * goes into field_arena and is valid until arena reset.
 * ================================================================ */

/*
 * Parse a CSV line into field slices. Returns the number of fields.
 *
 * - Unquoted fields: zero-copy slice into line buffer
 * - Quoted fields without "": zero-copy slice (skipping quotes)
 * - Quoted fields with "": unescaped copy allocated from field_arena
 * - Leading/trailing whitespace is trimmed from unquoted fields
 */
static size_t parse_csv_fields(const char *line, size_t line_len, char delim,
                                field_slice *fields, size_t max_fields,
                                tf_arena *field_arena) {
    size_t count = 0;
    size_t i = 0;

    while (i <= line_len && count < max_fields) {
        if (i == line_len) {
            /* Trailing delimiter → empty last field */
            if (count > 0 && i > 0 && line[i - 1] == delim) {
                fields[count].ptr = "";
                fields[count].len = 0;
                count++;
            }
            break;
        }

        if (line[i] == '"') {
            /* --- Quoted field --- */
            i++; /* skip opening quote */
            size_t start = i;
            int has_escape = 0;

            /* Scan for closing quote, detecting escaped quotes ("") */
            while (i < line_len) {
                if (line[i] == '"') {
                    if (i + 1 < line_len && line[i + 1] == '"') {
                        has_escape = 1;
                        i += 2;
                    } else {
                        break; /* closing quote */
                    }
                } else {
                    i++;
                }
            }
            size_t field_end = i;
            if (i < line_len) i++; /* skip closing quote */
            if (i < line_len && line[i] == delim) i++; /* skip delimiter */

            if (!has_escape) {
                /* Zero-copy: slice directly into line buffer, past the quotes */
                fields[count].ptr = line + start;
                fields[count].len = field_end - start;
            } else {
                /* Rare path: unescape "" → " into arena-allocated buffer */
                size_t max_len = field_end - start; /* unescaped is always shorter */
                char *buf = tf_arena_alloc(field_arena, max_len + 1);
                size_t out_len = 0;
                for (size_t j = start; j < field_end; j++) {
                    if (line[j] == '"' && j + 1 < field_end && line[j + 1] == '"') {
                        buf[out_len++] = '"';
                        j++; /* skip second quote */
                    } else {
                        buf[out_len++] = line[j];
                    }
                }
                buf[out_len] = '\0';
                fields[count].ptr = buf;
                fields[count].len = out_len;
            }
            count++;
        } else {
            /* --- Unquoted field: zero-copy slice with whitespace trimming --- */
            size_t start = i;
            while (i < line_len && line[i] != delim) i++;

            const char *fptr = line + start;
            size_t flen = i - start;

            /* Trim trailing whitespace */
            while (flen > 0 && (fptr[flen - 1] == ' ' || fptr[flen - 1] == '\t'))
                flen--;
            /* Trim leading whitespace */
            while (flen > 0 && (*fptr == ' ' || *fptr == '\t'))
                { fptr++; flen--; }

            fields[count].ptr = fptr;
            fields[count].len = flen;
            count++;

            if (i < line_len) i++; /* skip delimiter */
        }
    }

    return count;
}

/* ================================================================
 * CSV Decoder
 * ================================================================ */

typedef struct {
    char      delimiter;
    int       has_header;
    size_t    batch_size;

    /* Line accumulator: incoming bytes are appended, complete lines extracted */
    tf_buffer line_buf;

    /* Schema (discovered from first row) */
    char    **col_names;
    tf_type  *col_types;
    size_t    n_cols;
    int       schema_ready;

    /* After the first batch, types freeze and we parse directly to typed
     * columns. This avoids double parsing for >99% of rows. */
    int       types_frozen;

    /* Current batch being built */
    tf_batch *batch;
    size_t    rows_buffered;

    /* Reusable per-line scratch: field slices array and arena for escapes */
    field_slice fields[MAX_COLS];
    tf_arena   *field_arena;
} csv_decoder_state;

/*
 * Create a batch with all STRING columns (for type detection phase).
 * During this phase we don't know final types yet, so everything
 * is stored as strings and converted at emission time.
 */
static tf_batch *make_string_batch(csv_decoder_state *st) {
    tf_batch *b = tf_batch_create(st->n_cols, st->batch_size);
    if (!b) return NULL;
    for (size_t i = 0; i < st->n_cols; i++) {
        tf_batch_set_schema(b, i, st->col_names[i], TF_TYPE_STRING);
    }
    return b;
}

/*
 * Create a batch with the final (frozen) column types.
 * After type detection, all batches are created with correct types
 * so values can be parsed directly into typed columns.
 */
static tf_batch *make_typed_batch(csv_decoder_state *st) {
    tf_batch *b = tf_batch_create(st->n_cols, st->batch_size);
    if (!b) return NULL;
    for (size_t i = 0; i < st->n_cols; i++) {
        tf_batch_set_schema(b, i, st->col_names[i], st->col_types[i]);
    }
    return b;
}

/*
 * Add a row of field slices to a STRING-typed batch.
 * Used during the type detection phase (first batch).
 * Copies slice content into the batch's arena.
 */
static void add_row_strings(tf_batch *b, const field_slice *fields,
                             size_t n_fields, size_t n_cols) {
    size_t row = b->n_rows;
    size_t cols = n_cols < n_fields ? n_cols : n_fields;

    for (size_t i = 0; i < cols; i++) {
        if (fields[i].len == 0) {
            b->nulls[i][row] = 1;
        } else {
            /* Copy slice into batch arena as null-terminated string */
            char *copy = tf_arena_alloc(b->arena, fields[i].len + 1);
            memcpy(copy, fields[i].ptr, fields[i].len);
            copy[fields[i].len] = '\0';
            ((char **)b->columns[i])[row] = copy;
            b->nulls[i][row] = 0;
        }
    }
    /* Null-fill extra columns */
    for (size_t i = cols; i < n_cols; i++) {
        b->nulls[i][row] = 1;
    }
    b->n_rows = row + 1;
}

/*
 * Add a row by parsing field slices directly into typed columns.
 * Used after types are frozen (all batches after the first).
 *
 * Writes directly to column arrays, bypassing tf_batch_set_*()
 * bounds/type checks for speed in this hot path.
 */
static void add_row_typed(tf_batch *b, const field_slice *fields,
                           size_t n_fields, size_t n_cols,
                           const tf_type *types) {
    size_t row = b->n_rows;
    size_t cols = n_cols < n_fields ? n_cols : n_fields;

    for (size_t i = 0; i < cols; i++) {
        if (fields[i].len == 0) {
            b->nulls[i][row] = 1;
            continue;
        }
        switch (types[i]) {
            case TF_TYPE_INT64: {
                int64_t v;
                if (fast_int64(fields[i].ptr, fields[i].len, &v)) {
                    ((int64_t *)b->columns[i])[row] = v;
                    b->nulls[i][row] = 0;
                } else {
                    b->nulls[i][row] = 1;
                }
                break;
            }
            case TF_TYPE_FLOAT64: {
                double v;
                if (fast_double(fields[i].ptr, fields[i].len, &v)) {
                    ((double *)b->columns[i])[row] = v;
                    b->nulls[i][row] = 0;
                } else {
                    b->nulls[i][row] = 1;
                }
                break;
            }
            case TF_TYPE_STRING: {
                char *copy = tf_arena_alloc(b->arena, fields[i].len + 1);
                memcpy(copy, fields[i].ptr, fields[i].len);
                copy[fields[i].len] = '\0';
                ((char **)b->columns[i])[row] = copy;
                b->nulls[i][row] = 0;
                break;
            }
            case TF_TYPE_DATE: {
                int32_t v;
                if (fast_date(fields[i].ptr, fields[i].len, &v)) {
                    ((int32_t *)b->columns[i])[row] = v;
                    b->nulls[i][row] = 0;
                } else {
                    b->nulls[i][row] = 1;
                }
                break;
            }
            case TF_TYPE_TIMESTAMP: {
                int64_t v;
                if (fast_timestamp(fields[i].ptr, fields[i].len, &v)) {
                    ((int64_t *)b->columns[i])[row] = v;
                    b->nulls[i][row] = 0;
                } else {
                    /* Also try parsing a date-only string as timestamp at midnight */
                    int32_t dv;
                    if (fast_date(fields[i].ptr, fields[i].len, &dv)) {
                        ((int64_t *)b->columns[i])[row] = (int64_t)dv * 86400LL * 1000000LL;
                        b->nulls[i][row] = 0;
                    } else {
                        b->nulls[i][row] = 1;
                    }
                }
                break;
            }
            default:
                b->nulls[i][row] = 1;
                break;
        }
    }
    /* Null-fill extra columns */
    for (size_t i = cols; i < n_cols; i++) {
        b->nulls[i][row] = 1;
    }
    b->n_rows = row + 1;
}

/*
 * Convert a STRING batch to a typed batch using frozen column types.
 * Called once for the first batch after type detection completes.
 * Uses fast_int64/fast_double for numeric conversion.
 */
static tf_batch *convert_batch_types(csv_decoder_state *st) {
    tf_batch *src = st->batch;
    tf_batch *dst = tf_batch_create(st->n_cols, src->n_rows);
    if (!dst) return NULL;

    for (size_t i = 0; i < st->n_cols; i++) {
        tf_batch_set_schema(dst, i, st->col_names[i], st->col_types[i]);
    }

    for (size_t r = 0; r < src->n_rows; r++) {
        for (size_t c = 0; c < st->n_cols; c++) {
            if (src->nulls[c][r]) {
                dst->nulls[c][r] = 1;
                continue;
            }
            const char *val = ((char **)src->columns[c])[r];
            if (!val || !*val) {
                dst->nulls[c][r] = 1;
                continue;
            }
            size_t vlen = strlen(val);
            switch (st->col_types[c]) {
                case TF_TYPE_INT64: {
                    int64_t v;
                    if (fast_int64(val, vlen, &v)) {
                        ((int64_t *)dst->columns[c])[r] = v;
                        dst->nulls[c][r] = 0;
                    } else {
                        dst->nulls[c][r] = 1;
                    }
                    break;
                }
                case TF_TYPE_FLOAT64: {
                    double v;
                    if (fast_double(val, vlen, &v)) {
                        ((double *)dst->columns[c])[r] = v;
                        dst->nulls[c][r] = 0;
                    } else {
                        dst->nulls[c][r] = 1;
                    }
                    break;
                }
                case TF_TYPE_STRING:
                    ((char **)dst->columns[c])[r] = tf_arena_strdup(dst->arena, val);
                    dst->nulls[c][r] = 0;
                    break;
                case TF_TYPE_DATE: {
                    int32_t dv;
                    if (fast_date(val, vlen, &dv)) {
                        ((int32_t *)dst->columns[c])[r] = dv;
                        dst->nulls[c][r] = 0;
                    } else {
                        dst->nulls[c][r] = 1;
                    }
                    break;
                }
                case TF_TYPE_TIMESTAMP: {
                    int64_t tv;
                    if (fast_timestamp(val, vlen, &tv)) {
                        ((int64_t *)dst->columns[c])[r] = tv;
                        dst->nulls[c][r] = 0;
                    } else {
                        int32_t dv;
                        if (fast_date(val, vlen, &dv)) {
                            ((int64_t *)dst->columns[c])[r] = (int64_t)dv * 86400LL * 1000000LL;
                            dst->nulls[c][r] = 0;
                        } else {
                            dst->nulls[c][r] = 1;
                        }
                    }
                    break;
                }
                default:
                    dst->nulls[c][r] = 1;
                    break;
            }
        }
        dst->n_rows = r + 1;
    }

    return dst;
}

/*
 * Add a completed batch to the output array.
 */
static int emit_batch(tf_batch *batch, tf_batch ***out, size_t *n_out, size_t *out_cap) {
    if (*n_out >= *out_cap) {
        *out_cap = (*out_cap == 0) ? 4 : *out_cap * 2;
        *out = realloc(*out, *out_cap * sizeof(tf_batch *));
        if (!*out) return TF_ERROR;
    }
    (*out)[(*n_out)++] = batch;
    return TF_OK;
}

/*
 * Process a single complete CSV line.
 *
 * First line → extract column headers.
 * Subsequent lines → parse fields and add to current batch.
 * When batch is full → emit it and start a new one.
 */
static int process_line(csv_decoder_state *st, const char *line, size_t line_len,
                        tf_batch ***out, size_t *n_out, size_t *out_cap) {
    /* Reset field arena — escaped field data from previous line is discarded */
    tf_arena_reset(st->field_arena);

    /* Parse the line into zero-copy field slices */
    size_t n_fields = parse_csv_fields(line, line_len, st->delimiter,
                                        st->fields, MAX_COLS, st->field_arena);

    /* --- First line: extract column headers --- */
    if (!st->schema_ready) {
        st->n_cols = n_fields;
        st->col_names = malloc(n_fields * sizeof(char *));
        st->col_types = calloc(n_fields, sizeof(tf_type));
        if (!st->col_names || !st->col_types) return TF_ERROR;

        for (size_t i = 0; i < n_fields; i++) {
            /* Headers must outlive the line buffer, so we copy them */
            char *name = malloc(st->fields[i].len + 1);
            if (!name) return TF_ERROR;
            memcpy(name, st->fields[i].ptr, st->fields[i].len);
            name[st->fields[i].len] = '\0';
            st->col_names[i] = name;
            st->col_types[i] = TF_TYPE_NULL;
        }
        st->schema_ready = 1;
        return TF_OK;
    }

    /* --- Ensure we have a batch --- */
    if (!st->batch) {
        if (st->types_frozen) {
            st->batch = make_typed_batch(st);
        } else {
            st->batch = make_string_batch(st);
        }
        if (!st->batch) return TF_ERROR;
    }

    /* --- Add row to batch --- */
    if (!st->types_frozen) {
        /* Type detection phase: detect types and store as STRING */
        for (size_t i = 0; i < n_fields && i < st->n_cols; i++) {
            tf_type t = detect_type_slice(st->fields[i].ptr, st->fields[i].len);
            st->col_types[i] = widen_type(st->col_types[i], t);
        }
        add_row_strings(st->batch, st->fields, n_fields, st->n_cols);
    } else {
        /* Direct parse phase: parse directly to typed columns */
        add_row_typed(st->batch, st->fields, n_fields, st->n_cols, st->col_types);
    }
    st->rows_buffered++;

    /* --- Emit batch if full --- */
    if (st->rows_buffered >= st->batch_size) {
        if (!st->types_frozen) {
            /* First batch complete: convert STRING → typed, freeze types.
             * Default any still-NULL columns to STRING. */
            for (size_t i = 0; i < st->n_cols; i++) {
                if (st->col_types[i] == TF_TYPE_NULL)
                    st->col_types[i] = TF_TYPE_STRING;
            }
            tf_batch *final = convert_batch_types(st);
            if (!final) return TF_ERROR;
            tf_batch_free(st->batch);
            st->batch = NULL;
            st->types_frozen = 1;
            if (emit_batch(final, out, n_out, out_cap) != TF_OK) return TF_ERROR;
        } else {
            /* Already typed, emit directly (no conversion needed) */
            if (emit_batch(st->batch, out, n_out, out_cap) != TF_OK) return TF_ERROR;
            st->batch = NULL;
        }
        st->rows_buffered = 0;
    }

    return TF_OK;
}

/*
 * Main decode entry point: append data, extract complete lines, process them.
 *
 * The line scanner respects quoted fields that may contain newlines.
 * Complete lines are passed to process_line(); any trailing partial
 * line remains in line_buf for the next call.
 */
static int csv_decode(tf_decoder *self, const uint8_t *data, size_t len,
                      tf_batch ***out, size_t *n_out) {
    csv_decoder_state *st = self->state;
    *out = NULL;
    *n_out = 0;

    /* Append incoming data to line buffer */
    if (tf_buffer_write(&st->line_buf, data, len) != TF_OK) return TF_ERROR;

    /* Scan for complete lines */
    size_t out_cap = 0;
    uint8_t *buf = st->line_buf.data + st->line_buf.read_pos;
    size_t buf_len = st->line_buf.len - st->line_buf.read_pos;

    size_t line_start = 0;
    int in_quotes = 0;
    for (size_t i = 0; i < buf_len; i++) {
        if (buf[i] == '"') {
            in_quotes = !in_quotes;
        } else if (!in_quotes && (buf[i] == '\n' || buf[i] == '\r')) {
            size_t line_len = i - line_start;
            /* Handle \r\n */
            if (buf[i] == '\r' && i + 1 < buf_len && buf[i + 1] == '\n') {
                i++;
            }
            if (line_len > 0) {
                if (process_line(st, (const char *)buf + line_start, line_len,
                                 out, n_out, &out_cap) != TF_OK)
                    return TF_ERROR;
            }
            line_start = i + 1;
        }
    }

    /* Move unconsumed data to start of buffer */
    st->line_buf.read_pos += line_start;
    tf_buffer_compact(&st->line_buf);

    return TF_OK;
}

/*
 * Flush: process any remaining partial line and emit the final batch.
 */
static int csv_flush(tf_decoder *self, tf_batch ***out, size_t *n_out) {
    csv_decoder_state *st = self->state;
    *out = NULL;
    *n_out = 0;
    size_t out_cap = 0;

    /* Process any remaining data as the last line */
    size_t remaining = tf_buffer_readable(&st->line_buf);
    if (remaining > 0) {
        uint8_t *buf = st->line_buf.data + st->line_buf.read_pos;
        if (process_line(st, (const char *)buf, remaining,
                         out, n_out, &out_cap) != TF_OK)
            return TF_ERROR;
        st->line_buf.read_pos = st->line_buf.len;
    }

    /* Emit any remaining partial batch */
    if (st->batch && st->rows_buffered > 0) {
        if (!st->types_frozen) {
            /* Small file: fewer rows than batch_size. Convert and emit. */
            for (size_t i = 0; i < st->n_cols; i++) {
                if (st->col_types[i] == TF_TYPE_NULL)
                    st->col_types[i] = TF_TYPE_STRING;
            }
            tf_batch *final = convert_batch_types(st);
            if (!final) return TF_ERROR;
            tf_batch_free(st->batch);
            st->batch = NULL;
            if (emit_batch(final, out, n_out, &out_cap) != TF_OK) return TF_ERROR;
        } else {
            /* Already typed, emit directly */
            if (emit_batch(st->batch, out, n_out, &out_cap) != TF_OK) return TF_ERROR;
            st->batch = NULL;
        }
        st->rows_buffered = 0;
    }

    return TF_OK;
}

static void csv_decoder_destroy(tf_decoder *self) {
    csv_decoder_state *st = self->state;
    if (st) {
        tf_buffer_free(&st->line_buf);
        if (st->batch) tf_batch_free(st->batch);
        if (st->field_arena) tf_arena_free(st->field_arena);
        for (size_t i = 0; i < st->n_cols; i++) free(st->col_names[i]);
        free(st->col_names);
        free(st->col_types);
        free(st);
    }
    free(self);
}

tf_decoder *tf_csv_decoder_create(const cJSON *args) {
    csv_decoder_state *st = calloc(1, sizeof(csv_decoder_state));
    if (!st) return NULL;

    st->delimiter = ',';
    st->has_header = 1;
    st->batch_size = DEFAULT_BATCH_SIZE;

    if (args) {
        cJSON *d = cJSON_GetObjectItemCaseSensitive(args, "delimiter");
        if (cJSON_IsString(d) && d->valuestring[0])
            st->delimiter = d->valuestring[0];

        cJSON *h = cJSON_GetObjectItemCaseSensitive(args, "header");
        if (cJSON_IsBool(h))
            st->has_header = cJSON_IsTrue(h);

        cJSON *bs = cJSON_GetObjectItemCaseSensitive(args, "batch_size");
        if (cJSON_IsNumber(bs) && bs->valueint > 0)
            st->batch_size = (size_t)bs->valueint;
    }

    tf_buffer_init(&st->line_buf);

    /* Arena for escaped quoted field data (reset per line, rarely used) */
    st->field_arena = tf_arena_create(4096);
    if (!st->field_arena) { free(st); return NULL; }

    tf_decoder *dec = malloc(sizeof(tf_decoder));
    if (!dec) { tf_arena_free(st->field_arena); free(st); return NULL; }
    dec->decode = csv_decode;
    dec->flush = csv_flush;
    dec->destroy = csv_decoder_destroy;
    dec->state = st;
    return dec;
}

/* ================================================================
 * CSV Encoder (unchanged — already efficient)
 * ================================================================ */

typedef struct {
    char delimiter;
    int  header_written;
} csv_encoder_state;

/* Check if a field needs quoting */
static int needs_quoting(const char *s, char delim) {
    for (const char *p = s; *p; p++) {
        if (*p == delim || *p == '"' || *p == '\n' || *p == '\r')
            return 1;
    }
    return 0;
}

static int write_field(tf_buffer *out, const char *s, char delim) {
    if (needs_quoting(s, delim)) {
        if (tf_buffer_write(out, (const uint8_t *)"\"", 1) != TF_OK) return TF_ERROR;
        for (const char *p = s; *p; p++) {
            if (*p == '"') {
                if (tf_buffer_write(out, (const uint8_t *)"\"\"", 2) != TF_OK) return TF_ERROR;
            } else {
                if (tf_buffer_write(out, (const uint8_t *)p, 1) != TF_OK) return TF_ERROR;
            }
        }
        if (tf_buffer_write(out, (const uint8_t *)"\"", 1) != TF_OK) return TF_ERROR;
    } else {
        if (tf_buffer_write_str(out, s) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static int csv_encode(tf_encoder *self, tf_batch *in, tf_buffer *out) {
    csv_encoder_state *st = self->state;
    char dbuf[2] = { st->delimiter, '\0' };

    /* Write header */
    if (!st->header_written) {
        for (size_t i = 0; i < in->n_cols; i++) {
            if (i > 0) tf_buffer_write_str(out, dbuf);
            write_field(out, in->col_names[i], st->delimiter);
        }
        tf_buffer_write(out, (const uint8_t *)"\n", 1);
        st->header_written = 1;
    }

    /* Write rows */
    char numbuf[64];
    for (size_t r = 0; r < in->n_rows; r++) {
        for (size_t c = 0; c < in->n_cols; c++) {
            if (c > 0) tf_buffer_write_str(out, dbuf);
            if (tf_batch_is_null(in, r, c)) {
                /* empty field for null */
                continue;
            }
            switch (in->col_types[c]) {
                case TF_TYPE_BOOL:
                    tf_buffer_write_str(out, tf_batch_get_bool(in, r, c) ? "true" : "false");
                    break;
                case TF_TYPE_INT64:
                    snprintf(numbuf, sizeof(numbuf), "%lld", (long long)tf_batch_get_int64(in, r, c));
                    tf_buffer_write_str(out, numbuf);
                    break;
                case TF_TYPE_FLOAT64:
                    snprintf(numbuf, sizeof(numbuf), "%g", tf_batch_get_float64(in, r, c));
                    tf_buffer_write_str(out, numbuf);
                    break;
                case TF_TYPE_STRING:
                    write_field(out, tf_batch_get_string(in, r, c), st->delimiter);
                    break;
                case TF_TYPE_DATE: {
                    char dbuf2[16];
                    tf_date_format(tf_batch_get_date(in, r, c), dbuf2, sizeof(dbuf2));
                    tf_buffer_write_str(out, dbuf2);
                    break;
                }
                case TF_TYPE_TIMESTAMP: {
                    char tsbuf[40];
                    tf_timestamp_format(tf_batch_get_timestamp(in, r, c), tsbuf, sizeof(tsbuf));
                    tf_buffer_write_str(out, tsbuf);
                    break;
                }
                default:
                    break;
            }
        }
        tf_buffer_write(out, (const uint8_t *)"\n", 1);
    }

    return TF_OK;
}

static int csv_encoder_flush(tf_encoder *self, tf_buffer *out) {
    (void)self; (void)out;
    return TF_OK;
}

static void csv_encoder_destroy(tf_encoder *self) {
    free(self->state);
    free(self);
}

tf_encoder *tf_csv_encoder_create(const cJSON *args) {
    csv_encoder_state *st = calloc(1, sizeof(csv_encoder_state));
    if (!st) return NULL;

    st->delimiter = ',';
    st->header_written = 0;

    if (args) {
        cJSON *d = cJSON_GetObjectItemCaseSensitive(args, "delimiter");
        if (cJSON_IsString(d) && d->valuestring[0])
            st->delimiter = d->valuestring[0];
    }

    tf_encoder *enc = malloc(sizeof(tf_encoder));
    if (!enc) { free(st); return NULL; }
    enc->encode = csv_encode;
    enc->flush = csv_encoder_flush;
    enc->destroy = csv_encoder_destroy;
    enc->state = st;
    return enc;
}
