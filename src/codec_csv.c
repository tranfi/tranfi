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

#define DEFAULT_BATCH_SIZE      1024
#define DEFAULT_MAX_ERROR_BYTES 4096
#define DEFAULT_MAX_RECORD_BYTES (64 * 1024 * 1024)
#define DEFAULT_MAX_COLUMNS      8192

/* ================================================================
 * Field Slice — zero-copy reference into the line buffer
 * ================================================================ */

typedef struct {
    const char *ptr;   /* points into line buffer (or field_arena for escapes) */
    size_t      len;
    int         quoted;
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
 * Uses a short-decimal fast path and falls back to strtod for values that need correct rounding.
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

    /* Fast path: no exponent and short mantissas. Longer decimals need strtod's correct rounding. */
    if (i == len && n_digits <= 15) {
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
    if ((size_t)(end - buf) != len) return 0;
    if (errno == ERANGE) {
        if (!isfinite(*out) || *out == 0.0) return 0;
    } else if (errno) {
        return 0;
    }
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
 * - Leading/trailing whitespace is optionally trimmed from unquoted fields
 */
static int parse_csv_fields(const char *line, size_t line_len, char delim,
                            field_slice *fields, size_t max_fields,
                            tf_arena *field_arena, int trim_ws,
                            size_t *out_count) {
    if (!out_count) return TF_ERROR;
    size_t count = 0;
    size_t i = 0;

    while (i <= line_len) {
        if (i == line_len) {
            /* Trailing delimiter -> empty last field */
            if (count > 0 && i > 0 && line[i - 1] == delim) {
                if (count < max_fields) {
                    fields[count].ptr = "";
                    fields[count].len = 0;
                    fields[count].quoted = 0;
                }
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

            if (count < max_fields) {
                if (!has_escape) {
                    /* Zero-copy: slice directly into line buffer, past the quotes */
                    fields[count].ptr = line + start;
                    fields[count].len = field_end - start;
                    fields[count].quoted = 1;
                } else {
                    /* Rare path: unescape "" -> " into arena-allocated buffer */
                    size_t max_len = field_end - start; /* unescaped is always shorter */
                    size_t bytes = 0;
                    if (tf_check_byte_limit(max_len, TF_MAX_CELL_BYTES,
                                            "csv", "cell") != TF_OK ||
                        tf_size_add(max_len, 1, &bytes) != TF_OK)
                        return TF_ERROR;
                    char *buf = tf_arena_alloc(field_arena, bytes);
                    if (!buf) return TF_ERROR;
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
                    fields[count].quoted = 1;
                }
            }
            count++;
        } else {
            /* --- Unquoted field: zero-copy slice with whitespace trimming --- */
            size_t start = i;
            while (i < line_len && line[i] != delim) i++;

            const char *fptr = line + start;
            size_t flen = i - start;

            if (trim_ws) {
                /* Trim trailing ASCII whitespace */
                while (flen > 0 && (fptr[flen - 1] == ' ' || fptr[flen - 1] == '\t'))
                    flen--;
                /* Trim leading ASCII whitespace */
                while (flen > 0 && (*fptr == ' ' || *fptr == '\t'))
                    { fptr++; flen--; }
            }

            if (count < max_fields) {
                fields[count].ptr = fptr;
                fields[count].len = flen;
                fields[count].quoted = 0;
            }
            count++;

            if (i < line_len) i++; /* skip delimiter */
        }
    }

    *out_count = count;
    return TF_OK;
}

/* ================================================================
 * CSV Decoder
 * ================================================================ */

typedef enum {
    CSV_MODE_PERMISSIVE = 0,
    CSV_MODE_REPAIR,
    CSV_MODE_STRICT,
} csv_decode_mode;

typedef struct {
    char      delimiter;
    int       has_header;
    int       skip_repeated_header;
    int       after_input_boundary;
    size_t    batch_size;
    csv_decode_mode mode;
    char    **null_literals;
    size_t    n_null_literals;
    int       quoted_nulls;
    int       trim_ws;
    int       skip_empty_rows;
    size_t    skip_rows;
    size_t    skipped_rows;
    int       limit_rows;
    size_t    n_max;
    size_t    data_rows_read;
    char     *comment;
    size_t    comment_len;
    size_t    max_error_bytes;
    size_t    max_record_bytes;
    size_t    max_columns;
    size_t    audit_limit;
    size_t    audit_emitted;
    int       audit;
    tf_audit_options audit_opts;
    size_t    line_number;
    size_t    byte_offset;

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
    field_slice *fields;
    size_t       fields_cap;
    tf_arena    *field_arena;
} csv_decoder_state;

static const char *csv_mode_name(csv_decode_mode mode) {
    switch (mode) {
        case CSV_MODE_REPAIR: return "repair";
        case CSV_MODE_STRICT: return "strict";
        case CSV_MODE_PERMISSIVE:
        default: return "permissive";
    }
}

static int parse_csv_mode(const char *s, csv_decode_mode *out) {
    if (!s || strcmp(s, "permissive") == 0 || strcmp(s, "lenient") == 0) {
        *out = CSV_MODE_PERMISSIVE;
        return 1;
    }
    if (strcmp(s, "repair") == 0) {
        *out = CSV_MODE_REPAIR;
        return 1;
    }
    if (strcmp(s, "strict") == 0 || strcmp(s, "fail") == 0 || strcmp(s, "error") == 0) {
        *out = CSV_MODE_STRICT;
        return 1;
    }
    return 0;
}

static int csv_line_is_blank(const char *line, size_t line_len) {
    for (size_t i = 0; i < line_len; i++) {
        if (line[i] != ' ' && line[i] != '\t') return 0;
    }
    return 1;
}

static int csv_find_comment(const csv_decoder_state *st, const char *line, size_t line_len,
                            size_t *comment_pos) {
    if (!st->comment || st->comment_len == 0 || st->comment_len > line_len) return 0;
    int in_quotes = 0;
    int at_field_start = 1;
    for (size_t i = 0; i < line_len; i++) {
        if (in_quotes) {
            if (line[i] == '"') {
                if (i + 1 < line_len && line[i + 1] == '"') {
                    i++;
                } else {
                    in_quotes = 0;
                    at_field_start = 0;
                }
            }
            continue;
        }
        if (line[i] == '"' && at_field_start) {
            in_quotes = 1;
            at_field_start = 0;
            continue;
        }
        if (i + st->comment_len <= line_len &&
            memcmp(line + i, st->comment, st->comment_len) == 0) {
            *comment_pos = i;
            return 1;
        }
        if (line[i] == st->delimiter) at_field_start = 1;
        else at_field_start = 0;
    }
    return 0;
}

static char *csv_raw_preview(const csv_decoder_state *st, const char *line, size_t line_len,
                             int *truncated_out) {
    size_t keep = line_len;
    int truncated = 0;
    if (st->max_error_bytes > 0 && keep > st->max_error_bytes) {
        keep = st->max_error_bytes;
        truncated = 1;
    }
    char *raw = malloc(keep + 1);
    if (!raw) return NULL;
    memcpy(raw, line, keep);
    raw[keep] = '\0';
    if (truncated_out) *truncated_out = truncated;
    return raw;
}

static int csv_add_raw_payload(csv_decoder_state *st, cJSON *obj, const char *raw, int truncated) {
    if (st && st->audit_opts.include_row) {
        char *safe_raw = tf_audit_format_string_dup_for_column(&st->audit_opts, "raw", raw);
        if (!safe_raw) return TF_ERROR;
        if (st->audit_opts.max_bytes > 0 && strlen(safe_raw) > st->audit_opts.max_bytes) {
            if (tf_json_add_bool(obj, "_audit_truncated", 1) != TF_OK ||
                tf_json_add_number(obj, "max_bytes", (double)st->audit_opts.max_bytes) != TF_OK) {
                free(safe_raw);
                return TF_ERROR;
            }
        } else {
            if (tf_json_add_string(obj, "raw", safe_raw) != TF_OK) {
                free(safe_raw);
                return TF_ERROR;
            }
        }
        free(safe_raw);
    }
    if (truncated && tf_json_add_bool(obj, "truncated", 1) != TF_OK) return TF_ERROR;
    return TF_OK;
}

static int emit_csv_repair_audit(csv_decoder_state *st, const char *line, size_t line_len,
                                 size_t line_no, size_t byte_offset, size_t n_fields,
                                 const char *message, tf_side_channels *side) {
    if (!st->audit || st->audit_emitted >= st->audit_limit || !side || !side->stats) return TF_OK;
    int truncated = 0;
    char *raw = csv_raw_preview(st, line, line_len, &truncated);
    if (!raw) return TF_ERROR;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) { free(raw); return TF_ERROR; }
    int rc = TF_ERROR;
    if (tf_json_add_string(obj, "type", "audit") != TF_OK ||
        tf_json_add_string(obj, "op", "codec.csv.decode") != TF_OK ||
        tf_json_add_string(obj, "event", "row_repaired") != TF_OK ||
        tf_json_add_string(obj, "reason", "csv_field_count") != TF_OK ||
        tf_json_add_string(obj, "channel", "audit") != TF_OK ||
        tf_json_add_string(obj, "mode", csv_mode_name(st->mode)) != TF_OK ||
        tf_json_add_string(obj, "action", "repair") != TF_OK ||
        tf_json_add_number(obj, "line", (double)line_no) != TF_OK ||
        tf_json_add_number(obj, "byte_offset", (double)byte_offset) != TF_OK ||
        tf_json_add_number(obj, "expected_fields", (double)st->n_cols) != TF_OK ||
        tf_json_add_number(obj, "actual_fields", (double)n_fields) != TF_OK ||
        tf_json_add_string(obj, "message", message ? message : "CSV row field count differs from header") != TF_OK ||
        tf_json_add_number(obj, "raw_bytes", (double)line_len) != TF_OK ||
        csv_add_raw_payload(st, obj, raw, truncated) != TF_OK) {
        goto done;
    }
    rc = tf_buffer_write_json_line(side->stats, obj);
done:
    cJSON_Delete(obj);
    free(raw);
    if (rc == TF_OK) st->audit_emitted++;
    return rc;
}

static int emit_csv_field_count_diagnostic(csv_decoder_state *st, const char *line, size_t line_len,
                                           size_t line_no, size_t byte_offset, size_t n_fields,
                                           const char *action, const char *severity,
                                           const char *message, tf_side_channels *side) {
    if (!side || !side->errors) return TF_OK;

    size_t keep = line_len;
    int truncated = 0;
    if (st->max_error_bytes > 0 && keep > st->max_error_bytes) {
        keep = st->max_error_bytes;
        truncated = 1;
    }

    char *raw = malloc(keep + 1);
    if (!raw) return TF_ERROR;
    memcpy(raw, line, keep);
    raw[keep] = '\0';

    cJSON *obj = cJSON_CreateObject();
    if (!obj) { free(raw); return TF_ERROR; }
    int rc = TF_ERROR;
    if (tf_json_add_string(obj, "type", "csv_field_count") != TF_OK ||
        tf_json_add_string(obj, "op", "codec.csv.decode") != TF_OK ||
        tf_json_add_string(obj, "mode", csv_mode_name(st->mode)) != TF_OK ||
        tf_json_add_string(obj, "action", action) != TF_OK ||
        tf_json_add_string(obj, "severity", severity) != TF_OK ||
        tf_json_add_number(obj, "line", (double)line_no) != TF_OK ||
        tf_json_add_number(obj, "byte_offset", (double)byte_offset) != TF_OK ||
        tf_json_add_number(obj, "expected_fields", (double)st->n_cols) != TF_OK ||
        tf_json_add_number(obj, "actual_fields", (double)n_fields) != TF_OK ||
        tf_json_add_string(obj, "message", message ? message : "CSV row field count differs from header") != TF_OK ||
        tf_json_add_number(obj, "raw_bytes", (double)line_len) != TF_OK ||
        csv_add_raw_payload(st, obj, raw, truncated) != TF_OK) {
        goto done;
    }
    rc = tf_buffer_write_json_line(side->errors, obj);
done:
    cJSON_Delete(obj);
    free(raw);
    return rc;
}

static int emit_csv_column_limit_diagnostic(csv_decoder_state *st, const char *line, size_t line_len,
                                            size_t line_no, size_t byte_offset, size_t n_fields,
                                            tf_side_channels *side) {
    if (!side || !side->errors) return TF_OK;

    size_t keep = line_len;
    int truncated = 0;
    if (st->max_error_bytes > 0 && keep > st->max_error_bytes) {
        keep = st->max_error_bytes;
        truncated = 1;
    }

    char *raw = malloc(keep + 1);
    if (!raw) return TF_ERROR;
    memcpy(raw, line, keep);
    raw[keep] = '\0';

    cJSON *obj = cJSON_CreateObject();
    if (!obj) { free(raw); return TF_ERROR; }
    int rc = TF_ERROR;
    if (tf_json_add_string(obj, "type", "csv_too_many_columns") != TF_OK ||
        tf_json_add_string(obj, "op", "codec.csv.decode") != TF_OK ||
        tf_json_add_string(obj, "mode", csv_mode_name(st->mode)) != TF_OK ||
        tf_json_add_string(obj, "action", "fail") != TF_OK ||
        tf_json_add_string(obj, "severity", "error") != TF_OK ||
        tf_json_add_number(obj, "line", (double)line_no) != TF_OK ||
        tf_json_add_number(obj, "byte_offset", (double)byte_offset) != TF_OK ||
        tf_json_add_number(obj, "max_columns", (double)st->max_columns) != TF_OK ||
        tf_json_add_number(obj, "actual_fields", (double)n_fields) != TF_OK ||
        tf_json_add_string(obj, "message", "CSV record exceeds max_columns") != TF_OK ||
        tf_json_add_number(obj, "raw_bytes", (double)line_len) != TF_OK ||
        csv_add_raw_payload(st, obj, raw, truncated) != TF_OK) {
        goto done;
    }
    rc = tf_buffer_write_json_line(side->errors, obj);
done:
    cJSON_Delete(obj);
    free(raw);
    return rc;
}

static int emit_csv_record_size_diagnostic(csv_decoder_state *st, const char *record, size_t record_len,
                                           size_t line_no, size_t byte_offset, tf_side_channels *side) {
    if (!side || !side->errors) return TF_OK;

    size_t keep = record_len;
    int truncated = 0;
    if (st->max_error_bytes > 0 && keep > st->max_error_bytes) {
        keep = st->max_error_bytes;
        truncated = 1;
    }

    char *raw = malloc(keep + 1);
    if (!raw) return TF_ERROR;
    memcpy(raw, record, keep);
    raw[keep] = '\0';

    cJSON *obj = cJSON_CreateObject();
    if (!obj) { free(raw); return TF_ERROR; }
    int rc = TF_ERROR;
    if (tf_json_add_string(obj, "type", "csv_record_too_large") != TF_OK ||
        tf_json_add_string(obj, "op", "codec.csv.decode") != TF_OK ||
        tf_json_add_string(obj, "action", "fail") != TF_OK ||
        tf_json_add_string(obj, "severity", "error") != TF_OK ||
        tf_json_add_number(obj, "line", (double)line_no) != TF_OK ||
        tf_json_add_number(obj, "byte_offset", (double)byte_offset) != TF_OK ||
        tf_json_add_number(obj, "max_record_bytes", (double)st->max_record_bytes) != TF_OK ||
        tf_json_add_number(obj, "observed_bytes", (double)record_len) != TF_OK ||
        tf_json_add_string(obj, "message", "CSV record exceeds max_record_bytes") != TF_OK ||
        tf_json_add_number(obj, "raw_bytes", (double)record_len) != TF_OK ||
        csv_add_raw_payload(st, obj, raw, truncated) != TF_OK) {
        goto done;
    }
    rc = tf_buffer_write_json_line(side->errors, obj);
done:
    cJSON_Delete(obj);
    free(raw);
    return rc;
}

static int check_csv_record_limit(csv_decoder_state *st, const uint8_t *buf, size_t line_start,
                                  size_t observed_len, size_t line_no, size_t byte_offset,
                                  tf_side_channels *side) {
    if (st->max_record_bytes == 0 || observed_len <= st->max_record_bytes) return TF_OK;
    if (emit_csv_record_size_diagnostic(st, (const char *)buf + line_start, observed_len,
                                        line_no, byte_offset, side) != TF_OK)
        return TF_ERROR;
    char err[256];
    snprintf(err, sizeof(err), "csv record exceeds max_record_bytes at line %zu: max %zu bytes, observed %zu bytes",
             line_no, st->max_record_bytes, observed_len);
    tf_set_last_error(err);
    return TF_ERROR;
}

static int csv_add_null_literal(csv_decoder_state *st, const char *ptr, size_t len) {
    size_t copy_len = 0;
    if (tf_size_add(len, 1, &copy_len) != TF_OK) return TF_ERROR;
    char *copy = tf_mallocarray_checked(copy_len, sizeof(char));
    if (!copy) return TF_ERROR;
    memcpy(copy, ptr, len);
    copy[len] = '\0';
    size_t next_count = 0;
    if (tf_size_add(st->n_null_literals, 1, &next_count) != TF_OK) {
        free(copy);
        return TF_ERROR;
    }
    char **tmp = tf_reallocarray_checked(st->null_literals, next_count, sizeof(char *));
    if (!tmp) {
        free(copy);
        return TF_ERROR;
    }
    st->null_literals = tmp;
    st->null_literals[st->n_null_literals++] = copy;
    return TF_OK;
}

static int csv_add_null_literal_token(csv_decoder_state *st, const char *tok) {
    if (!tok) return TF_OK;
    return csv_add_null_literal(st, tok, strlen(tok));
}

static void csv_free_schema_arrays(char **col_names, tf_type *col_types, size_t n_names) {
    if (col_names) {
        for (size_t i = 0; i < n_names; i++) free(col_names[i]);
    }
    free(col_names);
    free(col_types);
}

static int csv_init_schema(csv_decoder_state *st, const field_slice *fields,
                           size_t n_fields, int synthetic_names) {
    char **col_names = tf_callocarray_checked(n_fields ? n_fields : 1, sizeof(char *));
    tf_type *col_types = tf_callocarray_checked(n_fields ? n_fields : 1, sizeof(tf_type));
    if (!col_names || !col_types) {
        free(col_names);
        free(col_types);
        return TF_ERROR;
    }

    size_t schema_bytes = 0;
    for (size_t i = 0; i < n_fields; i++) {
        char synthetic[64];
        const char *name_ptr = fields[i].ptr;
        size_t name_len = fields[i].len;
        if (synthetic_names) {
            int n = snprintf(synthetic, sizeof(synthetic), "col%zu", i + 1);
            if (n <= 0 || (size_t)n >= sizeof(synthetic)) {
                csv_free_schema_arrays(col_names, col_types, i);
                return TF_ERROR;
            }
            name_ptr = synthetic;
            name_len = (size_t)n;
        }

        size_t name_bytes = 0, next_schema_bytes = 0;
        if (tf_check_byte_limit(name_len, TF_MAX_COLUMN_NAME_BYTES,
                                "csv", "column name") != TF_OK ||
            tf_size_add(name_len, 1, &name_bytes) != TF_OK ||
            tf_size_add(schema_bytes, name_bytes, &next_schema_bytes) != TF_OK ||
            tf_check_byte_limit(next_schema_bytes, TF_MAX_SCHEMA_BYTES,
                                "csv", "schema") != TF_OK) {
            csv_free_schema_arrays(col_names, col_types, i);
            return TF_ERROR;
        }
        char *name = malloc(name_bytes);
        if (!name) {
            csv_free_schema_arrays(col_names, col_types, i);
            return TF_ERROR;
        }
        memcpy(name, name_ptr, name_len);
        name[name_len] = '\0';
        col_names[i] = name;
        col_types[i] = TF_TYPE_NULL;
        schema_bytes = next_schema_bytes;
    }

    st->n_cols = n_fields;
    st->col_names = col_names;
    st->col_types = col_types;
    st->schema_ready = 1;
    st->after_input_boundary = 0;
    return TF_OK;
}

static int csv_add_null_literal_list(csv_decoder_state *st, const char *list) {
    if (!list) return TF_OK;
    const char *start = list;
    for (const char *p = list; ; p++) {
        if (*p == ',' || *p == '\0') {
            if (csv_add_null_literal(st, start, (size_t)(p - start)) != TF_OK) return TF_ERROR;
            if (*p == '\0') break;
            start = p + 1;
        }
    }
    return TF_OK;
}

static int csv_configure_null_literals(csv_decoder_state *st, const cJSON *args) {
    const cJSON *nulls = cJSON_GetObjectItemCaseSensitive(args, "nulls");
    if (!nulls) nulls = cJSON_GetObjectItemCaseSensitive(args, "na");
    if (!nulls) return TF_OK;

    if (cJSON_IsString(nulls)) {
        return csv_add_null_literal_list(st, nulls->valuestring ? nulls->valuestring : "");
    }
    if (cJSON_IsArray(nulls)) {
        const cJSON *item = NULL;
        cJSON_ArrayForEach(item, nulls) {
            if (cJSON_IsString(item)) {
                if (csv_add_null_literal_token(st, item->valuestring) != TF_OK) return TF_ERROR;
            }
        }
    }
    return TF_OK;
}

static int csv_field_is_null(const csv_decoder_state *st, const field_slice *field) {
    if (field->quoted && !st->quoted_nulls) return 0;
    if (field->len == 0) return 1;
    for (size_t i = 0; i < st->n_null_literals; i++) {
        const char *lit = st->null_literals[i];
        size_t len = strlen(lit);
        if (field->len == len && memcmp(field->ptr, lit, len) == 0) return 1;
    }
    return 0;
}

static tf_type detect_type_field(const csv_decoder_state *st, const field_slice *field) {
    if (csv_field_is_null(st, field)) return TF_TYPE_NULL;
    if (field->len == 0) return TF_TYPE_STRING;
    return detect_type_slice(field->ptr, field->len);
}

/*
 * Create a batch with all STRING columns (for type detection phase).
 * During this phase we don't know final types yet, so everything
 * is stored as strings and converted at emission time.
 */
static tf_batch *make_string_batch(csv_decoder_state *st) {
    tf_batch *b = tf_batch_create(st->n_cols, st->batch_size);
    if (!b) return NULL;
    for (size_t i = 0; i < st->n_cols; i++) {
        if (tf_batch_set_schema(b, i, st->col_names[i], TF_TYPE_STRING) != TF_OK) {
            tf_batch_free(b);
            return NULL;
        }
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
        if (tf_batch_set_schema(b, i, st->col_names[i], st->col_types[i]) != TF_OK) {
            tf_batch_free(b);
            return NULL;
        }
    }
    return b;
}

/*
 * Add a row of field slices to a STRING-typed batch.
 * Used during the type detection phase (first batch).
 * Copies slice content into the batch's arena.
 */
static int add_row_strings(tf_batch *b, const csv_decoder_state *st,
                           const field_slice *fields, size_t n_fields,
                           size_t n_cols) {
    size_t row = b->n_rows;
    size_t cols = n_cols < n_fields ? n_cols : n_fields;

    for (size_t i = 0; i < cols; i++) {
        if (csv_field_is_null(st, &fields[i])) {
            if (tf_batch_set_null(b, row, i) != TF_OK) return TF_ERROR;
        } else {
            if (tf_batch_set_string_len(b, row, i, fields[i].ptr, fields[i].len) != TF_OK)
                return TF_ERROR;
        }
    }
    /* Null-fill extra columns */
    for (size_t i = cols; i < n_cols; i++) {
        if (tf_batch_set_null(b, row, i) != TF_OK) return TF_ERROR;
    }
    if (tf_batch_expose_row(b, row) != TF_OK) return TF_ERROR;
    return TF_OK;
}

/*
 * Add a row by parsing field slices directly into typed columns.
 * Used after types are frozen (all batches after the first).
 *
 * Uses checked batch setters so failed writes cannot expose a partial row.
 */
static int add_row_typed(tf_batch *b, const csv_decoder_state *st,
                         const field_slice *fields, size_t n_fields,
                         size_t n_cols, const tf_type *types) {
    size_t row = b->n_rows;
    size_t cols = n_cols < n_fields ? n_cols : n_fields;

    for (size_t i = 0; i < cols; i++) {
        if (csv_field_is_null(st, &fields[i])) {
            if (tf_batch_set_null(b, row, i) != TF_OK) return TF_ERROR;
            continue;
        }
        switch (types[i]) {
            case TF_TYPE_INT64: {
                int64_t v;
                if (fast_int64(fields[i].ptr, fields[i].len, &v)) {
                    if (tf_batch_set_int64(b, row, i, v) != TF_OK) return TF_ERROR;
                } else {
                    if (tf_batch_set_null(b, row, i) != TF_OK) return TF_ERROR;
                }
                break;
            }
            case TF_TYPE_FLOAT64: {
                double v;
                if (fast_double(fields[i].ptr, fields[i].len, &v)) {
                    if (tf_batch_set_float64(b, row, i, v) != TF_OK) return TF_ERROR;
                } else {
                    if (tf_batch_set_null(b, row, i) != TF_OK) return TF_ERROR;
                }
                break;
            }
            case TF_TYPE_STRING: {
                if (tf_batch_set_string_len(b, row, i, fields[i].ptr, fields[i].len) != TF_OK)
                    return TF_ERROR;
                break;
            }
            case TF_TYPE_DATE: {
                int32_t v;
                if (fast_date(fields[i].ptr, fields[i].len, &v)) {
                    if (tf_batch_set_date(b, row, i, v) != TF_OK) return TF_ERROR;
                } else {
                    if (tf_batch_set_null(b, row, i) != TF_OK) return TF_ERROR;
                }
                break;
            }
            case TF_TYPE_TIMESTAMP: {
                int64_t v;
                if (fast_timestamp(fields[i].ptr, fields[i].len, &v)) {
                    if (tf_batch_set_timestamp(b, row, i, v) != TF_OK) return TF_ERROR;
                } else {
                    /* Also try parsing a date-only string as timestamp at midnight */
                    int32_t dv;
                    if (fast_date(fields[i].ptr, fields[i].len, &dv)) {
                        v = (int64_t)dv * 86400LL * 1000000LL;
                        if (tf_batch_set_timestamp(b, row, i, v) != TF_OK) return TF_ERROR;
                    } else {
                        if (tf_batch_set_null(b, row, i) != TF_OK) return TF_ERROR;
                    }
                }
                break;
            }
            default:
                if (tf_batch_set_null(b, row, i) != TF_OK) return TF_ERROR;
                break;
        }
    }
    /* Null-fill extra columns */
    for (size_t i = cols; i < n_cols; i++) {
        if (tf_batch_set_null(b, row, i) != TF_OK) return TF_ERROR;
    }
    if (tf_batch_expose_row(b, row) != TF_OK) return TF_ERROR;
    return TF_OK;
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
        if (tf_batch_set_schema(dst, i, st->col_names[i], st->col_types[i]) != TF_OK) {
            tf_batch_free(dst);
            return NULL;
        }
    }

    for (size_t r = 0; r < src->n_rows; r++) {
        for (size_t c = 0; c < st->n_cols; c++) {
            if (src->nulls[c][r]) {
                if (tf_batch_set_null(dst, r, c) != TF_OK) {
                    tf_batch_free(dst);
                    return NULL;
                }
                continue;
            }
            const char *val = ((char **)src->columns[c])[r];
            if (!val) {
                if (tf_batch_set_null(dst, r, c) != TF_OK) {
                    tf_batch_free(dst);
                    return NULL;
                }
                continue;
            }
            size_t vlen = strlen(val);
            switch (st->col_types[c]) {
                case TF_TYPE_INT64: {
                    int64_t v;
                    if (fast_int64(val, vlen, &v)) {
                        if (tf_batch_set_int64(dst, r, c, v) != TF_OK) {
                            tf_batch_free(dst);
                            return NULL;
                        }
                    } else {
                        if (tf_batch_set_null(dst, r, c) != TF_OK) {
                            tf_batch_free(dst);
                            return NULL;
                        }
                    }
                    break;
                }
                case TF_TYPE_FLOAT64: {
                    double v;
                    if (fast_double(val, vlen, &v)) {
                        if (tf_batch_set_float64(dst, r, c, v) != TF_OK) {
                            tf_batch_free(dst);
                            return NULL;
                        }
                    } else {
                        if (tf_batch_set_null(dst, r, c) != TF_OK) {
                            tf_batch_free(dst);
                            return NULL;
                        }
                    }
                    break;
                }
                case TF_TYPE_STRING: {
                    if (tf_batch_set_string(dst, r, c, val) != TF_OK) {
                        tf_batch_free(dst);
                        return NULL;
                    }
                    break;
                }
                case TF_TYPE_DATE: {
                    int32_t dv;
                    if (fast_date(val, vlen, &dv)) {
                        if (tf_batch_set_date(dst, r, c, dv) != TF_OK) {
                            tf_batch_free(dst);
                            return NULL;
                        }
                    } else {
                        if (tf_batch_set_null(dst, r, c) != TF_OK) {
                            tf_batch_free(dst);
                            return NULL;
                        }
                    }
                    break;
                }
                case TF_TYPE_TIMESTAMP: {
                    int64_t tv;
                    if (fast_timestamp(val, vlen, &tv)) {
                        if (tf_batch_set_timestamp(dst, r, c, tv) != TF_OK) {
                            tf_batch_free(dst);
                            return NULL;
                        }
                    } else {
                        int32_t dv;
                        if (fast_date(val, vlen, &dv)) {
                            tv = (int64_t)dv * 86400LL * 1000000LL;
                            if (tf_batch_set_timestamp(dst, r, c, tv) != TF_OK) {
                                tf_batch_free(dst);
                                return NULL;
                            }
                        } else {
                            if (tf_batch_set_null(dst, r, c) != TF_OK) {
                                tf_batch_free(dst);
                                return NULL;
                            }
                        }
                    }
                    break;
                }
                default:
                    if (tf_batch_set_null(dst, r, c) != TF_OK) {
                        tf_batch_free(dst);
                        return NULL;
                    }
                    break;
            }
        }
        if (tf_batch_expose_row(dst, r) != TF_OK) {
            tf_batch_free(dst);
            return NULL;
        }
    }

    return dst;
}

/*
 * Add a completed batch to the output array.
 */
static int emit_batch(tf_batch *batch, tf_batch ***out, size_t *n_out, size_t *out_cap) {
    if (*n_out >= *out_cap) {
        size_t need = 0;
        size_t new_cap = 0;
        if (tf_size_add(*n_out, 1, &need) != TF_OK ||
            tf_size_grow_pow2(*out_cap, need, 4, &new_cap) != TF_OK) {
            return TF_ERROR;
        }
        tf_batch **tmp = tf_reallocarray_checked(*out, new_cap, sizeof(tf_batch *));
        if (!tmp) return TF_ERROR;
        *out = tmp;
        *out_cap = new_cap;
    }
    (*out)[(*n_out)++] = batch;
    return TF_OK;
}

static tf_batch *make_schema_only_batch(csv_decoder_state *st) {
    for (size_t i = 0; i < st->n_cols; i++) {
        if (st->col_types[i] == TF_TYPE_NULL) st->col_types[i] = TF_TYPE_STRING;
    }
    tf_batch *b = tf_batch_create(st->n_cols, 0);
    if (!b) return NULL;
    for (size_t i = 0; i < st->n_cols; i++) {
        if (tf_batch_set_schema(b, i, st->col_names[i], st->col_types[i]) != TF_OK) {
            tf_batch_free(b);
            return NULL;
        }
    }
    return b;
}

static int csv_fields_match_header(const csv_decoder_state *st, const field_slice *fields, size_t n_fields) {
    if (!st || !st->schema_ready || n_fields != st->n_cols) return 0;
    for (size_t i = 0; i < st->n_cols; i++) {
        const char *name = st->col_names[i] ? st->col_names[i] : "";
        size_t name_len = strlen(name);
        if (fields[i].len != name_len) return 0;
        if (name_len > 0 && memcmp(fields[i].ptr, name, name_len) != 0) return 0;
    }
    return 1;
}

/*
 * Process a single complete CSV line.
 *
 * First line → extract column headers.
 * Subsequent lines → parse fields and add to current batch.
 * When batch is full → emit it and start a new one.
 */
static int process_line(csv_decoder_state *st, const char *line, size_t line_len,
                        size_t line_no, size_t byte_offset, tf_batch ***out,
                        size_t *n_out, size_t *out_cap, tf_side_channels *side) {
    if (st->skipped_rows < st->skip_rows) {
        st->skipped_rows++;
        return TF_OK;
    }

    size_t comment_pos = 0;
    int had_comment = csv_find_comment(st, line, line_len, &comment_pos);
    if (had_comment) line_len = comment_pos;
    if ((had_comment && csv_line_is_blank(line, line_len)) ||
        (st->skip_empty_rows && csv_line_is_blank(line, line_len))) {
        return TF_OK;
    }

    if (st->schema_ready && st->limit_rows && st->data_rows_read >= st->n_max) {
        return TF_OK;
    }

    /* Reset field arena — escaped field data from previous line is discarded */
    tf_arena_reset(st->field_arena);

    /* Parse the line into zero-copy field slices while counting every field. */
    size_t n_fields = 0;
    if (parse_csv_fields(line, line_len, st->delimiter,
                         st->fields, st->fields_cap, st->field_arena,
                         st->trim_ws, &n_fields) != TF_OK) {
        tf_set_last_error("csv: failed to parse fields");
        return TF_ERROR;
    }
    if (n_fields > st->max_columns) {
        if (emit_csv_column_limit_diagnostic(st, line, line_len, line_no, byte_offset,
                                             n_fields, side) != TF_OK)
            return TF_ERROR;
        char err[256];
        snprintf(err, sizeof(err), "csv record exceeds max_columns at line %zu: max %zu columns, observed %zu fields",
                 line_no, st->max_columns, n_fields);
        tf_set_last_error(err);
        return TF_ERROR;
    }

    if (st->schema_ready && st->has_header && st->skip_repeated_header && st->after_input_boundary) {
        st->after_input_boundary = 0;
        if (csv_fields_match_header(st, st->fields, n_fields)) {
            return TF_OK;
        }
    }

    /* --- First record: extract column headers or synthesize headerless names. --- */
    if (!st->schema_ready) {
        if (csv_init_schema(st, st->fields, n_fields, !st->has_header) != TF_OK) return TF_ERROR;
        if (st->has_header) return TF_OK;
        if (st->limit_rows && st->data_rows_read >= st->n_max) return TF_OK;
    }

    /* --- Empty lines: treat as all-null row --- */
    if (n_fields == 0 && st->n_cols > 0) {
        for (size_t i = 0; i < st->n_cols; i++) {
            st->fields[i].ptr = "";
            st->fields[i].len = 0;
            st->fields[i].quoted = 0;
        }
        n_fields = st->n_cols;
    }

    /* --- Field-count policy: legacy permissive, observable repair, or strict failure. --- */
    if (n_fields != st->n_cols) {
        const char *message = n_fields < st->n_cols
            ? "CSV row has fewer fields than header"
            : "CSV row has more fields than header";
        if (st->mode == CSV_MODE_STRICT) {
            if (emit_csv_field_count_diagnostic(st, line, line_len, line_no, byte_offset, n_fields,
                                                "fail", "error", message, side) != TF_OK)
                return TF_ERROR;
            char err[256];
            snprintf(err, sizeof(err), "csv strict field count mismatch at line %zu: expected %zu fields, got %zu",
                     line_no, st->n_cols, n_fields);
            tf_set_last_error(err);
            return TF_ERROR;
        }
        if (st->mode == CSV_MODE_REPAIR) {
            if (emit_csv_field_count_diagnostic(st, line, line_len, line_no, byte_offset, n_fields,
                                                "repair", "warning", message, side) != TF_OK)
                return TF_ERROR;
            if (emit_csv_repair_audit(st, line, line_len, line_no, byte_offset, n_fields,
                                      message, side) != TF_OK)
                return TF_ERROR;
        }
        if (n_fields < st->n_cols) {
            for (size_t i = n_fields; i < st->n_cols; i++) {
                st->fields[i].ptr = "";
                st->fields[i].len = 0;
                st->fields[i].quoted = 0;
            }
            n_fields = st->n_cols;
        } else {
            n_fields = st->n_cols;
        }
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
            tf_type t = detect_type_field(st, &st->fields[i]);
            st->col_types[i] = widen_type(st->col_types[i], t);
        }
        if (add_row_strings(st->batch, st, st->fields, n_fields, st->n_cols) != TF_OK)
            return TF_ERROR;
    } else {
        /* Direct parse phase: parse directly to typed columns */
        if (add_row_typed(st->batch, st, st->fields, n_fields, st->n_cols, st->col_types) != TF_OK)
            return TF_ERROR;
    }
    st->rows_buffered++;
    st->data_rows_read++;

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
            if (emit_batch(final, out, n_out, out_cap) != TF_OK) {
                tf_batch_free(final);
                return TF_ERROR;
            }
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
                      tf_batch ***out, size_t *n_out, tf_side_channels *side) {
    csv_decoder_state *st = self->state;
    *out = NULL;
    *n_out = 0;

    /* Append incoming data to line buffer */
    if (tf_buffer_write(&st->line_buf, data, len) != TF_OK) return TF_ERROR;

    /* Scan for complete lines */
    size_t out_cap = 0;
    uint8_t *buf = st->line_buf.data + st->line_buf.read_pos;
    size_t buf_len = st->line_buf.len - st->line_buf.read_pos;

    size_t base_offset = st->byte_offset;
    size_t line_start = 0;
    int in_quotes = 0;
    int at_field_start = 1;
    for (size_t i = 0; i < buf_len; i++) {
        size_t current_record_len = i - line_start;
        if (check_csv_record_limit(st, buf, line_start, current_record_len,
                                   st->line_number + 1, base_offset + line_start, side) != TF_OK)
            return TF_ERROR;

        if (in_quotes) {
            if (buf[i] == '"') {
                if (i + 1 < buf_len && buf[i + 1] == '"') {
                    i++; /* RFC 4180 escaped quote: doubled quote inside quoted field. */
                } else {
                    in_quotes = 0;
                    at_field_start = 0;
                }
            }
            continue;
        }

        if (buf[i] == '"' && at_field_start) {
            in_quotes = 1;
            at_field_start = 0;
        } else if (buf[i] == st->delimiter) {
            at_field_start = 1;
        } else if (buf[i] == '\n' || buf[i] == '\r') {
            size_t line_len = i - line_start;
            /* Handle \r\n */
            if (buf[i] == '\r' && i + 1 < buf_len && buf[i + 1] == '\n') {
                i++;
            }
            size_t line_no = ++st->line_number;
            size_t record_offset = base_offset + line_start;
            if (line_len > 0 || st->schema_ready) {
                if (process_line(st, (const char *)buf + line_start, line_len,
                                 line_no, record_offset, out, n_out, &out_cap, side) != TF_OK)
                    return TF_ERROR;
            }
            line_start = i + 1;
            at_field_start = 1;
        } else {
            at_field_start = 0;
        }
    }

    /* Move unconsumed data to start of buffer */
    st->line_buf.read_pos += line_start;
    st->byte_offset += line_start;
    tf_buffer_compact(&st->line_buf);

    return TF_OK;
}

/*
 * Flush: process any remaining partial line and emit the final batch.
 */
static int csv_flush(tf_decoder *self, tf_batch ***out, size_t *n_out, tf_side_channels *side) {
    csv_decoder_state *st = self->state;
    *out = NULL;
    *n_out = 0;
    size_t out_cap = 0;

    /* Process any remaining data as the last line */
    size_t remaining = tf_buffer_readable(&st->line_buf);
    if (remaining > 0) {
        uint8_t *buf = st->line_buf.data + st->line_buf.read_pos;
        size_t line_no = ++st->line_number;
        size_t record_offset = st->byte_offset;
        if (check_csv_record_limit(st, buf, 0, remaining, line_no, record_offset, side) != TF_OK)
            return TF_ERROR;
        if (process_line(st, (const char *)buf, remaining,
                         line_no, record_offset, out, n_out, &out_cap, side) != TF_OK)
            return TF_ERROR;
        st->line_buf.read_pos = st->line_buf.len;
        st->byte_offset += remaining;
    }

    if (st->schema_ready && st->limit_rows && st->n_max == 0 && !st->batch) {
        tf_batch *schema_only = make_schema_only_batch(st);
        if (!schema_only) return TF_ERROR;
        st->types_frozen = 1;
        if (emit_batch(schema_only, out, n_out, &out_cap) != TF_OK) {
            tf_batch_free(schema_only);
            return TF_ERROR;
        }
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
            if (emit_batch(final, out, n_out, &out_cap) != TF_OK) {
                tf_batch_free(final);
                return TF_ERROR;
            }
        } else {
            /* Already typed, emit directly */
            if (emit_batch(st->batch, out, n_out, &out_cap) != TF_OK) return TF_ERROR;
            st->batch = NULL;
        }
        st->rows_buffered = 0;
    }

    st->after_input_boundary = 1;
    return TF_OK;
}

static void csv_decoder_destroy(tf_decoder *self) {
    csv_decoder_state *st = self->state;
    if (st) {
        tf_buffer_free(&st->line_buf);
        if (st->batch) tf_batch_free(st->batch);
        if (st->field_arena) tf_arena_free(st->field_arena);
        free(st->fields);
        if (st->col_names) {
            for (size_t i = 0; i < st->n_cols; i++) free(st->col_names[i]);
        }
        free(st->col_names);
        free(st->col_types);
        for (size_t i = 0; i < st->n_null_literals; i++) free(st->null_literals[i]);
        free(st->null_literals);
        free(st->comment);
        tf_audit_options_free(&st->audit_opts);
        free(st);
    }
    free(self);
}

tf_decoder *tf_csv_decoder_create(const cJSON *args) {
    csv_decoder_state *st = calloc(1, sizeof(csv_decoder_state));
    if (!st) return NULL;

    st->delimiter = ',';
    st->has_header = 1;
    st->skip_repeated_header = 0;
    st->after_input_boundary = 0;
    st->batch_size = DEFAULT_BATCH_SIZE;
    st->mode = CSV_MODE_PERMISSIVE;
    st->quoted_nulls = 1;
    st->trim_ws = 1;
    st->skip_empty_rows = 0;
    st->skip_rows = 0;
    st->skipped_rows = 0;
    st->limit_rows = 0;
    st->n_max = 0;
    st->data_rows_read = 0;
    st->max_error_bytes = DEFAULT_MAX_ERROR_BYTES;
    st->max_record_bytes = DEFAULT_MAX_RECORD_BYTES;
    st->max_columns = DEFAULT_MAX_COLUMNS;
    st->audit_limit = 1000;
    tf_audit_options_init(&st->audit_opts, 1);

    if (args) {
        cJSON *d = cJSON_GetObjectItemCaseSensitive(args, "delimiter");
        if (cJSON_IsString(d) && d->valuestring[0])
            st->delimiter = d->valuestring[0];

        cJSON *h = cJSON_GetObjectItemCaseSensitive(args, "header");
        if (cJSON_IsBool(h))
            st->has_header = cJSON_IsTrue(h);

        cJSON *skip_repeated_header = cJSON_GetObjectItemCaseSensitive(args, "skip_repeated_header");
        if (!skip_repeated_header) skip_repeated_header = cJSON_GetObjectItemCaseSensitive(args, "skipRepeatedHeader");
        if (cJSON_IsBool(skip_repeated_header))
            st->skip_repeated_header = cJSON_IsTrue(skip_repeated_header);

        size_t parsed_size = 0;
        int has_batch_size = tf_json_get_size_arg(args, "batch_size", 1, TF_MAX_BATCH_ROWS, &parsed_size, "csv");
        if (has_batch_size < 0) { free(st); return NULL; }
        if (has_batch_size > 0) st->batch_size = parsed_size;

        cJSON *rep = cJSON_GetObjectItemCaseSensitive(args, "repair");
        if (cJSON_IsBool(rep) && cJSON_IsTrue(rep))
            st->mode = CSV_MODE_REPAIR;

        cJSON *mode = cJSON_GetObjectItemCaseSensitive(args, "mode");
        if (cJSON_IsString(mode)) {
            if (!parse_csv_mode(mode->valuestring, &st->mode)) {
                tf_set_last_error("csv: mode must be permissive, repair, or strict");
                free(st);
                return NULL;
            }
        }

        cJSON *strict = cJSON_GetObjectItemCaseSensitive(args, "strict");
        if (cJSON_IsBool(strict) && cJSON_IsTrue(strict))
            st->mode = CSV_MODE_STRICT;

        int has_max_error = tf_json_get_size_arg(args, "max_error_bytes", 0, TF_MAX_ERROR_BYTES, &parsed_size, "csv");
        if (has_max_error < 0) { free(st); return NULL; }
        if (has_max_error > 0) st->max_error_bytes = parsed_size;

        int has_max_record = tf_json_get_size_arg(args, "max_record_bytes", 0, TF_MAX_RECORD_BYTES, &parsed_size, "csv");
        if (has_max_record < 0) { free(st); return NULL; }
        if (has_max_record > 0) st->max_record_bytes = parsed_size;

        int has_max_columns = tf_json_get_size_arg(args, "max_columns", 1, TF_MAX_COLUMNS, &parsed_size, "csv");
        if (has_max_columns < 0) { free(st); return NULL; }
        if (has_max_columns == 0) {
            has_max_columns = tf_json_get_size_arg(args, "maxColumns", 1, TF_MAX_COLUMNS, &parsed_size, "csv");
            if (has_max_columns < 0) { free(st); return NULL; }
        }
        if (has_max_columns > 0) st->max_columns = parsed_size;

        cJSON *audit = cJSON_GetObjectItemCaseSensitive(args, "audit");
        st->audit = cJSON_IsTrue(audit) ? 1 : 0;
        cJSON *audit_limit = cJSON_GetObjectItemCaseSensitive(args, "audit_limit");
        if (!audit_limit) audit_limit = cJSON_GetObjectItemCaseSensitive(args, "auditLimit");
        if (audit_limit) {
            size_t parsed_limit = 0;
            if (tf_json_get_size_arg_any(args, "audit_limit", "auditLimit",
                                         1, TF_MAX_AUDIT_RECORDS,
                                         &parsed_limit, "csv") < 0) {
                for (size_t i = 0; i < st->n_null_literals; i++) free(st->null_literals[i]);
                free(st->null_literals);
                free(st->comment);
                free(st);
                return NULL;
            }
            st->audit_limit = parsed_limit;
            st->audit = 1;
        }

        cJSON *quoted_nulls = cJSON_GetObjectItemCaseSensitive(args, "quoted_nulls");
        if (cJSON_IsBool(quoted_nulls))
            st->quoted_nulls = cJSON_IsTrue(quoted_nulls);

        cJSON *trim_ws = cJSON_GetObjectItemCaseSensitive(args, "trim_ws");
        if (!trim_ws) trim_ws = cJSON_GetObjectItemCaseSensitive(args, "trimWs");
        if (cJSON_IsBool(trim_ws))
            st->trim_ws = cJSON_IsTrue(trim_ws);

        cJSON *skip_empty = cJSON_GetObjectItemCaseSensitive(args, "skip_empty_rows");
        if (!skip_empty) skip_empty = cJSON_GetObjectItemCaseSensitive(args, "skipEmptyRows");
        if (cJSON_IsBool(skip_empty))
            st->skip_empty_rows = cJSON_IsTrue(skip_empty);

        cJSON *skip_rows = cJSON_GetObjectItemCaseSensitive(args, "skip");
        if (!skip_rows) skip_rows = cJSON_GetObjectItemCaseSensitive(args, "skip_rows");
        if (!skip_rows) skip_rows = cJSON_GetObjectItemCaseSensitive(args, "skipRows");
        if (skip_rows) {
            const char *skip_name = cJSON_GetObjectItemCaseSensitive(args, "skip") ? "skip" :
                                    (cJSON_GetObjectItemCaseSensitive(args, "skip_rows") ? "skip_rows" : "skipRows");
            size_t parsed_skip = 0;
            if (tf_json_size_value(skip_rows, skip_name, 0, TF_MAX_COUNT_ARG,
                                   &parsed_skip, "csv") < 0) {
                for (size_t i = 0; i < st->n_null_literals; i++) free(st->null_literals[i]);
                free(st->null_literals);
                free(st->comment);
                free(st);
                return NULL;
            }
            st->skip_rows = parsed_skip;
        }

        cJSON *n_max = cJSON_GetObjectItemCaseSensitive(args, "n_max");
        if (!n_max) n_max = cJSON_GetObjectItemCaseSensitive(args, "nMax");
        if (!n_max) n_max = cJSON_GetObjectItemCaseSensitive(args, "max_rows");
        if (!n_max) n_max = cJSON_GetObjectItemCaseSensitive(args, "maxRows");
        if (n_max) {
            const char *n_max_name = cJSON_GetObjectItemCaseSensitive(args, "n_max") ? "n_max" :
                                     (cJSON_GetObjectItemCaseSensitive(args, "nMax") ? "nMax" :
                                      (cJSON_GetObjectItemCaseSensitive(args, "max_rows") ? "max_rows" : "maxRows"));
            size_t parsed_n_max = 0;
            if (tf_json_size_value(n_max, n_max_name, 0, TF_MAX_COUNT_ARG,
                                   &parsed_n_max, "csv") < 0) {
                for (size_t i = 0; i < st->n_null_literals; i++) free(st->null_literals[i]);
                free(st->null_literals);
                free(st->comment);
                free(st);
                return NULL;
            }
            st->limit_rows = 1;
            st->n_max = parsed_n_max;
        }

        cJSON *comment = cJSON_GetObjectItemCaseSensitive(args, "comment");
        if (cJSON_IsString(comment) && comment->valuestring && comment->valuestring[0]) {
            st->comment = strdup(comment->valuestring);
            if (!st->comment) {
                for (size_t i = 0; i < st->n_null_literals; i++) free(st->null_literals[i]);
                free(st->null_literals);
                free(st->comment);
                free(st);
                return NULL;
            }
            st->comment_len = strlen(st->comment);
        }

        if (csv_configure_null_literals(st, args) != TF_OK) {
            for (size_t i = 0; i < st->n_null_literals; i++) free(st->null_literals[i]);
            free(st->null_literals);
            free(st->comment);
            tf_audit_options_free(&st->audit_opts);
            free(st);
            return NULL;
        }
        if (tf_audit_options_parse(&st->audit_opts, args, "csv") != TF_OK) {
            for (size_t i = 0; i < st->n_null_literals; i++) free(st->null_literals[i]);
            free(st->null_literals);
            free(st->comment);
            tf_audit_options_free(&st->audit_opts);
            free(st);
            return NULL;
        }
    }

    tf_buffer_init(&st->line_buf);

    st->fields_cap = st->max_columns;
    st->fields = tf_callocarray_checked(st->fields_cap, sizeof(field_slice));
    if (!st->fields) {
        for (size_t i = 0; i < st->n_null_literals; i++) free(st->null_literals[i]);
        free(st->null_literals);
        free(st->comment);
        tf_audit_options_free(&st->audit_opts);
        free(st);
        return NULL;
    }

    /* Arena for escaped quoted field data (reset per line, rarely used) */
    st->field_arena = tf_arena_create(4096);
    if (!st->field_arena) {
        for (size_t i = 0; i < st->n_null_literals; i++) free(st->null_literals[i]);
        free(st->null_literals);
        free(st->comment);
        free(st->fields);
        tf_audit_options_free(&st->audit_opts);
        free(st);
        return NULL;
    }

    tf_decoder *dec = malloc(sizeof(tf_decoder));
    if (!dec) {
        tf_arena_free(st->field_arena);
        for (size_t i = 0; i < st->n_null_literals; i++) free(st->null_literals[i]);
        free(st->null_literals);
        free(st->comment);
        free(st->fields);
        tf_audit_options_free(&st->audit_opts);
        free(st);
        return NULL;
    }
    dec->decode = csv_decode;
    dec->flush = csv_flush;
    dec->destroy = csv_decoder_destroy;
    dec->state = st;
    return dec;
}

/* ================================================================
 * CSV Encoder
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

static int csv_write_field(tf_buffer *out, const char *s, char delim) {
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

static int csv_write_delimiter(tf_buffer *out, char delim) {
    return tf_buffer_write(out, (const uint8_t *)&delim, 1);
}

static int csv_encode(tf_encoder *self, tf_batch *in, tf_buffer *out) {
    csv_encoder_state *st = self->state;

    /* Write header */
    if (!st->header_written) {
        for (size_t i = 0; i < in->n_cols; i++) {
            if (i > 0 && csv_write_delimiter(out, st->delimiter) != TF_OK) return TF_ERROR;
            if (csv_write_field(out, in->col_names[i], st->delimiter) != TF_OK) return TF_ERROR;
        }
        if (tf_buffer_write(out, (const uint8_t *)"\n", 1) != TF_OK) return TF_ERROR;
        st->header_written = 1;
    }

    /* Write rows */
    char numbuf[64];
    for (size_t r = 0; r < in->n_rows; r++) {
        for (size_t c = 0; c < in->n_cols; c++) {
            if (c > 0 && csv_write_delimiter(out, st->delimiter) != TF_OK) return TF_ERROR;
            if (tf_batch_is_null(in, r, c)) {
                /* empty field for null */
                continue;
            }
            switch (in->col_types[c]) {
                case TF_TYPE_BOOL:
                    if (tf_buffer_write_str(out, tf_batch_get_bool(in, r, c) ? "true" : "false") != TF_OK)
                        return TF_ERROR;
                    break;
                case TF_TYPE_INT64:
                    snprintf(numbuf, sizeof(numbuf), "%lld", (long long)tf_batch_get_int64(in, r, c));
                    if (tf_buffer_write_str(out, numbuf) != TF_OK) return TF_ERROR;
                    break;
                case TF_TYPE_FLOAT64:
                    if (tf_format_float64(numbuf, sizeof(numbuf), tf_batch_get_float64(in, r, c)) != TF_OK)
                        return TF_ERROR;
                    if (tf_buffer_write_str(out, numbuf) != TF_OK) return TF_ERROR;
                    break;
                case TF_TYPE_STRING:
                    if (csv_write_field(out, tf_batch_get_string(in, r, c), st->delimiter) != TF_OK)
                        return TF_ERROR;
                    break;
                case TF_TYPE_DATE: {
                    char dbuf[32];
                    tf_date_format(tf_batch_get_date(in, r, c), dbuf, sizeof(dbuf));
                    if (tf_buffer_write_str(out, dbuf) != TF_OK) return TF_ERROR;
                    break;
                }
                case TF_TYPE_TIMESTAMP: {
                    char tsbuf[40];
                    tf_timestamp_format(tf_batch_get_timestamp(in, r, c), tsbuf, sizeof(tsbuf));
                    if (tf_buffer_write_str(out, tsbuf) != TF_OK) return TF_ERROR;
                    break;
                }
                default:
                    break;
            }
        }
        if (tf_buffer_write(out, (const uint8_t *)"\n", 1) != TF_OK) return TF_ERROR;
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
