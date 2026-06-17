/*
 * codec_jsonl.c — JSON Lines streaming decoder and encoder.
 *
 * Decoder: splits on newlines, parses each line as JSON object using cJSON.
 *   - First line establishes schema (column names and types).
 *   - Subsequent lines match schema; missing keys → null.
 *   - Emits batch at batch_size.
 *
 * Encoder: batch → one JSON object per row, newline-separated.
 */

#include "internal.h"
#include "date_utils.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define DEFAULT_BATCH_SIZE 1024
#define DEFAULT_MAX_ERROR_BYTES 4096
#define DEFAULT_MAX_RECORD_BYTES (64 * 1024 * 1024)

/* ================================================================
 * JSONL Decoder
 * ================================================================ */

typedef enum {
    JSONL_ERROR_SKIP = 0,
    JSONL_ERROR_FAIL,
    JSONL_ERROR_WARN,
    JSONL_ERROR_QUARANTINE,
} jsonl_error_action;

static const char *jsonl_error_action_name(jsonl_error_action action) {
    switch (action) {
        case JSONL_ERROR_FAIL: return "fail";
        case JSONL_ERROR_WARN: return "warn";
        case JSONL_ERROR_QUARANTINE: return "quarantine";
        case JSONL_ERROR_SKIP:
        default: return "skip";
    }
}

static int parse_jsonl_error_action(const char *s, jsonl_error_action *out) {
    if (!s || strcmp(s, "skip") == 0 || strcmp(s, "ignore") == 0) {
        *out = JSONL_ERROR_SKIP;
        return 1;
    }
    if (strcmp(s, "fail") == 0 || strcmp(s, "error") == 0 || strcmp(s, "stop") == 0) {
        *out = JSONL_ERROR_FAIL;
        return 1;
    }
    if (strcmp(s, "warn") == 0 || strcmp(s, "warning") == 0) {
        *out = JSONL_ERROR_WARN;
        return 1;
    }
    if (strcmp(s, "quarantine") == 0 || strcmp(s, "dead-letter") == 0 || strcmp(s, "dead_letter") == 0) {
        *out = JSONL_ERROR_QUARANTINE;
        return 1;
    }
    return 0;
}

typedef struct {
    size_t    batch_size;
    jsonl_error_action on_error;
    size_t    max_error_bytes;
    size_t    max_record_bytes;
    size_t    line_number;
    size_t    byte_offset;
    size_t    bad_records;

    /* Line accumulator */
    tf_buffer line_buf;

    /* Schema (discovered from first object) */
    char    **col_names;
    tf_type  *col_types;
    size_t    n_cols;
    int       schema_ready;

    /* Current batch */
    tf_batch *batch;
    size_t    rows_buffered;
} jsonl_decoder_state;

static tf_type json_to_type(const cJSON *val) {
    if (cJSON_IsNumber(val)) {
        /* Check if integer */
        double d = val->valuedouble;
        if (d == (double)(int64_t)d && d >= -9007199254740992.0 && d <= 9007199254740992.0)
            return TF_TYPE_INT64;
        return TF_TYPE_FLOAT64;
    }
    if (cJSON_IsString(val)) return TF_TYPE_STRING;
    if (cJSON_IsBool(val)) return TF_TYPE_BOOL;
    if (cJSON_IsArray(val) || cJSON_IsObject(val)) return TF_TYPE_STRING;
    return TF_TYPE_NULL;
}

static tf_type widen_type(tf_type current, tf_type incoming) {
    if (current == incoming) return current;
    if (current == TF_TYPE_NULL) return incoming;
    if (incoming == TF_TYPE_NULL) return current;
    if (current == TF_TYPE_INT64 && incoming == TF_TYPE_FLOAT64) return TF_TYPE_FLOAT64;
    if (current == TF_TYPE_FLOAT64 && incoming == TF_TYPE_INT64) return TF_TYPE_FLOAT64;
    return TF_TYPE_STRING;
}

static size_t jsonl_type_size(tf_type t) {
    switch (t) {
        case TF_TYPE_BOOL: return sizeof(uint8_t);
        case TF_TYPE_INT64: return sizeof(int64_t);
        case TF_TYPE_FLOAT64: return sizeof(double);
        case TF_TYPE_STRING: return sizeof(char *);
        case TF_TYPE_DATE: return sizeof(int32_t);
        case TF_TYPE_TIMESTAMP: return sizeof(int64_t);
        default: return 0;
    }
}

static void jsonl_free_schema_arrays(char **col_names, tf_type *col_types, size_t n_names) {
    if (col_names) {
        for (size_t i = 0; i < n_names; i++) free(col_names[i]);
    }
    free(col_names);
    free(col_types);
}

static int jsonl_convert_batch_column(tf_batch *batch, size_t col, tf_type new_type) {
    if (!batch || col >= batch->n_cols || batch->col_types[col] == new_type) return TF_OK;

    tf_type old_type = batch->col_types[col];
    size_t sz = jsonl_type_size(new_type);
    void *new_col = NULL;
    uint8_t *new_nulls = NULL;
    if (sz == 0) {
        batch->col_types[col] = new_type;
        batch->columns[col] = NULL;
        batch->nulls[col] = NULL;
        return TF_OK;
    }

    size_t column_bytes = 0;
    if (tf_size_mul(sz, batch->capacity, &column_bytes) != TF_OK) return TF_ERROR;
    new_col = tf_arena_alloc(batch->arena, column_bytes);
    new_nulls = tf_arena_alloc(batch->arena, batch->capacity);
    if (!new_col || !new_nulls) return TF_ERROR;
    memset(new_nulls, 1, batch->capacity);

    for (size_t r = 0; r < batch->n_rows; r++) {
        int was_null = (!batch->nulls[col] || batch->nulls[col][r] != 0);
        if (was_null) continue;

        if (new_type == TF_TYPE_FLOAT64 && old_type == TF_TYPE_INT64) {
            ((double *)new_col)[r] = (double)((int64_t *)batch->columns[col])[r];
            new_nulls[r] = 0;
        } else if (new_type == TF_TYPE_FLOAT64 && old_type == TF_TYPE_FLOAT64) {
            ((double *)new_col)[r] = ((double *)batch->columns[col])[r];
            new_nulls[r] = 0;
        } else if (new_type == TF_TYPE_STRING) {
            char numbuf[64];
            const char *s = NULL;
            switch (old_type) {
                case TF_TYPE_BOOL:
                    s = ((uint8_t *)batch->columns[col])[r] ? "true" : "false";
                    break;
                case TF_TYPE_INT64:
                    snprintf(numbuf, sizeof(numbuf), "%lld",
                             (long long)((int64_t *)batch->columns[col])[r]);
                    s = numbuf;
                    break;
                case TF_TYPE_FLOAT64:
                    if (tf_format_float64(numbuf, sizeof(numbuf),
                                          ((double *)batch->columns[col])[r]) != TF_OK)
                        return TF_ERROR;
                    s = numbuf;
                    break;
                case TF_TYPE_STRING:
                    s = ((char **)batch->columns[col])[r];
                    break;
                default:
                    s = "";
                    break;
            }
            size_t len = 0, bytes = 0;
            const char *safe_s = s ? s : "";
            if (tf_string_length_bounded(safe_s, TF_MAX_CELL_BYTES,
                                         &len, "jsonl", "cell") != TF_OK ||
                tf_size_add(len, 1, &bytes) != TF_OK)
                return TF_ERROR;
            char *copy = tf_arena_alloc(batch->arena, bytes);
            if (!copy) return TF_ERROR;
            memcpy(copy, safe_s, len);
            copy[len] = '\0';
            ((char **)new_col)[r] = copy;
            new_nulls[r] = 0;
        }
    }

    batch->col_types[col] = new_type;
    batch->columns[col] = new_col;
    batch->nulls[col] = new_nulls;
    return TF_OK;
}

static int jsonl_widen_column(jsonl_decoder_state *st, size_t col, tf_type incoming) {
    tf_type widened = widen_type(st->col_types[col], incoming);
    if (widened == st->col_types[col]) return TF_OK;
    if (jsonl_convert_batch_column(st->batch, col, widened) != TF_OK) return TF_ERROR;
    st->col_types[col] = widened;
    return TF_OK;
}

static int emit_jsonl_malformed(jsonl_decoder_state *st, const char *line, size_t len,
                                size_t line_no, const char *reason,
                                tf_side_channels *side) {
    st->bad_records++;
    if (!side || !side->errors) return TF_OK;

    size_t keep = len;
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
    cJSON_AddStringToObject(obj, "type", "jsonl_malformed");
    cJSON_AddStringToObject(obj, "op", "codec.jsonl.decode");
    cJSON_AddStringToObject(obj, "action", jsonl_error_action_name(st->on_error));
    cJSON_AddStringToObject(obj, "severity", st->on_error == JSONL_ERROR_WARN ? "warning" : "error");
    cJSON_AddNumberToObject(obj, "line", (double)line_no);
    cJSON_AddStringToObject(obj, "message", reason ? reason : "invalid JSONL record");
    cJSON_AddNumberToObject(obj, "raw_bytes", (double)len);
    cJSON_AddStringToObject(obj, "raw", raw);
    if (truncated) cJSON_AddBoolToObject(obj, "truncated", 1);

    char *printed = cJSON_PrintUnformatted(obj);
    cJSON_Delete(obj);
    free(raw);
    if (!printed) return TF_ERROR;
    int rc = tf_buffer_write_line(side->errors, printed);
    free(printed);
    return rc;
}

static int handle_jsonl_malformed(jsonl_decoder_state *st, const char *line, size_t len,
                                  size_t line_no, const char *reason,
                                  tf_side_channels *side) {
    if (st->on_error == JSONL_ERROR_SKIP) {
        st->bad_records++;
        return TF_OK;
    }
    if (emit_jsonl_malformed(st, line, len, line_no, reason, side) != TF_OK)
        return TF_ERROR;
    if (st->on_error == JSONL_ERROR_FAIL) {
        char msg[256];
        snprintf(msg, sizeof(msg), "jsonl decode failed at line %zu: %s",
                 line_no, reason ? reason : "invalid JSONL record");
        tf_set_last_error(msg);
        return TF_ERROR;
    }
    return TF_OK;
}

static char *jsonl_record_preview(jsonl_decoder_state *st,
                                  const uint8_t *prefix, size_t prefix_len,
                                  const uint8_t *suffix, size_t suffix_len,
                                  int *truncated_out) {
    size_t total = prefix_len + suffix_len;
    size_t keep = total;
    int truncated = 0;
    if (st->max_error_bytes > 0 && keep > st->max_error_bytes) {
        keep = st->max_error_bytes;
        truncated = 1;
    }
    char *raw = malloc(keep + 1);
    if (!raw) return NULL;
    size_t copied = 0;
    size_t n = prefix_len < keep ? prefix_len : keep;
    if (n > 0 && prefix) {
        memcpy(raw, prefix, n);
        copied += n;
    }
    if (copied < keep && suffix) {
        size_t m = keep - copied;
        if (m > suffix_len) m = suffix_len;
        if (m > 0) {
            memcpy(raw + copied, suffix, m);
            copied += m;
        }
    }
    raw[copied] = '\0';
    if (truncated_out) *truncated_out = truncated;
    return raw;
}

static int emit_jsonl_record_size_diagnostic(jsonl_decoder_state *st,
                                             const uint8_t *prefix, size_t prefix_len,
                                             const uint8_t *suffix, size_t suffix_len,
                                             size_t line_no, size_t byte_offset,
                                             size_t observed_len,
                                             tf_side_channels *side) {
    if (!side || !side->errors) return TF_OK;
    int truncated = 0;
    char *raw = jsonl_record_preview(st, prefix, prefix_len, suffix, suffix_len, &truncated);
    if (!raw) return TF_ERROR;

    cJSON *obj = cJSON_CreateObject();
    if (!obj) { free(raw); return TF_ERROR; }
    cJSON_AddStringToObject(obj, "type", "jsonl_record_too_large");
    cJSON_AddStringToObject(obj, "op", "codec.jsonl.decode");
    cJSON_AddStringToObject(obj, "action", "fail");
    cJSON_AddStringToObject(obj, "severity", "error");
    cJSON_AddNumberToObject(obj, "line", (double)line_no);
    cJSON_AddNumberToObject(obj, "byte_offset", (double)byte_offset);
    cJSON_AddNumberToObject(obj, "max_record_bytes", (double)st->max_record_bytes);
    cJSON_AddNumberToObject(obj, "observed_bytes", (double)observed_len);
    cJSON_AddStringToObject(obj, "message", "JSONL record exceeds max_record_bytes");
    cJSON_AddNumberToObject(obj, "raw_bytes", (double)observed_len);
    cJSON_AddStringToObject(obj, "raw", raw);
    if (truncated) cJSON_AddBoolToObject(obj, "truncated", 1);

    char *printed = cJSON_PrintUnformatted(obj);
    cJSON_Delete(obj);
    free(raw);
    if (!printed) return TF_ERROR;
    int rc = tf_buffer_write_line(side->errors, printed);
    free(printed);
    return rc;
}

static int fail_jsonl_record_limit(jsonl_decoder_state *st,
                                   const uint8_t *prefix, size_t prefix_len,
                                   const uint8_t *suffix, size_t suffix_len,
                                   size_t line_no, size_t byte_offset,
                                   size_t observed_len,
                                   tf_side_channels *side) {
    if (emit_jsonl_record_size_diagnostic(st, prefix, prefix_len, suffix, suffix_len,
                                          line_no, byte_offset, observed_len, side) != TF_OK)
        return TF_ERROR;
    char err[256];
    snprintf(err, sizeof(err),
             "jsonl record exceeds max_record_bytes at line %zu: max %zu bytes, observed %zu bytes",
             line_no, st->max_record_bytes, observed_len);
    tf_set_last_error(err);
    return TF_ERROR;
}

static int check_jsonl_incoming_record_limit(jsonl_decoder_state *st,
                                             const uint8_t *data, size_t len,
                                             tf_side_channels *side) {
    if (st->max_record_bytes == 0 || len == 0) return TF_OK;

    size_t prefix_len = tf_buffer_readable(&st->line_buf);
    const uint8_t *prefix = prefix_len > 0 ? st->line_buf.data + st->line_buf.read_pos : NULL;
    size_t incoming_base = st->byte_offset + prefix_len;
    size_t line_no = st->line_number + 1;
    size_t line_offset = st->byte_offset;
    size_t segment_start = 0;
    size_t current_len = prefix_len;

    for (size_t i = 0; i < len; i++) {
        if (data[i] == '\n' || data[i] == '\r') {
            if (data[i] == '\r' && i + 1 < len && data[i + 1] == '\n') i++;
            prefix = NULL;
            prefix_len = 0;
            current_len = 0;
            segment_start = i + 1;
            line_offset = incoming_base + i + 1;
            line_no++;
            continue;
        }
        current_len++;
        if (current_len > st->max_record_bytes) {
            size_t suffix_len = i - segment_start + 1;
            return fail_jsonl_record_limit(st, prefix, prefix_len,
                                           data + segment_start, suffix_len,
                                           line_no, line_offset, current_len, side);
        }
    }
    return TF_OK;
}

static int check_jsonl_buffer_record_limit(jsonl_decoder_state *st, size_t observed_len,
                                           size_t line_no, size_t byte_offset,
                                           tf_side_channels *side) {
    if (st->max_record_bytes == 0 || observed_len <= st->max_record_bytes) return TF_OK;
    const uint8_t *buf = st->line_buf.data + st->line_buf.read_pos;
    return fail_jsonl_record_limit(st, buf, observed_len, NULL, 0,
                                   line_no, byte_offset, observed_len, side);
}

static int emit_jsonl_batch(tf_batch *batch, tf_batch ***out, size_t *n_out, size_t *out_cap) {
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

static tf_batch *make_jsonl_batch(jsonl_decoder_state *st) {
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

static int add_json_row(jsonl_decoder_state *st, cJSON *obj) {
    if (!st->batch) {
        st->batch = make_jsonl_batch(st);
        if (!st->batch) return TF_ERROR;
    }

    size_t row = st->batch->n_rows;
    size_t need_rows = 0;
    if (tf_size_add(row, 1, &need_rows) != TF_OK ||
        tf_batch_ensure_capacity(st->batch, need_rows) != TF_OK)
        return TF_ERROR;

    for (size_t c = 0; c < st->n_cols; c++) {
        cJSON *val = cJSON_GetObjectItemCaseSensitive(obj, st->col_names[c]);
        int rc = TF_OK;
        if (!val || cJSON_IsNull(val)) {
            rc = tf_batch_set_null(st->batch, row, c);
        } else {
            switch (st->col_types[c]) {
                case TF_TYPE_BOOL:
                    rc = tf_batch_set_bool(st->batch, row, c, cJSON_IsTrue(val));
                    break;
                case TF_TYPE_INT64:
                    rc = tf_batch_set_int64(st->batch, row, c, (int64_t)val->valuedouble);
                    break;
                case TF_TYPE_FLOAT64:
                    rc = tf_batch_set_float64(st->batch, row, c, val->valuedouble);
                    break;
                case TF_TYPE_STRING:
                    if (cJSON_IsString(val)) {
                        rc = tf_batch_set_string(st->batch, row, c, val->valuestring ? val->valuestring : "");
                    } else {
                        char *printed = cJSON_PrintUnformatted(val);
                        if (!printed) return TF_ERROR;
                        rc = tf_batch_set_string(st->batch, row, c, printed);
                        free(printed);
                    }
                    break;
                default:
                    rc = tf_batch_set_null(st->batch, row, c);
                    break;
            }
        }
        if (rc != TF_OK) return TF_ERROR;
    }
    if (tf_batch_expose_row(st->batch, row) != TF_OK) return TF_ERROR;
    st->rows_buffered++;
    return TF_OK;
}

static int process_jsonl_line(jsonl_decoder_state *st, const char *line, size_t len,
                              size_t line_no, tf_batch ***out, size_t *n_out,
                              size_t *out_cap, tf_side_channels *side) {
    /* Skip empty lines */
    if (len == 0) return TF_OK;

    /* Parse JSON */
    cJSON *obj = cJSON_ParseWithLength(line, len);
    if (!obj || !cJSON_IsObject(obj)) {
        const char *reason = obj ? "JSONL record is not an object" : "invalid JSON";
        cJSON_Delete(obj);
        return handle_jsonl_malformed(st, line, len, line_no, reason, side);
    }

    if (!st->schema_ready) {
        /* Discover schema from first object */
        int n = cJSON_GetArraySize(obj);
        if (n < 0) { cJSON_Delete(obj); return TF_ERROR; }
        size_t n_cols = (size_t)n;
        if (n_cols > TF_MAX_COLUMNS) {
            char err[256];
            snprintf(err, sizeof(err),
                     "jsonl schema exceeds max_columns: max %zu columns, observed %zu",
                     TF_MAX_COLUMNS, n_cols);
            tf_set_last_error(err);
            cJSON_Delete(obj);
            return TF_ERROR;
        }
        char **col_names = tf_callocarray_checked(n_cols ? n_cols : 1, sizeof(char *));
        tf_type *col_types = tf_callocarray_checked(n_cols ? n_cols : 1, sizeof(tf_type));
        if (!col_names || !col_types) {
            free(col_names);
            free(col_types);
            cJSON_Delete(obj);
            return TF_ERROR;
        }

        size_t i = 0;
        size_t schema_bytes = 0;
        cJSON *item;
        cJSON_ArrayForEach(item, obj) {
            const char *raw_name = item->string ? item->string : "";
            size_t name_len = 0, name_bytes = 0, next_schema_bytes = 0;
            if (tf_string_length_bounded(raw_name, TF_MAX_COLUMN_NAME_BYTES,
                                         &name_len, "jsonl", "column name") != TF_OK ||
                tf_size_add(name_len, 1, &name_bytes) != TF_OK ||
                tf_size_add(schema_bytes, name_bytes, &next_schema_bytes) != TF_OK ||
                tf_check_byte_limit(next_schema_bytes, TF_MAX_SCHEMA_BYTES,
                                    "jsonl", "schema") != TF_OK) {
                jsonl_free_schema_arrays(col_names, col_types, i);
                cJSON_Delete(obj);
                return TF_ERROR;
            }
            col_names[i] = malloc(name_bytes);
            if (!col_names[i]) {
                jsonl_free_schema_arrays(col_names, col_types, i);
                cJSON_Delete(obj);
                return TF_ERROR;
            }
            memcpy(col_names[i], raw_name, name_len);
            col_names[i][name_len] = '\0';
            col_types[i] = json_to_type(item);
            schema_bytes = next_schema_bytes;
            i++;
        }
        st->n_cols = n_cols;
        st->col_names = col_names;
        st->col_types = col_types;
        st->schema_ready = 1;
    } else {
        /* Update types from new row */
        cJSON *item;
        cJSON_ArrayForEach(item, obj) {
            if (!item->string) continue;
            for (size_t c = 0; c < st->n_cols; c++) {
                if (strcmp(st->col_names[c], item->string) == 0) {
                    if (jsonl_widen_column(st, c, json_to_type(item)) != TF_OK) {
                        cJSON_Delete(obj);
                        return TF_ERROR;
                    }
                    break;
                }
            }
        }
    }

    int rc = add_json_row(st, obj);
    cJSON_Delete(obj);
    if (rc != TF_OK) return rc;

    /* Emit batch if full */
    if (st->rows_buffered >= st->batch_size) {
        if (emit_jsonl_batch(st->batch, out, n_out, out_cap) != TF_OK) return TF_ERROR;
        st->batch = NULL;
        st->rows_buffered = 0;
    }

    return TF_OK;
}

static int jsonl_decode(tf_decoder *self, const uint8_t *data, size_t len,
                        tf_batch ***out, size_t *n_out, tf_side_channels *side) {
    jsonl_decoder_state *st = self->state;
    *out = NULL;
    *n_out = 0;

    if (check_jsonl_incoming_record_limit(st, data, len, side) != TF_OK) return TF_ERROR;
    if (tf_buffer_write(&st->line_buf, data, len) != TF_OK) return TF_ERROR;

    size_t out_cap = 0;
    uint8_t *buf = st->line_buf.data + st->line_buf.read_pos;
    size_t buf_len = st->line_buf.len - st->line_buf.read_pos;

    size_t line_start = 0;
    for (size_t i = 0; i < buf_len; i++) {
        if (buf[i] == '\n' || buf[i] == '\r') {
            size_t line_len = i - line_start;
            size_t line_no = ++st->line_number;
            if (buf[i] == '\r' && i + 1 < buf_len && buf[i + 1] == '\n') i++;
            if (line_len > 0) {
                if (process_jsonl_line(st, (const char *)buf + line_start, line_len,
                                       line_no, out, n_out, &out_cap, side) != TF_OK)
                    return TF_ERROR;
            }
            line_start = i + 1;
        }
    }

    st->line_buf.read_pos += line_start;
    st->byte_offset += line_start;
    tf_buffer_compact(&st->line_buf);
    return TF_OK;
}

static int jsonl_flush(tf_decoder *self, tf_batch ***out, size_t *n_out, tf_side_channels *side) {
    jsonl_decoder_state *st = self->state;
    *out = NULL;
    *n_out = 0;
    size_t out_cap = 0;

    /* Process remaining data */
    size_t remaining = tf_buffer_readable(&st->line_buf);
    if (remaining > 0) {
        uint8_t *buf = st->line_buf.data + st->line_buf.read_pos;
        size_t line_no = ++st->line_number;
        size_t record_offset = st->byte_offset;
        if (check_jsonl_buffer_record_limit(st, remaining, line_no, record_offset, side) != TF_OK)
            return TF_ERROR;
        if (process_jsonl_line(st, (const char *)buf, remaining, line_no,
                               out, n_out, &out_cap, side) != TF_OK)
            return TF_ERROR;
        st->line_buf.read_pos = st->line_buf.len;
        st->byte_offset += remaining;
    }

    /* Emit remaining batch */
    if (st->batch && st->rows_buffered > 0) {
        if (emit_jsonl_batch(st->batch, out, n_out, &out_cap) != TF_OK) return TF_ERROR;
        st->batch = NULL;
        st->rows_buffered = 0;
    }

    return TF_OK;
}

static void jsonl_decoder_destroy(tf_decoder *self) {
    jsonl_decoder_state *st = self->state;
    if (st) {
        tf_buffer_free(&st->line_buf);
        if (st->batch) tf_batch_free(st->batch);
        if (st->col_names) {
            for (size_t i = 0; i < st->n_cols; i++) free(st->col_names[i]);
        }
        free(st->col_names);
        free(st->col_types);
        free(st);
    }
    free(self);
}

tf_decoder *tf_jsonl_decoder_create(const cJSON *args) {
    jsonl_decoder_state *st = calloc(1, sizeof(jsonl_decoder_state));
    if (!st) return NULL;

    st->batch_size = DEFAULT_BATCH_SIZE;
    st->on_error = JSONL_ERROR_SKIP;
    st->max_error_bytes = DEFAULT_MAX_ERROR_BYTES;
    st->max_record_bytes = DEFAULT_MAX_RECORD_BYTES;
    if (args) {
        size_t parsed_size = 0;
        int has_batch_size = tf_json_get_size_arg(args, "batch_size", 1, TF_MAX_BATCH_ROWS, &parsed_size, "jsonl");
        if (has_batch_size < 0) { free(st); return NULL; }
        if (has_batch_size > 0) st->batch_size = parsed_size;
        cJSON *on_error = cJSON_GetObjectItemCaseSensitive(args, "on_error");
        if (cJSON_IsString(on_error)) {
            if (!parse_jsonl_error_action(on_error->valuestring, &st->on_error)) {
                tf_set_last_error("jsonl: on_error must be skip, fail, warn, or quarantine");
                free(st);
                return NULL;
            }
        }
        int has_max_error = tf_json_get_size_arg(args, "max_error_bytes", 0, TF_MAX_ERROR_BYTES, &parsed_size, "jsonl");
        if (has_max_error < 0) { free(st); return NULL; }
        if (has_max_error > 0) st->max_error_bytes = parsed_size;
        int has_max_record = tf_json_get_size_arg(args, "max_record_bytes", 0, TF_MAX_RECORD_BYTES, &parsed_size, "jsonl");
        if (has_max_record < 0) { free(st); return NULL; }
        if (has_max_record > 0) st->max_record_bytes = parsed_size;
    }

    tf_buffer_init(&st->line_buf);

    tf_decoder *dec = malloc(sizeof(tf_decoder));
    if (!dec) { free(st); return NULL; }
    dec->decode = jsonl_decode;
    dec->flush = jsonl_flush;
    dec->destroy = jsonl_decoder_destroy;
    dec->state = st;
    return dec;
}

/* ================================================================
 * JSONL Encoder
 * ================================================================ */

static int jsonl_write_escaped_string(tf_buffer *out, const char *s) {
    if (tf_buffer_write(out, (const uint8_t *)"\"", 1) != TF_OK) return TF_ERROR;
    for (const char *p = s ? s : ""; *p; p++) {
        int rc = TF_OK;
        switch (*p) {
            case '"':  rc = tf_buffer_write_str(out, "\\\""); break;
            case '\\': rc = tf_buffer_write_str(out, "\\\\"); break;
            case '\n': rc = tf_buffer_write_str(out, "\\n"); break;
            case '\r': rc = tf_buffer_write_str(out, "\\r"); break;
            case '\t': rc = tf_buffer_write_str(out, "\\t"); break;
            default:   rc = tf_buffer_write(out, (const uint8_t *)p, 1); break;
        }
        if (rc != TF_OK) return TF_ERROR;
    }
    return tf_buffer_write(out, (const uint8_t *)"\"", 1);
}

static int jsonl_encode(tf_encoder *self, tf_batch *in, tf_buffer *out) {
    (void)self;
    char numbuf[64];
#define JSONL_WRITE(expr) do { if ((expr) != TF_OK) return TF_ERROR; } while (0)

    for (size_t r = 0; r < in->n_rows; r++) {
        JSONL_WRITE(tf_buffer_write(out, (const uint8_t *)"{", 1));
        for (size_t c = 0; c < in->n_cols; c++) {
            if (c > 0) JSONL_WRITE(tf_buffer_write(out, (const uint8_t *)",", 1));

            /* Key */
            JSONL_WRITE(jsonl_write_escaped_string(out, in->col_names[c]));
            JSONL_WRITE(tf_buffer_write(out, (const uint8_t *)":", 1));

            /* Value */
            if (tf_batch_is_null(in, r, c)) {
                JSONL_WRITE(tf_buffer_write_str(out, "null"));
                continue;
            }
            switch (in->col_types[c]) {
                case TF_TYPE_BOOL:
                    JSONL_WRITE(tf_buffer_write_str(out, tf_batch_get_bool(in, r, c) ? "true" : "false"));
                    break;
                case TF_TYPE_INT64:
                    snprintf(numbuf, sizeof(numbuf), "%lld",
                             (long long)tf_batch_get_int64(in, r, c));
                    JSONL_WRITE(tf_buffer_write_str(out, numbuf));
                    break;
                case TF_TYPE_FLOAT64:
                    if (tf_format_float64(numbuf, sizeof(numbuf),
                                          tf_batch_get_float64(in, r, c)) != TF_OK)
                        return TF_ERROR;
                    JSONL_WRITE(tf_buffer_write_str(out, numbuf));
                    break;
                case TF_TYPE_STRING:
                    JSONL_WRITE(jsonl_write_escaped_string(out, tf_batch_get_string(in, r, c)));
                    break;
                case TF_TYPE_DATE: {
                    char dbuf[32];
                    tf_date_format(tf_batch_get_date(in, r, c), dbuf, sizeof(dbuf));
                    JSONL_WRITE(jsonl_write_escaped_string(out, dbuf));
                    break;
                }
                case TF_TYPE_TIMESTAMP: {
                    char tsbuf[40];
                    tf_timestamp_format(tf_batch_get_timestamp(in, r, c), tsbuf, sizeof(tsbuf));
                    JSONL_WRITE(jsonl_write_escaped_string(out, tsbuf));
                    break;
                }
                default:
                    JSONL_WRITE(tf_buffer_write_str(out, "null"));
                    break;
            }
        }
        JSONL_WRITE(tf_buffer_write(out, (const uint8_t *)"}\n", 2));
    }
#undef JSONL_WRITE
    return TF_OK;
}

static int jsonl_encoder_flush(tf_encoder *self, tf_buffer *out) {
    (void)self; (void)out;
    return TF_OK;
}

static void jsonl_encoder_destroy(tf_encoder *self) {
    free(self);
}

tf_encoder *tf_jsonl_encoder_create(const cJSON *args) {
    (void)args;
    tf_encoder *enc = malloc(sizeof(tf_encoder));
    if (!enc) return NULL;
    enc->encode = jsonl_encode;
    enc->flush = jsonl_encoder_flush;
    enc->destroy = jsonl_encoder_destroy;
    enc->state = NULL;
    return enc;
}
