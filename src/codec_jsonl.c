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
    size_t    line_number;
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

static int jsonl_convert_batch_column(tf_batch *batch, size_t col, tf_type new_type) {
    if (!batch || col >= batch->n_cols || batch->col_types[col] == new_type) return TF_OK;

    tf_type old_type = batch->col_types[col];
    size_t sz = jsonl_type_size(new_type);
    batch->col_types[col] = new_type;
    if (sz == 0) {
        batch->columns[col] = NULL;
        batch->nulls[col] = NULL;
        return TF_OK;
    }

    void *new_col = tf_arena_alloc(batch->arena, sz * batch->capacity);
    uint8_t *new_nulls = tf_arena_alloc(batch->arena, batch->capacity);
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
                    snprintf(numbuf, sizeof(numbuf), "%g",
                             ((double *)batch->columns[col])[r]);
                    s = numbuf;
                    break;
                case TF_TYPE_STRING:
                    s = ((char **)batch->columns[col])[r];
                    break;
                default:
                    s = "";
                    break;
            }
            ((char **)new_col)[r] = tf_arena_strdup(batch->arena, s ? s : "");
            if (!((char **)new_col)[r]) return TF_ERROR;
            new_nulls[r] = 0;
        }
    }

    batch->columns[col] = new_col;
    batch->nulls[col] = new_nulls;
    return TF_OK;
}

static int jsonl_widen_column(jsonl_decoder_state *st, size_t col, tf_type incoming) {
    tf_type widened = widen_type(st->col_types[col], incoming);
    if (widened == st->col_types[col]) return TF_OK;
    st->col_types[col] = widened;
    return jsonl_convert_batch_column(st->batch, col, widened);
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
    int rc = tf_buffer_write_str(side->errors, printed);
    if (rc == TF_OK) rc = tf_buffer_write_str(side->errors, "\n");
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

static tf_batch *make_jsonl_batch(jsonl_decoder_state *st) {
    tf_batch *b = tf_batch_create(st->n_cols, st->batch_size);
    if (!b) return NULL;
    for (size_t i = 0; i < st->n_cols; i++) {
        tf_batch_set_schema(b, i, st->col_names[i], st->col_types[i]);
    }
    return b;
}

static int add_json_row(jsonl_decoder_state *st, cJSON *obj) {
    if (!st->batch) {
        st->batch = make_jsonl_batch(st);
        if (!st->batch) return TF_ERROR;
    }

    size_t row = st->batch->n_rows;
    if (tf_batch_ensure_capacity(st->batch, row + 1) != TF_OK) return TF_ERROR;

    for (size_t c = 0; c < st->n_cols; c++) {
        cJSON *val = cJSON_GetObjectItemCaseSensitive(obj, st->col_names[c]);
        if (!val || cJSON_IsNull(val)) {
            tf_batch_set_null(st->batch, row, c);
            continue;
        }
        switch (st->col_types[c]) {
            case TF_TYPE_BOOL:
                tf_batch_set_bool(st->batch, row, c, cJSON_IsTrue(val));
                break;
            case TF_TYPE_INT64:
                tf_batch_set_int64(st->batch, row, c, (int64_t)val->valuedouble);
                break;
            case TF_TYPE_FLOAT64:
                tf_batch_set_float64(st->batch, row, c, val->valuedouble);
                break;
            case TF_TYPE_STRING:
                if (cJSON_IsString(val))
                    tf_batch_set_string(st->batch, row, c, val->valuestring);
                else {
                    char *printed = cJSON_PrintUnformatted(val);
                    tf_batch_set_string(st->batch, row, c, printed ? printed : "");
                    free(printed);
                }
                break;
            default:
                tf_batch_set_null(st->batch, row, c);
                break;
        }
    }
    st->batch->n_rows = row + 1;
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
        st->n_cols = (size_t)n;
        st->col_names = malloc(n * sizeof(char *));
        st->col_types = malloc(n * sizeof(tf_type));

        int i = 0;
        cJSON *item;
        cJSON_ArrayForEach(item, obj) {
            st->col_names[i] = strdup(item->string);
            st->col_types[i] = json_to_type(item);
            i++;
        }
        st->schema_ready = 1;
    } else {
        /* Update types from new row */
        cJSON *item;
        cJSON_ArrayForEach(item, obj) {
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
        if (*n_out >= *out_cap) {
            *out_cap = (*out_cap == 0) ? 4 : *out_cap * 2;
            *out = realloc(*out, *out_cap * sizeof(tf_batch *));
        }
        (*out)[(*n_out)++] = st->batch;
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

    if (tf_buffer_write(&st->line_buf, data, len) != TF_OK) return TF_ERROR;

    size_t out_cap = 0;
    uint8_t *buf = st->line_buf.data + st->line_buf.read_pos;
    size_t buf_len = st->line_buf.len - st->line_buf.read_pos;

    size_t line_start = 0;
    for (size_t i = 0; i < buf_len; i++) {
        if (buf[i] == '\n' || buf[i] == '\r') {
            size_t line_len = i - line_start;
            if (buf[i] == '\r' && i + 1 < buf_len && buf[i + 1] == '\n') i++;
            size_t line_no = ++st->line_number;
            if (line_len > 0) {
                if (process_jsonl_line(st, (const char *)buf + line_start, line_len,
                                       line_no, out, n_out, &out_cap, side) != TF_OK)
                    return TF_ERROR;
            }
            line_start = i + 1;
        }
    }

    st->line_buf.read_pos += line_start;
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
        if (process_jsonl_line(st, (const char *)buf, remaining, line_no,
                               out, n_out, &out_cap, side) != TF_OK)
            return TF_ERROR;
        st->line_buf.read_pos = st->line_buf.len;
    }

    /* Emit remaining batch */
    if (st->batch && st->rows_buffered > 0) {
        if (*n_out >= out_cap) {
            out_cap = (*n_out == 0) ? 1 : out_cap * 2;
            *out = realloc(*out, out_cap * sizeof(tf_batch *));
        }
        (*out)[(*n_out)++] = st->batch;
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
        for (size_t i = 0; i < st->n_cols; i++) free(st->col_names[i]);
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
    if (args) {
        cJSON *bs = cJSON_GetObjectItemCaseSensitive(args, "batch_size");
        if (cJSON_IsNumber(bs) && bs->valueint > 0)
            st->batch_size = (size_t)bs->valueint;
        cJSON *on_error = cJSON_GetObjectItemCaseSensitive(args, "on_error");
        if (cJSON_IsString(on_error)) {
            if (!parse_jsonl_error_action(on_error->valuestring, &st->on_error)) {
                tf_set_last_error("jsonl: on_error must be skip, fail, warn, or quarantine");
                free(st);
                return NULL;
            }
        }
        cJSON *max_err = cJSON_GetObjectItemCaseSensitive(args, "max_error_bytes");
        if (cJSON_IsNumber(max_err) && max_err->valueint >= 0)
            st->max_error_bytes = (size_t)max_err->valueint;
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

static int jsonl_encode(tf_encoder *self, tf_batch *in, tf_buffer *out) {
    (void)self;
    char numbuf[64];

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_buffer_write(out, (const uint8_t *)"{", 1);
        for (size_t c = 0; c < in->n_cols; c++) {
            if (c > 0) tf_buffer_write(out, (const uint8_t *)",", 1);

            /* Key */
            tf_buffer_write(out, (const uint8_t *)"\"", 1);
            tf_buffer_write_str(out, in->col_names[c]);
            tf_buffer_write(out, (const uint8_t *)"\":", 2);

            /* Value */
            if (tf_batch_is_null(in, r, c)) {
                tf_buffer_write_str(out, "null");
                continue;
            }
            switch (in->col_types[c]) {
                case TF_TYPE_BOOL:
                    tf_buffer_write_str(out, tf_batch_get_bool(in, r, c) ? "true" : "false");
                    break;
                case TF_TYPE_INT64:
                    snprintf(numbuf, sizeof(numbuf), "%lld",
                             (long long)tf_batch_get_int64(in, r, c));
                    tf_buffer_write_str(out, numbuf);
                    break;
                case TF_TYPE_FLOAT64:
                    snprintf(numbuf, sizeof(numbuf), "%g",
                             tf_batch_get_float64(in, r, c));
                    tf_buffer_write_str(out, numbuf);
                    break;
                case TF_TYPE_STRING: {
                    const char *s = tf_batch_get_string(in, r, c);
                    tf_buffer_write(out, (const uint8_t *)"\"", 1);
                    /* Escape special characters */
                    for (const char *p = s; *p; p++) {
                        switch (*p) {
                            case '"':  tf_buffer_write_str(out, "\\\""); break;
                            case '\\': tf_buffer_write_str(out, "\\\\"); break;
                            case '\n': tf_buffer_write_str(out, "\\n"); break;
                            case '\r': tf_buffer_write_str(out, "\\r"); break;
                            case '\t': tf_buffer_write_str(out, "\\t"); break;
                            default:   tf_buffer_write(out, (const uint8_t *)p, 1); break;
                        }
                    }
                    tf_buffer_write(out, (const uint8_t *)"\"", 1);
                    break;
                }
                case TF_TYPE_DATE: {
                    char dbuf[16];
                    tf_date_format(tf_batch_get_date(in, r, c), dbuf, sizeof(dbuf));
                    tf_buffer_write(out, (const uint8_t *)"\"", 1);
                    tf_buffer_write_str(out, dbuf);
                    tf_buffer_write(out, (const uint8_t *)"\"", 1);
                    break;
                }
                case TF_TYPE_TIMESTAMP: {
                    char tsbuf[40];
                    tf_timestamp_format(tf_batch_get_timestamp(in, r, c), tsbuf, sizeof(tsbuf));
                    tf_buffer_write(out, (const uint8_t *)"\"", 1);
                    tf_buffer_write_str(out, tsbuf);
                    tf_buffer_write(out, (const uint8_t *)"\"", 1);
                    break;
                }
                default:
                    tf_buffer_write_str(out, "null");
                    break;
            }
        }
        tf_buffer_write(out, (const uint8_t *)"}\n", 2);
    }
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
