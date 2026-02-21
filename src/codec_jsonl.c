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

/* ================================================================
 * JSONL Decoder
 * ================================================================ */

typedef struct {
    size_t    batch_size;

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
                              tf_batch ***out, size_t *n_out, size_t *out_cap) {
    /* Skip empty lines */
    if (len == 0) return TF_OK;

    /* Parse JSON */
    cJSON *obj = cJSON_ParseWithLength(line, len);
    if (!obj || !cJSON_IsObject(obj)) {
        cJSON_Delete(obj);
        return TF_OK; /* skip bad lines */
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
                    st->col_types[c] = widen_type(st->col_types[c], json_to_type(item));
                    break;
                }
            }
        }
    }

    /* Update batch column types (since they might have widened) */
    if (st->batch) {
        for (size_t c = 0; c < st->n_cols; c++) {
            st->batch->col_types[c] = st->col_types[c];
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
                        tf_batch ***out, size_t *n_out) {
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
            if (line_len > 0) {
                if (process_jsonl_line(st, (const char *)buf + line_start, line_len,
                                       out, n_out, &out_cap) != TF_OK)
                    return TF_ERROR;
            }
            line_start = i + 1;
        }
    }

    st->line_buf.read_pos += line_start;
    tf_buffer_compact(&st->line_buf);
    return TF_OK;
}

static int jsonl_flush(tf_decoder *self, tf_batch ***out, size_t *n_out) {
    jsonl_decoder_state *st = self->state;
    *out = NULL;
    *n_out = 0;
    size_t out_cap = 0;

    /* Process remaining data */
    size_t remaining = tf_buffer_readable(&st->line_buf);
    if (remaining > 0) {
        uint8_t *buf = st->line_buf.data + st->line_buf.read_pos;
        process_jsonl_line(st, (const char *)buf, remaining, out, n_out, &out_cap);
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
    if (args) {
        cJSON *bs = cJSON_GetObjectItemCaseSensitive(args, "batch_size");
        if (cJSON_IsNumber(bs) && bs->valueint > 0)
            st->batch_size = (size_t)bs->valueint;
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
