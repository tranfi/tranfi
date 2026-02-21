/*
 * codec_text.c — Plain text line codec (newline-split).
 *
 * Decoder: splits on newlines, each line → one row in single "_line" column.
 *   - No type detection, no field parsing — just memchr for newlines.
 *   - Emits batch at batch_size.
 *
 * Encoder: writes _line string + \n per row.
 *   - Fallback: if no _line column, concatenate all string columns with tab.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>

#define DEFAULT_BATCH_SIZE 1024

/* ================================================================
 * Text Decoder
 * ================================================================ */

typedef struct {
    size_t    batch_size;
    tf_buffer line_buf;
    tf_batch *batch;
    size_t    rows_buffered;
} text_decoder_state;

static tf_batch *make_text_batch(size_t capacity) {
    tf_batch *b = tf_batch_create(1, capacity);
    if (!b) return NULL;
    tf_batch_set_schema(b, 0, "_line", TF_TYPE_STRING);
    return b;
}

static int text_decode(tf_decoder *self, const uint8_t *data, size_t len,
                       tf_batch ***out, size_t *n_out) {
    text_decoder_state *st = self->state;
    *out = NULL;
    *n_out = 0;

    if (tf_buffer_write(&st->line_buf, data, len) != TF_OK) return TF_ERROR;

    size_t out_cap = 0;
    uint8_t *buf = st->line_buf.data + st->line_buf.read_pos;
    size_t buf_len = st->line_buf.len - st->line_buf.read_pos;

    size_t line_start = 0;
    for (size_t i = 0; i < buf_len; i++) {
        if (buf[i] == '\n') {
            size_t line_len = i - line_start;
            /* Strip trailing \r for CRLF */
            if (line_len > 0 && buf[line_start + line_len - 1] == '\r')
                line_len--;

            if (!st->batch) {
                st->batch = make_text_batch(st->batch_size);
                if (!st->batch) return TF_ERROR;
            }

            size_t row = st->batch->n_rows;
            if (tf_batch_ensure_capacity(st->batch, row + 1) != TF_OK)
                return TF_ERROR;

            /* Copy line into batch */
            char *line_str = malloc(line_len + 1);
            if (!line_str) return TF_ERROR;
            memcpy(line_str, buf + line_start, line_len);
            line_str[line_len] = '\0';
            tf_batch_set_string(st->batch, row, 0, line_str);
            free(line_str);
            st->batch->n_rows = row + 1;
            st->rows_buffered++;

            line_start = i + 1;

            /* Emit batch if full */
            if (st->rows_buffered >= st->batch_size) {
                if (*n_out >= out_cap) {
                    out_cap = (out_cap == 0) ? 4 : out_cap * 2;
                    *out = realloc(*out, out_cap * sizeof(tf_batch *));
                }
                (*out)[(*n_out)++] = st->batch;
                st->batch = NULL;
                st->rows_buffered = 0;
            }
        }
    }

    st->line_buf.read_pos += line_start;
    tf_buffer_compact(&st->line_buf);
    return TF_OK;
}

static int text_flush(tf_decoder *self, tf_batch ***out, size_t *n_out) {
    text_decoder_state *st = self->state;
    *out = NULL;
    *n_out = 0;
    size_t out_cap = 0;

    /* Process remaining data as a final line */
    size_t remaining = tf_buffer_readable(&st->line_buf);
    if (remaining > 0) {
        uint8_t *buf = st->line_buf.data + st->line_buf.read_pos;

        if (!st->batch) {
            st->batch = make_text_batch(st->batch_size);
            if (!st->batch) return TF_ERROR;
        }

        size_t row = st->batch->n_rows;
        if (tf_batch_ensure_capacity(st->batch, row + 1) != TF_OK)
            return TF_ERROR;

        /* Strip trailing \r */
        size_t line_len = remaining;
        if (line_len > 0 && buf[line_len - 1] == '\r')
            line_len--;

        char *line_str = malloc(line_len + 1);
        if (!line_str) return TF_ERROR;
        memcpy(line_str, buf, line_len);
        line_str[line_len] = '\0';
        tf_batch_set_string(st->batch, row, 0, line_str);
        free(line_str);
        st->batch->n_rows = row + 1;
        st->rows_buffered++;

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

static void text_decoder_destroy(tf_decoder *self) {
    text_decoder_state *st = self->state;
    if (st) {
        tf_buffer_free(&st->line_buf);
        if (st->batch) tf_batch_free(st->batch);
        free(st);
    }
    free(self);
}

tf_decoder *tf_text_decoder_create(const cJSON *args) {
    text_decoder_state *st = calloc(1, sizeof(text_decoder_state));
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
    dec->decode = text_decode;
    dec->flush = text_flush;
    dec->destroy = text_decoder_destroy;
    dec->state = st;
    return dec;
}

/* ================================================================
 * Text Encoder
 * ================================================================ */

static int text_encode(tf_encoder *self, tf_batch *in, tf_buffer *out) {
    (void)self;

    /* Find _line column index */
    int line_col = tf_batch_col_index(in, "_line");

    for (size_t r = 0; r < in->n_rows; r++) {
        if (line_col >= 0) {
            /* Write _line column value */
            if (!tf_batch_is_null(in, r, (size_t)line_col)) {
                const char *s = tf_batch_get_string(in, r, (size_t)line_col);
                tf_buffer_write(out, (const uint8_t *)s, strlen(s));
            }
        } else {
            /* Fallback: concatenate all string columns with tab */
            for (size_t c = 0; c < in->n_cols; c++) {
                if (c > 0) tf_buffer_write(out, (const uint8_t *)"\t", 1);
                if (!tf_batch_is_null(in, r, c) && in->col_types[c] == TF_TYPE_STRING) {
                    const char *s = tf_batch_get_string(in, r, c);
                    tf_buffer_write(out, (const uint8_t *)s, strlen(s));
                }
            }
        }
        tf_buffer_write(out, (const uint8_t *)"\n", 1);
    }
    return TF_OK;
}

static int text_encoder_flush(tf_encoder *self, tf_buffer *out) {
    (void)self; (void)out;
    return TF_OK;
}

static void text_encoder_destroy(tf_encoder *self) {
    free(self);
}

tf_encoder *tf_text_encoder_create(const cJSON *args) {
    (void)args;
    tf_encoder *enc = malloc(sizeof(tf_encoder));
    if (!enc) return NULL;
    enc->encode = text_encode;
    enc->flush = text_encoder_flush;
    enc->destroy = text_encoder_destroy;
    enc->state = NULL;
    return enc;
}
