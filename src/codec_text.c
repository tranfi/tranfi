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
#define DEFAULT_MAX_ERROR_BYTES 4096
#define DEFAULT_MAX_RECORD_BYTES (64 * 1024 * 1024)

/* ================================================================
 * Text Decoder
 * ================================================================ */

typedef struct {
    size_t    batch_size;
    size_t    max_error_bytes;
    size_t    max_record_bytes;
    size_t    line_number;
    size_t    byte_offset;
    tf_buffer line_buf;
    tf_batch *batch;
    size_t    rows_buffered;
} text_decoder_state;

static tf_batch *make_text_batch(size_t capacity) {
    tf_batch *b = tf_batch_create(1, capacity);
    if (!b) return NULL;
    if (tf_batch_set_schema(b, 0, "_line", TF_TYPE_STRING) != TF_OK) {
        tf_batch_free(b);
        return NULL;
    }
    return b;
}

static int text_write_json_line(tf_buffer *buf, cJSON *obj) {
    char *printed = cJSON_PrintUnformatted(obj);
    if (!printed) return TF_ERROR;
    int rc = tf_buffer_write_str(buf, printed);
    if (rc == TF_OK) rc = tf_buffer_write_str(buf, "\n");
    free(printed);
    return rc;
}

static char *text_record_preview(text_decoder_state *st,
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

static int emit_text_record_size_diagnostic(text_decoder_state *st,
                                            const uint8_t *prefix, size_t prefix_len,
                                            const uint8_t *suffix, size_t suffix_len,
                                            size_t line_no, size_t byte_offset,
                                            size_t observed_len,
                                            tf_side_channels *side) {
    if (!side || !side->errors) return TF_OK;
    int truncated = 0;
    char *raw = text_record_preview(st, prefix, prefix_len, suffix, suffix_len, &truncated);
    if (!raw) return TF_ERROR;

    cJSON *obj = cJSON_CreateObject();
    if (!obj) { free(raw); return TF_ERROR; }
    cJSON_AddStringToObject(obj, "type", "text_record_too_large");
    cJSON_AddStringToObject(obj, "op", "codec.text.decode");
    cJSON_AddStringToObject(obj, "action", "fail");
    cJSON_AddStringToObject(obj, "severity", "error");
    cJSON_AddNumberToObject(obj, "line", (double)line_no);
    cJSON_AddNumberToObject(obj, "byte_offset", (double)byte_offset);
    cJSON_AddNumberToObject(obj, "max_record_bytes", (double)st->max_record_bytes);
    cJSON_AddNumberToObject(obj, "observed_bytes", (double)observed_len);
    cJSON_AddStringToObject(obj, "message", "Text record exceeds max_record_bytes");
    cJSON_AddNumberToObject(obj, "raw_bytes", (double)observed_len);
    cJSON_AddStringToObject(obj, "raw", raw);
    if (truncated) cJSON_AddBoolToObject(obj, "truncated", 1);

    int rc = text_write_json_line(side->errors, obj);
    cJSON_Delete(obj);
    free(raw);
    return rc;
}

static int fail_text_record_limit(text_decoder_state *st,
                                  const uint8_t *prefix, size_t prefix_len,
                                  const uint8_t *suffix, size_t suffix_len,
                                  size_t line_no, size_t byte_offset,
                                  size_t observed_len,
                                  tf_side_channels *side) {
    if (emit_text_record_size_diagnostic(st, prefix, prefix_len, suffix, suffix_len,
                                         line_no, byte_offset, observed_len, side) != TF_OK)
        return TF_ERROR;
    char err[256];
    snprintf(err, sizeof(err),
             "text record exceeds max_record_bytes at line %zu: max %zu bytes, observed %zu bytes",
             line_no, st->max_record_bytes, observed_len);
    tf_set_last_error(err);
    return TF_ERROR;
}

static int check_text_incoming_record_limit(text_decoder_state *st,
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
        if (data[i] == '\n') {
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
            return fail_text_record_limit(st, prefix, prefix_len,
                                          data + segment_start, suffix_len,
                                          line_no, line_offset, current_len, side);
        }
    }
    return TF_OK;
}

static int check_text_buffer_record_limit(text_decoder_state *st, size_t observed_len,
                                          size_t line_no, size_t byte_offset,
                                          tf_side_channels *side) {
    if (st->max_record_bytes == 0 || observed_len <= st->max_record_bytes) return TF_OK;
    const uint8_t *buf = st->line_buf.data + st->line_buf.read_pos;
    return fail_text_record_limit(st, buf, observed_len, NULL, 0,
                                  line_no, byte_offset, observed_len, side);
}

static int text_set_line_cell(tf_batch *batch, size_t row,
                              const uint8_t *data, size_t len) {
    return tf_batch_set_string_len(batch, row, 0, (const char *)data, len);
}

static int text_append_output_batch(tf_batch ***out, size_t *n_out,
                                    size_t *out_cap, tf_batch *batch) {
    if (*n_out >= *out_cap) {
        size_t need = 0;
        size_t new_cap = 0;
        if (tf_size_add(*n_out, 1, &need) != TF_OK ||
            tf_size_grow_pow2(*out_cap, need, 1, &new_cap) != TF_OK) {
            return TF_ERROR;
        }
        tf_batch **new_out = tf_reallocarray_checked(*out, new_cap, sizeof(tf_batch *));
        if (!new_out) return TF_ERROR;
        *out = new_out;
        *out_cap = new_cap;
    }
    (*out)[(*n_out)++] = batch;
    return TF_OK;
}

static int text_decode(tf_decoder *self, const uint8_t *data, size_t len,
                       tf_batch ***out, size_t *n_out, tf_side_channels *side) {
    (void)side;
    text_decoder_state *st = self->state;
    *out = NULL;
    *n_out = 0;

    if (check_text_incoming_record_limit(st, data, len, side) != TF_OK) return TF_ERROR;
    if (tf_buffer_write(&st->line_buf, data, len) != TF_OK) return TF_ERROR;

    size_t out_cap = 0;
    uint8_t *buf = st->line_buf.data + st->line_buf.read_pos;
    size_t buf_len = st->line_buf.len - st->line_buf.read_pos;

    size_t line_start = 0;
    for (size_t i = 0; i < buf_len; i++) {
        if (buf[i] == '\n') {
            size_t line_len = i - line_start;
            size_t line_no = ++st->line_number;
            /* Strip trailing \r for CRLF */
            if (line_len > 0 && buf[line_start + line_len - 1] == '\r')
                line_len--;

            (void)line_no;
            if (!st->batch) {
                st->batch = make_text_batch(st->batch_size);
                if (!st->batch) return TF_ERROR;
            }

            size_t row = st->batch->n_rows;
            size_t need_rows = 0;
            if (tf_size_add(row, 1, &need_rows) != TF_OK ||
                tf_batch_ensure_capacity(st->batch, need_rows) != TF_OK)
                return TF_ERROR;

            /* Copy line into batch */
            if (text_set_line_cell(st->batch, row, buf + line_start, line_len) != TF_OK)
                return TF_ERROR;
            if (tf_batch_expose_row(st->batch, row) != TF_OK)
                return TF_ERROR;
            st->rows_buffered++;

            line_start = i + 1;

            /* Emit batch if full */
            if (st->rows_buffered >= st->batch_size) {
                if (text_append_output_batch(out, n_out, &out_cap, st->batch) != TF_OK)
                    return TF_ERROR;
                st->batch = NULL;
                st->rows_buffered = 0;
            }
        }
    }

    st->line_buf.read_pos += line_start;
    st->byte_offset += line_start;
    tf_buffer_compact(&st->line_buf);
    return TF_OK;
}

static int text_flush(tf_decoder *self, tf_batch ***out, size_t *n_out, tf_side_channels *side) {
    (void)side;
    text_decoder_state *st = self->state;
    *out = NULL;
    *n_out = 0;
    size_t out_cap = 0;

    /* Process remaining data as a final line */
    size_t remaining = tf_buffer_readable(&st->line_buf);
    if (remaining > 0) {
        uint8_t *buf = st->line_buf.data + st->line_buf.read_pos;
        size_t line_no = st->line_number + 1;
        size_t record_offset = st->byte_offset;
        if (check_text_buffer_record_limit(st, remaining, line_no, record_offset, side) != TF_OK)
            return TF_ERROR;
        st->line_number = line_no;

        if (!st->batch) {
            st->batch = make_text_batch(st->batch_size);
            if (!st->batch) return TF_ERROR;
        }

        size_t row = st->batch->n_rows;
        size_t need_rows = 0;
        if (tf_size_add(row, 1, &need_rows) != TF_OK ||
            tf_batch_ensure_capacity(st->batch, need_rows) != TF_OK)
            return TF_ERROR;

        /* Strip trailing \r */
        size_t line_len = remaining;
        if (line_len > 0 && buf[line_len - 1] == '\r')
            line_len--;

        if (text_set_line_cell(st->batch, row, buf, line_len) != TF_OK)
            return TF_ERROR;
        if (tf_batch_expose_row(st->batch, row) != TF_OK)
            return TF_ERROR;
        st->rows_buffered++;

        st->line_buf.read_pos = st->line_buf.len;
        st->byte_offset += remaining;
    }

    /* Emit remaining batch */
    if (st->batch && st->rows_buffered > 0) {
        if (text_append_output_batch(out, n_out, &out_cap, st->batch) != TF_OK)
            return TF_ERROR;
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
    st->max_error_bytes = DEFAULT_MAX_ERROR_BYTES;
    st->max_record_bytes = DEFAULT_MAX_RECORD_BYTES;
    if (args) {
        size_t parsed_size = 0;
        int has_batch_size = tf_json_get_size_arg(args, "batch_size", 1, TF_MAX_BATCH_ROWS, &parsed_size, "text");
        if (has_batch_size < 0) { free(st); return NULL; }
        if (has_batch_size > 0) st->batch_size = parsed_size;
        int has_max_error = tf_json_get_size_arg(args, "max_error_bytes", 0, TF_MAX_ERROR_BYTES, &parsed_size, "text");
        if (has_max_error < 0) { free(st); return NULL; }
        if (has_max_error > 0) st->max_error_bytes = parsed_size;
        int has_max_record = tf_json_get_size_arg(args, "max_record_bytes", 0, TF_MAX_RECORD_BYTES, &parsed_size, "text");
        if (has_max_record < 0) { free(st); return NULL; }
        if (has_max_record > 0) st->max_record_bytes = parsed_size;
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

static int text_write_string(tf_buffer *out, const char *s) {
    const char *value = s ? s : "";
    return tf_buffer_write(out, (const uint8_t *)value, strlen(value));
}

static int text_encode(tf_encoder *self, tf_batch *in, tf_buffer *out) {
    (void)self;

    /* Find _line column index */
    int line_col = tf_batch_col_index(in, "_line");

    for (size_t r = 0; r < in->n_rows; r++) {
        if (line_col >= 0) {
            /* Write _line column value */
            if (!tf_batch_is_null(in, r, (size_t)line_col)) {
                const char *s = tf_batch_get_string(in, r, (size_t)line_col);
                if (text_write_string(out, s) != TF_OK) return TF_ERROR;
            }
        } else {
            /* Fallback: concatenate all string columns with tab */
            for (size_t c = 0; c < in->n_cols; c++) {
                if (c > 0 && tf_buffer_write(out, (const uint8_t *)"\t", 1) != TF_OK)
                    return TF_ERROR;
                if (!tf_batch_is_null(in, r, c) && in->col_types[c] == TF_TYPE_STRING) {
                    const char *s = tf_batch_get_string(in, r, c);
                    if (text_write_string(out, s) != TF_OK) return TF_ERROR;
                }
            }
        }
        if (tf_buffer_write(out, (const uint8_t *)"\n", 1) != TF_OK) return TF_ERROR;
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
