/*
 * codec_table.c — Pretty-print Markdown-compatible table encoder.
 *
 * Buffers all rows to compute column widths, then on flush emits
 * a formatted table:
 *
 *   | name    | age | city |
 *   | ------- | --- | ---- |
 *   | Alice   |  30 | NY   |
 *   | Bob     |  25 | LA   |
 *
 * Args:
 *   max_width (int, optional) — truncate columns wider than this (default: 40)
 *   max_rows (int, optional) — limit output rows (default: 0 = unlimited)
 */

#include "internal.h"
#include "cJSON.h"
#include "date_utils.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define TABLE_MAX_COLS      256
#define TABLE_DEFAULT_WIDTH 40

typedef struct {
    char **values;    /* flat array: rows × n_cols, each cell is a malloc'd string */
    size_t n_rows;
    size_t n_cols;
    size_t capacity;  /* allocated rows */
    char **col_names;
    size_t max_width;
    size_t max_rows;
} table_encoder_state;

/* Format a cell value as a string. Caller must free. */
static char *cell_to_string(tf_batch *b, size_t row, size_t col) {
    if (tf_batch_is_null(b, row, col)) return strdup("");

    char buf[128];
    switch (b->col_types[col]) {
        case TF_TYPE_BOOL:
            return strdup(tf_batch_get_bool(b, row, col) ? "true" : "false");
        case TF_TYPE_INT64:
            snprintf(buf, sizeof(buf), "%lld", (long long)tf_batch_get_int64(b, row, col));
            return strdup(buf);
        case TF_TYPE_FLOAT64:
            snprintf(buf, sizeof(buf), "%g", tf_batch_get_float64(b, row, col));
            return strdup(buf);
        case TF_TYPE_STRING:
            return strdup(tf_batch_get_string(b, row, col));
        case TF_TYPE_DATE: {
            char dbuf[16];
            tf_date_format(tf_batch_get_date(b, row, col), dbuf, sizeof(dbuf));
            return strdup(dbuf);
        }
        case TF_TYPE_TIMESTAMP: {
            char tsbuf[40];
            tf_timestamp_format(tf_batch_get_timestamp(b, row, col), tsbuf, sizeof(tsbuf));
            return strdup(tsbuf);
        }
        default:
            return strdup("");
    }
}

static int table_encode(tf_encoder *self, tf_batch *in, tf_buffer *out) {
    table_encoder_state *st = self->state;
    (void)out;

    /* Capture column names on first batch */
    if (!st->col_names && in->n_cols > 0) {
        st->n_cols = in->n_cols < TABLE_MAX_COLS ? in->n_cols : TABLE_MAX_COLS;
        st->col_names = malloc(st->n_cols * sizeof(char *));
        for (size_t i = 0; i < st->n_cols; i++) {
            st->col_names[i] = strdup(in->col_names[i]);
        }
    }

    /* Buffer all cell values as strings */
    for (size_t r = 0; r < in->n_rows; r++) {
        if (st->max_rows > 0 && st->n_rows >= st->max_rows) break;

        if (st->n_rows >= st->capacity) {
            size_t new_cap = st->capacity == 0 ? 64 : st->capacity * 2;
            st->values = realloc(st->values, new_cap * st->n_cols * sizeof(char *));
            if (!st->values) return TF_ERROR;
            st->capacity = new_cap;
        }

        size_t base = st->n_rows * st->n_cols;
        for (size_t c = 0; c < st->n_cols; c++) {
            st->values[base + c] = cell_to_string(in, r, c);
        }
        st->n_rows++;
    }

    return TF_OK;
}

static int table_flush(tf_encoder *self, tf_buffer *out) {
    table_encoder_state *st = self->state;
    if (!st->col_names || st->n_cols == 0) return TF_OK;

    /* Compute column widths */
    size_t *widths = calloc(st->n_cols, sizeof(size_t));
    for (size_t c = 0; c < st->n_cols; c++) {
        widths[c] = strlen(st->col_names[c]);
    }
    for (size_t r = 0; r < st->n_rows; r++) {
        for (size_t c = 0; c < st->n_cols; c++) {
            size_t len = strlen(st->values[r * st->n_cols + c]);
            if (len > widths[c]) widths[c] = len;
        }
    }
    /* Apply max_width */
    for (size_t c = 0; c < st->n_cols; c++) {
        if (st->max_width > 0 && widths[c] > st->max_width)
            widths[c] = st->max_width;
    }

    char pad[256];

    /* Header row */
    tf_buffer_write(out, (const uint8_t *)"| ", 2);
    for (size_t c = 0; c < st->n_cols; c++) {
        if (c > 0) tf_buffer_write(out, (const uint8_t *)" | ", 3);
        const char *name = st->col_names[c];
        size_t nlen = strlen(name);
        size_t w = widths[c];
        if (nlen > w) nlen = w;
        tf_buffer_write(out, (const uint8_t *)name, nlen);
        if (nlen < w) {
            size_t plen = w - nlen;
            if (plen > sizeof(pad)) plen = sizeof(pad);
            memset(pad, ' ', plen);
            tf_buffer_write(out, (const uint8_t *)pad, plen);
        }
    }
    tf_buffer_write(out, (const uint8_t *)" |\n", 3);

    /* Separator row */
    tf_buffer_write(out, (const uint8_t *)"| ", 2);
    for (size_t c = 0; c < st->n_cols; c++) {
        if (c > 0) tf_buffer_write(out, (const uint8_t *)" | ", 3);
        size_t w = widths[c];
        if (w > sizeof(pad)) w = sizeof(pad);
        memset(pad, '-', w);
        tf_buffer_write(out, (const uint8_t *)pad, w);
    }
    tf_buffer_write(out, (const uint8_t *)" |\n", 3);

    /* Data rows */
    for (size_t r = 0; r < st->n_rows; r++) {
        tf_buffer_write(out, (const uint8_t *)"| ", 2);
        for (size_t c = 0; c < st->n_cols; c++) {
            if (c > 0) tf_buffer_write(out, (const uint8_t *)" | ", 3);
            const char *val = st->values[r * st->n_cols + c];
            size_t vlen = strlen(val);
            size_t w = widths[c];
            if (vlen > w) vlen = w;
            tf_buffer_write(out, (const uint8_t *)val, vlen);
            if (vlen < w) {
                size_t plen = w - vlen;
                if (plen > sizeof(pad)) plen = sizeof(pad);
                memset(pad, ' ', plen);
                tf_buffer_write(out, (const uint8_t *)pad, plen);
            }
        }
        tf_buffer_write(out, (const uint8_t *)" |\n", 3);
    }

    free(widths);
    return TF_OK;
}

static void table_encoder_destroy(tf_encoder *self) {
    table_encoder_state *st = self->state;
    if (st) {
        if (st->col_names) {
            for (size_t i = 0; i < st->n_cols; i++) free(st->col_names[i]);
            free(st->col_names);
        }
        if (st->values) {
            for (size_t i = 0; i < st->n_rows * st->n_cols; i++) free(st->values[i]);
            free(st->values);
        }
        free(st);
    }
    free(self);
}

tf_encoder *tf_table_encoder_create(const cJSON *args) {
    table_encoder_state *st = calloc(1, sizeof(table_encoder_state));
    if (!st) return NULL;

    st->max_width = TABLE_DEFAULT_WIDTH;
    st->max_rows = 0;

    if (args) {
        cJSON *mw = cJSON_GetObjectItemCaseSensitive(args, "max_width");
        if (cJSON_IsNumber(mw) && mw->valueint > 0)
            st->max_width = (size_t)mw->valueint;

        cJSON *mr = cJSON_GetObjectItemCaseSensitive(args, "max_rows");
        if (cJSON_IsNumber(mr) && mr->valueint > 0)
            st->max_rows = (size_t)mr->valueint;
    }

    tf_encoder *enc = malloc(sizeof(tf_encoder));
    if (!enc) { free(st); return NULL; }
    enc->encode = table_encode;
    enc->flush = table_flush;
    enc->destroy = table_encoder_destroy;
    enc->state = st;
    return enc;
}
