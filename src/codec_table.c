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
#include <stdlib.h>
#include <string.h>

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
    char buf[64];
    const char *text = NULL;
    if (tf_batch_format_cell_as_string(b, row, col, TF_CELL_STRING_HUMAN,
                                       buf, sizeof(buf), &text) != TF_OK) {
        return NULL;
    }
    return strdup(text ? text : "");
}

static int table_capture_schema(table_encoder_state *st, const tf_batch *in) {
    size_t n_cols = in->n_cols < TABLE_MAX_COLS ? in->n_cols : TABLE_MAX_COLS;
    if (n_cols == 0) return TF_OK;
    char **names = tf_callocarray_checked(n_cols, sizeof(char *));
    if (!names) return TF_ERROR;
    for (size_t i = 0; i < n_cols; i++) {
        names[i] = strdup(in->col_names[i] ? in->col_names[i] : "");
        if (!names[i]) {
            for (size_t j = 0; j < i; j++) free(names[j]);
            free(names);
            return TF_ERROR;
        }
    }
    st->col_names = names;
    st->n_cols = n_cols;
    return TF_OK;
}

static int table_ensure_value_capacity(table_encoder_state *st, size_t min_rows) {
    if (st->n_cols == 0 || min_rows <= st->capacity) return TF_OK;
    size_t new_cap = 0;
    size_t cells = 0;
    if (tf_size_grow_pow2(st->capacity, min_rows, 64, &new_cap) != TF_OK ||
        tf_size_mul(new_cap, st->n_cols, &cells) != TF_OK) {
        return TF_ERROR;
    }
    char **new_values = tf_reallocarray_checked(st->values, cells, sizeof(char *));
    if (!new_values) return TF_ERROR;
    st->values = new_values;
    st->capacity = new_cap;
    return TF_OK;
}

static int table_write_repeat(tf_buffer *out, char ch, size_t count) {
    char pad[256];
    memset(pad, ch, sizeof(pad));
    while (count > 0) {
        size_t chunk = count < sizeof(pad) ? count : sizeof(pad);
        if (tf_buffer_write(out, (const uint8_t *)pad, chunk) != TF_OK) return TF_ERROR;
        count -= chunk;
    }
    return TF_OK;
}

static int table_encode(tf_encoder *self, tf_batch *in, tf_buffer *out) {
    table_encoder_state *st = self->state;
    (void)out;

    /* Capture column names on first batch */
    if (!st->col_names && in->n_cols > 0) {
        if (table_capture_schema(st, in) != TF_OK) return TF_ERROR;
    }

    /* Buffer all cell values as strings */
    for (size_t r = 0; r < in->n_rows; r++) {
        if (st->max_rows > 0 && st->n_rows >= st->max_rows) break;
        size_t next_rows = 0;
        if (tf_size_add(st->n_rows, 1, &next_rows) != TF_OK ||
            table_ensure_value_capacity(st, next_rows) != TF_OK) {
            return TF_ERROR;
        }

        size_t base = 0;
        if (tf_size_mul(st->n_rows, st->n_cols, &base) != TF_OK) return TF_ERROR;
        size_t c = 0;
        for (; c < st->n_cols; c++) {
            st->values[base + c] = cell_to_string(in, r, c);
            if (!st->values[base + c]) {
                for (size_t j = 0; j < c; j++) {
                    free(st->values[base + j]);
                    st->values[base + j] = NULL;
                }
                return TF_ERROR;
            }
        }
        st->n_rows = next_rows;
    }

    return TF_OK;
}

static int table_flush(tf_encoder *self, tf_buffer *out) {
    table_encoder_state *st = self->state;
    if (!st->col_names || st->n_cols == 0) return TF_OK;

    /* Compute column widths */
    size_t *widths = tf_callocarray_checked(st->n_cols, sizeof(size_t));
    if (!widths) return TF_ERROR;
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

#define TABLE_WRITE(data, len) \
    do { \
        if (tf_buffer_write(out, (const uint8_t *)(data), (len)) != TF_OK) goto fail; \
    } while (0)

    /* Header row */
    TABLE_WRITE("| ", 2);
    for (size_t c = 0; c < st->n_cols; c++) {
        if (c > 0) TABLE_WRITE(" | ", 3);
        const char *name = st->col_names[c];
        size_t nlen = strlen(name);
        size_t w = widths[c];
        if (nlen > w) nlen = w;
        TABLE_WRITE(name, nlen);
        if (nlen < w && table_write_repeat(out, ' ', w - nlen) != TF_OK) goto fail;
    }
    TABLE_WRITE(" |\n", 3);

    /* Separator row */
    TABLE_WRITE("| ", 2);
    for (size_t c = 0; c < st->n_cols; c++) {
        if (c > 0) TABLE_WRITE(" | ", 3);
        if (table_write_repeat(out, '-', widths[c]) != TF_OK) goto fail;
    }
    TABLE_WRITE(" |\n", 3);

    /* Data rows */
    for (size_t r = 0; r < st->n_rows; r++) {
        TABLE_WRITE("| ", 2);
        for (size_t c = 0; c < st->n_cols; c++) {
            if (c > 0) TABLE_WRITE(" | ", 3);
            const char *val = st->values[r * st->n_cols + c];
            size_t vlen = strlen(val);
            size_t w = widths[c];
            if (vlen > w) vlen = w;
            TABLE_WRITE(val, vlen);
            if (vlen < w && table_write_repeat(out, ' ', w - vlen) != TF_OK) goto fail;
        }
        TABLE_WRITE(" |\n", 3);
    }

#undef TABLE_WRITE
    free(widths);
    return TF_OK;

fail:
#undef TABLE_WRITE
    free(widths);
    return TF_ERROR;
}

static void table_encoder_destroy(tf_encoder *self) {
    table_encoder_state *st = self->state;
    if (st) {
        if (st->col_names) {
            for (size_t i = 0; i < st->n_cols; i++) free(st->col_names[i]);
            free(st->col_names);
        }
        if (st->values) {
            size_t n_cells = 0;
            if (tf_size_mul(st->n_rows, st->n_cols, &n_cells) == TF_OK) {
                for (size_t i = 0; i < n_cells; i++) free(st->values[i]);
            }
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
        size_t parsed_size = 0;
        int has_max_width = tf_json_get_size_arg(args, "max_width",
                                                 1, TF_MAX_TABLE_WIDTH,
                                                 &parsed_size, "table");
        if (has_max_width < 0) { free(st); return NULL; }
        if (has_max_width > 0) st->max_width = parsed_size;

        int has_max_rows = tf_json_get_size_arg(args, "max_rows",
                                                0, TF_MAX_TABLE_ROWS,
                                                &parsed_size, "table");
        if (has_max_rows < 0) { free(st); return NULL; }
        if (has_max_rows > 0) st->max_rows = parsed_size;
    }

    tf_encoder *enc = malloc(sizeof(tf_encoder));
    if (!enc) { free(st); return NULL; }
    enc->encode = table_encode;
    enc->flush = table_flush;
    enc->destroy = table_encoder_destroy;
    enc->state = st;
    return enc;
}
