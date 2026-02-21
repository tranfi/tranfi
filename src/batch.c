/*
 * batch.c — Columnar batch: typed columns with per-cell null tracking.
 */

#include "internal.h"
#include <stdlib.h>
#include <string.h>

static size_t type_size(tf_type t) {
    switch (t) {
        case TF_TYPE_BOOL:      return sizeof(uint8_t);
        case TF_TYPE_INT64:     return sizeof(int64_t);
        case TF_TYPE_FLOAT64:   return sizeof(double);
        case TF_TYPE_STRING:    return sizeof(char *);
        case TF_TYPE_DATE:      return sizeof(int32_t);
        case TF_TYPE_TIMESTAMP: return sizeof(int64_t);
        default:                return 0;
    }
}

tf_batch *tf_batch_create(size_t n_cols, size_t capacity) {
    tf_arena *arena = tf_arena_create(0);
    if (!arena) return NULL;

    tf_batch *b = tf_arena_alloc(arena, sizeof(tf_batch));
    if (!b) { tf_arena_free(arena); return NULL; }

    b->arena = arena;
    b->n_cols = n_cols;
    b->n_rows = 0;
    b->capacity = capacity;
    b->col_names = tf_arena_alloc(arena, n_cols * sizeof(char *));
    b->col_types = tf_arena_alloc(arena, n_cols * sizeof(tf_type));
    b->columns = tf_arena_alloc(arena, n_cols * sizeof(void *));
    b->nulls = tf_arena_alloc(arena, n_cols * sizeof(uint8_t *));

    if (!b->col_names || !b->col_types || !b->columns || !b->nulls) {
        tf_arena_free(arena);
        return NULL;
    }

    for (size_t i = 0; i < n_cols; i++) {
        b->col_names[i] = NULL;
        b->col_types[i] = TF_TYPE_NULL;
        b->columns[i] = NULL;
        b->nulls[i] = NULL;
    }

    return b;
}

int tf_batch_set_schema(tf_batch *b, size_t col, const char *name, tf_type type) {
    if (col >= b->n_cols) return TF_ERROR;
    b->col_names[col] = tf_arena_strdup(b->arena, name);
    b->col_types[col] = type;

    /* Allocate column storage */
    size_t sz = type_size(type);
    if (sz > 0 && b->capacity > 0) {
        b->columns[col] = tf_arena_alloc(b->arena, sz * b->capacity);
        b->nulls[col] = tf_arena_alloc(b->arena, b->capacity);
        if (!b->columns[col] || !b->nulls[col]) return TF_ERROR;
        memset(b->nulls[col], 1, b->capacity); /* all null by default */
    }
    return TF_OK;
}

int tf_batch_ensure_capacity(tf_batch *b, size_t min_rows) {
    if (min_rows <= b->capacity) return TF_OK;

    /* We need to reallocate columns — but arena doesn't support realloc.
     * Allocate new larger arrays and copy. */
    size_t new_cap = b->capacity ? b->capacity : 16;
    while (new_cap < min_rows) new_cap *= 2;

    for (size_t i = 0; i < b->n_cols; i++) {
        size_t sz = type_size(b->col_types[i]);
        if (sz == 0) continue;

        void *new_col = tf_arena_alloc(b->arena, sz * new_cap);
        uint8_t *new_null = tf_arena_alloc(b->arena, new_cap);
        if (!new_col || !new_null) return TF_ERROR;

        if (b->columns[i] && b->n_rows > 0) {
            memcpy(new_col, b->columns[i], sz * b->n_rows);
            memcpy(new_null, b->nulls[i], b->n_rows);
        }
        memset(new_null + b->n_rows, 1, new_cap - b->n_rows);
        b->columns[i] = new_col;
        b->nulls[i] = new_null;
    }
    b->capacity = new_cap;
    return TF_OK;
}

/* ---- Setters ---- */

void tf_batch_set_null(tf_batch *b, size_t row, size_t col) {
    if (row < b->capacity && col < b->n_cols && b->nulls[col])
        b->nulls[col][row] = 1;
}

void tf_batch_set_bool(tf_batch *b, size_t row, size_t col, bool val) {
    if (row < b->capacity && col < b->n_cols && b->col_types[col] == TF_TYPE_BOOL) {
        ((uint8_t *)b->columns[col])[row] = val ? 1 : 0;
        b->nulls[col][row] = 0;
    }
}

void tf_batch_set_int64(tf_batch *b, size_t row, size_t col, int64_t val) {
    if (row < b->capacity && col < b->n_cols && b->col_types[col] == TF_TYPE_INT64) {
        ((int64_t *)b->columns[col])[row] = val;
        b->nulls[col][row] = 0;
    }
}

void tf_batch_set_float64(tf_batch *b, size_t row, size_t col, double val) {
    if (row < b->capacity && col < b->n_cols && b->col_types[col] == TF_TYPE_FLOAT64) {
        ((double *)b->columns[col])[row] = val;
        b->nulls[col][row] = 0;
    }
}

void tf_batch_set_string(tf_batch *b, size_t row, size_t col, const char *val) {
    if (row < b->capacity && col < b->n_cols && b->col_types[col] == TF_TYPE_STRING) {
        ((char **)b->columns[col])[row] = tf_arena_strdup(b->arena, val);
        b->nulls[col][row] = 0;
    }
}

void tf_batch_set_date(tf_batch *b, size_t row, size_t col, int32_t val) {
    if (row < b->capacity && col < b->n_cols && b->col_types[col] == TF_TYPE_DATE) {
        ((int32_t *)b->columns[col])[row] = val;
        b->nulls[col][row] = 0;
    }
}

void tf_batch_set_timestamp(tf_batch *b, size_t row, size_t col, int64_t val) {
    if (row < b->capacity && col < b->n_cols && b->col_types[col] == TF_TYPE_TIMESTAMP) {
        ((int64_t *)b->columns[col])[row] = val;
        b->nulls[col][row] = 0;
    }
}

/* ---- Getters ---- */

bool tf_batch_is_null(const tf_batch *b, size_t row, size_t col) {
    if (row >= b->n_rows || col >= b->n_cols) return true;
    return b->nulls[col][row] != 0;
}

bool tf_batch_get_bool(const tf_batch *b, size_t row, size_t col) {
    if (row >= b->n_rows || col >= b->n_cols) return false;
    return ((uint8_t *)b->columns[col])[row] != 0;
}

int64_t tf_batch_get_int64(const tf_batch *b, size_t row, size_t col) {
    if (row >= b->n_rows || col >= b->n_cols) return 0;
    return ((int64_t *)b->columns[col])[row];
}

double tf_batch_get_float64(const tf_batch *b, size_t row, size_t col) {
    if (row >= b->n_rows || col >= b->n_cols) return 0.0;
    return ((double *)b->columns[col])[row];
}

const char *tf_batch_get_string(const tf_batch *b, size_t row, size_t col) {
    if (row >= b->n_rows || col >= b->n_cols) return NULL;
    return ((char **)b->columns[col])[row];
}

int32_t tf_batch_get_date(const tf_batch *b, size_t row, size_t col) {
    if (row >= b->n_rows || col >= b->n_cols) return 0;
    return ((int32_t *)b->columns[col])[row];
}

int64_t tf_batch_get_timestamp(const tf_batch *b, size_t row, size_t col) {
    if (row >= b->n_rows || col >= b->n_cols) return 0;
    return ((int64_t *)b->columns[col])[row];
}

int tf_batch_copy_row(tf_batch *dst, size_t dst_row,
                      const tf_batch *src, size_t src_row) {
    if (tf_batch_ensure_capacity(dst, dst_row + 1) != TF_OK) return TF_ERROR;
    for (size_t c = 0; c < src->n_cols; c++) {
        if (tf_batch_is_null(src, src_row, c)) {
            tf_batch_set_null(dst, dst_row, c);
            continue;
        }
        switch (src->col_types[c]) {
            case TF_TYPE_BOOL:
                tf_batch_set_bool(dst, dst_row, c, tf_batch_get_bool(src, src_row, c));
                break;
            case TF_TYPE_INT64:
                tf_batch_set_int64(dst, dst_row, c, tf_batch_get_int64(src, src_row, c));
                break;
            case TF_TYPE_FLOAT64:
                tf_batch_set_float64(dst, dst_row, c, tf_batch_get_float64(src, src_row, c));
                break;
            case TF_TYPE_STRING:
                tf_batch_set_string(dst, dst_row, c, tf_batch_get_string(src, src_row, c));
                break;
            case TF_TYPE_DATE:
                tf_batch_set_date(dst, dst_row, c, tf_batch_get_date(src, src_row, c));
                break;
            case TF_TYPE_TIMESTAMP:
                tf_batch_set_timestamp(dst, dst_row, c, tf_batch_get_timestamp(src, src_row, c));
                break;
            default:
                tf_batch_set_null(dst, dst_row, c);
                break;
        }
    }
    return TF_OK;
}

int tf_batch_col_index(const tf_batch *b, const char *name) {
    for (size_t i = 0; i < b->n_cols; i++) {
        if (b->col_names[i] && strcmp(b->col_names[i], name) == 0)
            return (int)i;
    }
    return -1;
}

void tf_batch_free(tf_batch *b) {
    if (!b) return;
    tf_arena *arena = b->arena;
    tf_arena_free(arena);
}
