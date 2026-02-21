/*
 * op_sort.c — Sort all rows by column(s). Requires buffering all data.
 *
 * Config: {"columns": [{"name": "age", "desc": false}, {"name": "name", "desc": true}]}
 * Buffers all incoming batches, sorts on flush, emits as a single batch.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char *name;
    int   desc;   /* 1 = descending */
} sort_col;

typedef struct {
    /* Accumulated data */
    tf_batch *buf;       /* growing buffer of all rows */
    int       has_schema;

    /* Sort spec */
    sort_col *cols;
    size_t    n_cols;
} sort_state;

/* Copy a row from src batch to dst batch */
static int copy_row(tf_batch *dst, size_t dst_row,
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

static int sort_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    sort_state *st = self->state;
    *out = NULL;

    /* Initialize buffer on first batch */
    if (!st->has_schema) {
        st->buf = tf_batch_create(in->n_cols, in->n_rows > 0 ? in->n_rows : 16);
        if (!st->buf) return TF_ERROR;
        for (size_t c = 0; c < in->n_cols; c++) {
            tf_batch_set_schema(st->buf, c, in->col_names[c], in->col_types[c]);
        }
        st->has_schema = 1;
    }

    /* Append all rows */
    for (size_t r = 0; r < in->n_rows; r++) {
        size_t dst_row = st->buf->n_rows;
        if (copy_row(st->buf, dst_row, in, r) != TF_OK) return TF_ERROR;
        st->buf->n_rows = dst_row + 1;
    }

    return TF_OK;
}

/* Comparator context for qsort_r / qsort */
typedef struct {
    const tf_batch *batch;
    int            *col_indices;
    int            *col_desc;
    size_t          n_sort_cols;
} sort_ctx;

static sort_ctx *g_sort_ctx; /* global for qsort (no qsort_r on all platforms) */

static int compare_rows(const void *a, const void *b) {
    size_t ra = *(const size_t *)a;
    size_t rb = *(const size_t *)b;
    const tf_batch *batch = g_sort_ctx->batch;

    for (size_t k = 0; k < g_sort_ctx->n_sort_cols; k++) {
        int ci = g_sort_ctx->col_indices[k];
        if (ci < 0) continue;

        int null_a = tf_batch_is_null(batch, ra, ci);
        int null_b = tf_batch_is_null(batch, rb, ci);

        /* Nulls sort last */
        if (null_a && null_b) continue;
        if (null_a) return 1;
        if (null_b) return -1;

        int cmp = 0;
        switch (batch->col_types[ci]) {
            case TF_TYPE_INT64: {
                int64_t va = tf_batch_get_int64(batch, ra, ci);
                int64_t vb = tf_batch_get_int64(batch, rb, ci);
                cmp = (va > vb) - (va < vb);
                break;
            }
            case TF_TYPE_FLOAT64: {
                double va = tf_batch_get_float64(batch, ra, ci);
                double vb = tf_batch_get_float64(batch, rb, ci);
                cmp = (va > vb) - (va < vb);
                break;
            }
            case TF_TYPE_STRING:
                cmp = strcmp(tf_batch_get_string(batch, ra, ci),
                             tf_batch_get_string(batch, rb, ci));
                break;
            case TF_TYPE_BOOL: {
                bool va = tf_batch_get_bool(batch, ra, ci);
                bool vb = tf_batch_get_bool(batch, rb, ci);
                cmp = (int)va - (int)vb;
                break;
            }
            case TF_TYPE_DATE: {
                int32_t va = tf_batch_get_date(batch, ra, ci);
                int32_t vb = tf_batch_get_date(batch, rb, ci);
                cmp = (va > vb) - (va < vb);
                break;
            }
            case TF_TYPE_TIMESTAMP: {
                int64_t va = tf_batch_get_timestamp(batch, ra, ci);
                int64_t vb = tf_batch_get_timestamp(batch, rb, ci);
                cmp = (va > vb) - (va < vb);
                break;
            }
            default:
                break;
        }

        if (cmp != 0) {
            return g_sort_ctx->col_desc[k] ? -cmp : cmp;
        }
    }
    return 0;
}

static int sort_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    sort_state *st = self->state;
    *out = NULL;

    if (!st->buf || st->buf->n_rows == 0) return TF_OK;

    size_t n = st->buf->n_rows;

    /* Resolve sort column indices */
    int *col_indices = malloc(st->n_cols * sizeof(int));
    int *col_desc = malloc(st->n_cols * sizeof(int));
    if (!col_indices || !col_desc) { free(col_indices); free(col_desc); return TF_ERROR; }

    for (size_t k = 0; k < st->n_cols; k++) {
        col_indices[k] = tf_batch_col_index(st->buf, st->cols[k].name);
        col_desc[k] = st->cols[k].desc;
    }

    /* Build index array */
    size_t *indices = malloc(n * sizeof(size_t));
    if (!indices) { free(col_indices); free(col_desc); return TF_ERROR; }
    for (size_t i = 0; i < n; i++) indices[i] = i;

    /* Sort */
    sort_ctx ctx = {
        .batch = st->buf,
        .col_indices = col_indices,
        .col_desc = col_desc,
        .n_sort_cols = st->n_cols,
    };
    g_sort_ctx = &ctx;
    qsort(indices, n, sizeof(size_t), compare_rows);
    g_sort_ctx = NULL;

    /* Build output batch in sorted order */
    tf_batch *ob = tf_batch_create(st->buf->n_cols, n);
    if (!ob) { free(indices); free(col_indices); free(col_desc); return TF_ERROR; }
    for (size_t c = 0; c < st->buf->n_cols; c++) {
        tf_batch_set_schema(ob, c, st->buf->col_names[c], st->buf->col_types[c]);
    }

    for (size_t i = 0; i < n; i++) {
        if (copy_row(ob, i, st->buf, indices[i]) != TF_OK) {
            free(indices); free(col_indices); free(col_desc);
            tf_batch_free(ob);
            return TF_ERROR;
        }
        ob->n_rows = i + 1;
    }

    free(indices);
    free(col_indices);
    free(col_desc);

    *out = ob;
    return TF_OK;
}

static void sort_destroy(tf_step *self) {
    sort_state *st = self->state;
    if (st) {
        if (st->buf) tf_batch_free(st->buf);
        for (size_t i = 0; i < st->n_cols; i++) free(st->cols[i].name);
        free(st->cols);
        free(st);
    }
    free(self);
}

tf_step *tf_sort_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *columns = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!columns || !cJSON_IsArray(columns)) return NULL;

    int n = cJSON_GetArraySize(columns);
    if (n <= 0) return NULL;

    sort_state *st = calloc(1, sizeof(sort_state));
    if (!st) return NULL;
    st->cols = calloc(n, sizeof(sort_col));
    if (!st->cols) { free(st); return NULL; }
    st->n_cols = n;

    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(columns, i);
        cJSON *name_j = cJSON_GetObjectItemCaseSensitive(item, "name");
        cJSON *desc_j = cJSON_GetObjectItemCaseSensitive(item, "desc");
        if (!cJSON_IsString(name_j)) {
            for (int j = 0; j < i; j++) free(st->cols[j].name);
            free(st->cols); free(st);
            return NULL;
        }
        st->cols[i].name = strdup(name_j->valuestring);
        st->cols[i].desc = (desc_j && cJSON_IsBool(desc_j)) ? cJSON_IsTrue(desc_j) : 0;
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) {
        for (int i = 0; i < n; i++) free(st->cols[i].name);
        free(st->cols); free(st);
        return NULL;
    }
    step->process = sort_process;
    step->flush = sort_flush;
    step->destroy = sort_destroy;
    step->state = st;
    return step;
}
