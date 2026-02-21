/*
 * op_top.c — Top N rows by column via sorted buffer.
 *
 * Config: {"n": 10, "column": "score", "desc": true}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    size_t    n;
    char     *column;
    int       desc;       /* 1 = highest first (default) */
    tf_batch *buf;
    int       has_schema;
    int       col_idx;
} top_state;

static double top_get_val(const tf_batch *b, size_t r, int ci) {
    if (tf_batch_is_null(b, r, ci)) return -1e308;
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    if (b->col_types[ci] == TF_TYPE_FLOAT64) return tf_batch_get_float64(b, r, ci);
    if (b->col_types[ci] == TF_TYPE_STRING) return 0; /* string comparison not supported for top */
    return 0;
}

static int top_process(tf_step *self, tf_batch *in, tf_batch **out,
                       tf_side_channels *side) {
    (void)side;
    top_state *st = self->state;
    *out = NULL;

    if (!st->has_schema) {
        st->buf = tf_batch_create(in->n_cols, st->n + 1);
        if (!st->buf) return TF_ERROR;
        for (size_t c = 0; c < in->n_cols; c++)
            tf_batch_set_schema(st->buf, c, in->col_names[c], in->col_types[c]);
        st->col_idx = tf_batch_col_index(in, st->column);
        st->has_schema = 1;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        double new_val = top_get_val(in, r, st->col_idx >= 0 ? st->col_idx : 0);

        if (st->buf->n_rows < st->n) {
            /* Buffer not full, just add */
            size_t dst = st->buf->n_rows;
            tf_batch_copy_row(st->buf, dst, in, r);
            st->buf->n_rows++;
        } else {
            /* Find the worst element */
            size_t worst_idx = 0;
            double worst_val = top_get_val(st->buf, 0, st->col_idx >= 0 ? st->col_idx : 0);
            for (size_t i = 1; i < st->buf->n_rows; i++) {
                double v = top_get_val(st->buf, i, st->col_idx >= 0 ? st->col_idx : 0);
                if (st->desc ? (v < worst_val) : (v > worst_val)) {
                    worst_val = v;
                    worst_idx = i;
                }
            }
            /* Replace worst if new value is better */
            int replace = st->desc ? (new_val > worst_val) : (new_val < worst_val);
            if (replace) {
                tf_batch_copy_row(st->buf, worst_idx, in, r);
            }
        }
    }

    return TF_OK;
}

/* Sort comparator context */
typedef struct { const tf_batch *batch; int col_idx; int desc; } top_sort_ctx;
static top_sort_ctx *g_top_ctx;

static int top_compare(const void *a, const void *b) {
    size_t ra = *(const size_t *)a;
    size_t rb = *(const size_t *)b;
    double va = top_get_val(g_top_ctx->batch, ra, g_top_ctx->col_idx);
    double vb = top_get_val(g_top_ctx->batch, rb, g_top_ctx->col_idx);
    int cmp = (va > vb) - (va < vb);
    return g_top_ctx->desc ? -cmp : cmp;
}

static int top_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    top_state *st = self->state;
    *out = NULL;

    if (!st->buf || st->buf->n_rows == 0) return TF_OK;

    /* Sort the buffer */
    size_t n = st->buf->n_rows;
    size_t *indices = malloc(n * sizeof(size_t));
    if (!indices) return TF_ERROR;
    for (size_t i = 0; i < n; i++) indices[i] = i;

    top_sort_ctx ctx = { .batch = st->buf, .col_idx = st->col_idx >= 0 ? st->col_idx : 0, .desc = st->desc };
    g_top_ctx = &ctx;
    qsort(indices, n, sizeof(size_t), top_compare);
    g_top_ctx = NULL;

    tf_batch *ob = tf_batch_create(st->buf->n_cols, n);
    if (!ob) { free(indices); return TF_ERROR; }
    for (size_t c = 0; c < st->buf->n_cols; c++)
        tf_batch_set_schema(ob, c, st->buf->col_names[c], st->buf->col_types[c]);
    for (size_t i = 0; i < n; i++) {
        tf_batch_copy_row(ob, i, st->buf, indices[i]);
        ob->n_rows = i + 1;
    }

    free(indices);
    *out = ob;
    return TF_OK;
}

static void top_destroy(tf_step *self) {
    top_state *st = self->state;
    if (st) {
        free(st->column);
        if (st->buf) tf_batch_free(st->buf);
        free(st);
    }
    free(self);
}

tf_step *tf_top_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *n_j = cJSON_GetObjectItemCaseSensitive(args, "n");
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsNumber(n_j) || !cJSON_IsString(col_j)) return NULL;

    top_state *st = calloc(1, sizeof(top_state));
    if (!st) return NULL;
    st->n = (size_t)n_j->valueint;
    st->column = strdup(col_j->valuestring);

    cJSON *desc_j = cJSON_GetObjectItemCaseSensitive(args, "desc");
    st->desc = (desc_j && cJSON_IsBool(desc_j)) ? cJSON_IsTrue(desc_j) : 1; /* default desc */

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->column); free(st); return NULL; }
    step->process = top_process;
    step->flush = top_flush;
    step->destroy = top_destroy;
    step->state = st;
    return step;
}
