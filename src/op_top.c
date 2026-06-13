/*
 * op_top.c — Heap-backed bounded top/bottom N rows by column.
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
    size_t   *heap;       /* row indices in buf; root is worst retained row */
    size_t    heap_len;
    size_t    replacements_since_compact;
    int       has_schema;
    int       col_idx;
} top_state;

static int top_compare_cells(const tf_batch *a, size_t ra,
                             const tf_batch *b, size_t rb,
                             int ci, int desc) {
    int null_a = tf_batch_is_null(a, ra, ci);
    int null_b = tf_batch_is_null(b, rb, ci);

    /* Match sort semantics: nulls rank last for both ascending and descending. */
    if (null_a && null_b) return 0;
    if (null_a) return 1;
    if (null_b) return -1;

    int cmp = 0;
    switch (a->col_types[ci]) {
        case TF_TYPE_BOOL: {
            bool va = tf_batch_get_bool(a, ra, ci);
            bool vb = tf_batch_get_bool(b, rb, ci);
            cmp = (int)va - (int)vb;
            break;
        }
        case TF_TYPE_INT64: {
            int64_t va = tf_batch_get_int64(a, ra, ci);
            int64_t vb = tf_batch_get_int64(b, rb, ci);
            cmp = (va > vb) - (va < vb);
            break;
        }
        case TF_TYPE_FLOAT64: {
            double va = tf_batch_get_float64(a, ra, ci);
            double vb = tf_batch_get_float64(b, rb, ci);
            cmp = (va > vb) - (va < vb);
            break;
        }
        case TF_TYPE_STRING:
            cmp = strcmp(tf_batch_get_string(a, ra, ci),
                         tf_batch_get_string(b, rb, ci));
            break;
        case TF_TYPE_DATE: {
            int32_t va = tf_batch_get_date(a, ra, ci);
            int32_t vb = tf_batch_get_date(b, rb, ci);
            cmp = (va > vb) - (va < vb);
            break;
        }
        case TF_TYPE_TIMESTAMP: {
            int64_t va = tf_batch_get_timestamp(a, ra, ci);
            int64_t vb = tf_batch_get_timestamp(b, rb, ci);
            cmp = (va > vb) - (va < vb);
            break;
        }
        default:
            break;
    }

    return desc ? -cmp : cmp;
}

static int top_heap_worse(const top_state *st, size_t a, size_t b) {
    int ci = st->col_idx >= 0 ? st->col_idx : 0;
    return top_compare_cells(st->buf, a, st->buf, b, ci, st->desc) > 0;
}

static void top_heap_swap(size_t *a, size_t *b) {
    size_t tmp = *a;
    *a = *b;
    *b = tmp;
}

static void top_heap_sift_up(top_state *st, size_t pos) {
    while (pos > 0) {
        size_t parent = (pos - 1) / 2;
        if (!top_heap_worse(st, st->heap[pos], st->heap[parent])) break;
        top_heap_swap(&st->heap[pos], &st->heap[parent]);
        pos = parent;
    }
}

static void top_heap_sift_down(top_state *st, size_t pos) {
    for (;;) {
        size_t left = pos * 2 + 1;
        size_t right = left + 1;
        size_t worst = pos;

        if (left < st->heap_len && top_heap_worse(st, st->heap[left], st->heap[worst])) {
            worst = left;
        }
        if (right < st->heap_len && top_heap_worse(st, st->heap[right], st->heap[worst])) {
            worst = right;
        }
        if (worst == pos) break;
        top_heap_swap(&st->heap[pos], &st->heap[worst]);
        pos = worst;
    }
}

static void top_heap_push(top_state *st, size_t row) {
    st->heap[st->heap_len] = row;
    top_heap_sift_up(st, st->heap_len);
    st->heap_len++;
}

static void top_heap_rebuild(top_state *st) {
    st->heap_len = 0;
    for (size_t r = 0; r < st->buf->n_rows; r++) {
        top_heap_push(st, r);
    }
}

static int top_compact_buffer(top_state *st) {
    if (!st->buf) return TF_OK;

    tf_batch *old = st->buf;
    size_t cap = st->n > 0 ? st->n : 1;
    tf_batch *nb = tf_batch_create(old->n_cols, cap);
    if (!nb) return TF_ERROR;

    for (size_t c = 0; c < old->n_cols; c++) {
        if (tf_batch_set_schema(nb, c, old->col_names[c], old->col_types[c]) != TF_OK) {
            tf_batch_free(nb);
            return TF_ERROR;
        }
    }
    for (size_t r = 0; r < old->n_rows; r++) {
        if (tf_batch_copy_row(nb, r, old, r) != TF_OK) {
            tf_batch_free(nb);
            return TF_ERROR;
        }
        nb->n_rows = r + 1;
    }

    st->buf = nb;
    tf_batch_free(old);
    top_heap_rebuild(st);
    st->replacements_since_compact = 0;
    return TF_OK;
}

static int top_process(tf_step *self, tf_batch *in, tf_batch **out,
                       tf_side_channels *side) {
    (void)side;
    top_state *st = self->state;
    *out = NULL;

    if (!st->has_schema) {
        size_t cap = st->n > 0 ? st->n : 1;
        st->buf = tf_batch_create(in->n_cols, cap);
        if (!st->buf) return TF_ERROR;
        if (st->n > 0) {
            st->heap = calloc(st->n, sizeof(size_t));
            if (!st->heap) return TF_ERROR;
        }
        for (size_t c = 0; c < in->n_cols; c++)
            tf_batch_set_schema(st->buf, c, in->col_names[c], in->col_types[c]);
        st->col_idx = tf_batch_col_index(in, st->column);
        st->has_schema = 1;
    }

    if (st->n == 0) return TF_OK;

    int ci = st->col_idx >= 0 ? st->col_idx : 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        if (st->buf->n_rows < st->n) {
            size_t dst = st->buf->n_rows;
            if (tf_batch_copy_row(st->buf, dst, in, r) != TF_OK) return TF_ERROR;
            st->buf->n_rows++;
            top_heap_push(st, dst);
        } else {
            size_t worst_idx = st->heap[0];
            if (top_compare_cells(in, r, st->buf, worst_idx, ci, st->desc) < 0) {
                if (tf_batch_copy_row(st->buf, worst_idx, in, r) != TF_OK) return TF_ERROR;
                top_heap_sift_down(st, 0);
                st->replacements_since_compact++;
                size_t compact_every = st->n < 4096 ? st->n : 4096;
                if (compact_every > 0 && st->replacements_since_compact >= compact_every) {
                    if (top_compact_buffer(st) != TF_OK) return TF_ERROR;
                }
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
    return top_compare_cells(g_top_ctx->batch, ra, g_top_ctx->batch, rb,
                             g_top_ctx->col_idx, g_top_ctx->desc);
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
        free(st->heap);
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
    if (n_j->valueint < 0) return NULL;

    top_state *st = calloc(1, sizeof(top_state));
    if (!st) return NULL;
    st->n = (size_t)n_j->valueint;
    st->column = strdup(col_j->valuestring);
    if (!st->column) { free(st); return NULL; }

    cJSON *desc_j = cJSON_GetObjectItemCaseSensitive(args, "desc");
    st->desc = (desc_j && cJSON_IsBool(desc_j)) ? cJSON_IsTrue(desc_j) : 1; /* default desc */

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st->column); free(st); return NULL; }
    step->process = top_process;
    step->flush = top_flush;
    step->destroy = top_destroy;
    step->state = st;
    return step;
}

static tf_step *top_create_with_default_desc(const cJSON *args, int default_desc) {
    if (!args) return NULL;
    cJSON *copy = cJSON_Duplicate(args, 1);
    if (!copy) return NULL;
    if (!cJSON_GetObjectItemCaseSensitive(copy, "desc")) {
        cJSON_AddBoolToObject(copy, "desc", default_desc);
    }
    tf_step *step = tf_top_create(copy);
    cJSON_Delete(copy);
    return step;
}

tf_step *tf_top_k_create(const cJSON *args) {
    return top_create_with_default_desc(args, 1);
}

tf_step *tf_bottom_k_create(const cJSON *args) {
    return top_create_with_default_desc(args, 0);
}

tf_step *tf_slice_min_create(const cJSON *args) {
    return top_create_with_default_desc(args, 0);
}

tf_step *tf_slice_max_create(const cJSON *args) {
    return top_create_with_default_desc(args, 1);
}
