/*
 * op_top.c — Heap-backed bounded top/bottom N rows by column.
 *
 * Config: {"n": 10, "column": "score", "desc": true}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdio.h>
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
    return top_compare_cells(st->buf, a, st->buf, b, st->col_idx, st->desc) > 0;
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

    if (tf_batch_clone_schema(nb, old) != TF_OK) {
        tf_batch_free(nb);
        return TF_ERROR;
    }
    for (size_t r = 0; r < old->n_rows; r++) {
        if (tf_batch_copy_row(nb, r, old, r) != TF_OK) {
            tf_batch_free(nb);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(nb, r) != TF_OK) {
            tf_batch_free(nb);
            return TF_ERROR;
        }
    }

    st->buf = nb;
    tf_batch_free(old);
    top_heap_rebuild(st);
    st->replacements_since_compact = 0;
    return TF_OK;
}

static int top_init_schema(top_state *st, const tf_batch *in) {
    size_t cap = st->n > 0 ? st->n : 1;
    tf_batch *buf = tf_batch_create(in->n_cols, cap);
    if (!buf) return TF_ERROR;

    size_t *heap = NULL;
    if (st->n > 0) {
        heap = calloc(st->n, sizeof(size_t));
        if (!heap) {
            tf_batch_free(buf);
            return TF_ERROR;
        }
    }

    int col_idx = tf_batch_col_index(in, st->column);
    if (col_idx < 0) {
        char msg[256];
        snprintf(msg, sizeof(msg), "top: column '%s' not found", st->column ? st->column : "");
        tf_set_last_error(msg);
        free(heap);
        tf_batch_free(buf);
        return TF_ERROR;
    }

    if (tf_batch_clone_schema(buf, in) != TF_OK) {
        free(heap);
        tf_batch_free(buf);
        return TF_ERROR;
    }

    st->buf = buf;
    st->heap = heap;
    st->col_idx = col_idx;
    st->has_schema = 1;
    return TF_OK;
}

static int top_process(tf_step *self, tf_batch *in, tf_batch **out,
                       tf_side_channels *side) {
    (void)side;
    top_state *st = self->state;
    *out = NULL;

    if (!st->has_schema) {
        if (top_init_schema(st, in) != TF_OK) return TF_ERROR;
    }

    if (st->n == 0) return TF_OK;

    int ci = st->col_idx;
    for (size_t r = 0; r < in->n_rows; r++) {
        if (st->buf->n_rows < st->n) {
            size_t dst = st->buf->n_rows;
            if (tf_batch_copy_row(st->buf, dst, in, r) != TF_OK) return TF_ERROR;
            if (tf_batch_expose_row(st->buf, dst) != TF_OK) return TF_ERROR;
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

typedef struct {
    const tf_batch *batch;
    int col_idx;
    int desc;
} top_sort_ctx;

static int top_sort_cmp(const top_sort_ctx *ctx, size_t a, size_t b) {
    return top_compare_cells(ctx->batch, a, ctx->batch, b, ctx->col_idx, ctx->desc);
}

static int top_sort_cmp_index(const void *ctx, size_t a, size_t b) {
    return top_sort_cmp((const top_sort_ctx *)ctx, a, b);
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

    top_sort_ctx ctx = { .batch = st->buf, .col_idx = st->col_idx, .desc = st->desc };
    tf_sort_indices(indices, n, top_sort_cmp_index, &ctx);

    tf_batch *ob = tf_batch_create(st->buf->n_cols, n);
    if (!ob) { free(indices); return TF_ERROR; }
    if (tf_batch_clone_schema(ob, st->buf) != TF_OK) {
        free(indices);
        tf_batch_free(ob);
        return TF_ERROR;
    }
    for (size_t i = 0; i < n; i++) {
        if (tf_batch_copy_row(ob, i, st->buf, indices[i]) != TF_OK) {
            free(indices);
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, i) != TF_OK) {
            free(indices);
            tf_batch_free(ob);
            return TF_ERROR;
        }
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
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    size_t n = 0;
    int has_n = tf_json_get_size_arg(args, "n", 0, TF_MAX_OUTPUT_ROWS_PER_BATCH, &n, "top");
    if (has_n <= 0) return NULL;
    if (!cJSON_IsString(col_j) || !col_j->valuestring || col_j->valuestring[0] == '\0') {
        tf_set_last_error("top: column must be a non-empty string");
        return NULL;
    }

    top_state *st = calloc(1, sizeof(top_state));
    if (!st) return NULL;
    st->n = n;
    st->col_idx = -1;
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
        if (tf_json_add_bool(copy, "desc", default_desc) != TF_OK) {
            cJSON_Delete(copy);
            return NULL;
        }
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
