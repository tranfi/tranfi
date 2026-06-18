/*
 * op_diff.c — First (or higher-order) differencing.
 *
 * Config: {"column": "price", "order": 1, "result": "price_diff"}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define MAX_DIFF_ORDER 8

typedef struct {
    char   *column;
    char   *result;
    int     order;
    double  prev[MAX_DIFF_ORDER]; /* circular buffer of previous values */
    int     count; /* rows seen so far */
} diff_state;

static double get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    if (b->col_types[ci] == TF_TYPE_FLOAT64) return tf_batch_get_float64(b, r, ci);
    return 0;
}

static int diff_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    diff_state *st = self->state;
    *out = NULL;

    const char *extra_names[1] = {st->result};
    tf_type extra_types[1] = {TF_TYPE_FLOAT64};
    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_with_extra_cols(ob, in, extra_names, extra_types, 1) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    int ci = tf_batch_col_index(in, st->column);

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }

        if (ci < 0 || tf_batch_is_null(in, r, ci)) {
            if (tf_batch_set_null(ob, r, in->n_cols) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            if (tf_batch_expose_row(ob, r) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            continue;
        }

        double val = get_numeric(in, r, ci);

        if (st->count < st->order) {
            /* Not enough history yet -- shift and insert at front
             * so prev[0] is always the most recent value */
            if (tf_batch_set_null(ob, r, in->n_cols) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            if (tf_batch_expose_row(ob, r) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            for (int k = st->count; k > 0; k--)
                st->prev[k] = st->prev[k - 1];
            st->prev[0] = val;
            st->count++;
            continue;
        }

        /* Compute difference using binomial coefficients:
         * diff(order=1): val - prev[0]
         * diff(order=2): val - 2*prev[0] + prev[1]
         * General: sum_{k=0}^{order} (-1)^k * C(order,k) * x_{n-k}
         */
        double result = 0;
        int binom = 1;
        int sign = 1;
        /* x_n (current value) */
        result = val;
        /* x_{n-1} ... x_{n-order} from prev buffer (most recent first) */
        for (int k = 1; k <= st->order; k++) {
            binom = binom * (st->order - k + 1) / k;
            sign = -sign;
            /* prev[0] is the most recent previous value */
            result += sign * binom * st->prev[k - 1];
        }

        if (tf_batch_set_float64(ob, r, in->n_cols, result) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }

        /* Shift prev buffer: move everything down, put val at [0] */
        for (int k = st->order - 1; k > 0; k--)
            st->prev[k] = st->prev[k - 1];
        st->prev[0] = val;
    }

    *out = ob;
    return TF_OK;
}

static int diff_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void diff_destroy(tf_step *self) {
    diff_state *st = self->state;
    if (st) { free(st->column); free(st->result); free(st); }
    free(self);
}

tf_step *tf_diff_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j)) return NULL;

    diff_state *st = tf_callocarray_checked(1, sizeof(diff_state));
    if (!st) return NULL;
    st->column = tf_strdup_checked(col_j->valuestring);
    if (!st->column) { free(st); return NULL; }

    size_t order = 1;
    int has_order = tf_json_get_size_arg(args, "order", 1, MAX_DIFF_ORDER, &order, "diff");
    if (has_order < 0) { free(st->column); free(st); return NULL; }
    st->order = (int)order;

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = tf_strdup_checked(res_j->valuestring);
    } else {
        st->result = tf_string_append_suffix_checked(st->column, "_diff");
    }
    if (!st->result) { free(st->column); free(st); return NULL; }

    tf_step *step = tf_callocarray_checked(1, sizeof(tf_step));
    if (!step) { free(st->column); free(st->result); free(st); return NULL; }
    step->process = diff_process;
    step->flush = diff_flush;
    step->destroy = diff_destroy;
    step->state = st;
    return step;
}
