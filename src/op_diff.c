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

    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    tf_batch_set_schema(ob, in->n_cols, st->result, TF_TYPE_FLOAT64);

    int ci = tf_batch_col_index(in, st->column);

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);

        if (ci < 0 || tf_batch_is_null(in, r, ci)) {
            tf_batch_set_null(ob, r, in->n_cols);
            ob->n_rows = r + 1;
            continue;
        }

        double val = get_numeric(in, r, ci);

        if (st->count < st->order) {
            /* Not enough history yet */
            st->prev[st->count] = val;
            st->count++;
            tf_batch_set_null(ob, r, in->n_cols);
            ob->n_rows = r + 1;
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

        tf_batch_set_float64(ob, r, in->n_cols, result);
        ob->n_rows = r + 1;

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

    diff_state *st = calloc(1, sizeof(diff_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);

    cJSON *order_j = cJSON_GetObjectItemCaseSensitive(args, "order");
    st->order = (cJSON_IsNumber(order_j) && order_j->valueint > 0) ?
                order_j->valueint : 1;
    if (st->order > MAX_DIFF_ORDER) st->order = MAX_DIFF_ORDER;

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = strdup(res_j->valuestring);
    } else {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s_diff", st->column);
        st->result = strdup(buf);
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->column); free(st->result); free(st); return NULL; }
    step->process = diff_process;
    step->flush = diff_flush;
    step->destroy = diff_destroy;
    step->state = st;
    return step;
}
