/*
 * op_acf.c — Autocorrelation function.
 * Aggregate op: buffers all values, computes ACF for lags 0..N.
 * Output: 2-column table (lag, acf).
 *
 * Config: {"column": "price", "lags": 20}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

typedef struct {
    char   *column;
    int     lags;
    double *values;
    size_t  n_values;
    size_t  cap_values;
} acf_state;

static double get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    if (b->col_types[ci] == TF_TYPE_FLOAT64) return tf_batch_get_float64(b, r, ci);
    return 0;
}

static int acf_process(tf_step *self, tf_batch *in, tf_batch **out,
                       tf_side_channels *side) {
    (void)side;
    acf_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);
    if (ci < 0) return TF_OK;

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_is_null(in, r, ci)) continue;
        double val = get_numeric(in, r, ci);

        if (st->n_values >= st->cap_values) {
            size_t newcap = st->cap_values ? st->cap_values * 2 : 256;
            double *tmp = realloc(st->values, newcap * sizeof(double));
            if (!tmp) return TF_ERROR;
            st->values = tmp;
            st->cap_values = newcap;
        }
        st->values[st->n_values++] = val;
    }

    return TF_OK;
}

static int acf_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    acf_state *st = self->state;
    *out = NULL;

    size_t n = st->n_values;
    if (n < 2) return TF_OK;

    int max_lag = st->lags;
    if ((size_t)max_lag >= n) max_lag = (int)(n - 1);

    /* Compute mean */
    double mean = 0;
    for (size_t i = 0; i < n; i++) mean += st->values[i];
    mean /= (double)n;

    /* Compute variance (denominator for ACF) */
    double var = 0;
    for (size_t i = 0; i < n; i++) {
        double d = st->values[i] - mean;
        var += d * d;
    }
    if (var == 0) return TF_OK;

    /* Output: lag+1 rows (lag 0 to max_lag) */
    size_t out_rows = (size_t)(max_lag + 1);
    tf_batch *ob = tf_batch_create(2, out_rows);
    if (!ob) return TF_ERROR;
    tf_batch_set_schema(ob, 0, "lag", TF_TYPE_INT64);
    tf_batch_set_schema(ob, 1, "acf", TF_TYPE_FLOAT64);

    for (int k = 0; k <= max_lag; k++) {
        double cov = 0;
        for (size_t i = 0; i < n - (size_t)k; i++) {
            cov += (st->values[i] - mean) * (st->values[i + k] - mean);
        }
        double acf_val = cov / var;

        tf_batch_set_int64(ob, k, 0, k);
        tf_batch_set_float64(ob, k, 1, acf_val);
        ob->n_rows = k + 1;
    }

    *out = ob;
    return TF_OK;
}

static void acf_destroy(tf_step *self) {
    acf_state *st = self->state;
    if (st) {
        free(st->column);
        free(st->values);
        free(st);
    }
    free(self);
}

tf_step *tf_acf_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j)) return NULL;

    acf_state *st = calloc(1, sizeof(acf_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);

    cJSON *lags_j = cJSON_GetObjectItemCaseSensitive(args, "lags");
    st->lags = cJSON_IsNumber(lags_j) ? lags_j->valueint : 20;
    if (st->lags < 1) st->lags = 1;

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->column); free(st); return NULL; }
    step->process = acf_process;
    step->flush = acf_flush;
    step->destroy = acf_destroy;
    step->state = st;
    return step;
}
