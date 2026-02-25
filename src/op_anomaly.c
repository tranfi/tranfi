/*
 * op_anomaly.c — Streaming anomaly detection via z-score.
 * Uses Welford's online algorithm for mean/variance.
 *
 * Config: {"column": "price", "threshold": 3.0, "result": "price_anomaly"}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

typedef struct {
    char   *column;
    char   *result;
    double  threshold;
    /* Welford's online algorithm */
    size_t  count;
    double  mean;
    double  m2;
} anomaly_state;

static double get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    if (b->col_types[ci] == TF_TYPE_FLOAT64) return tf_batch_get_float64(b, r, ci);
    return 0;
}

static int anomaly_process(tf_step *self, tf_batch *in, tf_batch **out,
                           tf_side_channels *side) {
    (void)side;
    anomaly_state *st = self->state;
    *out = NULL;

    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    tf_batch_set_schema(ob, in->n_cols, st->result, TF_TYPE_INT64);

    int ci = tf_batch_col_index(in, st->column);

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);

        if (ci < 0 || tf_batch_is_null(in, r, ci)) {
            tf_batch_set_int64(ob, r, in->n_cols, 0);
            ob->n_rows = r + 1;
            continue;
        }

        double val = get_numeric(in, r, ci);

        /* Update Welford's */
        st->count++;
        double delta = val - st->mean;
        st->mean += delta / (double)st->count;
        double delta2 = val - st->mean;
        st->m2 += delta * delta2;

        /* Compute z-score (need at least 2 data points for variance) */
        int is_anomaly = 0;
        if (st->count >= 2) {
            double var = st->m2 / (double)(st->count - 1);
            double std = sqrt(var);
            if (std > 0) {
                double z = fabs((val - st->mean) / std);
                is_anomaly = z > st->threshold ? 1 : 0;
            }
        }

        tf_batch_set_int64(ob, r, in->n_cols, is_anomaly);
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int anomaly_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void anomaly_destroy(tf_step *self) {
    anomaly_state *st = self->state;
    if (st) { free(st->column); free(st->result); free(st); }
    free(self);
}

tf_step *tf_anomaly_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j)) return NULL;

    anomaly_state *st = calloc(1, sizeof(anomaly_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);

    cJSON *thresh_j = cJSON_GetObjectItemCaseSensitive(args, "threshold");
    st->threshold = cJSON_IsNumber(thresh_j) ? thresh_j->valuedouble : 3.0;

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = strdup(res_j->valuestring);
    } else {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s_anomaly", st->column);
        st->result = strdup(buf);
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->column); free(st->result); free(st); return NULL; }
    step->process = anomaly_process;
    step->flush = anomaly_flush;
    step->destroy = anomaly_destroy;
    step->state = st;
    return step;
}
