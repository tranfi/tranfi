/*
 * op_ewma.c — Exponentially weighted moving average.
 *
 * Config: {"column": "price", "alpha": 0.3, "result": "price_ewma"}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char   *column;
    char   *result;
    double  alpha;
    double  ewma;
    int     initialized;
} ewma_state;

static double get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    if (b->col_types[ci] == TF_TYPE_FLOAT64) return tf_batch_get_float64(b, r, ci);
    return 0;
}

static int ewma_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    ewma_state *st = self->state;
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
        if (!st->initialized) {
            st->ewma = val;
            st->initialized = 1;
        } else {
            st->ewma = st->alpha * val + (1.0 - st->alpha) * st->ewma;
        }

        tf_batch_set_float64(ob, r, in->n_cols, st->ewma);
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int ewma_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void ewma_destroy(tf_step *self) {
    ewma_state *st = self->state;
    if (st) { free(st->column); free(st->result); free(st); }
    free(self);
}

tf_step *tf_ewma_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    cJSON *alpha_j = cJSON_GetObjectItemCaseSensitive(args, "alpha");
    if (!cJSON_IsString(col_j) || !cJSON_IsNumber(alpha_j)) return NULL;

    ewma_state *st = calloc(1, sizeof(ewma_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    st->alpha = alpha_j->valuedouble;

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = strdup(res_j->valuestring);
    } else {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s_ewma", st->column);
        st->result = strdup(buf);
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->column); free(st->result); free(st); return NULL; }
    step->process = ewma_process;
    step->flush = ewma_flush;
    step->destroy = ewma_destroy;
    step->state = st;
    return step;
}
