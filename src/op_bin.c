/*
 * op_bin.c — Discretize numeric column into labeled bins.
 *
 * Config: {"column": "age", "boundaries": [18, 30, 50, 65]}
 * Adds <col>_bin column with labels like "<18", "18-30", "30-50", "50-65", "65+"
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char   *column;
    double *boundaries;
    size_t  n_boundaries;
} bin_state;

static int bin_process(tf_step *self, tf_batch *in, tf_batch **out,
                       tf_side_channels *side) {
    (void)side;
    bin_state *st = self->state;
    *out = NULL;

    char bin_col_name[256];
    snprintf(bin_col_name, sizeof(bin_col_name), "%s_bin", st->column);

    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    tf_batch_set_schema(ob, in->n_cols, bin_col_name, TF_TYPE_STRING);

    int ci = tf_batch_col_index(in, st->column);

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);

        if (ci < 0 || tf_batch_is_null(in, r, ci)) {
            tf_batch_set_null(ob, r, in->n_cols);
        } else {
            double val = 0;
            if (in->col_types[ci] == TF_TYPE_INT64) val = (double)tf_batch_get_int64(in, r, ci);
            else if (in->col_types[ci] == TF_TYPE_FLOAT64) val = tf_batch_get_float64(in, r, ci);

            char label[64];
            if (st->n_boundaries == 0) {
                snprintf(label, sizeof(label), "%g", val);
            } else if (val < st->boundaries[0]) {
                snprintf(label, sizeof(label), "<%g", st->boundaries[0]);
            } else {
                size_t b;
                for (b = 1; b < st->n_boundaries; b++) {
                    if (val < st->boundaries[b]) break;
                }
                if (b < st->n_boundaries) {
                    snprintf(label, sizeof(label), "%g-%g", st->boundaries[b - 1], st->boundaries[b]);
                } else {
                    snprintf(label, sizeof(label), "%g+", st->boundaries[st->n_boundaries - 1]);
                }
            }
            tf_batch_set_string(ob, r, in->n_cols, label);
        }
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int bin_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void bin_destroy(tf_step *self) {
    bin_state *st = self->state;
    if (st) { free(st->column); free(st->boundaries); free(st); }
    free(self);
}

tf_step *tf_bin_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j)) return NULL;

    bin_state *st = calloc(1, sizeof(bin_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);

    cJSON *bounds = cJSON_GetObjectItemCaseSensitive(args, "boundaries");
    if (bounds && cJSON_IsArray(bounds)) {
        int n = cJSON_GetArraySize(bounds);
        st->boundaries = malloc(n * sizeof(double));
        st->n_boundaries = n;
        for (int i = 0; i < n; i++) {
            cJSON *item = cJSON_GetArrayItem(bounds, i);
            st->boundaries[i] = cJSON_IsNumber(item) ? item->valuedouble : 0;
        }
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->column); free(st->boundaries); free(st); return NULL; }
    step->process = bin_process;
    step->flush = bin_flush;
    step->destroy = bin_destroy;
    step->state = st;
    return step;
}
