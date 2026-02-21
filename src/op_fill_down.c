/*
 * op_fill_down.c — Forward-fill nulls with last non-null value.
 *
 * Config: {"columns": ["city", "state"]}
 *   or {} for all columns.
 */

#include "internal.h"
#include "cJSON.h"
#include "date_utils.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char  **cols;
    size_t  n_cols;
    /* Last non-null value per column (stored as strings for simplicity) */
    char  **last_vals;
    int    *last_types; /* tf_type */
    int64_t *last_int;
    double  *last_float;
    int     *last_bool;
    int32_t *last_date;
    int64_t *last_timestamp;
    int      n_tracked;
    int      initialized;
} fill_down_state;

static int fill_down_process(tf_step *self, tf_batch *in, tf_batch **out,
                             tf_side_channels *side) {
    (void)side;
    fill_down_state *st = self->state;
    *out = NULL;

    if (!st->initialized) {
        st->n_tracked = (int)in->n_cols;
        st->last_vals = calloc(in->n_cols, sizeof(char *));
        st->last_types = calloc(in->n_cols, sizeof(int));
        st->last_int = calloc(in->n_cols, sizeof(int64_t));
        st->last_float = calloc(in->n_cols, sizeof(double));
        st->last_bool = calloc(in->n_cols, sizeof(int));
        st->last_date = calloc(in->n_cols, sizeof(int32_t));
        st->last_timestamp = calloc(in->n_cols, sizeof(int64_t));
        for (size_t c = 0; c < in->n_cols; c++) st->last_types[c] = -1; /* no value yet */
        st->initialized = 1;
    }

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);

        for (size_t c = 0; c < in->n_cols; c++) {
            /* Check if this column is targeted */
            int target = 0;
            if (st->n_cols == 0) {
                target = 1;
            } else {
                for (size_t k = 0; k < st->n_cols; k++) {
                    if (strcmp(in->col_names[c], st->cols[k]) == 0) { target = 1; break; }
                }
            }
            if (!target) continue;

            if (!tf_batch_is_null(in, r, c)) {
                /* Update last value */
                st->last_types[c] = (int)in->col_types[c];
                switch (in->col_types[c]) {
                    case TF_TYPE_STRING:
                        free(st->last_vals[c]);
                        st->last_vals[c] = strdup(tf_batch_get_string(in, r, c));
                        break;
                    case TF_TYPE_INT64:
                        st->last_int[c] = tf_batch_get_int64(in, r, c); break;
                    case TF_TYPE_FLOAT64:
                        st->last_float[c] = tf_batch_get_float64(in, r, c); break;
                    case TF_TYPE_BOOL:
                        st->last_bool[c] = tf_batch_get_bool(in, r, c); break;
                    case TF_TYPE_DATE:
                        st->last_date[c] = tf_batch_get_date(in, r, c); break;
                    case TF_TYPE_TIMESTAMP:
                        st->last_timestamp[c] = tf_batch_get_timestamp(in, r, c); break;
                    default: break;
                }
            } else if (st->last_types[c] >= 0) {
                /* Fill with last value */
                switch (in->col_types[c]) {
                    case TF_TYPE_STRING:
                        if (st->last_vals[c]) tf_batch_set_string(ob, r, c, st->last_vals[c]);
                        break;
                    case TF_TYPE_INT64:
                        tf_batch_set_int64(ob, r, c, st->last_int[c]); break;
                    case TF_TYPE_FLOAT64:
                        tf_batch_set_float64(ob, r, c, st->last_float[c]); break;
                    case TF_TYPE_BOOL:
                        tf_batch_set_bool(ob, r, c, st->last_bool[c]); break;
                    case TF_TYPE_DATE:
                        tf_batch_set_date(ob, r, c, st->last_date[c]); break;
                    case TF_TYPE_TIMESTAMP:
                        tf_batch_set_timestamp(ob, r, c, st->last_timestamp[c]); break;
                    default: break;
                }
            }
        }
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int fill_down_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void fill_down_destroy(tf_step *self) {
    fill_down_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_cols; i++) free(st->cols[i]);
        free(st->cols);
        if (st->last_vals) {
            for (int i = 0; i < st->n_tracked; i++) free(st->last_vals[i]);
            free(st->last_vals);
        }
        free(st->last_types); free(st->last_int);
        free(st->last_float); free(st->last_bool);
        free(st->last_date); free(st->last_timestamp);
        free(st);
    }
    free(self);
}

tf_step *tf_fill_down_create(const cJSON *args) {
    fill_down_state *st = calloc(1, sizeof(fill_down_state));
    if (!st) return NULL;

    if (args) {
        cJSON *columns = cJSON_GetObjectItemCaseSensitive(args, "columns");
        if (columns && cJSON_IsArray(columns)) {
            int n = cJSON_GetArraySize(columns);
            if (n > 0) {
                st->cols = calloc(n, sizeof(char *));
                st->n_cols = n;
                for (int i = 0; i < n; i++) {
                    cJSON *item = cJSON_GetArrayItem(columns, i);
                    if (cJSON_IsString(item)) st->cols[i] = strdup(item->valuestring);
                }
            }
        }
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st); return NULL; }
    step->process = fill_down_process;
    step->flush = fill_down_flush;
    step->destroy = fill_down_destroy;
    step->state = st;
    return step;
}
