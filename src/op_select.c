/*
 * op_select.c — Select and reorder columns.
 *
 * Config: {"columns": ["name", "age"]}
 * Creates output batch with only the specified columns in the given order.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char  **col_names;
    size_t  n_cols;
} select_state;

static int select_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    select_state *st = self->state;
    *out = NULL;

    /* Resolve column indices */
    int *indices = malloc(st->n_cols * sizeof(int));
    if (!indices) return TF_ERROR;

    for (size_t i = 0; i < st->n_cols; i++) {
        indices[i] = tf_batch_col_index(in, st->col_names[i]);
        if (indices[i] < 0 && side && side->errors) {
            char buf[128];
            snprintf(buf, sizeof(buf),
                     "{\"op\":\"select\",\"error\":\"column '%s' not found\"}\n",
                     st->col_names[i]);
            tf_buffer_write_str(side->errors, buf);
        }
    }

    /* Create output batch */
    tf_batch *ob = tf_batch_create(st->n_cols, in->n_rows);
    if (!ob) { free(indices); return TF_ERROR; }

    for (size_t i = 0; i < st->n_cols; i++) {
        if (indices[i] >= 0) {
            tf_batch_set_schema(ob, i, st->col_names[i], in->col_types[indices[i]]);
        } else {
            tf_batch_set_schema(ob, i, st->col_names[i], TF_TYPE_NULL);
        }
    }

    /* Copy rows */
    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_ensure_capacity(ob, r + 1);
        for (size_t i = 0; i < st->n_cols; i++) {
            int ci = indices[i];
            if (ci < 0 || tf_batch_is_null(in, r, ci)) {
                tf_batch_set_null(ob, r, i);
                continue;
            }
            switch (in->col_types[ci]) {
                case TF_TYPE_BOOL:
                    tf_batch_set_bool(ob, r, i, tf_batch_get_bool(in, r, ci));
                    break;
                case TF_TYPE_INT64:
                    tf_batch_set_int64(ob, r, i, tf_batch_get_int64(in, r, ci));
                    break;
                case TF_TYPE_FLOAT64:
                    tf_batch_set_float64(ob, r, i, tf_batch_get_float64(in, r, ci));
                    break;
                case TF_TYPE_STRING:
                    tf_batch_set_string(ob, r, i, tf_batch_get_string(in, r, ci));
                    break;
                case TF_TYPE_DATE:
                    tf_batch_set_date(ob, r, i, tf_batch_get_date(in, r, ci));
                    break;
                case TF_TYPE_TIMESTAMP:
                    tf_batch_set_timestamp(ob, r, i, tf_batch_get_timestamp(in, r, ci));
                    break;
                default:
                    tf_batch_set_null(ob, r, i);
                    break;
            }
        }
        ob->n_rows = r + 1;
    }

    free(indices);
    *out = ob;
    return TF_OK;
}

static int select_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static void select_destroy(tf_step *self) {
    select_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_cols; i++) free(st->col_names[i]);
        free(st->col_names);
        free(st);
    }
    free(self);
}

tf_step *tf_select_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!cJSON_IsArray(cols)) return NULL;

    int n = cJSON_GetArraySize(cols);
    if (n <= 0) return NULL;

    select_state *st = calloc(1, sizeof(select_state));
    if (!st) return NULL;
    st->n_cols = (size_t)n;
    st->col_names = malloc(n * sizeof(char *));
    if (!st->col_names) { free(st); return NULL; }

    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(cols, i);
        if (!cJSON_IsString(item)) {
            for (int j = 0; j < i; j++) free(st->col_names[j]);
            free(st->col_names);
            free(st);
            return NULL;
        }
        st->col_names[i] = strdup(item->valuestring);
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { select_destroy(&(tf_step){.state = st}); return NULL; }
    step->process = select_process;
    step->flush = select_flush;
    step->destroy = select_destroy;
    step->state = st;
    return step;
}
