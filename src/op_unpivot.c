/*
 * op_unpivot.c — Wide to long. Adds variable + value columns, multiplies rows.
 *
 * Config: {"columns": ["q1", "q2", "q3"]}
 *   Keeps non-listed columns, melts listed columns into variable/value pairs.
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
} unpivot_state;

static int unpivot_process(tf_step *self, tf_batch *in, tf_batch **out,
                           tf_side_channels *side) {
    (void)side;
    unpivot_state *st = self->state;
    *out = NULL;

    /* Determine which columns are value columns and which are id columns */
    int *is_value = calloc(in->n_cols, sizeof(int));
    if (!is_value) return TF_ERROR;
    size_t n_value = 0;
    for (size_t c = 0; c < in->n_cols; c++) {
        for (size_t k = 0; k < st->n_cols; k++) {
            if (strcmp(in->col_names[c], st->cols[k]) == 0) {
                is_value[c] = 1;
                n_value++;
                break;
            }
        }
    }
    size_t n_id = in->n_cols - n_value;
    if (n_value == 0) { free(is_value); return TF_OK; }

    /* Output: id columns + "variable" + "value" */
    size_t out_n_cols = n_id + 2;
    size_t max_rows = in->n_rows * n_value;
    tf_batch *ob = tf_batch_create(out_n_cols, max_rows > 0 ? max_rows : 16);
    if (!ob) { free(is_value); return TF_ERROR; }

    size_t oc = 0;
    for (size_t c = 0; c < in->n_cols; c++) {
        if (!is_value[c])
            tf_batch_set_schema(ob, oc++, in->col_names[c], in->col_types[c]);
    }
    tf_batch_set_schema(ob, oc, "variable", TF_TYPE_STRING);
    tf_batch_set_schema(ob, oc + 1, "value", TF_TYPE_STRING);

    size_t out_row = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        for (size_t c = 0; c < in->n_cols; c++) {
            if (!is_value[c]) continue;

            tf_batch_ensure_capacity(ob, out_row + 1);

            /* Copy id columns */
            size_t oidx = 0;
            for (size_t ic = 0; ic < in->n_cols; ic++) {
                if (is_value[ic]) continue;
                if (tf_batch_is_null(in, r, ic))
                    tf_batch_set_null(ob, out_row, oidx);
                else {
                    switch (in->col_types[ic]) {
                        case TF_TYPE_BOOL: tf_batch_set_bool(ob, out_row, oidx, tf_batch_get_bool(in, r, ic)); break;
                        case TF_TYPE_INT64: tf_batch_set_int64(ob, out_row, oidx, tf_batch_get_int64(in, r, ic)); break;
                        case TF_TYPE_FLOAT64: tf_batch_set_float64(ob, out_row, oidx, tf_batch_get_float64(in, r, ic)); break;
                        case TF_TYPE_STRING: tf_batch_set_string(ob, out_row, oidx, tf_batch_get_string(in, r, ic)); break;
                        case TF_TYPE_DATE: tf_batch_set_date(ob, out_row, oidx, tf_batch_get_date(in, r, ic)); break;
                        case TF_TYPE_TIMESTAMP: tf_batch_set_timestamp(ob, out_row, oidx, tf_batch_get_timestamp(in, r, ic)); break;
                        default: tf_batch_set_null(ob, out_row, oidx); break;
                    }
                }
                oidx++;
            }

            /* Set variable name */
            tf_batch_set_string(ob, out_row, n_id, in->col_names[c]);

            /* Set value (convert to string) */
            if (tf_batch_is_null(in, r, c)) {
                tf_batch_set_null(ob, out_row, n_id + 1);
            } else {
                char buf[64];
                switch (in->col_types[c]) {
                    case TF_TYPE_STRING: tf_batch_set_string(ob, out_row, n_id + 1, tf_batch_get_string(in, r, c)); break;
                    case TF_TYPE_INT64:
                        snprintf(buf, sizeof(buf), "%lld", (long long)tf_batch_get_int64(in, r, c));
                        tf_batch_set_string(ob, out_row, n_id + 1, buf); break;
                    case TF_TYPE_FLOAT64:
                        snprintf(buf, sizeof(buf), "%g", tf_batch_get_float64(in, r, c));
                        tf_batch_set_string(ob, out_row, n_id + 1, buf); break;
                    case TF_TYPE_BOOL:
                        tf_batch_set_string(ob, out_row, n_id + 1, tf_batch_get_bool(in, r, c) ? "true" : "false"); break;
                    case TF_TYPE_DATE:
                        tf_date_format(tf_batch_get_date(in, r, c), buf, sizeof(buf));
                        tf_batch_set_string(ob, out_row, n_id + 1, buf); break;
                    case TF_TYPE_TIMESTAMP:
                        tf_timestamp_format(tf_batch_get_timestamp(in, r, c), buf, sizeof(buf));
                        tf_batch_set_string(ob, out_row, n_id + 1, buf); break;
                    default:
                        tf_batch_set_null(ob, out_row, n_id + 1); break;
                }
            }

            ob->n_rows = ++out_row;
        }
    }

    free(is_value);
    if (out_row > 0) *out = ob;
    else tf_batch_free(ob);
    return TF_OK;
}

static int unpivot_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void unpivot_destroy(tf_step *self) {
    unpivot_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_cols; i++) free(st->cols[i]);
        free(st->cols); free(st);
    }
    free(self);
}

tf_step *tf_unpivot_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *columns = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!columns || !cJSON_IsArray(columns)) return NULL;

    int n = cJSON_GetArraySize(columns);
    if (n <= 0) return NULL;

    unpivot_state *st = calloc(1, sizeof(unpivot_state));
    if (!st) return NULL;
    st->cols = calloc(n, sizeof(char *));
    st->n_cols = n;
    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(columns, i);
        if (cJSON_IsString(item)) st->cols[i] = strdup(item->valuestring);
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { unpivot_destroy(&(tf_step){.state = st}); return NULL; }
    step->process = unpivot_process;
    step->flush = unpivot_flush;
    step->destroy = unpivot_destroy;
    step->state = st;
    return step;
}
