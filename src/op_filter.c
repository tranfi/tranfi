/*
 * op_filter.c — Filter rows by expression.
 *
 * Config: {"expr": "col('age') > 25"}
 * Evaluates expression for each row, keeps rows where result is true.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    tf_expr *expr;
    size_t   rows_in;
    size_t   rows_out;
} filter_state;

static int filter_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    filter_state *st = self->state;
    *out = NULL;

    /* Create output batch with same schema */
    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t i = 0; i < in->n_cols; i++) {
        tf_batch_set_schema(ob, i, in->col_names[i], in->col_types[i]);
    }

    /* Filter rows */
    size_t out_row = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        bool keep = false;
        if (tf_expr_eval(st->expr, in, r, &keep) != TF_OK) continue;
        if (!keep) continue;

        if (tf_batch_ensure_capacity(ob, out_row + 1) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }

        /* Copy row */
        for (size_t c = 0; c < in->n_cols; c++) {
            if (tf_batch_is_null(in, r, c)) {
                tf_batch_set_null(ob, out_row, c);
                continue;
            }
            switch (in->col_types[c]) {
                case TF_TYPE_BOOL:
                    tf_batch_set_bool(ob, out_row, c, tf_batch_get_bool(in, r, c));
                    break;
                case TF_TYPE_INT64:
                    tf_batch_set_int64(ob, out_row, c, tf_batch_get_int64(in, r, c));
                    break;
                case TF_TYPE_FLOAT64:
                    tf_batch_set_float64(ob, out_row, c, tf_batch_get_float64(in, r, c));
                    break;
                case TF_TYPE_STRING:
                    tf_batch_set_string(ob, out_row, c, tf_batch_get_string(in, r, c));
                    break;
                case TF_TYPE_DATE:
                    tf_batch_set_date(ob, out_row, c, tf_batch_get_date(in, r, c));
                    break;
                case TF_TYPE_TIMESTAMP:
                    tf_batch_set_timestamp(ob, out_row, c, tf_batch_get_timestamp(in, r, c));
                    break;
                default:
                    tf_batch_set_null(ob, out_row, c);
                    break;
            }
        }
        out_row++;
    }
    ob->n_rows = out_row;

    st->rows_in += in->n_rows;
    st->rows_out += out_row;

    /* Emit stats to side channel */
    if (side && side->stats) {
        char stats_buf[128];
        snprintf(stats_buf, sizeof(stats_buf),
                 "{\"op\":\"filter\",\"rows_in\":%zu,\"rows_out\":%zu}\n",
                 in->n_rows, out_row);
        tf_buffer_write_str(side->stats, stats_buf);
    }

    if (out_row > 0) {
        *out = ob;
    } else {
        tf_batch_free(ob);
    }
    return TF_OK;
}

static int filter_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static void filter_destroy(tf_step *self) {
    filter_state *st = self->state;
    if (st) {
        tf_expr_free(st->expr);
        free(st);
    }
    free(self);
}

tf_step *tf_filter_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *expr_json = cJSON_GetObjectItemCaseSensitive(args, "expr");
    if (!cJSON_IsString(expr_json)) return NULL;

    tf_expr *expr = tf_expr_parse(expr_json->valuestring);
    if (!expr) return NULL;

    filter_state *st = calloc(1, sizeof(filter_state));
    if (!st) { tf_expr_free(expr); return NULL; }
    st->expr = expr;

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { tf_expr_free(expr); free(st); return NULL; }
    step->process = filter_process;
    step->flush = filter_flush;
    step->destroy = filter_destroy;
    step->state = st;
    return step;
}
