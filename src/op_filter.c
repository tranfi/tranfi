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
#include <stdbool.h>

typedef struct {
    tf_expr *expr;
    char    *expr_text;
    size_t   rows_in;
    size_t   rows_out;
    size_t   row_index;
    size_t   audit_limit;
    size_t   audit_emitted;
    int      audit;
} filter_state;

static cJSON *filter_cell_to_json(const tf_batch *b, size_t row, size_t col) {
    if (tf_batch_is_null(b, row, col)) return cJSON_CreateNull();
    switch (b->col_types[col]) {
        case TF_TYPE_BOOL:
            return cJSON_CreateBool(tf_batch_get_bool(b, row, col));
        case TF_TYPE_INT64:
            return cJSON_CreateNumber((double)tf_batch_get_int64(b, row, col));
        case TF_TYPE_FLOAT64:
            return cJSON_CreateNumber(tf_batch_get_float64(b, row, col));
        case TF_TYPE_STRING:
            return cJSON_CreateString(tf_batch_get_string(b, row, col));
        case TF_TYPE_DATE:
            return cJSON_CreateNumber((double)tf_batch_get_date(b, row, col));
        case TF_TYPE_TIMESTAMP:
            return cJSON_CreateNumber((double)tf_batch_get_timestamp(b, row, col));
        default:
            return cJSON_CreateNull();
    }
}

static cJSON *filter_row_to_json(const tf_batch *b, size_t row) {
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return NULL;
    for (size_t c = 0; c < b->n_cols; c++) {
        cJSON *value = filter_cell_to_json(b, row, c);
        if (!value) { cJSON_Delete(obj); return NULL; }
        cJSON_AddItemToObject(obj, b->col_names[c] ? b->col_names[c] : "", value);
    }
    return obj;
}

static int filter_emit_drop_audit(filter_state *st, const tf_batch *b, size_t row,
                                  tf_side_channels *side, const char *reason) {
    if (!st->audit || st->audit_emitted >= st->audit_limit || !side || !side->stats) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    cJSON_AddStringToObject(obj, "type", "audit");
    cJSON_AddStringToObject(obj, "op", "filter");
    cJSON_AddStringToObject(obj, "event", "row_dropped");
    cJSON_AddStringToObject(obj, "reason", reason ? reason : "predicate_false");
    cJSON_AddStringToObject(obj, "channel", "audit");
    cJSON_AddStringToObject(obj, "expr", st->expr_text ? st->expr_text : "");
    cJSON_AddNumberToObject(obj, "row", (double)st->row_index);
    cJSON *data = filter_row_to_json(b, row);
    if (data) cJSON_AddItemToObject(obj, "data", data);
    char *line = cJSON_PrintUnformatted(obj);
    cJSON_Delete(obj);
    if (!line) return TF_ERROR;
    int rc = tf_buffer_write_str(side->stats, line);
    if (rc == TF_OK) rc = tf_buffer_write_str(side->stats, "\n");
    free(line);
    if (rc == TF_OK) st->audit_emitted++;
    return rc;
}

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
        st->row_index++;
        bool keep = false;
        int eval_ok = tf_expr_eval(st->expr, in, r, &keep) == TF_OK;
        if (!eval_ok || !keep) {
            if (filter_emit_drop_audit(st, in, r, side,
                                       eval_ok ? "predicate_false" : "predicate_error") != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            continue;
        }

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
        free(st->expr_text);
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
    st->expr_text = strdup(expr_json->valuestring);
    st->audit_limit = 1000;
    cJSON *audit_json = cJSON_GetObjectItemCaseSensitive(args, "audit");
    st->audit = cJSON_IsTrue(audit_json) ? 1 : 0;
    cJSON *audit_limit_json = cJSON_GetObjectItemCaseSensitive(args, "audit_limit");
    if (!audit_limit_json) audit_limit_json = cJSON_GetObjectItemCaseSensitive(args, "auditLimit");
    if (audit_limit_json) {
        if (!cJSON_IsNumber(audit_limit_json) || audit_limit_json->valuedouble <= 0) {
            tf_expr_free(expr);
            free(st->expr_text);
            free(st);
            tf_set_last_error("filter: audit_limit must be a positive integer");
            return NULL;
        }
        st->audit_limit = (size_t)audit_limit_json->valuedouble;
    }
    if (!st->expr_text) { tf_expr_free(expr); free(st); return NULL; }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { tf_expr_free(expr); free(st); return NULL; }
    step->process = filter_process;
    step->flush = filter_flush;
    step->destroy = filter_destroy;
    step->state = st;
    return step;
}
