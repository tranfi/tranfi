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
    tf_audit_options audit_opts;
} filter_state;

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
    cJSON *data = tf_audit_row_to_json(b, row, &st->audit_opts);
    if (data) cJSON_AddItemToObject(obj, "data", data);
    int rc = tf_buffer_write_json_line(side->stats, obj);
    cJSON_Delete(obj);
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
    if (tf_batch_clone_schema(ob, in) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
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

        if (tf_batch_copy_row(ob, out_row, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, out_row) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        out_row++;
    }

    st->rows_in += in->n_rows;
    st->rows_out += out_row;

    /* Emit stats to side channel */
    if (side && side->stats) {
        char stats_buf[128];
        snprintf(stats_buf, sizeof(stats_buf),
                 "{\"op\":\"filter\",\"rows_in\":%zu,\"rows_out\":%zu}",
                 in->n_rows, out_row);
        if (tf_buffer_write_line(side->stats, stats_buf) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
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
        tf_audit_options_free(&st->audit_opts);
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
    tf_audit_options_init(&st->audit_opts, 1);
    cJSON *audit_json = cJSON_GetObjectItemCaseSensitive(args, "audit");
    st->audit = cJSON_IsTrue(audit_json) ? 1 : 0;
    cJSON *audit_limit_json = cJSON_GetObjectItemCaseSensitive(args, "audit_limit");
    if (!audit_limit_json) audit_limit_json = cJSON_GetObjectItemCaseSensitive(args, "auditLimit");
    if (audit_limit_json) {
        size_t parsed_limit = 0;
        if (tf_json_get_size_arg_any(args, "audit_limit", "auditLimit",
                                     1, TF_MAX_AUDIT_RECORDS,
                                     &parsed_limit, "filter") < 0) {
            tf_expr_free(expr);
            free(st->expr_text);
            tf_audit_options_free(&st->audit_opts);
            free(st);
            return NULL;
        }
        st->audit_limit = parsed_limit;
    }
    if (tf_audit_options_parse(&st->audit_opts, args, "filter") != TF_OK) {
        tf_expr_free(expr);
        free(st->expr_text);
        tf_audit_options_free(&st->audit_opts);
        free(st);
        return NULL;
    }
    if (!st->expr_text) { tf_expr_free(expr); tf_audit_options_free(&st->audit_opts); free(st); return NULL; }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { tf_expr_free(expr); free(st->expr_text); tf_audit_options_free(&st->audit_opts); free(st); return NULL; }
    step->process = filter_process;
    step->flush = filter_flush;
    step->destroy = filter_destroy;
    step->state = st;
    return step;
}
