/*
 * op_quarantine.c -- Route matching rows to the errors side channel.
 *
 * Config: {"expr":"col('age') < 0", "name":"rule", "message":"bad row"}
 * Keeps rows where expr is false. Rows where expr is true, or where the
 * predicate cannot be evaluated, are dropped from main and emitted to errors.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef HAVE_STRDUP
extern char *strdup(const char *);
#endif

typedef struct {
    tf_expr *expr;
    char    *expr_text;
    char    *name;
    char    *message;
    size_t   row_index;
    size_t   checked_rows;
    size_t   kept_rows;
    size_t   quarantined_rows;
} quarantine_state;

static cJSON *quarantine_cell_to_json(const tf_batch *b, size_t row, size_t col) {
    if (!b || col >= b->n_cols || row >= b->n_rows || tf_batch_is_null(b, row, col)) return cJSON_CreateNull();
    switch (b->col_types[col]) {
        case TF_TYPE_BOOL: return cJSON_CreateBool(tf_batch_get_bool(b, row, col));
        case TF_TYPE_INT64: return cJSON_CreateNumber((double)tf_batch_get_int64(b, row, col));
        case TF_TYPE_FLOAT64: return cJSON_CreateNumber(tf_batch_get_float64(b, row, col));
        case TF_TYPE_STRING: return cJSON_CreateString(tf_batch_get_string(b, row, col));
        case TF_TYPE_DATE: return cJSON_CreateNumber((double)tf_batch_get_date(b, row, col));
        case TF_TYPE_TIMESTAMP: return cJSON_CreateNumber((double)tf_batch_get_timestamp(b, row, col));
        default: return cJSON_CreateNull();
    }
}

static cJSON *quarantine_row_to_json(const tf_batch *b, size_t row) {
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return NULL;
    for (size_t c = 0; c < b->n_cols; c++) {
        cJSON *value = quarantine_cell_to_json(b, row, c);
        if (!value) { cJSON_Delete(obj); return NULL; }
        cJSON_AddItemToObject(obj, b->col_names[c] ? b->col_names[c] : "", value);
    }
    return obj;
}

static int emit_quarantine_record(quarantine_state *st, const tf_batch *b, size_t row,
                                  tf_side_channels *side, const char *reason) {
    if (!side || !side->errors) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    cJSON_AddStringToObject(obj, "type", "quarantine");
    cJSON_AddStringToObject(obj, "op", "quarantine");
    cJSON_AddStringToObject(obj, "event", "row_quarantined");
    cJSON_AddStringToObject(obj, "reason", reason ? reason : "predicate_true");
    cJSON_AddStringToObject(obj, "severity", "error");
    cJSON_AddStringToObject(obj, "name", st->name ? st->name : "quarantine");
    cJSON_AddStringToObject(obj, "expr", st->expr_text ? st->expr_text : "");
    cJSON_AddNumberToObject(obj, "row", (double)st->row_index);
    if (st->message && st->message[0]) cJSON_AddStringToObject(obj, "message", st->message);
    cJSON *row_obj = quarantine_row_to_json(b, row);
    if (row_obj) cJSON_AddItemToObject(obj, "data", row_obj);
    char *line = cJSON_PrintUnformatted(obj);
    cJSON_Delete(obj);
    if (!line) return TF_ERROR;
    int rc = tf_buffer_write_str(side->errors, line);
    if (rc == TF_OK) rc = tf_buffer_write_str(side->errors, "\n");
    free(line);
    return rc;
}

static int copy_schema(tf_batch *dst, const tf_batch *src) {
    for (size_t c = 0; c < src->n_cols; c++) {
        if (tf_batch_set_schema(dst, c, src->col_names[c], src->col_types[c]) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static int quarantine_process(tf_step *self, tf_batch *in, tf_batch **out,
                              tf_side_channels *side) {
    quarantine_state *st = self->state;
    *out = NULL;

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) return TF_ERROR;
    if (copy_schema(ob, in) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }

    size_t out_row = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        st->row_index++;
        st->checked_rows++;
        bool should_quarantine = false;
        int eval_ok = tf_expr_eval(st->expr, in, r, &should_quarantine) == TF_OK;
        if (!eval_ok || should_quarantine) {
            st->quarantined_rows++;
            if (emit_quarantine_record(st, in, r, side, eval_ok ? "predicate_true" : "predicate_error") != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            continue;
        }

        if (tf_batch_copy_row(ob, out_row, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        out_row++;
        st->kept_rows++;
    }

    ob->n_rows = out_row;
    if (ob->n_rows > 0 || in->n_rows == 0) *out = ob;
    else tf_batch_free(ob);
    return TF_OK;
}

static int quarantine_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static int quarantine_append_stats(tf_step *self, tf_buffer *out) {
    if (!self || !self->state || !out) return TF_ERROR;
    quarantine_state *st = self->state;
    char buf[192];
    snprintf(buf, sizeof(buf),
             ",\"checked_rows\":%zu,\"kept_rows\":%zu,\"quarantined_rows\":%zu",
             st->checked_rows, st->kept_rows, st->quarantined_rows);
    return tf_buffer_write_str(out, buf);
}

static void quarantine_destroy(tf_step *self) {
    if (!self) return;
    quarantine_state *st = self->state;
    if (st) {
        tf_expr_free(st->expr);
        free(st->expr_text);
        free(st->name);
        free(st->message);
        free(st);
    }
    free(self);
}

tf_step *tf_quarantine_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *expr_json = cJSON_GetObjectItemCaseSensitive(args, "expr");
    if (!cJSON_IsString(expr_json) || !expr_json->valuestring[0]) {
        tf_set_last_error("quarantine: expr is required");
        return NULL;
    }

    tf_expr *expr = tf_expr_parse(expr_json->valuestring);
    if (!expr) return NULL;

    quarantine_state *st = calloc(1, sizeof(quarantine_state));
    if (!st) { tf_expr_free(expr); return NULL; }
    st->expr = expr;
    st->expr_text = strdup(expr_json->valuestring);
    cJSON *name_json = cJSON_GetObjectItemCaseSensitive(args, "name");
    cJSON *message_json = cJSON_GetObjectItemCaseSensitive(args, "message");
    st->name = strdup(cJSON_IsString(name_json) && name_json->valuestring[0] ? name_json->valuestring : "quarantine");
    st->message = strdup(cJSON_IsString(message_json) ? message_json->valuestring : "");
    if (!st->expr_text || !st->name || !st->message) {
        tf_expr_free(expr);
        free(st->expr_text);
        free(st->name);
        free(st->message);
        free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) {
        tf_expr_free(st->expr);
        free(st->expr_text);
        free(st->name);
        free(st->message);
        free(st);
        return NULL;
    }
    step->process = quarantine_process;
    step->flush = quarantine_flush;
    step->append_stats = quarantine_append_stats;
    step->destroy = quarantine_destroy;
    step->state = st;
    return step;
}
