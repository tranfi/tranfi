/*
 * op_lag.c - Bounded previous-row shift.
 *
 * Config: {"column": "price", "offset": 1, "result": "prev_price"}
 *
 * Appends the value from `offset` rows behind the current row. State retained
 * across batches is a ring buffer of the previous `offset` full rows so the
 * appended column preserves the source type and nulls.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* tf_shift_create supports type="lead" by delegating to the existing lead op. */
tf_step *tf_lead_create(const cJSON *args);

typedef struct {
    char     *column;
    char     *result;
    size_t    offset;
    tf_batch *history;
    size_t    hist_count;
    size_t    hist_pos;
} lag_state;

static void lag_set_error(tf_side_channels *side, const char *msg) {
    tf_set_last_error(msg);
    if (side && side->errors) {
        tf_buffer_write_str(side->errors, msg);
        tf_buffer_write_str(side->errors, "\n");
    }
}

static int copy_cell(tf_batch *dst, size_t dst_row, size_t dst_col,
                     const tf_batch *src, size_t src_row, size_t src_col) {
    if (tf_batch_is_null(src, src_row, src_col)) {
        tf_batch_set_null(dst, dst_row, dst_col);
        return TF_OK;
    }

    switch (src->col_types[src_col]) {
        case TF_TYPE_BOOL:
            tf_batch_set_bool(dst, dst_row, dst_col,
                              tf_batch_get_bool(src, src_row, src_col));
            return TF_OK;
        case TF_TYPE_INT64:
            tf_batch_set_int64(dst, dst_row, dst_col,
                               tf_batch_get_int64(src, src_row, src_col));
            return TF_OK;
        case TF_TYPE_FLOAT64:
            tf_batch_set_float64(dst, dst_row, dst_col,
                                 tf_batch_get_float64(src, src_row, src_col));
            return TF_OK;
        case TF_TYPE_STRING:
            tf_batch_set_string(dst, dst_row, dst_col,
                                tf_batch_get_string(src, src_row, src_col));
            return TF_OK;
        case TF_TYPE_DATE:
            tf_batch_set_date(dst, dst_row, dst_col,
                              tf_batch_get_date(src, src_row, src_col));
            return TF_OK;
        case TF_TYPE_TIMESTAMP:
            tf_batch_set_timestamp(dst, dst_row, dst_col,
                                   tf_batch_get_timestamp(src, src_row, src_col));
            return TF_OK;
        default:
            tf_batch_set_null(dst, dst_row, dst_col);
            return TF_OK;
    }
}

static int ensure_history(lag_state *st, const tf_batch *in) {
    if (st->history) return TF_OK;

    tf_batch *h = tf_batch_create(in->n_cols, st->offset);
    if (!h) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++) {
        if (tf_batch_set_schema(h, c, in->col_names[c], in->col_types[c]) != TF_OK) {
            tf_batch_free(h);
            return TF_ERROR;
        }
    }
    h->n_rows = st->offset;
    st->history = h;
    return TF_OK;
}

static int lag_process(tf_step *self, tf_batch *in, tf_batch **out,
                       tf_side_channels *side) {
    lag_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);
    if (ci < 0) {
        char msg[256];
        snprintf(msg, sizeof(msg), "lag: column '%s' not found", st->column);
        lag_set_error(side, msg);
        return TF_ERROR;
    }
    if (ensure_history(st, in) != TF_OK) return TF_ERROR;

    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++) {
        if (tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    if (tf_batch_set_schema(ob, in->n_cols, st->result, in->col_types[ci]) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }

        if (st->hist_count < st->offset) {
            tf_batch_set_null(ob, r, in->n_cols);
        } else {
            copy_cell(ob, r, in->n_cols, st->history, st->hist_pos, (size_t)ci);
        }

        if (tf_batch_copy_row(st->history, st->hist_pos, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        st->hist_pos = (st->hist_pos + 1) % st->offset;
        if (st->hist_count < st->offset) st->hist_count++;
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int lag_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self;
    (void)side;
    *out = NULL;
    return TF_OK;
}

static void lag_destroy(tf_step *self) {
    lag_state *st = self->state;
    if (st) {
        free(st->column);
        free(st->result);
        if (st->history) tf_batch_free(st->history);
        free(st);
    }
    free(self);
}

static tf_step *lag_create_with_suffix(const cJSON *args, const char *suffix) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j) || !col_j->valuestring[0]) return NULL;

    lag_state *st = calloc(1, sizeof(lag_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    if (!st->column) { free(st); return NULL; }

    cJSON *off_j = cJSON_GetObjectItemCaseSensitive(args, "offset");
    st->offset = (cJSON_IsNumber(off_j) && off_j->valueint > 0) ?
                 (size_t)off_j->valueint : 1;

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j) && res_j->valuestring[0]) {
        st->result = strdup(res_j->valuestring);
    } else {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s_%s", st->column, suffix);
        st->result = strdup(buf);
    }
    if (!st->result) { free(st->column); free(st); return NULL; }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) {
        free(st->column);
        free(st->result);
        free(st);
        return NULL;
    }
    step->process = lag_process;
    step->flush = lag_flush;
    step->destroy = lag_destroy;
    step->state = st;
    return step;
}

tf_step *tf_lag_create(const cJSON *args) {
    return lag_create_with_suffix(args, "lag");
}

tf_step *tf_shift_create(const cJSON *args) {
    cJSON *type_j = cJSON_GetObjectItemCaseSensitive(args, "type");
    const char *type = cJSON_IsString(type_j) ? type_j->valuestring : "lag";
    if (strcmp(type, "lead") == 0) return tf_lead_create(args);
    if (strcmp(type, "lag") == 0 || strcmp(type, "shift") == 0) {
        return lag_create_with_suffix(args, "shift");
    }
    return NULL;
}
