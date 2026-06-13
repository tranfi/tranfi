/*
 * op_lead.c - Bounded lookahead.
 *
 * Config: {"column": "price", "offset": 1, "result": "next_price"}
 *
 * Delays output by `offset` rows so the appended value can come from rows ahead
 * of the current row. On flush, emits the remaining pending rows with NULL lead
 * values. Pending state is bounded by `offset` rows.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char     *column;
    char     *result;
    size_t    offset;
    tf_batch *pending;
} lead_state;

static void lead_set_error(tf_side_channels *side, const char *msg) {
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

static const tf_batch *source_at(const lead_state *st, const tf_batch *in,
                                 size_t idx, size_t pend_count, size_t *row) {
    if (idx < pend_count) {
        *row = idx;
        return st->pending;
    }
    *row = idx - pend_count;
    return in;
}

static int copy_source_row(tf_batch *dst, size_t dst_row, const lead_state *st,
                           const tf_batch *in, size_t idx, size_t pend_count) {
    size_t src_row = 0;
    const tf_batch *src = source_at(st, in, idx, pend_count, &src_row);
    return tf_batch_copy_row(dst, dst_row, src, src_row);
}

static int lead_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    lead_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);
    if (ci < 0) {
        char msg[256];
        snprintf(msg, sizeof(msg), "lead: column '%s' not found", st->column);
        lead_set_error(side, msg);
        return TF_ERROR;
    }

    size_t pend_count = st->pending ? st->pending->n_rows : 0;
    size_t total = pend_count + in->n_rows;

    if (total <= st->offset) {
        tf_batch *new_pend = tf_batch_create(in->n_cols, total);
        if (!new_pend) return TF_ERROR;
        for (size_t c = 0; c < in->n_cols; c++) {
            if (tf_batch_set_schema(new_pend, c, in->col_names[c], in->col_types[c]) != TF_OK) {
                tf_batch_free(new_pend);
                return TF_ERROR;
            }
        }
        size_t row = 0;
        for (size_t r = 0; r < pend_count; r++) {
            if (tf_batch_copy_row(new_pend, row++, st->pending, r) != TF_OK) {
                tf_batch_free(new_pend);
                return TF_ERROR;
            }
        }
        for (size_t r = 0; r < in->n_rows; r++) {
            if (tf_batch_copy_row(new_pend, row++, in, r) != TF_OK) {
                tf_batch_free(new_pend);
                return TF_ERROR;
            }
        }
        new_pend->n_rows = total;
        if (st->pending) tf_batch_free(st->pending);
        st->pending = new_pend;
        return TF_OK;
    }

    size_t emit_count = total - st->offset;
    tf_batch *ob = tf_batch_create(in->n_cols + 1, emit_count);
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

    for (size_t i = 0; i < emit_count; i++) {
        if (copy_source_row(ob, i, st, in, i, pend_count) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }

        size_t lead_row = 0;
        const tf_batch *lead_src = source_at(st, in, i + st->offset, pend_count, &lead_row);
        int lead_ci = tf_batch_col_index(lead_src, st->column);
        if (lead_ci < 0) {
            tf_batch_free(ob);
            lead_set_error(side, "lead: buffered schema lost selected column");
            return TF_ERROR;
        }
        copy_cell(ob, i, in->n_cols, lead_src, lead_row, (size_t)lead_ci);
        ob->n_rows = i + 1;
    }

    tf_batch *new_pend = tf_batch_create(in->n_cols, st->offset);
    if (!new_pend) { tf_batch_free(ob); return TF_ERROR; }
    for (size_t c = 0; c < in->n_cols; c++) {
        if (tf_batch_set_schema(new_pend, c, in->col_names[c], in->col_types[c]) != TF_OK) {
            tf_batch_free(new_pend);
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    for (size_t i = 0; i < st->offset; i++) {
        size_t src_idx = total - st->offset + i;
        if (copy_source_row(new_pend, i, st, in, src_idx, pend_count) != TF_OK) {
            tf_batch_free(new_pend);
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    new_pend->n_rows = st->offset;

    if (st->pending) tf_batch_free(st->pending);
    st->pending = new_pend;
    *out = ob;
    return TF_OK;
}

static int lead_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    lead_state *st = self->state;
    *out = NULL;

    if (!st->pending || st->pending->n_rows == 0) return TF_OK;

    tf_batch *pend = st->pending;
    int ci = tf_batch_col_index(pend, st->column);
    if (ci < 0) {
        lead_set_error(side, "lead: buffered schema lost selected column");
        return TF_ERROR;
    }

    tf_batch *ob = tf_batch_create(pend->n_cols + 1, pend->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < pend->n_cols; c++) {
        if (tf_batch_set_schema(ob, c, pend->col_names[c], pend->col_types[c]) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    if (tf_batch_set_schema(ob, pend->n_cols, st->result, pend->col_types[ci]) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    for (size_t r = 0; r < pend->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, pend, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        tf_batch_set_null(ob, r, pend->n_cols);
        ob->n_rows = r + 1;
    }

    tf_batch_free(st->pending);
    st->pending = NULL;
    *out = ob;
    return TF_OK;
}

static void lead_destroy(tf_step *self) {
    lead_state *st = self->state;
    if (st) {
        free(st->column);
        free(st->result);
        if (st->pending) tf_batch_free(st->pending);
        free(st);
    }
    free(self);
}

tf_step *tf_lead_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j) || !col_j->valuestring[0]) return NULL;

    lead_state *st = calloc(1, sizeof(lead_state));
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
        snprintf(buf, sizeof(buf), "%s_lead", st->column);
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
    step->process = lead_process;
    step->flush = lead_flush;
    step->destroy = lead_destroy;
    step->state = st;
    return step;
}
