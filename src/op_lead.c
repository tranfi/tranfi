/*
 * op_lead.c — Lookahead: access value N rows ahead.
 *
 * Config: {"column": "price", "offset": 1, "result": "next_price"}
 *
 * Buffers the last `offset` rows across batch boundaries.
 * On flush, emits remaining rows with NULL lead values.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char   *column;
    char   *result;
    size_t  offset;
    /* Pending rows: stored as a batch that hasn't been emitted yet */
    tf_batch *pending;
} lead_state;

static double get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    if (b->col_types[ci] == TF_TYPE_FLOAT64) return tf_batch_get_float64(b, r, ci);
    return 0;
}

static int lead_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    lead_state *st = self->state;
    *out = NULL;

    size_t pend_count = st->pending ? st->pending->n_rows : 0;
    size_t total = pend_count + in->n_rows;

    if (total <= st->offset) {
        /* Not enough rows yet — append all to pending */
        tf_batch *new_pend = tf_batch_create(in->n_cols, total);
        if (!new_pend) return TF_ERROR;
        for (size_t c = 0; c < in->n_cols; c++)
            tf_batch_set_schema(new_pend, c, in->col_names[c], in->col_types[c]);
        size_t row = 0;
        if (st->pending) {
            for (size_t r = 0; r < pend_count; r++)
                tf_batch_copy_row(new_pend, row++, st->pending, r);
            tf_batch_free(st->pending);
        }
        for (size_t r = 0; r < in->n_rows; r++)
            tf_batch_copy_row(new_pend, row++, in, r);
        new_pend->n_rows = total;
        st->pending = new_pend;
        return TF_OK;
    }

    size_t emit_count = total - st->offset;
    tf_batch *ob = tf_batch_create(in->n_cols + 1, emit_count);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    tf_batch_set_schema(ob, in->n_cols, st->result, TF_TYPE_FLOAT64);

    /* Emit rows: for each emitted row i, lead value comes from row i+offset */
    for (size_t i = 0; i < emit_count; i++) {
        /* Source row for base data */
        if (i < pend_count) {
            tf_batch_copy_row(ob, i, st->pending, i);
        } else {
            tf_batch_copy_row(ob, i, in, i - pend_count);
        }

        /* Lead value from row i + offset */
        size_t lead_idx = i + st->offset;
        tf_batch *lead_src;
        size_t lead_row;
        if (lead_idx < pend_count) {
            lead_src = st->pending;
            lead_row = lead_idx;
        } else {
            lead_src = in;
            lead_row = lead_idx - pend_count;
        }

        int lead_ci = tf_batch_col_index(lead_src, st->column);
        if (lead_ci >= 0 && !tf_batch_is_null(lead_src, lead_row, lead_ci)) {
            tf_batch_set_float64(ob, i, in->n_cols, get_numeric(lead_src, lead_row, lead_ci));
        } else {
            tf_batch_set_null(ob, i, in->n_cols);
        }
    }
    ob->n_rows = emit_count;

    /* Store remaining rows as new pending */
    if (st->pending) {
        tf_batch_free(st->pending);
        st->pending = NULL;
    }
    size_t new_pend_count = st->offset;
    if (new_pend_count > 0) {
        tf_batch *new_pend = tf_batch_create(in->n_cols, new_pend_count);
        if (!new_pend) { tf_batch_free(ob); return TF_ERROR; }
        for (size_t c = 0; c < in->n_cols; c++)
            tf_batch_set_schema(new_pend, c, in->col_names[c], in->col_types[c]);
        for (size_t i = 0; i < new_pend_count; i++) {
            size_t src_idx = total - st->offset + i;
            if (src_idx < pend_count) {
                /* This shouldn't happen since we emit at least pend_count rows when total > offset */
                tf_batch_copy_row(new_pend, i, st->pending, src_idx);
            } else {
                tf_batch_copy_row(new_pend, i, in, src_idx - pend_count);
            }
        }
        new_pend->n_rows = new_pend_count;
        st->pending = new_pend;
    }

    *out = ob;
    return TF_OK;
}

static int lead_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    lead_state *st = self->state;
    *out = NULL;

    if (!st->pending || st->pending->n_rows == 0) return TF_OK;

    /* Emit remaining pending rows with NULL lead values */
    tf_batch *pend = st->pending;
    tf_batch *ob = tf_batch_create(pend->n_cols + 1, pend->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < pend->n_cols; c++)
        tf_batch_set_schema(ob, c, pend->col_names[c], pend->col_types[c]);
    tf_batch_set_schema(ob, pend->n_cols, st->result, TF_TYPE_FLOAT64);

    for (size_t r = 0; r < pend->n_rows; r++) {
        tf_batch_copy_row(ob, r, pend, r);
        tf_batch_set_null(ob, r, pend->n_cols);
    }
    ob->n_rows = pend->n_rows;

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
    if (!cJSON_IsString(col_j)) return NULL;

    lead_state *st = calloc(1, sizeof(lead_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);

    cJSON *off_j = cJSON_GetObjectItemCaseSensitive(args, "offset");
    st->offset = (cJSON_IsNumber(off_j) && off_j->valueint > 0) ? (size_t)off_j->valueint : 1;

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = strdup(res_j->valuestring);
    } else {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s_lead", st->column);
        st->result = strdup(buf);
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->column); free(st->result); free(st); return NULL; }
    step->process = lead_process;
    step->flush = lead_flush;
    step->destroy = lead_destroy;
    step->state = st;
    return step;
}
