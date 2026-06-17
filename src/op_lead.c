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

static int lead_set_error(tf_side_channels *side, const char *msg) {
    return tf_side_write_error(side, msg);
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
        if (lead_set_error(side, msg) != TF_OK) return TF_ERROR;
        return TF_ERROR;
    }

    size_t pend_count = st->pending ? st->pending->n_rows : 0;
    size_t total = pend_count + in->n_rows;

    if (total <= st->offset) {
        tf_batch *new_pend = tf_batch_create(in->n_cols, total);
        if (!new_pend) return TF_ERROR;
        if (tf_batch_clone_schema(new_pend, in) != TF_OK) {
            tf_batch_free(new_pend);
            return TF_ERROR;
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
    const char *extra_names[1] = {st->result};
    tf_type extra_types[1] = {in->col_types[ci]};
    tf_batch *ob = tf_batch_create(in->n_cols + 1, emit_count);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_with_extra_cols(ob, in, extra_names, extra_types, 1) != TF_OK) {
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
            if (lead_set_error(side, "lead: buffered schema lost selected column") != TF_OK) return TF_ERROR;
            return TF_ERROR;
        }
        if (tf_batch_copy_cell(ob, i, in->n_cols, lead_src, lead_row, (size_t)lead_ci) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        ob->n_rows = i + 1;
    }

    tf_batch *new_pend = tf_batch_create(in->n_cols, st->offset);
    if (!new_pend) { tf_batch_free(ob); return TF_ERROR; }
    if (tf_batch_clone_schema(new_pend, in) != TF_OK) {
        tf_batch_free(new_pend);
        tf_batch_free(ob);
        return TF_ERROR;
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
        if (lead_set_error(side, "lead: buffered schema lost selected column") != TF_OK) return TF_ERROR;
        return TF_ERROR;
    }

    const char *extra_names[1] = {st->result};
    tf_type extra_types[1] = {pend->col_types[ci]};
    tf_batch *ob = tf_batch_create(pend->n_cols + 1, pend->n_rows);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_with_extra_cols(ob, pend, extra_names, extra_types, 1) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    for (size_t r = 0; r < pend->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, pend, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_set_null(ob, r, pend->n_cols) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
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

    size_t offset = 1;
    int has_offset = tf_json_get_size_arg(args, "offset",
                                          1, TF_MAX_WINDOW_SIZE,
                                          &offset, "lead");
    if (has_offset < 0) { free(st->column); free(st); return NULL; }
    st->offset = offset;

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
