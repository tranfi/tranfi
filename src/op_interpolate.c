/*
 * op_interpolate.c — Fill null values via interpolation.
 * Methods: forward, backward, linear.
 *
 * For backward/linear: buffers rows with null target values and emits
 * them when the next non-null value arrives.
 *
 * Config: {"column": "price", "method": "linear"}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef enum {
    INTERP_FORWARD,
    INTERP_BACKWARD,
    INTERP_LINEAR
} interp_method;

/* Buffered row: we store complete batches and track which rows are pending */
typedef struct pending_row {
    tf_batch *batch; /* single-row batch (copy of original row) */
    size_t    target_col; /* column index in the batch */
} pending_row;

typedef struct {
    char          *column;
    interp_method  method;
    double         last_val;
    int            has_last;
    /* Pending null rows (for backward/linear) */
    pending_row   *pending;
    size_t         n_pending;
    size_t         cap_pending;
} interpolate_state;

static interp_method parse_method(const char *s) {
    if (!s) return INTERP_LINEAR;
    if (strcmp(s, "forward") == 0) return INTERP_FORWARD;
    if (strcmp(s, "backward") == 0) return INTERP_BACKWARD;
    return INTERP_LINEAR;
}

static double get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    if (b->col_types[ci] == TF_TYPE_FLOAT64) return tf_batch_get_float64(b, r, ci);
    return 0;
}

static void add_pending(interpolate_state *st, tf_batch *row_batch, size_t target_col) {
    if (st->n_pending >= st->cap_pending) {
        size_t newcap = st->cap_pending ? st->cap_pending * 2 : 16;
        pending_row *tmp = realloc(st->pending, newcap * sizeof(pending_row));
        if (!tmp) return;
        st->pending = tmp;
        st->cap_pending = newcap;
    }
    st->pending[st->n_pending].batch = row_batch;
    st->pending[st->n_pending].target_col = target_col;
    st->n_pending++;
}

/* Emit all pending rows into output batch, interpolating values */
static void flush_pending(interpolate_state *st, tf_batch *ob, size_t *out_row,
                          double end_val, size_t target_col) {
    if (st->n_pending == 0) return;

    for (size_t i = 0; i < st->n_pending; i++) {
        tf_batch *pb = st->pending[i].batch;
        size_t r = *out_row;
        tf_batch_copy_row(ob, r, pb, 0);

        double interp_val;
        if (st->method == INTERP_BACKWARD) {
            interp_val = end_val;
        } else {
            /* Linear: interpolate between last_val and end_val */
            if (st->has_last) {
                double t = (double)(i + 1) / (double)(st->n_pending + 1);
                interp_val = st->last_val + t * (end_val - st->last_val);
            } else {
                interp_val = end_val;
            }
        }

        tf_batch_set_float64(ob, r, target_col, interp_val);
        ob->n_rows = r + 1;
        (*out_row)++;

        tf_batch_free(pb);
    }
    st->n_pending = 0;
}

static int interpolate_process(tf_step *self, tf_batch *in, tf_batch **out,
                               tf_side_channels *side) {
    (void)side;
    interpolate_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);

    /* Count total rows we might output (pending + current batch) */
    size_t max_rows = st->n_pending + in->n_rows;
    tf_batch *ob = tf_batch_create(in->n_cols, max_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);

    size_t out_row = 0;

    for (size_t r = 0; r < in->n_rows; r++) {
        if (ci < 0) {
            /* No target column — just pass through */
            tf_batch_copy_row(ob, out_row, in, r);
            ob->n_rows = out_row + 1;
            out_row++;
            continue;
        }

        int is_null = tf_batch_is_null(in, r, ci);

        if (is_null) {
            if (st->method == INTERP_FORWARD && st->has_last) {
                /* Forward fill: use last known value */
                tf_batch_copy_row(ob, out_row, in, r);
                tf_batch_set_float64(ob, out_row, ci, st->last_val);
                ob->n_rows = out_row + 1;
                out_row++;
            } else if (st->method == INTERP_FORWARD) {
                /* No previous value yet — pass null through */
                tf_batch_copy_row(ob, out_row, in, r);
                ob->n_rows = out_row + 1;
                out_row++;
            } else {
                /* Backward/linear: buffer this row */
                tf_batch *row_copy = tf_batch_create(in->n_cols, 1);
                if (row_copy) {
                    for (size_t c = 0; c < in->n_cols; c++)
                        tf_batch_set_schema(row_copy, c, in->col_names[c], in->col_types[c]);
                    tf_batch_copy_row(row_copy, 0, in, r);
                    row_copy->n_rows = 1;
                    add_pending(st, row_copy, ci);
                }
            }
        } else {
            double val = get_numeric(in, r, ci);

            /* Flush any pending rows */
            if (st->n_pending > 0) {
                /* Grow output if needed */
                tf_batch_ensure_capacity(ob, out_row + st->n_pending + 1);
                flush_pending(st, ob, &out_row, val, ci);
            }

            /* Output current row */
            tf_batch_copy_row(ob, out_row, in, r);
            ob->n_rows = out_row + 1;
            out_row++;

            st->last_val = val;
            st->has_last = 1;
        }
    }

    if (ob->n_rows == 0) {
        tf_batch_free(ob);
        *out = NULL;
    } else {
        *out = ob;
    }
    return TF_OK;
}

static int interpolate_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    interpolate_state *st = self->state;
    *out = NULL;

    /* Emit any remaining pending rows as nulls (or forward-fill with last known) */
    if (st->n_pending > 0) {
        tf_batch *first = st->pending[0].batch;
        tf_batch *ob = tf_batch_create(first->n_cols, st->n_pending);
        if (!ob) return TF_OK;
        for (size_t c = 0; c < first->n_cols; c++)
            tf_batch_set_schema(ob, c, first->col_names[c], first->col_types[c]);

        for (size_t i = 0; i < st->n_pending; i++) {
            tf_batch *pb = st->pending[i].batch;
            tf_batch_copy_row(ob, i, pb, 0);
            /* For linear/backward at end of stream: use last known if available */
            if (st->has_last) {
                tf_batch_set_float64(ob, i, st->pending[i].target_col, st->last_val);
            }
            ob->n_rows = i + 1;
            tf_batch_free(pb);
        }
        st->n_pending = 0;
        *out = ob;
    }

    return TF_OK;
}

static void interpolate_destroy(tf_step *self) {
    interpolate_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_pending; i++)
            tf_batch_free(st->pending[i].batch);
        free(st->pending);
        free(st->column);
        free(st);
    }
    free(self);
}

tf_step *tf_interpolate_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j)) return NULL;

    interpolate_state *st = calloc(1, sizeof(interpolate_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);

    cJSON *method_j = cJSON_GetObjectItemCaseSensitive(args, "method");
    st->method = parse_method(cJSON_IsString(method_j) ? method_j->valuestring : NULL);

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->column); free(st); return NULL; }
    step->process = interpolate_process;
    step->flush = interpolate_flush;
    step->destroy = interpolate_destroy;
    step->state = st;
    return step;
}
