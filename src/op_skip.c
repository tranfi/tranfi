/*
 * op_skip.c — Skip first N rows.
 *
 * Config: {"n": 5}
 * Discards the first N rows, passes through the rest.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    size_t n;
    size_t seen;
} skip_state;

static int skip_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    skip_state *st = self->state;
    *out = NULL;

    size_t emit_start = 0;
    size_t next_seen = st->seen;
    if (st->seen < st->n) {
        size_t remaining_skip = st->n - st->seen;
        if (remaining_skip >= in->n_rows) {
            /* Skip entire batch */
            st->seen += in->n_rows;
            return TF_OK;
        }
        emit_start = remaining_skip;
        next_seen = st->n;
    }

    size_t emit_count = in->n_rows - emit_start;
    if (emit_count == 0) {
        st->seen = next_seen;
        return TF_OK;
    }

    tf_batch *ob = tf_batch_create(in->n_cols, emit_count);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_schema(ob, in) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }
    for (size_t i = 0; i < emit_count; i++) {
        size_t r = emit_start + i;
        if (tf_batch_copy_row(ob, i, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        ob->n_rows = i + 1;
    }

    st->seen = next_seen;
    *out = ob;
    return TF_OK;
}

static int skip_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static void skip_destroy(tf_step *self) {
    free(self->state);
    free(self);
}

tf_step *tf_skip_create(const cJSON *args) {
    if (!args) return NULL;
    size_t n = 0;
    int has_n = tf_json_get_size_arg(args, "n", 0, TF_MAX_COUNT_ARG, &n, "skip");
    if (has_n <= 0) return NULL;

    skip_state *st = calloc(1, sizeof(skip_state));
    if (!st) return NULL;
    st->n = n;
    st->seen = 0;

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st); return NULL; }
    step->process = skip_process;
    step->flush = skip_flush;
    step->destroy = skip_destroy;
    step->state = st;
    return step;
}
