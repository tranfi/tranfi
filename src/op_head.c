/*
 * op_head.c — Take first N rows.
 *
 * Config: {"n": 5}
 * Passes through rows until N have been seen, then emits empty batches.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    size_t limit;
    size_t seen;
} head_state;

static int head_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    head_state *st = self->state;
    *out = NULL;

    if (st->seen >= st->limit) {
        /* Already past limit, discard */
        return TF_OK;
    }

    size_t remaining = st->limit - st->seen;
    size_t take = in->n_rows < remaining ? in->n_rows : remaining;
    if (take == 0) return TF_OK;

    tf_batch *ob = tf_batch_create(in->n_cols, take);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_schema(ob, in) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }
    for (size_t r = 0; r < take; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    st->seen += take;
    *out = ob;
    return TF_OK;
}

static int head_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static void head_destroy(tf_step *self) {
    free(self->state);
    free(self);
}

tf_step *tf_head_create(const cJSON *args) {
    if (!args) return NULL;
    size_t n = 0;
    int has_n = tf_json_get_size_arg(args, "n", 0, TF_MAX_COUNT_ARG, &n, "head");
    if (has_n <= 0) return NULL;

    head_state *st = calloc(1, sizeof(head_state));
    if (!st) return NULL;
    st->limit = n;
    st->seen = 0;

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st); return NULL; }
    step->process = head_process;
    step->flush = head_flush;
    step->destroy = head_destroy;
    step->state = st;
    return step;
}
