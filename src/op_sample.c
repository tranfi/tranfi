/*
 * op_sample.c — Reservoir sampling (Algorithm R). Bounded memory.
 *
 * Config: {"n": 100}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    size_t    n;
    tf_batch *buf;
    size_t    seen;       /* total rows seen */
    int       has_schema;
    unsigned  seed;
} sample_state;

static int sample_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    (void)side;
    sample_state *st = self->state;
    *out = NULL;

    if (!st->has_schema) {
        st->buf = tf_batch_create(in->n_cols, st->n);
        if (!st->buf) return TF_ERROR;
        for (size_t c = 0; c < in->n_cols; c++)
            tf_batch_set_schema(st->buf, c, in->col_names[c], in->col_types[c]);
        st->has_schema = 1;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        if (st->seen < st->n) {
            /* Fill reservoir */
            tf_batch_copy_row(st->buf, st->seen, in, r);
            st->buf->n_rows = st->seen + 1;
        } else {
            /* Replace with probability n/seen */
            size_t j = (size_t)(rand_r(&st->seed) % (st->seen + 1));
            if (j < st->n) {
                tf_batch_copy_row(st->buf, j, in, r);
            }
        }
        st->seen++;
    }

    return TF_OK;
}

static int sample_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    sample_state *st = self->state;
    *out = NULL;

    if (!st->buf || st->buf->n_rows == 0) return TF_OK;

    /* Copy buffer to output */
    size_t n = st->buf->n_rows;
    tf_batch *ob = tf_batch_create(st->buf->n_cols, n);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < st->buf->n_cols; c++)
        tf_batch_set_schema(ob, c, st->buf->col_names[c], st->buf->col_types[c]);
    for (size_t i = 0; i < n; i++) {
        tf_batch_copy_row(ob, i, st->buf, i);
        ob->n_rows = i + 1;
    }

    *out = ob;
    return TF_OK;
}

static void sample_destroy(tf_step *self) {
    sample_state *st = self->state;
    if (st) {
        if (st->buf) tf_batch_free(st->buf);
        free(st);
    }
    free(self);
}

tf_step *tf_sample_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *n_json = cJSON_GetObjectItemCaseSensitive(args, "n");
    if (!cJSON_IsNumber(n_json) || n_json->valueint <= 0) return NULL;

    sample_state *st = calloc(1, sizeof(sample_state));
    if (!st) return NULL;
    st->n = (size_t)n_json->valueint;
    st->seed = (unsigned)time(NULL);

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st); return NULL; }
    step->process = sample_process;
    step->flush = sample_flush;
    step->destroy = sample_destroy;
    step->state = st;
    return step;
}
