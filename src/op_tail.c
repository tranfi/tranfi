/*
 * op_tail.c — Last N rows via circular buffer. Bounded memory.
 *
 * Config: {"n": 5}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    size_t    limit;
    tf_batch *buf;       /* circular buffer of rows */
    size_t    head;      /* next write position */
    size_t    count;     /* total rows seen */
    int       has_schema;
} tail_state;

static int tail_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    tail_state *st = self->state;
    *out = NULL;

    if (!st->has_schema) {
        st->buf = tf_batch_create(in->n_cols, st->limit);
        if (!st->buf) return TF_ERROR;
        for (size_t c = 0; c < in->n_cols; c++)
            tf_batch_set_schema(st->buf, c, in->col_names[c], in->col_types[c]);
        st->buf->n_rows = 0;
        st->has_schema = 1;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        size_t dst = st->head % st->limit;
        tf_batch_copy_row(st->buf, dst, in, r);
        st->head++;
        st->count++;
        if (st->buf->n_rows < st->limit) st->buf->n_rows++;
    }

    return TF_OK;
}

static int tail_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    tail_state *st = self->state;
    *out = NULL;

    if (!st->buf || st->buf->n_rows == 0) return TF_OK;

    size_t n = st->buf->n_rows;
    tf_batch *ob = tf_batch_create(st->buf->n_cols, n);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < st->buf->n_cols; c++)
        tf_batch_set_schema(ob, c, st->buf->col_names[c], st->buf->col_types[c]);

    /* Read from circular buffer in order */
    size_t start = (st->head >= n) ? (st->head - n) % st->limit : 0;
    for (size_t i = 0; i < n; i++) {
        size_t src = (start + i) % st->limit;
        tf_batch_copy_row(ob, i, st->buf, src);
        ob->n_rows = i + 1;
    }

    *out = ob;
    return TF_OK;
}

static void tail_destroy(tf_step *self) {
    tail_state *st = self->state;
    if (st) {
        if (st->buf) tf_batch_free(st->buf);
        free(st);
    }
    free(self);
}

tf_step *tf_tail_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *n_json = cJSON_GetObjectItemCaseSensitive(args, "n");
    if (!cJSON_IsNumber(n_json) || n_json->valueint <= 0) return NULL;

    tail_state *st = calloc(1, sizeof(tail_state));
    if (!st) return NULL;
    st->limit = (size_t)n_json->valueint;

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st); return NULL; }
    step->process = tail_process;
    step->flush = tail_flush;
    step->destroy = tail_destroy;
    step->state = st;
    return step;
}
