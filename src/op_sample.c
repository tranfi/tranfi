/*
 * op_sample.c -- Reservoir sampling (Algorithm R). Bounded memory.
 *
 * Config: {"n": 100, "seed": 123} or {"n": 100, "seed": "random"}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    size_t    n;
    tf_batch *buf;
    size_t    seen;       /* total rows seen */
    int       has_schema;
    uint64_t  rng;
} sample_state;

static uint64_t sample_random_seed(void) {
    uintptr_t stack_marker = 0;
    uintptr_t stack_addr = (uintptr_t)&stack_marker;
    return ((uint64_t)time(NULL) << 32) ^ (uint64_t)clock() ^ (uint64_t)stack_addr;
}

static uint64_t sample_next_u64(sample_state *st) {
    st->rng += UINT64_C(0x9E3779B97F4A7C15);
    uint64_t z = st->rng;
    z = (z ^ (z >> 30)) * UINT64_C(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)) * UINT64_C(0x94D049BB133111EB);
    return z ^ (z >> 31);
}

static size_t sample_bounded(sample_state *st, size_t bound) {
    if (bound <= 1) return 0;
    uint64_t b = (uint64_t)bound;
    uint64_t threshold = (uint64_t)(-b) % b;
    for (;;) {
        uint64_t r = sample_next_u64(st);
        if (r >= threshold) return (size_t)(r % b);
    }
}

static int sample_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    (void)side;
    sample_state *st = self->state;
    *out = NULL;

    if (!st->has_schema) {
        st->buf = tf_batch_create(in->n_cols, st->n);
        if (!st->buf) return TF_ERROR;
        if (tf_batch_clone_schema(st->buf, in) != TF_OK) return TF_ERROR;
        st->has_schema = 1;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        if (st->seen == SIZE_MAX) {
            tf_set_last_error("sample: row count overflow");
            return TF_ERROR;
        }
        if (st->seen < st->n) {
            if (tf_batch_copy_row(st->buf, st->seen, in, r) != TF_OK) return TF_ERROR;
            st->buf->n_rows = st->seen + 1;
        } else {
            size_t j = sample_bounded(st, st->seen + 1);
            if (j < st->n && tf_batch_copy_row(st->buf, j, in, r) != TF_OK)
                return TF_ERROR;
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

    size_t n = st->buf->n_rows;
    tf_batch *ob = tf_batch_create(st->buf->n_cols, n);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_schema(ob, st->buf) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }
    for (size_t i = 0; i < n; i++) {
        if (tf_batch_copy_row(ob, i, st->buf, i) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, i) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
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
    size_t n = 0;
    int has_n = tf_json_get_size_arg(args, "n", 1, TF_MAX_OUTPUT_ROWS_PER_BATCH, &n, "sample");
    if (has_n <= 0) return NULL;

    sample_state *st = calloc(1, sizeof(sample_state));
    if (!st) return NULL;
    st->n = n;
    st->rng = 0;

    cJSON *seed_json = cJSON_GetObjectItemCaseSensitive(args, "seed");
    if (seed_json) {
        if (cJSON_IsString(seed_json)) {
            if (strcmp(seed_json->valuestring, "random") != 0) {
                tf_set_last_error("sample: seed must be an integer or 'random'");
                free(st);
                return NULL;
            }
            st->rng = sample_random_seed();
        } else if (cJSON_IsNumber(seed_json) && seed_json->valuedouble >= 0.0) {
            size_t parsed = 0;
            if (tf_json_size_value(seed_json, "seed", 0, TF_MAX_SAFE_SIZE_ARG,
                                   &parsed, "sample") < 0) {
                free(st);
                return NULL;
            }
            st->rng = (uint64_t)parsed;
        } else {
            tf_set_last_error("sample: seed must be an integer or 'random'");
            free(st);
            return NULL;
        }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st); return NULL; }
    step->process = sample_process;
    step->flush = sample_flush;
    step->destroy = sample_destroy;
    step->state = st;
    return step;
}
