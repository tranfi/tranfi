/*
 * op_split_data.c — Train/test split with probabilistic assignment.
 * Uses LCG PRNG for reproducible splitting.
 *
 * Config: {"ratio": 0.8, "result": "_split", "seed": 42}
 */

#include "internal.h"
#include "cJSON.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char   *result;
    double  ratio;
    uint64_t seed;
    uint64_t row_index;
} split_data_state;

/* Simple LCG for deterministic random */
static double lcg_random(uint64_t seed, uint64_t index) {
    uint64_t x = seed ^ (index * 6364136223846793005ULL + 1442695040888963407ULL);
    x = x * 6364136223846793005ULL + 1442695040888963407ULL;
    x = x * 6364136223846793005ULL + 1442695040888963407ULL;
    return (double)(x >> 33) / (double)(1ULL << 31);
}

static int split_data_process(tf_step *self, tf_batch *in, tf_batch **out,
                              tf_side_channels *side) {
    (void)side;
    split_data_state *st = self->state;
    *out = NULL;

    const char *extra_names[1] = {st->result};
    tf_type extra_types[1] = {TF_TYPE_STRING};
    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_with_extra_cols(ob, in, extra_names, extra_types, 1) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }

        double rval = lcg_random(st->seed, st->row_index);
        const char *label = (rval < st->ratio) ? "train" : "test";
        if (tf_batch_set_string(ob, r, in->n_cols, label) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        st->row_index++;
    }

    *out = ob;
    return TF_OK;
}

static int split_data_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void split_data_destroy(tf_step *self) {
    split_data_state *st = self->state;
    if (st) { free(st->result); free(st); }
    free(self);
}

tf_step *tf_split_data_create(const cJSON *args) {
    if (!args) return NULL;

    split_data_state *st = calloc(1, sizeof(split_data_state));
    if (!st) return NULL;

    cJSON *ratio_j = cJSON_GetObjectItemCaseSensitive(args, "ratio");
    st->ratio = 0.8;
    if (ratio_j) {
        if (!cJSON_IsNumber(ratio_j) || !isfinite(ratio_j->valuedouble) ||
            ratio_j->valuedouble < 0.0 || ratio_j->valuedouble > 1.0) {
            tf_set_last_error("split-data: ratio must be a finite number between 0 and 1");
            free(st);
            return NULL;
        }
        st->ratio = ratio_j->valuedouble;
    }

    cJSON *seed_j = cJSON_GetObjectItemCaseSensitive(args, "seed");
    st->seed = 42;
    if (seed_j) {
        size_t parsed_seed = 0;
        if (tf_json_size_value(seed_j, "seed", 0, TF_MAX_SAFE_SIZE_ARG,
                               &parsed_seed, "split-data") < 0) {
            free(st);
            return NULL;
        }
        st->seed = (uint64_t)parsed_seed;
    }

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    st->result = strdup(cJSON_IsString(res_j) ? res_j->valuestring : "_split");
    if (!st->result) { free(st); return NULL; }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st->result); free(st); return NULL; }
    step->process = split_data_process;
    step->flush = split_data_flush;
    step->destroy = split_data_destroy;
    step->state = st;
    return step;
}
