/*
 * op_clip.c — Clamp numeric values to [min, max].
 *
 * Config: {"column": "score", "min": 0, "max": 100}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    char  *column;
    double min_val;
    double max_val;
    int    has_min;
    int    has_max;
} clip_state;

static int clip_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    clip_state *st = self->state;
    *out = NULL;

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);
        ob->n_rows = r + 1;
    }

    int ci = tf_batch_col_index(ob, st->column);
    if (ci >= 0) {
        for (size_t r = 0; r < ob->n_rows; r++) {
            if (tf_batch_is_null(ob, r, ci)) continue;
            if (ob->col_types[ci] == TF_TYPE_INT64) {
                int64_t v = tf_batch_get_int64(ob, r, ci);
                if (st->has_min && v < (int64_t)st->min_val) v = (int64_t)st->min_val;
                if (st->has_max && v > (int64_t)st->max_val) v = (int64_t)st->max_val;
                tf_batch_set_int64(ob, r, ci, v);
            } else if (ob->col_types[ci] == TF_TYPE_FLOAT64) {
                double v = tf_batch_get_float64(ob, r, ci);
                if (st->has_min && v < st->min_val) v = st->min_val;
                if (st->has_max && v > st->max_val) v = st->max_val;
                tf_batch_set_float64(ob, r, ci, v);
            }
        }
    }

    *out = ob;
    return TF_OK;
}

static int clip_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void clip_destroy(tf_step *self) {
    clip_state *st = self->state;
    if (st) { free(st->column); free(st); }
    free(self);
}

tf_step *tf_clip_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j)) return NULL;

    clip_state *st = calloc(1, sizeof(clip_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);

    cJSON *min_j = cJSON_GetObjectItemCaseSensitive(args, "min");
    if (cJSON_IsNumber(min_j)) { st->min_val = min_j->valuedouble; st->has_min = 1; }
    cJSON *max_j = cJSON_GetObjectItemCaseSensitive(args, "max");
    if (cJSON_IsNumber(max_j)) { st->max_val = max_j->valuedouble; st->has_max = 1; }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->column); free(st); return NULL; }
    step->process = clip_process;
    step->flush = clip_flush;
    step->destroy = clip_destroy;
    step->state = st;
    return step;
}
