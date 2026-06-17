/*
 * op_step.c — Running aggregations: running-sum, running-avg, running-min,
 *             running-max, running-count, delta, lag, ratio.
 *
 * Config: {"column": "price", "func": "running-sum", "result": "cumsum"}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <stdint.h>

typedef enum {
    STEP_RUNNING_SUM,
    STEP_RUNNING_AVG,
    STEP_RUNNING_MIN,
    STEP_RUNNING_MAX,
    STEP_RUNNING_COUNT,
    STEP_DELTA,
    STEP_LAG,
    STEP_RATIO,
} step_func;

typedef struct {
    char     *column;
    char     *result;
    step_func func;
    double    running_sum;
    double    running_min;
    double    running_max;
    size_t    running_count;
    double    prev_val;
    int       has_prev;
} step_state;

static step_func parse_func(const char *s) {
    if (strcmp(s, "running-sum") == 0 || strcmp(s, "cumsum") == 0) return STEP_RUNNING_SUM;
    if (strcmp(s, "running-avg") == 0 || strcmp(s, "cumavg") == 0) return STEP_RUNNING_AVG;
    if (strcmp(s, "running-min") == 0) return STEP_RUNNING_MIN;
    if (strcmp(s, "running-max") == 0) return STEP_RUNNING_MAX;
    if (strcmp(s, "running-count") == 0) return STEP_RUNNING_COUNT;
    if (strcmp(s, "delta") == 0) return STEP_DELTA;
    if (strcmp(s, "lag") == 0) return STEP_LAG;
    if (strcmp(s, "ratio") == 0) return STEP_RATIO;
    return STEP_RUNNING_SUM;
}

static double get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    if (b->col_types[ci] == TF_TYPE_FLOAT64) return tf_batch_get_float64(b, r, ci);
    return 0;
}

static int step_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    step_state *st = self->state;
    *out = NULL;

    const char *extra_names[1] = {st->result};
    tf_type extra_types[1] = {TF_TYPE_FLOAT64};
    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_with_extra_cols(ob, in, extra_names, extra_types, 1) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    int ci = tf_batch_col_index(in, st->column);

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }

        if (ci < 0 || tf_batch_is_null(in, r, ci)) {
            if (tf_batch_set_null(ob, r, in->n_cols) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            if (tf_batch_expose_row(ob, r) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            continue;
        }

        double val = get_numeric(in, r, ci);
        double result = 0;
        double next_running_sum = st->running_sum;
        double next_running_min = st->running_min;
        double next_running_max = st->running_max;
        size_t next_running_count = st->running_count;
        double next_prev_val = st->prev_val;
        int next_has_prev = st->has_prev;
        int write_null = 0;

        switch (st->func) {
            case STEP_RUNNING_SUM:
                next_running_sum += val;
                result = next_running_sum;
                break;
            case STEP_RUNNING_AVG:
                if (next_running_count == SIZE_MAX) { tf_batch_free(ob); return TF_ERROR; }
                next_running_sum += val;
                next_running_count++;
                result = next_running_sum / next_running_count;
                break;
            case STEP_RUNNING_MIN:
                if (next_running_count == SIZE_MAX) { tf_batch_free(ob); return TF_ERROR; }
                if (next_running_count == 0 || val < next_running_min)
                    next_running_min = val;
                next_running_count++;
                result = next_running_min;
                break;
            case STEP_RUNNING_MAX:
                if (next_running_count == SIZE_MAX) { tf_batch_free(ob); return TF_ERROR; }
                if (next_running_count == 0 || val > next_running_max)
                    next_running_max = val;
                next_running_count++;
                result = next_running_max;
                break;
            case STEP_RUNNING_COUNT:
                if (next_running_count == SIZE_MAX) { tf_batch_free(ob); return TF_ERROR; }
                next_running_count++;
                result = (double)next_running_count;
                break;
            case STEP_DELTA:
                if (st->has_prev) result = val - st->prev_val;
                else write_null = 1;
                next_prev_val = val;
                next_has_prev = 1;
                break;
            case STEP_LAG:
                if (st->has_prev) result = st->prev_val;
                else write_null = 1;
                next_prev_val = val;
                next_has_prev = 1;
                break;
            case STEP_RATIO:
                if (st->has_prev && st->prev_val != 0) result = val / st->prev_val;
                else write_null = 1;
                next_prev_val = val;
                next_has_prev = 1;
                break;
        }

        if (write_null) {
            if (tf_batch_set_null(ob, r, in->n_cols) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            if (tf_batch_expose_row(ob, r) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            st->prev_val = next_prev_val;
            st->has_prev = next_has_prev;
            continue;
        }

        if (tf_batch_set_float64(ob, r, in->n_cols, result) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        st->running_sum = next_running_sum;
        st->running_min = next_running_min;
        st->running_max = next_running_max;
        st->running_count = next_running_count;
        st->prev_val = next_prev_val;
        st->has_prev = next_has_prev;
    }

    *out = ob;
    return TF_OK;
}

static int step_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void step_destroy(tf_step *self) {
    step_state *st = self->state;
    if (st) { free(st->column); free(st->result); free(st); }
    free(self);
}

tf_step *tf_step_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    cJSON *func_j = cJSON_GetObjectItemCaseSensitive(args, "func");
    if (!cJSON_IsString(col_j) || !cJSON_IsString(func_j)) return NULL;

    step_state *st = calloc(1, sizeof(step_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    st->func = parse_func(func_j->valuestring);

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = strdup(res_j->valuestring);
    } else {
        /* Default result name */
        char buf[256];
        snprintf(buf, sizeof(buf), "%s_%s", st->column, func_j->valuestring);
        st->result = strdup(buf);
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st->column); free(st->result); free(st); return NULL; }
    step->process = step_process;
    step->flush = step_flush;
    step->destroy = step_destroy;
    step->state = st;
    return step;
}
