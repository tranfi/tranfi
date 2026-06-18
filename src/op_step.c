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

typedef enum {
    STEP_MISSING_ERROR,
    STEP_MISSING_NULL,
    STEP_MISSING_IGNORE,
} step_missing_policy;

typedef enum {
    STEP_TYPE_FAIL,
    STEP_TYPE_NULL,
} step_type_policy;

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
    step_missing_policy missing;
    step_type_policy on_type_error;
} step_state;

static int parse_func(const char *s, step_func *out) {
    if (!s || !out) return TF_ERROR;
    if (strcmp(s, "running-sum") == 0 || strcmp(s, "cumsum") == 0) *out = STEP_RUNNING_SUM;
    else if (strcmp(s, "running-avg") == 0 || strcmp(s, "cumavg") == 0) *out = STEP_RUNNING_AVG;
    else if (strcmp(s, "running-min") == 0) *out = STEP_RUNNING_MIN;
    else if (strcmp(s, "running-max") == 0) *out = STEP_RUNNING_MAX;
    else if (strcmp(s, "running-count") == 0) *out = STEP_RUNNING_COUNT;
    else if (strcmp(s, "delta") == 0) *out = STEP_DELTA;
    else if (strcmp(s, "lag") == 0) *out = STEP_LAG;
    else if (strcmp(s, "ratio") == 0) *out = STEP_RATIO;
    else return TF_ERROR;
    return TF_OK;
}

static int step_is_numeric_func(step_func func) {
    return func != STEP_RUNNING_COUNT;
}

static int step_is_numeric_type(tf_type type) {
    return type == TF_TYPE_INT64 || type == TF_TYPE_FLOAT64;
}

static double get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    return tf_batch_get_float64(b, r, ci);
}

static void step_set_col_error(const char *column, const char *suffix) {
    char msg[512];
    snprintf(msg, sizeof(msg), "step: column '%s' %s", column ? column : "", suffix);
    tf_set_last_error(msg);
}

static int step_parse_missing_policy(const cJSON *args, step_missing_policy *out) {
    *out = STEP_MISSING_ERROR;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "missing");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("step: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "error") == 0) *out = STEP_MISSING_ERROR;
    else if (strcmp(j->valuestring, "null") == 0) *out = STEP_MISSING_NULL;
    else if (strcmp(j->valuestring, "ignore") == 0) *out = STEP_MISSING_IGNORE;
    else {
        tf_set_last_error("step: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    return TF_OK;
}

static int step_parse_type_policy(const cJSON *args, step_type_policy *out) {
    *out = STEP_TYPE_FAIL;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "on_type_error");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("step: on_type_error must be fail or null");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "fail") == 0) *out = STEP_TYPE_FAIL;
    else if (strcmp(j->valuestring, "null") == 0) *out = STEP_TYPE_NULL;
    else {
        tf_set_last_error("step: on_type_error must be fail or null");
        return TF_ERROR;
    }
    return TF_OK;
}

static int step_passthrough(tf_batch *in, tf_batch **out) {
    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_schema(ob, in) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }
    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK ||
            tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    *out = ob;
    return TF_OK;
}

static int step_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    step_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);
    int force_null = 0;
    if (ci < 0) {
        if (st->missing == STEP_MISSING_ERROR) {
            step_set_col_error(st->column, "not found");
            return TF_ERROR;
        }
        if (st->missing == STEP_MISSING_IGNORE) {
            return step_passthrough(in, out);
        }
        force_null = 1;
    } else if (step_is_numeric_func(st->func) &&
               !step_is_numeric_type(in->col_types[(size_t)ci])) {
        if (st->on_type_error == STEP_TYPE_FAIL) {
            step_set_col_error(st->column, "must be numeric");
            return TF_ERROR;
        }
        force_null = 1;
    }

    const char *extra_names[1] = {st->result};
    tf_type extra_types[1] = {TF_TYPE_FLOAT64};
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

        if (force_null || tf_batch_is_null(in, r, (size_t)ci)) {
            if (tf_batch_set_null(ob, r, in->n_cols) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            if (tf_batch_expose_row(ob, r) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            continue;
        }

        double val = step_is_numeric_func(st->func) ? get_numeric(in, r, ci) : 0.0;
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

static char *step_default_result_name(const char *column, const char *func) {
    char *suffix = tf_string_append_suffix_checked("_", func);
    if (!suffix) return NULL;
    char *result = tf_string_append_suffix_checked(column, suffix);
    free(suffix);
    return result;
}

tf_step *tf_step_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    cJSON *func_j = cJSON_GetObjectItemCaseSensitive(args, "func");
    if (!cJSON_IsString(col_j) || !col_j->valuestring[0] ||
        !cJSON_IsString(func_j) || !func_j->valuestring[0]) {
        tf_set_last_error("step: column and func are required");
        return NULL;
    }

    step_state *st = tf_callocarray_checked(1, sizeof(step_state));
    if (!st) return NULL;
    st->column = tf_strdup_checked(col_j->valuestring);
    if (!st->column) { free(st); return NULL; }
    if (parse_func(func_j->valuestring, &st->func) != TF_OK) {
        tf_set_last_error("step: func must be running-sum, running-avg, running-min, running-max, running-count, delta, lag, or ratio");
        free(st->column);
        free(st);
        return NULL;
    }
    if (step_parse_missing_policy(args, &st->missing) != TF_OK ||
        step_parse_type_policy(args, &st->on_type_error) != TF_OK) {
        free(st->column);
        free(st);
        return NULL;
    }

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = tf_strdup_checked(res_j->valuestring);
    } else {
        st->result = step_default_result_name(st->column, func_j->valuestring);
    }
    if (!st->result) { free(st->column); free(st); return NULL; }

    tf_step *step = tf_callocarray_checked(1, sizeof(tf_step));
    if (!step) { free(st->column); free(st->result); free(st); return NULL; }
    step->process = step_process;
    step->flush = step_flush;
    step->destroy = step_destroy;
    step->state = st;
    return step;
}
