/*
 * op_window.c — Sliding window aggregations.
 *
 * Config: {"column": "price", "size": 3, "func": "avg", "result": "price_avg3"}
 * Supported funcs: avg, sum, min, max, count
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <ctype.h>

typedef enum {
    WIN_AVG, WIN_SUM, WIN_MIN, WIN_MAX, WIN_COUNT,
} win_func;

typedef struct {
    char    *column;
    char    *result;
    win_func func;
    size_t   size;
    double  *ring;     /* circular buffer */
    size_t   head;     /* next write position */
    size_t   count;    /* items in buffer */
} window_state;

static win_func parse_win_func(const char *s) {
    if (strcmp(s, "avg") == 0) return WIN_AVG;
    if (strcmp(s, "sum") == 0) return WIN_SUM;
    if (strcmp(s, "min") == 0) return WIN_MIN;
    if (strcmp(s, "max") == 0) return WIN_MAX;
    if (strcmp(s, "count") == 0) return WIN_COUNT;
    return WIN_AVG;
}

static int window_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    (void)side;
    window_state *st = self->state;
    *out = NULL;

    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    tf_batch_set_schema(ob, in->n_cols, st->result, TF_TYPE_FLOAT64);

    int ci = tf_batch_col_index(in, st->column);

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);

        if (ci < 0 || tf_batch_is_null(in, r, ci)) {
            tf_batch_set_null(ob, r, in->n_cols);
            ob->n_rows = r + 1;
            continue;
        }

        double val = 0;
        if (in->col_types[ci] == TF_TYPE_INT64) val = (double)tf_batch_get_int64(in, r, ci);
        else if (in->col_types[ci] == TF_TYPE_FLOAT64) val = tf_batch_get_float64(in, r, ci);

        /* Add to ring buffer */
        st->ring[st->head] = val;
        st->head = (st->head + 1) % st->size;
        if (st->count < st->size) st->count++;

        /* Compute window aggregate */
        double result = 0;
        switch (st->func) {
            case WIN_SUM:
            case WIN_AVG: {
                double sum = 0;
                for (size_t i = 0; i < st->count; i++) sum += st->ring[i];
                result = (st->func == WIN_AVG) ? sum / st->count : sum;
                break;
            }
            case WIN_MIN: {
                result = st->ring[0];
                for (size_t i = 1; i < st->count; i++)
                    if (st->ring[i] < result) result = st->ring[i];
                break;
            }
            case WIN_MAX: {
                result = st->ring[0];
                for (size_t i = 1; i < st->count; i++)
                    if (st->ring[i] > result) result = st->ring[i];
                break;
            }
            case WIN_COUNT:
                result = (double)st->count;
                break;
        }

        tf_batch_set_float64(ob, r, in->n_cols, result);
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int window_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void window_destroy(tf_step *self) {
    window_state *st = self->state;
    if (st) { free(st->column); free(st->result); free(st->ring); free(st); }
    free(self);
}


static tf_step *tf_window_create_alias(const cJSON *args, const char *func) {
    if (!args || !func) return NULL;
    cJSON *copy = cJSON_Duplicate(args, 1);
    if (!copy) return NULL;
    cJSON_DeleteItemFromObjectCaseSensitive(copy, "func");
    if (!cJSON_AddStringToObject(copy, "func", func)) {
        cJSON_Delete(copy);
        return NULL;
    }
    tf_step *step = tf_window_create(copy);
    cJSON_Delete(copy);
    return step;
}

tf_step *tf_rolling_sum_create(const cJSON *args) {
    return tf_window_create_alias(args, "sum");
}

tf_step *tf_rolling_mean_create(const cJSON *args) {
    return tf_window_create_alias(args, "avg");
}

tf_step *tf_rolling_min_create(const cJSON *args) {
    return tf_window_create_alias(args, "min");
}

tf_step *tf_rolling_max_create(const cJSON *args) {
    return tf_window_create_alias(args, "max");
}

typedef enum {
    BOOL_ROLL_ANY,
    BOOL_ROLL_ALL,
} bool_roll_func;

typedef enum {
    BOOL_NULLS_IGNORE,
    BOOL_NULLS_FALSE,
    BOOL_NULLS_TRUE,
    BOOL_NULLS_PROPAGATE,
} bool_null_policy;

typedef struct {
    char            *column;
    char            *result;
    bool_roll_func   func;
    bool_null_policy nulls;
    size_t           size;
    uint8_t         *values;
    uint8_t         *is_null;
    size_t           head;
    size_t           count;
} bool_window_state;

static int streq_ci(const char *a, const char *b) {
    if (!a || !b) return 0;
    while (*a && *b) {
        unsigned char ca = (unsigned char)*a;
        unsigned char cb = (unsigned char)*b;
        if (tolower(ca) != tolower(cb)) return 0;
        a++;
        b++;
    }
    return *a == '\0' && *b == '\0';
}

static int parse_bool_null_policy(const cJSON *args, bool_null_policy *out) {
    *out = BOOL_NULLS_IGNORE;
    cJSON *nulls_j = cJSON_GetObjectItemCaseSensitive(args, "nulls");
    if (!nulls_j) return TF_OK;
    if (!cJSON_IsString(nulls_j)) {
        tf_set_last_error("rolling-any/all: nulls must be ignore, false, true, or propagate");
        return TF_ERROR;
    }
    const char *s = nulls_j->valuestring;
    if (strcmp(s, "ignore") == 0) *out = BOOL_NULLS_IGNORE;
    else if (strcmp(s, "false") == 0) *out = BOOL_NULLS_FALSE;
    else if (strcmp(s, "true") == 0) *out = BOOL_NULLS_TRUE;
    else if (strcmp(s, "propagate") == 0) *out = BOOL_NULLS_PROPAGATE;
    else {
        tf_set_last_error("rolling-any/all: nulls must be ignore, false, true, or propagate");
        return TF_ERROR;
    }
    return TF_OK;
}

static int cell_to_bool(const tf_batch *b, size_t row, int col, uint8_t *out) {
    if (col < 0 || tf_batch_is_null(b, row, (size_t)col)) return 0;
    switch (b->col_types[col]) {
        case TF_TYPE_BOOL:
            *out = tf_batch_get_bool(b, row, (size_t)col) ? 1 : 0;
            return 1;
        case TF_TYPE_INT64:
            *out = tf_batch_get_int64(b, row, (size_t)col) != 0 ? 1 : 0;
            return 1;
        case TF_TYPE_FLOAT64:
            *out = tf_batch_get_float64(b, row, (size_t)col) != 0.0 ? 1 : 0;
            return 1;
        case TF_TYPE_STRING: {
            const char *s = tf_batch_get_string(b, row, (size_t)col);
            if (!s) return 0;
            if (streq_ci(s, "true") || streq_ci(s, "t") || streq_ci(s, "yes") ||
                streq_ci(s, "y") || strcmp(s, "1") == 0) {
                *out = 1;
                return 1;
            }
            if (streq_ci(s, "false") || streq_ci(s, "f") || streq_ci(s, "no") ||
                streq_ci(s, "n") || strcmp(s, "0") == 0 || strcmp(s, "") == 0) {
                *out = 0;
                return 1;
            }
            return 0;
        }
        default:
            return 0;
    }
}

static int bool_window_process(tf_step *self, tf_batch *in, tf_batch **out,
                               tf_side_channels *side) {
    (void)side;
    bool_window_state *st = self->state;
    *out = NULL;

    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    tf_batch_set_schema(ob, in->n_cols, st->result, TF_TYPE_BOOL);

    int ci = tf_batch_col_index(in, st->column);

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);

        uint8_t value = 0;
        uint8_t is_null = cell_to_bool(in, r, ci, &value) ? 0 : 1;
        st->values[st->head] = value;
        st->is_null[st->head] = is_null;
        st->head = (st->head + 1) % st->size;
        if (st->count < st->size) st->count++;

        size_t non_null = 0;
        int seen_null = 0;
        int any_true = 0;
        int any_false = 0;
        for (size_t i = 0; i < st->count; i++) {
            if (st->is_null[i]) {
                seen_null = 1;
                continue;
            }
            non_null++;
            if (st->values[i]) any_true = 1;
            else any_false = 1;
        }

        int out_null = 0;
        int out_value = 0;
        switch (st->nulls) {
            case BOOL_NULLS_IGNORE:
                if (non_null == 0) out_null = 1;
                else out_value = (st->func == BOOL_ROLL_ANY) ? any_true : !any_false;
                break;
            case BOOL_NULLS_FALSE:
                out_value = (st->func == BOOL_ROLL_ANY) ? any_true : (!seen_null && !any_false);
                break;
            case BOOL_NULLS_TRUE:
                out_value = (st->func == BOOL_ROLL_ANY) ? (seen_null || any_true) : !any_false;
                break;
            case BOOL_NULLS_PROPAGATE:
                if (seen_null) out_null = 1;
                else out_value = (st->func == BOOL_ROLL_ANY) ? any_true : !any_false;
                break;
        }

        if (out_null) tf_batch_set_null(ob, r, in->n_cols);
        else tf_batch_set_bool(ob, r, in->n_cols, out_value != 0);
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int bool_window_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void bool_window_destroy(tf_step *self) {
    bool_window_state *st = self->state;
    if (st) {
        free(st->column);
        free(st->result);
        free(st->values);
        free(st->is_null);
        free(st);
    }
    free(self);
}

static tf_step *tf_rolling_bool_create(const cJSON *args, bool_roll_func func) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    cJSON *size_j = cJSON_GetObjectItemCaseSensitive(args, "size");
    if (!cJSON_IsString(col_j) || !cJSON_IsNumber(size_j)) {
        tf_set_last_error("rolling-any/all: requires column and positive size");
        return NULL;
    }
    size_t win_size = (size_t)size_j->valueint;
    if (win_size == 0) {
        tf_set_last_error("rolling-any/all: size must be positive");
        return NULL;
    }

    bool_null_policy nulls;
    if (parse_bool_null_policy(args, &nulls) != TF_OK) return NULL;

    bool_window_state *st = calloc(1, sizeof(bool_window_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    st->func = func;
    st->nulls = nulls;
    st->size = win_size;
    st->values = calloc(win_size, sizeof(uint8_t));
    st->is_null = calloc(win_size, sizeof(uint8_t));

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = strdup(res_j->valuestring);
    } else {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s_%s%zu", st->column,
                 func == BOOL_ROLL_ANY ? "any" : "all", win_size);
        st->result = strdup(buf);
    }

    if (!st->column || !st->result || !st->values || !st->is_null) {
        free(st->column); free(st->result); free(st->values); free(st->is_null); free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st->column); free(st->result); free(st->values); free(st->is_null); free(st); return NULL; }
    step->process = bool_window_process;
    step->flush = bool_window_flush;
    step->destroy = bool_window_destroy;
    step->state = st;
    return step;
}

tf_step *tf_rolling_any_create(const cJSON *args) {
    return tf_rolling_bool_create(args, BOOL_ROLL_ANY);
}

tf_step *tf_rolling_all_create(const cJSON *args) {
    return tf_rolling_bool_create(args, BOOL_ROLL_ALL);
}

tf_step *tf_window_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    cJSON *size_j = cJSON_GetObjectItemCaseSensitive(args, "size");
    cJSON *func_j = cJSON_GetObjectItemCaseSensitive(args, "func");
    if (!cJSON_IsString(col_j) || !cJSON_IsNumber(size_j) || !cJSON_IsString(func_j))
        return NULL;

    size_t win_size = (size_t)size_j->valueint;
    if (win_size == 0) win_size = 1;

    window_state *st = calloc(1, sizeof(window_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    st->func = parse_win_func(func_j->valuestring);
    st->size = win_size;
    st->ring = calloc(win_size, sizeof(double));

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = strdup(res_j->valuestring);
    } else {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s_%s%zu", st->column,
                 func_j->valuestring, win_size);
        st->result = strdup(buf);
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st->column); free(st->result); free(st->ring); free(st); return NULL; }
    step->process = window_process;
    step->flush = window_flush;
    step->destroy = window_destroy;
    step->state = st;
    return step;
}
