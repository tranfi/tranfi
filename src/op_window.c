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

typedef enum {
    WIN_MISSING_ERROR,
    WIN_MISSING_NULL,
    WIN_MISSING_IGNORE,
} window_missing_policy;

typedef enum {
    WIN_TYPE_FAIL,
    WIN_TYPE_NULL,
} window_type_policy;

typedef struct {
    char    *column;
    char    *result;
    char    *op_label;
    win_func func;
    size_t   size;
    double  *ring;     /* circular buffer */
    size_t   head;     /* next write position */
    size_t   count;    /* items in buffer */
    window_missing_policy missing;
    window_type_policy on_type_error;
} window_state;

static int parse_win_func(const char *s, win_func *out) {
    if (strcmp(s, "avg") == 0) { *out = WIN_AVG; return TF_OK; }
    if (strcmp(s, "sum") == 0) { *out = WIN_SUM; return TF_OK; }
    if (strcmp(s, "min") == 0) { *out = WIN_MIN; return TF_OK; }
    if (strcmp(s, "max") == 0) { *out = WIN_MAX; return TF_OK; }
    if (strcmp(s, "count") == 0) { *out = WIN_COUNT; return TF_OK; }
    return TF_ERROR;
}

static int window_is_numeric_type(tf_type type) {
    return type == TF_TYPE_INT64 || type == TF_TYPE_FLOAT64;
}

static double window_get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    return tf_batch_get_float64(b, r, ci);
}

static const char *window_label(const window_state *st) {
    return (st && st->op_label) ? st->op_label : "window";
}

static void window_set_col_error(const window_state *st, const char *suffix) {
    char msg[512];
    snprintf(msg, sizeof(msg), "%s: column '%s' %s", window_label(st), st && st->column ? st->column : "", suffix);
    tf_set_last_error(msg);
}

static int window_parse_missing_policy(const cJSON *args, const char *label, window_missing_policy *out) {
    *out = WIN_MISSING_ERROR;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "missing");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        char msg[128];
        snprintf(msg, sizeof(msg), "%s: missing must be error, null, or ignore", label);
        tf_set_last_error(msg);
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "error") == 0) *out = WIN_MISSING_ERROR;
    else if (strcmp(j->valuestring, "null") == 0) *out = WIN_MISSING_NULL;
    else if (strcmp(j->valuestring, "ignore") == 0) *out = WIN_MISSING_IGNORE;
    else {
        char msg[128];
        snprintf(msg, sizeof(msg), "%s: missing must be error, null, or ignore", label);
        tf_set_last_error(msg);
        return TF_ERROR;
    }
    return TF_OK;
}

static int window_parse_type_policy(const cJSON *args, const char *label, window_type_policy *out) {
    *out = WIN_TYPE_FAIL;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "on_type_error");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        char msg[128];
        snprintf(msg, sizeof(msg), "%s: on_type_error must be fail or null", label);
        tf_set_last_error(msg);
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "fail") == 0) *out = WIN_TYPE_FAIL;
    else if (strcmp(j->valuestring, "null") == 0) *out = WIN_TYPE_NULL;
    else {
        char msg[128];
        snprintf(msg, sizeof(msg), "%s: on_type_error must be fail or null", label);
        tf_set_last_error(msg);
        return TF_ERROR;
    }
    return TF_OK;
}

static int window_passthrough(tf_batch *in, tf_batch **out) {
    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_schema(ob, in) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }
    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    *out = ob;
    return TF_OK;
}

static int window_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    (void)side;
    window_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);
    int force_null = 0;
    if (ci < 0) {
        if (st->missing == WIN_MISSING_ERROR) {
            window_set_col_error(st, "not found");
            return TF_ERROR;
        }
        if (st->missing == WIN_MISSING_IGNORE) return window_passthrough(in, out);
        force_null = 1;
    } else if (st->func != WIN_COUNT && !window_is_numeric_type(in->col_types[ci])) {
        if (st->on_type_error == WIN_TYPE_FAIL) {
            window_set_col_error(st, "must be numeric");
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

        if (force_null || tf_batch_is_null(in, r, ci)) {
            if (tf_batch_set_null(ob, r, in->n_cols) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            if (tf_batch_expose_row(ob, r) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            continue;
        }

        double val = st->func == WIN_COUNT ? 0.0 : window_get_numeric(in, r, ci);
        size_t next_head = (st->head + 1) % st->size;
        size_t next_count = st->count < st->size ? st->count + 1 : st->count;

        /* Compute window aggregate */
        double result = 0;
        switch (st->func) {
            case WIN_SUM:
            case WIN_AVG: {
                double sum = 0;
                for (size_t i = 0; i < next_count; i++)
                    sum += i == st->head ? val : st->ring[i];
                result = (st->func == WIN_AVG) ? sum / next_count : sum;
                break;
            }
            case WIN_MIN: {
                result = (st->head == 0) ? val : st->ring[0];
                for (size_t i = 1; i < next_count; i++) {
                    double item = i == st->head ? val : st->ring[i];
                    if (item < result) result = item;
                }
                break;
            }
            case WIN_MAX: {
                result = (st->head == 0) ? val : st->ring[0];
                for (size_t i = 1; i < next_count; i++) {
                    double item = i == st->head ? val : st->ring[i];
                    if (item > result) result = item;
                }
                break;
            }
            case WIN_COUNT:
                result = (double)next_count;
                break;
        }

        if (tf_batch_set_float64(ob, r, in->n_cols, result) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        st->ring[st->head] = val;
        st->head = next_head;
        st->count = next_count;
    }

    *out = ob;
    return TF_OK;
}

static int window_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void window_destroy(tf_step *self) {
    window_state *st = self->state;
    if (st) { free(st->column); free(st->result); free(st->op_label); free(st->ring); free(st); }
    free(self);
}


static tf_step *tf_window_create_alias(const cJSON *args, const char *func, const char *label) {
    if (!args || !func || !label) return NULL;
    cJSON *copy = cJSON_Duplicate(args, 1);
    if (!copy) return NULL;
    cJSON_DeleteItemFromObjectCaseSensitive(copy, "func");
    cJSON_DeleteItemFromObjectCaseSensitive(copy, "__op_label");
    if (!cJSON_AddStringToObject(copy, "__op_label", label) ||
        !cJSON_AddStringToObject(copy, "func", func)) {
        cJSON_Delete(copy);
        return NULL;
    }
    tf_step *step = tf_window_create(copy);
    cJSON_Delete(copy);
    return step;
}

tf_step *tf_rolling_sum_create(const cJSON *args) {
    return tf_window_create_alias(args, "sum", "rolling-sum");
}

tf_step *tf_rolling_mean_create(const cJSON *args) {
    return tf_window_create_alias(args, "avg", "rolling-mean");
}

tf_step *tf_rolling_min_create(const cJSON *args) {
    return tf_window_create_alias(args, "min", "rolling-min");
}

tf_step *tf_rolling_max_create(const cJSON *args) {
    return tf_window_create_alias(args, "max", "rolling-max");
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

    const char *extra_names[1] = {st->result};
    tf_type extra_types[1] = {TF_TYPE_BOOL};
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

        uint8_t value = 0;
        uint8_t is_null = cell_to_bool(in, r, ci, &value) ? 0 : 1;
        size_t next_head = (st->head + 1) % st->size;
        size_t next_count = st->count < st->size ? st->count + 1 : st->count;

        size_t non_null = 0;
        int seen_null = 0;
        int any_true = 0;
        int any_false = 0;
        for (size_t i = 0; i < next_count; i++) {
            uint8_t item_null = i == st->head ? is_null : st->is_null[i];
            uint8_t item_value = i == st->head ? value : st->values[i];
            if (item_null) {
                seen_null = 1;
                continue;
            }
            non_null++;
            if (item_value) any_true = 1;
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

        int write_rc = out_null ? tf_batch_set_null(ob, r, in->n_cols)
                                : tf_batch_set_bool(ob, r, in->n_cols, out_value != 0);
        if (write_rc != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        st->values[st->head] = value;
        st->is_null[st->head] = is_null;
        st->head = next_head;
        st->count = next_count;
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
    size_t win_size = 0;
    int has_size = tf_json_get_size_arg(args, "size",
                                        1, TF_MAX_WINDOW_SIZE,
                                        &win_size, "rolling-any/all");
    if (has_size < 0) return NULL;
    if (!cJSON_IsString(col_j) || has_size == 0) {
        tf_set_last_error("rolling-any/all: requires column and positive size");
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
    cJSON *func_j = cJSON_GetObjectItemCaseSensitive(args, "func");
    cJSON *label_j = cJSON_GetObjectItemCaseSensitive(args, "__op_label");
    const char *label = cJSON_IsString(label_j) ? label_j->valuestring : "window";
    size_t win_size = 0;
    int has_size = tf_json_get_size_arg(args, "size",
                                        1, TF_MAX_WINDOW_SIZE,
                                        &win_size, label);
    if (has_size < 0) return NULL;
    if (!cJSON_IsString(col_j) || !col_j->valuestring[0] || has_size == 0 || !cJSON_IsString(func_j)) {
        char msg[128];
        snprintf(msg, sizeof(msg), "%s: column, size, and func are required", label);
        tf_set_last_error(msg);
        return NULL;
    }

    win_func func;
    if (parse_win_func(func_j->valuestring, &func) != TF_OK) {
        char msg[128];
        snprintf(msg, sizeof(msg), "%s: func must be avg, sum, min, max, or count", label);
        tf_set_last_error(msg);
        return NULL;
    }

    window_state *st = calloc(1, sizeof(window_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    st->op_label = strdup(label);
    st->func = func;
    st->size = win_size;
    st->ring = calloc(win_size, sizeof(double));
    if (!st->column || !st->op_label || !st->ring) {
        free(st->column); free(st->op_label); free(st->ring); free(st); return NULL;
    }
    if (window_parse_missing_policy(args, label, &st->missing) != TF_OK ||
        window_parse_type_policy(args, label, &st->on_type_error) != TF_OK) {
        free(st->column); free(st->op_label); free(st->ring); free(st); return NULL;
    }

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = strdup(res_j->valuestring);
    } else {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s_%s%zu", st->column,
                 func_j->valuestring, win_size);
        st->result = strdup(buf);
    }
    if (!st->result) { free(st->column); free(st->op_label); free(st->ring); free(st); return NULL; }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st->column); free(st->result); free(st->op_label); free(st->ring); free(st); return NULL; }
    step->process = window_process;
    step->flush = window_flush;
    step->destroy = window_destroy;
    step->state = st;
    return step;
}
