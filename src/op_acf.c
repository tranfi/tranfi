/*
 * op_acf.c — Autocorrelation function.
 * Aggregate op: buffers numeric values, computes ACF for lags 0..N.
 * Output: 2-column table (lag, acf).
 *
 * Config: {"column": "price", "lags": 20}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <limits.h>

typedef enum {
    ACF_MISSING_ERROR,
    ACF_MISSING_NULL,
    ACF_MISSING_IGNORE
} acf_missing_policy;

typedef enum {
    ACF_TYPE_FAIL,
    ACF_TYPE_NULL
} acf_type_policy;

typedef struct {
    char   *column;
    int     lags;
    double *values;
    size_t  n_values;
    size_t  cap_values;
    int     checked_schema;
    int     col_idx;
    int     ignore;
    int     force_null;
    acf_missing_policy missing;
    acf_type_policy on_type_error;
} acf_state;

static int acf_is_numeric(tf_type type) {
    return type == TF_TYPE_INT64 || type == TF_TYPE_FLOAT64;
}

static double get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    if (b->col_types[ci] == TF_TYPE_FLOAT64) return tf_batch_get_float64(b, r, ci);
    return 0;
}

static int acf_parse_missing_policy(const cJSON *args, acf_missing_policy *out) {
    *out = ACF_MISSING_ERROR;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "missing");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("acf: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "error") == 0) *out = ACF_MISSING_ERROR;
    else if (strcmp(j->valuestring, "null") == 0) *out = ACF_MISSING_NULL;
    else if (strcmp(j->valuestring, "ignore") == 0) *out = ACF_MISSING_IGNORE;
    else {
        tf_set_last_error("acf: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    return TF_OK;
}

static int acf_parse_type_policy(const cJSON *args, acf_type_policy *out) {
    *out = ACF_TYPE_FAIL;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "on_type_error");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("acf: on_type_error must be fail or null");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "fail") == 0) *out = ACF_TYPE_FAIL;
    else if (strcmp(j->valuestring, "null") == 0) *out = ACF_TYPE_NULL;
    else {
        tf_set_last_error("acf: on_type_error must be fail or null");
        return TF_ERROR;
    }
    return TF_OK;
}

static int acf_set_col_error(acf_state *st, const tf_batch *in) {
    if (st->col_idx < 0) {
        char msg[256];
        snprintf(msg, sizeof(msg), "acf: column '%s' not found", st->column);
        tf_set_last_error(msg);
        return TF_ERROR;
    }
    if (!acf_is_numeric(in->col_types[st->col_idx])) {
        char msg[256];
        snprintf(msg, sizeof(msg), "acf: column '%s' must be numeric", st->column);
        tf_set_last_error(msg);
        return TF_ERROR;
    }
    return TF_OK;
}

static int acf_check_schema(acf_state *st, const tf_batch *in) {
    if (st->checked_schema) return TF_OK;
    st->checked_schema = 1;
    st->col_idx = tf_batch_col_index(in, st->column);
    if (st->col_idx < 0) {
        if (st->missing == ACF_MISSING_ERROR) return acf_set_col_error(st, in);
        if (st->missing == ACF_MISSING_IGNORE) st->ignore = 1;
        else st->force_null = 1;
        return TF_OK;
    }
    if (!acf_is_numeric(in->col_types[st->col_idx])) {
        if (st->on_type_error == ACF_TYPE_FAIL) return acf_set_col_error(st, in);
        st->force_null = 1;
    }
    return TF_OK;
}

static int acf_process(tf_step *self, tf_batch *in, tf_batch **out,
                       tf_side_channels *side) {
    (void)side;
    acf_state *st = self->state;
    *out = NULL;

    if (acf_check_schema(st, in) != TF_OK) return TF_ERROR;
    if (st->ignore || st->force_null || st->col_idx < 0) return TF_OK;

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_is_null(in, r, (size_t)st->col_idx)) continue;
        double val = get_numeric(in, r, st->col_idx);

        if (st->n_values >= st->cap_values) {
            size_t min_cap = 0, newcap = 0;
            if (tf_size_add(st->n_values, 1, &min_cap) != TF_OK ||
                tf_size_grow_pow2(st->cap_values, min_cap, 256, &newcap) != TF_OK) {
                return TF_ERROR;
            }
            double *tmp = tf_reallocarray_checked(st->values, newcap, sizeof(double));
            if (!tmp) return TF_ERROR;
            st->values = tmp;
            st->cap_values = newcap;
        }
        st->values[st->n_values++] = val;
    }

    return TF_OK;
}

static int acf_expose_row(tf_batch *ob, size_t row) {
    size_t next_rows = 0;
    if (tf_size_add(row, 1, &next_rows) != TF_OK) return TF_ERROR;
    ob->n_rows = next_rows;
    return TF_OK;
}

static int acf_emit_nulls(acf_state *st, tf_batch **out) {
    size_t out_rows = 0;
    if (tf_size_add((size_t)st->lags, 1, &out_rows) != TF_OK) return TF_ERROR;
    tf_batch *ob = tf_batch_create(2, out_rows);
    if (!ob) return TF_ERROR;
    if (tf_batch_set_schema(ob, 0, "lag", TF_TYPE_INT64) != TF_OK ||
        tf_batch_set_schema(ob, 1, "acf", TF_TYPE_FLOAT64) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }
    for (int k = 0; k <= st->lags; k++) {
        if (tf_batch_set_int64(ob, (size_t)k, 0, k) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (acf_expose_row(ob, (size_t)k) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    *out = ob;
    return TF_OK;
}

static int acf_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    acf_state *st = self->state;
    *out = NULL;

    if (st->ignore) return TF_OK;
    if (st->force_null) return acf_emit_nulls(st, out);

    size_t n = st->n_values;
    if (n < 2) return TF_OK;

    int max_lag = st->lags;
    if ((size_t)max_lag >= n) max_lag = (int)(n - 1);

    double mean = 0;
    for (size_t i = 0; i < n; i++) mean += st->values[i];
    mean /= (double)n;

    double var = 0;
    for (size_t i = 0; i < n; i++) {
        double d = st->values[i] - mean;
        var += d * d;
    }
    if (var == 0) return TF_OK;

    size_t out_rows = 0;
    if (tf_size_add((size_t)max_lag, 1, &out_rows) != TF_OK) return TF_ERROR;
    tf_batch *ob = tf_batch_create(2, out_rows);
    if (!ob) return TF_ERROR;
    if (tf_batch_set_schema(ob, 0, "lag", TF_TYPE_INT64) != TF_OK ||
        tf_batch_set_schema(ob, 1, "acf", TF_TYPE_FLOAT64) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    for (int k = 0; k <= max_lag; k++) {
        double cov = 0;
        for (size_t i = 0; i < n - (size_t)k; i++) {
            cov += (st->values[i] - mean) * (st->values[i + k] - mean);
        }
        double acf_val = cov / var;

        if (tf_batch_set_int64(ob, (size_t)k, 0, k) != TF_OK ||
            tf_batch_set_float64(ob, (size_t)k, 1, acf_val) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (acf_expose_row(ob, (size_t)k) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    *out = ob;
    return TF_OK;
}

static void acf_destroy(tf_step *self) {
    acf_state *st = self ? self->state : NULL;
    if (st) {
        free(st->column);
        free(st->values);
        free(st);
    }
    free(self);
}

static int parse_lags(const cJSON *lags_j, int *out) {
    *out = 20;
    if (!lags_j) return TF_OK;
    size_t parsed_lags = 0;
    if (tf_json_size_value(lags_j, "lags", 1, (size_t)INT_MAX,
                           &parsed_lags, "acf") < 0) {
        return TF_ERROR;
    }
    *out = (int)parsed_lags;
    return TF_OK;
}

tf_step *tf_acf_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j) || !col_j->valuestring[0]) {
        tf_set_last_error("acf: column is required");
        return NULL;
    }

    acf_state *st = calloc(1, sizeof(acf_state));
    if (!st) return NULL;
    st->col_idx = -1;
    st->column = strdup(col_j->valuestring);
    if (!st->column) { free(st); return NULL; }

    cJSON *lags_j = cJSON_GetObjectItemCaseSensitive(args, "lags");
    if (parse_lags(lags_j, &st->lags) != TF_OK ||
        acf_parse_missing_policy(args, &st->missing) != TF_OK ||
        acf_parse_type_policy(args, &st->on_type_error) != TF_OK) {
        free(st->column);
        free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st->column); free(st); return NULL; }
    step->process = acf_process;
    step->flush = acf_flush;
    step->destroy = acf_destroy;
    step->state = st;
    return step;
}
