/*
 * op_anomaly.c — Streaming anomaly detection via z-score.
 * Uses Welford's online algorithm for mean/variance.
 *
 * Config: {"column": "price", "threshold": 3.0, "result": "price_anomaly"}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <stdint.h>

typedef enum {
    ANOMALY_MISSING_ERROR,
    ANOMALY_MISSING_NULL,
    ANOMALY_MISSING_IGNORE,
} anomaly_missing_policy;

typedef enum {
    ANOMALY_TYPE_FAIL,
    ANOMALY_TYPE_NULL,
} anomaly_type_policy;

typedef struct {
    char   *column;
    char   *result;
    double  threshold;
    /* Welford's online algorithm */
    size_t  count;
    double  mean;
    double  m2;
    anomaly_missing_policy missing;
    anomaly_type_policy on_type_error;
} anomaly_state;

static int anomaly_is_numeric_type(tf_type type) {
    return type == TF_TYPE_INT64 || type == TF_TYPE_FLOAT64;
}

static double anomaly_get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    return tf_batch_get_float64(b, r, ci);
}

static void anomaly_set_col_error(const char *column, const char *suffix) {
    char msg[512];
    snprintf(msg, sizeof(msg), "anomaly: column '%s' %s", column ? column : "", suffix);
    tf_set_last_error(msg);
}

static int anomaly_parse_missing_policy(const cJSON *args, anomaly_missing_policy *out) {
    *out = ANOMALY_MISSING_ERROR;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "missing");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("anomaly: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "error") == 0) *out = ANOMALY_MISSING_ERROR;
    else if (strcmp(j->valuestring, "null") == 0) *out = ANOMALY_MISSING_NULL;
    else if (strcmp(j->valuestring, "ignore") == 0) *out = ANOMALY_MISSING_IGNORE;
    else {
        tf_set_last_error("anomaly: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    return TF_OK;
}

static int anomaly_parse_type_policy(const cJSON *args, anomaly_type_policy *out) {
    *out = ANOMALY_TYPE_FAIL;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "on_type_error");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("anomaly: on_type_error must be fail or null");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "fail") == 0) *out = ANOMALY_TYPE_FAIL;
    else if (strcmp(j->valuestring, "null") == 0) *out = ANOMALY_TYPE_NULL;
    else {
        tf_set_last_error("anomaly: on_type_error must be fail or null");
        return TF_ERROR;
    }
    return TF_OK;
}

static int anomaly_passthrough(tf_batch *in, tf_batch **out) {
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

static int anomaly_process(tf_step *self, tf_batch *in, tf_batch **out,
                           tf_side_channels *side) {
    (void)side;
    anomaly_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);
    int force_null = 0;
    if (ci < 0) {
        if (st->missing == ANOMALY_MISSING_ERROR) {
            anomaly_set_col_error(st->column, "not found");
            return TF_ERROR;
        }
        if (st->missing == ANOMALY_MISSING_IGNORE) return anomaly_passthrough(in, out);
        force_null = 1;
    } else if (!anomaly_is_numeric_type(in->col_types[ci])) {
        if (st->on_type_error == ANOMALY_TYPE_FAIL) {
            anomaly_set_col_error(st->column, "must be numeric");
            return TF_ERROR;
        }
        force_null = 1;
    }

    const char *extra_names[1] = {st->result};
    tf_type extra_types[1] = {TF_TYPE_INT64};
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

        if (force_null) {
            if (tf_batch_set_null(ob, r, in->n_cols) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            if (tf_batch_expose_row(ob, r) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            continue;
        }

        if (tf_batch_is_null(in, r, ci)) {
            if (tf_batch_set_int64(ob, r, in->n_cols, 0) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            if (tf_batch_expose_row(ob, r) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            continue;
        }

        double val = anomaly_get_numeric(in, r, ci);

        if (st->count == SIZE_MAX) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        size_t next_count = st->count + 1;
        double next_mean = st->mean;
        double next_m2 = st->m2;
        double delta = val - next_mean;
        next_mean += delta / (double)next_count;
        double delta2 = val - next_mean;
        next_m2 += delta * delta2;

        /* Compute z-score (need at least 2 data points for variance) */
        int is_anomaly = 0;
        if (next_count >= 2) {
            double var = next_m2 / (double)(next_count - 1);
            double std = sqrt(var);
            if (std > 0) {
                double z = fabs((val - next_mean) / std);
                is_anomaly = z > st->threshold ? 1 : 0;
            }
        }

        if (tf_batch_set_int64(ob, r, in->n_cols, is_anomaly) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        st->count = next_count;
        st->mean = next_mean;
        st->m2 = next_m2;
    }

    *out = ob;
    return TF_OK;
}

static int anomaly_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void anomaly_destroy(tf_step *self) {
    anomaly_state *st = self->state;
    if (st) { free(st->column); free(st->result); free(st); }
    free(self);
}

tf_step *tf_anomaly_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j) || !col_j->valuestring[0]) {
        tf_set_last_error("anomaly: column is required");
        return NULL;
    }

    anomaly_state *st = calloc(1, sizeof(anomaly_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    if (!st->column) { free(st); return NULL; }

    cJSON *thresh_j = cJSON_GetObjectItemCaseSensitive(args, "threshold");
    st->threshold = cJSON_IsNumber(thresh_j) ? thresh_j->valuedouble : 3.0;
    if (!isfinite(st->threshold) || st->threshold < 0.0) {
        tf_set_last_error("anomaly: threshold must be a non-negative finite number");
        free(st->column);
        free(st);
        return NULL;
    }
    if (anomaly_parse_missing_policy(args, &st->missing) != TF_OK ||
        anomaly_parse_type_policy(args, &st->on_type_error) != TF_OK) {
        free(st->column);
        free(st);
        return NULL;
    }

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = strdup(res_j->valuestring);
    } else {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s_anomaly", st->column);
        st->result = strdup(buf);
    }
    if (!st->result) { free(st->column); free(st); return NULL; }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st->column); free(st->result); free(st); return NULL; }
    step->process = anomaly_process;
    step->flush = anomaly_flush;
    step->destroy = anomaly_destroy;
    step->state = st;
    return step;
}
