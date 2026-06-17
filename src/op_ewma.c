/*
 * op_ewma.c — Exponentially weighted moving average.
 *
 * Config: {"column": "price", "alpha": 0.3, "result": "price_ewma"}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

typedef enum {
    EWMA_MISSING_ERROR,
    EWMA_MISSING_NULL,
    EWMA_MISSING_IGNORE,
} ewma_missing_policy;

typedef enum {
    EWMA_TYPE_FAIL,
    EWMA_TYPE_NULL,
} ewma_type_policy;

typedef struct {
    char   *column;
    char   *result;
    double  alpha;
    double  ewma;
    int     initialized;
    ewma_missing_policy missing;
    ewma_type_policy on_type_error;
} ewma_state;

static int ewma_is_numeric_type(tf_type type) {
    return type == TF_TYPE_INT64 || type == TF_TYPE_FLOAT64;
}

static double ewma_get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    return tf_batch_get_float64(b, r, ci);
}

static void ewma_set_col_error(const char *column, const char *suffix) {
    char msg[512];
    snprintf(msg, sizeof(msg), "ewma: column '%s' %s", column ? column : "", suffix);
    tf_set_last_error(msg);
}

static int ewma_parse_missing_policy(const cJSON *args, ewma_missing_policy *out) {
    *out = EWMA_MISSING_ERROR;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "missing");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("ewma: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "error") == 0) *out = EWMA_MISSING_ERROR;
    else if (strcmp(j->valuestring, "null") == 0) *out = EWMA_MISSING_NULL;
    else if (strcmp(j->valuestring, "ignore") == 0) *out = EWMA_MISSING_IGNORE;
    else {
        tf_set_last_error("ewma: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    return TF_OK;
}

static int ewma_parse_type_policy(const cJSON *args, ewma_type_policy *out) {
    *out = EWMA_TYPE_FAIL;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "on_type_error");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("ewma: on_type_error must be fail or null");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "fail") == 0) *out = EWMA_TYPE_FAIL;
    else if (strcmp(j->valuestring, "null") == 0) *out = EWMA_TYPE_NULL;
    else {
        tf_set_last_error("ewma: on_type_error must be fail or null");
        return TF_ERROR;
    }
    return TF_OK;
}

static int ewma_passthrough(tf_batch *in, tf_batch **out) {
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

static int ewma_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    ewma_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);
    int force_null = 0;
    if (ci < 0) {
        if (st->missing == EWMA_MISSING_ERROR) {
            ewma_set_col_error(st->column, "not found");
            return TF_ERROR;
        }
        if (st->missing == EWMA_MISSING_IGNORE) return ewma_passthrough(in, out);
        force_null = 1;
    } else if (!ewma_is_numeric_type(in->col_types[ci])) {
        if (st->on_type_error == EWMA_TYPE_FAIL) {
            ewma_set_col_error(st->column, "must be numeric");
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

        double val = ewma_get_numeric(in, r, ci);
        double next_ewma = st->initialized
            ? st->alpha * val + (1.0 - st->alpha) * st->ewma
            : val;

        if (tf_batch_set_float64(ob, r, in->n_cols, next_ewma) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        st->ewma = next_ewma;
        st->initialized = 1;
    }

    *out = ob;
    return TF_OK;
}

static int ewma_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void ewma_destroy(tf_step *self) {
    ewma_state *st = self->state;
    if (st) { free(st->column); free(st->result); free(st); }
    free(self);
}

tf_step *tf_ewma_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    cJSON *alpha_j = cJSON_GetObjectItemCaseSensitive(args, "alpha");
    if (!cJSON_IsString(col_j) || !col_j->valuestring[0] || !cJSON_IsNumber(alpha_j)) {
        tf_set_last_error("ewma: column and alpha are required");
        return NULL;
    }
    double alpha = alpha_j->valuedouble;
    if (!isfinite(alpha) || alpha < 0.0 || alpha > 1.0) {
        tf_set_last_error("ewma: alpha must be a finite number between 0 and 1");
        return NULL;
    }

    ewma_state *st = calloc(1, sizeof(ewma_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    st->alpha = alpha;
    if (!st->column) { free(st); return NULL; }
    if (ewma_parse_missing_policy(args, &st->missing) != TF_OK ||
        ewma_parse_type_policy(args, &st->on_type_error) != TF_OK) {
        free(st->column);
        free(st);
        return NULL;
    }

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = strdup(res_j->valuestring);
    } else {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s_ewma", st->column);
        st->result = strdup(buf);
    }
    if (!st->result) { free(st->column); free(st); return NULL; }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st->column); free(st->result); free(st); return NULL; }
    step->process = ewma_process;
    step->flush = ewma_flush;
    step->destroy = ewma_destroy;
    step->state = st;
    return step;
}
