/*
 * op_bin.c — Discretize numeric column into labeled bins.
 *
 * Config: {"column": "age", "boundaries": [18, 30, 50, 65]}
 * Adds <col>_bin column with labels like "<18", "18-30", "30-50", "50-65", "65+"
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

typedef enum {
    BIN_MISSING_ERROR,
    BIN_MISSING_NULL,
    BIN_MISSING_IGNORE,
} bin_missing_policy;

typedef enum {
    BIN_TYPE_FAIL,
    BIN_TYPE_NULL,
} bin_type_policy;

typedef struct {
    char   *column;
    double *boundaries;
    size_t  n_boundaries;
    bin_missing_policy missing;
    bin_type_policy on_type_error;
} bin_state;

static int bin_is_numeric_type(tf_type type) {
    return type == TF_TYPE_INT64 || type == TF_TYPE_FLOAT64;
}

static double bin_get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    return tf_batch_get_float64(b, r, ci);
}

static void bin_set_col_error(const char *column, const char *suffix) {
    char msg[512];
    snprintf(msg, sizeof(msg), "bin: column '%s' %s", column ? column : "", suffix);
    tf_set_last_error(msg);
}

static int bin_parse_missing_policy(const cJSON *args, bin_missing_policy *out) {
    *out = BIN_MISSING_ERROR;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "missing");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("bin: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "error") == 0) *out = BIN_MISSING_ERROR;
    else if (strcmp(j->valuestring, "null") == 0) *out = BIN_MISSING_NULL;
    else if (strcmp(j->valuestring, "ignore") == 0) *out = BIN_MISSING_IGNORE;
    else {
        tf_set_last_error("bin: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    return TF_OK;
}

static int bin_parse_type_policy(const cJSON *args, bin_type_policy *out) {
    *out = BIN_TYPE_FAIL;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "on_type_error");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("bin: on_type_error must be fail or null");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "fail") == 0) *out = BIN_TYPE_FAIL;
    else if (strcmp(j->valuestring, "null") == 0) *out = BIN_TYPE_NULL;
    else {
        tf_set_last_error("bin: on_type_error must be fail or null");
        return TF_ERROR;
    }
    return TF_OK;
}

static int bin_passthrough(tf_batch *in, tf_batch **out) {
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
        ob->n_rows = r + 1;
    }
    *out = ob;
    return TF_OK;
}

static int bin_process(tf_step *self, tf_batch *in, tf_batch **out,
                       tf_side_channels *side) {
    (void)side;
    bin_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);
    int force_null = 0;
    if (ci < 0) {
        if (st->missing == BIN_MISSING_ERROR) {
            bin_set_col_error(st->column, "not found");
            return TF_ERROR;
        }
        if (st->missing == BIN_MISSING_IGNORE) return bin_passthrough(in, out);
        force_null = 1;
    } else if (!bin_is_numeric_type(in->col_types[ci])) {
        if (st->on_type_error == BIN_TYPE_FAIL) {
            bin_set_col_error(st->column, "must be numeric");
            return TF_ERROR;
        }
        force_null = 1;
    }

    char bin_col_name[256];
    snprintf(bin_col_name, sizeof(bin_col_name), "%s_bin", st->column);
    const char *extra_names[1] = {bin_col_name};
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

        if (force_null || tf_batch_is_null(in, r, ci)) {
            if (tf_batch_set_null(ob, r, in->n_cols) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
        } else {
            double val = bin_get_numeric(in, r, ci);

            char label[64];
            if (st->n_boundaries == 0) {
                snprintf(label, sizeof(label), TF_FLOAT64_ROUNDTRIP_FORMAT, val);
            } else if (val < st->boundaries[0]) {
                snprintf(label, sizeof(label), "<" TF_FLOAT64_ROUNDTRIP_FORMAT, st->boundaries[0]);
            } else {
                size_t b;
                for (b = 1; b < st->n_boundaries; b++) {
                    if (val < st->boundaries[b]) break;
                }
                if (b < st->n_boundaries) {
                    snprintf(label, sizeof(label), TF_FLOAT64_ROUNDTRIP_FORMAT "-" TF_FLOAT64_ROUNDTRIP_FORMAT, st->boundaries[b - 1], st->boundaries[b]);
                } else {
                    snprintf(label, sizeof(label), TF_FLOAT64_ROUNDTRIP_FORMAT "+", st->boundaries[st->n_boundaries - 1]);
                }
            }
            if (tf_batch_set_string(ob, r, in->n_cols, label) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
        }
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int bin_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void bin_destroy(tf_step *self) {
    bin_state *st = self->state;
    if (st) { free(st->column); free(st->boundaries); free(st); }
    free(self);
}

tf_step *tf_bin_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j) || !col_j->valuestring[0]) {
        tf_set_last_error("bin: column is required");
        return NULL;
    }

    bin_state *st = calloc(1, sizeof(bin_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    if (!st->column) { free(st); return NULL; }
    if (bin_parse_missing_policy(args, &st->missing) != TF_OK ||
        bin_parse_type_policy(args, &st->on_type_error) != TF_OK) {
        free(st->column);
        free(st);
        return NULL;
    }

    cJSON *bounds = cJSON_GetObjectItemCaseSensitive(args, "boundaries");
    if (!bounds || !cJSON_IsArray(bounds)) {
        tf_set_last_error("bin: boundaries must be an array");
        free(st->column);
        free(st);
        return NULL;
    }
    int n = cJSON_GetArraySize(bounds);
    if (n > 0) {
        st->boundaries = malloc((size_t)n * sizeof(double));
        if (!st->boundaries) { free(st->column); free(st); return NULL; }
        st->n_boundaries = (size_t)n;
        for (int i = 0; i < n; i++) {
            cJSON *item = cJSON_GetArrayItem(bounds, i);
            if (!cJSON_IsNumber(item) || !isfinite(item->valuedouble)) {
                tf_set_last_error("bin: boundaries must be finite, strictly increasing numbers");
                free(st->column); free(st->boundaries); free(st); return NULL;
            }
            if (i > 0 && item->valuedouble <= st->boundaries[i - 1]) {
                tf_set_last_error("bin: boundaries must be finite, strictly increasing numbers");
                free(st->column); free(st->boundaries); free(st); return NULL;
            }
            st->boundaries[i] = item->valuedouble;
        }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st->column); free(st->boundaries); free(st); return NULL; }
    step->process = bin_process;
    step->flush = bin_flush;
    step->destroy = bin_destroy;
    step->state = st;
    return step;
}
