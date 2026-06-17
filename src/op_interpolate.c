/*
 * op_interpolate.c — Fill null values via interpolation.
 * Methods: forward, backward, linear.
 *
 * For backward/linear: buffers rows with null target values and emits
 * them when the next non-null value arrives.
 *
 * Config: {"column": "price", "method": "linear"}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef enum {
    INTERP_FORWARD,
    INTERP_BACKWARD,
    INTERP_LINEAR
} interp_method;

typedef enum {
    INTERP_MISSING_ERROR,
    INTERP_MISSING_NULL,
    INTERP_MISSING_IGNORE,
} interp_missing_policy;

typedef enum {
    INTERP_TYPE_FAIL,
    INTERP_TYPE_NULL,
} interp_type_policy;

/* Buffered row: we store complete batches and track which rows are pending */
typedef struct pending_row {
    tf_batch *batch; /* single-row batch (copy of original row) */
    size_t    target_col; /* column index in the batch */
} pending_row;

typedef struct {
    char          *column;
    interp_method  method;
    double         last_val;
    int            has_last;
    interp_missing_policy missing;
    interp_type_policy on_type_error;
    /* Pending null rows (for backward/linear) */
    pending_row   *pending;
    size_t         n_pending;
    size_t         cap_pending;
} interpolate_state;

static int parse_method(const char *s, interp_method *out) {
    if (!s) { *out = INTERP_LINEAR; return TF_OK; }
    if (strcmp(s, "forward") == 0) { *out = INTERP_FORWARD; return TF_OK; }
    if (strcmp(s, "backward") == 0) { *out = INTERP_BACKWARD; return TF_OK; }
    if (strcmp(s, "linear") == 0) { *out = INTERP_LINEAR; return TF_OK; }
    return TF_ERROR;
}

static int interpolate_is_numeric_type(tf_type type) {
    return type == TF_TYPE_INT64 || type == TF_TYPE_FLOAT64;
}

static double interpolate_get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    return tf_batch_get_float64(b, r, ci);
}

static int interpolate_set_numeric(tf_batch *b, size_t r, size_t ci, double val) {
    if (b->col_types[ci] == TF_TYPE_INT64)
        return tf_batch_set_int64(b, r, ci, (int64_t)val);
    if (b->col_types[ci] == TF_TYPE_FLOAT64)
        return tf_batch_set_float64(b, r, ci, val);
    return TF_ERROR;
}

static void interpolate_set_col_error(const char *column, const char *suffix) {
    char msg[512];
    snprintf(msg, sizeof(msg), "interpolate: column '%s' %s", column ? column : "", suffix);
    tf_set_last_error(msg);
}

static int interpolate_parse_missing_policy(const cJSON *args, interp_missing_policy *out) {
    *out = INTERP_MISSING_ERROR;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "missing");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("interpolate: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "error") == 0) *out = INTERP_MISSING_ERROR;
    else if (strcmp(j->valuestring, "null") == 0) *out = INTERP_MISSING_NULL;
    else if (strcmp(j->valuestring, "ignore") == 0) *out = INTERP_MISSING_IGNORE;
    else {
        tf_set_last_error("interpolate: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    return TF_OK;
}

static int interpolate_parse_type_policy(const cJSON *args, interp_type_policy *out) {
    *out = INTERP_TYPE_FAIL;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "on_type_error");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("interpolate: on_type_error must be fail or null");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "fail") == 0) *out = INTERP_TYPE_FAIL;
    else if (strcmp(j->valuestring, "null") == 0) *out = INTERP_TYPE_NULL;
    else {
        tf_set_last_error("interpolate: on_type_error must be fail or null");
        return TF_ERROR;
    }
    return TF_OK;
}

static int interpolate_expose_row(tf_batch *ob, size_t row) {
    size_t next_rows = 0;
    if (tf_size_add(row, 1, &next_rows) != TF_OK) return TF_ERROR;
    ob->n_rows = next_rows;
    return TF_OK;
}

static int interpolate_passthrough(tf_batch *in, tf_batch **out) {
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
        if (interpolate_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    *out = ob;
    return TF_OK;
}

static int interpolate_null_output(interpolate_state *st, tf_batch *in, int ci, tf_batch **out) {
    size_t result_col = ci < 0 ? in->n_cols : (size_t)ci;
    tf_batch *ob = NULL;
    if (ci < 0) {
        const char *extra_names[1] = {st->column};
        tf_type extra_types[1] = {TF_TYPE_FLOAT64};
        ob = tf_batch_create(in->n_cols + 1, in->n_rows);
        if (!ob) return TF_ERROR;
        if (tf_batch_clone_with_extra_cols(ob, in, extra_names, extra_types, 1) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    } else {
        ob = tf_batch_create(in->n_cols, in->n_rows);
        if (!ob) return TF_ERROR;
        if (tf_batch_clone_schema(ob, in) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK ||
            tf_batch_set_null(ob, r, result_col) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (interpolate_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    *out = ob;
    return TF_OK;
}

static int add_pending(interpolate_state *st, tf_batch *row_batch, size_t target_col) {
    size_t next_pending = 0;
    if (tf_size_add(st->n_pending, 1, &next_pending) != TF_OK) return TF_ERROR;
    if (next_pending > st->cap_pending) {
        size_t newcap = 0;
        if (tf_size_grow_pow2(st->cap_pending, next_pending, 16, &newcap) != TF_OK) {
            return TF_ERROR;
        }
        pending_row *tmp = tf_reallocarray_checked(st->pending, newcap, sizeof(pending_row));
        if (!tmp) return TF_ERROR;
        st->pending = tmp;
        st->cap_pending = newcap;
    }
    st->pending[st->n_pending].batch = row_batch;
    st->pending[st->n_pending].target_col = target_col;
    st->n_pending = next_pending;
    return TF_OK;
}

static tf_batch *copy_single_row(const tf_batch *in, size_t row) {
    tf_batch *row_copy = tf_batch_create(in->n_cols, 1);
    if (!row_copy) return NULL;
    if (tf_batch_clone_schema(row_copy, in) != TF_OK ||
        tf_batch_copy_row(row_copy, 0, in, row) != TF_OK) {
        tf_batch_free(row_copy);
        return NULL;
    }
    if (interpolate_expose_row(row_copy, 0) != TF_OK) {
        tf_batch_free(row_copy);
        return NULL;
    }
    return row_copy;
}

/* Emit all pending rows into output batch, interpolating values */
static int flush_pending(interpolate_state *st, tf_batch *ob, size_t *out_row,
                         double end_val, size_t target_col) {
    if (st->n_pending == 0) return TF_OK;

    for (size_t i = 0; i < st->n_pending; i++) {
        tf_batch *pb = st->pending[i].batch;
        size_t r = *out_row;
        if (tf_batch_copy_row(ob, r, pb, 0) != TF_OK) return TF_ERROR;

        double interp_val;
        if (st->method == INTERP_BACKWARD) {
            interp_val = end_val;
        } else {
            /* Linear: interpolate between last_val and end_val */
            if (st->has_last) {
                double t = (double)(i + 1) / (double)(st->n_pending + 1);
                interp_val = st->last_val + t * (end_val - st->last_val);
            } else {
                interp_val = end_val;
            }
        }

        if (interpolate_set_numeric(ob, r, target_col, interp_val) != TF_OK) return TF_ERROR;
        if (interpolate_expose_row(ob, r) != TF_OK) return TF_ERROR;
        (*out_row)++;
    }

    for (size_t i = 0; i < st->n_pending; i++) {
        tf_batch_free(st->pending[i].batch);
        st->pending[i].batch = NULL;
    }
    st->n_pending = 0;
    return TF_OK;
}

static int interpolate_process(tf_step *self, tf_batch *in, tf_batch **out,
                               tf_side_channels *side) {
    (void)side;
    interpolate_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);
    int force_null = 0;
    if (ci < 0) {
        if (st->missing == INTERP_MISSING_ERROR) {
            interpolate_set_col_error(st->column, "not found");
            return TF_ERROR;
        }
        if (st->missing == INTERP_MISSING_IGNORE) return interpolate_passthrough(in, out);
        force_null = 1;
    } else if (!interpolate_is_numeric_type(in->col_types[ci])) {
        if (st->on_type_error == INTERP_TYPE_FAIL) {
            interpolate_set_col_error(st->column, "must be numeric");
            return TF_ERROR;
        }
        force_null = 1;
    }
    if (force_null) return interpolate_null_output(st, in, ci, out);

    size_t max_rows = 0;
    if (tf_size_add(st->n_pending, in->n_rows, &max_rows) != TF_OK) return TF_ERROR;
    tf_batch *ob = tf_batch_create(in->n_cols, max_rows);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_schema(ob, in) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    size_t out_row = 0;

    for (size_t r = 0; r < in->n_rows; r++) {
        int is_null = tf_batch_is_null(in, r, ci);

        if (is_null) {
            if (st->method == INTERP_FORWARD && st->has_last) {
                /* Forward fill: use last known value */
                if (tf_batch_copy_row(ob, out_row, in, r) != TF_OK ||
                    interpolate_set_numeric(ob, out_row, (size_t)ci, st->last_val) != TF_OK) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                if (interpolate_expose_row(ob, out_row) != TF_OK) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                out_row++;
            } else if (st->method == INTERP_FORWARD) {
                /* No previous value yet — pass null through */
                if (tf_batch_copy_row(ob, out_row, in, r) != TF_OK) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                if (interpolate_expose_row(ob, out_row) != TF_OK) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                out_row++;
            } else {
                /* Backward/linear: buffer this row */
                tf_batch *row_copy = copy_single_row(in, r);
                if (!row_copy || add_pending(st, row_copy, (size_t)ci) != TF_OK) {
                    tf_batch_free(row_copy);
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
            }
        } else {
            double val = interpolate_get_numeric(in, r, ci);

            /* Flush any pending rows */
            if (st->n_pending > 0) {
                size_t needed = 0;
                if (tf_size_add(out_row, st->n_pending, &needed) != TF_OK ||
                    tf_size_add(needed, 1, &needed) != TF_OK ||
                    tf_batch_ensure_capacity(ob, needed) != TF_OK ||
                    flush_pending(st, ob, &out_row, val, (size_t)ci) != TF_OK) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
            }

            /* Output current row */
            if (tf_batch_copy_row(ob, out_row, in, r) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (interpolate_expose_row(ob, out_row) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            out_row++;

            st->last_val = val;
            st->has_last = 1;
        }
    }

    if (ob->n_rows == 0) {
        tf_batch_free(ob);
        *out = NULL;
    } else {
        *out = ob;
    }
    return TF_OK;
}

static int interpolate_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    interpolate_state *st = self->state;
    *out = NULL;

    /* Emit any remaining pending rows as nulls (or forward-fill with last known) */
    if (st->n_pending > 0) {
        tf_batch *first = st->pending[0].batch;
        tf_batch *ob = tf_batch_create(first->n_cols, st->n_pending);
        if (!ob) return TF_ERROR;
        if (tf_batch_clone_schema(ob, first) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }

        for (size_t i = 0; i < st->n_pending; i++) {
            tf_batch *pb = st->pending[i].batch;
            if (tf_batch_copy_row(ob, i, pb, 0) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            /* For linear/backward at end of stream: use last known if available */
            if (st->has_last && interpolate_set_numeric(ob, i, st->pending[i].target_col, st->last_val) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (interpolate_expose_row(ob, i) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
        }
        for (size_t i = 0; i < st->n_pending; i++) {
            tf_batch_free(st->pending[i].batch);
            st->pending[i].batch = NULL;
        }
        st->n_pending = 0;
        *out = ob;
    }

    return TF_OK;
}

static void interpolate_destroy(tf_step *self) {
    interpolate_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_pending; i++)
            tf_batch_free(st->pending[i].batch);
        free(st->pending);
        free(st->column);
        free(st);
    }
    free(self);
}

tf_step *tf_interpolate_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j) || !col_j->valuestring[0]) {
        tf_set_last_error("interpolate: column is required");
        return NULL;
    }

    interpolate_state *st = calloc(1, sizeof(interpolate_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    if (!st->column) { free(st); return NULL; }

    cJSON *method_j = cJSON_GetObjectItemCaseSensitive(args, "method");
    if (parse_method(cJSON_IsString(method_j) ? method_j->valuestring : NULL, &st->method) != TF_OK) {
        tf_set_last_error("interpolate: method must be forward, backward, or linear");
        free(st->column);
        free(st);
        return NULL;
    }
    if (interpolate_parse_missing_policy(args, &st->missing) != TF_OK ||
        interpolate_parse_type_policy(args, &st->on_type_error) != TF_OK) {
        free(st->column);
        free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st->column); free(st); return NULL; }
    step->process = interpolate_process;
    step->flush = interpolate_flush;
    step->destroy = interpolate_destroy;
    step->state = st;
    return step;
}
