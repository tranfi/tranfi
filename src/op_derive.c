/*
 * op_derive.c — Add computed columns using arithmetic expressions.
 *
 * Config: {"columns": [{"name": "total", "expr": "col(price)*col(qty)"}]}
 * For each row, evaluates each expression and appends the result as a new column.
 */

#include "internal.h"
#include "cJSON.h"
#include "date_utils.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>
#include <math.h>

typedef struct {
    char    *name;
    tf_expr *expr;
} derive_col;

typedef struct {
    derive_col *cols;
    size_t      n_cols;
    int         types_resolved;  /* have we determined output types? */
    tf_type    *col_types;       /* resolved type per derived column */
} derive_state;

/* Evaluate first row to determine derived column types. */
static int resolve_types(derive_state *st, const tf_batch *in) {
    st->col_types = calloc(st->n_cols ? st->n_cols : 1, sizeof(tf_type));
    if (!st->col_types) return TF_ERROR;

    for (size_t d = 0; d < st->n_cols; d++) {
        if (in->n_rows == 0) {
            st->col_types[d] = TF_TYPE_FLOAT64;
            continue;
        }
        tf_eval_result val;
        if (tf_expr_eval_val(st->cols[d].expr, in, 0, &val) != TF_OK) {
            st->col_types[d] = TF_TYPE_FLOAT64;
            continue;
        }
        switch (val.type) {
            case TF_TYPE_INT64:   st->col_types[d] = TF_TYPE_INT64; break;
            case TF_TYPE_FLOAT64: st->col_types[d] = TF_TYPE_FLOAT64; break;
            case TF_TYPE_STRING:  st->col_types[d] = TF_TYPE_STRING; break;
            case TF_TYPE_BOOL:    st->col_types[d] = TF_TYPE_BOOL; break;
            case TF_TYPE_DATE:    st->col_types[d] = TF_TYPE_DATE; break;
            case TF_TYPE_TIMESTAMP: st->col_types[d] = TF_TYPE_TIMESTAMP; break;
            default:              st->col_types[d] = TF_TYPE_FLOAT64; break;
        }
    }
    st->types_resolved = 1;
    return TF_OK;
}

static int derive_double_to_i64(double v, int64_t *out) {
    if (!isfinite(v) || v < (double)INT64_MIN || v > (double)INT64_MAX) return TF_ERROR;
    *out = (int64_t)v;
    return TF_OK;
}

static int set_derived_value(tf_batch *ob, size_t row, size_t col,
                             tf_type col_type, const tf_eval_result *val) {
    if (!val || val->type == TF_TYPE_NULL) return tf_batch_set_null(ob, row, col);

    switch (col_type) {
        case TF_TYPE_INT64:
            if (val->type == TF_TYPE_INT64) return tf_batch_set_int64(ob, row, col, val->i);
            if (val->type == TF_TYPE_FLOAT64) {
                int64_t iv = 0;
                if (derive_double_to_i64(val->f, &iv) != TF_OK) return tf_batch_set_null(ob, row, col);
                return tf_batch_set_int64(ob, row, col, iv);
            }
            return tf_batch_set_null(ob, row, col);
        case TF_TYPE_FLOAT64:
            if (val->type == TF_TYPE_FLOAT64) return tf_batch_set_float64(ob, row, col, val->f);
            if (val->type == TF_TYPE_INT64) return tf_batch_set_float64(ob, row, col, (double)val->i);
            return tf_batch_set_null(ob, row, col);
        case TF_TYPE_STRING:
            if (val->type == TF_TYPE_STRING) {
                if (!val->s) return tf_batch_set_null(ob, row, col);
                return tf_batch_set_string(ob, row, col, val->s);
            }
            return tf_batch_set_null(ob, row, col);
        case TF_TYPE_BOOL:
            if (val->type == TF_TYPE_BOOL) return tf_batch_set_bool(ob, row, col, val->b);
            return tf_batch_set_null(ob, row, col);
        case TF_TYPE_DATE:
            if (val->type == TF_TYPE_DATE) return tf_batch_set_date(ob, row, col, val->date);
            return tf_batch_set_null(ob, row, col);
        case TF_TYPE_TIMESTAMP:
            if (val->type == TF_TYPE_TIMESTAMP) return tf_batch_set_timestamp(ob, row, col, val->i);
            return tf_batch_set_null(ob, row, col);
        default:
            return tf_batch_set_null(ob, row, col);
    }
}

static int derive_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    (void)side;
    derive_state *st = self->state;
    *out = NULL;

    if (!st->types_resolved && resolve_types(st, in) != TF_OK) return TF_ERROR;

    size_t out_n_cols = 0;
    if (tf_size_add(in->n_cols, st->n_cols, &out_n_cols) != TF_OK) return TF_ERROR;

    tf_batch *ob = tf_batch_create(out_n_cols, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) return TF_ERROR;

    const char **extra_names = calloc(st->n_cols ? st->n_cols : 1, sizeof(char *));
    if (!extra_names) {
        tf_batch_free(ob);
        return TF_ERROR;
    }
    for (size_t d = 0; d < st->n_cols; d++) extra_names[d] = st->cols[d].name;
    if (tf_batch_clone_with_extra_cols(ob, in, extra_names, st->col_types, st->n_cols) != TF_OK) {
        free(extra_names);
        tf_batch_free(ob);
        return TF_ERROR;
    }
    free(extra_names);

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) goto fail;

        for (size_t d = 0; d < st->n_cols; d++) {
            size_t col_idx = in->n_cols + d;
            tf_eval_result val;
            if (tf_expr_eval_val(st->cols[d].expr, in, r, &val) != TF_OK) {
                if (tf_batch_set_null(ob, r, col_idx) != TF_OK) goto fail;
                continue;
            }
            if (set_derived_value(ob, r, col_idx, st->col_types[d], &val) != TF_OK) goto fail;
        }
        ob->n_rows = r + 1;
    }

    if (ob->n_rows > 0) {
        *out = ob;
    } else {
        tf_batch_free(ob);
    }
    return TF_OK;

fail:
    tf_batch_free(ob);
    return TF_ERROR;
}

static int derive_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static void derive_destroy(tf_step *self) {
    derive_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_cols; i++) {
            free(st->cols[i].name);
            tf_expr_free(st->cols[i].expr);
        }
        free(st->cols);
        free(st->col_types);
        free(st);
    }
    free(self);
}

tf_step *tf_derive_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *columns = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!columns || !cJSON_IsArray(columns)) return NULL;

    int n = cJSON_GetArraySize(columns);
    if (n <= 0) return NULL;

    derive_state *st = calloc(1, sizeof(derive_state));
    if (!st) return NULL;
    st->cols = calloc(n, sizeof(derive_col));
    if (!st->cols) { free(st); return NULL; }
    st->n_cols = n;

    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(columns, i);
        cJSON *name_j = cJSON_GetObjectItemCaseSensitive(item, "name");
        cJSON *expr_j = cJSON_GetObjectItemCaseSensitive(item, "expr");
        if (!cJSON_IsString(name_j) || !cJSON_IsString(expr_j)) goto fail;

        st->cols[i].name = strdup(name_j->valuestring);
        st->cols[i].expr = tf_expr_parse(expr_j->valuestring);
        if (!st->cols[i].name || !st->cols[i].expr) goto fail;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) goto fail;
    step->process = derive_process;
    step->flush = derive_flush;
    step->destroy = derive_destroy;
    step->state = st;
    return step;

fail:
    for (int i = 0; i < n; i++) {
        free(st->cols[i].name);
        tf_expr_free(st->cols[i].expr);
    }
    free(st->cols);
    free(st);
    return NULL;
}
