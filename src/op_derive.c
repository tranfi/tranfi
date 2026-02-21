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

/* Evaluate first row to determine derived column types */
static void resolve_types(derive_state *st, const tf_batch *in) {
    st->col_types = calloc(st->n_cols, sizeof(tf_type));
    if (!st->col_types) return;

    for (size_t d = 0; d < st->n_cols; d++) {
        if (in->n_rows == 0) {
            /* No rows to sample; default to FLOAT64 for arithmetic */
            st->col_types[d] = TF_TYPE_FLOAT64;
            continue;
        }
        tf_eval_result val;
        tf_expr_eval_val(st->cols[d].expr, in, 0, &val);
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
}

static void set_derived_value(tf_batch *ob, size_t row, size_t col,
                              tf_type col_type, const tf_eval_result *val) {
    if (val->type == TF_TYPE_NULL) {
        tf_batch_set_null(ob, row, col);
        return;
    }

    /* Convert value to the column's type */
    switch (col_type) {
        case TF_TYPE_INT64:
            if (val->type == TF_TYPE_INT64)
                tf_batch_set_int64(ob, row, col, val->i);
            else if (val->type == TF_TYPE_FLOAT64)
                tf_batch_set_int64(ob, row, col, (int64_t)val->f);
            else
                tf_batch_set_null(ob, row, col);
            break;
        case TF_TYPE_FLOAT64:
            if (val->type == TF_TYPE_FLOAT64)
                tf_batch_set_float64(ob, row, col, val->f);
            else if (val->type == TF_TYPE_INT64)
                tf_batch_set_float64(ob, row, col, (double)val->i);
            else
                tf_batch_set_null(ob, row, col);
            break;
        case TF_TYPE_STRING:
            if (val->type == TF_TYPE_STRING)
                tf_batch_set_string(ob, row, col, val->s);
            else
                tf_batch_set_null(ob, row, col);
            break;
        case TF_TYPE_BOOL:
            if (val->type == TF_TYPE_BOOL)
                tf_batch_set_bool(ob, row, col, val->b);
            else
                tf_batch_set_null(ob, row, col);
            break;
        case TF_TYPE_DATE:
            if (val->type == TF_TYPE_DATE)
                tf_batch_set_date(ob, row, col, val->date);
            else
                tf_batch_set_null(ob, row, col);
            break;
        case TF_TYPE_TIMESTAMP:
            if (val->type == TF_TYPE_TIMESTAMP)
                tf_batch_set_timestamp(ob, row, col, val->i);
            else
                tf_batch_set_null(ob, row, col);
            break;
        default:
            tf_batch_set_null(ob, row, col);
            break;
    }
}

static int derive_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    (void)side;
    derive_state *st = self->state;
    *out = NULL;

    /* Resolve types on first batch */
    if (!st->types_resolved) {
        resolve_types(st, in);
    }

    size_t out_n_cols = in->n_cols + st->n_cols;
    tf_batch *ob = tf_batch_create(out_n_cols, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) return TF_ERROR;

    /* Copy input schema */
    for (size_t c = 0; c < in->n_cols; c++) {
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    }
    /* Set derived column schemas with resolved types */
    for (size_t d = 0; d < st->n_cols; d++) {
        tf_batch_set_schema(ob, in->n_cols + d, st->cols[d].name, st->col_types[d]);
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_ensure_capacity(ob, r + 1);

        /* Copy input columns */
        for (size_t c = 0; c < in->n_cols; c++) {
            if (tf_batch_is_null(in, r, c)) {
                tf_batch_set_null(ob, r, c);
                continue;
            }
            switch (in->col_types[c]) {
                case TF_TYPE_BOOL:
                    tf_batch_set_bool(ob, r, c, tf_batch_get_bool(in, r, c));
                    break;
                case TF_TYPE_INT64:
                    tf_batch_set_int64(ob, r, c, tf_batch_get_int64(in, r, c));
                    break;
                case TF_TYPE_FLOAT64:
                    tf_batch_set_float64(ob, r, c, tf_batch_get_float64(in, r, c));
                    break;
                case TF_TYPE_STRING:
                    tf_batch_set_string(ob, r, c, tf_batch_get_string(in, r, c));
                    break;
                case TF_TYPE_DATE:
                    tf_batch_set_date(ob, r, c, tf_batch_get_date(in, r, c));
                    break;
                case TF_TYPE_TIMESTAMP:
                    tf_batch_set_timestamp(ob, r, c, tf_batch_get_timestamp(in, r, c));
                    break;
                default:
                    tf_batch_set_null(ob, r, c);
                    break;
            }
        }

        /* Evaluate derived columns */
        for (size_t d = 0; d < st->n_cols; d++) {
            size_t col_idx = in->n_cols + d;
            tf_eval_result val;
            if (tf_expr_eval_val(st->cols[d].expr, in, r, &val) != TF_OK) {
                tf_batch_set_null(ob, r, col_idx);
                continue;
            }
            set_derived_value(ob, r, col_idx, st->col_types[d], &val);
        }
        ob->n_rows = r + 1;
    }

    if (ob->n_rows > 0) {
        *out = ob;
    } else {
        tf_batch_free(ob);
    }
    return TF_OK;
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

    tf_step *step = malloc(sizeof(tf_step));
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
