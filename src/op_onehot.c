/*
 * op_onehot.c — One-hot encoding of a categorical column.
 * Expands a single column into N binary (0/1) columns.
 *
 * Config: {"column": "city", "drop": false}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char  *value;     /* category value string */
    char  *col_name;  /* generated column name: "column_value" */
} onehot_category;

typedef struct {
    char             *column;
    int               drop;    /* drop original column */
    onehot_category  *cats;
    size_t            n_cats;
    size_t            cap;
} onehot_state;

static const char *get_string_value(const tf_batch *b, size_t r, int ci, char *buf, size_t bufsz) {
    if (tf_batch_is_null(b, r, ci)) return NULL;
    switch (b->col_types[ci]) {
        case TF_TYPE_STRING: return tf_batch_get_string(b, r, ci);
        case TF_TYPE_INT64:
            snprintf(buf, bufsz, "%lld", (long long)tf_batch_get_int64(b, r, ci));
            return buf;
        case TF_TYPE_FLOAT64:
            snprintf(buf, bufsz, "%.17g", tf_batch_get_float64(b, r, ci));
            return buf;
        case TF_TYPE_BOOL:
            return tf_batch_get_bool(b, r, ci) ? "true" : "false";
        default: return NULL;
    }
}

static int find_or_add_category(onehot_state *st, const char *val) {
    for (size_t i = 0; i < st->n_cats; i++) {
        if (strcmp(st->cats[i].value, val) == 0)
            return (int)i;
    }
    /* Add new category */
    if (st->n_cats >= st->cap) {
        size_t newcap = st->cap ? st->cap * 2 : 16;
        onehot_category *tmp = realloc(st->cats, newcap * sizeof(onehot_category));
        if (!tmp) return -1;
        st->cats = tmp;
        st->cap = newcap;
    }
    st->cats[st->n_cats].value = strdup(val);
    char namebuf[512];
    snprintf(namebuf, sizeof(namebuf), "%s_%s", st->column, val);
    st->cats[st->n_cats].col_name = strdup(namebuf);
    st->n_cats++;
    return (int)(st->n_cats - 1);
}

static int onehot_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    (void)side;
    onehot_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);

    /* First pass: discover any new categories in this batch */
    size_t cats_before = st->n_cats;
    if (ci >= 0) {
        char buf[64];
        for (size_t r = 0; r < in->n_rows; r++) {
            const char *val = get_string_value(in, r, ci, buf, sizeof(buf));
            if (val) find_or_add_category(st, val);
        }
    }
    (void)cats_before;

    /* Compute output column count */
    size_t out_cols = st->drop ? (in->n_cols - 1 + st->n_cats) : (in->n_cols + st->n_cats);

    tf_batch *ob = tf_batch_create(out_cols, in->n_rows);
    if (!ob) return TF_ERROR;

    /* Set schema: copy input cols (optionally skipping target), append onehot cols */
    size_t oc = 0;
    for (size_t c = 0; c < in->n_cols; c++) {
        if (st->drop && ci >= 0 && c == (size_t)ci) continue;
        tf_batch_set_schema(ob, oc, in->col_names[c], in->col_types[c]);
        oc++;
    }
    for (size_t i = 0; i < st->n_cats; i++) {
        tf_batch_set_schema(ob, oc + i, st->cats[i].col_name, TF_TYPE_INT64);
    }

    /* Fill rows */
    char buf[64];
    for (size_t r = 0; r < in->n_rows; r++) {
        /* Copy input columns */
        oc = 0;
        for (size_t c = 0; c < in->n_cols; c++) {
            if (st->drop && ci >= 0 && c == (size_t)ci) continue;
            if (tf_batch_is_null(in, r, c)) {
                tf_batch_set_null(ob, r, oc);
            } else {
                switch (in->col_types[c]) {
                    case TF_TYPE_STRING:
                        tf_batch_set_string(ob, r, oc, tf_batch_get_string(in, r, c)); break;
                    case TF_TYPE_INT64:
                        tf_batch_set_int64(ob, r, oc, tf_batch_get_int64(in, r, c)); break;
                    case TF_TYPE_FLOAT64:
                        tf_batch_set_float64(ob, r, oc, tf_batch_get_float64(in, r, c)); break;
                    case TF_TYPE_BOOL:
                        tf_batch_set_bool(ob, r, oc, tf_batch_get_bool(in, r, c)); break;
                    case TF_TYPE_DATE:
                        tf_batch_set_date(ob, r, oc, tf_batch_get_date(in, r, c)); break;
                    case TF_TYPE_TIMESTAMP:
                        tf_batch_set_timestamp(ob, r, oc, tf_batch_get_timestamp(in, r, c)); break;
                    default: tf_batch_set_null(ob, r, oc); break;
                }
            }
            oc++;
        }

        /* Set onehot columns */
        const char *val = (ci >= 0) ? get_string_value(in, r, ci, buf, sizeof(buf)) : NULL;
        int match = -1;
        if (val) {
            for (size_t i = 0; i < st->n_cats; i++) {
                if (strcmp(st->cats[i].value, val) == 0) { match = (int)i; break; }
            }
        }
        for (size_t i = 0; i < st->n_cats; i++) {
            tf_batch_set_int64(ob, r, oc + i, (int)i == match ? 1 : 0);
        }

        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int onehot_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void onehot_destroy(tf_step *self) {
    onehot_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_cats; i++) {
            free(st->cats[i].value);
            free(st->cats[i].col_name);
        }
        free(st->cats);
        free(st->column);
        free(st);
    }
    free(self);
}

tf_step *tf_onehot_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j)) return NULL;

    onehot_state *st = calloc(1, sizeof(onehot_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);

    cJSON *drop_j = cJSON_GetObjectItemCaseSensitive(args, "drop");
    st->drop = cJSON_IsBool(drop_j) && cJSON_IsTrue(drop_j) ? 1 : 0;

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->column); free(st); return NULL; }
    step->process = onehot_process;
    step->flush = onehot_flush;
    step->destroy = onehot_destroy;
    step->state = st;
    return step;
}
