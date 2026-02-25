/*
 * op_normalize.c — Min-max or z-score normalization.
 * Aggregate op: buffers all rows, computes stats, then emits normalized.
 *
 * Config: {"columns": ["price", "score"], "method": "minmax"}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

typedef enum {
    NORM_MINMAX,
    NORM_ZSCORE
} norm_method;

typedef struct {
    int     col_idx;
    /* Welford's online stats */
    size_t  count;
    double  mean;
    double  m2;
    double  min_val;
    double  max_val;
} col_stats;

/* Row buffer entry */
typedef struct {
    tf_batch *batch; /* single-row batch */
} buf_row;

typedef struct {
    char       **columns;
    size_t       n_columns;
    norm_method  method;
    col_stats   *stats;
    buf_row     *rows;
    size_t       n_rows;
    size_t       cap_rows;
    int          has_schema;
    size_t       schema_n_cols;
    char       **schema_names;
    tf_type     *schema_types;
} normalize_state;

static norm_method parse_method(const char *s) {
    if (s && strcmp(s, "zscore") == 0) return NORM_ZSCORE;
    return NORM_MINMAX;
}

static double get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    if (b->col_types[ci] == TF_TYPE_FLOAT64) return tf_batch_get_float64(b, r, ci);
    return 0;
}

static void add_row(normalize_state *st, tf_batch *b, size_t r) {
    if (st->n_rows >= st->cap_rows) {
        size_t newcap = st->cap_rows ? st->cap_rows * 2 : 256;
        buf_row *tmp = realloc(st->rows, newcap * sizeof(buf_row));
        if (!tmp) return;
        st->rows = tmp;
        st->cap_rows = newcap;
    }
    /* Copy single row into its own batch */
    tf_batch *rb = tf_batch_create(b->n_cols, 1);
    if (!rb) return;
    for (size_t c = 0; c < b->n_cols; c++)
        tf_batch_set_schema(rb, c, b->col_names[c], b->col_types[c]);
    tf_batch_copy_row(rb, 0, b, r);
    rb->n_rows = 1;
    st->rows[st->n_rows].batch = rb;
    st->n_rows++;
}

static int normalize_process(tf_step *self, tf_batch *in, tf_batch **out,
                             tf_side_channels *side) {
    (void)side;
    normalize_state *st = self->state;
    *out = NULL;

    /* Save schema from first batch */
    if (!st->has_schema) {
        st->schema_n_cols = in->n_cols;
        st->schema_names = malloc(in->n_cols * sizeof(char *));
        st->schema_types = malloc(in->n_cols * sizeof(tf_type));
        for (size_t c = 0; c < in->n_cols; c++) {
            st->schema_names[c] = strdup(in->col_names[c]);
            st->schema_types[c] = in->col_types[c];
        }
        st->has_schema = 1;

        /* Resolve column indices */
        for (size_t i = 0; i < st->n_columns; i++) {
            st->stats[i].col_idx = tf_batch_col_index(in, st->columns[i]);
        }
    }

    /* Buffer rows and update stats */
    for (size_t r = 0; r < in->n_rows; r++) {
        add_row(st, in, r);

        for (size_t i = 0; i < st->n_columns; i++) {
            int ci = st->stats[i].col_idx;
            if (ci < 0 || tf_batch_is_null(in, r, ci)) continue;

            double val = get_numeric(in, r, ci);
            col_stats *cs = &st->stats[i];
            cs->count++;
            double delta = val - cs->mean;
            cs->mean += delta / (double)cs->count;
            double delta2 = val - cs->mean;
            cs->m2 += delta * delta2;

            if (cs->count == 1 || val < cs->min_val) cs->min_val = val;
            if (cs->count == 1 || val > cs->max_val) cs->max_val = val;
        }
    }

    return TF_OK;
}

static int normalize_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    normalize_state *st = self->state;
    *out = NULL;

    if (st->n_rows == 0) return TF_OK;

    tf_batch *ob = tf_batch_create(st->schema_n_cols, st->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < st->schema_n_cols; c++) {
        /* Normalized columns become FLOAT64 */
        tf_type type = st->schema_types[c];
        int is_norm_col = 0;
        for (size_t i = 0; i < st->n_columns; i++) {
            if (st->stats[i].col_idx == (int)c) { is_norm_col = 1; break; }
        }
        tf_batch_set_schema(ob, c, st->schema_names[c],
                            is_norm_col ? TF_TYPE_FLOAT64 : type);
    }

    for (size_t r = 0; r < st->n_rows; r++) {
        tf_batch *rb = st->rows[r].batch;
        tf_batch_copy_row(ob, r, rb, 0);

        /* Normalize target columns */
        for (size_t i = 0; i < st->n_columns; i++) {
            int ci = st->stats[i].col_idx;
            if (ci < 0 || tf_batch_is_null(rb, 0, ci)) continue;

            double val = get_numeric(rb, 0, ci);
            col_stats *cs = &st->stats[i];
            double norm;

            if (st->method == NORM_MINMAX) {
                double range = cs->max_val - cs->min_val;
                norm = (range > 0) ? (val - cs->min_val) / range : 0;
            } else {
                double std = (cs->count > 1) ? sqrt(cs->m2 / (double)(cs->count - 1)) : 1;
                norm = (std > 0) ? (val - cs->mean) / std : 0;
            }
            tf_batch_set_float64(ob, r, ci, norm);
        }
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static void normalize_destroy(tf_step *self) {
    normalize_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_columns; i++)
            free(st->columns[i]);
        free(st->columns);
        free(st->stats);
        for (size_t i = 0; i < st->n_rows; i++)
            tf_batch_free(st->rows[i].batch);
        free(st->rows);
        if (st->schema_names) {
            for (size_t c = 0; c < st->schema_n_cols; c++)
                free(st->schema_names[c]);
            free(st->schema_names);
        }
        free(st->schema_types);
        free(st);
    }
    free(self);
}

tf_step *tf_normalize_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *cols_j = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!cols_j || !cJSON_IsArray(cols_j)) return NULL;

    int n = cJSON_GetArraySize(cols_j);
    if (n == 0) return NULL;

    normalize_state *st = calloc(1, sizeof(normalize_state));
    if (!st) return NULL;

    st->n_columns = n;
    st->columns = malloc(n * sizeof(char *));
    st->stats = calloc(n, sizeof(col_stats));
    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(cols_j, i);
        st->columns[i] = strdup(cJSON_IsString(item) ? item->valuestring : "");
        st->stats[i].col_idx = -1;
    }

    cJSON *method_j = cJSON_GetObjectItemCaseSensitive(args, "method");
    st->method = parse_method(cJSON_IsString(method_j) ? method_j->valuestring : NULL);

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { normalize_destroy(&(tf_step){.state = st}); return NULL; }
    step->process = normalize_process;
    step->flush = normalize_flush;
    step->destroy = normalize_destroy;
    step->state = st;
    return step;
}
