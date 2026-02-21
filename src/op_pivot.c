/*
 * op_pivot.c — Pivot (long to wide). Full-load: buffers all data.
 *
 * Config: {"name_column": "metric", "value_column": "value", "agg": "first"}
 * Supported aggs: first, sum, count, avg, min, max.
 *
 * Pass-through columns = all columns except name_column and value_column.
 * Output: pass-through columns + one column per unique value of name_column.
 */

#include "internal.h"
#include "date_utils.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <float.h>

typedef enum {
    PIVOT_FIRST, PIVOT_SUM, PIVOT_COUNT, PIVOT_AVG, PIVOT_MIN, PIVOT_MAX,
} pivot_agg;

typedef struct {
    double *sums;
    double *mins;
    double *maxs;
    size_t *counts;
    int    *has_first;
    double *firsts;
} pivot_accum;

typedef struct {
    char  **keys;
    size_t *pass_rows;     /* first row index for each group (for pass-through values) */
    pivot_accum *accums;
    size_t  count;
    size_t  cap;
} pivot_map;

typedef struct {
    char      *name_column;
    char      *value_column;
    pivot_agg  agg;

    tf_batch  *buf;
    int        has_schema;

    char     **unique_names;
    size_t     n_names;
    size_t     names_cap;
} pivot_state;

static pivot_agg parse_pivot_agg(const char *s) {
    if (!s) return PIVOT_FIRST;
    if (strcmp(s, "first") == 0) return PIVOT_FIRST;
    if (strcmp(s, "sum") == 0) return PIVOT_SUM;
    if (strcmp(s, "count") == 0) return PIVOT_COUNT;
    if (strcmp(s, "avg") == 0) return PIVOT_AVG;
    if (strcmp(s, "min") == 0) return PIVOT_MIN;
    if (strcmp(s, "max") == 0) return PIVOT_MAX;
    return PIVOT_FIRST;
}

static int find_unique_name(pivot_state *st, const char *name) {
    for (size_t i = 0; i < st->n_names; i++) {
        if (strcmp(st->unique_names[i], name) == 0) return (int)i;
    }
    return -1;
}

static int add_unique_name(pivot_state *st, const char *name) {
    int idx = find_unique_name(st, name);
    if (idx >= 0) return idx;
    if (st->n_names >= st->names_cap) {
        size_t new_cap = st->names_cap ? st->names_cap * 2 : 32;
        st->unique_names = realloc(st->unique_names, new_cap * sizeof(char *));
        if (!st->unique_names) return -1;
        st->names_cap = new_cap;
    }
    st->unique_names[st->n_names] = strdup(name);
    return (int)st->n_names++;
}

/* Build group key from pass-through columns */
static char *build_pivot_key(const tf_batch *b, size_t row,
                             int *pt_cols, size_t n_pt) {
    size_t buf_cap = 256;
    char *buf = malloc(buf_cap);
    if (!buf) return NULL;
    size_t buf_len = 0;
    for (size_t k = 0; k < n_pt; k++) {
        int c = pt_cols[k];
        if (k > 0) buf[buf_len++] = '\x01';
        char val_buf[64];
        const char *val = "";
        size_t val_len = 0;
        if (tf_batch_is_null(b, row, c)) {
            val = "\\N"; val_len = 2;
        } else {
            switch (b->col_types[c]) {
                case TF_TYPE_STRING: val = tf_batch_get_string(b, row, c); val_len = strlen(val); break;
                case TF_TYPE_INT64:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%lld", (long long)tf_batch_get_int64(b, row, c));
                    val = val_buf; break;
                case TF_TYPE_FLOAT64:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%.17g", tf_batch_get_float64(b, row, c));
                    val = val_buf; break;
                case TF_TYPE_BOOL:
                    val = tf_batch_get_bool(b, row, c) ? "T" : "F"; val_len = 1; break;
                case TF_TYPE_DATE:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%d", (int)tf_batch_get_date(b, row, c));
                    val = val_buf; break;
                case TF_TYPE_TIMESTAMP:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%lld", (long long)tf_batch_get_timestamp(b, row, c));
                    val = val_buf; break;
                default: val = "\\N"; val_len = 2; break;
            }
        }
        while (buf_len + val_len + 2 >= buf_cap) { buf_cap *= 2; buf = realloc(buf, buf_cap); }
        memcpy(buf + buf_len, val, val_len);
        buf_len += val_len;
    }
    buf[buf_len] = '\0';
    return buf;
}

static int find_or_add_pivot_group(pivot_map *map, const char *key,
                                    size_t n_names, size_t src_row) {
    for (size_t i = 0; i < map->count; i++) {
        if (strcmp(map->keys[i], key) == 0) return (int)i;
    }
    if (map->count >= map->cap) {
        size_t new_cap = map->cap ? map->cap * 2 : 64;
        map->keys = realloc(map->keys, new_cap * sizeof(char *));
        map->pass_rows = realloc(map->pass_rows, new_cap * sizeof(size_t));
        map->accums = realloc(map->accums, new_cap * sizeof(pivot_accum));
        map->cap = new_cap;
    }
    size_t idx = map->count++;
    map->keys[idx] = strdup(key);
    map->pass_rows[idx] = src_row;
    pivot_accum *a = &map->accums[idx];
    a->sums = calloc(n_names, sizeof(double));
    a->mins = malloc(n_names * sizeof(double));
    a->maxs = malloc(n_names * sizeof(double));
    a->counts = calloc(n_names, sizeof(size_t));
    a->has_first = calloc(n_names, sizeof(int));
    a->firsts = calloc(n_names, sizeof(double));
    for (size_t i = 0; i < n_names; i++) { a->mins[i] = DBL_MAX; a->maxs[i] = -DBL_MAX; }
    return (int)idx;
}

static double get_numeric_value(const tf_batch *b, size_t row, int col) {
    if (tf_batch_is_null(b, row, col)) return 0.0;
    switch (b->col_types[col]) {
        case TF_TYPE_INT64:     return (double)tf_batch_get_int64(b, row, col);
        case TF_TYPE_FLOAT64:   return tf_batch_get_float64(b, row, col);
        case TF_TYPE_DATE:      return (double)tf_batch_get_date(b, row, col);
        case TF_TYPE_TIMESTAMP: return (double)tf_batch_get_timestamp(b, row, col);
        case TF_TYPE_BOOL:      return tf_batch_get_bool(b, row, col) ? 1.0 : 0.0;
        default: return 0.0;
    }
}

/* Get name column value as string */
static const char *get_name_str(const tf_batch *b, size_t row, int col, char *buf, size_t buf_sz) {
    if (tf_batch_is_null(b, row, col)) return NULL;
    switch (b->col_types[col]) {
        case TF_TYPE_STRING: return tf_batch_get_string(b, row, col);
        case TF_TYPE_INT64: snprintf(buf, buf_sz, "%lld", (long long)tf_batch_get_int64(b, row, col)); return buf;
        case TF_TYPE_FLOAT64: snprintf(buf, buf_sz, "%g", tf_batch_get_float64(b, row, col)); return buf;
        case TF_TYPE_BOOL: return tf_batch_get_bool(b, row, col) ? "true" : "false";
        case TF_TYPE_DATE: tf_date_format(tf_batch_get_date(b, row, col), buf, buf_sz); return buf;
        case TF_TYPE_TIMESTAMP: tf_timestamp_format(tf_batch_get_timestamp(b, row, col), buf, buf_sz); return buf;
        default: return NULL;
    }
}

static int pivot_process(tf_step *self, tf_batch *in, tf_batch **out,
                         tf_side_channels *side) {
    (void)side;
    pivot_state *st = self->state;
    *out = NULL;

    if (!st->has_schema) {
        st->buf = tf_batch_create(in->n_cols, in->n_rows > 0 ? in->n_rows : 16);
        if (!st->buf) return TF_ERROR;
        for (size_t c = 0; c < in->n_cols; c++)
            tf_batch_set_schema(st->buf, c, in->col_names[c], in->col_types[c]);
        st->has_schema = 1;
    }

    /* Track unique name values and buffer rows */
    int name_ci = tf_batch_col_index(in, st->name_column);
    for (size_t r = 0; r < in->n_rows; r++) {
        size_t dst_row = st->buf->n_rows;
        if (tf_batch_copy_row(st->buf, dst_row, in, r) != TF_OK) return TF_ERROR;
        st->buf->n_rows = dst_row + 1;

        if (name_ci >= 0 && !tf_batch_is_null(in, r, name_ci)) {
            char nbuf[64];
            const char *name = get_name_str(in, r, name_ci, nbuf, sizeof(nbuf));
            if (name) add_unique_name(st, name);
        }
    }

    return TF_OK;
}

static int pivot_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    pivot_state *st = self->state;
    *out = NULL;

    if (!st->buf || st->buf->n_rows == 0 || st->n_names == 0) return TF_OK;

    tf_batch *buf = st->buf;
    int name_ci = tf_batch_col_index(buf, st->name_column);
    int val_ci = tf_batch_col_index(buf, st->value_column);
    if (name_ci < 0 || val_ci < 0) return TF_OK;

    /* Determine pass-through columns */
    size_t n_pt = 0;
    int *pt_cols = malloc(buf->n_cols * sizeof(int));
    if (!pt_cols) return TF_ERROR;
    for (size_t c = 0; c < buf->n_cols; c++) {
        if ((int)c != name_ci && (int)c != val_ci)
            pt_cols[n_pt++] = (int)c;
    }

    /* Build group map */
    pivot_map map = {0};
    for (size_t r = 0; r < buf->n_rows; r++) {
        char *key = build_pivot_key(buf, r, pt_cols, n_pt);
        if (!key) { free(pt_cols); return TF_ERROR; }
        int gi = find_or_add_pivot_group(&map, key, st->n_names, r);
        free(key);
        if (gi < 0) { free(pt_cols); return TF_ERROR; }

        /* Get name index */
        char nbuf[64];
        const char *name = get_name_str(buf, r, name_ci, nbuf, sizeof(nbuf));
        if (!name) continue;
        int ni = find_unique_name(st, name);
        if (ni < 0) continue;

        double v = get_numeric_value(buf, r, val_ci);
        pivot_accum *a = &map.accums[gi];
        a->sums[ni] += v;
        if (v < a->mins[ni]) a->mins[ni] = v;
        if (v > a->maxs[ni]) a->maxs[ni] = v;
        a->counts[ni]++;
        if (!a->has_first[ni]) { a->firsts[ni] = v; a->has_first[ni] = 1; }
    }

    /* Build output batch */
    size_t n_out_cols = n_pt + st->n_names;
    tf_batch *ob = tf_batch_create(n_out_cols, map.count);
    if (!ob) { free(pt_cols); return TF_ERROR; }

    /* Pass-through column schemas */
    for (size_t k = 0; k < n_pt; k++)
        tf_batch_set_schema(ob, k, buf->col_names[pt_cols[k]], buf->col_types[pt_cols[k]]);

    /* Pivot column schemas */
    tf_type pivot_type = TF_TYPE_FLOAT64;
    if (st->agg == PIVOT_COUNT) pivot_type = TF_TYPE_INT64;
    for (size_t k = 0; k < st->n_names; k++)
        tf_batch_set_schema(ob, n_pt + k, st->unique_names[k], pivot_type);

    /* Fill output */
    for (size_t g = 0; g < map.count; g++) {
        tf_batch_ensure_capacity(ob, g + 1);

        /* Copy pass-through values from the first row of this group */
        size_t src_row = map.pass_rows[g];
        for (size_t k = 0; k < n_pt; k++) {
            int sc = pt_cols[k];
            if (tf_batch_is_null(buf, src_row, sc)) {
                tf_batch_set_null(ob, g, k);
            } else {
                switch (buf->col_types[sc]) {
                    case TF_TYPE_BOOL: tf_batch_set_bool(ob, g, k, tf_batch_get_bool(buf, src_row, sc)); break;
                    case TF_TYPE_INT64: tf_batch_set_int64(ob, g, k, tf_batch_get_int64(buf, src_row, sc)); break;
                    case TF_TYPE_FLOAT64: tf_batch_set_float64(ob, g, k, tf_batch_get_float64(buf, src_row, sc)); break;
                    case TF_TYPE_STRING: tf_batch_set_string(ob, g, k, tf_batch_get_string(buf, src_row, sc)); break;
                    case TF_TYPE_DATE: tf_batch_set_date(ob, g, k, tf_batch_get_date(buf, src_row, sc)); break;
                    case TF_TYPE_TIMESTAMP: tf_batch_set_timestamp(ob, g, k, tf_batch_get_timestamp(buf, src_row, sc)); break;
                    default: tf_batch_set_null(ob, g, k); break;
                }
            }
        }

        /* Set pivot values */
        pivot_accum *a = &map.accums[g];
        for (size_t k = 0; k < st->n_names; k++) {
            size_t oc = n_pt + k;
            if (a->counts[k] == 0) {
                tf_batch_set_null(ob, g, oc);
                continue;
            }
            double v = 0;
            switch (st->agg) {
                case PIVOT_FIRST: v = a->firsts[k]; break;
                case PIVOT_SUM:   v = a->sums[k]; break;
                case PIVOT_COUNT: tf_batch_set_int64(ob, g, oc, (int64_t)a->counts[k]); goto next_name;
                case PIVOT_AVG:   v = a->sums[k] / (double)a->counts[k]; break;
                case PIVOT_MIN:   v = a->mins[k]; break;
                case PIVOT_MAX:   v = a->maxs[k]; break;
            }
            tf_batch_set_float64(ob, g, oc, v);
            next_name:;
        }
        ob->n_rows = g + 1;
    }

    /* Cleanup map */
    for (size_t i = 0; i < map.count; i++) {
        free(map.keys[i]);
        free(map.accums[i].sums);
        free(map.accums[i].mins);
        free(map.accums[i].maxs);
        free(map.accums[i].counts);
        free(map.accums[i].has_first);
        free(map.accums[i].firsts);
    }
    free(map.keys);
    free(map.pass_rows);
    free(map.accums);
    free(pt_cols);

    *out = ob;
    return TF_OK;
}

static void pivot_destroy(tf_step *self) {
    pivot_state *st = self->state;
    if (st) {
        free(st->name_column);
        free(st->value_column);
        if (st->buf) tf_batch_free(st->buf);
        for (size_t i = 0; i < st->n_names; i++) free(st->unique_names[i]);
        free(st->unique_names);
        free(st);
    }
    free(self);
}

tf_step *tf_pivot_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *name_j = cJSON_GetObjectItemCaseSensitive(args, "name_column");
    cJSON *val_j = cJSON_GetObjectItemCaseSensitive(args, "value_column");
    if (!cJSON_IsString(name_j) || !cJSON_IsString(val_j)) return NULL;

    pivot_state *st = calloc(1, sizeof(pivot_state));
    if (!st) return NULL;
    st->name_column = strdup(name_j->valuestring);
    st->value_column = strdup(val_j->valuestring);

    cJSON *agg_j = cJSON_GetObjectItemCaseSensitive(args, "agg");
    st->agg = cJSON_IsString(agg_j) ? parse_pivot_agg(agg_j->valuestring) : PIVOT_FIRST;

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { pivot_destroy(&(tf_step){.state = st}); return NULL; }
    step->process = pivot_process;
    step->flush = pivot_flush;
    step->destroy = pivot_destroy;
    step->state = st;
    return step;
}
