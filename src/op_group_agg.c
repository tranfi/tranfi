/*
 * op_group_agg.c — Group by + aggregate (sum/avg/count/min/max).
 *
 * Config: {"group_by": ["city"], "aggs": [{"column": "sales", "func": "sum", "name": "total"}]}
 * Buffers all rows, emits on flush.
 */

#include "internal.h"
#include "cJSON.h"
#include "date_utils.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef enum {
    AGG_SUM, AGG_AVG, AGG_COUNT, AGG_MIN, AGG_MAX,
} agg_func;

typedef struct {
    char    *column;
    char    *name;
    agg_func func;
} agg_spec;

typedef struct {
    double  *sums;
    double  *mins;
    double  *maxs;
    size_t  *counts;
    size_t   n_aggs;
} group_accum;

typedef struct {
    char  **keys;
    group_accum *accums;
    size_t  count;
    size_t  cap;
} group_map;

typedef struct {
    char      **group_cols;
    size_t      n_group_cols;
    agg_spec   *aggs;
    size_t      n_aggs;
    group_map   map;
} group_agg_state;

static agg_func parse_agg_func(const char *s) {
    if (strcmp(s, "sum") == 0) return AGG_SUM;
    if (strcmp(s, "avg") == 0) return AGG_AVG;
    if (strcmp(s, "count") == 0) return AGG_COUNT;
    if (strcmp(s, "min") == 0) return AGG_MIN;
    if (strcmp(s, "max") == 0) return AGG_MAX;
    return AGG_COUNT;
}

static char *build_group_key(const tf_batch *b, size_t row,
                             int *col_indices, size_t n_keys) {
    size_t buf_cap = 256;
    char *buf = malloc(buf_cap);
    if (!buf) return NULL;
    size_t buf_len = 0;
    for (size_t k = 0; k < n_keys; k++) {
        int c = col_indices[k];
        if (k > 0) { buf[buf_len++] = '\x01'; }
        char val_buf[64];
        const char *val = "";
        size_t val_len = 0;
        if (c < 0 || tf_batch_is_null(b, row, c)) {
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

static int find_or_add_group(group_map *map, const char *key, size_t n_aggs) {
    for (size_t i = 0; i < map->count; i++) {
        if (strcmp(map->keys[i], key) == 0) return (int)i;
    }
    if (map->count >= map->cap) {
        size_t new_cap = map->cap ? map->cap * 2 : 64;
        map->keys = realloc(map->keys, new_cap * sizeof(char *));
        map->accums = realloc(map->accums, new_cap * sizeof(group_accum));
        map->cap = new_cap;
    }
    size_t idx = map->count++;
    map->keys[idx] = strdup(key);
    group_accum *a = &map->accums[idx];
    a->n_aggs = n_aggs;
    a->sums = calloc(n_aggs, sizeof(double));
    a->mins = malloc(n_aggs * sizeof(double));
    a->maxs = malloc(n_aggs * sizeof(double));
    a->counts = calloc(n_aggs, sizeof(size_t));
    for (size_t i = 0; i < n_aggs; i++) { a->mins[i] = 1e308; a->maxs[i] = -1e308; }
    return (int)idx;
}

static int group_agg_process(tf_step *self, tf_batch *in, tf_batch **out,
                             tf_side_channels *side) {
    (void)side;
    group_agg_state *st = self->state;
    *out = NULL;

    int *group_indices = malloc(st->n_group_cols * sizeof(int));
    int *agg_indices = malloc(st->n_aggs * sizeof(int));
    if (!group_indices || !agg_indices) { free(group_indices); free(agg_indices); return TF_ERROR; }

    for (size_t k = 0; k < st->n_group_cols; k++)
        group_indices[k] = tf_batch_col_index(in, st->group_cols[k]);
    for (size_t k = 0; k < st->n_aggs; k++)
        agg_indices[k] = tf_batch_col_index(in, st->aggs[k].column);

    for (size_t r = 0; r < in->n_rows; r++) {
        char *key = build_group_key(in, r, group_indices, st->n_group_cols);
        if (!key) { free(group_indices); free(agg_indices); return TF_ERROR; }
        int gi = find_or_add_group(&st->map, key, st->n_aggs);
        free(key);
        if (gi < 0) { free(group_indices); free(agg_indices); return TF_ERROR; }

        group_accum *a = &st->map.accums[gi];
        for (size_t k = 0; k < st->n_aggs; k++) {
            int ci = agg_indices[k];
            if (st->aggs[k].func == AGG_COUNT) {
                a->counts[k]++;
                continue;
            }
            if (ci < 0 || tf_batch_is_null(in, r, ci)) continue;
            double v = 0;
            if (in->col_types[ci] == TF_TYPE_INT64) v = (double)tf_batch_get_int64(in, r, ci);
            else if (in->col_types[ci] == TF_TYPE_FLOAT64) v = tf_batch_get_float64(in, r, ci);
            a->sums[k] += v;
            if (v < a->mins[k]) a->mins[k] = v;
            if (v > a->maxs[k]) a->maxs[k] = v;
            a->counts[k]++;
        }
    }

    free(group_indices);
    free(agg_indices);
    return TF_OK;
}

static int group_agg_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    group_agg_state *st = self->state;
    *out = NULL;

    if (st->map.count == 0) return TF_OK;

    size_t n_out_cols = st->n_group_cols + st->n_aggs;
    tf_batch *ob = tf_batch_create(n_out_cols, st->map.count);
    if (!ob) return TF_ERROR;

    for (size_t k = 0; k < st->n_group_cols; k++)
        tf_batch_set_schema(ob, k, st->group_cols[k], TF_TYPE_STRING);
    for (size_t k = 0; k < st->n_aggs; k++)
        tf_batch_set_schema(ob, st->n_group_cols + k, st->aggs[k].name, TF_TYPE_FLOAT64);

    for (size_t g = 0; g < st->map.count; g++) {
        tf_batch_ensure_capacity(ob, g + 1);

        /* Parse group key back to column values */
        char *key = strdup(st->map.keys[g]);
        char *p = key;
        for (size_t k = 0; k < st->n_group_cols; k++) {
            char *sep = strchr(p, '\x01');
            if (sep) *sep = '\0';
            if (strcmp(p, "\\N") == 0)
                tf_batch_set_null(ob, g, k);
            else
                tf_batch_set_string(ob, g, k, p);
            p = sep ? sep + 1 : p + strlen(p);
        }
        free(key);

        /* Set aggregates */
        group_accum *a = &st->map.accums[g];
        for (size_t k = 0; k < st->n_aggs; k++) {
            double v = 0;
            switch (st->aggs[k].func) {
                case AGG_SUM: v = a->sums[k]; break;
                case AGG_AVG: v = a->counts[k] > 0 ? a->sums[k] / a->counts[k] : 0; break;
                case AGG_COUNT: v = (double)a->counts[k]; break;
                case AGG_MIN: v = a->counts[k] > 0 ? a->mins[k] : 0; break;
                case AGG_MAX: v = a->counts[k] > 0 ? a->maxs[k] : 0; break;
            }
            tf_batch_set_float64(ob, g, st->n_group_cols + k, v);
        }
        ob->n_rows = g + 1;
    }

    *out = ob;
    return TF_OK;
}

static void group_agg_destroy(tf_step *self) {
    group_agg_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_group_cols; i++) free(st->group_cols[i]);
        free(st->group_cols);
        for (size_t i = 0; i < st->n_aggs; i++) { free(st->aggs[i].column); free(st->aggs[i].name); }
        free(st->aggs);
        for (size_t i = 0; i < st->map.count; i++) {
            free(st->map.keys[i]);
            free(st->map.accums[i].sums);
            free(st->map.accums[i].mins);
            free(st->map.accums[i].maxs);
            free(st->map.accums[i].counts);
        }
        free(st->map.keys);
        free(st->map.accums);
        free(st);
    }
    free(self);
}

tf_step *tf_group_agg_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *group_by = cJSON_GetObjectItemCaseSensitive(args, "group_by");
    cJSON *aggs_j = cJSON_GetObjectItemCaseSensitive(args, "aggs");
    if (!cJSON_IsArray(group_by) || !cJSON_IsArray(aggs_j)) return NULL;

    group_agg_state *st = calloc(1, sizeof(group_agg_state));
    if (!st) return NULL;

    int ng = cJSON_GetArraySize(group_by);
    st->group_cols = calloc(ng, sizeof(char *));
    st->n_group_cols = ng;
    for (int i = 0; i < ng; i++) {
        cJSON *item = cJSON_GetArrayItem(group_by, i);
        if (cJSON_IsString(item)) st->group_cols[i] = strdup(item->valuestring);
    }

    int na = cJSON_GetArraySize(aggs_j);
    st->aggs = calloc(na, sizeof(agg_spec));
    st->n_aggs = na;
    for (int i = 0; i < na; i++) {
        cJSON *item = cJSON_GetArrayItem(aggs_j, i);
        cJSON *col = cJSON_GetObjectItemCaseSensitive(item, "column");
        cJSON *func = cJSON_GetObjectItemCaseSensitive(item, "func");
        cJSON *name = cJSON_GetObjectItemCaseSensitive(item, "name");
        st->aggs[i].column = strdup(cJSON_IsString(col) ? col->valuestring : "");
        st->aggs[i].func = cJSON_IsString(func) ? parse_agg_func(func->valuestring) : AGG_COUNT;
        if (cJSON_IsString(name)) {
            st->aggs[i].name = strdup(name->valuestring);
        } else {
            char buf[256];
            snprintf(buf, sizeof(buf), "%s_%s",
                     st->aggs[i].column,
                     cJSON_IsString(func) ? func->valuestring : "count");
            st->aggs[i].name = strdup(buf);
        }
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { group_agg_destroy(&(tf_step){.state = st}); return NULL; }
    step->process = group_agg_process;
    step->flush = group_agg_flush;
    step->destroy = group_agg_destroy;
    step->state = st;
    return step;
}
