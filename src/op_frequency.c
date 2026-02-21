/*
 * op_frequency.c — Value counts. Hash map → emit sorted by count desc.
 *
 * Config: {"columns": ["city"]}
 *   or {} for frequency of all columns concatenated.
 */

#include "internal.h"
#include "cJSON.h"
#include "date_utils.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char    **keys;
    size_t   *counts;
    size_t    count;
    size_t    cap;
} freq_map;

typedef struct {
    char  **cols;
    size_t  n_cols;
    freq_map map;
} frequency_state;

static char *build_freq_key(const tf_batch *b, size_t row,
                            int *col_indices, size_t n_keys) {
    size_t buf_cap = 256;
    char *buf = malloc(buf_cap);
    if (!buf) return NULL;
    size_t buf_len = 0;
    for (size_t k = 0; k < n_keys; k++) {
        int c = col_indices[k];
        if (k > 0) { if (buf_len + 1 >= buf_cap) { buf_cap *= 2; buf = realloc(buf, buf_cap); } buf[buf_len++] = '\x01'; }
        char val_buf[64];
        const char *val;
        size_t val_len;
        if (c < 0 || tf_batch_is_null(b, row, c)) {
            val = "\\N"; val_len = 2;
        } else {
            switch (b->col_types[c]) {
                case TF_TYPE_STRING: val = tf_batch_get_string(b, row, c); val_len = strlen(val); break;
                case TF_TYPE_INT64:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%lld", (long long)tf_batch_get_int64(b, row, c));
                    val = val_buf; break;
                case TF_TYPE_FLOAT64:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%g", tf_batch_get_float64(b, row, c));
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

static int freq_add(freq_map *map, const char *key) {
    for (size_t i = 0; i < map->count; i++) {
        if (strcmp(map->keys[i], key) == 0) { map->counts[i]++; return 0; }
    }
    if (map->count >= map->cap) {
        size_t new_cap = map->cap ? map->cap * 2 : 64;
        map->keys = realloc(map->keys, new_cap * sizeof(char *));
        map->counts = realloc(map->counts, new_cap * sizeof(size_t));
        map->cap = new_cap;
    }
    map->keys[map->count] = strdup(key);
    map->counts[map->count] = 1;
    map->count++;
    return 0;
}

static int frequency_process(tf_step *self, tf_batch *in, tf_batch **out,
                             tf_side_channels *side) {
    (void)side;
    frequency_state *st = self->state;
    *out = NULL;

    size_t n_keys;
    int *col_indices;
    if (st->n_cols > 0) {
        n_keys = st->n_cols;
        col_indices = malloc(n_keys * sizeof(int));
        for (size_t k = 0; k < n_keys; k++)
            col_indices[k] = tf_batch_col_index(in, st->cols[k]);
    } else {
        n_keys = in->n_cols;
        col_indices = malloc(n_keys * sizeof(int));
        for (size_t k = 0; k < n_keys; k++) col_indices[k] = (int)k;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        char *key = build_freq_key(in, r, col_indices, n_keys);
        if (!key) { free(col_indices); return TF_ERROR; }
        freq_add(&st->map, key);
        free(key);
    }

    free(col_indices);
    return TF_OK;
}

static int frequency_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    frequency_state *st = self->state;
    *out = NULL;

    if (st->map.count == 0) return TF_OK;

    /* Sort by count descending */
    size_t n = st->map.count;
    size_t *indices = malloc(n * sizeof(size_t));
    if (!indices) return TF_ERROR;
    for (size_t i = 0; i < n; i++) indices[i] = i;

    /* Simple sort by count */
    for (size_t i = 0; i < n - 1; i++) {
        for (size_t j = i + 1; j < n; j++) {
            if (st->map.counts[indices[j]] > st->map.counts[indices[i]]) {
                size_t tmp = indices[i]; indices[i] = indices[j]; indices[j] = tmp;
            }
        }
    }

    tf_batch *ob = tf_batch_create(2, n);
    if (!ob) { free(indices); return TF_ERROR; }
    tf_batch_set_schema(ob, 0, "value", TF_TYPE_STRING);
    tf_batch_set_schema(ob, 1, "count", TF_TYPE_INT64);

    for (size_t i = 0; i < n; i++) {
        tf_batch_ensure_capacity(ob, i + 1);
        tf_batch_set_string(ob, i, 0, st->map.keys[indices[i]]);
        tf_batch_set_int64(ob, i, 1, (int64_t)st->map.counts[indices[i]]);
        ob->n_rows = i + 1;
    }

    free(indices);
    *out = ob;
    return TF_OK;
}

static void frequency_destroy(tf_step *self) {
    frequency_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_cols; i++) free(st->cols[i]);
        free(st->cols);
        for (size_t i = 0; i < st->map.count; i++) free(st->map.keys[i]);
        free(st->map.keys); free(st->map.counts);
        free(st);
    }
    free(self);
}

tf_step *tf_frequency_create(const cJSON *args) {
    frequency_state *st = calloc(1, sizeof(frequency_state));
    if (!st) return NULL;

    if (args) {
        cJSON *columns = cJSON_GetObjectItemCaseSensitive(args, "columns");
        if (columns && cJSON_IsArray(columns)) {
            int n = cJSON_GetArraySize(columns);
            if (n > 0) {
                st->cols = calloc(n, sizeof(char *));
                st->n_cols = n;
                for (int i = 0; i < n; i++) {
                    cJSON *item = cJSON_GetArrayItem(columns, i);
                    if (cJSON_IsString(item)) st->cols[i] = strdup(item->valuestring);
                }
            }
        }
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { frequency_destroy(&(tf_step){.state = st}); return NULL; }
    step->process = frequency_process;
    step->flush = frequency_flush;
    step->destroy = frequency_destroy;
    step->state = st;
    return step;
}
