/*
 * op_group_agg.c — Group by + aggregate (sum/avg/count/min/max).
 *
 * Config: {"group_by": ["city"], "aggs": [{"column": "sales", "func": "sum", "name": "total"}]}
 * Maintains one accumulator per group, emits on flush.
 */

#include "internal.h"
#include "cJSON.h"
#include "date_utils.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <errno.h>
#include <unistd.h>

#define GROUP_AGG_DEFAULT_RUN_ROWS 8192
#define GROUP_AGG_DEFAULT_OUTPUT_ROWS 1024
#define GROUP_AGG_MIN_RUN_ROWS 16

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
    char  **keys;       /* serialized lookup keys, indexed by group id */
    group_accum *accums;
    tf_batch *key_batch; /* first-seen group keys with their original types */
    size_t *slots;      /* open-addressed hash table: group id + 1, 0 = empty */
    size_t  slot_cap;
    size_t  count;
    size_t  cap;
    size_t  key_bytes;
} group_map;

typedef union {
    uint8_t b;
    int64_t i64;
    double f64;
    int32_t date;
    char *str;
} group_spill_cell;

typedef struct {
    uint64_t ordinal;
    uint8_t *nulls;
    group_spill_cell *cells;
} group_spill_row;

typedef struct {
    FILE *file;
    group_spill_row row;
    int has_row;
    int done;
} group_run_reader;

typedef struct {
    char      **group_cols;
    size_t      n_group_cols;
    size_t      max_groups;  /* 0 = unlimited */
    size_t      max_state_bytes; /* 0 = unlimited */
    agg_spec   *aggs;
    size_t      n_aggs;
    int         sorted;
    int         use_spill;
    int         sorted_have_current;
    size_t      sorted_groups_started;
    char       *sorted_key;
    group_accum sorted_accum;
    tf_batch   *sorted_key_batch;
    group_map   map;

    char       *spill_dir;
    size_t      spill_memory_bytes;
    size_t      configured_run_rows;
    size_t      run_rows;
    size_t      output_batch_rows;

    int         has_schema;
    char      **schema_names;
    tf_type    *schema_types;
    size_t      n_schema_cols;
    int        *group_indices;
    int        *agg_indices;

    char      **out_schema_names;
    tf_type    *out_schema_types;
    size_t      n_out_schema_cols;

    tf_batch   *buf;
    uint64_t   *buf_ordinals;
    size_t      buf_ordinal_cap;
    uint64_t    next_ordinal;

    char      **run_paths;
    size_t      n_runs;
    size_t      cap_runs;
    char      **out_run_paths;
    size_t      n_out_runs;
    size_t      cap_out_runs;
    size_t      run_seq;
    size_t      out_run_seq;

    group_run_reader *readers;
    size_t      n_readers;
    group_run_reader *out_readers;
    size_t      n_out_readers;

    tf_batch   *out_buf;
    uint64_t   *out_ordinals;
    size_t      out_ordinal_cap;

    int         key_merge_done;
    int         output_merge_started;
    int         output_merge_done;

    size_t      spilled_bytes;
    size_t      spill_runs_created;
    size_t      spill_output_batches;
    size_t      spill_output_rows;
    size_t      spill_distinct_groups;
    size_t      spill_key_bytes;
} group_agg_state;

static size_t group_agg_retained_state_bytes(const group_agg_state *st);

static void group_agg_write_error(tf_side_channels *side, const char *msg) {
    tf_set_last_error(msg);
    if (side && side->errors) {
        tf_buffer_write_str(side->errors, msg);
        tf_buffer_write_str(side->errors, "\n");
    }
}

static size_t json_size_arg(const cJSON *args, const char *name) {
    cJSON *item = cJSON_GetObjectItemCaseSensitive(args, name);
    if (!cJSON_IsNumber(item) || item->valuedouble <= 0.0) return 0;
    if (item->valuedouble > (double)SIZE_MAX) return SIZE_MAX;
    return (size_t)item->valuedouble;
}

static void group_agg_limit_error(const group_agg_state *st, tf_side_channels *side) {
    char msg[160];
    snprintf(msg, sizeof(msg),
             "group-agg: max_groups=%zu exceeded while tracking exact groups",
             st->max_groups);
    group_agg_write_error(side, msg);
}

static int group_agg_check_state_bytes(group_agg_state *st, tf_side_channels *side) {
    if (!st || st->max_state_bytes == 0) return 0;
    size_t retained = group_agg_retained_state_bytes(st);
    if (retained <= st->max_state_bytes) return 0;
    char msg[208];
    snprintf(msg, sizeof(msg),
             "group-agg: max_state_bytes=%zu exceeded while tracking exact groups (%zu bytes retained)",
             st->max_state_bytes, retained);
    group_agg_write_error(side, msg);
    return -1;
}

static agg_func parse_agg_func(const char *s) {
    if (strcmp(s, "sum") == 0) return AGG_SUM;
    if (strcmp(s, "avg") == 0) return AGG_AVG;
    if (strcmp(s, "count") == 0) return AGG_COUNT;
    if (strcmp(s, "min") == 0) return AGG_MIN;
    if (strcmp(s, "max") == 0) return AGG_MAX;
    return AGG_COUNT;
}

static int key_append(char **buf, size_t *buf_cap, size_t *buf_len,
                      const char *data, size_t data_len) {
    while (*buf_len + data_len + 1 >= *buf_cap) {
        size_t new_cap = *buf_cap * 2;
        char *tmp = realloc(*buf, new_cap);
        if (!tmp) return -1;
        *buf = tmp;
        *buf_cap = new_cap;
    }
    memcpy(*buf + *buf_len, data, data_len);
    *buf_len += data_len;
    (*buf)[*buf_len] = '\0';
    return 0;
}

static int key_append_cstr(char **buf, size_t *buf_cap, size_t *buf_len,
                           const char *s) {
    return key_append(buf, buf_cap, buf_len, s, strlen(s));
}

static int key_append_size(char **buf, size_t *buf_cap, size_t *buf_len,
                           size_t value) {
    char tmp[32];
    int n = snprintf(tmp, sizeof(tmp), "%zu", value);
    if (n < 0 || (size_t)n >= sizeof(tmp)) return -1;
    return key_append(buf, buf_cap, buf_len, tmp, (size_t)n);
}

static char *build_group_key(const tf_batch *b, size_t row,
                             int *col_indices, size_t n_keys) {
    size_t buf_cap = 256;
    char *buf = malloc(buf_cap);
    if (!buf) return NULL;
    size_t buf_len = 0;
    buf[0] = '\0';

    for (size_t k = 0; k < n_keys; k++) {
        int c = col_indices[k];
        char val_buf[64];
        const char *val = "";
        size_t val_len = 0;

        if (c < 0 || tf_batch_is_null(b, row, c)) {
            if (key_append_cstr(&buf, &buf_cap, &buf_len, "N|") != 0) {
                free(buf);
                return NULL;
            }
            continue;
        }

        switch (b->col_types[c]) {
            case TF_TYPE_STRING:
                val = tf_batch_get_string(b, row, c);
                if (!val) val = "";
                val_len = strlen(val);
                if (key_append_cstr(&buf, &buf_cap, &buf_len, "S") != 0 ||
                    key_append_size(&buf, &buf_cap, &buf_len, val_len) != 0 ||
                    key_append_cstr(&buf, &buf_cap, &buf_len, ":") != 0 ||
                    key_append(&buf, &buf_cap, &buf_len, val, val_len) != 0 ||
                    key_append_cstr(&buf, &buf_cap, &buf_len, "|") != 0) {
                    free(buf);
                    return NULL;
                }
                continue;
            case TF_TYPE_INT64: {
                int n = snprintf(val_buf, sizeof(val_buf), "%lld",
                                 (long long)tf_batch_get_int64(b, row, c));
                if (n < 0 || (size_t)n >= sizeof(val_buf)) { free(buf); return NULL; }
                val = val_buf;
                val_len = (size_t)n;
                if (key_append_cstr(&buf, &buf_cap, &buf_len, "I") != 0) { free(buf); return NULL; }
                break;
            }
            case TF_TYPE_FLOAT64: {
                int n = snprintf(val_buf, sizeof(val_buf), "%.17g",
                                 tf_batch_get_float64(b, row, c));
                if (n < 0 || (size_t)n >= sizeof(val_buf)) { free(buf); return NULL; }
                val = val_buf;
                val_len = (size_t)n;
                if (key_append_cstr(&buf, &buf_cap, &buf_len, "F") != 0) { free(buf); return NULL; }
                break;
            }
            case TF_TYPE_BOOL:
                val = tf_batch_get_bool(b, row, c) ? "1" : "0";
                val_len = 1;
                if (key_append_cstr(&buf, &buf_cap, &buf_len, "B") != 0) { free(buf); return NULL; }
                break;
            case TF_TYPE_DATE: {
                int n = snprintf(val_buf, sizeof(val_buf), "%d",
                                 (int)tf_batch_get_date(b, row, c));
                if (n < 0 || (size_t)n >= sizeof(val_buf)) { free(buf); return NULL; }
                val = val_buf;
                val_len = (size_t)n;
                if (key_append_cstr(&buf, &buf_cap, &buf_len, "D") != 0) { free(buf); return NULL; }
                break;
            }
            case TF_TYPE_TIMESTAMP: {
                int n = snprintf(val_buf, sizeof(val_buf), "%lld",
                                 (long long)tf_batch_get_timestamp(b, row, c));
                if (n < 0 || (size_t)n >= sizeof(val_buf)) { free(buf); return NULL; }
                val = val_buf;
                val_len = (size_t)n;
                if (key_append_cstr(&buf, &buf_cap, &buf_len, "T") != 0) { free(buf); return NULL; }
                break;
            }
            default:
                if (key_append_cstr(&buf, &buf_cap, &buf_len, "N|") != 0) {
                    free(buf);
                    return NULL;
                }
                continue;
        }

        if (key_append(&buf, &buf_cap, &buf_len, val, val_len) != 0 ||
            key_append_cstr(&buf, &buf_cap, &buf_len, "|") != 0) {
            free(buf);
            return NULL;
        }
    }

    return buf;
}

static size_t group_hash(const char *key) {
    uint64_t h = 1469598103934665603ULL;
    for (const unsigned char *p = (const unsigned char *)key; *p; p++) {
        h ^= (uint64_t)*p;
        h *= 1099511628211ULL;
    }
    return (size_t)h;
}

static int group_accum_init(group_accum *a, size_t n_aggs) {
    memset(a, 0, sizeof(*a));
    a->n_aggs = n_aggs;
    a->sums = calloc(n_aggs, sizeof(double));
    a->mins = malloc(n_aggs * sizeof(double));
    a->maxs = malloc(n_aggs * sizeof(double));
    a->counts = calloc(n_aggs, sizeof(size_t));
    if (!a->sums || !a->mins || !a->maxs || !a->counts) {
        free(a->sums);
        free(a->mins);
        free(a->maxs);
        free(a->counts);
        memset(a, 0, sizeof(*a));
        return -1;
    }
    for (size_t i = 0; i < n_aggs; i++) { a->mins[i] = 1e308; a->maxs[i] = -1e308; }
    return 0;
}

static void group_accum_free(group_accum *a) {
    free(a->sums);
    free(a->mins);
    free(a->maxs);
    free(a->counts);
}

static int group_map_init(group_map *map) {
    memset(map, 0, sizeof(*map));
    map->cap = 64;
    map->slot_cap = 256;
    map->keys = calloc(map->cap, sizeof(char *));
    map->accums = calloc(map->cap, sizeof(group_accum));
    map->slots = calloc(map->slot_cap, sizeof(size_t));
    if (!map->keys || !map->accums || !map->slots) return -1;
    return 0;
}

static void group_map_free(group_map *map) {
    for (size_t i = 0; i < map->count; i++) {
        free(map->keys[i]);
        group_accum_free(&map->accums[i]);
    }
    free(map->keys);
    free(map->accums);
    free(map->slots);
    if (map->key_batch) tf_batch_free(map->key_batch);
}

static size_t group_map_find_slot(const group_map *map, const char *key, int *found) {
    size_t idx = group_hash(key) % map->slot_cap;
    while (map->slots[idx] != 0) {
        size_t group_idx = map->slots[idx] - 1;
        if (strcmp(map->keys[group_idx], key) == 0) {
            *found = 1;
            return idx;
        }
        idx = (idx + 1) % map->slot_cap;
    }
    *found = 0;
    return idx;
}

static int group_map_rehash(group_map *map, size_t new_slot_cap) {
    size_t *new_slots = calloc(new_slot_cap, sizeof(size_t));
    if (!new_slots) return -1;

    for (size_t group_idx = 0; group_idx < map->count; group_idx++) {
        size_t idx = group_hash(map->keys[group_idx]) % new_slot_cap;
        while (new_slots[idx] != 0) idx = (idx + 1) % new_slot_cap;
        new_slots[idx] = group_idx + 1;
    }

    free(map->slots);
    map->slots = new_slots;
    map->slot_cap = new_slot_cap;
    return 0;
}

static int group_map_ensure_group_capacity(group_map *map, size_t min_groups) {
    if (min_groups <= map->cap) return 0;
    size_t old_cap = map->cap;
    size_t new_cap = map->cap ? map->cap : 64;
    while (new_cap < min_groups) new_cap *= 2;

    char **new_keys = realloc(map->keys, new_cap * sizeof(char *));
    if (!new_keys) return -1;
    map->keys = new_keys;
    memset(map->keys + old_cap, 0, (new_cap - old_cap) * sizeof(char *));

    group_accum *new_accums = realloc(map->accums, new_cap * sizeof(group_accum));
    if (!new_accums) return -1;
    map->accums = new_accums;
    memset(map->accums + old_cap, 0, (new_cap - old_cap) * sizeof(group_accum));
    map->cap = new_cap;
    return 0;
}

static int ensure_key_batch(group_agg_state *st, const tf_batch *in, int *group_indices) {
    if (st->map.key_batch) return 0;
    st->map.key_batch = tf_batch_create(st->n_group_cols, st->map.cap ? st->map.cap : 64);
    if (!st->map.key_batch) return -1;
    for (size_t k = 0; k < st->n_group_cols; k++) {
        int ci = group_indices[k];
        tf_type type = (ci >= 0) ? in->col_types[ci] : TF_TYPE_STRING;
        const char *name = st->group_cols[k] ? st->group_cols[k] : "?";
        if (tf_batch_set_schema(st->map.key_batch, k, name, type) != TF_OK) return -1;
    }
    return 0;
}

static void copy_group_key_values(tf_batch *keys, size_t dst_row,
                                  const tf_batch *in, size_t src_row,
                                  int *group_indices, size_t n_group_cols) {
    for (size_t k = 0; k < n_group_cols; k++) {
        int ci = group_indices[k];
        if (ci < 0 || tf_batch_is_null(in, src_row, ci)) {
            tf_batch_set_null(keys, dst_row, k);
            continue;
        }
        switch (keys->col_types[k]) {
            case TF_TYPE_BOOL:
                tf_batch_set_bool(keys, dst_row, k, tf_batch_get_bool(in, src_row, ci));
                break;
            case TF_TYPE_INT64:
                tf_batch_set_int64(keys, dst_row, k, tf_batch_get_int64(in, src_row, ci));
                break;
            case TF_TYPE_FLOAT64:
                tf_batch_set_float64(keys, dst_row, k, tf_batch_get_float64(in, src_row, ci));
                break;
            case TF_TYPE_STRING: {
                const char *s = tf_batch_get_string(in, src_row, ci);
                tf_batch_set_string(keys, dst_row, k, s ? s : "");
                break;
            }
            case TF_TYPE_DATE:
                tf_batch_set_date(keys, dst_row, k, tf_batch_get_date(in, src_row, ci));
                break;
            case TF_TYPE_TIMESTAMP:
                tf_batch_set_timestamp(keys, dst_row, k, tf_batch_get_timestamp(in, src_row, ci));
                break;
            default:
                tf_batch_set_null(keys, dst_row, k);
                break;
        }
    }
}


static int ensure_sorted_key_batch(group_agg_state *st, const tf_batch *in, int *group_indices) {
    if (st->sorted_key_batch) return 0;
    st->sorted_key_batch = tf_batch_create(st->n_group_cols, 1);
    if (!st->sorted_key_batch) return -1;
    for (size_t k = 0; k < st->n_group_cols; k++) {
        int ci = group_indices[k];
        tf_type type = (ci >= 0) ? in->col_types[ci] : TF_TYPE_STRING;
        const char *name = st->group_cols[k] ? st->group_cols[k] : "?";
        if (tf_batch_set_schema(st->sorted_key_batch, k, name, type) != TF_OK) return -1;
    }
    return 0;
}

static int agg_counts_rows(const agg_spec *agg) {
    return agg->func == AGG_COUNT &&
           (!agg->column || agg->column[0] == '\0' || strcmp(agg->column, "*") == 0);
}

static void group_accum_add_row(group_accum *a, const group_agg_state *st,
                                const tf_batch *in, size_t row, int *agg_indices) {
    for (size_t k = 0; k < st->n_aggs; k++) {
        int ci = agg_indices[k];
        if (st->aggs[k].func == AGG_COUNT) {
            if (agg_counts_rows(&st->aggs[k]) || (ci >= 0 && !tf_batch_is_null(in, row, ci))) {
                a->counts[k]++;
            }
            continue;
        }
        if (ci < 0 || tf_batch_is_null(in, row, ci)) continue;
        double v = 0;
        if (in->col_types[ci] == TF_TYPE_INT64) v = (double)tf_batch_get_int64(in, row, ci);
        else if (in->col_types[ci] == TF_TYPE_FLOAT64) v = tf_batch_get_float64(in, row, ci);
        a->sums[k] += v;
        if (v < a->mins[k]) a->mins[k] = v;
        if (v > a->maxs[k]) a->maxs[k] = v;
        a->counts[k]++;
    }
}

static void group_agg_set_aggregate_cell(group_agg_state *st, tf_batch *ob,
                                         size_t row, size_t agg_idx,
                                         const group_accum *a) {
    size_t col = st->n_group_cols + agg_idx;
    double v = 0;
    int is_null = 0;
    switch (st->aggs[agg_idx].func) {
        case AGG_SUM:
            if (a->counts[agg_idx] == 0) is_null = 1;
            else v = a->sums[agg_idx];
            break;
        case AGG_AVG:
            if (a->counts[agg_idx] == 0) is_null = 1;
            else v = a->sums[agg_idx] / a->counts[agg_idx];
            break;
        case AGG_COUNT:
            v = (double)a->counts[agg_idx];
            break;
        case AGG_MIN:
            if (a->counts[agg_idx] == 0) is_null = 1;
            else v = a->mins[agg_idx];
            break;
        case AGG_MAX:
            if (a->counts[agg_idx] == 0) is_null = 1;
            else v = a->maxs[agg_idx];
            break;
    }
    if (is_null) tf_batch_set_null(ob, row, col);
    else tf_batch_set_float64(ob, row, col, v);
}

static int group_agg_set_output_schema(group_agg_state *st, tf_batch *ob,
                                       const tf_batch *key_batch) {
    for (size_t k = 0; k < st->n_group_cols; k++) {
        tf_type type = key_batch ? key_batch->col_types[k] : TF_TYPE_STRING;
        if (tf_batch_set_schema(ob, k, st->group_cols[k], type) != TF_OK) return -1;
    }
    for (size_t k = 0; k < st->n_aggs; k++) {
        if (tf_batch_set_schema(ob, st->n_group_cols + k, st->aggs[k].name, TF_TYPE_FLOAT64) != TF_OK) return -1;
    }
    return 0;
}

static int group_agg_emit_row(group_agg_state *st, tf_batch *ob, size_t out_row,
                              const tf_batch *key_batch, size_t key_row,
                              const group_accum *a) {
    if (tf_batch_ensure_capacity(ob, out_row + 1) != TF_OK) return -1;
    if (key_batch && tf_batch_copy_row(ob, out_row, key_batch, key_row) != TF_OK) return -1;

    for (size_t k = 0; k < st->n_aggs; k++) {
        group_agg_set_aggregate_cell(st, ob, out_row, k, a);
    }
    ob->n_rows = out_row + 1;
    return 0;
}

static int ensure_ordinals(uint64_t **ord, size_t *cap, size_t need) {
    if (*cap >= need) return TF_OK;
    size_t new_cap = *cap ? *cap * 2 : 16;
    while (new_cap < need) new_cap *= 2;
    uint64_t *tmp = realloc(*ord, new_cap * sizeof(uint64_t));
    if (!tmp) return TF_ERROR;
    *ord = tmp;
    *cap = new_cap;
    return TF_OK;
}

static tf_batch *group_agg_create_input_buffer(const group_agg_state *st, size_t capacity) {
    tf_batch *b = tf_batch_create(st->n_schema_cols, capacity ? capacity : 16);
    if (!b) return NULL;
    for (size_t c = 0; c < st->n_schema_cols; c++) {
        if (tf_batch_set_schema(b, c, st->schema_names[c], st->schema_types[c]) != TF_OK) {
            tf_batch_free(b);
            return NULL;
        }
    }
    return b;
}

static tf_batch *group_agg_create_output_buffer(const group_agg_state *st, size_t capacity) {
    tf_batch *b = tf_batch_create(st->n_out_schema_cols, capacity ? capacity : 16);
    if (!b) return NULL;
    for (size_t c = 0; c < st->n_out_schema_cols; c++) {
        if (tf_batch_set_schema(b, c, st->out_schema_names[c], st->out_schema_types[c]) != TF_OK) {
            tf_batch_free(b);
            return NULL;
        }
    }
    return b;
}

static tf_batch *group_agg_create_key_batch(const group_agg_state *st) {
    tf_batch *b = tf_batch_create(st->n_group_cols, 1);
    if (!b) return NULL;
    for (size_t c = 0; c < st->n_group_cols; c++) {
        if (tf_batch_set_schema(b, c, st->out_schema_names[c], st->out_schema_types[c]) != TF_OK) {
            tf_batch_free(b);
            return NULL;
        }
    }
    return b;
}

static size_t group_agg_estimated_spill_row_bytes(const group_agg_state *st) {
    size_t bytes = 48;
    for (size_t c = 0; c < st->n_schema_cols; c++) {
        bytes += 1;
        switch (st->schema_types[c]) {
            case TF_TYPE_BOOL: bytes += 1; break;
            case TF_TYPE_INT64: bytes += sizeof(int64_t); break;
            case TF_TYPE_FLOAT64: bytes += sizeof(double); break;
            case TF_TYPE_STRING: bytes += sizeof(char *) + 64; break;
            case TF_TYPE_DATE: bytes += sizeof(int32_t); break;
            case TF_TYPE_TIMESTAMP: bytes += sizeof(int64_t); break;
            default: break;
        }
    }
    for (size_t c = 0; c < st->n_out_schema_cols; c++) {
        bytes += 1;
        switch (st->out_schema_types[c]) {
            case TF_TYPE_BOOL: bytes += 1; break;
            case TF_TYPE_INT64: bytes += sizeof(int64_t); break;
            case TF_TYPE_FLOAT64: bytes += sizeof(double); break;
            case TF_TYPE_STRING: bytes += sizeof(char *) + 64; break;
            case TF_TYPE_DATE: bytes += sizeof(int32_t); break;
            case TF_TYPE_TIMESTAMP: bytes += sizeof(int64_t); break;
            default: break;
        }
    }
    return bytes < 64 ? 64 : bytes;
}

static int group_agg_init_spill_schema(group_agg_state *st, const tf_batch *in) {
    if (st->has_schema) return TF_OK;

    st->n_schema_cols = in->n_cols;
    st->schema_names = calloc(in->n_cols ? in->n_cols : 1, sizeof(char *));
    st->schema_types = calloc(in->n_cols ? in->n_cols : 1, sizeof(tf_type));
    if (!st->schema_names || !st->schema_types) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++) {
        st->schema_names[c] = strdup(in->col_names[c] ? in->col_names[c] : "");
        if (!st->schema_names[c]) return TF_ERROR;
        st->schema_types[c] = in->col_types[c];
    }

    st->group_indices = calloc(st->n_group_cols ? st->n_group_cols : 1, sizeof(int));
    st->agg_indices = calloc(st->n_aggs ? st->n_aggs : 1, sizeof(int));
    if (!st->group_indices || !st->agg_indices) return TF_ERROR;
    for (size_t k = 0; k < st->n_group_cols; k++) st->group_indices[k] = tf_batch_col_index(in, st->group_cols[k]);
    for (size_t k = 0; k < st->n_aggs; k++) st->agg_indices[k] = tf_batch_col_index(in, st->aggs[k].column);

    st->n_out_schema_cols = st->n_group_cols + st->n_aggs;
    st->out_schema_names = calloc(st->n_out_schema_cols ? st->n_out_schema_cols : 1, sizeof(char *));
    st->out_schema_types = calloc(st->n_out_schema_cols ? st->n_out_schema_cols : 1, sizeof(tf_type));
    if (!st->out_schema_names || !st->out_schema_types) return TF_ERROR;
    for (size_t k = 0; k < st->n_group_cols; k++) {
        int ci = st->group_indices[k];
        st->out_schema_names[k] = strdup(st->group_cols[k] ? st->group_cols[k] : "?");
        if (!st->out_schema_names[k]) return TF_ERROR;
        st->out_schema_types[k] = ci >= 0 ? in->col_types[ci] : TF_TYPE_STRING;
    }
    for (size_t k = 0; k < st->n_aggs; k++) {
        size_t c = st->n_group_cols + k;
        st->out_schema_names[c] = strdup(st->aggs[k].name ? st->aggs[k].name : "agg");
        if (!st->out_schema_names[c]) return TF_ERROR;
        st->out_schema_types[c] = TF_TYPE_FLOAT64;
    }

    if (st->configured_run_rows > 0) {
        st->run_rows = st->configured_run_rows;
    } else if (st->spill_memory_bytes > 0) {
        size_t row_bytes = group_agg_estimated_spill_row_bytes(st);
        st->run_rows = st->spill_memory_bytes / (row_bytes * 4);
        if (st->run_rows < GROUP_AGG_MIN_RUN_ROWS) st->run_rows = GROUP_AGG_MIN_RUN_ROWS;
    } else {
        st->run_rows = GROUP_AGG_DEFAULT_RUN_ROWS;
    }
    if (st->run_rows == 0 || st->run_rows == SIZE_MAX) st->run_rows = GROUP_AGG_DEFAULT_RUN_ROWS;

    st->buf = group_agg_create_input_buffer(st, st->run_rows);
    st->out_buf = group_agg_create_output_buffer(st, st->run_rows);
    if (!st->buf || !st->out_buf) return TF_ERROR;
    st->has_schema = 1;
    return TF_OK;
}

static int compare_batch_group_key_rows(const group_agg_state *st, const tf_batch *batch, size_t ra, size_t rb) {
    for (size_t k = 0; k < st->n_group_cols; k++) {
        int ci = st->group_indices[k];
        if (ci < 0) continue;
        size_t c = (size_t)ci;
        int null_a = tf_batch_is_null(batch, ra, c);
        int null_b = tf_batch_is_null(batch, rb, c);
        if (null_a && null_b) continue;
        if (null_a) return 1;
        if (null_b) return -1;
        int cmp = 0;
        switch (batch->col_types[c]) {
            case TF_TYPE_BOOL: cmp = (int)tf_batch_get_bool(batch, ra, c) - (int)tf_batch_get_bool(batch, rb, c); break;
            case TF_TYPE_INT64: {
                int64_t a = tf_batch_get_int64(batch, ra, c), b = tf_batch_get_int64(batch, rb, c);
                cmp = (a > b) - (a < b);
                break;
            }
            case TF_TYPE_FLOAT64: {
                double a = tf_batch_get_float64(batch, ra, c), b = tf_batch_get_float64(batch, rb, c);
                cmp = (a > b) - (a < b);
                break;
            }
            case TF_TYPE_STRING: cmp = strcmp(tf_batch_get_string(batch, ra, c), tf_batch_get_string(batch, rb, c)); break;
            case TF_TYPE_DATE: {
                int32_t a = tf_batch_get_date(batch, ra, c), b = tf_batch_get_date(batch, rb, c);
                cmp = (a > b) - (a < b);
                break;
            }
            case TF_TYPE_TIMESTAMP: {
                int64_t a = tf_batch_get_timestamp(batch, ra, c), b = tf_batch_get_timestamp(batch, rb, c);
                cmp = (a > b) - (a < b);
                break;
            }
            default: break;
        }
        if (cmp != 0) return cmp;
    }
    uint64_t oa = st->buf_ordinals[ra], ob = st->buf_ordinals[rb];
    return (oa > ob) - (oa < ob);
}

static const group_agg_state *g_group_key_sort_state;
static int compare_group_key_indices(const void *a, const void *b) {
    size_t ra = *(const size_t *)a;
    size_t rb = *(const size_t *)b;
    return compare_batch_group_key_rows(g_group_key_sort_state, g_group_key_sort_state->buf, ra, rb);
}

static const group_agg_state *g_group_ordinal_sort_state;
static int compare_group_ordinal_indices(const void *a, const void *b) {
    size_t ra = *(const size_t *)a;
    size_t rb = *(const size_t *)b;
    uint64_t oa = g_group_ordinal_sort_state->out_ordinals[ra];
    uint64_t ob = g_group_ordinal_sort_state->out_ordinals[rb];
    return (oa > ob) - (oa < ob);
}

static size_t *group_agg_sorted_indices(size_t n, int by_ordinal, const group_agg_state *st) {
    size_t *idx = malloc((n ? n : 1) * sizeof(size_t));
    if (!idx) return NULL;
    for (size_t i = 0; i < n; i++) idx[i] = i;
    if (by_ordinal) {
        g_group_ordinal_sort_state = st;
        qsort(idx, n, sizeof(size_t), compare_group_ordinal_indices);
        g_group_ordinal_sort_state = NULL;
    } else {
        g_group_key_sort_state = st;
        qsort(idx, n, sizeof(size_t), compare_group_key_indices);
        g_group_key_sort_state = NULL;
    }
    return idx;
}

static int write_exact(FILE *f, const void *ptr, size_t len) {
    return fwrite(ptr, 1, len, f) == len ? TF_OK : TF_ERROR;
}

static int read_exact(FILE *f, void *ptr, size_t len) {
    return fread(ptr, 1, len, f) == len ? TF_OK : TF_ERROR;
}

static int write_cell(FILE *f, const tf_batch *b, size_t r, size_t c) {
    uint8_t is_null = tf_batch_is_null(b, r, c) ? 1 : 0;
    if (write_exact(f, &is_null, sizeof(is_null)) != TF_OK) return TF_ERROR;
    if (is_null) return TF_OK;
    switch (b->col_types[c]) {
        case TF_TYPE_BOOL: { uint8_t v = tf_batch_get_bool(b, r, c) ? 1 : 0; return write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_INT64: { int64_t v = tf_batch_get_int64(b, r, c); return write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_FLOAT64: { double v = tf_batch_get_float64(b, r, c); return write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_STRING: {
            const char *str = tf_batch_get_string(b, r, c);
            uint64_t len = str ? (uint64_t)strlen(str) : 0;
            if (write_exact(f, &len, sizeof(len)) != TF_OK) return TF_ERROR;
            return len ? write_exact(f, str, (size_t)len) : TF_OK;
        }
        case TF_TYPE_DATE: { int32_t v = tf_batch_get_date(b, r, c); return write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_TIMESTAMP: { int64_t v = tf_batch_get_timestamp(b, r, c); return write_exact(f, &v, sizeof(v)); }
        default: return TF_OK;
    }
}

static char *group_agg_make_run_path(group_agg_state *st, int output_run) {
    size_t dir_len = strlen(st->spill_dir);
    size_t cap = dir_len + 128;
    char *path = malloc(cap);
    if (!path) return NULL;
    snprintf(path, cap, "%s%stranfi-group-agg-%s-%ld-%zu.bin",
             st->spill_dir,
             (dir_len > 0 && st->spill_dir[dir_len - 1] == '/') ? "" : "/",
             output_run ? "out" : "key",
             (long)getpid(), output_run ? st->out_run_seq++ : st->run_seq++);
    return path;
}

static int append_path(char ***paths, size_t *n, size_t *cap, char *path) {
    if (*n == *cap) {
        size_t new_cap = *cap ? *cap * 2 : 8;
        char **tmp = realloc(*paths, new_cap * sizeof(char *));
        if (!tmp) return TF_ERROR;
        *paths = tmp;
        *cap = new_cap;
    }
    (*paths)[(*n)++] = path;
    return TF_OK;
}

static int group_agg_write_batch_run(group_agg_state *st, tf_batch *batch, const uint64_t *ordinals,
                                     size_t *indices, size_t n, int output_run) {
    char *path = group_agg_make_run_path(st, output_run);
    if (!path) return TF_ERROR;
    FILE *f = fopen(path, "wb");
    if (!f) {
        char msg[512];
        snprintf(msg, sizeof(msg), "group-agg spill: cannot create '%s': %s", path, strerror(errno));
        tf_set_last_error(msg);
        free(path);
        return TF_ERROR;
    }
    for (size_t i = 0; i < n; i++) {
        size_t r = indices[i];
        uint64_t ordinal = ordinals[r];
        if (write_exact(f, &ordinal, sizeof(ordinal)) != TF_OK) goto write_fail;
        for (size_t c = 0; c < batch->n_cols; c++) {
            if (write_cell(f, batch, r, c) != TF_OK) goto write_fail;
        }
    }
    long pos = ftell(f);
    if (pos > 0) st->spilled_bytes += (size_t)pos;
    if (fclose(f) != 0) {
        tf_set_last_error("group-agg spill: failed closing run file");
        remove(path);
        free(path);
        return TF_ERROR;
    }
    if (output_run) {
        if (append_path(&st->out_run_paths, &st->n_out_runs, &st->cap_out_runs, path) != TF_OK) {
            remove(path); free(path); return TF_ERROR;
        }
    } else {
        if (append_path(&st->run_paths, &st->n_runs, &st->cap_runs, path) != TF_OK) {
            remove(path); free(path); return TF_ERROR;
        }
    }
    st->spill_runs_created++;
    return TF_OK;

write_fail:
    tf_set_last_error("group-agg spill: failed writing run file");
    fclose(f);
    remove(path);
    free(path);
    return TF_ERROR;
}

static int group_agg_write_key_run(group_agg_state *st) {
    if (!st->buf || st->buf->n_rows == 0) return TF_OK;
    size_t *idx = group_agg_sorted_indices(st->buf->n_rows, 0, st);
    if (!idx) return TF_ERROR;
    int rc = group_agg_write_batch_run(st, st->buf, st->buf_ordinals, idx, st->buf->n_rows, 0);
    free(idx);
    if (rc != TF_OK) return TF_ERROR;
    tf_batch_free(st->buf);
    st->buf = group_agg_create_input_buffer(st, st->run_rows);
    st->buf_ordinal_cap = 0;
    free(st->buf_ordinals);
    st->buf_ordinals = NULL;
    return st->buf ? TF_OK : TF_ERROR;
}

static int group_agg_write_output_run(group_agg_state *st) {
    if (!st->out_buf || st->out_buf->n_rows == 0) return TF_OK;
    size_t *idx = group_agg_sorted_indices(st->out_buf->n_rows, 1, st);
    if (!idx) return TF_ERROR;
    int rc = group_agg_write_batch_run(st, st->out_buf, st->out_ordinals, idx, st->out_buf->n_rows, 1);
    free(idx);
    if (rc != TF_OK) return TF_ERROR;
    tf_batch_free(st->out_buf);
    st->out_buf = group_agg_create_output_buffer(st, st->run_rows);
    st->out_ordinal_cap = 0;
    free(st->out_ordinals);
    st->out_ordinals = NULL;
    return st->out_buf ? TF_OK : TF_ERROR;
}

static void spill_row_clear(group_spill_row *row, const tf_type *types, size_t n_cols) {
    if (!row || !row->cells || !row->nulls) return;
    for (size_t c = 0; c < n_cols; c++) {
        if (!row->nulls[c] && types[c] == TF_TYPE_STRING) free(row->cells[c].str);
        row->cells[c].str = NULL;
        row->nulls[c] = 1;
    }
}

static int spill_row_init(group_spill_row *row, size_t n_cols) {
    row->ordinal = 0;
    row->nulls = calloc(n_cols ? n_cols : 1, sizeof(uint8_t));
    row->cells = calloc(n_cols ? n_cols : 1, sizeof(group_spill_cell));
    if (!row->nulls || !row->cells) return TF_ERROR;
    for (size_t c = 0; c < n_cols; c++) row->nulls[c] = 1;
    return TF_OK;
}

static void spill_row_free(group_spill_row *row, const tf_type *types, size_t n_cols) {
    if (!row) return;
    spill_row_clear(row, types, n_cols);
    free(row->nulls);
    free(row->cells);
    row->nulls = NULL;
    row->cells = NULL;
}

static int read_cell_value(FILE *f, group_spill_row *row, tf_type type, size_t c) {
    switch (type) {
        case TF_TYPE_BOOL: { uint8_t v = 0; if (read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].b = v; return TF_OK; }
        case TF_TYPE_INT64: { int64_t v = 0; if (read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].i64 = v; return TF_OK; }
        case TF_TYPE_FLOAT64: { double v = 0; if (read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].f64 = v; return TF_OK; }
        case TF_TYPE_STRING: {
            uint64_t len = 0;
            if (read_exact(f, &len, sizeof(len)) != TF_OK) return TF_ERROR;
            if (len > (uint64_t)SIZE_MAX - 1) return TF_ERROR;
            char *str = malloc((size_t)len + 1);
            if (!str) return TF_ERROR;
            if (len && read_exact(f, str, (size_t)len) != TF_OK) { free(str); return TF_ERROR; }
            str[len] = '\0';
            row->cells[c].str = str;
            return TF_OK;
        }
        case TF_TYPE_DATE: { int32_t v = 0; if (read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].date = v; return TF_OK; }
        case TF_TYPE_TIMESTAMP: { int64_t v = 0; if (read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].i64 = v; return TF_OK; }
        default: return TF_OK;
    }
}

static const tf_type *group_agg_reader_types(const group_agg_state *st, int output_reader) {
    return output_reader ? st->out_schema_types : st->schema_types;
}

static size_t group_agg_reader_cols(const group_agg_state *st, int output_reader) {
    return output_reader ? st->n_out_schema_cols : st->n_schema_cols;
}

static int group_agg_reader_advance(group_agg_state *st, group_run_reader *reader, int output_reader) {
    if (!reader || !reader->file || reader->done) return 0;
    const tf_type *types = group_agg_reader_types(st, output_reader);
    size_t n_cols = group_agg_reader_cols(st, output_reader);
    spill_row_clear(&reader->row, types, n_cols);
    if (fread(&reader->row.ordinal, sizeof(reader->row.ordinal), 1, reader->file) != 1) {
        if (feof(reader->file)) {
            reader->done = 1;
            reader->has_row = 0;
            return 0;
        }
        tf_set_last_error("group-agg spill: failed reading run file");
        return -1;
    }
    for (size_t c = 0; c < n_cols; c++) {
        uint8_t is_null = 1;
        if (read_exact(reader->file, &is_null, sizeof(is_null)) != TF_OK) {
            tf_set_last_error("group-agg spill: corrupt run file");
            return -1;
        }
        reader->row.nulls[c] = is_null ? 1 : 0;
        if (!reader->row.nulls[c] && read_cell_value(reader->file, &reader->row, types[c], c) != TF_OK) {
            tf_set_last_error("group-agg spill: corrupt run file");
            return -1;
        }
    }
    reader->has_row = 1;
    return 1;
}

static int compare_spill_group_key_rows(const group_agg_state *st, const group_spill_row *a, const group_spill_row *b) {
    for (size_t k = 0; k < st->n_group_cols; k++) {
        int ci = st->group_indices[k];
        if (ci < 0) continue;
        size_t c = (size_t)ci;
        int null_a = a->nulls[c] != 0;
        int null_b = b->nulls[c] != 0;
        if (null_a && null_b) continue;
        if (null_a) return 1;
        if (null_b) return -1;
        int cmp = 0;
        switch (st->schema_types[c]) {
            case TF_TYPE_BOOL: cmp = (int)a->cells[c].b - (int)b->cells[c].b; break;
            case TF_TYPE_INT64:
            case TF_TYPE_TIMESTAMP: cmp = (a->cells[c].i64 > b->cells[c].i64) - (a->cells[c].i64 < b->cells[c].i64); break;
            case TF_TYPE_FLOAT64: cmp = (a->cells[c].f64 > b->cells[c].f64) - (a->cells[c].f64 < b->cells[c].f64); break;
            case TF_TYPE_STRING: cmp = strcmp(a->cells[c].str, b->cells[c].str); break;
            case TF_TYPE_DATE: cmp = (a->cells[c].date > b->cells[c].date) - (a->cells[c].date < b->cells[c].date); break;
            default: break;
        }
        if (cmp != 0) return cmp;
    }
    return (a->ordinal > b->ordinal) - (a->ordinal < b->ordinal);
}

static char *build_spill_row_key(const group_agg_state *st, const group_spill_row *row) {
    size_t buf_cap = 256;
    char *buf = malloc(buf_cap);
    if (!buf) return NULL;
    size_t buf_len = 0;
    buf[0] = '\0';

    for (size_t k = 0; k < st->n_group_cols; k++) {
        int ci = st->group_indices[k];
        char val_buf[64];
        const char *val = "";
        size_t val_len = 0;

        if (ci < 0 || row->nulls[(size_t)ci]) {
            if (key_append_cstr(&buf, &buf_cap, &buf_len, "N|") != 0) { free(buf); return NULL; }
            continue;
        }

        size_t c = (size_t)ci;
        switch (st->schema_types[c]) {
            case TF_TYPE_STRING:
                val = row->cells[c].str ? row->cells[c].str : "";
                val_len = strlen(val);
                if (key_append_cstr(&buf, &buf_cap, &buf_len, "S") != 0 ||
                    key_append_size(&buf, &buf_cap, &buf_len, val_len) != 0 ||
                    key_append_cstr(&buf, &buf_cap, &buf_len, ":") != 0 ||
                    key_append(&buf, &buf_cap, &buf_len, val, val_len) != 0 ||
                    key_append_cstr(&buf, &buf_cap, &buf_len, "|") != 0) {
                    free(buf);
                    return NULL;
                }
                continue;
            case TF_TYPE_INT64: {
                int n = snprintf(val_buf, sizeof(val_buf), "%lld", (long long)row->cells[c].i64);
                if (n < 0 || (size_t)n >= sizeof(val_buf)) { free(buf); return NULL; }
                val = val_buf; val_len = (size_t)n;
                if (key_append_cstr(&buf, &buf_cap, &buf_len, "I") != 0) { free(buf); return NULL; }
                break;
            }
            case TF_TYPE_FLOAT64: {
                int n = snprintf(val_buf, sizeof(val_buf), "%.17g", row->cells[c].f64);
                if (n < 0 || (size_t)n >= sizeof(val_buf)) { free(buf); return NULL; }
                val = val_buf; val_len = (size_t)n;
                if (key_append_cstr(&buf, &buf_cap, &buf_len, "F") != 0) { free(buf); return NULL; }
                break;
            }
            case TF_TYPE_BOOL:
                val = row->cells[c].b ? "1" : "0"; val_len = 1;
                if (key_append_cstr(&buf, &buf_cap, &buf_len, "B") != 0) { free(buf); return NULL; }
                break;
            case TF_TYPE_DATE: {
                int n = snprintf(val_buf, sizeof(val_buf), "%d", (int)row->cells[c].date);
                if (n < 0 || (size_t)n >= sizeof(val_buf)) { free(buf); return NULL; }
                val = val_buf; val_len = (size_t)n;
                if (key_append_cstr(&buf, &buf_cap, &buf_len, "D") != 0) { free(buf); return NULL; }
                break;
            }
            case TF_TYPE_TIMESTAMP: {
                int n = snprintf(val_buf, sizeof(val_buf), "%lld", (long long)row->cells[c].i64);
                if (n < 0 || (size_t)n >= sizeof(val_buf)) { free(buf); return NULL; }
                val = val_buf; val_len = (size_t)n;
                if (key_append_cstr(&buf, &buf_cap, &buf_len, "T") != 0) { free(buf); return NULL; }
                break;
            }
            default:
                if (key_append_cstr(&buf, &buf_cap, &buf_len, "N|") != 0) { free(buf); return NULL; }
                continue;
        }
        if (key_append(&buf, &buf_cap, &buf_len, val, val_len) != 0 ||
            key_append_cstr(&buf, &buf_cap, &buf_len, "|") != 0) {
            free(buf);
            return NULL;
        }
    }
    return buf;
}

static int spill_row_to_batch(const group_agg_state *st, tf_batch *out, size_t dst_row, const group_spill_row *row, int output_schema) {
    const tf_type *types = output_schema ? st->out_schema_types : st->schema_types;
    size_t n_cols = output_schema ? st->n_out_schema_cols : st->n_schema_cols;
    if (tf_batch_ensure_capacity(out, dst_row + 1) != TF_OK) return TF_ERROR;
    for (size_t c = 0; c < n_cols; c++) {
        if (row->nulls[c]) {
            tf_batch_set_null(out, dst_row, c);
            continue;
        }
        switch (types[c]) {
            case TF_TYPE_BOOL: tf_batch_set_bool(out, dst_row, c, row->cells[c].b != 0); break;
            case TF_TYPE_INT64: tf_batch_set_int64(out, dst_row, c, row->cells[c].i64); break;
            case TF_TYPE_FLOAT64: tf_batch_set_float64(out, dst_row, c, row->cells[c].f64); break;
            case TF_TYPE_STRING: tf_batch_set_string(out, dst_row, c, row->cells[c].str); break;
            case TF_TYPE_DATE: tf_batch_set_date(out, dst_row, c, row->cells[c].date); break;
            case TF_TYPE_TIMESTAMP: tf_batch_set_timestamp(out, dst_row, c, row->cells[c].i64); break;
            default: tf_batch_set_null(out, dst_row, c); break;
        }
    }
    return TF_OK;
}

static void group_agg_close_readers(group_agg_state *st, int output_readers) {
    group_run_reader **readers = output_readers ? &st->out_readers : &st->readers;
    size_t *n_readers = output_readers ? &st->n_out_readers : &st->n_readers;
    if (!*readers) return;
    const tf_type *types = group_agg_reader_types(st, output_readers);
    size_t n_cols = group_agg_reader_cols(st, output_readers);
    for (size_t i = 0; i < *n_readers; i++) {
        if ((*readers)[i].file) fclose((*readers)[i].file);
        spill_row_free(&(*readers)[i].row, types, n_cols);
    }
    free(*readers);
    *readers = NULL;
    *n_readers = 0;
}

static void group_agg_remove_paths(char ***paths, size_t *n, size_t *cap) {
    for (size_t i = 0; i < *n; i++) {
        if ((*paths)[i]) {
            remove((*paths)[i]);
            free((*paths)[i]);
            (*paths)[i] = NULL;
        }
    }
    free(*paths);
    *paths = NULL;
    *n = 0;
    *cap = 0;
}

static int group_agg_open_readers(group_agg_state *st, int output_readers) {
    char **paths = output_readers ? st->out_run_paths : st->run_paths;
    size_t n_paths = output_readers ? st->n_out_runs : st->n_runs;
    group_run_reader **readers = output_readers ? &st->out_readers : &st->readers;
    size_t *n_readers = output_readers ? &st->n_out_readers : &st->n_readers;
    if (n_paths == 0) return TF_OK;
    *readers = calloc(n_paths, sizeof(group_run_reader));
    if (!*readers) return TF_ERROR;
    *n_readers = n_paths;
    size_t n_cols = group_agg_reader_cols(st, output_readers);
    for (size_t i = 0; i < n_paths; i++) {
        (*readers)[i].file = fopen(paths[i], "rb");
        if (!(*readers)[i].file) { tf_set_last_error("group-agg spill: cannot reopen run file"); return TF_ERROR; }
        if (spill_row_init(&(*readers)[i].row, n_cols) != TF_OK) return TF_ERROR;
        int rc = group_agg_reader_advance(st, &(*readers)[i], output_readers);
        if (rc < 0) return TF_ERROR;
    }
    return TF_OK;
}

static int group_agg_best_key_reader(const group_agg_state *st) {
    int best = -1;
    for (size_t i = 0; i < st->n_readers; i++) {
        const group_run_reader *r = &st->readers[i];
        if (!r->has_row || r->done) continue;
        if (best < 0) { best = (int)i; continue; }
        int cmp = compare_spill_group_key_rows(st, &r->row, &st->readers[best].row);
        if (cmp < 0 || (cmp == 0 && i < (size_t)best)) best = (int)i;
    }
    return best;
}

static int group_agg_best_ordinal_reader(const group_agg_state *st) {
    int best = -1;
    for (size_t i = 0; i < st->n_out_readers; i++) {
        const group_run_reader *r = &st->out_readers[i];
        if (!r->has_row || r->done) continue;
        if (best < 0) { best = (int)i; continue; }
        uint64_t a = r->row.ordinal;
        uint64_t b = st->out_readers[best].row.ordinal;
        if (a < b || (a == b && i < (size_t)best)) best = (int)i;
    }
    return best;
}

static int copy_spill_group_key_values(tf_batch *keys, const group_agg_state *st, const group_spill_row *row) {
    if (tf_batch_ensure_capacity(keys, 1) != TF_OK) return TF_ERROR;
    for (size_t k = 0; k < st->n_group_cols; k++) {
        int ci = st->group_indices[k];
        if (ci < 0 || row->nulls[(size_t)ci]) {
            tf_batch_set_null(keys, 0, k);
            continue;
        }
        size_t c = (size_t)ci;
        switch (keys->col_types[k]) {
            case TF_TYPE_BOOL: tf_batch_set_bool(keys, 0, k, row->cells[c].b != 0); break;
            case TF_TYPE_INT64: tf_batch_set_int64(keys, 0, k, row->cells[c].i64); break;
            case TF_TYPE_FLOAT64: tf_batch_set_float64(keys, 0, k, row->cells[c].f64); break;
            case TF_TYPE_STRING: tf_batch_set_string(keys, 0, k, row->cells[c].str ? row->cells[c].str : ""); break;
            case TF_TYPE_DATE: tf_batch_set_date(keys, 0, k, row->cells[c].date); break;
            case TF_TYPE_TIMESTAMP: tf_batch_set_timestamp(keys, 0, k, row->cells[c].i64); break;
            default: tf_batch_set_null(keys, 0, k); break;
        }
    }
    keys->n_rows = 1;
    return TF_OK;
}

static void group_accum_add_spill_row(group_accum *a, const group_agg_state *st, const group_spill_row *row) {
    for (size_t k = 0; k < st->n_aggs; k++) {
        int ci = st->agg_indices[k];
        if (st->aggs[k].func == AGG_COUNT) {
            if (agg_counts_rows(&st->aggs[k]) || (ci >= 0 && !row->nulls[(size_t)ci])) {
                a->counts[k]++;
            }
            continue;
        }
        if (ci < 0 || row->nulls[(size_t)ci]) continue;
        size_t c = (size_t)ci;
        double v = 0;
        if (st->schema_types[c] == TF_TYPE_INT64) v = (double)row->cells[c].i64;
        else if (st->schema_types[c] == TF_TYPE_FLOAT64) v = row->cells[c].f64;
        a->sums[k] += v;
        if (v < a->mins[k]) a->mins[k] = v;
        if (v > a->maxs[k]) a->maxs[k] = v;
        a->counts[k]++;
    }
}

static int group_agg_append_spill_group(group_agg_state *st, const tf_batch *key_batch,
                                        const group_accum *accum, uint64_t first_ordinal) {
    size_t dst = st->out_buf->n_rows;
    if (group_agg_emit_row(st, st->out_buf, dst, key_batch, 0, accum) != 0) return TF_ERROR;
    if (ensure_ordinals(&st->out_ordinals, &st->out_ordinal_cap, dst + 1) != TF_OK) return TF_ERROR;
    st->out_ordinals[dst] = first_ordinal;
    st->spill_distinct_groups++;
    if (st->out_buf->n_rows >= st->run_rows) return group_agg_write_output_run(st);
    return TF_OK;
}

static int group_agg_start_spill_group(group_agg_state *st, const char *key,
                                       const group_spill_row *row,
                                       char **current_key,
                                       tf_batch **current_key_batch,
                                       group_accum *accum,
                                       uint64_t *first_ordinal) {
    *current_key = strdup(key);
    if (!*current_key) return TF_ERROR;
    *current_key_batch = group_agg_create_key_batch(st);
    if (!*current_key_batch) return TF_ERROR;
    if (copy_spill_group_key_values(*current_key_batch, st, row) != TF_OK) return TF_ERROR;
    if (group_accum_init(accum, st->n_aggs) != 0) return TF_ERROR;
    *first_ordinal = row->ordinal;
    return TF_OK;
}

static int group_agg_finish_spill_group(group_agg_state *st, char **current_key,
                                        tf_batch **current_key_batch,
                                        group_accum *accum,
                                        uint64_t first_ordinal) {
    if (!*current_key || !*current_key_batch) return TF_OK;
    st->spill_key_bytes += strlen(*current_key) + 1;
    int rc = group_agg_append_spill_group(st, *current_key_batch, accum, first_ordinal);
    group_accum_free(accum);
    memset(accum, 0, sizeof(*accum));
    free(*current_key);
    *current_key = NULL;
    tf_batch_free(*current_key_batch);
    *current_key_batch = NULL;
    return rc;
}

static int group_agg_produce_output_runs(group_agg_state *st) {
    if (st->key_merge_done) return TF_OK;
    if (!st->has_schema) {
        st->key_merge_done = 1;
        return TF_OK;
    }
    if (st->buf && st->buf->n_rows > 0 && group_agg_write_key_run(st) != TF_OK) return TF_ERROR;
    if (st->buf) { tf_batch_free(st->buf); st->buf = NULL; }
    free(st->buf_ordinals); st->buf_ordinals = NULL; st->buf_ordinal_cap = 0;

    if (group_agg_open_readers(st, 0) != TF_OK) return TF_ERROR;

    char *current_key = NULL;
    tf_batch *current_key_batch = NULL;
    group_accum accum;
    memset(&accum, 0, sizeof(accum));
    int have_group = 0;
    uint64_t first_ordinal = 0;

    for (;;) {
        int best = group_agg_best_key_reader(st);
        if (best < 0) break;
        group_run_reader *reader = &st->readers[best];
        char *key = build_spill_row_key(st, &reader->row);
        if (!key) goto fail;

        if (!have_group) {
            if (group_agg_start_spill_group(st, key, &reader->row, &current_key,
                                            &current_key_batch, &accum, &first_ordinal) != TF_OK) {
                free(key);
                goto fail;
            }
            have_group = 1;
        } else if (strcmp(current_key, key) != 0) {
            if (group_agg_finish_spill_group(st, &current_key, &current_key_batch, &accum, first_ordinal) != TF_OK) {
                free(key);
                goto fail;
            }
            if (group_agg_start_spill_group(st, key, &reader->row, &current_key,
                                            &current_key_batch, &accum, &first_ordinal) != TF_OK) {
                free(key);
                goto fail;
            }
        }
        free(key);

        group_accum_add_spill_row(&accum, st, &reader->row);
        int rc = group_agg_reader_advance(st, reader, 0);
        if (rc < 0) goto fail;
    }

    if (have_group && group_agg_finish_spill_group(st, &current_key, &current_key_batch, &accum, first_ordinal) != TF_OK) goto fail;
    if (st->out_buf && st->out_buf->n_rows > 0 && group_agg_write_output_run(st) != TF_OK) goto fail;
    group_agg_close_readers(st, 0);
    group_agg_remove_paths(&st->run_paths, &st->n_runs, &st->cap_runs);
    st->key_merge_done = 1;
    return TF_OK;

fail:
    if (current_key_batch) tf_batch_free(current_key_batch);
    if (have_group) group_accum_free(&accum);
    free(current_key);
    return TF_ERROR;
}

static int group_agg_begin_output_merge(group_agg_state *st) {
    if (st->output_merge_started) return TF_OK;
    st->output_merge_started = 1;
    if (st->out_buf) { tf_batch_free(st->out_buf); st->out_buf = NULL; }
    free(st->out_ordinals); st->out_ordinals = NULL; st->out_ordinal_cap = 0;
    if (st->n_out_runs == 0) {
        st->output_merge_done = 1;
        return TF_OK;
    }
    return group_agg_open_readers(st, 1);
}

static int group_agg_output_next_batch(group_agg_state *st, tf_batch **out) {
    *out = NULL;
    if (group_agg_produce_output_runs(st) != TF_OK) return TF_ERROR;
    if (group_agg_begin_output_merge(st) != TF_OK) return TF_ERROR;
    if (st->output_merge_done) return TF_OK;

    tf_batch *ob = group_agg_create_output_buffer(st, st->output_batch_rows);
    if (!ob) return TF_ERROR;
    while (ob->n_rows < st->output_batch_rows) {
        int best = group_agg_best_ordinal_reader(st);
        if (best < 0) break;
        group_run_reader *reader = &st->out_readers[best];
        if (spill_row_to_batch(st, ob, ob->n_rows, &reader->row, 1) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
        ob->n_rows++;
        int rc = group_agg_reader_advance(st, reader, 1);
        if (rc < 0) { tf_batch_free(ob); return TF_ERROR; }
    }
    if (ob->n_rows == 0) {
        tf_batch_free(ob);
        group_agg_close_readers(st, 1);
        group_agg_remove_paths(&st->out_run_paths, &st->n_out_runs, &st->cap_out_runs);
        st->output_merge_done = 1;
        return TF_OK;
    }
    st->spill_output_batches++;
    st->spill_output_rows += ob->n_rows;
    *out = ob;
    return TF_OK;
}

static int group_agg_process_spill(group_agg_state *st, tf_batch *in) {
    if (group_agg_init_spill_schema(st, in) != TF_OK) return TF_ERROR;
    for (size_t r = 0; r < in->n_rows; r++) {
        size_t dst = st->buf->n_rows;
        if (tf_batch_copy_row(st->buf, dst, in, r) != TF_OK) return TF_ERROR;
        if (ensure_ordinals(&st->buf_ordinals, &st->buf_ordinal_cap, dst + 1) != TF_OK) return TF_ERROR;
        st->buf_ordinals[dst] = st->next_ordinal++;
        st->buf->n_rows = dst + 1;
        if (st->buf->n_rows >= st->run_rows && group_agg_write_key_run(st) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static int group_agg_start_sorted_group(group_agg_state *st, char *key,
                                        const tf_batch *in, size_t row,
                                        int *group_indices, tf_side_channels *side) {
    if (st->max_groups > 0 && st->sorted_groups_started >= st->max_groups) {
        group_agg_limit_error(st, side);
        free(key);
        return -1;
    }
    if (ensure_sorted_key_batch(st, in, group_indices) != 0 ||
        tf_batch_ensure_capacity(st->sorted_key_batch, 1) != TF_OK) {
        free(key);
        return -1;
    }

    if (st->sorted_have_current) {
        group_accum_free(&st->sorted_accum);
        memset(&st->sorted_accum, 0, sizeof(st->sorted_accum));
        free(st->sorted_key);
        st->sorted_key = NULL;
        st->sorted_have_current = 0;
    }
    if (group_accum_init(&st->sorted_accum, st->n_aggs) != 0) {
        free(key);
        return -1;
    }

    st->sorted_key = key;
    st->sorted_have_current = 1;
    st->sorted_groups_started++;
    copy_group_key_values(st->sorted_key_batch, 0, in, row, group_indices, st->n_group_cols);
    st->sorted_key_batch->n_rows = 1;
    if (group_agg_check_state_bytes(st, side) != 0) return -1;
    return 0;
}

static int group_agg_process_sorted(tf_step *self, tf_batch *in, tf_batch **out,
                                    tf_side_channels *side) {
    group_agg_state *st = self->state;
    *out = NULL;

    int *group_indices = malloc(st->n_group_cols * sizeof(int));
    int *agg_indices = malloc(st->n_aggs * sizeof(int));
    if (!group_indices || !agg_indices) {
        free(group_indices);
        free(agg_indices);
        return TF_ERROR;
    }

    for (size_t k = 0; k < st->n_group_cols; k++)
        group_indices[k] = tf_batch_col_index(in, st->group_cols[k]);
    for (size_t k = 0; k < st->n_aggs; k++)
        agg_indices[k] = tf_batch_col_index(in, st->aggs[k].column);

    tf_batch *ob = NULL;
    size_t out_rows = 0;
    size_t n_out_cols = st->n_group_cols + st->n_aggs;

    for (size_t r = 0; r < in->n_rows; r++) {
        char *key = build_group_key(in, r, group_indices, st->n_group_cols);
        if (!key) goto fail;

        if (!st->sorted_have_current) {
            if (group_agg_start_sorted_group(st, key, in, r, group_indices, side) != 0) goto fail;
            key = NULL;
        } else if (strcmp(st->sorted_key, key) != 0) {
            if (!ob) {
                size_t cap = in->n_rows > 0 ? in->n_rows : 1;
                ob = tf_batch_create(n_out_cols, cap);
                if (!ob) { free(key); goto fail; }
                if (group_agg_set_output_schema(st, ob, st->sorted_key_batch) != 0) { free(key); goto fail; }
            }
            if (group_agg_emit_row(st, ob, out_rows++, st->sorted_key_batch, 0, &st->sorted_accum) != 0) {
                free(key);
                goto fail;
            }
            if (group_agg_start_sorted_group(st, key, in, r, group_indices, side) != 0) goto fail;
            key = NULL;
        } else {
            free(key);
            key = NULL;
        }

        group_accum_add_row(&st->sorted_accum, st, in, r, agg_indices);
    }

    free(group_indices);
    free(agg_indices);
    if (ob && out_rows > 0) *out = ob;
    else if (ob) tf_batch_free(ob);
    return TF_OK;

fail:
    free(group_indices);
    free(agg_indices);
    if (ob) tf_batch_free(ob);
    return TF_ERROR;
}

static int group_agg_flush_sorted(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    group_agg_state *st = self->state;
    *out = NULL;

    if (!st->sorted_have_current) return TF_OK;

    size_t n_out_cols = st->n_group_cols + st->n_aggs;
    tf_batch *ob = tf_batch_create(n_out_cols, 1);
    if (!ob) return TF_ERROR;
    if (group_agg_set_output_schema(st, ob, st->sorted_key_batch) != 0 ||
        group_agg_emit_row(st, ob, 0, st->sorted_key_batch, 0, &st->sorted_accum) != 0) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    group_accum_free(&st->sorted_accum);
    memset(&st->sorted_accum, 0, sizeof(st->sorted_accum));
    free(st->sorted_key);
    st->sorted_key = NULL;
    st->sorted_have_current = 0;
    if (st->sorted_key_batch) st->sorted_key_batch->n_rows = 0;

    *out = ob;
    return TF_OK;
}

static int find_or_add_group(group_agg_state *st, const char *key,
                             const tf_batch *in, size_t row,
                             int *group_indices, tf_side_channels *side) {
    group_map *map = &st->map;
    int found = 0;
    size_t slot = group_map_find_slot(map, key, &found);
    if (found) return (int)(map->slots[slot] - 1);

    if (st->max_groups > 0 && map->count >= st->max_groups) {
        group_agg_limit_error(st, side);
        return -1;
    }

    if (map->count * 4 >= map->slot_cap * 3) {
        if (group_map_rehash(map, map->slot_cap * 2) != 0) return -1;
        slot = group_map_find_slot(map, key, &found);
    }
    if (group_map_ensure_group_capacity(map, map->count + 1) != 0) return -1;
    if (ensure_key_batch(st, in, group_indices) != 0) return -1;
    if (tf_batch_ensure_capacity(map->key_batch, map->count + 1) != TF_OK) return -1;

    char *dup = strdup(key);
    if (!dup) return -1;
    group_accum accum;
    if (group_accum_init(&accum, st->n_aggs) != 0) { free(dup); return -1; }

    size_t idx = map->count;
    map->keys[idx] = dup;
    map->key_bytes += strlen(dup) + 1;
    map->accums[idx] = accum;
    copy_group_key_values(map->key_batch, idx, in, row, group_indices, st->n_group_cols);
    map->key_batch->n_rows = idx + 1;
    map->slots[slot] = idx + 1;
    map->count++;
    if (group_agg_check_state_bytes(st, side) != 0) return -1;
    return (int)idx;
}

static int group_agg_process(tf_step *self, tf_batch *in, tf_batch **out,
                             tf_side_channels *side) {
    group_agg_state *st = self->state;
    if (st->sorted) return group_agg_process_sorted(self, in, out, side);
    *out = NULL;
    if (st->use_spill) return group_agg_process_spill(st, in);

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
        int gi = find_or_add_group(st, key, in, r, group_indices, side);
        free(key);
        if (gi < 0) { free(group_indices); free(agg_indices); return TF_ERROR; }
        group_accum_add_row(&st->map.accums[gi], st, in, r, agg_indices);
    }

    free(group_indices);
    free(agg_indices);
    return TF_OK;
}

static int group_agg_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    group_agg_state *st = self->state;
    if (st->sorted) return group_agg_flush_sorted(self, out, side);
    (void)side;
    *out = NULL;
    if (st->use_spill) return group_agg_output_next_batch(st, out);

    if (st->map.count == 0) return TF_OK;

    size_t n_out_cols = st->n_group_cols + st->n_aggs;
    tf_batch *ob = tf_batch_create(n_out_cols, st->map.count);
    if (!ob) return TF_ERROR;

    for (size_t k = 0; k < st->n_group_cols; k++) {
        tf_type type = st->map.key_batch ? st->map.key_batch->col_types[k] : TF_TYPE_STRING;
        if (tf_batch_set_schema(ob, k, st->group_cols[k], type) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
    }
    for (size_t k = 0; k < st->n_aggs; k++) {
        if (tf_batch_set_schema(ob, st->n_group_cols + k, st->aggs[k].name, TF_TYPE_FLOAT64) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    for (size_t g = 0; g < st->map.count; g++) {
        tf_batch_ensure_capacity(ob, g + 1);

        if (st->map.key_batch && tf_batch_copy_row(ob, g, st->map.key_batch, g) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }

        group_accum *a = &st->map.accums[g];
        for (size_t k = 0; k < st->n_aggs; k++) {
            group_agg_set_aggregate_cell(st, ob, g, k, a);
        }
        ob->n_rows = g + 1;
    }

    *out = ob;
    return TF_OK;
}

static int group_agg_flush_next(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    group_agg_state *st = self->state;
    *out = NULL;
    if (!st->use_spill) return TF_OK;
    return group_agg_output_next_batch(st, out);
}

static size_t group_agg_accum_bytes(size_t n_groups, size_t n_aggs) {
    return n_groups * n_aggs * (3 * sizeof(double) + sizeof(size_t));
}

static void group_agg_add_bytes(size_t *acc, size_t value) {
    if (*acc > SIZE_MAX - value) *acc = SIZE_MAX;
    else *acc += value;
}

static void group_agg_add_mul_bytes(size_t *acc, size_t a, size_t b) {
    if (a != 0 && b > SIZE_MAX / a) *acc = SIZE_MAX;
    else group_agg_add_bytes(acc, a * b);
}

static size_t group_agg_type_size(tf_type t) {
    switch (t) {
        case TF_TYPE_BOOL:      return sizeof(uint8_t);
        case TF_TYPE_INT64:     return sizeof(int64_t);
        case TF_TYPE_FLOAT64:   return sizeof(double);
        case TF_TYPE_STRING:    return sizeof(char *);
        case TF_TYPE_DATE:      return sizeof(int32_t);
        case TF_TYPE_TIMESTAMP: return sizeof(int64_t);
        default:                return 0;
    }
}

static size_t group_agg_batch_retained_bytes(const tf_batch *b) {
    if (!b) return 0;
    size_t bytes = sizeof(tf_batch);
    group_agg_add_mul_bytes(&bytes, b->n_cols, sizeof(char *));
    group_agg_add_mul_bytes(&bytes, b->n_cols, sizeof(tf_type));
    group_agg_add_mul_bytes(&bytes, b->n_cols, sizeof(void *));
    group_agg_add_mul_bytes(&bytes, b->n_cols, sizeof(uint8_t *));
    for (size_t c = 0; c < b->n_cols; c++) {
        if (b->col_names && b->col_names[c]) group_agg_add_bytes(&bytes, strlen(b->col_names[c]) + 1);
        size_t sz = group_agg_type_size(b->col_types[c]);
        group_agg_add_mul_bytes(&bytes, sz, b->capacity);
        group_agg_add_bytes(&bytes, b->capacity);
        if (b->col_types[c] == TF_TYPE_STRING && b->columns && b->columns[c]) {
            char **vals = (char **)b->columns[c];
            for (size_t r = 0; r < b->n_rows; r++) {
                if (b->nulls && b->nulls[c] && b->nulls[c][r]) continue;
                if (vals[r]) group_agg_add_bytes(&bytes, strlen(vals[r]) + 1);
            }
        }
    }
    return bytes;
}

static size_t group_agg_sorted_key_bytes(const group_agg_state *st) {
    return (st && st->sorted_have_current && st->sorted_key) ? strlen(st->sorted_key) + 1 : 0;
}

static size_t group_agg_retained_state_bytes(const group_agg_state *st) {
    if (!st) return 0;
    if (st->use_spill) {
        size_t bytes = 0;
        bytes += group_agg_batch_retained_bytes(st->buf);
        bytes += group_agg_batch_retained_bytes(st->out_buf);
        group_agg_add_mul_bytes(&bytes, st->buf_ordinal_cap, sizeof(uint64_t));
        group_agg_add_mul_bytes(&bytes, st->out_ordinal_cap, sizeof(uint64_t));
        group_agg_add_mul_bytes(&bytes, st->n_readers + st->n_out_readers, sizeof(group_run_reader));
        return bytes;
    }
    if (st->sorted) {
        size_t current_key = group_agg_sorted_key_bytes(st);
        if (!st->sorted_have_current) return 0;
        return current_key + sizeof(group_accum) +
               group_agg_accum_bytes(1, st->n_aggs) +
               group_agg_batch_retained_bytes(st->sorted_key_batch);
    }
    return st->map.key_bytes +
           st->map.cap * (sizeof(char *) + sizeof(group_accum)) +
           st->map.slot_cap * sizeof(size_t) +
           group_agg_accum_bytes(st->map.count, st->n_aggs) +
           group_agg_batch_retained_bytes(st->map.key_batch);
}

static int group_agg_append_stats(tf_step *self, tf_buffer *out) {
    if (!self || !self->state || !out) return TF_ERROR;
    group_agg_state *st = self->state;
    size_t tracked_groups = st->use_spill ? st->spill_distinct_groups :
                            (st->sorted ? (st->sorted_have_current ? 1u : 0u) : st->map.count);
    size_t tracked_key_bytes = st->use_spill ? st->spill_key_bytes :
                               (st->sorted ? group_agg_sorted_key_bytes(st) : st->map.key_bytes);
    size_t retained = group_agg_retained_state_bytes(st);
    char buf[420];
    snprintf(buf, sizeof(buf),
             ",\"tracked_groups\":%zu,\"tracked_key_bytes\":%zu,"
             "\"retained_state_bytes\":%zu,\"max_state_bytes\":%zu",
             tracked_groups, tracked_key_bytes, retained, st->max_state_bytes);
    if (tf_buffer_write_str(out, buf) != TF_OK) return TF_ERROR;
    if (st->use_spill) {
        snprintf(buf, sizeof(buf),
                 ",\"spill_bytes\":%zu,\"spill_runs\":%zu,"
                 "\"spill_output_batches\":%zu,\"spill_output_rows\":%zu,"
                 "\"spill_distinct_groups\":%zu",
                 st->spilled_bytes, st->spill_runs_created,
                 st->spill_output_batches, st->spill_output_rows,
                 st->spill_distinct_groups);
        return tf_buffer_write_str(out, buf);
    }
    return TF_OK;
}

static void group_agg_state_free(group_agg_state *st) {
    if (st) {
        for (size_t i = 0; i < st->n_group_cols; i++) free(st->group_cols[i]);
        free(st->group_cols);
        for (size_t i = 0; i < st->n_aggs; i++) { free(st->aggs[i].column); free(st->aggs[i].name); }
        free(st->aggs);
        if (st->sorted_have_current) group_accum_free(&st->sorted_accum);
        free(st->sorted_key);
        if (st->sorted_key_batch) tf_batch_free(st->sorted_key_batch);
        if (st->buf) tf_batch_free(st->buf);
        if (st->out_buf) tf_batch_free(st->out_buf);
        free(st->buf_ordinals);
        free(st->out_ordinals);
        if (st->has_schema) {
            group_agg_close_readers(st, 0);
            group_agg_close_readers(st, 1);
        }
        group_agg_remove_paths(&st->run_paths, &st->n_runs, &st->cap_runs);
        group_agg_remove_paths(&st->out_run_paths, &st->n_out_runs, &st->cap_out_runs);
        for (size_t i = 0; i < st->n_schema_cols; i++) free(st->schema_names ? st->schema_names[i] : NULL);
        free(st->schema_names);
        free(st->schema_types);
        free(st->group_indices);
        free(st->agg_indices);
        for (size_t i = 0; i < st->n_out_schema_cols; i++) free(st->out_schema_names ? st->out_schema_names[i] : NULL);
        free(st->out_schema_names);
        free(st->out_schema_types);
        free(st->spill_dir);
        group_map_free(&st->map);
        free(st);
    }
}

static void group_agg_destroy(tf_step *self) {
    if (self) group_agg_state_free(self->state);
    free(self);
}

tf_step *tf_group_agg_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *group_by = cJSON_GetObjectItemCaseSensitive(args, "group_by");
    cJSON *aggs_j = cJSON_GetObjectItemCaseSensitive(args, "aggs");
    if (!cJSON_IsArray(group_by) || !cJSON_IsArray(aggs_j)) return NULL;

    group_agg_state *st = calloc(1, sizeof(group_agg_state));
    if (!st) return NULL;
    st->output_batch_rows = GROUP_AGG_DEFAULT_OUTPUT_ROWS;

    cJSON *sorted = cJSON_GetObjectItemCaseSensitive(args, "sorted");
    if (cJSON_IsBool(sorted) && cJSON_IsTrue(sorted)) st->sorted = 1;

    cJSON *spill_dir_j = cJSON_GetObjectItemCaseSensitive(args, "spill_dir");
    if (cJSON_IsString(spill_dir_j) && spill_dir_j->valuestring && spill_dir_j->valuestring[0]) {
        st->use_spill = 1;
        st->spill_dir = strdup(spill_dir_j->valuestring);
        if (!st->spill_dir) { group_agg_state_free(st); return NULL; }
        st->spill_memory_bytes = json_size_arg(args, "spill_memory_bytes");
        st->configured_run_rows = json_size_arg(args, "spill_run_rows");
        st->output_batch_rows = json_size_arg(args, "spill_output_rows");
        if (st->output_batch_rows == 0 || st->output_batch_rows == SIZE_MAX) st->output_batch_rows = GROUP_AGG_DEFAULT_OUTPUT_ROWS;
        if (st->configured_run_rows == SIZE_MAX) st->configured_run_rows = 0;
        if (st->spill_memory_bytes == SIZE_MAX) st->spill_memory_bytes = 0;
    }

    if (st->use_spill && st->sorted) {
        tf_set_last_error("group-agg: spill_dir and sorted=true are mutually exclusive");
        group_agg_state_free(st);
        return NULL;
    }
    if (!st->sorted && !st->use_spill && group_map_init(&st->map) != 0) { group_agg_state_free(st); return NULL; }

    cJSON *max_groups = cJSON_GetObjectItemCaseSensitive(args, "max_groups");
    if (cJSON_IsNumber(max_groups) && max_groups->valuedouble > 0) {
        st->max_groups = (size_t)max_groups->valuedouble;
    }

    cJSON *max_state_bytes = cJSON_GetObjectItemCaseSensitive(args, "max_state_bytes");
    if (max_state_bytes) {
        if (!cJSON_IsNumber(max_state_bytes) || max_state_bytes->valuedouble <= 0) {
            tf_set_last_error("group-agg: max_state_bytes must be positive");
            group_agg_state_free(st);
            return NULL;
        }
        st->max_state_bytes = (size_t)max_state_bytes->valuedouble;
    }

    int ng = cJSON_GetArraySize(group_by);
    st->group_cols = calloc(ng, sizeof(char *));
    st->n_group_cols = ng;
    if (ng > 0 && !st->group_cols) { group_agg_state_free(st); return NULL; }
    for (int i = 0; i < ng; i++) {
        cJSON *item = cJSON_GetArrayItem(group_by, i);
        if (cJSON_IsString(item)) {
            st->group_cols[i] = strdup(item->valuestring);
            if (!st->group_cols[i]) { group_agg_state_free(st); return NULL; }
        }
    }

    int na = cJSON_GetArraySize(aggs_j);
    st->aggs = calloc(na, sizeof(agg_spec));
    st->n_aggs = na;
    if (na > 0 && !st->aggs) { group_agg_state_free(st); return NULL; }
    for (int i = 0; i < na; i++) {
        cJSON *item = cJSON_GetArrayItem(aggs_j, i);
        cJSON *col = cJSON_GetObjectItemCaseSensitive(item, "column");
        cJSON *func = cJSON_GetObjectItemCaseSensitive(item, "func");
        cJSON *name = cJSON_GetObjectItemCaseSensitive(item, "name");
        st->aggs[i].column = strdup(cJSON_IsString(col) ? col->valuestring : "");
        if (!st->aggs[i].column) { group_agg_state_free(st); return NULL; }
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
        if (!st->aggs[i].name) { group_agg_state_free(st); return NULL; }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { group_agg_state_free(st); return NULL; }
    step->process = group_agg_process;
    step->flush = group_agg_flush;
    step->flush_next = st->use_spill ? group_agg_flush_next : NULL;
    step->append_stats = group_agg_append_stats;
    step->destroy = group_agg_destroy;
    step->state = st;
    return step;
}
