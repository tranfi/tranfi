/*
 * op_pivot.c - Pivot (long to wide).
 *
 * Default mode buffers input because output columns are data-dependent and
 * groups can reappear anywhere. Guarded sorted mode requires declared
 * categories and consecutive pass-through keys, then emits completed groups
 * with only current-group state.
 */

#include "internal.h"
#include "spill.h"
#include "date_utils.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <float.h>
#include <errno.h>
#include <stdint.h>
#include <unistd.h>

#define TF_OK 0
#define TF_ERROR (-1)

#define PIVOT_DEFAULT_RUN_ROWS 8192
#define PIVOT_DEFAULT_OUTPUT_ROWS 1024
#define PIVOT_MIN_RUN_ROWS 16

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
    size_t  n;
} pivot_accum;

typedef struct {
    char  **keys;
    size_t *pass_rows;
    pivot_accum *accums;
    size_t  count;
    size_t  cap;
} pivot_map;

typedef struct {
    uint64_t ordinal;
    uint8_t *nulls;
    tf_owned_cell_value *cells;
} pivot_spill_row;

typedef struct {
    FILE *file;
    pivot_spill_row row;
    int has_row;
    int done;
} pivot_run_reader;

typedef struct {
    char      *name_column;
    char      *value_column;
    pivot_agg  agg;
    int        sorted;
    size_t     max_categories;
    int        categories_declared;
    int        use_spill;

    char      *spill_dir;
    tf_spill_session *spill;
    size_t     spill_memory_bytes;
    size_t     configured_run_rows;
    size_t     run_rows;
    size_t     output_batch_rows;

    tf_batch  *buf;
    int        has_schema;
    char     **schema_names;
    tf_type   *schema_types;
    size_t     n_schema_cols;
    uint64_t  *buf_ordinals;
    size_t     buf_ordinal_cap;
    uint64_t   next_ordinal;

    char     **unique_names;
    size_t     n_names;
    size_t     names_cap;

    int       *pt_cols;
    size_t     n_pt;
    int        name_ci;
    int        val_ci;
    tf_batch  *current_pt;
    char      *current_key;
    pivot_accum current_accum;
    int        have_current;

    tf_batch  *out_buf;
    uint64_t  *out_ordinals;
    size_t     out_ordinal_cap;
    char     **run_paths;
    size_t     n_runs;
    size_t     cap_runs;
    char     **out_run_paths;
    size_t     n_out_runs;
    size_t     cap_out_runs;
    size_t     run_seq;
    size_t     out_run_seq;
    pivot_run_reader *readers;
    size_t     n_readers;
    pivot_run_reader *out_readers;
    size_t     n_out_readers;
    int        key_merge_done;
    int        output_merge_started;
    int        output_merge_done;

    size_t     spilled_bytes;
    size_t     spill_runs_created;
    size_t     spill_output_batches;
    size_t     spill_output_rows;
    size_t     spill_distinct_groups;
    size_t     spill_key_bytes;
} pivot_state;

static int pivot_resolve_name(pivot_state *st, const char *name,
                              tf_side_channels *side);

static int pivot_write_error(tf_side_channels *side, const char *msg) {
    return tf_side_write_error(side, msg);
}

static pivot_agg parse_pivot_agg(const char *s) {
    if (!s) return PIVOT_FIRST;
    if (strcmp(s, "first") == 0) return PIVOT_FIRST;
    if (strcmp(s, "sum") == 0) return PIVOT_SUM;
    if (strcmp(s, "count") == 0) return PIVOT_COUNT;
    if (strcmp(s, "avg") == 0 || strcmp(s, "mean") == 0) return PIVOT_AVG;
    if (strcmp(s, "min") == 0) return PIVOT_MIN;
    if (strcmp(s, "max") == 0) return PIVOT_MAX;
    return PIVOT_FIRST;
}

static int pivot_accum_init(pivot_accum *a, size_t n) {
    memset(a, 0, sizeof(*a));
    if (n == 0) return TF_OK;

    double *sums = tf_callocarray_checked(n, sizeof(double));
    double *mins = tf_mallocarray_checked(n, sizeof(double));
    double *maxs = tf_mallocarray_checked(n, sizeof(double));
    size_t *counts = tf_callocarray_checked(n, sizeof(size_t));
    int *has_first = tf_callocarray_checked(n, sizeof(int));
    double *firsts = tf_callocarray_checked(n, sizeof(double));
    if (!sums || !mins || !maxs || !counts || !has_first || !firsts) {
        free(sums);
        free(mins);
        free(maxs);
        free(counts);
        free(has_first);
        free(firsts);
        return TF_ERROR;
    }

    a->sums = sums;
    a->mins = mins;
    a->maxs = maxs;
    a->counts = counts;
    a->has_first = has_first;
    a->firsts = firsts;
    a->n = n;
    for (size_t i = 0; i < n; i++) {
        a->mins[i] = DBL_MAX;
        a->maxs[i] = -DBL_MAX;
    }
    return TF_OK;
}

static void pivot_accum_reset(pivot_accum *a) {
    if (!a || a->n == 0) return;
    memset(a->sums, 0, a->n * sizeof(double));
    memset(a->counts, 0, a->n * sizeof(size_t));
    memset(a->has_first, 0, a->n * sizeof(int));
    memset(a->firsts, 0, a->n * sizeof(double));
    for (size_t i = 0; i < a->n; i++) {
        a->mins[i] = DBL_MAX;
        a->maxs[i] = -DBL_MAX;
    }
}

static void pivot_accum_free(pivot_accum *a) {
    if (!a) return;
    free(a->sums);
    free(a->mins);
    free(a->maxs);
    free(a->counts);
    free(a->has_first);
    free(a->firsts);
    memset(a, 0, sizeof(*a));
}

static void pivot_accum_add(pivot_accum *a, size_t idx, double v) {
    if (!a || idx >= a->n) return;
    a->sums[idx] += v;
    if (v < a->mins[idx]) a->mins[idx] = v;
    if (v > a->maxs[idx]) a->maxs[idx] = v;
    a->counts[idx]++;
    if (!a->has_first[idx]) {
        a->firsts[idx] = v;
        a->has_first[idx] = 1;
    }
}

static int find_unique_name(const pivot_state *st, const char *name) {
    for (size_t i = 0; i < st->n_names; i++) {
        if (strcmp(st->unique_names[i], name) == 0) return (int)i;
    }
    return -1;
}

static int add_unique_name(pivot_state *st, const char *name, tf_side_channels *side) {
    int idx = find_unique_name(st, name);
    if (idx >= 0) return idx;
    if (st->max_categories > 0 && st->n_names >= st->max_categories) {
        char msg[256];
        snprintf(msg, sizeof(msg), "pivot: max_categories=%zu exceeded while tracking category '%s'",
                 st->max_categories, name ? name : "");
        if (pivot_write_error(side, msg) != TF_OK) return -1;
        return -1;
    }
    if (st->n_names >= st->names_cap) {
        size_t need = 0;
        size_t new_cap = 0;
        if (tf_size_add(st->n_names, 1, &need) != TF_OK ||
            tf_size_grow_pow2(st->names_cap, need, 32, &new_cap) != TF_OK) {
            return -1;
        }
        char **tmp = tf_reallocarray_checked(st->unique_names, new_cap, sizeof(char *));
        if (!tmp) return -1;
        st->unique_names = tmp;
        st->names_cap = new_cap;
    }
    st->unique_names[st->n_names] = strdup(name ? name : "");
    if (!st->unique_names[st->n_names]) return -1;
    return (int)st->n_names++;
}

static int unknown_declared_category(pivot_state *st, const char *name, tf_side_channels *side) {
    char msg[256];
    snprintf(msg, sizeof(msg), "pivot: unknown category '%s' for column '%s'",
             name ? name : "", st->name_column ? st->name_column : "");
    if (pivot_write_error(side, msg) != TF_OK) return TF_ERROR;
    return TF_ERROR;
}

static char *build_pivot_key(const tf_batch *b, size_t row,
                             const int *pt_cols, size_t n_pt) {
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
        if (tf_batch_is_null(b, row, (size_t)c)) {
            val = "\\N";
            val_len = 2;
        } else {
            switch (b->col_types[c]) {
                case TF_TYPE_STRING:
                    val = tf_batch_get_string(b, row, (size_t)c);
                    val_len = strlen(val);
                    break;
                case TF_TYPE_INT64:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%lld",
                                       (long long)tf_batch_get_int64(b, row, (size_t)c));
                    val = val_buf;
                    break;
                case TF_TYPE_FLOAT64:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%.17g", tf_batch_get_float64(b, row, (size_t)c));
                    val = val_buf;
                    break;
                case TF_TYPE_BOOL:
                    val = tf_batch_get_bool(b, row, (size_t)c) ? "T" : "F";
                    val_len = 1;
                    break;
                case TF_TYPE_DATE:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%d", (int)tf_batch_get_date(b, row, (size_t)c));
                    val = val_buf;
                    break;
                case TF_TYPE_TIMESTAMP:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%lld",
                                       (long long)tf_batch_get_timestamp(b, row, (size_t)c));
                    val = val_buf;
                    break;
                default:
                    val = "\\N";
                    val_len = 2;
                    break;
            }
        }
        size_t need = 0;
        if (tf_size_add(buf_len, val_len, &need) != TF_OK ||
            tf_size_add(need, 2, &need) != TF_OK) {
            free(buf);
            return NULL;
        }
        if (need >= buf_cap) {
            size_t new_cap = 0;
            size_t min_cap = 0;
            if (tf_size_add(need, 1, &min_cap) != TF_OK ||
                tf_size_grow_pow2(buf_cap, min_cap, 256, &new_cap) != TF_OK) {
                free(buf);
                return NULL;
            }
            char *tmp = tf_reallocarray_checked(buf, new_cap, sizeof(char));
            if (!tmp) { free(buf); return NULL; }
            buf = tmp;
            buf_cap = new_cap;
        }
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
        size_t need = 0;
        size_t new_cap = 0;
        if (tf_size_add(map->count, 1, &need) != TF_OK ||
            tf_size_grow_pow2(map->cap, need, 64, &new_cap) != TF_OK) {
            return -1;
        }
        size_t keys_bytes = 0;
        size_t pass_rows_bytes = 0;
        size_t accums_bytes = 0;
        if (map->count > 0 &&
            (tf_size_mul(map->count, sizeof(char *), &keys_bytes) != TF_OK ||
             tf_size_mul(map->count, sizeof(size_t), &pass_rows_bytes) != TF_OK ||
             tf_size_mul(map->count, sizeof(pivot_accum), &accums_bytes) != TF_OK)) {
            return -1;
        }
        char **keys = tf_mallocarray_checked(new_cap, sizeof(char *));
        size_t *pass_rows = tf_mallocarray_checked(new_cap, sizeof(size_t));
        pivot_accum *accums = tf_mallocarray_checked(new_cap, sizeof(pivot_accum));
        if (!keys || !pass_rows || !accums) {
            free(keys);
            free(pass_rows);
            free(accums);
            return -1;
        }
        if (map->count > 0) {
            memcpy(keys, map->keys, keys_bytes);
            memcpy(pass_rows, map->pass_rows, pass_rows_bytes);
            memcpy(accums, map->accums, accums_bytes);
        }
        free(map->keys);
        free(map->pass_rows);
        free(map->accums);
        map->keys = keys;
        map->pass_rows = pass_rows;
        map->accums = accums;
        map->cap = new_cap;
    }
    size_t idx = map->count;
    map->keys[idx] = strdup(key);
    if (!map->keys[idx]) return -1;
    map->pass_rows[idx] = src_row;
    if (pivot_accum_init(&map->accums[idx], n_names) != TF_OK) {
        free(map->keys[idx]);
        map->keys[idx] = NULL;
        return -1;
    }
    map->count = idx + 1;
    return (int)idx;
}

static void pivot_map_free(pivot_map *map) {
    if (!map) return;
    for (size_t i = 0; i < map->count; i++) {
        free(map->keys[i]);
        pivot_accum_free(&map->accums[i]);
    }
    free(map->keys);
    free(map->pass_rows);
    free(map->accums);
    memset(map, 0, sizeof(*map));
}

static double get_numeric_value(const tf_batch *b, size_t row, int col) {
    if (tf_batch_is_null(b, row, (size_t)col)) return 0.0;
    switch (b->col_types[col]) {
        case TF_TYPE_INT64:     return (double)tf_batch_get_int64(b, row, (size_t)col);
        case TF_TYPE_FLOAT64:   return tf_batch_get_float64(b, row, (size_t)col);
        case TF_TYPE_DATE:      return (double)tf_batch_get_date(b, row, (size_t)col);
        case TF_TYPE_TIMESTAMP: return (double)tf_batch_get_timestamp(b, row, (size_t)col);
        case TF_TYPE_BOOL:      return tf_batch_get_bool(b, row, (size_t)col) ? 1.0 : 0.0;
        default: return 0.0;
    }
}

static const char *get_name_str(const tf_batch *b, size_t row, int col, char *buf, size_t buf_sz) {
    if (tf_batch_is_null(b, row, (size_t)col)) return NULL;
    switch (b->col_types[col]) {
        case TF_TYPE_STRING: return tf_batch_get_string(b, row, (size_t)col);
        case TF_TYPE_INT64:
            snprintf(buf, buf_sz, "%lld", (long long)tf_batch_get_int64(b, row, (size_t)col));
            return buf;
        case TF_TYPE_FLOAT64:
            snprintf(buf, buf_sz, "%.17g", tf_batch_get_float64(b, row, (size_t)col));
            return buf;
        case TF_TYPE_BOOL:
            return tf_batch_get_bool(b, row, (size_t)col) ? "true" : "false";
        case TF_TYPE_DATE:
            tf_date_format(tf_batch_get_date(b, row, (size_t)col), buf, buf_sz);
            return buf;
        case TF_TYPE_TIMESTAMP:
            tf_timestamp_format(tf_batch_get_timestamp(b, row, (size_t)col), buf, buf_sz);
            return buf;
        default: return NULL;
    }
}

static int pivot_set_output_schema_from_source(const pivot_state *st, tf_batch *ob,
                                               const tf_batch *src,
                                               const int *pt_cols, size_t n_pt) {
    for (size_t k = 0; k < n_pt; k++) {
        int sc = pt_cols ? pt_cols[k] : (int)k;
        if (tf_batch_set_schema(ob, k, src->col_names[sc], src->col_types[sc]) != TF_OK)
            return TF_ERROR;
    }
    tf_type pivot_type = st->agg == PIVOT_COUNT ? TF_TYPE_INT64 : TF_TYPE_FLOAT64;
    for (size_t k = 0; k < st->n_names; k++) {
        if (tf_batch_set_schema(ob, n_pt + k, st->unique_names[k], pivot_type) != TF_OK)
            return TF_ERROR;
    }
    return TF_OK;
}

static int pivot_emit_row(const pivot_state *st, tf_batch *ob, size_t out_row,
                          const tf_batch *pt_src, size_t pt_row,
                          const int *pt_cols, size_t n_pt,
                          const pivot_accum *a) {
    if (tf_batch_ensure_capacity(ob, out_row + 1) != TF_OK) return TF_ERROR;
    for (size_t k = 0; k < n_pt; k++) {
        int sc = pt_cols ? pt_cols[k] : (int)k;
        if (tf_batch_copy_cell_index(ob, out_row, k, pt_src, pt_row, sc) != TF_OK) return TF_ERROR;
    }
    for (size_t k = 0; k < st->n_names; k++) {
        size_t oc = n_pt + k;
        if (a->counts[k] == 0) {
            if (tf_batch_set_null(ob, out_row, oc) != TF_OK) return TF_ERROR;
            continue;
        }
        double v = 0.0;
        switch (st->agg) {
            case PIVOT_FIRST: v = a->firsts[k]; break;
            case PIVOT_SUM:   v = a->sums[k]; break;
            case PIVOT_COUNT:
                if (tf_batch_set_int64(ob, out_row, oc, (int64_t)a->counts[k]) != TF_OK) return TF_ERROR;
                continue;
            case PIVOT_AVG:   v = a->sums[k] / (double)a->counts[k]; break;
            case PIVOT_MIN:   v = a->mins[k]; break;
            case PIVOT_MAX:   v = a->maxs[k]; break;
        }
        if (tf_batch_set_float64(ob, out_row, oc, v) != TF_OK) return TF_ERROR;
    }
    if (tf_batch_expose_row(ob, out_row) != TF_OK) return TF_ERROR;
    return TF_OK;
}


static int pivot_set_output_schema_from_arrays(const pivot_state *st, tf_batch *ob) {
    for (size_t k = 0; k < st->n_pt; k++) {
        int sc = st->pt_cols ? st->pt_cols[k] : (int)k;
        if (tf_batch_set_schema(ob, k, st->schema_names[sc], st->schema_types[sc]) != TF_OK)
            return TF_ERROR;
    }
    tf_type pivot_type = st->agg == PIVOT_COUNT ? TF_TYPE_INT64 : TF_TYPE_FLOAT64;
    for (size_t k = 0; k < st->n_names; k++) {
        if (tf_batch_set_schema(ob, st->n_pt + k, st->unique_names[k], pivot_type) != TF_OK)
            return TF_ERROR;
    }
    return TF_OK;
}

static int pivot_key_append(char **buf, size_t *buf_cap, size_t *buf_len,
                            const char *data, size_t data_len) {
    size_t need = 0;
    size_t min_cap = 0;
    if (tf_size_add(*buf_len, data_len, &need) != TF_OK ||
        tf_size_add(need, 1, &min_cap) != TF_OK) {
        return TF_ERROR;
    }
    if (min_cap >= *buf_cap) {
        size_t new_cap = 0;
        if (tf_size_grow_pow2(*buf_cap, min_cap, 256, &new_cap) != TF_OK) return TF_ERROR;
        char *tmp = tf_reallocarray_checked(*buf, new_cap, sizeof(char));
        if (!tmp) return TF_ERROR;
        *buf = tmp;
        *buf_cap = new_cap;
    }
    memcpy(*buf + *buf_len, data, data_len);
    *buf_len += data_len;
    (*buf)[*buf_len] = '\0';
    return TF_OK;
}

static int pivot_key_append_cstr(char **buf, size_t *buf_cap, size_t *buf_len,
                                 const char *s) {
    return pivot_key_append(buf, buf_cap, buf_len, s, strlen(s));
}

static int pivot_key_append_size(char **buf, size_t *buf_cap, size_t *buf_len,
                                 size_t value) {
    char tmp[32];
    int n = snprintf(tmp, sizeof(tmp), "%zu", value);
    if (n < 0 || (size_t)n >= sizeof(tmp)) return TF_ERROR;
    return pivot_key_append(buf, buf_cap, buf_len, tmp, (size_t)n);
}

static int pivot_ensure_ordinals(uint64_t **ord, size_t *cap, size_t need) {
    if (*cap >= need) return TF_OK;
    size_t new_cap = 0;
    if (tf_size_grow_pow2(*cap, need, 64, &new_cap) != TF_OK) return TF_ERROR;
    uint64_t *tmp = tf_reallocarray_checked(*ord, new_cap, sizeof(uint64_t));
    if (!tmp) return TF_ERROR;
    *ord = tmp;
    *cap = new_cap;
    return TF_OK;
}

static int pivot_write_exact(FILE *f, const void *ptr, size_t len) {
    return fwrite(ptr, 1, len, f) == len ? TF_OK : TF_ERROR;
}

static int pivot_read_exact(FILE *f, void *ptr, size_t len) {
    return fread(ptr, 1, len, f) == len ? TF_OK : TF_ERROR;
}

static int pivot_write_cell(FILE *f, const tf_batch *b, size_t r, size_t c) {
    uint8_t is_null = tf_batch_is_null(b, r, c) ? 1 : 0;
    if (pivot_write_exact(f, &is_null, sizeof(is_null)) != TF_OK) return TF_ERROR;
    if (is_null) return TF_OK;
    switch (b->col_types[c]) {
        case TF_TYPE_BOOL: { uint8_t v = tf_batch_get_bool(b, r, c) ? 1 : 0; return pivot_write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_INT64: { int64_t v = tf_batch_get_int64(b, r, c); return pivot_write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_FLOAT64: { double v = tf_batch_get_float64(b, r, c); return pivot_write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_STRING: {
            const char *str = tf_batch_get_string(b, r, c);
            uint64_t len = str ? (uint64_t)strlen(str) : 0;
            if (pivot_write_exact(f, &len, sizeof(len)) != TF_OK) return TF_ERROR;
            return len ? pivot_write_exact(f, str, (size_t)len) : TF_OK;
        }
        case TF_TYPE_DATE: { int32_t v = tf_batch_get_date(b, r, c); return pivot_write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_TIMESTAMP: { int64_t v = tf_batch_get_timestamp(b, r, c); return pivot_write_exact(f, &v, sizeof(v)); }
        default: return TF_OK;
    }
}

static int pivot_read_cell_value(FILE *f, pivot_spill_row *row, tf_type type, size_t c) {
    switch (type) {
        case TF_TYPE_BOOL: { uint8_t v = 0; if (pivot_read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].b = v; return TF_OK; }
        case TF_TYPE_INT64: { int64_t v = 0; if (pivot_read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].i64 = v; return TF_OK; }
        case TF_TYPE_FLOAT64: { double v = 0; if (pivot_read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].f64 = v; return TF_OK; }
        case TF_TYPE_STRING: {
            uint64_t len = 0;
            if (pivot_read_exact(f, &len, sizeof(len)) != TF_OK) return TF_ERROR;
            if (len > (uint64_t)SIZE_MAX - 1) return TF_ERROR;
            char *str = malloc((size_t)len + 1);
            if (!str) return TF_ERROR;
            if (len && pivot_read_exact(f, str, (size_t)len) != TF_OK) { free(str); return TF_ERROR; }
            str[len] = '\0';
            row->cells[c].str = str;
            return TF_OK;
        }
        case TF_TYPE_DATE: { int32_t v = 0; if (pivot_read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].date = v; return TF_OK; }
        case TF_TYPE_TIMESTAMP: { int64_t v = 0; if (pivot_read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].i64 = v; return TF_OK; }
        default: return TF_OK;
    }
}

static void pivot_spill_row_clear(pivot_spill_row *row, const tf_type *types, size_t n_cols) {
    if (!row || !row->cells || !row->nulls) return;
    for (size_t c = 0; c < n_cols; c++) {
        if (!row->nulls[c] && types[c] == TF_TYPE_STRING) free(row->cells[c].str);
        row->cells[c].str = NULL;
        row->nulls[c] = 1;
    }
}

static int pivot_spill_row_init(pivot_spill_row *row, size_t n_cols) {
    row->ordinal = 0;
    row->nulls = tf_callocarray_checked(n_cols ? n_cols : 1, sizeof(uint8_t));
    row->cells = tf_callocarray_checked(n_cols ? n_cols : 1, sizeof(*row->cells));
    if (!row->nulls || !row->cells) {
        free(row->nulls);
        free(row->cells);
        row->nulls = NULL;
        row->cells = NULL;
        return TF_ERROR;
    }
    for (size_t c = 0; c < n_cols; c++) row->nulls[c] = 1;
    return TF_OK;
}

static void pivot_spill_row_free(pivot_spill_row *row, const tf_type *types, size_t n_cols) {
    if (!row) return;
    pivot_spill_row_clear(row, types, n_cols);
    free(row->nulls);
    free(row->cells);
    row->nulls = NULL;
    row->cells = NULL;
}

static size_t pivot_reader_cols(const pivot_state *st, int output_reader) {
    return output_reader ? (st->n_pt + st->n_names) : st->n_schema_cols;
}

static tf_type pivot_output_col_type(const pivot_state *st, size_t c) {
    if (c < st->n_pt) {
        int sc = st->pt_cols ? st->pt_cols[c] : (int)c;
        return st->schema_types[sc];
    }
    return st->agg == PIVOT_COUNT ? TF_TYPE_INT64 : TF_TYPE_FLOAT64;
}

static int pivot_reader_advance(pivot_state *st, pivot_run_reader *reader, int output_reader) {
    if (!reader || !reader->file || reader->done) return 0;
    size_t n_cols = pivot_reader_cols(st, output_reader);
    tf_type *tmp_types = NULL;
    const tf_type *types = NULL;
    if (output_reader) {
        tmp_types = tf_callocarray_checked(n_cols ? n_cols : 1, sizeof(tf_type));
        if (!tmp_types) return -1;
        for (size_t c = 0; c < n_cols; c++) tmp_types[c] = pivot_output_col_type(st, c);
        types = tmp_types;
    } else {
        types = st->schema_types;
    }
    pivot_spill_row_clear(&reader->row, types, n_cols);
    if (fread(&reader->row.ordinal, sizeof(reader->row.ordinal), 1, reader->file) != 1) {
        free(tmp_types);
        if (feof(reader->file)) {
            reader->done = 1;
            reader->has_row = 0;
            return 0;
        }
        tf_set_last_error("pivot spill: failed reading run file");
        return -1;
    }
    for (size_t c = 0; c < n_cols; c++) {
        uint8_t is_null = 1;
        if (pivot_read_exact(reader->file, &is_null, sizeof(is_null)) != TF_OK) {
            free(tmp_types);
            tf_set_last_error("pivot spill: corrupt run file");
            return -1;
        }
        reader->row.nulls[c] = is_null ? 1 : 0;
        if (!reader->row.nulls[c] && pivot_read_cell_value(reader->file, &reader->row, types[c], c) != TF_OK) {
            free(tmp_types);
            tf_set_last_error("pivot spill: corrupt run file");
            return -1;
        }
    }
    free(tmp_types);
    reader->has_row = 1;
    return 1;
}

static void pivot_close_readers(pivot_state *st, int output_readers) {
    pivot_run_reader **readers = output_readers ? &st->out_readers : &st->readers;
    size_t *n_readers = output_readers ? &st->n_out_readers : &st->n_readers;
    if (!*readers) return;
    size_t n_cols = pivot_reader_cols(st, output_readers);
    tf_type *tmp_types = NULL;
    const tf_type *types = NULL;
    if (output_readers) {
        tmp_types = tf_callocarray_checked(n_cols ? n_cols : 1, sizeof(tf_type));
        if (tmp_types) for (size_t c = 0; c < n_cols; c++) tmp_types[c] = pivot_output_col_type(st, c);
        types = tmp_types;
    } else {
        types = st->schema_types;
    }
    for (size_t i = 0; i < *n_readers; i++) {
        if ((*readers)[i].file) fclose((*readers)[i].file);
        if (types) pivot_spill_row_free(&(*readers)[i].row, types, n_cols);
    }
    free(tmp_types);
    free(*readers);
    *readers = NULL;
    *n_readers = 0;
}

static void pivot_remove_paths(char ***paths, size_t *n, size_t *cap) {
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

static int pivot_append_path(char ***paths, size_t *n, size_t *cap, char *path) {
    if (*n == *cap) {
        size_t need = 0;
        size_t new_cap = 0;
        if (tf_size_add(*n, 1, &need) != TF_OK ||
            tf_size_grow_pow2(*cap, need, 8, &new_cap) != TF_OK) {
            return TF_ERROR;
        }
        char **tmp = tf_reallocarray_checked(*paths, new_cap, sizeof(char *));
        if (!tmp) return TF_ERROR;
        *paths = tmp;
        *cap = new_cap;
    }
    (*paths)[(*n)++] = path;
    return TF_OK;
}

static const char *pivot_spill_name_str(const pivot_state *st, const pivot_spill_row *row,
                                        char *buf, size_t buf_sz) {
    if (st->name_ci < 0 || row->nulls[(size_t)st->name_ci]) return NULL;
    size_t c = (size_t)st->name_ci;
    switch (st->schema_types[c]) {
        case TF_TYPE_STRING: return row->cells[c].str ? row->cells[c].str : "";
        case TF_TYPE_INT64: snprintf(buf, buf_sz, "%lld", (long long)row->cells[c].i64); return buf;
        case TF_TYPE_FLOAT64: snprintf(buf, buf_sz, "%.17g", row->cells[c].f64); return buf;
        case TF_TYPE_BOOL: return row->cells[c].b ? "true" : "false";
        case TF_TYPE_DATE: tf_date_format(row->cells[c].date, buf, buf_sz); return buf;
        case TF_TYPE_TIMESTAMP: tf_timestamp_format(row->cells[c].i64, buf, buf_sz); return buf;
        default: return NULL;
    }
}

static double pivot_spill_numeric_value(const pivot_state *st, const pivot_spill_row *row) {
    if (st->val_ci < 0 || row->nulls[(size_t)st->val_ci]) return 0.0;
    size_t c = (size_t)st->val_ci;
    switch (st->schema_types[c]) {
        case TF_TYPE_INT64: return (double)row->cells[c].i64;
        case TF_TYPE_FLOAT64: return row->cells[c].f64;
        case TF_TYPE_DATE: return (double)row->cells[c].date;
        case TF_TYPE_TIMESTAMP: return (double)row->cells[c].i64;
        case TF_TYPE_BOOL: return row->cells[c].b ? 1.0 : 0.0;
        default: return 0.0;
    }
}

static int pivot_compare_batch_cell(const tf_batch *b, size_t ra, size_t rb, size_t c) {
    int null_a = tf_batch_is_null(b, ra, c);
    int null_b = tf_batch_is_null(b, rb, c);
    if (null_a && null_b) return 0;
    if (null_a) return 1;
    if (null_b) return -1;
    switch (b->col_types[c]) {
        case TF_TYPE_BOOL: return (int)tf_batch_get_bool(b, ra, c) - (int)tf_batch_get_bool(b, rb, c);
        case TF_TYPE_INT64: {
            int64_t a = tf_batch_get_int64(b, ra, c), v = tf_batch_get_int64(b, rb, c);
            return (a > v) - (a < v);
        }
        case TF_TYPE_FLOAT64: {
            double a = tf_batch_get_float64(b, ra, c), v = tf_batch_get_float64(b, rb, c);
            return (a > v) - (a < v);
        }
        case TF_TYPE_STRING: return strcmp(tf_batch_get_string(b, ra, c), tf_batch_get_string(b, rb, c));
        case TF_TYPE_DATE: {
            int32_t a = tf_batch_get_date(b, ra, c), v = tf_batch_get_date(b, rb, c);
            return (a > v) - (a < v);
        }
        case TF_TYPE_TIMESTAMP: {
            int64_t a = tf_batch_get_timestamp(b, ra, c), v = tf_batch_get_timestamp(b, rb, c);
            return (a > v) - (a < v);
        }
        default: return 0;
    }
}

static int pivot_compare_batch_key_rows(const pivot_state *st, size_t ra, size_t rb) {
    for (size_t k = 0; k < st->n_pt; k++) {
        int ci = st->pt_cols[k];
        if (ci < 0) continue;
        int cmp = pivot_compare_batch_cell(st->buf, ra, rb, (size_t)ci);
        if (cmp != 0) return cmp;
    }
    char abuf[64], bbuf[64];
    const char *a = st->name_ci >= 0 ? get_name_str(st->buf, ra, st->name_ci, abuf, sizeof(abuf)) : NULL;
    const char *b = st->name_ci >= 0 ? get_name_str(st->buf, rb, st->name_ci, bbuf, sizeof(bbuf)) : NULL;
    if (!a && !b) {}
    else if (!a) return 1;
    else if (!b) return -1;
    else {
        int cmp = strcmp(a, b);
        if (cmp != 0) return cmp;
    }
    uint64_t oa = st->buf_ordinals[ra], ob = st->buf_ordinals[rb];
    return (oa > ob) - (oa < ob);
}

static int pivot_compare_spill_cell(const pivot_state *st, const pivot_spill_row *a,
                                    const pivot_spill_row *b, size_t c) {
    int null_a = a->nulls[c] != 0;
    int null_b = b->nulls[c] != 0;
    if (null_a && null_b) return 0;
    if (null_a) return 1;
    if (null_b) return -1;
    switch (st->schema_types[c]) {
        case TF_TYPE_BOOL: return (int)a->cells[c].b - (int)b->cells[c].b;
        case TF_TYPE_INT64:
        case TF_TYPE_TIMESTAMP: return (a->cells[c].i64 > b->cells[c].i64) - (a->cells[c].i64 < b->cells[c].i64);
        case TF_TYPE_FLOAT64: return (a->cells[c].f64 > b->cells[c].f64) - (a->cells[c].f64 < b->cells[c].f64);
        case TF_TYPE_STRING: return strcmp(a->cells[c].str, b->cells[c].str);
        case TF_TYPE_DATE: return (a->cells[c].date > b->cells[c].date) - (a->cells[c].date < b->cells[c].date);
        default: return 0;
    }
}

static int pivot_compare_spill_key_rows(const pivot_state *st, const pivot_spill_row *a,
                                        const pivot_spill_row *b) {
    for (size_t k = 0; k < st->n_pt; k++) {
        int ci = st->pt_cols[k];
        if (ci < 0) continue;
        int cmp = pivot_compare_spill_cell(st, a, b, (size_t)ci);
        if (cmp != 0) return cmp;
    }
    char abuf[64], bbuf[64];
    const char *an = pivot_spill_name_str(st, a, abuf, sizeof(abuf));
    const char *bn = pivot_spill_name_str(st, b, bbuf, sizeof(bbuf));
    if (!an && !bn) {}
    else if (!an) return 1;
    else if (!bn) return -1;
    else {
        int cmp = strcmp(an, bn);
        if (cmp != 0) return cmp;
    }
    return (a->ordinal > b->ordinal) - (a->ordinal < b->ordinal);
}

typedef struct {
    const pivot_state *st;
    int output_run;
} pivot_sort_ctx;

static int pivot_compare_indices(const void *ctx, size_t ra, size_t rb) {
    const pivot_sort_ctx *sort = (const pivot_sort_ctx *)ctx;
    if (!sort->output_run) return pivot_compare_batch_key_rows(sort->st, ra, rb);
    uint64_t oa = sort->st->out_ordinals[ra];
    uint64_t ob = sort->st->out_ordinals[rb];
    return (oa > ob) - (oa < ob);
}

static size_t *pivot_sorted_indices(size_t n, int output_run, const pivot_state *st) {
    size_t *idx = tf_mallocarray_checked(n ? n : 1, sizeof(size_t));
    if (!idx) return NULL;
    for (size_t i = 0; i < n; i++) idx[i] = i;
    pivot_sort_ctx ctx = { .st = st, .output_run = output_run };
    tf_sort_indices(idx, n, pivot_compare_indices, &ctx);
    return idx;
}

static tf_batch *pivot_create_input_buffer(const pivot_state *st, size_t capacity) {
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

static tf_batch *pivot_create_pt_batch(const pivot_state *st) {
    tf_batch *b = tf_batch_create(st->n_pt, 1);
    if (!b) return NULL;
    for (size_t k = 0; k < st->n_pt; k++) {
        int sc = st->pt_cols[k];
        if (tf_batch_set_schema(b, k, st->schema_names[sc], st->schema_types[sc]) != TF_OK) {
            tf_batch_free(b);
            return NULL;
        }
    }
    return b;
}

static tf_batch *pivot_create_output_buffer(const pivot_state *st, size_t capacity) {
    tf_batch *b = tf_batch_create(st->n_pt + st->n_names, capacity ? capacity : 16);
    if (!b) return NULL;
    if (pivot_set_output_schema_from_arrays(st, b) != TF_OK) {
        tf_batch_free(b);
        return NULL;
    }
    return b;
}

static size_t pivot_estimated_spill_row_bytes(const pivot_state *st) {
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
    bytes += st->max_categories ? st->max_categories * 64 : st->n_names * 64;
    return bytes < 64 ? 64 : bytes;
}

static int pivot_init_spill_schema(pivot_state *st, const tf_batch *in, tf_side_channels *side) {
    if (st->has_schema) return TF_OK;
    st->n_schema_cols = in->n_cols;
    st->schema_names = tf_callocarray_checked(in->n_cols ? in->n_cols : 1, sizeof(char *));
    st->schema_types = tf_callocarray_checked(in->n_cols ? in->n_cols : 1, sizeof(tf_type));
    if (!st->schema_names || !st->schema_types) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++) {
        st->schema_names[c] = strdup(in->col_names[c] ? in->col_names[c] : "");
        if (!st->schema_names[c]) return TF_ERROR;
        st->schema_types[c] = in->col_types[c];
    }
    st->name_ci = tf_batch_col_index(in, st->name_column);
    st->val_ci = tf_batch_col_index(in, st->value_column);
    if (st->name_ci < 0 || st->val_ci < 0) {
        pivot_write_error(side, "pivot spill: name_column or value_column not found");
        return TF_ERROR;
    }
    st->pt_cols = tf_mallocarray_checked(in->n_cols, sizeof(int));
    if (!st->pt_cols) return TF_ERROR;
    st->n_pt = 0;
    for (size_t c = 0; c < in->n_cols; c++) {
        if ((int)c != st->name_ci && (int)c != st->val_ci) st->pt_cols[st->n_pt++] = (int)c;
    }
    if (st->configured_run_rows > 0) {
        st->run_rows = st->configured_run_rows;
    } else if (st->spill_memory_bytes > 0) {
        size_t row_bytes = pivot_estimated_spill_row_bytes(st);
        st->run_rows = st->spill_memory_bytes / (row_bytes * 4);
        if (st->run_rows < PIVOT_MIN_RUN_ROWS) st->run_rows = PIVOT_MIN_RUN_ROWS;
    } else {
        st->run_rows = PIVOT_DEFAULT_RUN_ROWS;
    }
    if (st->run_rows == 0 || st->run_rows == SIZE_MAX) st->run_rows = PIVOT_DEFAULT_RUN_ROWS;
    st->buf = pivot_create_input_buffer(st, st->run_rows);
    if (!st->buf) return TF_ERROR;
    st->has_schema = 1;
    return TF_OK;
}

static int pivot_write_batch_run(pivot_state *st, tf_batch *batch, const uint64_t *ordinals,
                                 size_t *indices, size_t n, int output_run) {
    char *path = NULL;
    FILE *f = tf_spill_open_run_file(st->spill, output_run ? "pivot-out" : "pivot-key", &path);
    if (!f) return TF_ERROR;
    for (size_t i = 0; i < n; i++) {
        size_t r = indices[i];
        uint64_t ordinal = ordinals[r];
        if (pivot_write_exact(f, &ordinal, sizeof(ordinal)) != TF_OK) goto write_fail;
        for (size_t c = 0; c < batch->n_cols; c++) {
            if (pivot_write_cell(f, batch, r, c) != TF_OK) goto write_fail;
        }
    }
    long pos = ftell(f);
    if (pos > 0) st->spilled_bytes += (size_t)pos;
    if (fclose(f) != 0) {
        tf_set_last_error("pivot spill: failed closing run file");
        remove(path);
        free(path);
        return TF_ERROR;
    }
    if (output_run) {
        if (pivot_append_path(&st->out_run_paths, &st->n_out_runs, &st->cap_out_runs, path) != TF_OK) {
            remove(path); free(path); return TF_ERROR;
        }
    } else {
        if (pivot_append_path(&st->run_paths, &st->n_runs, &st->cap_runs, path) != TF_OK) {
            remove(path); free(path); return TF_ERROR;
        }
    }
    st->spill_runs_created++;
    return TF_OK;

write_fail:
    tf_set_last_error("pivot spill: failed writing run file");
    fclose(f);
    remove(path);
    free(path);
    return TF_ERROR;
}

static int pivot_write_key_run(pivot_state *st) {
    if (!st->buf || st->buf->n_rows == 0) return TF_OK;
    size_t *idx = pivot_sorted_indices(st->buf->n_rows, 0, st);
    if (!idx) return TF_ERROR;
    int rc = pivot_write_batch_run(st, st->buf, st->buf_ordinals, idx, st->buf->n_rows, 0);
    free(idx);
    if (rc != TF_OK) return TF_ERROR;
    tf_batch_free(st->buf);
    st->buf = pivot_create_input_buffer(st, st->run_rows);
    free(st->buf_ordinals);
    st->buf_ordinals = NULL;
    st->buf_ordinal_cap = 0;
    return st->buf ? TF_OK : TF_ERROR;
}

static int pivot_write_output_run(pivot_state *st) {
    if (!st->out_buf || st->out_buf->n_rows == 0) return TF_OK;
    size_t *idx = pivot_sorted_indices(st->out_buf->n_rows, 1, st);
    if (!idx) return TF_ERROR;
    int rc = pivot_write_batch_run(st, st->out_buf, st->out_ordinals, idx, st->out_buf->n_rows, 1);
    free(idx);
    if (rc != TF_OK) return TF_ERROR;
    tf_batch_free(st->out_buf);
    st->out_buf = pivot_create_output_buffer(st, st->run_rows);
    free(st->out_ordinals);
    st->out_ordinals = NULL;
    st->out_ordinal_cap = 0;
    return st->out_buf ? TF_OK : TF_ERROR;
}

static int pivot_open_readers(pivot_state *st, int output_readers) {
    char **paths = output_readers ? st->out_run_paths : st->run_paths;
    size_t n_paths = output_readers ? st->n_out_runs : st->n_runs;
    pivot_run_reader **readers = output_readers ? &st->out_readers : &st->readers;
    size_t *n_readers = output_readers ? &st->n_out_readers : &st->n_readers;
    if (n_paths == 0) return TF_OK;
    *readers = tf_callocarray_checked(n_paths, sizeof(pivot_run_reader));
    if (!*readers) return TF_ERROR;
    *n_readers = n_paths;
    size_t n_cols = pivot_reader_cols(st, output_readers);
    for (size_t i = 0; i < n_paths; i++) {
        (*readers)[i].file = fopen(paths[i], "rb");
        if (!(*readers)[i].file) { tf_set_last_error("pivot spill: cannot reopen run file"); return TF_ERROR; }
        if (pivot_spill_row_init(&(*readers)[i].row, n_cols) != TF_OK) return TF_ERROR;
        int rc = pivot_reader_advance(st, &(*readers)[i], output_readers);
        if (rc < 0) return TF_ERROR;
    }
    return TF_OK;
}

static int pivot_best_key_reader(const pivot_state *st) {
    int best = -1;
    for (size_t i = 0; i < st->n_readers; i++) {
        const pivot_run_reader *r = &st->readers[i];
        if (!r->has_row || r->done) continue;
        if (best < 0) { best = (int)i; continue; }
        int cmp = pivot_compare_spill_key_rows(st, &r->row, &st->readers[best].row);
        if (cmp < 0 || (cmp == 0 && i < (size_t)best)) best = (int)i;
    }
    return best;
}

static int pivot_best_ordinal_reader(const pivot_state *st) {
    int best = -1;
    for (size_t i = 0; i < st->n_out_readers; i++) {
        const pivot_run_reader *r = &st->out_readers[i];
        if (!r->has_row || r->done) continue;
        if (best < 0) { best = (int)i; continue; }
        uint64_t a = r->row.ordinal;
        uint64_t b = st->out_readers[best].row.ordinal;
        if (a < b || (a == b && i < (size_t)best)) best = (int)i;
    }
    return best;
}

static char *pivot_build_spill_group_key(const pivot_state *st, const pivot_spill_row *row) {
    size_t buf_cap = 256;
    char *buf = malloc(buf_cap);
    if (!buf) return NULL;
    size_t buf_len = 0;
    buf[0] = '\0';
    for (size_t k = 0; k < st->n_pt; k++) {
        int ci = st->pt_cols[k];
        char val_buf[64];
        const char *val = "";
        size_t val_len = 0;
        if (ci < 0 || row->nulls[(size_t)ci]) {
            if (pivot_key_append_cstr(&buf, &buf_cap, &buf_len, "N|") != TF_OK) { free(buf); return NULL; }
            continue;
        }
        size_t c = (size_t)ci;
        switch (st->schema_types[c]) {
            case TF_TYPE_STRING:
                val = row->cells[c].str ? row->cells[c].str : "";
                val_len = strlen(val);
                if (pivot_key_append_cstr(&buf, &buf_cap, &buf_len, "S") != TF_OK ||
                    pivot_key_append_size(&buf, &buf_cap, &buf_len, val_len) != TF_OK ||
                    pivot_key_append_cstr(&buf, &buf_cap, &buf_len, ":") != TF_OK ||
                    pivot_key_append(&buf, &buf_cap, &buf_len, val, val_len) != TF_OK ||
                    pivot_key_append_cstr(&buf, &buf_cap, &buf_len, "|") != TF_OK) { free(buf); return NULL; }
                continue;
            case TF_TYPE_INT64: {
                int n = snprintf(val_buf, sizeof(val_buf), "%lld", (long long)row->cells[c].i64);
                if (n < 0 || (size_t)n >= sizeof(val_buf)) { free(buf); return NULL; }
                val = val_buf; val_len = (size_t)n;
                if (pivot_key_append_cstr(&buf, &buf_cap, &buf_len, "I") != TF_OK) { free(buf); return NULL; }
                break;
            }
            case TF_TYPE_FLOAT64: {
                int n = snprintf(val_buf, sizeof(val_buf), "%.17g", row->cells[c].f64);
                if (n < 0 || (size_t)n >= sizeof(val_buf)) { free(buf); return NULL; }
                val = val_buf; val_len = (size_t)n;
                if (pivot_key_append_cstr(&buf, &buf_cap, &buf_len, "F") != TF_OK) { free(buf); return NULL; }
                break;
            }
            case TF_TYPE_BOOL:
                val = row->cells[c].b ? "1" : "0"; val_len = 1;
                if (pivot_key_append_cstr(&buf, &buf_cap, &buf_len, "B") != TF_OK) { free(buf); return NULL; }
                break;
            case TF_TYPE_DATE: {
                int n = snprintf(val_buf, sizeof(val_buf), "%d", (int)row->cells[c].date);
                if (n < 0 || (size_t)n >= sizeof(val_buf)) { free(buf); return NULL; }
                val = val_buf; val_len = (size_t)n;
                if (pivot_key_append_cstr(&buf, &buf_cap, &buf_len, "D") != TF_OK) { free(buf); return NULL; }
                break;
            }
            case TF_TYPE_TIMESTAMP: {
                int n = snprintf(val_buf, sizeof(val_buf), "%lld", (long long)row->cells[c].i64);
                if (n < 0 || (size_t)n >= sizeof(val_buf)) { free(buf); return NULL; }
                val = val_buf; val_len = (size_t)n;
                if (pivot_key_append_cstr(&buf, &buf_cap, &buf_len, "T") != TF_OK) { free(buf); return NULL; }
                break;
            }
            default:
                if (pivot_key_append_cstr(&buf, &buf_cap, &buf_len, "N|") != TF_OK) { free(buf); return NULL; }
                continue;
        }
        if (pivot_key_append(&buf, &buf_cap, &buf_len, val, val_len) != TF_OK ||
            pivot_key_append_cstr(&buf, &buf_cap, &buf_len, "|") != TF_OK) { free(buf); return NULL; }
    }
    return buf;
}

static int pivot_copy_spill_pt_values(tf_batch *dst, const pivot_state *st, const pivot_spill_row *row) {
    if (tf_batch_ensure_capacity(dst, 1) != TF_OK) return TF_ERROR;
    for (size_t k = 0; k < st->n_pt; k++) {
        int ci = st->pt_cols[k];
        if (ci < 0 || row->nulls[(size_t)ci]) {
            if (tf_batch_set_null(dst, 0, k) != TF_OK) return TF_ERROR;
        } else {
            size_t c = (size_t)ci;
            if (tf_batch_set_owned_cell_value(dst, 0, k, dst->col_types[k],
                                              0, &row->cells[c]) != TF_OK)
                return TF_ERROR;
        }
    }
    return tf_batch_expose_row(dst, 0);
}

static int pivot_spill_row_to_batch(const pivot_state *st, tf_batch *out, size_t dst_row,
                                    const pivot_spill_row *row) {
    if (tf_batch_ensure_capacity(out, dst_row + 1) != TF_OK) return TF_ERROR;
    for (size_t c = 0; c < out->n_cols; c++) {
        if (tf_batch_set_owned_cell_value(out, dst_row, c, pivot_output_col_type(st, c),
                                          row->nulls[c], &row->cells[c]) != TF_OK)
            return TF_ERROR;
    }
    return TF_OK;
}

static int pivot_append_spill_group(pivot_state *st, const tf_batch *pt_batch,
                                    const pivot_accum *accum, uint64_t first_ordinal) {
    size_t dst = st->out_buf->n_rows;
    if (pivot_emit_row(st, st->out_buf, dst, pt_batch, 0, NULL, st->n_pt, accum) != TF_OK) return TF_ERROR;
    if (pivot_ensure_ordinals(&st->out_ordinals, &st->out_ordinal_cap, dst + 1) != TF_OK) return TF_ERROR;
    st->out_ordinals[dst] = first_ordinal;
    st->spill_distinct_groups++;
    if (st->out_buf->n_rows >= st->run_rows) return pivot_write_output_run(st);
    return TF_OK;
}

static int pivot_finish_spill_group(pivot_state *st, char **current_key, tf_batch **current_pt,
                                    pivot_accum *accum, uint64_t first_ordinal) {
    if (!*current_key || !*current_pt) return TF_OK;
    size_t key_bytes_delta = 0;
    size_t new_spill_key_bytes = 0;
    int rc = TF_ERROR;
    if (tf_size_add(strlen(*current_key), 1, &key_bytes_delta) == TF_OK &&
        tf_size_add(st->spill_key_bytes, key_bytes_delta,
                    &new_spill_key_bytes) == TF_OK) {
        st->spill_key_bytes = new_spill_key_bytes;
        rc = pivot_append_spill_group(st, *current_pt, accum, first_ordinal);
    }
    pivot_accum_free(accum);
    memset(accum, 0, sizeof(*accum));
    free(*current_key);
    *current_key = NULL;
    tf_batch_free(*current_pt);
    *current_pt = NULL;
    return rc;
}

static int pivot_start_spill_group(pivot_state *st, const char *key, const pivot_spill_row *row,
                                   char **current_key, tf_batch **current_pt,
                                   pivot_accum *accum, uint64_t *first_ordinal) {
    *current_key = strdup(key);
    if (!*current_key) return TF_ERROR;
    *current_pt = pivot_create_pt_batch(st);
    if (!*current_pt) return TF_ERROR;
    if (pivot_copy_spill_pt_values(*current_pt, st, row) != TF_OK) return TF_ERROR;
    if (pivot_accum_init(accum, st->n_names) != TF_OK) return TF_ERROR;
    *first_ordinal = row->ordinal;
    return TF_OK;
}

static int pivot_produce_output_runs(pivot_state *st) {
    if (st->key_merge_done) return TF_OK;
    if (!st->has_schema || st->n_names == 0) {
        st->key_merge_done = 1;
        return TF_OK;
    }
    if (st->buf && st->buf->n_rows > 0 && pivot_write_key_run(st) != TF_OK) return TF_ERROR;
    if (st->buf) { tf_batch_free(st->buf); st->buf = NULL; }
    free(st->buf_ordinals); st->buf_ordinals = NULL; st->buf_ordinal_cap = 0;

    st->out_buf = pivot_create_output_buffer(st, st->run_rows);
    if (!st->out_buf) return TF_ERROR;
    if (pivot_open_readers(st, 0) != TF_OK) return TF_ERROR;

    char *current_key = NULL;
    tf_batch *current_pt = NULL;
    pivot_accum accum;
    memset(&accum, 0, sizeof(accum));
    int have_group = 0;
    uint64_t first_ordinal = 0;

    for (;;) {
        int best = pivot_best_key_reader(st);
        if (best < 0) break;
        pivot_run_reader *reader = &st->readers[best];
        char *key = pivot_build_spill_group_key(st, &reader->row);
        if (!key) goto fail;
        if (!have_group) {
            if (pivot_start_spill_group(st, key, &reader->row, &current_key, &current_pt,
                                        &accum, &first_ordinal) != TF_OK) { free(key); goto fail; }
            have_group = 1;
        } else if (strcmp(current_key, key) != 0) {
            if (pivot_finish_spill_group(st, &current_key, &current_pt, &accum, first_ordinal) != TF_OK) {
                free(key); goto fail;
            }
            if (pivot_start_spill_group(st, key, &reader->row, &current_key, &current_pt,
                                        &accum, &first_ordinal) != TF_OK) { free(key); goto fail; }
        } else if (reader->row.ordinal < first_ordinal) {
            if (pivot_copy_spill_pt_values(current_pt, st, &reader->row) != TF_OK) { free(key); goto fail; }
            first_ordinal = reader->row.ordinal;
        }
        free(key);

        char nbuf[64];
        const char *name = pivot_spill_name_str(st, &reader->row, nbuf, sizeof(nbuf));
        if (name) {
            int ni = find_unique_name(st, name);
            if (ni >= 0) pivot_accum_add(&accum, (size_t)ni, pivot_spill_numeric_value(st, &reader->row));
        }
        int rc = pivot_reader_advance(st, reader, 0);
        if (rc < 0) goto fail;
    }

    if (have_group && pivot_finish_spill_group(st, &current_key, &current_pt, &accum, first_ordinal) != TF_OK) goto fail;
    if (st->out_buf && st->out_buf->n_rows > 0 && pivot_write_output_run(st) != TF_OK) goto fail;
    pivot_close_readers(st, 0);
    pivot_remove_paths(&st->run_paths, &st->n_runs, &st->cap_runs);
    st->key_merge_done = 1;
    return TF_OK;

fail:
    if (current_pt) tf_batch_free(current_pt);
    if (have_group) pivot_accum_free(&accum);
    free(current_key);
    return TF_ERROR;
}

static int pivot_begin_output_merge(pivot_state *st) {
    if (st->output_merge_started) return TF_OK;
    st->output_merge_started = 1;
    if (st->out_buf) { tf_batch_free(st->out_buf); st->out_buf = NULL; }
    free(st->out_ordinals); st->out_ordinals = NULL; st->out_ordinal_cap = 0;
    if (st->n_out_runs == 0) {
        tf_spill_cleanup(st->spill);
        st->spill = NULL;
        st->output_merge_done = 1;
        return TF_OK;
    }
    return pivot_open_readers(st, 1);
}

static int pivot_output_next_batch(pivot_state *st, tf_batch **out) {
    *out = NULL;
    if (pivot_produce_output_runs(st) != TF_OK) return TF_ERROR;
    if (pivot_begin_output_merge(st) != TF_OK) return TF_ERROR;
    if (st->output_merge_done) return TF_OK;
    tf_batch *ob = pivot_create_output_buffer(st, st->output_batch_rows);
    if (!ob) return TF_ERROR;
    while (ob->n_rows < st->output_batch_rows) {
        int best = pivot_best_ordinal_reader(st);
        if (best < 0) break;
        pivot_run_reader *reader = &st->out_readers[best];
        size_t out_row = ob->n_rows;
        if (pivot_spill_row_to_batch(st, ob, out_row, &reader->row) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
        if (tf_batch_expose_row(ob, out_row) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
        int rc = pivot_reader_advance(st, reader, 1);
        if (rc < 0) { tf_batch_free(ob); return TF_ERROR; }
    }
    if (ob->n_rows == 0) {
        tf_batch_free(ob);
        pivot_close_readers(st, 1);
        pivot_remove_paths(&st->out_run_paths, &st->n_out_runs, &st->cap_out_runs);
        tf_spill_cleanup(st->spill);
        st->spill = NULL;
        st->output_merge_done = 1;
        return TF_OK;
    }
    st->spill_output_batches++;
    st->spill_output_rows += ob->n_rows;
    *out = ob;
    return TF_OK;
}

static int pivot_process_spill(pivot_state *st, tf_batch *in, tf_side_channels *side) {
    if (pivot_init_spill_schema(st, in, side) != TF_OK) return TF_ERROR;
    for (size_t r = 0; r < in->n_rows; r++) {
        if (!tf_batch_is_null(in, r, (size_t)st->name_ci)) {
            char nbuf[64];
            const char *name = get_name_str(in, r, st->name_ci, nbuf, sizeof(nbuf));
            if (name && pivot_resolve_name(st, name, side) < 0) return TF_ERROR;
        }
        size_t dst = st->buf->n_rows;
        if (tf_batch_copy_row(st->buf, dst, in, r) != TF_OK) return TF_ERROR;
        if (pivot_ensure_ordinals(&st->buf_ordinals, &st->buf_ordinal_cap, dst + 1) != TF_OK) return TF_ERROR;
        st->buf_ordinals[dst] = st->next_ordinal++;
        if (tf_batch_expose_row(st->buf, dst) != TF_OK) return TF_ERROR;
        if (st->buf->n_rows >= st->run_rows && pivot_write_key_run(st) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static int pivot_flush_next(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    pivot_state *st = self->state;
    *out = NULL;
    if (!st->use_spill) return TF_OK;
    return pivot_output_next_batch(st, out);
}

static int pivot_append_stats(tf_step *self, tf_buffer *out) {
    if (!self || !self->state || !out) return TF_ERROR;
    pivot_state *st = self->state;
    char buf[320];
    snprintf(buf, sizeof(buf), ",\"tracked_categories\":%zu", st->n_names);
    if (tf_buffer_write_str(out, buf) != TF_OK) return TF_ERROR;
    if (st->use_spill) {
        snprintf(buf, sizeof(buf),
                 ",\"spill_bytes\":%zu,\"spill_runs\":%zu,"
                 "\"spill_output_batches\":%zu,\"spill_output_rows\":%zu,"
                 "\"spill_distinct_groups\":%zu,\"tracked_key_bytes\":%zu",
                 st->spilled_bytes, st->spill_runs_created,
                 st->spill_output_batches, st->spill_output_rows,
                 st->spill_distinct_groups, st->spill_key_bytes);
        return tf_buffer_write_str(out, buf);
    }
    return TF_OK;
}

static int pivot_resolve_name(pivot_state *st, const char *name,
                              tf_side_channels *side) {
    int ni = find_unique_name(st, name);
    if (ni >= 0) return ni;
    if (st->categories_declared) {
        unknown_declared_category(st, name, side);
        return -1;
    }
    return add_unique_name(st, name, side);
}

static int pivot_prepare_sorted(pivot_state *st, const tf_batch *in,
                                tf_side_channels *side) {
    if (st->has_schema) return TF_OK;
    if (!st->categories_declared || st->n_names == 0) {
        pivot_write_error(side, "pivot: sorted=true requires declared categories");
        return TF_ERROR;
    }
    st->name_ci = tf_batch_col_index(in, st->name_column);
    st->val_ci = tf_batch_col_index(in, st->value_column);
    if (st->name_ci < 0 || st->val_ci < 0) {
        pivot_write_error(side, "pivot: name_column or value_column not found");
        return TF_ERROR;
    }
    st->pt_cols = tf_mallocarray_checked(in->n_cols, sizeof(int));
    if (!st->pt_cols) return TF_ERROR;
    st->n_pt = 0;
    for (size_t c = 0; c < in->n_cols; c++) {
        if ((int)c != st->name_ci && (int)c != st->val_ci)
            st->pt_cols[st->n_pt++] = (int)c;
    }
    st->current_pt = tf_batch_create(st->n_pt, 1);
    if (!st->current_pt) return TF_ERROR;
    for (size_t k = 0; k < st->n_pt; k++) {
        int sc = st->pt_cols[k];
        if (tf_batch_set_schema(st->current_pt, k, in->col_names[sc], in->col_types[sc]) != TF_OK)
            return TF_ERROR;
    }
    if (pivot_accum_init(&st->current_accum, st->n_names) != TF_OK) return TF_ERROR;
    st->has_schema = 1;
    return TF_OK;
}

static int pivot_start_sorted_group(pivot_state *st, char *key,
                                    const tf_batch *in, size_t row) {
    free(st->current_key);
    st->current_key = key;
    pivot_accum_reset(&st->current_accum);
    st->current_pt->n_rows = 0;
    if (tf_batch_ensure_capacity(st->current_pt, 1) != TF_OK) return TF_ERROR;
    for (size_t k = 0; k < st->n_pt; k++) {
        if (tf_batch_copy_cell_index(st->current_pt, 0, k, in, row, st->pt_cols[k]) != TF_OK) return TF_ERROR;
    }
    if (tf_batch_expose_row(st->current_pt, 0) != TF_OK) return TF_ERROR;
    st->have_current = 1;
    return TF_OK;
}

static int pivot_add_sorted_row(pivot_state *st, const tf_batch *in, size_t row,
                                tf_side_channels *side) {
    char nbuf[64];
    const char *name = get_name_str(in, row, st->name_ci, nbuf, sizeof(nbuf));
    if (!name) return TF_OK;
    int ni = pivot_resolve_name(st, name, side);
    if (ni < 0) return TF_ERROR;
    pivot_accum_add(&st->current_accum, (size_t)ni, get_numeric_value(in, row, st->val_ci));
    return TF_OK;
}

static int pivot_process_sorted(tf_step *self, tf_batch *in, tf_batch **out,
                                tf_side_channels *side) {
    pivot_state *st = self->state;
    *out = NULL;
    if (pivot_prepare_sorted(st, in, side) != TF_OK) return TF_ERROR;

    tf_batch *ob = tf_batch_create(st->n_pt + st->n_names, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) return TF_ERROR;
    if (pivot_set_output_schema_from_source(st, ob, st->current_pt, NULL, st->n_pt) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    size_t out_rows = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        char *key = build_pivot_key(in, r, st->pt_cols, st->n_pt);
        if (!key) { tf_batch_free(ob); return TF_ERROR; }
        if (!st->have_current) {
            if (pivot_start_sorted_group(st, key, in, r) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
        } else if (strcmp(st->current_key, key) != 0) {
            if (pivot_emit_row(st, ob, out_rows++, st->current_pt, 0, NULL, st->n_pt, &st->current_accum) != TF_OK) {
                free(key);
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (pivot_start_sorted_group(st, key, in, r) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
        } else {
            free(key);
        }
        if (pivot_add_sorted_row(st, in, r, side) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    if (out_rows > 0) *out = ob;
    else tf_batch_free(ob);
    return TF_OK;
}

static int pivot_process(tf_step *self, tf_batch *in, tf_batch **out,
                         tf_side_channels *side) {
    pivot_state *st = self->state;
    if (st->sorted) return pivot_process_sorted(self, in, out, side);
    *out = NULL;
    if (st->use_spill) return pivot_process_spill(st, in, side);

    if (!st->has_schema) {
        st->buf = tf_batch_create(in->n_cols, in->n_rows > 0 ? in->n_rows : 16);
        if (!st->buf) return TF_ERROR;
        for (size_t c = 0; c < in->n_cols; c++) {
            if (tf_batch_set_schema(st->buf, c, in->col_names[c], in->col_types[c]) != TF_OK)
                return TF_ERROR;
        }
        st->has_schema = 1;
    }

    int name_ci = tf_batch_col_index(in, st->name_column);
    for (size_t r = 0; r < in->n_rows; r++) {
        size_t dst_row = st->buf->n_rows;
        if (tf_batch_copy_row(st->buf, dst_row, in, r) != TF_OK) return TF_ERROR;
        if (tf_batch_expose_row(st->buf, dst_row) != TF_OK) return TF_ERROR;

        if (name_ci >= 0 && !tf_batch_is_null(in, r, (size_t)name_ci)) {
            char nbuf[64];
            const char *name = get_name_str(in, r, name_ci, nbuf, sizeof(nbuf));
            if (!name) continue;
            if (pivot_resolve_name(st, name, side) < 0) return TF_ERROR;
        }
    }

    return TF_OK;
}

static int pivot_flush_sorted(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    pivot_state *st = self->state;
    *out = NULL;
    if (!st->have_current) return TF_OK;
    tf_batch *ob = tf_batch_create(st->n_pt + st->n_names, 1);
    if (!ob) return TF_ERROR;
    if (pivot_set_output_schema_from_source(st, ob, st->current_pt, NULL, st->n_pt) != TF_OK ||
        pivot_emit_row(st, ob, 0, st->current_pt, 0, NULL, st->n_pt, &st->current_accum) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }
    free(st->current_key);
    st->current_key = NULL;
    st->have_current = 0;
    pivot_accum_reset(&st->current_accum);
    st->current_pt->n_rows = 0;
    *out = ob;
    return TF_OK;
}

static int pivot_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    pivot_state *st = self->state;
    if (st->sorted) return pivot_flush_sorted(self, out, side);
    *out = NULL;
    if (st->use_spill) return pivot_output_next_batch(st, out);

    if (!st->buf || st->buf->n_rows == 0 || st->n_names == 0) return TF_OK;

    tf_batch *buf = st->buf;
    int name_ci = tf_batch_col_index(buf, st->name_column);
    int val_ci = tf_batch_col_index(buf, st->value_column);
    if (name_ci < 0 || val_ci < 0) return TF_OK;

    int *pt_cols = tf_mallocarray_checked(buf->n_cols, sizeof(int));
    if (!pt_cols) return TF_ERROR;
    size_t n_pt = 0;
    for (size_t c = 0; c < buf->n_cols; c++) {
        if ((int)c != name_ci && (int)c != val_ci)
            pt_cols[n_pt++] = (int)c;
    }

    pivot_map map = {0};
    for (size_t r = 0; r < buf->n_rows; r++) {
        char *key = build_pivot_key(buf, r, pt_cols, n_pt);
        if (!key) { free(pt_cols); pivot_map_free(&map); return TF_ERROR; }
        int gi = find_or_add_pivot_group(&map, key, st->n_names, r);
        free(key);
        if (gi < 0) { free(pt_cols); pivot_map_free(&map); return TF_ERROR; }

        char nbuf[64];
        const char *name = get_name_str(buf, r, name_ci, nbuf, sizeof(nbuf));
        if (!name) continue;
        int ni = find_unique_name(st, name);
        if (ni < 0) continue;
        pivot_accum_add(&map.accums[gi], (size_t)ni, get_numeric_value(buf, r, val_ci));
    }

    tf_batch *ob = tf_batch_create(n_pt + st->n_names, map.count);
    if (!ob) { free(pt_cols); pivot_map_free(&map); return TF_ERROR; }
    if (pivot_set_output_schema_from_source(st, ob, buf, pt_cols, n_pt) != TF_OK) {
        free(pt_cols);
        pivot_map_free(&map);
        tf_batch_free(ob);
        return TF_ERROR;
    }

    for (size_t g = 0; g < map.count; g++) {
        if (pivot_emit_row(st, ob, g, buf, map.pass_rows[g], pt_cols, n_pt, &map.accums[g]) != TF_OK) {
            free(pt_cols);
            pivot_map_free(&map);
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    pivot_map_free(&map);
    free(pt_cols);
    *out = ob;
    return TF_OK;
}

static void pivot_state_free(pivot_state *st) {
    if (!st) return;
    if (st->has_schema) {
        pivot_close_readers(st, 0);
        pivot_close_readers(st, 1);
    }
    free(st->name_column);
    free(st->value_column);
    if (st->buf) tf_batch_free(st->buf);
    if (st->out_buf) tf_batch_free(st->out_buf);
    for (size_t i = 0; i < st->n_names; i++) free(st->unique_names[i]);
    free(st->unique_names);
    free(st->pt_cols);
    if (st->current_pt) tf_batch_free(st->current_pt);
    free(st->current_key);
    pivot_accum_free(&st->current_accum);
    pivot_remove_paths(&st->run_paths, &st->n_runs, &st->cap_runs);
    pivot_remove_paths(&st->out_run_paths, &st->n_out_runs, &st->cap_out_runs);
    for (size_t i = 0; i < st->n_schema_cols; i++) free(st->schema_names ? st->schema_names[i] : NULL);
    free(st->schema_names);
    free(st->schema_types);
    free(st->buf_ordinals);
    free(st->out_ordinals);
    tf_spill_cleanup(st->spill);
    free(st->spill_dir);
    free(st);
}

static void pivot_destroy(tf_step *self) {
    if (self) {
        pivot_state_free(self->state);
        free(self);
    }
}

static int parse_positive_size(const cJSON *args, const char *name, size_t *out) {
    return tf_json_get_size_arg(args, name, 1, TF_MAX_COUNT_ARG, out, "pivot");
}

static int load_declared_categories(pivot_state *st, const cJSON *args) {
    cJSON *cats = cJSON_GetObjectItemCaseSensitive(args, "categories");
    if (!cats) return TF_OK;
    if (!cJSON_IsArray(cats)) {
        tf_set_last_error("pivot: categories must be an array of non-empty strings");
        return TF_ERROR;
    }
    int n = cJSON_GetArraySize(cats);
    if (n <= 0) {
        tf_set_last_error("pivot: categories cannot be empty");
        return TF_ERROR;
    }
    st->categories_declared = 1;
    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(cats, i);
        if (!cJSON_IsString(item) || !item->valuestring || !item->valuestring[0]) {
            tf_set_last_error("pivot: categories must be non-empty strings");
            return TF_ERROR;
        }
        if (add_unique_name(st, item->valuestring, NULL) < 0) return TF_ERROR;
    }
    return TF_OK;
}

tf_step *tf_pivot_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *name_j = cJSON_GetObjectItemCaseSensitive(args, "name_column");
    cJSON *val_j = cJSON_GetObjectItemCaseSensitive(args, "value_column");
    if (!cJSON_IsString(name_j) || !cJSON_IsString(val_j)) return NULL;

    pivot_state *st = calloc(1, sizeof(pivot_state));
    if (!st) return NULL;
    st->output_batch_rows = PIVOT_DEFAULT_OUTPUT_ROWS;
    st->name_column = strdup(name_j->valuestring);
    st->value_column = strdup(val_j->valuestring);
    if (!st->name_column || !st->value_column) { pivot_state_free(st); return NULL; }

    cJSON *agg_j = cJSON_GetObjectItemCaseSensitive(args, "agg");
    st->agg = cJSON_IsString(agg_j) ? parse_pivot_agg(agg_j->valuestring) : PIVOT_FIRST;

    if (parse_positive_size(args, "max_categories", &st->max_categories) < 0) {
        pivot_state_free(st);
        return NULL;
    }
    if (load_declared_categories(st, args) != TF_OK) {
        pivot_state_free(st);
        return NULL;
    }

    cJSON *spill_dir_j = cJSON_GetObjectItemCaseSensitive(args, "spill_dir");
    if (cJSON_IsString(spill_dir_j) && spill_dir_j->valuestring && spill_dir_j->valuestring[0]) {
        st->use_spill = 1;
        st->spill_dir = strdup(spill_dir_j->valuestring);
        if (!st->spill_dir) { pivot_state_free(st); return NULL; }
        if (tf_spill_session_create(st->spill_dir, &st->spill) != TF_OK) { pivot_state_free(st); return NULL; }
        size_t parsed_size = 0;
        int has_spill_memory = tf_json_get_size_arg(args, "spill_memory_bytes",
                                                    1, TF_MAX_SPILL_MEMORY_BYTES,
                                                    &parsed_size, "pivot");
        if (has_spill_memory < 0) { pivot_state_free(st); return NULL; }
        if (has_spill_memory > 0) st->spill_memory_bytes = parsed_size;
        int has_spill_rows = tf_json_get_size_arg(args, "spill_run_rows",
                                                  1, TF_MAX_SPILL_RUN_ROWS,
                                                  &parsed_size, "pivot");
        if (has_spill_rows < 0) { pivot_state_free(st); return NULL; }
        if (has_spill_rows > 0) st->configured_run_rows = parsed_size;
        int has_output_rows = tf_json_get_size_arg(args, "spill_output_rows",
                                                   1, TF_MAX_SPILL_OUTPUT_ROWS,
                                                   &parsed_size, "pivot");
        if (has_output_rows < 0) {
            pivot_state_free(st);
            return NULL;
        }
        if (has_output_rows > 0) st->output_batch_rows = parsed_size;
    }

    cJSON *sorted_j = cJSON_GetObjectItemCaseSensitive(args, "sorted");
    if (sorted_j) {
        if (cJSON_IsTrue(sorted_j)) st->sorted = 1;
        else if (!cJSON_IsFalse(sorted_j)) {
            tf_set_last_error("pivot: sorted must be boolean");
            pivot_state_free(st);
            return NULL;
        }
    }
    if (st->sorted && (!st->categories_declared || st->n_names == 0)) {
        tf_set_last_error("pivot: sorted=true requires declared categories");
        pivot_state_free(st);
        return NULL;
    }
    if (st->use_spill && st->sorted) {
        tf_set_last_error("pivot: spill_dir and sorted=true are mutually exclusive");
        pivot_state_free(st);
        return NULL;
    }
    if (st->use_spill && !st->categories_declared && st->max_categories == 0) {
        tf_set_last_error("pivot: spill_dir needs declared categories or max_categories to bound output columns");
        pivot_state_free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { pivot_state_free(st); return NULL; }
    step->process = pivot_process;
    step->flush = pivot_flush;
    step->flush_next = st->use_spill ? pivot_flush_next : NULL;
    step->append_stats = pivot_append_stats;
    step->destroy = pivot_destroy;
    step->state = st;
    return step;
}
