/*
 * op_set.c -- Left-streaming row-set membership operations.
 *
 * intersect file.csv [columns=...] keeps distinct left rows whose key exists
 * in the lookup file. setdiff file.csv [columns=...] keeps distinct left rows
 * whose key does not exist in the lookup file. union file.csv keeps distinct
 * rows across input and file under an emitted-key cap. union-all file.csv
 * appends all rows and streams the file at finish.
 */

#include "internal.h"
#include "spill.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>
#include <stdarg.h>
#include <errno.h>
#include <stdint.h>
#include <unistd.h>

#define SET_DEFAULT_RUN_ROWS 8192
#define SET_DEFAULT_OUTPUT_ROWS 1024
#define SET_MIN_RUN_ROWS 16

typedef struct {
    char *key;
    size_t count;
} set_bucket;

typedef union {
    uint8_t b;
    int64_t i64;
    double f64;
    int32_t date;
    char *str;
} set_key_data;

typedef struct {
    int valid;
    int is_null;
    tf_type type;
    set_key_data v;
} set_key_value;

typedef struct {
    set_key_value *values;
    size_t n;
    int valid;
} set_key_tuple;

typedef struct {
    uint64_t ordinal;
    uint8_t *nulls;
    tf_owned_cell_value *cells;
} set_spill_row;

typedef struct {
    FILE *file;
    set_spill_row row;
    int has_row;
    int done;
    int kind; /* 0=left, 1=lookup, 2=output */
} set_spill_reader;

typedef struct {
    set_bucket *buckets;
    size_t      n_buckets;
    size_t      count;
    size_t      key_bytes;
} set_hash_map;

typedef struct {
    char   *file;
    char  **columns;
    size_t  n_columns;
    size_t  max_lookup_rows;
    size_t  max_lookup_keys;
    size_t  max_lookup_bytes;
    size_t  max_output_keys;
    size_t  max_state_bytes;
    int     mode; /* 0=intersect, 1=setdiff, 2=union, 3=union-all, 4=intersect-all, 5=setdiff-all */
    int     sorted;
    int     loaded;
    int     use_spill;

    char   *spill_dir;
    tf_spill_session *spill;
    size_t  spill_memory_bytes;
    size_t  configured_run_rows;
    size_t  run_rows;
    size_t  output_batch_rows;

    int      spill_has_schema;
    int      spill_lookup_cols_ready;
    char   **spill_schema_names;
    tf_type *spill_schema_types;
    tf_type *spill_key_types;
    size_t   spill_n_cols;

    tf_batch *spill_left_buf;
    tf_batch *spill_lookup_buf;
    tf_batch *spill_out_buf;
    uint64_t *spill_left_ordinals;
    uint64_t *spill_lookup_ordinals;
    uint64_t *spill_out_ordinals;
    size_t    spill_left_ordinal_cap;
    size_t    spill_lookup_ordinal_cap;
    size_t    spill_out_ordinal_cap;
    uint64_t  spill_next_left_ordinal;
    uint64_t  spill_next_lookup_ordinal;

    char    **spill_left_run_paths;
    char    **spill_lookup_run_paths;
    char    **spill_out_run_paths;
    size_t    spill_n_left_runs;
    size_t    spill_n_lookup_runs;
    size_t    spill_n_out_runs;
    size_t    spill_cap_left_runs;
    size_t    spill_cap_lookup_runs;
    size_t    spill_cap_out_runs;
    size_t    spill_left_run_seq;
    size_t    spill_lookup_run_seq;
    size_t    spill_out_run_seq;

    set_spill_reader *spill_left_readers;
    set_spill_reader *spill_lookup_readers;
    set_spill_reader *spill_out_readers;
    size_t    spill_n_left_readers;
    size_t    spill_n_lookup_readers;
    size_t    spill_n_out_readers;

    int       spill_lookup_loaded;
    int       spill_key_merge_done;
    int       spill_output_merge_started;
    int       spill_output_merge_done;
    char     *spill_last_lookup_key;
    char     *spill_last_left_key;

    size_t    spill_bytes;
    size_t    spill_runs;
    size_t    spill_output_batches;
    size_t    spill_output_rows;
    size_t    spill_distinct_rows;
    size_t    spill_kept_rows;
    size_t    spill_lookup_rows;
    size_t    spill_lookup_keys;
    size_t    spill_lookup_key_bytes;

    int      union_schema_ready;
    char   **union_col_names;
    tf_type *union_col_types;
    size_t   union_n_cols;
    int     *union_right_cols;
    size_t   union_file_rows;
    tf_batch *union_sorted_right_row;

    int    *left_cols;
    int    *right_cols;
    size_t  n_key_cols;
    char  **key_names;

    set_hash_map lookup;
    set_hash_map emitted;

    FILE       *sorted_file;
    tf_decoder *sorted_decoder;
    tf_batch  **sorted_batches;
    size_t      sorted_n_batches;
    size_t      sorted_batch_index;
    tf_batch   *sorted_current;
    size_t      sorted_row;
    int         sorted_flushed;
    int         sorted_have_row;
    int         sorted_exhausted;
    int         sorted_right_cols_ready;
    set_key_tuple prev_lookup_key;
    set_key_tuple current_lookup_key;
    set_key_tuple prev_left_key;
    size_t sorted_current_lookup_count;
    size_t sorted_left_run_count;
} set_state;

static size_t set_map_retained_bytes(const set_hash_map *m);
static size_t set_retained_state_bytes(const set_state *st);
static int set_check_state_bytes(const set_state *st, tf_side_channels *side);
static int set_copy_cell(tf_batch *dst, size_t dr, size_t dc,
                         const tf_batch *src, size_t sr, int sc,
                         tf_side_channels *side, const char *op);
static int set_spill_produce_union_output_runs(set_state *st, tf_side_channels *side);
static int union_next_decoded_batch(set_state *st, tf_batch **out,
                                    tf_side_channels *side);

typedef struct {
    char  *data;
    size_t len;
    size_t cap;
} keybuf;

static void keybuf_free(keybuf *b) {
    free(b->data);
    b->data = NULL;
    b->len = b->cap = 0;
}

static int keybuf_reserve(keybuf *b, size_t extra) {
    size_t need = 0;
    if (tf_size_add(b->len, extra, &need) != TF_OK ||
        tf_size_add(need, 1, &need) != TF_OK) {
        return TF_ERROR;
    }
    if (need <= b->cap) return TF_OK;
    size_t cap = 0;
    if (tf_size_grow_pow2(b->cap, need, 128, &cap) != TF_OK) return TF_ERROR;
    char *tmp = tf_reallocarray_checked(b->data, cap, sizeof(char));
    if (!tmp) return TF_ERROR;
    b->data = tmp;
    b->cap = cap;
    return TF_OK;
}

static int keybuf_append(keybuf *b, const char *s, size_t n) {
    if (keybuf_reserve(b, n) != TF_OK) return TF_ERROR;
    memcpy(b->data + b->len, s, n);
    b->len += n;
    b->data[b->len] = '\0';
    return TF_OK;
}

static int keybuf_appendf(keybuf *b, const char *fmt, ...) {
    char tmp[128];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    if (n < 0) return TF_ERROR;
    if ((size_t)n < sizeof(tmp)) return keybuf_append(b, tmp, (size_t)n);

    char *big = malloc((size_t)n + 1);
    if (!big) return TF_ERROR;
    va_start(ap, fmt);
    int n2 = vsnprintf(big, (size_t)n + 1, fmt, ap);
    va_end(ap);
    if (n2 < 0) { free(big); return TF_ERROR; }
    int rc = keybuf_append(b, big, (size_t)n2);
    free(big);
    return rc;
}

static int keybuf_append_field(keybuf *b, char tag, const char *value) {
    size_t len = value ? strlen(value) : 0;
    if (keybuf_appendf(b, "%c%zu:", tag, len) != TF_OK) return TF_ERROR;
    return len ? keybuf_append(b, value, len) : TF_OK;
}

static uint64_t set_hash(const char *s) {
    uint64_t h = 14695981039346656037ULL;
    while (*s) { h ^= (uint8_t)*s++; h *= 1099511628211ULL; }
    return h;
}

static void set_key_value_clear(set_key_value *v) {
    if (!v) return;
    if (v->valid && !v->is_null && v->type == TF_TYPE_STRING) free(v->v.str);
    memset(v, 0, sizeof(*v));
}

static int set_key_value_set(set_key_value *dst, const tf_batch *b, size_t row, int col) {
    if (!dst || !b || col < 0) return TF_ERROR;
    set_key_value_clear(dst);
    dst->valid = 1;
    dst->type = b->col_types[col];
    dst->is_null = tf_batch_is_null(b, row, (size_t)col) ? 1 : 0;
    if (dst->is_null) return TF_OK;

    switch (dst->type) {
        case TF_TYPE_BOOL:
            dst->v.b = tf_batch_get_bool(b, row, (size_t)col) ? 1 : 0;
            return TF_OK;
        case TF_TYPE_INT64:
            dst->v.i64 = tf_batch_get_int64(b, row, (size_t)col);
            return TF_OK;
        case TF_TYPE_FLOAT64:
            dst->v.f64 = tf_batch_get_float64(b, row, (size_t)col);
            return TF_OK;
        case TF_TYPE_STRING: {
            const char *s = tf_batch_get_string(b, row, (size_t)col);
            dst->v.str = strdup(s ? s : "");
            return dst->v.str ? TF_OK : TF_ERROR;
        }
        case TF_TYPE_DATE:
            dst->v.date = tf_batch_get_date(b, row, (size_t)col);
            return TF_OK;
        case TF_TYPE_TIMESTAMP:
            dst->v.i64 = tf_batch_get_timestamp(b, row, (size_t)col);
            return TF_OK;
        default:
            dst->is_null = 1;
            return TF_OK;
    }
}

static int set_key_value_copy(set_key_value *dst, const set_key_value *src) {
    if (!dst || !src || !src->valid) return TF_ERROR;
    set_key_value_clear(dst);
    *dst = *src;
    if (!src->is_null && src->type == TF_TYPE_STRING) {
        dst->v.str = strdup(src->v.str ? src->v.str : "");
        if (!dst->v.str) { memset(dst, 0, sizeof(*dst)); return TF_ERROR; }
    }
    return TF_OK;
}

static int set_key_value_compare(const set_key_value *a, const set_key_value *b, int *cmp) {
    if (!a || !b || !cmp || !a->valid || !b->valid) return TF_ERROR;
    if (a->type != b->type) return TF_ERROR;
    if (a->is_null && b->is_null) { *cmp = 0; return TF_OK; }
    if (a->is_null) { *cmp = 1; return TF_OK; }
    if (b->is_null) { *cmp = -1; return TF_OK; }

    switch (a->type) {
        case TF_TYPE_BOOL:
            *cmp = (int)a->v.b - (int)b->v.b;
            return TF_OK;
        case TF_TYPE_INT64:
        case TF_TYPE_TIMESTAMP:
            *cmp = (a->v.i64 > b->v.i64) - (a->v.i64 < b->v.i64);
            return TF_OK;
        case TF_TYPE_FLOAT64:
            *cmp = (a->v.f64 > b->v.f64) - (a->v.f64 < b->v.f64);
            return TF_OK;
        case TF_TYPE_STRING:
            *cmp = strcmp(a->v.str ? a->v.str : "", b->v.str ? b->v.str : "");
            return TF_OK;
        case TF_TYPE_DATE:
            *cmp = (a->v.date > b->v.date) - (a->v.date < b->v.date);
            return TF_OK;
        default:
            *cmp = 0;
            return TF_OK;
    }
}

static void set_key_tuple_clear(set_key_tuple *t) {
    if (!t) return;
    for (size_t i = 0; i < t->n; i++) set_key_value_clear(&t->values[i]);
    free(t->values);
    memset(t, 0, sizeof(*t));
}

static const char *set_mode_name(int mode) {
    switch (mode) {
        case 0: return "intersect";
        case 1: return "setdiff";
        case 2: return "union";
        case 3: return "union-all";
        case 4: return "intersect-all";
        case 5: return "setdiff-all";
        default: return "set op";
    }
}

static int set_key_tuple_set_from_row(set_key_tuple *dst, const tf_batch *b,
                                      size_t row, const int *cols, size_t n_cols) {
    if (!dst || !b || (!cols && n_cols > 0)) return TF_ERROR;
    set_key_tuple_clear(dst);
    dst->values = tf_callocarray_checked(n_cols ? n_cols : 1, sizeof(set_key_value));
    if (!dst->values) return TF_ERROR;
    dst->n = n_cols;
    dst->valid = 1;
    for (size_t i = 0; i < n_cols; i++) {
        if (set_key_value_set(&dst->values[i], b, row, cols[i]) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static int set_key_tuple_copy(set_key_tuple *dst, const set_key_tuple *src) {
    if (!dst || !src || !src->valid) return TF_ERROR;
    set_key_tuple_clear(dst);
    dst->values = tf_callocarray_checked(src->n ? src->n : 1, sizeof(set_key_value));
    if (!dst->values) return TF_ERROR;
    dst->n = src->n;
    dst->valid = 1;
    for (size_t i = 0; i < src->n; i++) {
        if (set_key_value_copy(&dst->values[i], &src->values[i]) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static int set_key_tuple_compare(const set_key_tuple *a, const set_key_tuple *b, int *cmp) {
    if (!a || !b || !cmp || !a->valid || !b->valid || a->n != b->n) return TF_ERROR;
    for (size_t i = 0; i < a->n; i++) {
        int c = 0;
        if (set_key_value_compare(&a->values[i], &b->values[i], &c) != TF_OK) return TF_ERROR;
        if (c != 0) { *cmp = c; return TF_OK; }
    }
    *cmp = 0;
    return TF_OK;
}

static int set_map_init(set_hash_map *m, size_t hint) {
    size_t n = 64;
    size_t target = 0;
    if (tf_size_mul(hint, 2, &target) != TF_OK) return TF_ERROR;
    while (n < target) {
        if (n > SIZE_MAX / 2) return TF_ERROR;
        n *= 2;
    }
    m->buckets = tf_callocarray_checked(n, sizeof(set_bucket));
    if (!m->buckets) return TF_ERROR;
    m->n_buckets = n;
    m->count = 0;
    m->key_bytes = 0;
    return TF_OK;
}

static void set_map_free(set_hash_map *m) {
    if (!m->buckets) return;
    for (size_t i = 0; i < m->n_buckets; i++) free(m->buckets[i].key);
    free(m->buckets);
    m->buckets = NULL;
    m->n_buckets = 0;
    m->count = 0;
    m->key_bytes = 0;
}

static void set_free_string_array(char **items, size_t n) {
    if (!items) return;
    for (size_t i = 0; i < n; i++) free(items[i]);
    free(items);
}

static int set_map_rehash(set_hash_map *m) {
    set_hash_map next = {0};
    if (set_map_init(&next, m->n_buckets) != TF_OK) return TF_ERROR;
    for (size_t i = 0; i < m->n_buckets; i++) {
        char *key = m->buckets[i].key;
        if (!key) continue;
        uint64_t h = set_hash(key);
        size_t idx = h & (next.n_buckets - 1);
        while (next.buckets[idx].key)
            idx = (idx + 1) & (next.n_buckets - 1);
        next.buckets[idx] = m->buckets[i];
        next.count++;
        m->buckets[i].key = NULL;
    }
    next.key_bytes = m->key_bytes;
    set_map_free(m);
    *m = next;
    return TF_OK;
}

static set_bucket *set_map_find_bucket(const set_hash_map *m, const char *key) {
    if (!m->buckets) return NULL;
    uint64_t h = set_hash(key);
    size_t idx = h & (m->n_buckets - 1);
    while (m->buckets[idx].key) {
        if (strcmp(m->buckets[idx].key, key) == 0) return (set_bucket *)&m->buckets[idx];
        idx = (idx + 1) & (m->n_buckets - 1);
    }
    return NULL;
}

static int set_map_contains(const set_hash_map *m, const char *key) {
    return set_map_find_bucket(m, key) != NULL;
}

static int set_write_error(tf_side_channels *side, const char *msg) {
    return tf_side_write_error(side, msg);
}

static int set_limit_error(tf_side_channels *side, const char *op,
                           const char *field, size_t limit, size_t actual) {
    char msg[224];
    snprintf(msg, sizeof(msg), "%s: %s=%zu exceeded (%zu)", op, field, limit, actual);
    return set_write_error(side, msg);
}

static int set_map_insert_owned(set_hash_map *m, char *key, size_t max_keys,
                                const char *op, const char *field,
                                tf_side_channels *side, int *inserted) {
    *inserted = 0;
    if (set_map_contains(m, key)) {
        free(key);
        return TF_OK;
    }
    if (max_keys > 0 && m->count >= max_keys) {
        if (set_limit_error(side, op, field, max_keys, m->count + 1) != TF_OK) {
            free(key);
            return TF_ERROR;
        }
        free(key);
        return TF_ERROR;
    }
    size_t load_count = 0;
    if (tf_size_mul(m->count, 2, &load_count) != TF_OK) {
        free(key);
        return TF_ERROR;
    }
    if (load_count >= m->n_buckets && set_map_rehash(m) != TF_OK) {
        free(key);
        return TF_ERROR;
    }
    uint64_t h = set_hash(key);
    size_t idx = h & (m->n_buckets - 1);
    while (m->buckets[idx].key)
        idx = (idx + 1) & (m->n_buckets - 1);
    size_t key_len = 0;
    size_t new_key_bytes = 0;
    if (tf_size_add(strlen(key), 1, &key_len) != TF_OK ||
        tf_size_add(m->key_bytes, key_len, &new_key_bytes) != TF_OK) {
        free(key);
        return TF_ERROR;
    }
    m->buckets[idx].key = key;
    m->buckets[idx].count = 1;
    m->count++;
    m->key_bytes = new_key_bytes;
    *inserted = 1;
    return TF_OK;
}

static int set_map_increment_owned(set_hash_map *m, char *key, size_t max_keys,
                                   const char *op, const char *field,
                                   tf_side_channels *side, int *inserted) {
    *inserted = 0;
    set_bucket *bucket = set_map_find_bucket(m, key);
    if (bucket) {
        if (bucket->count == SIZE_MAX) { free(key); return TF_ERROR; }
        bucket->count++;
        free(key);
        return TF_OK;
    }
    return set_map_insert_owned(m, key, max_keys, op, field, side, inserted);
}

static int set_map_decrement_if_present(set_hash_map *m, const char *key) {
    set_bucket *bucket = set_map_find_bucket(m, key);
    if (!bucket || bucket->count == 0) return 0;
    bucket->count--;
    return 1;
}

static char *format_set_key(const tf_batch *b, size_t row, const int *cols, size_t n_cols) {
    keybuf kb = {0};
    for (size_t i = 0; i < n_cols; i++) {
        int col = cols[i];
        char tmp[80];
        if (tf_batch_is_null(b, row, (size_t)col)) {
            if (keybuf_append_field(&kb, 'N', "") != TF_OK) goto fail;
            continue;
        }
        switch (b->col_types[col]) {
            case TF_TYPE_BOOL:
                if (keybuf_append_field(&kb, 'B', tf_batch_get_bool(b, row, (size_t)col) ? "true" : "false") != TF_OK) goto fail;
                break;
            case TF_TYPE_INT64:
                snprintf(tmp, sizeof(tmp), "%lld", (long long)tf_batch_get_int64(b, row, (size_t)col));
                if (keybuf_append_field(&kb, 'I', tmp) != TF_OK) goto fail;
                break;
            case TF_TYPE_FLOAT64:
                snprintf(tmp, sizeof(tmp), "%.17g", tf_batch_get_float64(b, row, (size_t)col));
                if (keybuf_append_field(&kb, 'F', tmp) != TF_OK) goto fail;
                break;
            case TF_TYPE_STRING:
                if (keybuf_append_field(&kb, 'S', tf_batch_get_string(b, row, (size_t)col)) != TF_OK) goto fail;
                break;
            case TF_TYPE_DATE:
                snprintf(tmp, sizeof(tmp), "%d", (int)tf_batch_get_date(b, row, (size_t)col));
                if (keybuf_append_field(&kb, 'D', tmp) != TF_OK) goto fail;
                break;
            case TF_TYPE_TIMESTAMP:
                snprintf(tmp, sizeof(tmp), "%lld", (long long)tf_batch_get_timestamp(b, row, (size_t)col));
                if (keybuf_append_field(&kb, 'T', tmp) != TF_OK) goto fail;
                break;
            default:
                if (keybuf_append_field(&kb, '?', "") != TF_OK) goto fail;
                break;
        }
        if (keybuf_append(&kb, "|", 1) != TF_OK) goto fail;
    }
    if (!kb.data) return strdup("");
    return kb.data;
fail:
    keybuf_free(&kb);
    return NULL;
}

static int csv_header_has_column(const uint8_t *data, size_t len, const char *name) {
    if (!data || !name) return 0;
    size_t name_len = strlen(name);
    size_t start = 0;
    for (size_t i = 0; i <= len; i++) {
        int end_field = (i == len || data[i] == ',' || data[i] == '\n' || data[i] == '\r');
        if (!end_field) continue;
        size_t a = start;
        size_t b = i;
        while (a < b && (data[a] == ' ' || data[a] == '\t')) a++;
        while (b > a && (data[b - 1] == ' ' || data[b - 1] == '\t')) b--;
        if (b > a + 1 && data[a] == '"' && data[b - 1] == '"') { a++; b--; }
        if (b - a == name_len && memcmp(data + a, name, name_len) == 0) return 1;
        if (i == len || data[i] == '\n' || data[i] == '\r') return 0;
        start = i + 1;
    }
    return 0;
}

static int csv_file_header_has_column(const char *path, const char *name) {
    if (!path || !name) return 0;
    FILE *f = fopen(path, "rb");
    if (!f) return 0;
    size_t cap = 4096;
    uint8_t *buf = malloc(cap);
    if (!buf) { fclose(f); return 0; }
    size_t len = 0;
    int found_eol = 0;
    while (len < cap) {
        int ch = fgetc(f);
        if (ch == EOF) break;
        buf[len++] = (uint8_t)ch;
        if (ch == '\n' || ch == '\r') { found_eol = 1; break; }
    }
    fclose(f);
    int ok = (len > 0 && (found_eol || len < cap)) ? csv_header_has_column(buf, len, name) : 0;
    free(buf);
    return ok;
}

static const char *set_key_column_name(const set_state *st, const tf_batch *left, size_t i) {
    return st->n_columns > 0 ? st->columns[i] : left->col_names[i];
}

static int prepare_sorted_key_columns(set_state *st, const tf_batch *left,
                                      tf_side_channels *side) {
    const char *op = set_mode_name(st->mode);
    size_t n = st->n_columns > 0 ? st->n_columns : left->n_cols;
    int *left_cols = tf_callocarray_checked(n ? n : 1, sizeof(int));
    int *right_cols = tf_callocarray_checked(n ? n : 1, sizeof(int));
    char **key_names = tf_callocarray_checked(n ? n : 1, sizeof(char *));
    if (!left_cols || !right_cols || !key_names) goto fail;
    for (size_t i = 0; i < n; i++) {
        const char *name = set_key_column_name(st, left, i);
        int lc = tf_batch_col_index(left, name);
        if (lc < 0) {
            char msg[192];
            snprintf(msg, sizeof(msg), "%s: input column '%s' not found", op, name);
            set_write_error(side, msg);
            goto fail;
        }
        if (!csv_file_header_has_column(st->file, name)) {
            char msg[192];
            snprintf(msg, sizeof(msg), "%s: lookup column '%s' not found", op, name);
            set_write_error(side, msg);
            goto fail;
        }
        left_cols[i] = lc;
        right_cols[i] = -1;
        key_names[i] = strdup(name ? name : "");
        if (!key_names[i]) goto fail;
    }
    st->left_cols = left_cols;
    st->right_cols = right_cols;
    st->key_names = key_names;
    st->n_key_cols = n;
    return TF_OK;

fail:
    free(left_cols);
    free(right_cols);
    set_free_string_array(key_names, n);
    return TF_ERROR;
}

static void free_batch_array(tf_batch **batches, size_t n_batches) {
    if (!batches) return;
    for (size_t i = 0; i < n_batches; i++) tf_batch_free(batches[i]);
    free(batches);
}


static int set_ensure_ordinals(uint64_t **ord, size_t *cap, size_t need) {
    if (*cap >= need) return TF_OK;
    size_t new_cap = 0;
    if (tf_size_grow_pow2(*cap, need, 16, &new_cap) != TF_OK) return TF_ERROR;
    uint64_t *tmp = tf_reallocarray_checked(*ord, new_cap, sizeof(uint64_t));
    if (!tmp) return TF_ERROR;
    *ord = tmp;
    *cap = new_cap;
    return TF_OK;
}

static tf_batch *set_spill_create_batch_from_schema(size_t n_cols, char **names,
                                                    const tf_type *types, size_t rows) {
    tf_batch *b = tf_batch_create(n_cols, rows ? rows : 1);
    if (!b) return NULL;
    for (size_t c = 0; c < n_cols; c++) {
        if (tf_batch_set_schema(b, c, names[c], types[c]) != TF_OK) {
            tf_batch_free(b);
            return NULL;
        }
    }
    return b;
}

static tf_batch *set_spill_create_left_batch(const set_state *st, size_t rows) {
    return set_spill_create_batch_from_schema(st->spill_n_cols, st->spill_schema_names,
                                              st->spill_schema_types, rows);
}

static tf_batch *set_spill_create_lookup_batch(const set_state *st, size_t rows) {
    tf_batch *b = tf_batch_create(st->n_key_cols, rows ? rows : 1);
    if (!b) return NULL;
    for (size_t k = 0; k < st->n_key_cols; k++) {
        if (tf_batch_set_schema(b, k, st->key_names[k], st->spill_key_types[k]) != TF_OK) {
            tf_batch_free(b);
            return NULL;
        }
    }
    return b;
}

static size_t set_spill_estimated_row_bytes_for_types(const tf_type *types, size_t n_cols) {
    size_t bytes = 40;
    for (size_t c = 0; c < n_cols; c++) {
        bytes += 1;
        switch (types[c]) {
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

static int set_spill_init_schema(set_state *st, const tf_batch *in, tf_side_channels *side) {
    if (st->spill_has_schema) return TF_OK;
    const char *op = set_mode_name(st->mode);
    size_t n_cols = in->n_cols;
    size_t n = st->n_columns > 0 ? st->n_columns : in->n_cols;
    char **schema_names = tf_callocarray_checked(n_cols ? n_cols : 1, sizeof(char *));
    tf_type *schema_types = tf_callocarray_checked(n_cols ? n_cols : 1, sizeof(tf_type));
    int *left_cols = NULL;
    int *right_cols = NULL;
    char **key_names = NULL;
    tf_type *key_types = NULL;
    tf_batch *left_buf = NULL;
    tf_batch *out_buf = NULL;
    if (!schema_names || !schema_types) goto fail;
    for (size_t c = 0; c < in->n_cols; c++) {
        schema_names[c] = strdup(in->col_names[c] ? in->col_names[c] : "");
        if (!schema_names[c]) goto fail;
        schema_types[c] = in->col_types[c];
    }

    left_cols = tf_callocarray_checked(n ? n : 1, sizeof(int));
    right_cols = tf_callocarray_checked(n ? n : 1, sizeof(int));
    key_names = tf_callocarray_checked(n ? n : 1, sizeof(char *));
    key_types = tf_callocarray_checked(n ? n : 1, sizeof(tf_type));
    if (!left_cols || !right_cols || !key_names || !key_types) goto fail;
    for (size_t i = 0; i < n; i++) {
        const char *name = st->n_columns > 0 ? st->columns[i] : in->col_names[i];
        int lc = tf_batch_col_index(in, name);
        if (lc < 0) {
            char msg[192];
            snprintf(msg, sizeof(msg), "%s: input column '%s' not found", op, name ? name : "");
            set_write_error(side, msg);
            goto fail;
        }
        if (!csv_file_header_has_column(st->file, name)) {
            char msg[192];
            snprintf(msg, sizeof(msg), "%s: lookup column '%s' not found", op, name ? name : "");
            set_write_error(side, msg);
            goto fail;
        }
        left_cols[i] = lc;
        right_cols[i] = -1;
        key_names[i] = strdup(name ? name : "");
        if (!key_names[i]) goto fail;
        key_types[i] = in->col_types[lc];
    }

    size_t run_rows = SET_DEFAULT_RUN_ROWS;
    if (st->configured_run_rows > 0) {
        run_rows = st->configured_run_rows;
    } else if (st->spill_memory_bytes > 0) {
        size_t row_bytes = set_spill_estimated_row_bytes_for_types(schema_types, n_cols);
        run_rows = st->spill_memory_bytes / (row_bytes * 4);
        if (run_rows < SET_MIN_RUN_ROWS) run_rows = SET_MIN_RUN_ROWS;
    }
    size_t output_batch_rows = st->output_batch_rows == 0 ? SET_DEFAULT_OUTPUT_ROWS : st->output_batch_rows;
    left_buf = set_spill_create_batch_from_schema(n_cols, schema_names, schema_types, run_rows);
    out_buf = set_spill_create_batch_from_schema(n_cols, schema_names, schema_types, run_rows);
    if (!left_buf || !out_buf) goto fail;

    st->spill_n_cols = n_cols;
    st->spill_schema_names = schema_names;
    st->spill_schema_types = schema_types;
    st->left_cols = left_cols;
    st->right_cols = right_cols;
    st->key_names = key_names;
    st->spill_key_types = key_types;
    st->n_key_cols = n;
    st->run_rows = run_rows;
    st->output_batch_rows = output_batch_rows;
    st->spill_left_buf = left_buf;
    st->spill_out_buf = out_buf;
    st->spill_has_schema = 1;
    return TF_OK;

fail:
    set_free_string_array(schema_names, n_cols);
    free(schema_types);
    free(left_cols);
    free(right_cols);
    set_free_string_array(key_names, n);
    free(key_types);
    if (left_buf) tf_batch_free(left_buf);
    if (out_buf) tf_batch_free(out_buf);
    return TF_ERROR;
}

static int set_spill_compare_batch_cell(const tf_batch *a, size_t ra, size_t ca,
                                        const tf_batch *b, size_t rb, size_t cb) {
    tf_type ta = a->col_types[ca], tb = b->col_types[cb];
    if (ta != tb) return (ta > tb) - (ta < tb);
    int na = tf_batch_is_null(a, ra, ca) ? 1 : 0;
    int nb = tf_batch_is_null(b, rb, cb) ? 1 : 0;
    if (na && nb) return 0;
    if (na) return 1;
    if (nb) return -1;
    switch (ta) {
        case TF_TYPE_BOOL: return (int)tf_batch_get_bool(a, ra, ca) - (int)tf_batch_get_bool(b, rb, cb);
        case TF_TYPE_INT64:
        case TF_TYPE_TIMESTAMP: {
            int64_t va = ta == TF_TYPE_TIMESTAMP ? tf_batch_get_timestamp(a, ra, ca) : tf_batch_get_int64(a, ra, ca);
            int64_t vb = tb == TF_TYPE_TIMESTAMP ? tf_batch_get_timestamp(b, rb, cb) : tf_batch_get_int64(b, rb, cb);
            return (va > vb) - (va < vb);
        }
        case TF_TYPE_FLOAT64: {
            double va = tf_batch_get_float64(a, ra, ca), vb = tf_batch_get_float64(b, rb, cb);
            return (va > vb) - (va < vb);
        }
        case TF_TYPE_STRING: return strcmp(tf_batch_get_string(a, ra, ca), tf_batch_get_string(b, rb, cb));
        case TF_TYPE_DATE: {
            int32_t va = tf_batch_get_date(a, ra, ca), vb = tf_batch_get_date(b, rb, cb);
            return (va > vb) - (va < vb);
        }
        default: return 0;
    }
}

static int set_spill_compare_batch_key_rows(const set_state *st, const tf_batch *b, size_t ra, size_t rb, int lookup) {
    for (size_t k = 0; k < st->n_key_cols; k++) {
        size_t ca = lookup ? k : (size_t)st->left_cols[k];
        int cmp = set_spill_compare_batch_cell(b, ra, ca, b, rb, ca);
        if (cmp != 0) return cmp;
    }
    return 0;
}

typedef struct {
    const set_state *st;
    const tf_batch *batch;
    const uint64_t *ordinals;
    int kind;
} set_spill_sort_ctx;

static int set_spill_compare_indices(const void *ctx, size_t a, size_t b) {
    const set_spill_sort_ctx *sort = (const set_spill_sort_ctx *)ctx;
    if (sort->kind == 2) {
        uint64_t oa = sort->ordinals[a], ob = sort->ordinals[b];
        return (oa > ob) - (oa < ob);
    }
    int cmp = set_spill_compare_batch_key_rows(sort->st, sort->batch, a, b, sort->kind == 1);
    if (cmp != 0) return cmp;
    uint64_t oa = sort->ordinals[a], ob = sort->ordinals[b];
    return (oa > ob) - (oa < ob);
}

static size_t *set_spill_sorted_indices(const set_state *st, const tf_batch *b,
                                        const uint64_t *ordinals, int kind) {
    size_t *idx = tf_mallocarray_checked(b->n_rows ? b->n_rows : 1, sizeof(size_t));
    if (!idx) return NULL;
    for (size_t i = 0; i < b->n_rows; i++) idx[i] = i;
    set_spill_sort_ctx ctx = { .st = st, .batch = b, .ordinals = ordinals, .kind = kind };
    tf_sort_indices(idx, b->n_rows, set_spill_compare_indices, &ctx);
    return idx;
}

static int set_spill_write_exact(FILE *f, const void *ptr, size_t len) {
    return fwrite(ptr, 1, len, f) == len ? TF_OK : TF_ERROR;
}

static int set_spill_read_exact(FILE *f, void *ptr, size_t len) {
    return fread(ptr, 1, len, f) == len ? TF_OK : TF_ERROR;
}

static int set_spill_write_cell(FILE *f, const tf_batch *b, size_t r, size_t c) {
    uint8_t is_null = tf_batch_is_null(b, r, c) ? 1 : 0;
    if (set_spill_write_exact(f, &is_null, sizeof(is_null)) != TF_OK) return TF_ERROR;
    if (is_null) return TF_OK;
    switch (b->col_types[c]) {
        case TF_TYPE_BOOL: { uint8_t v = tf_batch_get_bool(b, r, c) ? 1 : 0; return set_spill_write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_INT64: { int64_t v = tf_batch_get_int64(b, r, c); return set_spill_write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_FLOAT64: { double v = tf_batch_get_float64(b, r, c); return set_spill_write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_STRING: {
            const char *s = tf_batch_get_string(b, r, c);
            uint64_t len = s ? (uint64_t)strlen(s) : 0;
            if (set_spill_write_exact(f, &len, sizeof(len)) != TF_OK) return TF_ERROR;
            return len ? set_spill_write_exact(f, s, (size_t)len) : TF_OK;
        }
        case TF_TYPE_DATE: { int32_t v = tf_batch_get_date(b, r, c); return set_spill_write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_TIMESTAMP: { int64_t v = tf_batch_get_timestamp(b, r, c); return set_spill_write_exact(f, &v, sizeof(v)); }
        default: return TF_OK;
    }
}

static int set_spill_append_path(char ***paths, size_t *n, size_t *cap, char *path) {
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

static int set_spill_write_batch_run(set_state *st, tf_batch *batch, const uint64_t *ordinals,
                                     size_t *indices, size_t n, int kind) {
    const char *kind_label = kind == 0 ? "left" : (kind == 1 ? "lookup" : "out");
    char label[48];
    snprintf(label, sizeof(label), "set-%s", kind_label);
    char *path = NULL;
    FILE *f = tf_spill_open_run_file(st->spill, label, &path);
    if (!f) return TF_ERROR;
    for (size_t i = 0; i < n; i++) {
        size_t r = indices[i];
        uint64_t ordinal = ordinals[r];
        if (set_spill_write_exact(f, &ordinal, sizeof(ordinal)) != TF_OK) goto write_fail;
        for (size_t c = 0; c < batch->n_cols; c++) {
            if (set_spill_write_cell(f, batch, r, c) != TF_OK) goto write_fail;
        }
    }
    long pos = ftell(f);
    if (pos > 0) st->spill_bytes += (size_t)pos;
    if (fclose(f) != 0) {
        tf_set_last_error("set spill: failed closing run file");
        remove(path);
        free(path);
        return TF_ERROR;
    }
    int rc;
    if (kind == 0) rc = set_spill_append_path(&st->spill_left_run_paths, &st->spill_n_left_runs, &st->spill_cap_left_runs, path);
    else if (kind == 1) rc = set_spill_append_path(&st->spill_lookup_run_paths, &st->spill_n_lookup_runs, &st->spill_cap_lookup_runs, path);
    else rc = set_spill_append_path(&st->spill_out_run_paths, &st->spill_n_out_runs, &st->spill_cap_out_runs, path);
    if (rc != TF_OK) { remove(path); free(path); return TF_ERROR; }
    st->spill_runs++;
    return TF_OK;

write_fail:
    tf_set_last_error("set spill: failed writing run file");
    fclose(f);
    remove(path);
    free(path);
    return TF_ERROR;
}

static int set_spill_write_left_run(set_state *st) {
    if (!st->spill_left_buf || st->spill_left_buf->n_rows == 0) return TF_OK;
    size_t *idx = set_spill_sorted_indices(st, st->spill_left_buf, st->spill_left_ordinals, 0);
    if (!idx) return TF_ERROR;
    int rc = set_spill_write_batch_run(st, st->spill_left_buf, st->spill_left_ordinals, idx, st->spill_left_buf->n_rows, 0);
    free(idx);
    if (rc != TF_OK) return TF_ERROR;
    tf_batch_free(st->spill_left_buf);
    st->spill_left_buf = set_spill_create_left_batch(st, st->run_rows);
    free(st->spill_left_ordinals); st->spill_left_ordinals = NULL; st->spill_left_ordinal_cap = 0;
    return st->spill_left_buf ? TF_OK : TF_ERROR;
}

static int set_spill_write_lookup_run(set_state *st) {
    if (!st->spill_lookup_buf || st->spill_lookup_buf->n_rows == 0) return TF_OK;
    size_t *idx = set_spill_sorted_indices(st, st->spill_lookup_buf, st->spill_lookup_ordinals, 1);
    if (!idx) return TF_ERROR;
    int rc = set_spill_write_batch_run(st, st->spill_lookup_buf, st->spill_lookup_ordinals, idx, st->spill_lookup_buf->n_rows, 1);
    free(idx);
    if (rc != TF_OK) return TF_ERROR;
    tf_batch_free(st->spill_lookup_buf);
    st->spill_lookup_buf = set_spill_create_lookup_batch(st, st->run_rows);
    free(st->spill_lookup_ordinals); st->spill_lookup_ordinals = NULL; st->spill_lookup_ordinal_cap = 0;
    return st->spill_lookup_buf ? TF_OK : TF_ERROR;
}

static int set_spill_write_output_run(set_state *st) {
    if (!st->spill_out_buf || st->spill_out_buf->n_rows == 0) return TF_OK;
    size_t *idx = set_spill_sorted_indices(st, st->spill_out_buf, st->spill_out_ordinals, 2);
    if (!idx) return TF_ERROR;
    int rc = set_spill_write_batch_run(st, st->spill_out_buf, st->spill_out_ordinals, idx, st->spill_out_buf->n_rows, 2);
    free(idx);
    if (rc != TF_OK) return TF_ERROR;
    tf_batch_free(st->spill_out_buf);
    st->spill_out_buf = set_spill_create_left_batch(st, st->run_rows);
    free(st->spill_out_ordinals); st->spill_out_ordinals = NULL; st->spill_out_ordinal_cap = 0;
    return st->spill_out_buf ? TF_OK : TF_ERROR;
}

static void set_spill_row_clear(set_spill_row *row, const tf_type *types, size_t n_cols) {
    if (!row || !row->cells || !row->nulls) return;
    for (size_t c = 0; c < n_cols; c++) {
        if (!row->nulls[c] && types[c] == TF_TYPE_STRING) free(row->cells[c].str);
        row->cells[c].str = NULL;
        row->nulls[c] = 1;
    }
}

static int set_spill_row_init(set_spill_row *row, size_t n_cols) {
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

static void set_spill_row_free(set_spill_row *row, const tf_type *types, size_t n_cols) {
    if (!row) return;
    set_spill_row_clear(row, types, n_cols);
    free(row->nulls);
    free(row->cells);
    row->nulls = NULL;
    row->cells = NULL;
}

static int set_spill_read_cell_value(FILE *f, set_spill_row *row, const tf_type *types, size_t c) {
    switch (types[c]) {
        case TF_TYPE_BOOL: { uint8_t v = 0; if (set_spill_read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].b = v; return TF_OK; }
        case TF_TYPE_INT64: { int64_t v = 0; if (set_spill_read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].i64 = v; return TF_OK; }
        case TF_TYPE_FLOAT64: { double v = 0; if (set_spill_read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].f64 = v; return TF_OK; }
        case TF_TYPE_STRING: {
            uint64_t len = 0;
            if (set_spill_read_exact(f, &len, sizeof(len)) != TF_OK) return TF_ERROR;
            if (len > (uint64_t)SIZE_MAX - 1) return TF_ERROR;
            char *s = malloc((size_t)len + 1);
            if (!s) return TF_ERROR;
            if (len && set_spill_read_exact(f, s, (size_t)len) != TF_OK) { free(s); return TF_ERROR; }
            s[len] = '\0';
            row->cells[c].str = s;
            return TF_OK;
        }
        case TF_TYPE_DATE: { int32_t v = 0; if (set_spill_read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].date = v; return TF_OK; }
        case TF_TYPE_TIMESTAMP: { int64_t v = 0; if (set_spill_read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].i64 = v; return TF_OK; }
        default: return TF_OK;
    }
}

static const tf_type *set_spill_reader_types(const set_state *st, int kind, size_t *n_cols) {
    if (kind == 1) { *n_cols = st->n_key_cols; return st->spill_key_types; }
    *n_cols = st->spill_n_cols;
    return st->spill_schema_types;
}

static int set_spill_reader_advance(set_state *st, set_spill_reader *reader) {
    if (!reader || !reader->file || reader->done) return 0;
    size_t n_cols = 0;
    const tf_type *types = set_spill_reader_types(st, reader->kind, &n_cols);
    set_spill_row_clear(&reader->row, types, n_cols);
    if (fread(&reader->row.ordinal, sizeof(reader->row.ordinal), 1, reader->file) != 1) {
        if (feof(reader->file)) {
            reader->done = 1;
            reader->has_row = 0;
            return 0;
        }
        tf_set_last_error("set spill: failed reading run file");
        return -1;
    }
    for (size_t c = 0; c < n_cols; c++) {
        uint8_t is_null = 1;
        if (set_spill_read_exact(reader->file, &is_null, sizeof(is_null)) != TF_OK) {
            tf_set_last_error("set spill: corrupt run file");
            return -1;
        }
        reader->row.nulls[c] = is_null ? 1 : 0;
        if (!reader->row.nulls[c] && set_spill_read_cell_value(reader->file, &reader->row, types, c) != TF_OK) {
            tf_set_last_error("set spill: corrupt run file");
            return -1;
        }
    }
    reader->has_row = 1;
    return 1;
}

static void set_spill_close_readers(set_state *st, int kind) {
    set_spill_reader **readers;
    size_t *n_readers;
    if (kind == 0) { readers = &st->spill_left_readers; n_readers = &st->spill_n_left_readers; }
    else if (kind == 1) { readers = &st->spill_lookup_readers; n_readers = &st->spill_n_lookup_readers; }
    else { readers = &st->spill_out_readers; n_readers = &st->spill_n_out_readers; }
    if (!*readers) return;
    size_t n_cols = 0;
    const tf_type *types = set_spill_reader_types(st, kind, &n_cols);
    for (size_t i = 0; i < *n_readers; i++) {
        if ((*readers)[i].file) fclose((*readers)[i].file);
        set_spill_row_free(&(*readers)[i].row, types, n_cols);
    }
    free(*readers);
    *readers = NULL;
    *n_readers = 0;
}

static void set_spill_remove_paths(char ***paths, size_t *n, size_t *cap) {
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

static int set_spill_open_readers(set_state *st, int kind) {
    char **paths;
    size_t n_paths;
    set_spill_reader **readers;
    size_t *n_readers;
    if (kind == 0) { paths = st->spill_left_run_paths; n_paths = st->spill_n_left_runs; readers = &st->spill_left_readers; n_readers = &st->spill_n_left_readers; }
    else if (kind == 1) { paths = st->spill_lookup_run_paths; n_paths = st->spill_n_lookup_runs; readers = &st->spill_lookup_readers; n_readers = &st->spill_n_lookup_readers; }
    else { paths = st->spill_out_run_paths; n_paths = st->spill_n_out_runs; readers = &st->spill_out_readers; n_readers = &st->spill_n_out_readers; }
    if (n_paths == 0) return TF_OK;
    *readers = tf_callocarray_checked(n_paths, sizeof(set_spill_reader));
    if (!*readers) return TF_ERROR;
    *n_readers = n_paths;
    size_t n_cols = 0;
    set_spill_reader_types(st, kind, &n_cols);
    for (size_t i = 0; i < n_paths; i++) {
        (*readers)[i].kind = kind;
        (*readers)[i].file = fopen(paths[i], "rb");
        if (!(*readers)[i].file) { tf_set_last_error("set spill: cannot reopen run file"); return TF_ERROR; }
        if (set_spill_row_init(&(*readers)[i].row, n_cols) != TF_OK) return TF_ERROR;
        int rc = set_spill_reader_advance(st, &(*readers)[i]);
        if (rc < 0) return TF_ERROR;
    }
    return TF_OK;
}

static int set_spill_key_cell(const set_state *st, const set_spill_row *row, int kind, size_t k,
                              tf_type *type, int *is_null, const tf_owned_cell_value **cell) {
    size_t c = kind == 1 ? k : (size_t)st->left_cols[k];
    *type = kind == 1 ? st->spill_key_types[k] : st->spill_schema_types[c];
    *is_null = row->nulls[c] ? 1 : 0;
    *cell = &row->cells[c];
    return TF_OK;
}

static int set_spill_compare_key_rows(const set_state *st, const set_spill_row *a, int kind_a,
                                      const set_spill_row *b, int kind_b) {
    for (size_t k = 0; k < st->n_key_cols; k++) {
        tf_type ta, tb;
        int na, nb;
        const tf_owned_cell_value *ca, *cb;
        set_spill_key_cell(st, a, kind_a, k, &ta, &na, &ca);
        set_spill_key_cell(st, b, kind_b, k, &tb, &nb, &cb);
        if (ta != tb) return (ta > tb) - (ta < tb);
        if (na && nb) continue;
        if (na) return 1;
        if (nb) return -1;
        int cmp = 0;
        switch (ta) {
            case TF_TYPE_BOOL: cmp = (int)ca->b - (int)cb->b; break;
            case TF_TYPE_INT64:
            case TF_TYPE_TIMESTAMP: cmp = (ca->i64 > cb->i64) - (ca->i64 < cb->i64); break;
            case TF_TYPE_FLOAT64: cmp = (ca->f64 > cb->f64) - (ca->f64 < cb->f64); break;
            case TF_TYPE_STRING: cmp = strcmp(ca->str ? ca->str : "", cb->str ? cb->str : ""); break;
            case TF_TYPE_DATE: cmp = (ca->date > cb->date) - (ca->date < cb->date); break;
            default: break;
        }
        if (cmp != 0) return cmp;
    }
    return 0;
}

static char *set_spill_build_key(const set_state *st, const set_spill_row *row, int kind) {
    keybuf kb = {0};
    for (size_t k = 0; k < st->n_key_cols; k++) {
        tf_type type;
        int is_null;
        const tf_owned_cell_value *cell;
        set_spill_key_cell(st, row, kind, k, &type, &is_null, &cell);
        if (k > 0 && keybuf_append(&kb, "|", 1) != TF_OK) goto fail;
        char tmp[96];
        if (is_null) {
            snprintf(tmp, sizeof(tmp), "N:%d", (int)type);
            if (keybuf_append(&kb, tmp, strlen(tmp)) != TF_OK) goto fail;
            continue;
        }
        switch (type) {
            case TF_TYPE_BOOL:
                if (keybuf_append(&kb, cell->b ? "B:1" : "B:0", 3) != TF_OK) goto fail;
                break;
            case TF_TYPE_INT64:
                snprintf(tmp, sizeof(tmp), "I:%lld", (long long)cell->i64);
                if (keybuf_append(&kb, tmp, strlen(tmp)) != TF_OK) goto fail;
                break;
            case TF_TYPE_FLOAT64: {
                int n = snprintf(tmp, sizeof(tmp), "F:%.17g", cell->f64);
                if (n < 0 || keybuf_append(&kb, tmp, (size_t)n) != TF_OK) goto fail;
                break;
            }
            case TF_TYPE_STRING: {
                const char *s = cell->str ? cell->str : "";
                int n = snprintf(tmp, sizeof(tmp), "S:%zu:", strlen(s));
                if (n < 0 || keybuf_append(&kb, tmp, (size_t)n) != TF_OK) goto fail;
                if (keybuf_append(&kb, s, strlen(s)) != TF_OK) goto fail;
                break;
            }
            case TF_TYPE_DATE:
                snprintf(tmp, sizeof(tmp), "D:%lld", (long long)cell->date);
                if (keybuf_append(&kb, tmp, strlen(tmp)) != TF_OK) goto fail;
                break;
            case TF_TYPE_TIMESTAMP:
                snprintf(tmp, sizeof(tmp), "T:%lld", (long long)cell->i64);
                if (keybuf_append(&kb, tmp, strlen(tmp)) != TF_OK) goto fail;
                break;
            default:
                if (keybuf_append(&kb, "U", 1) != TF_OK) goto fail;
                break;
        }
    }
    if (!kb.data) return strdup("");
    return kb.data;
fail:
    keybuf_free(&kb);
    return NULL;
}

static int set_spill_best_reader(const set_state *st, int kind) {
    set_spill_reader *readers;
    size_t n_readers;
    if (kind == 0) { readers = st->spill_left_readers; n_readers = st->spill_n_left_readers; }
    else if (kind == 1) { readers = st->spill_lookup_readers; n_readers = st->spill_n_lookup_readers; }
    else { readers = st->spill_out_readers; n_readers = st->spill_n_out_readers; }
    int best = -1;
    for (size_t i = 0; i < n_readers; i++) {
        set_spill_reader *r = &readers[i];
        if (!r->has_row || r->done) continue;
        if (best < 0) { best = (int)i; continue; }
        int cmp;
        if (kind == 2) {
            uint64_t a = r->row.ordinal, b = readers[best].row.ordinal;
            cmp = (a > b) - (a < b);
        } else {
            cmp = set_spill_compare_key_rows(st, &r->row, kind, &readers[best].row, kind);
            if (cmp == 0) cmp = (r->row.ordinal > readers[best].row.ordinal) - (r->row.ordinal < readers[best].row.ordinal);
        }
        if (cmp < 0 || (cmp == 0 && i < (size_t)best)) best = (int)i;
    }
    return best;
}

static int set_spill_row_to_batch(const set_state *st, tf_batch *out, size_t dst_row,
                                  const set_spill_row *row) {
    if (tf_batch_ensure_capacity(out, dst_row + 1) != TF_OK) return TF_ERROR;
    for (size_t c = 0; c < st->spill_n_cols; c++) {
        if (tf_batch_set_owned_cell_value(out, dst_row, c, st->spill_schema_types[c],
                                          row->nulls[c], &row->cells[c]) != TF_OK)
            return TF_ERROR;
    }
    return TF_OK;
}

static int set_spill_count_lookup_key(set_state *st, const set_spill_row *row, tf_side_channels *side) {
    char *key = set_spill_build_key(st, row, 1);
    if (!key) return TF_ERROR;
    if (!st->spill_last_lookup_key || strcmp(st->spill_last_lookup_key, key) != 0) {
        if (st->max_lookup_keys > 0 && st->spill_lookup_keys >= st->max_lookup_keys) {
            set_limit_error(side, set_mode_name(st->mode), "max_lookup_keys", st->max_lookup_keys, st->spill_lookup_keys + 1);
            free(key);
            return TF_ERROR;
        }
        size_t key_bytes_delta = 0;
        size_t new_spill_lookup_key_bytes = 0;
        if (tf_size_add(strlen(key), 1, &key_bytes_delta) != TF_OK ||
            tf_size_add(st->spill_lookup_key_bytes, key_bytes_delta,
                        &new_spill_lookup_key_bytes) != TF_OK) {
            free(key);
            return TF_ERROR;
        }
        free(st->spill_last_lookup_key);
        st->spill_last_lookup_key = key;
        key = NULL;
        st->spill_lookup_keys++;
        st->spill_lookup_key_bytes = new_spill_lookup_key_bytes;
    }
    free(key);
    return TF_OK;
}

static int set_spill_append_selected_row(set_state *st, const set_spill_row *row) {
    size_t dst = st->spill_out_buf->n_rows;
    if (set_spill_row_to_batch(st, st->spill_out_buf, dst, row) != TF_OK) return TF_ERROR;
    if (set_ensure_ordinals(&st->spill_out_ordinals, &st->spill_out_ordinal_cap, dst + 1) != TF_OK) return TF_ERROR;
    st->spill_out_ordinals[dst] = row->ordinal;
    if (tf_batch_expose_row(st->spill_out_buf, dst) != TF_OK) return TF_ERROR;
    st->spill_kept_rows++;
    if (st->mode != 4 && st->mode != 5) st->spill_distinct_rows++;
    if (st->spill_out_buf->n_rows >= st->run_rows) return set_spill_write_output_run(st);
    return TF_OK;
}

static int set_spill_process_lookup_batch(set_state *st, const tf_batch *batch, tf_side_channels *side) {
    const char *op = set_mode_name(st->mode);
    if (!st->spill_lookup_cols_ready) {
        int *right_cols = tf_callocarray_checked(st->n_key_cols ? st->n_key_cols : 1, sizeof(int));
        tf_batch *lookup_buf = NULL;
        if (!right_cols) return TF_ERROR;
        for (size_t k = 0; k < st->n_key_cols; k++) {
            int rc = tf_batch_col_index(batch, st->key_names[k]);
            if (rc < 0) {
                char msg[192];
                snprintf(msg, sizeof(msg), "%s: lookup column '%s' not found", op, st->key_names[k]);
                set_write_error(side, msg);
                free(right_cols);
                return TF_ERROR;
            }
            right_cols[k] = rc;
        }
        lookup_buf = set_spill_create_lookup_batch(st, st->run_rows);
        if (!lookup_buf) {
            free(right_cols);
            return TF_ERROR;
        }
        for (size_t k = 0; k < st->n_key_cols; k++) st->right_cols[k] = right_cols[k];
        free(right_cols);
        st->spill_lookup_buf = lookup_buf;
        st->spill_lookup_cols_ready = 1;
    }
    for (size_t r = 0; r < batch->n_rows; r++) {
        if (st->max_lookup_rows > 0 && st->spill_lookup_rows >= st->max_lookup_rows) {
            set_limit_error(side, op, "max_lookup_rows", st->max_lookup_rows, st->spill_lookup_rows + 1);
            return TF_ERROR;
        }
        st->spill_lookup_rows++;
        int type_mismatch = 0;
        for (size_t k = 0; k < st->n_key_cols; k++) {
            int rc = st->right_cols[k];
            if (rc < 0) return TF_ERROR;
            if (batch->col_types[rc] != st->spill_key_types[k]) {
                type_mismatch = 1;
                break;
            }
        }
        if (type_mismatch) continue;

        size_t dst = st->spill_lookup_buf->n_rows;
        if (tf_batch_ensure_capacity(st->spill_lookup_buf, dst + 1) != TF_OK) return TF_ERROR;
        for (size_t k = 0; k < st->n_key_cols; k++) {
            int rc = st->right_cols[k];
            if (tf_batch_copy_cell(st->spill_lookup_buf, dst, k, batch, r, (size_t)rc) != TF_OK) {
                return TF_ERROR;
            }
        }
        if (set_ensure_ordinals(&st->spill_lookup_ordinals, &st->spill_lookup_ordinal_cap, dst + 1) != TF_OK) return TF_ERROR;
        st->spill_lookup_ordinals[dst] = st->spill_next_lookup_ordinal++;
        if (tf_batch_expose_row(st->spill_lookup_buf, dst) != TF_OK) return TF_ERROR;
        if (st->spill_lookup_buf->n_rows >= st->run_rows && set_spill_write_lookup_run(st) != TF_OK)
            return TF_ERROR;
    }
    return TF_OK;
}

static int set_spill_load_lookup_runs(set_state *st, tf_side_channels *side) {
    if (st->spill_lookup_loaded) return TF_OK;
    st->spill_lookup_loaded = 1;
    FILE *f = fopen(st->file, "rb");
    if (!f) {
        char msg[128];
        snprintf(msg, sizeof(msg), "%s: cannot open lookup file", set_mode_name(st->mode));
        set_write_error(side, msg);
        return TF_ERROR;
    }
    tf_decoder *dec = tf_csv_decoder_create(NULL);
    if (!dec) { fclose(f); return TF_ERROR; }
    size_t bytes_read = 0;
    int flushed = 0;
    int rc = TF_OK;
    while (!flushed) {
        uint8_t buf[64 * 1024];
        size_t n = fread(buf, 1, sizeof(buf), f);
        tf_batch **batches = NULL;
        size_t n_batches = 0;
        if (n > 0) {
            bytes_read += n;
            if (st->max_lookup_bytes > 0 && bytes_read > st->max_lookup_bytes) {
                set_limit_error(side, set_mode_name(st->mode), "max_lookup_bytes", st->max_lookup_bytes, bytes_read);
                rc = TF_ERROR;
                break;
            }
            rc = dec->decode(dec, buf, n, &batches, &n_batches, side);
        } else {
            if (ferror(f)) {
                char msg[160];
                snprintf(msg, sizeof(msg), "%s: failed reading lookup file", set_mode_name(st->mode));
                set_write_error(side, msg);
                rc = TF_ERROR;
                break;
            }
            flushed = 1;
            rc = dec->flush(dec, &batches, &n_batches, side);
        }
        if (rc != TF_OK) { free_batch_array(batches, n_batches); break; }
        for (size_t i = 0; i < n_batches; i++) {
            if (set_spill_process_lookup_batch(st, batches[i], side) != TF_OK) rc = TF_ERROR;
            tf_batch_free(batches[i]);
            if (rc != TF_OK) {
                for (size_t j = i + 1; j < n_batches; j++) tf_batch_free(batches[j]);
                break;
            }
        }
        free(batches);
        if (rc != TF_OK) break;
    }
    dec->destroy(dec);
    fclose(f);
    if (rc != TF_OK) return TF_ERROR;
    if (st->spill_lookup_buf && st->spill_lookup_buf->n_rows > 0)
        return set_spill_write_lookup_run(st);
    if (st->spill_lookup_buf) { tf_batch_free(st->spill_lookup_buf); st->spill_lookup_buf = NULL; }
    return TF_OK;
}


static int set_spill_lookup_count_for_left_key(set_state *st, const set_spill_row *left_row,
                                               size_t *count, tf_side_channels *side) {
    *count = 0;
    for (;;) {
        int lookup_idx = set_spill_best_reader(st, 1);
        if (lookup_idx < 0) return TF_OK;
        set_spill_reader *lookup = &st->spill_lookup_readers[lookup_idx];
        int cmp = set_spill_compare_key_rows(st, &lookup->row, 1, left_row, 0);
        if (cmp < 0) {
            if (set_spill_count_lookup_key(st, &lookup->row, side) != TF_OK) return TF_ERROR;
            int adv = set_spill_reader_advance(st, lookup);
            if (adv < 0) return TF_ERROR;
            continue;
        }
        if (cmp > 0) return TF_OK;

        while (lookup_idx >= 0) {
            lookup = &st->spill_lookup_readers[lookup_idx];
            cmp = set_spill_compare_key_rows(st, &lookup->row, 1, left_row, 0);
            if (cmp != 0) break;
            if (set_spill_count_lookup_key(st, &lookup->row, side) != TF_OK) return TF_ERROR;
            if (*count == SIZE_MAX) return TF_ERROR;
            (*count)++;
            int adv = set_spill_reader_advance(st, lookup);
            if (adv < 0) return TF_ERROR;
            lookup_idx = set_spill_best_reader(st, 1);
        }
        return TF_OK;
    }
}

static int set_spill_drain_lookup_stats(set_state *st, tf_side_channels *side) {
    for (;;) {
        int lookup_idx = set_spill_best_reader(st, 1);
        if (lookup_idx < 0) break;
        set_spill_reader *lookup = &st->spill_lookup_readers[lookup_idx];
        if (set_spill_count_lookup_key(st, &lookup->row, side) != TF_OK) return TF_ERROR;
        int adv = set_spill_reader_advance(st, lookup);
        if (adv < 0) return TF_ERROR;
    }
    return TF_OK;
}

static int set_spill_produce_bag_output_runs(set_state *st, tf_side_channels *side) {
    if (st->spill_key_merge_done) return TF_OK;
    if (!st->spill_has_schema) { st->spill_key_merge_done = 1; return TF_OK; }
    if (st->spill_left_buf && st->spill_left_buf->n_rows > 0 && set_spill_write_left_run(st) != TF_OK) return TF_ERROR;
    if (st->spill_left_buf) { tf_batch_free(st->spill_left_buf); st->spill_left_buf = NULL; }
    free(st->spill_left_ordinals); st->spill_left_ordinals = NULL; st->spill_left_ordinal_cap = 0;

    if (set_spill_load_lookup_runs(st, side) != TF_OK) return TF_ERROR;
    if (set_spill_open_readers(st, 0) != TF_OK) return TF_ERROR;
    if (set_spill_open_readers(st, 1) != TF_OK) return TF_ERROR;

    for (;;) {
        int left_idx = set_spill_best_reader(st, 0);
        if (left_idx < 0) break;
        set_spill_reader *left = &st->spill_left_readers[left_idx];
        char *run_key = set_spill_build_key(st, &left->row, 0);
        if (!run_key) return TF_ERROR;
        size_t lookup_count = 0;
        if (set_spill_lookup_count_for_left_key(st, &left->row, &lookup_count, side) != TF_OK) {
            free(run_key);
            return TF_ERROR;
        }

        size_t left_count = 0;
        for (;;) {
            left_idx = set_spill_best_reader(st, 0);
            if (left_idx < 0) break;
            left = &st->spill_left_readers[left_idx];
            char *left_key = set_spill_build_key(st, &left->row, 0);
            if (!left_key) { free(run_key); return TF_ERROR; }
            int same_key = strcmp(left_key, run_key) == 0;
            free(left_key);
            if (!same_key) break;

            if (left_count == SIZE_MAX) { free(run_key); return TF_ERROR; }
            left_count++;
            int keep = st->mode == 4 ? (left_count <= lookup_count) : (left_count > lookup_count);
            if (keep && set_spill_append_selected_row(st, &left->row) != TF_OK) {
                free(run_key);
                return TF_ERROR;
            }
            int adv_left = set_spill_reader_advance(st, left);
            if (adv_left < 0) { free(run_key); return TF_ERROR; }
        }
        free(run_key);
    }

    if (set_spill_drain_lookup_stats(st, side) != TF_OK) return TF_ERROR;
    if (st->spill_out_buf && st->spill_out_buf->n_rows > 0 && set_spill_write_output_run(st) != TF_OK) return TF_ERROR;
    set_spill_close_readers(st, 0);
    set_spill_close_readers(st, 1);
    set_spill_remove_paths(&st->spill_left_run_paths, &st->spill_n_left_runs, &st->spill_cap_left_runs);
    set_spill_remove_paths(&st->spill_lookup_run_paths, &st->spill_n_lookup_runs, &st->spill_cap_lookup_runs);
    st->spill_key_merge_done = 1;
    return TF_OK;
}

static int set_spill_produce_output_runs(set_state *st, tf_side_channels *side) {
    if (st->mode == 2) return set_spill_produce_union_output_runs(st, side);
    if (st->mode == 4 || st->mode == 5) return set_spill_produce_bag_output_runs(st, side);
    if (st->spill_key_merge_done) return TF_OK;
    if (!st->spill_has_schema) { st->spill_key_merge_done = 1; return TF_OK; }
    if (st->spill_left_buf && st->spill_left_buf->n_rows > 0 && set_spill_write_left_run(st) != TF_OK) return TF_ERROR;
    if (st->spill_left_buf) { tf_batch_free(st->spill_left_buf); st->spill_left_buf = NULL; }
    free(st->spill_left_ordinals); st->spill_left_ordinals = NULL; st->spill_left_ordinal_cap = 0;

    if (set_spill_load_lookup_runs(st, side) != TF_OK) return TF_ERROR;
    if (set_spill_open_readers(st, 0) != TF_OK) return TF_ERROR;
    if (set_spill_open_readers(st, 1) != TF_OK) return TF_ERROR;

    for (;;) {
        int left_idx = set_spill_best_reader(st, 0);
        if (left_idx < 0) break;
        set_spill_reader *left = &st->spill_left_readers[left_idx];
        char *left_key = set_spill_build_key(st, &left->row, 0);
        if (!left_key) return TF_ERROR;
        int duplicate_left = st->spill_last_left_key && strcmp(st->spill_last_left_key, left_key) == 0;
        if (duplicate_left) {
            free(left_key);
            int adv_left = set_spill_reader_advance(st, left);
            if (adv_left < 0) return TF_ERROR;
            continue;
        }
        free(st->spill_last_left_key);
        st->spill_last_left_key = left_key;
        left_key = NULL;

        int lookup_idx = set_spill_best_reader(st, 1);
        while (lookup_idx >= 0) {
            set_spill_reader *lookup = &st->spill_lookup_readers[lookup_idx];
            if (set_spill_count_lookup_key(st, &lookup->row, side) != TF_OK) return TF_ERROR;
            int cmp = set_spill_compare_key_rows(st, &lookup->row, 1, &left->row, 0);
            if (cmp >= 0) break;
            int adv = set_spill_reader_advance(st, lookup);
            if (adv < 0) return TF_ERROR;
            lookup_idx = set_spill_best_reader(st, 1);
        }
        int in_lookup = 0;
        if (lookup_idx >= 0) {
            set_spill_reader *lookup = &st->spill_lookup_readers[lookup_idx];
            if (set_spill_count_lookup_key(st, &lookup->row, side) != TF_OK) return TF_ERROR;
            in_lookup = set_spill_compare_key_rows(st, &lookup->row, 1, &left->row, 0) == 0;
        }
        int keep = st->mode == 0 ? in_lookup : !in_lookup;
        if (keep && set_spill_append_selected_row(st, &left->row) != TF_OK) return TF_ERROR;
        int adv_left = set_spill_reader_advance(st, left);
        if (adv_left < 0) return TF_ERROR;
    }
    for (;;) {
        int lookup_idx = set_spill_best_reader(st, 1);
        if (lookup_idx < 0) break;
        set_spill_reader *lookup = &st->spill_lookup_readers[lookup_idx];
        if (set_spill_count_lookup_key(st, &lookup->row, side) != TF_OK) return TF_ERROR;
        int adv = set_spill_reader_advance(st, lookup);
        if (adv < 0) return TF_ERROR;
    }
    if (st->spill_out_buf && st->spill_out_buf->n_rows > 0 && set_spill_write_output_run(st) != TF_OK) return TF_ERROR;
    set_spill_close_readers(st, 0);
    set_spill_close_readers(st, 1);
    set_spill_remove_paths(&st->spill_left_run_paths, &st->spill_n_left_runs, &st->spill_cap_left_runs);
    set_spill_remove_paths(&st->spill_lookup_run_paths, &st->spill_n_lookup_runs, &st->spill_cap_lookup_runs);
    st->spill_key_merge_done = 1;
    return TF_OK;
}

static int set_spill_begin_output_merge(set_state *st) {
    if (st->spill_output_merge_started) return TF_OK;
    st->spill_output_merge_started = 1;
    if (st->spill_out_buf) { tf_batch_free(st->spill_out_buf); st->spill_out_buf = NULL; }
    free(st->spill_out_ordinals); st->spill_out_ordinals = NULL; st->spill_out_ordinal_cap = 0;
    if (st->spill_n_out_runs == 0) {
        tf_spill_cleanup(st->spill);
        st->spill = NULL;
        st->spill_output_merge_done = 1;
        return TF_OK;
    }
    return set_spill_open_readers(st, 2);
}

static int set_spill_output_next_batch(set_state *st, tf_batch **out, tf_side_channels *side) {
    *out = NULL;
    if (set_spill_produce_output_runs(st, side) != TF_OK) return TF_ERROR;
    if (set_spill_begin_output_merge(st) != TF_OK) return TF_ERROR;
    if (st->spill_output_merge_done) return TF_OK;
    tf_batch *ob = set_spill_create_left_batch(st, st->output_batch_rows);
    if (!ob) return TF_ERROR;
    while (ob->n_rows < st->output_batch_rows) {
        int best = set_spill_best_reader(st, 2);
        if (best < 0) break;
        set_spill_reader *reader = &st->spill_out_readers[best];
        size_t out_row = ob->n_rows;
        if (set_spill_row_to_batch(st, ob, out_row, &reader->row) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
        if (tf_batch_expose_row(ob, out_row) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
        int rc = set_spill_reader_advance(st, reader);
        if (rc < 0) { tf_batch_free(ob); return TF_ERROR; }
    }
    if (ob->n_rows == 0) {
        tf_batch_free(ob);
        set_spill_close_readers(st, 2);
        set_spill_remove_paths(&st->spill_out_run_paths, &st->spill_n_out_runs, &st->spill_cap_out_runs);
        tf_spill_cleanup(st->spill);
        st->spill = NULL;
        st->spill_output_merge_done = 1;
        return TF_OK;
    }
    st->spill_output_batches++;
    st->spill_output_rows += ob->n_rows;
    *out = ob;
    return TF_OK;
}

static int set_process_spill(tf_step *self, tf_batch *in, tf_batch **out, tf_side_channels *side) {
    set_state *st = self->state;
    *out = NULL;
    if (set_spill_init_schema(st, in, side) != TF_OK) return TF_ERROR;
    for (size_t r = 0; r < in->n_rows; r++) {
        size_t dst = st->spill_left_buf->n_rows;
        if (tf_batch_copy_row(st->spill_left_buf, dst, in, r) != TF_OK) return TF_ERROR;
        if (set_ensure_ordinals(&st->spill_left_ordinals, &st->spill_left_ordinal_cap, dst + 1) != TF_OK) return TF_ERROR;
        st->spill_left_ordinals[dst] = st->spill_next_left_ordinal++;
        if (tf_batch_expose_row(st->spill_left_buf, dst) != TF_OK) return TF_ERROR;
        if (st->spill_left_buf->n_rows >= st->run_rows && set_spill_write_left_run(st) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static void sorted_set_free_pending_batches(set_state *st) {
    if (!st || !st->sorted_batches) return;
    for (size_t i = st->sorted_batch_index; i < st->sorted_n_batches; i++) {
        if (st->sorted_batches[i]) tf_batch_free(st->sorted_batches[i]);
    }
    free(st->sorted_batches);
    st->sorted_batches = NULL;
    st->sorted_n_batches = 0;
    st->sorted_batch_index = 0;
}

static void sorted_set_close(set_state *st) {
    if (!st) return;
    if (st->sorted_current) {
        tf_batch_free(st->sorted_current);
        st->sorted_current = NULL;
    }
    sorted_set_free_pending_batches(st);
    if (st->sorted_decoder) {
        st->sorted_decoder->destroy(st->sorted_decoder);
        st->sorted_decoder = NULL;
    }
    if (st->sorted_file) {
        fclose(st->sorted_file);
        st->sorted_file = NULL;
    }
    st->sorted_have_row = 0;
    st->sorted_exhausted = 1;
}

static int sorted_set_capture_schema(set_state *st, const tf_batch *b,
                                     tf_side_channels *side) {
    if (st->sorted_right_cols_ready) return TF_OK;
    const char *op = set_mode_name(st->mode);
    int *right_cols = tf_callocarray_checked(st->n_key_cols ? st->n_key_cols : 1, sizeof(int));
    if (!right_cols) return TF_ERROR;
    for (size_t i = 0; i < st->n_key_cols; i++) {
        int rc = tf_batch_col_index(b, st->key_names[i]);
        if (rc < 0) {
            char msg[192];
            snprintf(msg, sizeof(msg), "%s: lookup column '%s' not found", op, st->key_names[i]);
            set_write_error(side, msg);
            free(right_cols);
            return TF_ERROR;
        }
        right_cols[i] = rc;
    }
    for (size_t i = 0; i < st->n_key_cols; i++) st->right_cols[i] = right_cols[i];
    free(right_cols);
    st->sorted_right_cols_ready = 1;
    return TF_OK;
}

static int sorted_set_open(set_state *st, tf_side_channels *side) {
    if (!st || st->sorted_file || st->sorted_decoder) return TF_OK;
    st->sorted_file = fopen(st->file, "rb");
    if (!st->sorted_file) {
        set_write_error(side, st->mode == 0 ? "intersect: cannot open lookup file" : "setdiff: cannot open lookup file");
        return TF_ERROR;
    }
    st->sorted_decoder = tf_csv_decoder_create(NULL);
    if (!st->sorted_decoder) return TF_ERROR;
    return TF_OK;
}

static int sorted_set_next_batch(set_state *st, tf_side_channels *side) {
    if (st->sorted_current) {
        tf_batch_free(st->sorted_current);
        st->sorted_current = NULL;
    }

    for (;;) {
        if (st->sorted_batches && st->sorted_batch_index < st->sorted_n_batches) {
            st->sorted_current = st->sorted_batches[st->sorted_batch_index++];
            if (st->sorted_current && sorted_set_capture_schema(st, st->sorted_current, side) != TF_OK)
                return TF_ERROR;
            st->sorted_row = 0;
            if (st->sorted_current && st->sorted_current->n_rows > 0) return TF_OK;
            if (st->sorted_current) {
                tf_batch_free(st->sorted_current);
                st->sorted_current = NULL;
            }
            continue;
        }

        sorted_set_free_pending_batches(st);
        if (st->sorted_flushed) {
            st->sorted_exhausted = 1;
            return TF_OK;
        }

        if (sorted_set_open(st, side) != TF_OK) return TF_ERROR;
        uint8_t buf[64 * 1024];
        size_t n = fread(buf, 1, sizeof(buf), st->sorted_file);
        tf_batch **batches = NULL;
        size_t n_batches = 0;
        int rc;
        if (n > 0) {
            rc = st->sorted_decoder->decode(st->sorted_decoder, buf, n, &batches, &n_batches, side);
        } else {
            if (ferror(st->sorted_file)) {
                set_write_error(side, st->mode == 0 ? "intersect: failed reading lookup file" : "setdiff: failed reading lookup file");
                return TF_ERROR;
            }
            st->sorted_flushed = 1;
            rc = st->sorted_decoder->flush(st->sorted_decoder, &batches, &n_batches, side);
        }
        if (rc != TF_OK) return TF_ERROR;
        st->sorted_batches = batches;
        st->sorted_n_batches = n_batches;
        st->sorted_batch_index = 0;
        if (n_batches == 0) {
            free(batches);
            st->sorted_batches = NULL;
            if (st->sorted_flushed) {
                st->sorted_exhausted = 1;
                return TF_OK;
            }
        }
    }
}

static int sorted_set_load_row_if_needed(set_state *st, tf_side_channels *side) {
    if (st->sorted_have_row) return TF_OK;
    for (;;) {
        if (!st->sorted_current || st->sorted_row >= st->sorted_current->n_rows) {
            if (sorted_set_next_batch(st, side) != TF_OK) return TF_ERROR;
            if (st->sorted_exhausted) return TF_OK;
            continue;
        }

        set_key_tuple cur = {0};
        if (set_key_tuple_set_from_row(&cur, st->sorted_current, st->sorted_row,
                                       st->right_cols, st->n_key_cols) != TF_OK) {
            set_key_tuple_clear(&cur);
            return TF_ERROR;
        }
        if (st->prev_lookup_key.valid) {
            int cmp = 0;
            if (set_key_tuple_compare(&st->prev_lookup_key, &cur, &cmp) != TF_OK) {
                set_key_tuple_clear(&cur);
                char msg[192];
                snprintf(msg, sizeof(msg), "%s: lookup key types changed", set_mode_name(st->mode));
                set_write_error(side, msg);
                return TF_ERROR;
            }
            if (cmp > 0) {
                set_key_tuple_clear(&cur);
                char msg[192];
                snprintf(msg, sizeof(msg), "%s: lookup side is not sorted by key", set_mode_name(st->mode));
                set_write_error(side, msg);
                return TF_ERROR;
            }
        }
        if (set_key_tuple_copy(&st->prev_lookup_key, &cur) != TF_OK) {
            set_key_tuple_clear(&cur);
            return TF_ERROR;
        }
        set_key_tuple_clear(&cur);
        st->sorted_have_row = 1;
        return TF_OK;
    }
}

static void sorted_set_consume_row(set_state *st) {
    if (!st || !st->sorted_have_row) return;
    st->sorted_row++;
    st->sorted_have_row = 0;
}

static int sorted_set_load_next_lookup_key(set_state *st, tf_side_channels *side) {
    const char *op = set_mode_name(st->mode);
    set_key_tuple_clear(&st->current_lookup_key);
    st->sorted_current_lookup_count = 0;
    if (sorted_set_load_row_if_needed(st, side) != TF_OK) return TF_ERROR;
    if (st->sorted_exhausted && !st->sorted_have_row) return TF_OK;

    if (set_key_tuple_set_from_row(&st->current_lookup_key, st->sorted_current,
                                   st->sorted_row, st->right_cols, st->n_key_cols) != TF_OK)
        return TF_ERROR;

    while (st->sorted_have_row) {
        set_key_tuple row_key = {0};
        if (set_key_tuple_set_from_row(&row_key, st->sorted_current, st->sorted_row,
                                       st->right_cols, st->n_key_cols) != TF_OK) {
            set_key_tuple_clear(&row_key);
            return TF_ERROR;
        }
        int cmp = 0;
        if (set_key_tuple_compare(&st->current_lookup_key, &row_key, &cmp) != TF_OK) {
            set_key_tuple_clear(&row_key);
            char msg[192];
            snprintf(msg, sizeof(msg), "%s: lookup key types changed", op);
            set_write_error(side, msg);
            return TF_ERROR;
        }
        set_key_tuple_clear(&row_key);
        if (cmp != 0) break;
        if (st->sorted_current_lookup_count == SIZE_MAX) return TF_ERROR;
        st->sorted_current_lookup_count++;
        sorted_set_consume_row(st);
        if (sorted_set_load_row_if_needed(st, side) != TF_OK) return TF_ERROR;
        if (st->sorted_exhausted && !st->sorted_have_row) break;
    }
    return TF_OK;
}

static int sorted_set_ensure_lookup_at_least(set_state *st, const set_key_tuple *left_key,
                                             tf_side_channels *side) {
    if (!st->current_lookup_key.valid && !st->sorted_exhausted) {
        if (sorted_set_load_next_lookup_key(st, side) != TF_OK) return TF_ERROR;
    }
    while (st->current_lookup_key.valid) {
        int cmp = 0;
        if (set_key_tuple_compare(&st->current_lookup_key, left_key, &cmp) != TF_OK) {
            char msg[192];
            snprintf(msg, sizeof(msg), "%s: input and lookup key types differ", set_mode_name(st->mode));
            set_write_error(side, msg);
            return TF_ERROR;
        }
        if (cmp >= 0) return TF_OK;
        if (sorted_set_load_next_lookup_key(st, side) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static int set_process_sorted(tf_step *self, tf_batch *in, tf_batch **out,
                              tf_side_channels *side) {
    set_state *st = self->state;
    const char *op = set_mode_name(st->mode);
    *out = NULL;
    if (!st->loaded) {
        if (prepare_sorted_key_columns(st, in, side) != TF_OK) return TF_ERROR;
        st->loaded = 1;
    }

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows > 0 ? in->n_rows : 16);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++) {
        if (tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    size_t out_row = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        set_key_tuple left_key = {0};
        if (set_key_tuple_set_from_row(&left_key, in, r, st->left_cols, st->n_key_cols) != TF_OK) {
            set_key_tuple_clear(&left_key);
            tf_batch_free(ob);
            return TF_ERROR;
        }

        int duplicate_left = 0;
        if (st->prev_left_key.valid) {
            int cmp = 0;
            if (set_key_tuple_compare(&st->prev_left_key, &left_key, &cmp) != TF_OK) {
                set_key_tuple_clear(&left_key);
                tf_batch_free(ob);
                char msg[192];
                snprintf(msg, sizeof(msg), "%s: left key types changed", op);
                set_write_error(side, msg);
                return TF_ERROR;
            }
            if (cmp > 0) {
                set_key_tuple_clear(&left_key);
                tf_batch_free(ob);
                char msg[192];
                snprintf(msg, sizeof(msg), "%s: left side is not sorted by key", op);
                set_write_error(side, msg);
                return TF_ERROR;
            }
            duplicate_left = (cmp == 0);
        }
        if (set_key_tuple_copy(&st->prev_left_key, &left_key) != TF_OK) {
            set_key_tuple_clear(&left_key);
            tf_batch_free(ob);
            return TF_ERROR;
        }

        if (st->mode == 4 || st->mode == 5) {
            if (duplicate_left) {
                if (st->sorted_left_run_count == SIZE_MAX) {
                    set_key_tuple_clear(&left_key);
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                st->sorted_left_run_count++;
            } else {
                st->sorted_left_run_count = 1;
            }
        } else if (duplicate_left) {
            set_key_tuple_clear(&left_key);
            continue;
        }

        if (sorted_set_ensure_lookup_at_least(st, &left_key, side) != TF_OK) {
            set_key_tuple_clear(&left_key);
            tf_batch_free(ob);
            return TF_ERROR;
        }
        int in_lookup = 0;
        size_t lookup_count = 0;
        if (st->current_lookup_key.valid) {
            int cmp = 0;
            if (set_key_tuple_compare(&st->current_lookup_key, &left_key, &cmp) != TF_OK) {
                set_key_tuple_clear(&left_key);
                tf_batch_free(ob);
                char msg[192];
                snprintf(msg, sizeof(msg), "%s: input and lookup key types differ", op);
                set_write_error(side, msg);
                return TF_ERROR;
            }
            in_lookup = (cmp == 0);
            if (in_lookup) lookup_count = st->sorted_current_lookup_count;
        }
        int keep = 0;
        if (st->mode == 4) keep = st->sorted_left_run_count <= lookup_count;
        else if (st->mode == 5) keep = st->sorted_left_run_count > lookup_count;
        else keep = st->mode == 0 ? in_lookup : !in_lookup;
        if (keep) {
            if (tf_batch_copy_row(ob, out_row, in, r) != TF_OK) {
                set_key_tuple_clear(&left_key);
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (tf_batch_expose_row(ob, out_row) != TF_OK) {
                set_key_tuple_clear(&left_key);
                tf_batch_free(ob);
                return TF_ERROR;
            }
            out_row++;
        }
        set_key_tuple_clear(&left_key);
    }

    if (out_row > 0) *out = ob;
    else tf_batch_free(ob);
    return TF_OK;
}

static int read_lookup_file(set_state *st, uint8_t **out, size_t *out_len,
                            tf_side_channels *side) {
    FILE *f = fopen(st->file, "rb");
    if (!f) return TF_ERROR;
    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return TF_ERROR; }
    long fsize = ftell(f);
    if (fseek(f, 0, SEEK_SET) != 0) { fclose(f); return TF_ERROR; }
    if (fsize < 0) { fclose(f); return TF_ERROR; }
    if (st->max_lookup_bytes > 0 && (size_t)fsize > st->max_lookup_bytes) {
        fclose(f);
        set_limit_error(side, set_mode_name(st->mode),
                        "max_lookup_bytes", st->max_lookup_bytes, (size_t)fsize);
        return TF_ERROR;
    }
    uint8_t *data = NULL;
    if (fsize > 0) {
        data = malloc((size_t)fsize);
        if (!data) { fclose(f); return TF_ERROR; }
        size_t nread = fread(data, 1, (size_t)fsize, f);
        if (nread != (size_t)fsize) { free(data); fclose(f); return TF_ERROR; }
    }
    fclose(f);
    *out = data;
    *out_len = (size_t)fsize;
    return TF_OK;
}

static int prepare_key_columns(set_state *st, const tf_batch *left,
                               const tf_batch *right_schema,
                               const uint8_t *raw, size_t raw_len,
                               tf_side_channels *side) {
    const char *op = set_mode_name(st->mode);
    size_t n = st->n_columns > 0 ? st->n_columns : left->n_cols;
    int *left_cols = tf_callocarray_checked(n ? n : 1, sizeof(int));
    int *right_cols = tf_callocarray_checked(n ? n : 1, sizeof(int));
    if (!left_cols || !right_cols) goto fail;
    for (size_t i = 0; i < n; i++) {
        const char *name = st->n_columns > 0 ? st->columns[i] : left->col_names[i];
        int lc = tf_batch_col_index(left, name);
        if (lc < 0) {
            char msg[192];
            snprintf(msg, sizeof(msg), "%s: input column '%s' not found", op, name);
            set_write_error(side, msg);
            goto fail;
        }
        left_cols[i] = lc;
        if (right_schema) {
            int rc = tf_batch_col_index(right_schema, name);
            if (rc < 0) {
                char msg[192];
                snprintf(msg, sizeof(msg), "%s: lookup column '%s' not found", op, name);
                set_write_error(side, msg);
                goto fail;
            }
            right_cols[i] = rc;
        } else if (!csv_header_has_column(raw, raw_len, name)) {
            char msg[192];
            snprintf(msg, sizeof(msg), "%s: lookup column '%s' not found", op, name);
            set_write_error(side, msg);
            goto fail;
        }
    }
    st->left_cols = left_cols;
    st->right_cols = right_cols;
    st->n_key_cols = n;
    return TF_OK;

fail:
    free(left_cols);
    free(right_cols);
    return TF_ERROR;
}

static int load_lookup(set_state *st, const tf_batch *left, tf_side_channels *side) {
    uint8_t *data = NULL;
    size_t data_len = 0;
    tf_decoder *dec = NULL;
    tf_batch **batches = NULL, **flush_batches = NULL, **all_batches = NULL;
    size_t n_batches = 0, n_flush = 0, total_batches = 0, total_rows = 0;
    const char *op = set_mode_name(st->mode);

    if (read_lookup_file(st, &data, &data_len, side) != TF_OK) goto fail;
    dec = tf_csv_decoder_create(NULL);
    if (!dec) goto fail;
    if (data_len > 0 && dec->decode(dec, data, data_len, &batches, &n_batches, NULL) != TF_OK) goto fail;
    if (dec->flush(dec, &flush_batches, &n_flush, NULL) != TF_OK) goto fail;

    total_batches = n_batches + n_flush;
    all_batches = tf_mallocarray_checked(total_batches ? total_batches : 1, sizeof(tf_batch *));
    if (!all_batches) goto fail;
    for (size_t i = 0; i < n_batches; i++) {
        all_batches[i] = batches[i];
        total_rows += batches[i]->n_rows;
        if (st->max_lookup_rows > 0 && total_rows > st->max_lookup_rows) {
            set_limit_error(side, op, "max_lookup_rows", st->max_lookup_rows, total_rows);
            goto fail;
        }
    }
    for (size_t i = 0; i < n_flush; i++) {
        all_batches[n_batches + i] = flush_batches[i];
        total_rows += flush_batches[i]->n_rows;
        if (st->max_lookup_rows > 0 && total_rows > st->max_lookup_rows) {
            set_limit_error(side, op, "max_lookup_rows", st->max_lookup_rows, total_rows);
            goto fail;
        }
    }

    tf_batch *schema = total_batches > 0 ? all_batches[0] : NULL;
    if (prepare_key_columns(st, left, schema, data, data_len, side) != TF_OK) goto fail;
    if (set_map_init(&st->lookup, total_rows ? total_rows : 1) != TF_OK) goto fail;
    if (st->mode == 0 || st->mode == 1) {
        if (set_map_init(&st->emitted, st->max_output_keys ? st->max_output_keys : 64) != TF_OK) goto fail;
    }

    for (size_t b = 0; b < total_batches; b++) {
        for (size_t r = 0; r < all_batches[b]->n_rows; r++) {
            char *key = format_set_key(all_batches[b], r, st->right_cols, st->n_key_cols);
            if (!key) goto fail;
            int inserted = 0;
            int rc = (st->mode == 4 || st->mode == 5)
                ? set_map_increment_owned(&st->lookup, key, st->max_lookup_keys, op, "max_lookup_keys", side, &inserted)
                : set_map_insert_owned(&st->lookup, key, st->max_lookup_keys, op, "max_lookup_keys", side, &inserted);
            if (rc != TF_OK) goto fail;
            if (inserted && set_check_state_bytes(st, side) != TF_OK) goto fail;
        }
    }

    for (size_t i = 0; i < total_batches; i++) all_batches[i] = NULL;
    free_batch_array(batches, n_batches);
    free_batch_array(flush_batches, n_flush);
    free(all_batches);
    dec->destroy(dec);
    free(data);
    return TF_OK;

fail:
    if (all_batches) {
        for (size_t i = 0; i < total_batches; i++) tf_batch_free(all_batches[i]);
        free(all_batches);
        if (batches) for (size_t i = 0; i < n_batches; i++) batches[i] = NULL;
        if (flush_batches) for (size_t i = 0; i < n_flush; i++) flush_batches[i] = NULL;
    }
    free_batch_array(batches, n_batches);
    free_batch_array(flush_batches, n_flush);
    if (dec) dec->destroy(dec);
    free(data);
    return TF_ERROR;
}

static int set_process(tf_step *self, tf_batch *in, tf_batch **out,
                       tf_side_channels *side) {
    set_state *st = self->state;
    *out = NULL;
    const char *op = set_mode_name(st->mode);

    if (st->use_spill) return set_process_spill(self, in, out, side);
    if (st->sorted) return set_process_sorted(self, in, out, side);

    if (!st->loaded) {
        if (load_lookup(st, in, side) != TF_OK) return TF_ERROR;
        st->loaded = 1;
    }

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows > 0 ? in->n_rows : 16);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++) {
        if (tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    size_t out_row = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        char *key = format_set_key(in, r, st->left_cols, st->n_key_cols);
        if (!key) { tf_batch_free(ob); return TF_ERROR; }
        int keep = 0;
        if (st->mode == 4 || st->mode == 5) {
            int consumed = set_map_decrement_if_present(&st->lookup, key);
            keep = st->mode == 4 ? consumed : !consumed;
            free(key);
        } else {
            int in_lookup = set_map_contains(&st->lookup, key);
            keep = st->mode == 0 ? in_lookup : !in_lookup;
            if (!keep) { free(key); continue; }
            int inserted = 0;
            if (set_map_insert_owned(&st->emitted, key, st->max_output_keys,
                                     op, "max_output_keys", side, &inserted) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (inserted && set_check_state_bytes(st, side) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (!inserted) continue;
        }
        if (!keep) continue;
        if (tf_batch_copy_row(ob, out_row, in, r) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
        if (tf_batch_expose_row(ob, out_row) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
        out_row++;
    }

    if (out_row > 0) *out = ob;
    else tf_batch_free(ob);
    return TF_OK;
}


static int set_copy_cell(tf_batch *dst, size_t dr, size_t dc,
                         const tf_batch *src, size_t sr, int sc,
                         tf_side_channels *side, const char *op) {
    if (sc < 0) return TF_ERROR;
    tf_type dt = dst->col_types[dc];
    tf_type stype = src->col_types[sc];
    if (tf_batch_is_null(src, sr, (size_t)sc)) {
        return tf_batch_set_null(dst, dr, dc);
    }
    if (dt != stype && !(dt == TF_TYPE_FLOAT64 && stype == TF_TYPE_INT64)) {
        char msg[224];
        snprintf(msg, sizeof(msg), "%s: file column '%s' type does not match input schema",
                 op, src->col_names[sc] ? src->col_names[sc] : "");
        set_write_error(side, msg);
        return TF_ERROR;
    }
    switch (dt) {
        case TF_TYPE_BOOL:
            return tf_batch_set_bool(dst, dr, dc, tf_batch_get_bool(src, sr, (size_t)sc));
        case TF_TYPE_INT64:
            return tf_batch_set_int64(dst, dr, dc, tf_batch_get_int64(src, sr, (size_t)sc));
        case TF_TYPE_FLOAT64:
            if (stype == TF_TYPE_INT64)
                return tf_batch_set_float64(dst, dr, dc, (double)tf_batch_get_int64(src, sr, (size_t)sc));
            return tf_batch_set_float64(dst, dr, dc, tf_batch_get_float64(src, sr, (size_t)sc));
        case TF_TYPE_STRING:
            return tf_batch_set_string(dst, dr, dc, tf_batch_get_string(src, sr, (size_t)sc));
        case TF_TYPE_DATE:
            return tf_batch_set_date(dst, dr, dc, tf_batch_get_date(src, sr, (size_t)sc));
        case TF_TYPE_TIMESTAMP:
            return tf_batch_set_timestamp(dst, dr, dc, tf_batch_get_timestamp(src, sr, (size_t)sc));
        default:
            return tf_batch_set_null(dst, dr, dc);
    }
}

static int set_spill_capture_union_right_schema(set_state *st, const tf_batch *b,
                                                tf_side_channels *side) {
    const char *op = set_mode_name(st->mode);
    if (st->union_right_cols) return TF_OK;
    int *union_right_cols = tf_callocarray_checked(st->spill_n_cols ? st->spill_n_cols : 1, sizeof(int));
    int *key_right_cols = tf_callocarray_checked(st->n_key_cols ? st->n_key_cols : 1, sizeof(int));
    if (!union_right_cols || !key_right_cols) {
        free(union_right_cols);
        free(key_right_cols);
        return TF_ERROR;
    }
    for (size_t c = 0; c < st->spill_n_cols; c++) {
        int rc = tf_batch_col_index(b, st->spill_schema_names[c]);
        if (rc < 0) {
            char msg[192];
            snprintf(msg, sizeof(msg), "%s: file column '%s' not found", op, st->spill_schema_names[c]);
            set_write_error(side, msg);
            goto fail;
        }
        if (b->col_types[rc] != st->spill_schema_types[c] &&
            !(st->spill_schema_types[c] == TF_TYPE_FLOAT64 && b->col_types[rc] == TF_TYPE_INT64)) {
            char msg[224];
            snprintf(msg, sizeof(msg), "%s: file column '%s' type does not match input schema",
                     op, st->spill_schema_names[c]);
            set_write_error(side, msg);
            goto fail;
        }
        union_right_cols[c] = rc;
    }
    for (size_t k = 0; k < st->n_key_cols; k++) {
        int rc = tf_batch_col_index(b, st->key_names[k]);
        if (rc < 0) {
            char msg[192];
            snprintf(msg, sizeof(msg), "%s: file column '%s' not found", op, st->key_names[k]);
            set_write_error(side, msg);
            goto fail;
        }
        key_right_cols[k] = rc;
    }
    st->union_right_cols = union_right_cols;
    for (size_t k = 0; k < st->n_key_cols; k++) st->right_cols[k] = key_right_cols[k];
    free(key_right_cols);
    return TF_OK;

fail:
    free(union_right_cols);
    free(key_right_cols);
    return TF_ERROR;
}

static int set_spill_process_union_file_batch(set_state *st, const tf_batch *batch,
                                              tf_side_channels *side) {
    const char *op = set_mode_name(st->mode);
    if (set_spill_capture_union_right_schema(st, batch, side) != TF_OK) return TF_ERROR;
    for (size_t r = 0; r < batch->n_rows; r++) {
        if (st->max_lookup_rows > 0 && st->union_file_rows >= st->max_lookup_rows) {
            set_limit_error(side, op, "max_lookup_rows", st->max_lookup_rows, st->union_file_rows + 1);
            return TF_ERROR;
        }
        size_t dst = st->spill_left_buf->n_rows;
        if (tf_batch_ensure_capacity(st->spill_left_buf, dst + 1) != TF_OK) return TF_ERROR;
        for (size_t c = 0; c < st->spill_n_cols; c++) {
            if (set_copy_cell(st->spill_left_buf, dst, c, batch, r, st->union_right_cols[c], side, op) != TF_OK)
                return TF_ERROR;
        }
        if (set_ensure_ordinals(&st->spill_left_ordinals, &st->spill_left_ordinal_cap, dst + 1) != TF_OK) return TF_ERROR;
        st->spill_left_ordinals[dst] = st->spill_next_left_ordinal++;
        if (tf_batch_expose_row(st->spill_left_buf, dst) != TF_OK) return TF_ERROR;
        st->union_file_rows++;
        if (st->spill_left_buf->n_rows >= st->run_rows && set_spill_write_left_run(st) != TF_OK)
            return TF_ERROR;
    }
    return TF_OK;
}

static int set_spill_load_union_file_runs(set_state *st, tf_side_channels *side) {
    if (st->spill_lookup_loaded) return TF_OK;
    st->spill_lookup_loaded = 1;
    FILE *f = fopen(st->file, "rb");
    if (!f) { set_write_error(side, "union: cannot open file"); return TF_ERROR; }
    tf_decoder *dec = tf_csv_decoder_create(NULL);
    if (!dec) { fclose(f); return TF_ERROR; }
    size_t bytes_read = 0;
    int flushed = 0;
    int rc = TF_OK;
    while (!flushed) {
        uint8_t buf[64 * 1024];
        size_t n = fread(buf, 1, sizeof(buf), f);
        tf_batch **batches = NULL;
        size_t n_batches = 0;
        if (n > 0) {
            bytes_read += n;
            if (st->max_lookup_bytes > 0 && bytes_read > st->max_lookup_bytes) {
                set_limit_error(side, "union", "max_lookup_bytes", st->max_lookup_bytes, bytes_read);
                rc = TF_ERROR;
                break;
            }
            rc = dec->decode(dec, buf, n, &batches, &n_batches, side);
        } else {
            if (ferror(f)) {
                set_write_error(side, "union: failed reading file");
                rc = TF_ERROR;
                break;
            }
            flushed = 1;
            rc = dec->flush(dec, &batches, &n_batches, side);
        }
        if (rc != TF_OK) { free_batch_array(batches, n_batches); break; }
        for (size_t i = 0; i < n_batches; i++) {
            if (set_spill_process_union_file_batch(st, batches[i], side) != TF_OK) rc = TF_ERROR;
            tf_batch_free(batches[i]);
            if (rc != TF_OK) {
                for (size_t j = i + 1; j < n_batches; j++) tf_batch_free(batches[j]);
                break;
            }
        }
        free(batches);
        if (rc != TF_OK) break;
    }
    dec->destroy(dec);
    fclose(f);
    return rc == TF_OK ? TF_OK : TF_ERROR;
}

static int set_spill_produce_union_output_runs(set_state *st, tf_side_channels *side) {
    if (st->spill_key_merge_done) return TF_OK;
    if (!st->spill_has_schema) { st->spill_key_merge_done = 1; return TF_OK; }

    if (set_spill_load_union_file_runs(st, side) != TF_OK) return TF_ERROR;
    if (st->spill_left_buf && st->spill_left_buf->n_rows > 0 && set_spill_write_left_run(st) != TF_OK) return TF_ERROR;
    if (st->spill_left_buf) { tf_batch_free(st->spill_left_buf); st->spill_left_buf = NULL; }
    free(st->spill_left_ordinals); st->spill_left_ordinals = NULL; st->spill_left_ordinal_cap = 0;

    if (set_spill_open_readers(st, 0) != TF_OK) return TF_ERROR;
    for (;;) {
        int left_idx = set_spill_best_reader(st, 0);
        if (left_idx < 0) break;
        set_spill_reader *row = &st->spill_left_readers[left_idx];
        char *key = set_spill_build_key(st, &row->row, 0);
        if (!key) return TF_ERROR;
        int duplicate = st->spill_last_left_key && strcmp(st->spill_last_left_key, key) == 0;
        if (!duplicate) {
            free(st->spill_last_left_key);
            st->spill_last_left_key = key;
            key = NULL;
            if (st->max_output_keys > 0 && st->spill_distinct_rows >= st->max_output_keys) {
                set_limit_error(side, "union", "max_output_keys", st->max_output_keys, st->spill_distinct_rows + 1);
                return TF_ERROR;
            }
            if (set_spill_append_selected_row(st, &row->row) != TF_OK) return TF_ERROR;
        }
        free(key);
        int adv = set_spill_reader_advance(st, row);
        if (adv < 0) return TF_ERROR;
    }
    if (st->spill_out_buf && st->spill_out_buf->n_rows > 0 && set_spill_write_output_run(st) != TF_OK) return TF_ERROR;
    set_spill_close_readers(st, 0);
    set_spill_remove_paths(&st->spill_left_run_paths, &st->spill_n_left_runs, &st->spill_cap_left_runs);
    st->spill_key_merge_done = 1;
    return TF_OK;
}

static tf_batch *set_create_output_like_union_schema(const set_state *st, size_t rows) {
    tf_batch *ob = tf_batch_create(st->union_n_cols, rows > 0 ? rows : 1);
    if (!ob) return NULL;
    for (size_t c = 0; c < st->union_n_cols; c++) {
        if (tf_batch_set_schema(ob, c, st->union_col_names[c], st->union_col_types[c]) != TF_OK) {
            tf_batch_free(ob);
            return NULL;
        }
    }
    return ob;
}

static int union_capture_left_schema(set_state *st, const tf_batch *in,
                                     tf_side_channels *side) {
    const char *op = set_mode_name(st->mode);
    if (st->union_schema_ready) return TF_OK;

    size_t union_n_cols = in->n_cols;
    char **union_col_names = tf_callocarray_checked(union_n_cols ? union_n_cols : 1, sizeof(char *));
    tf_type *union_col_types = tf_callocarray_checked(union_n_cols ? union_n_cols : 1, sizeof(tf_type));
    int *left_cols = NULL;
    int *right_cols = NULL;
    char **key_names = NULL;
    size_t n_key_cols = 0;
    set_hash_map emitted = {0};

    if (!union_col_names || !union_col_types) goto fail;
    for (size_t c = 0; c < union_n_cols; c++) {
        union_col_names[c] = strdup(in->col_names[c] ? in->col_names[c] : "");
        if (!union_col_names[c]) goto fail;
        union_col_types[c] = in->col_types[c];
    }

    if (st->mode == 2) {
        size_t n = st->n_columns > 0 ? st->n_columns : in->n_cols;
        left_cols = tf_callocarray_checked(n ? n : 1, sizeof(int));
        right_cols = tf_callocarray_checked(n ? n : 1, sizeof(int));
        key_names = tf_callocarray_checked(n ? n : 1, sizeof(char *));
        if (!left_cols || !right_cols || !key_names) goto fail;
        n_key_cols = n;
        for (size_t i = 0; i < n; i++) {
            const char *name = st->n_columns > 0 ? st->columns[i] : in->col_names[i];
            int lc = tf_batch_col_index(in, name);
            if (lc < 0) {
                char msg[192];
                snprintf(msg, sizeof(msg), "%s: input column '%s' not found", op, name ? name : "");
                set_write_error(side, msg);
                goto fail;
            }
            left_cols[i] = lc;
            right_cols[i] = -1;
            key_names[i] = strdup(name ? name : "");
            if (!key_names[i]) goto fail;
        }
        if (set_map_init(&emitted, st->max_output_keys ? st->max_output_keys : 64) != TF_OK)
            goto fail;
    }

    st->union_n_cols = union_n_cols;
    st->union_col_names = union_col_names;
    st->union_col_types = union_col_types;
    st->left_cols = left_cols;
    st->right_cols = right_cols;
    st->key_names = key_names;
    st->n_key_cols = n_key_cols;
    st->emitted = emitted;
    st->union_schema_ready = 1;
    return TF_OK;

fail:
    set_free_string_array(union_col_names, union_n_cols);
    free(union_col_types);
    free(left_cols);
    free(right_cols);
    set_free_string_array(key_names, n_key_cols);
    set_map_free(&emitted);
    return TF_ERROR;
}

static int union_capture_right_schema(set_state *st, const tf_batch *b,
                                      tf_side_channels *side) {
    const char *op = set_mode_name(st->mode);
    if (st->union_right_cols) return TF_OK;
    int *union_right_cols = tf_callocarray_checked(st->union_n_cols ? st->union_n_cols : 1, sizeof(int));
    int *key_right_cols = NULL;
    if (st->mode == 2) {
        key_right_cols = tf_callocarray_checked(st->n_key_cols ? st->n_key_cols : 1, sizeof(int));
    }
    if (!union_right_cols || (st->mode == 2 && !key_right_cols)) {
        free(union_right_cols);
        free(key_right_cols);
        return TF_ERROR;
    }
    for (size_t c = 0; c < st->union_n_cols; c++) {
        int rc = tf_batch_col_index(b, st->union_col_names[c]);
        if (rc < 0) {
            char msg[192];
            snprintf(msg, sizeof(msg), "%s: file column '%s' not found", op, st->union_col_names[c]);
            set_write_error(side, msg);
            goto fail;
        }
        if (b->col_types[rc] != st->union_col_types[c] &&
            !(st->union_col_types[c] == TF_TYPE_FLOAT64 && b->col_types[rc] == TF_TYPE_INT64)) {
            char msg[224];
            snprintf(msg, sizeof(msg), "%s: file column '%s' type does not match input schema",
                     op, st->union_col_names[c]);
            set_write_error(side, msg);
            goto fail;
        }
        union_right_cols[c] = rc;
    }
    if (st->mode == 2) {
        for (size_t i = 0; i < st->n_key_cols; i++) {
            int rc = tf_batch_col_index(b, st->key_names[i]);
            if (rc < 0) {
                char msg[192];
                snprintf(msg, sizeof(msg), "%s: file column '%s' not found", op, st->key_names[i]);
                set_write_error(side, msg);
                goto fail;
            }
            key_right_cols[i] = rc;
        }
    }
    st->union_right_cols = union_right_cols;
    if (st->mode == 2) {
        for (size_t i = 0; i < st->n_key_cols; i++) st->right_cols[i] = key_right_cols[i];
    }
    free(key_right_cols);
    return TF_OK;

fail:
    free(union_right_cols);
    free(key_right_cols);
    return TF_ERROR;
}

static int union_sorted_next_batch(set_state *st, tf_side_channels *side) {
    if (st->sorted_current) {
        tf_batch_free(st->sorted_current);
        st->sorted_current = NULL;
    }
    for (;;) {
        tf_batch *b = NULL;
        if (union_next_decoded_batch(st, &b, side) != TF_OK) return TF_ERROR;
        if (!b) {
            st->sorted_exhausted = 1;
            return TF_OK;
        }
        if (union_capture_right_schema(st, b, side) != TF_OK) {
            tf_batch_free(b);
            return TF_ERROR;
        }
        st->sorted_current = b;
        st->sorted_row = 0;
        if (b->n_rows > 0) return TF_OK;
        tf_batch_free(st->sorted_current);
        st->sorted_current = NULL;
    }
}

static int union_sorted_load_row_if_needed(set_state *st, tf_side_channels *side) {
    if (st->sorted_have_row) return TF_OK;
    for (;;) {
        if (!st->sorted_current || st->sorted_row >= st->sorted_current->n_rows) {
            if (union_sorted_next_batch(st, side) != TF_OK) return TF_ERROR;
            if (st->sorted_exhausted) return TF_OK;
            continue;
        }

        set_key_tuple cur = {0};
        if (set_key_tuple_set_from_row(&cur, st->sorted_current, st->sorted_row,
                                       st->right_cols, st->n_key_cols) != TF_OK) {
            set_key_tuple_clear(&cur);
            return TF_ERROR;
        }
        if (st->prev_lookup_key.valid) {
            int cmp = 0;
            if (set_key_tuple_compare(&st->prev_lookup_key, &cur, &cmp) != TF_OK) {
                set_key_tuple_clear(&cur);
                set_write_error(side, "union: file key types changed");
                return TF_ERROR;
            }
            if (cmp > 0) {
                set_key_tuple_clear(&cur);
                set_write_error(side, "union: file side is not sorted by key");
                return TF_ERROR;
            }
        }
        if (set_key_tuple_copy(&st->prev_lookup_key, &cur) != TF_OK) {
            set_key_tuple_clear(&cur);
            return TF_ERROR;
        }
        set_key_tuple_clear(&cur);
        st->sorted_have_row = 1;
        return TF_OK;
    }
}

static void union_sorted_consume_row(set_state *st) {
    if (!st || !st->sorted_have_row) return;
    st->sorted_row++;
    st->sorted_have_row = 0;
}

static void union_sorted_clear_right_row(set_state *st) {
    if (st && st->union_sorted_right_row) {
        tf_batch_free(st->union_sorted_right_row);
        st->union_sorted_right_row = NULL;
    }
}

static int union_sorted_load_next_right_key(set_state *st, tf_side_channels *side) {
    set_key_tuple_clear(&st->current_lookup_key);
    union_sorted_clear_right_row(st);
    if (union_sorted_load_row_if_needed(st, side) != TF_OK) return TF_ERROR;
    if (st->sorted_exhausted && !st->sorted_have_row) return TF_OK;

    if (set_key_tuple_set_from_row(&st->current_lookup_key, st->sorted_current,
                                   st->sorted_row, st->right_cols, st->n_key_cols) != TF_OK)
        return TF_ERROR;

    st->union_sorted_right_row = set_create_output_like_union_schema(st, 1);
    if (!st->union_sorted_right_row) return TF_ERROR;
    for (size_t c = 0; c < st->union_n_cols; c++) {
        if (set_copy_cell(st->union_sorted_right_row, 0, c, st->sorted_current,
                          st->sorted_row, st->union_right_cols[c], side, "union") != TF_OK)
            return TF_ERROR;
    }
    st->union_sorted_right_row->n_rows = 1;

    while (st->sorted_have_row) {
        set_key_tuple row_key = {0};
        if (set_key_tuple_set_from_row(&row_key, st->sorted_current, st->sorted_row,
                                       st->right_cols, st->n_key_cols) != TF_OK) {
            set_key_tuple_clear(&row_key);
            return TF_ERROR;
        }
        int cmp = 0;
        if (set_key_tuple_compare(&st->current_lookup_key, &row_key, &cmp) != TF_OK) {
            set_key_tuple_clear(&row_key);
            set_write_error(side, "union: file key types changed");
            return TF_ERROR;
        }
        set_key_tuple_clear(&row_key);
        if (cmp != 0) break;
        st->union_file_rows++;
        if (st->max_lookup_rows > 0 && st->union_file_rows > st->max_lookup_rows) {
            set_limit_error(side, "union", "max_lookup_rows", st->max_lookup_rows, st->union_file_rows);
            return TF_ERROR;
        }
        union_sorted_consume_row(st);
        if (union_sorted_load_row_if_needed(st, side) != TF_OK) return TF_ERROR;
        if (st->sorted_exhausted && !st->sorted_have_row) break;
    }
    return TF_OK;
}

static int union_sorted_ensure_right_key(set_state *st, tf_side_channels *side) {
    if (!st->current_lookup_key.valid && !st->sorted_exhausted)
        return union_sorted_load_next_right_key(st, side);
    return TF_OK;
}

static int union_sorted_note_emit(set_state *st, tf_side_channels *side) {
    if (st->max_output_keys > 0 && st->emitted.count >= st->max_output_keys) {
        set_limit_error(side, "union", "max_output_keys", st->max_output_keys, st->emitted.count + 1);
        return TF_ERROR;
    }
    st->emitted.count++;
    return TF_OK;
}

static int union_sorted_append_left(set_state *st, tf_batch *ob, size_t *out_row,
                                    const tf_batch *in, size_t row,
                                    tf_side_channels *side) {
    if (union_sorted_note_emit(st, side) != TF_OK) return TF_ERROR;
    if (tf_batch_ensure_capacity(ob, *out_row + 1) != TF_OK) return TF_ERROR;
    if (tf_batch_copy_row(ob, *out_row, in, row) != TF_OK) return TF_ERROR;
    if (tf_batch_expose_row(ob, *out_row) != TF_OK) return TF_ERROR;
    (*out_row)++;
    return TF_OK;
}

static int union_sorted_append_right(set_state *st, tf_batch *ob, size_t *out_row,
                                     tf_side_channels *side) {
    if (!st->union_sorted_right_row) return TF_ERROR;
    if (union_sorted_note_emit(st, side) != TF_OK) return TF_ERROR;
    if (tf_batch_ensure_capacity(ob, *out_row + 1) != TF_OK) return TF_ERROR;
    if (tf_batch_copy_row(ob, *out_row, st->union_sorted_right_row, 0) != TF_OK) return TF_ERROR;
    if (tf_batch_expose_row(ob, *out_row) != TF_OK) return TF_ERROR;
    (*out_row)++;
    return TF_OK;
}

static int union_process_sorted(tf_step *self, tf_batch *in, tf_batch **out,
                                tf_side_channels *side) {
    set_state *st = self->state;
    *out = NULL;
    if (union_capture_left_schema(st, in, side) != TF_OK) return TF_ERROR;

    tf_batch *ob = set_create_output_like_union_schema(st, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) return TF_ERROR;
    size_t out_row = 0;

    for (size_t r = 0; r < in->n_rows; r++) {
        set_key_tuple left_key = {0};
        if (set_key_tuple_set_from_row(&left_key, in, r, st->left_cols, st->n_key_cols) != TF_OK) {
            set_key_tuple_clear(&left_key);
            tf_batch_free(ob);
            return TF_ERROR;
        }
        int duplicate_left = 0;
        if (st->prev_left_key.valid) {
            int cmp = 0;
            if (set_key_tuple_compare(&st->prev_left_key, &left_key, &cmp) != TF_OK) {
                set_key_tuple_clear(&left_key);
                tf_batch_free(ob);
                set_write_error(side, "union: left key types changed");
                return TF_ERROR;
            }
            if (cmp > 0) {
                set_key_tuple_clear(&left_key);
                tf_batch_free(ob);
                set_write_error(side, "union: left side is not sorted by key");
                return TF_ERROR;
            }
            duplicate_left = (cmp == 0);
        }
        if (set_key_tuple_copy(&st->prev_left_key, &left_key) != TF_OK) {
            set_key_tuple_clear(&left_key);
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (duplicate_left) {
            set_key_tuple_clear(&left_key);
            continue;
        }

        if (union_sorted_ensure_right_key(st, side) != TF_OK) {
            set_key_tuple_clear(&left_key);
            tf_batch_free(ob);
            return TF_ERROR;
        }
        while (st->current_lookup_key.valid) {
            int cmp = 0;
            if (set_key_tuple_compare(&st->current_lookup_key, &left_key, &cmp) != TF_OK) {
                set_key_tuple_clear(&left_key);
                tf_batch_free(ob);
                set_write_error(side, "union: input and file key types differ");
                return TF_ERROR;
            }
            if (cmp < 0) {
                if (union_sorted_append_right(st, ob, &out_row, side) != TF_OK) {
                    set_key_tuple_clear(&left_key);
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                if (union_sorted_load_next_right_key(st, side) != TF_OK) {
                    set_key_tuple_clear(&left_key);
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                continue;
            }
            if (cmp == 0) {
                if (union_sorted_append_left(st, ob, &out_row, in, r, side) != TF_OK) {
                    set_key_tuple_clear(&left_key);
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                if (union_sorted_load_next_right_key(st, side) != TF_OK) {
                    set_key_tuple_clear(&left_key);
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                set_key_tuple_clear(&left_key);
                goto next_left_row;
            }
            break;
        }
        if (union_sorted_append_left(st, ob, &out_row, in, r, side) != TF_OK) {
            set_key_tuple_clear(&left_key);
            tf_batch_free(ob);
            return TF_ERROR;
        }
        set_key_tuple_clear(&left_key);
next_left_row:
        ;
    }

    if (out_row > 0) *out = ob;
    else tf_batch_free(ob);
    return TF_OK;
}

static int union_sorted_flush_next(tf_step *self, tf_batch **out, tf_side_channels *side) {
    set_state *st = self->state;
    *out = NULL;
    if (!st->union_schema_ready) return TF_OK;
    size_t cap = st->output_batch_rows > 0 ? st->output_batch_rows : SET_DEFAULT_OUTPUT_ROWS;
    tf_batch *ob = set_create_output_like_union_schema(st, cap);
    if (!ob) return TF_ERROR;
    size_t out_row = 0;
    while (out_row < cap) {
        if (union_sorted_ensure_right_key(st, side) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (!st->current_lookup_key.valid) break;
        if (union_sorted_append_right(st, ob, &out_row, side) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (union_sorted_load_next_right_key(st, side) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    if (out_row > 0) *out = ob;
    else tf_batch_free(ob);
    return TF_OK;
}

static int union_process(tf_step *self, tf_batch *in, tf_batch **out,
                         tf_side_channels *side) {
    set_state *st = self->state;
    const char *op = set_mode_name(st->mode);
    *out = NULL;
    if (st->use_spill) return set_process_spill(self, in, out, side);
    if (st->sorted) return union_process_sorted(self, in, out, side);
    if (union_capture_left_schema(st, in, side) != TF_OK) return TF_ERROR;

    tf_batch *ob = set_create_output_like_union_schema(st, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) return TF_ERROR;
    size_t out_row = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        if (st->mode == 2) {
            char *key = format_set_key(in, r, st->left_cols, st->n_key_cols);
            if (!key) { tf_batch_free(ob); return TF_ERROR; }
            int inserted = 0;
            if (set_map_insert_owned(&st->emitted, key, st->max_output_keys,
                                     op, "max_output_keys", side, &inserted) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (inserted && set_check_state_bytes(st, side) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (!inserted) continue;
        }
        if (tf_batch_copy_row(ob, out_row, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, out_row) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        out_row++;
    }
    if (out_row > 0) *out = ob;
    else tf_batch_free(ob);
    return TF_OK;
}

static int union_open_file(set_state *st, tf_side_channels *side) {
    const char *op = set_mode_name(st->mode);
    if (st->sorted_file || st->sorted_decoder) return TF_OK;
    st->sorted_file = fopen(st->file, "rb");
    if (!st->sorted_file) {
        char msg[160];
        snprintf(msg, sizeof(msg), "%s: cannot open file", op);
        set_write_error(side, msg);
        return TF_ERROR;
    }
    if (st->max_lookup_bytes > 0) {
        if (fseek(st->sorted_file, 0, SEEK_END) == 0) {
            long sz = ftell(st->sorted_file);
            if (sz >= 0 && (size_t)sz > st->max_lookup_bytes) {
                set_limit_error(side, op, "max_lookup_bytes", st->max_lookup_bytes, (size_t)sz);
                return TF_ERROR;
            }
            fseek(st->sorted_file, 0, SEEK_SET);
        }
    }
    st->sorted_decoder = tf_csv_decoder_create(NULL);
    if (!st->sorted_decoder) return TF_ERROR;
    return TF_OK;
}

static int union_next_decoded_batch(set_state *st, tf_batch **out,
                                    tf_side_channels *side) {
    const char *op = set_mode_name(st->mode);
    *out = NULL;
    for (;;) {
        if (st->sorted_batches && st->sorted_batch_index < st->sorted_n_batches) {
            *out = st->sorted_batches[st->sorted_batch_index++];
            return TF_OK;
        }
        sorted_set_free_pending_batches(st);
        if (st->sorted_flushed) {
            st->sorted_exhausted = 1;
            return TF_OK;
        }
        if (union_open_file(st, side) != TF_OK) return TF_ERROR;
        uint8_t buf[64 * 1024];
        size_t n = fread(buf, 1, sizeof(buf), st->sorted_file);
        tf_batch **batches = NULL;
        size_t n_batches = 0;
        int rc = TF_OK;
        if (n > 0) {
            rc = st->sorted_decoder->decode(st->sorted_decoder, buf, n, &batches, &n_batches, side);
        } else {
            if (ferror(st->sorted_file)) {
                char msg[160];
                snprintf(msg, sizeof(msg), "%s: failed reading file", op);
                set_write_error(side, msg);
                return TF_ERROR;
            }
            st->sorted_flushed = 1;
            rc = st->sorted_decoder->flush(st->sorted_decoder, &batches, &n_batches, side);
        }
        if (rc != TF_OK) return TF_ERROR;
        st->sorted_batches = batches;
        st->sorted_n_batches = n_batches;
        st->sorted_batch_index = 0;
        if (n_batches == 0) {
            free(batches);
            st->sorted_batches = NULL;
            if (st->sorted_flushed) {
                st->sorted_exhausted = 1;
                return TF_OK;
            }
        }
    }
}

static int union_flush_next(tf_step *self, tf_batch **out, tf_side_channels *side) {
    set_state *st = self->state;
    const char *op = set_mode_name(st->mode);
    *out = NULL;
    if (st->use_spill) return set_spill_output_next_batch(st, out, side);
    if (st->sorted) return union_sorted_flush_next(self, out, side);
    if (!st->union_schema_ready) return TF_OK;

    for (;;) {
        tf_batch *b = NULL;
        if (union_next_decoded_batch(st, &b, side) != TF_OK) return TF_ERROR;
        if (!b) return TF_OK;
        if (union_capture_right_schema(st, b, side) != TF_OK) { tf_batch_free(b); return TF_ERROR; }

        tf_batch *ob = set_create_output_like_union_schema(st, b->n_rows > 0 ? b->n_rows : 1);
        if (!ob) { tf_batch_free(b); return TF_ERROR; }
        size_t out_row = 0;
        for (size_t r = 0; r < b->n_rows; r++) {
            st->union_file_rows++;
            if (st->max_lookup_rows > 0 && st->union_file_rows > st->max_lookup_rows) {
                set_limit_error(side, op, "max_lookup_rows", st->max_lookup_rows, st->union_file_rows);
                tf_batch_free(ob);
                tf_batch_free(b);
                return TF_ERROR;
            }
            if (st->mode == 2) {
                char *key = format_set_key(b, r, st->right_cols, st->n_key_cols);
                if (!key) { tf_batch_free(ob); tf_batch_free(b); return TF_ERROR; }
                int inserted = 0;
                if (set_map_insert_owned(&st->emitted, key, st->max_output_keys,
                                         op, "max_output_keys", side, &inserted) != TF_OK) {
                    tf_batch_free(ob);
                    tf_batch_free(b);
                    return TF_ERROR;
                }
                if (inserted && set_check_state_bytes(st, side) != TF_OK) {
                    tf_batch_free(ob);
                    tf_batch_free(b);
                    return TF_ERROR;
                }
                if (!inserted) continue;
            }
            if (tf_batch_ensure_capacity(ob, out_row + 1) != TF_OK) {
                tf_batch_free(ob);
                tf_batch_free(b);
                return TF_ERROR;
            }
            for (size_t c = 0; c < st->union_n_cols; c++) {
                if (set_copy_cell(ob, out_row, c, b, r, st->union_right_cols[c], side, op) != TF_OK) {
                    tf_batch_free(ob);
                    tf_batch_free(b);
                    return TF_ERROR;
                }
            }
            if (tf_batch_expose_row(ob, out_row) != TF_OK) {
                tf_batch_free(ob);
                tf_batch_free(b);
                return TF_ERROR;
            }
            out_row++;
        }
        tf_batch_free(b);
        if (out_row > 0) { *out = ob; return TF_OK; }
        tf_batch_free(ob);
    }
}

static int union_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    return union_flush_next(self, out, side);
}

static size_t set_map_retained_bytes(const set_hash_map *m) {
    if (!m || !m->buckets) return 0;
    return m->key_bytes + m->n_buckets * sizeof(set_bucket);
}

static size_t set_retained_state_bytes(const set_state *st) {
    if (!st) return 0;
    if (st->use_spill) {
        size_t bytes = 0;
        if (st->spill_left_buf) bytes += st->spill_left_buf->capacity * (sizeof(uint64_t) + 16);
        if (st->spill_lookup_buf) bytes += st->spill_lookup_buf->capacity * (sizeof(uint64_t) + 16);
        if (st->spill_out_buf) bytes += st->spill_out_buf->capacity * (sizeof(uint64_t) + 16);
        bytes += st->spill_n_left_readers * sizeof(set_spill_reader);
        bytes += st->spill_n_lookup_readers * sizeof(set_spill_reader);
        bytes += st->spill_n_out_readers * sizeof(set_spill_reader);
        bytes += st->spill_last_lookup_key ? strlen(st->spill_last_lookup_key) + 1 : 0;
        bytes += st->spill_last_left_key ? strlen(st->spill_last_left_key) + 1 : 0;
        return bytes;
    }
    return set_map_retained_bytes(&st->lookup) + set_map_retained_bytes(&st->emitted);
}

static int set_check_state_bytes(const set_state *st, tf_side_channels *side) {
    if (!st || st->max_state_bytes == 0 || st->sorted || st->mode == 3 || st->use_spill) return TF_OK;
    size_t retained = set_retained_state_bytes(st);
    if (retained <= st->max_state_bytes) return TF_OK;

    char msg[256];
    snprintf(msg, sizeof(msg),
             "%s: max_state_bytes=%zu exceeded while tracking set keys (%zu bytes retained)",
             set_mode_name(st->mode), st->max_state_bytes, retained);
    if (set_write_error(side, msg) != TF_OK) return TF_ERROR;
    return TF_ERROR;
}

static int set_append_stats(tf_step *self, tf_buffer *out) {
    if (!self || !self->state || !out) return TF_ERROR;
    set_state *st = self->state;
    int bag_mode = st->mode == 4 || st->mode == 5;
    size_t lookup_keys = st->use_spill ? st->spill_lookup_keys : st->lookup.count;
    size_t lookup_key_bytes = st->use_spill ? st->spill_lookup_key_bytes : st->lookup.key_bytes;
    size_t emitted_keys = st->use_spill ? (bag_mode ? 0 : st->spill_distinct_rows) : st->emitted.count;
    size_t emitted_key_bytes = st->use_spill ? 0 : st->emitted.key_bytes;
    size_t retained = set_retained_state_bytes(st);
    char buf[512];
    snprintf(buf, sizeof(buf),
             ",\"lookup_keys\":%zu,\"lookup_key_bytes\":%zu,"
             "\"emitted_keys\":%zu,\"emitted_key_bytes\":%zu,"
             "\"retained_state_bytes\":%zu,\"max_state_bytes\":%zu",
             lookup_keys, lookup_key_bytes, emitted_keys, emitted_key_bytes, retained, st->max_state_bytes);
    if (tf_buffer_write_str(out, buf) != TF_OK) return TF_ERROR;
    if (st->use_spill) {
        snprintf(buf, sizeof(buf),
                 ",\"spill_bytes\":%zu,\"spill_runs\":%zu,"
                 "\"spill_output_batches\":%zu,\"spill_output_rows\":%zu",
                 st->spill_bytes, st->spill_runs,
                 st->spill_output_batches, st->spill_output_rows);
        if (tf_buffer_write_str(out, buf) != TF_OK) return TF_ERROR;
        if (bag_mode) {
            snprintf(buf, sizeof(buf), ",\"spill_kept_rows\":%zu", st->spill_kept_rows);
        } else {
            snprintf(buf, sizeof(buf), ",\"spill_distinct_rows\":%zu", st->spill_distinct_rows);
        }
        if (tf_buffer_write_str(out, buf) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static int set_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    set_state *st = self ? self->state : NULL;
    *out = NULL;
    if (!st || !st->use_spill) return TF_OK;
    return set_spill_output_next_batch(st, out, side);
}

static int set_flush_next(tf_step *self, tf_batch **out, tf_side_channels *side) {
    set_state *st = self ? self->state : NULL;
    *out = NULL;
    if (!st || !st->use_spill) return TF_OK;
    return set_spill_output_next_batch(st, out, side);
}

static void set_state_free(set_state *st) {
    if (!st) return;
    free(st->file);
    if (st->columns) {
        for (size_t i = 0; i < st->n_columns; i++) free(st->columns[i]);
        free(st->columns);
    }
    if (st->key_names) {
        for (size_t i = 0; i < st->n_key_cols; i++) free(st->key_names[i]);
        free(st->key_names);
    }
    union_sorted_clear_right_row(st);
    if (st->union_col_names) {
        for (size_t i = 0; i < st->union_n_cols; i++) free(st->union_col_names[i]);
        free(st->union_col_names);
    }
    free(st->union_col_types);
    free(st->union_right_cols);
    set_spill_close_readers(st, 0);
    set_spill_close_readers(st, 1);
    set_spill_close_readers(st, 2);
    if (st->spill_schema_names) {
        for (size_t i = 0; i < st->spill_n_cols; i++) free(st->spill_schema_names[i]);
        free(st->spill_schema_names);
    }
    free(st->spill_schema_types);
    free(st->spill_key_types);
    if (st->spill_left_buf) tf_batch_free(st->spill_left_buf);
    if (st->spill_lookup_buf) tf_batch_free(st->spill_lookup_buf);
    if (st->spill_out_buf) tf_batch_free(st->spill_out_buf);
    free(st->spill_left_ordinals);
    free(st->spill_lookup_ordinals);
    free(st->spill_out_ordinals);
    set_spill_remove_paths(&st->spill_left_run_paths, &st->spill_n_left_runs, &st->spill_cap_left_runs);
    set_spill_remove_paths(&st->spill_lookup_run_paths, &st->spill_n_lookup_runs, &st->spill_cap_lookup_runs);
    set_spill_remove_paths(&st->spill_out_run_paths, &st->spill_n_out_runs, &st->spill_cap_out_runs);
    free(st->spill_last_lookup_key);
    free(st->spill_last_left_key);
    tf_spill_cleanup(st->spill);
    free(st->spill_dir);
    free(st->left_cols);
    free(st->right_cols);
    set_map_free(&st->lookup);
    set_map_free(&st->emitted);
    sorted_set_close(st);
    set_key_tuple_clear(&st->prev_lookup_key);
    set_key_tuple_clear(&st->current_lookup_key);
    set_key_tuple_clear(&st->prev_left_key);
    free(st);
}

static void set_destroy(tf_step *self) {
    if (self) {
        set_state_free(self->state);
        free(self);
    }
}

static int parse_positive_size_arg(const cJSON *args, const char *name,
                                   size_t max_value,
                                   size_t *out, const char *op_name) {
    return tf_json_get_size_arg(args, name, 1, max_value, out, op_name);
}

static tf_step *set_create_common(const cJSON *args, int mode) {
    if (!args) return NULL;
    const char *op = mode == 0 ? "intersect" : (mode == 1 ? "setdiff" : (mode == 4 ? "intersect-all" : "setdiff-all"));
    cJSON *file_j = cJSON_GetObjectItemCaseSensitive(args, "file");
    if (!cJSON_IsString(file_j) || !file_j->valuestring || !file_j->valuestring[0]) {
        tf_set_last_error("set op: file is required");
        return NULL;
    }

    set_state *st = calloc(1, sizeof(set_state));
    if (!st) return NULL;
    st->mode = mode;
    st->file = strdup(file_j->valuestring);
    if (!st->file) { set_state_free(st); return NULL; }


    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (cols && cJSON_IsArray(cols)) {
        int n = cJSON_GetArraySize(cols);
        st->columns = tf_callocarray_checked(n > 0 ? (size_t)n : 1, sizeof(char *));
        if (!st->columns) { set_state_free(st); return NULL; }
        for (int i = 0; i < n; i++) {
            cJSON *item = cJSON_GetArrayItem(cols, i);
            if (!cJSON_IsString(item) || !item->valuestring || !item->valuestring[0]) {
                tf_set_last_error("set op: columns must be non-empty strings");
                set_state_free(st);
                return NULL;
            }
            st->columns[st->n_columns] = strdup(item->valuestring);
            if (!st->columns[st->n_columns]) { set_state_free(st); return NULL; }
            st->n_columns++;
        }
    }

    cJSON *sorted_j = cJSON_GetObjectItemCaseSensitive(args, "sorted");
    if (sorted_j) {
        if (cJSON_IsTrue(sorted_j)) st->sorted = 1;
        else if (!cJSON_IsFalse(sorted_j)) {
            tf_set_last_error("set op: sorted must be boolean");
            set_state_free(st);
            return NULL;
        }
    }

    if (parse_positive_size_arg(args, "max_lookup_rows", TF_MAX_COUNT_ARG, &st->max_lookup_rows, op) < 0 ||
        parse_positive_size_arg(args, "max_lookup_keys", TF_MAX_COUNT_ARG, &st->max_lookup_keys, op) < 0 ||
        parse_positive_size_arg(args, "max_lookup_bytes", TF_MAX_STATE_BYTES, &st->max_lookup_bytes, op) < 0 ||
        parse_positive_size_arg(args, "max_output_keys", TF_MAX_COUNT_ARG, &st->max_output_keys, op) < 0 ||
        parse_positive_size_arg(args, "max_state_bytes", TF_MAX_STATE_BYTES, &st->max_state_bytes, op) < 0 ||
        parse_positive_size_arg(args, "spill_memory_bytes", TF_MAX_SPILL_MEMORY_BYTES, &st->spill_memory_bytes, op) < 0 ||
        parse_positive_size_arg(args, "spill_run_rows", TF_MAX_SPILL_RUN_ROWS, &st->configured_run_rows, op) < 0 ||
        parse_positive_size_arg(args, "spill_output_rows", TF_MAX_SPILL_OUTPUT_ROWS, &st->output_batch_rows, op) < 0) {
        set_state_free(st);
        return NULL;
    }

    if ((mode == 4 || mode == 5) && st->max_output_keys > 0) {
        tf_set_last_error("set op: max_output_keys is only valid for duplicate-eliminating set ops");
        set_state_free(st);
        return NULL;
    }
    cJSON *spill_j = cJSON_GetObjectItemCaseSensitive(args, "spill_dir");
    if (spill_j) {
        if (!cJSON_IsString(spill_j) || !spill_j->valuestring || !spill_j->valuestring[0]) {
            tf_set_last_error("set op: spill_dir must be a non-empty string");
            set_state_free(st);
            return NULL;
        }
        if (st->sorted) {
            tf_set_last_error("set op: spill_dir is only valid for unsorted set ops");
            set_state_free(st);
            return NULL;
        }
        st->spill_dir = strdup(spill_j->valuestring);
        if (!st->spill_dir) { set_state_free(st); return NULL; }
        if (tf_spill_session_create(st->spill_dir, &st->spill) != TF_OK) { set_state_free(st); return NULL; }
        st->use_spill = 1;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { set_state_free(st); return NULL; }
    step->process = set_process;
    step->flush = set_flush;
    step->flush_next = st->use_spill ? set_flush_next : NULL;
    step->append_stats = set_append_stats;
    step->destroy = set_destroy;
    step->state = st;
    return step;
}

tf_step *tf_intersect_create(const cJSON *args) {
    return set_create_common(args, 0);
}

tf_step *tf_setdiff_create(const cJSON *args) {
    return set_create_common(args, 1);
}

tf_step *tf_intersect_all_create(const cJSON *args) {
    return set_create_common(args, 4);
}

tf_step *tf_setdiff_all_create(const cJSON *args) {
    return set_create_common(args, 5);
}


static tf_step *union_create_common(const cJSON *args, int all) {
    if (!args) return NULL;
    const char *op = all ? "union-all" : "union";
    cJSON *file_j = cJSON_GetObjectItemCaseSensitive(args, "file");
    if (!cJSON_IsString(file_j) || !file_j->valuestring || !file_j->valuestring[0]) {
        tf_set_last_error("union: file is required");
        return NULL;
    }
    set_state *st = calloc(1, sizeof(set_state));
    if (!st) return NULL;
    st->mode = all ? 3 : 2;
    st->file = strdup(file_j->valuestring);
    if (!st->file) { set_state_free(st); return NULL; }

    cJSON *sorted_j = cJSON_GetObjectItemCaseSensitive(args, "sorted");
    if (sorted_j) {
        if (cJSON_IsTrue(sorted_j)) {
            if (all) {
                tf_set_last_error("union-all: sorted=true is only valid for duplicate-eliminating union");
                set_state_free(st);
                return NULL;
            }
            st->sorted = 1;
        } else if (!cJSON_IsFalse(sorted_j)) {
            tf_set_last_error("union: sorted must be boolean");
            set_state_free(st);
            return NULL;
        }
    }

    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (cols && cJSON_IsArray(cols)) {
        int n = cJSON_GetArraySize(cols);
        st->columns = tf_callocarray_checked(n > 0 ? (size_t)n : 1, sizeof(char *));
        if (!st->columns) { set_state_free(st); return NULL; }
        for (int i = 0; i < n; i++) {
            cJSON *item = cJSON_GetArrayItem(cols, i);
            if (!cJSON_IsString(item) || !item->valuestring || !item->valuestring[0]) {
                tf_set_last_error("union: columns must be non-empty strings");
                set_state_free(st);
                return NULL;
            }
            st->columns[st->n_columns] = strdup(item->valuestring);
            if (!st->columns[st->n_columns]) { set_state_free(st); return NULL; }
            st->n_columns++;
        }
    }

    if (parse_positive_size_arg(args, "max_lookup_rows", TF_MAX_COUNT_ARG, &st->max_lookup_rows, op) < 0 ||
        parse_positive_size_arg(args, "max_lookup_bytes", TF_MAX_STATE_BYTES, &st->max_lookup_bytes, op) < 0 ||
        parse_positive_size_arg(args, "max_output_keys", TF_MAX_COUNT_ARG, &st->max_output_keys, op) < 0 ||
        parse_positive_size_arg(args, "max_state_bytes", TF_MAX_STATE_BYTES, &st->max_state_bytes, op) < 0 ||
        parse_positive_size_arg(args, "spill_memory_bytes", TF_MAX_SPILL_MEMORY_BYTES, &st->spill_memory_bytes, op) < 0 ||
        parse_positive_size_arg(args, "spill_run_rows", TF_MAX_SPILL_RUN_ROWS, &st->configured_run_rows, op) < 0 ||
        parse_positive_size_arg(args, "spill_output_rows", TF_MAX_SPILL_OUTPUT_ROWS, &st->output_batch_rows, op) < 0) {
        set_state_free(st);
        return NULL;
    }
    if (all && st->max_state_bytes > 0) {
        tf_set_last_error("union-all: max_state_bytes is only valid for duplicate-eliminating set ops");
        set_state_free(st);
        return NULL;
    }

    cJSON *spill_j = cJSON_GetObjectItemCaseSensitive(args, "spill_dir");
    if (spill_j) {
        if (!cJSON_IsString(spill_j) || !spill_j->valuestring || !spill_j->valuestring[0]) {
            tf_set_last_error("union: spill_dir must be a non-empty string");
            set_state_free(st);
            return NULL;
        }
        if (all) {
            tf_set_last_error("union-all: spill_dir is only valid for duplicate-eliminating union");
            set_state_free(st);
            return NULL;
        }
        if (st->sorted) {
            tf_set_last_error("union: spill_dir is only valid for unsorted union");
            set_state_free(st);
            return NULL;
        }
        st->spill_dir = strdup(spill_j->valuestring);
        if (!st->spill_dir) { set_state_free(st); return NULL; }
        if (tf_spill_session_create(st->spill_dir, &st->spill) != TF_OK) { set_state_free(st); return NULL; }
        st->use_spill = 1;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { set_state_free(st); return NULL; }
    step->process = union_process;
    step->flush = union_flush;
    step->flush_next = union_flush_next;
    step->append_stats = set_append_stats;
    step->destroy = set_destroy;
    step->state = st;
    return step;
}

tf_step *tf_union_create(const cJSON *args) {
    return union_create_common(args, 0);
}

tf_step *tf_union_all_create(const cJSON *args) {
    return union_create_common(args, 1);
}
