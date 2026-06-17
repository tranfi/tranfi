/*
 * op_join.c — Hash join with lookup CSV file.
 *
 * Config: {"file": "lookup.csv", "on": "id" or "left=right",
 *          "how": "inner|left|semi|anti", "max_lookup_bytes": 1048576,
 *          "max_lookup_rows": 100000, "max_lookup_keys": 100000,
 *          "max_state_bytes": 8388608,
 *          "max_matches_per_row": 10, "max_output_rows": 1000000}
 * Loads a capped lookup file once, then streams main data through hash probes.
 * Inner/left joins append lookup columns. Semi/anti joins keep only left rows
 * and never duplicate them.
 */

#include "internal.h"
#include "spill.h"
#include "date_utils.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <limits.h>
#include <errno.h>
#include <stdint.h>
#include <unistd.h>

#define JOIN_DEFAULT_RUN_ROWS 8192
#define JOIN_DEFAULT_OUTPUT_ROWS 1024
#define JOIN_MIN_RUN_ROWS 16

/* Hash map bucket: key -> array of row indices in lookup batch */
typedef struct {
    char   *key;
    size_t *rows;
    size_t  n_rows;
    size_t  rows_cap;
} join_bucket;

typedef struct {
    join_bucket *buckets;
    size_t       n_buckets;  /* power of 2 */
    size_t       count;
    size_t       key_bytes;
    size_t       row_ref_capacity;
} join_hash_map;

typedef tf_owned_cell_value join_key_data;

typedef struct {
    int valid;
    int is_null;
    tf_type type;
    join_key_data v;
} join_key_value;


typedef struct {
    uint64_t ordinal;
    uint8_t *nulls;
    join_key_data *cells;
} join_spill_row;

typedef struct {
    FILE *file;
    join_spill_row row;
    int has_row;
    int done;
} join_spill_reader;

typedef struct {
    char       *file;
    char       *left_col;
    char       *right_col;
    int         how;          /* 0=inner, 1=left, 2=semi, 3=anti */
    int         sorted;       /* lookup and input are sorted by join key */
    size_t      max_lookup_rows;      /* 0 = unlimited */
    size_t      max_lookup_keys;      /* 0 = unlimited */
    size_t      max_lookup_bytes;     /* 0 = unlimited */
    size_t      max_state_bytes;      /* 0 = unlimited */
    size_t      max_matches_per_row;  /* 0 = unlimited */
    size_t      max_output_rows;      /* 0 = unlimited */
    size_t      output_rows;          /* emitted rows across process calls */
    size_t      lookup_rows;          /* decoded lookup rows retained or scanned */

    int         use_spill;            /* exact external semi/anti filtering join */
    char       *spill_dir;
    tf_spill_session *spill;
    size_t      spill_memory_bytes;
    size_t      configured_run_rows;
    size_t      run_rows;
    size_t      output_batch_rows;

    int         spill_has_schema;
    char      **spill_schema_names;
    tf_type    *spill_schema_types;
    size_t      spill_n_cols;
    int         spill_left_join_col;

    char      **spill_lookup_schema_names;
    tf_type    *spill_lookup_schema_types;
    size_t      spill_lookup_n_cols;
    int         spill_lookup_join_col;
    char      **spill_output_schema_names;
    tf_type    *spill_output_schema_types;
    size_t      spill_output_n_cols;

    tf_batch   *spill_left_buf;
    tf_batch   *spill_lookup_buf;
    tf_batch   *spill_out_buf;
    uint64_t   *spill_left_ordinals;
    uint64_t   *spill_lookup_ordinals;
    uint64_t   *spill_out_ordinals;
    size_t      spill_left_ordinal_cap;
    size_t      spill_lookup_ordinal_cap;
    size_t      spill_out_ordinal_cap;
    uint64_t    spill_next_left_ordinal;
    uint64_t    spill_next_lookup_ordinal;

    char      **spill_left_run_paths;
    size_t      spill_n_left_runs;
    size_t      spill_cap_left_runs;
    char      **spill_lookup_run_paths;
    size_t      spill_n_lookup_runs;
    size_t      spill_cap_lookup_runs;
    char      **spill_out_run_paths;
    size_t      spill_n_out_runs;
    size_t      spill_cap_out_runs;
    size_t      spill_left_run_seq;
    size_t      spill_lookup_run_seq;
    size_t      spill_out_run_seq;

    join_spill_reader *spill_left_readers;
    size_t      spill_n_left_readers;
    join_spill_reader *spill_lookup_readers;
    size_t      spill_n_lookup_readers;
    join_spill_reader *spill_out_readers;
    size_t      spill_n_out_readers;

    int         spill_lookup_loaded;
    int         spill_key_merge_done;
    int         spill_output_merge_started;
    int         spill_output_merge_done;
    char       *spill_last_lookup_key;

    size_t      spill_bytes;
    size_t      spill_runs;
    size_t      spill_output_batches;
    size_t      spill_output_rows;
    size_t      spill_kept_rows;
    size_t      spill_lookup_keys;
    size_t      spill_lookup_key_bytes;

    tf_batch   *lookup;
    int         loaded;
    int         lookup_join_col;
    tf_type     lookup_key_type;
    int         have_lookup_key_type;

    /* Lookup columns to include in output (excluding join key) */
    int        *lookup_out_cols;
    size_t      n_lookup_out;

    join_hash_map map;

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
    join_key_value prev_lookup_key;
    int         have_prev_lookup_key;
    join_key_value prev_left_key;
    int         have_prev_left_key;
    tf_batch   *right_run;
    join_key_value right_run_key;
    int         have_right_run;
    char      **lookup_schema_names;
    tf_type    *lookup_schema_types;
    size_t      n_lookup_schema_cols;
} join_state;

static size_t join_batch_storage_bytes(const tf_batch *b);
static size_t join_hash_map_retained_bytes(const join_hash_map *map);
static size_t join_unsorted_retained_state_bytes(const join_hash_map *map,
                                                 const tf_batch *lookup,
                                                 size_t n_lookup_out);
static int join_check_unsorted_state_bytes(const join_state *st,
                                           const join_hash_map *map,
                                           const tf_batch *lookup,
                                           size_t n_lookup_out,
                                           tf_side_channels *side);
static int join_reserve_output_row(join_state *st, tf_side_channels *side);
static size_t join_spill_retained_state_bytes(const join_state *st);

static int join_write_error(tf_side_channels *side, const char *msg) {
    return tf_side_write_error(side, msg);
}

static int join_limit_error_context(tf_side_channels *side, const char *field,
                                    size_t limit, size_t actual,
                                    const char *context) {
    char msg[224];
    snprintf(msg, sizeof(msg),
             "join: %s=%zu exceeded %s (%zu)",
             field, limit, context, actual);
    return join_write_error(side, msg);
}

static int join_limit_error(tf_side_channels *side, const char *field,
                            size_t limit, size_t actual) {
    return join_limit_error_context(side, field, limit, actual,
                                    "while loading lookup side");
}

/* FNV-1a hash */
static uint64_t fnv1a(const char *s) {
    uint64_t h = 14695981039346656037ULL;
    while (*s) { h ^= (uint8_t)*s++; h *= 1099511628211ULL; }
    return h;
}

static int map_init(join_hash_map *m, size_t hint) {
    size_t n = 64;
    size_t target = 0;
    if (tf_size_mul(hint, 2, &target) != TF_OK) return -1;
    while (n < target) {
        if (n > (SIZE_MAX / 2)) return -1;
        n *= 2;
    }
    m->buckets = tf_callocarray_checked(n, sizeof(join_bucket));
    if (!m->buckets) return -1;
    m->n_buckets = n;
    m->count = 0;
    m->key_bytes = 0;
    m->row_ref_capacity = 0;
    return 0;
}

static int map_insert(join_state *st, join_hash_map *m,
                      const char *key, size_t row,
                      tf_side_channels *side) {
    uint64_t h = fnv1a(key);
    size_t idx = h & (m->n_buckets - 1);
    /* Linear probe */
    while (m->buckets[idx].key) {
        if (strcmp(m->buckets[idx].key, key) == 0) {
            /* Filtering joins only need key presence, not duplicate right rows. */
            if (st->how >= 2) return 0;

            /* Add row to existing bucket */
            join_bucket *b = &m->buckets[idx];
            if (st->max_matches_per_row > 0 && b->n_rows >= st->max_matches_per_row) {
                join_limit_error_context(side, "max_matches_per_row",
                                         st->max_matches_per_row, b->n_rows + 1,
                                         "for lookup key");
                return -1;
            }
            if (b->n_rows >= b->rows_cap) {
                size_t old_cap = b->rows_cap;
                size_t need = 0;
                size_t new_cap = 0;
                size_t new_row_ref_capacity = 0;
                if (tf_size_add(b->n_rows, 1, &need) != TF_OK ||
                    tf_size_grow_pow2(b->rows_cap, need, 4, &new_cap) != TF_OK ||
                    tf_size_add(m->row_ref_capacity, new_cap - old_cap,
                                &new_row_ref_capacity) != TF_OK) {
                    return -1;
                }
                size_t *tmp = tf_reallocarray_checked(b->rows, new_cap, sizeof(size_t));
                if (!tmp) return -1;
                b->rows = tmp;
                b->rows_cap = new_cap;
                m->row_ref_capacity = new_row_ref_capacity;
            }
            b->rows[b->n_rows++] = row;
            return 0;
        }
        idx = (idx + 1) & (m->n_buckets - 1);
    }

    if (st->max_lookup_keys > 0 && m->count >= st->max_lookup_keys) {
        join_limit_error(side, "max_lookup_keys", st->max_lookup_keys, m->count + 1);
        return -1;
    }

    /* New key */
    join_bucket *b = &m->buckets[idx];
    char *key_copy = strdup(key);
    if (!key_copy) return -1;
    size_t key_bytes_delta = 0;
    size_t new_key_bytes = 0;
    if (tf_size_add(strlen(key_copy), 1, &key_bytes_delta) != TF_OK ||
        tf_size_add(m->key_bytes, key_bytes_delta, &new_key_bytes) != TF_OK) {
        free(key_copy);
        return -1;
    }
    size_t *rows = NULL;
    size_t rows_cap = 0;
    size_t n_rows = 0;
    size_t new_row_ref_capacity = m->row_ref_capacity;
    if (st->how >= 2) {
        rows = NULL;
        rows_cap = 0;
        n_rows = 0;
    } else {
        if (tf_size_add(m->row_ref_capacity, 4, &new_row_ref_capacity) != TF_OK) {
            free(key_copy);
            return -1;
        }
        rows = tf_mallocarray_checked(4, sizeof(size_t));
        if (!rows) { free(key_copy); return -1; }
        rows_cap = 4;
        rows[0] = row;
        n_rows = 1;
    }
    b->key = key_copy;
    b->rows = rows;
    b->rows_cap = rows_cap;
    b->n_rows = n_rows;
    m->key_bytes = new_key_bytes;
    m->row_ref_capacity = new_row_ref_capacity;
    m->count++;
    return 0;
}

static join_bucket *map_find(join_hash_map *m, const char *key) {
    if (!m->buckets) return NULL;
    uint64_t h = fnv1a(key);
    size_t idx = h & (m->n_buckets - 1);
    while (m->buckets[idx].key) {
        if (strcmp(m->buckets[idx].key, key) == 0)
            return &m->buckets[idx];
        idx = (idx + 1) & (m->n_buckets - 1);
    }
    return NULL;
}

static void map_free(join_hash_map *m) {
    if (!m->buckets) return;
    for (size_t i = 0; i < m->n_buckets; i++) {
        if (m->buckets[i].key) {
            free(m->buckets[i].key);
            free(m->buckets[i].rows);
        }
    }
    free(m->buckets);
    m->buckets = NULL;
    m->n_buckets = 0;
    m->count = 0;
    m->key_bytes = 0;
    m->row_ref_capacity = 0;
}

/* Serialize a typed join key. Tags avoid collisions such as int 1 vs string "1"
 * and null vs a literal sentinel string. */
static char *format_join_key(const tf_batch *b, size_t row, int col) {
    char buf[96];
    tf_type type = b->col_types[col];
    if (tf_batch_is_null(b, row, col)) {
        snprintf(buf, sizeof(buf), "N:%d", (int)type);
        return strdup(buf);
    }

    switch (type) {
        case TF_TYPE_STRING: {
            const char *v = tf_batch_get_string(b, row, col);
            size_t len = v ? strlen(v) : 0;
            char prefix[48];
            int n = snprintf(prefix, sizeof(prefix), "S:%zu:", len);
            if (n < 0) return NULL;
            size_t prefix_len = (size_t)n;
            char *out = malloc(prefix_len + len + 1);
            if (!out) return NULL;
            memcpy(out, prefix, prefix_len);
            if (len) memcpy(out + prefix_len, v, len);
            out[prefix_len + len] = '\0';
            return out;
        }
        case TF_TYPE_INT64:
            snprintf(buf, sizeof(buf), "I:%lld", (long long)tf_batch_get_int64(b, row, col));
            return strdup(buf);
        case TF_TYPE_FLOAT64:
            snprintf(buf, sizeof(buf), "F:%.17g", tf_batch_get_float64(b, row, col));
            return strdup(buf);
        case TF_TYPE_BOOL:
            return strdup(tf_batch_get_bool(b, row, col) ? "B:1" : "B:0");
        case TF_TYPE_DATE:
            snprintf(buf, sizeof(buf), "D:%d", (int)tf_batch_get_date(b, row, col));
            return strdup(buf);
        case TF_TYPE_TIMESTAMP:
            snprintf(buf, sizeof(buf), "T:%lld", (long long)tf_batch_get_timestamp(b, row, col));
            return strdup(buf);
        default:
            snprintf(buf, sizeof(buf), "U:%d", (int)type);
            return strdup(buf);
    }
}

static void join_key_value_clear(join_key_value *v) {
    if (!v) return;
    if (v->valid && !v->is_null && v->type == TF_TYPE_STRING) free(v->v.str);
    memset(v, 0, sizeof(*v));
}

static int join_key_value_set(join_key_value *dst, const tf_batch *b, size_t row, int col) {
    if (!dst || !b || col < 0) return TF_ERROR;
    join_key_value_clear(dst);
    dst->valid = 1;
    dst->type = b->col_types[col];
    dst->is_null = tf_batch_is_null(b, row, col) ? 1 : 0;
    if (dst->is_null) return TF_OK;

    switch (dst->type) {
        case TF_TYPE_BOOL:
            dst->v.b = tf_batch_get_bool(b, row, col) ? 1 : 0;
            return TF_OK;
        case TF_TYPE_INT64:
            dst->v.i64 = tf_batch_get_int64(b, row, col);
            return TF_OK;
        case TF_TYPE_FLOAT64:
            dst->v.f64 = tf_batch_get_float64(b, row, col);
            return TF_OK;
        case TF_TYPE_STRING: {
            const char *s = tf_batch_get_string(b, row, col);
            dst->v.str = strdup(s ? s : "");
            return dst->v.str ? TF_OK : TF_ERROR;
        }
        case TF_TYPE_DATE:
            dst->v.date = tf_batch_get_date(b, row, col);
            return TF_OK;
        case TF_TYPE_TIMESTAMP:
            dst->v.i64 = tf_batch_get_timestamp(b, row, col);
            return TF_OK;
        default:
            dst->is_null = 1;
            return TF_OK;
    }
}

static int join_key_value_copy(join_key_value *dst, const join_key_value *src) {
    if (!dst || !src || !src->valid) return TF_ERROR;
    join_key_value_clear(dst);
    *dst = *src;
    if (!src->is_null && src->type == TF_TYPE_STRING) {
        dst->v.str = strdup(src->v.str ? src->v.str : "");
        if (!dst->v.str) { memset(dst, 0, sizeof(*dst)); return TF_ERROR; }
    }
    return TF_OK;
}

static int join_key_compare_values(const join_key_value *a, const join_key_value *b, int *cmp) {
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

static int join_key_compare_value_to_cell(const join_key_value *a, const tf_batch *b,
                                          size_t row, int col, int *cmp) {
    join_key_value rhs = {0};
    if (join_key_value_set(&rhs, b, row, col) != TF_OK) return TF_ERROR;
    int rc = join_key_compare_values(a, &rhs, cmp);
    join_key_value_clear(&rhs);
    return rc;
}

static void free_batch_array(tf_batch **batches, size_t n_batches) {
    if (!batches) return;
    for (size_t i = 0; i < n_batches; i++) tf_batch_free(batches[i]);
    free(batches);
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
        if (b > a + 1 && data[a] == '"' && data[b - 1] == '"') {
            a++;
            b--;
        }
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

static void sorted_join_free_pending_batches(join_state *st) {
    if (!st || !st->sorted_batches) return;
    for (size_t i = st->sorted_batch_index; i < st->sorted_n_batches; i++) {
        if (st->sorted_batches[i]) tf_batch_free(st->sorted_batches[i]);
    }
    free(st->sorted_batches);
    st->sorted_batches = NULL;
    st->sorted_n_batches = 0;
    st->sorted_batch_index = 0;
}

static void sorted_join_close(join_state *st) {
    if (!st) return;
    if (st->sorted_current) {
        tf_batch_free(st->sorted_current);
        st->sorted_current = NULL;
    }
    sorted_join_free_pending_batches(st);
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

static void sorted_join_free_schema(join_state *st) {
    if (!st) return;
    for (size_t i = 0; i < st->n_lookup_schema_cols; i++) {
        free(st->lookup_schema_names ? st->lookup_schema_names[i] : NULL);
    }
    free(st->lookup_schema_names);
    free(st->lookup_schema_types);
    st->lookup_schema_names = NULL;
    st->lookup_schema_types = NULL;
    st->n_lookup_schema_cols = 0;
}

static int sorted_join_capture_schema(join_state *st, const tf_batch *b) {
    if (st->n_lookup_schema_cols > 0) return TF_OK;
    st->n_lookup_schema_cols = b->n_cols;
    st->lookup_schema_names = tf_callocarray_checked(b->n_cols ? b->n_cols : 1, sizeof(char *));
    st->lookup_schema_types = tf_callocarray_checked(b->n_cols ? b->n_cols : 1, sizeof(tf_type));
    if (!st->lookup_schema_names || !st->lookup_schema_types) return TF_ERROR;
    for (size_t c = 0; c < b->n_cols; c++) {
        st->lookup_schema_names[c] = strdup(b->col_names[c] ? b->col_names[c] : "");
        if (!st->lookup_schema_names[c]) return TF_ERROR;
        st->lookup_schema_types[c] = b->col_types[c];
    }
    st->lookup_join_col = tf_batch_col_index(b, st->right_col);
    if (st->lookup_join_col < 0) return TF_ERROR;
    st->lookup_key_type = b->col_types[st->lookup_join_col];
    st->have_lookup_key_type = 1;
    if (st->how < 2) {
        st->lookup_out_cols = tf_mallocarray_checked(b->n_cols ? b->n_cols : 1, sizeof(int));
        if (!st->lookup_out_cols) return TF_ERROR;
        st->n_lookup_out = 0;
        for (size_t c = 0; c < b->n_cols; c++) {
            if ((int)c != st->lookup_join_col) st->lookup_out_cols[st->n_lookup_out++] = (int)c;
        }
    }
    return TF_OK;
}

static tf_batch *sorted_join_create_run_batch(const join_state *st) {
    tf_batch *b = tf_batch_create(st->n_lookup_schema_cols, 16);
    if (!b) return NULL;
    for (size_t c = 0; c < st->n_lookup_schema_cols; c++) {
        if (tf_batch_set_schema(b, c, st->lookup_schema_names[c], st->lookup_schema_types[c]) != TF_OK) {
            tf_batch_free(b);
            return NULL;
        }
    }
    return b;
}

static int sorted_join_open(join_state *st, tf_side_channels *side) {
    if (!st || st->sorted_file || st->sorted_decoder) return TF_OK;
    if (!csv_file_header_has_column(st->file, st->right_col)) {
        join_write_error(side, "sorted join: lookup file is missing join column");
        return TF_ERROR;
    }
    st->sorted_file = fopen(st->file, "rb");
    if (!st->sorted_file) {
        join_write_error(side, "sorted join: cannot open lookup file");
        return TF_ERROR;
    }
    st->sorted_decoder = tf_csv_decoder_create(NULL);
    if (!st->sorted_decoder) return TF_ERROR;
    st->lookup_join_col = -1;
    return TF_OK;
}

static int sorted_join_next_batch(join_state *st, tf_side_channels *side) {
    if (st->sorted_current) {
        tf_batch_free(st->sorted_current);
        st->sorted_current = NULL;
    }

    for (;;) {
        if (st->sorted_batches && st->sorted_batch_index < st->sorted_n_batches) {
            st->sorted_current = st->sorted_batches[st->sorted_batch_index++];
            if (st->sorted_current && sorted_join_capture_schema(st, st->sorted_current) != TF_OK) {
                join_write_error(side, "sorted join: failed to resolve lookup schema");
                return TF_ERROR;
            }
            st->sorted_row = 0;
            if (st->sorted_current && st->sorted_current->n_rows > 0) return TF_OK;
            if (st->sorted_current) {
                tf_batch_free(st->sorted_current);
                st->sorted_current = NULL;
            }
            continue;
        }

        sorted_join_free_pending_batches(st);
        if (st->sorted_flushed) {
            st->sorted_exhausted = 1;
            return TF_OK;
        }

        uint8_t buf[64 * 1024];
        size_t n = fread(buf, 1, sizeof(buf), st->sorted_file);
        tf_batch **batches = NULL;
        size_t n_batches = 0;
        int rc;
        if (n > 0) {
            rc = st->sorted_decoder->decode(st->sorted_decoder, buf, n, &batches, &n_batches, side);
        } else {
            if (ferror(st->sorted_file)) {
                join_write_error(side, "sorted join: failed reading lookup file");
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

static int sorted_join_advance_lookup(join_state *st, tf_side_channels *side) {
    if (!st->sorted_file && sorted_join_open(st, side) != TF_OK) return TF_ERROR;
    if (st->sorted_have_row) {
        st->sorted_row++;
        st->sorted_have_row = 0;
    }

    for (;;) {
        if (!st->sorted_current || st->sorted_row >= st->sorted_current->n_rows) {
            if (sorted_join_next_batch(st, side) != TF_OK) return TF_ERROR;
            if (st->sorted_exhausted) return TF_OK;
            continue;
        }

        join_key_value cur = {0};
        if (join_key_value_set(&cur, st->sorted_current, st->sorted_row, st->lookup_join_col) != TF_OK) {
            return TF_ERROR;
        }
        if (st->have_prev_lookup_key) {
            int cmp = 0;
            if (join_key_compare_values(&st->prev_lookup_key, &cur, &cmp) != TF_OK) {
                join_key_value_clear(&cur);
                join_write_error(side, "sorted join: lookup key type changed across batches");
                return TF_ERROR;
            }
            if (cmp > 0) {
                join_key_value_clear(&cur);
                join_write_error(side, "sorted join: lookup side is not sorted by join key");
                return TF_ERROR;
            }
        }
        if (join_key_value_copy(&st->prev_lookup_key, &cur) != TF_OK) {
            join_key_value_clear(&cur);
            return TF_ERROR;
        }
        join_key_value_clear(&cur);
        st->have_prev_lookup_key = 1;
        st->sorted_have_row = 1;
        st->lookup_rows++;
        return TF_OK;
    }
}

static void sorted_join_clear_right_run(join_state *st) {
    if (!st) return;
    if (st->right_run) {
        tf_batch_free(st->right_run);
        st->right_run = NULL;
    }
    join_key_value_clear(&st->right_run_key);
    st->have_right_run = 0;
}

static int sorted_join_load_next_run(join_state *st, tf_side_channels *side) {
    sorted_join_clear_right_run(st);
    if (!st->sorted_have_row) {
        if (sorted_join_advance_lookup(st, side) != TF_OK) return TF_ERROR;
    }
    if (!st->sorted_have_row) {
        st->sorted_exhausted = 1;
        return TF_OK;
    }

    if (join_key_value_set(&st->right_run_key, st->sorted_current, st->sorted_row,
                           st->lookup_join_col) != TF_OK) return TF_ERROR;
    st->have_right_run = 1;

    if (st->how < 2) {
        st->right_run = sorted_join_create_run_batch(st);
        if (!st->right_run) return TF_ERROR;
    }

    while (st->sorted_have_row) {
        int cmp = 0;
        if (join_key_compare_value_to_cell(&st->right_run_key, st->sorted_current,
                                           st->sorted_row, st->lookup_join_col, &cmp) != TF_OK) {
            join_write_error(side, "sorted join: lookup key type changed across rows");
            return TF_ERROR;
        }
        if (cmp != 0) break;

        if (st->how < 2) {
            if (st->max_matches_per_row > 0 && st->right_run->n_rows >= st->max_matches_per_row) {
                join_limit_error_context(side, "max_matches_per_row", st->max_matches_per_row,
                                         st->right_run->n_rows + 1, "for sorted lookup key");
                return TF_ERROR;
            }
            size_t dst = st->right_run->n_rows;
            if (tf_batch_ensure_capacity(st->right_run, dst + 1) != TF_OK) return TF_ERROR;
            if (tf_batch_copy_row(st->right_run, dst, st->sorted_current, st->sorted_row) != TF_OK)
                return TF_ERROR;
            st->right_run->n_rows = dst + 1;
        }

        if (sorted_join_advance_lookup(st, side) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static int sorted_join_ensure_run_at_least(join_state *st, const tf_batch *left,
                                           size_t left_row, int left_ci,
                                           tf_side_channels *side) {
    if (!st->have_right_run && !st->sorted_exhausted) {
        if (sorted_join_load_next_run(st, side) != TF_OK) return TF_ERROR;
    }
    while (st->have_right_run) {
        int cmp = 0;
        if (join_key_compare_value_to_cell(&st->right_run_key, left, left_row, left_ci, &cmp) != TF_OK) {
            join_write_error(side, "sorted join: left and lookup join key types differ");
            return TF_ERROR;
        }
        if (cmp >= 0) return TF_OK;
        if (sorted_join_load_next_run(st, side) != TF_OK) return TF_ERROR;
        if (st->sorted_exhausted && !st->have_right_run) return TF_OK;
    }
    return TF_OK;
}

static int sorted_join_check_left_order(join_state *st, const tf_batch *left,
                                        size_t row, int left_ci,
                                        tf_side_channels *side) {
    join_key_value cur = {0};
    if (join_key_value_set(&cur, left, row, left_ci) != TF_OK) return TF_ERROR;
    if (st->have_prev_left_key) {
        int cmp = 0;
        if (join_key_compare_values(&st->prev_left_key, &cur, &cmp) != TF_OK) {
            join_key_value_clear(&cur);
            join_write_error(side, "sorted join: left join key type changed across batches");
            return TF_ERROR;
        }
        if (cmp > 0) {
            join_key_value_clear(&cur);
            join_write_error(side, "sorted join: left side is not sorted by join key");
            return TF_ERROR;
        }
    }
    if (join_key_value_copy(&st->prev_left_key, &cur) != TF_OK) {
        join_key_value_clear(&cur);
        return TF_ERROR;
    }
    join_key_value_clear(&cur);
    st->have_prev_left_key = 1;
    return TF_OK;
}

static int load_lookup(join_state *st, tf_side_channels *side) {
    FILE *f = NULL;
    uint8_t *data = NULL;
    tf_decoder *dec = NULL;
    tf_batch **batches = NULL;
    tf_batch **flush_batches = NULL;
    tf_batch **all_batches = NULL;
    size_t n_batches = 0, n_flush = 0, total_batches = 0, total_rows = 0;
    tf_batch *merged = NULL;
    int *lookup_out_cols = NULL;
    join_hash_map map = {0};

    f = fopen(st->file, "rb");
    if (!f) return TF_ERROR;

    /* Read entire lookup file, guarded by max_lookup_bytes when provided. */
    if (fseek(f, 0, SEEK_END) != 0) goto fail;
    long fsize = ftell(f);
    if (fseek(f, 0, SEEK_SET) != 0) goto fail;
    if (fsize <= 0) goto fail;
    if (st->max_lookup_bytes > 0 && (size_t)fsize > st->max_lookup_bytes) {
        join_limit_error(side, "max_lookup_bytes", st->max_lookup_bytes, (size_t)fsize);
        goto fail;
    }

    data = malloc((size_t)fsize);
    if (!data) goto fail;
    size_t nread = fread(data, 1, (size_t)fsize, f);
    fclose(f);
    f = NULL;
    if (nread != (size_t)fsize) goto fail;

    /* Decode using CSV decoder */
    dec = tf_csv_decoder_create(NULL);
    if (!dec) goto fail;

    if (dec->decode(dec, data, nread, &batches, &n_batches, NULL) != TF_OK) goto fail;
    if (dec->flush(dec, &flush_batches, &n_flush, NULL) != TF_OK) goto fail;

    total_batches = n_batches + n_flush;
    all_batches = tf_mallocarray_checked(total_batches ? total_batches : 1, sizeof(tf_batch *));
    if (!all_batches) goto fail;
    for (size_t i = 0; i < n_batches; i++) {
        all_batches[i] = batches[i];
        total_rows += batches[i]->n_rows;
        if (st->max_lookup_rows > 0 && total_rows > st->max_lookup_rows) {
            join_limit_error(side, "max_lookup_rows", st->max_lookup_rows, total_rows);
            goto fail;
        }
    }
    for (size_t i = 0; i < n_flush; i++) {
        all_batches[n_batches + i] = flush_batches[i];
        total_rows += flush_batches[i]->n_rows;
        if (st->max_lookup_rows > 0 && total_rows > st->max_lookup_rows) {
            join_limit_error(side, "max_lookup_rows", st->max_lookup_rows, total_rows);
            goto fail;
        }
    }

    if (total_batches == 0 && st->how >= 2) {
        if (!csv_header_has_column(data, nread, st->right_col)) goto fail;
        if (map_init(&map, 1) != 0) goto fail;
        if (join_check_unsorted_state_bytes(st, &map, NULL, 0, side) != TF_OK) goto fail;

        free(all_batches);
        all_batches = NULL;
        free_batch_array(batches, n_batches);
        batches = NULL;
        free_batch_array(flush_batches, n_flush);
        flush_batches = NULL;
        dec->destroy(dec);
        dec = NULL;
        free(data);
        data = NULL;

        st->lookup_rows = 0;
        st->lookup = NULL;
        st->lookup_join_col = -1;
        st->lookup_out_cols = NULL;
        st->n_lookup_out = 0;
        st->map = map;
        return TF_OK;
    }
    if (total_batches == 0) goto fail;
    if (total_rows == 0 && st->how >= 2) {
        tf_batch *first = all_batches[0];
        int lookup_join_col = tf_batch_col_index(first, st->right_col);
        if (lookup_join_col < 0) goto fail;
        if (map_init(&map, 1) != 0) goto fail;
        if (join_check_unsorted_state_bytes(st, &map, NULL, 0, side) != TF_OK) goto fail;

        free_batch_array(batches, n_batches);
        batches = NULL;
        free_batch_array(flush_batches, n_flush);
        flush_batches = NULL;
        free(all_batches);
        all_batches = NULL;
        dec->destroy(dec);
        dec = NULL;
        free(data);
        data = NULL;

        st->lookup_rows = 0;
        st->lookup = NULL;
        st->lookup_join_col = lookup_join_col;
        st->lookup_key_type = first->col_types[lookup_join_col];
        st->have_lookup_key_type = 1;
        st->lookup_out_cols = NULL;
        st->n_lookup_out = 0;
        st->map = map;
        return TF_OK;
    }
    if (total_rows == 0) goto fail;

    /* Use first batch as schema source */
    tf_batch *first = all_batches[0];
    merged = tf_batch_create(first->n_cols, total_rows);
    if (!merged) goto fail;
    for (size_t c = 0; c < first->n_cols; c++) {
        if (tf_batch_set_schema(merged, c, first->col_names[c], first->col_types[c]) != TF_OK) goto fail;
    }

    size_t dst_row = 0;
    for (size_t b = 0; b < total_batches; b++) {
        for (size_t r = 0; r < all_batches[b]->n_rows; r++) {
            if (tf_batch_copy_row(merged, dst_row, all_batches[b], r) != TF_OK) goto fail;
            merged->n_rows = ++dst_row;
        }
    }

    for (size_t i = 0; i < total_batches; i++) all_batches[i] = NULL;
    free_batch_array(batches, n_batches);
    batches = NULL;
    free_batch_array(flush_batches, n_flush);
    flush_batches = NULL;
    free(all_batches);
    all_batches = NULL;
    dec->destroy(dec);
    dec = NULL;
    free(data);
    data = NULL;

    /* Find join column in lookup */
    int lookup_join_col = tf_batch_col_index(merged, st->right_col);
    if (lookup_join_col < 0) goto fail;

    st->lookup_key_type = merged->col_types[lookup_join_col];
    st->have_lookup_key_type = 1;

    size_t n_lookup_out = 0;
    if (st->how < 2) {
        /* Determine output columns for mutating joins (all except join key). */
        lookup_out_cols = tf_mallocarray_checked(merged->n_cols ? merged->n_cols : 1, sizeof(int));
        if (!lookup_out_cols) goto fail;
        for (size_t c = 0; c < merged->n_cols; c++) {
            if ((int)c != lookup_join_col)
                lookup_out_cols[n_lookup_out++] = (int)c;
        }
    }

    /* Build hash map */
    if (map_init(&map, merged->n_rows) != 0) goto fail;
    const tf_batch *retained_lookup = (st->how >= 2) ? NULL : merged;
    size_t retained_lookup_out = (st->how >= 2) ? 0 : n_lookup_out;
    if (join_check_unsorted_state_bytes(st, &map, retained_lookup, retained_lookup_out, side) != TF_OK) goto fail;
    for (size_t r = 0; r < merged->n_rows; r++) {
        char *key = format_join_key(merged, r, lookup_join_col);
        if (!key) goto fail;
        int rc = map_insert(st, &map, key, r, side);
        free(key);
        if (rc != 0) goto fail;
        if (join_check_unsorted_state_bytes(st, &map, retained_lookup, retained_lookup_out, side) != TF_OK) goto fail;
    }

    if (st->how >= 2) {
        tf_batch_free(merged);
        merged = NULL;
    }
    st->lookup_rows = total_rows;
    st->lookup = merged;
    st->lookup_join_col = lookup_join_col;
    st->lookup_out_cols = lookup_out_cols;
    st->n_lookup_out = n_lookup_out;
    st->map = map;
    return TF_OK;

fail:
    if (f) fclose(f);
    if (all_batches) {
        for (size_t i = 0; i < total_batches; i++) tf_batch_free(all_batches[i]);
        free(all_batches);
        if (batches) {
            for (size_t i = 0; i < n_batches; i++) batches[i] = NULL;
        }
        if (flush_batches) {
            for (size_t i = 0; i < n_flush; i++) flush_batches[i] = NULL;
        }
    }
    free_batch_array(batches, n_batches);
    free_batch_array(flush_batches, n_flush);
    if (dec) dec->destroy(dec);
    free(data);
    if (merged) tf_batch_free(merged);
    free(lookup_out_cols);
    map_free(&map);
    return TF_ERROR;
}

static int join_reserve_output_row(join_state *st, tf_side_channels *side) {
    if (st->max_output_rows > 0 && st->output_rows >= st->max_output_rows) {
        join_limit_error_context(side, "max_output_rows",
                                 st->max_output_rows, st->output_rows + 1,
                                 "while emitting joined output");
        return 0;
    }
    st->output_rows++;
    return 1;
}


static int join_ensure_ordinals(uint64_t **ord, size_t *cap, size_t need) {
    if (*cap >= need) return TF_OK;
    size_t new_cap = 0;
    if (tf_size_grow_pow2(*cap, need, 16, &new_cap) != TF_OK) return TF_ERROR;
    uint64_t *tmp = tf_reallocarray_checked(*ord, new_cap, sizeof(uint64_t));
    if (!tmp) return TF_ERROR;
    *ord = tmp;
    *cap = new_cap;
    return TF_OK;
}

static size_t join_spill_estimated_row_bytes(const join_state *st) {
    size_t bytes = 40;
    for (size_t c = 0; c < st->spill_n_cols; c++) {
        bytes += 1;
        switch (st->spill_schema_types[c]) {
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

static tf_batch *join_spill_create_left_batch(const join_state *st, size_t capacity) {
    tf_batch *b = tf_batch_create(st->spill_n_cols, capacity ? capacity : 16);
    if (!b) return NULL;
    for (size_t c = 0; c < st->spill_n_cols; c++) {
        if (tf_batch_set_schema(b, c, st->spill_schema_names[c], st->spill_schema_types[c]) != TF_OK) {
            tf_batch_free(b);
            return NULL;
        }
    }
    return b;
}

static tf_batch *join_spill_create_lookup_batch(const join_state *st, size_t capacity) {
    if (st->how < 2) {
        tf_batch *b = tf_batch_create(st->spill_lookup_n_cols, capacity ? capacity : 16);
        if (!b) return NULL;
        for (size_t c = 0; c < st->spill_lookup_n_cols; c++) {
            if (tf_batch_set_schema(b, c, st->spill_lookup_schema_names[c],
                                    st->spill_lookup_schema_types[c]) != TF_OK) {
                tf_batch_free(b);
                return NULL;
            }
        }
        return b;
    }

    tf_batch *b = tf_batch_create(1, capacity ? capacity : 16);
    if (!b) return NULL;
    if (tf_batch_set_schema(b, 0, st->right_col ? st->right_col : "key", st->lookup_key_type) != TF_OK) {
        tf_batch_free(b);
        return NULL;
    }
    return b;
}

static tf_batch *join_spill_create_output_batch(const join_state *st, size_t capacity) {
    if (st->how < 2) {
        tf_batch *b = tf_batch_create(st->spill_output_n_cols, capacity ? capacity : 16);
        if (!b) return NULL;
        for (size_t c = 0; c < st->spill_output_n_cols; c++) {
            if (tf_batch_set_schema(b, c, st->spill_output_schema_names[c],
                                    st->spill_output_schema_types[c]) != TF_OK) {
                tf_batch_free(b);
                return NULL;
            }
        }
        return b;
    }
    return join_spill_create_left_batch(st, capacity);
}

static int join_spill_check_input_schema(join_state *st, const tf_batch *in, tf_side_channels *side) {
    if (in->n_cols != st->spill_n_cols) {
        join_write_error(side, "join spill: input schema changed across batches");
        return TF_ERROR;
    }
    for (size_t c = 0; c < in->n_cols; c++) {
        if (in->col_types[c] != st->spill_schema_types[c] ||
            strcmp(in->col_names[c] ? in->col_names[c] : "", st->spill_schema_names[c]) != 0) {
            join_write_error(side, "join spill: input schema changed across batches");
            return TF_ERROR;
        }
    }
    int left_ci = tf_batch_col_index(in, st->left_col);
    if (left_ci < 0 || left_ci != st->spill_left_join_col) {
        join_write_error(side, "join spill: left join column is missing or moved");
        return TF_ERROR;
    }
    return TF_OK;
}

static int join_spill_init_schema(join_state *st, const tf_batch *in, tf_side_channels *side) {
    if (st->spill_has_schema) return join_spill_check_input_schema(st, in, side);
    int left_ci = tf_batch_col_index(in, st->left_col);
    if (left_ci < 0) {
        join_write_error(side, "join spill: left join column not found");
        return TF_ERROR;
    }
    st->spill_n_cols = in->n_cols;
    st->spill_schema_names = tf_callocarray_checked(in->n_cols ? in->n_cols : 1, sizeof(char *));
    st->spill_schema_types = tf_callocarray_checked(in->n_cols ? in->n_cols : 1, sizeof(tf_type));
    if (!st->spill_schema_names || !st->spill_schema_types) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++) {
        st->spill_schema_names[c] = strdup(in->col_names[c] ? in->col_names[c] : "");
        if (!st->spill_schema_names[c]) return TF_ERROR;
        st->spill_schema_types[c] = in->col_types[c];
    }
    st->spill_left_join_col = left_ci;
    if (st->configured_run_rows > 0) {
        st->run_rows = st->configured_run_rows;
    } else if (st->spill_memory_bytes > 0) {
        size_t row_bytes = join_spill_estimated_row_bytes(st);
        st->run_rows = st->spill_memory_bytes / (row_bytes * 4);
        if (st->run_rows < JOIN_MIN_RUN_ROWS) st->run_rows = JOIN_MIN_RUN_ROWS;
    } else {
        st->run_rows = JOIN_DEFAULT_RUN_ROWS;
    }
    st->spill_left_buf = join_spill_create_left_batch(st, st->run_rows);
    if (st->how >= 2)
        st->spill_out_buf = join_spill_create_output_batch(st, st->run_rows);
    if (!st->spill_left_buf || (st->how >= 2 && !st->spill_out_buf)) return TF_ERROR;
    st->lookup_key_type = in->col_types[left_ci];
    st->have_lookup_key_type = 0;
    st->spill_has_schema = 1;
    return TF_OK;
}

static int join_spill_compare_batch_key(const tf_batch *b, size_t ca, size_t cb,
                                        size_t ra, size_t rb) {
    int null_a = tf_batch_is_null(b, ra, ca);
    int null_b = tf_batch_is_null(b, rb, cb);
    if (null_a && null_b) return 0;
    if (null_a) return 1;
    if (null_b) return -1;
    switch (b->col_types[ca]) {
        case TF_TYPE_BOOL:
            return (int)tf_batch_get_bool(b, ra, ca) - (int)tf_batch_get_bool(b, rb, cb);
        case TF_TYPE_INT64: {
            int64_t a = tf_batch_get_int64(b, ra, ca), v = tf_batch_get_int64(b, rb, cb);
            return (a > v) - (a < v);
        }
        case TF_TYPE_FLOAT64: {
            double a = tf_batch_get_float64(b, ra, ca), v = tf_batch_get_float64(b, rb, cb);
            return (a > v) - (a < v);
        }
        case TF_TYPE_STRING:
            return strcmp(tf_batch_get_string(b, ra, ca), tf_batch_get_string(b, rb, cb));
        case TF_TYPE_DATE: {
            int32_t a = tf_batch_get_date(b, ra, ca), v = tf_batch_get_date(b, rb, cb);
            return (a > v) - (a < v);
        }
        case TF_TYPE_TIMESTAMP: {
            int64_t a = tf_batch_get_timestamp(b, ra, ca), v = tf_batch_get_timestamp(b, rb, cb);
            return (a > v) - (a < v);
        }
        default:
            return 0;
    }
}

typedef struct {
    const join_state *st;
    const tf_batch *batch;
    const uint64_t *ordinals;
    int kind; /* 0=left by key, 1=lookup by key, 2=output by ordinal */
} join_spill_sort_ctx;

static int join_spill_compare_indices(const void *ctx, size_t ra, size_t rb) {
    const join_spill_sort_ctx *sort = (const join_spill_sort_ctx *)ctx;
    if (sort->kind == 2) {
        uint64_t oa = sort->ordinals[ra], ob = sort->ordinals[rb];
        return (oa > ob) - (oa < ob);
    }
    size_t col;
    if (sort->kind == 1)
        col = (sort->st->how < 2) ? (size_t)sort->st->spill_lookup_join_col : 0u;
    else
        col = (size_t)sort->st->spill_left_join_col;
    int cmp = join_spill_compare_batch_key(sort->batch, col, col, ra, rb);
    if (cmp != 0) return cmp;
    uint64_t oa = sort->ordinals[ra], ob = sort->ordinals[rb];
    return (oa > ob) - (oa < ob);
}

static size_t *join_spill_sorted_indices(const join_state *st, const tf_batch *b,
                                         const uint64_t *ordinals, int kind) {
    size_t n = b ? b->n_rows : 0;
    size_t *idx = tf_mallocarray_checked(n ? n : 1, sizeof(size_t));
    if (!idx) return NULL;
    for (size_t i = 0; i < n; i++) idx[i] = i;
    join_spill_sort_ctx ctx = { .st = st, .batch = b, .ordinals = ordinals, .kind = kind };
    tf_sort_indices(idx, n, join_spill_compare_indices, &ctx);
    return idx;
}

static int join_spill_write_exact(FILE *f, const void *ptr, size_t len) {
    return fwrite(ptr, 1, len, f) == len ? TF_OK : TF_ERROR;
}

static int join_spill_read_exact(FILE *f, void *ptr, size_t len) {
    return fread(ptr, 1, len, f) == len ? TF_OK : TF_ERROR;
}

static int join_spill_write_cell(FILE *f, const tf_batch *b, size_t r, size_t c) {
    uint8_t is_null = tf_batch_is_null(b, r, c) ? 1 : 0;
    if (join_spill_write_exact(f, &is_null, sizeof(is_null)) != TF_OK) return TF_ERROR;
    if (is_null) return TF_OK;
    switch (b->col_types[c]) {
        case TF_TYPE_BOOL: { uint8_t v = tf_batch_get_bool(b, r, c) ? 1 : 0; return join_spill_write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_INT64: { int64_t v = tf_batch_get_int64(b, r, c); return join_spill_write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_FLOAT64: { double v = tf_batch_get_float64(b, r, c); return join_spill_write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_STRING: {
            const char *str = tf_batch_get_string(b, r, c);
            uint64_t len = str ? (uint64_t)strlen(str) : 0;
            if (join_spill_write_exact(f, &len, sizeof(len)) != TF_OK) return TF_ERROR;
            return len ? join_spill_write_exact(f, str, (size_t)len) : TF_OK;
        }
        case TF_TYPE_DATE: { int32_t v = tf_batch_get_date(b, r, c); return join_spill_write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_TIMESTAMP: { int64_t v = tf_batch_get_timestamp(b, r, c); return join_spill_write_exact(f, &v, sizeof(v)); }
        default: return TF_OK;
    }
}

static int join_spill_append_path(char ***paths, size_t *n, size_t *cap, char *path) {
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

static int join_spill_write_run(join_state *st, tf_batch *batch, const uint64_t *ordinals,
                                const size_t *indices, size_t n, const char *kind) {
    if (n == 0) return TF_OK;
    char label[48];
    snprintf(label, sizeof(label), "join-%s", kind ? kind : "run");
    char *path = NULL;
    FILE *f = tf_spill_open_run_file(st->spill, label, &path);
    if (!f) return TF_ERROR;
    for (size_t i = 0; i < n; i++) {
        size_t r = indices[i];
        uint64_t ordinal = ordinals[r];
        if (join_spill_write_exact(f, &ordinal, sizeof(ordinal)) != TF_OK) goto fail;
        for (size_t c = 0; c < batch->n_cols; c++) {
            if (join_spill_write_cell(f, batch, r, c) != TF_OK) goto fail;
        }
    }
    long pos = ftell(f);
    if (pos > 0) st->spill_bytes += (size_t)pos;
    if (fclose(f) != 0) {
        tf_set_last_error("join spill: failed closing run file");
        remove(path);
        free(path);
        return TF_ERROR;
    }
    int rc;
    if (strcmp(kind, "left") == 0) {
        rc = join_spill_append_path(&st->spill_left_run_paths, &st->spill_n_left_runs, &st->spill_cap_left_runs, path);
    } else if (strcmp(kind, "lookup") == 0) {
        rc = join_spill_append_path(&st->spill_lookup_run_paths, &st->spill_n_lookup_runs, &st->spill_cap_lookup_runs, path);
    } else {
        rc = join_spill_append_path(&st->spill_out_run_paths, &st->spill_n_out_runs, &st->spill_cap_out_runs, path);
    }
    if (rc != TF_OK) { remove(path); free(path); return TF_ERROR; }
    st->spill_runs++;
    return TF_OK;
fail:
    tf_set_last_error("join spill: failed writing run file");
    fclose(f);
    remove(path);
    free(path);
    return TF_ERROR;
}

static int join_spill_write_left_run(join_state *st) {
    if (!st->spill_left_buf || st->spill_left_buf->n_rows == 0) return TF_OK;
    size_t *idx = join_spill_sorted_indices(st, st->spill_left_buf, st->spill_left_ordinals, 0);
    if (!idx) return TF_ERROR;
    int rc = join_spill_write_run(st, st->spill_left_buf, st->spill_left_ordinals, idx,
                                  st->spill_left_buf->n_rows, "left");
    free(idx);
    if (rc != TF_OK) return TF_ERROR;
    tf_batch_free(st->spill_left_buf);
    st->spill_left_buf = join_spill_create_left_batch(st, st->run_rows);
    free(st->spill_left_ordinals);
    st->spill_left_ordinals = NULL;
    st->spill_left_ordinal_cap = 0;
    return st->spill_left_buf ? TF_OK : TF_ERROR;
}

static int join_spill_write_lookup_run(join_state *st) {
    if (!st->spill_lookup_buf || st->spill_lookup_buf->n_rows == 0) return TF_OK;
    size_t *idx = join_spill_sorted_indices(st, st->spill_lookup_buf, st->spill_lookup_ordinals, 1);
    if (!idx) return TF_ERROR;
    int rc = join_spill_write_run(st, st->spill_lookup_buf, st->spill_lookup_ordinals, idx,
                                  st->spill_lookup_buf->n_rows, "lookup");
    free(idx);
    if (rc != TF_OK) return TF_ERROR;
    tf_batch_free(st->spill_lookup_buf);
    st->spill_lookup_buf = join_spill_create_lookup_batch(st, st->run_rows);
    free(st->spill_lookup_ordinals);
    st->spill_lookup_ordinals = NULL;
    st->spill_lookup_ordinal_cap = 0;
    return st->spill_lookup_buf ? TF_OK : TF_ERROR;
}

static int join_spill_write_output_run(join_state *st) {
    if (!st->spill_out_buf || st->spill_out_buf->n_rows == 0) return TF_OK;
    size_t *idx = join_spill_sorted_indices(st, st->spill_out_buf, st->spill_out_ordinals, 2);
    if (!idx) return TF_ERROR;
    int rc = join_spill_write_run(st, st->spill_out_buf, st->spill_out_ordinals, idx,
                                  st->spill_out_buf->n_rows, "out");
    free(idx);
    if (rc != TF_OK) return TF_ERROR;
    tf_batch_free(st->spill_out_buf);
    st->spill_out_buf = join_spill_create_output_batch(st, st->run_rows);
    free(st->spill_out_ordinals);
    st->spill_out_ordinals = NULL;
    st->spill_out_ordinal_cap = 0;
    return st->spill_out_buf ? TF_OK : TF_ERROR;
}

static void join_spill_row_clear(join_spill_row *row, const tf_type *types, size_t n_cols) {
    if (!row || !row->cells || !row->nulls) return;
    for (size_t c = 0; c < n_cols; c++) {
        if (!row->nulls[c] && types[c] == TF_TYPE_STRING) free(row->cells[c].str);
        row->cells[c].str = NULL;
        row->nulls[c] = 1;
    }
}

static int join_spill_row_init(join_spill_row *row, size_t n_cols) {
    row->ordinal = 0;
    row->nulls = tf_callocarray_checked(n_cols ? n_cols : 1, sizeof(uint8_t));
    row->cells = tf_callocarray_checked(n_cols ? n_cols : 1, sizeof(join_key_data));
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

static void join_spill_row_free(join_spill_row *row, const tf_type *types, size_t n_cols) {
    if (!row) return;
    join_spill_row_clear(row, types, n_cols);
    free(row->nulls);
    free(row->cells);
    row->nulls = NULL;
    row->cells = NULL;
}

static int join_spill_read_cell_value(FILE *f, join_spill_row *row, const tf_type *types, size_t c) {
    switch (types[c]) {
        case TF_TYPE_BOOL: { uint8_t v = 0; if (join_spill_read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].b = v; return TF_OK; }
        case TF_TYPE_INT64: { int64_t v = 0; if (join_spill_read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].i64 = v; return TF_OK; }
        case TF_TYPE_FLOAT64: { double v = 0; if (join_spill_read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].f64 = v; return TF_OK; }
        case TF_TYPE_STRING: {
            uint64_t len = 0;
            if (join_spill_read_exact(f, &len, sizeof(len)) != TF_OK) return TF_ERROR;
            if (len > (uint64_t)SIZE_MAX - 1) return TF_ERROR;
            char *str = malloc((size_t)len + 1);
            if (!str) return TF_ERROR;
            if (len && join_spill_read_exact(f, str, (size_t)len) != TF_OK) { free(str); return TF_ERROR; }
            str[len] = '\0';
            row->cells[c].str = str;
            return TF_OK;
        }
        case TF_TYPE_DATE: { int32_t v = 0; if (join_spill_read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].date = v; return TF_OK; }
        case TF_TYPE_TIMESTAMP: { int64_t v = 0; if (join_spill_read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].i64 = v; return TF_OK; }
        default: return TF_OK;
    }
}

static int join_spill_reader_advance(join_spill_reader *reader, const tf_type *types, size_t n_cols) {
    if (!reader || !reader->file || reader->done) return 0;
    join_spill_row_clear(&reader->row, types, n_cols);
    if (fread(&reader->row.ordinal, sizeof(reader->row.ordinal), 1, reader->file) != 1) {
        if (feof(reader->file)) {
            reader->done = 1;
            reader->has_row = 0;
            return 0;
        }
        tf_set_last_error("join spill: failed reading run file");
        return -1;
    }
    for (size_t c = 0; c < n_cols; c++) {
        uint8_t is_null = 1;
        if (join_spill_read_exact(reader->file, &is_null, sizeof(is_null)) != TF_OK) {
            tf_set_last_error("join spill: corrupt run file");
            return -1;
        }
        reader->row.nulls[c] = is_null ? 1 : 0;
        if (!reader->row.nulls[c] && join_spill_read_cell_value(reader->file, &reader->row, types, c) != TF_OK) {
            tf_set_last_error("join spill: corrupt run file");
            return -1;
        }
    }
    reader->has_row = 1;
    return 1;
}

static const tf_type *join_spill_reader_types(const join_state *st, int kind, size_t *n_cols) {
    if (kind == 1) {
        if (st->how < 2) {
            *n_cols = st->spill_lookup_n_cols;
            return st->spill_lookup_schema_types;
        }
        *n_cols = 1;
        return &st->lookup_key_type;
    }
    if (kind == 2 && st->how < 2) {
        *n_cols = st->spill_output_n_cols;
        return st->spill_output_schema_types;
    }
    *n_cols = st->spill_n_cols;
    return st->spill_schema_types;
}

static void join_spill_close_readers(join_state *st, int kind) {
    join_spill_reader **readers;
    size_t *n_readers;
    if (kind == 0) { readers = &st->spill_left_readers; n_readers = &st->spill_n_left_readers; }
    else if (kind == 1) { readers = &st->spill_lookup_readers; n_readers = &st->spill_n_lookup_readers; }
    else { readers = &st->spill_out_readers; n_readers = &st->spill_n_out_readers; }
    if (!*readers) return;
    size_t n_cols = 0;
    const tf_type *types = join_spill_reader_types(st, kind, &n_cols);
    for (size_t i = 0; i < *n_readers; i++) {
        if ((*readers)[i].file) fclose((*readers)[i].file);
        join_spill_row_free(&(*readers)[i].row, types, n_cols);
    }
    free(*readers);
    *readers = NULL;
    *n_readers = 0;
}

static void join_spill_remove_paths(char ***paths, size_t *n, size_t *cap) {
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

static int join_spill_open_readers(join_state *st, int kind) {
    char **paths;
    size_t n_paths;
    join_spill_reader **readers;
    size_t *n_readers;
    if (kind == 0) {
        paths = st->spill_left_run_paths; n_paths = st->spill_n_left_runs;
        readers = &st->spill_left_readers; n_readers = &st->spill_n_left_readers;
    } else if (kind == 1) {
        paths = st->spill_lookup_run_paths; n_paths = st->spill_n_lookup_runs;
        readers = &st->spill_lookup_readers; n_readers = &st->spill_n_lookup_readers;
    } else {
        paths = st->spill_out_run_paths; n_paths = st->spill_n_out_runs;
        readers = &st->spill_out_readers; n_readers = &st->spill_n_out_readers;
    }
    if (n_paths == 0) return TF_OK;
    *readers = tf_callocarray_checked(n_paths, sizeof(join_spill_reader));
    if (!*readers) return TF_ERROR;
    *n_readers = n_paths;
    size_t n_cols = 0;
    const tf_type *types = join_spill_reader_types(st, kind, &n_cols);
    for (size_t i = 0; i < n_paths; i++) {
        (*readers)[i].file = fopen(paths[i], "rb");
        if (!(*readers)[i].file) { tf_set_last_error("join spill: cannot reopen run file"); return TF_ERROR; }
        if (join_spill_row_init(&(*readers)[i].row, n_cols) != TF_OK) return TF_ERROR;
        int rc = join_spill_reader_advance(&(*readers)[i], types, n_cols);
        if (rc < 0) return TF_ERROR;
    }
    return TF_OK;
}

static int join_spill_compare_row_cell(const join_spill_row *a, size_t ac, tf_type at,
                                       const join_spill_row *b, size_t bc, tf_type bt) {
    if (at != bt) return (int)at - (int)bt;
    int null_a = a->nulls[ac] != 0;
    int null_b = b->nulls[bc] != 0;
    if (null_a && null_b) return 0;
    if (null_a) return 1;
    if (null_b) return -1;
    switch (at) {
        case TF_TYPE_BOOL: return (int)a->cells[ac].b - (int)b->cells[bc].b;
        case TF_TYPE_INT64:
        case TF_TYPE_TIMESTAMP: return (a->cells[ac].i64 > b->cells[bc].i64) - (a->cells[ac].i64 < b->cells[bc].i64);
        case TF_TYPE_FLOAT64: return (a->cells[ac].f64 > b->cells[bc].f64) - (a->cells[ac].f64 < b->cells[bc].f64);
        case TF_TYPE_STRING: return strcmp(a->cells[ac].str ? a->cells[ac].str : "", b->cells[bc].str ? b->cells[bc].str : "");
        case TF_TYPE_DATE: return (a->cells[ac].date > b->cells[bc].date) - (a->cells[ac].date < b->cells[bc].date);
        default: return 0;
    }
}

static int join_spill_compare_left_rows(const join_state *st, const join_spill_row *a,
                                        const join_spill_row *b) {
    size_t c = (size_t)st->spill_left_join_col;
    int cmp = join_spill_compare_row_cell(a, c, st->spill_schema_types[c], b, c, st->spill_schema_types[c]);
    if (cmp != 0) return cmp;
    return (a->ordinal > b->ordinal) - (a->ordinal < b->ordinal);
}

static int join_spill_compare_lookup_rows(const join_state *st, const join_spill_row *a,
                                          const join_spill_row *b) {
    size_t c = (st->how < 2) ? (size_t)st->spill_lookup_join_col : 0u;
    tf_type t = (st->how < 2) ? st->spill_lookup_schema_types[c] : st->lookup_key_type;
    int cmp = join_spill_compare_row_cell(a, c, t, b, c, t);
    if (cmp != 0) return cmp;
    return (a->ordinal > b->ordinal) - (a->ordinal < b->ordinal);
}

static int join_spill_compare_lookup_to_left(const join_state *st, const join_spill_row *lookup,
                                             const join_spill_row *left) {
    size_t lc = (size_t)st->spill_left_join_col;
    size_t rc = (st->how < 2) ? (size_t)st->spill_lookup_join_col : 0u;
    tf_type rt = (st->how < 2) ? st->spill_lookup_schema_types[rc] : st->lookup_key_type;
    return join_spill_compare_row_cell(lookup, rc, rt, left, lc, st->spill_schema_types[lc]);
}

static int join_spill_best_left_reader(const join_state *st) {
    int best = -1;
    for (size_t i = 0; i < st->spill_n_left_readers; i++) {
        const join_spill_reader *r = &st->spill_left_readers[i];
        if (!r->has_row || r->done) continue;
        if (best < 0) { best = (int)i; continue; }
        int cmp = join_spill_compare_left_rows(st, &r->row, &st->spill_left_readers[best].row);
        if (cmp < 0 || (cmp == 0 && i < (size_t)best)) best = (int)i;
    }
    return best;
}

static int join_spill_best_lookup_reader(const join_state *st) {
    int best = -1;
    for (size_t i = 0; i < st->spill_n_lookup_readers; i++) {
        const join_spill_reader *r = &st->spill_lookup_readers[i];
        if (!r->has_row || r->done) continue;
        if (best < 0) { best = (int)i; continue; }
        int cmp = join_spill_compare_lookup_rows(st, &r->row, &st->spill_lookup_readers[best].row);
        if (cmp < 0 || (cmp == 0 && i < (size_t)best)) best = (int)i;
    }
    return best;
}

static int join_spill_best_output_reader(const join_state *st) {
    int best = -1;
    for (size_t i = 0; i < st->spill_n_out_readers; i++) {
        const join_spill_reader *r = &st->spill_out_readers[i];
        if (!r->has_row || r->done) continue;
        if (best < 0) { best = (int)i; continue; }
        uint64_t a = r->row.ordinal;
        uint64_t b = st->spill_out_readers[best].row.ordinal;
        if (a < b || (a == b && i < (size_t)best)) best = (int)i;
    }
    return best;
}

static int join_spill_append_key_bytes(char **buf, size_t *len, size_t *cap, const void *src, size_t n) {
    size_t need = 0;
    if (tf_size_add(*len, n, &need) != TF_OK ||
        tf_size_add(need, 1, &need) != TF_OK) {
        return TF_ERROR;
    }
    if (need > *cap) {
        size_t new_cap = 0;
        if (tf_size_grow_pow2(*cap, need, 64, &new_cap) != TF_OK) return TF_ERROR;
        char *tmp = tf_reallocarray_checked(*buf, new_cap, sizeof(char));
        if (!tmp) return TF_ERROR;
        *buf = tmp;
        *cap = new_cap;
    }
    memcpy(*buf + *len, src, n);
    *len += n;
    (*buf)[*len] = '\0';
    return TF_OK;
}

static int join_spill_append_key_str(char **buf, size_t *len, size_t *cap, const char *str) {
    return join_spill_append_key_bytes(buf, len, cap, str, strlen(str));
}

static char *join_spill_build_key(const join_state *st, const join_spill_row *row, int lookup_row) {
    size_t c;
    tf_type type;
    if (lookup_row) {
        c = (st->how < 2) ? (size_t)st->spill_lookup_join_col : 0u;
        type = (st->how < 2) ? st->spill_lookup_schema_types[c] : st->lookup_key_type;
    } else {
        c = (size_t)st->spill_left_join_col;
        type = st->spill_schema_types[c];
    }
    char *buf = NULL;
    size_t len = 0, cap = 0;
    char tmp[96];
    if (row->nulls[c]) {
        snprintf(tmp, sizeof(tmp), "N:%d", (int)type);
        if (join_spill_append_key_str(&buf, &len, &cap, tmp) != TF_OK) goto fail;
    } else {
        switch (type) {
            case TF_TYPE_BOOL:
                if (join_spill_append_key_str(&buf, &len, &cap, row->cells[c].b ? "B:1" : "B:0") != TF_OK) goto fail;
                break;
            case TF_TYPE_INT64:
                snprintf(tmp, sizeof(tmp), "I:%lld", (long long)row->cells[c].i64);
                if (join_spill_append_key_str(&buf, &len, &cap, tmp) != TF_OK) goto fail;
                break;
            case TF_TYPE_FLOAT64: {
                int n = snprintf(tmp, sizeof(tmp), "F:%.17g", row->cells[c].f64);
                if (n < 0 || join_spill_append_key_bytes(&buf, &len, &cap, tmp, (size_t)n) != TF_OK) goto fail;
                break;
            }
            case TF_TYPE_STRING: {
                const char *str = row->cells[c].str ? row->cells[c].str : "";
                int n = snprintf(tmp, sizeof(tmp), "S:%zu:", strlen(str));
                if (n < 0 || join_spill_append_key_bytes(&buf, &len, &cap, tmp, (size_t)n) != TF_OK) goto fail;
                if (join_spill_append_key_str(&buf, &len, &cap, str) != TF_OK) goto fail;
                break;
            }
            case TF_TYPE_DATE:
                snprintf(tmp, sizeof(tmp), "D:%lld", (long long)row->cells[c].date);
                if (join_spill_append_key_str(&buf, &len, &cap, tmp) != TF_OK) goto fail;
                break;
            case TF_TYPE_TIMESTAMP:
                snprintf(tmp, sizeof(tmp), "T:%lld", (long long)row->cells[c].i64);
                if (join_spill_append_key_str(&buf, &len, &cap, tmp) != TF_OK) goto fail;
                break;
            default:
                if (join_spill_append_key_str(&buf, &len, &cap, "U") != TF_OK) goto fail;
                break;
        }
    }
    if (!buf) buf = strdup("");
    return buf;
fail:
    free(buf);
    return NULL;
}

static int join_spill_count_lookup_key(join_state *st, const join_spill_row *row, tf_side_channels *side) {
    char *key = join_spill_build_key(st, row, 1);
    if (!key) return TF_ERROR;
    if (!st->spill_last_lookup_key || strcmp(st->spill_last_lookup_key, key) != 0) {
        if (st->max_lookup_keys > 0 && st->spill_lookup_keys >= st->max_lookup_keys) {
            join_limit_error(side, "max_lookup_keys", st->max_lookup_keys, st->spill_lookup_keys + 1);
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

static int join_spill_set_batch_cell(tf_batch *out, size_t dst_row, size_t dst_col,
                                     const join_spill_row *row, size_t src_col,
                                     const tf_type *types) {
    tf_type type = types[src_col];
    return tf_batch_set_owned_cell_value(out, dst_row, dst_col, type,
                                         row->nulls[src_col], &row->cells[src_col]);
}

static int join_spill_row_to_batch_typed(tf_batch *out, size_t dst_row,
                                         const join_spill_row *row,
                                         const tf_type *types, size_t n_cols) {
    if (tf_batch_ensure_capacity(out, dst_row + 1) != TF_OK) return TF_ERROR;
    for (size_t c = 0; c < n_cols; c++) {
        if (join_spill_set_batch_cell(out, dst_row, c, row, c, types) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static int join_spill_row_to_batch(const join_state *st, tf_batch *out, size_t dst_row,
                                   const join_spill_row *row) {
    return join_spill_row_to_batch_typed(out, dst_row, row,
                                         st->spill_schema_types, st->spill_n_cols);
}

static int join_spill_append_selected_row(join_state *st, const join_spill_row *row,
                                          tf_side_channels *side) {
    if (!join_reserve_output_row(st, side)) return TF_ERROR;
    size_t dst = st->spill_out_buf->n_rows;
    if (join_ensure_ordinals(&st->spill_out_ordinals,
                             &st->spill_out_ordinal_cap, dst + 1) != TF_OK) {
        return TF_ERROR;
    }
    if (join_spill_row_to_batch(st, st->spill_out_buf, dst, row) != TF_OK) return TF_ERROR;
    st->spill_out_ordinals[dst] = row->ordinal;
    st->spill_out_buf->n_rows = dst + 1;
    st->spill_kept_rows++;
    if (st->spill_out_buf->n_rows >= st->run_rows) return join_spill_write_output_run(st);
    return TF_OK;
}

static int join_spill_capture_lookup_schema(join_state *st, const tf_batch *batch,
                                            int lookup_ci, tf_side_channels *side) {
    if (st->how >= 2) return TF_OK;
    if (lookup_ci < 0) return TF_ERROR;

    if (st->spill_lookup_n_cols > 0) {
        if (batch->n_cols != st->spill_lookup_n_cols || lookup_ci != st->spill_lookup_join_col) {
            join_write_error(side, "join spill: lookup schema changed across batches");
            return TF_ERROR;
        }
        for (size_t c = 0; c < batch->n_cols; c++) {
            if (batch->col_types[c] != st->spill_lookup_schema_types[c] ||
                strcmp(batch->col_names[c] ? batch->col_names[c] : "",
                       st->spill_lookup_schema_names[c] ? st->spill_lookup_schema_names[c] : "") != 0) {
                join_write_error(side, "join spill: lookup schema changed across batches");
                return TF_ERROR;
            }
        }
        return TF_OK;
    }

    st->spill_lookup_n_cols = batch->n_cols;
    st->spill_lookup_join_col = lookup_ci;
    st->spill_lookup_schema_names = tf_callocarray_checked(batch->n_cols ? batch->n_cols : 1, sizeof(char *));
    st->spill_lookup_schema_types = tf_callocarray_checked(batch->n_cols ? batch->n_cols : 1, sizeof(tf_type));
    if (!st->spill_lookup_schema_names || !st->spill_lookup_schema_types) return TF_ERROR;
    for (size_t c = 0; c < batch->n_cols; c++) {
        st->spill_lookup_schema_names[c] = strdup(batch->col_names[c] ? batch->col_names[c] : "");
        if (!st->spill_lookup_schema_names[c]) return TF_ERROR;
        st->spill_lookup_schema_types[c] = batch->col_types[c];
    }

    st->lookup_key_type = batch->col_types[lookup_ci];
    st->have_lookup_key_type = 1;

    st->lookup_out_cols = tf_mallocarray_checked(batch->n_cols ? batch->n_cols : 1, sizeof(int));
    if (!st->lookup_out_cols) return TF_ERROR;
    st->n_lookup_out = 0;
    for (size_t c = 0; c < batch->n_cols; c++) {
        if ((int)c != lookup_ci) st->lookup_out_cols[st->n_lookup_out++] = (int)c;
    }

    st->spill_output_n_cols = st->spill_n_cols + st->n_lookup_out;
    st->spill_output_schema_names = tf_callocarray_checked(st->spill_output_n_cols ? st->spill_output_n_cols : 1, sizeof(char *));
    st->spill_output_schema_types = tf_callocarray_checked(st->spill_output_n_cols ? st->spill_output_n_cols : 1, sizeof(tf_type));
    if (!st->spill_output_schema_names || !st->spill_output_schema_types) return TF_ERROR;
    for (size_t c = 0; c < st->spill_n_cols; c++) {
        st->spill_output_schema_names[c] = strdup(st->spill_schema_names[c] ? st->spill_schema_names[c] : "");
        if (!st->spill_output_schema_names[c]) return TF_ERROR;
        st->spill_output_schema_types[c] = st->spill_schema_types[c];
    }
    for (size_t k = 0; k < st->n_lookup_out; k++) {
        int lc = st->lookup_out_cols[k];
        size_t oc = st->spill_n_cols + k;
        st->spill_output_schema_names[oc] = strdup(st->spill_lookup_schema_names[lc] ? st->spill_lookup_schema_names[lc] : "");
        if (!st->spill_output_schema_names[oc]) return TF_ERROR;
        st->spill_output_schema_types[oc] = st->spill_lookup_schema_types[lc];
    }

    st->spill_lookup_buf = join_spill_create_lookup_batch(st, st->run_rows);
    st->spill_out_buf = join_spill_create_output_batch(st, st->run_rows);
    if (!st->spill_lookup_buf || !st->spill_out_buf) return TF_ERROR;
    return TF_OK;
}

static int join_spill_process_lookup_batch(join_state *st, const tf_batch *batch,
                                           tf_side_channels *side) {
    int lookup_ci = tf_batch_col_index(batch, st->right_col);
    if (lookup_ci < 0) {
        join_write_error(side, "join spill: lookup file is missing join column");
        return TF_ERROR;
    }
    if (batch->col_types[lookup_ci] != st->spill_schema_types[st->spill_left_join_col]) {
        join_write_error(side, "join spill: left and lookup join key types differ");
        return TF_ERROR;
    }
    if (st->how < 2) {
        if (join_spill_capture_lookup_schema(st, batch, lookup_ci, side) != TF_OK) return TF_ERROR;
    } else if (!st->have_lookup_key_type) {
        st->lookup_key_type = batch->col_types[lookup_ci];
        st->have_lookup_key_type = 1;
        st->spill_lookup_buf = join_spill_create_lookup_batch(st, st->run_rows);
        if (!st->spill_lookup_buf) return TF_ERROR;
    }
    for (size_t r = 0; r < batch->n_rows; r++) {
        if (st->max_lookup_rows > 0 && st->lookup_rows >= st->max_lookup_rows) {
            join_limit_error(side, "max_lookup_rows", st->max_lookup_rows, st->lookup_rows + 1);
            return TF_ERROR;
        }
        size_t dst = st->spill_lookup_buf->n_rows;
        if (tf_batch_ensure_capacity(st->spill_lookup_buf, dst + 1) != TF_OK) return TF_ERROR;
        if (st->how < 2) {
            if (tf_batch_copy_row(st->spill_lookup_buf, dst, batch, r) != TF_OK) return TF_ERROR;
        } else {
            if (tf_batch_copy_cell_index(st->spill_lookup_buf, dst, 0, batch, r, lookup_ci) != TF_OK) return TF_ERROR;
        }
        if (join_ensure_ordinals(&st->spill_lookup_ordinals, &st->spill_lookup_ordinal_cap, dst + 1) != TF_OK) return TF_ERROR;
        st->spill_lookup_ordinals[dst] = st->spill_next_lookup_ordinal++;
        st->spill_lookup_buf->n_rows = dst + 1;
        st->lookup_rows++;
        if (st->spill_lookup_buf->n_rows >= st->run_rows && join_spill_write_lookup_run(st) != TF_OK)
            return TF_ERROR;
    }
    return TF_OK;
}

static int join_spill_load_lookup_runs(join_state *st, tf_side_channels *side) {
    if (st->spill_lookup_loaded) return TF_OK;
    st->spill_lookup_loaded = 1;
    if (!csv_file_header_has_column(st->file, st->right_col)) {
        join_write_error(side, "join spill: lookup file is missing join column");
        return TF_ERROR;
    }
    FILE *f = fopen(st->file, "rb");
    if (!f) {
        join_write_error(side, "join spill: cannot open lookup file");
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
                join_limit_error(side, "max_lookup_bytes", st->max_lookup_bytes, bytes_read);
                rc = TF_ERROR;
                break;
            }
            rc = dec->decode(dec, buf, n, &batches, &n_batches, side);
        } else {
            if (ferror(f)) {
                join_write_error(side, "join spill: failed reading lookup file");
                rc = TF_ERROR;
                break;
            }
            flushed = 1;
            rc = dec->flush(dec, &batches, &n_batches, side);
        }
        if (rc != TF_OK) {
            free_batch_array(batches, n_batches);
            break;
        }
        for (size_t i = 0; i < n_batches; i++) {
            if (join_spill_process_lookup_batch(st, batches[i], side) != TF_OK) rc = TF_ERROR;
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
        return join_spill_write_lookup_run(st);
    if (st->spill_lookup_buf) {
        tf_batch_free(st->spill_lookup_buf);
        st->spill_lookup_buf = NULL;
    }
    return TF_OK;
}

static int join_spill_output_ordinal(join_state *st, uint64_t left_ordinal,
                                     size_t match_idx, uint64_t *out,
                                     tf_side_channels *side) {
    if (st->how >= 2) {
        *out = left_ordinal;
        return TF_OK;
    }
    if (st->max_matches_per_row == 0) {
        join_write_error(side, "join spill: inner/left joins need max_matches_per_row to bound lookup key runs");
        return TF_ERROR;
    }
    if (match_idx >= st->max_matches_per_row) {
        join_limit_error_context(side, "max_matches_per_row", st->max_matches_per_row,
                                 match_idx + 1, "for spilled lookup key");
        return TF_ERROR;
    }
    uint64_t slots = (uint64_t)st->max_matches_per_row;
    if (slots == 0 || left_ordinal > (UINT64_MAX - (uint64_t)match_idx) / slots) {
        join_write_error(side, "join spill: output ordinal overflow");
        return TF_ERROR;
    }
    *out = left_ordinal * slots + (uint64_t)match_idx;
    return TF_OK;
}

static int join_spill_append_joined_row(join_state *st, const join_spill_row *left,
                                        const tf_batch *lookup_run, size_t match_idx,
                                        tf_side_channels *side) {
    if (!st->spill_out_buf) {
        st->spill_out_buf = join_spill_create_output_batch(st, st->run_rows);
        if (!st->spill_out_buf) return TF_ERROR;
    }
    if (!join_reserve_output_row(st, side)) return TF_ERROR;
    size_t dst = st->spill_out_buf->n_rows;
    if (tf_batch_ensure_capacity(st->spill_out_buf, dst + 1) != TF_OK) return TF_ERROR;
    if (join_ensure_ordinals(&st->spill_out_ordinals, &st->spill_out_ordinal_cap, dst + 1) != TF_OK)
        return TF_ERROR;
    uint64_t output_ordinal = 0;
    if (join_spill_output_ordinal(st, left->ordinal, lookup_run ? match_idx : 0, &output_ordinal, side) != TF_OK)
        return TF_ERROR;

    for (size_t c = 0; c < st->spill_n_cols; c++) {
        if (join_spill_set_batch_cell(st->spill_out_buf, dst, c, left, c,
                                      st->spill_schema_types) != TF_OK) return TF_ERROR;
    }
    if (lookup_run) {
        for (size_t k = 0; k < st->n_lookup_out; k++) {
            size_t src = (size_t)st->lookup_out_cols[k];
            if (tf_batch_is_null(lookup_run, match_idx, src)) {
                if (tf_batch_set_null(st->spill_out_buf, dst, st->spill_n_cols + k) != TF_OK) return TF_ERROR;
            } else {
                if (tf_batch_copy_cell_index(st->spill_out_buf, dst, st->spill_n_cols + k,
                                             lookup_run, match_idx, (int)src) != TF_OK) return TF_ERROR;
            }
        }
    } else {
        for (size_t k = 0; k < st->n_lookup_out; k++) {
            if (tf_batch_set_null(st->spill_out_buf, dst, st->spill_n_cols + k) != TF_OK) return TF_ERROR;
        }
    }

    st->spill_out_ordinals[dst] = output_ordinal;
    st->spill_out_buf->n_rows = dst + 1;
    st->spill_kept_rows++;
    if (st->spill_out_buf->n_rows >= st->run_rows) return join_spill_write_output_run(st);
    return TF_OK;
}

static int join_spill_drain_lookup_before_left(join_state *st, const join_spill_row *left,
                                               tf_side_channels *side) {
    for (;;) {
        int lookup_idx = join_spill_best_lookup_reader(st);
        if (lookup_idx < 0) return TF_OK;
        join_spill_reader *lookup = &st->spill_lookup_readers[lookup_idx];
        if (join_spill_count_lookup_key(st, &lookup->row, side) != TF_OK) return TF_ERROR;
        int cmp = join_spill_compare_lookup_to_left(st, &lookup->row, left);
        if (cmp >= 0) return TF_OK;
        int adv = join_spill_reader_advance(lookup, st->spill_lookup_schema_types, st->spill_lookup_n_cols);
        if (adv < 0) return TF_ERROR;
    }
}

static int join_spill_collect_lookup_run(join_state *st, const join_spill_row *left,
                                         tf_batch **run_out, tf_side_channels *side) {
    *run_out = NULL;
    if (join_spill_drain_lookup_before_left(st, left, side) != TF_OK) return TF_ERROR;
    int lookup_idx = join_spill_best_lookup_reader(st);
    if (lookup_idx < 0) return TF_OK;
    join_spill_reader *lookup = &st->spill_lookup_readers[lookup_idx];
    if (join_spill_count_lookup_key(st, &lookup->row, side) != TF_OK) return TF_ERROR;
    if (join_spill_compare_lookup_to_left(st, &lookup->row, left) != 0) return TF_OK;

    size_t initial = st->max_matches_per_row < 16 ? st->max_matches_per_row : 16;
    tf_batch *run = join_spill_create_lookup_batch(st, initial ? initial : 16);
    if (!run) return TF_ERROR;

    for (;;) {
        lookup_idx = join_spill_best_lookup_reader(st);
        if (lookup_idx < 0) break;
        lookup = &st->spill_lookup_readers[lookup_idx];
        int cmp = join_spill_compare_lookup_to_left(st, &lookup->row, left);
        if (cmp != 0) break;
        if (join_spill_count_lookup_key(st, &lookup->row, side) != TF_OK) { tf_batch_free(run); return TF_ERROR; }
        if (st->max_matches_per_row > 0 && run->n_rows >= st->max_matches_per_row) {
            join_limit_error_context(side, "max_matches_per_row", st->max_matches_per_row,
                                     run->n_rows + 1, "for spilled lookup key");
            tf_batch_free(run);
            return TF_ERROR;
        }
        size_t dst = run->n_rows;
        if (join_spill_row_to_batch_typed(run, dst, &lookup->row,
                                          st->spill_lookup_schema_types,
                                          st->spill_lookup_n_cols) != TF_OK) {
            tf_batch_free(run);
            return TF_ERROR;
        }
        run->n_rows = dst + 1;
        int adv = join_spill_reader_advance(lookup, st->spill_lookup_schema_types, st->spill_lookup_n_cols);
        if (adv < 0) { tf_batch_free(run); return TF_ERROR; }
    }
    *run_out = run;
    return TF_OK;
}

static int join_spill_produce_mutating_output_runs(join_state *st, tf_side_channels *side) {
    if (st->spill_left_buf && st->spill_left_buf->n_rows > 0 && join_spill_write_left_run(st) != TF_OK)
        return TF_ERROR;
    if (st->spill_left_buf) { tf_batch_free(st->spill_left_buf); st->spill_left_buf = NULL; }
    free(st->spill_left_ordinals); st->spill_left_ordinals = NULL; st->spill_left_ordinal_cap = 0;

    if (join_spill_load_lookup_runs(st, side) != TF_OK) return TF_ERROR;
    if (st->spill_lookup_n_cols == 0) {
        join_write_error(side, "join spill: inner/left joins need a non-empty lookup to infer output schema");
        return TF_ERROR;
    }
    if (join_spill_open_readers(st, 0) != TF_OK) return TF_ERROR;
    if (join_spill_open_readers(st, 1) != TF_OK) return TF_ERROR;

    for (;;) {
        int left_idx = join_spill_best_left_reader(st);
        if (left_idx < 0) break;
        join_spill_reader *left = &st->spill_left_readers[left_idx];
        char *left_key = join_spill_build_key(st, &left->row, 0);
        if (!left_key) return TF_ERROR;

        tf_batch *lookup_run = NULL;
        if (join_spill_collect_lookup_run(st, &left->row, &lookup_run, side) != TF_OK) {
            free(left_key);
            return TF_ERROR;
        }

        for (;;) {
            left_idx = join_spill_best_left_reader(st);
            if (left_idx < 0) break;
            left = &st->spill_left_readers[left_idx];
            char *cur_key = join_spill_build_key(st, &left->row, 0);
            if (!cur_key) { free(left_key); tf_batch_free(lookup_run); return TF_ERROR; }
            int same_key = strcmp(cur_key, left_key) == 0;
            free(cur_key);
            if (!same_key) break;

            if (lookup_run && lookup_run->n_rows > 0) {
                for (size_t m = 0; m < lookup_run->n_rows; m++) {
                    if (join_spill_append_joined_row(st, &left->row, lookup_run, m, side) != TF_OK) {
                        free(left_key);
                        tf_batch_free(lookup_run);
                        return TF_ERROR;
                    }
                }
            } else if (st->how == 1) {
                if (join_spill_append_joined_row(st, &left->row, NULL, 0, side) != TF_OK) {
                    free(left_key);
                    tf_batch_free(lookup_run);
                    return TF_ERROR;
                }
            }

            int adv_left = join_spill_reader_advance(left, st->spill_schema_types, st->spill_n_cols);
            if (adv_left < 0) { free(left_key); tf_batch_free(lookup_run); return TF_ERROR; }
        }

        free(left_key);
        tf_batch_free(lookup_run);
    }

    for (;;) {
        int lookup_idx = join_spill_best_lookup_reader(st);
        if (lookup_idx < 0) break;
        join_spill_reader *lookup = &st->spill_lookup_readers[lookup_idx];
        if (join_spill_count_lookup_key(st, &lookup->row, side) != TF_OK) return TF_ERROR;
        int adv = join_spill_reader_advance(lookup, st->spill_lookup_schema_types, st->spill_lookup_n_cols);
        if (adv < 0) return TF_ERROR;
    }
    if (st->spill_out_buf && st->spill_out_buf->n_rows > 0 && join_spill_write_output_run(st) != TF_OK)
        return TF_ERROR;
    join_spill_close_readers(st, 0);
    join_spill_close_readers(st, 1);
    join_spill_remove_paths(&st->spill_left_run_paths, &st->spill_n_left_runs, &st->spill_cap_left_runs);
    join_spill_remove_paths(&st->spill_lookup_run_paths, &st->spill_n_lookup_runs, &st->spill_cap_lookup_runs);
    return TF_OK;
}

static int join_spill_produce_output_runs(join_state *st, tf_side_channels *side) {
    if (st->spill_key_merge_done) return TF_OK;
    if (!st->spill_has_schema) {
        st->spill_key_merge_done = 1;
        return TF_OK;
    }
    if (st->how < 2) {
        int rc = join_spill_produce_mutating_output_runs(st, side);
        if (rc != TF_OK) return rc;
        st->spill_key_merge_done = 1;
        return TF_OK;
    }
    if (st->spill_left_buf && st->spill_left_buf->n_rows > 0 && join_spill_write_left_run(st) != TF_OK)
        return TF_ERROR;
    if (st->spill_left_buf) { tf_batch_free(st->spill_left_buf); st->spill_left_buf = NULL; }
    free(st->spill_left_ordinals); st->spill_left_ordinals = NULL; st->spill_left_ordinal_cap = 0;

    if (join_spill_load_lookup_runs(st, side) != TF_OK) return TF_ERROR;
    if (join_spill_open_readers(st, 0) != TF_OK) return TF_ERROR;
    if (join_spill_open_readers(st, 1) != TF_OK) return TF_ERROR;

    for (;;) {
        int left_idx = join_spill_best_left_reader(st);
        if (left_idx < 0) break;
        join_spill_reader *left = &st->spill_left_readers[left_idx];
        int lookup_idx = join_spill_best_lookup_reader(st);
        while (lookup_idx >= 0) {
            join_spill_reader *lookup = &st->spill_lookup_readers[lookup_idx];
            if (join_spill_count_lookup_key(st, &lookup->row, side) != TF_OK) return TF_ERROR;
            int cmp = join_spill_compare_lookup_to_left(st, &lookup->row, &left->row);
            if (cmp >= 0) break;
            int adv = join_spill_reader_advance(lookup, &st->lookup_key_type, 1);
            if (adv < 0) return TF_ERROR;
            lookup_idx = join_spill_best_lookup_reader(st);
        }
        int has_match = 0;
        if (lookup_idx >= 0) {
            join_spill_reader *lookup = &st->spill_lookup_readers[lookup_idx];
            if (join_spill_count_lookup_key(st, &lookup->row, side) != TF_OK) return TF_ERROR;
            has_match = join_spill_compare_lookup_to_left(st, &lookup->row, &left->row) == 0;
        }
        int keep = (st->how == 2) ? has_match : !has_match;
        if (keep && join_spill_append_selected_row(st, &left->row, side) != TF_OK) return TF_ERROR;
        int adv_left = join_spill_reader_advance(left, st->spill_schema_types, st->spill_n_cols);
        if (adv_left < 0) return TF_ERROR;
    }
    for (;;) {
        int lookup_idx = join_spill_best_lookup_reader(st);
        if (lookup_idx < 0) break;
        join_spill_reader *lookup = &st->spill_lookup_readers[lookup_idx];
        if (join_spill_count_lookup_key(st, &lookup->row, side) != TF_OK) return TF_ERROR;
        int adv = join_spill_reader_advance(lookup, &st->lookup_key_type, 1);
        if (adv < 0) return TF_ERROR;
    }
    if (st->spill_out_buf && st->spill_out_buf->n_rows > 0 && join_spill_write_output_run(st) != TF_OK)
        return TF_ERROR;
    join_spill_close_readers(st, 0);
    join_spill_close_readers(st, 1);
    join_spill_remove_paths(&st->spill_left_run_paths, &st->spill_n_left_runs, &st->spill_cap_left_runs);
    join_spill_remove_paths(&st->spill_lookup_run_paths, &st->spill_n_lookup_runs, &st->spill_cap_lookup_runs);
    st->spill_key_merge_done = 1;
    return TF_OK;
}

static int join_spill_begin_output_merge(join_state *st) {
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
    return join_spill_open_readers(st, 2);
}

static int join_spill_output_next_batch(join_state *st, tf_batch **out, tf_side_channels *side) {
    *out = NULL;
    if (join_spill_produce_output_runs(st, side) != TF_OK) return TF_ERROR;
    if (join_spill_begin_output_merge(st) != TF_OK) return TF_ERROR;
    if (st->spill_output_merge_done) return TF_OK;

    tf_batch *ob = join_spill_create_output_batch(st, st->output_batch_rows);
    if (!ob) return TF_ERROR;
    size_t out_n_cols = 0;
    const tf_type *out_types = join_spill_reader_types(st, 2, &out_n_cols);
    while (ob->n_rows < st->output_batch_rows) {
        int best = join_spill_best_output_reader(st);
        if (best < 0) break;
        join_spill_reader *reader = &st->spill_out_readers[best];
        if (join_spill_row_to_batch_typed(ob, ob->n_rows, &reader->row,
                                          out_types, out_n_cols) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        ob->n_rows++;
        int adv = join_spill_reader_advance(reader, out_types, out_n_cols);
        if (adv < 0) { tf_batch_free(ob); return TF_ERROR; }
    }
    if (ob->n_rows == 0) {
        tf_batch_free(ob);
        join_spill_close_readers(st, 2);
        join_spill_remove_paths(&st->spill_out_run_paths, &st->spill_n_out_runs, &st->spill_cap_out_runs);
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

static int join_process_spill(join_state *st, tf_batch *in, tf_batch **out, tf_side_channels *side) {
    *out = NULL;
    if (join_spill_init_schema(st, in, side) != TF_OK) return TF_ERROR;
    for (size_t r = 0; r < in->n_rows; r++) {
        size_t dst = st->spill_left_buf->n_rows;
        if (tf_batch_copy_row(st->spill_left_buf, dst, in, r) != TF_OK) return TF_ERROR;
        if (join_ensure_ordinals(&st->spill_left_ordinals, &st->spill_left_ordinal_cap, dst + 1) != TF_OK) return TF_ERROR;
        st->spill_left_ordinals[dst] = st->spill_next_left_ordinal++;
        st->spill_left_buf->n_rows = dst + 1;
        if (st->spill_left_buf->n_rows >= st->run_rows && join_spill_write_left_run(st) != TF_OK)
            return TF_ERROR;
    }
    return TF_OK;
}

static size_t join_spill_retained_state_bytes(const join_state *st) {
    if (!st || !st->use_spill) return 0;
    size_t total = 0;
    total += join_batch_storage_bytes(st->spill_left_buf);
    total += join_batch_storage_bytes(st->spill_lookup_buf);
    total += join_batch_storage_bytes(st->spill_out_buf);
    total += st->spill_left_ordinal_cap * sizeof(uint64_t);
    total += st->spill_lookup_ordinal_cap * sizeof(uint64_t);
    total += st->spill_out_ordinal_cap * sizeof(uint64_t);
    total += st->spill_n_left_readers * sizeof(join_spill_reader);
    total += st->spill_n_lookup_readers * sizeof(join_spill_reader);
    total += st->spill_n_out_readers * sizeof(join_spill_reader);
    total += st->spill_last_lookup_key ? strlen(st->spill_last_lookup_key) + 1 : 0;
    return total;
}

static int sorted_join_process(tf_step *self, tf_batch *in, tf_batch **out,
                               tf_side_channels *side) {
    join_state *st = self->state;
    *out = NULL;

    int left_ci = tf_batch_col_index(in, st->left_col);
    if (left_ci < 0) return TF_ERROR;

    if (!st->loaded) {
        if (sorted_join_open(st, side) != TF_OK) return TF_ERROR;
        if (sorted_join_advance_lookup(st, side) != TF_OK) return TF_ERROR;
        if (st->how < 2) {
            if (!st->sorted_have_row) {
                join_write_error(side, "sorted join: mutating joins need a non-empty lookup to infer output schema");
                return TF_ERROR;
            }
            if (st->max_matches_per_row == 0) {
                join_write_error(side, "sorted join: inner/left joins need max_matches_per_row to bound the current lookup key run");
                return TF_ERROR;
            }
        }
        st->loaded = 1;
    }

    size_t n_out_cols = (st->how >= 2) ? in->n_cols : in->n_cols + st->n_lookup_out;
    size_t out_cap = in->n_rows > 0 ? in->n_rows : 16;
    tf_batch *ob = tf_batch_create(n_out_cols, out_cap);
    if (!ob) return TF_ERROR;

    for (size_t c = 0; c < in->n_cols; c++) {
        if (tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    if (st->how < 2) {
        for (size_t k = 0; k < st->n_lookup_out; k++) {
            int lc = st->lookup_out_cols[k];
            if (tf_batch_set_schema(ob, in->n_cols + k,
                                    st->lookup_schema_names[lc],
                                    st->lookup_schema_types[lc]) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
        }
    }

    size_t out_row = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        if (sorted_join_check_left_order(st, in, r, left_ci, side) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (sorted_join_ensure_run_at_least(st, in, r, left_ci, side) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }

        int has_match = 0;
        if (st->have_right_run) {
            int cmp = 0;
            if (join_key_compare_value_to_cell(&st->right_run_key, in, r, left_ci, &cmp) != TF_OK) {
                join_write_error(side, "sorted join: left and lookup join key types differ");
                tf_batch_free(ob);
                return TF_ERROR;
            }
            has_match = (cmp == 0);
        }

        if (st->how == 2 || st->how == 3) {
            int keep = (st->how == 2) ? has_match : !has_match;
            if (keep) {
                if (!join_reserve_output_row(st, side)) { tf_batch_free(ob); return TF_ERROR; }
                if (tf_batch_ensure_capacity(ob, out_row + 1) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
                for (size_t c = 0; c < in->n_cols; c++) {
                    if (tf_batch_copy_cell_index(ob, out_row, c, in, r, (int)c) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
                }
                ob->n_rows = ++out_row;
            }
        } else if (has_match) {
            for (size_t m = 0; m < st->right_run->n_rows; m++) {
                if (!join_reserve_output_row(st, side)) { tf_batch_free(ob); return TF_ERROR; }
                if (tf_batch_ensure_capacity(ob, out_row + 1) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
                for (size_t c = 0; c < in->n_cols; c++) {
                    if (tf_batch_copy_cell_index(ob, out_row, c, in, r, (int)c) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
                }
                for (size_t k = 0; k < st->n_lookup_out; k++) {
                    if (tf_batch_copy_cell_index(ob, out_row, in->n_cols + k, st->right_run, m, st->lookup_out_cols[k]) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
                }
                ob->n_rows = ++out_row;
            }
        } else if (st->how == 1) {
            if (!join_reserve_output_row(st, side)) { tf_batch_free(ob); return TF_ERROR; }
            if (tf_batch_ensure_capacity(ob, out_row + 1) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            for (size_t c = 0; c < in->n_cols; c++) {
                if (tf_batch_copy_cell_index(ob, out_row, c, in, r, (int)c) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            }
            for (size_t k = 0; k < st->n_lookup_out; k++) {
                if (tf_batch_set_null(ob, out_row, in->n_cols + k) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            }
            ob->n_rows = ++out_row;
        }
    }

    if (out_row > 0)
        *out = ob;
    else
        tf_batch_free(ob);

    return TF_OK;
}

static int join_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    join_state *st = self->state;
    *out = NULL;
    if (st->sorted) return sorted_join_process(self, in, out, side);
    if (st->use_spill) return join_process_spill(st, in, out, side);

    if (!st->loaded) {
        if (load_lookup(st, side) != TF_OK) return TF_ERROR;
        st->loaded = 1;
    }

    int left_ci = tf_batch_col_index(in, st->left_col);
    if (left_ci < 0) return TF_ERROR;
    if (st->have_lookup_key_type && in->col_types[left_ci] != st->lookup_key_type) {
        join_write_error(side, "join: left and lookup join key types differ");
        return TF_ERROR;
    }

    size_t n_out_cols = (st->how >= 2) ? in->n_cols : in->n_cols + st->n_lookup_out;

    /* Filtering joins retain at most one output row per input row. Mutating
     * joins may grow if lookup keys have duplicate matches. */
    size_t out_cap = in->n_rows > 0 ? in->n_rows : 16;
    tf_batch *ob = tf_batch_create(n_out_cols, out_cap);
    if (!ob) return TF_ERROR;

    /* Set schema: main columns, plus lookup columns for mutating joins. */
    for (size_t c = 0; c < in->n_cols; c++) {
        if (tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    if (st->how < 2) {
        for (size_t k = 0; k < st->n_lookup_out; k++) {
            int lc = st->lookup_out_cols[k];
            if (tf_batch_set_schema(ob, in->n_cols + k,
                                    st->lookup->col_names[lc],
                                    st->lookup->col_types[lc]) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
        }
    }

    size_t out_row = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        char *key = format_join_key(in, r, left_ci);
        if (!key) continue;

        join_bucket *bucket = map_find(&st->map, key);
        free(key);

        if (st->how == 2 || st->how == 3) {
            int keep = (st->how == 2) ? (bucket != NULL) : (bucket == NULL);
            if (keep) {
                if (!join_reserve_output_row(st, side)) { tf_batch_free(ob); return TF_ERROR; }
                if (tf_batch_ensure_capacity(ob, out_row + 1) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
                for (size_t c = 0; c < in->n_cols; c++) {
                    if (tf_batch_copy_cell_index(ob, out_row, c, in, r, (int)c) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
                }
                ob->n_rows = ++out_row;
            }
        } else if (bucket) {
            if (st->max_matches_per_row > 0 && bucket->n_rows > st->max_matches_per_row) {
                join_limit_error_context(side, "max_matches_per_row",
                                         st->max_matches_per_row, bucket->n_rows,
                                         "for input row");
                tf_batch_free(ob);
                return TF_ERROR;
            }
            /* Emit one row per match */
            for (size_t m = 0; m < bucket->n_rows; m++) {
                size_t lr = bucket->rows[m];
                if (!join_reserve_output_row(st, side)) { tf_batch_free(ob); return TF_ERROR; }
                if (tf_batch_ensure_capacity(ob, out_row + 1) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
                /* Copy main columns */
                for (size_t c = 0; c < in->n_cols; c++) {
                    if (tf_batch_copy_cell_index(ob, out_row, c, in, r, (int)c) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
                }
                /* Copy lookup columns */
                for (size_t k = 0; k < st->n_lookup_out; k++) {
                    if (tf_batch_copy_cell_index(ob, out_row, in->n_cols + k, st->lookup, lr, st->lookup_out_cols[k]) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
                }
                ob->n_rows = ++out_row;
            }
        } else if (st->how == 1) {
            /* Left join: emit main + nulls */
            if (!join_reserve_output_row(st, side)) { tf_batch_free(ob); return TF_ERROR; }
            if (tf_batch_ensure_capacity(ob, out_row + 1) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            for (size_t c = 0; c < in->n_cols; c++) {
                if (tf_batch_copy_cell_index(ob, out_row, c, in, r, (int)c) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            }
            for (size_t k = 0; k < st->n_lookup_out; k++) {
                if (tf_batch_set_null(ob, out_row, in->n_cols + k) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            }
            ob->n_rows = ++out_row;
        }
        /* Inner join + no match: skip */
    }

    if (out_row > 0)
        *out = ob;
    else
        tf_batch_free(ob);

    return TF_OK;
}

static int join_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    join_state *st = self ? self->state : NULL;
    *out = NULL;
    if (st && st->use_spill) return join_spill_output_next_batch(st, out, side);
    (void)side;
    return TF_OK;
}

static int join_flush_next(tf_step *self, tf_batch **out, tf_side_channels *side) {
    join_state *st = self ? self->state : NULL;
    *out = NULL;
    if (st && st->use_spill) return join_spill_output_next_batch(st, out, side);
    (void)side;
    return TF_OK;
}

static size_t join_type_storage_size(tf_type type) {
    switch (type) {
        case TF_TYPE_BOOL:      return sizeof(uint8_t);
        case TF_TYPE_INT64:     return sizeof(int64_t);
        case TF_TYPE_FLOAT64:   return sizeof(double);
        case TF_TYPE_STRING:    return sizeof(char *);
        case TF_TYPE_DATE:      return sizeof(int32_t);
        case TF_TYPE_TIMESTAMP: return sizeof(int64_t);
        default:                return 0;
    }
}

static size_t join_batch_storage_bytes(const tf_batch *b) {
    if (!b) return 0;
    size_t total = sizeof(*b) +
                   b->n_cols * (sizeof(char *) + sizeof(tf_type) +
                                sizeof(void *) + sizeof(uint8_t *));
    for (size_t c = 0; c < b->n_cols; c++) {
        if (b->col_names && b->col_names[c]) total += strlen(b->col_names[c]) + 1;
        size_t cell_size = join_type_storage_size(b->col_types[c]);
        total += cell_size * b->capacity;
        total += b->capacity;
        if (b->col_types[c] == TF_TYPE_STRING) {
            for (size_t r = 0; r < b->n_rows; r++) {
                if (!tf_batch_is_null(b, r, c)) {
                    const char *s = tf_batch_get_string(b, r, c);
                    if (s) total += strlen(s) + 1;
                }
            }
        }
    }
    return total;
}

static size_t join_hash_map_retained_bytes(const join_hash_map *map) {
    if (!map) return 0;
    return map->n_buckets * sizeof(join_bucket) +
           map->key_bytes +
           map->row_ref_capacity * sizeof(size_t);
}

static size_t join_unsorted_retained_state_bytes(const join_hash_map *map,
                                                 const tf_batch *lookup,
                                                 size_t n_lookup_out) {
    return join_hash_map_retained_bytes(map) +
           join_batch_storage_bytes(lookup) +
           n_lookup_out * sizeof(int);
}

static int join_check_unsorted_state_bytes(const join_state *st,
                                           const join_hash_map *map,
                                           const tf_batch *lookup,
                                           size_t n_lookup_out,
                                           tf_side_channels *side) {
    if (!st || st->max_state_bytes == 0) return TF_OK;
    size_t retained = join_unsorted_retained_state_bytes(map, lookup, n_lookup_out);
    if (retained <= st->max_state_bytes) return TF_OK;
    if (join_limit_error_context(side, "max_state_bytes", st->max_state_bytes, retained,
                                 "while tracking lookup state") != TF_OK) {
        return TF_ERROR;
    }
    return TF_ERROR;
}

static size_t join_key_value_retained_bytes(const join_key_value *v) {
    if (!v || !v->valid || v->is_null) return 0;
    if (v->type == TF_TYPE_STRING) return v->v.str ? strlen(v->v.str) + 1 : 0;
    switch (v->type) {
        case TF_TYPE_BOOL:      return sizeof(uint8_t);
        case TF_TYPE_INT64:     return sizeof(int64_t);
        case TF_TYPE_FLOAT64:   return sizeof(double);
        case TF_TYPE_DATE:      return sizeof(int32_t);
        case TF_TYPE_TIMESTAMP: return sizeof(int64_t);
        default:                return 0;
    }
}

static size_t join_sorted_retained_state_bytes(const join_state *st) {
    if (!st) return 0;
    size_t total = join_batch_storage_bytes(st->right_run);
    total += join_key_value_retained_bytes(&st->right_run_key);
    total += join_key_value_retained_bytes(&st->prev_lookup_key);
    total += join_key_value_retained_bytes(&st->prev_left_key);
    total += st->n_lookup_out * sizeof(int);
    total += st->n_lookup_schema_cols * (sizeof(char *) + sizeof(tf_type));
    for (size_t i = 0; i < st->n_lookup_schema_cols; i++) {
        if (st->lookup_schema_names && st->lookup_schema_names[i]) {
            total += strlen(st->lookup_schema_names[i]) + 1;
        }
    }
    return total;
}

static int join_append_stats(tf_step *self, tf_buffer *out) {
    if (!self || !self->state || !out) return TF_ERROR;
    join_state *st = self->state;
    size_t lookup_keys = 0;
    size_t lookup_key_bytes = 0;
    size_t lookup_row_refs = 0;
    size_t lookup_batch_bytes = 0;
    size_t retained = 0;
    if (st->use_spill) {
        lookup_keys = st->spill_lookup_keys;
        lookup_key_bytes = st->spill_lookup_key_bytes;
        retained = join_spill_retained_state_bytes(st);
    } else if (st->sorted) {
        lookup_keys = st->have_right_run ? 1u : 0u;
        lookup_key_bytes = join_key_value_retained_bytes(&st->right_run_key);
        lookup_row_refs = st->right_run ? st->right_run->capacity : 0u;
        lookup_batch_bytes = join_batch_storage_bytes(st->right_run);
        retained = join_sorted_retained_state_bytes(st);
    } else {
        lookup_keys = st->map.count;
        lookup_key_bytes = st->map.key_bytes;
        lookup_row_refs = st->map.row_ref_capacity;
        lookup_batch_bytes = join_batch_storage_bytes(st->lookup);
        retained = join_unsorted_retained_state_bytes(&st->map, st->lookup, st->n_lookup_out);
    }
    char buf[384];
    snprintf(buf, sizeof(buf),
             ",\"lookup_rows\":%zu,\"lookup_keys\":%zu,"
             "\"lookup_key_bytes\":%zu,\"lookup_row_refs\":%zu,"
             "\"lookup_batch_bytes\":%zu,\"retained_state_bytes\":%zu,"
             "\"max_state_bytes\":%zu",
             st->lookup_rows, lookup_keys, lookup_key_bytes, lookup_row_refs,
             lookup_batch_bytes, retained, st->max_state_bytes);
    if (tf_buffer_write_str(out, buf) != TF_OK) return TF_ERROR;
    if (st->use_spill) {
        snprintf(buf, sizeof(buf),
                 ",\"spill_bytes\":%zu,\"spill_runs\":%zu,"
                 "\"spill_output_batches\":%zu,\"spill_output_rows\":%zu,"
                 "\"spill_kept_rows\":%zu",
                 st->spill_bytes, st->spill_runs,
                 st->spill_output_batches, st->spill_output_rows,
                 st->spill_kept_rows);
        return tf_buffer_write_str(out, buf);
    }
    return TF_OK;
}

static void join_state_free(join_state *st) {
    if (!st) return;
    free(st->file);
    free(st->left_col);
    free(st->right_col);
    if (st->lookup) tf_batch_free(st->lookup);
    sorted_join_close(st);
    sorted_join_clear_right_run(st);
    join_key_value_clear(&st->prev_lookup_key);
    join_key_value_clear(&st->prev_left_key);
    sorted_join_free_schema(st);
    free(st->lookup_out_cols);
    map_free(&st->map);
    if (st->spill_left_buf) tf_batch_free(st->spill_left_buf);
    if (st->spill_lookup_buf) tf_batch_free(st->spill_lookup_buf);
    if (st->spill_out_buf) tf_batch_free(st->spill_out_buf);
    free(st->spill_left_ordinals);
    free(st->spill_lookup_ordinals);
    free(st->spill_out_ordinals);
    join_spill_close_readers(st, 0);
    join_spill_close_readers(st, 1);
    join_spill_close_readers(st, 2);
    join_spill_remove_paths(&st->spill_left_run_paths, &st->spill_n_left_runs, &st->spill_cap_left_runs);
    join_spill_remove_paths(&st->spill_lookup_run_paths, &st->spill_n_lookup_runs, &st->spill_cap_lookup_runs);
    join_spill_remove_paths(&st->spill_out_run_paths, &st->spill_n_out_runs, &st->spill_cap_out_runs);
    for (size_t i = 0; i < st->spill_n_cols; i++) free(st->spill_schema_names ? st->spill_schema_names[i] : NULL);
    free(st->spill_schema_names);
    free(st->spill_schema_types);
    for (size_t i = 0; i < st->spill_lookup_n_cols; i++) free(st->spill_lookup_schema_names ? st->spill_lookup_schema_names[i] : NULL);
    free(st->spill_lookup_schema_names);
    free(st->spill_lookup_schema_types);
    for (size_t i = 0; i < st->spill_output_n_cols; i++) free(st->spill_output_schema_names ? st->spill_output_schema_names[i] : NULL);
    free(st->spill_output_schema_names);
    free(st->spill_output_schema_types);
    free(st->spill_last_lookup_key);
    tf_spill_cleanup(st->spill);
    free(st->spill_dir);
    free(st);
}

static void join_destroy(tf_step *self) {
    if (self) {
        join_state_free(self->state);
        free(self);
    }
}

static int parse_positive_size_arg(const cJSON *args, const char *name,
                                   size_t *out, const char *op_name) {
    return tf_json_get_size_arg(args, name, 1, TF_MAX_COUNT_ARG, out, op_name);
}

tf_step *tf_join_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *file_j = cJSON_GetObjectItemCaseSensitive(args, "file");
    cJSON *on_j = cJSON_GetObjectItemCaseSensitive(args, "on");
    if (!cJSON_IsString(file_j) || !cJSON_IsString(on_j)) return NULL;

    join_state *st = calloc(1, sizeof(join_state));
    if (!st) return NULL;
    st->output_batch_rows = JOIN_DEFAULT_OUTPUT_ROWS;

    st->file = strdup(file_j->valuestring);
    if (!st->file) { join_state_free(st); return NULL; }

    /* Parse "on" field: "col" or "left_col=right_col" */
    const char *on = on_j->valuestring;
    const char *eq = strchr(on, '=');
    if (eq) {
        st->left_col = strndup(on, (size_t)(eq - on));
        st->right_col = strdup(eq + 1);
    } else {
        st->left_col = strdup(on);
        st->right_col = strdup(on);
    }
    if (!st->left_col || !st->right_col) { join_state_free(st); return NULL; }

    cJSON *sorted_j = cJSON_GetObjectItemCaseSensitive(args, "sorted");
    if (sorted_j && !cJSON_IsBool(sorted_j)) {
        tf_set_last_error("join: sorted must be true or false");
        join_state_free(st);
        return NULL;
    }
    st->sorted = cJSON_IsTrue(sorted_j) ? 1 : 0;

    cJSON *how_j = cJSON_GetObjectItemCaseSensitive(args, "how");
    if (cJSON_IsString(how_j)) {
        if (strcmp(how_j->valuestring, "inner") == 0) st->how = 0;
        else if (strcmp(how_j->valuestring, "left") == 0) st->how = 1;
        else if (strcmp(how_j->valuestring, "semi") == 0) st->how = 2;
        else if (strcmp(how_j->valuestring, "anti") == 0) st->how = 3;
        else {
            tf_set_last_error("join: how must be inner, left, semi, or anti");
            join_state_free(st);
            return NULL;
        }
    }

    if (parse_positive_size_arg(args, "max_lookup_rows", &st->max_lookup_rows, "join") < 0 ||
        parse_positive_size_arg(args, "max_lookup_keys", &st->max_lookup_keys, "join") < 0 ||
        parse_positive_size_arg(args, "max_lookup_bytes", &st->max_lookup_bytes, "join") < 0 ||
        parse_positive_size_arg(args, "max_state_bytes", &st->max_state_bytes, "join") < 0 ||
        parse_positive_size_arg(args, "max_matches_per_row", &st->max_matches_per_row, "join") < 0 ||
        parse_positive_size_arg(args, "max_output_rows", &st->max_output_rows, "join") < 0) {
        join_state_free(st);
        return NULL;
    }

    cJSON *spill_dir_j = cJSON_GetObjectItemCaseSensitive(args, "spill_dir");
    if (cJSON_IsString(spill_dir_j) && spill_dir_j->valuestring && spill_dir_j->valuestring[0]) {
        st->use_spill = 1;
        st->spill_dir = strdup(spill_dir_j->valuestring);
        if (!st->spill_dir) { join_state_free(st); return NULL; }
        if (tf_spill_session_create(st->spill_dir, &st->spill) != TF_OK) { join_state_free(st); return NULL; }
        size_t parsed_size = 0;
        int has_spill_memory = tf_json_get_size_arg(args, "spill_memory_bytes",
                                                    1, TF_MAX_SPILL_MEMORY_BYTES,
                                                    &parsed_size, "join");
        if (has_spill_memory < 0) { join_state_free(st); return NULL; }
        if (has_spill_memory > 0) st->spill_memory_bytes = parsed_size;
        int has_spill_rows = tf_json_get_size_arg(args, "spill_run_rows",
                                                  1, TF_MAX_SPILL_RUN_ROWS,
                                                  &parsed_size, "join");
        if (has_spill_rows < 0) { join_state_free(st); return NULL; }
        if (has_spill_rows > 0) st->configured_run_rows = parsed_size;
        int has_output_rows = tf_json_get_size_arg(args, "spill_output_rows",
                                                   1, TF_MAX_SPILL_OUTPUT_ROWS,
                                                   &parsed_size, "join");
        if (has_output_rows < 0) { join_state_free(st); return NULL; }
        if (has_output_rows > 0) st->output_batch_rows = parsed_size;
    }

    if (st->use_spill && st->sorted) {
        tf_set_last_error("join: spill_dir and sorted=true are mutually exclusive");
        join_state_free(st);
        return NULL;
    }
    if (st->use_spill && st->how < 2 && st->max_matches_per_row == 0) {
        tf_set_last_error("join: spill_dir for inner/left joins needs max_matches_per_row");
        join_state_free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { join_state_free(st); return NULL; }
    step->process = join_process;
    step->flush = join_flush;
    step->flush_next = st->use_spill ? join_flush_next : NULL;
    step->append_stats = join_append_stats;
    step->destroy = join_destroy;
    step->state = st;
    return step;
}

static tf_step *tf_join_create_with_how(const cJSON *args, const char *how) {
    if (!args) return NULL;
    cJSON *copy = cJSON_Duplicate(args, 1);
    if (!copy) return NULL;
    cJSON_DeleteItemFromObjectCaseSensitive(copy, "how");
    cJSON_AddStringToObject(copy, "how", how);
    tf_step *step = tf_join_create(copy);
    cJSON_Delete(copy);
    return step;
}

tf_step *tf_semi_join_create(const cJSON *args) {
    return tf_join_create_with_how(args, "semi");
}

tf_step *tf_anti_join_create(const cJSON *args) {
    return tf_join_create_with_how(args, "anti");
}
