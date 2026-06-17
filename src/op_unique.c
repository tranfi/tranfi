/*
 * op_unique.c -- Deduplicate rows by key columns.
 *
 * Default mode is a streaming hash set with optional caps. sorted=true is an
 * adjacent-key streaming mode. spill_dir enables exact external dedup without
 * retaining all keys: rows are sorted by key+ordinal to choose the first row
 * per key, then selected rows are sorted by original ordinal before emission.
 */

#include "internal.h"
#include "spill.h"
#include "cJSON.h"
#include "date_utils.h"
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define UNIQUE_DEFAULT_RUN_ROWS 8192
#define UNIQUE_DEFAULT_OUTPUT_ROWS 1024
#define UNIQUE_MIN_RUN_ROWS 16

typedef tf_owned_cell_value unique_cell;

typedef struct {
    uint64_t ordinal;
    uint8_t *nulls;
    unique_cell *cells;
} unique_spill_row;

typedef struct {
    FILE *file;
    unique_spill_row row;
    int has_row;
    int done;
} unique_run_reader;

/* ---- Simple hash set for capped in-memory mode ---- */

typedef struct {
    char  **keys;
    size_t  count;
    size_t  cap;
    size_t  key_bytes;
} hash_set;

static int hs_init(hash_set *hs, size_t cap) {
    hs->cap = cap;
    hs->count = 0;
    hs->key_bytes = 0;
    hs->keys = tf_callocarray_checked(cap, sizeof(char *));
    return hs->keys ? 0 : -1;
}

static uint32_t hs_hash(const char *key) {
    uint32_t h = 5381;
    for (const char *p = key; *p; p++)
        h = ((h << 5) + h) ^ (uint32_t)(unsigned char)*p;
    return h;
}

static int hs_grow(hash_set *hs) {
    size_t new_cap = 0;
    if (tf_size_mul(hs->cap, 2, &new_cap) != TF_OK) return -1;
    char **new_keys = tf_callocarray_checked(new_cap, sizeof(char *));
    if (!new_keys) return -1;

    for (size_t i = 0; i < hs->cap; i++) {
        if (hs->keys[i]) {
            uint32_t idx = hs_hash(hs->keys[i]) % new_cap;
            while (new_keys[idx]) idx = (idx + 1) % new_cap;
            new_keys[idx] = hs->keys[i];
        }
    }
    free(hs->keys);
    hs->keys = new_keys;
    hs->cap = new_cap;
    return 0;
}

static int hs_contains(const hash_set *hs, const char *key) {
    if (!hs->keys || hs->cap == 0) return 0;
    uint32_t idx = hs_hash(key) % hs->cap;
    while (hs->keys[idx]) {
        if (strcmp(hs->keys[idx], key) == 0) return 1;
        idx = (idx + 1) % hs->cap;
    }
    return 0;
}

static int hs_insert(hash_set *hs, const char *key) {
    size_t load_count = 0;
    size_t load_limit = 0;
    if (tf_size_mul(hs->count, 4, &load_count) != TF_OK ||
        tf_size_mul(hs->cap, 3, &load_limit) != TF_OK) {
        return -1;
    }
    if (load_count >= load_limit) {
        if (hs_grow(hs) != 0) return -1;
    }
    uint32_t idx = hs_hash(key) % hs->cap;
    while (hs->keys[idx]) {
        if (strcmp(hs->keys[idx], key) == 0) return 0;
        idx = (idx + 1) % hs->cap;
    }
    size_t key_bytes_delta = 0;
    size_t new_key_bytes = 0;
    if (tf_size_add(strlen(key), 1, &key_bytes_delta) != TF_OK ||
        tf_size_add(hs->key_bytes, key_bytes_delta, &new_key_bytes) != TF_OK) {
        return -1;
    }
    hs->keys[idx] = strdup(key);
    if (!hs->keys[idx]) return -1;
    hs->key_bytes = new_key_bytes;
    hs->count++;
    return 1;
}

static void hs_free(hash_set *hs) {
    if (!hs) return;
    if (hs->keys) {
        for (size_t i = 0; i < hs->cap; i++) free(hs->keys[i]);
    }
    free(hs->keys);
    memset(hs, 0, sizeof(*hs));
}

/* ---- Unique transform ---- */

typedef struct {
    char    **key_cols;
    size_t    n_key_cols;
    size_t    max_keys;
    size_t    max_state_bytes;
    int       sorted;
    int       use_spill;

    char     *prev_key;
    int       have_prev_key;
    hash_set  seen;

    char     *spill_dir;
    tf_spill_session *spill;
    size_t    spill_memory_bytes;
    size_t    configured_run_rows;
    size_t    run_rows;
    size_t    output_batch_rows;

    int       has_schema;
    char    **schema_names;
    tf_type  *schema_types;
    size_t    n_schema_cols;
    int      *key_indices;
    size_t    n_keys;

    tf_batch *buf;
    uint64_t *buf_ordinals;
    size_t    buf_ordinal_cap;
    uint64_t  next_ordinal;

    char    **run_paths;
    size_t    n_runs;
    size_t    cap_runs;
    char    **out_run_paths;
    size_t    n_out_runs;
    size_t    cap_out_runs;
    size_t    run_seq;
    size_t    out_run_seq;

    unique_run_reader *readers;
    size_t    n_readers;
    unique_run_reader *out_readers;
    size_t    n_out_readers;

    tf_batch *out_buf;
    uint64_t *out_ordinals;
    size_t    out_ordinal_cap;

    int       key_merge_done;
    int       output_merge_started;
    int       output_merge_done;
    char     *last_spill_key;

    size_t    spilled_bytes;
    size_t    spill_runs_created;
    size_t    spill_output_batches;
    size_t    spill_output_rows;
    size_t    spill_distinct_rows;
    size_t    spill_key_bytes;
} unique_state;

static int unique_write_error(tf_side_channels *side, const char *msg) {
    return tf_side_write_error(side, msg);
}

static int unique_limit_error(unique_state *st, tf_side_channels *side) {
    char msg[160];
    snprintf(msg, sizeof(msg),
             "unique: max_keys=%zu exceeded while tracking distinct keys",
             st->max_keys);
    return unique_write_error(side, msg);
}

static int append_bytes(char **buf, size_t *len, size_t *cap, const void *src, size_t n) {
    size_t need = 0;
    if (tf_size_add(*len, n, &need) != TF_OK ||
        tf_size_add(need, 1, &need) != TF_OK) {
        return TF_ERROR;
    }
    if (need > *cap) {
        size_t new_cap = 0;
        if (tf_size_grow_pow2(*cap, need, 128, &new_cap) != TF_OK) return TF_ERROR;
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

static int append_str(char **buf, size_t *len, size_t *cap, const char *s) {
    return append_bytes(buf, len, cap, s, strlen(s));
}

static int append_fmt(char **buf, size_t *len, size_t *cap, const char *fmt, long long v) {
    char tmp[64];
    int n = snprintf(tmp, sizeof(tmp), fmt, v);
    if (n < 0) return TF_ERROR;
    return append_bytes(buf, len, cap, tmp, (size_t)n);
}

static char *build_row_key(const tf_batch *b, size_t row, int *col_indices, size_t n_keys) {
    char *buf = NULL;
    size_t len = 0, cap = 0;
    for (size_t k = 0; k < n_keys; k++) {
        int c = col_indices[k];
        if (c < 0) continue;
        if (k > 0 && append_str(&buf, &len, &cap, "|") != TF_OK) goto fail;
        if (tf_batch_is_null(b, row, c)) {
            char tmp[32];
            snprintf(tmp, sizeof(tmp), "N:%d", (int)b->col_types[c]);
            if (append_str(&buf, &len, &cap, tmp) != TF_OK) goto fail;
            continue;
        }
        switch (b->col_types[c]) {
            case TF_TYPE_BOOL:
                if (append_str(&buf, &len, &cap, tf_batch_get_bool(b, row, c) ? "B:1" : "B:0") != TF_OK) goto fail;
                break;
            case TF_TYPE_INT64:
                if (append_fmt(&buf, &len, &cap, "I:%lld", (long long)tf_batch_get_int64(b, row, c)) != TF_OK) goto fail;
                break;
            case TF_TYPE_FLOAT64: {
                char tmp[80];
                int n = snprintf(tmp, sizeof(tmp), "F:%.17g", tf_batch_get_float64(b, row, c));
                if (n < 0 || append_bytes(&buf, &len, &cap, tmp, (size_t)n) != TF_OK) goto fail;
                break;
            }
            case TF_TYPE_STRING: {
                const char *s = tf_batch_get_string(b, row, c);
                char tmp[64];
                int n = snprintf(tmp, sizeof(tmp), "S:%zu:", s ? strlen(s) : 0u);
                if (n < 0 || append_bytes(&buf, &len, &cap, tmp, (size_t)n) != TF_OK) goto fail;
                if (s && append_str(&buf, &len, &cap, s) != TF_OK) goto fail;
                break;
            }
            case TF_TYPE_DATE:
                if (append_fmt(&buf, &len, &cap, "D:%lld", (long long)tf_batch_get_date(b, row, c)) != TF_OK) goto fail;
                break;
            case TF_TYPE_TIMESTAMP:
                if (append_fmt(&buf, &len, &cap, "T:%lld", (long long)tf_batch_get_timestamp(b, row, c)) != TF_OK) goto fail;
                break;
            default:
                if (append_str(&buf, &len, &cap, "U") != TF_OK) goto fail;
                break;
        }
    }
    if (!buf) buf = strdup("");
    return buf;
fail:
    free(buf);
    return NULL;
}

static tf_batch *create_buffer_from_schema(const unique_state *st, size_t capacity) {
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

static size_t unique_estimated_row_bytes(const unique_state *st) {
    size_t bytes = 40;
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
    return bytes < 64 ? 64 : bytes;
}

static int ensure_ordinals(uint64_t **ord, size_t *cap, size_t need) {
    if (*cap >= need) return TF_OK;
    size_t new_cap = 0;
    if (tf_size_grow_pow2(*cap, need, 16, &new_cap) != TF_OK) return TF_ERROR;
    uint64_t *tmp = tf_reallocarray_checked(*ord, new_cap, sizeof(uint64_t));
    if (!tmp) return TF_ERROR;
    *ord = tmp;
    *cap = new_cap;
    return TF_OK;
}

static int init_spill_schema(unique_state *st, const tf_batch *in) {
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

    if (st->n_key_cols > 0) {
        st->n_keys = st->n_key_cols;
        st->key_indices = tf_callocarray_checked(st->n_keys ? st->n_keys : 1, sizeof(int));
        if (!st->key_indices) return TF_ERROR;
        for (size_t k = 0; k < st->n_keys; k++) {
            int idx = tf_batch_col_index(in, st->key_cols[k]);
            if (idx < 0) {
                char msg[256];
                snprintf(msg, sizeof(msg), "unique: column '%s' not found",
                         st->key_cols[k] ? st->key_cols[k] : "");
                tf_set_last_error(msg);
                return TF_ERROR;
            }
            st->key_indices[k] = idx;
        }
    } else {
        st->n_keys = in->n_cols;
        st->key_indices = tf_callocarray_checked(st->n_keys ? st->n_keys : 1, sizeof(int));
        if (!st->key_indices) return TF_ERROR;
        for (size_t k = 0; k < st->n_keys; k++) st->key_indices[k] = (int)k;
    }

    if (st->configured_run_rows > 0) {
        st->run_rows = st->configured_run_rows;
    } else if (st->spill_memory_bytes > 0) {
        size_t row_bytes = unique_estimated_row_bytes(st);
        st->run_rows = st->spill_memory_bytes / (row_bytes * 4);
        if (st->run_rows < UNIQUE_MIN_RUN_ROWS) st->run_rows = UNIQUE_MIN_RUN_ROWS;
    } else {
        st->run_rows = UNIQUE_DEFAULT_RUN_ROWS;
    }
    st->buf = create_buffer_from_schema(st, st->run_rows);
    st->out_buf = create_buffer_from_schema(st, st->run_rows);
    if (!st->buf || !st->out_buf) return TF_ERROR;
    st->has_schema = 1;
    return TF_OK;
}

static int compare_batch_key_rows(const unique_state *st, const tf_batch *batch, size_t ra, size_t rb) {
    for (size_t k = 0; k < st->n_keys; k++) {
        int ci = st->key_indices[k];
        if (ci < 0) continue;
        int null_a = tf_batch_is_null(batch, ra, (size_t)ci);
        int null_b = tf_batch_is_null(batch, rb, (size_t)ci);
        if (null_a && null_b) continue;
        if (null_a) return 1;
        if (null_b) return -1;
        int cmp = 0;
        switch (batch->col_types[ci]) {
            case TF_TYPE_BOOL: cmp = (int)tf_batch_get_bool(batch, ra, (size_t)ci) - (int)tf_batch_get_bool(batch, rb, (size_t)ci); break;
            case TF_TYPE_INT64: {
                int64_t a = tf_batch_get_int64(batch, ra, (size_t)ci), b = tf_batch_get_int64(batch, rb, (size_t)ci);
                cmp = (a > b) - (a < b);
                break;
            }
            case TF_TYPE_FLOAT64: {
                double a = tf_batch_get_float64(batch, ra, (size_t)ci), b = tf_batch_get_float64(batch, rb, (size_t)ci);
                cmp = (a > b) - (a < b);
                break;
            }
            case TF_TYPE_STRING: cmp = strcmp(tf_batch_get_string(batch, ra, (size_t)ci), tf_batch_get_string(batch, rb, (size_t)ci)); break;
            case TF_TYPE_DATE: {
                int32_t a = tf_batch_get_date(batch, ra, (size_t)ci), b = tf_batch_get_date(batch, rb, (size_t)ci);
                cmp = (a > b) - (a < b);
                break;
            }
            case TF_TYPE_TIMESTAMP: {
                int64_t a = tf_batch_get_timestamp(batch, ra, (size_t)ci), b = tf_batch_get_timestamp(batch, rb, (size_t)ci);
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

typedef struct {
    const unique_state *st;
    int by_ordinal;
} unique_sort_ctx;

static int unique_compare_indices(const void *ctx, size_t a, size_t b) {
    const unique_sort_ctx *sort = (const unique_sort_ctx *)ctx;
    if (!sort->by_ordinal) return compare_batch_key_rows(sort->st, sort->st->buf, a, b);
    uint64_t oa = sort->st->out_ordinals[a];
    uint64_t ob = sort->st->out_ordinals[b];
    return (oa > ob) - (oa < ob);
}

static size_t *sorted_indices(size_t n, int by_ordinal, const unique_state *st) {
    size_t *idx = tf_mallocarray_checked(n ? n : 1, sizeof(size_t));
    if (!idx) return NULL;
    for (size_t i = 0; i < n; i++) idx[i] = i;
    unique_sort_ctx ctx = { .st = st, .by_ordinal = by_ordinal };
    tf_sort_indices(idx, n, unique_compare_indices, &ctx);
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
            const char *s = tf_batch_get_string(b, r, c);
            uint64_t len = s ? (uint64_t)strlen(s) : 0;
            if (write_exact(f, &len, sizeof(len)) != TF_OK) return TF_ERROR;
            return len ? write_exact(f, s, (size_t)len) : TF_OK;
        }
        case TF_TYPE_DATE: { int32_t v = tf_batch_get_date(b, r, c); return write_exact(f, &v, sizeof(v)); }
        case TF_TYPE_TIMESTAMP: { int64_t v = tf_batch_get_timestamp(b, r, c); return write_exact(f, &v, sizeof(v)); }
        default: return TF_OK;
    }
}

static int append_path(char ***paths, size_t *n, size_t *cap, char *path) {
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

static int write_batch_run(unique_state *st, tf_batch *batch, const uint64_t *ordinals,
                           size_t *indices, size_t n, int output_run) {
    char *path = NULL;
    FILE *f = tf_spill_open_run_file(st->spill, output_run ? "unique-out" : "unique-key", &path);
    if (!f) return TF_ERROR;
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
        tf_set_last_error("unique spill: failed closing run file");
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
    tf_set_last_error("unique spill: failed writing run file");
    fclose(f);
    remove(path);
    free(path);
    return TF_ERROR;
}

static int write_key_run(unique_state *st) {
    if (!st->buf || st->buf->n_rows == 0) return TF_OK;
    size_t *idx = sorted_indices(st->buf->n_rows, 0, st);
    if (!idx) return TF_ERROR;
    int rc = write_batch_run(st, st->buf, st->buf_ordinals, idx, st->buf->n_rows, 0);
    free(idx);
    if (rc != TF_OK) return TF_ERROR;
    tf_batch_free(st->buf);
    st->buf = create_buffer_from_schema(st, st->run_rows);
    st->buf_ordinal_cap = 0;
    free(st->buf_ordinals);
    st->buf_ordinals = NULL;
    return st->buf ? TF_OK : TF_ERROR;
}

static int write_output_run(unique_state *st) {
    if (!st->out_buf || st->out_buf->n_rows == 0) return TF_OK;
    size_t *idx = sorted_indices(st->out_buf->n_rows, 1, st);
    if (!idx) return TF_ERROR;
    int rc = write_batch_run(st, st->out_buf, st->out_ordinals, idx, st->out_buf->n_rows, 1);
    free(idx);
    if (rc != TF_OK) return TF_ERROR;
    tf_batch_free(st->out_buf);
    st->out_buf = create_buffer_from_schema(st, st->run_rows);
    st->out_ordinal_cap = 0;
    free(st->out_ordinals);
    st->out_ordinals = NULL;
    return st->out_buf ? TF_OK : TF_ERROR;
}

static void spill_row_clear(unique_spill_row *row, const tf_type *types, size_t n_cols) {
    if (!row || !row->cells || !row->nulls) return;
    for (size_t c = 0; c < n_cols; c++) {
        if (!row->nulls[c] && types[c] == TF_TYPE_STRING) free(row->cells[c].str);
        row->cells[c].str = NULL;
        row->nulls[c] = 1;
    }
}

static int spill_row_init(unique_spill_row *row, size_t n_cols) {
    row->ordinal = 0;
    row->nulls = tf_callocarray_checked(n_cols ? n_cols : 1, sizeof(uint8_t));
    row->cells = tf_callocarray_checked(n_cols ? n_cols : 1, sizeof(unique_cell));
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

static void spill_row_free(unique_spill_row *row, const tf_type *types, size_t n_cols) {
    if (!row) return;
    spill_row_clear(row, types, n_cols);
    free(row->nulls);
    free(row->cells);
    row->nulls = NULL;
    row->cells = NULL;
}

static int read_cell_value(FILE *f, unique_spill_row *row, const tf_type *types, size_t c) {
    switch (types[c]) {
        case TF_TYPE_BOOL: { uint8_t v = 0; if (read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].b = v; return TF_OK; }
        case TF_TYPE_INT64: { int64_t v = 0; if (read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].i64 = v; return TF_OK; }
        case TF_TYPE_FLOAT64: { double v = 0; if (read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].f64 = v; return TF_OK; }
        case TF_TYPE_STRING: {
            uint64_t len = 0;
            if (read_exact(f, &len, sizeof(len)) != TF_OK) return TF_ERROR;
            if (len > (uint64_t)SIZE_MAX - 1) return TF_ERROR;
            char *s = malloc((size_t)len + 1);
            if (!s) return TF_ERROR;
            if (len && read_exact(f, s, (size_t)len) != TF_OK) { free(s); return TF_ERROR; }
            s[len] = '\0';
            row->cells[c].str = s;
            return TF_OK;
        }
        case TF_TYPE_DATE: { int32_t v = 0; if (read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].date = v; return TF_OK; }
        case TF_TYPE_TIMESTAMP: { int64_t v = 0; if (read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR; row->cells[c].i64 = v; return TF_OK; }
        default: return TF_OK;
    }
}

static int reader_advance(unique_state *st, unique_run_reader *reader) {
    if (!reader || !reader->file || reader->done) return 0;
    spill_row_clear(&reader->row, st->schema_types, st->n_schema_cols);
    if (fread(&reader->row.ordinal, sizeof(reader->row.ordinal), 1, reader->file) != 1) {
        if (feof(reader->file)) {
            reader->done = 1;
            reader->has_row = 0;
            return 0;
        }
        tf_set_last_error("unique spill: failed reading run file");
        return -1;
    }
    for (size_t c = 0; c < st->n_schema_cols; c++) {
        uint8_t is_null = 1;
        if (read_exact(reader->file, &is_null, sizeof(is_null)) != TF_OK) {
            tf_set_last_error("unique spill: corrupt run file");
            return -1;
        }
        reader->row.nulls[c] = is_null ? 1 : 0;
        if (!reader->row.nulls[c] && read_cell_value(reader->file, &reader->row, st->schema_types, c) != TF_OK) {
            tf_set_last_error("unique spill: corrupt run file");
            return -1;
        }
    }
    reader->has_row = 1;
    return 1;
}

static int compare_spill_key_rows(const unique_state *st, const unique_spill_row *a, const unique_spill_row *b) {
    for (size_t k = 0; k < st->n_keys; k++) {
        int ci = st->key_indices[k];
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

static char *build_spill_row_key(const unique_state *st, const unique_spill_row *row) {
    char *buf = NULL;
    size_t len = 0, cap = 0;
    for (size_t k = 0; k < st->n_keys; k++) {
        int ci = st->key_indices[k];
        if (ci < 0) continue;
        size_t c = (size_t)ci;
        if (k > 0 && append_str(&buf, &len, &cap, "|") != TF_OK) goto fail;
        if (row->nulls[c]) {
            char tmp[32];
            snprintf(tmp, sizeof(tmp), "N:%d", (int)st->schema_types[c]);
            if (append_str(&buf, &len, &cap, tmp) != TF_OK) goto fail;
            continue;
        }
        switch (st->schema_types[c]) {
            case TF_TYPE_BOOL:
                if (append_str(&buf, &len, &cap, row->cells[c].b ? "B:1" : "B:0") != TF_OK) goto fail;
                break;
            case TF_TYPE_INT64:
                if (append_fmt(&buf, &len, &cap, "I:%lld", (long long)row->cells[c].i64) != TF_OK) goto fail;
                break;
            case TF_TYPE_FLOAT64: {
                char tmp[80];
                int n = snprintf(tmp, sizeof(tmp), "F:%.17g", row->cells[c].f64);
                if (n < 0 || append_bytes(&buf, &len, &cap, tmp, (size_t)n) != TF_OK) goto fail;
                break;
            }
            case TF_TYPE_STRING: {
                const char *s = row->cells[c].str;
                char tmp[64];
                int n = snprintf(tmp, sizeof(tmp), "S:%zu:", s ? strlen(s) : 0u);
                if (n < 0 || append_bytes(&buf, &len, &cap, tmp, (size_t)n) != TF_OK) goto fail;
                if (s && append_str(&buf, &len, &cap, s) != TF_OK) goto fail;
                break;
            }
            case TF_TYPE_DATE:
                if (append_fmt(&buf, &len, &cap, "D:%lld", (long long)row->cells[c].date) != TF_OK) goto fail;
                break;
            case TF_TYPE_TIMESTAMP:
                if (append_fmt(&buf, &len, &cap, "T:%lld", (long long)row->cells[c].i64) != TF_OK) goto fail;
                break;
            default:
                if (append_str(&buf, &len, &cap, "U") != TF_OK) goto fail;
                break;
        }
    }
    if (!buf) buf = strdup("");
    return buf;
fail:
    free(buf);
    return NULL;
}

static int spill_row_to_batch(const unique_state *st, tf_batch *out,
                              size_t dst_row, const unique_spill_row *row) {
    if (tf_batch_ensure_capacity(out, dst_row + 1) != TF_OK) return TF_ERROR;
    for (size_t c = 0; c < st->n_schema_cols; c++) {
        if (tf_batch_set_owned_cell_value(out, dst_row, c, st->schema_types[c],
                                          row->nulls[c], &row->cells[c]) != TF_OK) {
            return TF_ERROR;
        }
    }
    return TF_OK;
}

static void close_readers(unique_state *st, int output_readers) {
    unique_run_reader **readers = output_readers ? &st->out_readers : &st->readers;
    size_t *n_readers = output_readers ? &st->n_out_readers : &st->n_readers;
    if (!*readers) return;
    for (size_t i = 0; i < *n_readers; i++) {
        if ((*readers)[i].file) fclose((*readers)[i].file);
        spill_row_free(&(*readers)[i].row, st->schema_types, st->n_schema_cols);
    }
    free(*readers);
    *readers = NULL;
    *n_readers = 0;
}

static void remove_paths(char ***paths, size_t *n, size_t *cap) {
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

static int open_readers(unique_state *st, int output_readers) {
    char **paths = output_readers ? st->out_run_paths : st->run_paths;
    size_t n_paths = output_readers ? st->n_out_runs : st->n_runs;
    unique_run_reader **readers = output_readers ? &st->out_readers : &st->readers;
    size_t *n_readers = output_readers ? &st->n_out_readers : &st->n_readers;
    if (n_paths == 0) return TF_OK;
    *readers = tf_callocarray_checked(n_paths, sizeof(unique_run_reader));
    if (!*readers) return TF_ERROR;
    *n_readers = n_paths;
    for (size_t i = 0; i < n_paths; i++) {
        (*readers)[i].file = fopen(paths[i], "rb");
        if (!(*readers)[i].file) { tf_set_last_error("unique spill: cannot reopen run file"); return TF_ERROR; }
        if (spill_row_init(&(*readers)[i].row, st->n_schema_cols) != TF_OK) return TF_ERROR;
        int rc = reader_advance(st, &(*readers)[i]);
        if (rc < 0) return TF_ERROR;
    }
    return TF_OK;
}

static int best_key_reader(const unique_state *st) {
    int best = -1;
    for (size_t i = 0; i < st->n_readers; i++) {
        const unique_run_reader *r = &st->readers[i];
        if (!r->has_row || r->done) continue;
        if (best < 0) { best = (int)i; continue; }
        int cmp = compare_spill_key_rows(st, &r->row, &st->readers[best].row);
        if (cmp < 0 || (cmp == 0 && i < (size_t)best)) best = (int)i;
    }
    return best;
}

static int best_ordinal_reader(const unique_state *st) {
    int best = -1;
    for (size_t i = 0; i < st->n_out_readers; i++) {
        const unique_run_reader *r = &st->out_readers[i];
        if (!r->has_row || r->done) continue;
        if (best < 0) { best = (int)i; continue; }
        uint64_t a = r->row.ordinal;
        uint64_t b = st->out_readers[best].row.ordinal;
        if (a < b || (a == b && i < (size_t)best)) best = (int)i;
    }
    return best;
}

static int append_selected_row(unique_state *st, const unique_spill_row *row) {
    size_t dst = st->out_buf->n_rows;
    if (ensure_ordinals(&st->out_ordinals, &st->out_ordinal_cap, dst + 1) != TF_OK) return TF_ERROR;
    if (spill_row_to_batch(st, st->out_buf, dst, row) != TF_OK) return TF_ERROR;
    st->out_ordinals[dst] = row->ordinal;
    if (tf_batch_expose_row(st->out_buf, dst) != TF_OK) return TF_ERROR;
    st->spill_distinct_rows++;
    if (st->out_buf->n_rows >= st->run_rows) return write_output_run(st);
    return TF_OK;
}

static int produce_output_runs(unique_state *st) {
    if (st->key_merge_done) return TF_OK;
    if (st->buf && st->buf->n_rows > 0 && write_key_run(st) != TF_OK) return TF_ERROR;
    if (st->buf) { tf_batch_free(st->buf); st->buf = NULL; }
    free(st->buf_ordinals); st->buf_ordinals = NULL; st->buf_ordinal_cap = 0;

    if (open_readers(st, 0) != TF_OK) return TF_ERROR;
    for (;;) {
        int best = best_key_reader(st);
        if (best < 0) break;
        unique_run_reader *reader = &st->readers[best];
        char *key = build_spill_row_key(st, &reader->row);
        if (!key) return TF_ERROR;
        int keep = !st->last_spill_key || strcmp(st->last_spill_key, key) != 0;
        if (keep) {
            size_t key_bytes_delta = 0;
            size_t new_spill_key_bytes = 0;
            if (tf_size_add(strlen(key), 1, &key_bytes_delta) != TF_OK ||
                tf_size_add(st->spill_key_bytes, key_bytes_delta,
                            &new_spill_key_bytes) != TF_OK) {
                free(key);
                return TF_ERROR;
            }
            free(st->last_spill_key);
            st->last_spill_key = key;
            st->spill_key_bytes = new_spill_key_bytes;
            key = NULL;
            if (append_selected_row(st, &reader->row) != TF_OK) { free(key); return TF_ERROR; }
        }
        free(key);
        int rc = reader_advance(st, reader);
        if (rc < 0) return TF_ERROR;
    }
    if (st->out_buf && st->out_buf->n_rows > 0 && write_output_run(st) != TF_OK) return TF_ERROR;
    close_readers(st, 0);
    remove_paths(&st->run_paths, &st->n_runs, &st->cap_runs);
    st->key_merge_done = 1;
    return TF_OK;
}

static int begin_output_merge(unique_state *st) {
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
    return open_readers(st, 1);
}

static int output_next_batch(unique_state *st, tf_batch **out) {
    *out = NULL;
    if (produce_output_runs(st) != TF_OK) return TF_ERROR;
    if (begin_output_merge(st) != TF_OK) return TF_ERROR;
    if (st->output_merge_done) return TF_OK;

    tf_batch *ob = create_buffer_from_schema(st, st->output_batch_rows);
    if (!ob) return TF_ERROR;
    while (ob->n_rows < st->output_batch_rows) {
        int best = best_ordinal_reader(st);
        if (best < 0) break;
        unique_run_reader *reader = &st->out_readers[best];
        size_t out_row = ob->n_rows;
        if (spill_row_to_batch(st, ob, out_row, &reader->row) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
        if (tf_batch_expose_row(ob, out_row) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
        int rc = reader_advance(st, reader);
        if (rc < 0) { tf_batch_free(ob); return TF_ERROR; }
    }
    if (ob->n_rows == 0) {
        tf_batch_free(ob);
        close_readers(st, 1);
        remove_paths(&st->out_run_paths, &st->n_out_runs, &st->cap_out_runs);
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

static size_t unique_prev_key_bytes(const unique_state *st) {
    return (st && st->have_prev_key && st->prev_key) ? strlen(st->prev_key) + 1 : 0;
}

static size_t unique_retained_state_bytes(const unique_state *st) {
    if (!st) return 0;
    if (st->use_spill) {
        size_t bytes = 0;
        if (st->buf) bytes += st->buf->capacity * (sizeof(uint64_t) + 16);
        if (st->out_buf) bytes += st->out_buf->capacity * (sizeof(uint64_t) + 16);
        bytes += st->n_readers * sizeof(unique_run_reader) + st->n_out_readers * sizeof(unique_run_reader);
        bytes += st->last_spill_key ? strlen(st->last_spill_key) + 1 : 0;
        return bytes;
    }
    if (st->sorted) return unique_prev_key_bytes(st);
    return st->seen.key_bytes + st->seen.cap * sizeof(char *);
}

static int unique_check_state_bytes(unique_state *st, tf_side_channels *side) {
    if (!st || st->max_state_bytes == 0 || st->use_spill) return 0;
    size_t retained = unique_retained_state_bytes(st);
    if (retained <= st->max_state_bytes) return 0;
    char msg[192];
    snprintf(msg, sizeof(msg),
             "unique: max_state_bytes=%zu exceeded while tracking distinct keys (%zu bytes retained)",
             st->max_state_bytes, retained);
    if (unique_write_error(side, msg) != TF_OK) return -1;
    return -1;
}

static int unique_process_spill(unique_state *st, tf_batch *in) {
    if (init_spill_schema(st, in) != TF_OK) return TF_ERROR;
    for (size_t r = 0; r < in->n_rows; r++) {
        size_t dst = st->buf->n_rows;
        if (ensure_ordinals(&st->buf_ordinals, &st->buf_ordinal_cap, dst + 1) != TF_OK) return TF_ERROR;
        if (tf_batch_copy_row(st->buf, dst, in, r) != TF_OK) return TF_ERROR;
        st->buf_ordinals[dst] = st->next_ordinal++;
        if (tf_batch_expose_row(st->buf, dst) != TF_OK) return TF_ERROR;
        if (st->buf->n_rows >= st->run_rows && write_key_run(st) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static int unique_process(tf_step *self, tf_batch *in, tf_batch **out, tf_side_channels *side) {
    unique_state *st = self->state;
    *out = NULL;

    if (st->use_spill) return unique_process_spill(st, in);

    size_t n_keys;
    int *col_indices;
    if (st->n_key_cols > 0) {
        n_keys = st->n_key_cols;
        col_indices = tf_mallocarray_checked(n_keys, sizeof(int));
        if (!col_indices) return TF_ERROR;
        for (size_t k = 0; k < n_keys; k++) {
            int idx = tf_batch_col_index(in, st->key_cols[k]);
            if (idx < 0) {
                char msg[256];
                snprintf(msg, sizeof(msg), "unique: column '%s' not found",
                         st->key_cols[k] ? st->key_cols[k] : "");
                tf_set_last_error(msg);
                free(col_indices);
                return TF_ERROR;
            }
            col_indices[k] = idx;
        }
    } else {
        n_keys = in->n_cols;
        col_indices = tf_mallocarray_checked(n_keys, sizeof(int));
        if (!col_indices) return TF_ERROR;
        for (size_t k = 0; k < n_keys; k++) col_indices[k] = (int)k;
    }

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) { free(col_indices); return TF_ERROR; }
    if (tf_batch_clone_schema(ob, in) != TF_OK) { free(col_indices); tf_batch_free(ob); return TF_ERROR; }

    size_t out_row = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        char *key = build_row_key(in, r, col_indices, n_keys);
        if (!key) { free(col_indices); tf_batch_free(ob); return TF_ERROR; }
        int emit_row = 0;
        if (st->sorted) {
            if (!st->have_prev_key || strcmp(st->prev_key, key) != 0) {
                free(st->prev_key);
                st->prev_key = key;
                key = NULL;
                st->have_prev_key = 1;
                if (unique_check_state_bytes(st, side) != 0) { free(col_indices); tf_batch_free(ob); return TF_ERROR; }
                emit_row = 1;
            }
        } else {
            if (!hs_contains(&st->seen, key) && st->max_keys > 0 && st->seen.count >= st->max_keys) {
                if (unique_limit_error(st, side) != TF_OK) {
                    free(key);
                    free(col_indices);
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                free(key);
                free(col_indices);
                tf_batch_free(ob);
                return TF_ERROR;
            }
            int inserted = hs_insert(&st->seen, key);
            if (inserted < 0) { free(key); free(col_indices); tf_batch_free(ob); return TF_ERROR; }
            if (inserted == 1 && unique_check_state_bytes(st, side) != 0) {
                free(key);
                free(col_indices);
                tf_batch_free(ob);
                return TF_ERROR;
            }
            emit_row = inserted == 1;
        }
        free(key);
        if (!emit_row) continue;
        if (tf_batch_copy_row(ob, out_row, in, r) != TF_OK) { free(col_indices); tf_batch_free(ob); return TF_ERROR; }
        if (tf_batch_expose_row(ob, out_row) != TF_OK) { free(col_indices); tf_batch_free(ob); return TF_ERROR; }
        out_row++;
    }
    free(col_indices);
    if (out_row > 0) *out = ob;
    else tf_batch_free(ob);
    return TF_OK;
}

static int unique_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    unique_state *st = self->state;
    *out = NULL;
    if (!st->use_spill) return TF_OK;
    return output_next_batch(st, out);
}

static int unique_flush_next(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    unique_state *st = self->state;
    *out = NULL;
    if (!st->use_spill) return TF_OK;
    return output_next_batch(st, out);
}

static int unique_append_stats(tf_step *self, tf_buffer *out) {
    if (!self || !self->state || !out) return TF_ERROR;
    unique_state *st = self->state;
    size_t tracked_keys = st->use_spill ? st->spill_distinct_rows : (st->sorted ? (st->have_prev_key ? 1u : 0u) : st->seen.count);
    size_t tracked_key_bytes = st->use_spill ? st->spill_key_bytes : (st->sorted ? unique_prev_key_bytes(st) : st->seen.key_bytes);
    size_t retained = unique_retained_state_bytes(st);
    char buf[360];
    snprintf(buf, sizeof(buf),
             ",\"tracked_keys\":%zu,\"tracked_key_bytes\":%zu,"
             "\"retained_state_bytes\":%zu,\"max_state_bytes\":%zu",
             tracked_keys, tracked_key_bytes, retained, st->max_state_bytes);
    if (tf_buffer_write_str(out, buf) != TF_OK) return TF_ERROR;
    if (st->use_spill) {
        snprintf(buf, sizeof(buf),
                 ",\"spill_bytes\":%zu,\"spill_runs\":%zu,"
                 "\"spill_output_batches\":%zu,\"spill_output_rows\":%zu,"
                 "\"spill_distinct_rows\":%zu",
                 st->spilled_bytes, st->spill_runs_created,
                 st->spill_output_batches, st->spill_output_rows,
                 st->spill_distinct_rows);
        return tf_buffer_write_str(out, buf);
    }
    return TF_OK;
}

static void unique_state_free(unique_state *st) {
    if (!st) return;
    if (st->key_cols) {
        for (size_t i = 0; i < st->n_key_cols; i++) free(st->key_cols[i]);
    }
    free(st->key_cols);
    free(st->prev_key);
    free(st->last_spill_key);
    hs_free(&st->seen);
    if (st->buf) tf_batch_free(st->buf);
    if (st->out_buf) tf_batch_free(st->out_buf);
    free(st->buf_ordinals);
    free(st->out_ordinals);
    close_readers(st, 0);
    close_readers(st, 1);
    remove_paths(&st->run_paths, &st->n_runs, &st->cap_runs);
    remove_paths(&st->out_run_paths, &st->n_out_runs, &st->cap_out_runs);
    for (size_t i = 0; i < st->n_schema_cols; i++) free(st->schema_names ? st->schema_names[i] : NULL);
    free(st->schema_names);
    free(st->schema_types);
    free(st->key_indices);
    tf_spill_cleanup(st->spill);
    free(st->spill_dir);
    free(st);
}

static void unique_destroy(tf_step *self) {
    if (self) unique_state_free(self->state);
    free(self);
}

tf_step *tf_unique_create(const cJSON *args) {
    unique_state *st = calloc(1, sizeof(unique_state));
    if (!st) return NULL;
    st->output_batch_rows = UNIQUE_DEFAULT_OUTPUT_ROWS;

    if (args) {
        cJSON *sorted = cJSON_GetObjectItemCaseSensitive(args, "sorted");
        st->sorted = cJSON_IsBool(sorted) && cJSON_IsTrue(sorted);

        size_t parsed_size = 0;
        int has_max_keys = tf_json_get_size_arg(args, "max_keys",
                                                1, TF_MAX_COUNT_ARG,
                                                &parsed_size, "unique");
        if (has_max_keys < 0) { unique_state_free(st); return NULL; }
        if (has_max_keys > 0) st->max_keys = parsed_size;

        int has_max_state = tf_json_get_size_arg(args, "max_state_bytes",
                                                 1, TF_MAX_STATE_BYTES,
                                                 &parsed_size, "unique");
        if (has_max_state < 0) { unique_state_free(st); return NULL; }
        if (has_max_state > 0) st->max_state_bytes = parsed_size;

        cJSON *spill_dir_j = cJSON_GetObjectItemCaseSensitive(args, "spill_dir");
        if (cJSON_IsString(spill_dir_j) && spill_dir_j->valuestring && spill_dir_j->valuestring[0]) {
            st->use_spill = 1;
            st->spill_dir = strdup(spill_dir_j->valuestring);
            if (!st->spill_dir) { unique_state_free(st); return NULL; }
            if (tf_spill_session_create(st->spill_dir, &st->spill) != TF_OK) { unique_state_free(st); return NULL; }
            int has_spill_memory = tf_json_get_size_arg(args, "spill_memory_bytes",
                                                        1, TF_MAX_SPILL_MEMORY_BYTES,
                                                        &parsed_size, "unique");
            if (has_spill_memory < 0) { unique_state_free(st); return NULL; }
            if (has_spill_memory > 0) st->spill_memory_bytes = parsed_size;
            int has_spill_rows = tf_json_get_size_arg(args, "spill_run_rows",
                                                      1, TF_MAX_SPILL_RUN_ROWS,
                                                      &parsed_size, "unique");
            if (has_spill_rows < 0) { unique_state_free(st); return NULL; }
            if (has_spill_rows > 0) st->configured_run_rows = parsed_size;
            int has_output_rows = tf_json_get_size_arg(args, "spill_output_rows",
                                                       1, TF_MAX_SPILL_OUTPUT_ROWS,
                                                       &parsed_size, "unique");
            if (has_output_rows < 0) { unique_state_free(st); return NULL; }
            if (has_output_rows > 0) st->output_batch_rows = parsed_size;
        }

        cJSON *columns = cJSON_GetObjectItemCaseSensitive(args, "columns");
        if (columns) {
            if (!cJSON_IsArray(columns)) {
                tf_set_last_error("unique: columns must be an array");
                unique_state_free(st);
                return NULL;
            }
            int n = cJSON_GetArraySize(columns);
            if (n > 0) {
                st->key_cols = tf_callocarray_checked((size_t)n, sizeof(char *));
                if (!st->key_cols) { unique_state_free(st); return NULL; }
                st->n_key_cols = (size_t)n;
                for (int i = 0; i < n; i++) {
                    cJSON *item = cJSON_GetArrayItem(columns, i);
                    if (!cJSON_IsString(item) || !item->valuestring || item->valuestring[0] == '\0') {
                        tf_set_last_error("unique: column names must be non-empty strings");
                        unique_state_free(st);
                        return NULL;
                    }
                    st->key_cols[i] = strdup(item->valuestring);
                    if (!st->key_cols[i]) { unique_state_free(st); return NULL; }
                }
            }
        }
    }

    if (st->use_spill && st->sorted) {
        tf_set_last_error("unique: spill_dir and sorted=true are mutually exclusive");
        unique_state_free(st);
        return NULL;
    }
    if (!st->sorted && !st->use_spill && hs_init(&st->seen, 256) != 0) { unique_state_free(st); return NULL; }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { unique_state_free(st); return NULL; }
    step->process = unique_process;
    step->flush = unique_flush;
    step->flush_next = st->use_spill ? unique_flush_next : NULL;
    step->append_stats = unique_append_stats;
    step->destroy = unique_destroy;
    step->state = st;
    return step;
}
