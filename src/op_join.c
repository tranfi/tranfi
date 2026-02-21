/*
 * op_join.c — Hash join with lookup CSV file.
 *
 * Config: {"file": "lookup.csv", "on": "id" or "left=right", "how": "inner|left"}
 * Loads lookup file into memory once, then streams main data through with
 * hash map probes. Supports inner and left join.
 */

#include "internal.h"
#include "date_utils.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Hash map bucket: key → array of row indices in lookup batch */
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
} join_hash_map;

typedef struct {
    char       *file;
    char       *left_col;
    char       *right_col;
    int         how;          /* 0=inner, 1=left */

    tf_batch   *lookup;
    int         loaded;
    int         lookup_join_col;

    /* Lookup columns to include in output (excluding join key) */
    int        *lookup_out_cols;
    size_t      n_lookup_out;

    join_hash_map map;
} join_state;

/* FNV-1a hash */
static uint64_t fnv1a(const char *s) {
    uint64_t h = 14695981039346656037ULL;
    while (*s) { h ^= (uint8_t)*s++; h *= 1099511628211ULL; }
    return h;
}

static void map_init(join_hash_map *m, size_t hint) {
    size_t n = 64;
    while (n < hint * 2) n *= 2;
    m->buckets = calloc(n, sizeof(join_bucket));
    m->n_buckets = n;
    m->count = 0;
}

static void map_insert(join_hash_map *m, const char *key, size_t row) {
    uint64_t h = fnv1a(key);
    size_t idx = h & (m->n_buckets - 1);
    /* Linear probe */
    while (m->buckets[idx].key) {
        if (strcmp(m->buckets[idx].key, key) == 0) {
            /* Add row to existing bucket */
            join_bucket *b = &m->buckets[idx];
            if (b->n_rows >= b->rows_cap) {
                b->rows_cap = b->rows_cap ? b->rows_cap * 2 : 4;
                b->rows = realloc(b->rows, b->rows_cap * sizeof(size_t));
            }
            b->rows[b->n_rows++] = row;
            return;
        }
        idx = (idx + 1) & (m->n_buckets - 1);
    }
    /* New key */
    join_bucket *b = &m->buckets[idx];
    b->key = strdup(key);
    b->rows = malloc(4 * sizeof(size_t));
    b->rows_cap = 4;
    b->rows[0] = row;
    b->n_rows = 1;
    m->count++;
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
}

/* Format a cell as string for join key comparison */
static char *format_join_key(const tf_batch *b, size_t row, int col) {
    if (tf_batch_is_null(b, row, col)) return strdup("\\N");
    char buf[64];
    switch (b->col_types[col]) {
        case TF_TYPE_STRING: return strdup(tf_batch_get_string(b, row, col));
        case TF_TYPE_INT64:
            snprintf(buf, sizeof(buf), "%lld", (long long)tf_batch_get_int64(b, row, col));
            return strdup(buf);
        case TF_TYPE_FLOAT64:
            snprintf(buf, sizeof(buf), "%.17g", tf_batch_get_float64(b, row, col));
            return strdup(buf);
        case TF_TYPE_BOOL:
            return strdup(tf_batch_get_bool(b, row, col) ? "true" : "false");
        case TF_TYPE_DATE:
            snprintf(buf, sizeof(buf), "%d", (int)tf_batch_get_date(b, row, col));
            return strdup(buf);
        case TF_TYPE_TIMESTAMP:
            snprintf(buf, sizeof(buf), "%lld", (long long)tf_batch_get_timestamp(b, row, col));
            return strdup(buf);
        default:
            return strdup("\\N");
    }
}

/* Copy a cell from src batch to dst batch (different column index) */
static void copy_cell(tf_batch *dst, size_t dr, size_t dc,
                      const tf_batch *src, size_t sr, int sc) {
    if (tf_batch_is_null(src, sr, sc)) {
        tf_batch_set_null(dst, dr, dc);
        return;
    }
    switch (src->col_types[sc]) {
        case TF_TYPE_BOOL:      tf_batch_set_bool(dst, dr, dc, tf_batch_get_bool(src, sr, sc)); break;
        case TF_TYPE_INT64:     tf_batch_set_int64(dst, dr, dc, tf_batch_get_int64(src, sr, sc)); break;
        case TF_TYPE_FLOAT64:   tf_batch_set_float64(dst, dr, dc, tf_batch_get_float64(src, sr, sc)); break;
        case TF_TYPE_STRING:    tf_batch_set_string(dst, dr, dc, tf_batch_get_string(src, sr, sc)); break;
        case TF_TYPE_DATE:      tf_batch_set_date(dst, dr, dc, tf_batch_get_date(src, sr, sc)); break;
        case TF_TYPE_TIMESTAMP: tf_batch_set_timestamp(dst, dr, dc, tf_batch_get_timestamp(src, sr, sc)); break;
        default: tf_batch_set_null(dst, dr, dc); break;
    }
}

static int load_lookup(join_state *st) {
    FILE *f = fopen(st->file, "rb");
    if (!f) return TF_ERROR;

    /* Read entire file */
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (fsize <= 0) { fclose(f); return TF_ERROR; }

    uint8_t *data = malloc((size_t)fsize);
    if (!data) { fclose(f); return TF_ERROR; }
    size_t nread = fread(data, 1, (size_t)fsize, f);
    fclose(f);
    if (nread != (size_t)fsize) { free(data); return TF_ERROR; }

    /* Decode using CSV decoder */
    tf_decoder *dec = tf_csv_decoder_create(NULL);
    if (!dec) { free(data); return TF_ERROR; }

    tf_batch **batches = NULL;
    size_t n_batches = 0;
    if (dec->decode(dec, data, nread, &batches, &n_batches) != TF_OK) {
        dec->destroy(dec);
        free(data);
        return TF_ERROR;
    }

    tf_batch **flush_batches = NULL;
    size_t n_flush = 0;
    dec->flush(dec, &flush_batches, &n_flush);

    /* Count total rows and merge into single batch */
    size_t total_rows = 0;
    size_t total_batches = n_batches + n_flush;
    tf_batch **all_batches = malloc(total_batches * sizeof(tf_batch *));
    if (!all_batches) { dec->destroy(dec); free(data); return TF_ERROR; }
    for (size_t i = 0; i < n_batches; i++) {
        all_batches[i] = batches[i];
        total_rows += batches[i]->n_rows;
    }
    for (size_t i = 0; i < n_flush; i++) {
        all_batches[n_batches + i] = flush_batches[i];
        total_rows += flush_batches[i]->n_rows;
    }

    if (total_batches == 0 || total_rows == 0) {
        free(all_batches);
        free(batches); free(flush_batches);
        dec->destroy(dec); free(data);
        return TF_ERROR;
    }

    /* Use first batch as schema source */
    tf_batch *first = all_batches[0];
    tf_batch *merged = tf_batch_create(first->n_cols, total_rows);
    if (!merged) {
        for (size_t i = 0; i < total_batches; i++) tf_batch_free(all_batches[i]);
        free(all_batches); free(batches); free(flush_batches);
        dec->destroy(dec); free(data);
        return TF_ERROR;
    }
    for (size_t c = 0; c < first->n_cols; c++)
        tf_batch_set_schema(merged, c, first->col_names[c], first->col_types[c]);

    size_t dst_row = 0;
    for (size_t b = 0; b < total_batches; b++) {
        for (size_t r = 0; r < all_batches[b]->n_rows; r++) {
            tf_batch_copy_row(merged, dst_row, all_batches[b], r);
            merged->n_rows = ++dst_row;
        }
    }

    for (size_t i = 0; i < total_batches; i++) tf_batch_free(all_batches[i]);
    free(all_batches);
    free(batches);
    free(flush_batches);
    dec->destroy(dec);
    free(data);

    st->lookup = merged;

    /* Find join column in lookup */
    st->lookup_join_col = tf_batch_col_index(merged, st->right_col);
    if (st->lookup_join_col < 0) return TF_ERROR;

    /* Determine output columns (all except join key) */
    st->lookup_out_cols = malloc(merged->n_cols * sizeof(int));
    st->n_lookup_out = 0;
    for (size_t c = 0; c < merged->n_cols; c++) {
        if ((int)c != st->lookup_join_col)
            st->lookup_out_cols[st->n_lookup_out++] = (int)c;
    }

    /* Build hash map */
    map_init(&st->map, merged->n_rows);
    for (size_t r = 0; r < merged->n_rows; r++) {
        char *key = format_join_key(merged, r, st->lookup_join_col);
        if (key) { map_insert(&st->map, key, r); free(key); }
    }

    return TF_OK;
}

static int join_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    join_state *st = self->state;
    *out = NULL;

    if (!st->loaded) {
        if (load_lookup(st) != TF_OK) return TF_ERROR;
        st->loaded = 1;
    }

    int left_ci = tf_batch_col_index(in, st->left_col);
    if (left_ci < 0) return TF_ERROR;

    size_t n_out_cols = in->n_cols + st->n_lookup_out;

    /* Worst case: each input row matches multiple lookup rows.
     * Start with input size, grow as needed. */
    size_t out_cap = in->n_rows > 0 ? in->n_rows : 16;
    tf_batch *ob = tf_batch_create(n_out_cols, out_cap);
    if (!ob) return TF_ERROR;

    /* Set schema: main columns + lookup columns */
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    for (size_t k = 0; k < st->n_lookup_out; k++) {
        int lc = st->lookup_out_cols[k];
        tf_batch_set_schema(ob, in->n_cols + k,
                            st->lookup->col_names[lc],
                            st->lookup->col_types[lc]);
    }

    size_t out_row = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        char *key = format_join_key(in, r, left_ci);
        if (!key) continue;

        join_bucket *bucket = map_find(&st->map, key);
        free(key);

        if (bucket) {
            /* Emit one row per match */
            for (size_t m = 0; m < bucket->n_rows; m++) {
                size_t lr = bucket->rows[m];
                tf_batch_ensure_capacity(ob, out_row + 1);
                /* Copy main columns */
                for (size_t c = 0; c < in->n_cols; c++)
                    copy_cell(ob, out_row, c, in, r, (int)c);
                /* Copy lookup columns */
                for (size_t k = 0; k < st->n_lookup_out; k++)
                    copy_cell(ob, out_row, in->n_cols + k, st->lookup, lr, st->lookup_out_cols[k]);
                ob->n_rows = ++out_row;
            }
        } else if (st->how == 1) {
            /* Left join: emit main + nulls */
            tf_batch_ensure_capacity(ob, out_row + 1);
            for (size_t c = 0; c < in->n_cols; c++)
                copy_cell(ob, out_row, c, in, r, (int)c);
            for (size_t k = 0; k < st->n_lookup_out; k++)
                tf_batch_set_null(ob, out_row, in->n_cols + k);
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
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void join_destroy(tf_step *self) {
    join_state *st = self->state;
    if (st) {
        free(st->file);
        free(st->left_col);
        free(st->right_col);
        if (st->lookup) tf_batch_free(st->lookup);
        free(st->lookup_out_cols);
        map_free(&st->map);
        free(st);
    }
    free(self);
}

tf_step *tf_join_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *file_j = cJSON_GetObjectItemCaseSensitive(args, "file");
    cJSON *on_j = cJSON_GetObjectItemCaseSensitive(args, "on");
    if (!cJSON_IsString(file_j) || !cJSON_IsString(on_j)) return NULL;

    join_state *st = calloc(1, sizeof(join_state));
    if (!st) return NULL;

    st->file = strdup(file_j->valuestring);

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

    cJSON *how_j = cJSON_GetObjectItemCaseSensitive(args, "how");
    if (cJSON_IsString(how_j) && strcmp(how_j->valuestring, "left") == 0)
        st->how = 1;

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { join_destroy(&(tf_step){.state = st}); return NULL; }
    step->process = join_process;
    step->flush = join_flush;
    step->destroy = join_destroy;
    step->state = st;
    return step;
}
