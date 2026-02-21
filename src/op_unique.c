/*
 * op_unique.c — Deduplicate rows by key columns using a hash set.
 *
 * Config: {"columns": ["name", "city"]}
 *   or {} for dedup by all columns.
 */

#include "internal.h"
#include "cJSON.h"
#include "date_utils.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ---- Simple hash set (open addressing) ---- */

typedef struct {
    char  **keys;
    size_t  count;
    size_t  cap;
} hash_set;

static void hs_init(hash_set *hs, size_t cap) {
    hs->cap = cap;
    hs->count = 0;
    hs->keys = calloc(cap, sizeof(char *));
}

static uint32_t hs_hash(const char *key) {
    uint32_t h = 5381;
    for (const char *p = key; *p; p++)
        h = ((h << 5) + h) ^ (uint32_t)(unsigned char)*p;
    return h;
}

static int hs_grow(hash_set *hs) {
    size_t new_cap = hs->cap * 2;
    char **new_keys = calloc(new_cap, sizeof(char *));
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

/* Returns 1 if key was newly inserted, 0 if already present, -1 on error */
static int hs_insert(hash_set *hs, const char *key) {
    if (hs->count * 4 >= hs->cap * 3) { /* load factor > 0.75 */
        if (hs_grow(hs) != 0) return -1;
    }
    uint32_t idx = hs_hash(key) % hs->cap;
    while (hs->keys[idx]) {
        if (strcmp(hs->keys[idx], key) == 0) return 0; /* already exists */
        idx = (idx + 1) % hs->cap;
    }
    hs->keys[idx] = strdup(key);
    if (!hs->keys[idx]) return -1;
    hs->count++;
    return 1;
}

static void hs_free(hash_set *hs) {
    for (size_t i = 0; i < hs->cap; i++) free(hs->keys[i]);
    free(hs->keys);
}

/* ---- Unique transform ---- */

typedef struct {
    char    **key_cols;     /* column names to dedup by (NULL = all) */
    size_t    n_key_cols;
    hash_set  seen;
} unique_state;

static char *build_row_key(const tf_batch *b, size_t row,
                           int *col_indices, size_t n_keys) {
    /* Build key by concatenating column values with \x01 separator */
    size_t buf_cap = 256;
    char *buf = malloc(buf_cap);
    if (!buf) return NULL;
    size_t buf_len = 0;

    for (size_t k = 0; k < n_keys; k++) {
        int c = col_indices[k];
        if (c < 0) continue;

        if (k > 0) {
            if (buf_len + 1 >= buf_cap) {
                buf_cap *= 2;
                char *tmp = realloc(buf, buf_cap);
                if (!tmp) { free(buf); return NULL; }
                buf = tmp;
            }
            buf[buf_len++] = '\x01';
        }

        char val_buf[64];
        const char *val = NULL;
        size_t val_len = 0;

        if (tf_batch_is_null(b, row, c)) {
            val = "\\N";
            val_len = 2;
        } else {
            switch (b->col_types[c]) {
                case TF_TYPE_BOOL:
                    val = tf_batch_get_bool(b, row, c) ? "T" : "F";
                    val_len = 1;
                    break;
                case TF_TYPE_INT64:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%lld",
                                       (long long)tf_batch_get_int64(b, row, c));
                    val = val_buf;
                    break;
                case TF_TYPE_FLOAT64:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%.17g",
                                       tf_batch_get_float64(b, row, c));
                    val = val_buf;
                    break;
                case TF_TYPE_STRING:
                    val = tf_batch_get_string(b, row, c);
                    val_len = strlen(val);
                    break;
                case TF_TYPE_DATE:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%d",
                                       (int)tf_batch_get_date(b, row, c));
                    val = val_buf;
                    break;
                case TF_TYPE_TIMESTAMP:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%lld",
                                       (long long)tf_batch_get_timestamp(b, row, c));
                    val = val_buf;
                    break;
                default:
                    val = "\\N";
                    val_len = 2;
                    break;
            }
        }

        while (buf_len + val_len >= buf_cap) {
            buf_cap *= 2;
            char *tmp = realloc(buf, buf_cap);
            if (!tmp) { free(buf); return NULL; }
            buf = tmp;
        }
        memcpy(buf + buf_len, val, val_len);
        buf_len += val_len;
    }
    buf[buf_len] = '\0';
    return buf;
}

static int unique_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    (void)side;
    unique_state *st = self->state;
    *out = NULL;

    /* Resolve column indices */
    size_t n_keys;
    int *col_indices;
    if (st->n_key_cols > 0) {
        n_keys = st->n_key_cols;
        col_indices = malloc(n_keys * sizeof(int));
        if (!col_indices) return TF_ERROR;
        for (size_t k = 0; k < n_keys; k++) {
            col_indices[k] = tf_batch_col_index(in, st->key_cols[k]);
        }
    } else {
        /* All columns */
        n_keys = in->n_cols;
        col_indices = malloc(n_keys * sizeof(int));
        if (!col_indices) return TF_ERROR;
        for (size_t k = 0; k < n_keys; k++) {
            col_indices[k] = (int)k;
        }
    }

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) { free(col_indices); return TF_ERROR; }
    for (size_t c = 0; c < in->n_cols; c++) {
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    }

    size_t out_row = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        char *key = build_row_key(in, r, col_indices, n_keys);
        if (!key) { free(col_indices); tf_batch_free(ob); return TF_ERROR; }

        int inserted = hs_insert(&st->seen, key);
        free(key);
        if (inserted < 0) { free(col_indices); tf_batch_free(ob); return TF_ERROR; }
        if (inserted == 0) continue; /* duplicate */

        /* Copy row */
        tf_batch_ensure_capacity(ob, out_row + 1);
        for (size_t c = 0; c < in->n_cols; c++) {
            if (tf_batch_is_null(in, r, c)) {
                tf_batch_set_null(ob, out_row, c);
                continue;
            }
            switch (in->col_types[c]) {
                case TF_TYPE_BOOL:
                    tf_batch_set_bool(ob, out_row, c, tf_batch_get_bool(in, r, c));
                    break;
                case TF_TYPE_INT64:
                    tf_batch_set_int64(ob, out_row, c, tf_batch_get_int64(in, r, c));
                    break;
                case TF_TYPE_FLOAT64:
                    tf_batch_set_float64(ob, out_row, c, tf_batch_get_float64(in, r, c));
                    break;
                case TF_TYPE_STRING:
                    tf_batch_set_string(ob, out_row, c, tf_batch_get_string(in, r, c));
                    break;
                case TF_TYPE_DATE:
                    tf_batch_set_date(ob, out_row, c, tf_batch_get_date(in, r, c));
                    break;
                case TF_TYPE_TIMESTAMP:
                    tf_batch_set_timestamp(ob, out_row, c, tf_batch_get_timestamp(in, r, c));
                    break;
                default:
                    tf_batch_set_null(ob, out_row, c);
                    break;
            }
        }
        out_row++;
    }
    ob->n_rows = out_row;
    free(col_indices);

    if (out_row > 0) {
        *out = ob;
    } else {
        tf_batch_free(ob);
    }
    return TF_OK;
}

static int unique_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static void unique_destroy(tf_step *self) {
    unique_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_key_cols; i++) free(st->key_cols[i]);
        free(st->key_cols);
        hs_free(&st->seen);
        free(st);
    }
    free(self);
}

tf_step *tf_unique_create(const cJSON *args) {
    unique_state *st = calloc(1, sizeof(unique_state));
    if (!st) return NULL;
    hs_init(&st->seen, 256);

    if (args) {
        cJSON *columns = cJSON_GetObjectItemCaseSensitive(args, "columns");
        if (columns && cJSON_IsArray(columns)) {
            int n = cJSON_GetArraySize(columns);
            if (n > 0) {
                st->key_cols = calloc(n, sizeof(char *));
                st->n_key_cols = n;
                for (int i = 0; i < n; i++) {
                    cJSON *item = cJSON_GetArrayItem(columns, i);
                    if (cJSON_IsString(item)) {
                        st->key_cols[i] = strdup(item->valuestring);
                    }
                }
            }
        }
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) {
        for (size_t i = 0; i < st->n_key_cols; i++) free(st->key_cols[i]);
        free(st->key_cols);
        hs_free(&st->seen);
        free(st);
        return NULL;
    }
    step->process = unique_process;
    step->flush = unique_flush;
    step->destroy = unique_destroy;
    step->state = st;
    return step;
}
