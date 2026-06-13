/*
 * op_rowid.c - Row counters, globally or within key groups.
 *
 * Config examples:
 *   {"result": "_rowid"}
 *   {"columns": ["city"], "result": "city_row", "max_keys": 10000}
 *   {"columns": ["city"], "result": "city_row", "sorted": true}
 *
 * With no columns, appends a global 1-based row counter with O(1) state.
 * With columns and sorted=true, counts consecutive sorted groups using only the
 * previous key and current count. With columns and sorted=false/default, counts
 * exact occurrences per key with key-state memory, optionally capped by
 * max_keys.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdint.h>
#include <limits.h>

typedef struct {
    char  *data;
    size_t len;
    size_t cap;
} key_buf;

typedef struct {
    char    **keys;
    int64_t  *counts;
    size_t    count;
    size_t    cap;
    size_t    key_bytes;
} rowid_map;

typedef struct {
    char      **cols;
    size_t      n_cols;
    char       *result;
    int         sorted;
    size_t      max_keys;
    size_t      max_state_bytes; /* 0 = unlimited */
    int64_t     global_count;
    char       *prev_key;
    int         seen_prev;
    int64_t     current_group_count;
    rowid_map   map;
} rowid_state;

static size_t rowid_retained_state_bytes(const rowid_state *st);

static void rowid_set_error(tf_side_channels *side, const char *msg) {
    tf_set_last_error(msg);
    if (side && side->errors) {
        tf_buffer_write_str(side->errors, msg);
        tf_buffer_write_str(side->errors, "\n");
    }
}

static int rowid_check_state_bytes(rowid_state *st, tf_side_channels *side) {
    if (!st || st->max_state_bytes == 0) return TF_OK;
    size_t retained = rowid_retained_state_bytes(st);
    if (retained <= st->max_state_bytes) return TF_OK;
    char msg[192];
    snprintf(msg, sizeof(msg),
             "rowid: max_state_bytes=%zu exceeded while tracking group row ids (%zu bytes retained)",
             st->max_state_bytes, retained);
    rowid_set_error(side, msg);
    return TF_ERROR;
}

static int key_buf_init(key_buf *b) {
    b->cap = 128;
    b->len = 0;
    b->data = malloc(b->cap);
    if (!b->data) return -1;
    b->data[0] = '\0';
    return 0;
}

static int key_buf_ensure(key_buf *b, size_t extra) {
    if (b->len + extra + 1 <= b->cap) return 0;
    size_t new_cap = b->cap ? b->cap : 128;
    while (new_cap < b->len + extra + 1) {
        if (new_cap > SIZE_MAX / 2) return -1;
        new_cap *= 2;
    }
    char *tmp = realloc(b->data, new_cap);
    if (!tmp) return -1;
    b->data = tmp;
    b->cap = new_cap;
    return 0;
}

static int key_buf_appendn(key_buf *b, const char *s, size_t n) {
    if (key_buf_ensure(b, n) != 0) return -1;
    memcpy(b->data + b->len, s, n);
    b->len += n;
    b->data[b->len] = '\0';
    return 0;
}

static int key_buf_append(key_buf *b, const char *s) {
    return key_buf_appendn(b, s, strlen(s));
}

static int key_buf_appendf(key_buf *b, const char *fmt, ...) {
    char tmp[128];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    if (n < 0) return -1;
    if ((size_t)n < sizeof(tmp)) return key_buf_appendn(b, tmp, (size_t)n);

    char *dyn = malloc((size_t)n + 1);
    if (!dyn) return -1;
    va_start(ap, fmt);
    vsnprintf(dyn, (size_t)n + 1, fmt, ap);
    va_end(ap);
    int rc = key_buf_appendn(b, dyn, (size_t)n);
    free(dyn);
    return rc;
}

static int append_cell_key(key_buf *b, const tf_batch *in, size_t row, int ci) {
    if (tf_batch_is_null(in, row, ci)) {
        return key_buf_appendf(b, "c%d:t%d:N;", ci, (int)in->col_types[ci]);
    }

    char num[96];
    const char *val = NULL;
    size_t len = 0;
    switch (in->col_types[ci]) {
        case TF_TYPE_STRING:
            val = tf_batch_get_string(in, row, ci);
            len = val ? strlen(val) : 0;
            break;
        case TF_TYPE_INT64:
            snprintf(num, sizeof(num), "%lld", (long long)tf_batch_get_int64(in, row, ci));
            val = num;
            len = strlen(num);
            break;
        case TF_TYPE_FLOAT64:
            snprintf(num, sizeof(num), "%.17g", tf_batch_get_float64(in, row, ci));
            val = num;
            len = strlen(num);
            break;
        case TF_TYPE_BOOL:
            val = tf_batch_get_bool(in, row, ci) ? "true" : "false";
            len = strlen(val);
            break;
        case TF_TYPE_DATE:
            snprintf(num, sizeof(num), "%d", (int)tf_batch_get_date(in, row, ci));
            val = num;
            len = strlen(num);
            break;
        case TF_TYPE_TIMESTAMP:
            snprintf(num, sizeof(num), "%lld", (long long)tf_batch_get_timestamp(in, row, ci));
            val = num;
            len = strlen(num);
            break;
        default:
            val = "";
            len = 0;
            break;
    }

    if (key_buf_appendf(b, "c%d:t%d:V%zu:", ci, (int)in->col_types[ci], len) != 0) return -1;
    if (key_buf_appendn(b, val ? val : "", len) != 0) return -1;
    return key_buf_append(b, ";");
}

static char *format_key(const tf_batch *in, size_t row,
                        const int *col_indices, size_t n_cols) {
    key_buf b;
    if (key_buf_init(&b) != 0) return NULL;
    for (size_t i = 0; i < n_cols; i++) {
        if (append_cell_key(&b, in, row, col_indices[i]) != 0) {
            free(b.data);
            return NULL;
        }
    }
    return b.data;
}

static uint32_t rowid_hash(const char *key) {
    uint32_t h = 5381;
    for (const char *p = key; *p; p++) {
        h = ((h << 5) + h) ^ (uint32_t)(unsigned char)*p;
    }
    return h;
}

static int map_init(rowid_map *m, size_t cap) {
    m->keys = calloc(cap, sizeof(char *));
    m->counts = calloc(cap, sizeof(int64_t));
    if (!m->keys || !m->counts) {
        free(m->keys);
        free(m->counts);
        m->keys = NULL;
        m->counts = NULL;
        return -1;
    }
    m->cap = cap;
    m->count = 0;
    m->key_bytes = 0;
    return 0;
}

static int map_grow(rowid_map *m) {
    size_t new_cap = m->cap ? m->cap * 2 : 64;
    char **new_keys = calloc(new_cap, sizeof(char *));
    int64_t *new_counts = calloc(new_cap, sizeof(int64_t));
    if (!new_keys || !new_counts) {
        free(new_keys);
        free(new_counts);
        return -1;
    }

    for (size_t i = 0; i < m->cap; i++) {
        if (!m->keys[i]) continue;
        uint32_t idx = rowid_hash(m->keys[i]) % new_cap;
        while (new_keys[idx]) idx = (idx + 1) % new_cap;
        new_keys[idx] = m->keys[i];
        new_counts[idx] = m->counts[i];
    }

    free(m->keys);
    free(m->counts);
    m->keys = new_keys;
    m->counts = new_counts;
    m->cap = new_cap;
    return 0;
}

static int map_increment(rowid_state *st, char *key, int64_t *out_count,
                         tf_side_channels *side) {
    if (!st->map.keys && map_init(&st->map, 64) != 0) {
        free(key);
        return TF_ERROR;
    }

    uint32_t idx = rowid_hash(key) % st->map.cap;
    while (st->map.keys[idx]) {
        if (strcmp(st->map.keys[idx], key) == 0) {
            st->map.counts[idx]++;
            *out_count = st->map.counts[idx];
            free(key);
            return TF_OK;
        }
        idx = (idx + 1) % st->map.cap;
    }

    if (st->max_keys > 0 && st->map.count >= st->max_keys) {
        char msg[160];
        snprintf(msg, sizeof(msg),
                 "rowid: max_keys=%zu exceeded while tracking group row ids",
                 st->max_keys);
        rowid_set_error(side, msg);
        free(key);
        return TF_ERROR;
    }

    if (st->map.count * 4 >= st->map.cap * 3) {
        if (map_grow(&st->map) != 0) {
            free(key);
            return TF_ERROR;
        }
        idx = rowid_hash(key) % st->map.cap;
        while (st->map.keys[idx]) idx = (idx + 1) % st->map.cap;
    }

    st->map.keys[idx] = key;
    st->map.key_bytes += strlen(key) + 1;
    st->map.counts[idx] = 1;
    st->map.count++;
    if (rowid_check_state_bytes(st, side) != TF_OK) return TF_ERROR;
    *out_count = 1;
    return TF_OK;
}

static int resolve_columns(const rowid_state *st, const tf_batch *in,
                           int **out_indices, size_t *out_n,
                           tf_side_channels *side) {
    *out_indices = NULL;
    *out_n = 0;
    if (st->n_cols == 0) return TF_OK;

    int *idx = malloc(st->n_cols * sizeof(int));
    if (!idx) return TF_ERROR;
    for (size_t i = 0; i < st->n_cols; i++) {
        idx[i] = tf_batch_col_index(in, st->cols[i]);
        if (idx[i] < 0) {
            char msg[256];
            snprintf(msg, sizeof(msg), "rowid: column '%s' not found", st->cols[i]);
            rowid_set_error(side, msg);
            free(idx);
            return TF_ERROR;
        }
    }
    *out_indices = idx;
    *out_n = st->n_cols;
    return TF_OK;
}

static int rowid_process(tf_step *self, tf_batch *in, tf_batch **out,
                         tf_side_channels *side) {
    rowid_state *st = self->state;
    *out = NULL;

    int *col_indices = NULL;
    size_t n_keys = 0;
    if (resolve_columns(st, in, &col_indices, &n_keys, side) != TF_OK) return TF_ERROR;

    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!ob) { free(col_indices); return TF_ERROR; }
    for (size_t c = 0; c < in->n_cols; c++) {
        if (tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]) != TF_OK) {
            tf_batch_free(ob);
            free(col_indices);
            return TF_ERROR;
        }
    }
    if (tf_batch_set_schema(ob, in->n_cols, st->result, TF_TYPE_INT64) != TF_OK) {
        tf_batch_free(ob);
        free(col_indices);
        return TF_ERROR;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        int64_t row_number = 0;
        if (n_keys == 0) {
            st->global_count++;
            row_number = st->global_count;
        } else {
            char *key = format_key(in, r, col_indices, n_keys);
            if (!key) { tf_batch_free(ob); free(col_indices); return TF_ERROR; }

            if (st->sorted) {
                if (!st->seen_prev || strcmp(key, st->prev_key) != 0) {
                    free(st->prev_key);
                    st->prev_key = key;
                    st->seen_prev = 1;
                    st->current_group_count = 1;
                    if (rowid_check_state_bytes(st, side) != TF_OK) {
                        tf_batch_free(ob);
                        free(col_indices);
                        return TF_ERROR;
                    }
                } else {
                    free(key);
                    st->current_group_count++;
                }
                row_number = st->current_group_count;
            } else {
                if (map_increment(st, key, &row_number, side) != TF_OK) {
                    tf_batch_free(ob);
                    free(col_indices);
                    return TF_ERROR;
                }
            }
        }

        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            free(col_indices);
            return TF_ERROR;
        }
        tf_batch_set_int64(ob, r, in->n_cols, row_number);
        ob->n_rows = r + 1;
    }

    free(col_indices);
    *out = ob;
    return TF_OK;
}

static int rowid_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self;
    (void)side;
    *out = NULL;
    return TF_OK;
}

static size_t rowid_prev_key_bytes(const rowid_state *st) {
    return (st && st->seen_prev && st->prev_key) ? strlen(st->prev_key) + 1 : 0;
}

static size_t rowid_retained_state_bytes(const rowid_state *st) {
    if (!st || st->n_cols == 0) return 0;
    if (st->sorted) return rowid_prev_key_bytes(st) + (st->seen_prev ? sizeof(int64_t) : 0);
    return st->map.key_bytes + st->map.cap * (sizeof(char *) + sizeof(int64_t));
}

static int rowid_append_stats(tf_step *self, tf_buffer *out) {
    if (!self || !self->state || !out) return TF_ERROR;
    rowid_state *st = self->state;
    size_t tracked_keys = 0;
    size_t tracked_key_bytes = 0;
    if (st->n_cols > 0) {
        tracked_keys = st->sorted ? (st->seen_prev ? 1u : 0u) : st->map.count;
        tracked_key_bytes = st->sorted ? rowid_prev_key_bytes(st) : st->map.key_bytes;
    }
    size_t retained = rowid_retained_state_bytes(st);
    char buf[300];
    snprintf(buf, sizeof(buf),
             ",\"tracked_keys\":%zu,\"tracked_key_bytes\":%zu,"
             "\"retained_state_bytes\":%zu,\"max_state_bytes\":%zu",
             tracked_keys, tracked_key_bytes, retained, st->max_state_bytes);
    return tf_buffer_write_str(out, buf);
}

static void rowid_state_free(rowid_state *st) {
    if (!st) return;
    for (size_t i = 0; i < st->n_cols; i++) free(st->cols[i]);
    free(st->cols);
    free(st->result);
    free(st->prev_key);
    for (size_t i = 0; i < st->map.cap; i++) free(st->map.keys ? st->map.keys[i] : NULL);
    free(st->map.keys);
    free(st->map.counts);
    free(st);
}

static void rowid_destroy(tf_step *self) {
    if (!self) return;
    rowid_state_free(self->state);
    free(self);
}

tf_step *tf_rowid_create(const cJSON *args) {
    if (!args) return NULL;

    rowid_state *st = calloc(1, sizeof(rowid_state));
    if (!st) return NULL;

    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (cJSON_IsArray(cols)) {
        int n = cJSON_GetArraySize(cols);
        if (n < 0) { rowid_state_free(st); return NULL; }
        st->cols = calloc((size_t)n, sizeof(char *));
        if (n > 0 && !st->cols) { rowid_state_free(st); return NULL; }
        st->n_cols = (size_t)n;
        for (int i = 0; i < n; i++) {
            cJSON *item = cJSON_GetArrayItem(cols, i);
            if (!cJSON_IsString(item) || !item->valuestring[0]) {
                rowid_state_free(st);
                return NULL;
            }
            st->cols[i] = strdup(item->valuestring);
            if (!st->cols[i]) { rowid_state_free(st); return NULL; }
        }
    }

    cJSON *res = cJSON_GetObjectItemCaseSensitive(args, "result");
    st->result = strdup(cJSON_IsString(res) && res->valuestring[0] ? res->valuestring : "_rowid");
    if (!st->result) { rowid_state_free(st); return NULL; }

    cJSON *sorted = cJSON_GetObjectItemCaseSensitive(args, "sorted");
    st->sorted = cJSON_IsBool(sorted) && cJSON_IsTrue(sorted);

    cJSON *max_keys = cJSON_GetObjectItemCaseSensitive(args, "max_keys");
    if (cJSON_IsNumber(max_keys) && max_keys->valuedouble > 0) {
        st->max_keys = (size_t)max_keys->valuedouble;
    }

    cJSON *max_state_bytes = cJSON_GetObjectItemCaseSensitive(args, "max_state_bytes");
    if (max_state_bytes) {
        if (!cJSON_IsNumber(max_state_bytes) || max_state_bytes->valuedouble <= 0) {
            tf_set_last_error("rowid: max_state_bytes must be positive");
            rowid_state_free(st);
            return NULL;
        }
        st->max_state_bytes = (size_t)max_state_bytes->valuedouble;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { rowid_state_free(st); return NULL; }
    step->process = rowid_process;
    step->flush = rowid_flush;
    step->append_stats = rowid_append_stats;
    step->destroy = rowid_destroy;
    step->state = st;
    return step;
}
