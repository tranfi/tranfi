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

static int rowid_set_error(tf_side_channels *side, const char *msg) {
    return tf_side_write_error(side, msg);
}

static int rowid_check_state_bytes(rowid_state *st, tf_side_channels *side) {
    if (!st || st->max_state_bytes == 0) return TF_OK;
    size_t retained = rowid_retained_state_bytes(st);
    if (retained <= st->max_state_bytes) return TF_OK;
    char msg[192];
    snprintf(msg, sizeof(msg),
             "rowid: max_state_bytes=%zu exceeded while tracking group row ids (%zu bytes retained)",
             st->max_state_bytes, retained);
    if (rowid_set_error(side, msg) != TF_OK) return TF_ERROR;
    return TF_ERROR;
}

static int key_buf_init(key_buf *b) {
    b->cap = 128;
    b->len = 0;
    b->data = tf_mallocarray_checked(b->cap, sizeof(char));
    if (!b->data) return -1;
    b->data[0] = '\0';
    return 0;
}

static int key_buf_ensure(key_buf *b, size_t extra) {
    size_t need = 0;
    if (!b || !b->data ||
        tf_size_add(b->len, extra, &need) != TF_OK ||
        tf_size_add(need, 1, &need) != TF_OK) {
        return -1;
    }
    if (need <= b->cap) return 0;
    size_t new_cap = 0;
    if (tf_size_grow_pow2(b->cap, need, 128, &new_cap) != TF_OK) return -1;
    char *tmp = tf_reallocarray_checked(b->data, new_cap, sizeof(char));
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

    size_t dyn_len = 0;
    if (tf_size_add((size_t)n, 1, &dyn_len) != TF_OK) return -1;
    char *dyn = tf_mallocarray_checked(dyn_len, sizeof(char));
    if (!dyn) return -1;
    va_start(ap, fmt);
    vsnprintf(dyn, dyn_len, fmt, ap);
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
    m->keys = tf_callocarray_checked(cap ? cap : 1, sizeof(char *));
    m->counts = tf_callocarray_checked(cap ? cap : 1, sizeof(int64_t));
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
    size_t new_cap = 0;
    if (m->cap == 0) {
        new_cap = 64;
    } else if (tf_size_mul(m->cap, 2, &new_cap) != TF_OK) {
        return -1;
    }
    char **new_keys = tf_callocarray_checked(new_cap, sizeof(char *));
    int64_t *new_counts = tf_callocarray_checked(new_cap, sizeof(int64_t));
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
            if (st->map.counts[idx] == INT64_MAX) {
                free(key);
                return TF_ERROR;
            }
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
        int err_rc = rowid_set_error(side, msg);
        free(key);
        if (err_rc != TF_OK) return TF_ERROR;
        return TF_ERROR;
    }

    size_t load_count = 0;
    size_t load_limit = 0;
    if (tf_size_mul(st->map.count, 4, &load_count) != TF_OK ||
        tf_size_mul(st->map.cap, 3, &load_limit) != TF_OK) {
        free(key);
        return TF_ERROR;
    }
    if (load_count >= load_limit) {
        if (map_grow(&st->map) != 0) {
            free(key);
            return TF_ERROR;
        }
        idx = rowid_hash(key) % st->map.cap;
        while (st->map.keys[idx]) idx = (idx + 1) % st->map.cap;
    }

    size_t key_bytes_delta = 0;
    size_t new_key_bytes = 0;
    size_t new_count = 0;
    if (tf_size_add(strlen(key), 1, &key_bytes_delta) != TF_OK ||
        tf_size_add(st->map.key_bytes, key_bytes_delta, &new_key_bytes) != TF_OK ||
        tf_size_add(st->map.count, 1, &new_count) != TF_OK) {
        free(key);
        return TF_ERROR;
    }
    st->map.keys[idx] = key;
    st->map.key_bytes = new_key_bytes;
    st->map.counts[idx] = 1;
    st->map.count = new_count;
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

    int *idx = tf_mallocarray_checked(st->n_cols ? st->n_cols : 1, sizeof(int));
    if (!idx) return TF_ERROR;
    for (size_t i = 0; i < st->n_cols; i++) {
        idx[i] = tf_batch_col_index(in, st->cols[i]);
        if (idx[i] < 0) {
            char msg[256];
            snprintf(msg, sizeof(msg), "rowid: column '%s' not found", st->cols[i]);
            int err_rc = rowid_set_error(side, msg);
            free(idx);
            if (err_rc != TF_OK) return TF_ERROR;
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

    size_t out_cols = 0;
    if (tf_size_add(in->n_cols, 1, &out_cols) != TF_OK) {
        free(col_indices);
        return TF_ERROR;
    }
    tf_batch *ob = tf_batch_create(out_cols, in->n_rows);
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
            if (st->global_count == INT64_MAX) {
                tf_batch_free(ob);
                free(col_indices);
                return TF_ERROR;
            }
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
                    if (st->current_group_count == INT64_MAX) {
                        tf_batch_free(ob);
                        free(col_indices);
                        return TF_ERROR;
                    }
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

        if (tf_batch_copy_row(ob, r, in, r) != TF_OK ||
            tf_batch_set_int64(ob, r, in->n_cols, row_number) != TF_OK) {
            tf_batch_free(ob);
            free(col_indices);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            free(col_indices);
            return TF_ERROR;
        }
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
    size_t bytes = 0;
    if (!st || !st->seen_prev || !st->prev_key) return 0;
    if (tf_size_add(strlen(st->prev_key), 1, &bytes) != TF_OK) return SIZE_MAX;
    return bytes;
}

static size_t rowid_retained_state_bytes(const rowid_state *st) {
    if (!st || st->n_cols == 0) return 0;
    if (st->sorted) {
        size_t bytes = rowid_prev_key_bytes(st);
        if (st->seen_prev && tf_size_add(bytes, sizeof(int64_t), &bytes) != TF_OK) return SIZE_MAX;
        return bytes;
    }
    size_t slot_width = 0;
    size_t slot_bytes = 0;
    size_t bytes = st->map.key_bytes;
    if (tf_size_add(sizeof(char *), sizeof(int64_t), &slot_width) != TF_OK ||
        tf_size_mul(st->map.cap, slot_width, &slot_bytes) != TF_OK ||
        tf_size_add(bytes, slot_bytes, &bytes) != TF_OK) {
        return SIZE_MAX;
    }
    return bytes;
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
    if (st->cols) {
        for (size_t i = 0; i < st->n_cols; i++) free(st->cols[i]);
    }
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
    if (cols) {
        if (!cJSON_IsArray(cols)) {
            tf_set_last_error("rowid: columns must be an array");
            rowid_state_free(st);
            return NULL;
        }
        int n = cJSON_GetArraySize(cols);
        if (n < 0) { rowid_state_free(st); return NULL; }
        st->n_cols = (size_t)n;
        if (n > 0) {
            st->cols = tf_callocarray_checked((size_t)n, sizeof(char *));
            if (!st->cols) { rowid_state_free(st); return NULL; }
        }
        for (int i = 0; i < n; i++) {
            cJSON *item = cJSON_GetArrayItem(cols, i);
            if (!cJSON_IsString(item) || !item->valuestring || item->valuestring[0] == '\0') {
                tf_set_last_error("rowid: column names must be non-empty strings");
                rowid_state_free(st);
                return NULL;
            }
            st->cols[i] = strdup(item->valuestring);
            if (!st->cols[i]) { rowid_state_free(st); return NULL; }
        }
    }

    cJSON *res = cJSON_GetObjectItemCaseSensitive(args, "result");
    st->result = strdup(cJSON_IsString(res) && res->valuestring && res->valuestring[0] ? res->valuestring : "_rowid");
    if (!st->result) { rowid_state_free(st); return NULL; }

    cJSON *sorted = cJSON_GetObjectItemCaseSensitive(args, "sorted");
    st->sorted = cJSON_IsBool(sorted) && cJSON_IsTrue(sorted);

    size_t parsed_size = 0;
    int has_max_keys = tf_json_get_size_arg(args, "max_keys",
                                            1, TF_MAX_COUNT_ARG,
                                            &parsed_size, "rowid");
    if (has_max_keys < 0) { rowid_state_free(st); return NULL; }
    if (has_max_keys > 0) st->max_keys = parsed_size;

    int has_max_state = tf_json_get_size_arg(args, "max_state_bytes",
                                             1, TF_MAX_STATE_BYTES,
                                             &parsed_size, "rowid");
    if (has_max_state < 0) { rowid_state_free(st); return NULL; }
    if (has_max_state > 0) st->max_state_bytes = parsed_size;

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { rowid_state_free(st); return NULL; }
    step->process = rowid_process;
    step->flush = rowid_flush;
    step->append_stats = rowid_append_stats;
    step->destroy = rowid_destroy;
    step->state = st;
    return step;
}
