/*
 * op_rleid.c - Consecutive run identifier.
 *
 * Config: {"columns": ["city", "status"], "result": "run_id"}
 *
 * Appends an int64 run id that starts at 1 and increments whenever the
 * selected key changes from the previous row. State retained across batches is
 * only the previous formatted key and the current id.
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
} rleid_buf;

typedef struct {
    char   **cols;
    size_t   n_cols;
    char    *result;
    char    *prev_key;
    int      seen;
    int64_t  current_id;
} rleid_state;

static int rleid_buf_init(rleid_buf *b) {
    b->cap = 128;
    b->len = 0;
    b->data = tf_mallocarray_checked(b->cap, sizeof(char));
    if (!b->data) return -1;
    b->data[0] = '\0';
    return 0;
}

static int rleid_buf_ensure(rleid_buf *b, size_t extra) {
    size_t need = 0;
    if (tf_size_add(b->len, extra, &need) != TF_OK ||
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

static int rleid_buf_appendn(rleid_buf *b, const char *s, size_t n) {
    if (rleid_buf_ensure(b, n) != 0) return -1;
    memcpy(b->data + b->len, s, n);
    b->len += n;
    b->data[b->len] = '\0';
    return 0;
}

static int rleid_buf_append(rleid_buf *b, const char *s) {
    return rleid_buf_appendn(b, s, strlen(s));
}

static int rleid_buf_appendf(rleid_buf *b, const char *fmt, ...) {
    char tmp[128];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(tmp, sizeof(tmp), fmt, ap);
    va_end(ap);
    if (n < 0) return -1;
    if ((size_t)n < sizeof(tmp)) return rleid_buf_appendn(b, tmp, (size_t)n);

    char *dyn = malloc((size_t)n + 1);
    if (!dyn) return -1;
    va_start(ap, fmt);
    vsnprintf(dyn, (size_t)n + 1, fmt, ap);
    va_end(ap);
    int rc = rleid_buf_appendn(b, dyn, (size_t)n);
    free(dyn);
    return rc;
}

static int rleid_set_error(tf_side_channels *side, const char *msg) {
    return tf_side_write_error(side, msg);
}

static int append_cell_key(rleid_buf *b, const tf_batch *in, size_t row, int ci) {
    if (tf_batch_is_null(in, row, ci)) {
        return rleid_buf_appendf(b, "c%d:t%d:N;", ci, (int)in->col_types[ci]);
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

    if (rleid_buf_appendf(b, "c%d:t%d:V%zu:", ci, (int)in->col_types[ci], len) != 0) return -1;
    if (rleid_buf_appendn(b, val ? val : "", len) != 0) return -1;
    return rleid_buf_append(b, ";");
}

static char *format_rleid_key(const tf_batch *in, size_t row, const int *col_indices, size_t n_cols) {
    rleid_buf b;
    if (rleid_buf_init(&b) != 0) return NULL;
    for (size_t i = 0; i < n_cols; i++) {
        if (append_cell_key(&b, in, row, col_indices[i]) != 0) {
            free(b.data);
            return NULL;
        }
    }
    return b.data;
}

static int resolve_columns(const rleid_state *st, const tf_batch *in, int **out_indices,
                           size_t *out_n, tf_side_channels *side) {
    if (st->n_cols == 0) {
        if (rleid_set_error(side, "rleid: columns must not be empty") != TF_OK) return TF_ERROR;
        return TF_ERROR;
    }
    int *idx = malloc(st->n_cols * sizeof(int));
    if (!idx) return TF_ERROR;
    for (size_t i = 0; i < st->n_cols; i++) {
        idx[i] = tf_batch_col_index(in, st->cols[i]);
        if (idx[i] < 0) {
            char msg[256];
            snprintf(msg, sizeof(msg), "rleid: column '%s' not found", st->cols[i]);
            int err_rc = rleid_set_error(side, msg);
            free(idx);
            if (err_rc != TF_OK) return TF_ERROR;
            return TF_ERROR;
        }
    }
    *out_indices = idx;
    *out_n = st->n_cols;
    return TF_OK;
}

static int rleid_process(tf_step *self, tf_batch *in, tf_batch **out,
                         tf_side_channels *side) {
    rleid_state *st = self->state;
    *out = NULL;

    int *col_indices = NULL;
    size_t n_keys = 0;
    if (resolve_columns(st, in, &col_indices, &n_keys, side) != TF_OK) return TF_ERROR;

    const char *extra_names[1] = {st->result};
    tf_type extra_types[1] = {TF_TYPE_INT64};
    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!ob) { free(col_indices); return TF_ERROR; }
    if (tf_batch_clone_with_extra_cols(ob, in, extra_names, extra_types, 1) != TF_OK) {
        tf_batch_free(ob);
        free(col_indices);
        return TF_ERROR;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        char *key = format_rleid_key(in, r, col_indices, n_keys);
        if (!key) { tf_batch_free(ob); free(col_indices); return TF_ERROR; }
        if (!st->seen || strcmp(key, st->prev_key) != 0) {
            st->current_id++;
            st->seen = 1;
            free(st->prev_key);
            st->prev_key = key;
        } else {
            free(key);
        }
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            free(col_indices);
            return TF_ERROR;
        }
        if (tf_batch_set_int64(ob, r, in->n_cols, st->current_id) != TF_OK) {
            tf_batch_free(ob);
            free(col_indices);
            return TF_ERROR;
        }
        ob->n_rows = r + 1;
    }

    free(col_indices);
    *out = ob;
    return TF_OK;
}

static int rleid_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self;
    (void)side;
    *out = NULL;
    return TF_OK;
}

static void rleid_state_free(rleid_state *st) {
    if (!st) return;
    if (st->cols) {
        for (size_t i = 0; i < st->n_cols; i++) free(st->cols[i]);
    }
    free(st->cols);
    free(st->result);
    free(st->prev_key);
    free(st);
}

static void rleid_destroy(tf_step *self) {
    if (!self) return;
    rleid_state_free(self->state);
    free(self);
}

tf_step *tf_rleid_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!cJSON_IsArray(cols) || cJSON_GetArraySize(cols) <= 0) {
        tf_set_last_error("rleid: columns must be a non-empty array");
        return NULL;
    }

    rleid_state *st = calloc(1, sizeof(rleid_state));
    if (!st) return NULL;

    int n = cJSON_GetArraySize(cols);
    st->cols = calloc((size_t)n, sizeof(char *));
    if (!st->cols) { rleid_state_free(st); return NULL; }
    st->n_cols = (size_t)n;
    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(cols, i);
        if (!cJSON_IsString(item) || !item->valuestring || item->valuestring[0] == '\0') {
            tf_set_last_error("rleid: column names must be non-empty strings");
            rleid_state_free(st);
            return NULL;
        }
        st->cols[i] = strdup(item->valuestring);
        if (!st->cols[i]) { rleid_state_free(st); return NULL; }
    }

    cJSON *res = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res) && (!res->valuestring || res->valuestring[0] == '\0')) {
        tf_set_last_error("rleid: result must be a non-empty string");
        rleid_state_free(st);
        return NULL;
    }
    st->result = strdup(cJSON_IsString(res) && res->valuestring ? res->valuestring : "_rleid");
    if (!st->result) { rleid_state_free(st); return NULL; }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { rleid_state_free(st); return NULL; }
    step->process = rleid_process;
    step->flush = rleid_flush;
    step->destroy = rleid_destroy;
    step->state = st;
    return step;
}
