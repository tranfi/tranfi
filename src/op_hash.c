/*
 * op_hash.c — DJB2 hash of columns, adds _hash column.
 *
 * Config: {"columns": ["name", "city"]}
 *   or {} for all columns.
 */

#include "internal.h"
#include "cJSON.h"
#include "date_utils.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char  **cols;
    size_t  n_cols;
} hash_state;

static int hash_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    hash_state *st = self->state;
    *out = NULL;

    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!ob) return TF_ERROR;
    const char *extra_names[] = {"_hash"};
    const tf_type extra_types[] = {TF_TYPE_INT64};
    if (tf_batch_clone_with_extra_cols(ob, in, extra_names, extra_types, 1) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    /* Resolve column indices */
    size_t n_keys;
    int *col_indices;
    if (st->n_cols > 0) {
        n_keys = st->n_cols;
        col_indices = malloc(n_keys * sizeof(int));
        if (!col_indices) { tf_batch_free(ob); return TF_ERROR; }
        for (size_t k = 0; k < n_keys; k++)
            col_indices[k] = tf_batch_col_index(in, st->cols[k]);
    } else {
        n_keys = in->n_cols;
        col_indices = malloc(n_keys * sizeof(int));
        if (!col_indices) { tf_batch_free(ob); return TF_ERROR; }
        for (size_t k = 0; k < n_keys; k++) col_indices[k] = (int)k;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            free(col_indices);
            tf_batch_free(ob);
            return TF_ERROR;
        }
        /* Compute hash */
        uint32_t h = 5381;
        char val_buf[64];
        for (size_t k = 0; k < n_keys; k++) {
            int ci = col_indices[k];
            if (ci < 0 || tf_batch_is_null(in, r, ci)) continue;
            const char *val;
            switch (in->col_types[ci]) {
                case TF_TYPE_STRING: val = tf_batch_get_string(in, r, ci); break;
                case TF_TYPE_INT64:
                    snprintf(val_buf, sizeof(val_buf), "%lld", (long long)tf_batch_get_int64(in, r, ci));
                    val = val_buf; break;
                case TF_TYPE_FLOAT64:
                    snprintf(val_buf, sizeof(val_buf), "%.17g", tf_batch_get_float64(in, r, ci));
                    val = val_buf; break;
                case TF_TYPE_BOOL:
                    val = tf_batch_get_bool(in, r, ci) ? "T" : "F"; break;
                case TF_TYPE_DATE:
                    snprintf(val_buf, sizeof(val_buf), "%d", (int)tf_batch_get_date(in, r, ci));
                    val = val_buf; break;
                case TF_TYPE_TIMESTAMP:
                    snprintf(val_buf, sizeof(val_buf), "%lld", (long long)tf_batch_get_timestamp(in, r, ci));
                    val = val_buf; break;
                default: val = ""; break;
            }
            for (const unsigned char *p = (const unsigned char *)val; *p; p++)
                h = ((h << 5) + h) ^ *p;
        }
        if (tf_batch_set_int64(ob, r, in->n_cols, (int64_t)h) != TF_OK) {
            free(col_indices);
            tf_batch_free(ob);
            return TF_ERROR;
        }
        ob->n_rows = r + 1;
    }

    free(col_indices);
    *out = ob;
    return TF_OK;
}

static int hash_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void hash_state_free(hash_state *st) {
    if (st) {
        if (st->cols) {
            for (size_t i = 0; i < st->n_cols; i++) free(st->cols[i]);
        }
        free(st->cols);
        free(st);
    }
}

static void hash_destroy(tf_step *self) {
    hash_state_free(self ? self->state : NULL);
    free(self);
}

tf_step *tf_hash_create(const cJSON *args) {
    hash_state *st = calloc(1, sizeof(hash_state));
    if (!st) return NULL;

    if (args) {
        cJSON *columns = cJSON_GetObjectItemCaseSensitive(args, "columns");
        if (columns) {
            if (!cJSON_IsArray(columns)) {
                tf_set_last_error("hash: columns must be an array");
                hash_state_free(st);
                return NULL;
            }
            int n = cJSON_GetArraySize(columns);
            if (n > 0) {
                st->cols = calloc(n, sizeof(char *));
                if (!st->cols) { hash_state_free(st); return NULL; }
                st->n_cols = (size_t)n;
                for (int i = 0; i < n; i++) {
                    cJSON *item = cJSON_GetArrayItem(columns, i);
                    if (!cJSON_IsString(item) || !item->valuestring || !item->valuestring[0]) {
                        tf_set_last_error("hash: column names must be non-empty strings");
                        hash_state_free(st);
                        return NULL;
                    }
                    st->cols[i] = strdup(item->valuestring);
                    if (!st->cols[i]) { hash_state_free(st); return NULL; }
                }
            }
        }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { hash_state_free(st); return NULL; }
    step->process = hash_process;
    step->flush = hash_flush;
    step->destroy = hash_destroy;
    step->state = st;
    return step;
}
