/*
 * op_label_encode.c — Map categorical values to sequential integers.
 *
 * Config: {"column": "city", "result": "city_encoded"}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct label_entry {
    char  *value;
    int64_t label;
} label_entry;

typedef struct {
    char         *column;
    char         *result;
    label_entry  *entries;
    size_t        n_entries;
    size_t        cap;
    int64_t       next_label;
} label_encode_state;

static const char *get_string_value(const tf_batch *b, size_t r, int ci, char *buf, size_t bufsz) {
    if (tf_batch_is_null(b, r, ci)) return NULL;
    switch (b->col_types[ci]) {
        case TF_TYPE_STRING: return tf_batch_get_string(b, r, ci);
        case TF_TYPE_INT64:
            snprintf(buf, bufsz, "%lld", (long long)tf_batch_get_int64(b, r, ci));
            return buf;
        case TF_TYPE_FLOAT64:
            snprintf(buf, bufsz, "%.17g", tf_batch_get_float64(b, r, ci));
            return buf;
        case TF_TYPE_BOOL:
            return tf_batch_get_bool(b, r, ci) ? "true" : "false";
        default: return NULL;
    }
}

static int64_t lookup_or_assign(label_encode_state *st, const char *val) {
    for (size_t i = 0; i < st->n_entries; i++) {
        if (strcmp(st->entries[i].value, val) == 0)
            return st->entries[i].label;
    }
    /* New value — assign next label */
    if (st->n_entries >= st->cap) {
        size_t newcap = st->cap ? st->cap * 2 : 16;
        label_entry *tmp = realloc(st->entries, newcap * sizeof(label_entry));
        if (!tmp) return -1;
        st->entries = tmp;
        st->cap = newcap;
    }
    st->entries[st->n_entries].value = strdup(val);
    st->entries[st->n_entries].label = st->next_label;
    st->n_entries++;
    return st->next_label++;
}

static int label_encode_process(tf_step *self, tf_batch *in, tf_batch **out,
                                tf_side_channels *side) {
    (void)side;
    label_encode_state *st = self->state;
    *out = NULL;

    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    tf_batch_set_schema(ob, in->n_cols, st->result, TF_TYPE_INT64);

    int ci = tf_batch_col_index(in, st->column);

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);

        if (ci < 0 || tf_batch_is_null(in, r, ci)) {
            tf_batch_set_null(ob, r, in->n_cols);
            ob->n_rows = r + 1;
            continue;
        }

        char buf[64];
        const char *val = get_string_value(in, r, ci, buf, sizeof(buf));
        if (!val) {
            tf_batch_set_null(ob, r, in->n_cols);
            ob->n_rows = r + 1;
            continue;
        }

        int64_t label = lookup_or_assign(st, val);
        tf_batch_set_int64(ob, r, in->n_cols, label);
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int label_encode_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void label_encode_destroy(tf_step *self) {
    label_encode_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_entries; i++)
            free(st->entries[i].value);
        free(st->entries);
        free(st->column);
        free(st->result);
        free(st);
    }
    free(self);
}

tf_step *tf_label_encode_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j)) return NULL;

    label_encode_state *st = calloc(1, sizeof(label_encode_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = strdup(res_j->valuestring);
    } else {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s_encoded", st->column);
        st->result = strdup(buf);
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->column); free(st->result); free(st); return NULL; }
    step->process = label_encode_process;
    step->flush = label_encode_flush;
    step->destroy = label_encode_destroy;
    step->state = st;
    return step;
}
