/*
 * op_split.c — Split column into multiple columns.
 *
 * Config: {"column": "name", "delimiter": " ", "names": ["first", "last"]}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    char  *column;
    char  *delimiter;
    char **names;
    size_t n_names;
} split_state;

static int split_process(tf_step *self, tf_batch *in, tf_batch **out,
                         tf_side_channels *side) {
    (void)side;
    split_state *st = self->state;
    *out = NULL;

    size_t out_cols = 0;
    if (tf_size_add(in->n_cols, st->n_names, &out_cols) != TF_OK) return TF_ERROR;
    tf_batch *ob = tf_batch_create(out_cols, in->n_rows);
    if (!ob) return TF_ERROR;

    /* Copy existing schema */
    for (size_t c = 0; c < in->n_cols; c++) {
        if (tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    /* Add new columns */
    for (size_t k = 0; k < st->n_names; k++) {
        if (tf_batch_set_schema(ob, in->n_cols + k, st->names[k], TF_TYPE_STRING) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    int ci = tf_batch_col_index(in, st->column);
    size_t delim_len = strlen(st->delimiter);

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        /* Initialize new columns as null */
        for (size_t k = 0; k < st->n_names; k++) {
            if (tf_batch_set_null(ob, r, in->n_cols + k) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
        }

        if (ci >= 0 && !tf_batch_is_null(in, r, ci) && in->col_types[ci] == TF_TYPE_STRING) {
            const char *val = tf_batch_get_string(in, r, ci);
            const char *p = val ? val : "";
            for (size_t k = 0; k < st->n_names && *p; k++) {
                const char *found = strstr(p, st->delimiter);
                size_t tok_len = found ? (size_t)(found - p) : strlen(p);
                char *tok = malloc(tok_len + 1);
                if (!tok) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                memcpy(tok, p, tok_len);
                tok[tok_len] = '\0';
                int rc = tf_batch_set_string(ob, r, in->n_cols + k, tok);
                free(tok);
                if (rc != TF_OK) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                if (found) p = found + delim_len;
                else break;
            }
        }
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int split_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void split_state_free(split_state *st) {
    if (!st) return;
    free(st->column);
    free(st->delimiter);
    if (st->names) {
        for (size_t i = 0; i < st->n_names; i++) free(st->names[i]);
    }
    free(st->names);
    free(st);
}

static void split_destroy(tf_step *self) {
    split_state_free(self ? self->state : NULL);
    free(self);
}

tf_step *tf_split_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    cJSON *names_j = cJSON_GetObjectItemCaseSensitive(args, "names");
    if (!cJSON_IsString(col_j) || !cJSON_IsArray(names_j)) return NULL;

    int n = cJSON_GetArraySize(names_j);
    if (n <= 0) return NULL;

    split_state *st = calloc(1, sizeof(split_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    if (!st->column) { split_state_free(st); return NULL; }

    cJSON *delim_j = cJSON_GetObjectItemCaseSensitive(args, "delimiter");
    const char *delim = cJSON_IsString(delim_j) ? delim_j->valuestring : " ";
    if (delim[0] == '\0') { split_state_free(st); return NULL; }
    st->delimiter = strdup(delim);
    if (!st->delimiter) { split_state_free(st); return NULL; }

    st->names = calloc((size_t)n, sizeof(char *));
    if (!st->names) { split_state_free(st); return NULL; }
    st->n_names = (size_t)n;
    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(names_j, i);
        if (!cJSON_IsString(item)) { split_state_free(st); return NULL; }
        st->names[i] = strdup(item->valuestring);
        if (!st->names[i]) { split_state_free(st); return NULL; }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { split_state_free(st); return NULL; }
    step->process = split_process;
    step->flush = split_flush;
    step->destroy = split_destroy;
    step->state = st;
    return step;
}
