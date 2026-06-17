/*
 * op_rename.c — Rename columns.
 *
 * Config: {"mapping": {"old_name": "new_name", ...}}
 * Creates output batch with renamed columns. Unmatched columns pass through.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    char  **old_names;
    char  **new_names;
    size_t  n_mappings;
} rename_state;

static const char *find_rename(rename_state *st, const char *name) {
    if (!name) return "";
    for (size_t i = 0; i < st->n_mappings; i++) {
        if (strcmp(st->old_names[i], name) == 0)
            return st->new_names[i];
    }
    return name; /* unchanged */
}

static int rename_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    (void)side;
    rename_state *st = self->state;
    *out = NULL;

    /* Create output batch with renamed columns */
    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) return TF_ERROR;

    for (size_t i = 0; i < in->n_cols; i++) {
        const char *new_name = find_rename(st, in->col_names[i]);
        if (tf_batch_set_schema(ob, i, new_name, in->col_types[i]) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    /* Copy all rows */
    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    *out = ob;
    return TF_OK;
}

static int rename_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static void rename_state_free(rename_state *st) {
    if (!st) return;
    for (size_t i = 0; i < st->n_mappings; i++) {
        if (st->old_names) free(st->old_names[i]);
        if (st->new_names) free(st->new_names[i]);
    }
    free(st->old_names);
    free(st->new_names);
    free(st);
}

static void rename_destroy(tf_step *self) {
    rename_state_free(self ? self->state : NULL);
    free(self);
}

tf_step *tf_rename_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *mapping = cJSON_GetObjectItemCaseSensitive(args, "mapping");
    if (!mapping || !cJSON_IsObject(mapping)) return NULL;

    int n = cJSON_GetArraySize(mapping);
    if (n <= 0) return NULL;

    rename_state *st = calloc(1, sizeof(rename_state));
    if (!st) return NULL;
    st->n_mappings = (size_t)n;
    st->old_names = calloc((size_t)n, sizeof(char *));
    st->new_names = calloc((size_t)n, sizeof(char *));
    if (!st->old_names || !st->new_names) {
        free(st->old_names);
        free(st->new_names);
        free(st);
        return NULL;
    }

    int i = 0;
    cJSON *item;
    cJSON_ArrayForEach(item, mapping) {
        const char *old_name = item->string ? item->string : "";
        const char *new_name = cJSON_IsString(item) ? item->valuestring : old_name;
        st->old_names[i] = strdup(old_name);
        st->new_names[i] = strdup(new_name ? new_name : "");
        if (!st->old_names[i] || !st->new_names[i]) {
            rename_state_free(st);
            return NULL;
        }
        i++;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) {
        rename_state_free(st);
        return NULL;
    }
    step->process = rename_process;
    step->flush = rename_flush;
    step->destroy = rename_destroy;
    step->state = st;
    return step;
}
