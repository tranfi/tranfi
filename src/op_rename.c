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

    /* Create output batch with renamed columns */
    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) return TF_ERROR;

    for (size_t i = 0; i < in->n_cols; i++) {
        const char *new_name = find_rename(st, in->col_names[i]);
        tf_batch_set_schema(ob, i, new_name, in->col_types[i]);
    }

    /* Copy all rows */
    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_ensure_capacity(ob, r + 1);
        for (size_t c = 0; c < in->n_cols; c++) {
            if (tf_batch_is_null(in, r, c)) {
                tf_batch_set_null(ob, r, c);
                continue;
            }
            switch (in->col_types[c]) {
                case TF_TYPE_BOOL:
                    tf_batch_set_bool(ob, r, c, tf_batch_get_bool(in, r, c));
                    break;
                case TF_TYPE_INT64:
                    tf_batch_set_int64(ob, r, c, tf_batch_get_int64(in, r, c));
                    break;
                case TF_TYPE_FLOAT64:
                    tf_batch_set_float64(ob, r, c, tf_batch_get_float64(in, r, c));
                    break;
                case TF_TYPE_STRING:
                    tf_batch_set_string(ob, r, c, tf_batch_get_string(in, r, c));
                    break;
                case TF_TYPE_DATE:
                    tf_batch_set_date(ob, r, c, tf_batch_get_date(in, r, c));
                    break;
                case TF_TYPE_TIMESTAMP:
                    tf_batch_set_timestamp(ob, r, c, tf_batch_get_timestamp(in, r, c));
                    break;
                default:
                    tf_batch_set_null(ob, r, c);
                    break;
            }
        }
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int rename_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static void rename_destroy(tf_step *self) {
    rename_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_mappings; i++) {
            free(st->old_names[i]);
            free(st->new_names[i]);
        }
        free(st->old_names);
        free(st->new_names);
        free(st);
    }
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
    st->old_names = malloc(n * sizeof(char *));
    st->new_names = malloc(n * sizeof(char *));
    if (!st->old_names || !st->new_names) {
        free(st->old_names);
        free(st->new_names);
        free(st);
        return NULL;
    }

    int i = 0;
    cJSON *item;
    cJSON_ArrayForEach(item, mapping) {
        st->old_names[i] = strdup(item->string);
        st->new_names[i] = strdup(cJSON_IsString(item) ? item->valuestring : item->string);
        i++;
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) {
        rename_destroy(&(tf_step){.state = st});
        return NULL;
    }
    step->process = rename_process;
    step->flush = rename_flush;
    step->destroy = rename_destroy;
    step->state = st;
    return step;
}
