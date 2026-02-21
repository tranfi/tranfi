/*
 * op_fill_null.c — Replace nulls with default values.
 *
 * Config: {"mapping": {"col1": "default1", "col2": "0"}}
 */

#include "internal.h"
#include "cJSON.h"
#include "date_utils.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    char **col_names;
    char **defaults;
    size_t n;
} fill_null_state;

static int fill_null_process(tf_step *self, tf_batch *in, tf_batch **out,
                             tf_side_channels *side) {
    (void)side;
    fill_null_state *st = self->state;
    *out = NULL;

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);
        ob->n_rows = r + 1;
    }

    /* Fill nulls */
    for (size_t k = 0; k < st->n; k++) {
        int ci = tf_batch_col_index(ob, st->col_names[k]);
        if (ci < 0) continue;
        for (size_t r = 0; r < ob->n_rows; r++) {
            if (!tf_batch_is_null(ob, r, ci)) continue;
            const char *def = st->defaults[k];
            switch (ob->col_types[ci]) {
                case TF_TYPE_STRING:
                    tf_batch_set_string(ob, r, ci, def);
                    break;
                case TF_TYPE_INT64: {
                    char *end;
                    int64_t v = strtoll(def, &end, 10);
                    if (*end == '\0') tf_batch_set_int64(ob, r, ci, v);
                    break;
                }
                case TF_TYPE_FLOAT64: {
                    char *end;
                    double v = strtod(def, &end);
                    if (*end == '\0') tf_batch_set_float64(ob, r, ci, v);
                    break;
                }
                case TF_TYPE_BOOL:
                    tf_batch_set_bool(ob, r, ci, strcmp(def, "true") == 0);
                    break;
                case TF_TYPE_DATE: {
                    char *end;
                    int32_t v = (int32_t)strtol(def, &end, 10);
                    if (*end == '\0') tf_batch_set_date(ob, r, ci, v);
                    break;
                }
                case TF_TYPE_TIMESTAMP: {
                    char *end;
                    int64_t v = strtoll(def, &end, 10);
                    if (*end == '\0') tf_batch_set_timestamp(ob, r, ci, v);
                    break;
                }
                default:
                    break;
            }
        }
    }

    *out = ob;
    return TF_OK;
}

static int fill_null_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void fill_null_destroy(tf_step *self) {
    fill_null_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n; i++) { free(st->col_names[i]); free(st->defaults[i]); }
        free(st->col_names); free(st->defaults);
        free(st);
    }
    free(self);
}

tf_step *tf_fill_null_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *mapping = cJSON_GetObjectItemCaseSensitive(args, "mapping");
    if (!mapping || !cJSON_IsObject(mapping)) return NULL;

    int n = cJSON_GetArraySize(mapping);
    fill_null_state *st = calloc(1, sizeof(fill_null_state));
    if (!st) return NULL;
    st->col_names = calloc(n, sizeof(char *));
    st->defaults = calloc(n, sizeof(char *));
    st->n = n;

    int i = 0;
    cJSON *entry = NULL;
    cJSON_ArrayForEach(entry, mapping) {
        st->col_names[i] = strdup(entry->string);
        st->defaults[i] = strdup(cJSON_IsString(entry) ? entry->valuestring : "");
        i++;
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { fill_null_destroy(&(tf_step){.state = st}); return NULL; }
    step->process = fill_null_process;
    step->flush = fill_null_flush;
    step->destroy = fill_null_destroy;
    step->state = st;
    return step;
}
