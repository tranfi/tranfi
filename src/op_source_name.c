/*
 * op_source_name.c -- Append the current host-provided source name.
 *
 * The source name is pipeline metadata set by the embedding host before push()
 * or input-boundary flushes. The op itself is row-local and never opens files.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char *result;
    char *default_value;
} source_name_state;

static int source_name_process(tf_step *self, tf_batch *in, tf_batch **out,
                               tf_side_channels *side) {
    source_name_state *st = self ? self->state : NULL;
    if (!st || !in || !out) return TF_ERROR;
    *out = NULL;

    if (tf_batch_col_index(in, st->result) >= 0) {
        char msg[256];
        snprintf(msg, sizeof(msg), "source-name result column already exists: %s", st->result);
        tf_set_last_error(msg);
        return TF_ERROR;
    }

    const char *extra_names[1] = {st->result};
    tf_type extra_types[1] = {TF_TYPE_STRING};
    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_with_extra_cols(ob, in, extra_names, extra_types, 1) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    const char *value = st->default_value ? st->default_value : "";
    if (side && side->source_name && side->source_name[0]) value = side->source_name;

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_set_string(ob, r, in->n_cols, value) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    if (ob->n_rows > 0) {
        *out = ob;
    } else {
        tf_batch_free(ob);
    }
    return TF_OK;
}

static int source_name_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self;
    (void)side;
    *out = NULL;
    return TF_OK;
}

static void source_name_destroy(tf_step *self) {
    if (!self) return;
    source_name_state *st = self->state;
    if (st) {
        free(st->result);
        free(st->default_value);
        free(st);
    }
    free(self);
}

tf_step *tf_source_name_create(const cJSON *args) {
    const char *result = "_source";
    const char *default_value = "";
    if (args) {
        cJSON *res = cJSON_GetObjectItemCaseSensitive(args, "result");
        if (!cJSON_IsString(res)) res = cJSON_GetObjectItemCaseSensitive(args, "column");
        if (!cJSON_IsString(res)) res = cJSON_GetObjectItemCaseSensitive(args, "as");
        if (cJSON_IsString(res) && res->valuestring && res->valuestring[0]) result = res->valuestring;
        cJSON *def = cJSON_GetObjectItemCaseSensitive(args, "default");
        if (cJSON_IsString(def) && def->valuestring) default_value = def->valuestring;
    }

    source_name_state *st = calloc(1, sizeof(source_name_state));
    if (!st) return NULL;
    st->result = strdup(result);
    st->default_value = strdup(default_value);
    if (!st->result || !st->default_value) {
        free(st->result);
        free(st->default_value);
        free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) {
        free(st->result);
        free(st->default_value);
        free(st);
        return NULL;
    }
    step->process = source_name_process;
    step->flush = source_name_flush;
    step->destroy = source_name_destroy;
    step->state = st;
    return step;
}
