/*
 * op_skip.c — Skip first N rows.
 *
 * Config: {"n": 5}
 * Discards the first N rows, passes through the rest.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    size_t n;
    size_t seen;
} skip_state;

static int skip_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    skip_state *st = self->state;
    *out = NULL;

    if (st->seen >= st->n) {
        /* Already past skip region — pass through all rows */
        tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
        if (!ob) return TF_ERROR;
        for (size_t c = 0; c < in->n_cols; c++) {
            tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
        }
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

    size_t remaining_skip = st->n - st->seen;

    if (remaining_skip >= in->n_rows) {
        /* Skip entire batch */
        st->seen += in->n_rows;
        return TF_OK;
    }

    /* Partial skip — emit rows after the skip region */
    size_t emit_start = remaining_skip;
    size_t emit_count = in->n_rows - emit_start;
    st->seen = st->n;

    tf_batch *ob = tf_batch_create(in->n_cols, emit_count);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++) {
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    }
    for (size_t i = 0; i < emit_count; i++) {
        size_t r = emit_start + i;
        tf_batch_ensure_capacity(ob, i + 1);
        for (size_t c = 0; c < in->n_cols; c++) {
            if (tf_batch_is_null(in, r, c)) {
                tf_batch_set_null(ob, i, c);
                continue;
            }
            switch (in->col_types[c]) {
                case TF_TYPE_BOOL:
                    tf_batch_set_bool(ob, i, c, tf_batch_get_bool(in, r, c));
                    break;
                case TF_TYPE_INT64:
                    tf_batch_set_int64(ob, i, c, tf_batch_get_int64(in, r, c));
                    break;
                case TF_TYPE_FLOAT64:
                    tf_batch_set_float64(ob, i, c, tf_batch_get_float64(in, r, c));
                    break;
                case TF_TYPE_STRING:
                    tf_batch_set_string(ob, i, c, tf_batch_get_string(in, r, c));
                    break;
                case TF_TYPE_DATE:
                    tf_batch_set_date(ob, i, c, tf_batch_get_date(in, r, c));
                    break;
                case TF_TYPE_TIMESTAMP:
                    tf_batch_set_timestamp(ob, i, c, tf_batch_get_timestamp(in, r, c));
                    break;
                default:
                    tf_batch_set_null(ob, i, c);
                    break;
            }
        }
        ob->n_rows = i + 1;
    }
    *out = ob;
    return TF_OK;
}

static int skip_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static void skip_destroy(tf_step *self) {
    free(self->state);
    free(self);
}

tf_step *tf_skip_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *n_json = cJSON_GetObjectItemCaseSensitive(args, "n");
    if (!cJSON_IsNumber(n_json) || n_json->valueint <= 0) return NULL;

    skip_state *st = calloc(1, sizeof(skip_state));
    if (!st) return NULL;
    st->n = (size_t)n_json->valueint;
    st->seen = 0;

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st); return NULL; }
    step->process = skip_process;
    step->flush = skip_flush;
    step->destroy = skip_destroy;
    step->state = st;
    return step;
}
