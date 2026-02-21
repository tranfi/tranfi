/*
 * op_head.c — Take first N rows.
 *
 * Config: {"n": 5}
 * Passes through rows until N have been seen, then emits empty batches.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    size_t limit;
    size_t seen;
} head_state;

static int head_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    head_state *st = self->state;
    *out = NULL;

    if (st->seen >= st->limit) {
        /* Already past limit, discard */
        return TF_OK;
    }

    size_t remaining = st->limit - st->seen;
    size_t take = in->n_rows < remaining ? in->n_rows : remaining;

    if (take == in->n_rows) {
        /* Take all rows — create a copy */
        tf_batch *ob = tf_batch_create(in->n_cols, take);
        if (!ob) return TF_ERROR;
        for (size_t c = 0; c < in->n_cols; c++) {
            tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
        }
        for (size_t r = 0; r < take; r++) {
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
        st->seen += take;
        *out = ob;
    } else {
        /* Partial take — only copy first `take` rows */
        tf_batch *ob = tf_batch_create(in->n_cols, take);
        if (!ob) return TF_ERROR;
        for (size_t c = 0; c < in->n_cols; c++) {
            tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
        }
        for (size_t r = 0; r < take; r++) {
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
        st->seen += take;
        *out = ob;
    }

    return TF_OK;
}

static int head_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static void head_destroy(tf_step *self) {
    free(self->state);
    free(self);
}

tf_step *tf_head_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *n_json = cJSON_GetObjectItemCaseSensitive(args, "n");
    if (!cJSON_IsNumber(n_json) || n_json->valueint <= 0) return NULL;

    head_state *st = calloc(1, sizeof(head_state));
    if (!st) return NULL;
    st->limit = (size_t)n_json->valueint;
    st->seen = 0;

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st); return NULL; }
    step->process = head_process;
    step->flush = head_flush;
    step->destroy = head_destroy;
    step->state = st;
    return step;
}
