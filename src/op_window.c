/*
 * op_window.c — Sliding window aggregations.
 *
 * Config: {"column": "price", "size": 3, "func": "avg", "result": "price_avg3"}
 * Supported funcs: avg, sum, min, max, count
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

typedef enum {
    WIN_AVG, WIN_SUM, WIN_MIN, WIN_MAX, WIN_COUNT,
} win_func;

typedef struct {
    char    *column;
    char    *result;
    win_func func;
    size_t   size;
    double  *ring;     /* circular buffer */
    size_t   head;     /* next write position */
    size_t   count;    /* items in buffer */
} window_state;

static win_func parse_win_func(const char *s) {
    if (strcmp(s, "avg") == 0) return WIN_AVG;
    if (strcmp(s, "sum") == 0) return WIN_SUM;
    if (strcmp(s, "min") == 0) return WIN_MIN;
    if (strcmp(s, "max") == 0) return WIN_MAX;
    if (strcmp(s, "count") == 0) return WIN_COUNT;
    return WIN_AVG;
}

static int window_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    (void)side;
    window_state *st = self->state;
    *out = NULL;

    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    tf_batch_set_schema(ob, in->n_cols, st->result, TF_TYPE_FLOAT64);

    int ci = tf_batch_col_index(in, st->column);

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);

        if (ci < 0 || tf_batch_is_null(in, r, ci)) {
            tf_batch_set_null(ob, r, in->n_cols);
            ob->n_rows = r + 1;
            continue;
        }

        double val = 0;
        if (in->col_types[ci] == TF_TYPE_INT64) val = (double)tf_batch_get_int64(in, r, ci);
        else if (in->col_types[ci] == TF_TYPE_FLOAT64) val = tf_batch_get_float64(in, r, ci);

        /* Add to ring buffer */
        st->ring[st->head] = val;
        st->head = (st->head + 1) % st->size;
        if (st->count < st->size) st->count++;

        /* Compute window aggregate */
        double result = 0;
        switch (st->func) {
            case WIN_SUM:
            case WIN_AVG: {
                double sum = 0;
                for (size_t i = 0; i < st->count; i++) sum += st->ring[i];
                result = (st->func == WIN_AVG) ? sum / st->count : sum;
                break;
            }
            case WIN_MIN: {
                result = st->ring[0];
                for (size_t i = 1; i < st->count; i++)
                    if (st->ring[i] < result) result = st->ring[i];
                break;
            }
            case WIN_MAX: {
                result = st->ring[0];
                for (size_t i = 1; i < st->count; i++)
                    if (st->ring[i] > result) result = st->ring[i];
                break;
            }
            case WIN_COUNT:
                result = (double)st->count;
                break;
        }

        tf_batch_set_float64(ob, r, in->n_cols, result);
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int window_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void window_destroy(tf_step *self) {
    window_state *st = self->state;
    if (st) { free(st->column); free(st->result); free(st->ring); free(st); }
    free(self);
}

tf_step *tf_window_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    cJSON *size_j = cJSON_GetObjectItemCaseSensitive(args, "size");
    cJSON *func_j = cJSON_GetObjectItemCaseSensitive(args, "func");
    if (!cJSON_IsString(col_j) || !cJSON_IsNumber(size_j) || !cJSON_IsString(func_j))
        return NULL;

    size_t win_size = (size_t)size_j->valueint;
    if (win_size == 0) win_size = 1;

    window_state *st = calloc(1, sizeof(window_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    st->func = parse_win_func(func_j->valuestring);
    st->size = win_size;
    st->ring = calloc(win_size, sizeof(double));

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = strdup(res_j->valuestring);
    } else {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s_%s%zu", st->column,
                 func_j->valuestring, win_size);
        st->result = strdup(buf);
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->column); free(st->result); free(st->ring); free(st); return NULL; }
    step->process = window_process;
    step->flush = window_flush;
    step->destroy = window_destroy;
    step->state = st;
    return step;
}
