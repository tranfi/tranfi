/*
 * op_fill_down.c — Forward-fill nulls with last non-null value.
 *
 * Config: {"columns": ["city", "state"]}
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
    /* Last non-null value per column (stored as strings for simplicity) */
    char  **last_vals;
    int    *last_types; /* tf_type */
    int64_t *last_int;
    double  *last_float;
    int     *last_bool;
    int32_t *last_date;
    int64_t *last_timestamp;
    int      n_tracked;
    int      initialized;
} fill_down_state;

static int fill_down_is_target(const fill_down_state *st, const tf_batch *in, size_t c) {
    if (st->n_cols == 0) return 1;
    for (size_t k = 0; k < st->n_cols; k++) {
        if (strcmp(in->col_names[c], st->cols[k]) == 0) return 1;
    }
    return 0;
}

static void fill_down_free_string_slots(char **slots, size_t n) {
    if (!slots) return;
    for (size_t i = 0; i < n; i++) {
        free(slots[i]);
        slots[i] = NULL;
    }
}

static int fill_down_process(tf_step *self, tf_batch *in, tf_batch **out,
                             tf_side_channels *side) {
    (void)side;
    fill_down_state *st = self->state;
    *out = NULL;

    if (!st->initialized) {
        char **last_vals = calloc(in->n_cols, sizeof(char *));
        int *last_types = calloc(in->n_cols, sizeof(int));
        int64_t *last_int = calloc(in->n_cols, sizeof(int64_t));
        double *last_float = calloc(in->n_cols, sizeof(double));
        int *last_bool = calloc(in->n_cols, sizeof(int));
        int32_t *last_date = calloc(in->n_cols, sizeof(int32_t));
        int64_t *last_timestamp = calloc(in->n_cols, sizeof(int64_t));
        if (!last_vals || !last_types || !last_int || !last_float ||
            !last_bool || !last_date || !last_timestamp) {
            free(last_vals);
            free(last_types);
            free(last_int);
            free(last_float);
            free(last_bool);
            free(last_date);
            free(last_timestamp);
            return TF_ERROR;
        }
        for (size_t c = 0; c < in->n_cols; c++) last_types[c] = -1; /* no value yet */
        st->n_tracked = (int)in->n_cols;
        st->last_vals = last_vals;
        st->last_types = last_types;
        st->last_int = last_int;
        st->last_float = last_float;
        st->last_bool = last_bool;
        st->last_date = last_date;
        st->last_timestamp = last_timestamp;
        st->initialized = 1;
    }

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_schema(ob, in) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    char **pending_strings = calloc(in->n_cols, sizeof(char *));
    if (!pending_strings) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        memset(pending_strings, 0, sizeof(char *) * in->n_cols);
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            goto fail;
        }

        for (size_t c = 0; c < in->n_cols; c++) {
            if (!fill_down_is_target(st, in, c)) continue;

            if (!tf_batch_is_null(in, r, c)) {
                if (in->col_types[c] == TF_TYPE_STRING) {
                    const char *src = tf_batch_get_string(in, r, c);
                    pending_strings[c] = strdup(src ? src : "");
                    if (!pending_strings[c]) {
                        goto fail;
                    }
                }
            } else if (st->last_types[c] >= 0) {
                /* Fill with last value */
                int write_rc = TF_OK;
                switch (in->col_types[c]) {
                    case TF_TYPE_STRING:
                        if (st->last_vals[c]) write_rc = tf_batch_set_string(ob, r, c, st->last_vals[c]);
                        break;
                    case TF_TYPE_INT64:
                        write_rc = tf_batch_set_int64(ob, r, c, st->last_int[c]); break;
                    case TF_TYPE_FLOAT64:
                        write_rc = tf_batch_set_float64(ob, r, c, st->last_float[c]); break;
                    case TF_TYPE_BOOL:
                        write_rc = tf_batch_set_bool(ob, r, c, st->last_bool[c]); break;
                    case TF_TYPE_DATE:
                        write_rc = tf_batch_set_date(ob, r, c, st->last_date[c]); break;
                    case TF_TYPE_TIMESTAMP:
                        write_rc = tf_batch_set_timestamp(ob, r, c, st->last_timestamp[c]); break;
                    default: break;
                }
                if (write_rc != TF_OK) goto fail;
            }
        }

        for (size_t c = 0; c < in->n_cols; c++) {
            if (!fill_down_is_target(st, in, c) || tf_batch_is_null(in, r, c)) continue;

            st->last_types[c] = (int)in->col_types[c];
            switch (in->col_types[c]) {
                case TF_TYPE_STRING:
                    free(st->last_vals[c]);
                    st->last_vals[c] = pending_strings[c];
                    pending_strings[c] = NULL;
                    break;
                case TF_TYPE_INT64:
                    st->last_int[c] = tf_batch_get_int64(in, r, c); break;
                case TF_TYPE_FLOAT64:
                    st->last_float[c] = tf_batch_get_float64(in, r, c); break;
                case TF_TYPE_BOOL:
                    st->last_bool[c] = tf_batch_get_bool(in, r, c); break;
                case TF_TYPE_DATE:
                    st->last_date[c] = tf_batch_get_date(in, r, c); break;
                case TF_TYPE_TIMESTAMP:
                    st->last_timestamp[c] = tf_batch_get_timestamp(in, r, c); break;
                default:
                    break;
            }
        }
        ob->n_rows = r + 1;
    }

    free(pending_strings);
    *out = ob;
    return TF_OK;

fail:
    fill_down_free_string_slots(pending_strings, in->n_cols);
    free(pending_strings);
    tf_batch_free(ob);
    return TF_ERROR;
}

static int fill_down_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void fill_down_state_free(fill_down_state *st) {
    if (st) {
        if (st->cols) {
            for (size_t i = 0; i < st->n_cols; i++) free(st->cols[i]);
        }
        free(st->cols);
        if (st->last_vals) {
            for (int i = 0; i < st->n_tracked; i++) free(st->last_vals[i]);
            free(st->last_vals);
        }
        free(st->last_types); free(st->last_int);
        free(st->last_float); free(st->last_bool);
        free(st->last_date); free(st->last_timestamp);
        free(st);
    }
}

static void fill_down_destroy(tf_step *self) {
    fill_down_state_free(self->state);
    free(self);
}

tf_step *tf_fill_down_create(const cJSON *args) {
    fill_down_state *st = calloc(1, sizeof(fill_down_state));
    if (!st) return NULL;

    if (args) {
        cJSON *columns = cJSON_GetObjectItemCaseSensitive(args, "columns");
        if (columns && cJSON_IsArray(columns)) {
            int n = cJSON_GetArraySize(columns);
            if (n > 0) {
                st->cols = calloc(n, sizeof(char *));
                if (!st->cols) { fill_down_state_free(st); return NULL; }
                st->n_cols = (size_t)n;
                for (int i = 0; i < n; i++) {
                    cJSON *item = cJSON_GetArrayItem(columns, i);
                    if (!cJSON_IsString(item) || !item->valuestring[0]) {
                        fill_down_state_free(st);
                        return NULL;
                    }
                    st->cols[i] = strdup(item->valuestring);
                    if (!st->cols[i]) {
                        fill_down_state_free(st);
                        return NULL;
                    }
                }
            }
        }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { fill_down_state_free(st); return NULL; }
    step->process = fill_down_process;
    step->flush = fill_down_flush;
    step->destroy = fill_down_destroy;
    step->state = st;
    return step;
}
