/*
 * op_unpivot.c — Wide to long. Adds variable + value columns, multiplies rows.
 *
 * Config: {"columns": ["q1", "q2", "q3"]}
 *   Keeps non-listed columns, melts listed columns into variable/value pairs.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char  **cols;
    size_t  n_cols;
    size_t  max_output_rows_per_input_row;
    size_t  max_output_rows_per_batch;
} unpivot_state;

static int unpivot_set_cap_error(const char *name, size_t limit) {
    char msg[160];
    snprintf(msg, sizeof(msg), "unpivot: %s=%zu exceeded", name, limit);
    tf_set_last_error(msg);
    return TF_ERROR;
}

static int unpivot_process(tf_step *self, tf_batch *in, tf_batch **out,
                           tf_side_channels *side) {
    (void)side;
    unpivot_state *st = self->state;
    *out = NULL;

    /* Determine which columns are value columns and which are id columns */
    int *is_value = tf_callocarray_checked(in->n_cols ? in->n_cols : 1, sizeof(int));
    if (!is_value) return TF_ERROR;
    size_t n_value = 0;
    for (size_t c = 0; c < in->n_cols; c++) {
        for (size_t k = 0; k < st->n_cols; k++) {
            if (strcmp(in->col_names[c], st->cols[k]) == 0) {
                is_value[c] = 1;
                n_value++;
                break;
            }
        }
    }
    size_t n_id = in->n_cols - n_value;
    if (n_value == 0) { free(is_value); return TF_OK; }

    /* Output: id columns + "variable" + "value" */
    size_t out_n_cols = 0;
    if (tf_size_add(n_id, 2, &out_n_cols) != TF_OK) { free(is_value); return TF_ERROR; }
    if (n_value > st->max_output_rows_per_input_row) {
        unpivot_set_cap_error("max_output_rows_per_input_row", st->max_output_rows_per_input_row);
        free(is_value);
        return TF_ERROR;
    }

    size_t *id_cols = NULL;
    if (n_id > 0) {
        id_cols = tf_callocarray_checked(n_id, sizeof(size_t));
        if (!id_cols) { free(is_value); return TF_ERROR; }
    }

    size_t max_rows = 0;
    if (tf_size_mul(in->n_rows, n_value, &max_rows) != TF_OK ||
        max_rows > st->max_output_rows_per_batch) {
        unpivot_set_cap_error("max_output_rows_per_batch", st->max_output_rows_per_batch);
        free(id_cols);
        free(is_value);
        return TF_ERROR;
    }
    tf_batch *ob = tf_batch_create(out_n_cols, max_rows > 0 ? max_rows : 16);
    if (!ob) { free(id_cols); free(is_value); return TF_ERROR; }

    size_t oc = 0;
    for (size_t c = 0; c < in->n_cols; c++) {
        if (!is_value[c]) {
            id_cols[oc] = c;
            if (tf_batch_set_schema(ob, oc++, in->col_names[c], in->col_types[c]) != TF_OK) goto fail;
        }
    }
    if (tf_batch_set_schema(ob, oc, "variable", TF_TYPE_STRING) != TF_OK ||
        tf_batch_set_schema(ob, oc + 1, "value", TF_TYPE_STRING) != TF_OK) goto fail;

    size_t out_row = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        for (size_t c = 0; c < in->n_cols; c++) {
            if (!is_value[c]) continue;

            if (out_row >= st->max_output_rows_per_batch) {
                unpivot_set_cap_error("max_output_rows_per_batch", st->max_output_rows_per_batch);
                goto fail;
            }
            size_t need_rows = 0;
            if (tf_size_add(out_row, 1, &need_rows) != TF_OK ||
                tf_batch_ensure_capacity(ob, need_rows) != TF_OK) goto fail;

            /* Copy id columns */
            if (n_id > 0 &&
                tf_batch_copy_selected_row(ob, out_row, in, r, id_cols, n_id) != TF_OK) goto fail;

            /* Set variable name */
            if (tf_batch_set_string(ob, out_row, n_id, in->col_names[c]) != TF_OK) goto fail;

            if (tf_batch_copy_cell_as_string(ob, out_row, n_id + 1, in, r, c) != TF_OK)
                goto fail;

            if (tf_batch_expose_row(ob, out_row) != TF_OK) goto fail;
            out_row++;
        }
    }

    free(id_cols);
    free(is_value);
    if (out_row > 0) *out = ob;
    else tf_batch_free(ob);
    return TF_OK;

fail:
    free(id_cols);
    free(is_value);
    tf_batch_free(ob);
    return TF_ERROR;
}

static int unpivot_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void unpivot_state_free(unpivot_state *st) {
    if (st) {
        if (st->cols) {
            for (size_t i = 0; i < st->n_cols; i++) free(st->cols[i]);
        }
        free(st->cols); free(st);
    }
}

static void unpivot_destroy(tf_step *self) {
    unpivot_state_free(self->state);
    free(self);
}

tf_step *tf_unpivot_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *columns = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!columns || !cJSON_IsArray(columns)) {
        tf_set_last_error("unpivot: columns must be a non-empty array");
        return NULL;
    }

    int n = cJSON_GetArraySize(columns);
    if (n <= 0) {
        tf_set_last_error("unpivot: columns must be a non-empty array");
        return NULL;
    }

    unpivot_state *st = calloc(1, sizeof(unpivot_state));
    if (!st) return NULL;
    st->max_output_rows_per_input_row = TF_MAX_EXPANDING_ROWS_PER_INPUT;
    st->max_output_rows_per_batch = TF_MAX_OUTPUT_ROWS_PER_BATCH;

    size_t parsed_size = 0;
    int has_size = tf_json_get_size_arg(args, "max_output_rows_per_input_row", 1, TF_MAX_EXPANDING_ROWS_PER_INPUT, &parsed_size, "unpivot");
    if (has_size < 0) { free(st); return NULL; }
    if (has_size > 0) st->max_output_rows_per_input_row = parsed_size;
    has_size = tf_json_get_size_arg(args, "max_output_rows_per_batch", 1, TF_MAX_OUTPUT_ROWS_PER_BATCH, &parsed_size, "unpivot");
    if (has_size < 0) { free(st); return NULL; }
    if (has_size > 0) st->max_output_rows_per_batch = parsed_size;

    st->cols = tf_callocarray_checked((size_t)n, sizeof(char *));
    if (!st->cols) { free(st); return NULL; }
    st->n_cols = (size_t)n;
    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(columns, i);
        if (!cJSON_IsString(item) || !item->valuestring || item->valuestring[0] == '\0') {
            tf_set_last_error("unpivot: column names must be non-empty strings");
            unpivot_state_free(st);
            return NULL;
        }
        st->cols[i] = strdup(item->valuestring);
        if (!st->cols[i]) { unpivot_state_free(st); return NULL; }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { unpivot_state_free(st); return NULL; }
    step->process = unpivot_process;
    step->flush = unpivot_flush;
    step->destroy = unpivot_destroy;
    step->state = st;
    return step;
}
