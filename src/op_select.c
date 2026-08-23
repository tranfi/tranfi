/*
 * op_select.c — Select and reorder columns.
 *
 * Config: {"columns": ["name", "age"]}
 * Creates output batch with only the specified columns in the given order.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char  **col_names;
    size_t  n_cols;
    int     use_selector_syntax;
} select_state;

static int select_write_error(tf_side_channels *side, const char *msg) {
    if (!side || !side->errors || !msg) return TF_OK;
    char buf[256];
    snprintf(buf, sizeof(buf), "{\"op\":\"select\",\"error\":\"%s\"}", msg);
    return tf_buffer_write_line(side->errors, buf);
}

static int select_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    select_state *st = self->state;
    *out = NULL;

    int *indices = NULL;
    size_t n_indices = st->n_cols;

    if (st->use_selector_syntax) {
        char *error = NULL;
        if (tf_column_selectors_resolve(st->col_names, st->n_cols,
                                        in->col_names, in->col_types, in->n_cols,
                                        &indices, &n_indices, &error) != TF_OK) {
            int err_rc = select_write_error(side, error ? error : "selector resolution failed");
            free(error);
            if (err_rc != TF_OK) return TF_ERROR;
            return TF_ERROR;
        }
    } else {
        indices = malloc(st->n_cols * sizeof(int));
        if (!indices) return TF_ERROR;
        for (size_t i = 0; i < st->n_cols; i++) {
            indices[i] = tf_batch_col_index(in, st->col_names[i]);
            if (indices[i] < 0 && side && side->errors) {
                char msg[128];
                snprintf(msg, sizeof(msg), "column '%s' not found", st->col_names[i]);
                if (select_write_error(side, msg) != TF_OK) {
                    free(indices);
                    return TF_ERROR;
                }
            }
        }
    }

    tf_batch *ob = tf_batch_create(n_indices, in->n_rows);
    if (!ob) { free(indices); return TF_ERROR; }

    size_t *selected_cols = malloc(n_indices * sizeof(size_t));
    if (!selected_cols) { free(indices); tf_batch_free(ob); return TF_ERROR; }
    for (size_t i = 0; i < n_indices; i++) {
        int ci = indices[i];
        if (ci >= 0) {
            selected_cols[i] = (size_t)ci;
            if (tf_batch_set_schema(ob, i, in->col_names[(size_t)ci], in->col_types[(size_t)ci]) != TF_OK) {
                free(selected_cols); free(indices); tf_batch_free(ob); return TF_ERROR;
            }
        } else {
            selected_cols[i] = SIZE_MAX;
            if (tf_batch_set_schema(ob, i, st->col_names[i], TF_TYPE_NULL) != TF_OK) {
                free(selected_cols); free(indices); tf_batch_free(ob); return TF_ERROR;
            }
        }
    }

    if (tf_batch_copy_selected_columns(ob, in, selected_cols, n_indices) != TF_OK) {
        free(selected_cols); free(indices); tf_batch_free(ob); return TF_ERROR;
    }

    free(selected_cols);
    free(indices);
    *out = ob;
    return TF_OK;
}

static int select_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static void select_state_free(select_state *st) {
    if (st) {
        if (st->col_names) {
            for (size_t i = 0; i < st->n_cols; i++) free(st->col_names[i]);
        }
        free(st->col_names);
        free(st);
    }
}

static void select_destroy(tf_step *self) {
    if (!self) return;
    select_state_free(self->state);
    free(self);
}

tf_step *tf_select_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!cJSON_IsArray(cols)) return NULL;

    int n = cJSON_GetArraySize(cols);
    if (n <= 0) return NULL;

    select_state *st = calloc(1, sizeof(select_state));
    if (!st) return NULL;
    st->n_cols = (size_t)n;
    st->col_names = calloc((size_t)n, sizeof(char *));
    if (!st->col_names) { select_state_free(st); return NULL; }

    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(cols, i);
        if (!cJSON_IsString(item)) { select_state_free(st); return NULL; }
        st->col_names[i] = strdup(item->valuestring);
        if (!st->col_names[i]) { select_state_free(st); return NULL; }
        if (tf_column_selector_has_syntax(item->valuestring)) st->use_selector_syntax = 1;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { select_state_free(st); return NULL; }
    step->process = select_process;
    step->flush = select_flush;
    step->destroy = select_destroy;
    step->state = st;
    return step;
}


typedef struct {
    char  **move_names;
    size_t  n_move;
    char   *before;
    char   *after;
} relocate_state;

static int relocate_write_error(tf_side_channels *side, const char *msg, const char *name) {
    if (!side || !side->errors) return TF_OK;
    char buf[256];
    snprintf(buf, sizeof(buf),
             "{\"op\":\"relocate\",\"error\":\"%s '%s'\"}",
             msg, name ? name : "");
    return tf_buffer_write_line(side->errors, buf);
}

static int relocate_process(tf_step *self, tf_batch *in, tf_batch **out,
                            tf_side_channels *side) {
    relocate_state *st = self->state;
    *out = NULL;

    size_t n_in = in->n_cols;
    int *move_idx = NULL;
    size_t n_move = 0;
    char *selector_error = NULL;
    if (tf_column_selectors_resolve(st->move_names, st->n_move,
                                    in->col_names, in->col_types, in->n_cols,
                                    &move_idx, &n_move, &selector_error) != TF_OK) {
        int err_rc = relocate_write_error(side, selector_error ? selector_error : "selector resolution failed", "");
        free(selector_error);
        if (err_rc != TF_OK) return TF_ERROR;
        return TF_ERROR;
    }

    int *is_moving = calloc(n_in ? n_in : 1, sizeof(int));
    int *order = malloc(n_in ? n_in * sizeof(int) : sizeof(int));
    if (!is_moving || !order) {
        free(move_idx); free(is_moving); free(order);
        return TF_ERROR;
    }

    for (size_t i = 0; i < n_move; i++) {
        int ci = move_idx[i];
        if (ci < 0 || (size_t)ci >= n_in || is_moving[ci]) {
            int err_rc = relocate_write_error(side, "invalid relocated column", "");
            free(move_idx); free(is_moving); free(order);
            if (err_rc != TF_OK) return TF_ERROR;
            return TF_ERROR;
        }
        is_moving[ci] = 1;
    }

    const char *anchor = st->before ? st->before : st->after;
    int anchor_idx = -1;
    if (anchor) {
        anchor_idx = tf_batch_col_index(in, anchor);
        if (anchor_idx < 0) {
            int err_rc = relocate_write_error(side, "anchor column not found", anchor);
            free(move_idx); free(is_moving); free(order);
            if (err_rc != TF_OK) return TF_ERROR;
            return TF_ERROR;
        }
        if (is_moving[anchor_idx]) {
            int err_rc = relocate_write_error(side, "anchor column is being relocated", anchor);
            free(move_idx); free(is_moving); free(order);
            if (err_rc != TF_OK) return TF_ERROR;
            return TF_ERROR;
        }
    }

    size_t n_order = 0;
    if (!anchor) {
        for (size_t i = 0; i < n_move; i++) order[n_order++] = move_idx[i];
        for (size_t i = 0; i < n_in; i++) {
            if (!is_moving[i]) order[n_order++] = (int)i;
        }
    } else {
        for (size_t i = 0; i < n_in; i++) {
            if (is_moving[i]) continue;
            if (st->before && (int)i == anchor_idx) {
                for (size_t j = 0; j < n_move; j++) order[n_order++] = move_idx[j];
            }
            order[n_order++] = (int)i;
            if (st->after && (int)i == anchor_idx) {
                for (size_t j = 0; j < n_move; j++) order[n_order++] = move_idx[j];
            }
        }
    }

    if (n_order != n_in) {
        int err_rc = relocate_write_error(side, "internal order size mismatch", "");
        free(move_idx); free(is_moving); free(order);
        if (err_rc != TF_OK) return TF_ERROR;
        return TF_ERROR;
    }

    tf_batch *ob = tf_batch_create(n_in, in->n_rows);
    if (!ob) {
        free(move_idx); free(is_moving); free(order);
        return TF_ERROR;
    }

    size_t *selected_cols = malloc(n_in * sizeof(size_t));
    if (!selected_cols) {
        free(move_idx); free(is_moving); free(order); tf_batch_free(ob);
        return TF_ERROR;
    }
    for (size_t i = 0; i < n_in; i++) {
        int ci = order[i];
        selected_cols[i] = (size_t)ci;
        if (tf_batch_set_schema(ob, i, in->col_names[(size_t)ci], in->col_types[(size_t)ci]) != TF_OK) {
            free(selected_cols); free(move_idx); free(is_moving); free(order); tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    if (tf_batch_copy_selected_columns(ob, in, selected_cols, n_in) != TF_OK) {
        free(selected_cols); free(move_idx); free(is_moving); free(order); tf_batch_free(ob);
        return TF_ERROR;
    }

    free(selected_cols);
    free(move_idx);
    free(is_moving);
    free(order);
    *out = ob;
    return TF_OK;
}

static int relocate_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static void relocate_state_free(relocate_state *st) {
    if (st) {
        if (st->move_names) {
            for (size_t i = 0; i < st->n_move; i++) free(st->move_names[i]);
        }
        free(st->move_names);
        free(st->before);
        free(st->after);
        free(st);
    }
}

static void relocate_destroy(tf_step *self) {
    if (!self) return;
    relocate_state_free(self->state);
    free(self);
}

tf_step *tf_relocate_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!cJSON_IsArray(cols)) return NULL;

    cJSON *before = cJSON_GetObjectItemCaseSensitive(args, "before");
    cJSON *after = cJSON_GetObjectItemCaseSensitive(args, "after");
    if (before && after) return NULL;
    if (before && !cJSON_IsString(before)) return NULL;
    if (after && !cJSON_IsString(after)) return NULL;

    int n = cJSON_GetArraySize(cols);
    if (n <= 0) return NULL;

    relocate_state *st = calloc(1, sizeof(relocate_state));
    if (!st) return NULL;
    st->n_move = (size_t)n;
    st->move_names = calloc((size_t)n, sizeof(char *));
    if (!st->move_names) { relocate_state_free(st); return NULL; }

    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(cols, i);
        if (!cJSON_IsString(item)) { relocate_state_free(st); return NULL; }
        st->move_names[i] = strdup(item->valuestring);
        if (!st->move_names[i]) { relocate_state_free(st); return NULL; }
    }
    if (before) {
        st->before = strdup(before->valuestring);
        if (!st->before) { relocate_state_free(st); return NULL; }
    }
    if (after) {
        st->after = strdup(after->valuestring);
        if (!st->after) { relocate_state_free(st); return NULL; }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { relocate_state_free(st); return NULL; }
    step->process = relocate_process;
    step->flush = relocate_flush;
    step->destroy = relocate_destroy;
    step->state = st;
    return step;
}
