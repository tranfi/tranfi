/*
 * op_explode.c -- Split delimited string into multiple rows.
 *
 * Config: {"column": "tags", "delimiter": ",", "max_tokens_per_row": 1024}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char *column;
    char *delimiter;
    size_t max_tokens_per_row;
    size_t max_output_rows_per_input_row;
    size_t max_output_rows_per_batch;
    size_t max_token_bytes;
} explode_state;

static int explode_set_cap_error(const char *name, size_t limit) {
    char msg[160];
    snprintf(msg, sizeof(msg), "explode: %s=%zu exceeded", name, limit);
    tf_set_last_error(msg);
    return TF_ERROR;
}

static int explode_process(tf_step *self, tf_batch *in, tf_batch **out,
                           tf_side_channels *side) {
    (void)side;
    explode_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);

    size_t initial_cap = in->n_rows ? in->n_rows : 1;
    if (initial_cap > st->max_output_rows_per_batch)
        initial_cap = st->max_output_rows_per_batch;
    if (initial_cap < 16 && st->max_output_rows_per_batch >= 16)
        initial_cap = 16;
    tf_batch *ob = tf_batch_create(in->n_cols, initial_cap);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_schema(ob, in) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    size_t out_row = 0;
    size_t delim_len = strlen(st->delimiter);

    for (size_t r = 0; r < in->n_rows; r++) {
        if (ci < 0 || tf_batch_is_null(in, r, ci) || in->col_types[ci] != TF_TYPE_STRING) {
            if (out_row >= st->max_output_rows_per_batch) {
                explode_set_cap_error("max_output_rows_per_batch", st->max_output_rows_per_batch);
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (tf_batch_copy_row(ob, out_row, in, r) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (tf_batch_expose_row(ob, out_row) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            out_row++;
            continue;
        }

        const char *val = tf_batch_get_string(in, r, ci);
        const char *p = val;
        size_t row_outputs = 0;

        while (*p) {
            if (row_outputs >= st->max_tokens_per_row) {
                explode_set_cap_error("max_tokens_per_row", st->max_tokens_per_row);
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (row_outputs >= st->max_output_rows_per_input_row) {
                explode_set_cap_error("max_output_rows_per_input_row", st->max_output_rows_per_input_row);
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (out_row >= st->max_output_rows_per_batch) {
                explode_set_cap_error("max_output_rows_per_batch", st->max_output_rows_per_batch);
                tf_batch_free(ob);
                return TF_ERROR;
            }

            const char *found = strstr(p, st->delimiter);
            size_t tok_len = found ? (size_t)(found - p) : strlen(p);
            if (tok_len > st->max_token_bytes) {
                explode_set_cap_error("max_token_bytes", st->max_token_bytes);
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (tf_batch_copy_row(ob, out_row, in, r) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            /* Override the exploded column */
            size_t tok_cap = 0;
            if (tf_size_add(tok_len, 1, &tok_cap) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            char *tok = malloc(tok_cap);
            if (!tok) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            memcpy(tok, p, tok_len);
            tok[tok_len] = '\0';
            /* Trim leading/trailing whitespace */
            char *s = tok;
            while (*s == ' ') s++;
            char *e = s + strlen(s);
            while (e > s && *(e - 1) == ' ') e--;
            *e = '\0';
            if (tf_batch_set_string(ob, out_row, (size_t)ci, s) != TF_OK) {
                free(tok);
                tf_batch_free(ob);
                return TF_ERROR;
            }
            free(tok);
            if (tf_batch_expose_row(ob, out_row) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            out_row++;
            row_outputs++;

            if (found) p = found + delim_len;
            else break;
        }
    }

    if (out_row > 0) {
        *out = ob;
    } else {
        tf_batch_free(ob);
    }
    return TF_OK;
}

static int explode_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void explode_destroy(tf_step *self) {
    explode_state *st = self->state;
    if (st) { free(st->column); free(st->delimiter); free(st); }
    free(self);
}

tf_step *tf_explode_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j)) return NULL;

    explode_state *st = calloc(1, sizeof(explode_state));
    if (!st) return NULL;
    st->max_tokens_per_row = TF_MAX_EXPLODE_TOKENS_PER_ROW;
    st->max_output_rows_per_input_row = TF_MAX_EXPANDING_ROWS_PER_INPUT;
    st->max_output_rows_per_batch = TF_MAX_OUTPUT_ROWS_PER_BATCH;
    st->max_token_bytes = TF_MAX_RECORD_BYTES;

    size_t parsed_size = 0;
    int has_size = tf_json_get_size_arg(args, "max_tokens_per_row", 1, TF_MAX_EXPLODE_TOKENS_PER_ROW, &parsed_size, "explode");
    if (has_size < 0) goto fail;
    if (has_size > 0) st->max_tokens_per_row = parsed_size;
    has_size = tf_json_get_size_arg(args, "max_output_rows_per_input_row", 1, TF_MAX_EXPANDING_ROWS_PER_INPUT, &parsed_size, "explode");
    if (has_size < 0) goto fail;
    if (has_size > 0) st->max_output_rows_per_input_row = parsed_size;
    has_size = tf_json_get_size_arg(args, "max_output_rows_per_batch", 1, TF_MAX_OUTPUT_ROWS_PER_BATCH, &parsed_size, "explode");
    if (has_size < 0) goto fail;
    if (has_size > 0) st->max_output_rows_per_batch = parsed_size;
    has_size = tf_json_get_size_arg(args, "max_token_bytes", 0, TF_MAX_RECORD_BYTES, &parsed_size, "explode");
    if (has_size < 0) goto fail;
    if (has_size > 0) st->max_token_bytes = parsed_size;

    st->column = strdup(col_j->valuestring);
    if (!st->column) goto fail;

    cJSON *delim_j = cJSON_GetObjectItemCaseSensitive(args, "delimiter");
    const char *delim = cJSON_IsString(delim_j) ? delim_j->valuestring : ",";
    if (delim[0] == '\0') goto fail;
    st->delimiter = strdup(delim);
    if (!st->delimiter) goto fail;

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) goto fail;
    step->process = explode_process;
    step->flush = explode_flush;
    step->destroy = explode_destroy;
    step->state = st;
    return step;

fail:
    free(st->column);
    free(st->delimiter);
    free(st);
    return NULL;
}
