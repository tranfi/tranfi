/*
 * op_grep.c — Substring/regex filter.
 *
 * Config: {"pattern": "error", "invert": false, "column": "_line", "regex": false}
 * For each row, checks if column value matches pattern.
 * Uses strstr for substring mode, regexec for regex mode.
 * Keeps or discards based on invert flag.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <regex.h>

typedef struct {
    char   *pattern;
    char   *column;
    int     invert;
    int     use_regex;
    regex_t compiled;
    size_t  rows_in;
    size_t  rows_out;
} grep_state;

static int grep_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    grep_state *st = self->state;
    *out = NULL;

    /* Find target column */
    int col_idx = tf_batch_col_index(in, st->column);
    if (col_idx < 0) {
        /* Column not found — pass through or drop all depending on invert */
        if (st->invert) {
            tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
            if (!ob) return TF_ERROR;
            if (tf_batch_clone_schema(ob, in) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            for (size_t r = 0; r < in->n_rows; r++) {
                if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                if (tf_batch_expose_row(ob, r) != TF_OK) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
            }
            *out = ob;
        }
        return TF_OK;
    }

    /* Create output batch with same schema */
    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_schema(ob, in) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    size_t out_row = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        int match = 0;

        if (tf_batch_is_null(in, r, (size_t)col_idx)) {
            match = 0;
        } else if (in->col_types[(size_t)col_idx] == TF_TYPE_STRING) {
            const char *val = tf_batch_get_string(in, r, (size_t)col_idx);
            if (st->use_regex) {
                match = (regexec(&st->compiled, val, 0, NULL, 0) == 0) ? 1 : 0;
            } else {
                match = (strstr(val, st->pattern) != NULL) ? 1 : 0;
            }
        }

        int keep = st->invert ? !match : match;
        if (!keep) continue;

        if (tf_batch_ensure_capacity(ob, out_row + 1) != TF_OK) {
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
    }

    st->rows_in += in->n_rows;
    st->rows_out += out_row;

    if (side && side->stats) {
        char stats_buf[128];
        snprintf(stats_buf, sizeof(stats_buf),
                 "{\"op\":\"grep\",\"rows_in\":%zu,\"rows_out\":%zu}",
                 in->n_rows, out_row);
        if (tf_buffer_write_line(side->stats, stats_buf) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    if (out_row > 0) {
        *out = ob;
    } else {
        tf_batch_free(ob);
    }
    return TF_OK;
}

static int grep_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static void grep_destroy(tf_step *self) {
    grep_state *st = self->state;
    if (st) {
        if (st->use_regex) regfree(&st->compiled);
        free(st->pattern);
        free(st->column);
        free(st);
    }
    free(self);
}

tf_step *tf_grep_create(const cJSON *args) {
    if (!args) return NULL;

    cJSON *pattern_json = cJSON_GetObjectItemCaseSensitive(args, "pattern");
    if (!cJSON_IsString(pattern_json)) return NULL;

    grep_state *st = calloc(1, sizeof(grep_state));
    if (!st) return NULL;

    st->pattern = strdup(pattern_json->valuestring);
    if (!st->pattern) { free(st); return NULL; }

    cJSON *column_json = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (cJSON_IsString(column_json))
        st->column = strdup(column_json->valuestring);
    else
        st->column = strdup("_line");
    if (!st->column) { free(st->pattern); free(st); return NULL; }

    cJSON *invert_json = cJSON_GetObjectItemCaseSensitive(args, "invert");
    st->invert = (cJSON_IsBool(invert_json) && cJSON_IsTrue(invert_json)) ? 1 : 0;

    cJSON *regex_json = cJSON_GetObjectItemCaseSensitive(args, "regex");
    st->use_regex = (cJSON_IsBool(regex_json) && cJSON_IsTrue(regex_json)) ? 1 : 0;

    if (st->use_regex) {
        int rc = regcomp(&st->compiled, st->pattern, REG_EXTENDED | REG_NOSUB);
        if (rc != 0) {
            free(st->pattern);
            free(st->column);
            free(st);
            return NULL;
        }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st->pattern); free(st->column); free(st); return NULL; }
    step->process = grep_process;
    step->flush = grep_flush;
    step->destroy = grep_destroy;
    step->state = st;
    return step;
}
