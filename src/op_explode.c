/*
 * op_explode.c — Split delimited string into multiple rows.
 *
 * Config: {"column": "tags", "delimiter": ","}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    char *column;
    char *delimiter;
} explode_state;

static int explode_process(tf_step *self, tf_batch *in, tf_batch **out,
                           tf_side_channels *side) {
    (void)side;
    explode_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);

    /* Estimate max output rows */
    size_t max_out = in->n_rows * 4;
    tf_batch *ob = tf_batch_create(in->n_cols, max_out > 0 ? max_out : 16);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);

    size_t out_row = 0;
    size_t delim_len = strlen(st->delimiter);

    for (size_t r = 0; r < in->n_rows; r++) {
        if (ci < 0 || tf_batch_is_null(in, r, ci) || in->col_types[ci] != TF_TYPE_STRING) {
            tf_batch_copy_row(ob, out_row, in, r);
            ob->n_rows = ++out_row;
            continue;
        }

        const char *val = tf_batch_get_string(in, r, ci);
        const char *p = val;

        while (*p) {
            const char *found = strstr(p, st->delimiter);
            size_t tok_len = found ? (size_t)(found - p) : strlen(p);

            tf_batch_copy_row(ob, out_row, in, r);
            /* Override the exploded column */
            char *tok = malloc(tok_len + 1);
            if (tok) {
                memcpy(tok, p, tok_len);
                tok[tok_len] = '\0';
                /* Trim leading/trailing whitespace */
                char *s = tok;
                while (*s == ' ') s++;
                char *e = s + strlen(s);
                while (e > s && *(e - 1) == ' ') e--;
                *e = '\0';
                tf_batch_set_string(ob, out_row, ci, s);
                free(tok);
            }
            ob->n_rows = ++out_row;

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
    st->column = strdup(col_j->valuestring);

    cJSON *delim_j = cJSON_GetObjectItemCaseSensitive(args, "delimiter");
    st->delimiter = strdup(cJSON_IsString(delim_j) ? delim_j->valuestring : ",");

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->column); free(st->delimiter); free(st); return NULL; }
    step->process = explode_process;
    step->flush = explode_flush;
    step->destroy = explode_destroy;
    step->state = st;
    return step;
}
