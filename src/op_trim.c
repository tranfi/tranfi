/*
 * op_trim.c — Trim whitespace from string columns.
 *
 * Config: {"columns": ["name", "city"]}
 *   or {} for all string columns.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

typedef struct {
    char  **cols;
    size_t  n_cols;
} trim_state;

static const char *trim_str(const char *s, char *buf, size_t buf_sz) {
    while (*s && isspace((unsigned char)*s)) s++;
    size_t len = strlen(s);
    while (len > 0 && isspace((unsigned char)s[len - 1])) len--;
    if (len >= buf_sz) len = buf_sz - 1;
    memcpy(buf, s, len);
    buf[len] = '\0';
    return buf;
}

static int trim_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    trim_state *st = self->state;
    *out = NULL;

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);
        ob->n_rows = r + 1;
    }

    /* Trim target columns */
    for (size_t c = 0; c < in->n_cols; c++) {
        if (in->col_types[c] != TF_TYPE_STRING) continue;
        int target = 0;
        if (st->n_cols == 0) {
            target = 1; /* all string cols */
        } else {
            for (size_t k = 0; k < st->n_cols; k++) {
                if (strcmp(in->col_names[c], st->cols[k]) == 0) { target = 1; break; }
            }
        }
        if (!target) continue;
        char buf[4096];
        for (size_t r = 0; r < ob->n_rows; r++) {
            if (tf_batch_is_null(ob, r, c)) continue;
            const char *val = tf_batch_get_string(ob, r, c);
            tf_batch_set_string(ob, r, c, trim_str(val, buf, sizeof(buf)));
        }
    }

    *out = ob;
    return TF_OK;
}

static int trim_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void trim_destroy(tf_step *self) {
    trim_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_cols; i++) free(st->cols[i]);
        free(st->cols);
        free(st);
    }
    free(self);
}

tf_step *tf_trim_create(const cJSON *args) {
    trim_state *st = calloc(1, sizeof(trim_state));
    if (!st) return NULL;

    if (args) {
        cJSON *columns = cJSON_GetObjectItemCaseSensitive(args, "columns");
        if (columns && cJSON_IsArray(columns)) {
            int n = cJSON_GetArraySize(columns);
            if (n > 0) {
                st->cols = calloc(n, sizeof(char *));
                st->n_cols = n;
                for (int i = 0; i < n; i++) {
                    cJSON *item = cJSON_GetArrayItem(columns, i);
                    if (cJSON_IsString(item)) st->cols[i] = strdup(item->valuestring);
                }
            }
        }
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st); return NULL; }
    step->process = trim_process;
    step->flush = trim_flush;
    step->destroy = trim_destroy;
    step->state = st;
    return step;
}
