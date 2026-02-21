/*
 * op_validate.c — Like filter but adds _valid bool column, keeps all rows.
 *
 * Config: {"expr": "col('age') > 0"}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    tf_expr *expr;
} validate_state;

static int validate_process(tf_step *self, tf_batch *in, tf_batch **out,
                            tf_side_channels *side) {
    (void)side;
    validate_state *st = self->state;
    *out = NULL;

    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    tf_batch_set_schema(ob, in->n_cols, "_valid", TF_TYPE_BOOL);

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);
        bool valid = false;
        tf_expr_eval(st->expr, in, r, &valid);
        tf_batch_set_bool(ob, r, in->n_cols, valid);
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int validate_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static void validate_destroy(tf_step *self) {
    validate_state *st = self->state;
    if (st) { tf_expr_free(st->expr); free(st); }
    free(self);
}

tf_step *tf_validate_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *expr_json = cJSON_GetObjectItemCaseSensitive(args, "expr");
    if (!cJSON_IsString(expr_json)) return NULL;

    tf_expr *expr = tf_expr_parse(expr_json->valuestring);
    if (!expr) return NULL;

    validate_state *st = calloc(1, sizeof(validate_state));
    if (!st) { tf_expr_free(expr); return NULL; }
    st->expr = expr;

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { tf_expr_free(expr); free(st); return NULL; }
    step->process = validate_process;
    step->flush = validate_flush;
    step->destroy = validate_destroy;
    step->state = st;
    return step;
}
