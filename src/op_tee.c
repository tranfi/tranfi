/*
 * op_tee.c -- Streaming side-channel row snapshots.
 *
 * Config: {
 *   "expr": "optional row predicate",
 *   "channel": "samples|errors|stats",
 *   "columns": ["optional", "selectors"],
 *   "limit": 1000,
 *   "every": 1,
 *   "name": "tee",
 *   "include_row": true
 * }
 */

#include "internal.h"
#include "cJSON.h"
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    tf_expr *expr;
    char    *expr_text;
    char    *name;
    char    *channel_name;
    int      channel;
    char   **selectors;
    size_t   n_selectors;
    int     *indices;
    size_t   n_indices;
    int      resolved;
    size_t   limit;
    size_t   every;
    size_t   row_index;
    size_t   emitted;
    int      include_row;
    tf_audit_options audit_opts;
} tee_state;

static int parse_channel(const char *s, int *out, const char **name_out) {
    if (!s || !*s || strcmp(s, "samples") == 0 || strcmp(s, "sample") == 0) {
        *out = TF_CHAN_SAMPLES;
        *name_out = "samples";
        return 1;
    }
    if (strcmp(s, "errors") == 0 || strcmp(s, "error") == 0) {
        *out = TF_CHAN_ERRORS;
        *name_out = "errors";
        return 1;
    }
    if (strcmp(s, "stats") == 0 || strcmp(s, "audit") == 0) {
        *out = TF_CHAN_STATS;
        *name_out = strcmp(s, "audit") == 0 ? "audit" : "stats";
        return 1;
    }
    return 0;
}

static tf_buffer *tee_side_buffer(tf_side_channels *side, int channel) {
    if (!side) return NULL;
    switch (channel) {
        case TF_CHAN_ERRORS: return side->errors;
        case TF_CHAN_STATS: return side->stats;
        case TF_CHAN_SAMPLES: return side->samples;
        default: return NULL;
    }
}

static int tee_audit_column_allowed(const tee_state *st, const char *name) {
    if (!st || st->audit_opts.n_columns == 0) return 1;
    for (size_t i = 0; i < st->audit_opts.n_columns; i++) {
        if (st->audit_opts.columns[i] && strcmp(st->audit_opts.columns[i], name ? name : "") == 0)
            return 1;
    }
    return 0;
}

static cJSON *tee_selected_row_to_json(const tee_state *st, const tf_batch *in, size_t row) {
    cJSON *data = cJSON_CreateObject();
    if (!data) return NULL;
    for (size_t i = 0; i < st->n_indices; i++) {
        int ci = st->indices[i];
        if (ci < 0 || (size_t)ci >= in->n_cols) continue;
        const char *name = in->col_names[ci] ? in->col_names[ci] : "";
        if (!tee_audit_column_allowed(st, name)) continue;
        cJSON *value = tf_audit_cell_to_json(in, row, (size_t)ci, &st->audit_opts);
        if (!value) { cJSON_Delete(data); return NULL; }
        cJSON_AddItemToObject(data, name, value);
    }
    if (st->audit_opts.max_bytes > 0) {
        char *printed = cJSON_PrintUnformatted(data);
        if (!printed) { cJSON_Delete(data); return NULL; }
        size_t n = strlen(printed);
        free(printed);
        if (n > st->audit_opts.max_bytes) {
            cJSON_Delete(data);
            data = cJSON_CreateObject();
            if (!data) return NULL;
            cJSON_AddBoolToObject(data, "_audit_truncated", 1);
            cJSON_AddNumberToObject(data, "max_bytes", (double)st->audit_opts.max_bytes);
        }
    }
    return data;
}

static int tee_resolve_columns(tee_state *st, const tf_batch *in) {
    if (st->resolved) return TF_OK;
    free(st->indices);
    st->indices = NULL;
    st->n_indices = 0;

    if (st->n_selectors == 0) {
        st->indices = malloc((in->n_cols ? in->n_cols : 1) * sizeof(int));
        if (!st->indices) return TF_ERROR;
        for (size_t i = 0; i < in->n_cols; i++) st->indices[i] = (int)i;
        st->n_indices = in->n_cols;
        st->resolved = 1;
        return TF_OK;
    }

    char *error = NULL;
    if (tf_column_selectors_resolve(st->selectors, st->n_selectors,
                                    in->col_names, in->col_types, in->n_cols,
                                    &st->indices, &st->n_indices, &error) != TF_OK) {
        char msg[512];
        snprintf(msg, sizeof(msg), "tee: %s", error ? error : "invalid columns");
        tf_set_last_error(msg);
        free(error);
        return TF_ERROR;
    }
    free(error);
    st->resolved = 1;
    return TF_OK;
}

static int tee_emit_row(tee_state *st, const tf_batch *in, size_t row,
                        tf_side_channels *side) {
    tf_buffer *buf = tee_side_buffer(side, st->channel);
    if (!buf) return TF_OK;

    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    cJSON_AddStringToObject(obj, "type", "tee");
    cJSON_AddStringToObject(obj, "op", "tee");
    cJSON_AddStringToObject(obj, "name", st->name ? st->name : "tee");
    cJSON_AddStringToObject(obj, "channel", st->channel_name ? st->channel_name : "samples");
    if (st->expr_text && st->expr_text[0]) cJSON_AddStringToObject(obj, "expr", st->expr_text);
    if (st->include_row) cJSON_AddNumberToObject(obj, "row", (double)st->row_index);

    if (st->audit_opts.include_row) {
        cJSON *data = tee_selected_row_to_json(st, in, row);
        if (!data) { cJSON_Delete(obj); return TF_ERROR; }
        cJSON_AddItemToObject(obj, "data", data);
    }

    int rc = tf_buffer_write_json_line(buf, obj);
    cJSON_Delete(obj);
    return rc;
}

static int tee_process(tf_step *self, tf_batch *in, tf_batch **out,
                       tf_side_channels *side) {
    tee_state *st = self->state;
    *out = NULL;

    if (tee_resolve_columns(st, in) != TF_OK) return TF_ERROR;

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_schema(ob, in) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }

    for (size_t r = 0; r < in->n_rows; r++) {
        st->row_index++;
        bool should_emit = true;
        if (st->expr) {
            if (tf_expr_eval(st->expr, in, r, &should_emit) != TF_OK) should_emit = false;
        }
        if (should_emit && st->every > 1 && ((st->row_index - 1) % st->every) != 0)
            should_emit = false;
        if (should_emit && st->emitted < st->limit) {
            if (tee_emit_row(st, in, r, side) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            st->emitted++;
        }
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
        if (tf_batch_expose_row(ob, r) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
    }

    *out = ob;
    return TF_OK;
}

static int tee_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static void tee_state_free(tee_state *st) {
    if (!st) return;
    tf_expr_free(st->expr);
    free(st->expr_text);
    free(st->name);
    free(st->channel_name);
    tf_audit_options_free(&st->audit_opts);
    for (size_t i = 0; i < st->n_selectors; i++) free(st->selectors[i]);
    free(st->selectors);
    free(st->indices);
    free(st);
}

static void tee_destroy(tf_step *self) {
    if (!self) return;
    tee_state_free(self->state);
    free(self);
}

static int tee_copy_columns_arg(tee_state *st, const cJSON *cols) {
    if (!cols) return TF_OK;
    if (!cJSON_IsArray(cols)) {
        tf_set_last_error("tee: columns must be an array");
        return TF_ERROR;
    }
    int n = cJSON_GetArraySize((cJSON *)cols);
    if (n <= 0) return TF_OK;
    st->selectors = calloc((size_t)n, sizeof(char *));
    if (!st->selectors) return TF_ERROR;
    st->n_selectors = (size_t)n;
    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem((cJSON *)cols, i);
        if (!cJSON_IsString(item) || !item->valuestring[0]) {
            tf_set_last_error("tee: columns must contain non-empty strings");
            return TF_ERROR;
        }
        st->selectors[i] = strdup(item->valuestring);
        if (!st->selectors[i]) return TF_ERROR;
    }
    return TF_OK;
}

tf_step *tf_tee_create(const cJSON *args) {
    if (!args) return NULL;

    tee_state *st = calloc(1, sizeof(tee_state));
    if (!st) return NULL;
    st->limit = 1000;
    st->every = 1;
    st->include_row = 1;
    tf_audit_options_init(&st->audit_opts, 1);

    cJSON *expr_json = cJSON_GetObjectItemCaseSensitive(args, "expr");
    if (cJSON_IsString(expr_json) && expr_json->valuestring[0]) {
        st->expr = tf_expr_parse(expr_json->valuestring);
        if (!st->expr) { tee_state_free(st); return NULL; }
        st->expr_text = strdup(expr_json->valuestring);
        if (!st->expr_text) { tee_state_free(st); return NULL; }
    }

    cJSON *channel_json = cJSON_GetObjectItemCaseSensitive(args, "channel");
    const char *channel_name = NULL;
    if (!parse_channel(cJSON_IsString(channel_json) ? channel_json->valuestring : NULL,
                       &st->channel, &channel_name)) {
        tf_set_last_error("tee: channel must be samples, errors, stats, or audit");
        tee_state_free(st);
        return NULL;
    }
    st->channel_name = strdup(channel_name);

    cJSON *name_json = cJSON_GetObjectItemCaseSensitive(args, "name");
    st->name = strdup(cJSON_IsString(name_json) && name_json->valuestring[0] ? name_json->valuestring : "tee");

    cJSON *limit_json = cJSON_GetObjectItemCaseSensitive(args, "limit");
    if (!limit_json) limit_json = cJSON_GetObjectItemCaseSensitive(args, "max_rows");
    if (limit_json) {
        size_t parsed_limit = 0;
        if (tf_json_get_size_arg_any(args, "limit", "max_rows",
                                     1, TF_MAX_AUDIT_RECORDS,
                                     &parsed_limit, "tee") < 0) {
            tee_state_free(st);
            return NULL;
        }
        st->limit = parsed_limit;
    }

    cJSON *every_json = cJSON_GetObjectItemCaseSensitive(args, "every");
    if (every_json) {
        size_t parsed_every = 0;
        if (tf_json_get_size_arg(args, "every", 1, TF_MAX_COUNT_ARG,
                                 &parsed_every, "tee") < 0) {
            tee_state_free(st);
            return NULL;
        }
        st->every = parsed_every;
    }

    cJSON *include_json = cJSON_GetObjectItemCaseSensitive(args, "include_row");
    if (!include_json) include_json = cJSON_GetObjectItemCaseSensitive(args, "includeRow");
    if (include_json) st->include_row = cJSON_IsTrue(include_json);

    cJSON *columns_json = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (tee_copy_columns_arg(st, columns_json) != TF_OK) {
        tee_state_free(st);
        return NULL;
    }

    if (tf_audit_options_parse(&st->audit_opts, args, "tee") != TF_OK) {
        tee_state_free(st);
        return NULL;
    }

    if (!st->name || !st->channel_name) {
        tee_state_free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) {
        tee_state_free(st);
        return NULL;
    }
    step->process = tee_process;
    step->flush = tee_flush;
    step->destroy = tee_destroy;
    step->state = st;
    return step;
}
