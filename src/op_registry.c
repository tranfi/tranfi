/*
 * op_registry.c — Declarative registry of all built-in ops.
 *
 * Each entry describes an op's kind, capabilities, arguments,
 * schema inference callback, and native constructor.
 */

#include "ir.h"
#include "internal.h"
#include "cJSON.h"
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <stdlib.h>

/* ---- Schema inference callbacks ---- */

/* Decoders: output schema unknown until runtime */
static int infer_schema_unknown(const tf_ir_node *node,
                                const tf_schema *in, tf_schema *out) {
    (void)node; (void)in;
    out->col_names = NULL;
    out->col_types = NULL;
    out->n_cols = 0;
    out->known = false;
    return TF_OK;
}

/* Encoders: consume schema, no output */
static int infer_schema_sink(const tf_ir_node *node,
                             const tf_schema *in, tf_schema *out) {
    (void)node; (void)in;
    out->col_names = NULL;
    out->col_types = NULL;
    out->n_cols = 0;
    out->known = false;
    return TF_OK;
}

/* filter, head, skip, unique: output schema = input schema */
static int infer_schema_passthrough(const tf_ir_node *node,
                                    const tf_schema *in, tf_schema *out) {
    (void)node;
    if (!in->known) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }
    tf_schema_copy(out, in);
    return TF_OK;
}

/* select: output schema = subset of input columns */
static int infer_schema_select(const tf_ir_node *node,
                               const tf_schema *in, tf_schema *out) {
    if (!in->known) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }

    cJSON *cols = cJSON_GetObjectItemCaseSensitive(node->args, "columns");
    if (!cols || !cJSON_IsArray(cols)) return TF_ERROR;

    if (tf_column_selectors_have_syntax_json(cols)) {
        int *indices = NULL;
        size_t n_indices = 0;
        char *error = NULL;
        int rc = tf_column_selectors_resolve_json(cols, in->col_names, in->col_types, in->n_cols,
                                                  &indices, &n_indices, &error);
        free(error);
        if (rc != TF_OK) return TF_ERROR;
        out->col_names = calloc(n_indices, sizeof(char *));
        out->col_types = calloc(n_indices, sizeof(tf_type));
        if (!out->col_names || !out->col_types) {
            free(indices);
            tf_schema_free(out);
            return TF_ERROR;
        }
        out->n_cols = n_indices;
        out->known = true;
        for (size_t i = 0; i < n_indices; i++) {
            int ci = indices[i];
            out->col_names[i] = strdup(in->col_names[ci]);
            out->col_types[i] = in->col_types[ci];
        }
        free(indices);
        return TF_OK;
    }

    int n = cJSON_GetArraySize(cols);
    out->col_names = calloc(n, sizeof(char *));
    out->col_types = calloc(n, sizeof(tf_type));
    out->n_cols = n;
    out->known = true;

    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(cols, i);
        if (!cJSON_IsString(item)) {
            tf_schema_free(out);
            return TF_ERROR;
        }
        const char *name = item->valuestring;
        /* Find column in input schema */
        bool found = false;
        for (size_t j = 0; j < in->n_cols; j++) {
            if (strcmp(in->col_names[j], name) == 0) {
                out->col_names[i] = strdup(name);
                out->col_types[i] = in->col_types[j];
                found = true;
                break;
            }
        }
        if (!found) {
            /* Column not in input — still record it, validation can catch it */
            out->col_names[i] = strdup(name);
            out->col_types[i] = TF_TYPE_NULL;
        }
    }
    return TF_OK;
}

static int infer_schema_relocate(const tf_ir_node *node,
                                 const tf_schema *in, tf_schema *out) {
    if (!in->known) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }

    cJSON *cols = cJSON_GetObjectItemCaseSensitive(node->args, "columns");
    if (!cols || !cJSON_IsArray(cols)) return TF_ERROR;
    cJSON *before = cJSON_GetObjectItemCaseSensitive(node->args, "before");
    cJSON *after = cJSON_GetObjectItemCaseSensitive(node->args, "after");
    if (before && after) return TF_ERROR;
    if (before && !cJSON_IsString(before)) return TF_ERROR;
    if (after && !cJSON_IsString(after)) return TF_ERROR;

    size_t n_in = in->n_cols;
    int *move_idx = NULL;
    size_t n_move = 0;
    char *selector_error = NULL;
    int rc = tf_column_selectors_resolve_json(cols, in->col_names, in->col_types, in->n_cols,
                                              &move_idx, &n_move, &selector_error);
    free(selector_error);
    if (rc != TF_OK) return TF_ERROR;

    int *is_moving = calloc(n_in ? n_in : 1, sizeof(int));
    int *order = malloc(n_in ? n_in * sizeof(int) : sizeof(int));
    if (!is_moving || !order) {
        free(move_idx); free(is_moving); free(order);
        return TF_ERROR;
    }

    for (size_t i = 0; i < n_move; i++) {
        int found = move_idx[i];
        if (found < 0 || (size_t)found >= n_in || is_moving[found]) {
            free(move_idx); free(is_moving); free(order);
            return TF_ERROR;
        }
        is_moving[found] = 1;
    }

    const char *anchor = before ? before->valuestring : (after ? after->valuestring : NULL);
    int anchor_idx = -1;
    if (anchor) {
        for (size_t i = 0; i < n_in; i++) {
            if (strcmp(in->col_names[i], anchor) == 0) {
                anchor_idx = (int)i;
                break;
            }
        }
        if (anchor_idx < 0 || is_moving[anchor_idx]) {
            free(move_idx); free(is_moving); free(order);
            return TF_ERROR;
        }
    }

    size_t n_order = 0;
    if (!anchor) {
        for (size_t i = 0; i < n_move; i++) order[n_order++] = move_idx[i];
        for (size_t i = 0; i < n_in; i++)
            if (!is_moving[i]) order[n_order++] = (int)i;
    } else {
        for (size_t i = 0; i < n_in; i++) {
            if (is_moving[i]) continue;
            if (before && (int)i == anchor_idx) {
                for (size_t j = 0; j < n_move; j++) order[n_order++] = move_idx[j];
            }
            order[n_order++] = (int)i;
            if (after && (int)i == anchor_idx) {
                for (size_t j = 0; j < n_move; j++) order[n_order++] = move_idx[j];
            }
        }
    }

    if (n_order != n_in) {
        free(move_idx); free(is_moving); free(order);
        return TF_ERROR;
    }

    out->col_names = calloc(n_in, sizeof(char *));
    out->col_types = calloc(n_in, sizeof(tf_type));
    if (!out->col_names || !out->col_types) {
        free(move_idx); free(is_moving); free(order);
        tf_schema_free(out);
        return TF_ERROR;
    }
    out->n_cols = n_in;
    out->known = true;
    for (size_t i = 0; i < n_in; i++) {
        int ci = order[i];
        out->col_names[i] = strdup(in->col_names[ci]);
        out->col_types[i] = in->col_types[ci];
    }

    free(move_idx);
    free(is_moving);
    free(order);
    return TF_OK;
}

/* rename: output schema = input schema with renamed columns */
static int infer_schema_rename(const tf_ir_node *node,
                               const tf_schema *in, tf_schema *out) {
    if (!in->known) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }

    tf_schema_copy(out, in);

    cJSON *mapping = cJSON_GetObjectItemCaseSensitive(node->args, "mapping");
    if (!mapping || !cJSON_IsObject(mapping)) return TF_OK; /* no renames */

    cJSON *entry = NULL;
    cJSON_ArrayForEach(entry, mapping) {
        if (!cJSON_IsString(entry)) continue;
        const char *old_name = entry->string;
        const char *new_name = entry->valuestring;
        for (size_t i = 0; i < out->n_cols; i++) {
            if (strcmp(out->col_names[i], old_name) == 0) {
                free(out->col_names[i]);
                out->col_names[i] = strdup(new_name);
                break;
            }
        }
    }
    return TF_OK;
}

/* derive: output schema = input + derived columns */
static int infer_schema_derive(const tf_ir_node *node,
                               const tf_schema *in, tf_schema *out) {
    if (!in->known) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }

    cJSON *columns = cJSON_GetObjectItemCaseSensitive(node->args, "columns");
    int n_derived = columns ? cJSON_GetArraySize(columns) : 0;
    size_t total = in->n_cols + n_derived;

    out->col_names = calloc(total, sizeof(char *));
    out->col_types = calloc(total, sizeof(tf_type));
    out->n_cols = total;
    out->known = true;

    /* Copy input columns */
    for (size_t i = 0; i < in->n_cols; i++) {
        out->col_names[i] = strdup(in->col_names[i]);
        out->col_types[i] = in->col_types[i];
    }

    /* Add derived columns (type unknown at compile time) */
    for (int i = 0; i < n_derived; i++) {
        cJSON *item = cJSON_GetArrayItem(columns, i);
        cJSON *name_j = cJSON_GetObjectItemCaseSensitive(item, "name");
        out->col_names[in->n_cols + i] = strdup(name_j ? name_j->valuestring : "?");
        out->col_types[in->n_cols + i] = TF_TYPE_NULL;
    }
    return TF_OK;
}


static int registry_across_token_matches(const char *p, const char *tok) {
    return strncmp(p, tok, strlen(tok)) == 0;
}

static char *registry_across_format_name(const char *tmpl, const char *col, const char *fn) {
    if (!tmpl || !tmpl[0]) tmpl = "{col}_{fn}";
    if (!col) col = "";
    if (!fn) fn = "";
    size_t len = 0;
    for (const char *p = tmpl; *p;) {
        if (registry_across_token_matches(p, "{.col}")) { len += strlen(col); p += 6; }
        else if (registry_across_token_matches(p, "{col}")) { len += strlen(col); p += 5; }
        else if (registry_across_token_matches(p, "{.fn}")) { len += strlen(fn); p += 5; }
        else if (registry_across_token_matches(p, "{fn}")) { len += strlen(fn); p += 4; }
        else { len++; p++; }
    }
    char *out = malloc(len + 1);
    if (!out) return NULL;
    char *w = out;
    for (const char *p = tmpl; *p;) {
        const char *rep = NULL;
        size_t tok_len = 0;
        if (registry_across_token_matches(p, "{.col}")) { rep = col; tok_len = 6; }
        else if (registry_across_token_matches(p, "{col}")) { rep = col; tok_len = 5; }
        else if (registry_across_token_matches(p, "{.fn}")) { rep = fn; tok_len = 5; }
        else if (registry_across_token_matches(p, "{fn}")) { rep = fn; tok_len = 4; }
        if (rep) {
            size_t n = strlen(rep);
            memcpy(w, rep, n);
            w += n;
            p += tok_len;
        } else {
            *w++ = *p++;
        }
    }
    *w = '\0';
    return out;
}

static tf_type registry_across_output_type(const char *fn, tf_type input) {
    if (!fn) return TF_TYPE_NULL;
    if (strcasecmp(fn, "trim") == 0 || strcasecmp(fn, "lower") == 0 ||
        strcasecmp(fn, "tolower") == 0 || strcasecmp(fn, "upper") == 0 ||
        strcasecmp(fn, "toupper") == 0) {
        return input == TF_TYPE_STRING ? TF_TYPE_STRING : TF_TYPE_NULL;
    }
    if (!(input == TF_TYPE_INT64 || input == TF_TYPE_FLOAT64)) return TF_TYPE_NULL;
    if (strcasecmp(fn, "abs") == 0) return input;
    if (strcasecmp(fn, "round") == 0 || strcasecmp(fn, "floor") == 0 ||
        strcasecmp(fn, "ceil") == 0 || strcasecmp(fn, "ceiling") == 0) return TF_TYPE_INT64;
    if (strcasecmp(fn, "sqrt") == 0 || strcasecmp(fn, "log") == 0 ||
        strcasecmp(fn, "exp") == 0) return TF_TYPE_FLOAT64;
    return TF_TYPE_NULL;
}

static int registry_across_functions(const tf_ir_node *node, cJSON **out_fns, int *out_n) {
    cJSON *functions = cJSON_GetObjectItemCaseSensitive(node->args, "functions");
    if (!functions) functions = cJSON_GetObjectItemCaseSensitive(node->args, "fns");
    cJSON *fn = cJSON_GetObjectItemCaseSensitive(node->args, "fn");
    if (cJSON_IsArray(functions)) {
        int n = cJSON_GetArraySize(functions);
        if (n <= 0) return TF_ERROR;
        *out_fns = functions;
        *out_n = n;
        return TF_OK;
    }
    if (cJSON_IsString(functions) || cJSON_IsString(fn)) {
        *out_fns = cJSON_IsString(functions) ? functions : fn;
        *out_n = 1;
        return TF_OK;
    }
    return TF_ERROR;
}

/* across: input schema with selected columns replaced, or input + generated columns */
static int infer_schema_across(const tf_ir_node *node,
                               const tf_schema *in, tf_schema *out) {
    if (!in->known) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }

    cJSON *cols = cJSON_GetObjectItemCaseSensitive(node->args, "columns");
    if (!cols || !cJSON_IsArray(cols)) return TF_ERROR;
    int *indices = NULL;
    size_t n_indices = 0;
    char *error = NULL;
    int rc = tf_column_selectors_resolve_json(cols, in->col_names, in->col_types, in->n_cols,
                                              &indices, &n_indices, &error);
    free(error);
    if (rc != TF_OK) return TF_ERROR;

    cJSON *fns = NULL;
    int n_fns = 0;
    if (registry_across_functions(node, &fns, &n_fns) != TF_OK) { free(indices); return TF_ERROR; }
    cJSON *names = cJSON_GetObjectItemCaseSensitive(node->args, "names");
    cJSON *replace_j = cJSON_GetObjectItemCaseSensitive(node->args, "replace");
    int replace = cJSON_IsBool(replace_j) ? cJSON_IsTrue(replace_j) : (n_fns == 1 && !cJSON_IsString(names));
    if (replace && n_fns != 1) { free(indices); return TF_ERROR; }
    const char *tmpl = cJSON_IsString(names) ? names->valuestring : (replace ? "{col}" : "{col}_{fn}");

    size_t total = in->n_cols + (replace ? 0 : n_indices * (size_t)n_fns);
    out->col_names = calloc(total ? total : 1, sizeof(char *));
    out->col_types = calloc(total ? total : 1, sizeof(tf_type));
    if (!out->col_names || !out->col_types) { free(indices); tf_schema_free(out); return TF_ERROR; }
    out->n_cols = total;
    out->known = true;
    for (size_t i = 0; i < in->n_cols; i++) {
        out->col_names[i] = strdup(in->col_names[i]);
        out->col_types[i] = in->col_types[i];
    }

    if (replace) {
        const char *fn_name = cJSON_IsArray(fns) ? cJSON_GetArrayItem(fns, 0)->valuestring : fns->valuestring;
        for (size_t i = 0; i < n_indices; i++) {
            size_t ci = (size_t)indices[i];
            tf_type t = registry_across_output_type(fn_name, in->col_types[ci]);
            if (t == TF_TYPE_NULL) { free(indices); tf_schema_free(out); return TF_ERROR; }
            out->col_types[ci] = t;
        }
    } else {
        size_t a = 0;
        for (size_t i = 0; i < n_indices; i++) {
            size_t ci = (size_t)indices[i];
            for (int f = 0; f < n_fns; f++) {
                cJSON *fn_j = cJSON_IsArray(fns) ? cJSON_GetArrayItem(fns, f) : fns;
                if (!cJSON_IsString(fn_j)) { free(indices); tf_schema_free(out); return TF_ERROR; }
                tf_type t = registry_across_output_type(fn_j->valuestring, in->col_types[ci]);
                if (t == TF_TYPE_NULL) { free(indices); tf_schema_free(out); return TF_ERROR; }
                out->col_names[in->n_cols + a] = registry_across_format_name(tmpl, in->col_names[ci], fn_j->valuestring);
                out->col_types[in->n_cols + a] = t;
                if (!out->col_names[in->n_cols + a]) { free(indices); tf_schema_free(out); return TF_ERROR; }
                a++;
            }
        }
    }
    free(indices);
    return TF_OK;
}

/* validate: input + _valid bool column */
static int infer_schema_validate(const tf_ir_node *node,
                                 const tf_schema *in, tf_schema *out) {
    (void)node;
    if (!in->known) { out->known = false; out->col_names = NULL; out->col_types = NULL; out->n_cols = 0; return TF_OK; }
    out->n_cols = in->n_cols + 1;
    out->col_names = calloc(out->n_cols, sizeof(char *));
    out->col_types = calloc(out->n_cols, sizeof(tf_type));
    out->known = true;
    for (size_t i = 0; i < in->n_cols; i++) {
        out->col_names[i] = strdup(in->col_names[i]);
        out->col_types[i] = in->col_types[i];
    }
    out->col_names[in->n_cols] = strdup("_valid");
    out->col_types[in->n_cols] = TF_TYPE_BOOL;
    return TF_OK;
}

static int infer_schema_assert(const tf_ir_node *node,
                               const tf_schema *in, tf_schema *out) {
    cJSON *action_j = cJSON_GetObjectItemCaseSensitive(node->args, "action");
    const char *action = cJSON_IsString(action_j) ? action_j->valuestring : "fail";
    if (strcmp(action, "annotate") != 0) return infer_schema_passthrough(node, in, out);
    if (!in->known) { out->known = false; out->col_names = NULL; out->col_types = NULL; out->n_cols = 0; return TF_OK; }
    cJSON *result_j = cJSON_GetObjectItemCaseSensitive(node->args, "result");
    const char *result = cJSON_IsString(result_j) && result_j->valuestring[0]
        ? result_j->valuestring : "_assert";
    out->n_cols = in->n_cols + 1;
    out->col_names = calloc(out->n_cols, sizeof(char *));
    out->col_types = calloc(out->n_cols, sizeof(tf_type));
    if (!out->col_names || !out->col_types) { tf_schema_free(out); return TF_ERROR; }
    out->known = true;
    for (size_t i = 0; i < in->n_cols; i++) {
        out->col_names[i] = strdup(in->col_names[i]);
        out->col_types[i] = in->col_types[i];
    }
    out->col_names[in->n_cols] = strdup(result);
    out->col_types[in->n_cols] = TF_TYPE_BOOL;
    return TF_OK;
}

static int infer_schema_schema(const tf_ir_node *node,
                               const tf_schema *in, tf_schema *out) {
    cJSON *action_j = cJSON_GetObjectItemCaseSensitive(node->args, "action");
    cJSON *mode_j = cJSON_GetObjectItemCaseSensitive(node->args, "mode");
    const char *action = cJSON_IsString(action_j) ? action_j->valuestring :
        (cJSON_IsString(mode_j) ? mode_j->valuestring : "fail");
    if (strcmp(action, "annotate") != 0) return infer_schema_passthrough(node, in, out);
    if (!in->known) { out->known = false; out->col_names = NULL; out->col_types = NULL; out->n_cols = 0; return TF_OK; }
    cJSON *result_j = cJSON_GetObjectItemCaseSensitive(node->args, "result");
    const char *result = cJSON_IsString(result_j) && result_j->valuestring[0]
        ? result_j->valuestring : "_schema";
    out->n_cols = in->n_cols + 1;
    out->col_names = calloc(out->n_cols, sizeof(char *));
    out->col_types = calloc(out->n_cols, sizeof(tf_type));
    if (!out->col_names || !out->col_types) { tf_schema_free(out); return TF_ERROR; }
    out->known = true;
    for (size_t i = 0; i < in->n_cols; i++) {
        out->col_names[i] = strdup(in->col_names[i]);
        out->col_types[i] = in->col_types[i];
    }
    out->col_names[in->n_cols] = strdup(result);
    out->col_types[in->n_cols] = TF_TYPE_BOOL;
    return TF_OK;
}

static int infer_schema_schema_infer(const tf_ir_node *node,
                                     const tf_schema *in, tf_schema *out) {
    (void)node; (void)in;
    static const char *names[] = {
        "column", "type", "nullable", "non_null", "rows_seen",
        "rows_sampled", "missing", "non_missing", "observed_types", "warning"
    };
    static const tf_type types[] = {
        TF_TYPE_STRING, TF_TYPE_STRING, TF_TYPE_BOOL, TF_TYPE_BOOL, TF_TYPE_INT64,
        TF_TYPE_INT64, TF_TYPE_INT64, TF_TYPE_INT64, TF_TYPE_STRING, TF_TYPE_STRING
    };
    size_t n = sizeof(names) / sizeof(names[0]);
    out->col_names = calloc(n, sizeof(char *));
    out->col_types = calloc(n, sizeof(tf_type));
    out->n_cols = n;
    out->known = true;
    if (!out->col_names || !out->col_types) { tf_schema_free(out); return TF_ERROR; }
    for (size_t i = 0; i < n; i++) {
        out->col_names[i] = strdup(names[i]);
        out->col_types[i] = types[i];
        if (!out->col_names[i]) { tf_schema_free(out); return TF_ERROR; }
    }
    return TF_OK;
}

static int infer_schema_add_int64_column(const tf_schema *in, tf_schema *out, const char *name) {
    if (!in->known) { out->known = false; out->col_names = NULL; out->col_types = NULL; out->n_cols = 0; return TF_OK; }
    if (tf_size_add(in->n_cols, 1, &out->n_cols) != TF_OK) return TF_ERROR;
    out->col_names = calloc(out->n_cols, sizeof(char *));
    out->col_types = calloc(out->n_cols, sizeof(tf_type));
    if (!out->col_names || !out->col_types) { tf_schema_free(out); return TF_ERROR; }
    out->known = true;
    for (size_t i = 0; i < in->n_cols; i++) {
        out->col_names[i] = strdup(in->col_names[i]);
        out->col_types[i] = in->col_types[i];
        if (!out->col_names[i]) { tf_schema_free(out); return TF_ERROR; }
    }
    out->col_names[in->n_cols] = strdup(name);
    out->col_types[in->n_cols] = TF_TYPE_INT64;
    if (!out->col_names[in->n_cols]) { tf_schema_free(out); return TF_ERROR; }
    return TF_OK;
}

/* hash: input + _hash int column */
static int infer_schema_add_hash(const tf_ir_node *node,
                                 const tf_schema *in, tf_schema *out) {
    (void)node;
    return infer_schema_add_int64_column(in, out, "_hash");
}

static int infer_schema_rleid(const tf_ir_node *node,
                              const tf_schema *in, tf_schema *out) {
    const char *name = "_rleid";
    cJSON *res = cJSON_GetObjectItemCaseSensitive(node->args, "result");
    if (cJSON_IsString(res) && res->valuestring[0] != '\0') name = res->valuestring;
    return infer_schema_add_int64_column(in, out, name);
}

static int infer_schema_rowid(const tf_ir_node *node,
                              const tf_schema *in, tf_schema *out) {
    const char *name = "_rowid";
    cJSON *res = cJSON_GetObjectItemCaseSensitive(node->args, "result");
    if (cJSON_IsString(res) && res->valuestring[0] != '\0') name = res->valuestring;
    return infer_schema_add_int64_column(in, out, name);
}

static int infer_schema_source_name(const tf_ir_node *node,
                                    const tf_schema *in, tf_schema *out) {
    if (!in->known) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }
    const char *name = "_source";
    cJSON *res = cJSON_GetObjectItemCaseSensitive(node->args, "result");
    if (!cJSON_IsString(res)) res = cJSON_GetObjectItemCaseSensitive(node->args, "column");
    if (!cJSON_IsString(res)) res = cJSON_GetObjectItemCaseSensitive(node->args, "as");
    if (cJSON_IsString(res) && res->valuestring[0] != '\0') name = res->valuestring;

    if (tf_size_add(in->n_cols, 1, &out->n_cols) != TF_OK) return TF_ERROR;
    out->col_names = calloc(out->n_cols, sizeof(char *));
    out->col_types = calloc(out->n_cols, sizeof(tf_type));
    if (!out->col_names || !out->col_types) {
        tf_schema_free(out);
        return TF_ERROR;
    }
    out->known = true;
    for (size_t i = 0; i < in->n_cols; i++) {
        out->col_names[i] = strdup(in->col_names[i]);
        out->col_types[i] = in->col_types[i];
        if (!out->col_names[i]) {
            tf_schema_free(out);
            return TF_ERROR;
        }
    }
    out->col_names[in->n_cols] = strdup(name);
    out->col_types[in->n_cols] = TF_TYPE_STRING;
    if (!out->col_names[in->n_cols]) {
        tf_schema_free(out);
        return TF_ERROR;
    }
    return TF_OK;
}

/* frequency: output is value + count */
static int infer_schema_frequency(const tf_ir_node *node,
                                  const tf_schema *in, tf_schema *out) {
    (void)node; (void)in;
    out->n_cols = 2;
    out->col_names = calloc(2, sizeof(char *));
    out->col_types = calloc(2, sizeof(tf_type));
    if (!out->col_names || !out->col_types) {
        tf_schema_free(out);
        return TF_ERROR;
    }
    out->col_names[0] = strdup("value");
    out->col_types[0] = TF_TYPE_STRING;
    out->col_names[1] = strdup("count");
    out->col_types[1] = TF_TYPE_INT64;
    if (!out->col_names[0] || !out->col_names[1]) {
        tf_schema_free(out);
        return TF_ERROR;
    }
    out->known = true;
    return TF_OK;
}

static int schema_col_index(const tf_schema *schema, const char *name) {
    if (!schema || !schema->known || !name) return -1;
    for (size_t i = 0; i < schema->n_cols; i++) {
        if (schema->col_names[i] && strcmp(schema->col_names[i], name) == 0) return (int)i;
    }
    return -1;
}


static int infer_schema_append_source_column(const tf_ir_node *node,
                                             const tf_schema *in,
                                             tf_schema *out,
                                             const char *default_suffix) {
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(node->args, "column");
    const char *column = cJSON_IsString(col_j) ? col_j->valuestring : NULL;
    if (!in->known || !column) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }

    int ci = schema_col_index(in, column);
    if (ci < 0) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }

    const char *name = NULL;
    cJSON *res = cJSON_GetObjectItemCaseSensitive(node->args, "result");
    if (cJSON_IsString(res) && res->valuestring[0] != '\0') name = res->valuestring;

    char fallback[256];
    if (!name) {
        snprintf(fallback, sizeof(fallback), "%s_%s", column, default_suffix);
        name = fallback;
    }

    if (tf_size_add(in->n_cols, 1, &out->n_cols) != TF_OK) return TF_ERROR;
    out->col_names = calloc(out->n_cols, sizeof(char *));
    out->col_types = calloc(out->n_cols, sizeof(tf_type));
    if (!out->col_names || !out->col_types) {
        tf_schema_free(out);
        return TF_ERROR;
    }
    out->known = true;
    for (size_t i = 0; i < in->n_cols; i++) {
        out->col_names[i] = strdup(in->col_names[i]);
        out->col_types[i] = in->col_types[i];
        if (!out->col_names[i]) {
            tf_schema_free(out);
            return TF_ERROR;
        }
    }
    out->col_names[in->n_cols] = strdup(name);
    out->col_types[in->n_cols] = in->col_types[ci];
    if (!out->col_names[in->n_cols]) {
        tf_schema_free(out);
        return TF_ERROR;
    }
    return TF_OK;
}

static int infer_schema_lead(const tf_ir_node *node,
                             const tf_schema *in, tf_schema *out) {
    return infer_schema_append_source_column(node, in, out, "lead");
}

static int infer_schema_lag(const tf_ir_node *node,
                            const tf_schema *in, tf_schema *out) {
    return infer_schema_append_source_column(node, in, out, "lag");
}

static int infer_schema_shift(const tf_ir_node *node,
                              const tf_schema *in, tf_schema *out) {
    const char *suffix = "shift";
    cJSON *type = cJSON_GetObjectItemCaseSensitive(node->args, "type");
    if (cJSON_IsString(type) && strcmp(type->valuestring, "lead") == 0) suffix = "lead";
    return infer_schema_append_source_column(node, in, out, suffix);
}

static tf_type json_extract_type_from_arg(const cJSON *args) {
    cJSON *type = cJSON_GetObjectItemCaseSensitive(args, "type");
    const char *s = cJSON_IsString(type) ? type->valuestring : "string";
    if (strcmp(s, "int") == 0 || strcmp(s, "int64") == 0) return TF_TYPE_INT64;
    if (strcmp(s, "float") == 0 || strcmp(s, "float64") == 0 || strcmp(s, "number") == 0) return TF_TYPE_FLOAT64;
    if (strcmp(s, "bool") == 0 || strcmp(s, "boolean") == 0) return TF_TYPE_BOOL;
    return TF_TYPE_STRING;
}

static int infer_schema_json_extract(const tf_ir_node *node,
                                     const tf_schema *in, tf_schema *out) {
    if (!in->known) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }

    cJSON *res = cJSON_GetObjectItemCaseSensitive(node->args, "result");
    if (!cJSON_IsString(res) || res->valuestring[0] == '\0') return TF_ERROR;

    out->n_cols = in->n_cols + 1;
    out->col_names = calloc(out->n_cols, sizeof(char *));
    out->col_types = calloc(out->n_cols, sizeof(tf_type));
    if (!out->col_names || !out->col_types) {
        tf_schema_free(out);
        return TF_ERROR;
    }
    out->known = true;
    for (size_t i = 0; i < in->n_cols; i++) {
        out->col_names[i] = strdup(in->col_names[i]);
        out->col_types[i] = in->col_types[i];
    }
    out->col_names[in->n_cols] = strdup(res->valuestring);
    out->col_types[in->n_cols] = json_extract_type_from_arg(node->args);
    return TF_OK;
}

static int infer_schema_json_schema(const tf_ir_node *node,
                                    const tf_schema *in, tf_schema *out) {
    cJSON *mode_j = cJSON_GetObjectItemCaseSensitive(node->args, "mode");
    const char *mode = cJSON_IsString(mode_j) ? mode_j->valuestring : "annotate";
    if (strcmp(mode, "filter") == 0 || strcmp(mode, "keep") == 0) {
        return infer_schema_passthrough(node, in, out);
    }

    if (!in->known) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }

    const char *result = "_valid";
    cJSON *res = cJSON_GetObjectItemCaseSensitive(node->args, "result");
    if (cJSON_IsString(res) && res->valuestring[0] != '\0') result = res->valuestring;

    out->n_cols = in->n_cols + 1;
    out->col_names = calloc(out->n_cols, sizeof(char *));
    out->col_types = calloc(out->n_cols, sizeof(tf_type));
    if (!out->col_names || !out->col_types) {
        tf_schema_free(out);
        return TF_ERROR;
    }
    out->known = true;
    for (size_t i = 0; i < in->n_cols; i++) {
        out->col_names[i] = strdup(in->col_names[i]);
        out->col_types[i] = in->col_types[i];
    }
    out->col_names[in->n_cols] = strdup(result);
    out->col_types[in->n_cols] = TF_TYPE_BOOL;
    return TF_OK;
}

static tf_type json_flatten_type_from_string(const char *type_s) {
    if (!type_s || strcmp(type_s, "string") == 0 || strcmp(type_s, "str") == 0 || strcmp(type_s, "json") == 0) return TF_TYPE_STRING;
    if (strcmp(type_s, "int") == 0 || strcmp(type_s, "int64") == 0) return TF_TYPE_INT64;
    if (strcmp(type_s, "float") == 0 || strcmp(type_s, "float64") == 0 || strcmp(type_s, "number") == 0) return TF_TYPE_FLOAT64;
    if (strcmp(type_s, "bool") == 0 || strcmp(type_s, "boolean") == 0) return TF_TYPE_BOOL;
    return TF_TYPE_NULL;
}

static int json_flatten_parse_string_field(const char *spec, char **name, tf_type *type) {
    if (!spec) return TF_ERROR;
    char *copy = strdup(spec);
    if (!copy) return TF_ERROR;
    char *first = strchr(copy, ':');
    if (!first) { free(copy); return TF_ERROR; }
    *first = '\0';
    char *second = strchr(first + 1, ':');
    if (second) *second = '\0';
    tf_type parsed = json_flatten_type_from_string(second ? second + 1 : "string");
    if (first[1] == '\0' || parsed == TF_TYPE_NULL) {
        free(copy);
        return TF_ERROR;
    }
    *name = strdup(first + 1);
    *type = parsed;
    free(copy);
    return *name ? TF_OK : TF_ERROR;
}

static int json_flatten_field_schema(const cJSON *item, char **name, tf_type *type) {
    *name = NULL;
    *type = TF_TYPE_STRING;
    if (cJSON_IsString(item)) return json_flatten_parse_string_field(item->valuestring, name, type);
    if (!cJSON_IsObject(item)) return TF_ERROR;
    cJSON *name_j = cJSON_GetObjectItemCaseSensitive(item, "name");
    if (!cJSON_IsString(name_j)) name_j = cJSON_GetObjectItemCaseSensitive(item, "result");
    cJSON *type_j = cJSON_GetObjectItemCaseSensitive(item, "type");
    tf_type parsed = json_flatten_type_from_string(cJSON_IsString(type_j) ? type_j->valuestring : "string");
    if (!cJSON_IsString(name_j) || name_j->valuestring[0] == '\0' || parsed == TF_TYPE_NULL) return TF_ERROR;
    *name = strdup(name_j->valuestring);
    *type = parsed;
    return *name ? TF_OK : TF_ERROR;
}

static int infer_schema_json_flatten(const tf_ir_node *node,
                                     const tf_schema *in, tf_schema *out) {
    if (!in->known) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }
    cJSON *fields = cJSON_GetObjectItemCaseSensitive(node->args, "fields");
    if (!cJSON_IsArray(fields) || cJSON_GetArraySize(fields) <= 0) return TF_ERROR;
    int n_fields = cJSON_GetArraySize(fields);
    out->n_cols = in->n_cols + (size_t)n_fields;
    out->col_names = calloc(out->n_cols, sizeof(char *));
    out->col_types = calloc(out->n_cols, sizeof(tf_type));
    if (!out->col_names || !out->col_types) {
        tf_schema_free(out);
        return TF_ERROR;
    }
    out->known = true;
    for (size_t i = 0; i < in->n_cols; i++) {
        out->col_names[i] = strdup(in->col_names[i]);
        out->col_types[i] = in->col_types[i];
    }
    for (int i = 0; i < n_fields; i++) {
        char *name = NULL;
        tf_type type = TF_TYPE_STRING;
        if (json_flatten_field_schema(cJSON_GetArrayItem(fields, i), &name, &type) != TF_OK) {
            free(name);
            tf_schema_free(out);
            return TF_ERROR;
        }
        out->col_names[in->n_cols + (size_t)i] = name;
        out->col_types[in->n_cols + (size_t)i] = type;
    }
    return TF_OK;
}


/* group-agg: typed group columns + aggregate columns (float64) */
static int infer_schema_group_agg(const tf_ir_node *node,
                                  const tf_schema *in, tf_schema *out) {
    cJSON *group_by = cJSON_GetObjectItemCaseSensitive(node->args, "group_by");
    cJSON *aggs = cJSON_GetObjectItemCaseSensitive(node->args, "aggs");
    int ng = group_by ? cJSON_GetArraySize(group_by) : 0;
    int na = aggs ? cJSON_GetArraySize(aggs) : 0;
    if (ng < 0 || na < 0) return TF_ERROR;
    out->n_cols = (size_t)ng + (size_t)na;
    out->col_names = calloc(out->n_cols ? out->n_cols : 1, sizeof(char *));
    out->col_types = calloc(out->n_cols ? out->n_cols : 1, sizeof(tf_type));
    if (!out->col_names || !out->col_types) {
        tf_schema_free(out);
        return TF_ERROR;
    }
    for (int i = 0; i < ng; i++) {
        cJSON *item = cJSON_GetArrayItem(group_by, i);
        const char *name = cJSON_IsString(item) ? item->valuestring : "?";
        out->col_names[i] = strdup(name);
        if (!out->col_names[i]) {
            tf_schema_free(out);
            return TF_ERROR;
        }
        int input_idx = schema_col_index(in, name);
        out->col_types[i] = input_idx >= 0 ? in->col_types[input_idx] : TF_TYPE_STRING;
    }
    for (int i = 0; i < na; i++) {
        cJSON *item = cJSON_GetArrayItem(aggs, i);
        cJSON *name_j = cJSON_GetObjectItemCaseSensitive(item, "name");
        out->col_names[(size_t)ng + (size_t)i] = strdup(name_j && cJSON_IsString(name_j) ? name_j->valuestring : "?");
        if (!out->col_names[(size_t)ng + (size_t)i]) {
            tf_schema_free(out);
            return TF_ERROR;
        }
        out->col_types[(size_t)ng + (size_t)i] = TF_TYPE_FLOAT64;
    }
    out->known = true;
    return TF_OK;
}

/* stats/scan: output schema is known from requested or profile-default stats */
static int infer_schema_stats(const tf_ir_node *node,
                              const tf_schema *in, tf_schema *out) {
    (void)in;
    int w_count = 0, w_missing = 0, w_complete_rate = 0;
    int w_sum = 0, w_avg = 0, w_min = 0, w_max = 0;
    int w_var = 0, w_stddev = 0;
    int w_median = 0, w_p25 = 0, w_p75 = 0;
    int w_skewness = 0, w_kurtosis = 0;
    int w_distinct = 0, w_hist = 0, w_sample = 0;

    cJSON *arr = node ? cJSON_GetObjectItemCaseSensitive(node->args, "stats") : NULL;
    if (arr && cJSON_IsArray(arr)) {
        int n = cJSON_GetArraySize(arr);
        for (int i = 0; i < n; i++) {
            cJSON *item = cJSON_GetArrayItem(arr, i);
            if (!cJSON_IsString(item)) continue;
            const char *s = item->valuestring;
            if (strcmp(s, "count") == 0) w_count = 1;
            else if (strcmp(s, "missing") == 0) w_missing = 1;
            else if (strcmp(s, "complete_rate") == 0) w_complete_rate = 1;
            else if (strcmp(s, "sum") == 0) w_sum = 1;
            else if (strcmp(s, "avg") == 0 || strcmp(s, "mean") == 0) w_avg = 1;
            else if (strcmp(s, "min") == 0) w_min = 1;
            else if (strcmp(s, "max") == 0) w_max = 1;
            else if (strcmp(s, "var") == 0 || strcmp(s, "variance") == 0) w_var = 1;
            else if (strcmp(s, "stddev") == 0 || strcmp(s, "sd") == 0) w_stddev = 1;
            else if (strcmp(s, "median") == 0) w_median = 1;
            else if (strcmp(s, "p25") == 0 || strcmp(s, "q1") == 0) w_p25 = 1;
            else if (strcmp(s, "p75") == 0 || strcmp(s, "q3") == 0) w_p75 = 1;
            else if (strcmp(s, "skewness") == 0 || strcmp(s, "skew") == 0) w_skewness = 1;
            else if (strcmp(s, "kurtosis") == 0 || strcmp(s, "kurt") == 0) w_kurtosis = 1;
            else if (strcmp(s, "distinct") == 0) w_distinct = 1;
            else if (strcmp(s, "hist") == 0) w_hist = 1;
            else if (strcmp(s, "sample") == 0) w_sample = 1;
        }
    } else if (node && node->op && strcmp(node->op, "scan") == 0) {
        w_count = w_missing = w_complete_rate = 1;
        w_distinct = 1;
        w_min = w_max = w_avg = w_stddev = 1;
        w_p25 = w_median = w_p75 = 1;
        w_hist = w_sample = 1;
    } else {
        /* Default: basic + variance + median */
        w_count = w_sum = w_avg = w_min = w_max = 1;
        w_var = w_stddev = w_median = 1;
    }

    size_t n_cols = 1; /* column */
    if (w_count) n_cols++;
    if (w_missing) n_cols++;
    if (w_complete_rate) n_cols++;
    if (w_sum) n_cols++;
    if (w_avg) n_cols++;
    if (w_min) n_cols++;
    if (w_max) n_cols++;
    if (w_var) n_cols++;
    if (w_stddev) n_cols++;
    if (w_median) n_cols++;
    if (w_p25) n_cols++;
    if (w_p75) n_cols++;
    if (w_skewness) n_cols++;
    if (w_kurtosis) n_cols++;
    if (w_distinct) n_cols++;
    if (w_hist) n_cols++;
    if (w_sample) n_cols++;

    out->col_names = calloc(n_cols, sizeof(char *));
    out->col_types = calloc(n_cols, sizeof(tf_type));
    out->n_cols = n_cols;
    if (!out->col_names || !out->col_types) {
        tf_schema_free(out);
        return TF_ERROR;
    }
    out->known = true;

    size_t ci = 0;
#define ADD_STAT_COLUMN(name, type) do { \
        out->col_names[ci] = strdup((name)); \
        out->col_types[ci] = (type); \
        if (!out->col_names[ci]) { \
            tf_schema_free(out); \
            return TF_ERROR; \
        } \
        ci++; \
    } while (0)

    ADD_STAT_COLUMN("column", TF_TYPE_STRING);
    if (w_count)         ADD_STAT_COLUMN("count", TF_TYPE_INT64);
    if (w_missing)       ADD_STAT_COLUMN("missing", TF_TYPE_INT64);
    if (w_complete_rate) ADD_STAT_COLUMN("complete_rate", TF_TYPE_FLOAT64);
    if (w_sum)           ADD_STAT_COLUMN("sum", TF_TYPE_FLOAT64);
    if (w_avg)           ADD_STAT_COLUMN("avg", TF_TYPE_FLOAT64);
    if (w_min)           ADD_STAT_COLUMN("min", TF_TYPE_FLOAT64);
    if (w_max)           ADD_STAT_COLUMN("max", TF_TYPE_FLOAT64);
    if (w_var)           ADD_STAT_COLUMN("var", TF_TYPE_FLOAT64);
    if (w_stddev)        ADD_STAT_COLUMN("stddev", TF_TYPE_FLOAT64);
    if (w_median)        ADD_STAT_COLUMN("median", TF_TYPE_FLOAT64);
    if (w_p25)           ADD_STAT_COLUMN("p25", TF_TYPE_FLOAT64);
    if (w_p75)           ADD_STAT_COLUMN("p75", TF_TYPE_FLOAT64);
    if (w_skewness)      ADD_STAT_COLUMN("skewness", TF_TYPE_FLOAT64);
    if (w_kurtosis)      ADD_STAT_COLUMN("kurtosis", TF_TYPE_FLOAT64);
    if (w_distinct)      ADD_STAT_COLUMN("distinct", TF_TYPE_INT64);
    if (w_hist)          ADD_STAT_COLUMN("hist", TF_TYPE_STRING);
    if (w_sample)        ADD_STAT_COLUMN("sample", TF_TYPE_STRING);
#undef ADD_STAT_COLUMN

    return TF_OK;
}

/* ---- Built-in ops table ---- */

static tf_arg_desc csv_decode_args[] = {
    {"delimiter", "string", false, "\",\""},
    {"header",    "bool",   false, "true"},
    {"batch_size","int",    false, "1024"},
    {"repair",    "bool",   false, "false"},
    {"mode",      "string", false, "permissive"},
    {"strict",    "bool",   false, "false"},
    {"max_error_bytes", "int", false, "4096"},
    {"max_record_bytes", "int", false, "67108864"},
    {"max_columns", "int", false, "8192"},
    {"nulls",    "string|array", false, """"},
    {"na",       "string|array", false, """"},
    {"quoted_nulls", "bool", false, "true"},
    {"audit", "bool", false, "false"},
    {"audit_limit", "int", false, "1000"},
    {"audit_include_row", "bool", false, "true"},
    {"audit_columns", "string[]", false, NULL},
    {"audit_redact", "string[]", false, NULL},
    {"audit_hash_columns", "string[]", false, NULL},
    {"audit_max_bytes", "size", false, "0"},
    {"audit_max_cell_bytes", "size", false, "0"},
};

static tf_arg_desc csv_encode_args[] = {
    {"delimiter", "string", false, "\",\""},
    {"header",    "bool",   false, "true"},
};

static tf_arg_desc jsonl_decode_args[] = {
    {"batch_size", "int", false, "1024"},
    {"on_error", "string", false, "skip"},
    {"max_error_bytes", "int", false, "4096"},
    {"max_record_bytes", "int", false, "67108864"},
    {"max_columns", "int", false, "8192"},
};

static tf_arg_desc jsonl_encode_args[] = {
    {0}  /* no args */
};

static tf_arg_desc text_decode_args[] = {
    {"batch_size", "int", false, "1024"},
    {"max_error_bytes", "int", false, "4096"},
    {"max_record_bytes", "int", false, "67108864"},
    {"max_columns", "int", false, "8192"},
};

static tf_arg_desc text_encode_args[] = {
    {0}  /* no args */
};

static tf_arg_desc grep_args[] = {
    {"pattern", "string", true, NULL},
    {"invert", "bool", false, "false"},
    {"column", "string", false, "\"_line\""},
    {"regex", "bool", false, "false"},
};

static tf_arg_desc filter_args[] = {
    {"expr", "string", true, NULL},
    {"audit", "bool", false, "false"},
    {"audit_limit", "int", false, "1000"},
    {"audit_include_row", "bool", false, "true"},
    {"audit_columns", "string[]", false, NULL},
    {"audit_redact", "string[]", false, NULL},
    {"audit_hash_columns", "string[]", false, NULL},
    {"audit_max_bytes", "int", false, NULL},
    {"audit_max_cell_bytes", "int", false, NULL},
};

static tf_arg_desc select_args[] = {
    {"columns", "string[]", true, NULL},
};

static tf_arg_desc relocate_args[] = {
    {"columns", "string[]", true, NULL},
    {"before", "string", false, NULL},
    {"after", "string", false, NULL},
};

static tf_arg_desc rename_args[] = {
    {"mapping", "map", true, NULL},
};

static tf_arg_desc head_args[] = {
    {"n", "int", true, NULL},
};

static tf_arg_desc skip_args[] = {
    {"n", "int", true, NULL},
};

static tf_arg_desc derive_args[] = {
    {"columns", "map[]", true, NULL},
};


static tf_arg_desc across_args[] = {
    {"columns", "string[]", true, NULL},
    {"fn", "string", false, NULL},
    {"functions", "string[]", false, NULL},
    {"names", "string", false, NULL},
    {"replace", "bool", false, NULL},
};

static tf_arg_desc stats_args[] = {
    {"stats", "string[]", false, NULL},
};

static tf_arg_desc scan_args[] = {
    {"stats", "string[]", false, NULL},
};

static tf_arg_desc unique_args[] = {
    {"columns", "string[]", false, NULL},
    {"max_keys", "int", false, NULL},
    {"max_state_bytes", "int", false, NULL},
    {"spill_dir", "string", false, NULL},
    {"spill_memory_bytes", "int", false, NULL},
    {"spill_run_rows", "int", false, NULL},
    {"spill_output_rows", "int", false, NULL},
    {"sorted", "bool", false, "false"},
};

static tf_arg_desc sort_args[] = {
    {"columns", "map[]", true, NULL},
    {"spill_dir", "string", false, NULL},
    {"spill_memory_bytes", "int", false, NULL},
    {"spill_run_rows", "int", false, NULL},
    {"spill_output_rows", "int", false, NULL},
};

static tf_arg_desc validate_args[] = {
    {"expr", "string", false, NULL},
    {"rules", "array|object", false, NULL},
    {"rules_file", "string", false, NULL},
    {"audit", "bool", false, "false"},
    {"audit_limit", "int", false, "1000"},
    {"audit_include_row", "bool", false, "true"},
    {"audit_columns", "string[]", false, NULL},
    {"audit_redact", "string[]", false, NULL},
    {"audit_hash_columns", "string[]", false, NULL},
    {"audit_max_bytes", "int", false, NULL},
    {"audit_max_cell_bytes", "int", false, NULL},
    {"max_failures", "int", false, NULL},
    {"max_failure_rate", "number", false, NULL},
    {"warn_failure_rate", "number", false, NULL},
    {"name", "string", false, "\"validate\""},
    {"message", "string", false, "\"\""},
};

static tf_arg_desc assert_args[] = {
    {"expr", "string", false, NULL},
    {"aggregate", "string", false, NULL},
    {"column", "string", false, NULL},
    {"op", "string", false, NULL},
    {"value", "number", false, NULL},
    {"tolerance", "number", false, "1e-12"},
    {"rel", "bool", false, "true"},
    {"action", "string", false, "\"fail\""},
    {"name", "string", false, "\"assert\""},
    {"message", "string", false, "\"\""},
    {"result", "string", false, "\"_assert\""},
    {"audit", "bool", false, "false"},
    {"audit_limit", "int", false, "1000"},
    {"audit_include_row", "bool", false, "true"},
    {"audit_columns", "string[]", false, NULL},
    {"audit_redact", "string[]", false, NULL},
    {"audit_hash_columns", "string[]", false, NULL},
    {"audit_max_bytes", "int", false, NULL},
    {"audit_max_cell_bytes", "int", false, NULL},
};

static tf_arg_desc quarantine_args[] = {
    {"expr", "string", true, NULL},
    {"name", "string", false, "\"quarantine\""},
    {"message", "string", false, "\"\""},
    {"audit_include_row", "bool", false, "true"},
    {"audit_columns", "string[]", false, NULL},
    {"audit_redact", "string[]", false, NULL},
    {"audit_hash_columns", "string[]", false, NULL},
    {"audit_max_bytes", "size", false, "0"},
    {"audit_max_cell_bytes", "size", false, "0"},
};

static tf_arg_desc tee_args[] = {
    {"expr", "string", false, NULL},
    {"channel", "string", false, "\"samples\""},
    {"columns", "string[]", false, NULL},
    {"limit", "int", false, "1000"},
    {"every", "int", false, "1"},
    {"name", "string", false, "\"tee\""},
    {"include_row", "bool", false, "true"},
    {"audit_include_row", "bool", false, "true"},
    {"audit_columns", "string[]", false, NULL},
    {"audit_redact", "string[]", false, NULL},
    {"audit_hash_columns", "string[]", false, NULL},
    {"audit_max_bytes", "size", false, "0"},
    {"audit_max_cell_bytes", "size", false, "0"},
};

static tf_arg_desc schema_args[] = {
    {"columns", "map", false, NULL},
    {"required", "string[]", false, NULL},
    {"non_null", "string[]", false, NULL},
    {"nullable", "string[]", false, NULL},
    {"values", "map", false, NULL},
    {"min", "map", false, NULL},
    {"max", "map", false, NULL},
    {"regex", "map", false, NULL},
    {"max_regex_pattern_bytes", "int", false, "4096"},
    {"max_regex_cell_bytes", "int", false, "65536"},
    {"audit_include_row", "bool", false, "true"},
    {"audit_columns", "string[]", false, NULL},
    {"audit_redact", "string[]", false, NULL},
    {"audit_hash_columns", "string[]", false, NULL},
    {"audit_max_bytes", "int", false, NULL},
    {"audit_max_cell_bytes", "int", false, NULL},
    {"mode", "string", false, "\"fail\""},
    {"action", "string", false, "\"fail\""},
    {"name", "string", false, "\"schema\""},
    {"message", "string", false, "\"\""},
    {"result", "string", false, "\"_schema\""},
};

static tf_arg_desc schema_infer_args[] = {
    {"rows", "int", false, "10000"},
};

static tf_arg_desc trim_args[] = {
    {"columns", "string[]", false, NULL},
};

static tf_arg_desc fill_null_args[] = {
    {"mapping", "map", true, NULL},
    {"audit", "bool", false, "false"},
    {"audit_limit", "int", false, "1000"},
    {"audit_include_row", "bool", false, "true"},
    {"audit_columns", "string[]", false, NULL},
    {"audit_redact", "string[]", false, NULL},
    {"audit_hash_columns", "string[]", false, NULL},
    {"audit_max_bytes", "size", false, "0"},
    {"audit_max_cell_bytes", "size", false, "0"},
};

static tf_arg_desc cast_args[] = {
    {"mapping", "map", true, NULL},
    {"audit", "bool", false, "false"},
    {"audit_limit", "int", false, "1000"},
    {"audit_include_row", "bool", false, "true"},
    {"audit_columns", "string[]", false, NULL},
    {"audit_redact", "string[]", false, NULL},
    {"audit_hash_columns", "string[]", false, NULL},
    {"audit_max_bytes", "size", false, "0"},
    {"audit_max_cell_bytes", "size", false, "0"},
};

static tf_arg_desc clip_args[] = {
    {"column", "string", true, NULL},
    {"min", "float", false, NULL},
    {"max", "float", false, NULL},
};

static tf_arg_desc replace_args[] = {
    {"column", "string", true, NULL},
    {"pattern", "string", true, NULL},
    {"replacement", "string", true, NULL},
    {"regex", "bool", false, "false"},
    {"audit", "bool", false, "false"},
    {"audit_limit", "int", false, "1000"},
    {"audit_include_row", "bool", false, "true"},
    {"audit_columns", "string[]", false, NULL},
    {"audit_redact", "string[]", false, NULL},
    {"audit_hash_columns", "string[]", false, NULL},
    {"audit_max_bytes", "size", false, "0"},
    {"audit_max_cell_bytes", "size", false, "0"},
};

static tf_arg_desc hash_args[] = {
    {"columns", "string[]", false, NULL},
};

static tf_arg_desc bin_args[] = {
    {"column", "string", true, NULL},
    {"boundaries", "float[]", true, NULL},
    {"missing", "string", false, "\"error\""},
    {"on_type_error", "string", false, "\"fail\""},
};

static tf_arg_desc fill_down_args[] = {
    {"columns", "string[]", false, NULL},
};

static tf_arg_desc step_args[] = {
    {"column", "string", true, NULL},
    {"func", "string", true, NULL},
    {"result", "string", false, NULL},
};

static tf_arg_desc window_args[] = {
    {"column", "string", true, NULL},
    {"size", "int", true, NULL},
    {"func", "string", true, NULL},
    {"result", "string", false, NULL},
    {"missing", "string", false, "\"error\""},
    {"on_type_error", "string", false, "\"fail\""},
};

static tf_arg_desc rolling_args[] = {
    {"column", "string", true, NULL},
    {"size", "int", true, NULL},
    {"result", "string", false, NULL},
    {"missing", "string", false, "\"error\""},
    {"on_type_error", "string", false, "\"fail\""},
};

static tf_arg_desc rolling_bool_args[] = {
    {"column", "string", true, NULL},
    {"size", "int", true, NULL},
    {"result", "string", false, NULL},
    {"nulls", "string", false, "\"ignore\""},
};

static tf_arg_desc explode_args[] = {
    {"column", "string", true, NULL},
    {"delimiter", "string", false, "\",\""},
    {"max_tokens_per_row", "int", false, NULL},
    {"max_output_rows_per_input_row", "int", false, NULL},
    {"max_output_rows_per_batch", "int", false, NULL},
    {"max_token_bytes", "int", false, NULL},
};

static tf_arg_desc split_args[] = {
    {"column", "string", true, NULL},
    {"delimiter", "string", false, "\" \""},
    {"names", "string[]", true, NULL},
};

static tf_arg_desc unpivot_args[] = {
    {"columns", "string[]", true, NULL},
    {"max_output_rows_per_input_row", "int", false, NULL},
    {"max_output_rows_per_batch", "int", false, NULL},
};

static tf_arg_desc tail_args[] = {
    {"n", "int", true, NULL},
};

static tf_arg_desc top_args[] = {
    {"n", "int", true, NULL},
    {"column", "string", true, NULL},
    {"desc", "bool", false, "true"},
    {"with_ties", "bool", false, "false"},
};

static tf_arg_desc sample_args[] = {
    {"n", "int", true, NULL},
    {"seed", "int|string", false, "0"},
};

static tf_arg_desc group_agg_args[] = {
    {"group_by", "string[]", true, NULL},
    {"aggs", "map[]", true, NULL},
    {"max_groups", "int", false, NULL},
    {"max_state_bytes", "int", false, NULL},
    {"spill_dir", "string", false, NULL},
    {"spill_memory_bytes", "int", false, NULL},
    {"spill_run_rows", "int", false, NULL},
    {"spill_output_rows", "int", false, NULL},
    {"sorted", "bool", false, "false"},
};

static tf_arg_desc frequency_args[] = {
    {"columns", "string[]", false, NULL},
    {"max_values", "int", false, NULL},
    {"max_state_bytes", "int", false, NULL},
    {"overflow", "string", false, "\"error\""},
    {"other", "string", false, "\"__other__\""},
    {"audit", "bool", false, "false"},
    {"audit_limit", "int", false, "1000"},
    {"audit_include_row", "bool", false, "true"},
    {"audit_columns", "string[]", false, NULL},
    {"audit_redact", "string[]", false, NULL},
    {"audit_hash_columns", "string[]", false, NULL},
    {"audit_max_bytes", "size", false, "0"},
    {"audit_max_cell_bytes", "size", false, "0"},
};

static tf_arg_desc datetime_args[] = {
    {"column", "string", true, NULL},
    {"extract", "string[]", false, NULL},
    {"missing", "string", false, "\"error\""},
    {"on_type_error", "string", false, "\"fail\""},
};

static tf_arg_desc pivot_args[] = {
    {"name_column", "string", true, NULL},
    {"value_column", "string", true, NULL},
    {"agg", "string", false, "\"first\""},
    {"categories", "array", false, NULL},
    {"max_categories", "int", false, NULL},
    {"sorted", "bool", false, "false"},
    {"spill_dir", "string", false, NULL},
    {"spill_memory_bytes", "int", false, NULL},
    {"spill_run_rows", "int", false, NULL},
    {"spill_output_rows", "int", false, NULL},
};

static tf_arg_desc join_args[] = {
    {"file", "string", true, NULL},
    {"on", "string", true, NULL},
    {"how", "string", false, "\"inner\""},
    {"max_lookup_rows", "int", false, NULL},
    {"max_lookup_keys", "int", false, NULL},
    {"max_lookup_bytes", "int", false, NULL},
    {"max_state_bytes", "int", false, NULL},
    {"max_matches_per_row", "int", false, NULL},
    {"max_output_rows", "int", false, NULL},
    {"spill_dir", "string", false, NULL},
    {"spill_memory_bytes", "int", false, NULL},
    {"spill_run_rows", "int", false, NULL},
    {"spill_output_rows", "int", false, NULL},
    {"sorted", "bool", false, "false"},
};

static tf_arg_desc set_file_args[] = {
    {"file", "string", true, NULL},
    {"columns", "string[]", false, NULL},
    {"max_lookup_rows", "int", false, NULL},
    {"max_lookup_keys", "int", false, NULL},
    {"max_lookup_bytes", "int", false, NULL},
    {"max_output_keys", "int", false, NULL},
    {"max_state_bytes", "int", false, NULL},
    {"spill_dir", "string", false, NULL},
    {"spill_memory_bytes", "int", false, NULL},
    {"spill_run_rows", "int", false, NULL},
    {"spill_output_rows", "int", false, NULL},
    {"sorted", "bool", false, "false"},
};

static tf_arg_desc union_args[] = {
    {"file", "string", true, NULL},
    {"columns", "string[]", false, NULL},
    {"max_lookup_rows", "int", false, NULL},
    {"max_lookup_bytes", "int", false, NULL},
    {"max_output_keys", "int", false, NULL},
    {"max_state_bytes", "int", false, NULL},
    {"spill_dir", "string", false, NULL},
    {"spill_memory_bytes", "int", false, NULL},
    {"spill_run_rows", "int", false, NULL},
    {"spill_output_rows", "int", false, NULL},
    {"sorted", "bool", false, "false"},
};

static tf_arg_desc union_all_args[] = {
    {"file", "string", true, NULL},
    {"max_lookup_rows", "int", false, NULL},
    {"max_lookup_bytes", "int", false, NULL},
};

static tf_arg_desc stack_args[] = {
    {"file", "string", true, NULL},
    {"tag", "string", false, NULL},
    {"tag_value", "string", false, NULL},
};

static tf_arg_desc lead_args[] = {
    {"column", "string", true, NULL},
    {"offset", "int", false, "1"},
    {"result", "string", false, NULL},
};

static tf_arg_desc lag_args[] = {
    {"column", "string", true, NULL},
    {"offset", "int", false, "1"},
    {"result", "string", false, NULL},
};

static tf_arg_desc shift_args[] = {
    {"column", "string", true, NULL},
    {"offset", "int", false, "1"},
    {"result", "string", false, NULL},
    {"type", "string", false, "\"lag\""},
};

static tf_arg_desc rowid_args[] = {
    {"columns", "string[]", false, NULL},
    {"result", "string", false, "\"_rowid\""},
    {"sorted", "bool", false, "false"},
    {"max_keys", "int", false, NULL},
    {"max_state_bytes", "int", false, NULL},
};

static tf_arg_desc rleid_args[] = {
    {"columns", "string[]", true, NULL},
    {"result", "string", false, "\"_rleid\""},
};

static tf_arg_desc date_trunc_args[] = {
    {"column", "string", true, NULL},
    {"trunc", "string", true, NULL},
    {"result", "string", false, NULL},
    {"missing", "string", false, "\"error\""},
    {"on_type_error", "string", false, "\"fail\""},
};

static tf_arg_desc onehot_args[] = {
    {"column", "string", true, NULL},
    {"drop", "bool", false, "false"},
    {"categories", "array", false, NULL},
    {"max_categories", "int", false, NULL},
    {"max_state_bytes", "int", false, NULL},
    {"unknown", "string", false, NULL},
};

static tf_arg_desc label_encode_args[] = {
    {"column", "string", true, NULL},
    {"result", "string", false, NULL},
    {"categories", "array", false, NULL},
    {"max_categories", "int", false, NULL},
    {"max_state_bytes", "int", false, NULL},
    {"unknown", "string", false, NULL},
};

static tf_arg_desc ewma_args[] = {
    {"column", "string", true, NULL},
    {"alpha", "float", true, NULL},
    {"result", "string", false, NULL},
    {"missing", "string", false, "\"error\""},
    {"on_type_error", "string", false, "\"fail\""},
};

static tf_arg_desc diff_args[] = {
    {"column", "string", true, NULL},
    {"order", "int", false, "1"},
    {"result", "string", false, NULL},
};

static tf_arg_desc anomaly_args[] = {
    {"column", "string", true, NULL},
    {"threshold", "float", false, "3.0"},
    {"result", "string", false, NULL},
    {"missing", "string", false, "\"error\""},
    {"on_type_error", "string", false, "\"fail\""},
};

static tf_arg_desc split_data_args[] = {
    {"ratio", "float", false, "0.8"},
    {"result", "string", false, "\"_split\""},
    {"seed", "int", false, "42"},
};

static tf_arg_desc source_name_args[] = {
    {"result", "string", false, "\"_source\""},
    {"default", "string", false, "\"\""},
};

static tf_arg_desc interpolate_args[] = {
    {"column", "string", true, NULL},
    {"method", "string", false, "\"linear\""},
    {"missing", "string", false, "\"error\""},
    {"on_type_error", "string", false, "\"fail\""},
};

static tf_arg_desc normalize_args[] = {
    {"columns", "string[]", true, NULL},
    {"method", "string", false, "\"minmax\""},
    {"missing", "string", false, "\"error\""},
    {"on_type_error", "string", false, "\"fail\""},
    {"audit", "bool", false, "false"},
    {"audit_limit", "int", false, "1000"},
    {"audit_include_row", "bool", false, "true"},
    {"audit_columns", "string[]", false, NULL},
    {"audit_redact", "string[]", false, NULL},
    {"audit_hash_columns", "string[]", false, NULL},
    {"audit_max_bytes", "size", false, "0"},
    {"audit_max_cell_bytes", "size", false, "0"},
};

static tf_arg_desc acf_args[] = {
    {"column", "string", true, NULL},
    {"lags", "int", false, "20"},
    {"missing", "string", false, "\"error\""},
    {"on_type_error", "string", false, "\"fail\""},
};

static tf_arg_desc json_extract_args[] = {
    {"column", "string", false, "\"_line\""},
    {"path", "string", true, NULL},
    {"result", "string", true, NULL},
    {"type", "string", false, "\"string\""},
};

static tf_arg_desc json_filter_args[] = {
    {"column", "string", false, "\"_line\""},
    {"path", "string", true, NULL},
    {"op", "string", false, "\"exists\""},
    {"value", "string", false, NULL},
    {"type", "string", false, "\"auto\""},
};

static tf_arg_desc json_schema_args[] = {
    {"column", "string", false, "\"_line\""},
    {"schema", "object|string|bool", true, NULL},
    {"mode", "string", false, "\"annotate\""},
    {"result", "string", false, "\"_valid\""},
    {"audit", "bool", false, "false"},
    {"audit_limit", "int", false, "1000"},
    {"audit_include_row", "bool", false, "true"},
    {"audit_columns", "string[]", false, NULL},
    {"audit_redact", "string[]", false, NULL},
    {"audit_hash_columns", "string[]", false, NULL},
    {"audit_max_bytes", "int", false, NULL},
    {"audit_max_cell_bytes", "int", false, NULL},
};

static tf_arg_desc json_flatten_args[] = {
    {"column", "string", false, "\"_line\""},
    {"fields", "object[]|string[]", true, NULL},
};

static tf_arg_desc table_encode_args[] = {
    {"max_width", "int", false, "40"},
    {"max_rows", "int", false, "0"},
};


const char *tf_memory_class_name(tf_memory_class cls) {
    switch (cls) {
        case TF_MEM_ROW_LOCAL: return "row_local";
        case TF_MEM_BOUNDED_STATE: return "bounded_state";
        case TF_MEM_KEY_STATE: return "key_state";
        case TF_MEM_BLOCKING: return "blocking";
        case TF_MEM_EXTERNAL: return "external";
        default: return "unknown";
    }
}

const char *tf_emit_class_name(tf_emit_class cls) {
    switch (cls) {
        case TF_EMIT_PER_BATCH: return "per_batch";
        case TF_EMIT_ON_FLUSH: return "on_flush";
        case TF_EMIT_SIDE_ONLY: return "side_only";
        case TF_EMIT_MIXED: return "mixed";
        default: return "unknown";
    }
}

const char *tf_schema_class_name(tf_schema_class cls) {
    switch (cls) {
        case TF_SCHEMA_STABLE: return "stable";
        case TF_SCHEMA_PARAMETRIC: return "parametric";
        case TF_SCHEMA_DATA_DEPENDENT: return "data_dependent";
        default: return "unknown";
    }
}

static tf_op_entry builtin_ops[] = {
    {
        .name = "codec.csv.decode",
        .kind = TF_OP_DECODER,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_DATA_DEPENDENT,
        .state_estimate = "O(batch_size * columns)",
        .args = csv_decode_args,
        .n_args = 20,
        .infer_schema = infer_schema_unknown,
        .create_native = (void *(*)(const cJSON *))tf_csv_decoder_create,
    },
    {
        .name = "codec.csv.encode",
        .kind = TF_OP_ENCODER,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(output_chunk)",
        .args = csv_encode_args,
        .n_args = 2,
        .infer_schema = infer_schema_sink,
        .create_native = (void *(*)(const cJSON *))tf_csv_encoder_create,
    },
    {
        .name = "codec.jsonl.decode",
        .kind = TF_OP_DECODER,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_DATA_DEPENDENT,
        .state_estimate = "O(batch_size)",
        .args = jsonl_decode_args,
        .n_args = 3,
        .infer_schema = infer_schema_unknown,
        .create_native = (void *(*)(const cJSON *))tf_jsonl_decoder_create,
    },
    {
        .name = "codec.jsonl.encode",
        .kind = TF_OP_ENCODER,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(output_chunk)",
        .args = jsonl_encode_args,
        .n_args = 0,
        .infer_schema = infer_schema_sink,
        .create_native = (void *(*)(const cJSON *))tf_jsonl_encoder_create,
    },
    {
        .name = "codec.text.decode",
        .kind = TF_OP_DECODER,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_size)",
        .args = text_decode_args,
        .n_args = 1,
        .infer_schema = infer_schema_unknown,
        .create_native = (void *(*)(const cJSON *))tf_text_decoder_create,
    },
    {
        .name = "codec.text.encode",
        .kind = TF_OP_ENCODER,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(output_chunk)",
        .args = text_encode_args,
        .n_args = 0,
        .infer_schema = infer_schema_sink,
        .create_native = (void *(*)(const cJSON *))tf_text_encoder_create,
    },
    {
        .name = "grep",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(batch_rows)",
        .args = grep_args,
        .n_args = 4,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_grep_create,
    },
    {
        .name = "filter",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(batch_rows * columns)",
        .args = filter_args,
        .n_args = 9,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_filter_create,
    },
    {
        .name = "select",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows * selected_columns)",
        .args = select_args,
        .n_args = 1,
        .infer_schema = infer_schema_select,
        .create_native = (void *(*)(const cJSON *))tf_select_create,
    },
    {
        .name = "rename",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(columns)",
        .args = rename_args,
        .n_args = 1,
        .infer_schema = infer_schema_rename,
        .create_native = (void *(*)(const cJSON *))tf_rename_create,
    },
    {
        .name = "head",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(1)",
        .args = head_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_head_create,
    },
    {
        .name = "slice-head",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(1)",
        .args = head_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_head_create,
    },
    {
        .name = "skip",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(1)",
        .args = skip_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_skip_create,
    },
    {
        .name = "derive",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows * derived_columns)",
        .args = derive_args,
        .n_args = 1,
        .infer_schema = infer_schema_derive,
        .create_native = (void *(*)(const cJSON *))tf_derive_create,
    },

    {
        .name = "source-name",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows)",
        .args = source_name_args,
        .n_args = 2,
        .infer_schema = infer_schema_source_name,
        .create_native = (void *(*)(const cJSON *))tf_source_name_create,
    },

    {
        .name = "across",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows * selected_columns * functions)",
        .args = across_args,
        .n_args = 5,
        .infer_schema = infer_schema_across,
        .create_native = (void *(*)(const cJSON *))tf_across_create,
    },
    {
        .name = "stats",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(columns * requested_stats)",
        .args = stats_args,
        .n_args = 1,
        .infer_schema = infer_schema_stats,
        .create_native = (void *(*)(const cJSON *))tf_stats_create,
    },
    {
        .name = "scan",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(columns * profile_stats)",
        .args = scan_args,
        .n_args = 1,
        .infer_schema = infer_schema_stats,
        .create_native = (void *(*)(const cJSON *))tf_scan_create,
    },
    {
        .name = "unique",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_KEY_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(1) sorted, O(distinct_keys) unsorted or capped by max_keys/max_state_bytes, or O(spill_run_rows * columns) RAM with spill_dir",
        .args = unique_args,
        .n_args = 8,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_unique_create,
    },
    {
        .name = "sort",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BLOCKING,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(input_rows)",
        .args = sort_args,
        .n_args = 5,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_sort_create,
    },
    /* ---- Aliases ---- */
    {
        .name = "reorder",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows * selected_columns)",
        .args = select_args,
        .n_args = 1,
        .infer_schema = infer_schema_select,
        .create_native = (void *(*)(const cJSON *))tf_select_create,
    },
    {
        .name = "relocate",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows * columns)",
        .args = relocate_args,
        .n_args = 3,
        .infer_schema = infer_schema_relocate,
        .create_native = (void *(*)(const cJSON *))tf_relocate_create,
    },
    {
        .name = "dedup",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_KEY_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(1) sorted, O(distinct_keys) unsorted or capped by max_keys/max_state_bytes",
        .args = unique_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_unique_create,
    },
    /* ---- Simple streaming ---- */
    {
        .name = "validate",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_MIXED,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows * columns)",
        .args = validate_args,
        .n_args = 16,
        .infer_schema = infer_schema_validate,
        .create_native = (void *(*)(const cJSON *))tf_validate_create,
    },
    {
        .name = "assert",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_MIXED,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows * columns) row mode, O(1) aggregate mode",
        .args = assert_args,
        .n_args = 19,
        .infer_schema = infer_schema_assert,
        .create_native = (void *(*)(const cJSON *))tf_assert_create,
    },
    {
        .name = "quarantine",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_MIXED,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(batch_rows * columns)",
        .args = quarantine_args,
        .n_args = 9,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_quarantine_create,
    },
    {
        .name = "tee",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_MIXED,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(batch_rows * selected_columns)",
        .args = tee_args,
        .n_args = 13,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_tee_create,
    },
    {
        .name = "schema",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_MIXED,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows * declared_columns)",
        .args = schema_args,
        .n_args = 13,
        .infer_schema = infer_schema_schema,
        .create_native = (void *(*)(const cJSON *))tf_schema_create,
    },
    {
        .name = "schema-infer",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(columns)",
        .args = schema_infer_args,
        .n_args = 1,
        .infer_schema = infer_schema_schema_infer,
        .create_native = (void *(*)(const cJSON *))tf_schema_infer_create,
    },
    {
        .name = "trim",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(batch_rows * columns)",
        .args = trim_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_trim_create,
    },
    {
        .name = "fill-null",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(batch_rows * columns)",
        .args = fill_null_args,
        .n_args = 9,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_fill_null_create,
    },
    {
        .name = "cast",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows * columns)",
        .args = cast_args,
        .n_args = 9,
        .infer_schema = infer_schema_passthrough,  /* type changes at runtime */
        .create_native = (void *(*)(const cJSON *))tf_cast_create,
    },
    {
        .name = "clip",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(batch_rows)",
        .args = clip_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_clip_create,
    },
    {
        .name = "replace",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(batch_rows)",
        .args = replace_args,
        .n_args = 12,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_replace_create,
    },
    {
        .name = "hash",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows * selected_columns)",
        .args = hash_args,
        .n_args = 1,
        .infer_schema = infer_schema_add_hash,
        .create_native = (void *(*)(const cJSON *))tf_hash_create,
    },
    {
        .name = "bin",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(boundaries)",
        .args = bin_args,
        .n_args = 4,
        .infer_schema = infer_schema_passthrough,  /* adds column at runtime */
        .create_native = (void *(*)(const cJSON *))tf_bin_create,
    },
    /* ---- Stateful streaming ---- */
    {
        .name = "fill-down",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(columns)",
        .args = fill_down_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_fill_down_create,
    },
    {
        .name = "step",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(1)",
        .args = step_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,  /* adds column at runtime */
        .create_native = (void *(*)(const cJSON *))tf_step_create,
    },
    {
        .name = "window",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(window_size)",
        .args = window_args,
        .n_args = 6,
        .infer_schema = infer_schema_passthrough,  /* adds column at runtime */
        .create_native = (void *(*)(const cJSON *))tf_window_create,
    },
    {
        .name = "rolling-sum",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(window_size)",
        .args = rolling_args,
        .n_args = 5,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_rolling_sum_create,
    },
    {
        .name = "rolling-mean",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(window_size)",
        .args = rolling_args,
        .n_args = 5,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_rolling_mean_create,
    },
    {
        .name = "rolling-min",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(window_size)",
        .args = rolling_args,
        .n_args = 5,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_rolling_min_create,
    },
    {
        .name = "rolling-max",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(window_size)",
        .args = rolling_args,
        .n_args = 5,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_rolling_max_create,
    },
    {
        .name = "rolling-any",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(window_size)",
        .args = rolling_bool_args,
        .n_args = 4,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_rolling_any_create,
    },
    {
        .name = "rolling-all",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(window_size)",
        .args = rolling_bool_args,
        .n_args = 4,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_rolling_all_create,
    },
    /* ---- Row-multiplying streaming ---- */
    {
        .name = "explode",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(batch_rows)",
        .args = explode_args,
        .n_args = 2,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_explode_create,
    },
    {
        .name = "split",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows * output_columns)",
        .args = split_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,  /* adds columns at runtime */
        .create_native = (void *(*)(const cJSON *))tf_split_create,
    },
    {
        .name = "unpivot",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows * unpivot_columns)",
        .args = unpivot_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,  /* schema changes at runtime */
        .create_native = (void *(*)(const cJSON *))tf_unpivot_create,
    },
    /* ---- Buffering ---- */
    {
        .name = "tail",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(n)",
        .args = tail_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_tail_create,
    },
    {
        .name = "slice-tail",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(n)",
        .args = tail_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_tail_create,
    },
    {
        .name = "top",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(n)",
        .args = top_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_top_create,
    },
    {
        .name = "top-k",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(n)",
        .args = top_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_top_k_create,
    },
    {
        .name = "bottom-k",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(n)",
        .args = top_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_bottom_k_create,
    },
    {
        .name = "slice-min",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(n)",
        .args = top_args,
        .n_args = 4,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_slice_min_create,
    },
    {
        .name = "slice-max",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(n)",
        .args = top_args,
        .n_args = 4,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_slice_max_create,
    },
    {
        .name = "sample",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(n)",
        .args = sample_args,
        .n_args = 2,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_sample_create,
    },
    {
        .name = "group-agg",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_KEY_STATE,
        .emit_class = TF_EMIT_MIXED,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(1) sorted, O(distinct_groups) unsorted or capped by max_groups/max_state_bytes, or O(spill_run_rows * columns) RAM with spill_dir",
        .args = group_agg_args,
        .n_args = 9,
        .infer_schema = infer_schema_group_agg,
        .create_native = (void *(*)(const cJSON *))tf_group_agg_create,
    },
    {
        .name = "frequency",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_KEY_STATE,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(distinct_values) or capped by max_values/max_state_bytes",
        .args = frequency_args,
        .n_args = 13,
        .infer_schema = infer_schema_frequency,
        .create_native = (void *(*)(const cJSON *))tf_frequency_create,
    },
    /* ---- Complex ---- */
    {
        .name = "datetime",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows)",
        .args = datetime_args,
        .n_args = 4,
        .infer_schema = infer_schema_passthrough,  /* adds columns at runtime */
        .create_native = (void *(*)(const cJSON *))tf_datetime_create,
    },
    {
        .name = "json-extract",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows + path_segments)",
        .args = json_extract_args,
        .n_args = 4,
        .infer_schema = infer_schema_json_extract,
        .create_native = (void *(*)(const cJSON *))tf_json_extract_create,
    },
    {
        .name = "json-filter",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(batch_rows + path_segments)",
        .args = json_filter_args,
        .n_args = 5,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_json_filter_create,
    },
    {
        .name = "json-schema",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows + schema_nodes)",
        .args = json_schema_args,
        .n_args = 12,
        .infer_schema = infer_schema_json_schema,
        .create_native = (void *(*)(const cJSON *))tf_json_schema_create,
    },
    {
        .name = "json-flatten",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows * fields + path_segments)",
        .args = json_flatten_args,
        .n_args = 2,
        .infer_schema = infer_schema_json_flatten,
        .create_native = (void *(*)(const cJSON *))tf_json_flatten_create,
    },
    {
        .name = "flatten",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows * fields)",
        .args = NULL,
        .n_args = 0,
        .infer_schema = infer_schema_passthrough,
        .create_native = NULL,  /* passthrough — no-op at runtime */
    },
    /* ---- Pivot / Join ---- */
    {
        .name = "pivot",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BLOCKING,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_DATA_DEPENDENT,
        .state_estimate = "O(input_rows + distinct_categories), O(current_group + categories) with categories+sorted=true, or external spill with categories/max_categories",
        .args = pivot_args,
        .n_args = 10,
        .infer_schema = infer_schema_passthrough,  /* schema changes at runtime */
        .create_native = (void *(*)(const cJSON *))tf_pivot_create,
    },
    {
        .name = "join",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_FS | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_KEY_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_DATA_DEPENDENT,
        .state_estimate = "O(current_lookup_run) sorted, O(lookup_rows + lookup_keys + output_batch) unsorted or capped by max_lookup_*/max_state_bytes/max_output_rows, or O(spill_run_rows * columns + max_matches_per_row) RAM with spill_dir",
        .args = join_args,
        .n_args = 14,
        .infer_schema = infer_schema_passthrough,  /* schema depends on lookup file */
        .create_native = (void *(*)(const cJSON *))tf_join_create,
    },
    {
        .name = "semi-join",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_FS | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_KEY_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(1) sorted, O(lookup_keys) unsorted or capped by max_lookup_*/max_state_bytes/max_output_rows, or O(spill_run_rows * columns) RAM with spill_dir",
        .args = join_args,
        .n_args = 14,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_semi_join_create,
    },
    {
        .name = "anti-join",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_FS | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_KEY_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(1) sorted, O(lookup_keys) unsorted or capped by max_lookup_*/max_state_bytes/max_output_rows, or O(spill_run_rows * columns) RAM with spill_dir",
        .args = join_args,
        .n_args = 14,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_anti_join_create,
    },
    {
        .name = "intersect",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_FS | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_KEY_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(1) sorted, O(lookup_keys + emitted_keys) unsorted or capped by max_lookup_*/max_output_keys/max_state_bytes",
        .args = set_file_args,
        .n_args = 12,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_intersect_create,
    },
    {
        .name = "setdiff",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_FS | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_KEY_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(1) sorted, O(lookup_keys + emitted_keys) unsorted or capped by max_lookup_*/max_output_keys/max_state_bytes",
        .args = set_file_args,
        .n_args = 12,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_setdiff_create,
    },
    {
        .name = "intersect-all",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_FS | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_KEY_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(lookup_keys) unsorted with max_lookup_keys/max_state_bytes; O(spill_run_rows * columns) RAM with spill_dir; O(1) current-run state with sorted=true; emits min(left_count, lookup_count) rows per key",
        .args = set_file_args,
        .n_args = 12,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_intersect_all_create,
    },
    {
        .name = "setdiff-all",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_FS | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_KEY_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(lookup_keys) unsorted with max_lookup_keys/max_state_bytes; O(spill_run_rows * columns) RAM with spill_dir; O(1) current-run state with sorted=true; emits max(left_count - lookup_count, 0) rows per key",
        .args = set_file_args,
        .n_args = 12,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_setdiff_all_create,
    },
    {
        .name = "union",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_FS | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_KEY_STATE,
        .emit_class = TF_EMIT_MIXED,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(emitted_keys), capped by max_output_keys/max_state_bytes; O(previous_left_key + current_file_key) with sorted=true; O(spill_run_rows * columns) RAM with spill_dir",
        .args = union_args,
        .n_args = 11,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_union_create,
    },
    {
        .name = "union-all",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_FS | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_MIXED,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(batch_rows)",
        .args = union_all_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_union_all_create,
    },
    {
        .name = "stack",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_FS | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BLOCKING,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(input_rows + file_rows)",
        .args = stack_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_stack_create,
    },
    {
        .name = "lead",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(offset)",
        .args = lead_args,
        .n_args = 3,
        .infer_schema = infer_schema_lead,
        .create_native = (void *(*)(const cJSON *))tf_lead_create,
    },
    {
        .name = "lag",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(offset * row_width)",
        .args = lag_args,
        .n_args = 3,
        .infer_schema = infer_schema_lag,
        .create_native = (void *(*)(const cJSON *))tf_lag_create,
    },
    {
        .name = "shift",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(offset * row_width)",
        .args = shift_args,
        .n_args = 4,
        .infer_schema = infer_schema_shift,
        .create_native = (void *(*)(const cJSON *))tf_shift_create,
    },
    {
        .name = "rowid",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_KEY_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(1) global/sorted, O(distinct_keys) unsorted or capped by max_keys/max_state_bytes",
        .args = rowid_args,
        .n_args = 5,
        .infer_schema = infer_schema_rowid,
        .create_native = (void *(*)(const cJSON *))tf_rowid_create,
    },
    {
        .name = "rleid",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(key_width)",
        .args = rleid_args,
        .n_args = 2,
        .infer_schema = infer_schema_rleid,
        .create_native = (void *(*)(const cJSON *))tf_rleid_create,
    },
    {
        .name = "date-trunc",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_ROW_LOCAL,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(batch_rows)",
        .args = date_trunc_args,
        .n_args = 5,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_date_trunc_create,
    },
    {
        .name = "onehot",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_KEY_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_DATA_DEPENDENT,
        .state_estimate = "O(distinct_categories) or capped by max_categories/max_state_bytes",
        .args = onehot_args,
        .n_args = 6,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_onehot_create,
    },
    {
        .name = "label-encode",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_KEY_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(distinct_categories) or capped by max_categories/max_state_bytes",
        .args = label_encode_args,
        .n_args = 6,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_label_encode_create,
    },
    {
        .name = "ewma",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(1)",
        .args = ewma_args,
        .n_args = 5,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_ewma_create,
    },
    {
        .name = "diff",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(order)",
        .args = diff_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_diff_create,
    },
    {
        .name = "anomaly",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(1)",
        .args = anomaly_args,
        .n_args = 5,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_anomaly_create,
    },
    {
        .name = "split-data",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE,
        .memory_class = TF_MEM_BOUNDED_STATE,
        .emit_class = TF_EMIT_PER_BATCH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(1)",
        .args = split_data_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_split_data_create,
    },
    {
        .name = "interpolate",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BLOCKING,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(input_rows)",
        .args = interpolate_args,
        .n_args = 4,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_interpolate_create,
    },
    {
        .name = "normalize",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BLOCKING,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(input_rows)",
        .args = normalize_args,
        .n_args = 12,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_normalize_create,
    },
    {
        .name = "acf",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BLOCKING,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_PARAMETRIC,
        .state_estimate = "O(input_rows)",
        .args = acf_args,
        .n_args = 4,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_acf_create,
    },
    {
        .name = "codec.table.encode",
        .kind = TF_OP_ENCODER,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .memory_class = TF_MEM_BLOCKING,
        .emit_class = TF_EMIT_ON_FLUSH,
        .schema_class = TF_SCHEMA_STABLE,
        .state_estimate = "O(input_rows)",
        .args = table_encode_args,
        .n_args = 2,
        .infer_schema = infer_schema_sink,
        .create_native = (void *(*)(const cJSON *))tf_table_encoder_create,
    },
};

static const size_t n_builtin_ops = sizeof(builtin_ops) / sizeof(builtin_ops[0]);

/* ---- Public API ---- */

const tf_op_entry *tf_op_registry_find(const char *name) {
    for (size_t i = 0; i < n_builtin_ops; i++) {
        if (strcmp(builtin_ops[i].name, name) == 0)
            return &builtin_ops[i];
    }
    return NULL;
}

size_t tf_op_registry_count(void) {
    return n_builtin_ops;
}

const tf_op_entry *tf_op_registry_get(size_t index) {
    if (index >= n_builtin_ops) return NULL;
    return &builtin_ops[index];
}
