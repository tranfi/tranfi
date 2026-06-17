/*
 * op_across.c -- Apply deterministic row-local functions across selected columns.
 *
 * Config:
 *   {"columns":["starts_with(score_)"], "fn":"round"}
 *   {"columns":["name"], "functions":["trim","lower"], "replace":false,
 *    "names":"{col}_{fn}"}
 *
 * This is a compact Tranfi subset of dplyr across(): selector expansion plus a
 * whitelist of scalar functions. It is not a lambda or grouped-evaluation runtime.
 */

#include "internal.h"
#include "cJSON.h"
#include <ctype.h>
#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

typedef enum {
    ACROSS_FN_TRIM,
    ACROSS_FN_LOWER,
    ACROSS_FN_UPPER,
    ACROSS_FN_ABS,
    ACROSS_FN_ROUND,
    ACROSS_FN_FLOOR,
    ACROSS_FN_CEIL,
    ACROSS_FN_SQRT,
    ACROSS_FN_LOG,
    ACROSS_FN_EXP
} across_fn_kind;

typedef struct {
    char           **selectors;
    size_t           n_selectors;
    char           **fn_names;
    across_fn_kind  *fns;
    size_t           n_fns;
    char            *names_template;
    int              replace;
} across_state;

static int across_write_error(tf_side_channels *side, const char *msg,
                              const char *column, const char *fn) {
    if (!side || !side->errors) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    cJSON_AddStringToObject(obj, "op", "across");
    cJSON_AddStringToObject(obj, "error", msg ? msg : "across error");
    if (column) cJSON_AddStringToObject(obj, "column", column);
    if (fn) cJSON_AddStringToObject(obj, "function", fn);
    char *line = cJSON_PrintUnformatted(obj);
    cJSON_Delete(obj);
    if (!line) return TF_ERROR;
    int rc = tf_buffer_write_line(side->errors, line);
    free(line);
    return rc;
}

static int across_fn_from_name(const char *name, across_fn_kind *out) {
    if (!name || !out) return TF_ERROR;
    if (strcasecmp(name, "trim") == 0) { *out = ACROSS_FN_TRIM; return TF_OK; }
    if (strcasecmp(name, "lower") == 0 || strcasecmp(name, "tolower") == 0) { *out = ACROSS_FN_LOWER; return TF_OK; }
    if (strcasecmp(name, "upper") == 0 || strcasecmp(name, "toupper") == 0) { *out = ACROSS_FN_UPPER; return TF_OK; }
    if (strcasecmp(name, "abs") == 0) { *out = ACROSS_FN_ABS; return TF_OK; }
    if (strcasecmp(name, "round") == 0) { *out = ACROSS_FN_ROUND; return TF_OK; }
    if (strcasecmp(name, "floor") == 0) { *out = ACROSS_FN_FLOOR; return TF_OK; }
    if (strcasecmp(name, "ceil") == 0 || strcasecmp(name, "ceiling") == 0) { *out = ACROSS_FN_CEIL; return TF_OK; }
    if (strcasecmp(name, "sqrt") == 0) { *out = ACROSS_FN_SQRT; return TF_OK; }
    if (strcasecmp(name, "log") == 0) { *out = ACROSS_FN_LOG; return TF_OK; }
    if (strcasecmp(name, "exp") == 0) { *out = ACROSS_FN_EXP; return TF_OK; }
    return TF_ERROR;
}

static int across_fn_is_string(across_fn_kind fn) {
    return fn == ACROSS_FN_TRIM || fn == ACROSS_FN_LOWER || fn == ACROSS_FN_UPPER;
}

static int across_fn_is_numeric(across_fn_kind fn) {
    return fn == ACROSS_FN_ABS || fn == ACROSS_FN_ROUND || fn == ACROSS_FN_FLOOR ||
           fn == ACROSS_FN_CEIL || fn == ACROSS_FN_SQRT || fn == ACROSS_FN_LOG ||
           fn == ACROSS_FN_EXP;
}

static tf_type across_output_type(across_fn_kind fn, tf_type input) {
    if (across_fn_is_string(fn)) return input == TF_TYPE_STRING ? TF_TYPE_STRING : TF_TYPE_NULL;
    if (!across_fn_is_numeric(fn)) return TF_TYPE_NULL;
    if (!(input == TF_TYPE_INT64 || input == TF_TYPE_FLOAT64)) return TF_TYPE_NULL;
    if (fn == ACROSS_FN_ABS) return input;
    if (fn == ACROSS_FN_ROUND || fn == ACROSS_FN_FLOOR || fn == ACROSS_FN_CEIL) return TF_TYPE_INT64;
    return TF_TYPE_FLOAT64;
}

static int across_set_string_transform(tf_batch *out, size_t row, size_t col,
                                       across_fn_kind fn, const char *s) {
    if (!s) return tf_batch_set_null(out, row, col);
    const char *start = s;
    size_t len = strlen(s);
    if (fn == ACROSS_FN_TRIM) {
        while (*start && isspace((unsigned char)*start)) start++;
        len = strlen(start);
        while (len > 0 && isspace((unsigned char)start[len - 1])) len--;
    }
    size_t cap = 0;
    if (tf_size_add(len, 1, &cap) != TF_OK) return TF_ERROR;
    char *buf = tf_mallocarray_checked(cap, sizeof(char));
    if (!buf) return TF_ERROR;
    for (size_t i = 0; i < len; i++) {
        unsigned char ch = (unsigned char)start[i];
        if (fn == ACROSS_FN_LOWER) buf[i] = (char)tolower(ch);
        else if (fn == ACROSS_FN_UPPER) buf[i] = (char)toupper(ch);
        else buf[i] = (char)ch;
    }
    buf[len] = '\0';
    int rc = tf_batch_set_string(out, row, col, buf);
    free(buf);
    return rc;
}

static int across_get_numeric(const tf_batch *in, size_t row, size_t col, double *out) {
    if (in->col_types[col] == TF_TYPE_INT64) {
        *out = (double)tf_batch_get_int64(in, row, col);
        return TF_OK;
    }
    if (in->col_types[col] == TF_TYPE_FLOAT64) {
        *out = tf_batch_get_float64(in, row, col);
        return TF_OK;
    }
    return TF_ERROR;
}

static int across_double_to_i64(double v, int64_t *out) {
    if (!isfinite(v) || v < (double)INT64_MIN || v > (double)INT64_MAX) return TF_ERROR;
    *out = (int64_t)v;
    return TF_OK;
}

static int across_apply_cell(tf_batch *out, size_t out_row, size_t out_col,
                             const tf_batch *in, size_t in_row, size_t in_col,
                             across_fn_kind fn) {
    if (tf_batch_is_null(in, in_row, in_col)) return tf_batch_set_null(out, out_row, out_col);
    if (across_fn_is_string(fn)) {
        return across_set_string_transform(out, out_row, out_col, fn,
                                           tf_batch_get_string(in, in_row, in_col));
    }

    if (fn == ACROSS_FN_ABS && in->col_types[in_col] == TF_TYPE_INT64) {
        int64_t v = tf_batch_get_int64(in, in_row, in_col);
        if (v == INT64_MIN) return tf_batch_set_null(out, out_row, out_col);
        return tf_batch_set_int64(out, out_row, out_col, v < 0 ? -v : v);
    }

    double v = 0.0;
    if (across_get_numeric(in, in_row, in_col, &v) != TF_OK) return tf_batch_set_null(out, out_row, out_col);

    if (fn == ACROSS_FN_ABS) {
        return tf_batch_set_float64(out, out_row, out_col, fabs(v));
    } else if (fn == ACROSS_FN_ROUND || fn == ACROSS_FN_FLOOR || fn == ACROSS_FN_CEIL) {
        double rv = fn == ACROSS_FN_ROUND ? round(v) : (fn == ACROSS_FN_FLOOR ? floor(v) : ceil(v));
        int64_t iv = 0;
        if (across_double_to_i64(rv, &iv) != TF_OK) return tf_batch_set_null(out, out_row, out_col);
        return tf_batch_set_int64(out, out_row, out_col, iv);
    } else if (fn == ACROSS_FN_SQRT) {
        if (v < 0.0) return tf_batch_set_null(out, out_row, out_col);
        return tf_batch_set_float64(out, out_row, out_col, sqrt(v));
    } else if (fn == ACROSS_FN_LOG) {
        if (v <= 0.0) return tf_batch_set_null(out, out_row, out_col);
        return tf_batch_set_float64(out, out_row, out_col, log(v));
    } else if (fn == ACROSS_FN_EXP) {
        double ev = exp(v);
        if (!isfinite(ev)) return tf_batch_set_null(out, out_row, out_col);
        return tf_batch_set_float64(out, out_row, out_col, ev);
    }
    return tf_batch_set_null(out, out_row, out_col);
}

static int token_matches(const char *p, const char *tok) {
    return strncmp(p, tok, strlen(tok)) == 0;
}

static int across_add_len(size_t *len, size_t add) {
    if (!len) return TF_ERROR;
    return tf_size_add(*len, add, len);
}

static char *across_format_name(const char *tmpl, const char *col, const char *fn) {
    if (!tmpl || !tmpl[0]) tmpl = "{col}_{fn}";
    if (!col) col = "";
    if (!fn) fn = "";
    size_t len = 0;
    for (const char *p = tmpl; *p;) {
        if (token_matches(p, "{.col}")) {
            if (across_add_len(&len, strlen(col)) != TF_OK) return NULL;
            p += 6;
        } else if (token_matches(p, "{col}")) {
            if (across_add_len(&len, strlen(col)) != TF_OK) return NULL;
            p += 5;
        } else if (token_matches(p, "{.fn}")) {
            if (across_add_len(&len, strlen(fn)) != TF_OK) return NULL;
            p += 5;
        } else if (token_matches(p, "{fn}")) {
            if (across_add_len(&len, strlen(fn)) != TF_OK) return NULL;
            p += 4;
        } else {
            if (across_add_len(&len, 1) != TF_OK) return NULL;
            p++;
        }
    }
    size_t alloc_len = 0;
    if (tf_size_add(len, 1, &alloc_len) != TF_OK) return NULL;
    char *out = tf_mallocarray_checked(alloc_len, sizeof(char));
    if (!out) return NULL;
    char *w = out;
    for (const char *p = tmpl; *p;) {
        const char *rep = NULL;
        size_t tok_len = 0;
        if (token_matches(p, "{.col}")) { rep = col; tok_len = 6; }
        else if (token_matches(p, "{col}")) { rep = col; tok_len = 5; }
        else if (token_matches(p, "{.fn}")) { rep = fn; tok_len = 5; }
        else if (token_matches(p, "{fn}")) { rep = fn; tok_len = 4; }
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

static int across_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    across_state *st = self->state;
    *out = NULL;

    int rc = TF_ERROR;
    int *indices = NULL;
    size_t n_indices = 0;
    char *selector_error = NULL;
    int *selected_pos = NULL;
    tf_type *out_types = NULL;
    char **append_names = NULL;
    size_t *append_src = NULL;
    size_t *append_fn = NULL;
    tf_batch *ob = NULL;
    size_t n_appended = 0;
    size_t out_n_cols = 0;

    if (tf_column_selectors_resolve(st->selectors, st->n_selectors,
                                    in->col_names, in->col_types, in->n_cols,
                                    &indices, &n_indices, &selector_error) != TF_OK) {
        if (across_write_error(side, selector_error ? selector_error : "selector resolution failed", NULL, NULL) != TF_OK)
            goto done;
        goto done;
    }

    size_t selected_pos_count = in->n_cols ? in->n_cols : 1;
    selected_pos = tf_mallocarray_checked(selected_pos_count, sizeof(int));
    if (!selected_pos) goto done;
    for (size_t i = 0; i < selected_pos_count; i++) selected_pos[i] = -1;
    for (size_t i = 0; i < n_indices; i++) {
        if (indices[i] < 0 || (size_t)indices[i] >= in->n_cols) goto done;
        selected_pos[indices[i]] = (int)i;
    }

    if (st->replace && st->n_fns != 1) {
        if (across_write_error(side, "replace=true requires exactly one function", NULL, NULL) != TF_OK)
            goto done;
        goto done;
    }

    if (!st->replace && tf_size_mul(n_indices, st->n_fns, &n_appended) != TF_OK) goto done;
    if (tf_size_add(in->n_cols, n_appended, &out_n_cols) != TF_OK) goto done;
    out_types = tf_callocarray_checked(out_n_cols ? out_n_cols : 1, sizeof(tf_type));
    append_names = n_appended ? tf_callocarray_checked(n_appended, sizeof(char *)) : NULL;
    append_src = n_appended ? tf_callocarray_checked(n_appended, sizeof(size_t)) : NULL;
    append_fn = n_appended ? tf_callocarray_checked(n_appended, sizeof(size_t)) : NULL;
    if (!out_types || (n_appended && (!append_names || !append_src || !append_fn))) goto done;

    for (size_t c = 0; c < in->n_cols; c++) out_types[c] = in->col_types[c];
    if (st->replace) {
        for (size_t i = 0; i < n_indices; i++) {
            size_t ci = (size_t)indices[i];
            tf_type t = across_output_type(st->fns[0], in->col_types[ci]);
            if (t == TF_TYPE_NULL) {
                if (across_write_error(side, "function is incompatible with column type",
                                       in->col_names[ci], st->fn_names[0]) != TF_OK)
                    goto done;
                goto done;
            }
            out_types[ci] = t;
        }
    } else {
        size_t a = 0;
        for (size_t i = 0; i < n_indices; i++) {
            size_t ci = (size_t)indices[i];
            for (size_t f = 0; f < st->n_fns; f++) {
                tf_type t = across_output_type(st->fns[f], in->col_types[ci]);
                if (t == TF_TYPE_NULL) {
                    if (across_write_error(side, "function is incompatible with column type",
                                           in->col_names[ci], st->fn_names[f]) != TF_OK)
                        goto done;
                    goto done;
                }
                append_src[a] = ci;
                append_fn[a] = f;
                append_names[a] = across_format_name(st->names_template, in->col_names[ci], st->fn_names[f]);
                if (!append_names[a]) goto done;
                out_types[in->n_cols + a] = t;
                a++;
            }
        }
    }

    ob = tf_batch_create(out_n_cols, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) goto done;
    for (size_t c = 0; c < in->n_cols; c++) {
        if (tf_batch_set_schema(ob, c, in->col_names[c], out_types[c]) != TF_OK) goto done;
    }
    for (size_t a = 0; a < n_appended; a++) {
        if (tf_batch_set_schema(ob, in->n_cols + a, append_names[a], out_types[in->n_cols + a]) != TF_OK) goto done;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        size_t need_rows = 0;
        if (tf_size_add(r, 1, &need_rows) != TF_OK ||
            tf_batch_ensure_capacity(ob, need_rows) != TF_OK)
            goto done;
        for (size_t c = 0; c < in->n_cols; c++) {
            int write_rc = (st->replace && selected_pos[c] >= 0)
                ? across_apply_cell(ob, r, c, in, r, c, st->fns[0])
                : tf_batch_copy_cell(ob, r, c, in, r, c);
            if (write_rc != TF_OK) goto done;
        }
        for (size_t a = 0; a < n_appended; a++) {
            if (across_apply_cell(ob, r, in->n_cols + a, in, r, append_src[a], st->fns[append_fn[a]]) != TF_OK)
                goto done;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) goto done;
    }

    *out = ob;
    ob = NULL;
    rc = TF_OK;

done:
    if (ob) tf_batch_free(ob);
    for (size_t k = 0; k < n_appended; k++) free(append_names ? append_names[k] : NULL);
    free(out_types);
    free(append_names);
    free(append_src);
    free(append_fn);
    free(selected_pos);
    free(indices);
    free(selector_error);
    return rc;
}

static int across_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void across_destroy(tf_step *self) {
    across_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_selectors; i++) free(st->selectors[i]);
        free(st->selectors);
        for (size_t i = 0; i < st->n_fns; i++) free(st->fn_names[i]);
        free(st->fn_names);
        free(st->fns);
        free(st->names_template);
        free(st);
    }
    free(self);
}

static int parse_string_array(const cJSON *arr, char ***out_items, size_t *out_n) {
    if (!cJSON_IsArray(arr)) return TF_ERROR;
    int n = cJSON_GetArraySize((cJSON *)arr);
    if (n <= 0) return TF_ERROR;
    char **items = tf_callocarray_checked((size_t)n, sizeof(char *));
    if (!items) return TF_ERROR;
    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem((cJSON *)arr, i);
        if (!cJSON_IsString(item) || !item->valuestring[0]) {
            for (int j = 0; j < i; j++) free(items[j]);
            free(items);
            return TF_ERROR;
        }
        items[i] = strdup(item->valuestring);
        if (!items[i]) {
            for (int j = 0; j < i; j++) free(items[j]);
            free(items);
            return TF_ERROR;
        }
    }
    *out_items = items;
    *out_n = (size_t)n;
    return TF_OK;
}

tf_step *tf_across_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!cJSON_IsArray(cols)) return NULL;

    across_state *st = calloc(1, sizeof(across_state));
    if (!st) return NULL;
    if (parse_string_array(cols, &st->selectors, &st->n_selectors) != TF_OK) goto fail;

    cJSON *functions = cJSON_GetObjectItemCaseSensitive(args, "functions");
    if (!functions) functions = cJSON_GetObjectItemCaseSensitive(args, "fns");
    cJSON *fn = cJSON_GetObjectItemCaseSensitive(args, "fn");
    if (functions) {
        if (cJSON_IsString(functions)) {
            st->fn_names = tf_callocarray_checked(1, sizeof(char *));
            st->fns = tf_callocarray_checked(1, sizeof(across_fn_kind));
            if (!st->fn_names || !st->fns) goto fail;
            st->fn_names[0] = strdup(functions->valuestring);
            st->n_fns = 1;
        } else if (parse_string_array(functions, &st->fn_names, &st->n_fns) != TF_OK) {
            goto fail;
        }
    } else if (cJSON_IsString(fn)) {
        st->fn_names = tf_callocarray_checked(1, sizeof(char *));
        st->fns = tf_callocarray_checked(1, sizeof(across_fn_kind));
        if (!st->fn_names || !st->fns) goto fail;
        st->fn_names[0] = strdup(fn->valuestring);
        st->n_fns = 1;
    } else {
        goto fail;
    }

    if (!st->fns) {
        st->fns = tf_callocarray_checked(st->n_fns, sizeof(across_fn_kind));
        if (!st->fns) goto fail;
    }
    for (size_t i = 0; i < st->n_fns; i++) {
        if (!st->fn_names[i] || across_fn_from_name(st->fn_names[i], &st->fns[i]) != TF_OK) goto fail;
    }

    cJSON *names = cJSON_GetObjectItemCaseSensitive(args, "names");
    cJSON *replace = cJSON_GetObjectItemCaseSensitive(args, "replace");
    st->replace = cJSON_IsBool(replace) ? cJSON_IsTrue(replace) : (st->n_fns == 1 && !cJSON_IsString(names));
    if (st->replace && st->n_fns != 1) goto fail;
    const char *tmpl = cJSON_IsString(names) ? names->valuestring : (st->replace ? "{col}" : "{col}_{fn}");
    st->names_template = strdup(tmpl);
    if (!st->names_template) goto fail;

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) goto fail;
    step->process = across_process;
    step->flush = across_flush;
    step->destroy = across_destroy;
    step->state = st;
    return step;

fail:
    if (st) {
        for (size_t i = 0; i < st->n_selectors; i++) free(st->selectors ? st->selectors[i] : NULL);
        free(st->selectors);
        for (size_t i = 0; i < st->n_fns; i++) free(st->fn_names ? st->fn_names[i] : NULL);
        free(st->fn_names);
        free(st->fns);
        free(st->names_template);
        free(st);
    }
    return NULL;
}
