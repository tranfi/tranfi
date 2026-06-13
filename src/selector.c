/*
 * selector.c -- Column selector expansion for schema-aware row-local ops.
 *
 * Supported syntax is a compact, C-friendly subset of tidyselect:
 *   name, !name, -name
 *   starts_with(prefix), ends_with(suffix), contains(substr), matches(regex)
 *   where(type)
 *   all_of(name,...), any_of(name,...)
 *   start:end
 *   !selection, selection1 & selection2, selection1 | selection2, (selection)
 *
 * Pattern helpers are case-insensitive for parity with tidyselect defaults.
 * all_of() is strict about missing names; any_of() is lenient.
 * Ranges are strict exact-name endpoints over the incoming schema order.
 * Boolean selector algebra is schema-only and never inspects row data.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <stdio.h>
#include <regex.h>

static char *selector_strdup_range(const char *start, size_t len) {
    char *out = malloc(len + 1);
    if (!out) return NULL;
    memcpy(out, start, len);
    out[len] = '\0';
    return out;
}

static char *selector_trim_copy(const char *s) {
    if (!s) return NULL;
    while (isspace((unsigned char)*s)) s++;
    size_t len = strlen(s);
    while (len > 0 && isspace((unsigned char)s[len - 1])) len--;
    return selector_strdup_range(s, len);
}

static void selector_set_error(char **error, const char *msg, const char *arg) {
    if (!error) return;
    if (!msg) msg = "selector error";
    if (!arg) arg = "";
    size_t n = strlen(msg) + strlen(arg) + 8;
    char *buf = malloc(n);
    if (!buf) return;
    snprintf(buf, n, "%s%s%s", msg, *arg ? ": " : "", arg);
    *error = buf;
}

static int selector_is_name_list_helper(const char *func) {
    return func && (strcasecmp(func, "all_of") == 0 || strcasecmp(func, "any_of") == 0);
}

static const char *selector_find_range_colon(const char *s) {
    if (!s) return NULL;
    int paren = 0, bracket = 0, brace = 0;
    char quote = '\0';
    for (const char *p = s; *p; p++) {
        char c = *p;
        if (quote != '\0') {
            if (c == quote) quote = '\0';
            continue;
        }
        if (c == '\'' || c == '"') {
            quote = c;
            continue;
        }
        if (c == '(') { paren++; continue; }
        if (c == ')' && paren > 0) { paren--; continue; }
        if (c == '[') { bracket++; continue; }
        if (c == ']' && bracket > 0) { bracket--; continue; }
        if (c == '{') { brace++; continue; }
        if (c == '}' && brace > 0) { brace--; continue; }
        if (c == ':' && paren == 0 && bracket == 0 && brace == 0 && p != s && p[1] != '\0') return p;
    }
    return NULL;
}

static int selector_has_range_syntax(const char *s) {
    return selector_find_range_colon(s) != NULL;
}

static int selector_has_boolean_algebra(const char *raw) {
    char *s = selector_trim_copy(raw);
    if (!s) return 0;
    char *p = s;
    int has = 0;
    if (*p == '(') {
        has = 1;
        goto done;
    }
    if ((*p == '!' || *p == '-') && p[1] != '\0') {
        char *q = p + 1;
        while (isspace((unsigned char)*q)) q++;
        if (*q == '(') {
            has = 1;
            goto done;
        }
    }

    int paren = 0, bracket = 0, brace = 0;
    char quote = '\0';
    for (char *q = p; *q; q++) {
        char c = *q;
        if (quote != '\0') {
            if (c == quote) quote = '\0';
            continue;
        }
        if (c == '\'' || c == '"') {
            quote = c;
            continue;
        }
        if (c == '(') { paren++; continue; }
        if (c == ')' && paren > 0) { paren--; continue; }
        if (c == '[') { bracket++; continue; }
        if (c == ']' && bracket > 0) { bracket--; continue; }
        if (c == '{') { brace++; continue; }
        if (c == '}' && brace > 0) { brace--; continue; }
        if ((c == '&' || c == '|') && paren == 0 && bracket == 0 && brace == 0) {
            has = 1;
            break;
        }
    }

done:
    free(s);
    return has;
}

int tf_column_selector_has_syntax(const char *raw) {
    char *s = selector_trim_copy(raw);
    if (!s) return 0;
    char *p = s;
    if ((p[0] == '!' || p[0] == '-') && p[1] != '\0') {
        free(s);
        return 1;
    }
    int has = selector_has_boolean_algebra(p) ||
              (strchr(p, '(') != NULL && p[strlen(p) ? strlen(p) - 1 : 0] == ')') ||
              selector_has_range_syntax(p);
    free(s);
    return has;
}

static int parse_selector(const char *raw, int *negated, char **func, char **arg, char **exact) {
    *negated = 0;
    *func = NULL;
    *arg = NULL;
    *exact = NULL;

    char *s = selector_trim_copy(raw);
    if (!s) return TF_ERROR;
    char *p = s;
    if ((p[0] == '!' || p[0] == '-') && p[1] != '\0') {
        *negated = 1;
        p++;
        while (isspace((unsigned char)*p)) p++;
    }

    size_t len = strlen(p);
    char *lp = strchr(p, '(');
    if (lp && len > 0 && p[len - 1] == ')') {
        char *fname = selector_strdup_range(p, (size_t)(lp - p));
        char *farg = selector_strdup_range(lp + 1, (size_t)((p + len - 1) - (lp + 1)));
        if (!fname || !farg) { free(s); free(fname); free(farg); return TF_ERROR; }
        char *fname_trim = selector_trim_copy(fname);
        char *arg_trim = selector_trim_copy(farg);
        free(fname);
        free(farg);
        if (!fname_trim || !arg_trim) { free(s); free(fname_trim); free(arg_trim); return TF_ERROR; }
        size_t alen = strlen(arg_trim);
        if (!selector_is_name_list_helper(fname_trim) &&
            alen >= 2 && ((arg_trim[0] == '\'' && arg_trim[alen - 1] == '\'') ||
                          (arg_trim[0] == '"' && arg_trim[alen - 1] == '"'))) {
            char *unquoted = selector_strdup_range(arg_trim + 1, alen - 2);
            free(arg_trim);
            arg_trim = unquoted;
            if (!arg_trim) { free(s); free(fname_trim); return TF_ERROR; }
        }
        *func = fname_trim;
        *arg = arg_trim;
        free(s);
        return TF_OK;
    }

    *exact = strdup(p);
    free(s);
    return *exact ? TF_OK : TF_ERROR;
}

static int starts_with_ci(const char *s, const char *prefix) {
    size_t n = strlen(prefix);
    return strncasecmp(s, prefix, n) == 0;
}

static int ends_with_ci(const char *s, const char *suffix) {
    size_t slen = strlen(s);
    size_t xlen = strlen(suffix);
    if (xlen > slen) return 0;
    return strcasecmp(s + slen - xlen, suffix) == 0;
}

static int contains_ci(const char *s, const char *needle) {
    size_t nlen = strlen(needle);
    if (nlen == 0) return 1;
    for (const char *p = s; *p; p++) {
        if (strncasecmp(p, needle, nlen) == 0) return 1;
    }
    return 0;
}

static int type_matches(tf_type t, const char *name) {
    if (strcasecmp(name, "numeric") == 0 || strcasecmp(name, "number") == 0)
        return t == TF_TYPE_INT64 || t == TF_TYPE_FLOAT64;
    if (strcasecmp(name, "int") == 0 || strcasecmp(name, "int64") == 0 ||
        strcasecmp(name, "integer") == 0)
        return t == TF_TYPE_INT64;
    if (strcasecmp(name, "float") == 0 || strcasecmp(name, "float64") == 0 ||
        strcasecmp(name, "double") == 0)
        return t == TF_TYPE_FLOAT64;
    if (strcasecmp(name, "string") == 0 || strcasecmp(name, "str") == 0)
        return t == TF_TYPE_STRING;
    if (strcasecmp(name, "bool") == 0 || strcasecmp(name, "boolean") == 0)
        return t == TF_TYPE_BOOL;
    if (strcasecmp(name, "date") == 0)
        return t == TF_TYPE_DATE;
    if (strcasecmp(name, "timestamp") == 0)
        return t == TF_TYPE_TIMESTAMP;
    if (strcasecmp(name, "temporal") == 0)
        return t == TF_TYPE_DATE || t == TF_TYPE_TIMESTAMP;
    return 0;
}

static int selector_matches_column(const char *func, const char *arg,
                                   const char *name, tf_type type,
                                   regex_t *compiled) {
    if (strcasecmp(func, "starts_with") == 0) return starts_with_ci(name, arg);
    if (strcasecmp(func, "ends_with") == 0) return ends_with_ci(name, arg);
    if (strcasecmp(func, "contains") == 0) return contains_ci(name, arg);
    if (strcasecmp(func, "matches") == 0) return regexec(compiled, name, 0, NULL, 0) == 0;
    if (strcasecmp(func, "where") == 0) return type_matches(type, arg);
    return 0;
}

static int selector_find_column(char **names, size_t n_cols, const char *name) {
    if (!names || !name) return -1;
    for (size_t i = 0; i < n_cols; i++) {
        if (strcmp(names[i], name) == 0) return (int)i;
    }
    return -1;
}

static int append_index(int *order, size_t *n_order, int *included, int idx);
static void remove_index(int *order, size_t n_order, int *included, int idx);

static int selector_apply_range(const char *expr, int neg, char **names, size_t n_cols,
                                int *included, int *order, size_t *n_order,
                                char **error) {
    const char *colon = selector_find_range_colon(expr);
    if (!colon) return TF_ERROR;
    char *left_raw = selector_strdup_range(expr, (size_t)(colon - expr));
    char *right_raw = strdup(colon + 1);
    if (!left_raw || !right_raw) {
        free(left_raw); free(right_raw);
        selector_set_error(error, "selector out of memory", NULL);
        return TF_ERROR;
    }
    char *left = selector_trim_copy(left_raw);
    char *right = selector_trim_copy(right_raw);
    free(left_raw); free(right_raw);
    if (!left || !right || left[0] == '\0' || right[0] == '\0') {
        free(left); free(right);
        selector_set_error(error, "invalid range selector", expr);
        return TF_ERROR;
    }

    int li = selector_find_column(names, n_cols, left);
    int ri = selector_find_column(names, n_cols, right);
    if (li < 0 || ri < 0) {
        const char *missing = li < 0 ? left : right;
        selector_set_error(error, "range endpoint not found", missing);
        free(left); free(right);
        return TF_ERROR;
    }

    int step = li <= ri ? 1 : -1;
    for (int idx = li;; idx += step) {
        if (neg) remove_index(order, *n_order, included, idx);
        else append_index(order, n_order, included, idx);
        if (idx == ri) break;
    }

    free(left); free(right);
    return TF_OK;
}

static void selector_free_name_list(char **items, size_t n_items) {
    if (!items) return;
    for (size_t i = 0; i < n_items; i++) free(items[i]);
    free(items);
}

static int selector_append_name_item(char ***items, size_t *n_items, size_t *cap,
                                     const char *start, size_t len) {
    char *raw = selector_strdup_range(start, len);
    if (!raw) return TF_ERROR;
    char *trimmed = selector_trim_copy(raw);
    free(raw);
    if (!trimmed) return TF_ERROR;

    size_t tlen = strlen(trimmed);
    if (tlen == 0) {
        free(trimmed);
        return TF_ERROR;
    }
    if (trimmed[0] == '\'' || trimmed[0] == '"') {
        char q = trimmed[0];
        if (tlen < 2 || trimmed[tlen - 1] != q) {
            free(trimmed);
            return TF_ERROR;
        }
        char *unquoted = selector_strdup_range(trimmed + 1, tlen - 2);
        free(trimmed);
        trimmed = unquoted;
        if (!trimmed) return TF_ERROR;
        if (trimmed[0] == '\0') {
            free(trimmed);
            return TF_ERROR;
        }
    }

    if (*n_items == *cap) {
        size_t next = *cap ? *cap * 2 : 4;
        char **grown = realloc(*items, next * sizeof(char *));
        if (!grown) {
            free(trimmed);
            return TF_ERROR;
        }
        *items = grown;
        *cap = next;
    }
    (*items)[(*n_items)++] = trimmed;
    return TF_OK;
}

static int selector_parse_name_list(const char *arg, char ***out_items, size_t *out_n) {
    *out_items = NULL;
    *out_n = 0;
    if (!arg) return TF_ERROR;

    char **items = NULL;
    size_t n_items = 0, cap = 0;
    const char *start = arg;
    char quote = '\0';
    for (const char *p = arg;; p++) {
        char c = *p;
        if (c == '\0') {
            if (quote != '\0') {
                selector_free_name_list(items, n_items);
                return TF_ERROR;
            }
            if (selector_append_name_item(&items, &n_items, &cap, start, (size_t)(p - start)) != TF_OK) {
                selector_free_name_list(items, n_items);
                return TF_ERROR;
            }
            break;
        }
        if (quote != '\0') {
            if (c == quote) quote = '\0';
            continue;
        }
        if (c == '\'' || c == '"') {
            quote = c;
            continue;
        }
        if (c == ',') {
            if (selector_append_name_item(&items, &n_items, &cap, start, (size_t)(p - start)) != TF_OK) {
                selector_free_name_list(items, n_items);
                return TF_ERROR;
            }
            start = p + 1;
        }
    }

    if (n_items == 0) {
        selector_free_name_list(items, n_items);
        return TF_ERROR;
    }
    *out_items = items;
    *out_n = n_items;
    return TF_OK;
}

static int append_index(int *order, size_t *n_order, int *included, int idx) {
    if (idx < 0) return TF_ERROR;
    if (included[idx]) return TF_OK;
    included[idx] = 1;
    order[(*n_order)++] = idx;
    return TF_OK;
}

static void remove_index(int *order, size_t n_order, int *included, int idx) {
    if (idx < 0) return;
    included[idx] = 0;
    for (size_t i = 0; i < n_order; i++) {
        if (order[i] == idx) order[i] = -1;
    }
}


typedef struct {
    int *order;
    size_t n_order;
    int *included;
    size_t n_cols;
} selector_result;

static int selector_result_init(selector_result *r, size_t n_cols) {
    r->n_cols = n_cols;
    r->n_order = 0;
    r->order = malloc((n_cols ? n_cols : 1) * sizeof(int));
    r->included = calloc(n_cols ? n_cols : 1, sizeof(int));
    if (!r->order || !r->included) {
        free(r->order);
        free(r->included);
        r->order = NULL;
        r->included = NULL;
        r->n_cols = 0;
        return TF_ERROR;
    }
    return TF_OK;
}

static void selector_result_free(selector_result *r) {
    if (!r) return;
    free(r->order);
    free(r->included);
    r->order = NULL;
    r->included = NULL;
    r->n_order = 0;
    r->n_cols = 0;
}

static int selector_result_union(const selector_result *left,
                                 const selector_result *right,
                                 selector_result *out) {
    if (selector_result_init(out, left->n_cols) != TF_OK) return TF_ERROR;
    for (size_t i = 0; i < left->n_order; i++) {
        int idx = left->order[i];
        if (idx >= 0 && left->included[idx]) append_index(out->order, &out->n_order, out->included, idx);
    }
    for (size_t i = 0; i < right->n_order; i++) {
        int idx = right->order[i];
        if (idx >= 0 && right->included[idx]) append_index(out->order, &out->n_order, out->included, idx);
    }
    return TF_OK;
}

static int selector_result_intersection(const selector_result *left,
                                        const selector_result *right,
                                        selector_result *out) {
    if (selector_result_init(out, left->n_cols) != TF_OK) return TF_ERROR;
    for (size_t i = 0; i < left->n_order; i++) {
        int idx = left->order[i];
        if (idx >= 0 && left->included[idx] && right->included[idx]) {
            append_index(out->order, &out->n_order, out->included, idx);
        }
    }
    return TF_OK;
}

static int selector_result_complement(const selector_result *in, selector_result *out) {
    if (selector_result_init(out, in->n_cols) != TF_OK) return TF_ERROR;
    for (size_t i = 0; i < in->n_cols; i++) {
        if (!in->included[i]) append_index(out->order, &out->n_order, out->included, (int)i);
    }
    return TF_OK;
}

static int selector_resolve_atomic(const char *raw,
                                   char **names, const tf_type *types, size_t n_cols,
                                   selector_result *out,
                                   char **error) {
    selector_result base;
    if (selector_result_init(&base, n_cols) != TF_OK) {
        selector_set_error(error, "selector out of memory", NULL);
        return TF_ERROR;
    }

    int neg = 0;
    char *func = NULL, *arg = NULL, *exact = NULL;
    if (parse_selector(raw, &neg, &func, &arg, &exact) != TF_OK) {
        selector_result_free(&base);
        selector_set_error(error, "invalid selector", raw);
        return TF_ERROR;
    }

    regex_t compiled;
    int have_regex = 0;
    if (func && strcasecmp(func, "matches") == 0) {
        int rc = regcomp(&compiled, arg, REG_EXTENDED | REG_NOSUB | REG_ICASE);
        if (rc != 0) {
            char msg[128];
            regerror(rc, &compiled, msg, sizeof(msg));
            free(func); free(arg); free(exact);
            selector_result_free(&base);
            selector_set_error(error, "invalid matches() regex", msg);
            return TF_ERROR;
        }
        have_regex = 1;
    }

    if (exact) {
        int exact_idx = selector_find_column(names, n_cols, exact);
        if (exact_idx >= 0) {
            append_index(base.order, &base.n_order, base.included, exact_idx);
        } else if (selector_has_range_syntax(exact)) {
            if (selector_apply_range(exact, 0, names, n_cols, base.included,
                                     base.order, &base.n_order, error) != TF_OK) {
                free(func); free(arg); free(exact);
                if (have_regex) regfree(&compiled);
                selector_result_free(&base);
                return TF_ERROR;
            }
        } else {
            char *missing = exact;
            free(func); free(arg);
            if (have_regex) regfree(&compiled);
            selector_result_free(&base);
            selector_set_error(error, "column not found", missing);
            free(missing);
            return TF_ERROR;
        }
    } else if (func) {
        if (selector_is_name_list_helper(func)) {
            char **items = NULL;
            size_t n_items = 0;
            if (selector_parse_name_list(arg, &items, &n_items) != TF_OK) {
                free(func); free(arg); free(exact);
                if (have_regex) regfree(&compiled);
                selector_result_free(&base);
                selector_set_error(error, "invalid selector name list", raw);
                return TF_ERROR;
            }
            int strict = strcasecmp(func, "all_of") == 0;
            for (size_t i = 0; i < n_items; i++) {
                int idx = selector_find_column(names, n_cols, items[i]);
                if (idx < 0) {
                    if (strict) {
                        char *missing = strdup(items[i]);
                        selector_free_name_list(items, n_items);
                        free(func); free(arg); free(exact);
                        if (have_regex) regfree(&compiled);
                        selector_result_free(&base);
                        selector_set_error(error, "all_of column not found", missing ? missing : "");
                        free(missing);
                        return TF_ERROR;
                    }
                    continue;
                }
                append_index(base.order, &base.n_order, base.included, idx);
            }
            selector_free_name_list(items, n_items);
        } else {
            if (!(strcasecmp(func, "starts_with") == 0 || strcasecmp(func, "ends_with") == 0 ||
                  strcasecmp(func, "contains") == 0 || strcasecmp(func, "matches") == 0 ||
                  strcasecmp(func, "where") == 0)) {
                free(func); free(arg); free(exact);
                if (have_regex) regfree(&compiled);
                selector_result_free(&base);
                selector_set_error(error, "unknown selector helper", raw);
                return TF_ERROR;
            }
            for (size_t i = 0; i < n_cols; i++) {
                if (selector_matches_column(func, arg, names[i], types[i], &compiled)) {
                    append_index(base.order, &base.n_order, base.included, (int)i);
                }
            }
        }
    }

    if (have_regex) regfree(&compiled);
    free(func); free(arg); free(exact);

    if (neg) {
        selector_result comp;
        if (selector_result_complement(&base, &comp) != TF_OK) {
            selector_result_free(&base);
            selector_set_error(error, "selector out of memory", NULL);
            return TF_ERROR;
        }
        selector_result_free(&base);
        *out = comp;
    } else {
        *out = base;
    }
    return TF_OK;
}

typedef struct {
    const char *s;
    size_t len;
    size_t pos;
    char **names;
    const tf_type *types;
    size_t n_cols;
    char **error;
} selector_expr_parser;

static void selector_expr_skip_ws(selector_expr_parser *p) {
    while (p->pos < p->len && isspace((unsigned char)p->s[p->pos])) p->pos++;
}

static int selector_expr_parse_or(selector_expr_parser *p, selector_result *out);

static int selector_expr_parse_primary(selector_expr_parser *p, selector_result *out) {
    selector_expr_skip_ws(p);
    if (p->pos >= p->len) {
        selector_set_error(p->error, "expected selector expression", NULL);
        return TF_ERROR;
    }

    if (p->s[p->pos] == '(') {
        p->pos++;
        selector_result inner;
        if (selector_expr_parse_or(p, &inner) != TF_OK) return TF_ERROR;
        selector_expr_skip_ws(p);
        if (p->pos >= p->len || p->s[p->pos] != ')') {
            selector_result_free(&inner);
            selector_set_error(p->error, "unclosed selector expression", NULL);
            return TF_ERROR;
        }
        p->pos++;
        *out = inner;
        return TF_OK;
    }

    size_t start = p->pos;
    int paren = 0, bracket = 0, brace = 0;
    char quote = '\0';
    while (p->pos < p->len) {
        char c = p->s[p->pos];
        if (quote != '\0') {
            if (c == quote) quote = '\0';
            p->pos++;
            continue;
        }
        if (c == '\'' || c == '"') {
            quote = c;
            p->pos++;
            continue;
        }
        if (c == '(') { paren++; p->pos++; continue; }
        if (c == ')' && paren > 0) { paren--; p->pos++; continue; }
        if (c == '[') { bracket++; p->pos++; continue; }
        if (c == ']' && bracket > 0) { bracket--; p->pos++; continue; }
        if (c == '{') { brace++; p->pos++; continue; }
        if (c == '}' && brace > 0) { brace--; p->pos++; continue; }
        if ((c == '&' || c == '|' || c == ')') && paren == 0 && bracket == 0 && brace == 0) break;
        p->pos++;
    }

    char *atom_raw = selector_strdup_range(p->s + start, p->pos - start);
    char *atom = atom_raw ? selector_trim_copy(atom_raw) : NULL;
    free(atom_raw);
    if (!atom || atom[0] == '\0') {
        free(atom);
        selector_set_error(p->error, "expected selector expression", NULL);
        return TF_ERROR;
    }
    int rc = selector_resolve_atomic(atom, p->names, p->types, p->n_cols, out, p->error);
    free(atom);
    return rc;
}

static int selector_expr_parse_unary(selector_expr_parser *p, selector_result *out) {
    selector_expr_skip_ws(p);
    if (p->pos < p->len && (p->s[p->pos] == '!' || p->s[p->pos] == '-')) {
        p->pos++;
        selector_result child;
        if (selector_expr_parse_unary(p, &child) != TF_OK) return TF_ERROR;
        selector_result comp;
        if (selector_result_complement(&child, &comp) != TF_OK) {
            selector_result_free(&child);
            selector_set_error(p->error, "selector out of memory", NULL);
            return TF_ERROR;
        }
        selector_result_free(&child);
        *out = comp;
        return TF_OK;
    }
    return selector_expr_parse_primary(p, out);
}

static int selector_expr_parse_and(selector_expr_parser *p, selector_result *out) {
    selector_result left;
    if (selector_expr_parse_unary(p, &left) != TF_OK) return TF_ERROR;
    for (;;) {
        selector_expr_skip_ws(p);
        if (p->pos >= p->len || p->s[p->pos] != '&') break;
        p->pos++;
        selector_result right;
        if (selector_expr_parse_unary(p, &right) != TF_OK) {
            selector_result_free(&left);
            return TF_ERROR;
        }
        selector_result joined;
        if (selector_result_intersection(&left, &right, &joined) != TF_OK) {
            selector_result_free(&left);
            selector_result_free(&right);
            selector_set_error(p->error, "selector out of memory", NULL);
            return TF_ERROR;
        }
        selector_result_free(&left);
        selector_result_free(&right);
        left = joined;
    }
    *out = left;
    return TF_OK;
}

static int selector_expr_parse_or(selector_expr_parser *p, selector_result *out) {
    selector_result left;
    if (selector_expr_parse_and(p, &left) != TF_OK) return TF_ERROR;
    for (;;) {
        selector_expr_skip_ws(p);
        if (p->pos >= p->len || p->s[p->pos] != '|') break;
        p->pos++;
        selector_result right;
        if (selector_expr_parse_and(p, &right) != TF_OK) {
            selector_result_free(&left);
            return TF_ERROR;
        }
        selector_result joined;
        if (selector_result_union(&left, &right, &joined) != TF_OK) {
            selector_result_free(&left);
            selector_result_free(&right);
            selector_set_error(p->error, "selector out of memory", NULL);
            return TF_ERROR;
        }
        selector_result_free(&left);
        selector_result_free(&right);
        left = joined;
    }
    *out = left;
    return TF_OK;
}

static int selector_eval_boolean_algebra(const char *raw,
                                         char **names, const tf_type *types, size_t n_cols,
                                         selector_result *out,
                                         char **error) {
    selector_expr_parser p = {
        .s = raw ? raw : "",
        .len = raw ? strlen(raw) : 0,
        .pos = 0,
        .names = names,
        .types = types,
        .n_cols = n_cols,
        .error = error,
    };
    if (selector_expr_parse_or(&p, out) != TF_OK) return TF_ERROR;
    selector_expr_skip_ws(&p);
    if (p.pos != p.len) {
        selector_result_free(out);
        selector_set_error(error, "invalid selector expression", raw);
        return TF_ERROR;
    }
    return TF_OK;
}

int tf_column_selectors_resolve(char **selectors, size_t n_selectors,
                                char **names, const tf_type *types, size_t n_cols,
                                int **out_indices, size_t *out_n,
                                char **error) {
    *out_indices = NULL;
    *out_n = 0;
    if (!selectors || n_selectors == 0) {
        selector_set_error(error, "selector list is empty", NULL);
        return TF_ERROR;
    }

    int *included = calloc(n_cols ? n_cols : 1, sizeof(int));
    int *order = malloc((n_cols ? n_cols : 1) * sizeof(int));
    if (!included || !order) {
        free(included); free(order);
        selector_set_error(error, "selector out of memory", NULL);
        return TF_ERROR;
    }

    int has_positive = 0;
    for (size_t i = 0; i < n_selectors; i++) {
        if (selector_has_boolean_algebra(selectors[i])) {
            has_positive = 1;
            continue;
        }
        int neg = 0;
        char *func = NULL, *arg = NULL, *exact = NULL;
        if (parse_selector(selectors[i], &neg, &func, &arg, &exact) != TF_OK) {
            free(included); free(order);
            selector_set_error(error, "invalid selector", selectors[i]);
            return TF_ERROR;
        }
        if (!neg) has_positive = 1;
        free(func); free(arg); free(exact);
    }

    size_t n_order = 0;
    if (!has_positive) {
        for (size_t i = 0; i < n_cols; i++) append_index(order, &n_order, included, (int)i);
    }

    for (size_t si = 0; si < n_selectors; si++) {
        if (selector_has_boolean_algebra(selectors[si])) {
            selector_result expr;
            if (selector_eval_boolean_algebra(selectors[si], names, types, n_cols, &expr, error) != TF_OK) {
                free(included); free(order);
                return TF_ERROR;
            }
            for (size_t i = 0; i < expr.n_order; i++) {
                int idx = expr.order[i];
                if (idx >= 0 && expr.included[idx]) append_index(order, &n_order, included, idx);
            }
            selector_result_free(&expr);
            continue;
        }

        int neg = 0;
        char *func = NULL, *arg = NULL, *exact = NULL;
        if (parse_selector(selectors[si], &neg, &func, &arg, &exact) != TF_OK) {
            free(included); free(order);
            selector_set_error(error, "invalid selector", selectors[si]);
            return TF_ERROR;
        }

        regex_t compiled;
        int have_regex = 0;
        if (func && strcasecmp(func, "matches") == 0) {
            int rc = regcomp(&compiled, arg, REG_EXTENDED | REG_NOSUB | REG_ICASE);
            if (rc != 0) {
                char msg[128];
                regerror(rc, &compiled, msg, sizeof(msg));
                free(func); free(arg); free(exact); free(included); free(order);
                selector_set_error(error, "invalid matches() regex", msg);
                return TF_ERROR;
            }
            have_regex = 1;
        }

        if (exact) {
            int exact_idx = selector_find_column(names, n_cols, exact);
            if (exact_idx >= 0) {
                if (neg) remove_index(order, n_order, included, exact_idx);
                else append_index(order, &n_order, included, exact_idx);
            } else if (selector_has_range_syntax(exact)) {
                if (selector_apply_range(exact, neg, names, n_cols, included, order, &n_order, error) != TF_OK) {
                    free(func); free(arg); free(exact);
                    if (have_regex) regfree(&compiled);
                    free(included); free(order);
                    return TF_ERROR;
                }
            } else {
                char *missing = exact;
                free(func); free(arg);
                if (have_regex) regfree(&compiled);
                free(included); free(order);
                selector_set_error(error, "column not found", missing);
                free(missing);
                return TF_ERROR;
            }
        } else if (func) {
            if (selector_is_name_list_helper(func)) {
                char **items = NULL;
                size_t n_items = 0;
                if (selector_parse_name_list(arg, &items, &n_items) != TF_OK) {
                    free(func); free(arg); free(exact);
                    if (have_regex) regfree(&compiled);
                    free(included); free(order);
                    selector_set_error(error, "invalid selector name list", selectors[si]);
                    return TF_ERROR;
                }
                int strict = strcasecmp(func, "all_of") == 0;
                for (size_t i = 0; i < n_items; i++) {
                    int idx = selector_find_column(names, n_cols, items[i]);
                    if (idx < 0) {
                        if (strict) {
                            char *missing = strdup(items[i]);
                            selector_free_name_list(items, n_items);
                            free(func); free(arg); free(exact);
                            if (have_regex) regfree(&compiled);
                            free(included); free(order);
                            selector_set_error(error, "all_of column not found", missing ? missing : "");
                            free(missing);
                            return TF_ERROR;
                        }
                        continue;
                    }
                    if (neg) remove_index(order, n_order, included, idx);
                    else append_index(order, &n_order, included, idx);
                }
                selector_free_name_list(items, n_items);
            } else {
                if (!(strcasecmp(func, "starts_with") == 0 || strcasecmp(func, "ends_with") == 0 ||
                      strcasecmp(func, "contains") == 0 || strcasecmp(func, "matches") == 0 ||
                      strcasecmp(func, "where") == 0)) {
                    free(func); free(arg); free(exact);
                    if (have_regex) regfree(&compiled);
                    free(included); free(order);
                    selector_set_error(error, "unknown selector helper", selectors[si]);
                    return TF_ERROR;
                }
                for (size_t i = 0; i < n_cols; i++) {
                    if (selector_matches_column(func, arg, names[i], types[i], &compiled)) {
                        if (neg) remove_index(order, n_order, included, (int)i);
                        else append_index(order, &n_order, included, (int)i);
                    }
                }
            }
        }

        if (have_regex) regfree(&compiled);
        free(func); free(arg); free(exact);
    }

    size_t final_n = 0;
    for (size_t i = 0; i < n_order; i++) {
        int idx = order[i];
        if (idx >= 0 && included[idx]) final_n++;
    }
    if (final_n == 0) {
        free(included); free(order);
        selector_set_error(error, "selectors resolved no columns", NULL);
        return TF_ERROR;
    }

    int *out = malloc(final_n * sizeof(int));
    if (!out) {
        free(included); free(order);
        selector_set_error(error, "selector out of memory", NULL);
        return TF_ERROR;
    }
    size_t j = 0;
    for (size_t i = 0; i < n_order; i++) {
        int idx = order[i];
        if (idx >= 0 && included[idx]) out[j++] = idx;
    }

    free(included);
    free(order);
    *out_indices = out;
    *out_n = final_n;
    return TF_OK;
}

int tf_column_selectors_resolve_json(const cJSON *selectors,
                                     char **names, const tf_type *types, size_t n_cols,
                                     int **out_indices, size_t *out_n,
                                     char **error) {
    if (!selectors || !cJSON_IsArray(selectors)) {
        selector_set_error(error, "selector list must be an array", NULL);
        return TF_ERROR;
    }
    int n = cJSON_GetArraySize(selectors);
    if (n <= 0) {
        selector_set_error(error, "selector list is empty", NULL);
        return TF_ERROR;
    }
    char **items = calloc((size_t)n, sizeof(char *));
    if (!items) {
        selector_set_error(error, "selector out of memory", NULL);
        return TF_ERROR;
    }
    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(selectors, i);
        if (!cJSON_IsString(item)) {
            free(items);
            selector_set_error(error, "selector item must be a string", NULL);
            return TF_ERROR;
        }
        items[i] = item->valuestring;
    }
    int rc = tf_column_selectors_resolve(items, (size_t)n, names, types, n_cols,
                                         out_indices, out_n, error);
    free(items);
    return rc;
}

int tf_column_selectors_have_syntax_json(const cJSON *selectors) {
    if (!selectors || !cJSON_IsArray(selectors)) return 0;
    int n = cJSON_GetArraySize(selectors);
    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(selectors, i);
        if (cJSON_IsString(item) && tf_column_selector_has_syntax(item->valuestring)) return 1;
    }
    return 0;
}
