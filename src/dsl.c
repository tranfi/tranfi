/*
 * dsl.c — Pipe-style DSL parser (L3 → L2 IR).
 *
 * Grammar:
 *   pipeline  = stage ( '|' stage )*
 *   stage     = op_name arg*
 *   arg       = quoted_string | key=value | bare_word
 *
 * Positional codec resolution:
 *   "csv"   at first position → "codec.csv.decode"
 *   "csv"   at last  position → "codec.csv.encode"
 *   "jsonl" at first position → "codec.jsonl.decode"
 *   "jsonl" at last  position → "codec.jsonl.encode"
 *
 * Explicit forms ("csv.decode", "csv.encode", etc.) always work.
 */

#include "dsl.h"
#include "ir.h"
#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include <stdarg.h>
#include <stdbool.h>
#include <math.h>

#define TF_OK    0
#define TF_ERROR (-1)

/* ---- Error helpers ---- */

static void set_error(char **error, const char *msg) {
    if (error) {
        free(*error);
        size_t len = strlen(msg) + 1;
        *error = malloc(len);
        if (*error) memcpy(*error, msg, len);
    }
}

static void set_errorf(char **error, const char *fmt, ...) {
    if (error) {
        char buf[256];
        va_list ap;
        va_start(ap, fmt);
        vsnprintf(buf, sizeof(buf), fmt, ap);
        va_end(ap);
        set_error(error, buf);
    }
}

static void set_oom_error_if_unset(char **error) {
    if (error && !*error) set_error(error, "out of memory");
}

static int dsl_parse_positive_size(const char *text, const char *name, size_t *out, char **error);
static int dsl_set_object_item(cJSON *obj, const char *key, cJSON *item);
static int dsl_set_string_arg(cJSON *obj, const char *key, const char *value);
static int dsl_set_number_arg(cJSON *obj, const char *key, double value);
static int dsl_set_bool_arg(cJSON *obj, const char *key, int value);
static int dsl_add_string_array_item(cJSON *arr, const char *value);
static int dsl_add_array_duplicate(cJSON *arr, cJSON *src);
static int add_audit_csv_arg(cJSON *args, const char *key, const char *spec);
static char *trimmed_token_copy(const char *start, size_t len);
static int add_comma_columns(cJSON *cols, const char *spec, char **error, const char *op_label);

/* ---- Token type ---- */

typedef struct {
    char  **items;
    size_t  count;
    size_t  cap;
} token_list;

static void tl_init(token_list *tl) {
    tl->items = NULL;
    tl->count = 0;
    tl->cap = 0;
}

static int tl_push(token_list *tl, const char *s, size_t len) {
    if (tl->count >= tl->cap) {
        size_t need = 0;
        size_t new_cap = 0;
        if (tf_size_add(tl->count, 1, &need) != TF_OK ||
            tf_size_grow_pow2(tl->cap, need, 8, &new_cap) != TF_OK) {
            return -1;
        }
        char **new_items = tf_reallocarray_checked(tl->items, new_cap, sizeof(char *));
        if (!new_items) return -1;
        tl->items = new_items;
        tl->cap = new_cap;
    }
    size_t alloc_len = 0;
    if (tf_size_add(len, 1, &alloc_len) != TF_OK) return -1;
    char *dup = tf_mallocarray_checked(alloc_len, sizeof(char));
    if (!dup) return -1;
    memcpy(dup, s, len);
    dup[len] = '\0';
    tl->items[tl->count++] = dup;
    return 0;
}

static void tl_free(token_list *tl) {
    for (size_t i = 0; i < tl->count; i++) free(tl->items[i]);
    free(tl->items);
    tl->items = NULL;
    tl->count = 0;
    tl->cap = 0;
}

static const char *dsl_find_top_level_comma(const char *start, const char *end) {
    int paren = 0, bracket = 0, brace = 0;
    char quote = '\0';
    for (const char *p = start; p < end; p++) {
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
        if (c == ',' && paren == 0 && bracket == 0 && brace == 0) return p;
    }
    return NULL;
}

static int dsl_token_has_top_level_comma(const char *start, size_t len) {
    return dsl_find_top_level_comma(start, start + len) != NULL;
}

static int tl_push_top_level_comma_split(token_list *out, const char *start, size_t len) {
    const char *p = start;
    const char *end = start + len;
    while (p <= end) {
        const char *comma = dsl_find_top_level_comma(p, end);
        size_t tok_len = comma ? (size_t)(comma - p) : (size_t)(end - p);
        if (tok_len > 0 && tl_push(out, p, tok_len) != 0) return -1;
        if (!comma) break;
        p = comma + 1;
    }
    return 0;
}

/* ---- Stage splitting ---- */

/*
 * Split input on '|' while respecting double-quoted strings.
 * Each stage is trimmed of leading/trailing whitespace.
 */
static int split_stages(const char *text, size_t len, token_list *out) {
    tl_init(out);
    size_t start = 0;
    int in_quote = 0;

    for (size_t i = 0; i < len; i++) {
        if (text[i] == '"') {
            in_quote = !in_quote;
        } else if (text[i] == '|' && !in_quote) {
            /* Trim whitespace */
            size_t s = start, e = i;
            while (s < e && isspace((unsigned char)text[s])) s++;
            while (e > s && isspace((unsigned char)text[e - 1])) e--;
            if (s == e) return -1; /* empty stage */
            if (tl_push(out, text + s, e - s) != 0) return -1;
            start = i + 1;
        }
    }

    /* Last stage */
    size_t s = start, e = len;
    while (s < e && isspace((unsigned char)text[s])) s++;
    while (e > s && isspace((unsigned char)text[e - 1])) e--;
    if (s < e && tl_push(out, text + s, e - s) != 0) return -1;

    return (out->count > 0) ? 0 : -1;
}

/* ---- Stage tokenization ---- */

/*
 * Tokenize a single stage into op name + args.
 * Handles: "quoted strings", key=value, bare words.
 * Comma-separated bare words are split into individual tokens.
 *
 * For derive, we need special handling: key="expr with spaces"
 * is kept as a single token "key=expr with spaces".
 */
static int tokenize_stage(const char *stage, token_list *out) {
    tl_init(out);
    const char *p = stage;

    while (*p) {
        /* Skip whitespace */
        while (*p && isspace((unsigned char)*p)) p++;
        if (!*p) break;

        if (*p == '"') {
            /* Quoted string — capture content without quotes */
            p++;
            const char *start = p;
            while (*p && *p != '"') p++;
            if (tl_push(out, start, p - start) != 0) return -1;
            if (*p == '"') p++;
        } else {
            /* Bare word or key=value — delimited by whitespace */
            const char *start = p;

            /* Check if this is key="value with spaces" */
            const char *eq = NULL;
            const char *scan = p;
            while (*scan && !isspace((unsigned char)*scan) && *scan != '"') {
                if (*scan == '=' && !eq) eq = scan;
                scan++;
            }

            if (eq && *scan == '"') {
                /* key="quoted value" — read until closing quote */
                scan++; /* skip opening quote */
                while (*scan && *scan != '"') scan++;
                if (*scan == '"') scan++; /* skip closing quote */
                /* The full token is from start to scan, but we need to
                 * strip the quotes from the value part */
                size_t key_len = eq - start;
                const char *val_start = eq + 2; /* skip = and " */
                const char *val_end = scan - 1; /* before closing " */
                size_t val_len = (size_t)(val_end - val_start);
                size_t total = 0;
                size_t alloc_len = 0;
                if (tf_size_add(key_len, 1, &total) != TF_OK ||
                    tf_size_add(total, val_len, &total) != TF_OK ||
                    tf_size_add(total, 1, &alloc_len) != TF_OK) {
                    return -1;
                }
                char *tok = tf_mallocarray_checked(alloc_len, sizeof(char));
                if (!tok) return -1;
                memcpy(tok, start, key_len);
                tok[key_len] = '=';
                memcpy(tok + key_len + 1, val_start, val_len);
                tok[total] = '\0';
                if (out->count >= out->cap) {
                    size_t need = 0;
                    size_t new_cap = 0;
                    if (tf_size_add(out->count, 1, &need) != TF_OK ||
                        tf_size_grow_pow2(out->cap, need, 8, &new_cap) != TF_OK) {
                        free(tok);
                        return -1;
                    }
                    char **new_items = tf_reallocarray_checked(out->items, new_cap, sizeof(char *));
                    if (!new_items) {
                        free(tok);
                        return -1;
                    }
                    out->items = new_items;
                    out->cap = new_cap;
                }
                out->items[out->count++] = tok;
                p = scan;
            } else {
                while (*p && !isspace((unsigned char)*p) && *p != '"') p++;
                size_t len = p - start;

                /* Split comma-separated tokens (for "name,age" -> "name", "age"),
                 * but keep commas inside selector/helper calls such as all_of(a,b). */
                if (memchr(start, '=', len) == NULL && dsl_token_has_top_level_comma(start, len)) {
                    if (tl_push_top_level_comma_split(out, start, len) != 0) return -1;
                } else {
                    if (tl_push(out, start, len) != 0) return -1;
                }
            }
        }
    }

    return (out->count > 0) ? 0 : -1;
}

/* ---- Codec resolution ---- */

/*
 * Resolve bare codec names to full op names based on position.
 * Returns a malloc'd string (caller frees) or NULL if not a codec shorthand.
 */
static const char *resolve_op_alias(const char *name) {
    if (strcmp(name, "mutate") == 0) return "derive";
    if (strcmp(name, "source-file") == 0) return "source-name";
    if (strcmp(name, "summarise") == 0 || strcmp(name, "summarize") == 0) return "group-agg";
    if (strcmp(name, "distinct") == 0) return "unique";
    if (strcmp(name, "arrange") == 0) return "sort";
    if (strcmp(name, "slice_head") == 0) return "slice-head";
    if (strcmp(name, "slice_tail") == 0) return "slice-tail";
    if (strcmp(name, "slice_min") == 0) return "slice-min";
    if (strcmp(name, "slice_max") == 0) return "slice-max";
    return name;
}

static char *resolve_codec(const char *name, int is_first, int is_last) {
    /* Already explicit */
    if (strcmp(name, "codec.csv.decode") == 0 ||
        strcmp(name, "codec.csv.encode") == 0 ||
        strcmp(name, "codec.jsonl.decode") == 0 ||
        strcmp(name, "codec.jsonl.encode") == 0 ||
        strcmp(name, "codec.text.decode") == 0 ||
        strcmp(name, "codec.text.encode") == 0 ||
        strcmp(name, "csv.decode") == 0 ||
        strcmp(name, "csv.encode") == 0 ||
        strcmp(name, "jsonl.decode") == 0 ||
        strcmp(name, "jsonl.encode") == 0 ||
        strcmp(name, "text.decode") == 0 ||
        strcmp(name, "text.encode") == 0) {
        /* Normalize short explicit forms */
        if (strcmp(name, "csv.decode") == 0) return strdup("codec.csv.decode");
        if (strcmp(name, "csv.encode") == 0) return strdup("codec.csv.encode");
        if (strcmp(name, "jsonl.decode") == 0) return strdup("codec.jsonl.decode");
        if (strcmp(name, "jsonl.encode") == 0) return strdup("codec.jsonl.encode");
        if (strcmp(name, "text.decode") == 0) return strdup("codec.text.decode");
        if (strcmp(name, "text.encode") == 0) return strdup("codec.text.encode");
        return strdup(name);
    }

    if (strcmp(name, "csv") == 0) {
        if (is_first) return strdup("codec.csv.decode");
        if (is_last)  return strdup("codec.csv.encode");
        return NULL; /* ambiguous */
    }
    if (strcmp(name, "jsonl") == 0) {
        if (is_first) return strdup("codec.jsonl.decode");
        if (is_last)  return strdup("codec.jsonl.encode");
        return NULL;
    }
    if (strcmp(name, "text") == 0) {
        if (is_first) return strdup("codec.text.decode");
        if (is_last)  return strdup("codec.text.encode");
        return NULL;
    }
    if (strcmp(name, "table") == 0) {
        if (is_last) return strdup("codec.table.encode");
        return NULL;
    }

    return NULL; /* not a codec */
}

/* ---- Arg builders per op type ---- */

static cJSON *build_codec_args(const token_list *tokens) {
    /* tokens[0] is op name, rest are key=value pairs */
    cJSON *args = cJSON_CreateObject();
    if (!args) return NULL;
    for (size_t i = 1; i < tokens->count; i++) {
        if (strcmp(tokens->items[i], "audit") == 0) {
            if (tf_json_add_bool(args, "audit", 1) != TF_OK) goto fail;
            continue;
        }
        if (strcmp(tokens->items[i], "audit=false") == 0) {
            if (tf_json_add_bool(args, "audit", 0) != TF_OK) goto fail;
            continue;
        }
        if (strncmp(tokens->items[i], "audit-limit=", 12) == 0) {
            char *copy = strdup(tokens->items[i]);
            if (!copy) { cJSON_Delete(args); return NULL; }
            copy[5] = '_';
            char *eq = strchr(copy, '=');
            if (eq) {
                *eq = '\0';
                char *end = NULL;
                long num = strtol(eq + 1, &end, 10);
                int rc = (*end == '\0' && end != eq + 1)
                    ? tf_json_add_number(args, copy, (double)num)
                    : tf_json_add_string(args, copy, eq + 1);
                if (rc != TF_OK) {
                    free(copy);
                    goto fail;
                }
            }
            free(copy);
            continue;
        }
        char *eq = strchr(tokens->items[i], '=');
        if (eq) {
            *eq = '\0';
            const char *key = tokens->items[i];
            const char *val = eq + 1;
            /* Try to detect booleans and ints */
            int rc = TF_OK;
            if (strcmp(val, "true") == 0 || strcmp(val, "false") == 0) {
                rc = tf_json_add_bool(args, key, strcmp(val, "true") == 0);
            } else {
                /* Check if integer */
                char *end;
                long num = strtol(val, &end, 10);
                if (*end == '\0' && end != val) {
                    rc = tf_json_add_number(args, key, (double)num);
                } else {
                    rc = tf_json_add_string(args, key, val);
                }
            }
            *eq = '='; /* restore */
            if (rc != TF_OK) goto fail;
        }
    }
    return args;
fail:
    cJSON_Delete(args);
    return NULL;
}

static int add_audit_option_arg(cJSON *args, const char *tok, char **error, const char *op_name);
static int audit_privacy_option_token(const char *tok);

static cJSON *ensure_validate_rules_array(cJSON *args) {
    cJSON *arr = cJSON_GetObjectItemCaseSensitive(args, "rules");
    if (arr) return cJSON_IsArray(arr) ? arr : NULL;
    arr = cJSON_CreateArray();
    if (!arr) return NULL;
    if (tf_json_add_item(args, "rules", arr) != TF_OK) return NULL;
    return arr;
}

static char *dsl_strdup_range(const char *start, size_t len) {
    char *out = malloc(len + 1);
    if (!out) return NULL;
    memcpy(out, start, len);
    out[len] = '\0';
    return out;
}

static int add_validate_rule_spec(cJSON *args, const char *spec, char **error) {
    const char *colon = strchr(spec ? spec : "", ':');
    if (!colon || colon == spec || !colon[1]) {
        set_error(error, "validate rule= expects name:expr");
        return TF_ERROR;
    }
    cJSON *arr = ensure_validate_rules_array(args);
    if (!arr) return TF_ERROR;
    char *name = dsl_strdup_range(spec, (size_t)(colon - spec));
    if (!name) return TF_ERROR;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) { free(name); return TF_ERROR; }
    if (tf_json_add_string(obj, "name", name) != TF_OK ||
        tf_json_add_string(obj, "expr", colon + 1) != TF_OK) {
        cJSON_Delete(obj);
        free(name);
        return TF_ERROR;
    }
    free(name);
    if (tf_json_add_array_item(arr, obj) != TF_OK) return TF_ERROR;
    return TF_OK;
}

static int add_validate_failure_rate_arg(cJSON *args, const char *key, const char *value, char **error) {
    char *end = NULL;
    double rate = strtod(value, &end);
    if (!value[0] || *end != '\0' || rate < 0.0 || rate > 1.0) {
        set_errorf(error, "validate: %s must be between 0 and 1", key);
        return TF_ERROR;
    }
    return tf_json_add_number(args, key, rate);
}

static int is_validate_rules_file_token(const char *tok) {
    return strncmp(tok, "rules_file=", 11) == 0 || strncmp(tok, "rules-file=", 11) == 0;
}

static cJSON *build_expr_audit_args(const token_list *tokens, char **error, const char *op_name) {
    /* filter/validate "expr" [audit=true] [audit_limit=N] [name=...] [message=...]
     * validate also accepts repeated rule=name:expr tokens and rules_file=PATH suites. */
    if (tokens->count < 2) {
        set_errorf(error, "%s requires an expression argument", op_name);
        return NULL;
    }
    int validate_rules_mode = strcmp(op_name, "validate") == 0 &&
                              strncmp(tokens->items[1], "rule=", 5) == 0;
    int validate_rules_file_mode = strcmp(op_name, "validate") == 0 &&
                                   is_validate_rules_file_token(tokens->items[1]);
    cJSON *args = cJSON_CreateObject();
    if (!args) return NULL;
    if (!validate_rules_mode && !validate_rules_file_mode &&
        tf_json_add_string(args, "expr", tokens->items[1]) != TF_OK) {
        cJSON_Delete(args);
        set_oom_error_if_unset(error);
        return NULL;
    }
    for (size_t i = (validate_rules_mode || validate_rules_file_mode) ? 1u : 2u; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        int audit_rc = add_audit_option_arg(args, tok, error, op_name);
        if (audit_rc < 0) { cJSON_Delete(args); return NULL; }
        if (audit_rc > 0) continue;
        if (strcmp(op_name, "validate") == 0 && strncmp(tok, "rule=", 5) == 0) {
            if (add_validate_rule_spec(args, tok + 5, error) != TF_OK) {
                cJSON_Delete(args);
                set_oom_error_if_unset(error);
                return NULL;
            }
        } else if (strcmp(op_name, "validate") == 0 && is_validate_rules_file_token(tok)) {
            const char *path = tok + 11;
            if (!path[0]) {
                cJSON_Delete(args);
                set_error(error, "validate: rules_file must be a non-empty string");
                return NULL;
            }
            if (tf_json_add_string(args, "rules_file", path) != TF_OK) {
                cJSON_Delete(args);
                set_oom_error_if_unset(error);
                return NULL;
            }
        } else if (strcmp(op_name, "validate") == 0 &&
            (strncmp(tok, "max_failures=", 13) == 0 || strncmp(tok, "max-failures=", 13) == 0)) {
            const char *value = tok + 13;
            char *end = NULL;
            long n = strtol(value, &end, 10);
            if (*end != '\0' || n < 0) {
                cJSON_Delete(args);
                set_error(error, "validate: max_failures must be a non-negative integer");
                return NULL;
            }
            if (tf_json_add_number(args, "max_failures", (double)n) != TF_OK) {
                cJSON_Delete(args);
                set_oom_error_if_unset(error);
                return NULL;
            }
        } else if (strcmp(op_name, "validate") == 0 &&
            (strncmp(tok, "max_failure_rate=", 17) == 0 || strncmp(tok, "max-failure-rate=", 17) == 0)) {
            if (add_validate_failure_rate_arg(args, "max_failure_rate", tok + 17, error) != TF_OK) {
                cJSON_Delete(args);
                set_oom_error_if_unset(error);
                return NULL;
            }
        } else if (strcmp(op_name, "validate") == 0 &&
            (strncmp(tok, "warn_failure_rate=", 18) == 0 || strncmp(tok, "warn-failure-rate=", 18) == 0)) {
            if (add_validate_failure_rate_arg(args, "warn_failure_rate", tok + 18, error) != TF_OK) {
                cJSON_Delete(args);
                set_oom_error_if_unset(error);
                return NULL;
            }
        } else if (strncmp(tok, "name=", 5) == 0) {
            if (tf_json_add_string(args, "name", tok + 5) != TF_OK) {
                cJSON_Delete(args);
                set_oom_error_if_unset(error);
                return NULL;
            }
        } else if (strncmp(tok, "message=", 8) == 0) {
            if (tf_json_add_string(args, "message", tok + 8) != TF_OK) {
                cJSON_Delete(args);
                set_oom_error_if_unset(error);
                return NULL;
            }
        } else {
            cJSON_Delete(args);
            set_errorf(error, "%s: unexpected argument '%s'", op_name, tok);
            return NULL;
        }
    }
    if (validate_rules_mode) {
        cJSON *rules = cJSON_GetObjectItemCaseSensitive(args, "rules");
        if (!cJSON_IsArray(rules) || cJSON_GetArraySize(rules) <= 0) {
            cJSON_Delete(args);
            set_error(error, "validate requires at least one rule=name:expr");
            return NULL;
        }
    }
    if (validate_rules_file_mode) {
        cJSON *rules_file = cJSON_GetObjectItemCaseSensitive(args, "rules_file");
        if (!cJSON_IsString(rules_file) || !rules_file->valuestring[0]) {
            cJSON_Delete(args);
            set_error(error, "validate requires rules_file=PATH");
            return NULL;
        }
    }
    return args;
}

static cJSON *build_quarantine_args(const token_list *tokens, char **error) {
    if (tokens->count < 2) {
        set_error(error, "quarantine requires an expression argument");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    if (!args) return NULL;
    if (tf_json_add_string(args, "expr", tokens->items[1]) != TF_OK) {
        cJSON_Delete(args);
        set_oom_error_if_unset(error);
        return NULL;
    }
    for (size_t i = 2; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (strncmp(tok, "name=", 5) == 0) {
            if (tf_json_add_string(args, "name", tok + 5) != TF_OK) {
                cJSON_Delete(args);
                set_oom_error_if_unset(error);
                return NULL;
            }
        } else if (strncmp(tok, "message=", 8) == 0) {
            if (tf_json_add_string(args, "message", tok + 8) != TF_OK) {
                cJSON_Delete(args);
                set_oom_error_if_unset(error);
                return NULL;
            }
        } else {
            int audit_rc = add_audit_option_arg(args, tok, error, "quarantine");
            if (audit_rc < 0) { cJSON_Delete(args); return NULL; }
            if (audit_rc == 0) {
                cJSON_Delete(args);
                set_errorf(error, "quarantine: unexpected argument '%s'", tok);
                return NULL;
            }
        }
    }
    return args;
}

static int assert_action_valid(const char *action) {
    return strcmp(action, "fail") == 0 || strcmp(action, "error") == 0 || strcmp(action, "stop") == 0 ||
           strcmp(action, "warn") == 0 || strcmp(action, "warning") == 0 ||
           strcmp(action, "filter") == 0 || strcmp(action, "drop") == 0 ||
           strcmp(action, "quarantine") == 0 || strcmp(action, "annotate") == 0;
}

static int assert_is_option_token(const char *tok) {
    return strncmp(tok, "action=", 7) == 0 || strncmp(tok, "mode=", 5) == 0 ||
           strncmp(tok, "name=", 5) == 0 || strncmp(tok, "message=", 8) == 0 ||
           strncmp(tok, "result=", 7) == 0 || strncmp(tok, "as=", 3) == 0 ||
           strcmp(tok, "audit") == 0 || strcmp(tok, "audit=true") == 0 ||
           strcmp(tok, "audit=false") == 0 || strncmp(tok, "audit_limit=", 12) == 0 ||
           strncmp(tok, "audit-limit=", 12) == 0 ||
           strncmp(tok, "audit_include_row=", 18) == 0 || strncmp(tok, "audit-include-row=", 18) == 0 ||
           strncmp(tok, "audit_columns=", 14) == 0 || strncmp(tok, "audit-columns=", 14) == 0 ||
           strncmp(tok, "audit_redact=", 13) == 0 || strncmp(tok, "audit-redact=", 13) == 0 ||
           strncmp(tok, "audit_hash_columns=", 19) == 0 || strncmp(tok, "audit-hash-columns=", 19) == 0 ||
           strncmp(tok, "audit_max_bytes=", 16) == 0 || strncmp(tok, "audit-max-bytes=", 16) == 0 ||
           strncmp(tok, "audit_max_cell_bytes=", 21) == 0 || strncmp(tok, "audit-max-cell-bytes=", 21) == 0 ||
           strncmp(tok, "aggregate=", 10) == 0 ||
           strncmp(tok, "agg=", 4) == 0 || strncmp(tok, "column=", 7) == 0 ||
           strncmp(tok, "col=", 4) == 0 || strncmp(tok, "op=", 3) == 0 ||
           strncmp(tok, "cmp=", 4) == 0 || strncmp(tok, "comparison=", 11) == 0 ||
           strncmp(tok, "value=", 6) == 0 || strncmp(tok, "threshold=", 10) == 0 ||
           strncmp(tok, "tolerance=", 10) == 0 || strncmp(tok, "tol=", 4) == 0 ||
           strncmp(tok, "rel=", 4) == 0 || strncmp(tok, "relative=", 9) == 0;
}

static int assert_parse_number_token(const char *value, const char *name, double *out, char **error) {
    if (!value) {
        set_errorf(error, "%s must be numeric", name);
        return TF_ERROR;
    }
    while (*value && isspace((unsigned char)*value)) value++;
    if (!*value) {
        set_errorf(error, "%s must be numeric", name);
        return TF_ERROR;
    }
    char *end = NULL;
    double parsed = strtod(value, &end);
    if (!end || end == value) {
        set_errorf(error, "%s must be numeric", name);
        return TF_ERROR;
    }
    while (*end && isspace((unsigned char)*end)) end++;
    if (*end != '\0') {
        set_errorf(error, "%s must be numeric", name);
        return TF_ERROR;
    }
    *out = parsed;
    return TF_OK;
}

static int parse_finite_number_token(const char *value, const char *name, double *out, char **error) {
    if (assert_parse_number_token(value, name, out, error) != TF_OK) return TF_ERROR;
    if (!isfinite(*out)) {
        set_errorf(error, "%s must be finite", name);
        return TF_ERROR;
    }
    return TF_OK;
}

static int add_h14_policy_option(cJSON *args, const char *tok, const char *op_name, char **error) {
    if (strncmp(tok, "missing=", 8) == 0) {
        const char *value = tok + 8;
        if (strcmp(value, "error") != 0 && strcmp(value, "null") != 0 && strcmp(value, "ignore") != 0) {
            set_errorf(error, "%s missing must be error, null, or ignore", op_name);
            return TF_ERROR;
        }
        if (tf_json_add_string(args, "missing", value) != TF_OK) {
            set_oom_error_if_unset(error);
            return TF_ERROR;
        }
        return TF_OK;
    }
    if (strncmp(tok, "on_type_error=", 14) == 0 || strncmp(tok, "on-type-error=", 14) == 0) {
        const char *value = tok + 14;
        if (strcmp(value, "fail") != 0 && strcmp(value, "null") != 0) {
            set_errorf(error, "%s on_type_error must be fail or null", op_name);
            return TF_ERROR;
        }
        if (tf_json_add_string(args, "on_type_error", value) != TF_OK) {
            set_oom_error_if_unset(error);
            return TF_ERROR;
        }
        return TF_OK;
    }
    return 1;
}

static int assert_parse_bool_token(const char *value, const char *name, int *out, char **error) {
    if (value && (strcmp(value, "true") == 0 || strcmp(value, "1") == 0 || strcmp(value, "yes") == 0)) {
        *out = 1;
        return TF_OK;
    }
    if (value && (strcmp(value, "false") == 0 || strcmp(value, "0") == 0 || strcmp(value, "no") == 0)) {
        *out = 0;
        return TF_OK;
    }
    set_errorf(error, "%s must be true or false", name);
    return TF_ERROR;
}

static cJSON *build_assert_args(const token_list *tokens, char **error) {
    if (tokens->count < 2) {
        set_error(error, "assert requires an expression argument or aggregate=...");
        return NULL;
    }
    const char *action = "fail";
    const char *name = "assert";
    const char *message = "";
    const char *result = "_assert";
    int audit = 0;
    size_t audit_limit = 0;
    int audit_include_row_set = 0;
    int audit_include_row = 1;
    const char *audit_columns = NULL;
    const char *audit_redact = NULL;
    const char *audit_hash_columns = NULL;
    size_t audit_max_bytes = 0;
    size_t audit_max_cell_bytes = 0;
    int has_aggregate = 0;
    int has_cmp = 0;
    int has_value = 0;
    int has_tolerance = 0;
    int has_rel = 0;
    double value = 0.0;
    double tolerance = 0.0;
    int rel = 1;
    const char *aggregate = NULL;
    const char *column = NULL;
    const char *cmp = NULL;
    size_t start_idx = 1;

    cJSON *args = cJSON_CreateObject();
    if (!args) return NULL;
    if (!assert_is_option_token(tokens->items[1])) {
        if (tf_json_add_string(args, "expr", tokens->items[1]) != TF_OK) {
            cJSON_Delete(args);
            set_oom_error_if_unset(error);
            return NULL;
        }
        start_idx = 2;
    }

    for (size_t i = start_idx; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (strncmp(tok, "action=", 7) == 0) action = tok + 7;
        else if (strncmp(tok, "mode=", 5) == 0) action = tok + 5;
        else if (strncmp(tok, "name=", 5) == 0) name = tok + 5;
        else if (strncmp(tok, "message=", 8) == 0) message = tok + 8;
        else if (strncmp(tok, "result=", 7) == 0) result = tok + 7;
        else if (strncmp(tok, "as=", 3) == 0) result = tok + 3;
        else if (strcmp(tok, "audit") == 0 || strcmp(tok, "audit=true") == 0) audit = 1;
        else if (strcmp(tok, "audit=false") == 0) audit = 0;
        else if (strncmp(tok, "audit_limit=", 12) == 0 || strncmp(tok, "audit-limit=", 12) == 0) {
            const char *v = tok + 12;
            if (dsl_parse_positive_size(v, "assert audit_limit", &audit_limit, error) != TF_OK) {
                cJSON_Delete(args);
                return NULL;
            }
        }
        else if (strncmp(tok, "audit_include_row=", 18) == 0 || strncmp(tok, "audit-include-row=", 18) == 0) {
            const char *v = strchr(tok, '=');
            v = v ? v + 1 : "";
            if (assert_parse_bool_token(v, "assert audit_include_row", &audit_include_row, error) != TF_OK) {
                cJSON_Delete(args);
                return NULL;
            }
            audit_include_row_set = 1;
        }
        else if (strncmp(tok, "audit_columns=", 14) == 0 || strncmp(tok, "audit-columns=", 14) == 0) {
            audit_columns = strchr(tok, '=');
            audit_columns = audit_columns ? audit_columns + 1 : "";
        }
        else if (strncmp(tok, "audit_redact=", 13) == 0 || strncmp(tok, "audit-redact=", 13) == 0) {
            audit_redact = strchr(tok, '=');
            audit_redact = audit_redact ? audit_redact + 1 : "";
        }
        else if (strncmp(tok, "audit_hash_columns=", 19) == 0 || strncmp(tok, "audit-hash-columns=", 19) == 0) {
            audit_hash_columns = strchr(tok, '=');
            audit_hash_columns = audit_hash_columns ? audit_hash_columns + 1 : "";
        }
        else if (strncmp(tok, "audit_max_bytes=", 16) == 0 || strncmp(tok, "audit-max-bytes=", 16) == 0) {
            const char *v = strchr(tok, '=');
            char *end = NULL;
            long n = strtol(v ? v + 1 : "", &end, 10);
            if (!v || v[1] == '\0' || !end || *end != '\0' || n < 0) {
                cJSON_Delete(args);
                set_error(error, "assert audit_max_bytes must be a non-negative integer");
                return NULL;
            }
            audit_max_bytes = (size_t)n;
        }
        else if (strncmp(tok, "audit_max_cell_bytes=", 21) == 0 || strncmp(tok, "audit-max-cell-bytes=", 21) == 0) {
            const char *v = strchr(tok, '=');
            char *end = NULL;
            long n = strtol(v ? v + 1 : "", &end, 10);
            if (!v || v[1] == '\0' || !end || *end != '\0' || n < 0) {
                cJSON_Delete(args);
                set_error(error, "assert audit_max_cell_bytes must be a non-negative integer");
                return NULL;
            }
            audit_max_cell_bytes = (size_t)n;
        }
        else if (strncmp(tok, "aggregate=", 10) == 0) { aggregate = tok + 10; has_aggregate = 1; }
        else if (strncmp(tok, "agg=", 4) == 0) { aggregate = tok + 4; has_aggregate = 1; }
        else if (strncmp(tok, "column=", 7) == 0) column = tok + 7;
        else if (strncmp(tok, "col=", 4) == 0) column = tok + 4;
        else if (strncmp(tok, "op=", 3) == 0) { cmp = tok + 3; has_cmp = 1; }
        else if (strncmp(tok, "cmp=", 4) == 0) { cmp = tok + 4; has_cmp = 1; }
        else if (strncmp(tok, "comparison=", 11) == 0) { cmp = tok + 11; has_cmp = 1; }
        else if (strncmp(tok, "value=", 6) == 0) {
            if (assert_parse_number_token(tok + 6, "assert value", &value, error) != TF_OK) { cJSON_Delete(args); return NULL; }
            has_value = 1;
        }
        else if (strncmp(tok, "threshold=", 10) == 0) {
            if (assert_parse_number_token(tok + 10, "assert threshold", &value, error) != TF_OK) { cJSON_Delete(args); return NULL; }
            has_value = 1;
        }
        else if (strncmp(tok, "tolerance=", 10) == 0) {
            if (assert_parse_number_token(tok + 10, "assert tolerance", &tolerance, error) != TF_OK || tolerance < 0.0) {
                if (!error || !*error) set_error(error, "assert tolerance must be a non-negative number");
                cJSON_Delete(args);
                return NULL;
            }
            has_tolerance = 1;
        }
        else if (strncmp(tok, "tol=", 4) == 0) {
            if (assert_parse_number_token(tok + 4, "assert tolerance", &tolerance, error) != TF_OK || tolerance < 0.0) {
                if (!error || !*error) set_error(error, "assert tolerance must be a non-negative number");
                cJSON_Delete(args);
                return NULL;
            }
            has_tolerance = 1;
        }
        else if (strncmp(tok, "rel=", 4) == 0) {
            if (assert_parse_bool_token(tok + 4, "assert rel", &rel, error) != TF_OK) { cJSON_Delete(args); return NULL; }
            has_rel = 1;
        }
        else if (strncmp(tok, "relative=", 9) == 0) {
            if (assert_parse_bool_token(tok + 9, "assert relative", &rel, error) != TF_OK) { cJSON_Delete(args); return NULL; }
            has_rel = 1;
        }
        else {
            cJSON_Delete(args);
            set_errorf(error, "assert: unexpected argument '%s'", tok);
            return NULL;
        }
    }
    if (!assert_action_valid(action)) {
        cJSON_Delete(args);
        set_error(error, "assert action must be fail, warn, filter, quarantine, or annotate");
        return NULL;
    }
    if (has_aggregate) {
        if (!aggregate || !aggregate[0]) {
            cJSON_Delete(args);
            set_error(error, "assert aggregate requires a name");
            return NULL;
        }
        if (cJSON_GetObjectItemCaseSensitive(args, "expr")) {
            cJSON_Delete(args);
            set_error(error, "assert cannot combine expr and aggregate");
            return NULL;
        }
        if (!has_cmp) {
            cJSON_Delete(args);
            set_error(error, "assert aggregate requires op=...");
            return NULL;
        }
        if (!has_value) {
            cJSON_Delete(args);
            set_error(error, "assert aggregate requires value=...");
            return NULL;
        }
        if (tf_json_add_string(args, "aggregate", aggregate) != TF_OK ||
            (column && column[0] && tf_json_add_string(args, "column", column) != TF_OK) ||
            tf_json_add_string(args, "op", cmp) != TF_OK ||
            tf_json_add_number(args, "value", value) != TF_OK ||
            (has_tolerance && tf_json_add_number(args, "tolerance", tolerance) != TF_OK) ||
            (has_rel && tf_json_add_bool(args, "rel", rel ? 1 : 0) != TF_OK)) {
            cJSON_Delete(args);
            set_oom_error_if_unset(error);
            return NULL;
        }
    } else if (has_tolerance || has_rel) {
        cJSON_Delete(args);
        set_error(error, "assert tolerance and rel require aggregate=...");
        return NULL;
    } else if (!cJSON_GetObjectItemCaseSensitive(args, "expr")) {
        cJSON_Delete(args);
        set_error(error, "assert requires an expression argument or aggregate=...");
        return NULL;
    }
    if (tf_json_add_string(args, "action", action) != TF_OK ||
        tf_json_add_string(args, "name", name && name[0] ? name : "assert") != TF_OK ||
        tf_json_add_string(args, "message", message ? message : "") != TF_OK ||
        tf_json_add_string(args, "result", result && result[0] ? result : "_assert") != TF_OK ||
        (audit && tf_json_add_bool(args, "audit", 1) != TF_OK) ||
        (audit_limit > 0 && tf_json_add_number(args, "audit_limit", (double)audit_limit) != TF_OK) ||
        (audit_include_row_set && tf_json_add_bool(args, "audit_include_row", audit_include_row) != TF_OK)) {
        cJSON_Delete(args);
        set_oom_error_if_unset(error);
        return NULL;
    }
    if (audit_columns && audit_columns[0] && add_audit_csv_arg(args, "audit_columns", audit_columns) != TF_OK) {
        cJSON_Delete(args);
        set_oom_error_if_unset(error);
        return NULL;
    }
    if (audit_redact && audit_redact[0] && add_audit_csv_arg(args, "audit_redact", audit_redact) != TF_OK) {
        cJSON_Delete(args);
        set_oom_error_if_unset(error);
        return NULL;
    }
    if (audit_hash_columns && audit_hash_columns[0] && add_audit_csv_arg(args, "audit_hash_columns", audit_hash_columns) != TF_OK) {
        cJSON_Delete(args);
        set_oom_error_if_unset(error);
        return NULL;
    }
    if ((audit_max_bytes > 0 && tf_json_add_number(args, "audit_max_bytes", (double)audit_max_bytes) != TF_OK) ||
        (audit_max_cell_bytes > 0 && tf_json_add_number(args, "audit_max_cell_bytes", (double)audit_max_cell_bytes) != TF_OK)) {
        cJSON_Delete(args);
        set_oom_error_if_unset(error);
        return NULL;
    }
    return args;
}

static int dsl_is_tee_option_token(const char *tok) {
    return tok && (strncmp(tok, "channel=", 8) == 0 ||
                   strncmp(tok, "name=", 5) == 0 ||
                   strncmp(tok, "limit=", 6) == 0 ||
                   strncmp(tok, "max_rows=", 9) == 0 ||
                   strncmp(tok, "every=", 6) == 0 ||
                   strncmp(tok, "columns=", 8) == 0 ||
                   strncmp(tok, "cols=", 5) == 0 ||
                   strncmp(tok, "include_row=", 12) == 0 ||
                   audit_privacy_option_token(tok));
}

static int dsl_parse_positive_size(const char *text, const char *name, size_t *out, char **error) {
    char *end = NULL;
    long v = strtol(text, &end, 10);
    if (!text || *text == '\0' || *end != '\0' || v <= 0) {
        set_errorf(error, "%s must be a positive integer", name);
        return TF_ERROR;
    }
    *out = (size_t)v;
    return TF_OK;
}

static int dsl_set_object_item(cJSON *obj, const char *key, cJSON *item) {
    if (!obj || !key || !item) return TF_ERROR;
    if (!cJSON_ReplaceItemInObject(obj, key, item)) {
        return tf_json_add_item(obj, key, item);
    }
    return TF_OK;
}

static int dsl_set_string_arg(cJSON *obj, const char *key, const char *value) {
    cJSON *item = cJSON_CreateString(value ? value : "");
    if (!item) return TF_ERROR;
    return dsl_set_object_item(obj, key, item);
}

static int dsl_set_number_arg(cJSON *obj, const char *key, double value) {
    cJSON *item = cJSON_CreateNumber(value);
    if (!item) return TF_ERROR;
    return dsl_set_object_item(obj, key, item);
}

static int dsl_set_bool_arg(cJSON *obj, const char *key, int value) {
    cJSON *item = cJSON_CreateBool(value ? 1 : 0);
    if (!item) return TF_ERROR;
    return dsl_set_object_item(obj, key, item);
}

static int dsl_add_string_array_item(cJSON *arr, const char *value) {
    cJSON *item = cJSON_CreateString(value ? value : "");
    if (!item) return TF_ERROR;
    return tf_json_add_array_item(arr, item);
}

static int dsl_add_array_duplicate(cJSON *arr, cJSON *src) {
    cJSON *item = cJSON_Duplicate(src, 1);
    if (!item) return TF_ERROR;
    return tf_json_add_array_item(arr, item);
}

static int dsl_add_csv_list(cJSON *arr, const char *text) {
    char *copy = strdup(text ? text : "");
    if (!copy) return TF_ERROR;
    char *tok = strtok(copy, ",");
    while (tok) {
        while (*tok == ' ' || *tok == '	') tok++;
        size_t len = strlen(tok);
        while (len > 0 && (tok[len - 1] == ' ' || tok[len - 1] == '	')) tok[--len] = '\0';
        if (len > 0) {
            if (dsl_add_string_array_item(arr, tok) != TF_OK) {
                free(copy);
                return TF_ERROR;
            }
        }
        tok = strtok(NULL, ",");
    }
    free(copy);
    return TF_OK;
}

static cJSON *build_tee_args(const token_list *tokens, char **error) {
    cJSON *args = cJSON_CreateObject();
    if (!args) return NULL;
    if (tf_json_add_string(args, "channel", "samples") != TF_OK ||
        tf_json_add_number(args, "limit", 1000) != TF_OK ||
        tf_json_add_number(args, "every", 1) != TF_OK ||
        tf_json_add_string(args, "name", "tee") != TF_OK ||
        tf_json_add_bool(args, "include_row", 1) != TF_OK) {
        cJSON_Delete(args);
        set_oom_error_if_unset(error);
        return NULL;
    }

    size_t i = 1;
    if (i < tokens->count && !dsl_is_tee_option_token(tokens->items[i])) {
        if (tf_json_add_string(args, "expr", tokens->items[i]) != TF_OK) {
            cJSON_Delete(args);
            set_oom_error_if_unset(error);
            return NULL;
        }
        i++;
    }

    for (; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (strncmp(tok, "channel=", 8) == 0) {
            const char *v = tok + 8;
            if (strcmp(v, "samples") != 0 && strcmp(v, "sample") != 0 &&
                strcmp(v, "errors") != 0 && strcmp(v, "error") != 0 &&
                strcmp(v, "stats") != 0 && strcmp(v, "audit") != 0) {
                cJSON_Delete(args);
                set_error(error, "tee channel must be samples, errors, stats, or audit");
                return NULL;
            }
            if (dsl_set_string_arg(args, "channel", v) != TF_OK) goto oom;
        } else if (strncmp(tok, "name=", 5) == 0) {
            if (dsl_set_string_arg(args, "name", tok + 5) != TF_OK) goto oom;
        } else if (strncmp(tok, "limit=", 6) == 0 || strncmp(tok, "max_rows=", 9) == 0) {
            const char *v = tok + (strncmp(tok, "limit=", 6) == 0 ? 6 : 9);
            size_t parsed = 0;
            if (dsl_parse_positive_size(v, "tee limit", &parsed, error) != TF_OK) {
                cJSON_Delete(args);
                return NULL;
            }
            if (dsl_set_number_arg(args, "limit", (double)parsed) != TF_OK) goto oom;
        } else if (strncmp(tok, "every=", 6) == 0) {
            size_t parsed = 0;
            if (dsl_parse_positive_size(tok + 6, "tee every", &parsed, error) != TF_OK) {
                cJSON_Delete(args);
                return NULL;
            }
            if (dsl_set_number_arg(args, "every", (double)parsed) != TF_OK) goto oom;
        } else if (strncmp(tok, "columns=", 8) == 0 || strncmp(tok, "cols=", 5) == 0) {
            const char *v = tok + (strncmp(tok, "columns=", 8) == 0 ? 8 : 5);
            cJSON *arr = cJSON_CreateArray();
            if (!arr || dsl_add_csv_list(arr, v) != TF_OK || cJSON_GetArraySize(arr) == 0) {
                cJSON_Delete(arr);
                cJSON_Delete(args);
                set_error(error, "tee columns must be a non-empty comma-separated list");
                return NULL;
            }
            if (dsl_set_object_item(args, "columns", arr) != TF_OK) goto oom;
        } else if (strncmp(tok, "include_row=", 12) == 0) {
            const char *v = tok + 12;
            if (strcmp(v, "true") != 0 && strcmp(v, "false") != 0) {
                cJSON_Delete(args);
                set_error(error, "tee include_row must be true or false");
                return NULL;
            }
            if (dsl_set_bool_arg(args, "include_row", strcmp(v, "true") == 0) != TF_OK) goto oom;
        } else {
            int audit_rc = add_audit_option_arg(args, tok, error, "tee");
            if (audit_rc < 0) { cJSON_Delete(args); return NULL; }
            if (audit_rc == 0) {
                cJSON_Delete(args);
                set_errorf(error, "tee: unexpected argument '%s'", tok);
                return NULL;
            }
        }
    }
    return args;
oom:
    cJSON_Delete(args);
    set_oom_error_if_unset(error);
    return NULL;
}

static cJSON *ensure_object_item(cJSON *parent, const char *name) {
    cJSON *obj = cJSON_GetObjectItemCaseSensitive(parent, name);
    if (obj) return cJSON_IsObject(obj) ? obj : NULL;
    obj = cJSON_CreateObject();
    if (!obj) return NULL;
    if (tf_json_add_item(parent, name, obj) != TF_OK) return NULL;
    return obj;
}

static cJSON *ensure_array_item(cJSON *parent, const char *name) {
    cJSON *arr = cJSON_GetObjectItemCaseSensitive(parent, name);
    if (arr) return cJSON_IsArray(arr) ? arr : NULL;
    arr = cJSON_CreateArray();
    if (!arr) return NULL;
    if (tf_json_add_item(parent, name, arr) != TF_OK) return NULL;
    return arr;
}

static int add_csv_strings(cJSON *arr, const char *text) {
    char *copy = strdup(text ? text : "");
    if (!copy) return TF_ERROR;
    char *tok = strtok(copy, ",");
    while (tok) {
        if (*tok) {
            if (dsl_add_string_array_item(arr, tok) != TF_OK) {
                free(copy);
                return TF_ERROR;
            }
        }
        tok = strtok(NULL, ",");
    }
    free(copy);
    return TF_OK;
}

static int add_schema_columns(cJSON *args, const char *text) {
    cJSON *cols = ensure_object_item(args, "columns");
    if (!cols) return TF_ERROR;
    const char *spec = text ? text : "";
    const char *p = spec;
    const char *end = spec + strlen(spec);
    while (p < end) {
        const char *comma = dsl_find_top_level_comma(p, end);
        size_t len = comma ? (size_t)(comma - p) : (size_t)(end - p);
        char *tok = trimmed_token_copy(p, len);
        if (!tok) return TF_ERROR;
        if (tok[0]) {
            char *colon = strchr(tok, ':');
            if (colon) {
                *colon = '\0';
                if (tf_json_add_string(cols, tok, colon + 1) != TF_OK) {
                    free(tok);
                    return TF_ERROR;
                }
            } else {
                if (tf_json_add_string(cols, tok, "any") != TF_OK) {
                    free(tok);
                    return TF_ERROR;
                }
            }
        }
        free(tok);
        if (!comma) break;
        p = comma + 1;
    }
    return TF_OK;
}


static int add_schema_map_value(cJSON *args, const char *key, const char *spec, int value_list) {
    cJSON *obj = ensure_object_item(args, key);
    if (!obj) return TF_ERROR;
    char *copy = strdup(spec ? spec : "");
    if (!copy) return TF_ERROR;
    char *colon = strchr(copy, ':');
    if (!colon || colon == copy) { free(copy); return TF_ERROR; }
    *colon = '\0';
    const char *col = copy;
    const char *val = colon + 1;
    if (value_list) {
        cJSON *arr = cJSON_CreateArray();
        if (!arr) { free(copy); return TF_ERROR; }
        if (add_csv_strings(arr, val) != TF_OK) { cJSON_Delete(arr); free(copy); return TF_ERROR; }
        if (tf_json_add_item(obj, col, arr) != TF_OK) { free(copy); return TF_ERROR; }
    } else {
        char *end = NULL;
        double num = strtod(val, &end);
        if (end && *end == '\0' && end != val) {
            if (tf_json_add_number(obj, col, num) != TF_OK) { free(copy); return TF_ERROR; }
        } else {
            if (tf_json_add_string(obj, col, val) != TF_OK) { free(copy); return TF_ERROR; }
        }
    }
    free(copy);
    return TF_OK;
}

static cJSON *build_schema_infer_args(const token_list *tokens, char **error, int skip_infer_token) {
    cJSON *args = cJSON_CreateObject();
    if (!args) return NULL;
    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (skip_infer_token && i == 1 && strcmp(tok, "infer") == 0) continue;
        if (strncmp(tok, "rows=", 5) == 0 || strncmp(tok, "guess_max=", 10) == 0) {
            const char *val = strchr(tok, '=');
            val = val ? val + 1 : "";
            char *end = NULL;
            long n = strtol(val, &end, 10);
            if (!val[0] || !end || *end != '\0' || n <= 0) goto invalid;
            if (tf_json_add_number(args, "rows", (double)n) != TF_OK) goto oom;
        } else {
            goto invalid;
        }
    }
    return args;
oom:
    cJSON_Delete(args);
    set_oom_error_if_unset(error);
    return NULL;
invalid:
    cJSON_Delete(args);
    set_error(error, "schema infer: expected rows=N with a positive row count");
    return NULL;
}

static cJSON *build_schema_args(const token_list *tokens, char **error) {
    cJSON *args = cJSON_CreateObject();
    if (!args) return NULL;
    const char *mode = "fail";
    const char *name = "schema";
    const char *message = "";
    const char *result = "_schema";
    int audit = 0;
    int audit_include_row_set = 0;
    int audit_include_row = 1;
    const char *audit_columns = NULL;
    const char *audit_redact = NULL;
    const char *audit_hash_columns = NULL;
    size_t audit_limit = 0;
    size_t audit_max_bytes = 0;
    size_t audit_max_cell_bytes = 0;
    size_t max_regex_pattern_bytes = 0;
    size_t max_regex_cell_bytes = 0;
    int allow_extra_columns_set = 0;
    int allow_extra_columns = 1;
    int require_values_seen_set = 0;
    int require_values_seen = 0;
    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (strncmp(tok, "columns=", 8) == 0) {
            if (add_schema_columns(args, tok + 8) != TF_OK) goto invalid;
        } else if (strncmp(tok, "required=", 9) == 0) {
            cJSON *arr = ensure_array_item(args, "required");
            if (!arr || add_csv_strings(arr, tok + 9) != TF_OK) goto invalid;
        } else if (strncmp(tok, "non_null=", 9) == 0 || strncmp(tok, "non-null=", 9) == 0) {
            cJSON *arr = ensure_array_item(args, "non_null");
            if (!arr || add_csv_strings(arr, tok + 9) != TF_OK) goto invalid;
        } else if (strncmp(tok, "nullable=", 9) == 0) {
            cJSON *arr = ensure_array_item(args, "nullable");
            if (!arr || add_csv_strings(arr, tok + 9) != TF_OK) goto invalid;
        } else if (strncmp(tok, "values=", 7) == 0) {
            if (add_schema_map_value(args, "values", tok + 7, 1) != TF_OK) goto invalid;
        } else if (strncmp(tok, "min=", 4) == 0) {
            if (add_schema_map_value(args, "min", tok + 4, 0) != TF_OK) goto invalid;
        } else if (strncmp(tok, "max=", 4) == 0) {
            if (add_schema_map_value(args, "max", tok + 4, 0) != TF_OK) goto invalid;
        } else if (strncmp(tok, "regex=", 6) == 0) {
            if (add_schema_map_value(args, "regex", tok + 6, 0) != TF_OK) goto invalid;
        } else if (strncmp(tok, "max_regex_pattern_bytes=", 24) == 0 || strncmp(tok, "max-regex-pattern-bytes=", 24) == 0) {
            const char *val = strchr(tok, '=');
            val = val ? val + 1 : "";
            char *end = NULL;
            long n = strtol(val, &end, 10);
            if (!val[0] || !end || *end != '\0' || n <= 0) goto invalid;
            max_regex_pattern_bytes = (size_t)n;
        } else if (strncmp(tok, "max_regex_cell_bytes=", 21) == 0 || strncmp(tok, "max-regex-cell-bytes=", 21) == 0) {
            const char *val = strchr(tok, '=');
            val = val ? val + 1 : "";
            char *end = NULL;
            long n = strtol(val, &end, 10);
            if (!val[0] || !end || *end != '\0' || n <= 0) goto invalid;
            max_regex_cell_bytes = (size_t)n;
        } else if (strncmp(tok, "allow_extra_columns=", 20) == 0 || strncmp(tok, "allow-extra-columns=", 20) == 0) {
            const char *val = strchr(tok, '=');
            val = val ? val + 1 : "";
            if (strcmp(val, "true") != 0 && strcmp(val, "false") != 0) goto invalid;
            allow_extra_columns = strcmp(val, "true") == 0;
            allow_extra_columns_set = 1;
        } else if (strncmp(tok, "require_values_seen=", 20) == 0 || strncmp(tok, "require-values-seen=", 20) == 0) {
            const char *val = strchr(tok, '=');
            val = val ? val + 1 : "";
            if (strcmp(val, "true") != 0 && strcmp(val, "false") != 0) goto invalid;
            require_values_seen = strcmp(val, "true") == 0;
            require_values_seen_set = 1;
        } else if (strncmp(tok, "mode=", 5) == 0) mode = tok + 5;
        else if (strncmp(tok, "action=", 7) == 0) mode = tok + 7;
        else if (strncmp(tok, "name=", 5) == 0) name = tok + 5;
        else if (strncmp(tok, "message=", 8) == 0) message = tok + 8;
        else if (strncmp(tok, "result=", 7) == 0) result = tok + 7;
        else if (strncmp(tok, "as=", 3) == 0) result = tok + 3;
        else if (strcmp(tok, "audit") == 0 || strcmp(tok, "audit=true") == 0) audit = 1;
        else if (strcmp(tok, "audit=false") == 0) audit = 0;
        else if (strncmp(tok, "audit_limit=", 12) == 0 || strncmp(tok, "audit-limit=", 12) == 0) {
            const char *val = strchr(tok, '=');
            val = val ? val + 1 : "";
            char *end = NULL;
            long n = strtol(val, &end, 10);
            if (!val[0] || !end || *end != '\0' || n <= 0) goto invalid;
            audit_limit = (size_t)n;
            audit = 1;
        } else if (strncmp(tok, "audit_include_row=", 18) == 0 || strncmp(tok, "audit-include-row=", 18) == 0) {
            const char *val = strchr(tok, '=');
            val = val ? val + 1 : "";
            if (strcmp(val, "true") != 0 && strcmp(val, "false") != 0) goto invalid;
            audit_include_row = strcmp(val, "true") == 0;
            audit_include_row_set = 1;
        } else if (strncmp(tok, "audit_columns=", 14) == 0 || strncmp(tok, "audit-columns=", 14) == 0) {
            audit_columns = strchr(tok, '=');
            audit_columns = audit_columns ? audit_columns + 1 : "";
        } else if (strncmp(tok, "audit_redact=", 13) == 0 || strncmp(tok, "audit-redact=", 13) == 0) {
            audit_redact = strchr(tok, '=');
            audit_redact = audit_redact ? audit_redact + 1 : "";
        } else if (strncmp(tok, "audit_hash_columns=", 19) == 0 || strncmp(tok, "audit-hash-columns=", 19) == 0) {
            audit_hash_columns = strchr(tok, '=');
            audit_hash_columns = audit_hash_columns ? audit_hash_columns + 1 : "";
        } else if (strncmp(tok, "audit_max_bytes=", 16) == 0 || strncmp(tok, "audit-max-bytes=", 16) == 0) {
            const char *val = strchr(tok, '=');
            val = val ? val + 1 : "";
            char *end = NULL;
            long n = strtol(val, &end, 10);
            if (!val[0] || !end || *end != '\0' || n < 0) goto invalid;
            audit_max_bytes = (size_t)n;
        } else if (strncmp(tok, "audit_max_cell_bytes=", 21) == 0 || strncmp(tok, "audit-max-cell-bytes=", 21) == 0) {
            const char *val = strchr(tok, '=');
            val = val ? val + 1 : "";
            char *end = NULL;
            long n = strtol(val, &end, 10);
            if (!val[0] || !end || *end != '\0' || n < 0) goto invalid;
            audit_max_cell_bytes = (size_t)n;
        } else if (strchr(tok, ':') != NULL) {
            if (add_schema_columns(args, tok) != TF_OK) goto invalid;
        } else {
            goto invalid;
        }
    }
    if (!assert_action_valid(mode)) {
        cJSON_Delete(args);
        set_error(error, "schema mode must be fail, warn, filter, quarantine, or annotate");
        return NULL;
    }
    if (tf_json_add_string(args, "mode", mode) != TF_OK ||
        tf_json_add_string(args, "action", mode) != TF_OK ||
        tf_json_add_string(args, "name", name && name[0] ? name : "schema") != TF_OK ||
        tf_json_add_string(args, "message", message ? message : "") != TF_OK ||
        tf_json_add_string(args, "result", result && result[0] ? result : "_schema") != TF_OK ||
        (audit && tf_json_add_bool(args, "audit", 1) != TF_OK) ||
        (audit_limit > 0 && tf_json_add_number(args, "audit_limit", (double)audit_limit) != TF_OK) ||
        (audit_include_row_set && tf_json_add_bool(args, "audit_include_row", audit_include_row) != TF_OK)) {
        goto oom;
    }
    if (audit_columns && audit_columns[0]) {
        if (add_audit_csv_arg(args, "audit_columns", audit_columns) != TF_OK) goto oom;
    }
    if (audit_redact && audit_redact[0]) {
        if (add_audit_csv_arg(args, "audit_redact", audit_redact) != TF_OK) goto oom;
    }
    if (audit_hash_columns && audit_hash_columns[0]) {
        if (add_audit_csv_arg(args, "audit_hash_columns", audit_hash_columns) != TF_OK) goto oom;
    }
    if ((audit_max_bytes > 0 && tf_json_add_number(args, "audit_max_bytes", (double)audit_max_bytes) != TF_OK) ||
        (audit_max_cell_bytes > 0 && tf_json_add_number(args, "audit_max_cell_bytes", (double)audit_max_cell_bytes) != TF_OK) ||
        (max_regex_pattern_bytes > 0 && tf_json_add_number(args, "max_regex_pattern_bytes", (double)max_regex_pattern_bytes) != TF_OK) ||
        (max_regex_cell_bytes > 0 && tf_json_add_number(args, "max_regex_cell_bytes", (double)max_regex_cell_bytes) != TF_OK) ||
        (allow_extra_columns_set && tf_json_add_bool(args, "allow_extra_columns", allow_extra_columns) != TF_OK) ||
        (require_values_seen_set && tf_json_add_bool(args, "require_values_seen", require_values_seen) != TF_OK)) {
        goto oom;
    }
    return args;
oom:
    cJSON_Delete(args);
    set_oom_error_if_unset(error);
    return NULL;
invalid:
    cJSON_Delete(args);
    set_error(error, "schema: invalid or unexpected argument");
    return NULL;
}

static cJSON *build_select_args(const token_list *tokens, char **error) {
    /* select name,age or select name age — tokens[1..n] are column names */
    if (tokens->count < 2) {
        set_error(error, "select requires at least one column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *cols = cJSON_CreateArray();
    if (!args || !cols) goto oom;
    for (size_t i = 1; i < tokens->count; i++) {
        if (dsl_add_string_array_item(cols, tokens->items[i]) != TF_OK) goto oom;
    }
    if (tf_json_add_item(args, "columns", cols) != TF_OK) { cols = NULL; goto oom; }
    return args;
oom:
    cJSON_Delete(args);
    cJSON_Delete(cols);
    set_oom_error_if_unset(error);
    return NULL;
}

static cJSON *build_relocate_args(const token_list *tokens, char **error) {
    /* relocate col[,col...] [before=anchor|after=anchor] */
    if (tokens->count < 2) {
        set_error(error, "relocate requires at least one column name");
        return NULL;
    }

    cJSON *args = cJSON_CreateObject();
    cJSON *cols = cJSON_CreateArray();
    if (!args || !cols) goto oom;
    bool has_before = false;
    bool has_after = false;

    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (strncmp(tok, "before=", 7) == 0 || strncmp(tok, ".before=", 8) == 0) {
            const char *value = strchr(tok, '=') + 1;
            if (*value == '\0') {
                cJSON_Delete(args);
                cJSON_Delete(cols);
                set_error(error, "relocate: before requires a column name");
                return NULL;
            }
            if (has_after) {
                cJSON_Delete(args);
                cJSON_Delete(cols);
                set_error(error, "relocate: before and after are mutually exclusive");
                return NULL;
            }
            if (tf_json_add_string(args, "before", value) != TF_OK) goto oom;
            has_before = true;
            continue;
        }
        if (strncmp(tok, "after=", 6) == 0 || strncmp(tok, ".after=", 7) == 0) {
            const char *value = strchr(tok, '=') + 1;
            if (*value == '\0') {
                cJSON_Delete(args);
                cJSON_Delete(cols);
                set_error(error, "relocate: after requires a column name");
                return NULL;
            }
            if (has_before) {
                cJSON_Delete(args);
                cJSON_Delete(cols);
                set_error(error, "relocate: before and after are mutually exclusive");
                return NULL;
            }
            if (tf_json_add_string(args, "after", value) != TF_OK) goto oom;
            has_after = true;
            continue;
        }

        if (add_comma_columns(cols, tok, error, "relocate") != 0) {
            cJSON_Delete(args);
            cJSON_Delete(cols);
            return NULL;
        }

    }

    if (cJSON_GetArraySize(cols) <= 0) {
        cJSON_Delete(args);
        cJSON_Delete(cols);
        set_error(error, "relocate requires at least one column name");
        return NULL;
    }

    if (tf_json_add_item(args, "columns", cols) != TF_OK) { cols = NULL; goto oom; }
    return args;
oom:
    cJSON_Delete(args);
    cJSON_Delete(cols);
    set_oom_error_if_unset(error);
    return NULL;
}

static cJSON *build_rename_args(const token_list *tokens, char **error) {
    /* rename old=new,old2=new2 or rename old=new old2=new2 */
    if (tokens->count < 2) {
        set_error(error, "rename requires at least one old=new mapping");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *mapping = cJSON_CreateObject();
    if (!args || !mapping) {
        cJSON_Delete(args);
        cJSON_Delete(mapping);
        set_error(error, "out of memory building rename args");
        return NULL;
    }

    for (size_t i = 1; i < tokens->count; i++) {
        /* Each token might be "old=new" or comma-separated "old=new,old2=new2" */
        char *tok = tokens->items[i];
        /* Split on commas within this token */
        char *saveptr = NULL;
        char *copy = strdup(tok);
        if (!copy) {
            cJSON_Delete(mapping);
            cJSON_Delete(args);
            set_error(error, "out of memory building rename args");
            return NULL;
        }
        char *part = strtok_r(copy, ",", &saveptr);
        while (part) {
            char *eq = strchr(part, '=');
            if (!eq) {
                set_errorf(error, "invalid rename mapping: '%s' (expected old=new)", part);
                free(copy);
                cJSON_Delete(mapping);
                cJSON_Delete(args);
                return NULL;
            }
            *eq = '\0';
            if (tf_json_add_string(mapping, part, eq + 1) != TF_OK) {
                free(copy);
                cJSON_Delete(mapping);
                cJSON_Delete(args);
                set_error(error, "out of memory building rename args");
                return NULL;
            }
            part = strtok_r(NULL, ",", &saveptr);
        }
        free(copy);
    }

    if (tf_json_add_item(args, "mapping", mapping) != TF_OK) {
        mapping = NULL;
        cJSON_Delete(args);
        set_error(error, "out of memory building rename args");
        return NULL;
    }
    return args;
}


static int audit_privacy_option_token(const char *tok) {
    return (strncmp(tok, "audit_include_row=", 18) == 0 || strncmp(tok, "audit-include-row=", 18) == 0 ||
            strncmp(tok, "audit_columns=", 14) == 0 || strncmp(tok, "audit-columns=", 14) == 0 ||
            strncmp(tok, "audit_redact=", 13) == 0 || strncmp(tok, "audit-redact=", 13) == 0 ||
            strncmp(tok, "audit_hash_columns=", 19) == 0 || strncmp(tok, "audit-hash-columns=", 19) == 0 ||
            strncmp(tok, "audit_max_bytes=", 16) == 0 || strncmp(tok, "audit-max-bytes=", 16) == 0 ||
            strncmp(tok, "audit_max_cell_bytes=", 21) == 0 || strncmp(tok, "audit-max-cell-bytes=", 21) == 0);
}

static int audit_privacy_supported(const char *op_name) {
    return op_name && (strcmp(op_name, "filter") == 0 || strcmp(op_name, "validate") == 0 ||
                       strcmp(op_name, "assert") == 0 || strcmp(op_name, "json-schema") == 0 ||
                       strcmp(op_name, "schema") == 0 || strcmp(op_name, "fill-null") == 0 ||
                       strcmp(op_name, "replace") == 0 || strcmp(op_name, "cast") == 0 ||
                       strcmp(op_name, "normalize") == 0 || strcmp(op_name, "frequency") == 0 ||
                       strcmp(op_name, "tee") == 0 || strcmp(op_name, "quarantine") == 0);
}

static int add_audit_csv_arg(cJSON *args, const char *key, const char *spec) {
    cJSON *arr = cJSON_CreateArray();
    if (!arr || add_csv_strings(arr, spec) != TF_OK) {
        cJSON_Delete(arr);
        return TF_ERROR;
    }
    return dsl_set_object_item(args, key, arr);
}

static int audit_add_result(int rc, char **error) {
    if (rc == TF_OK) return 1;
    set_oom_error_if_unset(error);
    return -1;
}

static int add_audit_option_arg(cJSON *args, const char *tok, char **error, const char *op_name) {
    if (strcmp(tok, "audit") == 0) {
        return audit_add_result(tf_json_add_bool(args, "audit", 1), error);
    }
    if (strcmp(tok, "audit=false") == 0) {
        return audit_add_result(tf_json_add_bool(args, "audit", 0), error);
    }
    if (strcmp(tok, "audit=true") == 0) {
        return audit_add_result(tf_json_add_bool(args, "audit", 1), error);
    }
    if (strncmp(tok, "audit_limit=", 12) == 0 || strncmp(tok, "audit-limit=", 12) == 0) {
        const char *value = tok + 12;
        char *end = NULL;
        long n = strtol(value, &end, 10);
        if (*end != '\0' || n <= 0) {
            set_errorf(error, "%s: audit_limit must be a positive integer", op_name);
            return -1;
        }
        return audit_add_result(tf_json_add_number(args, "audit_limit", (double)n), error);
    }
    if (!audit_privacy_option_token(tok)) return 0;
    if (!audit_privacy_supported(op_name)) {
        set_errorf(error, "%s: audit privacy options are not supported for this operator yet", op_name);
        return -1;
    }
    if (strncmp(tok, "audit_include_row=", 18) == 0 || strncmp(tok, "audit-include-row=", 18) == 0) {
        const char *value = strchr(tok, '=');
        value = value ? value + 1 : "";
        if (strcmp(value, "true") != 0 && strcmp(value, "false") != 0) {
            set_errorf(error, "%s: audit_include_row must be true or false", op_name);
            return -1;
        }
        return audit_add_result(tf_json_add_bool(args, "audit_include_row", strcmp(value, "true") == 0), error);
    }
    if (strncmp(tok, "audit_columns=", 14) == 0 || strncmp(tok, "audit-columns=", 14) == 0) {
        const char *value = strchr(tok, '=');
        if (!value || add_audit_csv_arg(args, "audit_columns", value + 1) != TF_OK) {
            set_errorf(error, "%s: invalid audit_columns", op_name);
            return -1;
        }
        return 1;
    }
    if (strncmp(tok, "audit_redact=", 13) == 0 || strncmp(tok, "audit-redact=", 13) == 0) {
        const char *value = strchr(tok, '=');
        if (!value || add_audit_csv_arg(args, "audit_redact", value + 1) != TF_OK) {
            set_errorf(error, "%s: invalid audit_redact", op_name);
            return -1;
        }
        return 1;
    }
    if (strncmp(tok, "audit_hash_columns=", 19) == 0 || strncmp(tok, "audit-hash-columns=", 19) == 0) {
        const char *value = strchr(tok, '=');
        if (!value || add_audit_csv_arg(args, "audit_hash_columns", value + 1) != TF_OK) {
            set_errorf(error, "%s: invalid audit_hash_columns", op_name);
            return -1;
        }
        return 1;
    }
    if (strncmp(tok, "audit_max_bytes=", 16) == 0 || strncmp(tok, "audit-max-bytes=", 16) == 0) {
        const char *value = strchr(tok, '=');
        char *end = NULL;
        long n = strtol(value ? value + 1 : "", &end, 10);
        if (!value || value[1] == '\0' || !end || *end != '\0' || n < 0) {
            set_errorf(error, "%s: audit_max_bytes must be a non-negative integer", op_name);
            return -1;
        }
        return audit_add_result(tf_json_add_number(args, "audit_max_bytes", (double)n), error);
    }
    if (strncmp(tok, "audit_max_cell_bytes=", 21) == 0 || strncmp(tok, "audit-max-cell-bytes=", 21) == 0) {
        const char *value = strchr(tok, '=');
        char *end = NULL;
        long n = strtol(value ? value + 1 : "", &end, 10);
        if (!value || value[1] == '\0' || !end || *end != '\0' || n < 0) {
            set_errorf(error, "%s: audit_max_cell_bytes must be a non-negative integer", op_name);
            return -1;
        }
        return audit_add_result(tf_json_add_number(args, "audit_max_cell_bytes", (double)n), error);
    }
    return 0;
}

static cJSON *build_mapping_args_with_audit(const token_list *tokens, char **error, const char *op_name) {
    if (tokens->count < 2) {
        set_errorf(error, "%s requires at least one name=value mapping", op_name);
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *mapping = cJSON_CreateObject();
    if (!args || !mapping) {
        cJSON_Delete(args);
        cJSON_Delete(mapping);
        set_oom_error_if_unset(error);
        return NULL;
    }

    for (size_t i = 1; i < tokens->count; i++) {
        int audit_rc = add_audit_option_arg(args, tokens->items[i], error, op_name);
        if (audit_rc < 0) { cJSON_Delete(args); cJSON_Delete(mapping); return NULL; }
        if (audit_rc > 0) continue;

        if (strcmp(op_name, "cast") == 0 &&
            (strncmp(tokens->items[i], "on_error=", 9) == 0 ||
             strncmp(tokens->items[i], "on-error=", 9) == 0 ||
             strncmp(tokens->items[i], "on_coercion_error=", 18) == 0 ||
             strncmp(tokens->items[i], "on-coercion-error=", 18) == 0)) {
            const char *value = strchr(tokens->items[i], '=');
            value = value ? value + 1 : "";
            if (strcmp(value, "coerce") != 0 && strcmp(value, "legacy") != 0 &&
                strcmp(value, "default") != 0 && strcmp(value, "fail") != 0 &&
                strcmp(value, "error") != 0 && strcmp(value, "strict") != 0 &&
                strcmp(value, "null") != 0 && strcmp(value, "nulling") != 0) {
                cJSON_Delete(args);
                cJSON_Delete(mapping);
                set_error(error, "cast: on_error must be coerce, fail, or null");
                return NULL;
            }
            if (tf_json_add_string(args, "on_error", value) != TF_OK) {
                cJSON_Delete(args);
                cJSON_Delete(mapping);
                set_oom_error_if_unset(error);
                return NULL;
            }
            continue;
        }

        char *saveptr = NULL;
        char *copy = strdup(tokens->items[i]);
        if (!copy) {
            cJSON_Delete(args);
            cJSON_Delete(mapping);
            set_oom_error_if_unset(error);
            return NULL;
        }
        char *part = strtok_r(copy, ",", &saveptr);
        while (part) {
            char *eq = strchr(part, '=');
            if (!eq) {
                free(copy);
                cJSON_Delete(args);
                cJSON_Delete(mapping);
                set_errorf(error, "invalid %s mapping: '%s' (expected name=value)", op_name, part);
                return NULL;
            }
            *eq = '\0';
            if (tf_json_add_string(mapping, part, eq + 1) != TF_OK) {
                free(copy);
                cJSON_Delete(args);
                cJSON_Delete(mapping);
                set_oom_error_if_unset(error);
                return NULL;
            }
            part = strtok_r(NULL, ",", &saveptr);
        }
        free(copy);
    }

    if (tf_json_add_item(args, "mapping", mapping) != TF_OK) {
        mapping = NULL;
        cJSON_Delete(args);
        set_oom_error_if_unset(error);
        return NULL;
    }
    return args;
}

static cJSON *build_head_args(const token_list *tokens, char **error) {
    /* head N, head n=N, slice-head n=N */
    if (tokens->count < 2) {
        set_error(error, "head requires a count argument");
        return NULL;
    }
    const char *raw = tokens->items[1];
    const char *value = raw;
    if (strncmp(raw, "n=", 2) == 0) value = raw + 2;
    char *end;
    long n = strtol(value, &end, 10);
    if (*end != '\0' || n < 0) {
        set_errorf(error, "head: invalid count '%s'", raw);
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    if (!args || tf_json_add_number(args, "n", n) != TF_OK) {
        cJSON_Delete(args);
        set_error(error, "out of memory building head args");
        return NULL;
    }
    return args;
}


static cJSON *build_skip_args(const token_list *tokens, char **error) {
    /* skip N */
    if (tokens->count < 2) {
        set_error(error, "skip requires a count argument");
        return NULL;
    }
    char *end;
    long n = strtol(tokens->items[1], &end, 10);
    if (*end != '\0' || n < 0) {
        set_errorf(error, "skip: invalid count '%s'", tokens->items[1]);
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    if (!args || tf_json_add_number(args, "n", n) != TF_OK) {
        cJSON_Delete(args);
        set_error(error, "out of memory building skip args");
        return NULL;
    }
    return args;
}

static cJSON *build_derive_args(const token_list *tokens, char **error) {
    /* derive name=expr name2=expr2 */
    if (tokens->count < 2) {
        set_error(error, "derive requires at least one name=expression mapping");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *columns = cJSON_CreateArray();
    if (!args || !columns) goto oom;

    for (size_t i = 1; i < tokens->count; i++) {
        char *eq = strchr(tokens->items[i], '=');
        if (!eq) {
            cJSON_Delete(args);
            cJSON_Delete(columns);
            set_errorf(error, "derive: invalid mapping '%s' (expected name=expr)", tokens->items[i]);
            return NULL;
        }
        *eq = '\0';
        cJSON *col = cJSON_CreateObject();
        if (!col ||
            tf_json_add_string(col, "name", tokens->items[i]) != TF_OK ||
            tf_json_add_string(col, "expr", eq + 1) != TF_OK) {
            *eq = '=';
            cJSON_Delete(col);
            goto oom;
        }
        if (tf_json_add_array_item(columns, col) != TF_OK) {
            *eq = '=';
            col = NULL;
            goto oom;
        }
        *eq = '='; /* restore */
    }

    if (tf_json_add_item(args, "columns", columns) != TF_OK) { columns = NULL; goto oom; }
    return args;
oom:
    cJSON_Delete(args);
    cJSON_Delete(columns);
    set_oom_error_if_unset(error);
    return NULL;
}


static int parse_bool_value(const char *s, int *out);

static int add_comma_values_to_array(cJSON *arr, const char *value, char **error, const char *op_label) {
    const char *p = value;
    const char *end = value + strlen(value);
    while (p <= end) {
        const char *comma = dsl_find_top_level_comma(p, end);
        size_t len = comma ? (size_t)(comma - p) : (size_t)(end - p);
        while (len > 0 && isspace((unsigned char)*p)) { p++; len--; }
        while (len > 0 && isspace((unsigned char)p[len - 1])) len--;
        if (len == 0) {
            set_errorf(error, "%s: empty list item", op_label);
            return -1;
        }
        char *item = malloc(len + 1);
        if (!item) {
            set_oom_error_if_unset(error);
            return -1;
        }
        memcpy(item, p, len);
        item[len] = '\0';
        if (dsl_add_string_array_item(arr, item) != TF_OK) {
            free(item);
            set_oom_error_if_unset(error);
            return -1;
        }
        free(item);
        if (!comma) break;
        p = comma + 1;
    }
    return 0;
}

static cJSON *build_source_name_args(const token_list *tokens, char **error) {
    cJSON *args = cJSON_CreateObject();
    if (!args) {
        set_oom_error_if_unset(error);
        return NULL;
    }
    int have_result = 0;
    int have_default = 0;
    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        const char *result = NULL;
        if (strncmp(tok, "result=", 7) == 0) result = tok + 7;
        else if (strncmp(tok, "as=", 3) == 0) result = tok + 3;
        else if (strncmp(tok, "column=", 7) == 0) result = tok + 7;
        if (result) {
            if (!result[0] || have_result) {
                set_error(error, "source-name result specified more than once or empty");
                cJSON_Delete(args);
                return NULL;
            }
            if (tf_json_add_string(args, "result", result) != TF_OK) goto oom;
            have_result = 1;
            continue;
        }
        if (strncmp(tok, "default=", 8) == 0) {
            if (have_default) {
                set_error(error, "source-name default specified more than once");
                cJSON_Delete(args);
                return NULL;
            }
            if (tf_json_add_string(args, "default", tok + 8) != TF_OK) goto oom;
            have_default = 1;
            continue;
        }
        if (!have_result && strchr(tok, '=') == NULL) {
            if (!tok[0]) {
                set_error(error, "source-name result cannot be empty");
                cJSON_Delete(args);
                return NULL;
            }
            if (tf_json_add_string(args, "result", tok) != TF_OK) goto oom;
            have_result = 1;
            continue;
        }
        set_errorf(error, "source-name: unexpected argument '%s'", tok);
        cJSON_Delete(args);
        return NULL;
    }
    return args;
oom:
    cJSON_Delete(args);
    set_oom_error_if_unset(error);
    return NULL;
}

static cJSON *build_across_args(const token_list *tokens, char **error) {
    /* across columns=selector[,selector] fn=round [names={col}_{fn}] [replace=false]
     * Compact form: across selector round */
    if (tokens->count < 3) {
        set_error(error, "across requires columns and a function");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *cols = cJSON_CreateArray();
    cJSON *functions = cJSON_CreateArray();
    cJSON *bare = cJSON_CreateArray();
    if (!args || !cols || !functions || !bare) {
        cJSON_Delete(args); cJSON_Delete(cols); cJSON_Delete(functions); cJSON_Delete(bare);
        set_oom_error_if_unset(error);
        return NULL;
    }

    int have_explicit_fn = 0;
    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (strncmp(tok, "columns=", 8) == 0 || strncmp(tok, "cols=", 5) == 0) {
            const char *value = strchr(tok, '=') + 1;
            if (!value[0] || add_comma_values_to_array(cols, value, error, "across") != 0) goto fail;
            continue;
        }
        if (strncmp(tok, "fn=", 3) == 0 || strncmp(tok, ".fns=", 5) == 0 ||
            strncmp(tok, "functions=", 10) == 0 || strncmp(tok, "fns=", 4) == 0) {
            const char *value = strchr(tok, '=') + 1;
            if (!value[0] || add_comma_values_to_array(functions, value, error, "across") != 0) goto fail;
            have_explicit_fn = 1;
            continue;
        }
        if (strncmp(tok, "names=", 6) == 0 || strncmp(tok, ".names=", 7) == 0) {
            const char *value = strchr(tok, '=') + 1;
            if (!value[0]) { set_error(error, "across names cannot be empty"); goto fail; }
            if (tf_json_add_string(args, "names", value) != TF_OK) goto oom;
            continue;
        }
        if (strncmp(tok, "replace=", 8) == 0) {
            int value = 0;
            if (parse_bool_value(tok + 8, &value) != 0) {
                set_error(error, "across replace must be true or false");
                goto fail;
            }
            if (tf_json_add_bool(args, "replace", value) != TF_OK) goto oom;
            continue;
        }
        if (dsl_add_string_array_item(bare, tok) != TF_OK) goto oom;
    }

    int n_bare = cJSON_GetArraySize(bare);
    if (have_explicit_fn) {
        for (int i = 0; i < n_bare; i++) {
            if (dsl_add_array_duplicate(cols, cJSON_GetArrayItem(bare, i)) != TF_OK) goto oom;
        }
    } else {
        if (n_bare < 2 && cJSON_GetArraySize(cols) == 0) {
            set_error(error, "across compact form requires selector and function");
            goto fail;
        }
        if (n_bare > 0) {
            for (int i = 0; i < n_bare - 1; i++) {
                if (dsl_add_array_duplicate(cols, cJSON_GetArrayItem(bare, i)) != TF_OK) goto oom;
            }
            if (dsl_add_array_duplicate(functions, cJSON_GetArrayItem(bare, n_bare - 1)) != TF_OK) goto oom;
        }
    }

    if (cJSON_GetArraySize(cols) <= 0) { set_error(error, "across requires at least one column selector"); goto fail; }
    if (cJSON_GetArraySize(functions) <= 0) { set_error(error, "across requires at least one function"); goto fail; }
    if (tf_json_add_item(args, "columns", cols) != TF_OK) { cols = NULL; goto oom; }
    cols = NULL;
    if (tf_json_add_item(args, "functions", functions) != TF_OK) { functions = NULL; goto oom; }
    functions = NULL;
    cJSON_Delete(bare);
    return args;

oom:
    set_oom_error_if_unset(error);
fail:
    cJSON_Delete(args);
    cJSON_Delete(cols);
    cJSON_Delete(functions);
    cJSON_Delete(bare);
    return NULL;
}

static cJSON *build_stats_args(const token_list *tokens, char **error) {
    /* stats [count,sum,avg,min,max] */
    cJSON *args = cJSON_CreateObject();
    if (!args) {
        set_oom_error_if_unset(error);
        return NULL;
    }
    if (tokens->count >= 2) {
        /* Parse comma-separated stat names */
        cJSON *stats = cJSON_CreateArray();
        if (!stats) goto oom;
        for (size_t i = 1; i < tokens->count; i++) {
            if (dsl_add_string_array_item(stats, tokens->items[i]) != TF_OK) {
                cJSON_Delete(stats);
                goto oom;
            }
        }
        if (tf_json_add_item(args, "stats", stats) != TF_OK) goto oom;
    }
    return args;
oom:
    cJSON_Delete(args);
    set_oom_error_if_unset(error);
    return NULL;
}

static int parse_positive_option(const char *tok, const char *name,
                                 size_t *out, char **error,
                                 const char *op_label) {
    size_t name_len = strlen(name);
    if (strncmp(tok, name, name_len) != 0 || tok[name_len] != '=') return 0;
    const char *s = tok + name_len + 1;
    char *end = NULL;
    long n = strtol(s, &end, 10);
    if (!s[0] || !end || *end != '\0' || n <= 0) {
        set_errorf(error, "%s: invalid %s '%s'", op_label, name, tok);
        return -1;
    }
    *out = (size_t)n;
    return 1;
}


static char *trimmed_token_copy(const char *start, size_t len) {
    while (len > 0 && isspace((unsigned char)*start)) { start++; len--; }
    while (len > 0 && isspace((unsigned char)start[len - 1])) len--;
    char *out = malloc(len + 1);
    if (!out) return NULL;
    memcpy(out, start, len);
    out[len] = '\0';
    return out;
}

static int append_category_value(cJSON **cats, const char *start, size_t len,
                                 char **error, const char *op_label) {
    char *value = trimmed_token_copy(start, len);
    if (!value) {
        set_oom_error_if_unset(error);
        return -1;
    }
    if (value[0] == '\0') {
        free(value);
        set_errorf(error, "%s: empty category", op_label);
        return -1;
    }
    if (!*cats) {
        *cats = cJSON_CreateArray();
        if (!*cats) {
            free(value);
            set_oom_error_if_unset(error);
            return -1;
        }
    }
    if (dsl_add_string_array_item(*cats, value) != TF_OK) {
        free(value);
        set_oom_error_if_unset(error);
        return -1;
    }
    free(value);
    return 0;
}

static int parse_categories_option(const char *tok, cJSON **cats,
                                   char **error, const char *op_label) {
    const char *value = NULL;
    if (strncmp(tok, "categories=", 11) == 0) {
        value = tok + 11;
        if (!value[0]) {
            set_errorf(error, "%s: categories cannot be empty", op_label);
            return -1;
        }
        const char *p = value;
        while (1) {
            const char *comma = strchr(p, ',');
            size_t len = comma ? (size_t)(comma - p) : strlen(p);
            if (append_category_value(cats, p, len, error, op_label) != 0) return -1;
            if (!comma) break;
            p = comma + 1;
        }
        return 1;
    }
    if (strncmp(tok, "category=", 9) == 0) {
        value = tok + 9;
        if (!value[0]) {
            set_errorf(error, "%s: category cannot be empty", op_label);
            return -1;
        }
        if (append_category_value(cats, value, strlen(value), error, op_label) != 0) return -1;
        return 1;
    }
    return 0;
}

static int parse_unknown_option(const char *tok, cJSON *args,
                                char **error, const char *op_label) {
    if (strncmp(tok, "unknown=", 8) != 0) return 0;
    const char *value = tok + 8;
    if (strcmp(value, "error") != 0 &&
        strcmp(value, "other") != 0 &&
        strcmp(value, "null") != 0) {
        set_errorf(error, "%s: unknown must be error, other, or null", op_label);
        return -1;
    }
    if (tf_json_add_string(args, "unknown", value) != TF_OK) {
        set_oom_error_if_unset(error);
        return -1;
    }
    return 1;
}

static cJSON *build_unique_args(const token_list *tokens, char **error) {
    /* unique [col1,col2] [max_keys=N] [max_state_bytes=N] [spill_dir=DIR]
     * [spill_memory_bytes=N] [spill_run_rows=N] [spill_output_rows=N]
     * [sorted=true|--sorted] [mode=exact|approx] [approx=true|--approx]
     * [bloom_bytes=N] [bloom_hashes=N] */
    cJSON *args = cJSON_CreateObject();
    cJSON *cols = cJSON_CreateArray();
    if (!args || !cols) goto oom;
    int have_sorted = 0;
    int have_approx = 0;
    int have_mode = 0;
    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (strncmp(tok, "mode=", 5) == 0) {
            const char *mode = tok + 5;
            if (strcmp(mode, "exact") != 0 && strcmp(mode, "approx") != 0) {
                set_error(error, "unique mode must be exact or approx");
                goto fail;
            }
            if (have_mode) {
                set_error(error, "unique mode specified more than once");
                goto fail;
            }
            if (tf_json_add_string(args, "mode", mode) != TF_OK) goto oom;
            have_mode = 1;
            continue;
        }
        if (strcmp(tok, "approx") == 0 || strcmp(tok, "--approx") == 0) {
            if (have_approx) {
                set_error(error, "unique approx specified more than once");
                goto fail;
            }
            if (tf_json_add_bool(args, "approx", 1) != TF_OK) goto oom;
            have_approx = 1;
            continue;
        }
        if (strncmp(tok, "approx=", 7) == 0) {
            int approx = 0;
            if (parse_bool_value(tok + 7, &approx) != 0) {
                set_error(error, "unique approx must be true or false");
                goto fail;
            }
            if (have_approx) {
                set_error(error, "unique approx specified more than once");
                goto fail;
            }
            if (tf_json_add_bool(args, "approx", approx) != TF_OK) goto oom;
            have_approx = 1;
            continue;
        }
        size_t max_keys = 0;
        int opt = parse_positive_option(tok, "max_keys", &max_keys, error, "unique");
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (tf_json_add_number(args, "max_keys", (double)max_keys) != TF_OK) goto oom;
            continue;
        }
        size_t max_state_bytes = 0;
        opt = parse_positive_option(tok, "max_state_bytes", &max_state_bytes, error, "unique");
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (tf_json_add_number(args, "max_state_bytes", (double)max_state_bytes) != TF_OK) goto oom;
            continue;
        }
        size_t bloom_value = 0;
        opt = parse_positive_option(tok, "bloom_bytes", &bloom_value, error, "unique");
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (tf_json_add_number(args, "bloom_bytes", (double)bloom_value) != TF_OK) goto oom;
            continue;
        }
        opt = parse_positive_option(tok, "bloom_hashes", &bloom_value, error, "unique");
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (tf_json_add_number(args, "bloom_hashes", (double)bloom_value) != TF_OK) goto oom;
            continue;
        }
        if (strncmp(tok, "spill_dir=", 10) == 0) {
            const char *dir = tok + 10;
            if (!dir[0]) {
                set_error(error, "unique: spill_dir cannot be empty");
                goto fail;
            }
            if (tf_json_add_string(args, "spill_dir", dir) != TF_OK) goto oom;
            continue;
        }
        size_t spill_value = 0;
        opt = parse_positive_option(tok, "spill_memory_bytes", &spill_value, error, "unique");
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (tf_json_add_number(args, "spill_memory_bytes", (double)spill_value) != TF_OK) goto oom;
            continue;
        }
        opt = parse_positive_option(tok, "spill_run_rows", &spill_value, error, "unique");
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (tf_json_add_number(args, "spill_run_rows", (double)spill_value) != TF_OK) goto oom;
            continue;
        }
        opt = parse_positive_option(tok, "spill_output_rows", &spill_value, error, "unique");
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (tf_json_add_number(args, "spill_output_rows", (double)spill_value) != TF_OK) goto oom;
            continue;
        }
        if (strcmp(tok, "sorted") == 0 || strcmp(tok, "--sorted") == 0) {
            if (have_sorted) {
                set_error(error, "unique sorted specified more than once");
                goto fail;
            }
            if (tf_json_add_bool(args, "sorted", 1) != TF_OK) goto oom;
            have_sorted = 1;
            continue;
        }
        if (strncmp(tok, "sorted=", 7) == 0) {
            int sorted = 0;
            if (parse_bool_value(tok + 7, &sorted) != 0) {
                set_error(error, "unique sorted must be true or false");
                goto fail;
            }
            if (have_sorted) {
                set_error(error, "unique sorted specified more than once");
                goto fail;
            }
            if (tf_json_add_bool(args, "sorted", sorted) != TF_OK) goto oom;
            have_sorted = 1;
            continue;
        }
        if (dsl_add_string_array_item(cols, tok) != TF_OK) goto oom;
    }
    if (cJSON_GetArraySize(cols) > 0) {
        if (tf_json_add_item(args, "columns", cols) != TF_OK) { cols = NULL; goto oom; }
        cols = NULL;
    } else {
        cJSON_Delete(cols);
        cols = NULL;
    }
    return args;
oom:
    set_oom_error_if_unset(error);
fail:
    cJSON_Delete(cols);
    cJSON_Delete(args);
    return NULL;
}

static cJSON *build_sort_args(const token_list *tokens, char **error) {
    /* sort col1,-col2,col3 */
    if (tokens->count < 2) {
        set_error(error, "sort requires at least one column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *columns = cJSON_CreateArray();
    if (!args || !columns) goto oom;

    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        int desc = 0;
        if (tok[0] == '-') {
            desc = 1;
            tok++;
        }
        cJSON *col = cJSON_CreateObject();
        if (!col ||
            tf_json_add_string(col, "name", tok) != TF_OK ||
            tf_json_add_bool(col, "desc", desc) != TF_OK) {
            cJSON_Delete(col);
            goto oom;
        }
        if (tf_json_add_array_item(columns, col) != TF_OK) {
            col = NULL;
            goto oom;
        }
    }

    if (tf_json_add_item(args, "columns", columns) != TF_OK) { columns = NULL; goto oom; }
    return args;
oom:
    cJSON_Delete(columns);
    cJSON_Delete(args);
    set_oom_error_if_unset(error);
    return NULL;
}

static int parse_count_token(const char *tok, long *out) {
    const char *s = tok;
    if (strncmp(s, "n=", 2) == 0) s += 2;
    else if (strncmp(s, "--n=", 4) == 0) s += 4;
    if (*s == '\0') return 0;

    char *end;
    long n = strtol(s, &end, 10);
    if (*end != '\0' || n <= 0) return 0;
    *out = n;
    return 1;
}

static cJSON *build_top_args_with_default(const token_list *tokens, char **error,
                                          int default_desc,
                                          const char *op_label) {
    /* top 10 score, top n=10 score, bottom-k 10 score, top 10 -score */
    if (tokens->count < 3) {
        set_errorf(error, "%s requires N and column arguments", op_label);
        return NULL;
    }

    long n = 0;
    if (!parse_count_token(tokens->items[1], &n)) {
        set_errorf(error, "%s: invalid count '%s'", op_label, tokens->items[1]);
        return NULL;
    }

    const char *col = tokens->items[2];
    int desc = default_desc;
    if (col[0] == '-') { col++; desc = 1; }
    else if (col[0] == '+') { col++; desc = 0; }
    if (*col == '\0') {
        set_errorf(error, "%s: empty column name", op_label);
        return NULL;
    }

    cJSON *args = cJSON_CreateObject();
    if (!args ||
        tf_json_add_number(args, "n", n) != TF_OK ||
        tf_json_add_string(args, "column", col) != TF_OK ||
        tf_json_add_bool(args, "desc", desc) != TF_OK) {
        cJSON_Delete(args);
        set_oom_error_if_unset(error);
        return NULL;
    }
    return args;
}

static cJSON *build_slice_rank_args(const token_list *tokens, char **error,
                                    int desc, const char *op_label) {
    /* slice-min score n=10, slice-max score 10, slice-min n=10 score with_ties=false */
    const char *column = NULL;
    long n = 0;
    int have_with_ties = 0;
    int with_ties = 0;

    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        long parsed = 0;
        if (parse_count_token(tok, &parsed)) {
            if (n != 0) {
                set_errorf(error, "%s: duplicate count argument", op_label);
                return NULL;
            }
            n = parsed;
        } else if (strncmp(tok, "with_ties=", 10) == 0) {
            if (have_with_ties) {
                set_errorf(error, "%s: duplicate with_ties argument", op_label);
                return NULL;
            }
            if (parse_bool_value(tok + 10, &with_ties) != 0) {
                set_errorf(error, "%s: with_ties must be true or false", op_label);
                return NULL;
            }
            have_with_ties = 1;
            if (with_ties) {
                set_errorf(error, "%s: with_ties=true is not supported by bounded exact-N execution; use with_ties=false", op_label);
                return NULL;
            }
        } else if (!column) {
            column = tok;
        } else {
            set_errorf(error, "%s: unexpected argument '%s'", op_label, tok);
            return NULL;
        }
    }

    if (!column) {
        set_errorf(error, "%s requires a column argument", op_label);
        return NULL;
    }
    if (n <= 0) {
        set_errorf(error, "%s requires n=N", op_label);
        return NULL;
    }

    if (column[0] == '-' || column[0] == '+') column++;
    if (*column == '\0') {
        set_errorf(error, "%s: empty column name", op_label);
        return NULL;
    }

    cJSON *args = cJSON_CreateObject();
    if (!args ||
        tf_json_add_number(args, "n", n) != TF_OK ||
        tf_json_add_string(args, "column", column) != TF_OK ||
        tf_json_add_bool(args, "desc", desc) != TF_OK ||
        (have_with_ties && tf_json_add_bool(args, "with_ties", with_ties) != TF_OK)) {
        cJSON_Delete(args);
        set_oom_error_if_unset(error);
        return NULL;
    }
    return args;
}

static cJSON *build_replace_args(const token_list *tokens, char **error) {
    /* replace [--regex] column pattern replacement [audit] [audit_limit=N] */
    if (tokens->count < 4) {
        set_error(error, "replace requires column, pattern, and replacement");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    size_t idx = 1;
    if (strcmp(tokens->items[idx], "--regex") == 0 || strcmp(tokens->items[idx], "-r") == 0) {
        cJSON_AddBoolToObject(args, "regex", 1);
        idx++;
        if (idx + 2 >= tokens->count) {
            cJSON_Delete(args);
            set_error(error, "replace requires column, pattern, and replacement");
            return NULL;
        }
    }
    cJSON_AddStringToObject(args, "column", tokens->items[idx]);
    cJSON_AddStringToObject(args, "pattern", tokens->items[idx + 1]);
    cJSON_AddStringToObject(args, "replacement", tokens->items[idx + 2]);
    for (size_t i = idx + 3; i < tokens->count; i++) {
        int audit_rc = add_audit_option_arg(args, tokens->items[i], error, "replace");
        if (audit_rc < 0) { cJSON_Delete(args); return NULL; }
        if (audit_rc > 0) continue;
        cJSON_Delete(args);
        set_errorf(error, "replace: unexpected option '%s'", tokens->items[i]);
        return NULL;
    }
    return args;
}

static cJSON *build_clip_args(const token_list *tokens, char **error) {
    /* clip column min=0 max=100
     * clip column 0 100 */
    if (tokens->count < 2) {
        set_error(error, "clip requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    int positional = 0;
    for (size_t i = 2; i < tokens->count; i++) {
        char *eq = strchr(tokens->items[i], '=');
        if (eq) {
            *eq = '\0';
            double v = strtod(eq + 1, NULL);
            cJSON_AddNumberToObject(args, tokens->items[i], v);
            *eq = '=';
        } else {
            char *endptr = NULL;
            double v = strtod(tokens->items[i], &endptr);
            if (endptr && *endptr == '\0') {
                cJSON_AddNumberToObject(args, positional == 0 ? "min" : "max", v);
                positional++;
            }
        }
    }
    return args;
}

static cJSON *build_bin_args(const token_list *tokens, char **error) {
    /* bin column 10,20,30 [missing=error|null|ignore] [on_type_error=fail|null] */
    if (tokens->count < 3) {
        set_error(error, "bin requires column and boundaries");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    cJSON *boundaries = cJSON_CreateArray();
    size_t n_boundaries = 0;
    double prev = 0.0;
    for (size_t i = 2; i < tokens->count; i++) {
        int opt = add_h14_policy_option(args, tokens->items[i], "bin", error);
        if (opt == TF_ERROR) goto fail;
        if (opt == TF_OK) continue;
        double v = 0.0;
        if (parse_finite_number_token(tokens->items[i], "bin boundary", &v, error) != TF_OK) goto fail;
        if (n_boundaries > 0 && v <= prev) {
            set_error(error, "bin boundaries must be strictly increasing");
            goto fail;
        }
        cJSON_AddItemToArray(boundaries, cJSON_CreateNumber(v));
        prev = v;
        n_boundaries++;
    }
    if (n_boundaries == 0) {
        set_error(error, "bin requires at least one boundary");
        goto fail;
    }
    cJSON_AddItemToObject(args, "boundaries", boundaries);
    return args;
fail:
    cJSON_Delete(boundaries);
    cJSON_Delete(args);
    return NULL;
}

static int valid_datetime_extract(const char *s) {
    return strcmp(s, "year") == 0 || strcmp(s, "month") == 0 || strcmp(s, "day") == 0 ||
           strcmp(s, "hour") == 0 || strcmp(s, "minute") == 0 || strcmp(s, "second") == 0 ||
           strcmp(s, "weekday") == 0 || strcmp(s, "epoch") == 0;
}

static int valid_date_trunc_unit(const char *s) {
    return strcmp(s, "year") == 0 || strcmp(s, "month") == 0 || strcmp(s, "day") == 0 ||
           strcmp(s, "hour") == 0 || strcmp(s, "minute") == 0 || strcmp(s, "second") == 0;
}

static int add_datetime_extracts(cJSON *extract, const char *spec, char **error) {
    const char *text = spec ? spec : "";
    const char *p = text;
    const char *end = text + strlen(text);
    while (1) {
        const char *comma = dsl_find_top_level_comma(p, end);
        size_t len = comma ? (size_t)(comma - p) : (size_t)(end - p);
        char *value = trimmed_token_copy(p, len);
        if (!value) return TF_ERROR;
        if (value[0] == '\0' || !valid_datetime_extract(value)) {
            free(value);
            set_error(error, "datetime extract must be year, month, day, hour, minute, second, weekday, or epoch");
            return TF_ERROR;
        }
        cJSON_AddItemToArray(extract, cJSON_CreateString(value));
        free(value);
        if (!comma) break;
        p = comma + 1;
    }
    return TF_OK;
}

static cJSON *build_datetime_args(const token_list *tokens, char **error) {
    /* datetime date_col [year,month,...|extract=...] [missing=error|null|ignore] [on_type_error=fail|null] */
    if (tokens->count < 2) {
        set_error(error, "datetime requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *extract = NULL;
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    for (size_t i = 2; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        int opt = add_h14_policy_option(args, tok, "datetime", error);
        if (opt == TF_ERROR) goto fail;
        if (opt == TF_OK) continue;
        const char *spec = tok;
        if (strncmp(tok, "extract=", 8) == 0) spec = tok + 8;
        if (!extract) extract = cJSON_CreateArray();
        if (add_datetime_extracts(extract, spec, error) != TF_OK) goto fail;
    }
    if (extract) {
        if (cJSON_GetArraySize(extract) <= 0) { set_error(error, "datetime extract must not be empty"); goto fail; }
        cJSON_AddItemToObject(args, "extract", extract);
        extract = NULL;
    }
    return args;
fail:
    if (extract) cJSON_Delete(extract);
    cJSON_Delete(args);
    return NULL;
}

static cJSON *build_explode_args(const token_list *tokens, char **error) {
    /* explode column [delimiter] [max_tokens_per_row=N] [max_output_rows_per_input_row=N]
     *         [max_output_rows_per_batch=N] [max_token_bytes=N] */
    if (tokens->count < 2) {
        set_error(error, "explode requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    int saw_delim = 0;
    for (size_t i = 2; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        size_t value = 0;
        int opt = parse_positive_option(tok, "max_tokens_per_row", &value, error, "explode");
        if (opt < 0) { cJSON_Delete(args); return NULL; }
        if (opt > 0) { cJSON_AddNumberToObject(args, "max_tokens_per_row", (double)value); continue; }
        opt = parse_positive_option(tok, "max_output_rows_per_input_row", &value, error, "explode");
        if (opt < 0) { cJSON_Delete(args); return NULL; }
        if (opt > 0) { cJSON_AddNumberToObject(args, "max_output_rows_per_input_row", (double)value); continue; }
        opt = parse_positive_option(tok, "max_output_rows_per_batch", &value, error, "explode");
        if (opt < 0) { cJSON_Delete(args); return NULL; }
        if (opt > 0) { cJSON_AddNumberToObject(args, "max_output_rows_per_batch", (double)value); continue; }
        opt = parse_positive_option(tok, "max_token_bytes", &value, error, "explode");
        if (opt < 0) { cJSON_Delete(args); return NULL; }
        if (opt > 0) { cJSON_AddNumberToObject(args, "max_token_bytes", (double)value); continue; }
        if (saw_delim) {
            set_error(error, "explode accepts at most one delimiter");
            cJSON_Delete(args);
            return NULL;
        }
        cJSON_AddStringToObject(args, "delimiter", tok);
        saw_delim = 1;
    }
    return args;
}

static cJSON *build_split_args(const token_list *tokens, char **error) {
    /* split column delimiter name1,name2 */
    if (tokens->count < 4) {
        set_error(error, "split requires column, delimiter, and names");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *names = cJSON_CreateArray();
    if (!args || !names ||
        !cJSON_AddStringToObject(args, "column", tokens->items[1]) ||
        !cJSON_AddStringToObject(args, "delimiter", tokens->items[2])) {
        cJSON_Delete(names);
        cJSON_Delete(args);
        set_error(error, "out of memory building split args");
        return NULL;
    }
    for (size_t i = 3; i < tokens->count; i++) {
        cJSON *name = cJSON_CreateString(tokens->items[i]);
        if (!name || !cJSON_AddItemToArray(names, name)) {
            cJSON_Delete(name);
            cJSON_Delete(names);
            cJSON_Delete(args);
            set_error(error, "out of memory building split args");
            return NULL;
        }
    }
    if (!cJSON_AddItemToObject(args, "names", names)) {
        cJSON_Delete(names);
        cJSON_Delete(args);
        set_error(error, "out of memory building split args");
        return NULL;
    }
    return args;
}

static cJSON *build_unpivot_args(const token_list *tokens, char **error) {
    /* unpivot col1,col2,col3 [max_output_rows_per_input_row=N] [max_output_rows_per_batch=N] */
    if (tokens->count < 2) {
        set_error(error, "unpivot requires at least one column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *cols = cJSON_CreateArray();
    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        size_t value = 0;
        int opt = parse_positive_option(tok, "max_output_rows_per_input_row", &value, error, "unpivot");
        if (opt < 0) { cJSON_Delete(cols); cJSON_Delete(args); return NULL; }
        if (opt > 0) { cJSON_AddNumberToObject(args, "max_output_rows_per_input_row", (double)value); continue; }
        opt = parse_positive_option(tok, "max_output_rows_per_batch", &value, error, "unpivot");
        if (opt < 0) { cJSON_Delete(cols); cJSON_Delete(args); return NULL; }
        if (opt > 0) { cJSON_AddNumberToObject(args, "max_output_rows_per_batch", (double)value); continue; }
        cJSON_AddItemToArray(cols, cJSON_CreateString(tok));
    }
    if (cJSON_GetArraySize(cols) <= 0) {
        set_error(error, "unpivot requires at least one column name");
        cJSON_Delete(cols);
        cJSON_Delete(args);
        return NULL;
    }
    cJSON_AddItemToObject(args, "columns", cols);
    return args;
}

static int is_agg_func(const char *s) {
    return strcmp(s, "count") == 0 || strcmp(s, "sum") == 0 ||
           strcmp(s, "avg") == 0 || strcmp(s, "mean") == 0 ||
           strcmp(s, "min") == 0 || strcmp(s, "max") == 0;
}

static cJSON *build_group_agg_args(const token_list *tokens, char **error) {
    /* group-agg group_col1,group_col2 col:func[:name] [max_groups=N] [max_state_bytes=N] [spill_dir=DIR]
     * [spill_memory_bytes=N] [spill_run_rows=N] [spill_output_rows=N] [sorted=true|--sorted]
     * group-agg group_col1,group_col2 func:col[:name] [max_groups=N] [max_state_bytes=N] [spill_dir=DIR]
     * [spill_memory_bytes=N] [spill_run_rows=N] [spill_output_rows=N] [sorted=true|--sorted] */
    if (tokens->count < 3) {
        set_error(error, "group-agg requires group columns and at least one aggregation");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *group_by = cJSON_CreateArray();
    cJSON *aggs = cJSON_CreateArray();
    if (!args || !group_by || !aggs) goto oom;

    if (dsl_add_string_array_item(group_by, tokens->items[1]) != TF_OK) goto oom;
    if (tf_json_add_item(args, "group_by", group_by) != TF_OK) { group_by = NULL; goto oom; }
    group_by = NULL;

    int have_sorted = 0;
    for (size_t i = 2; i < tokens->count; i++) {
        const char *tok_s = tokens->items[i];
        if (strcmp(tok_s, "sorted") == 0 || strcmp(tok_s, "--sorted") == 0) {
            if (have_sorted) {
                set_error(error, "group-agg sorted specified more than once");
                goto fail;
            }
            if (tf_json_add_bool(args, "sorted", 1) != TF_OK) goto oom;
            have_sorted = 1;
            continue;
        }
        if (strncmp(tok_s, "sorted=", 7) == 0) {
            int sorted = 0;
            if (parse_bool_value(tok_s + 7, &sorted) != 0) {
                set_error(error, "group-agg sorted must be true or false");
                goto fail;
            }
            if (have_sorted) {
                set_error(error, "group-agg sorted specified more than once");
                goto fail;
            }
            if (tf_json_add_bool(args, "sorted", sorted) != TF_OK) goto oom;
            have_sorted = 1;
            continue;
        }

        size_t max_groups = 0;
        int opt = parse_positive_option(tok_s, "max_groups", &max_groups, error, "group-agg");
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (tf_json_add_number(args, "max_groups", (double)max_groups) != TF_OK) goto oom;
            continue;
        }

        size_t max_state_bytes = 0;
        opt = parse_positive_option(tok_s, "max_state_bytes", &max_state_bytes, error, "group-agg");
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (tf_json_add_number(args, "max_state_bytes", (double)max_state_bytes) != TF_OK) goto oom;
            continue;
        }

        if (strncmp(tok_s, "spill_dir=", 10) == 0) {
            const char *dir = tok_s + 10;
            if (!dir[0]) { set_error(error, "group-agg: spill_dir cannot be empty"); goto fail; }
            if (tf_json_add_string(args, "spill_dir", dir) != TF_OK) goto oom;
            continue;
        }
        size_t spill_value = 0;
        opt = parse_positive_option(tok_s, "spill_memory_bytes", &spill_value, error, "group-agg");
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (tf_json_add_number(args, "spill_memory_bytes", (double)spill_value) != TF_OK) goto oom;
            continue;
        }
        opt = parse_positive_option(tok_s, "spill_run_rows", &spill_value, error, "group-agg");
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (tf_json_add_number(args, "spill_run_rows", (double)spill_value) != TF_OK) goto oom;
            continue;
        }
        opt = parse_positive_option(tok_s, "spill_output_rows", &spill_value, error, "group-agg");
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (tf_json_add_number(args, "spill_output_rows", (double)spill_value) != TF_OK) goto oom;
            continue;
        }

        char *tok = strdup(tokens->items[i]);
        if (!tok) goto oom;
        char *colon1 = strchr(tok, ':');
        if (!colon1) { free(tok); continue; }
        *colon1 = '\0';
        char *second = colon1 + 1;
        char *colon2 = strchr(second, ':');
        char *name = NULL;
        if (colon2) { *colon2 = '\0'; name = colon2 + 1; }

        const char *column = tok;
        const char *func = second;
        if (is_agg_func(tok) && *second) {
            func = tok;
            column = second;
        }

        cJSON *agg = cJSON_CreateObject();
        if (!agg ||
            tf_json_add_string(agg, "column", column) != TF_OK ||
            tf_json_add_string(agg, "func", func) != TF_OK ||
            (name && tf_json_add_string(agg, "name", name) != TF_OK)) {
            cJSON_Delete(agg);
            free(tok);
            goto oom;
        }
        if (tf_json_add_array_item(aggs, agg) != TF_OK) {
            agg = NULL;
            free(tok);
            goto oom;
        }
        free(tok);
    }
    if (tf_json_add_item(args, "aggs", aggs) != TF_OK) { aggs = NULL; goto oom; }
    aggs = NULL;
    return args;

oom:
    set_oom_error_if_unset(error);
fail:
    cJSON_Delete(group_by);
    cJSON_Delete(aggs);
    cJSON_Delete(args);
    return NULL;
}

static cJSON *build_frequency_args(const token_list *tokens, char **error) {
    /* frequency [col1,col2] [max_values=N] [max_state_bytes=N] [overflow=error|other] [other=name] [audit] [audit_limit=N] */
    cJSON *args = cJSON_CreateObject();
    cJSON *cols = cJSON_CreateArray();
    if (!args || !cols) goto oom;
    int has_max_values = 0;
    int overflow_other = 0;
    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        int audit_rc = add_audit_option_arg(args, tok, error, "frequency");
        if (audit_rc < 0) goto fail;
        if (audit_rc > 0) continue;
        size_t max_values = 0;
        int opt = parse_positive_option(tok, "max_values", &max_values, error, "frequency");
        if (opt < 0) goto fail;
        if (opt > 0) {
            has_max_values = 1;
            if (tf_json_add_number(args, "max_values", (double)max_values) != TF_OK) goto oom;
            continue;
        }
        size_t max_state_bytes = 0;
        opt = parse_positive_option(tok, "max_state_bytes", &max_state_bytes, error, "frequency");
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (tf_json_add_number(args, "max_state_bytes", (double)max_state_bytes) != TF_OK) goto oom;
            continue;
        }
        if (strncmp(tok, "overflow=", 9) == 0) {
            const char *value = tok + 9;
            if (strcmp(value, "error") != 0 && strcmp(value, "other") != 0) {
                set_error(error, "frequency: overflow must be error or other");
                goto fail;
            }
            if (strcmp(value, "other") == 0) overflow_other = 1;
            if (tf_json_add_string(args, "overflow", value) != TF_OK) goto oom;
            continue;
        }
        if (strncmp(tok, "other=", 6) == 0) {
            const char *value = tok + 6;
            if (!value[0]) {
                set_error(error, "frequency: other label cannot be empty");
                goto fail;
            }
            if (tf_json_add_string(args, "other", value) != TF_OK) goto oom;
            continue;
        }
        if (dsl_add_string_array_item(cols, tok) != TF_OK) goto oom;
    }
    if (overflow_other && !has_max_values) {
        set_error(error, "frequency: overflow=other requires max_values");
        goto fail;
    }
    if (cJSON_GetArraySize(cols) > 0) {
        if (tf_json_add_item(args, "columns", cols) != TF_OK) { cols = NULL; goto oom; }
        cols = NULL;
    } else {
        cJSON_Delete(cols);
        cols = NULL;
    }
    return args;
oom:
    set_oom_error_if_unset(error);
fail:
    cJSON_Delete(cols);
    cJSON_Delete(args);
    return NULL;
}

static int valid_window_func(const char *s) {
    return strcmp(s, "avg") == 0 || strcmp(s, "sum") == 0 ||
           strcmp(s, "min") == 0 || strcmp(s, "max") == 0 ||
           strcmp(s, "count") == 0;
}

static cJSON *build_window_args(const token_list *tokens, char **error) {
    /* window column size func [result_name] [missing=error|null|ignore] [on_type_error=fail|null] */
    if (tokens->count < 4) {
        set_error(error, "window requires column, size, and func");
        return NULL;
    }
    char *end = NULL;
    long size = strtol(tokens->items[2], &end, 10);
    if (!end || *end != '\0' || size <= 0) {
        set_error(error, "window size must be a positive integer");
        return NULL;
    }
    if (!valid_window_func(tokens->items[3])) {
        set_error(error, "window func must be avg, sum, min, max, or count");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    cJSON_AddNumberToObject(args, "size", (double)size);
    cJSON_AddStringToObject(args, "func", tokens->items[3]);
    int result_set = 0;
    for (size_t i = 4; i < tokens->count; i++) {
        int opt = add_h14_policy_option(args, tokens->items[i], "window", error);
        if (opt == TF_ERROR) goto fail;
        if (opt == TF_OK) continue;
        if (strncmp(tokens->items[i], "result=", 7) == 0 || strncmp(tokens->items[i], "as=", 3) == 0) {
            const char *value = tokens->items[i] + (tokens->items[i][0] == 'a' ? 3 : 7);
            if (!*value) { set_error(error, "window result cannot be empty"); goto fail; }
            if (result_set) { set_error(error, "window duplicate result argument"); goto fail; }
            cJSON_AddStringToObject(args, "result", value);
            result_set = 1;
            continue;
        }
        if (!result_set) {
            cJSON_AddStringToObject(args, "result", tokens->items[i]);
            result_set = 1;
            continue;
        }
        set_errorf(error, "window unexpected argument '%s'", tokens->items[i]);
        goto fail;
    }
    return args;
fail:
    cJSON_Delete(args);
    return NULL;
}

static int valid_bool_null_policy(const char *s) {
    return strcmp(s, "ignore") == 0 || strcmp(s, "false") == 0 ||
           strcmp(s, "true") == 0 || strcmp(s, "propagate") == 0;
}

static cJSON *build_rolling_args(const token_list *tokens, char **error,
                                 const char *op_label, int allow_nulls) {
    /* rolling-sum column size [result_name] [missing=error|null|ignore] [on_type_error=fail|null]
     * rolling-any column size [result_name] [nulls=ignore|false|true|propagate]
     */
    if (tokens->count < 3) {
        set_errorf(error, "%s requires column and size", op_label);
        return NULL;
    }
    char *endptr = NULL;
    long size = strtol(tokens->items[2], &endptr, 10);
    if (!endptr || *endptr != '\0' || size <= 0) {
        set_errorf(error, "%s size must be a positive integer", op_label);
        return NULL;
    }

    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    cJSON_AddNumberToObject(args, "size", (double)size);
    const char *result = NULL;
    const char *nulls = NULL;
    for (size_t i = 3; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (!allow_nulls) {
            int opt = add_h14_policy_option(args, tok, op_label, error);
            if (opt == TF_ERROR) goto fail;
            if (opt == TF_OK) continue;
        }
        if (strncmp(tok, "nulls=", 6) == 0) {
            if (!allow_nulls) {
                set_errorf(error, "%s: nulls option is only supported for boolean rolling ops", op_label);
                goto fail;
            }
            if (nulls) {
                set_errorf(error, "%s: duplicate nulls option", op_label);
                goto fail;
            }
            nulls = tok + 6;
            if (!valid_bool_null_policy(nulls)) {
                set_errorf(error, "%s: nulls must be ignore, false, true, or propagate", op_label);
                goto fail;
            }
        } else if (strncmp(tok, "result=", 7) == 0) {
            if (result) {
                set_errorf(error, "%s: duplicate result argument", op_label);
                goto fail;
            }
            result = tok + 7;
        } else if (!result) {
            result = tok;
        } else {
            set_errorf(error, "%s: unexpected argument", op_label);
            goto fail;
        }
    }

    if (result) cJSON_AddStringToObject(args, "result", result);
    if (nulls) cJSON_AddStringToObject(args, "nulls", nulls);
    return args;
fail:
    cJSON_Delete(args);
    return NULL;
}

static cJSON *build_step_args(const token_list *tokens, char **error) {
    /* step column func [result_name] */
    if (tokens->count < 3) {
        set_error(error, "step requires column and func");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    cJSON_AddStringToObject(args, "func", tokens->items[2]);
    if (tokens->count >= 4)
        cJSON_AddStringToObject(args, "result", tokens->items[3]);
    return args;
}

static cJSON *build_flatten_args(const token_list *tokens, char **error) {
    (void)tokens; (void)error;
    return cJSON_CreateObject();
}


static int json_flatten_type_valid(const char *type) {
    return strcmp(type, "string") == 0 || strcmp(type, "str") == 0 || strcmp(type, "json") == 0 ||
           strcmp(type, "int") == 0 || strcmp(type, "int64") == 0 ||
           strcmp(type, "float") == 0 || strcmp(type, "float64") == 0 || strcmp(type, "number") == 0 ||
           strcmp(type, "bool") == 0 || strcmp(type, "boolean") == 0;
}

static int json_flatten_add_field_spec(cJSON *fields, const char *spec, char **error) {
    char *copy = strdup(spec ? spec : "");
    if (!copy) return -1;
    char *first = strchr(copy, ':');
    if (!first) {
        free(copy);
        set_error(error, "json-flatten field specs must be path:name[:type]");
        return -1;
    }
    *first = '\0';
    char *second = strchr(first + 1, ':');
    if (second) *second = '\0';
    const char *path = copy;
    const char *name = first + 1;
    const char *type = second ? second + 1 : "string";
    if (path[0] == '\0' || name[0] == '\0') {
        free(copy);
        set_error(error, "json-flatten field specs require non-empty path and name");
        return -1;
    }
    if (!json_flatten_type_valid(type)) {
        free(copy);
        set_error(error, "json-flatten field type must be string, int, float, or bool");
        return -1;
    }
    cJSON *field = cJSON_CreateObject();
    if (!field) { free(copy); return -1; }
    cJSON_AddStringToObject(field, "path", path);
    cJSON_AddStringToObject(field, "name", name);
    cJSON_AddStringToObject(field, "type", type);
    cJSON_AddItemToArray(fields, field);
    free(copy);
    return 0;
}

static int json_flatten_add_field_list(cJSON *fields, const char *spec, char **error) {
    char *copy = strdup(spec ? spec : "");
    if (!copy) return -1;
    char *saveptr = NULL;
    for (char *part = strtok_r(copy, ",", &saveptr); part; part = strtok_r(NULL, ",", &saveptr)) {
        if (part[0] == '\0') continue;
        if (json_flatten_add_field_spec(fields, part, error) != 0) {
            free(copy);
            return -1;
        }
    }
    free(copy);
    return 0;
}

static cJSON *build_json_flatten_args(const token_list *tokens, char **error) {
    const char *column = "_line";
    int saw_column = 0;
    cJSON *args = cJSON_CreateObject();
    cJSON *fields = cJSON_CreateArray();
    if (!args || !fields) { cJSON_Delete(args); cJSON_Delete(fields); return NULL; }

    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (strncmp(tok, "column=", 7) == 0) {
            column = tok + 7;
            saw_column = 1;
        } else if (strncmp(tok, "fields=", 7) == 0) {
            if (json_flatten_add_field_list(fields, tok + 7, error) != 0) {
                cJSON_Delete(args); cJSON_Delete(fields); return NULL;
            }
        } else if (strchr(tok, ':')) {
            if (json_flatten_add_field_spec(fields, tok, error) != 0) {
                cJSON_Delete(args); cJSON_Delete(fields); return NULL;
            }
        } else if (!saw_column) {
            column = tok;
            saw_column = 1;
        } else {
            set_error(error, "json-flatten: unexpected argument");
            cJSON_Delete(args); cJSON_Delete(fields); return NULL;
        }
    }

    if (cJSON_GetArraySize(fields) <= 0) {
        set_error(error, "json-flatten requires fields=path:name[:type],...");
        cJSON_Delete(args); cJSON_Delete(fields); return NULL;
    }
    cJSON_AddStringToObject(args, "column", column && column[0] ? column : "_line");
    cJSON_AddItemToObject(args, "fields", fields);
    return args;
}

static cJSON *build_json_extract_args(const token_list *tokens, char **error) {
    const char *column = "_line";
    const char *path = NULL;
    const char *result = NULL;
    const char *type = "string";
    const char *pos[3] = {0};
    size_t n_pos = 0;

    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (strncmp(tok, "column=", 7) == 0) {
            column = tok + 7;
        } else if (strncmp(tok, "path=", 5) == 0) {
            path = tok + 5;
        } else if (strncmp(tok, "result=", 7) == 0) {
            result = tok + 7;
        } else if (strncmp(tok, "as=", 3) == 0) {
            result = tok + 3;
        } else if (strncmp(tok, "type=", 5) == 0) {
            type = tok + 5;
        } else if (n_pos < 3) {
            pos[n_pos++] = tok;
        } else {
            set_error(error, "json-extract: unexpected argument");
            return NULL;
        }
    }

    if (n_pos == 2) {
        if (!path) path = pos[0];
        if (!result) result = pos[1];
    } else if (n_pos == 3) {
        column = pos[0];
        if (!path) path = pos[1];
        if (!result) result = pos[2];
    } else if (n_pos != 0) {
        set_error(error, "json-extract requires path and result, or column path result");
        return NULL;
    }

    if (!path || !result || result[0] == '\0') {
        set_error(error, "json-extract requires path and result");
        return NULL;
    }

    if (!(strcmp(type, "string") == 0 || strcmp(type, "str") == 0 || strcmp(type, "json") == 0 ||
          strcmp(type, "int") == 0 || strcmp(type, "int64") == 0 ||
          strcmp(type, "float") == 0 || strcmp(type, "float64") == 0 || strcmp(type, "number") == 0 ||
          strcmp(type, "bool") == 0 || strcmp(type, "boolean") == 0)) {
        set_error(error, "json-extract type must be string, int, float, or bool");
        return NULL;
    }

    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", column && column[0] ? column : "_line");
    cJSON_AddStringToObject(args, "path", path);
    cJSON_AddStringToObject(args, "result", result);
    cJSON_AddStringToObject(args, "type", type);
    return args;
}

static int json_filter_is_op_token(const char *s) {
    return strcmp(s, "exists") == 0 || strcmp(s, "present") == 0 ||
           strcmp(s, "missing") == 0 || strcmp(s, "absent") == 0 ||
           strcmp(s, "truthy") == 0 || strcmp(s, "true") == 0 ||
           strcmp(s, "falsey") == 0 || strcmp(s, "falsy") == 0 || strcmp(s, "false") == 0 ||
           strcmp(s, "eq") == 0 || strcmp(s, "==") == 0 || strcmp(s, "=") == 0 ||
           strcmp(s, "ne") == 0 || strcmp(s, "!=") == 0 ||
           strcmp(s, "gt") == 0 || strcmp(s, ">") == 0 ||
           strcmp(s, "ge") == 0 || strcmp(s, ">=") == 0 ||
           strcmp(s, "lt") == 0 || strcmp(s, "<") == 0 ||
           strcmp(s, "le") == 0 || strcmp(s, "<=") == 0 ||
           strcmp(s, "contains") == 0 ||
           strcmp(s, "starts-with") == 0 || strcmp(s, "starts_with") == 0 ||
           strcmp(s, "ends-with") == 0 || strcmp(s, "ends_with") == 0;
}

static int json_filter_op_requires_value(const char *s) {
    if (!s) return 0;
    return !(strcmp(s, "exists") == 0 || strcmp(s, "present") == 0 ||
             strcmp(s, "missing") == 0 || strcmp(s, "absent") == 0 ||
             strcmp(s, "truthy") == 0 || strcmp(s, "true") == 0 ||
             strcmp(s, "falsey") == 0 || strcmp(s, "falsy") == 0 || strcmp(s, "false") == 0);
}

static int json_filter_type_valid(const char *type) {
    return strcmp(type, "auto") == 0 ||
           strcmp(type, "string") == 0 || strcmp(type, "str") == 0 || strcmp(type, "json") == 0 ||
           strcmp(type, "int") == 0 || strcmp(type, "int64") == 0 ||
           strcmp(type, "float") == 0 || strcmp(type, "float64") == 0 || strcmp(type, "number") == 0 ||
           strcmp(type, "bool") == 0 || strcmp(type, "boolean") == 0;
}

static cJSON *build_json_filter_args(const token_list *tokens, char **error) {
    const char *column = "_line";
    const char *path = NULL;
    const char *op = "exists";
    const char *value = NULL;
    const char *type = "auto";
    const char *pos[4] = {0};
    size_t n_pos = 0;

    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (strncmp(tok, "column=", 7) == 0) {
            column = tok + 7;
        } else if (strncmp(tok, "path=", 5) == 0) {
            path = tok + 5;
        } else if (strncmp(tok, "op=", 3) == 0) {
            op = tok + 3;
        } else if (strncmp(tok, "value=", 6) == 0) {
            value = tok + 6;
        } else if (strncmp(tok, "type=", 5) == 0) {
            type = tok + 5;
        } else if (n_pos < 4) {
            pos[n_pos++] = tok;
        } else {
            set_error(error, "json-filter: unexpected argument");
            return NULL;
        }
    }

    if (n_pos == 1) {
        if (!path) path = pos[0];
    } else if (n_pos == 2) {
        if (json_filter_is_op_token(pos[1])) {
            if (!path) path = pos[0];
            op = pos[1];
        } else {
            column = pos[0];
            if (!path) path = pos[1];
        }
    } else if (n_pos == 3) {
        if (json_filter_is_op_token(pos[1])) {
            if (!path) path = pos[0];
            op = pos[1];
            if (!value) value = pos[2];
        } else {
            column = pos[0];
            if (!path) path = pos[1];
            op = pos[2];
        }
    } else if (n_pos == 4) {
        column = pos[0];
        if (!path) path = pos[1];
        op = pos[2];
        if (!value) value = pos[3];
    }

    if (!path || path[0] == '\0') {
        set_error(error, "json-filter requires a path");
        return NULL;
    }
    if (!json_filter_is_op_token(op)) {
        set_error(error, "json-filter op must be exists, missing, truthy, falsey, eq, ne, gt, ge, lt, le, contains, starts-with, or ends-with");
        return NULL;
    }
    if (json_filter_op_requires_value(op) && !value) {
        set_error(error, "json-filter comparison op requires value");
        return NULL;
    }
    if (!json_filter_type_valid(type)) {
        set_error(error, "json-filter type must be auto, string, int, float, or bool");
        return NULL;
    }

    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", column && column[0] ? column : "_line");
    cJSON_AddStringToObject(args, "path", path);
    cJSON_AddStringToObject(args, "op", op);
    if (value) cJSON_AddStringToObject(args, "value", value);
    cJSON_AddStringToObject(args, "type", type);
    return args;
}

static const char *json_schema_normalize_type(const char *s) {
    if (strcmp(s, "str") == 0 || strcmp(s, "string") == 0) return "string";
    if (strcmp(s, "int") == 0 || strcmp(s, "int64") == 0 || strcmp(s, "integer") == 0) return "integer";
    if (strcmp(s, "float") == 0 || strcmp(s, "float64") == 0 || strcmp(s, "number") == 0) return "number";
    if (strcmp(s, "bool") == 0 || strcmp(s, "boolean") == 0) return "boolean";
    if (strcmp(s, "object") == 0 || strcmp(s, "array") == 0 || strcmp(s, "null") == 0) return s;
    return NULL;
}

static int json_schema_mode_valid(const char *s) {
    return strcmp(s, "annotate") == 0 || strcmp(s, "check") == 0 ||
           strcmp(s, "filter") == 0 || strcmp(s, "keep") == 0;
}

static int json_schema_add_required(cJSON *schema, const char *spec, char **error) {
    cJSON *arr = cJSON_CreateArray();
    if (!arr) return -1;
    char *copy = strdup(spec ? spec : "");
    if (!copy) { cJSON_Delete(arr); return -1; }
    int n = 0;
    char *saveptr = NULL;
    for (char *part = strtok_r(copy, ",", &saveptr); part; part = strtok_r(NULL, ",", &saveptr)) {
        if (part[0] == '\0') continue;
        cJSON_AddItemToArray(arr, cJSON_CreateString(part));
        n++;
    }
    free(copy);
    if (n == 0) {
        cJSON_Delete(arr);
        set_error(error, "json-schema required= needs at least one field");
        return -1;
    }
    cJSON_AddItemToObject(schema, "required", arr);
    return 0;
}

static int json_schema_add_types(cJSON *schema, const char *spec, char **error) {
    cJSON *props = cJSON_GetObjectItemCaseSensitive(schema, "properties");
    if (!props) {
        props = cJSON_CreateObject();
        if (!props) return -1;
        cJSON_AddItemToObject(schema, "properties", props);
    }

    char *copy = strdup(spec ? spec : "");
    if (!copy) return -1;
    int n = 0;
    char *saveptr = NULL;
    for (char *part = strtok_r(copy, ",", &saveptr); part; part = strtok_r(NULL, ",", &saveptr)) {
        if (part[0] == '\0') continue;
        char *colon = strchr(part, ':');
        if (!colon) {
            free(copy);
            set_error(error, "json-schema types= entries must be name:type");
            return -1;
        }
        *colon = '\0';
        const char *name = part;
        const char *type = json_schema_normalize_type(colon + 1);
        if (!name[0] || !type) {
            free(copy);
            set_error(error, "json-schema types= uses unsupported type");
            return -1;
        }
        cJSON *prop = cJSON_CreateObject();
        if (!prop) { free(copy); return -1; }
        cJSON_AddStringToObject(prop, "type", type);
        cJSON_AddItemToObject(props, name, prop);
        n++;
    }
    free(copy);
    if (n == 0) {
        set_error(error, "json-schema types= needs at least one name:type entry");
        return -1;
    }
    return 0;
}

static cJSON *build_json_schema_args(const token_list *tokens, char **error) {
    const char *column = "_line";
    const char *mode = "annotate";
    const char *result = "_valid";
    const char *schema_raw = NULL;
    const char *required = NULL;
    const char *types = NULL;
    const char *pos = NULL;
    int audit = 0;
    size_t audit_limit = 0;
    int audit_include_row_set = 0;
    int audit_include_row = 1;
    const char *audit_columns = NULL;
    const char *audit_redact = NULL;
    const char *audit_hash_columns = NULL;
    size_t audit_max_bytes = 0;
    size_t audit_max_cell_bytes = 0;

    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (strncmp(tok, "column=", 7) == 0) column = tok + 7;
        else if (strncmp(tok, "mode=", 5) == 0) mode = tok + 5;
        else if (strncmp(tok, "result=", 7) == 0) result = tok + 7;
        else if (strncmp(tok, "schema=", 7) == 0) schema_raw = tok + 7;
        else if (strncmp(tok, "required=", 9) == 0) required = tok + 9;
        else if (strncmp(tok, "types=", 6) == 0) types = tok + 6;
        else if (strcmp(tok, "audit") == 0 || strcmp(tok, "audit=true") == 0) audit = 1;
        else if (strcmp(tok, "audit=false") == 0) audit = 0;
        else if (strncmp(tok, "audit_limit=", 12) == 0 || strncmp(tok, "audit-limit=", 12) == 0) {
            const char *v = tok + 12;
            if (dsl_parse_positive_size(v, "json-schema audit_limit", &audit_limit, error) != TF_OK) return NULL;
        }
        else if (strncmp(tok, "audit_include_row=", 18) == 0 || strncmp(tok, "audit-include-row=", 18) == 0) {
            const char *v = strchr(tok, '=');
            v = v ? v + 1 : "";
            if (assert_parse_bool_token(v, "json-schema audit_include_row", &audit_include_row, error) != TF_OK) return NULL;
            audit_include_row_set = 1;
        }
        else if (strncmp(tok, "audit_columns=", 14) == 0 || strncmp(tok, "audit-columns=", 14) == 0) {
            audit_columns = strchr(tok, '=');
            audit_columns = audit_columns ? audit_columns + 1 : "";
        }
        else if (strncmp(tok, "audit_redact=", 13) == 0 || strncmp(tok, "audit-redact=", 13) == 0) {
            audit_redact = strchr(tok, '=');
            audit_redact = audit_redact ? audit_redact + 1 : "";
        }
        else if (strncmp(tok, "audit_hash_columns=", 19) == 0 || strncmp(tok, "audit-hash-columns=", 19) == 0) {
            audit_hash_columns = strchr(tok, '=');
            audit_hash_columns = audit_hash_columns ? audit_hash_columns + 1 : "";
        }
        else if (strncmp(tok, "audit_max_bytes=", 16) == 0 || strncmp(tok, "audit-max-bytes=", 16) == 0) {
            const char *v = strchr(tok, '=');
            char *end = NULL;
            long n = strtol(v ? v + 1 : "", &end, 10);
            if (!v || v[1] == '\0' || !end || *end != '\0' || n < 0) {
                set_error(error, "json-schema audit_max_bytes must be a non-negative integer");
                return NULL;
            }
            audit_max_bytes = (size_t)n;
        }
        else if (strncmp(tok, "audit_max_cell_bytes=", 21) == 0 || strncmp(tok, "audit-max-cell-bytes=", 21) == 0) {
            const char *v = strchr(tok, '=');
            char *end = NULL;
            long n = strtol(v ? v + 1 : "", &end, 10);
            if (!v || v[1] == '\0' || !end || *end != '\0' || n < 0) {
                set_error(error, "json-schema audit_max_cell_bytes must be a non-negative integer");
                return NULL;
            }
            audit_max_cell_bytes = (size_t)n;
        }
        else if (!pos) pos = tok;
        else {
            set_error(error, "json-schema: unexpected argument");
            return NULL;
        }
    }

    if (pos) column = pos;
    if (!json_schema_mode_valid(mode)) {
        set_error(error, "json-schema mode must be annotate or filter");
        return NULL;
    }

    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", column && column[0] ? column : "_line");
    cJSON_AddStringToObject(args, "mode", mode);
    cJSON_AddStringToObject(args, "result", result && result[0] ? result : "_valid");
    if (audit) cJSON_AddBoolToObject(args, "audit", 1);
    if (audit_limit > 0) cJSON_AddNumberToObject(args, "audit_limit", (double)audit_limit);
    if (audit_include_row_set) cJSON_AddBoolToObject(args, "audit_include_row", audit_include_row);
    if (audit_columns && audit_columns[0] && add_audit_csv_arg(args, "audit_columns", audit_columns) != TF_OK) {
        cJSON_Delete(args);
        set_error(error, "json-schema: invalid audit_columns");
        return NULL;
    }
    if (audit_redact && audit_redact[0] && add_audit_csv_arg(args, "audit_redact", audit_redact) != TF_OK) {
        cJSON_Delete(args);
        set_error(error, "json-schema: invalid audit_redact");
        return NULL;
    }
    if (audit_hash_columns && audit_hash_columns[0] && add_audit_csv_arg(args, "audit_hash_columns", audit_hash_columns) != TF_OK) {
        cJSON_Delete(args);
        set_error(error, "json-schema: invalid audit_hash_columns");
        return NULL;
    }
    if (audit_max_bytes > 0) cJSON_AddNumberToObject(args, "audit_max_bytes", (double)audit_max_bytes);
    if (audit_max_cell_bytes > 0) cJSON_AddNumberToObject(args, "audit_max_cell_bytes", (double)audit_max_cell_bytes);

    if (schema_raw && strcmp(schema_raw, "true") == 0) {
        cJSON_AddBoolToObject(args, "schema", 1);
    } else if (schema_raw && strcmp(schema_raw, "false") == 0) {
        cJSON_AddBoolToObject(args, "schema", 0);
    } else if (schema_raw) {
        cJSON_AddStringToObject(args, "schema", schema_raw);
    } else {
        if (!required && !types) {
            cJSON_Delete(args);
            set_error(error, "json-schema requires schema=, required=, or types=");
            return NULL;
        }
        cJSON *schema = cJSON_CreateObject();
        cJSON_AddStringToObject(schema, "type", "object");
        if (required && json_schema_add_required(schema, required, error) != 0) {
            cJSON_Delete(schema);
            cJSON_Delete(args);
            return NULL;
        }
        if (types && json_schema_add_types(schema, types, error) != 0) {
            cJSON_Delete(schema);
            cJSON_Delete(args);
            return NULL;
        }
        cJSON_AddItemToObject(args, "schema", schema);
    }
    return args;
}

static cJSON *build_grep_args(const token_list *tokens, char **error) {
    /* grep [-v] [-r] pattern */
    if (tokens->count < 2) {
        set_error(error, "grep requires a pattern argument");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    size_t idx = 1;
    while (idx < tokens->count && tokens->items[idx][0] == '-' && tokens->items[idx][1] != '\0') {
        const char *flag = tokens->items[idx];
        if (strcmp(flag, "-v") == 0) {
            cJSON_AddBoolToObject(args, "invert", 1);
        } else if (strcmp(flag, "-r") == 0 || strcmp(flag, "--regex") == 0) {
            cJSON_AddBoolToObject(args, "regex", 1);
        } else if (strcmp(flag, "-rv") == 0 || strcmp(flag, "-vr") == 0) {
            cJSON_AddBoolToObject(args, "invert", 1);
            cJSON_AddBoolToObject(args, "regex", 1);
        } else {
            break; /* not a flag, must be pattern */
        }
        idx++;
    }
    if (idx >= tokens->count) {
        cJSON_Delete(args);
        set_error(error, "grep requires a pattern argument");
        return NULL;
    }
    cJSON_AddStringToObject(args, "pattern", tokens->items[idx]);
    /* Optional column name after pattern */
    if (idx + 1 < tokens->count) {
        cJSON_AddStringToObject(args, "column", tokens->items[idx + 1]);
    }
    return args;
}

static cJSON *build_pivot_args(const token_list *tokens, char **error) {
    /* pivot name_col value_col [agg] [categories=a,b] [max_categories=N] [sorted=true|--sorted] [spill_dir=DIR] */
    if (tokens->count < 3) {
        set_error(error, "pivot requires name_column and value_column");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *cats = NULL;
    int agg_set = 0;
    cJSON_AddStringToObject(args, "name_column", tokens->items[1]);
    cJSON_AddStringToObject(args, "value_column", tokens->items[2]);
    for (size_t i = 3; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        size_t max_categories = 0;
        int opt = parse_positive_option(tok, "max_categories", &max_categories, error, "pivot");
        if (opt < 0) goto fail;
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "max_categories", (double)max_categories);
            continue;
        }
        opt = parse_categories_option(tok, &cats, error, "pivot");
        if (opt < 0) goto fail;
        if (opt > 0) continue;
        size_t spill_num = 0;
        opt = parse_positive_option(tok, "spill_memory_bytes", &spill_num, error, "pivot");
        if (opt < 0) goto fail;
        if (opt > 0) { cJSON_AddNumberToObject(args, "spill_memory_bytes", (double)spill_num); continue; }
        opt = parse_positive_option(tok, "spill_run_rows", &spill_num, error, "pivot");
        if (opt < 0) goto fail;
        if (opt > 0) { cJSON_AddNumberToObject(args, "spill_run_rows", (double)spill_num); continue; }
        opt = parse_positive_option(tok, "spill_output_rows", &spill_num, error, "pivot");
        if (opt < 0) goto fail;
        if (opt > 0) { cJSON_AddNumberToObject(args, "spill_output_rows", (double)spill_num); continue; }
        if (strncmp(tok, "spill_dir=", 10) == 0) {
            const char *dir = tok + 10;
            if (!dir[0]) { set_error(error, "pivot spill_dir cannot be empty"); goto fail; }
            cJSON_AddStringToObject(args, "spill_dir", dir);
            continue;
        }
        if (strcmp(tok, "sorted") == 0 || strcmp(tok, "--sorted") == 0) {
            cJSON_AddBoolToObject(args, "sorted", 1);
            continue;
        }
        if (strncmp(tok, "sorted=", 7) == 0) {
            int sorted = 0;
            if (parse_bool_value(tok + 7, &sorted) != 0) {
                set_error(error, "pivot sorted must be true or false");
                goto fail;
            }
            cJSON_AddBoolToObject(args, "sorted", sorted);
            continue;
        }
        if (!agg_set) {
            cJSON_AddStringToObject(args, "agg", tok);
            agg_set = 1;
            continue;
        }
        set_errorf(error, "pivot: unexpected argument '%s'", tok);
        goto fail;
    }
    if (cats) {
        cJSON_AddItemToObject(args, "categories", cats);
        cats = NULL;
    }
    return args;
fail:
    if (cats) cJSON_Delete(cats);
    cJSON_Delete(args);
    return NULL;
}

static cJSON *build_join_args(const token_list *tokens, char **error) {
    /* join lookup.csv on id [--left|--semi|--anti]
     * semi-join lookup.csv on id
     * anti-join lookup.csv on id
     * join lookup.csv on id=lookup_id [--left|--semi|--anti] [sorted=true]
     * join lookup.csv on=id [--left|--semi|--anti] [sorted=true]
     * join lookup.csv on id [max_state_bytes=N] */
    if (tokens->count < 3) {
        set_error(error, "join requires file and join key");
        return NULL;
    }

    const char *on = NULL;
    size_t opt_start = 3;
    if (strcmp(tokens->items[2], "on") == 0) {
        if (tokens->count < 4) {
            set_error(error, "join requires column after 'on'");
            return NULL;
        }
        on = tokens->items[3];
        opt_start = 4;
    } else if (strncmp(tokens->items[2], "on=", 3) == 0 && tokens->items[2][3] != '\0') {
        on = tokens->items[2] + 3;
        opt_start = 3;
    } else {
        set_error(error, "join: expected 'on' or 'on=column'");
        return NULL;
    }

    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "file", tokens->items[1]);
    cJSON_AddStringToObject(args, "on", on);
    for (size_t i = opt_start; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (strcmp(tok, "--left") == 0) {
            cJSON_AddStringToObject(args, "how", "left");
            continue;
        }
        if (strcmp(tok, "--semi") == 0) {
            cJSON_AddStringToObject(args, "how", "semi");
            continue;
        }
        if (strcmp(tok, "--anti") == 0) {
            cJSON_AddStringToObject(args, "how", "anti");
            continue;
        }
        if (strcmp(tok, "sorted") == 0 || strcmp(tok, "--sorted") == 0) {
            cJSON_AddBoolToObject(args, "sorted", 1);
            continue;
        }
        if (strncmp(tok, "sorted=", 7) == 0) {
            int sorted = 0;
            if (parse_bool_value(tok + 7, &sorted) != 0) {
                set_error(error, "join sorted must be true or false");
                cJSON_Delete(args);
                return NULL;
            }
            cJSON_AddBoolToObject(args, "sorted", sorted);
            continue;
        }
        size_t max_lookup_rows = 0;
        int opt = parse_positive_option(tok, "max_lookup_rows", &max_lookup_rows, error, "join");
        if (opt < 0) { cJSON_Delete(args); return NULL; }
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "max_lookup_rows", (double)max_lookup_rows);
            continue;
        }
        size_t max_lookup_keys = 0;
        opt = parse_positive_option(tok, "max_lookup_keys", &max_lookup_keys, error, "join");
        if (opt < 0) { cJSON_Delete(args); return NULL; }
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "max_lookup_keys", (double)max_lookup_keys);
            continue;
        }
        size_t max_lookup_bytes = 0;
        opt = parse_positive_option(tok, "max_lookup_bytes", &max_lookup_bytes, error, "join");
        if (opt < 0) { cJSON_Delete(args); return NULL; }
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "max_lookup_bytes", (double)max_lookup_bytes);
            continue;
        }
        size_t max_state_bytes = 0;
        opt = parse_positive_option(tok, "max_state_bytes", &max_state_bytes, error, "join");
        if (opt < 0) { cJSON_Delete(args); return NULL; }
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "max_state_bytes", (double)max_state_bytes);
            continue;
        }
        size_t max_matches_per_row = 0;
        opt = parse_positive_option(tok, "max_matches_per_row", &max_matches_per_row, error, "join");
        if (opt < 0) { cJSON_Delete(args); return NULL; }
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "max_matches_per_row", (double)max_matches_per_row);
            continue;
        }
        size_t max_output_rows = 0;
        opt = parse_positive_option(tok, "max_output_rows", &max_output_rows, error, "join");
        if (opt < 0) { cJSON_Delete(args); return NULL; }
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "max_output_rows", (double)max_output_rows);
            continue;
        }
        if (strncmp(tok, "spill_dir=", 10) == 0) {
            const char *value_s = tok + 10;
            if (!value_s[0]) {
                set_error(error, "join: spill_dir cannot be empty");
                cJSON_Delete(args);
                return NULL;
            }
            cJSON_AddStringToObject(args, "spill_dir", value_s);
            continue;
        }
        size_t spill_value = 0;
        opt = parse_positive_option(tok, "spill_memory_bytes", &spill_value, error, "join");
        if (opt < 0) { cJSON_Delete(args); return NULL; }
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "spill_memory_bytes", (double)spill_value);
            continue;
        }
        opt = parse_positive_option(tok, "spill_run_rows", &spill_value, error, "join");
        if (opt < 0) { cJSON_Delete(args); return NULL; }
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "spill_run_rows", (double)spill_value);
            continue;
        }
        opt = parse_positive_option(tok, "spill_output_rows", &spill_value, error, "join");
        if (opt < 0) { cJSON_Delete(args); return NULL; }
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "spill_output_rows", (double)spill_value);
            continue;
        }
        set_errorf(error, "join: unexpected argument '%s'", tok);
        cJSON_Delete(args);
        return NULL;
    }
    return args;
}

static cJSON *build_stack_args(const token_list *tokens, char **error) {
    /* stack file.csv [--tag source] */
    if (tokens->count < 2) {
        set_error(error, "stack requires a file path");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "file", tokens->items[1]);
    for (size_t i = 2; i < tokens->count; i++) {
        if (strcmp(tokens->items[i], "--tag") == 0 && i + 1 < tokens->count) {
            cJSON_AddStringToObject(args, "tag", tokens->items[i + 1]);
            i++;
        }
    }
    return args;
}

static int add_comma_columns(cJSON *cols, const char *spec, char **error, const char *op_label) {
    const char *text = spec ? spec : "";
    const char *p = text;
    const char *end = text + strlen(text);
    while (1) {
        const char *comma = dsl_find_top_level_comma(p, end);
        size_t len = comma ? (size_t)(comma - p) : (size_t)(end - p);
        char *value = trimmed_token_copy(p, len);
        if (!value) {
            set_oom_error_if_unset(error);
            return -1;
        }
        if (value[0] == '\0') {
            free(value);
            set_errorf(error, "%s: empty column name", op_label);
            return -1;
        }
        if (dsl_add_string_array_item(cols, value) != TF_OK) {
            free(value);
            set_oom_error_if_unset(error);
            return -1;
        }
        free(value);
        if (!comma) break;
        p = comma + 1;
    }
    return 0;
}



static int set_op_is_union_all_label(const char *op_label) {
    return strcmp(op_label, "union-all") == 0 || strcmp(op_label, "union_all") == 0;
}

static int set_op_is_bag_label(const char *op_label) {
    return strcmp(op_label, "intersect-all") == 0 || strcmp(op_label, "intersect_all") == 0 ||
           strcmp(op_label, "setdiff-all") == 0 || strcmp(op_label, "setdiff_all") == 0 ||
           strcmp(op_label, "except-all") == 0 || strcmp(op_label, "except_all") == 0;
}

static cJSON *build_set_file_args(const token_list *tokens, char **error, const char *op_label) {
    /* intersect/setdiff/intersect-all/setdiff-all/union/union-all file.csv [columns=a,b|a,b]
     * [max_lookup_rows=N] [max_lookup_bytes=N] [max_output_keys=N]
     * [max_state_bytes=N] [spill_dir=DIR] [spill_memory_bytes=N]
     * [spill_run_rows=N] [spill_output_rows=N] [sorted=true|--sorted] */
    if (tokens->count < 2) {
        set_errorf(error, "%s requires a file path", op_label);
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *cols = cJSON_CreateArray();
    if (!args || !cols) { cJSON_Delete(args); cJSON_Delete(cols); return NULL; }
    cJSON_AddStringToObject(args, "file", tokens->items[1]);

    for (size_t i = 2; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        size_t value = 0;
        int opt = parse_positive_option(tok, "max_lookup_rows", &value, error, op_label);
        if (opt < 0) goto fail;
        if (opt > 0) { cJSON_AddNumberToObject(args, "max_lookup_rows", (double)value); continue; }
        opt = parse_positive_option(tok, "max_lookup_keys", &value, error, op_label);
        if (opt < 0) goto fail;
        if (opt > 0) { cJSON_AddNumberToObject(args, "max_lookup_keys", (double)value); continue; }
        opt = parse_positive_option(tok, "max_lookup_bytes", &value, error, op_label);
        if (opt < 0) goto fail;
        if (opt > 0) { cJSON_AddNumberToObject(args, "max_lookup_bytes", (double)value); continue; }
        opt = parse_positive_option(tok, "max_output_keys", &value, error, op_label);
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (set_op_is_bag_label(op_label)) {
                set_errorf(error, "%s: max_output_keys is only valid for duplicate-eliminating set ops", op_label);
                goto fail;
            }
            cJSON_AddNumberToObject(args, "max_output_keys", (double)value);
            continue;
        }
        opt = parse_positive_option(tok, "max_state_bytes", &value, error, op_label);
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (set_op_is_union_all_label(op_label)) {
                set_errorf(error, "%s: max_state_bytes is only valid for duplicate-eliminating or bag-count set ops", op_label);
                goto fail;
            }
            cJSON_AddNumberToObject(args, "max_state_bytes", (double)value);
            continue;
        }

        if (strncmp(tok, "spill_dir=", 10) == 0) {
            const char *value_s = tok + 10;
            if (!value_s[0]) {
                set_errorf(error, "%s: spill_dir cannot be empty", op_label);
                goto fail;
            }
            if (set_op_is_union_all_label(op_label)) {
                set_errorf(error, "%s: spill_dir is only valid for duplicate-eliminating or bag-count set ops", op_label);
                goto fail;
            }
            cJSON_AddStringToObject(args, "spill_dir", value_s);
            continue;
        }
        opt = parse_positive_option(tok, "spill_memory_bytes", &value, error, op_label);
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (set_op_is_union_all_label(op_label)) {
                set_errorf(error, "%s: spill_memory_bytes is only valid for duplicate-eliminating or bag-count set ops", op_label);
                goto fail;
            }
            cJSON_AddNumberToObject(args, "spill_memory_bytes", (double)value);
            continue;
        }
        opt = parse_positive_option(tok, "spill_run_rows", &value, error, op_label);
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (set_op_is_union_all_label(op_label)) {
                set_errorf(error, "%s: spill_run_rows is only valid for duplicate-eliminating or bag-count set ops", op_label);
                goto fail;
            }
            cJSON_AddNumberToObject(args, "spill_run_rows", (double)value);
            continue;
        }
        opt = parse_positive_option(tok, "spill_output_rows", &value, error, op_label);
        if (opt < 0) goto fail;
        if (opt > 0) {
            if (set_op_is_union_all_label(op_label)) {
                set_errorf(error, "%s: spill_output_rows is only valid for duplicate-eliminating or bag-count set ops", op_label);
                goto fail;
            }
            cJSON_AddNumberToObject(args, "spill_output_rows", (double)value);
            continue;
        }

        if (strcmp(tok, "sorted") == 0 || strcmp(tok, "--sorted") == 0) {
            cJSON_AddBoolToObject(args, "sorted", 1);
            continue;
        }
        if (strncmp(tok, "sorted=", 7) == 0) {
            int sorted = 0;
            if (parse_bool_value(tok + 7, &sorted) != 0) {
                set_errorf(error, "%s: sorted must be true or false", op_label);
                goto fail;
            }
            cJSON_AddBoolToObject(args, "sorted", sorted);
            continue;
        }

        if (strncmp(tok, "columns=", 8) == 0) {
            if (add_comma_columns(cols, tok + 8, error, op_label) != 0) goto fail;
            continue;
        }
        if (cJSON_GetArraySize(cols) == 0 && strchr(tok, ',')) {
            if (add_comma_columns(cols, tok, error, op_label) != 0) goto fail;
            continue;
        }
        if (cJSON_GetArraySize(cols) == 0) {
            cJSON_AddItemToArray(cols, cJSON_CreateString(tok));
            continue;
        }
        set_errorf(error, "%s: unexpected argument '%s'", op_label, tok);
        goto fail;
    }

    if (cJSON_GetArraySize(cols) > 0) cJSON_AddItemToObject(args, "columns", cols);
    else cJSON_Delete(cols);
    return args;
fail:
    cJSON_Delete(cols);
    cJSON_Delete(args);
    return NULL;
}

static int parse_bool_value(const char *s, int *out) {
    if (strcmp(s, "true") == 0 || strcmp(s, "1") == 0 || strcmp(s, "yes") == 0) {
        *out = 1;
        return 0;
    }
    if (strcmp(s, "false") == 0 || strcmp(s, "0") == 0 || strcmp(s, "no") == 0) {
        *out = 0;
        return 0;
    }
    return -1;
}

static cJSON *build_rowid_args(const token_list *tokens, char **error) {
    /* rowid [col1,col2] [result=name|as=name] [sorted=true|--sorted] [max_keys=N] [max_state_bytes=N] */
    cJSON *args = cJSON_CreateObject();
    cJSON *cols = cJSON_CreateArray();
    if (!args || !cols) { cJSON_Delete(args); cJSON_Delete(cols); return NULL; }

    int have_result = 0;
    int have_sorted = 0;
    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        size_t max_keys = 0;
        int opt = parse_positive_option(tok, "max_keys", &max_keys, error, "rowid");
        if (opt < 0) { cJSON_Delete(args); cJSON_Delete(cols); return NULL; }
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "max_keys", (double)max_keys);
            continue;
        }

        size_t max_state_bytes = 0;
        opt = parse_positive_option(tok, "max_state_bytes", &max_state_bytes, error, "rowid");
        if (opt < 0) { cJSON_Delete(args); cJSON_Delete(cols); return NULL; }
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "max_state_bytes", (double)max_state_bytes);
            continue;
        }

        if (strcmp(tok, "--sorted") == 0 || strcmp(tok, "sorted") == 0) {
            if (have_sorted) {
                set_error(error, "rowid sorted specified more than once");
                cJSON_Delete(args); cJSON_Delete(cols); return NULL;
            }
            cJSON_AddBoolToObject(args, "sorted", 1);
            have_sorted = 1;
            continue;
        }
        if (strncmp(tok, "sorted=", 7) == 0) {
            int sorted = 0;
            if (parse_bool_value(tok + 7, &sorted) != 0) {
                set_error(error, "rowid sorted must be true or false");
                cJSON_Delete(args); cJSON_Delete(cols); return NULL;
            }
            if (have_sorted) {
                set_error(error, "rowid sorted specified more than once");
                cJSON_Delete(args); cJSON_Delete(cols); return NULL;
            }
            cJSON_AddBoolToObject(args, "sorted", sorted);
            have_sorted = 1;
            continue;
        }

        const char *result = NULL;
        if (strncmp(tok, "result=", 7) == 0) result = tok + 7;
        else if (strncmp(tok, "as=", 3) == 0) result = tok + 3;
        if (result) {
            if (!result[0] || have_result) {
                set_error(error, "rowid result specified more than once or empty");
                cJSON_Delete(args); cJSON_Delete(cols); return NULL;
            }
            cJSON_AddStringToObject(args, "result", result);
            have_result = 1;
            continue;
        }

        if (add_comma_columns(cols, tok, error, "rowid") != 0) {
            cJSON_Delete(args);
            cJSON_Delete(cols);
            return NULL;
        }
    }

    if (cJSON_GetArraySize(cols) > 0) cJSON_AddItemToObject(args, "columns", cols);
    else cJSON_Delete(cols);
    return args;
}

static cJSON *build_rleid_args(const token_list *tokens, char **error) {
    /* rleid col1,col2 [result=name] */
    if (tokens->count < 2) {
        set_error(error, "rleid requires one or more columns");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *cols = cJSON_CreateArray();
    if (!args || !cols) { cJSON_Delete(args); cJSON_Delete(cols); return NULL; }

    int result_set = 0;
    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        const char *result = NULL;
        if (strncmp(tok, "result=", 7) == 0) result = tok + 7;
        else if (strncmp(tok, "as=", 3) == 0) result = tok + 3;
        if (result) {
            if (result_set) {
                set_error(error, "rleid result specified more than once");
                cJSON_Delete(args);
                cJSON_Delete(cols);
                return NULL;
            }
            if (result[0] == '\0') {
                set_error(error, "rleid result name cannot be empty");
                cJSON_Delete(args);
                cJSON_Delete(cols);
                return NULL;
            }
            cJSON_AddStringToObject(args, "result", result);
            result_set = 1;
            continue;
        }
        if (add_comma_columns(cols, tok, error, "rleid") != 0) {
            cJSON_Delete(args);
            cJSON_Delete(cols);
            return NULL;
        }
    }
    if (cJSON_GetArraySize(cols) == 0) {
        set_error(error, "rleid requires one or more columns");
        cJSON_Delete(args);
        cJSON_Delete(cols);
        return NULL;
    }
    cJSON_AddItemToObject(args, "columns", cols);
    return args;
}

static int parse_positive_offset(const char *s, long *out);

static cJSON *build_sample_args(const token_list *tokens, char **error) {
    /* sample N [seed=N|seed=random] */
    if (tokens->count < 2) {
        set_error(error, "sample requires N");
        return NULL;
    }
    long n = 0;
    if (parse_positive_offset(tokens->items[1], &n) != 0) {
        set_error(error, "sample N must be a positive integer");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    if (!args) return NULL;
    cJSON_AddNumberToObject(args, "n", n);
    int seed_set = 0;
    for (size_t i = 2; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (strncmp(tok, "seed=", 5) == 0) {
            if (seed_set) {
                set_error(error, "sample seed specified more than once");
                cJSON_Delete(args);
                return NULL;
            }
            const char *value = tok + 5;
            if (strcmp(value, "random") == 0) {
                cJSON_AddStringToObject(args, "seed", "random");
            } else {
                char *end = NULL;
                long seed = strtol(value, &end, 10);
                if (!value[0] || !end || *end != '\0' || seed < 0) {
                    set_error(error, "sample seed must be a non-negative integer or random");
                    cJSON_Delete(args);
                    return NULL;
                }
                cJSON_AddNumberToObject(args, "seed", seed);
            }
            seed_set = 1;
            continue;
        }
        set_errorf(error, "sample: unknown option '%s'", tok);
        cJSON_Delete(args);
        return NULL;
    }
    return args;
}

static int parse_positive_offset(const char *s, long *out) {
    char *end = NULL;
    long v = strtol(s, &end, 10);
    if (!s[0] || *end != '\0' || v <= 0) return -1;
    *out = v;
    return 0;
}

static int add_shift_type(cJSON *args, const char *type, const char *op_name,
                          char **error) {
    if (strcmp(type, "lag") != 0 && strcmp(type, "lead") != 0 && strcmp(type, "shift") != 0) {
        set_errorf(error, "%s type must be lag or lead", op_name);
        return -1;
    }
    cJSON *prev = cJSON_GetObjectItemCaseSensitive(args, "type");
    if (prev && cJSON_IsString(prev) && strcmp(prev->valuestring, type) != 0) {
        set_errorf(error, "%s type specified more than once", op_name);
        return -1;
    }
    if (!prev) cJSON_AddStringToObject(args, "type", type);
    return 0;
}

static cJSON *build_shift_like_args(const token_list *tokens, char **error,
                                    const char *op_name) {
    /* lead|lag column [offset] [result_name]
     * shift column [offset] [result_name] [type=lag|lead]
     */
    if (tokens->count < 2) {
        set_errorf(error, "%s requires a column name", op_name);
        return NULL;
    }

    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    int have_offset = 0;
    int have_result = 0;

    for (size_t i = 2; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        const char *val = NULL;

        if (strncmp(tok, "offset=", 7) == 0) val = tok + 7;
        else if (strncmp(tok, "n=", 2) == 0) val = tok + 2;
        if (val) {
            long off = 0;
            if (parse_positive_offset(val, &off) != 0) {
                set_errorf(error, "%s offset must be a positive integer", op_name);
                cJSON_Delete(args);
                return NULL;
            }
            if (have_offset) {
                set_errorf(error, "%s offset specified more than once", op_name);
                cJSON_Delete(args);
                return NULL;
            }
            cJSON_AddNumberToObject(args, "offset", off);
            have_offset = 1;
            continue;
        }

        if (strncmp(tok, "result=", 7) == 0) val = tok + 7;
        else if (strncmp(tok, "as=", 3) == 0) val = tok + 3;
        else val = NULL;
        if (val) {
            if (!val[0] || have_result) {
                set_errorf(error, "%s result specified more than once or empty", op_name);
                cJSON_Delete(args);
                return NULL;
            }
            cJSON_AddStringToObject(args, "result", val);
            have_result = 1;
            continue;
        }

        if (strncmp(tok, "type=", 5) == 0) {
            if (strcmp(op_name, "shift") != 0) {
                set_errorf(error, "%s does not accept type=", op_name);
                cJSON_Delete(args);
                return NULL;
            }
            if (add_shift_type(args, tok + 5, op_name, error) != 0) {
                cJSON_Delete(args);
                return NULL;
            }
            continue;
        }

        if (strcmp(op_name, "shift") == 0 &&
            (strcmp(tok, "lag") == 0 || strcmp(tok, "lead") == 0 || strcmp(tok, "shift") == 0)) {
            if (add_shift_type(args, tok, op_name, error) != 0) {
                cJSON_Delete(args);
                return NULL;
            }
            continue;
        }

        long off = 0;
        if (parse_positive_offset(tok, &off) == 0) {
            if (have_offset) {
                set_errorf(error, "%s offset specified more than once", op_name);
                cJSON_Delete(args);
                return NULL;
            }
            cJSON_AddNumberToObject(args, "offset", off);
            have_offset = 1;
            continue;
        }

        if (!have_result) {
            cJSON_AddStringToObject(args, "result", tok);
            have_result = 1;
            continue;
        }

        set_errorf(error, "unexpected %s argument '%s'", op_name, tok);
        cJSON_Delete(args);
        return NULL;
    }

    return args;
}

static cJSON *build_date_trunc_args(const token_list *tokens, char **error) {
    /* date-trunc column granularity [result_name|result=...] [missing=error|null|ignore] [on_type_error=fail|null] */
    if (tokens->count < 3) {
        set_error(error, "date-trunc requires column and granularity");
        return NULL;
    }
    if (!valid_date_trunc_unit(tokens->items[2])) {
        set_error(error, "date-trunc trunc must be year, month, day, hour, minute, or second");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    cJSON_AddStringToObject(args, "trunc", tokens->items[2]);
    int result_set = 0;
    for (size_t i = 3; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        int opt = add_h14_policy_option(args, tok, "date-trunc", error);
        if (opt == TF_ERROR) goto fail;
        if (opt == TF_OK) continue;
        if (strncmp(tok, "result=", 7) == 0 || strncmp(tok, "as=", 3) == 0) {
            const char *value = tok + (tok[0] == 'a' ? 3 : 7);
            if (!*value) { set_error(error, "date-trunc result cannot be empty"); goto fail; }
            if (result_set) { set_error(error, "date-trunc duplicate result argument"); goto fail; }
            cJSON_AddStringToObject(args, "result", value);
            result_set = 1;
            continue;
        }
        if (!result_set) {
            cJSON_AddStringToObject(args, "result", tok);
            result_set = 1;
            continue;
        }
        set_errorf(error, "date-trunc unexpected argument '%s'", tok);
        goto fail;
    }
    return args;
fail:
    cJSON_Delete(args);
    return NULL;
}

static cJSON *build_onehot_args(const token_list *tokens, char **error) {
    /* onehot column [--drop] [categories=a,b] [max_categories=N] [max_state_bytes=N] [unknown=error|other|null] */
    if (tokens->count < 2) {
        set_error(error, "onehot requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *cats = NULL;
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    for (size_t i = 2; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        if (strcmp(tok, "--drop") == 0) {
            cJSON_AddBoolToObject(args, "drop", 1);
            continue;
        }
        size_t max_categories = 0;
        int opt = parse_positive_option(tok, "max_categories", &max_categories, error, "onehot");
        if (opt < 0) goto fail;
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "max_categories", (double)max_categories);
            continue;
        }
        size_t max_state_bytes = 0;
        opt = parse_positive_option(tok, "max_state_bytes", &max_state_bytes, error, "onehot");
        if (opt < 0) goto fail;
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "max_state_bytes", (double)max_state_bytes);
            continue;
        }
        opt = parse_unknown_option(tok, args, error, "onehot");
        if (opt < 0) goto fail;
        if (opt > 0) continue;
        opt = parse_categories_option(tok, &cats, error, "onehot");
        if (opt < 0) goto fail;
        if (opt > 0) continue;
        set_errorf(error, "onehot: unexpected argument '%s'", tok);
        goto fail;
    }
    if (cats) {
        cJSON_AddItemToObject(args, "categories", cats);
        cats = NULL;
    }
    return args;
fail:
    if (cats) cJSON_Delete(cats);
    cJSON_Delete(args);
    return NULL;
}

static cJSON *build_label_encode_args(const token_list *tokens, char **error) {
    /* label-encode column [result_name] [categories=a,b] [max_categories=N] [max_state_bytes=N] [unknown=error|other|null] */
    if (tokens->count < 2) {
        set_error(error, "label-encode requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *cats = NULL;
    int result_set = 0;
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    for (size_t i = 2; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        size_t max_categories = 0;
        int opt = parse_positive_option(tok, "max_categories", &max_categories, error, "label-encode");
        if (opt < 0) goto fail;
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "max_categories", (double)max_categories);
            continue;
        }
        size_t max_state_bytes = 0;
        opt = parse_positive_option(tok, "max_state_bytes", &max_state_bytes, error, "label-encode");
        if (opt < 0) goto fail;
        if (opt > 0) {
            cJSON_AddNumberToObject(args, "max_state_bytes", (double)max_state_bytes);
            continue;
        }
        opt = parse_unknown_option(tok, args, error, "label-encode");
        if (opt < 0) goto fail;
        if (opt > 0) continue;
        opt = parse_categories_option(tok, &cats, error, "label-encode");
        if (opt < 0) goto fail;
        if (opt > 0) continue;
        if (!result_set) {
            cJSON_AddStringToObject(args, "result", tok);
            result_set = 1;
            continue;
        }
        set_errorf(error, "label-encode: unexpected argument '%s'", tok);
        goto fail;
    }
    if (cats) {
        cJSON_AddItemToObject(args, "categories", cats);
        cats = NULL;
    }
    return args;
fail:
    if (cats) cJSON_Delete(cats);
    cJSON_Delete(args);
    return NULL;
}

static cJSON *build_ewma_args(const token_list *tokens, char **error) {
    /* ewma column alpha [result_name] [missing=error|null|ignore] [on_type_error=fail|null] */
    if (tokens->count < 3) {
        set_error(error, "ewma requires column and alpha");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    int alpha_set = 0;
    int result_set = 0;
    for (size_t i = 2; i < tokens->count; i++) {
        int opt = add_h14_policy_option(args, tokens->items[i], "ewma", error);
        if (opt == TF_ERROR) goto fail;
        if (opt == TF_OK) continue;
        if (strncmp(tokens->items[i], "result=", 7) == 0 || strncmp(tokens->items[i], "as=", 3) == 0) {
            const char *value = tokens->items[i] + (tokens->items[i][0] == 'a' ? 3 : 7);
            if (!*value) { set_error(error, "ewma result cannot be empty"); goto fail; }
            cJSON_AddStringToObject(args, "result", value);
            result_set = 1;
            continue;
        }
        if (!alpha_set) {
            double alpha = 0.0;
            if (parse_finite_number_token(tokens->items[i], "ewma alpha", &alpha, error) != TF_OK) goto fail;
            if (alpha < 0.0 || alpha > 1.0) { set_error(error, "ewma alpha must be between 0 and 1"); goto fail; }
            cJSON_AddNumberToObject(args, "alpha", alpha);
            alpha_set = 1;
            continue;
        }
        if (!result_set) {
            cJSON_AddStringToObject(args, "result", tokens->items[i]);
            result_set = 1;
            continue;
        }
        set_errorf(error, "ewma unexpected argument '%s'", tokens->items[i]);
        goto fail;
    }
    if (!alpha_set) { set_error(error, "ewma requires alpha"); goto fail; }
    return args;
fail:
    cJSON_Delete(args);
    return NULL;
}

static cJSON *build_diff_args(const token_list *tokens, char **error) {
    /* diff column [order] [result_name] */
    if (tokens->count < 2) {
        set_error(error, "diff requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    if (tokens->count >= 3) {
        char *end;
        long order = strtol(tokens->items[2], &end, 10);
        if (*end == '\0') {
            cJSON_AddNumberToObject(args, "order", order);
            if (tokens->count >= 4)
                cJSON_AddStringToObject(args, "result", tokens->items[3]);
        } else {
            cJSON_AddStringToObject(args, "result", tokens->items[2]);
        }
    }
    return args;
}

static cJSON *build_anomaly_args(const token_list *tokens, char **error) {
    /* anomaly column [threshold] [result_name] [missing=error|null|ignore] [on_type_error=fail|null] */
    if (tokens->count < 2) {
        set_error(error, "anomaly requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    int threshold_set = 0;
    int result_set = 0;
    for (size_t i = 2; i < tokens->count; i++) {
        int opt = add_h14_policy_option(args, tokens->items[i], "anomaly", error);
        if (opt == TF_ERROR) goto fail;
        if (opt == TF_OK) continue;
        if (strncmp(tokens->items[i], "result=", 7) == 0 || strncmp(tokens->items[i], "as=", 3) == 0) {
            const char *value = tokens->items[i] + (tokens->items[i][0] == 'a' ? 3 : 7);
            if (!*value) { set_error(error, "anomaly result cannot be empty"); goto fail; }
            cJSON_AddStringToObject(args, "result", value);
            result_set = 1;
            continue;
        }
        double threshold = 0.0;
        char *end = NULL;
        threshold = strtod(tokens->items[i], &end);
        if (end && end != tokens->items[i]) {
            while (*end && isspace((unsigned char)*end)) end++;
        }
        if (end && end != tokens->items[i] && *end == '\0') {
            if (threshold_set) { set_error(error, "anomaly threshold specified more than once"); goto fail; }
            if (!isfinite(threshold) || threshold < 0.0) { set_error(error, "anomaly threshold must be non-negative and finite"); goto fail; }
            cJSON_AddNumberToObject(args, "threshold", threshold);
            threshold_set = 1;
            continue;
        }
        if (!result_set) {
            cJSON_AddStringToObject(args, "result", tokens->items[i]);
            result_set = 1;
            continue;
        }
        set_errorf(error, "anomaly unexpected argument '%s'", tokens->items[i]);
        goto fail;
    }
    return args;
fail:
    cJSON_Delete(args);
    return NULL;
}

static cJSON *build_split_data_args(const token_list *tokens, char **error) {
    /* split-data [ratio] [--seed N] [result_name] */
    (void)error;
    cJSON *args = cJSON_CreateObject();
    size_t idx = 1;
    if (idx < tokens->count) {
        char *end;
        double ratio = strtod(tokens->items[idx], &end);
        if (*end == '\0') {
            cJSON_AddNumberToObject(args, "ratio", ratio);
            idx++;
        }
    }
    while (idx < tokens->count) {
        if (strcmp(tokens->items[idx], "--seed") == 0 && idx + 1 < tokens->count) {
            cJSON_AddNumberToObject(args, "seed", atoi(tokens->items[idx + 1]));
            idx += 2;
        } else {
            cJSON_AddStringToObject(args, "result", tokens->items[idx]);
            idx++;
        }
    }
    return args;
}

static int valid_interpolate_method(const char *s) {
    return strcmp(s, "forward") == 0 || strcmp(s, "backward") == 0 || strcmp(s, "linear") == 0;
}

static cJSON *build_interpolate_args(const token_list *tokens, char **error) {
    /* interpolate column [forward|backward|linear|method=...] [missing=error|null|ignore] [on_type_error=fail|null] */
    if (tokens->count < 2) {
        set_error(error, "interpolate requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    int method_set = 0;
    for (size_t i = 2; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        int opt = add_h14_policy_option(args, tok, "interpolate", error);
        if (opt == TF_ERROR) goto fail;
        if (opt == TF_OK) continue;
        if (strncmp(tok, "method=", 7) == 0) {
            const char *method = tok + 7;
            if (!valid_interpolate_method(method)) {
                set_error(error, "interpolate method must be forward, backward, or linear");
                goto fail;
            }
            if (method_set) { set_error(error, "interpolate method specified more than once"); goto fail; }
            cJSON_AddStringToObject(args, "method", method);
            method_set = 1;
            continue;
        }
        if (valid_interpolate_method(tok)) {
            if (method_set) { set_error(error, "interpolate method specified more than once"); goto fail; }
            cJSON_AddStringToObject(args, "method", tok);
            method_set = 1;
            continue;
        }
        set_errorf(error, "interpolate unexpected argument '%s'", tok);
        goto fail;
    }
    return args;
fail:
    cJSON_Delete(args);
    return NULL;
}

static cJSON *build_normalize_args(const token_list *tokens, char **error) {
    /* normalize col1,col2,... [method|minmax|zscore] [audit audit_limit=N]
     *           [missing=error|null|ignore] [on_type_error=fail|null] */
    if (tokens->count < 2) {
        set_error(error, "normalize requires column names");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *cols = cJSON_CreateArray();
    cJSON_AddItemToObject(args, "columns", cols);

    const char *method = NULL;
    for (size_t i = 1; i < tokens->count; i++) {
        const char *arg = tokens->items[i];
        int opt = add_h14_policy_option(args, arg, "normalize", error);
        if (opt == TF_ERROR) { cJSON_Delete(args); return NULL; }
        if (opt == TF_OK) continue;
        int audit_rc = add_audit_option_arg(args, arg, error, "normalize");
        if (audit_rc < 0) { cJSON_Delete(args); return NULL; }
        if (audit_rc > 0) continue;
        if (strcmp(arg, "minmax") == 0 || strcmp(arg, "zscore") == 0) {
            if (method) { cJSON_Delete(args); set_error(error, "normalize: method specified more than once"); return NULL; }
            method = arg;
        } else if (strncmp(arg, "method=", 7) == 0) {
            const char *value = arg + 7;
            if (strcmp(value, "minmax") != 0 && strcmp(value, "zscore") != 0) {
                cJSON_Delete(args);
                set_error(error, "normalize method must be minmax or zscore");
                return NULL;
            }
            if (method) { cJSON_Delete(args); set_error(error, "normalize: method specified more than once"); return NULL; }
            method = value;
        } else {
            if (add_comma_columns(cols, arg, error, "normalize") != 0) {
                cJSON_Delete(args);
                return NULL;
            }
        }
    }
    if (cJSON_GetArraySize(cols) <= 0) {
        cJSON_Delete(args);
        set_error(error, "normalize requires column names");
        return NULL;
    }
    if (method) cJSON_AddStringToObject(args, "method", method);
    return args;
}

static cJSON *build_acf_args(const token_list *tokens, char **error) {
    /* acf column [lags|lags=N] [missing=error|null|ignore] [on_type_error=fail|null] */
    if (tokens->count < 2) {
        set_error(error, "acf requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    int saw_lags = 0;
    for (size_t i = 2; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        int opt = add_h14_policy_option(args, tok, "acf", error);
        if (opt == TF_ERROR) { cJSON_Delete(args); return NULL; }
        if (opt == TF_OK) continue;
        const char *value = tok;
        if (strncmp(tok, "lags=", 5) == 0) value = tok + 5;
        if (saw_lags) {
            cJSON_Delete(args);
            set_error(error, "acf: lags specified more than once");
            return NULL;
        }
        size_t lags = 0;
        if (dsl_parse_positive_size(value, "acf lags", &lags, error) != TF_OK) {
            cJSON_Delete(args);
            return NULL;
        }
        cJSON_AddNumberToObject(args, "lags", (double)lags);
        saw_lags = 1;
    }
    return args;
}

/* ---- Main parser ---- */


static void dsl_node_clear(tf_ir_node *node) {
    free((char *)node->op);
    if (node->args) cJSON_Delete(node->args);
    tf_schema_free(&node->input_schema);
    tf_schema_free(&node->output_schema);
    memset(node, 0, sizeof(*node));
}

static void rewrite_sort_head_to_bounded_topk(tf_ir_plan *plan) {
    if (!plan || plan->n_nodes < 2) return;

    for (size_t i = 0; i + 1 < plan->n_nodes; i++) {
        tf_ir_node *sort = &plan->nodes[i];
        tf_ir_node *head = &plan->nodes[i + 1];
        if (!sort->op || !head->op) continue;
        if (strcmp(sort->op, "sort") != 0 || strcmp(head->op, "head") != 0) continue;

        cJSON *cols = cJSON_GetObjectItemCaseSensitive(sort->args, "columns");
        cJSON *n_j = cJSON_GetObjectItemCaseSensitive(head->args, "n");
        if (!cJSON_IsArray(cols) || !cJSON_IsNumber(n_j)) continue;
        if (cJSON_GetArraySize(cols) != 1) continue; /* preserve multi-key tie semantics */

        int n = n_j->valueint;
        cJSON *col0 = cJSON_GetArrayItem(cols, 0);
        cJSON *name_j = cJSON_GetObjectItemCaseSensitive(col0, "name");
        cJSON *desc_j = cJSON_GetObjectItemCaseSensitive(col0, "desc");
        if (!cJSON_IsString(name_j)) continue;

        const char *rewrite_op = "top";
        int desc = cJSON_IsTrue(desc_j);
        if (!desc) rewrite_op = "bottom-k";

        cJSON *args = cJSON_CreateObject();
        if (!args) continue;
        if (tf_json_add_number(args, "n", n) != TF_OK ||
            tf_json_add_string(args, "column", name_j->valuestring) != TF_OK ||
            tf_json_add_bool(args, "desc", desc) != TF_OK) {
            cJSON_Delete(args);
            continue;
        }

        tf_ir_node rewritten;
        memset(&rewritten, 0, sizeof(rewritten));
        rewritten.op = strdup(n == 0 ? "head" : rewrite_op);
        if (n == 0) {
            cJSON_Delete(args);
            rewritten.args = cJSON_CreateObject();
            if (!rewritten.args ||
                tf_json_add_number(rewritten.args, "n", 0) != TF_OK) {
                dsl_node_clear(&rewritten);
                continue;
            }
        } else {
            rewritten.args = args;
        }
        rewritten.index = i;
        if (!rewritten.op || !rewritten.args) {
            dsl_node_clear(&rewritten);
            continue;
        }

        size_t old_n = plan->n_nodes;
        dsl_node_clear(&plan->nodes[i]);
        dsl_node_clear(&plan->nodes[i + 1]);
        size_t tail_count = old_n - (i + 2);
        if (tail_count > 0) {
            memmove(&plan->nodes[i + 1], &plan->nodes[i + 2],
                    tail_count * sizeof(tf_ir_node));
        }
        plan->nodes[i] = rewritten;
        plan->n_nodes = old_n - 1;
        memset(&plan->nodes[plan->n_nodes], 0, sizeof(tf_ir_node));
        for (size_t j = i; j < plan->n_nodes; j++) plan->nodes[j].index = j;
        plan->validated = false;
        plan->schema_inferred = false;
    }
}

tf_ir_plan *tf_dsl_parse(const char *text, size_t len, char **error) {
    if (error) *error = NULL;

    if (!text || len == 0) {
        set_error(error, "empty pipeline");
        return NULL;
    }

    /* Split into stages */
    token_list stages;
    if (split_stages(text, len, &stages) != 0 || stages.count == 0) {
        set_error(error, "empty pipeline");
        tl_free(&stages);
        return NULL;
    }

    tf_ir_plan *plan = tf_ir_plan_create();
    if (!plan) {
        set_error(error, "out of memory");
        tl_free(&stages);
        return NULL;
    }

    for (size_t i = 0; i < stages.count; i++) {
        /* Tokenize this stage */
        token_list tokens;
        if (tokenize_stage(stages.items[i], &tokens) != 0 || tokens.count == 0) {
            set_errorf(error, "empty stage at position %zu", i);
            tl_free(&tokens);
            goto fail;
        }

        const char *raw_op = tokens.items[0];
        int is_first = (i == 0);
        int is_last = (i == stages.count - 1);

        /* Resolve op name */
        char *resolved = resolve_codec(raw_op, is_first, is_last);
        const char *op_name = resolved ? resolved : resolve_op_alias(raw_op);

        /* Build args based on op type */
        cJSON *args = NULL;
        const char *node_op_name = op_name;

        /* Check if it's a codec (starts with "codec." or resolved from shorthand) */
        if (strncmp(op_name, "codec.", 6) == 0) {
            args = build_codec_args(&tokens);
            if (!args) { set_oom_error_if_unset(error); free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "filter") == 0) {
            args = build_expr_audit_args(&tokens, error, "filter");
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "select") == 0) {
            args = build_select_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "relocate") == 0) {
            args = build_relocate_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "rename") == 0) {
            args = build_rename_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "head") == 0 || strcmp(op_name, "slice-head") == 0) {
            args = build_head_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "skip") == 0) {
            args = build_skip_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "derive") == 0) {
            args = build_derive_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "source-name") == 0) {
            args = build_source_name_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "across") == 0) {
            args = build_across_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "stats") == 0 || strcmp(op_name, "scan") == 0) {
            args = build_stats_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "unique") == 0) {
            args = build_unique_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "sort") == 0) {
            args = build_sort_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "reorder") == 0) {
            args = build_select_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "dedup") == 0) {
            args = build_unique_args(&tokens, error);
            if (!args) { set_oom_error_if_unset(error); free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "validate") == 0) {
            args = build_expr_audit_args(&tokens, error, "validate");
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "assert") == 0) {
            args = build_assert_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "quarantine") == 0) {
            args = build_quarantine_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "tee") == 0) {
            args = build_tee_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "schema") == 0) {
            if (tokens.count > 1 && strcmp(tokens.items[1], "infer") == 0) {
                node_op_name = "schema-infer";
                args = build_schema_infer_args(&tokens, error, 1);
            } else {
                args = build_schema_args(&tokens, error);
            }
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "schema-infer") == 0) {
            args = build_schema_infer_args(&tokens, error, 0);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "trim") == 0) {
            args = build_unique_args(&tokens, error);
            if (!args) { set_oom_error_if_unset(error); free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "fill-null") == 0) {
            args = build_mapping_args_with_audit(&tokens, error, "fill-null");
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "cast") == 0) {
            args = build_mapping_args_with_audit(&tokens, error, "cast");
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "clip") == 0) {
            args = build_clip_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "replace") == 0) {
            args = build_replace_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "hash") == 0) {
            args = build_unique_args(&tokens, error);
            if (!args) { set_oom_error_if_unset(error); free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "bin") == 0) {
            args = build_bin_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "fill-down") == 0) {
            args = build_unique_args(&tokens, error);
            if (!args) { set_oom_error_if_unset(error); free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "step") == 0) {
            args = build_step_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "window") == 0) {
            args = build_window_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "rolling-sum") == 0 ||
                   strcmp(op_name, "rolling-mean") == 0 ||
                   strcmp(op_name, "rolling-min") == 0 ||
                   strcmp(op_name, "rolling-max") == 0 ||
                   strcmp(op_name, "rolling-any") == 0 ||
                   strcmp(op_name, "rolling-all") == 0) {
            int allow_nulls = (strcmp(op_name, "rolling-any") == 0 || strcmp(op_name, "rolling-all") == 0);
            args = build_rolling_args(&tokens, error, op_name, allow_nulls);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "explode") == 0) {
            args = build_explode_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "split") == 0) {
            args = build_split_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "unpivot") == 0) {
            args = build_unpivot_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "tail") == 0 || strcmp(op_name, "slice-tail") == 0) {
            args = build_head_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "top") == 0 || strcmp(op_name, "top-k") == 0) {
            args = build_top_args_with_default(&tokens, error, 1, op_name);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "bottom-k") == 0) {
            args = build_top_args_with_default(&tokens, error, 0, op_name);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "slice-min") == 0) {
            args = build_slice_rank_args(&tokens, error, 0, op_name);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "slice-max") == 0) {
            args = build_slice_rank_args(&tokens, error, 1, op_name);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "sample") == 0) {
            args = build_sample_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "group-agg") == 0) {
            args = build_group_agg_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "frequency") == 0) {
            args = build_frequency_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "datetime") == 0) {
            args = build_datetime_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "json-extract") == 0) {
            args = build_json_extract_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "json-filter") == 0) {
            args = build_json_filter_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "json-schema") == 0) {
            args = build_json_schema_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "json-flatten") == 0) {
            args = build_json_flatten_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "flatten") == 0) {
            args = build_flatten_args(&tokens, error);
            if (!args) { set_oom_error_if_unset(error); free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "grep") == 0) {
            args = build_grep_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "pivot") == 0) {
            args = build_pivot_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "join") == 0 || strcmp(op_name, "semi-join") == 0 || strcmp(op_name, "anti-join") == 0) {
            args = build_join_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "intersect") == 0 || strcmp(op_name, "setdiff") == 0 ||
                   strcmp(op_name, "intersect-all") == 0 || strcmp(op_name, "intersect_all") == 0 ||
                   strcmp(op_name, "setdiff-all") == 0 || strcmp(op_name, "setdiff_all") == 0 ||
                   strcmp(op_name, "except-all") == 0 || strcmp(op_name, "except_all") == 0 ||
                   strcmp(op_name, "union") == 0 || strcmp(op_name, "union-all") == 0 ||
                   strcmp(op_name, "union_all") == 0) {
            if (strcmp(op_name, "union_all") == 0) {
                op_name = "union-all";
                node_op_name = "union-all";
            } else if (strcmp(op_name, "intersect_all") == 0) {
                op_name = "intersect-all";
                node_op_name = "intersect-all";
            } else if (strcmp(op_name, "setdiff_all") == 0 || strcmp(op_name, "except-all") == 0 || strcmp(op_name, "except_all") == 0) {
                op_name = "setdiff-all";
                node_op_name = "setdiff-all";
            }
            args = build_set_file_args(&tokens, error, op_name);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "stack") == 0) {
            args = build_stack_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "lead") == 0 || strcmp(op_name, "lag") == 0 || strcmp(op_name, "shift") == 0) {
            args = build_shift_like_args(&tokens, error, op_name);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "rowid") == 0) {
            args = build_rowid_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "rleid") == 0) {
            args = build_rleid_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "date-trunc") == 0) {
            args = build_date_trunc_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "onehot") == 0) {
            args = build_onehot_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "label-encode") == 0) {
            args = build_label_encode_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "ewma") == 0) {
            args = build_ewma_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "diff") == 0) {
            args = build_diff_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "anomaly") == 0) {
            args = build_anomaly_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "split-data") == 0) {
            args = build_split_data_args(&tokens, error);
            if (!args) { set_oom_error_if_unset(error); free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "interpolate") == 0) {
            args = build_interpolate_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "normalize") == 0) {
            args = build_normalize_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "acf") == 0) {
            args = build_acf_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else {
            /* Unknown op — pass through with codec-style args, let validation catch it */
            args = build_codec_args(&tokens);
            if (!args) { set_oom_error_if_unset(error); free(resolved); tl_free(&tokens); goto fail; }
        }

        if (tf_ir_plan_add_node(plan, node_op_name, args) != 0) {
            set_error(error, "out of memory adding node");
            cJSON_Delete(args);
            free(resolved);
            tl_free(&tokens);
            goto fail;
        }

        cJSON_Delete(args);
        free(resolved);
        tl_free(&tokens);
    }

    rewrite_sort_head_to_bounded_topk(plan);
    tl_free(&stages);
    return plan;

fail:
    tf_ir_plan_free(plan);
    tl_free(&stages);
    return NULL;
}
