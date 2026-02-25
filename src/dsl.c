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
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <ctype.h>

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

static void set_errorf(char **error, const char *fmt, const char *a) {
    if (error) {
        char buf[256];
        snprintf(buf, sizeof(buf), fmt, a);
        set_error(error, buf);
    }
}

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
        size_t new_cap = tl->cap ? tl->cap * 2 : 8;
        char **new_items = realloc(tl->items, new_cap * sizeof(char *));
        if (!new_items) return -1;
        tl->items = new_items;
        tl->cap = new_cap;
    }
    char *dup = malloc(len + 1);
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
            tl_push(out, text + s, e - s);
            start = i + 1;
        }
    }

    /* Last stage */
    size_t s = start, e = len;
    while (s < e && isspace((unsigned char)text[s])) s++;
    while (e > s && isspace((unsigned char)text[e - 1])) e--;
    if (s < e) tl_push(out, text + s, e - s);

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
            tl_push(out, start, p - start);
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
                size_t total = key_len + 1 + (val_end - val_start);
                char *tok = malloc(total + 1);
                if (tok) {
                    memcpy(tok, start, key_len);
                    tok[key_len] = '=';
                    memcpy(tok + key_len + 1, val_start, val_end - val_start);
                    tok[total] = '\0';
                    if (out->count >= out->cap) {
                        size_t new_cap = out->cap ? out->cap * 2 : 8;
                        char **new_items = realloc(out->items, new_cap * sizeof(char *));
                        if (new_items) { out->items = new_items; out->cap = new_cap; }
                    }
                    out->items[out->count++] = tok;
                }
                p = scan;
            } else {
                while (*p && !isspace((unsigned char)*p) && *p != '"') p++;
                size_t len = p - start;

                /* Split comma-separated tokens (for "name,age" → "name", "age") */
                /* But not if it contains '=' (key=value pair) */
                if (memchr(start, '=', len) == NULL && memchr(start, ',', len) != NULL) {
                    const char *cs = start;
                    while (cs < start + len) {
                        const char *comma = memchr(cs, ',', (start + len) - cs);
                        size_t tok_len = comma ? (size_t)(comma - cs) : (size_t)((start + len) - cs);
                        if (tok_len > 0) tl_push(out, cs, tok_len);
                        cs += tok_len + 1;
                    }
                } else {
                    tl_push(out, start, len);
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
    for (size_t i = 1; i < tokens->count; i++) {
        char *eq = strchr(tokens->items[i], '=');
        if (eq) {
            *eq = '\0';
            const char *key = tokens->items[i];
            const char *val = eq + 1;
            /* Try to detect booleans and ints */
            if (strcmp(val, "true") == 0 || strcmp(val, "false") == 0) {
                cJSON_AddBoolToObject(args, key, strcmp(val, "true") == 0);
            } else {
                /* Check if integer */
                char *end;
                long num = strtol(val, &end, 10);
                if (*end == '\0' && end != val) {
                    cJSON_AddNumberToObject(args, key, num);
                } else {
                    cJSON_AddStringToObject(args, key, val);
                }
            }
            *eq = '='; /* restore */
        }
    }
    return args;
}

static cJSON *build_filter_args(const token_list *tokens, char **error) {
    /* filter "expr" — tokens[1] should be the expression */
    if (tokens->count < 2) {
        set_error(error, "filter requires an expression argument");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "expr", tokens->items[1]);
    return args;
}

static cJSON *build_select_args(const token_list *tokens, char **error) {
    /* select name,age or select name age — tokens[1..n] are column names */
    if (tokens->count < 2) {
        set_error(error, "select requires at least one column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *cols = cJSON_CreateArray();
    for (size_t i = 1; i < tokens->count; i++) {
        cJSON_AddItemToArray(cols, cJSON_CreateString(tokens->items[i]));
    }
    cJSON_AddItemToObject(args, "columns", cols);
    return args;
}

static cJSON *build_rename_args(const token_list *tokens, char **error) {
    /* rename old=new,old2=new2 or rename old=new old2=new2 */
    if (tokens->count < 2) {
        set_error(error, "rename requires at least one old=new mapping");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *mapping = cJSON_CreateObject();

    for (size_t i = 1; i < tokens->count; i++) {
        /* Each token might be "old=new" or comma-separated "old=new,old2=new2" */
        char *tok = tokens->items[i];
        /* Split on commas within this token */
        char *saveptr = NULL;
        char *copy = strdup(tok);
        char *part = strtok_r(copy, ",", &saveptr);
        while (part) {
            char *eq = strchr(part, '=');
            if (!eq) {
                free(copy);
                cJSON_Delete(args);
                set_errorf(error, "invalid rename mapping: '%s' (expected old=new)", part);
                return NULL;
            }
            *eq = '\0';
            cJSON_AddStringToObject(mapping, part, eq + 1);
            part = strtok_r(NULL, ",", &saveptr);
        }
        free(copy);
    }

    cJSON_AddItemToObject(args, "mapping", mapping);
    return args;
}

static cJSON *build_head_args(const token_list *tokens, char **error) {
    /* head N */
    if (tokens->count < 2) {
        set_error(error, "head requires a count argument");
        return NULL;
    }
    char *end;
    long n = strtol(tokens->items[1], &end, 10);
    if (*end != '\0' || n <= 0) {
        set_errorf(error, "head: invalid count '%s'", tokens->items[1]);
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddNumberToObject(args, "n", n);
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
    if (*end != '\0' || n <= 0) {
        set_errorf(error, "skip: invalid count '%s'", tokens->items[1]);
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddNumberToObject(args, "n", n);
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

    for (size_t i = 1; i < tokens->count; i++) {
        char *eq = strchr(tokens->items[i], '=');
        if (!eq) {
            cJSON_Delete(args);
            set_errorf(error, "derive: invalid mapping '%s' (expected name=expr)", tokens->items[i]);
            return NULL;
        }
        *eq = '\0';
        cJSON *col = cJSON_CreateObject();
        cJSON_AddStringToObject(col, "name", tokens->items[i]);
        cJSON_AddStringToObject(col, "expr", eq + 1);
        cJSON_AddItemToArray(columns, col);
        *eq = '='; /* restore */
    }

    cJSON_AddItemToObject(args, "columns", columns);
    return args;
}

static cJSON *build_stats_args(const token_list *tokens, char **error) {
    (void)error;
    /* stats [count,sum,avg,min,max] */
    cJSON *args = cJSON_CreateObject();
    if (tokens->count >= 2) {
        /* Parse comma-separated stat names */
        cJSON *stats = cJSON_CreateArray();
        for (size_t i = 1; i < tokens->count; i++) {
            cJSON_AddItemToArray(stats, cJSON_CreateString(tokens->items[i]));
        }
        cJSON_AddItemToObject(args, "stats", stats);
    }
    return args;
}

static cJSON *build_unique_args(const token_list *tokens, char **error) {
    (void)error;
    /* unique [col1,col2] */
    cJSON *args = cJSON_CreateObject();
    if (tokens->count >= 2) {
        cJSON *cols = cJSON_CreateArray();
        for (size_t i = 1; i < tokens->count; i++) {
            cJSON_AddItemToArray(cols, cJSON_CreateString(tokens->items[i]));
        }
        cJSON_AddItemToObject(args, "columns", cols);
    }
    return args;
}

static cJSON *build_sort_args(const token_list *tokens, char **error) {
    /* sort col1,-col2,col3 */
    if (tokens->count < 2) {
        set_error(error, "sort requires at least one column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *columns = cJSON_CreateArray();

    for (size_t i = 1; i < tokens->count; i++) {
        const char *tok = tokens->items[i];
        int desc = 0;
        if (tok[0] == '-') {
            desc = 1;
            tok++;
        }
        cJSON *col = cJSON_CreateObject();
        cJSON_AddStringToObject(col, "name", tok);
        cJSON_AddBoolToObject(col, "desc", desc);
        cJSON_AddItemToArray(columns, col);
    }

    cJSON_AddItemToObject(args, "columns", columns);
    return args;
}

static cJSON *build_top_args(const token_list *tokens, char **error) {
    /* top 10 score or top 10 -score */
    if (tokens->count < 3) {
        set_error(error, "top requires N and column arguments");
        return NULL;
    }
    char *end;
    long n = strtol(tokens->items[1], &end, 10);
    if (*end != '\0' || n <= 0) {
        set_errorf(error, "top: invalid count '%s'", tokens->items[1]);
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddNumberToObject(args, "n", n);
    const char *col = tokens->items[2];
    int desc = 1; /* default: highest first */
    if (col[0] == '-') { col++; desc = 1; }
    else if (col[0] == '+') { col++; desc = 0; }
    cJSON_AddStringToObject(args, "column", col);
    cJSON_AddBoolToObject(args, "desc", desc);
    return args;
}

static cJSON *build_replace_args(const token_list *tokens, char **error) {
    /* replace [--regex] column pattern replacement */
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
    return args;
}

static cJSON *build_clip_args(const token_list *tokens, char **error) {
    /* clip column min=0 max=100 */
    if (tokens->count < 2) {
        set_error(error, "clip requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    for (size_t i = 2; i < tokens->count; i++) {
        char *eq = strchr(tokens->items[i], '=');
        if (eq) {
            *eq = '\0';
            double v = strtod(eq + 1, NULL);
            cJSON_AddNumberToObject(args, tokens->items[i], v);
            *eq = '=';
        }
    }
    return args;
}

static cJSON *build_bin_args(const token_list *tokens, char **error) {
    /* bin column 10,20,30 */
    if (tokens->count < 3) {
        set_error(error, "bin requires column and boundaries");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    cJSON *boundaries = cJSON_CreateArray();
    for (size_t i = 2; i < tokens->count; i++) {
        cJSON_AddItemToArray(boundaries, cJSON_CreateNumber(strtod(tokens->items[i], NULL)));
    }
    cJSON_AddItemToObject(args, "boundaries", boundaries);
    return args;
}

static cJSON *build_datetime_args(const token_list *tokens, char **error) {
    /* datetime date_col year,month,day */
    if (tokens->count < 2) {
        set_error(error, "datetime requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    if (tokens->count >= 3) {
        cJSON *extract = cJSON_CreateArray();
        for (size_t i = 2; i < tokens->count; i++)
            cJSON_AddItemToArray(extract, cJSON_CreateString(tokens->items[i]));
        cJSON_AddItemToObject(args, "extract", extract);
    }
    return args;
}

static cJSON *build_explode_args(const token_list *tokens, char **error) {
    /* explode column [delimiter] */
    if (tokens->count < 2) {
        set_error(error, "explode requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    if (tokens->count >= 3)
        cJSON_AddStringToObject(args, "delimiter", tokens->items[2]);
    return args;
}

static cJSON *build_split_args(const token_list *tokens, char **error) {
    /* split column delimiter name1,name2 */
    if (tokens->count < 4) {
        set_error(error, "split requires column, delimiter, and names");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    cJSON_AddStringToObject(args, "delimiter", tokens->items[2]);
    cJSON *names = cJSON_CreateArray();
    for (size_t i = 3; i < tokens->count; i++)
        cJSON_AddItemToArray(names, cJSON_CreateString(tokens->items[i]));
    cJSON_AddItemToObject(args, "names", names);
    return args;
}

static cJSON *build_unpivot_args(const token_list *tokens, char **error) {
    /* unpivot col1,col2,col3 */
    if (tokens->count < 2) {
        set_error(error, "unpivot requires at least one column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON *cols = cJSON_CreateArray();
    for (size_t i = 1; i < tokens->count; i++)
        cJSON_AddItemToArray(cols, cJSON_CreateString(tokens->items[i]));
    cJSON_AddItemToObject(args, "columns", cols);
    return args;
}

static cJSON *build_group_agg_args(const token_list *tokens, char **error) {
    /* group-agg group_col1,group_col2 col:func[:name] ... */
    if (tokens->count < 3) {
        set_error(error, "group-agg requires group columns and at least one aggregation");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();

    /* First arg: group columns (comma-separated already split by tokenizer) */
    cJSON *group_by = cJSON_CreateArray();
    /* The first token after op name contains group columns */
    cJSON_AddItemToArray(group_by, cJSON_CreateString(tokens->items[1]));
    cJSON_AddItemToObject(args, "group_by", group_by);

    /* Remaining args: col:func or col:func:name */
    cJSON *aggs = cJSON_CreateArray();
    for (size_t i = 2; i < tokens->count; i++) {
        char *tok = strdup(tokens->items[i]);
        char *colon1 = strchr(tok, ':');
        if (!colon1) { free(tok); continue; }
        *colon1 = '\0';
        char *func = colon1 + 1;
        char *colon2 = strchr(func, ':');
        char *name = NULL;
        if (colon2) { *colon2 = '\0'; name = colon2 + 1; }

        cJSON *agg = cJSON_CreateObject();
        cJSON_AddStringToObject(agg, "column", tok);
        cJSON_AddStringToObject(agg, "func", func);
        if (name) cJSON_AddStringToObject(agg, "name", name);
        cJSON_AddItemToArray(aggs, agg);
        free(tok);
    }
    cJSON_AddItemToObject(args, "aggs", aggs);
    return args;
}

static cJSON *build_frequency_args(const token_list *tokens, char **error) {
    (void)error;
    /* frequency [col1,col2] */
    cJSON *args = cJSON_CreateObject();
    if (tokens->count >= 2) {
        cJSON *cols = cJSON_CreateArray();
        for (size_t i = 1; i < tokens->count; i++)
            cJSON_AddItemToArray(cols, cJSON_CreateString(tokens->items[i]));
        cJSON_AddItemToObject(args, "columns", cols);
    }
    return args;
}

static cJSON *build_window_args(const token_list *tokens, char **error) {
    /* window column size func [result_name] */
    if (tokens->count < 4) {
        set_error(error, "window requires column, size, and func");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    cJSON_AddNumberToObject(args, "size", strtol(tokens->items[2], NULL, 10));
    cJSON_AddStringToObject(args, "func", tokens->items[3]);
    if (tokens->count >= 5)
        cJSON_AddStringToObject(args, "result", tokens->items[4]);
    return args;
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
    return args;
}

static cJSON *build_pivot_args(const token_list *tokens, char **error) {
    /* pivot name_col value_col [agg] */
    if (tokens->count < 3) {
        set_error(error, "pivot requires name_column and value_column");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "name_column", tokens->items[1]);
    cJSON_AddStringToObject(args, "value_column", tokens->items[2]);
    if (tokens->count >= 4)
        cJSON_AddStringToObject(args, "agg", tokens->items[3]);
    return args;
}

static cJSON *build_join_args(const token_list *tokens, char **error) {
    /* join lookup.csv on id [--left]
     * join lookup.csv on id=lookup_id [--left] */
    if (tokens->count < 4) {
        set_error(error, "join requires file, 'on', and column");
        return NULL;
    }
    if (strcmp(tokens->items[2], "on") != 0) {
        set_error(error, "join: expected 'on' keyword");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "file", tokens->items[1]);
    cJSON_AddStringToObject(args, "on", tokens->items[3]);
    for (size_t i = 4; i < tokens->count; i++) {
        if (strcmp(tokens->items[i], "--left") == 0)
            cJSON_AddStringToObject(args, "how", "left");
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

static cJSON *build_lead_args(const token_list *tokens, char **error) {
    /* lead column [offset] [result_name] */
    if (tokens->count < 2) {
        set_error(error, "lead requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    if (tokens->count >= 3) {
        /* Check if next arg is numeric (offset) or a name (result) */
        char *end;
        long off = strtol(tokens->items[2], &end, 10);
        if (*end == '\0') {
            cJSON_AddNumberToObject(args, "offset", off);
            if (tokens->count >= 4)
                cJSON_AddStringToObject(args, "result", tokens->items[3]);
        } else {
            cJSON_AddStringToObject(args, "result", tokens->items[2]);
        }
    }
    return args;
}

static cJSON *build_date_trunc_args(const token_list *tokens, char **error) {
    /* date-trunc column granularity [result_name] */
    if (tokens->count < 3) {
        set_error(error, "date-trunc requires column and granularity");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    cJSON_AddStringToObject(args, "trunc", tokens->items[2]);
    if (tokens->count >= 4)
        cJSON_AddStringToObject(args, "result", tokens->items[3]);
    return args;
}

static cJSON *build_onehot_args(const token_list *tokens, char **error) {
    /* onehot column [--drop] */
    if (tokens->count < 2) {
        set_error(error, "onehot requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    for (size_t i = 2; i < tokens->count; i++) {
        if (strcmp(tokens->items[i], "--drop") == 0)
            cJSON_AddBoolToObject(args, "drop", 1);
    }
    return args;
}

static cJSON *build_label_encode_args(const token_list *tokens, char **error) {
    /* label-encode column [result_name] */
    if (tokens->count < 2) {
        set_error(error, "label-encode requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    if (tokens->count >= 3)
        cJSON_AddStringToObject(args, "result", tokens->items[2]);
    return args;
}

static cJSON *build_ewma_args(const token_list *tokens, char **error) {
    /* ewma column alpha [result_name] */
    if (tokens->count < 3) {
        set_error(error, "ewma requires column and alpha");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    cJSON_AddNumberToObject(args, "alpha", atof(tokens->items[2]));
    if (tokens->count >= 4)
        cJSON_AddStringToObject(args, "result", tokens->items[3]);
    return args;
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
    /* anomaly column [threshold] [result_name] */
    if (tokens->count < 2) {
        set_error(error, "anomaly requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    if (tokens->count >= 3) {
        char *end;
        double thresh = strtod(tokens->items[2], &end);
        if (*end == '\0') {
            cJSON_AddNumberToObject(args, "threshold", thresh);
            if (tokens->count >= 4)
                cJSON_AddStringToObject(args, "result", tokens->items[3]);
        } else {
            cJSON_AddStringToObject(args, "result", tokens->items[2]);
        }
    }
    return args;
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

static cJSON *build_interpolate_args(const token_list *tokens, char **error) {
    /* interpolate column [method] */
    if (tokens->count < 2) {
        set_error(error, "interpolate requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    if (tokens->count >= 3)
        cJSON_AddStringToObject(args, "method", tokens->items[2]);
    return args;
}

static cJSON *build_normalize_args(const token_list *tokens, char **error) {
    /* normalize col1,col2,... [method] */
    if (tokens->count < 2) {
        set_error(error, "normalize requires column names");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    /* Parse comma-separated columns */
    cJSON *cols = cJSON_CreateArray();
    char *dup = strdup(tokens->items[1]);
    char *tok = strtok(dup, ",");
    while (tok) {
        cJSON_AddItemToArray(cols, cJSON_CreateString(tok));
        tok = strtok(NULL, ",");
    }
    free(dup);
    cJSON_AddItemToObject(args, "columns", cols);
    if (tokens->count >= 3)
        cJSON_AddStringToObject(args, "method", tokens->items[2]);
    return args;
}

static cJSON *build_acf_args(const token_list *tokens, char **error) {
    /* acf column [lags] */
    if (tokens->count < 2) {
        set_error(error, "acf requires a column name");
        return NULL;
    }
    cJSON *args = cJSON_CreateObject();
    cJSON_AddStringToObject(args, "column", tokens->items[1]);
    if (tokens->count >= 3)
        cJSON_AddNumberToObject(args, "lags", atoi(tokens->items[2]));
    return args;
}

/* ---- Main parser ---- */

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
            set_errorf(error, "empty stage at position %zu", "");
            tl_free(&tokens);
            goto fail;
        }

        const char *raw_op = tokens.items[0];
        int is_first = (i == 0);
        int is_last = (i == stages.count - 1);

        /* Resolve op name */
        char *resolved = resolve_codec(raw_op, is_first, is_last);
        const char *op_name = resolved ? resolved : raw_op;

        /* Build args based on op type */
        cJSON *args = NULL;

        /* Check if it's a codec (starts with "codec." or resolved from shorthand) */
        if (strncmp(op_name, "codec.", 6) == 0) {
            args = build_codec_args(&tokens);
        } else if (strcmp(op_name, "filter") == 0) {
            args = build_filter_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "select") == 0) {
            args = build_select_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "rename") == 0) {
            args = build_rename_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "head") == 0) {
            args = build_head_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "skip") == 0) {
            args = build_skip_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "derive") == 0) {
            args = build_derive_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "stats") == 0) {
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
        } else if (strcmp(op_name, "validate") == 0) {
            args = build_filter_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "trim") == 0) {
            args = build_unique_args(&tokens, error);
        } else if (strcmp(op_name, "fill-null") == 0) {
            args = build_rename_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "cast") == 0) {
            args = build_rename_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "clip") == 0) {
            args = build_clip_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "replace") == 0) {
            args = build_replace_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "hash") == 0) {
            args = build_unique_args(&tokens, error);
        } else if (strcmp(op_name, "bin") == 0) {
            args = build_bin_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "fill-down") == 0) {
            args = build_unique_args(&tokens, error);
        } else if (strcmp(op_name, "step") == 0) {
            args = build_step_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "window") == 0) {
            args = build_window_args(&tokens, error);
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
        } else if (strcmp(op_name, "tail") == 0) {
            args = build_head_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "top") == 0) {
            args = build_top_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "sample") == 0) {
            args = build_head_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "group-agg") == 0) {
            args = build_group_agg_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "frequency") == 0) {
            args = build_frequency_args(&tokens, error);
        } else if (strcmp(op_name, "datetime") == 0) {
            args = build_datetime_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "flatten") == 0) {
            args = build_flatten_args(&tokens, error);
        } else if (strcmp(op_name, "grep") == 0) {
            args = build_grep_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "pivot") == 0) {
            args = build_pivot_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "join") == 0) {
            args = build_join_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "stack") == 0) {
            args = build_stack_args(&tokens, error);
            if (!args) { free(resolved); tl_free(&tokens); goto fail; }
        } else if (strcmp(op_name, "lead") == 0) {
            args = build_lead_args(&tokens, error);
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
        }

        if (tf_ir_plan_add_node(plan, op_name, args) != 0) {
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

    tl_free(&stages);
    return plan;

fail:
    tf_ir_plan_free(plan);
    tl_free(&stages);
    return NULL;
}
