/*
 * op_json_filter.c -- Filter rows by a scalar predicate over per-row JSON text.
 *
 * Config: {"column":"_line", "path":"/user/age", "op":"ge", "value":"30", "type":"float"}
 */

#include "internal.h"
#include "cJSON.h"
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

typedef enum {
    JSON_FILTER_EXISTS,
    JSON_FILTER_MISSING,
    JSON_FILTER_TRUTHY,
    JSON_FILTER_FALSEY,
    JSON_FILTER_EQ,
    JSON_FILTER_NE,
    JSON_FILTER_GT,
    JSON_FILTER_GE,
    JSON_FILTER_LT,
    JSON_FILTER_LE,
    JSON_FILTER_CONTAINS,
    JSON_FILTER_STARTS_WITH,
    JSON_FILTER_ENDS_WITH,
} json_filter_op;

typedef enum {
    JSON_FILTER_TYPE_AUTO,
    JSON_FILTER_TYPE_STRING,
    JSON_FILTER_TYPE_INT,
    JSON_FILTER_TYPE_FLOAT,
    JSON_FILTER_TYPE_BOOL,
} json_filter_type;

typedef struct {
    char             *column;
    char             *path;
    char             *value;
    json_filter_op    op;
    json_filter_type  type;
} json_filter_state;

static int str_eq_ci(const char *a, const char *b) {
    while (*a && *b) {
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) return 0;
        a++; b++;
    }
    return *a == '\0' && *b == '\0';
}

static int parse_strict_double(const char *s, double *out) {
    if (!s) return 0;
    errno = 0;
    char *end = NULL;
    double v = strtod(s, &end);
    if (errno != 0 || end == s) return 0;
    while (*end && isspace((unsigned char)*end)) end++;
    if (*end != '\0') return 0;
    *out = v;
    return 1;
}

static int parse_strict_int64(const char *s, int64_t *out) {
    if (!s) return 0;
    errno = 0;
    char *end = NULL;
    long long v = strtoll(s, &end, 10);
    if (errno != 0 || end == s) return 0;
    while (*end && isspace((unsigned char)*end)) end++;
    if (*end != '\0') return 0;
    *out = (int64_t)v;
    return 1;
}

static int parse_bool_string(const char *s, int *out) {
    if (!s) return 0;
    if (str_eq_ci(s, "true") || strcmp(s, "1") == 0) { *out = 1; return 1; }
    if (str_eq_ci(s, "false") || strcmp(s, "0") == 0) { *out = 0; return 1; }
    return 0;
}

static int json_value_to_double(const cJSON *val, double *out) {
    if (!val || cJSON_IsNull(val)) return 0;
    if (cJSON_IsNumber(val)) { *out = val->valuedouble; return 1; }
    if (cJSON_IsBool(val)) { *out = cJSON_IsTrue(val) ? 1.0 : 0.0; return 1; }
    if (cJSON_IsString(val)) return parse_strict_double(val->valuestring, out);
    return 0;
}

static int json_value_to_int64(const cJSON *val, int64_t *out) {
    if (!val || cJSON_IsNull(val)) return 0;
    if (cJSON_IsNumber(val)) { *out = (int64_t)val->valuedouble; return 1; }
    if (cJSON_IsBool(val)) { *out = cJSON_IsTrue(val) ? 1 : 0; return 1; }
    if (cJSON_IsString(val)) return parse_strict_int64(val->valuestring, out);
    return 0;
}

static int json_value_to_bool(const cJSON *val, int *out) {
    if (!val || cJSON_IsNull(val)) return 0;
    if (cJSON_IsBool(val)) { *out = cJSON_IsTrue(val) ? 1 : 0; return 1; }
    if (cJSON_IsNumber(val)) { *out = val->valuedouble != 0.0; return 1; }
    if (cJSON_IsString(val)) return parse_bool_string(val->valuestring, out);
    return 0;
}

static const char *json_value_to_string(const cJSON *val, char **owned) {
    *owned = NULL;
    if (!val || cJSON_IsNull(val)) return NULL;
    if (cJSON_IsString(val)) return val->valuestring ? val->valuestring : "";
    *owned = cJSON_PrintUnformatted((cJSON *)val);
    return *owned;
}

static int value_truthy(const cJSON *val) {
    if (!val || cJSON_IsNull(val) || cJSON_IsFalse(val)) return 0;
    if (cJSON_IsTrue(val)) return 1;
    if (cJSON_IsNumber(val)) return val->valuedouble != 0.0;
    if (cJSON_IsString(val)) return val->valuestring && val->valuestring[0] != '\0';
    if (cJSON_IsArray(val) || cJSON_IsObject(val)) return cJSON_GetArraySize((cJSON *)val) > 0;
    return 0;
}

static int string_contains(const char *haystack, const char *needle) {
    return haystack && needle && strstr(haystack, needle) != NULL;
}

static int string_starts_with(const char *s, const char *prefix) {
    if (!s || !prefix) return 0;
    size_t n = strlen(prefix);
    return strncmp(s, prefix, n) == 0;
}

static int string_ends_with(const char *s, const char *suffix) {
    if (!s || !suffix) return 0;
    size_t sl = strlen(s), nl = strlen(suffix);
    return sl >= nl && strcmp(s + sl - nl, suffix) == 0;
}

static int compare_strings(const char *a, const char *b, json_filter_op op) {
    int cmp = strcmp(a ? a : "", b ? b : "");
    switch (op) {
        case JSON_FILTER_EQ: return cmp == 0;
        case JSON_FILTER_NE: return cmp != 0;
        case JSON_FILTER_CONTAINS: return string_contains(a, b);
        case JSON_FILTER_STARTS_WITH: return string_starts_with(a, b);
        case JSON_FILTER_ENDS_WITH: return string_ends_with(a, b);
        default: return 0;
    }
}

static int compare_numeric(double a, double b, json_filter_op op) {
    switch (op) {
        case JSON_FILTER_EQ: return a == b;
        case JSON_FILTER_NE: return a != b;
        case JSON_FILTER_GT: return a > b;
        case JSON_FILTER_GE: return a >= b;
        case JSON_FILTER_LT: return a < b;
        case JSON_FILTER_LE: return a <= b;
        default: return 0;
    }
}

static int compare_bool(int a, int b, json_filter_op op) {
    switch (op) {
        case JSON_FILTER_EQ: return a == b;
        case JSON_FILTER_NE: return a != b;
        default: return 0;
    }
}

static int eval_filter(const json_filter_state *st, const cJSON *val) {
    switch (st->op) {
        case JSON_FILTER_EXISTS: return val && !cJSON_IsNull(val);
        case JSON_FILTER_MISSING: return !val || cJSON_IsNull(val);
        case JSON_FILTER_TRUTHY: return value_truthy(val);
        case JSON_FILTER_FALSEY: return !value_truthy(val);
        default: break;
    }

    if (!st->value) return 0;

    if (st->type == JSON_FILTER_TYPE_INT) {
        int64_t a = 0, b = 0;
        if (!json_value_to_int64(val, &a) || !parse_strict_int64(st->value, &b)) return 0;
        return compare_numeric((double)a, (double)b, st->op);
    }
    if (st->type == JSON_FILTER_TYPE_FLOAT) {
        double a = 0.0, b = 0.0;
        if (!json_value_to_double(val, &a) || !parse_strict_double(st->value, &b)) return 0;
        return compare_numeric(a, b, st->op);
    }
    if (st->type == JSON_FILTER_TYPE_BOOL) {
        int a = 0, b = 0;
        if (!json_value_to_bool(val, &a) || !parse_bool_string(st->value, &b)) return 0;
        return compare_bool(a, b, st->op);
    }
    if (st->type == JSON_FILTER_TYPE_STRING) {
        char *owned = NULL;
        const char *a = json_value_to_string(val, &owned);
        int ok = a ? compare_strings(a, st->value, st->op) : 0;
        free(owned);
        return ok;
    }

    if (st->op == JSON_FILTER_GT || st->op == JSON_FILTER_GE ||
        st->op == JSON_FILTER_LT || st->op == JSON_FILTER_LE) {
        double a = 0.0, b = 0.0;
        if (!json_value_to_double(val, &a) || !parse_strict_double(st->value, &b)) return 0;
        return compare_numeric(a, b, st->op);
    }

    if (cJSON_IsBool(val)) {
        int a = 0, b = 0;
        if (json_value_to_bool(val, &a) && parse_bool_string(st->value, &b))
            return compare_bool(a, b, st->op);
    }
    if (cJSON_IsNumber(val)) {
        double b = 0.0;
        if (parse_strict_double(st->value, &b)) return compare_numeric(val->valuedouble, b, st->op);
    }

    char *owned = NULL;
    const char *a = json_value_to_string(val, &owned);
    int ok = a ? compare_strings(a, st->value, st->op) : 0;
    free(owned);
    return ok;
}

static int json_filter_process(tf_step *self, tf_batch *in, tf_batch **out,
                               tf_side_channels *side) {
    (void)side;
    json_filter_state *st = self->state;
    *out = NULL;

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++) {
        if (tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    int ci = tf_batch_col_index(in, st->column);
    int can_parse = ci >= 0 && in->col_types[(size_t)ci] == TF_TYPE_STRING;
    size_t out_row = 0;

    for (size_t r = 0; r < in->n_rows; r++) {
        const cJSON *val = NULL;
        cJSON *root = NULL;
        if (can_parse && !tf_batch_is_null(in, r, (size_t)ci)) {
            const char *json = tf_batch_get_string(in, r, (size_t)ci);
            if (json && json[0] != '\0') {
                root = cJSON_Parse(json);
                if (root) val = tf_json_path_resolve(root, st->path);
            }
        }
        int keep = eval_filter(st, val);
        if (root) cJSON_Delete(root);
        if (!keep) continue;
        if (tf_batch_copy_row(ob, out_row, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        ob->n_rows = ++out_row;
    }

    if (ob->n_rows > 0) {
        *out = ob;
    } else {
        tf_batch_free(ob);
    }
    return TF_OK;
}

static int json_filter_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void json_filter_state_free(json_filter_state *st) {
    if (!st) return;
    free(st->column);
    free(st->path);
    free(st->value);
    free(st);
}

static void json_filter_destroy(tf_step *self) {
    json_filter_state_free(self ? self->state : NULL);
    free(self);
}

static int parse_filter_op(const char *s, json_filter_op *out) {
    if (!s || strcmp(s, "exists") == 0 || strcmp(s, "present") == 0) { *out = JSON_FILTER_EXISTS; return 1; }
    if (strcmp(s, "missing") == 0 || strcmp(s, "absent") == 0) { *out = JSON_FILTER_MISSING; return 1; }
    if (strcmp(s, "truthy") == 0 || strcmp(s, "true") == 0) { *out = JSON_FILTER_TRUTHY; return 1; }
    if (strcmp(s, "falsey") == 0 || strcmp(s, "falsy") == 0 || strcmp(s, "false") == 0) { *out = JSON_FILTER_FALSEY; return 1; }
    if (strcmp(s, "eq") == 0 || strcmp(s, "==") == 0 || strcmp(s, "=") == 0) { *out = JSON_FILTER_EQ; return 1; }
    if (strcmp(s, "ne") == 0 || strcmp(s, "!=") == 0) { *out = JSON_FILTER_NE; return 1; }
    if (strcmp(s, "gt") == 0 || strcmp(s, ">") == 0) { *out = JSON_FILTER_GT; return 1; }
    if (strcmp(s, "ge") == 0 || strcmp(s, ">=") == 0) { *out = JSON_FILTER_GE; return 1; }
    if (strcmp(s, "lt") == 0 || strcmp(s, "<") == 0) { *out = JSON_FILTER_LT; return 1; }
    if (strcmp(s, "le") == 0 || strcmp(s, "<=") == 0) { *out = JSON_FILTER_LE; return 1; }
    if (strcmp(s, "contains") == 0) { *out = JSON_FILTER_CONTAINS; return 1; }
    if (strcmp(s, "starts-with") == 0 || strcmp(s, "starts_with") == 0) { *out = JSON_FILTER_STARTS_WITH; return 1; }
    if (strcmp(s, "ends-with") == 0 || strcmp(s, "ends_with") == 0) { *out = JSON_FILTER_ENDS_WITH; return 1; }
    return 0;
}

static int op_requires_value(json_filter_op op) {
    return !(op == JSON_FILTER_EXISTS || op == JSON_FILTER_MISSING ||
             op == JSON_FILTER_TRUTHY || op == JSON_FILTER_FALSEY);
}

static int parse_filter_type(const char *s, json_filter_type *out) {
    if (!s || strcmp(s, "auto") == 0) { *out = JSON_FILTER_TYPE_AUTO; return 1; }
    if (strcmp(s, "string") == 0 || strcmp(s, "str") == 0 || strcmp(s, "json") == 0) { *out = JSON_FILTER_TYPE_STRING; return 1; }
    if (strcmp(s, "int") == 0 || strcmp(s, "int64") == 0) { *out = JSON_FILTER_TYPE_INT; return 1; }
    if (strcmp(s, "float") == 0 || strcmp(s, "float64") == 0 || strcmp(s, "number") == 0) { *out = JSON_FILTER_TYPE_FLOAT; return 1; }
    if (strcmp(s, "bool") == 0 || strcmp(s, "boolean") == 0) { *out = JSON_FILTER_TYPE_BOOL; return 1; }
    return 0;
}

static char *value_arg_to_string(const cJSON *value_j) {
    if (!value_j) return NULL;
    if (cJSON_IsString(value_j)) return strdup(value_j->valuestring ? value_j->valuestring : "");
    char *printed = cJSON_PrintUnformatted((cJSON *)value_j);
    if (!printed) return NULL;
    char *out = strdup(printed);
    free(printed);
    return out;
}

tf_step *tf_json_filter_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *path_j = cJSON_GetObjectItemCaseSensitive(args, "path");
    if (!cJSON_IsString(path_j)) return NULL;

    cJSON *column_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    const char *column = cJSON_IsString(column_j) && column_j->valuestring[0] != '\0'
        ? column_j->valuestring : "_line";

    json_filter_op op = JSON_FILTER_EXISTS;
    cJSON *op_j = cJSON_GetObjectItemCaseSensitive(args, "op");
    if (op_j && (!cJSON_IsString(op_j) || !parse_filter_op(op_j->valuestring, &op))) return NULL;

    json_filter_type type = JSON_FILTER_TYPE_AUTO;
    cJSON *type_j = cJSON_GetObjectItemCaseSensitive(args, "type");
    if (type_j && (!cJSON_IsString(type_j) || !parse_filter_type(type_j->valuestring, &type))) return NULL;

    cJSON *value_j = cJSON_GetObjectItemCaseSensitive(args, "value");
    char *value = value_arg_to_string(value_j);
    if (op_requires_value(op) && !value) return NULL;

    json_filter_state *st = calloc(1, sizeof(json_filter_state));
    if (!st) { free(value); return NULL; }
    st->column = strdup(column);
    st->path = strdup(path_j->valuestring);
    st->value = value;
    st->op = op;
    st->type = type;
    if (!st->column || !st->path) {
        json_filter_state_free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { json_filter_state_free(st); return NULL; }
    step->process = json_filter_process;
    step->flush = json_filter_flush;
    step->destroy = json_filter_destroy;
    step->state = st;
    return step;
}
