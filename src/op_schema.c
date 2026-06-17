/*
 * op_schema.c -- Row-local table schema/data-quality contracts.
 *
 * Config:
 * {
 *   "columns": {"name":"string", "age":{"type":"int","nullable":false}},
 *   "required": ["name"], "non_null": ["age"],
 *   "values": {"city":["NY","LA"]}, "min": {"age":0}, "max": {"age":120},
 *   "regex": {"code":"^[A-Z]+$"},
 *   "mode": "fail|warn|filter|quarantine|annotate"
 * }
 */

#include "internal.h"
#include "cJSON.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <regex.h>
#include <math.h>

typedef enum {
    SCHEMA_EXPECT_ANY = 0,
    SCHEMA_EXPECT_BOOL,
    SCHEMA_EXPECT_INT,
    SCHEMA_EXPECT_FLOAT,
    SCHEMA_EXPECT_NUMBER,
    SCHEMA_EXPECT_STRING,
    SCHEMA_EXPECT_DATE,
    SCHEMA_EXPECT_TIMESTAMP,
} schema_expected_type;

typedef enum {
    SCHEMA_FAIL = 0,
    SCHEMA_WARN,
    SCHEMA_FILTER,
    SCHEMA_QUARANTINE,
    SCHEMA_ANNOTATE,
} schema_action;

typedef struct {
    char *column;
    schema_expected_type expected_type;
    char *expected_type_name;
    int required;
    int nullable;
    int check_type;
    int has_values;
    char **values;
    size_t n_values;
    int has_min;
    int has_max;
    double min_value;
    double max_value;
    int has_regex;
    char *regex_pattern;
    regex_t regex;
    int compiled_regex;
    int col_idx;
} schema_rule;

typedef struct {
    char **selectors;
    size_t n_selectors;
    int type_set;
    schema_expected_type expected_type;
    char *expected_type_name;
    int required_set;
    int required;
    int nullable_set;
    int nullable;
    int has_values;
    char **values;
    size_t n_values;
    int has_min;
    int has_max;
    double min_value;
    double max_value;
    int has_regex;
    char *regex_pattern;
} schema_selector_rule;

typedef struct {
    schema_rule *rules;
    size_t n_rules;
    schema_selector_rule *selector_rules;
    size_t n_selector_rules;
    schema_action action;
    char *name;
    char *message;
    char *result;
    size_t row_index;
    size_t audit_limit;
    size_t audit_emitted;
    size_t checked_rows;
    size_t passed_rows;
    size_t failed_rows;
    size_t violation_count;
    size_t required_failures;
    size_t type_failures;
    size_t nullable_failures;
    size_t values_failures;
    size_t min_failures;
    size_t max_failures;
    size_t regex_failures;
    size_t schema_failures;
    int audit;
    tf_audit_options audit_opts;
    size_t max_regex_pattern_bytes;
    size_t max_regex_cell_bytes;
    int checked_schema;
    int selectors_expanded;
    int schema_ok;
} schema_state;

static const char *schema_action_name(schema_action action) {
    switch (action) {
        case SCHEMA_FAIL: return "fail";
        case SCHEMA_WARN: return "warn";
        case SCHEMA_FILTER: return "filter";
        case SCHEMA_QUARANTINE: return "quarantine";
        case SCHEMA_ANNOTATE: return "annotate";
        default: return "fail";
    }
}

static int parse_action(const char *s, schema_action *out) {
    if (!s || strcmp(s, "fail") == 0 || strcmp(s, "error") == 0 || strcmp(s, "stop") == 0) {
        *out = SCHEMA_FAIL;
        return 1;
    }
    if (strcmp(s, "warn") == 0 || strcmp(s, "warning") == 0) { *out = SCHEMA_WARN; return 1; }
    if (strcmp(s, "filter") == 0 || strcmp(s, "drop") == 0) { *out = SCHEMA_FILTER; return 1; }
    if (strcmp(s, "quarantine") == 0) { *out = SCHEMA_QUARANTINE; return 1; }
    if (strcmp(s, "annotate") == 0) { *out = SCHEMA_ANNOTATE; return 1; }
    return 0;
}

static int parse_expected_type(const char *s, schema_expected_type *out) {
    if (!s || !s[0] || strcmp(s, "any") == 0 || strcmp(s, "*") == 0) {
        *out = SCHEMA_EXPECT_ANY;
        return 1;
    }
    if (strcmp(s, "bool") == 0 || strcmp(s, "boolean") == 0) { *out = SCHEMA_EXPECT_BOOL; return 1; }
    if (strcmp(s, "int") == 0 || strcmp(s, "int64") == 0 || strcmp(s, "integer") == 0) { *out = SCHEMA_EXPECT_INT; return 1; }
    if (strcmp(s, "float") == 0 || strcmp(s, "float64") == 0 || strcmp(s, "double") == 0) { *out = SCHEMA_EXPECT_FLOAT; return 1; }
    if (strcmp(s, "number") == 0 || strcmp(s, "numeric") == 0) { *out = SCHEMA_EXPECT_NUMBER; return 1; }
    if (strcmp(s, "string") == 0 || strcmp(s, "str") == 0 || strcmp(s, "text") == 0) { *out = SCHEMA_EXPECT_STRING; return 1; }
    if (strcmp(s, "date") == 0) { *out = SCHEMA_EXPECT_DATE; return 1; }
    if (strcmp(s, "timestamp") == 0 || strcmp(s, "datetime") == 0) { *out = SCHEMA_EXPECT_TIMESTAMP; return 1; }
    return 0;
}

static const char *type_name(tf_type type) {
    switch (type) {
        case TF_TYPE_BOOL: return "bool";
        case TF_TYPE_INT64: return "int";
        case TF_TYPE_FLOAT64: return "float";
        case TF_TYPE_STRING: return "string";
        case TF_TYPE_DATE: return "date";
        case TF_TYPE_TIMESTAMP: return "timestamp";
        case TF_TYPE_NULL: return "null";
        default: return "unknown";
    }
}

static int type_matches(schema_expected_type expected, tf_type actual) {
    switch (expected) {
        case SCHEMA_EXPECT_ANY: return 1;
        case SCHEMA_EXPECT_BOOL: return actual == TF_TYPE_BOOL;
        case SCHEMA_EXPECT_INT: return actual == TF_TYPE_INT64;
        case SCHEMA_EXPECT_FLOAT: return actual == TF_TYPE_FLOAT64;
        case SCHEMA_EXPECT_NUMBER: return actual == TF_TYPE_INT64 || actual == TF_TYPE_FLOAT64;
        case SCHEMA_EXPECT_STRING: return actual == TF_TYPE_STRING;
        case SCHEMA_EXPECT_DATE: return actual == TF_TYPE_DATE;
        case SCHEMA_EXPECT_TIMESTAMP: return actual == TF_TYPE_TIMESTAMP;
        default: return 0;
    }
}

static schema_rule *find_rule(schema_state *st, const char *column) {
    for (size_t i = 0; i < st->n_rules; i++) {
        if (strcmp(st->rules[i].column, column) == 0) return &st->rules[i];
    }
    return NULL;
}

static void schema_record_failure(schema_state *st, const char *rule) {
    if (!st) return;
    st->violation_count++;
    if (!rule) {
        st->schema_failures++;
    } else if (strcmp(rule, "required") == 0) {
        st->required_failures++;
    } else if (strcmp(rule, "type") == 0) {
        st->type_failures++;
    } else if (strcmp(rule, "nullable") == 0) {
        st->nullable_failures++;
    } else if (strcmp(rule, "values") == 0) {
        st->values_failures++;
    } else if (strcmp(rule, "min") == 0) {
        st->min_failures++;
    } else if (strcmp(rule, "max") == 0) {
        st->max_failures++;
    } else if (strcmp(rule, "regex") == 0) {
        st->regex_failures++;
    } else {
        st->schema_failures++;
    }
}

static schema_rule *ensure_rule(schema_state *st, const char *column) {
    schema_rule *existing = find_rule(st, column);
    if (existing) return existing;
    size_t next = 0;
    if (tf_size_add(st->n_rules, 1, &next) != TF_OK) return NULL;
    schema_rule *nr = tf_reallocarray_checked(st->rules, next, sizeof(schema_rule));
    if (!nr) return NULL;
    st->rules = nr;
    schema_rule *r = &st->rules[st->n_rules];
    st->n_rules = next;
    memset(r, 0, sizeof(*r));
    r->column = strdup(column);
    r->expected_type = SCHEMA_EXPECT_ANY;
    r->expected_type_name = strdup("any");
    r->required = 0;
    r->nullable = 1;
    r->col_idx = -1;
    if (!r->column || !r->expected_type_name) return NULL;
    return r;
}

static int add_value(schema_rule *r, const char *value) {
    size_t next = 0;
    if (tf_size_add(r->n_values, 1, &next) != TF_OK) return TF_ERROR;
    char **nv = tf_reallocarray_checked(r->values, next, sizeof(char *));
    if (!nv) return TF_ERROR;
    r->values = nv;
    r->values[r->n_values] = strdup(value ? value : "");
    if (!r->values[r->n_values]) return TF_ERROR;
    r->n_values++;
    r->has_values = 1;
    return TF_OK;
}

static int add_json_value(schema_rule *r, const cJSON *item) {
    char buf[128];
    if (cJSON_IsString(item)) return add_value(r, item->valuestring);
    if (cJSON_IsBool(item)) return add_value(r, cJSON_IsTrue(item) ? "true" : "false");
    if (cJSON_IsNumber(item)) {
        snprintf(buf, sizeof(buf), "%.17g", item->valuedouble);
        return add_value(r, buf);
    }
    if (cJSON_IsNull(item)) return add_value(r, "");
    return TF_OK;
}

static int rule_set_expected_type(schema_rule *r, const char *type_s) {
    schema_expected_type parsed;
    if (!parse_expected_type(type_s, &parsed)) return TF_ERROR;
    char *name = strdup(type_s);
    if (!name) return TF_ERROR;
    free(r->expected_type_name);
    r->expected_type_name = name;
    r->expected_type = parsed;
    r->check_type = parsed != SCHEMA_EXPECT_ANY;
    return TF_OK;
}

static int schema_regex_pattern_within_cap(const schema_state *st, const char *pattern) {
    size_t len = strlen(pattern ? pattern : "");
    return len <= st->max_regex_pattern_bytes;
}

static int rule_set_regex_pattern(schema_state *st, schema_rule *r, const char *pattern, int compile_now) {
    if (!schema_regex_pattern_within_cap(st, pattern)) {
        tf_set_last_error("schema: regex pattern exceeds max_regex_pattern_bytes");
        return TF_ERROR;
    }
    char *copy = strdup(pattern ? pattern : "");
    if (!copy) return TF_ERROR;
    if (r->compiled_regex) {
        regfree(&r->regex);
        r->compiled_regex = 0;
    }
    free(r->regex_pattern);
    r->regex_pattern = copy;
    r->has_regex = 1;
    if (compile_now) {
        if (regcomp(&r->regex, r->regex_pattern, REG_EXTENDED | REG_NOSUB) != 0) return TF_ERROR;
        r->compiled_regex = 1;
    }
    return TF_OK;
}

static schema_selector_rule *add_selector_rule_items(schema_state *st, char **items, size_t n_items) {
    if (!items || n_items == 0) return NULL;
    size_t next = 0;
    if (tf_size_add(st->n_selector_rules, 1, &next) != TF_OK) return NULL;
    schema_selector_rule *nr = tf_reallocarray_checked(st->selector_rules, next, sizeof(schema_selector_rule));
    if (!nr) return NULL;
    st->selector_rules = nr;
    schema_selector_rule *sr = &st->selector_rules[st->n_selector_rules];
    st->n_selector_rules = next;
    memset(sr, 0, sizeof(*sr));
    sr->selectors = tf_callocarray_checked(n_items, sizeof(char *));
    if (!sr->selectors) return NULL;
    sr->n_selectors = n_items;
    sr->expected_type = SCHEMA_EXPECT_ANY;
    for (size_t i = 0; i < n_items; i++) {
        sr->selectors[i] = strdup(items[i] ? items[i] : "");
        if (!sr->selectors[i]) return NULL;
    }
    return sr;
}

static schema_selector_rule *add_selector_rule_single(schema_state *st, const char *selector) {
    char *items[1];
    items[0] = (char *)(selector ? selector : "");
    return add_selector_rule_items(st, items, 1);
}

static schema_selector_rule *add_selector_rule_json_array(schema_state *st, const cJSON *arr) {
    if (!arr || !cJSON_IsArray(arr)) return NULL;
    int n = cJSON_GetArraySize(arr);
    if (n <= 0) return NULL;
    char **items = tf_callocarray_checked((size_t)n, sizeof(char *));
    if (!items) return NULL;
    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(arr, i);
        if (!cJSON_IsString(item) || !item->valuestring[0]) { free(items); return NULL; }
        items[i] = item->valuestring;
    }
    schema_selector_rule *sr = add_selector_rule_items(st, items, (size_t)n);
    free(items);
    return sr;
}

static int selector_rule_set_expected_type(schema_selector_rule *sr, const char *type_s) {
    schema_expected_type parsed;
    if (!parse_expected_type(type_s, &parsed)) return TF_ERROR;
    char *name = strdup(type_s);
    if (!name) return TF_ERROR;
    free(sr->expected_type_name);
    sr->expected_type_name = name;
    sr->expected_type = parsed;
    sr->type_set = 1;
    return TF_OK;
}

static int selector_rule_add_value(schema_selector_rule *sr, const char *value) {
    size_t next = 0;
    if (tf_size_add(sr->n_values, 1, &next) != TF_OK) return TF_ERROR;
    char **nv = tf_reallocarray_checked(sr->values, next, sizeof(char *));
    if (!nv) return TF_ERROR;
    sr->values = nv;
    sr->values[sr->n_values] = strdup(value ? value : "");
    if (!sr->values[sr->n_values]) return TF_ERROR;
    sr->n_values++;
    sr->has_values = 1;
    return TF_OK;
}

static int selector_rule_add_json_value(schema_selector_rule *sr, const cJSON *item) {
    char buf[128];
    if (cJSON_IsString(item)) return selector_rule_add_value(sr, item->valuestring);
    if (cJSON_IsBool(item)) return selector_rule_add_value(sr, cJSON_IsTrue(item) ? "true" : "false");
    if (cJSON_IsNumber(item)) {
        snprintf(buf, sizeof(buf), "%.17g", item->valuedouble);
        return selector_rule_add_value(sr, buf);
    }
    if (cJSON_IsNull(item)) return selector_rule_add_value(sr, "");
    return TF_OK;
}

static int parse_string_list_into_selector_rule(schema_selector_rule *sr, const cJSON *arr) {
    if (!cJSON_IsArray(arr)) return TF_ERROR;
    cJSON *item = NULL;
    cJSON_ArrayForEach(item, arr) {
        if (selector_rule_add_json_value(sr, item) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static int selector_rule_set_regex_pattern(schema_state *st, schema_selector_rule *sr, const char *pattern) {
    if (!schema_regex_pattern_within_cap(st, pattern)) {
        tf_set_last_error("schema: regex pattern exceeds max_regex_pattern_bytes");
        return TF_ERROR;
    }
    char *copy = strdup(pattern ? pattern : "");
    if (!copy) return TF_ERROR;
    free(sr->regex_pattern);
    sr->regex_pattern = copy;
    sr->has_regex = 1;
    return TF_OK;
}

static int parse_string_list_into_rule(schema_rule *r, const cJSON *arr) {
    if (!cJSON_IsArray(arr)) return TF_ERROR;
    cJSON *item = NULL;
    cJSON_ArrayForEach(item, arr) {
        if (add_json_value(r, item) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static int parse_columns(schema_state *st, const cJSON *columns) {
    if (!columns) return TF_OK;
    if (!cJSON_IsObject(columns)) return TF_ERROR;
    cJSON *entry = NULL;
    cJSON_ArrayForEach(entry, columns) {
        if (!entry->string || !entry->string[0]) return TF_ERROR;
        int is_selector = tf_column_selector_has_syntax(entry->string);
        schema_rule *r = NULL;
        schema_selector_rule *sr = NULL;
        if (is_selector) {
            sr = add_selector_rule_single(st, entry->string);
            if (!sr) return TF_ERROR;
            sr->required_set = 1;
            sr->required = 1;
        } else {
            r = ensure_rule(st, entry->string);
            if (!r) return TF_ERROR;
            r->required = 1;
        }

        const char *type_s = NULL;
        if (cJSON_IsString(entry)) {
            type_s = entry->valuestring;
        } else if (cJSON_IsObject(entry)) {
            cJSON *type_j = cJSON_GetObjectItemCaseSensitive(entry, "type");
            cJSON *required_j = cJSON_GetObjectItemCaseSensitive(entry, "required");
            cJSON *nullable_j = cJSON_GetObjectItemCaseSensitive(entry, "nullable");
            cJSON *values_j = cJSON_GetObjectItemCaseSensitive(entry, "values");
            cJSON *min_j = cJSON_GetObjectItemCaseSensitive(entry, "min");
            cJSON *max_j = cJSON_GetObjectItemCaseSensitive(entry, "max");
            cJSON *regex_j = cJSON_GetObjectItemCaseSensitive(entry, "regex");
            if (cJSON_IsString(type_j)) type_s = type_j->valuestring;
            if (cJSON_IsBool(required_j)) {
                if (sr) { sr->required_set = 1; sr->required = cJSON_IsTrue(required_j) ? 1 : 0; }
                else r->required = cJSON_IsTrue(required_j) ? 1 : 0;
            }
            if (cJSON_IsBool(nullable_j)) {
                if (sr) { sr->nullable_set = 1; sr->nullable = cJSON_IsTrue(nullable_j) ? 1 : 0; }
                else r->nullable = cJSON_IsTrue(nullable_j) ? 1 : 0;
            }
            if (values_j) {
                if (sr) {
                    if (parse_string_list_into_selector_rule(sr, values_j) != TF_OK) return TF_ERROR;
                } else if (parse_string_list_into_rule(r, values_j) != TF_OK) {
                    return TF_ERROR;
                }
            }
            if (cJSON_IsNumber(min_j)) {
                if (sr) { sr->has_min = 1; sr->min_value = min_j->valuedouble; }
                else { r->has_min = 1; r->min_value = min_j->valuedouble; }
            }
            if (cJSON_IsNumber(max_j)) {
                if (sr) { sr->has_max = 1; sr->max_value = max_j->valuedouble; }
                else { r->has_max = 1; r->max_value = max_j->valuedouble; }
            }
            if (cJSON_IsString(regex_j)) {
                if (sr) {
                    if (selector_rule_set_regex_pattern(st, sr, regex_j->valuestring) != TF_OK) return TF_ERROR;
                } else if (rule_set_regex_pattern(st, r, regex_j->valuestring, 0) != TF_OK) {
                    return TF_ERROR;
                }
            }
        } else {
            return TF_ERROR;
        }
        if (type_s) {
            if (sr) {
                if (selector_rule_set_expected_type(sr, type_s) != TF_OK) return TF_ERROR;
            } else if (rule_set_expected_type(r, type_s) != TF_OK) {
                return TF_ERROR;
            }
        }
    }
    return TF_OK;
}

static int parse_string_array_rules(schema_state *st, const cJSON *arr, int required, int nullable) {
    if (!arr) return TF_OK;
    if (!cJSON_IsArray(arr)) return TF_ERROR;
    if (tf_column_selectors_have_syntax_json(arr)) {
        schema_selector_rule *sr = add_selector_rule_json_array(st, arr);
        if (!sr) return TF_ERROR;
        if (required >= 0) { sr->required_set = 1; sr->required = required; }
        if (nullable >= 0) { sr->nullable_set = 1; sr->nullable = nullable; }
        return TF_OK;
    }
    cJSON *item = NULL;
    cJSON_ArrayForEach(item, arr) {
        if (!cJSON_IsString(item) || !item->valuestring[0]) return TF_ERROR;
        schema_rule *r = ensure_rule(st, item->valuestring);
        if (!r) return TF_ERROR;
        if (required >= 0) r->required = required;
        if (nullable >= 0) r->nullable = nullable;
    }
    return TF_OK;
}

static int parse_values_map(schema_state *st, const cJSON *obj) {
    if (!obj) return TF_OK;
    if (!cJSON_IsObject(obj)) return TF_ERROR;
    cJSON *entry = NULL;
    cJSON_ArrayForEach(entry, obj) {
        if (!entry->string || !entry->string[0]) return TF_ERROR;
        if (tf_column_selector_has_syntax(entry->string)) {
            schema_selector_rule *sr = add_selector_rule_single(st, entry->string);
            if (!sr) return TF_ERROR;
            sr->required_set = 1;
            sr->required = 1;
            if (parse_string_list_into_selector_rule(sr, entry) != TF_OK) return TF_ERROR;
        } else {
            schema_rule *r = ensure_rule(st, entry->string);
            if (!r) return TF_ERROR;
            r->required = 1;
            if (parse_string_list_into_rule(r, entry) != TF_OK) return TF_ERROR;
        }
    }
    return TF_OK;
}

static int parse_number_map(schema_state *st, const cJSON *obj, int is_min) {
    if (!obj) return TF_OK;
    if (!cJSON_IsObject(obj)) return TF_ERROR;
    cJSON *entry = NULL;
    cJSON_ArrayForEach(entry, obj) {
        if (!entry->string || !entry->string[0] || !cJSON_IsNumber(entry)) return TF_ERROR;
        if (tf_column_selector_has_syntax(entry->string)) {
            schema_selector_rule *sr = add_selector_rule_single(st, entry->string);
            if (!sr) return TF_ERROR;
            sr->required_set = 1;
            sr->required = 1;
            if (is_min) { sr->has_min = 1; sr->min_value = entry->valuedouble; }
            else { sr->has_max = 1; sr->max_value = entry->valuedouble; }
        } else {
            schema_rule *r = ensure_rule(st, entry->string);
            if (!r) return TF_ERROR;
            r->required = 1;
            if (is_min) { r->has_min = 1; r->min_value = entry->valuedouble; }
            else { r->has_max = 1; r->max_value = entry->valuedouble; }
        }
    }
    return TF_OK;
}

static int parse_regex_map(schema_state *st, const cJSON *obj) {
    if (!obj) return TF_OK;
    if (!cJSON_IsObject(obj)) return TF_ERROR;
    cJSON *entry = NULL;
    cJSON_ArrayForEach(entry, obj) {
        if (!entry->string || !entry->string[0] || !cJSON_IsString(entry)) return TF_ERROR;
        if (tf_column_selector_has_syntax(entry->string)) {
            schema_selector_rule *sr = add_selector_rule_single(st, entry->string);
            if (!sr) return TF_ERROR;
            sr->required_set = 1;
            sr->required = 1;
            if (selector_rule_set_regex_pattern(st, sr, entry->valuestring) != TF_OK) return TF_ERROR;
        } else {
            schema_rule *r = ensure_rule(st, entry->string);
            if (!r) return TF_ERROR;
            r->required = 1;
            if (rule_set_regex_pattern(st, r, entry->valuestring, 0) != TF_OK) return TF_ERROR;
        }
    }
    return TF_OK;
}

static int cell_to_string(const tf_batch *b, size_t row, size_t col, char *buf, size_t buf_size) {
    if (tf_batch_is_null(b, row, col)) {
        if (buf_size) buf[0] = '\0';
        return 0;
    }
    switch (b->col_types[col]) {
        case TF_TYPE_BOOL:
            snprintf(buf, buf_size, "%s", tf_batch_get_bool(b, row, col) ? "true" : "false");
            return 1;
        case TF_TYPE_INT64:
            snprintf(buf, buf_size, "%lld", (long long)tf_batch_get_int64(b, row, col));
            return 1;
        case TF_TYPE_FLOAT64:
            snprintf(buf, buf_size, "%.17g", tf_batch_get_float64(b, row, col));
            return 1;
        case TF_TYPE_STRING: {
            const char *s = tf_batch_get_string(b, row, col);
            snprintf(buf, buf_size, "%s", s ? s : "");
            return 1;
        }
        case TF_TYPE_DATE:
            snprintf(buf, buf_size, "%d", (int)tf_batch_get_date(b, row, col));
            return 1;
        case TF_TYPE_TIMESTAMP:
            snprintf(buf, buf_size, "%lld", (long long)tf_batch_get_timestamp(b, row, col));
            return 1;
        default:
            if (buf_size) buf[0] = '\0';
            return 0;
    }
}

static int cell_to_number(const tf_batch *b, size_t row, size_t col, double *out) {
    if (tf_batch_is_null(b, row, col)) return 0;
    switch (b->col_types[col]) {
        case TF_TYPE_INT64: *out = (double)tf_batch_get_int64(b, row, col); return 1;
        case TF_TYPE_FLOAT64: *out = tf_batch_get_float64(b, row, col); return 1;
        case TF_TYPE_DATE: *out = (double)tf_batch_get_date(b, row, col); return 1;
        case TF_TYPE_TIMESTAMP: *out = (double)tf_batch_get_timestamp(b, row, col); return 1;
        default: return 0;
    }
}

static int emit_failure(schema_state *st, const char *rule, const char *column,
                        const char *expected, const char *actual,
                        const tf_batch *b, size_t row, tf_side_channels *side,
                        int include_row) {
    if (!side || !side->errors) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    cJSON_AddStringToObject(obj, "type", "schema_failure");
    cJSON_AddStringToObject(obj, "op", "schema");
    cJSON_AddStringToObject(obj, "action", schema_action_name(st->action));
    cJSON_AddStringToObject(obj, "severity", st->action == SCHEMA_WARN ? "warning" : "error");
    cJSON_AddStringToObject(obj, "name", st->name ? st->name : "schema");
    cJSON_AddStringToObject(obj, "rule", rule ? rule : "schema");
    if (column) cJSON_AddStringToObject(obj, "column", column);
    if (expected) cJSON_AddStringToObject(obj, "expected", expected);
    if (actual) {
        char actual_buf[256];
        cJSON_AddStringToObject(obj, "actual",
                                tf_audit_format_string_for_column(&st->audit_opts, column, actual,
                                                                  actual_buf, sizeof(actual_buf)));
    }
    if (st->message && st->message[0]) cJSON_AddStringToObject(obj, "message", st->message);
    if (row > 0) cJSON_AddNumberToObject(obj, "row", (double)row);
    if (include_row && b) {
        cJSON *row_obj = tf_audit_row_to_json(b, row - 1, &st->audit_opts);
        if (row_obj) cJSON_AddItemToObject(obj, "data", row_obj);
    }
    char *line = cJSON_PrintUnformatted(obj);
    cJSON_Delete(obj);
    if (!line) return TF_ERROR;
    int rc = tf_buffer_write_line(side->errors, line);
    free(line);
    return rc;
}

static int emit_schema_audit(schema_state *st, const char *rule, const char *column,
                             const char *expected, const char *actual,
                             const tf_batch *b, size_t row, tf_side_channels *side) {
    if (!st->audit || st->audit_emitted >= st->audit_limit || !side || !side->stats) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    cJSON_AddStringToObject(obj, "type", "audit");
    cJSON_AddStringToObject(obj, "op", "schema");
    cJSON_AddStringToObject(obj, "event", "row_dropped");
    cJSON_AddStringToObject(obj, "reason", "schema_failed");
    cJSON_AddStringToObject(obj, "channel", "audit");
    cJSON_AddStringToObject(obj, "action", schema_action_name(st->action));
    cJSON_AddStringToObject(obj, "name", st->name ? st->name : "schema");
    cJSON_AddStringToObject(obj, "rule", rule ? rule : "schema");
    if (column) cJSON_AddStringToObject(obj, "column", column);
    if (expected) cJSON_AddStringToObject(obj, "expected", expected);
    if (actual) {
        char actual_buf[256];
        cJSON_AddStringToObject(obj, "actual",
                                tf_audit_format_string_for_column(&st->audit_opts, column, actual,
                                                                  actual_buf, sizeof(actual_buf)));
    }
    if (st->message && st->message[0]) cJSON_AddStringToObject(obj, "message", st->message);
    cJSON_AddNumberToObject(obj, "row", (double)st->row_index);
    cJSON *row_obj = tf_audit_row_to_json(b, row, &st->audit_opts);
    if (row_obj) cJSON_AddItemToObject(obj, "data", row_obj);
    char *line = cJSON_PrintUnformatted(obj);
    cJSON_Delete(obj);
    if (!line) return TF_ERROR;
    int rc = tf_buffer_write_line(side->stats, line);
    free(line);
    if (rc == TF_OK) st->audit_emitted++;
    return rc;
}

static int handle_schema_level_failure(schema_state *st, const char *rule, const char *column,
                                       const char *expected, const char *actual,
                                       tf_side_channels *side) {
    if (emit_failure(st, rule, column, expected, actual, NULL, 0, side, 0) != TF_OK)
        return TF_ERROR;
    if (st->action == SCHEMA_FAIL || st->action == SCHEMA_FILTER || st->action == SCHEMA_QUARANTINE) {
        char msg[512];
        snprintf(msg, sizeof(msg), "schema failed: %s column '%s' expected %s got %s",
                 rule ? rule : "schema", column ? column : "", expected ? expected : "", actual ? actual : "");
        tf_set_last_error(msg);
        return TF_ERROR;
    }
    return TF_OK;
}

static int apply_selector_rule_to_rule(schema_state *st, schema_rule *r, const schema_selector_rule *sr) {
    if (sr->type_set) {
        char *name = strdup(sr->expected_type_name ? sr->expected_type_name : "any");
        if (!name) return TF_ERROR;
        free(r->expected_type_name);
        r->expected_type_name = name;
        r->expected_type = sr->expected_type;
        r->check_type = sr->expected_type != SCHEMA_EXPECT_ANY;
    }
    if (sr->required_set) r->required = sr->required;
    if (sr->nullable_set) r->nullable = sr->nullable;
    for (size_t i = 0; i < sr->n_values; i++) {
        if (add_value(r, sr->values[i]) != TF_OK) return TF_ERROR;
    }
    if (sr->has_min) { r->has_min = 1; r->min_value = sr->min_value; }
    if (sr->has_max) { r->has_max = 1; r->max_value = sr->max_value; }
    if (sr->has_regex) {
        if (rule_set_regex_pattern(st, r, sr->regex_pattern, 1) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static int expand_schema_selectors(schema_state *st, const tf_batch *in, tf_side_channels *side) {
    if (st->selectors_expanded) return TF_OK;
    st->selectors_expanded = 1;
    for (size_t i = 0; i < st->n_selector_rules; i++) {
        schema_selector_rule *sr = &st->selector_rules[i];
        int *indices = NULL;
        size_t n_indices = 0;
        char *error = NULL;
        if (tf_column_selectors_resolve(sr->selectors, sr->n_selectors,
                                        in->col_names, in->col_types, in->n_cols,
                                        &indices, &n_indices, &error) != TF_OK) {
            char msg[512];
            snprintf(msg, sizeof(msg), "schema selector failed: %s", error ? error : "invalid selector");
            int err_rc = emit_failure(st, "selector", NULL, "matching columns",
                                      error ? error : "invalid selector",
                                      NULL, 0, side, 0);
            schema_record_failure(st, "schema");
            tf_set_last_error(msg);
            free(error);
            if (err_rc != TF_OK) return TF_ERROR;
            return TF_ERROR;
        }
        for (size_t j = 0; j < n_indices; j++) {
            int idx = indices[j];
            if (idx < 0 || (size_t)idx >= in->n_cols) { free(indices); return TF_ERROR; }
            schema_rule *r = ensure_rule(st, in->col_names[(size_t)idx]);
            if (!r) { free(indices); return TF_ERROR; }
            if (apply_selector_rule_to_rule(st, r, sr) != TF_OK) { free(indices); return TF_ERROR; }
        }
        free(indices);
    }
    return TF_OK;
}

static int check_schema_once(schema_state *st, const tf_batch *in, tf_side_channels *side) {
    if (st->checked_schema) return TF_OK;
    st->checked_schema = 1;
    st->schema_ok = 1;
    if (expand_schema_selectors(st, in, side) != TF_OK) return TF_ERROR;
    for (size_t i = 0; i < st->n_rules; i++) {
        schema_rule *r = &st->rules[i];
        r->col_idx = tf_batch_col_index(in, r->column);
        if (r->col_idx < 0) {
            if (r->required) {
                st->schema_ok = 0;
                schema_record_failure(st, "required");
                if (handle_schema_level_failure(st, "required", r->column, "present", "missing", side) != TF_OK)
                    return TF_ERROR;
            }
            continue;
        }
        if (r->check_type && !type_matches(r->expected_type, in->col_types[(size_t)r->col_idx])) {
            st->schema_ok = 0;
            schema_record_failure(st, "type");
            if (handle_schema_level_failure(st, "type", r->column,
                                            r->expected_type_name ? r->expected_type_name : "type",
                                            type_name(in->col_types[(size_t)r->col_idx]), side) != TF_OK)
                return TF_ERROR;
        }
    }
    return TF_OK;
}

static int row_rule_ok(schema_state *st, schema_rule *r, const tf_batch *in, size_t row,
                       tf_side_channels *side, const char **failed_rule,
                       const char **failed_column, const char **expected,
                       char *actual, size_t actual_size) {
    if (r->col_idx < 0) return 1;
    size_t col = (size_t)r->col_idx;
    if (tf_batch_is_null(in, row, col)) {
        if (!r->nullable) {
            *failed_rule = "nullable";
            *failed_column = r->column;
            *expected = "non-null";
            snprintf(actual, actual_size, "null");
            if (st->action == SCHEMA_FAIL || st->action == SCHEMA_WARN || st->action == SCHEMA_QUARANTINE) {
                if (emit_failure(st, *failed_rule, r->column, *expected, actual, in, st->row_index, side,
                                 st->action == SCHEMA_FAIL || st->action == SCHEMA_QUARANTINE) != TF_OK)
                    return -1;
            }
            return 0;
        }
        return 1;
    }

    if (r->has_min || r->has_max) {
        double val = 0.0;
        if (!cell_to_number(in, row, col, &val)) {
            *failed_rule = r->has_min ? "min" : "max";
            *failed_column = r->column;
            *expected = "numeric";
            cell_to_string(in, row, col, actual, actual_size);
            if (st->action == SCHEMA_FAIL || st->action == SCHEMA_WARN || st->action == SCHEMA_QUARANTINE) {
                if (emit_failure(st, *failed_rule, r->column, *expected, actual, in, st->row_index, side,
                                 st->action == SCHEMA_FAIL || st->action == SCHEMA_QUARANTINE) != TF_OK)
                    return -1;
            }
            return 0;
        }
        if (r->has_min && val < r->min_value) {
            static char expbuf[64];
            snprintf(expbuf, sizeof(expbuf), ">= %.17g", r->min_value);
            *failed_rule = "min";
            *failed_column = r->column;
            *expected = expbuf;
            snprintf(actual, actual_size, "%.17g", val);
            if (st->action == SCHEMA_FAIL || st->action == SCHEMA_WARN || st->action == SCHEMA_QUARANTINE) {
                if (emit_failure(st, *failed_rule, r->column, *expected, actual, in, st->row_index, side,
                                 st->action == SCHEMA_FAIL || st->action == SCHEMA_QUARANTINE) != TF_OK)
                    return -1;
            }
            return 0;
        }
        if (r->has_max && val > r->max_value) {
            static char expbuf[64];
            snprintf(expbuf, sizeof(expbuf), "<= %.17g", r->max_value);
            *failed_rule = "max";
            *failed_column = r->column;
            *expected = expbuf;
            snprintf(actual, actual_size, "%.17g", val);
            if (st->action == SCHEMA_FAIL || st->action == SCHEMA_WARN || st->action == SCHEMA_QUARANTINE) {
                if (emit_failure(st, *failed_rule, r->column, *expected, actual, in, st->row_index, side,
                                 st->action == SCHEMA_FAIL || st->action == SCHEMA_QUARANTINE) != TF_OK)
                    return -1;
            }
            return 0;
        }
    }

    if (r->has_values) {
        char valbuf[256];
        cell_to_string(in, row, col, valbuf, sizeof(valbuf));
        int found = 0;
        for (size_t i = 0; i < r->n_values; i++) {
            if (strcmp(valbuf, r->values[i]) == 0) { found = 1; break; }
        }
        if (!found) {
            *failed_rule = "values";
            *failed_column = r->column;
            *expected = "allowed value";
            snprintf(actual, actual_size, "%s", valbuf);
            if (st->action == SCHEMA_FAIL || st->action == SCHEMA_WARN || st->action == SCHEMA_QUARANTINE) {
                if (emit_failure(st, *failed_rule, r->column, *expected, actual, in, st->row_index, side,
                                 st->action == SCHEMA_FAIL || st->action == SCHEMA_QUARANTINE) != TF_OK)
                    return -1;
            }
            return 0;
        }
    }

    if (r->has_regex) {
        if (in->col_types[col] != TF_TYPE_STRING) {
            *failed_rule = "regex";
            *failed_column = r->column;
            *expected = "string";
            snprintf(actual, actual_size, "%s", type_name(in->col_types[col]));
            if (st->action == SCHEMA_FAIL || st->action == SCHEMA_WARN || st->action == SCHEMA_QUARANTINE) {
                if (emit_failure(st, *failed_rule, r->column, *expected, actual, in, st->row_index, side,
                                 st->action == SCHEMA_FAIL || st->action == SCHEMA_QUARANTINE) != TF_OK)
                    return -1;
            }
            return 0;
        }
        const char *s = tf_batch_get_string(in, row, col);
        if (s && strlen(s) > st->max_regex_cell_bytes) {
            *failed_rule = "regex";
            *failed_column = r->column;
            *expected = "regex cell within max_regex_cell_bytes";
            snprintf(actual, actual_size, "cell exceeds %zu bytes", st->max_regex_cell_bytes);
            if (st->action == SCHEMA_FAIL || st->action == SCHEMA_WARN || st->action == SCHEMA_QUARANTINE) {
                if (emit_failure(st, *failed_rule, r->column, *expected, actual, in, st->row_index, side,
                                 st->action == SCHEMA_FAIL || st->action == SCHEMA_QUARANTINE) != TF_OK)
                    return -1;
            }
            return 0;
        }
        if (!s || regexec(&r->regex, s, 0, NULL, 0) != 0) {
            *failed_rule = "regex";
            *failed_column = r->column;
            *expected = r->regex_pattern;
            snprintf(actual, actual_size, "%s", s ? s : "");
            if (st->action == SCHEMA_FAIL || st->action == SCHEMA_WARN || st->action == SCHEMA_QUARANTINE) {
                if (emit_failure(st, *failed_rule, r->column, *expected, actual, in, st->row_index, side,
                                 st->action == SCHEMA_FAIL || st->action == SCHEMA_QUARANTINE) != TF_OK)
                    return -1;
            }
            return 0;
        }
    }

    return 1;
}

static int schema_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    schema_state *st = self->state;
    *out = NULL;
    if (check_schema_once(st, in, side) != TF_OK) return TF_ERROR;

    size_t extra = st->action == SCHEMA_ANNOTATE ? 1u : 0u;
    size_t out_cols = 0;
    if (tf_size_add(in->n_cols, extra, &out_cols) != TF_OK) return TF_ERROR;
    tf_batch *ob = tf_batch_create(out_cols, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) return TF_ERROR;
    if (extra) {
        const char *names[] = {st->result && st->result[0] ? st->result : "_schema"};
        const tf_type types[] = {TF_TYPE_BOOL};
        if (tf_batch_clone_with_extra_cols(ob, in, names, types, 1) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
    } else if (tf_batch_clone_schema(ob, in) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    size_t out_row = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        st->row_index++;
        st->checked_rows++;
        int ok = st->schema_ok;
        const char *failed_rule = NULL;
        const char *failed_column = NULL;
        char expected_copy[256] = {0};
        char actual_copy[256] = {0};
        if (ok) {
            for (size_t i = 0; i < st->n_rules; i++) {
                const char *rule = NULL;
                const char *column = NULL;
                const char *expected = NULL;
                char actual[256] = {0};
                int rule_ok = row_rule_ok(st, &st->rules[i], in, r, side,
                                          &rule, &column, &expected, actual, sizeof(actual));
                if (rule_ok < 0) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                if (!rule_ok) {
                    schema_record_failure(st, rule);
                    if (ok) {
                        failed_rule = rule;
                        failed_column = column;
                        snprintf(expected_copy, sizeof(expected_copy), "%s", expected ? expected : "");
                        snprintf(actual_copy, sizeof(actual_copy), "%s", actual);
                    }
                    ok = 0;
                    if (st->action == SCHEMA_FAIL) break;
                }
            }
        }

        if (!ok) {
            st->failed_rows++;
            if (st->action == SCHEMA_FAIL) {
                if (!failed_rule &&
                    emit_failure(st, "schema", NULL, "valid schema", "invalid schema",
                                 in, st->row_index, side, 1) != TF_OK) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                char msg[512];
                snprintf(msg, sizeof(msg), "schema failed at row %zu%s%s",
                         st->row_index,
                         failed_rule ? ": " : "",
                         failed_rule ? failed_rule : "");
                tf_set_last_error(msg);
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (st->action == SCHEMA_FILTER) {
                if (emit_schema_audit(st, failed_rule ? failed_rule : "schema", failed_column,
                                      expected_copy[0] ? expected_copy : NULL,
                                      actual_copy, in, r, side) != TF_OK) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                continue;
            }
            if (st->action == SCHEMA_QUARANTINE) continue;
        } else {
            st->passed_rows++;
        }

        if (tf_batch_copy_row(ob, out_row, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (st->action == SCHEMA_ANNOTATE &&
            tf_batch_set_bool(ob, out_row, in->n_cols, ok ? true : false) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, out_row) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        out_row++;
    }

    if (ob->n_rows > 0 || st->action == SCHEMA_ANNOTATE || in->n_rows == 0) *out = ob;
    else tf_batch_free(ob);
    return TF_OK;
}

static int schema_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side;
    *out = NULL;
    return TF_OK;
}

static int schema_append_stats(tf_step *self, tf_buffer *out) {
    if (!self || !self->state || !out) return TF_ERROR;
    schema_state *st = self->state;
    char buf[768];
    snprintf(buf, sizeof(buf),
             ",\"checked_rows\":%zu,\"passed_rows\":%zu,\"failed_rows\":%zu,"
             "\"violation_count\":%zu,\"audit_emitted\":%zu,"
             "\"required_failures\":%zu,\"type_failures\":%zu,"
             "\"nullable_failures\":%zu,\"values_failures\":%zu,"
             "\"min_failures\":%zu,\"max_failures\":%zu,"
             "\"regex_failures\":%zu,\"schema_failures\":%zu,"
             "\"max_regex_pattern_bytes\":%zu,\"max_regex_cell_bytes\":%zu",
             st->checked_rows, st->passed_rows, st->failed_rows,
             st->violation_count, st->audit_emitted,
             st->required_failures, st->type_failures,
             st->nullable_failures, st->values_failures,
             st->min_failures, st->max_failures,
             st->regex_failures, st->schema_failures,
             st->max_regex_pattern_bytes, st->max_regex_cell_bytes);
    return tf_buffer_write_str(out, buf);
}

static void schema_state_free(schema_state *st) {
    if (!st) return;
    for (size_t i = 0; i < st->n_rules; i++) {
        schema_rule *r = &st->rules[i];
        free(r->column);
        free(r->expected_type_name);
        for (size_t j = 0; j < r->n_values; j++) free(r->values[j]);
        free(r->values);
        if (r->compiled_regex) regfree(&r->regex);
        free(r->regex_pattern);
    }
    free(st->rules);
    for (size_t i = 0; i < st->n_selector_rules; i++) {
        schema_selector_rule *sr = &st->selector_rules[i];
        for (size_t j = 0; j < sr->n_selectors; j++) free(sr->selectors[j]);
        free(sr->selectors);
        free(sr->expected_type_name);
        for (size_t j = 0; j < sr->n_values; j++) free(sr->values[j]);
        free(sr->values);
        free(sr->regex_pattern);
    }
    free(st->selector_rules);
    free(st->name);
    free(st->message);
    free(st->result);
    tf_audit_options_free(&st->audit_opts);
    free(st);
}

static void schema_destroy(tf_step *self) {
    if (!self) return;
    schema_state_free((schema_state *)self->state);
    free(self);
}

tf_step *tf_schema_create(const cJSON *args) {
    if (!args) return NULL;
    schema_state *st = calloc(1, sizeof(schema_state));
    if (!st) return NULL;
    st->schema_ok = 1;
    st->action = SCHEMA_FAIL;
    st->audit_limit = 1000;
    tf_audit_options_init(&st->audit_opts, 1);
    st->max_regex_pattern_bytes = TF_MAX_REGEX_PATTERN_BYTES;
    st->max_regex_cell_bytes = TF_MAX_REGEX_CELL_BYTES;

    if (tf_json_get_size_arg(args, "max_regex_pattern_bytes", 1, TF_MAX_RECORD_BYTES,
                             &st->max_regex_pattern_bytes, "schema") < 0 ||
        tf_json_get_size_arg(args, "maxRegexPatternBytes", 1, TF_MAX_RECORD_BYTES,
                             &st->max_regex_pattern_bytes, "schema") < 0 ||
        tf_json_get_size_arg(args, "max_regex_cell_bytes", 1, TF_MAX_RECORD_BYTES,
                             &st->max_regex_cell_bytes, "schema") < 0 ||
        tf_json_get_size_arg(args, "maxRegexCellBytes", 1, TF_MAX_RECORD_BYTES,
                             &st->max_regex_cell_bytes, "schema") < 0) {
        schema_state_free(st);
        return NULL;
    }

    cJSON *audit_j = cJSON_GetObjectItemCaseSensitive(args, "audit");
    st->audit = cJSON_IsTrue(audit_j) ? 1 : 0;
    cJSON *audit_limit_j = cJSON_GetObjectItemCaseSensitive(args, "audit_limit");
    if (!audit_limit_j) audit_limit_j = cJSON_GetObjectItemCaseSensitive(args, "auditLimit");
    if (audit_limit_j) {
        size_t parsed_limit = 0;
        if (tf_json_get_size_arg_any(args, "audit_limit", "auditLimit",
                                     1, TF_MAX_AUDIT_RECORDS,
                                     &parsed_limit, "schema") < 0) {
            schema_state_free(st);
            return NULL;
        }
        st->audit_limit = parsed_limit;
    }
    if (tf_audit_options_parse(&st->audit_opts, args, "schema") != TF_OK) {
        schema_state_free(st);
        return NULL;
    }

    cJSON *mode_j = cJSON_GetObjectItemCaseSensitive(args, "mode");
    cJSON *action_j = cJSON_GetObjectItemCaseSensitive(args, "action");
    const char *action_s = cJSON_IsString(action_j) ? action_j->valuestring : (cJSON_IsString(mode_j) ? mode_j->valuestring : "fail");
    if (!parse_action(action_s, &st->action)) {
        tf_set_last_error("schema: mode must be fail, warn, filter, quarantine, or annotate");
        schema_state_free(st);
        return NULL;
    }

    cJSON *name_j = cJSON_GetObjectItemCaseSensitive(args, "name");
    cJSON *message_j = cJSON_GetObjectItemCaseSensitive(args, "message");
    cJSON *result_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    st->name = strdup(cJSON_IsString(name_j) && name_j->valuestring[0] ? name_j->valuestring : "schema");
    st->message = strdup(cJSON_IsString(message_j) ? message_j->valuestring : "");
    st->result = strdup(cJSON_IsString(result_j) && result_j->valuestring[0] ? result_j->valuestring : "_schema");
    if (!st->name || !st->message || !st->result) { schema_state_free(st); return NULL; }

    tf_set_last_error(NULL);
    if (parse_columns(st, cJSON_GetObjectItemCaseSensitive(args, "columns")) != TF_OK ||
        parse_string_array_rules(st, cJSON_GetObjectItemCaseSensitive(args, "required"), 1, -1) != TF_OK ||
        parse_string_array_rules(st, cJSON_GetObjectItemCaseSensitive(args, "non_null"), 1, 0) != TF_OK ||
        parse_string_array_rules(st, cJSON_GetObjectItemCaseSensitive(args, "nullable"), -1, 1) != TF_OK ||
        parse_values_map(st, cJSON_GetObjectItemCaseSensitive(args, "values")) != TF_OK ||
        parse_number_map(st, cJSON_GetObjectItemCaseSensitive(args, "min"), 1) != TF_OK ||
        parse_number_map(st, cJSON_GetObjectItemCaseSensitive(args, "max"), 0) != TF_OK ||
        parse_regex_map(st, cJSON_GetObjectItemCaseSensitive(args, "regex")) != TF_OK) {
        if (!tf_last_error() || !tf_last_error()[0])
            tf_set_last_error("schema: invalid schema arguments");
        schema_state_free(st);
        return NULL;
    }

    for (size_t i = 0; i < st->n_rules; i++) {
        schema_rule *r = &st->rules[i];
        if (r->has_regex) {
            if (regcomp(&r->regex, r->regex_pattern, REG_EXTENDED | REG_NOSUB) != 0) {
                tf_set_last_error("schema: invalid regex");
                schema_state_free(st);
                return NULL;
            }
            r->compiled_regex = 1;
        }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { schema_state_free(st); return NULL; }
    step->process = schema_process;
    step->flush = schema_flush;
    step->destroy = schema_destroy;
    step->append_stats = schema_append_stats;
    step->state = st;
    return step;
}
