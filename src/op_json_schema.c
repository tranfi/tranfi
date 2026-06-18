/*
 * op_json_schema.c -- Row-local JSON Schema subset validation.
 *
 * Config: {"column":"_line", "schema":{...}, "mode":"annotate", "result":"_valid"}
 * Supported schema keywords: type, required, properties, items, enum, const,
 * minLength, maxLength, minimum, maximum, exclusiveMinimum, exclusiveMaximum,
 * minItems, maxItems. This is intentionally not a full JSON Schema engine.
 */

#include "internal.h"
#include "cJSON.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>

typedef enum {
    JSON_SCHEMA_ANNOTATE,
    JSON_SCHEMA_FILTER,
} json_schema_mode;

typedef struct {
    char             *column;
    char             *result;
    cJSON            *schema;
    json_schema_mode  mode;
    size_t            row_index;
    size_t            audit_limit;
    size_t            audit_emitted;
    int               audit;
    tf_audit_options  audit_opts;
} json_schema_state;

static int type_name_valid(const char *s) {
    return strcmp(s, "object") == 0 || strcmp(s, "array") == 0 ||
           strcmp(s, "string") == 0 || strcmp(s, "number") == 0 ||
           strcmp(s, "integer") == 0 || strcmp(s, "boolean") == 0 ||
           strcmp(s, "bool") == 0 || strcmp(s, "null") == 0;
}

static int schema_type_supported(const cJSON *type) {
    if (!type) return 1;
    if (cJSON_IsString(type)) return type_name_valid(type->valuestring);
    if (!cJSON_IsArray(type)) return 0;
    const cJSON *item = NULL;
    cJSON_ArrayForEach(item, type) {
        if (!cJSON_IsString(item) || !type_name_valid(item->valuestring)) return 0;
    }
    return 1;
}

static int string_array_supported(const cJSON *arr) {
    if (!cJSON_IsArray(arr)) return 0;
    const cJSON *item = NULL;
    cJSON_ArrayForEach(item, arr) {
        if (!cJSON_IsString(item)) return 0;
    }
    return 1;
}

static int schema_supported(const cJSON *schema);

static int properties_supported(const cJSON *props) {
    if (!cJSON_IsObject(props)) return 0;
    const cJSON *prop = NULL;
    cJSON_ArrayForEach(prop, props) {
        if (!schema_supported(prop)) return 0;
    }
    return 1;
}

static int schema_size_keyword_supported(const cJSON *item, const char *key) {
    size_t parsed = 0;
    size_t max_value = (strcmp(key, "minLength") == 0 || strcmp(key, "maxLength") == 0)
        ? TF_MAX_RECORD_BYTES
        : TF_MAX_COUNT_ARG;
    return tf_json_size_value(item, key, 0, max_value, &parsed, "json-schema") == 1;
}

static int schema_number_keyword_supported(const cJSON *item) {
    return cJSON_IsNumber(item) && isfinite(item->valuedouble);
}

static int schema_supported(const cJSON *schema) {
    if (cJSON_IsBool(schema)) return 1;
    if (!cJSON_IsObject(schema)) return 0;

    const cJSON *child = NULL;
    cJSON_ArrayForEach(child, schema) {
        const char *key = child->string ? child->string : "";
        if (strcmp(key, "$schema") == 0 || strcmp(key, "$id") == 0 ||
            strcmp(key, "title") == 0 || strcmp(key, "description") == 0) {
            continue;
        }
        if (strcmp(key, "type") == 0) {
            if (!schema_type_supported(child)) return 0;
        } else if (strcmp(key, "required") == 0) {
            if (!string_array_supported(child)) return 0;
        } else if (strcmp(key, "properties") == 0) {
            if (!properties_supported(child)) return 0;
        } else if (strcmp(key, "items") == 0) {
            if (!schema_supported(child)) return 0;
        } else if (strcmp(key, "enum") == 0) {
            if (!cJSON_IsArray(child)) return 0;
        } else if (strcmp(key, "const") == 0) {
            continue;
        } else if (strcmp(key, "minLength") == 0 || strcmp(key, "maxLength") == 0 ||
                   strcmp(key, "minItems") == 0 || strcmp(key, "maxItems") == 0) {
            if (!schema_size_keyword_supported(child, key)) return 0;
        } else if (strcmp(key, "minimum") == 0 || strcmp(key, "maximum") == 0) {
            if (!schema_number_keyword_supported(child)) return 0;
        } else if (strcmp(key, "exclusiveMinimum") == 0 || strcmp(key, "exclusiveMaximum") == 0) {
            if (cJSON_IsNumber(child)) {
                if (!schema_number_keyword_supported(child)) return 0;
            } else if (!cJSON_IsBool(child)) {
                return 0;
            }
        } else {
            return 0;
        }
    }
    return 1;
}

static int json_is_integer(const cJSON *v) {
    if (!cJSON_IsNumber(v)) return 0;
    double d = v->valuedouble;
    if (!isfinite(d)) return 0;
    double integral = 0.0;
    return modf(d, &integral) == 0.0;
}

static int value_matches_type(const cJSON *value, const char *type) {
    if (strcmp(type, "object") == 0) return cJSON_IsObject(value);
    if (strcmp(type, "array") == 0) return cJSON_IsArray(value);
    if (strcmp(type, "string") == 0) return cJSON_IsString(value);
    if (strcmp(type, "number") == 0) return cJSON_IsNumber(value);
    if (strcmp(type, "integer") == 0) return json_is_integer(value);
    if (strcmp(type, "boolean") == 0 || strcmp(type, "bool") == 0) return cJSON_IsBool(value);
    if (strcmp(type, "null") == 0) return cJSON_IsNull(value);
    return 0;
}

static int value_matches_type_schema(const cJSON *value, const cJSON *type) {
    if (!type) return 1;
    if (cJSON_IsString(type)) return value_matches_type(value, type->valuestring);
    if (!cJSON_IsArray(type)) return 0;
    const cJSON *item = NULL;
    cJSON_ArrayForEach(item, type) {
        if (cJSON_IsString(item) && value_matches_type(value, item->valuestring)) return 1;
    }
    return 0;
}

static int validate_against_schema(const cJSON *schema, const cJSON *value);

static int validate_enum(const cJSON *schema, const cJSON *value) {
    const cJSON *choices = cJSON_GetObjectItemCaseSensitive(schema, "enum");
    if (!choices) return 1;
    const cJSON *item = NULL;
    cJSON_ArrayForEach(item, choices) {
        if (cJSON_Compare(value, item, 1)) return 1;
    }
    return 0;
}

static int validate_const(const cJSON *schema, const cJSON *value) {
    const cJSON *constant = cJSON_GetObjectItemCaseSensitive(schema, "const");
    if (!constant) return 1;
    return cJSON_Compare(value, constant, 1) ? 1 : 0;
}

static int validate_string_constraints(const cJSON *schema, const cJSON *value) {
    const cJSON *min_len = cJSON_GetObjectItemCaseSensitive(schema, "minLength");
    const cJSON *max_len = cJSON_GetObjectItemCaseSensitive(schema, "maxLength");
    if (!min_len && !max_len) return 1;
    if (!cJSON_IsString(value)) return 0;
    size_t len = strlen(value->valuestring ? value->valuestring : "");
    size_t limit = 0;
    if (min_len) {
        if (tf_json_size_value(min_len, "minLength", 0, TF_MAX_RECORD_BYTES,
                               &limit, "json-schema") != 1) return 0;
        if (len < limit) return 0;
    }
    if (max_len) {
        if (tf_json_size_value(max_len, "maxLength", 0, TF_MAX_RECORD_BYTES,
                               &limit, "json-schema") != 1) return 0;
        if (len > limit) return 0;
    }
    return 1;
}

static int validate_number_constraints(const cJSON *schema, const cJSON *value) {
    const cJSON *minimum = cJSON_GetObjectItemCaseSensitive(schema, "minimum");
    const cJSON *maximum = cJSON_GetObjectItemCaseSensitive(schema, "maximum");
    const cJSON *exclusive_min = cJSON_GetObjectItemCaseSensitive(schema, "exclusiveMinimum");
    const cJSON *exclusive_max = cJSON_GetObjectItemCaseSensitive(schema, "exclusiveMaximum");
    if (!minimum && !maximum && !exclusive_min && !exclusive_max) return 1;
    if (!cJSON_IsNumber(value)) return 0;
    double v = value->valuedouble;
    if (minimum) {
        int exclusive = cJSON_IsTrue(exclusive_min);
        if ((exclusive && !(v > minimum->valuedouble)) || (!exclusive && !(v >= minimum->valuedouble))) return 0;
    }
    if (maximum) {
        int exclusive = cJSON_IsTrue(exclusive_max);
        if ((exclusive && !(v < maximum->valuedouble)) || (!exclusive && !(v <= maximum->valuedouble))) return 0;
    }
    if (exclusive_min && cJSON_IsNumber(exclusive_min) && !(v > exclusive_min->valuedouble)) return 0;
    if (exclusive_max && cJSON_IsNumber(exclusive_max) && !(v < exclusive_max->valuedouble)) return 0;
    return 1;
}

static int validate_object_constraints(const cJSON *schema, const cJSON *value) {
    const cJSON *required = cJSON_GetObjectItemCaseSensitive(schema, "required");
    const cJSON *props = cJSON_GetObjectItemCaseSensitive(schema, "properties");
    if (!required && !props) return 1;
    if (!cJSON_IsObject(value)) return 0;

    const cJSON *item = NULL;
    cJSON_ArrayForEach(item, required) {
        if (!cJSON_GetObjectItemCaseSensitive((cJSON *)value, item->valuestring)) return 0;
    }

    const cJSON *prop_schema = NULL;
    cJSON_ArrayForEach(prop_schema, props) {
        const char *name = prop_schema->string ? prop_schema->string : "";
        const cJSON *child = cJSON_GetObjectItemCaseSensitive((cJSON *)value, name);
        if (child && !validate_against_schema(prop_schema, child)) return 0;
    }
    return 1;
}

static int validate_array_constraints(const cJSON *schema, const cJSON *value) {
    const cJSON *items = cJSON_GetObjectItemCaseSensitive(schema, "items");
    const cJSON *min_items = cJSON_GetObjectItemCaseSensitive(schema, "minItems");
    const cJSON *max_items = cJSON_GetObjectItemCaseSensitive(schema, "maxItems");
    if (!items && !min_items && !max_items) return 1;
    if (!cJSON_IsArray(value)) return 0;

    int raw_n = cJSON_GetArraySize((cJSON *)value);
    size_t n = raw_n > 0 ? (size_t)raw_n : 0;
    size_t limit = 0;
    if (min_items) {
        if (tf_json_size_value(min_items, "minItems", 0, TF_MAX_COUNT_ARG,
                               &limit, "json-schema") != 1) return 0;
        if (n < limit) return 0;
    }
    if (max_items) {
        if (tf_json_size_value(max_items, "maxItems", 0, TF_MAX_COUNT_ARG,
                               &limit, "json-schema") != 1) return 0;
        if (n > limit) return 0;
    }
    if (items) {
        const cJSON *item = NULL;
        cJSON_ArrayForEach(item, value) {
            if (!validate_against_schema(items, item)) return 0;
        }
    }
    return 1;
}

static int validate_against_schema(const cJSON *schema, const cJSON *value) {
    if (cJSON_IsBool(schema)) return cJSON_IsTrue(schema) ? 1 : 0;
    if (!cJSON_IsObject(schema) || !value) return 0;

    const cJSON *type = cJSON_GetObjectItemCaseSensitive(schema, "type");
    if (!value_matches_type_schema(value, type)) return 0;
    if (!validate_enum(schema, value)) return 0;
    if (!validate_const(schema, value)) return 0;
    if (!validate_string_constraints(schema, value)) return 0;
    if (!validate_number_constraints(schema, value)) return 0;
    if (!validate_object_constraints(schema, value)) return 0;
    if (!validate_array_constraints(schema, value)) return 0;
    return 1;
}

static const char *json_schema_mode_name(json_schema_mode mode) {
    return mode == JSON_SCHEMA_FILTER ? "filter" : "annotate";
}

static int emit_json_schema_audit(json_schema_state *st, const tf_batch *b, size_t row,
                                  tf_side_channels *side, const char *actual) {
    if (!st->audit || st->audit_emitted >= st->audit_limit || !side || !side->stats) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    int rc = TF_ERROR;
    if (tf_json_add_string(obj, "type", "audit") != TF_OK ||
        tf_json_add_string(obj, "op", "json-schema") != TF_OK ||
        tf_json_add_string(obj, "event", "row_dropped") != TF_OK ||
        tf_json_add_string(obj, "reason", "json_schema_failed") != TF_OK ||
        tf_json_add_string(obj, "channel", "audit") != TF_OK ||
        tf_json_add_string(obj, "mode", json_schema_mode_name(st->mode)) != TF_OK ||
        tf_json_add_string(obj, "rule", "json-schema") != TF_OK ||
        tf_json_add_string(obj, "column", st->column ? st->column : "_line") != TF_OK) {
        goto done;
    }
    if (tf_json_add_audit_string(obj, "actual", &st->audit_opts, st->column,
                                 actual ? actual : "schema_mismatch") != TF_OK ||
        tf_json_add_number(obj, "row", (double)st->row_index) != TF_OK) {
        goto done;
    }
    cJSON *row_obj = tf_audit_row_to_json(b, row, &st->audit_opts);
    if (row_obj) {
        if (tf_json_add_item(obj, "data", row_obj) != TF_OK) goto done;
    } else if (st->audit_opts.include_row) {
        goto done;
    }
    rc = tf_buffer_write_json_line(side->stats, obj);
done:
    cJSON_Delete(obj);
    if (rc == TF_OK) st->audit_emitted++;
    return rc;
}

static int json_schema_process(tf_step *self, tf_batch *in, tf_batch **out,
                               tf_side_channels *side) {
    json_schema_state *st = self->state;
    *out = NULL;

    size_t out_cols = in->n_cols + (st->mode == JSON_SCHEMA_ANNOTATE ? 1 : 0);
    tf_batch *ob = tf_batch_create(out_cols, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) return TF_ERROR;
    if (st->mode == JSON_SCHEMA_ANNOTATE) {
        const char *extra_names[] = {st->result};
        const tf_type extra_types[] = {TF_TYPE_BOOL};
        if (tf_batch_clone_with_extra_cols(ob, in, extra_names, extra_types, 1) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    } else if (tf_batch_clone_schema(ob, in) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    int ci = tf_batch_col_index(in, st->column);
    int can_parse = ci >= 0 && in->col_types[(size_t)ci] == TF_TYPE_STRING;
    size_t out_row = 0;

    for (size_t r = 0; r < in->n_rows; r++) {
        st->row_index++;
        int valid = 0;
        const char *actual = "schema_mismatch";
        cJSON *root = NULL;
        if (ci < 0) {
            actual = "missing_column";
        } else if (!can_parse) {
            actual = "non_string";
        } else if (tf_batch_is_null(in, r, (size_t)ci)) {
            actual = "null";
        } else {
            const char *json = tf_batch_get_string(in, r, (size_t)ci);
            if (!json || json[0] == '\0') {
                actual = "empty_json";
            } else {
                root = cJSON_Parse(json);
                if (root) {
                    valid = validate_against_schema(st->schema, root);
                    actual = valid ? "valid" : "schema_mismatch";
                } else if (cJSON_ParseHadAllocationFailure()) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                } else {
                    actual = "invalid_json";
                }
            }
        }

        if (st->mode == JSON_SCHEMA_FILTER && !valid) {
            if (emit_json_schema_audit(st, in, r, side, actual) != TF_OK) {
                if (root) cJSON_Delete(root);
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (root) cJSON_Delete(root);
            continue;
        }
        if (tf_batch_copy_row(ob, out_row, in, r) != TF_OK) {
            if (root) cJSON_Delete(root);
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (st->mode == JSON_SCHEMA_ANNOTATE &&
            tf_batch_set_bool(ob, out_row, in->n_cols, valid != 0) != TF_OK) {
            if (root) cJSON_Delete(root);
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, out_row) != TF_OK) {
            if (root) cJSON_Delete(root);
            tf_batch_free(ob);
            return TF_ERROR;
        }
        out_row++;
        if (root) cJSON_Delete(root);
    }

    if (ob->n_rows > 0) *out = ob;
    else tf_batch_free(ob);
    return TF_OK;
}

static int json_schema_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void json_schema_state_free(json_schema_state *st) {
    if (!st) return;
    free(st->column);
    free(st->result);
    if (st->schema) cJSON_Delete(st->schema);
    tf_audit_options_free(&st->audit_opts);
    free(st);
}

static void json_schema_destroy(tf_step *self) {
    json_schema_state_free(self ? self->state : NULL);
    free(self);
}

static int parse_mode(const char *s, json_schema_mode *out) {
    if (!s || strcmp(s, "annotate") == 0 || strcmp(s, "check") == 0) {
        *out = JSON_SCHEMA_ANNOTATE;
        return 1;
    }
    if (strcmp(s, "filter") == 0 || strcmp(s, "keep") == 0) {
        *out = JSON_SCHEMA_FILTER;
        return 1;
    }
    return 0;
}

static cJSON *schema_arg_to_json(const cJSON *schema_j) {
    if (!schema_j) return NULL;
    if (cJSON_IsString(schema_j)) return cJSON_Parse(schema_j->valuestring);
    if (cJSON_IsObject(schema_j) || cJSON_IsArray(schema_j) || cJSON_IsBool(schema_j)) {
        return cJSON_Duplicate((cJSON *)schema_j, 1);
    }
    return NULL;
}

tf_step *tf_json_schema_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *schema_j = cJSON_GetObjectItemCaseSensitive(args, "schema");
    cJSON *schema = schema_arg_to_json(schema_j);
    if (!schema || !schema_supported(schema)) {
        if (schema) cJSON_Delete(schema);
        return NULL;
    }

    cJSON *column_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    const char *column = cJSON_IsString(column_j) && column_j->valuestring[0] != '\0'
        ? column_j->valuestring : "_line";

    cJSON *result_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    const char *result = cJSON_IsString(result_j) && result_j->valuestring[0] != '\0'
        ? result_j->valuestring : "_valid";

    json_schema_mode mode = JSON_SCHEMA_ANNOTATE;
    cJSON *mode_j = cJSON_GetObjectItemCaseSensitive(args, "mode");
    if (mode_j && (!cJSON_IsString(mode_j) || !parse_mode(mode_j->valuestring, &mode))) {
        cJSON_Delete(schema);
        return NULL;
    }

    json_schema_state *st = calloc(1, sizeof(json_schema_state));
    if (!st) { cJSON_Delete(schema); return NULL; }
    st->column = strdup(column);
    st->result = strdup(result);
    st->schema = schema;
    st->mode = mode;
    st->audit_limit = 1000;
    tf_audit_options_init(&st->audit_opts, 1);
    cJSON *audit_j = cJSON_GetObjectItemCaseSensitive(args, "audit");
    st->audit = cJSON_IsTrue(audit_j) ? 1 : 0;
    cJSON *audit_limit_j = cJSON_GetObjectItemCaseSensitive(args, "audit_limit");
    if (!audit_limit_j) audit_limit_j = cJSON_GetObjectItemCaseSensitive(args, "auditLimit");
    if (audit_limit_j) {
        size_t parsed_limit = 0;
        if (tf_json_get_size_arg_any(args, "audit_limit", "auditLimit",
                                     1, TF_MAX_AUDIT_RECORDS,
                                     &parsed_limit, "json-schema") < 0) {
            json_schema_state_free(st);
            return NULL;
        }
        st->audit_limit = parsed_limit;
    }
    if (tf_audit_options_parse(&st->audit_opts, args, "json-schema") != TF_OK) {
        json_schema_state_free(st);
        return NULL;
    }
    if (!st->column || !st->result) {
        json_schema_state_free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { json_schema_state_free(st); return NULL; }
    step->process = json_schema_process;
    step->flush = json_schema_flush;
    step->destroy = json_schema_destroy;
    step->state = st;
    return step;
}
