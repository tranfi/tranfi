/*
 * op_json_flatten.c -- Append declared JSON path fields as columns per row.
 *
 * Config: {"column":"_line", "fields":[{"path":"/user/id", "name":"user_id", "type":"int"}]}
 * This is a bounded row-local flatten: output columns must be declared up front.
 */

#include "internal.h"
#include "cJSON.h"
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char   *path;
    char   *name;
    tf_type type;
} json_flatten_field;

typedef struct {
    char               *column;
    json_flatten_field *fields;
    size_t              n_fields;
} json_flatten_state;

static tf_type parse_output_type(const char *s) {
    if (!s || strcmp(s, "string") == 0 || strcmp(s, "str") == 0 || strcmp(s, "json") == 0)
        return TF_TYPE_STRING;
    if (strcmp(s, "int") == 0 || strcmp(s, "int64") == 0)
        return TF_TYPE_INT64;
    if (strcmp(s, "float") == 0 || strcmp(s, "float64") == 0 || strcmp(s, "number") == 0)
        return TF_TYPE_FLOAT64;
    if (strcmp(s, "bool") == 0 || strcmp(s, "boolean") == 0)
        return TF_TYPE_BOOL;
    return TF_TYPE_NULL;
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

static int str_eq_ci(const char *a, const char *b) {
    while (*a && *b) {
        if (tolower((unsigned char)*a) != tolower((unsigned char)*b)) return 0;
        a++; b++;
    }
    return *a == '\0' && *b == '\0';
}

static int parse_bool_string(const char *s, int *out) {
    if (!s) return 0;
    if (str_eq_ci(s, "true") || strcmp(s, "1") == 0) { *out = 1; return 1; }
    if (str_eq_ci(s, "false") || strcmp(s, "0") == 0) { *out = 0; return 1; }
    return 0;
}

static int set_flattened_value(tf_batch *ob, size_t row, size_t col,
                               tf_type type, const cJSON *val) {
    if (!val || cJSON_IsNull(val)) {
        return tf_batch_set_null(ob, row, col);
    }

    switch (type) {
        case TF_TYPE_STRING: {
            if (cJSON_IsString(val)) {
                return tf_batch_set_string(ob, row, col, val->valuestring ? val->valuestring : "");
            }
            char *printed = cJSON_PrintUnformatted((cJSON *)val);
            if (!printed) return tf_batch_set_null(ob, row, col);
            int rc = tf_batch_set_string(ob, row, col, printed);
            free(printed);
            return rc;
        }
        case TF_TYPE_INT64: {
            int64_t out = 0;
            if (cJSON_IsNumber(val)) return tf_batch_set_int64(ob, row, col, (int64_t)val->valuedouble);
            if (cJSON_IsBool(val)) return tf_batch_set_int64(ob, row, col, cJSON_IsTrue(val) ? 1 : 0);
            if (cJSON_IsString(val) && parse_strict_int64(val->valuestring, &out)) return tf_batch_set_int64(ob, row, col, out);
            return tf_batch_set_null(ob, row, col);
        }
        case TF_TYPE_FLOAT64: {
            double out = 0.0;
            if (cJSON_IsNumber(val)) return tf_batch_set_float64(ob, row, col, val->valuedouble);
            if (cJSON_IsBool(val)) return tf_batch_set_float64(ob, row, col, cJSON_IsTrue(val) ? 1.0 : 0.0);
            if (cJSON_IsString(val) && parse_strict_double(val->valuestring, &out)) return tf_batch_set_float64(ob, row, col, out);
            return tf_batch_set_null(ob, row, col);
        }
        case TF_TYPE_BOOL: {
            int out = 0;
            if (cJSON_IsBool(val)) return tf_batch_set_bool(ob, row, col, cJSON_IsTrue(val));
            if (cJSON_IsNumber(val)) return tf_batch_set_bool(ob, row, col, val->valuedouble != 0.0);
            if (cJSON_IsString(val) && parse_bool_string(val->valuestring, &out)) return tf_batch_set_bool(ob, row, col, out != 0);
            return tf_batch_set_null(ob, row, col);
        }
        default:
            return tf_batch_set_null(ob, row, col);
    }
}

static int json_flatten_process(tf_step *self, tf_batch *in, tf_batch **out,
                                tf_side_channels *side) {
    (void)side;
    json_flatten_state *st = self->state;
    *out = NULL;

    const char **extra_names = NULL;
    tf_type *extra_types = NULL;
    if (st->n_fields > 0) {
        extra_names = malloc(st->n_fields * sizeof(char *));
        extra_types = malloc(st->n_fields * sizeof(tf_type));
        if (!extra_names || !extra_types) {
            free(extra_names);
            free(extra_types);
            return TF_ERROR;
        }
        for (size_t i = 0; i < st->n_fields; i++) {
            extra_names[i] = st->fields[i].name;
            extra_types[i] = st->fields[i].type;
        }
    }

    tf_batch *ob = tf_batch_create(in->n_cols + st->n_fields, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) { free(extra_names); free(extra_types); return TF_ERROR; }
    if (tf_batch_clone_with_extra_cols(ob, in, extra_names, extra_types, st->n_fields) != TF_OK) {
        tf_batch_free(ob);
        free(extra_names);
        free(extra_types);
        return TF_ERROR;
    }
    free(extra_names);
    free(extra_types);

    int ci = tf_batch_col_index(in, st->column);
    int can_parse = ci >= 0 && in->col_types[(size_t)ci] == TF_TYPE_STRING;

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) goto fail;
        for (size_t i = 0; i < st->n_fields; i++) {
            if (tf_batch_set_null(ob, r, in->n_cols + i) != TF_OK) goto fail;
        }

        cJSON *root = NULL;
        if (can_parse && !tf_batch_is_null(in, r, (size_t)ci)) {
            const char *json = tf_batch_get_string(in, r, (size_t)ci);
            if (json && json[0] != '\0') root = cJSON_Parse(json);
        }
        if (root) {
            for (size_t i = 0; i < st->n_fields; i++) {
                const cJSON *val = tf_json_path_resolve(root, st->fields[i].path);
                if (set_flattened_value(ob, r, in->n_cols + i, st->fields[i].type, val) != TF_OK) {
                    cJSON_Delete(root);
                    goto fail;
                }
            }
            cJSON_Delete(root);
        }
        ob->n_rows = r + 1;
    }

    if (ob->n_rows > 0) *out = ob;
    else tf_batch_free(ob);
    return TF_OK;

fail:
    tf_batch_free(ob);
    return TF_ERROR;
}

static int json_flatten_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void json_flatten_field_free(json_flatten_field *f) {
    if (!f) return;
    free(f->path);
    free(f->name);
}

static void json_flatten_state_free(json_flatten_state *st) {
    if (!st) return;
    free(st->column);
    for (size_t i = 0; i < st->n_fields; i++) json_flatten_field_free(&st->fields[i]);
    free(st->fields);
    free(st);
}

static void json_flatten_destroy(tf_step *self) {
    json_flatten_state_free(self ? self->state : NULL);
    free(self);
}

static int parse_field_spec(const char *spec, char **path, char **name, tf_type *type) {
    if (!spec) return 0;
    char *copy = strdup(spec);
    if (!copy) return 0;
    char *first = strchr(copy, ':');
    if (!first) { free(copy); return 0; }
    *first = '\0';
    char *second = strchr(first + 1, ':');
    if (second) *second = '\0';
    const char *type_s = second ? second + 1 : "string";
    tf_type parsed = parse_output_type(type_s);
    if (copy[0] == '\0' || first[1] == '\0' || parsed == TF_TYPE_NULL) {
        free(copy);
        return 0;
    }
    if (tf_json_path_validate(copy) != TF_OK) {
        free(copy);
        return 0;
    }
    *path = strdup(copy);
    *name = strdup(first + 1);
    *type = parsed;
    free(copy);
    return *path && *name;
}

static int init_field_from_json(const cJSON *item, json_flatten_field *field) {
    memset(field, 0, sizeof(*field));
    if (cJSON_IsString(item)) {
        return parse_field_spec(item->valuestring, &field->path, &field->name, &field->type);
    }
    if (!cJSON_IsObject(item)) return 0;
    cJSON *path_j = cJSON_GetObjectItemCaseSensitive(item, "path");
    cJSON *name_j = cJSON_GetObjectItemCaseSensitive(item, "name");
    if (!cJSON_IsString(name_j)) name_j = cJSON_GetObjectItemCaseSensitive(item, "result");
    cJSON *type_j = cJSON_GetObjectItemCaseSensitive(item, "type");
    tf_type type = parse_output_type(cJSON_IsString(type_j) ? type_j->valuestring : "string");
    if (!cJSON_IsString(path_j) || !cJSON_IsString(name_j) ||
        path_j->valuestring[0] == '\0' || name_j->valuestring[0] == '\0' ||
        type == TF_TYPE_NULL) return 0;
    if (tf_json_path_validate(path_j->valuestring) != TF_OK) return 0;
    field->path = strdup(path_j->valuestring);
    field->name = strdup(name_j->valuestring);
    field->type = type;
    return field->path && field->name;
}

tf_step *tf_json_flatten_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *fields_j = cJSON_GetObjectItemCaseSensitive(args, "fields");
    if (!cJSON_IsArray(fields_j) || cJSON_GetArraySize(fields_j) <= 0) return NULL;

    cJSON *column_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    const char *column = cJSON_IsString(column_j) && column_j->valuestring[0] != '\0'
        ? column_j->valuestring : "_line";

    int n_fields = cJSON_GetArraySize(fields_j);
    json_flatten_state *st = calloc(1, sizeof(json_flatten_state));
    if (!st) return NULL;
    st->column = strdup(column);
    st->fields = calloc((size_t)n_fields, sizeof(json_flatten_field));
    st->n_fields = (size_t)n_fields;
    if (!st->column || !st->fields) { json_flatten_state_free(st); return NULL; }

    for (int i = 0; i < n_fields; i++) {
        cJSON *item = cJSON_GetArrayItem(fields_j, i);
        if (!init_field_from_json(item, &st->fields[i])) {
            json_flatten_state_free(st);
            return NULL;
        }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { json_flatten_state_free(st); return NULL; }
    step->process = json_flatten_process;
    step->flush = json_flatten_flush;
    step->destroy = json_flatten_destroy;
    step->state = st;
    return step;
}
