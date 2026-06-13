/*
 * op_json_extract.c -- Extract scalar or JSON text values from per-row JSON columns.
 *
 * Config: {"column":"_line", "path":"/user/id", "result":"user_id", "type":"int"}
 * Supports JSON Pointer plus a small JSONPath-style subset: $.a[0].b or a[0].b.
 */

#include "internal.h"
#include "cJSON.h"
#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char   *column;
    char   *path;
    char   *result;
    tf_type type;
} json_extract_state;

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

static void set_extracted_value(tf_batch *ob, size_t row, size_t col,
                                tf_type type, const cJSON *val) {
    if (!val || cJSON_IsNull(val)) {
        tf_batch_set_null(ob, row, col);
        return;
    }

    switch (type) {
        case TF_TYPE_STRING: {
            if (cJSON_IsString(val)) {
                tf_batch_set_string(ob, row, col, val->valuestring ? val->valuestring : "");
            } else {
                char *printed = cJSON_PrintUnformatted((cJSON *)val);
                if (printed) {
                    tf_batch_set_string(ob, row, col, printed);
                    free(printed);
                } else {
                    tf_batch_set_null(ob, row, col);
                }
            }
            break;
        }
        case TF_TYPE_INT64: {
            int64_t out = 0;
            if (cJSON_IsNumber(val)) {
                tf_batch_set_int64(ob, row, col, (int64_t)val->valuedouble);
            } else if (cJSON_IsBool(val)) {
                tf_batch_set_int64(ob, row, col, cJSON_IsTrue(val) ? 1 : 0);
            } else if (cJSON_IsString(val) && parse_strict_int64(val->valuestring, &out)) {
                tf_batch_set_int64(ob, row, col, out);
            } else {
                tf_batch_set_null(ob, row, col);
            }
            break;
        }
        case TF_TYPE_FLOAT64: {
            double out = 0.0;
            if (cJSON_IsNumber(val)) {
                tf_batch_set_float64(ob, row, col, val->valuedouble);
            } else if (cJSON_IsBool(val)) {
                tf_batch_set_float64(ob, row, col, cJSON_IsTrue(val) ? 1.0 : 0.0);
            } else if (cJSON_IsString(val) && parse_strict_double(val->valuestring, &out)) {
                tf_batch_set_float64(ob, row, col, out);
            } else {
                tf_batch_set_null(ob, row, col);
            }
            break;
        }
        case TF_TYPE_BOOL: {
            int out = 0;
            if (cJSON_IsBool(val)) {
                tf_batch_set_bool(ob, row, col, cJSON_IsTrue(val));
            } else if (cJSON_IsNumber(val)) {
                tf_batch_set_bool(ob, row, col, val->valuedouble != 0.0);
            } else if (cJSON_IsString(val) && parse_bool_string(val->valuestring, &out)) {
                tf_batch_set_bool(ob, row, col, out != 0);
            } else {
                tf_batch_set_null(ob, row, col);
            }
            break;
        }
        default:
            tf_batch_set_null(ob, row, col);
            break;
    }
}

static int json_extract_process(tf_step *self, tf_batch *in, tf_batch **out,
                                tf_side_channels *side) {
    (void)side;
    json_extract_state *st = self->state;
    *out = NULL;

    tf_batch *ob = tf_batch_create(in->n_cols + 1, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) return TF_ERROR;

    for (size_t c = 0; c < in->n_cols; c++)
        if (tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    if (tf_batch_set_schema(ob, in->n_cols, st->result, st->type) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    int ci = tf_batch_col_index(in, st->column);
    int can_parse = ci >= 0 && in->col_types[(size_t)ci] == TF_TYPE_STRING;

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        tf_batch_set_null(ob, r, in->n_cols);

        if (can_parse && !tf_batch_is_null(in, r, (size_t)ci)) {
            const char *json = tf_batch_get_string(in, r, (size_t)ci);
            if (json && json[0] != '\0') {
                cJSON *root = cJSON_Parse(json);
                if (root) {
                    const cJSON *val = tf_json_path_resolve(root, st->path);
                    set_extracted_value(ob, r, in->n_cols, st->type, val);
                    cJSON_Delete(root);
                }
            }
        }
        ob->n_rows = r + 1;
    }

    if (ob->n_rows > 0) {
        *out = ob;
    } else {
        tf_batch_free(ob);
    }
    return TF_OK;
}

static int json_extract_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void json_extract_state_free(json_extract_state *st) {
    if (!st) return;
    free(st->column);
    free(st->path);
    free(st->result);
    free(st);
}

static void json_extract_destroy(tf_step *self) {
    json_extract_state_free(self ? self->state : NULL);
    free(self);
}

tf_step *tf_json_extract_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *path_j = cJSON_GetObjectItemCaseSensitive(args, "path");
    cJSON *result_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (!cJSON_IsString(path_j) || !cJSON_IsString(result_j) || result_j->valuestring[0] == '\0')
        return NULL;

    cJSON *column_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    const char *column = cJSON_IsString(column_j) && column_j->valuestring[0] != '\0'
        ? column_j->valuestring : "_line";
    cJSON *type_j = cJSON_GetObjectItemCaseSensitive(args, "type");
    tf_type type = parse_output_type(cJSON_IsString(type_j) ? type_j->valuestring : "string");
    if (type == TF_TYPE_NULL) return NULL;

    json_extract_state *st = calloc(1, sizeof(json_extract_state));
    if (!st) return NULL;
    st->column = strdup(column);
    st->path = strdup(path_j->valuestring);
    st->result = strdup(result_j->valuestring);
    st->type = type;
    if (!st->column || !st->path || !st->result) {
        free(st->column); free(st->path); free(st->result); free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { json_extract_state_free(st); return NULL; }
    step->process = json_extract_process;
    step->flush = json_extract_flush;
    step->destroy = json_extract_destroy;
    step->state = st;
    return step;
}
