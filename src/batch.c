/*
 * batch.c — Columnar batch: typed columns with per-cell null tracking.
 */

#include "internal.h"
#include "cJSON.h"
#include "date_utils.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static size_t type_size(tf_type t) {
    switch (t) {
        case TF_TYPE_BOOL:      return sizeof(uint8_t);
        case TF_TYPE_INT64:     return sizeof(int64_t);
        case TF_TYPE_FLOAT64:   return sizeof(double);
        case TF_TYPE_STRING:    return sizeof(char *);
        case TF_TYPE_DATE:      return sizeof(int32_t);
        case TF_TYPE_TIMESTAMP: return sizeof(int64_t);
        default:                return 0;
    }
}

tf_batch *tf_batch_create(size_t n_cols, size_t capacity) {
    if (n_cols > TF_MAX_COLUMNS) {
        char err[256];
        snprintf(err, sizeof(err),
                 "batch: columns exceeds maximum %zu, observed %zu",
                 TF_MAX_COLUMNS, n_cols);
        tf_set_last_error(err);
        return NULL;
    }

    tf_arena *arena = tf_arena_create(0);
    if (!arena) return NULL;

    tf_batch *b = tf_arena_alloc(arena, sizeof(tf_batch));
    if (!b) { tf_arena_free(arena); return NULL; }

    b->arena = arena;
    b->n_cols = n_cols;
    b->n_rows = 0;
    b->capacity = capacity;
    b->schema_name_bytes = 0;
    size_t names_bytes = 0, types_bytes = 0, columns_bytes = 0, nulls_bytes = 0;
    if (tf_size_mul(n_cols, sizeof(char *), &names_bytes) != TF_OK ||
        tf_size_mul(n_cols, sizeof(tf_type), &types_bytes) != TF_OK ||
        tf_size_mul(n_cols, sizeof(void *), &columns_bytes) != TF_OK ||
        tf_size_mul(n_cols, sizeof(uint8_t *), &nulls_bytes) != TF_OK) {
        tf_arena_free(arena);
        return NULL;
    }
    b->col_names = tf_arena_alloc(arena, names_bytes);
    b->col_types = tf_arena_alloc(arena, types_bytes);
    b->columns = tf_arena_alloc(arena, columns_bytes);
    b->nulls = tf_arena_alloc(arena, nulls_bytes);

    if (!b->col_names || !b->col_types || !b->columns || !b->nulls) {
        tf_arena_free(arena);
        return NULL;
    }

    for (size_t i = 0; i < n_cols; i++) {
        b->col_names[i] = NULL;
        b->col_types[i] = TF_TYPE_NULL;
        b->columns[i] = NULL;
        b->nulls[i] = NULL;
    }

    return b;
}

int tf_batch_set_schema(tf_batch *b, size_t col, const char *name, tf_type type) {
    if (!b || col >= b->n_cols) return TF_ERROR;
    const char *safe_name = name ? name : "";
    size_t name_len = 0, name_bytes = 0, schema_bytes = 0;
    if (tf_string_length_bounded(safe_name, TF_MAX_COLUMN_NAME_BYTES,
                                 &name_len, "batch", "column name") != TF_OK ||
        tf_size_add(name_len, 1, &name_bytes) != TF_OK ||
        tf_size_add(b->schema_name_bytes, name_bytes, &schema_bytes) != TF_OK ||
        tf_check_byte_limit(schema_bytes, TF_MAX_SCHEMA_BYTES,
                            "batch", "schema") != TF_OK)
        return TF_ERROR;
    char *name_copy = tf_arena_alloc(b->arena, name_bytes);
    if (!name_copy) return TF_ERROR;
    memcpy(name_copy, safe_name, name_len);
    name_copy[name_len] = '\0';

    void *column = NULL;
    uint8_t *nulls = NULL;
    size_t sz = type_size(type);
    if (sz > 0 && b->capacity > 0) {
        size_t column_bytes = 0;
        if (tf_size_mul(sz, b->capacity, &column_bytes) != TF_OK) return TF_ERROR;
        column = tf_arena_alloc(b->arena, column_bytes);
        nulls = tf_arena_alloc(b->arena, b->capacity);
        if (!column || !nulls) return TF_ERROR;
        memset(nulls, 1, b->capacity); /* all null by default */
    }

    b->col_names[col] = name_copy;
    b->col_types[col] = type;
    b->columns[col] = column;
    b->nulls[col] = nulls;
    b->schema_name_bytes = schema_bytes;
    return TF_OK;
}

int tf_batch_ensure_capacity(tf_batch *b, size_t min_rows) {
    if (min_rows <= b->capacity) return TF_OK;

    /* We need to reallocate columns — but arena doesn't support realloc.
     * Allocate new larger arrays and copy. */
    size_t new_cap = 0;
    if (tf_size_grow_pow2(b->capacity, min_rows, 16, &new_cap) != TF_OK) return TF_ERROR;

    for (size_t i = 0; i < b->n_cols; i++) {
        size_t sz = type_size(b->col_types[i]);
        if (sz == 0) continue;

        size_t column_bytes = 0;
        size_t copy_bytes = 0;
        if (tf_size_mul(sz, new_cap, &column_bytes) != TF_OK ||
            tf_size_mul(sz, b->n_rows, &copy_bytes) != TF_OK) return TF_ERROR;
        void *new_col = tf_arena_alloc(b->arena, column_bytes);
        uint8_t *new_null = tf_arena_alloc(b->arena, new_cap);
        if (!new_col || !new_null) return TF_ERROR;

        if (b->columns[i] && b->n_rows > 0) {
            memcpy(new_col, b->columns[i], copy_bytes);
            memcpy(new_null, b->nulls[i], b->n_rows);
        }
        memset(new_null + b->n_rows, 1, new_cap - b->n_rows);
        b->columns[i] = new_col;
        b->nulls[i] = new_null;
    }
    b->capacity = new_cap;
    return TF_OK;
}

int tf_batch_expose_row(tf_batch *b, size_t row) {
    if (!b) return TF_ERROR;
    size_t next_rows = 0;
    if (tf_size_add(row, 1, &next_rows) != TF_OK) return TF_ERROR;
    if (next_rows > b->capacity) return TF_ERROR;
    b->n_rows = next_rows;
    return TF_OK;
}

/* ---- Setters ---- */

int tf_batch_set_null(tf_batch *b, size_t row, size_t col) {
    if (!b || row >= b->capacity || col >= b->n_cols) return TF_ERROR;
    if (!b->nulls[col]) return TF_OK;
    b->nulls[col][row] = 1;
    return TF_OK;
}

int tf_batch_set_bool(tf_batch *b, size_t row, size_t col, bool val) {
    if (!b || row >= b->capacity || col >= b->n_cols ||
        b->col_types[col] != TF_TYPE_BOOL || !b->columns[col] || !b->nulls[col]) return TF_ERROR;
    ((uint8_t *)b->columns[col])[row] = val ? 1 : 0;
    b->nulls[col][row] = 0;
    return TF_OK;
}

int tf_batch_set_int64(tf_batch *b, size_t row, size_t col, int64_t val) {
    if (!b || row >= b->capacity || col >= b->n_cols ||
        b->col_types[col] != TF_TYPE_INT64 || !b->columns[col] || !b->nulls[col]) return TF_ERROR;
    ((int64_t *)b->columns[col])[row] = val;
    b->nulls[col][row] = 0;
    return TF_OK;
}

int tf_batch_set_float64(tf_batch *b, size_t row, size_t col, double val) {
    if (!b || row >= b->capacity || col >= b->n_cols ||
        b->col_types[col] != TF_TYPE_FLOAT64 || !b->columns[col] || !b->nulls[col]) return TF_ERROR;
    ((double *)b->columns[col])[row] = val;
    b->nulls[col][row] = 0;
    return TF_OK;
}

int tf_batch_set_string(tf_batch *b, size_t row, size_t col, const char *val) {
    size_t len = 0;
    if (tf_string_length_bounded(val, TF_MAX_CELL_BYTES,
                                 &len, "batch", "cell") != TF_OK)
        return TF_ERROR;
    return tf_batch_set_string_len(b, row, col, val, len);
}

int tf_batch_set_string_len(tf_batch *b, size_t row, size_t col,
                            const char *val, size_t len) {
    if (!b || row >= b->capacity || col >= b->n_cols || !val ||
        b->col_types[col] != TF_TYPE_STRING || !b->columns[col] || !b->nulls[col]) return TF_ERROR;
    if (tf_check_byte_limit(len, TF_MAX_CELL_BYTES, "batch", "cell") != TF_OK)
        return TF_ERROR;
    size_t bytes = 0;
    if (tf_size_add(len, 1, &bytes) != TF_OK) return TF_ERROR;
    char *copy = tf_arena_alloc(b->arena, bytes);
    if (!copy) return TF_ERROR;
    memcpy(copy, val, len);
    copy[len] = '\0';
    ((char **)b->columns[col])[row] = copy;
    b->nulls[col][row] = 0;
    return TF_OK;
}

int tf_batch_set_date(tf_batch *b, size_t row, size_t col, int32_t val) {
    if (!b || row >= b->capacity || col >= b->n_cols ||
        b->col_types[col] != TF_TYPE_DATE || !b->columns[col] || !b->nulls[col]) return TF_ERROR;
    ((int32_t *)b->columns[col])[row] = val;
    b->nulls[col][row] = 0;
    return TF_OK;
}

int tf_batch_set_timestamp(tf_batch *b, size_t row, size_t col, int64_t val) {
    if (!b || row >= b->capacity || col >= b->n_cols ||
        b->col_types[col] != TF_TYPE_TIMESTAMP || !b->columns[col] || !b->nulls[col]) return TF_ERROR;
    ((int64_t *)b->columns[col])[row] = val;
    b->nulls[col][row] = 0;
    return TF_OK;
}

int tf_batch_set_cell_value(tf_batch *b, size_t row, size_t col,
                            tf_type type, int is_null, const tf_cell_value *value) {
    if (!b || col >= b->n_cols || b->col_types[col] != type) return TF_ERROR;
    if (is_null) return tf_batch_set_null(b, row, col);
    if (!value) return TF_ERROR;

    switch (type) {
        case TF_TYPE_BOOL:
            return tf_batch_set_bool(b, row, col, value->b);
        case TF_TYPE_INT64:
            return tf_batch_set_int64(b, row, col, value->i64);
        case TF_TYPE_FLOAT64:
            return tf_batch_set_float64(b, row, col, value->f64);
        case TF_TYPE_STRING:
            return tf_batch_set_string(b, row, col, value->str ? value->str : "");
        case TF_TYPE_DATE:
            return tf_batch_set_date(b, row, col, value->date);
        case TF_TYPE_TIMESTAMP:
            return tf_batch_set_timestamp(b, row, col, value->i64);
        default:
            return tf_batch_set_null(b, row, col);
    }
}

int tf_batch_set_owned_cell_value(tf_batch *b, size_t row, size_t col,
                                  tf_type type, int is_null,
                                  const tf_owned_cell_value *value) {
    tf_cell_value tmp;
    memset(&tmp, 0, sizeof(tmp));
    if (value) {
        switch (type) {
            case TF_TYPE_BOOL:
                tmp.b = value->b != 0;
                break;
            case TF_TYPE_INT64:
                tmp.i64 = value->i64;
                break;
            case TF_TYPE_FLOAT64:
                tmp.f64 = value->f64;
                break;
            case TF_TYPE_STRING:
                tmp.str = value->str;
                break;
            case TF_TYPE_DATE:
                tmp.date = value->date;
                break;
            case TF_TYPE_TIMESTAMP:
                tmp.i64 = value->i64;
                break;
            default:
                break;
        }
    }
    return tf_batch_set_cell_value(b, row, col, type, is_null, value ? &tmp : NULL);
}

/* ---- Getters ---- */

size_t tf_batch_num_rows(const tf_batch *b) {
    return b ? b->n_rows : 0;
}

size_t tf_batch_num_cols(const tf_batch *b) {
    return b ? b->n_cols : 0;
}

const char *tf_batch_col_name(const tf_batch *b, size_t col) {
    if (!b || col >= b->n_cols) return NULL;
    return b->col_names[col];
}

tf_type tf_batch_col_type(const tf_batch *b, size_t col) {
    if (!b || col >= b->n_cols) return TF_TYPE_NULL;
    return b->col_types[col];
}

bool tf_batch_is_null(const tf_batch *b, size_t row, size_t col) {
    if (!b || row >= b->n_rows || col >= b->n_cols) return true;
    if (!b->nulls[col]) return true;
    return b->nulls[col][row] != 0;
}

bool tf_batch_get_bool(const tf_batch *b, size_t row, size_t col) {
    if (!b || row >= b->n_rows || col >= b->n_cols) return false;
    return ((uint8_t *)b->columns[col])[row] != 0;
}

int64_t tf_batch_get_int64(const tf_batch *b, size_t row, size_t col) {
    if (!b || row >= b->n_rows || col >= b->n_cols) return 0;
    return ((int64_t *)b->columns[col])[row];
}

double tf_batch_get_float64(const tf_batch *b, size_t row, size_t col) {
    if (!b || row >= b->n_rows || col >= b->n_cols) return 0.0;
    return ((double *)b->columns[col])[row];
}

const char *tf_batch_get_string(const tf_batch *b, size_t row, size_t col) {
    if (!b || row >= b->n_rows || col >= b->n_cols) return NULL;
    return ((char **)b->columns[col])[row];
}

int32_t tf_batch_get_date(const tf_batch *b, size_t row, size_t col) {
    if (!b || row >= b->n_rows || col >= b->n_cols) return 0;
    return ((int32_t *)b->columns[col])[row];
}

int64_t tf_batch_get_timestamp(const tf_batch *b, size_t row, size_t col) {
    if (!b || row >= b->n_rows || col >= b->n_cols) return 0;
    return ((int64_t *)b->columns[col])[row];
}

int tf_batch_copy_cell(tf_batch *dst, size_t dst_row, size_t dst_col,
                       const tf_batch *src, size_t src_row, size_t src_col) {
    if (!dst || !src || src_row >= src->n_rows || src_col >= src->n_cols || dst_col >= dst->n_cols)
        return TF_ERROR;

    tf_type type = src->col_types[src_col];
    int is_null = tf_batch_is_null(src, src_row, src_col);
    tf_cell_value value;
    memset(&value, 0, sizeof(value));
    if (!is_null) {
        switch (type) {
            case TF_TYPE_BOOL:
                value.b = tf_batch_get_bool(src, src_row, src_col);
                break;
            case TF_TYPE_INT64:
                value.i64 = tf_batch_get_int64(src, src_row, src_col);
                break;
            case TF_TYPE_FLOAT64:
                value.f64 = tf_batch_get_float64(src, src_row, src_col);
                break;
            case TF_TYPE_STRING:
                value.str = tf_batch_get_string(src, src_row, src_col);
                break;
            case TF_TYPE_DATE:
                value.date = tf_batch_get_date(src, src_row, src_col);
                break;
            case TF_TYPE_TIMESTAMP:
                value.i64 = tf_batch_get_timestamp(src, src_row, src_col);
                break;
            default:
                break;
        }
    }
    return tf_batch_set_cell_value(dst, dst_row, dst_col, type, is_null, &value);
}

int tf_batch_copy_cell_index(tf_batch *dst, size_t dst_row, size_t dst_col,
                             const tf_batch *src, size_t src_row, int src_col) {
    if (src_col < 0) return TF_ERROR;
    return tf_batch_copy_cell(dst, dst_row, dst_col, src, src_row, (size_t)src_col);
}

int tf_batch_copy_cell_as_string(tf_batch *dst, size_t dst_row, size_t dst_col,
                                 const tf_batch *src, size_t src_row, size_t src_col) {
    if (!dst || !src || src_row >= src->n_rows || src_col >= src->n_cols ||
        dst_col >= dst->n_cols || dst->col_types[dst_col] != TF_TYPE_STRING) {
        return TF_ERROR;
    }
    if (tf_batch_is_null(src, src_row, src_col)) {
        return tf_batch_set_null(dst, dst_row, dst_col);
    }

    char buf[64];
    int n = 0;
    switch (src->col_types[src_col]) {
        case TF_TYPE_STRING:
            return tf_batch_set_string(dst, dst_row, dst_col,
                                       tf_batch_get_string(src, src_row, src_col));
        case TF_TYPE_INT64:
            n = snprintf(buf, sizeof(buf), "%lld",
                         (long long)tf_batch_get_int64(src, src_row, src_col));
            if (n < 0 || (size_t)n >= sizeof(buf)) return TF_ERROR;
            return tf_batch_set_string(dst, dst_row, dst_col, buf);
        case TF_TYPE_FLOAT64:
            if (tf_format_float64(buf, sizeof(buf),
                                  tf_batch_get_float64(src, src_row, src_col)) != TF_OK) {
                return TF_ERROR;
            }
            return tf_batch_set_string(dst, dst_row, dst_col, buf);
        case TF_TYPE_BOOL:
            return tf_batch_set_string(dst, dst_row, dst_col,
                                       tf_batch_get_bool(src, src_row, src_col) ? "true" : "false");
        case TF_TYPE_DATE:
            tf_date_format(tf_batch_get_date(src, src_row, src_col), buf, sizeof(buf));
            return tf_batch_set_string(dst, dst_row, dst_col, buf);
        case TF_TYPE_TIMESTAMP:
            tf_timestamp_format(tf_batch_get_timestamp(src, src_row, src_col), buf, sizeof(buf));
            return tf_batch_set_string(dst, dst_row, dst_col, buf);
        default:
            return tf_batch_set_null(dst, dst_row, dst_col);
    }
}

int tf_batch_clone_schema(tf_batch *dst, const tf_batch *src) {
    if (!dst || !src || dst->n_cols < src->n_cols) return TF_ERROR;
    for (size_t c = 0; c < src->n_cols; c++) {
        if (tf_batch_set_schema(dst, c, src->col_names[c], src->col_types[c]) != TF_OK)
            return TF_ERROR;
    }
    return TF_OK;
}

int tf_batch_clone_with_extra_cols(tf_batch *dst, const tf_batch *src,
                                   const char **names, const tf_type *types,
                                   size_t n_extra) {
    if (!dst || !src) return TF_ERROR;
    size_t needed_cols = 0;
    if (tf_size_add(src->n_cols, n_extra, &needed_cols) != TF_OK || dst->n_cols < needed_cols)
        return TF_ERROR;
    if (n_extra > 0 && (!names || !types)) return TF_ERROR;
    if (tf_batch_clone_schema(dst, src) != TF_OK) return TF_ERROR;
    for (size_t i = 0; i < n_extra; i++) {
        if (tf_batch_set_schema(dst, src->n_cols + i, names[i], types[i]) != TF_OK)
            return TF_ERROR;
    }
    return TF_OK;
}

int tf_batch_copy_selected_row(tf_batch *dst, size_t dst_row,
                               const tf_batch *src, size_t src_row,
                               const size_t *cols, size_t n_cols) {
    if (!dst || !src || !cols || src_row >= src->n_rows || dst->n_cols < n_cols) return TF_ERROR;
    size_t need_rows = 0;
    if (tf_size_add(dst_row, 1, &need_rows) != TF_OK) return TF_ERROR;
    if (tf_batch_ensure_capacity(dst, need_rows) != TF_OK) return TF_ERROR;
    for (size_t i = 0; i < n_cols; i++) {
        if (cols[i] == SIZE_MAX) {
            if (tf_batch_set_null(dst, dst_row, i) != TF_OK) return TF_ERROR;
            continue;
        }
        if (cols[i] >= src->n_cols) return TF_ERROR;
        if (tf_batch_copy_cell(dst, dst_row, i, src, src_row, cols[i]) != TF_OK)
            return TF_ERROR;
    }
    return TF_OK;
}

int tf_batch_copy_row(tf_batch *dst, size_t dst_row,
                      const tf_batch *src, size_t src_row) {
    if (!dst || !src || src_row >= src->n_rows || dst->n_cols < src->n_cols) return TF_ERROR;
    size_t need_rows = 0;
    if (tf_size_add(dst_row, 1, &need_rows) != TF_OK) return TF_ERROR;
    if (tf_batch_ensure_capacity(dst, need_rows) != TF_OK) return TF_ERROR;
    for (size_t c = 0; c < src->n_cols; c++) {
        if (tf_batch_copy_cell(dst, dst_row, c, src, src_row, c) != TF_OK)
            return TF_ERROR;
    }
    return TF_OK;
}

int tf_batch_col_index(const tf_batch *b, const char *name) {
    for (size_t i = 0; i < b->n_cols; i++) {
        if (b->col_names[i] && strcmp(b->col_names[i], name) == 0)
            return (int)i;
    }
    return -1;
}


/* ---- Audit / side-channel row serialization helpers ---- */

void tf_audit_options_init(tf_audit_options *opts, int default_include_row) {
    if (!opts) return;
    memset(opts, 0, sizeof(*opts));
    opts->include_row = default_include_row ? 1 : 0;
}

static void audit_string_list_free(char **items, size_t n_items) {
    if (!items) return;
    for (size_t i = 0; i < n_items; i++) free(items[i]);
    free(items);
}

void tf_audit_options_free(tf_audit_options *opts) {
    if (!opts) return;
    audit_string_list_free(opts->columns, opts->n_columns);
    audit_string_list_free(opts->redact_columns, opts->n_redact_columns);
    audit_string_list_free(opts->hash_columns, opts->n_hash_columns);
    memset(opts, 0, sizeof(*opts));
}

static int audit_add_string(char ***items, size_t *n_items, const char *value) {
    if (!items || !n_items || !value || !value[0]) return TF_ERROR;
    char *dup = strdup(value);
    if (!dup) return TF_ERROR;
    size_t next_count = 0;
    if (tf_size_add(*n_items, 1, &next_count) != TF_OK) {
        free(dup);
        return TF_ERROR;
    }
    char **next = tf_reallocarray_checked(*items, next_count, sizeof(char *));
    if (!next) {
        free(dup);
        return TF_ERROR;
    }
    *items = next;
    (*items)[*n_items] = dup;
    (*n_items)++;
    return TF_OK;
}

static char *audit_trim(char *s) {
    while (*s == ' ' || *s == '\t' || *s == '\n' || *s == '\r') s++;
    char *end = s + strlen(s);
    while (end > s && (end[-1] == ' ' || end[-1] == '\t' || end[-1] == '\n' || end[-1] == '\r')) end--;
    *end = '\0';
    return s;
}

static int audit_parse_string_list_value(const cJSON *value, char ***items, size_t *n_items,
                                         const char *context, const char *name) {
    if (!value) return TF_OK;
    if (cJSON_IsArray(value)) {
        int n = cJSON_GetArraySize((cJSON *)value);
        for (int i = 0; i < n; i++) {
            cJSON *item = cJSON_GetArrayItem((cJSON *)value, i);
            if (!cJSON_IsString(item) || !item->valuestring[0]) goto invalid;
            if (audit_add_string(items, n_items, item->valuestring) != TF_OK) return TF_ERROR;
        }
        return TF_OK;
    }
    if (cJSON_IsString(value)) {
        char *copy = strdup(value->valuestring ? value->valuestring : "");
        if (!copy) return TF_ERROR;
        char *save = NULL;
        for (char *tok = strtok_r(copy, ",", &save); tok; tok = strtok_r(NULL, ",", &save)) {
            char *trimmed = audit_trim(tok);
            if (trimmed[0] && audit_add_string(items, n_items, trimmed) != TF_OK) {
                free(copy);
                return TF_ERROR;
            }
        }
        free(copy);
        return TF_OK;
    }
invalid:
    {
        char msg[256];
        snprintf(msg, sizeof(msg), "%s: %s must be a string or string array",
                 context ? context : "audit", name ? name : "audit option");
        tf_set_last_error(msg);
    }
    return TF_ERROR;
}

static const cJSON *audit_get_arg(const cJSON *args, const char *snake, const char *camel) {
    const cJSON *item = args && snake ? cJSON_GetObjectItemCaseSensitive(args, snake) : NULL;
    if (!item && args && camel) item = cJSON_GetObjectItemCaseSensitive(args, camel);
    return item;
}

int tf_audit_options_parse(tf_audit_options *opts, const cJSON *args, const char *context) {
    if (!opts) return TF_ERROR;
    if (!args) return TF_OK;

    const cJSON *include = audit_get_arg(args, "audit_include_row", "auditIncludeRow");
    if (include) {
        if (!cJSON_IsBool(include)) {
            char msg[256];
            snprintf(msg, sizeof(msg), "%s: audit_include_row must be true or false", context ? context : "audit");
            tf_set_last_error(msg);
            return TF_ERROR;
        }
        opts->include_row = cJSON_IsTrue(include) ? 1 : 0;
    }

    if (audit_parse_string_list_value(audit_get_arg(args, "audit_columns", "auditColumns"),
                                      &opts->columns, &opts->n_columns, context, "audit_columns") != TF_OK ||
        audit_parse_string_list_value(audit_get_arg(args, "audit_redact", "auditRedact"),
                                      &opts->redact_columns, &opts->n_redact_columns, context, "audit_redact") != TF_OK ||
        audit_parse_string_list_value(audit_get_arg(args, "audit_hash_columns", "auditHashColumns"),
                                      &opts->hash_columns, &opts->n_hash_columns, context, "audit_hash_columns") != TF_OK) {
        return TF_ERROR;
    }

    if (tf_json_get_size_arg(args, "audit_max_bytes", 0, TF_MAX_ERROR_BYTES, &opts->max_bytes, context) < 0 ||
        tf_json_get_size_arg(args, "auditMaxBytes", 0, TF_MAX_ERROR_BYTES, &opts->max_bytes, context) < 0 ||
        tf_json_get_size_arg(args, "audit_max_cell_bytes", 0, TF_MAX_ERROR_BYTES, &opts->max_cell_bytes, context) < 0 ||
        tf_json_get_size_arg(args, "auditMaxCellBytes", 0, TF_MAX_ERROR_BYTES, &opts->max_cell_bytes, context) < 0) {
        return TF_ERROR;
    }
    return TF_OK;
}

static int audit_name_in_list(char **items, size_t n_items, const char *name) {
    if (!name) return 0;
    for (size_t i = 0; i < n_items; i++) {
        if (items[i] && strcmp(items[i], name) == 0) return 1;
    }
    return 0;
}

int tf_audit_column_is_redacted(const tf_audit_options *opts, const char *column) {
    return opts ? audit_name_in_list(opts->redact_columns, opts->n_redact_columns, column) : 0;
}

int tf_audit_column_is_hashed(const tf_audit_options *opts, const char *column) {
    return opts ? audit_name_in_list(opts->hash_columns, opts->n_hash_columns, column) : 0;
}

int tf_audit_hash_string(const char *value, char *out, size_t out_size) {
    if (!out || out_size < 25) return TF_ERROR;
    uint64_t h = UINT64_C(1469598103934665603);
    const unsigned char *p = (const unsigned char *)(value ? value : "");
    while (*p) {
        h ^= (uint64_t)*p++;
        h *= UINT64_C(1099511628211);
    }
    snprintf(out, out_size, "fnv1a64:%016llx", (unsigned long long)h);
    return TF_OK;
}

const char *tf_audit_format_string_for_column(const tf_audit_options *opts, const char *column,
                                              const char *value, char *buf, size_t buf_size) {
    const char *src = value ? value : "";
    if (!opts || !buf || buf_size == 0) return src;
    if (tf_audit_column_is_redacted(opts, column)) {
        snprintf(buf, buf_size, "[REDACTED]");
        return buf;
    }
    if (tf_audit_column_is_hashed(opts, column)) {
        if (tf_audit_hash_string(src, buf, buf_size) == TF_OK) return buf;
        return "[HASHED]";
    }
    if (opts->max_cell_bytes > 0 && strlen(src) > opts->max_cell_bytes) {
        size_t n = opts->max_cell_bytes;
        if (n + 4 > buf_size) n = buf_size > 4 ? buf_size - 4 : 0;
        if (n > 0) memcpy(buf, src, n);
        snprintf(buf + n, buf_size - n, "...");
        return buf;
    }
    return src;
}

static cJSON *audit_string_json(const tf_audit_options *opts, const char *column, const char *value) {
    char buf[256];
    const char *safe = tf_audit_format_string_for_column(opts, column, value, buf, sizeof(buf));
    return cJSON_CreateString(safe ? safe : "");
}

cJSON *tf_audit_cell_to_json(const tf_batch *b, size_t row, size_t col, const tf_audit_options *opts) {
    const char *name = b->col_names[col] ? b->col_names[col] : "";
    if (tf_batch_is_null(b, row, col)) return cJSON_CreateNull();
    if (tf_audit_column_is_redacted(opts, name) || tf_audit_column_is_hashed(opts, name)) {
        char raw[128];
        switch (b->col_types[col]) {
            case TF_TYPE_BOOL:
                snprintf(raw, sizeof(raw), "%s", tf_batch_get_bool(b, row, col) ? "true" : "false");
                break;
            case TF_TYPE_INT64:
                snprintf(raw, sizeof(raw), "%lld", (long long)tf_batch_get_int64(b, row, col));
                break;
            case TF_TYPE_FLOAT64:
                if (tf_format_float64(raw, sizeof(raw), tf_batch_get_float64(b, row, col)) != TF_OK)
                    return NULL;
                break;
            case TF_TYPE_STRING:
                return audit_string_json(opts, name, tf_batch_get_string(b, row, col));
            case TF_TYPE_DATE:
                snprintf(raw, sizeof(raw), "%d", (int)tf_batch_get_date(b, row, col));
                break;
            case TF_TYPE_TIMESTAMP:
                snprintf(raw, sizeof(raw), "%lld", (long long)tf_batch_get_timestamp(b, row, col));
                break;
            default:
                raw[0] = '\0';
                break;
        }
        return audit_string_json(opts, name, raw);
    }
    switch (b->col_types[col]) {
        case TF_TYPE_BOOL: return cJSON_CreateBool(tf_batch_get_bool(b, row, col));
        case TF_TYPE_INT64: return cJSON_CreateNumber((double)tf_batch_get_int64(b, row, col));
        case TF_TYPE_FLOAT64: return cJSON_CreateNumber(tf_batch_get_float64(b, row, col));
        case TF_TYPE_STRING: return audit_string_json(opts, name, tf_batch_get_string(b, row, col));
        case TF_TYPE_DATE: return cJSON_CreateNumber((double)tf_batch_get_date(b, row, col));
        case TF_TYPE_TIMESTAMP: return cJSON_CreateNumber((double)tf_batch_get_timestamp(b, row, col));
        default: return cJSON_CreateNull();
    }
}

cJSON *tf_audit_row_to_json(const tf_batch *b, size_t row, const tf_audit_options *opts) {
    if (!b || row >= b->n_rows) return NULL;
    if (opts && !opts->include_row) return NULL;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return NULL;
    for (size_t c = 0; c < b->n_cols; c++) {
        const char *name = b->col_names[c] ? b->col_names[c] : "";
        if (opts && opts->n_columns > 0 && !audit_name_in_list(opts->columns, opts->n_columns, name)) continue;
        cJSON *value = tf_audit_cell_to_json(b, row, c, opts);
        if (!value) { cJSON_Delete(obj); return NULL; }
        cJSON_AddItemToObject(obj, name, value);
    }
    if (opts && opts->max_bytes > 0) {
        char *printed = cJSON_PrintUnformatted(obj);
        if (!printed) { cJSON_Delete(obj); return NULL; }
        size_t n = strlen(printed);
        free(printed);
        if (n > opts->max_bytes) {
            cJSON_Delete(obj);
            obj = cJSON_CreateObject();
            if (!obj) return NULL;
            cJSON_AddBoolToObject(obj, "_audit_truncated", 1);
            cJSON_AddNumberToObject(obj, "max_bytes", (double)opts->max_bytes);
        }
    }
    return obj;
}

void tf_batch_free(tf_batch *b) {
    if (!b) return;
    tf_arena *arena = b->arena;
    tf_arena_free(arena);
}
