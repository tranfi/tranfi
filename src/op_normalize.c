/*
 * op_normalize.c — Min-max or z-score normalization.
 * Aggregate op: buffers all rows, computes stats, then emits normalized.
 *
 * Config: {"columns": ["price", "score"], "method": "minmax"}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <math.h>

typedef enum {
    NORM_MINMAX,
    NORM_ZSCORE
} norm_method;

typedef enum {
    NORM_MISSING_ERROR,
    NORM_MISSING_NULL,
    NORM_MISSING_IGNORE
} norm_missing_policy;

typedef enum {
    NORM_TYPE_FAIL,
    NORM_TYPE_NULL
} norm_type_policy;

typedef struct {
    int     col_idx;
    int     missing_null;
    int     ignored;
    int     type_null;
    size_t  count;
    double  mean;
    double  m2;
    double  min_val;
    double  max_val;
} col_stats;

typedef struct {
    tf_batch *batch; /* single-row batch */
} buf_row;

typedef struct {
    char       **columns;
    size_t       n_columns;
    norm_method  method;
    norm_missing_policy missing;
    norm_type_policy on_type_error;
    col_stats   *stats;
    buf_row     *rows;
    size_t       n_rows;
    size_t       cap_rows;
    int          has_schema;
    size_t       schema_n_cols;
    char       **schema_names;
    tf_type     *schema_types;
    size_t       audit_limit;
    size_t       audit_emitted;
    int          audit;
    tf_audit_options audit_opts;
} normalize_state;

static int parse_method(const char *s, norm_method *out) {
    if (!s || strcmp(s, "minmax") == 0) { *out = NORM_MINMAX; return TF_OK; }
    if (strcmp(s, "zscore") == 0) { *out = NORM_ZSCORE; return TF_OK; }
    tf_set_last_error("normalize: method must be minmax or zscore");
    return TF_ERROR;
}

static const char *method_name(norm_method method) {
    return method == NORM_ZSCORE ? "zscore" : "minmax";
}

static int normalize_is_numeric(tf_type type) {
    return type == TF_TYPE_INT64 || type == TF_TYPE_FLOAT64;
}

static int normalize_parse_missing_policy(const cJSON *args, norm_missing_policy *out) {
    *out = NORM_MISSING_ERROR;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "missing");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("normalize: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "error") == 0) *out = NORM_MISSING_ERROR;
    else if (strcmp(j->valuestring, "null") == 0) *out = NORM_MISSING_NULL;
    else if (strcmp(j->valuestring, "ignore") == 0) *out = NORM_MISSING_IGNORE;
    else {
        tf_set_last_error("normalize: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    return TF_OK;
}

static int normalize_parse_type_policy(const cJSON *args, norm_type_policy *out) {
    *out = NORM_TYPE_FAIL;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "on_type_error");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("normalize: on_type_error must be fail or null");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "fail") == 0) *out = NORM_TYPE_FAIL;
    else if (strcmp(j->valuestring, "null") == 0) *out = NORM_TYPE_NULL;
    else {
        tf_set_last_error("normalize: on_type_error must be fail or null");
        return TF_ERROR;
    }
    return TF_OK;
}

static int normalize_set_col_error(normalize_state *st, const tf_batch *in, size_t i) {
    if (st->stats[i].col_idx < 0) {
        char msg[256];
        snprintf(msg, sizeof(msg), "normalize: column '%s' not found", st->columns[i]);
        tf_set_last_error(msg);
        return TF_ERROR;
    }
    if (!normalize_is_numeric(in->col_types[st->stats[i].col_idx])) {
        char msg[256];
        snprintf(msg, sizeof(msg), "normalize: column '%s' must be numeric", st->columns[i]);
        tf_set_last_error(msg);
        return TF_ERROR;
    }
    return TF_OK;
}

static double normalize_compute(norm_method method, const col_stats *cs, double val) {
    if (method == NORM_MINMAX) {
        double range = cs->max_val - cs->min_val;
        return (range > 0) ? (val - cs->min_val) / range : 0;
    }
    double std = (cs->count > 1) ? sqrt(cs->m2 / (double)(cs->count - 1)) : 1;
    return (std > 0) ? (val - cs->mean) / std : 0;
}

static double normalize_stddev(const col_stats *cs) {
    return (cs->count > 1) ? sqrt(cs->m2 / (double)(cs->count - 1)) : 0;
}

static int normalize_add_audit_number(cJSON *obj, const char *key,
                                      const tf_audit_options *opts,
                                      const char *column, double value) {
    if (opts && (tf_audit_column_is_redacted(opts, column) || tf_audit_column_is_hashed(opts, column))) {
        char raw[128];
        char safe_buf[256];
        if (tf_format_float64(raw, sizeof(raw), value) != TF_OK) return TF_ERROR;
        const char *safe = tf_audit_format_string_for_column(opts, column, raw, safe_buf, sizeof(safe_buf));
        return tf_json_add_string(obj, key, safe ? safe : "");
    }
    return tf_json_add_number(obj, key, value);
}

static double get_numeric(const tf_batch *b, size_t r, int ci) {
    if (b->col_types[ci] == TF_TYPE_INT64) return (double)tf_batch_get_int64(b, r, ci);
    if (b->col_types[ci] == TF_TYPE_FLOAT64) return tf_batch_get_float64(b, r, ci);
    return 0;
}

static int emit_normalize_audit(normalize_state *st, const tf_batch *before_b,
                                const tf_batch *after_b, size_t before_row,
                                size_t after_row, size_t col, size_t row_no,
                                const col_stats *cs, tf_side_channels *side) {
    if (!st->audit || st->audit_emitted >= st->audit_limit || !side || !side->stats) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    int rc = TF_ERROR;
    if (tf_json_add_string(obj, "type", "audit") != TF_OK ||
        tf_json_add_string(obj, "op", "normalize") != TF_OK ||
        tf_json_add_string(obj, "event", "value_changed") != TF_OK ||
        tf_json_add_string(obj, "reason",
                           st->method == NORM_ZSCORE ? "normalize_zscore" : "normalize_minmax") != TF_OK ||
        tf_json_add_string(obj, "channel", "audit") != TF_OK) {
        goto done;
    }
    const char *column_name = after_b->col_names[col] ? after_b->col_names[col] : "";
    if (tf_json_add_string(obj, "column", column_name) != TF_OK ||
        tf_json_add_string(obj, "method", method_name(st->method)) != TF_OK ||
        tf_json_add_number(obj, "row", (double)row_no) != TF_OK) {
        goto done;
    }
    cJSON *before = tf_audit_cell_to_json(before_b, before_row, col, &st->audit_opts);
    if (!before || tf_json_add_item(obj, "before", before) != TF_OK) goto done;
    cJSON *after = tf_audit_cell_to_json(after_b, after_row, col, &st->audit_opts);
    if (!after || tf_json_add_item(obj, "after", after) != TF_OK) goto done;
    if (tf_json_add_number(obj, "count", (double)cs->count) != TF_OK) goto done;
    if (normalize_add_audit_number(obj, "min", &st->audit_opts, column_name, cs->min_val) != TF_OK ||
        normalize_add_audit_number(obj, "max", &st->audit_opts, column_name, cs->max_val) != TF_OK ||
        normalize_add_audit_number(obj, "mean", &st->audit_opts, column_name, cs->mean) != TF_OK ||
        normalize_add_audit_number(obj, "stddev", &st->audit_opts, column_name, normalize_stddev(cs)) != TF_OK) {
        goto done;
    }
    cJSON *row_obj = tf_audit_row_to_json(after_b, after_row, &st->audit_opts);
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

static int add_row(normalize_state *st, tf_batch *b, size_t r) {
    if (st->n_rows >= st->cap_rows) {
        size_t min_cap = 0, newcap = 0;
        if (tf_size_add(st->n_rows, 1, &min_cap) != TF_OK ||
            tf_size_grow_pow2(st->cap_rows, min_cap, 256, &newcap) != TF_OK) {
            return TF_ERROR;
        }
        buf_row *tmp = tf_reallocarray_checked(st->rows, newcap, sizeof(buf_row));
        if (!tmp) return TF_ERROR;
        st->rows = tmp;
        st->cap_rows = newcap;
    }
    tf_batch *rb = tf_batch_create(b->n_cols, 1);
    if (!rb) return TF_ERROR;
    if (tf_batch_clone_schema(rb, b) != TF_OK) {
        tf_batch_free(rb);
        return TF_ERROR;
    }
    if (tf_batch_copy_row(rb, 0, b, r) != TF_OK) {
        tf_batch_free(rb);
        return TF_ERROR;
    }
    if (tf_batch_expose_row(rb, 0) != TF_OK) {
        tf_batch_free(rb);
        return TF_ERROR;
    }
    st->rows[st->n_rows].batch = rb;
    st->n_rows++;
    return TF_OK;
}

static int normalize_capture_schema(normalize_state *st, const tf_batch *in) {
    st->schema_n_cols = in->n_cols;
    st->schema_names = tf_callocarray_checked(in->n_cols, sizeof(char *));
    st->schema_types = tf_callocarray_checked(in->n_cols, sizeof(tf_type));
    if (!st->schema_names || !st->schema_types) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++) {
        st->schema_names[c] = strdup(in->col_names[c] ? in->col_names[c] : "");
        if (!st->schema_names[c]) return TF_ERROR;
        st->schema_types[c] = in->col_types[c];
    }
    st->has_schema = 1;
    return TF_OK;
}

static int normalize_resolve_columns(normalize_state *st, const tf_batch *in) {
    for (size_t i = 0; i < st->n_columns; i++) {
        col_stats *cs = &st->stats[i];
        cs->col_idx = tf_batch_col_index(in, st->columns[i]);
        if (cs->col_idx < 0) {
            if (st->missing == NORM_MISSING_ERROR) return normalize_set_col_error(st, in, i);
            if (st->missing == NORM_MISSING_IGNORE) cs->ignored = 1;
            else cs->missing_null = 1;
            continue;
        }
        if (!normalize_is_numeric(in->col_types[cs->col_idx])) {
            if (st->on_type_error == NORM_TYPE_FAIL) return normalize_set_col_error(st, in, i);
            cs->type_null = 1;
        }
    }
    return TF_OK;
}

static int normalize_process(tf_step *self, tf_batch *in, tf_batch **out,
                             tf_side_channels *side) {
    (void)side;
    normalize_state *st = self->state;
    *out = NULL;

    if (!st->has_schema) {
        if (normalize_capture_schema(st, in) != TF_OK) return TF_ERROR;
        if (normalize_resolve_columns(st, in) != TF_OK) return TF_ERROR;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        if (add_row(st, in, r) != TF_OK) return TF_ERROR;

        for (size_t i = 0; i < st->n_columns; i++) {
            col_stats *cs = &st->stats[i];
            int ci = cs->col_idx;
            if (cs->ignored || cs->missing_null || cs->type_null || ci < 0 || tf_batch_is_null(in, r, (size_t)ci)) continue;

            double val = get_numeric(in, r, ci);
            cs->count++;
            double delta = val - cs->mean;
            cs->mean += delta / (double)cs->count;
            double delta2 = val - cs->mean;
            cs->m2 += delta * delta2;

            if (cs->count == 1 || val < cs->min_val) cs->min_val = val;
            if (cs->count == 1 || val > cs->max_val) cs->max_val = val;
        }
    }

    return TF_OK;
}

static size_t normalize_missing_null_count(const normalize_state *st) {
    size_t n = 0;
    for (size_t i = 0; i < st->n_columns; i++) {
        if (st->stats[i].missing_null) n++;
    }
    return n;
}

static int normalize_is_output_norm_col(const normalize_state *st, size_t col) {
    for (size_t i = 0; i < st->n_columns; i++) {
        const col_stats *cs = &st->stats[i];
        if (cs->col_idx == (int)col && !cs->ignored) return 1;
    }
    return 0;
}

static int normalize_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    normalize_state *st = self->state;
    *out = NULL;

    if (st->n_rows == 0) return TF_OK;

    size_t extra_cols = normalize_missing_null_count(st);
    size_t out_cols = 0;
    if (tf_size_add(st->schema_n_cols, extra_cols, &out_cols) != TF_OK) return TF_ERROR;
    tf_batch *ob = tf_batch_create(out_cols, st->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < st->schema_n_cols; c++) {
        tf_type type = st->schema_types[c];
        if (normalize_is_output_norm_col(st, c)) type = TF_TYPE_FLOAT64;
        if (tf_batch_set_schema(ob, c, st->schema_names[c], type) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    size_t extra = 0;
    for (size_t i = 0; i < st->n_columns; i++) {
        if (!st->stats[i].missing_null) continue;
        if (tf_batch_set_schema(ob, st->schema_n_cols + extra, st->columns[i], TF_TYPE_FLOAT64) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        extra++;
    }

    for (size_t r = 0; r < st->n_rows; r++) {
        tf_batch *rb = st->rows[r].batch;
        for (size_t c = 0; c < st->schema_n_cols; c++) {
            if (normalize_is_output_norm_col(st, c)) continue;
            if (tf_batch_copy_cell(ob, r, c, rb, 0, c) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
        }

        for (size_t i = 0; i < st->n_columns; i++) {
            col_stats *cs = &st->stats[i];
            int ci = cs->col_idx;
            if (cs->ignored || cs->missing_null || ci < 0) continue;
            if (cs->type_null) {
                if (tf_batch_set_null(ob, r, (size_t)ci) != TF_OK) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                continue;
            }
            if (tf_batch_is_null(rb, 0, (size_t)ci)) continue;

            double val = get_numeric(rb, 0, ci);
            double norm = normalize_compute(st->method, cs, val);
            if (tf_batch_set_float64(ob, r, (size_t)ci, norm) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }

        if (st->audit) {
            for (size_t i = 0; i < st->n_columns; i++) {
                col_stats *cs = &st->stats[i];
                int ci = cs->col_idx;
                if (cs->ignored || cs->missing_null || cs->type_null || ci < 0 || tf_batch_is_null(rb, 0, (size_t)ci)) continue;

                double val = get_numeric(rb, 0, ci);
                double norm = normalize_compute(st->method, cs, val);
                if (val == norm) continue;
                if (emit_normalize_audit(st, rb, ob, 0, r, (size_t)ci, r + 1, cs, side) != TF_OK) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
            }
        }
    }

    *out = ob;
    return TF_OK;
}

static void normalize_state_free(normalize_state *st) {
    if (!st) return;
    tf_audit_options_free(&st->audit_opts);
    if (st->columns) {
        for (size_t i = 0; i < st->n_columns; i++)
            free(st->columns[i]);
    }
    free(st->columns);
    free(st->stats);
    if (st->rows) {
        for (size_t i = 0; i < st->n_rows; i++)
            tf_batch_free(st->rows[i].batch);
    }
    free(st->rows);
    if (st->schema_names) {
        for (size_t c = 0; c < st->schema_n_cols; c++)
            free(st->schema_names[c]);
        free(st->schema_names);
    }
    free(st->schema_types);
    free(st);
}

static void normalize_destroy(tf_step *self) {
    if (self) normalize_state_free(self->state);
    free(self);
}

tf_step *tf_normalize_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *cols_j = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!cols_j || !cJSON_IsArray(cols_j)) {
        tf_set_last_error("normalize: columns are required");
        return NULL;
    }

    int n = cJSON_GetArraySize(cols_j);
    if (n == 0) {
        tf_set_last_error("normalize: columns are required");
        return NULL;
    }

    normalize_state *st = calloc(1, sizeof(normalize_state));
    if (!st) return NULL;
    tf_audit_options_init(&st->audit_opts, 1);

    st->audit_limit = 1000;
    st->n_columns = (size_t)n;
    st->columns = tf_callocarray_checked((size_t)n, sizeof(char *));
    st->stats = tf_callocarray_checked((size_t)n, sizeof(col_stats));
    if (!st->columns || !st->stats) { normalize_state_free(st); return NULL; }
    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(cols_j, i);
        if (!cJSON_IsString(item) || !item->valuestring[0]) {
            tf_set_last_error("normalize: columns must be non-empty strings");
            normalize_state_free(st);
            return NULL;
        }
        st->columns[i] = strdup(item->valuestring);
        if (!st->columns[i]) { normalize_state_free(st); return NULL; }
        st->stats[i].col_idx = -1;
    }

    cJSON *method_j = cJSON_GetObjectItemCaseSensitive(args, "method");
    if (method_j && !cJSON_IsString(method_j)) {
        tf_set_last_error("normalize: method must be minmax or zscore");
        normalize_state_free(st);
        return NULL;
    }
    if (parse_method(cJSON_IsString(method_j) ? method_j->valuestring : NULL, &st->method) != TF_OK ||
        normalize_parse_missing_policy(args, &st->missing) != TF_OK ||
        normalize_parse_type_policy(args, &st->on_type_error) != TF_OK) {
        normalize_state_free(st);
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
                                     &parsed_limit, "normalize") < 0) {
            normalize_state_free(st);
            return NULL;
        }
        st->audit_limit = parsed_limit;
    }

    if (tf_audit_options_parse(&st->audit_opts, args, "normalize") != TF_OK) {
        normalize_state_free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { normalize_state_free(st); return NULL; }
    step->process = normalize_process;
    step->flush = normalize_flush;
    step->destroy = normalize_destroy;
    step->state = st;
    return step;
}
