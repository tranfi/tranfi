/*
 * op_assert.c -- Data-quality assertions.
 *
 * Row mode:
 *   {"expr":"col('age') >= 0", "action":"fail|warn|filter|quarantine|annotate"}
 * Aggregate mode:
 *   {"aggregate":"count|sum:col|avg:col|min:col|max:col|missing:col|non_null:col",
 *    "op":">=", "value":1, "action":"fail|warn"}
 */

#include "internal.h"
#include "cJSON.h"
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef enum {
    ASSERT_FAIL = 0,
    ASSERT_WARN,
    ASSERT_FILTER,
    ASSERT_QUARANTINE,
    ASSERT_ANNOTATE,
} assert_action;

typedef enum {
    ASSERT_MODE_ROW = 0,
    ASSERT_MODE_AGGREGATE,
} assert_mode;

typedef enum {
    ASSERT_AGG_COUNT = 0,
    ASSERT_AGG_NON_NULL,
    ASSERT_AGG_MISSING,
    ASSERT_AGG_SUM,
    ASSERT_AGG_AVG,
    ASSERT_AGG_MIN,
    ASSERT_AGG_MAX,
} assert_agg_kind;

typedef enum {
    ASSERT_CMP_EQ = 0,
    ASSERT_CMP_NE,
    ASSERT_CMP_LT,
    ASSERT_CMP_LTE,
    ASSERT_CMP_GT,
    ASSERT_CMP_GTE,
} assert_cmp;

typedef struct {
    tf_expr       *expr;
    char          *expr_text;
    char          *name;
    char          *message;
    char          *result;
    assert_action  action;
    assert_mode    mode;
    size_t         row_index;
    size_t         failures;
    size_t         audit_limit;
    size_t         audit_emitted;
    int            audit;

    assert_agg_kind agg_kind;
    assert_cmp      cmp;
    char           *agg_text;
    char           *agg_col;
    int             agg_col_index;
    int             agg_col_resolved;
    double          threshold;
    size_t          agg_rows;
    size_t          agg_non_null;
    size_t          agg_missing;
    double          agg_sum;
    double          agg_min;
    double          agg_max;
    int             agg_has_numeric;
    int             agg_evaluated;
    int             agg_passed;
} assert_state;

static const char *assert_action_name(assert_action action) {
    switch (action) {
        case ASSERT_FAIL: return "fail";
        case ASSERT_WARN: return "warn";
        case ASSERT_FILTER: return "filter";
        case ASSERT_QUARANTINE: return "quarantine";
        case ASSERT_ANNOTATE: return "annotate";
        default: return "fail";
    }
}

static const char *assert_agg_kind_name(assert_agg_kind kind) {
    switch (kind) {
        case ASSERT_AGG_COUNT: return "count";
        case ASSERT_AGG_NON_NULL: return "non_null";
        case ASSERT_AGG_MISSING: return "missing";
        case ASSERT_AGG_SUM: return "sum";
        case ASSERT_AGG_AVG: return "avg";
        case ASSERT_AGG_MIN: return "min";
        case ASSERT_AGG_MAX: return "max";
        default: return "count";
    }
}

static const char *assert_cmp_name(assert_cmp cmp) {
    switch (cmp) {
        case ASSERT_CMP_EQ: return "==";
        case ASSERT_CMP_NE: return "!=";
        case ASSERT_CMP_LT: return "<";
        case ASSERT_CMP_LTE: return "<=";
        case ASSERT_CMP_GT: return ">";
        case ASSERT_CMP_GTE: return ">=";
        default: return "==";
    }
}

static int parse_action(const char *s, assert_action *out) {
    if (!s || strcmp(s, "fail") == 0 || strcmp(s, "error") == 0 || strcmp(s, "stop") == 0) {
        *out = ASSERT_FAIL;
        return 1;
    }
    if (strcmp(s, "warn") == 0 || strcmp(s, "warning") == 0) { *out = ASSERT_WARN; return 1; }
    if (strcmp(s, "filter") == 0 || strcmp(s, "drop") == 0) { *out = ASSERT_FILTER; return 1; }
    if (strcmp(s, "quarantine") == 0) { *out = ASSERT_QUARANTINE; return 1; }
    if (strcmp(s, "annotate") == 0) { *out = ASSERT_ANNOTATE; return 1; }
    return 0;
}

static int parse_cmp(const char *s, assert_cmp *out) {
    if (!s || !s[0]) return 0;
    if (strcmp(s, "==") == 0 || strcmp(s, "=") == 0 || strcmp(s, "eq") == 0) { *out = ASSERT_CMP_EQ; return 1; }
    if (strcmp(s, "!=") == 0 || strcmp(s, "ne") == 0) { *out = ASSERT_CMP_NE; return 1; }
    if (strcmp(s, "<") == 0 || strcmp(s, "lt") == 0) { *out = ASSERT_CMP_LT; return 1; }
    if (strcmp(s, "<=") == 0 || strcmp(s, "lte") == 0 || strcmp(s, "le") == 0) { *out = ASSERT_CMP_LTE; return 1; }
    if (strcmp(s, ">") == 0 || strcmp(s, "gt") == 0) { *out = ASSERT_CMP_GT; return 1; }
    if (strcmp(s, ">=") == 0 || strcmp(s, "gte") == 0 || strcmp(s, "ge") == 0) { *out = ASSERT_CMP_GTE; return 1; }
    return 0;
}

static int parse_agg_kind_name(const char *s, assert_agg_kind *out) {
    if (!s || !s[0]) return 0;
    if (strcmp(s, "count") == 0 || strcmp(s, "n") == 0 || strcmp(s, "rows") == 0) { *out = ASSERT_AGG_COUNT; return 1; }
    if (strcmp(s, "non_null") == 0 || strcmp(s, "non-null") == 0 || strcmp(s, "complete") == 0 || strcmp(s, "present") == 0) { *out = ASSERT_AGG_NON_NULL; return 1; }
    if (strcmp(s, "missing") == 0 || strcmp(s, "null") == 0 || strcmp(s, "nulls") == 0) { *out = ASSERT_AGG_MISSING; return 1; }
    if (strcmp(s, "sum") == 0) { *out = ASSERT_AGG_SUM; return 1; }
    if (strcmp(s, "avg") == 0 || strcmp(s, "mean") == 0) { *out = ASSERT_AGG_AVG; return 1; }
    if (strcmp(s, "min") == 0) { *out = ASSERT_AGG_MIN; return 1; }
    if (strcmp(s, "max") == 0) { *out = ASSERT_AGG_MAX; return 1; }
    return 0;
}

static int agg_needs_column(assert_agg_kind kind) {
    return kind != ASSERT_AGG_COUNT;
}

static int agg_needs_numeric(assert_agg_kind kind) {
    return kind == ASSERT_AGG_SUM || kind == ASSERT_AGG_AVG ||
           kind == ASSERT_AGG_MIN || kind == ASSERT_AGG_MAX;
}

static int parse_number_arg(const cJSON *item, double *out) {
    if (cJSON_IsNumber(item)) { *out = item->valuedouble; return 1; }
    if (cJSON_IsString(item) && item->valuestring && item->valuestring[0]) {
        char *end = NULL;
        double v = strtod(item->valuestring, &end);
        if (end && *end == '\0') { *out = v; return 1; }
    }
    return 0;
}

static int parse_aggregate_spec(const cJSON *args, assert_agg_kind *kind, char **agg_text, char **column) {
    cJSON *agg_json = cJSON_GetObjectItemCaseSensitive(args, "aggregate");
    if (!agg_json) agg_json = cJSON_GetObjectItemCaseSensitive(args, "agg");
    if (!cJSON_IsString(agg_json) || !agg_json->valuestring || !agg_json->valuestring[0]) return 0;

    char *spec = strdup(agg_json->valuestring);
    if (!spec) return -1;
    char *colon = strchr(spec, ':');
    if (colon) *colon++ = '\0';
    if (!parse_agg_kind_name(spec, kind)) {
        free(spec);
        return -2;
    }

    cJSON *col_json = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!col_json) col_json = cJSON_GetObjectItemCaseSensitive(args, "col");
    const char *col = cJSON_IsString(col_json) && col_json->valuestring && col_json->valuestring[0]
        ? col_json->valuestring : colon;
    int has_col = col && col[0];

    *agg_text = strdup(agg_json->valuestring);
    *column = has_col ? strdup(col) : NULL;
    free(spec);
    if (!*agg_text || (has_col && !*column)) {
        free(*agg_text); *agg_text = NULL;
        free(*column); *column = NULL;
        return -1;
    }
    return 1;
}

static int compare_aggregate(double actual, double threshold, assert_cmp cmp) {
    const double eps = 1e-12;
    switch (cmp) {
        case ASSERT_CMP_EQ: return fabs(actual - threshold) <= eps;
        case ASSERT_CMP_NE: return fabs(actual - threshold) > eps;
        case ASSERT_CMP_LT: return actual < threshold;
        case ASSERT_CMP_LTE: return actual <= threshold || fabs(actual - threshold) <= eps;
        case ASSERT_CMP_GT: return actual > threshold;
        case ASSERT_CMP_GTE: return actual >= threshold || fabs(actual - threshold) <= eps;
        default: return 0;
    }
}

static int aggregate_value(const assert_state *st, double *out) {
    switch (st->agg_kind) {
        case ASSERT_AGG_COUNT: *out = (double)st->agg_rows; return 1;
        case ASSERT_AGG_NON_NULL: *out = (double)st->agg_non_null; return 1;
        case ASSERT_AGG_MISSING: *out = (double)st->agg_missing; return 1;
        case ASSERT_AGG_SUM:
            if (!st->agg_has_numeric) return 0;
            *out = st->agg_sum;
            return 1;
        case ASSERT_AGG_AVG:
            if (!st->agg_has_numeric || st->agg_non_null == 0) return 0;
            *out = st->agg_sum / (double)st->agg_non_null;
            return 1;
        case ASSERT_AGG_MIN:
            if (!st->agg_has_numeric) return 0;
            *out = st->agg_min;
            return 1;
        case ASSERT_AGG_MAX:
            if (!st->agg_has_numeric) return 0;
            *out = st->agg_max;
            return 1;
        default:
            return 0;
    }
}

static int append_json_string_field(tf_buffer *out, const char *field, const char *value) {
    cJSON *s = cJSON_CreateString(value ? value : "");
    if (!s) return TF_ERROR;
    char *printed = cJSON_PrintUnformatted(s);
    cJSON_Delete(s);
    if (!printed) return TF_ERROR;
    int rc = tf_buffer_write_str(out, ",\"");
    if (rc == TF_OK) rc = tf_buffer_write_str(out, field);
    if (rc == TF_OK) rc = tf_buffer_write_str(out, "\":");
    if (rc == TF_OK) rc = tf_buffer_write_str(out, printed);
    free(printed);
    return rc;
}

static cJSON *batch_cell_to_json(const tf_batch *b, size_t row, size_t col) {
    if (tf_batch_is_null(b, row, col)) return cJSON_CreateNull();
    switch (b->col_types[col]) {
        case TF_TYPE_BOOL:
            return cJSON_CreateBool(tf_batch_get_bool(b, row, col));
        case TF_TYPE_INT64:
            return cJSON_CreateNumber((double)tf_batch_get_int64(b, row, col));
        case TF_TYPE_FLOAT64:
            return cJSON_CreateNumber(tf_batch_get_float64(b, row, col));
        case TF_TYPE_STRING:
            return cJSON_CreateString(tf_batch_get_string(b, row, col));
        case TF_TYPE_DATE:
            return cJSON_CreateNumber((double)tf_batch_get_date(b, row, col));
        case TF_TYPE_TIMESTAMP:
            return cJSON_CreateNumber((double)tf_batch_get_timestamp(b, row, col));
        default:
            return cJSON_CreateNull();
    }
}

static cJSON *batch_row_to_json(const tf_batch *b, size_t row) {
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return NULL;
    for (size_t c = 0; c < b->n_cols; c++) {
        cJSON *value = batch_cell_to_json(b, row, c);
        if (!value) { cJSON_Delete(obj); return NULL; }
        cJSON_AddItemToObject(obj, b->col_names[c] ? b->col_names[c] : "", value);
    }
    return obj;
}

static int emit_assert_audit(assert_state *st, const tf_batch *b, size_t row,
                             tf_side_channels *side, const char *event) {
    if (!st->audit || st->audit_emitted >= st->audit_limit || !side || !side->stats) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    cJSON_AddStringToObject(obj, "type", "audit");
    cJSON_AddStringToObject(obj, "op", "assert");
    cJSON_AddStringToObject(obj, "event", event ? event : "row_dropped");
    cJSON_AddStringToObject(obj, "reason", "assert_failed");
    cJSON_AddStringToObject(obj, "channel", "audit");
    cJSON_AddStringToObject(obj, "action", assert_action_name(st->action));
    cJSON_AddStringToObject(obj, "name", st->name ? st->name : "assert");
    cJSON_AddStringToObject(obj, "expr", st->expr_text ? st->expr_text : "");
    cJSON_AddNumberToObject(obj, "row", (double)st->row_index);
    if (st->message && st->message[0]) cJSON_AddStringToObject(obj, "message", st->message);
    cJSON *row_obj = batch_row_to_json(b, row);
    if (row_obj) cJSON_AddItemToObject(obj, "data", row_obj);
    char *line = cJSON_PrintUnformatted(obj);
    cJSON_Delete(obj);
    if (!line) return TF_ERROR;
    int rc = tf_buffer_write_str(side->stats, line);
    if (rc == TF_OK) rc = tf_buffer_write_str(side->stats, "\n");
    free(line);
    if (rc == TF_OK) st->audit_emitted++;
    return rc;
}

static void emit_failure(assert_state *st, const tf_batch *b, size_t row,
                         tf_side_channels *side, int include_row) {
    if (!side || !side->errors) return;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return;
    cJSON_AddStringToObject(obj, "type", "assert_failure");
    cJSON_AddStringToObject(obj, "op", "assert");
    cJSON_AddStringToObject(obj, "action", assert_action_name(st->action));
    cJSON_AddStringToObject(obj, "severity", st->action == ASSERT_WARN ? "warning" : "error");
    cJSON_AddStringToObject(obj, "name", st->name ? st->name : "assert");
    cJSON_AddStringToObject(obj, "expr", st->expr_text ? st->expr_text : "");
    cJSON_AddNumberToObject(obj, "row", (double)st->row_index);
    if (st->message && st->message[0]) cJSON_AddStringToObject(obj, "message", st->message);
    if (include_row) {
        cJSON *row_obj = batch_row_to_json(b, row);
        if (row_obj) cJSON_AddItemToObject(obj, "data", row_obj);
    }
    char *line = cJSON_PrintUnformatted(obj);
    if (line) {
        tf_buffer_write_str(side->errors, line);
        tf_buffer_write_str(side->errors, "\n");
        free(line);
    }
    cJSON_Delete(obj);
}

static void emit_aggregate_failure(assert_state *st, tf_side_channels *side, double actual, int has_actual) {
    if (!side || !side->errors) return;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return;
    cJSON_AddStringToObject(obj, "type", "assert_failure");
    cJSON_AddStringToObject(obj, "op", "assert");
    cJSON_AddStringToObject(obj, "reason", "aggregate_assert_failed");
    cJSON_AddStringToObject(obj, "action", assert_action_name(st->action));
    cJSON_AddStringToObject(obj, "severity", st->action == ASSERT_WARN ? "warning" : "error");
    cJSON_AddStringToObject(obj, "name", st->name ? st->name : "assert");
    cJSON_AddStringToObject(obj, "aggregate", assert_agg_kind_name(st->agg_kind));
    if (st->agg_col && st->agg_col[0]) cJSON_AddStringToObject(obj, "column", st->agg_col);
    cJSON_AddStringToObject(obj, "comparison", assert_cmp_name(st->cmp));
    cJSON_AddNumberToObject(obj, "threshold", st->threshold);
    if (has_actual) cJSON_AddNumberToObject(obj, "actual", actual);
    else cJSON_AddNullToObject(obj, "actual");
    cJSON_AddNumberToObject(obj, "rows", (double)st->agg_rows);
    cJSON_AddNumberToObject(obj, "non_null", (double)st->agg_non_null);
    cJSON_AddNumberToObject(obj, "missing", (double)st->agg_missing);
    if (st->message && st->message[0]) cJSON_AddStringToObject(obj, "message", st->message);
    char *line = cJSON_PrintUnformatted(obj);
    if (line) {
        tf_buffer_write_str(side->errors, line);
        tf_buffer_write_str(side->errors, "\n");
        free(line);
    }
    cJSON_Delete(obj);
}

static int copy_schema(tf_batch *dst, const tf_batch *src, size_t extra_cols, const char *result) {
    for (size_t c = 0; c < src->n_cols; c++) {
        if (tf_batch_set_schema(dst, c, src->col_names[c], src->col_types[c]) != TF_OK) return TF_ERROR;
    }
    if (extra_cols > 0) {
        if (tf_batch_set_schema(dst, src->n_cols, result && result[0] ? result : "_assert", TF_TYPE_BOOL) != TF_OK)
            return TF_ERROR;
    }
    return TF_OK;
}

static int resolve_aggregate_column(assert_state *st, const tf_batch *in) {
    if (!agg_needs_column(st->agg_kind) || st->agg_col_resolved) return TF_OK;
    int idx = tf_batch_col_index(in, st->agg_col);
    if (idx < 0) {
        char msg[256];
        snprintf(msg, sizeof(msg), "assert: aggregate column not found: %s", st->agg_col ? st->agg_col : "");
        tf_set_last_error(msg);
        return TF_ERROR;
    }
    if (agg_needs_numeric(st->agg_kind) &&
        in->col_types[idx] != TF_TYPE_INT64 && in->col_types[idx] != TF_TYPE_FLOAT64) {
        char msg[256];
        snprintf(msg, sizeof(msg), "assert: aggregate '%s' requires numeric column '%s'",
                 assert_agg_kind_name(st->agg_kind), st->agg_col ? st->agg_col : "");
        tf_set_last_error(msg);
        return TF_ERROR;
    }
    st->agg_col_index = idx;
    st->agg_col_resolved = 1;
    return TF_OK;
}

static int update_aggregate(assert_state *st, const tf_batch *in) {
    if (resolve_aggregate_column(st, in) != TF_OK) return TF_ERROR;
    for (size_t r = 0; r < in->n_rows; r++) {
        st->agg_rows++;
        if (st->agg_kind == ASSERT_AGG_COUNT) continue;
        size_t c = (size_t)st->agg_col_index;
        if (tf_batch_is_null(in, r, c)) {
            st->agg_missing++;
            continue;
        }
        st->agg_non_null++;
        if (!agg_needs_numeric(st->agg_kind)) continue;
        double v = in->col_types[c] == TF_TYPE_INT64
            ? (double)tf_batch_get_int64(in, r, c)
            : tf_batch_get_float64(in, r, c);
        st->agg_sum += v;
        if (!st->agg_has_numeric) {
            st->agg_min = v;
            st->agg_max = v;
            st->agg_has_numeric = 1;
        } else {
            if (v < st->agg_min) st->agg_min = v;
            if (v > st->agg_max) st->agg_max = v;
        }
    }
    return TF_OK;
}

static int assert_process_aggregate(tf_step *self, tf_batch *in, tf_batch **out) {
    assert_state *st = self->state;
    *out = NULL;
    if (update_aggregate(st, in) != TF_OK) return TF_ERROR;

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) return TF_ERROR;
    if (copy_schema(ob, in, 0, NULL) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    ob->n_rows = in->n_rows;
    *out = ob;
    return TF_OK;
}

static int assert_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    assert_state *st = self->state;
    *out = NULL;

    if (st->mode == ASSERT_MODE_AGGREGATE) return assert_process_aggregate(self, in, out);

    size_t extra = st->action == ASSERT_ANNOTATE ? 1u : 0u;
    tf_batch *ob = tf_batch_create(in->n_cols + extra, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) return TF_ERROR;
    if (copy_schema(ob, in, extra, st->result) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }

    size_t out_row = 0;
    for (size_t r = 0; r < in->n_rows; r++) {
        st->row_index++;
        bool ok = false;
        if (tf_expr_eval(st->expr, in, r, &ok) != TF_OK) ok = false;

        if (!ok) {
            st->failures++;
            if (st->action == ASSERT_FAIL) {
                emit_failure(st, in, r, side, 1);
                char msg[512];
                snprintf(msg, sizeof(msg), "assert failed at row %zu: %s", st->row_index,
                         st->message && st->message[0] ? st->message : (st->expr_text ? st->expr_text : "rule failed"));
                tf_set_last_error(msg);
                tf_batch_free(ob);
                return TF_ERROR;
            }
            if (st->action == ASSERT_WARN) {
                emit_failure(st, in, r, side, 1);
            } else if (st->action == ASSERT_QUARANTINE) {
                emit_failure(st, in, r, side, 1);
                continue;
            } else if (st->action == ASSERT_FILTER) {
                if (emit_assert_audit(st, in, r, side, "row_dropped") != TF_OK) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                continue;
            }
        }

        if (tf_batch_copy_row(ob, out_row, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (st->action == ASSERT_ANNOTATE) tf_batch_set_bool(ob, out_row, in->n_cols, ok);
        out_row++;
    }

    ob->n_rows = out_row;
    if (ob->n_rows > 0 || st->action == ASSERT_ANNOTATE || in->n_rows == 0) *out = ob;
    else tf_batch_free(ob);
    return TF_OK;
}

static int assert_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    assert_state *st = self ? self->state : NULL;
    *out = NULL;
    if (!st || st->mode != ASSERT_MODE_AGGREGATE) return TF_OK;

    double actual = 0.0;
    int has_actual = aggregate_value(st, &actual);
    st->agg_evaluated = 1;
    st->agg_passed = has_actual && compare_aggregate(actual, st->threshold, st->cmp);
    if (st->agg_passed) return TF_OK;

    st->failures = 1;
    emit_aggregate_failure(st, side, actual, has_actual);
    if (st->action == ASSERT_WARN) return TF_OK;

    char msg[512];
    if (has_actual) {
        snprintf(msg, sizeof(msg), "assert aggregate failed: %s%s %s %.17g (actual %.17g)",
                 assert_agg_kind_name(st->agg_kind), st->agg_col ? ":" : "",
                 assert_cmp_name(st->cmp), st->threshold, actual);
    } else {
        snprintf(msg, sizeof(msg), "assert aggregate failed: %s%s has no numeric values",
                 assert_agg_kind_name(st->agg_kind), st->agg_col ? ":" : "");
    }
    tf_set_last_error(msg);
    return TF_ERROR;
}

static int assert_append_stats(tf_step *self, tf_buffer *out) {
    if (!self || !self->state || !out) return TF_ERROR;
    assert_state *st = self->state;
    if (st->mode != ASSERT_MODE_AGGREGATE) return TF_OK;
    double actual = 0.0;
    int has_actual = aggregate_value(st, &actual);
    if (append_json_string_field(out, "assert_mode", "aggregate") != TF_OK) return TF_ERROR;
    if (append_json_string_field(out, "aggregate", assert_agg_kind_name(st->agg_kind)) != TF_OK) return TF_ERROR;
    if (st->agg_col && append_json_string_field(out, "aggregate_column", st->agg_col) != TF_OK) return TF_ERROR;
    if (append_json_string_field(out, "comparison", assert_cmp_name(st->cmp)) != TF_OK) return TF_ERROR;
    char buf[320];
    snprintf(buf, sizeof(buf), ",\"threshold\":%.17g,\"aggregate_rows\":%zu,\"aggregate_non_null\":%zu,\"aggregate_missing\":%zu,\"aggregate_passed\":%s",
             st->threshold, st->agg_rows, st->agg_non_null, st->agg_missing,
             st->agg_evaluated && st->agg_passed ? "true" : "false");
    if (tf_buffer_write_str(out, buf) != TF_OK) return TF_ERROR;
    if (has_actual) snprintf(buf, sizeof(buf), ",\"aggregate_value\":%.17g", actual);
    else snprintf(buf, sizeof(buf), ",\"aggregate_value\":null");
    return tf_buffer_write_str(out, buf);
}

static void assert_destroy(tf_step *self) {
    if (!self) return;
    assert_state *st = self->state;
    if (st) {
        tf_expr_free(st->expr);
        free(st->expr_text);
        free(st->name);
        free(st->message);
        free(st->result);
        free(st->agg_text);
        free(st->agg_col);
        free(st);
    }
    free(self);
}

static void assert_state_free(assert_state *st) {
    if (!st) return;
    tf_expr_free(st->expr);
    free(st->expr_text);
    free(st->name);
    free(st->message);
    free(st->result);
    free(st->agg_text);
    free(st->agg_col);
    free(st);
}

tf_step *tf_assert_create(const cJSON *args) {
    if (!args) return NULL;

    cJSON *expr_json = cJSON_GetObjectItemCaseSensitive(args, "expr");
    cJSON *agg_json = cJSON_GetObjectItemCaseSensitive(args, "aggregate");
    if (!agg_json) agg_json = cJSON_GetObjectItemCaseSensitive(args, "agg");
    int has_expr = cJSON_IsString(expr_json) && expr_json->valuestring && expr_json->valuestring[0];
    int has_aggregate = cJSON_IsString(agg_json) && agg_json->valuestring && agg_json->valuestring[0];
    if (!has_expr && !has_aggregate) {
        tf_set_last_error("assert: expr or aggregate is required");
        return NULL;
    }
    if (has_expr && has_aggregate) {
        tf_set_last_error("assert: use either expr or aggregate, not both");
        return NULL;
    }

    cJSON *action_json = cJSON_GetObjectItemCaseSensitive(args, "action");
    assert_action action = ASSERT_FAIL;
    if (!parse_action(cJSON_IsString(action_json) ? action_json->valuestring : NULL, &action)) {
        tf_set_last_error("assert: action must be fail, warn, filter, quarantine, or annotate");
        return NULL;
    }

    assert_state *st = calloc(1, sizeof(assert_state));
    if (!st) return NULL;
    st->action = action;
    st->mode = has_aggregate ? ASSERT_MODE_AGGREGATE : ASSERT_MODE_ROW;
    st->audit_limit = 1000;
    st->agg_col_index = -1;

    if (st->mode == ASSERT_MODE_ROW) {
        st->expr = tf_expr_parse(expr_json->valuestring);
        if (!st->expr) { assert_state_free(st); return NULL; }
        st->expr_text = strdup(expr_json->valuestring);
    } else {
        if (action != ASSERT_FAIL && action != ASSERT_WARN) {
            tf_set_last_error("assert: aggregate mode supports only fail or warn actions");
            assert_state_free(st);
            return NULL;
        }
        int prc = parse_aggregate_spec(args, &st->agg_kind, &st->agg_text, &st->agg_col);
        if (prc == -2) tf_set_last_error("assert: unknown aggregate");
        else if (prc <= 0) tf_set_last_error("assert: aggregate must be a non-empty string");
        if (prc <= 0) { assert_state_free(st); return NULL; }
        if (agg_needs_column(st->agg_kind) && (!st->agg_col || !st->agg_col[0])) {
            tf_set_last_error("assert: aggregate requires a column");
            assert_state_free(st);
            return NULL;
        }
        cJSON *cmp_json = cJSON_GetObjectItemCaseSensitive(args, "op");
        if (!cmp_json) cmp_json = cJSON_GetObjectItemCaseSensitive(args, "cmp");
        if (!cmp_json) cmp_json = cJSON_GetObjectItemCaseSensitive(args, "comparison");
        if (!parse_cmp(cJSON_IsString(cmp_json) ? cmp_json->valuestring : NULL, &st->cmp)) {
            tf_set_last_error("assert: aggregate op must be one of ==, !=, <, <=, >, >=");
            assert_state_free(st);
            return NULL;
        }
        cJSON *value_json = cJSON_GetObjectItemCaseSensitive(args, "value");
        if (!value_json) value_json = cJSON_GetObjectItemCaseSensitive(args, "threshold");
        if (!parse_number_arg(value_json, &st->threshold)) {
            tf_set_last_error("assert: aggregate value must be numeric");
            assert_state_free(st);
            return NULL;
        }
    }

    cJSON *audit_json = cJSON_GetObjectItemCaseSensitive(args, "audit");
    st->audit = cJSON_IsTrue(audit_json) ? 1 : 0;
    cJSON *audit_limit_json = cJSON_GetObjectItemCaseSensitive(args, "audit_limit");
    if (!audit_limit_json) audit_limit_json = cJSON_GetObjectItemCaseSensitive(args, "auditLimit");
    if (audit_limit_json) {
        if (!cJSON_IsNumber(audit_limit_json) || audit_limit_json->valuedouble <= 0) {
            tf_set_last_error("assert: audit_limit must be a positive integer");
            assert_state_free(st);
            return NULL;
        }
        st->audit_limit = (size_t)audit_limit_json->valuedouble;
    }

    cJSON *name_json = cJSON_GetObjectItemCaseSensitive(args, "name");
    cJSON *message_json = cJSON_GetObjectItemCaseSensitive(args, "message");
    cJSON *result_json = cJSON_GetObjectItemCaseSensitive(args, "result");
    st->name = strdup(cJSON_IsString(name_json) && name_json->valuestring[0] ? name_json->valuestring : "assert");
    st->message = strdup(cJSON_IsString(message_json) ? message_json->valuestring : "");
    st->result = strdup(cJSON_IsString(result_json) && result_json->valuestring[0] ? result_json->valuestring : "_assert");
    if (!st->name || !st->message || !st->result || (st->mode == ASSERT_MODE_ROW && !st->expr_text)) {
        assert_state_free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) {
        assert_state_free(st);
        return NULL;
    }
    step->process = assert_process;
    step->flush = assert_flush;
    step->append_stats = assert_append_stats;
    step->destroy = assert_destroy;
    step->state = st;
    return step;
}
