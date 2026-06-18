/*
 * op_cast.c - Type conversion.
 *
 * Config: {"mapping": {"age": "int", "score": "float", "active": "bool"}}
 */

#include "internal.h"
#include "date_utils.h"
#include "cJSON.h"
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* Cast is row-local. Audit records are opt-in and capped so parse/coercion
 * diagnostics cannot become an unbounded second output stream. */
typedef enum {
    CAST_ON_ERROR_COERCE = 0,
    CAST_ON_ERROR_FAIL,
    CAST_ON_ERROR_NULL,
} cast_error_policy;

typedef struct {
    char    **col_names;
    tf_type  *target_types;
    size_t    n;
    size_t    row_index;
    size_t    audit_limit;
    size_t    audit_emitted;
    size_t    coercion_failures;
    size_t    coercion_nulled;
    int       audit;
    cast_error_policy on_error;
    tf_audit_options audit_opts;
} cast_state;

static const char *cast_error_policy_name(cast_error_policy p) {
    switch (p) {
        case CAST_ON_ERROR_COERCE: return "coerce";
        case CAST_ON_ERROR_FAIL: return "fail";
        case CAST_ON_ERROR_NULL: return "null";
        default: return "coerce";
    }
}

static int parse_cast_error_policy(const char *s, cast_error_policy *out) {
    if (!s || !s[0] || strcmp(s, "coerce") == 0 || strcmp(s, "legacy") == 0 ||
        strcmp(s, "default") == 0) {
        *out = CAST_ON_ERROR_COERCE;
        return 1;
    }
    if (strcmp(s, "fail") == 0 || strcmp(s, "error") == 0 || strcmp(s, "strict") == 0) {
        *out = CAST_ON_ERROR_FAIL;
        return 1;
    }
    if (strcmp(s, "null") == 0 || strcmp(s, "nulling") == 0) {
        *out = CAST_ON_ERROR_NULL;
        return 1;
    }
    return 0;
}

static const char *cast_type_name(tf_type t) {
    switch (t) {
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

static tf_type parse_type(const char *s) {
    if (strcmp(s, "int") == 0 || strcmp(s, "int64") == 0) return TF_TYPE_INT64;
    if (strcmp(s, "float") == 0 || strcmp(s, "float64") == 0) return TF_TYPE_FLOAT64;
    if (strcmp(s, "string") == 0 || strcmp(s, "str") == 0) return TF_TYPE_STRING;
    if (strcmp(s, "bool") == 0 || strcmp(s, "boolean") == 0) return TF_TYPE_BOOL;
    if (strcmp(s, "date") == 0) return TF_TYPE_DATE;
    if (strcmp(s, "timestamp") == 0 || strcmp(s, "datetime") == 0) return TF_TYPE_TIMESTAMP;
    return TF_TYPE_NULL;
}

static int only_ascii_ws(const char *s) {
    if (!s) return 1;
    while (*s) {
        if (*s != ' ' && *s != '\t' && *s != '\n' && *s != '\r' && *s != '\f' && *s != '\v') return 0;
        s++;
    }
    return 1;
}

static int strict_int_parse_ok(const char *s, const char **reason) {
    char *end = NULL;
    errno = 0;
    (void)strtoll(s ? s : "", &end, 10);
    if (!s || end == s) { if (reason) *reason = "invalid_integer"; return 0; }
    if (errno == ERANGE) { if (reason) *reason = "integer_out_of_range"; return 0; }
    if (!only_ascii_ws(end)) { if (reason) *reason = "trailing_characters"; return 0; }
    return 1;
}

static int strict_float_parse_ok(const char *s, const char **reason) {
    char *end = NULL;
    errno = 0;
    (void)strtod(s ? s : "", &end);
    if (!s || end == s) { if (reason) *reason = "invalid_float"; return 0; }
    if (errno == ERANGE) { if (reason) *reason = "float_out_of_range"; return 0; }
    if (!only_ascii_ws(end)) { if (reason) *reason = "trailing_characters"; return 0; }
    return 1;
}

static int strict_date_parse_ok(const char *s, const char **reason) {
    int y, m, d, nread = 0;
    if (s && sscanf(s, "%d-%d-%d%n", &y, &m, &d, &nread) == 3) {
        if (only_ascii_ws(s + nread)) return 1;
        if (reason) *reason = "trailing_characters";
        return 0;
    }
    if (reason) *reason = "invalid_date";
    return 0;
}

static int strict_timestamp_parse_ok(const char *s, const char **reason) {
    int y, mo, d, h, mi, se, nread = 0;
    if (s && sscanf(s, "%d-%d-%dT%d:%d:%d%n", &y, &mo, &d, &h, &mi, &se, &nread) == 6) {
        if (only_ascii_ws(s + nread)) return 1;
        if (reason) *reason = "trailing_characters";
        return 0;
    }
    nread = 0;
    if (s && sscanf(s, "%d-%d-%d %d:%d:%d%n", &y, &mo, &d, &h, &mi, &se, &nread) == 6) {
        if (only_ascii_ws(s + nread)) return 1;
        if (reason) *reason = "trailing_characters";
        return 0;
    }
    nread = 0;
    if (s && sscanf(s, "%d-%d-%d%n", &y, &mo, &d, &nread) == 3) {
        if (only_ascii_ws(s + nread)) return 1;
        if (reason) *reason = "trailing_characters";
        return 0;
    }
    if (reason) *reason = "invalid_timestamp";
    return 0;
}

static int emit_cast_audit(cast_state *st, const tf_batch *before_b, const tf_batch *after_b,
                           size_t row, size_t col, size_t row_no, tf_type src_t, tf_type dst_t,
                           const char *event, const char *reason, tf_side_channels *side) {
    if (!st->audit || st->audit_emitted >= st->audit_limit || !side || !side->stats) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    cJSON_AddStringToObject(obj, "type", "audit");
    cJSON_AddStringToObject(obj, "op", "cast");
    cJSON_AddStringToObject(obj, "event", event ? event : "value_changed");
    cJSON_AddStringToObject(obj, "reason", reason ? reason : "type_cast");
    cJSON_AddStringToObject(obj, "channel", "audit");
    const char *column_name = before_b->col_names[col] ? before_b->col_names[col] : "";
    cJSON_AddStringToObject(obj, "column", column_name);
    cJSON_AddStringToObject(obj, "from_type", cast_type_name(src_t));
    cJSON_AddStringToObject(obj, "to_type", cast_type_name(dst_t));
    cJSON_AddStringToObject(obj, "expected", cast_type_name(dst_t));
    cJSON_AddStringToObject(obj, "on_error", cast_error_policy_name(st->on_error));
    if (src_t == TF_TYPE_STRING && !tf_batch_is_null(before_b, row, col)) {
        char actual_buf[256];
        const char *actual = tf_audit_format_string_for_column(&st->audit_opts, column_name,
                                                               tf_batch_get_string(before_b, row, col),
                                                               actual_buf, sizeof(actual_buf));
        cJSON_AddStringToObject(obj, "actual", actual ? actual : "");
    }
    cJSON_AddStringToObject(obj, "action", "cast");
    cJSON_AddNumberToObject(obj, "row", (double)row_no);
    cJSON *before = tf_audit_cell_to_json(before_b, row, col, &st->audit_opts);
    if (before) cJSON_AddItemToObject(obj, "before", before);
    cJSON *after = tf_audit_cell_to_json(after_b, row, col, &st->audit_opts);
    if (after) cJSON_AddItemToObject(obj, "after", after);
    cJSON *row_obj = tf_audit_row_to_json(after_b, row, &st->audit_opts);
    if (row_obj) cJSON_AddItemToObject(obj, "data", row_obj);
    int rc = tf_buffer_write_json_line(side->stats, obj);
    cJSON_Delete(obj);
    if (rc == TF_OK) st->audit_emitted++;
    return rc;
}

static int cast_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    cast_state *st = self->state;
    *out = NULL;
    size_t row_base = st->row_index;

    tf_type *out_types = malloc(in->n_cols * sizeof(tf_type));
    if (!out_types) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++) out_types[c] = in->col_types[c];
    for (size_t k = 0; k < st->n; k++) {
        int ci = tf_batch_col_index(in, st->col_names[k]);
        if (ci >= 0) out_types[ci] = st->target_types[k];
    }

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) { free(out_types); return TF_ERROR; }
    for (size_t c = 0; c < in->n_cols; c++) {
        if (tf_batch_set_schema(ob, c, in->col_names[c], out_types[c]) != TF_OK) goto fail;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_ensure_capacity(ob, r + 1) != TF_OK) goto fail;
        ob->n_rows = r + 1; /* audit row serialization needs this row visible */
        for (size_t c = 0; c < in->n_cols; c++) {
            if (tf_batch_is_null(in, r, c)) {
                if (tf_batch_set_null(ob, r, c) != TF_OK) goto fail;
                continue;
            }
            tf_type src_t = in->col_types[c];
            tf_type dst_t = out_types[c];
            const char *failure_reason = NULL;
            int write_rc = TF_OK;

            if (src_t == dst_t) {
                if (tf_batch_copy_cell(ob, r, c, in, r, c) != TF_OK) goto fail;
                continue;
            }

            if (dst_t == TF_TYPE_STRING) {
                switch (src_t) {
                    case TF_TYPE_INT64:
                    case TF_TYPE_FLOAT64:
                    case TF_TYPE_BOOL:
                    case TF_TYPE_DATE:
                    case TF_TYPE_TIMESTAMP:
                        write_rc = tf_batch_copy_cell_as_string(ob, r, c, in, r, c);
                        break;
                    default:
                        failure_reason = "unsupported_conversion";
                        write_rc = tf_batch_set_string(ob, r, c, "");
                        break;
                }
            } else if (dst_t == TF_TYPE_INT64) {
                int64_t v = 0;
                if (src_t == TF_TYPE_FLOAT64) v = (int64_t)tf_batch_get_float64(in, r, c);
                else if (src_t == TF_TYPE_STRING) {
                    const char *s = tf_batch_get_string(in, r, c);
                    v = strtoll(s ? s : "", NULL, 10);
                    strict_int_parse_ok(s, &failure_reason);
                } else if (src_t == TF_TYPE_BOOL) v = tf_batch_get_bool(in, r, c) ? 1 : 0;
                else if (src_t == TF_TYPE_TIMESTAMP) v = tf_batch_get_timestamp(in, r, c);
                else failure_reason = "unsupported_conversion";
                write_rc = tf_batch_set_int64(ob, r, c, v);
            } else if (dst_t == TF_TYPE_FLOAT64) {
                double v = 0;
                if (src_t == TF_TYPE_INT64) v = (double)tf_batch_get_int64(in, r, c);
                else if (src_t == TF_TYPE_STRING) {
                    const char *s = tf_batch_get_string(in, r, c);
                    v = strtod(s ? s : "", NULL);
                    strict_float_parse_ok(s, &failure_reason);
                } else if (src_t == TF_TYPE_BOOL) v = tf_batch_get_bool(in, r, c) ? 1.0 : 0.0;
                else failure_reason = "unsupported_conversion";
                write_rc = tf_batch_set_float64(ob, r, c, v);
            } else if (dst_t == TF_TYPE_BOOL) {
                bool v = false;
                if (src_t == TF_TYPE_INT64) v = tf_batch_get_int64(in, r, c) != 0;
                else if (src_t == TF_TYPE_FLOAT64) v = tf_batch_get_float64(in, r, c) != 0.0;
                else if (src_t == TF_TYPE_STRING) {
                    const char *s = tf_batch_get_string(in, r, c);
                    v = s && strlen(s) > 0 && strcmp(s, "false") != 0;
                    if (!s || (strcmp(s, "true") != 0 && strcmp(s, "false") != 0)) failure_reason = "non_canonical_bool";
                } else failure_reason = "unsupported_conversion";
                write_rc = tf_batch_set_bool(ob, r, c, v);
            } else if (dst_t == TF_TYPE_DATE) {
                int32_t v = 0;
                if (src_t == TF_TYPE_STRING) {
                    const char *s = tf_batch_get_string(in, r, c);
                    int y, m, d;
                    if (s && sscanf(s, "%d-%d-%d", &y, &m, &d) == 3)
                        v = tf_date_from_ymd(y, m, d);
                    strict_date_parse_ok(s, &failure_reason);
                } else if (src_t == TF_TYPE_TIMESTAMP) {
                    v = (int32_t)(tf_batch_get_timestamp(in, r, c) / (86400LL * 1000000LL));
                } else failure_reason = "unsupported_conversion";
                write_rc = tf_batch_set_date(ob, r, c, v);
            } else if (dst_t == TF_TYPE_TIMESTAMP) {
                int64_t v = 0;
                if (src_t == TF_TYPE_STRING) {
                    const char *s = tf_batch_get_string(in, r, c);
                    size_t slen = s ? strlen(s) : 0;
                    int32_t dv;
                    if (slen >= 19) {
                        int y, mo, d, h, mi, se;
                        if (sscanf(s, "%d-%d-%dT%d:%d:%d", &y, &mo, &d, &h, &mi, &se) == 6)
                            v = tf_timestamp_from_parts(y, mo, d, h, mi, se, 0);
                        else if (sscanf(s, "%d-%d-%d %d:%d:%d", &y, &mo, &d, &h, &mi, &se) == 6)
                            v = tf_timestamp_from_parts(y, mo, d, h, mi, se, 0);
                    } else if (slen == 10) {
                        int y, m, d;
                        if (sscanf(s, "%d-%d-%d", &y, &m, &d) == 3) {
                            dv = tf_date_from_ymd(y, m, d);
                            v = (int64_t)dv * 86400LL * 1000000LL;
                        }
                    }
                    strict_timestamp_parse_ok(s, &failure_reason);
                } else if (src_t == TF_TYPE_DATE) {
                    v = (int64_t)tf_batch_get_date(in, r, c) * 86400LL * 1000000LL;
                } else if (src_t == TF_TYPE_INT64) {
                    v = tf_batch_get_int64(in, r, c);
                } else failure_reason = "unsupported_conversion";
                write_rc = tf_batch_set_timestamp(ob, r, c, v);
            } else {
                failure_reason = dst_t == TF_TYPE_NULL ? "invalid_target_type" : "unsupported_conversion";
                write_rc = tf_batch_set_null(ob, r, c);
            }
            if (write_rc != TF_OK) goto fail;

            if (failure_reason) {
                st->coercion_failures++;
                if (st->on_error == CAST_ON_ERROR_NULL || st->on_error == CAST_ON_ERROR_FAIL) {
                    if (tf_batch_set_null(ob, r, c) != TF_OK) goto fail;
                    if (st->on_error == CAST_ON_ERROR_NULL) st->coercion_nulled++;
                }
            }

            if (st->audit) {
                const char *event = failure_reason ? "coercion_failed" : "value_changed";
                const char *reason = failure_reason ? failure_reason : "type_cast";
                if (emit_cast_audit(st, in, ob, r, c, row_base + r + 1, src_t, dst_t, event, reason, side) != TF_OK) goto fail;
            }

            if (failure_reason && st->on_error == CAST_ON_ERROR_FAIL) {
                char msg[512];
                snprintf(msg, sizeof(msg), "cast failed at row %zu column '%s': %s",
                         row_base + r + 1,
                         in->col_names[c] ? in->col_names[c] : "",
                         failure_reason);
                tf_set_last_error(msg);
                goto fail;
            }
        }
    }

    st->row_index += in->n_rows;
    free(out_types);
    *out = ob;
    return TF_OK;

fail:
    tf_batch_free(ob);
    free(out_types);
    return TF_ERROR;
}

static int cast_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static int cast_append_stats(tf_step *self, tf_buffer *out) {
    if (!self || !self->state || !out) return TF_ERROR;
    cast_state *st = (cast_state *)self->state;
    char buf[256];
    snprintf(buf, sizeof(buf),
             ",\"on_error\":\"%s\",\"coercion_failures\":%zu,"
             "\"coercion_nulled\":%zu,\"audit_emitted\":%zu",
             cast_error_policy_name(st->on_error), st->coercion_failures,
             st->coercion_nulled, st->audit_emitted);
    return tf_buffer_write_str(out, buf);
}

static void cast_state_free(cast_state *st) {
    if (!st) return;
    tf_audit_options_free(&st->audit_opts);
    if (st->col_names) {
        for (size_t i = 0; i < st->n; i++) free(st->col_names[i]);
    }
    free(st->col_names);
    free(st->target_types);
    free(st);
}

static void cast_destroy(tf_step *self) {
    if (self) cast_state_free(self->state);
    free(self);
}

tf_step *tf_cast_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *mapping = cJSON_GetObjectItemCaseSensitive(args, "mapping");
    if (!mapping || !cJSON_IsObject(mapping)) return NULL;

    int n = cJSON_GetArraySize(mapping);
    cast_state *st = calloc(1, sizeof(cast_state));
    if (!st) return NULL;
    tf_audit_options_init(&st->audit_opts, 1);
    st->col_names = tf_callocarray_checked(n > 0 ? (size_t)n : 1, sizeof(char *));
    st->target_types = tf_callocarray_checked(n > 0 ? (size_t)n : 1, sizeof(tf_type));
    st->n = (size_t)n;
    st->audit_limit = 1000;
    st->on_error = CAST_ON_ERROR_COERCE;
    if (!st->col_names || !st->target_types) { cast_state_free(st); return NULL; }

    int i = 0;
    cJSON *entry = NULL;
    cJSON_ArrayForEach(entry, mapping) {
        if (!entry->string || !entry->string[0]) { cast_state_free(st); return NULL; }
        st->col_names[i] = strdup(entry->string);
        st->target_types[i] = cJSON_IsString(entry) ? parse_type(entry->valuestring) : TF_TYPE_NULL;
        if (!st->col_names[i]) { cast_state_free(st); return NULL; }
        i++;
    }

    cJSON *on_error_j = cJSON_GetObjectItemCaseSensitive(args, "on_error");
    if (!on_error_j) on_error_j = cJSON_GetObjectItemCaseSensitive(args, "onError");
    if (!on_error_j) on_error_j = cJSON_GetObjectItemCaseSensitive(args, "on_coercion_error");
    if (!on_error_j) on_error_j = cJSON_GetObjectItemCaseSensitive(args, "onCoercionError");
    if (on_error_j) {
        if (!cJSON_IsString(on_error_j) ||
            !parse_cast_error_policy(on_error_j->valuestring, &st->on_error)) {
            tf_set_last_error("cast: on_error must be coerce, fail, or null");
            cast_state_free(st);
            return NULL;
        }
    }

    cJSON *audit_j = cJSON_GetObjectItemCaseSensitive(args, "audit");
    st->audit = cJSON_IsTrue(audit_j) ? 1 : 0;
    cJSON *audit_limit_j = cJSON_GetObjectItemCaseSensitive(args, "audit_limit");
    if (!audit_limit_j) audit_limit_j = cJSON_GetObjectItemCaseSensitive(args, "auditLimit");
    if (audit_limit_j) {
        size_t parsed_limit = 0;
        if (tf_json_get_size_arg_any(args, "audit_limit", "auditLimit",
                                     1, TF_MAX_AUDIT_RECORDS,
                                     &parsed_limit, "cast") < 0) {
            cast_state_free(st);
            return NULL;
        }
        st->audit_limit = parsed_limit;
    }

    if (tf_audit_options_parse(&st->audit_opts, args, "cast") != TF_OK) {
        cast_state_free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { cast_state_free(st); return NULL; }
    step->process = cast_process;
    step->flush = cast_flush;
    step->destroy = cast_destroy;
    step->append_stats = cast_append_stats;
    step->state = st;
    return step;
}
