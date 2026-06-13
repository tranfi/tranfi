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
typedef struct {
    char    **col_names;
    tf_type  *target_types;
    size_t    n;
    size_t    row_index;
    size_t    audit_limit;
    size_t    audit_emitted;
    int       audit;
} cast_state;

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

static cJSON *cast_cell_to_json(const tf_batch *b, size_t row, size_t col) {
    if (!b || col >= b->n_cols || row >= b->n_rows || tf_batch_is_null(b, row, col)) return cJSON_CreateNull();
    switch (b->col_types[col]) {
        case TF_TYPE_BOOL: return cJSON_CreateBool(tf_batch_get_bool(b, row, col));
        case TF_TYPE_INT64: return cJSON_CreateNumber((double)tf_batch_get_int64(b, row, col));
        case TF_TYPE_FLOAT64: return cJSON_CreateNumber(tf_batch_get_float64(b, row, col));
        case TF_TYPE_STRING: return cJSON_CreateString(tf_batch_get_string(b, row, col));
        case TF_TYPE_DATE: return cJSON_CreateNumber((double)tf_batch_get_date(b, row, col));
        case TF_TYPE_TIMESTAMP: return cJSON_CreateNumber((double)tf_batch_get_timestamp(b, row, col));
        default: return cJSON_CreateNull();
    }
}

static cJSON *cast_row_to_json(const tf_batch *b, size_t row) {
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return NULL;
    for (size_t c = 0; c < b->n_cols; c++) {
        cJSON *value = cast_cell_to_json(b, row, c);
        if (!value) { cJSON_Delete(obj); return NULL; }
        cJSON_AddItemToObject(obj, b->col_names[c] ? b->col_names[c] : "", value);
    }
    return obj;
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
    cJSON_AddStringToObject(obj, "column", before_b->col_names[col] ? before_b->col_names[col] : "");
    cJSON_AddStringToObject(obj, "from_type", cast_type_name(src_t));
    cJSON_AddStringToObject(obj, "to_type", cast_type_name(dst_t));
    cJSON_AddStringToObject(obj, "expected", cast_type_name(dst_t));
    if (src_t == TF_TYPE_STRING && !tf_batch_is_null(before_b, row, col))
        cJSON_AddStringToObject(obj, "actual", tf_batch_get_string(before_b, row, col));
    cJSON_AddStringToObject(obj, "action", "cast");
    cJSON_AddNumberToObject(obj, "row", (double)row_no);
    cJSON *before = cast_cell_to_json(before_b, row, col);
    if (before) cJSON_AddItemToObject(obj, "before", before);
    cJSON *after = cast_cell_to_json(after_b, row, col);
    if (after) cJSON_AddItemToObject(obj, "after", after);
    cJSON *row_obj = cast_row_to_json(after_b, row);
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
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], out_types[c]);

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_ensure_capacity(ob, r + 1);
        ob->n_rows = r + 1;
        for (size_t c = 0; c < in->n_cols; c++) {
            if (tf_batch_is_null(in, r, c)) {
                tf_batch_set_null(ob, r, c);
                continue;
            }
            tf_type src_t = in->col_types[c];
            tf_type dst_t = out_types[c];
            const char *failure_reason = NULL;

            if (src_t == dst_t) {
                switch (src_t) {
                    case TF_TYPE_BOOL: tf_batch_set_bool(ob, r, c, tf_batch_get_bool(in, r, c)); break;
                    case TF_TYPE_INT64: tf_batch_set_int64(ob, r, c, tf_batch_get_int64(in, r, c)); break;
                    case TF_TYPE_FLOAT64: tf_batch_set_float64(ob, r, c, tf_batch_get_float64(in, r, c)); break;
                    case TF_TYPE_STRING: tf_batch_set_string(ob, r, c, tf_batch_get_string(in, r, c)); break;
                    case TF_TYPE_DATE: tf_batch_set_date(ob, r, c, tf_batch_get_date(in, r, c)); break;
                    case TF_TYPE_TIMESTAMP: tf_batch_set_timestamp(ob, r, c, tf_batch_get_timestamp(in, r, c)); break;
                    default: tf_batch_set_null(ob, r, c); break;
                }
                continue;
            }

            if (dst_t == TF_TYPE_STRING) {
                char buf[64];
                switch (src_t) {
                    case TF_TYPE_INT64: snprintf(buf, sizeof(buf), "%lld", (long long)tf_batch_get_int64(in, r, c)); break;
                    case TF_TYPE_FLOAT64: snprintf(buf, sizeof(buf), "%g", tf_batch_get_float64(in, r, c)); break;
                    case TF_TYPE_BOOL: snprintf(buf, sizeof(buf), "%s", tf_batch_get_bool(in, r, c) ? "true" : "false"); break;
                    case TF_TYPE_DATE: tf_date_format(tf_batch_get_date(in, r, c), buf, sizeof(buf)); break;
                    case TF_TYPE_TIMESTAMP: tf_timestamp_format(tf_batch_get_timestamp(in, r, c), buf, sizeof(buf)); break;
                    default: buf[0] = '\0'; failure_reason = "unsupported_conversion"; break;
                }
                tf_batch_set_string(ob, r, c, buf);
            } else if (dst_t == TF_TYPE_INT64) {
                int64_t v = 0;
                if (src_t == TF_TYPE_FLOAT64) v = (int64_t)tf_batch_get_float64(in, r, c);
                else if (src_t == TF_TYPE_STRING) {
                    const char *s = tf_batch_get_string(in, r, c);
                    char *end;
                    v = strtoll(s, &end, 10);
                    strict_int_parse_ok(s, &failure_reason);
                } else if (src_t == TF_TYPE_BOOL) v = tf_batch_get_bool(in, r, c) ? 1 : 0;
                else if (src_t == TF_TYPE_TIMESTAMP) v = tf_batch_get_timestamp(in, r, c);
                else failure_reason = "unsupported_conversion";
                tf_batch_set_int64(ob, r, c, v);
            } else if (dst_t == TF_TYPE_FLOAT64) {
                double v = 0;
                if (src_t == TF_TYPE_INT64) v = (double)tf_batch_get_int64(in, r, c);
                else if (src_t == TF_TYPE_STRING) {
                    const char *s = tf_batch_get_string(in, r, c);
                    char *end;
                    v = strtod(s, &end);
                    strict_float_parse_ok(s, &failure_reason);
                } else if (src_t == TF_TYPE_BOOL) v = tf_batch_get_bool(in, r, c) ? 1.0 : 0.0;
                else failure_reason = "unsupported_conversion";
                tf_batch_set_float64(ob, r, c, v);
            } else if (dst_t == TF_TYPE_BOOL) {
                bool v = false;
                if (src_t == TF_TYPE_INT64) v = tf_batch_get_int64(in, r, c) != 0;
                else if (src_t == TF_TYPE_FLOAT64) v = tf_batch_get_float64(in, r, c) != 0.0;
                else if (src_t == TF_TYPE_STRING) {
                    const char *s = tf_batch_get_string(in, r, c);
                    v = strlen(s) > 0 && strcmp(s, "false") != 0;
                    if (strcmp(s, "true") != 0 && strcmp(s, "false") != 0) failure_reason = "non_canonical_bool";
                } else failure_reason = "unsupported_conversion";
                tf_batch_set_bool(ob, r, c, v);
            } else if (dst_t == TF_TYPE_DATE) {
                int32_t v = 0;
                if (src_t == TF_TYPE_STRING) {
                    const char *s = tf_batch_get_string(in, r, c);
                    int y, m, d;
                    if (sscanf(s, "%d-%d-%d", &y, &m, &d) == 3)
                        v = tf_date_from_ymd(y, m, d);
                    strict_date_parse_ok(s, &failure_reason);
                } else if (src_t == TF_TYPE_TIMESTAMP) {
                    v = (int32_t)(tf_batch_get_timestamp(in, r, c) / (86400LL * 1000000LL));
                } else failure_reason = "unsupported_conversion";
                tf_batch_set_date(ob, r, c, v);
            } else if (dst_t == TF_TYPE_TIMESTAMP) {
                int64_t v = 0;
                if (src_t == TF_TYPE_STRING) {
                    const char *s = tf_batch_get_string(in, r, c);
                    size_t slen = strlen(s);
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
                tf_batch_set_timestamp(ob, r, c, v);
            } else {
                failure_reason = dst_t == TF_TYPE_NULL ? "invalid_target_type" : "unsupported_conversion";
                tf_batch_set_null(ob, r, c);
            }

            if (st->audit) {
                const char *event = failure_reason ? "coercion_failed" : "value_changed";
                const char *reason = failure_reason ? failure_reason : "type_cast";
                if (emit_cast_audit(st, in, ob, r, c, row_base + r + 1, src_t, dst_t, event, reason, side) != TF_OK) {
                    tf_batch_free(ob);
                    free(out_types);
                    return TF_ERROR;
                }
            }
        }
    }

    st->row_index += in->n_rows;
    free(out_types);
    *out = ob;
    return TF_OK;
}

static int cast_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void cast_state_free(cast_state *st) {
    if (!st) return;
    for (size_t i = 0; i < st->n; i++) free(st->col_names[i]);
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
    st->col_names = calloc((size_t)n, sizeof(char *));
    st->target_types = calloc((size_t)n, sizeof(tf_type));
    st->n = (size_t)n;
    st->audit_limit = 1000;
    if (!st->col_names || !st->target_types) { cast_state_free(st); return NULL; }

    int i = 0;
    cJSON *entry = NULL;
    cJSON_ArrayForEach(entry, mapping) {
        st->col_names[i] = strdup(entry->string);
        st->target_types[i] = cJSON_IsString(entry) ? parse_type(entry->valuestring) : TF_TYPE_NULL;
        if (!st->col_names[i]) { cast_state_free(st); return NULL; }
        i++;
    }

    cJSON *audit_j = cJSON_GetObjectItemCaseSensitive(args, "audit");
    st->audit = cJSON_IsTrue(audit_j) ? 1 : 0;
    cJSON *audit_limit_j = cJSON_GetObjectItemCaseSensitive(args, "audit_limit");
    if (!audit_limit_j) audit_limit_j = cJSON_GetObjectItemCaseSensitive(args, "auditLimit");
    if (audit_limit_j) {
        if (!cJSON_IsNumber(audit_limit_j) || audit_limit_j->valuedouble <= 0) {
            tf_set_last_error("cast: audit_limit must be a positive integer");
            cast_state_free(st);
            return NULL;
        }
        st->audit_limit = (size_t)audit_limit_j->valuedouble;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { cast_state_free(st); return NULL; }
    step->process = cast_process;
    step->flush = cast_flush;
    step->destroy = cast_destroy;
    step->state = st;
    return step;
}
