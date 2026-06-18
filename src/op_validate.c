/*
 * op_validate.c - Row-local validation rule sets.
 *
 * Config:
 *   {"expr": "col('age') > 0", "audit": true, "audit_limit": 1000}
 *   {"rules": [{"name":"age_ok", "expr":"col('age') > 0"}], "audit": true}
 */

#include "internal.h"
#include "cJSON.h"
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define TF_VALIDATE_RULES_FILE_MAX_BYTES (1024 * 1024)

/* Validate is row-local. Rule sets are evaluated independently for each row;
 * per-rule counts are O(number_of_rules), and audit records are opt-in/capped. */
typedef struct {
    tf_expr *expr;
    char    *expr_text;
    char    *name;
    char    *message;
    size_t   checked_rows;
    size_t   passed_rows;
    size_t   failed_rows;
    size_t   audit_emitted;
} validate_rule;

typedef struct {
    validate_rule *rules;
    size_t   n_rules;
    char    *name;
    char    *message;
    size_t   row_index;
    size_t   checked_rows;
    size_t   passed_rows;
    size_t   failed_rows;
    size_t   audit_limit;
    size_t   audit_emitted;
    size_t   max_failures;
    int      has_max_failures;
    double   max_failure_rate;
    int      has_max_failure_rate;
    double   warn_failure_rate;
    int      has_warn_failure_rate;
    int      audit;
    tf_audit_options audit_opts;
} validate_state;

static double validate_failure_rate(size_t failed, size_t checked) {
    return checked ? (double)failed / (double)checked : 0.0;
}

static void validate_rule_free(validate_rule *rule) {
    if (!rule) return;
    tf_expr_free(rule->expr);
    free(rule->expr_text);
    free(rule->name);
    free(rule->message);
}

static int add_validate_rule(validate_state *st, const char *expr_text,
                             const char *name, const char *message) {
    if (!st || !expr_text || !expr_text[0]) return TF_ERROR;
    tf_expr *expr = tf_expr_parse(expr_text);
    if (!expr) return TF_ERROR;
    size_t next = 0;
    if (tf_size_add(st->n_rules, 1, &next) != TF_OK) {
        tf_expr_free(expr);
        return TF_ERROR;
    }
    validate_rule *nr = tf_reallocarray_checked(st->rules, next, sizeof(validate_rule));
    if (!nr) { tf_expr_free(expr); return TF_ERROR; }
    st->rules = nr;
    validate_rule *rule = &st->rules[st->n_rules];
    st->n_rules = next;
    memset(rule, 0, sizeof(*rule));
    rule->expr = expr;
    rule->expr_text = strdup(expr_text);
    rule->name = strdup(name && name[0] ? name : "validate");
    rule->message = strdup(message ? message : "");
    if (!rule->expr_text || !rule->name || !rule->message) return TF_ERROR;
    return TF_OK;
}

static int emit_validate_audit(validate_state *st, validate_rule *rule,
                               const tf_batch *b, size_t row,
                               tf_side_channels *side) {
    if (!st->audit || st->audit_emitted >= st->audit_limit || !side || !side->stats) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    int rc = TF_ERROR;
    if (tf_json_add_string(obj, "type", "audit") != TF_OK ||
        tf_json_add_string(obj, "op", "validate") != TF_OK ||
        tf_json_add_string(obj, "event", "validation_failed") != TF_OK ||
        tf_json_add_string(obj, "reason", "validate_false") != TF_OK ||
        tf_json_add_string(obj, "channel", "audit") != TF_OK ||
        tf_json_add_string(obj, "action", "annotate") != TF_OK ||
        tf_json_add_string(obj, "suite", st->name ? st->name : "validate") != TF_OK ||
        tf_json_add_string(obj, "name", rule->name ? rule->name : "validate") != TF_OK ||
        tf_json_add_string(obj, "expr", rule->expr_text ? rule->expr_text : "") != TF_OK ||
        tf_json_add_string(obj, "result", "_valid") != TF_OK ||
        tf_json_add_bool(obj, "valid", 0) != TF_OK ||
        tf_json_add_number(obj, "row", (double)st->row_index) != TF_OK) {
        goto done;
    }
    if (rule->message && rule->message[0]) {
        if (tf_json_add_string(obj, "message", rule->message) != TF_OK) goto done;
    } else if (st->message && st->message[0]) {
        if (tf_json_add_string(obj, "message", st->message) != TF_OK) goto done;
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
    if (rc == TF_OK) {
        st->audit_emitted++;
        rule->audit_emitted++;
    }
    return rc;
}

static int emit_validate_threshold_error(validate_state *st, tf_side_channels *side) {
    char msg[512];
    snprintf(msg, sizeof(msg),
             "validate max_failures exceeded at row %zu: failed_rows=%zu max_failures=%zu",
             st->row_index, st->failed_rows, st->max_failures);
    tf_set_last_error(msg);
    if (!side || !side->errors) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    int rc = TF_ERROR;
    if (tf_json_add_string(obj, "type", "validate_failure") != TF_OK ||
        tf_json_add_string(obj, "op", "validate") != TF_OK ||
        tf_json_add_string(obj, "event", "threshold_exceeded") != TF_OK ||
        tf_json_add_string(obj, "reason", "max_failures_exceeded") != TF_OK ||
        tf_json_add_string(obj, "severity", "error") != TF_OK ||
        tf_json_add_string(obj, "name", st->name ? st->name : "validate") != TF_OK ||
        tf_json_add_number(obj, "row", (double)st->row_index) != TF_OK ||
        tf_json_add_number(obj, "checked_rows", (double)st->checked_rows) != TF_OK ||
        tf_json_add_number(obj, "failed_rows", (double)st->failed_rows) != TF_OK ||
        tf_json_add_number(obj, "failure_rate", validate_failure_rate(st->failed_rows, st->checked_rows)) != TF_OK ||
        tf_json_add_number(obj, "max_failures", (double)st->max_failures) != TF_OK) {
        goto done;
    }
    if (st->message && st->message[0] &&
        tf_json_add_string(obj, "message", st->message) != TF_OK) {
        goto done;
    }
    rc = tf_buffer_write_json_line(side->errors, obj);
done:
    cJSON_Delete(obj);
    return rc;
}

static int emit_validate_rate_event(validate_state *st, tf_side_channels *side,
                                    const char *event, const char *reason,
                                    const char *severity, const char *threshold_name,
                                    double threshold, double failure_rate) {
    if (!side || !side->errors) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    int rc = TF_ERROR;
    if (tf_json_add_string(obj, "type", "validate_failure") != TF_OK ||
        tf_json_add_string(obj, "op", "validate") != TF_OK ||
        tf_json_add_string(obj, "event", event) != TF_OK ||
        tf_json_add_string(obj, "reason", reason) != TF_OK ||
        tf_json_add_string(obj, "severity", severity) != TF_OK ||
        tf_json_add_string(obj, "name", st->name ? st->name : "validate") != TF_OK ||
        tf_json_add_number(obj, "checked_rows", (double)st->checked_rows) != TF_OK ||
        tf_json_add_number(obj, "failed_rows", (double)st->failed_rows) != TF_OK ||
        tf_json_add_number(obj, "failure_rate", failure_rate) != TF_OK ||
        tf_json_add_number(obj, threshold_name, threshold) != TF_OK) {
        goto done;
    }
    if (st->message && st->message[0] &&
        tf_json_add_string(obj, "message", st->message) != TF_OK) {
        goto done;
    }
    rc = tf_buffer_write_json_line(side->errors, obj);
done:
    cJSON_Delete(obj);
    return rc;
}

static int emit_validate_rate_warning(validate_state *st, tf_side_channels *side, double failure_rate) {
    return emit_validate_rate_event(st, side, "threshold_warning",
                                    "warn_failure_rate_exceeded", "warning",
                                    "warn_failure_rate", st->warn_failure_rate,
                                    failure_rate);
}

static int emit_validate_rate_error(validate_state *st, tf_side_channels *side, double failure_rate) {
    char msg[512];
    snprintf(msg, sizeof(msg),
             "validate max_failure_rate exceeded: failure_rate=%.17g max_failure_rate=%.17g failed_rows=%zu checked_rows=%zu",
             failure_rate, st->max_failure_rate, st->failed_rows, st->checked_rows);
    tf_set_last_error(msg);
    return emit_validate_rate_event(st, side, "threshold_exceeded",
                                    "max_failure_rate_exceeded", "error",
                                    "max_failure_rate", st->max_failure_rate,
                                    failure_rate);
}

static int validate_process(tf_step *self, tf_batch *in, tf_batch **out,
                            tf_side_channels *side) {
    validate_state *st = self->state;
    *out = NULL;

    size_t out_cols = 0;
    if (tf_size_add(in->n_cols, 1, &out_cols) != TF_OK) return TF_ERROR;
    tf_batch *ob = tf_batch_create(out_cols, in->n_rows > 0 ? in->n_rows : 1);
    if (!ob) return TF_ERROR;
    const char *extra_names[] = {"_valid"};
    const tf_type extra_types[] = {TF_TYPE_BOOL};
    if (tf_batch_clone_with_extra_cols(ob, in, extra_names, extra_types, 1) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        st->row_index++;
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        int row_valid = 1;
        st->checked_rows++;
        for (size_t i = 0; i < st->n_rules; i++) {
            validate_rule *rule = &st->rules[i];
            bool valid = false;
            if (tf_expr_eval(rule->expr, in, r, &valid) != TF_OK) valid = false;
            rule->checked_rows++;
            if (valid) {
                rule->passed_rows++;
            } else {
                rule->failed_rows++;
                row_valid = 0;
                if (emit_validate_audit(st, rule, in, r, side) != TF_OK) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
            }
        }
        if (row_valid) st->passed_rows++;
        else st->failed_rows++;
        if (tf_batch_set_bool(ob, r, in->n_cols, row_valid ? true : false) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (!row_valid && st->has_max_failures && st->failed_rows > st->max_failures) {
            if (emit_validate_threshold_error(st, side) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    *out = ob;
    return TF_OK;
}

static int validate_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    if (!self || !self->state || !out) return TF_ERROR;
    validate_state *st = self->state;
    *out = NULL;
    double rate = validate_failure_rate(st->failed_rows, st->checked_rows);
    if (st->has_warn_failure_rate && rate > st->warn_failure_rate) {
        if (emit_validate_rate_warning(st, side, rate) != TF_OK) return TF_ERROR;
    }
    if (st->has_max_failure_rate && rate > st->max_failure_rate) {
        int rc = emit_validate_rate_error(st, side, rate);
        return rc == TF_OK ? TF_ERROR : rc;
    }
    return TF_OK;
}

static int validate_append_rule_stats(validate_state *st, tf_buffer *out) {
    cJSON *arr = cJSON_CreateArray();
    if (!arr) return TF_ERROR;
    for (size_t i = 0; i < st->n_rules; i++) {
        validate_rule *rule = &st->rules[i];
        cJSON *obj = cJSON_CreateObject();
        if (!obj) { cJSON_Delete(arr); return TF_ERROR; }
        if (tf_json_add_string(obj, "name", rule->name ? rule->name : "validate") != TF_OK ||
            tf_json_add_string(obj, "expr", rule->expr_text ? rule->expr_text : "") != TF_OK ||
            tf_json_add_number(obj, "checked_rows", (double)rule->checked_rows) != TF_OK ||
            tf_json_add_number(obj, "passed_rows", (double)rule->passed_rows) != TF_OK ||
            tf_json_add_number(obj, "failed_rows", (double)rule->failed_rows) != TF_OK ||
            tf_json_add_number(obj, "failure_rate",
                               validate_failure_rate(rule->failed_rows, rule->checked_rows)) != TF_OK ||
            tf_json_add_number(obj, "audit_emitted", (double)rule->audit_emitted) != TF_OK) {
            cJSON_Delete(obj);
            cJSON_Delete(arr);
            return TF_ERROR;
        }
        if (tf_json_add_array_item(arr, obj) != TF_OK) {
            cJSON_Delete(arr);
            return TF_ERROR;
        }
    }
    char *printed = cJSON_PrintUnformatted(arr);
    cJSON_Delete(arr);
    if (!printed) return TF_ERROR;
    int rc = tf_buffer_write_str(out, ",\"rules\":");
    if (rc == TF_OK) rc = tf_buffer_write_str(out, printed);
    free(printed);
    return rc;
}

static int validate_append_stats(tf_step *self, tf_buffer *out) {
    if (!self || !self->state || !out) return TF_ERROR;
    validate_state *st = self->state;
    char buf[512];
    snprintf(buf, sizeof(buf),
             ",\"checked_rows\":%zu,\"passed_rows\":%zu,"
             "\"failed_rows\":%zu,\"failure_rate\":%.17g,"
             "\"audit_emitted\":%zu,\"rule_count\":%zu",
             st->checked_rows, st->passed_rows, st->failed_rows,
             validate_failure_rate(st->failed_rows, st->checked_rows),
             st->audit_emitted, st->n_rules);
    if (tf_buffer_write_str(out, buf) != TF_OK) return TF_ERROR;
    if (st->has_max_failures) {
        snprintf(buf, sizeof(buf), ",\"max_failures\":%zu", st->max_failures);
        if (tf_buffer_write_str(out, buf) != TF_OK) return TF_ERROR;
    }
    if (st->has_max_failure_rate) {
        snprintf(buf, sizeof(buf), ",\"max_failure_rate\":%.17g", st->max_failure_rate);
        if (tf_buffer_write_str(out, buf) != TF_OK) return TF_ERROR;
    }
    if (st->has_warn_failure_rate) {
        snprintf(buf, sizeof(buf), ",\"warn_failure_rate\":%.17g", st->warn_failure_rate);
        if (tf_buffer_write_str(out, buf) != TF_OK) return TF_ERROR;
    }
    return validate_append_rule_stats(st, out);
}

static void validate_state_free(validate_state *st) {
    if (!st) return;
    for (size_t i = 0; i < st->n_rules; i++) validate_rule_free(&st->rules[i]);
    free(st->rules);
    free(st->name);
    free(st->message);
    tf_audit_options_free(&st->audit_opts);
    free(st);
}

static void validate_destroy(tf_step *self) {
    if (!self) return;
    validate_state_free((validate_state *)self->state);
    free(self);
}

static const cJSON *validate_object_item(const cJSON *obj, const char *key) {
    return cJSON_IsObject(obj) ? cJSON_GetObjectItemCaseSensitive((cJSON *)obj, key) : NULL;
}

static const cJSON *validate_arg(const cJSON *args, const cJSON *rules_doc,
                                 const char *snake, const char *camel) {
    const cJSON *item = validate_object_item(args, snake);
    if (!item && camel) item = validate_object_item(args, camel);
    if (item) return item;
    item = validate_object_item(rules_doc, snake);
    if (!item && camel) item = validate_object_item(rules_doc, camel);
    return item;
}

static int validate_doc_has_suite_metadata(const cJSON *doc) {
    return validate_object_item(doc, "name") || validate_object_item(doc, "message") ||
           validate_object_item(doc, "audit") || validate_object_item(doc, "audit_limit") ||
           validate_object_item(doc, "auditLimit") ||
           validate_object_item(doc, "max_failures") || validate_object_item(doc, "maxFailures") ||
           validate_object_item(doc, "max_failure_rate") || validate_object_item(doc, "maxFailureRate") ||
           validate_object_item(doc, "warn_failure_rate") || validate_object_item(doc, "warnFailureRate");
}

static cJSON *load_validate_rules_file(const cJSON *args, int *status) {
    *status = 0;
    const cJSON *path_json = validate_object_item(args, "rules_file");
    if (!path_json) path_json = validate_object_item(args, "rulesFile");
    if (!path_json) return NULL;
    *status = -1;
    if (!cJSON_IsString(path_json) || !path_json->valuestring || !path_json->valuestring[0]) {
        tf_set_last_error("validate: rules_file must be a non-empty string");
        return NULL;
    }

    const char *validated_path = tf_policy_validated_path_arg(args, "rules_file");
    if (!validated_path) validated_path = tf_policy_validated_path_arg(args, "rulesFile");
    FILE *f = tf_policy_fopen_read(path_json->valuestring, validated_path);
    if (!f) {
        tf_set_last_error("validate: unable to read rules_file");
        return NULL;
    }
    if (fseek(f, 0, SEEK_END) != 0) {
        fclose(f);
        tf_set_last_error("validate: unable to read rules_file");
        return NULL;
    }
    long len = ftell(f);
    if (len < 0) {
        fclose(f);
        tf_set_last_error("validate: unable to read rules_file");
        return NULL;
    }
    if ((size_t)len > TF_VALIDATE_RULES_FILE_MAX_BYTES) {
        fclose(f);
        tf_set_last_error("validate: rules_file exceeds 1048576 bytes");
        return NULL;
    }
    if (fseek(f, 0, SEEK_SET) != 0) {
        fclose(f);
        tf_set_last_error("validate: unable to read rules_file");
        return NULL;
    }

    char *buf = malloc((size_t)len + 1);
    if (!buf) { fclose(f); return NULL; }
    size_t n = fread(buf, 1, (size_t)len, f);
    fclose(f);
    if (n != (size_t)len) {
        free(buf);
        tf_set_last_error("validate: unable to read rules_file");
        return NULL;
    }
    buf[n] = '\0';
    cJSON *doc = cJSON_ParseWithLength(buf, n);
    free(buf);
    if (!doc) {
        tf_set_last_error("validate: invalid rules_file JSON");
        return NULL;
    }
    *status = 1;
    return doc;
}

static const cJSON *validate_rules_from_doc(const cJSON *doc) {
    if (!doc) return NULL;
    if (cJSON_IsArray(doc)) return doc;
    if (!cJSON_IsObject(doc)) return NULL;
    const cJSON *rules = validate_object_item(doc, "rules");
    if (rules) return rules;
    if (validate_doc_has_suite_metadata(doc)) return NULL;
    return doc;
}

static int parse_validate_rules(validate_state *st, const cJSON *rules_json) {
    if (!rules_json) return TF_OK;
    if (cJSON_IsArray(rules_json)) {
        int n = cJSON_GetArraySize(rules_json);
        for (int i = 0; i < n; i++) {
            cJSON *item = cJSON_GetArrayItem(rules_json, i);
            char default_name[64];
            snprintf(default_name, sizeof(default_name), "rule_%d", i + 1);
            if (cJSON_IsString(item)) {
                if (add_validate_rule(st, item->valuestring, default_name, st->message) != TF_OK) return TF_ERROR;
            } else if (cJSON_IsObject(item)) {
                cJSON *expr_j = cJSON_GetObjectItemCaseSensitive(item, "expr");
                cJSON *name_j = cJSON_GetObjectItemCaseSensitive(item, "name");
                cJSON *message_j = cJSON_GetObjectItemCaseSensitive(item, "message");
                if (!cJSON_IsString(expr_j) || !expr_j->valuestring[0]) return TF_ERROR;
                const char *name = cJSON_IsString(name_j) && name_j->valuestring[0] ? name_j->valuestring : default_name;
                const char *message = cJSON_IsString(message_j) ? message_j->valuestring : st->message;
                if (add_validate_rule(st, expr_j->valuestring, name, message) != TF_OK) return TF_ERROR;
            } else {
                return TF_ERROR;
            }
        }
        return st->n_rules > 0 ? TF_OK : TF_ERROR;
    }
    if (cJSON_IsObject(rules_json)) {
        cJSON *entry = NULL;
        cJSON_ArrayForEach(entry, rules_json) {
            if (!entry->string || !entry->string[0]) return TF_ERROR;
            if (cJSON_IsString(entry)) {
                if (add_validate_rule(st, entry->valuestring, entry->string, st->message) != TF_OK) return TF_ERROR;
            } else if (cJSON_IsObject(entry)) {
                cJSON *expr_j = cJSON_GetObjectItemCaseSensitive(entry, "expr");
                cJSON *name_j = cJSON_GetObjectItemCaseSensitive(entry, "name");
                cJSON *message_j = cJSON_GetObjectItemCaseSensitive(entry, "message");
                if (!cJSON_IsString(expr_j) || !expr_j->valuestring[0]) return TF_ERROR;
                const char *name = cJSON_IsString(name_j) && name_j->valuestring[0] ? name_j->valuestring : entry->string;
                const char *message = cJSON_IsString(message_j) ? message_j->valuestring : st->message;
                if (add_validate_rule(st, expr_j->valuestring, name, message) != TF_OK) return TF_ERROR;
            } else {
                return TF_ERROR;
            }
        }
        return st->n_rules > 0 ? TF_OK : TF_ERROR;
    }
    return TF_ERROR;
}

tf_step *tf_validate_create(const cJSON *args) {
    if (!args) return NULL;

    validate_state *st = calloc(1, sizeof(validate_state));
    if (!st) return NULL;
    st->audit_limit = 1000;
    tf_audit_options_init(&st->audit_opts, 1);

    int rules_file_status = 0;
    cJSON *rules_doc = load_validate_rules_file(args, &rules_file_status);
    if (rules_file_status < 0) { validate_state_free(st); return NULL; }

    const cJSON *name_json = validate_arg(args, rules_doc, "name", NULL);
    const cJSON *message_json = validate_arg(args, rules_doc, "message", NULL);
    st->name = strdup(cJSON_IsString(name_json) && name_json->valuestring[0] ? name_json->valuestring : "validate");
    st->message = strdup(cJSON_IsString(message_json) ? message_json->valuestring : "");
    if (!st->name || !st->message) { cJSON_Delete(rules_doc); validate_state_free(st); return NULL; }

    const cJSON *audit_json = validate_arg(args, rules_doc, "audit", NULL);
    st->audit = cJSON_IsTrue(audit_json) ? 1 : 0;
    const cJSON *audit_limit_json = validate_arg(args, rules_doc, "audit_limit", "auditLimit");
    if (audit_limit_json) {
        size_t parsed_limit = 0;
        const cJSON *arg_limit = cJSON_GetObjectItemCaseSensitive(args, "audit_limit");
        const cJSON *arg_limit_alt = cJSON_GetObjectItemCaseSensitive(args, "auditLimit");
        const char *limit_name = arg_limit ? "audit_limit" :
                                 (arg_limit_alt ? "auditLimit" : "audit_limit");
        if (tf_json_size_value(audit_limit_json, limit_name,
                               1, TF_MAX_AUDIT_RECORDS,
                               &parsed_limit, "validate") < 0) {
            cJSON_Delete(rules_doc);
            validate_state_free(st);
            return NULL;
        }
        st->audit_limit = parsed_limit;
    }

    if (tf_audit_options_parse(&st->audit_opts, args, "validate") != TF_OK) {
        cJSON_Delete(rules_doc);
        validate_state_free(st);
        return NULL;
    }

    const cJSON *max_failures_json = validate_arg(args, rules_doc, "max_failures", "maxFailures");
    if (max_failures_json) {
        size_t parsed_limit = 0;
        const cJSON *arg_limit = cJSON_GetObjectItemCaseSensitive(args, "max_failures");
        const cJSON *arg_limit_alt = cJSON_GetObjectItemCaseSensitive(args, "maxFailures");
        const char *limit_name = arg_limit ? "max_failures" :
                                 (arg_limit_alt ? "maxFailures" : "max_failures");
        if (tf_json_size_value(max_failures_json, limit_name,
                               0, TF_MAX_COUNT_ARG,
                               &parsed_limit, "validate") < 0) {
            cJSON_Delete(rules_doc);
            validate_state_free(st);
            return NULL;
        }
        st->max_failures = parsed_limit;
        st->has_max_failures = 1;
    }

    const cJSON *max_rate_json = validate_arg(args, rules_doc, "max_failure_rate", "maxFailureRate");
    if (max_rate_json) {
        if (!cJSON_IsNumber(max_rate_json) || !isfinite(max_rate_json->valuedouble) ||
            max_rate_json->valuedouble < 0.0 || max_rate_json->valuedouble > 1.0) {
            tf_set_last_error("validate: max_failure_rate must be between 0 and 1");
            cJSON_Delete(rules_doc);
            validate_state_free(st);
            return NULL;
        }
        st->max_failure_rate = max_rate_json->valuedouble;
        st->has_max_failure_rate = 1;
    }

    const cJSON *warn_rate_json = validate_arg(args, rules_doc, "warn_failure_rate", "warnFailureRate");
    if (warn_rate_json) {
        if (!cJSON_IsNumber(warn_rate_json) || !isfinite(warn_rate_json->valuedouble) ||
            warn_rate_json->valuedouble < 0.0 || warn_rate_json->valuedouble > 1.0) {
            tf_set_last_error("validate: warn_failure_rate must be between 0 and 1");
            cJSON_Delete(rules_doc);
            validate_state_free(st);
            return NULL;
        }
        st->warn_failure_rate = warn_rate_json->valuedouble;
        st->has_warn_failure_rate = 1;
    }

    const cJSON *file_rules = validate_rules_from_doc(rules_doc);
    if (rules_doc && !file_rules) {
        tf_set_last_error("validate: rules_file must contain a rules array/object or rule map");
        cJSON_Delete(rules_doc);
        validate_state_free(st);
        return NULL;
    }
    if (file_rules && parse_validate_rules(st, file_rules) != TF_OK) {
        tf_set_last_error("validate: invalid rules_file rules");
        cJSON_Delete(rules_doc);
        validate_state_free(st);
        return NULL;
    }

    const cJSON *rules_json = cJSON_GetObjectItemCaseSensitive(args, "rules");
    if (rules_json && parse_validate_rules(st, rules_json) != TF_OK) {
        tf_set_last_error("validate: invalid rules");
        cJSON_Delete(rules_doc);
        validate_state_free(st);
        return NULL;
    }

    const cJSON *expr_json = cJSON_GetObjectItemCaseSensitive(args, "expr");
    if (expr_json) {
        if (!cJSON_IsString(expr_json) || !expr_json->valuestring[0]) {
            tf_set_last_error("validate: expr must be a non-empty string");
            cJSON_Delete(rules_doc);
            validate_state_free(st);
            return NULL;
        }
        if (add_validate_rule(st, expr_json->valuestring, st->name, st->message) != TF_OK) {
            tf_set_last_error("validate: invalid expr");
            cJSON_Delete(rules_doc);
            validate_state_free(st);
            return NULL;
        }
    }

    if (st->n_rules == 0) {
        tf_set_last_error("validate: expr, rules, or rules_file is required");
        cJSON_Delete(rules_doc);
        validate_state_free(st);
        return NULL;
    }
    cJSON_Delete(rules_doc);

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { validate_state_free(st); return NULL; }
    step->process = validate_process;
    step->flush = validate_flush;
    step->append_stats = validate_append_stats;
    step->destroy = validate_destroy;
    step->state = st;
    return step;
}
