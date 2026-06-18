/*
 * op_replace.c — String find/replace (substring or regex).
 *
 * Config: {"column": "name", "pattern": "foo", "replacement": "bar", "regex": false}
 * In regex mode, & in replacement refers to the whole match.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <regex.h>

typedef struct {
    char   *column;
    char   *pattern;
    char   *replacement;
    int     use_regex;
    regex_t compiled;
    size_t  row_index;
    size_t  audit_limit;
    size_t  audit_emitted;
    int     audit;
    tf_audit_options audit_opts;
} replace_state;

static int replace_ensure_buffer(char **buf, size_t *cap, size_t need) {
    if (!buf || !cap) return TF_ERROR;
    if (need > 0) {
        size_t payload_len = need - 1; /* reserve space for the trailing NUL */
        if (tf_check_byte_limit(payload_len, TF_MAX_CELL_BYTES, "replace", "output cell") != TF_OK) {
            return TF_ERROR;
        }
    }
    if (need <= *cap) return TF_OK;
    size_t new_cap = 0;
    if (tf_size_grow_pow2(*cap, need, 64, &new_cap) != TF_OK) return TF_ERROR;
    size_t max_cap = 0;
    if (tf_size_add(TF_MAX_CELL_BYTES, 1, &max_cap) != TF_OK) return TF_ERROR;
    if (new_cap > max_cap) new_cap = need;
    char *nb = tf_reallocarray_checked(*buf, new_cap, sizeof(char));
    if (!nb) return TF_ERROR;
    *buf = nb;
    *cap = new_cap;
    return TF_OK;
}

static int replace_append_bytes(char **buf, size_t *cap, size_t *len,
                                const char *src, size_t n) {
    if (!buf || !cap || !len || (!src && n > 0)) return TF_ERROR;
    size_t end = 0;
    size_t need = 0;
    if (tf_size_add(*len, n, &end) != TF_OK ||
        tf_size_add(end, 1, &need) != TF_OK) {
        return TF_ERROR;
    }
    if (replace_ensure_buffer(buf, cap, need) != TF_OK) return TF_ERROR;
    if (n > 0) memcpy(*buf + *len, src, n);
    *len = end;
    (*buf)[*len] = '\0';
    return TF_OK;
}

static int replace_append_char(char **buf, size_t *cap, size_t *len, char ch) {
    return replace_append_bytes(buf, cap, len, &ch, 1);
}

static int emit_replace_audit(replace_state *st, const tf_batch *b, size_t row, size_t col,
                              size_t row_no, const char *before, const char *after,
                              tf_side_channels *side) {
    if (!st->audit || st->audit_emitted >= st->audit_limit || !side || !side->stats) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    int rc = TF_ERROR;
    if (tf_json_add_string(obj, "type", "audit") != TF_OK ||
        tf_json_add_string(obj, "op", "replace") != TF_OK ||
        tf_json_add_string(obj, "event", "value_changed") != TF_OK ||
        tf_json_add_string(obj, "reason", "replace_match") != TF_OK ||
        tf_json_add_string(obj, "channel", "audit") != TF_OK) {
        goto done;
    }
    const char *column_name = b->col_names[col] ? b->col_names[col] : "";
    if (tf_json_add_string(obj, "column", column_name) != TF_OK) goto done;
    char pattern_buf[256];
    char replacement_buf[256];
    char before_buf[256];
    char after_buf[256];
    const char *safe_pattern = tf_audit_format_string_for_column(&st->audit_opts, column_name,
                                                                 st->pattern, pattern_buf, sizeof(pattern_buf));
    const char *safe_replacement = tf_audit_format_string_for_column(&st->audit_opts, column_name,
                                                                     st->replacement, replacement_buf, sizeof(replacement_buf));
    if (tf_json_add_string(obj, "pattern", safe_pattern ? safe_pattern : "") != TF_OK ||
        tf_json_add_string(obj, "replacement", safe_replacement ? safe_replacement : "") != TF_OK ||
        tf_json_add_bool(obj, "regex", st->use_regex ? 1 : 0) != TF_OK ||
        tf_json_add_number(obj, "row", (double)row_no) != TF_OK) {
        goto done;
    }
    const char *safe_before = tf_audit_format_string_for_column(&st->audit_opts, column_name, before, before_buf, sizeof(before_buf));
    const char *safe_after = tf_audit_format_string_for_column(&st->audit_opts, column_name, after, after_buf, sizeof(after_buf));
    if (tf_json_add_string(obj, "before", safe_before ? safe_before : "") != TF_OK ||
        tf_json_add_string(obj, "after", safe_after ? safe_after : "") != TF_OK) {
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

static int replace_build_regex(replace_state *st, const char *val,
                               char **out_buf, int *out_replaced) {
    *out_buf = NULL;
    *out_replaced = 0;
    size_t buf_cap = 0;
    size_t buf_len = 0;
    char *buf = NULL;
    const char *p = val;
    regmatch_t match;
    while (regexec(&st->compiled, p, 1, &match, 0) == 0 && match.rm_so >= 0) {
        *out_replaced = 1;
        size_t prefix = (size_t)match.rm_so;
        size_t match_len = (size_t)(match.rm_eo - match.rm_so);
        if (replace_append_bytes(&buf, &buf_cap, &buf_len, p, prefix) != TF_OK) {
            free(buf);
            return TF_ERROR;
        }
        for (const char *rp = st->replacement; *rp; rp++) {
            if (*rp == '&') {
                if (replace_append_bytes(&buf, &buf_cap, &buf_len,
                                         p + match.rm_so, match_len) != TF_OK) {
                    free(buf);
                    return TF_ERROR;
                }
            } else if (replace_append_char(&buf, &buf_cap, &buf_len, *rp) != TF_OK) {
                free(buf);
                return TF_ERROR;
            }
        }
        p += match.rm_eo;
        if (match_len == 0) {
            if (*p) {
                if (replace_append_char(&buf, &buf_cap, &buf_len, *p++) != TF_OK) {
                    free(buf);
                    return TF_ERROR;
                }
            } else {
                break;
            }
        }
    }
    if (*out_replaced) {
        if (replace_append_bytes(&buf, &buf_cap, &buf_len, p, strlen(p)) != TF_OK) {
            free(buf);
            return TF_ERROR;
        }
        *out_buf = buf;
    }
    return TF_OK;
}

static int replace_build_literal(replace_state *st, const char *val,
                                 char **out_buf, int *out_replaced) {
    *out_buf = NULL;
    *out_replaced = 0;
    size_t pat_len = strlen(st->pattern);
    if (pat_len == 0 || !strstr(val, st->pattern)) return TF_OK;

    size_t rep_len = strlen(st->replacement);
    size_t buf_cap = 0;
    size_t buf_len = 0;
    char *buf = NULL;
    const char *p = val;
    const char *found;
    while ((found = strstr(p, st->pattern)) != NULL) {
        size_t prefix = (size_t)(found - p);
        if (replace_append_bytes(&buf, &buf_cap, &buf_len, p, prefix) != TF_OK ||
            replace_append_bytes(&buf, &buf_cap, &buf_len, st->replacement, rep_len) != TF_OK) {
            free(buf);
            return TF_ERROR;
        }
        p = found + pat_len;
    }
    if (replace_append_bytes(&buf, &buf_cap, &buf_len, p, strlen(p)) != TF_OK) {
        free(buf);
        return TF_ERROR;
    }
    *out_replaced = 1;
    *out_buf = buf;
    return TF_OK;
}

static int replace_process(tf_step *self, tf_batch *in, tf_batch **out,
                           tf_side_channels *side) {
    replace_state *st = self->state;
    *out = NULL;
    size_t row_base = st->row_index;

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_schema(ob, in) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    int ci = tf_batch_col_index(in, st->column);
    int can_replace = ci >= 0 && in->col_types[ci] == TF_TYPE_STRING;
    for (size_t r = 0; r < in->n_rows; r++) {
        char *buf = NULL;
        char *before_copy = NULL;
        int replaced = 0;
        const char *val = NULL;
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (can_replace && !tf_batch_is_null(in, r, (size_t)ci)) {
            val = tf_batch_get_string(in, r, (size_t)ci);
            if (val) {
                int build_rc = st->use_regex
                    ? replace_build_regex(st, val, &buf, &replaced)
                    : replace_build_literal(st, val, &buf, &replaced);
                if (build_rc != TF_OK) {
                    tf_batch_free(ob);
                    return TF_ERROR;
                }
                if (replaced) {
                    if (st->audit && st->audit_emitted < st->audit_limit) {
                        before_copy = strdup(val);
                        if (!before_copy) {
                            free(buf);
                            tf_batch_free(ob);
                            return TF_ERROR;
                        }
                    }
                    if (tf_batch_set_string(ob, r, (size_t)ci, buf) != TF_OK) {
                        free(before_copy);
                        free(buf);
                        tf_batch_free(ob);
                        return TF_ERROR;
                    }
                }
            }
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            free(before_copy);
            free(buf);
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (replaced) {
            size_t row_no = 0;
            if (tf_size_add(row_base, r, &row_no) != TF_OK ||
                tf_size_add(row_no, 1, &row_no) != TF_OK) {
                free(before_copy);
                free(buf);
                tf_batch_free(ob);
                return TF_ERROR;
            }
            const char *after = tf_batch_get_string(ob, r, (size_t)ci);
            if (emit_replace_audit(st, ob, r, (size_t)ci, row_no,
                                   before_copy ? before_copy : val, after, side) != TF_OK) {
                free(before_copy);
                free(buf);
                tf_batch_free(ob);
                return TF_ERROR;
            }
        }
        free(before_copy);
        free(buf);
    }

    if (tf_size_add(st->row_index, in->n_rows, &st->row_index) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }
    *out = ob;
    return TF_OK;
}

static int replace_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void replace_state_free(replace_state *st) {
    if (!st) return;
    if (st->use_regex) regfree(&st->compiled);
    tf_audit_options_free(&st->audit_opts);
    free(st->column);
    free(st->pattern);
    free(st->replacement);
    free(st);
}

static void replace_destroy(tf_step *self) {
    if (self) replace_state_free(self->state);
    free(self);
}

tf_step *tf_replace_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    cJSON *pat_j = cJSON_GetObjectItemCaseSensitive(args, "pattern");
    cJSON *rep_j = cJSON_GetObjectItemCaseSensitive(args, "replacement");
    if (!cJSON_IsString(col_j) || !cJSON_IsString(pat_j) || !cJSON_IsString(rep_j))
        return NULL;

    replace_state *st = calloc(1, sizeof(replace_state));
    if (!st) return NULL;
    tf_audit_options_init(&st->audit_opts, 1);
    st->column = strdup(col_j->valuestring);
    st->pattern = strdup(pat_j->valuestring);
    st->replacement = strdup(rep_j->valuestring);
    st->audit_limit = 1000;
    if (!st->column || !st->pattern || !st->replacement) { replace_state_free(st); return NULL; }

    cJSON *regex_j = cJSON_GetObjectItemCaseSensitive(args, "regex");
    st->use_regex = (cJSON_IsBool(regex_j) && cJSON_IsTrue(regex_j)) ? 1 : 0;

    cJSON *audit_j = cJSON_GetObjectItemCaseSensitive(args, "audit");
    st->audit = cJSON_IsTrue(audit_j) ? 1 : 0;
    cJSON *audit_limit_j = cJSON_GetObjectItemCaseSensitive(args, "audit_limit");
    if (!audit_limit_j) audit_limit_j = cJSON_GetObjectItemCaseSensitive(args, "auditLimit");
    if (audit_limit_j) {
        size_t parsed_limit = 0;
        if (tf_json_get_size_arg_any(args, "audit_limit", "auditLimit",
                                     1, TF_MAX_AUDIT_RECORDS,
                                     &parsed_limit, "replace") < 0) {
            replace_state_free(st);
            return NULL;
        }
        st->audit_limit = parsed_limit;
    }

    if (tf_audit_options_parse(&st->audit_opts, args, "replace") != TF_OK) {
        replace_state_free(st);
        return NULL;
    }

    if (st->use_regex) {
        int rc = regcomp(&st->compiled, st->pattern, REG_EXTENDED);
        if (rc != 0) {
            replace_state_free(st);
            return NULL;
        }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) {
        replace_state_free(st);
        return NULL;
    }
    step->process = replace_process;
    step->flush = replace_flush;
    step->destroy = replace_destroy;
    step->state = st;
    return step;
}
