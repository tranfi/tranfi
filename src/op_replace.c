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
    cJSON_AddStringToObject(obj, "type", "audit");
    cJSON_AddStringToObject(obj, "op", "replace");
    cJSON_AddStringToObject(obj, "event", "value_changed");
    cJSON_AddStringToObject(obj, "reason", "replace_match");
    cJSON_AddStringToObject(obj, "channel", "audit");
    const char *column_name = b->col_names[col] ? b->col_names[col] : "";
    cJSON_AddStringToObject(obj, "column", column_name);
    char pattern_buf[256];
    char replacement_buf[256];
    char before_buf[256];
    char after_buf[256];
    const char *safe_pattern = tf_audit_format_string_for_column(&st->audit_opts, column_name,
                                                                 st->pattern, pattern_buf, sizeof(pattern_buf));
    const char *safe_replacement = tf_audit_format_string_for_column(&st->audit_opts, column_name,
                                                                     st->replacement, replacement_buf, sizeof(replacement_buf));
    cJSON_AddStringToObject(obj, "pattern", safe_pattern ? safe_pattern : "");
    cJSON_AddStringToObject(obj, "replacement", safe_replacement ? safe_replacement : "");
    cJSON_AddBoolToObject(obj, "regex", st->use_regex ? 1 : 0);
    cJSON_AddNumberToObject(obj, "row", (double)row_no);
    const char *safe_before = tf_audit_format_string_for_column(&st->audit_opts, column_name, before, before_buf, sizeof(before_buf));
    const char *safe_after = tf_audit_format_string_for_column(&st->audit_opts, column_name, after, after_buf, sizeof(after_buf));
    cJSON_AddStringToObject(obj, "before", safe_before ? safe_before : "");
    cJSON_AddStringToObject(obj, "after", safe_after ? safe_after : "");
    cJSON *row_obj = tf_audit_row_to_json(b, row, &st->audit_opts);
    if (row_obj) cJSON_AddItemToObject(obj, "data", row_obj);
    int rc = tf_buffer_write_json_line(side->stats, obj);
    cJSON_Delete(obj);
    if (rc == TF_OK) st->audit_emitted++;
    return rc;
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
    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    int ci = tf_batch_col_index(ob, st->column);
    if (ci >= 0 && ob->col_types[ci] == TF_TYPE_STRING) {
        if (st->use_regex) {
            /* Regex mode: use regexec to find matches */
            for (size_t r = 0; r < ob->n_rows; r++) {
                if (tf_batch_is_null(ob, r, (size_t)ci)) continue;
                const char *val = tf_batch_get_string(ob, r, (size_t)ci);
                size_t buf_cap = 0;
                char *buf = NULL;
                size_t buf_len = 0;
                const char *p = val;
                regmatch_t match;
                int replaced = 0;
                while (regexec(&st->compiled, p, 1, &match, 0) == 0 && match.rm_so >= 0) {
                    replaced = 1;
                    size_t prefix = (size_t)match.rm_so;
                    size_t match_len = (size_t)(match.rm_eo - match.rm_so);
                    if (replace_append_bytes(&buf, &buf_cap, &buf_len, p, prefix) != TF_OK) {
                        free(buf);
                        tf_batch_free(ob);
                        return TF_ERROR;
                    }
                    /* Expand replacement: & refers to whole match */
                    for (const char *rp = st->replacement; *rp; rp++) {
                        if (*rp == '&') {
                            if (replace_append_bytes(&buf, &buf_cap, &buf_len,
                                                     p + match.rm_so, match_len) != TF_OK) {
                                free(buf);
                                tf_batch_free(ob);
                                return TF_ERROR;
                            }
                        } else {
                            if (replace_append_char(&buf, &buf_cap, &buf_len, *rp) != TF_OK) {
                                free(buf);
                                tf_batch_free(ob);
                                return TF_ERROR;
                            }
                        }
                    }
                    p += match.rm_eo;
                    if (match_len == 0) {
                        /* Zero-length match: copy one char to avoid infinite loop */
                        if (*p) {
                            if (replace_append_char(&buf, &buf_cap, &buf_len, *p++) != TF_OK) {
                                free(buf);
                                tf_batch_free(ob);
                                return TF_ERROR;
                            }
                        } else break;
                    }
                }
                if (replaced) {
                    /* Copy remaining */
                    size_t rest = strlen(p);
                    if (replace_append_bytes(&buf, &buf_cap, &buf_len, p, rest) != TF_OK) {
                        free(buf);
                        tf_batch_free(ob);
                        return TF_ERROR;
                    }
                    char *before_copy = NULL;
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
                    const char *after = tf_batch_get_string(ob, r, (size_t)ci);
                    if (emit_replace_audit(st, ob, r, (size_t)ci, row_base + r + 1, before_copy ? before_copy : val, after, side) != TF_OK) {
                        free(before_copy);
                        free(buf);
                        tf_batch_free(ob);
                        return TF_ERROR;
                    }
                    free(before_copy);
                }
                free(buf);
            }
        } else {
            /* Substring mode */
            size_t pat_len = strlen(st->pattern);
            size_t rep_len = strlen(st->replacement);
            if (pat_len > 0) {
                for (size_t r = 0; r < ob->n_rows; r++) {
                    if (tf_batch_is_null(ob, r, (size_t)ci)) continue;
                    const char *val = tf_batch_get_string(ob, r, (size_t)ci);
                    const char *p = val;
                    if (!strstr(p, st->pattern)) continue;

                    size_t buf_cap = 0;
                    size_t buf_len = 0;
                    char *buf = NULL;
                    const char *found;
                    while ((found = strstr(p, st->pattern)) != NULL) {
                        size_t prefix = (size_t)(found - p);
                        if (replace_append_bytes(&buf, &buf_cap, &buf_len, p, prefix) != TF_OK ||
                            replace_append_bytes(&buf, &buf_cap, &buf_len, st->replacement, rep_len) != TF_OK) {
                            free(buf);
                            tf_batch_free(ob);
                            return TF_ERROR;
                        }
                        p = found + pat_len;
                    }
                    if (replace_append_bytes(&buf, &buf_cap, &buf_len, p, strlen(p)) != TF_OK) {
                        free(buf);
                        tf_batch_free(ob);
                        return TF_ERROR;
                    }
                    char *before_copy = NULL;
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
                    const char *after = tf_batch_get_string(ob, r, (size_t)ci);
                    if (emit_replace_audit(st, ob, r, (size_t)ci, row_base + r + 1, before_copy ? before_copy : val, after, side) != TF_OK) {
                        free(before_copy);
                        free(buf);
                        tf_batch_free(ob);
                        return TF_ERROR;
                    }
                    free(before_copy);
                    free(buf);
                }
            }
        }
    }

    st->row_index += in->n_rows;
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
