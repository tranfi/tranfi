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
} replace_state;

static cJSON *replace_cell_to_json(const tf_batch *b, size_t row, size_t col) {
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

static cJSON *replace_row_to_json(const tf_batch *b, size_t row) {
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return NULL;
    for (size_t c = 0; c < b->n_cols; c++) {
        cJSON *value = replace_cell_to_json(b, row, c);
        if (!value) { cJSON_Delete(obj); return NULL; }
        cJSON_AddItemToObject(obj, b->col_names[c] ? b->col_names[c] : "", value);
    }
    return obj;
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
    cJSON_AddStringToObject(obj, "column", b->col_names[col] ? b->col_names[col] : "");
    cJSON_AddStringToObject(obj, "pattern", st->pattern ? st->pattern : "");
    cJSON_AddStringToObject(obj, "replacement", st->replacement ? st->replacement : "");
    cJSON_AddBoolToObject(obj, "regex", st->use_regex ? 1 : 0);
    cJSON_AddNumberToObject(obj, "row", (double)row_no);
    cJSON_AddStringToObject(obj, "before", before ? before : "");
    cJSON_AddStringToObject(obj, "after", after ? after : "");
    cJSON *row_obj = replace_row_to_json(b, row);
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

static int replace_process(tf_step *self, tf_batch *in, tf_batch **out,
                           tf_side_channels *side) {
    replace_state *st = self->state;
    *out = NULL;
    size_t row_base = st->row_index;

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);
        ob->n_rows = r + 1;
    }

    int ci = tf_batch_col_index(ob, st->column);
    if (ci >= 0 && ob->col_types[ci] == TF_TYPE_STRING) {
        if (st->use_regex) {
            /* Regex mode: use regexec to find matches */
            for (size_t r = 0; r < ob->n_rows; r++) {
                if (tf_batch_is_null(ob, r, (size_t)ci)) continue;
                const char *val = tf_batch_get_string(ob, r, (size_t)ci);
                size_t val_len = strlen(val);
                size_t buf_cap = val_len * 2 + 64;
                char *buf = malloc(buf_cap);
                if (!buf) continue;
                size_t buf_len = 0;
                const char *p = val;
                regmatch_t match;
                int replaced = 0;
                while (regexec(&st->compiled, p, 1, &match, 0) == 0 && match.rm_so >= 0) {
                    replaced = 1;
                    size_t prefix = (size_t)match.rm_so;
                    size_t match_len = (size_t)(match.rm_eo - match.rm_so);
                    /* Expand replacement: & refers to whole match */
                    size_t rep_need = 0;
                    for (const char *rp = st->replacement; *rp; rp++) {
                        if (*rp == '&') rep_need += match_len;
                        else rep_need++;
                    }
                    size_t need = buf_len + prefix + rep_need + 1;
                    if (need > buf_cap) {
                        buf_cap = need * 2;
                        char *nb = realloc(buf, buf_cap);
                        if (!nb) { free(buf); buf = NULL; break; }
                        buf = nb;
                    }
                    memcpy(buf + buf_len, p, prefix);
                    buf_len += prefix;
                    for (const char *rp = st->replacement; *rp; rp++) {
                        if (*rp == '&') {
                            memcpy(buf + buf_len, p + match.rm_so, match_len);
                            buf_len += match_len;
                        } else {
                            buf[buf_len++] = *rp;
                        }
                    }
                    p += match.rm_eo;
                    if (match_len == 0) {
                        /* Zero-length match: copy one char to avoid infinite loop */
                        if (*p) {
                            if (buf_len + 2 > buf_cap) {
                                buf_cap = (buf_len + 2) * 2;
                                char *nb = realloc(buf, buf_cap);
                                if (!nb) { free(buf); buf = NULL; break; }
                                buf = nb;
                            }
                            buf[buf_len++] = *p++;
                        } else break;
                    }
                }
                if (buf && replaced) {
                    /* Copy remaining */
                    size_t rest = strlen(p);
                    if (buf_len + rest + 1 > buf_cap) {
                        buf_cap = buf_len + rest + 1;
                        char *nb = realloc(buf, buf_cap);
                        if (!nb) { free(buf); continue; }
                        buf = nb;
                    }
                    memcpy(buf + buf_len, p, rest);
                    buf_len += rest;
                    buf[buf_len] = '\0';
                    const char *before = val;
                    tf_batch_set_string(ob, r, (size_t)ci, buf);
                    const char *after = tf_batch_get_string(ob, r, (size_t)ci);
                    if (emit_replace_audit(st, ob, r, (size_t)ci, row_base + r + 1, before, after, side) != TF_OK) {
                        free(buf);
                        tf_batch_free(ob);
                        return TF_ERROR;
                    }
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
                    /* Count occurrences */
                    size_t count = 0;
                    const char *p = val;
                    while ((p = strstr(p, st->pattern)) != NULL) { count++; p += pat_len; }
                    if (count == 0) continue;

                    size_t val_len = strlen(val);
                    size_t new_len = rep_len >= pat_len
                        ? val_len + count * (rep_len - pat_len)
                        : val_len - count * (pat_len - rep_len);
                    char *buf = malloc(new_len + 1);
                    if (!buf) continue;
                    char *dst = buf;
                    p = val;
                    const char *found;
                    while ((found = strstr(p, st->pattern)) != NULL) {
                        size_t prefix = (size_t)(found - p);
                        memcpy(dst, p, prefix);
                        dst += prefix;
                        memcpy(dst, st->replacement, rep_len);
                        dst += rep_len;
                        p = found + pat_len;
                    }
                    strcpy(dst, p);
                    const char *before = val;
                    tf_batch_set_string(ob, r, (size_t)ci, buf);
                    const char *after = tf_batch_get_string(ob, r, (size_t)ci);
                    if (emit_replace_audit(st, ob, r, (size_t)ci, row_base + r + 1, before, after, side) != TF_OK) {
                        free(buf);
                        tf_batch_free(ob);
                        return TF_ERROR;
                    }
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
        if (!cJSON_IsNumber(audit_limit_j) || audit_limit_j->valuedouble <= 0) {
            tf_set_last_error("replace: audit_limit must be a positive integer");
            replace_state_free(st);
            return NULL;
        }
        st->audit_limit = (size_t)audit_limit_j->valuedouble;
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
