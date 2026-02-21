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
} replace_state;

static int replace_process(tf_step *self, tf_batch *in, tf_batch **out,
                           tf_side_channels *side) {
    (void)side;
    replace_state *st = self->state;
    *out = NULL;

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
                if (tf_batch_is_null(ob, r, ci)) continue;
                const char *val = tf_batch_get_string(ob, r, ci);
                /* Build result by repeatedly matching and replacing */
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
                    tf_batch_set_string(ob, r, ci, buf);
                }
                free(buf);
            }
        } else {
            /* Substring mode */
            size_t pat_len = strlen(st->pattern);
            size_t rep_len = strlen(st->replacement);
            if (pat_len > 0) {
                for (size_t r = 0; r < ob->n_rows; r++) {
                    if (tf_batch_is_null(ob, r, ci)) continue;
                    const char *val = tf_batch_get_string(ob, r, ci);
                    /* Count occurrences */
                    size_t count = 0;
                    const char *p = val;
                    while ((p = strstr(p, st->pattern)) != NULL) { count++; p += pat_len; }
                    if (count == 0) continue;

                    size_t new_len = strlen(val) + count * (rep_len - pat_len);
                    char *buf = malloc(new_len + 1);
                    if (!buf) continue;
                    char *dst = buf;
                    p = val;
                    const char *found;
                    while ((found = strstr(p, st->pattern)) != NULL) {
                        size_t prefix = found - p;
                        memcpy(dst, p, prefix);
                        dst += prefix;
                        memcpy(dst, st->replacement, rep_len);
                        dst += rep_len;
                        p = found + pat_len;
                    }
                    strcpy(dst, p);
                    tf_batch_set_string(ob, r, ci, buf);
                    free(buf);
                }
            }
        }
    }

    *out = ob;
    return TF_OK;
}

static int replace_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void replace_destroy(tf_step *self) {
    replace_state *st = self->state;
    if (st) {
        if (st->use_regex) regfree(&st->compiled);
        free(st->column);
        free(st->pattern);
        free(st->replacement);
        free(st);
    }
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

    cJSON *regex_j = cJSON_GetObjectItemCaseSensitive(args, "regex");
    st->use_regex = (cJSON_IsBool(regex_j) && cJSON_IsTrue(regex_j)) ? 1 : 0;

    if (st->use_regex) {
        int rc = regcomp(&st->compiled, st->pattern, REG_EXTENDED);
        if (rc != 0) {
            free(st->column); free(st->pattern); free(st->replacement); free(st);
            return NULL;
        }
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) {
        if (st->use_regex) regfree(&st->compiled);
        free(st->column); free(st->pattern); free(st->replacement); free(st);
        return NULL;
    }
    step->process = replace_process;
    step->flush = replace_flush;
    step->destroy = replace_destroy;
    step->state = st;
    return step;
}
