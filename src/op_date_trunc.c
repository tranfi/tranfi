/*
 * op_date_trunc.c — Truncate date/timestamp to a granularity.
 *
 * Config: {"column": "ts", "trunc": "month"}
 *
 * Supported granularities: year, month, day, hour, minute, second.
 * Input: TF_TYPE_DATE, TF_TYPE_TIMESTAMP, or TF_TYPE_STRING (auto-parsed).
 * Output: same type as input (truncated in place).
 */

#include "internal.h"
#include "date_utils.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef enum {
    TRUNC_YEAR,
    TRUNC_MONTH,
    TRUNC_DAY,
    TRUNC_HOUR,
    TRUNC_MINUTE,
    TRUNC_SECOND,
} trunc_level;

typedef enum {
    DATE_TRUNC_MISSING_ERROR,
    DATE_TRUNC_MISSING_NULL,
    DATE_TRUNC_MISSING_IGNORE,
} date_trunc_missing_policy;

typedef enum {
    DATE_TRUNC_TYPE_FAIL,
    DATE_TRUNC_TYPE_NULL,
} date_trunc_type_policy;

typedef struct {
    char       *column;
    char       *result;
    trunc_level level;
    date_trunc_missing_policy missing;
    date_trunc_type_policy on_type_error;
} date_trunc_state;

static int parse_level(const char *s, trunc_level *out) {
    if (!s) return TF_ERROR;
    if (strcmp(s, "year") == 0) { *out = TRUNC_YEAR; return TF_OK; }
    if (strcmp(s, "month") == 0) { *out = TRUNC_MONTH; return TF_OK; }
    if (strcmp(s, "day") == 0) { *out = TRUNC_DAY; return TF_OK; }
    if (strcmp(s, "hour") == 0) { *out = TRUNC_HOUR; return TF_OK; }
    if (strcmp(s, "minute") == 0) { *out = TRUNC_MINUTE; return TF_OK; }
    if (strcmp(s, "second") == 0) { *out = TRUNC_SECOND; return TF_OK; }
    return TF_ERROR;
}

static int date_trunc_is_temporal_type(tf_type type) {
    return type == TF_TYPE_STRING || type == TF_TYPE_DATE || type == TF_TYPE_TIMESTAMP;
}

/* Truncate a date (days since epoch) to the given level. */
static int32_t trunc_date(int32_t days, trunc_level level) {
    int y, m, d;
    tf_date_to_ymd(days, &y, &m, &d);
    switch (level) {
        case TRUNC_YEAR:   return tf_date_from_ymd(y, 1, 1);
        case TRUNC_MONTH:  return tf_date_from_ymd(y, m, 1);
        default:           return days; /* day or finer — date is already at day granularity */
    }
}

/* Truncate a timestamp (microseconds since epoch) to the given level. */
static int64_t trunc_timestamp(int64_t us, trunc_level level) {
    int y, mo, d, h, mi, s, frac;
    tf_timestamp_to_parts(us, &y, &mo, &d, &h, &mi, &s, &frac);
    switch (level) {
        case TRUNC_YEAR:   return tf_timestamp_from_parts(y, 1, 1, 0, 0, 0, 0);
        case TRUNC_MONTH:  return tf_timestamp_from_parts(y, mo, 1, 0, 0, 0, 0);
        case TRUNC_DAY:    return tf_timestamp_from_parts(y, mo, d, 0, 0, 0, 0);
        case TRUNC_HOUR:   return tf_timestamp_from_parts(y, mo, d, h, 0, 0, 0);
        case TRUNC_MINUTE: return tf_timestamp_from_parts(y, mo, d, h, mi, 0, 0);
        case TRUNC_SECOND: return tf_timestamp_from_parts(y, mo, d, h, mi, s, 0);
    }
    return us;
}

/* Try to parse a date string. Returns 1 on success. */
static int parse_date_string(const char *s, int *y, int *m, int *d) {
    return sscanf(s, "%d-%d-%d", y, m, d) == 3;
}

static int date_trunc_set_error(tf_side_channels *side, const char *msg) {
    return tf_side_write_error(side, msg);
}

static int date_trunc_set_col_error(tf_side_channels *side, const char *column, const char *suffix) {
    char msg[512];
    snprintf(msg, sizeof(msg), "date-trunc: column '%s' %s", column ? column : "", suffix);
    return date_trunc_set_error(side, msg);
}

static int date_trunc_parse_missing_policy(const cJSON *args, date_trunc_missing_policy *out) {
    *out = DATE_TRUNC_MISSING_ERROR;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "missing");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("date-trunc: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "error") == 0) *out = DATE_TRUNC_MISSING_ERROR;
    else if (strcmp(j->valuestring, "null") == 0) *out = DATE_TRUNC_MISSING_NULL;
    else if (strcmp(j->valuestring, "ignore") == 0) *out = DATE_TRUNC_MISSING_IGNORE;
    else {
        tf_set_last_error("date-trunc: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    return TF_OK;
}

static int date_trunc_parse_type_policy(const cJSON *args, date_trunc_type_policy *out) {
    *out = DATE_TRUNC_TYPE_FAIL;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "on_type_error");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("date-trunc: on_type_error must be fail or null");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "fail") == 0) *out = DATE_TRUNC_TYPE_FAIL;
    else if (strcmp(j->valuestring, "null") == 0) *out = DATE_TRUNC_TYPE_NULL;
    else {
        tf_set_last_error("date-trunc: on_type_error must be fail or null");
        return TF_ERROR;
    }
    return TF_OK;
}

static int date_trunc_passthrough(tf_batch *in, tf_batch **out) {
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
        ob->n_rows = r + 1;
    }
    *out = ob;
    return TF_OK;
}

static int date_trunc_null_output(date_trunc_state *st, tf_batch *in, int ci, tf_batch **out) {
    int in_place = ci >= 0 && strcmp(st->result, st->column) == 0;
    size_t result_col = in_place ? (size_t)ci : in->n_cols;
    tf_batch *ob = NULL;
    if (in_place) {
        ob = tf_batch_create(in->n_cols, in->n_rows);
        if (!ob) return TF_ERROR;
        if (tf_batch_clone_schema(ob, in) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    } else {
        const char *extra_names[1] = {st->result};
        tf_type extra_types[1] = {ci >= 0 ? in->col_types[ci] : TF_TYPE_STRING};
        ob = tf_batch_create(in->n_cols + 1, in->n_rows);
        if (!ob) return TF_ERROR;
        if (tf_batch_clone_with_extra_cols(ob, in, extra_names, extra_types, 1) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK ||
            tf_batch_set_null(ob, r, result_col) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        ob->n_rows = r + 1;
    }
    *out = ob;
    return TF_OK;
}

static int date_trunc_process(tf_step *self, tf_batch *in, tf_batch **out,
                              tf_side_channels *side) {
    date_trunc_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);
    if (ci < 0) {
        if (st->missing == DATE_TRUNC_MISSING_ERROR) {
            if (date_trunc_set_col_error(side, st->column, "not found") != TF_OK) return TF_ERROR;
            return TF_ERROR;
        }
        if (st->missing == DATE_TRUNC_MISSING_IGNORE) return date_trunc_passthrough(in, out);
        return date_trunc_null_output(st, in, ci, out);
    }
    if (!date_trunc_is_temporal_type(in->col_types[ci])) {
        if (st->on_type_error == DATE_TRUNC_TYPE_FAIL) {
            if (date_trunc_set_col_error(side, st->column, "must be string, date, or timestamp") != TF_OK) return TF_ERROR;
            return TF_ERROR;
        }
        return date_trunc_null_output(st, in, ci, out);
    }

    int in_place = (strcmp(st->result, st->column) == 0);

    size_t out_cols = in_place ? in->n_cols : in->n_cols + 1;
    tf_batch *ob = tf_batch_create(out_cols, in->n_rows);
    if (!ob) return TF_ERROR;

    size_t result_col;
    if (in_place) {
        if (tf_batch_clone_schema(ob, in) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        result_col = (size_t)ci;
    } else {
        const char *extra_names[1] = {st->result};
        tf_type extra_types[1] = {in->col_types[ci]};
        if (tf_batch_clone_with_extra_cols(ob, in, extra_names, extra_types, 1) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        result_col = in->n_cols;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }

        if (tf_batch_is_null(in, r, (size_t)ci)) {
            if (!in_place && tf_batch_set_null(ob, r, result_col) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            ob->n_rows = r + 1;
            continue;
        }

        switch (in->col_types[ci]) {
            case TF_TYPE_DATE: {
                int32_t d = tf_batch_get_date(in, r, ci);
                if (tf_batch_set_date(ob, r, result_col, trunc_date(d, st->level)) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
                break;
            }
            case TF_TYPE_TIMESTAMP: {
                int64_t ts = tf_batch_get_timestamp(in, r, ci);
                if (tf_batch_set_timestamp(ob, r, result_col, trunc_timestamp(ts, st->level)) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
                break;
            }
            case TF_TYPE_STRING: {
                const char *s = tf_batch_get_string(in, r, ci);
                int y, m, d;
                if (s && parse_date_string(s, &y, &m, &d)) {
                    int32_t days = tf_date_from_ymd(y, m, d);
                    int32_t trunc = trunc_date(days, st->level);
                    char buf[32];
                    tf_date_format(trunc, buf, sizeof(buf));
                    if (tf_batch_set_string(ob, r, result_col, buf) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
                } else {
                    if (tf_batch_set_string(ob, r, result_col, s ? s : "") != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
                }
                break;
            }
            default:
                tf_batch_free(ob);
                return TF_ERROR;
        }
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int date_trunc_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void date_trunc_state_free(date_trunc_state *st) {
    if (!st) return;
    free(st->column);
    free(st->result);
    free(st);
}

static void date_trunc_destroy(tf_step *self) {
    if (self) date_trunc_state_free(self->state);
    free(self);
}

tf_step *tf_date_trunc_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    cJSON *trunc_j = cJSON_GetObjectItemCaseSensitive(args, "trunc");
    if (!cJSON_IsString(col_j) || !col_j->valuestring[0] || !cJSON_IsString(trunc_j) || !trunc_j->valuestring[0]) {
        tf_set_last_error("date-trunc: column and trunc are required");
        return NULL;
    }

    date_trunc_state *st = calloc(1, sizeof(date_trunc_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    if (!st->column) { free(st); return NULL; }
    if (parse_level(trunc_j->valuestring, &st->level) != TF_OK) {
        tf_set_last_error("date-trunc: trunc must be year, month, day, hour, minute, or second");
        date_trunc_state_free(st);
        return NULL;
    }

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        if (!res_j->valuestring[0]) {
            tf_set_last_error("date-trunc: result cannot be empty");
            date_trunc_state_free(st);
            return NULL;
        }
        st->result = strdup(res_j->valuestring);
    } else {
        /* Default: overwrite in place */
        st->result = strdup(st->column);
    }
    if (!st->result) { date_trunc_state_free(st); return NULL; }

    if (date_trunc_parse_missing_policy(args, &st->missing) != TF_OK ||
        date_trunc_parse_type_policy(args, &st->on_type_error) != TF_OK) {
        date_trunc_state_free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { date_trunc_state_free(st); return NULL; }
    step->process = date_trunc_process;
    step->flush = date_trunc_flush;
    step->destroy = date_trunc_destroy;
    step->state = st;
    return step;
}
