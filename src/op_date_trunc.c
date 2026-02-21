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

typedef struct {
    char       *column;
    char       *result;
    trunc_level level;
} date_trunc_state;

static trunc_level parse_level(const char *s) {
    if (strcmp(s, "year") == 0) return TRUNC_YEAR;
    if (strcmp(s, "month") == 0) return TRUNC_MONTH;
    if (strcmp(s, "day") == 0) return TRUNC_DAY;
    if (strcmp(s, "hour") == 0) return TRUNC_HOUR;
    if (strcmp(s, "minute") == 0) return TRUNC_MINUTE;
    if (strcmp(s, "second") == 0) return TRUNC_SECOND;
    return TRUNC_DAY;
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

static int date_trunc_process(tf_step *self, tf_batch *in, tf_batch **out,
                              tf_side_channels *side) {
    (void)side;
    date_trunc_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);
    int in_place = (strcmp(st->result, st->column) == 0);

    size_t out_cols = in_place ? in->n_cols : in->n_cols + 1;
    tf_batch *ob = tf_batch_create(out_cols, in->n_rows);
    if (!ob) return TF_ERROR;

    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);

    size_t result_col;
    if (in_place) {
        result_col = (size_t)ci;
    } else {
        result_col = in->n_cols;
        /* Output type matches input column type */
        tf_type out_type = (ci >= 0) ? in->col_types[ci] : TF_TYPE_STRING;
        tf_batch_set_schema(ob, result_col, st->result, out_type);
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);

        if (ci < 0 || tf_batch_is_null(in, r, ci)) {
            if (!in_place) tf_batch_set_null(ob, r, result_col);
            ob->n_rows = r + 1;
            continue;
        }

        switch (in->col_types[ci]) {
            case TF_TYPE_DATE: {
                int32_t d = tf_batch_get_date(in, r, ci);
                tf_batch_set_date(ob, r, result_col, trunc_date(d, st->level));
                break;
            }
            case TF_TYPE_TIMESTAMP: {
                int64_t ts = tf_batch_get_timestamp(in, r, ci);
                tf_batch_set_timestamp(ob, r, result_col, trunc_timestamp(ts, st->level));
                break;
            }
            case TF_TYPE_STRING: {
                const char *s = tf_batch_get_string(in, r, ci);
                int y, m, d;
                if (parse_date_string(s, &y, &m, &d)) {
                    int32_t days = tf_date_from_ymd(y, m, d);
                    int32_t trunc = trunc_date(days, st->level);
                    char buf[16];
                    tf_date_format(trunc, buf, sizeof(buf));
                    tf_batch_set_string(ob, r, result_col, buf);
                } else {
                    tf_batch_set_string(ob, r, result_col, s);
                }
                break;
            }
            default:
                if (!in_place) tf_batch_set_null(ob, r, result_col);
                break;
        }
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int date_trunc_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void date_trunc_destroy(tf_step *self) {
    date_trunc_state *st = self->state;
    if (st) { free(st->column); free(st->result); free(st); }
    free(self);
}

tf_step *tf_date_trunc_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    cJSON *trunc_j = cJSON_GetObjectItemCaseSensitive(args, "trunc");
    if (!cJSON_IsString(col_j) || !cJSON_IsString(trunc_j)) return NULL;

    date_trunc_state *st = calloc(1, sizeof(date_trunc_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    st->level = parse_level(trunc_j->valuestring);

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = strdup(res_j->valuestring);
    } else {
        /* Default: overwrite in place */
        st->result = strdup(st->column);
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->column); free(st->result); free(st); return NULL; }
    step->process = date_trunc_process;
    step->flush = date_trunc_flush;
    step->destroy = date_trunc_destroy;
    step->state = st;
    return step;
}
