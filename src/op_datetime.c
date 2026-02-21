/*
 * op_datetime.c — Extract date/time components from date strings.
 *
 * Config: {"column": "date", "extract": ["year", "month", "day"]}
 * Supports: YYYY-MM-DD, YYYY-MM-DD HH:MM:SS, epoch seconds.
 * Components: year, month, day, hour, minute, second, weekday, epoch
 */

#include "internal.h"
#include "date_utils.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char  *column;
    int    w_year, w_month, w_day, w_hour, w_minute, w_second, w_weekday, w_epoch;
} datetime_state;

/* Days in each month (non-leap) */
static int days_in_month(int m, int y) {
    static const int d[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    if (m == 2 && ((y%4==0 && y%100!=0) || y%400==0)) return 29;
    return (m >= 1 && m <= 12) ? d[m] : 30;
}

/* Zeller-like weekday: 0=Sunday..6=Saturday */
static int weekday(int y, int m, int d) {
    /* Tomohiko Sakamoto's algorithm */
    static int t[] = {0, 3, 2, 5, 0, 3, 5, 1, 4, 6, 2, 4};
    if (m < 3) y--;
    return (y + y/4 - y/100 + y/400 + t[m-1] + d) % 7;
}

/* Convert date to epoch (seconds since 1970-01-01) */
static int64_t date_to_epoch(int y, int mo, int d, int h, int mi, int s) {
    /* Simplified: assume Gregorian, no timezone */
    int64_t days = 0;
    for (int yr = 1970; yr < y; yr++) {
        days += 365 + ((yr%4==0 && yr%100!=0) || yr%400==0);
    }
    for (int m = 1; m < mo; m++) days += days_in_month(m, y);
    days += d - 1;
    return days * 86400 + h * 3600 + mi * 60 + s;
}

static int parse_date(const char *s, int *y, int *mo, int *d, int *h, int *mi, int *se) {
    *y = *mo = *d = *h = *mi = *se = 0;

    /* Try epoch (pure number) */
    char *end;
    double epoch = strtod(s, &end);
    if (*end == '\0' && end != s) {
        /* Convert epoch to components */
        int64_t ts = (int64_t)epoch;
        *se = ts % 60; ts /= 60;
        *mi = ts % 60; ts /= 60;
        *h = ts % 24; ts /= 24;
        /* Days since 1970-01-01 */
        int64_t dd = ts;
        *y = 1970;
        while (1) {
            int dy = 365 + ((*y%4==0 && *y%100!=0) || *y%400==0);
            if (dd < dy) break;
            dd -= dy;
            (*y)++;
        }
        *mo = 1;
        while (1) {
            int dm = days_in_month(*mo, *y);
            if (dd < dm) break;
            dd -= dm;
            (*mo)++;
        }
        *d = (int)dd + 1;
        return 1;
    }

    /* Try YYYY-MM-DD [HH:MM:SS] */
    int n = sscanf(s, "%d-%d-%d %d:%d:%d", y, mo, d, h, mi, se);
    return n >= 3;
}

static int datetime_process(tf_step *self, tf_batch *in, tf_batch **out,
                            tf_side_channels *side) {
    (void)side;
    datetime_state *st = self->state;
    *out = NULL;

    /* Count extra columns */
    size_t n_extra = 0;
    if (st->w_year) n_extra++;
    if (st->w_month) n_extra++;
    if (st->w_day) n_extra++;
    if (st->w_hour) n_extra++;
    if (st->w_minute) n_extra++;
    if (st->w_second) n_extra++;
    if (st->w_weekday) n_extra++;
    if (st->w_epoch) n_extra++;

    tf_batch *ob = tf_batch_create(in->n_cols + n_extra, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);

    size_t ei = in->n_cols;
    char col_prefix[256];
    snprintf(col_prefix, sizeof(col_prefix), "%s_", st->column);
    char col_name[256];
    if (st->w_year)    { snprintf(col_name, sizeof(col_name), "%syear", col_prefix);    tf_batch_set_schema(ob, ei++, col_name, TF_TYPE_INT64); }
    if (st->w_month)   { snprintf(col_name, sizeof(col_name), "%smonth", col_prefix);   tf_batch_set_schema(ob, ei++, col_name, TF_TYPE_INT64); }
    if (st->w_day)     { snprintf(col_name, sizeof(col_name), "%sday", col_prefix);     tf_batch_set_schema(ob, ei++, col_name, TF_TYPE_INT64); }
    if (st->w_hour)    { snprintf(col_name, sizeof(col_name), "%shour", col_prefix);    tf_batch_set_schema(ob, ei++, col_name, TF_TYPE_INT64); }
    if (st->w_minute)  { snprintf(col_name, sizeof(col_name), "%sminute", col_prefix);  tf_batch_set_schema(ob, ei++, col_name, TF_TYPE_INT64); }
    if (st->w_second)  { snprintf(col_name, sizeof(col_name), "%ssecond", col_prefix);  tf_batch_set_schema(ob, ei++, col_name, TF_TYPE_INT64); }
    if (st->w_weekday) { snprintf(col_name, sizeof(col_name), "%sweekday", col_prefix); tf_batch_set_schema(ob, ei++, col_name, TF_TYPE_INT64); }
    if (st->w_epoch)   { snprintf(col_name, sizeof(col_name), "%sepoch", col_prefix);   tf_batch_set_schema(ob, ei++, col_name, TF_TYPE_INT64); }

    int ci = tf_batch_col_index(in, st->column);

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);

        ei = in->n_cols;
        if (ci >= 0 && !tf_batch_is_null(in, r, ci)) {
            int y = 0, mo = 0, d = 0, h = 0, mi = 0, se = 0;
            int parsed = 0;
            if (in->col_types[ci] == TF_TYPE_STRING) {
                parsed = parse_date(tf_batch_get_string(in, r, ci), &y, &mo, &d, &h, &mi, &se);
            } else if (in->col_types[ci] == TF_TYPE_DATE) {
                tf_date_to_ymd(tf_batch_get_date(in, r, ci), &y, &mo, &d);
                parsed = 1;
            } else if (in->col_types[ci] == TF_TYPE_TIMESTAMP) {
                int frac;
                tf_timestamp_to_parts(tf_batch_get_timestamp(in, r, ci), &y, &mo, &d, &h, &mi, &se, &frac);
                parsed = 1;
            }
            if (parsed) {
                if (st->w_year)    tf_batch_set_int64(ob, r, ei++, y);
                if (st->w_month)   tf_batch_set_int64(ob, r, ei++, mo);
                if (st->w_day)     tf_batch_set_int64(ob, r, ei++, d);
                if (st->w_hour)    tf_batch_set_int64(ob, r, ei++, h);
                if (st->w_minute)  tf_batch_set_int64(ob, r, ei++, mi);
                if (st->w_second)  tf_batch_set_int64(ob, r, ei++, se);
                if (st->w_weekday) tf_batch_set_int64(ob, r, ei++, weekday(y, mo, d));
                if (st->w_epoch)   tf_batch_set_int64(ob, r, ei++, date_to_epoch(y, mo, d, h, mi, se));
            } else {
                for (size_t k = in->n_cols; k < in->n_cols + n_extra; k++)
                    tf_batch_set_null(ob, r, k);
            }
        } else {
            for (size_t k = in->n_cols; k < in->n_cols + n_extra; k++)
                tf_batch_set_null(ob, r, k);
        }
        ob->n_rows = r + 1;
    }

    *out = ob;
    return TF_OK;
}

static int datetime_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void datetime_destroy(tf_step *self) {
    datetime_state *st = self->state;
    if (st) { free(st->column); free(st); }
    free(self);
}

tf_step *tf_datetime_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j)) return NULL;

    datetime_state *st = calloc(1, sizeof(datetime_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);

    cJSON *extract = cJSON_GetObjectItemCaseSensitive(args, "extract");
    if (extract && cJSON_IsArray(extract)) {
        int n = cJSON_GetArraySize(extract);
        for (int i = 0; i < n; i++) {
            cJSON *item = cJSON_GetArrayItem(extract, i);
            if (!cJSON_IsString(item)) continue;
            const char *s = item->valuestring;
            if (strcmp(s, "year") == 0) st->w_year = 1;
            else if (strcmp(s, "month") == 0) st->w_month = 1;
            else if (strcmp(s, "day") == 0) st->w_day = 1;
            else if (strcmp(s, "hour") == 0) st->w_hour = 1;
            else if (strcmp(s, "minute") == 0) st->w_minute = 1;
            else if (strcmp(s, "second") == 0) st->w_second = 1;
            else if (strcmp(s, "weekday") == 0) st->w_weekday = 1;
            else if (strcmp(s, "epoch") == 0) st->w_epoch = 1;
        }
    } else {
        /* Default: all */
        st->w_year = st->w_month = st->w_day = 1;
        st->w_hour = st->w_minute = st->w_second = 1;
        st->w_weekday = st->w_epoch = 1;
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->column); free(st); return NULL; }
    step->process = datetime_process;
    step->flush = datetime_flush;
    step->destroy = datetime_destroy;
    step->state = st;
    return step;
}
