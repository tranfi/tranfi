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

typedef enum {
    DATETIME_MISSING_ERROR,
    DATETIME_MISSING_NULL,
    DATETIME_MISSING_IGNORE,
} datetime_missing_policy;

typedef enum {
    DATETIME_TYPE_FAIL,
    DATETIME_TYPE_NULL,
} datetime_type_policy;

typedef struct {
    char  *column;
    int    w_year, w_month, w_day, w_hour, w_minute, w_second, w_weekday, w_epoch;
    datetime_missing_policy missing;
    datetime_type_policy on_type_error;
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

static int datetime_is_temporal_type(tf_type type) {
    return type == TF_TYPE_STRING || type == TF_TYPE_DATE || type == TF_TYPE_TIMESTAMP;
}

static void datetime_set_col_error(const char *column, const char *suffix) {
    char msg[512];
    snprintf(msg, sizeof(msg), "datetime: column '%s' %s", column ? column : "", suffix);
    tf_set_last_error(msg);
}

static int datetime_parse_missing_policy(const cJSON *args, datetime_missing_policy *out) {
    *out = DATETIME_MISSING_ERROR;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "missing");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("datetime: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "error") == 0) *out = DATETIME_MISSING_ERROR;
    else if (strcmp(j->valuestring, "null") == 0) *out = DATETIME_MISSING_NULL;
    else if (strcmp(j->valuestring, "ignore") == 0) *out = DATETIME_MISSING_IGNORE;
    else {
        tf_set_last_error("datetime: missing must be error, null, or ignore");
        return TF_ERROR;
    }
    return TF_OK;
}

static int datetime_parse_type_policy(const cJSON *args, datetime_type_policy *out) {
    *out = DATETIME_TYPE_FAIL;
    const cJSON *j = cJSON_GetObjectItemCaseSensitive(args, "on_type_error");
    if (!j) return TF_OK;
    if (!cJSON_IsString(j)) {
        tf_set_last_error("datetime: on_type_error must be fail or null");
        return TF_ERROR;
    }
    if (strcmp(j->valuestring, "fail") == 0) *out = DATETIME_TYPE_FAIL;
    else if (strcmp(j->valuestring, "null") == 0) *out = DATETIME_TYPE_NULL;
    else {
        tf_set_last_error("datetime: on_type_error must be fail or null");
        return TF_ERROR;
    }
    return TF_OK;
}

static int datetime_passthrough(tf_batch *in, tf_batch **out) {
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
    *out = ob;
    return TF_OK;
}

static size_t datetime_extra_columns(const datetime_state *st, const char **extra_names,
                                     tf_type *extra_types, char extra_name_storage[8][256]) {
    size_t n_extra = 0;
#define ADD_DATETIME_EXTRA(name) do { \
        snprintf(extra_name_storage[n_extra], sizeof(extra_name_storage[n_extra]), "%s_%s", st->column, name); \
        extra_names[n_extra] = extra_name_storage[n_extra]; \
        extra_types[n_extra] = TF_TYPE_INT64; \
        n_extra++; \
    } while (0)
    if (st->w_year) ADD_DATETIME_EXTRA("year");
    if (st->w_month) ADD_DATETIME_EXTRA("month");
    if (st->w_day) ADD_DATETIME_EXTRA("day");
    if (st->w_hour) ADD_DATETIME_EXTRA("hour");
    if (st->w_minute) ADD_DATETIME_EXTRA("minute");
    if (st->w_second) ADD_DATETIME_EXTRA("second");
    if (st->w_weekday) ADD_DATETIME_EXTRA("weekday");
    if (st->w_epoch) ADD_DATETIME_EXTRA("epoch");
#undef ADD_DATETIME_EXTRA
    return n_extra;
}

static int datetime_set_extra_nulls(tf_batch *ob, size_t row, size_t start, size_t n_extra) {
    for (size_t k = start; k < start + n_extra; k++) {
        if (tf_batch_set_null(ob, row, k) != TF_OK) return TF_ERROR;
    }
    return TF_OK;
}

static int datetime_apply_extracts(const datetime_state *st, tf_batch *ob, size_t row,
                                   size_t *ei, int y, int mo, int d, int h, int mi, int se) {
    if (st->w_year && tf_batch_set_int64(ob, row, (*ei)++, y) != TF_OK) return TF_ERROR;
    if (st->w_month && tf_batch_set_int64(ob, row, (*ei)++, mo) != TF_OK) return TF_ERROR;
    if (st->w_day && tf_batch_set_int64(ob, row, (*ei)++, d) != TF_OK) return TF_ERROR;
    if (st->w_hour && tf_batch_set_int64(ob, row, (*ei)++, h) != TF_OK) return TF_ERROR;
    if (st->w_minute && tf_batch_set_int64(ob, row, (*ei)++, mi) != TF_OK) return TF_ERROR;
    if (st->w_second && tf_batch_set_int64(ob, row, (*ei)++, se) != TF_OK) return TF_ERROR;
    if (st->w_weekday && tf_batch_set_int64(ob, row, (*ei)++, weekday(y, mo, d)) != TF_OK) return TF_ERROR;
    if (st->w_epoch && tf_batch_set_int64(ob, row, (*ei)++, date_to_epoch(y, mo, d, h, mi, se)) != TF_OK) return TF_ERROR;
    return TF_OK;
}

static int datetime_process(tf_step *self, tf_batch *in, tf_batch **out,
                            tf_side_channels *side) {
    (void)side;
    datetime_state *st = self->state;
    *out = NULL;

    const char *extra_names[8];
    tf_type extra_types[8];
    char extra_name_storage[8][256];
    size_t n_extra = datetime_extra_columns(st, extra_names, extra_types, extra_name_storage);

    int ci = tf_batch_col_index(in, st->column);
    int force_null = 0;
    if (ci < 0) {
        if (st->missing == DATETIME_MISSING_ERROR) {
            datetime_set_col_error(st->column, "not found");
            return TF_ERROR;
        }
        if (st->missing == DATETIME_MISSING_IGNORE) return datetime_passthrough(in, out);
        force_null = 1;
    } else if (!datetime_is_temporal_type(in->col_types[ci])) {
        if (st->on_type_error == DATETIME_TYPE_FAIL) {
            datetime_set_col_error(st->column, "must be string, date, or timestamp");
            return TF_ERROR;
        }
        force_null = 1;
    }

    tf_batch *ob = tf_batch_create(in->n_cols + n_extra, in->n_rows);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_with_extra_cols(ob, in, extra_names, extra_types, n_extra) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }

        size_t ei = in->n_cols;
        if (force_null || tf_batch_is_null(in, r, (size_t)ci)) {
            if (datetime_set_extra_nulls(ob, r, in->n_cols, n_extra) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            if (tf_batch_expose_row(ob, r) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
            continue;
        }

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
            if (datetime_apply_extracts(st, ob, r, &ei, y, mo, d, h, mi, se) != TF_OK) { tf_batch_free(ob); return TF_ERROR; }
        } else if (datetime_set_extra_nulls(ob, r, in->n_cols, n_extra) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    *out = ob;
    return TF_OK;
}

static int datetime_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void datetime_state_free(datetime_state *st) {
    if (!st) return;
    free(st->column);
    free(st);
}

static void datetime_destroy(tf_step *self) {
    if (self) datetime_state_free(self->state);
    free(self);
}

static int datetime_enable_component(datetime_state *st, const char *s) {
    if (strcmp(s, "year") == 0) st->w_year = 1;
    else if (strcmp(s, "month") == 0) st->w_month = 1;
    else if (strcmp(s, "day") == 0) st->w_day = 1;
    else if (strcmp(s, "hour") == 0) st->w_hour = 1;
    else if (strcmp(s, "minute") == 0) st->w_minute = 1;
    else if (strcmp(s, "second") == 0) st->w_second = 1;
    else if (strcmp(s, "weekday") == 0) st->w_weekday = 1;
    else if (strcmp(s, "epoch") == 0) st->w_epoch = 1;
    else return TF_ERROR;
    return TF_OK;
}

static int datetime_has_component(const datetime_state *st) {
    return st->w_year || st->w_month || st->w_day || st->w_hour || st->w_minute ||
           st->w_second || st->w_weekday || st->w_epoch;
}

tf_step *tf_datetime_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j) || !col_j->valuestring[0]) {
        tf_set_last_error("datetime: column is required");
        return NULL;
    }

    datetime_state *st = calloc(1, sizeof(datetime_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    if (!st->column) { free(st); return NULL; }

    cJSON *extract = cJSON_GetObjectItemCaseSensitive(args, "extract");
    if (extract && cJSON_IsArray(extract)) {
        int n = cJSON_GetArraySize(extract);
        for (int i = 0; i < n; i++) {
            cJSON *item = cJSON_GetArrayItem(extract, i);
            if (!cJSON_IsString(item) || !item->valuestring[0] ||
                datetime_enable_component(st, item->valuestring) != TF_OK) {
                tf_set_last_error("datetime: extract must contain year, month, day, hour, minute, second, weekday, or epoch");
                datetime_state_free(st);
                return NULL;
            }
        }
        if (!datetime_has_component(st)) {
            tf_set_last_error("datetime: extract must contain at least one component");
            datetime_state_free(st);
            return NULL;
        }
    } else {
        /* Default: all */
        st->w_year = st->w_month = st->w_day = 1;
        st->w_hour = st->w_minute = st->w_second = 1;
        st->w_weekday = st->w_epoch = 1;
    }

    if (datetime_parse_missing_policy(args, &st->missing) != TF_OK ||
        datetime_parse_type_policy(args, &st->on_type_error) != TF_OK) {
        free(st->column);
        free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st->column); free(st); return NULL; }
    step->process = datetime_process;
    step->flush = datetime_flush;
    step->destroy = datetime_destroy;
    step->state = st;
    return step;
}
