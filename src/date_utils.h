/*
 * date_utils.h — Date/timestamp conversion utilities (header-only).
 *
 * Uses Howard Hinnant's civil date algorithms (public domain).
 * Epoch: 1970-01-01.  Date = int32_t days.  Timestamp = int64_t microseconds.
 */

#ifndef TF_DATE_UTILS_H
#define TF_DATE_UTILS_H

#include <stdint.h>
#include <stdio.h>

/* Days from civil date to epoch (1970-01-01 = 0). */
static inline int32_t tf_date_from_ymd(int y, int m, int d) {
    y -= (m <= 2);
    int era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + (unsigned)d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int)doe - 719468;
}

/* Epoch days back to civil date. */
static inline void tf_date_to_ymd(int32_t days, int *y, int *m, int *d) {
    days += 719468;
    int era = (days >= 0 ? days : days - 146096) / 146097;
    unsigned doe = (unsigned)(days - era * 146097);
    unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    *y = (int)yoe + era * 400;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned mp = (5 * doy + 2) / 153;
    *d = (int)(doy - (153 * mp + 2) / 5 + 1);
    *m = (int)(mp < 10 ? mp + 3 : mp - 9);
    *y += (*m <= 2);
}

/* Microseconds since epoch to date/time components. */
static inline void tf_timestamp_to_parts(int64_t us, int *y, int *mo, int *d,
                                          int *h, int *mi, int *s, int *frac_us) {
    int64_t day_us = 86400LL * 1000000LL;
    int32_t days = (int32_t)(us / day_us);
    int64_t rem = us % day_us;
    if (rem < 0) { days--; rem += day_us; }
    tf_date_to_ymd(days, y, mo, d);
    *frac_us = (int)(rem % 1000000);
    rem /= 1000000;
    *s = (int)(rem % 60); rem /= 60;
    *mi = (int)(rem % 60); rem /= 60;
    *h = (int)rem;
}

/* Date/time components to microseconds since epoch. */
static inline int64_t tf_timestamp_from_parts(int y, int mo, int d,
                                               int h, int mi, int s, int frac_us) {
    int32_t days = tf_date_from_ymd(y, mo, d);
    return (int64_t)days * 86400LL * 1000000LL
         + (int64_t)h * 3600000000LL
         + (int64_t)mi * 60000000LL
         + (int64_t)s * 1000000LL
         + frac_us;
}

/* Weekday: 0=Sunday..6=Saturday. */
static inline int tf_date_weekday(int32_t days) {
    return days >= -4 ? (days + 4) % 7 : (days + 5) % 7 + 6;
}

/* Format date as YYYY-MM-DD into buf (must be >= 11 bytes). Returns buf. */
static inline char *tf_date_format(int32_t days, char *buf, size_t buflen) {
    int y, m, d;
    tf_date_to_ymd(days, &y, &m, &d);
    snprintf(buf, buflen, "%04d-%02d-%02d", y, m, d);
    return buf;
}

/* Format timestamp as ISO 8601 into buf (must be >= 32 bytes). Returns buf. */
static inline char *tf_timestamp_format(int64_t us, char *buf, size_t buflen) {
    int y, mo, d, h, mi, s, frac;
    tf_timestamp_to_parts(us, &y, &mo, &d, &h, &mi, &s, &frac);
    if (frac > 0) {
        int len = snprintf(buf, buflen, "%04d-%02d-%02dT%02d:%02d:%02d.%06d",
                           y, mo, d, h, mi, s, frac);
        /* Trim trailing zeros */
        while (len > 0 && buf[len - 1] == '0') len--;
        buf[len++] = 'Z';
        buf[len] = '\0';
    } else {
        snprintf(buf, buflen, "%04d-%02d-%02dT%02d:%02d:%02dZ", y, mo, d, h, mi, s);
    }
    return buf;
}

#endif /* TF_DATE_UTILS_H */
