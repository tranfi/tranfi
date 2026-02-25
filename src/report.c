/*
 * report.c — Rich ANSI terminal report for stats output.
 *
 * Parses the CSV text produced by the stats channel and renders a
 * compact per-column summary with Unicode sparkline histograms.
 */

#include "report.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <math.h>

/* ---- ANSI escape codes ---- */

#define ANSI_BOLD    "\033[1m"
#define ANSI_DIM     "\033[2m"
#define ANSI_CYAN    "\033[36m"
#define ANSI_GREEN   "\033[32m"
#define ANSI_YELLOW  "\033[33m"
#define ANSI_RESET   "\033[0m"

/* ---- Dynamic string buffer ---- */

typedef struct {
  char  *data;
  size_t len;
  size_t cap;
} strbuf;

static void sb_init(strbuf *sb) {
  sb->cap = 4096;
  sb->data = malloc(sb->cap);
  sb->len = 0;
  if (sb->data) sb->data[0] = '\0';
}

static void sb_ensure(strbuf *sb, size_t extra) {
  if (sb->len + extra + 1 > sb->cap) {
    while (sb->len + extra + 1 > sb->cap) sb->cap *= 2;
    sb->data = realloc(sb->data, sb->cap);
  }
}

static void sb_append(strbuf *sb, const char *s) {
  size_t n = strlen(s);
  sb_ensure(sb, n);
  memcpy(sb->data + sb->len, s, n);
  sb->len += n;
  sb->data[sb->len] = '\0';
}

static void sb_printf(strbuf *sb, const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  int needed = vsnprintf(NULL, 0, fmt, ap);
  va_end(ap);
  if (needed < 0) return;
  sb_ensure(sb, (size_t)needed);
  va_start(ap, fmt);
  vsnprintf(sb->data + sb->len, (size_t)needed + 1, fmt, ap);
  va_end(ap);
  sb->len += (size_t)needed;
}

/* ---- Sparkline ---- */

static const char *spark_chars[] = {
  "\xe2\x96\x81", /* ▁ U+2581 */
  "\xe2\x96\x82", /* ▂ U+2582 */
  "\xe2\x96\x83", /* ▃ U+2583 */
  "\xe2\x96\x84", /* ▄ U+2584 */
  "\xe2\x96\x85", /* ▅ U+2585 */
  "\xe2\x96\x86", /* ▆ U+2586 */
  "\xe2\x96\x87", /* ▇ U+2587 */
  "\xe2\x96\x88", /* █ U+2588 */
};

/* ---- CSV parsing helpers ---- */

#define MAX_COLS 32
#define MAX_ROWS 256
#define MAX_FIELD 1024

typedef struct {
  char   *headers[MAX_COLS];
  char   *cells[MAX_ROWS][MAX_COLS];
  size_t  n_cols;
  size_t  n_rows;
} csv_table;

/* Find column index by name, -1 if not found */
static int csv_col(const csv_table *t, const char *name) {
  for (size_t i = 0; i < t->n_cols; i++) {
    if (t->headers[i] && strcmp(t->headers[i], name) == 0)
      return (int)i;
  }
  return -1;
}

/* Parse a simple CSV (no quoted fields with embedded commas needed for stats) */
static csv_table *csv_parse(const char *csv, size_t len) {
  csv_table *t = calloc(1, sizeof(csv_table));
  if (!t) return NULL;

  const char *p = csv;
  const char *end = csv + len;

  /* Parse header line */
  while (p < end && *p != '\n' && *p != '\r') {
    const char *start = p;
    while (p < end && *p != ',' && *p != '\n' && *p != '\r') p++;
    size_t flen = (size_t)(p - start);
    if (flen > MAX_FIELD - 1) flen = MAX_FIELD - 1;
    if (t->n_cols < MAX_COLS) {
      t->headers[t->n_cols] = malloc(flen + 1);
      if (t->headers[t->n_cols]) {
        memcpy(t->headers[t->n_cols], start, flen);
        t->headers[t->n_cols][flen] = '\0';
      }
      t->n_cols++;
    }
    if (p < end && *p == ',') p++;
  }
  /* Skip newline */
  if (p < end && *p == '\r') p++;
  if (p < end && *p == '\n') p++;

  /* Parse data rows */
  while (p < end && t->n_rows < MAX_ROWS) {
    if (*p == '\n' || *p == '\r') { p++; continue; }
    size_t ci = 0;
    while (p < end && *p != '\n' && *p != '\r') {
      const char *start = p;
      /* Handle quoted fields (hist column has colons and commas inside quotes) */
      if (*p == '"') {
        p++; /* skip opening quote */
        start = p;
        while (p < end && *p != '"') p++;
        size_t flen = (size_t)(p - start);
        if (flen > MAX_FIELD - 1) flen = MAX_FIELD - 1;
        if (ci < MAX_COLS) {
          t->cells[t->n_rows][ci] = malloc(flen + 1);
          if (t->cells[t->n_rows][ci]) {
            memcpy(t->cells[t->n_rows][ci], start, flen);
            t->cells[t->n_rows][ci][flen] = '\0';
          }
        }
        if (p < end && *p == '"') p++; /* skip closing quote */
        if (p < end && *p == ',') p++;
        ci++;
        continue;
      }
      while (p < end && *p != ',' && *p != '\n' && *p != '\r') p++;
      size_t flen = (size_t)(p - start);
      if (flen > MAX_FIELD - 1) flen = MAX_FIELD - 1;
      if (ci < MAX_COLS) {
        t->cells[t->n_rows][ci] = malloc(flen + 1);
        if (t->cells[t->n_rows][ci]) {
          memcpy(t->cells[t->n_rows][ci], start, flen);
          t->cells[t->n_rows][ci][flen] = '\0';
        }
      }
      if (p < end && *p == ',') p++;
      ci++;
    }
    t->n_rows++;
    if (p < end && *p == '\r') p++;
    if (p < end && *p == '\n') p++;
  }

  return t;
}

static void csv_free(csv_table *t) {
  if (!t) return;
  for (size_t i = 0; i < t->n_cols; i++) free(t->headers[i]);
  for (size_t r = 0; r < t->n_rows; r++)
    for (size_t c = 0; c < t->n_cols; c++)
      free(t->cells[r][c]);
  free(t);
}

static const char *csv_get(const csv_table *t, size_t row, int col) {
  if (col < 0 || (size_t)col >= t->n_cols || row >= t->n_rows) return NULL;
  return t->cells[row][col];
}

static double csv_getf(const csv_table *t, size_t row, int col) {
  const char *s = csv_get(t, row, col);
  if (!s || *s == '\0') return NAN;
  return atof(s);
}

static long long csv_getll(const csv_table *t, size_t row, int col) {
  const char *s = csv_get(t, row, col);
  if (!s || *s == '\0') return 0;
  return atoll(s);
}

/* ---- Histogram parsing ---- */

#define HIST_BINS 32

typedef struct {
  double lo, hi;
  size_t counts[HIST_BINS];
  size_t total;
} parsed_hist;

static int parse_hist(const char *s, parsed_hist *h) {
  if (!s || !*s) return 0;
  memset(h, 0, sizeof(*h));

  /* Format: "lo:hi:c1,c2,...,c32" */
  char *endp;
  h->lo = strtod(s, &endp);
  if (*endp != ':') return 0;
  endp++;
  h->hi = strtod(endp, &endp);
  if (*endp != ':') return 0;
  endp++;

  for (int i = 0; i < HIST_BINS; i++) {
    h->counts[i] = (size_t)strtoul(endp, &endp, 10);
    h->total += h->counts[i];
    if (*endp == ',') endp++;
  }
  return 1;
}

/* ---- Number formatting ---- */

static void fmt_num(char *buf, size_t bufsz, double v) {
  if (isnan(v)) { snprintf(buf, bufsz, "-"); return; }
  double av = fabs(v);
  if (av == 0.0)
    snprintf(buf, bufsz, "0");
  else if (av >= 1e6)
    snprintf(buf, bufsz, "%.3g", v);
  else if (av >= 100.0)
    snprintf(buf, bufsz, "%.1f", v);
  else if (av >= 1.0)
    snprintf(buf, bufsz, "%.2f", v);
  else
    snprintf(buf, bufsz, "%.4f", v);
}

static void fmt_int(char *buf, size_t bufsz, long long v) {
  /* Add thousand separators */
  char raw[32];
  snprintf(raw, sizeof(raw), "%lld", v < 0 ? -v : v);
  size_t rlen = strlen(raw);
  size_t pos = 0;
  if (v < 0 && pos < bufsz - 1) buf[pos++] = '-';
  for (size_t i = 0; i < rlen && pos < bufsz - 1; i++) {
    if (i > 0 && (rlen - i) % 3 == 0 && pos < bufsz - 1) buf[pos++] = ',';
    buf[pos++] = raw[i];
  }
  buf[pos] = '\0';
}

/* ---- Main report formatter ---- */

char *tf_report_format(const char *stats_csv, size_t stats_len, int use_color) {
  if (!stats_csv || stats_len == 0) return NULL;

  csv_table *t = csv_parse(stats_csv, stats_len);
  if (!t || t->n_rows == 0) { csv_free(t); return NULL; }

  /* Find column indices */
  int ci_name     = csv_col(t, "column");
  int ci_count    = csv_col(t, "count");
  int ci_avg      = csv_col(t, "avg");
  int ci_min      = csv_col(t, "min");
  int ci_max      = csv_col(t, "max");
  int ci_stddev   = csv_col(t, "stddev");
  int ci_median   = csv_col(t, "median");
  int ci_p25      = csv_col(t, "p25");
  int ci_p75      = csv_col(t, "p75");
  int ci_distinct = csv_col(t, "distinct");
  int ci_hist     = csv_col(t, "hist");
  int ci_sample   = csv_col(t, "sample");

  if (ci_name < 0) { csv_free(t); return NULL; }

  const char *bold  = use_color ? ANSI_BOLD  : "";
  const char *dim   = use_color ? ANSI_DIM   : "";
  const char *cyan  = use_color ? ANSI_CYAN  : "";
  const char *green = use_color ? ANSI_GREEN : "";
  const char *reset = use_color ? ANSI_RESET : "";

  strbuf sb;
  sb_init(&sb);
  if (!sb.data) { csv_free(t); return NULL; }

  /* Header */
  long long total_count = 0;
  if (ci_count >= 0 && t->n_rows > 0)
    total_count = csv_getll(t, 0, ci_count);

  char count_str[32];
  fmt_int(count_str, sizeof(count_str), total_count);

  sb_printf(&sb, "\n");
  if (use_color) {
    sb_printf(&sb, "  %s%zu columns%s  %s%s rows%s\n\n",
              bold, t->n_rows, reset,
              dim, count_str, reset);
  } else {
    sb_printf(&sb, "  %zu columns  %s rows\n\n", t->n_rows, count_str);
  }

  /* Per-column stats */
  for (size_t r = 0; r < t->n_rows; r++) {
    const char *name = csv_get(t, r, ci_name);
    if (!name) continue;

    /* Column name */
    sb_printf(&sb, "  %s%-20s%s", bold, name, reset);

    /* Detect numeric vs string: if min/max are present and not empty */
    const char *min_s = csv_get(t, r, ci_min);
    int is_numeric = (min_s && *min_s != '\0' && strcmp(min_s, "") != 0);

    if (is_numeric) {
      /* Numeric column: show key stats inline */
      char buf[32];

      if (ci_min >= 0) {
        fmt_num(buf, sizeof(buf), csv_getf(t, r, ci_min));
        sb_printf(&sb, "  %smin%s %-10s", dim, reset, buf);
      }
      if (ci_max >= 0) {
        fmt_num(buf, sizeof(buf), csv_getf(t, r, ci_max));
        sb_printf(&sb, "  %smax%s %-10s", dim, reset, buf);
      }
      if (ci_avg >= 0) {
        fmt_num(buf, sizeof(buf), csv_getf(t, r, ci_avg));
        sb_printf(&sb, "  %savg%s %-10s", dim, reset, buf);
      }
      if (ci_stddev >= 0) {
        fmt_num(buf, sizeof(buf), csv_getf(t, r, ci_stddev));
        sb_printf(&sb, "  %sstd%s %-10s", dim, reset, buf);
      }
      sb_append(&sb, "\n");

      /* Quantiles line */
      if (ci_median >= 0 || ci_p25 >= 0 || ci_p75 >= 0) {
        sb_printf(&sb, "  %-20s", "");
        if (ci_p25 >= 0) {
          fmt_num(buf, sizeof(buf), csv_getf(t, r, ci_p25));
          sb_printf(&sb, "  %sp25%s %-10s", dim, reset, buf);
        }
        if (ci_median >= 0) {
          fmt_num(buf, sizeof(buf), csv_getf(t, r, ci_median));
          sb_printf(&sb, "  %smed%s %-10s", dim, reset, buf);
        }
        if (ci_p75 >= 0) {
          fmt_num(buf, sizeof(buf), csv_getf(t, r, ci_p75));
          sb_printf(&sb, "  %sp75%s %-10s", dim, reset, buf);
        }
        sb_append(&sb, "\n");
      }

      /* Histogram sparkline */
      if (ci_hist >= 0) {
        const char *hist_str = csv_get(t, r, ci_hist);
        parsed_hist h;
        if (parse_hist(hist_str, &h) && h.total > 0) {
          /* Find max count for scaling */
          size_t max_c = 0;
          for (int i = 0; i < HIST_BINS; i++)
            if (h.counts[i] > max_c) max_c = h.counts[i];

          sb_printf(&sb, "  %-20s  %s", "", cyan);
          for (int i = 0; i < HIST_BINS; i++) {
            if (max_c == 0) {
              sb_append(&sb, spark_chars[0]);
            } else {
              int level = (int)((double)h.counts[i] / (double)max_c * 7.0);
              if (level < 0) level = 0;
              if (level > 7) level = 7;
              /* Use at least level 1 for non-zero bins */
              if (h.counts[i] > 0 && level == 0) level = 1;
              sb_append(&sb, spark_chars[level]);
            }
          }

          char lo_buf[16], hi_buf[16];
          fmt_num(lo_buf, sizeof(lo_buf), h.lo);
          fmt_num(hi_buf, sizeof(hi_buf), h.hi);
          sb_printf(&sb, "%s  %s%s — %s%s\n", reset, dim, lo_buf, hi_buf, reset);
        }
      }

      /* Sample dots */
      if (ci_sample >= 0) {
        const char *samp = csv_get(t, r, ci_sample);
        if (samp && *samp) {
          double vmin = csv_getf(t, r, ci_min);
          double vmax = csv_getf(t, r, ci_max);
          double range = vmax - vmin;
          if (range > 0) {
            sb_printf(&sb, "  %-20s  %s", "", green);
            /* Parse sample values and place dots in a 32-char strip */
            char strip[33];
            memset(strip, ' ', 32);
            strip[32] = '\0';

            const char *p = samp;
            while (*p) {
              char *ep;
              double v = strtod(p, &ep);
              if (ep == p) break;
              int pos = (int)((v - vmin) / range * 31.0);
              if (pos < 0) pos = 0;
              if (pos > 31) pos = 31;
              strip[pos] = '.';
              p = ep;
              if (*p == ',') p++;
            }
            /* Print using unicode dots for better visibility */
            for (int i = 0; i < 32; i++) {
              if (strip[i] == '.')
                sb_append(&sb, "\xc2\xb7"); /* · U+00B7 */
              else
                sb_append(&sb, " ");
            }
            sb_printf(&sb, "%s\n", reset);
          }
        }
      }
    } else {
      /* String/non-numeric column */
      if (ci_count >= 0) {
        char cnt_buf[32];
        fmt_int(cnt_buf, sizeof(cnt_buf), csv_getll(t, r, ci_count));
        sb_printf(&sb, "  %sn%s %-10s", dim, reset, cnt_buf);
      }
      if (ci_distinct >= 0) {
        long long dist = csv_getll(t, r, ci_distinct);
        long long cnt = ci_count >= 0 ? csv_getll(t, r, ci_count) : 0;
        char dist_buf[32];
        fmt_int(dist_buf, sizeof(dist_buf), dist);
        if (cnt > 0) {
          double pct = 100.0 * (double)dist / (double)cnt;
          sb_printf(&sb, "  %suniq%s %s (%.1f%%)", dim, reset, dist_buf, pct);
        } else {
          sb_printf(&sb, "  %suniq%s %s", dim, reset, dist_buf);
        }
      }
      sb_append(&sb, "\n");
    }
  }

  sb_append(&sb, "\n");

  csv_free(t);

  return sb.data;
}
