/*
 * op_stats.c — Streaming aggregates with online algorithms.
 *
 * Config: {"stats": ["count","sum","avg","min","max","var","stddev",
 *                     "median","p25","p75","skewness","kurtosis",
 *                     "distinct","hist","sample"]}
 *   or {} for default stats (count, sum, avg, min, max, var, stddev, median).
 *
 * Algorithms ported from OnlineStats.jl (MIT):
 *   - Welford's online variance (var, stddev)
 *   - Moments via non-central moment tracking (skewness, kurtosis)
 *   - P2 quantile — Jain-Chlamtac (median, p25, p75)
 *   - HyperLogLog cardinality estimation (distinct)
 *   - Streaming histogram with adaptive bins (hist)
 *   - Reservoir sampling — Algorithm R (sample)
 *
 * Output: long format, one row per input column.
 */

#include "internal.h"
#include "cJSON.h"
#include "date_utils.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <float.h>
#include <math.h>

/* ---- P2 Quantile (Jain-Chlamtac) ---- */

typedef struct {
    double q[5];       /* marker heights */
    int    n[5];       /* marker positions */
    double np[5];      /* desired positions */
    double dn[5];      /* increment for desired positions */
    double tau;        /* target quantile */
    int    nobs;
    double init[5];    /* first 5 observations for init */
} p2_quantile;

static void p2_init(p2_quantile *p, double tau) {
    memset(p, 0, sizeof(*p));
    p->tau = tau;
    p->dn[0] = 0;
    p->dn[1] = tau / 2.0;
    p->dn[2] = tau;
    p->dn[3] = (1.0 + tau) / 2.0;
    p->dn[4] = 1.0;
}

static int dbl_cmp(const void *a, const void *b) {
    double da = *(const double *)a, db = *(const double *)b;
    return (da > db) - (da < db);
}

static void p2_interpolate(double *q, int *n, int i, int d) {
    double df = (double)d;
    double qi = q[i-1], qm = q[i], qp = q[i+1];
    int ni = n[i-1], nm = n[i], np = n[i+1];

    double v1 = (double)(nm - ni + d) * (qp - qm) / (double)(np - nm);
    double v2 = (double)(np - nm - d) * (qm - qi) / (double)(nm - ni);
    double qnew = qm + df / (double)(np - ni) * (v1 + v2);

    if (qi < qnew && qnew < qp) {
        q[i] = qnew;
    } else {
        /* linear fallback */
        q[i] = qm + df * (q[i + d] - qm) / (double)(n[i + d] - nm);
    }
    n[i] += d;
}

static void p2_update(p2_quantile *p, double y) {
    p->nobs++;
    if (p->nobs <= 5) {
        p->init[p->nobs - 1] = y;
        if (p->nobs == 5) {
            qsort(p->init, 5, sizeof(double), dbl_cmp);
            for (int i = 0; i < 5; i++) {
                p->q[i] = p->init[i];
                p->n[i] = i + 1;
            }
            p->np[0] = 1;
            p->np[1] = 1 + 2 * p->tau;
            p->np[2] = 1 + 4 * p->tau;
            p->np[3] = 3 + 2 * p->tau;
            p->np[4] = 5;
        }
        return;
    }

    /* Find cell k */
    int k;
    if (y < p->q[0]) {
        p->q[0] = y;
        k = 0;
    } else if (y >= p->q[4]) {
        p->q[4] = y > p->q[4] ? y : p->q[4];
        k = 3;
    } else {
        k = 0;
        for (int i = 1; i < 5; i++) {
            if (y < p->q[i]) { k = i - 1; break; }
        }
    }

    /* Increment positions */
    for (int i = k + 1; i < 5; i++) p->n[i]++;
    for (int i = 0; i < 5; i++) p->np[i] += p->dn[i];

    /* Adjust markers 1-3 */
    for (int i = 1; i <= 3; i++) {
        double di = p->np[i] - (double)p->n[i];
        if ((di >= 1.0 && p->n[i+1] - p->n[i] > 1) ||
            (di <= -1.0 && p->n[i-1] - p->n[i] < -1)) {
            int d = di >= 0 ? 1 : -1;
            p2_interpolate(p->q, p->n, i, d);
        }
    }
}

static double p2_value(const p2_quantile *p) {
    if (p->nobs < 5) {
        /* Not enough data; sort what we have and pick quantile */
        if (p->nobs == 0) return NAN;
        double tmp[5];
        memcpy(tmp, p->init, p->nobs * sizeof(double));
        qsort(tmp, p->nobs, sizeof(double), dbl_cmp);
        size_t idx = (size_t)(p->tau * (p->nobs - 1));
        return tmp[idx];
    }
    return p->q[2]; /* middle marker */
}

/* ---- HyperLogLog ---- */

#define HLL_P 10
#define HLL_M (1 << HLL_P)  /* 1024 registers */

typedef struct {
    uint8_t M[HLL_M];
    size_t  n;
} hll_state;

static void hll_init(hll_state *h) {
    memset(h, 0, sizeof(*h));
}

static uint32_t hll_hash(const char *key) {
    /* MurmurHash3 finalizer-style mixing */
    uint32_t h = 0x811c9dc5u;
    for (const char *p = key; *p; p++) {
        h ^= (uint32_t)(unsigned char)*p;
        h *= 0x01000193u;
    }
    h ^= h >> 16;
    h *= 0x85ebca6bu;
    h ^= h >> 13;
    h *= 0xc2b2ae35u;
    h ^= h >> 16;
    return h;
}

static int hll_clz32(uint32_t x) {
    if (x == 0) return 32;
#if defined(__GNUC__) || defined(__clang__)
    return __builtin_clz(x);
#else
    int n = 0;
    if (!(x & 0xFFFF0000u)) { n += 16; x <<= 16; }
    if (!(x & 0xFF000000u)) { n +=  8; x <<=  8; }
    if (!(x & 0xF0000000u)) { n +=  4; x <<=  4; }
    if (!(x & 0xC0000000u)) { n +=  2; x <<=  2; }
    if (!(x & 0x80000000u)) { n +=  1; }
    return n;
#endif
}

static void hll_update(hll_state *h, const char *val) {
    h->n++;
    uint32_t x = hll_hash(val);
    uint32_t idx = x & (HLL_M - 1);
    uint32_t w = x >> HLL_P;
    int rho = hll_clz32(w) + 1;
    if (rho > 32 - HLL_P) rho = 32 - HLL_P;
    if ((uint8_t)rho > h->M[idx]) h->M[idx] = (uint8_t)rho;
}

static double hll_estimate(const hll_state *h) {
    double alpha;
    if (HLL_P == 4)      alpha = 0.673;
    else if (HLL_P == 5) alpha = 0.697;
    else if (HLL_P == 6) alpha = 0.709;
    else                  alpha = 0.7213 / (1.0 + 1.079 / HLL_M);

    double sum = 0;
    int V = 0;
    for (int i = 0; i < HLL_M; i++) {
        sum += 1.0 / (1u << h->M[i]);
        if (h->M[i] == 0) V++;
    }
    double E = alpha * HLL_M * HLL_M / sum;

    /* Small/large range corrections */
    if (E <= 2.5 * HLL_M) {
        if (V > 0) return HLL_M * log((double)HLL_M / V);
        return E;
    }
    if (E <= (1.0 / 30.0) * 4294967296.0) return E;
    return -4294967296.0 * log(1.0 - E / 4294967296.0);
}

/* ---- Streaming Histogram (adaptive bins) ---- */

#define HIST_NBINS 32

typedef struct {
    double edges[HIST_NBINS + 1];
    size_t counts[HIST_NBINS];
    size_t n;
    double lo, hi;
    int    initialized;
} hist_state;

static void hist_init(hist_state *h) {
    memset(h, 0, sizeof(*h));
}

static void hist_setup_edges(hist_state *h) {
    double step = (h->hi - h->lo) / HIST_NBINS;
    for (int i = 0; i <= HIST_NBINS; i++)
        h->edges[i] = h->lo + step * i;
}

static int hist_bin(const hist_state *h, double y) {
    if (y < h->edges[0]) return -1;
    if (y >= h->edges[HIST_NBINS]) return HIST_NBINS - 1; /* last bin is closed */
    /* binary search */
    int lo = 0, hi = HIST_NBINS - 1;
    while (lo < hi) {
        int mid = (lo + hi) / 2;
        if (y < h->edges[mid + 1]) hi = mid;
        else lo = mid + 1;
    }
    return lo;
}

static void hist_expand(hist_state *h, double y) {
    if (y >= h->lo && y <= h->hi) return;

    if (y > h->hi) {
        /* Expand right: double range until y fits */
        while (y > h->hi) {
            double w = h->hi - h->lo;
            h->hi = h->lo + w * 2.0;
            /* Merge bin pairs: bins [0,1]→0, [2,3]→1, etc. */
            for (int i = 0; i < HIST_NBINS / 2; i++)
                h->counts[i] = h->counts[2*i] + h->counts[2*i + 1];
            for (int i = HIST_NBINS / 2; i < HIST_NBINS; i++)
                h->counts[i] = 0;
            hist_setup_edges(h);
        }
    } else {
        /* Expand left */
        while (y < h->lo) {
            double w = h->hi - h->lo;
            h->lo = h->hi - w * 2.0;
            for (int i = HIST_NBINS - 1; i >= HIST_NBINS / 2; i--)
                h->counts[i] = h->counts[2*(i - HIST_NBINS/2)] +
                                h->counts[2*(i - HIST_NBINS/2) + 1];
            for (int i = 0; i < HIST_NBINS / 2; i++)
                h->counts[i] = 0;
            hist_setup_edges(h);
        }
    }
}

static void hist_update(hist_state *h, double y) {
    h->n++;
    if (!h->initialized) {
        h->lo = y;
        h->hi = y;
        h->initialized = 1;
        return; /* single value, no bin yet */
    }
    if (h->lo == h->hi) {
        /* Second distinct value */
        if (y == h->lo) return;
        if (y < h->lo) h->lo = y; else h->hi = y;
        /* Put prior observations in appropriate bin */
        double range = h->hi - h->lo;
        h->lo -= range * 0.01;
        h->hi += range * 0.01;
        hist_setup_edges(h);
        /* All prior n-1 points were the same value, assign to their bin */
        int b = hist_bin(h, h->lo + range * 0.01 + range * 0.5);
        if (b >= 0 && b < HIST_NBINS) h->counts[b] = h->n - 1;
        b = hist_bin(h, y);
        if (b >= 0 && b < HIST_NBINS) h->counts[b]++;
        return;
    }

    hist_expand(h, y);
    int b = hist_bin(h, y);
    if (b >= 0 && b < HIST_NBINS) h->counts[b]++;
}

/* ---- Reservoir Sample (Algorithm R) ---- */

#define RESERVOIR_K 10

typedef struct {
    double values[RESERVOIR_K];
    size_t n;
    /* Simple xorshift RNG state */
    uint64_t rng;
} reservoir_state;

static void reservoir_init(reservoir_state *r) {
    memset(r, 0, sizeof(*r));
    r->rng = 0x12345678deadbeefULL;
}

static uint64_t reservoir_rand(reservoir_state *r) {
    r->rng ^= r->rng << 13;
    r->rng ^= r->rng >> 7;
    r->rng ^= r->rng << 17;
    return r->rng;
}

static void reservoir_update(reservoir_state *r, double y) {
    r->n++;
    if (r->n <= RESERVOIR_K) {
        r->values[r->n - 1] = y;
    } else {
        uint64_t j = reservoir_rand(r) % r->n;
        if (j < RESERVOIR_K) {
            r->values[j] = y;
        }
    }
}

/* ---- Per-column accumulator ---- */

typedef struct {
    /* Basic stats */
    size_t count;
    double sum;
    double min;
    double max;
    int    has_numeric;

    /* Welford's variance */
    double wf_mean;
    double wf_m2;      /* sum of (x - mean)^2 */

    /* Moments (non-central) for skewness/kurtosis */
    double mom[4];      /* E[X], E[X^2], E[X^3], E[X^4] */

    /* P2 quantiles */
    p2_quantile p2_median;
    p2_quantile p2_p25;
    p2_quantile p2_p75;

    /* HyperLogLog */
    hll_state *hll;

    /* Histogram */
    hist_state *hist;

    /* Reservoir sample */
    reservoir_state *reservoir;
} col_accum;

/* ---- Stats state ---- */

typedef struct {
    col_accum  *accums;
    char      **col_names;
    size_t      n_cols;
    int         initialized;
    /* Which stats to output */
    int want_count, want_sum, want_avg, want_min, want_max;
    int want_var, want_stddev;
    int want_median, want_p25, want_p75;
    int want_skewness, want_kurtosis;
    int want_distinct;
    int want_hist;
    int want_sample;
} stats_state;

static void accum_init(col_accum *a, const stats_state *st) {
    a->min = DBL_MAX;
    a->max = -DBL_MAX;
    p2_init(&a->p2_median, 0.5);
    p2_init(&a->p2_p25, 0.25);
    p2_init(&a->p2_p75, 0.75);

    if (st->want_distinct) {
        a->hll = calloc(1, sizeof(hll_state));
        if (a->hll) hll_init(a->hll);
    }
    if (st->want_hist) {
        a->hist = calloc(1, sizeof(hist_state));
        if (a->hist) hist_init(a->hist);
    }
    if (st->want_sample) {
        a->reservoir = calloc(1, sizeof(reservoir_state));
        if (a->reservoir) reservoir_init(a->reservoir);
    }
}

static void accum_update(col_accum *a, double val, const char *str_val,
                          int is_num) {
    a->count++;
    if (is_num) {
        a->has_numeric = 1;
        a->sum += val;
        if (val < a->min) a->min = val;
        if (val > a->max) a->max = val;

        /* Welford */
        double old_mean = a->wf_mean;
        a->wf_mean += (val - a->wf_mean) / (double)a->count;
        a->wf_m2 += (val - a->wf_mean) * (val - old_mean);

        /* Moments */
        double gamma = 1.0 / (double)a->count;
        double y2 = val * val;
        a->mom[0] += gamma * (val      - a->mom[0]);
        a->mom[1] += gamma * (y2       - a->mom[1]);
        a->mom[2] += gamma * (val * y2 - a->mom[2]);
        a->mom[3] += gamma * (y2 * y2  - a->mom[3]);

        /* P2 quantiles */
        p2_update(&a->p2_median, val);
        p2_update(&a->p2_p25, val);
        p2_update(&a->p2_p75, val);

        /* Histogram */
        if (a->hist) hist_update(a->hist, val);

        /* Reservoir */
        if (a->reservoir) reservoir_update(a->reservoir, val);
    }

    /* HyperLogLog uses string representation for all types */
    if (a->hll && str_val) hll_update(a->hll, str_val);
}

static void accum_free(col_accum *a) {
    free(a->hll);
    free(a->hist);
    free(a->reservoir);
}

/* ---- Process / Flush / Destroy ---- */

static int stats_process(tf_step *self, tf_batch *in, tf_batch **out,
                         tf_side_channels *side) {
    (void)side;
    stats_state *st = self->state;
    *out = NULL;

    if (!st->initialized) {
        st->n_cols = in->n_cols;
        st->accums = calloc(in->n_cols, sizeof(col_accum));
        st->col_names = calloc(in->n_cols, sizeof(char *));
        if (!st->accums || !st->col_names) return TF_ERROR;
        for (size_t c = 0; c < in->n_cols; c++) {
            st->col_names[c] = strdup(in->col_names[c]);
            accum_init(&st->accums[c], st);
        }
        st->initialized = 1;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        for (size_t c = 0; c < st->n_cols && c < in->n_cols; c++) {
            if (tf_batch_is_null(in, r, c)) continue;

            double val = 0;
            int is_num = 0;
            char str_buf[64];
            const char *str_val = NULL;

            switch (in->col_types[c]) {
                case TF_TYPE_INT64: {
                    int64_t iv = tf_batch_get_int64(in, r, c);
                    val = (double)iv;
                    is_num = 1;
                    snprintf(str_buf, sizeof(str_buf), "%lld", (long long)iv);
                    str_val = str_buf;
                    break;
                }
                case TF_TYPE_FLOAT64:
                    val = tf_batch_get_float64(in, r, c);
                    is_num = 1;
                    snprintf(str_buf, sizeof(str_buf), "%.17g", val);
                    str_val = str_buf;
                    break;
                case TF_TYPE_STRING:
                    str_val = tf_batch_get_string(in, r, c);
                    break;
                case TF_TYPE_BOOL:
                    str_val = tf_batch_get_bool(in, r, c) ? "true" : "false";
                    break;
                case TF_TYPE_DATE: {
                    int32_t dv = tf_batch_get_date(in, r, c);
                    val = (double)dv;
                    is_num = 1;
                    snprintf(str_buf, sizeof(str_buf), "%d", (int)dv);
                    str_val = str_buf;
                    break;
                }
                case TF_TYPE_TIMESTAMP: {
                    int64_t tv = tf_batch_get_timestamp(in, r, c);
                    val = (double)tv;
                    is_num = 1;
                    snprintf(str_buf, sizeof(str_buf), "%lld", (long long)tv);
                    str_val = str_buf;
                    break;
                }
                default:
                    break;
            }
            accum_update(&st->accums[c], val, str_val, is_num);
        }
    }

    return TF_OK;
}

static int stats_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    stats_state *st = self->state;
    *out = NULL;

    if (!st->initialized || st->n_cols == 0) return TF_OK;

    /* Count output columns */
    size_t n_stat_cols = 1; /* "column" */
    if (st->want_count)    n_stat_cols++;
    if (st->want_sum)      n_stat_cols++;
    if (st->want_avg)      n_stat_cols++;
    if (st->want_min)      n_stat_cols++;
    if (st->want_max)      n_stat_cols++;
    if (st->want_var)      n_stat_cols++;
    if (st->want_stddev)   n_stat_cols++;
    if (st->want_median)   n_stat_cols++;
    if (st->want_p25)      n_stat_cols++;
    if (st->want_p75)      n_stat_cols++;
    if (st->want_skewness) n_stat_cols++;
    if (st->want_kurtosis) n_stat_cols++;
    if (st->want_distinct) n_stat_cols++;
    if (st->want_hist)     n_stat_cols++;
    if (st->want_sample)   n_stat_cols++;

    tf_batch *ob = tf_batch_create(n_stat_cols, st->n_cols);
    if (!ob) return TF_ERROR;

    /* Set schema */
    size_t ci = 0;
    tf_batch_set_schema(ob, ci++, "column", TF_TYPE_STRING);
    if (st->want_count)    tf_batch_set_schema(ob, ci++, "count", TF_TYPE_INT64);
    if (st->want_sum)      tf_batch_set_schema(ob, ci++, "sum", TF_TYPE_FLOAT64);
    if (st->want_avg)      tf_batch_set_schema(ob, ci++, "avg", TF_TYPE_FLOAT64);
    if (st->want_min)      tf_batch_set_schema(ob, ci++, "min", TF_TYPE_FLOAT64);
    if (st->want_max)      tf_batch_set_schema(ob, ci++, "max", TF_TYPE_FLOAT64);
    if (st->want_var)      tf_batch_set_schema(ob, ci++, "var", TF_TYPE_FLOAT64);
    if (st->want_stddev)   tf_batch_set_schema(ob, ci++, "stddev", TF_TYPE_FLOAT64);
    if (st->want_median)   tf_batch_set_schema(ob, ci++, "median", TF_TYPE_FLOAT64);
    if (st->want_p25)      tf_batch_set_schema(ob, ci++, "p25", TF_TYPE_FLOAT64);
    if (st->want_p75)      tf_batch_set_schema(ob, ci++, "p75", TF_TYPE_FLOAT64);
    if (st->want_skewness) tf_batch_set_schema(ob, ci++, "skewness", TF_TYPE_FLOAT64);
    if (st->want_kurtosis) tf_batch_set_schema(ob, ci++, "kurtosis", TF_TYPE_FLOAT64);
    if (st->want_distinct) tf_batch_set_schema(ob, ci++, "distinct", TF_TYPE_INT64);
    if (st->want_hist)     tf_batch_set_schema(ob, ci++, "hist", TF_TYPE_STRING);
    if (st->want_sample)   tf_batch_set_schema(ob, ci++, "sample", TF_TYPE_STRING);

    /* One row per input column */
    for (size_t c = 0; c < st->n_cols; c++) {
        tf_batch_ensure_capacity(ob, c + 1);
        ci = 0;
        tf_batch_set_string(ob, c, ci++, st->col_names[c]);

        col_accum *a = &st->accums[c];

        if (st->want_count) {
            tf_batch_set_int64(ob, c, ci++, (int64_t)a->count);
        }
        if (st->want_sum) {
            if (a->has_numeric) tf_batch_set_float64(ob, c, ci, a->sum);
            else tf_batch_set_null(ob, c, ci);
            ci++;
        }
        if (st->want_avg) {
            if (a->has_numeric && a->count > 0)
                tf_batch_set_float64(ob, c, ci, a->sum / (double)a->count);
            else tf_batch_set_null(ob, c, ci);
            ci++;
        }
        if (st->want_min) {
            if (a->has_numeric && a->count > 0) tf_batch_set_float64(ob, c, ci, a->min);
            else tf_batch_set_null(ob, c, ci);
            ci++;
        }
        if (st->want_max) {
            if (a->has_numeric && a->count > 0) tf_batch_set_float64(ob, c, ci, a->max);
            else tf_batch_set_null(ob, c, ci);
            ci++;
        }
        if (st->want_var) {
            if (a->has_numeric && a->count > 1)
                tf_batch_set_float64(ob, c, ci, a->wf_m2 / (double)(a->count - 1));
            else tf_batch_set_null(ob, c, ci);
            ci++;
        }
        if (st->want_stddev) {
            if (a->has_numeric && a->count > 1)
                tf_batch_set_float64(ob, c, ci, sqrt(a->wf_m2 / (double)(a->count - 1)));
            else tf_batch_set_null(ob, c, ci);
            ci++;
        }
        if (st->want_median) {
            if (a->has_numeric && a->count > 0)
                tf_batch_set_float64(ob, c, ci, p2_value(&a->p2_median));
            else tf_batch_set_null(ob, c, ci);
            ci++;
        }
        if (st->want_p25) {
            if (a->has_numeric && a->count > 0)
                tf_batch_set_float64(ob, c, ci, p2_value(&a->p2_p25));
            else tf_batch_set_null(ob, c, ci);
            ci++;
        }
        if (st->want_p75) {
            if (a->has_numeric && a->count > 0)
                tf_batch_set_float64(ob, c, ci, p2_value(&a->p2_p75));
            else tf_batch_set_null(ob, c, ci);
            ci++;
        }
        if (st->want_skewness) {
            if (a->has_numeric && a->count > 2) {
                double m1 = a->mom[0], m2 = a->mom[1], m3 = a->mom[2];
                double vr = m2 - m1 * m1;
                if (vr > 1e-15) {
                    double sk = (m3 - 3.0*m1*vr - m1*m1*m1) / pow(vr, 1.5);
                    tf_batch_set_float64(ob, c, ci, sk);
                } else {
                    tf_batch_set_float64(ob, c, ci, 0.0);
                }
            } else tf_batch_set_null(ob, c, ci);
            ci++;
        }
        if (st->want_kurtosis) {
            if (a->has_numeric && a->count > 3) {
                double m1 = a->mom[0], m2 = a->mom[1];
                double m3 = a->mom[2], m4 = a->mom[3];
                double vr = m2 - m1 * m1;
                if (vr > 1e-15) {
                    double kt = (m4 - 4.0*m1*m3 + 6.0*m1*m1*m2
                                 - 3.0*m1*m1*m1*m1) / (vr*vr) - 3.0;
                    tf_batch_set_float64(ob, c, ci, kt);
                } else {
                    tf_batch_set_float64(ob, c, ci, 0.0);
                }
            } else tf_batch_set_null(ob, c, ci);
            ci++;
        }
        if (st->want_distinct) {
            if (a->hll && a->count > 0)
                tf_batch_set_int64(ob, c, ci, (int64_t)round(hll_estimate(a->hll)));
            else tf_batch_set_null(ob, c, ci);
            ci++;
        }
        if (st->want_hist) {
            if (a->hist && a->hist->n > 1) {
                /* Format: "lo:hi:c1,c2,...,cN" */
                char buf[1024];
                int pos = snprintf(buf, sizeof(buf), "%.6g:%.6g:",
                                   a->hist->edges[0], a->hist->edges[HIST_NBINS]);
                for (int b = 0; b < HIST_NBINS && pos < (int)sizeof(buf) - 16; b++) {
                    if (b > 0) buf[pos++] = ',';
                    pos += snprintf(buf + pos, sizeof(buf) - pos, "%zu", a->hist->counts[b]);
                }
                tf_batch_set_string(ob, c, ci, buf);
            } else {
                tf_batch_set_null(ob, c, ci);
            }
            ci++;
        }
        if (st->want_sample) {
            if (a->reservoir && a->reservoir->n > 0) {
                char buf[512];
                int pos = 0;
                size_t k = a->reservoir->n < RESERVOIR_K ? a->reservoir->n : RESERVOIR_K;
                for (size_t s = 0; s < k && pos < (int)sizeof(buf) - 32; s++) {
                    if (s > 0) buf[pos++] = ',';
                    pos += snprintf(buf + pos, sizeof(buf) - pos,
                                    "%.6g", a->reservoir->values[s]);
                }
                tf_batch_set_string(ob, c, ci, buf);
            } else {
                tf_batch_set_null(ob, c, ci);
            }
            ci++;
        }
        ob->n_rows = c + 1;
    }

    *out = ob;
    return TF_OK;
}

static void stats_destroy(tf_step *self) {
    stats_state *st = self->state;
    if (st) {
        if (st->accums) {
            for (size_t i = 0; i < st->n_cols; i++)
                accum_free(&st->accums[i]);
            free(st->accums);
        }
        if (st->col_names) {
            for (size_t i = 0; i < st->n_cols; i++) free(st->col_names[i]);
            free(st->col_names);
        }
        free(st);
    }
    free(self);
}

tf_step *tf_stats_create(const cJSON *args) {
    stats_state *st = calloc(1, sizeof(stats_state));
    if (!st) return NULL;

    cJSON *stats_arr = args ? cJSON_GetObjectItemCaseSensitive(args, "stats") : NULL;
    if (stats_arr && cJSON_IsArray(stats_arr)) {
        int n = cJSON_GetArraySize(stats_arr);
        for (int i = 0; i < n; i++) {
            cJSON *item = cJSON_GetArrayItem(stats_arr, i);
            if (!cJSON_IsString(item)) continue;
            const char *s = item->valuestring;
            if (strcmp(s, "count") == 0)         st->want_count = 1;
            else if (strcmp(s, "sum") == 0)      st->want_sum = 1;
            else if (strcmp(s, "avg") == 0)      st->want_avg = 1;
            else if (strcmp(s, "min") == 0)      st->want_min = 1;
            else if (strcmp(s, "max") == 0)      st->want_max = 1;
            else if (strcmp(s, "var") == 0)      st->want_var = 1;
            else if (strcmp(s, "stddev") == 0)   st->want_stddev = 1;
            else if (strcmp(s, "median") == 0)   st->want_median = 1;
            else if (strcmp(s, "p25") == 0)      st->want_p25 = 1;
            else if (strcmp(s, "p75") == 0)      st->want_p75 = 1;
            else if (strcmp(s, "skewness") == 0) st->want_skewness = 1;
            else if (strcmp(s, "kurtosis") == 0) st->want_kurtosis = 1;
            else if (strcmp(s, "distinct") == 0) st->want_distinct = 1;
            else if (strcmp(s, "hist") == 0)     st->want_hist = 1;
            else if (strcmp(s, "sample") == 0)   st->want_sample = 1;
        }
    } else {
        /* Default: basic + variance + median */
        st->want_count = 1;
        st->want_sum = 1;
        st->want_avg = 1;
        st->want_min = 1;
        st->want_max = 1;
        st->want_var = 1;
        st->want_stddev = 1;
        st->want_median = 1;
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st); return NULL; }
    step->process = stats_process;
    step->flush = stats_flush;
    step->destroy = stats_destroy;
    step->state = st;
    return step;
}
