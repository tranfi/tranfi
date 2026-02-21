/*
 * bench.c — Throughput benchmarks for tranfi-core.
 *
 * Generates CSV data in memory, pushes through various pipelines,
 * measures wall-clock time and reports rows/sec and MB/sec.
 *
 * Usage: ./bench [rows]    (default: 1000000)
 */

#include "tranfi.h"
#include "internal.h"
#include "ir.h"
#include "dsl.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define PULL_BUF (64 * 1024)

/* ---- Helpers ---- */

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec * 1e-9;
}

/* Generate CSV: name,age,score,city\n with N data rows */
static char *gen_csv(size_t n_rows, size_t *out_len) {
    const char *names[]  = {"Alice", "Bob", "Charlie", "Diana", "Eve",
                            "Frank", "Grace", "Hank", "Ivy", "Jack"};
    const char *cities[] = {"NYC", "LA", "Chicago", "Houston", "Phoenix",
                            "Philly", "San Antonio", "San Diego", "Dallas", "Austin"};
    size_t cap = 64 + n_rows * 48;
    char *buf = malloc(cap);
    if (!buf) return NULL;
    size_t len = 0;

    len += snprintf(buf + len, cap - len, "name,age,score,city\n");

    for (size_t i = 0; i < n_rows; i++) {
        int age   = 18 + (int)(i % 60);
        int score = 50 + (int)(i % 50);
        len += snprintf(buf + len, cap - len, "%s,%d,%d,%s\n",
                        names[i % 10], age, score, cities[i % 10]);
    }
    *out_len = len;
    return buf;
}

/* Drain all output from a pipeline */
static size_t drain(tf_pipeline *p) {
    uint8_t buf[PULL_BUF];
    size_t total = 0, n;
    while ((n = tf_pipeline_pull(p, TF_CHAN_MAIN, buf, sizeof(buf))) > 0)
        total += n;
    return total;
}

/* Generate plain text lines for text codec benchmarks */
static char *gen_text(size_t n_rows, size_t *out_len) {
    const char *prefixes[] = {"info", "warn", "error", "debug", "trace"};
    const char *msgs[] = {
        "request received from client",
        "database query executed successfully",
        "error connecting to upstream service",
        "cache miss for key user_session",
        "response sent in 42ms",
        "warning: high memory usage detected",
        "error: timeout waiting for response",
        "debug: parsing configuration file",
        "connection pool size: 10",
        "health check passed",
    };
    size_t cap = n_rows * 64;
    char *buf = malloc(cap);
    if (!buf) return NULL;
    size_t len = 0;

    for (size_t i = 0; i < n_rows; i++) {
        len += snprintf(buf + len, cap - len, "%s: %s\n",
                        prefixes[i % 5], msgs[i % 10]);
    }
    *out_len = len;
    return buf;
}

typedef struct {
    const char *label;
    const char *dsl;
} bench_case;

static void run_bench(const char *label, const char *dsl,
                      const char *csv, size_t csv_len, size_t n_rows) {
    /* Compile pipeline */
    char *err = NULL;
    tf_ir_plan *ir = tf_dsl_parse(dsl, strlen(dsl), &err);
    if (!ir) {
        fprintf(stderr, "  %-28s  SKIP (parse: %s)\n", label, err ? err : "?");
        free(err);
        return;
    }
    tf_ir_validate(ir);
    tf_ir_infer_schema(ir);
    tf_pipeline *p = tf_pipeline_create_from_ir(ir);
    tf_ir_plan_free(ir);
    if (!p) {
        fprintf(stderr, "  %-28s  SKIP (compile: %s)\n", label,
                tf_last_error() ? tf_last_error() : "?");
        return;
    }

    /* Push data in 64KB chunks */
    double t0 = now_sec();
    size_t chunk = 64 * 1024;
    size_t off = 0;
    size_t out_bytes = 0;
    while (off < csv_len) {
        size_t n = csv_len - off;
        if (n > chunk) n = chunk;
        if (tf_pipeline_push(p, (const uint8_t *)csv + off, n) != TF_OK) {
            fprintf(stderr, "  %-28s  FAIL (push)\n", label);
            tf_pipeline_free(p);
            return;
        }
        off += n;
        out_bytes += drain(p);
    }
    tf_pipeline_finish(p);
    out_bytes += drain(p);
    double elapsed = now_sec() - t0;

    double mb_in  = (double)csv_len / (1024.0 * 1024.0);
    double mb_out = (double)out_bytes / (1024.0 * 1024.0);
    double rows_per_sec = (double)n_rows / elapsed;

    printf("  %-28s  %7.1f ms  %8.0f Krows/s  %6.1f MB/s in  %6.1f MB out\n",
           label, elapsed * 1000.0, rows_per_sec / 1000.0,
           mb_in / elapsed, mb_out);

    tf_pipeline_free(p);
}

int main(int argc, char **argv) {
    size_t n_rows = 1000000;
    if (argc > 1) n_rows = (size_t)atol(argv[1]);

    printf("Generating %zu rows of CSV data...\n", n_rows);
    size_t csv_len = 0;
    char *csv = gen_csv(n_rows, &csv_len);
    if (!csv) {
        fprintf(stderr, "Failed to generate data\n");
        return 1;
    }
    printf("Generated %.1f MB\n\n", (double)csv_len / (1024.0 * 1024.0));

    bench_case cases[] = {
        {"passthrough",        "csv | csv"},
        {"filter (50%%)",      "csv | filter \"col(age) > 47\" | csv"},
        {"select 2 cols",      "csv | select name,age | csv"},
        {"rename",             "csv | rename name=full_name | csv"},
        {"head 1000",          "csv | head 1000 | csv"},
        {"skip 1000",          "csv | skip 1000 | csv"},
        {"derive (arith)",     "csv | derive total=col(age)*col(score) | csv"},
        {"unique (name)",      "csv | unique name | csv"},
        {"unique (name,city)", "csv | unique name,city | csv"},
        {"sort (age)",         "csv | sort age | csv"},
        {"sort (-score)",      "csv | sort -score | csv"},
        {"stats (default)",    "csv | stats | csv"},
        {"stats (count,sum)",  "csv | stats count,sum | csv"},
        {"stats (var,stddev)", "csv | stats var,stddev | csv"},
        {"stats (median)",     "csv | stats median | csv"},
        {"stats (p25,median,p75)", "csv | stats p25,median,p75 | csv"},
        {"stats (skew,kurt)",  "csv | stats skewness,kurtosis | csv"},
        {"stats (distinct)",   "csv | stats distinct | csv"},
        {"stats (hist)",       "csv | stats hist | csv"},
        {"stats (sample)",     "csv | stats sample | csv"},
        {"stats (all 15)",     "csv | stats count,sum,avg,min,max,var,stddev,median,p25,p75,skewness,kurtosis,distinct,hist,sample | csv"},
        {"filter+derive+head", "csv | filter \"col(age) > 30\" | derive x=col(score)*2 | head 10000 | csv"},
        {"csv to jsonl",       "csv | jsonl"},
        {"filter+sort+head",   "csv | filter \"col(age) > 40\" | sort -score | head 100 | csv"},
        /* New operators */
        {"tail 1000",          "csv | tail 1000 | csv"},
        {"clip (score)",       "csv | clip score 60 90 | csv"},
        {"replace (name)",     "csv | replace name Alice Alicia | csv"},
        {"trim (name)",        "csv | trim name | csv"},
        {"validate",           "csv | validate \"col(age) > 30\" | csv"},
        {"hash (name,city)",   "csv | hash name,city | csv"},
        {"bin (age)",          "csv | bin age 25,35,50,65 | csv"},
        {"step (running-sum)", "csv | step score running-sum | csv"},
        {"window (avg 10)",    "csv | window score 10 avg | csv"},
        {"explode (city)",     "csv | explode city , | csv"},
        {"datetime (epoch)",   "csv | datetime name year | csv"},
        {"frequency (city)",   "csv | frequency city | csv"},
        {"group-agg (city)",   "csv | group-agg city sum:score:total avg:age:avg_age | csv"},
        {"top 100 (score)",    "csv | top 100 score | csv"},
        {"sample 1000",        "csv | sample 1000 | csv"},
        {"dedup (name)",       "csv | dedup name | csv"},
        /* Complex combos with new ops */
        {"clip+step+window",   "csv | clip score 60 90 | step score running-sum | window score 5 avg | csv"},
        {"filter+freq",        "csv | filter \"col(age) > 40\" | frequency city | csv"},
    };
    size_t n_cases = sizeof(cases) / sizeof(cases[0]);

    printf("%-30s  %9s  %13s  %12s  %12s\n",
           "  Pipeline", "Time", "Throughput", "Input rate", "Output");
    printf("  %s\n", "----------------------------  ---------  -------------  ------------  ------------");

    for (size_t i = 0; i < n_cases; i++) {
        run_bench(cases[i].label, cases[i].dsl, csv, csv_len, n_rows);
    }

    /* Text codec benchmarks */
    printf("\nText codec benchmarks:\n");
    printf("%-30s  %9s  %13s  %12s  %12s\n",
           "  Pipeline", "Time", "Throughput", "Input rate", "Output");
    printf("  %s\n", "----------------------------  ---------  -------------  ------------  ------------");

    size_t text_len = 0;
    char *text = gen_text(n_rows, &text_len);
    if (text) {
        printf("  Generated %.1f MB of text\n", (double)text_len / (1024.0 * 1024.0));

        bench_case text_cases[] = {
            {"text | text",             "text | text"},
            {"text | head 1000 | text", "text | head 1000 | text"},
            {"text | tail 1000 | text", "text | tail 1000 | text"},
            {"text | grep error | text","text | grep error | text"},
            {"text | grep -v error",    "text | grep -v error | text"},
        };
        size_t n_text_cases = sizeof(text_cases) / sizeof(text_cases[0]);
        for (size_t i = 0; i < n_text_cases; i++) {
            run_bench(text_cases[i].label, text_cases[i].dsl, text, text_len, n_rows);
        }
        free(text);
    }

    printf("\nDone.\n");
    free(csv);
    return 0;
}
