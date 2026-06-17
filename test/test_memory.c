/*
 * test_memory.c - native streaming memory/output-draining regressions.
 *
 * These tests intentionally generate input rows on demand. They should fail if
 * row-local or bounded-state pipelines silently retain full input/output.
 */

#include "tranfi.h"
#include "internal.h"

#include <assert.h>
#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef __has_feature
#define __has_feature(x) 0
#endif

#if defined(__unix__) || defined(__APPLE__)
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

#define ROW_LOCAL_ROWS 200000
#define TOP_ROWS       200000
#define TOP_STRING_ROWS 120000
#define KEY_STATE_ROWS 200000
#define STATS_ROWS     200000
#define BOUNDED_ROWS   200000
#define SPILL_SORT_ROWS 50000
#define JOIN_ROWS      100000
#define BLOCKING_ROWS  4096
#define PIVOT_IDS      1024
#define PIVOT_STRESS_IDS 4096
#define PIVOT_STRESS_CATS 17
#define JOIN_STRESS_ROWS 24000
#define JOIN_STRESS_KEYS 113
#define JOIN_STRESS_MATCHES 3
#define CHUNK_ROWS     128
#define GROUP_CARDINALITY 97
#define RSS_DELTA_LIMIT_KB (96L * 1024L)
#define STREAM_READABLE_LIMIT (1024 * 1024)
#define STREAM_CAP_LIMIT      (1024 * 1024)

static int sanitizer_build(void) {
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_UNDEFINED__) || __has_feature(address_sanitizer) || __has_feature(undefined_behavior_sanitizer)
    return 1;
#else
    return 0;
#endif
}

static long current_rss_kb(void) {
#if defined(__linux__)
    FILE *f = fopen("/proc/self/status", "r");
    if (f) {
        char line[256];
        while (fgets(line, sizeof(line), f)) {
            if (strncmp(line, "VmRSS:", 6) == 0) {
                long kb = 0;
                if (sscanf(line + 6, "%ld", &kb) == 1) {
                    fclose(f);
                    return kb;
                }
            }
        }
        fclose(f);
    }
#endif
#if defined(__unix__) || defined(__APPLE__)
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) == 0) {
#if defined(__APPLE__)
        return (long)(ru.ru_maxrss / 1024);
#else
        return (long)ru.ru_maxrss;
#endif
    }
#endif
    return -1;
}

static void assert_rss_delta_bounded(long start_kb, long peak_kb) {
    if (sanitizer_build()) return;
    if (start_kb < 0 || peak_kb < 0) return;
    assert(peak_kb >= start_kb);
    long delta = peak_kb - start_kb;
    assert(delta < RSS_DELTA_LIMIT_KB);
}

static void assert_contract_for_op(const char *dsl, const char *op,
                                   tf_memory_class mem,
                                   tf_emit_class emit) {
    char *error = NULL;
    char *json = tf_compile_dsl(dsl, strlen(dsl), &error);
    if (!json) {
        fprintf(stderr, "compile failed: %s\n", error ? error : "unknown");
        free(error);
        assert(json != NULL);
    }

    tf_ir_plan *plan = tf_ir_from_json(json, strlen(json), &error);
    assert(plan != NULL);
    assert(tf_ir_validate(plan) == TF_OK);

    int found = 0;
    for (size_t i = 0; i < plan->n_nodes; i++) {
        if (strcmp(plan->nodes[i].op, op) == 0) {
            assert(plan->nodes[i].memory_class == mem);
            assert(plan->nodes[i].emit_class == emit);
            assert(plan->nodes[i].state_estimate != NULL);
            found = 1;
            break;
        }
    }
    assert(found);

    tf_ir_plan_free(plan);
    tf_string_free(json);
    free(error);
}

static tf_pipeline *create_pipeline_from_dsl(const char *dsl) {
    char *error = NULL;
    char *json = tf_compile_dsl(dsl, strlen(dsl), &error);
    if (!json) {
        fprintf(stderr, "compile failed: %s\n", error ? error : "unknown");
        free(error);
        assert(json != NULL);
    }
    tf_pipeline *p = tf_pipeline_create(json, strlen(json));
    if (!p) {
        fprintf(stderr, "pipeline create failed: %s\n", tf_last_error() ? tf_last_error() : "unknown");
    }
    tf_string_free(json);
    free(error);
    assert(p != NULL);
    return p;
}

static size_t drain_channel(tf_pipeline *p, int channel) {
    uint8_t buf[8192];
    size_t total = 0;
    for (;;) {
        size_t n = tf_pipeline_pull(p, channel, buf, sizeof(buf));
        if (n == 0) break;
        total += n;
    }
    return total;
}

static size_t write_lookup_rows(FILE *f, size_t n_groups) {
    fprintf(f, "group,label\n");
    for (size_t i = 0; i < n_groups; i++) {
        fprintf(f, "%zu,g%zu\n", i, i);
    }
    return n_groups;
}

static size_t write_rows(char *buf, size_t cap, size_t first_row, size_t n_rows) {
    size_t len = 0;
    for (size_t i = 0; i < n_rows; i++) {
        size_t id = first_row + i;
        int score = (int)((id * 17) % 100000);
        int group = (int)(id % 97);
        int n = snprintf(buf + len, cap - len, "%zu,%d,%d\n", id, score, group);
        assert(n > 0);
        assert((size_t)n < cap - len);
        len += (size_t)n;
    }
    return len;
}

static size_t write_top_string_rows(char *buf, size_t cap, size_t first_row, size_t n_rows) {
    static char pad[1025];
    static int pad_ready = 0;
    if (!pad_ready) {
        memset(pad, 'x', sizeof(pad) - 1);
        pad[sizeof(pad) - 1] = '\0';
        pad_ready = 1;
    }

    size_t len = 0;
    for (size_t i = 0; i < n_rows; i++) {
        size_t id = first_row + i;
        int n = snprintf(buf + len, cap - len, "%zu,%zu,item_%zu_%s\n", id, id, id, pad);
        assert(n > 0);
        assert((size_t)n < cap - len);
        len += (size_t)n;
    }
    return len;
}

static size_t write_pivot_rows(char *buf, size_t cap, size_t first_id, size_t n_ids) {
    size_t len = 0;
    for (size_t i = 0; i < n_ids; i++) {
        size_t id = first_id + i;
        int v1 = (int)(id % 1000);
        int v2 = (int)((id * 3) % 1000);
        int n = snprintf(buf + len, cap - len, "%zu,a,%d\n%zu,b,%d\n", id, v1, id, v2);
        assert(n > 0);
        assert((size_t)n < cap - len);
        len += (size_t)n;
    }
    return len;
}


static size_t write_pivot_stress_rows(char *buf, size_t cap, size_t first_id, size_t n_ids) {
    size_t len = 0;
    for (size_t i = 0; i < n_ids; i++) {
        size_t id = first_id + i;
        for (size_t c = 0; c < PIVOT_STRESS_CATS; c++) {
            int value = (int)((id * 31 + c * 7) % 10000);
            int n = snprintf(buf + len, cap - len, "%zu,c%02zu,%d\n", id, c, value);
            assert(n > 0);
            assert((size_t)n < cap - len);
            len += (size_t)n;
        }
    }
    return len;
}

static size_t write_join_stress_rows(char *buf, size_t cap, size_t first_row, size_t n_rows) {
    size_t len = 0;
    for (size_t i = 0; i < n_rows; i++) {
        size_t row = first_row + i;
        size_t id = row % JOIN_STRESS_KEYS;
        int score = (int)((row * 19) % 100000);
        int group = (int)(row % 29);
        int n = snprintf(buf + len, cap - len, "%zu,%d,%d\n", id, score, group);
        assert(n > 0);
        assert((size_t)n < cap - len);
        len += (size_t)n;
    }
    return len;
}

static void update_peak(long *peak_kb) {
    long rss = current_rss_kb();
    if (rss > *peak_kb) *peak_kb = rss;
}

static int dir_has_regular_entries(const char *path) {
    DIR *d = opendir(path);
    assert(d != NULL);
    struct dirent *ent = NULL;
    while ((ent = readdir(d)) != NULL) {
        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
        closedir(d);
        return 1;
    }
    closedir(d);
    return 0;
}

static void test_buffer_releases_large_empty_capacity(void) {
    tf_buffer b;
    tf_buffer_init(&b);

    size_t n = 3 * 1024 * 1024;
    uint8_t *data = malloc(n);
    assert(data != NULL);
    memset(data, 'x', n);
    assert(tf_buffer_write(&b, data, n) == TF_OK);
    assert(b.cap >= n);

    uint8_t out[8192];
    size_t total = 0;
    while (total < n) {
        total += tf_buffer_read(&b, out, sizeof(out));
    }
    assert(total == n);
    assert(tf_buffer_readable(&b) == 0);
    assert(b.cap <= STREAM_CAP_LIMIT);

    free(data);
    tf_buffer_free(&b);
}

static void test_row_local_pipeline_drains_incrementally(void) {
    tf_pipeline *p = create_pipeline_from_dsl(
        "csv | filter \"col(score) >= 0\" | select id,score | csv");

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t total_out = 0;
    size_t prefinish_drains = 0;
    size_t max_readable = 0;
    size_t max_cap = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < ROW_LOCAL_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > ROW_LOCAL_ROWS) n_rows = ROW_LOCAL_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);

        size_t readable = tf_buffer_readable(&p->output[TF_CHAN_MAIN]);
        if (readable > 0) prefinish_drains++;
        if (readable > max_readable) max_readable = readable;

        total_out += drain_channel(p, TF_CHAN_MAIN);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        if (p->output[TF_CHAN_MAIN].cap > max_cap) max_cap = p->output[TF_CHAN_MAIN].cap;

        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_drains > 0);
    assert(max_readable < STREAM_READABLE_LIMIT);
    assert(max_cap <= STREAM_CAP_LIMIT);

    assert(tf_pipeline_finish(p) == TF_OK);
    total_out += drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == ROW_LOCAL_ROWS);
    assert(p->rows_out == ROW_LOCAL_ROWS);
    assert(total_out > ROW_LOCAL_ROWS);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
}

typedef struct memory_sink_capture {
    size_t bytes;
    size_t calls;
} memory_sink_capture;

static int memory_counting_sink(int channel, const uint8_t *data, size_t len, void *user) {
    memory_sink_capture *cap = (memory_sink_capture *)user;
    assert(channel == TF_CHAN_MAIN);
    assert(data != NULL || len == 0);
    cap->bytes += len;
    cap->calls++;
    return TF_OK;
}


static void assert_no_newline_record_cap_stays_bounded(const char *dsl,
                                                       const char *prefix,
                                                       const char *expected_error,
                                                       const char *expected_diag) {
    tf_pipeline *p = create_pipeline_from_dsl(dsl);
    assert(tf_pipeline_push(p, (const uint8_t *)prefix, strlen(prefix)) == TF_OK);
    drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_ERRORS);

    char chunk[4096];
    memset(chunk, 'x', sizeof(chunk));
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;
    int failed = 0;

    for (size_t i = 0; i < 64; i++) {
        int rc = tf_pipeline_push(p, (const uint8_t *)chunk, sizeof(chunk));
        update_peak(&peak_kb);
        if (rc == TF_ERROR) {
            failed = 1;
            break;
        }
        drain_channel(p, TF_CHAN_MAIN);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    }

    assert(failed);
    const char *err = tf_pipeline_error(p);
    assert(err != NULL);
    assert(strstr(err, expected_error) != NULL);

    uint8_t errors[2048];
    size_t e = tf_pipeline_pull(p, TF_CHAN_ERRORS, errors, sizeof(errors) - 1);
    assert(e > 0);
    errors[e] = '\0';
    assert(strstr((char *)errors, expected_diag) != NULL);
    assert(strstr((char *)errors, "record exceeds max_record_bytes") != NULL);

    update_peak(&peak_kb);
    assert_rss_delta_bounded(start_kb, peak_kb);
    tf_pipeline_free(p);
}

static void test_text_jsonl_record_caps_bound_no_newline_input(void) {
    assert_no_newline_record_cap_stays_bounded(
        "text max_record_bytes=32768 max_error_bytes=32 | text",
        "ok\n",
        "text record exceeds max_record_bytes",
        "text_record_too_large");

    assert_no_newline_record_cap_stays_bounded(
        "jsonl max_record_bytes=32768 max_error_bytes=32 | csv",
        "{\"id\":1}\n{\"name\":\"",
        "jsonl record exceeds max_record_bytes",
        "jsonl_record_too_large");
}

static void test_row_local_pipeline_sink_stays_drained(void) {
    tf_pipeline *p = create_pipeline_from_dsl(
        "csv | filter \"col(score) >= 0\" | select id,score | csv");
    memory_sink_capture cap = {0};
    assert(tf_pipeline_set_sink(p, TF_CHAN_MAIN, memory_counting_sink, &cap) == TF_OK);

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);

    char chunk[8192];
    size_t max_cap = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < ROW_LOCAL_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > ROW_LOCAL_ROWS) n_rows = ROW_LOCAL_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        if (p->output[TF_CHAN_MAIN].cap > max_cap) max_cap = p->output[TF_CHAN_MAIN].cap;
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(cap.calls > 0);
    assert(cap.bytes > ROW_LOCAL_ROWS);
    assert(max_cap <= STREAM_CAP_LIMIT);
    assert(peak_kb - start_kb < RSS_DELTA_LIMIT_KB);

    assert(tf_pipeline_finish(p) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    assert(drain_channel(p, TF_CHAN_STATS) > 0);
    tf_pipeline_free(p);
}

static void test_bounded_flush_latent_top_stays_small(void) {
    tf_pipeline *p = create_pipeline_from_dsl("csv | top 25 score | csv");

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t prefinish_out = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < TOP_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > TOP_ROWS) n_rows = TOP_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        prefinish_out += drain_channel(p, TF_CHAN_MAIN);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_out == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    size_t final_out = drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == TOP_ROWS);
    assert(p->rows_out == 25);
    assert(final_out > 0);
    assert(final_out < 8192);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
}

static void test_top_heap_replacements_compact_string_storage(void) {
    assert_contract_for_op("csv | top-k 25 score | csv", "top-k",
                           TF_MEM_BOUNDED_STATE, TF_EMIT_ON_FLUSH);
    tf_pipeline *p = create_pipeline_from_dsl("csv | top-k 25 score | csv");

    const char *header = "id,score,name\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[196608];
    size_t prefinish_out = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < TOP_STRING_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > TOP_STRING_ROWS) n_rows = TOP_STRING_ROWS - row;
        size_t len = write_top_string_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        prefinish_out += drain_channel(p, TF_CHAN_MAIN);
        if ((row / CHUNK_ROWS) % 8 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_out == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    size_t final_out = drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == TOP_STRING_ROWS);
    assert(p->rows_out == 25);
    assert(final_out > 0);
    assert(final_out < 65536);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
}


static void test_tail_flush_latent_stays_small(void) {
    assert_contract_for_op("csv | tail 50 | csv", "tail",
                           TF_MEM_BOUNDED_STATE, TF_EMIT_ON_FLUSH);
    tf_pipeline *p = create_pipeline_from_dsl("csv | tail 50 | csv");

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t prefinish_out = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < BOUNDED_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > BOUNDED_ROWS) n_rows = BOUNDED_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        prefinish_out += drain_channel(p, TF_CHAN_MAIN);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_out == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    size_t final_out = drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == BOUNDED_ROWS);
    assert(p->rows_out == 50);
    assert(final_out > 0);
    assert(final_out < 8192);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
}

static void test_lag_and_lead_offsets_stay_small(void) {
    assert_contract_for_op("csv | lag score 64 prev_score | csv", "lag",
                           TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH);
    assert_contract_for_op("csv | lead score 64 next_score | csv", "lead",
                           TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH);
    assert_contract_for_op("csv | shift score 64 shifted_score | csv", "shift",
                           TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH);
    tf_pipeline *p = create_pipeline_from_dsl(
        "csv batch_size=64 | lag score 64 prev_score | shift score 64 shifted_score | lead score 64 next_score | csv");

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t total_out = 0;
    size_t prefinish_drains = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < BOUNDED_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > BOUNDED_ROWS) n_rows = BOUNDED_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        if (tf_buffer_readable(&p->output[TF_CHAN_MAIN]) > 0) prefinish_drains++;
        total_out += drain_channel(p, TF_CHAN_MAIN);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_drains > 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    total_out += drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == BOUNDED_ROWS);
    assert(p->rows_out == BOUNDED_ROWS);
    assert(total_out > BOUNDED_ROWS);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
}

static void test_window_and_rolling_aliases_stay_small(void) {
    assert_contract_for_op("csv | window score 64 avg score_ma64 | csv", "window",
                           TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH);
    assert_contract_for_op("csv | rolling-sum score 64 score_sum64 | csv", "rolling-sum",
                           TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH);
    tf_pipeline *p = create_pipeline_from_dsl(
        "csv batch_size=64 | window score 64 avg score_ma64 | rolling-sum score 64 score_sum64 | rolling-max score 64 score_max64 | csv");

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t total_out = 0;
    size_t prefinish_drains = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < BOUNDED_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > BOUNDED_ROWS) n_rows = BOUNDED_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        if (tf_buffer_readable(&p->output[TF_CHAN_MAIN]) > 0) prefinish_drains++;
        total_out += drain_channel(p, TF_CHAN_MAIN);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_drains > 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    total_out += drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == BOUNDED_ROWS);
    assert(p->rows_out == BOUNDED_ROWS);
    assert(total_out > BOUNDED_ROWS);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
}

static void test_sample_flush_latent_stays_small(void) {
    assert_contract_for_op("csv | sample 50 | csv", "sample",
                           TF_MEM_BOUNDED_STATE, TF_EMIT_ON_FLUSH);
    tf_pipeline *p = create_pipeline_from_dsl("csv | sample 50 | csv");

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t prefinish_out = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < BOUNDED_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > BOUNDED_ROWS) n_rows = BOUNDED_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        prefinish_out += drain_channel(p, TF_CHAN_MAIN);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_out == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    size_t final_out = drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == BOUNDED_ROWS);
    assert(p->rows_out == 50);
    assert(final_out > 0);
    assert(final_out < 8192);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
}

static void test_frequency_overflow_stays_small(void) {
    assert_contract_for_op("csv | frequency group max_values=16 overflow=other other=REST | csv", "frequency",
                           TF_MEM_KEY_STATE, TF_EMIT_ON_FLUSH);
    tf_pipeline *p = create_pipeline_from_dsl(
        "csv | frequency group max_values=16 overflow=other other=REST | csv");

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t prefinish_out = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < BOUNDED_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > BOUNDED_ROWS) n_rows = BOUNDED_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        prefinish_out += drain_channel(p, TF_CHAN_MAIN);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_out == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    size_t final_out = drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == BOUNDED_ROWS);
    assert(p->rows_out == 17);
    assert(final_out > 0);
    assert(final_out < 8192);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
}

static void test_onehot_declared_categories_stays_small(void) {
    const char *dsl = "csv batch_size=64 | onehot group categories=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 max_categories=17 unknown=other --drop | csv";
    assert_contract_for_op(dsl, "onehot", TF_MEM_KEY_STATE, TF_EMIT_PER_BATCH);
    tf_pipeline *p = create_pipeline_from_dsl(dsl);

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t total_out = 0;
    size_t prefinish_drains = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < BOUNDED_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > BOUNDED_ROWS) n_rows = BOUNDED_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        if (tf_buffer_readable(&p->output[TF_CHAN_MAIN]) > 0) prefinish_drains++;
        total_out += drain_channel(p, TF_CHAN_MAIN);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_drains > 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    total_out += drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == BOUNDED_ROWS);
    assert(p->rows_out == BOUNDED_ROWS);
    assert(total_out > BOUNDED_ROWS);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
}

static void test_label_encode_declared_categories_stays_small(void) {
    const char *dsl = "csv batch_size=64 | label-encode group group_code categories=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15 max_categories=17 unknown=other | csv";
    assert_contract_for_op(dsl, "label-encode", TF_MEM_KEY_STATE, TF_EMIT_PER_BATCH);
    tf_pipeline *p = create_pipeline_from_dsl(dsl);

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t total_out = 0;
    size_t prefinish_drains = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < BOUNDED_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > BOUNDED_ROWS) n_rows = BOUNDED_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        if (tf_buffer_readable(&p->output[TF_CHAN_MAIN]) > 0) prefinish_drains++;
        total_out += drain_channel(p, TF_CHAN_MAIN);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_drains > 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    total_out += drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == BOUNDED_ROWS);
    assert(p->rows_out == BOUNDED_ROWS);
    assert(total_out > BOUNDED_ROWS);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
}

static void test_stats_on_flush_stays_small(void) {
    assert_contract_for_op("csv | stats count,sum,avg,distinct | csv", "stats",
                           TF_MEM_BOUNDED_STATE, TF_EMIT_ON_FLUSH);
    tf_pipeline *p = create_pipeline_from_dsl("csv | stats count,sum,avg,distinct | csv");

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t prefinish_out = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < STATS_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > STATS_ROWS) n_rows = STATS_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        prefinish_out += drain_channel(p, TF_CHAN_MAIN);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_out == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    size_t final_out = drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == STATS_ROWS);
    assert(p->rows_out == 3);
    assert(final_out > 0);
    assert(final_out < 8192);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
}

static void test_low_cardinality_unique_drains_and_stays_small(void) {
    assert_contract_for_op("csv | unique group | csv", "unique",
                           TF_MEM_KEY_STATE, TF_EMIT_PER_BATCH);
    tf_pipeline *p = create_pipeline_from_dsl("csv | unique group | csv");

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t total_out = 0;
    size_t prefinish_drains = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < KEY_STATE_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > KEY_STATE_ROWS) n_rows = KEY_STATE_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        if (tf_buffer_readable(&p->output[TF_CHAN_MAIN]) > 0) prefinish_drains++;
        total_out += drain_channel(p, TF_CHAN_MAIN);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_drains > 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    total_out += drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == KEY_STATE_ROWS);
    assert(p->rows_out == GROUP_CARDINALITY);
    assert(total_out > 0);
    assert(total_out < 16384);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
}


static void test_low_cardinality_rowid_drains_and_stays_small(void) {
    assert_contract_for_op("csv | rowid group max_keys=97 | csv", "rowid",
                           TF_MEM_KEY_STATE, TF_EMIT_PER_BATCH);
    tf_pipeline *p = create_pipeline_from_dsl("csv | rowid group max_keys=97 | csv");

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t total_out = 0;
    size_t prefinish_drains = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < KEY_STATE_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > KEY_STATE_ROWS) n_rows = KEY_STATE_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        if (tf_buffer_readable(&p->output[TF_CHAN_MAIN]) > 0) prefinish_drains++;
        total_out += drain_channel(p, TF_CHAN_MAIN);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_drains > 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    total_out += drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == KEY_STATE_ROWS);
    assert(p->rows_out == KEY_STATE_ROWS);
    assert(total_out > 0);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
}

static void test_low_cardinality_group_agg_stays_small(void) {
    assert_contract_for_op("csv | group-agg group sum:score:total count:score:n | csv", "group-agg",
                           TF_MEM_KEY_STATE, TF_EMIT_MIXED);
    tf_pipeline *p = create_pipeline_from_dsl("csv | group-agg group sum:score:total count:score:n | csv");

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t prefinish_out = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < KEY_STATE_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > KEY_STATE_ROWS) n_rows = KEY_STATE_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        prefinish_out += drain_channel(p, TF_CHAN_MAIN);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_out == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    size_t final_out = drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == KEY_STATE_ROWS);
    assert(p->rows_out == GROUP_CARDINALITY);
    assert(final_out > 0);
    assert(final_out < 16384);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
}

static void test_small_lookup_join_streams_left_side(void) {
    assert_contract_for_op("csv | join /tmp/tranfi_memory_lookup.csv on=group | csv", "join",
                           TF_MEM_KEY_STATE, TF_EMIT_PER_BATCH);

    const char *lookup_path = "/tmp/tranfi_memory_lookup.csv";
    FILE *f = fopen(lookup_path, "w");
    assert(f != NULL);
    write_lookup_rows(f, GROUP_CARDINALITY);
    fclose(f);

    char dsl[256];
    snprintf(dsl, sizeof(dsl), "csv | join %s on=group | csv", lookup_path);
    tf_pipeline *p = create_pipeline_from_dsl(dsl);

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t total_out = 0;
    size_t prefinish_drains = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < JOIN_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > JOIN_ROWS) n_rows = JOIN_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        if (tf_buffer_readable(&p->output[TF_CHAN_MAIN]) > 0) prefinish_drains++;
        total_out += drain_channel(p, TF_CHAN_MAIN);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_drains > 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    total_out += drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == JOIN_ROWS);
    assert(p->rows_out == JOIN_ROWS);
    assert(total_out > JOIN_ROWS);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
    remove(lookup_path);
}


static void test_small_lookup_set_ops_stream_left_side(void) {
    assert_contract_for_op("csv | intersect /tmp/tranfi_memory_set_lookup.csv columns=group max_lookup_bytes=4096 max_lookup_keys=97 max_output_keys=97 | csv",
                           "intersect", TF_MEM_KEY_STATE, TF_EMIT_PER_BATCH);

    const char *lookup_path = "/tmp/tranfi_memory_set_lookup.csv";
    FILE *f = fopen(lookup_path, "w");
    assert(f != NULL);
    write_lookup_rows(f, GROUP_CARDINALITY);
    fclose(f);

    char dsl[512];
    snprintf(dsl, sizeof(dsl),
             "csv | intersect %s columns=group max_lookup_bytes=4096 max_lookup_keys=97 max_output_keys=97 | csv",
             lookup_path);
    tf_pipeline *p = create_pipeline_from_dsl(dsl);

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t total_out = 0;
    size_t prefinish_drains = 0;
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;

    for (size_t row = 0; row < JOIN_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > JOIN_ROWS) n_rows = JOIN_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        if (tf_buffer_readable(&p->output[TF_CHAN_MAIN]) > 0) prefinish_drains++;
        total_out += drain_channel(p, TF_CHAN_MAIN);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_drains > 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    total_out += drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);

    assert(p->rows_in == JOIN_ROWS);
    assert(p->rows_out == GROUP_CARDINALITY);
    assert(total_out > GROUP_CARDINALITY);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
    remove(lookup_path);
}

static void test_union_all_streams_appended_file(void) {
    assert_contract_for_op("csv | union-all /tmp/tranfi_memory_union_all.csv | csv", "union-all",
                           TF_MEM_BOUNDED_STATE, TF_EMIT_MIXED);

    const char *file_path = "/tmp/tranfi_memory_union_all.csv";
    FILE *f = fopen(file_path, "w");
    assert(f != NULL);
    fprintf(f, "id,score,group\n");

    char chunk[8192];
    for (size_t row = 0; row < JOIN_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > JOIN_ROWS) n_rows = JOIN_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(fwrite(chunk, 1, len, f) == len);
    }
    fclose(f);

    char dsl[512];
    snprintf(dsl, sizeof(dsl), "csv | union-all %s | csv", file_path);
    tf_pipeline *p = create_pipeline_from_dsl(dsl);

    memory_sink_capture cap = {0};
    assert(tf_pipeline_set_sink(p, TF_CHAN_MAIN, memory_counting_sink, &cap) == TF_OK);

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    size_t len = write_rows(chunk, sizeof(chunk), JOIN_ROWS, CHUNK_ROWS);
    assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);

    long start_kb = current_rss_kb();
    long peak_kb = start_kb;
    assert(tf_pipeline_finish(p) == TF_OK);
    update_peak(&peak_kb);
    drain_channel(p, TF_CHAN_STATS);

    assert(p->rows_in == CHUNK_ROWS);
    assert(p->rows_out == JOIN_ROWS + CHUNK_ROWS);
    assert(cap.calls > 1);
    assert(cap.bytes > JOIN_ROWS);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
    remove(file_path);
}

static void test_blocking_sort_contract_and_flush_latency(void) {
    assert_contract_for_op("csv | sort score | csv", "sort",
                           TF_MEM_BLOCKING, TF_EMIT_ON_FLUSH);
    tf_pipeline *p = create_pipeline_from_dsl("csv | sort score | csv");

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t prefinish_out = 0;
    for (size_t row = 0; row < BLOCKING_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > BLOCKING_ROWS) n_rows = BLOCKING_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        prefinish_out += drain_channel(p, TF_CHAN_MAIN);
    }

    assert(prefinish_out == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    size_t final_out = drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    assert(p->rows_in == BLOCKING_ROWS);
    assert(p->rows_out == BLOCKING_ROWS);
    assert(final_out > BLOCKING_ROWS);

    tf_pipeline_free(p);
}

static void test_spill_sort_streams_flush_chunks_and_cleans_up(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_spill_sort_%ld", (long)getpid());
    if (mkdir(spill_dir, 0700) != 0 && errno != EEXIST) {
        assert(!"failed to create spill dir");
    }

    char json[1024];
    snprintf(json, sizeof(json),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":64}},"
             "{\"op\":\"sort\",\"args\":{\"columns\":[{\"name\":\"score\",\"desc\":false}],"
             "\"spill_dir\":\"%s\",\"spill_run_rows\":1024,\"spill_output_rows\":128}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
             spill_dir);
    tf_pipeline *p = tf_pipeline_create(json, strlen(json));
    assert(p != NULL);

    memory_sink_capture cap = {0};
    assert(tf_pipeline_set_sink(p, TF_CHAN_MAIN, memory_counting_sink, &cap) == TF_OK);

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);

    char chunk[8192];
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;
    for (size_t row = 0; row < SPILL_SORT_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > SPILL_SORT_ROWS) n_rows = SPILL_SORT_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(cap.calls == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    update_peak(&peak_kb);

    assert(p->rows_in == SPILL_SORT_ROWS);
    assert(p->rows_out == SPILL_SORT_ROWS);
    assert(cap.calls > 1);
    assert(cap.bytes > SPILL_SORT_ROWS);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);
    assert(dir_has_regular_entries(spill_dir) == 0);

    drain_channel(p, TF_CHAN_STATS);
    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
}


static void test_spill_unique_streams_flush_chunks_and_cleans_up(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_spill_unique_%ld", (long)getpid());
    if (mkdir(spill_dir, 0700) != 0 && errno != EEXIST) {
        assert(!"failed to create spill dir");
    }

    char json[1024];
    snprintf(json, sizeof(json),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":64}},"
             "{\"op\":\"unique\",\"args\":{\"columns\":[\"id\"],"
             "\"spill_dir\":\"%s\",\"spill_run_rows\":1024,\"spill_output_rows\":128}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
             spill_dir);
    tf_pipeline *p = tf_pipeline_create(json, strlen(json));
    assert(p != NULL);

    memory_sink_capture cap = {0};
    assert(tf_pipeline_set_sink(p, TF_CHAN_MAIN, memory_counting_sink, &cap) == TF_OK);

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);

    char chunk[8192];
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;
    for (size_t row = 0; row < SPILL_SORT_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > SPILL_SORT_ROWS) n_rows = SPILL_SORT_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(cap.calls == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    update_peak(&peak_kb);

    assert(p->rows_in == SPILL_SORT_ROWS);
    assert(p->rows_out == SPILL_SORT_ROWS);
    assert(cap.calls > 1);
    assert(cap.bytes > SPILL_SORT_ROWS);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);
    assert(dir_has_regular_entries(spill_dir) == 0);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats, "\"spill_distinct_rows\":50000") != NULL);
    assert(strstr((char *)stats, "\"spill_output_rows\":50000") != NULL);

    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
}


static void test_spill_group_agg_streams_flush_chunks_and_cleans_up(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_spill_group_agg_%ld", (long)getpid());
    if (mkdir(spill_dir, 0700) != 0 && errno != EEXIST) {
        assert(!"failed to create spill dir");
    }

    char json[1536];
    snprintf(json, sizeof(json),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":64}},"
             "{\"op\":\"group-agg\",\"args\":{\"group_by\":[\"id\"],"
             "\"spill_dir\":\"%s\",\"spill_run_rows\":1024,\"spill_output_rows\":128,"
             "\"aggs\":[{\"column\":\"score\",\"func\":\"sum\",\"name\":\"total\"}]}} ,"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
             spill_dir);
    tf_pipeline *p = tf_pipeline_create(json, strlen(json));
    assert(p != NULL);

    memory_sink_capture cap = {0};
    assert(tf_pipeline_set_sink(p, TF_CHAN_MAIN, memory_counting_sink, &cap) == TF_OK);

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);

    char chunk[8192];
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;
    for (size_t row = 0; row < SPILL_SORT_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > SPILL_SORT_ROWS) n_rows = SPILL_SORT_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(cap.calls == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    update_peak(&peak_kb);

    assert(p->rows_in == SPILL_SORT_ROWS);
    assert(p->rows_out == SPILL_SORT_ROWS);
    assert(cap.calls > 1);
    assert(cap.bytes > SPILL_SORT_ROWS);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);
    assert(dir_has_regular_entries(spill_dir) == 0);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats, "\"spill_distinct_groups\":50000") != NULL);
    assert(strstr((char *)stats, "\"spill_output_rows\":50000") != NULL);

    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
}


static void test_spill_filtering_join_streams_flush_chunks_and_cleans_up(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_spill_join_%ld", (long)getpid());
    if (mkdir(spill_dir, 0700) != 0 && errno != EEXIST) {
        assert(!"failed to create spill dir");
    }

    char lookup_path[256];
    snprintf(lookup_path, sizeof(lookup_path), "/tmp/tranfi_spill_join_lookup_%ld.csv", (long)getpid());
    FILE *lookup = fopen(lookup_path, "w");
    assert(lookup != NULL);
    fprintf(lookup, "id,label\n");
    for (size_t row = 0; row < SPILL_SORT_ROWS; row += 2) {
        fprintf(lookup, "%zu,k%zu\n", row, row);
    }
    fclose(lookup);

    char json[1536];
    snprintf(json, sizeof(json),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":64}},"
             "{\"op\":\"semi-join\",\"args\":{\"file\":\"%s\",\"on\":\"id\","
             "\"spill_dir\":\"%s\",\"spill_run_rows\":1024,\"spill_output_rows\":128}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
             lookup_path, spill_dir);
    tf_pipeline *p = tf_pipeline_create(json, strlen(json));
    assert(p != NULL);

    memory_sink_capture cap = {0};
    assert(tf_pipeline_set_sink(p, TF_CHAN_MAIN, memory_counting_sink, &cap) == TF_OK);

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);

    char chunk[8192];
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;
    for (size_t row = 0; row < SPILL_SORT_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > SPILL_SORT_ROWS) n_rows = SPILL_SORT_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(cap.calls == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    update_peak(&peak_kb);

    assert(p->rows_in == SPILL_SORT_ROWS);
    assert(p->rows_out == SPILL_SORT_ROWS / 2);
    assert(cap.calls > 1);
    assert(cap.bytes > SPILL_SORT_ROWS / 2);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);
    assert(dir_has_regular_entries(spill_dir) == 0);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats, "\"spill_output_rows\":25000") != NULL);
    assert(strstr((char *)stats, "\"spill_kept_rows\":25000") != NULL);

    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
    remove(lookup_path);
}

static void test_spill_mutating_join_streams_flush_chunks_and_cleans_up(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_spill_mutating_join_%ld", (long)getpid());
    if (mkdir(spill_dir, 0700) != 0 && errno != EEXIST) {
        assert(!"failed to create spill dir");
    }

    char lookup_path[256];
    snprintf(lookup_path, sizeof(lookup_path), "/tmp/tranfi_spill_mutating_join_lookup_%ld.csv", (long)getpid());
    FILE *lookup = fopen(lookup_path, "w");
    assert(lookup != NULL);
    fprintf(lookup, "id,label,tag\n");
    for (size_t row = 0; row < SPILL_SORT_ROWS; row += 2) {
        fprintf(lookup, "%zu,k%zu,a\n%zu,k%zu,b\n", row, row, row, row);
    }
    fclose(lookup);

    char json[1600];
    snprintf(json, sizeof(json),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":64}},"
             "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\","
             "\"max_matches_per_row\":2,\"spill_dir\":\"%s\","
             "\"spill_run_rows\":1024,\"spill_output_rows\":128}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
             lookup_path, spill_dir);
    tf_pipeline *p = tf_pipeline_create(json, strlen(json));
    assert(p != NULL);

    memory_sink_capture cap = {0};
    assert(tf_pipeline_set_sink(p, TF_CHAN_MAIN, memory_counting_sink, &cap) == TF_OK);

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);

    char chunk[8192];
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;
    for (size_t row = 0; row < SPILL_SORT_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > SPILL_SORT_ROWS) n_rows = SPILL_SORT_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(cap.calls == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    update_peak(&peak_kb);

    assert(p->rows_in == SPILL_SORT_ROWS);
    assert(p->rows_out == SPILL_SORT_ROWS);
    assert(cap.calls > 1);
    assert(cap.bytes > SPILL_SORT_ROWS);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);
    assert(dir_has_regular_entries(spill_dir) == 0);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats, "\"schema_class\":\"data_dependent\"") != NULL);
    assert(strstr((char *)stats, "\"spill_output_rows\":50000") != NULL);
    assert(strstr((char *)stats, "\"spill_kept_rows\":50000") != NULL);

    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
    remove(lookup_path);
}


static void test_spill_mutating_join_tiny_runs_with_repeated_left_keys(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_spill_mutating_join_stress_%ld", (long)getpid());
    if (mkdir(spill_dir, 0700) != 0 && errno != EEXIST) {
        assert(!"failed to create spill dir");
    }

    char lookup_path[256];
    snprintf(lookup_path, sizeof(lookup_path), "/tmp/tranfi_spill_mutating_join_stress_lookup_%ld.csv", (long)getpid());
    FILE *lookup = fopen(lookup_path, "w");
    assert(lookup != NULL);
    fprintf(lookup, "id,label,tag\n");
    for (size_t key = 0; key < JOIN_STRESS_KEYS; key++) {
        for (size_t m = 0; m < JOIN_STRESS_MATCHES; m++) {
            fprintf(lookup, "%zu,k%zu,m%zu\n", key, key, m);
        }
    }
    fclose(lookup);

    char json[1600];
    snprintf(json, sizeof(json),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":5}},"
             "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\","
             "\"max_matches_per_row\":%d,\"spill_dir\":\"%s\","
             "\"spill_run_rows\":97,\"spill_output_rows\":31}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
             lookup_path, JOIN_STRESS_MATCHES, spill_dir);
    tf_pipeline *p = tf_pipeline_create(json, strlen(json));
    assert(p != NULL);

    memory_sink_capture cap = {0};
    assert(tf_pipeline_set_sink(p, TF_CHAN_MAIN, memory_counting_sink, &cap) == TF_OK);

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);

    char chunk[8192];
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;
    for (size_t row = 0; row < JOIN_STRESS_ROWS; row += 37) {
        size_t n_rows = 37;
        if (row + n_rows > JOIN_STRESS_ROWS) n_rows = JOIN_STRESS_ROWS - row;
        size_t len = write_join_stress_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        if ((row / 37) % 64 == 0) update_peak(&peak_kb);
    }

    assert(cap.calls == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    update_peak(&peak_kb);

    size_t expected_rows = JOIN_STRESS_ROWS * JOIN_STRESS_MATCHES;
    assert(p->rows_in == JOIN_STRESS_ROWS);
    assert(p->rows_out == expected_rows);
    assert(cap.calls > 10);
    assert(cap.bytes > expected_rows);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);
    assert(dir_has_regular_entries(spill_dir) == 0);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats, "\"schema_class\":\"data_dependent\"") != NULL);
    assert(strstr((char *)stats, "\"spill_output_rows\":72000") != NULL);
    assert(strstr((char *)stats, "\"spill_kept_rows\":72000") != NULL);

    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
    remove(lookup_path);
}

static void test_spill_set_ops_streams_flush_chunks_and_cleans_up(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_spill_set_%ld", (long)getpid());
    if (mkdir(spill_dir, 0700) != 0 && errno != EEXIST) {
        assert(!"failed to create spill dir");
    }

    char lookup_path[256];
    snprintf(lookup_path, sizeof(lookup_path), "/tmp/tranfi_spill_set_lookup_%ld.csv", (long)getpid());
    FILE *lookup = fopen(lookup_path, "w");
    assert(lookup != NULL);
    fprintf(lookup, "id,label\n");
    for (size_t row = 0; row < SPILL_SORT_ROWS; row += 2) {
        fprintf(lookup, "%zu,k%zu\n", row, row);
    }
    fclose(lookup);

    char json[1536];
    snprintf(json, sizeof(json),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":64}},"
             "{\"op\":\"intersect\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],"
             "\"spill_dir\":\"%s\",\"spill_run_rows\":1024,\"spill_output_rows\":128}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
             lookup_path, spill_dir);
    tf_pipeline *p = tf_pipeline_create(json, strlen(json));
    assert(p != NULL);

    memory_sink_capture cap = {0};
    assert(tf_pipeline_set_sink(p, TF_CHAN_MAIN, memory_counting_sink, &cap) == TF_OK);

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);

    char chunk[8192];
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;
    for (size_t row = 0; row < SPILL_SORT_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > SPILL_SORT_ROWS) n_rows = SPILL_SORT_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(cap.calls == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    update_peak(&peak_kb);

    assert(p->rows_in == SPILL_SORT_ROWS);
    assert(p->rows_out == SPILL_SORT_ROWS / 2);
    assert(cap.calls > 1);
    assert(cap.bytes > SPILL_SORT_ROWS / 2);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);
    assert(dir_has_regular_entries(spill_dir) == 0);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats, "\"spill_output_rows\":25000") != NULL);
    assert(strstr((char *)stats, "\"spill_distinct_rows\":25000") != NULL);

    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
    remove(lookup_path);
}


static void test_spill_union_streams_flush_chunks_and_cleans_up(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_spill_union_%ld", (long)getpid());
    if (mkdir(spill_dir, 0700) != 0 && errno != EEXIST) {
        assert(!"failed to create spill dir");
    }

    char lookup_path[256];
    snprintf(lookup_path, sizeof(lookup_path), "/tmp/tranfi_spill_union_lookup_%ld.csv", (long)getpid());
    FILE *lookup = fopen(lookup_path, "w");
    assert(lookup != NULL);
    fprintf(lookup, "id,score,group\n");
    for (size_t row = SPILL_SORT_ROWS / 2; row < SPILL_SORT_ROWS + SPILL_SORT_ROWS / 2; row++) {
        int score = (int)((row * 17) % 100000);
        int group = (int)(row % 97);
        fprintf(lookup, "%zu,%d,%d\n", row, score, group);
    }
    fclose(lookup);

    char json[1536];
    snprintf(json, sizeof(json),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":64}},"
             "{\"op\":\"union\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],"
             "\"spill_dir\":\"%s\",\"spill_run_rows\":1024,\"spill_output_rows\":128}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
             lookup_path, spill_dir);
    tf_pipeline *p = tf_pipeline_create(json, strlen(json));
    assert(p != NULL);

    memory_sink_capture cap = {0};
    assert(tf_pipeline_set_sink(p, TF_CHAN_MAIN, memory_counting_sink, &cap) == TF_OK);

    const char *header = "id,score,group\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);

    char chunk[8192];
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;
    for (size_t row = 0; row < SPILL_SORT_ROWS; row += CHUNK_ROWS) {
        size_t n_rows = CHUNK_ROWS;
        if (row + n_rows > SPILL_SORT_ROWS) n_rows = SPILL_SORT_ROWS - row;
        size_t len = write_rows(chunk, sizeof(chunk), row, n_rows);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        if ((row / CHUNK_ROWS) % 32 == 0) update_peak(&peak_kb);
    }

    assert(cap.calls == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    update_peak(&peak_kb);

    assert(p->rows_in == SPILL_SORT_ROWS);
    assert(p->rows_out == SPILL_SORT_ROWS + SPILL_SORT_ROWS / 2);
    assert(cap.calls > 1);
    assert(cap.bytes > SPILL_SORT_ROWS);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);
    assert(dir_has_regular_entries(spill_dir) == 0);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats, "\"spill_output_rows\":75000") != NULL);
    assert(strstr((char *)stats, "\"spill_distinct_rows\":75000") != NULL);

    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
    remove(lookup_path);
}

static void test_blocking_pivot_contract_and_flush_latency(void) {
    assert_contract_for_op("csv | pivot metric value sum | csv", "pivot",
                           TF_MEM_BLOCKING, TF_EMIT_ON_FLUSH);
    tf_pipeline *p = create_pipeline_from_dsl("csv | pivot metric value sum | csv");

    const char *header = "id,metric,value\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t prefinish_out = 0;
    for (size_t id = 0; id < PIVOT_IDS; id += CHUNK_ROWS) {
        size_t n_ids = CHUNK_ROWS;
        if (id + n_ids > PIVOT_IDS) n_ids = PIVOT_IDS - id;
        size_t len = write_pivot_rows(chunk, sizeof(chunk), id, n_ids);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        prefinish_out += drain_channel(p, TF_CHAN_MAIN);
    }

    assert(prefinish_out == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    size_t final_out = drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    assert(p->rows_in == PIVOT_IDS * 2);
    assert(p->rows_out == PIVOT_IDS);
    assert(final_out > PIVOT_IDS);

    tf_pipeline_free(p);
}



static void test_spill_pivot_streams_flush_chunks_and_cleans_up(void) {
    assert_contract_for_op("csv batch_size=1 | pivot metric value sum max_categories=2 spill_dir=/tmp/tranfi-spill | csv", "pivot",
                           TF_MEM_EXTERNAL, TF_EMIT_ON_FLUSH);
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_memory_pivot_spill_%ld", (long)getpid());
    rmdir(spill_dir);
    assert(mkdir(spill_dir, 0700) == 0);

    char dsl[1024];
    snprintf(dsl, sizeof(dsl),
             "csv batch_size=1 | pivot metric value sum max_categories=2 spill_dir=%s spill_run_rows=64 spill_output_rows=32 | csv",
             spill_dir);
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;
    tf_pipeline *p = create_pipeline_from_dsl(dsl);
    memory_sink_capture cap = {0};
    assert(tf_pipeline_set_sink(p, TF_CHAN_MAIN, memory_counting_sink, &cap) == TF_OK);

    const char *header = "id,metric,value\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);

    char chunk[8192];
    for (size_t id = 0; id < PIVOT_IDS; id += CHUNK_ROWS) {
        size_t n_ids = CHUNK_ROWS;
        if (id + n_ids > PIVOT_IDS) n_ids = PIVOT_IDS - id;
        size_t len = write_pivot_rows(chunk, sizeof(chunk), id, n_ids);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        if ((id / CHUNK_ROWS) % 4 == 0) update_peak(&peak_kb);
    }

    assert(cap.calls == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    update_peak(&peak_kb);
    assert(p->rows_in == PIVOT_IDS * 2);
    assert(p->rows_out == PIVOT_IDS);
    assert(cap.calls > 1);
    assert(cap.bytes > PIVOT_IDS);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);
    assert(dir_has_regular_entries(spill_dir) == 0);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats, "\"schema_class\":\"data_dependent\"") != NULL);
    assert(strstr((char *)stats, "\"tracked_categories\":2") != NULL);
    assert(strstr((char *)stats, "\"spill_output_rows\":1024") != NULL);
    assert(strstr((char *)stats, "\"spill_distinct_groups\":1024") != NULL);

    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
}


static void test_spill_pivot_high_category_tiny_runs_stays_bounded(void) {
    assert_contract_for_op("csv batch_size=7 | pivot metric value sum max_categories=17 spill_dir=/tmp/tranfi-spill | csv", "pivot",
                           TF_MEM_EXTERNAL, TF_EMIT_ON_FLUSH);
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_memory_pivot_spill_stress_%ld", (long)getpid());
    rmdir(spill_dir);
    assert(mkdir(spill_dir, 0700) == 0);

    char dsl[1024];
    snprintf(dsl, sizeof(dsl),
             "csv batch_size=7 | pivot metric value sum max_categories=%d spill_dir=%s spill_run_rows=128 spill_output_rows=17 | csv",
             PIVOT_STRESS_CATS, spill_dir);
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;
    tf_pipeline *p = create_pipeline_from_dsl(dsl);
    memory_sink_capture cap = {0};
    assert(tf_pipeline_set_sink(p, TF_CHAN_MAIN, memory_counting_sink, &cap) == TF_OK);

    const char *header = "id,metric,value\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);

    char chunk[65536];
    for (size_t id = 0; id < PIVOT_STRESS_IDS; id += 11) {
        size_t n_ids = 11;
        if (id + n_ids > PIVOT_STRESS_IDS) n_ids = PIVOT_STRESS_IDS - id;
        size_t len = write_pivot_stress_rows(chunk, sizeof(chunk), id, n_ids);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        if ((id / 11) % 32 == 0) update_peak(&peak_kb);
    }

    assert(cap.calls == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    update_peak(&peak_kb);
    assert(p->rows_in == PIVOT_STRESS_IDS * PIVOT_STRESS_CATS);
    assert(p->rows_out == PIVOT_STRESS_IDS);
    assert(cap.calls > 10);
    assert(cap.bytes > PIVOT_STRESS_IDS * PIVOT_STRESS_CATS);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);
    assert(dir_has_regular_entries(spill_dir) == 0);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats, "\"schema_class\":\"data_dependent\"") != NULL);
    assert(strstr((char *)stats, "\"tracked_categories\":17") != NULL);
    assert(strstr((char *)stats, "\"spill_output_rows\":4096") != NULL);
    assert(strstr((char *)stats, "\"spill_distinct_groups\":4096") != NULL);

    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
}

static void test_sorted_pivot_declared_categories_streams_current_group(void) {
    assert_contract_for_op("csv batch_size=1 | pivot metric value sum categories=a,b sorted=true | csv", "pivot",
                           TF_MEM_BOUNDED_STATE, TF_EMIT_MIXED);
    long start_kb = current_rss_kb();
    long peak_kb = start_kb;
    tf_pipeline *p = create_pipeline_from_dsl("csv batch_size=1 | pivot metric value sum categories=a,b sorted=true | csv");

    const char *header = "id,metric,value\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    assert(drain_channel(p, TF_CHAN_MAIN) == 0);

    char chunk[8192];
    size_t prefinish_out = 0;
    for (size_t id = 0; id < PIVOT_IDS; id += CHUNK_ROWS) {
        size_t n_ids = CHUNK_ROWS;
        if (id + n_ids > PIVOT_IDS) n_ids = PIVOT_IDS - id;
        size_t len = write_pivot_rows(chunk, sizeof(chunk), id, n_ids);
        assert(tf_pipeline_push(p, (const uint8_t *)chunk, len) == TF_OK);
        prefinish_out += drain_channel(p, TF_CHAN_MAIN);
        assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
        assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
        if ((id / CHUNK_ROWS) % 8 == 0) update_peak(&peak_kb);
    }

    assert(prefinish_out > PIVOT_IDS);
    assert(tf_pipeline_finish(p) == TF_OK);
    size_t final_out = drain_channel(p, TF_CHAN_MAIN);
    drain_channel(p, TF_CHAN_STATS);
    update_peak(&peak_kb);
    assert(p->rows_in == PIVOT_IDS * 2);
    assert(p->rows_out == PIVOT_IDS);
    assert(final_out > 0);
    assert(p->output[TF_CHAN_MAIN].cap <= STREAM_CAP_LIMIT);
    assert_rss_delta_bounded(start_kb, peak_kb);

    tf_pipeline_free(p);
}

int main(void) {
    printf("Tranfi Memory Regression Tests\n");
    printf("================================\n");

    test_buffer_releases_large_empty_capacity();
    printf("  buffer releases large drained capacity       PASS\n");

    test_row_local_pipeline_drains_incrementally();
    printf("  row-local pipeline drains incrementally      PASS\n");

    test_row_local_pipeline_sink_stays_drained();
    printf("  row-local pipeline sink auto-drains          PASS\n");

    test_text_jsonl_record_caps_bound_no_newline_input();
    printf("  text/jsonl record caps bound no-newline input PASS\n");

    test_bounded_flush_latent_top_stays_small();
    printf("  bounded flush-latent top stays small         PASS\n");

    test_top_heap_replacements_compact_string_storage();
    printf("  top-k heap replacements compact strings      PASS\n");

    test_tail_flush_latent_stays_small();
    printf("  bounded tail stays small                     PASS\n");

    test_lag_and_lead_offsets_stay_small();
    printf("  bounded lag/lead offsets stay small          PASS\n");

    test_window_and_rolling_aliases_stay_small();
    printf("  bounded window/rolling ops stay small        PASS\n");

    test_sample_flush_latent_stays_small();
    printf("  bounded sample stays small                   PASS\n");

    test_frequency_overflow_stays_small();
    printf("  capped frequency overflow stays small        PASS\n");

    test_onehot_declared_categories_stays_small();
    printf("  declared onehot categories stay small        PASS\n");

    test_label_encode_declared_categories_stays_small();
    printf("  declared label-encode categories stay small  PASS\n");

    test_stats_on_flush_stays_small();
    printf("  bounded stats stays small                    PASS\n");

    test_low_cardinality_unique_drains_and_stays_small();
    printf("  low-cardinality unique drains and stays small PASS\n");

    test_low_cardinality_rowid_drains_and_stays_small();
    printf("  low-cardinality rowid drains and stays small PASS\n");

    test_low_cardinality_group_agg_stays_small();
    printf("  low-cardinality group-agg stays small        PASS\n");

    test_small_lookup_join_streams_left_side();
    printf("  small lookup join streams left side          PASS\n");

    test_small_lookup_set_ops_stream_left_side();
    printf("  small lookup set ops stream left side       PASS\n");

    test_union_all_streams_appended_file();
    printf("  union-all appended file streams             PASS\n");

    test_blocking_sort_contract_and_flush_latency();
    printf("  blocking sort contract and flush latency     PASS\n");

    test_spill_sort_streams_flush_chunks_and_cleans_up();
    printf("  spill sort streams flush chunks              PASS\n");

    test_spill_unique_streams_flush_chunks_and_cleans_up();
    printf("  spill unique streams flush chunks            PASS\n");

    test_spill_group_agg_streams_flush_chunks_and_cleans_up();
    printf("  spill group-agg streams flush chunks         PASS\n");

    test_spill_filtering_join_streams_flush_chunks_and_cleans_up();
    printf("  spill filtering join streams flush chunks   PASS\n");

    test_spill_mutating_join_streams_flush_chunks_and_cleans_up();
    printf("  spill mutating join streams flush chunks    PASS\n");

    test_spill_mutating_join_tiny_runs_with_repeated_left_keys();
    printf("  spill mutating join tiny-run stress         PASS\n");

    test_spill_set_ops_streams_flush_chunks_and_cleans_up();
    printf("  spill set ops stream flush chunks           PASS\n");

    test_spill_union_streams_flush_chunks_and_cleans_up();
    printf("  spill union streams flush chunks           PASS\n");

    test_blocking_pivot_contract_and_flush_latency();
    printf("  blocking pivot contract and flush latency    PASS\n");

    test_spill_pivot_streams_flush_chunks_and_cleans_up();
    printf("  spill pivot streams flush chunks             PASS\n");

    test_spill_pivot_high_category_tiny_runs_stays_bounded();
    printf("  spill pivot high-category tiny-run stress    PASS\n");

    test_sorted_pivot_declared_categories_streams_current_group();
    printf("  sorted pivot declared categories stream      PASS\n");

    printf("\n33/33 memory regression tests passed\n");
    return 0;
}
