/*
 * test_core.c — Unit tests for the Tranfi core.
 * Simple assert-based testing. Returns non-zero on failure.
 */

#include "tranfi.h"
#include "internal.h"
#include "ir.h"
#include "dsl.h"
#include "recipes.h"
#include "date_utils.h"
#include "spill.h"
#include "report.h"
#include "cJSON.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <errno.h>
#include <dirent.h>
#ifndef _WIN32
#include <pthread.h>
#endif
#include <sys/stat.h>
#include <unistd.h>

static int tests_run = 0;
static int tests_passed = 0;
static int test_filter_count = 0;
static const char **test_filters = NULL;

static int test_should_run(const char *name) {
    if (test_filter_count == 0) return 1;
    for (int i = 0; i < test_filter_count; i++) {
        if (strcmp(name, test_filters[i]) == 0) return 1;
    }
    return 0;
}

#define TEST(name) do { \
    if (test_should_run(#name)) { \
        printf("  %-50s", #name); \
        tests_run++; \
        name(); \
        tests_passed++; \
        printf("PASS\n"); \
    } \
} while(0)

#define ASSERT_OK(expr) assert((expr) == TF_OK)

/* ================================================================
 * Arena tests
 * ================================================================ */

static void test_arena_basic(void) {
    tf_arena *a = tf_arena_create(256);
    assert(a != NULL);

    void *p1 = tf_arena_alloc(a, 64);
    assert(p1 != NULL);

    void *p2 = tf_arena_alloc(a, 128);
    assert(p2 != NULL);
    assert(p2 != p1);

    char *s = tf_arena_strdup(a, "hello world");
    assert(s != NULL);
    assert(strcmp(s, "hello world") == 0);

    tf_arena_free(a);
}

static void test_arena_large_alloc(void) {
    tf_arena *a = tf_arena_create(64);
    assert(a != NULL);

    /* Allocate larger than block size */
    void *p = tf_arena_alloc(a, 256);
    assert(p != NULL);

    tf_arena_free(a);
}

/* ================================================================
 * Buffer tests
 * ================================================================ */

static void test_buffer_basic(void) {
    tf_buffer b;
    tf_buffer_init(&b);

    const char *data = "hello world";
    assert(tf_buffer_write(&b, (const uint8_t *)data, 11) == TF_OK);
    assert(tf_buffer_readable(&b) == 11);

    uint8_t out[128];
    size_t n = tf_buffer_read(&b, out, sizeof(out));
    assert(n == 11);
    assert(memcmp(out, data, 11) == 0);
    assert(tf_buffer_readable(&b) == 0);

    tf_buffer_free(&b);
}

static void test_buffer_partial_read(void) {
    tf_buffer b;
    tf_buffer_init(&b);

    ASSERT_OK(tf_buffer_write(&b, (const uint8_t *)"abcdefgh", 8));

    uint8_t out[4];
    size_t n = tf_buffer_read(&b, out, 4);
    assert(n == 4);
    assert(memcmp(out, "abcd", 4) == 0);
    assert(tf_buffer_readable(&b) == 4);

    n = tf_buffer_read(&b, out, 4);
    assert(n == 4);
    assert(memcmp(out, "efgh", 4) == 0);
    assert(tf_buffer_readable(&b) == 0);

    tf_buffer_free(&b);
}



static void test_buffer_line_and_side_error(void) {
    tf_buffer b;
    tf_buffer_init(&b);
    assert(tf_buffer_write_line(&b, "alpha") == TF_OK);
    assert(tf_buffer_readable(&b) == 6);
    uint8_t out[128];
    size_t n = tf_buffer_read(&b, out, sizeof(out));
    assert(n == 6);
    assert(memcmp(out, "alpha\n", 6) == 0);
    tf_buffer_free(&b);

    tf_buffer json;
    tf_buffer_init(&json);
    cJSON *obj = cJSON_CreateObject();
    assert(obj != NULL);
    assert(tf_json_add_string(obj, "kind", "line") == TF_OK);
    assert(tf_json_add_number(obj, "n", 2) == TF_OK);
    assert(tf_json_add_bool(obj, "ok", 1) == TF_OK);
    assert(tf_json_add_null(obj, "empty") == TF_OK);
    assert(tf_json_add_item(obj, "nested", cJSON_CreateString("x")) == TF_OK);
    assert(tf_buffer_write_json_line(&json, obj) == TF_OK);
    cJSON_Delete(obj);
    n = tf_buffer_read(&json, out, sizeof(out));
    assert(n > 0 && n < sizeof(out));
    assert(out[n - 1] == '\n');
    out[n] = '\0';
    assert(strstr((const char *)out, "\"kind\":\"line\"") != NULL);
    assert(strstr((const char *)out, "\"ok\":true") != NULL);
    assert(strstr((const char *)out, "\"empty\":null") != NULL);
    assert(strstr((const char *)out, "\"nested\":\"x\"") != NULL);
    tf_buffer_free(&json);

    tf_buffer err;
    tf_buffer_init(&err);
    tf_side_channels side = {0};
    side.errors = &err;
    assert(tf_side_write_error(&side, "side failure") == TF_OK);
    assert(strcmp(tf_last_error(), "side failure") == 0);
    n = tf_buffer_read(&err, out, sizeof(out));
    assert(n == strlen("side failure\n"));
    assert(memcmp(out, "side failure\n", n) == 0);
    tf_buffer_free(&err);

    assert(tf_side_write_error(NULL, "no side channel") == TF_OK);
    assert(strcmp(tf_last_error(), "no side channel") == 0);
}

static void test_size_checked_arithmetic(void) {
    size_t out = 0;
    assert(tf_size_add(10, 20, &out) == TF_OK && out == 30);
    assert(tf_size_add(SIZE_MAX, 1, &out) == TF_ERROR);
    assert(tf_size_mul(12, 11, &out) == TF_OK && out == 132);
    assert(tf_size_mul((SIZE_MAX / 2) + 1, 2, &out) == TF_ERROR);
    assert(tf_size_align(9, 8, &out) == TF_OK && out == 16);
    assert(tf_size_align(SIZE_MAX - 3, 8, &out) == TF_ERROR);
    assert(tf_size_grow_pow2(8, 33, 16, &out) == TF_OK && out == 64);
    assert(tf_size_grow_pow2((SIZE_MAX / 2) + 1, SIZE_MAX, 16, &out) == TF_ERROR);
}

static void test_global_byte_caps(void) {
    size_t len = 0;
    assert(tf_check_byte_limit(TF_MAX_CELL_BYTES, TF_MAX_CELL_BYTES,
                               "test", "cell") == TF_OK);
    assert(tf_check_byte_limit(TF_MAX_CELL_BYTES + 1, TF_MAX_CELL_BYTES,
                               "test", "cell") == TF_ERROR);

    char *long_name = malloc(TF_MAX_COLUMN_NAME_BYTES + 2);
    assert(long_name != NULL);
    memset(long_name, 'a', TF_MAX_COLUMN_NAME_BYTES + 1);
    long_name[TF_MAX_COLUMN_NAME_BYTES + 1] = '\0';
    assert(tf_string_length_bounded(long_name, TF_MAX_COLUMN_NAME_BYTES,
                                    &len, "test", "column name") == TF_ERROR);

    tf_batch *b = tf_batch_create(1, 1);
    assert(b != NULL);
    assert(tf_batch_set_schema(b, 0, long_name, TF_TYPE_STRING) == TF_ERROR);
    tf_batch_free(b);

    b = tf_batch_create(1, 1);
    assert(b != NULL);
    assert(tf_batch_set_schema(b, 0, "x", TF_TYPE_STRING) == TF_OK);
    assert(tf_batch_set_string_len(b, 0, 0, "x", TF_MAX_CELL_BYTES + 1) == TF_ERROR);
    tf_batch_free(b);

    assert(tf_check_byte_limit(TF_MAX_SCHEMA_BYTES + 1, TF_MAX_SCHEMA_BYTES,
                               "test", "schema") == TF_ERROR);
    free(long_name);
}

static void test_batch_allocation_overflow_guards(void) {
    assert(tf_batch_create((SIZE_MAX / sizeof(char *)) + 1, 1) == NULL);

    tf_batch *b = tf_batch_create(1, (SIZE_MAX / sizeof(char *)) + 1);
    assert(b != NULL);
    assert(tf_batch_set_schema(b, 0, "s", TF_TYPE_STRING) == TF_ERROR);
    tf_batch_free(b);

    b = tf_batch_create(1, 1);
    assert(b != NULL);
    assert(tf_batch_set_schema(b, 0, "x", TF_TYPE_INT64) == TF_OK);
    b->capacity = (SIZE_MAX / 2) + 1;
    assert(tf_batch_ensure_capacity(b, SIZE_MAX) == TF_ERROR);
    tf_batch_free(b);
}

static void assert_pipeline_create_fails_with(const char *plan, const char *needle) {
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p == NULL);
    assert(strstr(tf_last_error(), needle) != NULL);
}

static void test_codec_size_argument_clamps(void) {
    char plan[512];
    snprintf(plan, sizeof(plan),
             "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":%zu}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
             TF_MAX_BATCH_ROWS + 1);
    assert_pipeline_create_fails_with(plan, "batch_size");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1.5}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "batch_size");

    snprintf(plan, sizeof(plan),
             "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"max_columns\":%zu}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
             TF_MAX_COLUMNS + 1);
    assert_pipeline_create_fails_with(plan, "max_columns");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"max_columns\":1.5}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "max_columns");

    snprintf(plan, sizeof(plan),
             "{\"steps\":[{\"op\":\"codec.jsonl.decode\",\"args\":{\"batch_size\":%zu}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
             TF_MAX_BATCH_ROWS + 1);
    assert_pipeline_create_fails_with(plan, "batch_size");

    snprintf(plan, sizeof(plan),
             "{\"steps\":[{\"op\":\"codec.text.decode\",\"args\":{\"batch_size\":%zu}},{\"op\":\"codec.text.encode\",\"args\":{}}]}",
             TF_MAX_BATCH_ROWS + 1);
    assert_pipeline_create_fails_with(plan, "batch_size");

    snprintf(plan, sizeof(plan),
             "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"max_record_bytes\":%zu}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
             TF_MAX_RECORD_BYTES + 1);
    assert_pipeline_create_fails_with(plan, "max_record_bytes");

    tf_pipeline *p = tf_pipeline_create(
        "{\"steps\":[{\"op\":\"codec.text.decode\",\"args\":{\"max_record_bytes\":0}},{\"op\":\"codec.text.encode\",\"args\":{}}]}",
        strlen("{\"steps\":[{\"op\":\"codec.text.decode\",\"args\":{\"max_record_bytes\":0}},{\"op\":\"codec.text.encode\",\"args\":{}}]}"));
    assert(p != NULL);
    tf_pipeline_free(p);
}

static void test_op_numeric_argument_clamps(void) {
    char plan[1024];

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"skip\":1.5}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "skip");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"head\",\"args\":{\"n\":1.5}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "n");

    snprintf(plan, sizeof(plan),
             "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
             "{\"op\":\"tail\",\"args\":{\"n\":%zu}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
             TF_MAX_OUTPUT_ROWS_PER_BATCH + 1);
    assert_pipeline_create_fails_with(plan, "n");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"lag\",\"args\":{\"column\":\"x\",\"offset\":1.5}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "offset");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"window\",\"args\":{\"column\":\"x\",\"size\":1.5,\"func\":\"sum\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "size");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"unique\",\"args\":{\"columns\":[\"x\"],\"max_keys\":1.5}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "max_keys");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"rowid\",\"args\":{\"columns\":[\"x\"],\"max_state_bytes\":1.5}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "max_state_bytes");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"join\",\"args\":{\"file\":\"lookup.csv\",\"on\":\"x\",\"max_lookup_rows\":1.5}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "max_lookup_rows");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"validate\",\"args\":{\"expr\":\"true\",\"max_failures\":1.5}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "max_failures");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"sample\",\"args\":{\"n\":1,\"seed\":1.5}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "seed");

    snprintf(plan, sizeof(plan),
             "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
             "{\"op\":\"codec.table.encode\",\"args\":{\"max_width\":%zu}}]}",
             TF_MAX_TABLE_WIDTH + 1);
    assert_pipeline_create_fails_with(plan, "max_width");
}

/* ================================================================
 * Batch tests
 * ================================================================ */

static void test_batch_create(void) {
    tf_batch *b = tf_batch_create(3, 10);
    assert(b != NULL);
    assert(b->n_cols == 3);
    assert(b->n_rows == 0);

    ASSERT_OK(tf_batch_set_schema(b, 0, "name", TF_TYPE_STRING));
    ASSERT_OK(tf_batch_set_schema(b, 1, "age", TF_TYPE_INT64));
    ASSERT_OK(tf_batch_set_schema(b, 2, "score", TF_TYPE_FLOAT64));

    assert(strcmp(b->col_names[0], "name") == 0);
    assert(b->col_types[1] == TF_TYPE_INT64);

    tf_batch_free(b);
}

static void test_batch_set_get(void) {
    tf_batch *b = tf_batch_create(3, 4);
    ASSERT_OK(tf_batch_set_schema(b, 0, "name", TF_TYPE_STRING));
    ASSERT_OK(tf_batch_set_schema(b, 1, "age", TF_TYPE_INT64));
    ASSERT_OK(tf_batch_set_schema(b, 2, "score", TF_TYPE_FLOAT64));

    ASSERT_OK(tf_batch_set_string(b, 0, 0, "Alice"));
    ASSERT_OK(tf_batch_set_int64(b, 0, 1, 30));
    ASSERT_OK(tf_batch_set_float64(b, 0, 2, 85.5));
    b->n_rows = 1;

    assert(strcmp(tf_batch_get_string(b, 0, 0), "Alice") == 0);
    assert(tf_batch_get_int64(b, 0, 1) == 30);
    assert(tf_batch_get_float64(b, 0, 2) == 85.5);
    assert(!tf_batch_is_null(b, 0, 0));

    ASSERT_OK(tf_batch_set_null(b, 0, 2));
    assert(tf_batch_is_null(b, 0, 2));

    tf_batch_free(b);
}

static void test_batch_setters_report_failures(void) {
    tf_batch *b = tf_batch_create(2, 1);
    assert(b != NULL);
    assert(tf_batch_set_schema(b, 0, "name", TF_TYPE_STRING) == TF_OK);
    assert(tf_batch_set_schema(b, 1, "age", TF_TYPE_INT64) == TF_OK);

    assert(tf_batch_set_string(b, 0, 0, "Alice") == TF_OK);
    b->n_rows = 1;
    assert(!tf_batch_is_null(b, 0, 0));
    assert(strcmp(tf_batch_get_string(b, 0, 0), "Alice") == 0);

    assert(tf_batch_set_int64(b, 0, 0, 42) == TF_ERROR);
    assert(!tf_batch_is_null(b, 0, 0));
    assert(strcmp(tf_batch_get_string(b, 0, 0), "Alice") == 0);

    assert(tf_batch_set_string(b, 1, 0, "Bob") == TF_ERROR);
    assert(tf_batch_set_string(b, 0, 0, NULL) == TF_ERROR);

    tf_batch *dst = tf_batch_create(1, 1);
    assert(dst != NULL);
    assert(tf_batch_set_schema(dst, 0, "name", TF_TYPE_INT64) == TF_OK);
    assert(tf_batch_copy_row(dst, 0, b, 0) == TF_ERROR);
    assert(dst->n_rows == 0);

    tf_batch_free(dst);
    tf_batch_free(b);
}

static void test_batch_schema_copy_helpers(void) {
    tf_batch *src = tf_batch_create(3, 2);
    assert(src != NULL);
    assert(tf_batch_set_schema(src, 0, "name", TF_TYPE_STRING) == TF_OK);
    assert(tf_batch_set_schema(src, 1, "age", TF_TYPE_INT64) == TF_OK);
    assert(tf_batch_set_schema(src, 2, "score", TF_TYPE_FLOAT64) == TF_OK);
    assert(tf_batch_set_string(src, 0, 0, "Alice") == TF_OK);
    assert(tf_batch_set_int64(src, 0, 1, 30) == TF_OK);
    assert(tf_batch_set_float64(src, 0, 2, 85.5) == TF_OK);
    src->n_rows = 1;

    tf_batch *clone = tf_batch_create(3, 1);
    assert(clone != NULL);
    assert(tf_batch_clone_schema(clone, src) == TF_OK);
    assert(strcmp(clone->col_names[0], "name") == 0);
    assert(clone->col_types[2] == TF_TYPE_FLOAT64);
    assert(tf_batch_copy_row(clone, 0, src, 0) == TF_OK);
    clone->n_rows = 1;
    assert(strcmp(tf_batch_get_string(clone, 0, 0), "Alice") == 0);
    assert(tf_batch_get_int64(clone, 0, 1) == 30);

    const char *extra_names[] = {"flag"};
    const tf_type extra_types[] = {TF_TYPE_BOOL};
    tf_batch *extra = tf_batch_create(4, 1);
    assert(extra != NULL);
    assert(tf_batch_clone_with_extra_cols(extra, src, extra_names, extra_types, 1) == TF_OK);
    assert(strcmp(extra->col_names[3], "flag") == 0);
    assert(extra->col_types[3] == TF_TYPE_BOOL);

    tf_batch *selected = tf_batch_create(2, 1);
    assert(selected != NULL);
    assert(tf_batch_set_schema(selected, 0, "score", TF_TYPE_FLOAT64) == TF_OK);
    assert(tf_batch_set_schema(selected, 1, "name", TF_TYPE_STRING) == TF_OK);
    size_t cols[] = {2, 0};
    assert(tf_batch_copy_selected_row(selected, 0, src, 0, cols, 2) == TF_OK);
    selected->n_rows = 1;
    assert(tf_batch_get_float64(selected, 0, 0) == 85.5);
    assert(strcmp(tf_batch_get_string(selected, 0, 1), "Alice") == 0);
    assert(tf_batch_copy_cell(selected, 0, 0, src, 0, 1) == TF_ERROR);
    assert(tf_batch_copy_cell(selected, 0, 0, src, 0, 2) == TF_OK);
    assert(tf_batch_get_float64(selected, 0, 0) == 85.5);
    cols[0] = 99;
    assert(tf_batch_copy_selected_row(selected, 0, src, 0, cols, 2) == TF_ERROR);

    tf_batch *fmt_src = tf_batch_create(7, 1);
    assert(fmt_src != NULL);
    assert(tf_batch_set_schema(fmt_src, 0, "flag", TF_TYPE_BOOL) == TF_OK);
    assert(tf_batch_set_schema(fmt_src, 1, "count", TF_TYPE_INT64) == TF_OK);
    assert(tf_batch_set_schema(fmt_src, 2, "ratio", TF_TYPE_FLOAT64) == TF_OK);
    assert(tf_batch_set_schema(fmt_src, 3, "label", TF_TYPE_STRING) == TF_OK);
    assert(tf_batch_set_schema(fmt_src, 4, "day", TF_TYPE_DATE) == TF_OK);
    assert(tf_batch_set_schema(fmt_src, 5, "when", TF_TYPE_TIMESTAMP) == TF_OK);
    assert(tf_batch_set_schema(fmt_src, 6, "missing", TF_TYPE_STRING) == TF_OK);
    assert(tf_batch_set_bool(fmt_src, 0, 0, true) == TF_OK);
    assert(tf_batch_set_int64(fmt_src, 0, 1, -42) == TF_OK);
    assert(tf_batch_set_float64(fmt_src, 0, 2, 0.1) == TF_OK);
    assert(tf_batch_set_string(fmt_src, 0, 3, "ok") == TF_OK);
    int32_t fmt_day = tf_date_from_ymd(2024, 3, 15);
    int64_t fmt_when = tf_timestamp_from_parts(2024, 3, 15, 12, 34, 56, 120000);
    assert(tf_batch_set_date(fmt_src, 0, 4, fmt_day) == TF_OK);
    assert(tf_batch_set_timestamp(fmt_src, 0, 5, fmt_when) == TF_OK);
    assert(tf_batch_expose_row(fmt_src, 0) == TF_OK);

    char fmt_buf[64];
    const char *fmt_text = NULL;
    assert(tf_batch_format_cell_as_string(fmt_src, 0, 4, TF_CELL_STRING_ROUNDTRIP,
                                          fmt_buf, sizeof(fmt_buf), &fmt_text) == TF_OK);
    assert(fmt_text != NULL && strcmp(fmt_text, "2024-03-15") == 0);
    assert(tf_batch_format_cell_as_string(fmt_src, 0, 6, TF_CELL_STRING_ROUNDTRIP,
                                          fmt_buf, sizeof(fmt_buf), &fmt_text) == TF_OK);
    assert(fmt_text == NULL);
    assert(tf_batch_format_cell_as_string(fmt_src, 0, 2, TF_CELL_STRING_HUMAN,
                                          fmt_buf, sizeof(fmt_buf), &fmt_text) == TF_OK);
    assert(fmt_text != NULL && strcmp(fmt_text, "0.1") == 0);
    char expected_num[64];
    snprintf(expected_num, sizeof(expected_num), "%d", (int)fmt_day);
    assert(tf_batch_format_cell_as_string(fmt_src, 0, 4, TF_CELL_STRING_NUMERIC_TIME,
                                          fmt_buf, sizeof(fmt_buf), &fmt_text) == TF_OK);
    assert(fmt_text != NULL && strcmp(fmt_text, expected_num) == 0);
    snprintf(expected_num, sizeof(expected_num), "%lld", (long long)fmt_when);
    assert(tf_batch_format_cell_as_string(fmt_src, 0, 5, TF_CELL_STRING_NUMERIC_TIME,
                                          fmt_buf, sizeof(fmt_buf), &fmt_text) == TF_OK);
    assert(fmt_text != NULL && strcmp(fmt_text, expected_num) == 0);
    assert(tf_batch_format_cell_as_string(fmt_src, 1, 0, TF_CELL_STRING_ROUNDTRIP,
                                          fmt_buf, sizeof(fmt_buf), &fmt_text) == TF_ERROR);

    tf_batch *fmt_dst = tf_batch_create(7, 1);
    assert(fmt_dst != NULL);
    for (size_t i = 0; i < 7; i++) {
        assert(tf_batch_set_schema(fmt_dst, i, fmt_src->col_names[i], TF_TYPE_STRING) == TF_OK);
        assert(tf_batch_copy_cell_as_string(fmt_dst, 0, i, fmt_src, 0, i) == TF_OK);
    }
    assert(tf_batch_expose_row(fmt_dst, 0) == TF_OK);
    assert(strcmp(tf_batch_get_string(fmt_dst, 0, 0), "true") == 0);
    assert(strcmp(tf_batch_get_string(fmt_dst, 0, 1), "-42") == 0);
    assert(strcmp(tf_batch_get_string(fmt_dst, 0, 2), "0.10000000000000001") == 0);
    assert(strcmp(tf_batch_get_string(fmt_dst, 0, 3), "ok") == 0);
    assert(strcmp(tf_batch_get_string(fmt_dst, 0, 4), "2024-03-15") == 0);
    assert(strcmp(tf_batch_get_string(fmt_dst, 0, 5), "2024-03-15T12:34:56.12Z") == 0);
    assert(tf_batch_is_null(fmt_dst, 0, 6));
    assert(tf_batch_copy_cell_as_string(fmt_src, 0, 0, fmt_dst, 0, 0) == TF_ERROR);

    tf_batch_free(fmt_dst);
    tf_batch_free(fmt_src);
    tf_batch_free(selected);
    tf_batch_free(extra);
    tf_batch_free(clone);
    tf_batch_free(src);
}

static void test_report_format_stats_csv(void) {
    const char stats[] =
        "column,count,avg,min,max,stddev,median,p25,p75,distinct,hist,sample\n"
        "age,3,25,20,30,5,25,20,30,3,\"20:30:0,1,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1\",\"20,25,30\"\n";

    char *report = tf_report_format(stats, sizeof(stats) - 1, 0);
    assert(report != NULL);
    assert(strstr(report, "1 columns") != NULL);
    assert(strstr(report, "3 rows") != NULL);
    assert(strstr(report, "age") != NULL);
    assert(strstr(report, "min") != NULL);

    free(report);
}

static void test_batch_col_index(void) {
    tf_batch *b = tf_batch_create(2, 1);
    ASSERT_OK(tf_batch_set_schema(b, 0, "foo", TF_TYPE_INT64));
    ASSERT_OK(tf_batch_set_schema(b, 1, "bar", TF_TYPE_STRING));

    assert(tf_batch_col_index(b, "foo") == 0);
    assert(tf_batch_col_index(b, "bar") == 1);
    assert(tf_batch_col_index(b, "baz") == -1);

    tf_batch_free(b);
}

/* ================================================================
 * Expression tests
 * ================================================================ */

static void test_expr_parse_simple(void) {
    tf_expr *e = tf_expr_parse("col('x') > 0");
    assert(e != NULL);
    tf_expr_free(e);
}

static void test_expr_parse_compound(void) {
    tf_expr *e = tf_expr_parse("col('age') >= 25 and col('score') < 90.0");
    assert(e != NULL);
    tf_expr_free(e);
}


static void test_expr_depth_limit(void) {
    size_t depth = 300;
    size_t inner_len = strlen("col(a)");
    char *expr = malloc(depth + inner_len + depth + 1);
    assert(expr != NULL);
    char *p = expr;
    for (size_t i = 0; i < depth; i++) *p++ = '(';
    memcpy(p, "col(a)", inner_len);
    p += inner_len;
    for (size_t i = 0; i < depth; i++) *p++ = ')';
    *p = '\0';

    tf_expr *e = tf_expr_parse(expr);
    assert(e == NULL);
    assert(tf_last_error() != NULL && strstr(tf_last_error(), "nesting too deep") != NULL);
    free(expr);
}

static void test_expr_parse_string_cmp(void) {
    tf_expr *e = tf_expr_parse("col('city') == 'London'");
    assert(e != NULL);
    tf_expr_free(e);
}

static void test_expr_eval_numeric(void) {
    tf_batch *b = tf_batch_create(1, 2);
    ASSERT_OK(tf_batch_set_schema(b, 0, "x", TF_TYPE_INT64));
    ASSERT_OK(tf_batch_set_int64(b, 0, 0, 10));
    ASSERT_OK(tf_batch_set_int64(b, 1, 0, -5));
    b->n_rows = 2;

    tf_expr *e = tf_expr_parse("col('x') > 0");
    assert(e != NULL);

    bool result;
    tf_expr_eval(e, b, 0, &result);
    assert(result == true);

    tf_expr_eval(e, b, 1, &result);
    assert(result == false);

    tf_expr_free(e);
    tf_batch_free(b);
}

static void test_expr_eval_string(void) {
    tf_batch *b = tf_batch_create(1, 2);
    ASSERT_OK(tf_batch_set_schema(b, 0, "city", TF_TYPE_STRING));
    ASSERT_OK(tf_batch_set_string(b, 0, 0, "London"));
    ASSERT_OK(tf_batch_set_string(b, 1, 0, "Paris"));
    b->n_rows = 2;

    tf_expr *e = tf_expr_parse("col('city') == 'London'");
    assert(e != NULL);

    bool result;
    tf_expr_eval(e, b, 0, &result);
    assert(result == true);

    tf_expr_eval(e, b, 1, &result);
    assert(result == false);

    tf_expr_free(e);
    tf_batch_free(b);
}

static void test_expr_eval_and_or(void) {
    tf_batch *b = tf_batch_create(2, 1);
    ASSERT_OK(tf_batch_set_schema(b, 0, "a", TF_TYPE_INT64));
    ASSERT_OK(tf_batch_set_schema(b, 1, "b", TF_TYPE_INT64));
    ASSERT_OK(tf_batch_set_int64(b, 0, 0, 10));
    ASSERT_OK(tf_batch_set_int64(b, 0, 1, 20));
    b->n_rows = 1;

    tf_expr *e1 = tf_expr_parse("col('a') > 5 and col('b') > 15");
    bool r;
    tf_expr_eval(e1, b, 0, &r);
    assert(r == true);
    tf_expr_free(e1);

    tf_expr *e2 = tf_expr_parse("col('a') > 50 or col('b') > 15");
    tf_expr_eval(e2, b, 0, &r);
    assert(r == true);
    tf_expr_free(e2);

    tf_expr *e3 = tf_expr_parse("not col('a') > 50");
    tf_expr_eval(e3, b, 0, &r);
    assert(r == true);
    tf_expr_free(e3);

    tf_batch_free(b);
}

/* ================================================================
 * Full pipeline tests
 * ================================================================ */

static void test_pipeline_csv_passthrough(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name,age\nAlice,30\nBob,25\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* Should contain header and both rows */
    assert(strstr((char *)out, "name,age") != NULL);
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);

    tf_pipeline_free(p);
}

static uint64_t double_bits(double v) {
    uint64_t bits = 0;
    memcpy(&bits, &v, sizeof(bits));
    return bits;
}

static size_t run_plan_chunked(const char *plan, const char *input, size_t chunk,
                               char *out, size_t out_cap) {
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    size_t len = strlen(input);
    for (size_t off = 0; off < len; off += chunk) {
        size_t n = len - off;
        if (n > chunk) n = chunk;
        assert(tf_pipeline_push(p, (const uint8_t *)input + off, n) == TF_OK);
    }
    assert(tf_pipeline_finish(p) == TF_OK);
    size_t total = 0;
    for (;;) {
        assert(total + 1 < out_cap);
        size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, (uint8_t *)out + total, out_cap - total - 1);
        if (n == 0) break;
        total += n;
    }
    out[total] = '\0';
    tf_pipeline_free(p);
    return total;
}

static void test_pipeline_csv_header_false(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"header\":false,\"batch_size\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    char out[1024];
    run_plan_chunked(plan, "Alice,30\nBob,25\n", 2, out, sizeof(out));
    assert(strcmp(out, "col1,col2\nAlice,30\nBob,25\n") == 0);

    const char *select_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"header\":false,\"batch_size\":1}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"col2\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    run_plan_chunked(select_plan, "Alice,30\nBob,25\n", 3, out, sizeof(out));
    assert(strcmp(out, "col2\n30\n25\n") == 0);

    const char *zero_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"header\":false,\"max_rows\":0}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    run_plan_chunked(zero_plan, "Alice,30\nBob,25\n", 4, out, sizeof(out));
    assert(strcmp(out, "col1,col2\n") == 0);
}

static void assert_csv_float_bits(const char *literal, size_t chunk) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    char input[160];
    snprintf(input, sizeof(input), "x\n%s\n", literal);
    char out[512];
    run_plan_chunked(plan, input, chunk, out, sizeof(out));
    const char *line = strchr(out, '\n');
    assert(line != NULL);
    line++;
    char emitted[128];
    size_t n = strcspn(line, "\r\n,");
    assert(n > 0 && n < sizeof(emitted));
    memcpy(emitted, line, n);
    emitted[n] = '\0';
    char *end_in = NULL;
    char *end_out = NULL;
    double expected = strtod(literal, &end_in);
    double got = strtod(emitted, &end_out);
    assert(end_in && *end_in == '\0');
    assert(end_out && *end_out == '\0');
    assert(double_bits(got) == double_bits(expected));
}

static void assert_jsonl_float_bits(const char *literal, size_t chunk) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"codec.jsonl.encode\",\"args\":{}}"
        "]}";
    char input[160];
    snprintf(input, sizeof(input), "x\n%s\n", literal);
    char out[512];
    run_plan_chunked(plan, input, chunk, out, sizeof(out));
    const char *colon = strchr(out, ':');
    assert(colon != NULL);
    char *end_in = NULL;
    char *end_out = NULL;
    double expected = strtod(literal, &end_in);
    double got = strtod(colon + 1, &end_out);
    assert(end_in && *end_in == '\0');
    assert(end_out && (*end_out == '}' || *end_out == '\n'));
    assert(double_bits(got) == double_bits(expected));
}

static void test_pipeline_float_roundtrip_bits(void) {
    const char *values[] = {
        "0.12345678901234566",
        "1.2345678901234567e+100",
        "-2.2250738585072014e-308",
        "4.9406564584124654e-324",
        "-0.0",
    };
    size_t chunks[] = {1, 2, 7, 64};
    for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
        for (size_t j = 0; j < sizeof(chunks) / sizeof(chunks[0]); j++) {
            assert_csv_float_bits(values[i], chunks[j]);
            assert_jsonl_float_bits(values[i], chunks[j]);
        }
    }
}

static void test_pipeline_table_human_float_format(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"codec.table.encode\",\"args\":{}}"
        "]}";
    char out[1024];
    run_plan_chunked(plan, "x\n0.1\n", 1, out, sizeof(out));
    assert(strstr(out, "0.1") != NULL);
    assert(strstr(out, "0.10000000000000001") == NULL);
}

typedef struct sink_capture {
    char data[4096];
    size_t len;
    int calls;
} sink_capture;

static int capture_sink(int channel, const uint8_t *data, size_t len, void *user) {
    sink_capture *cap = (sink_capture *)user;
    assert(channel == TF_CHAN_MAIN);
    assert(cap->len + len < sizeof(cap->data));
    memcpy(cap->data + cap->len, data, len);
    cap->len += len;
    cap->data[cap->len] = 0;
    cap->calls++;
    return TF_OK;
}

static int failing_sink(int channel, const uint8_t *data, size_t len, void *user) {
    (void)channel;
    (void)data;
    (void)len;
    (void)user;
    return TF_ERROR;
}

typedef struct batch_capture {
    size_t calls;
    size_t rows;
    int saw_alice;
    int saw_carol;
} batch_capture;

static int capture_batch_sink(const tf_batch *batch, void *user) {
    batch_capture *cap = (batch_capture *)user;
    cap->calls++;
    assert(tf_batch_num_cols(batch) == 2);
    assert(strcmp(tf_batch_col_name(batch, 0), "name") == 0);
    assert(strcmp(tf_batch_col_name(batch, 1), "age") == 0);
    int name_col = tf_batch_col_index(batch, "name");
    int age_col = tf_batch_col_index(batch, "age");
    assert(name_col == 0);
    assert(age_col == 1);
    assert(tf_batch_col_type(batch, (size_t)name_col) == TF_TYPE_STRING);
    assert(tf_batch_col_type(batch, (size_t)age_col) == TF_TYPE_INT64);
    for (size_t r = 0; r < tf_batch_num_rows(batch); r++) {
        assert(!tf_batch_is_null(batch, r, (size_t)name_col));
        assert(!tf_batch_is_null(batch, r, (size_t)age_col));
        const char *name = tf_batch_get_string(batch, r, (size_t)name_col);
        int64_t age = tf_batch_get_int64(batch, r, (size_t)age_col);
        if (strcmp(name, "Alice") == 0) {
            assert(age == 30);
            cap->saw_alice = 1;
        } else if (strcmp(name, "Carol") == 0) {
            assert(age == 40);
            cap->saw_carol = 1;
        } else {
            assert(0 && "unexpected filtered batch row");
        }
        cap->rows++;
    }
    return TF_OK;
}

static int failing_batch_sink(const tf_batch *batch, void *user) {
    (void)batch;
    (void)user;
    return TF_ERROR;
}

static void test_pipeline_batch_sink_callbacks(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col(age) >= 30\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    batch_capture cap = {0};
    assert(tf_pipeline_set_batch_sink(p, capture_batch_sink, &cap) == TF_OK);
    assert(tf_pipeline_set_encode_output(p, 0) == TF_OK);

    const char *csv = "name,age\nAlice,30\nBob,25\nCarol,40\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    assert(cap.calls > 0);
    assert(cap.rows == 2);
    assert(cap.saw_alice);
    assert(cap.saw_carol);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    uint8_t main_buf[16];
    assert(tf_pipeline_pull(p, TF_CHAN_MAIN, main_buf, sizeof(main_buf)) == 0);

    uint8_t stats[256];
    size_t n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = 0;
    assert(strstr((char *)stats, "\"rows_out\":2") != NULL);
    assert(strstr((char *)stats, "\"bytes_out\":0") != NULL);
    tf_pipeline_free(p);

    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_set_batch_sink(p, failing_batch_sink, NULL) == TF_OK);
    assert(tf_pipeline_set_encode_output(p, 0) == TF_OK);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_ERROR);
    assert(tf_pipeline_error(p) != NULL);
    assert(strstr(tf_pipeline_error(p), "batch sink callback failed") != NULL);
    tf_pipeline_free(p);
}

typedef struct progress_capture {
    size_t calls;
    size_t done_calls;
    size_t last_rows_in;
    size_t last_rows_out;
    size_t last_bytes_in;
    size_t last_bytes_out;
    size_t last_batches_in;
    size_t last_batches_out;
    int saw_push;
} progress_capture;

static int capture_progress(const tf_pipeline_progress *progress, void *user) {
    progress_capture *cap = (progress_capture *)user;
    assert(progress != NULL);
    assert(progress->phase != NULL);
    assert(progress->rows_in >= cap->last_rows_in);
    assert(progress->rows_out >= cap->last_rows_out);
    assert(progress->bytes_in >= cap->last_bytes_in);
    assert(progress->bytes_out >= cap->last_bytes_out);
    assert(progress->batches_in >= cap->last_batches_in);
    assert(progress->batches_out >= cap->last_batches_out);
    if (strcmp(progress->phase, "push") == 0) cap->saw_push = 1;
    if (strcmp(progress->phase, "done") == 0) {
        assert(progress->finished == 1);
        cap->done_calls++;
    }
    cap->calls++;
    cap->last_rows_in = progress->rows_in;
    cap->last_rows_out = progress->rows_out;
    cap->last_bytes_in = progress->bytes_in;
    cap->last_bytes_out = progress->bytes_out;
    cap->last_batches_in = progress->batches_in;
    cap->last_batches_out = progress->batches_out;
    return TF_OK;
}

static int failing_progress(const tf_pipeline_progress *progress, void *user) {
    (void)progress;
    (void)user;
    return TF_ERROR;
}

static void test_pipeline_progress_callback(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col(age) >= 30\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    const char *csv = "name,age\nAlice,30\nBob,25\nCarol,40\n";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    progress_capture cap = {0};
    assert(tf_pipeline_set_progress_callback(p, capture_progress, &cap, 0) == TF_OK);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    assert(cap.calls >= 2);
    assert(cap.saw_push);
    assert(cap.done_calls == 1);
    assert(cap.last_rows_in == 3);
    assert(cap.last_rows_out == 2);
    assert(cap.last_batches_in >= 3);
    assert(cap.last_batches_out == 2);
    assert(cap.last_bytes_in == strlen(csv));
    assert(cap.last_bytes_out > 0);
    tf_pipeline_free(p);

    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_set_progress_callback(p, failing_progress, NULL, 0) == TF_OK);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_ERROR);
    assert(tf_pipeline_error(p) != NULL);
    assert(strstr(tf_pipeline_error(p), "progress callback failed") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_sink_callbacks(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col(age) >= 30\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    sink_capture cap = {0};
    assert(tf_pipeline_set_sink(p, TF_CHAN_MAIN, capture_sink, &cap) == TF_OK);
    const char *csv1 = "name,age\nAlice,30\nBob,25\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv1, strlen(csv1)) == TF_OK);

    const char *csv2 = "Carol,40\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv2, strlen(csv2)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    assert(cap.calls > 0);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    uint8_t empty_main[16];
    assert(tf_pipeline_pull(p, TF_CHAN_MAIN, empty_main, sizeof(empty_main)) == 0);
    assert(strstr(cap.data, "Alice,30") != NULL);
    assert(strstr(cap.data, "Bob,25") == NULL);
    assert(strstr(cap.data, "Carol,40") != NULL);

    uint8_t stats[256];
    size_t n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = 0;
    assert(strstr((char *)stats, "\"bytes_out\":0") == NULL);
    tf_pipeline_free(p);

    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv3 = "name,age\nAlice,30\nBob,25\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv3, strlen(csv3)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) > 0);
    sink_capture manual = {0};
    assert(tf_pipeline_drain(p, TF_CHAN_MAIN, capture_sink, &manual) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    assert(strstr(manual.data, "Alice,30") != NULL);
    tf_pipeline_free(p);

    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_set_sink(p, TF_CHAN_MAIN, failing_sink, NULL) == TF_OK);
    assert(tf_pipeline_push(p, (const uint8_t *)csv1, strlen(csv1)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_ERROR);
    assert(tf_pipeline_error(p) != NULL);
    assert(strstr(tf_pipeline_error(p), "sink callback failed") != NULL);
    tf_pipeline_free(p);
}

static int write_all_fd(int fd, const char *data, size_t len) {
    const char *cursor = data;
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t n = write(fd, cursor, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        cursor += (size_t)n;
        remaining -= (size_t)n;
    }
    return 0;
}

static size_t read_all_fd(int fd, char *buf, size_t cap) {
    size_t used = 0;
    while (used + 1 < cap) {
        ssize_t n = read(fd, buf + used, cap - used - 1);
        if (n < 0) {
            if (errno == EINTR) continue;
            break;
        }
        if (n == 0) break;
        used += (size_t)n;
    }
    buf[used] = 0;
    return used;
}

static void test_pipeline_file_runner(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col(age) >= 30\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    FILE *in = tmpfile();
    FILE *out = tmpfile();
    assert(in != NULL);
    assert(out != NULL);
    fputs("name,age\nAlice,30\nBob,25\nCarol,40\n", in);
    rewind(in);

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_run_file(p, in, out, 5) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);

    rewind(out);
    char output[1024];
    size_t n = fread(output, 1, sizeof(output) - 1, out);
    output[n] = 0;
    assert(strstr(output, "name,age") != NULL);
    assert(strstr(output, "Alice,30") != NULL);
    assert(strstr(output, "Carol,40") != NULL);
    assert(strstr(output, "Bob,25") == NULL);

    uint8_t stats[256];
    n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = 0;
    assert(strstr((char *)stats, "\"rows_out\":2") != NULL);
    assert(strstr((char *)stats, "\"bytes_out\":0") == NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)"Dave,50\n", 8) == TF_ERROR);

    tf_pipeline_free(p);
    fclose(in);
    fclose(out);
}

static void test_pipeline_fd_runner(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col(age) >= 30\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    int in_pipe[2];
    int out_pipe[2];
    assert(pipe(in_pipe) == 0);
    assert(pipe(out_pipe) == 0);

    const char *input = "name,age\nAlice,30\nBob,25\nCarol,40\n";
    assert(write_all_fd(in_pipe[1], input, strlen(input)) == 0);
    assert(close(in_pipe[1]) == 0);

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_run_fd(p, in_pipe[0], out_pipe[1], 4) == TF_OK);
    assert(tf_buffer_readable(&p->output[TF_CHAN_MAIN]) == 0);
    assert(close(in_pipe[0]) == 0);
    assert(close(out_pipe[1]) == 0);

    char output[1024];
    size_t n = read_all_fd(out_pipe[0], output, sizeof(output));
    assert(n > 0);
    assert(close(out_pipe[0]) == 0);
    assert(strstr(output, "name,age") != NULL);
    assert(strstr(output, "Alice,30") != NULL);
    assert(strstr(output, "Carol,40") != NULL);
    assert(strstr(output, "Bob,25") == NULL);

    uint8_t stats[256];
    n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = 0;
    assert(strstr((char *)stats, "\"rows_out\":2") != NULL);
    assert(strstr((char *)stats, "\"bytes_out\":0") == NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)"Dave,50\n", 8) == TF_ERROR);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_filter(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col('age') > 27\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name,age,score\nAlice,30,85\nBob,25,92\nCharlie,35,78\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* Alice (30) and Charlie (35) should pass, Bob (25) should not */
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Charlie") != NULL);
    assert(strstr((char *)out, "Bob") == NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_filter_audit_side_channel(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col('age') > 25\",\"audit\":true,\"audit_limit\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name,age\nAlice,30\nBob,20\nCara,40\nDave,10\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Cara") != NULL);
    assert(strstr((char *)out, "Bob") == NULL);
    assert(strstr((char *)out, "Dave") == NULL);

    uint8_t stats[8192];
    size_t sn = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(sn > 0);
    stats[sn] = '\0';
    assert(strstr((char *)stats, "\"type\":\"audit\"") != NULL);
    assert(strstr((char *)stats, "\"op\":\"filter\"") != NULL);
    assert(strstr((char *)stats, "\"event\":\"row_dropped\"") != NULL);
    assert(strstr((char *)stats, "\"reason\":\"predicate_false\"") != NULL);
    assert(strstr((char *)stats, "\"channel\":\"audit\"") != NULL);
    assert(strstr((char *)stats, "\"row\":2") != NULL);
    assert(strstr((char *)stats, "\"Bob\"") != NULL);
    assert(strstr((char *)stats, "\"Dave\"") == NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_select(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"name\",\"score\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name,age,score\nAlice,30,85\nBob,25,92\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* Should have name and score but not age */
    assert(strstr((char *)out, "name,score") != NULL);
    assert(strstr((char *)out, "age") == NULL);

    tf_pipeline_free(p);
}



static void test_pipeline_csv_select_helpers(void) {
    const char *csv = "id,score_math,score_read,name,active\n1,90,80,Alice,true\n2,70,95,Bob,false\n";
    uint8_t out[1024];
    size_t n;

    const char *prefix_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"starts_with(score_)\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(prefix_plan, strlen(prefix_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "score_math,score_read\n", strlen("score_math,score_read\n")) == 0);
    tf_pipeline_free(p);

    const char *negative_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"!score_read\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(negative_plan, strlen(negative_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "id,score_math,name,active\n", strlen("id,score_math,name,active\n")) == 0);
    tf_pipeline_free(p);

    const char *where_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"where(numeric)\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(where_plan, strlen(where_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "id,score_math,score_read\n", strlen("id,score_math,score_read\n")) == 0);
    tf_pipeline_free(p);

    const char *matches_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"matches(^score_)\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(matches_plan, strlen(matches_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "score_math,score_read\n", strlen("score_math,score_read\n")) == 0);
    tf_pipeline_free(p);

    const char *all_of_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"all_of(score_read,name)\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(all_of_plan, strlen(all_of_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "score_read,name\n", strlen("score_read,name\n")) == 0);
    tf_pipeline_free(p);

    const char *any_of_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"any_of('missing',score_math)\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(any_of_plan, strlen(any_of_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "score_math\n", strlen("score_math\n")) == 0);
    tf_pipeline_free(p);

    const char *negative_any_of_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"!any_of(score_read,missing)\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(negative_any_of_plan, strlen(negative_any_of_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "id,score_math,name,active\n", strlen("id,score_math,name,active\n")) == 0);
    tf_pipeline_free(p);

    const char *range_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"id:score_read\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(range_plan, strlen(range_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "id,score_math,score_read\n", strlen("id,score_math,score_read\n")) == 0);
    tf_pipeline_free(p);

    const char *reverse_range_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"score_read:id\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(reverse_range_plan, strlen(reverse_range_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "score_read,score_math,id\n", strlen("score_read,score_math,id\n")) == 0);
    tf_pipeline_free(p);

    const char *negative_range_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"!id:score_read\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(negative_range_plan, strlen(negative_range_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "name,active\n", strlen("name,active\n")) == 0);
    tf_pipeline_free(p);

    const char *readd_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"id:score_read\",\"!score_math\",\"score_math\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(readd_plan, strlen(readd_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "id,score_read,score_math\n", strlen("id,score_read,score_math\n")) == 0);
    tf_pipeline_free(p);

    const char *intersection_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"starts_with(score_)&where(numeric)\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(intersection_plan, strlen(intersection_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "score_math,score_read\n", strlen("score_math,score_read\n")) == 0);
    tf_pipeline_free(p);

    const char *difference_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"starts_with(score_)&!ends_with(read)\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(difference_plan, strlen(difference_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "score_math\n", strlen("score_math\n")) == 0);
    tf_pipeline_free(p);

    const char *union_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"starts_with(score_read)|id\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(union_plan, strlen(union_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "score_read,id\n", strlen("score_read,id\n")) == 0);
    tf_pipeline_free(p);

    const char *complement_expr_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"!(id:score_read)\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(complement_expr_plan, strlen(complement_expr_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "name,active\n", strlen("name,active\n")) == 0);
    tf_pipeline_free(p);

    const char *missing_range_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"id:missing\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(missing_range_plan, strlen(missing_range_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_ERROR);
    tf_pipeline_free(p);

    const char *missing_all_of_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"all_of(score_read,missing)\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(missing_all_of_plan, strlen(missing_all_of_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_ERROR);
    tf_pipeline_free(p);
}

static void test_pipeline_csv_relocate(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"relocate\",\"args\":{\"columns\":[\"score\"],\"before\":\"age\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name,age,score\nAlice,30,85\nBob,25,92\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    assert(strstr((char *)out, "name,score,age") != NULL);
    assert(strstr((char *)out, "Alice,85,30") != NULL);
    assert(strstr((char *)out, "Bob,92,25") != NULL);

    tf_pipeline_free(p);
}


static void test_pipeline_csv_relocate_helpers(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"relocate\",\"args\":{\"columns\":[\"starts_with(score_)\"],\"after\":\"name\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "id,name,score_math,score_read,active\n1,Alice,90,80,true\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strncmp((char *)out, "id,name,score_math,score_read,active\n", strlen("id,name,score_math,score_read,active\n")) == 0);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_head(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"head\",\"args\":{\"n\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name,age\nAlice,30\nBob,25\nCharlie,35\nDiana,28\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* Should have Alice and Bob but not Charlie or Diana */
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);
    assert(strstr((char *)out, "Charlie") == NULL);
    assert(strstr((char *)out, "Diana") == NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_rename(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"rename\",\"args\":{\"mapping\":{\"name\":\"full_name\",\"age\":\"years\"}}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name,age\nAlice,30\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    assert(strstr((char *)out, "full_name") != NULL);
    assert(strstr((char *)out, "years") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_jsonl_passthrough(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{}},"
        "{\"op\":\"codec.jsonl.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *jsonl =
        "{\"name\":\"Alice\",\"age\":30}\n"
        "{\"name\":\"Bob\",\"age\":25}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);

    tf_pipeline_free(p);
}


static void test_pipeline_jsonl_malformed_default_skip(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *jsonl =
        "{\"name\":\"Alice\",\"age\":30}\n"
        "not json\n"
        "{\"name\":\"Bob\",\"age\":25}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);
    assert(strstr((char *)out, "not json") == NULL);

    uint8_t errors[128];
    size_t e = tf_pipeline_pull(p, TF_CHAN_ERRORS, errors, sizeof(errors));
    assert(e == 0);

    tf_pipeline_free(p);
}

static void test_pipeline_jsonl_malformed_warn_errors(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{\"on_error\":\"warn\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *jsonl =
        "{\"name\":\"Alice\",\"age\":30}\n"
        "not json\n"
        "[1,2]\n"
        "{\"name\":\"Bob\",\"age\":25}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);
    assert(strstr((char *)out, "not json") == NULL);

    uint8_t errors[2048];
    size_t e = tf_pipeline_pull(p, TF_CHAN_ERRORS, errors, sizeof(errors) - 1);
    assert(e > 0);
    errors[e] = '\0';
    assert(strstr((char *)errors, "jsonl_malformed") != NULL);
    assert(strstr((char *)errors, "\"action\":\"warn\"") != NULL);
    assert(strstr((char *)errors, "\"severity\":\"warning\"") != NULL);
    assert(strstr((char *)errors, "\"line\":2") != NULL);
    assert(strstr((char *)errors, "\"line\":3") != NULL);
    assert(strstr((char *)errors, "not json") != NULL);
    assert(strstr((char *)errors, "JSONL record is not an object") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_jsonl_malformed_quarantine_truncates(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{\"on_error\":\"quarantine\",\"max_error_bytes\":8}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *jsonl =
        "{\"name\":\"Alice\",\"age\":30}\n"
        "not-json-record-long\n"
        "{\"name\":\"Bob\",\"age\":25}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);

    uint8_t errors[2048];
    size_t e = tf_pipeline_pull(p, TF_CHAN_ERRORS, errors, sizeof(errors) - 1);
    assert(e > 0);
    errors[e] = '\0';
    assert(strstr((char *)errors, "\"action\":\"quarantine\"") != NULL);
    assert(strstr((char *)errors, "\"severity\":\"error\"") != NULL);
    assert(strstr((char *)errors, "\"raw_bytes\":20") != NULL);
    assert(strstr((char *)errors, "\"raw\":\"not-json\"") != NULL);
    assert(strstr((char *)errors, "\"truncated\":true") != NULL);
    assert(strstr((char *)errors, "record-long") == NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_jsonl_malformed_fail(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{\"on_error\":\"fail\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *jsonl =
        "{\"name\":\"Alice\",\"age\":30}\n"
        "not json\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_ERROR);
    const char *err = tf_pipeline_error(p);
    assert(err != NULL);
    assert(strstr(err, "jsonl decode failed at line 2") != NULL);

    uint8_t errors[1024];
    size_t e = tf_pipeline_pull(p, TF_CHAN_ERRORS, errors, sizeof(errors) - 1);
    assert(e > 0);
    errors[e] = '\0';
    assert(strstr((char *)errors, "jsonl_malformed") != NULL);
    assert(strstr((char *)errors, "\"action\":\"fail\"") != NULL);
    assert(strstr((char *)errors, "not json") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_jsonl_malformed_chunk_line_numbers(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{\"on_error\":\"warn\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *part1 = "{\"name\":\"Alice\",\"age\":30}\n{\"name\":";
    const char *part2 = "oops}\n{\"name\":\"Bob\",\"age\":25}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)part1, strlen(part1)) == TF_OK);
    assert(tf_pipeline_push(p, (const uint8_t *)part2, strlen(part2)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);

    uint8_t errors[1024];
    size_t e = tf_pipeline_pull(p, TF_CHAN_ERRORS, errors, sizeof(errors) - 1);
    assert(e > 0);
    errors[e] = '\0';
    assert(strstr((char *)errors, "\"line\":2") != NULL);
    assert(strstr((char *)errors, "oops") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_jsonl_max_record_bytes(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{\"max_record_bytes\":16,\"max_error_bytes\":6}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *part1 = "{\"id\":1}\n{\"name\":\"";
    const char *part2 = "abcdefghijklmnop\"}";
    assert(tf_pipeline_push(p, (const uint8_t *)part1, strlen(part1)) == TF_OK);
    assert(tf_pipeline_push(p, (const uint8_t *)part2, strlen(part2)) == TF_ERROR);
    const char *err = tf_pipeline_error(p);
    assert(err != NULL);
    assert(strstr(err, "jsonl record exceeds max_record_bytes at line 2") != NULL);

    uint8_t errors[1024];
    size_t e = tf_pipeline_pull(p, TF_CHAN_ERRORS, errors, sizeof(errors) - 1);
    assert(e > 0);
    errors[e] = '\0';
    assert(strstr((char *)errors, "jsonl_record_too_large") != NULL);
    assert(strstr((char *)errors, "\"action\":\"fail\"") != NULL);
    assert(strstr((char *)errors, "\"severity\":\"error\"") != NULL);
    assert(strstr((char *)errors, "\"line\":2") != NULL);
    assert(strstr((char *)errors, "\"byte_offset\":9") != NULL);
    assert(strstr((char *)errors, "\"max_record_bytes\":16") != NULL);
    assert(strstr((char *)errors, "\"observed_bytes\":17") != NULL);
    assert(strstr((char *)errors, "\"raw\":\"{\\\"name") != NULL);
    assert(strstr((char *)errors, "\"truncated\":true") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_jsonl_column_name_byte_cap(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    size_t name_len = TF_MAX_COLUMN_NAME_BYTES + 1;
    size_t input_len = strlen("{\"") + name_len + strlen("\":1}\n") + 1;
    char *json = malloc(input_len);
    assert(json != NULL);
    size_t pos = 0;
    memcpy(json + pos, "{\"", 2);
    pos += 2;
    memset(json + pos, 'a', name_len);
    pos += name_len;
    memcpy(json + pos, "\":1}\n", 6);
    pos += 5;
    json[pos] = '\0';

    assert(tf_pipeline_push(p, (const uint8_t *)json, pos) == TF_ERROR);
    const char *err = tf_pipeline_error(p);
    assert(err != NULL);
    assert(strstr(err, "column name") != NULL);
    assert(strstr(err, "exceeds maximum") != NULL);

    free(json);
    tf_pipeline_free(p);
}

static void test_pipeline_jsonl_filter(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col('age') >= 30\"}},"
        "{\"op\":\"codec.jsonl.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *jsonl =
        "{\"name\":\"Alice\",\"age\":30}\n"
        "{\"name\":\"Bob\",\"age\":25}\n"
        "{\"name\":\"Charlie\",\"age\":35}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Charlie") != NULL);
    assert(strstr((char *)out, "Bob") == NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_jsonl_group_agg_preserves_int_key(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{}},"
        "{\"op\":\"group-agg\",\"args\":{\"group_by\":[\"id\"],\"aggs\":[{\"column\":\"score\",\"func\":\"count\",\"name\":\"n\"}]}},"
        "{\"op\":\"codec.jsonl.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *jsonl =
        "{\"id\":1,\"score\":10}\n"
        "{\"id\":1,\"score\":20}\n"
        "{\"id\":2,\"score\":30}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';

    assert(p->rows_out == 2);
    assert(strstr((char *)out, "\"id\":1") != NULL);
    assert(strstr((char *)out, "\"id\":\"1\"") == NULL);
    assert(strstr((char *)out, "\"id\":2") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_jsonl_type_widening(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{\"batch_size\":4}},"
        "{\"op\":\"codec.jsonl.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *jsonl =
        "{\"id\":1,\"value\":10}\n"
        "{\"id\":2,\"value\":10.5}\n"
        "{\"id\":3,\"value\":\"unknown\"}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';

    assert(strstr((char *)out, "\"value\":\"10\"") != NULL);
    assert(strstr((char *)out, "\"value\":\"10.5\"") != NULL);
    assert(strstr((char *)out, "\"value\":\"unknown\"") != NULL);
    tf_pipeline_free(p);
}


static void test_pipeline_json_extract_text(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.text.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"json-extract\",\"args\":{\"path\":\"/user/id\",\"result\":\"user_id\",\"type\":\"int\"}},"
        "{\"op\":\"json-extract\",\"args\":{\"path\":\"$.user.name\",\"result\":\"name\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *jsonl =
        "{\"user\":{\"id\":42,\"name\":\"Ada\"}}\n"
        "{\"user\":{\"id\":7,\"name\":\"Ben\"}}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';

    assert(strstr((char *)out, "_line,user_id,name") != NULL);
    assert(strstr((char *)out, "42,Ada") != NULL);
    assert(strstr((char *)out, "7,Ben") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_jsonl_preserves_nested_json_and_extracts(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{}},"
        "{\"op\":\"json-extract\",\"args\":{\"column\":\"payload\",\"path\":\"/city\",\"result\":\"city\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *jsonl =
        "{\"id\":1,\"payload\":{\"city\":\"NY\",\"zip\":10001}}\n"
        "{\"id\":2,\"payload\":{\"city\":\"LA\",\"zip\":90001}}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';

    assert(strstr((char *)out, "id,payload,city") != NULL);
    assert(strstr((char *)out, "NY") != NULL);
    assert(strstr((char *)out, "LA") != NULL);
    assert(strstr((char *)out, "payload") != NULL);
    assert(strstr((char *)out, "zip") != NULL);
    tf_pipeline_free(p);
}


static void test_pipeline_json_filter_text(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.text.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"json-filter\",\"args\":{\"path\":\"/user/age\",\"op\":\"ge\",\"value\":\"30\",\"type\":\"float\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *jsonl =
        "{\"user\":{\"name\":\"Ada\",\"age\":42}}\n"
        "{\"user\":{\"name\":\"Ben\",\"age\":7}}\n"
        "{\"user\":{\"name\":\"Cara\",\"age\":30}}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Ada") != NULL);
    assert(strstr((char *)out, "Cara") != NULL);
    assert(strstr((char *)out, "Ben") == NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_json_filter_nested_jsonl(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{}},"
        "{\"op\":\"json-filter\",\"args\":{\"column\":\"payload\",\"path\":\"$.city\",\"op\":\"eq\",\"value\":\"NY\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *jsonl =
        "{\"id\":1,\"payload\":{\"city\":\"NY\",\"zip\":10001}}\n"
        "{\"id\":2,\"payload\":{\"city\":\"LA\",\"zip\":90001}}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "id,payload") != NULL);
    assert(strstr((char *)out, "NY") != NULL);
    assert(strstr((char *)out, "LA") == NULL);
    tf_pipeline_free(p);
}


static void test_pipeline_json_schema_filter_text(void) {
    const char *plan = "{\"steps\":[{\"op\":\"codec.text.decode\",\"args\":{\"batch_size\":1}},{\"op\":\"json-schema\",\"args\":{\"mode\":\"filter\",\"audit\":true,\"audit_limit\":1,\"schema\":{\"type\":\"object\",\"required\":[\"user\"],\"properties\":{\"user\":{\"type\":\"object\",\"required\":[\"name\",\"age\"],\"properties\":{\"name\":{\"type\":\"string\",\"minLength\":1},\"age\":{\"type\":\"integer\",\"minimum\":18}}}}}}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *jsonl =
        "{\"user\":{\"name\":\"Ada\",\"age\":42}}\n"
        "{\"user\":{\"name\":\"Ben\",\"age\":7}}\n"
        "{\"user\":{\"name\":\"Cara\"}}\n"
        "not json\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[4096];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Ada") != NULL);
    assert(strstr((char *)out, "Ben") == NULL);
    assert(strstr((char *)out, "Cara") == NULL);
    assert(strstr((char *)out, "not json") == NULL);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "\"type\":\"audit\"") != NULL);
    assert(strstr((char *)out, "\"op\":\"json-schema\"") != NULL);
    assert(strstr((char *)out, "\"reason\":\"json_schema_failed\"") != NULL);
    assert(strstr((char *)out, "\"mode\":\"filter\"") != NULL);
    assert(strstr((char *)out, "\"row\":2") != NULL);
    assert(strstr((char *)out, "schema_mismatch") != NULL);
    assert(strstr((char *)out, "Ben") != NULL);
    assert(strstr((char *)out, "Cara") == NULL);
    tf_pipeline_free(p);

    const char *bad_plan = "{\"steps\":[{\"op\":\"codec.text.decode\",\"args\":{}},{\"op\":\"json-schema\",\"args\":{\"schema\":{\"type\":\"object\",\"additionalProperties\":false}}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}";
    p = tf_pipeline_create(bad_plan, strlen(bad_plan));
    assert(p == NULL);
    assert(tf_last_error() != NULL);
}

static void test_pipeline_json_schema_annotate_nested_jsonl(void) {
    const char *plan = "{\"steps\":[{\"op\":\"codec.jsonl.decode\",\"args\":{}},{\"op\":\"json-schema\",\"args\":{\"column\":\"payload\",\"result\":\"ok\",\"schema\":{\"type\":\"object\",\"required\":[\"city\",\"zip\"],\"properties\":{\"city\":{\"type\":\"string\",\"enum\":[\"NY\",\"LA\"]},\"zip\":{\"type\":\"integer\",\"minimum\":10000}}}}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *jsonl =
        "{\"id\":1,\"payload\":{\"city\":\"NY\",\"zip\":10001}}\n"
        "{\"id\":2,\"payload\":{\"city\":7,\"zip\":90001}}\n"
        "{\"id\":3,\"payload\":{\"city\":\"SF\",\"zip\":94105}}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[4096];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "id,payload,ok") != NULL);
    assert(strstr((char *)out, "NY") != NULL);
    assert(strstr((char *)out, "true") != NULL);
    assert(strstr((char *)out, "false") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_json_schema_keyword_clamps(void) {
    const char *bad_plans[] = {
        "{\"steps\":[{\"op\":\"codec.text.decode\",\"args\":{}},{\"op\":\"json-schema\",\"args\":{\"schema\":{\"type\":\"string\",\"minLength\":1.5}}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "{\"steps\":[{\"op\":\"codec.text.decode\",\"args\":{}},{\"op\":\"json-schema\",\"args\":{\"schema\":{\"type\":\"string\",\"maxLength\":1073741825}}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "{\"steps\":[{\"op\":\"codec.text.decode\",\"args\":{}},{\"op\":\"json-schema\",\"args\":{\"schema\":{\"type\":\"array\",\"minItems\":-1}}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "{\"steps\":[{\"op\":\"codec.text.decode\",\"args\":{}},{\"op\":\"json-schema\",\"args\":{\"schema\":{\"type\":\"array\",\"maxItems\":1.5}}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "{\"steps\":[{\"op\":\"codec.text.decode\",\"args\":{}},{\"op\":\"json-schema\",\"args\":{\"schema\":{\"type\":\"number\",\"minimum\":1e309}}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
    };
    for (size_t i = 0; i < sizeof(bad_plans) / sizeof(bad_plans[0]); i++) {
        tf_pipeline *p = tf_pipeline_create(bad_plans[i], strlen(bad_plans[i]));
        assert(p == NULL);
        assert(tf_last_error() != NULL);
    }

    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.text.decode\",\"args\":{}},"
        "{\"op\":\"json-schema\",\"args\":{\"mode\":\"filter\",\"schema\":{\"type\":\"array\",\"minItems\":1,\"maxItems\":2,\"items\":{\"type\":\"integer\"}}}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *input = "[1,2]\n[]\n[1,2,3]\n";
    assert(tf_pipeline_push(p, (const uint8_t *)input, strlen(input)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "[1,2]") != NULL);
    assert(strstr((char *)out, "[]") == NULL);
    assert(strstr((char *)out, "[1,2,3]") == NULL);
    tf_pipeline_free(p);
}


static void test_pipeline_json_flatten_text(void) {
    const char *plan = "{\"steps\":[{\"op\":\"codec.text.decode\",\"args\":{\"batch_size\":1}},{\"op\":\"json-flatten\",\"args\":{\"fields\":[{\"path\":\"/user/id\",\"name\":\"user_id\",\"type\":\"int\"},{\"path\":\"$.user.name\",\"name\":\"name\",\"type\":\"string\"},{\"path\":\"/user/active\",\"name\":\"active\",\"type\":\"bool\"},{\"path\":\"/scores\",\"name\":\"scores\",\"type\":\"string\"}]}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *jsonl =
        "{\"user\":{\"id\":42,\"name\":\"Ada\",\"active\":true},\"scores\":[10,20]}\n"
        "{\"user\":{\"id\":7,\"name\":\"Ben\",\"active\":false}}\n"
        "not json\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[4096];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "_line,user_id,name,active,scores") != NULL);
    assert(strstr((char *)out, "42,Ada,true") != NULL);
    assert(strstr((char *)out, "[10,20]") != NULL);
    assert(strstr((char *)out, "7,Ben,false") != NULL);
    assert(strstr((char *)out, "not json") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_json_flatten_nested_jsonl(void) {
    const char *plan = "{\"steps\":[{\"op\":\"codec.jsonl.decode\",\"args\":{}},{\"op\":\"json-flatten\",\"args\":{\"column\":\"payload\",\"fields\":[{\"path\":\"$.city\",\"name\":\"city\"},{\"path\":\"/zip\",\"name\":\"zip\",\"type\":\"int\"}]}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *jsonl =
        "{\"id\":1,\"payload\":{\"city\":\"NY\",\"zip\":10001}}\n"
        "{\"id\":2,\"payload\":{\"city\":\"LA\",\"zip\":90001}}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[4096];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "id,payload,city,zip") != NULL);
    assert(strstr((char *)out, "NY,10001") != NULL);
    assert(strstr((char *)out, "LA,90001") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_group_agg_hash_key_collision_regression(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"group-agg\",\"args\":{\"group_by\":[\"k1\",\"k2\"],\"aggs\":[{\"column\":\"v\",\"func\":\"count\",\"name\":\"n\"}]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv =
        "k1,k2,v\n"
        "A" "\x01" "B,C,1\n"
        "A,B" "\x01" "C,2\n"
        "A" "\x01" "B,C,3\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';

    assert(p->rows_out == 2);
    assert(strstr((char *)out, "A" "\x01" "B,C,2") != NULL);
    assert(strstr((char *)out, "A,B" "\x01" "C,1") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_group_agg_hash_rehashes_many_groups(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"group-agg\",\"args\":{\"group_by\":[\"g\"],\"aggs\":[{\"column\":\"v\",\"func\":\"sum\",\"name\":\"total\"}]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *header = "g,v\n";
    assert(tf_pipeline_push(p, (const uint8_t *)header, strlen(header)) == TF_OK);
    char row[64];
    for (int i = 0; i < 300; i++) {
        int len = snprintf(row, sizeof(row), "g%03d,%d\n", i, i);
        assert(len > 0 && (size_t)len < sizeof(row));
        assert(tf_pipeline_push(p, (const uint8_t *)row, (size_t)len) == TF_OK);
    }
    assert(tf_pipeline_finish(p) == TF_OK);
    assert(p->rows_out == 300);

    uint8_t out[4096];
    while (tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out)) > 0) {}
    tf_pipeline_free(p);
}

static void test_pipeline_group_agg_sorted_streams_runs(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"group-agg\",\"args\":{\"group_by\":[\"city\"],\"aggs\":[{\"column\":\"sales\",\"func\":\"sum\",\"name\":\"total\"},{\"column\":\"sales\",\"func\":\"count\",\"name\":\"n\"}],\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    uint8_t out[1024];
    const char *a_rows = "city,sales\nA,1\nA,2\n";
    assert(tf_pipeline_push(p, (const uint8_t *)a_rows, strlen(a_rows)) == TF_OK);
    assert(tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out)) == 0);

    const char *b_rows = "B,10\nB,5\n";
    assert(tf_pipeline_push(p, (const uint8_t *)b_rows, strlen(b_rows)) == TF_OK);
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "city,total,n") != NULL);
    assert(strstr((char *)out, "A,3") != NULL);
    assert(strstr((char *)out, "B,15") == NULL);

    const char *a_again = "A,7\n";
    assert(tf_pipeline_push(p, (const uint8_t *)a_again, strlen(a_again)) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "B,15") != NULL);

    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "A,7") != NULL);

    tf_pipeline_free(p);
}

static void assert_group_agg_null_semantics_output(const char *out) {
    assert(strstr(out, "city,total,avg,n,rows,min,max") != NULL);
    assert(strstr(out, "A,15,7.5,2,3,5,10") != NULL);
    assert(strstr(out, "B,7,7,1,2,7,7") != NULL);
    assert(strstr(out, "C,,,0,1,,") != NULL);
}

static void test_pipeline_group_agg_null_semantics_unsorted(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"group-agg\",\"args\":{\"group_by\":[\"city\"],\"aggs\":["
        "{\"column\":\"amount\",\"func\":\"sum\",\"name\":\"total\"},"
        "{\"column\":\"amount\",\"func\":\"avg\",\"name\":\"avg\"},"
        "{\"column\":\"amount\",\"func\":\"count\",\"name\":\"n\"},"
        "{\"column\":\"*\",\"func\":\"count\",\"name\":\"rows\"},"
        "{\"column\":\"amount\",\"func\":\"min\",\"name\":\"min\"},"
        "{\"column\":\"amount\",\"func\":\"max\",\"name\":\"max\"}]}} ,"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *jsonl =
        "{\"city\":\"A\",\"amount\":10}\n"
        "{\"city\":\"A\",\"amount\":null}\n"
        "{\"city\":\"A\",\"amount\":5}\n"
        "{\"city\":\"B\",\"amount\":null}\n"
        "{\"city\":\"B\",\"amount\":7}\n"
        "{\"city\":\"C\",\"amount\":null}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[4096];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert_group_agg_null_semantics_output((const char *)out);
    tf_pipeline_free(p);
}

static void test_pipeline_group_agg_null_semantics_sorted(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"group-agg\",\"args\":{\"group_by\":[\"city\"],\"sorted\":true,\"aggs\":["
        "{\"column\":\"amount\",\"func\":\"sum\",\"name\":\"total\"},"
        "{\"column\":\"amount\",\"func\":\"avg\",\"name\":\"avg\"},"
        "{\"column\":\"amount\",\"func\":\"count\",\"name\":\"n\"},"
        "{\"column\":\"*\",\"func\":\"count\",\"name\":\"rows\"},"
        "{\"column\":\"amount\",\"func\":\"min\",\"name\":\"min\"},"
        "{\"column\":\"amount\",\"func\":\"max\",\"name\":\"max\"}]}} ,"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *part1 =
        "{\"city\":\"A\",\"amount\":10}\n"
        "{\"city\":\"A\",\"amount\":null}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)part1, strlen(part1)) == TF_OK);

    const char *part2 =
        "{\"city\":\"A\",\"amount\":5}\n"
        "{\"city\":\"B\",\"amount\":null}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)part2, strlen(part2)) == TF_OK);

    uint8_t out[4096];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "city,total,avg,n,rows,min,max") != NULL);
    assert(strstr((char *)out, "A,15,7.5,2,3,5,10") != NULL);
    assert(strstr((char *)out, "B,7") == NULL);

    const char *part3 =
        "{\"city\":\"B\",\"amount\":7}\n"
        "{\"city\":\"C\",\"amount\":null}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)part3, strlen(part3)) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "B,7,7,1,2,7,7") != NULL);

    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "C,,,0,1,,") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_text_passthrough(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.text.decode\",\"args\":{}},"
        "{\"op\":\"codec.text.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *text = "hello world\nfoo bar\nbaz\n";
    assert(tf_pipeline_push(p, (const uint8_t *)text, strlen(text)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    assert(strstr((char *)out, "hello world") != NULL);
    assert(strstr((char *)out, "foo bar") != NULL);
    assert(strstr((char *)out, "baz") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_text_head(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.text.decode\",\"args\":{}},"
        "{\"op\":\"head\",\"args\":{\"n\":2}},"
        "{\"op\":\"codec.text.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *text = "line1\nline2\nline3\nline4\nline5\n";
    assert(tf_pipeline_push(p, (const uint8_t *)text, strlen(text)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    assert(strstr((char *)out, "line1") != NULL);
    assert(strstr((char *)out, "line2") != NULL);
    assert(strstr((char *)out, "line3") == NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_text_grep(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.text.decode\",\"args\":{}},"
        "{\"op\":\"grep\",\"args\":{\"pattern\":\"error\"}},"
        "{\"op\":\"codec.text.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *text = "info: started\nerror: something failed\ninfo: done\nerror: another\n";
    assert(tf_pipeline_push(p, (const uint8_t *)text, strlen(text)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    assert(strstr((char *)out, "error: something failed") != NULL);
    assert(strstr((char *)out, "error: another") != NULL);
    assert(strstr((char *)out, "info: started") == NULL);
    assert(strstr((char *)out, "info: done") == NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_text_grep_invert(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.text.decode\",\"args\":{}},"
        "{\"op\":\"grep\",\"args\":{\"pattern\":\"error\",\"invert\":true}},"
        "{\"op\":\"codec.text.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *text = "info: started\nerror: something failed\ninfo: done\n";
    assert(tf_pipeline_push(p, (const uint8_t *)text, strlen(text)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    assert(strstr((char *)out, "info: started") != NULL);
    assert(strstr((char *)out, "info: done") != NULL);
    assert(strstr((char *)out, "error") == NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_text_grep_regex(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.text.decode\",\"args\":{}},"
        "{\"op\":\"grep\",\"args\":{\"pattern\":\"^error:.*fail\",\"regex\":true}},"
        "{\"op\":\"codec.text.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *text = "info: started\nerror: something failed\ninfo: done\nerror: timeout\n";
    assert(tf_pipeline_push(p, (const uint8_t *)text, strlen(text)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* Only "error: something failed" matches ^error:.*fail */
    assert(strstr((char *)out, "error: something failed") != NULL);
    assert(strstr((char *)out, "error: timeout") == NULL);
    assert(strstr((char *)out, "info") == NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_text_max_record_bytes(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.text.decode\",\"args\":{\"max_record_bytes\":8,\"max_error_bytes\":5}},"
        "{\"op\":\"codec.text.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *part1 = "ok\n1234";
    const char *part2 = "56789";
    assert(tf_pipeline_push(p, (const uint8_t *)part1, strlen(part1)) == TF_OK);
    assert(tf_pipeline_push(p, (const uint8_t *)part2, strlen(part2)) == TF_ERROR);
    const char *err = tf_pipeline_error(p);
    assert(err != NULL);
    assert(strstr(err, "text record exceeds max_record_bytes at line 2") != NULL);

    uint8_t errors[1024];
    size_t e = tf_pipeline_pull(p, TF_CHAN_ERRORS, errors, sizeof(errors) - 1);
    assert(e > 0);
    errors[e] = '\0';
    assert(strstr((char *)errors, "text_record_too_large") != NULL);
    assert(strstr((char *)errors, "\"action\":\"fail\"") != NULL);
    assert(strstr((char *)errors, "\"severity\":\"error\"") != NULL);
    assert(strstr((char *)errors, "\"line\":2") != NULL);
    assert(strstr((char *)errors, "\"byte_offset\":3") != NULL);
    assert(strstr((char *)errors, "\"max_record_bytes\":8") != NULL);
    assert(strstr((char *)errors, "\"observed_bytes\":9") != NULL);
    assert(strstr((char *)errors, "\"raw\":\"12345\"") != NULL);
    assert(strstr((char *)errors, "\"truncated\":true") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_replace_regex(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"replace\",\"args\":{\"column\":\"name\",\"pattern\":\"A.*e\",\"replacement\":\"X\",\"regex\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name\nAlice\nBob\nAnne\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* Alice -> X (A.*e matches "Alice"), Anne -> X (A.*e matches "Anne"), Bob unchanged */
    assert(strstr((char *)out, "Bob") != NULL);
    /* Alice and Anne should be replaced with X */
    assert(strstr((char *)out, "Alice") == NULL);
    assert(strstr((char *)out, "Anne") == NULL);
    tf_pipeline_free(p);
}

static void test_dsl_grep_regex(void) {
    /* Test DSL parsing: grep -r "pattern" */
    const char *dsl1 = "text | grep -r \"^error\" | text";
    tf_ir_plan *plan = tf_dsl_parse(dsl1, strlen(dsl1), NULL);
    assert(plan != NULL);
    assert(plan->n_nodes == 3);
    /* Check that regex flag is set */
    cJSON *args = plan->nodes[1].args;
    cJSON *regex = cJSON_GetObjectItemCaseSensitive(args, "regex");
    assert(cJSON_IsTrue(regex));
    cJSON *pattern = cJSON_GetObjectItemCaseSensitive(args, "pattern");
    assert(strcmp(pattern->valuestring, "^error") == 0);
    tf_ir_plan_free(plan);

    /* Test -rv combined flag */
    const char *dsl2 = "text | grep -rv \"debug\" | text";
    plan = tf_dsl_parse(dsl2, strlen(dsl2), NULL);
    assert(plan != NULL);
    assert(plan->n_nodes == 3);
    args = plan->nodes[1].args;
    assert(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(args, "regex")));
    assert(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(args, "invert")));
    tf_ir_plan_free(plan);
}

static void test_dsl_replace_regex(void) {
    /* Test DSL parsing: replace --regex column pattern replacement */
    const char *dsl1 = "csv | replace --regex name \"A.*e\" X | csv";
    tf_ir_plan *plan = tf_dsl_parse(dsl1, strlen(dsl1), NULL);
    assert(plan != NULL);
    assert(plan->n_nodes == 3);
    cJSON *args = plan->nodes[1].args;
    cJSON *regex = cJSON_GetObjectItemCaseSensitive(args, "regex");
    assert(cJSON_IsTrue(regex));
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(args, "column")->valuestring, "name") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(args, "pattern")->valuestring, "A.*e") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(args, "replacement")->valuestring, "X") == 0);
    tf_ir_plan_free(plan);

    const char *dsl2 = "csv | replace name Alice Alicia audit audit_limit=4 | csv";
    plan = tf_dsl_parse(dsl2, strlen(dsl2), NULL);
    assert(plan != NULL);
    args = plan->nodes[1].args;
    assert(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(args, "audit")));
    assert(cJSON_GetObjectItemCaseSensitive(args, "audit_limit")->valueint == 4);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(args, "pattern")->valuestring, "Alice") == 0);
    tf_ir_plan_free(plan);
}

static void test_pipeline_stats_channel(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "x\n1\n2\n3\n";
    tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv));
    tf_pipeline_finish(p);

    uint8_t stats[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats));
    assert(n > 0);
    stats[n] = '\0';
    assert(strstr((char *)stats, "rows_in") != NULL);
    assert(strstr((char *)stats, "\"type\":\"step_stats\"") != NULL);
    assert(strstr((char *)stats, "\"steps\":[]") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_finish_step_flush_boundaries(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"top\",\"args\":{\"n\":2,\"column\":\"score\",\"desc\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name,score\nA,1\nB,4\nC,2\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);

    uint8_t early[128];
    assert(tf_pipeline_pull(p, TF_CHAN_MAIN, early, sizeof(early)) == 0);

    char out[512];
    size_t out_len = 0;
    int saw_output_before_done = 0;
    size_t calls = 0;
    for (;;) {
        int rc = tf_pipeline_finish_step(p);
        assert(rc == TF_OK || rc == TF_DONE);
        calls++;

        uint8_t chunk[64];
        size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, chunk, sizeof(chunk));
        if (n > 0 && rc != TF_DONE) saw_output_before_done = 1;
        assert(out_len + n < sizeof(out));
        memcpy(out + out_len, chunk, n);
        out_len += n;

        if (rc == TF_DONE) break;
        assert(calls < 20);
    }
    out[out_len] = '\0';

    assert(saw_output_before_done);
    assert(strstr(out, "name,score") != NULL);
    assert(strstr(out, "B,4") != NULL);
    assert(strstr(out, "C,2") != NULL);
    assert(strstr(out, "A,1") == NULL);

    uint8_t stats[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = '\0';
    assert(strstr((char *)stats, "rows_in") != NULL);
    assert(strstr((char *)stats, "\"type\":\"step_stats\"") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_step_stats_channel(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col('x') > 1\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "x\n1\n2\n3\n";
    tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv));
    tf_pipeline_finish(p);

    uint8_t stats[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = '\0';
    assert(strstr((char *)stats, "\"type\":\"step_stats\"") != NULL);
    assert(strstr((char *)stats, "\"op\":\"filter\"") != NULL);
    assert(strstr((char *)stats, "\"execution_target\":\"native\"") != NULL);
    assert(strstr((char *)stats, "\"memory_class\":\"row_local\"") != NULL);
    assert(strstr((char *)stats, "\"emit_class\":\"per_batch\"") != NULL);
    assert(strstr((char *)stats, "\"state_estimate\":\"O(batch_rows * columns)\"") != NULL);
    assert(strstr((char *)stats, "\"state_bytes_estimate\":null") != NULL);
    assert(strstr((char *)stats, "\"warnings\":[]") != NULL);
    assert(strstr((char *)stats, "\"batches_in\":1") != NULL);
    assert(strstr((char *)stats, "\"batches_out\":1") != NULL);
    assert(strstr((char *)stats, "\"rows_in\":3") != NULL);
    assert(strstr((char *)stats, "\"rows_out\":2") != NULL);

    tf_pipeline_free(p);
}


static void test_pipeline_step_stats_state_bytes(void) {
    const char *bounded_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"unique\",\"args\":{\"columns\":[\"city\"],\"max_keys\":3}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(bounded_plan, strlen(bounded_plan));
    assert(p != NULL);
    const char *csv = "city\nNY\nLA\nNY\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t stats[4096];
    size_t n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = '\0';
    assert(strstr((char *)stats, "\"op\":\"unique\"") != NULL);
    assert(strstr((char *)stats, "\"state_bytes_estimate\":1888") != NULL);
    assert(strstr((char *)stats, "state_bytes_reason") == NULL);
    assert(strstr((char *)stats, "\"warnings\":[]") != NULL);
    assert(strstr((char *)stats, "\"tracked_keys\":2") != NULL);
    assert(strstr((char *)stats, "\"tracked_key_bytes\":14") != NULL);
    assert(strstr((char *)stats, "\"retained_state_bytes\":") != NULL);
    tf_pipeline_free(p);

    const char *uncapped_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"unique\",\"args\":{\"columns\":[\"city\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    p = tf_pipeline_create(uncapped_plan, strlen(uncapped_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = '\0';
    assert(strstr((char *)stats, "\"state_bytes_estimate\":null") != NULL);
    assert(strstr((char *)stats, "\"state_bytes_reason\":\"step 'unique' needs max_keys") != NULL);
    assert(strstr((char *)stats, "\"warnings\":[\"unbounded_state\"]") != NULL);
    assert(strstr((char *)stats, "\"tracked_keys\":2") != NULL);
    assert(strstr((char *)stats, "\"tracked_key_bytes\":14") != NULL);
    assert(strstr((char *)stats, "\"retained_state_bytes\":") != NULL);
    tf_pipeline_free(p);

    const char *group_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"group-agg\",\"args\":{\"group_by\":[\"city\"],\"aggs\":[{\"column\":\"sales\",\"func\":\"sum\",\"name\":\"total\"}],\"max_groups\":3}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    p = tf_pipeline_create(group_plan, strlen(group_plan));
    assert(p != NULL);
    const char *group_csv = "city,sales\nNY,1\nLA,2\nNY,3\n";
    assert(tf_pipeline_push(p, (const uint8_t *)group_csv, strlen(group_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = '\0';
    assert(strstr((char *)stats, "\"op\":\"group-agg\"") != NULL);
    assert(strstr((char *)stats, "\"tracked_groups\":2") != NULL);
    assert(strstr((char *)stats, "\"tracked_key_bytes\":14") != NULL);
    assert(strstr((char *)stats, "\"retained_state_bytes\":") != NULL);
    tf_pipeline_free(p);

    const char *rowid_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"rowid\",\"args\":{\"columns\":[\"city\"],\"result\":\"city_row\",\"max_keys\":3}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    p = tf_pipeline_create(rowid_plan, strlen(rowid_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = '\0';
    assert(strstr((char *)stats, "\"op\":\"rowid\"") != NULL);
    assert(strstr((char *)stats, "\"tracked_keys\":2") != NULL);
    assert(strstr((char *)stats, "\"tracked_key_bytes\":26") != NULL);
    assert(strstr((char *)stats, "\"retained_state_bytes\":") != NULL);
    tf_pipeline_free(p);

    const char *onehot_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"onehot\",\"args\":{\"column\":\"city\",\"max_categories\":3}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    p = tf_pipeline_create(onehot_plan, strlen(onehot_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = '\0';
    assert(strstr((char *)stats, "\"op\":\"onehot\"") != NULL);
    assert(strstr((char *)stats, "\"tracked_categories\":2") != NULL);
    assert(strstr((char *)stats, "\"category_value_bytes\":6") != NULL);
    assert(strstr((char *)stats, "\"category_output_name_bytes\":16") != NULL);
    assert(strstr((char *)stats, "\"retained_state_bytes\":") != NULL);
    assert(strstr((char *)stats, "\"max_state_bytes\":0") != NULL);
    tf_pipeline_free(p);

    const char *label_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"label-encode\",\"args\":{\"column\":\"city\",\"result\":\"city_id\",\"max_categories\":3}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    p = tf_pipeline_create(label_plan, strlen(label_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = '\0';
    assert(strstr((char *)stats, "\"op\":\"label-encode\"") != NULL);
    assert(strstr((char *)stats, "\"tracked_categories\":2") != NULL);
    assert(strstr((char *)stats, "\"category_value_bytes\":6") != NULL);
    assert(strstr((char *)stats, "\"retained_state_bytes\":") != NULL);
    assert(strstr((char *)stats, "\"max_state_bytes\":0") != NULL);
    tf_pipeline_free(p);

    const char *sort_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"sort\",\"args\":{\"columns\":[{\"name\":\"city\",\"desc\":false}]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    p = tf_pipeline_create(sort_plan, strlen(sort_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = '\0';
    assert(strstr((char *)stats, "\"op\":\"sort\"") != NULL);
    assert(strstr((char *)stats, "\"warnings\":[\"blocking\",\"flush_latent\",\"unbounded_state\"]") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_error_handling(void) {
    /* Bad JSON */
    tf_pipeline *p = tf_pipeline_create("not json", 8);
    assert(p == NULL);
    assert(tf_last_error() != NULL);

    /* Missing decoder */
    const char *plan = "{\"steps\":[{\"op\":\"codec.csv.encode\",\"args\":{}}]}";
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p == NULL);
}


#ifndef _WIN32
typedef struct {
    int id;
    int failures;
} error_thread_arg;

static void *error_thread_worker(void *argp) {
    error_thread_arg *arg = (error_thread_arg *)argp;
    char expected[128];
    char bad_json[128];

    for (int i = 0; i < 250; i++) {
        snprintf(expected, sizeof(expected), "thread-%d-iter-%d", arg->id, i);
        tf_set_last_error(expected);
        const char *last = tf_last_error();
        if (!last || strcmp(last, expected) != 0) {
            arg->failures++;
            return NULL;
        }

        snprintf(bad_json, sizeof(bad_json), "{\"steps\":[{\"op\":%d}]}", arg->id * 1000 + i);
        char *error = NULL;
        tf_ir_plan *plan = tf_ir_from_json(bad_json, strlen(bad_json), &error);
        if (plan != NULL) {
            tf_ir_plan_free(plan);
            free(error);
            arg->failures++;
            return NULL;
        }
        if (!error || strstr(error, "missing 'op' string") == NULL) {
            free(error);
            arg->failures++;
            return NULL;
        }
        free(error);

        last = tf_last_error();
        if (!last || strcmp(last, expected) != 0) {
            arg->failures++;
            return NULL;
        }

        char *dsl_error = NULL;
        char *json = tf_compile_dsl("csv | unknown-op | csv", strlen("csv | unknown-op | csv"), &dsl_error);
        if (json != NULL) {
            tf_string_free(json);
            free(dsl_error);
            arg->failures++;
            return NULL;
        }
        if (!dsl_error || strstr(dsl_error, "unknown op") == NULL) {
            free(dsl_error);
            arg->failures++;
            return NULL;
        }
        free(dsl_error);

        last = tf_last_error();
        if (!last || strcmp(last, expected) != 0) {
            arg->failures++;
            return NULL;
        }
    }

    return NULL;
}

static void test_thread_local_last_error(void) {
    enum { N_THREADS = 8 };
    pthread_t threads[N_THREADS];
    error_thread_arg args[N_THREADS];

    tf_set_last_error("main-thread-error");
    for (int i = 0; i < N_THREADS; i++) {
        args[i].id = i;
        args[i].failures = 0;
        assert(pthread_create(&threads[i], NULL, error_thread_worker, &args[i]) == 0);
    }
    for (int i = 0; i < N_THREADS; i++) {
        assert(pthread_join(threads[i], NULL) == 0);
        assert(args[i].failures == 0);
    }

    const char *last = tf_last_error();
    assert(last != NULL && strcmp(last, "main-thread-error") == 0);
    tf_set_last_error(NULL);
    assert(tf_last_error() == NULL);
}
#else
static void test_thread_local_last_error(void) {
    tf_set_last_error("error");
    assert(tf_last_error() != NULL && strcmp(tf_last_error(), "error") == 0);
    tf_set_last_error(NULL);
    assert(tf_last_error() == NULL);
}
#endif

static void assert_push_cap_error(const char *plan, const char *csv, const char *expected) {
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_ERROR);
    const char *err = tf_pipeline_error(p);
    assert(err != NULL);
    assert(strstr(err, expected) != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_key_state_caps(void) {
    const char *unique_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"unique\",\"args\":{\"columns\":[\"city\"],\"max_keys\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(unique_plan, "city\nNY\nLA\n", "max_keys=1");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"unique\",\"args\":{\"columns\":{\"city\":true}}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "unique: columns must be an array");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"unique\",\"args\":{\"columns\":[123]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "unique: column names must be non-empty strings");

    const char *unique_missing_col_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"unique\",\"args\":{\"columns\":[\"missing\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(unique_missing_col_plan, "city\nNY\n", "unique: column 'missing' not found");

    const char *unique_byte_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"unique\",\"args\":{\"columns\":[\"city\"],\"max_state_bytes\":2048}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(unique_byte_plan, "city\nNY\n", "max_state_bytes=2048");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"unique\",\"args\":{\"columns\":[\"city\"],\"mode\":\"approx\",\"max_keys\":10}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "mode=approx uses bloom_bytes");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"unique\",\"args\":{\"columns\":[\"city\"],\"mode\":\"approx\",\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "mode=approx and sorted=true");

    const char *rowid_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"rowid\",\"args\":{\"columns\":[\"city\"],\"max_keys\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(rowid_plan, "city\nNY\nLA\n", "max_keys=1");

    const char *rowid_byte_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"rowid\",\"args\":{\"columns\":[\"city\"],\"max_state_bytes\":1024}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(rowid_byte_plan, "city\nNY\n", "max_state_bytes=1024");

    const char *unique_dupes_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"unique\",\"args\":{\"columns\":[\"city\"],\"max_keys\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(unique_dupes_plan, strlen(unique_dupes_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)"city\nNY\nNY\n", strlen("city\nNY\nNY\n")) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    tf_pipeline_free(p);

    const char *group_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"group-agg\",\"args\":{\"group_by\":[\"city\"],\"aggs\":[{\"column\":\"sales\",\"func\":\"sum\",\"name\":\"total\"}],\"max_groups\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(group_plan, "city,sales\nNY,1\nLA,2\n", "max_groups=1");

    const char *group_byte_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"group-agg\",\"args\":{\"group_by\":[\"city\"],\"aggs\":[{\"column\":\"sales\",\"func\":\"sum\",\"name\":\"total\"}],\"max_state_bytes\":4096}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(group_byte_plan, "city,sales\nNY,1\n", "max_state_bytes=4096");

    const char *freq_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"frequency\",\"args\":{\"columns\":[\"city\"],\"max_values\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(freq_plan, "city\nNY\nLA\n", "max_values=1");

    const char *freq_byte_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"frequency\",\"args\":{\"columns\":[\"city\"],\"max_state_bytes\":1024}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(freq_byte_plan, "city\nNY\n", "max_state_bytes=1024");

    const char *onehot_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"onehot\",\"args\":{\"column\":\"color\",\"max_categories\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(onehot_plan, "color\nred\nblue\n", "max_categories=1");

    const char *onehot_byte_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"onehot\",\"args\":{\"column\":\"color\",\"max_state_bytes\":128}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(onehot_byte_plan, "color\nred\n", "max_state_bytes=128");

    const char *label_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"label-encode\",\"args\":{\"column\":\"city\",\"max_categories\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(label_plan, "city\nNY\nLA\n", "max_categories=1");

    const char *label_byte_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"label-encode\",\"args\":{\"column\":\"city\",\"max_state_bytes\":128}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(label_byte_plan, "city\nNY\n", "max_state_bytes=128");
}

static void test_version(void) {
    const char *v = tf_version();
    assert(v != NULL);
    assert(strlen(v) > 0);
}

static void test_pipeline_combined(void) {
    /* CSV decode → filter → select → rename → head → CSV encode */
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col('age') > 25\"}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"name\",\"age\"]}},"
        "{\"op\":\"rename\",\"args\":{\"mapping\":{\"name\":\"person\"}}},"
        "{\"op\":\"head\",\"args\":{\"n\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv =
        "name,age,score\n"
        "Alice,30,85\n"
        "Bob,25,92\n"
        "Charlie,35,78\n"
        "Diana,28,95\n"
        "Eve,42,88\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* Should have: person,age header; Alice(30), Charlie(35); not Bob(25) */
    assert(strstr((char *)out, "person,age") != NULL);
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") == NULL);
    /* Head(2) means max 2 rows after filter: Alice, Charlie */
    /* Diana and Eve are filtered in (>25) but head limits to 2 */

    tf_pipeline_free(p);
}

static void test_pipeline_csv_null_literals(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2,\"nulls\":[\"NA\",\"NULL\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv =
        "name,score,note\n"
        "A,10,ok\n"
        "B,NA,NA\n"
        "C,NULL,NULL\n"
        "D,5,\"NA\"\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "name,score,note") != NULL);
    assert(strstr((char *)out, "A,10,ok") != NULL);
    assert(strstr((char *)out, "B,,") != NULL);
    assert(strstr((char *)out, "C,,") != NULL);
    assert(strstr((char *)out, "D,5,") != NULL);
    assert(strstr((char *)out, "NA") == NULL);
    assert(strstr((char *)out, "NULL") == NULL);
    tf_pipeline_free(p);
}



static void test_pipeline_csv_quoted_newline_and_escaped_quote_chunks(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *part1 = "id,note\n1,\"hello \"";
    const char *part2 = "\"world\nline\"\n2,ok\n";
    assert(tf_pipeline_push(p, (const uint8_t *)part1, strlen(part1)) == TF_OK);
    assert(tf_pipeline_push(p, (const uint8_t *)part2, strlen(part2)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "id,note") != NULL);
    assert(strstr((char *)out, "1,\"hello \"\"world\nline\"") != NULL);
    assert(strstr((char *)out, "2,ok") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_unquoted_quote_does_not_merge_records(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "a,b\nx\"y,1\nz,2\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "a,b") != NULL);
    assert(strstr((char *)out, "\"x\"\"y\",1") != NULL);
    assert(strstr((char *)out, "z,2") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_repair_diagnostics(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2,\"repair\":true,\"max_error_bytes\":5,\"audit\":true,\"audit_limit\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv =
        "a,b,c\n"
        "1,2\n"
        "3,4,5,6\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "a,b,c") != NULL);
    assert(strstr((char *)out, "1,2,") != NULL);
    assert(strstr((char *)out, "3,4,5") != NULL);
    assert(strstr((char *)out, "3,4,5,6") == NULL);

    uint8_t errors[2048];
    size_t e = tf_pipeline_pull(p, TF_CHAN_ERRORS, errors, sizeof(errors) - 1);
    assert(e > 0);
    errors[e] = '\0';
    assert(strstr((char *)errors, "csv_field_count") != NULL);
    assert(strstr((char *)errors, "\"mode\":\"repair\"") != NULL);
    assert(strstr((char *)errors, "\"action\":\"repair\"") != NULL);
    assert(strstr((char *)errors, "\"severity\":\"warning\"") != NULL);
    assert(strstr((char *)errors, "\"line\":2") != NULL);
    assert(strstr((char *)errors, "\"line\":3") != NULL);
    assert(strstr((char *)errors, "\"byte_offset\":6") != NULL);
    assert(strstr((char *)errors, "\"expected_fields\":3") != NULL);
    assert(strstr((char *)errors, "\"actual_fields\":2") != NULL);
    assert(strstr((char *)errors, "\"actual_fields\":4") != NULL);
    assert(strstr((char *)errors, "\"raw\":\"1,2\"") != NULL);
    assert(strstr((char *)errors, "\"raw\":\"3,4,5\"") != NULL);
    assert(strstr((char *)errors, "\"truncated\":true") != NULL);

    uint8_t stats[2048];
    size_t sn = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(sn > 0);
    stats[sn] = '\0';
    assert(strstr((char *)stats, "\"type\":\"audit\"") != NULL);
    assert(strstr((char *)stats, "\"op\":\"codec.csv.decode\"") != NULL);
    assert(strstr((char *)stats, "\"event\":\"row_repaired\"") != NULL);
    assert(strstr((char *)stats, "\"reason\":\"csv_field_count\"") != NULL);
    assert(strstr((char *)stats, "\"channel\":\"audit\"") != NULL);
    assert(strstr((char *)stats, "\"line\":2") != NULL);
    assert(strstr((char *)stats, "\"actual_fields\":2") != NULL);
    assert(strstr((char *)stats, "\"raw\":\"1,2\"") != NULL);
    assert(strstr((char *)stats, "\"line\":3") == NULL);
    assert(strstr((char *)stats, "\"raw\":\"3,4,5\"") == NULL);

    tf_pipeline_free(p);
}


static char *make_wide_csv_bytes(size_t n_cols) {
    size_t cap = n_cols * 32 + 32;
    char *buf = malloc(cap);
    assert(buf != NULL);
    size_t len = 0;
    for (size_t i = 0; i < n_cols; i++) {
        int n = snprintf(buf + len, cap - len, "%scol%zu", i ? "," : "", i);
        assert(n > 0 && (size_t)n < cap - len);
        len += (size_t)n;
    }
    assert(len + 1 < cap);
    buf[len++] = '\n';
    for (size_t i = 0; i < n_cols; i++) {
        int n = snprintf(buf + len, cap - len, "%sv%zu", i ? "," : "", i);
        assert(n > 0 && (size_t)n < cap - len);
        len += (size_t)n;
    }
    assert(len + 2 < cap);
    buf[len++] = '\n';
    buf[len] = '\0';
    return buf;
}

static void test_pipeline_csv_wide_columns(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    char *csv = make_wide_csv_bytes(300);
    char out[16384];
    run_plan_chunked(plan, csv, 13, out, sizeof(out));
    assert(strstr(out, "col0,col1") != NULL);
    assert(strstr(out, "col255") != NULL);
    assert(strstr(out, "col299") != NULL);
    assert(strstr(out, "v255") != NULL);
    assert(strstr(out, "v299") != NULL);
    free(csv);
}

static void test_pipeline_csv_max_columns(void) {
    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{\"max_columns\":0}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "max_columns");

    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"max_columns\":3,\"max_error_bytes\":8}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "a,b,c,d\n1,2,3,4\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_ERROR);
    const char *err = tf_pipeline_error(p);
    assert(err != NULL);
    assert(strstr(err, "csv record exceeds max_columns at line 1") != NULL);

    uint8_t errors[1024];
    size_t e = tf_pipeline_pull(p, TF_CHAN_ERRORS, errors, sizeof(errors) - 1);
    assert(e > 0);
    errors[e] = '\0';
    assert(strstr((char *)errors, "csv_too_many_columns") != NULL);
    assert(strstr((char *)errors, "\"action\":\"fail\"") != NULL);
    assert(strstr((char *)errors, "\"severity\":\"error\"") != NULL);
    assert(strstr((char *)errors, "\"line\":1") != NULL);
    assert(strstr((char *)errors, "\"max_columns\":3") != NULL);
    assert(strstr((char *)errors, "\"actual_fields\":4") != NULL);
    assert(strstr((char *)errors, "\"raw\":\"a,b,c,d\"") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_csv_column_name_byte_cap(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    size_t name_len = TF_MAX_COLUMN_NAME_BYTES + 1;
    size_t input_len = name_len + strlen("\n1\n") + 1;
    char *csv = malloc(input_len);
    assert(csv != NULL);
    memset(csv, 'a', name_len);
    memcpy(csv + name_len, "\n1\n", 4);

    assert(tf_pipeline_push(p, (const uint8_t *)csv, input_len - 1) == TF_ERROR);
    const char *err = tf_pipeline_error(p);
    assert(err != NULL);
    assert(strstr(err, "column name") != NULL);
    assert(strstr(err, "exceeds maximum") != NULL);

    free(csv);
    tf_pipeline_free(p);
}

static void test_pipeline_csv_max_record_bytes(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"max_record_bytes\":8,\"max_error_bytes\":5}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "a,b\n123456789";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_ERROR);
    const char *err = tf_pipeline_error(p);
    assert(err != NULL);
    assert(strstr(err, "csv record exceeds max_record_bytes at line 2") != NULL);

    uint8_t errors[1024];
    size_t e = tf_pipeline_pull(p, TF_CHAN_ERRORS, errors, sizeof(errors) - 1);
    assert(e > 0);
    errors[e] = '\0';
    assert(strstr((char *)errors, "csv_record_too_large") != NULL);
    assert(strstr((char *)errors, "\"action\":\"fail\"") != NULL);
    assert(strstr((char *)errors, "\"severity\":\"error\"") != NULL);
    assert(strstr((char *)errors, "\"line\":2") != NULL);
    assert(strstr((char *)errors, "\"byte_offset\":4") != NULL);
    assert(strstr((char *)errors, "\"max_record_bytes\":8") != NULL);
    assert(strstr((char *)errors, "\"observed_bytes\":9") != NULL);
    assert(strstr((char *)errors, "\"raw\":\"12345\"") != NULL);
    assert(strstr((char *)errors, "\"truncated\":true") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_strict_field_count(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"mode\":\"strict\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "a,b,c\n1,2\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_ERROR);
    const char *err = tf_pipeline_error(p);
    assert(err != NULL);
    assert(strstr(err, "csv strict field count mismatch at line 2") != NULL);

    uint8_t errors[1024];
    size_t e = tf_pipeline_pull(p, TF_CHAN_ERRORS, errors, sizeof(errors) - 1);
    assert(e > 0);
    errors[e] = '\0';
    assert(strstr((char *)errors, "csv_field_count") != NULL);
    assert(strstr((char *)errors, "\"mode\":\"strict\"") != NULL);
    assert(strstr((char *)errors, "\"action\":\"fail\"") != NULL);
    assert(strstr((char *)errors, "\"severity\":\"error\"") != NULL);
    assert(strstr((char *)errors, "\"line\":2") != NULL);
    assert(strstr((char *)errors, "\"byte_offset\":6") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_quoted_null_literals_disabled(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2,\"nulls\":\"NA\",\"quoted_nulls\":false}},"
        "{\"op\":\"fill-null\",\"args\":{\"mapping\":{\"note\":\"MISSING\"}}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv =
        "name,note\n"
        "A,\"NA\"\n"
        "B,NA\n"
        "C,\"\"\n"
        "D,\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "name,note") != NULL);
    assert(strstr((char *)out, "A,NA") != NULL);
    assert(strstr((char *)out, "B,MISSING") != NULL);
    assert(strstr((char *)out, "C,") != NULL);
    assert(strstr((char *)out, "C,MISSING") == NULL);
    assert(strstr((char *)out, "D,MISSING") != NULL);
    tf_pipeline_free(p);
}



static void test_pipeline_csv_comment_skip_empty_trim(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1,\"comment\":\"#\",\"skip_empty_rows\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv =
        "# generated\n"
        "name,score,note\n"
        " Alice , 10 , ok # trailing comment\n"
        "\n"
        "\"Bob # literal\",20,\" keep # inside \"\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, 17) == TF_OK);
    assert(tf_pipeline_push(p, (const uint8_t *)csv + 17, strlen(csv) - 17) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0 && n < sizeof(out));
    out[n] = '\0';
    assert(strstr((char *)out, "name,score,note\n") != NULL);
    assert(strstr((char *)out, "Alice,10,ok\n") != NULL);
    assert(strstr((char *)out, "Bob # literal,20, keep # inside \n") != NULL);
    assert(strstr((char *)out, "generated") == NULL);
    assert(strstr((char *)out, "trailing comment") == NULL);
    tf_pipeline_free(p);

    const char *notrim_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"trim_ws\":false}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(notrim_plan, strlen(notrim_plan));
    assert(p != NULL);
    const char *spaced = "name,score\n Alice , 10 \n";
    assert(tf_pipeline_push(p, (const uint8_t *)spaced, strlen(spaced)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0 && n < sizeof(out));
    out[n] = '\0';
    assert(strstr((char *)out, " Alice , 10 \n") != NULL);
    tf_pipeline_free(p);

    const char *skip_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"skip\":2,\"comment\":\"#\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(skip_plan, strlen(skip_plan));
    assert(p != NULL);
    const char *preamble =
        "generated by upstream system\n"
        "exported 2026-06-12\n"
        "# ignored after skip\n"
        "name,score\n"
        "Alice,10\n"
        "Bob,20\n";
    assert(tf_pipeline_push(p, (const uint8_t *)preamble, 11) == TF_OK);
    assert(tf_pipeline_push(p, (const uint8_t *)preamble + 11, strlen(preamble) - 11) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0 && n < sizeof(out));
    out[n] = '\0';
    assert(strstr((char *)out, "name,score\n") != NULL);
    assert(strstr((char *)out, "Alice,10\n") != NULL);
    assert(strstr((char *)out, "Bob,20\n") != NULL);
    assert(strstr((char *)out, "generated by") == NULL);
    assert(strstr((char *)out, "exported 2026") == NULL);
    assert(strstr((char *)out, "ignored after skip") == NULL);
    tf_pipeline_free(p);

    const char *nmax_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1,\"n_max\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(nmax_plan, strlen(nmax_plan));
    assert(p != NULL);
    const char *limited =
        "name,score\n"
        "Alice,10\n"
        "Bob,20\n"
        "Cara,30\n";
    assert(tf_pipeline_push(p, (const uint8_t *)limited, 9) == TF_OK);
    assert(tf_pipeline_push(p, (const uint8_t *)limited + 9, strlen(limited) - 9) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0 && n < sizeof(out));
    out[n] = '\0';
    assert(strstr((char *)out, "name,score\n") != NULL);
    assert(strstr((char *)out, "Alice,10\n") != NULL);
    assert(strstr((char *)out, "Bob,20\n") != NULL);
    assert(strstr((char *)out, "Cara,30") == NULL);
    tf_pipeline_free(p);

    const char *zero_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"max_rows\":0}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(zero_plan, strlen(zero_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)limited, strlen(limited)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0 && n < sizeof(out));
    out[n] = '\0';
    assert(strcmp((char *)out, "name,score\n") == 0);
    tf_pipeline_free(p);
}

static void test_pipeline_source_name_boundary_flush(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"source-name\",\"args\":{\"result\":\"src\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *first = "name\nAlice";
    assert(tf_pipeline_set_source_name(p, "part-a.csv") == TF_OK);
    assert(tf_pipeline_push(p, (const uint8_t *)first, strlen(first)) == TF_OK);
    assert(tf_pipeline_flush_input(p) == TF_OK);

    const char *second = "Bob\n";
    assert(tf_pipeline_set_source_name(p, "part-b.csv") == TF_OK);
    assert(tf_pipeline_push(p, (const uint8_t *)second, strlen(second)) == TF_OK);
    assert(tf_pipeline_flush_input(p) == TF_OK);

    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "name,src") != NULL);
    assert(strstr((char *)out, "Alice,part-a.csv") != NULL);
    assert(strstr((char *)out, "Bob,part-b.csv") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_csv_skip_repeated_header_boundary(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"skip_repeated_header\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *first = "name,age\nAlice,30";
    assert(tf_pipeline_push(p, (const uint8_t *)first, strlen(first)) == TF_OK);
    assert(tf_pipeline_flush_input(p) == TF_OK);

    const char *second = "name,age\nBob,25\n";
    assert(tf_pipeline_push(p, (const uint8_t *)second, strlen(second)) == TF_OK);
    assert(tf_pipeline_flush_input(p) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    char *first_header = strstr((char *)out, "name,age");
    assert(first_header != NULL);
    assert(strstr(first_header + strlen("name,age"), "name,age") == NULL);
    assert(strstr((char *)out, "Alice,30") != NULL);
    assert(strstr((char *)out, "Bob,25") != NULL);
    tf_pipeline_free(p);

    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *single_input = "name,age\nAlice,30\nname,age\nBob,25\n";
    assert(tf_pipeline_push(p, (const uint8_t *)single_input, strlen(single_input)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    first_header = strstr((char *)out, "name,age");
    assert(first_header != NULL);
    assert(strstr(first_header + strlen("name,age"), "name,age") != NULL);
    tf_pipeline_free(p);

    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_flush_input(p) == TF_OK);
    const char *after_empty_source = "name,age\nname,age\nBob,25\n";
    assert(tf_pipeline_push(p, (const uint8_t *)after_empty_source, strlen(after_empty_source)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    first_header = strstr((char *)out, "name,age");
    assert(first_header != NULL);
    assert(strstr(first_header + strlen("name,age"), "name,age") != NULL);
    assert(strstr((char *)out, "Bob,25") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_source_name_default(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"source-name\",\"args\":{\"result\":\"src\",\"default\":\"unknown\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name\nAlice\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[512];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "name,src") != NULL);
    assert(strstr((char *)out, "Alice,unknown") != NULL);
    tf_pipeline_free(p);
}

static void test_dsl_source_name(void) {
    char *error = NULL;
    const char *dsl = "csv | source-file src default=missing | csv";
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(plan->n_nodes == 3);
    assert(strcmp(plan->nodes[1].op, "source-name") == 0);
    cJSON *res = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "result");
    cJSON *def = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "default");
    assert(cJSON_IsString(res) && strcmp(res->valuestring, "src") == 0);
    assert(cJSON_IsString(def) && strcmp(def->valuestring, "missing") == 0);
    tf_ir_plan_free(plan);
}

/* ================================================================
 * Op Registry tests
 * ================================================================ */

static void test_registry_find_all_ops(void) {
    /* All built-in ops should be found */
    const char *ops[] = {
        "codec.csv.decode", "codec.csv.encode",
        "codec.jsonl.decode", "codec.jsonl.encode",
        "filter", "select", "relocate", "rename", "head",
        "skip", "derive", "source-name", "across", "stats", "scan", "unique", "sort",
        "reorder", "dedup", "validate", "assert", "quarantine", "schema", "schema-infer", "trim", "fill-null",
        "cast", "clip", "replace", "hash", "bin",
        "fill-down", "step", "window", "rolling-sum", "rolling-mean", "rolling-min", "rolling-max", "rolling-any", "rolling-all",
        "explode", "split", "unpivot",
        "tail", "slice-head", "slice-tail", "top", "top-k", "bottom-k", "slice-min", "slice-max",
        "sample", "group-agg", "frequency",
        "datetime", "json-extract", "json-filter", "json-schema", "json-flatten", "flatten", "join", "semi-join", "anti-join", "intersect", "setdiff", "union", "union-all", "lead", "lag", "shift", "rowid", "rleid"
    };
    for (size_t i = 0; i < sizeof(ops) / sizeof(ops[0]); i++) {
        const tf_op_entry *e = tf_op_registry_find(ops[i]);
        assert(e != NULL);
        assert(strcmp(e->name, ops[i]) == 0);
    }
    /* Unknown op should return NULL */
    assert(tf_op_registry_find("nonexistent") == NULL);
}

static void test_registry_op_kinds(void) {
    assert(tf_op_registry_find("codec.csv.decode")->kind == TF_OP_DECODER);
    assert(tf_op_registry_find("codec.csv.encode")->kind == TF_OP_ENCODER);
    assert(tf_op_registry_find("filter")->kind == TF_OP_TRANSFORM);
    assert(tf_op_registry_find("select")->kind == TF_OP_TRANSFORM);
}

static void test_registry_capabilities(void) {
    const tf_op_entry *csv_dec = tf_op_registry_find("codec.csv.decode");
    assert(csv_dec->caps & TF_CAP_STREAMING);
    assert(csv_dec->caps & TF_CAP_BOUNDED_MEMORY);
    assert(csv_dec->caps & TF_CAP_BROWSER_SAFE);
    assert(csv_dec->caps & TF_CAP_DETERMINISTIC);
    assert(!(csv_dec->caps & TF_CAP_FS));
    assert(!(csv_dec->caps & TF_CAP_NET));
}


static void assert_contract(const char *name, tf_memory_class mem,
                            tf_emit_class emit, tf_schema_class schema) {
    const tf_op_entry *e = tf_op_registry_find(name);
    assert(e != NULL);
    assert(e->memory_class == mem);
    assert(e->emit_class == emit);
    assert(e->schema_class == schema);
    assert(strcmp(tf_memory_class_name(e->memory_class), "unknown") != 0);
    assert(strcmp(tf_emit_class_name(e->emit_class), "unknown") != 0);
    assert(strcmp(tf_schema_class_name(e->schema_class), "unknown") != 0);
    assert(e->state_estimate != NULL);
    assert(strcmp(e->state_estimate, "unknown") != 0);
}

static void test_registry_contract_metadata(void) {
    assert_contract("filter", TF_MEM_ROW_LOCAL, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_STABLE);
    assert_contract("source-name", TF_MEM_ROW_LOCAL, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("json-extract", TF_MEM_ROW_LOCAL, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("json-filter", TF_MEM_ROW_LOCAL, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_STABLE);
    assert_contract("json-schema", TF_MEM_ROW_LOCAL, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("json-flatten", TF_MEM_ROW_LOCAL, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("assert", TF_MEM_ROW_LOCAL, TF_EMIT_MIXED,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("quarantine", TF_MEM_ROW_LOCAL, TF_EMIT_MIXED,
                    TF_SCHEMA_STABLE);
    assert_contract("tee", TF_MEM_ROW_LOCAL, TF_EMIT_MIXED,
                    TF_SCHEMA_STABLE);
    assert_contract("schema", TF_MEM_ROW_LOCAL, TF_EMIT_MIXED,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("schema-infer", TF_MEM_BOUNDED_STATE, TF_EMIT_ON_FLUSH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("relocate", TF_MEM_ROW_LOCAL, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("head", TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_STABLE);
    assert_contract("tail", TF_MEM_BOUNDED_STATE, TF_EMIT_ON_FLUSH,
                    TF_SCHEMA_STABLE);
    assert_contract("slice-head", TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_STABLE);
    assert_contract("slice-tail", TF_MEM_BOUNDED_STATE, TF_EMIT_ON_FLUSH,
                    TF_SCHEMA_STABLE);
    assert_contract("top", TF_MEM_BOUNDED_STATE, TF_EMIT_ON_FLUSH,
                    TF_SCHEMA_STABLE);
    assert_contract("top-k", TF_MEM_BOUNDED_STATE, TF_EMIT_ON_FLUSH,
                    TF_SCHEMA_STABLE);
    assert_contract("bottom-k", TF_MEM_BOUNDED_STATE, TF_EMIT_ON_FLUSH,
                    TF_SCHEMA_STABLE);
    assert_contract("slice-min", TF_MEM_BOUNDED_STATE, TF_EMIT_ON_FLUSH,
                    TF_SCHEMA_STABLE);
    assert_contract("slice-max", TF_MEM_BOUNDED_STATE, TF_EMIT_ON_FLUSH,
                    TF_SCHEMA_STABLE);
    assert_contract("stats", TF_MEM_BOUNDED_STATE, TF_EMIT_ON_FLUSH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("scan", TF_MEM_BOUNDED_STATE, TF_EMIT_ON_FLUSH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("unique", TF_MEM_KEY_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_STABLE);
    assert_contract("group-agg", TF_MEM_KEY_STATE, TF_EMIT_MIXED,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("frequency", TF_MEM_KEY_STATE, TF_EMIT_ON_FLUSH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("join", TF_MEM_KEY_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_DATA_DEPENDENT);
    assert_contract("semi-join", TF_MEM_KEY_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_STABLE);
    assert_contract("anti-join", TF_MEM_KEY_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_STABLE);
    assert_contract("lead", TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("lag", TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("shift", TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("rowid", TF_MEM_KEY_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("rleid", TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("rolling-sum", TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("rolling-mean", TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("rolling-min", TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("rolling-max", TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("rolling-any", TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("rolling-all", TF_MEM_BOUNDED_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("onehot", TF_MEM_KEY_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_DATA_DEPENDENT);
    assert_contract("sort", TF_MEM_BLOCKING, TF_EMIT_ON_FLUSH,
                    TF_SCHEMA_STABLE);
    assert_contract("normalize", TF_MEM_BLOCKING, TF_EMIT_ON_FLUSH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("acf", TF_MEM_BLOCKING, TF_EMIT_ON_FLUSH,
                    TF_SCHEMA_PARAMETRIC);
    assert_contract("codec.table.encode", TF_MEM_BLOCKING, TF_EMIT_ON_FLUSH,
                    TF_SCHEMA_STABLE);
    assert_contract("intersect-all", TF_MEM_KEY_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_STABLE);
    assert_contract("setdiff-all", TF_MEM_KEY_STATE, TF_EMIT_PER_BATCH,
                    TF_SCHEMA_STABLE);
    assert_contract("union", TF_MEM_KEY_STATE, TF_EMIT_MIXED,
                    TF_SCHEMA_STABLE);
    assert_contract("union-all", TF_MEM_BOUNDED_STATE, TF_EMIT_MIXED,
                    TF_SCHEMA_STABLE);
}

static void test_registry_count_and_iterate(void) {
    size_t count = tf_op_registry_count();
    assert(count == 91);  /* 7 codecs + 84 transforms */
    for (size_t i = 0; i < count; i++) {
        const tf_op_entry *e = tf_op_registry_get(i);
        assert(e != NULL);
        assert(e->name != NULL);
    }
    assert(tf_op_registry_get(count) == NULL);
}

/* ================================================================
 * IR plan construction tests
 * ================================================================ */

static void test_ir_plan_create_and_free(void) {
    tf_ir_plan *plan = tf_ir_plan_create();
    assert(plan != NULL);
    assert(plan->n_nodes == 0);
    tf_ir_plan_free(plan);
}

static void test_ir_plan_add_nodes(void) {
    tf_ir_plan *plan = tf_ir_plan_create();
    cJSON *args = cJSON_CreateObject();

    assert(tf_ir_plan_add_node(plan, "codec.csv.decode", args) == 0);
    assert(tf_ir_plan_add_node(plan, "filter", args) == 0);
    assert(tf_ir_plan_add_node(plan, "codec.csv.encode", args) == 0);

    assert(plan->n_nodes == 3);
    assert(strcmp(plan->nodes[0].op, "codec.csv.decode") == 0);
    assert(strcmp(plan->nodes[1].op, "filter") == 0);
    assert(strcmp(plan->nodes[2].op, "codec.csv.encode") == 0);
    assert(plan->nodes[0].index == 0);
    assert(plan->nodes[2].index == 2);

    cJSON_Delete(args);
    tf_ir_plan_free(plan);
}

static void test_ir_plan_clone(void) {
    tf_ir_plan *plan = tf_ir_plan_create();
    cJSON *args = cJSON_CreateObject();
    tf_ir_plan_add_node(plan, "codec.csv.decode", args);
    tf_ir_plan_add_node(plan, "codec.csv.encode", args);
    cJSON_Delete(args);

    tf_ir_plan *clone = tf_ir_plan_clone(plan);
    assert(clone != NULL);
    assert(clone->n_nodes == 2);
    assert(strcmp(clone->nodes[0].op, "codec.csv.decode") == 0);
    assert(strcmp(clone->nodes[1].op, "codec.csv.encode") == 0);

    /* Modifying original shouldn't affect clone */
    tf_ir_plan_free(plan);
    assert(strcmp(clone->nodes[0].op, "codec.csv.decode") == 0);

    tf_ir_plan_free(clone);
}

/* ================================================================
 * IR serialization tests
 * ================================================================ */

static void test_ir_from_json(void) {
    const char *json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"delimiter\":\",\"}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col('x') > 0\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    char *error = NULL;
    tf_ir_plan *plan = tf_ir_from_json(json, strlen(json), &error);
    assert(plan != NULL);
    assert(error == NULL);
    assert(plan->n_nodes == 3);
    assert(strcmp(plan->nodes[0].op, "codec.csv.decode") == 0);
    assert(strcmp(plan->nodes[1].op, "filter") == 0);

    /* Check args preserved */
    cJSON *delim = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "delimiter");
    assert(delim != NULL && cJSON_IsString(delim));
    assert(strcmp(delim->valuestring, ",") == 0);

    tf_ir_plan_free(plan);
}

static void test_ir_from_json_errors(void) {
    char *error = NULL;

    /* Bad JSON */
    tf_ir_plan *p = tf_ir_from_json("not json", 8, &error);
    assert(p == NULL);
    assert(error != NULL);
    free(error); error = NULL;

    /* Missing steps array */
    p = tf_ir_from_json("{}", 2, &error);
    assert(p == NULL);
    free(error); error = NULL;

    /* Empty steps */
    p = tf_ir_from_json("{\"steps\":[]}", 12, &error);
    assert(p == NULL);
    free(error); error = NULL;
}

static void test_ir_roundtrip(void) {
    const char *json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"delimiter\":\",\"}},"
        "{\"op\":\"select\",\"args\":{\"columns\":[\"name\",\"age\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    char *error = NULL;
    tf_ir_plan *plan = tf_ir_from_json(json, strlen(json), &error);
    assert(plan != NULL);

    char *out = tf_ir_to_json(plan);
    assert(out != NULL);

    /* Re-parse the serialized output */
    tf_ir_plan *plan2 = tf_ir_from_json(out, strlen(out), &error);
    assert(plan2 != NULL);
    assert(plan2->n_nodes == 3);
    assert(strcmp(plan2->nodes[0].op, "codec.csv.decode") == 0);
    assert(strcmp(plan2->nodes[1].op, "select") == 0);

    /* Check args survived roundtrip */
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(plan2->nodes[1].args, "columns");
    assert(cols != NULL && cJSON_IsArray(cols));
    assert(cJSON_GetArraySize(cols) == 2);

    free(out);
    tf_ir_plan_free(plan);
    tf_ir_plan_free(plan2);
}

/* ================================================================
 * IR validation tests
 * ================================================================ */

static void test_ir_validate_valid_plan(void) {
    const char *json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col('x') > 0\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    char *error = NULL;
    tf_ir_plan *plan = tf_ir_from_json(json, strlen(json), &error);
    assert(plan != NULL);
    assert(tf_ir_validate(plan) == TF_OK);
    assert(plan->validated == true);
    assert(plan->error == NULL);
    tf_ir_plan_free(plan);
}

static void test_ir_validate_no_decoder(void) {
    const char *json =
        "{\"steps\":["
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col('x') > 0\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    char *error = NULL;
    tf_ir_plan *plan = tf_ir_from_json(json, strlen(json), &error);
    assert(plan != NULL);
    assert(tf_ir_validate(plan) != TF_OK);
    assert(plan->error != NULL);
    assert(strstr(plan->error, "decoder") != NULL);
    tf_ir_plan_free(plan);
}

static void test_ir_validate_no_encoder(void) {
    const char *json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col('x') > 0\"}}"
        "]}";
    char *error = NULL;
    tf_ir_plan *plan = tf_ir_from_json(json, strlen(json), &error);
    assert(plan != NULL);
    assert(tf_ir_validate(plan) != TF_OK);
    assert(plan->error != NULL);
    assert(strstr(plan->error, "encoder") != NULL);
    tf_ir_plan_free(plan);
}

static void test_ir_validate_unknown_op(void) {
    const char *json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"bogus_op\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    char *error = NULL;
    tf_ir_plan *plan = tf_ir_from_json(json, strlen(json), &error);
    assert(plan != NULL);
    assert(tf_ir_validate(plan) != TF_OK);
    assert(strstr(plan->error, "unknown op") != NULL);
    tf_ir_plan_free(plan);
}

static void test_ir_validate_missing_required_arg(void) {
    /* filter requires 'expr' */
    const char *json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"filter\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    char *error = NULL;
    tf_ir_plan *plan = tf_ir_from_json(json, strlen(json), &error);
    assert(plan != NULL);
    assert(tf_ir_validate(plan) != TF_OK);
    assert(strstr(plan->error, "expr") != NULL);
    tf_ir_plan_free(plan);

    const char *validate_json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"validate\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    plan = tf_ir_from_json(validate_json, strlen(validate_json), &error);
    assert(plan != NULL);
    assert(tf_ir_validate(plan) != TF_OK);
    assert(strstr(plan->error, "expr, rules, or rules_file") != NULL);
    tf_ir_plan_free(plan);
}

static void test_ir_validate_plan_caps(void) {
    const char *json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    char *error = NULL;
    tf_ir_plan *plan = tf_ir_from_json(json, strlen(json), &error);
    assert(plan != NULL);
    assert(tf_ir_validate(plan) == TF_OK);
    /* All built-in ops are streaming + browser-safe */
    assert(plan->plan_caps & TF_CAP_STREAMING);
    assert(plan->plan_caps & TF_CAP_BROWSER_SAFE);
    tf_ir_plan_free(plan);
}


static void test_ir_validate_contract_metadata(void) {
    const char *json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"sort\",\"args\":{\"columns\":[{\"name\":\"age\",\"desc\":false}]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    char *error = NULL;
    tf_ir_plan *plan = tf_ir_from_json(json, strlen(json), &error);
    assert(plan != NULL);
    assert(tf_ir_validate(plan) == TF_OK);
    assert(plan->nodes[1].memory_class == TF_MEM_BLOCKING);
    assert(plan->nodes[1].emit_class == TF_EMIT_ON_FLUSH);
    assert(plan->nodes[1].schema_class == TF_SCHEMA_STABLE);
    assert(plan->nodes[1].state_estimate != NULL);
    assert(strcmp(plan->nodes[1].state_estimate, "O(input_rows)") == 0);
    tf_ir_plan_free(plan);
}

static void test_ir_validate_dynamic_pivot_contract_metadata(void) {
    const char *json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"pivot\",\"args\":{\"name_column\":\"metric\",\"value_column\":\"value\",\"agg\":\"sum\",\"categories\":[\"x\",\"y\"],\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    char *error = NULL;
    tf_ir_plan *plan = tf_ir_from_json(json, strlen(json), &error);
    assert(plan != NULL);
    assert(tf_ir_validate(plan) == TF_OK);
    assert(plan->nodes[1].memory_class == TF_MEM_BOUNDED_STATE);
    assert(plan->nodes[1].emit_class == TF_EMIT_MIXED);
    assert(plan->nodes[1].schema_class == TF_SCHEMA_PARAMETRIC);
    assert(strcmp(plan->nodes[1].state_estimate, "O(current_group + categories)") == 0);
    tf_ir_plan_free(plan);
}


static void test_ir_validate_host_policy_denies_file_args(void) {
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse("csv | join lookup.csv on id max_lookup_bytes=1024 | csv",
                                    strlen("csv | join lookup.csv on id max_lookup_bytes=1024 | csv"), &error);
    assert(plan != NULL);
    assert(error == NULL);
    tf_host_policy policy = {0};
    policy.allow_blocking = true;
    assert(tf_ir_validate_with_host_policy(plan, &policy) != TF_OK);
    assert(plan->error != NULL);
    assert(strstr(plan->error, "capability denied") != NULL);
    assert(strstr(plan->error, "allow_fs=false") != NULL);
    tf_ir_plan_free(plan);
}

static void test_ir_validate_host_policy_denies_rules_file_and_spill(void) {
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse("csv | validate rules_file=quality.json | csv",
                                    strlen("csv | validate rules_file=quality.json | csv"), &error);
    assert(plan != NULL);
    assert(error == NULL);
    tf_host_policy policy = {0};
    policy.allow_blocking = true;
    policy.allow_fs = true;
    assert(tf_ir_validate_with_host_policy(plan, &policy) != TF_OK);
    assert(plan->error != NULL);
    assert(strstr(plan->error, "allow_rules_file=false") != NULL);
    tf_ir_plan_free(plan);

    const char *spill_json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"sort\",\"args\":{\"columns\":[{\"name\":\"score\"}],\"spill_dir\":\"/tmp/tranfi-spill\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    plan = tf_ir_from_json(spill_json, strlen(spill_json), &error);
    assert(plan != NULL);
    assert(error == NULL);
    policy.allow_rules_file = true;
    policy.allow_spill = false;
    assert(tf_ir_validate_with_host_policy(plan, &policy) != TF_OK);
    assert(plan->error != NULL);
    assert(strstr(plan->error, "allow_spill=false") != NULL);
    tf_ir_plan_free(plan);
}

static void test_ir_validate_host_policy_workspace_resolver(void) {
    char root[256];
    snprintf(root, sizeof(root), "/tmp/tranfi_policy_%ld", (long)getpid());
    rmdir(root);
    assert(mkdir(root, 0700) == 0);

    char rules_path[512];
    snprintf(rules_path, sizeof(rules_path), "%s/rules.json", root);
    FILE *f = fopen(rules_path, "wb");
    assert(f != NULL);
    fputs("{\"rules\":[{\"name\":\"ok\",\"expr\":\"col('age') > 0\"}]}", f);
    fclose(f);

    const char *plan_json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"validate\",\"args\":{\"rules_file\":\"rules.json\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_host_policy policy = {0};
    policy.allow_blocking = true;
    policy.allow_fs = true;
    policy.allow_rules_file = true;
    policy.workspace_root = root;

    tf_pipeline *p = tf_pipeline_create_with_host_policy(plan_json, strlen(plan_json), &policy);
    assert(p != NULL);
    const char *csv = "name,age\nAlice,30\nBob,-1\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[512];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice,30,true") != NULL);
    assert(strstr((char *)out, "Bob,-1,false") != NULL);
    tf_pipeline_free(p);

    const char *escape_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"validate\",\"args\":{\"rules_file\":\"../rules.json\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create_with_host_policy(escape_plan, strlen(escape_plan), &policy);
    assert(p == NULL);
    assert(tf_last_error() != NULL);
    assert(strstr(tf_last_error(), "host path resolver") != NULL);

    unlink(rules_path);
    assert(rmdir(root) == 0);
}

static void test_ir_validate_host_policy_dynamic_caps(void) {
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse("csv | validate rules_file=quality.json | csv",
                                    strlen("csv | validate rules_file=quality.json | csv"), &error);
    assert(plan != NULL);
    assert(error == NULL);
    assert(tf_ir_validate(plan) == TF_OK);
    assert((plan->nodes[1].caps & TF_CAP_FS) != 0);
    assert((plan->nodes[1].caps & TF_CAP_BROWSER_SAFE) == 0);
    tf_ir_plan_free(plan);
}

/* ================================================================
 * Schema inference tests
 * ================================================================ */

static void test_ir_schema_passthrough(void) {
    /* Decoder output is unknown, transforms propagate unknown */
    const char *json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col('x') > 0\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    char *error = NULL;
    tf_ir_plan *plan = tf_ir_from_json(json, strlen(json), &error);
    assert(plan != NULL);
    tf_ir_validate(plan);
    tf_ir_infer_schema(plan);

    assert(plan->schema_inferred);
    /* csv decoder output is unknown */
    assert(!plan->nodes[0].output_schema.known);
    /* filter propagates unknown */
    assert(!plan->nodes[1].output_schema.known);

    tf_ir_plan_free(plan);
}

static void test_ir_schema_select_known(void) {
    /* Build an IR with a known input schema to test select inference */
    tf_ir_plan *plan = tf_ir_plan_create();

    cJSON *dec_args = cJSON_CreateObject();
    tf_ir_plan_add_node(plan, "codec.csv.decode", dec_args);
    cJSON_Delete(dec_args);

    /* Manually set the decoder's output schema to known for testing */
    plan->nodes[0].output_schema.known = true;
    plan->nodes[0].output_schema.n_cols = 3;
    plan->nodes[0].output_schema.col_names = calloc(3, sizeof(char *));
    plan->nodes[0].output_schema.col_types = calloc(3, sizeof(tf_type));
    plan->nodes[0].output_schema.col_names[0] = strdup("name");
    plan->nodes[0].output_schema.col_names[1] = strdup("age");
    plan->nodes[0].output_schema.col_names[2] = strdup("score");
    plan->nodes[0].output_schema.col_types[0] = TF_TYPE_STRING;
    plan->nodes[0].output_schema.col_types[1] = TF_TYPE_INT64;
    plan->nodes[0].output_schema.col_types[2] = TF_TYPE_FLOAT64;

    cJSON *sel_args = cJSON_CreateObject();
    cJSON *cols = cJSON_CreateArray();
    cJSON_AddItemToArray(cols, cJSON_CreateString("name"));
    cJSON_AddItemToArray(cols, cJSON_CreateString("age"));
    cJSON_AddItemToObject(sel_args, "columns", cols);
    tf_ir_plan_add_node(plan, "select", sel_args);
    cJSON_Delete(sel_args);

    cJSON *enc_args = cJSON_CreateObject();
    tf_ir_plan_add_node(plan, "codec.csv.encode", enc_args);
    cJSON_Delete(enc_args);

    /* Run schema inference starting from node 1 (decoder output is set manually) */
    /* We need to call the registry's infer_schema for select */
    const tf_op_entry *sel_entry = tf_op_registry_find("select");
    assert(sel_entry != NULL);

    tf_schema sel_out = {0};
    int rc = sel_entry->infer_schema(&plan->nodes[1],
                                     &plan->nodes[0].output_schema, &sel_out);
    assert(rc == TF_OK);
    assert(sel_out.known);
    assert(sel_out.n_cols == 2);
    assert(strcmp(sel_out.col_names[0], "name") == 0);
    assert(strcmp(sel_out.col_names[1], "age") == 0);
    assert(sel_out.col_types[0] == TF_TYPE_STRING);
    assert(sel_out.col_types[1] == TF_TYPE_INT64);

    tf_schema_free(&sel_out);
    tf_ir_plan_free(plan);
}

static void test_ir_schema_rename_known(void) {
    /* Test rename schema inference with known input */
    tf_schema in = {0};
    in.known = true;
    in.n_cols = 2;
    in.col_names = calloc(2, sizeof(char *));
    in.col_types = calloc(2, sizeof(tf_type));
    in.col_names[0] = strdup("name");
    in.col_names[1] = strdup("age");
    in.col_types[0] = TF_TYPE_STRING;
    in.col_types[1] = TF_TYPE_INT64;

    /* Build args for rename: name → full_name */
    cJSON *args = cJSON_CreateObject();
    cJSON *mapping = cJSON_CreateObject();
    cJSON_AddStringToObject(mapping, "name", "full_name");
    cJSON_AddItemToObject(args, "mapping", mapping);

    tf_ir_node node = {0};
    node.op = "rename";
    node.args = args;

    const tf_op_entry *entry = tf_op_registry_find("rename");
    tf_schema out = {0};
    int rc = entry->infer_schema(&node, &in, &out);
    assert(rc == TF_OK);
    assert(out.known);
    assert(out.n_cols == 2);
    assert(strcmp(out.col_names[0], "full_name") == 0);
    assert(strcmp(out.col_names[1], "age") == 0);

    tf_schema_free(&out);
    tf_schema_free(&in);
    cJSON_Delete(args);
}

static void test_ir_schema_group_agg_preserves_key_types(void) {
    tf_schema in = {0};
    in.known = true;
    in.n_cols = 3;
    in.col_names = calloc(3, sizeof(char *));
    in.col_types = calloc(3, sizeof(tf_type));
    in.col_names[0] = strdup("id");
    in.col_names[1] = strdup("city");
    in.col_names[2] = strdup("score");
    in.col_types[0] = TF_TYPE_INT64;
    in.col_types[1] = TF_TYPE_STRING;
    in.col_types[2] = TF_TYPE_FLOAT64;

    cJSON *args = cJSON_CreateObject();
    cJSON *group_by = cJSON_CreateArray();
    cJSON_AddItemToArray(group_by, cJSON_CreateString("id"));
    cJSON_AddItemToArray(group_by, cJSON_CreateString("city"));
    cJSON_AddItemToObject(args, "group_by", group_by);
    cJSON *aggs = cJSON_CreateArray();
    cJSON *agg = cJSON_CreateObject();
    cJSON_AddStringToObject(agg, "column", "score");
    cJSON_AddStringToObject(agg, "func", "sum");
    cJSON_AddStringToObject(agg, "name", "total");
    cJSON_AddItemToArray(aggs, agg);
    cJSON_AddItemToObject(args, "aggs", aggs);

    tf_ir_node node = {0};
    node.op = "group-agg";
    node.args = args;

    const tf_op_entry *entry = tf_op_registry_find("group-agg");
    tf_schema out = {0};
    int rc = entry->infer_schema(&node, &in, &out);
    assert(rc == TF_OK);
    assert(out.known);
    assert(out.n_cols == 3);
    assert(strcmp(out.col_names[0], "id") == 0);
    assert(strcmp(out.col_names[1], "city") == 0);
    assert(strcmp(out.col_names[2], "total") == 0);
    assert(out.col_types[0] == TF_TYPE_INT64);
    assert(out.col_types[1] == TF_TYPE_STRING);
    assert(out.col_types[2] == TF_TYPE_FLOAT64);

    tf_schema_free(&out);
    tf_schema_free(&in);
    cJSON_Delete(args);
}

/* ================================================================
 * Compiler tests
 * ================================================================ */

static void test_compile_native_valid(void) {
    const char *json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col('x') > 0\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    char *error = NULL;
    tf_ir_plan *plan = tf_ir_from_json(json, strlen(json), &error);
    assert(plan != NULL);
    assert(tf_ir_validate(plan) == TF_OK);

    tf_decoder *decoder = NULL;
    tf_step **steps = NULL;
    size_t n_steps = 0;
    tf_encoder *encoder = NULL;

    int rc = tf_compile_native(plan, &decoder, &steps, &n_steps, &encoder, &error);
    assert(rc == TF_OK);
    assert(decoder != NULL);
    assert(encoder != NULL);
    assert(n_steps == 1);  /* filter */
    assert(steps[0] != NULL);

    /* Cleanup */
    decoder->destroy(decoder);
    encoder->destroy(encoder);
    steps[0]->destroy(steps[0]);
    free(steps);
    tf_ir_plan_free(plan);
}

static void test_compile_to_sql_grep_literal_chars(void) {
    const char *dsl = "csv | grep % name | csv";
    char *error = NULL;
    char *sql = tf_compile_to_sql(dsl, strlen(dsl), &error);
    assert(sql != NULL);
    assert(error == NULL);
    assert(strstr(sql, "contains(CAST(\"name\" AS VARCHAR), '%')") != NULL);
    assert(strstr(sql, "LIKE") == NULL);
    tf_string_free(sql);

    dsl = "csv | grep _ name | csv";
    sql = tf_compile_to_sql(dsl, strlen(dsl), &error);
    assert(sql != NULL);
    assert(error == NULL);
    assert(strstr(sql, "contains(CAST(\"name\" AS VARCHAR), '_')") != NULL);
    tf_string_free(sql);

    dsl = "csv | grep \\\\ name | csv";
    sql = tf_compile_to_sql(dsl, strlen(dsl), &error);
    assert(sql != NULL);
    assert(error == NULL);
    assert(strstr(sql, "contains(CAST(\"name\" AS VARCHAR), '\\\\')") != NULL);
    tf_string_free(sql);
}

static void test_compile_to_sql_rejects_sample(void) {
    const char *dsl = "csv | sample 2 seed=42 | csv";
    char *error = NULL;
    char *sql = tf_compile_to_sql(dsl, strlen(dsl), &error);
    assert(sql == NULL);
    assert(error != NULL);
    assert(strstr(error, "sample") != NULL);
    assert(strstr(error, "cannot be lowered to SQL") != NULL);
    free(error);
}

static void test_compile_to_sql_rejects_stats(void) {
    const char *dsl = "csv | stats count,missing,complete_rate | csv";
    char *error = NULL;
    char *sql = tf_compile_to_sql(dsl, strlen(dsl), &error);
    assert(sql == NULL);
    assert(error != NULL);
    assert(strstr(error, "stats") != NULL);
    assert(strstr(error, "cannot be lowered to SQL") != NULL);
    free(error);
}

typedef struct {
    const char *name;
    const char *dsl;
    const char *needle;
} sql_supported_case;

typedef struct {
    const char *name;
    const char *dsl;
    const char *needle;
} sql_rejected_case;

static void assert_compile_to_sql_supported(const sql_supported_case *tc) {
    char *error = NULL;
    char *sql = tf_compile_to_sql(tc->dsl, strlen(tc->dsl), &error);
    if (!sql) {
        fprintf(stderr, "SQL support case failed: %s: %s\n",
                tc->name, error ? error : "unknown");
    }
    assert(sql != NULL);
    assert(error == NULL);
    if (tc->needle) {
        assert(strstr(sql, tc->needle) != NULL);
    }
    tf_string_free(sql);
}

static void assert_compile_to_sql_rejected(const sql_rejected_case *tc) {
    char *error = NULL;
    char *sql = tf_compile_to_sql(tc->dsl, strlen(tc->dsl), &error);
    if (sql) {
        fprintf(stderr, "SQL rejection case compiled unexpectedly: %s: %s\n",
                tc->name, sql);
    }
    assert(sql == NULL);
    assert(error != NULL);
    assert(strstr(error, tc->needle) != NULL);
    free(error);
}

static void test_compile_to_sql_supported_matrix(void) {
    const sql_supported_case cases[] = {
        {"filter", "csv | filter \"col('age') > 25\" | csv", "WHERE"},
        {"relocate-default", "csv | relocate score | csv", "EXCLUDE"},
        {"select", "csv | select name,age | csv", "SELECT \"name\", \"age\""},
        {"rename", "csv | rename age=years | csv", "RENAME"},
        {"derive", "csv | derive total=col('price')*col('qty') | csv", "\"total\""},
        {"validate-expression", "csv | validate \"col('age') > 0\" | csv", "\"_valid\""},
        {"trim-explicit", "csv | trim name,city | csv", "trim(\"name\")"},
        {"fill-null", "csv | fill-null age=0 | csv", "COALESCE"},
        {"cast", "csv | cast age=int | csv", "CAST(\"age\" AS BIGINT)"},
        {"clip", "csv | clip score min=0 max=100 | csv", "GREATEST"},
        {"replace", "csv | replace name Alice Alicia | csv", "replace(\"name\""},
        {"hash-columns", "csv | hash name,age | csv", "hash(\"name\", \"age\")"},
        {"bin", "csv | bin score 80,90 | csv", "CASE WHEN"},
        {"sort", "csv | sort -score | csv", "ORDER BY \"score\" DESC"},
        {"head", "csv | head 10 | csv", "LIMIT 10"},
        {"tail", "csv | tail 10 | csv", "_total - 10"},
        {"skip", "csv | skip 5 | csv", "OFFSET 5"},
        {"top", "csv | top 10 score | csv", "ORDER BY \"score\" DESC LIMIT 10"},
        {"top-k", "csv | top-k 10 score | csv", "ORDER BY \"score\" DESC LIMIT 10"},
        {"bottom-k", "csv | bottom-k 10 score | csv", "ORDER BY \"score\" ASC LIMIT 10"},
        {"slice-min", "csv | slice-min score n=10 | csv", "ORDER BY \"score\" ASC LIMIT 10"},
        {"slice-max", "csv | slice-max score n=10 | csv", "ORDER BY \"score\" DESC LIMIT 10"},
        {"unique", "csv | unique city | csv", "SELECT DISTINCT ON"},
        {"dedup", "csv | dedup | csv", "SELECT DISTINCT *"},
        {"group-agg", "csv | group-agg city sum:price:total | csv", "SUM(\"price\")"},
        {"frequency", "csv | frequency city | csv", "\"value\", COUNT(*)"},
        {"join", "csv | join lookup.csv on city | csv", "INNER JOIN read_csv_auto"},
        {"semi-join", "csv | semi-join lookup.csv on city | csv", "WHERE EXISTS"},
        {"anti-join", "csv | anti-join lookup.csv on city | csv", "WHERE NOT EXISTS"},
        {"intersect", "csv | intersect other.csv | csv", " INTERSECT SELECT "},
        {"setdiff", "csv | setdiff other.csv | csv", " EXCEPT SELECT "},
        {"intersect-all", "csv | intersect-all other.csv | csv", " INTERSECT ALL SELECT "},
        {"setdiff-all", "csv | setdiff-all other.csv | csv", " EXCEPT ALL SELECT "},
        {"union", "csv | union other.csv | csv", " UNION SELECT "},
        {"union-all", "csv | union-all other.csv | csv", " UNION ALL SELECT "},
        {"stack", "csv | stack other.csv | csv", "UNION ALL SELECT"},
        {"explode", "csv | explode tags ; | csv", "string_split"},
        {"split", "csv | split name \" \" first,last | csv", "string_split"},
        {"unpivot", "csv | unpivot jan,feb | csv", "UNPIVOT"},
        {"pivot", "csv | pivot metric value sum | csv", "PIVOT"},
        {"step", "csv | step price running-sum cumsum | csv", "UNBOUNDED PRECEDING"},
        {"lead", "csv | lead price 1 next_price | csv", "LEAD"},
        {"lag", "csv | lag price 1 prev_price | csv", "LAG"},
        {"shift-lead", "csv | shift price 1 next_price type=lead | csv", "LEAD"},
        {"rowid", "csv | rowid | csv", "ROW_NUMBER()"},
        {"rleid", "csv | rleid city result=run | csv", "__tf_rleid_changed"},
        {"window", "csv | window price 3 avg ma3 | csv", "ROWS BETWEEN 2 PRECEDING"},
        {"rolling-any", "csv | rolling-any flag 3 any3 | csv", "BOOL_OR"},
        {"datetime", "csv | datetime date year,month | csv", "EXTRACT(year"},
        {"date-trunc", "csv | date-trunc date month date_month | csv", "date_trunc('month'"},
    };
    size_t n = sizeof(cases) / sizeof(cases[0]);
    for (size_t i = 0; i < n; i++) {
        assert_compile_to_sql_supported(&cases[i]);
    }
}

static void test_compile_to_sql_rejected_matrix(void) {
    const sql_rejected_case cases[] = {
        {"relocate-anchored", "csv | relocate score before=age | csv", "requires known schema"},
        {"select-selector", "csv | select starts_with(score_) | csv", "selector helpers"},
        {"assert", "csv | assert \"col('age') > 0\" | csv", "unsupported op"},
        {"schema", "csv | schema age:int | csv", "unsupported op"},
        {"schema-infer", "csv | schema infer rows=10 | csv", "unsupported op"},
        {"trim-all", "csv | trim | csv", "requires explicit columns"},
        {"sample", "csv | sample 10 seed=1 | csv", "deterministic reservoir"},
        {"unique-sorted", "csv | unique city sorted=true | csv", "adjacent-run mode"},
        {"unique-approx", "csv | unique city mode=approx | csv", "approximate mode"},
        {"frequency-all", "csv | frequency | csv", "requires explicit columns"},
        {"frequency-overflow", "csv | frequency city max_values=2 overflow=other | csv", "overflow=other"},
        {"selected-key-set", "csv | intersect other.csv columns=id | csv", "all-column set operations"},
        {"stats", "csv | stats count | csv", "native report shape"},
        {"scan", "csv | scan count | csv", "unsupported op"},
        {"source-name", "csv | source-name src | csv", "unsupported op"},
        {"json-extract", "text | json-extract /user/id user_id type=int | csv", "unsupported op"},
        {"json-filter", "text | json-filter /user/age >= 30 type=float | csv", "unsupported op"},
        {"json-schema", "text | json-schema required=user mode=filter | csv", "unsupported op"},
        {"json-flatten", "text | json-flatten fields=/user/id:user_id:int | csv", "unsupported op"},
        {"across", "csv | across name lower | csv", "unsupported op"},
        {"onehot", "csv | onehot city | csv", "unsupported op"},
        {"label-encode", "csv | label-encode city | csv", "unsupported op"},
        {"split-data", "csv | split-data 0.8 | csv", "unsupported op"},
        {"ewma", "csv | ewma price 0.3 | csv", "unsupported op"},
        {"diff", "csv | diff price | csv", "unsupported op"},
        {"anomaly", "csv | anomaly price 3.0 | csv", "unsupported op"},
        {"interpolate", "csv | interpolate price linear | csv", "unsupported op"},
        {"normalize", "csv | normalize price minmax | csv", "unsupported op"},
        {"acf", "csv | acf price 3 | csv", "unsupported op"},
    };
    size_t n = sizeof(cases) / sizeof(cases[0]);
    for (size_t i = 0; i < n; i++) {
        assert_compile_to_sql_rejected(&cases[i]);
    }
}

static void test_pipeline_create_from_ir(void) {
    const char *json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    char *error = NULL;
    tf_ir_plan *plan = tf_ir_from_json(json, strlen(json), &error);
    assert(plan != NULL);
    assert(tf_ir_validate(plan) == TF_OK);

    tf_pipeline *p = tf_pipeline_create_from_ir(plan);
    assert(p != NULL);

    const char *csv = "x,y\n1,2\n3,4\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "x,y") != NULL);

    tf_pipeline_free(p);
    tf_ir_plan_free(plan);
}

/* ================================================================
 * Public IR API tests (via tranfi.h)
 * ================================================================ */

static void test_public_ir_api(void) {
    const char *json =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{}},"
        "{\"op\":\"head\",\"args\":{\"n\":5}},"
        "{\"op\":\"codec.jsonl.encode\",\"args\":{}}"
        "]}";

    char *error = NULL;
    tf_ir_plan *plan = tf_ir_plan_from_json(json, strlen(json), &error);
    assert(plan != NULL);

    assert(tf_ir_plan_validate(plan) == TF_OK);
    tf_ir_plan_infer_schema(plan);

    char *out = tf_ir_plan_to_json(plan);
    assert(out != NULL);
    assert(strstr(out, "codec.jsonl.decode") != NULL);
    free(out);

    tf_ir_plan_destroy(plan);
}

/* ================================================================
 * DSL parser tests
 * ================================================================ */

static void test_dsl_csv_passthrough(void) {
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse("csv | csv", 9, &error);
    assert(plan != NULL);
    assert(plan->n_nodes == 2);
    assert(strcmp(plan->nodes[0].op, "codec.csv.decode") == 0);
    assert(strcmp(plan->nodes[1].op, "codec.csv.encode") == 0);
    tf_ir_plan_free(plan);
}

static void test_dsl_jsonl_passthrough(void) {
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse("jsonl | jsonl", 13, &error);
    assert(plan != NULL);
    assert(plan->n_nodes == 2);
    assert(strcmp(plan->nodes[0].op, "codec.jsonl.decode") == 0);
    assert(strcmp(plan->nodes[1].op, "codec.jsonl.encode") == 0);
    tf_ir_plan_free(plan);
}

static void test_dsl_text(void) {
    /* text | text passthrough */
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse("text | text", 11, &error);
    assert(plan != NULL);
    assert(plan->n_nodes == 2);
    assert(strcmp(plan->nodes[0].op, "codec.text.decode") == 0);
    assert(strcmp(plan->nodes[1].op, "codec.text.encode") == 0);
    tf_ir_plan_free(plan);

    /* text | head 5 | text */
    const char *dsl2 = "text | head 5 | text";
    plan = tf_dsl_parse(dsl2, strlen(dsl2), &error);
    assert(plan != NULL);
    assert(plan->n_nodes == 3);
    assert(strcmp(plan->nodes[0].op, "codec.text.decode") == 0);
    assert(strcmp(plan->nodes[1].op, "head") == 0);
    assert(strcmp(plan->nodes[2].op, "codec.text.encode") == 0);
    tf_ir_plan_free(plan);

    /* text | grep pattern | text */
    const char *dsl3 = "text | grep error | text";
    plan = tf_dsl_parse(dsl3, strlen(dsl3), &error);
    assert(plan != NULL);
    assert(plan->n_nodes == 3);
    assert(strcmp(plan->nodes[0].op, "codec.text.decode") == 0);
    assert(strcmp(plan->nodes[1].op, "grep") == 0);
    assert(strcmp(plan->nodes[2].op, "codec.text.encode") == 0);
    /* Check grep pattern arg */
    cJSON *pat = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "pattern");
    assert(cJSON_IsString(pat));
    assert(strcmp(pat->valuestring, "error") == 0);
    tf_ir_plan_free(plan);

    /* text | grep -v warning | text */
    const char *dsl4 = "text | grep -v warning | text";
    plan = tf_dsl_parse(dsl4, strlen(dsl4), &error);
    assert(plan != NULL);
    assert(plan->n_nodes == 3);
    assert(strcmp(plan->nodes[1].op, "grep") == 0);
    cJSON *inv = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "invert");
    assert(cJSON_IsTrue(inv));
    pat = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "pattern");
    assert(strcmp(pat->valuestring, "warning") == 0);
    tf_ir_plan_free(plan);

    /* Explicit forms */
    const char *dsl5 = "text.decode | text.encode";
    plan = tf_dsl_parse(dsl5, strlen(dsl5), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[0].op, "codec.text.decode") == 0);
    assert(strcmp(plan->nodes[1].op, "codec.text.encode") == 0);
    tf_ir_plan_free(plan);
}

static void test_dsl_filter(void) {
    const char *dsl = "csv | filter \"col(age) > 25\" | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(plan->n_nodes == 3);
    assert(strcmp(plan->nodes[1].op, "filter") == 0);
    cJSON *expr = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "expr");
    assert(expr && cJSON_IsString(expr));
    assert(strcmp(expr->valuestring, "col(age) > 25") == 0);
    tf_ir_plan_free(plan);

    const char *audit_dsl = "csv | filter \"col(age) > 25\" audit audit_limit=7 | csv";
    plan = tf_dsl_parse(audit_dsl, strlen(audit_dsl), &error);
    assert(plan != NULL);
    cJSON *audit = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit");
    cJSON *audit_limit = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_limit");
    assert(cJSON_IsTrue(audit));
    assert(cJSON_IsNumber(audit_limit) && audit_limit->valueint == 7);
    tf_ir_plan_free(plan);
}

static void test_dsl_select(void) {
    const char *dsl = "csv | select name,age | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(plan->n_nodes == 3);
    assert(strcmp(plan->nodes[1].op, "select") == 0);
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    assert(cols && cJSON_IsArray(cols));
    assert(cJSON_GetArraySize(cols) == 2);
    assert(strcmp(cJSON_GetArrayItem(cols, 0)->valuestring, "name") == 0);
    assert(strcmp(cJSON_GetArrayItem(cols, 1)->valuestring, "age") == 0);
    tf_ir_plan_free(plan);
}

static void test_dsl_select_spaces(void) {
    /* Space-separated column names also work */
    const char *dsl = "csv | select name age score | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    assert(cJSON_GetArraySize(cols) == 3);
    tf_ir_plan_free(plan);
}


static void test_dsl_relocate(void) {
    const char *dsl = "csv | relocate score before=age | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(plan->n_nodes == 3);
    assert(strcmp(plan->nodes[1].op, "relocate") == 0);
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    assert(cols && cJSON_IsArray(cols));
    assert(cJSON_GetArraySize(cols) == 1);
    assert(strcmp(cJSON_GetArrayItem(cols, 0)->valuestring, "score") == 0);
    cJSON *before = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "before");
    assert(before && cJSON_IsString(before));
    assert(strcmp(before->valuestring, "age") == 0);
    tf_ir_plan_free(plan);
}

static void test_dsl_rename(void) {
    const char *dsl = "csv | rename name=full_name,age=years | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "rename") == 0);
    cJSON *mapping = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "mapping");
    assert(mapping && cJSON_IsObject(mapping));
    cJSON *v1 = cJSON_GetObjectItemCaseSensitive(mapping, "name");
    assert(v1 && strcmp(v1->valuestring, "full_name") == 0);
    cJSON *v2 = cJSON_GetObjectItemCaseSensitive(mapping, "age");
    assert(v2 && strcmp(v2->valuestring, "years") == 0);
    tf_ir_plan_free(plan);
}

static void test_dsl_head(void) {
    const char *dsl = "csv | head 5 | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "head") == 0);
    cJSON *n = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "n");
    assert(n && cJSON_IsNumber(n));
    assert(n->valuedouble == 5);
    tf_ir_plan_free(plan);
}

static void test_dsl_combined(void) {
    const char *dsl = "csv | filter \"col(age) > 25\" | select name,age | head 10 | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(plan->n_nodes == 5);
    assert(strcmp(plan->nodes[0].op, "codec.csv.decode") == 0);
    assert(strcmp(plan->nodes[1].op, "filter") == 0);
    assert(strcmp(plan->nodes[2].op, "select") == 0);
    assert(strcmp(plan->nodes[3].op, "head") == 0);
    assert(strcmp(plan->nodes[4].op, "codec.csv.encode") == 0);

    /* Also validates successfully */
    assert(tf_ir_validate(plan) == TF_OK);
    tf_ir_plan_free(plan);
}

static void test_dsl_explicit_codec(void) {
    const char *dsl = "csv.decode | csv.encode";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(plan->n_nodes == 2);
    assert(strcmp(plan->nodes[0].op, "codec.csv.decode") == 0);
    assert(strcmp(plan->nodes[1].op, "codec.csv.encode") == 0);
    tf_ir_plan_free(plan);
}

static void test_dsl_codec_options(void) {
    const char *dsl = "csv delimiter=; | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    cJSON *delim = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "delimiter");
    assert(delim && cJSON_IsString(delim));
    assert(strcmp(delim->valuestring, ";") == 0);
    tf_ir_plan_free(plan);

    dsl = "csv nulls=NA,NULL quoted_nulls=false | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    cJSON *nulls = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "nulls");
    assert(nulls && cJSON_IsString(nulls));
    assert(strcmp(nulls->valuestring, "NA,NULL") == 0);
    cJSON *quoted_nulls = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "quoted_nulls");
    assert(quoted_nulls && cJSON_IsBool(quoted_nulls) && cJSON_IsFalse(quoted_nulls));
    tf_ir_plan_free(plan);

    dsl = "csv skip=2 n_max=3 comment=# skip_empty_rows=true trim_ws=false | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    cJSON *csv_skip = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "skip");
    assert(csv_skip && cJSON_IsNumber(csv_skip) && csv_skip->valueint == 2);
    cJSON *csv_n_max = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "n_max");
    assert(csv_n_max && cJSON_IsNumber(csv_n_max) && csv_n_max->valueint == 3);
    cJSON *csv_comment = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "comment");
    assert(csv_comment && cJSON_IsString(csv_comment));
    assert(strcmp(csv_comment->valuestring, "#") == 0);
    cJSON *csv_skip_empty = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "skip_empty_rows");
    assert(csv_skip_empty && cJSON_IsBool(csv_skip_empty) && cJSON_IsTrue(csv_skip_empty));
    cJSON *csv_trim_ws = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "trim_ws");
    assert(csv_trim_ws && cJSON_IsBool(csv_trim_ws) && cJSON_IsFalse(csv_trim_ws));
    tf_ir_plan_free(plan);

    dsl = "csv mode=repair max_error_bytes=5 max_record_bytes=8 audit audit_limit=7 | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    cJSON *mode = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "mode");
    assert(mode && cJSON_IsString(mode));
    assert(strcmp(mode->valuestring, "repair") == 0);
    cJSON *csv_max_error_bytes = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "max_error_bytes");
    assert(csv_max_error_bytes && cJSON_IsNumber(csv_max_error_bytes) && csv_max_error_bytes->valueint == 5);
    cJSON *csv_max_record_bytes = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "max_record_bytes");
    assert(csv_max_record_bytes && cJSON_IsNumber(csv_max_record_bytes) && csv_max_record_bytes->valueint == 8);
    cJSON *csv_audit = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "audit");
    assert(csv_audit && cJSON_IsBool(csv_audit) && cJSON_IsTrue(csv_audit));
    cJSON *csv_audit_limit = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "audit_limit");
    assert(csv_audit_limit && cJSON_IsNumber(csv_audit_limit) && csv_audit_limit->valueint == 7);
    tf_ir_plan_free(plan);

    dsl = "jsonl on_error=warn max_error_bytes=12 max_record_bytes=16 | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    cJSON *jsonl_on_error = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "on_error");
    assert(jsonl_on_error && cJSON_IsString(jsonl_on_error));
    assert(strcmp(jsonl_on_error->valuestring, "warn") == 0);
    cJSON *jsonl_max_error_bytes = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "max_error_bytes");
    assert(jsonl_max_error_bytes && cJSON_IsNumber(jsonl_max_error_bytes) && jsonl_max_error_bytes->valueint == 12);
    cJSON *jsonl_max_record_bytes = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "max_record_bytes");
    assert(jsonl_max_record_bytes && cJSON_IsNumber(jsonl_max_record_bytes) && jsonl_max_record_bytes->valueint == 16);
    tf_ir_plan_free(plan);

    dsl = "text batch_size=2 max_error_bytes=6 max_record_bytes=17 | text";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    cJSON *text_batch_size = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "batch_size");
    assert(text_batch_size && cJSON_IsNumber(text_batch_size) && text_batch_size->valueint == 2);
    cJSON *text_max_error_bytes = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "max_error_bytes");
    assert(text_max_error_bytes && cJSON_IsNumber(text_max_error_bytes) && text_max_error_bytes->valueint == 6);
    cJSON *text_max_record_bytes = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "max_record_bytes");
    assert(text_max_record_bytes && cJSON_IsNumber(text_max_record_bytes) && text_max_record_bytes->valueint == 17);
    tf_ir_plan_free(plan);

    dsl = "csv | fill-null note=MISSING audit audit_limit=2 | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    cJSON *fill_args = plan->nodes[1].args;
    assert(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(fill_args, "audit")));
    assert(cJSON_GetObjectItemCaseSensitive(fill_args, "audit_limit")->valueint == 2);
    cJSON *fill_mapping = cJSON_GetObjectItemCaseSensitive(fill_args, "mapping");
    assert(fill_mapping && cJSON_IsObject(fill_mapping));
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(fill_mapping, "note")->valuestring, "MISSING") == 0);
    tf_ir_plan_free(plan);



    dsl = "csv | validate \"col(age) > 0\" audit audit_limit=3 name=positive_age message=bad_age | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    cJSON *validate_args = plan->nodes[1].args;
    assert(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(validate_args, "audit")));
    assert(cJSON_GetObjectItemCaseSensitive(validate_args, "audit_limit")->valueint == 3);
    assert(cJSON_GetObjectItemCaseSensitive(validate_args, "max_failures") == NULL);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(validate_args, "name")->valuestring, "positive_age") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(validate_args, "message")->valuestring, "bad_age") == 0);
    tf_ir_plan_free(plan);

    dsl = "csv | validate \"col(age) > 0\" max_failures=0 | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(error == NULL);
    validate_args = plan->nodes[1].args;
    assert(cJSON_GetObjectItemCaseSensitive(validate_args, "max_failures")->valueint == 0);
    tf_ir_plan_free(plan);

    dsl = "csv | validate \"col(age) > 0\" max_failure_rate=0.25 warn-failure-rate=0.1 | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(error == NULL);
    validate_args = plan->nodes[1].args;
    assert(cJSON_GetObjectItemCaseSensitive(validate_args, "max_failure_rate")->valuedouble == 0.25);
    assert(cJSON_GetObjectItemCaseSensitive(validate_args, "warn_failure_rate")->valuedouble == 0.1);
    tf_ir_plan_free(plan);

    dsl = "csv | quarantine \"col(age) < 25\" name=young message=too_young | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(error == NULL);
    assert(strcmp(plan->nodes[1].op, "quarantine") == 0);
    cJSON *quarantine_args = plan->nodes[1].args;
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(quarantine_args, "expr")->valuestring, "col(age) < 25") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(quarantine_args, "name")->valuestring, "young") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(quarantine_args, "message")->valuestring, "too_young") == 0);
    tf_ir_plan_free(plan);

    dsl = "csv | cast age=int on_error=null audit audit_limit=4 audit_redact=age | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    cJSON *cast_args = plan->nodes[1].args;
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(cast_args, "on_error")->valuestring, "null") == 0);
    assert(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(cast_args, "audit")));
    assert(cJSON_GetObjectItemCaseSensitive(cast_args, "audit_limit")->valueint == 4);
    cJSON *cast_redact = cJSON_GetObjectItemCaseSensitive(cast_args, "audit_redact");
    assert(cJSON_IsArray(cast_redact));
    assert(strcmp(cJSON_GetArrayItem(cast_redact, 0)->valuestring, "age") == 0);
    cJSON *cast_mapping = cJSON_GetObjectItemCaseSensitive(cast_args, "mapping");
    assert(cast_mapping && cJSON_IsObject(cast_mapping));
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(cast_mapping, "age")->valuestring, "int") == 0);
    tf_ir_plan_free(plan);

    dsl = "jsonl on_error=warn max_error_bytes=12 | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[0].op, "codec.jsonl.decode") == 0);
    cJSON *on_error = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "on_error");
    assert(on_error && cJSON_IsString(on_error));
    assert(strcmp(on_error->valuestring, "warn") == 0);
    cJSON *max_error_bytes = cJSON_GetObjectItemCaseSensitive(plan->nodes[0].args, "max_error_bytes");
    assert(max_error_bytes && cJSON_IsNumber(max_error_bytes));
    assert(max_error_bytes->valueint == 12);
    tf_ir_plan_free(plan);
}

static void test_dsl_errors(void) {
    char *error = NULL;

    /* Empty pipeline */
    tf_ir_plan *p = tf_dsl_parse("", 0, &error);
    assert(p == NULL);
    free(error); error = NULL;

    /* Filter without expression */
    p = tf_dsl_parse("csv | filter | csv", 18, &error);
    assert(p == NULL);
    assert(error != NULL);
    free(error); error = NULL;

    /* Head without number */
    p = tf_dsl_parse("csv | head | csv", 16, &error);
    assert(p == NULL);
    free(error); error = NULL;
}

static void test_dsl_expr_bare_col(void) {
    /* col(name) without quotes should work in expressions */
    tf_expr *e = tf_expr_parse("col(x) > 0");
    assert(e != NULL);
    tf_expr_free(e);

    e = tf_expr_parse("col(age) >= 25 and col(score) < 90");
    assert(e != NULL);
    tf_expr_free(e);
}

/* ================================================================
 * Expression arithmetic tests
 * ================================================================ */

static void test_expr_arithmetic_parse(void) {
    tf_expr *e = tf_expr_parse("col(a) + col(b)");
    assert(e != NULL);
    tf_expr_free(e);

    e = tf_expr_parse("col(a) * 2 + col(b) / 3");
    assert(e != NULL);
    tf_expr_free(e);

    e = tf_expr_parse("col(price) * col(qty)");
    assert(e != NULL);
    tf_expr_free(e);

    e = tf_expr_parse("(col(a) + col(b)) * 2");
    assert(e != NULL);
    tf_expr_free(e);
}

static void test_expr_arithmetic_eval_int(void) {
    tf_batch *b = tf_batch_create(2, 1);
    ASSERT_OK(tf_batch_set_schema(b, 0, "a", TF_TYPE_INT64));
    ASSERT_OK(tf_batch_set_schema(b, 1, "b", TF_TYPE_INT64));
    ASSERT_OK(tf_batch_set_int64(b, 0, 0, 10));
    ASSERT_OK(tf_batch_set_int64(b, 0, 1, 3));
    b->n_rows = 1;

    tf_eval_result val;

    tf_expr *e = tf_expr_parse("col(a) + col(b)");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &val);
    assert(val.type == TF_TYPE_INT64);
    assert(val.i == 13);
    tf_expr_free(e);

    e = tf_expr_parse("col(a) - col(b)");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &val);
    assert(val.type == TF_TYPE_INT64);
    assert(val.i == 7);
    tf_expr_free(e);

    e = tf_expr_parse("col(a) * col(b)");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &val);
    assert(val.type == TF_TYPE_INT64);
    assert(val.i == 30);
    tf_expr_free(e);

    e = tf_expr_parse("col(a) / col(b)");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &val);
    assert(val.type == TF_TYPE_FLOAT64);
    /* 10 / 3 = 3.333... */
    assert(val.f > 3.3 && val.f < 3.4);
    tf_expr_free(e);

    tf_batch_free(b);
}

static void test_expr_arithmetic_precedence(void) {
    tf_batch *b = tf_batch_create(2, 1);
    ASSERT_OK(tf_batch_set_schema(b, 0, "a", TF_TYPE_INT64));
    ASSERT_OK(tf_batch_set_schema(b, 1, "b", TF_TYPE_INT64));
    ASSERT_OK(tf_batch_set_int64(b, 0, 0, 2));
    ASSERT_OK(tf_batch_set_int64(b, 0, 1, 3));
    b->n_rows = 1;

    tf_eval_result val;

    /* 2 + 3 * 2 = 8 (not 10) */
    tf_expr *e = tf_expr_parse("col(a) + col(b) * 2");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &val);
    assert(val.type == TF_TYPE_INT64);
    assert(val.i == 8);
    tf_expr_free(e);

    /* (2 + 3) * 2 = 10 */
    e = tf_expr_parse("(col(a) + col(b)) * 2");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &val);
    assert(val.type == TF_TYPE_INT64);
    assert(val.i == 10);
    tf_expr_free(e);

    tf_batch_free(b);
}

static void test_expr_arithmetic_comparison(void) {
    /* Arithmetic in comparisons: col(a) + col(b) > 10 */
    tf_batch *b = tf_batch_create(2, 1);
    ASSERT_OK(tf_batch_set_schema(b, 0, "a", TF_TYPE_INT64));
    ASSERT_OK(tf_batch_set_schema(b, 1, "b", TF_TYPE_INT64));
    ASSERT_OK(tf_batch_set_int64(b, 0, 0, 7));
    ASSERT_OK(tf_batch_set_int64(b, 0, 1, 5));
    b->n_rows = 1;

    bool result;
    tf_expr *e = tf_expr_parse("col(a) + col(b) > 10");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &result);
    assert(result == true); /* 7 + 5 = 12 > 10 */
    tf_expr_free(e);

    e = tf_expr_parse("col(a) * col(b) < 30");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &result);
    assert(result == false); /* 7 * 5 = 35 < 30 → false */
    tf_expr_free(e);

    tf_batch_free(b);
}

static void test_expr_string_functions(void) {
    tf_batch *b = tf_batch_create(2, 1);
    ASSERT_OK(tf_batch_set_schema(b, 0, "name", TF_TYPE_STRING));
    ASSERT_OK(tf_batch_set_schema(b, 1, "age", TF_TYPE_INT64));
    ASSERT_OK(tf_batch_set_string(b, 0, 0, "Alice"));
    ASSERT_OK(tf_batch_set_int64(b, 0, 1, 30));
    b->n_rows = 1;

    tf_eval_result result;

    /* upper */
    tf_expr *e = tf_expr_parse("upper(col(name))");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_STRING);
    assert(strcmp(result.s, "ALICE") == 0);
    tf_expr_free(e);

    /* lower */
    e = tf_expr_parse("lower(col(name))");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_STRING);
    assert(strcmp(result.s, "alice") == 0);
    tf_expr_free(e);

    /* len */
    e = tf_expr_parse("len(col(name))");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_INT64);
    assert(result.i == 5);
    tf_expr_free(e);

    /* starts_with */
    bool bres;
    e = tf_expr_parse("starts_with(col(name), 'Al')");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == true);
    tf_expr_free(e);

    e = tf_expr_parse("starts_with(col(name), 'Bo')");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == false);
    tf_expr_free(e);

    /* ends_with */
    e = tf_expr_parse("ends_with(col(name), 'ce')");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == true);
    tf_expr_free(e);

    /* contains */
    e = tf_expr_parse("contains(col(name), 'lic')");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == true);
    tf_expr_free(e);

    /* slice */
    e = tf_expr_parse("slice(col(name), 0, 3)");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_STRING);
    assert(strcmp(result.s, "Ali") == 0);
    tf_expr_free(e);

    /* concat */
    e = tf_expr_parse("concat(col(name), ' is ', col(age))");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_STRING);
    assert(strcmp(result.s, "Alice is 30") == 0);
    tf_expr_free(e);

    /* pad_left */
    e = tf_expr_parse("pad_left(col(name), 8, '.')");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_STRING);
    assert(strcmp(result.s, "...Alice") == 0);
    tf_expr_free(e);

    tf_batch_free(b);
}


static void test_expr_date_functions(void) {
    tf_batch *b = tf_batch_create(3, 1);
    ASSERT_OK(tf_batch_set_schema(b, 0, "d", TF_TYPE_DATE));
    ASSERT_OK(tf_batch_set_schema(b, 1, "ts", TF_TYPE_TIMESTAMP));
    ASSERT_OK(tf_batch_set_schema(b, 2, "raw", TF_TYPE_STRING));
    ASSERT_OK(tf_batch_set_date(b, 0, 0, tf_date_from_ymd(2024, 3, 15)));
    ASSERT_OK(tf_batch_set_timestamp(b, 0, 1, tf_timestamp_from_parts(2024, 3, 15, 12, 34, 56, 789000)));
    ASSERT_OK(tf_batch_set_string(b, 0, 2, "2023-12-25T08:09:10Z"));
    b->n_rows = 1;

    tf_eval_result result;
    bool bres = false;

    tf_expr *e = tf_expr_parse("year(col(d))");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_INT64 && result.i == 2024);
    tf_expr_free(e);

    e = tf_expr_parse("month(col(d)) == 3 and day(col(d)) == 15 and weekday(col(d)) == 5");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == true);
    tf_expr_free(e);

    e = tf_expr_parse("hour(col(ts)) == 12 and minute(col(ts)) == 34 and second(col(ts)) == 56");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == true);
    tf_expr_free(e);

    e = tf_expr_parse("epoch(col(d))");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_INT64 && result.i == (int64_t)tf_date_from_ymd(2024, 3, 15) * 86400LL);
    tf_expr_free(e);

    e = tf_expr_parse("date_trunc(col(d), 'month')");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_DATE && result.date == tf_date_from_ymd(2024, 3, 1));
    tf_expr_free(e);

    e = tf_expr_parse("date_trunc(col(ts), 'hour')");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_TIMESTAMP && result.i == tf_timestamp_from_parts(2024, 3, 15, 12, 0, 0, 0));
    tf_expr_free(e);

    e = tf_expr_parse("date_trunc(col(raw), 'day')");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_STRING && strcmp(result.s, "2023-12-25T00:00:00Z") == 0);
    tf_expr_free(e);

    e = tf_expr_parse("year('2023-12-25') == 2023 and month('2023-12-25') == 12");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == true);
    tf_expr_free(e);

    tf_batch_free(b);
}

static void test_expr_conditional_functions(void) {
    tf_batch *b = tf_batch_create(2, 1);
    ASSERT_OK(tf_batch_set_schema(b, 0, "age", TF_TYPE_INT64));
    ASSERT_OK(tf_batch_set_schema(b, 1, "name", TF_TYPE_STRING));
    ASSERT_OK(tf_batch_set_int64(b, 0, 0, 30));
    ASSERT_OK(tf_batch_set_string(b, 0, 1, "Alice"));
    b->n_rows = 1;

    tf_eval_result result;

    /* if(cond, then, else) */
    tf_expr *e = tf_expr_parse("if(col(age) > 25, 'adult', 'young')");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_STRING);
    assert(strcmp(result.s, "adult") == 0);
    tf_expr_free(e);

    e = tf_expr_parse("if(col(age) > 50, 'old', 'not old')");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_STRING);
    assert(strcmp(result.s, "not old") == 0);
    tf_expr_free(e);

    /* coalesce */
    e = tf_expr_parse("coalesce(col(missing), col(name), 'default')");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_STRING);
    assert(strcmp(result.s, "Alice") == 0);
    tf_expr_free(e);

    /* between / inrange */
    bool bres = false;
    e = tf_expr_parse("between(col(age), 25, 30)");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == true);
    tf_expr_free(e);

    e = tf_expr_parse("between(col(age), 31, 40)");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == false);
    tf_expr_free(e);

    e = tf_expr_parse("inrange(col(name), 'A', 'M')");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == true);
    tf_expr_free(e);

    /* case_when */
    e = tf_expr_parse("case_when(col(age) < 18, 'minor', col(age) < 65, 'adult', 'senior')");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_STRING);
    assert(strcmp(result.s, "adult") == 0);
    tf_expr_free(e);

    e = tf_expr_parse("case_when(col(age) < 18, 'minor', col(age) > 65, 'senior')");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_NULL);
    tf_expr_free(e);

    /* case_match */
    e = tf_expr_parse("case_match(col(name), 'Alice', 'A', 'Bob', 'B', 'other')");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_STRING);
    assert(strcmp(result.s, "A") == 0);
    tf_expr_free(e);

    e = tf_expr_parse("case_match(col(name), 'Cara', 'C')");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_NULL);
    tf_expr_free(e);

    /* if_any / if_all */
    e = tf_expr_parse("if_any(col(age) < 25, contains(col(name), 'li'))");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == true);
    tf_expr_free(e);

    e = tf_expr_parse("if_any(col(age) < 25, col(age) > 40)");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == false);
    tf_expr_free(e);

    e = tf_expr_parse("if_all(col(age) > 25, starts_with(col(name), 'A'))");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == true);
    tf_expr_free(e);

    e = tf_expr_parse("if_all(col(age) > 25, col(age) > 40)");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == false);
    tf_expr_free(e);

    e = tf_expr_parse("if_any()");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == false);
    tf_expr_free(e);

    e = tf_expr_parse("if_all()");
    assert(e != NULL);
    tf_expr_eval(e, b, 0, &bres);
    assert(bres == true);
    tf_expr_free(e);

    /* Math: abs, round, min, max */
    e = tf_expr_parse("abs(-5)");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_INT64);
    assert(result.i == 5);
    tf_expr_free(e);

    e = tf_expr_parse("max(col(age), 50)");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_INT64);
    assert(result.i == 50);
    tf_expr_free(e);

    e = tf_expr_parse("min(col(age), 50)");
    assert(e != NULL);
    tf_expr_eval_val(e, b, 0, &result);
    assert(result.type == TF_TYPE_INT64);
    assert(result.i == 30);
    tf_expr_free(e);

    tf_batch_free(b);
}


static void test_pipeline_derive_date_funcs(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"year(col(d)) == 2024\"}},"
        "{\"op\":\"derive\",\"args\":{\"columns\":["
        "{\"name\":\"d_year\",\"expr\":\"year(col(d))\"},"
        "{\"name\":\"d_month\",\"expr\":\"month(col(d))\"},"
        "{\"name\":\"month_start\",\"expr\":\"date_trunc(col(d), 'month')\"},"
        "{\"name\":\"hour_start\",\"expr\":\"date_trunc(col(ts), 'hour')\"}"
        "]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv =
        "d,ts\n"
        "2024-03-15,2024-03-15T12:34:56Z\n"
        "2023-12-25,2023-12-25T08:09:10Z\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "d,ts,d_year,d_month,month_start,hour_start") != NULL);
    assert(strstr((char *)out, "2024-03-15,2024-03-15T12:34:56Z,2024,3,2024-03-01,2024-03-15T12:00:00Z") != NULL);
    assert(strstr((char *)out, "2023-12-25") == NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_derive_string_funcs(void) {
    /* Test string functions end-to-end through derive */
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"derive\",\"args\":{\"columns\":["
        "{\"name\":\"upper_name\",\"expr\":\"upper(col(name))\"},"
        "{\"name\":\"name_len\",\"expr\":\"len(col(name))\"},"
        "{\"name\":\"label\",\"expr\":\"if(col(age) > 25, 'senior', 'junior')\"}"
        "]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,age\nAlice,30\nBob,20\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "ALICE") != NULL);
    assert(strstr((char *)out, "BOB") != NULL);
    assert(strstr((char *)out, "senior") != NULL);
    assert(strstr((char *)out, "junior") != NULL);
    tf_pipeline_free(p);
}

/* ================================================================
 * Pipeline tests for new transforms
 * ================================================================ */

static void test_pipeline_csv_skip(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"skip\",\"args\":{\"n\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name,age\nAlice,30\nBob,25\nCharlie,35\nDiana,28\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* Should have Charlie and Diana but not Alice or Bob */
    assert(strstr((char *)out, "Charlie") != NULL);
    assert(strstr((char *)out, "Diana") != NULL);
    assert(strstr((char *)out, "Alice") == NULL);
    assert(strstr((char *)out, "Bob") == NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_derive(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"derive\",\"args\":{\"columns\":[{\"name\":\"total\",\"expr\":\"col(price)*col(qty)\"}]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "price,qty\n10,3\n20,5\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* Should have total column with 30 and 100 */
    assert(strstr((char *)out, "total") != NULL);
    assert(strstr((char *)out, "30") != NULL);
    assert(strstr((char *)out, "100") != NULL);

    tf_pipeline_free(p);
}


static void test_pipeline_csv_across(void) {
    const char *round_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"across\",\"args\":{\"columns\":[\"starts_with(score_)\"],\"fn\":\"round\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(round_plan, strlen(round_plan));
    assert(p != NULL);
    const char *csv = "id,score_math,score_read,name\n1,90.4,80.6,Alice\n2,70.2,95.8,Bob\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "id,score_math,score_read,name\n") != NULL);
    assert(strstr((char *)out, "1,90,81,Alice") != NULL);
    assert(strstr((char *)out, "2,70,96,Bob") != NULL);
    tf_pipeline_free(p);

    const char *append_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"across\",\"args\":{\"columns\":[\"name\"],\"functions\":[\"lower\"],\"replace\":false,\"names\":\"{col}_{fn}\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(append_plan, strlen(append_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)"name,score\nAlice,1\nBOB,2\n", strlen("name,score\nAlice,1\nBOB,2\n")) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "name,score,name_lower\n") != NULL);
    assert(strstr((char *)out, "Alice,1,alice") != NULL);
    assert(strstr((char *)out, "BOB,2,bob") != NULL);
    tf_pipeline_free(p);

    const char *bad_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"across\",\"args\":{\"columns\":[\"score\"],\"fn\":\"lower\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(bad_plan, strlen(bad_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)"score\n1\n", strlen("score\n1\n")) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_ERROR);
    tf_pipeline_free(p);
}

static void test_pipeline_csv_stats(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"stats\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name,age\nAlice,30\nBob,25\nCharlie,35\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* Should have column names and new default stats in output */
    assert(strstr((char *)out, "column") != NULL);
    assert(strstr((char *)out, "count") != NULL);
    assert(strstr((char *)out, "var") != NULL);
    assert(strstr((char *)out, "stddev") != NULL);
    assert(strstr((char *)out, "median") != NULL);
    assert(strstr((char *)out, "name") != NULL);
    assert(strstr((char *)out, "age") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_stats_advanced(void) {
    /* Test variance, stddev, median with selective stats */
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"stats\",\"args\":{\"stats\":[\"count\",\"var\",\"stddev\",\"median\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "val\n10\n20\n30\n40\n50\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* Check header has var, stddev, median */
    assert(strstr((char *)out, "column,count,var,stddev,median") != NULL);
    /* Variance of {10,20,30,40,50} = 250, stddev ~15.811 */
    assert(strstr((char *)out, "val,5,250") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_stats_distinct(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"stats\",\"args\":{\"stats\":[\"count\",\"distinct\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name\nAlice\nBob\nAlice\nCharlie\nBob\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* 5 total, 3 distinct */
    assert(strstr((char *)out, "name,5,3") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_stats_distinct_large(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"stats\",\"args\":{\"stats\":[\"count\",\"distinct\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const int n_rows = 5000;
    size_t cap = 16 + (size_t)n_rows * 16;
    char *csv = malloc(cap);
    assert(csv != NULL);
    size_t len = (size_t)snprintf(csv, cap, "x\n");
    for (int i = 0; i < n_rows; i++) {
        len += (size_t)snprintf(csv + len, cap - len, "v%d\n", i);
    }

    assert(tf_pipeline_push(p, (const uint8_t *)csv, len) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';

    char *row = strstr((char *)out, "x,5000,");
    assert(row != NULL);
    long estimate = strtol(row + strlen("x,5000,"), NULL, 10);
    assert(estimate > 4000);
    assert(estimate < 6000);

    free(csv);
    tf_pipeline_free(p);
}

static void test_pipeline_csv_stats_hist_sample(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"stats\",\"args\":{\"stats\":[\"distinct\",\"hist\",\"sample\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "val\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* distinct, hist, and sample each allocate bounded online state. */
    assert(strstr((char *)out, "column,distinct,hist,sample") != NULL);
    /* hist output contains colons for "lo:hi:counts" */
    char *data_line = strstr((char *)out, "\nval,");
    assert(data_line != NULL);
    assert(strstr(data_line, "10,") != NULL);
    assert(strstr(data_line, ":") != NULL); /* hist has colons */

    tf_pipeline_free(p);
}


static void test_pipeline_csv_scan(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"scan\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name,age\nAlice,30\nBob,\nAlice,35\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);

    uint8_t out[4096];
    assert(tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out)) == 0);
    assert(tf_pipeline_finish(p) == TF_OK);

    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';

    assert(strstr((char *)out, "column,count,missing,complete_rate") != NULL);
    assert(strstr((char *)out, "distinct") != NULL);
    assert(strstr((char *)out, "hist") != NULL);
    assert(strstr((char *)out, "sample") != NULL);
    assert(strstr((char *)out, "name") != NULL);
    assert(strstr((char *)out, "age") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_stats_missing(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"stats\",\"args\":{\"stats\":[\"count\",\"missing\",\"complete_rate\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "x,y\n1,a\n,b\n3,\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';

    assert(strstr((char *)out, "column,count,missing,complete_rate") != NULL);
    assert(strstr((char *)out, "x,2,1") != NULL);
    assert(strstr((char *)out, "y,2,1") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_unique(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"unique\",\"args\":{\"columns\":[\"name\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name,age\nAlice,30\nBob,25\nAlice,35\nCharlie,28\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* Alice should appear once (first occurrence), Bob and Charlie once */
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);
    assert(strstr((char *)out, "Charlie") != NULL);
    /* Second Alice (age 35) should be deduplicated */
    /* Count occurrences of "Alice" */
    int alice_count = 0;
    const char *search = (char *)out;
    while ((search = strstr(search, "Alice")) != NULL) {
        alice_count++;
        search++;
    }
    assert(alice_count == 1);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_unique_approx(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"unique\",\"args\":{\"columns\":[\"city\"],\"mode\":\"approx\",\"bloom_bytes\":65536,\"bloom_hashes\":4}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "city,score\nNY,1\nLA,2\nNY,3\nSF,4\nLA,5\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';

    assert(strstr((char *)out, "NY,1") != NULL);
    assert(strstr((char *)out, "LA,2") != NULL);
    assert(strstr((char *)out, "SF,4") != NULL);
    assert(strstr((char *)out, "NY,3") == NULL);
    assert(strstr((char *)out, "LA,5") == NULL);

    uint8_t stats[2048];
    n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = '\0';
    assert(strstr((char *)stats, "\"approximate\":true") != NULL);
    assert(strstr((char *)stats, "\"bloom_bytes\":65536") != NULL);
    assert(strstr((char *)stats, "\"bloom_hashes\":4") != NULL);
    assert(strstr((char *)stats, "\"approx_inserted\":3") != NULL);
    assert(strstr((char *)stats, "\"approx_filtered\":2") != NULL);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_sort(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"sort\",\"args\":{\"columns\":[{\"name\":\"age\",\"desc\":false}]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name,age\nCharlie,35\nAlice,30\nBob,25\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* Should be sorted by age ascending: Bob(25), Alice(30), Charlie(35) */
    char *bob_pos = strstr((char *)out, "Bob");
    char *alice_pos = strstr((char *)out, "Alice");
    char *charlie_pos = strstr((char *)out, "Charlie");
    assert(bob_pos != NULL && alice_pos != NULL && charlie_pos != NULL);
    assert(bob_pos < alice_pos);
    assert(alice_pos < charlie_pos);

    tf_pipeline_free(p);

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"sort\",\"args\":{\"columns\":[{\"name\":\"\"}]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "sort: column names must be non-empty strings");

    const char *missing_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"sort\",\"args\":{\"columns\":[{\"name\":\"missing\"}]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(missing_plan, strlen(missing_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_ERROR);
    const char *err = tf_pipeline_error(p);
    assert(err && strstr(err, "sort: column 'missing' not found") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_csv_sort_desc(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"sort\",\"args\":{\"columns\":[{\"name\":\"age\",\"desc\":true}]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name,age\nAlice,30\nBob,25\nCharlie,35\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* Should be sorted by age descending: Charlie(35), Alice(30), Bob(25) */
    char *bob_pos = strstr((char *)out, "Bob");
    char *alice_pos = strstr((char *)out, "Alice");
    char *charlie_pos = strstr((char *)out, "Charlie");
    assert(bob_pos != NULL && alice_pos != NULL && charlie_pos != NULL);
    assert(charlie_pos < alice_pos);
    assert(alice_pos < bob_pos);

    tf_pipeline_free(p);
}

static void test_pipeline_csv_sort_stable_equal_keys(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"sort\",\"args\":{\"columns\":[{\"name\":\"age\",\"desc\":false}]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv =
        "name,city,age\n"
        "Alice,NY,30\n"
        "Bob,LA,25\n"
        "Eve,SF,30\n"
        "Diana,CHI,25\n"
        "Grace,LA,30\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    char *s = (char *)out;

    const char *rows[] = {
        "Bob,LA,25",
        "Diana,CHI,25",
        "Alice,NY,30",
        "Eve,SF,30",
        "Grace,LA,30"
    };
    char *prev = strstr(s, "name,city,age");
    assert(prev != NULL);
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        char *pos = strstr(s, rows[i]);
        assert(pos != NULL);
        assert(prev < pos);
        prev = pos;
    }

    tf_pipeline_free(p);
}

static void test_pipeline_skip_head_combo(void) {
    /* skip 2 | head 2 should give rows 3-4 */
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"skip\",\"args\":{\"n\":2}},"
        "{\"op\":\"head\",\"args\":{\"n\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv = "name\nA\nB\nC\nD\nE\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';

    /* Should have C and D only */
    assert(strstr((char *)out, "\nC\n") != NULL || strstr((char *)out, ",C") != NULL
           || strstr((char *)out, "C\n") != NULL);
    assert(strstr((char *)out, "\nD\n") != NULL || strstr((char *)out, ",D") != NULL
           || strstr((char *)out, "D\n") != NULL);
    assert(strstr((char *)out, "\nA\n") == NULL);
    assert(strstr((char *)out, "\nB\n") == NULL);
    assert(strstr((char *)out, "\nE\n") == NULL);

    tf_pipeline_free(p);
}

/* ================================================================
 * DSL tests for new transforms
 * ================================================================ */

static void test_dsl_skip(void) {
    const char *dsl = "csv | skip 10 | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "skip") == 0);
    cJSON *n = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "n");
    assert(n && cJSON_IsNumber(n));
    assert(n->valuedouble == 10);
    tf_ir_plan_free(plan);
}

static void test_dsl_derive(void) {
    const char *dsl = "csv | derive total=col(price)*col(qty) | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "derive") == 0);
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    assert(cols && cJSON_IsArray(cols));
    assert(cJSON_GetArraySize(cols) == 1);
    cJSON *first = cJSON_GetArrayItem(cols, 0);
    cJSON *name = cJSON_GetObjectItemCaseSensitive(first, "name");
    cJSON *expr = cJSON_GetObjectItemCaseSensitive(first, "expr");
    assert(name && strcmp(name->valuestring, "total") == 0);
    assert(expr && strcmp(expr->valuestring, "col(price)*col(qty)") == 0);
    tf_ir_plan_free(plan);
}


static void test_dsl_across(void) {
    const char *dsl = "csv | across starts_with(score_) round | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "across") == 0);
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    cJSON *fns = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "functions");
    assert(cols && cJSON_IsArray(cols) && cJSON_GetArraySize(cols) == 1);
    assert(fns && cJSON_IsArray(fns) && cJSON_GetArraySize(fns) == 1);
    assert(strcmp(cJSON_GetArrayItem(cols, 0)->valuestring, "starts_with(score_)") == 0);
    assert(strcmp(cJSON_GetArrayItem(fns, 0)->valuestring, "round") == 0);
    tf_ir_plan_free(plan);

    dsl = "csv | across columns=name fn=lower replace=false names={col}_{fn} | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "across") == 0);
    cJSON *replace = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "replace");
    cJSON *names = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "names");
    assert(replace && cJSON_IsFalse(replace));
    assert(names && strcmp(names->valuestring, "{col}_{fn}") == 0);
    tf_ir_plan_free(plan);
}

static void test_dsl_stats(void) {
    const char *dsl = "csv | stats | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "stats") == 0);
    tf_ir_plan_free(plan);
}

static void test_dsl_stats_selective(void) {
    const char *dsl = "csv | stats count,sum | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "stats") == 0);
    cJSON *stats = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "stats");
    assert(stats && cJSON_IsArray(stats));
    assert(cJSON_GetArraySize(stats) == 2);
    tf_ir_plan_free(plan);
}

static void test_dsl_scan(void) {
    const char *dsl = "csv | scan | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "scan") == 0);
    tf_ir_plan_free(plan);
}

static void test_dsl_scan_selective(void) {
    const char *dsl = "csv | scan count,missing,complete_rate | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "scan") == 0);
    cJSON *stats = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "stats");
    assert(stats && cJSON_IsArray(stats));
    assert(cJSON_GetArraySize(stats) == 3);
    tf_ir_plan_free(plan);
}

static void test_dsl_schema_infer(void) {
    const char *dsl = "csv | schema infer rows=25 | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "schema-infer") == 0);
    cJSON *rows = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "rows");
    assert(cJSON_IsNumber(rows));
    assert(rows->valueint == 25);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | schema-infer guess_max=7 | csv",
                        strlen("csv | schema-infer guess_max=7 | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "schema-infer") == 0);
    rows = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "rows");
    assert(cJSON_IsNumber(rows));
    assert(rows->valueint == 7);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | schema infer rows=0 | csv",
                        strlen("csv | schema infer rows=0 | csv"), &error);
    assert(plan == NULL);
    assert(error != NULL);
    assert(strstr(error, "schema infer") != NULL);
    free(error);
}


static void test_dsl_tee(void) {
    const char *dsl = "csv | tee \"col(age) >= 30\" channel=samples columns=name,age limit=2 every=3 name=age_sample | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "tee") == 0);
    cJSON *expr = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "expr");
    assert(cJSON_IsString(expr) && strcmp(expr->valuestring, "col(age) >= 30") == 0);
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    assert(cols && cJSON_IsArray(cols) && cJSON_GetArraySize(cols) == 2);
    cJSON *limit = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "limit");
    assert(cJSON_IsNumber(limit) && limit->valueint == 2);
    cJSON *every = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "every");
    assert(cJSON_IsNumber(every) && every->valueint == 3);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | tee limit=0 | csv", strlen("csv | tee limit=0 | csv"), &error);
    assert(plan == NULL);
    assert(error != NULL && strstr(error, "positive integer") != NULL);
    free(error);
}

static void test_dsl_unique(void) {
    const char *dsl = "csv | unique name,city | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "unique") == 0);
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    assert(cols && cJSON_IsArray(cols));
    assert(cJSON_GetArraySize(cols) == 2);
    tf_ir_plan_free(plan);

    const char *sorted_dsl = "csv | unique name sorted=true | csv";
    plan = tf_dsl_parse(sorted_dsl, strlen(sorted_dsl), &error);
    assert(plan != NULL);
    cJSON *sorted = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "sorted");
    assert(cJSON_IsBool(sorted) && cJSON_IsTrue(sorted));
    tf_ir_plan_free(plan);

    const char *dedup_sorted_dsl = "csv | dedup name --sorted | csv";
    plan = tf_dsl_parse(dedup_sorted_dsl, strlen(dedup_sorted_dsl), &error);
    assert(plan != NULL);
    sorted = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "sorted");
    assert(cJSON_IsBool(sorted) && cJSON_IsTrue(sorted));
    tf_ir_plan_free(plan);

    const char *approx_dsl = "csv | unique city mode=approx bloom_bytes=65536 bloom_hashes=4 | csv";
    plan = tf_dsl_parse(approx_dsl, strlen(approx_dsl), &error);
    assert(plan != NULL);
    cJSON *mode = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "mode");
    cJSON *bloom_bytes = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "bloom_bytes");
    cJSON *bloom_hashes = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "bloom_hashes");
    assert(cJSON_IsString(mode) && strcmp(mode->valuestring, "approx") == 0);
    assert(cJSON_IsNumber(bloom_bytes) && (int)bloom_bytes->valuedouble == 65536);
    assert(cJSON_IsNumber(bloom_hashes) && (int)bloom_hashes->valuedouble == 4);
    tf_ir_plan_free(plan);

    const char *approx_bool_dsl = "csv | dedup city --approx | csv";
    plan = tf_dsl_parse(approx_bool_dsl, strlen(approx_bool_dsl), &error);
    assert(plan != NULL);
    cJSON *approx = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "approx");
    assert(cJSON_IsBool(approx) && cJSON_IsTrue(approx));
    tf_ir_plan_free(plan);
}

static void test_dsl_sort(void) {
    const char *dsl = "csv | sort age | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "sort") == 0);
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    assert(cols && cJSON_IsArray(cols));
    assert(cJSON_GetArraySize(cols) == 1);
    cJSON *first = cJSON_GetArrayItem(cols, 0);
    cJSON *name = cJSON_GetObjectItemCaseSensitive(first, "name");
    cJSON *desc = cJSON_GetObjectItemCaseSensitive(first, "desc");
    assert(name && strcmp(name->valuestring, "age") == 0);
    assert(desc && cJSON_IsFalse(desc));
    tf_ir_plan_free(plan);
}

static void test_dsl_sort_desc(void) {
    const char *dsl = "csv | sort -age | csv";
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    cJSON *first = cJSON_GetArrayItem(cols, 0);
    cJSON *name = cJSON_GetObjectItemCaseSensitive(first, "name");
    cJSON *desc = cJSON_GetObjectItemCaseSensitive(first, "desc");
    assert(name && strcmp(name->valuestring, "age") == 0);
    assert(desc && cJSON_IsTrue(desc));
    tf_ir_plan_free(plan);
}

/* ================================================================
 * Registry tests for new ops
 * ================================================================ */

/* ================================================================
 * Tests for new operators
 * ================================================================ */

static void test_pipeline_tail(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"tail\",\"args\":{\"n\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name\nAlice\nBob\nCharlie\nDiana\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Charlie") != NULL);
    assert(strstr((char *)out, "Diana") != NULL);
    assert(strstr((char *)out, "Alice") == NULL);
    assert(strstr((char *)out, "Bob") == NULL);
    tf_pipeline_free(p);
}


static void test_pipeline_fill_null_audit_side_channel(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2,\"nulls\":\"NA\"}},"
        "{\"op\":\"fill-null\",\"args\":{\"mapping\":{\"note\":\"MISSING\"},\"audit\":true,\"audit_limit\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,note\nA,ok\nB,NA\nC,\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "B,MISSING") != NULL);
    assert(strstr((char *)out, "C,MISSING") != NULL);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"type\":\"audit\"") != NULL);
    assert(strstr((char *)stats, "\"op\":\"fill-null\"") != NULL);
    assert(strstr((char *)stats, "\"event\":\"null_filled\"") != NULL);
    assert(strstr((char *)stats, "\"column\":\"note\"") != NULL);
    assert(strstr((char *)stats, "\"row\":2") != NULL);
    assert(strstr((char *)stats, "\"before\":null") != NULL);
    assert(strstr((char *)stats, "\"after\":\"MISSING\"") != NULL);
    assert(strstr((char *)stats, "\"row\":3") == NULL);
    tf_pipeline_free(p);
}


static void test_pipeline_cast_audit_side_channel(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"cast\",\"args\":{\"mapping\":{\"age\":\"int\"},\"audit\":true,\"audit_limit\":3}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "age\n10\nbad\n12x\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "10") != NULL);
    assert(strstr((char *)out, "0") != NULL);
    assert(strstr((char *)out, "12") != NULL);

    uint8_t stats[8192];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"type\":\"audit\"") != NULL);
    assert(strstr((char *)stats, "\"op\":\"cast\"") != NULL);
    assert(strstr((char *)stats, "\"event\":\"value_changed\"") != NULL);
    assert(strstr((char *)stats, "\"event\":\"coercion_failed\"") != NULL);
    assert(strstr((char *)stats, "\"reason\":\"invalid_integer\"") != NULL);
    assert(strstr((char *)stats, "\"reason\":\"trailing_characters\"") != NULL);
    assert(strstr((char *)stats, "\"column\":\"age\"") != NULL);
    assert(strstr((char *)stats, "\"from_type\":\"string\"") != NULL);
    assert(strstr((char *)stats, "\"to_type\":\"int\"") != NULL);
    assert(strstr((char *)stats, "\"expected\":\"int\"") != NULL);
    assert(strstr((char *)stats, "\"on_error\":\"coerce\"") != NULL);
    assert(strstr((char *)stats, "\"actual\":\"bad\"") != NULL);
    assert(strstr((char *)stats, "\"before\":\"bad\"") != NULL);
    assert(strstr((char *)stats, "\"after\":0") != NULL);
    assert(strstr((char *)stats, "\"coercion_failures\":2") != NULL);
    assert(strstr((char *)stats, "\"coercion_nulled\":0") != NULL);
    assert(strstr((char *)stats, "\"audit_emitted\":3") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_cast_on_error_policies(void) {
    const char *null_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"cast\",\"args\":{\"mapping\":{\"age\":\"int\"},\"on_error\":\"null\",\"audit\":true,\"audit_limit\":3}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(null_plan, strlen(null_plan));
    assert(p != NULL);
    const char *csv = "age\n10\nbad\n12x\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "\n10\n") != NULL);
    assert(strstr((char *)out, "\n0\n") == NULL);
    assert(strstr((char *)out, "\n12\n") == NULL);

    uint8_t stats[8192];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"on_error\":\"null\"") != NULL);
    assert(strstr((char *)stats, "\"event\":\"coercion_failed\"") != NULL);
    assert(strstr((char *)stats, "\"after\":null") != NULL);
    assert(strstr((char *)stats, "\"coercion_failures\":2") != NULL);
    assert(strstr((char *)stats, "\"coercion_nulled\":2") != NULL);
    tf_pipeline_free(p);

    const char *fail_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"cast\",\"args\":{\"mapping\":{\"age\":\"int\"},\"on_error\":\"fail\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(fail_plan, strlen(fail_plan));
    assert(p != NULL);
    const char *bad = "age\nbad\n";
    assert(tf_pipeline_push(p, (const uint8_t *)bad, strlen(bad)) == TF_ERROR);
    assert(tf_pipeline_error(p) != NULL);
    assert(strstr(tf_pipeline_error(p), "cast failed at row 1 column 'age': invalid_integer") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_clip(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"clip\",\"args\":{\"column\":\"val\",\"min\":0,\"max\":10}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "val\n-5\n5\n15\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* -5 clipped to 0, 5 stays, 15 clipped to 10 */
    assert(strstr((char *)out, "\n0\n") != NULL);
    assert(strstr((char *)out, "\n5\n") != NULL);
    assert(strstr((char *)out, "\n10\n") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_replace(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"replace\",\"args\":{\"column\":\"name\",\"pattern\":\"Alice\",\"replacement\":\"Alicia\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name\nAlice\nBob\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alicia") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);
    tf_pipeline_free(p);
}


static void test_pipeline_replace_audit_side_channel(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"replace\",\"args\":{\"column\":\"name\",\"pattern\":\"Alice\",\"replacement\":\"Alicia\",\"audit\":true,\"audit_limit\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name\nAlice\nAlice Jones\nBob\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alicia") != NULL);
    assert(strstr((char *)out, "Alicia Jones") != NULL);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"type\":\"audit\"") != NULL);
    assert(strstr((char *)stats, "\"op\":\"replace\"") != NULL);
    assert(strstr((char *)stats, "\"event\":\"value_changed\"") != NULL);
    assert(strstr((char *)stats, "\"reason\":\"replace_match\"") != NULL);
    assert(strstr((char *)stats, "\"column\":\"name\"") != NULL);
    assert(strstr((char *)stats, "\"row\":1") != NULL);
    assert(strstr((char *)stats, "\"before\":\"Alice\"") != NULL);
    assert(strstr((char *)stats, "\"after\":\"Alicia\"") != NULL);
    assert(strstr((char *)stats, "\"row\":2") == NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_explode(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"explode\",\"args\":{\"column\":\"tags\",\"delimiter\":\",\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,tags\nAlice,a;b;c\nBob,x\n";
    /* Use semicolons first, then test with comma */
    tf_pipeline_free(p);

    /* Retry with semicolons as delimiter */
    const char *plan2 =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"explode\",\"args\":{\"column\":\"tags\",\"delimiter\":\";\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(plan2, strlen(plan2));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* Alice should appear 3 times (a, b, c), Bob once */
    int alice_count = 0;
    const char *search = (char *)out;
    while ((search = strstr(search, "Alice")) != NULL) { alice_count++; search++; }
    assert(alice_count == 3);
    tf_pipeline_free(p);
}

static void test_pipeline_explode_unpivot_caps(void) {
    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"explode\",\"args\":{\"column\":\"tags\",\"max_tokens_per_row\":0}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "max_tokens_per_row");

    const char *explode_tokens_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"explode\",\"args\":{\"column\":\"tags\",\"delimiter\":\"|\",\"max_tokens_per_row\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(explode_tokens_plan, "name,tags\nAlice,a|b|c\n", "max_tokens_per_row=2");

    const char *explode_token_bytes_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"explode\",\"args\":{\"column\":\"tags\",\"max_token_bytes\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(explode_token_bytes_plan, "name,tags\nAlice,aa\n", "max_token_bytes=1");

    const char *explode_batch_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"explode\",\"args\":{\"column\":\"tags\",\"delimiter\":\"|\",\"max_output_rows_per_batch\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(explode_batch_plan, "name,tags\nAlice,a|b\n", "max_output_rows_per_batch=1");

    const char *unpivot_row_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"unpivot\",\"args\":{\"columns\":[\"q1\",\"q2\"],\"max_output_rows_per_input_row\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(unpivot_row_plan, "id,q1,q2\n1,10,20\n", "max_output_rows_per_input_row=1");

    const char *unpivot_batch_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"unpivot\",\"args\":{\"columns\":[\"q1\",\"q2\"],\"max_output_rows_per_batch\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    assert_push_cap_error(unpivot_batch_plan, "id,q1,q2\n1,10,20\n2,30,40\n", "max_output_rows_per_batch=2");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"unpivot\",\"args\":{\"columns\":[]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "unpivot: columns must be a non-empty array");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"unpivot\",\"args\":{\"columns\":[123]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "unpivot: column names must be non-empty strings");

    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(
        "csv | explode tags max_tokens_per_row=2 max_token_bytes=8 | csv",
        strlen("csv | explode tags max_tokens_per_row=2 max_token_bytes=8 | csv"), &error);
    assert(plan != NULL && error == NULL);
    cJSON *max_tokens = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_tokens_per_row");
    cJSON *max_bytes = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_token_bytes");
    assert(cJSON_IsNumber(max_tokens) && max_tokens->valueint == 2);
    assert(cJSON_IsNumber(max_bytes) && max_bytes->valueint == 8);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse(
        "csv | unpivot q1,q2 max_output_rows_per_batch=10 | csv",
        strlen("csv | unpivot q1,q2 max_output_rows_per_batch=10 | csv"), &error);
    assert(plan != NULL && error == NULL);
    cJSON *max_rows = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_output_rows_per_batch");
    assert(cJSON_IsNumber(max_rows) && max_rows->valueint == 10);
    tf_ir_plan_free(plan);
}

static void test_pipeline_trim(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.jsonl.decode\",\"args\":{}},"
        "{\"op\":\"trim\",\"args\":{}},"
        "{\"op\":\"codec.jsonl.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *jsonl = "{\"name\":\"  Alice  \"}\n{\"name\":\"Bob\"}\n";
    assert(tf_pipeline_push(p, (const uint8_t *)jsonl, strlen(jsonl)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* "  Alice  " should become "Alice" */
    assert(strstr((char *)out, "\"Alice\"") != NULL);
    assert(strstr((char *)out, "  Alice  ") == NULL);
    tf_pipeline_free(p);

    const size_t long_len = 5000;
    char *long_jsonl = malloc(long_len + 32);
    assert(long_jsonl != NULL);
    size_t pos = 0;
    memcpy(long_jsonl + pos, "{\"name\":\"   ", 12); pos += 12;
    memset(long_jsonl + pos, 'x', long_len); pos += long_len;
    memcpy(long_jsonl + pos, "   \"}\n", 6); pos += 6;

    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)long_jsonl, pos) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t *long_out = malloc(long_len + 128);
    assert(long_out != NULL);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, long_out, long_len + 127);
    assert(n > long_len);
    long_out[n] = '\0';
    size_t x_count = 0;
    for (size_t i = 0; i < n; i++) {
        if (long_out[i] == 'x') x_count++;
    }
    assert(x_count == long_len);
    assert(strstr((char *)long_out, "   ") == NULL);
    free(long_out);
    tf_pipeline_free(p);
    free(long_jsonl);

    const char *bad_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"trim\",\"args\":{\"columns\":[123]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(bad_plan, strlen(bad_plan));
    assert(p == NULL);
    const char *err = tf_last_error();
    assert(err && strstr(err, "trim: column names must be non-empty strings") != NULL);
}

static void test_pipeline_hash(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"hash\",\"args\":{\"columns\":[\"name\",\"city\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,city\nAlice,NY\nBob,LA\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[512];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "name,city,_hash") != NULL);
    assert(strstr((char *)out, "Alice,NY,") != NULL);
    assert(strstr((char *)out, "Bob,LA,") != NULL);
    tf_pipeline_free(p);

    const char *bad_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"hash\",\"args\":{\"columns\":[123]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(bad_plan, strlen(bad_plan));
    assert(p == NULL);
    const char *err = tf_last_error();
    assert(err && strstr(err, "hash: column names must be non-empty strings") != NULL);
}

static void test_pipeline_validate(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"validate\",\"args\":{\"expr\":\"col('age') > 25\",\"audit\":true,\"audit_limit\":1,\"name\":\"age_check\",\"message\":\"age too low\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,age\nAlice,30\nBob,20\nCara,10\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* All rows should be present, with _valid column */
    assert(strstr((char *)out, "_valid") != NULL);
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);
    assert(strstr((char *)out, "Cara") != NULL);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"type\":\"audit\"") != NULL);
    assert(strstr((char *)stats, "\"op\":\"validate\"") != NULL);
    assert(strstr((char *)stats, "\"event\":\"validation_failed\"") != NULL);
    assert(strstr((char *)stats, "\"reason\":\"validate_false\"") != NULL);
    assert(strstr((char *)stats, "\"action\":\"annotate\"") != NULL);
    assert(strstr((char *)stats, "\"checked_rows\":3") != NULL);
    assert(strstr((char *)stats, "\"passed_rows\":1") != NULL);
    assert(strstr((char *)stats, "\"failed_rows\":2") != NULL);
    assert(strstr((char *)stats, "\"audit_emitted\":1") != NULL);
    assert(strstr((char *)stats, "\"name\":\"age_check\"") != NULL);
    assert(strstr((char *)stats, "\"message\":\"age too low\"") != NULL);
    assert(strstr((char *)stats, "\"expr\":\"col('age') > 25\"") != NULL);
    assert(strstr((char *)stats, "\"result\":\"_valid\"") != NULL);
    assert(strstr((char *)stats, "\"valid\":false") != NULL);
    assert(strstr((char *)stats, "\"row\":2") != NULL);
    assert(strstr((char *)stats, "\"Bob\"") != NULL);
    assert(strstr((char *)stats, "\"row\":3") == NULL);
    tf_pipeline_free(p);

    const char *threshold_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"validate\",\"args\":{\"expr\":\"col('age') > 25\",\"max_failures\":1,\"name\":\"age_check\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(threshold_plan, strlen(threshold_plan));
    assert(p != NULL);
    const char *first = "name,age\nAlice,30\nBob,20\n";
    const char *second = "Cara,10\n";
    assert(tf_pipeline_push(p, (const uint8_t *)first, strlen(first)) == TF_OK);
    assert(tf_pipeline_push(p, (const uint8_t *)second, strlen(second)) != TF_OK);
    assert(tf_pipeline_error(p) != NULL);
    assert(strstr(tf_pipeline_error(p), "validate max_failures exceeded") != NULL);
    uint8_t errbuf[1024];
    size_t err_n = tf_pipeline_pull(p, TF_CHAN_ERRORS, errbuf, sizeof(errbuf) - 1);
    assert(err_n > 0);
    errbuf[err_n] = '\0';
    assert(strstr((char *)errbuf, "\"event\":\"threshold_exceeded\"") != NULL);
    assert(strstr((char *)errbuf, "\"max_failures\":1") != NULL);
    tf_pipeline_free(p);

    const char *warn_rate_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"validate\",\"args\":{\"expr\":\"col('age') > 25\",\"warn_failure_rate\":0.25,\"name\":\"age_rate\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(warn_rate_plan, strlen(warn_rate_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t warn_errors[2048];
    size_t warn_err_n = tf_pipeline_pull(p, TF_CHAN_ERRORS, warn_errors, sizeof(warn_errors) - 1);
    assert(warn_err_n > 0);
    warn_errors[warn_err_n] = '\0';
    assert(strstr((char *)warn_errors, "\"event\":\"threshold_warning\"") != NULL);
    assert(strstr((char *)warn_errors, "\"reason\":\"warn_failure_rate_exceeded\"") != NULL);
    assert(strstr((char *)warn_errors, "\"severity\":\"warning\"") != NULL);
    assert(strstr((char *)warn_errors, "\"warn_failure_rate\":0.25") != NULL);
    uint8_t warn_stats[4096];
    size_t warn_stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, warn_stats, sizeof(warn_stats) - 1);
    assert(warn_stats_n > 0);
    warn_stats[warn_stats_n] = '\0';
    assert(strstr((char *)warn_stats, "\"failure_rate\":") != NULL);
    assert(strstr((char *)warn_stats, "\"warn_failure_rate\":0.25") != NULL);
    tf_pipeline_free(p);

    const char *max_rate_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"validate\",\"args\":{\"expr\":\"col('age') > 25\",\"max_failure_rate\":0.5,\"name\":\"age_rate\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(max_rate_plan, strlen(max_rate_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) != TF_OK);
    assert(tf_pipeline_error(p) != NULL);
    assert(strstr(tf_pipeline_error(p), "validate max_failure_rate exceeded") != NULL);
    uint8_t rate_errbuf[2048];
    size_t rate_err_n = tf_pipeline_pull(p, TF_CHAN_ERRORS, rate_errbuf, sizeof(rate_errbuf) - 1);
    assert(rate_err_n > 0);
    rate_errbuf[rate_err_n] = '\0';
    assert(strstr((char *)rate_errbuf, "\"event\":\"threshold_exceeded\"") != NULL);
    assert(strstr((char *)rate_errbuf, "\"reason\":\"max_failure_rate_exceeded\"") != NULL);
    assert(strstr((char *)rate_errbuf, "\"max_failure_rate\":0.5") != NULL);
    assert(strstr((char *)rate_errbuf, "\"failed_rows\":2") != NULL);
    assert(strstr((char *)rate_errbuf, "\"checked_rows\":3") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_validate_rules(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"validate\",\"args\":{\"name\":\"quality\",\"audit\":true,\"audit_limit\":3,\"max_failures\":2,"
        "\"rules\":["
        "{\"name\":\"age_positive\",\"expr\":\"col('age') > 0\",\"message\":\"age must be positive\"},"
        "{\"name\":\"age_under_25\",\"expr\":\"col('age') < 25\"}"
        "]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,age\nAlice,30\nBob,-1\nCara,20\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "name,age,_valid") != NULL);
    assert(strstr((char *)out, "Alice,30,false") != NULL);
    assert(strstr((char *)out, "Bob,-1,false") != NULL);
    assert(strstr((char *)out, "Cara,20,true") != NULL);

    uint8_t stats[8192];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"suite\":\"quality\"") != NULL);
    assert(strstr((char *)stats, "\"name\":\"age_positive\"") != NULL);
    assert(strstr((char *)stats, "\"name\":\"age_under_25\"") != NULL);
    assert(strstr((char *)stats, "age must be positive") != NULL);
    assert(strstr((char *)stats, "\"checked_rows\":3") != NULL);
    assert(strstr((char *)stats, "\"passed_rows\":1") != NULL);
    assert(strstr((char *)stats, "\"failed_rows\":2") != NULL);
    assert(strstr((char *)stats, "\"audit_emitted\":2") != NULL);
    assert(strstr((char *)stats, "\"rule_count\":2") != NULL);
    assert(strstr((char *)stats, "\"max_failures\":2") != NULL);
    assert(strstr((char *)stats, "\"rules\":[") != NULL);
    assert(strstr((char *)stats, "\"expr\":\"col('age') > 0\"") != NULL);
    assert(strstr((char *)stats, "\"expr\":\"col('age') < 25\"") != NULL);
    tf_pipeline_free(p);

    char *error = NULL;
    const char *dsl = "csv | validate rule=age_positive:col(age)>0 rule=age_under_25:col(age)<25 audit audit_limit=3 max_failures=2 name=quality | csv";
    tf_ir_plan *parsed = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(parsed != NULL);
    assert(error == NULL);
    cJSON *rules = cJSON_GetObjectItemCaseSensitive(parsed->nodes[1].args, "rules");
    assert(cJSON_IsArray(rules));
    assert(cJSON_GetArraySize(rules) == 2);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(cJSON_GetArrayItem(rules, 0), "name")->valuestring, "age_positive") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(cJSON_GetArrayItem(rules, 1), "expr")->valuestring, "col(age)<25") == 0);
    tf_ir_plan_free(parsed);

    char rules_path[256];
    snprintf(rules_path, sizeof(rules_path), "/tmp/tranfi_validate_rules_%ld.json", (long)getpid());
    FILE *rf = fopen(rules_path, "wb");
    assert(rf != NULL);
    fputs("{\"name\":\"quality_file\",\"audit\":true,\"audit_limit\":4,\"warn_failure_rate\":0.25,\"rules\":["
          "{\"name\":\"age_positive\",\"expr\":\"col('age') > 0\",\"message\":\"age must be positive\"},"
          "{\"name\":\"adult\",\"expr\":\"col('age') >= 18\"}"
          "]}", rf);
    fclose(rf);

    char file_plan[1024];
    snprintf(file_plan, sizeof(file_plan),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
             "{\"op\":\"validate\",\"args\":{\"rules_file\":\"%s\"}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}"
             "]}", rules_path);
    p = tf_pipeline_create(file_plan, strlen(file_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice,30,true") != NULL);
    assert(strstr((char *)out, "Bob,-1,false") != NULL);
    assert(strstr((char *)out, "Cara,20,true") != NULL);
    stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"suite\":\"quality_file\"") != NULL);
    assert(strstr((char *)stats, "age must be positive") != NULL);
    assert(strstr((char *)stats, "\"rule_count\":2") != NULL);
    assert(strstr((char *)stats, "\"warn_failure_rate\":0.25") != NULL);
    uint8_t errors[2048];
    size_t err_n = tf_pipeline_pull(p, TF_CHAN_ERRORS, errors, sizeof(errors) - 1);
    assert(err_n > 0);
    errors[err_n] = '\0';
    assert(strstr((char *)errors, "\"event\":\"threshold_warning\"") != NULL);
    assert(strstr((char *)errors, "\"name\":\"quality_file\"") != NULL);
    tf_pipeline_free(p);

    char dsl_buf[512];
    snprintf(dsl_buf, sizeof(dsl_buf), "csv | validate rules_file=%s audit | csv", rules_path);
    parsed = tf_dsl_parse(dsl_buf, strlen(dsl_buf), &error);
    assert(parsed != NULL);
    assert(error == NULL);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(parsed->nodes[1].args, "rules_file")->valuestring, rules_path) == 0);
    tf_ir_plan_free(parsed);
    unlink(rules_path);
}

static void test_pipeline_quarantine(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"quarantine\",\"args\":{\"expr\":\"col('age') < 25\",\"name\":\"young\",\"message\":\"too young\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,age\nAlice,30\nBob,20\nCara,10\nDana,40\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "name,age") != NULL);
    assert(strstr((char *)out, "Alice,30") != NULL);
    assert(strstr((char *)out, "Dana,40") != NULL);
    assert(strstr((char *)out, "Bob") == NULL);
    assert(strstr((char *)out, "Cara") == NULL);

    uint8_t errors[4096];
    size_t err_n = tf_pipeline_pull(p, TF_CHAN_ERRORS, errors, sizeof(errors) - 1);
    assert(err_n > 0);
    errors[err_n] = '\0';
    assert(strstr((char *)errors, "\"type\":\"quarantine\"") != NULL);
    assert(strstr((char *)errors, "\"op\":\"quarantine\"") != NULL);
    assert(strstr((char *)errors, "\"event\":\"row_quarantined\"") != NULL);
    assert(strstr((char *)errors, "\"reason\":\"predicate_true\"") != NULL);
    assert(strstr((char *)errors, "\"name\":\"young\"") != NULL);
    assert(strstr((char *)errors, "too young") != NULL);
    assert(strstr((char *)errors, "\"row\":2") != NULL);
    assert(strstr((char *)errors, "\"row\":3") != NULL);
    assert(strstr((char *)errors, "\"Bob\"") != NULL);
    assert(strstr((char *)errors, "\"Cara\"") != NULL);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"op\":\"quarantine\"") != NULL);
    assert(strstr((char *)stats, "\"checked_rows\":4") != NULL);
    assert(strstr((char *)stats, "\"kept_rows\":2") != NULL);
    assert(strstr((char *)stats, "\"quarantined_rows\":2") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_assert_actions(void) {
    const char *csv = "name,age\nAlice,30\nBob,20\nCara,40\n";

    const char *annotate_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"assert\",\"args\":{\"expr\":\"col('age') > 25\",\"action\":\"annotate\",\"result\":\"age_ok\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(annotate_plan, strlen(annotate_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "name,age,age_ok") != NULL);
    assert(strstr((char *)out, "Alice,30,true") != NULL);
    assert(strstr((char *)out, "Bob,20,false") != NULL);
    tf_pipeline_free(p);

    const char *filter_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"assert\",\"args\":{\"expr\":\"col('age') > 25\",\"action\":\"filter\",\"audit\":true,\"audit_limit\":1,\"name\":\"age_check\",\"message\":\"age too low\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(filter_plan, strlen(filter_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Cara") != NULL);
    assert(strstr((char *)out, "Bob") == NULL);
    assert(tf_pipeline_pull(p, TF_CHAN_ERRORS, out, sizeof(out)) == 0);
    uint8_t audit_stats[8192];
    size_t audit_n = tf_pipeline_pull(p, TF_CHAN_STATS, audit_stats, sizeof(audit_stats) - 1);
    assert(audit_n > 0);
    audit_stats[audit_n] = '\0';
    assert(strstr((char *)audit_stats, "\"type\":\"audit\"") != NULL);
    assert(strstr((char *)audit_stats, "\"op\":\"assert\"") != NULL);
    assert(strstr((char *)audit_stats, "\"action\":\"filter\"") != NULL);
    assert(strstr((char *)audit_stats, "age_check") != NULL);
    assert(strstr((char *)audit_stats, "age too low") != NULL);
    assert(strstr((char *)audit_stats, "\"Bob\"") != NULL);
    tf_pipeline_free(p);

    const char *quarantine_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"assert\",\"args\":{\"expr\":\"col('age') > 25\",\"action\":\"quarantine\",\"name\":\"age_check\",\"message\":\"age too low\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(quarantine_plan, strlen(quarantine_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Bob") == NULL);
    uint8_t err[2048];
    size_t en = tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err) - 1);
    assert(en > 0);
    err[en] = '\0';
    assert(strstr((char *)err, "assert_failure") != NULL);
    assert(strstr((char *)err, "age_check") != NULL);
    assert(strstr((char *)err, "age too low") != NULL);
    assert(strstr((char *)err, "\"Bob\"") != NULL);
    tf_pipeline_free(p);

    const char *warn_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"assert\",\"args\":{\"expr\":\"col('age') > 25\",\"action\":\"warn\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(warn_plan, strlen(warn_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Bob") != NULL);
    en = tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err) - 1);
    assert(en > 0);
    err[en] = '\0';
    assert(strstr((char *)err, "\"severity\":\"warning\"") != NULL);
    assert(strstr((char *)err, "\"Bob\"") != NULL);
    tf_pipeline_free(p);

    const char *fail_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"assert\",\"args\":{\"expr\":\"col('age') > 25\",\"action\":\"fail\",\"message\":\"age rule\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(fail_plan, strlen(fail_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_ERROR);
    assert(tf_pipeline_error(p) != NULL);
    assert(strstr(tf_pipeline_error(p), "assert failed at row 2") != NULL);
    en = tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err) - 1);
    assert(en > 0);
    err[en] = '\0';
    assert(strstr((char *)err, "assert_failure") != NULL);
    assert(strstr((char *)err, "age rule") != NULL);
    tf_pipeline_free(p);

    const char *agg_warn_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"assert\",\"args\":{\"aggregate\":\"sum:amount\",\"op\":\">=\",\"value\":100,\"action\":\"warn\",\"name\":\"sales_total\",\"message\":\"too low\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    const char *amount_csv = "name,amount\nA,30\nB,20\n";
    p = tf_pipeline_create(agg_warn_plan, strlen(agg_warn_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)amount_csv, strlen(amount_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "A,30") != NULL);
    assert(strstr((char *)out, "B,20") != NULL);
    en = tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err) - 1);
    assert(en > 0);
    err[en] = '\0';
    assert(strstr((char *)err, "aggregate_assert_failed") != NULL);
    assert(strstr((char *)err, "\"severity\":\"warning\"") != NULL);
    assert(strstr((char *)err, "sales_total") != NULL);
    assert(strstr((char *)err, "\"actual\":50") != NULL);
    uint8_t stats[4096];
    size_t sn = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(sn > 0);
    stats[sn] = '\0';
    assert(strstr((char *)stats, "\"memory_class\":\"bounded_state\"") != NULL);
    assert(strstr((char *)stats, "\"state_estimate\":\"O(1) aggregate counters\"") != NULL);
    assert(strstr((char *)stats, "\"assert_mode\":\"aggregate\"") != NULL);
    assert(strstr((char *)stats, "\"aggregate\":\"sum\"") != NULL);
    assert(strstr((char *)stats, "\"aggregate_column\":\"amount\"") != NULL);
    assert(strstr((char *)stats, "\"aggregate_value\":50") != NULL);
    assert(strstr((char *)stats, "\"aggregate_passed\":false") != NULL);
    tf_pipeline_free(p);

    const char *rate_csv = "name,score\nA,10\nB,\nC,30\n";
    const char *missing_rate_warn_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"assert\",\"args\":{\"aggregate\":\"missing_rate:score\",\"op\":\"<=\",\"value\":0.25,\"action\":\"warn\",\"name\":\"score_missing_rate\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(missing_rate_warn_plan, strlen(missing_rate_warn_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)rate_csv, strlen(rate_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "A,10") != NULL);
    assert(strstr((char *)out, "B,") != NULL);
    en = tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err) - 1);
    assert(en > 0);
    err[en] = '\0';
    assert(strstr((char *)err, "aggregate_assert_failed") != NULL);
    assert(strstr((char *)err, "\"aggregate\":\"missing_rate\"") != NULL);
    assert(strstr((char *)err, "\"column\":\"score\"") != NULL);
    assert(strstr((char *)err, "\"rows\":3") != NULL);
    assert(strstr((char *)err, "\"missing\":1") != NULL);
    sn = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(sn > 0);
    stats[sn] = '\0';
    assert(strstr((char *)stats, "\"aggregate\":\"missing_rate\"") != NULL);
    assert(strstr((char *)stats, "\"aggregate_column\":\"score\"") != NULL);
    assert(strstr((char *)stats, "\"aggregate_rows\":3") != NULL);
    assert(strstr((char *)stats, "\"aggregate_non_null\":2") != NULL);
    assert(strstr((char *)stats, "\"aggregate_missing\":1") != NULL);
    assert(strstr((char *)stats, "\"aggregate_passed\":false") != NULL);
    tf_pipeline_free(p);

    const char *complete_rate_pass_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"assert\",\"args\":{\"aggregate\":\"complete_rate:score\",\"op\":\">=\",\"value\":0.66,\"action\":\"fail\",\"name\":\"score_complete_rate\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(complete_rate_pass_plan, strlen(complete_rate_pass_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)rate_csv, strlen(rate_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    assert(tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err)) == 0);
    sn = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(sn > 0);
    stats[sn] = '\0';
    assert(strstr((char *)stats, "\"aggregate\":\"complete_rate\"") != NULL);
    assert(strstr((char *)stats, "\"aggregate_passed\":true") != NULL);
    tf_pipeline_free(p);

    const char *agg_fail_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"assert\",\"args\":{\"aggregate\":\"count\",\"op\":\">=\",\"value\":3,\"action\":\"fail\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(agg_fail_plan, strlen(agg_fail_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)amount_csv, strlen(amount_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_ERROR);
    assert(tf_pipeline_error(p) != NULL);
    assert(strstr(tf_pipeline_error(p), "assert aggregate failed") != NULL);
    en = tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err) - 1);
    assert(en > 0);
    err[en] = '\0';
    assert(strstr((char *)err, "aggregate_assert_failed") != NULL);
    assert(strstr((char *)err, "\"aggregate\":\"count\"") != NULL);
    assert(strstr((char *)err, "\"actual\":2") != NULL);
    tf_pipeline_free(p);

    const char *agg_relative_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"assert\",\"args\":{\"aggregate\":\"sum:amount\",\"op\":\"==\",\"value\":1000000000000100,\"action\":\"fail\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    const char *large_amount_csv = "amount\n1000000000000000\n";
    p = tf_pipeline_create(agg_relative_plan, strlen(agg_relative_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)large_amount_csv, strlen(large_amount_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    assert(tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err)) == 0);
    sn = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(sn > 0);
    stats[sn] = '\0';
    assert(strstr((char *)stats, "\"aggregate_passed\":true") != NULL);
    assert(strstr((char *)stats, "\"relative_tolerance\":true") != NULL);
    tf_pipeline_free(p);

    const char *agg_absolute_warn_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"assert\",\"args\":{\"aggregate\":\"sum:amount\",\"op\":\"==\",\"value\":1000000000000100,\"tolerance\":1e-12,\"rel\":false,\"action\":\"warn\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(agg_absolute_warn_plan, strlen(agg_absolute_warn_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)large_amount_csv, strlen(large_amount_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    en = tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err) - 1);
    assert(en > 0);
    err[en] = '\0';
    assert(strstr((char *)err, "aggregate_assert_failed") != NULL);
    assert(strstr((char *)err, "\"relative_tolerance\":false") != NULL);
    sn = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(sn > 0);
    stats[sn] = '\0';
    assert(strstr((char *)stats, "\"aggregate_passed\":false") != NULL);
    assert(strstr((char *)stats, "\"relative_tolerance\":false") != NULL);
    tf_pipeline_free(p);

    const char *agg_whitespace_value_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"assert\",\"args\":{\"aggregate\":\"sum:amount\",\"op\":\"==\",\"value\":\"50 \",\"action\":\"fail\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(agg_whitespace_value_plan, strlen(agg_whitespace_value_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)amount_csv, strlen(amount_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    tf_pipeline_free(p);

    const char *agg_dsl_text = "csv | assert aggregate=count op=>= value=2 tolerance=0.001 rel=false action=warn name=row_count | csv";
    tf_ir_plan *agg_dsl = tf_dsl_parse(agg_dsl_text, strlen(agg_dsl_text), NULL);
    assert(agg_dsl != NULL);
    assert(tf_ir_validate(agg_dsl) == TF_OK);
    assert(strcmp(agg_dsl->nodes[1].op, "assert") == 0);
    assert(agg_dsl->nodes[1].memory_class == TF_MEM_BOUNDED_STATE);
    cJSON *agg_arg = cJSON_GetObjectItemCaseSensitive(agg_dsl->nodes[1].args, "aggregate");
    cJSON *op_arg = cJSON_GetObjectItemCaseSensitive(agg_dsl->nodes[1].args, "op");
    cJSON *value_arg = cJSON_GetObjectItemCaseSensitive(agg_dsl->nodes[1].args, "value");
    cJSON *tol_arg = cJSON_GetObjectItemCaseSensitive(agg_dsl->nodes[1].args, "tolerance");
    cJSON *rel_arg = cJSON_GetObjectItemCaseSensitive(agg_dsl->nodes[1].args, "rel");
    assert(cJSON_IsString(agg_arg) && strcmp(agg_arg->valuestring, "count") == 0);
    assert(cJSON_IsString(op_arg) && strcmp(op_arg->valuestring, ">=") == 0);
    assert(cJSON_IsNumber(value_arg) && value_arg->valuedouble == 2.0);
    assert(cJSON_IsNumber(tol_arg) && tol_arg->valuedouble == 0.001);
    assert(cJSON_IsBool(rel_arg) && !cJSON_IsTrue(rel_arg));
    tf_ir_plan_free(agg_dsl);

    const char *rate_dsl_text = "csv | assert aggregate=missing_rate:score op=<= value=0.1 action=warn name=score_missing_rate | csv";
    tf_ir_plan *rate_dsl = tf_dsl_parse(rate_dsl_text, strlen(rate_dsl_text), NULL);
    assert(rate_dsl != NULL);
    assert(tf_ir_validate(rate_dsl) == TF_OK);
    assert(rate_dsl->nodes[1].memory_class == TF_MEM_BOUNDED_STATE);
    cJSON *rate_arg = cJSON_GetObjectItemCaseSensitive(rate_dsl->nodes[1].args, "aggregate");
    assert(cJSON_IsString(rate_arg) && strcmp(rate_arg->valuestring, "missing_rate:score") == 0);
    tf_ir_plan_free(rate_dsl);
}


static void test_pipeline_schema_actions(void) {
    const char *csv = "name,age,city\nAlice,30,NY\nBob,200,SF\nCara,,LA\n";

    const char *annotate_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"schema\",\"args\":{\"columns\":{\"name\":\"string\",\"age\":\"int\",\"city\":\"string\"},\"non_null\":[\"name\",\"age\"],\"values\":{\"city\":[\"NY\",\"LA\"]},\"min\":{\"age\":0},\"max\":{\"age\":120},\"action\":\"annotate\",\"result\":\"schema_ok\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(annotate_plan, strlen(annotate_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[4096];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "name,age,city,schema_ok") != NULL);
    assert(strstr((char *)out, "Alice,30,NY,true") != NULL);
    assert(strstr((char *)out, "Bob,200,SF,false") != NULL);
    assert(strstr((char *)out, "Cara,,LA,false") != NULL);
    tf_pipeline_free(p);

    const char *filter_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"schema\",\"args\":{\"columns\":{\"name\":\"string\",\"age\":\"int\",\"city\":\"string\"},\"non_null\":[\"name\",\"age\"],\"values\":{\"city\":[\"NY\",\"LA\"]},\"min\":{\"age\":0},\"max\":{\"age\":120},\"action\":\"filter\",\"audit\":true,\"audit_limit\":1,\"name\":\"schema_check\",\"message\":\"schema rule\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(filter_plan, strlen(filter_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") == NULL);
    assert(strstr((char *)out, "Cara") == NULL);
    assert(tf_pipeline_pull(p, TF_CHAN_ERRORS, out, sizeof(out)) == 0);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "\"type\":\"audit\"") != NULL);
    assert(strstr((char *)out, "\"op\":\"schema\"") != NULL);
    assert(strstr((char *)out, "\"event\":\"row_dropped\"") != NULL);
    assert(strstr((char *)out, "\"reason\":\"schema_failed\"") != NULL);
    assert(strstr((char *)out, "\"action\":\"filter\"") != NULL);
    assert(strstr((char *)out, "schema_check") != NULL);
    assert(strstr((char *)out, "schema rule") != NULL);
    assert(strstr((char *)out, "\"rule\":\"max\"") != NULL);
    assert(strstr((char *)out, "\"column\":\"age\"") != NULL);
    assert(strstr((char *)out, "\"row\":2") != NULL);
    assert(strstr((char *)out, "\"Bob\"") != NULL);
    assert(strstr((char *)out, "\"Cara\"") == NULL);
    assert(strstr((char *)out, "\"checked_rows\":3") != NULL);
    assert(strstr((char *)out, "\"passed_rows\":1") != NULL);
    assert(strstr((char *)out, "\"failed_rows\":2") != NULL);
    assert(strstr((char *)out, "\"violation_count\":3") != NULL);
    assert(strstr((char *)out, "\"max_failures\":1") != NULL);
    assert(strstr((char *)out, "\"values_failures\":1") != NULL);
    assert(strstr((char *)out, "\"nullable_failures\":1") != NULL);
    assert(strstr((char *)out, "\"audit_emitted\":1") != NULL);
    tf_pipeline_free(p);

    const char *quarantine_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"schema\",\"args\":{\"columns\":{\"name\":\"string\",\"age\":\"int\",\"city\":\"string\"},\"non_null\":[\"name\",\"age\"],\"values\":{\"city\":[\"NY\",\"LA\"]},\"min\":{\"age\":0},\"max\":{\"age\":120},\"action\":\"quarantine\",\"name\":\"schema_check\",\"message\":\"schema rule\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(quarantine_plan, strlen(quarantine_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") == NULL);
    assert(strstr((char *)out, "Cara") == NULL);
    uint8_t err[4096];
    size_t en = tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err) - 1);
    assert(en > 0);
    err[en] = '\0';
    assert(strstr((char *)err, "schema_failure") != NULL);
    assert(strstr((char *)err, "schema_check") != NULL);
    assert(strstr((char *)err, "schema rule") != NULL);
    assert(strstr((char *)err, "\"severity\":\"error\"") != NULL);
    assert(strstr((char *)err, "\"rule\":\"max\"") != NULL);
    assert(strstr((char *)err, "\"rule\":\"values\"") != NULL);
    assert(strstr((char *)err, "\"rule\":\"nullable\"") != NULL);
    assert(strstr((char *)err, "\"Bob\"") != NULL);
    assert(strstr((char *)err, "\"Cara\"") != NULL);
    tf_pipeline_free(p);

    const char *warn_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"schema\",\"args\":{\"columns\":{\"name\":\"string\",\"age\":\"int\",\"city\":\"string\"},\"non_null\":[\"name\",\"age\"],\"values\":{\"city\":[\"NY\",\"LA\"]},\"min\":{\"age\":0},\"max\":{\"age\":120},\"action\":\"warn\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(warn_plan, strlen(warn_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);
    assert(strstr((char *)out, "Cara") != NULL);
    en = tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err) - 1);
    assert(en > 0);
    err[en] = '\0';
    assert(strstr((char *)err, "schema_failure") != NULL);
    assert(strstr((char *)err, "\"severity\":\"warning\"") != NULL);
    assert(strstr((char *)err, "\"rule\":\"values\"") != NULL);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "\"checked_rows\":3") != NULL);
    assert(strstr((char *)out, "\"failed_rows\":2") != NULL);
    assert(strstr((char *)out, "\"violation_count\":3") != NULL);
    tf_pipeline_free(p);

    const char *fail_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"schema\",\"args\":{\"columns\":{\"name\":\"string\",\"age\":\"int\"},\"max\":{\"age\":120},\"action\":\"fail\",\"message\":\"schema rule\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    const char *fail_csv = "name,age\nAlice,30\nBob,200\n";
    p = tf_pipeline_create(fail_plan, strlen(fail_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)fail_csv, strlen(fail_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_ERROR);
    assert(tf_pipeline_error(p) != NULL);
    assert(strstr(tf_pipeline_error(p), "schema failed at row 2: max") != NULL);
    en = tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err) - 1);
    assert(en > 0);
    err[en] = '\0';
    assert(strstr((char *)err, "schema_failure") != NULL);
    assert(strstr((char *)err, "schema rule") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_schema_baseline_drift(void) {
    const char *csv =
        "name,age,city,extra\n"
        "Alice,30,NY,x\n"
        "Bob,31,SF,y\n";
    const char *warn_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"schema\",\"args\":{\"baseline\":{"
        "\"columns\":{\"name\":\"string\",\"age\":\"int\",\"city\":\"string\"},"
        "\"values\":{\"city\":[\"NY\",\"LA\"]}},"
        "\"action\":\"warn\",\"name\":\"delivery_baseline\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(warn_plan, strlen(warn_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[4096];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice,30,NY,x") != NULL);
    assert(strstr((char *)out, "Bob,31,SF,y") != NULL);
    uint8_t err[4096];
    size_t en = tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err) - 1);
    assert(en > 0);
    err[en] = '\0';
    assert(strstr((char *)err, "\"name\":\"delivery_baseline\"") != NULL);
    assert(strstr((char *)err, "\"rule\":\"extra_column\"") != NULL);
    assert(strstr((char *)err, "\"column\":\"extra\"") != NULL);
    assert(strstr((char *)err, "\"rule\":\"values\"") != NULL);
    assert(strstr((char *)err, "\"actual\":\"SF\"") != NULL);
    assert(strstr((char *)err, "\"rule\":\"missing_category\"") != NULL);
    assert(strstr((char *)err, "\"expected\":\"LA\"") != NULL);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "\"baseline_mode\":true") != NULL);
    assert(strstr((char *)out, "\"allow_extra_columns\":false") != NULL);
    assert(strstr((char *)out, "\"require_values_seen\":true") != NULL);
    assert(strstr((char *)out, "\"extra_column_failures\":1") != NULL);
    assert(strstr((char *)out, "\"values_failures\":1") != NULL);
    assert(strstr((char *)out, "\"missing_category_failures\":1") != NULL);
    tf_pipeline_free(p);

    const char *fail_csv = "name,city\nAlice,NY\n";
    const char *fail_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"schema\",\"args\":{\"values\":{\"city\":[\"NY\",\"LA\"]},"
        "\"require_values_seen\":true,\"action\":\"fail\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(fail_plan, strlen(fail_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)fail_csv, strlen(fail_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_ERROR);
    assert(tf_pipeline_error(p) != NULL);
    assert(strstr(tf_pipeline_error(p), "schema failed at finish: missing_category") != NULL);
    en = tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err) - 1);
    assert(en > 0);
    err[en] = '\0';
    assert(strstr((char *)err, "\"rule\":\"missing_category\"") != NULL);
    assert(strstr((char *)err, "\"expected\":\"LA\"") != NULL);
    tf_pipeline_free(p);
}


static void test_selector_depth_limit(void) {
    size_t depth = 300;
    size_t inner_len = strlen("id");
    char *selector = malloc(depth + inner_len + depth + 1);
    assert(selector != NULL);
    char *p = selector;
    for (size_t i = 0; i < depth; i++) *p++ = '(';
    memcpy(p, "id", inner_len);
    p += inner_len;
    for (size_t i = 0; i < depth; i++) *p++ = ')';
    *p = '\0';

    char *names[] = { "id", "value" };
    tf_type types[] = { TF_TYPE_INT64, TF_TYPE_STRING };
    char *selectors[] = { selector };
    int *indices = NULL;
    size_t n_indices = 0;
    char *error = NULL;
    int rc = tf_column_selectors_resolve(selectors, 1, names, types, 2, &indices, &n_indices, &error);
    assert(rc == TF_ERROR);
    assert(error != NULL && strstr(error, "selector nesting too deep") != NULL);
    free(error);
    free(indices);
    free(selector);
}

static void test_json_path_depth_limit(void) {
    size_t depth = 300;
    char *path = malloc(depth * 2 + 1);
    assert(path != NULL);
    char *p = path;
    for (size_t i = 0; i < depth; i++) {
        *p++ = '/';
        *p++ = 'a';
    }
    *p = '\0';

    assert(tf_json_path_validate(path) == TF_ERROR);
    assert(tf_last_error() != NULL && strstr(tf_last_error(), "json path nesting too deep") != NULL);

    size_t plan_len = strlen(path) + 256;
    char *plan = malloc(plan_len);
    assert(plan != NULL);
    snprintf(plan, plan_len,
             "{\"steps\":[{\"op\":\"codec.jsonl.decode\",\"args\":{}},{\"op\":\"json-extract\",\"args\":{\"column\":\"payload\",\"path\":\"%s\",\"result\":\"x\"}},{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
             path);
    tf_pipeline *pipeline = tf_pipeline_create(plan, strlen(plan));
    assert(pipeline == NULL);
    assert(tf_last_error() != NULL && strstr(tf_last_error(), "json path nesting too deep") != NULL);

    free(plan);
    free(path);
}

static void test_pipeline_schema_selectors(void) {
    const char *csv = "id,score_a,score_b,code,note\n1,10,20,AA,x\n2,,200,bad,y\n";
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"schema\",\"args\":{"
        "\"columns\":{\"starts_with(score_)\":\"number\",\"ends_with(code)\":\"string\"},"
        "\"non_null\":[\"starts_with(score_)\"],"
        "\"min\":{\"where(number)\":0},"
        "\"max\":{\"starts_with(score_)\":100},"
        "\"regex\":{\"ends_with(code)\":\"^[A-Z]+$\"},"
        "\"action\":\"warn\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[4096];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "id,score_a,score_b,code,note") != NULL);
    assert(strstr((char *)out, "1,10,20,AA,x") != NULL);
    assert(strstr((char *)out, "2,,200,bad,y") != NULL);

    uint8_t err[4096];
    size_t en = tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err) - 1);
    assert(en > 0);
    err[en] = '\0';
    assert(strstr((char *)err, "schema_failure") != NULL);
    assert(strstr((char *)err, "\"column\":\"score_a\"") != NULL);
    assert(strstr((char *)err, "\"rule\":\"nullable\"") != NULL);
    assert(strstr((char *)err, "\"column\":\"score_b\"") != NULL);
    assert(strstr((char *)err, "\"rule\":\"max\"") != NULL);
    assert(strstr((char *)err, "\"column\":\"code\"") != NULL);
    assert(strstr((char *)err, "\"rule\":\"regex\"") != NULL);

    n = tf_pipeline_pull(p, TF_CHAN_STATS, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "\"checked_rows\":2") != NULL);
    assert(strstr((char *)out, "\"passed_rows\":1") != NULL);
    assert(strstr((char *)out, "\"failed_rows\":1") != NULL);
    assert(strstr((char *)out, "\"violation_count\":3") != NULL);
    assert(strstr((char *)out, "\"nullable_failures\":1") != NULL);
    assert(strstr((char *)out, "\"max_failures\":1") != NULL);
    assert(strstr((char *)out, "\"regex_failures\":1") != NULL);
    tf_pipeline_free(p);

    char *error = NULL;
    const char *dsl = "csv | schema columns=starts_with(score_):number,code:string non_null=starts_with(score_) min=where(number):0 max=starts_with(score_):100 regex=ends_with(code):^[A-Z]+$ mode=warn | csv";
    tf_ir_plan *parsed = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(parsed != NULL);
    assert(error == NULL);
    assert(strcmp(parsed->nodes[1].op, "schema") == 0);
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(parsed->nodes[1].args, "columns");
    assert(cJSON_IsObject(cols));
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(cols, "starts_with(score_)")->valuestring, "number") == 0);
    cJSON *nonnull = cJSON_GetObjectItemCaseSensitive(parsed->nodes[1].args, "non_null");
    assert(cJSON_IsArray(nonnull));
    assert(strcmp(cJSON_GetArrayItem(nonnull, 0)->valuestring, "starts_with(score_)") == 0);
    tf_ir_plan_free(parsed);
}


static void test_pipeline_schema_regex_budgets(void) {
    const char *pattern_cap_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"schema\",\"args\":{\"regex\":{\"code\":\"^[A-Z]+$\"},\"max_regex_pattern_bytes\":3,\"action\":\"warn\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(pattern_cap_plan, strlen(pattern_cap_plan));
    assert(p == NULL);
    assert(tf_last_error() != NULL && strstr(tf_last_error(), "max_regex_pattern_bytes") != NULL);

    const char *selector_pattern_cap_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"schema\",\"args\":{\"regex\":{\"ends_with(code)\":\"^[A-Z]+$\"},\"max_regex_pattern_bytes\":3,\"action\":\"warn\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(selector_pattern_cap_plan, strlen(selector_pattern_cap_plan));
    assert(p == NULL);
    assert(tf_last_error() != NULL && strstr(tf_last_error(), "max_regex_pattern_bytes") != NULL);

    const char *cell_cap_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"schema\",\"args\":{\"regex\":{\"code\":\"^[A-Z]+$\"},\"max_regex_pattern_bytes\":32,\"max_regex_cell_bytes\":3,\"action\":\"warn\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    const char *csv = "code\nAA\nTOOLONG\n";
    p = tf_pipeline_create(cell_cap_plan, strlen(cell_cap_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[4096];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "AA") != NULL);
    assert(strstr((char *)out, "TOOLONG") != NULL);
    uint8_t err[4096];
    size_t en = tf_pipeline_pull(p, TF_CHAN_ERRORS, err, sizeof(err) - 1);
    assert(en > 0);
    err[en] = '\0';
    assert(strstr((char *)err, "schema_failure") != NULL);
    assert(strstr((char *)err, "\"rule\":\"regex\"") != NULL);
    assert(strstr((char *)err, "regex cell within max_regex_cell_bytes") != NULL);
    assert(strstr((char *)err, "cell exceeds 3 bytes") != NULL);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "\"checked_rows\":2") != NULL);
    assert(strstr((char *)out, "\"passed_rows\":1") != NULL);
    assert(strstr((char *)out, "\"failed_rows\":1") != NULL);
    assert(strstr((char *)out, "\"regex_failures\":1") != NULL);
    assert(strstr((char *)out, "\"max_regex_pattern_bytes\":32") != NULL);
    assert(strstr((char *)out, "\"max_regex_cell_bytes\":3") != NULL);
    tf_pipeline_free(p);

    char *error = NULL;
    const char *dsl = "csv | schema regex=code:^[A-Z]+$ max_regex_cell_bytes=3 max_regex_pattern_bytes=32 mode=warn | csv";
    tf_ir_plan *parsed = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(parsed != NULL);
    assert(error == NULL);
    cJSON *cell = cJSON_GetObjectItemCaseSensitive(parsed->nodes[1].args, "max_regex_cell_bytes");
    cJSON *pattern = cJSON_GetObjectItemCaseSensitive(parsed->nodes[1].args, "max_regex_pattern_bytes");
    assert(cJSON_IsNumber(cell) && cell->valuedouble == 3.0);
    assert(cJSON_IsNumber(pattern) && pattern->valuedouble == 32.0);
    tf_ir_plan_free(parsed);
}


static void test_pipeline_schema_audit_privacy_controls(void) {
    const char *csv = "name,ssn,age\nAliciaSecret,111-22-3333,200\n";
    uint8_t buf[8192];
    size_t n = 0;

    const char *omit_row_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"schema\",\"args\":{\"max\":{\"age\":120},\"action\":\"filter\",\"audit\":true,\"audit_limit\":1,\"audit_include_row\":false}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(omit_row_plan, strlen(omit_row_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "\"type\":\"audit\"") != NULL);
    assert(strstr((char *)buf, "\"data\"") == NULL);
    assert(strstr((char *)buf, "AliciaSecret") == NULL);
    assert(strstr((char *)buf, "111-22-3333") == NULL);
    tf_pipeline_free(p);

    const char *redact_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"schema\",\"args\":{\"values\":{\"ssn\":[\"OK\"]},\"action\":\"filter\",\"audit\":true,\"audit_limit\":1,\"audit_columns\":[\"ssn\"],\"audit_redact\":[\"ssn\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(redact_plan, strlen(redact_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "[REDACTED]") != NULL);
    assert(strstr((char *)buf, "111-22-3333") == NULL);
    assert(strstr((char *)buf, "AliciaSecret") == NULL);
    assert(strstr((char *)buf, "\"data\":{\"ssn\":\"[REDACTED]\"}") != NULL);
    tf_pipeline_free(p);

    const char *hash_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"schema\",\"args\":{\"values\":{\"ssn\":[\"OK\"]},\"action\":\"filter\",\"audit\":true,\"audit_limit\":1,\"audit_hash_columns\":[\"ssn\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(hash_plan, strlen(hash_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "fnv1a64:") != NULL);
    assert(strstr((char *)buf, "111-22-3333") == NULL);
    tf_pipeline_free(p);

    const char *cell_cap_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"schema\",\"args\":{\"max\":{\"age\":120},\"action\":\"filter\",\"audit\":true,\"audit_limit\":1,\"audit_max_cell_bytes\":3}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(cell_cap_plan, strlen(cell_cap_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "Ali...") != NULL);
    assert(strstr((char *)buf, "111...") != NULL);
    assert(strstr((char *)buf, "AliciaSecret") == NULL);
    assert(strstr((char *)buf, "111-22-3333") == NULL);
    tf_pipeline_free(p);

    const char *bytes_cap_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"schema\",\"args\":{\"max\":{\"age\":120},\"action\":\"filter\",\"audit\":true,\"audit_limit\":1,\"audit_max_bytes\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(bytes_cap_plan, strlen(bytes_cap_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "_audit_truncated") != NULL);
    assert(strstr((char *)buf, "AliciaSecret") == NULL);
    assert(strstr((char *)buf, "111-22-3333") == NULL);
    tf_pipeline_free(p);

    const char *warn_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"schema\",\"args\":{\"regex\":{\"ssn\":\"^OK$\"},\"action\":\"warn\",\"audit_redact\":[\"ssn\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(warn_plan, strlen(warn_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_ERRORS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "schema_failure") != NULL);
    assert(strstr((char *)buf, "[REDACTED]") != NULL);
    assert(strstr((char *)buf, "111-22-3333") == NULL);
    tf_pipeline_free(p);

    char *error = NULL;
    const char *dsl = "csv | schema max=age:120 mode=filter audit audit_include_row=false audit_columns=name,ssn audit_redact=ssn audit_hash_columns=name audit_max_bytes=42 audit_max_cell_bytes=3 | csv";
    tf_ir_plan *parsed = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(parsed != NULL);
    assert(error == NULL);
    cJSON *args = parsed->nodes[1].args;
    assert(cJSON_IsBool(cJSON_GetObjectItemCaseSensitive(args, "audit_include_row")));
    assert(!cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(args, "audit_include_row")));
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(args, "audit_columns");
    assert(cJSON_IsArray(cols) && cJSON_GetArraySize(cols) == 2);
    assert(strcmp(cJSON_GetArrayItem(cols, 0)->valuestring, "name") == 0);
    assert(strcmp(cJSON_GetArrayItem(cols, 1)->valuestring, "ssn") == 0);
    cJSON *redact = cJSON_GetObjectItemCaseSensitive(args, "audit_redact");
    assert(cJSON_IsArray(redact) && strcmp(cJSON_GetArrayItem(redact, 0)->valuestring, "ssn") == 0);
    cJSON *hash = cJSON_GetObjectItemCaseSensitive(args, "audit_hash_columns");
    assert(cJSON_IsArray(hash) && strcmp(cJSON_GetArrayItem(hash, 0)->valuestring, "name") == 0);
    assert(cJSON_GetObjectItemCaseSensitive(args, "audit_max_bytes")->valuedouble == 42.0);
    assert(cJSON_GetObjectItemCaseSensitive(args, "audit_max_cell_bytes")->valuedouble == 3.0);
    tf_ir_plan_free(parsed);

    const char *frequency_dsl = "csv | frequency city max_values=1 overflow=other audit_columns=city audit_redact=city | csv";
    parsed = tf_dsl_parse(frequency_dsl, strlen(frequency_dsl), &error);
    assert(parsed != NULL);
    assert(error == NULL);
    args = parsed->nodes[1].args;
    cols = cJSON_GetObjectItemCaseSensitive(args, "audit_columns");
    assert(cJSON_IsArray(cols) && cJSON_GetArraySize(cols) == 1);
    assert(strcmp(cJSON_GetArrayItem(cols, 0)->valuestring, "city") == 0);
    redact = cJSON_GetObjectItemCaseSensitive(args, "audit_redact");
    assert(cJSON_IsArray(redact) && strcmp(cJSON_GetArrayItem(redact, 0)->valuestring, "city") == 0);
    tf_ir_plan_free(parsed);
}

static void test_pipeline_audit_privacy_migrated_producers(void) {
    const char *csv = "name,ssn,age\nAlice,111-22-3333,20\nBob,222-33-4444,40\n";
    uint8_t buf[8192];
    size_t n = 0;

    const char *filter_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col('age') > 30\",\"audit\":true,\"audit_limit\":1,\"audit_include_row\":false}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(filter_plan, strlen(filter_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "\"op\":\"filter\"") != NULL);
    assert(strstr((char *)buf, "\"data\"") == NULL);
    assert(strstr((char *)buf, "Alice") == NULL);
    assert(strstr((char *)buf, "111-22-3333") == NULL);
    tf_pipeline_free(p);

    const char *validate_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"validate\",\"args\":{\"expr\":\"col('age') > 30\",\"audit\":true,\"audit_limit\":1,\"audit_columns\":[\"ssn\"],\"audit_redact\":[\"ssn\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(validate_plan, strlen(validate_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "\"op\":\"validate\"") != NULL);
    assert(strstr((char *)buf, "[REDACTED]") != NULL);
    assert(strstr((char *)buf, "111-22-3333") == NULL);
    assert(strstr((char *)buf, "Alice") == NULL);
    assert(strstr((char *)buf, "\"data\":{\"ssn\":\"[REDACTED]\"}") != NULL);
    tf_pipeline_free(p);

    const char *assert_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"assert\",\"args\":{\"expr\":\"col('age') > 30\",\"action\":\"warn\",\"audit_columns\":[\"ssn\"],\"audit_hash_columns\":[\"ssn\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(assert_plan, strlen(assert_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_ERRORS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "assert_failure") != NULL);
    assert(strstr((char *)buf, "fnv1a64:") != NULL);
    assert(strstr((char *)buf, "111-22-3333") == NULL);
    assert(strstr((char *)buf, "Alice") == NULL);
    tf_pipeline_free(p);

    const char *json_plan =
        "{\"steps\":["
        "{\"op\":\"codec.text.decode\",\"args\":{}},"
        "{\"op\":\"json-schema\",\"args\":{\"schema\":{\"type\":\"object\",\"required\":[\"user\"]},\"mode\":\"filter\",\"audit\":true,\"audit_limit\":1,\"audit_columns\":[\"_line\"],\"audit_redact\":[\"_line\"]}},"
        "{\"op\":\"codec.text.encode\",\"args\":{}}"
        "]}";
    const char *json = "{\"ssn\":\"111-22-3333\"}\n";
    p = tf_pipeline_create(json_plan, strlen(json_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)json, strlen(json)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "\"op\":\"json-schema\"") != NULL);
    assert(strstr((char *)buf, "[REDACTED]") != NULL);
    assert(strstr((char *)buf, "111-22-3333") == NULL);
    tf_pipeline_free(p);

    const char *fill_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1,\"nulls\":\"NA\"}},"
        "{\"op\":\"fill-null\",\"args\":{\"mapping\":{\"note\":\"SECRET\"},\"audit\":true,\"audit_limit\":1,\"audit_columns\":[\"note\"],\"audit_redact\":[\"note\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(fill_plan, strlen(fill_plan));
    assert(p != NULL);
    const char *fill_csv = "name,note\nB,NA\nC,ok\n";
    assert(tf_pipeline_push(p, (const uint8_t *)fill_csv, strlen(fill_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "\"op\":\"fill-null\"") != NULL);
    assert(strstr((char *)buf, "[REDACTED]") != NULL);
    assert(strstr((char *)buf, "SECRET") == NULL);
    assert(strstr((char *)buf, "\"B\"") == NULL);
    assert(strstr((char *)buf, "\"data\":{\"note\":\"[REDACTED]\"}") != NULL);
    tf_pipeline_free(p);

    const char *replace_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"replace\",\"args\":{\"column\":\"ssn\",\"pattern\":\"111\",\"replacement\":\"999\",\"audit\":true,\"audit_limit\":1,\"audit_columns\":[\"ssn\"],\"audit_redact\":[\"ssn\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(replace_plan, strlen(replace_plan));
    assert(p != NULL);
    const char *replace_csv = "name,ssn\nAlice,111-22-3333\nBob,222-33-4444\n";
    assert(tf_pipeline_push(p, (const uint8_t *)replace_csv, strlen(replace_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "\"op\":\"replace\"") != NULL);
    assert(strstr((char *)buf, "[REDACTED]") != NULL);
    assert(strstr((char *)buf, "111") == NULL);
    assert(strstr((char *)buf, "999") == NULL);
    assert(strstr((char *)buf, "Alice") == NULL);
    assert(strstr((char *)buf, "\"data\":{\"ssn\":\"[REDACTED]\"}") != NULL);
    tf_pipeline_free(p);


    const char *cast_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"cast\",\"args\":{\"mapping\":{\"secret\":\"int\"},\"audit\":true,\"audit_limit\":1,\"audit_columns\":[\"secret\"],\"audit_redact\":[\"secret\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(cast_plan, strlen(cast_plan));
    assert(p != NULL);
    const char *cast_csv = "name,secret\nAlice,111-22-3333\n";
    assert(tf_pipeline_push(p, (const uint8_t *)cast_csv, strlen(cast_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "\"op\":\"cast\"") != NULL);
    assert(strstr((char *)buf, "[REDACTED]") != NULL);
    assert(strstr((char *)buf, "111") == NULL);
    assert(strstr((char *)buf, "Alice") == NULL);
    assert(strstr((char *)buf, "\"data\":{\"secret\":\"[REDACTED]\"}") != NULL);
    tf_pipeline_free(p);

    const char *normalize_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"normalize\",\"args\":{\"columns\":[\"score\"],\"audit\":true,\"audit_limit\":1,\"audit_columns\":[\"score\"],\"audit_redact\":[\"score\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(normalize_plan, strlen(normalize_plan));
    assert(p != NULL);
    const char *normalize_csv = "name,score\nAlice,100\nBob,200\n";
    assert(tf_pipeline_push(p, (const uint8_t *)normalize_csv, strlen(normalize_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "\"op\":\"normalize\"") != NULL);
    assert(strstr((char *)buf, "[REDACTED]") != NULL);
    assert(strstr((char *)buf, "100") == NULL);
    assert(strstr((char *)buf, "200") == NULL);
    assert(strstr((char *)buf, "Alice") == NULL);
    assert(strstr((char *)buf, "\"data\":{\"score\":\"[REDACTED]\"}") != NULL);
    tf_pipeline_free(p);

    const char *frequency_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"frequency\",\"args\":{\"columns\":[\"city\"],\"max_values\":1,\"overflow\":\"other\",\"audit\":true,\"audit_limit\":1,\"audit_columns\":[\"city\"],\"audit_redact\":[\"city\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(frequency_plan, strlen(frequency_plan));
    assert(p != NULL);
    const char *frequency_csv = "name,city\nAlice,NY\nBob,LA\n";
    assert(tf_pipeline_push(p, (const uint8_t *)frequency_csv, strlen(frequency_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "\"op\":\"frequency\"") != NULL);
    assert(strstr((char *)buf, "\"event\":\"category_overflow\"") != NULL);
    assert(strstr((char *)buf, "[REDACTED]") != NULL);
    assert(strstr((char *)buf, "LA") == NULL);
    assert(strstr((char *)buf, "Bob") == NULL);
    assert(strstr((char *)buf, "Alice") == NULL);
    assert(strstr((char *)buf, "\"data\":{\"city\":\"[REDACTED]\"}") != NULL);
    tf_pipeline_free(p);

    const char *repair_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"repair\":true,\"audit\":true,\"audit_limit\":1,\"audit_redact\":[\"raw\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(repair_plan, strlen(repair_plan));
    assert(p != NULL);
    const char *repair_csv = "name,secret\nAlice,SECRET,extra\n";
    assert(tf_pipeline_push(p, (const uint8_t *)repair_csv, strlen(repair_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "\"event\":\"row_repaired\"") != NULL);
    assert(strstr((char *)buf, "[REDACTED]") != NULL);
    assert(strstr((char *)buf, "SECRET") == NULL);
    assert(strstr((char *)buf, "Alice") == NULL);
    n = tf_pipeline_pull(p, TF_CHAN_ERRORS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "csv_field_count") != NULL);
    assert(strstr((char *)buf, "[REDACTED]") != NULL);
    assert(strstr((char *)buf, "SECRET") == NULL);
    assert(strstr((char *)buf, "Alice") == NULL);
    tf_pipeline_free(p);

    const char *tee_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"tee\",\"args\":{\"expr\":\"col('age') >= 20\",\"channel\":\"audit\",\"columns\":[\"name\",\"ssn\"],\"limit\":1,\"audit_columns\":[\"ssn\"],\"audit_redact\":[\"ssn\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(tee_plan, strlen(tee_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "\"op\":\"tee\"") != NULL);
    assert(strstr((char *)buf, "[REDACTED]") != NULL);
    assert(strstr((char *)buf, "111-22-3333") == NULL);
    assert(strstr((char *)buf, "Alice") == NULL);
    assert(strstr((char *)buf, "\"data\":{\"ssn\":\"[REDACTED]\"}") != NULL);
    tf_pipeline_free(p);

    const char *quarantine_privacy_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"quarantine\",\"args\":{\"expr\":\"col('age') < 30\",\"audit_columns\":[\"ssn\"],\"audit_redact\":[\"ssn\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(quarantine_privacy_plan, strlen(quarantine_privacy_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_ERRORS, buf, sizeof(buf) - 1);
    assert(n > 0);
    buf[n] = '\0';
    assert(strstr((char *)buf, "\"op\":\"quarantine\"") != NULL);
    assert(strstr((char *)buf, "[REDACTED]") != NULL);
    assert(strstr((char *)buf, "111-22-3333") == NULL);
    assert(strstr((char *)buf, "Alice") == NULL);
    assert(strstr((char *)buf, "\"data\":{\"ssn\":\"[REDACTED]\"}") != NULL);
    tf_pipeline_free(p);

}


static void test_pipeline_schema_infer(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"schema-infer\",\"args\":{\"rows\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    const char *csv = "name,age,score\nAlice,30,1.5\nBob,,2.25\nCara,40,3.5\n";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[4096];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "column,type,nullable,non_null,rows_seen,rows_sampled,missing,non_missing,observed_types,warning") != NULL);
    assert(strstr((char *)out, "name,string,false,true,3,2,0,2,string:2,sample_limited") != NULL);
    assert(strstr((char *)out, "age,int,true,false,3,2,1,1,int:1;null:1,sample_limited") != NULL);
    assert(strstr((char *)out, "score,float,false,true,3,2,0,2,float:2,sample_limited") != NULL);
    tf_pipeline_free(p);

    const char *bad_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"schema-infer\",\"args\":{\"rows\":0}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(bad_plan, strlen(bad_plan));
    assert(p == NULL);
}


static void test_pipeline_tee_side_channel(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"tee\",\"args\":{\"expr\":\"col('age') >= 30\",\"channel\":\"samples\",\"columns\":[\"name\"],\"limit\":2,\"every\":1,\"name\":\"senior_sample\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *part1 = "name,age\nAlice,30\nBob,25\n";
    const char *part2 = "Cara,35\nDave,45\n";
    assert(tf_pipeline_push(p, (const uint8_t *)part1, strlen(part1)) == TF_OK);
    uint8_t samples[2048];
    size_t sn = tf_pipeline_pull(p, TF_CHAN_SAMPLES, samples, sizeof(samples) - 1);
    assert(sn > 0);
    samples[sn] = '\0';
    assert(strstr((char *)samples, "\"type\":\"tee\"") != NULL);
    assert(strstr((char *)samples, "senior_sample") != NULL);
    assert(strstr((char *)samples, "\"Alice\"") != NULL);
    assert(strstr((char *)samples, "\"age\"") == NULL);
    assert(strstr((char *)samples, "\"Bob\"") == NULL);

    assert(tf_pipeline_push(p, (const uint8_t *)part2, strlen(part2)) == TF_OK);
    sn = tf_pipeline_pull(p, TF_CHAN_SAMPLES, samples, sizeof(samples) - 1);
    assert(sn > 0);
    samples[sn] = '\0';
    assert(strstr((char *)samples, "\"Cara\"") != NULL);
    assert(strstr((char *)samples, "\"Dave\"") == NULL);

    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice,30") != NULL);
    assert(strstr((char *)out, "Bob,25") != NULL);
    assert(strstr((char *)out, "Cara,35") != NULL);
    assert(strstr((char *)out, "Dave,45") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_stack_preserves_long_cells(void) {
    const char *path = "/tmp/tranfi_stack_long_cell.csv";
    char *long_cell = malloc(5001);
    assert(long_cell != NULL);
    memset(long_cell, 'x', 5000);
    long_cell[5000] = '\0';

    FILE *f = fopen(path, "wb");
    assert(f != NULL);
    assert(fputs("name,blob\nStacked,", f) >= 0);
    assert(fputs(long_cell, f) >= 0);
    assert(fputc('\n', f) != EOF);
    assert(fclose(f) == 0);

    char plan[512];
    snprintf(plan, sizeof(plan),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
             "{\"op\":\"stack\",\"args\":{\"file\":\"%s\"}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}"
             "]}",
             path);

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *input = "name,blob\nInput,seed\n";
    assert(tf_pipeline_push(p, (const uint8_t *)input, strlen(input)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    char *out = malloc(7000);
    assert(out != NULL);
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, (uint8_t *)out, 6999);
    out[n] = '\0';
    assert(strstr(out, "Input,seed") != NULL);
    assert(strstr(out, "Stacked,") != NULL);
    assert(strstr(out, long_cell) != NULL);

    free(out);
    tf_pipeline_free(p);
    remove(path);
    free(long_cell);
}

static void test_pipeline_datetime(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"datetime\",\"args\":{\"column\":\"date\",\"extract\":[\"year\",\"month\",\"day\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "date\n2024-03-15\n2023-12-25\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "date_year") != NULL);
    assert(strstr((char *)out, "date_month") != NULL);
    assert(strstr((char *)out, "date_day") != NULL);
    assert(strstr((char *)out, "2024") != NULL);
    assert(strstr((char *)out, "2023") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_step_running_sum(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"step\",\"args\":{\"column\":\"val\",\"func\":\"running-sum\",\"result\":\"cumsum\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "val\n1\n2\n3\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "cumsum") != NULL);
    /* 1, 3, 6 */
    assert(strstr((char *)out, ",1\n") != NULL);
    assert(strstr((char *)out, ",3\n") != NULL);
    assert(strstr((char *)out, ",6\n") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_frequency(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"frequency\",\"args\":{\"columns\":[\"name\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name\nAlice\nBob\nAlice\nAlice\nBob\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* Alice:3, Bob:2 — sorted by count desc */
    assert(strstr((char *)out, "value,count") != NULL);
    assert(strstr((char *)out, "Alice,3") != NULL);
    assert(strstr((char *)out, "Bob,2") != NULL);
    /* Alice should come before Bob (higher count) */
    char *alice_pos = strstr((char *)out, "Alice,3");
    char *bob_pos = strstr((char *)out, "Bob,2");
    assert(alice_pos < bob_pos);
    uint8_t stats[2048];
    n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = 0;
    assert(strstr((char *)stats, "\"op\":\"frequency\"") != NULL);
    assert(strstr((char *)stats, "\"tracked_values\":2") != NULL);
    assert(strstr((char *)stats, "\"tracked_key_bytes\":10") != NULL);
    assert(strstr((char *)stats, "\"overflow_count\":0") != NULL);
    assert(strstr((char *)stats, "\"retained_state_bytes\":") != NULL);
    tf_pipeline_free(p);

    const char *bad_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"frequency\",\"args\":{\"columns\":[123]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(bad_plan, strlen(bad_plan));
    assert(p == NULL);
    const char *err = tf_last_error();
    assert(err && strstr(err, "frequency: column names must be non-empty strings") != NULL);
}


static void test_pipeline_frequency_overflow_other(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"frequency\",\"args\":{\"columns\":[\"city\"],\"max_values\":2,\"overflow\":\"other\",\"other\":\"REST\",\"audit\":true,\"audit_limit\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "city\nNY\nLA\nSF\nTX\nNY\nSF\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = 0;
    assert(strstr((char *)out, "value,count") != NULL);
    assert(strstr((char *)out, "REST,3") != NULL);
    assert(strstr((char *)out, "NY,2") != NULL);
    assert(strstr((char *)out, "LA,1") != NULL);
    assert(strstr((char *)out, "SF,") == NULL);
    assert(strstr((char *)out, "TX,") == NULL);
    uint8_t stats[4096];
    n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = 0;
    assert(strstr((char *)stats, "\"type\":\"audit\"") != NULL);
    assert(strstr((char *)stats, "\"op\":\"frequency\"") != NULL);
    assert(strstr((char *)stats, "\"event\":\"category_overflow\"") != NULL);
    assert(strstr((char *)stats, "\"reason\":\"max_values_overflow\"") != NULL);
    assert(strstr((char *)stats, "\"value\":\"SF\"") != NULL);
    assert(strstr((char *)stats, "\"value\":\"TX\"") != NULL);
    assert(strstr((char *)stats, "\"bucket\":\"REST\"") != NULL);
    assert(strstr((char *)stats, "\"row\":3") != NULL);
    assert(strstr((char *)stats, "\"row\":4") != NULL);
    assert(strstr((char *)stats, "\"row\":6") == NULL);
    assert(strstr((char *)stats, "\"tracked_values\":2") != NULL);
    assert(strstr((char *)stats, "\"tracked_key_bytes\":6") != NULL);
    assert(strstr((char *)stats, "\"overflow_count\":3") != NULL);
    assert(strstr((char *)stats, "\"retained_state_bytes\":") != NULL);
    tf_pipeline_free(p);
}
static void test_pipeline_top(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"top\",\"args\":{\"n\":2,\"column\":\"score\",\"desc\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,score\nAlice,85\nBob,92\nCharlie,78\nDiana,95\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* Top 2 by score desc: Diana(95), Bob(92) */
    assert(strstr((char *)out, "Diana") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);
    assert(strstr((char *)out, "Charlie") == NULL);
    tf_pipeline_free(p);

    const char *bad_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"top\",\"args\":{\"n\":2,\"column\":\"\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(bad_plan, strlen(bad_plan));
    assert(p == NULL);
    const char *err = tf_last_error();
    assert(err && strstr(err, "top: column must be a non-empty string") != NULL);

    const char *missing_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"top\",\"args\":{\"n\":2,\"column\":\"missing\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(missing_plan, strlen(missing_plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_ERROR);
    err = tf_pipeline_error(p);
    assert(err && strstr(err, "top: column 'missing' not found") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_bottom_k(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"bottom-k\",\"args\":{\"n\":2,\"column\":\"score\",\"desc\":false}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,score\nAlice,85\nBob,92\nCharlie,78\nDiana,95\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Charlie") != NULL);
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") == NULL);
    assert(strstr((char *)out, "Diana") == NULL);
    assert(strstr((char *)out, "Charlie") < strstr((char *)out, "Alice"));
    tf_pipeline_free(p);
}

static void test_pipeline_slice_min_string(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"slice-min\",\"args\":{\"n\":2,\"column\":\"name\",\"desc\":false}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,score\nDiana,95\nBob,92\nCharlie,78\nAlice,85\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);
    assert(strstr((char *)out, "Charlie") == NULL);
    assert(strstr((char *)out, "Diana") == NULL);
    assert(strstr((char *)out, "Alice") < strstr((char *)out, "Bob"));
    tf_pipeline_free(p);
}

static void test_dsl_new_ops(void) {
    char *error = NULL;
    tf_ir_plan *plan;

    /* tail */
    plan = tf_dsl_parse("csv | tail 5 | csv", 18, &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "tail") == 0);
    tf_ir_plan_free(plan);

    /* top */
    plan = tf_dsl_parse("csv | top 10 score | csv", strlen("csv | top 10 score | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "top") == 0);
    tf_ir_plan_free(plan);

    /* assert */
    const char *dsl_assert = "csv | assert \"col(age) > 25\" action=quarantine name=age_check message=bad_age | csv";
    plan = tf_dsl_parse(dsl_assert, strlen(dsl_assert), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "assert") == 0);
    cJSON *assert_action = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "action");
    cJSON *assert_name = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "name");
    assert(cJSON_IsString(assert_action) && strcmp(assert_action->valuestring, "quarantine") == 0);
    assert(cJSON_IsString(assert_name) && strcmp(assert_name->valuestring, "age_check") == 0);
    tf_ir_plan_free(plan);

    const char *dsl_assert_audit = "csv | assert \"col(age) > 25\" action=filter audit audit_limit=3 | csv";
    plan = tf_dsl_parse(dsl_assert_audit, strlen(dsl_assert_audit), &error);
    assert(plan != NULL);
    assert_action = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "action");
    cJSON *assert_audit = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit");
    cJSON *assert_audit_limit = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_limit");
    assert(cJSON_IsString(assert_action) && strcmp(assert_action->valuestring, "filter") == 0);
    assert(cJSON_IsTrue(assert_audit));
    assert(cJSON_IsNumber(assert_audit_limit) && assert_audit_limit->valueint == 3);
    tf_ir_plan_free(plan);


    /* rolling */
    plan = tf_dsl_parse("csv | rolling-sum price 3 price_sum3 | csv", strlen("csv | rolling-sum price 3 price_sum3 | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "rolling-sum") == 0);
    cJSON *roll_size = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "size");
    cJSON *roll_result = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "result");
    assert(cJSON_IsNumber(roll_size) && roll_size->valueint == 3);
    assert(cJSON_IsString(roll_result) && strcmp(roll_result->valuestring, "price_sum3") == 0);
    assert(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "func") == NULL);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | rolling-any flag 3 any3 nulls=propagate | csv", strlen("csv | rolling-any flag 3 any3 nulls=propagate | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "rolling-any") == 0);
    cJSON *roll_nulls = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "nulls");
    cJSON *roll_bool_result = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "result");
    assert(cJSON_IsString(roll_nulls) && strcmp(roll_nulls->valuestring, "propagate") == 0);
    assert(cJSON_IsString(roll_bool_result) && strcmp(roll_bool_result->valuestring, "any3") == 0);
    tf_ir_plan_free(plan);

    /* lag */
    plan = tf_dsl_parse("csv | lag price 2 prev_price | csv", strlen("csv | lag price 2 prev_price | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "lag") == 0);
    cJSON *lag_offset = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "offset");
    cJSON *lag_result = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "result");
    assert(cJSON_IsNumber(lag_offset) && lag_offset->valueint == 2);
    assert(cJSON_IsString(lag_result) && strcmp(lag_result->valuestring, "prev_price") == 0);
    tf_ir_plan_free(plan);

    /* shift */
    plan = tf_dsl_parse("csv | shift price offset=3 result=next_price type=lead | csv", strlen("csv | shift price offset=3 result=next_price type=lead | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "shift") == 0);
    cJSON *shift_offset = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "offset");
    cJSON *shift_type = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "type");
    assert(cJSON_IsNumber(shift_offset) && shift_offset->valueint == 3);
    assert(cJSON_IsString(shift_type) && strcmp(shift_type->valuestring, "lead") == 0);
    tf_ir_plan_free(plan);


    /* rowid */
    plan = tf_dsl_parse("csv | rowid city,status result=within_key max_keys=7 | csv", strlen("csv | rowid city,status result=within_key max_keys=7 | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "rowid") == 0);
    cJSON *rowid_cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    cJSON *rowid_result = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "result");
    cJSON *rowid_max = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_keys");
    assert(cJSON_IsArray(rowid_cols) && cJSON_GetArraySize(rowid_cols) == 2);
    assert(strcmp(cJSON_GetArrayItem(rowid_cols, 0)->valuestring, "city") == 0);
    assert(strcmp(cJSON_GetArrayItem(rowid_cols, 1)->valuestring, "status") == 0);
    assert(cJSON_IsString(rowid_result) && strcmp(rowid_result->valuestring, "within_key") == 0);
    assert(cJSON_IsNumber(rowid_max) && rowid_max->valueint == 7);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | rowid city as=run_n sorted=true | csv", strlen("csv | rowid city as=run_n sorted=true | csv"), &error);
    assert(plan != NULL);
    cJSON *rowid_sorted = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "sorted");
    assert(cJSON_IsBool(rowid_sorted) && cJSON_IsTrue(rowid_sorted));
    tf_ir_plan_free(plan);

    /* rleid */
    plan = tf_dsl_parse("csv | rleid city,status result=run_id | csv", strlen("csv | rleid city,status result=run_id | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "rleid") == 0);
    cJSON *rleid_cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    cJSON *rleid_result = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "result");
    assert(cJSON_IsArray(rleid_cols) && cJSON_GetArraySize(rleid_cols) == 2);
    assert(strcmp(cJSON_GetArrayItem(rleid_cols, 0)->valuestring, "city") == 0);
    assert(strcmp(cJSON_GetArrayItem(rleid_cols, 1)->valuestring, "status") == 0);
    assert(cJSON_IsString(rleid_result) && strcmp(rleid_result->valuestring, "run_id") == 0);
    tf_ir_plan_free(plan);

    /* top-k */
    plan = tf_dsl_parse("csv | top-k n=10 score | csv", strlen("csv | top-k n=10 score | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "top-k") == 0);
    tf_ir_plan_free(plan);

    /* bottom-k */
    plan = tf_dsl_parse("csv | bottom-k 10 score | csv", strlen("csv | bottom-k 10 score | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "bottom-k") == 0);
    cJSON *desc_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "desc");
    assert(cJSON_IsFalse(desc_j));
    tf_ir_plan_free(plan);

    /* slice-min / slice-max */
    plan = tf_dsl_parse("csv | slice-min score n=10 with_ties=false | csv", strlen("csv | slice-min score n=10 with_ties=false | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "slice-min") == 0);
    cJSON *with_ties_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "with_ties");
    assert(cJSON_IsFalse(with_ties_j));
    tf_ir_plan_free(plan);
    plan = tf_dsl_parse("csv | slice-max score 10 | csv", strlen("csv | slice-max score 10 | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "slice-max") == 0);
    tf_ir_plan_free(plan);
    plan = tf_dsl_parse("csv | slice-min score n=10 with_ties=true | csv", strlen("csv | slice-min score n=10 with_ties=true | csv"), &error);
    assert(plan == NULL);
    assert(error != NULL && strstr(error, "with_ties=true") != NULL);
    free(error); error = NULL;

    plan = tf_dsl_parse("csv | slice_head n=3 | csv", strlen("csv | slice_head n=3 | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "slice-head") == 0);
    cJSON *slice_n = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "n");
    assert(slice_n && slice_n->valueint == 3);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | slice-tail 2 | csv", strlen("csv | slice-tail 2 | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "slice-tail") == 0);
    slice_n = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "n");
    assert(slice_n && slice_n->valueint == 2);
    tf_ir_plan_free(plan);

    /* sample */
    plan = tf_dsl_parse("csv | sample 50 | csv", 21, &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "sample") == 0);
    tf_ir_plan_free(plan);

    /* reorder (alias for select) */
    plan = tf_dsl_parse("csv | reorder age,name | csv", 28, &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "reorder") == 0);
    tf_ir_plan_free(plan);

    /* dedup (alias for unique) */
    plan = tf_dsl_parse("csv | dedup name | csv", 22, &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "dedup") == 0);
    tf_ir_plan_free(plan);

    /* json-extract */
    const char *dsl_json_extract = "text | json-extract /user/id user_id type=int | csv";
    plan = tf_dsl_parse(dsl_json_extract, strlen(dsl_json_extract), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "json-extract") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "column")->valuestring, "_line") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "path")->valuestring, "/user/id") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "result")->valuestring, "user_id") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "type")->valuestring, "int") == 0);
    tf_ir_plan_free(plan);

    const char *dsl_json_extract_col = "jsonl | json-extract payload $.city city | csv";
    plan = tf_dsl_parse(dsl_json_extract_col, strlen(dsl_json_extract_col), &error);
    assert(plan != NULL);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "column")->valuestring, "payload") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "path")->valuestring, "$.city") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "result")->valuestring, "city") == 0);
    tf_ir_plan_free(plan);

    /* json-filter */
    const char *dsl_json_filter = "text | json-filter /user/age >= 30 type=float | csv";
    plan = tf_dsl_parse(dsl_json_filter, strlen(dsl_json_filter), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "json-filter") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "column")->valuestring, "_line") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "path")->valuestring, "/user/age") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "op")->valuestring, ">=") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "value")->valuestring, "30") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "type")->valuestring, "float") == 0);
    tf_ir_plan_free(plan);

    const char *dsl_json_filter_col = "jsonl | json-filter payload $.city == NY | csv";
    plan = tf_dsl_parse(dsl_json_filter_col, strlen(dsl_json_filter_col), &error);
    assert(plan != NULL);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "column")->valuestring, "payload") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "path")->valuestring, "$.city") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "op")->valuestring, "==") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "value")->valuestring, "NY") == 0);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("text | json-filter /active | csv", strlen("text | json-filter /active | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "op")->valuestring, "exists") == 0);
    tf_ir_plan_free(plan);

    /* json-schema */
    const char *dsl_json_schema = "text | json-schema required=user types=user:object mode=filter audit audit_limit=5 | csv";
    plan = tf_dsl_parse(dsl_json_schema, strlen(dsl_json_schema), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "json-schema") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "mode")->valuestring, "filter") == 0);
    assert(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit")));
    assert(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_limit")->valueint == 5);
    cJSON *schema = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "schema");
    assert(cJSON_IsObject(schema));
    assert(cJSON_IsArray(cJSON_GetObjectItemCaseSensitive(schema, "required")));
    assert(cJSON_IsObject(cJSON_GetObjectItemCaseSensitive(schema, "properties")));
    tf_ir_plan_free(plan);

    const char *dsl_filter_privacy = "csv | filter \"col(age)>0\" audit audit_include_row=false audit_redact=ssn audit-max-cell-bytes=3 | csv";
    plan = tf_dsl_parse(dsl_filter_privacy, strlen(dsl_filter_privacy), &error);
    assert(plan != NULL);
    assert(cJSON_IsFalse(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_include_row")));
    assert(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_max_cell_bytes")->valueint == 3);
    cJSON *filter_redact = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_redact");
    assert(cJSON_IsArray(filter_redact));
    assert(strcmp(cJSON_GetArrayItem(filter_redact, 0)->valuestring, "ssn") == 0);
    tf_ir_plan_free(plan);

    const char *dsl_validate_privacy = "csv | validate \"col(age)>0\" audit audit_columns=ssn audit-hash-columns=ssn audit_max_bytes=64 | csv";
    plan = tf_dsl_parse(dsl_validate_privacy, strlen(dsl_validate_privacy), &error);
    assert(plan != NULL);
    assert(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_max_bytes")->valueint == 64);
    cJSON *validate_hash = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_hash_columns");
    assert(cJSON_IsArray(validate_hash));
    assert(strcmp(cJSON_GetArrayItem(validate_hash, 0)->valuestring, "ssn") == 0);
    tf_ir_plan_free(plan);

    const char *dsl_assert_privacy = "csv | assert \"col(age)>0\" action=warn audit_columns=ssn audit_redact=ssn | csv";
    plan = tf_dsl_parse(dsl_assert_privacy, strlen(dsl_assert_privacy), &error);
    assert(plan != NULL);
    cJSON *assert_redact = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_redact");
    assert(cJSON_IsArray(assert_redact));
    assert(strcmp(cJSON_GetArrayItem(assert_redact, 0)->valuestring, "ssn") == 0);
    tf_ir_plan_free(plan);

    const char *dsl_json_schema_privacy = "text | json-schema required=user audit audit_include_row=false audit_redact=_line audit_max_cell_bytes=4 | text";
    plan = tf_dsl_parse(dsl_json_schema_privacy, strlen(dsl_json_schema_privacy), &error);
    assert(plan != NULL);
    assert(cJSON_IsFalse(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_include_row")));
    assert(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_max_cell_bytes")->valueint == 4);
    cJSON *json_redact = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_redact");
    assert(cJSON_IsArray(json_redact));
    assert(strcmp(cJSON_GetArrayItem(json_redact, 0)->valuestring, "_line") == 0);
    tf_ir_plan_free(plan);

    const char *dsl_fill_privacy = "csv | fill-null audit_redact=ssn audit_include_row=false name=x | csv";
    plan = tf_dsl_parse(dsl_fill_privacy, strlen(dsl_fill_privacy), &error);
    assert(plan != NULL);
    assert(cJSON_IsFalse(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_include_row")));
    cJSON *fill_redact = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_redact");
    assert(cJSON_IsArray(fill_redact));
    assert(strcmp(cJSON_GetArrayItem(fill_redact, 0)->valuestring, "ssn") == 0);
    tf_ir_plan_free(plan);

    const char *dsl_replace_privacy = "csv | replace ssn 111 999 audit_redact=ssn audit_max_cell_bytes=4 | csv";
    plan = tf_dsl_parse(dsl_replace_privacy, strlen(dsl_replace_privacy), &error);
    assert(plan != NULL);
    cJSON *replace_redact = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_redact");
    assert(cJSON_IsArray(replace_redact));
    assert(strcmp(cJSON_GetArrayItem(replace_redact, 0)->valuestring, "ssn") == 0);
    assert(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_max_cell_bytes")->valueint == 4);
    tf_ir_plan_free(plan);

    const char *dsl_cast_privacy = "csv | cast secret=int audit_redact=secret audit_include_row=false | csv";
    plan = tf_dsl_parse(dsl_cast_privacy, strlen(dsl_cast_privacy), &error);
    assert(plan != NULL);
    assert(cJSON_IsFalse(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_include_row")));
    cJSON *cast_redact_privacy = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_redact");
    assert(cJSON_IsArray(cast_redact_privacy));
    assert(strcmp(cJSON_GetArrayItem(cast_redact_privacy, 0)->valuestring, "secret") == 0);
    tf_ir_plan_free(plan);

    const char *dsl_normalize_privacy = "csv | normalize score audit_columns=score audit_redact=score audit_max_bytes=64 | csv";
    plan = tf_dsl_parse(dsl_normalize_privacy, strlen(dsl_normalize_privacy), &error);
    assert(plan != NULL);
    cJSON *normalize_redact = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_redact");
    assert(cJSON_IsArray(normalize_redact));
    assert(strcmp(cJSON_GetArrayItem(normalize_redact, 0)->valuestring, "score") == 0);
    assert(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_max_bytes")->valueint == 64);
    tf_ir_plan_free(plan);


    /* schema */
    const char *dsl_schema = "csv | schema name:string age:int city:string non_null=name,age min=age:0 max=age:120 values=city:NY,LA allow_extra_columns=false require_values_seen=true mode=quarantine result=schema_ok audit audit_limit=2 | csv";
    plan = tf_dsl_parse(dsl_schema, strlen(dsl_schema), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "schema") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "mode")->valuestring, "quarantine") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "result")->valuestring, "schema_ok") == 0);
    assert(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit")));
    assert(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_limit")->valueint == 2);
    cJSON *schema_cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    assert(cJSON_IsObject(schema_cols));
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(schema_cols, "name")->valuestring, "string") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(schema_cols, "age")->valuestring, "int") == 0);
    cJSON *schema_non_null = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "non_null");
    assert(cJSON_IsArray(schema_non_null));
    assert(cJSON_GetArraySize(schema_non_null) == 2);
    cJSON *schema_min = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "min");
    cJSON *schema_age_min = cJSON_GetObjectItemCaseSensitive(schema_min, "age");
    assert(cJSON_IsNumber(schema_age_min) && schema_age_min->valuedouble == 0.0);
    cJSON *schema_values = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "values");
    cJSON *schema_city_values = cJSON_GetObjectItemCaseSensitive(schema_values, "city");
    assert(cJSON_IsArray(schema_city_values));
    assert(cJSON_GetArraySize(schema_city_values) == 2);
    assert(cJSON_IsFalse(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "allow_extra_columns")));
    assert(cJSON_IsTrue(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "require_values_seen")));
    tf_ir_plan_free(plan);

    /* json-flatten */
    const char *dsl_json_flatten = "text | json-flatten fields=/user/id:user_id:int,$.user.name:name:string | csv";
    plan = tf_dsl_parse(dsl_json_flatten, strlen(dsl_json_flatten), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "json-flatten") == 0);
    cJSON *fields = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "fields");
    assert(cJSON_IsArray(fields));
    assert(cJSON_GetArraySize(fields) == 2);
    cJSON *field0 = cJSON_GetArrayItem(fields, 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(field0, "path")->valuestring, "/user/id") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(field0, "name")->valuestring, "user_id") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(field0, "type")->valuestring, "int") == 0);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("jsonl | json-flatten payload fields=$.city:city,$.zip:zip:int | csv", strlen("jsonl | json-flatten payload fields=$.city:city,$.zip:zip:int | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "column")->valuestring, "payload") == 0);
    assert(cJSON_GetArraySize(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "fields")) == 2);
    tf_ir_plan_free(plan);

    /* flatten */
    plan = tf_dsl_parse("jsonl | flatten | jsonl", 23, &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "flatten") == 0);
    tf_ir_plan_free(plan);

    /* trim */
    plan = tf_dsl_parse("csv | trim name | csv", 21, &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "trim") == 0);
    tf_ir_plan_free(plan);

    /* explode */
    const char *dsl_explode = "csv | explode tags ; | csv";
    plan = tf_dsl_parse(dsl_explode, strlen(dsl_explode), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "explode") == 0);
    tf_ir_plan_free(plan);

    /* datetime */
    const char *dsl_dt = "csv | datetime date year,month,day | csv";
    plan = tf_dsl_parse(dsl_dt, strlen(dsl_dt), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "datetime") == 0);
    tf_ir_plan_free(plan);

    const char *dsl_dt_policy = "csv | datetime date year,month missing=null on_type_error=null | csv";
    plan = tf_dsl_parse(dsl_dt_policy, strlen(dsl_dt_policy), &error);
    assert(plan != NULL);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "missing")->valuestring, "null") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "on_type_error")->valuestring, "null") == 0);
    tf_ir_plan_free(plan);

    const char *dsl_date_trunc = "csv | date-trunc date month result=date_month missing=null on_type_error=null | csv";
    plan = tf_dsl_parse(dsl_date_trunc, strlen(dsl_date_trunc), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "date-trunc") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "result")->valuestring, "date_month") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "missing")->valuestring, "null") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "on_type_error")->valuestring, "null") == 0);
    tf_ir_plan_free(plan);
}

static void test_dsl_sort_head_rewrite(void) {
    char *error = NULL;
    tf_ir_plan *plan;

    plan = tf_dsl_parse("csv | sort -score | head 2 | csv",
                        strlen("csv | sort -score | head 2 | csv"), &error);
    assert(plan != NULL);
    assert(plan->n_nodes == 3);
    assert(strcmp(plan->nodes[1].op, "top") == 0);
    cJSON *col = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "column");
    cJSON *desc = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "desc");
    assert(cJSON_IsString(col) && strcmp(col->valuestring, "score") == 0);
    assert(cJSON_IsTrue(desc));
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | sort score | head 2 | csv",
                        strlen("csv | sort score | head 2 | csv"), &error);
    assert(plan != NULL);
    assert(plan->n_nodes == 3);
    assert(strcmp(plan->nodes[1].op, "bottom-k") == 0);
    desc = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "desc");
    assert(cJSON_IsFalse(desc));
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | sort score name | head 2 | csv",
                        strlen("csv | sort score name | head 2 | csv"), &error);
    assert(plan != NULL);
    assert(plan->n_nodes == 4);
    assert(strcmp(plan->nodes[1].op, "sort") == 0);
    assert(strcmp(plan->nodes[2].op, "head") == 0);
    tf_ir_plan_free(plan);
}

static void test_dsl_compatibility_forms(void) {
    char *error = NULL;
    tf_ir_plan *plan;

    plan = tf_dsl_parse("csv | head 0 | csv", strlen("csv | head 0 | csv"), &error);
    assert(plan != NULL);
    cJSON *n = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "n");
    assert(cJSON_IsNumber(n) && n->valueint == 0);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | clip score 0 100 | csv", strlen("csv | clip score 0 100 | csv"), &error);
    assert(plan != NULL);
    cJSON *min_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "min");
    cJSON *max_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max");
    assert(cJSON_IsNumber(min_j) && min_j->valuedouble == 0.0);
    assert(cJSON_IsNumber(max_j) && max_j->valuedouble == 100.0);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | join lookup.csv on=city | csv", strlen("csv | join lookup.csv on=city | csv"), &error);
    assert(plan != NULL);
    cJSON *on_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "on");
    assert(cJSON_IsString(on_j) && strcmp(on_j->valuestring, "city") == 0);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | group-agg city sum:price:total | csv", strlen("csv | group-agg city sum:price:total | csv"), &error);
    assert(plan != NULL);
    cJSON *aggs = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "aggs");
    cJSON *agg = cJSON_GetArrayItem(aggs, 0);
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(agg, "column");
    cJSON *func_j = cJSON_GetObjectItemCaseSensitive(agg, "func");
    assert(cJSON_IsString(col_j) && strcmp(col_j->valuestring, "price") == 0);
    assert(cJSON_IsString(func_j) && strcmp(func_j->valuestring, "sum") == 0);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | unique city max_keys=7 | csv", strlen("csv | unique city max_keys=7 | csv"), &error);
    assert(plan != NULL);
    cJSON *max_keys = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_keys");
    assert(cJSON_IsNumber(max_keys) && (int)max_keys->valuedouble == 7);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | unique city max_state_bytes=4096 | csv", strlen("csv | unique city max_state_bytes=4096 | csv"), &error);
    assert(plan != NULL);
    cJSON *max_state_bytes = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_state_bytes");
    assert(cJSON_IsNumber(max_state_bytes) && (int)max_state_bytes->valuedouble == 4096);
    tf_ir_plan_free(plan);

    const char *unique_spill_dsl = "csv | unique city spill_dir=/tmp/tranfi-dsl spill_run_rows=2 spill_output_rows=3 spill_memory_bytes=4096 | csv";
    plan = tf_dsl_parse(unique_spill_dsl, strlen(unique_spill_dsl), &error);
    assert(plan != NULL);
    cJSON *unique_spill_dir = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "spill_dir");
    cJSON *unique_spill_rows = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "spill_run_rows");
    cJSON *unique_output_rows = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "spill_output_rows");
    cJSON *unique_spill_memory = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "spill_memory_bytes");
    assert(cJSON_IsString(unique_spill_dir) && strcmp(unique_spill_dir->valuestring, "/tmp/tranfi-dsl") == 0);
    assert(cJSON_IsNumber(unique_spill_rows) && (int)unique_spill_rows->valuedouble == 2);
    assert(cJSON_IsNumber(unique_output_rows) && (int)unique_output_rows->valuedouble == 3);
    assert(cJSON_IsNumber(unique_spill_memory) && (int)unique_spill_memory->valuedouble == 4096);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | group-agg city sum:price:total max_groups=5 | csv", strlen("csv | group-agg city sum:price:total max_groups=5 | csv"), &error);
    assert(plan != NULL);
    cJSON *max_groups = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_groups");
    assert(cJSON_IsNumber(max_groups) && (int)max_groups->valuedouble == 5);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | group-agg city sum:price:total max_state_bytes=8192 | csv", strlen("csv | group-agg city sum:price:total max_state_bytes=8192 | csv"), &error);
    assert(plan != NULL);
    cJSON *group_max_state_bytes = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_state_bytes");
    assert(cJSON_IsNumber(group_max_state_bytes) && (int)group_max_state_bytes->valuedouble == 8192);
    tf_ir_plan_free(plan);

    const char *group_spill_dsl = "csv | group-agg city sum:price:total spill_dir=/tmp/tranfi-dsl spill_run_rows=2 spill_output_rows=3 spill_memory_bytes=4096 | csv";
    plan = tf_dsl_parse(group_spill_dsl, strlen(group_spill_dsl), &error);
    assert(plan != NULL);
    cJSON *group_spill_dir = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "spill_dir");
    cJSON *group_spill_rows = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "spill_run_rows");
    cJSON *group_output_rows = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "spill_output_rows");
    cJSON *group_spill_memory = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "spill_memory_bytes");
    assert(cJSON_IsString(group_spill_dir) && strcmp(group_spill_dir->valuestring, "/tmp/tranfi-dsl") == 0);
    assert(cJSON_IsNumber(group_spill_rows) && (int)group_spill_rows->valuedouble == 2);
    assert(cJSON_IsNumber(group_output_rows) && (int)group_output_rows->valuedouble == 3);
    assert(cJSON_IsNumber(group_spill_memory) && (int)group_spill_memory->valuedouble == 4096);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | group-agg city sum:price:total sorted=true | csv", strlen("csv | group-agg city sum:price:total sorted=true | csv"), &error);
    assert(plan != NULL);
    cJSON *group_sorted = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "sorted");
    assert(cJSON_IsBool(group_sorted) && cJSON_IsTrue(group_sorted));
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | group-agg city sum:price:total --sorted | csv", strlen("csv | group-agg city sum:price:total --sorted | csv"), &error);
    assert(plan != NULL);
    group_sorted = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "sorted");
    assert(cJSON_IsBool(group_sorted) && cJSON_IsTrue(group_sorted));
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | frequency city max_values=3 | csv", strlen("csv | frequency city max_values=3 | csv"), &error);
    assert(plan != NULL);
    cJSON *max_values = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_values");
    assert(cJSON_IsNumber(max_values) && (int)max_values->valuedouble == 3);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | frequency city max_state_bytes=4096 | csv", strlen("csv | frequency city max_state_bytes=4096 | csv"), &error);
    assert(plan != NULL);
    cJSON *freq_max_state_bytes = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_state_bytes");
    assert(cJSON_IsNumber(freq_max_state_bytes) && (int)freq_max_state_bytes->valuedouble == 4096);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | rowid city result=within_key max_state_bytes=2048 | csv", strlen("csv | rowid city result=within_key max_state_bytes=2048 | csv"), &error);
    assert(plan != NULL);
    cJSON *rowid_max_state_bytes = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_state_bytes");
    assert(cJSON_IsNumber(rowid_max_state_bytes) && (int)rowid_max_state_bytes->valuedouble == 2048);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | mutate total=col('price')*2 | csv", strlen("csv | mutate total=col('price')*2 | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "derive") == 0);
    cJSON *derive_cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    cJSON *derive_col = cJSON_GetArrayItem(derive_cols, 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(derive_col, "name")->valuestring, "total") == 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(derive_col, "expr")->valuestring, "col('price')*2") == 0);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | summarize city sum:price:total | csv", strlen("csv | summarize city sum:price:total | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "group-agg") == 0);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | summarise city count:*:rows | csv", strlen("csv | summarise city count:*:rows | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "group-agg") == 0);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | distinct city max_keys=7 | csv", strlen("csv | distinct city max_keys=7 | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "unique") == 0);
    cJSON *distinct_max_keys = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_keys");
    assert(cJSON_IsNumber(distinct_max_keys) && (int)distinct_max_keys->valuedouble == 7);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | arrange -score name | csv", strlen("csv | arrange -score name | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "sort") == 0);
    cJSON *sort_cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    assert(cJSON_GetArraySize(sort_cols) == 2);
    tf_ir_plan_free(plan);

    const char *freq_overflow_dsl = "csv | frequency city max_values=2 overflow=other other=REST audit audit_limit=5 | csv";
    plan = tf_dsl_parse(freq_overflow_dsl, strlen(freq_overflow_dsl), &error);
    assert(plan != NULL);
    max_values = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_values");
    cJSON *freq_overflow = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "overflow");
    cJSON *freq_other = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "other");
    cJSON *freq_audit = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit");
    cJSON *freq_audit_limit = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "audit_limit");
    assert(cJSON_IsNumber(max_values) && (int)max_values->valuedouble == 2);
    assert(cJSON_IsString(freq_overflow) && strcmp(freq_overflow->valuestring, "other") == 0);
    assert(cJSON_IsString(freq_other) && strcmp(freq_other->valuestring, "REST") == 0);
    assert(cJSON_IsTrue(freq_audit));
    assert(cJSON_IsNumber(freq_audit_limit) && (int)freq_audit_limit->valuedouble == 5);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | frequency city overflow=other | csv", strlen("csv | frequency city overflow=other | csv"), &error);
    assert(plan == NULL);
    assert(error != NULL && strstr(error, "requires max_values") != NULL);
    free(error);
    error = NULL;

    plan = tf_dsl_parse("csv | onehot city categories=NY,LA max_categories=3 max_state_bytes=4096 unknown=other --drop | csv", strlen("csv | onehot city categories=NY,LA max_categories=3 max_state_bytes=4096 unknown=other --drop | csv"), &error);
    assert(plan != NULL);
    cJSON *onehot_cats = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "categories");
    cJSON *onehot_unknown = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "unknown");
    cJSON *onehot_drop = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "drop");
    cJSON *onehot_max = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_categories");
    cJSON *onehot_max_state = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_state_bytes");
    assert(cJSON_IsArray(onehot_cats) && cJSON_GetArraySize(onehot_cats) == 2);
    assert(cJSON_IsString(onehot_unknown) && strcmp(onehot_unknown->valuestring, "other") == 0);
    assert(cJSON_IsBool(onehot_drop) && cJSON_IsTrue(onehot_drop));
    assert(cJSON_IsNumber(onehot_max) && (int)onehot_max->valuedouble == 3);
    assert(cJSON_IsNumber(onehot_max_state) && (int)onehot_max_state->valuedouble == 4096);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | label-encode city city_id categories=NY,LA max_categories=3 max_state_bytes=4096 unknown=null | csv", strlen("csv | label-encode city city_id categories=NY,LA max_categories=3 max_state_bytes=4096 unknown=null | csv"), &error);
    assert(plan != NULL);
    cJSON *label_cats = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "categories");
    cJSON *label_unknown = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "unknown");
    cJSON *label_result = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "result");
    cJSON *label_max = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_categories");
    cJSON *label_max_state = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_state_bytes");
    assert(cJSON_IsArray(label_cats) && cJSON_GetArraySize(label_cats) == 2);
    assert(cJSON_IsString(label_unknown) && strcmp(label_unknown->valuestring, "null") == 0);
    assert(cJSON_IsString(label_result) && strcmp(label_result->valuestring, "city_id") == 0);
    assert(cJSON_IsNumber(label_max) && (int)label_max->valuedouble == 3);
    assert(cJSON_IsNumber(label_max_state) && (int)label_max_state->valuedouble == 4096);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | unique city max_keys=0 | csv", strlen("csv | unique city max_keys=0 | csv"), &error);
    assert(plan == NULL);
    assert(error != NULL && strstr(error, "max_keys") != NULL);
    free(error);
    error = NULL;

    plan = tf_dsl_parse("csv | unique city mode=maybe | csv", strlen("csv | unique city mode=maybe | csv"), &error);
    assert(plan == NULL);
    assert(error != NULL && strstr(error, "mode") != NULL);
    free(error);
    error = NULL;

    plan = tf_dsl_parse("csv | onehot city unknown=ignore | csv", strlen("csv | onehot city unknown=ignore | csv"), &error);
    assert(plan == NULL);
    assert(error != NULL && strstr(error, "unknown") != NULL);
    free(error);
    error = NULL;

    plan = tf_dsl_parse("csv | label-encode city max_categories=0 | csv", strlen("csv | label-encode city max_categories=0 | csv"), &error);
    assert(plan == NULL);
    assert(error != NULL && strstr(error, "max_categories") != NULL);
    free(error);
    error = NULL;
}

static void test_pipeline_rejects_empty_delimiters(void) {
    const char *explode_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"explode\",\"args\":{\"column\":\"tags\",\"delimiter\":\"\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(explode_plan, strlen(explode_plan));
    assert(p == NULL);

    const char *split_plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"split\",\"args\":{\"column\":\"name\",\"delimiter\":\"\",\"names\":[\"first\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    p = tf_pipeline_create(split_plan, strlen(split_plan));
    assert(p == NULL);
}

static void test_registry_new_ops(void) {
    const char *new_ops[] = { "skip", "derive", "source-name", "across", "stats", "scan", "unique", "intersect", "setdiff", "intersect-all", "setdiff-all", "union", "union-all", "sort", "top-k", "bottom-k", "slice-head", "slice-tail", "slice-min", "slice-max", "assert", "quarantine", "tee", "schema", "schema-infer", "lag", "shift", "rowid", "rleid", "rolling-sum", "rolling-mean", "rolling-min", "rolling-max", "rolling-any", "rolling-all" };
    for (size_t i = 0; i < sizeof(new_ops) / sizeof(new_ops[0]); i++) {
        const tf_op_entry *e = tf_op_registry_find(new_ops[i]);
        assert(e != NULL);
        assert(strcmp(e->name, new_ops[i]) == 0);
        assert(e->kind == TF_OP_TRANSFORM);
    }
}

static void test_registry_count_updated(void) {
    size_t count = tf_op_registry_count();
    assert(count == 91);  /* 7 codecs + 84 transforms */
}

/* ================================================================
 * Date/Timestamp tests
 * ================================================================ */

static void test_csv_date_autodetect(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "date\n2024-03-15\n2023-12-25\n1970-01-01\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* Roundtrip: dates should come back as YYYY-MM-DD */
    assert(strstr((char *)out, "2024-03-15") != NULL);
    assert(strstr((char *)out, "2023-12-25") != NULL);
    assert(strstr((char *)out, "1970-01-01") != NULL);
    tf_pipeline_free(p);
}

static void test_csv_timestamp_autodetect(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "ts\n2024-03-15T10:30:00Z\n2023-12-25T23:59:59Z\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* Roundtrip: timestamps come back as ISO 8601 */
    assert(strstr((char *)out, "2024-03-15T10:30:00Z") != NULL);
    assert(strstr((char *)out, "2023-12-25T23:59:59Z") != NULL);
    tf_pipeline_free(p);
}

static void test_csv_date_timestamp_widening(void) {
    /* Mixed date + timestamp column should widen to timestamp */
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "when\n2024-03-15\n2024-03-15T10:30:00Z\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* Date widened to timestamp at midnight */
    assert(strstr((char *)out, "2024-03-15T00:00:00Z") != NULL);
    assert(strstr((char *)out, "2024-03-15T10:30:00Z") != NULL);
    tf_pipeline_free(p);
}

static void test_cast_string_to_date(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"cast\",\"args\":{\"mapping\":{\"d\":\"date\"}}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "d,v\n2024-03-15,hello\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "2024-03-15") != NULL);
    tf_pipeline_free(p);
}

static void test_cast_date_to_timestamp(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"cast\",\"args\":{\"mapping\":{\"d\":\"timestamp\"}}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "d\n2024-03-15\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* Date cast to timestamp at midnight */
    assert(strstr((char *)out, "2024-03-15T00:00:00Z") != NULL);
    tf_pipeline_free(p);
}

static void test_filter_date_comparison(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"filter\",\"args\":{\"expr\":\"col('date') > '2024-01-01'\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,date\nAlice,2024-03-15\nBob,2023-06-01\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") == NULL);
    tf_pipeline_free(p);
}

static void test_sort_by_date(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"sort\",\"args\":{\"columns\":[{\"name\":\"date\",\"desc\":false}]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,date\nBob,2024-06-01\nAlice,2024-01-15\nCharlie,2024-03-20\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* Should be sorted: Alice (Jan) < Charlie (Mar) < Bob (Jun) */
    char *alice_pos = strstr((char *)out, "Alice");
    char *charlie_pos = strstr((char *)out, "Charlie");
    char *bob_pos = strstr((char *)out, "Bob");
    assert(alice_pos != NULL && charlie_pos != NULL && bob_pos != NULL);
    assert(alice_pos < charlie_pos);
    assert(charlie_pos < bob_pos);
    tf_pipeline_free(p);
}


static int dir_has_prefix(const char *dir, const char *prefix) {
    DIR *d = opendir(dir);
    assert(d != NULL);
    size_t n = strlen(prefix);
    struct dirent *ent;
    int found = 0;
    while ((ent = readdir(d)) != NULL) {
        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) continue;
        if (strncmp(ent->d_name, prefix, n) == 0) { found = 1; break; }
    }
    closedir(d);
    return found;
}

static void test_spill_session_security_basics(void) {
    char tmpl[] = "/tmp/tranfi_spill_sec_XXXXXX";
    char *root = mkdtemp(tmpl);
    assert(root != NULL);

    assert(chmod(root, 0777) == 0);
    tf_spill_session *session = NULL;
    assert(tf_spill_session_create(root, &session) == TF_ERROR);
    assert(session == NULL);
    assert(strstr(tf_last_error(), "world-writable") != NULL);

    assert(chmod(root, 0700) == 0);
    assert(tf_spill_session_create(root, &session) == TF_OK);
    assert(session != NULL);

    char *path = NULL;
    int fd = -1;
    assert(tf_spill_open_run(session, "sort", &fd, &path) == TF_OK);
    assert(fd >= 0);
    assert(path != NULL);
    assert(write(fd, "abc", 3) == 3);
    assert(close(fd) == 0);

    struct stat st;
    assert(stat(path, &st) == 0);
    assert((st.st_mode & 0777) == 0600);

    char session_dir[512];
    snprintf(session_dir, sizeof(session_dir), "%s", path);
    char *slash = strrchr(session_dir, '/');
    assert(slash != NULL);
    *slash = '\0';
    assert(stat(session_dir, &st) == 0);
    assert(S_ISDIR(st.st_mode));
    assert((st.st_mode & 0777) == 0700);

    assert(tf_spill_cleanup(session) == TF_OK);
    assert(access(session_dir, F_OK) != 0);
    assert(access(path, F_OK) != 0);
    free(path);
    assert(rmdir(root) == 0);
}


static void test_spill_session_cleanup_after_abort(void) {
    char tmpl[] = "/tmp/tranfi_spill_abort_XXXXXX";
    char *root = mkdtemp(tmpl);
    assert(root != NULL);

    tf_spill_session *session = NULL;
    assert(tf_spill_session_create(root, &session) == TF_OK);
    assert(session != NULL);

    char *path1 = NULL;
    char *path2 = NULL;
    int fd1 = -1;
    int fd2 = -1;
    assert(tf_spill_open_run(session, "sort", &fd1, &path1) == TF_OK);
    assert(tf_spill_open_run(session, "join", &fd2, &path2) == TF_OK);
    assert(write(fd1, "left", 4) == 4);
    assert(write(fd2, "right", 5) == 5);
    assert(close(fd1) == 0);
    assert(close(fd2) == 0);

    char session_dir[512];
    snprintf(session_dir, sizeof(session_dir), "%s", path1);
    char *slash = strrchr(session_dir, '/');
    assert(slash != NULL);
    *slash = '\0';
    assert(access(path1, F_OK) == 0);
    assert(access(path2, F_OK) == 0);
    assert(access(session_dir, F_OK) == 0);

    assert(tf_spill_cleanup(session) == TF_OK);
    assert(access(path1, F_OK) != 0);
    assert(access(path2, F_OK) != 0);
    assert(access(session_dir, F_OK) != 0);
    free(path1);
    free(path2);
    assert(rmdir(root) == 0);
}

static void test_spill_sort_uses_private_session_dir(void) {
    char tmpl[] = "/tmp/tranfi_spill_symlink_XXXXXX";
    char *spill_dir = mkdtemp(tmpl);
    assert(spill_dir != NULL);

    char victim[512];
    char old_path[512];
    snprintf(victim, sizeof(victim), "%s/victim.txt", spill_dir);
    snprintf(old_path, sizeof(old_path), "%s/tranfi-sort-%ld-0.bin", spill_dir, (long)getpid());
    FILE *vf = fopen(victim, "wb");
    assert(vf != NULL);
    assert(fwrite("sentinel", 1, 8, vf) == 8);
    assert(fclose(vf) == 0);
    assert(symlink(victim, old_path) == 0);

    char plan[1024];
    snprintf(plan, sizeof(plan),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
             "{\"op\":\"sort\",\"args\":{\"columns\":[{\"name\":\"score\"}],"
             "\"spill_dir\":\"%s\",\"spill_run_rows\":1,\"spill_output_rows\":1}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}"
             "]}", spill_dir);
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,score\nB,2\nA,1\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[256];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    out[n] = 0;
    assert(strstr((char *)out, "A,1") != NULL);
    assert(strstr((char *)out, "B,2") != NULL);
    tf_pipeline_free(p);

    char buf[32] = {0};
    vf = fopen(victim, "rb");
    assert(vf != NULL);
    assert(fread(buf, 1, 8, vf) == 8);
    assert(fclose(vf) == 0);
    assert(strcmp(buf, "sentinel") == 0);
    struct stat lst;
    assert(lstat(old_path, &lst) == 0);
    assert(S_ISLNK(lst.st_mode));
    assert(!dir_has_prefix(spill_dir, "tranfi-spill-"));

    assert(unlink(old_path) == 0);
    assert(unlink(victim) == 0);
    assert(rmdir(spill_dir) == 0);
}

static void test_spill_sort_typed_multi_key_null_ordering(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_core_spill_typed_%ld", (long)getpid());
    rmdir(spill_dir);
    assert(mkdir(spill_dir, 0700) == 0);

    char plan[2048];
    snprintf(plan, sizeof(plan),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2,\"nulls\":[\"NA\"]}},"
             "{\"op\":\"sort\",\"args\":{\"columns\":["
             "{\"name\":\"grp\",\"desc\":false},"
             "{\"name\":\"d\",\"desc\":true},"
             "{\"name\":\"ts\",\"desc\":true},"
             "{\"name\":\"score\",\"desc\":false},"
             "{\"name\":\"flag\",\"desc\":true},"
             "{\"name\":\"name\",\"desc\":false}],"
             "\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}"
             "]}", spill_dir);

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv =
        "id,grp,d,ts,score,flag,name\n"
        "r1,B,2024-01-01,2024-01-01T09:00:00Z,1.5,true,zeta\n"
        "r2,A,2024-01-02,2024-01-02T07:00:00Z,3.0,false,beta\n"
        "r3,A,2024-01-02,2024-01-02T07:00:00Z,2.0,true,alpha\n"
        "r4,A,2024-01-02,2024-01-02T07:00:00Z,NA,false,gamma\n"
        "r5,A,2024-01-01,2024-01-01T12:00:00Z,0.5,true,delta\n"
        "r6,B,2024-01-01,2024-01-01T10:00:00Z,1.0,false,eta\n"
        "r7,A,2024-01-02,2024-01-02T07:00:00Z,2.0,false,aardvark\n"
        "r8,A,2024-01-02,2024-01-02T07:00:00Z,2.0,false,bravo\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[4096];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    char *s = (char *)out;

    const char *ids[] = {"r3,", "r7,", "r8,", "r2,", "r4,", "r5,", "r6,", "r1,"};
    char *prev = strstr(s, "id,grp,d,ts,score,flag,name");
    assert(prev != NULL);
    for (size_t i = 0; i < sizeof(ids) / sizeof(ids[0]); i++) {
        char *pos = strstr(s, ids[i]);
        assert(pos != NULL);
        assert(prev < pos);
        prev = pos;
    }
    assert(strstr(s, "r4,A,2024-01-02,2024-01-02T07:00:00Z,,false,gamma") != NULL);

    uint8_t stats_buf[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats_buf, sizeof(stats_buf) - 1);
    assert(stats_n > 0);
    stats_buf[stats_n] = '\0';
    assert(strstr((char *)stats_buf, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_bytes\":") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_runs\":4") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_output_batches\":4") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_output_rows\":8") != NULL);
    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
}

static void test_spill_sort_stable_equal_keys(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_core_spill_sort_stable_%ld", (long)getpid());
    rmdir(spill_dir);
    assert(mkdir(spill_dir, 0700) == 0);

    char plan[2048];
    snprintf(plan, sizeof(plan),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
             "{\"op\":\"sort\",\"args\":{\"columns\":[{\"name\":\"age\",\"desc\":false}],"
             "\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}"
             "]}", spill_dir);

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv =
        "name,age\n"
        "Alice,20\n"
        "Bob,20\n"
        "Cara,20\n"
        "Drew,30\n"
        "Eve,30\n"
        "Finn,30\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    char *s = (char *)out;

    const char *rows[] = {
        "Alice,20",
        "Bob,20",
        "Cara,20",
        "Drew,30",
        "Eve,30",
        "Finn,30"
    };
    char *prev = strstr(s, "name,age");
    assert(prev != NULL);
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        char *pos = strstr(s, rows[i]);
        assert(pos != NULL);
        assert(prev < pos);
        prev = pos;
    }

    uint8_t stats_buf[2048];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats_buf, sizeof(stats_buf) - 1);
    assert(stats_n > 0);
    stats_buf[stats_n] = '\0';
    assert(strstr((char *)stats_buf, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_runs\":3") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_output_rows\":6") != NULL);

    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
}


static void test_spill_unique_preserves_first_row_order(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_core_spill_unique_%ld", (long)getpid());
    rmdir(spill_dir);
    assert(mkdir(spill_dir, 0700) == 0);

    char plan[2048];
    snprintf(plan, sizeof(plan),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
             "{\"op\":\"unique\",\"args\":{\"columns\":[\"id\"],"
             "\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}"
             "]}", spill_dir);

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv =
        "id,name,score\n"
        "3,C,30\n"
        "1,A,10\n"
        "2,B,20\n"
        "1,A2,11\n"
        "3,C2,31\n"
        "4,D,40\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    char *s = (char *)out;
    const char *rows[] = {"3,C,30", "1,A,10", "2,B,20", "4,D,40"};
    char *prev = strstr(s, "id,name,score");
    assert(prev != NULL);
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        char *pos = strstr(s, rows[i]);
        assert(pos != NULL);
        assert(prev < pos);
        prev = pos;
    }
    assert(strstr(s, "1,A2,11") == NULL);
    assert(strstr(s, "3,C2,31") == NULL);

    uint8_t stats_buf[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats_buf, sizeof(stats_buf) - 1);
    assert(stats_n > 0);
    stats_buf[stats_n] = '\0';
    assert(strstr((char *)stats_buf, "\"op\":\"unique\"") != NULL);
    assert(strstr((char *)stats_buf, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats_buf, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats_buf, "\"emit_class\":\"on_flush\"") != NULL);
    assert(strstr((char *)stats_buf, "\"warnings\":[\"flush_latent\"]") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_bytes\":") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_runs\":") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_output_batches\":2") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_output_rows\":4") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_distinct_rows\":4") != NULL);

    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
}


static void test_spill_group_agg_preserves_first_group_order(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_core_spill_group_agg_%ld", (long)getpid());
    rmdir(spill_dir);
    assert(mkdir(spill_dir, 0700) == 0);

    char plan[3072];
    snprintf(plan, sizeof(plan),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
             "{\"op\":\"group-agg\",\"args\":{\"group_by\":[\"city\"],"
             "\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2,"
             "\"aggs\":["
             "{\"column\":\"sales\",\"func\":\"sum\",\"name\":\"total\"},"
             "{\"column\":\"sales\",\"func\":\"count\",\"name\":\"n\"},"
             "{\"column\":\"*\",\"func\":\"count\",\"name\":\"rows\"}]}} ,"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}"
             "]}", spill_dir);

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *csv =
        "city,sales\n"
        "B,10\n"
        "A,1\n"
        "C,5\n"
        "A,2\n"
        "B,3\n"
        "D,7\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    char *text = (char *)out;
    const char *rows[] = {"B,13,2,2", "A,3,2,2", "C,5,1,1", "D,7,1,1"};
    char *prev = strstr(text, "city,total,n,rows");
    assert(prev != NULL);
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        char *pos = strstr(text, rows[i]);
        assert(pos != NULL);
        assert(prev < pos);
        prev = pos;
    }

    uint8_t stats_buf[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats_buf, sizeof(stats_buf) - 1);
    assert(stats_n > 0);
    stats_buf[stats_n] = '\0';
    assert(strstr((char *)stats_buf, "\"op\":\"group-agg\"") != NULL);
    assert(strstr((char *)stats_buf, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats_buf, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats_buf, "\"emit_class\":\"on_flush\"") != NULL);
    assert(strstr((char *)stats_buf, "\"warnings\":[\"flush_latent\"]") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_bytes\":") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_runs\":") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_output_batches\":2") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_output_rows\":4") != NULL);
    assert(strstr((char *)stats_buf, "\"spill_distinct_groups\":4") != NULL);

    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
}

static void test_datetime_native_date(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"datetime\",\"args\":{\"column\":\"d\",\"extract\":[\"year\",\"month\",\"day\",\"weekday\"]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    /* 2024-03-15 is a Friday (weekday=5) */
    const char *csv = "d\n2024-03-15\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "d_year") != NULL);
    assert(strstr((char *)out, "2024") != NULL);
    assert(strstr((char *)out, ",3,") != NULL || strstr((char *)out, ",3\n") != NULL);  /* month */
    assert(strstr((char *)out, ",15,") != NULL || strstr((char *)out, ",15\n") != NULL); /* day */
    tf_pipeline_free(p);
}

/* ================================================================
 * Pivot tests
 * ================================================================ */

static void test_pipeline_pivot_first(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"pivot\",\"args\":{\"name_column\":\"metric\",\"value_column\":\"value\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,metric,value\nA,x,1\nA,y,2\nB,x,3\nB,y,4\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* Should have name,x,y columns with A row and B row */
    assert(strstr((char *)out, "name") != NULL);
    assert(strstr((char *)out, ",x,") != NULL || strstr((char *)out, ",x\n") != NULL ||
           strstr((char *)out, "\nx,") != NULL || strstr((char *)out, "x") != NULL);
    /* Values: A has x=1,y=2; B has x=3,y=4 */
    assert(strstr((char *)out, "A") != NULL);
    assert(strstr((char *)out, "B") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_pivot_sum(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"pivot\",\"args\":{\"name_column\":\"metric\",\"value_column\":\"value\",\"agg\":\"sum\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    /* A has x twice: 1+10=11 */
    const char *csv = "name,metric,value\nA,x,1\nA,x,10\nA,y,2\nB,x,3\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* A's x should be 11 */
    assert(strstr((char *)out, "11") != NULL);
    tf_pipeline_free(p);
}


static void test_pipeline_pivot_spill_preserves_group_and_category_order(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_core_pivot_spill_%ld", (long)getpid());
    rmdir(spill_dir);
    assert(mkdir(spill_dir, 0700) == 0);

    char plan[2048];
    snprintf(plan, sizeof(plan),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
             "{\"op\":\"pivot\",\"args\":{\"name_column\":\"metric\",\"value_column\":\"value\",\"agg\":\"sum\","
             "\"max_categories\":2,\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":1}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}"
             "]}", spill_dir);

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv =
        "id,metric,value\n"
        "B,y,4\n"
        "A,x,1\n"
        "B,x,3\n"
        "A,y,2\n"
        "A,x,5\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    uint8_t out[2048];
    assert(tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out)) == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    char *text = (char *)out;
    assert(strstr(text, "id,y,x") != NULL);
    char *b = strstr(text, "B,4,3");
    char *a = strstr(text, "A,2,6");
    assert(b != NULL && a != NULL && b < a);

    uint8_t stats_buf[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats_buf, sizeof(stats_buf) - 1);
    assert(stats_n > 0);
    stats_buf[stats_n] = '\0';
    char *stats = (char *)stats_buf;
    assert(strstr(stats, "\"op\":\"pivot\"") != NULL);
    assert(strstr(stats, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr(stats, "\"memory_class\":\"external\"") != NULL);
    assert(strstr(stats, "\"schema_class\":\"data_dependent\"") != NULL);
    assert(strstr(stats, "\"tracked_categories\":2") != NULL);
    assert(strstr(stats, "\"spill_output_batches\":2") != NULL);
    assert(strstr(stats, "\"spill_output_rows\":2") != NULL);
    assert(strstr(stats, "\"spill_distinct_groups\":2") != NULL);

    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
}

static void test_pipeline_pivot_sorted_declared_streaming(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"pivot\",\"args\":{\"name_column\":\"metric\",\"value_column\":\"value\",\"agg\":\"sum\",\"categories\":[\"x\",\"y\"],\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);

    const char *part1 = "name,metric,value\nA,x,1\nA,y,2\n";
    assert(tf_pipeline_push(p, (const uint8_t *)part1, strlen(part1)) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n == 0);

    const char *part2 = "B,x,3\n";
    assert(tf_pipeline_push(p, (const uint8_t *)part2, strlen(part2)) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "name,x,y") != NULL);
    assert(strstr((char *)out, "A,1,2") != NULL);
    assert(strstr((char *)out, "B,3") == NULL);

    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "B,3,") != NULL || strstr((char *)out, "B,3\n") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_pivot_declared_unknown_category_errors(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"pivot\",\"args\":{\"name_column\":\"metric\",\"value_column\":\"value\",\"agg\":\"sum\",\"categories\":[\"x\"],\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,metric,value\nA,z,1\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) != TF_OK);
    assert(tf_last_error() != NULL && strstr(tf_last_error(), "unknown category") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_pivot_max_categories_errors(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"pivot\",\"args\":{\"name_column\":\"metric\",\"value_column\":\"value\",\"agg\":\"sum\",\"max_categories\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,metric,value\nA,x,1\nA,y,2\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) != TF_OK);
    assert(tf_last_error() != NULL && strstr(tf_last_error(), "max_categories=1") != NULL);
    tf_pipeline_free(p);
}

static void test_dsl_pivot(void) {
    char *error = NULL;
    const char *dsl = "csv | pivot metric value sum categories=x,y sorted=true max_categories=2 | csv";
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "pivot") == 0);
    cJSON *nc = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "name_column");
    assert(nc != NULL && strcmp(nc->valuestring, "metric") == 0);
    cJSON *vc = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "value_column");
    assert(vc != NULL && strcmp(vc->valuestring, "value") == 0);
    cJSON *agg = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "agg");
    assert(agg != NULL && strcmp(agg->valuestring, "sum") == 0);
    cJSON *cats = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "categories");
    assert(cJSON_IsArray(cats) && cJSON_GetArraySize(cats) == 2);
    cJSON *sorted = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "sorted");
    assert(cJSON_IsTrue(sorted));
    cJSON *max_categories = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_categories");
    assert(cJSON_IsNumber(max_categories) && max_categories->valueint == 2);
    assert(tf_ir_validate(plan) == TF_OK);
    assert(plan->nodes[1].memory_class == TF_MEM_BOUNDED_STATE);
    assert(plan->nodes[1].emit_class == TF_EMIT_MIXED);
    tf_ir_plan_free(plan);
    free(error);
}

/* ================================================================
 * Join tests
 * ================================================================ */

static void test_pipeline_join_inner(void) {
    /* Write temp lookup file */
    const char *lookup_path = "/tmp/tranfi_test_lookup.csv";
    FILE *f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,city\n1,London\n2,Paris\n3,Tokyo\n");
    fclose(f);

    char plan_buf[512];
    snprintf(plan_buf, sizeof(plan_buf),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"how\":\"inner\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    tf_pipeline *p = tf_pipeline_create(plan_buf, strlen(plan_buf));
    assert(p != NULL);
    const char *csv = "id,name\n1,Alice\n2,Bob\n4,Dave\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* Inner: Alice+London, Bob+Paris. Dave (id=4) excluded. */
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "London") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);
    assert(strstr((char *)out, "Paris") != NULL);
    assert(strstr((char *)out, "Dave") == NULL);
    tf_pipeline_free(p);
    remove(lookup_path);
}

static void test_pipeline_join_left(void) {
    const char *lookup_path = "/tmp/tranfi_test_lookup2.csv";
    FILE *f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,city\n1,London\n2,Paris\n");
    fclose(f);

    char plan_buf[512];
    snprintf(plan_buf, sizeof(plan_buf),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"how\":\"left\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    tf_pipeline *p = tf_pipeline_create(plan_buf, strlen(plan_buf));
    assert(p != NULL);
    const char *csv = "id,name\n1,Alice\n3,Charlie\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* Left: Alice+London, Charlie+null */
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "London") != NULL);
    assert(strstr((char *)out, "Charlie") != NULL);
    /* Charlie's city should be empty (null encoded as empty in CSV) */
    tf_pipeline_free(p);
    remove(lookup_path);
}

static void test_pipeline_filtering_joins(void) {
    const char *lookup_path = "/tmp/tranfi_test_filtering_lookup.csv";
    FILE *f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,val\n1,a\n1,b\n3,c\n");
    fclose(f);

    char plan[2048];
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"semi-join\",\"args\":{\"file\":\"%s\",\"on\":\"id\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "id,name\n1,Alice\n2,Bob\n3,Charlie\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "id,name") != NULL);
    assert(strstr((char *)out, "val") == NULL);
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Charlie") != NULL);
    assert(strstr((char *)out, "Bob") == NULL);
    assert(strstr((char *)out, "Alice\n1,Alice") == NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"anti-join\",\"args\":{\"file\":\"%s\",\"on\":\"id\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Bob") != NULL);
    assert(strstr((char *)out, "Alice") == NULL);
    assert(strstr((char *)out, "Charlie") == NULL);
    tf_pipeline_free(p);
    remove(lookup_path);

    const char *empty_path = "/tmp/tranfi_test_filtering_empty_lookup.csv";
    f = fopen(empty_path, "w");
    assert(f != NULL);
    fprintf(f, "id,val\n");
    fclose(f);
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"anti-join\",\"args\":{\"file\":\"%s\",\"on\":\"id\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", empty_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);
    assert(strstr((char *)out, "Charlie") != NULL);
    tf_pipeline_free(p);
    remove(empty_path);
}


static void test_pipeline_filtering_join_spill_preserves_left_order(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_core_spill_join_%ld", (long)getpid());
    rmdir(spill_dir);
    assert(mkdir(spill_dir, 0700) == 0);

    const char *lookup_path = "/tmp/tranfi_test_spill_filtering_lookup.csv";
    FILE *f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,val\n3,c\n1,a\n1,a2\n5,e\n9,z\n");
    fclose(f);

    char plan[1600];
    snprintf(plan, sizeof(plan),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
             "{\"op\":\"semi-join\",\"args\":{\"file\":\"%s\",\"on\":\"id\","
             "\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}"
             "]}", lookup_path, spill_dir);

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "id,name\n4,D\n1,A\n2,B\n3,C\n5,E\n6,F\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_pull(p, TF_CHAN_MAIN, (uint8_t *)plan, 1) == 0);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    char *text = (char *)out;
    const char *semi_rows[] = {"1,A", "3,C", "5,E"};
    char *prev = strstr(text, "id,name");
    assert(prev != NULL);
    for (size_t i = 0; i < sizeof(semi_rows) / sizeof(semi_rows[0]); i++) {
        char *pos = strstr(text, semi_rows[i]);
        assert(pos != NULL);
        assert(prev < pos);
        prev = pos;
    }
    assert(strstr(text, "2,B") == NULL);
    assert(strstr(text, "4,D") == NULL);
    assert(strstr(text, "6,F") == NULL);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"op\":\"semi-join\"") != NULL);
    assert(strstr((char *)stats, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats, "\"emit_class\":\"on_flush\"") != NULL);
    assert(strstr((char *)stats, "\"spill_output_rows\":3") != NULL);
    assert(strstr((char *)stats, "\"spill_kept_rows\":3") != NULL);
    assert(strstr((char *)stats, "\"lookup_keys\":4") != NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
             "{\"op\":\"anti-join\",\"args\":{\"file\":\"%s\",\"on\":\"id\","
             "\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}"
             "]}", lookup_path, spill_dir);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    text = (char *)out;
    const char *anti_rows[] = {"4,D", "2,B", "6,F"};
    prev = strstr(text, "id,name");
    assert(prev != NULL);
    for (size_t i = 0; i < sizeof(anti_rows) / sizeof(anti_rows[0]); i++) {
        char *pos = strstr(text, anti_rows[i]);
        assert(pos != NULL);
        assert(prev < pos);
        prev = pos;
    }
    assert(strstr(text, "1,A") == NULL);
    assert(strstr(text, "3,C") == NULL);
    assert(strstr(text, "5,E") == NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
             "{\"op\":\"semi-join\",\"args\":{\"file\":\"%s\",\"on\":\"id\","
             "\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2,"
             "\"max_lookup_keys\":3}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}"
             "]}", lookup_path, spill_dir);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) != TF_OK);
    assert(tf_pipeline_error(p) != NULL);
    assert(strstr(tf_pipeline_error(p), "max_lookup_keys=3") != NULL);
    tf_pipeline_free(p);

    assert(rmdir(spill_dir) == 0);
    remove(lookup_path);
}

static void test_pipeline_mutating_join_spill_preserves_left_and_match_order(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_core_spill_mutating_join_%ld", (long)getpid());
    rmdir(spill_dir);
    assert(mkdir(spill_dir, 0700) == 0);

    const char *lookup_path = "/tmp/tranfi_test_spill_mutating_lookup.csv";
    FILE *f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,city,rank\n3,CHI,30\n1,LA,10\n1,SF,11\n5,SEA,50\n9,NY,90\n");
    fclose(f);

    const char *csv = "id,name\n4,D\n1,A\n2,B\n3,C\n1,A2\n5,E\n";
    char plan[1800];
    snprintf(plan, sizeof(plan),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
             "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\","
             "\"max_matches_per_row\":2,\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}"
             "]}", lookup_path, spill_dir);

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_pull(p, TF_CHAN_MAIN, (uint8_t *)plan, 1) == 0);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[4096];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    char *text = (char *)out;
    const char *inner_rows[] = {
        "1,A,LA,10",
        "1,A,SF,11",
        "3,C,CHI,30",
        "1,A2,LA,10",
        "1,A2,SF,11",
        "5,E,SEA,50"
    };
    char *prev = strstr(text, "id,name,city,rank");
    assert(prev != NULL);
    for (size_t i = 0; i < sizeof(inner_rows) / sizeof(inner_rows[0]); i++) {
        char *pos = strstr(text, inner_rows[i]);
        assert(pos != NULL);
        assert(prev < pos);
        prev = pos;
    }
    assert(strstr(text, "4,D") == NULL);
    assert(strstr(text, "2,B") == NULL);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"op\":\"join\"") != NULL);
    assert(strstr((char *)stats, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats, "\"emit_class\":\"on_flush\"") != NULL);
    assert(strstr((char *)stats, "\"schema_class\":\"data_dependent\"") != NULL);
    assert(strstr((char *)stats, "\"spill_output_rows\":6") != NULL);
    assert(strstr((char *)stats, "\"spill_kept_rows\":6") != NULL);
    assert(strstr((char *)stats, "\"lookup_keys\":4") != NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
             "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"how\":\"left\","
             "\"max_matches_per_row\":2,\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}"
             "]}", lookup_path, spill_dir);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    text = (char *)out;
    const char *left_rows[] = {
        "4,D,,",
        "1,A,LA,10",
        "1,A,SF,11",
        "2,B,,",
        "3,C,CHI,30",
        "1,A2,LA,10",
        "1,A2,SF,11",
        "5,E,SEA,50"
    };
    prev = strstr(text, "id,name,city,rank");
    assert(prev != NULL);
    for (size_t i = 0; i < sizeof(left_rows) / sizeof(left_rows[0]); i++) {
        char *pos = strstr(text, left_rows[i]);
        assert(pos != NULL);
        assert(prev < pos);
        prev = pos;
    }
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
             "{\"steps\":["
             "{\"op\":\"codec.csv.decode\",\"args\":{}},"
             "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\","
             "\"max_matches_per_row\":1,\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2}},"
             "{\"op\":\"codec.csv.encode\",\"args\":{}}"
             "]}", lookup_path, spill_dir);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) != TF_OK);
    assert(tf_pipeline_error(p) != NULL);
    assert(strstr(tf_pipeline_error(p), "max_matches_per_row=1") != NULL);
    tf_pipeline_free(p);

    assert(rmdir(spill_dir) == 0);
    remove(lookup_path);
}

static void test_pipeline_join_typed_keys(void) {
    char plan[1200];
    uint8_t out[1024];
    size_t n;

    const char *typed_lookup = "/tmp/tranfi_test_join_typed_lookup.csv";
    FILE *f = fopen(typed_lookup, "w");
    assert(f != NULL);
    fprintf(f, "id,val\n1,string-one\nx,other\n");
    fclose(f);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", typed_lookup);
    assert_push_cap_error(plan, "id\n1\n", "join key types differ");
    remove(typed_lookup);

    const char *sentinel_lookup = "/tmp/tranfi_test_join_sentinel_lookup.csv";
    f = fopen(sentinel_lookup, "w");
    assert(f != NULL);
    fprintf(f, "id,val\n\\N,sentinel\nx,other\n");
    fclose(f);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", sentinel_lookup);
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "id,name\n,empty\n\\N,literal\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "\\N,literal,sentinel") != NULL);
    assert(strstr((char *)out, "empty") == NULL);
    tf_pipeline_free(p);
    remove(sentinel_lookup);
}


static void test_pipeline_join_caps(void) {
    const char *lookup_path = "/tmp/tranfi_test_join_caps.csv";
    FILE *f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,val\n1,a\n2,b\n");
    fclose(f);

    char plan[1024];
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"max_lookup_rows\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    assert_push_cap_error(plan, "id\n1\n", "max_lookup_rows=1");

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"max_lookup_keys\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    assert_push_cap_error(plan, "id\n1\n", "max_lookup_keys=1");

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"max_lookup_bytes\":8}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    assert_push_cap_error(plan, "id\n1\n", "max_lookup_bytes=8");

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"max_state_bytes\":512}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    assert_push_cap_error(plan, "id\n1\n", "max_state_bytes=512");

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"max_lookup_rows\":2,\"max_lookup_keys\":2,\"max_lookup_bytes\":1024,\"max_state_bytes\":65536}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)"id\n1\n2\n", strlen("id\n1\n2\n")) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    tf_pipeline_free(p);

    remove(lookup_path);
}

static void test_pipeline_join_output_caps(void) {
    const char *lookup_path = "/tmp/tranfi_test_join_output_caps.csv";
    FILE *f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,val\n1,a\n1,b\n");
    fclose(f);

    char plan[1200];
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"max_matches_per_row\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    assert_push_cap_error(plan, "id\n1\n", "max_matches_per_row=1");

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"max_output_rows\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    assert_push_cap_error(plan, "id\n1\n", "max_output_rows=1");

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"max_matches_per_row\":2,\"max_output_rows\":2,\"max_state_bytes\":65536}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)"id\n1\n", strlen("id\n1\n")) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[256];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    out[n] = '\0';
    assert(strstr((char *)out, "1,a") != NULL);
    assert(strstr((char *)out, "1,b") != NULL);
    uint8_t stats[4096];
    n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(n > 0);
    stats[n] = '\0';
    assert(strstr((char *)stats, "\"op\":\"join\"") != NULL);
    assert(strstr((char *)stats, "\"lookup_rows\":2") != NULL);
    assert(strstr((char *)stats, "\"lookup_keys\":1") != NULL);
    assert(strstr((char *)stats, "\"lookup_key_bytes\":4") != NULL);
    assert(strstr((char *)stats, "\"lookup_row_refs\":4") != NULL);
    assert(strstr((char *)stats, "\"lookup_batch_bytes\":") != NULL);
    assert(strstr((char *)stats, "\"retained_state_bytes\":") != NULL);
    assert(strstr((char *)stats, "\"max_state_bytes\":65536") != NULL);
    tf_pipeline_free(p);

    remove(lookup_path);
}


static void test_pipeline_join_sorted_mode(void) {
    const char *lookup_path = "/tmp/tranfi_test_sorted_join_lookup.csv";
    FILE *f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,city\n1,London\n2,Paris\n2,Lyon\n4,Rome\n");
    fclose(f);

    const char *csv = "id,name\n1,Alice\n2,Bob\n2,Beth\n3,Cara\n4,Dave\n";
    char plan[1200];
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"semi-join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);
    assert(strstr((char *)out, "Beth") != NULL);
    assert(strstr((char *)out, "Dave") != NULL);
    assert(strstr((char *)out, "Cara") == NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"anti-join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Cara") != NULL);
    assert(strstr((char *)out, "Alice") == NULL);
    assert(strstr((char *)out, "Bob") == NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"sorted\":true,\"max_matches_per_row\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "id,name,city") != NULL);
    assert(strstr((char *)out, "1,Alice,London") != NULL);
    assert(strstr((char *)out, "2,Bob,Paris") != NULL);
    assert(strstr((char *)out, "2,Bob,Lyon") != NULL);
    assert(strstr((char *)out, "2,Beth,Paris") != NULL);
    assert(strstr((char *)out, "4,Dave,Rome") != NULL);
    assert(strstr((char *)out, "3,Cara") == NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"how\":\"left\",\"sorted\":true,\"max_matches_per_row\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "3,Cara,") != NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"semi-join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    assert_push_cap_error(plan, "id,name\n2,Bob\n1,Alice\n", "left side is not sorted");

    const char *bad_lookup = "/tmp/tranfi_test_sorted_join_bad_lookup.csv";
    f = fopen(bad_lookup, "w");
    assert(f != NULL);
    fprintf(f, "id,city\n2,Paris\n1,London\n");
    fclose(f);
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"semi-join\",\"args\":{\"file\":\"%s\",\"on\":\"id\",\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", bad_lookup);
    assert_push_cap_error(plan, "id,name\n1,Alice\n2,Bob\n", "lookup side is not sorted");

    remove(lookup_path);
    remove(bad_lookup);
}

static void test_pipeline_set_ops(void) {
    const char *lookup_path = "/tmp/tranfi_test_set_lookup.csv";
    FILE *f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,name\n1,Alice\n3,Charlie\n");
    fclose(f);

    char plan[1024];
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"intersect\",\"args\":{\"file\":\"%s\",\"max_lookup_bytes\":1024,\"max_lookup_keys\":10,\"max_output_keys\":10}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "id,name\n1,Alice\n2,Bob\n1,Alice\n3,Charlie\n3,Other\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "id,name") != NULL);
    assert(strstr((char *)out, "1,Alice") != NULL);
    assert(strstr((char *)out, "3,Charlie") != NULL);
    assert(strstr((char *)out, "2,Bob") == NULL);
    assert(strstr((char *)out, "3,Other") == NULL);
    assert(strstr((char *)out, "1,Alice\n1,Alice") == NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"intersect\",\"args\":{\"file\":\"%s\",\"max_lookup_bytes\":1024,\"max_state_bytes\":4096}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "1,Alice") != NULL);
    uint8_t set_stats[4096];
    size_t set_stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, set_stats, sizeof(set_stats) - 1);
    assert(set_stats_n > 0);
    set_stats[set_stats_n] = '\0';
    assert(strstr((char *)set_stats, "\"max_state_bytes\":4096") != NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"intersect\",\"args\":{\"file\":\"%s\",\"max_lookup_bytes\":1024,\"max_state_bytes\":128}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    assert_push_cap_error(plan, csv, "max_state_bytes=128");

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"setdiff\",\"args\":{\"file\":\"%s\",\"max_lookup_bytes\":1024,\"max_lookup_keys\":10,\"max_output_keys\":10}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "2,Bob") != NULL);
    assert(strstr((char *)out, "3,Other") != NULL);
    assert(strstr((char *)out, "1,Alice") == NULL);
    assert(strstr((char *)out, "3,Charlie") == NULL);
    tf_pipeline_free(p);

    f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,label\n1,a\n1,a2\n3,c\n");
    fclose(f);
    const char *bag_csv = "id,name\n1,A1\n1,A2\n1,A3\n2,B\n3,C\n3,C2\n";

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"intersect-all\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"max_lookup_bytes\":1024,\"max_lookup_keys\":10}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)bag_csv, strlen(bag_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strcmp((char *)out, "id,name\n1,A1\n1,A2\n3,C\n") == 0);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"setdiff-all\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"max_lookup_bytes\":1024,\"max_lookup_keys\":10}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)bag_csv, strlen(bag_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strcmp((char *)out, "id,name\n1,A3\n2,B\n3,C2\n") == 0);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"intersect-all\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"max_lookup_bytes\":1024,\"max_lookup_keys\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    assert_push_cap_error(plan, bag_csv, "max_lookup_keys=1");

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"setdiff-all\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"max_lookup_bytes\":1024,\"max_output_keys\":10}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p == NULL);

    remove(lookup_path);
}

static void test_pipeline_set_ops_spill_preserves_left_order(void) {
    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_core_spill_set_%ld", (long)getpid());
    rmdir(spill_dir);
    assert(mkdir(spill_dir, 0700) == 0);

    const char *lookup_path = "/tmp/tranfi_test_spill_set_lookup.csv";
    FILE *f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,label\n3,c\n1,a\n1,a2\n9,z\n");
    fclose(f);

    char plan[1800];
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"intersect\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],"
        "\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path, spill_dir);

    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "id,name\n3,C\n4,D\n1,A\n2,B\n1,A2\n5,E\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_pull(p, TF_CHAN_MAIN, (uint8_t *)plan, 1) == 0);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    char *text = (char *)out;
    char *header = strstr(text, "id,name");
    char *c = strstr(text, "3,C");
    char *a = strstr(text, "1,A");
    assert(header && c && a);
    assert(header < c && c < a);
    assert(strstr(text, "1,A2") == NULL);
    assert(strstr(text, "4,D") == NULL);
    assert(strstr(text, "2,B") == NULL);
    assert(strstr(text, "5,E") == NULL);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"op\":\"intersect\"") != NULL);
    assert(strstr((char *)stats, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats, "\"emit_class\":\"on_flush\"") != NULL);
    assert(strstr((char *)stats, "\"lookup_keys\":3") != NULL);
    assert(strstr((char *)stats, "\"spill_output_rows\":2") != NULL);
    assert(strstr((char *)stats, "\"spill_distinct_rows\":2") != NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"setdiff\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],"
        "\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path, spill_dir);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    text = (char *)out;
    const char *setdiff_rows[] = {"4,D", "2,B", "5,E"};
    char *prev = strstr(text, "id,name");
    assert(prev != NULL);
    for (size_t i = 0; i < sizeof(setdiff_rows) / sizeof(setdiff_rows[0]); i++) {
        char *pos = strstr(text, setdiff_rows[i]);
        assert(pos != NULL);
        assert(prev < pos);
        prev = pos;
    }
    assert(strstr(text, "3,C") == NULL);
    assert(strstr(text, "1,A") == NULL);
    assert(strstr(text, "1,A2") == NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"intersect-all\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],"
        "\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":8}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path, spill_dir);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    text = (char *)out;
    const char *inter_all_rows[] = {"3,C", "1,A", "1,A2"};
    prev = strstr(text, "id,name");
    assert(prev != NULL);
    for (size_t i = 0; i < sizeof(inter_all_rows) / sizeof(inter_all_rows[0]); i++) {
        char *pos = strstr(text, inter_all_rows[i]);
        assert(pos != NULL);
        assert(prev < pos);
        prev = pos;
    }
    assert(strstr(text, "4,D") == NULL);
    assert(strstr(text, "2,B") == NULL);
    assert(strstr(text, "5,E") == NULL);
    stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"op\":\"intersect-all\"") != NULL);
    assert(strstr((char *)stats, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats, "\"spill_output_rows\":3") != NULL);
    assert(strstr((char *)stats, "\"spill_kept_rows\":3") != NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"setdiff-all\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],"
        "\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":8}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path, spill_dir);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    text = (char *)out;
    const char *diff_all_rows[] = {"4,D", "2,B", "5,E"};
    prev = strstr(text, "id,name");
    assert(prev != NULL);
    for (size_t i = 0; i < sizeof(diff_all_rows) / sizeof(diff_all_rows[0]); i++) {
        char *pos = strstr(text, diff_all_rows[i]);
        assert(pos != NULL);
        assert(prev < pos);
        prev = pos;
    }
    assert(strstr(text, "3,C") == NULL);
    assert(strstr(text, "1,A") == NULL);
    assert(strstr(text, "1,A2") == NULL);
    stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"op\":\"setdiff-all\"") != NULL);
    assert(strstr((char *)stats, "\"spill_kept_rows\":3") != NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"intersect\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],"
        "\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2,\"max_lookup_keys\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path, spill_dir);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) != TF_OK);
    assert(tf_pipeline_error(p) != NULL);
    assert(strstr(tf_pipeline_error(p), "max_lookup_keys=2") != NULL);
    tf_pipeline_free(p);

    f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,label\nx,bad\n");
    fclose(f);
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"intersect\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],"
        "\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path, spill_dir);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *mismatch_csv = "id,name\n,Empty\n1,A\n";
    assert(tf_pipeline_push(p, (const uint8_t *)mismatch_csv, strlen(mismatch_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n == 0);
    stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = 0;
    assert(strstr((char *)stats, "\"lookup_keys\":0") != NULL);
    tf_pipeline_free(p);

    assert(rmdir(spill_dir) == 0);
    remove(lookup_path);
}


static void test_pipeline_union_ops(void) {
    const char *lookup_path = "/tmp/tranfi_test_union_lookup.csv";
    FILE *f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,name\n2,Bob\n3,Charlie\n1,Alice\n");
    fclose(f);

    char plan[2048];
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"union-all\",\"args\":{\"file\":\"%s\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "id,name\n1,Alice\n2,Bob\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    uint8_t out[4096];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "1,Alice") != NULL);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "3,Charlie") != NULL);
    assert(strstr((char *)out, "1,Alice") != NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"union\",\"args\":{\"file\":\"%s\",\"max_output_keys\":10}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *dupes = "id,name\n1,Alice\n2,Bob\n1,Alice\n";
    assert(tf_pipeline_push(p, (const uint8_t *)dupes, strlen(dupes)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "id,name") != NULL);
    assert(strstr((char *)out, "1,Alice") != NULL);
    assert(strstr((char *)out, "2,Bob") != NULL);
    assert(strstr((char *)out, "3,Charlie") != NULL);
    assert(strstr((char *)out, "1,Alice\n1,Alice") == NULL);
    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"emitted_keys\":3") != NULL);
    assert(strstr((char *)stats, "\"emitted_key_bytes\":") != NULL);
    assert(strstr((char *)stats, "\"retained_state_bytes\":") != NULL);
    assert(strstr((char *)stats, "\"max_state_bytes\":0") != NULL);
    tf_pipeline_free(p);

    f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,name\n1,A_file\n1,A_file_dup\n3,C_file\n5,E_file\n5,E_file_dup\n");
    fclose(f);
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"union\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *sorted_union_csv = "id,name\n1,A_left\n1,A_left_dup\n2,B_left\n4,D_left\n5,E_left\n";
    assert(tf_pipeline_push(p, (const uint8_t *)sorted_union_csv, strlen(sorted_union_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strcmp((char *)out, "id,name\n"
                                "1,A_left\n"
                                "2,B_left\n"
                                "3,C_file\n"
                                "4,D_left\n"
                                "5,E_left\n") == 0);
    stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"emitted_keys\":5") != NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"union\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    assert_push_cap_error(plan, "id,name\n2,B\n1,A\n", "union: left side is not sorted");

    f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,name\n3,C\n1,A\n");
    fclose(f);
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"union\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    assert_push_cap_error(plan, "id,name\n2,B\n4,D\n", "union: file side is not sorted");

    f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,name\n2,Bob\n3,Charlie\n1,Alice\n");
    fclose(f);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"union\",\"args\":{\"file\":\"%s\",\"max_state_bytes\":4096}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)dupes, strlen(dupes)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "3,Charlie") != NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"union\",\"args\":{\"file\":\"%s\",\"max_state_bytes\":128}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    assert_push_cap_error(plan, "id,name\n1,Alice\n", "max_state_bytes=128");

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"union\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"max_output_keys\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) != TF_OK);
    assert(tf_last_error() && strstr(tf_last_error(), "max_output_keys=2") != NULL);
    tf_pipeline_free(p);

    char spill_dir[256];
    snprintf(spill_dir, sizeof(spill_dir), "/tmp/tranfi_core_spill_union_%ld", (long)getpid());
    rmdir(spill_dir);
    assert(mkdir(spill_dir, 0700) == 0);

    const char *spill_lookup_path = "/tmp/tranfi_test_spill_union_lookup.csv";
    f = fopen(spill_lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,name,score\n2,B_file,200\n5,E,50\n3,C_file,300\n5,E2,51\n6,F,60\n");
    fclose(f);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"union\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],"
        "\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", spill_lookup_path, spill_dir);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *spill_csv = "id,name,score\n3,C,30\n1,A,10\n2,B,20\n1,A2,11\n4,D,40\n";
    assert(tf_pipeline_push(p, (const uint8_t *)spill_csv, strlen(spill_csv)) == TF_OK);
    assert(tf_pipeline_pull(p, TF_CHAN_MAIN, (uint8_t *)plan, 1) == 0);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    char *text = (char *)out;
    const char *union_rows[] = {"3,C,30", "1,A,10", "2,B,20", "4,D,40", "5,E,50", "6,F,60"};
    char *prev = strstr(text, "id,name,score");
    assert(prev != NULL);
    for (size_t i = 0; i < sizeof(union_rows) / sizeof(union_rows[0]); i++) {
        char *pos = strstr(text, union_rows[i]);
        assert(pos != NULL);
        assert(prev < pos);
        prev = pos;
    }
    assert(strstr(text, "1,A2,11") == NULL);
    assert(strstr(text, "2,B_file,200") == NULL);
    assert(strstr(text, "3,C_file,300") == NULL);
    assert(strstr(text, "5,E2,51") == NULL);
    stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    assert(strstr((char *)stats, "\"op\":\"union\"") != NULL);
    assert(strstr((char *)stats, "\"execution_target\":\"native_spill\"") != NULL);
    assert(strstr((char *)stats, "\"memory_class\":\"external\"") != NULL);
    assert(strstr((char *)stats, "\"emit_class\":\"on_flush\"") != NULL);
    assert(strstr((char *)stats, "\"spill_output_rows\":6") != NULL);
    assert(strstr((char *)stats, "\"spill_distinct_rows\":6") != NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"union\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],"
        "\"spill_dir\":\"%s\",\"spill_run_rows\":2,\"spill_output_rows\":2,\"max_output_keys\":2}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", spill_lookup_path, spill_dir);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)spill_csv, strlen(spill_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) != TF_OK);
    assert(tf_pipeline_error(p) != NULL);
    assert(strstr(tf_pipeline_error(p), "max_output_keys=2") != NULL);
    tf_pipeline_free(p);
    assert(rmdir(spill_dir) == 0);
    remove(spill_lookup_path);
    remove(lookup_path);
}

static void test_pipeline_set_ops_columns_and_caps(void) {
    const char *lookup_path = "/tmp/tranfi_test_set_lookup_cols.csv";
    FILE *f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,label\n1,x\n3,y\n");
    fclose(f);

    char plan[1024];
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"intersect\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"max_lookup_bytes\":1024,\"max_lookup_keys\":10,\"max_output_keys\":10}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "id,name\n1,Alice\n2,Bob\n3,Charlie\n3,Other\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "1,Alice") != NULL);
    assert(strstr((char *)out, "3,Charlie") != NULL);
    assert(strstr((char *)out, "3,Other") == NULL);
    assert(strstr((char *)out, "2,Bob") == NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"intersect\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"max_lookup_keys\":1}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    assert_push_cap_error(plan, "id,name\n1,Alice\n", "max_lookup_keys=1");
    remove(lookup_path);
}

static void test_pipeline_set_ops_sorted_mode(void) {
    const char *lookup_path = "/tmp/tranfi_test_set_sorted_lookup.csv";
    FILE *f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,label\n1,x\n3,y\n5,z\n");
    fclose(f);

    char plan[1024];
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"intersect\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "id,name\n1,Alice\n1,Alicia\n2,Bob\n3,Charlie\n3,Other\n4,Dana\n5,Eve\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "1,Alice") != NULL);
    assert(strstr((char *)out, "1,Alicia") == NULL);
    assert(strstr((char *)out, "3,Charlie") != NULL);
    assert(strstr((char *)out, "3,Other") == NULL);
    assert(strstr((char *)out, "5,Eve") != NULL);
    assert(strstr((char *)out, "2,Bob") == NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"setdiff\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "2,Bob") != NULL);
    assert(strstr((char *)out, "4,Dana") != NULL);
    assert(strstr((char *)out, "1,Alice") == NULL);
    assert(strstr((char *)out, "3,Charlie") == NULL);
    assert(strstr((char *)out, "5,Eve") == NULL);
    tf_pipeline_free(p);

    f = fopen(lookup_path, "w");
    assert(f != NULL);
    fprintf(f, "id,label\n1,a\n1,a2\n3,c\n5,e\n5,e2\n");
    fclose(f);
    const char *bag_csv = "id,name\n"
                          "1,Alice\n"
                          "1,Alicia\n"
                          "1,Alina\n"
                          "2,Bob\n"
                          "3,Charlie\n"
                          "3,Other\n"
                          "4,Dana\n"
                          "5,Eve\n";

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"intersect-all\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)bag_csv, strlen(bag_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strcmp((char *)out, "id,name\n"
                                "1,Alice\n"
                                "1,Alicia\n"
                                "3,Charlie\n"
                                "5,Eve\n") == 0);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":2}},"
        "{\"op\":\"setdiff-all\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)bag_csv, strlen(bag_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strcmp((char *)out, "id,name\n"
                                "1,Alina\n"
                                "2,Bob\n"
                                "3,Other\n"
                                "4,Dana\n") == 0);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"intersect-all\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    assert_push_cap_error(plan, "id,name\n2,Bob\n1,Alice\n",
                          "intersect-all: left side is not sorted");

    const char *typed_lookup = "/tmp/tranfi_test_set_sorted_typed_lookup.csv";
    f = fopen(typed_lookup, "w");
    assert(f != NULL);
    fprintf(f, "id,name\n2,Bob\n10,Jane\n");
    fclose(f);
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"intersect\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", typed_lookup);
    p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *typed_csv = "id,name\n2,Bob\n10,Jane\n";
    assert(tf_pipeline_push(p, (const uint8_t *)typed_csv, strlen(typed_csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "2,Bob") != NULL);
    assert(strstr((char *)out, "10,Jane") != NULL);
    tf_pipeline_free(p);

    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"intersect\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", lookup_path);
    assert_push_cap_error(plan, "id,name\n2,Bob\n1,Alice\n", "left side is not sorted");

    const char *bad_lookup = "/tmp/tranfi_test_set_sorted_bad_lookup.csv";
    f = fopen(bad_lookup, "w");
    assert(f != NULL);
    fprintf(f, "id,label\n3,y\n1,x\n");
    fclose(f);
    snprintf(plan, sizeof(plan),
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{\"batch_size\":1}},"
        "{\"op\":\"intersect\",\"args\":{\"file\":\"%s\",\"columns\":[\"id\"],\"sorted\":true}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}", bad_lookup);
    assert_push_cap_error(plan, "id,name\n1,Alice\n3,Charlie\n", "lookup side is not sorted");

    remove(lookup_path);
    remove(typed_lookup);
    remove(bad_lookup);
}

static void test_dsl_join(void) {
    char *error = NULL;
    const char *join_dsl = "csv | join lookup.csv on id --left max_state_bytes=8192 | csv";
    tf_ir_plan *plan = tf_dsl_parse(join_dsl, strlen(join_dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "join") == 0);
    cJSON *file_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "file");
    assert(file_j != NULL && strcmp(file_j->valuestring, "lookup.csv") == 0);
    cJSON *on_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "on");
    assert(on_j != NULL && strcmp(on_j->valuestring, "id") == 0);
    cJSON *how_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "how");
    assert(how_j != NULL && strcmp(how_j->valuestring, "left") == 0);
    cJSON *max_state_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_state_bytes");
    assert(max_state_j != NULL && (size_t)max_state_j->valuedouble == 8192);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | semi-join lookup.csv on id | csv", strlen("csv | semi-join lookup.csv on id | csv"), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "semi-join") == 0);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | join lookup.csv on id --anti | csv", strlen("csv | join lookup.csv on id --anti | csv"), &error);
    assert(plan != NULL);
    how_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "how");
    assert(how_j != NULL && strcmp(how_j->valuestring, "anti") == 0);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | semi-join lookup.csv on id sorted=true | csv", strlen("csv | semi-join lookup.csv on id sorted=true | csv"), &error);
    assert(plan != NULL);
    cJSON *sorted_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "sorted");
    assert(sorted_j != NULL && cJSON_IsTrue(sorted_j));
    tf_ir_plan_free(plan);

    const char *spill_join_dsl = "csv | join lookup.csv on id max_matches_per_row=2 spill_dir=/tmp/tranfi-dsl spill_run_rows=2 spill_output_rows=3 spill_memory_bytes=4096 | csv";
    plan = tf_dsl_parse(spill_join_dsl, strlen(spill_join_dsl), &error);
    assert(plan != NULL);
    cJSON *spill_dir_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "spill_dir");
    cJSON *spill_rows_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "spill_run_rows");
    cJSON *spill_output_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "spill_output_rows");
    cJSON *spill_memory_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "spill_memory_bytes");
    cJSON *max_matches_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_matches_per_row");
    assert(spill_dir_j != NULL && strcmp(spill_dir_j->valuestring, "/tmp/tranfi-dsl") == 0);
    assert(spill_rows_j != NULL && (int)spill_rows_j->valuedouble == 2);
    assert(spill_output_j != NULL && (int)spill_output_j->valuedouble == 3);
    assert(spill_memory_j != NULL && (int)spill_memory_j->valuedouble == 4096);
    assert(max_matches_j != NULL && (int)max_matches_j->valuedouble == 2);
    tf_ir_plan_free(plan);
}


static void test_dsl_set_ops(void) {
    char *error = NULL;
    const char *dsl = "csv | intersect lookup.csv id max_lookup_rows=5 max_lookup_keys=4 max_lookup_bytes=99 max_output_keys=7 max_state_bytes=4096 | csv";
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "intersect") == 0);
    cJSON *file_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "file");
    assert(file_j && strcmp(file_j->valuestring, "lookup.csv") == 0);
    cJSON *cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    assert(cols && cJSON_IsArray(cols) && cJSON_GetArraySize(cols) == 1);
    cJSON *max_output = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_output_keys");
    assert(max_output && max_output->valueint == 7);
    cJSON *max_state = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_state_bytes");
    assert(max_state && max_state->valueint == 4096);
    tf_ir_plan_free(plan);

    dsl = "csv | setdiff lookup.csv columns=id,city max_lookup_bytes=99 max_output_keys=7 | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "setdiff") == 0);
    cols = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "columns");
    assert(cols && cJSON_IsArray(cols) && cJSON_GetArraySize(cols) == 2);
    tf_ir_plan_free(plan);

    dsl = "csv | intersect lookup.csv columns=id sorted=true | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    cJSON *sorted_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "sorted");
    assert(sorted_j && cJSON_IsTrue(sorted_j));
    tf_ir_plan_free(plan);

    dsl = "csv | union lookup.csv columns=id max_output_keys=7 max_state_bytes=4096 | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "union") == 0);
    max_output = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_output_keys");
    assert(max_output && max_output->valueint == 7);
    max_state = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_state_bytes");
    assert(max_state && max_state->valueint == 4096);
    tf_ir_plan_free(plan);

    dsl = "csv | union_all lookup.csv max_lookup_rows=5 | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "union-all") == 0);
    cJSON *max_rows = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_lookup_rows");
    assert(max_rows && max_rows->valueint == 5);
    tf_ir_plan_free(plan);

    dsl = "csv | union lookup.csv columns=id sorted=true | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "union") == 0);
    sorted_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "sorted");
    assert(sorted_j && cJSON_IsTrue(sorted_j));
    tf_ir_plan_free(plan);

    dsl = "csv | intersect_all lookup.csv id max_lookup_bytes=1024 max_lookup_keys=10 max_state_bytes=4096 | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "intersect-all") == 0);
    max_state = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_state_bytes");
    assert(max_state && max_state->valueint == 4096);
    tf_ir_plan_free(plan);

    dsl = "csv | except-all lookup.csv columns=id max_lookup_bytes=1024 max_lookup_keys=10 | csv";
    plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "setdiff-all") == 0);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | setdiff-all lookup.csv max_output_keys=3 | csv",
                        strlen("csv | setdiff-all lookup.csv max_output_keys=3 | csv"), &error);
    assert(plan == NULL);
    free(error);
    error = NULL;

    plan = tf_dsl_parse("csv | intersect-all lookup.csv sorted=true | csv",
                        strlen("csv | intersect-all lookup.csv sorted=true | csv"), &error);
    assert(plan != NULL);
    sorted_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "sorted");
    assert(sorted_j && cJSON_IsTrue(sorted_j));
    assert(strcmp(plan->nodes[1].op, "intersect-all") == 0);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | intersect lookup.csv max_output_keys=0 | csv",
                        strlen("csv | intersect lookup.csv max_output_keys=0 | csv"), &error);
    assert(plan == NULL);
    free(error);
}

static void test_dsl_join_caps(void) {
    char *error = NULL;
    const char *dsl = "csv | join lookup.csv on id max_lookup_rows=5 max_lookup_keys=4 max_lookup_bytes=99 max_matches_per_row=2 max_output_rows=7 --left | csv";
    tf_ir_plan *plan = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(plan != NULL);
    cJSON *max_rows = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_lookup_rows");
    cJSON *max_keys = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_lookup_keys");
    cJSON *max_bytes = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_lookup_bytes");
    cJSON *max_matches = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_matches_per_row");
    cJSON *max_output = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "max_output_rows");
    cJSON *how = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "how");
    assert(cJSON_IsNumber(max_rows) && (int)max_rows->valuedouble == 5);
    assert(cJSON_IsNumber(max_keys) && (int)max_keys->valuedouble == 4);
    assert(cJSON_IsNumber(max_bytes) && (int)max_bytes->valuedouble == 99);
    assert(cJSON_IsNumber(max_matches) && (int)max_matches->valuedouble == 2);
    assert(cJSON_IsNumber(max_output) && (int)max_output->valuedouble == 7);
    assert(cJSON_IsString(how) && strcmp(how->valuestring, "left") == 0);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | join lookup.csv on id max_lookup_rows=0 | csv", strlen("csv | join lookup.csv on id max_lookup_rows=0 | csv"), &error);
    assert(plan == NULL);
    assert(error != NULL && strstr(error, "max_lookup_rows") != NULL);
    free(error);
    error = NULL;

    plan = tf_dsl_parse("csv | join lookup.csv on id max_output_rows=0 | csv", strlen("csv | join lookup.csv on id max_output_rows=0 | csv"), &error);
    assert(plan == NULL);
    assert(error != NULL && strstr(error, "max_output_rows") != NULL);
    free(error);
    error = NULL;

    plan = tf_dsl_parse("csv | join lookup.csv on id typo=1 | csv", strlen("csv | join lookup.csv on id typo=1 | csv"), &error);
    assert(plan == NULL);
    assert(error != NULL && strstr(error, "unexpected") != NULL);
    free(error);
}

static void test_dsl_join_eq(void) {
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse("csv | join data.csv on id=lookup_id | csv", 42, &error);
    assert(plan != NULL);
    cJSON *on_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "on");
    assert(on_j != NULL && strcmp(on_j->valuestring, "id=lookup_id") == 0);
    tf_ir_plan_free(plan);
}

/* ================================================================
 * Recipe tests
 * ================================================================ */

static void test_compile_dsl(void) {
    char *error = NULL;
    char *json = tf_compile_dsl("csv | head 3 | csv", 18, &error);
    assert(json != NULL);
    assert(strstr(json, "codec.csv.decode") != NULL);
    assert(strstr(json, "head") != NULL);
    assert(strstr(json, "codec.csv.encode") != NULL);
    assert(strstr(json, "\"memory_class\":\"bounded_state\"") != NULL);
    assert(strstr(json, "\"emit_class\":\"per_batch\"") != NULL);
    assert(strstr(json, "\"schema_class\":\"stable\"") != NULL);
    assert(strstr(json, "\"state_bytes_estimate\":null") != NULL);
    tf_string_free(json);

    json = tf_compile_dsl("csv | unique name max_keys=3 | csv",
                          strlen("csv | unique name max_keys=3 | csv"), &error);
    assert(json != NULL);
    assert(strstr(json, "\"state_bytes_estimate\":1888") != NULL);
    assert(strstr(json, "state_bytes_reason") == NULL);
    tf_string_free(json);

    json = tf_compile_dsl("csv | unique name | csv", strlen("csv | unique name | csv"), &error);
    assert(json != NULL);
    assert(strstr(json, "\"state_bytes_estimate\":null") != NULL);
    assert(strstr(json, "\"state_bytes_reason\":\"step 'unique' needs max_keys") != NULL);
    tf_string_free(json);

    json = tf_compile_dsl("csv | summarise city count:*:rows | select starts_with(score) | csv",
                          strlen("csv | summarise city count:*:rows | select starts_with(score) | csv"),
                          &error);
    assert(json == NULL);
    assert(error != NULL);
    assert(strstr(error, "schema inference failed") != NULL);
    assert(strstr(error, "select schema") != NULL);
    assert(strstr(error, "selectors resolved no columns") != NULL);
    free(error);
    error = NULL;
}

static void test_recipe_roundtrip(void) {
    /* Compile DSL to recipe JSON */
    char *error = NULL;
    char *recipe = tf_compile_dsl("csv | head 1 | csv", 18, &error);
    assert(recipe != NULL);

    /* Create pipeline from recipe JSON */
    tf_pipeline *p = tf_pipeline_create(recipe, strlen(recipe));
    tf_string_free(recipe);
    assert(p != NULL);

    const char *csv = "x\n1\n2\n3\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[256];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "1") != NULL);
    assert(strstr((char *)out, "3") == NULL);  /* head 1 should only keep first row */
    tf_pipeline_free(p);
}

static void test_recipe_count(void) {
    assert(tf_recipe_count() == 22);
}

static void test_recipe_find(void) {
    const char *dsl = tf_recipe_find_dsl("profile");
    assert(dsl != NULL);
    assert(strstr(dsl, "stats") != NULL);

    /* Case-insensitive */
    const char *dsl2 = tf_recipe_find_dsl("PREVIEW");
    assert(dsl2 != NULL);
    assert(strstr(dsl2, "head 10") != NULL);

    const char *dsl3 = tf_recipe_find_dsl("sniff");
    assert(dsl3 != NULL);
    assert(strstr(dsl3, "schema infer rows=1000") != NULL);

    /* Unknown recipe */
    assert(tf_recipe_find_dsl("nonexistent") == NULL);
}

static void test_recipe_accessors(void) {
    assert(tf_recipe_name(0) != NULL);
    assert(strcmp(tf_recipe_name(0), "profile") == 0);
    assert(tf_recipe_dsl(0) != NULL);
    assert(tf_recipe_description(0) != NULL);
    assert(tf_recipe_name(99) == NULL);
}

static void test_recipe_run_preview(void) {
    const char *dsl = tf_recipe_find_dsl("preview");
    assert(dsl != NULL);

    char *error = NULL;
    tf_ir_plan *ir = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(ir != NULL);
    tf_pipeline *p = tf_pipeline_create_from_ir(ir);
    tf_ir_plan_destroy(ir);
    assert(p != NULL);

    const char *csv = "name,age\nAlice,30\nBob,25\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);
    tf_pipeline_free(p);
}

static void test_recipe_run_sniff(void) {
    const char *dsl = tf_recipe_find_dsl("sniff");
    assert(dsl != NULL);

    char *error = NULL;
    tf_ir_plan *ir = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(ir != NULL);
    tf_pipeline *p = tf_pipeline_create_from_ir(ir);
    tf_ir_plan_destroy(ir);
    assert(p != NULL);

    const char *csv = "name,age,score\nAlice,30,91.5\nBob,,88\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out) - 1);
    assert(n > 0);
    out[n] = '\0';
    assert(strstr((char *)out, "column,type,nullable") != NULL);
    assert(strstr((char *)out, "name,string,false,true") != NULL);
    assert(strstr((char *)out, "age,int,true,false") != NULL);
    assert(strstr((char *)out, "score,float,false,true") != NULL);
    tf_pipeline_free(p);
}

static void test_recipe_run_dedup(void) {
    const char *dsl = tf_recipe_find_dsl("dedup");
    assert(dsl != NULL);

    char *error = NULL;
    tf_ir_plan *ir = tf_dsl_parse(dsl, strlen(dsl), &error);
    assert(ir != NULL);
    tf_pipeline *p = tf_pipeline_create_from_ir(ir);
    tf_ir_plan_destroy(ir);
    assert(p != NULL);

    const char *csv = "x\n1\n2\n1\n3\n2\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* Should have 3 unique values: 1, 2, 3 */
    char *line = (char *)out;
    int count = 0;
    while (*line) { if (*line == '\n') count++; line++; }
    /* header + 3 data rows = 4 lines (last may or may not have trailing \n) */
    assert(count >= 3 && count <= 4);
    tf_pipeline_free(p);
}

/* ================================================================
 * Data Prep & Time Series Ops
 * ================================================================ */

static tf_pipeline *pipeline_from_dsl(const char *dsl) {
    char *json = tf_compile_dsl(dsl, strlen(dsl), NULL);
    if (!json) return NULL;
    tf_pipeline *p = tf_pipeline_create(json, strlen(json));
    tf_string_free(json);
    return p;
}

/* Helper: run DSL pipeline and return output as string. Caller uses stack buf. */
static size_t run_dsl(const char *dsl, const char *input,
                      char *out, size_t outsz) {
    tf_pipeline *p = pipeline_from_dsl(dsl);
    assert(p != NULL);
    assert(tf_pipeline_push(p, (const uint8_t *)input, strlen(input)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, (uint8_t *)out, outsz - 1);
    out[n] = '\0';
    tf_pipeline_free(p);
    return n;
}

static void expect_dsl_runtime_error(const char *dsl, const char *input, const char *needle) {
    tf_pipeline *p = pipeline_from_dsl(dsl);
    assert(p != NULL);
    int rc = tf_pipeline_push(p, (const uint8_t *)input, strlen(input));
    if (rc == TF_OK) rc = tf_pipeline_finish(p);
    assert(rc == TF_ERROR);
    const char *err = tf_pipeline_error(p);
    assert(err != NULL);
    assert(strstr(err, needle) != NULL);
    tf_pipeline_free(p);
}

static void expect_dsl_compile_error(const char *dsl, const char *needle) {
    char *error = NULL;
    char *json = tf_compile_dsl(dsl, strlen(dsl), &error);
    assert(json == NULL);
    assert(error != NULL);
    assert(strstr(error, needle) != NULL);
    free(error);
}

/* Helper: get Nth line (0-based) from output string */
static const char *get_line(const char *s, int n) {
    for (int i = 0; i < n; i++) {
        s = strchr(s, '\n');
        if (!s) return NULL;
        s++;
    }
    return s;
}

/* Helper: check that line N starts with prefix */
static int line_starts_with(const char *output, int lineno, const char *prefix) {
    const char *line = get_line(output, lineno);
    if (!line) return 0;
    return strncmp(line, prefix, strlen(prefix)) == 0;
}


static void test_pipeline_unique_sorted(void) {
    char out[2048];
    run_dsl("csv batch_size=2 | unique city sorted=true | csv",
            "city,val\nA,1\nA,2\nA,3\nB,4\nB,5\nA,6\nA,7\n",
            out, sizeof(out));
    assert(line_starts_with(out, 0, "city,val"));
    assert(line_starts_with(out, 1, "A,1"));
    assert(line_starts_with(out, 2, "B,4"));
    assert(line_starts_with(out, 3, "A,6"));
    assert(strstr(out, "A,2") == NULL);
    assert(strstr(out, "A,3") == NULL);
    assert(strstr(out, "B,5") == NULL);
    assert(strstr(out, "A,7") == NULL);
}



static size_t run_dsl_chunked(const char *dsl, const char *input,
                              size_t chunk_size, char *out, size_t outsz) {
    tf_pipeline *p = pipeline_from_dsl(dsl);
    assert(p != NULL);
    size_t len = strlen(input);
    for (size_t off = 0; off < len;) {
        size_t n = len - off;
        if (n > chunk_size) n = chunk_size;
        assert(tf_pipeline_push(p, (const uint8_t *)input + off, n) == TF_OK);
        off += n;
    }
    assert(tf_pipeline_finish(p) == TF_OK);
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, (uint8_t *)out, outsz - 1);
    out[n] = '\0';
    tf_pipeline_free(p);
    return n;
}

static void test_pipeline_sample_deterministic_seed(void) {
    const char *input = "name\nAlice\nBob\nCharlie\nDiana\nEve\nFrank\nGrace\n";
    char out_default_a[512], out_default_b[512], out_seed_zero[512];
    char out_seed_a[512], out_seed_b[512], out_seed_other[512];

    run_dsl_chunked("csv batch_size=2 | sample 3 | csv", input, 7, out_default_a, sizeof(out_default_a));
    run_dsl_chunked("csv batch_size=2 | sample 3 | csv", input, 3, out_default_b, sizeof(out_default_b));
    run_dsl_chunked("csv batch_size=2 | sample 3 seed=0 | csv", input, 7, out_seed_zero, sizeof(out_seed_zero));
    assert(strcmp(out_default_a, out_default_b) == 0);
    assert(strcmp(out_default_a, out_seed_zero) == 0);
    assert(line_starts_with(out_default_a, 1, "Eve"));
    assert(line_starts_with(out_default_a, 2, "Frank"));
    assert(line_starts_with(out_default_a, 3, "Charlie"));

    run_dsl_chunked("csv batch_size=2 | sample 3 seed=123 | csv", input, 7, out_seed_a, sizeof(out_seed_a));
    run_dsl_chunked("csv batch_size=2 | sample 3 seed=123 | csv", input, 3, out_seed_b, sizeof(out_seed_b));
    run_dsl_chunked("csv batch_size=2 | sample 3 seed=124 | csv", input, 7, out_seed_other, sizeof(out_seed_other));
    assert(strcmp(out_seed_a, out_seed_b) == 0);
    assert(line_starts_with(out_seed_a, 1, "Alice"));
    assert(line_starts_with(out_seed_a, 2, "Grace"));
    assert(line_starts_with(out_seed_a, 3, "Charlie"));
    assert(line_starts_with(out_seed_other, 1, "Alice"));
    assert(line_starts_with(out_seed_other, 2, "Grace"));
    assert(line_starts_with(out_seed_other, 3, "Eve"));

    char *err = NULL;
    tf_ir_plan *plan = tf_dsl_parse("csv | sample 3 seed=random | csv", strlen("csv | sample 3 seed=random | csv"), &err);
    assert(plan != NULL && err == NULL);
    cJSON *seed = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "seed");
    assert(cJSON_IsString(seed) && strcmp(seed->valuestring, "random") == 0);
    tf_ir_plan_free(plan);

    plan = tf_dsl_parse("csv | sample 3 seed=-1 | csv", strlen("csv | sample 3 seed=-1 | csv"), &err);
    assert(plan == NULL && err != NULL && strstr(err, "seed") != NULL);
    free(err);
}

static void test_pipeline_rowid_global(void) {
    char out[2048];
    run_dsl("csv batch_size=2 | rowid result=row_n | csv",
            "id,val\n1,10\n2,20\n3,30\n4,40\n",
            out, sizeof(out));
    assert(line_starts_with(out, 0, "id,val,row_n"));
    assert(line_starts_with(out, 1, "1,10,1"));
    assert(line_starts_with(out, 2, "2,20,2"));
    assert(line_starts_with(out, 3, "3,30,3"));
    assert(line_starts_with(out, 4, "4,40,4"));

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"rowid\",\"args\":{\"columns\":{\"x\":true}}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "rowid: columns must be an array");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"rowid\",\"args\":{\"columns\":[123]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "rowid: column names must be non-empty strings");
}

static void test_pipeline_rowid_unsorted_grouped(void) {
    char out[2048];
    run_dsl("csv batch_size=2 | rowid x result=x_row max_keys=3 | csv",
            "x,y\n20,a\n10,a\n10,a\n30,b\n30,b\n20,b\n",
            out, sizeof(out));
    assert(line_starts_with(out, 0, "x,y,x_row"));
    assert(line_starts_with(out, 1, "20,a,1"));
    assert(line_starts_with(out, 2, "10,a,1"));
    assert(line_starts_with(out, 3, "10,a,2"));
    assert(line_starts_with(out, 4, "30,b,1"));
    assert(line_starts_with(out, 5, "30,b,2"));
    assert(line_starts_with(out, 6, "20,b,2"));
}

static void test_pipeline_rowid_sorted_grouped(void) {
    char out[2048];
    run_dsl("csv batch_size=2 | rowid x result=x_run sorted=true | csv",
            "x\nA\nA\nB\nB\nA\n",
            out, sizeof(out));
    assert(line_starts_with(out, 0, "x,x_run"));
    assert(line_starts_with(out, 1, "A,1"));
    assert(line_starts_with(out, 2, "A,2"));
    assert(line_starts_with(out, 3, "B,1"));
    assert(line_starts_with(out, 4, "B,2"));
    assert(line_starts_with(out, 5, "A,1"));
}

static void test_pipeline_rolling_ops_chunk_boundary(void) {
    char out[4096];
    run_dsl("csv batch_size=2 | rolling-sum val 3 sum3 | rolling-mean val 3 mean3 | rolling-min val 3 min3 | rolling-max val 3 max3 | csv",
            "val\n10\n20\n30\n5\n",
            out, sizeof(out));
    assert(line_starts_with(out, 0, "val,sum3,mean3,min3,max3"));
    assert(line_starts_with(out, 1, "10,10,10,10,10"));
    assert(line_starts_with(out, 2, "20,30,15,10,20"));
    assert(line_starts_with(out, 3, "30,60,20,10,30"));
    assert(line_starts_with(out, 4, "5,55,"));
    assert(strstr(out, "5,55,18.333") != NULL);
}

static void test_pipeline_rolling_bool_chunk_boundary(void) {
    char out[4096];
    run_dsl("csv batch_size=2 | rolling-any flag 3 any3 | rolling-all flag 3 all3 | csv",
            "flag\ntrue\nfalse\ntrue\ntrue\n",
            out, sizeof(out));
    assert(line_starts_with(out, 0, "flag,any3,all3"));
    assert(line_starts_with(out, 1, "true,true,true"));
    assert(line_starts_with(out, 2, "false,true,false"));
    assert(line_starts_with(out, 3, "true,true,false"));
    assert(line_starts_with(out, 4, "true,true,false"));
}

static void test_pipeline_rolling_bool_null_policies(void) {
    char out[4096];
    run_dsl("csv batch_size=2 | rolling-any flag 3 any_ignore | rolling-all flag 3 all_ignore | rolling-any flag 3 any_false nulls=false | rolling-all flag 3 all_false nulls=false | rolling-any flag 3 any_true nulls=true | rolling-all flag 3 all_true nulls=true | rolling-any flag 3 any_prop nulls=propagate | rolling-all flag 3 all_prop nulls=propagate | csv",
            "id,flag\n1,true\n2,\n3,false\n4,\n",
            out, sizeof(out));
    assert(line_starts_with(out, 0, "id,flag,any_ignore,all_ignore,any_false,all_false,any_true,all_true,any_prop,all_prop"));
    assert(line_starts_with(out, 1, "1,true,true,true,true,true,true,true,true,true"));
    assert(line_starts_with(out, 2, "2,,true,true,true,false,true,true,,"));
    assert(line_starts_with(out, 3, "3,false,true,false,true,false,true,false,,"));
    assert(line_starts_with(out, 4, "4,,false,false,false,false,true,false,,"));
}

static void test_pipeline_lag_chunk_boundary(void) {
    char out[2048];
    run_dsl("csv batch_size=2 | lag val 2 prev_val | csv",
            "id,val\n1,10\n2,20\n3,30\n4,40\n",
            out, sizeof(out));
    assert(line_starts_with(out, 0, "id,val,prev_val"));
    assert(line_starts_with(out, 1, "1,10,"));
    assert(line_starts_with(out, 2, "2,20,"));
    assert(line_starts_with(out, 3, "3,30,10"));
    assert(line_starts_with(out, 4, "4,40,20"));
}

static void test_pipeline_lag_preserves_string_type(void) {
    char out[2048];
    run_dsl("csv batch_size=2 | lag name 1 prev_name | csv",
            "id,name\n1,Alice\n2,Bob\n3,Cara\n",
            out, sizeof(out));
    assert(line_starts_with(out, 0, "id,name,prev_name"));
    assert(line_starts_with(out, 1, "1,Alice,"));
    assert(line_starts_with(out, 2, "2,Bob,Alice"));
    assert(line_starts_with(out, 3, "3,Cara,Bob"));
}

static void test_pipeline_shift_lead_large_offset_chunks(void) {
    char out[2048];
    run_dsl("csv batch_size=2 | shift val 3 next_val type=lead | csv",
            "id,val\n1,10\n2,20\n3,30\n4,40\n5,50\n",
            out, sizeof(out));
    assert(line_starts_with(out, 0, "id,val,next_val"));
    assert(line_starts_with(out, 1, "1,10,40"));
    assert(line_starts_with(out, 2, "2,20,50"));
    assert(line_starts_with(out, 3, "3,30,"));
    assert(line_starts_with(out, 4, "4,40,"));
    assert(line_starts_with(out, 5, "5,50,"));
}

static void test_pipeline_rleid(void) {
    char out[2048];
    run_dsl("csv batch_size=2 | rleid city,status result=run_id | csv",
            "city,status\nA,on\nA,on\nA,off\nB,off\nA,off\nA,off\n",
            out, sizeof(out));
    assert(line_starts_with(out, 0, "city,status,run_id"));
    assert(line_starts_with(out, 1, "A,on,1"));
    assert(line_starts_with(out, 2, "A,on,1"));
    assert(line_starts_with(out, 3, "A,off,2"));
    assert(line_starts_with(out, 4, "B,off,3"));
    assert(line_starts_with(out, 5, "A,off,4"));
    assert(line_starts_with(out, 6, "A,off,4"));

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"rleid\",\"args\":{\"columns\":[]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "rleid: columns must be a non-empty array");

    assert_pipeline_create_fails_with(
        "{\"steps\":[{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"rleid\",\"args\":{\"columns\":[123]}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}]}",
        "rleid: column names must be non-empty strings");
}

static void test_pipeline_ewma(void) {
    char out[2048];
    run_dsl("csv | ewma x 0.5 | csv", "x\n10\n20\n30\n", out, sizeof(out));
    /* header: x,x_ewma */
    assert(line_starts_with(out, 0, "x,x_ewma"));
    /* ewma(0.5): row1=10, row2=0.5*20+0.5*10=15, row3=0.5*30+0.5*15=22.5 */
    assert(line_starts_with(out, 1, "10,10"));
    assert(line_starts_with(out, 2, "20,15"));
    assert(line_starts_with(out, 3, "30,22.5"));
}

static void test_pipeline_h14_numeric_missing_type_policies(void) {
    char out[2048];

    expect_dsl_runtime_error("csv | ewma missing 0.5 | csv", "x\n1\n", "ewma: column 'missing' not found");
    run_dsl("csv | ewma missing 0.5 missing=null | csv", "x\n1\n2\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,missing_ewma"));
    assert(line_starts_with(out, 1, "1,"));
    run_dsl("csv | ewma missing 0.5 missing=ignore | csv", "x\n1\n2\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x"));
    assert(strstr(out, "missing_ewma") == NULL);
    expect_dsl_runtime_error("csv | ewma x 0.5 | csv", "x\na\n", "ewma: column 'x' must be numeric");
    run_dsl("csv | ewma x 0.5 on_type_error=null | csv", "x\na\nb\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,x_ewma"));
    assert(line_starts_with(out, 1, "a,"));

    expect_dsl_runtime_error("csv | anomaly missing | csv", "x\n1\n", "anomaly: column 'missing' not found");
    run_dsl("csv | anomaly missing missing=null | csv", "x\n1\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,missing_anomaly"));
    assert(line_starts_with(out, 1, "1,"));
    expect_dsl_runtime_error("csv | anomaly x | csv", "x\na\n", "anomaly: column 'x' must be numeric");
    run_dsl("csv | anomaly x on_type_error=null | csv", "x\na\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,x_anomaly"));
    assert(line_starts_with(out, 1, "a,"));

    expect_dsl_runtime_error("csv | bin missing 10 | csv", "x\n1\n", "bin: column 'missing' not found");
    run_dsl("csv | bin missing 10 missing=null | csv", "x\n1\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,missing_bin"));
    assert(line_starts_with(out, 1, "1,"));
    run_dsl("csv | bin missing 10 missing=ignore | csv", "x\n1\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x"));
    assert(strstr(out, "missing_bin") == NULL);
    expect_dsl_runtime_error("csv | bin x 10 | csv", "x\na\n", "bin: column 'x' must be numeric");
    run_dsl("csv | bin x 10 on_type_error=null | csv", "x\na\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,x_bin"));
    assert(line_starts_with(out, 1, "a,"));

    expect_dsl_runtime_error("csv | window missing 3 avg | csv", "x\n1\n", "window: column 'missing' not found");
    run_dsl("csv | window missing 3 avg missing=null | csv", "x\n1\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,missing_avg3"));
    assert(line_starts_with(out, 1, "1,"));
    run_dsl("csv | window missing 3 avg missing=ignore | csv", "x\n1\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x"));
    assert(strstr(out, "missing_avg3") == NULL);
    expect_dsl_runtime_error("csv | window x 3 avg | csv", "x\na\n", "window: column 'x' must be numeric");
    run_dsl("csv | window x 3 avg x_avg on_type_error=null | csv", "x\na\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,x_avg"));
    assert(line_starts_with(out, 1, "a,"));
    run_dsl("csv | window name 3 count name_count | csv", "name\nAlice\nBob\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "name,name_count"));
    assert(line_starts_with(out, 1, "Alice,1"));

    expect_dsl_runtime_error("csv | rolling-sum missing 3 sum3 | csv", "x\n1\n", "rolling-sum: column 'missing' not found");
    run_dsl("csv | rolling-sum missing 3 sum3 missing=null | csv", "x\n1\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,sum3"));
    assert(line_starts_with(out, 1, "1,"));
    run_dsl("csv | rolling-sum missing 3 sum3 missing=ignore | csv", "x\n1\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x"));
    assert(strstr(out, "sum3") == NULL);
    expect_dsl_runtime_error("csv | rolling-sum x 3 sum3 | csv", "x\na\n", "rolling-sum: column 'x' must be numeric");
    run_dsl("csv | rolling-mean x 3 mean3 on_type_error=null | csv", "x\na\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,mean3"));
    assert(line_starts_with(out, 1, "a,"));

    expect_dsl_runtime_error("csv | interpolate missing forward | csv", "x\n1\n", "interpolate: column 'missing' not found");
    run_dsl("csv | interpolate missing forward missing=null | csv", "x\n1\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,missing"));
    assert(line_starts_with(out, 1, "1,"));
    run_dsl("csv | interpolate missing forward missing=ignore | csv", "x\n1\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x"));
    assert(strstr(out, "missing") == NULL);
    expect_dsl_runtime_error("csv | interpolate x forward | csv", "x\na\n", "interpolate: column 'x' must be numeric");
    run_dsl("csv | interpolate x forward on_type_error=null | csv", "x\na\n", out, sizeof(out));
    assert(strcmp(out, "x\n\n") == 0);

    expect_dsl_runtime_error("csv | datetime missing year | csv", "x\n1\n", "datetime: column 'missing' not found");
    run_dsl("csv | datetime missing year missing=null | csv", "x\n1\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,missing_year"));
    assert(line_starts_with(out, 1, "1,"));
    run_dsl("csv | datetime missing year missing=ignore | csv", "x\n1\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x"));
    assert(strstr(out, "missing_year") == NULL);
    expect_dsl_runtime_error("jsonl | datetime x year | csv", "{\"x\":true}\n", "datetime: column 'x' must be string, date, or timestamp");
    run_dsl("jsonl | datetime x year on_type_error=null | csv", "{\"x\":true}\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,x_year"));
    assert(line_starts_with(out, 1, "true,"));

    expect_dsl_runtime_error("csv | date-trunc missing month | csv", "x\n1\n", "date-trunc: column 'missing' not found");
    run_dsl("csv | date-trunc missing month result=month_start missing=null | csv", "x\n1\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,month_start"));
    assert(line_starts_with(out, 1, "1,"));
    run_dsl("csv | date-trunc missing month missing=ignore | csv", "x\n1\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x"));
    assert(strstr(out, "month_start") == NULL);
    expect_dsl_runtime_error("jsonl | date-trunc x month | csv", "{\"x\":true}\n", "date-trunc: column 'x' must be string, date, or timestamp");
    run_dsl("jsonl | date-trunc x month on_type_error=null | csv", "{\"x\":true}\n", out, sizeof(out));
    assert(strcmp(out, "x\n\n") == 0);


    expect_dsl_runtime_error("csv | normalize missing | csv", "x\n1\n", "normalize: column 'missing' not found");
    run_dsl("csv | normalize missing missing=null | csv", "x\n1\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,missing"));
    assert(line_starts_with(out, 1, "1,"));
    run_dsl("csv | normalize missing missing=ignore | csv", "x\n1\n", out, sizeof(out));
    assert(strcmp(out, "x\n1\n") == 0);
    expect_dsl_runtime_error("jsonl | normalize x | csv", "{\"x\":true}\n", "normalize: column 'x' must be numeric");
    run_dsl("jsonl | normalize x on_type_error=null | csv", "{\"x\":true}\n", out, sizeof(out));
    assert(strcmp(out, "x\n\n") == 0);

    expect_dsl_runtime_error("csv | acf missing 2 | csv", "x\n1\n", "acf: column 'missing' not found");
    run_dsl("csv | acf missing 2 missing=null | csv", "x\n1\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "lag,acf"));
    assert(line_starts_with(out, 1, "0,"));
    assert(line_starts_with(out, 3, "2,"));
    run_dsl("csv | acf missing 2 missing=ignore | csv", "x\n1\n", out, sizeof(out));
    assert(out[0] == '\0');
    expect_dsl_runtime_error("jsonl | acf x 2 | csv", "{\"x\":true}\n", "acf: column 'x' must be numeric");
    run_dsl("jsonl | acf x 2 on_type_error=null | csv", "{\"x\":true}\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "lag,acf"));
    assert(line_starts_with(out, 1, "0,"));
    assert(line_starts_with(out, 3, "2,"));

    expect_dsl_compile_error("csv | ewma x 2 | csv", "ewma alpha must be between 0 and 1");
    expect_dsl_compile_error("csv | anomaly x -1 | csv", "anomaly threshold must be non-negative and finite");
    expect_dsl_compile_error("csv | bin x 10,10 | csv", "bin boundaries must be strictly increasing");
    expect_dsl_compile_error("csv | window x 0 avg | csv", "window size must be a positive integer");
    expect_dsl_compile_error("csv | window x 3 nope | csv", "window func must be avg, sum, min, max, or count");
    expect_dsl_compile_error("csv | rolling-sum x 0 | csv", "rolling-sum size must be a positive integer");
    expect_dsl_compile_error("csv | interpolate x nearest | csv", "interpolate unexpected argument 'nearest'");
    expect_dsl_compile_error("csv | interpolate x method=nearest | csv", "interpolate method must be forward, backward, or linear");
    expect_dsl_compile_error("csv | datetime x decade | csv", "datetime extract must be year");
    expect_dsl_compile_error("csv | date-trunc x decade | csv", "date-trunc trunc must be year");
    expect_dsl_compile_error("csv | normalize x method=bad | csv", "normalize method must be minmax or zscore");
    expect_dsl_compile_error("csv | acf x 0 | csv", "acf lags must be a positive integer");
    expect_dsl_compile_error("csv | acf x lags=0 | csv", "acf lags must be a positive integer");
}

static void test_pipeline_diff(void) {
    char out[2048];
    run_dsl("csv | diff x | csv", "x\n10\n13\n18\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,x_diff"));
    /* row1: null (no prev), row2: 13-10=3, row3: 18-13=5 */
    assert(line_starts_with(out, 1, "10,"));
    assert(line_starts_with(out, 2, "13,3"));
    assert(line_starts_with(out, 3, "18,5"));
}

static void test_pipeline_diff_order2(void) {
    char out[2048];
    run_dsl("csv | diff x 2 | csv", "x\n1\n3\n7\n13\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,x_diff"));
    /* order-2 diff: null, null, 7-2*3+1=2, 13-2*7+3=2 */
    assert(line_starts_with(out, 3, "7,2"));
    assert(line_starts_with(out, 4, "13,2"));
}

static void test_pipeline_date_trunc_missing_column_errors(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"date-trunc\",\"args\":{\"column\":\"missing\",\"trunc\":\"month\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "d\n2024-03-15\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_ERROR);
    assert(tf_pipeline_error(p) != NULL);
    assert(strstr(tf_pipeline_error(p), "date-trunc: column 'missing' not found") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_label_encode(void) {
    char out[2048];
    run_dsl("csv | label-encode city | csv",
            "city\nParis\nLondon\nParis\nBerlin\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "city,city_encoded"));
    /* Paris=0, London=1, Paris=0, Berlin=2 */
    assert(line_starts_with(out, 1, "Paris,0"));
    assert(line_starts_with(out, 2, "London,1"));
    assert(line_starts_with(out, 3, "Paris,0"));
    assert(line_starts_with(out, 4, "Berlin,2"));
}

static void test_pipeline_label_encode_category_policy(void) {
    char out[2048];
    run_dsl("csv | label-encode city city_code categories=Paris,London unknown=other | csv",
            "city\nParis\nBerlin\nLondon\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "city,city_code"));
    assert(line_starts_with(out, 1, "Paris,0"));
    assert(line_starts_with(out, 2, "Berlin,2"));
    assert(line_starts_with(out, 3, "London,1"));

    run_dsl("csv | label-encode city city_code categories=Paris,London unknown=null | csv",
            "city\nBerlin\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "city,city_code"));
    assert(line_starts_with(out, 1, "Berlin,"));
}

static void test_pipeline_anomaly(void) {
    char out[2048];
    run_dsl("csv | anomaly x 2 | csv",
            "x\n10\n11\n9\n10\n11\n10\n100\n10\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,x_anomaly"));
    /* Normal values get 0, outlier 100 gets 1 */
    assert(line_starts_with(out, 1, "10,0"));
    assert(line_starts_with(out, 2, "11,0"));
    assert(line_starts_with(out, 7, "100,1"));
    assert(line_starts_with(out, 8, "10,0"));
}

static void test_pipeline_split_data(void) {
    char out[2048];
    run_dsl("csv | split-data 0.5 | csv",
            "x\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x,_split"));
    /* With seed=42, ratio=0.5, should get a mix of train/test */
    assert(strstr(out, "train") != NULL);
    assert(strstr(out, "test") != NULL);
    /* Deterministic: same seed should give same split */
    char out2[2048];
    run_dsl("csv | split-data 0.5 | csv",
            "x\n1\n2\n3\n4\n5\n6\n7\n8\n9\n10\n", out2, sizeof(out2));
    assert(strcmp(out, out2) == 0);
}

static void test_pipeline_onehot(void) {
    char out[2048];
    run_dsl("csv | onehot color | csv",
            "name,color\nA,red\nB,blue\nC,red\nD,green\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "name,color,color_red,color_blue,color_green"));
    /* A,red  → 1,0,0 */
    assert(line_starts_with(out, 1, "A,red,1,0,0"));
    /* B,blue → 0,1,0 */
    assert(line_starts_with(out, 2, "B,blue,0,1,0"));
    /* C,red  → 1,0,0 */
    assert(line_starts_with(out, 3, "C,red,1,0,0"));
    /* D,green → 0,0,1 */
    assert(line_starts_with(out, 4, "D,green,0,0,1"));
}

static void test_pipeline_onehot_drop(void) {
    char out[2048];
    run_dsl("csv | onehot color --drop | csv",
            "name,color\nA,red\nB,blue\n", out, sizeof(out));
    /* Original "color" column should be dropped */
    assert(line_starts_with(out, 0, "name,color_red,color_blue"));
    assert(line_starts_with(out, 1, "A,1,0"));
    assert(line_starts_with(out, 2, "B,0,1"));
}

static void test_pipeline_onehot_category_policy(void) {
    char out[2048];
    run_dsl("csv | onehot color categories=red,blue unknown=null | csv",
            "name,color\nA,red\nB,green\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "name,color,color_red,color_blue"));
    assert(line_starts_with(out, 1, "A,red,1,0"));
    assert(line_starts_with(out, 2, "B,green,0,0"));

    run_dsl("csv | onehot color categories=red,blue unknown=other --drop | csv",
            "name,color\nA,red\nB,green\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "name,color_red,color_blue,color___other__"));
    assert(line_starts_with(out, 1, "A,1,0,0"));
    assert(line_starts_with(out, 2, "B,0,0,1"));
}

static void test_pipeline_onehot_preserves_long_generated_names(void) {
    char category[601];
    memset(category, 'x', sizeof(category) - 1);
    category[sizeof(category) - 1] = '\0';

    char input[700];
    snprintf(input, sizeof(input), "color\n%s\n", category);

    char out[1600];
    run_dsl("csv | onehot color --drop | csv", input, out, sizeof(out));

    char expected_header[700];
    snprintf(expected_header, sizeof(expected_header), "color_%s", category);
    assert(line_starts_with(out, 0, expected_header));
    assert(line_starts_with(out, 1, "1"));
}

static void test_pipeline_interpolate_forward(void) {
    char out[2048];
    run_dsl("csv | interpolate x forward | csv",
            "x\n10\n\n\n20\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x"));
    /* Forward fill: 10, 10, 10, 20 */
    assert(line_starts_with(out, 1, "10"));
    assert(line_starts_with(out, 2, "10"));
    assert(line_starts_with(out, 3, "10"));
    assert(line_starts_with(out, 4, "20"));
}

static void test_pipeline_interpolate_linear(void) {
    char out[2048];
    run_dsl("csv | interpolate x linear | csv",
            "x\n10\n\n\n40\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x"));
    /* Linear: 10, 20, 30, 40 */
    assert(line_starts_with(out, 1, "10"));
    assert(line_starts_with(out, 2, "20"));
    assert(line_starts_with(out, 3, "30"));
    assert(line_starts_with(out, 4, "40"));
}

static void test_pipeline_normalize_minmax(void) {
    char out[2048];
    run_dsl("csv | normalize x | csv", "x\n0\n50\n100\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x"));
    /* minmax: 0→0, 50→0.5, 100→1 */
    assert(line_starts_with(out, 1, "0"));
    assert(line_starts_with(out, 2, "0.5"));
    assert(line_starts_with(out, 3, "1"));
}

static void test_pipeline_normalize_zscore(void) {
    char out[2048];
    run_dsl("csv | normalize x zscore | csv",
            "x\n10\n20\n30\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "x"));
    /* zscore of [10,20,30]: mean=20, std=10 → -1, 0, 1 */
    assert(strstr(out, "-1") != NULL);
    /* Middle value should be 0 */
    assert(line_starts_with(out, 2, "0"));
}

static void test_pipeline_normalize_audit_side_channel(void) {
    tf_pipeline *p = pipeline_from_dsl("csv batch_size=2 | normalize x minmax audit audit_limit=2 | csv");
    assert(p != NULL);
    const char *input = "name,x\nA,10\nB,20\nC,30\n";
    assert(tf_pipeline_push(p, (const uint8_t *)input, strlen(input)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);

    char out[2048];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, (uint8_t *)out, sizeof(out) - 1);
    out[n] = '\0';
    assert(strstr(out, "A,0") != NULL);
    assert(strstr(out, "B,0.5") != NULL);
    assert(strstr(out, "C,1") != NULL);

    uint8_t stats[4096];
    size_t stats_n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats) - 1);
    assert(stats_n > 0);
    stats[stats_n] = '\0';
    char *text = (char *)stats;
    assert(strstr(text, "\"op\":\"normalize\"") != NULL);
    assert(strstr(text, "\"event\":\"value_changed\"") != NULL);
    assert(strstr(text, "\"reason\":\"normalize_minmax\"") != NULL);
    assert(strstr(text, "\"method\":\"minmax\"") != NULL);
    assert(strstr(text, "\"row\":1") != NULL);
    assert(strstr(text, "\"row\":2") != NULL);
    assert(strstr(text, "\"row\":3") == NULL);
    assert(strstr(text, "\"before\":10") != NULL);
    assert(strstr(text, "\"after\":0") != NULL);
    assert(strstr(text, "\"data\":") != NULL);
    tf_pipeline_free(p);
}

static void test_pipeline_acf(void) {
    char out[2048];
    run_dsl("csv | acf x 3 | csv",
            "x\n1\n2\n3\n4\n5\n6\n7\n8\n", out, sizeof(out));
    assert(line_starts_with(out, 0, "lag,acf"));
    /* lag 0 → acf = 1.0 */
    assert(line_starts_with(out, 1, "0,1"));
    /* lag 1 → acf = 0.625 */
    assert(line_starts_with(out, 2, "1,0.625"));
    /* 4 rows total: lag 0,1,2,3 */
    assert(line_starts_with(out, 4, "3,"));
}

static void test_dsl_data_prep_ops(void) {
    /* Test that all new ops can be parsed from DSL */
    const char *dsls[] = {
        "csv | ewma x 0.3 | csv",
        "csv | diff x | csv",
        "csv | diff x 2 | csv",
        "csv | label-encode city | csv",
        "csv | label-encode city city_id categories=NY,LA max_categories=3 unknown=other | csv",
        "csv | anomaly x 2.5 | csv",
        "csv | split-data 0.8 | csv",
        "csv | onehot city | csv",
        "csv | onehot city --drop | csv",
        "csv | onehot city categories=NY,LA max_categories=3 unknown=null | csv",
        "csv | interpolate x linear | csv",
        "csv | interpolate x forward | csv",
        "csv | interpolate x method=backward missing=null on_type_error=null | csv",
        "csv | normalize x,y | csv",
        "csv | normalize x,y zscore | csv",
        "csv | acf x 20 | csv",
    };
    size_t n = sizeof(dsls) / sizeof(dsls[0]);
    for (size_t i = 0; i < n; i++) {
        char *err = NULL;
        tf_ir_plan *plan = tf_dsl_parse(dsls[i], strlen(dsls[i]), &err);
        assert(plan != NULL);
        tf_ir_plan_free(plan);
    }
}

/* ================================================================
 * Main
 * ================================================================ */

int main(int argc, char **argv) {
    if (argc > 1) {
        test_filter_count = argc - 1;
        test_filters = (const char **)(argv + 1);
    }

    printf("Tranfi Core Tests\n");
    printf("==================\n");
    if (test_filter_count > 0) {
        printf("\nFilter:");
        for (int i = 0; i < test_filter_count; i++) printf(" %s", test_filters[i]);
    }
    printf("\n\n");

    printf("Arena:\n");
    TEST(test_arena_basic);
    TEST(test_arena_large_alloc);

    printf("\nBuffer:\n");
    TEST(test_buffer_basic);
    TEST(test_buffer_partial_read);
    TEST(test_buffer_line_and_side_error);

    printf("\nSize Safety:\n");
    TEST(test_size_checked_arithmetic);
    TEST(test_global_byte_caps);
    TEST(test_codec_size_argument_clamps);
    TEST(test_op_numeric_argument_clamps);

    printf("\nBatch:\n");
    TEST(test_batch_create);
    TEST(test_batch_set_get);
    TEST(test_batch_setters_report_failures);
    TEST(test_batch_schema_copy_helpers);
    TEST(test_report_format_stats_csv);
    TEST(test_batch_col_index);
    TEST(test_batch_allocation_overflow_guards);

    printf("\nExpressions:\n");
    TEST(test_expr_parse_simple);
    TEST(test_expr_parse_compound);
    TEST(test_expr_depth_limit);
    TEST(test_expr_parse_string_cmp);
    TEST(test_expr_eval_numeric);
    TEST(test_expr_eval_string);
    TEST(test_expr_eval_and_or);

    printf("\nPipeline (CSV):\n");
    TEST(test_pipeline_csv_passthrough);
    TEST(test_pipeline_csv_header_false);
    TEST(test_pipeline_float_roundtrip_bits);
    TEST(test_pipeline_table_human_float_format);
    TEST(test_pipeline_sink_callbacks);
    TEST(test_pipeline_batch_sink_callbacks);
    TEST(test_pipeline_progress_callback);
    TEST(test_pipeline_file_runner);
    TEST(test_pipeline_fd_runner);
    TEST(test_pipeline_csv_filter);
    TEST(test_pipeline_filter_audit_side_channel);
    TEST(test_pipeline_csv_select);
    TEST(test_pipeline_csv_select_helpers);
    TEST(test_pipeline_csv_relocate);
    TEST(test_pipeline_csv_relocate_helpers);
    TEST(test_pipeline_csv_head);
    TEST(test_pipeline_csv_rename);
    TEST(test_pipeline_combined);
    TEST(test_pipeline_csv_null_literals);
    TEST(test_pipeline_csv_quoted_newline_and_escaped_quote_chunks);
    TEST(test_pipeline_csv_unquoted_quote_does_not_merge_records);
    TEST(test_pipeline_csv_repair_diagnostics);
    TEST(test_pipeline_csv_strict_field_count);
    TEST(test_pipeline_csv_max_record_bytes);
    TEST(test_pipeline_csv_wide_columns);
    TEST(test_pipeline_csv_max_columns);
    TEST(test_pipeline_csv_column_name_byte_cap);
    TEST(test_pipeline_csv_quoted_null_literals_disabled);
    TEST(test_pipeline_csv_comment_skip_empty_trim);
    TEST(test_pipeline_source_name_boundary_flush);
    TEST(test_pipeline_csv_skip_repeated_header_boundary);
    TEST(test_pipeline_source_name_default);
    TEST(test_pipeline_fill_null_audit_side_channel);

    printf("\nPipeline (JSONL):\n");
    TEST(test_pipeline_jsonl_passthrough);
    TEST(test_pipeline_jsonl_malformed_default_skip);
    TEST(test_pipeline_jsonl_malformed_warn_errors);
    TEST(test_pipeline_jsonl_malformed_quarantine_truncates);
    TEST(test_pipeline_jsonl_malformed_fail);
    TEST(test_pipeline_jsonl_malformed_chunk_line_numbers);
    TEST(test_pipeline_jsonl_max_record_bytes);
    TEST(test_pipeline_jsonl_column_name_byte_cap);
    TEST(test_pipeline_jsonl_filter);
    TEST(test_pipeline_jsonl_group_agg_preserves_int_key);
    TEST(test_pipeline_jsonl_type_widening);
    TEST(test_pipeline_json_extract_text);
    TEST(test_pipeline_jsonl_preserves_nested_json_and_extracts);
    TEST(test_pipeline_json_filter_text);
    TEST(test_pipeline_json_filter_nested_jsonl);
    TEST(test_pipeline_json_schema_filter_text);
    TEST(test_pipeline_json_schema_annotate_nested_jsonl);
    TEST(test_pipeline_json_schema_keyword_clamps);
    TEST(test_pipeline_json_flatten_text);
    TEST(test_pipeline_json_flatten_nested_jsonl);
    TEST(test_pipeline_group_agg_hash_key_collision_regression);
    TEST(test_pipeline_group_agg_hash_rehashes_many_groups);
    TEST(test_pipeline_group_agg_sorted_streams_runs);
    TEST(test_pipeline_group_agg_null_semantics_unsorted);
    TEST(test_pipeline_group_agg_null_semantics_sorted);

    printf("\nPipeline (Text):\n");
    TEST(test_pipeline_text_passthrough);
    TEST(test_pipeline_text_head);
    TEST(test_pipeline_text_grep);
    TEST(test_pipeline_text_grep_invert);
    TEST(test_pipeline_text_grep_regex);
    TEST(test_pipeline_text_max_record_bytes);

    printf("\nMisc:\n");
    TEST(test_pipeline_stats_channel);
    TEST(test_pipeline_finish_step_flush_boundaries);
    TEST(test_pipeline_step_stats_channel);
    TEST(test_pipeline_step_stats_state_bytes);
    TEST(test_pipeline_error_handling);
    TEST(test_thread_local_last_error);
    TEST(test_pipeline_key_state_caps);
    TEST(test_version);

    printf("\nOp Registry:\n");
    TEST(test_registry_find_all_ops);
    TEST(test_registry_op_kinds);
    TEST(test_registry_capabilities);
    TEST(test_registry_contract_metadata);
    TEST(test_registry_count_and_iterate);

    printf("\nIR Plan:\n");
    TEST(test_ir_plan_create_and_free);
    TEST(test_ir_plan_add_nodes);
    TEST(test_ir_plan_clone);

    printf("\nIR Serialization:\n");
    TEST(test_ir_from_json);
    TEST(test_ir_from_json_errors);
    TEST(test_ir_roundtrip);

    printf("\nIR Validation:\n");
    TEST(test_ir_validate_valid_plan);
    TEST(test_ir_validate_no_decoder);
    TEST(test_ir_validate_no_encoder);
    TEST(test_ir_validate_unknown_op);
    TEST(test_ir_validate_missing_required_arg);
    TEST(test_ir_validate_plan_caps);
    TEST(test_ir_validate_contract_metadata);
    TEST(test_ir_validate_dynamic_pivot_contract_metadata);
    TEST(test_ir_validate_host_policy_denies_file_args);
    TEST(test_ir_validate_host_policy_denies_rules_file_and_spill);
    TEST(test_ir_validate_host_policy_workspace_resolver);
    TEST(test_ir_validate_host_policy_dynamic_caps);

    printf("\nSchema Inference:\n");
    TEST(test_ir_schema_passthrough);
    TEST(test_ir_schema_select_known);
    TEST(test_ir_schema_rename_known);
    TEST(test_ir_schema_group_agg_preserves_key_types);

    printf("\nCompiler:\n");
    TEST(test_compile_native_valid);
    TEST(test_compile_to_sql_grep_literal_chars);
    TEST(test_compile_to_sql_rejects_sample);
    TEST(test_compile_to_sql_rejects_stats);
    TEST(test_compile_to_sql_supported_matrix);
    TEST(test_compile_to_sql_rejected_matrix);
    TEST(test_pipeline_create_from_ir);
    TEST(test_public_ir_api);

    printf("\nDSL Parser:\n");
    TEST(test_dsl_csv_passthrough);
    TEST(test_dsl_jsonl_passthrough);
    TEST(test_dsl_text);
    TEST(test_dsl_filter);
    TEST(test_dsl_select);
    TEST(test_dsl_select_spaces);
    TEST(test_dsl_relocate);
    TEST(test_dsl_rename);
    TEST(test_dsl_head);
    TEST(test_dsl_combined);
    TEST(test_dsl_explicit_codec);
    TEST(test_dsl_codec_options);
    TEST(test_dsl_errors);
    TEST(test_dsl_expr_bare_col);

    printf("\nExpression Arithmetic:\n");
    TEST(test_expr_arithmetic_parse);
    TEST(test_expr_arithmetic_eval_int);
    TEST(test_expr_arithmetic_precedence);
    TEST(test_expr_arithmetic_comparison);

    printf("\nString & Conditional Functions:\n");
    TEST(test_expr_string_functions);
    TEST(test_expr_date_functions);
    TEST(test_expr_conditional_functions);
    TEST(test_pipeline_derive_date_funcs);
    TEST(test_pipeline_derive_string_funcs);

    printf("\nNew Transforms:\n");
    TEST(test_pipeline_csv_skip);
    TEST(test_pipeline_csv_derive);
    TEST(test_pipeline_csv_across);
    TEST(test_pipeline_csv_stats);
    TEST(test_pipeline_csv_stats_advanced);
    TEST(test_pipeline_csv_stats_distinct);
    TEST(test_pipeline_csv_stats_distinct_large);
    TEST(test_pipeline_csv_stats_hist_sample);
    TEST(test_pipeline_csv_scan);
    TEST(test_pipeline_csv_stats_missing);
    TEST(test_pipeline_csv_unique);
    TEST(test_pipeline_csv_unique_approx);
    TEST(test_pipeline_unique_sorted);
    TEST(test_pipeline_csv_sort);
    TEST(test_pipeline_csv_sort_desc);
    TEST(test_pipeline_csv_sort_stable_equal_keys);
    TEST(test_pipeline_skip_head_combo);

    printf("\nNew DSL:\n");
    TEST(test_dsl_skip);
    TEST(test_dsl_derive);
    TEST(test_dsl_across);
    TEST(test_dsl_stats);
    TEST(test_dsl_stats_selective);
    TEST(test_dsl_scan);
    TEST(test_dsl_scan_selective);
    TEST(test_dsl_schema_infer);
    TEST(test_dsl_tee);
    TEST(test_dsl_unique);
    TEST(test_dsl_sort);
    TEST(test_dsl_sort_desc);
    TEST(test_dsl_source_name);

    printf("\nNew Registry:\n");
    TEST(test_registry_new_ops);
    TEST(test_registry_count_updated);
    TEST(test_dsl_sort_head_rewrite);
    TEST(test_dsl_compatibility_forms);
    TEST(test_pipeline_rejects_empty_delimiters);

    printf("\nNew Operators:\n");
    TEST(test_pipeline_tail);
    TEST(test_pipeline_cast_audit_side_channel);
    TEST(test_pipeline_cast_on_error_policies);
    TEST(test_pipeline_clip);
    TEST(test_pipeline_replace);
    TEST(test_pipeline_replace_audit_side_channel);
    TEST(test_pipeline_replace_regex);
    TEST(test_pipeline_explode);
    TEST(test_pipeline_explode_unpivot_caps);
    TEST(test_pipeline_trim);
    TEST(test_pipeline_hash);
    TEST(test_pipeline_validate);
    TEST(test_pipeline_validate_rules);
    TEST(test_pipeline_quarantine);
    TEST(test_pipeline_assert_actions);
    TEST(test_pipeline_schema_actions);
    TEST(test_pipeline_schema_baseline_drift);
    TEST(test_pipeline_schema_selectors);
    TEST(test_pipeline_schema_regex_budgets);
    TEST(test_pipeline_schema_audit_privacy_controls);
    TEST(test_pipeline_audit_privacy_migrated_producers);
    TEST(test_selector_depth_limit);
    TEST(test_json_path_depth_limit);
    TEST(test_pipeline_schema_infer);
    TEST(test_pipeline_tee_side_channel);
    TEST(test_pipeline_stack_preserves_long_cells);
    TEST(test_pipeline_datetime);
    TEST(test_pipeline_step_running_sum);
    TEST(test_pipeline_frequency);
    TEST(test_pipeline_frequency_overflow_other);
    TEST(test_pipeline_top);
    TEST(test_pipeline_bottom_k);
    TEST(test_pipeline_slice_min_string);
    TEST(test_dsl_new_ops);
    TEST(test_dsl_grep_regex);
    TEST(test_dsl_replace_regex);

    printf("\nSpill Security:\n");
    TEST(test_spill_session_security_basics);
    TEST(test_spill_session_cleanup_after_abort);
    TEST(test_spill_sort_uses_private_session_dir);

    printf("\nDate/Timestamp:\n");
    TEST(test_csv_date_autodetect);
    TEST(test_csv_timestamp_autodetect);
    TEST(test_csv_date_timestamp_widening);
    TEST(test_cast_string_to_date);
    TEST(test_cast_date_to_timestamp);
    TEST(test_filter_date_comparison);
    TEST(test_sort_by_date);
    TEST(test_spill_sort_typed_multi_key_null_ordering);
    TEST(test_spill_sort_stable_equal_keys);
    TEST(test_spill_unique_preserves_first_row_order);
    TEST(test_spill_group_agg_preserves_first_group_order);
    TEST(test_datetime_native_date);

    printf("\nPivot:\n");
    TEST(test_pipeline_pivot_first);
    TEST(test_pipeline_pivot_sum);
    TEST(test_pipeline_pivot_spill_preserves_group_and_category_order);
    TEST(test_pipeline_pivot_sorted_declared_streaming);
    TEST(test_pipeline_pivot_declared_unknown_category_errors);
    TEST(test_pipeline_pivot_max_categories_errors);
    TEST(test_dsl_pivot);

    printf("\nJoin:\n");
    TEST(test_pipeline_join_inner);
    TEST(test_pipeline_join_left);
    TEST(test_pipeline_filtering_joins);
    TEST(test_pipeline_filtering_join_spill_preserves_left_order);
    TEST(test_pipeline_mutating_join_spill_preserves_left_and_match_order);
    TEST(test_pipeline_join_typed_keys);
    TEST(test_pipeline_set_ops);
    TEST(test_pipeline_set_ops_spill_preserves_left_order);
    TEST(test_pipeline_union_ops);
    TEST(test_pipeline_set_ops_columns_and_caps);
    TEST(test_pipeline_set_ops_sorted_mode);
    TEST(test_pipeline_join_caps);
    TEST(test_pipeline_join_output_caps);
    TEST(test_pipeline_join_sorted_mode);
    TEST(test_dsl_join);
    TEST(test_dsl_set_ops);
    TEST(test_dsl_join_caps);
    TEST(test_dsl_join_eq);

    printf("\nRecipes:\n");
    TEST(test_compile_dsl);
    TEST(test_recipe_roundtrip);

    printf("\nBuilt-in Recipes:\n");
    TEST(test_recipe_count);
    TEST(test_recipe_find);
    TEST(test_recipe_accessors);
    TEST(test_recipe_run_preview);
    TEST(test_recipe_run_sniff);
    TEST(test_recipe_run_dedup);

    printf("\nData Prep & Time Series:\n");
    TEST(test_pipeline_sample_deterministic_seed);
    TEST(test_pipeline_rowid_global);
    TEST(test_pipeline_rowid_unsorted_grouped);
    TEST(test_pipeline_rowid_sorted_grouped);
    TEST(test_pipeline_rolling_ops_chunk_boundary);
    TEST(test_pipeline_rolling_bool_chunk_boundary);
    TEST(test_pipeline_rolling_bool_null_policies);
    TEST(test_pipeline_lag_chunk_boundary);
    TEST(test_pipeline_lag_preserves_string_type);
    TEST(test_pipeline_shift_lead_large_offset_chunks);
    TEST(test_pipeline_rleid);
    TEST(test_pipeline_ewma);
    TEST(test_pipeline_h14_numeric_missing_type_policies);
    TEST(test_pipeline_diff);
    TEST(test_pipeline_diff_order2);
    TEST(test_pipeline_date_trunc_missing_column_errors);
    TEST(test_pipeline_label_encode);
    TEST(test_pipeline_label_encode_category_policy);
    TEST(test_pipeline_anomaly);
    TEST(test_pipeline_split_data);
    TEST(test_pipeline_onehot);
    TEST(test_pipeline_onehot_drop);
    TEST(test_pipeline_onehot_category_policy);
    TEST(test_pipeline_onehot_preserves_long_generated_names);
    TEST(test_pipeline_interpolate_forward);
    TEST(test_pipeline_interpolate_linear);
    TEST(test_pipeline_normalize_minmax);
    TEST(test_pipeline_normalize_zscore);
    TEST(test_pipeline_normalize_audit_side_channel);
    TEST(test_pipeline_acf);
    TEST(test_dsl_data_prep_ops);

    printf("\n==================\n");
    if (tests_run == 0) {
        fprintf(stderr, "No selected tests matched\n");
        return 1;
    }
    printf("%d/%d tests passed\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
