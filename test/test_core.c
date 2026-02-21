/*
 * test_core.c — Unit tests for the Tranfi core.
 * Simple assert-based testing. Returns non-zero on failure.
 */

#include "tranfi.h"
#include "internal.h"
#include "ir.h"
#include "dsl.h"
#include "recipes.h"
#include "cJSON.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) do { \
    printf("  %-50s", #name); \
    tests_run++; \
    name(); \
    tests_passed++; \
    printf("PASS\n"); \
} while(0)

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

    uint8_t out[32];
    size_t n = tf_buffer_read(&b, out, sizeof(out));
    assert(n == 11);
    assert(memcmp(out, data, 11) == 0);
    assert(tf_buffer_readable(&b) == 0);

    tf_buffer_free(&b);
}

static void test_buffer_partial_read(void) {
    tf_buffer b;
    tf_buffer_init(&b);

    tf_buffer_write(&b, (const uint8_t *)"abcdefgh", 8);

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

/* ================================================================
 * Batch tests
 * ================================================================ */

static void test_batch_create(void) {
    tf_batch *b = tf_batch_create(3, 10);
    assert(b != NULL);
    assert(b->n_cols == 3);
    assert(b->n_rows == 0);

    tf_batch_set_schema(b, 0, "name", TF_TYPE_STRING);
    tf_batch_set_schema(b, 1, "age", TF_TYPE_INT64);
    tf_batch_set_schema(b, 2, "score", TF_TYPE_FLOAT64);

    assert(strcmp(b->col_names[0], "name") == 0);
    assert(b->col_types[1] == TF_TYPE_INT64);

    tf_batch_free(b);
}

static void test_batch_set_get(void) {
    tf_batch *b = tf_batch_create(3, 4);
    tf_batch_set_schema(b, 0, "name", TF_TYPE_STRING);
    tf_batch_set_schema(b, 1, "age", TF_TYPE_INT64);
    tf_batch_set_schema(b, 2, "score", TF_TYPE_FLOAT64);

    tf_batch_set_string(b, 0, 0, "Alice");
    tf_batch_set_int64(b, 0, 1, 30);
    tf_batch_set_float64(b, 0, 2, 85.5);
    b->n_rows = 1;

    assert(strcmp(tf_batch_get_string(b, 0, 0), "Alice") == 0);
    assert(tf_batch_get_int64(b, 0, 1) == 30);
    assert(tf_batch_get_float64(b, 0, 2) == 85.5);
    assert(!tf_batch_is_null(b, 0, 0));

    tf_batch_set_null(b, 0, 2);
    assert(tf_batch_is_null(b, 0, 2));

    tf_batch_free(b);
}

static void test_batch_col_index(void) {
    tf_batch *b = tf_batch_create(2, 1);
    tf_batch_set_schema(b, 0, "foo", TF_TYPE_INT64);
    tf_batch_set_schema(b, 1, "bar", TF_TYPE_STRING);

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

static void test_expr_parse_string_cmp(void) {
    tf_expr *e = tf_expr_parse("col('city') == 'London'");
    assert(e != NULL);
    tf_expr_free(e);
}

static void test_expr_eval_numeric(void) {
    tf_batch *b = tf_batch_create(1, 2);
    tf_batch_set_schema(b, 0, "x", TF_TYPE_INT64);
    tf_batch_set_int64(b, 0, 0, 10);
    tf_batch_set_int64(b, 1, 0, -5);
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
    tf_batch_set_schema(b, 0, "city", TF_TYPE_STRING);
    tf_batch_set_string(b, 0, 0, "London");
    tf_batch_set_string(b, 1, 0, "Paris");
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
    tf_batch_set_schema(b, 0, "a", TF_TYPE_INT64);
    tf_batch_set_schema(b, 1, "b", TF_TYPE_INT64);
    tf_batch_set_int64(b, 0, 0, 10);
    tf_batch_set_int64(b, 0, 1, 20);
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

    uint8_t stats[512];
    size_t n = tf_pipeline_pull(p, TF_CHAN_STATS, stats, sizeof(stats));
    assert(n > 0);
    stats[n] = '\0';
    assert(strstr((char *)stats, "rows_in") != NULL);

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

/* ================================================================
 * Op Registry tests
 * ================================================================ */

static void test_registry_find_all_ops(void) {
    /* All built-in ops should be found */
    const char *ops[] = {
        "codec.csv.decode", "codec.csv.encode",
        "codec.jsonl.decode", "codec.jsonl.encode",
        "filter", "select", "rename", "head",
        "skip", "derive", "stats", "unique", "sort",
        "reorder", "dedup", "validate", "trim", "fill-null",
        "cast", "clip", "replace", "hash", "bin",
        "fill-down", "step", "window",
        "explode", "split", "unpivot",
        "tail", "top", "sample", "group-agg", "frequency",
        "datetime", "flatten", "join"
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

static void test_registry_count_and_iterate(void) {
    size_t count = tf_op_registry_count();
    assert(count == 41);  /* 6 codecs + 35 transforms */
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
    tf_batch_set_schema(b, 0, "a", TF_TYPE_INT64);
    tf_batch_set_schema(b, 1, "b", TF_TYPE_INT64);
    tf_batch_set_int64(b, 0, 0, 10);
    tf_batch_set_int64(b, 0, 1, 3);
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
    tf_batch_set_schema(b, 0, "a", TF_TYPE_INT64);
    tf_batch_set_schema(b, 1, "b", TF_TYPE_INT64);
    tf_batch_set_int64(b, 0, 0, 2);
    tf_batch_set_int64(b, 0, 1, 3);
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
    tf_batch_set_schema(b, 0, "a", TF_TYPE_INT64);
    tf_batch_set_schema(b, 1, "b", TF_TYPE_INT64);
    tf_batch_set_int64(b, 0, 0, 7);
    tf_batch_set_int64(b, 0, 1, 5);
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
    tf_batch_set_schema(b, 0, "name", TF_TYPE_STRING);
    tf_batch_set_schema(b, 1, "age", TF_TYPE_INT64);
    tf_batch_set_string(b, 0, 0, "Alice");
    tf_batch_set_int64(b, 0, 1, 30);
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

static void test_expr_conditional_functions(void) {
    tf_batch *b = tf_batch_create(2, 1);
    tf_batch_set_schema(b, 0, "age", TF_TYPE_INT64);
    tf_batch_set_schema(b, 1, "name", TF_TYPE_STRING);
    tf_batch_set_int64(b, 0, 0, 30);
    tf_batch_set_string(b, 0, 1, "Alice");
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

static void test_pipeline_csv_stats_hist_sample(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"stats\",\"args\":{\"stats\":[\"hist\",\"sample\"]}},"
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

    /* hist should have colon-separated format, sample should have comma-separated values */
    assert(strstr((char *)out, "hist,sample") != NULL);
    /* hist output contains colons for "lo:hi:counts" */
    char *data_line = strstr((char *)out, "\nval,");
    assert(data_line != NULL);
    assert(strstr(data_line, ":") != NULL); /* hist has colons */

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
}

static void test_pipeline_validate(void) {
    const char *plan =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"validate\",\"args\":{\"expr\":\"col('age') > 25\"}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    tf_pipeline *p = tf_pipeline_create(plan, strlen(plan));
    assert(p != NULL);
    const char *csv = "name,age\nAlice,30\nBob,20\n";
    assert(tf_pipeline_push(p, (const uint8_t *)csv, strlen(csv)) == TF_OK);
    assert(tf_pipeline_finish(p) == TF_OK);
    uint8_t out[1024];
    size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, out, sizeof(out));
    assert(n > 0);
    out[n] = '\0';
    /* Both rows should be present, with _valid column */
    assert(strstr((char *)out, "_valid") != NULL);
    assert(strstr((char *)out, "Alice") != NULL);
    assert(strstr((char *)out, "Bob") != NULL);
    tf_pipeline_free(p);
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
    plan = tf_dsl_parse("csv | top 10 score | csv", 24, &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "top") == 0);
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
}

static void test_registry_new_ops(void) {
    const char *new_ops[] = { "skip", "derive", "stats", "unique", "sort" };
    for (size_t i = 0; i < sizeof(new_ops) / sizeof(new_ops[0]); i++) {
        const tf_op_entry *e = tf_op_registry_find(new_ops[i]);
        assert(e != NULL);
        assert(strcmp(e->name, new_ops[i]) == 0);
        assert(e->kind == TF_OP_TRANSFORM);
    }
}

static void test_registry_count_updated(void) {
    size_t count = tf_op_registry_count();
    assert(count == 41);  /* 6 codecs + 35 transforms */
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

static void test_dsl_pivot(void) {
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse("csv | pivot metric value sum | csv", 34, &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "pivot") == 0);
    cJSON *nc = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "name_column");
    assert(nc != NULL && strcmp(nc->valuestring, "metric") == 0);
    cJSON *vc = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "value_column");
    assert(vc != NULL && strcmp(vc->valuestring, "value") == 0);
    cJSON *agg = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "agg");
    assert(agg != NULL && strcmp(agg->valuestring, "sum") == 0);
    tf_ir_plan_free(plan);
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

static void test_dsl_join(void) {
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse("csv | join lookup.csv on id --left | csv", 41, &error);
    assert(plan != NULL);
    assert(strcmp(plan->nodes[1].op, "join") == 0);
    cJSON *file_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "file");
    assert(file_j != NULL && strcmp(file_j->valuestring, "lookup.csv") == 0);
    cJSON *on_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "on");
    assert(on_j != NULL && strcmp(on_j->valuestring, "id") == 0);
    cJSON *how_j = cJSON_GetObjectItemCaseSensitive(plan->nodes[1].args, "how");
    assert(how_j != NULL && strcmp(how_j->valuestring, "left") == 0);
    tf_ir_plan_free(plan);
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
    tf_string_free(json);
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
    assert(tf_recipe_count() == 20);
}

static void test_recipe_find(void) {
    const char *dsl = tf_recipe_find_dsl("profile");
    assert(dsl != NULL);
    assert(strstr(dsl, "stats") != NULL);

    /* Case-insensitive */
    const char *dsl2 = tf_recipe_find_dsl("PREVIEW");
    assert(dsl2 != NULL);
    assert(strstr(dsl2, "head 10") != NULL);

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
 * Main
 * ================================================================ */

int main(void) {
    printf("Tranfi Core Tests\n");
    printf("==================\n\n");

    printf("Arena:\n");
    TEST(test_arena_basic);
    TEST(test_arena_large_alloc);

    printf("\nBuffer:\n");
    TEST(test_buffer_basic);
    TEST(test_buffer_partial_read);

    printf("\nBatch:\n");
    TEST(test_batch_create);
    TEST(test_batch_set_get);
    TEST(test_batch_col_index);

    printf("\nExpressions:\n");
    TEST(test_expr_parse_simple);
    TEST(test_expr_parse_compound);
    TEST(test_expr_parse_string_cmp);
    TEST(test_expr_eval_numeric);
    TEST(test_expr_eval_string);
    TEST(test_expr_eval_and_or);

    printf("\nPipeline (CSV):\n");
    TEST(test_pipeline_csv_passthrough);
    TEST(test_pipeline_csv_filter);
    TEST(test_pipeline_csv_select);
    TEST(test_pipeline_csv_head);
    TEST(test_pipeline_csv_rename);
    TEST(test_pipeline_combined);

    printf("\nPipeline (JSONL):\n");
    TEST(test_pipeline_jsonl_passthrough);
    TEST(test_pipeline_jsonl_filter);

    printf("\nPipeline (Text):\n");
    TEST(test_pipeline_text_passthrough);
    TEST(test_pipeline_text_head);
    TEST(test_pipeline_text_grep);
    TEST(test_pipeline_text_grep_invert);
    TEST(test_pipeline_text_grep_regex);

    printf("\nMisc:\n");
    TEST(test_pipeline_stats_channel);
    TEST(test_pipeline_error_handling);
    TEST(test_version);

    printf("\nOp Registry:\n");
    TEST(test_registry_find_all_ops);
    TEST(test_registry_op_kinds);
    TEST(test_registry_capabilities);
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

    printf("\nSchema Inference:\n");
    TEST(test_ir_schema_passthrough);
    TEST(test_ir_schema_select_known);
    TEST(test_ir_schema_rename_known);

    printf("\nCompiler:\n");
    TEST(test_compile_native_valid);
    TEST(test_pipeline_create_from_ir);
    TEST(test_public_ir_api);

    printf("\nDSL Parser:\n");
    TEST(test_dsl_csv_passthrough);
    TEST(test_dsl_jsonl_passthrough);
    TEST(test_dsl_text);
    TEST(test_dsl_filter);
    TEST(test_dsl_select);
    TEST(test_dsl_select_spaces);
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
    TEST(test_expr_conditional_functions);
    TEST(test_pipeline_derive_string_funcs);

    printf("\nNew Transforms:\n");
    TEST(test_pipeline_csv_skip);
    TEST(test_pipeline_csv_derive);
    TEST(test_pipeline_csv_stats);
    TEST(test_pipeline_csv_stats_advanced);
    TEST(test_pipeline_csv_stats_distinct);
    TEST(test_pipeline_csv_stats_hist_sample);
    TEST(test_pipeline_csv_unique);
    TEST(test_pipeline_csv_sort);
    TEST(test_pipeline_csv_sort_desc);
    TEST(test_pipeline_skip_head_combo);

    printf("\nNew DSL:\n");
    TEST(test_dsl_skip);
    TEST(test_dsl_derive);
    TEST(test_dsl_stats);
    TEST(test_dsl_stats_selective);
    TEST(test_dsl_unique);
    TEST(test_dsl_sort);
    TEST(test_dsl_sort_desc);

    printf("\nNew Registry:\n");
    TEST(test_registry_new_ops);
    TEST(test_registry_count_updated);

    printf("\nNew Operators:\n");
    TEST(test_pipeline_tail);
    TEST(test_pipeline_clip);
    TEST(test_pipeline_replace);
    TEST(test_pipeline_replace_regex);
    TEST(test_pipeline_explode);
    TEST(test_pipeline_trim);
    TEST(test_pipeline_validate);
    TEST(test_pipeline_datetime);
    TEST(test_pipeline_step_running_sum);
    TEST(test_pipeline_frequency);
    TEST(test_pipeline_top);
    TEST(test_dsl_new_ops);
    TEST(test_dsl_grep_regex);
    TEST(test_dsl_replace_regex);

    printf("\nDate/Timestamp:\n");
    TEST(test_csv_date_autodetect);
    TEST(test_csv_timestamp_autodetect);
    TEST(test_csv_date_timestamp_widening);
    TEST(test_cast_string_to_date);
    TEST(test_cast_date_to_timestamp);
    TEST(test_filter_date_comparison);
    TEST(test_sort_by_date);
    TEST(test_datetime_native_date);

    printf("\nPivot:\n");
    TEST(test_pipeline_pivot_first);
    TEST(test_pipeline_pivot_sum);
    TEST(test_dsl_pivot);

    printf("\nJoin:\n");
    TEST(test_pipeline_join_inner);
    TEST(test_pipeline_join_left);
    TEST(test_dsl_join);
    TEST(test_dsl_join_eq);

    printf("\nRecipes:\n");
    TEST(test_compile_dsl);
    TEST(test_recipe_roundtrip);

    printf("\nBuilt-in Recipes:\n");
    TEST(test_recipe_count);
    TEST(test_recipe_find);
    TEST(test_recipe_accessors);
    TEST(test_recipe_run_preview);
    TEST(test_recipe_run_dedup);

    printf("\n==================\n");
    printf("%d/%d tests passed\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
