/*
 * test_wasm_api.c — Native checks for the raw WASM export ABI.
 *
 * This links src/wasm_api.c as a normal C translation unit so signed length
 * and error-propagation mistakes are caught under ASan/UBSan without needing a
 * browser runtime.
 */

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int wasm_pipeline_create(const char *json, int len);
int wasm_pipeline_push(int handle, const uint8_t *data, int len);
int wasm_pipeline_finish(int handle);
int wasm_pipeline_pull(int handle, int channel, uint8_t *buf, int buf_len);
char *wasm_compile_dsl(const char *dsl, int len);
char *wasm_compile_to_sql(const char *dsl, int len);
const char *wasm_pipeline_error(int handle);
void wasm_pipeline_free(int handle);

static void assert_int_rejected(const char *label, int value) {
    assert(value < 0);
    assert(wasm_pipeline_error(-1) != NULL);
    printf("  %-42s PASS\n", label);
}

static void assert_ptr_rejected(const char *label, char *value) {
    assert(value == NULL);
    assert(wasm_pipeline_error(-1) != NULL);
    printf("  %-42s PASS\n", label);
}

static void test_wasm_signed_lengths_and_null_buffers(void) {
    const char *json =
        "{\"steps\":["
        "{\"op\":\"codec.csv.decode\",\"args\":{}},"
        "{\"op\":\"codec.csv.encode\",\"args\":{}}"
        "]}";
    const char *dsl = "csv | csv";

    assert_int_rejected("create negative len", wasm_pipeline_create(json, -1));
    assert_int_rejected("create null positive len", wasm_pipeline_create(NULL, 4));
    assert_ptr_rejected("compile_dsl negative len", wasm_compile_dsl(dsl, -1));
    assert_ptr_rejected("compile_dsl null positive len", wasm_compile_dsl(NULL, 4));
    assert_ptr_rejected("compile_to_sql negative len", wasm_compile_to_sql(dsl, -1));
    assert_ptr_rejected("compile_to_sql null positive len", wasm_compile_to_sql(NULL, 4));

    int h = wasm_pipeline_create(json, (int)strlen(json));
    assert(h > 0);
    const char *csv = "a\n123456\n";
    assert_int_rejected("push negative len", wasm_pipeline_push(h, (const uint8_t *)csv, -1));
    assert_int_rejected("push null positive len", wasm_pipeline_push(h, NULL, 4));
    assert(wasm_pipeline_push(h, (const uint8_t *)csv, (int)strlen(csv)) == 0);
    assert(wasm_pipeline_finish(h) == 0);

    uint8_t one[1] = {0};
    assert_int_rejected("pull negative len", wasm_pipeline_pull(h, 0, one, -1));
    assert(one[0] == 0);
    assert_int_rejected("pull null positive len", wasm_pipeline_pull(h, 0, NULL, 4));
    wasm_pipeline_free(h);
}

static void test_wasm_sql_error_propagation(void) {
    const char *bad_dsl = "csv | not-an-op | csv";
    char *json = wasm_compile_dsl(bad_dsl, (int)strlen(bad_dsl));
    assert(json == NULL);
    const char *first = wasm_pipeline_error(-1);
    assert(first != NULL);
    char first_copy[512];
    snprintf(first_copy, sizeof(first_copy), "%s", first);

    const char *bad_sql = "csv | sample 1 | csv";
    char *sql = wasm_compile_to_sql(bad_sql, (int)strlen(bad_sql));
    assert(sql == NULL);
    const char *second = wasm_pipeline_error(-1);
    assert(second != NULL);
    assert(strcmp(first_copy, second) != 0);
    assert(strstr(second, "sample") != NULL);
    assert(strstr(second, "cannot be lowered to SQL") != NULL);
    printf("  %-42s PASS\n", "compile_to_sql sets fresh error");
}

int main(void) {
    printf("Tranfi WASM ABI Tests\n");
    printf("=====================\n");
    test_wasm_signed_lengths_and_null_buffers();
    test_wasm_sql_error_propagation();
    printf("\nWASM ABI tests passed\n");
    return 0;
}
