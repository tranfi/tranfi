/*
 * test_wasm_api.c — Native checks for the raw WASM export ABI.
 *
 * This links src/wasm_api.c as a normal C translation unit so signed length
 * and error-propagation mistakes are caught under ASan/UBSan without needing a
 * browser runtime.
 */

#include <assert.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "transform.h"
#include "transform_wasm.h"

int wasm_pipeline_create(const char *json, int len);
int wasm_pipeline_push(int handle, const uint8_t *data, int len);
int wasm_pipeline_finish(int handle);
int wasm_pipeline_pull(int handle, int channel, uint8_t *buf, int buf_len);
char *wasm_compile_dsl(const char *dsl, int len);
char *wasm_compile_to_sql(const char *dsl, int len);
const char *wasm_pipeline_error(int handle);
void wasm_pipeline_free(int handle);

static uint64_t raw_double_bits(double value) {
    uint64_t bits;
    memcpy(&bits, &value, sizeof(bits));
    return bits;
}

static float raw_float_from_bits(uint32_t bits) {
    float value;
    memcpy(&value, &bits, sizeof(value));
    return value;
}

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

typedef struct raw_heap {
    uint8_t *bytes;
    uint32_t size;
    uint32_t next;
} raw_heap;

static uint32_t raw_alloc(raw_heap *heap, uint32_t size, uint32_t alignment) {
    uint32_t offset = heap->next;
    if (alignment > 1) {
        uint32_t remainder = offset % alignment;
        if (remainder) offset += alignment - remainder;
    }
    assert(offset > 0 && size <= heap->size - offset);
    heap->next = offset + size;
    return offset;
}

static uint32_t raw_alloc_odd(raw_heap *heap, uint32_t size) {
    if ((heap->next & 1u) == 0) ++heap->next;
    return raw_alloc(heap, size, 1);
}

static uint32_t raw_put(
    raw_heap *heap, const void *value, uint32_t size, uint32_t alignment) {
    uint32_t offset = raw_alloc(heap, size, alignment);
    memcpy(heap->bytes + offset, value, size);
    return offset;
}

static uint32_t raw_put_odd(raw_heap *heap, const void *value, uint32_t size) {
    uint32_t offset = raw_alloc_odd(heap, size);
    memcpy(heap->bytes + offset, value, size);
    return offset;
}

static uint32_t raw_word(const raw_heap *heap, uint32_t offset) {
    uint32_t value = 0;
    memcpy(&value, heap->bytes + offset, sizeof(value));
    return value;
}

static uint32_t raw_schema_x0_dtype(raw_heap *heap, uint32_t dtype) {
    static const char name[] = "x0";
    tf_wasm_field_view_v1 field = {0};
    tf_wasm_schema_view_v1 schema = {0};
    uint32_t id_offset = raw_put(heap, name, 2, 1);
    uint32_t display_offset = raw_put(heap, name, 2, 1);
    uint32_t field_offset;
    field.abi_version = 1;
    field.struct_size = sizeof(field);
    field.dtype = dtype;
    field.id_offset = id_offset;
    field.id_bytes = 2;
    field.name_offset = display_offset;
    field.name_bytes = 2;
    field_offset = raw_put_odd(heap, &field, sizeof(field));
    schema.abi_version = 1;
    schema.struct_size = sizeof(schema);
    schema.column_count = 1;
    schema.fields_offset = field_offset;
    schema.fields_bytes = sizeof(field);
    return raw_put_odd(heap, &schema, sizeof(schema));
}

static uint32_t raw_schema_x0(raw_heap *heap) {
    return raw_schema_x0_dtype(heap, TF_VIEW_FLOAT64);
}

static uint32_t raw_table_x0(
    raw_heap *heap, const double *values, uint32_t rows, int misalign_data) {
    tf_wasm_column_view_v1 column = {0};
    tf_wasm_table_view_v1 table = {0};
    uint32_t data_offset = raw_put(
        heap, values, rows * (uint32_t)sizeof(*values), 8);
    uint32_t column_offset;
    if (misalign_data) ++data_offset;
    column.abi_version = 1;
    column.struct_size = sizeof(column);
    column.data_offset = data_offset;
    column.data_bytes = rows * (uint32_t)sizeof(*values);
    column.stride_bytes = sizeof(*values);
    column_offset = raw_put_odd(heap, &column, sizeof(column));
    table.abi_version = 1;
    table.struct_size = sizeof(table);
    table.row_count = rows;
    table.column_count = 1;
    table.columns_offset = column_offset;
    table.columns_bytes = sizeof(column);
    return raw_put_odd(heap, &table, sizeof(table));
}

static uint32_t raw_table_x0_f32(
    raw_heap *heap, const float *values, uint32_t rows) {
    tf_wasm_column_view_v1 column = {0};
    tf_wasm_table_view_v1 table = {0};
    uint32_t data_offset = raw_put(
        heap, values, rows * (uint32_t)sizeof(*values), 4);
    uint32_t column_offset;
    column.abi_version = 1;
    column.struct_size = sizeof(column);
    column.data_offset = data_offset;
    column.data_bytes = rows * (uint32_t)sizeof(*values);
    column.stride_bytes = sizeof(*values);
    column_offset = raw_put_odd(heap, &column, sizeof(column));
    table.abi_version = 1;
    table.struct_size = sizeof(table);
    table.row_count = rows;
    table.column_count = 1;
    table.columns_offset = column_offset;
    table.columns_bytes = sizeof(column);
    return raw_put_odd(heap, &table, sizeof(table));
}

static uint32_t raw_create_recipe(
    raw_heap *heap, uint32_t recipe_offset, uint32_t recipe_len,
    uint32_t limits_offset) {
    uint32_t output = raw_alloc_odd(heap, 4);
    uint32_t error = raw_alloc_odd(heap, 4);
    assert(tf_wasm_transform_recipe_from_json(
        recipe_offset, recipe_len, limits_offset, output, error)
        == TF_TRANSFORM_OK);
    assert(raw_word(heap, error) == 0);
    assert((raw_word(heap, output) >> 28) == TF_WASM_HANDLE_RECIPE);
    return raw_word(heap, output);
}

static void test_prepared_transform_raw_wasm_abi(void) {
    static const char recipe_json[] =
        "{\"columns\":[{\"categorical\":null,\"kind\":{\"maxCategories\":null,"
        "\"op\":\"declared\",\"rule\":null,\"value\":\"numeric\"},\"numeric\":{"
        "\"impute\":{\"allMissing\":\"zero\",\"constant\":null,\"op\":\"mean\"},"
        "\"normalize\":{\"ddof\":0,\"op\":\"standard\"}},\"sourceId\":\"x0\"}],"
        "\"format\":\"tranfi.transform-recipe\",\"outputDtype\":\"float64\","
        "\"policyVersion\":1,\"semanticLimits\":{\"maxOutputColumns\":65536,"
        "\"maxOutputElementsPerApply\":134217728},\"version\":1}";
    raw_heap heap = {0};
    tf_transform_limits_v1 safe;
    double values[] = {1.0, 2.0, 3.0};
    uint32_t limits_offset;
    uint32_t recipe_offset;
    uint32_t schema_offset;
    uint32_t table_offset;
    uint32_t recipe;
    uint32_t analyzer;
    uint32_t plan;
    uint32_t apply;
    uint32_t bytes_handle;
    uint32_t bytes_len;
    uint32_t bytes_copy;

    heap.size = 4u * 1024u * 1024u;
    heap.bytes = (uint8_t *)calloc(1, heap.size);
    assert(heap.bytes != NULL);
    heap.next = 1;
    tf_wasm_transform_test_set_heap(heap.bytes, heap.size);

    limits_offset = raw_alloc_odd(&heap, sizeof(safe));
    memset(heap.bytes + limits_offset, 0x5a, sizeof(safe));
    assert(tf_wasm_transform_limits_init_safe_v1(
        limits_offset, sizeof(safe) - 1) == TF_TRANSFORM_INVALID_ARGUMENT);
    assert(heap.bytes[limits_offset] == 0x5a);
    assert(tf_wasm_transform_limits_init_safe_v1(
        limits_offset, sizeof(safe)) == TF_TRANSFORM_OK);
    memcpy(&safe, heap.bytes + limits_offset, sizeof(safe));
    assert(safe.abi_version == 1 && safe.struct_size == sizeof(safe));
    assert(safe.max_live_handles == 65535);

    recipe_offset = raw_put(
        &heap, recipe_json, (uint32_t)strlen(recipe_json), 1);
    schema_offset = raw_schema_x0(&heap);
    table_offset = raw_table_x0(&heap, values, 3, 0);

    {
        uint32_t overlap = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_recipe_from_json(
            recipe_offset, (uint32_t)strlen(recipe_json), limits_offset,
            overlap, overlap) == TF_TRANSFORM_INVALID_ARGUMENT);
    }
    recipe = raw_create_recipe(
        &heap, recipe_offset, (uint32_t)strlen(recipe_json), limits_offset);

    {
        uint32_t token_out = raw_alloc_odd(&heap, 4);
        uint32_t error_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_cancel_create(
            TF_WASM_TRANSFORM_NO_HOST_CANCEL, limits_offset,
            token_out, error_out) == TF_TRANSFORM_OK);
        uint32_t token = raw_word(&heap, token_out);
        assert((token >> 28) == TF_WASM_HANDLE_CANCEL_TOKEN);

        uint32_t analyzer_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_analyzer_create(
            recipe, schema_offset, limits_offset, token,
            analyzer_out, error_out) == TF_TRANSFORM_OK);
        analyzer = raw_word(&heap, analyzer_out);
        assert((analyzer >> 28) == TF_WASM_HANDLE_ANALYZER);
        assert(tf_wasm_transform_handle_destroy(token) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_handle_destroy(token)
               == TF_TRANSFORM_INVALID_ARGUMENT);
    }

    assert(tf_wasm_transform_handle_destroy(recipe) == TF_TRANSFORM_OK);
    assert(tf_wasm_transform_handle_destroy(recipe)
           == TF_TRANSFORM_INVALID_ARGUMENT);
    {
        uint32_t error_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_analyzer_push(
            analyzer, table_offset, error_out) == TF_TRANSFORM_OK);
        assert(raw_word(&heap, error_out) == 0);
        uint32_t plan_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_analyzer_finalize(
            analyzer, plan_out, error_out) == TF_TRANSFORM_OK);
        plan = raw_word(&heap, plan_out);
        assert((plan >> 28) == TF_WASM_HANDLE_PLAN);
        assert(tf_wasm_transform_handle_destroy(analyzer) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_analyzer_push(
            plan, table_offset, error_out) == TF_TRANSFORM_INVALID_ARGUMENT);
    }

    {
        uint32_t bytes_out = raw_alloc_odd(&heap, 4);
        uint32_t error_out = raw_alloc_odd(&heap, 4);
        uint32_t size_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_plan_export(
            plan, limits_offset, bytes_out, error_out) == TF_TRANSFORM_OK);
        bytes_handle = raw_word(&heap, bytes_out);
        assert((bytes_handle >> 28) == TF_WASM_HANDLE_OWNED_BYTES);
        assert(tf_wasm_transform_owned_bytes_size(
            bytes_handle, size_out) == TF_TRANSFORM_OK);
        bytes_len = raw_word(&heap, size_out);
        assert(bytes_len > 52);
        bytes_copy = raw_alloc(&heap, bytes_len, 1);
        memset(heap.bytes + bytes_copy, 0xa5, bytes_len);
        assert(tf_wasm_transform_owned_bytes_copy(
            bytes_handle, bytes_copy, bytes_len - 1)
            == TF_TRANSFORM_RESOURCE_LIMIT);
        assert(heap.bytes[bytes_copy] == 0xa5);
        assert(tf_wasm_transform_owned_bytes_copy(
            bytes_handle, bytes_copy, bytes_len) == TF_TRANSFORM_OK);
    }

    {
        uint32_t count_out = raw_alloc_odd(&heap, 4);
        uint32_t error_out = raw_alloc_odd(&heap, 4);
        uint32_t info_out = raw_alloc_odd(
            &heap, sizeof(tf_wasm_schema_field_info_v1));
        tf_wasm_schema_field_info_v1 info;
        assert(tf_wasm_transform_plan_schema_field_count(
            plan, TF_TRANSFORM_SCHEMA_OUTPUT, count_out, error_out)
            == TF_TRANSFORM_OK);
        assert(raw_word(&heap, count_out) == 1);
        assert(tf_wasm_transform_plan_schema_field_info(
            plan, TF_TRANSFORM_SCHEMA_OUTPUT, 0, info_out, error_out)
            == TF_TRANSFORM_OK);
        memcpy(&info, heap.bytes + info_out, sizeof(info));
        assert(info.dtype == TF_VIEW_FLOAT64);
        assert(info.id_bytes == 2 && info.name_bytes == 2);
        uint32_t id_out = raw_alloc(&heap, 2, 1);
        uint32_t name_out = raw_alloc(&heap, 2, 1);
        heap.bytes[id_out] = 0x7b;
        assert(tf_wasm_transform_plan_schema_field_copy(
            plan, TF_TRANSFORM_SCHEMA_OUTPUT, 0,
            id_out, 1, name_out, 2, error_out)
            == TF_TRANSFORM_RESOURCE_LIMIT);
        assert(heap.bytes[id_out] == 0x7b);
        assert(tf_wasm_transform_plan_schema_field_copy(
            plan, TF_TRANSFORM_SCHEMA_OUTPUT, 0,
            id_out, 2, name_out, 2, error_out) == TF_TRANSFORM_OK);
        assert(memcmp(heap.bytes + id_out, "x0", 2) == 0);
    }

    {
        uint32_t apply_out = raw_alloc_odd(&heap, 4);
        uint32_t error_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_apply_create(
            plan, schema_offset, limits_offset, 0,
            apply_out, error_out) == TF_TRANSFORM_OK);
        apply = raw_word(&heap, apply_out);
        assert((apply >> 28) == TF_WASM_HANDLE_APPLY);
        uint32_t bad_table = raw_table_x0(&heap, values, 3, 1);
        uint32_t dense_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_apply_run(
            apply, bad_table, dense_out, error_out)
            == TF_TRANSFORM_INVALID_ARGUMENT);
        assert(raw_word(&heap, dense_out) == 0);
        assert(tf_wasm_transform_handle_destroy(plan) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_apply_run(
            apply, table_offset, dense_out, error_out) == TF_TRANSFORM_OK);
        uint32_t dense = raw_word(&heap, dense_out);
        uint32_t info_out = raw_alloc_odd(
            &heap, sizeof(tf_wasm_dense_info_v1));
        tf_wasm_dense_info_v1 info;
        assert(tf_wasm_transform_owned_dense_info(dense, info_out)
               == TF_TRANSFORM_OK);
        memcpy(&info, heap.bytes + info_out, sizeof(info));
        assert(info.rows == 3 && info.columns == 1);
        assert(info.dtype == TF_VIEW_FLOAT64 && info.data_bytes == 24);
        uint32_t data_out = raw_alloc(&heap, info.data_bytes, 1);
        assert(tf_wasm_transform_owned_dense_copy(
            dense, data_out, info.data_bytes) == TF_TRANSFORM_OK);
        double actual[3];
        memcpy(actual, heap.bytes + data_out, sizeof(actual));
        assert(fabs(actual[0] + 1.224744871391589) < 1e-15);
        assert(actual[1] == 0.0);
        assert(fabs(actual[2] - 1.224744871391589) < 1e-15);
        assert(tf_wasm_transform_handle_destroy(dense) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_handle_destroy(dense)
               == TF_TRANSFORM_INVALID_ARGUMENT);
        assert(tf_wasm_transform_handle_destroy(apply) == TF_TRANSFORM_OK);
    }

    {
        uint32_t plan_out = raw_alloc_odd(&heap, 4);
        uint32_t error_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_plan_import(
            bytes_copy, bytes_len, limits_offset, 0,
            plan_out, error_out) == TF_TRANSFORM_OK);
        uint32_t imported_plan = raw_word(&heap, plan_out);
        assert((imported_plan >> 28) == TF_WASM_HANDLE_PLAN);

        uint32_t token_out = raw_alloc_odd(&heap, 4);
        uint32_t apply_out = raw_alloc_odd(&heap, 4);
        uint32_t dense_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_cancel_create(
            TF_WASM_TRANSFORM_NO_HOST_CANCEL, limits_offset,
            token_out, error_out) == TF_TRANSFORM_OK);
        uint32_t token = raw_word(&heap, token_out);
        assert(tf_wasm_transform_apply_create(
            imported_plan, schema_offset, limits_offset, token,
            apply_out, error_out) == TF_TRANSFORM_OK);
        uint32_t cancel_apply = raw_word(&heap, apply_out);
        assert(tf_wasm_transform_apply_run(
            cancel_apply, table_offset, dense_out, error_out)
            == TF_TRANSFORM_OK);
        uint32_t cancel_dense = raw_word(&heap, dense_out);
        assert(tf_wasm_transform_cancel_request(token) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_handle_destroy(token) == TF_TRANSFORM_OK);
        uint32_t cancelled_copy = raw_alloc(&heap, sizeof(values), 1);
        assert(tf_wasm_transform_owned_dense_copy(
            cancel_dense, cancelled_copy, sizeof(values))
            == TF_TRANSFORM_CANCELLED);
        assert(tf_wasm_transform_handle_destroy(cancel_dense)
               == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_handle_destroy(cancel_apply)
               == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_handle_destroy(imported_plan)
               == TF_TRANSFORM_OK);

        uint32_t corrupt = raw_alloc(&heap, bytes_len, 1);
        memcpy(heap.bytes + corrupt, heap.bytes + bytes_copy, bytes_len);
        heap.bytes[corrupt] ^= 1;
        assert(tf_wasm_transform_plan_import(
            corrupt, bytes_len, limits_offset, 0,
            plan_out, error_out) == TF_TRANSFORM_CORRUPT_PLAN);
        assert(raw_word(&heap, plan_out) == 0);
        uint32_t error_handle = raw_word(&heap, error_out);
        assert((error_handle >> 28) == TF_WASM_HANDLE_ERROR);
        uint32_t code_out = raw_alloc_odd(&heap, 4);
        uint32_t message_size_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_error_code(error_handle, code_out)
               == TF_TRANSFORM_OK);
        assert(raw_word(&heap, code_out) == TF_TRANSFORM_CORRUPT_PLAN);
        assert(tf_wasm_transform_error_message_size(
            error_handle, message_size_out) == TF_TRANSFORM_OK);
        uint32_t message_len = raw_word(&heap, message_size_out);
        assert(message_len > 0);
        uint32_t message_out = raw_alloc(&heap, message_len, 1);
        assert(tf_wasm_transform_error_message_copy(
            error_handle, message_out, message_len) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_handle_destroy(error_handle)
               == TF_TRANSFORM_OK);
    }
    assert(tf_wasm_transform_handle_destroy(bytes_handle) == TF_TRANSFORM_OK);

    {
        uint32_t cancel_recipe = raw_create_recipe(
            &heap, recipe_offset, (uint32_t)strlen(recipe_json), limits_offset);
        uint32_t token_out = raw_alloc_odd(&heap, 4);
        uint32_t error_out = raw_alloc_odd(&heap, 4);
        uint32_t analyzer_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_cancel_create(
            TF_WASM_TRANSFORM_NO_HOST_CANCEL, limits_offset,
            token_out, error_out) == TF_TRANSFORM_OK);
        uint32_t token = raw_word(&heap, token_out);
        assert(tf_wasm_transform_analyzer_create(
            cancel_recipe, schema_offset, limits_offset, token,
            analyzer_out, error_out) == TF_TRANSFORM_OK);
        uint32_t cancel_analyzer = raw_word(&heap, analyzer_out);
        assert(tf_wasm_transform_cancel_request(token) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_handle_destroy(token) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_analyzer_push(
            cancel_analyzer, table_offset, error_out)
            == TF_TRANSFORM_CANCELLED);
        uint32_t error_handle = raw_word(&heap, error_out);
        assert((error_handle >> 28) == TF_WASM_HANDLE_ERROR);
        assert(tf_wasm_transform_handle_destroy(error_handle)
               == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_handle_destroy(cancel_analyzer)
               == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_handle_destroy(cancel_recipe)
               == TF_TRANSFORM_OK);
    }

    {
        tf_transform_limits_v1 two_live = safe;
        two_live.max_live_handles = 2;
        uint32_t two_limits = raw_put_odd(
            &heap, &two_live, sizeof(two_live));
        uint32_t limited_recipe = raw_create_recipe(
            &heap, recipe_offset, (uint32_t)strlen(recipe_json), two_limits);
        uint32_t analyzer_out = raw_alloc_odd(&heap, 4);
        uint32_t plan_out = raw_alloc_odd(&heap, 4);
        uint32_t error_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_analyzer_create(
            limited_recipe, schema_offset, two_limits, 0,
            analyzer_out, error_out) == TF_TRANSFORM_OK);
        uint32_t limited_analyzer = raw_word(&heap, analyzer_out);
        assert(tf_wasm_transform_analyzer_push(
            limited_analyzer, table_offset, error_out) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_analyzer_finalize(
            limited_analyzer, plan_out, error_out)
            == TF_TRANSFORM_RESOURCE_LIMIT);
        assert(raw_word(&heap, plan_out) == 0);
        assert(tf_wasm_transform_handle_destroy(limited_recipe)
               == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_analyzer_finalize(
            limited_analyzer, plan_out, error_out) == TF_TRANSFORM_OK);
        uint32_t limited_plan = raw_word(&heap, plan_out);
        assert(tf_wasm_transform_handle_destroy(limited_analyzer)
               == TF_TRANSFORM_OK);

        uint32_t apply_out = raw_alloc_odd(&heap, 4);
        uint32_t dense_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_apply_create(
            limited_plan, schema_offset, two_limits, 0,
            apply_out, error_out) == TF_TRANSFORM_OK);
        uint32_t limited_apply = raw_word(&heap, apply_out);
        assert(tf_wasm_transform_apply_run(
            limited_apply, table_offset, dense_out, error_out)
            == TF_TRANSFORM_RESOURCE_LIMIT);
        assert(raw_word(&heap, dense_out) == 0);
        assert(tf_wasm_transform_handle_destroy(limited_plan)
               == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_apply_run(
            limited_apply, table_offset, dense_out, error_out)
            == TF_TRANSFORM_OK);
        uint32_t limited_dense = raw_word(&heap, dense_out);
        assert(tf_wasm_transform_handle_destroy(limited_dense)
               == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_handle_destroy(limited_apply)
               == TF_TRANSFORM_OK);
    }

    {
        tf_transform_limits_v1 one_live = safe;
        one_live.max_live_handles = 1;
        uint32_t one_limits = raw_put_odd(
            &heap, &one_live, sizeof(one_live));
        uint32_t token_output = raw_alloc_odd(&heap, 4);
        uint32_t token_error = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_cancel_create(
            TF_WASM_TRANSFORM_NO_HOST_CANCEL, one_limits,
            token_output, token_error) == TF_TRANSFORM_OK);
        uint32_t token = raw_word(&heap, token_output);
        uint32_t output = raw_alloc_odd(&heap, 4);
        uint32_t error = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_recipe_from_json(
            recipe_offset, (uint32_t)strlen(recipe_json), one_limits,
            output, error) == TF_TRANSFORM_RESOURCE_LIMIT);
        assert(raw_word(&heap, output) == 0);
        assert(tf_wasm_transform_handle_destroy(token) == TF_TRANSFORM_OK);
    }

    {
        tf_transform_limits_v1 three_live = safe;
        three_live.max_live_handles = 3;
        uint32_t three_limits = raw_put_odd(
            &heap, &three_live, sizeof(three_live));
        uint32_t limited_recipe = raw_create_recipe(
            &heap, recipe_offset, (uint32_t)strlen(recipe_json), three_limits);
        uint32_t token_out = raw_alloc_odd(&heap, 4);
        uint32_t analyzer_out = raw_alloc_odd(&heap, 4);
        uint32_t plan_out = raw_alloc_odd(&heap, 4);
        uint32_t error_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_cancel_create(
            TF_WASM_TRANSFORM_NO_HOST_CANCEL, three_limits,
            token_out, error_out) == TF_TRANSFORM_OK);
        uint32_t token = raw_word(&heap, token_out);
        assert(tf_wasm_transform_analyzer_create(
            limited_recipe, schema_offset, three_limits, token,
            analyzer_out, error_out) == TF_TRANSFORM_OK);
        uint32_t limited_analyzer = raw_word(&heap, analyzer_out);
        assert(tf_wasm_transform_analyzer_push(
            limited_analyzer, table_offset, error_out) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_cancel_request(token) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_analyzer_finalize(
            limited_analyzer, plan_out, error_out) == TF_TRANSFORM_CANCELLED);
        assert(raw_word(&heap, plan_out) == 0);
        assert(tf_wasm_transform_analyzer_finalize(
            limited_analyzer, plan_out, error_out) == TF_TRANSFORM_INVALID_STATE);
        assert(tf_wasm_transform_handle_destroy(limited_analyzer)
               == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_handle_destroy(token) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_handle_destroy(limited_recipe)
               == TF_TRANSFORM_OK);
    }

    {
        tf_transform_limits_v1 three_live = safe;
        three_live.max_live_handles = 3;
        uint32_t three_limits = raw_put_odd(
            &heap, &three_live, sizeof(three_live));
        uint32_t limited_recipe = raw_create_recipe(
            &heap, recipe_offset, (uint32_t)strlen(recipe_json), three_limits);
        uint32_t analyzer_out = raw_alloc_odd(&heap, 4);
        uint32_t plan_out = raw_alloc_odd(&heap, 4);
        uint32_t error_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_analyzer_create(
            limited_recipe, schema_offset, three_limits, 0,
            analyzer_out, error_out) == TF_TRANSFORM_OK);
        uint32_t limited_analyzer = raw_word(&heap, analyzer_out);
        assert(tf_wasm_transform_analyzer_push(
            limited_analyzer, table_offset, error_out) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_analyzer_finalize(
            limited_analyzer, plan_out, error_out) == TF_TRANSFORM_OK);
        uint32_t limited_plan = raw_word(&heap, plan_out);
        assert(tf_wasm_transform_handle_destroy(limited_analyzer)
               == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_handle_destroy(limited_recipe)
               == TF_TRANSFORM_OK);

        uint32_t token_out = raw_alloc_odd(&heap, 4);
        uint32_t apply_out = raw_alloc_odd(&heap, 4);
        uint32_t dense_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_cancel_create(
            TF_WASM_TRANSFORM_NO_HOST_CANCEL, three_limits,
            token_out, error_out) == TF_TRANSFORM_OK);
        uint32_t token = raw_word(&heap, token_out);
        assert(tf_wasm_transform_apply_create(
            limited_plan, schema_offset, three_limits, token,
            apply_out, error_out) == TF_TRANSFORM_OK);
        uint32_t limited_apply = raw_word(&heap, apply_out);
        assert(tf_wasm_transform_cancel_request(token) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_apply_run(
            limited_apply, table_offset, dense_out, error_out)
            == TF_TRANSFORM_CANCELLED);
        assert(raw_word(&heap, dense_out) == 0);
        assert(tf_wasm_transform_apply_run(
            limited_apply, table_offset, dense_out, error_out)
            == TF_TRANSFORM_INVALID_STATE);
        assert(tf_wasm_transform_handle_destroy(limited_apply)
               == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_handle_destroy(token) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_handle_destroy(limited_plan)
               == TF_TRANSFORM_OK);
    }

    {
        uint32_t token_out = raw_alloc_odd(&heap, 4);
        uint32_t error_out = raw_alloc_odd(&heap, 4);
        uint32_t first_handle = 0;
        uint16_t first_slot = 0;
        uint32_t token = 0;
        size_t churn = 0;
        for (;;) {
            assert(tf_wasm_transform_cancel_create(
                TF_WASM_TRANSFORM_NO_HOST_CANCEL, limits_offset,
                token_out, error_out) == TF_TRANSFORM_OK);
            token = raw_word(&heap, token_out);
            if (first_handle == 0) {
                first_handle = token;
                first_slot = (uint16_t)(token & 0xffffu);
            } else if ((uint16_t)(token & 0xffffu) != first_slot) {
                break;
            }
            assert(tf_wasm_transform_handle_destroy(token) == TF_TRANSFORM_OK);
            ++churn;
            assert(churn <= 4096);
        }
        assert(churn > 0 && churn <= 4096);
        assert(tf_wasm_transform_handle_destroy(first_handle)
               == TF_TRANSFORM_INVALID_ARGUMENT);
        assert(tf_wasm_transform_handle_destroy(token) == TF_TRANSFORM_OK);
    }

    free(heap.bytes);
    tf_wasm_transform_test_set_heap(NULL, 0);
    printf("  %-42s PASS\n", "prepared-transform typed wasm32 ABI");
}

static void test_prepared_transform_raw_wasm_onehot(void) {
    static const char recipe_json[] =
        "{\"columns\":[{\"categorical\":{\"encode\":{\"categories\":"
        "\"discover\",\"op\":\"onehot\",\"sentinelLabel\":null,"
        "\"unknown\":\"other\"},\"impute\":{\"allMissing\":\"error\","
        "\"constant\":null,\"op\":\"mode\"}},\"kind\":{"
        "\"maxCategories\":null,\"op\":\"declared\",\"rule\":null,"
        "\"value\":\"categorical\"},\"numeric\":null,\"sourceId\":\"x0\"}],"
        "\"format\":\"tranfi.transform-recipe\",\"outputDtype\":\"float64\","
        "\"policyVersion\":1,\"semanticLimits\":{\"maxOutputColumns\":64,"
        "\"maxOutputElementsPerApply\":1024},\"version\":1}";
    const float analyze_values[] = {
        -0x1p-149f, -0.0f, 0.0f, 0x1p-149f, 0x1p-149f
    };
    const float apply_values[] = {
        -0x1p-149f, -0.0f, 0x1p-149f, 1.0f,
        /* Canonical quiet NaN is a missing input. */
        0.0f
    };
    const uint64_t expected[] = {
        UINT64_C(0x3ff0000000000000), 0, 0, 0,
        0, UINT64_C(0x3ff0000000000000), 0, 0,
        0, 0, UINT64_C(0x3ff0000000000000), 0,
        0, 0, 0, UINT64_C(0x3ff0000000000000),
        0, UINT64_C(0x3ff0000000000000), 0, 0
    };
    raw_heap heap = {0};
    tf_transform_limits_v1 safe;
    uint32_t limits_offset;
    uint32_t recipe_offset;
    uint32_t schema_offset;
    uint32_t analyze_table;
    uint32_t apply_table;
    uint32_t error_out;
    uint32_t recipe;
    uint32_t analyzer;
    uint32_t plan;
    uint32_t apply;
    uint32_t dense;

    heap.size = 1024u * 1024u;
    heap.bytes = (uint8_t *)calloc(1, heap.size);
    assert(heap.bytes != NULL);
    heap.next = 1;
    tf_wasm_transform_test_set_heap(heap.bytes, heap.size);
    limits_offset = raw_alloc_odd(&heap, sizeof(safe));
    assert(tf_wasm_transform_limits_init_safe_v1(
        limits_offset, sizeof(safe)) == TF_TRANSFORM_OK);
    recipe_offset = raw_put(
        &heap, recipe_json, (uint32_t)strlen(recipe_json), 1);
    {
        float values[5];
        memcpy(values, apply_values, sizeof(values));
        values[4] = raw_float_from_bits(UINT32_C(0x7fc00000));
        schema_offset = raw_schema_x0_dtype(&heap, TF_VIEW_FLOAT32);
        analyze_table = raw_table_x0_f32(&heap, analyze_values, 5);
        apply_table = raw_table_x0_f32(&heap, values, 5);
    }
    error_out = raw_alloc_odd(&heap, 4);
    recipe = raw_create_recipe(
        &heap, recipe_offset, (uint32_t)strlen(recipe_json), limits_offset);
    {
        uint32_t analyzer_out = raw_alloc_odd(&heap, 4);
        uint32_t plan_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_analyzer_create(
            recipe, schema_offset, limits_offset, 0,
            analyzer_out, error_out) == TF_TRANSFORM_OK);
        analyzer = raw_word(&heap, analyzer_out);
        assert(tf_wasm_transform_analyzer_push(
            analyzer, analyze_table, error_out) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_analyzer_finalize(
            analyzer, plan_out, error_out) == TF_TRANSFORM_OK);
        plan = raw_word(&heap, plan_out);
    }
    {
        static const char expected_schema[] =
            "[{\"category\":{\"t\":\"f32\",\"v\":\"80000001\"},"
            "\"dtype\":\"float64\",\"id\":\"x0%3Aonehot%3A0\","
            "\"name\":\"x0%3Aonehot%3A0\",\"role\":\"onehot\","
            "\"sourceId\":\"x0\"},{\"category\":{\"t\":\"f32\","
            "\"v\":\"00000000\"},\"dtype\":\"float64\","
            "\"id\":\"x0%3Aonehot%3A1\",\"name\":\"x0%3Aonehot%3A1\","
            "\"role\":\"onehot\",\"sourceId\":\"x0\"},{\"category\":{"
            "\"t\":\"f32\",\"v\":\"00000001\"},\"dtype\":\"float64\","
            "\"id\":\"x0%3Aonehot%3A2\",\"name\":\"x0%3Aonehot%3A2\","
            "\"role\":\"onehot\",\"sourceId\":\"x0\"},{\"category\":{"
            "\"t\":\"other\"},\"dtype\":\"float64\","
            "\"id\":\"x0%3Aonehot%3A3\",\"name\":\"x0%3Aonehot%3A3\","
            "\"role\":\"onehot\",\"sourceId\":\"x0\"}]";
        uint32_t count_out = raw_alloc_odd(&heap, 4);
        uint32_t apply_out = raw_alloc_odd(&heap, 4);
        uint32_t dense_out = raw_alloc_odd(&heap, 4);
        uint32_t info_out = raw_alloc_odd(
            &heap, sizeof(tf_wasm_dense_info_v1));
        tf_wasm_dense_info_v1 info;
        assert(tf_wasm_transform_plan_schema_field_count(
            plan, TF_TRANSFORM_SCHEMA_OUTPUT, count_out, error_out)
            == TF_TRANSFORM_OK);
        assert(raw_word(&heap, count_out) == 4);
        {
            uint32_t bytes_out = raw_alloc_odd(&heap, 4);
            uint32_t size_out = raw_alloc_odd(&heap, 4);
            uint32_t schema_bytes;
            uint32_t schema_len;
            uint32_t destination;
            assert(tf_wasm_transform_plan_schema_json(
                plan, TF_TRANSFORM_SCHEMA_OUTPUT, limits_offset,
                bytes_out, error_out) == TF_TRANSFORM_OK);
            schema_bytes = raw_word(&heap, bytes_out);
            assert(tf_wasm_transform_owned_bytes_size(
                schema_bytes, size_out) == TF_TRANSFORM_OK);
            schema_len = raw_word(&heap, size_out);
            assert(schema_len == strlen(expected_schema));
            destination = raw_alloc(&heap, schema_len, 1);
            assert(tf_wasm_transform_owned_bytes_copy(
                schema_bytes, destination, schema_len) == TF_TRANSFORM_OK);
            assert(memcmp(
                heap.bytes + destination, expected_schema, schema_len) == 0);
            assert(tf_wasm_transform_handle_destroy(schema_bytes)
                   == TF_TRANSFORM_OK);
        }
        assert(tf_wasm_transform_apply_create(
            plan, schema_offset, limits_offset, 0,
            apply_out, error_out) == TF_TRANSFORM_OK);
        apply = raw_word(&heap, apply_out);
        assert(tf_wasm_transform_apply_run(
            apply, apply_table, dense_out, error_out) == TF_TRANSFORM_OK);
        dense = raw_word(&heap, dense_out);
        assert(tf_wasm_transform_owned_dense_info(dense, info_out)
               == TF_TRANSFORM_OK);
        memcpy(&info, heap.bytes + info_out, sizeof(info));
        assert(info.rows == 5 && info.columns == 4
               && info.data_bytes == 20 * sizeof(double));
        {
            uint32_t data_out = raw_alloc(&heap, info.data_bytes, 8);
            double actual[20];
            assert(tf_wasm_transform_owned_dense_copy(
                dense, data_out, info.data_bytes) == TF_TRANSFORM_OK);
            memcpy(actual, heap.bytes + data_out, sizeof(actual));
            for (size_t i = 0; i < 20; ++i)
                assert(raw_double_bits(actual[i]) == expected[i]);
        }
    }
    assert(tf_wasm_transform_handle_destroy(dense) == TF_TRANSFORM_OK);
    assert(tf_wasm_transform_handle_destroy(apply) == TF_TRANSFORM_OK);
    assert(tf_wasm_transform_handle_destroy(plan) == TF_TRANSFORM_OK);
    assert(tf_wasm_transform_handle_destroy(analyzer) == TF_TRANSFORM_OK);
    assert(tf_wasm_transform_handle_destroy(recipe) == TF_TRANSFORM_OK);
    free(heap.bytes);
    tf_wasm_transform_test_set_heap(NULL, 0);
    printf("  %-42s PASS\n", "prepared-transform raw one-hot parity");
}

static void test_prepared_transform_raw_wasm_inference(void) {
    static const char recipe_json[] =
        "{\"columns\":[{\"categorical\":{\"encode\":{\"categories\":"
        "\"discover\",\"op\":\"label\",\"sentinelLabel\":null,"
        "\"unknown\":\"error\"},\"impute\":{\"allMissing\":\"error\","
        "\"constant\":null,\"op\":\"mode\"}},\"kind\":{\"maxCategories\":2,"
        "\"op\":\"infer\",\"rule\":\"finite-integer-cardinality-v1\","
        "\"value\":null},\"numeric\":{\"impute\":{\"allMissing\":\"zero\","
        "\"constant\":null,\"op\":\"mean\"},\"normalize\":{\"ddof\":null,"
        "\"op\":\"none\"}},\"sourceId\":\"x0\"}],\"format\":"
        "\"tranfi.transform-recipe\",\"outputDtype\":\"float64\","
        "\"policyVersion\":1,\"semanticLimits\":{\"maxOutputColumns\":64,"
        "\"maxOutputElementsPerApply\":1024},\"version\":1}";
    const double analyze_values[] = {0.0, 1.0, 2.0};
    const double apply_values[] = {NAN, 3.0};
    const uint64_t expected[] = {
        UINT64_C(0x3ff0000000000000), UINT64_C(0x4008000000000000)
    };
    raw_heap heap = {0};
    tf_transform_limits_v1 safe;
    uint32_t limits_offset;
    uint32_t recipe_offset;
    uint32_t schema_offset;
    uint32_t analyze_table;
    uint32_t apply_table;
    uint32_t error_out;
    uint32_t recipe;
    uint32_t analyzer;
    uint32_t plan;
    uint32_t apply;
    uint32_t dense;

    heap.size = 1024u * 1024u;
    heap.bytes = (uint8_t *)calloc(1, heap.size);
    assert(heap.bytes != NULL);
    heap.next = 1;
    tf_wasm_transform_test_set_heap(heap.bytes, heap.size);
    limits_offset = raw_alloc_odd(&heap, sizeof(safe));
    assert(tf_wasm_transform_limits_init_safe_v1(
        limits_offset, sizeof(safe)) == TF_TRANSFORM_OK);
    recipe_offset = raw_put(
        &heap, recipe_json, (uint32_t)strlen(recipe_json), 1);
    schema_offset = raw_schema_x0(&heap);
    analyze_table = raw_table_x0(&heap, analyze_values, 3, 0);
    apply_table = raw_table_x0(&heap, apply_values, 2, 0);
    error_out = raw_alloc_odd(&heap, 4);
    recipe = raw_create_recipe(
        &heap, recipe_offset, (uint32_t)strlen(recipe_json), limits_offset);
    {
        uint32_t analyzer_out = raw_alloc_odd(&heap, 4);
        uint32_t plan_out = raw_alloc_odd(&heap, 4);
        assert(tf_wasm_transform_analyzer_create(
            recipe, schema_offset, limits_offset, 0,
            analyzer_out, error_out) == TF_TRANSFORM_OK);
        analyzer = raw_word(&heap, analyzer_out);
        assert(tf_wasm_transform_analyzer_push(
            analyzer, analyze_table, error_out) == TF_TRANSFORM_OK);
        assert(tf_wasm_transform_analyzer_finalize(
            analyzer, plan_out, error_out) == TF_TRANSFORM_OK);
        plan = raw_word(&heap, plan_out);
    }
    {
        uint32_t apply_out = raw_alloc_odd(&heap, 4);
        uint32_t dense_out = raw_alloc_odd(&heap, 4);
        uint32_t info_out = raw_alloc_odd(
            &heap, sizeof(tf_wasm_dense_info_v1));
        tf_wasm_dense_info_v1 info;
        assert(tf_wasm_transform_apply_create(
            plan, schema_offset, limits_offset, 0,
            apply_out, error_out) == TF_TRANSFORM_OK);
        apply = raw_word(&heap, apply_out);
        assert(tf_wasm_transform_apply_run(
            apply, apply_table, dense_out, error_out) == TF_TRANSFORM_OK);
        dense = raw_word(&heap, dense_out);
        assert(tf_wasm_transform_owned_dense_info(dense, info_out)
               == TF_TRANSFORM_OK);
        memcpy(&info, heap.bytes + info_out, sizeof(info));
        assert(info.rows == 2 && info.columns == 1
               && info.data_bytes == 2 * sizeof(double));
        {
            uint32_t data_out = raw_alloc(&heap, info.data_bytes, 8);
            double actual[2];
            assert(tf_wasm_transform_owned_dense_copy(
                dense, data_out, info.data_bytes) == TF_TRANSFORM_OK);
            memcpy(actual, heap.bytes + data_out, sizeof(actual));
            for (size_t i = 0; i < 2; ++i)
                assert(raw_double_bits(actual[i]) == expected[i]);
        }
    }
    assert(tf_wasm_transform_handle_destroy(dense) == TF_TRANSFORM_OK);
    assert(tf_wasm_transform_handle_destroy(apply) == TF_TRANSFORM_OK);
    assert(tf_wasm_transform_handle_destroy(plan) == TF_TRANSFORM_OK);
    assert(tf_wasm_transform_handle_destroy(analyzer) == TF_TRANSFORM_OK);
    assert(tf_wasm_transform_handle_destroy(recipe) == TF_TRANSFORM_OK);
    free(heap.bytes);
    tf_wasm_transform_test_set_heap(NULL, 0);
    printf("  %-42s PASS\n", "prepared-transform raw kind inference");
}

int main(void) {
    printf("Tranfi WASM ABI Tests\n");
    printf("=====================\n");
    test_wasm_signed_lengths_and_null_buffers();
    test_wasm_sql_error_propagation();
    test_prepared_transform_raw_wasm_abi();
    test_prepared_transform_raw_wasm_onehot();
    test_prepared_transform_raw_wasm_inference();
    printf("\nWASM ABI tests passed\n");
    return 0;
}
