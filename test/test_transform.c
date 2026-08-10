#include "transform_internal.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#if defined(__i386__) || defined(__x86_64__)
#include <xmmintrin.h>
#endif

static size_t cjson_test_allocations = 0;

static void *counting_cjson_malloc(size_t bytes) {
    ++cjson_test_allocations;
    return malloc(bytes);
}

static void counting_cjson_free(void *memory) {
    free(memory);
}

static char *read_file(const char *path) {
    FILE *stream = fopen(path, "rb");
    long size;
    char *data;
    assert(stream != NULL);
    assert(fseek(stream, 0, SEEK_END) == 0);
    size = ftell(stream);
    assert(size >= 0);
    assert(fseek(stream, 0, SEEK_SET) == 0);
    data = (char *)malloc((size_t)size + 1);
    assert(data != NULL);
    assert(fread(data, 1, (size_t)size, stream) == (size_t)size);
    assert(fclose(stream) == 0);
    data[size] = '\0';
    return data;
}

static uint64_t parse_hex64(const char *text) {
    uint64_t value = 0;
    assert(text != NULL && strlen(text) == 16);
    for (size_t i = 0; i < 16; ++i) {
        unsigned int digit;
        if (text[i] >= '0' && text[i] <= '9') digit = (unsigned int)(text[i] - '0');
        else {
            assert(text[i] >= 'a' && text[i] <= 'f');
            digit = (unsigned int)(text[i] - 'a' + 10);
        }
        value = (value << 4) | digit;
    }
    return value;
}

static uint32_t parse_hex32(const char *text) {
    uint32_t value = 0;
    assert(text != NULL && strlen(text) == 8);
    for (size_t i = 0; i < 8; ++i) {
        unsigned int digit;
        if (text[i] >= '0' && text[i] <= '9') digit = (unsigned int)(text[i] - '0');
        else {
            assert(text[i] >= 'a' && text[i] <= 'f');
            digit = (unsigned int)(text[i] - 'a' + 10);
        }
        value = (value << 4) | digit;
    }
    return value;
}

static tf_transform_recipe *load_vector_recipe(const char *name) {
    char *text = read_file("test/vectors/prepared_transform_v1.json");
    cJSON *root = cJSON_Parse(text);
    cJSON *recipes;
    cJSON *recipe_json;
    char *serialized;
    tf_transform_limits_v1 limits;
    tf_transform_recipe *recipe = NULL;
    tf_transform_error *error = NULL;
    assert(root != NULL);
    recipes = cJSON_GetObjectItemCaseSensitive(root, "recipes");
    recipe_json = cJSON_GetObjectItemCaseSensitive(recipes, name);
    assert(recipe_json != NULL);
    serialized = cJSON_PrintUnformatted(recipe_json);
    assert(serialized != NULL);
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    assert(tf_transform_recipe_from_json(
        (const uint8_t *)serialized, strlen(serialized), &limits,
        &recipe, &error) == TF_TRANSFORM_OK);
    assert(error == NULL && recipe != NULL);
    free(serialized);
    cJSON_Delete(root);
    free(text);
    return recipe;
}

static void make_x0_schema(
    uint32_t dtype, tf_field_view_v1 *field, tf_schema_view_v1 *schema) {
    static const uint8_t x0[] = {'x', '0'};
    memset(field, 0, sizeof(*field));
    field->abi_version = 1;
    field->struct_size = (uint32_t)sizeof(*field);
    field->dtype = dtype;
    field->id_utf8 = x0;
    field->id_bytes = sizeof(x0);
    field->name_utf8 = x0;
    field->name_bytes = sizeof(x0);
    memset(schema, 0, sizeof(*schema));
    schema->abi_version = 1;
    schema->struct_size = (uint32_t)sizeof(*schema);
    schema->column_count = 1;
    schema->fields = field;
    schema->fields_bytes = sizeof(*field);
}

static void make_f64_table(
    const double *values, size_t rows,
    tf_column_view_v1 *column, tf_table_view_v1 *table) {
    memset(column, 0, sizeof(*column));
    column->abi_version = 1;
    column->struct_size = (uint32_t)sizeof(*column);
    column->data = values;
    column->data_bytes = rows * sizeof(*values);
    column->stride_bytes = sizeof(*values);
    memset(table, 0, sizeof(*table));
    table->abi_version = 1;
    table->struct_size = (uint32_t)sizeof(*table);
    table->row_count = rows;
    table->column_count = 1;
    table->columns = column;
    table->columns_bytes = sizeof(*column);
}

static void make_f32_table(
    const float *values, size_t rows,
    tf_column_view_v1 *column, tf_table_view_v1 *table) {
    memset(column, 0, sizeof(*column));
    column->abi_version = 1;
    column->struct_size = (uint32_t)sizeof(*column);
    column->data = values;
    column->data_bytes = rows * sizeof(*values);
    column->stride_bytes = sizeof(*values);
    memset(table, 0, sizeof(*table));
    table->abi_version = 1;
    table->struct_size = (uint32_t)sizeof(*table);
    table->row_count = rows;
    table->column_count = 1;
    table->columns = column;
    table->columns_bytes = sizeof(*column);
}

static tf_transform_plan *fit_f64_plan(
    const char *recipe_name, const double *values, size_t rows,
    size_t first_chunk) {
    tf_transform_recipe *recipe = load_vector_recipe(recipe_name);
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_error *error = NULL;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
    assert(tf_transform_analyzer_create(
        recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
    if (first_chunk < rows) {
        make_f64_table(values, first_chunk, &column, &table);
        assert(tf_transform_analyzer_push(analyzer, &table, &error)
               == TF_TRANSFORM_OK);
        make_f64_table(values + first_chunk, rows - first_chunk, &column, &table);
        assert(tf_transform_analyzer_push(analyzer, &table, &error)
               == TF_TRANSFORM_OK);
    } else {
        make_f64_table(values, rows, &column, &table);
        assert(tf_transform_analyzer_push(analyzer, &table, &error)
               == TF_TRANSFORM_OK);
    }
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_OK);
    assert(error == NULL && plan != NULL);
    tf_transform_analyzer_destroy(&analyzer);
    tf_transform_recipe_destroy(&recipe);
    return plan;
}

static tf_transform_plan *fit_f32_plan(
    const char *recipe_name, const float *values, size_t rows) {
    tf_transform_recipe *recipe = load_vector_recipe(recipe_name);
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_error *error = NULL;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    make_x0_schema(TF_VIEW_FLOAT32, &field, &schema);
    assert(tf_transform_analyzer_create(
        recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
    make_f32_table(values, rows, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_OK);
    assert(error == NULL && plan != NULL);
    tf_transform_analyzer_destroy(&analyzer);
    tf_transform_recipe_destroy(&recipe);
    return plan;
}

static void assert_apply_bits(
    tf_transform_plan *plan, const double *values, size_t rows,
    const uint64_t *expected) {
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    tf_transform_apply *apply = NULL;
    tf_transform_error *error = NULL;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    tf_owned_dense_v1 dense;
    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
    assert(tf_transform_apply_create(plan, &schema, NULL, &apply, &error)
           == TF_TRANSFORM_OK);
    make_f64_table(values, rows, &column, &table);
    assert(tf_transform_apply_run(apply, &table, &dense, &error)
           == TF_TRANSFORM_OK);
    assert(dense.rows == rows && dense.columns == 1);
    for (size_t i = 0; i < rows; ++i)
        assert(tf_transform_double_bits(((double *)dense.data)[i]) == expected[i]);
    tf_owned_dense_free(&dense);
    tf_transform_apply_destroy(&apply);
}

static void assert_hex_bytes(const uint8_t *bytes, size_t len, const char *hex) {
    static const char digits[] = "0123456789abcdef";
    assert(strlen(hex) == len * 2);
    for (size_t i = 0; i < len; ++i) {
        assert(hex[i * 2] == digits[bytes[i] >> 4]);
        assert(hex[i * 2 + 1] == digits[bytes[i] & 15]);
    }
}

static uint8_t *decode_hex(const char *hex, size_t *out_len) {
    size_t len = strlen(hex);
    uint8_t *bytes;
    assert(len % 2 == 0);
    bytes = (uint8_t *)malloc(len / 2);
    assert(bytes != NULL);
    for (size_t i = 0; i < len / 2; ++i) {
        unsigned int value = 0;
        for (size_t j = 0; j < 2; ++j) {
            unsigned char c = (unsigned char)hex[i * 2 + j];
            unsigned int digit;
            if (c >= '0' && c <= '9') digit = c - '0';
            else {
                assert(c >= 'a' && c <= 'f');
                digit = c - 'a' + 10u;
            }
            value = (value << 4) | digit;
        }
        bytes[i] = (uint8_t)value;
    }
    *out_len = len / 2;
    return bytes;
}

static void test_abi_and_limits(void) {
    tf_transform_limits_v1 limits;
    unsigned char short_buffer[sizeof(limits)];
    unsigned char expected[sizeof(limits)];
    memset(&limits, 0, sizeof(limits));
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits)) == TF_TRANSFORM_OK);
    assert(limits.abi_version == 1);
    assert(limits.struct_size == 184);
    assert(limits.max_recipe_bytes == UINT64_C(1048576));
    assert(limits.max_plan_bytes == UINT64_C(67108864));
    assert(limits.max_output_elements_per_call == UINT64_C(134217728));
    assert(limits.max_live_handles == UINT64_C(65535));
    memset(short_buffer, 0xa5, sizeof(short_buffer));
    memcpy(expected, short_buffer, sizeof(expected));
    assert(tf_transform_limits_init_safe_v1(
        (tf_transform_limits_v1 *)short_buffer, sizeof(limits) - 1)
        == TF_TRANSFORM_INVALID_ARGUMENT);
    assert(memcmp(short_buffer, expected, sizeof(expected)) == 0);
    assert(TF_TRANSFORM_UNSUPPORTED_RUNTIME == 113);
    assert(TF_VIEW_FLOAT32 == 0x0101);
    assert(TF_VIEW_FLOAT64 == 0x0102);
}

static void test_sqrt_vectors(void) {
    char *text = read_file("test/vectors/prepared_transform_v1.json");
    cJSON *root = cJSON_Parse(text);
    cJSON *cases;
    int count;
    assert(root != NULL);
    cases = cJSON_GetObjectItemCaseSensitive(root, "sqrtCases");
    assert(cJSON_IsArray(cases));
    count = cJSON_GetArraySize(cases);
    assert(count >= 7);
    for (int i = 0; i < count; ++i) {
        cJSON *entry = cJSON_GetArrayItem(cases, i);
        cJSON *input = cJSON_GetObjectItemCaseSensitive(entry, "input");
        cJSON *expected = cJSON_GetObjectItemCaseSensitive(entry, "expected");
        double source;
        double result;
        assert(cJSON_IsString(input) && cJSON_IsString(expected));
        source = tf_transform_double_from_bits(parse_hex64(input->valuestring));
        result = tf_transform_sqrt_f64_rne(source);
        assert(tf_transform_double_bits(result) == parse_hex64(expected->valuestring));
    }
    cJSON_Delete(root);
    free(text);
}

static void test_sha256(void) {
    uint8_t digest[32];
    tf_transform_sha256(NULL, 0, digest);
    assert_hex_bytes(digest, sizeof(digest),
                     "e3b0c44298fc1c149afbf4c8996fb924"
                     "27ae41e4649b934ca495991b7852b855");
    tf_transform_sha256((const uint8_t *)"abc", 3, digest);
    assert_hex_bytes(digest, sizeof(digest),
                     "ba7816bf8f01cfea414140de5dae2223"
                     "b00361a396177a9cb410ff61f20015ad");
}

static void test_recipe_vectors_and_canonical_json(void) {
    char *text = read_file("test/vectors/prepared_transform_v1.json");
    cJSON *root = cJSON_Parse(text);
    cJSON *recipes;
    const char *numeric_names[] = {
        "numeric_mean_standard", "numeric_median_none",
        "numeric_median_zero_none",
        "numeric_none_none", "numeric_zero_minmax"
    };
    tf_transform_limits_v1 limits;
    assert(root != NULL);
    recipes = cJSON_GetObjectItemCaseSensitive(root, "recipes");
    assert(cJSON_IsObject(recipes));
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits)) == TF_TRANSFORM_OK);
    for (size_t i = 0; i < sizeof(numeric_names) / sizeof(numeric_names[0]); ++i) {
        cJSON *recipe_json = cJSON_GetObjectItemCaseSensitive(recipes, numeric_names[i]);
        char *serialized = cJSON_PrintUnformatted(recipe_json);
        tf_transform_recipe *recipe = NULL;
        tf_transform_error *error = NULL;
        assert(serialized != NULL);
        assert(tf_transform_recipe_from_json(
            (const uint8_t *)serialized, strlen(serialized), &limits,
            &recipe, &error) == TF_TRANSFORM_OK);
        assert(error == NULL);
        assert(recipe != NULL && recipe->column_count == 1);
        tf_transform_recipe_destroy(&recipe);
        tf_transform_recipe_destroy(&recipe);
        free(serialized);
    }
    {
        const char duplicate[] =
            "{\"columns\":[],\"columns\":[],\"format\":\"tranfi.transform-recipe\"}";
        tf_transform_recipe *recipe = NULL;
        tf_transform_error *error = NULL;
        size_t message_len = 0;
        assert(tf_transform_recipe_from_json(
            (const uint8_t *)duplicate, strlen(duplicate), &limits,
            &recipe, &error) == TF_TRANSFORM_INVALID_RECIPE);
        assert(recipe == NULL);
        assert(tf_transform_error_get_code(error) == TF_TRANSFORM_INVALID_RECIPE);
        assert(tf_transform_error_message(error, &message_len) != NULL);
        assert(message_len != 0);
        tf_transform_error_destroy(&error);
    }
    {
        cJSON *object = cJSON_CreateObject();
        uint8_t *canonical = NULL;
        size_t canonical_len = 0;
        tf_transform_error *error = NULL;
        const char expected[] = "{\"a\":\"\\u000a\",\"b\":1}";
        assert(object != NULL);
        assert(cJSON_AddNumberToObject(object, "b", 1) != NULL);
        assert(cJSON_AddStringToObject(object, "a", "\n") != NULL);
        assert(tf_transform_json_print_canonical(
            object, &limits, &canonical, &canonical_len, &error)
            == TF_TRANSFORM_OK);
        assert(error == NULL);
        assert(canonical_len == strlen(expected));
        assert(memcmp(canonical, expected, canonical_len) == 0);
        tf_transform_bytes_free(&canonical, &canonical_len);
        cJSON_Delete(object);
    }
    cJSON_Delete(root);
    free(text);
}

static void test_fp_guard_restores_rounding(void) {
    tf_transform_fp_guard guard;
    tf_transform_error *error = NULL;
#if !defined(__wasm__)
    int original = fegetround();
#if defined(__i386__) || defined(__x86_64__)
    unsigned int original_mxcsr = _mm_getcsr();
    unsigned int caller_mxcsr;
#endif
    assert(original != -1);
    assert(fesetround(FE_DOWNWARD) == 0);
#if defined(__i386__) || defined(__x86_64__)
    caller_mxcsr = _mm_getcsr() | (1u << 15) | (1u << 6);
    _mm_setcsr(caller_mxcsr);
    assert(_mm_getcsr() == caller_mxcsr);
#endif
    assert(tf_transform_fp_begin(&guard, &error) == TF_TRANSFORM_OK);
    assert(error == NULL);
    assert(fegetround() == FE_TONEAREST);
#if defined(__i386__) || defined(__x86_64__)
    assert((_mm_getcsr() & ((3u << 13) | (1u << 15) | (1u << 6))) == 0);
#endif
    tf_transform_fp_end(&guard);
    assert(fegetround() == FE_DOWNWARD);
#if defined(__i386__) || defined(__x86_64__)
    assert(_mm_getcsr() == caller_mxcsr);
#endif
    assert(fesetround(original) == 0);
#if defined(__i386__) || defined(__x86_64__)
    _mm_setcsr(original_mxcsr);
    assert(_mm_getcsr() == original_mxcsr);
#endif
#else
    assert(tf_transform_fp_begin(&guard, &error) == TF_TRANSFORM_OK);
    tf_transform_fp_end(&guard);
#endif
    tf_transform_error_destroy(&error);
}

static void test_numeric_lifecycle_and_schema(void) {
    const double chunk1[] = {1.0};
    const double chunk2[] = {
        tf_transform_double_from_bits(UINT64_C(0x7ff8000000000000)),
        3.0,
        tf_transform_double_from_bits(UINT64_C(0x7ff8000000000000))
    };
    const double apply_values[] = {
        1.0,
        tf_transform_double_from_bits(UINT64_C(0x7ff8000000000001)),
        3.0
    };
    const uint64_t expected[] = {
        UINT64_C(0xbff6a09e667f3bcc),
        UINT64_C(0x0000000000000000),
        UINT64_C(0x3ff6a09e667f3bcc)
    };
    tf_transform_recipe *recipe = load_vector_recipe("numeric_mean_standard");
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_plan *extra_plan = NULL;
    tf_transform_apply *apply = NULL;
    tf_transform_error *error = NULL;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    tf_owned_dense_v1 dense;
    const tf_transform_schema *borrowed = NULL;
    tf_field_view_v1 output_field;
    uint8_t *schema_json = NULL;
    size_t schema_json_len = 0;
    tf_transform_limits_v1 limits;

    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
    assert(tf_transform_analyzer_create(
        recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
    assert(error == NULL && analyzer != NULL);
    tf_transform_recipe_destroy(&recipe);
    make_f64_table(chunk1, 1, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    make_f64_table(chunk2, 3, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_OK);
    assert(plan != NULL && error == NULL);
    assert(tf_transform_analyzer_finalize(analyzer, &extra_plan, &error)
           == TF_TRANSFORM_INVALID_STATE);
    assert(extra_plan == NULL);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);

    assert(tf_transform_plan_schema(
        plan, TF_TRANSFORM_SCHEMA_OUTPUT, &borrowed, &error)
        == TF_TRANSFORM_OK);
    assert(tf_transform_schema_field_count(borrowed) == 1);
    assert(tf_transform_schema_field(borrowed, 0, &output_field, &error)
           == TF_TRANSFORM_OK);
    assert(output_field.dtype == TF_VIEW_FLOAT64);
    assert(output_field.id_bytes == 2
           && memcmp(output_field.id_utf8, "x0", 2) == 0);
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    assert(tf_transform_plan_schema_json(
        plan, TF_TRANSFORM_SCHEMA_OUTPUT, &limits,
        &schema_json, &schema_json_len, &error) == TF_TRANSFORM_OK);
    assert(schema_json_len == strlen(
        "[{\"category\":null,\"dtype\":\"float64\",\"id\":\"x0\","
        "\"name\":\"x0\",\"role\":\"value\",\"sourceId\":\"x0\"}]")
        && memcmp(schema_json,
        "[{\"category\":null,\"dtype\":\"float64\",\"id\":\"x0\","
        "\"name\":\"x0\",\"role\":\"value\",\"sourceId\":\"x0\"}]",
        schema_json_len) == 0);
    tf_transform_bytes_free(&schema_json, &schema_json_len);

    assert(tf_transform_apply_create(plan, &schema, NULL, &apply, &error)
           == TF_TRANSFORM_OK);
    tf_transform_plan_destroy(&plan);
    make_f64_table(apply_values, 3, &column, &table);
    memset(&dense, 0xa5, sizeof(dense));
    assert(tf_transform_apply_run(apply, &table, &dense, &error)
           == TF_TRANSFORM_OK);
    assert(dense.abi_version == 1 && dense.dtype == TF_VIEW_FLOAT64);
    assert(dense.rows == 3 && dense.columns == 1
           && dense.data_bytes == 3 * sizeof(double));
    for (size_t i = 0; i < 3; ++i)
        assert(tf_transform_double_bits(((double *)dense.data)[i]) == expected[i]);
    tf_owned_dense_free(&dense);
    make_f64_table(NULL, 0, &column, &table);
    assert(tf_transform_apply_run(apply, &table, &dense, &error)
           == TF_TRANSFORM_OK);
    assert(dense.rows == 0 && dense.columns == 1
           && dense.data == NULL && dense.data_bytes == 0);
    tf_owned_dense_free(&dense);
    tf_transform_apply_destroy(&apply);
}

static int always_cancel(void *user) {
    (void)user;
    return 1;
}

static int cancel_when_set(void *user) {
    return user && *(const int *)user;
}

typedef struct cancel_counter {
    size_t calls;
    size_t cancel_at;
} cancel_counter;

static int cancel_on_call(void *user) {
    cancel_counter *counter = (cancel_counter *)user;
    assert(counter != NULL);
    ++counter->calls;
    return counter->cancel_at != 0 && counter->calls >= counter->cancel_at;
}

typedef struct fp_mutation_probe {
    size_t calls;
    size_t mutate_at;
    int mutated;
} fp_mutation_probe;

static int mutate_fp_on_call(void *user) {
    fp_mutation_probe *probe = (fp_mutation_probe *)user;
    assert(probe != NULL);
    ++probe->calls;
#if !defined(__wasm__)
    if (!probe->mutated && probe->mutate_at != 0
        && probe->calls == probe->mutate_at) {
        assert(fesetround(FE_DOWNWARD) == 0);
#if defined(__i386__) || defined(__x86_64__)
        _mm_setcsr(_mm_getcsr() | (1u << 15) | (1u << 6));
#endif
        probe->mutated = 1;
    }
#endif
    return 0;
}

typedef struct wide_categorical_fixture {
    tf_transform_recipe *recipe;
    tf_field_view_v1 *fields;
    tf_column_view_v1 *columns;
    tf_schema_view_v1 schema;
    tf_table_view_v1 table;
    double value;
    size_t column_count;
} wide_categorical_fixture;

static void wide_categorical_fixture_clear(wide_categorical_fixture *fixture) {
    if (!fixture) return;
    free(fixture->fields);
    free(fixture->columns);
    tf_transform_recipe_destroy(&fixture->recipe);
    memset(fixture, 0, sizeof(*fixture));
}

static void wide_categorical_fixture_init(
    wide_categorical_fixture *fixture, size_t column_count) {
    tf_transform_recipe *recipe;
    assert(fixture != NULL && column_count != 0);
    memset(fixture, 0, sizeof(*fixture));
    recipe = (tf_transform_recipe *)calloc(1, sizeof(*recipe));
    fixture->fields = (tf_field_view_v1 *)calloc(
        column_count, sizeof(*fixture->fields));
    fixture->columns = (tf_column_view_v1 *)calloc(
        column_count, sizeof(*fixture->columns));
    assert(recipe != NULL && fixture->fields != NULL
           && fixture->columns != NULL);
    recipe->columns = (tf_transform_recipe_column *)calloc(
        column_count, sizeof(*recipe->columns));
    assert(recipe->columns != NULL);
    atomic_init(&recipe->refcount, 1u);
    recipe->column_count = column_count;
    recipe->max_output_columns = column_count;
    recipe->max_output_elements_per_apply = column_count * 4;
    fixture->recipe = recipe;
    fixture->column_count = column_count;
    fixture->value = 0.0;
    for (size_t i = 0; i < column_count; ++i) {
        char id[32];
        int id_len = snprintf(id, sizeof(id), "c%05zu", i);
        assert(id_len > 0 && (size_t)id_len < sizeof(id));
        recipe->columns[i].source_id = (char *)malloc((size_t)id_len + 1);
        assert(recipe->columns[i].source_id != NULL);
        memcpy(recipe->columns[i].source_id, id, (size_t)id_len + 1);
        recipe->columns[i].source_id_len = (size_t)id_len;
        recipe->columns[i].kind = TF_TRANSFORM_KIND_NUMERIC;
        recipe->columns[i].impute = TF_TRANSFORM_IMPUTE_NONE;
        recipe->columns[i].all_missing = TF_TRANSFORM_ALL_MISSING_NONE;
        recipe->columns[i].normalize = TF_TRANSFORM_NORMALIZE_NONE;
        recipe->columns[i].ddof = 1;
        fixture->fields[i].abi_version = 1;
        fixture->fields[i].struct_size = (uint32_t)sizeof(fixture->fields[i]);
        fixture->fields[i].dtype = TF_VIEW_FLOAT64;
        fixture->fields[i].id_utf8
            = (const uint8_t *)recipe->columns[i].source_id;
        fixture->fields[i].id_bytes = (size_t)id_len;
        fixture->fields[i].name_utf8
            = (const uint8_t *)recipe->columns[i].source_id;
        fixture->fields[i].name_bytes = (size_t)id_len;
        fixture->columns[i].abi_version = 1;
        fixture->columns[i].struct_size
            = (uint32_t)sizeof(fixture->columns[i]);
        fixture->columns[i].data = &fixture->value;
        fixture->columns[i].data_bytes = sizeof(fixture->value);
        fixture->columns[i].stride_bytes = sizeof(fixture->value);
    }
    recipe->columns[column_count - 1].kind = TF_TRANSFORM_KIND_CATEGORICAL;
    recipe->columns[column_count - 1].categorical_impute
        = TF_TRANSFORM_CATEGORICAL_IMPUTE_MODE;
    recipe->columns[column_count - 1].categorical_all_missing
        = TF_TRANSFORM_ALL_MISSING_ZERO;
    recipe->columns[column_count - 1].categorical_encode
        = TF_TRANSFORM_ENCODE_NONE;
    recipe->columns[column_count - 1].categorical_discover = 1;
    fixture->schema.abi_version = 1;
    fixture->schema.struct_size = (uint32_t)sizeof(fixture->schema);
    fixture->schema.column_count = column_count;
    fixture->schema.fields = fixture->fields;
    fixture->schema.fields_bytes = column_count * sizeof(*fixture->fields);
    fixture->table.abi_version = 1;
    fixture->table.struct_size = (uint32_t)sizeof(fixture->table);
    fixture->table.row_count = 1;
    fixture->table.column_count = column_count;
    fixture->table.columns = fixture->columns;
    fixture->table.columns_bytes = column_count * sizeof(*fixture->columns);
}

static void wide_categorical_fixture_set_field(
    wide_categorical_fixture *fixture, size_t index,
    const char *id, const char *name) {
    size_t id_len;
    size_t name_len;
    char *owned_id;
    assert(fixture != NULL && index < fixture->column_count
           && id != NULL && name != NULL);
    id_len = strlen(id);
    name_len = strlen(name);
    owned_id = (char *)malloc(id_len + 1);
    assert(owned_id != NULL);
    memcpy(owned_id, id, id_len + 1);
    free(fixture->recipe->columns[index].source_id);
    fixture->recipe->columns[index].source_id = owned_id;
    fixture->recipe->columns[index].source_id_len = id_len;
    fixture->fields[index].id_utf8 = (const uint8_t *)owned_id;
    fixture->fields[index].id_bytes = id_len;
    fixture->fields[index].name_utf8 = (const uint8_t *)name;
    fixture->fields[index].name_bytes = name_len;
}

static void wide_categorical_fixture_set_label(
    wide_categorical_fixture *fixture, size_t index,
    tf_transform_unknown_policy unknown, int64_t sentinel) {
    tf_transform_recipe_column *column;
    assert(fixture != NULL && index < fixture->column_count);
    column = &fixture->recipe->columns[index];
    column->kind = TF_TRANSFORM_KIND_CATEGORICAL;
    column->categorical_impute = TF_TRANSFORM_CATEGORICAL_IMPUTE_MODE;
    column->categorical_all_missing = TF_TRANSFORM_ALL_MISSING_ERROR;
    column->categorical_encode = TF_TRANSFORM_ENCODE_LABEL;
    column->categorical_discover = 1;
    column->categorical_unknown = unknown;
    column->categorical_sentinel_label = sentinel;
    column->categorical_has_sentinel_label
        = unknown == TF_TRANSFORM_UNKNOWN_SENTINEL;
}

static void wide_categorical_fixture_set_onehot(
    wide_categorical_fixture *fixture, size_t index,
    tf_transform_unknown_policy unknown) {
    tf_transform_recipe_column *column;
    assert(fixture != NULL && index < fixture->column_count
           && (unknown == TF_TRANSFORM_UNKNOWN_ERROR
               || unknown == TF_TRANSFORM_UNKNOWN_ALL_ZERO
               || unknown == TF_TRANSFORM_UNKNOWN_OTHER));
    column = &fixture->recipe->columns[index];
    column->kind = TF_TRANSFORM_KIND_CATEGORICAL;
    column->categorical_impute = TF_TRANSFORM_CATEGORICAL_IMPUTE_MODE;
    column->categorical_all_missing = TF_TRANSFORM_ALL_MISSING_ERROR;
    column->categorical_encode = TF_TRANSFORM_ENCODE_ONEHOT;
    column->categorical_discover = 1;
    column->categorical_unknown = unknown;
    column->categorical_sentinel_label = 0;
    column->categorical_has_sentinel_label = 0;
}

static uint64_t owned_schema_resident_for_test(
    const tf_transform_schema *schema) {
    uint64_t resident;
    assert(schema != NULL
           && schema->field_count <= UINT64_MAX / sizeof(*schema->fields));
    resident = (uint64_t)schema->field_count * sizeof(*schema->fields);
    for (size_t i = 0; i < schema->field_count; ++i) {
        const tf_transform_schema_field_owned *field = &schema->fields[i];
        assert((uint64_t)field->id_len + 1 <= UINT64_MAX - resident);
        resident += (uint64_t)field->id_len + 1;
        assert((uint64_t)field->name_len + 1 <= UINT64_MAX - resident);
        resident += (uint64_t)field->name_len + 1;
        if (field->source_id) {
            assert((uint64_t)field->source_id_len + 1 <= UINT64_MAX - resident);
            resident += (uint64_t)field->source_id_len + 1;
        }
    }
    return resident;
}

static void test_guarded_callback_fp_mutation(void) {
#if !defined(__wasm__)
    const double numeric_values[] = {1.0, 2.0, 3.0};
    tf_transform_recipe *recipe = load_vector_recipe("numeric_mean_standard");
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_apply *apply = NULL;
    tf_transform_error *error = NULL;
    tf_transform_runtime_v1 runtime;
    tf_owned_dense_v1 dense;
    fp_mutation_probe probe = {0, 0, 0};
    size_t before;
    size_t finalize_polls;
    int original_rounding = fegetround();
#if defined(__i386__) || defined(__x86_64__)
    unsigned int original_mxcsr = _mm_getcsr();
#endif

    assert(original_rounding != -1);
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.cancel = mutate_fp_on_call;
    runtime.cancel_user = &probe;
    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
    make_f64_table(numeric_values, 3, &column, &table);
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error) == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    before = probe.calls;
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_OK);
    finalize_polls = probe.calls - before;
    assert(finalize_polls > 1 && plan != NULL);
    tf_transform_plan_destroy(&plan);
    tf_transform_analyzer_destroy(&analyzer);

    probe.calls = 0;
    probe.mutate_at = 0;
    probe.mutated = 0;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error) == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    probe.mutate_at = probe.calls + finalize_polls;
    original_rounding = fegetround();
#if defined(__i386__) || defined(__x86_64__)
    original_mxcsr = _mm_getcsr();
#endif
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_UNSUPPORTED_RUNTIME);
    assert(plan == NULL && probe.mutated
           && tf_transform_error_get_code(error)
                == TF_TRANSFORM_UNSUPPORTED_RUNTIME);
    assert(fegetround() == original_rounding);
#if defined(__i386__) || defined(__x86_64__)
    assert(_mm_getcsr() == original_mxcsr);
#endif
    tf_transform_error_destroy(&error);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_INVALID_STATE);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);
    tf_transform_recipe_destroy(&recipe);

    {
        wide_categorical_fixture fixture;
        wide_categorical_fixture_init(&fixture, 2);
        fixture.recipe->columns[0].kind = TF_TRANSFORM_KIND_CATEGORICAL;
        fixture.recipe->columns[0].categorical_impute
            = TF_TRANSFORM_CATEGORICAL_IMPUTE_MODE;
        fixture.recipe->columns[0].categorical_all_missing
            = TF_TRANSFORM_ALL_MISSING_ZERO;
        fixture.recipe->columns[0].categorical_encode
            = TF_TRANSFORM_ENCODE_NONE;
        fixture.recipe->columns[0].categorical_discover = 1;
        fixture.recipe->columns[1].kind = TF_TRANSFORM_KIND_NUMERIC;
        fixture.recipe->columns[1].categorical_impute
            = TF_TRANSFORM_CATEGORICAL_IMPUTE_NONE;
        fixture.recipe->columns[1].categorical_all_missing
            = TF_TRANSFORM_ALL_MISSING_NONE;
        fixture.recipe->columns[1].categorical_encode
            = TF_TRANSFORM_ENCODE_NONE;
        fixture.recipe->columns[1].categorical_discover = 0;
        assert(tf_transform_analyzer_create(
            fixture.recipe, &fixture.schema, NULL, &analyzer, &error)
            == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_OK);
        tf_transform_analyzer_destroy(&analyzer);

        probe.calls = 0;
        probe.mutate_at = 0;
        probe.mutated = 0;
        assert(tf_transform_apply_create(
            plan, &fixture.schema, &runtime, &apply, &error) == TF_TRANSFORM_OK);
        before = probe.calls;
        probe.mutate_at = before + 6;
        original_rounding = fegetround();
#if defined(__i386__) || defined(__x86_64__)
        original_mxcsr = _mm_getcsr();
#endif
        assert(tf_transform_apply_run(apply, &fixture.table, &dense, &error)
               == TF_TRANSFORM_UNSUPPORTED_RUNTIME);
        assert(dense.data == NULL && probe.mutated
               && tf_transform_error_get_code(error)
                    == TF_TRANSFORM_UNSUPPORTED_RUNTIME);
        assert(fegetround() == original_rounding);
#if defined(__i386__) || defined(__x86_64__)
        assert(_mm_getcsr() == original_mxcsr);
#endif
        tf_transform_error_destroy(&error);
        assert(tf_transform_apply_run(apply, &fixture.table, &dense, &error)
               == TF_TRANSFORM_INVALID_STATE);
        tf_transform_error_destroy(&error);
        tf_transform_apply_destroy(&apply);
        tf_transform_plan_destroy(&plan);
        wide_categorical_fixture_clear(&fixture);
    }
#endif
}

typedef struct median_sort_cancel_probe {
    tf_transform_analyzer *analyzer;
    double original_first;
    size_t calls;
    int armed;
    int observed_sort;
} median_sort_cancel_probe;

static int cancel_during_median_sort(void *user) {
    median_sort_cancel_probe *probe = (median_sort_cancel_probe *)user;
    assert(probe != NULL);
    ++probe->calls;
    if (probe->armed && probe->analyzer
        && probe->analyzer->median_stores
        && probe->analyzer->median_stores[0].count != 0
        && probe->analyzer->median_stores[0].blocks[0][0]
            != probe->original_first) {
        probe->observed_sort = 1;
        return 1;
    }
    return 0;
}

static void write_u16_le_test(uint8_t *out, uint16_t value) {
    out[0] = (uint8_t)value;
    out[1] = (uint8_t)(value >> 8);
}

static void write_u32_le_test(uint8_t *out, uint32_t value) {
    for (size_t i = 0; i < 4; ++i) out[i] = (uint8_t)(value >> (8 * i));
}

static void write_u64_le_test(uint8_t *out, uint64_t value) {
    for (size_t i = 0; i < 8; ++i) out[i] = (uint8_t)(value >> (8 * i));
}

static void test_runtime_safe_points_and_parser_limits(void) {
    const size_t payload_len = TF_TRANSFORM_CANCEL_BYTES_V1 * 3 + 17;
    uint8_t *payload = (uint8_t *)malloc(payload_len);
    uint8_t *tftr = (uint8_t *)malloc(52 + payload_len);
    uint8_t digest[32];
    tf_transform_runtime_v1 runtime;
    tf_transform_runtime_copy copied;
    tf_transform_limits_v1 limits;
    tf_transform_plan *plan = NULL;
    tf_transform_error *error = NULL;
    cancel_counter counter = {0, 4};
    cJSON *parsed = NULL;
    cJSON *large_string = NULL;
    tf_transform_resource_ledger ledger;
    uint8_t *canonical = NULL;
    size_t canonical_len = 0;
    char *json;

    assert(payload != NULL && tftr != NULL);
    memset(payload, ' ', payload_len);
    payload[0] = '{';
    payload[payload_len - 1] = '}';
    tf_transform_sha256(payload, payload_len, digest);
    memcpy(tftr, "TFTR", 4);
    write_u16_le_test(tftr + 4, 1);
    write_u16_le_test(tftr + 6, 0);
    write_u32_le_test(tftr + 8, 52);
    write_u64_le_test(tftr + 12, payload_len);
    memcpy(tftr + 20, digest, sizeof(digest));
    memcpy(tftr + 52, payload, payload_len);
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.cancel = cancel_on_call;
    runtime.cancel_user = &counter;
    assert(tf_transform_plan_import(
        tftr, 52 + payload_len, &runtime, &plan, &error)
        == TF_TRANSFORM_CANCELLED);
    assert(plan == NULL && counter.calls == counter.cancel_at);
    tf_transform_error_destroy(&error);

    json = (char *)malloc(payload_len + 3);
    assert(json != NULL);
    memset(json, ' ', payload_len);
    json[payload_len] = '{';
    json[payload_len + 1] = '}';
    json[payload_len + 2] = '\0';
    counter.calls = 0;
    counter.cancel_at = 3;
    assert(tf_transform_copy_runtime(&runtime, &copied, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_json_parse_bounded_runtime(
        (const uint8_t *)json, payload_len + 2, &copied,
        &parsed, NULL, NULL, &error, TF_TRANSFORM_CORRUPT_PLAN)
        == TF_TRANSFORM_CANCELLED);
    assert(parsed == NULL && counter.calls == counter.cancel_at);
    tf_transform_error_destroy(&error);

    memset(json, 'x', payload_len);
    json[payload_len] = '\0';
    large_string = cJSON_CreateString(json);
    assert(large_string != NULL);
    counter.calls = 0;
    counter.cancel_at = 3;
    assert(tf_transform_json_print_canonical_runtime(
        large_string, &copied, &canonical, &canonical_len, &error)
        == TF_TRANSFORM_CANCELLED);
    assert(canonical == NULL && canonical_len == 0
           && counter.calls == counter.cancel_at);
    tf_transform_error_destroy(&error);
    cJSON_Delete(large_string);

    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    limits.max_allocations_per_session = 2;
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.limits = &limits;
    assert(tf_transform_copy_runtime(&runtime, &copied, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_json_parse_bounded_runtime(
        (const uint8_t *)"[1,2]", 5, &copied,
        &parsed, NULL, NULL, &error, TF_TRANSFORM_CORRUPT_PLAN)
        == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(parsed == NULL);
    tf_transform_error_destroy(&error);

    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    limits.max_allocation_bytes = sizeof(cJSON) - 1;
    runtime.limits = &limits;
    assert(tf_transform_copy_runtime(&runtime, &copied, &error)
           == TF_TRANSFORM_OK);
    tf_transform_resource_ledger_init(&ledger, &copied);
    assert(tf_transform_json_parse_bounded_runtime_ledger(
        (const uint8_t *)"{}", 2, &copied, &ledger,
        &parsed, NULL, NULL, &error, TF_TRANSFORM_CORRUPT_PLAN)
        == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(parsed == NULL && ledger.allocation_count == 0);
    tf_transform_error_destroy(&error);

    {
        size_t string_len = sizeof(cJSON) * 2;
        char *oversized = (char *)malloc(string_len + 3);
        assert(oversized != NULL);
        oversized[0] = '"';
        memset(oversized + 1, 'x', string_len);
        oversized[string_len + 1] = '"';
        oversized[string_len + 2] = '\0';
        limits.max_allocation_bytes = sizeof(cJSON);
        runtime.limits = &limits;
        assert(tf_transform_copy_runtime(&runtime, &copied, &error)
               == TF_TRANSFORM_OK);
        tf_transform_resource_ledger_init(&ledger, &copied);
        assert(tf_transform_json_parse_bounded_runtime_ledger(
            (const uint8_t *)oversized, string_len + 2,
            &copied, &ledger, &parsed, NULL, NULL,
            &error, TF_TRANSFORM_CORRUPT_PLAN)
            == TF_TRANSFORM_RESOURCE_LIMIT);
        assert(parsed == NULL && ledger.allocation_count == 0);
        tf_transform_error_destroy(&error);
        free(oversized);
    }

    {
        char *vectors = read_file("test/vectors/prepared_transform_v1.json");
        cJSON *vector_root = cJSON_Parse(vectors);
        cJSON *recipes = cJSON_GetObjectItemCaseSensitive(
            vector_root, "recipes");
        cJSON *recipe_json = cJSON_Duplicate(
            cJSON_GetObjectItemCaseSensitive(recipes, "numeric_none_none"), 1);
        cJSON *columns = cJSON_GetObjectItemCaseSensitive(recipe_json, "columns");
        cJSON *second = cJSON_Duplicate(columns->child, 1);
        char *serialized;
        tf_transform_recipe *limited_recipe = NULL;
        assert(vector_root != NULL && recipe_json != NULL && second != NULL);
        assert(cJSON_ReplaceItemInObjectCaseSensitive(
            second, "sourceId", cJSON_CreateString("x1")));
        assert(cJSON_AddItemToArray(columns, second));
        serialized = cJSON_PrintUnformatted(recipe_json);
        assert(serialized != NULL);
        limits.max_allocation_bytes = sizeof(cJSON);
        assert(tf_transform_recipe_from_json(
            (const uint8_t *)serialized, strlen(serialized), &limits,
            &limited_recipe, &error) == TF_TRANSFORM_RESOURCE_LIMIT);
        assert(limited_recipe == NULL);
        tf_transform_error_destroy(&error);
        free(serialized);
        cJSON_Delete(recipe_json);
        cJSON_Delete(vector_root);
        free(vectors);
    }

    free(json);
    free(tftr);
    free(payload);
}

static void test_session_allocation_limits(void) {
    const double value[] = {1.0};
    tf_transform_recipe *recipe = load_vector_recipe("numeric_none_none");
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_apply *apply = NULL;
    tf_transform_error *error = NULL;
    tf_transform_limits_v1 limits;
    tf_transform_runtime_v1 runtime;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    tf_owned_dense_v1 dense;

    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    limits.max_allocations_per_session = 6;
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.limits = &limits;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error)
        == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(analyzer == NULL);
    tf_transform_error_destroy(&error);

    limits.max_allocations_per_session = 7;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error)
        == TF_TRANSFORM_OK);
    assert(analyzer != NULL && analyzer->allocation_count == 7);
    tf_transform_analyzer_destroy(&analyzer);

    limits.max_allocations_per_session = 15;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error)
        == TF_TRANSFORM_OK);
    make_f64_table(value, 1, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
        == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
        == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(plan == NULL);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);

    limits.max_allocations_per_session = 16;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error)
        == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
        == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
        == TF_TRANSFORM_OK);
    assert(plan != NULL && analyzer->allocation_count == 16);
    tf_transform_analyzer_destroy(&analyzer);
    tf_transform_plan_destroy(&plan);

    plan = fit_f64_plan("numeric_none_none", value, 1, 1);
    limits.max_allocations_per_session = 2;
    assert(tf_transform_apply_create(
        plan, &schema, &runtime, &apply, &error) == TF_TRANSFORM_OK);
    make_f64_table(value, 1, &column, &table);
    assert(tf_transform_apply_run(apply, &table, &dense, &error)
           == TF_TRANSFORM_OK);
    tf_owned_dense_free(&dense);
    assert(tf_transform_apply_run(apply, &table, &dense, &error)
           == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(dense.data == NULL && dense.data_bytes == 0);
    tf_transform_error_destroy(&error);
    assert(tf_transform_apply_run(apply, &table, &dense, &error)
           == TF_TRANSFORM_INVALID_STATE);
    tf_transform_error_destroy(&error);
    tf_transform_apply_destroy(&apply);
    tf_transform_plan_destroy(&plan);
    tf_transform_recipe_destroy(&recipe);
}

typedef struct large_transform_fixture {
    tf_transform_recipe *recipe;
    char *ids[2];
    char *names[2];
    tf_field_view_v1 fields[2];
    tf_schema_view_v1 schema;
} large_transform_fixture;

static void large_transform_fixture_clear(large_transform_fixture *fixture) {
    if (!fixture) return;
    tf_transform_recipe_destroy(&fixture->recipe);
    for (size_t i = 0; i < 2; ++i) {
        free(fixture->ids[i]);
        free(fixture->names[i]);
    }
    memset(fixture, 0, sizeof(*fixture));
}

static void large_transform_fixture_init(
    large_transform_fixture *fixture, size_t string_len) {
    char *vectors = read_file("test/vectors/prepared_transform_v1.json");
    cJSON *vector_root = cJSON_Parse(vectors);
    cJSON *recipes;
    cJSON *recipe_json;
    cJSON *columns;
    cJSON *first;
    cJSON *second;
    cJSON *semantic;
    char *serialized;
    tf_transform_limits_v1 limits;
    tf_transform_error *error = NULL;
    assert(fixture != NULL && string_len > TF_TRANSFORM_CANCEL_BYTES_V1 * 3);
    memset(fixture, 0, sizeof(*fixture));
    assert(vector_root != NULL);
    recipes = cJSON_GetObjectItemCaseSensitive(vector_root, "recipes");
    recipe_json = cJSON_Duplicate(
        cJSON_GetObjectItemCaseSensitive(recipes, "numeric_none_none"), 1);
    assert(recipe_json != NULL);
    columns = cJSON_GetObjectItemCaseSensitive(recipe_json, "columns");
    first = cJSON_GetArrayItem(columns, 0);
    second = cJSON_Duplicate(first, 1);
    assert(first != NULL && second != NULL);
    for (size_t i = 0; i < 2; ++i) {
        fixture->ids[i] = (char *)malloc(string_len + 1);
        fixture->names[i] = (char *)malloc(string_len + 1);
        assert(fixture->ids[i] != NULL && fixture->names[i] != NULL);
        memset(fixture->ids[i], 'i', string_len);
        memset(fixture->names[i], 'n', string_len);
        fixture->ids[i][string_len - 1] = (char)('a' + i);
        fixture->names[i][string_len - 1] = (char)('a' + i);
        fixture->ids[i][string_len] = '\0';
        fixture->names[i][string_len] = '\0';
    }
    assert(cJSON_ReplaceItemInObjectCaseSensitive(
        first, "sourceId", cJSON_CreateString(fixture->ids[0])));
    assert(cJSON_ReplaceItemInObjectCaseSensitive(
        second, "sourceId", cJSON_CreateString(fixture->ids[1])));
    assert(cJSON_AddItemToArray(columns, second));
    semantic = cJSON_GetObjectItemCaseSensitive(recipe_json, "semanticLimits");
    assert(cJSON_ReplaceItemInObjectCaseSensitive(
        semantic, "maxOutputColumns", cJSON_CreateNumber(2)));
    serialized = cJSON_PrintUnformatted(recipe_json);
    assert(serialized != NULL);
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    assert(tf_transform_recipe_from_json(
        (const uint8_t *)serialized, strlen(serialized), &limits,
        &fixture->recipe, &error) == TF_TRANSFORM_OK);
    assert(fixture->recipe != NULL && error == NULL);
    for (size_t i = 0; i < 2; ++i) {
        fixture->fields[i].abi_version = 1;
        fixture->fields[i].struct_size = (uint32_t)sizeof(fixture->fields[i]);
        fixture->fields[i].dtype = TF_VIEW_FLOAT64;
        fixture->fields[i].id_utf8 = (const uint8_t *)fixture->ids[i];
        fixture->fields[i].id_bytes = string_len;
        fixture->fields[i].name_utf8 = (const uint8_t *)fixture->names[i];
        fixture->fields[i].name_bytes = string_len;
    }
    fixture->schema.abi_version = 1;
    fixture->schema.struct_size = (uint32_t)sizeof(fixture->schema);
    fixture->schema.column_count = 2;
    fixture->schema.fields = fixture->fields;
    fixture->schema.fields_bytes = sizeof(fixture->fields);
    free(serialized);
    cJSON_Delete(recipe_json);
    cJSON_Delete(vector_root);
    free(vectors);
}

static tf_transform_plan *fit_large_transform_fixture(
    const large_transform_fixture *fixture) {
    const double values[] = {1.0, 2.0};
    tf_column_view_v1 columns[2];
    tf_table_view_v1 table;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_error *error = NULL;
    memset(columns, 0, sizeof(columns));
    for (size_t i = 0; i < 2; ++i) {
        columns[i].abi_version = 1;
        columns[i].struct_size = (uint32_t)sizeof(columns[i]);
        columns[i].data = &values[i];
        columns[i].data_bytes = sizeof(values[i]);
        columns[i].stride_bytes = sizeof(values[i]);
    }
    memset(&table, 0, sizeof(table));
    table.abi_version = 1;
    table.struct_size = (uint32_t)sizeof(table);
    table.row_count = 1;
    table.column_count = 2;
    table.columns = columns;
    table.columns_bytes = sizeof(columns);
    assert(tf_transform_analyzer_create(
        fixture->recipe, &fixture->schema, NULL, &analyzer, &error)
        == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_OK);
    assert(plan != NULL && error == NULL);
    tf_transform_analyzer_destroy(&analyzer);
    return plan;
}

static uint8_t *make_large_valid_tftr(size_t string_len, size_t *out_len) {
    large_transform_fixture fixture;
    tf_transform_plan *plan;
    tf_transform_error *error = NULL;
    uint8_t *bytes = NULL;
    assert(out_len != NULL);
    large_transform_fixture_init(&fixture, string_len);
    plan = fit_large_transform_fixture(&fixture);
    assert(tf_transform_plan_export(
        plan, NULL, &bytes, out_len, &error) == TF_TRANSFORM_OK);
    assert(bytes != NULL && *out_len > string_len * 8);
    tf_transform_plan_destroy(&plan);
    large_transform_fixture_clear(&fixture);
    return bytes;
}

static void test_json_construction_preflight_limits(void) {
    const size_t string_len = TF_TRANSFORM_CANCEL_BYTES_V1 * 3 + 17;
    large_transform_fixture fixture;
    tf_transform_plan *plan;
    tf_transform_limits_v1 limits;
    tf_transform_error *error = NULL;
    uint8_t *bytes = NULL;
    size_t bytes_len = 0;

    large_transform_fixture_init(&fixture, string_len);
    plan = fit_large_transform_fixture(&fixture);
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);

    limits.max_string_bytes = string_len - 1;
    assert(tf_transform_plan_schema_json(
        plan, TF_TRANSFORM_SCHEMA_INPUT, &limits,
        &bytes, &bytes_len, &error) == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(bytes == NULL && bytes_len == 0 && error != NULL);
    tf_transform_error_destroy(&error);
    assert(tf_transform_plan_export(
        plan, &limits, &bytes, &bytes_len, &error)
        == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(bytes == NULL && bytes_len == 0 && error != NULL);
    tf_transform_error_destroy(&error);

    limits.max_string_bytes = string_len;
    assert(tf_transform_plan_schema_json(
        plan, TF_TRANSFORM_SCHEMA_INPUT, &limits,
        &bytes, &bytes_len, &error) == TF_TRANSFORM_OK);
    assert(bytes != NULL && bytes_len > string_len && error == NULL);
    tf_transform_bytes_free(&bytes, &bytes_len);
    assert(tf_transform_plan_export(
        plan, &limits, &bytes, &bytes_len, &error) == TF_TRANSFORM_OK);
    assert(bytes != NULL && bytes_len > string_len * 8 && error == NULL);
    tf_transform_bytes_free(&bytes, &bytes_len);

    limits.max_input_columns = 2;
    limits.max_output_columns = 1;
    assert(tf_transform_plan_schema_json(
        plan, TF_TRANSFORM_SCHEMA_INPUT, &limits,
        &bytes, &bytes_len, &error) == TF_TRANSFORM_OK);
    tf_transform_bytes_free(&bytes, &bytes_len);
    assert(tf_transform_plan_schema_json(
        plan, TF_TRANSFORM_SCHEMA_OUTPUT, &limits,
        &bytes, &bytes_len, &error) == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(bytes == NULL && bytes_len == 0);
    tf_transform_error_destroy(&error);

    limits.max_input_columns = 1;
    limits.max_output_columns = 2;
    assert(tf_transform_plan_schema_json(
        plan, TF_TRANSFORM_SCHEMA_OUTPUT, &limits,
        &bytes, &bytes_len, &error) == TF_TRANSFORM_OK);
    tf_transform_bytes_free(&bytes, &bytes_len);

    tf_transform_plan_destroy(&plan);
    large_transform_fixture_clear(&fixture);
}

static void test_long_schema_runtime_cancellation(void) {
    const size_t string_len = TF_TRANSFORM_CANCEL_BYTES_V1 * 3 + 17;
    large_transform_fixture fixture;
    tf_transform_runtime_v1 runtime;
    cancel_counter counter = {0, 0};
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_apply *apply = NULL;
    tf_transform_plan *plan;
    tf_transform_error *error = NULL;
    size_t total_calls;
    large_transform_fixture_init(&fixture, string_len);
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.cancel = cancel_on_call;
    runtime.cancel_user = &counter;

    assert(tf_transform_analyzer_create(
        fixture.recipe, &fixture.schema, &runtime, &analyzer, &error)
        == TF_TRANSFORM_OK);
    assert(analyzer != NULL && error == NULL);
    total_calls = counter.calls;
    assert(total_calls > 30);
    tf_transform_analyzer_destroy(&analyzer);
    for (size_t cancel_at = 1; cancel_at <= total_calls; ++cancel_at) {
        counter.calls = 0;
        counter.cancel_at = cancel_at;
        assert(tf_transform_analyzer_create(
            fixture.recipe, &fixture.schema, &runtime, &analyzer, &error)
            == TF_TRANSFORM_CANCELLED);
        assert(analyzer == NULL && error != NULL
               && tf_transform_error_get_code(error) == TF_TRANSFORM_CANCELLED
               && counter.calls >= cancel_at);
        tf_transform_error_destroy(&error);
    }

    counter.calls = 0;
    counter.cancel_at = 0;
    plan = fit_large_transform_fixture(&fixture);
    assert(tf_transform_apply_create(
        plan, &fixture.schema, &runtime, &apply, &error) == TF_TRANSFORM_OK);
    assert(apply != NULL && error == NULL);
    total_calls = counter.calls;
    assert(total_calls > 20);
    tf_transform_apply_destroy(&apply);
    for (size_t cancel_at = 1; cancel_at <= total_calls; ++cancel_at) {
        counter.calls = 0;
        counter.cancel_at = cancel_at;
        assert(tf_transform_apply_create(
            plan, &fixture.schema, &runtime, &apply, &error)
            == TF_TRANSFORM_CANCELLED);
        assert(apply == NULL && error != NULL
               && tf_transform_error_get_code(error) == TF_TRANSFORM_CANCELLED
               && counter.calls >= cancel_at);
        tf_transform_error_destroy(&error);
    }
    tf_transform_plan_destroy(&plan);
    large_transform_fixture_clear(&fixture);
}

static void test_valid_import_cancellation_sweep(void) {
    size_t tftr_len = 0;
    uint8_t *tftr = make_large_valid_tftr(
        TF_TRANSFORM_CANCEL_BYTES_V1 * 3 + 17, &tftr_len);
    tf_transform_runtime_v1 runtime;
    tf_transform_plan *plan = NULL;
    tf_transform_error *error = NULL;
    cancel_counter counter = {0, 0};
    size_t total_calls;
    uint64_t exact_allocations;
    uint64_t exact_peak;
    tf_transform_limits_v1 limits;
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.cancel = cancel_on_call;
    runtime.cancel_user = &counter;
    assert(tf_transform_plan_import(
        tftr, tftr_len, &runtime, &plan, &error) == TF_TRANSFORM_OK);
    assert(plan != NULL && error == NULL);
    total_calls = counter.calls;
    exact_allocations = plan->import_allocation_count;
    exact_peak = plan->import_peak_resident_bytes;
    assert(total_calls > 20);
    tf_transform_plan_destroy(&plan);
    for (size_t cancel_at = 1; cancel_at <= total_calls; ++cancel_at) {
        counter.calls = 0;
        counter.cancel_at = cancel_at;
        assert(tf_transform_plan_import(
            tftr, tftr_len, &runtime, &plan, &error)
            == TF_TRANSFORM_CANCELLED);
        assert(plan == NULL && error != NULL
               && tf_transform_error_get_code(error) == TF_TRANSFORM_CANCELLED
               && counter.calls >= cancel_at);
        tf_transform_error_destroy(&error);
    }
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    runtime.cancel = NULL;
    runtime.cancel_user = NULL;
    runtime.limits = &limits;
    limits.max_allocations_per_session = exact_allocations - 1;
    assert(tf_transform_plan_import(
        tftr, tftr_len, &runtime, &plan, &error)
        == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(plan == NULL);
    tf_transform_error_destroy(&error);
    limits.max_allocations_per_session = exact_allocations;
    assert(tf_transform_plan_import(
        tftr, tftr_len, &runtime, &plan, &error) == TF_TRANSFORM_OK);
    assert(plan != NULL && plan->import_allocation_count == exact_allocations);
    tf_transform_plan_destroy(&plan);
    limits.max_resident_state_bytes = exact_peak - 1;
    assert(tf_transform_plan_import(
        tftr, tftr_len, &runtime, &plan, &error)
        == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(plan == NULL);
    tf_transform_error_destroy(&error);
    limits.max_resident_state_bytes = exact_peak;
    assert(tf_transform_plan_import(
        tftr, tftr_len, &runtime, &plan, &error) == TF_TRANSFORM_OK);
    assert(plan != NULL && plan->import_peak_resident_bytes == exact_peak);
    tf_transform_plan_destroy(&plan);
    free(tftr);
}

static void test_numeric_failure_is_terminal(void) {
    const double infinity[] = {
        tf_transform_double_from_bits(UINT64_C(0x7ff0000000000000))
    };
    const double valid[] = {1.0};
    tf_transform_recipe *recipe = load_vector_recipe("numeric_none_none");
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_error *error = NULL;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
    assert(tf_transform_analyzer_create(
        recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
    make_f64_table(infinity, 1, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_NUMERIC_DOMAIN);
    tf_transform_error_destroy(&error);
    make_f64_table(valid, 1, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_INVALID_STATE);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);
    tf_transform_recipe_destroy(&recipe);
}

static void test_numeric_vector_semantics(void) {
    const double missing =
        tf_transform_double_from_bits(UINT64_C(0x7ff8000000000042));
    const double mean_rows[] = {1.0, missing, 3.0, missing};
    const double mean_apply[] = {1.0, missing, 3.0};
    const uint64_t mean_expected[] = {
        UINT64_C(0xbff6a09e667f3bcc),
        UINT64_C(0x0000000000000000),
        UINT64_C(0x3ff6a09e667f3bcc)
    };
    const double minmax_rows[] = {-2.0, missing, 6.0};
    const uint64_t minmax_expected[] = {
        UINT64_C(0x0000000000000000),
        UINT64_C(0x3fd0000000000000),
        UINT64_C(0x3ff0000000000000)
    };
    const double all_missing[] = {missing, missing};
    const double all_missing_apply[] = {missing, 2.0};
    const uint64_t all_missing_expected[] = {
        UINT64_C(0x0000000000000000),
        UINT64_C(0x4000000000000000)
    };
    const double none_rows[] = {1.0, missing};
    const double none_apply[] = {missing, 2.0};
    const uint64_t none_expected[] = {
        UINT64_C(0x7ff8000000000000),
        UINT64_C(0x4000000000000000)
    };
    tf_transform_plan *one_chunk = fit_f64_plan(
        "numeric_mean_standard", mean_rows, 4, 4);
    tf_transform_plan *split = fit_f64_plan(
        "numeric_mean_standard", mean_rows, 4, 1);
    tf_transform_plan *plan;
    tf_transform_error *error = NULL;
    uint8_t *left = NULL;
    size_t left_len = 0;
    uint8_t *right = NULL;
    size_t right_len = 0;

    assert(one_chunk->states[0].value.numeric.location == 2.0);
    assert(tf_transform_double_bits(one_chunk->states[0].value.numeric.scale)
           == UINT64_C(0x3fe6a09e667f3bcd));
    assert(tf_transform_plan_export(
        one_chunk, NULL, &left, &left_len, &error) == TF_TRANSFORM_OK);
    assert(tf_transform_plan_export(
        split, NULL, &right, &right_len, &error) == TF_TRANSFORM_OK);
    assert(left_len == right_len && memcmp(left, right, left_len) == 0);
    tf_transform_bytes_free(&left, &left_len);
    tf_transform_bytes_free(&right, &right_len);
    assert_apply_bits(one_chunk, mean_apply, 3, mean_expected);
    tf_transform_plan_destroy(&one_chunk);
    tf_transform_plan_destroy(&split);

    plan = fit_f64_plan("numeric_zero_minmax", minmax_rows, 3, 1);
    assert(tf_transform_double_bits(plan->states[0].value.numeric.location)
           == UINT64_C(0xc000000000000000));
    assert(tf_transform_double_bits(plan->states[0].value.numeric.scale)
           == UINT64_C(0x4020000000000000));
    assert_apply_bits(plan, minmax_rows, 3, minmax_expected);
    tf_transform_plan_destroy(&plan);

    plan = fit_f64_plan("numeric_mean_standard", all_missing, 2, 1);
    assert(tf_transform_double_bits(plan->states[0].value.numeric.location) == 0);
    assert(tf_transform_double_bits(plan->states[0].value.numeric.scale)
           == UINT64_C(0x3ff0000000000000));
    assert_apply_bits(plan, all_missing_apply, 2, all_missing_expected);
    tf_transform_plan_destroy(&plan);

    plan = fit_f64_plan("numeric_none_none", none_rows, 2, 1);
    assert_apply_bits(plan, none_apply, 2, none_expected);
    tf_transform_plan_destroy(&plan);
}

static void test_categorical_mode_semantics(void) {
    const double missing = tf_transform_double_from_bits(
        UINT64_C(0x7ff8000000000042));
    const double values[] = {-0.0, 0.0, 1.0, 3.0, 1.0, 3.0, missing};
    const double permuted[] = {3.0, 1.0, missing, 0.0, 3.0, -0.0, 1.0};
    const double apply_values[] = {-0.0, 0.0, missing, 1.0, 3.0};
    const uint64_t expected[] = {
        UINT64_C(0x8000000000000000), UINT64_C(0), UINT64_C(0),
        UINT64_C(0x3ff0000000000000), UINT64_C(0x4008000000000000)
    };
    const double unknown[] = {2.0};
    const double known[] = {1.0};
    tf_transform_plan *one_chunk = fit_f64_plan(
        "categorical_mode_none", values, 7, 7);
    tf_transform_plan *split = fit_f64_plan(
        "categorical_mode_none", values, 7, 2);
    tf_transform_plan *reordered = fit_f64_plan(
        "categorical_mode_none", permuted, 7, 3);
    tf_transform_plan *imported = NULL;
    tf_transform_error *error = NULL;
    uint8_t *reference = NULL;
    size_t reference_len = 0;
    uint8_t *candidate = NULL;
    size_t candidate_len = 0;
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    tf_transform_apply *apply = NULL;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    tf_owned_dense_v1 dense;

    assert(one_chunk->states[0].kind == TF_TRANSFORM_KIND_CATEGORICAL);
    assert(one_chunk->states[0].value.categorical.category_count == 3);
    assert(one_chunk->states[0].value.categorical.categories[0].bits == 0);
    assert(one_chunk->states[0].value.categorical.impute_bits == 0);
    assert(tf_transform_plan_export(
        one_chunk, NULL, &reference, &reference_len, &error) == TF_TRANSFORM_OK);
    assert(tf_transform_plan_export(
        split, NULL, &candidate, &candidate_len, &error) == TF_TRANSFORM_OK);
    assert(candidate_len == reference_len
           && memcmp(candidate, reference, reference_len) == 0);
    tf_transform_bytes_free(&candidate, &candidate_len);
    assert(tf_transform_plan_export(
        reordered, NULL, &candidate, &candidate_len, &error) == TF_TRANSFORM_OK);
    assert(candidate_len == reference_len
           && memcmp(candidate, reference, reference_len) == 0);
    tf_transform_bytes_free(&candidate, &candidate_len);
    assert(tf_transform_plan_import(
        reference, reference_len, NULL, &imported, &error) == TF_TRANSFORM_OK);
    assert(imported != NULL
           && imported->states[0].kind == TF_TRANSFORM_KIND_CATEGORICAL
           && imported->states[0].value.categorical.category_count == 3);
    assert_apply_bits(imported, apply_values, 5, expected);

    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
    assert(tf_transform_apply_create(
        imported, &schema, NULL, &apply, &error) == TF_TRANSFORM_OK);
    make_f64_table(unknown, 1, &column, &table);
    assert(tf_transform_apply_run(apply, &table, &dense, &error)
           == TF_TRANSFORM_UNKNOWN_CATEGORY);
    assert(error != NULL && tf_transform_error_get_code(error)
           == TF_TRANSFORM_UNKNOWN_CATEGORY);
    tf_transform_error_destroy(&error);
    make_f64_table(known, 1, &column, &table);
    assert(tf_transform_apply_run(apply, &table, &dense, &error)
           == TF_TRANSFORM_INVALID_STATE);
    tf_transform_error_destroy(&error);
    tf_transform_apply_destroy(&apply);

    tf_transform_bytes_free(&reference, &reference_len);
    tf_transform_plan_destroy(&imported);
    tf_transform_plan_destroy(&reordered);
    tf_transform_plan_destroy(&split);
    tf_transform_plan_destroy(&one_chunk);
}

static void test_categorical_subnormal_fp_environment(void) {
    const uint32_t f32_bits[] = {
        UINT32_C(0x00000001), UINT32_C(0x80000001), UINT32_C(0x00000000)
    };
    const uint32_t f32_apply_bits[] = {
        UINT32_C(0x7fc00001), UINT32_C(0x80000001),
        UINT32_C(0x00000000), UINT32_C(0x00000001)
    };
    const uint64_t f64_bits[] = {
        UINT64_C(0x0000000000000001), UINT64_C(0x8000000000000001),
        UINT64_C(0x0000000000000000)
    };
    const uint64_t f64_apply_bits[] = {
        UINT64_C(0x7ff8000000000001), UINT64_C(0x8000000000000001),
        UINT64_C(0x0000000000000000), UINT64_C(0x0000000000000001)
    };
    float f32_values[3];
    float f32_apply_values[4];
    double f64_values[3];
    double f64_apply_values[4];
    uint64_t f32_expected[4];
    const uint64_t f64_expected[] = {
        UINT64_C(0x8000000000000001), UINT64_C(0x8000000000000001),
        UINT64_C(0x0000000000000000), UINT64_C(0x0000000000000001)
    };
    tf_transform_plan *plan32;
    tf_transform_plan *plan64;
    tf_transform_plan *imported32 = NULL;
    tf_transform_plan *imported64 = NULL;
    tf_transform_apply *apply32 = NULL;
    tf_transform_error *error = NULL;
    tf_field_view_v1 field32;
    tf_schema_view_v1 schema32;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    tf_owned_dense_v1 dense;
    uint8_t *baseline32 = NULL;
    size_t baseline32_len = 0;
    uint8_t *baseline64 = NULL;
    size_t baseline64_len = 0;
    uint8_t *candidate = NULL;
    size_t candidate_len = 0;
#if !defined(__wasm__)
    int original_rounding = fegetround();
#if defined(__i386__) || defined(__x86_64__)
    unsigned int original_mxcsr = _mm_getcsr();
    unsigned int caller_mxcsr;
#endif
#endif

    memcpy(f32_values, f32_bits, sizeof(f32_values));
    memcpy(f32_apply_values, f32_apply_bits, sizeof(f32_apply_values));
    for (size_t i = 0; i < 3; ++i)
        f64_values[i] = tf_transform_double_from_bits(f64_bits[i]);
    for (size_t i = 0; i < 4; ++i) {
        f64_apply_values[i] = tf_transform_double_from_bits(f64_apply_bits[i]);
        f32_expected[i] = i == 0
            ? tf_transform_double_bits((double)f32_apply_values[1])
            : tf_transform_double_bits((double)f32_apply_values[i]);
    }
    plan32 = fit_f32_plan("categorical_mode_none", f32_values, 3);
    plan64 = fit_f64_plan("categorical_mode_none", f64_values, 3, 3);
    assert(plan32->states[0].value.categorical.categories[0].bits
           == UINT32_C(0x80000001));
    assert(plan32->states[0].value.categorical.categories[1].bits == 0);
    assert(plan32->states[0].value.categorical.categories[2].bits == 1);
    assert(plan64->states[0].value.categorical.categories[0].bits
           == UINT64_C(0x8000000000000001));
    assert(tf_transform_plan_export(
        plan32, NULL, &baseline32, &baseline32_len, &error) == TF_TRANSFORM_OK);
    assert(tf_transform_plan_export(
        plan64, NULL, &baseline64, &baseline64_len, &error) == TF_TRANSFORM_OK);

#if !defined(__wasm__)
    assert(original_rounding != -1);
    assert(fesetround(FE_DOWNWARD) == 0);
#if defined(__i386__) || defined(__x86_64__)
    caller_mxcsr = _mm_getcsr() | (1u << 15) | (1u << 6);
    _mm_setcsr(caller_mxcsr);
    assert(_mm_getcsr() == caller_mxcsr);
#endif
#endif
    assert(tf_transform_plan_export(
        plan32, NULL, &candidate, &candidate_len, &error) == TF_TRANSFORM_OK);
    assert(candidate_len == baseline32_len
           && memcmp(candidate, baseline32, candidate_len) == 0);
    assert(tf_transform_plan_import(
        candidate, candidate_len, NULL, &imported32, &error) == TF_TRANSFORM_OK);
    tf_transform_bytes_free(&candidate, &candidate_len);
    assert(tf_transform_plan_export(
        plan64, NULL, &candidate, &candidate_len, &error) == TF_TRANSFORM_OK);
    assert(candidate_len == baseline64_len
           && memcmp(candidate, baseline64, candidate_len) == 0);
    assert(tf_transform_plan_import(
        candidate, candidate_len, NULL, &imported64, &error) == TF_TRANSFORM_OK);
    tf_transform_bytes_free(&candidate, &candidate_len);

    make_x0_schema(TF_VIEW_FLOAT32, &field32, &schema32);
    assert(tf_transform_apply_create(
        imported32, &schema32, NULL, &apply32, &error) == TF_TRANSFORM_OK);
    make_f32_table(f32_apply_values, 4, &column, &table);
    assert(tf_transform_apply_run(apply32, &table, &dense, &error)
           == TF_TRANSFORM_OK);
    for (size_t i = 0; i < 4; ++i)
        assert(tf_transform_double_bits(((double *)dense.data)[i])
               == f32_expected[i]);
    tf_owned_dense_free(&dense);
    assert_apply_bits(imported64, f64_apply_values, 4, f64_expected);
#if !defined(__wasm__)
    assert(fegetround() == FE_DOWNWARD);
#if defined(__i386__) || defined(__x86_64__)
    assert(_mm_getcsr() == caller_mxcsr);
#endif
    assert(fesetround(original_rounding) == 0);
#if defined(__i386__) || defined(__x86_64__)
    _mm_setcsr(original_mxcsr);
    assert(_mm_getcsr() == original_mxcsr);
#endif
#endif

    tf_transform_apply_destroy(&apply32);
    tf_transform_bytes_free(&baseline32, &baseline32_len);
    tf_transform_bytes_free(&baseline64, &baseline64_len);
    tf_transform_plan_destroy(&imported32);
    tf_transform_plan_destroy(&imported64);
    tf_transform_plan_destroy(&plan32);
    tf_transform_plan_destroy(&plan64);
}

static void test_categorical_mode_resources_and_cancellation(void) {
    const size_t category_count = 8192;
    double *values = (double *)malloc(category_count * sizeof(*values));
    tf_transform_recipe *recipe = load_vector_recipe("categorical_mode_none");
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_error *error = NULL;
    tf_transform_limits_v1 limits;
    tf_transform_runtime_v1 runtime;
    uint64_t base_resident;
    uint64_t old_table_bytes;
    uint64_t new_table_bytes;
    cancel_counter counter = {0, 0};
    size_t finalize_polls;

    assert(values != NULL);
    for (size_t i = 0; i < category_count; ++i) values[i] = (double)i;
    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);

    assert(tf_transform_analyzer_create(
        recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
    base_resident = analyzer->resident_state_bytes;
    make_f64_table(values, 8, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    old_table_bytes = analyzer->resident_state_bytes - base_resident;
    make_f64_table(values + 8, 1, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    new_table_bytes = analyzer->resident_state_bytes - base_resident;
    assert(old_table_bytes > 0 && new_table_bytes > old_table_bytes);
    tf_transform_analyzer_destroy(&analyzer);

    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.limits = &limits;
    limits.max_resident_state_bytes = base_resident
        + old_table_bytes + new_table_bytes - 1;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error) == TF_TRANSFORM_OK);
    make_f64_table(values, 8, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    make_f64_table(values + 8, 1, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(analyzer->state == TF_ANALYZER_FAILED);
    tf_transform_error_destroy(&error);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_INVALID_STATE);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);

    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    limits.max_categories_per_column = 8;
    runtime.limits = &limits;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error) == TF_TRANSFORM_OK);
    make_f64_table(values, 9, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_RESOURCE_LIMIT);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);

    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    runtime.limits = &limits;
    runtime.cancel = cancel_on_call;
    runtime.cancel_user = &counter;
    counter.calls = 0;
    counter.cancel_at = 0;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error) == TF_TRANSFORM_OK);
    counter.cancel_at = counter.calls + 10;
    make_f64_table(values, category_count, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_CANCELLED);
    assert(analyzer->state == TF_ANALYZER_FAILED);
    tf_transform_error_destroy(&error);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_INVALID_STATE);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);

    counter.calls = 0;
    counter.cancel_at = 0;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error) == TF_TRANSFORM_OK);
    make_f64_table(values, category_count, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    {
        size_t before = counter.calls;
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_OK);
        finalize_polls = counter.calls - before;
        assert(finalize_polls > 10);
    }
    tf_transform_plan_destroy(&plan);
    tf_transform_analyzer_destroy(&analyzer);

    counter.calls = 0;
    counter.cancel_at = 0;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error) == TF_TRANSFORM_OK);
    make_f64_table(values, category_count, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    counter.cancel_at = counter.calls + finalize_polls / 2;
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_CANCELLED);
    assert(plan == NULL && analyzer->state == TF_ANALYZER_FAILED);
    tf_transform_error_destroy(&error);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_INVALID_STATE);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);

    tf_transform_recipe_destroy(&recipe);
    free(values);
}

static void test_kind_inference_release_and_domain(void) {
    const double candidate_values[] = {0.0, 1.0};
    const double extra_value[] = {2.0};
    const double infinity[] = {
        tf_transform_double_from_bits(UINT64_C(0x7ff0000000000000))
    };
    tf_transform_recipe *recipe = load_vector_recipe("infer_two_categories");
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_error *error = NULL;
    tf_transform_limits_v1 limits;
    tf_transform_runtime_v1 runtime;
    uint64_t base_resident;
    uint64_t candidate_resident;
    uint64_t candidate_allocations;

    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.limits = &limits;

    limits.max_categories_per_column = 1;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error)
        == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(analyzer == NULL);
    tf_transform_error_destroy(&error);

    limits.max_categories_per_column = 2;
    limits.max_total_categories = 1;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error)
        == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(analyzer == NULL);
    tf_transform_error_destroy(&error);

    limits.max_total_categories = 2;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error) == TF_TRANSFORM_OK);
    base_resident = analyzer->resident_state_bytes;
    make_f64_table(candidate_values, 2, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    candidate_resident = analyzer->resident_state_bytes;
    candidate_allocations = analyzer->allocation_count;
    assert(candidate_resident > base_resident
           && analyzer->total_categories == 2);

    make_f64_table(extra_value, 1, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    assert(analyzer->resident_state_bytes == base_resident
           && analyzer->total_categories == 0
           && analyzer->allocation_count == candidate_allocations);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_OK);
    assert(plan != NULL && plan->states[0].kind == TF_TRANSFORM_KIND_NUMERIC
           && plan->states[0].value.numeric.has_impute_value
           && tf_transform_double_bits(
                plan->states[0].value.numeric.impute_value)
                == UINT64_C(0x3ff0000000000000));
    tf_transform_plan_destroy(&plan);
    tf_transform_analyzer_destroy(&analyzer);

    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error) == TF_TRANSFORM_OK);
    make_f64_table(infinity, 1, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_NUMERIC_DOMAIN);
    assert(analyzer->state == TF_ANALYZER_FAILED);
    tf_transform_error_destroy(&error);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_INVALID_STATE);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);
    tf_transform_recipe_destroy(&recipe);
}

static void test_wide_categorical_lifecycle_cancellation(void) {
    const size_t column_count = TF_TRANSFORM_CANCEL_ITERS_V1 * 3 + 1;
    wide_categorical_fixture fixture;
    tf_transform_runtime_v1 runtime;
    cancel_counter counter = {0, 0};
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_plan *cancelled_plan = NULL;
    tf_transform_apply *apply = NULL;
    tf_transform_error *error = NULL;
    tf_owned_dense_v1 dense;
    size_t create_polls;
    size_t push_polls;
    size_t finalize_polls;
    size_t apply_polls;
    size_t before;

    wide_categorical_fixture_init(&fixture, column_count);
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.cancel = cancel_on_call;
    runtime.cancel_user = &counter;

    assert(tf_transform_analyzer_create(
        fixture.recipe, &fixture.schema, &runtime, &analyzer, &error)
        == TF_TRANSFORM_OK);
    create_polls = counter.calls;
    assert(create_polls > 3);
    before = counter.calls;
    assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
           == TF_TRANSFORM_OK);
    push_polls = counter.calls - before;
    assert(push_polls > 3);
    before = counter.calls;
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_OK);
    finalize_polls = counter.calls - before;
    assert(finalize_polls > 3 && plan != NULL);
    tf_transform_analyzer_destroy(&analyzer);
    assert(tf_transform_apply_create(
        plan, &fixture.schema, &runtime, &apply, &error) == TF_TRANSFORM_OK);
    before = counter.calls;
    assert(tf_transform_apply_run(apply, &fixture.table, &dense, &error)
           == TF_TRANSFORM_OK);
    apply_polls = counter.calls - before;
    assert(apply_polls > 3);
    tf_owned_dense_free(&dense);
    tf_transform_apply_destroy(&apply);

    counter.calls = 0;
    counter.cancel_at = create_polls - 1;
    assert(tf_transform_analyzer_create(
        fixture.recipe, &fixture.schema, &runtime, &analyzer, &error)
        == TF_TRANSFORM_CANCELLED);
    assert(analyzer == NULL);
    tf_transform_error_destroy(&error);

    counter.calls = 0;
    counter.cancel_at = 0;
    assert(tf_transform_analyzer_create(
        fixture.recipe, &fixture.schema, &runtime, &analyzer, &error)
        == TF_TRANSFORM_OK);
    before = counter.calls;
    counter.cancel_at = before + push_polls - 1;
    assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
           == TF_TRANSFORM_CANCELLED);
    assert(analyzer->state == TF_ANALYZER_FAILED);
    tf_transform_error_destroy(&error);
    assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
           == TF_TRANSFORM_INVALID_STATE);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);

    counter.calls = 0;
    counter.cancel_at = 0;
    assert(tf_transform_analyzer_create(
        fixture.recipe, &fixture.schema, &runtime, &analyzer, &error)
        == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
           == TF_TRANSFORM_OK);
    before = counter.calls;
    counter.cancel_at = before + finalize_polls - 1;
    assert(tf_transform_analyzer_finalize(analyzer, &cancelled_plan, &error)
           == TF_TRANSFORM_CANCELLED);
    assert(cancelled_plan == NULL && analyzer->state == TF_ANALYZER_FAILED);
    tf_transform_error_destroy(&error);
    assert(tf_transform_analyzer_finalize(analyzer, &cancelled_plan, &error)
           == TF_TRANSFORM_INVALID_STATE);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);

    counter.calls = 0;
    counter.cancel_at = 0;
    assert(tf_transform_apply_create(
        plan, &fixture.schema, &runtime, &apply, &error) == TF_TRANSFORM_OK);
    before = counter.calls;
    counter.cancel_at = before + apply_polls - 1;
    assert(tf_transform_apply_run(apply, &fixture.table, &dense, &error)
           == TF_TRANSFORM_CANCELLED);
    assert(apply->state == TF_APPLY_FAILED && dense.data == NULL);
    tf_transform_error_destroy(&error);
    assert(tf_transform_apply_run(apply, &fixture.table, &dense, &error)
           == TF_TRANSFORM_INVALID_STATE);
    tf_transform_error_destroy(&error);
    tf_transform_apply_destroy(&apply);

    tf_transform_plan_destroy(&plan);
    wide_categorical_fixture_clear(&fixture);
}

static void test_median_blocks_limits_and_cancellation(void) {
    const size_t row_count = 20001;
    double *values = (double *)malloc(row_count * sizeof(*values));
    tf_transform_recipe *recipe = load_vector_recipe("numeric_median_none");
    tf_transform_plan *one_chunk = NULL;
    tf_transform_plan *split = NULL;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_error *error = NULL;
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    tf_transform_limits_v1 limits;
    tf_transform_runtime_v1 runtime;
    median_sort_cancel_probe probe = {0};
    uint8_t *left = NULL;
    size_t left_len = 0;
    uint8_t *right = NULL;
    size_t right_len = 0;
    uint64_t base_resident;
    uint64_t base_allocations;

    assert(values != NULL);
    for (size_t i = 0; i < row_count; ++i)
        values[i] = (double)(row_count - i - 1);
    one_chunk = fit_f64_plan(
        "numeric_median_none", values, row_count, row_count);
    split = fit_f64_plan(
        "numeric_median_none", values, row_count, 8191);
    assert(tf_transform_double_bits(one_chunk->states[0].value.numeric.impute_value)
           == tf_transform_double_bits(10000.0));
    assert(tf_transform_plan_export(
        one_chunk, NULL, &left, &left_len, &error) == TF_TRANSFORM_OK);
    assert(tf_transform_plan_export(
        split, NULL, &right, &right_len, &error) == TF_TRANSFORM_OK);
    assert(left_len == right_len && memcmp(left, right, left_len) == 0);
    tf_transform_bytes_free(&left, &left_len);
    tf_transform_bytes_free(&right, &right_len);
    tf_transform_plan_destroy(&one_chunk);
    tf_transform_plan_destroy(&split);

    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
    assert(tf_transform_analyzer_create(
        recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
    base_resident = analyzer->resident_state_bytes;
    base_allocations = analyzer->allocation_count;
    assert(analyzer->median_stores != NULL && base_allocations == 8);
    tf_transform_analyzer_destroy(&analyzer);

    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.limits = &limits;
    limits.max_allocations_per_session = base_allocations + 1;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error) == TF_TRANSFORM_OK);
    make_f64_table(values, 1, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(analyzer->state == TF_ANALYZER_FAILED);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);

    limits.max_allocations_per_session = UINT64_MAX;
    limits.max_resident_state_bytes = base_resident
        + 4 * sizeof(double *) + TF_TRANSFORM_CANCEL_BYTES_V1 - 1;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error) == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(analyzer->state == TF_ANALYZER_FAILED);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);

    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    probe.original_first = values[0];
    runtime.limits = &limits;
    runtime.cancel = cancel_during_median_sort;
    runtime.cancel_user = &probe;
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error) == TF_TRANSFORM_OK);
    probe.analyzer = analyzer;
    make_f64_table(values, row_count, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    probe.armed = 1;
    assert(tf_transform_analyzer_finalize(analyzer, &one_chunk, &error)
           == TF_TRANSFORM_CANCELLED);
    assert(one_chunk == NULL && probe.observed_sort && probe.calls > 1);
    tf_transform_error_destroy(&error);
    assert(tf_transform_analyzer_finalize(analyzer, &one_chunk, &error)
           == TF_TRANSFORM_INVALID_STATE);
    assert(one_chunk == NULL);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);
    tf_transform_recipe_destroy(&recipe);
    free(values);
}

static tf_transform_code parse_recipe_entry(
    const cJSON *recipes, const char *name,
    tf_transform_recipe **out, tf_transform_error **error) {
    const cJSON *entry = cJSON_GetObjectItemCaseSensitive(recipes, name);
    char *serialized;
    tf_transform_limits_v1 limits;
    tf_transform_code code;
    assert(entry != NULL);
    serialized = cJSON_PrintUnformatted(entry);
    assert(serialized != NULL);
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    code = tf_transform_recipe_from_json(
        (const uint8_t *)serialized, strlen(serialized), &limits,
        out, error);
    free(serialized);
    return code;
}

static double *decode_single_column_rows(
    const cJSON *rows, size_t *row_count) {
    int count = cJSON_GetArraySize(rows);
    double *values;
    assert(cJSON_IsArray(rows) && count >= 0 && row_count != NULL);
    values = count ? (double *)malloc((size_t)count * sizeof(*values)) : NULL;
    assert(count == 0 || values != NULL);
    for (int i = 0; i < count; ++i) {
        const cJSON *row = cJSON_GetArrayItem(rows, i);
        const cJSON *cell;
        assert(cJSON_IsArray(row) && cJSON_GetArraySize(row) == 1);
        cell = cJSON_GetArrayItem(row, 0);
        if (cJSON_IsNull(cell))
            values[i] = tf_transform_double_from_bits(
                UINT64_C(0x7ff8000000000000));
        else {
            assert(cJSON_IsString(cell));
            values[i] = tf_transform_double_from_bits(
                parse_hex64(cell->valuestring));
        }
    }
    *row_count = (size_t)count;
    return values;
}

static void execute_semantic_case(
    const cJSON *entry, tf_transform_recipe *recipe) {
    const cJSON *analyze = cJSON_GetObjectItemCaseSensitive(entry, "analyze");
    const cJSON *apply = cJSON_GetObjectItemCaseSensitive(entry, "apply");
    const cJSON *expected_plan = cJSON_GetObjectItemCaseSensitive(
        entry, "expectedPlan");
    const cJSON *expected_error = cJSON_GetObjectItemCaseSensitive(
        entry, "expectedError");
    const cJSON *input_dtype_value = cJSON_GetObjectItemCaseSensitive(
        entry, "inputDtype");
    int input_f32 = input_dtype_value
        && strcmp(input_dtype_value->valuestring, "float32") == 0;
    const cJSON *splits = cJSON_GetObjectItemCaseSensitive(analyze, "chunkSplits");
    size_t analyze_count;
    size_t apply_count;
    double *analyze_values = decode_single_column_rows(
        cJSON_GetObjectItemCaseSensitive(analyze, "rows"), &analyze_count);
    double *apply_values = decode_single_column_rows(
        cJSON_GetObjectItemCaseSensitive(apply, "rows"), &apply_count);
    float *analyze_values_f32 = input_f32 && analyze_count
        ? (float *)malloc(analyze_count * sizeof(*analyze_values_f32)) : NULL;
    float *apply_values_f32 = input_f32 && apply_count
        ? (float *)malloc(apply_count * sizeof(*apply_values_f32)) : NULL;
    uint8_t *reference_bytes = NULL;
    size_t reference_len = 0;
    int split_count = cJSON_GetArraySize(splits);
    assert(!input_f32 || analyze_count == 0 || analyze_values_f32 != NULL);
    assert(!input_f32 || apply_count == 0 || apply_values_f32 != NULL);
    for (size_t i = 0; input_f32 && i < analyze_count; ++i)
        analyze_values_f32[i] = (float)analyze_values[i];
    for (size_t i = 0; input_f32 && i < apply_count; ++i)
        apply_values_f32[i] = (float)apply_values[i];
    assert(split_count > 0);
    for (int split_index = 0; split_index < split_count; ++split_index) {
        const cJSON *split = cJSON_GetArrayItem(splits, split_index);
        tf_field_view_v1 field;
        tf_schema_view_v1 schema;
        tf_transform_analyzer *analyzer = NULL;
        tf_transform_plan *plan = NULL;
        tf_transform_apply *apply_session = NULL;
        tf_transform_error *error = NULL;
        tf_transform_code code;
        size_t offset = 0;
        tf_column_view_v1 column;
        tf_table_view_v1 table;
        tf_owned_dense_v1 dense;
        uint8_t *plan_bytes = NULL;
        size_t plan_len = 0;
        int analyze_failed = 0;
        make_x0_schema(input_f32 ? TF_VIEW_FLOAT32 : TF_VIEW_FLOAT64,
                       &field, &schema);
        assert(tf_transform_analyzer_create(
            recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
        for (int chunk_index = 0;
             chunk_index < cJSON_GetArraySize(split); ++chunk_index) {
            const cJSON *chunk = cJSON_GetArrayItem(split, chunk_index);
            size_t chunk_rows;
            assert(cJSON_IsNumber(chunk) && chunk->valuedouble >= 0);
            chunk_rows = (size_t)chunk->valuedouble;
            assert(chunk_rows <= analyze_count - offset);
            if (input_f32)
                make_f32_table(
                    chunk_rows ? analyze_values_f32 + offset : NULL,
                    chunk_rows, &column, &table);
            else
                make_f64_table(
                    chunk_rows ? analyze_values + offset : NULL,
                    chunk_rows, &column, &table);
            code = tf_transform_analyzer_push(analyzer, &table, &error);
            offset += chunk_rows;
            if (expected_error
                && strcmp(cJSON_GetObjectItemCaseSensitive(
                    expected_error, "phase")->valuestring, "analyze") == 0) {
                tf_transform_code expected = (tf_transform_code)
                    cJSON_GetObjectItemCaseSensitive(
                        expected_error, "code")->valueint;
                if (chunk_index + 1 == cJSON_GetArraySize(split)) {
                    assert(code == expected && error != NULL
                           && tf_transform_error_get_code(error) == expected);
                    tf_transform_error_destroy(&error);
                    assert(tf_transform_analyzer_push(analyzer, &table, &error)
                           == TF_TRANSFORM_INVALID_STATE);
                    tf_transform_error_destroy(&error);
                    analyze_failed = 1;
                    break;
                }
            }
            assert(code == TF_TRANSFORM_OK);
        }
        assert(offset == analyze_count);
        if (analyze_failed) {
            tf_transform_analyzer_destroy(&analyzer);
            continue;
        }
        if (expected_error
            && strcmp(cJSON_GetObjectItemCaseSensitive(
                expected_error, "phase")->valuestring, "finalize") == 0) {
            tf_transform_code expected = (tf_transform_code)
                cJSON_GetObjectItemCaseSensitive(
                    expected_error, "code")->valueint;
            assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
                   == expected);
            assert(plan == NULL);
            tf_transform_error_destroy(&error);
            assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
                   == TF_TRANSFORM_INVALID_STATE);
            assert(plan == NULL);
            tf_transform_error_destroy(&error);
            tf_transform_analyzer_destroy(&analyzer);
            continue;
        }
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_OK);
        assert(plan != NULL && expected_plan != NULL);
        if (strcmp(cJSON_GetObjectItemCaseSensitive(
                expected_plan, "kind")->valuestring, "categorical") == 0) {
            const cJSON *expected_categories = cJSON_GetObjectItemCaseSensitive(
                expected_plan, "categories");
            const tf_transform_categorical_state *state
                = &plan->states[0].value.categorical;
            assert(plan->states[0].kind == TF_TRANSFORM_KIND_CATEGORICAL);
            assert(state->source_dtype
                   == (input_f32 ? TF_VIEW_FLOAT32 : TF_VIEW_FLOAT64));
            assert(cJSON_GetArraySize(expected_categories)
                   == (int)state->category_count);
            for (size_t i = 0; i < state->category_count; ++i)
                assert(state->categories[i].bits == (input_f32
                    ? (uint64_t)parse_hex32(cJSON_GetArrayItem(
                        expected_categories, (int)i)->valuestring)
                    : parse_hex64(cJSON_GetArrayItem(
                        expected_categories, (int)i)->valuestring)));
            assert(state->impute_bits == (input_f32
                ? (uint64_t)parse_hex32(cJSON_GetObjectItemCaseSensitive(
                    expected_plan, "impute")->valuestring)
                : parse_hex64(cJSON_GetObjectItemCaseSensitive(
                    expected_plan, "impute")->valuestring)));
            if (cJSON_HasObjectItem(expected_plan, "unknown")) {
                const char *unknown = cJSON_GetObjectItemCaseSensitive(
                    expected_plan, "unknown")->valuestring;
                const cJSON *sentinel = cJSON_GetObjectItemCaseSensitive(
                    expected_plan, "sentinelLabel");
                const cJSON *output_ids = cJSON_GetObjectItemCaseSensitive(
                    expected_plan, "outputIds");
                int onehot = recipe->columns[0].categorical_encode
                    == TF_TRANSFORM_ENCODE_ONEHOT;
                assert(state->encode == (onehot
                    ? TF_TRANSFORM_ENCODE_ONEHOT : TF_TRANSFORM_ENCODE_LABEL));
                if (strcmp(unknown, "error") == 0) {
                    assert(state->unknown == TF_TRANSFORM_UNKNOWN_ERROR);
                    assert(!state->has_sentinel_label
                           && !state->has_other_ordinal);
                } else if (strcmp(unknown, "sentinel") == 0) {
                    assert(state->unknown == TF_TRANSFORM_UNKNOWN_SENTINEL);
                    assert(state->has_sentinel_label
                           && state->sentinel_label == (int64_t)sentinel->valuedouble);
                    assert(!state->has_other_ordinal);
                } else if (strcmp(unknown, "other") == 0) {
                    assert(strcmp(unknown, "other") == 0);
                    assert(state->unknown == TF_TRANSFORM_UNKNOWN_OTHER);
                    assert(!state->has_sentinel_label
                           && state->has_other_ordinal
                           && state->other_ordinal == state->category_count);
                } else {
                    assert(onehot && strcmp(unknown, "all_zero") == 0);
                    assert(state->unknown == TF_TRANSFORM_UNKNOWN_ALL_ZERO);
                    assert(!state->has_sentinel_label
                           && !state->has_other_ordinal);
                }
                assert(plan->output_schema.field_count
                       == (size_t)cJSON_GetArraySize(output_ids));
                for (size_t output_index = 0;
                     output_index < plan->output_schema.field_count;
                     ++output_index) {
                    const tf_transform_schema_field_owned *output_field
                        = &plan->output_schema.fields[output_index];
                    const char *expected_id = cJSON_GetArrayItem(
                        output_ids, (int)output_index)->valuestring;
                    assert(output_field->dtype == TF_VIEW_FLOAT64
                           && output_field->role == (onehot
                               ? TF_TRANSFORM_ROLE_ONEHOT
                               : TF_TRANSFORM_ROLE_LABEL));
                    assert(output_field->source_id_len == 2
                           && memcmp(output_field->source_id, "x0", 2) == 0);
                    assert(output_field->id_len == strlen(expected_id)
                           && memcmp(
                               output_field->id, expected_id,
                               output_field->id_len) == 0);
                    if (!onehot) {
                        assert(output_field->category_kind
                               == TF_TRANSFORM_SCHEMA_CATEGORY_NONE);
                    } else if (output_index < state->category_count) {
                        assert(output_field->category_kind
                                   == TF_TRANSFORM_SCHEMA_CATEGORY_VALUE
                               && output_field->category_dtype
                                   == state->source_dtype
                               && output_field->category_bits
                                   == state->categories[output_index].bits);
                    } else {
                        assert(output_index == state->category_count
                               && state->has_other_ordinal
                               && output_field->category_kind
                                   == TF_TRANSFORM_SCHEMA_CATEGORY_OTHER);
                    }
                }
            }
        } else {
            const cJSON *expected_impute = cJSON_GetObjectItemCaseSensitive(
                expected_plan, "impute");
            assert(plan->states[0].kind == TF_TRANSFORM_KIND_NUMERIC);
            assert(tf_transform_double_bits(plan->states[0].value.numeric.location)
                   == parse_hex64(cJSON_GetObjectItemCaseSensitive(
                       expected_plan, "location")->valuestring));
            assert(tf_transform_double_bits(plan->states[0].value.numeric.scale)
                   == parse_hex64(cJSON_GetObjectItemCaseSensitive(
                       expected_plan, "scale")->valuestring));
            if (cJSON_IsString(expected_impute))
                assert(tf_transform_double_bits(plan->states[0].value.numeric.impute_value)
                       == parse_hex64(expected_impute->valuestring));
        }
        assert(tf_transform_plan_export(
            plan, NULL, &plan_bytes, &plan_len, &error) == TF_TRANSFORM_OK);
        if (!reference_bytes) {
            reference_bytes = plan_bytes;
            reference_len = plan_len;
            plan_bytes = NULL;
            plan_len = 0;
        } else {
            assert(plan_len == reference_len
                   && memcmp(plan_bytes, reference_bytes, plan_len) == 0);
            tf_transform_bytes_free(&plan_bytes, &plan_len);
        }
        assert(tf_transform_apply_create(
            plan, &schema, NULL, &apply_session, &error) == TF_TRANSFORM_OK);
        if (input_f32)
            make_f32_table(apply_values_f32, apply_count, &column, &table);
        else
            make_f64_table(apply_values, apply_count, &column, &table);
        if (expected_error) {
            tf_transform_code expected = (tf_transform_code)
                cJSON_GetObjectItemCaseSensitive(
                    expected_error, "code")->valueint;
            assert(strcmp(cJSON_GetObjectItemCaseSensitive(
                expected_error, "phase")->valuestring, "apply") == 0);
            assert(tf_transform_apply_run(
                apply_session, &table, &dense, &error) == expected);
            assert(error != NULL && tf_transform_error_get_code(error) == expected);
            tf_transform_error_destroy(&error);
            assert(tf_transform_apply_run(
                apply_session, &table, &dense, &error)
                == TF_TRANSFORM_INVALID_STATE);
            tf_transform_error_destroy(&error);
        } else {
        assert(tf_transform_apply_run(
            apply_session, &table, &dense, &error) == TF_TRANSFORM_OK);
        {
            const cJSON *expected_rows = cJSON_GetObjectItemCaseSensitive(
                apply, "expectedRows");
            const cJSON *expected_columns_value
                = cJSON_GetObjectItemCaseSensitive(apply, "expectedColumns");
            size_t expected_columns = expected_columns_value
                ? (size_t)expected_columns_value->valuedouble
                : (apply_count == 0 ? 1u : (size_t)cJSON_GetArraySize(
                    cJSON_GetArrayItem(expected_rows, 0)));
            assert(cJSON_GetArraySize(expected_rows) == (int)apply_count);
            assert(dense.rows == apply_count
                   && dense.columns == expected_columns);
            for (size_t i = 0; i < apply_count; ++i) {
                const cJSON *expected_row = cJSON_GetArrayItem(
                    expected_rows, (int)i);
                assert(cJSON_GetArraySize(expected_row)
                       == (int)expected_columns);
                for (size_t j = 0; j < expected_columns; ++j) {
                    const cJSON *expected_cell = cJSON_GetArrayItem(
                        expected_row, (int)j);
                    assert(tf_transform_double_bits(
                        ((double *)dense.data)[i * expected_columns + j])
                        == parse_hex64(expected_cell->valuestring));
                }
            }
        }
        tf_owned_dense_free(&dense);
        }
        tf_transform_apply_destroy(&apply_session);
        tf_transform_plan_destroy(&plan);
        tf_transform_analyzer_destroy(&analyzer);
    }
    tf_transform_bytes_free(&reference_bytes, &reference_len);
    free(apply_values_f32);
    free(analyze_values_f32);
    free(apply_values);
    free(analyze_values);
}

static void test_shared_semantic_case_table(void) {
    char *text = read_file("test/vectors/prepared_transform_v1.json");
    cJSON *root = cJSON_Parse(text);
    const cJSON *recipes;
    const cJSON *cases;
    size_t executed = 0;
    size_t deferred = 0;
    assert(root != NULL);
    recipes = cJSON_GetObjectItemCaseSensitive(root, "recipes");
    cases = cJSON_GetObjectItemCaseSensitive(root, "semanticCases");
    assert(cJSON_IsObject(recipes) && cJSON_IsArray(cases));
    for (int i = 0; i < cJSON_GetArraySize(cases); ++i) {
        const cJSON *entry = cJSON_GetArrayItem(cases, i);
        const cJSON *recipe_name = cJSON_GetObjectItemCaseSensitive(
            entry, "recipe");
        tf_transform_recipe *recipe = NULL;
        tf_transform_error *error = NULL;
        tf_transform_code code;
        assert(cJSON_IsString(recipe_name));
        code = parse_recipe_entry(
            recipes, recipe_name->valuestring, &recipe, &error);
        if (code == TF_TRANSFORM_INVALID_RECIPE) {
            assert(recipe == NULL);
            ++deferred;
            tf_transform_error_destroy(&error);
            continue;
        }
        assert(code == TF_TRANSFORM_OK && recipe != NULL && error == NULL);
        execute_semantic_case(entry, recipe);
        ++executed;
        tf_transform_recipe_destroy(&recipe);
    }
    assert(executed == 40 && deferred == 1);
    cJSON_Delete(root);
    free(text);
}

static tf_transform_code parse_recipe_json_value(
    const cJSON *recipe_json, tf_transform_recipe **out,
    tf_transform_error **error) {
    char *serialized = cJSON_PrintUnformatted(recipe_json);
    tf_transform_limits_v1 limits;
    tf_transform_code code;
    assert(serialized != NULL);
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    code = tf_transform_recipe_from_json(
        (const uint8_t *)serialized, strlen(serialized), &limits,
        out, error);
    free(serialized);
    return code;
}

static cJSON *mutated_validation_recipe(
    const cJSON *recipes, const cJSON *entry, const char **recipe_name_out) {
    const cJSON *mutation = cJSON_GetObjectItemCaseSensitive(entry, "mutation");
    const cJSON *recipe_name = mutation
        ? cJSON_GetObjectItemCaseSensitive(mutation, "recipe")
        : cJSON_GetObjectItemCaseSensitive(entry, "recipe");
    cJSON *copy;
    assert(cJSON_IsString(recipe_name));
    *recipe_name_out = recipe_name->valuestring;
    copy = cJSON_Duplicate(
        cJSON_GetObjectItemCaseSensitive(recipes, recipe_name->valuestring), 1);
    assert(copy != NULL);
    if (mutation) {
        const cJSON *path = cJSON_GetObjectItemCaseSensitive(mutation, "path");
        const cJSON *value = cJSON_GetObjectItemCaseSensitive(mutation, "value");
        cJSON *column = cJSON_GetArrayItem(
            cJSON_GetObjectItemCaseSensitive(copy, "columns"), 0);
        assert(cJSON_IsString(path) && value != NULL);
        if (strcmp(path->valuestring, "/columns/0/kind/maxCategories") == 0) {
            cJSON *kind = cJSON_GetObjectItemCaseSensitive(column, "kind");
            assert(cJSON_ReplaceItemInObjectCaseSensitive(
                kind, "maxCategories", cJSON_Duplicate(value, 1)));
        } else if (strcmp(path->valuestring, "/columns/0/kind/rule") == 0) {
            cJSON *kind = cJSON_GetObjectItemCaseSensitive(column, "kind");
            assert(cJSON_ReplaceItemInObjectCaseSensitive(
                kind, "rule", cJSON_Duplicate(value, 1)));
        } else if (strcmp(path->valuestring, "/columns/0/numeric") == 0) {
            assert(cJSON_ReplaceItemInObjectCaseSensitive(
                column, "numeric", cJSON_Duplicate(value, 1)));
        } else if (strcmp(path->valuestring, "/columns/0/categorical") == 0) {
            assert(cJSON_ReplaceItemInObjectCaseSensitive(
                column, "categorical", cJSON_Duplicate(value, 1)));
        } else if (strcmp(
                path->valuestring,
                "/columns/0/categorical/encode/sentinelLabel") == 0) {
            cJSON *categorical = cJSON_GetObjectItemCaseSensitive(
                column, "categorical");
            cJSON *encode = cJSON_GetObjectItemCaseSensitive(
                categorical, "encode");
            assert(cJSON_ReplaceItemInObjectCaseSensitive(
                encode, "sentinelLabel", cJSON_CreateNumber(value->valuedouble)));
        } else {
            cJSON *numeric = cJSON_GetObjectItemCaseSensitive(column, "numeric");
            cJSON *normalize = cJSON_GetObjectItemCaseSensitive(
                numeric, "normalize");
            assert(strcmp(
                path->valuestring, "/columns/0/numeric/normalize/ddof") == 0);
            assert(cJSON_ReplaceItemInObjectCaseSensitive(
                normalize, "ddof", cJSON_CreateNumber(value->valuedouble)));
        }
    }
    return copy;
}

static void test_shared_validation_case_table(void) {
    char *text = read_file("test/vectors/prepared_transform_v1.json");
    cJSON *root = cJSON_Parse(text);
    const cJSON *recipes;
    const cJSON *cases;
    size_t executed = 0;
    size_t deferred = 0;
    assert(root != NULL);
    recipes = cJSON_GetObjectItemCaseSensitive(root, "recipes");
    cases = cJSON_GetObjectItemCaseSensitive(root, "validationCases");
    assert(cJSON_IsObject(recipes) && cJSON_IsArray(cases));
    for (int i = 0; i < cJSON_GetArraySize(cases); ++i) {
        const cJSON *entry = cJSON_GetArrayItem(cases, i);
        const cJSON *phase = cJSON_GetObjectItemCaseSensitive(entry, "phase");
        const cJSON *rows = cJSON_GetObjectItemCaseSensitive(entry, "rows");
        const char *recipe_name = NULL;
        cJSON *recipe_json = mutated_validation_recipe(
            recipes, entry, &recipe_name);
        tf_transform_recipe *recipe = NULL;
        tf_transform_error *error = NULL;
        tf_transform_code expected = (tf_transform_code)
            cJSON_GetObjectItemCaseSensitive(entry, "expectedCode")->valueint;
        tf_transform_code code = parse_recipe_json_value(
            recipe_json, &recipe, &error);
        cJSON_Delete(recipe_json);
        assert(cJSON_IsString(phase));
        if (strcmp(phase->valuestring, "recipe") == 0) {
            if (code != expected) fprintf(
                stderr, "validation case %d (%s): got %d expected %d\n",
                i, recipe_name, (int)code, (int)expected);
            assert(code == expected);
            ++executed;
            tf_transform_error_destroy(&error);
            tf_transform_recipe_destroy(&recipe);
            continue;
        }
        assert(code == TF_TRANSFORM_OK && recipe != NULL && error == NULL);
        {
            tf_field_view_v1 field;
            tf_schema_view_v1 schema;
            tf_transform_analyzer *analyzer = NULL;
            tf_transform_plan *plan = NULL;
            tf_column_view_v1 column;
            tf_table_view_v1 table;
            double *values;
            size_t row_count;
            values = decode_single_column_rows(rows, &row_count);
            make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
            assert(tf_transform_analyzer_create(
                recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
            make_f64_table(values, row_count, &column, &table);
            code = tf_transform_analyzer_push(analyzer, &table, &error);
            if (strcmp(phase->valuestring, "analyze_push") == 0) {
                assert(code == expected);
            } else {
                assert(code == TF_TRANSFORM_OK);
                code = tf_transform_analyzer_finalize(analyzer, &plan, &error);
                assert(code == expected && plan == NULL);
            }
            free(values);
            tf_transform_error_destroy(&error);
            tf_transform_plan_destroy(&plan);
            tf_transform_analyzer_destroy(&analyzer);
        }
        ++executed;
        tf_transform_recipe_destroy(&recipe);
    }
    assert(executed == 11 && deferred == 0);
    cJSON_Delete(root);
    free(text);
}

static void test_f32_stride_validity_and_apply_terminal(void) {
    struct strided_f32 { float value; float padding; } values[] = {
        {1.0f, 99.0f},
        {tf_transform_double_from_bits(UINT64_C(0x7ff0000000000000)), 99.0f},
        {3.0f, 99.0f}
    };
    uint8_t validity[] = {(uint8_t)((1u << 1) | (1u << 5))};
    tf_transform_recipe *recipe = load_vector_recipe("numeric_none_none");
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_apply *apply = NULL;
    tf_transform_error *error = NULL;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    tf_owned_dense_v1 dense;
    uint8_t misaligned_storage[24];

    make_x0_schema(TF_VIEW_FLOAT32, &field, &schema);
    assert(tf_transform_analyzer_create(
        recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
    memset(&column, 0, sizeof(column));
    column.abi_version = 1;
    column.struct_size = (uint32_t)sizeof(column);
    column.data = values;
    column.data_bytes = sizeof(values);
    column.stride_bytes = sizeof(values[0]);
    column.validity = validity;
    column.validity_bytes = sizeof(validity);
    column.validity_bit_offset = 1;
    column.validity_bit_stride = 2;
    memset(&table, 0, sizeof(table));
    table.abi_version = 1;
    table.struct_size = (uint32_t)sizeof(table);
    table.row_count = 3;
    table.column_count = 1;
    table.columns = &column;
    table.columns_bytes = sizeof(column);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_apply_create(plan, &schema, NULL, &apply, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_apply_run(apply, &table, &dense, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_double_bits(((double *)dense.data)[0])
           == UINT64_C(0x3ff0000000000000));
    assert(tf_transform_double_bits(((double *)dense.data)[1])
           == UINT64_C(0x7ff8000000000000));
    assert(tf_transform_double_bits(((double *)dense.data)[2])
           == UINT64_C(0x4008000000000000));
    tf_owned_dense_free(&dense);
    tf_transform_apply_destroy(&apply);
    tf_transform_plan_destroy(&plan);
    tf_transform_analyzer_destroy(&analyzer);

    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
    assert(tf_transform_analyzer_create(
        recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
    memset(&column, 0, sizeof(column));
    column.abi_version = 1;
    column.struct_size = (uint32_t)sizeof(column);
    column.data = misaligned_storage + 1;
    column.data_bytes = sizeof(double);
    column.stride_bytes = sizeof(double);
    memset(&table, 0, sizeof(table));
    table.abi_version = 1;
    table.struct_size = (uint32_t)sizeof(table);
    table.row_count = 1;
    table.column_count = 1;
    table.columns = &column;
    table.columns_bytes = sizeof(column);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_INVALID_ARGUMENT);
    tf_transform_error_destroy(&error);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_INVALID_STATE);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);

    {
        const double good[] = {1.0};
        const double infinity[] = {
            tf_transform_double_from_bits(UINT64_C(0x7ff0000000000000))
        };
        plan = fit_f64_plan("numeric_none_none", good, 1, 1);
        make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
        assert(tf_transform_apply_create(plan, &schema, NULL, &apply, &error)
               == TF_TRANSFORM_OK);
        make_f64_table(infinity, 1, &column, &table);
        memset(&dense, 0xa5, sizeof(dense));
        assert(tf_transform_apply_run(apply, &table, &dense, &error)
               == TF_TRANSFORM_NUMERIC_DOMAIN);
        assert(dense.data == NULL && dense.data_bytes == 0);
        tf_transform_error_destroy(&error);
        make_f64_table(good, 1, &column, &table);
        assert(tf_transform_apply_run(apply, &table, &dense, &error)
               == TF_TRANSFORM_INVALID_STATE);
        tf_transform_error_destroy(&error);
        tf_transform_apply_destroy(&apply);
        tf_transform_plan_destroy(&plan);
    }
    tf_transform_recipe_destroy(&recipe);
}

static void test_numeric_cancellation_is_terminal(void) {
    const double valid[] = {1.0};
    tf_transform_recipe *recipe = load_vector_recipe("numeric_none_none");
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_error *error = NULL;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    tf_transform_runtime_v1 runtime;
    int cancel_requested = 0;
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.cancel = cancel_when_set;
    runtime.cancel_user = &cancel_requested;
    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
    assert(tf_transform_analyzer_create(
        recipe, &schema, &runtime, &analyzer, &error) == TF_TRANSFORM_OK);
    cancel_requested = 1;
    make_f64_table(valid, 1, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_CANCELLED);
    tf_transform_error_destroy(&error);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_INVALID_STATE);
    assert(plan == NULL);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);
    tf_transform_recipe_destroy(&recipe);
}

static void test_tftr_baseline_and_malformed(void) {
    const double value[] = {1.0};
    char *text = read_file("test/vectors/tftr_malformed_v1.json");
    cJSON *root = cJSON_Parse(text);
    cJSON *base;
    cJSON *hex;
    uint8_t *expected;
    size_t expected_len;
    tf_transform_recipe *recipe = load_vector_recipe("numeric_none_none");
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_plan *imported = NULL;
    tf_transform_error *error = NULL;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    uint8_t *exported = NULL;
    size_t exported_len = 0;
    uint8_t *roundtrip = NULL;
    size_t roundtrip_len = 0;
    uint8_t *mutated;
    tf_transform_limits_v1 limits;
    tf_transform_runtime_v1 runtime;
    uint64_t exact_import_allocations;
    uint64_t exact_import_peak;

    assert(root != NULL);
    base = cJSON_GetObjectItemCaseSensitive(root, "base");
    hex = cJSON_GetObjectItemCaseSensitive(base, "tftrHex");
    assert(cJSON_IsString(hex));
    expected = decode_hex(hex->valuestring, &expected_len);
    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
    assert(tf_transform_analyzer_create(
        recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
    make_f64_table(value, 1, &column, &table);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_plan_export(
        plan, NULL, &exported, &exported_len, &error) == TF_TRANSFORM_OK);
    assert(exported_len == expected_len
           && memcmp(exported, expected, expected_len) == 0);
    assert(tf_transform_plan_import(
        expected, expected_len, NULL, &imported, &error) == TF_TRANSFORM_OK);
    exact_import_allocations = imported->import_allocation_count;
    exact_import_peak = imported->import_peak_resident_bytes;
    assert(exact_import_allocations > 1 && exact_import_peak > 1);
    assert(tf_transform_plan_export(
        imported, NULL, &roundtrip, &roundtrip_len, &error) == TF_TRANSFORM_OK);
    assert(roundtrip_len == expected_len
           && memcmp(roundtrip, expected, expected_len) == 0);
    tf_transform_bytes_free(&roundtrip, &roundtrip_len);
    tf_transform_plan_destroy(&imported);

    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.limits = &limits;
    limits.max_allocations_per_session = exact_import_allocations - 1;
    assert(tf_transform_plan_import(
        expected, expected_len, &runtime, &imported, &error)
        == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(imported == NULL);
    tf_transform_error_destroy(&error);
    limits.max_allocations_per_session = exact_import_allocations;
    assert(tf_transform_plan_import(
        expected, expected_len, &runtime, &imported, &error)
        == TF_TRANSFORM_OK);
    assert(imported != NULL
           && imported->import_allocation_count == exact_import_allocations);
    tf_transform_plan_destroy(&imported);
    limits.max_resident_state_bytes = exact_import_peak - 1;
    assert(tf_transform_plan_import(
        expected, expected_len, &runtime, &imported, &error)
        == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(imported == NULL);
    tf_transform_error_destroy(&error);
    limits.max_resident_state_bytes = exact_import_peak;
    assert(tf_transform_plan_import(
        expected, expected_len, &runtime, &imported, &error)
        == TF_TRANSFORM_OK);
    assert(imported != NULL
           && imported->import_peak_resident_bytes == exact_import_peak);
    tf_transform_plan_destroy(&imported);

    mutated = (uint8_t *)malloc(expected_len + 1);
    assert(mutated != NULL);
    memcpy(mutated, expected, expected_len);
    mutated[0] ^= 1u;
    assert(tf_transform_plan_import(
        mutated, expected_len, NULL, &imported, &error)
        == TF_TRANSFORM_CORRUPT_PLAN);
    tf_transform_error_destroy(&error);
    memcpy(mutated, expected, expected_len);
    mutated[4] = 2;
    assert(tf_transform_plan_import(
        mutated, expected_len, NULL, &imported, &error)
        == TF_TRANSFORM_UNSUPPORTED_VERSION);
    tf_transform_error_destroy(&error);
    memcpy(mutated, expected, expected_len);
    mutated[6] = 1;
    assert(tf_transform_plan_import(
        mutated, expected_len, NULL, &imported, &error)
        == TF_TRANSFORM_UNSUPPORTED_VERSION);
    tf_transform_error_destroy(&error);
    memcpy(mutated, expected, expected_len);
    mutated[8] = 51;
    assert(tf_transform_plan_import(
        mutated, expected_len, NULL, &imported, &error)
        == TF_TRANSFORM_CORRUPT_PLAN);
    tf_transform_error_destroy(&error);
    assert(tf_transform_plan_import(
        expected, expected_len - 1, NULL, &imported, &error)
        == TF_TRANSFORM_CORRUPT_PLAN);
    tf_transform_error_destroy(&error);
    memcpy(mutated, expected, expected_len);
    mutated[expected_len] = 0;
    assert(tf_transform_plan_import(
        mutated, expected_len + 1, NULL, &imported, &error)
        == TF_TRANSFORM_CORRUPT_PLAN);
    tf_transform_error_destroy(&error);
    memcpy(mutated, expected, expected_len);
    mutated[52] ^= 1u;
    assert(tf_transform_plan_import(
        mutated, expected_len, NULL, &imported, &error)
        == TF_TRANSFORM_CORRUPT_PLAN);
    tf_transform_error_destroy(&error);
    {
        size_t payload_len = expected_len - 52;
        uint8_t digest[32];
        memcpy(mutated, expected, 52);
        mutated[52] = expected[52];
        mutated[53] = ' ';
        memcpy(mutated + 54, expected + 53, payload_len - 1);
        for (size_t i = 0; i < 8; ++i)
            mutated[12 + i] = (uint8_t)((payload_len + 1) >> (i * 8));
        tf_transform_sha256(mutated + 52, payload_len + 1, digest);
        memcpy(mutated + 20, digest, sizeof(digest));
        assert(tf_transform_plan_import(
            mutated, expected_len + 1, NULL, &imported, &error)
            == TF_TRANSFORM_CORRUPT_PLAN);
        tf_transform_error_destroy(&error);
    }

    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    limits.max_plan_bytes = expected_len - 1;
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.limits = &limits;
    assert(tf_transform_plan_import(
        expected, expected_len, &runtime, &imported, &error)
        == TF_TRANSFORM_RESOURCE_LIMIT);
    tf_transform_error_destroy(&error);

    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    runtime.limits = &limits;
    runtime.cancel = always_cancel;
    assert(tf_transform_plan_import(
        expected, expected_len, &runtime, &imported, &error)
        == TF_TRANSFORM_CANCELLED);
    tf_transform_error_destroy(&error);

    free(mutated);
    tf_transform_bytes_free(&exported, &exported_len);
    tf_transform_plan_destroy(&plan);
    tf_transform_analyzer_destroy(&analyzer);
    tf_transform_recipe_destroy(&recipe);
    free(expected);
    cJSON_Delete(root);
    free(text);
}

static uint8_t *wrap_tftr_test(
    const uint8_t *payload, size_t payload_len,
    uint16_t version, uint16_t flags, uint32_t header_len,
    uint64_t declared_payload_len, const char magic[4], size_t *out_len) {
    uint8_t digest[32];
    uint8_t *result;
    assert(payload != NULL && out_len != NULL && payload_len <= SIZE_MAX - 52);
    result = (uint8_t *)malloc(52 + payload_len);
    assert(result != NULL);
    tf_transform_sha256(payload, payload_len, digest);
    memcpy(result, magic, 4);
    write_u16_le_test(result + 4, version);
    write_u16_le_test(result + 6, flags);
    write_u32_le_test(result + 8, header_len);
    write_u64_le_test(result + 12, declared_payload_len);
    memcpy(result + 20, digest, sizeof(digest));
    memcpy(result + 52, payload, payload_len);
    *out_len = 52 + payload_len;
    return result;
}

static void test_median_negative_zero_import_rejected(void) {
    const double values[] = {-0.0, 0.0, 1.0};
    tf_transform_plan *plan = fit_f64_plan(
        "numeric_median_none", values, 3, 1);
    tf_transform_plan *imported = NULL;
    tf_transform_error *error = NULL;
    tf_transform_limits_v1 limits;
    uint8_t *exported = NULL;
    size_t exported_len = 0;
    cJSON *root;
    cJSON *step;
    cJSON *numeric;
    cJSON *impute;
    cJSON *bits;
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    uint8_t *mutated;
    size_t mutated_len = 0;

    assert(tf_transform_double_bits(
        plan->states[0].value.numeric.impute_value) == 0);
    assert(tf_transform_plan_export(
        plan, NULL, &exported, &exported_len, &error) == TF_TRANSFORM_OK);
    assert(error == NULL && exported_len > 52);
    root = cJSON_ParseWithLength(
        (const char *)exported + 52, exported_len - 52);
    assert(root != NULL);
    step = cJSON_GetArrayItem(
        cJSON_GetObjectItemCaseSensitive(root, "steps"), 0);
    numeric = cJSON_GetObjectItemCaseSensitive(step, "numeric");
    impute = cJSON_GetObjectItemCaseSensitive(numeric, "impute");
    bits = cJSON_GetObjectItemCaseSensitive(
        cJSON_GetObjectItemCaseSensitive(impute, "value"), "v");
    assert(cJSON_IsString(bits)
           && strcmp(bits->valuestring, "0000000000000000") == 0);
    assert(cJSON_SetValuestring(bits, "8000000000000000") != NULL);
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    assert(tf_transform_json_print_canonical(
        root, &limits, &payload, &payload_len, &error) == TF_TRANSFORM_OK);
    assert(error == NULL && payload != NULL);
    mutated = wrap_tftr_test(
        payload, payload_len, 1, 0, 52, payload_len,
        "TFTR", &mutated_len);
    assert(tf_transform_plan_import(
        mutated, mutated_len, NULL, &imported, &error)
        == TF_TRANSFORM_CORRUPT_PLAN);
    assert(imported == NULL && error != NULL
           && tf_transform_error_get_code(error) == TF_TRANSFORM_CORRUPT_PLAN);

    tf_transform_error_destroy(&error);
    free(mutated);
    tf_transform_bytes_free(&payload, &payload_len);
    cJSON_Delete(root);
    tf_transform_bytes_free(&exported, &exported_len);
    tf_transform_plan_destroy(&plan);
}

static void assert_categorical_root_import_code(
    const cJSON *root, const tf_transform_limits_v1 *import_limits,
    tf_transform_code expected, int expect_no_json_growth) {
    tf_transform_limits_v1 print_limits;
    tf_transform_runtime_v1 runtime;
    tf_transform_plan *plan = NULL;
    tf_transform_error *error = NULL;
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    uint8_t *tftr;
    size_t tftr_len = 0;
    assert(tf_transform_limits_init_safe_v1(&print_limits, sizeof(print_limits))
           == TF_TRANSFORM_OK);
    assert(tf_transform_json_print_canonical(
        root, &print_limits, &payload, &payload_len, &error) == TF_TRANSFORM_OK);
    assert(error == NULL && payload != NULL);
    tftr = wrap_tftr_test(
        payload, payload_len, 1, 0, 52, payload_len, "TFTR", &tftr_len);
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.limits = import_limits;
    if (expect_no_json_growth) {
        cJSON_Hooks hooks = {counting_cjson_malloc, counting_cjson_free};
        cjson_test_allocations = 0;
        cJSON_InitHooks(&hooks);
    }
    assert(tf_transform_plan_import(
        tftr, tftr_len, import_limits ? &runtime : NULL, &plan, &error)
        == expected);
    if (expect_no_json_growth) {
        cJSON_InitHooks(NULL);
        assert(cjson_test_allocations == 0);
    }
    if (expected == TF_TRANSFORM_OK)
        assert(plan != NULL && error == NULL);
    else
        assert(plan == NULL && error != NULL
               && tf_transform_error_get_code(error) == expected);
    tf_transform_error_destroy(&error);
    tf_transform_plan_destroy(&plan);
    free(tftr);
    tf_transform_bytes_free(&payload, &payload_len);
}

static void update_recipe_fingerprint_for_test(cJSON *root) {
    static const char digits[] = "0123456789abcdef";
    cJSON *object = cJSON_CreateObject();
    cJSON *input_schema = cJSON_Duplicate(
        cJSON_GetObjectItemCaseSensitive(root, "inputSchema"), 1);
    cJSON *recipe = cJSON_Duplicate(
        cJSON_GetObjectItemCaseSensitive(root, "recipe"), 1);
    cJSON *fingerprint = cJSON_GetObjectItemCaseSensitive(root, "recipeSha256");
    tf_transform_limits_v1 limits;
    tf_transform_error *error = NULL;
    uint8_t *canonical = NULL;
    size_t canonical_len = 0;
    uint8_t digest[32];
    char hex[65];
    assert(object != NULL && input_schema != NULL && recipe != NULL
           && cJSON_IsString(fingerprint));
    assert(cJSON_AddItemToObject(object, "inputSchema", input_schema));
    assert(cJSON_AddNumberToObject(object, "policyVersion", 1) != NULL);
    assert(cJSON_AddItemToObject(object, "recipe", recipe));
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    assert(tf_transform_json_print_canonical(
        object, &limits, &canonical, &canonical_len, &error) == TF_TRANSFORM_OK);
    assert(error == NULL && canonical != NULL);
    tf_transform_sha256(canonical, canonical_len, digest);
    for (size_t i = 0; i < sizeof(digest); ++i) {
        hex[i * 2] = digits[digest[i] >> 4];
        hex[i * 2 + 1] = digits[digest[i] & 15u];
    }
    hex[64] = '\0';
    assert(cJSON_SetValuestring(fingerprint, hex) != NULL);
    tf_transform_bytes_free(&canonical, &canonical_len);
    cJSON_Delete(object);
}

static void test_kind_inference_import_contract(void) {
    const double categorical_values[] = {0.0, 1.0, 0.0};
    const double numeric_values[] = {0.0, 1.0, 2.0};
    tf_transform_plan *categorical_plan = fit_f64_plan(
        "infer_two_categories", categorical_values, 3, 1);
    tf_transform_plan *numeric_plan = fit_f64_plan(
        "infer_two_categories", numeric_values, 3, 1);
    tf_transform_error *error = NULL;
    tf_transform_limits_v1 limits;
    uint8_t *bytes = NULL;
    size_t bytes_len = 0;
    cJSON *categorical_root;
    cJSON *numeric_root;
    cJSON *recipe_kind;
    cJSON *step;

    assert(categorical_plan->states[0].kind == TF_TRANSFORM_KIND_CATEGORICAL
           && numeric_plan->states[0].kind == TF_TRANSFORM_KIND_NUMERIC);
    assert(tf_transform_plan_export(
        categorical_plan, NULL, &bytes, &bytes_len, &error) == TF_TRANSFORM_OK);
    categorical_root = cJSON_ParseWithLength(
        (const char *)bytes + 52, bytes_len - 52);
    assert(categorical_root != NULL);
    tf_transform_bytes_free(&bytes, &bytes_len);

    recipe_kind = cJSON_GetObjectItemCaseSensitive(
        cJSON_GetArrayItem(cJSON_GetObjectItemCaseSensitive(
            cJSON_GetObjectItemCaseSensitive(categorical_root, "recipe"),
            "columns"), 0),
        "kind");
    step = cJSON_GetArrayItem(
        cJSON_GetObjectItemCaseSensitive(categorical_root, "steps"), 0);
    assert(cJSON_IsString(cJSON_GetObjectItemCaseSensitive(recipe_kind, "op"))
           && strcmp(cJSON_GetObjectItemCaseSensitive(
                recipe_kind, "op")->valuestring, "infer") == 0
           && strcmp(cJSON_GetObjectItemCaseSensitive(
                recipe_kind, "rule")->valuestring,
                "finite-integer-cardinality-v1") == 0
           && cJSON_GetObjectItemCaseSensitive(
                recipe_kind, "maxCategories")->valuedouble == 2.0
           && cJSON_IsString(cJSON_GetObjectItemCaseSensitive(step, "kind"))
           && strcmp(cJSON_GetObjectItemCaseSensitive(
                step, "kind")->valuestring, "categorical") == 0);
    assert_categorical_root_import_code(
        categorical_root, NULL, TF_TRANSFORM_OK, 0);

    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    limits.max_categories_per_column = 1;
    limits.max_total_categories = 1;
    assert_categorical_root_import_code(
        categorical_root, &limits, TF_TRANSFORM_RESOURCE_LIMIT, 0);
    {
        cJSON *copy = cJSON_Duplicate(categorical_root, 1);
        cJSON *categories = cJSON_GetObjectItemCaseSensitive(
            cJSON_GetObjectItemCaseSensitive(
                cJSON_GetArrayItem(
                    cJSON_GetObjectItemCaseSensitive(copy, "steps"), 0),
                "categorical"),
            "categories");
        assert(copy != NULL && cJSON_GetArraySize(categories) == 2);
        cJSON_DeleteItemFromArray(categories, 1);
        assert(cJSON_GetArraySize(categories) == 1);
        assert_categorical_root_import_code(
            copy, &limits, TF_TRANSFORM_CORRUPT_PLAN, 0);
        cJSON_Delete(copy);
    }

    assert(tf_transform_plan_export(
        numeric_plan, NULL, &bytes, &bytes_len, &error) == TF_TRANSFORM_OK);
    numeric_root = cJSON_ParseWithLength(
        (const char *)bytes + 52, bytes_len - 52);
    assert(numeric_root != NULL);
    step = cJSON_GetArrayItem(
        cJSON_GetObjectItemCaseSensitive(numeric_root, "steps"), 0);
    assert(strcmp(cJSON_GetObjectItemCaseSensitive(
        step, "kind")->valuestring, "numeric") == 0);
    assert_categorical_root_import_code(
        numeric_root, NULL, TF_TRANSFORM_OK, 0);

    cJSON_Delete(numeric_root);
    cJSON_Delete(categorical_root);
    tf_transform_bytes_free(&bytes, &bytes_len);
    tf_transform_plan_destroy(&numeric_plan);
    tf_transform_plan_destroy(&categorical_plan);
}

static void test_categorical_import_validation_and_limits(void) {
    const double values[] = {0.0, 1.0, 3.0, 1.0, 3.0};
    tf_transform_plan *plan = fit_f64_plan(
        "categorical_mode_none", values, 5, 2);
    tf_transform_error *error = NULL;
    uint8_t *exported = NULL;
    size_t exported_len = 0;
    cJSON *root;
    tf_transform_limits_v1 limits;
    uint8_t *limited_bytes = NULL;
    size_t limited_len = 0;

    assert(tf_transform_plan_export(
        plan, NULL, &exported, &exported_len, &error) == TF_TRANSFORM_OK);
    root = cJSON_ParseWithLength(
        (const char *)exported + 52, exported_len - 52);
    assert(root != NULL);
    assert_categorical_root_import_code(root, NULL, TF_TRANSFORM_OK, 0);

    for (int mutation = 0; mutation < 5; ++mutation) {
        cJSON *copy = cJSON_Duplicate(root, 1);
        cJSON *step = cJSON_GetArrayItem(
            cJSON_GetObjectItemCaseSensitive(copy, "steps"), 0);
        cJSON *categorical = cJSON_GetObjectItemCaseSensitive(
            step, "categorical");
        cJSON *categories = cJSON_GetObjectItemCaseSensitive(
            categorical, "categories");
        cJSON *first = cJSON_GetArrayItem(categories, 0);
        cJSON *second = cJSON_GetArrayItem(categories, 1);
        assert(copy && first && second);
        if (mutation == 0) {
            assert(cJSON_SetValuestring(
                cJSON_GetObjectItemCaseSensitive(first, "v"),
                "8000000000000000") != NULL);
        } else if (mutation == 1) {
            assert(cJSON_SetValuestring(
                cJSON_GetObjectItemCaseSensitive(first, "t"), "f32") != NULL);
        } else if (mutation == 2) {
            assert(cJSON_SetValuestring(
                cJSON_GetObjectItemCaseSensitive(second, "v"),
                "0000000000000000") != NULL);
        } else if (mutation == 3) {
            assert(cJSON_SetValuestring(
                cJSON_GetObjectItemCaseSensitive(first, "v"),
                "3ff0000000000000") != NULL);
            assert(cJSON_SetValuestring(
                cJSON_GetObjectItemCaseSensitive(second, "v"),
                "0000000000000000") != NULL);
        } else {
            cJSON *impute = cJSON_GetObjectItemCaseSensitive(
                categorical, "impute");
            cJSON *mode = cJSON_GetObjectItemCaseSensitive(impute, "value");
            assert(cJSON_SetValuestring(
                cJSON_GetObjectItemCaseSensitive(mode, "v"),
                "4000000000000000") != NULL);
        }
        assert_categorical_root_import_code(
            copy, NULL, TF_TRANSFORM_CORRUPT_PLAN, 0);
        cJSON_Delete(copy);
    }

    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    limits.max_categories_per_column = 4;
    limits.max_total_categories = 4;
    assert_categorical_root_import_code(
        root, &limits, TF_TRANSFORM_OK, 0);
    limits.max_categories_per_column = 3;
    limits.max_total_categories = 3;
    assert_categorical_root_import_code(
        root, &limits, TF_TRANSFORM_OK, 0);
    limits.max_categories_per_column = 2;
    assert(tf_transform_plan_export(
        plan, &limits, &limited_bytes, &limited_len, &error)
        == TF_TRANSFORM_RESOURCE_LIMIT);
    assert(limited_bytes == NULL && limited_len == 0);
    tf_transform_error_destroy(&error);
    assert_categorical_root_import_code(
        root, &limits, TF_TRANSFORM_RESOURCE_LIMIT, 1);
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    limits.max_total_categories = 2;
    assert_categorical_root_import_code(
        root, &limits, TF_TRANSFORM_RESOURCE_LIMIT, 1);
    {
        cJSON *copy = cJSON_Duplicate(root, 1);
        cJSON *extra = cJSON_CreateObject();
        cJSON *categories = cJSON_Duplicate(
            cJSON_GetObjectItemCaseSensitive(
                cJSON_GetObjectItemCaseSensitive(
                    cJSON_GetArrayItem(
                        cJSON_GetObjectItemCaseSensitive(copy, "steps"), 0),
                    "categorical"),
                "categories"),
            1);
        assert(copy && extra && categories);
        assert(cJSON_AddItemToObject(extra, "categories", categories));
        assert(cJSON_AddItemToObject(copy, "extra", extra));
        assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
               == TF_TRANSFORM_OK);
        limits.max_categories_per_column = 3;
        limits.max_total_categories = 5;
        assert_categorical_root_import_code(
            copy, &limits, TF_TRANSFORM_RESOURCE_LIMIT, 1);
        cJSON_Delete(copy);
    }

    cJSON_Delete(root);
    tf_transform_bytes_free(&exported, &exported_len);
    tf_transform_plan_destroy(&plan);
}

static tf_transform_code parse_label_sentinel_literal(
    const char *literal, tf_transform_recipe **out,
    tf_transform_error **error) {
    static const char marker[] = "\"sentinelLabel\":-1";
    char *text = read_file("test/vectors/prepared_transform_v1.json");
    cJSON *root = cJSON_Parse(text);
    cJSON *recipes;
    cJSON *recipe_json;
    char *serialized;
    char *position;
    char *mutated;
    size_t prefix;
    size_t literal_len;
    size_t suffix_len;
    tf_transform_limits_v1 limits;
    tf_transform_code code;
    assert(root != NULL && literal != NULL && out != NULL);
    recipes = cJSON_GetObjectItemCaseSensitive(root, "recipes");
    recipe_json = cJSON_GetObjectItemCaseSensitive(
        recipes, "categorical_mode_label_sentinel");
    serialized = cJSON_PrintUnformatted(recipe_json);
    assert(serialized != NULL);
    position = strstr(serialized, marker);
    assert(position != NULL);
    prefix = (size_t)(position - serialized) + strlen("\"sentinelLabel\":");
    literal_len = strlen(literal);
    suffix_len = strlen(position + strlen(marker));
    mutated = (char *)malloc(prefix + literal_len + suffix_len + 1);
    assert(mutated != NULL);
    memcpy(mutated, serialized, prefix);
    memcpy(mutated + prefix, literal, literal_len);
    memcpy(mutated + prefix + literal_len,
           position + strlen(marker), suffix_len + 1);
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    code = tf_transform_recipe_from_json(
        (const uint8_t *)mutated, strlen(mutated), &limits,
        out, error);
    free(mutated);
    free(serialized);
    cJSON_Delete(root);
    free(text);
    return code;
}

static void test_categorical_label_schema_collisions_and_boundaries(void) {
    static const char special_id[] = "a_:%\xc3\xa9";
    static const char generated_id[] = "a%5F%3A%25%C3%A9%3Alabel";
    static const char expected_schema[] =
        "[{\"category\":null,\"dtype\":\"float64\","
        "\"id\":\"a%5F%3A%25%C3%A9%3Alabel\","
        "\"name\":\"a%5F%3A%25%C3%A9%3Alabel\","
        "\"role\":\"label\",\"sourceId\":\"a_:%\xc3\xa9\"}]";
    static const char max_safe_text[] = "9007199254740991";
    static const char max_safe_fragment[]
        = "\"sentinelLabel\":9007199254740991";
    const char *valid_sentinels[] = {
        "-9007199254740991", "2", "9007199254740991"
    };
    const char *invalid_sentinels[] = {
        "-9007199254740992", "9007199254740992", "0.5"
    };
    wide_categorical_fixture fixture;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_plan *imported = NULL;
    tf_transform_apply *apply = NULL;
    tf_transform_recipe *recipe = NULL;
    tf_transform_error *error = NULL;
    tf_owned_dense_v1 dense;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    uint8_t *bytes = NULL;
    size_t bytes_len = 0;
    int found_fragment = 0;

    for (size_t i = 0; i < sizeof(valid_sentinels) / sizeof(valid_sentinels[0]);
         ++i) {
        assert(parse_label_sentinel_literal(
            valid_sentinels[i], &recipe, &error) == TF_TRANSFORM_OK);
        assert(recipe != NULL && error == NULL
               && recipe->columns[0].categorical_has_sentinel_label
               && recipe->columns[0].categorical_sentinel_label
                   == strtoll(valid_sentinels[i], NULL, 10));
        tf_transform_recipe_destroy(&recipe);
    }
    for (size_t i = 0;
         i < sizeof(invalid_sentinels) / sizeof(invalid_sentinels[0]); ++i) {
        assert(parse_label_sentinel_literal(
            invalid_sentinels[i], &recipe, &error)
            == TF_TRANSFORM_INVALID_RECIPE);
        assert(recipe == NULL && error != NULL);
        tf_transform_error_destroy(&error);
    }

    assert(parse_label_sentinel_literal(
        max_safe_text, &recipe, &error) == TF_TRANSFORM_OK);
    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
    assert(tf_transform_analyzer_create(
        recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
    {
        const double observed[] = {0.0};
        make_f64_table(observed, 1, &column, &table);
        assert(tf_transform_analyzer_push(analyzer, &table, &error)
               == TF_TRANSFORM_OK);
    }
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_OK);
    tf_transform_analyzer_destroy(&analyzer);
    assert(tf_transform_plan_export(
        plan, NULL, &bytes, &bytes_len, &error) == TF_TRANSFORM_OK);
    for (size_t i = 52; i + strlen(max_safe_fragment) <= bytes_len; ++i) {
        if (memcmp(bytes + i, max_safe_fragment,
                   strlen(max_safe_fragment)) == 0) {
            found_fragment = 1;
            break;
        }
    }
    assert(found_fragment);
    assert(tf_transform_plan_import(
        bytes, bytes_len, NULL, &imported, &error) == TF_TRANSFORM_OK);
    assert(tf_transform_apply_create(
        imported, &schema, NULL, &apply, &error) == TF_TRANSFORM_OK);
    {
        const double unknown[] = {1.0};
        make_f64_table(unknown, 1, &column, &table);
        assert(tf_transform_apply_run(apply, &table, &dense, &error)
               == TF_TRANSFORM_OK);
        assert(((double *)dense.data)[0] == 9007199254740991.0);
        tf_owned_dense_free(&dense);
    }
    tf_transform_apply_destroy(&apply);
    tf_transform_plan_destroy(&imported);
    tf_transform_bytes_free(&bytes, &bytes_len);
    tf_transform_plan_destroy(&plan);
    tf_transform_recipe_destroy(&recipe);

    assert(parse_label_sentinel_literal("2", &recipe, &error)
           == TF_TRANSFORM_OK);
    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
    assert(tf_transform_analyzer_create(
        recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
    {
        const double observed[] = {1.0, 2.0};
        make_f64_table(observed, 2, &column, &table);
        assert(tf_transform_analyzer_push(analyzer, &table, &error)
               == TF_TRANSFORM_OK);
    }
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_apply_create(
        plan, &schema, NULL, &apply, &error) == TF_TRANSFORM_OK);
    {
        const double unknown[] = {3.0};
        make_f64_table(unknown, 1, &column, &table);
        assert(tf_transform_apply_run(apply, &table, &dense, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_double_bits(((double *)dense.data)[0])
               == UINT64_C(0x4000000000000000));
        tf_owned_dense_free(&dense);
    }
    tf_transform_apply_destroy(&apply);
    tf_transform_plan_destroy(&plan);
    tf_transform_analyzer_destroy(&analyzer);
    tf_transform_recipe_destroy(&recipe);

    {
        const float analyze32[] = {2.0f, 1.0f, 2.0f};
        const float apply32[] = {
            1.0f, 3.0f,
            tf_transform_double_from_bits(UINT64_C(0x7ff8000000000000))
        };
        tf_transform_plan *plan32 = fit_f32_plan(
            "categorical_mode_label_other", analyze32, 3);
        make_x0_schema(TF_VIEW_FLOAT32, &field, &schema);
        assert(plan32->states[0].value.categorical.source_dtype
               == TF_VIEW_FLOAT32);
        assert(tf_transform_apply_create(
            plan32, &schema, NULL, &apply, &error) == TF_TRANSFORM_OK);
        make_f32_table(apply32, 3, &column, &table);
        assert(tf_transform_apply_run(apply, &table, &dense, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_double_bits(((double *)dense.data)[0]) == 0
               && tf_transform_double_bits(((double *)dense.data)[1])
                    == UINT64_C(0x4000000000000000)
               && tf_transform_double_bits(((double *)dense.data)[2])
                    == UINT64_C(0x3ff0000000000000));
        tf_owned_dense_free(&dense);
        tf_transform_apply_destroy(&apply);
        tf_transform_plan_destroy(&plan32);
    }

    {
        const double other_values[] = {1.0, 2.0, 2.0};
        cJSON *root;
        cJSON *copy;
        cJSON *step;
        cJSON *encode;
        tf_transform_limits_v1 limits;
        plan = fit_f64_plan(
            "categorical_mode_label_other", other_values, 3, 2);
        assert(tf_transform_plan_export(
            plan, NULL, &bytes, &bytes_len, &error) == TF_TRANSFORM_OK);
        root = cJSON_ParseWithLength(
            (const char *)bytes + 52, bytes_len - 52);
        assert(root != NULL);
        assert_categorical_root_import_code(root, NULL, TF_TRANSFORM_OK, 0);
        copy = cJSON_Duplicate(root, 1);
        step = cJSON_GetArrayItem(
            cJSON_GetObjectItemCaseSensitive(copy, "steps"), 0);
        encode = cJSON_GetObjectItemCaseSensitive(
            cJSON_GetObjectItemCaseSensitive(step, "categorical"), "encode");
        assert(cJSON_ReplaceItemInObjectCaseSensitive(
            encode, "otherOrdinal", cJSON_CreateNumber(1)));
        assert_categorical_root_import_code(
            copy, NULL, TF_TRANSFORM_CORRUPT_PLAN, 0);
        cJSON_Delete(copy);

        copy = cJSON_Duplicate(root, 1);
        step = cJSON_GetArrayItem(
            cJSON_GetObjectItemCaseSensitive(copy, "steps"), 0);
        encode = cJSON_GetObjectItemCaseSensitive(
            cJSON_GetObjectItemCaseSensitive(step, "categorical"), "encode");
        assert(cJSON_ReplaceItemInObjectCaseSensitive(
            encode, "sentinelLabel", cJSON_CreateNumber(-1)));
        assert_categorical_root_import_code(
            copy, NULL, TF_TRANSFORM_CORRUPT_PLAN, 0);
        cJSON_Delete(copy);

        assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
               == TF_TRANSFORM_OK);
        limits.max_categories_per_column = 1;
        assert_categorical_root_import_code(
            root, &limits, TF_TRANSFORM_RESOURCE_LIMIT, 1);
        cJSON_Delete(root);
        tf_transform_bytes_free(&bytes, &bytes_len);
        tf_transform_plan_destroy(&plan);
    }

    wide_categorical_fixture_init(&fixture, 1);
    wide_categorical_fixture_set_field(
        &fixture, 0, special_id, "ignored input name");
    wide_categorical_fixture_set_label(
        &fixture, 0, TF_TRANSFORM_UNKNOWN_SENTINEL, -1);
    assert(tf_transform_analyzer_create(
        fixture.recipe, &fixture.schema, NULL, &analyzer, &error)
        == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_OK);
    assert(plan->output_schema.field_count == 1
           && plan->output_schema.fields[0].role == TF_TRANSFORM_ROLE_LABEL
           && plan->output_schema.fields[0].id_len == strlen(generated_id)
           && memcmp(plan->output_schema.fields[0].id,
                     generated_id, strlen(generated_id)) == 0);
    assert(tf_transform_plan_schema_json(
        plan, TF_TRANSFORM_SCHEMA_OUTPUT, NULL,
        &bytes, &bytes_len, &error) == TF_TRANSFORM_OK);
    assert(bytes_len == strlen(expected_schema)
           && memcmp(bytes, expected_schema, bytes_len) == 0);
    tf_transform_bytes_free(&bytes, &bytes_len);
    assert(tf_transform_plan_export(
        plan, NULL, &bytes, &bytes_len, &error) == TF_TRANSFORM_OK);
    {
        tf_transform_limits_v1 limits;
        tf_transform_runtime_v1 runtime;
        uint64_t exact_allocations;
        assert(tf_transform_plan_import(
            bytes, bytes_len, NULL, &imported, &error) == TF_TRANSFORM_OK);
        exact_allocations = imported->import_allocation_count;
        assert(exact_allocations > 1);
        tf_transform_plan_destroy(&imported);
        assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
               == TF_TRANSFORM_OK);
        memset(&runtime, 0, sizeof(runtime));
        runtime.abi_version = 1;
        runtime.struct_size = (uint32_t)sizeof(runtime);
        runtime.limits = &limits;
        limits.max_allocations_per_session = exact_allocations - 1;
        assert(tf_transform_plan_import(
            bytes, bytes_len, &runtime, &imported, &error)
            == TF_TRANSFORM_RESOURCE_LIMIT);
        assert(imported == NULL);
        tf_transform_error_destroy(&error);
        limits.max_allocations_per_session = exact_allocations;
        assert(tf_transform_plan_import(
            bytes, bytes_len, &runtime, &imported, &error) == TF_TRANSFORM_OK);
        assert(imported != NULL
               && imported->import_allocation_count == exact_allocations);
        tf_transform_plan_destroy(&imported);
    }
    {
        cJSON *root = cJSON_ParseWithLength(
            (const char *)bytes + 52, bytes_len - 52);
        cJSON *copy;
        cJSON *output;
        cJSON *step;
        cJSON *encode;
        assert(root != NULL);
        assert_categorical_root_import_code(root, NULL, TF_TRANSFORM_OK, 0);

        copy = cJSON_Duplicate(root, 1);
        output = cJSON_GetArrayItem(
            cJSON_GetObjectItemCaseSensitive(copy, "outputSchema"), 0);
        assert(cJSON_ReplaceItemInObjectCaseSensitive(
            output, "role", cJSON_CreateString("value")));
        assert_categorical_root_import_code(
            copy, NULL, TF_TRANSFORM_CORRUPT_PLAN, 0);
        cJSON_Delete(copy);

        copy = cJSON_Duplicate(root, 1);
        output = cJSON_GetArrayItem(
            cJSON_GetObjectItemCaseSensitive(copy, "outputSchema"), 0);
        assert(cJSON_ReplaceItemInObjectCaseSensitive(
            output, "sourceId", cJSON_CreateString("wrong")));
        assert_categorical_root_import_code(
            copy, NULL, TF_TRANSFORM_CORRUPT_PLAN, 0);
        cJSON_Delete(copy);

        copy = cJSON_Duplicate(root, 1);
        step = cJSON_GetArrayItem(
            cJSON_GetObjectItemCaseSensitive(copy, "steps"), 0);
        encode = cJSON_GetObjectItemCaseSensitive(
            cJSON_GetObjectItemCaseSensitive(step, "categorical"), "encode");
        assert(cJSON_ReplaceItemInObjectCaseSensitive(
            encode, "sentinelLabel", cJSON_CreateNumber(0)));
        assert_categorical_root_import_code(
            copy, NULL, TF_TRANSFORM_CORRUPT_PLAN, 0);
        cJSON_Delete(copy);
        cJSON_Delete(root);
    }
    tf_transform_bytes_free(&bytes, &bytes_len);
    tf_transform_plan_destroy(&plan);
    tf_transform_analyzer_destroy(&analyzer);
    wide_categorical_fixture_clear(&fixture);

    {
        tf_transform_limits_v1 limits;
        tf_transform_runtime_v1 runtime;
        uint64_t input_resident;
        uint64_t output_resident = 0;
        uint64_t output_allocations = 0;
        uint64_t collision_resident = 0;
        uint64_t output_field_count = 0;
        uint64_t category_resident = 0;
        uint64_t category_allocations = 0;
        uint64_t plan_base;
        uint64_t persistent_peak;
        uint64_t collision_peak;
        uint64_t exact_peak;

        wide_categorical_fixture_init(&fixture, 1);
        wide_categorical_fixture_set_field(&fixture, 0, "_", "n");
        wide_categorical_fixture_set_label(
            &fixture, 0, TF_TRANSFORM_UNKNOWN_ERROR, 0);
        assert(tf_transform_analyzer_create(
            fixture.recipe, &fixture.schema, NULL, &analyzer, &error)
            == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
               == TF_TRANSFORM_OK);
        input_resident = owned_schema_resident_for_test(&analyzer->input_schema);
        assert(tf_transform_output_schema_requirements(
            &analyzer->input_schema, analyzer->recipe, analyzer, NULL,
            &analyzer->runtime, &output_field_count, &output_resident,
            &output_allocations, &collision_resident, &error)
            == TF_TRANSFORM_OK);
        assert(output_field_count == 1);
        assert(tf_transform_category_plan_requirements(
            analyzer, 0, &category_resident, &category_allocations, &error)
            == TF_TRANSFORM_OK);
        assert(output_allocations > 0 && category_allocations == 1);
        plan_base = sizeof(tf_transform_plan) + input_resident + output_resident;
        persistent_peak = plan_base + sizeof(tf_transform_column_state)
            + category_resident;
        collision_peak = plan_base + collision_resident;
        exact_peak = analyzer->resident_state_bytes
            + (persistent_peak > collision_peak ? persistent_peak : collision_peak);
        assert(exact_peak > 1);
        tf_transform_analyzer_destroy(&analyzer);

        assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
               == TF_TRANSFORM_OK);
        memset(&runtime, 0, sizeof(runtime));
        runtime.abi_version = 1;
        runtime.struct_size = (uint32_t)sizeof(runtime);
        runtime.limits = &limits;
        limits.max_resident_state_bytes = exact_peak - 1;
        assert(tf_transform_analyzer_create(
            fixture.recipe, &fixture.schema, &runtime, &analyzer, &error)
            == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_RESOURCE_LIMIT);
        assert(plan == NULL);
        tf_transform_error_destroy(&error);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_INVALID_STATE);
        tf_transform_error_destroy(&error);
        tf_transform_analyzer_destroy(&analyzer);

        limits.max_resident_state_bytes = exact_peak;
        assert(tf_transform_analyzer_create(
            fixture.recipe, &fixture.schema, &runtime, &analyzer, &error)
            == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_OK);
        assert(plan != NULL && error == NULL);
        tf_transform_plan_destroy(&plan);
        tf_transform_analyzer_destroy(&analyzer);
        wide_categorical_fixture_clear(&fixture);
    }

    {
        cJSON *root;
        cJSON *input;
        cJSON *output;
        cJSON *recipe_columns;
        cJSON *steps;
        static const char collision_id[] = "a%3Alabel";

        wide_categorical_fixture_init(&fixture, 2);
        wide_categorical_fixture_set_field(&fixture, 0, "a", "first");
        wide_categorical_fixture_set_field(&fixture, 1, "b", "second");
        wide_categorical_fixture_set_label(
            &fixture, 0, TF_TRANSFORM_UNKNOWN_ERROR, 0);
        assert(tf_transform_analyzer_create(
            fixture.recipe, &fixture.schema, NULL, &analyzer, &error)
            == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_plan_export(
            plan, NULL, &bytes, &bytes_len, &error) == TF_TRANSFORM_OK);
        root = cJSON_ParseWithLength(
            (const char *)bytes + 52, bytes_len - 52);
        assert(root != NULL);
        input = cJSON_GetObjectItemCaseSensitive(root, "inputSchema");
        output = cJSON_GetObjectItemCaseSensitive(root, "outputSchema");
        recipe_columns = cJSON_GetObjectItemCaseSensitive(
            cJSON_GetObjectItemCaseSensitive(root, "recipe"), "columns");
        steps = cJSON_GetObjectItemCaseSensitive(root, "steps");
        assert(cJSON_SetValuestring(
            cJSON_GetObjectItemCaseSensitive(cJSON_GetArrayItem(input, 1), "id"),
            collision_id) != NULL);
        assert(cJSON_SetValuestring(
            cJSON_GetObjectItemCaseSensitive(
                cJSON_GetArrayItem(recipe_columns, 1), "sourceId"),
            collision_id) != NULL);
        assert(cJSON_SetValuestring(
            cJSON_GetObjectItemCaseSensitive(cJSON_GetArrayItem(steps, 1), "sourceId"),
            collision_id) != NULL);
        assert(cJSON_SetValuestring(
            cJSON_GetObjectItemCaseSensitive(cJSON_GetArrayItem(output, 1), "id"),
            collision_id) != NULL);
        assert(cJSON_SetValuestring(
            cJSON_GetObjectItemCaseSensitive(
                cJSON_GetArrayItem(output, 1), "sourceId"),
            collision_id) != NULL);
        update_recipe_fingerprint_for_test(root);
        assert_categorical_root_import_code(
            root, NULL, TF_TRANSFORM_CORRUPT_PLAN, 0);
        cJSON_Delete(root);
        tf_transform_bytes_free(&bytes, &bytes_len);
        tf_transform_plan_destroy(&plan);
        tf_transform_analyzer_destroy(&analyzer);
        wide_categorical_fixture_clear(&fixture);
    }

    {
        tf_transform_limits_v1 limits;
        tf_transform_runtime_v1 runtime;
        wide_categorical_fixture_init(&fixture, 1);
        wide_categorical_fixture_set_field(&fixture, 0, "_", "n");
        wide_categorical_fixture_set_label(
            &fixture, 0, TF_TRANSFORM_UNKNOWN_ERROR, 0);
        assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
               == TF_TRANSFORM_OK);
        memset(&runtime, 0, sizeof(runtime));
        runtime.abi_version = 1;
        runtime.struct_size = (uint32_t)sizeof(runtime);
        runtime.limits = &limits;
        limits.max_string_bytes = 10;
        assert(tf_transform_analyzer_create(
            fixture.recipe, &fixture.schema, &runtime, &analyzer, &error)
            == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_RESOURCE_LIMIT);
        assert(plan == NULL);
        tf_transform_error_destroy(&error);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_INVALID_STATE);
        tf_transform_error_destroy(&error);
        tf_transform_analyzer_destroy(&analyzer);

        limits.max_string_bytes = 11;
        assert(tf_transform_analyzer_create(
            fixture.recipe, &fixture.schema, &runtime, &analyzer, &error)
            == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_OK);
        assert(plan->output_schema.fields[0].id_len == 11
               && memcmp(plan->output_schema.fields[0].id,
                         "%5F%3Alabel", 11) == 0);
        tf_transform_plan_destroy(&plan);
        tf_transform_analyzer_destroy(&analyzer);
        wide_categorical_fixture_clear(&fixture);
    }

    wide_categorical_fixture_init(&fixture, 2);
    wide_categorical_fixture_set_field(&fixture, 0, "a", "first");
    wide_categorical_fixture_set_field(
        &fixture, 1, "a%3Alabel", "second");
    wide_categorical_fixture_set_label(
        &fixture, 0, TF_TRANSFORM_UNKNOWN_ERROR, 0);
    wide_categorical_fixture_set_label(
        &fixture, 1, TF_TRANSFORM_UNKNOWN_ERROR, 0);
    assert(tf_transform_analyzer_create(
        fixture.recipe, &fixture.schema, NULL, &analyzer, &error)
        == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_SCHEMA_MISMATCH);
    assert(plan == NULL);
    tf_transform_error_destroy(&error);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_INVALID_STATE);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);
    wide_categorical_fixture_clear(&fixture);

    wide_categorical_fixture_init(&fixture, 2);
    wide_categorical_fixture_set_field(&fixture, 0, "a", "first");
    wide_categorical_fixture_set_field(
        &fixture, 1, "b", "a%3Alabel");
    wide_categorical_fixture_set_label(
        &fixture, 0, TF_TRANSFORM_UNKNOWN_ERROR, 0);
    wide_categorical_fixture_set_label(
        &fixture, 1, TF_TRANSFORM_UNKNOWN_ERROR, 0);
    assert(tf_transform_analyzer_create(
        fixture.recipe, &fixture.schema, NULL, &analyzer, &error)
        == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_SCHEMA_MISMATCH);
    assert(plan == NULL);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);
    wide_categorical_fixture_clear(&fixture);

}

static void test_categorical_onehot_schema_resources_and_import(void) {
    static const char *const expected_ids[] = {
        "a%3Aonehot%3A0", "a%3Aonehot%3A1", "b",
        "c%3Aonehot%3A0"
    };
    wide_categorical_fixture fixture;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_plan *imported = NULL;
    tf_transform_apply *apply = NULL;
    tf_transform_recipe *recipe = NULL;
    tf_transform_error *error = NULL;
    tf_owned_dense_v1 dense;
    uint8_t *bytes = NULL;
    uint8_t *roundtrip = NULL;
    size_t bytes_len = 0;
    size_t roundtrip_len = 0;

    wide_categorical_fixture_init(&fixture, 3);
    wide_categorical_fixture_set_field(&fixture, 0, "a", "first");
    wide_categorical_fixture_set_field(&fixture, 1, "b", "middle");
    wide_categorical_fixture_set_field(&fixture, 2, "c", "last");
    wide_categorical_fixture_set_onehot(
        &fixture, 0, TF_TRANSFORM_UNKNOWN_OTHER);
    wide_categorical_fixture_set_onehot(
        &fixture, 2, TF_TRANSFORM_UNKNOWN_ALL_ZERO);
    fixture.recipe->max_output_columns = 4;
    fixture.recipe->max_output_elements_per_apply = 16;
    assert(tf_transform_analyzer_create(
        fixture.recipe, &fixture.schema, NULL, &analyzer, &error)
        == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_OK);
    assert(plan != NULL && plan->output_schema.field_count == 4);
    for (size_t i = 0; i < 4; ++i) {
        const tf_transform_schema_field_owned *field
            = &plan->output_schema.fields[i];
        assert(field->id_len == strlen(expected_ids[i])
               && memcmp(field->id, expected_ids[i], field->id_len) == 0);
    }
    assert(plan->output_schema.fields[0].role == TF_TRANSFORM_ROLE_ONEHOT
           && plan->output_schema.fields[0].category_kind
               == TF_TRANSFORM_SCHEMA_CATEGORY_VALUE
           && plan->output_schema.fields[1].category_kind
               == TF_TRANSFORM_SCHEMA_CATEGORY_OTHER
           && plan->output_schema.fields[2].role == TF_TRANSFORM_ROLE_VALUE
           && plan->output_schema.fields[3].role
               == TF_TRANSFORM_ROLE_ONEHOT);
    assert(tf_transform_apply_create(
        plan, &fixture.schema, NULL, &apply, &error) == TF_TRANSFORM_OK);
    {
        const double values[] = {1.0, 7.0, 2.0};
        const uint64_t expected[] = {
            UINT64_C(0x0000000000000000), UINT64_C(0x3ff0000000000000),
            UINT64_C(0x401c000000000000), UINT64_C(0x0000000000000000)
        };
        tf_column_view_v1 columns[3];
        tf_table_view_v1 table = fixture.table;
        memcpy(columns, fixture.columns, sizeof(columns));
        for (size_t i = 0; i < 3; ++i) {
            columns[i].data = &values[i];
            columns[i].data_bytes = sizeof(values[i]);
        }
        table.columns = columns;
        table.columns_bytes = sizeof(columns);
        assert(tf_transform_apply_run(apply, &table, &dense, &error)
               == TF_TRANSFORM_OK);
        assert(dense.rows == 1 && dense.columns == 4);
        for (size_t i = 0; i < 4; ++i)
            assert(tf_transform_double_bits(((double *)dense.data)[i])
                   == expected[i]);
        tf_owned_dense_free(&dense);
    }
    tf_transform_apply_destroy(&apply);
    assert(tf_transform_plan_export(
        plan, NULL, &bytes, &bytes_len, &error) == TF_TRANSFORM_OK);
    assert(tf_transform_plan_import(
        bytes, bytes_len, NULL, &imported, &error) == TF_TRANSFORM_OK);
    assert(imported->output_schema.field_count == 4);
    assert(tf_transform_plan_export(
        imported, NULL, &roundtrip, &roundtrip_len, &error)
        == TF_TRANSFORM_OK);
    assert(roundtrip_len == bytes_len
           && memcmp(roundtrip, bytes, bytes_len) == 0);
    tf_transform_bytes_free(&roundtrip, &roundtrip_len);
    tf_transform_plan_destroy(&imported);
    tf_transform_bytes_free(&bytes, &bytes_len);
    tf_transform_plan_destroy(&plan);
    tf_transform_analyzer_destroy(&analyzer);
    wide_categorical_fixture_clear(&fixture);

    {
        const double values[] = {1.0, 2.0};
        tf_field_view_v1 field;
        tf_schema_view_v1 schema;
        tf_column_view_v1 column;
        tf_table_view_v1 table;
        tf_transform_limits_v1 limits;
        tf_transform_runtime_v1 runtime;
        recipe = load_vector_recipe("categorical_mode_onehot_other");
        make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
        make_f64_table(values, 2, &column, &table);
        assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
               == TF_TRANSFORM_OK);
        limits.max_output_columns = 2;
        memset(&runtime, 0, sizeof(runtime));
        runtime.abi_version = 1;
        runtime.struct_size = (uint32_t)sizeof(runtime);
        runtime.limits = &limits;
        assert(tf_transform_analyzer_create(
            recipe, &schema, &runtime, &analyzer, &error)
            == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_push(analyzer, &table, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_RESOURCE_LIMIT);
        assert(plan == NULL);
        tf_transform_error_destroy(&error);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_INVALID_STATE);
        tf_transform_error_destroy(&error);
        tf_transform_analyzer_destroy(&analyzer);
        tf_transform_recipe_destroy(&recipe);
    }

    {
        const double values[] = {1.0, 2.0};
        tf_field_view_v1 field;
        tf_schema_view_v1 schema;
        tf_column_view_v1 column;
        tf_table_view_v1 table;
        recipe = load_vector_recipe("categorical_mode_onehot_other");
        recipe->max_output_columns = 2;
        make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
        make_f64_table(values, 2, &column, &table);
        assert(tf_transform_analyzer_create(
            recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_push(analyzer, &table, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_RESOURCE_LIMIT);
        assert(plan == NULL);
        tf_transform_error_destroy(&error);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_INVALID_STATE);
        tf_transform_error_destroy(&error);
        tf_transform_analyzer_destroy(&analyzer);
        tf_transform_recipe_destroy(&recipe);
    }

    {
        tf_transform_limits_v1 limits;
        tf_transform_runtime_v1 runtime;
        uint64_t input_resident;
        uint64_t output_resident = 0;
        uint64_t output_allocations = 0;
        uint64_t collision_resident = 0;
        uint64_t output_field_count = 0;
        uint64_t category_resident = 0;
        uint64_t category_allocations = 0;
        uint64_t exact_peak;
        uint64_t exact_allocations;

        wide_categorical_fixture_init(&fixture, 1);
        wide_categorical_fixture_set_field(&fixture, 0, "_", "n");
        wide_categorical_fixture_set_onehot(
            &fixture, 0, TF_TRANSFORM_UNKNOWN_ERROR);
        assert(tf_transform_analyzer_create(
            fixture.recipe, &fixture.schema, NULL, &analyzer, &error)
            == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
               == TF_TRANSFORM_OK);
        input_resident = owned_schema_resident_for_test(&analyzer->input_schema);
        assert(tf_transform_output_schema_requirements(
            &analyzer->input_schema, analyzer->recipe, analyzer, NULL,
            &analyzer->runtime, &output_field_count, &output_resident,
            &output_allocations, &collision_resident, &error)
            == TF_TRANSFORM_OK);
        assert(output_field_count == 1 && output_allocations > 0);
        assert(tf_transform_category_plan_requirements(
            analyzer, 0, &category_resident, &category_allocations, &error)
            == TF_TRANSFORM_OK);
        assert(category_allocations == 1);
        exact_peak = analyzer->resident_state_bytes + sizeof(tf_transform_plan)
            + input_resident + output_resident
            + sizeof(tf_transform_column_state) + category_resident
            + collision_resident;
        assert(exact_peak > 1);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_OK);
        exact_allocations = analyzer->allocation_count;
        assert(exact_allocations > 1 && plan != NULL);
        tf_transform_plan_destroy(&plan);
        tf_transform_analyzer_destroy(&analyzer);

        assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
               == TF_TRANSFORM_OK);
        memset(&runtime, 0, sizeof(runtime));
        runtime.abi_version = 1;
        runtime.struct_size = (uint32_t)sizeof(runtime);
        runtime.limits = &limits;
        limits.max_resident_state_bytes = exact_peak - 1;
        assert(tf_transform_analyzer_create(
            fixture.recipe, &fixture.schema, &runtime, &analyzer, &error)
            == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_RESOURCE_LIMIT);
        assert(plan == NULL);
        tf_transform_error_destroy(&error);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_INVALID_STATE);
        tf_transform_error_destroy(&error);
        tf_transform_analyzer_destroy(&analyzer);

        limits.max_resident_state_bytes = exact_peak;
        assert(tf_transform_analyzer_create(
            fixture.recipe, &fixture.schema, &runtime, &analyzer, &error)
            == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_OK);
        assert(plan != NULL && error == NULL);
        tf_transform_plan_destroy(&plan);
        tf_transform_analyzer_destroy(&analyzer);

        assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
               == TF_TRANSFORM_OK);
        limits.max_allocations_per_session = exact_allocations - 1;
        assert(tf_transform_analyzer_create(
            fixture.recipe, &fixture.schema, &runtime, &analyzer, &error)
            == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_RESOURCE_LIMIT);
        assert(plan == NULL);
        tf_transform_error_destroy(&error);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_INVALID_STATE);
        tf_transform_error_destroy(&error);
        tf_transform_analyzer_destroy(&analyzer);

        limits.max_allocations_per_session = exact_allocations;
        assert(tf_transform_analyzer_create(
            fixture.recipe, &fixture.schema, &runtime, &analyzer, &error)
            == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_OK);
        assert(plan != NULL && error == NULL
               && analyzer->allocation_count == exact_allocations);
        tf_transform_plan_destroy(&plan);
        tf_transform_analyzer_destroy(&analyzer);
        wide_categorical_fixture_clear(&fixture);
    }

    {
        const double values[] = {1.0, 2.0};
        cJSON *root;
        cJSON *copy;
        cJSON *output;
        cJSON *semantic;
        tf_transform_limits_v1 limits;
        uint64_t exact_import_allocations;
        plan = fit_f64_plan(
            "categorical_mode_onehot_other", values, 2, 1);
        assert(tf_transform_plan_export(
            plan, NULL, &bytes, &bytes_len, &error) == TF_TRANSFORM_OK);
        assert(tf_transform_plan_import(
            bytes, bytes_len, NULL, &imported, &error) == TF_TRANSFORM_OK);
        exact_import_allocations = imported->import_allocation_count;
        assert(exact_import_allocations > 1);
        tf_transform_plan_destroy(&imported);
        root = cJSON_ParseWithLength(
            (const char *)bytes + 52, bytes_len - 52);
        assert(root != NULL);
        assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
               == TF_TRANSFORM_OK);

        limits.max_allocations_per_session = exact_import_allocations - 1;
        assert_categorical_root_import_code(
            root, &limits, TF_TRANSFORM_RESOURCE_LIMIT, 0);
        limits.max_allocations_per_session = exact_import_allocations;
        assert_categorical_root_import_code(
            root, &limits, TF_TRANSFORM_OK, 0);

        limits.max_output_columns = 2;
        assert_categorical_root_import_code(
            root, &limits, TF_TRANSFORM_RESOURCE_LIMIT, 0);

        limits.max_output_columns = 3;
        limits.max_output_elements_per_call = UINT64_C(134217727);
        assert_categorical_root_import_code(
            root, &limits, TF_TRANSFORM_RESOURCE_LIMIT, 0);
        limits.max_output_elements_per_call = UINT64_C(134217728);
        assert_categorical_root_import_code(
            root, &limits, TF_TRANSFORM_OK, 0);

        copy = cJSON_Duplicate(root, 1);
        semantic = cJSON_GetObjectItemCaseSensitive(
            cJSON_GetObjectItemCaseSensitive(copy, "recipe"),
            "semanticLimits");
        assert(cJSON_ReplaceItemInObjectCaseSensitive(
            semantic, "maxOutputColumns", cJSON_CreateNumber(2)));
        update_recipe_fingerprint_for_test(copy);
        limits.max_output_columns = 1;
        assert_categorical_root_import_code(
            copy, &limits, TF_TRANSFORM_CORRUPT_PLAN, 0);
        cJSON_Delete(copy);

        copy = cJSON_Duplicate(root, 1);
        output = cJSON_GetObjectItemCaseSensitive(copy, "outputSchema");
        cJSON_DeleteItemFromArray(output, 2);
        assert_categorical_root_import_code(
            copy, &limits, TF_TRANSFORM_CORRUPT_PLAN, 0);
        cJSON_Delete(copy);

        copy = cJSON_Duplicate(root, 1);
        output = cJSON_GetArrayItem(
            cJSON_GetObjectItemCaseSensitive(copy, "outputSchema"), 0);
        assert(cJSON_ReplaceItemInObjectCaseSensitive(
            output, "category", cJSON_CreateObject()));
        limits.max_output_columns = 3;
        assert_categorical_root_import_code(
            copy, &limits, TF_TRANSFORM_CORRUPT_PLAN, 0);
        cJSON_Delete(copy);

        cJSON_Delete(root);
        tf_transform_bytes_free(&bytes, &bytes_len);
        tf_transform_plan_destroy(&plan);
    }

    {
        const double analyze_values[] = {1.0, 2.0};
        const double apply_values[] = {1.0, 3.0};
        tf_field_view_v1 field;
        tf_schema_view_v1 schema;
        tf_column_view_v1 column;
        tf_table_view_v1 table;
        tf_transform_limits_v1 limits;
        tf_transform_runtime_v1 runtime;

        plan = fit_f64_plan(
            "categorical_mode_onehot_other", analyze_values, 2, 1);
        make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
        make_f64_table(apply_values, 2, &column, &table);
        assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
               == TF_TRANSFORM_OK);
        memset(&runtime, 0, sizeof(runtime));
        runtime.abi_version = 1;
        runtime.struct_size = (uint32_t)sizeof(runtime);
        runtime.limits = &limits;
        limits.max_output_elements_per_call = 5;
        assert(tf_transform_apply_create(
            plan, &schema, &runtime, &apply, &error) == TF_TRANSFORM_OK);
        assert(tf_transform_apply_run(apply, &table, &dense, &error)
               == TF_TRANSFORM_RESOURCE_LIMIT);
        assert(dense.data == NULL);
        tf_transform_error_destroy(&error);
        assert(tf_transform_apply_run(apply, &table, &dense, &error)
               == TF_TRANSFORM_INVALID_STATE);
        tf_transform_error_destroy(&error);
        tf_transform_apply_destroy(&apply);

        limits.max_output_elements_per_call = 6;
        assert(tf_transform_apply_create(
            plan, &schema, &runtime, &apply, &error) == TF_TRANSFORM_OK);
        assert(tf_transform_apply_run(apply, &table, &dense, &error)
               == TF_TRANSFORM_OK);
        assert(dense.rows == 2 && dense.columns == 3);
        tf_owned_dense_free(&dense);
        tf_transform_apply_destroy(&apply);
        tf_transform_plan_destroy(&plan);

        for (uint64_t semantic_limit = 5; semantic_limit <= 6;
             ++semantic_limit) {
            recipe = load_vector_recipe("categorical_mode_onehot_other");
            recipe->max_output_elements_per_apply = semantic_limit;
            make_f64_table(analyze_values, 2, &column, &table);
            assert(tf_transform_analyzer_create(
                recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
            assert(tf_transform_analyzer_push(analyzer, &table, &error)
                   == TF_TRANSFORM_OK);
            assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
                   == TF_TRANSFORM_OK);
            make_f64_table(apply_values, 2, &column, &table);
            assert(tf_transform_apply_create(
                plan, &schema, NULL, &apply, &error) == TF_TRANSFORM_OK);
            if (semantic_limit == 5) {
                assert(tf_transform_apply_run(apply, &table, &dense, &error)
                       == TF_TRANSFORM_RESOURCE_LIMIT);
                assert(dense.data == NULL);
                tf_transform_error_destroy(&error);
            } else {
                assert(tf_transform_apply_run(apply, &table, &dense, &error)
                       == TF_TRANSFORM_OK);
                tf_owned_dense_free(&dense);
            }
            tf_transform_apply_destroy(&apply);
            tf_transform_plan_destroy(&plan);
            tf_transform_analyzer_destroy(&analyzer);
            tf_transform_recipe_destroy(&recipe);
        }
    }

    {
        static const char source_id[] = "\xC3\xA9:\xF0\x9F\x94\xA5";
        static const char ordinal_ten_id[]
            = "%C3%A9%3A%F0%9F%94%A5%3Aonehot%3A10";
        double analyze_values[11];
        const double apply_values[] = {10.0};
        tf_field_view_v1 field;
        tf_schema_view_v1 schema;
        tf_column_view_v1 column;
        tf_table_view_v1 table;
        char *owned_source;
        for (size_t i = 0; i < 11; ++i) analyze_values[i] = (double)i;
        recipe = load_vector_recipe("categorical_mode_onehot_all_zero");
        owned_source = (char *)malloc(sizeof(source_id));
        assert(owned_source != NULL);
        memcpy(owned_source, source_id, sizeof(source_id));
        free(recipe->columns[0].source_id);
        recipe->columns[0].source_id = owned_source;
        recipe->columns[0].source_id_len = sizeof(source_id) - 1;
        make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
        field.id_utf8 = (const uint8_t *)source_id;
        field.id_bytes = sizeof(source_id) - 1;
        field.name_utf8 = (const uint8_t *)source_id;
        field.name_bytes = sizeof(source_id) - 1;
        make_f64_table(analyze_values, 11, &column, &table);
        assert(tf_transform_analyzer_create(
            recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_push(analyzer, &table, &error)
               == TF_TRANSFORM_OK);
        assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
               == TF_TRANSFORM_OK);
        assert(plan->output_schema.field_count == 11);
        assert(plan->output_schema.fields[10].id_len
                   == strlen(ordinal_ten_id)
               && memcmp(plan->output_schema.fields[10].id,
                         ordinal_ten_id, strlen(ordinal_ten_id)) == 0
               && plan->output_schema.fields[10].name_len
                   == strlen(ordinal_ten_id)
               && memcmp(plan->output_schema.fields[10].name,
                         ordinal_ten_id, strlen(ordinal_ten_id)) == 0);
        assert(tf_transform_plan_export(
            plan, NULL, &bytes, &bytes_len, &error) == TF_TRANSFORM_OK);
        assert(tf_transform_plan_import(
            bytes, bytes_len, NULL, &imported, &error) == TF_TRANSFORM_OK);
        assert(imported->output_schema.field_count == 11
               && imported->output_schema.fields[10].id_len
                   == strlen(ordinal_ten_id)
               && memcmp(imported->output_schema.fields[10].id,
                         ordinal_ten_id, strlen(ordinal_ten_id)) == 0);
        assert(tf_transform_plan_export(
            imported, NULL, &roundtrip, &roundtrip_len, &error)
               == TF_TRANSFORM_OK);
        assert(roundtrip_len == bytes_len
               && memcmp(roundtrip, bytes, bytes_len) == 0);
        tf_transform_bytes_free(&roundtrip, &roundtrip_len);
        assert(tf_transform_apply_create(
            imported, &schema, NULL, &apply, &error) == TF_TRANSFORM_OK);
        make_f64_table(apply_values, 1, &column, &table);
        assert(tf_transform_apply_run(apply, &table, &dense, &error)
               == TF_TRANSFORM_OK);
        assert(dense.rows == 1 && dense.columns == 11);
        for (size_t i = 0; i < 11; ++i)
            assert(tf_transform_double_bits(((double *)dense.data)[i])
                   == (i == 10 ? UINT64_C(0x3ff0000000000000) : 0));
        tf_owned_dense_free(&dense);
        tf_transform_apply_destroy(&apply);
        tf_transform_plan_destroy(&imported);
        tf_transform_bytes_free(&bytes, &bytes_len);
        tf_transform_plan_destroy(&plan);
        tf_transform_analyzer_destroy(&analyzer);
        tf_transform_recipe_destroy(&recipe);
    }

    wide_categorical_fixture_init(&fixture, 2);
    wide_categorical_fixture_set_field(&fixture, 0, "a", "first");
    wide_categorical_fixture_set_field(
        &fixture, 1, "a%3Aonehot%3A0", "second");
    wide_categorical_fixture_set_onehot(
        &fixture, 0, TF_TRANSFORM_UNKNOWN_ERROR);
    fixture.recipe->max_output_columns = 2;
    assert(tf_transform_analyzer_create(
        fixture.recipe, &fixture.schema, NULL, &analyzer, &error)
        == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_SCHEMA_MISMATCH);
    assert(plan == NULL);
    tf_transform_error_destroy(&error);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_INVALID_STATE);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);
    wide_categorical_fixture_clear(&fixture);

    wide_categorical_fixture_init(&fixture, 2);
    wide_categorical_fixture_set_field(&fixture, 0, "a", "first");
    wide_categorical_fixture_set_field(
        &fixture, 1, "second", "a%3Aonehot%3A0");
    wide_categorical_fixture_set_onehot(
        &fixture, 0, TF_TRANSFORM_UNKNOWN_ERROR);
    fixture.recipe->max_output_columns = 2;
    assert(tf_transform_analyzer_create(
        fixture.recipe, &fixture.schema, NULL, &analyzer, &error)
        == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_push(analyzer, &fixture.table, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_SCHEMA_MISMATCH);
    assert(plan == NULL);
    tf_transform_error_destroy(&error);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_INVALID_STATE);
    tf_transform_error_destroy(&error);
    tf_transform_analyzer_destroy(&analyzer);
    wide_categorical_fixture_clear(&fixture);
}

static void test_categorical_onehot_apply_cancellation_sweep(void) {
    const size_t category_count = TF_TRANSFORM_CANCEL_ITERS_V1 * 3 + 1;
    double *values = (double *)malloc(category_count * sizeof(*values));
    const double apply_value[] = {0.0};
    tf_transform_recipe *recipe
        = load_vector_recipe("categorical_mode_onehot_all_zero");
    tf_field_view_v1 field;
    tf_schema_view_v1 schema;
    tf_column_view_v1 column;
    tf_table_view_v1 table;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_apply *apply = NULL;
    tf_transform_error *error = NULL;
    tf_owned_dense_v1 dense;
    tf_transform_limits_v1 limits;
    tf_transform_runtime_v1 runtime;
    cancel_counter counter = {0, 0};
    size_t apply_polls;
    assert(values != NULL);
    for (size_t i = 0; i < category_count; ++i) values[i] = (double)i;
    recipe->max_output_columns = category_count;
    recipe->max_output_elements_per_apply = category_count;
    make_x0_schema(TF_VIEW_FLOAT64, &field, &schema);
    make_f64_table(values, category_count, &column, &table);
    assert(tf_transform_analyzer_create(
        recipe, &schema, NULL, &analyzer, &error) == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_push(analyzer, &table, &error)
           == TF_TRANSFORM_OK);
    assert(tf_transform_analyzer_finalize(analyzer, &plan, &error)
           == TF_TRANSFORM_OK);
    assert(plan->output_schema.field_count == category_count);
    tf_transform_analyzer_destroy(&analyzer);
    tf_transform_recipe_destroy(&recipe);

    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.limits = &limits;
    runtime.cancel = cancel_on_call;
    runtime.cancel_user = &counter;
    assert(tf_transform_apply_create(
        plan, &schema, &runtime, &apply, &error) == TF_TRANSFORM_OK);
    make_f64_table(apply_value, 1, &column, &table);
    {
        size_t baseline = counter.calls;
        assert(tf_transform_apply_run(apply, &table, &dense, &error)
               == TF_TRANSFORM_OK);
        apply_polls = counter.calls - baseline;
        assert(apply_polls > 3);
        tf_owned_dense_free(&dense);
    }
    tf_transform_apply_destroy(&apply);
    for (size_t cancel_offset = 1; cancel_offset <= apply_polls;
         ++cancel_offset) {
        counter.cancel_at = 0;
        assert(tf_transform_apply_create(
            plan, &schema, &runtime, &apply, &error) == TF_TRANSFORM_OK);
        counter.cancel_at = counter.calls + cancel_offset;
        assert(tf_transform_apply_run(apply, &table, &dense, &error)
               == TF_TRANSFORM_CANCELLED);
        assert(error != NULL
               && tf_transform_error_get_code(error) == TF_TRANSFORM_CANCELLED);
        tf_transform_error_destroy(&error);
        counter.cancel_at = 0;
        assert(tf_transform_apply_run(apply, &table, &dense, &error)
               == TF_TRANSFORM_INVALID_STATE);
        tf_transform_error_destroy(&error);
        tf_transform_apply_destroy(&apply);
    }
    tf_transform_plan_destroy(&plan);
    free(values);
}

static void test_oversized_tagged_state_is_bounded(void) {
    const size_t string_len = TF_TRANSFORM_CANCEL_BYTES_V1 * 3 + 17;
    char *text = read_file("test/vectors/tftr_malformed_v1.json");
    cJSON *root = cJSON_Parse(text);
    cJSON *base;
    cJSON *copy;
    cJSON *step;
    cJSON *numeric;
    cJSON *normalize;
    cJSON *scale;
    cJSON *bits;
    char *oversized = (char *)malloc(string_len + 1);
    tf_transform_limits_v1 limits;
    tf_transform_runtime_v1 runtime;
    cancel_counter counter = {0, 0};
    tf_transform_plan *plan = NULL;
    tf_transform_error *error = NULL;
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    uint8_t *tftr;
    size_t tftr_len = 0;
    assert(root != NULL && oversized != NULL);
    memset(oversized, '0', string_len);
    oversized[string_len] = '\0';
    base = cJSON_GetObjectItemCaseSensitive(root, "base");
    copy = cJSON_Duplicate(
        cJSON_GetObjectItemCaseSensitive(base, "canonicalPayload"), 1);
    assert(copy != NULL);
    step = cJSON_GetArrayItem(
        cJSON_GetObjectItemCaseSensitive(copy, "steps"), 0);
    numeric = cJSON_GetObjectItemCaseSensitive(step, "numeric");
    normalize = cJSON_GetObjectItemCaseSensitive(numeric, "normalize");
    scale = cJSON_GetObjectItemCaseSensitive(normalize, "scale");
    bits = cJSON_GetObjectItemCaseSensitive(scale, "v");
    assert(cJSON_SetValuestring(bits, oversized) != NULL);
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    assert(tf_transform_json_print_canonical(
        copy, &limits, &payload, &payload_len, &error) == TF_TRANSFORM_OK);
    assert(payload != NULL && error == NULL);
    tftr = wrap_tftr_test(
        payload, payload_len, 1, 0, 52, payload_len, "TFTR", &tftr_len);
    memset(&runtime, 0, sizeof(runtime));
    runtime.abi_version = 1;
    runtime.struct_size = (uint32_t)sizeof(runtime);
    runtime.cancel = cancel_on_call;
    runtime.cancel_user = &counter;
    assert(tf_transform_plan_import(
        tftr, tftr_len, &runtime, &plan, &error)
        == TF_TRANSFORM_CORRUPT_PLAN);
    assert(plan == NULL && error != NULL
           && tf_transform_error_get_code(error) == TF_TRANSFORM_CORRUPT_PLAN
           && counter.calls > 3);
    tf_transform_error_destroy(&error);
    counter.calls = 0;
    counter.cancel_at = 3;
    assert(tf_transform_plan_import(
        tftr, tftr_len, &runtime, &plan, &error)
        == TF_TRANSFORM_CANCELLED);
    assert(plan == NULL && error != NULL
           && tf_transform_error_get_code(error) == TF_TRANSFORM_CANCELLED
           && counter.calls >= counter.cancel_at);
    tf_transform_error_destroy(&error);
    free(tftr);
    tf_transform_bytes_free(&payload, &payload_len);
    free(oversized);
    cJSON_Delete(copy);
    cJSON_Delete(root);
    free(text);
}

static uint8_t *reordered_payload_test(
    const cJSON *base, size_t *out_len) {
    static const char *const keys[] = {
        "version", "steps", "recipeSha256", "recipe",
        "outputSchema", "inputSchema", "format"
    };
    char *values[sizeof(keys) / sizeof(keys[0])] = {0};
    size_t len = 2;
    uint8_t *result;
    size_t offset = 0;
    for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); ++i) {
        values[i] = cJSON_PrintUnformatted(
            cJSON_GetObjectItemCaseSensitive(base, keys[i]));
        assert(values[i] != NULL);
        len += strlen(keys[i]) + strlen(values[i]) + 3;
        if (i != 0) ++len;
    }
    result = (uint8_t *)malloc(len);
    assert(result != NULL);
    result[offset++] = '{';
    for (size_t i = 0; i < sizeof(keys) / sizeof(keys[0]); ++i) {
        size_t key_len = strlen(keys[i]);
        size_t value_len = strlen(values[i]);
        if (i != 0) result[offset++] = ',';
        result[offset++] = '"';
        memcpy(result + offset, keys[i], key_len);
        offset += key_len;
        result[offset++] = '"';
        result[offset++] = ':';
        memcpy(result + offset, values[i], value_len);
        offset += value_len;
        free(values[i]);
    }
    result[offset++] = '}';
    assert(offset == len);
    *out_len = len;
    return result;
}

static uint8_t *canonical_mutation_payload_test(
    const cJSON *base, const char *mutation, size_t *out_len) {
    cJSON *copy = cJSON_Duplicate(base, 1);
    tf_transform_limits_v1 limits;
    tf_transform_error *error = NULL;
    uint8_t *payload = NULL;
    assert(copy != NULL);
    if (strcmp(mutation, "unknown_top_key_canonical_rehashed") == 0) {
        assert(cJSON_AddNumberToObject(copy, "unknown", 1) != NULL);
    } else if (strcmp(mutation, "nonfinite_scale_canonical_rehashed") == 0) {
        cJSON *steps = cJSON_GetObjectItemCaseSensitive(copy, "steps");
        cJSON *step = cJSON_GetArrayItem(steps, 0);
        cJSON *numeric = cJSON_GetObjectItemCaseSensitive(step, "numeric");
        cJSON *normalize = cJSON_GetObjectItemCaseSensitive(numeric, "normalize");
        cJSON *scale = cJSON_GetObjectItemCaseSensitive(normalize, "scale");
        assert(cJSON_SetValuestring(
            cJSON_GetObjectItemCaseSensitive(scale, "v"),
            "7ff0000000000000") != NULL);
    } else if (strcmp(
                   mutation, "recipe_sha_mismatch_canonical_rehashed") == 0) {
        assert(cJSON_SetValuestring(
            cJSON_GetObjectItemCaseSensitive(copy, "recipeSha256"),
            "0000000000000000000000000000000000000000000000000000000000000000")
            != NULL);
    } else if (strcmp(
                   mutation, "step_source_mismatch_canonical_rehashed") == 0) {
        cJSON *step = cJSON_GetArrayItem(
            cJSON_GetObjectItemCaseSensitive(copy, "steps"), 0);
        assert(cJSON_SetValuestring(
            cJSON_GetObjectItemCaseSensitive(step, "sourceId"), "x1") != NULL);
    } else {
        assert(strcmp(mutation, "plan_version_2_canonical_rehashed") == 0);
        cJSON_SetNumberValue(
            cJSON_GetObjectItemCaseSensitive(copy, "version"), 2);
    }
    assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
           == TF_TRANSFORM_OK);
    assert(tf_transform_json_print_canonical(
        copy, &limits, &payload, out_len, &error) == TF_TRANSFORM_OK);
    assert(error == NULL && payload != NULL);
    cJSON_Delete(copy);
    return payload;
}

static void test_shared_tftr_case_table(void) {
    char *text = read_file("test/vectors/tftr_malformed_v1.json");
    cJSON *root = cJSON_Parse(text);
    const cJSON *base;
    const cJSON *base_payload_json;
    const cJSON *cases;
    const cJSON *hex;
    uint8_t *base_bytes;
    size_t base_len;
    const uint8_t *base_payload;
    size_t base_payload_len;
    size_t executed = 0;
    size_t deferred = 0;
    assert(root != NULL);
    base = cJSON_GetObjectItemCaseSensitive(root, "base");
    base_payload_json = cJSON_GetObjectItemCaseSensitive(base, "canonicalPayload");
    cases = cJSON_GetObjectItemCaseSensitive(root, "cases");
    hex = cJSON_GetObjectItemCaseSensitive(base, "tftrHex");
    base_bytes = decode_hex(hex->valuestring, &base_len);
    base_payload = base_bytes + 52;
    base_payload_len = base_len - 52;
    for (int i = 0; i < cJSON_GetArraySize(cases); ++i) {
        const cJSON *entry = cJSON_GetArrayItem(cases, i);
        const char *mutation = cJSON_GetObjectItemCaseSensitive(
            entry, "mutation")->valuestring;
        tf_transform_code expected = (tf_transform_code)
            cJSON_GetObjectItemCaseSensitive(entry, "expectedCode")->valueint;
        const cJSON *host_limits = cJSON_GetObjectItemCaseSensitive(
            entry, "hostLimits");
        const cJSON *runtime_probe = cJSON_GetObjectItemCaseSensitive(
            entry, "runtimeProbe");
        uint8_t *mutated = NULL;
        size_t mutated_len = 0;
        tf_transform_plan *plan = NULL;
        tf_transform_error *error = NULL;
        tf_transform_runtime_v1 runtime;
        tf_transform_limits_v1 limits;
        tf_transform_code code;

        if (runtime_probe) {
            assert(expected == TF_TRANSFORM_UNSUPPORTED_RUNTIME);
            assert(tf_transform_plan_import(
                base_bytes, base_len, NULL, &plan, &error) == TF_TRANSFORM_OK);
            tf_transform_plan_destroy(&plan);
            ++deferred;
            continue;
        }
        if (strcmp(mutation, "none") == 0) {
            mutated = (uint8_t *)malloc(base_len);
            assert(mutated != NULL);
            memcpy(mutated, base_bytes, base_len);
            mutated_len = base_len;
        } else if (strcmp(mutation, "bad_magic") == 0) {
            mutated = (uint8_t *)malloc(base_len);
            assert(mutated != NULL);
            memcpy(mutated, base_bytes, base_len);
            mutated[0] = 'X';
            mutated_len = base_len;
        } else if (strcmp(mutation, "envelope_version_2") == 0
                   || strcmp(mutation, "envelope_flags_1") == 0
                   || strcmp(mutation, "header_length_51") == 0
                   || strcmp(mutation, "payload_length_max") == 0) {
            mutated = wrap_tftr_test(
                base_payload, base_payload_len,
                strcmp(mutation, "envelope_version_2") == 0 ? 2 : 1,
                strcmp(mutation, "envelope_flags_1") == 0 ? 1 : 0,
                strcmp(mutation, "header_length_51") == 0 ? 51 : 52,
                strcmp(mutation, "payload_length_max") == 0
                    ? UINT64_MAX : (uint64_t)base_payload_len,
                "TFTR", &mutated_len);
        } else if (strcmp(mutation, "truncate_one") == 0) {
            mutated_len = base_len - 1;
            mutated = (uint8_t *)malloc(mutated_len);
            assert(mutated != NULL);
            memcpy(mutated, base_bytes, mutated_len);
        } else if (strcmp(mutation, "append_zero") == 0) {
            mutated_len = base_len + 1;
            mutated = (uint8_t *)malloc(mutated_len);
            assert(mutated != NULL);
            memcpy(mutated, base_bytes, base_len);
            mutated[base_len] = 0;
        } else if (strcmp(mutation, "flip_payload_without_hash") == 0) {
            mutated_len = base_len;
            mutated = (uint8_t *)malloc(mutated_len);
            assert(mutated != NULL);
            memcpy(mutated, base_bytes, base_len);
            mutated[mutated_len - 1] ^= 1u;
        } else {
            uint8_t *payload = NULL;
            size_t payload_len = 0;
            if (strcmp(mutation, "reordered_top_level_rehashed") == 0) {
                payload = reordered_payload_test(base_payload_json, &payload_len);
            } else if (strcmp(mutation, "whitespace_rehashed") == 0) {
                payload_len = base_payload_len + 1;
                payload = (uint8_t *)malloc(payload_len);
                assert(payload != NULL);
                memcpy(payload, base_payload, base_payload_len);
                payload[base_payload_len] = '\n';
            } else if (strcmp(mutation, "alternate_escape_rehashed") == 0) {
                const uint8_t needle[] = {'"', 'x', '0', '"'};
                const uint8_t replacement[] = {
                    '"', '\\', 'u', '0', '0', '7', '8', '0', '"'
                };
                const uint8_t *found = NULL;
                for (size_t j = 0; j + sizeof(needle) <= base_payload_len; ++j) {
                    if (memcmp(base_payload + j, needle, sizeof(needle)) == 0) {
                        found = base_payload + j;
                        break;
                    }
                }
                assert(found != NULL);
                payload_len = base_payload_len
                    - sizeof(needle) + sizeof(replacement);
                payload = (uint8_t *)malloc(payload_len);
                assert(payload != NULL);
                {
                    size_t prefix = (size_t)(found - base_payload);
                    memcpy(payload, base_payload, prefix);
                    memcpy(payload + prefix, replacement, sizeof(replacement));
                    memcpy(payload + prefix + sizeof(replacement),
                           found + sizeof(needle),
                           base_payload_len - prefix - sizeof(needle));
                }
            } else if (strcmp(mutation, "duplicate_top_key_rehashed") == 0) {
                static const uint8_t suffix[] = ",\"version\":1}";
                payload_len = base_payload_len - 1 + sizeof(suffix) - 1;
                payload = (uint8_t *)malloc(payload_len);
                assert(payload != NULL);
                memcpy(payload, base_payload, base_payload_len - 1);
                memcpy(payload + base_payload_len - 1, suffix, sizeof(suffix) - 1);
            } else {
                payload = canonical_mutation_payload_test(
                    base_payload_json, mutation, &payload_len);
            }
            mutated = wrap_tftr_test(
                payload, payload_len, 1, 0, 52, payload_len,
                "TFTR", &mutated_len);
            free(payload);
        }
        memset(&runtime, 0, sizeof(runtime));
        runtime.abi_version = 1;
        runtime.struct_size = (uint32_t)sizeof(runtime);
        if (host_limits) {
            assert(tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
                   == TF_TRANSFORM_OK);
            limits.max_plan_bytes = 52;
            runtime.limits = &limits;
            code = tf_transform_plan_import(
                mutated, mutated_len, &runtime, &plan, &error);
        } else {
            code = tf_transform_plan_import(
                mutated, mutated_len, NULL, &plan, &error);
        }
        assert(code == expected);
        if (code == TF_TRANSFORM_OK) assert(plan != NULL && error == NULL);
        else assert(plan == NULL && error != NULL);
        tf_transform_error_destroy(&error);
        tf_transform_plan_destroy(&plan);
        free(mutated);
        ++executed;
    }
    assert(executed == 19 && deferred == 1);
    free(base_bytes);
    cJSON_Delete(root);
    free(text);
}

int main(void) {
    test_abi_and_limits();
    test_sqrt_vectors();
    test_sha256();
    test_recipe_vectors_and_canonical_json();
    test_fp_guard_restores_rounding();
    test_guarded_callback_fp_mutation();
    test_numeric_lifecycle_and_schema();
    test_numeric_failure_is_terminal();
    test_numeric_vector_semantics();
    test_categorical_mode_semantics();
    test_categorical_subnormal_fp_environment();
    test_categorical_mode_resources_and_cancellation();
    test_kind_inference_release_and_domain();
    test_wide_categorical_lifecycle_cancellation();
    test_median_blocks_limits_and_cancellation();
    test_shared_semantic_case_table();
    test_shared_validation_case_table();
    test_f32_stride_validity_and_apply_terminal();
    test_numeric_cancellation_is_terminal();
    test_runtime_safe_points_and_parser_limits();
    test_session_allocation_limits();
    test_json_construction_preflight_limits();
    test_long_schema_runtime_cancellation();
    test_valid_import_cancellation_sweep();
    test_tftr_baseline_and_malformed();
    test_median_negative_zero_import_rejected();
    test_kind_inference_import_contract();
    test_categorical_import_validation_and_limits();
    test_categorical_label_schema_collisions_and_boundaries();
    test_categorical_onehot_schema_resources_and_import();
    test_categorical_onehot_apply_cancellation_sweep();
    test_oversized_tagged_state_is_bounded();
    test_shared_tftr_case_table();
    puts("prepared transform primitive tests: OK");
    return 0;
}
