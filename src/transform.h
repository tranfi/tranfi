/* Generic prepared-transform C API. */

#ifndef TRANFI_TRANSFORM_H
#define TRANFI_TRANSFORM_H

#include "tranfi.h"
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum tf_transform_code {
    TF_TRANSFORM_OK = 0,
    TF_TRANSFORM_INVALID_ARGUMENT = 100,
    TF_TRANSFORM_INVALID_RECIPE = 101,
    TF_TRANSFORM_SCHEMA_MISMATCH = 102,
    TF_TRANSFORM_INSUFFICIENT_DATA = 103,
    TF_TRANSFORM_RESOURCE_LIMIT = 104,
    TF_TRANSFORM_UNSUPPORTED_VERSION = 105,
    TF_TRANSFORM_CORRUPT_PLAN = 106,
    TF_TRANSFORM_NUMERIC_DOMAIN = 107,
    TF_TRANSFORM_UNKNOWN_CATEGORY = 108,
    TF_TRANSFORM_CANCELLED = 109,
    TF_TRANSFORM_ALLOCATION = 110,
    TF_TRANSFORM_IO = 111,
    TF_TRANSFORM_INVALID_STATE = 112,
    TF_TRANSFORM_UNSUPPORTED_RUNTIME = 113,
    TF_TRANSFORM_INTERNAL = 199
} tf_transform_code;

enum {
    TF_VIEW_FLOAT32 = 0x0101,
    TF_VIEW_FLOAT64 = 0x0102
};

#define TF_TRANSFORM_SCHEMA_INPUT  1u
#define TF_TRANSFORM_SCHEMA_OUTPUT 2u

#define TF_TRANSFORM_CANCEL_ROWS_V1       4096u
#define TF_TRANSFORM_CANCEL_ELEMENTS_V1  65536u
#define TF_TRANSFORM_CANCEL_BYTES_V1     65536u
#define TF_TRANSFORM_CANCEL_ITERS_V1      4096u
#define TF_TRANSFORM_CANCEL_IO_BYTES_V1 1048576u

typedef struct tf_transform_recipe tf_transform_recipe;
typedef struct tf_transform_analyzer tf_transform_analyzer;
typedef struct tf_transform_plan tf_transform_plan;
typedef struct tf_transform_apply tf_transform_apply;
typedef struct tf_transform_schema tf_transform_schema;
typedef struct tf_transform_error tf_transform_error;

typedef int (*tf_transform_cancel_fn)(void *user);

typedef struct tf_transform_limits_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    uint64_t max_recipe_bytes;
    uint64_t max_plan_bytes;
    uint64_t max_json_depth;
    uint64_t max_object_keys;
    uint64_t max_steps;
    uint64_t max_input_columns;
    uint64_t max_output_columns;
    uint64_t max_categories_per_column;
    uint64_t max_total_categories;
    uint64_t max_string_bytes;
    uint64_t max_decoded_string_bytes;
    uint64_t max_analyzer_rows;
    uint64_t max_analyzer_input_bytes;
    uint64_t max_resident_state_bytes;
    uint64_t max_spill_bytes;
    uint64_t max_apply_rows;
    uint64_t max_apply_input_bytes;
    uint64_t max_output_elements_per_call;
    uint64_t max_allocation_bytes;
    uint64_t max_allocations_per_session;
    uint64_t max_live_handles;
    uint64_t max_retired_handle_slots;
} tf_transform_limits_v1;

typedef struct tf_transform_runtime_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    const tf_transform_limits_v1 *limits;
    const tf_host_policy *host_policy;
    const uint8_t *spill_dir_utf8;
    size_t spill_dir_bytes;
    tf_transform_cancel_fn cancel;
    void *cancel_user;
    uint32_t flags;
    uint32_t reserved;
} tf_transform_runtime_v1;

typedef struct tf_field_view_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t dtype;
    uint32_t flags;
    const uint8_t *id_utf8;
    size_t id_bytes;
    const uint8_t *name_utf8;
    size_t name_bytes;
} tf_field_view_v1;

typedef struct tf_schema_view_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    size_t column_count;
    const tf_field_view_v1 *fields;
    size_t fields_bytes;
} tf_schema_view_v1;

typedef struct tf_column_view_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    const void *data;
    size_t data_bytes;
    size_t stride_bytes;
    const uint8_t *validity;
    size_t validity_bytes;
    size_t validity_bit_offset;
    size_t validity_bit_stride;
} tf_column_view_v1;

typedef struct tf_table_view_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    size_t row_count;
    size_t column_count;
    const tf_column_view_v1 *columns;
    size_t columns_bytes;
} tf_table_view_v1;

typedef struct tf_owned_dense_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t dtype;
    uint32_t flags;
    size_t rows;
    size_t columns;
    void *data;
    size_t data_bytes;
} tf_owned_dense_v1;

tf_transform_code tf_transform_limits_init_safe_v1(
    tf_transform_limits_v1 *out, size_t out_size);

tf_transform_code tf_transform_recipe_from_json(
    const uint8_t *json, size_t json_len,
    const tf_transform_limits_v1 *limits,
    tf_transform_recipe **out, tf_transform_error **error);
tf_transform_code tf_transform_analyzer_create(
    const tf_transform_recipe *recipe, const tf_schema_view_v1 *input_schema,
    const tf_transform_runtime_v1 *runtime,
    tf_transform_analyzer **out, tf_transform_error **error);
tf_transform_code tf_transform_analyzer_push(
    tf_transform_analyzer *analyzer, const tf_table_view_v1 *table,
    tf_transform_error **error);
tf_transform_code tf_transform_analyzer_finalize(
    tf_transform_analyzer *analyzer, tf_transform_plan **out,
    tf_transform_error **error);
tf_transform_code tf_transform_plan_export(
    const tf_transform_plan *plan, const tf_transform_limits_v1 *limits,
    uint8_t **out, size_t *out_len, tf_transform_error **error);
tf_transform_code tf_transform_plan_import(
    const uint8_t *bytes, size_t len, const tf_transform_runtime_v1 *runtime,
    tf_transform_plan **out, tf_transform_error **error);
tf_transform_code tf_transform_apply_create(
    const tf_transform_plan *plan, const tf_schema_view_v1 *runtime_schema,
    const tf_transform_runtime_v1 *runtime,
    tf_transform_apply **out, tf_transform_error **error);
tf_transform_code tf_transform_apply_run(
    tf_transform_apply *apply, const tf_table_view_v1 *table,
    tf_owned_dense_v1 *out, tf_transform_error **error);

void tf_transform_recipe_destroy(tf_transform_recipe **recipe);
void tf_transform_analyzer_destroy(tf_transform_analyzer **analyzer);
void tf_transform_plan_destroy(tf_transform_plan **plan);
void tf_transform_apply_destroy(tf_transform_apply **apply);
void tf_transform_error_destroy(tf_transform_error **error);
void tf_transform_bytes_free(uint8_t **bytes, size_t *len);
void tf_owned_dense_free(tf_owned_dense_v1 *dense);

tf_transform_code tf_transform_error_get_code(const tf_transform_error *error);
const uint8_t *tf_transform_error_message(
    const tf_transform_error *error, size_t *len);

tf_transform_code tf_transform_plan_schema(
    const tf_transform_plan *plan, uint32_t which,
    const tf_transform_schema **out, tf_transform_error **error);
size_t tf_transform_schema_field_count(const tf_transform_schema *schema);
tf_transform_code tf_transform_schema_field(
    const tf_transform_schema *schema, size_t index,
    tf_field_view_v1 *out, tf_transform_error **error);
tf_transform_code tf_transform_plan_schema_json(
    const tf_transform_plan *plan, uint32_t which,
    const tf_transform_limits_v1 *limits,
    uint8_t **out, size_t *out_len, tf_transform_error **error);

#ifdef __cplusplus
}
#endif

#endif
