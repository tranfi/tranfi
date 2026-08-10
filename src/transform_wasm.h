/* Standalone wasm32 prepared-transform ABI. */

#ifndef TRANFI_TRANSFORM_WASM_H
#define TRANFI_TRANSFORM_WASM_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TF_WASM_TRANSFORM_NO_HOST_CANCEL UINT32_MAX

enum {
    TF_WASM_HANDLE_RECIPE = 1,
    TF_WASM_HANDLE_ANALYZER = 2,
    TF_WASM_HANDLE_PLAN = 3,
    TF_WASM_HANDLE_APPLY = 4,
    TF_WASM_HANDLE_ERROR = 5,
    TF_WASM_HANDLE_OWNED_BYTES = 6,
    TF_WASM_HANDLE_OWNED_DENSE = 7,
    TF_WASM_HANDLE_CANCEL_TOKEN = 8
};

typedef struct tf_wasm_field_view_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t dtype;
    uint32_t flags;
    uint32_t id_offset;
    uint32_t id_bytes;
    uint32_t name_offset;
    uint32_t name_bytes;
} tf_wasm_field_view_v1;

typedef struct tf_wasm_schema_view_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t column_count;
    uint32_t fields_offset;
    uint32_t fields_bytes;
} tf_wasm_schema_view_v1;

typedef struct tf_wasm_column_view_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t data_offset;
    uint32_t data_bytes;
    uint32_t stride_bytes;
    uint32_t validity_offset;
    uint32_t validity_bytes;
    uint32_t validity_bit_offset;
    uint32_t validity_bit_stride;
} tf_wasm_column_view_v1;

typedef struct tf_wasm_table_view_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t row_count;
    uint32_t column_count;
    uint32_t columns_offset;
    uint32_t columns_bytes;
} tf_wasm_table_view_v1;

typedef struct tf_wasm_dense_info_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t dtype;
    uint32_t flags;
    uint32_t rows;
    uint32_t columns;
    uint32_t data_bytes;
} tf_wasm_dense_info_v1;

typedef struct tf_wasm_schema_field_info_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    uint32_t dtype;
    uint32_t id_bytes;
    uint32_t name_bytes;
} tf_wasm_schema_field_info_v1;

uint32_t tf_wasm_transform_limits_init_safe_v1(
    uint32_t out_offset, uint32_t out_size);
uint32_t tf_wasm_transform_cancel_create(
    uint32_t host_poll_slot, uint32_t limits_offset,
    uint32_t out_handle_offset, uint32_t out_error_offset);
uint32_t tf_wasm_transform_cancel_request(uint32_t token_handle);

uint32_t tf_wasm_transform_recipe_from_json(
    uint32_t json_offset, uint32_t json_bytes, uint32_t limits_offset,
    uint32_t out_handle_offset, uint32_t out_error_offset);
uint32_t tf_wasm_transform_analyzer_create(
    uint32_t recipe_handle, uint32_t schema_offset,
    uint32_t limits_offset, uint32_t cancel_token_handle,
    uint32_t out_handle_offset, uint32_t out_error_offset);
uint32_t tf_wasm_transform_analyzer_push(
    uint32_t analyzer_handle, uint32_t table_offset,
    uint32_t out_error_offset);
uint32_t tf_wasm_transform_analyzer_finalize(
    uint32_t analyzer_handle, uint32_t out_handle_offset,
    uint32_t out_error_offset);
uint32_t tf_wasm_transform_plan_export(
    uint32_t plan_handle, uint32_t limits_offset,
    uint32_t out_handle_offset, uint32_t out_error_offset);
uint32_t tf_wasm_transform_plan_import(
    uint32_t bytes_offset, uint32_t bytes_len, uint32_t limits_offset,
    uint32_t cancel_token_handle, uint32_t out_handle_offset,
    uint32_t out_error_offset);
uint32_t tf_wasm_transform_plan_schema_json(
    uint32_t plan_handle, uint32_t which, uint32_t limits_offset,
    uint32_t out_handle_offset, uint32_t out_error_offset);
uint32_t tf_wasm_transform_plan_recipe_sha256(
    uint32_t plan_handle, uint32_t limits_offset,
    uint32_t out_handle_offset, uint32_t out_error_offset);
uint32_t tf_wasm_transform_plan_schema_field_count(
    uint32_t plan_handle, uint32_t which, uint32_t out_count_offset,
    uint32_t out_error_offset);
uint32_t tf_wasm_transform_plan_schema_field_info(
    uint32_t plan_handle, uint32_t which, uint32_t index,
    uint32_t out_info_offset, uint32_t out_error_offset);
uint32_t tf_wasm_transform_plan_schema_field_copy(
    uint32_t plan_handle, uint32_t which, uint32_t index,
    uint32_t id_offset, uint32_t id_capacity,
    uint32_t name_offset, uint32_t name_capacity,
    uint32_t out_error_offset);
uint32_t tf_wasm_transform_apply_create(
    uint32_t plan_handle, uint32_t schema_offset,
    uint32_t limits_offset, uint32_t cancel_token_handle,
    uint32_t out_handle_offset, uint32_t out_error_offset);
uint32_t tf_wasm_transform_apply_run(
    uint32_t apply_handle, uint32_t table_offset,
    uint32_t out_handle_offset, uint32_t out_error_offset);

uint32_t tf_wasm_transform_error_code(
    uint32_t error_handle, uint32_t out_code_offset);
uint32_t tf_wasm_transform_error_message_size(
    uint32_t error_handle, uint32_t out_size_offset);
uint32_t tf_wasm_transform_error_message_copy(
    uint32_t error_handle, uint32_t destination_offset,
    uint32_t destination_capacity);
uint32_t tf_wasm_transform_owned_bytes_size(
    uint32_t bytes_handle, uint32_t out_size_offset);
uint32_t tf_wasm_transform_owned_bytes_copy(
    uint32_t bytes_handle, uint32_t destination_offset,
    uint32_t destination_capacity);
uint32_t tf_wasm_transform_owned_dense_info(
    uint32_t dense_handle, uint32_t out_info_offset);
uint32_t tf_wasm_transform_owned_dense_copy(
    uint32_t dense_handle, uint32_t destination_offset,
    uint32_t destination_capacity);
uint32_t tf_wasm_transform_handle_destroy(uint32_t handle);

#ifndef __EMSCRIPTEN__
void tf_wasm_transform_test_set_heap(void *base, uint32_t size);
#endif

#ifdef __cplusplus
}
#endif

#endif
