#ifndef TRANFI_TRANSFORM_INTERNAL_H
#define TRANFI_TRANSFORM_INTERNAL_H

#include "transform.h"
#include "cJSON.h"
#include <fenv.h>
#include <stdatomic.h>

typedef enum tf_transform_impute_op {
    TF_TRANSFORM_IMPUTE_NONE = 0,
    TF_TRANSFORM_IMPUTE_ZERO,
    TF_TRANSFORM_IMPUTE_CONSTANT,
    TF_TRANSFORM_IMPUTE_MEAN
} tf_transform_impute_op;

typedef enum tf_transform_normalize_op {
    TF_TRANSFORM_NORMALIZE_NONE = 0,
    TF_TRANSFORM_NORMALIZE_STANDARD,
    TF_TRANSFORM_NORMALIZE_MINMAX
} tf_transform_normalize_op;

typedef enum tf_transform_all_missing {
    TF_TRANSFORM_ALL_MISSING_NONE = 0,
    TF_TRANSFORM_ALL_MISSING_ERROR,
    TF_TRANSFORM_ALL_MISSING_ZERO
} tf_transform_all_missing;

typedef struct tf_transform_recipe_column {
    char *source_id;
    size_t source_id_len;
    tf_transform_impute_op impute;
    tf_transform_all_missing all_missing;
    double constant;
    uint32_t constant_dtype;
    tf_transform_normalize_op normalize;
    int ddof;
} tf_transform_recipe_column;

struct tf_transform_recipe {
    atomic_uint refcount;
    size_t column_count;
    tf_transform_recipe_column *columns;
    uint64_t max_output_columns;
    uint64_t max_output_elements_per_apply;
};

typedef struct tf_transform_schema_field_owned {
    uint32_t dtype;
    char *id;
    size_t id_len;
    char *name;
    size_t name_len;
} tf_transform_schema_field_owned;

struct tf_transform_schema {
    size_t field_count;
    tf_transform_schema_field_owned *fields;
    int is_output;
};

typedef struct tf_transform_numeric_state {
    tf_transform_impute_op impute;
    tf_transform_all_missing all_missing;
    tf_transform_normalize_op normalize;
    int ddof;
    double impute_value;
    int has_impute_value;
    double location;
    double scale;
} tf_transform_numeric_state;

struct tf_transform_plan {
    atomic_uint refcount;
    tf_transform_recipe *recipe;
    tf_transform_schema input_schema;
    tf_transform_schema output_schema;
    tf_transform_numeric_state *states;
    uint64_t import_allocation_count;
    uint64_t import_peak_resident_bytes;
};

typedef struct tf_transform_running_stats {
    uint64_t observed;
    uint64_t missing;
    double mean;
    double m2;
    double minimum;
    double maximum;
    int has_value;
} tf_transform_running_stats;

typedef struct tf_transform_runtime_copy {
    tf_transform_limits_v1 limits;
    tf_transform_cancel_fn cancel;
    void *cancel_user;
} tf_transform_runtime_copy;

typedef struct tf_transform_resource_ledger {
    const tf_transform_runtime_copy *runtime;
    uint64_t allocation_count;
    uint64_t resident_bytes;
    uint64_t peak_resident_bytes;
    tf_transform_code last_code;
} tf_transform_resource_ledger;

typedef enum tf_transform_analyzer_state {
    TF_ANALYZER_ACTIVE = 0,
    TF_ANALYZER_FINALIZED,
    TF_ANALYZER_FAILED
} tf_transform_analyzer_state;

struct tf_transform_analyzer {
    tf_transform_recipe *recipe;
    tf_transform_schema input_schema;
    tf_transform_runtime_copy runtime;
    tf_transform_running_stats *stats;
    tf_transform_running_stats *scratch;
    uint64_t total_rows;
    uint64_t total_input_bytes;
    uint64_t allocation_count;
    uint64_t resident_state_bytes;
    tf_transform_analyzer_state state;
};

typedef enum tf_transform_apply_state {
    TF_APPLY_READY = 0,
    TF_APPLY_FAILED
} tf_transform_apply_state;

struct tf_transform_apply {
    tf_transform_plan *plan;
    tf_transform_runtime_copy runtime;
    uint64_t total_rows;
    uint64_t total_input_bytes;
    uint64_t allocation_count;
    uint64_t resident_state_bytes;
    tf_transform_apply_state state;
};

struct tf_transform_error {
    tf_transform_code code;
    char *message;
    size_t message_len;
};

typedef struct tf_transform_buffer {
    uint8_t *data;
    size_t len;
    size_t cap;
    int count_only;
} tf_transform_buffer;

typedef struct tf_transform_fp_guard {
#if !defined(__wasm__)
    fenv_t environment;
#endif
#if defined(__i386__) || defined(__x86_64__)
    unsigned int mxcsr;
#endif
    int active;
} tf_transform_fp_guard;

tf_transform_code tf_transform_set_error(
    tf_transform_error **error, tf_transform_code code, const char *message);
void tf_transform_clear_error(tf_transform_error **error);

tf_transform_code tf_transform_copy_limits(
    const tf_transform_limits_v1 *source, tf_transform_limits_v1 *out,
    tf_transform_error **error);
tf_transform_code tf_transform_copy_runtime(
    const tf_transform_runtime_v1 *source, tf_transform_runtime_copy *out,
    tf_transform_error **error);
tf_transform_code tf_transform_poll_cancel(
    const tf_transform_runtime_copy *runtime, tf_transform_error **error);
tf_transform_code tf_transform_copy_bytes_runtime(
    void *destination, const void *source, size_t len,
    const tf_transform_runtime_copy *runtime,
    tf_transform_error **error);
tf_transform_code tf_transform_compare_bytes_runtime(
    const void *left, size_t left_len,
    const void *right, size_t right_len,
    const tf_transform_runtime_copy *runtime,
    int *comparison, tf_transform_error **error);
void tf_transform_resource_ledger_init(
    tf_transform_resource_ledger *ledger,
    const tf_transform_runtime_copy *runtime);
tf_transform_code tf_transform_resource_reserve(
    tf_transform_resource_ledger *ledger, uint64_t bytes,
    tf_transform_error **error);
tf_transform_code tf_transform_resource_charge_batch(
    tf_transform_resource_ledger *ledger, uint64_t allocations,
    uint64_t resident_bytes, uint64_t peak_bytes,
    tf_transform_error **error);
void tf_transform_resource_release(
    tf_transform_resource_ledger *ledger, uint64_t bytes);
void *tf_transform_resource_malloc(
    tf_transform_resource_ledger *ledger, size_t bytes,
    tf_transform_error **error);
void *tf_transform_resource_calloc(
    tf_transform_resource_ledger *ledger, size_t count, size_t size,
    tf_transform_error **error);
tf_transform_code tf_transform_check_runtime_fp(tf_transform_error **error);
tf_transform_code tf_transform_fp_begin(
    tf_transform_fp_guard *guard, tf_transform_error **error);
void tf_transform_fp_end(tf_transform_fp_guard *guard);

void tf_transform_recipe_retain(tf_transform_recipe *recipe);
void tf_transform_recipe_release(tf_transform_recipe *recipe);
void tf_transform_plan_retain(tf_transform_plan *plan);
void tf_transform_plan_release(tf_transform_plan *plan);

tf_transform_code tf_transform_schema_copy(
    const tf_schema_view_v1 *source, const tf_transform_limits_v1 *limits,
    tf_transform_schema *out, tf_transform_error **error);
tf_transform_code tf_transform_schema_copy_runtime(
    const tf_schema_view_v1 *source,
    const tf_transform_runtime_copy *runtime,
    tf_transform_schema *out, tf_transform_error **error);
tf_transform_code tf_transform_schema_copy_runtime_ledger(
    const tf_schema_view_v1 *source,
    const tf_transform_runtime_copy *runtime,
    tf_transform_resource_ledger *ledger,
    tf_transform_schema *out, tf_transform_error **error);
tf_transform_code tf_transform_schema_clone(
    const tf_transform_schema *source, tf_transform_schema *out,
    tf_transform_error **error);
tf_transform_code tf_transform_schema_clone_runtime(
    const tf_transform_schema *source,
    const tf_transform_runtime_copy *runtime,
    tf_transform_schema *out, tf_transform_error **error);
void tf_transform_schema_clear(tf_transform_schema *schema);
int tf_transform_schema_equal_view(
    const tf_transform_schema *schema, const tf_schema_view_v1 *view);
tf_transform_code tf_transform_schema_equal_view_runtime(
    const tf_transform_schema *schema, const tf_schema_view_v1 *view,
    const tf_transform_runtime_copy *runtime, int *equal,
    tf_transform_error **error);

tf_transform_code tf_transform_recipe_parse_numeric(
    const uint8_t *json, size_t json_len,
    const tf_transform_limits_v1 *limits,
    tf_transform_recipe **out, tf_transform_error **error);
cJSON *tf_transform_recipe_to_json(const tf_transform_recipe *recipe);
cJSON *tf_transform_schema_to_json(const tf_transform_schema *schema);
tf_transform_code tf_transform_schema_json_preflight(
    const tf_transform_schema *schema, const tf_transform_limits_v1 *limits,
    tf_transform_error **error);
tf_transform_code tf_transform_plan_json_preflight(
    const tf_transform_plan *plan, const tf_transform_limits_v1 *limits,
    tf_transform_error **error);
cJSON *tf_transform_plan_to_json(
    const tf_transform_plan *plan, const tf_transform_limits_v1 *limits,
    tf_transform_error **error);
tf_transform_code tf_transform_plan_from_json(
    const uint8_t *json, size_t json_len,
    const tf_transform_runtime_copy *runtime,
    tf_transform_plan **out, uint8_t **canonical_out,
    size_t *canonical_len_out, tf_transform_error **error);

tf_transform_code tf_transform_json_print_canonical(
    const cJSON *value, const tf_transform_limits_v1 *limits,
    uint8_t **out, size_t *out_len, tf_transform_error **error);
tf_transform_code tf_transform_json_print_canonical_runtime(
    const cJSON *value, const tf_transform_runtime_copy *runtime,
    uint8_t **out, size_t *out_len, tf_transform_error **error);
tf_transform_code tf_transform_json_print_canonical_runtime_ledger(
    const cJSON *value, const tf_transform_runtime_copy *runtime,
    tf_transform_resource_ledger *ledger,
    uint8_t **out, size_t *out_len, tf_transform_error **error);
tf_transform_code tf_transform_json_parse_bounded(
    const uint8_t *json, size_t json_len,
    const tf_transform_limits_v1 *limits, cJSON **out,
    tf_transform_error **error, tf_transform_code malformed_code);
tf_transform_code tf_transform_json_parse_bounded_runtime(
    const uint8_t *json, size_t json_len,
    const tf_transform_runtime_copy *runtime, cJSON **out,
    uint64_t *resident_bytes_out, uint64_t *allocation_count_out,
    tf_transform_error **error, tf_transform_code malformed_code);
tf_transform_code tf_transform_json_parse_bounded_runtime_ledger(
    const uint8_t *json, size_t json_len,
    const tf_transform_runtime_copy *runtime,
    tf_transform_resource_ledger *ledger, cJSON **out,
    uint64_t *resident_bytes_out, uint64_t *allocation_count_out,
    tf_transform_error **error, tf_transform_code malformed_code);

void tf_transform_sha256(const uint8_t *data, size_t len, uint8_t out[32]);
tf_transform_code tf_transform_sha256_runtime(
    const uint8_t *data, size_t len,
    const tf_transform_runtime_copy *runtime,
    uint8_t out[32], tf_transform_error **error);
double tf_transform_sqrt_f64_rne(double value);

int tf_transform_double_is_finite(double value);
int tf_transform_double_is_nan(double value);
uint64_t tf_transform_double_bits(double value);
double tf_transform_double_from_bits(uint64_t bits);
int tf_transform_valid_utf8(const uint8_t *data, size_t len);

#endif
