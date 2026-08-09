#include "napi_transform.h"

#include "transform.h"

#include <math.h>
#include <stddef.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

typedef enum napi_transform_handle_type {
    NAPI_TRANSFORM_RECIPE = 1,
    NAPI_TRANSFORM_ANALYZER = 2,
    NAPI_TRANSFORM_PLAN = 3,
    NAPI_TRANSFORM_APPLY = 4
} napi_transform_handle_type;

typedef struct napi_transform_handle {
    napi_transform_handle_type type;
    void *pointer;
    size_t column_count;
    size_t output_column_count;
    uint32_t *dtypes;
    tf_transform_limits_v1 limits;
    struct napi_transform_cancel *cancel;
} napi_transform_handle;

typedef struct napi_transform_cancel {
    const _Atomic int32_t *flag;
    _Atomic uint32_t *poll_count;
    napi_ref reference;
} napi_transform_cancel;

typedef struct napi_transform_bytes {
    const uint8_t *data;
    size_t len;
    uint8_t *owned;
} napi_transform_bytes;

typedef struct napi_transform_schema_owner {
    tf_schema_view_v1 view;
    tf_field_view_v1 *fields;
    char **ids;
    char **names;
    uint32_t *dtypes;
    size_t count;
} napi_transform_schema_owner;

typedef struct napi_transform_table_owner {
    tf_table_view_v1 view;
    tf_column_view_v1 *columns;
} napi_transform_table_owner;

typedef struct napi_transform_dense_result_owner {
    napi_value result;
    void *data;
    size_t data_bytes;
    size_t rows;
    size_t columns;
} napi_transform_dense_result_owner;

typedef struct napi_transform_limit_field {
    const char *name;
    size_t offset;
} napi_transform_limit_field;

static const napi_type_tag NAPI_TRANSFORM_HANDLE_TAG = {
    UINT64_C(0x7ac65b1a42a84bc0),
    UINT64_C(0xa116f19a14f96c52)
};

#define LIMIT_FIELD(js_name, member) \
    {js_name, offsetof(tf_transform_limits_v1, member)}

static const napi_transform_limit_field LIMIT_FIELDS[] = {
    LIMIT_FIELD("maxRecipeBytes", max_recipe_bytes),
    LIMIT_FIELD("maxPlanBytes", max_plan_bytes),
    LIMIT_FIELD("maxJsonDepth", max_json_depth),
    LIMIT_FIELD("maxObjectKeys", max_object_keys),
    LIMIT_FIELD("maxSteps", max_steps),
    LIMIT_FIELD("maxInputColumns", max_input_columns),
    LIMIT_FIELD("maxOutputColumns", max_output_columns),
    LIMIT_FIELD("maxCategoriesPerColumn", max_categories_per_column),
    LIMIT_FIELD("maxTotalCategories", max_total_categories),
    LIMIT_FIELD("maxStringBytes", max_string_bytes),
    LIMIT_FIELD("maxDecodedStringBytes", max_decoded_string_bytes),
    LIMIT_FIELD("maxAnalyzerRows", max_analyzer_rows),
    LIMIT_FIELD("maxAnalyzerInputBytes", max_analyzer_input_bytes),
    LIMIT_FIELD("maxResidentStateBytes", max_resident_state_bytes),
    LIMIT_FIELD("maxSpillBytes", max_spill_bytes),
    LIMIT_FIELD("maxApplyRows", max_apply_rows),
    LIMIT_FIELD("maxApplyInputBytes", max_apply_input_bytes),
    LIMIT_FIELD("maxOutputElementsPerCall", max_output_elements_per_call),
    LIMIT_FIELD("maxAllocationBytes", max_allocation_bytes),
    LIMIT_FIELD("maxAllocationsPerSession", max_allocations_per_session),
    LIMIT_FIELD("maxLiveHandles", max_live_handles),
    LIMIT_FIELD("maxRetiredHandleSlots", max_retired_handle_slots),
};

static int napi_transform_check(napi_env env, napi_status status) {
    if (status == napi_ok) return 1;
    napi_throw_error(env, NULL, "N-API prepared-transform call failed");
    return 0;
}

static napi_value napi_transform_undefined(napi_env env) {
    napi_value value = NULL;
    (void)napi_get_undefined(env, &value);
    return value;
}

static napi_value napi_transform_throw_code(
    napi_env env, tf_transform_code code, const char *message) {
    napi_value message_value = NULL;
    napi_value error_value = NULL;
    napi_value name_value = NULL;
    napi_value code_value = NULL;
    const char *text = message ? message : "prepared transform failed";
    if (napi_create_string_utf8(env, text, NAPI_AUTO_LENGTH, &message_value)
            != napi_ok
        || napi_create_error(env, NULL, message_value, &error_value) != napi_ok
        || napi_create_string_utf8(
            env, "TranfiTransformError", NAPI_AUTO_LENGTH, &name_value) != napi_ok
        || napi_create_uint32(env, (uint32_t)code, &code_value) != napi_ok
        || napi_set_named_property(env, error_value, "name", name_value) != napi_ok
        || napi_set_named_property(env, error_value, "code", code_value) != napi_ok
        || napi_throw(env, error_value) != napi_ok) {
        napi_throw_error(env, NULL, "failed to construct TranfiTransformError");
    }
    return NULL;
}

static napi_value napi_transform_throw_error(
    napi_env env, tf_transform_code code, tf_transform_error **error) {
    char *message = NULL;
    const uint8_t *span = NULL;
    size_t len = 0;
    napi_value result;
    if (error && *error) {
        span = tf_transform_error_message(*error, &len);
        if (span && len <= SIZE_MAX - 1) {
            message = (char *)malloc(len + 1);
            if (message) {
                memcpy(message, span, len);
                message[len] = '\0';
            }
        }
    }
    if (!message) {
        static const char fallback[] = "prepared transform failed";
        message = (char *)malloc(sizeof(fallback));
        if (message) memcpy(message, fallback, sizeof(fallback));
    }
    tf_transform_error_destroy(error);
    result = napi_transform_throw_code(env, code, message);
    free(message);
    return result;
}

static int napi_transform_cancel_poll(void *user) {
    const napi_transform_cancel *cancel = (const napi_transform_cancel *)user;
    if (!cancel || !cancel->flag) return 0;
    if (cancel->poll_count) {
        (void)atomic_fetch_add_explicit(
            cancel->poll_count, UINT32_C(1), memory_order_release);
    }
    return atomic_load_explicit(cancel->flag, memory_order_acquire) != 0;
}

static void napi_transform_cancel_destroy(
    napi_env env, napi_transform_cancel **cancel) {
    if (!cancel || !*cancel) return;
    if ((*cancel)->reference) {
        (void)napi_delete_reference(env, (*cancel)->reference);
    }
    free(*cancel);
    *cancel = NULL;
}

static void napi_transform_destroy_payload(
    napi_env env, napi_transform_handle *handle) {
    if (!handle) return;
    if (handle->pointer) {
        if (handle->type == NAPI_TRANSFORM_RECIPE) {
            tf_transform_recipe *value = (tf_transform_recipe *)handle->pointer;
            tf_transform_recipe_destroy(&value);
        } else if (handle->type == NAPI_TRANSFORM_ANALYZER) {
            tf_transform_analyzer *value = (tf_transform_analyzer *)handle->pointer;
            tf_transform_analyzer_destroy(&value);
        } else if (handle->type == NAPI_TRANSFORM_PLAN) {
            tf_transform_plan *value = (tf_transform_plan *)handle->pointer;
            tf_transform_plan_destroy(&value);
        } else if (handle->type == NAPI_TRANSFORM_APPLY) {
            tf_transform_apply *value = (tf_transform_apply *)handle->pointer;
            tf_transform_apply_destroy(&value);
        }
    }
    handle->pointer = NULL;
    free(handle->dtypes);
    handle->dtypes = NULL;
    handle->column_count = 0;
    handle->output_column_count = 0;
    memset(&handle->limits, 0, sizeof(handle->limits));
    napi_transform_cancel_destroy(env, &handle->cancel);
}

static void napi_transform_finalize(
    napi_env env, void *data, void *hint) {
    napi_transform_handle *handle = (napi_transform_handle *)data;
    (void)env;
    (void)hint;
    napi_transform_destroy_payload(env, handle);
    free(handle);
}

static napi_value napi_transform_new_handle_with_cancel(
    napi_env env, napi_transform_handle_type type, void *pointer,
    const uint32_t *dtypes, size_t column_count,
    size_t output_column_count, const tf_transform_limits_v1 *limits,
    napi_transform_cancel *cancel) {
    napi_transform_handle *handle = NULL;
    napi_value object = NULL;
    handle = (napi_transform_handle *)calloc(1, sizeof(*handle));
    if (!handle) {
        napi_transform_handle temporary = {
            .type = type,
            .pointer = pointer,
            .cancel = cancel
        };
        napi_transform_destroy_payload(env, &temporary);
        napi_throw_error(env, NULL, "prepared-transform handle allocation failed");
        return NULL;
    }
    handle->type = type;
    handle->pointer = pointer;
    handle->cancel = cancel;
    handle->output_column_count = output_column_count;
    if (limits) handle->limits = *limits;
    if (column_count != 0) {
        if (!dtypes || column_count > SIZE_MAX / sizeof(*handle->dtypes)
            || (limits && (uint64_t)column_count
                > limits->max_allocation_bytes / sizeof(*handle->dtypes))) {
            napi_transform_destroy_payload(env, handle);
            free(handle);
            napi_transform_throw_code(
                env, TF_TRANSFORM_RESOURCE_LIMIT,
                "prepared-transform dtype metadata exceeds allocation limit");
            return NULL;
        }
        handle->dtypes = (uint32_t *)malloc(
            column_count * sizeof(*handle->dtypes));
        if (!handle->dtypes) {
            napi_transform_destroy_payload(env, handle);
            free(handle);
            napi_throw_error(env, NULL, "prepared-transform dtype allocation failed");
            return NULL;
        }
        memcpy(handle->dtypes, dtypes, column_count * sizeof(*handle->dtypes));
        handle->column_count = column_count;
    }
    if (napi_create_object(env, &object) != napi_ok
        || napi_type_tag_object(env, object, &NAPI_TRANSFORM_HANDLE_TAG) != napi_ok
        || napi_wrap(
            env, object, handle, napi_transform_finalize, NULL, NULL) != napi_ok) {
        napi_transform_destroy_payload(env, handle);
        free(handle);
        napi_throw_error(env, NULL, "prepared-transform handle object allocation failed");
        return NULL;
    }
    return object;
}

static napi_value napi_transform_new_handle(
    napi_env env, napi_transform_handle_type type, void *pointer,
    const uint32_t *dtypes, size_t column_count) {
    return napi_transform_new_handle_with_cancel(
        env, type, pointer, dtypes, column_count, 0, NULL, NULL);
}

static napi_transform_handle *napi_transform_unwrap_handle(
    napi_env env, napi_value value) {
    napi_transform_handle *handle = NULL;
    bool matches = false;
    if (napi_check_object_type_tag(
            env, value, &NAPI_TRANSFORM_HANDLE_TAG, &matches) != napi_ok
        || !matches
        || napi_unwrap(env, value, (void **)&handle) != napi_ok
        || !handle) {
        napi_throw_type_error(env, NULL, "expected a Tranfi prepared-transform handle");
        return NULL;
    }
    return handle;
}

static napi_transform_handle *napi_transform_get_handle(
    napi_env env, napi_value value, napi_transform_handle_type expected) {
    napi_transform_handle *handle = napi_transform_unwrap_handle(env, value);
    if (!handle) return NULL;
    if (handle->type != expected) {
        napi_throw_type_error(env, NULL, "prepared-transform handle type mismatch");
        return NULL;
    }
    if (!handle->pointer) {
        napi_transform_throw_code(
            env, TF_TRANSFORM_INVALID_STATE,
            "prepared-transform handle is disposed");
        return NULL;
    }
    return handle;
}

static int napi_transform_get_optional_uint64(
    napi_env env, napi_value object, const char *name,
    uint64_t *out, int *present) {
    bool has = false;
    napi_value value;
    napi_valuetype type;
    double number;
    if (!napi_transform_check(
            env, napi_has_named_property(env, object, name, &has))) return 0;
    if (!has) {
        *present = 0;
        return 1;
    }
    if (!napi_transform_check(
            env, napi_get_named_property(env, object, name, &value))
        || !napi_transform_check(env, napi_typeof(env, value, &type))) return 0;
    if (type == napi_undefined || type == napi_null) {
        *present = 0;
        return 1;
    }
    if (type != napi_number
        || !napi_transform_check(env, napi_get_value_double(env, value, &number))) {
        napi_throw_type_error(env, NULL, "transform limits must be numbers");
        return 0;
    }
    if (!isfinite(number) || number < 0.0 || floor(number) != number
        || number > 9007199254740991.0) {
        napi_throw_range_error(
            env, NULL, "transform limits must be nonnegative safe integers");
        return 0;
    }
    *out = (uint64_t)number;
    *present = 1;
    return 1;
}

static int napi_transform_parse_limits(
    napi_env env, napi_value value, tf_transform_limits_v1 *limits,
    int *provided) {
    napi_valuetype type;
    if (tf_transform_limits_init_safe_v1(limits, sizeof(*limits))
        != TF_TRANSFORM_OK) {
        napi_throw_error(env, NULL, "failed to initialize transform limits");
        return 0;
    }
    *provided = 0;
    if (!value) return 1;
    if (!napi_transform_check(env, napi_typeof(env, value, &type))) return 0;
    if (type == napi_undefined || type == napi_null) return 1;
    if (type != napi_object) {
        napi_throw_type_error(env, NULL, "transform limits must be an object");
        return 0;
    }
    *provided = 1;
    for (size_t i = 0; i < sizeof(LIMIT_FIELDS) / sizeof(LIMIT_FIELDS[0]); ++i) {
        uint64_t value_u64 = 0;
        int present = 0;
        uint64_t *field;
        if (!napi_transform_get_optional_uint64(
                env, value, LIMIT_FIELDS[i].name, &value_u64, &present)) return 0;
        if (!present) continue;
        if (value_u64 == 0) {
            napi_transform_throw_code(
                env, TF_TRANSFORM_INVALID_ARGUMENT,
                "transform limits must be positive safe integers");
            return 0;
        }
        field = (uint64_t *)((uint8_t *)limits + LIMIT_FIELDS[i].offset);
        *field = value_u64;
    }
    return 1;
}

static int napi_transform_parse_cancel_flag(
    napi_env env, napi_value value, napi_transform_cancel **out) {
    bool is_typedarray = false;
    napi_typedarray_type typed_type;
    size_t count = 0;
    void *data = NULL;
    napi_value arraybuffer;
    size_t byte_offset = 0;
    napi_valuetype value_type;
    napi_transform_cancel *cancel;
    *out = NULL;
    if (!value) return 1;
    if (!napi_transform_check(env, napi_typeof(env, value, &value_type)))
        return 0;
    if (value_type == napi_undefined || value_type == napi_null) return 1;
    if (!napi_transform_check(env, napi_is_typedarray(
            env, value, &is_typedarray))
        || !is_typedarray
        || !napi_transform_check(env, napi_get_typedarray_info(
            env, value, &typed_type, &count, &data,
            &arraybuffer, &byte_offset))) {
        napi_throw_type_error(
            env, NULL, "cancelFlag must be a nonempty Int32Array");
        return 0;
    }
    (void)arraybuffer;
    (void)byte_offset;
    if (typed_type != napi_int32_array || count == 0 || !data
        || (uintptr_t)data % _Alignof(_Atomic int32_t) != 0) {
        napi_throw_type_error(
            env, NULL, "cancelFlag must be an aligned nonempty Int32Array");
        return 0;
    }
    cancel = (napi_transform_cancel *)calloc(1, sizeof(*cancel));
    if (!cancel) {
        napi_throw_error(env, NULL, "cancelFlag reference allocation failed");
        return 0;
    }
    cancel->flag = (const _Atomic int32_t *)data;
    cancel->poll_count = count >= 2
        ? (_Atomic uint32_t *)((uint8_t *)data + sizeof(int32_t))
        : NULL;
    if (!atomic_is_lock_free(cancel->flag)
        || (cancel->poll_count && !atomic_is_lock_free(cancel->poll_count))) {
        free(cancel);
        napi_transform_throw_code(
            env, TF_TRANSFORM_UNSUPPORTED_RUNTIME,
            "cancelFlag does not provide lock-free atomic int32 access");
        return 0;
    }
    if (napi_create_reference(env, value, 1, &cancel->reference) != napi_ok) {
        free(cancel);
        napi_throw_error(env, NULL, "cancelFlag reference creation failed");
        return 0;
    }
    *out = cancel;
    return 1;
}

static int napi_transform_parse_runtime_options(
    napi_env env, napi_value value,
    tf_transform_limits_v1 *limits, int *provided,
    napi_transform_cancel **cancel) {
    bool has_limits = false;
    bool has_cancel = false;
    napi_value limits_value = NULL;
    napi_value cancel_value = NULL;
    napi_valuetype type;
    *cancel = NULL;
    if (!value) return napi_transform_parse_limits(
        env, NULL, limits, provided);
    if (!napi_transform_check(env, napi_typeof(env, value, &type))) return 0;
    if (type == napi_undefined || type == napi_null)
        return napi_transform_parse_limits(env, NULL, limits, provided);
    if (type != napi_object) {
        napi_throw_type_error(env, NULL, "transform runtime options must be an object");
        return 0;
    }
    if (!napi_transform_check(env, napi_has_named_property(
            env, value, "limits", &has_limits))
        || !napi_transform_check(env, napi_has_named_property(
            env, value, "cancelFlag", &has_cancel))) return 0;
    if (!has_limits && !has_cancel)
        return napi_transform_parse_limits(env, value, limits, provided);
    if (has_limits && !napi_transform_check(env, napi_get_named_property(
            env, value, "limits", &limits_value))) return 0;
    if (has_cancel && !napi_transform_check(env, napi_get_named_property(
            env, value, "cancelFlag", &cancel_value))) return 0;
    if (!napi_transform_parse_limits(
            env, has_limits ? limits_value : NULL, limits, provided)
        || !napi_transform_parse_cancel_flag(
            env, has_cancel ? cancel_value : NULL, cancel)) {
        napi_transform_cancel_destroy(env, cancel);
        return 0;
    }
    return 1;
}

static void napi_transform_runtime_init(
    tf_transform_runtime_v1 *runtime,
    const tf_transform_limits_v1 *limits, int provided,
    const napi_transform_cancel *cancel) {
    memset(runtime, 0, sizeof(*runtime));
    runtime->abi_version = 1;
    runtime->struct_size = (uint32_t)sizeof(*runtime);
    runtime->limits = provided ? limits : NULL;
    if (cancel) {
        runtime->cancel = napi_transform_cancel_poll;
        runtime->cancel_user = (void *)cancel;
    }
}

static int napi_transform_get_size(
    napi_env env, napi_value value, const char *label, size_t *out) {
    double number;
    if (napi_get_value_double(env, value, &number) != napi_ok
        || !isfinite(number) || number < 0.0 || floor(number) != number
        || number > 9007199254740991.0
        || number > (double)SIZE_MAX) {
        napi_throw_range_error(env, NULL, label);
        return 0;
    }
    *out = (size_t)number;
    return 1;
}

static int napi_transform_get_named_size(
    napi_env env, napi_value object, const char *name,
    size_t default_value, int required, size_t *out) {
    bool has = false;
    napi_value value;
    if (!napi_transform_check(
            env, napi_has_named_property(env, object, name, &has))) return 0;
    if (!has) {
        if (required) {
            napi_throw_type_error(env, NULL, "required numeric property is missing");
            return 0;
        }
        *out = default_value;
        return 1;
    }
    if (!napi_transform_check(
            env, napi_get_named_property(env, object, name, &value))) return 0;
    return napi_transform_get_size(
        env, value, "numeric property must be a nonnegative safe integer", out);
}

static int napi_transform_parse_bytes(
    napi_env env, napi_value value, int allow_string,
    uint64_t max_len, uint64_t max_string_allocation,
    napi_transform_bytes *bytes);

static int napi_transform_get_string(
    napi_env env, napi_value value, char **out, size_t *out_len,
    uint64_t max_len, uint64_t max_allocation,
    uint64_t *decoded_total, uint64_t max_decoded,
    const napi_transform_cancel *cancel) {
    size_t len = 0;
    size_t offset = 0;
    char *copy;
    napi_valuetype type;
    if (!napi_transform_check(env, napi_typeof(env, value, &type))) return 0;
    if (napi_transform_cancel_poll((void *)cancel)) {
        napi_transform_throw_code(
            env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
        return 0;
    }
    if (type == napi_string) {
        if (!napi_transform_check(
                env, napi_get_value_string_utf8(
                    env, value, NULL, 0, &len))) return 0;
    } else {
        napi_transform_bytes bytes = {0};
        if (!napi_transform_parse_bytes(
                env, value, 0, max_len, max_allocation, &bytes)) return 0;
        len = bytes.len;
    }
    if ((uint64_t)len > max_len
        || (uint64_t)len >= max_allocation
        || (decoded_total
            && ((uint64_t)len > max_decoded - *decoded_total))) {
        napi_transform_throw_code(
            env, TF_TRANSFORM_RESOURCE_LIMIT,
            "schema strings exceed configured transform limits");
        return 0;
    }
    copy = (char *)malloc(len + 1);
    if (!copy) {
        napi_throw_error(env, NULL, "schema string allocation failed");
        return 0;
    }
    if (type == napi_string) {
        if (!napi_transform_check(env, napi_get_value_string_utf8(
                env, value, copy, len + 1, &len))) {
            free(copy);
            return 0;
        }
    } else {
        napi_transform_bytes bytes = {0};
        if (!napi_transform_parse_bytes(
                env, value, 0, max_len, max_allocation, &bytes)) {
            free(copy);
            return 0;
        }
        while (offset < len) {
            size_t chunk = len - offset;
            if (chunk > TF_TRANSFORM_CANCEL_BYTES_V1)
                chunk = TF_TRANSFORM_CANCEL_BYTES_V1;
            if (napi_transform_cancel_poll((void *)cancel)) {
                free(copy);
                napi_transform_throw_code(
                    env, TF_TRANSFORM_CANCELLED,
                    "prepared transform cancelled");
                return 0;
            }
            memcpy(copy + offset, bytes.data + offset, chunk);
            offset += chunk;
        }
        copy[len] = '\0';
    }
    if (napi_transform_cancel_poll((void *)cancel)) {
        free(copy);
        napi_transform_throw_code(
            env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
        return 0;
    }
    if (len == 0 || memchr(copy, '\0', len) != NULL) {
        free(copy);
        napi_throw_type_error(env, NULL, "schema ID/name must be nonempty UTF-8");
        return 0;
    }
    *out = copy;
    *out_len = len;
    if (decoded_total) *decoded_total += (uint64_t)len;
    return 1;
}

static int napi_transform_get_named_string(
    napi_env env, napi_value object, const char *name,
    char **out, size_t *out_len, const tf_transform_limits_v1 *limits,
    uint64_t *decoded_total, const napi_transform_cancel *cancel) {
    napi_value value;
    if (!napi_transform_check(
            env, napi_get_named_property(env, object, name, &value))) return 0;
    return napi_transform_get_string(
        env, value, out, out_len, limits->max_string_bytes,
        limits->max_allocation_bytes,
        decoded_total, limits->max_decoded_string_bytes, cancel);
}

static void napi_transform_schema_clear(
    napi_transform_schema_owner *owner) {
    if (!owner) return;
    for (size_t i = 0; i < owner->count; ++i) {
        free(owner->ids ? owner->ids[i] : NULL);
        free(owner->names ? owner->names[i] : NULL);
    }
    free(owner->fields);
    free(owner->ids);
    free(owner->names);
    free(owner->dtypes);
    memset(owner, 0, sizeof(*owner));
}

static int napi_transform_parse_schema(
    napi_env env, napi_value value, const tf_transform_limits_v1 *limits,
    const napi_transform_cancel *cancel,
    napi_transform_schema_owner *owner) {
    napi_value fields_value;
    bool is_array = false;
    uint32_t count_u32 = 0;
    uint64_t decoded_total = 0;
    memset(owner, 0, sizeof(*owner));
    if (!napi_transform_check(
            env, napi_get_named_property(env, value, "fields", &fields_value))
        || !napi_transform_check(env, napi_is_array(env, fields_value, &is_array)))
        return 0;
    if (!is_array
        || !napi_transform_check(
            env, napi_get_array_length(env, fields_value, &count_u32))) {
        napi_throw_type_error(env, NULL, "schema.fields must be an array");
        return 0;
    }
    if (count_u32 == 0) {
        napi_throw_type_error(env, NULL, "schema.fields must be nonempty");
        return 0;
    }
    if ((uint64_t)count_u32 > limits->max_input_columns) {
        napi_transform_throw_code(
            env, TF_TRANSFORM_RESOURCE_LIMIT,
            "schema column count exceeds configured transform limits");
        return 0;
    }
    owner->count = (size_t)count_u32;
    if ((uint64_t)owner->count > limits->max_allocation_bytes
            / sizeof(*owner->fields)
        || (uint64_t)owner->count > limits->max_allocation_bytes
            / sizeof(*owner->ids)
        || (uint64_t)owner->count > limits->max_allocation_bytes
            / sizeof(*owner->names)
        || (uint64_t)owner->count > limits->max_allocation_bytes
            / sizeof(*owner->dtypes)) {
        napi_transform_throw_code(
            env, TF_TRANSFORM_RESOURCE_LIMIT,
            "schema descriptors exceed configured allocation limit");
        return 0;
    }
    owner->fields = (tf_field_view_v1 *)calloc(
        owner->count, sizeof(*owner->fields));
    owner->ids = (char **)calloc(owner->count, sizeof(*owner->ids));
    owner->names = (char **)calloc(owner->count, sizeof(*owner->names));
    owner->dtypes = (uint32_t *)calloc(owner->count, sizeof(*owner->dtypes));
    if (!owner->fields || !owner->ids || !owner->names || !owner->dtypes) {
        napi_transform_schema_clear(owner);
        napi_throw_error(env, NULL, "schema descriptor allocation failed");
        return 0;
    }
    for (size_t i = 0; i < owner->count; ++i) {
        napi_value field;
        napi_value dtype_value;
        char *dtype = NULL;
        size_t dtype_len = 0;
        size_t id_len = 0;
        size_t name_len = 0;
        if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0
            && napi_transform_cancel_poll((void *)cancel)) {
            napi_transform_schema_clear(owner);
            napi_transform_throw_code(
                env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
            return 0;
        }
        if (!napi_transform_check(
                env, napi_get_element(env, fields_value, (uint32_t)i, &field))
            || !napi_transform_get_named_string(
                env, field, "id", &owner->ids[i], &id_len,
                limits, &decoded_total, cancel)
            || !napi_transform_get_named_string(
                env, field, "name", &owner->names[i], &name_len,
                limits, &decoded_total, cancel)
            || !napi_transform_check(
                env, napi_get_named_property(env, field, "dtype", &dtype_value))
            || !napi_transform_get_string(
                env, dtype_value, &dtype, &dtype_len,
                UINT64_MAX, limits->max_allocation_bytes,
                NULL, UINT64_MAX, cancel)) {
            free(dtype);
            napi_transform_schema_clear(owner);
            return 0;
        }
        if (dtype_len == 7 && memcmp(dtype, "float32", 7) == 0)
            owner->dtypes[i] = TF_VIEW_FLOAT32;
        else if (dtype_len == 7 && memcmp(dtype, "float64", 7) == 0)
            owner->dtypes[i] = TF_VIEW_FLOAT64;
        else {
            free(dtype);
            napi_transform_schema_clear(owner);
            napi_throw_type_error(env, NULL, "schema dtype must be float32 or float64");
            return 0;
        }
        free(dtype);
        owner->fields[i].abi_version = 1;
        owner->fields[i].struct_size = (uint32_t)sizeof(owner->fields[i]);
        owner->fields[i].dtype = owner->dtypes[i];
        owner->fields[i].id_utf8 = (const uint8_t *)owner->ids[i];
        owner->fields[i].id_bytes = id_len;
        owner->fields[i].name_utf8 = (const uint8_t *)owner->names[i];
        owner->fields[i].name_bytes = name_len;
    }
    owner->view.abi_version = 1;
    owner->view.struct_size = (uint32_t)sizeof(owner->view);
    owner->view.column_count = owner->count;
    owner->view.fields = owner->fields;
    owner->view.fields_bytes = owner->count * sizeof(*owner->fields);
    if (napi_transform_cancel_poll((void *)cancel)) {
        napi_transform_schema_clear(owner);
        napi_transform_throw_code(
            env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
        return 0;
    }
    return 1;
}

static void napi_transform_bytes_clear(napi_transform_bytes *bytes) {
    if (!bytes) return;
    free(bytes->owned);
    memset(bytes, 0, sizeof(*bytes));
}

static int napi_transform_parse_bytes(
    napi_env env, napi_value value, int allow_string,
    uint64_t max_len, uint64_t max_string_allocation,
    napi_transform_bytes *bytes) {
    bool is_buffer = false;
    bool is_typedarray = false;
    napi_valuetype type;
    memset(bytes, 0, sizeof(*bytes));
    if (!napi_transform_check(env, napi_typeof(env, value, &type))) return 0;
    if (allow_string && type == napi_string) {
        size_t written = 0;
        if (!napi_transform_check(
                env, napi_get_value_string_utf8(env, value, NULL, 0, &bytes->len)))
            return 0;
        if ((uint64_t)bytes->len > max_len
            || (uint64_t)bytes->len >= max_string_allocation) {
            napi_transform_throw_code(
                env, TF_TRANSFORM_RESOURCE_LIMIT,
                "prepared-transform input exceeds configured byte limit");
            return 0;
        }
        bytes->owned = (uint8_t *)malloc(bytes->len + 1);
        if (!bytes->owned) {
            napi_throw_error(env, NULL, "prepared-transform byte allocation failed");
            return 0;
        }
        if (!napi_transform_check(env, napi_get_value_string_utf8(
                env, value, (char *)bytes->owned, bytes->len + 1, &written))) {
            napi_transform_bytes_clear(bytes);
            return 0;
        }
        bytes->len = written;
        bytes->data = bytes->owned;
        return 1;
    }
    if (!napi_transform_check(env, napi_is_buffer(env, value, &is_buffer))) return 0;
    if (is_buffer) {
        void *data = NULL;
        if (!napi_transform_check(
                env, napi_get_buffer_info(env, value, &data, &bytes->len))) return 0;
        if ((uint64_t)bytes->len > max_len) {
            napi_transform_throw_code(
                env, TF_TRANSFORM_RESOURCE_LIMIT,
                "prepared-transform input exceeds configured byte limit");
            return 0;
        }
        bytes->data = (const uint8_t *)data;
        return 1;
    }
    if (!napi_transform_check(
            env, napi_is_typedarray(env, value, &is_typedarray))) return 0;
    if (is_typedarray) {
        napi_typedarray_type typed_type;
        size_t count;
        void *data = NULL;
        napi_value arraybuffer;
        size_t byte_offset;
        if (!napi_transform_check(env, napi_get_typedarray_info(
                env, value, &typed_type, &count, &data,
                &arraybuffer, &byte_offset))) return 0;
        (void)arraybuffer;
        (void)byte_offset;
        if (typed_type != napi_uint8_array
            && typed_type != napi_uint8_clamped_array) {
            napi_throw_type_error(env, NULL, "expected a UTF-8 string or byte array");
            return 0;
        }
        if ((uint64_t)count > max_len) {
            napi_transform_throw_code(
                env, TF_TRANSFORM_RESOURCE_LIMIT,
                "prepared-transform input exceeds configured byte limit");
            return 0;
        }
        bytes->data = (const uint8_t *)data;
        bytes->len = count;
        return 1;
    }
    napi_throw_type_error(env, NULL, "expected a UTF-8 string or byte array");
    return 0;
}

static int napi_transform_parse_numeric_array(
    napi_env env, napi_value value, uint32_t expected_dtype,
    const void **data, size_t *data_bytes, size_t *item_size) {
    bool is_typedarray = false;
    napi_typedarray_type typed_type;
    size_t count;
    void *pointer = NULL;
    napi_value arraybuffer;
    size_t byte_offset;
    if (!napi_transform_check(
            env, napi_is_typedarray(env, value, &is_typedarray))) return 0;
    if (!is_typedarray
        || !napi_transform_check(env, napi_get_typedarray_info(
            env, value, &typed_type, &count, &pointer,
            &arraybuffer, &byte_offset))) {
        napi_throw_type_error(env, NULL, "table data must be a typed array");
        return 0;
    }
    (void)arraybuffer;
    (void)byte_offset;
    if (expected_dtype == TF_VIEW_FLOAT32 && typed_type == napi_float32_array)
        *item_size = sizeof(float);
    else if (expected_dtype == TF_VIEW_FLOAT64 && typed_type == napi_float64_array)
        *item_size = sizeof(double);
    else {
        napi_throw_type_error(env, NULL, "table typed array dtype mismatches schema");
        return 0;
    }
    if (count > SIZE_MAX / *item_size) {
        napi_throw_range_error(env, NULL, "table typed array is too large");
        return 0;
    }
    *data_bytes = count * *item_size;
    *data = *data_bytes == 0 ? NULL : pointer;
    return 1;
}

static int napi_transform_parse_validity(
    napi_env env, napi_value value, const uint8_t **data, size_t *len) {
    napi_transform_bytes bytes = {0};
    if (!napi_transform_parse_bytes(
            env, value, 0, UINT64_MAX, UINT64_MAX, &bytes)) return 0;
    *data = bytes.len == 0 ? NULL : bytes.data;
    *len = bytes.len;
    return 1;
}

static void napi_transform_table_clear(napi_transform_table_owner *owner) {
    if (!owner) return;
    free(owner->columns);
    memset(owner, 0, sizeof(*owner));
}

static int napi_transform_parse_table(
    napi_env env, napi_value value,
    const napi_transform_handle *handle,
    napi_transform_table_owner *owner, int *cancelled) {
    napi_value rows_value;
    napi_value columns_value;
    bool is_array = false;
    uint32_t count_u32 = 0;
    size_t rows;
    memset(owner, 0, sizeof(*owner));
    if (cancelled) *cancelled = 0;
    if (!napi_transform_check(
            env, napi_get_named_property(env, value, "rows", &rows_value))
        || !napi_transform_get_size(
            env, rows_value, "table.rows must be a nonnegative safe integer", &rows)
        || !napi_transform_check(
            env, napi_get_named_property(env, value, "columns", &columns_value))
        || !napi_transform_check(env, napi_is_array(env, columns_value, &is_array))
        || !is_array
        || !napi_transform_check(
            env, napi_get_array_length(env, columns_value, &count_u32))) {
        if (!is_array) napi_throw_type_error(env, NULL, "table.columns must be an array");
        return 0;
    }
    if ((size_t)count_u32 != handle->column_count) {
        napi_throw_range_error(env, NULL, "table column count mismatches schema");
        return 0;
    }
    if ((uint64_t)handle->column_count > handle->limits.max_allocation_bytes
            / sizeof(*owner->columns)) {
        napi_transform_throw_code(
            env, TF_TRANSFORM_RESOURCE_LIMIT,
            "table descriptors exceed configured allocation limit");
        return 0;
    }
    owner->columns = (tf_column_view_v1 *)calloc(
        handle->column_count, sizeof(*owner->columns));
    if (!owner->columns) {
        napi_throw_error(env, NULL, "table descriptor allocation failed");
        return 0;
    }
    for (size_t i = 0; i < handle->column_count; ++i) {
        napi_value config;
        napi_value data_value;
        napi_value validity_value;
        napi_valuetype config_type;
        bool direct_typed = false;
        bool has_validity = false;
        const void *data = NULL;
        const uint8_t *validity = NULL;
        size_t data_bytes = 0;
        size_t validity_bytes = 0;
        size_t item_size = 0;
        size_t stride;
        size_t bit_offset = 0;
        size_t bit_stride = 0;
        if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0
            && napi_transform_cancel_poll(handle->cancel)) {
            if (cancelled) *cancelled = 1;
            napi_transform_throw_code(
                env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
            goto fail;
        }
        if (!napi_transform_check(
                env, napi_get_element(env, columns_value, (uint32_t)i, &config))
            || !napi_transform_check(env, napi_is_typedarray(
                env, config, &direct_typed))) goto fail;
        if (direct_typed) {
            data_value = config;
        } else {
            if (!napi_transform_check(env, napi_typeof(
                    env, config, &config_type)) || config_type != napi_object) {
                napi_throw_type_error(
                    env, NULL, "table columns must be typed arrays or objects");
                goto fail;
            }
            if (!napi_transform_check(env, napi_get_named_property(
                    env, config, "data", &data_value))) goto fail;
        }
        if (!napi_transform_parse_numeric_array(
                env, data_value, handle->dtypes[i],
                &data, &data_bytes, &item_size)) goto fail;
        stride = item_size;
        if (!direct_typed) {
            if (!napi_transform_get_named_size(
                    env, config, "strideBytes", item_size, 0, &stride)
                || !napi_transform_check(env, napi_has_named_property(
                    env, config, "validity", &has_validity))) goto fail;
        }
        if (stride == 0) {
            napi_throw_range_error(env, NULL, "table strideBytes must be positive");
            goto fail;
        }
        if (has_validity) {
            if (!napi_transform_check(env, napi_get_named_property(
                    env, config, "validity", &validity_value))
                || !napi_transform_parse_validity(
                    env, validity_value, &validity, &validity_bytes)
                || !napi_transform_get_named_size(
                    env, config, "validityBitOffset", 0, 0, &bit_offset)
                || !napi_transform_get_named_size(
                    env, config, "validityBitStride", 1, 0, &bit_stride)) goto fail;
            if (bit_stride == 0) {
                napi_throw_range_error(
                    env, NULL, "table validityBitStride must be positive");
                goto fail;
            }
        }
        owner->columns[i].abi_version = 1;
        owner->columns[i].struct_size = (uint32_t)sizeof(owner->columns[i]);
        owner->columns[i].data = data;
        owner->columns[i].data_bytes = data_bytes;
        owner->columns[i].stride_bytes = stride;
        owner->columns[i].validity = validity;
        owner->columns[i].validity_bytes = validity_bytes;
        owner->columns[i].validity_bit_offset = bit_offset;
        owner->columns[i].validity_bit_stride = bit_stride;
    }
    owner->view.abi_version = 1;
    owner->view.struct_size = (uint32_t)sizeof(owner->view);
    owner->view.row_count = rows;
    owner->view.column_count = handle->column_count;
    owner->view.columns = owner->columns;
    owner->view.columns_bytes = handle->column_count * sizeof(*owner->columns);
    if (napi_transform_cancel_poll(handle->cancel)) {
        if (cancelled) *cancelled = 1;
        napi_transform_throw_code(
            env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
        goto fail;
    }
    return 1;
fail:
    napi_transform_table_clear(owner);
    return 0;
}

static napi_value napi_transform_buffer_copy(
    napi_env env, const uint8_t *bytes, size_t len) {
    napi_value result = NULL;
    if (!napi_transform_check(
            env, napi_create_buffer_copy(env, len, bytes, NULL, &result))) return NULL;
    return result;
}

static int napi_transform_prepare_dense_result(
    napi_env env, size_t rows, size_t columns,
    const tf_transform_limits_v1 *limits,
    const napi_transform_cancel *cancel, int *cancelled,
    napi_transform_dense_result_owner *owner) {
    napi_value rows_value = NULL;
    napi_value columns_value = NULL;
    napi_value arraybuffer = NULL;
    napi_value data = NULL;
    size_t elements;
    memset(owner, 0, sizeof(*owner));
    if (cancelled) *cancelled = 0;
    if (columns == 0 || rows > SIZE_MAX / columns) {
        napi_transform_throw_code(
            env, TF_TRANSFORM_RESOURCE_LIMIT,
            "prepared-transform output shape exceeds host limits");
        return 0;
    }
    elements = rows * columns;
    if ((uint64_t)elements > limits->max_output_elements_per_call
        || elements > SIZE_MAX / sizeof(double)
        || (uint64_t)(elements * sizeof(double))
            > limits->max_allocation_bytes) {
        napi_transform_throw_code(
            env, TF_TRANSFORM_RESOURCE_LIMIT,
            "prepared-transform output exceeds configured limits");
        return 0;
    }
    owner->rows = rows;
    owner->columns = columns;
    owner->data_bytes = elements * sizeof(double);
    if (!napi_transform_check(env, napi_create_object(env, &owner->result))
        || !napi_transform_check(env, napi_create_double(
            env, (double)rows, &rows_value))
        || !napi_transform_check(env, napi_create_double(
            env, (double)columns, &columns_value))) return 0;
    if (napi_transform_cancel_poll((void *)cancel)) {
        if (cancelled) *cancelled = 1;
        napi_transform_throw_code(
            env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
        return 0;
    }
    if (!napi_transform_check(env, napi_create_arraybuffer(
            env, owner->data_bytes, &owner->data, &arraybuffer))
        || !napi_transform_check(env, napi_create_typedarray(
            env, napi_float64_array, elements, arraybuffer, 0, &data))) return 0;
    if (!napi_transform_check(env, napi_set_named_property(
            env, owner->result, "rows", rows_value))
        || !napi_transform_check(env, napi_set_named_property(
            env, owner->result, "columns", columns_value))
        || !napi_transform_check(env, napi_set_named_property(
            env, owner->result, "data", data))) return 0;
    return 1;
}

static int napi_transform_copy_dense_result(
    napi_env env, napi_transform_dense_result_owner *owner,
    const tf_owned_dense_v1 *dense,
    const napi_transform_cancel *cancel) {
    size_t offset = 0;
    if (dense->dtype != TF_VIEW_FLOAT64
        || dense->rows != owner->rows
        || dense->columns != owner->columns
        || dense->data_bytes != owner->data_bytes
        || (dense->data_bytes != 0 && !dense->data)) {
        napi_throw_error(env, NULL, "prepared-transform dense output is invalid");
        return 0;
    }
    while (offset < dense->data_bytes) {
        size_t chunk = dense->data_bytes - offset;
        if (chunk > TF_TRANSFORM_CANCEL_BYTES_V1)
            chunk = TF_TRANSFORM_CANCEL_BYTES_V1;
        if (napi_transform_cancel_poll((void *)cancel)) {
            napi_transform_throw_code(
                env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
            return 0;
        }
        memcpy((uint8_t *)owner->data + offset,
               (const uint8_t *)dense->data + offset, chunk);
        offset += chunk;
    }
    if (napi_transform_cancel_poll((void *)cancel)) {
        napi_transform_throw_code(
            env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
        return 0;
    }
    return 1;
}

static napi_value napi_transform_safe_limits(
    napi_env env, napi_callback_info info) {
    tf_transform_limits_v1 limits;
    napi_value result = NULL;
    (void)info;
    if (tf_transform_limits_init_safe_v1(&limits, sizeof(limits))
            != TF_TRANSFORM_OK
        || !napi_transform_check(env, napi_create_object(env, &result))) return NULL;
    for (size_t i = 0; i < sizeof(LIMIT_FIELDS) / sizeof(LIMIT_FIELDS[0]); ++i) {
        const uint64_t *field = (const uint64_t *)(
            (const uint8_t *)&limits + LIMIT_FIELDS[i].offset);
        napi_value value = NULL;
        if (*field > UINT64_C(9007199254740991)
            || !napi_transform_check(
                env, napi_create_double(env, (double)*field, &value))
            || !napi_transform_check(env, napi_set_named_property(
                env, result, LIMIT_FIELDS[i].name, value))) return NULL;
    }
    return result;
}

static napi_value napi_transform_recipe_from_json(
    napi_env env, napi_callback_info info) {
    napi_value argv[2] = {0};
    size_t argc = 2;
    napi_transform_bytes bytes = {0};
    tf_transform_limits_v1 limits;
    tf_transform_recipe *recipe = NULL;
    tf_transform_error *error = NULL;
    tf_transform_code code;
    int provided;
    if (!napi_transform_check(
            env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL))) return NULL;
    if (argc < 1) {
        napi_throw_type_error(env, NULL, "transformRecipeFromJson requires bytes");
        return NULL;
    }
    if (!napi_transform_parse_limits(
            env, argc >= 2 ? argv[1] : NULL, &limits, &provided)
        || !napi_transform_parse_bytes(
            env, argv[0], 1,
            limits.max_recipe_bytes < limits.max_allocation_bytes
                ? limits.max_recipe_bytes : limits.max_allocation_bytes,
            limits.max_allocation_bytes,
            &bytes)) {
        napi_transform_bytes_clear(&bytes);
        return NULL;
    }
    code = tf_transform_recipe_from_json(
        bytes.data, bytes.len, provided ? &limits : NULL, &recipe, &error);
    napi_transform_bytes_clear(&bytes);
    if (code != TF_TRANSFORM_OK)
        return napi_transform_throw_error(env, code, &error);
    return napi_transform_new_handle(
        env, NAPI_TRANSFORM_RECIPE, recipe, NULL, 0);
}

static napi_value napi_transform_analyzer_create(
    napi_env env, napi_callback_info info) {
    napi_value argv[3] = {0};
    size_t argc = 3;
    napi_transform_handle *recipe;
    napi_transform_schema_owner schema = {0};
    tf_transform_limits_v1 limits;
    tf_transform_runtime_v1 runtime;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_error *error = NULL;
    napi_transform_cancel *cancel = NULL;
    tf_transform_code code;
    int provided;
    if (!napi_transform_check(
            env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL))) return NULL;
    if (argc < 2) {
        napi_throw_type_error(env, NULL, "transformAnalyzerCreate requires recipe and schema");
        return NULL;
    }
    recipe = napi_transform_get_handle(env, argv[0], NAPI_TRANSFORM_RECIPE);
    if (!recipe) return NULL;
    if (!napi_transform_parse_runtime_options(
            env, argc >= 3 ? argv[2] : NULL,
            &limits, &provided, &cancel)) {
        napi_transform_cancel_destroy(env, &cancel);
        return NULL;
    }
    if (napi_transform_cancel_poll(cancel)) {
        napi_transform_cancel_destroy(env, &cancel);
        return napi_transform_throw_code(
            env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
    }
    if (!napi_transform_parse_schema(
            env, argv[1], &limits, cancel, &schema)) {
        napi_transform_schema_clear(&schema);
        napi_transform_cancel_destroy(env, &cancel);
        return NULL;
    }
    if (napi_transform_cancel_poll(cancel)) {
        napi_transform_schema_clear(&schema);
        napi_transform_cancel_destroy(env, &cancel);
        return napi_transform_throw_code(
            env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
    }
    napi_transform_runtime_init(&runtime, &limits, provided, cancel);
    code = tf_transform_analyzer_create(
        (tf_transform_recipe *)recipe->pointer, &schema.view,
        &runtime, &analyzer, &error);
    if (code != TF_TRANSFORM_OK) {
        napi_transform_schema_clear(&schema);
        napi_transform_cancel_destroy(env, &cancel);
        return napi_transform_throw_error(env, code, &error);
    }
    {
        napi_value result = napi_transform_new_handle_with_cancel(
            env, NAPI_TRANSFORM_ANALYZER, analyzer,
            schema.dtypes, schema.count, 0, &limits, cancel);
        napi_transform_schema_clear(&schema);
        return result;
    }
}

static napi_value napi_transform_analyzer_push(
    napi_env env, napi_callback_info info) {
    napi_value argv[2] = {0};
    size_t argc = 2;
    napi_transform_handle *analyzer;
    napi_transform_table_owner table;
    tf_transform_error *error = NULL;
    tf_transform_code code;
    int cancelled = 0;
    if (!napi_transform_check(
            env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL))) return NULL;
    if (argc < 2) {
        napi_throw_type_error(env, NULL, "transformAnalyzerPush requires analyzer and table");
        return NULL;
    }
    analyzer = napi_transform_get_handle(env, argv[0], NAPI_TRANSFORM_ANALYZER);
    if (!analyzer) return NULL;
    if (napi_transform_cancel_poll(analyzer->cancel)) {
        napi_transform_destroy_payload(env, analyzer);
        return napi_transform_throw_code(
            env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
    }
    if (!napi_transform_parse_table(
            env, argv[1], analyzer, &table, &cancelled)) {
        if (cancelled) napi_transform_destroy_payload(env, analyzer);
        return NULL;
    }
    if (napi_transform_cancel_poll(analyzer->cancel)) {
        napi_transform_table_clear(&table);
        napi_transform_destroy_payload(env, analyzer);
        return napi_transform_throw_code(
            env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
    }
    code = tf_transform_analyzer_push(
        (tf_transform_analyzer *)analyzer->pointer, &table.view, &error);
    napi_transform_table_clear(&table);
    if (code != TF_TRANSFORM_OK) {
        if (code == TF_TRANSFORM_CANCELLED)
            napi_transform_destroy_payload(env, analyzer);
        return napi_transform_throw_error(env, code, &error);
    }
    return napi_transform_undefined(env);
}

static napi_value napi_transform_analyzer_finalize(
    napi_env env, napi_callback_info info) {
    napi_value argv[1] = {0};
    size_t argc = 1;
    napi_transform_handle *analyzer;
    tf_transform_plan *plan = NULL;
    tf_transform_error *error = NULL;
    tf_transform_code code;
    if (!napi_transform_check(
            env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL))) return NULL;
    if (argc < 1) {
        napi_throw_type_error(env, NULL, "transformAnalyzerFinalize requires analyzer");
        return NULL;
    }
    analyzer = napi_transform_get_handle(env, argv[0], NAPI_TRANSFORM_ANALYZER);
    if (!analyzer) return NULL;
    code = tf_transform_analyzer_finalize(
        (tf_transform_analyzer *)analyzer->pointer, &plan, &error);
    if (code != TF_TRANSFORM_OK) {
        if (code == TF_TRANSFORM_CANCELLED)
            napi_transform_destroy_payload(env, analyzer);
        return napi_transform_throw_error(env, code, &error);
    }
    return napi_transform_new_handle(env, NAPI_TRANSFORM_PLAN, plan, NULL, 0);
}

static napi_value napi_transform_plan_export(
    napi_env env, napi_callback_info info) {
    napi_value argv[2] = {0};
    size_t argc = 2;
    napi_transform_handle *plan;
    tf_transform_limits_v1 limits;
    uint8_t *bytes = NULL;
    size_t len = 0;
    tf_transform_error *error = NULL;
    tf_transform_code code;
    int provided;
    napi_value result;
    if (!napi_transform_check(
            env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL))) return NULL;
    if (argc < 1) {
        napi_throw_type_error(env, NULL, "transformPlanExport requires plan");
        return NULL;
    }
    plan = napi_transform_get_handle(env, argv[0], NAPI_TRANSFORM_PLAN);
    if (!plan || !napi_transform_parse_limits(
            env, argc >= 2 ? argv[1] : NULL, &limits, &provided)) return NULL;
    code = tf_transform_plan_export(
        (tf_transform_plan *)plan->pointer, provided ? &limits : NULL,
        &bytes, &len, &error);
    if (code != TF_TRANSFORM_OK)
        return napi_transform_throw_error(env, code, &error);
    result = napi_transform_buffer_copy(env, bytes, len);
    tf_transform_bytes_free(&bytes, &len);
    return result;
}

static napi_value napi_transform_plan_import(
    napi_env env, napi_callback_info info) {
    napi_value argv[2] = {0};
    size_t argc = 2;
    napi_transform_bytes bytes = {0};
    tf_transform_limits_v1 limits;
    tf_transform_runtime_v1 runtime;
    tf_transform_plan *plan = NULL;
    tf_transform_error *error = NULL;
    napi_transform_cancel *cancel = NULL;
    tf_transform_code code;
    int provided;
    if (!napi_transform_check(
            env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL))) return NULL;
    if (argc < 1) {
        napi_throw_type_error(env, NULL, "transformPlanImport requires bytes");
        return NULL;
    }
    if (!napi_transform_parse_runtime_options(
            env, argc >= 2 ? argv[1] : NULL,
            &limits, &provided, &cancel)) {
        napi_transform_cancel_destroy(env, &cancel);
        return NULL;
    }
    if (napi_transform_cancel_poll(cancel)) {
        napi_transform_cancel_destroy(env, &cancel);
        return napi_transform_throw_code(
            env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
    }
    if (!napi_transform_parse_bytes(
            env, argv[0], 0,
            limits.max_plan_bytes < limits.max_allocation_bytes
                ? limits.max_plan_bytes : limits.max_allocation_bytes,
            limits.max_allocation_bytes,
            &bytes)) {
        napi_transform_bytes_clear(&bytes);
        napi_transform_cancel_destroy(env, &cancel);
        return NULL;
    }
    napi_transform_runtime_init(&runtime, &limits, provided, cancel);
    code = tf_transform_plan_import(
        bytes.data, bytes.len, &runtime, &plan, &error);
    napi_transform_bytes_clear(&bytes);
    napi_transform_cancel_destroy(env, &cancel);
    if (code != TF_TRANSFORM_OK)
        return napi_transform_throw_error(env, code, &error);
    return napi_transform_new_handle(env, NAPI_TRANSFORM_PLAN, plan, NULL, 0);
}

static napi_value napi_transform_plan_schema_json(
    napi_env env, napi_callback_info info) {
    napi_value argv[3] = {0};
    size_t argc = 3;
    napi_transform_handle *plan;
    uint32_t which;
    tf_transform_limits_v1 limits;
    uint8_t *bytes = NULL;
    size_t len = 0;
    tf_transform_error *error = NULL;
    tf_transform_code code;
    int provided;
    napi_value result;
    if (!napi_transform_check(
            env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL))) return NULL;
    if (argc < 2) {
        napi_throw_type_error(env, NULL, "transformPlanSchemaJson requires plan and selector");
        return NULL;
    }
    plan = napi_transform_get_handle(env, argv[0], NAPI_TRANSFORM_PLAN);
    if (!plan
        || !napi_transform_check(env, napi_get_value_uint32(env, argv[1], &which))
        || !napi_transform_parse_limits(
            env, argc >= 3 ? argv[2] : NULL, &limits, &provided)) return NULL;
    code = tf_transform_plan_schema_json(
        (tf_transform_plan *)plan->pointer, which,
        provided ? &limits : NULL, &bytes, &len, &error);
    if (code != TF_TRANSFORM_OK)
        return napi_transform_throw_error(env, code, &error);
    result = napi_transform_buffer_copy(env, bytes, len);
    tf_transform_bytes_free(&bytes, &len);
    return result;
}

static napi_value napi_transform_apply_create(
    napi_env env, napi_callback_info info) {
    napi_value argv[3] = {0};
    size_t argc = 3;
    napi_transform_handle *plan;
    napi_transform_schema_owner schema = {0};
    tf_transform_limits_v1 limits;
    tf_transform_runtime_v1 runtime;
    tf_transform_apply *apply = NULL;
    const tf_transform_schema *output_schema = NULL;
    size_t output_column_count = 0;
    tf_transform_error *error = NULL;
    napi_transform_cancel *cancel = NULL;
    tf_transform_code code;
    int provided;
    if (!napi_transform_check(
            env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL))) return NULL;
    if (argc < 2) {
        napi_throw_type_error(env, NULL, "transformApplyCreate requires plan and schema");
        return NULL;
    }
    plan = napi_transform_get_handle(env, argv[0], NAPI_TRANSFORM_PLAN);
    if (!plan) return NULL;
    if (!napi_transform_parse_runtime_options(
            env, argc >= 3 ? argv[2] : NULL,
            &limits, &provided, &cancel)) {
        napi_transform_cancel_destroy(env, &cancel);
        return NULL;
    }
    if (napi_transform_cancel_poll(cancel)) {
        napi_transform_cancel_destroy(env, &cancel);
        return napi_transform_throw_code(
            env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
    }
    if (!napi_transform_parse_schema(
            env, argv[1], &limits, cancel, &schema)) {
        napi_transform_schema_clear(&schema);
        napi_transform_cancel_destroy(env, &cancel);
        return NULL;
    }
    if (napi_transform_cancel_poll(cancel)) {
        napi_transform_schema_clear(&schema);
        napi_transform_cancel_destroy(env, &cancel);
        return napi_transform_throw_code(
            env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
    }
    napi_transform_runtime_init(&runtime, &limits, provided, cancel);
    code = tf_transform_plan_schema(
        (tf_transform_plan *)plan->pointer, TF_TRANSFORM_SCHEMA_OUTPUT,
        &output_schema, &error);
    if (code != TF_TRANSFORM_OK) {
        napi_transform_schema_clear(&schema);
        napi_transform_cancel_destroy(env, &cancel);
        return napi_transform_throw_error(env, code, &error);
    }
    output_column_count = tf_transform_schema_field_count(output_schema);
    code = tf_transform_apply_create(
        (tf_transform_plan *)plan->pointer, &schema.view,
        &runtime, &apply, &error);
    if (code != TF_TRANSFORM_OK) {
        napi_transform_schema_clear(&schema);
        napi_transform_cancel_destroy(env, &cancel);
        return napi_transform_throw_error(env, code, &error);
    }
    {
        napi_value result = napi_transform_new_handle_with_cancel(
            env, NAPI_TRANSFORM_APPLY, apply,
            schema.dtypes, schema.count, output_column_count, &limits, cancel);
        napi_transform_schema_clear(&schema);
        return result;
    }
}

static napi_value napi_transform_apply_run(
    napi_env env, napi_callback_info info) {
    napi_value argv[2] = {0};
    size_t argc = 2;
    napi_transform_handle *apply;
    napi_transform_table_owner table;
    napi_transform_dense_result_owner result_owner;
    tf_owned_dense_v1 dense;
    tf_transform_error *error = NULL;
    tf_transform_code code;
    int cancelled = 0;
    memset(&dense, 0, sizeof(dense));
    if (!napi_transform_check(
            env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL))) return NULL;
    if (argc < 2) {
        napi_throw_type_error(env, NULL, "transformApplyRun requires apply and table");
        return NULL;
    }
    apply = napi_transform_get_handle(env, argv[0], NAPI_TRANSFORM_APPLY);
    if (!apply) return NULL;
    if (napi_transform_cancel_poll(apply->cancel)) {
        napi_transform_destroy_payload(env, apply);
        return napi_transform_throw_code(
            env, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
    }
    if (!napi_transform_parse_table(
            env, argv[1], apply, &table, &cancelled)) {
        if (cancelled) napi_transform_destroy_payload(env, apply);
        return NULL;
    }
    if (!napi_transform_prepare_dense_result(
            env, table.view.row_count, apply->output_column_count,
            &apply->limits, apply->cancel, &cancelled, &result_owner)) {
        napi_transform_table_clear(&table);
        if (cancelled) napi_transform_destroy_payload(env, apply);
        return NULL;
    }
    code = tf_transform_apply_run(
        (tf_transform_apply *)apply->pointer, &table.view, &dense, &error);
    napi_transform_table_clear(&table);
    if (code != TF_TRANSFORM_OK) {
        if (code == TF_TRANSFORM_CANCELLED)
            napi_transform_destroy_payload(env, apply);
        return napi_transform_throw_error(env, code, &error);
    }
    if (!napi_transform_copy_dense_result(
            env, &result_owner, &dense, apply->cancel)) {
        tf_owned_dense_free(&dense);
        napi_transform_destroy_payload(env, apply);
        return NULL;
    }
    tf_owned_dense_free(&dense);
    return result_owner.result;
}

static napi_value napi_transform_dispose(
    napi_env env, napi_callback_info info) {
    napi_value argv[1] = {0};
    size_t argc = 1;
    napi_transform_handle *handle = NULL;
    if (!napi_transform_check(
            env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL))) return NULL;
    if (argc < 1) {
        napi_throw_type_error(env, NULL, "transformDispose requires a transform handle");
        return NULL;
    }
    handle = napi_transform_unwrap_handle(env, argv[0]);
    if (!handle) return NULL;
    napi_transform_destroy_payload(env, handle);
    return napi_transform_undefined(env);
}

napi_status tranfi_napi_define_transform(napi_env env, napi_value exports) {
    napi_property_descriptor properties[] = {
        {"transformSafeLimits", NULL, napi_transform_safe_limits,
         NULL, NULL, NULL, napi_default, NULL},
        {"transformRecipeFromJson", NULL, napi_transform_recipe_from_json,
         NULL, NULL, NULL, napi_default, NULL},
        {"transformAnalyzerCreate", NULL, napi_transform_analyzer_create,
         NULL, NULL, NULL, napi_default, NULL},
        {"transformAnalyzerPush", NULL, napi_transform_analyzer_push,
         NULL, NULL, NULL, napi_default, NULL},
        {"transformAnalyzerFinalize", NULL, napi_transform_analyzer_finalize,
         NULL, NULL, NULL, napi_default, NULL},
        {"transformPlanExport", NULL, napi_transform_plan_export,
         NULL, NULL, NULL, napi_default, NULL},
        {"transformPlanImport", NULL, napi_transform_plan_import,
         NULL, NULL, NULL, napi_default, NULL},
        {"transformPlanSchemaJson", NULL, napi_transform_plan_schema_json,
         NULL, NULL, NULL, napi_default, NULL},
        {"transformApplyCreate", NULL, napi_transform_apply_create,
         NULL, NULL, NULL, napi_default, NULL},
        {"transformApplyRun", NULL, napi_transform_apply_run,
         NULL, NULL, NULL, napi_default, NULL},
        {"transformDispose", NULL, napi_transform_dispose,
         NULL, NULL, NULL, napi_default, NULL},
    };
    return napi_define_properties(
        env, exports, sizeof(properties) / sizeof(properties[0]), properties);
}
