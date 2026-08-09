#include "transform_internal.h"

#include <float.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

_Static_assert(sizeof(tf_transform_limits_v1) == 184,
               "tf_transform_limits_v1 ABI drift");
#if SIZE_MAX > UINT32_MAX
_Static_assert(sizeof(tf_transform_runtime_v1) == 64,
               "tf_transform_runtime_v1 LP64 ABI drift");
_Static_assert(sizeof(tf_field_view_v1) == 48, "field view LP64 ABI drift");
_Static_assert(sizeof(tf_schema_view_v1) == 32, "schema view LP64 ABI drift");
_Static_assert(sizeof(tf_column_view_v1) == 64, "column view LP64 ABI drift");
_Static_assert(sizeof(tf_table_view_v1) == 40, "table view LP64 ABI drift");
_Static_assert(sizeof(tf_owned_dense_v1) == 48, "dense view LP64 ABI drift");
#else
_Static_assert(sizeof(tf_transform_runtime_v1) == 40,
               "tf_transform_runtime_v1 ILP32 ABI drift");
_Static_assert(sizeof(tf_field_view_v1) == 32, "field view ILP32 ABI drift");
_Static_assert(sizeof(tf_schema_view_v1) == 20, "schema view ILP32 ABI drift");
_Static_assert(sizeof(tf_column_view_v1) == 36, "column view ILP32 ABI drift");
_Static_assert(sizeof(tf_table_view_v1) == 24, "table view ILP32 ABI drift");
_Static_assert(sizeof(tf_owned_dense_v1) == 32, "dense view ILP32 ABI drift");
#endif

static char *duplicate_bytes(const uint8_t *data, size_t len) {
    char *copy;
    if (!data || len == 0 || len == SIZE_MAX) return NULL;
    copy = (char *)malloc(len + 1);
    if (!copy) return NULL;
    memcpy(copy, data, len);
    copy[len] = '\0';
    return copy;
}

static int valid_utf8_common(
    const uint8_t *data, size_t len,
    const tf_transform_runtime_copy *runtime,
    tf_transform_error **error) {
    size_t i = 0;
    size_t since_poll = 0;
    if (runtime && tf_transform_poll_cancel(runtime, error) != TF_TRANSFORM_OK)
        return -1;
    while (i < len) {
        uint8_t c = data[i++];
        uint32_t code;
        size_t needed;
        if (++since_poll >= TF_TRANSFORM_CANCEL_BYTES_V1) {
            if (runtime
                && tf_transform_poll_cancel(runtime, error) != TF_TRANSFORM_OK)
                return -1;
            since_poll = 0;
        }
        if (c <= 0x7f) {
            if (c == 0) return 0;
            continue;
        }
        if (c >= 0xc2 && c <= 0xdf) {
            code = (uint32_t)(c & 0x1f);
            needed = 1;
        } else if (c >= 0xe0 && c <= 0xef) {
            code = (uint32_t)(c & 0x0f);
            needed = 2;
        } else if (c >= 0xf0 && c <= 0xf4) {
            code = (uint32_t)(c & 0x07);
            needed = 3;
        } else {
            return 0;
        }
        if (needed > len - i) return 0;
        for (size_t j = 0; j < needed; ++j) {
            uint8_t next = data[i++];
            if (++since_poll >= TF_TRANSFORM_CANCEL_BYTES_V1) {
                if (runtime
                    && tf_transform_poll_cancel(runtime, error)
                        != TF_TRANSFORM_OK)
                    return -1;
                since_poll = 0;
            }
            if ((next & 0xc0) != 0x80) return 0;
            code = (code << 6) | (uint32_t)(next & 0x3f);
        }
        if ((needed == 1 && code < 0x80)
            || (needed == 2 && code < 0x800)
            || (needed == 3 && code < 0x10000)
            || code > 0x10ffff
            || (code >= 0xd800 && code <= 0xdfff)) return 0;
    }
    if (runtime && tf_transform_poll_cancel(runtime, error) != TF_TRANSFORM_OK)
        return -1;
    return 1;
}

int tf_transform_valid_utf8(const uint8_t *data, size_t len) {
    return valid_utf8_common(data, len, NULL, NULL) == 1;
}

void tf_transform_clear_error(tf_transform_error **error) {
    if (!error) return;
    tf_transform_error_destroy(error);
}

tf_transform_code tf_transform_set_error(
    tf_transform_error **error, tf_transform_code code, const char *message) {
    tf_transform_error *created;
    size_t len;
    if (!error) return code;
    tf_transform_clear_error(error);
    created = (tf_transform_error *)calloc(1, sizeof(*created));
    if (!created) return code;
    created->code = code;
    if (!message) message = "prepared-transform error";
    len = strlen(message);
    created->message = duplicate_bytes((const uint8_t *)message, len);
    if (!created->message) {
        free(created);
        return code;
    }
    created->message_len = len;
    *error = created;
    return code;
}

void tf_transform_error_destroy(tf_transform_error **error) {
    if (!error || !*error) return;
    free((*error)->message);
    free(*error);
    *error = NULL;
}

tf_transform_code tf_transform_error_get_code(const tf_transform_error *error) {
    return error ? error->code : TF_TRANSFORM_INVALID_ARGUMENT;
}

const uint8_t *tf_transform_error_message(
    const tf_transform_error *error, size_t *len) {
    if (len) *len = error ? error->message_len : 0;
    return error ? (const uint8_t *)error->message : NULL;
}

tf_transform_code tf_transform_limits_init_safe_v1(
    tf_transform_limits_v1 *out, size_t out_size) {
    tf_transform_limits_v1 value;
    if (!out || out_size < sizeof(value)) return TF_TRANSFORM_INVALID_ARGUMENT;
    memset(&value, 0, sizeof(value));
    value.abi_version = 1;
    value.struct_size = (uint32_t)sizeof(value);
    value.max_recipe_bytes = UINT64_C(1048576);
    value.max_plan_bytes = UINT64_C(67108864);
    value.max_json_depth = UINT64_C(64);
    value.max_object_keys = UINT64_C(65536);
    value.max_steps = UINT64_C(256);
    value.max_input_columns = UINT64_C(65536);
    value.max_output_columns = UINT64_C(65536);
    value.max_categories_per_column = UINT64_C(65536);
    value.max_total_categories = UINT64_C(1048576);
    value.max_string_bytes = UINT64_C(1048576);
    value.max_decoded_string_bytes = UINT64_C(67108864);
    value.max_analyzer_rows = UINT64_C(4294967295);
    value.max_analyzer_input_bytes = UINT64_C(68719476736);
    value.max_resident_state_bytes = UINT64_C(536870912);
    value.max_spill_bytes = UINT64_C(68719476736);
    value.max_apply_rows = UINT64_C(4294967295);
    value.max_apply_input_bytes = UINT64_C(68719476736);
    value.max_output_elements_per_call = UINT64_C(134217728);
    value.max_allocation_bytes = UINT64_C(1073741824);
    value.max_allocations_per_session = UINT64_C(1000000);
    value.max_live_handles = UINT64_C(65535);
    value.max_retired_handle_slots = UINT64_C(65535);
    memcpy(out, &value, sizeof(value));
    return TF_TRANSFORM_OK;
}

static int limits_all_nonzero(const tf_transform_limits_v1 *value) {
    uint64_t fields[22];
    memcpy(fields, (const uint8_t *)value + 8, sizeof(fields));
    for (size_t i = 0; i < sizeof(fields) / sizeof(fields[0]); ++i) {
        if (fields[i] == 0) return 0;
    }
    return 1;
}

tf_transform_code tf_transform_copy_limits(
    const tf_transform_limits_v1 *source, tf_transform_limits_v1 *out,
    tf_transform_error **error) {
    if (!out) return tf_transform_set_error(
        error, TF_TRANSFORM_INVALID_ARGUMENT, "limits output is null");
    if (!source) {
        if (tf_transform_limits_init_safe_v1(out, sizeof(*out)) != TF_TRANSFORM_OK)
            return tf_transform_set_error(
                error, TF_TRANSFORM_INTERNAL, "safe limits initialization failed");
        return TF_TRANSFORM_OK;
    }
    if (source->abi_version != 1 || source->struct_size != sizeof(*source)
        || !limits_all_nonzero(source))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "invalid transform limits V1");
    memcpy(out, source, sizeof(*out));
    return TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_copy_runtime(
    const tf_transform_runtime_v1 *source, tf_transform_runtime_copy *out,
    tf_transform_error **error) {
    tf_transform_code code;
    if (!out) return tf_transform_set_error(
        error, TF_TRANSFORM_INVALID_ARGUMENT, "runtime output is null");
    memset(out, 0, sizeof(*out));
    if (!source) return tf_transform_copy_limits(NULL, &out->limits, error);
    if (source->abi_version != 1 || source->struct_size != sizeof(*source)
        || source->flags != 0 || source->reserved != 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "invalid transform runtime V1");
    if ((source->spill_dir_bytes != 0 && !source->spill_dir_utf8)
        || (source->spill_dir_bytes == 0 && source->spill_dir_utf8))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "invalid spill directory span");
    code = tf_transform_copy_limits(source->limits, &out->limits, error);
    if (code != TF_TRANSFORM_OK) return code;
    if (source->spill_dir_bytes > out->limits.max_string_bytes
        || (source->spill_dir_bytes != 0
            && !tf_transform_valid_utf8(
                source->spill_dir_utf8, source->spill_dir_bytes)))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "spill directory is not bounded valid UTF-8");
    out->cancel = source->cancel;
    out->cancel_user = source->cancel_user;
    return TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_poll_cancel(
    const tf_transform_runtime_copy *runtime, tf_transform_error **error) {
    if (runtime && runtime->cancel && runtime->cancel(runtime->cancel_user))
        return tf_transform_set_error(
            error, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
    return TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_copy_bytes_runtime(
    void *destination, const void *source, size_t len,
    const tf_transform_runtime_copy *runtime,
    tf_transform_error **error) {
    size_t offset = 0;
    if (len != 0 && (!destination || !source))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "runtime byte copy span is null");
    while (offset < len) {
        size_t chunk = len - offset;
        tf_transform_code code;
        if (chunk > TF_TRANSFORM_CANCEL_BYTES_V1)
            chunk = TF_TRANSFORM_CANCEL_BYTES_V1;
        code = tf_transform_poll_cancel(runtime, error);
        if (code != TF_TRANSFORM_OK) return code;
        memcpy((uint8_t *)destination + offset,
               (const uint8_t *)source + offset, chunk);
        offset += chunk;
    }
    return tf_transform_poll_cancel(runtime, error);
}

tf_transform_code tf_transform_compare_bytes_runtime(
    const void *left, size_t left_len,
    const void *right, size_t right_len,
    const tf_transform_runtime_copy *runtime,
    int *comparison, tf_transform_error **error) {
    size_t common;
    size_t offset = 0;
    if (!comparison || (left_len != 0 && !left)
        || (right_len != 0 && !right))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "runtime byte comparison span is null");
    common = left_len < right_len ? left_len : right_len;
    while (offset < common) {
        size_t chunk = common - offset;
        int compared;
        tf_transform_code code;
        if (chunk > TF_TRANSFORM_CANCEL_BYTES_V1)
            chunk = TF_TRANSFORM_CANCEL_BYTES_V1;
        code = tf_transform_poll_cancel(runtime, error);
        if (code != TF_TRANSFORM_OK) return code;
        compared = memcmp(
            (const uint8_t *)left + offset,
            (const uint8_t *)right + offset, chunk);
        if (compared != 0) {
            *comparison = compared < 0 ? -1 : 1;
            return TF_TRANSFORM_OK;
        }
        offset += chunk;
    }
    *comparison = left_len < right_len ? -1 : (left_len > right_len ? 1 : 0);
    return tf_transform_poll_cancel(runtime, error);
}

void tf_transform_resource_ledger_init(
    tf_transform_resource_ledger *ledger,
    const tf_transform_runtime_copy *runtime) {
    if (!ledger) return;
    memset(ledger, 0, sizeof(*ledger));
    ledger->runtime = runtime;
    ledger->last_code = TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_resource_reserve(
    tf_transform_resource_ledger *ledger, uint64_t bytes,
    tf_transform_error **error) {
    const tf_transform_limits_v1 *limits;
    tf_transform_code code;
    uint64_t resident;
    if (!ledger || !ledger->runtime) {
        tf_transform_code invalid = tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "resource ledger is not initialized");
        if (ledger) ledger->last_code = invalid;
        return invalid;
    }
    limits = &ledger->runtime->limits;
    if (bytes > limits->max_allocation_bytes
        || ledger->allocation_count == UINT64_MAX
        || ledger->allocation_count + 1 > limits->max_allocations_per_session
        || bytes > UINT64_MAX - ledger->resident_bytes
        || ledger->resident_bytes + bytes > limits->max_resident_state_bytes)
    {
        ledger->last_code = tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "prepared-transform allocation exceeds resource limits");
        return ledger->last_code;
    }
    code = tf_transform_poll_cancel(ledger->runtime, error);
    if (code != TF_TRANSFORM_OK) {
        ledger->last_code = code;
        return code;
    }
    resident = ledger->resident_bytes + bytes;
    ++ledger->allocation_count;
    ledger->resident_bytes = resident;
    if (resident > ledger->peak_resident_bytes)
        ledger->peak_resident_bytes = resident;
    ledger->last_code = TF_TRANSFORM_OK;
    return TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_resource_charge_batch(
    tf_transform_resource_ledger *ledger, uint64_t allocations,
    uint64_t resident_bytes, uint64_t peak_bytes,
    tf_transform_error **error) {
    const tf_transform_limits_v1 *limits;
    tf_transform_code code;
    uint64_t peak;
    if (!ledger || !ledger->runtime || peak_bytes < resident_bytes) {
        tf_transform_code invalid = tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "resource batch metric is invalid");
        if (ledger) ledger->last_code = invalid;
        return invalid;
    }
    limits = &ledger->runtime->limits;
    if (allocations > UINT64_MAX - ledger->allocation_count
        || ledger->allocation_count + allocations
            > limits->max_allocations_per_session
        || resident_bytes > UINT64_MAX - ledger->resident_bytes
        || peak_bytes > UINT64_MAX - ledger->resident_bytes
        || ledger->resident_bytes + peak_bytes
            > limits->max_resident_state_bytes)
    {
        ledger->last_code = tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "prepared-transform allocation batch exceeds resource limits");
        return ledger->last_code;
    }
    code = tf_transform_poll_cancel(ledger->runtime, error);
    if (code != TF_TRANSFORM_OK) {
        ledger->last_code = code;
        return code;
    }
    peak = ledger->resident_bytes + peak_bytes;
    ledger->allocation_count += allocations;
    ledger->resident_bytes += resident_bytes;
    if (peak > ledger->peak_resident_bytes)
        ledger->peak_resident_bytes = peak;
    ledger->last_code = TF_TRANSFORM_OK;
    return TF_TRANSFORM_OK;
}

void tf_transform_resource_release(
    tf_transform_resource_ledger *ledger, uint64_t bytes) {
    if (!ledger) return;
    if (bytes <= ledger->resident_bytes) ledger->resident_bytes -= bytes;
}

void *tf_transform_resource_malloc(
    tf_transform_resource_ledger *ledger, size_t bytes,
    tf_transform_error **error) {
    void *result;
    tf_transform_code code = tf_transform_resource_reserve(
        ledger, (uint64_t)bytes, error);
    if (code != TF_TRANSFORM_OK) return NULL;
    result = malloc(bytes);
    if (!result) {
        tf_transform_resource_release(ledger, (uint64_t)bytes);
        ledger->last_code = tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION,
            "prepared-transform allocation failed");
    }
    return result;
}

void *tf_transform_resource_calloc(
    tf_transform_resource_ledger *ledger, size_t count, size_t size,
    tf_transform_error **error) {
    void *result;
    size_t bytes;
    if (size != 0 && count > SIZE_MAX / size) {
        if (ledger) ledger->last_code = tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "prepared-transform allocation size overflows");
        return NULL;
    }
    bytes = count * size;
    result = tf_transform_resource_malloc(ledger, bytes, error);
    if (result) memset(result, 0, bytes);
    return result;
}

void tf_transform_recipe_retain(tf_transform_recipe *recipe) {
    if (recipe) (void)atomic_fetch_add_explicit(
        &recipe->refcount, 1u, memory_order_relaxed);
}

void tf_transform_recipe_release(tf_transform_recipe *recipe) {
    if (!recipe) return;
    if (atomic_fetch_sub_explicit(&recipe->refcount, 1u, memory_order_acq_rel) != 1u)
        return;
    if (recipe->columns) {
        for (size_t i = 0; i < recipe->column_count; ++i)
            free(recipe->columns[i].source_id);
    }
    free(recipe->columns);
    free(recipe);
}

void tf_transform_recipe_destroy(tf_transform_recipe **recipe) {
    if (!recipe || !*recipe) return;
    tf_transform_recipe_release(*recipe);
    *recipe = NULL;
}

void tf_transform_schema_clear(tf_transform_schema *schema) {
    if (!schema) return;
    for (size_t i = 0; i < schema->field_count; ++i) {
        free(schema->fields[i].id);
        free(schema->fields[i].name);
    }
    free(schema->fields);
    memset(schema, 0, sizeof(*schema));
}

static int span_equal(const char *left, size_t left_len,
                      const uint8_t *right, size_t right_len) {
    return left_len == right_len && memcmp(left, right, left_len) == 0;
}

static tf_transform_code poll_runtime_iteration(
    const tf_transform_runtime_copy *runtime, size_t iteration,
    tf_transform_error **error) {
    if (!runtime || iteration % TF_TRANSFORM_CANCEL_ITERS_V1 != 0)
        return TF_TRANSFORM_OK;
    return tf_transform_poll_cancel(runtime, error);
}

static tf_transform_code schema_field_compare_runtime(
    const tf_transform_schema_field_owned *left,
    const tf_transform_schema_field_owned *right, int by_name,
    const tf_transform_runtime_copy *runtime, int *comparison,
    tf_transform_error **error) {
    const char *a = by_name ? left->name : left->id;
    const char *b = by_name ? right->name : right->id;
    size_t a_len = by_name ? left->name_len : left->id_len;
    size_t b_len = by_name ? right->name_len : right->id_len;
    return tf_transform_compare_bytes_runtime(
        a, a_len, b, b_len, runtime, comparison, error);
}

static tf_transform_code sort_schema_fields(
    tf_transform_schema_field_owned **items, size_t count, int by_name,
    const tf_transform_runtime_copy *runtime,
    tf_transform_error **error) {
    size_t start = count / 2;
    size_t end = count;
    size_t iterations = 0;
    if (runtime) {
        tf_transform_code code = tf_transform_poll_cancel(runtime, error);
        if (code != TF_TRANSFORM_OK) return code;
    }
    while (count > 1) {
        size_t root;
        tf_transform_schema_field_owned *saved;
        tf_transform_code code;
        if (start != 0) {
            --start;
            root = start;
            saved = items[root];
        } else {
            --end;
            if (end == 0) break;
            saved = items[end];
            items[end] = items[0];
            root = 0;
        }
        while (root <= (end - 1) / 2 && end > 1) {
            size_t child = root * 2 + 1;
            int comparison;
            if (child + 1 < end) {
                code = schema_field_compare_runtime(
                    items[child], items[child + 1], by_name,
                    runtime, &comparison, error);
                if (code != TF_TRANSFORM_OK) return code;
                if (comparison < 0) ++child;
            }
            code = poll_runtime_iteration(runtime, ++iterations, error);
            if (code != TF_TRANSFORM_OK) return code;
            code = schema_field_compare_runtime(
                saved, items[child], by_name,
                runtime, &comparison, error);
            if (code != TF_TRANSFORM_OK) return code;
            if (comparison >= 0) break;
            items[root] = items[child];
            root = child;
        }
        items[root] = saved;
    }
    return runtime ? tf_transform_poll_cancel(runtime, error) : TF_TRANSFORM_OK;
}

static tf_transform_code schema_validate_unique(
    tf_transform_schema *schema, const tf_transform_limits_v1 *limits,
    const tf_transform_runtime_copy *runtime,
    tf_transform_resource_ledger *ledger,
    tf_transform_error **error) {
    tf_transform_schema_field_owned **items;
    size_t bytes;
    tf_transform_code code;
    if (schema->field_count > SIZE_MAX / sizeof(*items))
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "schema uniqueness index overflows");
    bytes = schema->field_count * sizeof(*items);
    if ((uint64_t)bytes > limits->max_allocation_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "schema uniqueness index exceeds limits");
    if (runtime) {
        code = tf_transform_poll_cancel(runtime, error);
        if (code != TF_TRANSFORM_OK) return code;
    }
    items = ledger
        ? (tf_transform_schema_field_owned **)tf_transform_resource_malloc(
            ledger, bytes, error)
        : (tf_transform_schema_field_owned **)malloc(bytes);
    if (!items) return ledger && ledger->last_code != TF_TRANSFORM_OK
        ? ledger->last_code : tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION,
            "schema uniqueness index allocation failed");
    for (size_t i = 0; i < schema->field_count; ++i) items[i] = &schema->fields[i];
    code = sort_schema_fields(items, schema->field_count, 0, runtime, error);
    if (code == TF_TRANSFORM_OK) {
        for (size_t i = 1; i < schema->field_count; ++i) {
            int comparison;
            code = poll_runtime_iteration(runtime, i, error);
            if (code != TF_TRANSFORM_OK) break;
            code = schema_field_compare_runtime(
                items[i - 1], items[i], 0,
                runtime, &comparison, error);
            if (code != TF_TRANSFORM_OK) break;
            if (comparison == 0) {
                code = tf_transform_set_error(
                    error, TF_TRANSFORM_INVALID_ARGUMENT,
                    "schema IDs must be unique");
                break;
            }
        }
    }
    if (code == TF_TRANSFORM_OK) {
        code = sort_schema_fields(items, schema->field_count, 1, runtime, error);
    }
    if (code == TF_TRANSFORM_OK) {
        for (size_t i = 1; i < schema->field_count; ++i) {
            int comparison;
            code = poll_runtime_iteration(runtime, i, error);
            if (code != TF_TRANSFORM_OK) break;
            code = schema_field_compare_runtime(
                items[i - 1], items[i], 1,
                runtime, &comparison, error);
            if (code != TF_TRANSFORM_OK) break;
            if (comparison == 0) {
                code = tf_transform_set_error(
                    error, TF_TRANSFORM_INVALID_ARGUMENT,
                    "schema names must be unique");
                break;
            }
        }
    }
    free(items);
    if (ledger) tf_transform_resource_release(ledger, (uint64_t)bytes);
    return code;
}

static tf_transform_code schema_copy_common(
    const tf_schema_view_v1 *source, const tf_transform_limits_v1 *limits,
    const tf_transform_runtime_copy *runtime,
    tf_transform_resource_ledger *ledger,
    tf_transform_schema *out, tf_transform_error **error) {
    size_t fields_bytes;
    uint64_t resident_bytes;
    uint64_t allocation_count;
    tf_transform_schema result;
    if (!source || !limits || !out)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "schema argument is null");
    memset(&result, 0, sizeof(result));
    if (source->abi_version != 1 || source->struct_size != sizeof(*source)
        || source->column_count == 0
        || source->column_count > limits->max_input_columns
        || source->column_count > SIZE_MAX / sizeof(tf_field_view_v1))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "invalid schema V1 header");
    fields_bytes = source->column_count * sizeof(tf_field_view_v1);
    if (!source->fields || source->fields_bytes != fields_bytes
        || fields_bytes > limits->max_allocation_bytes
        || fields_bytes > limits->max_resident_state_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "schema fields exceed limits");
    resident_bytes = (uint64_t)source->column_count
        * sizeof(tf_transform_schema_field_owned);
    allocation_count = 2 + (uint64_t)source->column_count * 2;
    if ((uint64_t)source->column_count * sizeof(void *)
        > UINT64_MAX - resident_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "schema ownership byte count overflows");
    resident_bytes += (uint64_t)source->column_count * sizeof(void *);
    if (resident_bytes > limits->max_resident_state_bytes
        || resident_bytes > limits->max_allocation_bytes
        || allocation_count > limits->max_allocations_per_session)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "schema ownership exceeds allocation limits");
    if (runtime) {
        tf_transform_code code = tf_transform_poll_cancel(runtime, error);
        if (code != TF_TRANSFORM_OK) return code;
    }
    result.fields = ledger
        ? (tf_transform_schema_field_owned *)tf_transform_resource_calloc(
            ledger, source->column_count, sizeof(*result.fields), error)
        : (tf_transform_schema_field_owned *)calloc(
            source->column_count, sizeof(*result.fields));
    if (!result.fields)
        return ledger && ledger->last_code != TF_TRANSFORM_OK
            ? ledger->last_code : tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION, "schema allocation failed");
    result.field_count = source->column_count;
    for (size_t i = 0; i < source->column_count; ++i) {
        const tf_field_view_v1 *field = &source->fields[i];
        tf_transform_code code = poll_runtime_iteration(runtime, i, error);
        int id_valid;
        int name_valid;
        if (code != TF_TRANSFORM_OK) {
            tf_transform_schema_clear(&result);
            return code;
        }
        if (field->abi_version != 1 || field->struct_size != sizeof(*field)
            || field->flags != 0
            || (field->dtype != TF_VIEW_FLOAT32 && field->dtype != TF_VIEW_FLOAT64)
            || !field->id_utf8 || field->id_bytes == 0
            || !field->name_utf8 || field->name_bytes == 0
            || field->id_bytes > limits->max_string_bytes
            || field->name_bytes > limits->max_string_bytes) {
            tf_transform_schema_clear(&result);
            return tf_transform_set_error(
                error, TF_TRANSFORM_INVALID_ARGUMENT,
                "invalid schema field V1");
        }
        id_valid = field->id_utf8
            ? valid_utf8_common(
                field->id_utf8, field->id_bytes, runtime, error) : 0;
        if (id_valid < 0) {
            tf_transform_schema_clear(&result);
            return TF_TRANSFORM_CANCELLED;
        }
        name_valid = field->name_utf8
            ? valid_utf8_common(
                field->name_utf8, field->name_bytes, runtime, error) : 0;
        if (name_valid < 0) {
            tf_transform_schema_clear(&result);
            return TF_TRANSFORM_CANCELLED;
        }
        if (!id_valid || !name_valid) {
            tf_transform_schema_clear(&result);
            return tf_transform_set_error(
                error, TF_TRANSFORM_INVALID_ARGUMENT, "invalid schema field V1");
        }
        if ((uint64_t)field->id_bytes + 1 > UINT64_MAX - resident_bytes
            || (uint64_t)field->name_bytes + 1
                > UINT64_MAX - resident_bytes
                    - ((uint64_t)field->id_bytes + 1)) {
            tf_transform_schema_clear(&result);
            return tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "schema string byte count overflows");
        }
        resident_bytes += (uint64_t)field->id_bytes + 1;
        resident_bytes += (uint64_t)field->name_bytes + 1;
        if (resident_bytes > limits->max_resident_state_bytes
            || resident_bytes > limits->max_decoded_string_bytes) {
            tf_transform_schema_clear(&result);
            return tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "schema strings exceed resource limits");
        }
        result.fields[i].dtype = field->dtype;
        if (runtime) {
            code = tf_transform_poll_cancel(runtime, error);
            if (code != TF_TRANSFORM_OK) {
                tf_transform_schema_clear(&result);
                return code;
            }
        }
        result.fields[i].id = ledger
            ? (char *)tf_transform_resource_malloc(
                ledger, field->id_bytes + 1, error)
            : (char *)malloc(field->id_bytes + 1);
        result.fields[i].name = ledger
            ? (char *)tf_transform_resource_malloc(
                ledger, field->name_bytes + 1, error)
            : (char *)malloc(field->name_bytes + 1);
        if (!result.fields[i].id || !result.fields[i].name) {
            tf_transform_schema_clear(&result);
            return ledger && ledger->last_code != TF_TRANSFORM_OK
                ? ledger->last_code : tf_transform_set_error(
                error, TF_TRANSFORM_ALLOCATION,
                "schema string allocation failed");
        }
        code = tf_transform_copy_bytes_runtime(
            result.fields[i].id, field->id_utf8, field->id_bytes,
            runtime, error);
        if (code != TF_TRANSFORM_OK) {
            tf_transform_schema_clear(&result);
            return code;
        }
        result.fields[i].id[field->id_bytes] = '\0';
        code = tf_transform_copy_bytes_runtime(
            result.fields[i].name, field->name_utf8, field->name_bytes,
            runtime, error);
        if (code != TF_TRANSFORM_OK) {
            tf_transform_schema_clear(&result);
            return code;
        }
        result.fields[i].name[field->name_bytes] = '\0';
        result.fields[i].id_len = field->id_bytes;
        result.fields[i].name_len = field->name_bytes;
    }
    {
        tf_transform_code code = schema_validate_unique(
            &result, limits, runtime, ledger, error);
        if (code != TF_TRANSFORM_OK) {
            tf_transform_schema_clear(&result);
            return code;
        }
    }
    tf_transform_schema_clear(out);
    *out = result;
    return TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_schema_copy(
    const tf_schema_view_v1 *source, const tf_transform_limits_v1 *limits,
    tf_transform_schema *out, tf_transform_error **error) {
    return schema_copy_common(source, limits, NULL, NULL, out, error);
}

tf_transform_code tf_transform_schema_copy_runtime(
    const tf_schema_view_v1 *source,
    const tf_transform_runtime_copy *runtime,
    tf_transform_schema *out, tf_transform_error **error) {
    if (!runtime) return tf_transform_set_error(
        error, TF_TRANSFORM_INVALID_ARGUMENT, "schema runtime is null");
    return schema_copy_common(
        source, &runtime->limits, runtime, NULL, out, error);
}

tf_transform_code tf_transform_schema_copy_runtime_ledger(
    const tf_schema_view_v1 *source,
    const tf_transform_runtime_copy *runtime,
    tf_transform_resource_ledger *ledger,
    tf_transform_schema *out, tf_transform_error **error) {
    if (!runtime || !ledger || ledger->runtime != runtime)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "schema resource ledger is invalid");
    return schema_copy_common(
        source, &runtime->limits, runtime, ledger, out, error);
}

static tf_transform_code schema_clone_common(
    const tf_transform_schema *source, tf_transform_schema *out,
    const tf_transform_runtime_copy *runtime,
    tf_transform_error **error) {
    tf_transform_schema result;
    if (!source || !out) return tf_transform_set_error(
        error, TF_TRANSFORM_INVALID_ARGUMENT, "schema clone argument is null");
    memset(&result, 0, sizeof(result));
    if (runtime) {
        tf_transform_code code = tf_transform_poll_cancel(runtime, error);
        if (code != TF_TRANSFORM_OK) return code;
    }
    result.fields = (tf_transform_schema_field_owned *)calloc(
        source->field_count, sizeof(*result.fields));
    if (!result.fields) return tf_transform_set_error(
        error, TF_TRANSFORM_ALLOCATION, "schema clone allocation failed");
    result.field_count = source->field_count;
    result.is_output = source->is_output;
    for (size_t i = 0; i < source->field_count; ++i) {
        tf_transform_code code = poll_runtime_iteration(runtime, i, error);
        if (code != TF_TRANSFORM_OK) {
            tf_transform_schema_clear(&result);
            return code;
        }
        result.fields[i].dtype = source->fields[i].dtype;
        result.fields[i].id_len = source->fields[i].id_len;
        result.fields[i].name_len = source->fields[i].name_len;
        if (runtime) {
            code = tf_transform_poll_cancel(runtime, error);
            if (code != TF_TRANSFORM_OK) {
                tf_transform_schema_clear(&result);
                return code;
            }
        }
        result.fields[i].id = (char *)malloc(source->fields[i].id_len + 1);
        result.fields[i].name = (char *)malloc(source->fields[i].name_len + 1);
        if (!result.fields[i].id || !result.fields[i].name) {
            tf_transform_schema_clear(&result);
            return tf_transform_set_error(
                error, TF_TRANSFORM_ALLOCATION, "schema clone string failed");
        }
        code = tf_transform_copy_bytes_runtime(
            result.fields[i].id, source->fields[i].id,
            source->fields[i].id_len, runtime, error);
        if (code != TF_TRANSFORM_OK) {
            tf_transform_schema_clear(&result);
            return code;
        }
        result.fields[i].id[source->fields[i].id_len] = '\0';
        code = tf_transform_copy_bytes_runtime(
            result.fields[i].name, source->fields[i].name,
            source->fields[i].name_len, runtime, error);
        if (code != TF_TRANSFORM_OK) {
            tf_transform_schema_clear(&result);
            return code;
        }
        result.fields[i].name[source->fields[i].name_len] = '\0';
    }
    tf_transform_schema_clear(out);
    *out = result;
    return TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_schema_clone(
    const tf_transform_schema *source, tf_transform_schema *out,
    tf_transform_error **error) {
    return schema_clone_common(source, out, NULL, error);
}

tf_transform_code tf_transform_schema_clone_runtime(
    const tf_transform_schema *source,
    const tf_transform_runtime_copy *runtime,
    tf_transform_schema *out, tf_transform_error **error) {
    if (!runtime) return tf_transform_set_error(
        error, TF_TRANSFORM_INVALID_ARGUMENT, "schema runtime is null");
    return schema_clone_common(source, out, runtime, error);
}

int tf_transform_schema_equal_view(
    const tf_transform_schema *schema, const tf_schema_view_v1 *view) {
    if (!schema || !view || view->abi_version != 1
        || view->struct_size != sizeof(*view)
        || view->column_count != schema->field_count || !view->fields
        || view->column_count > SIZE_MAX / sizeof(tf_field_view_v1)
        || view->fields_bytes != view->column_count * sizeof(tf_field_view_v1))
        return 0;
    for (size_t i = 0; i < schema->field_count; ++i) {
        const tf_field_view_v1 *field = &view->fields[i];
        if (field->abi_version != 1 || field->struct_size != sizeof(*field)
            || field->flags != 0 || field->dtype != schema->fields[i].dtype
            || !field->id_utf8 || !field->name_utf8
            || !span_equal(schema->fields[i].id, schema->fields[i].id_len,
                           field->id_utf8, field->id_bytes)
            || !span_equal(schema->fields[i].name, schema->fields[i].name_len,
                           field->name_utf8, field->name_bytes)) return 0;
    }
    return 1;
}

tf_transform_code tf_transform_schema_equal_view_runtime(
    const tf_transform_schema *schema, const tf_schema_view_v1 *view,
    const tf_transform_runtime_copy *runtime, int *equal,
    tf_transform_error **error) {
    if (equal) *equal = 0;
    if (!runtime || !equal)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "schema comparison runtime or output is null");
    {
        tf_transform_code code = tf_transform_poll_cancel(runtime, error);
        if (code != TF_TRANSFORM_OK) return code;
    }
    if (!schema || !view || view->abi_version != 1
        || view->struct_size != sizeof(*view)
        || view->column_count != schema->field_count || !view->fields
        || view->column_count > SIZE_MAX / sizeof(tf_field_view_v1)
        || view->fields_bytes != view->column_count * sizeof(tf_field_view_v1))
        return TF_TRANSFORM_OK;
    for (size_t i = 0; i < schema->field_count; ++i) {
        const tf_field_view_v1 *field = &view->fields[i];
        int comparison;
        tf_transform_code code = poll_runtime_iteration(runtime, i, error);
        if (code != TF_TRANSFORM_OK) return code;
        if (field->abi_version != 1 || field->struct_size != sizeof(*field)
            || field->flags != 0 || field->dtype != schema->fields[i].dtype
            || !field->id_utf8 || !field->name_utf8)
            return TF_TRANSFORM_OK;
        code = tf_transform_compare_bytes_runtime(
            schema->fields[i].id, schema->fields[i].id_len,
            field->id_utf8, field->id_bytes,
            runtime, &comparison, error);
        if (code != TF_TRANSFORM_OK) return code;
        if (comparison != 0) return TF_TRANSFORM_OK;
        code = tf_transform_compare_bytes_runtime(
            schema->fields[i].name, schema->fields[i].name_len,
            field->name_utf8, field->name_bytes,
            runtime, &comparison, error);
        if (code != TF_TRANSFORM_OK) return code;
        if (comparison != 0) return TF_TRANSFORM_OK;
    }
    *equal = 1;
    return tf_transform_poll_cancel(runtime, error);
}

void tf_transform_plan_retain(tf_transform_plan *plan) {
    if (plan) (void)atomic_fetch_add_explicit(
        &plan->refcount, 1u, memory_order_relaxed);
}

void tf_transform_plan_release(tf_transform_plan *plan) {
    if (!plan) return;
    if (atomic_fetch_sub_explicit(&plan->refcount, 1u, memory_order_acq_rel) != 1u)
        return;
    tf_transform_recipe_release(plan->recipe);
    tf_transform_schema_clear(&plan->input_schema);
    tf_transform_schema_clear(&plan->output_schema);
    free(plan->states);
    free(plan);
}

void tf_transform_plan_destroy(tf_transform_plan **plan) {
    if (!plan || !*plan) return;
    tf_transform_plan_release(*plan);
    *plan = NULL;
}

void tf_transform_bytes_free(uint8_t **bytes, size_t *len) {
    if (bytes) {
        free(*bytes);
        *bytes = NULL;
    }
    if (len) *len = 0;
}

void tf_owned_dense_free(tf_owned_dense_v1 *dense) {
    if (!dense) return;
    free(dense->data);
    memset(dense, 0, sizeof(*dense));
}

int tf_transform_double_is_nan(double value) {
    uint64_t bits = tf_transform_double_bits(value);
    return (bits & UINT64_C(0x7ff0000000000000))
        == UINT64_C(0x7ff0000000000000)
        && (bits & UINT64_C(0x000fffffffffffff)) != 0;
}

int tf_transform_double_is_finite(double value) {
    return (tf_transform_double_bits(value) & UINT64_C(0x7ff0000000000000))
        != UINT64_C(0x7ff0000000000000);
}

uint64_t tf_transform_double_bits(double value) {
    uint64_t bits;
    memcpy(&bits, &value, sizeof(bits));
    return bits;
}

double tf_transform_double_from_bits(uint64_t bits) {
    double value;
    memcpy(&value, &bits, sizeof(value));
    return value;
}

tf_transform_code tf_transform_plan_schema(
    const tf_transform_plan *plan, uint32_t which,
    const tf_transform_schema **out, tf_transform_error **error) {
    if (out) *out = NULL;
    tf_transform_clear_error(error);
    if (!plan || !out)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "plan schema argument is null");
    if (which == TF_TRANSFORM_SCHEMA_INPUT)
        *out = &plan->input_schema;
    else if (which == TF_TRANSFORM_SCHEMA_OUTPUT)
        *out = &plan->output_schema;
    else return tf_transform_set_error(
        error, TF_TRANSFORM_INVALID_ARGUMENT, "unknown plan schema selector");
    return TF_TRANSFORM_OK;
}

size_t tf_transform_schema_field_count(const tf_transform_schema *schema) {
    return schema ? schema->field_count : 0;
}

tf_transform_code tf_transform_schema_field(
    const tf_transform_schema *schema, size_t index,
    tf_field_view_v1 *out, tf_transform_error **error) {
    tf_field_view_v1 field;
    if (out) memset(out, 0, sizeof(*out));
    tf_transform_clear_error(error);
    if (!schema || !out || index >= schema->field_count)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "schema field index is invalid");
    memset(&field, 0, sizeof(field));
    field.abi_version = 1;
    field.struct_size = (uint32_t)sizeof(field);
    field.dtype = schema->fields[index].dtype;
    field.id_utf8 = (const uint8_t *)schema->fields[index].id;
    field.id_bytes = schema->fields[index].id_len;
    field.name_utf8 = (const uint8_t *)schema->fields[index].name;
    field.name_bytes = schema->fields[index].name_len;
    memcpy(out, &field, sizeof(field));
    return TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_plan_schema_json(
    const tf_transform_plan *plan, uint32_t which,
    const tf_transform_limits_v1 *limits,
    uint8_t **out, size_t *out_len, tf_transform_error **error) {
    tf_transform_limits_v1 copied;
    const tf_transform_schema *schema;
    cJSON *json = NULL;
    tf_transform_code code;
    if (out) *out = NULL;
    if (out_len) *out_len = 0;
    tf_transform_clear_error(error);
    if (!plan || !out || !out_len)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "plan schema JSON argument is null");
    code = tf_transform_copy_limits(limits, &copied, error);
    if (code != TF_TRANSFORM_OK) return code;
    if (which == TF_TRANSFORM_SCHEMA_INPUT) schema = &plan->input_schema;
    else if (which == TF_TRANSFORM_SCHEMA_OUTPUT) schema = &plan->output_schema;
    else return tf_transform_set_error(
        error, TF_TRANSFORM_INVALID_ARGUMENT, "unknown plan schema selector");
    json = tf_transform_schema_to_json(schema);
    if (!json) return tf_transform_set_error(
        error, TF_TRANSFORM_ALLOCATION, "schema JSON allocation failed");
    code = tf_transform_json_print_canonical(
        json, &copied, out, out_len, error);
    cJSON_Delete(json);
    return code;
}

static void write_u16_le(uint8_t *out, uint16_t value) {
    out[0] = (uint8_t)value;
    out[1] = (uint8_t)(value >> 8);
}

static void write_u32_le(uint8_t *out, uint32_t value) {
    for (size_t i = 0; i < 4; ++i) out[i] = (uint8_t)(value >> (i * 8));
}

static void write_u64_le(uint8_t *out, uint64_t value) {
    for (size_t i = 0; i < 8; ++i) out[i] = (uint8_t)(value >> (i * 8));
}

static uint16_t read_u16_le(const uint8_t *in) {
    return (uint16_t)in[0] | ((uint16_t)in[1] << 8);
}

static uint32_t read_u32_le(const uint8_t *in) {
    uint32_t value = 0;
    for (size_t i = 0; i < 4; ++i) value |= (uint32_t)in[i] << (i * 8);
    return value;
}

static uint64_t read_u64_le(const uint8_t *in) {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; ++i) value |= (uint64_t)in[i] << (i * 8);
    return value;
}

static tf_transform_code runtime_bytes_equal(
    const uint8_t *left, const uint8_t *right, size_t len,
    const tf_transform_runtime_copy *runtime, int *equal,
    tf_transform_error **error) {
    size_t offset = 0;
    if (!left || !right || !runtime || !equal)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "runtime byte comparison argument is null");
    while (offset < len) {
        size_t chunk = len - offset;
        tf_transform_code code;
        if (chunk > TF_TRANSFORM_CANCEL_BYTES_V1)
            chunk = TF_TRANSFORM_CANCEL_BYTES_V1;
        code = tf_transform_poll_cancel(runtime, error);
        if (code != TF_TRANSFORM_OK) return code;
        if (memcmp(left + offset, right + offset, chunk) != 0) {
            *equal = 0;
            return TF_TRANSFORM_OK;
        }
        offset += chunk;
    }
    *equal = 1;
    return tf_transform_poll_cancel(runtime, error);
}

tf_transform_code tf_transform_plan_export(
    const tf_transform_plan *plan, const tf_transform_limits_v1 *limits,
    uint8_t **out, size_t *out_len, tf_transform_error **error) {
    tf_transform_limits_v1 copied;
    cJSON *json = NULL;
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    uint8_t *result = NULL;
    size_t result_len;
    uint8_t digest[32];
    tf_transform_code code;
    if (out) *out = NULL;
    if (out_len) *out_len = 0;
    tf_transform_clear_error(error);
    if (!plan || !out || !out_len)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "plan export argument is null");
    code = tf_transform_copy_limits(limits, &copied, error);
    if (code != TF_TRANSFORM_OK) return code;
    json = tf_transform_plan_to_json(plan, error);
    if (!json) return error && *error
        ? (*error)->code : TF_TRANSFORM_ALLOCATION;
    code = tf_transform_json_print_canonical(
        json, &copied, &payload, &payload_len, error);
    if (code != TF_TRANSFORM_OK) goto done;
    if (payload_len > SIZE_MAX - 52) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "TFTR byte count overflows");
        goto done;
    }
    result_len = 52 + payload_len;
    if ((uint64_t)result_len > copied.max_plan_bytes
        || (uint64_t)result_len > copied.max_allocation_bytes) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "TFTR exceeds plan byte limits");
        goto done;
    }
    result = (uint8_t *)malloc(result_len);
    if (!result) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION, "TFTR allocation failed");
        goto done;
    }
    memcpy(result, "TFTR", 4);
    write_u16_le(result + 4, 1);
    write_u16_le(result + 6, 0);
    write_u32_le(result + 8, 52);
    write_u64_le(result + 12, (uint64_t)payload_len);
    tf_transform_sha256(payload, payload_len, digest);
    memcpy(result + 20, digest, sizeof(digest));
    memcpy(result + 52, payload, payload_len);
    *out = result;
    *out_len = result_len;
    result = NULL;
    code = TF_TRANSFORM_OK;
done:
    free(result);
    tf_transform_bytes_free(&payload, &payload_len);
    cJSON_Delete(json);
    return code;
}

tf_transform_code tf_transform_plan_import(
    const uint8_t *bytes, size_t len, const tf_transform_runtime_v1 *runtime,
    tf_transform_plan **out, tf_transform_error **error) {
    tf_transform_runtime_copy copied;
    tf_transform_plan *plan = NULL;
    uint8_t *canonical = NULL;
    size_t canonical_len = 0;
    uint8_t digest[32];
    uint64_t payload_u64;
    size_t payload_len;
    tf_transform_fp_guard guard;
    tf_transform_code code;
    int canonical_equal = 0;
    if (out) *out = NULL;
    tf_transform_clear_error(error);
    if (!bytes || !out)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "plan import argument is null");
    code = tf_transform_copy_runtime(runtime, &copied, error);
    if (code != TF_TRANSFORM_OK) return code;
    if ((uint64_t)len > copied.limits.max_plan_bytes) return tf_transform_set_error(
        error, TF_TRANSFORM_RESOURCE_LIMIT, "TFTR exceeds plan byte limit");
    code = tf_transform_poll_cancel(&copied, error);
    if (code != TF_TRANSFORM_OK) return code;
    if (len < 52 || memcmp(bytes, "TFTR", 4) != 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN, "invalid TFTR envelope");
    if (read_u16_le(bytes + 4) != 1)
        return tf_transform_set_error(
            error, TF_TRANSFORM_UNSUPPORTED_VERSION,
            "TFTR envelope version is unsupported");
    if (read_u16_le(bytes + 6) != 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_UNSUPPORTED_VERSION,
            "TFTR envelope flags are unsupported");
    if (read_u32_le(bytes + 8) != 52)
        return tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN, "invalid TFTR header length");
    payload_u64 = read_u64_le(bytes + 12);
    if (payload_u64 > SIZE_MAX || payload_u64 > copied.limits.max_plan_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN, "invalid TFTR payload length");
    payload_len = (size_t)payload_u64;
    if (payload_len > SIZE_MAX - 52 || 52 + payload_len != len)
        return tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN, "TFTR length does not match payload");
    code = tf_transform_sha256_runtime(
        bytes + 52, payload_len, &copied, digest, error);
    if (code != TF_TRANSFORM_OK) return code;
    if (memcmp(digest, bytes + 20, sizeof(digest)) != 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN, "TFTR payload hash mismatches");
    code = tf_transform_plan_from_json(
        bytes + 52, payload_len, &copied, &plan,
        &canonical, &canonical_len, error);
    if (code != TF_TRANSFORM_OK) goto done;
    if (canonical_len == payload_len)
        code = runtime_bytes_equal(
            canonical, bytes + 52, payload_len,
            &copied, &canonical_equal, error);
    if (code != TF_TRANSFORM_OK) goto done;
    if (canonical_len != payload_len || !canonical_equal) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN,
            "TFTR payload is not the canonical plan encoding");
        goto done;
    }
    code = tf_transform_fp_begin(&guard, error);
    if (code != TF_TRANSFORM_OK) goto done;
    code = tf_transform_poll_cancel(&copied, error);
    if (code == TF_TRANSFORM_OK)
        code = tf_transform_check_runtime_fp(error);
    tf_transform_fp_end(&guard);
    if (code != TF_TRANSFORM_OK) goto done;
    *out = plan;
    plan = NULL;
done:
    tf_transform_bytes_free(&canonical, &canonical_len);
    tf_transform_plan_release(plan);
    return code;
}
