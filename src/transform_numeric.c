#include "transform_internal.h"

#include <float.h>
#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#if defined(__i386__) || defined(__x86_64__)
#include <xmmintrin.h>
#endif

typedef struct tf_u128 {
    uint64_t high;
    uint64_t low;
} tf_u128;

static int u128_compare(tf_u128 left, tf_u128 right) {
    if (left.high != right.high) return left.high < right.high ? -1 : 1;
    if (left.low != right.low) return left.low < right.low ? -1 : 1;
    return 0;
}

static tf_u128 u128_add(tf_u128 left, tf_u128 right) {
    tf_u128 result;
    result.low = left.low + right.low;
    result.high = left.high + right.high + (result.low < left.low ? 1u : 0u);
    return result;
}

static tf_u128 u128_subtract(tf_u128 left, tf_u128 right) {
    tf_u128 result;
    result.high = left.high - right.high - (left.low < right.low ? 1u : 0u);
    result.low = left.low - right.low;
    return result;
}

static tf_u128 u128_shift_right(tf_u128 value, unsigned int amount) {
    tf_u128 result = {0, 0};
    if (amount == 0) return value;
    if (amount < 64) {
        result.low = (value.low >> amount) | (value.high << (64u - amount));
        result.high = value.high >> amount;
    } else if (amount < 128) {
        result.low = value.high >> (amount - 64u);
    }
    return result;
}

static uint64_t integer_sqrt_u128(tf_u128 value, tf_u128 *remainder) {
    tf_u128 result = {0, 0};
    tf_u128 bit = {UINT64_C(1) << 62, 0};
    while (u128_compare(bit, value) > 0) bit = u128_shift_right(bit, 2);
    while (bit.high != 0 || bit.low != 0) {
        tf_u128 candidate = u128_add(result, bit);
        if (u128_compare(value, candidate) >= 0) {
            value = u128_subtract(value, candidate);
            result = u128_add(u128_shift_right(result, 1), bit);
        } else {
            result = u128_shift_right(result, 1);
        }
        bit = u128_shift_right(bit, 2);
    }
    if (remainder) *remainder = value;
    return result.low;
}

static unsigned int highest_bit_u64(uint64_t value) {
    unsigned int bit = 0;
    while (value >>= 1) ++bit;
    return bit;
}

double tf_transform_sqrt_f64_rne(double value) {
    uint64_t bits = tf_transform_double_bits(value);
    uint64_t fraction = bits & UINT64_C(0x000fffffffffffff);
    unsigned int encoded_exponent = (unsigned int)((bits >> 52) & 0x7ffu);
    uint64_t significand;
    int exponent;
    tf_u128 radicand;
    tf_u128 remainder;
    uint64_t root;
    uint64_t result_bits;

    if ((bits >> 63) != 0 || encoded_exponent == 0x7ffu) return value;
    if (encoded_exponent == 0 && fraction == 0) return 0.0;
    if (encoded_exponent == 0) {
        unsigned int top = highest_bit_u64(fraction);
        unsigned int shift = 52u - top;
        significand = fraction << shift;
        exponent = -1022 - (int)shift;
    } else {
        significand = UINT64_C(0x0010000000000000) | fraction;
        exponent = (int)encoded_exponent - 1023;
    }
    if ((exponent & 1) != 0) {
        significand <<= 1;
        --exponent;
    }
    radicand.high = significand >> 12;
    radicand.low = significand << 52;
    root = integer_sqrt_u128(radicand, &remainder);
    if (u128_compare(remainder, (tf_u128){0, root}) > 0) ++root;
    if (root == UINT64_C(0x0020000000000000)) {
        root >>= 1;
        exponent += 2;
    }
    result_bits = ((uint64_t)(exponent / 2 + 1023) << 52)
        | (root & UINT64_C(0x000fffffffffffff));
    return tf_transform_double_from_bits(result_bits);
}

tf_transform_code tf_transform_check_runtime_fp(tf_transform_error **error) {
    volatile double minimum = DBL_MIN;
    volatile double subnormal = minimum * 0.5;
    if (FLT_RADIX != 2 || FLT_MANT_DIG != 24 || DBL_MANT_DIG != 53
        || FLT_EVAL_METHOD != 0 || subnormal == 0.0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_UNSUPPORTED_RUNTIME,
            "runtime does not provide prepared-transform V1 floating semantics");
#if !defined(__wasm__)
    if (fegetround() != FE_TONEAREST)
        return tf_transform_set_error(
            error, TF_TRANSFORM_UNSUPPORTED_RUNTIME,
            "prepared-transform floating environment is not round-to-nearest");
#endif
#if defined(__i386__) || defined(__x86_64__)
    if ((_mm_getcsr() & ((1u << 15) | (1u << 6))) != 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_UNSUPPORTED_RUNTIME,
            "prepared-transform floating environment flushes subnormals");
#endif
    return TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_fp_begin(
    tf_transform_fp_guard *guard, tf_transform_error **error) {
    tf_transform_code code;
    if (!guard) return tf_transform_set_error(
        error, TF_TRANSFORM_INVALID_ARGUMENT, "floating guard is null");
    memset(guard, 0, sizeof(*guard));
#if defined(__i386__) || defined(__x86_64__)
    guard->mxcsr = _mm_getcsr();
#endif
#if !defined(__wasm__)
    if (fegetenv(&guard->environment) != 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_UNSUPPORTED_RUNTIME,
            "cannot save the caller floating environment");
    if (fesetround(FE_TONEAREST) != 0) {
        (void)fesetenv(&guard->environment);
#if defined(__i386__) || defined(__x86_64__)
        _mm_setcsr(guard->mxcsr);
#endif
        return tf_transform_set_error(
            error, TF_TRANSFORM_UNSUPPORTED_RUNTIME,
            "cannot select round-to-nearest floating environment");
    }
#endif
#if defined(__i386__) || defined(__x86_64__)
    {
        unsigned int active_mxcsr = _mm_getcsr();
        active_mxcsr &= ~((1u << 15) | (1u << 6));
        active_mxcsr &= ~(3u << 13);
        _mm_setcsr(active_mxcsr);
    }
#endif
    guard->active = 1;
    code = tf_transform_check_runtime_fp(error);
    if (code != TF_TRANSFORM_OK) tf_transform_fp_end(guard);
    return code;
}

void tf_transform_fp_end(tf_transform_fp_guard *guard) {
    if (!guard || !guard->active) return;
#if !defined(__wasm__)
    (void)fesetenv(&guard->environment);
#endif
#if defined(__i386__) || defined(__x86_64__)
    _mm_setcsr(guard->mxcsr);
#endif
    guard->active = 0;
}

static tf_transform_code checked_add_u64(
    uint64_t left, uint64_t right, uint64_t *out,
    tf_transform_error **error, const char *message) {
    if (right > UINT64_MAX - left)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, message);
    *out = left + right;
    return TF_TRANSFORM_OK;
}

static tf_transform_code checked_mul_size(
    size_t left, size_t right, size_t *out,
    tf_transform_error **error, const char *message) {
    if (left != 0 && right > SIZE_MAX / left)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, message);
    *out = left * right;
    return TF_TRANSFORM_OK;
}

static tf_transform_code validate_column_view(
    const tf_column_view_v1 *column, size_t rows, uint32_t dtype,
    size_t *input_bytes, tf_transform_error **error) {
    size_t item_size = dtype == TF_VIEW_FLOAT32 ? sizeof(float) : sizeof(double);
    size_t data_span = 0;
    size_t validity_span = 0;
    size_t last;

    if (!column || !input_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "column view argument is null");
    if (column->abi_version != 1 || column->struct_size != sizeof(*column))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "invalid column view V1 header");
    if (column->stride_bytes == 0 || column->stride_bytes % item_size != 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "invalid numeric column stride");
    if (rows == 0) {
        if (column->data != NULL || column->data_bytes != 0
            || column->validity != NULL || column->validity_bytes != 0
            || column->validity_bit_offset != 0
            || column->validity_bit_stride != 0)
            return tf_transform_set_error(
                error, TF_TRANSFORM_INVALID_ARGUMENT,
                "zero-row column must carry empty data and validity spans");
        *input_bytes = 0;
        return TF_TRANSFORM_OK;
    }
    if (!column->data || (uintptr_t)column->data % item_size != 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "numeric column data is null or misaligned");
    if (rows - 1 > (SIZE_MAX - item_size) / column->stride_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "numeric column span overflows");
    data_span = (rows - 1) * column->stride_bytes + item_size;
    if (data_span > column->data_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "numeric column data is truncated");

    if (!column->validity) {
        if (column->validity_bytes != 0 || column->validity_bit_offset != 0
            || column->validity_bit_stride != 0)
            return tf_transform_set_error(
                error, TF_TRANSFORM_INVALID_ARGUMENT, "invalid null validity span");
    } else {
        if (column->validity_bit_stride == 0)
            return tf_transform_set_error(
                error, TF_TRANSFORM_INVALID_ARGUMENT, "validity stride must be positive");
        if (rows - 1 > (SIZE_MAX - column->validity_bit_offset)
                / column->validity_bit_stride)
            return tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT, "validity bit span overflows");
        last = column->validity_bit_offset
            + (rows - 1) * column->validity_bit_stride;
        if (last > SIZE_MAX - 8)
            return tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT, "validity byte span overflows");
        validity_span = last / 8 + 1;
        if (validity_span > column->validity_bytes)
            return tf_transform_set_error(
                error, TF_TRANSFORM_INVALID_ARGUMENT, "validity bitmap is truncated");
    }
    if (data_span > SIZE_MAX - validity_span)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "column input byte count overflows");
    *input_bytes = data_span + validity_span;
    return TF_TRANSFORM_OK;
}

static tf_transform_code validate_table_view(
    const tf_transform_schema *schema, const tf_table_view_v1 *table,
    uint64_t row_limit, uint64_t byte_limit, size_t *input_bytes,
    const tf_transform_runtime_copy *runtime,
    tf_transform_error **error) {
    size_t descriptor_bytes = 0;
    size_t total = 0;
    tf_transform_code code;

    if (!schema || !table || !input_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "table argument is null");
    if (table->abi_version != 1 || table->struct_size != sizeof(*table)
        || table->column_count != schema->field_count
        || table->column_count == 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_SCHEMA_MISMATCH, "table column count does not match schema");
    code = checked_mul_size(
        table->column_count, sizeof(tf_column_view_v1), &descriptor_bytes,
        error, "table descriptor byte count overflows");
    if (code != TF_TRANSFORM_OK) return code;
    if (!table->columns || table->columns_bytes != descriptor_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "invalid table descriptor span");
    if ((uint64_t)table->row_count > row_limit)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "table row count exceeds limit");
    for (size_t i = 0; i < table->column_count; ++i) {
        size_t column_bytes = 0;
        if (runtime && i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            code = tf_transform_poll_cancel(runtime, error);
            if (code != TF_TRANSFORM_OK) return code;
            code = tf_transform_check_runtime_fp(error);
            if (code != TF_TRANSFORM_OK) return code;
        }
        code = validate_column_view(
            &table->columns[i], table->row_count, schema->fields[i].dtype,
            &column_bytes, error);
        if (code != TF_TRANSFORM_OK) return code;
        if (column_bytes > SIZE_MAX - total)
            return tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT, "table input byte count overflows");
        total += column_bytes;
    }
    if ((uint64_t)total > byte_limit)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "table input bytes exceed limit");
    *input_bytes = total;
    if (!runtime) return TF_TRANSFORM_OK;
    code = tf_transform_poll_cancel(runtime, error);
    if (code != TF_TRANSFORM_OK) return code;
    return tf_transform_check_runtime_fp(error);
}

static int column_value_is_valid(const tf_column_view_v1 *column, size_t row) {
    size_t bit;
    if (!column->validity) return 1;
    bit = column->validity_bit_offset + row * column->validity_bit_stride;
    return (int)((column->validity[bit / 8] >> (bit % 8)) & 1u);
}

static double read_numeric_value(
    const tf_column_view_v1 *column, size_t row, uint32_t dtype) {
    const uint8_t *address = (const uint8_t *)column->data
        + row * column->stride_bytes;
    if (dtype == TF_VIEW_FLOAT32) {
        float value;
        memcpy(&value, address, sizeof(value));
        return (double)value;
    }
    {
        double value;
        memcpy(&value, address, sizeof(value));
        return value;
    }
}

static tf_transform_code poll_and_recheck(
    const tf_transform_runtime_copy *runtime, tf_transform_error **error) {
    tf_transform_code code = tf_transform_poll_cancel(runtime, error);
    if (code != TF_TRANSFORM_OK) return code;
    return tf_transform_check_runtime_fp(error);
}

static tf_transform_code schema_owned_metrics(
    const tf_transform_schema *schema, uint64_t *resident_bytes,
    uint64_t *allocation_count, const tf_transform_runtime_copy *runtime,
    int recheck_fp, tf_transform_error **error) {
    uint64_t resident;
    uint64_t allocations;
    if (!schema || !resident_bytes || !allocation_count)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "schema metric argument is null");
    if (schema->field_count > UINT64_MAX / sizeof(*schema->fields))
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "schema resident byte count overflows");
    resident = (uint64_t)schema->field_count * sizeof(*schema->fields);
    allocations = 1;
    for (size_t i = 0; i < schema->field_count; ++i) {
        if (runtime && i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            tf_transform_code code = tf_transform_poll_cancel(runtime, error);
            if (code != TF_TRANSFORM_OK) return code;
            if (recheck_fp) {
                code = tf_transform_check_runtime_fp(error);
                if (code != TF_TRANSFORM_OK) return code;
            }
        }
        uint64_t strings = (uint64_t)schema->fields[i].id_len + 1;
        if ((uint64_t)schema->fields[i].name_len + 1 > UINT64_MAX - strings
            || strings + (uint64_t)schema->fields[i].name_len + 1
                > UINT64_MAX - resident)
            return tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "schema resident string bytes overflow");
        resident += strings + (uint64_t)schema->fields[i].name_len + 1;
        if (schema->fields[i].source_id) {
            if ((uint64_t)schema->fields[i].source_id_len + 1
                > UINT64_MAX - resident)
                return tf_transform_set_error(
                    error, TF_TRANSFORM_RESOURCE_LIMIT,
                    "schema source ID bytes overflow");
            resident += (uint64_t)schema->fields[i].source_id_len + 1;
        }
        if (allocations > UINT64_MAX - 2
            - (schema->fields[i].source_id ? 1u : 0u))
            return tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "schema allocation count overflows");
        allocations += 2 + (schema->fields[i].source_id ? 1u : 0u);
    }
    *resident_bytes = resident;
    *allocation_count = allocations;
    return TF_TRANSFORM_OK;
}

static tf_transform_code session_check_totals(
    const tf_transform_runtime_copy *runtime,
    uint64_t current_resident, uint64_t added_resident,
    uint64_t current_allocations, uint64_t added_allocations,
    tf_transform_error **error) {
    if (!runtime || added_resident > UINT64_MAX - current_resident
        || added_allocations > UINT64_MAX - current_allocations
        || current_resident + added_resident
            > runtime->limits.max_resident_state_bytes
        || current_allocations + added_allocations
            > runtime->limits.max_allocations_per_session)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "prepared-transform session resource limit exceeded");
    return tf_transform_poll_cancel(runtime, error);
}

static void median_store_clear(tf_transform_median_store *store) {
    if (!store) return;
    for (size_t i = 0; i < store->block_count; ++i) free(store->blocks[i]);
    free(store->blocks);
    memset(store, 0, sizeof(*store));
}

static void analyzer_median_stores_clear(tf_transform_analyzer *analyzer) {
    if (!analyzer || !analyzer->median_stores) return;
    for (size_t i = 0; i < analyzer->input_schema.field_count; ++i)
        median_store_clear(&analyzer->median_stores[i]);
    free(analyzer->median_stores);
    analyzer->median_stores = NULL;
}

tf_transform_code tf_transform_analyzer_allocate_retained(
    tf_transform_analyzer *analyzer, size_t bytes, void **out,
    tf_transform_error **error) {
    void *allocated;
    tf_transform_code code;
    if (out) *out = NULL;
    if (!analyzer || !out || bytes == 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "invalid analyzer retained-state allocation");
    if ((uint64_t)bytes > analyzer->runtime.limits.max_allocation_bytes
        || analyzer->allocation_count == UINT64_MAX
        || analyzer->allocation_count + 1
            > analyzer->runtime.limits.max_allocations_per_session
        || analyzer->resident_state_bytes > UINT64_MAX - (uint64_t)bytes
        || analyzer->resident_state_bytes + (uint64_t)bytes
            > analyzer->runtime.limits.max_resident_state_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "retained state exceeds analyzer limits");
    code = poll_and_recheck(&analyzer->runtime, error);
    if (code != TF_TRANSFORM_OK) return code;
    allocated = malloc(bytes);
    if (!allocated)
        return tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION,
            "retained-state allocation failed");
    ++analyzer->allocation_count;
    analyzer->resident_state_bytes += (uint64_t)bytes;
    *out = allocated;
    return TF_TRANSFORM_OK;
}

static tf_transform_code median_store_grow_index(
    tf_transform_analyzer *analyzer, tf_transform_median_store *store,
    size_t required, tf_transform_error **error) {
    size_t maximum;
    size_t capacity;
    size_t bytes;
    size_t old_bytes;
    uint64_t maximum_u64;
    double **blocks = NULL;
    tf_transform_code code;
    if (required <= store->block_capacity) return TF_TRANSFORM_OK;
    maximum_u64 = analyzer->runtime.limits.max_allocation_bytes
        / sizeof(*blocks);
    maximum = maximum_u64 > (uint64_t)SIZE_MAX
        ? SIZE_MAX : (size_t)maximum_u64;
    if (required > maximum)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "median block index exceeds allocation limit");
    capacity = store->block_capacity == 0 ? 4 : store->block_capacity;
    if (capacity > maximum) capacity = maximum;
    while (capacity < required) {
        if (capacity > maximum / 2) {
            capacity = maximum;
            break;
        }
        capacity *= 2;
    }
    if (capacity < required || capacity > SIZE_MAX / sizeof(*blocks))
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "median block index size overflows");
    bytes = capacity * sizeof(*blocks);
    code = tf_transform_analyzer_allocate_retained(
        analyzer, bytes, (void **)&blocks, error);
    if (code != TF_TRANSFORM_OK) return code;
    old_bytes = store->block_capacity * sizeof(*blocks);
    if (store->block_count != 0) {
        code = tf_transform_copy_bytes_runtime(
            blocks, store->blocks,
            store->block_count * sizeof(*blocks),
            &analyzer->runtime, error);
        if (code != TF_TRANSFORM_OK) {
            free(blocks);
            analyzer->resident_state_bytes -= (uint64_t)bytes;
            return code;
        }
    }
    free(store->blocks);
    analyzer->resident_state_bytes -= (uint64_t)old_bytes;
    store->blocks = blocks;
    store->block_capacity = capacity;
    return TF_TRANSFORM_OK;
}

static tf_transform_code median_store_append(
    tf_transform_analyzer *analyzer, tf_transform_median_store *store,
    double value, tf_transform_error **error) {
    uint64_t block_index_u64;
    size_t block_index;
    size_t offset;
    size_t block_bytes;
    double *block = NULL;
    tf_transform_code code;
    if (!store || store->values_per_block == 0 || store->count == UINT64_MAX)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "median retained-value count exceeds limits");
    block_index_u64 = store->count / (uint64_t)store->values_per_block;
    if (block_index_u64 > SIZE_MAX)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "median block index overflows");
    block_index = (size_t)block_index_u64;
    offset = (size_t)(store->count % (uint64_t)store->values_per_block);
    if (block_index == store->block_count) {
        if (store->block_count == SIZE_MAX)
            return tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "median block count overflows");
        code = median_store_grow_index(
            analyzer, store, store->block_count + 1, error);
        if (code != TF_TRANSFORM_OK) return code;
        block_bytes = store->values_per_block * sizeof(*block);
        code = tf_transform_analyzer_allocate_retained(
            analyzer, block_bytes, (void **)&block, error);
        if (code != TF_TRANSFORM_OK) return code;
        store->blocks[store->block_count++] = block;
    } else if (block_index > store->block_count) {
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "median retained-value block sequence is invalid");
    }
    store->blocks[block_index][offset] = value;
    ++store->count;
    return TF_TRANSFORM_OK;
}

static double median_store_get(
    const tf_transform_median_store *store, uint64_t index) {
    size_t block_index = (size_t)(index / (uint64_t)store->values_per_block);
    size_t offset = (size_t)(index % (uint64_t)store->values_per_block);
    return store->blocks[block_index][offset];
}

static void median_store_set(
    tf_transform_median_store *store, uint64_t index, double value) {
    size_t block_index = (size_t)(index / (uint64_t)store->values_per_block);
    size_t offset = (size_t)(index % (uint64_t)store->values_per_block);
    store->blocks[block_index][offset] = value;
}

static int median_value_compare(double left, double right) {
    if (left < right) return -1;
    if (left > right) return 1;
    return 0;
}

static tf_transform_code median_sort_tick(
    const tf_transform_runtime_copy *runtime, size_t *ticks,
    tf_transform_error **error) {
    ++*ticks;
    if (*ticks < TF_TRANSFORM_CANCEL_ITERS_V1) return TF_TRANSFORM_OK;
    *ticks = 0;
    return poll_and_recheck(runtime, error);
}

static tf_transform_code median_store_sift_down(
    tf_transform_median_store *store, uint64_t root, uint64_t end,
    const tf_transform_runtime_copy *runtime, size_t *ticks,
    tf_transform_error **error) {
    if (end == 0) return TF_TRANSFORM_OK;
    while (root <= (end - 1) / 2) {
        uint64_t child = root * 2 + 1;
        uint64_t candidate = root;
        tf_transform_code code = median_sort_tick(runtime, ticks, error);
        if (code != TF_TRANSFORM_OK) return code;
        if (median_value_compare(
                median_store_get(store, candidate),
                median_store_get(store, child)) < 0)
            candidate = child;
        if (child < end) {
            code = median_sort_tick(runtime, ticks, error);
            if (code != TF_TRANSFORM_OK) return code;
            if (median_value_compare(
                    median_store_get(store, candidate),
                    median_store_get(store, child + 1)) < 0)
                candidate = child + 1;
        }
        if (candidate == root) return TF_TRANSFORM_OK;
        {
            double root_value = median_store_get(store, root);
            double candidate_value = median_store_get(store, candidate);
            median_store_set(store, root, candidate_value);
            median_store_set(store, candidate, root_value);
        }
        root = candidate;
    }
    return TF_TRANSFORM_OK;
}

static tf_transform_code median_store_resolve(
    tf_transform_median_store *store,
    const tf_transform_runtime_copy *runtime,
    double *out, tf_transform_error **error) {
    size_t ticks = 0;
    tf_transform_code code;
    if (!store || !runtime || !out || store->count == 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "median retained state is unavailable");
    if (store->count > 1) {
        uint64_t start = store->count / 2;
        uint64_t end = store->count - 1;
        while (start != 0) {
            --start;
            code = median_store_sift_down(
                store, start, end, runtime, &ticks, error);
            if (code != TF_TRANSFORM_OK) return code;
        }
        while (end != 0) {
            double first = median_store_get(store, 0);
            double last = median_store_get(store, end);
            median_store_set(store, 0, last);
            median_store_set(store, end, first);
            --end;
            code = median_store_sift_down(
                store, 0, end, runtime, &ticks, error);
            if (code != TF_TRANSFORM_OK) return code;
        }
    }
    code = poll_and_recheck(runtime, error);
    if (code != TF_TRANSFORM_OK) return code;
    if ((store->count & UINT64_C(1)) != 0) {
        *out = median_store_get(store, store->count / 2);
    } else {
        double lower = median_store_get(store, store->count / 2 - 1);
        double upper = median_store_get(store, store->count / 2);
        double lower_half = lower / 2.0;
        double upper_half = upper / 2.0;
        *out = lower_half + upper_half;
        if (!tf_transform_double_is_finite(*out))
            return tf_transform_set_error(
                error, TF_TRANSFORM_NUMERIC_DOMAIN,
                "median imputation value became nonfinite");
    }
    if (*out == 0.0) *out = 0.0;
    return TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_analyzer_create(
    const tf_transform_recipe *recipe, const tf_schema_view_v1 *input_schema,
    const tf_transform_runtime_v1 *runtime,
    tf_transform_analyzer **out, tf_transform_error **error) {
    tf_transform_analyzer *created = NULL;
    tf_transform_runtime_copy runtime_copy;
    tf_transform_schema schema = {0};
    tf_transform_code code;
    size_t state_bytes = 0;
    size_t median_store_bytes = 0;
    size_t median_values_per_block = 0;
    uint64_t category_store_resident = 0;
    uint64_t category_store_allocations = 0;
    uint64_t schema_resident = 0;
    uint64_t schema_allocations = 0;
    uint64_t session_resident;
    uint64_t session_allocations;
    uint64_t analyzer_allocations;
    int uses_median = 0;
    int uses_categorical = 0;

    if (out) *out = NULL;
    tf_transform_clear_error(error);
    if (!recipe || !input_schema || !out)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "analyzer create argument is null");
    code = tf_transform_copy_runtime(runtime, &runtime_copy, error);
    if (code != TF_TRANSFORM_OK) return code;
    code = tf_transform_poll_cancel(&runtime_copy, error);
    if (code != TF_TRANSFORM_OK) return code;
    code = tf_transform_schema_copy_runtime(
        input_schema, &runtime_copy, &schema, error);
    if (code != TF_TRANSFORM_OK) return code;
    if (schema.field_count != recipe->column_count) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_SCHEMA_MISMATCH,
            "recipe columns do not match the input schema");
        goto fail;
    }
    for (size_t i = 0; i < schema.field_count; ++i) {
        const tf_transform_recipe_column *column = &recipe->columns[i];
        int comparison;
        code = tf_transform_compare_bytes_runtime(
            column->source_id, column->source_id_len,
            schema.fields[i].id, schema.fields[i].id_len,
            &runtime_copy, &comparison, error);
        if (code != TF_TRANSFORM_OK) goto fail;
        if (comparison != 0
            || (column->kind == TF_TRANSFORM_KIND_NUMERIC
                && column->impute == TF_TRANSFORM_IMPUTE_CONSTANT
                && column->constant_dtype != schema.fields[i].dtype)) {
            code = tf_transform_set_error(
                error, TF_TRANSFORM_SCHEMA_MISMATCH,
                "recipe source or constant dtype does not match schema");
            goto fail;
        }
        if (column->kind == TF_TRANSFORM_KIND_CATEGORICAL)
            uses_categorical = 1;
        else if (column->impute == TF_TRANSFORM_IMPUTE_MEDIAN)
            uses_median = 1;
    }
    code = checked_mul_size(
        schema.field_count, sizeof(tf_transform_running_stats), &state_bytes,
        error, "analyzer state byte count overflows");
    if (code != TF_TRANSFORM_OK) goto fail;
    if (uses_median) {
        uint64_t target_block_bytes = runtime_copy.limits.max_allocation_bytes;
        if (target_block_bytes > TF_TRANSFORM_CANCEL_BYTES_V1)
            target_block_bytes = TF_TRANSFORM_CANCEL_BYTES_V1;
        median_values_per_block = (size_t)(target_block_bytes / sizeof(double));
        if (median_values_per_block == 0) {
            code = tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "median value blocks exceed the allocation limit");
            goto fail;
        }
        code = checked_mul_size(
            schema.field_count, sizeof(tf_transform_median_store),
            &median_store_bytes, error,
            "median store byte count overflows");
        if (code != TF_TRANSFORM_OK) goto fail;
        if ((uint64_t)median_store_bytes
                > runtime_copy.limits.max_allocation_bytes) {
            code = tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "median stores exceed the allocation limit");
            goto fail;
        }
    }
    if (uses_categorical) {
        code = tf_transform_category_stores_requirements(
            schema.field_count, &category_store_resident,
            &category_store_allocations, error);
        if (code != TF_TRANSFORM_OK) goto fail;
        if (category_store_resident > runtime_copy.limits.max_allocation_bytes) {
            code = tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "categorical stores exceed the allocation limit");
            goto fail;
        }
    }
    if ((uint64_t)state_bytes > runtime_copy.limits.max_resident_state_bytes
        || (uint64_t)state_bytes > runtime_copy.limits.max_allocation_bytes) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "analyzer state exceeds limits");
        goto fail;
    }
    code = schema_owned_metrics(
        &schema, &schema_resident, &schema_allocations,
        &runtime_copy, 0, error);
    if (code != TF_TRANSFORM_OK) goto fail;
    if ((uint64_t)state_bytes > (UINT64_MAX - sizeof(*created)) / 2)
        goto resource_overflow;
    session_resident = sizeof(*created) + (uint64_t)state_bytes * 2;
    if ((uint64_t)median_store_bytes > UINT64_MAX - session_resident)
        goto resource_overflow;
    session_resident += (uint64_t)median_store_bytes;
    if (category_store_resident > UINT64_MAX - session_resident)
        goto resource_overflow;
    session_resident += category_store_resident;
    if (schema_resident > UINT64_MAX - session_resident)
        goto resource_overflow;
    session_resident += schema_resident;
    analyzer_allocations = 4 + (uses_median ? 1u : 0u);
    if (category_store_allocations > UINT64_MAX - analyzer_allocations)
        goto resource_overflow;
    analyzer_allocations += category_store_allocations;
    if (schema_allocations > UINT64_MAX - analyzer_allocations)
        goto resource_overflow;
    /* Schema copy also allocates and frees one uniqueness index. */
    session_allocations = schema_allocations + analyzer_allocations;
    code = session_check_totals(
        &runtime_copy, 0, session_resident, 0, session_allocations, error);
    if (code != TF_TRANSFORM_OK) goto fail;
    created = (tf_transform_analyzer *)calloc(1, sizeof(*created));
    if (!created) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION, "analyzer allocation failed");
        goto fail;
    }
    if (schema.field_count == 0) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL, "validated schema is unexpectedly empty");
        goto fail;
    }
    code = tf_transform_poll_cancel(&runtime_copy, error);
    if (code != TF_TRANSFORM_OK) goto fail;
    created->stats = (tf_transform_running_stats *)calloc(
        schema.field_count, sizeof(*created->stats));
    if (!created->stats) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION, "analyzer state allocation failed");
        goto fail;
    }
    code = tf_transform_poll_cancel(&runtime_copy, error);
    if (code != TF_TRANSFORM_OK) goto fail;
    created->scratch = (tf_transform_running_stats *)calloc(
        schema.field_count, sizeof(*created->scratch));
    if (!created->scratch) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION,
            "analyzer scratch allocation failed");
        goto fail;
    }
    if (uses_median) {
        code = tf_transform_poll_cancel(&runtime_copy, error);
        if (code != TF_TRANSFORM_OK) goto fail;
        created->median_stores = (tf_transform_median_store *)calloc(
            schema.field_count, sizeof(*created->median_stores));
        if (!created->median_stores) {
            code = tf_transform_set_error(
                error, TF_TRANSFORM_ALLOCATION,
                "median store allocation failed");
            goto fail;
        }
        for (size_t i = 0; i < schema.field_count; ++i) {
            if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
                code = tf_transform_poll_cancel(&runtime_copy, error);
                if (code != TF_TRANSFORM_OK) goto fail;
            }
            if (recipe->columns[i].impute == TF_TRANSFORM_IMPUTE_MEDIAN)
                created->median_stores[i].values_per_block
                    = median_values_per_block;
        }
    }
    created->recipe = (tf_transform_recipe *)recipe;
    tf_transform_recipe_retain(created->recipe);
    created->input_schema = schema;
    memset(&schema, 0, sizeof(schema));
    created->runtime = runtime_copy;
    created->allocation_count = session_allocations - category_store_allocations;
    created->resident_state_bytes = session_resident - category_store_resident;
    if (uses_categorical) {
        code = tf_transform_category_stores_init(created, error);
        if (code != TF_TRANSFORM_OK) goto fail;
    }
    created->state = TF_ANALYZER_ACTIVE;
    *out = created;
    return TF_TRANSFORM_OK;
resource_overflow:
    code = tf_transform_set_error(
        error, TF_TRANSFORM_RESOURCE_LIMIT,
        "analyzer session resource count overflows");
fail:
    tf_transform_schema_clear(&schema);
    if (created) {
        tf_transform_recipe_release(created->recipe);
        analyzer_median_stores_clear(created);
        tf_transform_category_stores_clear(created);
        tf_transform_schema_clear(&created->input_schema);
        free(created->stats);
        free(created->scratch);
        free(created);
    }
    return code;
}

void tf_transform_analyzer_destroy(tf_transform_analyzer **analyzer) {
    if (!analyzer || !*analyzer) return;
    tf_transform_recipe_release((*analyzer)->recipe);
    analyzer_median_stores_clear(*analyzer);
    tf_transform_category_stores_clear(*analyzer);
    tf_transform_schema_clear(&(*analyzer)->input_schema);
    free((*analyzer)->stats);
    free((*analyzer)->scratch);
    free(*analyzer);
    *analyzer = NULL;
}

tf_transform_code tf_transform_analyzer_push(
    tf_transform_analyzer *analyzer, const tf_table_view_v1 *table,
    tf_transform_error **error) {
    tf_transform_running_stats *pending;
    tf_transform_fp_guard guard;
    tf_transform_code code;
    size_t input_bytes = 0;
    size_t stats_bytes;
    uint64_t new_rows = 0;
    uint64_t new_input_bytes = 0;

    tf_transform_clear_error(error);
    if (!analyzer)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "analyzer push argument is null");
    if (analyzer->state != TF_ANALYZER_ACTIVE)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_STATE, "analyzer is not active");
    if (!table) {
        analyzer->state = TF_ANALYZER_FAILED;
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "analyzer table is null");
    }
    code = tf_transform_fp_begin(&guard, error);
    if (code != TF_TRANSFORM_OK) goto failed;
    analyzer->runtime.fp_guard_active = 1;
    code = poll_and_recheck(&analyzer->runtime, error);
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    code = validate_table_view(
        &analyzer->input_schema, table, analyzer->runtime.limits.max_analyzer_rows,
        analyzer->runtime.limits.max_analyzer_input_bytes, &input_bytes,
        &analyzer->runtime, error);
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    code = checked_add_u64(
        analyzer->total_rows, (uint64_t)table->row_count, &new_rows,
        error, "analyzer row counter overflows");
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    code = checked_add_u64(
        analyzer->total_input_bytes, (uint64_t)input_bytes, &new_input_bytes,
        error, "analyzer input byte counter overflows");
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    if (new_rows > analyzer->runtime.limits.max_analyzer_rows
        || new_input_bytes > analyzer->runtime.limits.max_analyzer_input_bytes) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "cumulative analyzer input exceeds limits");
        goto guarded_failed;
    }
    stats_bytes = analyzer->input_schema.field_count * sizeof(*pending);
    if (stats_bytes == 0) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL, "analyzer has no transactional state");
        goto guarded_failed;
    }
    pending = analyzer->scratch;
    if (!pending) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "analyzer scratch state is unavailable");
        goto guarded_failed;
    }
    code = tf_transform_copy_bytes_runtime(
        pending, analyzer->stats, stats_bytes,
        &analyzer->runtime, error);
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    code = poll_and_recheck(&analyzer->runtime, error);
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    if (analyzer->median_stores) {
        for (size_t i = 0; i < analyzer->input_schema.field_count; ++i) {
            if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
                code = poll_and_recheck(&analyzer->runtime, error);
                if (code != TF_TRANSFORM_OK) goto guarded_failed;
            }
            if (analyzer->recipe->columns[i].impute
                    == TF_TRANSFORM_IMPUTE_MEDIAN
                && analyzer->median_stores[i].count
                    != analyzer->stats[i].observed) {
                code = tf_transform_set_error(
                    error, TF_TRANSFORM_INTERNAL,
                    "median retained state does not match analyzer statistics");
                goto guarded_failed;
            }
        }
    }
    if (analyzer->category_stores) {
        for (size_t i = 0; i < analyzer->input_schema.field_count; ++i) {
            if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
                code = poll_and_recheck(&analyzer->runtime, error);
                if (code != TF_TRANSFORM_OK) goto guarded_failed;
            }
            if (analyzer->recipe->columns[i].kind
                    != TF_TRANSFORM_KIND_CATEGORICAL)
                continue;
            code = tf_transform_category_check_observed(
                analyzer, i, analyzer->stats[i].observed, error);
            if (code != TF_TRANSFORM_OK) goto guarded_failed;
        }
    }
    for (size_t row = 0; row < table->row_count; ++row) {
        if (row != 0 && row % TF_TRANSFORM_CANCEL_ROWS_V1 == 0) {
            code = poll_and_recheck(&analyzer->runtime, error);
            if (code != TF_TRANSFORM_OK) goto guarded_failed;
        }
        for (size_t column_index = 0;
             column_index < analyzer->input_schema.field_count; ++column_index) {
            const tf_column_view_v1 *column = &table->columns[column_index];
            tf_transform_running_stats *stats = &pending[column_index];
            double value;
            double delta;
            double mean;
            double delta2;
            double term;
            double m2;
            uint64_t observed;

            if (column_index != 0
                && column_index % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
                code = poll_and_recheck(&analyzer->runtime, error);
                if (code != TF_TRANSFORM_OK) goto guarded_failed;
            }
            if (!column_value_is_valid(column, row)) {
                if (stats->missing == UINT64_MAX) {
                    code = tf_transform_set_error(
                        error, TF_TRANSFORM_RESOURCE_LIMIT,
                        "missing-value counter overflows");
                    goto guarded_failed;
                }
                ++stats->missing;
                continue;
            }
            value = read_numeric_value(
                column, row, analyzer->input_schema.fields[column_index].dtype);
            if (tf_transform_double_is_nan(value)) {
                if (stats->missing == UINT64_MAX) {
                    code = tf_transform_set_error(
                        error, TF_TRANSFORM_RESOURCE_LIMIT,
                        "missing-value counter overflows");
                    goto guarded_failed;
                }
                ++stats->missing;
                continue;
            }
            if (!tf_transform_double_is_finite(value)) {
                code = tf_transform_set_error(
                    error, TF_TRANSFORM_NUMERIC_DOMAIN,
                    "infinite analyzer input is not supported");
                goto guarded_failed;
            }
            if (analyzer->recipe->columns[column_index].kind
                    == TF_TRANSFORM_KIND_CATEGORICAL) {
                if (stats->observed == UINT64_MAX) {
                    code = tf_transform_set_error(
                        error, TF_TRANSFORM_RESOURCE_LIMIT,
                        "categorical observed-value counter overflows");
                    goto guarded_failed;
                }
                code = tf_transform_category_observe(
                    analyzer, column_index, value,
                    analyzer->input_schema.fields[column_index].dtype, error);
                if (code != TF_TRANSFORM_OK) goto guarded_failed;
                ++stats->observed;
                continue;
            }
            if (stats->observed == UINT64_MAX) {
                code = tf_transform_set_error(
                    error, TF_TRANSFORM_RESOURCE_LIMIT,
                    "observed-value counter overflows");
                goto guarded_failed;
            }
            observed = stats->observed + 1;
            delta = value - stats->mean;
            mean = stats->mean + delta / (double)observed;
            delta2 = value - mean;
            term = delta * delta2;
            m2 = stats->m2 + term;
            if (!tf_transform_double_is_finite(delta)
                || !tf_transform_double_is_finite(mean)
                || !tf_transform_double_is_finite(delta2)
                || !tf_transform_double_is_finite(term)
                || !tf_transform_double_is_finite(m2)) {
                code = tf_transform_set_error(
                    error, TF_TRANSFORM_NUMERIC_DOMAIN,
                    "numeric analyzer state became nonfinite");
                goto guarded_failed;
            }
            if (analyzer->recipe->columns[column_index].impute
                    == TF_TRANSFORM_IMPUTE_MEDIAN) {
                code = median_store_append(
                    analyzer, &analyzer->median_stores[column_index],
                    value, error);
                if (code != TF_TRANSFORM_OK) goto guarded_failed;
            }
            stats->observed = observed;
            stats->mean = mean;
            stats->m2 = m2;
            if (!stats->has_value) {
                stats->minimum = value;
                stats->maximum = value;
                stats->has_value = 1;
            } else {
                if (value < stats->minimum) stats->minimum = value;
                if (value > stats->maximum) stats->maximum = value;
            }
        }
    }
    analyzer->runtime.fp_guard_active = 0;
    tf_transform_fp_end(&guard);
    {
        tf_transform_running_stats *committed = analyzer->stats;
        analyzer->stats = analyzer->scratch;
        analyzer->scratch = committed;
    }
    analyzer->total_rows = new_rows;
    analyzer->total_input_bytes = new_input_bytes;
    return TF_TRANSFORM_OK;
guarded_failed:
    analyzer->runtime.fp_guard_active = 0;
    tf_transform_fp_end(&guard);
failed:
    analyzer->state = TF_ANALYZER_FAILED;
    return code;
}

static tf_transform_code finite_or_domain(
    double value, tf_transform_error **error, const char *message) {
    if (!tf_transform_double_is_finite(value))
        return tf_transform_set_error(
            error, TF_TRANSFORM_NUMERIC_DOMAIN, message);
    return TF_TRANSFORM_OK;
}

static tf_transform_code finalize_numeric_column(
    const tf_transform_recipe_column *recipe,
    const tf_transform_running_stats *stats,
    double median_value, int has_median_value,
    tf_transform_numeric_state *state, tf_transform_error **error) {
    uint64_t logical_count = stats->observed;
    double mean = stats->mean;
    double m2 = stats->m2;
    double minimum = stats->minimum;
    double maximum = stats->maximum;
    int has_value = stats->has_value;
    double impute_value = 0.0;
    int has_impute = recipe->impute != TF_TRANSFORM_IMPUTE_NONE;
    tf_transform_code code;

    memset(state, 0, sizeof(*state));
    state->impute = recipe->impute;
    state->all_missing = recipe->all_missing;
    state->normalize = recipe->normalize;
    state->ddof = recipe->ddof;
    if (stats->observed == 0 && stats->missing == 0
        && (recipe->impute == TF_TRANSFORM_IMPUTE_MEAN
            || recipe->impute == TF_TRANSFORM_IMPUTE_MEDIAN))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INSUFFICIENT_DATA,
            "learned imputation requires at least one analyzed row");
    if (recipe->impute == TF_TRANSFORM_IMPUTE_ZERO)
        impute_value = 0.0;
    else if (recipe->impute == TF_TRANSFORM_IMPUTE_CONSTANT)
        impute_value = recipe->constant;
    else if (recipe->impute == TF_TRANSFORM_IMPUTE_MEAN) {
        if (stats->observed != 0) impute_value = stats->mean;
        else if (recipe->all_missing == TF_TRANSFORM_ALL_MISSING_ZERO)
            impute_value = 0.0;
        else return tf_transform_set_error(
            error, TF_TRANSFORM_INSUFFICIENT_DATA,
            "mean imputation has no observed values");
    } else if (recipe->impute == TF_TRANSFORM_IMPUTE_MEDIAN) {
        if (stats->observed != 0) {
            if (!has_median_value)
                return tf_transform_set_error(
                    error, TF_TRANSFORM_INTERNAL,
                    "median imputation value is unavailable");
            impute_value = median_value;
        } else if (recipe->all_missing == TF_TRANSFORM_ALL_MISSING_ZERO) {
            impute_value = 0.0;
        } else {
            return tf_transform_set_error(
                error, TF_TRANSFORM_INSUFFICIENT_DATA,
                "median imputation has no observed values");
        }
    }
    if (has_impute) {
        state->impute_value = impute_value;
        state->has_impute_value = 1;
        if (stats->missing != 0) {
            if (stats->observed == 0) {
                logical_count = stats->missing;
                mean = impute_value;
                m2 = 0.0;
                minimum = impute_value;
                maximum = impute_value;
                has_value = 1;
            } else {
                uint64_t total;
                double delta;
                double nm;
                double weight;
                double delta2;
                double cross;
                double missing_weight;
                double merged_mean;
                double merged_m2;
                if (stats->missing > UINT64_MAX - stats->observed)
                    return tf_transform_set_error(
                        error, TF_TRANSFORM_RESOURCE_LIMIT,
                        "logical value count overflows");
                total = stats->observed + stats->missing;
                delta = impute_value - stats->mean;
                nm = (double)stats->observed * (double)stats->missing;
                weight = nm / (double)total;
                delta2 = delta * delta;
                cross = delta2 * weight;
                missing_weight = (double)stats->missing / (double)total;
                merged_mean = stats->mean + delta * missing_weight;
                merged_m2 = stats->m2 + cross;
                if (!tf_transform_double_is_finite(delta)
                    || !tf_transform_double_is_finite(nm)
                    || !tf_transform_double_is_finite(weight)
                    || !tf_transform_double_is_finite(delta2)
                    || !tf_transform_double_is_finite(cross)
                    || !tf_transform_double_is_finite(missing_weight)
                    || !tf_transform_double_is_finite(merged_mean)
                    || !tf_transform_double_is_finite(merged_m2))
                    return tf_transform_set_error(
                        error, TF_TRANSFORM_NUMERIC_DOMAIN,
                        "imputed analyzer state became nonfinite");
                logical_count = total;
                mean = merged_mean;
                m2 = merged_m2;
                if (impute_value < minimum) minimum = impute_value;
                if (impute_value > maximum) maximum = impute_value;
            }
        }
    }
    if (recipe->normalize == TF_TRANSFORM_NORMALIZE_NONE) {
        state->location = 0.0;
        state->scale = 1.0;
        return TF_TRANSFORM_OK;
    }
    if (!has_value || logical_count == 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INSUFFICIENT_DATA,
            "normalization has no logical values");
    if (recipe->normalize == TF_TRANSFORM_NORMALIZE_MINMAX) {
        double range = maximum - minimum;
        code = finite_or_domain(
            range, error, "min-max range became nonfinite");
        if (code != TF_TRANSFORM_OK) return code;
        state->location = minimum;
        state->scale = range == 0.0 ? 1.0 : range;
        return TF_TRANSFORM_OK;
    }
    if (logical_count <= (uint64_t)recipe->ddof)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INSUFFICIENT_DATA,
            "standard normalization has too few logical values");
    if (m2 < 0.0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_NUMERIC_DOMAIN,
            "standard normalization has negative M2");
    {
        double variance = m2 / (double)(logical_count - (uint64_t)recipe->ddof);
        double scale;
        code = finite_or_domain(
            variance, error, "standard variance became nonfinite");
        if (code != TF_TRANSFORM_OK) return code;
        scale = variance == 0.0 ? 1.0 : tf_transform_sqrt_f64_rne(variance);
        if (!tf_transform_double_is_finite(scale) || scale <= 0.0)
            return tf_transform_set_error(
                error, TF_TRANSFORM_NUMERIC_DOMAIN,
                "standard scale is not positive and finite");
        state->location = mean;
        state->scale = scale;
    }
    return TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_analyzer_finalize(
    tf_transform_analyzer *analyzer, tf_transform_plan **out,
    tf_transform_error **error) {
    tf_transform_plan *plan = NULL;
    tf_transform_fp_guard guard;
    tf_transform_code code;
    size_t state_bytes = 0;
    uint64_t schema_resident = 0;
    uint64_t schema_allocations = 0;
    uint64_t output_schema_resident = 0;
    uint64_t output_schema_allocations = 0;
    uint64_t output_collision_bytes = 0;
    uint64_t category_plan_resident = 0;
    uint64_t category_plan_allocations = 0;
    uint64_t total_plan_categories = 0;
    uint64_t plan_base_resident;
    uint64_t plan_resident;
    uint64_t plan_allocations;

    if (out) *out = NULL;
    tf_transform_clear_error(error);
    if (!analyzer)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "analyzer finalize argument is null");
    if (analyzer->state != TF_ANALYZER_ACTIVE)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_STATE, "analyzer is not active");
    if (!out) {
        analyzer->state = TF_ANALYZER_FAILED;
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "plan output is null");
    }
    code = tf_transform_fp_begin(&guard, error);
    if (code != TF_TRANSFORM_OK) goto failed;
    analyzer->runtime.fp_guard_active = 1;
    code = poll_and_recheck(&analyzer->runtime, error);
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    code = checked_mul_size(
        analyzer->input_schema.field_count, sizeof(*plan->states), &state_bytes,
        error, "plan state byte count overflows");
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    code = schema_owned_metrics(
        &analyzer->input_schema, &schema_resident, &schema_allocations,
        &analyzer->runtime, 1, error);
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    code = tf_transform_output_schema_requirements(
        &analyzer->input_schema, analyzer->recipe, &analyzer->runtime,
        &output_schema_resident, &output_schema_allocations,
        &output_collision_bytes, error);
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    for (size_t i = 0; i < analyzer->input_schema.field_count; ++i) {
        uint64_t resident = 0;
        uint64_t allocations = 0;
        uint64_t count;
        if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            code = poll_and_recheck(&analyzer->runtime, error);
            if (code != TF_TRANSFORM_OK) goto guarded_failed;
        }
        if (analyzer->recipe->columns[i].kind
                != TF_TRANSFORM_KIND_CATEGORICAL)
            continue;
        code = tf_transform_category_check_observed(
            analyzer, i, analyzer->stats[i].observed, error);
        if (code != TF_TRANSFORM_OK) goto guarded_failed;
        code = tf_transform_category_plan_requirements(
            analyzer, i, &resident, &allocations, error);
        if (code != TF_TRANSFORM_OK) goto guarded_failed;
        count = resident / sizeof(tf_transform_category_value);
        if (category_plan_resident > UINT64_MAX - resident
            || category_plan_allocations > UINT64_MAX - allocations
            || total_plan_categories > UINT64_MAX - count) {
            code = tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "categorical plan resource count overflows");
            goto guarded_failed;
        }
        category_plan_resident += resident;
        category_plan_allocations += allocations;
        total_plan_categories += count;
    }
    if (total_plan_categories
            > analyzer->runtime.limits.max_total_categories) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "categorical plan exceeds the total category limit");
        goto guarded_failed;
    }
    if (schema_resident > UINT64_MAX - sizeof(*plan)
        || output_schema_resident > UINT64_MAX - sizeof(*plan)
            - schema_resident) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "plan construction resource count overflows");
        goto guarded_failed;
    }
    plan_base_resident = sizeof(*plan)
        + schema_resident + output_schema_resident;
    if ((uint64_t)state_bytes > UINT64_MAX - plan_base_resident
        || category_plan_resident > UINT64_MAX
            - plan_base_resident - (uint64_t)state_bytes
        || output_collision_bytes > UINT64_MAX - plan_base_resident
        || category_plan_allocations > UINT64_MAX - 2
        || schema_allocations > UINT64_MAX - 2 - category_plan_allocations
        || output_schema_allocations > UINT64_MAX - 2
            - category_plan_allocations - schema_allocations) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "plan construction resource count overflows");
        goto guarded_failed;
    }
    plan_resident = plan_base_resident
        + (uint64_t)state_bytes + category_plan_resident;
    if (plan_base_resident + output_collision_bytes > plan_resident)
        plan_resident = plan_base_resident + output_collision_bytes;
    plan_allocations = 2 + schema_allocations + output_schema_allocations
        + category_plan_allocations;
    code = session_check_totals(
        &analyzer->runtime, analyzer->resident_state_bytes, plan_resident,
        analyzer->allocation_count, plan_allocations, error);
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    plan = (tf_transform_plan *)calloc(1, sizeof(*plan));
    if (!plan) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION, "plan allocation failed");
        goto guarded_failed;
    }
    atomic_init(&plan->refcount, 1u);
    plan->recipe = analyzer->recipe;
    tf_transform_recipe_retain(plan->recipe);
    code = tf_transform_schema_clone_runtime(
        &analyzer->input_schema, &analyzer->runtime,
        &plan->input_schema, error);
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    code = tf_transform_output_schema_build(
        &analyzer->input_schema, analyzer->recipe, &analyzer->runtime,
        &plan->output_schema, error);
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    code = tf_transform_output_schema_validate_contract(
        &plan->input_schema, plan->recipe, &plan->output_schema,
        &analyzer->runtime, NULL, TF_TRANSFORM_SCHEMA_MISMATCH, error);
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    if ((uint64_t)state_bytes > analyzer->runtime.limits.max_resident_state_bytes
        || (uint64_t)state_bytes > analyzer->runtime.limits.max_allocation_bytes) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "plan state exceeds limits");
        goto guarded_failed;
    }
    code = tf_transform_poll_cancel(&analyzer->runtime, error);
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    plan->states = (tf_transform_column_state *)calloc(
        plan->input_schema.field_count, sizeof(*plan->states));
    if (!plan->states) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION, "plan state allocation failed");
        goto guarded_failed;
    }
    for (size_t i = 0; i < plan->input_schema.field_count; ++i) {
        double median_value = 0.0;
        int has_median_value = 0;
        if (i != 0 && i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            code = poll_and_recheck(&analyzer->runtime, error);
            if (code != TF_TRANSFORM_OK) goto guarded_failed;
        }
        if (plan->recipe->columns[i].kind == TF_TRANSFORM_KIND_CATEGORICAL) {
            plan->states[i].kind = TF_TRANSFORM_KIND_CATEGORICAL;
            code = tf_transform_category_finalize(
                analyzer, i, &plan->states[i].value.categorical, error);
            if (code != TF_TRANSFORM_OK) goto guarded_failed;
            continue;
        }
        plan->states[i].kind = TF_TRANSFORM_KIND_NUMERIC;
        if (plan->recipe->columns[i].impute == TF_TRANSFORM_IMPUTE_MEDIAN
            && analyzer->stats[i].observed != 0) {
            tf_transform_median_store *store = analyzer->median_stores
                ? &analyzer->median_stores[i] : NULL;
            if (!store || store->count != analyzer->stats[i].observed) {
                code = tf_transform_set_error(
                    error, TF_TRANSFORM_INTERNAL,
                    "median retained state does not match finalized statistics");
                goto guarded_failed;
            }
            code = median_store_resolve(
                store, &analyzer->runtime, &median_value, error);
            if (code != TF_TRANSFORM_OK) goto guarded_failed;
            has_median_value = 1;
        }
        code = finalize_numeric_column(
            &plan->recipe->columns[i], &analyzer->stats[i],
            median_value, has_median_value,
            &plan->states[i].value.numeric, error);
        if (code != TF_TRANSFORM_OK) goto guarded_failed;
    }
    analyzer->runtime.fp_guard_active = 0;
    tf_transform_fp_end(&guard);
    analyzer->allocation_count += plan_allocations;
    analyzer->state = TF_ANALYZER_FINALIZED;
    *out = plan;
    return TF_TRANSFORM_OK;
guarded_failed:
    analyzer->runtime.fp_guard_active = 0;
    tf_transform_fp_end(&guard);
failed:
    tf_transform_plan_release(plan);
    analyzer->state = TF_ANALYZER_FAILED;
    return code;
}

tf_transform_code tf_transform_apply_create(
    const tf_transform_plan *plan, const tf_schema_view_v1 *runtime_schema,
    const tf_transform_runtime_v1 *runtime,
    tf_transform_apply **out, tf_transform_error **error) {
    tf_transform_apply *created;
    tf_transform_runtime_copy runtime_copy;
    tf_transform_code code;
    int schema_equal = 0;

    if (out) *out = NULL;
    tf_transform_clear_error(error);
    if (!plan || !runtime_schema || !out)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "apply create argument is null");
    code = tf_transform_copy_runtime(runtime, &runtime_copy, error);
    if (code != TF_TRANSFORM_OK) return code;
    code = tf_transform_poll_cancel(&runtime_copy, error);
    if (code != TF_TRANSFORM_OK) return code;
    code = tf_transform_schema_equal_view_runtime(
        &plan->input_schema, runtime_schema, &runtime_copy,
        &schema_equal, error);
    if (code != TF_TRANSFORM_OK) return code;
    if (!schema_equal)
        return tf_transform_set_error(
            error, TF_TRANSFORM_SCHEMA_MISMATCH,
            "runtime schema does not match fitted input schema");
    code = session_check_totals(
        &runtime_copy, 0, sizeof(*created), 0, 1, error);
    if (code != TF_TRANSFORM_OK) return code;
    created = (tf_transform_apply *)calloc(1, sizeof(*created));
    if (!created) return tf_transform_set_error(
        error, TF_TRANSFORM_ALLOCATION, "apply session allocation failed");
    created->plan = (tf_transform_plan *)plan;
    tf_transform_plan_retain(created->plan);
    created->runtime = runtime_copy;
    created->allocation_count = 1;
    created->resident_state_bytes = sizeof(*created);
    created->state = TF_APPLY_READY;
    *out = created;
    return TF_TRANSFORM_OK;
}

void tf_transform_apply_destroy(tf_transform_apply **apply) {
    if (!apply || !*apply) return;
    tf_transform_plan_release((*apply)->plan);
    free(*apply);
    *apply = NULL;
}

tf_transform_code tf_transform_apply_run(
    tf_transform_apply *apply, const tf_table_view_v1 *table,
    tf_owned_dense_v1 *out, tf_transform_error **error) {
    tf_owned_dense_v1 pending;
    tf_transform_fp_guard guard;
    tf_transform_code code;
    size_t input_bytes = 0;
    size_t elements = 0;
    size_t data_bytes = 0;
    uint64_t new_rows = 0;
    uint64_t new_input_bytes = 0;

    if (out) memset(out, 0, sizeof(*out));
    memset(&pending, 0, sizeof(pending));
    tf_transform_clear_error(error);
    if (!apply)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "apply run argument is null");
    if (apply->state != TF_APPLY_READY)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_STATE, "apply session is not ready");
    if (!table || !out) {
        apply->state = TF_APPLY_FAILED;
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "apply table or output is null");
    }
    code = tf_transform_fp_begin(&guard, error);
    if (code != TF_TRANSFORM_OK) goto failed;
    apply->runtime.fp_guard_active = 1;
    code = poll_and_recheck(&apply->runtime, error);
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    code = validate_table_view(
        &apply->plan->input_schema, table, apply->runtime.limits.max_apply_rows,
        apply->runtime.limits.max_apply_input_bytes, &input_bytes,
        &apply->runtime, error);
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    code = checked_add_u64(
        apply->total_rows, (uint64_t)table->row_count, &new_rows,
        error, "apply row counter overflows");
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    code = checked_add_u64(
        apply->total_input_bytes, (uint64_t)input_bytes, &new_input_bytes,
        error, "apply input byte counter overflows");
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    if (new_rows > apply->runtime.limits.max_apply_rows
        || new_input_bytes > apply->runtime.limits.max_apply_input_bytes) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "cumulative apply input exceeds limits");
        goto guarded_failed;
    }
    code = checked_mul_size(
        table->row_count, apply->plan->output_schema.field_count, &elements,
        error, "output element count overflows");
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    if ((uint64_t)elements > apply->plan->recipe->max_output_elements_per_apply
        || (uint64_t)elements
            > apply->runtime.limits.max_output_elements_per_call)
    {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "output element count exceeds limits");
        goto guarded_failed;
    }
    code = checked_mul_size(
        elements, sizeof(double), &data_bytes,
        error, "output byte count overflows");
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    if ((uint64_t)data_bytes > apply->runtime.limits.max_allocation_bytes) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "output allocation exceeds limit");
        goto guarded_failed;
    }
    pending.abi_version = 1;
    pending.struct_size = (uint32_t)sizeof(pending);
    pending.dtype = TF_VIEW_FLOAT64;
    pending.rows = table->row_count;
    pending.columns = apply->plan->output_schema.field_count;
    pending.data_bytes = data_bytes;
    if (data_bytes != 0) {
        if (apply->allocation_count
            >= apply->runtime.limits.max_allocations_per_session) {
            code = tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "apply session allocation count exceeds limit");
            goto guarded_failed;
        }
        code = poll_and_recheck(&apply->runtime, error);
        if (code != TF_TRANSFORM_OK) goto guarded_failed;
        pending.data = malloc(data_bytes);
        if (!pending.data) {
            code = tf_transform_set_error(
                error, TF_TRANSFORM_ALLOCATION, "dense output allocation failed");
            goto guarded_failed;
        }
        ++apply->allocation_count;
    }
    if (table->row_count != 0 && !pending.data) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL, "nonempty output has no allocation");
        goto guarded_failed;
    }
    code = poll_and_recheck(&apply->runtime, error);
    if (code != TF_TRANSFORM_OK) goto guarded_failed;
    for (size_t row = 0; row < table->row_count; ++row) {
        size_t element_poll_rows = TF_TRANSFORM_CANCEL_ELEMENTS_V1
            / apply->plan->output_schema.field_count;
        if (element_poll_rows == 0) element_poll_rows = 1;
        if (row != 0 && (row % TF_TRANSFORM_CANCEL_ROWS_V1 == 0
            || row % element_poll_rows == 0)) {
            code = poll_and_recheck(&apply->runtime, error);
            if (code != TF_TRANSFORM_OK) goto guarded_failed;
        }
        for (size_t column_index = 0;
             column_index < apply->plan->input_schema.field_count; ++column_index) {
            const tf_column_view_v1 *column = &table->columns[column_index];
            const tf_transform_column_state *column_state
                = &apply->plan->states[column_index];
            const tf_transform_numeric_state *state;
            double value;
            double centered;
            double transformed;
            size_t output_index = row * pending.columns + column_index;
            int missing = !column_value_is_valid(column, row);
            if (column_index != 0
                && column_index % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
                code = poll_and_recheck(&apply->runtime, error);
                if (code != TF_TRANSFORM_OK) goto guarded_failed;
            }
            if (!missing) {
                value = read_numeric_value(
                    column, row, apply->plan->input_schema.fields[column_index].dtype);
                missing = tf_transform_double_is_nan(value);
                if (!missing && !tf_transform_double_is_finite(value)) {
                    code = tf_transform_set_error(
                        error, TF_TRANSFORM_NUMERIC_DOMAIN,
                        "infinite apply input is not supported");
                    goto guarded_failed;
                }
            }
            if (column_state->kind == TF_TRANSFORM_KIND_CATEGORICAL) {
                const tf_transform_categorical_state *categorical
                    = &column_state->value.categorical;
                uint64_t bits;
                size_t ordinal = 0;
                int known = 0;
                if (missing) {
                    if (!categorical->has_impute_value) {
                        code = tf_transform_set_error(
                            error, TF_TRANSFORM_INTERNAL,
                            "categorical plan has no mode value");
                        goto guarded_failed;
                    }
                    bits = categorical->impute_bits;
                    value = tf_transform_category_decode(
                        categorical->impute_bits, categorical->source_dtype);
                } else {
                    code = tf_transform_category_key(
                        value, categorical->source_dtype, &bits, error);
                    if (code != TF_TRANSFORM_OK) goto guarded_failed;
                }
                code = tf_transform_category_lookup(
                    categorical, bits, &apply->runtime,
                    &ordinal, &known, error);
                if (code != TF_TRANSFORM_OK) goto guarded_failed;
                if (known && categorical->encode == TF_TRANSFORM_ENCODE_LABEL) {
                    ((double *)pending.data)[output_index] = (double)ordinal;
                    continue;
                }
                if (!known) {
                    if (categorical->encode == TF_TRANSFORM_ENCODE_LABEL
                        && categorical->unknown == TF_TRANSFORM_UNKNOWN_SENTINEL
                        && categorical->has_sentinel_label) {
                        ((double *)pending.data)[output_index]
                            = (double)categorical->sentinel_label;
                        continue;
                    }
                    if (categorical->encode == TF_TRANSFORM_ENCODE_LABEL
                        && categorical->unknown == TF_TRANSFORM_UNKNOWN_OTHER
                        && categorical->has_other_ordinal) {
                        ((double *)pending.data)[output_index]
                            = (double)categorical->other_ordinal;
                        continue;
                    }
                    code = tf_transform_set_error(
                        error, TF_TRANSFORM_UNKNOWN_CATEGORY,
                        "apply input contains an unknown category");
                    goto guarded_failed;
                }
                if (categorical->encode != TF_TRANSFORM_ENCODE_NONE) {
                    code = tf_transform_set_error(
                        error, TF_TRANSFORM_INTERNAL,
                        "categorical encoding state is unsupported");
                    goto guarded_failed;
                }
                ((double *)pending.data)[output_index] = value;
                continue;
            }
            state = &column_state->value.numeric;
            if (missing && !state->has_impute_value) {
                ((double *)pending.data)[output_index] =
                    tf_transform_double_from_bits(UINT64_C(0x7ff8000000000000));
                continue;
            }
            if (missing) value = state->impute_value;
            centered = value - state->location;
            transformed = centered / state->scale;
            if (!tf_transform_double_is_finite(centered)
                || !tf_transform_double_is_finite(transformed)) {
                code = tf_transform_set_error(
                    error, TF_TRANSFORM_NUMERIC_DOMAIN,
                    "numeric transform output became nonfinite");
                goto guarded_failed;
            }
            ((double *)pending.data)[output_index] = transformed;
        }
    }
    apply->runtime.fp_guard_active = 0;
    tf_transform_fp_end(&guard);
    apply->total_rows = new_rows;
    apply->total_input_bytes = new_input_bytes;
    *out = pending;
    return TF_TRANSFORM_OK;
guarded_failed:
    apply->runtime.fp_guard_active = 0;
    tf_transform_fp_end(&guard);
failed:
    free(pending.data);
    apply->state = TF_APPLY_FAILED;
    return code;
}
