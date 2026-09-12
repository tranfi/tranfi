#include "transform_internal.h"

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

typedef struct tf_transform_category_slot {
    uint64_t bits;
    uint64_t count;
    int occupied;
} tf_transform_category_slot;

struct tf_transform_category_store {
    tf_transform_category_slot *slots;
    size_t capacity;
    size_t category_count;
    uint64_t observed;
    uint32_t dtype;
    int inference_numeric;
};

static tf_transform_code category_poll(
    const tf_transform_runtime_copy *runtime, tf_transform_error **error) {
    tf_transform_code code = tf_transform_poll_cancel(runtime, error);
    if (code != TF_TRANSFORM_OK) return code;
    return tf_transform_check_runtime_fp(error);
}

static tf_transform_code fixed_category_known(
    const tf_transform_recipe_column *recipe, uint64_t bits,
    const tf_transform_runtime_copy *runtime, int *known,
    tf_transform_error **error) {
    /* Borrow immutable recipe storage; lookup never owns or modifies it. */
    tf_transform_categorical_state dictionary = {0};
    size_t ordinal = 0;
    dictionary.categories = recipe->categorical_fixed;
    dictionary.category_count = recipe->categorical_fixed_count;
    dictionary.source_dtype = recipe->categorical_fixed_dtype;
    return tf_transform_category_lookup(
        &dictionary, bits, runtime, &ordinal, known, error);
}

static uint64_t category_hash(uint64_t value) {
    value ^= value >> 30;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value *= UINT64_C(0x94d049bb133111eb);
    value ^= value >> 31;
    return value;
}

tf_transform_code tf_transform_category_key(
    double value, uint32_t dtype, uint64_t *out, tf_transform_error **error) {
    if (!out || (dtype != TF_VIEW_FLOAT32 && dtype != TF_VIEW_FLOAT64))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "invalid categorical value type");
    if (!tf_transform_double_is_finite(value))
        return tf_transform_set_error(
            error, TF_TRANSFORM_NUMERIC_DOMAIN,
            "categorical value is not finite");
    if (value == 0.0) {
        *out = 0;
        return TF_TRANSFORM_OK;
    }
    if (dtype == TF_VIEW_FLOAT32) {
        float narrowed = (float)value;
        uint32_t bits;
        memcpy(&bits, &narrowed, sizeof(bits));
        *out = bits;
    } else {
        *out = tf_transform_double_bits(value);
    }
    return TF_TRANSFORM_OK;
}

double tf_transform_category_decode(uint64_t bits, uint32_t dtype) {
    if (dtype == TF_VIEW_FLOAT32) {
        uint32_t narrowed_bits = (uint32_t)bits;
        float narrowed;
        memcpy(&narrowed, &narrowed_bits, sizeof(narrowed));
        return (double)narrowed;
    }
    return tf_transform_double_from_bits(bits);
}

int tf_transform_category_compare(uint64_t left, uint64_t right, uint32_t dtype) {
    uint64_t sign;
    uint64_t mask;
    uint64_t left_key;
    uint64_t right_key;
    if (dtype == TF_VIEW_FLOAT32) {
        sign = UINT64_C(0x80000000);
        mask = UINT64_C(0xffffffff);
    } else {
        sign = UINT64_C(0x8000000000000000);
        mask = UINT64_MAX;
    }
    left &= mask;
    right &= mask;
    left_key = (left & sign) ? (~left & mask) : (left ^ sign);
    right_key = (right & sign) ? (~right & mask) : (right ^ sign);
    if (left_key < right_key) return -1;
    if (left_key > right_key) return 1;
    return 0;
}

tf_transform_code tf_transform_category_stores_requirements(
    size_t column_count, uint64_t *resident_bytes,
    uint64_t *allocation_count, tf_transform_error **error) {
    if (!resident_bytes || !allocation_count || column_count == 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical store requirements are invalid");
    if (column_count > SIZE_MAX / sizeof(tf_transform_category_store))
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "categorical store size overflows");
    *resident_bytes = (uint64_t)(
        column_count * sizeof(tf_transform_category_store));
    *allocation_count = 1;
    return TF_TRANSFORM_OK;
}

static void category_store_clear(tf_transform_category_store *store) {
    if (!store) return;
    free(store->slots);
    memset(store, 0, sizeof(*store));
}

void tf_transform_category_stores_clear(tf_transform_analyzer *analyzer) {
    if (!analyzer || !analyzer->category_stores) return;
    for (size_t i = 0; i < analyzer->category_stores_initialized; ++i)
        category_store_clear(&analyzer->category_stores[i]);
    free(analyzer->category_stores);
    analyzer->category_stores = NULL;
    analyzer->category_stores_initialized = 0;
    analyzer->total_categories = 0;
}

tf_transform_code tf_transform_category_stores_init(
    tf_transform_analyzer *analyzer, tf_transform_error **error) {
    size_t bytes;
    void *memory = NULL;
    tf_transform_code code;
    if (!analyzer || analyzer->input_schema.field_count == 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical store initialization is invalid");
    if (analyzer->input_schema.field_count
            > SIZE_MAX / sizeof(*analyzer->category_stores))
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "categorical store size overflows");
    bytes = analyzer->input_schema.field_count
        * sizeof(*analyzer->category_stores);
    code = tf_transform_analyzer_allocate_retained(
        analyzer, bytes, &memory, error);
    if (code != TF_TRANSFORM_OK) return code;
    analyzer->category_stores = (tf_transform_category_store *)memory;
    for (size_t i = 0; i < analyzer->input_schema.field_count; ++i) {
        if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            code = category_poll(&analyzer->runtime, error);
            if (code != TF_TRANSFORM_OK) return code;
        }
        memset(&analyzer->category_stores[i], 0,
               sizeof(analyzer->category_stores[i]));
        analyzer->category_stores_initialized = i + 1;
        if (analyzer->recipe->columns[i].kind
                != TF_TRANSFORM_KIND_NUMERIC)
            analyzer->category_stores[i].dtype
                = analyzer->input_schema.fields[i].dtype;
    }
    return category_poll(&analyzer->runtime, error);
}

static tf_transform_code category_slots_zero(
    tf_transform_category_slot *slots, size_t capacity,
    const tf_transform_runtime_copy *runtime,
    tf_transform_error **error) {
    for (size_t i = 0; i < capacity; ++i) {
        if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            tf_transform_code code = category_poll(runtime, error);
            if (code != TF_TRANSFORM_OK) return code;
        }
        memset(&slots[i], 0, sizeof(slots[i]));
    }
    return category_poll(runtime, error);
}

static tf_transform_code category_find_slot(
    const tf_transform_category_store *store, uint64_t bits,
    const tf_transform_runtime_copy *runtime,
    size_t *slot_index, int *found, tf_transform_error **error) {
    size_t mask;
    size_t index;
    if (slot_index) *slot_index = 0;
    if (found) *found = 0;
    if (!store || !store->slots || store->capacity == 0
        || (store->capacity & (store->capacity - 1)) != 0
        || !slot_index || !found)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical hash table is invalid");
    mask = store->capacity - 1;
    index = (size_t)(category_hash(bits) & (uint64_t)mask);
    for (size_t probe = 0; probe < store->capacity; ++probe) {
        const tf_transform_category_slot *slot;
        if (probe % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            tf_transform_code code = category_poll(runtime, error);
            if (code != TF_TRANSFORM_OK) return code;
        }
        slot = &store->slots[index];
        if (!slot->occupied) {
            *slot_index = index;
            *found = 0;
            return TF_TRANSFORM_OK;
        }
        if (slot->bits == bits) {
            *slot_index = index;
            *found = 1;
            return TF_TRANSFORM_OK;
        }
        index = (index + 1) & mask;
    }
    return tf_transform_set_error(
        error, TF_TRANSFORM_INTERNAL,
        "categorical hash table has no free slot");
}

static tf_transform_code category_store_resize(
    tf_transform_analyzer *analyzer, tf_transform_category_store *store,
    size_t capacity, tf_transform_error **error) {
    tf_transform_category_slot *replacement = NULL;
    tf_transform_category_slot *previous;
    size_t previous_capacity;
    size_t bytes;
    size_t previous_bytes;
    tf_transform_code code;
    if (!store || ((store->slots == NULL) != (store->capacity == 0)))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical hash storage is inconsistent");
    if (capacity == 0 || (capacity & (capacity - 1)) != 0
        || capacity > SIZE_MAX / sizeof(*replacement))
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "categorical hash capacity overflows");
    bytes = capacity * sizeof(*replacement);
    code = tf_transform_analyzer_allocate_retained(
        analyzer, bytes, (void **)&replacement, error);
    if (code != TF_TRANSFORM_OK) return code;
    code = category_slots_zero(
        replacement, capacity, &analyzer->runtime, error);
    if (code != TF_TRANSFORM_OK) goto failed;
    previous = store->slots;
    previous_capacity = store->capacity;
    previous_bytes = previous_capacity * sizeof(*previous);
    store->slots = replacement;
    store->capacity = capacity;
    for (size_t i = 0; i < previous_capacity; ++i) {
        size_t target = 0;
        int found = 0;
        if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            code = category_poll(&analyzer->runtime, error);
            if (code != TF_TRANSFORM_OK) {
                store->slots = previous;
                store->capacity = previous_capacity;
                goto failed;
            }
        }
        if (!previous[i].occupied) continue;
        code = category_find_slot(
            store, previous[i].bits, &analyzer->runtime,
            &target, &found, error);
        if (code != TF_TRANSFORM_OK || found) {
            if (code == TF_TRANSFORM_OK)
                code = tf_transform_set_error(
                    error, TF_TRANSFORM_INTERNAL,
                    "categorical rehash found a duplicate key");
            store->slots = previous;
            store->capacity = previous_capacity;
            goto failed;
        }
        store->slots[target] = previous[i];
    }
    free(previous);
    analyzer->resident_state_bytes -= (uint64_t)previous_bytes;
    return category_poll(&analyzer->runtime, error);
failed:
    free(replacement);
    analyzer->resident_state_bytes -= (uint64_t)bytes;
    return code;
}

tf_transform_code tf_transform_category_observe(
    tf_transform_analyzer *analyzer, size_t column_index,
    double value, uint32_t dtype, tf_transform_error **error) {
    tf_transform_category_store *store;
    uint64_t bits = 0;
    size_t slot_index = 0;
    int found = 0;
    tf_transform_code code;
    if (!analyzer || !analyzer->category_stores
        || column_index >= analyzer->input_schema.field_count)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical analyzer state is unavailable");
    store = &analyzer->category_stores[column_index];
    if (store->dtype != dtype)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical analyzer dtype drifted");
    if ((store->slots == NULL) != (store->capacity == 0)
        || store->category_count > store->capacity / 2)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical analyzer hash state is inconsistent");
    code = tf_transform_category_key(value, dtype, &bits, error);
    if (code != TF_TRANSFORM_OK) return code;
    const tf_transform_recipe_column *recipe = &analyzer->recipe->columns[column_index];
    if (recipe->categorical_fixed_count) {
        int known = 0;
        code = fixed_category_known(recipe, bits, &analyzer->runtime, &known, error);
        if (code != TF_TRANSFORM_OK) return code;
        if (!known) {
            if (recipe->categorical_unknown == TF_TRANSFORM_UNKNOWN_ERROR
                || recipe->categorical_unknown == TF_TRANSFORM_UNKNOWN_NONE)
                return tf_transform_set_error(error, TF_TRANSFORM_UNKNOWN_CATEGORY,
                    "analyzed value is outside the fixed dictionary");
            if (store->observed == UINT64_MAX)
                return tf_transform_set_error(error, TF_TRANSFORM_RESOURCE_LIMIT,
                    "categorical observed count overflows");
            /* Accepted unknowns count as observations, but never as mode votes. */
            ++store->observed;
            return TF_TRANSFORM_OK;
        }
        if (recipe->categorical_impute == TF_TRANSFORM_CATEGORICAL_IMPUTE_NONE) {
            /* The dictionary is already frozen; only mode needs learned counts. */
            if (store->observed == UINT64_MAX)
                return tf_transform_set_error(error, TF_TRANSFORM_RESOURCE_LIMIT,
                    "categorical observed count overflows");
            ++store->observed;
            return TF_TRANSFORM_OK;
        }
    }
    if (store->slots) {
        code = category_find_slot(
            store, bits, &analyzer->runtime,
            &slot_index, &found, error);
        if (code != TF_TRANSFORM_OK) return code;
        if (found) {
            if (store->slots[slot_index].count == UINT64_MAX
                || store->observed == UINT64_MAX)
                return tf_transform_set_error(
                    error, TF_TRANSFORM_RESOURCE_LIMIT,
                    "categorical count overflows");
            ++store->slots[slot_index].count;
            ++store->observed;
            return TF_TRANSFORM_OK;
        }
    }
    if ((uint64_t)store->category_count
            >= analyzer->runtime.limits.max_categories_per_column
        || analyzer->total_categories
            >= analyzer->runtime.limits.max_total_categories)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "categorical discovery exceeds category limits");
    if (!store->slots || (store->category_count + 1) > store->capacity / 2) {
        size_t next_capacity = store->capacity == 0 ? 16 : store->capacity;
        while ((store->category_count + 1) > next_capacity / 2) {
            if (next_capacity > SIZE_MAX / 2)
                return tf_transform_set_error(
                    error, TF_TRANSFORM_RESOURCE_LIMIT,
                    "categorical hash capacity overflows");
            next_capacity *= 2;
        }
        code = category_store_resize(analyzer, store, next_capacity, error);
        if (code != TF_TRANSFORM_OK) return code;
    }
    if (!store->slots || store->capacity == 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical analyzer hash storage is unavailable");
    code = category_find_slot(
        store, bits, &analyzer->runtime, &slot_index, &found, error);
    if (code != TF_TRANSFORM_OK) return code;
    if (found)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical key appeared during insertion");
    if (store->observed == UINT64_MAX)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "categorical observed count overflows");
    store->slots[slot_index].bits = bits;
    store->slots[slot_index].count = 1;
    store->slots[slot_index].occupied = 1;
    ++store->category_count;
    ++store->observed;
    ++analyzer->total_categories;
    return TF_TRANSFORM_OK;
}

static tf_transform_code category_inference_resolve_numeric(
    tf_transform_analyzer *analyzer, tf_transform_category_store *store,
    tf_transform_error **error) {
    uint64_t slot_bytes;
    uint32_t dtype;
    if (!analyzer || !store)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical inference state is unavailable");
    if (store->inference_numeric) return TF_TRANSFORM_OK;
    if (store->capacity > SIZE_MAX / sizeof(*store->slots))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical inference capacity is inconsistent");
    slot_bytes = (uint64_t)(store->capacity * sizeof(*store->slots));
    if (slot_bytes > analyzer->resident_state_bytes
        || (uint64_t)store->category_count > analyzer->total_categories)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical inference accounting is inconsistent");
    dtype = store->dtype;
    free(store->slots);
    analyzer->resident_state_bytes -= slot_bytes;
    analyzer->total_categories -= (uint64_t)store->category_count;
    memset(store, 0, sizeof(*store));
    store->dtype = dtype;
    store->inference_numeric = 1;
    return category_poll(&analyzer->runtime, error);
}

tf_transform_code tf_transform_category_infer_observe(
    tf_transform_analyzer *analyzer, size_t column_index,
    double value, uint32_t dtype, int is_integer, uint64_t max_categories,
    int *resolved_numeric, tf_transform_error **error) {
    tf_transform_category_store *store;
    uint64_t bits = 0;
    size_t slot_index = 0;
    int found = 0;
    tf_transform_code code;
    if (!analyzer || !analyzer->category_stores
        || column_index >= analyzer->input_schema.field_count
        || analyzer->recipe->columns[column_index].kind
            != TF_TRANSFORM_KIND_INFER
        || max_categories < 2 || !resolved_numeric)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical inference arguments are invalid");
    store = &analyzer->category_stores[column_index];
    *resolved_numeric = 0;
    if (store->dtype != dtype)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical inference dtype drifted");
    if (store->inference_numeric) {
        *resolved_numeric = 1;
        return TF_TRANSFORM_OK;
    }
    if (!is_integer) {
        code = category_inference_resolve_numeric(analyzer, store, error);
        if (code == TF_TRANSFORM_OK) *resolved_numeric = 1;
        return code;
    }
    code = tf_transform_category_key(value, dtype, &bits, error);
    if (code != TF_TRANSFORM_OK) return code;
    if (store->slots) {
        code = category_find_slot(
            store, bits, &analyzer->runtime, &slot_index, &found, error);
        if (code != TF_TRANSFORM_OK) return code;
    }
    if (found) {
        if (store->slots[slot_index].count == UINT64_MAX
            || store->observed == UINT64_MAX)
            return tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "categorical inference count overflows");
        ++store->slots[slot_index].count;
        ++store->observed;
        return TF_TRANSFORM_OK;
    }
    if ((uint64_t)store->category_count >= max_categories) {
        code = category_inference_resolve_numeric(analyzer, store, error);
        if (code == TF_TRANSFORM_OK) *resolved_numeric = 1;
        return code;
    }
    return tf_transform_category_observe(
        analyzer, column_index, value, dtype, error);
}

tf_transform_code tf_transform_analyzer_resolve_kind(
    const tf_transform_analyzer *analyzer, size_t column_index,
    tf_transform_column_kind *out, tf_transform_error **error) {
    const tf_transform_recipe_column *recipe;
    const tf_transform_category_store *store;
    if (!analyzer || !out
        || column_index >= analyzer->input_schema.field_count)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "column-kind resolution arguments are invalid");
    recipe = &analyzer->recipe->columns[column_index];
    if (recipe->kind == TF_TRANSFORM_KIND_NUMERIC
        || recipe->kind == TF_TRANSFORM_KIND_CATEGORICAL) {
        *out = recipe->kind;
        return TF_TRANSFORM_OK;
    }
    if (recipe->kind != TF_TRANSFORM_KIND_INFER
        || !analyzer->category_stores)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "column-kind inference state is unavailable");
    if (analyzer->total_rows == 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INSUFFICIENT_DATA,
            "kind inference requires at least one analyzed row");
    store = &analyzer->category_stores[column_index];
    if (store->inference_numeric || store->category_count < 2) {
        *out = TF_TRANSFORM_KIND_NUMERIC;
        return TF_TRANSFORM_OK;
    }
    if ((uint64_t)store->category_count > recipe->infer_max_categories)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical inference cardinality is inconsistent");
    *out = TF_TRANSFORM_KIND_CATEGORICAL;
    return TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_category_check_observed(
    const tf_transform_analyzer *analyzer, size_t column_index,
    uint64_t observed, tf_transform_error **error) {
    if (!analyzer || !analyzer->category_stores
        || column_index >= analyzer->input_schema.field_count)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical analyzer state is unavailable");
    if (analyzer->recipe->columns[column_index].kind
            == TF_TRANSFORM_KIND_CATEGORICAL
        && analyzer->recipe->columns[column_index].categorical_impute
            == TF_TRANSFORM_CATEGORICAL_IMPUTE_NONE
        && analyzer->recipe->columns[column_index].categorical_encode
            == TF_TRANSFORM_ENCODE_NONE)
        return TF_TRANSFORM_OK;
    if (analyzer->recipe->columns[column_index].kind == TF_TRANSFORM_KIND_INFER
        && analyzer->category_stores[column_index].inference_numeric)
        return TF_TRANSFORM_OK;
    if (analyzer->category_stores[column_index].observed != observed)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical retained state does not match analyzer statistics");
    return TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_category_plan_requirements(
    const tf_transform_analyzer *analyzer, size_t column_index,
    uint64_t *resident_bytes, uint64_t *allocation_count,
    tf_transform_error **error) {
    const tf_transform_category_store *store;
    const tf_transform_recipe_column *recipe;
    uint64_t count;
    if (!analyzer || !analyzer->category_stores
        || column_index >= analyzer->input_schema.field_count
        || !resident_bytes || !allocation_count)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical plan requirement arguments are invalid");
    store = &analyzer->category_stores[column_index];
    recipe = &analyzer->recipe->columns[column_index];
    if (recipe->kind == TF_TRANSFORM_KIND_CATEGORICAL
        && recipe->categorical_impute == TF_TRANSFORM_CATEGORICAL_IMPUTE_NONE
        && recipe->categorical_encode == TF_TRANSFORM_ENCODE_NONE) {
        *resident_bytes = 0;
        *allocation_count = 0;
        return TF_TRANSFORM_OK;
    }
    if (analyzer->total_rows == 0
        && !(recipe->categorical_fixed_count
             && recipe->categorical_impute == TF_TRANSFORM_CATEGORICAL_IMPUTE_NONE))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INSUFFICIENT_DATA,
            "categorical mode requires at least one analyzed row");
    count = recipe->categorical_fixed_count
        ? (uint64_t)recipe->categorical_fixed_count : (uint64_t)store->category_count;
    if (recipe->kind == TF_TRANSFORM_KIND_INFER
        && (count < 2 || count > recipe->infer_max_categories))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "inferred categorical cardinality is inconsistent");
    if (count == 0) {
        if (recipe->categorical_impute != TF_TRANSFORM_CATEGORICAL_IMPUTE_MODE
            || recipe->categorical_all_missing != TF_TRANSFORM_ALL_MISSING_ZERO)
            return tf_transform_set_error(
                error, TF_TRANSFORM_INSUFFICIENT_DATA,
                "categorical mode has no observed values");
        count = 1;
    }
    if (recipe->categorical_encode == TF_TRANSFORM_ENCODE_LABEL
        || recipe->categorical_encode == TF_TRANSFORM_ENCODE_ONEHOT) {
        if (count > (uint64_t)TF_TRANSFORM_MAX_SAFE_INTEGER_V1)
            return tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "categorical encoding count exceeds the safe-integer domain");
        if (recipe->categorical_encode == TF_TRANSFORM_ENCODE_LABEL
            && recipe->categorical_unknown == TF_TRANSFORM_UNKNOWN_SENTINEL
            && recipe->categorical_sentinel_label >= 0
            && (uint64_t)recipe->categorical_sentinel_label < count)
            return tf_transform_set_error(
                error, TF_TRANSFORM_INVALID_RECIPE,
                "categorical sentinel collides with a learned label");
    }
    if (count > analyzer->runtime.limits.max_categories_per_column
        || count > analyzer->runtime.limits.max_total_categories
        || count > SIZE_MAX / sizeof(tf_transform_category_value)
        || count * sizeof(tf_transform_category_value)
            > analyzer->runtime.limits.max_allocation_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "categorical plan state exceeds limits");
    *resident_bytes = count * sizeof(tf_transform_category_value);
    *allocation_count = 1;
    return TF_TRANSFORM_OK;
}

static tf_transform_code category_sort_tick(
    const tf_transform_runtime_copy *runtime, size_t *ticks,
    tf_transform_error **error) {
    ++*ticks;
    if (*ticks < TF_TRANSFORM_CANCEL_ITERS_V1) return TF_TRANSFORM_OK;
    *ticks = 0;
    return category_poll(runtime, error);
}

static tf_transform_code category_sift_down(
    tf_transform_category_value *values, size_t root, size_t end,
    uint32_t dtype, const tf_transform_runtime_copy *runtime,
    size_t *ticks, tf_transform_error **error) {
    if (end == 0) return TF_TRANSFORM_OK;
    while (root <= (end - 1) / 2) {
        size_t child = root * 2 + 1;
        size_t candidate = root;
        tf_transform_code code = category_sort_tick(runtime, ticks, error);
        if (code != TF_TRANSFORM_OK) return code;
        if (tf_transform_category_compare(
                values[candidate].bits, values[child].bits, dtype) < 0)
            candidate = child;
        if (child < end) {
            code = category_sort_tick(runtime, ticks, error);
            if (code != TF_TRANSFORM_OK) return code;
            if (tf_transform_category_compare(
                    values[candidate].bits, values[child + 1].bits, dtype) < 0)
                candidate = child + 1;
        }
        if (candidate == root) return TF_TRANSFORM_OK;
        {
            tf_transform_category_value temporary = values[root];
            values[root] = values[candidate];
            values[candidate] = temporary;
        }
        root = candidate;
    }
    return TF_TRANSFORM_OK;
}

static tf_transform_code category_sort(
    tf_transform_category_value *values, size_t count, uint32_t dtype,
    const tf_transform_runtime_copy *runtime, tf_transform_error **error) {
    size_t ticks = 0;
    tf_transform_code code = category_poll(runtime, error);
    if (code != TF_TRANSFORM_OK || count < 2) return code;
    for (size_t start = count / 2; start > 0; --start) {
        code = category_sift_down(
            values, start - 1, count - 1, dtype, runtime, &ticks, error);
        if (code != TF_TRANSFORM_OK) return code;
    }
    for (size_t end = count - 1; end > 0; --end) {
        tf_transform_category_value temporary = values[0];
        values[0] = values[end];
        values[end] = temporary;
        code = category_sift_down(
            values, 0, end - 1, dtype, runtime, &ticks, error);
        if (code != TF_TRANSFORM_OK) return code;
    }
    return category_poll(runtime, error);
}

static tf_transform_code category_finalize_encoding(
    const tf_transform_recipe_column *recipe,
    tf_transform_categorical_state *out, tf_transform_error **error) {
    if (!recipe || !out)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical encoding state is invalid");
    out->unknown = recipe->categorical_unknown;
    out->sentinel_label = recipe->categorical_sentinel_label;
    out->has_sentinel_label = recipe->categorical_has_sentinel_label;
    if (out->encode != TF_TRANSFORM_ENCODE_LABEL
        && out->encode != TF_TRANSFORM_ENCODE_ONEHOT)
        return TF_TRANSFORM_OK;
    if ((uint64_t)out->category_count
            > (uint64_t)TF_TRANSFORM_MAX_SAFE_INTEGER_V1)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "categorical encoding count exceeds the safe-integer domain");
    if (out->encode == TF_TRANSFORM_ENCODE_ONEHOT) {
        out->sentinel_label = 0;
        out->has_sentinel_label = 0;
        if (out->unknown == TF_TRANSFORM_UNKNOWN_OTHER) {
            out->other_ordinal = (uint64_t)out->category_count;
            out->has_other_ordinal = 1;
        } else if (out->unknown != TF_TRANSFORM_UNKNOWN_ERROR
                   && out->unknown != TF_TRANSFORM_UNKNOWN_ALL_ZERO) {
            return tf_transform_set_error(
                error, TF_TRANSFORM_INVALID_RECIPE,
                "one-hot unknown-category policy is invalid");
        }
        return TF_TRANSFORM_OK;
    }
    if (out->unknown == TF_TRANSFORM_UNKNOWN_SENTINEL) {
        if (!out->has_sentinel_label)
            return tf_transform_set_error(
                error, TF_TRANSFORM_INTERNAL,
                "categorical sentinel state is missing");
        if (out->sentinel_label >= 0
            && (uint64_t)out->sentinel_label < (uint64_t)out->category_count)
            return tf_transform_set_error(
                error, TF_TRANSFORM_INVALID_RECIPE,
                "categorical sentinel collides with a learned label");
    } else if (out->unknown == TF_TRANSFORM_UNKNOWN_OTHER) {
        out->other_ordinal = (uint64_t)out->category_count;
        out->has_other_ordinal = 1;
    }
    return TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_category_finalize(
    const tf_transform_analyzer *analyzer, size_t column_index,
    tf_transform_categorical_state *out, tf_transform_error **error) {
    const tf_transform_category_store *store;
    const tf_transform_recipe_column *recipe;
    uint64_t resident = 0;
    uint64_t allocations = 0;
    uint64_t best_bits = 0;
    uint64_t best_count = 0;
    size_t output_index = 0;
    tf_transform_code code;
    if (!out)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical plan output is null");
    memset(out, 0, sizeof(*out));
    code = tf_transform_category_plan_requirements(
        analyzer, column_index, &resident, &allocations, error);
    if (code != TF_TRANSFORM_OK) return code;
    (void)allocations;
    store = &analyzer->category_stores[column_index];
    recipe = &analyzer->recipe->columns[column_index];
    out->impute = recipe->categorical_impute;
    out->all_missing = recipe->categorical_all_missing;
    out->encode = recipe->categorical_encode;
    out->source_dtype = analyzer->input_schema.fields[column_index].dtype;
    out->category_count = (size_t)(resident / sizeof(*out->categories));
    if (resident % sizeof(*out->categories) != 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical plan requirement count is invalid");
    if (out->category_count == 0) {
        if (recipe->kind != TF_TRANSFORM_KIND_CATEGORICAL
            || recipe->categorical_impute != TF_TRANSFORM_CATEGORICAL_IMPUTE_NONE
            || recipe->categorical_encode != TF_TRANSFORM_ENCODE_NONE)
            return tf_transform_set_error(
                error, TF_TRANSFORM_INTERNAL,
                "categorical plan has an illegal empty dictionary");
        code = category_finalize_encoding(recipe, out, error);
        if (code != TF_TRANSFORM_OK) return code;
        return category_poll(&analyzer->runtime, error);
    }
    code = category_poll(&analyzer->runtime, error);
    if (code != TF_TRANSFORM_OK) return code;
    out->categories = (tf_transform_category_value *)calloc(
        out->category_count, sizeof(*out->categories));
    if (!out->categories)
        return tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION,
            "categorical plan allocation failed");
    if (recipe->categorical_fixed_count) {
        code = tf_transform_copy_bytes_runtime(
            out->categories, recipe->categorical_fixed, (size_t)resident,
            &analyzer->runtime, error);
        if (code != TF_TRANSFORM_OK) goto failed;
        for (size_t i = 0; i < store->capacity; ++i) {
            if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
                code = category_poll(&analyzer->runtime, error);
                if (code != TF_TRANSFORM_OK) goto failed;
            }
            const tf_transform_category_slot *slot = &store->slots[i];
            if (slot->occupied && (slot->count > best_count
                || (slot->count == best_count && tf_transform_category_compare(
                    slot->bits, best_bits, store->dtype) < 0))) {
                best_count = slot->count;
                best_bits = slot->bits;
            }
        }
        if (recipe->categorical_impute == TF_TRANSFORM_CATEGORICAL_IMPUTE_MODE) {
            if (!best_count) {
                int zero_known = 0;
                code = fixed_category_known(recipe, 0, &analyzer->runtime, &zero_known, error);
                if (code != TF_TRANSFORM_OK) goto failed;
                if (recipe->categorical_all_missing != TF_TRANSFORM_ALL_MISSING_ZERO || !zero_known) {
                    code = tf_transform_set_error(error, TF_TRANSFORM_INSUFFICIENT_DATA,
                        "fixed dictionary mode has no known observations or declared zero fallback");
                    goto failed;
                }
                best_bits = 0;
            }
            out->impute_bits = best_bits;
            out->has_impute_value = 1;
        }
        code = category_finalize_encoding(recipe, out, error);
        if (code != TF_TRANSFORM_OK) goto failed;
        return category_poll(&analyzer->runtime, error);
    }
    if (store->category_count == 0) {
        out->categories[0].bits = 0;
        out->impute_bits = 0;
        out->has_impute_value = 1;
        code = category_finalize_encoding(recipe, out, error);
        if (code != TF_TRANSFORM_OK) goto failed;
        return category_poll(&analyzer->runtime, error);
    }
    for (size_t i = 0; i < store->capacity; ++i) {
        const tf_transform_category_slot *slot;
        if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            code = category_poll(&analyzer->runtime, error);
            if (code != TF_TRANSFORM_OK) goto failed;
        }
        slot = &store->slots[i];
        if (!slot->occupied) continue;
        if (output_index >= out->category_count) {
            code = tf_transform_set_error(
                error, TF_TRANSFORM_INTERNAL,
                "categorical plan category count drifted");
            goto failed;
        }
        out->categories[output_index++].bits = slot->bits;
        if (slot->count > best_count
            || (slot->count == best_count
                && tf_transform_category_compare(
                    slot->bits, best_bits, store->dtype) < 0)) {
            best_count = slot->count;
            best_bits = slot->bits;
        }
    }
    if (output_index != out->category_count
        || (recipe->categorical_impute == TF_TRANSFORM_CATEGORICAL_IMPUTE_MODE
            && best_count == 0)) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical plan state is incomplete");
        goto failed;
    }
    code = category_sort(
        out->categories, out->category_count, out->source_dtype,
        &analyzer->runtime, error);
    if (code != TF_TRANSFORM_OK) goto failed;
    if (recipe->categorical_impute == TF_TRANSFORM_CATEGORICAL_IMPUTE_MODE) {
        out->impute_bits = best_bits;
        out->has_impute_value = 1;
    }
    code = category_finalize_encoding(recipe, out, error);
    if (code != TF_TRANSFORM_OK) goto failed;
    return TF_TRANSFORM_OK;
failed:
    tf_transform_categorical_state_clear(out);
    return code;
}

void tf_transform_categorical_state_clear(tf_transform_categorical_state *state) {
    if (!state) return;
    free(state->categories);
    memset(state, 0, sizeof(*state));
}

tf_transform_code tf_transform_category_lookup(
    const tf_transform_categorical_state *state, uint64_t bits,
    const tf_transform_runtime_copy *runtime, size_t *ordinal, int *found,
    tf_transform_error **error) {
    size_t lower = 0;
    size_t upper;
    size_t ticks = 0;
    tf_transform_code code;
    if (!state || !runtime || !ordinal || !found)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "categorical lookup arguments are invalid");
    upper = state->category_count;
    code = tf_transform_poll_cancel(runtime, error);
    if (code != TF_TRANSFORM_OK) return code;
    while (lower < upper) {
        size_t middle = lower + (upper - lower) / 2;
        int comparison;
        ++ticks;
        if (ticks >= TF_TRANSFORM_CANCEL_ITERS_V1) {
            ticks = 0;
            code = tf_transform_poll_cancel(runtime, error);
        } else code = TF_TRANSFORM_OK;
        if (code != TF_TRANSFORM_OK) return code;
        comparison = tf_transform_category_compare(
            state->categories[middle].bits, bits, state->source_dtype);
        if (comparison < 0) lower = middle + 1;
        else if (comparison > 0) upper = middle;
        else {
            *ordinal = middle;
            *found = 1;
            return TF_TRANSFORM_OK;
        }
    }
    *ordinal = 0;
    *found = 0;
    return TF_TRANSFORM_OK;
}
