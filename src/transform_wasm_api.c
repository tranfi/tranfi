/* Checked standalone wasm32 shim for the generic prepared-transform API. */

#include "transform_wasm.h"

#include "transform.h"
#include "transform_internal.h"

#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifdef __EMSCRIPTEN__
#include <emscripten.h>
#include <emscripten/heap.h>
#include <emscripten/em_js.h>
#define TF_WASM_EXPORT EMSCRIPTEN_KEEPALIVE
EM_JS(int, tf_wasm_transform_host_cancel_poll, (uint32_t slot), {
    var poll = Module['tfTransformCancelPoll'];
    return typeof poll === 'function' && poll(slot) ? 1 : 0;
});
#else
#define TF_WASM_EXPORT
static uint8_t *tf_wasm_test_heap_base;
static uint32_t tf_wasm_test_heap_size;
static int tf_wasm_transform_host_cancel_poll(uint32_t slot) {
    (void)slot;
    return 0;
}
void tf_wasm_transform_test_set_heap(void *base, uint32_t size) {
    tf_wasm_test_heap_base = (uint8_t *)base;
    tf_wasm_test_heap_size = size;
}
#endif

_Static_assert(sizeof(tf_wasm_field_view_v1) == 32,
               "wasm32 field descriptor drift");
_Static_assert(sizeof(tf_wasm_schema_view_v1) == 20,
               "wasm32 schema descriptor drift");
_Static_assert(sizeof(tf_wasm_column_view_v1) == 36,
               "wasm32 column descriptor drift");
_Static_assert(sizeof(tf_wasm_table_view_v1) == 24,
               "wasm32 table descriptor drift");
_Static_assert(sizeof(tf_wasm_dense_info_v1) == 28,
               "wasm32 dense info drift");
_Static_assert(sizeof(tf_wasm_schema_field_info_v1) == 20,
               "wasm32 schema field info drift");
_Static_assert(sizeof(tf_transform_limits_v1) == 184,
               "transform limits wire layout drift");

#define TF_WASM_SLOT_COUNT 65536u
#define TF_WASM_TYPE_SHIFT 28u
#define TF_WASM_GENERATION_SHIFT 16u
#define TF_WASM_TYPE_MASK 0x0fu
#define TF_WASM_GENERATION_MASK 0x0fffu
#define TF_WASM_SLOT_MASK 0xffffu

typedef struct tf_wasm_cancel_token {
    atomic_uint references;
    atomic_int requested;
    uint32_t host_poll_slot;
} tf_wasm_cancel_token;

typedef struct tf_wasm_owned_bytes {
    uint8_t *data;
    size_t len;
} tf_wasm_owned_bytes;

typedef struct tf_wasm_owned_dense {
    tf_owned_dense_v1 value;
} tf_wasm_owned_dense;

typedef struct tf_wasm_handle_slot {
    void *pointer;
    tf_wasm_cancel_token *token;
    uint32_t *dtypes;
    uint64_t max_live_handles;
    uint64_t max_retired_slots;
    uint64_t max_allocation_bytes;
    size_t column_count;
    uint16_t generation;
    uint16_t next_free;
    uint8_t type;
    uint8_t retired;
} tf_wasm_handle_slot;

typedef struct tf_wasm_schema_owner {
    tf_schema_view_v1 view;
    tf_field_view_v1 *fields;
    uint32_t *dtypes;
    size_t count;
} tf_wasm_schema_owner;

typedef struct tf_wasm_table_owner {
    tf_table_view_v1 view;
    tf_column_view_v1 *columns;
} tf_wasm_table_owner;

static tf_wasm_handle_slot tf_wasm_slots[TF_WASM_SLOT_COUNT];
static uint32_t tf_wasm_next_virgin_slot = 1;
static uint16_t tf_wasm_free_slot_head;
static uint64_t tf_wasm_live_handles;
static uint64_t tf_wasm_retired_slots;

static int tf_wasm_has_available_slot(void) {
    return tf_wasm_free_slot_head != 0
        || tf_wasm_next_virgin_slot < TF_WASM_SLOT_COUNT;
}

static uint32_t tf_wasm_take_available_slot(void) {
    uint32_t index;
    if (tf_wasm_free_slot_head != 0) {
        index = tf_wasm_free_slot_head;
        tf_wasm_free_slot_head = tf_wasm_slots[index].next_free;
        tf_wasm_slots[index].next_free = 0;
        return index;
    }
    if (tf_wasm_next_virgin_slot >= TF_WASM_SLOT_COUNT) return 0;
    index = tf_wasm_next_virgin_slot;
    ++tf_wasm_next_virgin_slot;
    return index;
}

static tf_transform_code tf_wasm_preflight_handle(
    const tf_transform_limits_v1 *limits) {
    if (!limits) return TF_TRANSFORM_INVALID_ARGUMENT;
    if (tf_wasm_live_handles >= limits->max_live_handles
        || tf_wasm_retired_slots > limits->max_retired_handle_slots
        || !tf_wasm_has_available_slot())
        return TF_TRANSFORM_RESOURCE_LIMIT;
    return TF_TRANSFORM_OK;
}

static uint64_t tf_wasm_heap_size(void) {
#ifdef __EMSCRIPTEN__
    return (uint64_t)emscripten_get_heap_size();
#else
    return (uint64_t)tf_wasm_test_heap_size;
#endif
}

static uint8_t *tf_wasm_heap_span(uint32_t offset, uint32_t len) {
    uint64_t end;
    if (len == 0) {
        if (offset == 0) return NULL;
        end = (uint64_t)offset;
    } else {
        if (offset == 0) return NULL;
        end = (uint64_t)offset + (uint64_t)len;
    }
    if (end > tf_wasm_heap_size()) return NULL;
#ifdef __EMSCRIPTEN__
    return (uint8_t *)(uintptr_t)offset;
#else
    if (!tf_wasm_test_heap_base) return NULL;
    return tf_wasm_test_heap_base + offset;
#endif
}

static int tf_wasm_read(uint32_t offset, void *out, uint32_t len) {
    uint8_t *source;
    if (len == 0) return 1;
    source = tf_wasm_heap_span(offset, len);
    if (!source || !out) return 0;
    memcpy(out, source, len);
    return 1;
}

static int tf_wasm_write(uint32_t offset, const void *value, uint32_t len) {
    uint8_t *destination;
    if (len == 0) return 1;
    destination = tf_wasm_heap_span(offset, len);
    if (!destination || !value) return 0;
    memcpy(destination, value, len);
    return 1;
}

static int tf_wasm_write_u32(uint32_t offset, uint32_t value) {
    return tf_wasm_write(offset, &value, (uint32_t)sizeof(value));
}

static int tf_wasm_spans_overlap(
    uint32_t first, uint32_t first_len,
    uint32_t second, uint32_t second_len) {
    uint64_t first_end = (uint64_t)first + first_len;
    uint64_t second_end = (uint64_t)second + second_len;
    return first_len != 0 && second_len != 0
        && (uint64_t)first < second_end && (uint64_t)second < first_end;
}

static tf_transform_code tf_wasm_prepare_error(uint32_t error_offset) {
    if (!tf_wasm_heap_span(error_offset, 4))
        return TF_TRANSFORM_INVALID_ARGUMENT;
    return tf_wasm_write_u32(error_offset, 0)
        ? TF_TRANSFORM_OK : TF_TRANSFORM_INVALID_ARGUMENT;
}

static tf_transform_code tf_wasm_prepare_outputs(
    uint32_t output_offset, uint32_t error_offset) {
    if (tf_wasm_spans_overlap(output_offset, 4, error_offset, 4)
        || !tf_wasm_heap_span(output_offset, 4)
        || !tf_wasm_heap_span(error_offset, 4))
        return TF_TRANSFORM_INVALID_ARGUMENT;
    if (!tf_wasm_write_u32(output_offset, 0)
        || !tf_wasm_write_u32(error_offset, 0))
        return TF_TRANSFORM_INVALID_ARGUMENT;
    return TF_TRANSFORM_OK;
}

static uint32_t tf_wasm_make_handle(
    uint8_t type, uint16_t generation, uint16_t slot) {
    return ((uint32_t)type << TF_WASM_TYPE_SHIFT)
        | ((uint32_t)generation << TF_WASM_GENERATION_SHIFT)
        | (uint32_t)slot;
}

static tf_wasm_handle_slot *tf_wasm_get_slot(
    uint32_t handle, uint8_t expected_type) {
    uint8_t type = (uint8_t)((handle >> TF_WASM_TYPE_SHIFT) & TF_WASM_TYPE_MASK);
    uint16_t generation = (uint16_t)(
        (handle >> TF_WASM_GENERATION_SHIFT) & TF_WASM_GENERATION_MASK);
    uint16_t index = (uint16_t)(handle & TF_WASM_SLOT_MASK);
    tf_wasm_handle_slot *slot;
    if (handle == 0 || index == 0 || generation == 0 || type == 0)
        return NULL;
    if (expected_type != 0 && type != expected_type) return NULL;
    slot = &tf_wasm_slots[index];
    if (!slot->pointer || slot->retired || slot->type != type
        || slot->generation != generation) return NULL;
    return slot;
}

static void tf_wasm_token_retain(tf_wasm_cancel_token *token) {
    if (token) (void)atomic_fetch_add_explicit(
        &token->references, 1u, memory_order_relaxed);
}

static void tf_wasm_token_release(tf_wasm_cancel_token *token) {
    if (token && atomic_fetch_sub_explicit(
            &token->references, 1u, memory_order_acq_rel) == 1u) free(token);
}

static int tf_wasm_cancel_poll(void *user) {
    tf_wasm_cancel_token *token = (tf_wasm_cancel_token *)user;
    if (!token) return 0;
    if (atomic_load_explicit(&token->requested, memory_order_acquire) != 0)
        return 1;
    return token->host_poll_slot != TF_WASM_TRANSFORM_NO_HOST_CANCEL
        && tf_wasm_transform_host_cancel_poll(token->host_poll_slot) != 0;
}

static void tf_wasm_destroy_pointer(uint8_t type, void *pointer) {
    if (!pointer) return;
    if (type == TF_WASM_HANDLE_RECIPE) {
        tf_transform_recipe *value = (tf_transform_recipe *)pointer;
        tf_transform_recipe_destroy(&value);
    } else if (type == TF_WASM_HANDLE_ANALYZER) {
        tf_transform_analyzer *value = (tf_transform_analyzer *)pointer;
        tf_transform_analyzer_destroy(&value);
    } else if (type == TF_WASM_HANDLE_PLAN) {
        tf_transform_plan *value = (tf_transform_plan *)pointer;
        tf_transform_plan_destroy(&value);
    } else if (type == TF_WASM_HANDLE_APPLY) {
        tf_transform_apply *value = (tf_transform_apply *)pointer;
        tf_transform_apply_destroy(&value);
    } else if (type == TF_WASM_HANDLE_ERROR) {
        tf_transform_error *value = (tf_transform_error *)pointer;
        tf_transform_error_destroy(&value);
    } else if (type == TF_WASM_HANDLE_OWNED_BYTES) {
        tf_wasm_owned_bytes *value = (tf_wasm_owned_bytes *)pointer;
        tf_transform_bytes_free(&value->data, &value->len);
        free(value);
    } else if (type == TF_WASM_HANDLE_OWNED_DENSE) {
        tf_wasm_owned_dense *value = (tf_wasm_owned_dense *)pointer;
        tf_owned_dense_free(&value->value);
        free(value);
    } else if (type == TF_WASM_HANDLE_CANCEL_TOKEN) {
        tf_wasm_token_release((tf_wasm_cancel_token *)pointer);
    }
}

static tf_transform_code tf_wasm_install_handle(
    uint8_t type, void *pointer, tf_wasm_cancel_token *token,
    const uint32_t *dtypes, size_t column_count,
    const tf_transform_limits_v1 *limits, uint32_t *out) {
    uint32_t *dtype_copy = NULL;
    uint32_t index = 0;
    if (!pointer || !limits || !out || type == 0 || type > 8)
        return TF_TRANSFORM_INVALID_ARGUMENT;
    if (tf_wasm_live_handles >= limits->max_live_handles
        || tf_wasm_retired_slots > limits->max_retired_handle_slots)
        return TF_TRANSFORM_RESOURCE_LIMIT;
    if (column_count != 0) {
        uint64_t bytes = (uint64_t)column_count * sizeof(*dtype_copy);
        if (!dtypes || bytes > SIZE_MAX || bytes > limits->max_allocation_bytes)
            return TF_TRANSFORM_RESOURCE_LIMIT;
        dtype_copy = (uint32_t *)malloc((size_t)bytes);
        if (!dtype_copy) return TF_TRANSFORM_ALLOCATION;
        memcpy(dtype_copy, dtypes, (size_t)bytes);
    }
    index = tf_wasm_take_available_slot();
    if (index == 0) {
        free(dtype_copy);
        return TF_TRANSFORM_RESOURCE_LIMIT;
    }
    tf_wasm_handle_slot *slot = &tf_wasm_slots[index];
    if (slot->generation == 0) slot->generation = 1;
    slot->pointer = pointer;
    slot->token = token;
    slot->dtypes = dtype_copy;
    slot->column_count = column_count;
    slot->type = type;
    slot->max_live_handles = limits->max_live_handles;
    slot->max_retired_slots = limits->max_retired_handle_slots;
    slot->max_allocation_bytes = limits->max_allocation_bytes;
    tf_wasm_token_retain(token);
    ++tf_wasm_live_handles;
    *out = tf_wasm_make_handle(type, slot->generation, (uint16_t)index);
    return TF_TRANSFORM_OK;
}

static tf_transform_code tf_wasm_read_limits(
    uint32_t offset, tf_transform_limits_v1 *out,
    tf_transform_error **error) {
    tf_transform_limits_v1 raw;
    if (offset == 0) return tf_transform_limits_init_safe_v1(out, sizeof(*out));
    if (!tf_wasm_read(offset, &raw, (uint32_t)sizeof(raw)))
        return TF_TRANSFORM_INVALID_ARGUMENT;
    return tf_transform_copy_limits(&raw, out, error);
}

static tf_transform_limits_v1 tf_wasm_safe_limits(void) {
    tf_transform_limits_v1 limits;
    memset(&limits, 0, sizeof(limits));
    (void)tf_transform_limits_init_safe_v1(&limits, sizeof(limits));
    return limits;
}

static tf_transform_code tf_wasm_transfer_error(
    tf_transform_error **error, const tf_transform_limits_v1 *limits,
    uint32_t error_offset, tf_transform_code code) {
    uint32_t handle = 0;
    tf_transform_code install_code;
    if (!error || !*error) return code;
    install_code = tf_wasm_install_handle(
        TF_WASM_HANDLE_ERROR, *error, NULL, NULL, 0, limits, &handle);
    if (install_code != TF_TRANSFORM_OK) {
        tf_transform_error_destroy(error);
        return code;
    }
    *error = NULL;
    if (!tf_wasm_write_u32(error_offset, handle)) {
        (void)tf_wasm_transform_handle_destroy(handle);
        return TF_TRANSFORM_INVALID_ARGUMENT;
    }
    return code;
}

static tf_transform_code tf_wasm_transfer_error_safe(
    tf_transform_error **error, uint32_t error_offset,
    tf_transform_code code) {
    tf_transform_limits_v1 limits = tf_wasm_safe_limits();
    return tf_wasm_transfer_error(error, &limits, error_offset, code);
}

static tf_wasm_cancel_token *tf_wasm_optional_token(uint32_t handle) {
    tf_wasm_handle_slot *slot;
    if (handle == 0) return NULL;
    slot = tf_wasm_get_slot(handle, TF_WASM_HANDLE_CANCEL_TOKEN);
    return slot ? (tf_wasm_cancel_token *)slot->pointer : NULL;
}

static void tf_wasm_runtime_init(
    tf_transform_runtime_v1 *runtime,
    const tf_transform_limits_v1 *limits, tf_wasm_cancel_token *token) {
    memset(runtime, 0, sizeof(*runtime));
    runtime->abi_version = 1;
    runtime->struct_size = (uint32_t)sizeof(*runtime);
    runtime->limits = limits;
    if (token) {
        runtime->cancel = tf_wasm_cancel_poll;
        runtime->cancel_user = token;
    }
}

static void tf_wasm_schema_clear(tf_wasm_schema_owner *owner) {
    if (!owner) return;
    free(owner->fields);
    free(owner->dtypes);
    memset(owner, 0, sizeof(*owner));
}

static tf_transform_code tf_wasm_parse_schema(
    uint32_t offset, const tf_transform_limits_v1 *limits,
    tf_wasm_cancel_token *token, tf_wasm_schema_owner *owner) {
    tf_wasm_schema_view_v1 wire;
    uint64_t expected_bytes;
    memset(owner, 0, sizeof(*owner));
    if (!tf_wasm_read(offset, &wire, sizeof(wire))
        || wire.abi_version != 1 || wire.struct_size != sizeof(wire)
        || wire.column_count == 0)
        return TF_TRANSFORM_INVALID_ARGUMENT;
    if ((uint64_t)wire.column_count > limits->max_input_columns)
        return TF_TRANSFORM_RESOURCE_LIMIT;
    expected_bytes = (uint64_t)wire.column_count * sizeof(tf_wasm_field_view_v1);
    if (expected_bytes != wire.fields_bytes
        || !tf_wasm_heap_span(wire.fields_offset, wire.fields_bytes))
        return TF_TRANSFORM_INVALID_ARGUMENT;
    if (expected_bytes > limits->max_allocation_bytes)
        return TF_TRANSFORM_RESOURCE_LIMIT;
    owner->count = wire.column_count;
    if ((uint64_t)owner->count * sizeof(*owner->fields)
            > limits->max_allocation_bytes
        || (uint64_t)owner->count * sizeof(*owner->dtypes)
            > limits->max_allocation_bytes)
        return TF_TRANSFORM_RESOURCE_LIMIT;
    owner->fields = (tf_field_view_v1 *)calloc(
        owner->count, sizeof(*owner->fields));
    owner->dtypes = (uint32_t *)calloc(owner->count, sizeof(*owner->dtypes));
    if (!owner->fields || !owner->dtypes) {
        tf_wasm_schema_clear(owner);
        return TF_TRANSFORM_ALLOCATION;
    }
    for (size_t i = 0; i < owner->count; ++i) {
        tf_wasm_field_view_v1 field;
        uint32_t field_offset = (uint32_t)(
            (uint64_t)wire.fields_offset + i * sizeof(field));
        uint8_t *id;
        uint8_t *name;
        if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0
            && tf_wasm_cancel_poll(token)) {
            tf_wasm_schema_clear(owner);
            return TF_TRANSFORM_CANCELLED;
        }
        if (!tf_wasm_read(field_offset, &field, sizeof(field))
            || field.abi_version != 1 || field.struct_size != sizeof(field)
            || field.flags != 0
            || (field.dtype != TF_VIEW_FLOAT32
                && field.dtype != TF_VIEW_FLOAT64)
            || field.id_bytes == 0 || field.name_bytes == 0)
            goto invalid;
        id = tf_wasm_heap_span(field.id_offset, field.id_bytes);
        name = tf_wasm_heap_span(field.name_offset, field.name_bytes);
        if (!id || !name) goto invalid;
        owner->fields[i].abi_version = 1;
        owner->fields[i].struct_size = (uint32_t)sizeof(owner->fields[i]);
        owner->fields[i].dtype = field.dtype;
        owner->fields[i].id_utf8 = id;
        owner->fields[i].id_bytes = field.id_bytes;
        owner->fields[i].name_utf8 = name;
        owner->fields[i].name_bytes = field.name_bytes;
        owner->dtypes[i] = field.dtype;
    }
    owner->view.abi_version = 1;
    owner->view.struct_size = (uint32_t)sizeof(owner->view);
    owner->view.column_count = owner->count;
    owner->view.fields = owner->fields;
    owner->view.fields_bytes = owner->count * sizeof(*owner->fields);
    return TF_TRANSFORM_OK;
invalid:
    tf_wasm_schema_clear(owner);
    return TF_TRANSFORM_INVALID_ARGUMENT;
}

static void tf_wasm_table_clear(tf_wasm_table_owner *owner) {
    if (!owner) return;
    free(owner->columns);
    memset(owner, 0, sizeof(*owner));
}

static tf_transform_code tf_wasm_parse_table(
    uint32_t offset, const tf_wasm_handle_slot *slot,
    tf_wasm_cancel_token *token, tf_wasm_table_owner *owner) {
    tf_wasm_table_view_v1 wire;
    uint64_t expected_bytes;
    memset(owner, 0, sizeof(*owner));
    if (!tf_wasm_read(offset, &wire, sizeof(wire))
        || wire.abi_version != 1 || wire.struct_size != sizeof(wire)
        || wire.column_count != slot->column_count)
        return TF_TRANSFORM_INVALID_ARGUMENT;
    expected_bytes = (uint64_t)wire.column_count * sizeof(tf_wasm_column_view_v1);
    if (expected_bytes != wire.columns_bytes
        || !tf_wasm_heap_span(wire.columns_offset, wire.columns_bytes))
        return TF_TRANSFORM_INVALID_ARGUMENT;
    if (expected_bytes > slot->max_allocation_bytes)
        return TF_TRANSFORM_RESOURCE_LIMIT;
    if ((uint64_t)wire.column_count * sizeof(*owner->columns)
        > slot->max_allocation_bytes) return TF_TRANSFORM_RESOURCE_LIMIT;
    owner->columns = (tf_column_view_v1 *)calloc(
        wire.column_count, sizeof(*owner->columns));
    if (!owner->columns) return TF_TRANSFORM_ALLOCATION;
    for (size_t i = 0; i < wire.column_count; ++i) {
        tf_wasm_column_view_v1 column;
        uint32_t column_offset = (uint32_t)(
            (uint64_t)wire.columns_offset + i * sizeof(column));
        uint32_t item_size = slot->dtypes[i] == TF_VIEW_FLOAT32 ? 4u : 8u;
        uint8_t *data;
        uint8_t *validity = NULL;
        if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0
            && tf_wasm_cancel_poll(token)) {
            tf_wasm_table_clear(owner);
            return TF_TRANSFORM_CANCELLED;
        }
        if (!tf_wasm_read(column_offset, &column, sizeof(column))
            || column.abi_version != 1 || column.struct_size != sizeof(column)
            || column.stride_bytes == 0
            || column.stride_bytes % item_size != 0)
            goto invalid;
        data = tf_wasm_heap_span(column.data_offset, column.data_bytes);
        if (wire.row_count != 0
            && (!data || column.data_offset % item_size != 0)) goto invalid;
        if (column.validity_offset != 0 || column.validity_bytes != 0) {
            validity = tf_wasm_heap_span(
                column.validity_offset, column.validity_bytes);
            if (!validity || column.validity_bit_stride == 0) goto invalid;
        } else if (column.validity_bit_offset != 0
                   || column.validity_bit_stride != 0) goto invalid;
        owner->columns[i].abi_version = 1;
        owner->columns[i].struct_size = (uint32_t)sizeof(owner->columns[i]);
        owner->columns[i].data = data;
        owner->columns[i].data_bytes = column.data_bytes;
        owner->columns[i].stride_bytes = column.stride_bytes;
        owner->columns[i].validity = validity;
        owner->columns[i].validity_bytes = column.validity_bytes;
        owner->columns[i].validity_bit_offset = column.validity_bit_offset;
        owner->columns[i].validity_bit_stride = column.validity_bit_stride;
    }
    owner->view.abi_version = 1;
    owner->view.struct_size = (uint32_t)sizeof(owner->view);
    owner->view.row_count = wire.row_count;
    owner->view.column_count = wire.column_count;
    owner->view.columns = owner->columns;
    owner->view.columns_bytes = wire.column_count * sizeof(*owner->columns);
    return TF_TRANSFORM_OK;
invalid:
    tf_wasm_table_clear(owner);
    return TF_TRANSFORM_INVALID_ARGUMENT;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_limits_init_safe_v1(
    uint32_t out_offset, uint32_t out_size) {
    tf_transform_limits_v1 limits;
    tf_transform_code code;
    if (out_size < sizeof(limits)
        || !tf_wasm_heap_span(out_offset, (uint32_t)sizeof(limits)))
        return TF_TRANSFORM_INVALID_ARGUMENT;
    code = tf_transform_limits_init_safe_v1(&limits, sizeof(limits));
    if (code != TF_TRANSFORM_OK) return code;
    return tf_wasm_write(out_offset, &limits, sizeof(limits))
        ? TF_TRANSFORM_OK : TF_TRANSFORM_INVALID_ARGUMENT;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_cancel_create(
    uint32_t host_poll_slot, uint32_t limits_offset,
    uint32_t out_handle_offset, uint32_t out_error_offset) {
    tf_transform_limits_v1 limits;
    tf_transform_error *error = NULL;
    tf_wasm_cancel_token *token;
    tf_transform_code code = tf_wasm_prepare_outputs(
        out_handle_offset, out_error_offset);
    uint32_t handle = 0;
    if (code != TF_TRANSFORM_OK) return code;
    code = tf_wasm_read_limits(limits_offset, &limits, &error);
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error_safe(&error, out_error_offset, code);
    token = (tf_wasm_cancel_token *)calloc(1, sizeof(*token));
    if (!token) return TF_TRANSFORM_ALLOCATION;
    atomic_init(&token->references, 1u);
    atomic_init(&token->requested, 0);
    token->host_poll_slot = host_poll_slot;
    code = tf_wasm_install_handle(
        TF_WASM_HANDLE_CANCEL_TOKEN, token, NULL, NULL, 0, &limits, &handle);
    if (code != TF_TRANSFORM_OK) {
        tf_wasm_token_release(token);
        return code;
    }
    if (!tf_wasm_write_u32(out_handle_offset, handle)) {
        (void)tf_wasm_transform_handle_destroy(handle);
        return TF_TRANSFORM_INVALID_ARGUMENT;
    }
    return TF_TRANSFORM_OK;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_cancel_request(uint32_t token_handle) {
    tf_wasm_handle_slot *slot = tf_wasm_get_slot(
        token_handle, TF_WASM_HANDLE_CANCEL_TOKEN);
    if (!slot) return TF_TRANSFORM_INVALID_ARGUMENT;
    atomic_store_explicit(
        &((tf_wasm_cancel_token *)slot->pointer)->requested,
        1, memory_order_release);
    return TF_TRANSFORM_OK;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_recipe_from_json(
    uint32_t json_offset, uint32_t json_bytes, uint32_t limits_offset,
    uint32_t out_handle_offset, uint32_t out_error_offset) {
    tf_transform_limits_v1 limits;
    tf_transform_error *error = NULL;
    tf_transform_recipe *recipe = NULL;
    uint8_t *json;
    uint32_t handle = 0;
    tf_transform_code code = tf_wasm_prepare_outputs(
        out_handle_offset, out_error_offset);
    if (code != TF_TRANSFORM_OK) return code;
    code = tf_wasm_read_limits(limits_offset, &limits, &error);
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error_safe(&error, out_error_offset, code);
    json = tf_wasm_heap_span(json_offset, json_bytes);
    if (json_bytes != 0 && !json) return TF_TRANSFORM_INVALID_ARGUMENT;
    code = tf_transform_recipe_from_json(
        json, json_bytes, &limits, &recipe, &error);
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error(&error, &limits, out_error_offset, code);
    code = tf_wasm_install_handle(
        TF_WASM_HANDLE_RECIPE, recipe, NULL, NULL, 0, &limits, &handle);
    if (code != TF_TRANSFORM_OK) {
        tf_transform_recipe_destroy(&recipe);
        return code;
    }
    if (!tf_wasm_write_u32(out_handle_offset, handle)) {
        (void)tf_wasm_transform_handle_destroy(handle);
        return TF_TRANSFORM_INVALID_ARGUMENT;
    }
    return TF_TRANSFORM_OK;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_analyzer_create(
    uint32_t recipe_handle, uint32_t schema_offset,
    uint32_t limits_offset, uint32_t cancel_token_handle,
    uint32_t out_handle_offset, uint32_t out_error_offset) {
    tf_wasm_handle_slot *recipe_slot;
    tf_wasm_cancel_token *token;
    tf_wasm_schema_owner schema;
    tf_transform_limits_v1 limits;
    tf_transform_runtime_v1 runtime;
    tf_transform_analyzer *analyzer = NULL;
    tf_transform_error *error = NULL;
    uint32_t handle = 0;
    tf_transform_code code = tf_wasm_prepare_outputs(
        out_handle_offset, out_error_offset);
    memset(&schema, 0, sizeof(schema));
    if (code != TF_TRANSFORM_OK) return code;
    recipe_slot = tf_wasm_get_slot(recipe_handle, TF_WASM_HANDLE_RECIPE);
    if (!recipe_slot) return TF_TRANSFORM_INVALID_ARGUMENT;
    token = tf_wasm_optional_token(cancel_token_handle);
    if (cancel_token_handle != 0 && !token) return TF_TRANSFORM_INVALID_ARGUMENT;
    code = tf_wasm_read_limits(limits_offset, &limits, &error);
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error_safe(&error, out_error_offset, code);
    tf_wasm_runtime_init(&runtime, &limits, token);
    if (tf_wasm_cancel_poll(token)) {
        code = tf_transform_analyzer_create(
            (tf_transform_recipe *)recipe_slot->pointer, &schema.view,
            &runtime, &analyzer, &error);
        return tf_wasm_transfer_error(
            &error, &limits, out_error_offset, code);
    }
    code = tf_wasm_parse_schema(schema_offset, &limits, token, &schema);
    if (code == TF_TRANSFORM_CANCELLED) {
        code = tf_transform_analyzer_create(
            (tf_transform_recipe *)recipe_slot->pointer, &schema.view,
            &runtime, &analyzer, &error);
        tf_wasm_schema_clear(&schema);
        return tf_wasm_transfer_error(
            &error, &limits, out_error_offset, code);
    }
    if (code != TF_TRANSFORM_OK) return code;
    if (tf_wasm_cancel_poll(token)) {
        code = tf_transform_analyzer_create(
            (tf_transform_recipe *)recipe_slot->pointer, &schema.view,
            &runtime, &analyzer, &error);
        tf_wasm_schema_clear(&schema);
        return tf_wasm_transfer_error(
            &error, &limits, out_error_offset, code);
    }
    code = tf_transform_analyzer_create(
        (tf_transform_recipe *)recipe_slot->pointer, &schema.view,
        &runtime, &analyzer, &error);
    if (code != TF_TRANSFORM_OK) {
        tf_wasm_schema_clear(&schema);
        return tf_wasm_transfer_error(&error, &limits, out_error_offset, code);
    }
    code = tf_wasm_install_handle(
        TF_WASM_HANDLE_ANALYZER, analyzer, token,
        schema.dtypes, schema.count, &limits, &handle);
    tf_wasm_schema_clear(&schema);
    if (code != TF_TRANSFORM_OK) {
        tf_transform_analyzer_destroy(&analyzer);
        return code;
    }
    if (!tf_wasm_write_u32(out_handle_offset, handle)) {
        (void)tf_wasm_transform_handle_destroy(handle);
        return TF_TRANSFORM_INVALID_ARGUMENT;
    }
    return TF_TRANSFORM_OK;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_analyzer_push(
    uint32_t analyzer_handle, uint32_t table_offset,
    uint32_t out_error_offset) {
    tf_wasm_handle_slot *slot;
    tf_wasm_table_owner table;
    tf_transform_error *error = NULL;
    tf_transform_limits_v1 limits = tf_wasm_safe_limits();
    tf_transform_code code = tf_wasm_prepare_error(out_error_offset);
    memset(&table, 0, sizeof(table));
    if (code != TF_TRANSFORM_OK) return code;
    slot = tf_wasm_get_slot(analyzer_handle, TF_WASM_HANDLE_ANALYZER);
    if (!slot) return TF_TRANSFORM_INVALID_ARGUMENT;
    limits.max_live_handles = slot->max_live_handles;
    limits.max_retired_handle_slots = slot->max_retired_slots;
    if (tf_wasm_cancel_poll(slot->token)) {
        code = tf_transform_analyzer_push(
            (tf_transform_analyzer *)slot->pointer, &table.view, &error);
        return tf_wasm_transfer_error(
            &error, &limits, out_error_offset, code);
    }
    code = tf_wasm_parse_table(table_offset, slot, slot->token, &table);
    if (code == TF_TRANSFORM_CANCELLED) {
        code = tf_transform_analyzer_push(
            (tf_transform_analyzer *)slot->pointer, &table.view, &error);
        tf_wasm_table_clear(&table);
        return tf_wasm_transfer_error(
            &error, &limits, out_error_offset, code);
    }
    if (code != TF_TRANSFORM_OK) return code;
    if (tf_wasm_cancel_poll(slot->token)) {
        code = tf_transform_analyzer_push(
            (tf_transform_analyzer *)slot->pointer, &table.view, &error);
        tf_wasm_table_clear(&table);
        return tf_wasm_transfer_error(
            &error, &limits, out_error_offset, code);
    }
    code = tf_transform_analyzer_push(
        (tf_transform_analyzer *)slot->pointer, &table.view, &error);
    tf_wasm_table_clear(&table);
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error(&error, &limits, out_error_offset, code);
    return TF_TRANSFORM_OK;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_analyzer_finalize(
    uint32_t analyzer_handle, uint32_t out_handle_offset,
    uint32_t out_error_offset) {
    tf_wasm_handle_slot *slot;
    tf_transform_plan *plan = NULL;
    tf_transform_error *error = NULL;
    tf_transform_limits_v1 limits = tf_wasm_safe_limits();
    uint32_t handle = 0;
    tf_transform_code code = tf_wasm_prepare_outputs(
        out_handle_offset, out_error_offset);
    if (code != TF_TRANSFORM_OK) return code;
    slot = tf_wasm_get_slot(analyzer_handle, TF_WASM_HANDLE_ANALYZER);
    if (!slot) return TF_TRANSFORM_INVALID_ARGUMENT;
    limits.max_live_handles = slot->max_live_handles;
    limits.max_retired_handle_slots = slot->max_retired_slots;
    limits.max_allocation_bytes = slot->max_allocation_bytes;
    if (tf_wasm_cancel_poll(slot->token)) {
        code = tf_transform_analyzer_finalize(
            (tf_transform_analyzer *)slot->pointer, &plan, &error);
        return tf_wasm_transfer_error(
            &error, &limits, out_error_offset, code);
    }
    code = tf_wasm_preflight_handle(&limits);
    if (code != TF_TRANSFORM_OK) return code;
    code = tf_transform_analyzer_finalize(
        (tf_transform_analyzer *)slot->pointer, &plan, &error);
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error(&error, &limits, out_error_offset, code);
    code = tf_wasm_install_handle(
        TF_WASM_HANDLE_PLAN, plan, NULL, NULL, 0, &limits, &handle);
    if (code != TF_TRANSFORM_OK) {
        tf_transform_plan_destroy(&plan);
        return code;
    }
    if (!tf_wasm_write_u32(out_handle_offset, handle)) {
        (void)tf_wasm_transform_handle_destroy(handle);
        return TF_TRANSFORM_INVALID_ARGUMENT;
    }
    return TF_TRANSFORM_OK;
}

static tf_transform_code tf_wasm_wrap_bytes(
    uint8_t **data, size_t *len, const tf_transform_limits_v1 *limits,
    uint32_t output_offset) {
    tf_wasm_owned_bytes *owned;
    uint32_t handle = 0;
    tf_transform_code code;
    owned = (tf_wasm_owned_bytes *)calloc(1, sizeof(*owned));
    if (!owned) {
        tf_transform_bytes_free(data, len);
        return TF_TRANSFORM_ALLOCATION;
    }
    owned->data = *data;
    owned->len = *len;
    *data = NULL;
    *len = 0;
    code = tf_wasm_install_handle(
        TF_WASM_HANDLE_OWNED_BYTES, owned, NULL, NULL, 0, limits, &handle);
    if (code != TF_TRANSFORM_OK) {
        tf_wasm_destroy_pointer(TF_WASM_HANDLE_OWNED_BYTES, owned);
        return code;
    }
    if (!tf_wasm_write_u32(output_offset, handle)) {
        (void)tf_wasm_transform_handle_destroy(handle);
        return TF_TRANSFORM_INVALID_ARGUMENT;
    }
    return TF_TRANSFORM_OK;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_plan_export(
    uint32_t plan_handle, uint32_t limits_offset,
    uint32_t out_handle_offset, uint32_t out_error_offset) {
    tf_wasm_handle_slot *slot;
    tf_transform_limits_v1 limits;
    tf_transform_error *error = NULL;
    uint8_t *data = NULL;
    size_t len = 0;
    tf_transform_code code = tf_wasm_prepare_outputs(
        out_handle_offset, out_error_offset);
    if (code != TF_TRANSFORM_OK) return code;
    slot = tf_wasm_get_slot(plan_handle, TF_WASM_HANDLE_PLAN);
    if (!slot) return TF_TRANSFORM_INVALID_ARGUMENT;
    code = tf_wasm_read_limits(limits_offset, &limits, &error);
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error_safe(&error, out_error_offset, code);
    code = tf_transform_plan_export(
        (tf_transform_plan *)slot->pointer, &limits, &data, &len, &error);
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error(&error, &limits, out_error_offset, code);
    return tf_wasm_wrap_bytes(&data, &len, &limits, out_handle_offset);
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_plan_import(
    uint32_t bytes_offset, uint32_t bytes_len, uint32_t limits_offset,
    uint32_t cancel_token_handle, uint32_t out_handle_offset,
    uint32_t out_error_offset) {
    tf_transform_limits_v1 limits;
    tf_transform_runtime_v1 runtime;
    tf_wasm_cancel_token *token;
    tf_transform_plan *plan = NULL;
    tf_transform_error *error = NULL;
    uint8_t *bytes;
    uint32_t handle = 0;
    tf_transform_code code = tf_wasm_prepare_outputs(
        out_handle_offset, out_error_offset);
    if (code != TF_TRANSFORM_OK) return code;
    token = tf_wasm_optional_token(cancel_token_handle);
    if (cancel_token_handle != 0 && !token) return TF_TRANSFORM_INVALID_ARGUMENT;
    code = tf_wasm_read_limits(limits_offset, &limits, &error);
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error_safe(&error, out_error_offset, code);
    bytes = tf_wasm_heap_span(bytes_offset, bytes_len);
    if (bytes_len != 0 && !bytes) return TF_TRANSFORM_INVALID_ARGUMENT;
    tf_wasm_runtime_init(&runtime, &limits, token);
    code = tf_transform_plan_import(
        bytes, bytes_len, &runtime, &plan, &error);
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error(&error, &limits, out_error_offset, code);
    code = tf_wasm_install_handle(
        TF_WASM_HANDLE_PLAN, plan, NULL, NULL, 0, &limits, &handle);
    if (code != TF_TRANSFORM_OK) {
        tf_transform_plan_destroy(&plan);
        return code;
    }
    if (!tf_wasm_write_u32(out_handle_offset, handle)) {
        (void)tf_wasm_transform_handle_destroy(handle);
        return TF_TRANSFORM_INVALID_ARGUMENT;
    }
    return TF_TRANSFORM_OK;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_plan_schema_json(
    uint32_t plan_handle, uint32_t which, uint32_t limits_offset,
    uint32_t out_handle_offset, uint32_t out_error_offset) {
    tf_wasm_handle_slot *slot;
    tf_transform_limits_v1 limits;
    tf_transform_error *error = NULL;
    uint8_t *data = NULL;
    size_t len = 0;
    tf_transform_code code = tf_wasm_prepare_outputs(
        out_handle_offset, out_error_offset);
    if (code != TF_TRANSFORM_OK) return code;
    slot = tf_wasm_get_slot(plan_handle, TF_WASM_HANDLE_PLAN);
    if (!slot) return TF_TRANSFORM_INVALID_ARGUMENT;
    code = tf_wasm_read_limits(limits_offset, &limits, &error);
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error_safe(&error, out_error_offset, code);
    code = tf_transform_plan_schema_json(
        (tf_transform_plan *)slot->pointer, which, &limits,
        &data, &len, &error);
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error(&error, &limits, out_error_offset, code);
    return tf_wasm_wrap_bytes(&data, &len, &limits, out_handle_offset);
}

static tf_transform_code tf_wasm_get_schema_field(
    uint32_t plan_handle, uint32_t which, uint32_t index,
    tf_field_view_v1 *field, tf_transform_error **error,
    tf_wasm_handle_slot **slot_out) {
    tf_wasm_handle_slot *slot = tf_wasm_get_slot(
        plan_handle, TF_WASM_HANDLE_PLAN);
    const tf_transform_schema *schema = NULL;
    tf_transform_code code;
    if (!slot) return TF_TRANSFORM_INVALID_ARGUMENT;
    code = tf_transform_plan_schema(
        (tf_transform_plan *)slot->pointer, which, &schema, error);
    if (code != TF_TRANSFORM_OK) return code;
    code = tf_transform_schema_field(schema, index, field, error);
    if (code == TF_TRANSFORM_OK && slot_out) *slot_out = slot;
    return code;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_plan_schema_field_count(
    uint32_t plan_handle, uint32_t which, uint32_t out_count_offset,
    uint32_t out_error_offset) {
    tf_wasm_handle_slot *slot;
    const tf_transform_schema *schema = NULL;
    tf_transform_error *error = NULL;
    tf_transform_limits_v1 limits = tf_wasm_safe_limits();
    size_t count;
    tf_transform_code code = tf_wasm_prepare_outputs(
        out_count_offset, out_error_offset);
    if (code != TF_TRANSFORM_OK) return code;
    slot = tf_wasm_get_slot(plan_handle, TF_WASM_HANDLE_PLAN);
    if (!slot) return TF_TRANSFORM_INVALID_ARGUMENT;
    limits.max_live_handles = slot->max_live_handles;
    limits.max_retired_handle_slots = slot->max_retired_slots;
    code = tf_transform_plan_schema(
        (tf_transform_plan *)slot->pointer, which, &schema, &error);
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error(&error, &limits, out_error_offset, code);
    count = tf_transform_schema_field_count(schema);
    if (count > UINT32_MAX) return TF_TRANSFORM_INTERNAL;
    return tf_wasm_write_u32(out_count_offset, (uint32_t)count)
        ? TF_TRANSFORM_OK : TF_TRANSFORM_INVALID_ARGUMENT;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_plan_schema_field_info(
    uint32_t plan_handle, uint32_t which, uint32_t index,
    uint32_t out_info_offset, uint32_t out_error_offset) {
    tf_field_view_v1 field;
    tf_wasm_schema_field_info_v1 info;
    tf_wasm_handle_slot *slot = NULL;
    tf_transform_error *error = NULL;
    tf_transform_limits_v1 limits = tf_wasm_safe_limits();
    tf_transform_code code;
    if (tf_wasm_spans_overlap(
            out_info_offset, sizeof(info), out_error_offset, 4)
        || !tf_wasm_heap_span(out_info_offset, sizeof(info)))
        return TF_TRANSFORM_INVALID_ARGUMENT;
    code = tf_wasm_prepare_error(out_error_offset);
    if (code != TF_TRANSFORM_OK) return code;
    memset(&field, 0, sizeof(field));
    code = tf_wasm_get_schema_field(
        plan_handle, which, index, &field, &error, &slot);
    if (slot) {
        limits.max_live_handles = slot->max_live_handles;
        limits.max_retired_handle_slots = slot->max_retired_slots;
    }
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error(&error, &limits, out_error_offset, code);
    if (field.id_bytes > UINT32_MAX || field.name_bytes > UINT32_MAX)
        return TF_TRANSFORM_INTERNAL;
    info.abi_version = 1;
    info.struct_size = sizeof(info);
    info.dtype = field.dtype;
    info.id_bytes = (uint32_t)field.id_bytes;
    info.name_bytes = (uint32_t)field.name_bytes;
    return tf_wasm_write(out_info_offset, &info, sizeof(info))
        ? TF_TRANSFORM_OK : TF_TRANSFORM_INVALID_ARGUMENT;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_plan_schema_field_copy(
    uint32_t plan_handle, uint32_t which, uint32_t index,
    uint32_t id_offset, uint32_t id_capacity,
    uint32_t name_offset, uint32_t name_capacity,
    uint32_t out_error_offset) {
    tf_field_view_v1 field;
    tf_wasm_handle_slot *slot = NULL;
    tf_transform_error *error = NULL;
    tf_transform_limits_v1 limits = tf_wasm_safe_limits();
    uint8_t *id_destination;
    uint8_t *name_destination;
    tf_transform_code code = tf_wasm_prepare_error(out_error_offset);
    if (code != TF_TRANSFORM_OK) return code;
    memset(&field, 0, sizeof(field));
    code = tf_wasm_get_schema_field(
        plan_handle, which, index, &field, &error, &slot);
    if (slot) {
        limits.max_live_handles = slot->max_live_handles;
        limits.max_retired_handle_slots = slot->max_retired_slots;
    }
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error(&error, &limits, out_error_offset, code);
    if (field.id_bytes > id_capacity || field.name_bytes > name_capacity)
        return TF_TRANSFORM_RESOURCE_LIMIT;
    if (tf_wasm_spans_overlap(id_offset, id_capacity, name_offset, name_capacity)
        || tf_wasm_spans_overlap(id_offset, id_capacity, out_error_offset, 4)
        || tf_wasm_spans_overlap(name_offset, name_capacity, out_error_offset, 4))
        return TF_TRANSFORM_INVALID_ARGUMENT;
    id_destination = tf_wasm_heap_span(id_offset, id_capacity);
    name_destination = tf_wasm_heap_span(name_offset, name_capacity);
    if ((id_capacity != 0 && !id_destination)
        || (name_capacity != 0 && !name_destination))
        return TF_TRANSFORM_INVALID_ARGUMENT;
    if (field.id_bytes != 0) memcpy(id_destination, field.id_utf8, field.id_bytes);
    if (field.name_bytes != 0)
        memcpy(name_destination, field.name_utf8, field.name_bytes);
    return TF_TRANSFORM_OK;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_apply_create(
    uint32_t plan_handle, uint32_t schema_offset,
    uint32_t limits_offset, uint32_t cancel_token_handle,
    uint32_t out_handle_offset, uint32_t out_error_offset) {
    tf_wasm_handle_slot *plan_slot;
    tf_wasm_cancel_token *token;
    tf_wasm_schema_owner schema;
    tf_transform_limits_v1 limits;
    tf_transform_runtime_v1 runtime;
    tf_transform_apply *apply = NULL;
    tf_transform_error *error = NULL;
    uint32_t handle = 0;
    tf_transform_code code = tf_wasm_prepare_outputs(
        out_handle_offset, out_error_offset);
    memset(&schema, 0, sizeof(schema));
    if (code != TF_TRANSFORM_OK) return code;
    plan_slot = tf_wasm_get_slot(plan_handle, TF_WASM_HANDLE_PLAN);
    if (!plan_slot) return TF_TRANSFORM_INVALID_ARGUMENT;
    token = tf_wasm_optional_token(cancel_token_handle);
    if (cancel_token_handle != 0 && !token) return TF_TRANSFORM_INVALID_ARGUMENT;
    code = tf_wasm_read_limits(limits_offset, &limits, &error);
    if (code != TF_TRANSFORM_OK)
        return tf_wasm_transfer_error_safe(&error, out_error_offset, code);
    tf_wasm_runtime_init(&runtime, &limits, token);
    if (tf_wasm_cancel_poll(token)) {
        code = tf_transform_apply_create(
            (tf_transform_plan *)plan_slot->pointer, &schema.view,
            &runtime, &apply, &error);
        return tf_wasm_transfer_error(
            &error, &limits, out_error_offset, code);
    }
    code = tf_wasm_parse_schema(schema_offset, &limits, token, &schema);
    if (code == TF_TRANSFORM_CANCELLED) {
        code = tf_transform_apply_create(
            (tf_transform_plan *)plan_slot->pointer, &schema.view,
            &runtime, &apply, &error);
        tf_wasm_schema_clear(&schema);
        return tf_wasm_transfer_error(
            &error, &limits, out_error_offset, code);
    }
    if (code != TF_TRANSFORM_OK) return code;
    if (tf_wasm_cancel_poll(token)) {
        code = tf_transform_apply_create(
            (tf_transform_plan *)plan_slot->pointer, &schema.view,
            &runtime, &apply, &error);
        tf_wasm_schema_clear(&schema);
        return tf_wasm_transfer_error(
            &error, &limits, out_error_offset, code);
    }
    code = tf_transform_apply_create(
        (tf_transform_plan *)plan_slot->pointer, &schema.view,
        &runtime, &apply, &error);
    if (code != TF_TRANSFORM_OK) {
        tf_wasm_schema_clear(&schema);
        return tf_wasm_transfer_error(&error, &limits, out_error_offset, code);
    }
    code = tf_wasm_install_handle(
        TF_WASM_HANDLE_APPLY, apply, token,
        schema.dtypes, schema.count, &limits, &handle);
    tf_wasm_schema_clear(&schema);
    if (code != TF_TRANSFORM_OK) {
        tf_transform_apply_destroy(&apply);
        return code;
    }
    if (!tf_wasm_write_u32(out_handle_offset, handle)) {
        (void)tf_wasm_transform_handle_destroy(handle);
        return TF_TRANSFORM_INVALID_ARGUMENT;
    }
    return TF_TRANSFORM_OK;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_apply_run(
    uint32_t apply_handle, uint32_t table_offset,
    uint32_t out_handle_offset, uint32_t out_error_offset) {
    tf_wasm_handle_slot *slot;
    tf_wasm_table_owner table;
    tf_wasm_owned_dense *owned;
    tf_transform_error *error = NULL;
    tf_transform_limits_v1 limits = tf_wasm_safe_limits();
    uint32_t handle = 0;
    tf_transform_code code = tf_wasm_prepare_outputs(
        out_handle_offset, out_error_offset);
    memset(&table, 0, sizeof(table));
    if (code != TF_TRANSFORM_OK) return code;
    slot = tf_wasm_get_slot(apply_handle, TF_WASM_HANDLE_APPLY);
    if (!slot) return TF_TRANSFORM_INVALID_ARGUMENT;
    limits.max_live_handles = slot->max_live_handles;
    limits.max_retired_handle_slots = slot->max_retired_slots;
    limits.max_allocation_bytes = slot->max_allocation_bytes;
    if (tf_wasm_cancel_poll(slot->token)) {
        tf_owned_dense_v1 cancelled = {0};
        code = tf_transform_apply_run(
            (tf_transform_apply *)slot->pointer, &table.view,
            &cancelled, &error);
        return tf_wasm_transfer_error(
            &error, &limits, out_error_offset, code);
    }
    code = tf_wasm_parse_table(table_offset, slot, slot->token, &table);
    if (code == TF_TRANSFORM_CANCELLED) {
        tf_owned_dense_v1 cancelled = {0};
        code = tf_transform_apply_run(
            (tf_transform_apply *)slot->pointer, &table.view,
            &cancelled, &error);
        tf_wasm_table_clear(&table);
        return tf_wasm_transfer_error(
            &error, &limits, out_error_offset, code);
    }
    if (code != TF_TRANSFORM_OK) return code;
    if (tf_wasm_cancel_poll(slot->token)) {
        tf_owned_dense_v1 cancelled = {0};
        code = tf_transform_apply_run(
            (tf_transform_apply *)slot->pointer, &table.view,
            &cancelled, &error);
        tf_wasm_table_clear(&table);
        return tf_wasm_transfer_error(
            &error, &limits, out_error_offset, code);
    }
    code = tf_wasm_preflight_handle(&limits);
    if (code != TF_TRANSFORM_OK) {
        tf_wasm_table_clear(&table);
        return code;
    }
    owned = (tf_wasm_owned_dense *)calloc(1, sizeof(*owned));
    if (!owned) {
        tf_wasm_table_clear(&table);
        return TF_TRANSFORM_ALLOCATION;
    }
    code = tf_transform_apply_run(
        (tf_transform_apply *)slot->pointer, &table.view,
        &owned->value, &error);
    tf_wasm_table_clear(&table);
    if (code != TF_TRANSFORM_OK) {
        tf_wasm_destroy_pointer(TF_WASM_HANDLE_OWNED_DENSE, owned);
        return tf_wasm_transfer_error(&error, &limits, out_error_offset, code);
    }
    code = tf_wasm_install_handle(
        TF_WASM_HANDLE_OWNED_DENSE, owned, slot->token, NULL, 0,
        &limits, &handle);
    if (code != TF_TRANSFORM_OK) {
        tf_wasm_destroy_pointer(TF_WASM_HANDLE_OWNED_DENSE, owned);
        return code;
    }
    if (!tf_wasm_write_u32(out_handle_offset, handle)) {
        (void)tf_wasm_transform_handle_destroy(handle);
        return TF_TRANSFORM_INVALID_ARGUMENT;
    }
    return TF_TRANSFORM_OK;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_error_code(
    uint32_t error_handle, uint32_t out_code_offset) {
    tf_wasm_handle_slot *slot = tf_wasm_get_slot(
        error_handle, TF_WASM_HANDLE_ERROR);
    tf_transform_code code;
    if (!slot || !tf_wasm_heap_span(out_code_offset, 4))
        return TF_TRANSFORM_INVALID_ARGUMENT;
    code = tf_transform_error_get_code((tf_transform_error *)slot->pointer);
    return tf_wasm_write_u32(out_code_offset, (uint32_t)code)
        ? TF_TRANSFORM_OK : TF_TRANSFORM_INVALID_ARGUMENT;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_error_message_size(
    uint32_t error_handle, uint32_t out_size_offset) {
    tf_wasm_handle_slot *slot = tf_wasm_get_slot(
        error_handle, TF_WASM_HANDLE_ERROR);
    size_t len = 0;
    if (!slot || !tf_wasm_heap_span(out_size_offset, 4))
        return TF_TRANSFORM_INVALID_ARGUMENT;
    (void)tf_transform_error_message((tf_transform_error *)slot->pointer, &len);
    if (len > UINT32_MAX) return TF_TRANSFORM_INTERNAL;
    return tf_wasm_write_u32(out_size_offset, (uint32_t)len)
        ? TF_TRANSFORM_OK : TF_TRANSFORM_INVALID_ARGUMENT;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_error_message_copy(
    uint32_t error_handle, uint32_t destination_offset,
    uint32_t destination_capacity) {
    tf_wasm_handle_slot *slot = tf_wasm_get_slot(
        error_handle, TF_WASM_HANDLE_ERROR);
    size_t len = 0;
    const uint8_t *message;
    uint8_t *destination;
    if (!slot) return TF_TRANSFORM_INVALID_ARGUMENT;
    message = tf_transform_error_message(
        (tf_transform_error *)slot->pointer, &len);
    if (len > destination_capacity) return TF_TRANSFORM_RESOURCE_LIMIT;
    destination = tf_wasm_heap_span(destination_offset, destination_capacity);
    if (destination_capacity != 0 && !destination)
        return TF_TRANSFORM_INVALID_ARGUMENT;
    if (len != 0) memcpy(destination, message, len);
    return TF_TRANSFORM_OK;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_owned_bytes_size(
    uint32_t bytes_handle, uint32_t out_size_offset) {
    tf_wasm_handle_slot *slot = tf_wasm_get_slot(
        bytes_handle, TF_WASM_HANDLE_OWNED_BYTES);
    tf_wasm_owned_bytes *owned;
    if (!slot || !tf_wasm_heap_span(out_size_offset, 4))
        return TF_TRANSFORM_INVALID_ARGUMENT;
    owned = (tf_wasm_owned_bytes *)slot->pointer;
    if (owned->len > UINT32_MAX) return TF_TRANSFORM_INTERNAL;
    return tf_wasm_write_u32(out_size_offset, (uint32_t)owned->len)
        ? TF_TRANSFORM_OK : TF_TRANSFORM_INVALID_ARGUMENT;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_owned_bytes_copy(
    uint32_t bytes_handle, uint32_t destination_offset,
    uint32_t destination_capacity) {
    tf_wasm_handle_slot *slot = tf_wasm_get_slot(
        bytes_handle, TF_WASM_HANDLE_OWNED_BYTES);
    tf_wasm_owned_bytes *owned;
    uint8_t *destination;
    if (!slot) return TF_TRANSFORM_INVALID_ARGUMENT;
    owned = (tf_wasm_owned_bytes *)slot->pointer;
    if (owned->len > destination_capacity) return TF_TRANSFORM_RESOURCE_LIMIT;
    destination = tf_wasm_heap_span(destination_offset, destination_capacity);
    if (destination_capacity != 0 && !destination)
        return TF_TRANSFORM_INVALID_ARGUMENT;
    if (owned->len != 0) memcpy(destination, owned->data, owned->len);
    return TF_TRANSFORM_OK;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_owned_dense_info(
    uint32_t dense_handle, uint32_t out_info_offset) {
    tf_wasm_handle_slot *slot = tf_wasm_get_slot(
        dense_handle, TF_WASM_HANDLE_OWNED_DENSE);
    tf_wasm_owned_dense *owned;
    tf_wasm_dense_info_v1 info;
    if (!slot || !tf_wasm_heap_span(out_info_offset, sizeof(info)))
        return TF_TRANSFORM_INVALID_ARGUMENT;
    owned = (tf_wasm_owned_dense *)slot->pointer;
    if (owned->value.rows > UINT32_MAX
        || owned->value.columns > UINT32_MAX
        || owned->value.data_bytes > UINT32_MAX)
        return TF_TRANSFORM_INTERNAL;
    info.abi_version = 1;
    info.struct_size = sizeof(info);
    info.dtype = owned->value.dtype;
    info.flags = owned->value.flags;
    info.rows = (uint32_t)owned->value.rows;
    info.columns = (uint32_t)owned->value.columns;
    info.data_bytes = (uint32_t)owned->value.data_bytes;
    return tf_wasm_write(out_info_offset, &info, sizeof(info))
        ? TF_TRANSFORM_OK : TF_TRANSFORM_INVALID_ARGUMENT;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_owned_dense_copy(
    uint32_t dense_handle, uint32_t destination_offset,
    uint32_t destination_capacity) {
    tf_wasm_handle_slot *slot = tf_wasm_get_slot(
        dense_handle, TF_WASM_HANDLE_OWNED_DENSE);
    tf_wasm_owned_dense *owned;
    uint8_t *destination;
    size_t offset = 0;
    if (!slot) return TF_TRANSFORM_INVALID_ARGUMENT;
    owned = (tf_wasm_owned_dense *)slot->pointer;
    if (owned->value.data_bytes > destination_capacity)
        return TF_TRANSFORM_RESOURCE_LIMIT;
    destination = tf_wasm_heap_span(destination_offset, destination_capacity);
    if (destination_capacity != 0 && !destination)
        return TF_TRANSFORM_INVALID_ARGUMENT;
    while (offset < owned->value.data_bytes) {
        size_t chunk = owned->value.data_bytes - offset;
        if (chunk > TF_TRANSFORM_CANCEL_BYTES_V1)
            chunk = TF_TRANSFORM_CANCEL_BYTES_V1;
        if (tf_wasm_cancel_poll(slot->token)) return TF_TRANSFORM_CANCELLED;
        memcpy(destination + offset,
               (const uint8_t *)owned->value.data + offset, chunk);
        offset += chunk;
    }
    if (tf_wasm_cancel_poll(slot->token)) return TF_TRANSFORM_CANCELLED;
    return TF_TRANSFORM_OK;
}

TF_WASM_EXPORT
uint32_t tf_wasm_transform_handle_destroy(uint32_t handle) {
    tf_wasm_handle_slot *slot = tf_wasm_get_slot(handle, 0);
    uint16_t index = (uint16_t)(handle & TF_WASM_SLOT_MASK);
    if (!slot) return TF_TRANSFORM_INVALID_ARGUMENT;
    tf_wasm_destroy_pointer(slot->type, slot->pointer);
    tf_wasm_token_release(slot->token);
    free(slot->dtypes);
    slot->pointer = NULL;
    slot->token = NULL;
    slot->dtypes = NULL;
    slot->column_count = 0;
    slot->type = 0;
    if (tf_wasm_live_handles != 0) --tf_wasm_live_handles;
    if (slot->generation == TF_WASM_GENERATION_MASK) {
        slot->retired = 1;
        ++tf_wasm_retired_slots;
    } else {
        ++slot->generation;
        slot->next_free = tf_wasm_free_slot_head;
        tf_wasm_free_slot_head = index;
    }
    return TF_TRANSFORM_OK;
}
