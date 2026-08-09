#include "transform_internal.h"

#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct tf_json_work {
    const tf_transform_runtime_copy *runtime;
    tf_transform_resource_ledger *ledger;
    tf_transform_error **error;
    size_t bytes_since_poll;
    size_t iterations_since_poll;
    int cancelled;
} tf_json_work;

typedef struct tf_json_scan_metrics {
    uint64_t node_count_upper;
    uint64_t string_count;
    uint64_t number_count;
    uint64_t object_count;
    uint64_t decoded_string_bytes;
    uint64_t max_string_bytes;
    uint64_t max_number_bytes;
    uint64_t resident_bytes_persistent;
    uint64_t resident_bytes_upper;
    uint64_t allocation_count_upper;
} tf_json_scan_metrics;

static tf_transform_code json_work_progress(
    tf_json_work *work, size_t bytes, size_t iterations, int force) {
    tf_transform_code code;
    if (!work || !work->runtime) return TF_TRANSFORM_OK;
    if (bytes > SIZE_MAX - work->bytes_since_poll
        || iterations > SIZE_MAX - work->iterations_since_poll)
        return tf_transform_set_error(
            work->error, TF_TRANSFORM_RESOURCE_LIMIT,
            "JSON progress counter overflows");
    work->bytes_since_poll += bytes;
    work->iterations_since_poll += iterations;
    if (!force
        && work->bytes_since_poll < TF_TRANSFORM_CANCEL_BYTES_V1
        && work->iterations_since_poll < TF_TRANSFORM_CANCEL_ITERS_V1)
        return TF_TRANSFORM_OK;
    code = tf_transform_poll_cancel(work->runtime, work->error);
    if (code != TF_TRANSFORM_OK) {
        work->cancelled = 1;
        return code;
    }
    work->bytes_since_poll = 0;
    work->iterations_since_poll = 0;
    return TF_TRANSFORM_OK;
}

static cJSON_bool json_parser_poll(void *user_data) {
    tf_json_work *work = (tf_json_work *)user_data;
    if (!work || !work->runtime || !work->runtime->cancel) return 1;
    if (work->runtime->cancel(work->runtime->cancel_user)) {
        work->cancelled = 1;
        return 0;
    }
    return 1;
}

static tf_transform_code json_work_allocation(
    tf_json_work *work, uint64_t bytes, tf_transform_error **error) {
    if (!work || !work->ledger) return tf_transform_set_error(
        error, TF_TRANSFORM_INTERNAL, "JSON resource ledger is missing");
    return tf_transform_resource_reserve(work->ledger, bytes, error);
}

static void json_work_release(tf_json_work *work, uint64_t bytes) {
    if (work) tf_transform_resource_release(work->ledger, bytes);
}

static tf_transform_code buffer_reserve(
    tf_transform_buffer *buffer, size_t extra,
    const tf_transform_limits_v1 *limits, tf_json_work *work,
    tf_transform_error **error) {
    size_t needed;
    size_t capacity;
    uint8_t *data;
    if (extra > SIZE_MAX - buffer->len)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "canonical JSON size overflow");
    needed = buffer->len + extra;
    if (needed > limits->max_plan_bytes || needed > limits->max_allocation_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "canonical JSON exceeds byte limit");
    if (buffer->count_only || needed <= buffer->cap) return TF_TRANSFORM_OK;
    capacity = needed;
    if (buffer->data || capacity != buffer->cap)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "canonical JSON fixed buffer is too small");
    (void)work;
    (void)data;
    return TF_TRANSFORM_OK;
}

static tf_transform_code buffer_write(
    tf_transform_buffer *buffer, const void *data, size_t len,
    const tf_transform_limits_v1 *limits, tf_json_work *work,
    tf_transform_error **error) {
    tf_transform_code code = buffer_reserve(
        buffer, len, limits, work, error);
    if (code != TF_TRANSFORM_OK) return code;
    if (len != 0 && (!buffer->count_only && !buffer->data))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL, "canonical JSON buffer invariant failed");
    if (len != 0 && !data)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL, "canonical JSON source is null");
    if (len != 0 && !buffer->count_only)
        memcpy(buffer->data + buffer->len, data, len);
    buffer->len += len;
    return json_work_progress(work, len, 0, 0);
}

static tf_transform_code write_json_string(
    tf_transform_buffer *buffer, const char *text,
    const tf_transform_limits_v1 *limits, tf_json_work *work,
    tf_transform_error **error) {
    static const char hex[] = "0123456789abcdef";
    tf_transform_code code;
    size_t len = 0;
    if (!text) return tf_transform_set_error(
        error, TF_TRANSFORM_INTERNAL, "JSON string is null");
    while (len <= limits->max_string_bytes && text[len] != '\0') {
        code = json_work_progress(work, 1, 0, 0);
        if (code != TF_TRANSFORM_OK) return code;
        ++len;
    }
    if (len > limits->max_string_bytes) return tf_transform_set_error(
        error, TF_TRANSFORM_RESOURCE_LIMIT, "JSON string exceeds limit");
    code = buffer_write(buffer, "\"", 1, limits, work, error);
    if (code != TF_TRANSFORM_OK) return code;
    for (size_t i = 0; i < len; ++i) {
        unsigned char byte = (unsigned char)text[i];
        if (byte == '"' || byte == '\\') {
            char escaped[2] = {'\\', (char)byte};
            code = buffer_write(
                buffer, escaped, sizeof(escaped), limits, work, error);
        } else if (byte < 0x20) {
            char escaped[6] = {'\\', 'u', '0', '0', hex[byte >> 4], hex[byte & 15]};
            code = buffer_write(
                buffer, escaped, sizeof(escaped), limits, work, error);
        } else {
            code = buffer_write(buffer, &text[i], 1, limits, work, error);
        }
        if (code != TF_TRANSFORM_OK) return code;
    }
    return buffer_write(buffer, "\"", 1, limits, work, error);
}

static tf_transform_code compare_object_keys(
    const cJSON *left, const cJSON *right,
    const tf_transform_limits_v1 *limits, tf_json_work *work,
    int *result, tf_transform_error **error) {
    const unsigned char *a;
    const unsigned char *b;
    size_t compared = 0;
    if (!left || !right || !left->string || !right->string || !result)
        return tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN,
            "canonical JSON object key is invalid");
    a = (const unsigned char *)left->string;
    b = (const unsigned char *)right->string;
    while (compared <= limits->max_string_bytes && *a && *b && *a == *b) {
        tf_transform_code code = json_work_progress(work, 1, 0, 0);
        if (code != TF_TRANSFORM_OK) return code;
        ++a;
        ++b;
        ++compared;
    }
    if (compared > limits->max_string_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "canonical JSON object key exceeds limit");
    *result = *a < *b ? -1 : (*a > *b ? 1 : 0);
    return TF_TRANSFORM_OK;
}

static tf_transform_code sort_object_items(
    const cJSON **items, size_t count, tf_json_work *work,
    tf_transform_error **error) {
    size_t start;
    size_t end;
    (void)error;
    if (count < 2) return TF_TRANSFORM_OK;
    start = count / 2;
    end = count;
    for (;;) {
        size_t root;
        const cJSON *saved;
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
            if (child + 1 < end) {
                int compared;
                code = json_work_progress(work, 0, 1, 0);
                if (code != TF_TRANSFORM_OK) return code;
                code = compare_object_keys(
                    items[child], items[child + 1],
                    &work->runtime->limits, work, &compared, error);
                if (code != TF_TRANSFORM_OK) return code;
                if (compared < 0)
                    ++child;
            }
            code = json_work_progress(work, 0, 1, 0);
            if (code != TF_TRANSFORM_OK) return code;
            {
                int compared;
                code = compare_object_keys(
                    saved, items[child], &work->runtime->limits,
                    work, &compared, error);
                if (code != TF_TRANSFORM_OK) return code;
                if (compared >= 0) break;
            }
            items[root] = items[child];
            root = child;
        }
        items[root] = saved;
    }
    return TF_TRANSFORM_OK;
}

static tf_transform_code write_canonical_value(
    const cJSON *value, tf_transform_buffer *buffer,
    const tf_transform_limits_v1 *limits, size_t depth,
    tf_json_work *work, tf_transform_error **error) {
    tf_transform_code code;
    if (!value || depth > limits->max_json_depth)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "canonical JSON depth exceeds limit");
    code = json_work_progress(work, 0, 1, 0);
    if (code != TF_TRANSFORM_OK) return code;
    if (cJSON_IsNull(value))
        return buffer_write(buffer, "null", 4, limits, work, error);
    if (cJSON_IsTrue(value))
        return buffer_write(buffer, "true", 4, limits, work, error);
    if (cJSON_IsFalse(value))
        return buffer_write(buffer, "false", 5, limits, work, error);
    if (cJSON_IsString(value))
        return write_json_string(
            buffer, value->valuestring, limits, work, error);
    if (cJSON_IsNumber(value)) {
        char number[32];
        int count;
        double integer;
        if (!isfinite(value->valuedouble)
            || modf(value->valuedouble, &integer) != 0.0
            || integer < -9007199254740991.0
            || integer > 9007199254740991.0)
            return tf_transform_set_error(
                error, TF_TRANSFORM_CORRUPT_PLAN,
                "canonical JSON number is not a safe integer");
        if (integer == 0.0) integer = 0.0;
        count = snprintf(number, sizeof(number), "%.0f", integer);
        if (count <= 0 || (size_t)count >= sizeof(number))
            return tf_transform_set_error(
                error, TF_TRANSFORM_INTERNAL, "canonical integer formatting failed");
        return buffer_write(
            buffer, number, (size_t)count, limits, work, error);
    }
    if (cJSON_IsArray(value)) {
        const cJSON *child;
        int first = 1;
        code = buffer_write(buffer, "[", 1, limits, work, error);
        if (code != TF_TRANSFORM_OK) return code;
        cJSON_ArrayForEach(child, value) {
            if (!first) {
                code = buffer_write(buffer, ",", 1, limits, work, error);
                if (code != TF_TRANSFORM_OK) return code;
            }
            first = 0;
            code = write_canonical_value(
                child, buffer, limits, depth + 1, work, error);
            if (code != TF_TRANSFORM_OK) return code;
        }
        return buffer_write(buffer, "]", 1, limits, work, error);
    }
    if (cJSON_IsObject(value)) {
        const cJSON *child;
        const cJSON *count_child = value->child;
        const cJSON **items;
        size_t count = 0;
        size_t index = 0;
        cJSON_ArrayForEach(child, value) {
            code = json_work_progress(work, 0, 1, 0);
            if (code != TF_TRANSFORM_OK) return code;
            ++count;
        }
        if (count > limits->max_object_keys
            || count > SIZE_MAX / sizeof(items[0]))
            return tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT, "canonical object exceeds key limit");
        if (count > limits->max_allocation_bytes / sizeof(items[0]))
            return tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "canonical key array exceeds allocation limit");
        items = NULL;
        if (!buffer->count_only && count != 0) {
            uint64_t allocation_bytes = (uint64_t)count * sizeof(items[0]);
            code = json_work_allocation(work, allocation_bytes, error);
            if (code != TF_TRANSFORM_OK) return code;
            items = (const cJSON **)malloc((size_t)allocation_bytes);
        }
        if (!buffer->count_only && count && !items) {
            json_work_release(work, (uint64_t)count * sizeof(items[0]));
            return tf_transform_set_error(
                error, TF_TRANSFORM_ALLOCATION,
                "canonical key allocation failed");
        }
        if (!buffer->count_only) {
            cJSON_ArrayForEach(child, value) items[index++] = child;
            code = sort_object_items(items, count, work, error);
            if (code != TF_TRANSFORM_OK) goto object_done;
            for (size_t i = 1; i < count; ++i) {
                int compared;
                code = compare_object_keys(
                    items[i - 1], items[i], limits,
                    work, &compared, error);
                if (code != TF_TRANSFORM_OK) goto object_done;
                if (compared == 0) {
                    code = tf_transform_set_error(
                        error, TF_TRANSFORM_CORRUPT_PLAN,
                        "duplicate JSON object key");
                    goto object_done;
                }
            }
        }
        code = buffer_write(buffer, "{", 1, limits, work, error);
        for (size_t i = 0; code == TF_TRANSFORM_OK && i < count; ++i) {
            const cJSON *item = buffer->count_only ? count_child : items[i];
            if (!item) {
                code = tf_transform_set_error(
                    error, TF_TRANSFORM_INTERNAL,
                    "canonical object traversal failed");
                break;
            }
            if (i != 0)
                code = buffer_write(buffer, ",", 1, limits, work, error);
            if (code == TF_TRANSFORM_OK) code = write_json_string(
                buffer, item->string, limits, work, error);
            if (code == TF_TRANSFORM_OK)
                code = buffer_write(buffer, ":", 1, limits, work, error);
            if (code == TF_TRANSFORM_OK)
                code = write_canonical_value(
                    item, buffer, limits, depth + 1, work, error);
            if (buffer->count_only) count_child = item->next;
        }
object_done:
        free(items);
        if (!buffer->count_only && count != 0)
            json_work_release(work, (uint64_t)count * sizeof(items[0]));
        if (code != TF_TRANSFORM_OK) return code;
        return buffer_write(buffer, "}", 1, limits, work, error);
    }
    return tf_transform_set_error(
        error, TF_TRANSFORM_CORRUPT_PLAN, "unsupported canonical JSON value");
}

static tf_transform_code json_print_canonical_common(
    const cJSON *value, const tf_transform_runtime_copy *runtime,
    tf_transform_resource_ledger *shared_ledger,
    uint8_t **out, size_t *out_len, tf_transform_error **error) {
    tf_transform_buffer count_buffer = {0};
    tf_transform_buffer write_buffer = {0};
    tf_json_work work = {0};
    tf_transform_resource_ledger local_ledger;
    tf_transform_code code;
    if (out) *out = NULL;
    if (out_len) *out_len = 0;
    tf_transform_clear_error(error);
    if (!value || !runtime || !out || !out_len)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "canonical JSON argument is null");
    work.runtime = runtime;
    if (shared_ledger) work.ledger = shared_ledger;
    else {
        tf_transform_resource_ledger_init(&local_ledger, runtime);
        work.ledger = &local_ledger;
    }
    work.error = error;
    code = json_work_progress(&work, 0, 0, 1);
    if (code != TF_TRANSFORM_OK) return code;
    count_buffer.count_only = 1;
    code = write_canonical_value(
        value, &count_buffer, &runtime->limits, 1, &work, error);
    if (code != TF_TRANSFORM_OK) return code;
    if ((uint64_t)count_buffer.len > runtime->limits.max_plan_bytes
        || (uint64_t)count_buffer.len > runtime->limits.max_allocation_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "canonical JSON output exceeds limits");
    if (count_buffer.len != 0) {
        code = json_work_allocation(&work, (uint64_t)count_buffer.len, error);
        if (code != TF_TRANSFORM_OK) return code;
        write_buffer.data = (uint8_t *)malloc(count_buffer.len);
        if (!write_buffer.data) {
            json_work_release(&work, (uint64_t)count_buffer.len);
            return tf_transform_set_error(
                error, TF_TRANSFORM_ALLOCATION,
                "canonical JSON allocation failed");
        }
        write_buffer.cap = count_buffer.len;
    }
    code = write_canonical_value(
        value, &write_buffer, &runtime->limits, 1, &work, error);
    if (code != TF_TRANSFORM_OK) {
        free(write_buffer.data);
        json_work_release(&work, (uint64_t)count_buffer.len);
        return code;
    }
    if (write_buffer.len != count_buffer.len) {
        free(write_buffer.data);
        json_work_release(&work, (uint64_t)count_buffer.len);
        return tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "canonical JSON sizing pass drifted");
    }
    code = json_work_progress(&work, 0, 0, 1);
    if (code != TF_TRANSFORM_OK) {
        free(write_buffer.data);
        json_work_release(&work, (uint64_t)count_buffer.len);
        return code;
    }
    *out = write_buffer.data;
    *out_len = write_buffer.len;
    return TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_json_print_canonical_runtime(
    const cJSON *value, const tf_transform_runtime_copy *runtime,
    uint8_t **out, size_t *out_len, tf_transform_error **error) {
    return json_print_canonical_common(
        value, runtime, NULL, out, out_len, error);
}

tf_transform_code tf_transform_json_print_canonical_runtime_ledger(
    const cJSON *value, const tf_transform_runtime_copy *runtime,
    tf_transform_resource_ledger *ledger,
    uint8_t **out, size_t *out_len, tf_transform_error **error) {
    if (!ledger || ledger->runtime != runtime) {
        if (out) *out = NULL;
        if (out_len) *out_len = 0;
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "canonical JSON resource ledger is invalid");
    }
    return json_print_canonical_common(
        value, runtime, ledger, out, out_len, error);
}

tf_transform_code tf_transform_json_print_canonical(
    const cJSON *value, const tf_transform_limits_v1 *limits,
    uint8_t **out, size_t *out_len, tf_transform_error **error) {
    tf_transform_runtime_copy runtime;
    if (!limits) {
        if (out) *out = NULL;
        if (out_len) *out_len = 0;
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "canonical JSON limits are null");
    }
    memset(&runtime, 0, sizeof(runtime));
    runtime.limits = *limits;
    return json_print_canonical_common(
        value, &runtime, NULL, out, out_len, error);
}

static int json_string_token_equals_ascii(
    const uint8_t *token, size_t len, const char *expected) {
    size_t input = 0;
    size_t output = 0;
    size_t expected_len = strlen(expected);
    while (input < len) {
        uint32_t decoded = token[input++];
        if (decoded == '\\') {
            unsigned char escaped;
            if (input >= len) return 0;
            escaped = token[input++];
            if (escaped == 'u') {
                decoded = 0;
                if (len - input < 4) return 0;
                for (size_t i = 0; i < 4; ++i) {
                    unsigned char byte = token[input++];
                    unsigned int nibble;
                    if (byte >= '0' && byte <= '9') nibble = byte - '0';
                    else if (byte >= 'a' && byte <= 'f')
                        nibble = byte - 'a' + 10u;
                    else if (byte >= 'A' && byte <= 'F')
                        nibble = byte - 'A' + 10u;
                    else return 0;
                    decoded = (decoded << 4) | nibble;
                }
            } else if (escaped == '"' || escaped == '\\' || escaped == '/') {
                decoded = escaped;
            } else if (escaped == 'b') decoded = '\b';
            else if (escaped == 'f') decoded = '\f';
            else if (escaped == 'n') decoded = '\n';
            else if (escaped == 'r') decoded = '\r';
            else if (escaped == 't') decoded = '\t';
            else return 0;
        }
        if (decoded > UINT8_MAX || output >= expected_len
            || (unsigned char)expected[output] != (unsigned char)decoded)
            return 0;
        ++output;
    }
    return output == expected_len;
}

static tf_transform_code scan_json_array_value(
    size_t depth, const char *containers,
    unsigned char *array_expects_value,
    const unsigned char *category_arrays,
    uint64_t *category_counts, size_t *array_elements,
    uint64_t *total_categories, const tf_transform_limits_v1 *limits,
    tf_transform_error **error) {
    size_t index;
    if (depth == 0 || containers[depth - 1] != '['
        || !array_expects_value[depth - 1]) return TF_TRANSFORM_OK;
    index = depth - 1;
    if (*array_elements == SIZE_MAX)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "JSON array element count overflows");
    ++*array_elements;
    array_expects_value[index] = 0;
    if (!category_arrays[index]) return TF_TRANSFORM_OK;
    if (category_counts[index] == UINT64_MAX
        || *total_categories == UINT64_MAX)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "JSON category count overflows");
    ++category_counts[index];
    ++*total_categories;
    if (category_counts[index] > limits->max_categories_per_column
        || *total_categories > limits->max_total_categories)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "JSON categories exceed configured limits before parsing");
    return TF_TRANSFORM_OK;
}

static tf_transform_code scan_json_bounds(
    const uint8_t *json, size_t len, const tf_transform_limits_v1 *limits,
    tf_json_work *work, tf_json_scan_metrics *metrics,
    tf_transform_error **error, tf_transform_code malformed_code) {
    size_t depth = 0;
    size_t keys = 0;
    size_t strings = 0;
    size_t current_string = 0;
    size_t array_elements = 0;
    size_t objects = 0;
    size_t string_count = 0;
    size_t number_count = 0;
    size_t max_string = 0;
    size_t current_number = 0;
    size_t max_number = 0;
    size_t last_string_start = 0;
    size_t last_string_end = 0;
    uint64_t total_categories = 0;
    int in_string = 0;
    int escaped = 0;
    int in_number = 0;
    int have_last_string = 0;
    int expect_categories_array = 0;
    char containers[CJSON_NESTING_LIMIT];
    unsigned char array_expects_value[CJSON_NESTING_LIMIT];
    unsigned char category_arrays[CJSON_NESTING_LIMIT];
    uint64_t category_counts[CJSON_NESTING_LIMIT];
    memset(category_arrays, 0, sizeof(category_arrays));
    memset(category_counts, 0, sizeof(category_counts));
    memset(metrics, 0, sizeof(*metrics));
    for (size_t i = 0; i < len; ++i) {
        uint8_t byte = json[i];
        tf_transform_code progress = json_work_progress(work, 1, 0, 0);
        if (progress != TF_TRANSFORM_OK) return progress;
        if (byte == 0) return tf_transform_set_error(
            error, malformed_code, "JSON contains an embedded null byte");
        if (in_string) {
            if (escaped) {
                if (byte == 'u' && i + 4 < len
                    && json[i + 1] == '0' && json[i + 2] == '0'
                    && json[i + 3] == '0' && json[i + 4] == '0')
                    return tf_transform_set_error(
                        error, malformed_code,
                        "JSON strings may not contain U+0000");
                ++current_string;
                escaped = 0;
            } else if (byte == '\\') {
                ++current_string;
                escaped = 1;
            }
            else if (byte == '"') {
                in_string = 0;
                if (string_count == SIZE_MAX) return tf_transform_set_error(
                    error, TF_TRANSFORM_RESOURCE_LIMIT,
                    "JSON string count overflows");
                ++string_count;
                if (current_string > max_string) max_string = current_string;
                if (current_string > limits->max_string_bytes)
                    return tf_transform_set_error(
                        error, TF_TRANSFORM_RESOURCE_LIMIT,
                        "JSON string exceeds resource limit");
                if (strings > SIZE_MAX - current_string)
                    return tf_transform_set_error(
                        error, TF_TRANSFORM_RESOURCE_LIMIT,
                        "JSON string byte counter overflow");
                strings += current_string;
                if (strings > limits->max_decoded_string_bytes)
                    return tf_transform_set_error(
                        error, TF_TRANSFORM_RESOURCE_LIMIT,
                        "JSON strings exceed resource limit");
                last_string_end = i;
                have_last_string = 1;
            } else {
                ++current_string;
            }
            continue;
        }
        if (in_number) {
            if ((byte >= '0' && byte <= '9') || byte == '+' || byte == '-'
                || byte == '.' || byte == 'e' || byte == 'E') {
                if (current_number == SIZE_MAX) return tf_transform_set_error(
                    error, TF_TRANSFORM_RESOURCE_LIMIT,
                    "JSON number length overflows");
                ++current_number;
                continue;
            }
            in_number = 0;
            if (number_count == SIZE_MAX) return tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "JSON number count overflows");
            ++number_count;
            if (current_number > max_number) max_number = current_number;
            current_number = 0;
        }
        if (byte == '"') {
            tf_transform_code code = scan_json_array_value(
                depth, containers, array_expects_value,
                category_arrays, category_counts, &array_elements,
                &total_categories, limits, error);
            if (code != TF_TRANSFORM_OK) return code;
            expect_categories_array = 0;
            have_last_string = 0;
            in_string = 1;
            current_string = 0;
            last_string_start = i + 1;
        } else if (byte == '{' || byte == '[') {
            int is_category_array = byte == '[' && expect_categories_array;
            tf_transform_code code = scan_json_array_value(
                depth, containers, array_expects_value,
                category_arrays, category_counts, &array_elements,
                &total_categories, limits, error);
            if (code != TF_TRANSFORM_OK) return code;
            expect_categories_array = 0;
            have_last_string = 0;
            if (byte == '{') ++objects;
            if (depth >= CJSON_NESTING_LIMIT
                || ++depth > limits->max_json_depth)
                return tf_transform_set_error(
                    error, TF_TRANSFORM_RESOURCE_LIMIT, "JSON depth exceeds limit");
            containers[depth - 1] = (char)byte;
            array_expects_value[depth - 1] = byte == '[' ? 1u : 0u;
            category_arrays[depth - 1] = is_category_array ? 1u : 0u;
            category_counts[depth - 1] = 0;
        } else if (byte == '}' || byte == ']') {
            expect_categories_array = 0;
            have_last_string = 0;
            if (depth == 0) return tf_transform_set_error(
                error, malformed_code, "unbalanced JSON structure");
            if ((byte == '}' && containers[depth - 1] != '{')
                || (byte == ']' && containers[depth - 1] != '['))
                return tf_transform_set_error(
                    error, malformed_code, "mismatched JSON structure");
            --depth;
        } else if (byte == ':') {
            if (++keys > limits->max_object_keys)
                return tf_transform_set_error(
                    error, TF_TRANSFORM_RESOURCE_LIMIT, "JSON key count exceeds limit");
            expect_categories_array = have_last_string
                && json_string_token_equals_ascii(
                    json + last_string_start,
                    last_string_end - last_string_start, "categories");
            have_last_string = 0;
        } else if (byte == ',') {
            expect_categories_array = 0;
            have_last_string = 0;
            if (depth != 0 && containers[depth - 1] == '[')
                array_expects_value[depth - 1] = 1;
        } else if (byte == '-' || (byte >= '0' && byte <= '9')) {
            tf_transform_code code = scan_json_array_value(
                depth, containers, array_expects_value,
                category_arrays, category_counts, &array_elements,
                &total_categories, limits, error);
            if (code != TF_TRANSFORM_OK) return code;
            expect_categories_array = 0;
            have_last_string = 0;
            in_number = 1;
            current_number = 1;
        } else if (byte == 't' || byte == 'f' || byte == 'n') {
            tf_transform_code code = scan_json_array_value(
                depth, containers, array_expects_value,
                category_arrays, category_counts, &array_elements,
                &total_categories, limits, error);
            if (code != TF_TRANSFORM_OK) return code;
            expect_categories_array = 0;
            have_last_string = 0;
        } else if (byte != ' ' && byte != '\t'
                   && byte != '\r' && byte != '\n') {
            expect_categories_array = 0;
            have_last_string = 0;
        }
    }
    if (in_number) {
        ++number_count;
        if (current_number > max_number) max_number = current_number;
    }
    if (in_string || depth != 0) return tf_transform_set_error(
        error, malformed_code, "unterminated JSON structure");
    if (max_number > 64) return tf_transform_set_error(
        error, malformed_code,
        "JSON numeric token exceeds bounded safe-integer syntax");
    {
        uint64_t nodes = 1;
        uint64_t allocation_count;
        uint64_t resident;
        uint64_t persistent;
        if ((uint64_t)keys > UINT64_MAX - nodes) goto metric_overflow;
        nodes += (uint64_t)keys;
        if ((uint64_t)array_elements > UINT64_MAX - nodes) goto metric_overflow;
        nodes += (uint64_t)array_elements;
        allocation_count = nodes;
        if ((uint64_t)string_count > UINT64_MAX - allocation_count)
            goto metric_overflow;
        allocation_count += (uint64_t)string_count;
        if ((uint64_t)number_count > UINT64_MAX - allocation_count)
            goto metric_overflow;
        allocation_count += (uint64_t)number_count;
        if (nodes > UINT64_MAX / sizeof(cJSON)) goto metric_overflow;
        persistent = nodes * sizeof(cJSON);
        if ((uint64_t)strings > UINT64_MAX - persistent) goto metric_overflow;
        persistent += (uint64_t)strings;
        if ((uint64_t)string_count > UINT64_MAX - persistent)
            goto metric_overflow;
        persistent += (uint64_t)string_count;
        resident = persistent;
        if (number_count != 0
            && (uint64_t)max_number + 1 > UINT64_MAX - resident)
            goto metric_overflow;
        if (number_count != 0) resident += (uint64_t)max_number + 1;
        if (allocation_count > limits->max_allocations_per_session
            || resident > limits->max_resident_state_bytes
            || sizeof(cJSON) > limits->max_allocation_bytes
            || (string_count != 0
                && (uint64_t)max_string + 1 > limits->max_allocation_bytes)
            || (number_count != 0
                && (uint64_t)max_number + 1 > limits->max_allocation_bytes))
            return tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "JSON parser growth exceeds session limits");
        metrics->node_count_upper = nodes;
        metrics->string_count = string_count;
        metrics->number_count = number_count;
        metrics->object_count = objects;
        metrics->decoded_string_bytes = strings;
        metrics->max_string_bytes = max_string;
        metrics->max_number_bytes = max_number;
        metrics->resident_bytes_persistent = persistent;
        metrics->resident_bytes_upper = resident;
        metrics->allocation_count_upper = allocation_count;
    }
    return TF_TRANSFORM_OK;
metric_overflow:
    return tf_transform_set_error(
        error, TF_TRANSFORM_RESOURCE_LIMIT,
        "JSON parser resource estimate overflows");
}

static int json_valid_utf8_with_work(
    const uint8_t *data, size_t len, tf_json_work *work) {
    size_t i = 0;
    while (i < len) {
        uint8_t c = data[i++];
        uint32_t code;
        size_t needed;
        if (json_work_progress(work, 1, 0, 0) != TF_TRANSFORM_OK) return -1;
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
        } else return 0;
        if (needed > len - i) return 0;
        for (size_t j = 0; j < needed; ++j) {
            uint8_t next = data[i++];
            if (json_work_progress(work, 1, 0, 0) != TF_TRANSFORM_OK) return -1;
            if ((next & 0xc0) != 0x80) return 0;
            code = (code << 6) | (uint32_t)(next & 0x3f);
        }
        if ((needed == 1 && code < 0x80)
            || (needed == 2 && code < 0x800)
            || (needed == 3 && code < 0x10000)
            || code > 0x10ffff
            || (code >= 0xd800 && code <= 0xdfff)) return 0;
    }
    return 1;
}

static tf_transform_code reject_duplicate_keys(
    const cJSON *value, size_t depth, const tf_transform_limits_v1 *limits,
    tf_json_work *work, tf_transform_error **error,
    tf_transform_code malformed_code) {
    const cJSON *child;
    tf_transform_code code;
    if (depth > limits->max_json_depth) return tf_transform_set_error(
        error, TF_TRANSFORM_RESOURCE_LIMIT, "JSON depth exceeds limit");
    if (cJSON_IsObject(value)) {
        const cJSON **items = NULL;
        size_t count = 0;
        size_t index = 0;
        cJSON_ArrayForEach(child, value) ++count;
        if (count > 1) {
            uint64_t bytes = (uint64_t)count * sizeof(items[0]);
            code = json_work_allocation(work, bytes, error);
            if (code != TF_TRANSFORM_OK) return code;
            items = (const cJSON **)malloc((size_t)bytes);
            if (!items) {
                json_work_release(work, bytes);
                return tf_transform_set_error(
                    error, TF_TRANSFORM_ALLOCATION,
                    "duplicate-key index allocation failed");
            }
            cJSON_ArrayForEach(child, value) items[index++] = child;
            code = sort_object_items(items, count, work, error);
            if (code == TF_TRANSFORM_OK) {
                for (size_t i = 1; i < count; ++i) {
                    int compared;
                    code = json_work_progress(work, 0, 1, 0);
                    if (code != TF_TRANSFORM_OK) break;
                    code = compare_object_keys(
                        items[i - 1], items[i], limits,
                        work, &compared, error);
                    if (code != TF_TRANSFORM_OK) break;
                    if (compared == 0) {
                        code = tf_transform_set_error(
                            error, malformed_code,
                            "duplicate JSON object key");
                        break;
                    }
                }
            }
            free(items);
            json_work_release(work, bytes);
            if (code != TF_TRANSFORM_OK) return code;
        }
    }
    cJSON_ArrayForEach(child, value) {
        code = json_work_progress(work, 0, 1, 0);
        if (code != TF_TRANSFORM_OK) return code;
        code = reject_duplicate_keys(
            child, depth + 1, limits, work, error, malformed_code);
        if (code != TF_TRANSFORM_OK) return code;
    }
    return TF_TRANSFORM_OK;
}

static void delete_json_runtime_walk(
    cJSON *item, const tf_transform_runtime_copy *runtime,
    size_t *iterations, int *cancelled) {
    while (item) {
        cJSON *next = item->next;
        if (!(item->type & cJSON_IsReference) && item->child)
            delete_json_runtime_walk(
                item->child, runtime, iterations, cancelled);
        if (!(item->type & cJSON_IsReference) && item->valuestring)
            cJSON_free(item->valuestring);
        if (!(item->type & cJSON_StringIsConst) && item->string)
            cJSON_free(item->string);
        cJSON_free(item);
        ++*iterations;
        if (*iterations % TF_TRANSFORM_CANCEL_ITERS_V1 == 0
            && runtime && runtime->cancel
            && runtime->cancel(runtime->cancel_user))
            *cancelled = 1;
        item = next;
    }
}

static int delete_json_runtime(
    cJSON *item, const tf_transform_runtime_copy *runtime) {
    size_t iterations = 0;
    int cancelled = 0;
    if (runtime && runtime->cancel && runtime->cancel(runtime->cancel_user))
        cancelled = 1;
    delete_json_runtime_walk(item, runtime, &iterations, &cancelled);
    if (runtime && runtime->cancel && runtime->cancel(runtime->cancel_user))
        cancelled = 1;
    return cancelled;
}

static tf_transform_code json_parse_bounded_common(
    const uint8_t *json, size_t json_len,
    const tf_transform_runtime_copy *runtime,
    tf_transform_resource_ledger *shared_ledger, cJSON **out,
    uint64_t *resident_bytes_out, uint64_t *allocation_count_out,
    tf_transform_error **error, tf_transform_code malformed_code) {
    cJSON *value;
    const char *parse_end = NULL;
    tf_transform_code code;
    tf_json_work work = {0};
    tf_transform_resource_ledger local_ledger;
    tf_transform_resource_ledger *ledger;
    tf_json_scan_metrics metrics;
    uint64_t allocation_start;
    int valid_utf8;
    if (out) *out = NULL;
    if (resident_bytes_out) *resident_bytes_out = 0;
    if (allocation_count_out) *allocation_count_out = 0;
    if (!json || json_len == 0 || !runtime || !out)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "JSON argument is null or empty");
    work.runtime = runtime;
    if (shared_ledger) ledger = shared_ledger;
    else {
        tf_transform_resource_ledger_init(&local_ledger, runtime);
        ledger = &local_ledger;
    }
    work.ledger = ledger;
    work.error = error;
    allocation_start = ledger->allocation_count;
    code = json_work_progress(&work, 0, 0, 1);
    if (code != TF_TRANSFORM_OK) return code;
    if (json_len > runtime->limits.max_plan_bytes || json_len > SIZE_MAX - 1)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "JSON input exceeds byte limit");
    valid_utf8 = json_valid_utf8_with_work(json, json_len, &work);
    if (valid_utf8 < 0) return TF_TRANSFORM_CANCELLED;
    if (!valid_utf8)
        return tf_transform_set_error(
            error, malformed_code, "JSON is not valid UTF-8");
    code = scan_json_bounds(
        json, json_len, &runtime->limits, &work, &metrics,
        error, malformed_code);
    if (code != TF_TRANSFORM_OK) return code;
    code = tf_transform_resource_charge_batch(
        ledger, metrics.allocation_count_upper,
        metrics.resident_bytes_persistent, metrics.resident_bytes_upper,
        error);
    if (code != TF_TRANSFORM_OK) return code;
    value = cJSON_ParseWithLengthOptsAndPoll(
        (const char *)json, json_len, &parse_end, 0,
        json_parser_poll, &work, TF_TRANSFORM_CANCEL_BYTES_V1,
        TF_TRANSFORM_CANCEL_ITERS_V1);
    if (!value && work.cancelled) return tf_transform_set_error(
        error, TF_TRANSFORM_CANCELLED, "prepared transform cancelled");
    if (!value) return tf_transform_set_error(
        error, cJSON_ParseHadAllocationFailure()
            ? TF_TRANSFORM_ALLOCATION : malformed_code,
        cJSON_ParseHadAllocationFailure()
            ? "JSON parser allocation failed" : "invalid JSON");
    while ((size_t)(parse_end - (const char *)json) < json_len
           && (*parse_end == ' ' || *parse_end == '\t'
               || *parse_end == '\r' || *parse_end == '\n')) {
        code = json_work_progress(&work, 1, 0, 0);
        if (code != TF_TRANSFORM_OK) {
            (void)delete_json_runtime(value, runtime);
            return code;
        }
        ++parse_end;
    }
    if ((size_t)(parse_end - (const char *)json) != json_len) {
        (void)delete_json_runtime(value, runtime);
        return tf_transform_set_error(
            error, malformed_code, "JSON contains trailing data");
    }
    code = reject_duplicate_keys(
        value, 1, &runtime->limits, &work, error, malformed_code);
    if (code != TF_TRANSFORM_OK) {
        (void)delete_json_runtime(value, runtime);
        return code;
    }
    code = json_work_progress(&work, 0, 0, 1);
    if (code != TF_TRANSFORM_OK) {
        (void)delete_json_runtime(value, runtime);
        return code;
    }
    *out = value;
    if (resident_bytes_out)
        *resident_bytes_out = metrics.resident_bytes_persistent;
    if (allocation_count_out)
        *allocation_count_out = ledger->allocation_count - allocation_start;
    return TF_TRANSFORM_OK;
}

tf_transform_code tf_transform_json_parse_bounded_runtime(
    const uint8_t *json, size_t json_len,
    const tf_transform_runtime_copy *runtime, cJSON **out,
    uint64_t *resident_bytes_out, uint64_t *allocation_count_out,
    tf_transform_error **error, tf_transform_code malformed_code) {
    return json_parse_bounded_common(
        json, json_len, runtime, NULL, out,
        resident_bytes_out, allocation_count_out,
        error, malformed_code);
}

tf_transform_code tf_transform_json_parse_bounded_runtime_ledger(
    const uint8_t *json, size_t json_len,
    const tf_transform_runtime_copy *runtime,
    tf_transform_resource_ledger *ledger, cJSON **out,
    uint64_t *resident_bytes_out, uint64_t *allocation_count_out,
    tf_transform_error **error, tf_transform_code malformed_code) {
    if (!ledger || ledger->runtime != runtime) {
        if (out) *out = NULL;
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "JSON parser resource ledger is invalid");
    }
    return json_parse_bounded_common(
        json, json_len, runtime, ledger, out,
        resident_bytes_out, allocation_count_out,
        error, malformed_code);
}

tf_transform_code tf_transform_json_parse_bounded(
    const uint8_t *json, size_t json_len,
    const tf_transform_limits_v1 *limits, cJSON **out,
    tf_transform_error **error, tf_transform_code malformed_code) {
    tf_transform_runtime_copy runtime;
    if (!limits) {
        if (out) *out = NULL;
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "JSON limits are null");
    }
    memset(&runtime, 0, sizeof(runtime));
    runtime.limits = *limits;
    return json_parse_bounded_common(
        json, json_len, &runtime, NULL, out, NULL, NULL,
        error, malformed_code);
}

static int exact_keys(const cJSON *object, const char *const *keys, size_t count) {
    const cJSON *child;
    size_t seen = 0;
    if (!cJSON_IsObject(object)) return 0;
    cJSON_ArrayForEach(child, object) {
        int found = 0;
        for (size_t i = 0; i < count; ++i) {
            if (strcmp(child->string, keys[i]) == 0) { found = 1; break; }
        }
        if (!found) return 0;
        ++seen;
    }
    return seen == count;
}

static const cJSON *required_item(const cJSON *object, const char *name) {
    return cJSON_GetObjectItemCaseSensitive(object, name);
}

static int json_string_equals(const cJSON *value, const char *expected) {
    return cJSON_IsString(value) && value->valuestring
        && strcmp(value->valuestring, expected) == 0;
}

static int safe_uint64(const cJSON *value, uint64_t *out) {
    double integer;
    if (!cJSON_IsNumber(value) || !isfinite(value->valuedouble)
        || value->valuedouble < 0
        || modf(value->valuedouble, &integer) != 0.0
        || integer >= 9007199254740992.0) return 0;
    *out = (uint64_t)integer;
    return 1;
}

static int safe_int64(const cJSON *value, int64_t *out) {
    double integer;
    if (!cJSON_IsNumber(value) || !isfinite(value->valuedouble)
        || modf(value->valuedouble, &integer) != 0.0
        || integer <= -9007199254740992.0
        || integer >= 9007199254740992.0) return 0;
    *out = (int64_t)integer;
    return 1;
}

static tf_transform_code runtime_cstring_length(
    const char *text, size_t maximum,
    const tf_transform_runtime_copy *runtime,
    size_t *out, tf_transform_error **error) {
    size_t length = 0;
    if (!text || !runtime || !out)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "runtime string argument is null");
    while (length <= maximum && text[length] != '\0') {
        if (length % TF_TRANSFORM_CANCEL_BYTES_V1 == 0) {
            tf_transform_code code = tf_transform_poll_cancel(runtime, error);
            if (code != TF_TRANSFORM_OK) return code;
        }
        ++length;
    }
    if (length > maximum)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "runtime string exceeds byte limit");
    *out = length;
    return tf_transform_poll_cancel(runtime, error);
}

static tf_transform_code runtime_span_equal(
    const char *left, size_t left_len,
    const char *right, size_t right_len,
    const tf_transform_runtime_copy *runtime,
    int *equal, tf_transform_error **error) {
    int comparison = 0;
    tf_transform_code code;
    if (!left || !right || !runtime || !equal)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "runtime span argument is null");
    code = tf_transform_compare_bytes_runtime(
        left, left_len, right, right_len,
        runtime, &comparison, error);
    if (code != TF_TRANSFORM_OK) return code;
    *equal = comparison == 0;
    return TF_TRANSFORM_OK;
}

static int parse_tagged_bits(
    const cJSON *value, uint64_t *out, uint32_t *dtype) {
    static const char *const keys[] = {"t", "v"};
    const cJSON *tag;
    const cJSON *bits;
    uint64_t decoded = 0;
    size_t digits;
    if (!out || !dtype || !exact_keys(value, keys, 2)) return 0;
    tag = required_item(value, "t");
    bits = required_item(value, "v");
    if (!cJSON_IsString(tag) || !tag->valuestring
        || !cJSON_IsString(bits) || !bits->valuestring) return 0;
    if (strcmp(tag->valuestring, "f64") == 0) {
        *dtype = TF_VIEW_FLOAT64;
        digits = 16;
    } else if (strcmp(tag->valuestring, "f32") == 0) {
        *dtype = TF_VIEW_FLOAT32;
        digits = 8;
    } else return 0;
    for (size_t i = 0; i < digits; ++i) {
        unsigned char c = (unsigned char)bits->valuestring[i];
        unsigned int nibble;
        if (c == '\0') return 0;
        if (c >= '0' && c <= '9') nibble = c - '0';
        else if (c >= 'a' && c <= 'f') nibble = c - 'a' + 10u;
        else return 0;
        decoded = (decoded << 4) | nibble;
    }
    if (bits->valuestring[digits] != '\0') return 0;
    *out = decoded;
    return 1;
}

static int parse_tagged_constant(const cJSON *value, double *out, uint32_t *dtype) {
    uint64_t decoded = 0;
    if (!out || !parse_tagged_bits(value, &decoded, dtype)) return 0;
    if (*dtype == TF_VIEW_FLOAT64) *out = tf_transform_double_from_bits(decoded);
    else {
        uint32_t fbits = (uint32_t)decoded;
        float fvalue;
        memcpy(&fvalue, &fbits, sizeof(fvalue));
        *out = (double)fvalue;
    }
    return tf_transform_double_is_finite(*out);
}

static tf_transform_code parse_recipe_column(
    const cJSON *value, const tf_transform_runtime_copy *runtime,
    tf_transform_resource_ledger *ledger,
    tf_transform_recipe_column *out, tf_transform_error **error) {
    static const char *const column_keys[] = {
        "categorical", "kind", "numeric", "sourceId"
    };
    static const char *const kind_keys[] = {
        "maxCategories", "op", "rule", "value"
    };
    static const char *const numeric_keys[] = {"impute", "normalize"};
    static const char *const impute_keys[] = {"allMissing", "constant", "op"};
    static const char *const normalize_keys[] = {"ddof", "op"};
    static const char *const categorical_keys[] = {"encode", "impute"};
    static const char *const encode_keys[] = {
        "categories", "op", "sentinelLabel", "unknown"
    };
    const cJSON *kind;
    const cJSON *numeric;
    const cJSON *categorical;
    const cJSON *impute;
    const cJSON *normalize;
    const cJSON *source_id;
    const cJSON *op;
    const cJSON *policy;
    const cJSON *constant;
    size_t source_len;
    memset(out, 0, sizeof(*out));
    const tf_transform_limits_v1 *limits = &runtime->limits;
    tf_transform_code code;
    if (!exact_keys(value, column_keys, 4))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_RECIPE, "recipe column shape is invalid");
    kind = required_item(value, "kind");
    if (!exact_keys(kind, kind_keys, 4)
        || !cJSON_IsNull(required_item(kind, "maxCategories"))
        || !json_string_equals(required_item(kind, "op"), "declared")
        || !cJSON_IsNull(required_item(kind, "rule")))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_RECIPE,
            "prepared transforms require a supported declared kind");
    source_id = required_item(value, "sourceId");
    if (!cJSON_IsString(source_id) || !source_id->valuestring)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_RECIPE, "invalid recipe sourceId");
    code = runtime_cstring_length(
        source_id->valuestring, (size_t)limits->max_string_bytes,
        runtime, &source_len, error);
    if (code != TF_TRANSFORM_OK || source_len == 0)
        return code != TF_TRANSFORM_OK ? code : tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_RECIPE, "invalid recipe sourceId");
    out->source_id = (char *)tf_transform_resource_malloc(
        ledger, source_len + 1, error);
    if (!out->source_id) return ledger->last_code != TF_TRANSFORM_OK
        ? ledger->last_code : tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION,
            "recipe sourceId allocation failed");
    code = tf_transform_copy_bytes_runtime(
        out->source_id, source_id->valuestring, source_len + 1,
        runtime, error);
    if (code != TF_TRANSFORM_OK) goto cleanup;
    out->source_id_len = source_len;
    if (json_string_equals(required_item(kind, "value"), "categorical")) {
        const cJSON *encode;
        out->kind = TF_TRANSFORM_KIND_CATEGORICAL;
        if (!cJSON_IsNull(required_item(value, "numeric"))) goto invalid;
        categorical = required_item(value, "categorical");
        if (!exact_keys(categorical, categorical_keys, 2)) goto invalid;
        impute = required_item(categorical, "impute");
        encode = required_item(categorical, "encode");
        if (!exact_keys(impute, impute_keys, 3)
            || !json_string_equals(required_item(impute, "op"), "mode")
            || !cJSON_IsNull(required_item(impute, "constant"))) goto invalid;
        policy = required_item(impute, "allMissing");
        if (json_string_equals(policy, "error"))
            out->categorical_all_missing = TF_TRANSFORM_ALL_MISSING_ERROR;
        else if (json_string_equals(policy, "zero"))
            out->categorical_all_missing = TF_TRANSFORM_ALL_MISSING_ZERO;
        else goto invalid;
        out->categorical_impute = TF_TRANSFORM_CATEGORICAL_IMPUTE_MODE;
        if (!exact_keys(encode, encode_keys, 4)
            || !json_string_equals(required_item(encode, "categories"), "discover"))
            goto invalid;
        if (json_string_equals(required_item(encode, "op"), "none")) {
            if (!cJSON_IsNull(required_item(encode, "sentinelLabel"))
                || !cJSON_IsNull(required_item(encode, "unknown"))) goto invalid;
            out->categorical_encode = TF_TRANSFORM_ENCODE_NONE;
            out->categorical_unknown = TF_TRANSFORM_UNKNOWN_NONE;
        } else if (json_string_equals(required_item(encode, "op"), "label")) {
            const cJSON *unknown = required_item(encode, "unknown");
            const cJSON *sentinel = required_item(encode, "sentinelLabel");
            out->categorical_encode = TF_TRANSFORM_ENCODE_LABEL;
            if (json_string_equals(unknown, "error")
                && cJSON_IsNull(sentinel))
                out->categorical_unknown = TF_TRANSFORM_UNKNOWN_ERROR;
            else if (json_string_equals(unknown, "other")
                     && cJSON_IsNull(sentinel))
                out->categorical_unknown = TF_TRANSFORM_UNKNOWN_OTHER;
            else if (json_string_equals(unknown, "sentinel")
                     && safe_int64(sentinel, &out->categorical_sentinel_label)) {
                out->categorical_unknown = TF_TRANSFORM_UNKNOWN_SENTINEL;
                out->categorical_has_sentinel_label = 1;
            } else goto invalid;
        } else goto invalid;
        out->categorical_discover = 1;
        return TF_TRANSFORM_OK;
    }
    if (!json_string_equals(required_item(kind, "value"), "numeric")
        || !cJSON_IsNull(required_item(value, "categorical"))) goto invalid;
    out->kind = TF_TRANSFORM_KIND_NUMERIC;
    numeric = required_item(value, "numeric");
    if (!exact_keys(numeric, numeric_keys, 2)) goto invalid;
    impute = required_item(numeric, "impute");
    if (!exact_keys(impute, impute_keys, 3)) goto invalid;
    op = required_item(impute, "op");
    policy = required_item(impute, "allMissing");
    constant = required_item(impute, "constant");
    if (json_string_equals(op, "none")) {
        if (!cJSON_IsNull(policy) || !cJSON_IsNull(constant)) goto invalid;
        out->impute = TF_TRANSFORM_IMPUTE_NONE;
    } else if (json_string_equals(op, "zero")) {
        if (!cJSON_IsNull(policy) || !cJSON_IsNull(constant)) goto invalid;
        out->impute = TF_TRANSFORM_IMPUTE_ZERO;
        out->constant = 0.0;
    } else if (json_string_equals(op, "constant")) {
        if (!cJSON_IsNull(policy)
            || !parse_tagged_constant(constant, &out->constant, &out->constant_dtype))
            goto invalid;
        out->impute = TF_TRANSFORM_IMPUTE_CONSTANT;
    } else if (json_string_equals(op, "mean")
               || json_string_equals(op, "median")) {
        if (!cJSON_IsNull(constant)) goto invalid;
        if (json_string_equals(policy, "error"))
            out->all_missing = TF_TRANSFORM_ALL_MISSING_ERROR;
        else if (json_string_equals(policy, "zero"))
            out->all_missing = TF_TRANSFORM_ALL_MISSING_ZERO;
        else goto invalid;
        out->impute = json_string_equals(op, "mean")
            ? TF_TRANSFORM_IMPUTE_MEAN : TF_TRANSFORM_IMPUTE_MEDIAN;
    } else goto invalid;
    normalize = required_item(numeric, "normalize");
    if (!exact_keys(normalize, normalize_keys, 2)) goto invalid;
    op = required_item(normalize, "op");
    if (json_string_equals(op, "none")) {
        if (!cJSON_IsNull(required_item(normalize, "ddof"))) goto invalid;
        out->normalize = TF_TRANSFORM_NORMALIZE_NONE;
    } else if (json_string_equals(op, "minmax")) {
        if (!cJSON_IsNull(required_item(normalize, "ddof"))) goto invalid;
        out->normalize = TF_TRANSFORM_NORMALIZE_MINMAX;
    } else if (json_string_equals(op, "standard")) {
        uint64_t ddof;
        if (!safe_uint64(required_item(normalize, "ddof"), &ddof) || ddof > 1)
            goto invalid;
        out->normalize = TF_TRANSFORM_NORMALIZE_STANDARD;
        out->ddof = (int)ddof;
    } else goto invalid;
    return TF_TRANSFORM_OK;
invalid:
    code = tf_transform_set_error(
        error, TF_TRANSFORM_INVALID_RECIPE,
        "invalid prepared-transform recipe column");
cleanup:
    free(out->source_id);
    tf_transform_resource_release(ledger, (uint64_t)source_len + 1);
    memset(out, 0, sizeof(*out));
    return code;
}

static tf_transform_code recipe_from_json_value(
    const cJSON *root, const tf_transform_runtime_copy *runtime,
    tf_transform_resource_ledger *ledger,
    tf_transform_recipe **out, tf_transform_error **error) {
    static const char *const top_keys[] = {
        "columns", "format", "outputDtype", "policyVersion",
        "semanticLimits", "version"
    };
    static const char *const semantic_keys[] = {
        "maxOutputColumns", "maxOutputElementsPerApply"
    };
    const tf_transform_limits_v1 *limits = &runtime->limits;
    const cJSON *columns;
    const cJSON *semantic;
    const cJSON *column_json;
    tf_transform_recipe *recipe = NULL;
    tf_transform_code code;
    uint64_t version;
    uint64_t policy_version;
    uint64_t max_columns;
    uint64_t max_elements;
    size_t count = 0;
    if (out) *out = NULL;
    if (!root || !runtime || !ledger || !out)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "recipe value argument is null");
    if (!exact_keys(root, top_keys, 6)
        || !json_string_equals(required_item(root, "format"),
                               "tranfi.transform-recipe")
        || !json_string_equals(required_item(root, "outputDtype"), "float64")
        || !safe_uint64(required_item(root, "version"), &version)
        || !safe_uint64(required_item(root, "policyVersion"), &policy_version))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_RECIPE, "invalid recipe header");
    if (version != 1 || policy_version != 1)
        return tf_transform_set_error(
            error, TF_TRANSFORM_UNSUPPORTED_VERSION,
            "recipe or policy version is unsupported");
    semantic = required_item(root, "semanticLimits");
    if (!exact_keys(semantic, semantic_keys, 2)
        || !safe_uint64(required_item(semantic, "maxOutputColumns"), &max_columns)
        || !safe_uint64(required_item(semantic, "maxOutputElementsPerApply"),
                        &max_elements)
        || max_columns == 0 || max_elements == 0)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_RECIPE,
            "invalid recipe semantic limits");
    if (max_columns > limits->max_output_columns
        || max_elements > limits->max_output_elements_per_call)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "recipe semantic limits exceed host limits");
    columns = required_item(root, "columns");
    if (!cJSON_IsArray(columns))
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_RECIPE, "invalid recipe columns");
    cJSON_ArrayForEach(column_json, columns) {
        if (count % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            code = tf_transform_poll_cancel(runtime, error);
            if (code != TF_TRANSFORM_OK) return code;
        }
        if (count == SIZE_MAX) return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "recipe column count overflows");
        ++count;
    }
    if (count == 0 || (uint64_t)count > limits->max_steps
        || (uint64_t)count > limits->max_input_columns)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_RECIPE, "invalid recipe columns");
    if ((uint64_t)count > max_columns)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_RECIPE,
            "recipe output columns exceed its semantic limit");
    if (count > SIZE_MAX / sizeof(tf_transform_recipe_column))
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "recipe column allocation overflows");
    recipe = (tf_transform_recipe *)tf_transform_resource_calloc(
        ledger, 1, sizeof(*recipe), error);
    if (!recipe) return ledger->last_code != TF_TRANSFORM_OK
        ? ledger->last_code : tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION, "recipe allocation failed");
    atomic_init(&recipe->refcount, 1u);
    recipe->column_count = count;
    recipe->max_output_columns = max_columns;
    recipe->max_output_elements_per_apply = max_elements;
    recipe->columns = (tf_transform_recipe_column *)
        tf_transform_resource_calloc(
            ledger, count, sizeof(*recipe->columns), error);
    if (!recipe->columns) {
        code = ledger->last_code != TF_TRANSFORM_OK
            ? ledger->last_code : tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION,
            "recipe column allocation failed");
        goto done;
    }
    column_json = columns->child;
    for (size_t i = 0; i < count; ++i, column_json = column_json->next) {
        code = tf_transform_poll_cancel(runtime, error);
        if (code != TF_TRANSFORM_OK) goto done;
        code = parse_recipe_column(
            column_json, runtime, ledger, &recipe->columns[i], error);
        if (code != TF_TRANSFORM_OK) goto done;
        for (size_t j = 0; j < i; ++j) {
            int equal = 0;
            code = runtime_span_equal(
                recipe->columns[i].source_id,
                recipe->columns[i].source_id_len,
                recipe->columns[j].source_id,
                recipe->columns[j].source_id_len,
                runtime, &equal, error);
            if (code != TF_TRANSFORM_OK) goto done;
            if (equal) {
                code = tf_transform_set_error(
                    error, TF_TRANSFORM_INVALID_RECIPE,
                    "recipe sourceIds must be unique");
                goto done;
            }
        }
    }
    *out = recipe;
    recipe = NULL;
    code = TF_TRANSFORM_OK;
done:
    tf_transform_recipe_release(recipe);
    return code;
}

tf_transform_code tf_transform_recipe_parse_numeric(
    const uint8_t *json, size_t json_len,
    const tf_transform_limits_v1 *limits,
    tf_transform_recipe **out, tf_transform_error **error) {
    cJSON *root = NULL;
    tf_transform_runtime_copy runtime;
    tf_transform_resource_ledger ledger;
    tf_transform_code code;
    uint64_t ast_resident = 0;
    if (out) *out = NULL;
    if (!json || json_len == 0 || !limits || !out)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "recipe argument is null or empty");
    if (json_len > limits->max_recipe_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "recipe exceeds byte limit");
    memset(&runtime, 0, sizeof(runtime));
    runtime.limits = *limits;
    tf_transform_resource_ledger_init(&ledger, &runtime);
    code = tf_transform_json_parse_bounded_runtime_ledger(
        json, json_len, &runtime, &ledger, &root,
        &ast_resident, NULL, error, TF_TRANSFORM_INVALID_RECIPE);
    if (code != TF_TRANSFORM_OK) return code;
    code = recipe_from_json_value(root, &runtime, &ledger, out, error);
    cJSON_Delete(root);
    tf_transform_resource_release(&ledger, ast_resident);
    return code;
}

tf_transform_code tf_transform_recipe_from_json(
    const uint8_t *json, size_t json_len,
    const tf_transform_limits_v1 *limits,
    tf_transform_recipe **out, tf_transform_error **error) {
    tf_transform_limits_v1 copied;
    tf_transform_code code;
    if (out) *out = NULL;
    tf_transform_clear_error(error);
    if (!out) return tf_transform_set_error(
        error, TF_TRANSFORM_INVALID_ARGUMENT, "recipe output is null");
    code = tf_transform_copy_limits(limits, &copied, error);
    if (code != TF_TRANSFORM_OK) return code;
    return tf_transform_recipe_parse_numeric(json, json_len, &copied, out, error);
}

static cJSON *json_null(void) { return cJSON_CreateNull(); }
static cJSON *json_string(const char *value) { return cJSON_CreateString(value); }
static cJSON *json_number(double value) { return cJSON_CreateNumber(value); }

static int add_item(cJSON *object, const char *key, cJSON *item) {
    if (!item) return 0;
    if (!cJSON_AddItemToObject(object, key, item)) {
        cJSON_Delete(item);
        return 0;
    }
    return 1;
}

static int transfer_item(cJSON *object, const char *key, cJSON **item) {
    cJSON *owned;
    if (!item) return 0;
    owned = *item;
    *item = NULL;
    return add_item(object, key, owned);
}

static int transfer_array_item(cJSON *array, cJSON **item) {
    cJSON *owned;
    if (!item || !*item) return 0;
    owned = *item;
    *item = NULL;
    if (!cJSON_AddItemToArray(array, owned)) {
        cJSON_Delete(owned);
        return 0;
    }
    return 1;
}

static cJSON *tagged_bits(uint64_t raw, uint32_t dtype) {
    static const char hex[] = "0123456789abcdef";
    cJSON *object = cJSON_CreateObject();
    char bits[17];
    size_t digits;
    if (!object) return NULL;
    if (dtype == TF_VIEW_FLOAT32) {
        raw &= UINT64_C(0xffffffff);
        digits = 8;
        if (!add_item(object, "t", json_string("f32"))) goto fail;
    } else if (dtype == TF_VIEW_FLOAT64) {
        digits = 16;
        if (!add_item(object, "t", json_string("f64"))) goto fail;
    } else goto fail;
    for (size_t i = 0; i < digits; ++i) {
        unsigned int shift = (unsigned int)((digits - 1 - i) * 4);
        bits[i] = hex[(raw >> shift) & 15u];
    }
    bits[digits] = '\0';
    if (!add_item(object, "v", json_string(bits))) goto fail;
    return object;
fail:
    cJSON_Delete(object);
    return NULL;
}

static cJSON *tagged_constant(double value, uint32_t dtype) {
    if (dtype == TF_VIEW_FLOAT32) {
        float narrowed = (float)value;
        uint32_t raw;
        memcpy(&raw, &narrowed, sizeof(raw));
        return tagged_bits(raw, dtype);
    }
    return tagged_bits(tf_transform_double_bits(value), dtype);
}

typedef struct tf_json_build_budget {
    uint64_t nodes;
    uint64_t strings;
    uint64_t string_bytes;
    uint64_t object_keys;
    uint64_t objects;
    uint64_t max_string_bytes;
    uint64_t max_object_width;
    uint64_t depth;
} tf_json_build_budget;

static int build_budget_add(uint64_t *target, uint64_t value) {
    if (*target > UINT64_MAX - value) return 0;
    *target += value;
    return 1;
}

static int build_budget_node(tf_json_build_budget *budget) {
    return build_budget_add(&budget->nodes, 1);
}

static int build_budget_string(
    tf_json_build_budget *budget, uint64_t bytes) {
    if (!build_budget_add(&budget->strings, 1)
        || !build_budget_add(&budget->string_bytes, bytes)) return 0;
    if (bytes > budget->max_string_bytes) budget->max_string_bytes = bytes;
    return 1;
}

static int build_budget_key(
    tf_json_build_budget *budget, const char *key) {
    size_t len = strlen(key);
    return build_budget_add(&budget->object_keys, 1)
        && build_budget_string(budget, (uint64_t)len);
}

static int build_budget_object(
    tf_json_build_budget *budget, uint64_t width) {
    if (!build_budget_node(budget)
        || !build_budget_add(&budget->objects, 1)) return 0;
    if (width > budget->max_object_width) budget->max_object_width = width;
    return 1;
}

static int build_budget_array(tf_json_build_budget *budget) {
    return build_budget_node(budget);
}

static int build_budget_primitive(
    tf_json_build_budget *budget, const char *key) {
    return build_budget_key(budget, key) && build_budget_node(budget);
}

static int build_budget_string_property(
    tf_json_build_budget *budget, const char *key, uint64_t value_bytes) {
    return build_budget_primitive(budget, key)
        && build_budget_string(budget, value_bytes);
}

static int build_budget_child(
    tf_json_build_budget *budget, const char *key) {
    return build_budget_key(budget, key);
}

static int build_budget_tagged(
    tf_json_build_budget *budget, const char *key, uint64_t value_bytes) {
    return build_budget_child(budget, key)
        && build_budget_object(budget, 2)
        && build_budget_string_property(budget, "t", 3)
        && build_budget_string_property(budget, "v", value_bytes);
}

static int build_budget_schema(
    const tf_transform_schema *schema, tf_json_build_budget *budget) {
    if (!schema || !build_budget_array(budget)) return 0;
    for (size_t i = 0; i < schema->field_count; ++i) {
        const tf_transform_schema_field_owned *field = &schema->fields[i];
        if (!build_budget_object(budget, schema->is_output ? 6 : 3)) return 0;
        if (schema->is_output) {
            if (field->category_kind == TF_TRANSFORM_SCHEMA_CATEGORY_NONE) {
                if (!build_budget_primitive(budget, "category")) return 0;
            } else if (field->category_kind == TF_TRANSFORM_SCHEMA_CATEGORY_OTHER) {
                if (!build_budget_child(budget, "category")
                    || !build_budget_object(budget, 1)
                    || !build_budget_string_property(budget, "t", 5)) return 0;
            } else {
                if (!build_budget_tagged(
                        budget, "category",
                        field->category_dtype == TF_VIEW_FLOAT32 ? 8 : 16)) return 0;
            }
        }
        if (!build_budget_string_property(budget, "dtype", 7)
            || !build_budget_string_property(
                budget, "id", (uint64_t)field->id_len)
            || !build_budget_string_property(
                budget, "name", (uint64_t)field->name_len)) return 0;
        if (schema->is_output
            && (!build_budget_string_property(
                    budget, "role",
                    field->role == TF_TRANSFORM_ROLE_ONEHOT ? 6 : 5)
                || !build_budget_string_property(
                    budget, "sourceId", (uint64_t)field->source_id_len))) return 0;
    }
    if (budget->depth < 3) budget->depth = 3;
    return 1;
}

static int build_budget_recipe(
    const tf_transform_recipe *recipe, tf_json_build_budget *budget) {
    if (!recipe || !build_budget_object(budget, 6)
        || !build_budget_child(budget, "columns")
        || !build_budget_array(budget)) return 0;
    for (size_t i = 0; i < recipe->column_count; ++i) {
        const tf_transform_recipe_column *column = &recipe->columns[i];
        if (!build_budget_object(budget, 4)
            || !build_budget_child(budget, "kind")
            || !build_budget_object(budget, 4)
            || !build_budget_primitive(budget, "maxCategories")
            || !build_budget_string_property(budget, "op", 8)
            || !build_budget_primitive(budget, "rule")
            || !build_budget_string_property(
                budget, "value",
                column->kind == TF_TRANSFORM_KIND_CATEGORICAL ? 11 : 7))
            return 0;
        if (column->kind == TF_TRANSFORM_KIND_CATEGORICAL) {
            if (!build_budget_child(budget, "categorical")
                || !build_budget_object(budget, 2)
                || !build_budget_child(budget, "encode")
                || !build_budget_object(budget, 4)
                || !build_budget_string_property(budget, "categories", 8)
                || !build_budget_string_property(
                    budget, "op",
                    column->categorical_encode == TF_TRANSFORM_ENCODE_LABEL ? 5 : 4)
                || !build_budget_primitive(budget, "sentinelLabel"))
                return 0;
            if (column->categorical_unknown == TF_TRANSFORM_UNKNOWN_NONE) {
                if (!build_budget_primitive(budget, "unknown")) return 0;
            } else if (!build_budget_string_property(
                    budget, "unknown",
                    column->categorical_unknown == TF_TRANSFORM_UNKNOWN_SENTINEL
                        ? 8 : 5)) return 0;
            if (!build_budget_child(budget, "impute")
                || !build_budget_object(budget, 3)
                || !build_budget_string_property(budget, "allMissing", 5)
                || !build_budget_primitive(budget, "constant")
                || !build_budget_string_property(budget, "op", 4)
                || !build_budget_primitive(budget, "numeric")
                || !build_budget_string_property(
                    budget, "sourceId", (uint64_t)column->source_id_len))
                return 0;
            continue;
        }
        if (!build_budget_primitive(budget, "categorical")
            || !build_budget_child(budget, "numeric")
            || !build_budget_object(budget, 2)
            || !build_budget_child(budget, "impute")
            || !build_budget_object(budget, 3)) return 0;
        if (column->impute == TF_TRANSFORM_IMPUTE_MEAN
            || column->impute == TF_TRANSFORM_IMPUTE_MEDIAN) {
            if (!build_budget_string_property(budget, "allMissing", 5)) return 0;
        } else if (!build_budget_primitive(budget, "allMissing")) return 0;
        if (column->impute == TF_TRANSFORM_IMPUTE_CONSTANT) {
            if (!build_budget_tagged(
                    budget, "constant",
                    column->constant_dtype == TF_VIEW_FLOAT32 ? 8 : 16)) return 0;
        } else if (!build_budget_primitive(budget, "constant")) return 0;
        if (!build_budget_string_property(budget, "op", 8)
            || !build_budget_child(budget, "normalize")
            || !build_budget_object(budget, 2)) return 0;
        if (column->normalize == TF_TRANSFORM_NORMALIZE_STANDARD) {
            if (!build_budget_primitive(budget, "ddof")) return 0;
        } else if (!build_budget_primitive(budget, "ddof")) return 0;
        if (!build_budget_string_property(budget, "op", 8)
            || !build_budget_string_property(
                budget, "sourceId", (uint64_t)column->source_id_len)) return 0;
    }
    if (!build_budget_string_property(budget, "format", 23)
        || !build_budget_string_property(budget, "outputDtype", 7)
        || !build_budget_primitive(budget, "policyVersion")
        || !build_budget_child(budget, "semanticLimits")
        || !build_budget_object(budget, 2)
        || !build_budget_primitive(budget, "maxOutputColumns")
        || !build_budget_primitive(budget, "maxOutputElementsPerApply")
        || !build_budget_primitive(budget, "version")) return 0;
    if (budget->depth < 8) budget->depth = 8;
    return 1;
}

static int build_budget_step(
    const tf_transform_recipe_column *recipe,
    const tf_transform_column_state *column_state,
    tf_json_build_budget *budget) {
    if (recipe->kind == TF_TRANSFORM_KIND_CATEGORICAL) {
        const tf_transform_categorical_state *state
            = &column_state->value.categorical;
        uint64_t digits = state->source_dtype == TF_VIEW_FLOAT32 ? 8 : 16;
        if (column_state->kind != TF_TRANSFORM_KIND_CATEGORICAL
            || !build_budget_object(budget, 6)
            || !build_budget_child(budget, "categorical")
            || !build_budget_object(budget, 3)
            || !build_budget_child(budget, "categories")
            || !build_budget_array(budget)) return 0;
        for (size_t i = 0; i < state->category_count; ++i)
            if (!build_budget_object(budget, 2)
                || !build_budget_string_property(budget, "t", 3)
                || !build_budget_string_property(budget, "v", digits)) return 0;
        if (!build_budget_child(budget, "encode")
            || !build_budget_object(budget, 4)
            || !build_budget_string_property(
                budget, "op",
                state->encode == TF_TRANSFORM_ENCODE_LABEL ? 5 : 4)
            || !build_budget_primitive(budget, "otherOrdinal")
            || !build_budget_primitive(budget, "sentinelLabel")) return 0;
        if (state->unknown == TF_TRANSFORM_UNKNOWN_NONE) {
            if (!build_budget_primitive(budget, "unknown")) return 0;
        } else if (!build_budget_string_property(
                budget, "unknown",
                state->unknown == TF_TRANSFORM_UNKNOWN_SENTINEL ? 8 : 5)) return 0;
        if (!build_budget_child(budget, "impute")
            || !build_budget_object(budget, 3)
            || !build_budget_string_property(budget, "allMissing", 5)
            || !build_budget_string_property(budget, "op", 4)
            || !build_budget_tagged(budget, "value", digits)
            || !build_budget_string_property(budget, "kind", 11)
            || !build_budget_primitive(budget, "numeric")
            || !build_budget_string_property(
                budget, "sourceId", (uint64_t)recipe->source_id_len)
            || !build_budget_primitive(budget, "stateVersion")) return 0;
        return 1;
    }
    {
        const tf_transform_numeric_state *state = &column_state->value.numeric;
        if (column_state->kind != TF_TRANSFORM_KIND_NUMERIC) return 0;
        if (!build_budget_object(budget, 6)
            || !build_budget_primitive(budget, "categorical")
            || !build_budget_string_property(budget, "kind", 7)
            || !build_budget_child(budget, "numeric")
            || !build_budget_object(budget, 2)
            || !build_budget_child(budget, "impute")
            || !build_budget_object(budget, 3)) return 0;
        if (recipe->impute == TF_TRANSFORM_IMPUTE_MEAN
            || recipe->impute == TF_TRANSFORM_IMPUTE_MEDIAN) {
            if (!build_budget_string_property(
                    budget, "allMissing", 5)) return 0;
        } else if (!build_budget_primitive(budget, "allMissing")) return 0;
        if (!build_budget_string_property(budget, "op", 8)) return 0;
        if (state->has_impute_value) {
            if (!build_budget_tagged(budget, "value", 16)) return 0;
        } else if (!build_budget_primitive(budget, "value")) return 0;
        if (!build_budget_child(budget, "normalize")
            || !build_budget_object(budget, 5)
            || !build_budget_primitive(budget, "ddof")
            || !build_budget_tagged(budget, "location", 16)
            || !build_budget_string_property(budget, "op", 8)
            || !build_budget_tagged(budget, "scale", 16)
            || !build_budget_string_property(
                budget, "sourceId", (uint64_t)recipe->source_id_len)
            || !build_budget_primitive(budget, "stateVersion")) return 0;
        return 1;
    }
}

static tf_transform_code check_build_budget(
    const tf_json_build_budget *budget,
    const tf_transform_limits_v1 *limits,
    uint64_t extra_allocations, uint64_t output_copies,
    tf_transform_error **error) {
    uint64_t allocations = budget->nodes;
    uint64_t resident;
    uint64_t output_upper;
    uint64_t sort_bytes;
    if (!build_budget_add(&allocations, budget->strings)
        || !build_budget_add(&allocations, budget->objects)
        || !build_budget_add(&allocations, extra_allocations)) goto overflow;
    if (budget->nodes > UINT64_MAX / sizeof(cJSON)) goto overflow;
    resident = budget->nodes * sizeof(cJSON);
    if (!build_budget_add(&resident, budget->string_bytes)
        || !build_budget_add(&resident, budget->strings)) goto overflow;
    if (budget->max_object_width > UINT64_MAX / sizeof(cJSON *)) goto overflow;
    sort_bytes = budget->max_object_width * sizeof(cJSON *);
    if (!build_budget_add(&resident, sort_bytes)) goto overflow;
    if (budget->nodes > UINT64_MAX / 32) goto overflow;
    output_upper = budget->nodes * 32;
    if (budget->string_bytes > (UINT64_MAX - output_upper) / 6)
        goto overflow;
    output_upper += budget->string_bytes * 6;
    if (output_upper > limits->max_plan_bytes)
        output_upper = limits->max_plan_bytes;
    if (output_copies != 0 && output_upper > UINT64_MAX / output_copies)
        goto overflow;
    if (!build_budget_add(&resident, output_upper * output_copies)) goto overflow;
    if (budget->depth > limits->max_json_depth
        || budget->object_keys > limits->max_object_keys
        || budget->string_bytes > limits->max_decoded_string_bytes
        || budget->max_string_bytes > limits->max_string_bytes
        || allocations > limits->max_allocations_per_session
        || resident > limits->max_resident_state_bytes
        || sizeof(cJSON) > limits->max_allocation_bytes
        || sort_bytes > limits->max_allocation_bytes
        || (budget->strings != 0
            && (budget->max_string_bytes == UINT64_MAX
                || budget->max_string_bytes + 1
                    > limits->max_allocation_bytes)))
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "JSON construction exceeds configured limits");
    return TF_TRANSFORM_OK;
overflow:
    return tf_transform_set_error(
        error, TF_TRANSFORM_RESOURCE_LIMIT,
        "JSON construction resource estimate overflows");
}

tf_transform_code tf_transform_schema_json_preflight(
    const tf_transform_schema *schema, const tf_transform_limits_v1 *limits,
    tf_transform_error **error) {
    tf_json_build_budget budget = {0};
    if (!schema || !limits)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "schema JSON preflight argument is null");
    if ((uint64_t)schema->field_count
            > (schema->is_output ? limits->max_output_columns
                                 : limits->max_input_columns)
        || !build_budget_schema(schema, &budget))
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "schema JSON construction estimate overflows");
    return check_build_budget(&budget, limits, 1, 1, error);
}

tf_transform_code tf_transform_plan_json_preflight(
    const tf_transform_plan *plan, const tf_transform_limits_v1 *limits,
    tf_transform_error **error) {
    tf_json_build_budget fingerprint = {0};
    tf_json_build_budget plan_json = {0};
    tf_transform_code code;
    uint64_t total_allocations;
    uint64_t total_categories = 0;
    if (!plan || !limits)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "plan JSON preflight argument is null");
    if ((uint64_t)plan->input_schema.field_count > limits->max_input_columns
        || (uint64_t)plan->output_schema.field_count > limits->max_output_columns
        || (uint64_t)plan->input_schema.field_count > limits->max_steps)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "plan JSON structure exceeds configured limits");
    for (size_t i = 0; i < plan->input_schema.field_count; ++i) {
        uint64_t count;
        if (plan->states[i].kind != TF_TRANSFORM_KIND_CATEGORICAL) continue;
        count = (uint64_t)plan->states[i].value.categorical.category_count;
        if (count > limits->max_categories_per_column
            || total_categories > UINT64_MAX - count
            || total_categories + count > limits->max_total_categories)
            return tf_transform_set_error(
                error, TF_TRANSFORM_RESOURCE_LIMIT,
                "plan categories exceed configured limits");
        total_categories += count;
    }
    if (!build_budget_object(&fingerprint, 3)
        || !build_budget_child(&fingerprint, "inputSchema")
        || !build_budget_schema(&plan->input_schema, &fingerprint)
        || !build_budget_primitive(&fingerprint, "policyVersion")
        || !build_budget_child(&fingerprint, "recipe")
        || !build_budget_recipe(plan->recipe, &fingerprint)) goto overflow;
    if (!build_budget_object(&plan_json, 7)
        || !build_budget_string_property(&plan_json, "format", 21)
        || !build_budget_child(&plan_json, "inputSchema")
        || !build_budget_schema(&plan->input_schema, &plan_json)
        || !build_budget_child(&plan_json, "outputSchema")
        || !build_budget_schema(&plan->output_schema, &plan_json)
        || !build_budget_child(&plan_json, "recipe")
        || !build_budget_recipe(plan->recipe, &plan_json)
        || !build_budget_string_property(&plan_json, "recipeSha256", 64)
        || !build_budget_child(&plan_json, "steps")
        || !build_budget_array(&plan_json)) goto overflow;
    for (size_t i = 0; i < plan->input_schema.field_count; ++i) {
        if (!build_budget_step(
                &plan->recipe->columns[i], &plan->states[i], &plan_json))
            goto overflow;
    }
    if (!build_budget_primitive(&plan_json, "version")) goto overflow;
    if (plan_json.depth < 8) plan_json.depth = 8;
    code = check_build_budget(&fingerprint, limits, 1, 1, error);
    if (code != TF_TRANSFORM_OK) return code;
    code = check_build_budget(&plan_json, limits, 2, 2, error);
    if (code != TF_TRANSFORM_OK) return code;
    total_allocations = fingerprint.nodes;
    if (!build_budget_add(&total_allocations, fingerprint.strings)
        || !build_budget_add(&total_allocations, fingerprint.objects)
        || !build_budget_add(&total_allocations, plan_json.nodes)
        || !build_budget_add(&total_allocations, plan_json.strings)
        || !build_budget_add(&total_allocations, plan_json.objects)
        || !build_budget_add(&total_allocations, 3)) goto overflow;
    if (total_allocations > limits->max_allocations_per_session)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "plan JSON allocation count exceeds configured limit");
    return TF_TRANSFORM_OK;
overflow:
    return tf_transform_set_error(
        error, TF_TRANSFORM_RESOURCE_LIMIT,
        "plan JSON construction estimate overflows");
}

cJSON *tf_transform_recipe_to_json(const tf_transform_recipe *recipe) {
    cJSON *root = cJSON_CreateObject();
    cJSON *columns = cJSON_CreateArray();
    cJSON *semantic = NULL;
    if (!recipe || !root || !columns) goto fail;
    if (!transfer_item(root, "columns", &columns)) goto fail;
    for (size_t i = 0; i < recipe->column_count; ++i) {
        const tf_transform_recipe_column *column = &recipe->columns[i];
        if (column->kind == TF_TRANSFORM_KIND_CATEGORICAL) {
            cJSON *entry = cJSON_CreateObject();
            cJSON *kind = cJSON_CreateObject();
            cJSON *categorical = cJSON_CreateObject();
            cJSON *impute = cJSON_CreateObject();
            cJSON *encode = cJSON_CreateObject();
            cJSON *sentinel = column->categorical_has_sentinel_label
                ? json_number((double)column->categorical_sentinel_label)
                : json_null();
            cJSON *unknown = column->categorical_unknown == TF_TRANSFORM_UNKNOWN_NONE
                ? json_null() : json_string(
                    column->categorical_unknown == TF_TRANSFORM_UNKNOWN_SENTINEL
                        ? "sentinel"
                        : column->categorical_unknown == TF_TRANSFORM_UNKNOWN_OTHER
                            ? "other" : "error");
            if (!entry || !kind || !categorical || !impute || !encode
                || !sentinel || !unknown
                || !add_item(kind, "maxCategories", json_null())
                || !add_item(kind, "op", json_string("declared"))
                || !add_item(kind, "rule", json_null())
                || !add_item(kind, "value", json_string("categorical"))
                || !transfer_item(entry, "kind", &kind)
                || !add_item(encode, "categories", json_string("discover"))
                || !add_item(encode, "op", json_string(
                    column->categorical_encode == TF_TRANSFORM_ENCODE_LABEL
                        ? "label" : "none"))
                || !transfer_item(encode, "sentinelLabel", &sentinel)
                || !transfer_item(encode, "unknown", &unknown)
                || !transfer_item(categorical, "encode", &encode)
                || !add_item(impute, "allMissing", json_string(
                    column->categorical_all_missing == TF_TRANSFORM_ALL_MISSING_ZERO
                        ? "zero" : "error"))
                || !add_item(impute, "constant", json_null())
                || !add_item(impute, "op", json_string("mode"))
                || !transfer_item(categorical, "impute", &impute)
                || !transfer_item(entry, "categorical", &categorical)
                || !add_item(entry, "numeric", json_null())
                || !add_item(entry, "sourceId", json_string(column->source_id))
                || !transfer_array_item(
                    cJSON_GetObjectItemCaseSensitive(root, "columns"), &entry)) {
                cJSON_Delete(entry);
                cJSON_Delete(kind);
                cJSON_Delete(categorical);
                cJSON_Delete(impute);
                cJSON_Delete(encode);
                cJSON_Delete(sentinel);
                cJSON_Delete(unknown);
                goto fail;
            }
            continue;
        }
        cJSON *entry = cJSON_CreateObject();
        cJSON *kind = cJSON_CreateObject();
        cJSON *numeric = cJSON_CreateObject();
        cJSON *impute = cJSON_CreateObject();
        cJSON *normalize = cJSON_CreateObject();
        const char *impute_name;
        const char *normalize_name;
        if (!entry || !kind || !numeric || !impute || !normalize) {
            cJSON_Delete(entry); cJSON_Delete(kind); cJSON_Delete(numeric);
            cJSON_Delete(impute); cJSON_Delete(normalize); goto fail;
        }
        if (!add_item(entry, "categorical", json_null())
            || !add_item(kind, "maxCategories", json_null())
            || !add_item(kind, "op", json_string("declared"))
            || !add_item(kind, "rule", json_null())
            || !add_item(kind, "value", json_string("numeric"))
            || !transfer_item(entry, "kind", &kind)) goto entry_fail;
        if (column->impute == TF_TRANSFORM_IMPUTE_MEAN
            || column->impute == TF_TRANSFORM_IMPUTE_MEDIAN) {
            if (!add_item(impute, "allMissing", json_string(
                    column->all_missing == TF_TRANSFORM_ALL_MISSING_ZERO
                        ? "zero" : "error"))) goto entry_fail;
        } else if (!add_item(impute, "allMissing", json_null())) goto entry_fail;
        if (column->impute == TF_TRANSFORM_IMPUTE_CONSTANT) {
            if (!add_item(impute, "constant", tagged_constant(
                    column->constant, column->constant_dtype))) goto entry_fail;
        } else if (!add_item(impute, "constant", json_null())) goto entry_fail;
        impute_name = column->impute == TF_TRANSFORM_IMPUTE_NONE ? "none"
            : column->impute == TF_TRANSFORM_IMPUTE_ZERO ? "zero"
            : column->impute == TF_TRANSFORM_IMPUTE_CONSTANT ? "constant"
            : column->impute == TF_TRANSFORM_IMPUTE_MEAN ? "mean" : "median";
        if (!add_item(impute, "op", json_string(impute_name))) goto entry_fail;
        if (!transfer_item(numeric, "impute", &impute)) goto entry_fail;
        if (column->normalize == TF_TRANSFORM_NORMALIZE_STANDARD) {
            if (!add_item(normalize, "ddof", json_number(column->ddof))) goto entry_fail;
        } else if (!add_item(normalize, "ddof", json_null())) goto entry_fail;
        normalize_name = column->normalize == TF_TRANSFORM_NORMALIZE_NONE ? "none"
            : column->normalize == TF_TRANSFORM_NORMALIZE_STANDARD
                ? "standard" : "minmax";
        if (!add_item(normalize, "op", json_string(normalize_name))) goto entry_fail;
        if (!transfer_item(numeric, "normalize", &normalize)) goto entry_fail;
        if (!transfer_item(entry, "numeric", &numeric)) goto entry_fail;
        if (!add_item(entry, "sourceId", json_string(column->source_id))) goto entry_fail;
        if (!transfer_array_item(
                cJSON_GetObjectItemCaseSensitive(root, "columns"), &entry))
            goto entry_fail;
        continue;
entry_fail:
        cJSON_Delete(entry); cJSON_Delete(kind); cJSON_Delete(numeric);
        cJSON_Delete(impute); cJSON_Delete(normalize); goto fail;
    }
    if (!add_item(root, "format", json_string("tranfi.transform-recipe"))
        || !add_item(root, "outputDtype", json_string("float64"))
        || !add_item(root, "policyVersion", json_number(1))) goto fail;
    semantic = cJSON_CreateObject();
    if (!semantic
        || !add_item(semantic, "maxOutputColumns",
                     json_number((double)recipe->max_output_columns))
        || !add_item(semantic, "maxOutputElementsPerApply",
                     json_number((double)recipe->max_output_elements_per_apply)))
        goto fail;
    if (!transfer_item(root, "semanticLimits", &semantic)) goto fail;
    if (!add_item(root, "version", json_number(1))) goto fail;
    return root;
fail:
    cJSON_Delete(columns);
    cJSON_Delete(semantic);
    cJSON_Delete(root);
    return NULL;
}

cJSON *tf_transform_schema_to_json(const tf_transform_schema *schema) {
    cJSON *array = cJSON_CreateArray();
    if (!schema || !array) return NULL;
    for (size_t i = 0; i < schema->field_count; ++i) {
        const tf_transform_schema_field_owned *field = &schema->fields[i];
        cJSON *entry = cJSON_CreateObject();
        if (!entry) goto fail;
        if (schema->is_output) {
            cJSON *category = NULL;
            if (field->category_kind == TF_TRANSFORM_SCHEMA_CATEGORY_NONE)
                category = json_null();
            else if (field->category_kind == TF_TRANSFORM_SCHEMA_CATEGORY_OTHER) {
                category = cJSON_CreateObject();
                if (category && !add_item(category, "t", json_string("other"))) {
                    cJSON_Delete(category);
                    category = NULL;
                }
            } else category = tagged_bits(
                field->category_bits, field->category_dtype);
            if (!category || !transfer_item(entry, "category", &category)) {
                cJSON_Delete(category);
                goto entry_fail;
            }
        }
        if (!add_item(entry, "dtype", json_string(
                field->dtype == TF_VIEW_FLOAT32 ? "float32" : "float64"))
            || !add_item(entry, "id", json_string(field->id))
            || !add_item(entry, "name", json_string(field->name))) goto entry_fail;
        if (schema->is_output
            && (!add_item(entry, "role", json_string(
                    field->role == TF_TRANSFORM_ROLE_LABEL ? "label"
                    : field->role == TF_TRANSFORM_ROLE_ONEHOT ? "onehot" : "value"))
                || !add_item(entry, "sourceId", json_string(field->source_id))))
            goto entry_fail;
        if (!transfer_array_item(array, &entry)) goto entry_fail;
        continue;
entry_fail:
        cJSON_Delete(entry);
        goto fail;
    }
    return array;
fail:
    cJSON_Delete(array);
    return NULL;
}

static void digest_hex(const uint8_t digest[32], char out[65]) {
    static const char digits[] = "0123456789abcdef";
    for (size_t i = 0; i < 32; ++i) {
        out[i * 2] = digits[digest[i] >> 4];
        out[i * 2 + 1] = digits[digest[i] & 15u];
    }
    out[64] = '\0';
}

static tf_transform_code plan_recipe_fingerprint(
    const tf_transform_recipe *recipe, const tf_transform_schema *input_schema,
    const tf_transform_limits_v1 *limits, char out[65],
    tf_transform_error **error) {
    cJSON *object = cJSON_CreateObject();
    cJSON *schema = NULL;
    cJSON *recipe_json = NULL;
    uint8_t *canonical = NULL;
    size_t canonical_len = 0;
    uint8_t digest[32];
    tf_transform_code code;
    if (!object) return tf_transform_set_error(
        error, TF_TRANSFORM_ALLOCATION, "recipe fingerprint allocation failed");
    schema = tf_transform_schema_to_json(input_schema);
    recipe_json = tf_transform_recipe_to_json(recipe);
    if (!schema || !recipe_json
        || !transfer_item(object, "inputSchema", &schema)) goto allocation_failed;
    if (!add_item(object, "policyVersion", json_number(1))
        || !transfer_item(object, "recipe", &recipe_json)) goto allocation_failed;
    code = tf_transform_json_print_canonical(
        object, limits, &canonical, &canonical_len, error);
    if (code != TF_TRANSFORM_OK) goto done;
    tf_transform_sha256(canonical, canonical_len, digest);
    digest_hex(digest, out);
done:
    tf_transform_bytes_free(&canonical, &canonical_len);
    cJSON_Delete(schema);
    cJSON_Delete(recipe_json);
    cJSON_Delete(object);
    return code;
allocation_failed:
    code = tf_transform_set_error(
        error, TF_TRANSFORM_ALLOCATION, "recipe fingerprint JSON failed");
    goto done;
}

static tf_transform_code imported_recipe_fingerprint(
    const cJSON *plan_root, const tf_transform_runtime_copy *runtime,
    tf_transform_resource_ledger *ledger, char out[65],
    tf_transform_error **error) {
    const cJSON *input_schema = required_item(plan_root, "inputSchema");
    const cJSON *recipe = required_item(plan_root, "recipe");
    cJSON object;
    cJSON input_copy;
    cJSON policy;
    cJSON recipe_copy;
    uint8_t *canonical = NULL;
    size_t canonical_len = 0;
    uint8_t digest[32];
    tf_transform_code code;
    if (!input_schema || !recipe)
        return tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN,
            "recipe fingerprint inputs are missing");
    memset(&object, 0, sizeof(object));
    input_copy = *input_schema;
    memset(&policy, 0, sizeof(policy));
    recipe_copy = *recipe;
    object.type = cJSON_Object;
    object.child = &input_copy;
    input_copy.string = (char *)"inputSchema";
    input_copy.prev = NULL;
    input_copy.next = &policy;
    policy.type = cJSON_Number;
    policy.valueint = 1;
    policy.valuedouble = 1.0;
    policy.string = (char *)"policyVersion";
    policy.prev = &input_copy;
    policy.next = &recipe_copy;
    recipe_copy.string = (char *)"recipe";
    recipe_copy.prev = &policy;
    recipe_copy.next = NULL;
    code = tf_transform_json_print_canonical_runtime_ledger(
        &object, runtime, ledger, &canonical, &canonical_len, error);
    if (code != TF_TRANSFORM_OK) return code;
    code = tf_transform_sha256_runtime(
        canonical, canonical_len, runtime, digest, error);
    if (code == TF_TRANSFORM_OK) digest_hex(digest, out);
    free(canonical);
    tf_transform_resource_release(ledger, (uint64_t)canonical_len);
    return code;
}

static const char *impute_op_name(tf_transform_impute_op op) {
    if (op == TF_TRANSFORM_IMPUTE_NONE) return "none";
    if (op == TF_TRANSFORM_IMPUTE_ZERO) return "zero";
    if (op == TF_TRANSFORM_IMPUTE_CONSTANT) return "constant";
    if (op == TF_TRANSFORM_IMPUTE_MEAN) return "mean";
    return "median";
}

static const char *normalize_op_name(tf_transform_normalize_op op) {
    if (op == TF_TRANSFORM_NORMALIZE_NONE) return "none";
    if (op == TF_TRANSFORM_NORMALIZE_STANDARD) return "standard";
    return "minmax";
}

static cJSON *numeric_step_to_json(
    const tf_transform_recipe_column *recipe,
    const tf_transform_numeric_state *state) {
    cJSON *step = cJSON_CreateObject();
    cJSON *numeric = cJSON_CreateObject();
    cJSON *impute = cJSON_CreateObject();
    cJSON *normalize = cJSON_CreateObject();
    if (!step || !numeric || !impute || !normalize) goto fail;
    if (!add_item(step, "categorical", json_null())
        || !add_item(step, "kind", json_string("numeric"))) goto fail;
    if (recipe->impute == TF_TRANSFORM_IMPUTE_MEAN
        || recipe->impute == TF_TRANSFORM_IMPUTE_MEDIAN) {
        if (!add_item(impute, "allMissing", json_string(
                recipe->all_missing == TF_TRANSFORM_ALL_MISSING_ZERO
                    ? "zero" : "error"))) goto fail;
    } else if (!add_item(impute, "allMissing", json_null())) goto fail;
    if (!add_item(impute, "op", json_string(impute_op_name(recipe->impute))))
        goto fail;
    if (state->has_impute_value) {
        if (!add_item(impute, "value", tagged_constant(
                state->impute_value, TF_VIEW_FLOAT64))) goto fail;
    } else if (!add_item(impute, "value", json_null())) goto fail;
    if (!transfer_item(numeric, "impute", &impute)) goto fail;
    if (recipe->normalize == TF_TRANSFORM_NORMALIZE_STANDARD) {
        if (!add_item(normalize, "ddof", json_number(recipe->ddof))) goto fail;
    } else if (!add_item(normalize, "ddof", json_null())) goto fail;
    if (!add_item(normalize, "location", tagged_constant(
            state->location, TF_VIEW_FLOAT64))
        || !add_item(normalize, "op", json_string(
            normalize_op_name(recipe->normalize)))
        || !add_item(normalize, "scale", tagged_constant(
            state->scale, TF_VIEW_FLOAT64))) goto fail;
    if (!transfer_item(numeric, "normalize", &normalize)) goto fail;
    if (!transfer_item(step, "numeric", &numeric)) goto fail;
    if (!add_item(step, "sourceId", json_string(recipe->source_id))
        || !add_item(step, "stateVersion", json_number(1))) goto fail;
    return step;
fail:
    cJSON_Delete(step);
    cJSON_Delete(numeric);
    cJSON_Delete(impute);
    cJSON_Delete(normalize);
    return NULL;
}

static cJSON *categorical_step_to_json(
    const tf_transform_recipe_column *recipe,
    const tf_transform_categorical_state *state) {
    cJSON *step = cJSON_CreateObject();
    cJSON *categorical = cJSON_CreateObject();
    cJSON *categories = cJSON_CreateArray();
    cJSON *encode = cJSON_CreateObject();
    cJSON *impute = cJSON_CreateObject();
    cJSON *other_ordinal = state->has_other_ordinal
        ? json_number((double)state->other_ordinal) : json_null();
    cJSON *sentinel = state->has_sentinel_label
        ? json_number((double)state->sentinel_label) : json_null();
    cJSON *unknown = state->unknown == TF_TRANSFORM_UNKNOWN_NONE
        ? json_null() : json_string(
            state->unknown == TF_TRANSFORM_UNKNOWN_SENTINEL ? "sentinel"
            : state->unknown == TF_TRANSFORM_UNKNOWN_OTHER ? "other" : "error");
    if (!step || !categorical || !categories || !encode || !impute
        || !other_ordinal || !sentinel || !unknown) goto fail;
    for (size_t i = 0; i < state->category_count; ++i) {
        cJSON *tagged = tagged_bits(
            state->categories[i].bits, state->source_dtype);
        if (!tagged || !transfer_array_item(categories, &tagged)) {
            cJSON_Delete(tagged);
            goto fail;
        }
    }
    if (!transfer_item(categorical, "categories", &categories)
        || !add_item(encode, "op", json_string(
            state->encode == TF_TRANSFORM_ENCODE_LABEL ? "label" : "none"))
        || !transfer_item(encode, "otherOrdinal", &other_ordinal)
        || !transfer_item(encode, "sentinelLabel", &sentinel)
        || !transfer_item(encode, "unknown", &unknown)
        || !transfer_item(categorical, "encode", &encode)
        || !add_item(impute, "allMissing", json_string(
            recipe->categorical_all_missing == TF_TRANSFORM_ALL_MISSING_ZERO
                ? "zero" : "error"))
        || !add_item(impute, "op", json_string("mode"))
        || !add_item(impute, "value", tagged_bits(
            state->impute_bits, state->source_dtype))
        || !transfer_item(categorical, "impute", &impute)
        || !transfer_item(step, "categorical", &categorical)
        || !add_item(step, "kind", json_string("categorical"))
        || !add_item(step, "numeric", json_null())
        || !add_item(step, "sourceId", json_string(recipe->source_id))
        || !add_item(step, "stateVersion", json_number(1))) goto fail;
    return step;
fail:
    cJSON_Delete(step);
    cJSON_Delete(categorical);
    cJSON_Delete(categories);
    cJSON_Delete(encode);
    cJSON_Delete(impute);
    cJSON_Delete(other_ordinal);
    cJSON_Delete(sentinel);
    cJSON_Delete(unknown);
    return NULL;
}

cJSON *tf_transform_plan_to_json(
    const tf_transform_plan *plan, const tf_transform_limits_v1 *limits,
    tf_transform_error **error) {
    cJSON *root = NULL;
    cJSON *input_schema = NULL;
    cJSON *output_schema = NULL;
    cJSON *recipe = NULL;
    cJSON *steps = NULL;
    char fingerprint[65];
    tf_transform_code code;
    if (!plan || !limits) {
        tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT,
            "plan JSON argument is null");
        goto failed;
    }
    code = tf_transform_plan_json_preflight(plan, limits, error);
    if (code != TF_TRANSFORM_OK) goto failed;
    root = cJSON_CreateObject();
    steps = cJSON_CreateArray();
    if (!root || !steps) goto allocation_failed;
    code = plan_recipe_fingerprint(
        plan->recipe, &plan->input_schema, limits, fingerprint, error);
    if (code != TF_TRANSFORM_OK) goto failed;
    input_schema = tf_transform_schema_to_json(&plan->input_schema);
    output_schema = tf_transform_schema_to_json(&plan->output_schema);
    recipe = tf_transform_recipe_to_json(plan->recipe);
    if (!input_schema || !output_schema || !recipe
        || !add_item(root, "format", json_string("tranfi.transform-plan"))
        || !transfer_item(root, "inputSchema", &input_schema)) goto allocation_failed;
    if (!transfer_item(root, "outputSchema", &output_schema)) goto allocation_failed;
    if (!transfer_item(root, "recipe", &recipe)) goto allocation_failed;
    if (!add_item(root, "recipeSha256", json_string(fingerprint)))
        goto allocation_failed;
    for (size_t i = 0; i < plan->input_schema.field_count; ++i) {
        cJSON *step = plan->states[i].kind == TF_TRANSFORM_KIND_CATEGORICAL
            ? categorical_step_to_json(
                &plan->recipe->columns[i],
                &plan->states[i].value.categorical)
            : numeric_step_to_json(
                &plan->recipe->columns[i],
                &plan->states[i].value.numeric);
        if (!step || !transfer_array_item(steps, &step)) {
            cJSON_Delete(step);
            goto allocation_failed;
        }
    }
    if (!transfer_item(root, "steps", &steps)) goto allocation_failed;
    if (!add_item(root, "version", json_number(1))) goto allocation_failed;
    return root;
allocation_failed:
    tf_transform_set_error(
        error, TF_TRANSFORM_ALLOCATION, "plan JSON allocation failed");
failed:
    cJSON_Delete(input_schema);
    cJSON_Delete(output_schema);
    cJSON_Delete(recipe);
    cJSON_Delete(steps);
    cJSON_Delete(root);
    return NULL;
}

static tf_transform_code parse_schema_json(
    const cJSON *array, int is_output,
    const tf_transform_runtime_copy *runtime,
    tf_transform_resource_ledger *ledger,
    tf_transform_schema *out, tf_transform_error **error) {
    static const char *const input_keys[] = {"dtype", "id", "name"};
    static const char *const output_keys[] = {
        "category", "dtype", "id", "name", "role", "sourceId"
    };
    tf_field_view_v1 *fields = NULL;
    tf_schema_view_v1 view;
    tf_transform_code code;
    size_t count = 0;
    const cJSON *entry;
    if (!cJSON_IsArray(array))
        return tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN, "plan schema is not a nonempty array");
    if (!runtime) return tf_transform_set_error(
        error, TF_TRANSFORM_INVALID_ARGUMENT,
        "plan schema runtime is null");
    cJSON_ArrayForEach(entry, array) {
        if (count % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            code = tf_transform_poll_cancel(runtime, error);
            if (code != TF_TRANSFORM_OK) return code;
        }
        if (count == SIZE_MAX) return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "plan schema column count overflows");
        ++count;
    }
    if (count == 0) return tf_transform_set_error(
        error, TF_TRANSFORM_CORRUPT_PLAN,
        "plan schema is not a nonempty array");
    if ((uint64_t)count > (is_output ? runtime->limits.max_output_columns
                                    : runtime->limits.max_input_columns)
        || (size_t)count > SIZE_MAX / sizeof(*fields)
        || (uint64_t)count * sizeof(*fields)
            > runtime->limits.max_allocation_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "plan schema exceeds limits");
    code = tf_transform_poll_cancel(runtime, error);
    if (code != TF_TRANSFORM_OK) return code;
    fields = (tf_field_view_v1 *)tf_transform_resource_calloc(
        ledger, count, sizeof(*fields), error);
    if (!fields) return ledger->last_code != TF_TRANSFORM_OK
        ? ledger->last_code : tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION,
            "plan schema view allocation failed");
    entry = array->child;
    for (size_t i = 0; i < count; ++i, entry = entry->next) {
        const cJSON *dtype;
        const cJSON *id;
        const cJSON *name;
        size_t id_len;
        size_t name_len;
        if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            code = tf_transform_poll_cancel(runtime, error);
            if (code != TF_TRANSFORM_OK) goto done;
        }
        if (!exact_keys(entry, is_output ? output_keys : input_keys,
                        is_output ? 6 : 3)) {
            code = tf_transform_set_error(
                error, TF_TRANSFORM_CORRUPT_PLAN, "plan schema field shape is invalid");
            goto done;
        }
        dtype = required_item(entry, "dtype");
        id = required_item(entry, "id");
        name = required_item(entry, "name");
        if ((!json_string_equals(dtype, "float32")
             && !json_string_equals(dtype, "float64"))
            || !cJSON_IsString(id) || !id->valuestring
            || !cJSON_IsString(name) || !name->valuestring) {
            code = tf_transform_set_error(
                error, TF_TRANSFORM_CORRUPT_PLAN, "plan schema field is invalid");
            goto done;
        }
        if (is_output) {
            const cJSON *source_id = required_item(entry, "sourceId");
            size_t source_id_len = 0;
            if (cJSON_IsString(source_id) && source_id->valuestring) {
                code = runtime_cstring_length(
                    source_id->valuestring,
                    (size_t)runtime->limits.max_string_bytes,
                    runtime, &source_id_len, error);
                if (code != TF_TRANSFORM_OK) goto done;
            }
            code = runtime_cstring_length(
                id->valuestring, (size_t)runtime->limits.max_string_bytes,
                runtime, &id_len, error);
            if (code != TF_TRANSFORM_OK) goto done;
            if (!cJSON_IsNull(required_item(entry, "category"))
                || (!json_string_equals(required_item(entry, "role"), "value")
                    && !json_string_equals(required_item(entry, "role"), "label"))
                || !cJSON_IsString(source_id) || !source_id->valuestring
                || !json_string_equals(dtype, "float64")) {
                code = tf_transform_set_error(
                    error, TF_TRANSFORM_CORRUPT_PLAN,
                    "output schema metadata is invalid");
                goto done;
            }
        }
        fields[i].abi_version = 1;
        fields[i].struct_size = (uint32_t)sizeof(fields[i]);
        fields[i].dtype = json_string_equals(dtype, "float32")
            ? TF_VIEW_FLOAT32 : TF_VIEW_FLOAT64;
        if (!is_output) {
            code = runtime_cstring_length(
                id->valuestring, (size_t)runtime->limits.max_string_bytes,
                runtime, &id_len, error);
            if (code != TF_TRANSFORM_OK) goto done;
        }
        code = runtime_cstring_length(
            name->valuestring, (size_t)runtime->limits.max_string_bytes,
            runtime, &name_len, error);
        if (code != TF_TRANSFORM_OK) goto done;
        fields[i].id_utf8 = (const uint8_t *)id->valuestring;
        fields[i].id_bytes = id_len;
        fields[i].name_utf8 = (const uint8_t *)name->valuestring;
        fields[i].name_bytes = name_len;
    }
    memset(&view, 0, sizeof(view));
    view.abi_version = 1;
    view.struct_size = (uint32_t)sizeof(view);
    view.column_count = count;
    view.fields = fields;
    view.fields_bytes = count * sizeof(*fields);
    code = tf_transform_schema_copy_runtime_ledger(
        &view, runtime, ledger, out, error);
    if (code == TF_TRANSFORM_INVALID_ARGUMENT)
        code = tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN, "plan schema validation failed");
    if (code == TF_TRANSFORM_OK) out->is_output = is_output;
    if (code == TF_TRANSFORM_OK && is_output) {
        entry = array->child;
        for (size_t i = 0; i < count; ++i, entry = entry->next) {
            const cJSON *source_id = required_item(entry, "sourceId");
            size_t source_id_len = 0;
            if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
                code = tf_transform_poll_cancel(runtime, error);
                if (code != TF_TRANSFORM_OK) break;
            }
            code = runtime_cstring_length(
                source_id->valuestring,
                (size_t)runtime->limits.max_string_bytes,
                runtime, &source_id_len, error);
            if (code != TF_TRANSFORM_OK) break;
            out->fields[i].source_id = (char *)tf_transform_resource_malloc(
                ledger, source_id_len + 1, error);
            if (!out->fields[i].source_id) {
                code = ledger->last_code != TF_TRANSFORM_OK
                    ? ledger->last_code : tf_transform_set_error(
                        error, TF_TRANSFORM_ALLOCATION,
                        "output schema source ID allocation failed");
                break;
            }
            code = tf_transform_copy_bytes_runtime(
                out->fields[i].source_id, source_id->valuestring,
                source_id_len + 1, runtime, error);
            if (code != TF_TRANSFORM_OK) break;
            out->fields[i].source_id_len = source_id_len;
            out->fields[i].role = json_string_equals(
                required_item(entry, "role"), "label")
                ? TF_TRANSFORM_ROLE_LABEL : TF_TRANSFORM_ROLE_VALUE;
            out->fields[i].category_kind = TF_TRANSFORM_SCHEMA_CATEGORY_NONE;
        }
        if (code != TF_TRANSFORM_OK) tf_transform_schema_clear(out);
    }
done:
    free(fields);
    tf_transform_resource_release(
        ledger, (uint64_t)count * sizeof(*fields));
    return code;
}

static tf_transform_code lowercase_sha256_string_runtime(
    const cJSON *value, const tf_transform_runtime_copy *runtime,
    int *valid, tf_transform_error **error) {
    size_t length = 0;
    tf_transform_code code;
    if (!valid) return tf_transform_set_error(
        error, TF_TRANSFORM_INVALID_ARGUMENT,
        "fingerprint validation output is null");
    *valid = 0;
    if (!cJSON_IsString(value) || !value->valuestring) return TF_TRANSFORM_OK;
    code = runtime_cstring_length(
        value->valuestring, (size_t)runtime->limits.max_string_bytes,
        runtime, &length, error);
    if (code != TF_TRANSFORM_OK) return code;
    if (length != 64) return TF_TRANSFORM_OK;
    for (size_t i = 0; i < 64; ++i) {
        unsigned char c = (unsigned char)value->valuestring[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')))
            return TF_TRANSFORM_OK;
    }
    *valid = 1;
    return TF_TRANSFORM_OK;
}

static int tagged_f64(const cJSON *value, double *out) {
    uint32_t dtype = 0;
    return parse_tagged_constant(value, out, &dtype) && dtype == TF_VIEW_FLOAT64;
}

static int tagged_category_bits(
    const cJSON *value, uint32_t expected_dtype, uint64_t *out) {
    uint64_t decoded = 0;
    uint32_t dtype = 0;
    if (!out || !parse_tagged_bits(value, &decoded, &dtype)
        || dtype != expected_dtype)
        return 0;
    if (dtype == TF_VIEW_FLOAT32) {
        uint32_t bits = (uint32_t)decoded;
        if ((bits & UINT32_C(0x7f800000)) == UINT32_C(0x7f800000)
            || bits == UINT32_C(0x80000000)) return 0;
        *out = bits;
        return 1;
    }
    if ((decoded & UINT64_C(0x7ff0000000000000))
            == UINT64_C(0x7ff0000000000000)
        || decoded == UINT64_C(0x8000000000000000)) return 0;
    *out = decoded;
    return 1;
}

static int null_or_policy(
    const cJSON *value, tf_transform_all_missing policy,
    tf_transform_impute_op op) {
    if (op != TF_TRANSFORM_IMPUTE_MEAN
        && op != TF_TRANSFORM_IMPUTE_MEDIAN) return cJSON_IsNull(value);
    return json_string_equals(value,
        policy == TF_TRANSFORM_ALL_MISSING_ZERO ? "zero" : "error");
}

static tf_transform_code parse_numeric_step(
    const cJSON *value, const tf_transform_recipe_column *recipe,
    const tf_transform_runtime_copy *runtime,
    tf_transform_numeric_state *out, tf_transform_error **error) {
    static const char *const step_keys[] = {
        "categorical", "kind", "numeric", "sourceId", "stateVersion"
    };
    static const char *const numeric_keys[] = {"impute", "normalize"};
    static const char *const impute_keys[] = {"allMissing", "op", "value"};
    static const char *const normalize_keys[] = {
        "ddof", "location", "op", "scale"
    };
    const cJSON *numeric;
    const cJSON *impute;
    const cJSON *normalize;
    const cJSON *impute_value;
    const cJSON *ddof;
    uint64_t state_version;
    uint64_t parsed_ddof;
    double location;
    double scale;
    double resolved = 0.0;
    const cJSON *source_id = required_item(value, "sourceId");
    size_t source_id_len = 0;
    int source_equal = 0;
    tf_transform_code code;
    memset(out, 0, sizeof(*out));
    if (cJSON_IsString(source_id) && source_id->valuestring) {
        code = runtime_cstring_length(
            source_id->valuestring,
            (size_t)runtime->limits.max_string_bytes,
            runtime, &source_id_len, error);
        if (code != TF_TRANSFORM_OK) return code;
        code = runtime_span_equal(
            source_id->valuestring, source_id_len,
            recipe->source_id, recipe->source_id_len,
            runtime, &source_equal, error);
        if (code != TF_TRANSFORM_OK) return code;
    }
    if (!exact_keys(value, step_keys, 5)
        || !cJSON_IsNull(required_item(value, "categorical"))
        || !json_string_equals(required_item(value, "kind"), "numeric")
        || !source_equal
        || !safe_uint64(required_item(value, "stateVersion"), &state_version))
        return tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN, "numeric plan step shape is invalid");
    if (state_version != 1)
        return tf_transform_set_error(
            error, TF_TRANSFORM_UNSUPPORTED_VERSION,
            "numeric plan state version is unsupported");
    numeric = required_item(value, "numeric");
    if (!exact_keys(numeric, numeric_keys, 2)) goto corrupt;
    impute = required_item(numeric, "impute");
    normalize = required_item(numeric, "normalize");
    if (!exact_keys(impute, impute_keys, 3)
        || !json_string_equals(required_item(impute, "op"),
                              impute_op_name(recipe->impute))
        || !null_or_policy(required_item(impute, "allMissing"),
                           recipe->all_missing, recipe->impute)) goto corrupt;
    impute_value = required_item(impute, "value");
    if (recipe->impute == TF_TRANSFORM_IMPUTE_NONE) {
        if (!cJSON_IsNull(impute_value)) goto corrupt;
    } else {
        if (!tagged_f64(impute_value, &resolved)) goto corrupt;
        if (recipe->impute == TF_TRANSFORM_IMPUTE_ZERO
            && tf_transform_double_bits(resolved) != 0) goto corrupt;
        if (recipe->impute == TF_TRANSFORM_IMPUTE_MEDIAN
            && resolved == 0.0
            && tf_transform_double_bits(resolved) != 0) goto corrupt;
        if (recipe->impute == TF_TRANSFORM_IMPUTE_CONSTANT
            && tf_transform_double_bits(resolved)
                != tf_transform_double_bits(recipe->constant)) goto corrupt;
        out->has_impute_value = 1;
        out->impute_value = resolved;
    }
    if (!exact_keys(normalize, normalize_keys, 4)
        || !json_string_equals(required_item(normalize, "op"),
                              normalize_op_name(recipe->normalize))
        || !tagged_f64(required_item(normalize, "location"), &location)
        || !tagged_f64(required_item(normalize, "scale"), &scale)
        || scale <= 0.0) goto corrupt;
    ddof = required_item(normalize, "ddof");
    if (recipe->normalize == TF_TRANSFORM_NORMALIZE_STANDARD) {
        if (!safe_uint64(ddof, &parsed_ddof)
            || parsed_ddof != (uint64_t)recipe->ddof) goto corrupt;
    } else if (!cJSON_IsNull(ddof)) goto corrupt;
    if (recipe->normalize == TF_TRANSFORM_NORMALIZE_NONE
        && (tf_transform_double_bits(location) != 0
            || tf_transform_double_bits(scale) != UINT64_C(0x3ff0000000000000)))
        goto corrupt;
    out->impute = recipe->impute;
    out->all_missing = recipe->all_missing;
    out->normalize = recipe->normalize;
    out->ddof = recipe->ddof;
    out->location = location;
    out->scale = scale;
    return TF_TRANSFORM_OK;
corrupt:
    return tf_transform_set_error(
        error, TF_TRANSFORM_CORRUPT_PLAN, "numeric plan state is inconsistent");
}

static tf_transform_code parse_categorical_step(
    const cJSON *value, const tf_transform_recipe_column *recipe,
    uint32_t source_dtype, const tf_transform_runtime_copy *runtime,
    tf_transform_resource_ledger *ledger, uint64_t *total_categories,
    tf_transform_categorical_state *out, tf_transform_error **error) {
    static const char *const step_keys[] = {
        "categorical", "kind", "numeric", "sourceId", "stateVersion"
    };
    static const char *const categorical_keys[] = {
        "categories", "encode", "impute"
    };
    static const char *const encode_keys[] = {
        "op", "otherOrdinal", "sentinelLabel", "unknown"
    };
    static const char *const impute_keys[] = {"allMissing", "op", "value"};
    const cJSON *categorical;
    const cJSON *categories;
    const cJSON *encode;
    const cJSON *impute;
    const cJSON *entry;
    const cJSON *source_id = required_item(value, "sourceId");
    size_t source_id_len = 0;
    size_t count = 0;
    size_t bytes = 0;
    uint64_t state_version;
    uint64_t mode_bits;
    uint64_t other_ordinal = 0;
    int64_t sentinel_label = 0;
    int source_equal = 0;
    int mode_known = 0;
    tf_transform_code code;
    memset(out, 0, sizeof(*out));
    if (cJSON_IsString(source_id) && source_id->valuestring) {
        code = runtime_cstring_length(
            source_id->valuestring, (size_t)runtime->limits.max_string_bytes,
            runtime, &source_id_len, error);
        if (code != TF_TRANSFORM_OK) return code;
        code = runtime_span_equal(
            source_id->valuestring, source_id_len,
            recipe->source_id, recipe->source_id_len,
            runtime, &source_equal, error);
        if (code != TF_TRANSFORM_OK) return code;
    }
    if (!exact_keys(value, step_keys, 5)
        || !json_string_equals(required_item(value, "kind"), "categorical")
        || !cJSON_IsNull(required_item(value, "numeric"))
        || !source_equal
        || !safe_uint64(required_item(value, "stateVersion"), &state_version))
        goto corrupt;
    if (state_version != 1)
        return tf_transform_set_error(
            error, TF_TRANSFORM_UNSUPPORTED_VERSION,
            "categorical plan state version is unsupported");
    categorical = required_item(value, "categorical");
    if (!exact_keys(categorical, categorical_keys, 3)) goto corrupt;
    categories = required_item(categorical, "categories");
    encode = required_item(categorical, "encode");
    impute = required_item(categorical, "impute");
    if (!cJSON_IsArray(categories)
        || !exact_keys(encode, encode_keys, 4)
        || !exact_keys(impute, impute_keys, 3)
        || !json_string_equals(required_item(impute, "op"), "mode")
        || !json_string_equals(
            required_item(impute, "allMissing"),
            recipe->categorical_all_missing == TF_TRANSFORM_ALL_MISSING_ZERO
                ? "zero" : "error"))
        goto corrupt;
    if (recipe->categorical_encode == TF_TRANSFORM_ENCODE_NONE) {
        if (!json_string_equals(required_item(encode, "op"), "none")
            || !cJSON_IsNull(required_item(encode, "otherOrdinal"))
            || !cJSON_IsNull(required_item(encode, "sentinelLabel"))
            || !cJSON_IsNull(required_item(encode, "unknown"))) goto corrupt;
    } else if (recipe->categorical_encode == TF_TRANSFORM_ENCODE_LABEL) {
        if (!json_string_equals(required_item(encode, "op"), "label")) goto corrupt;
        if (recipe->categorical_unknown == TF_TRANSFORM_UNKNOWN_ERROR) {
            if (!cJSON_IsNull(required_item(encode, "otherOrdinal"))
                || !cJSON_IsNull(required_item(encode, "sentinelLabel"))
                || !json_string_equals(required_item(encode, "unknown"), "error"))
                goto corrupt;
        } else if (recipe->categorical_unknown == TF_TRANSFORM_UNKNOWN_SENTINEL) {
            if (!cJSON_IsNull(required_item(encode, "otherOrdinal"))
                || !safe_int64(
                    required_item(encode, "sentinelLabel"), &sentinel_label)
                || sentinel_label != recipe->categorical_sentinel_label
                || !json_string_equals(
                    required_item(encode, "unknown"), "sentinel")) goto corrupt;
        } else if (recipe->categorical_unknown == TF_TRANSFORM_UNKNOWN_OTHER) {
            if (!safe_uint64(
                    required_item(encode, "otherOrdinal"), &other_ordinal)
                || !cJSON_IsNull(required_item(encode, "sentinelLabel"))
                || !json_string_equals(required_item(encode, "unknown"), "other"))
                goto corrupt;
        } else goto corrupt;
    } else goto corrupt;
    cJSON_ArrayForEach(entry, categories) {
        if (count % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            code = tf_transform_poll_cancel(runtime, error);
            if (code != TF_TRANSFORM_OK) return code;
        }
        if (count == SIZE_MAX) return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "categorical plan category count overflows");
        ++count;
    }
    if (count == 0) goto corrupt;
    if ((uint64_t)count > runtime->limits.max_categories_per_column
        || *total_categories > UINT64_MAX - (uint64_t)count
        || *total_categories + (uint64_t)count
            > runtime->limits.max_total_categories
        || count > SIZE_MAX / sizeof(*out->categories))
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "categorical plan state exceeds category limits");
    if (recipe->categorical_encode == TF_TRANSFORM_ENCODE_LABEL
        && (uint64_t)count > (uint64_t)TF_TRANSFORM_MAX_SAFE_INTEGER_V1)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "categorical label count exceeds the safe-integer domain");
    bytes = count * sizeof(*out->categories);
    if ((uint64_t)bytes > runtime->limits.max_allocation_bytes)
        return tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT,
            "categorical plan allocation exceeds limits");
    out->categories = (tf_transform_category_value *)
        tf_transform_resource_calloc(ledger, count, sizeof(*out->categories), error);
    if (!out->categories)
        return ledger->last_code != TF_TRANSFORM_OK
            ? ledger->last_code : tf_transform_set_error(
                error, TF_TRANSFORM_ALLOCATION,
                "categorical plan state allocation failed");
    out->source_dtype = source_dtype;
    out->category_count = count;
    entry = categories->child;
    for (size_t i = 0; i < count; ++i, entry = entry->next) {
        uint64_t bits;
        if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            code = tf_transform_poll_cancel(runtime, error);
            if (code != TF_TRANSFORM_OK) goto failed;
        }
        if (!tagged_category_bits(entry, source_dtype, &bits)
            || (i != 0 && tf_transform_category_compare(
                out->categories[i - 1].bits, bits, source_dtype) >= 0))
            goto corrupt_allocated;
        out->categories[i].bits = bits;
    }
    if (!tagged_category_bits(
            required_item(impute, "value"), source_dtype, &mode_bits))
        goto corrupt_allocated;
    out->impute = TF_TRANSFORM_CATEGORICAL_IMPUTE_MODE;
    out->all_missing = recipe->categorical_all_missing;
    out->encode = recipe->categorical_encode;
    out->unknown = recipe->categorical_unknown;
    if (out->unknown == TF_TRANSFORM_UNKNOWN_SENTINEL) {
        if (sentinel_label >= 0 && (uint64_t)sentinel_label < (uint64_t)count)
            goto corrupt_allocated;
        out->sentinel_label = sentinel_label;
        out->has_sentinel_label = 1;
    } else if (out->unknown == TF_TRANSFORM_UNKNOWN_OTHER) {
        if (other_ordinal != (uint64_t)count) goto corrupt_allocated;
        out->other_ordinal = other_ordinal;
        out->has_other_ordinal = 1;
    }
    out->impute_bits = mode_bits;
    out->has_impute_value = 1;
    {
        size_t mode_ordinal = 0;
        code = tf_transform_category_lookup(
            out, mode_bits, runtime, &mode_ordinal, &mode_known, error);
    }
    if (code != TF_TRANSFORM_OK) goto failed;
    if (!mode_known) goto corrupt_allocated;
    *total_categories += (uint64_t)count;
    return TF_TRANSFORM_OK;
corrupt_allocated:
    code = tf_transform_set_error(
        error, TF_TRANSFORM_CORRUPT_PLAN,
        "categorical plan state is inconsistent");
failed:
    tf_transform_categorical_state_clear(out);
    tf_transform_resource_release(ledger, (uint64_t)bytes);
    return code;
corrupt:
    return tf_transform_set_error(
        error, TF_TRANSFORM_CORRUPT_PLAN,
        "categorical plan state is inconsistent");
}

tf_transform_code tf_transform_plan_from_json(
    const uint8_t *json, size_t json_len,
    const tf_transform_runtime_copy *runtime,
    tf_transform_plan **out, uint8_t **canonical_out,
    size_t *canonical_len_out, tf_transform_error **error) {
    static const char *const top_keys[] = {
        "format", "inputSchema", "outputSchema", "recipe",
        "recipeSha256", "steps", "version"
    };
    cJSON *root = NULL;
    tf_transform_recipe *recipe = NULL;
    tf_transform_plan *plan = NULL;
    tf_transform_resource_ledger ledger;
    tf_transform_code code;
    uint64_t version;
    char expected_fingerprint[65];
    const cJSON *steps;
    size_t step_count = 0;
    uint64_t total_categories = 0;
    uint64_t ast_resident_bytes = 0;
    if (out) *out = NULL;
    if (canonical_out) *canonical_out = NULL;
    if (canonical_len_out) *canonical_len_out = 0;
    if (!json || json_len == 0 || !runtime || !out
        || !canonical_out || !canonical_len_out)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "plan JSON argument is null");
    tf_transform_resource_ledger_init(&ledger, runtime);
    code = tf_transform_json_parse_bounded_runtime_ledger(
        json, json_len, runtime, &ledger, &root,
        &ast_resident_bytes, NULL, error,
        TF_TRANSFORM_CORRUPT_PLAN);
    if (code != TF_TRANSFORM_OK) return code;
    code = tf_transform_poll_cancel(runtime, error);
    if (code != TF_TRANSFORM_OK) goto done;
    if (!exact_keys(root, top_keys, 7)
        || !json_string_equals(required_item(root, "format"),
                               "tranfi.transform-plan")
        || !safe_uint64(required_item(root, "version"), &version)) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN, "plan JSON header is invalid");
        goto done;
    }
    if (version != 1) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_UNSUPPORTED_VERSION, "plan JSON version is unsupported");
        goto done;
    }
    {
        const cJSON *embedded_recipe = required_item(root, "recipe");
        uint64_t recipe_version;
        uint64_t policy_version;
        if (cJSON_IsObject(embedded_recipe)
            && safe_uint64(required_item(embedded_recipe, "version"),
                           &recipe_version)
            && safe_uint64(required_item(embedded_recipe, "policyVersion"),
                           &policy_version)
            && (recipe_version != 1 || policy_version != 1)) {
            code = tf_transform_set_error(
                error, TF_TRANSFORM_UNSUPPORTED_VERSION,
                "embedded recipe version is unsupported");
            goto done;
        }
    }
    code = recipe_from_json_value(
        required_item(root, "recipe"), runtime, &ledger, &recipe, error);
    if (code != TF_TRANSFORM_OK) {
        if (code == TF_TRANSFORM_INVALID_RECIPE
            || code == TF_TRANSFORM_INVALID_ARGUMENT)
            code = tf_transform_set_error(
                error, TF_TRANSFORM_CORRUPT_PLAN, "embedded recipe is invalid");
        goto done;
    }
    if (!recipe) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL,
            "embedded recipe parser returned no recipe");
        goto done;
    }
    plan = (tf_transform_plan *)tf_transform_resource_calloc(
        &ledger, 1, sizeof(*plan), error);
    if (!plan) {
        code = ledger.last_code != TF_TRANSFORM_OK
            ? ledger.last_code : tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION,
            "imported plan allocation failed");
        goto done;
    }
    atomic_init(&plan->refcount, 1u);
    plan->recipe = recipe;
    recipe = NULL;
    if (!plan->recipe) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_INTERNAL, "recipe parser returned no recipe");
        goto done;
    }
    code = parse_schema_json(
        required_item(root, "inputSchema"), 0, runtime,
        &ledger, &plan->input_schema, error);
    if (code != TF_TRANSFORM_OK) goto done;
    code = parse_schema_json(
        required_item(root, "outputSchema"), 1, runtime,
        &ledger, &plan->output_schema, error);
    if (code != TF_TRANSFORM_OK) goto done;
    if (plan->input_schema.field_count != plan->recipe->column_count
        || plan->output_schema.field_count != plan->input_schema.field_count
        || plan->output_schema.field_count > plan->recipe->max_output_columns) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN,
            "plan recipe and schema column counts disagree");
        goto done;
    }
    for (size_t i = 0; i < plan->input_schema.field_count; ++i) {
        const tf_transform_recipe_column *recipe_column = &plan->recipe->columns[i];
        const tf_transform_schema_field_owned *input = &plan->input_schema.fields[i];
        int source_equal = 0;
        if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            code = tf_transform_poll_cancel(runtime, error);
            if (code != TF_TRANSFORM_OK) goto done;
        }
        code = runtime_span_equal(
            recipe_column->source_id, recipe_column->source_id_len,
            input->id, input->id_len, runtime, &source_equal, error);
        if (code != TF_TRANSFORM_OK) goto done;
        if (!source_equal
            || (recipe_column->kind == TF_TRANSFORM_KIND_NUMERIC
                && recipe_column->impute == TF_TRANSFORM_IMPUTE_CONSTANT
                && recipe_column->constant_dtype != input->dtype)) {
            code = tf_transform_set_error(
                error, TF_TRANSFORM_CORRUPT_PLAN,
                "plan recipe and schema fields disagree");
            goto done;
        }
    }
    code = tf_transform_output_schema_validate_contract(
        &plan->input_schema, plan->recipe, &plan->output_schema,
        runtime, &ledger, TF_TRANSFORM_CORRUPT_PLAN, error);
    if (code != TF_TRANSFORM_OK) goto done;
    {
        int valid_fingerprint = 0;
        code = lowercase_sha256_string_runtime(
            required_item(root, "recipeSha256"), runtime,
            &valid_fingerprint, error);
        if (code != TF_TRANSFORM_OK) goto done;
        if (!valid_fingerprint) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN, "plan recipe fingerprint is invalid");
        goto done;
        }
    }
    code = imported_recipe_fingerprint(
        root, runtime, &ledger, expected_fingerprint, error);
    if (code != TF_TRANSFORM_OK) goto done;
    if (strcmp(required_item(root, "recipeSha256")->valuestring,
               expected_fingerprint) != 0) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN, "plan recipe fingerprint mismatches");
        goto done;
    }
    steps = required_item(root, "steps");
    if (!cJSON_IsArray(steps)) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN, "plan step count is invalid");
        goto done;
    }
    {
        const cJSON *step;
        cJSON_ArrayForEach(step, steps) {
            if (step_count % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
                code = tf_transform_poll_cancel(runtime, error);
                if (code != TF_TRANSFORM_OK) goto done;
            }
            if (step_count == SIZE_MAX) {
                code = tf_transform_set_error(
                    error, TF_TRANSFORM_RESOURCE_LIMIT,
                    "plan step count overflows");
                goto done;
            }
            ++step_count;
        }
    }
    if (step_count != plan->input_schema.field_count) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_CORRUPT_PLAN, "plan step count is invalid");
        goto done;
    }
    if (plan->input_schema.field_count > SIZE_MAX / sizeof(*plan->states)
        || (uint64_t)plan->input_schema.field_count * sizeof(*plan->states)
            > runtime->limits.max_allocation_bytes) {
        code = tf_transform_set_error(
            error, TF_TRANSFORM_RESOURCE_LIMIT, "imported plan state exceeds limits");
        goto done;
    }
    plan->states = (tf_transform_column_state *)tf_transform_resource_calloc(
        &ledger, plan->input_schema.field_count, sizeof(*plan->states), error);
    if (!plan->states) {
        code = ledger.last_code != TF_TRANSFORM_OK
            ? ledger.last_code : tf_transform_set_error(
            error, TF_TRANSFORM_ALLOCATION,
            "imported plan state allocation failed");
        goto done;
    }
    {
        const cJSON *step = steps->child;
        for (size_t i = 0; i < plan->input_schema.field_count;
             ++i, step = step->next) {
        if (i % TF_TRANSFORM_CANCEL_ITERS_V1 == 0) {
            code = tf_transform_poll_cancel(runtime, error);
            if (code != TF_TRANSFORM_OK) goto done;
        }
        if (plan->recipe->columns[i].kind == TF_TRANSFORM_KIND_CATEGORICAL) {
            plan->states[i].kind = TF_TRANSFORM_KIND_CATEGORICAL;
            code = parse_categorical_step(
                step, &plan->recipe->columns[i],
                plan->input_schema.fields[i].dtype,
                runtime, &ledger, &total_categories,
                &plan->states[i].value.categorical, error);
        } else {
            plan->states[i].kind = TF_TRANSFORM_KIND_NUMERIC;
            code = parse_numeric_step(
                step, &plan->recipe->columns[i], runtime,
                &plan->states[i].value.numeric, error);
        }
        if (code != TF_TRANSFORM_OK) goto done;
        }
    }
    code = tf_transform_json_print_canonical_runtime_ledger(
        root, runtime, &ledger, canonical_out, canonical_len_out, error);
    if (code != TF_TRANSFORM_OK) goto done;
    code = tf_transform_poll_cancel(runtime, error);
    if (code != TF_TRANSFORM_OK) goto done;
    plan->import_allocation_count = ledger.allocation_count;
    plan->import_peak_resident_bytes = ledger.peak_resident_bytes;
    code = TF_TRANSFORM_OK;
done:
    tf_transform_recipe_release(recipe);
    {
        int cleanup_cancelled = delete_json_runtime(root, runtime);
        tf_transform_resource_release(&ledger, ast_resident_bytes);
        if (code == TF_TRANSFORM_OK && cleanup_cancelled)
            code = tf_transform_set_error(
                error, TF_TRANSFORM_CANCELLED,
                "prepared transform cancelled during JSON cleanup");
    }
    if (code == TF_TRANSFORM_OK) {
        *out = plan;
        plan = NULL;
    }
    tf_transform_plan_release(plan);
    if (code != TF_TRANSFORM_OK)
        tf_transform_bytes_free(canonical_out, canonical_len_out);
    return code;
}
