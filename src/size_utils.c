/*
 * size_utils.c -- checked size arithmetic and JSON size arguments.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdio.h>
#include <stdlib.h>

int tf_format_float64(char *buf, size_t buf_size, double value) {
    if (!buf || buf_size == 0) return TF_ERROR;
    int n = snprintf(buf, buf_size, TF_FLOAT64_ROUNDTRIP_FORMAT, value);
    if (n < 0 || (size_t)n >= buf_size) return TF_ERROR;
    return TF_OK;
}

int tf_size_add(size_t a, size_t b, size_t *out) {
    if (!out) return TF_ERROR;
    if (a > SIZE_MAX - b) return TF_ERROR;
    *out = a + b;
    return TF_OK;
}

int tf_size_mul(size_t a, size_t b, size_t *out) {
    if (!out) return TF_ERROR;
    if (a != 0 && b > SIZE_MAX / a) return TF_ERROR;
    *out = a * b;
    return TF_OK;
}

int tf_size_align(size_t x, size_t align, size_t *out) {
    if (!out || align == 0) return TF_ERROR;
    size_t rem = x % align;
    if (rem == 0) {
        *out = x;
        return TF_OK;
    }
    return tf_size_add(x, align - rem, out);
}

int tf_size_grow_pow2(size_t current, size_t min_value, size_t min_capacity, size_t *out) {
    if (!out) return TF_ERROR;
    size_t cap = current ? current : min_capacity;
    if (cap == 0) cap = 1;
    while (cap < min_value) {
        if (cap > SIZE_MAX / 2) return TF_ERROR;
        cap *= 2;
    }
    *out = cap;
    return TF_OK;
}

void *tf_mallocarray_checked(size_t count, size_t elem_size) {
    size_t bytes = 0;
    if (tf_size_mul(count, elem_size, &bytes) != TF_OK) return NULL;
    if (bytes == 0) bytes = 1;
    return malloc(bytes);
}

void *tf_callocarray_checked(size_t count, size_t elem_size) {
    if (count == 0 || elem_size == 0) return calloc(1, 1);
    size_t bytes = 0;
    if (tf_size_mul(count, elem_size, &bytes) != TF_OK) return NULL;
    return calloc(1, bytes);
}

void *tf_reallocarray_checked(void *ptr, size_t count, size_t elem_size) {
    size_t bytes = 0;
    if (tf_size_mul(count, elem_size, &bytes) != TF_OK) return NULL;
    if (bytes == 0) bytes = 1;
    return realloc(ptr, bytes);
}

static void tf_index_swap(size_t *a, size_t *b) {
    size_t tmp = *a;
    *a = *b;
    *b = tmp;
}

static void tf_sort_indices_sift_down(size_t *indices, size_t start, size_t end,
                                      tf_index_compare_fn compare, const void *ctx) {
    size_t root = start;
    while (root <= (end - 1) / 2) {
        size_t child = root * 2 + 1;
        size_t swap_idx = root;
        if (compare(ctx, indices[swap_idx], indices[child]) < 0) {
            swap_idx = child;
        }
        if (child + 1 <= end &&
            compare(ctx, indices[swap_idx], indices[child + 1]) < 0) {
            swap_idx = child + 1;
        }
        if (swap_idx == root) return;
        tf_index_swap(&indices[root], &indices[swap_idx]);
        root = swap_idx;
    }
}

void tf_sort_indices(size_t *indices, size_t n, tf_index_compare_fn compare, const void *ctx) {
    if (!indices || !compare || n < 2) return;

    size_t start = (n - 2) / 2 + 1;
    while (start > 0) {
        start--;
        tf_sort_indices_sift_down(indices, start, n - 1, compare, ctx);
    }

    size_t end = n - 1;
    while (end > 0) {
        tf_index_swap(&indices[0], &indices[end]);
        end--;
        if (end > 0) tf_sort_indices_sift_down(indices, 0, end, compare, ctx);
    }
}

int tf_check_byte_limit(size_t n, size_t max_value,
                        const char *context, const char *name) {
    if (n <= max_value) return TF_OK;
    char msg[256];
    snprintf(msg, sizeof(msg), "%s: %s exceeds maximum %zu bytes",
             context ? context : "size", name ? name : "value", max_value);
    tf_set_last_error(msg);
    return TF_ERROR;
}

int tf_string_length_bounded(const char *s, size_t max_value,
                             size_t *out, const char *context,
                             const char *name) {
    if (!s || !out) return TF_ERROR;
    for (size_t i = 0; ; i++) {
        if (s[i] == '\0') {
            *out = i;
            return TF_OK;
        }
        if (i == max_value) break;
    }
    return tf_check_byte_limit(max_value == SIZE_MAX ? SIZE_MAX : max_value + 1,
                               max_value, context, name);
}

int tf_json_size_value(const cJSON *item, const char *name,
                       size_t min_value, size_t max_value,
                       size_t *out, const char *context) {
    if (!out) return -1;
    if (!item) return 0;
    double d = item->valuedouble;
    if (!cJSON_IsNumber(item) || !(d >= 0.0) ||
        d < (double)min_value || d > (double)max_value) {
        char msg[256];
        snprintf(msg, sizeof(msg), "%s: %s must be an integer between %zu and %zu",
                 context ? context : "argument", name, min_value, max_value);
        tf_set_last_error(msg);
        return -1;
    }
    size_t parsed = (size_t)d;
    if ((double)parsed != d) {
        char msg[256];
        snprintf(msg, sizeof(msg), "%s: %s must be an integer between %zu and %zu",
                 context ? context : "argument", name, min_value, max_value);
        tf_set_last_error(msg);
        return -1;
    }
    *out = parsed;
    return 1;
}

int tf_json_get_size_arg(const cJSON *args, const char *name,
                         size_t min_value, size_t max_value,
                         size_t *out, const char *context) {
    if (!out) return -1;
    if (!args || !name) return 0;
    const cJSON *item = cJSON_GetObjectItemCaseSensitive(args, name);
    if (!item) return 0;
    return tf_json_size_value(item, name, min_value, max_value, out, context);
}

int tf_json_get_size_arg_any(const cJSON *args, const char *name,
                             const char *alt_name,
                             size_t min_value, size_t max_value,
                             size_t *out, const char *context) {
    if (!out) return -1;
    if (!args || !name) return 0;
    const cJSON *item = cJSON_GetObjectItemCaseSensitive(args, name);
    const char *used_name = name;
    if (!item && alt_name) {
        item = cJSON_GetObjectItemCaseSensitive(args, alt_name);
        used_name = alt_name;
    }
    if (!item) return 0;
    return tf_json_size_value(item, used_name, min_value, max_value, out, context);
}
