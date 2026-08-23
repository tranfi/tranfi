/*
 * size_utils.c -- checked size arithmetic and JSON size arguments.
 */

#if defined(__linux__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif

#include "internal.h"
#include "cJSON.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

char *tf_strdup_checked(const char *s) {
    const char *src = s ? s : "";
    size_t len = strlen(src);
    size_t bytes = 0;
    if (tf_size_add(len, 1, &bytes) != TF_OK) return NULL;
    char *copy = tf_mallocarray_checked(bytes, sizeof(char));
    if (!copy) return NULL;
    memcpy(copy, src, bytes);
    return copy;
}

char *tf_string_append_suffix_checked(const char *prefix, const char *suffix) {
    const char *a = prefix ? prefix : "";
    const char *b = suffix ? suffix : "";
    size_t a_len = strlen(a);
    size_t b_len = strlen(b);
    size_t text_len = 0;
    size_t bytes = 0;
    if (tf_size_add(a_len, b_len, &text_len) != TF_OK ||
        tf_size_add(text_len, 1, &bytes) != TF_OK) return NULL;
    char *copy = tf_mallocarray_checked(bytes, sizeof(char));
    if (!copy) return NULL;
    memcpy(copy, a, a_len);
    memcpy(copy + a_len, b, b_len);
    copy[text_len] = '\0';
    return copy;
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

static void tf_sort_indices_heap_range(size_t *indices, size_t n,
                                       tf_index_compare_fn compare, const void *ctx) {
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

static void tf_sort_indices_insertion_range(size_t *indices, size_t lo, size_t hi,
                                            tf_index_compare_fn compare, const void *ctx) {
    for (size_t i = lo + 1; i < hi; i++) {
        size_t v = indices[i];
        size_t j = i;
        while (j > lo && compare(ctx, v, indices[j - 1]) < 0) {
            indices[j] = indices[j - 1];
            j--;
        }
        indices[j] = v;
    }
}

static size_t tf_sort_indices_median3(size_t *indices, size_t a, size_t b, size_t c,
                                      tf_index_compare_fn compare, const void *ctx) {
    size_t va = indices[a];
    size_t vb = indices[b];
    size_t vc = indices[c];

    if (compare(ctx, va, vb) < 0) {
        if (compare(ctx, vb, vc) < 0) return vb;
        return compare(ctx, va, vc) < 0 ? vc : va;
    }
    if (compare(ctx, va, vc) < 0) return va;
    return compare(ctx, vb, vc) < 0 ? vc : vb;
}

static size_t tf_sort_indices_partition(size_t *indices, size_t lo, size_t hi, size_t pivot,
                                        tf_index_compare_fn compare, const void *ctx) {
    size_t i = lo;
    size_t j = hi;
    for (;;) {
        while (compare(ctx, indices[i], pivot) < 0) i++;
        do {
            j--;
        } while (compare(ctx, pivot, indices[j]) < 0);
        if (i >= j) return i;
        tf_index_swap(&indices[i], &indices[j]);
        i++;
    }
}

static size_t tf_sort_indices_depth_limit(size_t n) {
    size_t depth = 0;
    while (n > 1) {
        depth++;
        n >>= 1;
    }
    return depth * 2;
}

static void tf_sort_indices_intro_loop(size_t *indices, size_t lo, size_t hi, size_t depth_limit,
                                       tf_index_compare_fn compare, const void *ctx) {
    enum { INSERTION_THRESHOLD = 16 };

    while (hi - lo > INSERTION_THRESHOLD) {
        if (depth_limit == 0) {
            tf_sort_indices_heap_range(indices + lo, hi - lo, compare, ctx);
            return;
        }
        depth_limit--;

        size_t mid = lo + (hi - lo) / 2;
        size_t pivot = tf_sort_indices_median3(indices, lo, mid, hi - 1, compare, ctx);
        size_t cut = tf_sort_indices_partition(indices, lo, hi, pivot, compare, ctx);
        if (cut <= lo || cut >= hi) {
            tf_sort_indices_heap_range(indices + lo, hi - lo, compare, ctx);
            return;
        }

        if (cut - lo < hi - cut) {
            tf_sort_indices_intro_loop(indices, lo, cut, depth_limit, compare, ctx);
            lo = cut;
        } else {
            tf_sort_indices_intro_loop(indices, cut, hi, depth_limit, compare, ctx);
            hi = cut;
        }
    }
    tf_sort_indices_insertion_range(indices, lo, hi, compare, ctx);
}

typedef struct {
    tf_index_compare_fn compare;
    const void *ctx;
} tf_sort_indices_qsort_ctx;

static int tf_sort_indices_total_compare(const void *ctx, size_t ia, size_t ib) {
    const tf_sort_indices_qsort_ctx *qctx = (const tf_sort_indices_qsort_ctx *)ctx;
    int result = qctx->compare(qctx->ctx, ia, ib);
    if (result != 0) return result;
    return ia < ib ? -1 : ia > ib ? 1 : 0;
}

#if defined(__GLIBC__)
static int tf_sort_indices_qsort_compare_glibc(const void *a, const void *b, void *thunk) {
    const tf_sort_indices_qsort_ctx *qctx = (const tf_sort_indices_qsort_ctx *)thunk;
    size_t ia = *(const size_t *)a;
    size_t ib = *(const size_t *)b;
    return tf_sort_indices_total_compare(qctx, ia, ib);
}
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
static int tf_sort_indices_qsort_compare_bsd(void *thunk, const void *a, const void *b) {
    const tf_sort_indices_qsort_ctx *qctx = (const tf_sort_indices_qsort_ctx *)thunk;
    size_t ia = *(const size_t *)a;
    size_t ib = *(const size_t *)b;
    return tf_sort_indices_total_compare(qctx, ia, ib);
}
#endif

void tf_sort_indices(size_t *indices, size_t n, tf_index_compare_fn compare, const void *ctx) {
    if (!indices || !compare || n < 2) return;

#if defined(__GLIBC__)
    tf_sort_indices_qsort_ctx qctx = { compare, ctx };
    qsort_r(indices, n, sizeof(size_t), tf_sort_indices_qsort_compare_glibc, &qctx);
#elif defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    tf_sort_indices_qsort_ctx qctx = { compare, ctx };
    qsort_r(indices, n, sizeof(size_t), &qctx, tf_sort_indices_qsort_compare_bsd);
#else
    tf_sort_indices_qsort_ctx qctx = { compare, ctx };
    tf_sort_indices_intro_loop(indices, 0, n, tf_sort_indices_depth_limit(n),
                               tf_sort_indices_total_compare, &qctx);
#endif
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
