/*
 * memory_estimate.c -- conservative retained-state byte estimates.
 *
 * These estimates are plan-policy metadata, not measured allocator accounting.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define UNIQUE_DEFAULT_BLOOM_BYTES (1024u * 1024u)

static int checked_add_size(size_t *acc, size_t value) {
    if (*acc > SIZE_MAX - value) return -1;
    *acc += value;
    return 0;
}

static int checked_mul_size(size_t a, size_t b, size_t *out) {
    if (a != 0 && b > SIZE_MAX / a) return -1;
    *out = a * b;
    return 0;
}

static int checked_add_mul_size(size_t *acc, size_t a, size_t b) {
    size_t product = 0;
    if (checked_mul_size(a, b, &product) != 0) return -1;
    return checked_add_size(acc, product);
}

static int json_size_arg(const cJSON *args, const char *name, size_t *out) {
    return tf_json_get_size_arg(args, name, 1, TF_MAX_SAFE_SIZE_ARG,
                                out, "memory-estimate");
}

static size_t json_array_len_arg(const cJSON *args, const char *name) {
    const cJSON *item = cJSON_GetObjectItemCaseSensitive(args, name);
    if (!cJSON_IsArray(item)) return 0;
    int n = cJSON_GetArraySize(item);
    return n > 0 ? (size_t)n : 0;
}

static size_t json_string_array_bytes_arg(const cJSON *args, const char *name) {
    const cJSON *item = cJSON_GetObjectItemCaseSensitive(args, name);
    if (!cJSON_IsArray(item)) return 0;
    size_t bytes = 0;
    const cJSON *el = NULL;
    cJSON_ArrayForEach(el, item) {
        if (cJSON_IsString(el) && el->valuestring) {
            size_t len = strlen(el->valuestring);
            if (checked_add_size(&bytes, len + 1) != 0) return SIZE_MAX;
        }
    }
    return bytes;
}

static int json_bool_arg_true(const cJSON *args, const char *name) {
    const cJSON *item = cJSON_GetObjectItemCaseSensitive(args, name);
    return cJSON_IsBool(item) && cJSON_IsTrue(item);
}

static const char *json_string_arg(const cJSON *args, const char *name) {
    const cJSON *item = cJSON_GetObjectItemCaseSensitive(args, name);
    return cJSON_IsString(item) ? item->valuestring : NULL;
}

static int category_cap_arg(const cJSON *args, size_t *cap, size_t *literal_bytes) {
    size_t max_categories = 0;
    int has_max = json_size_arg(args, "max_categories", &max_categories);
    if (has_max < 0) return -1;

    size_t n_categories = json_array_len_arg(args, "categories");
    size_t string_bytes = json_string_array_bytes_arg(args, "categories");
    if (string_bytes == SIZE_MAX) return -1;

    const cJSON *unknown = cJSON_GetObjectItemCaseSensitive(args, "unknown");
    if (cJSON_IsString(unknown) && unknown->valuestring && strcmp(unknown->valuestring, "other") == 0) {
        if (checked_add_size(&n_categories, 1) != 0) return -1;
        if (checked_add_size(&string_bytes, strlen("other") + 1) != 0) return -1;
    }

    if (has_max > 0) {
        *cap = max_categories;
    } else if (n_categories > 0) {
        *cap = n_categories;
    } else {
        *cap = 0;
    }
    *literal_bytes = string_bytes;
    return *cap > 0 ? 1 : 0;
}

int tf_estimate_step_state_bytes(const tf_ir_node *node, size_t *out,
                                         char *reason, size_t reason_size) {
    const char *op = node->op ? node->op : "unknown";
    const cJSON *args = node->args;
    size_t est = 0;

    if (strcmp(op, "rowid") == 0) {
        size_t n_cols = json_array_len_arg(args, "columns");
        if (n_cols == 0 || json_bool_arg_true(args, "sorted")) {
            est = 2048;
            if (checked_add_mul_size(&est, n_cols, 128) != 0) goto overflow;
            *out = est;
            return 1;
        }
        size_t max_state_bytes = 0;
        int has_state_bytes = json_size_arg(args, "max_state_bytes", &max_state_bytes);
        if (has_state_bytes < 0) goto overflow;
        if (has_state_bytes) {
            *out = max_state_bytes;
            return 1;
        }
        size_t max_keys = 0;
        int has_max = json_size_arg(args, "max_keys", &max_keys);
        if (has_max < 0) goto overflow;
        if (!has_max) {
            snprintf(reason, reason_size, "step '%s' needs max_keys, max_state_bytes, or sorted=true", op);
            return 0;
        }
        est = 1024;
        if (checked_add_mul_size(&est, max_keys, 224) != 0) goto overflow;
        if (checked_add_mul_size(&est, n_cols, 128) != 0) goto overflow;
        *out = est;
        return 1;
    }

    if (strcmp(op, "unique") == 0 || strcmp(op, "dedup") == 0) {
        size_t n_cols = json_array_len_arg(args, "columns");
        if (json_bool_arg_true(args, "sorted")) {
            est = 2048;
            if (checked_add_mul_size(&est, n_cols, 128) != 0) goto overflow;
            *out = est;
            return 1;
        }
        const char *mode = json_string_arg(args, "mode");
        if (json_bool_arg_true(args, "approx") ||
            (mode && strcmp(mode, "approx") == 0)) {
            size_t bloom_bytes = 0;
            int has_bloom = json_size_arg(args, "bloom_bytes", &bloom_bytes);
            if (has_bloom < 0) goto overflow;
            *out = has_bloom ? bloom_bytes : UNIQUE_DEFAULT_BLOOM_BYTES;
            return 1;
        }
        size_t max_state_bytes = 0;
        int has_state_bytes = json_size_arg(args, "max_state_bytes", &max_state_bytes);
        if (has_state_bytes < 0) goto overflow;
        if (has_state_bytes) {
            *out = max_state_bytes;
            return 1;
        }
        size_t max_keys = 0;
        int has_max = json_size_arg(args, "max_keys", &max_keys);
        if (has_max < 0) goto overflow;
        if (!has_max) {
            snprintf(reason, reason_size, "step '%s' needs max_keys, max_state_bytes, or sorted=true for byte-bounded native execution", op);
            return 0;
        }
        est = 1024;
        if (checked_add_mul_size(&est, max_keys, 256 + n_cols * 32) != 0) goto overflow;
        *out = est;
        return 1;
    }

    if (strcmp(op, "group-agg") == 0) {
        size_t n_group = json_array_len_arg(args, "group_by");
        size_t n_aggs = json_array_len_arg(args, "aggs");
        if (json_string_arg(args, "spill_dir")) {
            size_t spill_memory_bytes = 0;
            int has_spill_bytes = json_size_arg(args, "spill_memory_bytes", &spill_memory_bytes);
            if (has_spill_bytes < 0) goto overflow;
            *out = has_spill_bytes ? spill_memory_bytes : 0;
            return has_spill_bytes ? 1 : 0;
        }
        size_t max_state_bytes = 0;
        int has_state_bytes = json_size_arg(args, "max_state_bytes", &max_state_bytes);
        if (has_state_bytes < 0) goto overflow;
        if (has_state_bytes) {
            *out = max_state_bytes;
            return 1;
        }
        if (json_bool_arg_true(args, "sorted")) {
            est = 4096;
            if (checked_add_mul_size(&est, n_group, 128) != 0) goto overflow;
            if (checked_add_mul_size(&est, n_aggs, 128) != 0) goto overflow;
            *out = est;
            return 1;
        }
        size_t max_groups = 0;
        int has_max = json_size_arg(args, "max_groups", &max_groups);
        if (has_max < 0) goto overflow;
        if (!has_max) {
            snprintf(reason, reason_size, "step '%s' needs max_groups, max_state_bytes, or sorted=true for byte-bounded native execution", op);
            return 0;
        }
        size_t per_group = 384;
        if (checked_add_mul_size(&per_group, n_group, 64) != 0) goto overflow;
        if (checked_add_mul_size(&per_group, n_aggs, 96) != 0) goto overflow;
        est = 2048;
        if (checked_add_mul_size(&est, max_groups, per_group) != 0) goto overflow;
        *out = est;
        return 1;
    }

    if (strcmp(op, "frequency") == 0) {
        size_t max_state_bytes = 0;
        int has_state_bytes = json_size_arg(args, "max_state_bytes", &max_state_bytes);
        if (has_state_bytes < 0) goto overflow;
        if (has_state_bytes) {
            *out = max_state_bytes;
            return 1;
        }
        size_t max_values = 0;
        int has_max = json_size_arg(args, "max_values", &max_values);
        if (has_max < 0) goto overflow;
        if (!has_max) {
            snprintf(reason, reason_size, "step '%s' needs max_values or max_state_bytes for byte-bounded native execution", op);
            return 0;
        }
        size_t n_cols = json_array_len_arg(args, "columns");
        cJSON *overflow_arg = args ? cJSON_GetObjectItemCaseSensitive(args, "overflow") : NULL;
        if (cJSON_IsString(overflow_arg) && strcmp(overflow_arg->valuestring, "other") == 0) {
            if (max_values == SIZE_MAX) goto overflow;
            max_values += 1;
        }
        est = 1024;
        if (checked_add_mul_size(&est, max_values, 224 + n_cols * 32) != 0) goto overflow;
        *out = est;
        return 1;
    }

    if (strcmp(op, "onehot") == 0 || strcmp(op, "label-encode") == 0) {
        size_t max_state_bytes = 0;
        int has_state_bytes = json_size_arg(args, "max_state_bytes", &max_state_bytes);
        if (has_state_bytes < 0) goto overflow;
        if (has_state_bytes) {
            *out = max_state_bytes;
            return 1;
        }
        size_t cap = 0;
        size_t literal_bytes = 0;
        int has_cap = category_cap_arg(args, &cap, &literal_bytes);
        if (has_cap < 0) goto overflow;
        if (!has_cap) {
            snprintf(reason, reason_size, "step '%s' needs max_categories, max_state_bytes, or declared categories", op);
            return 0;
        }
        est = 1024;
        if (checked_add_size(&est, literal_bytes) != 0) goto overflow;
        if (checked_add_mul_size(&est, cap, strcmp(op, "onehot") == 0 ? 320 : 224) != 0) goto overflow;
        *out = est;
        return 1;
    }

    if (strcmp(op, "intersect") == 0 || strcmp(op, "setdiff") == 0 ||
        strcmp(op, "intersect-all") == 0 || strcmp(op, "setdiff-all") == 0) {
        int bag_set_op = strcmp(op, "intersect-all") == 0 || strcmp(op, "setdiff-all") == 0;
        if (json_string_arg(args, "spill_dir")) {
            size_t spill_memory_bytes = 0;
            int has_spill_bytes = json_size_arg(args, "spill_memory_bytes", &spill_memory_bytes);
            if (has_spill_bytes < 0) goto overflow;
            *out = has_spill_bytes ? spill_memory_bytes : 0;
            return has_spill_bytes ? 1 : 0;
        }
        if (json_bool_arg_true(args, "sorted")) {
            size_t n_cols = json_array_len_arg(args, "columns");
            est = 4096;
            if (checked_add_mul_size(&est, n_cols, 256) != 0) goto overflow;
            *out = est;
            return 1;
        }
        size_t max_lookup_bytes = 0;
        int has_bytes = json_size_arg(args, "max_lookup_bytes", &max_lookup_bytes);
        if (has_bytes < 0) goto overflow;
        if (!has_bytes) {
            snprintf(reason, reason_size, "step '%s' needs max_lookup_bytes or sorted=true for byte-bounded native execution", op);
            return 0;
        }
        size_t max_state_bytes = 0;
        int has_state_bytes = json_size_arg(args, "max_state_bytes", &max_state_bytes);
        if (has_state_bytes < 0) goto overflow;
        if (has_state_bytes) {
            est = 4096;
            if (checked_add_mul_size(&est, max_lookup_bytes, 3) != 0) goto overflow;
            if (checked_add_size(&est, max_state_bytes) != 0) goto overflow;
            *out = est;
            return 1;
        }
        size_t n_cols = json_array_len_arg(args, "columns");
        est = 4096;
        if (checked_add_mul_size(&est, max_lookup_bytes, 3) != 0) goto overflow;
        size_t max_lookup_keys = 0;
        int has_lookup_keys = json_size_arg(args, "max_lookup_keys", &max_lookup_keys);
        if (has_lookup_keys < 0) goto overflow;
        if (bag_set_op) {
            if (!has_lookup_keys) {
                snprintf(reason, reason_size, "step '%s' needs max_lookup_keys or max_state_bytes for bag-count set semantics", op);
                return 0;
            }
            if (checked_add_mul_size(&est, max_lookup_keys, 224 + n_cols * 32) != 0) goto overflow;
            *out = est;
            return 1;
        }
        size_t max_output_keys = 0;
        int has_output_keys = json_size_arg(args, "max_output_keys", &max_output_keys);
        if (has_output_keys < 0) goto overflow;
        if (!has_output_keys) {
            snprintf(reason, reason_size, "step '%s' needs max_output_keys or max_state_bytes for duplicate-eliminating set semantics", op);
            return 0;
        }
        if (has_lookup_keys) {
            if (checked_add_mul_size(&est, max_lookup_keys, 192 + n_cols * 32) != 0) goto overflow;
        }
        if (checked_add_mul_size(&est, max_output_keys, 192 + n_cols * 32) != 0) goto overflow;
        *out = est;
        return 1;
    }

    if (strcmp(op, "pivot") == 0) {
        size_t cap = 0;
        size_t literal_bytes = 0;
        int has_cap = category_cap_arg(args, &cap, &literal_bytes);
        if (has_cap < 0) goto overflow;
        if (json_string_arg(args, "spill_dir")) {
            if (!has_cap) {
                snprintf(reason, reason_size,
                         "step 'pivot' spill mode needs categories or max_categories for byte-bounded output columns");
                return 0;
            }
            size_t spill_memory_bytes = 0;
            int has_spill_bytes = json_size_arg(args, "spill_memory_bytes", &spill_memory_bytes);
            if (has_spill_bytes < 0) goto overflow;
            *out = has_spill_bytes ? spill_memory_bytes : 0;
            return has_spill_bytes ? 1 : 0;
        }
        if (json_bool_arg_true(args, "sorted") && has_cap) {
            est = 4096 + literal_bytes;
            if (checked_add_mul_size(&est, cap, 96) != 0) goto overflow;
            *out = est;
            return 1;
        }
    }

    if (strcmp(op, "union") == 0) {
        if (json_bool_arg_true(args, "sorted")) {
            size_t n_cols = json_array_len_arg(args, "columns");
            est = 4096;
            if (checked_add_mul_size(&est, n_cols, 256) != 0) goto overflow;
            *out = est;
            return 1;
        }
        if (json_string_arg(args, "spill_dir")) {
            size_t spill_memory_bytes = 0;
            int has_spill_bytes = json_size_arg(args, "spill_memory_bytes", &spill_memory_bytes);
            if (has_spill_bytes < 0) goto overflow;
            *out = has_spill_bytes ? spill_memory_bytes : 0;
            return has_spill_bytes ? 1 : 0;
        }
        size_t max_state_bytes = 0;
        int has_state_bytes = json_size_arg(args, "max_state_bytes", &max_state_bytes);
        if (has_state_bytes < 0) goto overflow;
        if (has_state_bytes) {
            *out = max_state_bytes;
            return 1;
        }
        size_t max_output_keys = 0;
        int has_output_keys = json_size_arg(args, "max_output_keys", &max_output_keys);
        if (has_output_keys < 0) goto overflow;
        if (!has_output_keys) {
            snprintf(reason, reason_size, "step 'union' needs max_output_keys or max_state_bytes for duplicate-eliminating set semantics");
            return 0;
        }
        size_t n_cols = json_array_len_arg(args, "columns");
        est = 4096;
        if (checked_add_mul_size(&est, max_output_keys, 192 + n_cols * 32) != 0) goto overflow;
        *out = est;
        return 1;
    }

    if (strcmp(op, "join") == 0 || strcmp(op, "semi-join") == 0 || strcmp(op, "anti-join") == 0) {
        const char *how = json_string_arg(args, "how");
        int filtering = strcmp(op, "semi-join") == 0 || strcmp(op, "anti-join") == 0 ||
                        (how && (strcmp(how, "semi") == 0 || strcmp(how, "anti") == 0));
        if (json_string_arg(args, "spill_dir")) {
            if (!filtering) {
                size_t max_matches = 0;
                int has_matches = json_size_arg(args, "max_matches_per_row", &max_matches);
                if (has_matches < 0) goto overflow;
                if (!has_matches) {
                    snprintf(reason, reason_size,
                             "step '%s' spill mode needs max_matches_per_row for byte-bounded mutating join", op);
                    return 0;
                }
            }
            size_t spill_memory_bytes = 0;
            int has_spill_bytes = json_size_arg(args, "spill_memory_bytes", &spill_memory_bytes);
            if (has_spill_bytes < 0) goto overflow;
            *out = has_spill_bytes ? spill_memory_bytes : 0;
            return has_spill_bytes ? 1 : 0;
        }
        if (json_bool_arg_true(args, "sorted")) {
            est = 4096;
            if (!filtering) {
                size_t max_matches = 0;
                int has_matches = json_size_arg(args, "max_matches_per_row", &max_matches);
                if (has_matches < 0) goto overflow;
                if (!has_matches) {
                    snprintf(reason, reason_size,
                             "step '%s' sorted=true needs max_matches_per_row for byte-bounded mutating join", op);
                    return 0;
                }
                if (checked_add_mul_size(&est, max_matches, 512) != 0) goto overflow;
            }
            *out = est;
            return 1;
        }
        size_t max_lookup_bytes = 0;
        int has_bytes = json_size_arg(args, "max_lookup_bytes", &max_lookup_bytes);
        if (has_bytes < 0) goto overflow;
        if (!has_bytes) {
            snprintf(reason, reason_size, "step '%s' needs max_lookup_bytes or sorted=true for byte-bounded native execution", op);
            return 0;
        }
        est = 4096;
        if (checked_add_mul_size(&est, max_lookup_bytes, 4) != 0) goto overflow;
        size_t max_state_bytes = 0;
        int has_state_bytes = json_size_arg(args, "max_state_bytes", &max_state_bytes);
        if (has_state_bytes < 0) goto overflow;
        if (has_state_bytes) {
            if (checked_add_size(&est, max_state_bytes) != 0) goto overflow;
            *out = est;
            return 1;
        }
        size_t max_lookup_rows = 0;
        if (json_size_arg(args, "max_lookup_rows", &max_lookup_rows) > 0) {
            if (checked_add_mul_size(&est, max_lookup_rows, 96) != 0) goto overflow;
        }
        size_t max_lookup_keys = 0;
        if (json_size_arg(args, "max_lookup_keys", &max_lookup_keys) > 0) {
            if (checked_add_mul_size(&est, max_lookup_keys, 192) != 0) goto overflow;
        }
        *out = est;
        return 1;
    }

    snprintf(reason, reason_size, "step '%s' has no native byte estimator", op);
    return 0;

overflow:
    snprintf(reason, reason_size, "step '%s' byte estimate overflowed", op);
    return 0;
}

int tf_estimate_key_state_plan_bytes(const tf_ir_plan *ir, size_t *out,
                                         const tf_ir_node **failed_node,
                                         char *reason, size_t reason_size) {
    size_t total = 0;
    *failed_node = NULL;
    for (size_t i = 0; i < ir->n_nodes; i++) {
        const tf_ir_node *node = &ir->nodes[i];
        if (node->memory_class != TF_MEM_KEY_STATE) continue;
        size_t node_est = 0;
        if (!tf_estimate_step_state_bytes(node, &node_est, reason, reason_size)) {
            *failed_node = node;
            return 0;
        }
        if (checked_add_size(&total, node_est) != 0) {
            snprintf(reason, reason_size, "total key-state byte estimate overflowed");
            *failed_node = node;
            return 0;
        }
    }
    *out = total;
    return 1;
}
