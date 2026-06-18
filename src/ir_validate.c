/*
 * ir_validate.c — Validation pass over an IR plan.
 *
 * Checks:
 * 1. At least one node
 * 2. First node must be a decoder
 * 3. Last node must be an encoder
 * 4. All op names exist in the registry
 * 5. Required args are present
 * 6. No multiple decoders or encoders
 */

#include "ir.h"
#include "tranfi.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define TF_OK    0
#define TF_ERROR (-1)

static void set_plan_error(tf_ir_plan *plan, const char *msg) {
    free(plan->error);
    plan->error = strdup(msg);
}

static void set_plan_errorf(tf_ir_plan *plan, const char *fmt, const char *detail) {
    free(plan->error);
    char buf[256];
    snprintf(buf, sizeof(buf), fmt, detail);
    plan->error = strdup(buf);
}

static int node_bool_arg_true(const tf_ir_node *node, const char *name) {
    cJSON *item = node && node->args ? cJSON_GetObjectItemCaseSensitive(node->args, name) : NULL;
    return cJSON_IsTrue(item);
}

static int node_array_arg_nonempty(const tf_ir_node *node, const char *name) {
    cJSON *item = node && node->args ? cJSON_GetObjectItemCaseSensitive(node->args, name) : NULL;
    return cJSON_IsArray(item) && cJSON_GetArraySize(item) > 0;
}


#define TF_POLICY_PATH_MAX 4096

static int path_is_absolute(const char *path) {
    if (!path || !path[0]) return 0;
    if (path[0] == '/' || path[0] == '\\') return 1;
    return ((path[0] >= 'A' && path[0] <= 'Z') || (path[0] >= 'a' && path[0] <= 'z')) &&
           path[1] == ':';
}

static int append_char(char *out, size_t out_sz, size_t *len, char ch) {
    if (*len + 1 >= out_sz) return TF_ERROR;
    out[(*len)++] = ch;
    out[*len] = '\0';
    return TF_OK;
}

static int append_segment(char *out, size_t out_sz, size_t *len,
                          const char *seg, size_t seg_len, size_t prefix_len) {
    if (seg_len == 0) return TF_OK;
    if (*len > 0 && out[*len - 1] != '/' &&
        !(*len == prefix_len && prefix_len > 0 && out[prefix_len - 1] == '/')) {
        if (append_char(out, out_sz, len, '/') != TF_OK) return TF_ERROR;
    }
    if (*len + seg_len >= out_sz) return TF_ERROR;
    memcpy(out + *len, seg, seg_len);
    *len += seg_len;
    out[*len] = '\0';
    return TF_OK;
}

static int pop_segment(char *out, size_t *len, size_t prefix_len) {
    if (*len <= prefix_len) return TF_ERROR;
    while (*len > prefix_len && out[*len - 1] != '/') (*len)--;
    if (*len > prefix_len && out[*len - 1] == '/') (*len)--;
    if (*len < prefix_len) *len = prefix_len;
    out[*len] = '\0';
    return TF_OK;
}

static int normalize_path_lexical(const char *in, char *out, size_t out_sz) {
    if (!in || !in[0] || !out || out_sz == 0) return TF_ERROR;
    size_t len = 0;
    size_t prefix_len = 0;
    const char *p = in;

    if (((p[0] >= 'A' && p[0] <= 'Z') || (p[0] >= 'a' && p[0] <= 'z')) && p[1] == ':') {
        if (out_sz < 3) return TF_ERROR;
        out[len++] = p[0];
        out[len++] = ':';
        out[len] = '\0';
        p += 2;
        if (*p == '/' || *p == '\\') {
            if (append_char(out, out_sz, &len, '/') != TF_OK) return TF_ERROR;
            while (*p == '/' || *p == '\\') p++;
        }
        prefix_len = len;
    } else if (*p == '/' || *p == '\\') {
        if (append_char(out, out_sz, &len, '/') != TF_OK) return TF_ERROR;
        while (*p == '/' || *p == '\\') p++;
        prefix_len = len;
    }

    while (*p) {
        while (*p == '/' || *p == '\\') p++;
        const char *seg = p;
        while (*p && *p != '/' && *p != '\\') p++;
        size_t seg_len = (size_t)(p - seg);
        if (seg_len == 0 || (seg_len == 1 && seg[0] == '.')) continue;
        if (seg_len == 2 && seg[0] == '.' && seg[1] == '.') {
            if (pop_segment(out, &len, prefix_len) != TF_OK) return TF_ERROR;
            continue;
        }
        if (append_segment(out, out_sz, &len, seg, seg_len, prefix_len) != TF_OK) return TF_ERROR;
    }

    if (len == 0) {
        if (out_sz < 2) return TF_ERROR;
        out[0] = '.';
        out[1] = '\0';
    }
    return TF_OK;
}

static int path_is_under_root(const char *root, const char *path) {
    size_t n = strlen(root);
    if (strcmp(root, "/") == 0) return path[0] == '/';
    return strcmp(root, path) == 0 || (strncmp(root, path, n) == 0 && path[n] == '/');
}

static int copy_path_checked(const char *src, char *out, size_t out_sz) {
    if (!src || !out || out_sz == 0) return TF_ERROR;
    size_t n = strlen(src);
    if (n + 1 > out_sz) return TF_ERROR;
    memcpy(out, src, n + 1);
    return TF_OK;
}

static int canonicalize_existing_path(const char *path, char *out, size_t out_sz) {
#ifdef _WIN32
    return normalize_path_lexical(path, out, out_sz);
#else
    char *resolved = realpath(path, NULL);
    if (!resolved) return TF_ERROR;
    int rc = copy_path_checked(resolved, out, out_sz);
    free(resolved);
    return rc;
#endif
}

static int resolve_workspace_path(const char *workspace_root, const char *logical,
                                  char *out, size_t out_sz) {
    char root[TF_POLICY_PATH_MAX];
    char joined[TF_POLICY_PATH_MAX];
    char candidate[TF_POLICY_PATH_MAX];
    char resolved[TF_POLICY_PATH_MAX];
    if (canonicalize_existing_path(workspace_root, root, sizeof(root)) != TF_OK) return TF_ERROR;
    if (path_is_absolute(logical)) {
        if (normalize_path_lexical(logical, candidate, sizeof(candidate)) != TF_OK) return TF_ERROR;
    } else {
        int n = snprintf(joined, sizeof(joined), "%s/%s", root, logical);
        if (n < 0 || (size_t)n >= sizeof(joined)) return TF_ERROR;
        if (normalize_path_lexical(joined, candidate, sizeof(candidate)) != TF_OK) return TF_ERROR;
    }
    if (canonicalize_existing_path(candidate, resolved, sizeof(resolved)) != TF_OK) return TF_ERROR;
    if (!path_is_under_root(root, resolved)) return TF_ERROR;
    if (copy_path_checked(resolved, out, out_sz) != TF_OK) return TF_ERROR;
    return TF_OK;
}

static void set_policy_error(tf_ir_plan *plan, const tf_ir_node *node,
                             const char *arg, const char *reason) {
    free(plan->error);
    char buf[384];
    snprintf(buf, sizeof(buf), "capability denied: op '%s' arg '%s' %s",
             node && node->op ? node->op : "unknown", arg ? arg : "?", reason ? reason : "rejected");
    plan->error = strdup(buf);
}

static int policy_rewrite_path_arg(tf_ir_plan *plan, tf_ir_node *node,
                                   const tf_host_policy *policy,
                                   const char *arg, int is_spill, int is_rules_file) {
    cJSON *item = node && node->args ? cJSON_GetObjectItemCaseSensitive(node->args, arg) : NULL;
    if (!cJSON_IsString(item) || !item->valuestring || item->valuestring[0] == '\0') return TF_OK;
    if (!policy) return TF_OK;

    if (!policy->allow_fs) {
        set_policy_error(plan, node, arg, "requires filesystem access (allow_fs=false)");
        return TF_ERROR;
    }
    if (is_spill && !policy->allow_spill) {
        set_policy_error(plan, node, arg, "requires spill access (allow_spill=false)");
        return TF_ERROR;
    }
    if (is_rules_file && !policy->allow_rules_file) {
        set_policy_error(plan, node, arg, "requires rules_file access (allow_rules_file=false)");
        return TF_ERROR;
    }

    if (policy->resolve_path || (policy->workspace_root && policy->workspace_root[0])) {
        char resolved[TF_POLICY_PATH_MAX];
        int rc = policy->resolve_path
            ? policy->resolve_path(item->valuestring, resolved, sizeof(resolved), policy->user)
            : resolve_workspace_path(policy->workspace_root, item->valuestring, resolved, sizeof(resolved));
        if (rc != TF_OK || resolved[0] == '\0') {
            set_policy_error(plan, node, arg, "was rejected by host path resolver");
            return TF_ERROR;
        }
        if (!cJSON_SetValuestring(item, resolved)) {
            set_plan_error(plan, "out of memory while resolving host path");
            return TF_ERROR;
        }
    }
    return TF_OK;
}

static int enforce_host_policy(tf_ir_plan *plan, tf_ir_node *node, const tf_host_policy *policy) {
    if (!policy) return TF_OK;
    if (!policy->allow_net && (node->caps & TF_CAP_NET)) {
        set_policy_error(plan, node, "network", "requires network access (allow_net=false)");
        return TF_ERROR;
    }
    if (!policy->allow_blocking && node->memory_class == TF_MEM_BLOCKING) {
        set_policy_error(plan, node, "memory", "is blocking (allow_blocking=false)");
        return TF_ERROR;
    }
    if (policy_rewrite_path_arg(plan, node, policy, "file", 0, 0) != TF_OK) return TF_ERROR;
    if (policy_rewrite_path_arg(plan, node, policy, "spill_dir", 1, 0) != TF_OK) return TF_ERROR;
    if (policy_rewrite_path_arg(plan, node, policy, "rules_file", 0, 1) != TF_OK) return TF_ERROR;
    if (policy_rewrite_path_arg(plan, node, policy, "rulesFile", 0, 1) != TF_OK) return TF_ERROR;
    return TF_OK;
}

static int node_string_arg_nonempty(const tf_ir_node *node, const char *name) {
    cJSON *item = node && node->args ? cJSON_GetObjectItemCaseSensitive(node->args, name) : NULL;
    return cJSON_IsString(item) && item->valuestring && item->valuestring[0] != '\0';
}

static int node_string_arg_equals(const tf_ir_node *node, const char *name, const char *value) {
    cJSON *item = node && node->args ? cJSON_GetObjectItemCaseSensitive(node->args, name) : NULL;
    return cJSON_IsString(item) && item->valuestring && value &&
           strcmp(item->valuestring, value) == 0;
}

static int node_op_is_unique(const tf_ir_node *node) {
    return node && node->op &&
           (strcmp(node->op, "unique") == 0 || strcmp(node->op, "dedup") == 0);
}

static int node_op_is_group_agg(const tf_ir_node *node) {
    return node && node->op && strcmp(node->op, "group-agg") == 0;
}

static int node_array_arg_missing_or_empty(const tf_ir_node *node, const char *name) {
    cJSON *item = node && node->args ? cJSON_GetObjectItemCaseSensitive(node->args, name) : NULL;
    if (!item) return 1;
    if (!cJSON_IsArray(item)) return 0;
    return cJSON_GetArraySize(item) <= 0;
}

static int node_number_arg_positive(const tf_ir_node *node, const char *name) {
    cJSON *item = node && node->args ? cJSON_GetObjectItemCaseSensitive(node->args, name) : NULL;
    return cJSON_IsNumber(item) && item->valuedouble > 0.0;
}

static int node_op_is_row_set_spillable(const tf_ir_node *node) {
    return node && node->op &&
           (strcmp(node->op, "intersect") == 0 || strcmp(node->op, "setdiff") == 0 ||
            strcmp(node->op, "intersect-all") == 0 || strcmp(node->op, "setdiff-all") == 0 ||
            strcmp(node->op, "union") == 0);
}

static int node_op_is_sorted_set_bounded(const tf_ir_node *node) {
    return node && node->op && node_bool_arg_true(node, "sorted") &&
           (strcmp(node->op, "intersect") == 0 || strcmp(node->op, "setdiff") == 0 ||
            strcmp(node->op, "intersect-all") == 0 || strcmp(node->op, "setdiff-all") == 0 ||
            strcmp(node->op, "union") == 0);
}

static int node_op_is_filtering_join(const tf_ir_node *node) {
    if (!node || !node->op) return 0;
    if (strcmp(node->op, "semi-join") == 0 || strcmp(node->op, "anti-join") == 0) return 1;
    if (strcmp(node->op, "join") != 0 || !node->args) return 0;
    cJSON *how = cJSON_GetObjectItemCaseSensitive(node->args, "how");
    return cJSON_IsString(how) && how->valuestring &&
           (strcmp(how->valuestring, "semi") == 0 || strcmp(how->valuestring, "anti") == 0);
}

static int node_op_is_join_spillable(const tf_ir_node *node) {
    if (!node || !node->op) return 0;
    if (strcmp(node->op, "semi-join") == 0 || strcmp(node->op, "anti-join") == 0) return 1;
    if (strcmp(node->op, "join") != 0) return 0;
    if (!node->args) return 1;
    cJSON *how = cJSON_GetObjectItemCaseSensitive(node->args, "how");
    if (!cJSON_IsString(how) || !how->valuestring) return 1;
    return strcmp(how->valuestring, "inner") == 0 || strcmp(how->valuestring, "left") == 0 ||
           strcmp(how->valuestring, "semi") == 0 || strcmp(how->valuestring, "anti") == 0;
}

static void apply_dynamic_contract(tf_ir_node *node) {
    if (!node || !node->op) return;
    if (strcmp(node->op, "validate") == 0 &&
        (node_string_arg_nonempty(node, "rules_file") || node_string_arg_nonempty(node, "rulesFile"))) {
        node->caps |= TF_CAP_FS;
        node->caps &= ~TF_CAP_BROWSER_SAFE;
    }
    if (strcmp(node->op, "pivot") == 0) {
        if (node_string_arg_nonempty(node, "spill_dir")) {
            node->caps |= TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_FS | TF_CAP_DETERMINISTIC;
            node->caps &= ~TF_CAP_BROWSER_SAFE;
            node->memory_class = TF_MEM_EXTERNAL;
            node->emit_class = TF_EMIT_ON_FLUSH;
            node->schema_class = node_array_arg_nonempty(node, "categories") ?
                                 TF_SCHEMA_PARAMETRIC : TF_SCHEMA_DATA_DEPENDENT;
            node->state_estimate = "O(spill_run_rows * columns + categories) RAM + O(input_rows + output_rows) spill";
        } else if (node_bool_arg_true(node, "sorted") && node_array_arg_nonempty(node, "categories")) {
            node->caps |= TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC;
            node->memory_class = TF_MEM_BOUNDED_STATE;
            node->emit_class = TF_EMIT_MIXED;
            node->schema_class = TF_SCHEMA_PARAMETRIC;
            node->state_estimate = "O(current_group + categories)";
        } else if (node_array_arg_nonempty(node, "categories")) {
            node->schema_class = TF_SCHEMA_PARAMETRIC;
        }
    }
    if (strcmp(node->op, "rowid") == 0 &&
        (node_array_arg_missing_or_empty(node, "columns") || node_bool_arg_true(node, "sorted"))) {
        node->caps |= TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC;
        node->memory_class = TF_MEM_BOUNDED_STATE;
        node->emit_class = TF_EMIT_PER_BATCH;
        node->schema_class = TF_SCHEMA_PARAMETRIC;
        node->state_estimate = node_array_arg_missing_or_empty(node, "columns") ?
            "O(1)" : "O(previous_key + counter)";
    }
    if (node_op_is_unique(node)) {
        if (node_string_arg_nonempty(node, "spill_dir")) {
            node->caps |= TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_FS | TF_CAP_DETERMINISTIC;
            node->caps &= ~TF_CAP_BROWSER_SAFE;
            node->memory_class = TF_MEM_EXTERNAL;
            node->emit_class = TF_EMIT_ON_FLUSH;
            node->schema_class = TF_SCHEMA_STABLE;
            node->state_estimate = "O(spill_run_rows * columns) RAM + O(input_rows) spill";
        } else if (node_string_arg_equals(node, "mode", "approx") || node_bool_arg_true(node, "approx")) {
            node->caps |= TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC;
            node->memory_class = TF_MEM_BOUNDED_STATE;
            node->emit_class = TF_EMIT_PER_BATCH;
            node->schema_class = TF_SCHEMA_STABLE;
            node->state_estimate = "O(bloom_bytes)";
        } else if (node_bool_arg_true(node, "sorted")) {
            node->caps |= TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC;
            node->memory_class = TF_MEM_BOUNDED_STATE;
            node->emit_class = TF_EMIT_PER_BATCH;
            node->schema_class = TF_SCHEMA_STABLE;
            node->state_estimate = "O(previous_key)";
        }
    }
    if (node_op_is_group_agg(node)) {
        if (node_string_arg_nonempty(node, "spill_dir")) {
            node->caps |= TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_FS | TF_CAP_DETERMINISTIC;
            node->caps &= ~TF_CAP_BROWSER_SAFE;
            node->memory_class = TF_MEM_EXTERNAL;
            node->emit_class = TF_EMIT_ON_FLUSH;
            node->schema_class = TF_SCHEMA_PARAMETRIC;
            node->state_estimate = "O(spill_run_rows * columns) RAM + O(input_rows) spill";
        } else if (node_bool_arg_true(node, "sorted")) {
            node->caps |= TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC;
            node->memory_class = TF_MEM_BOUNDED_STATE;
            node->emit_class = TF_EMIT_MIXED;
            node->schema_class = TF_SCHEMA_PARAMETRIC;
            node->state_estimate = "O(current_group + aggregate_accumulators)";
        }
    }
    if (node_op_is_sorted_set_bounded(node)) {
        node->caps |= TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_DETERMINISTIC;
        if (node->caps & (TF_CAP_FS | TF_CAP_NET)) node->caps &= ~TF_CAP_BROWSER_SAFE;
        else node->caps |= TF_CAP_BROWSER_SAFE;
        node->memory_class = TF_MEM_BOUNDED_STATE;
        node->emit_class = strcmp(node->op, "union") == 0 ? TF_EMIT_MIXED : TF_EMIT_PER_BATCH;
        node->schema_class = TF_SCHEMA_STABLE;
        node->state_estimate = strcmp(node->op, "union") == 0
            ? "O(previous_left_key + current_file_key)"
            : ((strcmp(node->op, "intersect-all") == 0 || strcmp(node->op, "setdiff-all") == 0)
                ? "O(current_lookup_run + current_left_run)"
                : "O(previous_left_key + current_lookup_key)");
    }
    if (node_op_is_join_spillable(node) && node_bool_arg_true(node, "sorted") &&
        !node_string_arg_nonempty(node, "spill_dir")) {
        int filtering = node_op_is_filtering_join(node);
        node->caps |= TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_DETERMINISTIC;
        if (node->caps & (TF_CAP_FS | TF_CAP_NET)) node->caps &= ~TF_CAP_BROWSER_SAFE;
        else node->caps |= TF_CAP_BROWSER_SAFE;
        node->memory_class = TF_MEM_BOUNDED_STATE;
        node->emit_class = TF_EMIT_PER_BATCH;
        node->schema_class = filtering ? TF_SCHEMA_STABLE : TF_SCHEMA_DATA_DEPENDENT;
        node->state_estimate = filtering
            ? "O(current_lookup_run)"
            : "O(current_lookup_run + max_matches_per_row)";
    }
    if (node_op_is_join_spillable(node) && node_string_arg_nonempty(node, "spill_dir")) {
        int filtering = node_op_is_filtering_join(node);
        node->caps |= TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_FS | TF_CAP_DETERMINISTIC;
        node->caps &= ~TF_CAP_BROWSER_SAFE;
        node->memory_class = TF_MEM_EXTERNAL;
        node->emit_class = TF_EMIT_ON_FLUSH;
        node->schema_class = filtering ? TF_SCHEMA_STABLE : TF_SCHEMA_DATA_DEPENDENT;
        node->state_estimate = filtering
            ? "O(spill_run_rows * columns) RAM + O(left_rows + lookup_keys) spill"
            : "O(spill_run_rows * columns + max_matches_per_row) RAM + O(left_rows + lookup_rows + output_rows) spill";
    }

    if (strcmp(node->op, "assert") == 0 && node_string_arg_nonempty(node, "aggregate")) {
        node->memory_class = TF_MEM_BOUNDED_STATE;
        node->emit_class = TF_EMIT_MIXED;
        node->schema_class = TF_SCHEMA_STABLE;
        node->state_estimate = "O(1) aggregate counters";
    }
    if (node_op_is_row_set_spillable(node) && node_string_arg_nonempty(node, "spill_dir")) {
        node->caps |= TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_FS | TF_CAP_DETERMINISTIC;
        node->caps &= ~TF_CAP_BROWSER_SAFE;
        node->memory_class = TF_MEM_EXTERNAL;
        node->emit_class = TF_EMIT_ON_FLUSH;
        node->schema_class = TF_SCHEMA_STABLE;
        node->state_estimate = strcmp(node->op, "union") == 0
            ? "O(spill_run_rows * columns) RAM + O(left_rows + file_rows) spill"
            : ((strcmp(node->op, "intersect-all") == 0 || strcmp(node->op, "setdiff-all") == 0)
                ? "O(spill_run_rows * columns) RAM + O(left_rows + lookup_rows) spill"
                : "O(spill_run_rows * columns) RAM + O(left_rows + lookup_keys) spill");
    }
}

static int tf_ir_validate_impl(tf_ir_plan *plan, const tf_host_policy *policy) {
    plan->validated = false;
    free(plan->error);
    plan->error = NULL;

    /* 1. At least one node */
    if (plan->n_nodes == 0) {
        set_plan_error(plan, "plan has no steps");
        return TF_ERROR;
    }

    bool has_decoder = false;
    bool has_encoder = false;

    for (size_t i = 0; i < plan->n_nodes; i++) {
        tf_ir_node *node = &plan->nodes[i];

        /* 4. Op must exist in registry */
        const tf_op_entry *entry = tf_op_registry_find(node->op);
        if (!entry) {
            set_plan_errorf(plan, "unknown op: '%s'", node->op);
            return TF_ERROR;
        }

        /* Populate execution contract metadata from registry. */
        node->caps = entry->caps;
        node->memory_class = entry->memory_class;
        node->emit_class = entry->emit_class;
        node->schema_class = entry->schema_class;
        node->state_estimate = entry->state_estimate ? entry->state_estimate : "unknown";
        apply_dynamic_contract(node);

        /* Check decoder/encoder placement */
        if (entry->kind == TF_OP_DECODER) {
            if (has_decoder) {
                set_plan_error(plan, "multiple decoders not supported");
                return TF_ERROR;
            }
            /* 2. Decoder must be the first node */
            if (i != 0) {
                set_plan_errorf(plan, "decoder '%s' must be the first step", node->op);
                return TF_ERROR;
            }
            has_decoder = true;
        } else if (entry->kind == TF_OP_ENCODER) {
            if (has_encoder) {
                set_plan_error(plan, "multiple encoders not supported");
                return TF_ERROR;
            }
            /* 3. Encoder must be the last node */
            if (i != plan->n_nodes - 1) {
                set_plan_errorf(plan, "encoder '%s' must be the last step", node->op);
                return TF_ERROR;
            }
            has_encoder = true;
        } else {
            /* Transform must not be first or last if we expect decoder/encoder */
        }

        /* 5. Required args present */
        for (size_t a = 0; a < entry->n_args; a++) {
            if (!entry->args[a].required) continue;
            cJSON *val = cJSON_GetObjectItemCaseSensitive(node->args,
                                                          entry->args[a].name);
            if (!val) {
                char buf[256];
                snprintf(buf, sizeof(buf), "op '%s' missing required arg '%s'",
                         node->op, entry->args[a].name);
                set_plan_error(plan, buf);
                return TF_ERROR;
            }
        }
        if (strcmp(node->op, "validate") == 0) {
            cJSON *expr = cJSON_GetObjectItemCaseSensitive(node->args, "expr");
            cJSON *rules = cJSON_GetObjectItemCaseSensitive(node->args, "rules");
            cJSON *rules_file = cJSON_GetObjectItemCaseSensitive(node->args, "rules_file");
            if (!rules_file) rules_file = cJSON_GetObjectItemCaseSensitive(node->args, "rulesFile");
            if (!expr && !rules && !rules_file) {
                set_plan_error(plan, "op 'validate' requires expr, rules, or rules_file");
                return TF_ERROR;
            }
        }
        if (strcmp(node->op, "assert") == 0) {
            cJSON *expr = cJSON_GetObjectItemCaseSensitive(node->args, "expr");
            cJSON *agg = cJSON_GetObjectItemCaseSensitive(node->args, "aggregate");
            if (!agg) agg = cJSON_GetObjectItemCaseSensitive(node->args, "agg");
            if (!expr && !agg) {
                set_plan_error(plan, "op 'assert' requires expr or aggregate");
                return TF_ERROR;
            }
        }
        if (node_op_is_join_spillable(node) && !node_op_is_filtering_join(node) &&
            (node_bool_arg_true(node, "sorted") || node_string_arg_nonempty(node, "spill_dir")) &&
            !node_number_arg_positive(node, "max_matches_per_row")) {
            set_plan_error(plan,
                           "op 'join' sorted=true or spill_dir for inner/left joins requires max_matches_per_row");
            return TF_ERROR;
        }
        if (enforce_host_policy(plan, node, policy) != TF_OK) {
            return TF_ERROR;
        }
    }

    if (!has_decoder) {
        set_plan_error(plan, "plan has no decoder (need a codec.*.decode step)");
        return TF_ERROR;
    }
    if (!has_encoder) {
        set_plan_error(plan, "plan has no encoder (need a codec.*.encode step)");
        return TF_ERROR;
    }

    /* Compute plan-level caps (intersection of all node caps) */
    plan->plan_caps = ~(uint32_t)0;
    for (size_t i = 0; i < plan->n_nodes; i++) {
        plan->plan_caps &= plan->nodes[i].caps;
    }

    plan->validated = true;
    return TF_OK;
}


int tf_ir_validate_with_host_policy(tf_ir_plan *plan, const tf_host_policy *policy) {
    if (!plan) return TF_ERROR;
    return tf_ir_validate_impl(plan, policy);
}

int tf_ir_validate(tf_ir_plan *plan) {
    return tf_ir_validate_with_host_policy(plan, NULL);
}
