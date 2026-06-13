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

static int node_string_arg_nonempty(const tf_ir_node *node, const char *name) {
    cJSON *item = node && node->args ? cJSON_GetObjectItemCaseSensitive(node->args, name) : NULL;
    return cJSON_IsString(item) && item->valuestring && item->valuestring[0] != '\0';
}

static int node_op_is_unique(const tf_ir_node *node) {
    return node && node->op &&
           (strcmp(node->op, "unique") == 0 || strcmp(node->op, "dedup") == 0);
}

static int node_op_is_group_agg(const tf_ir_node *node) {
    return node && node->op && strcmp(node->op, "group-agg") == 0;
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
            node->memory_class = TF_MEM_BOUNDED_STATE;
            node->emit_class = TF_EMIT_MIXED;
            node->schema_class = TF_SCHEMA_PARAMETRIC;
            node->state_estimate = "O(current_group + categories)";
        }
    }
    if (node_op_is_unique(node)) {
        if (node_string_arg_nonempty(node, "spill_dir")) {
            node->caps |= TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_FS | TF_CAP_DETERMINISTIC;
            node->caps &= ~TF_CAP_BROWSER_SAFE;
            node->memory_class = TF_MEM_EXTERNAL;
            node->emit_class = TF_EMIT_ON_FLUSH;
            node->schema_class = TF_SCHEMA_STABLE;
            node->state_estimate = "O(spill_run_rows * columns) RAM + O(input_rows) spill";
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
        node->caps |= TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC;
        node->memory_class = TF_MEM_BOUNDED_STATE;
        node->emit_class = strcmp(node->op, "union") == 0 ? TF_EMIT_MIXED : TF_EMIT_PER_BATCH;
        node->schema_class = TF_SCHEMA_STABLE;
        node->state_estimate = strcmp(node->op, "union") == 0
            ? "O(previous_left_key + current_file_key)"
            : ((strcmp(node->op, "intersect-all") == 0 || strcmp(node->op, "setdiff-all") == 0)
                ? "O(current_lookup_run + current_left_run)"
                : "O(previous_left_key + current_lookup_key)");
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

int tf_ir_validate(tf_ir_plan *plan) {
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
