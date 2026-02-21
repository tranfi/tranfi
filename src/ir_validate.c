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

        /* Populate caps from registry */
        node->caps = entry->caps;

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
