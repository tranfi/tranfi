/*
 * ir_schema.c — Forward schema inference pass.
 *
 * Walks nodes in execution order, calling each op's infer_schema callback
 * to propagate schema information forward through the plan.
 * Best-effort and non-fatal — unknown schemas propagate as known=false.
 */

#include "ir.h"
#include <stdlib.h>

#define TF_OK    0
#define TF_ERROR (-1)

int tf_ir_infer_schema(tf_ir_plan *plan) {
    if (plan->n_nodes == 0) return TF_OK;

    /* Start with an empty, unknown input schema for the first node (decoder) */
    tf_schema current = {0};
    current.known = false;

    for (size_t i = 0; i < plan->n_nodes; i++) {
        tf_ir_node *node = &plan->nodes[i];
        const tf_op_entry *entry = tf_op_registry_find(node->op);

        /* Copy current schema as this node's input */
        tf_schema_free(&node->input_schema);
        tf_schema_copy(&node->input_schema, &current);

        if (entry && entry->infer_schema) {
            /* Clear old output schema */
            tf_schema_free(&node->output_schema);
            entry->infer_schema(node, &current, &node->output_schema);

            /* Advance current to this node's output */
            tf_schema_free(&current);
            tf_schema_copy(&current, &node->output_schema);
        } else {
            /* No schema inference available — propagate unknown */
            tf_schema_free(&node->output_schema);
            node->output_schema.known = false;
            tf_schema_free(&current);
            current.known = false;
        }
    }

    /* Store final schema (schema before encoder, i.e., last transform's output) */
    tf_schema_free(&plan->final_schema);
    if (plan->n_nodes >= 2) {
        /* The schema before the encoder is the second-to-last node's output */
        tf_schema_copy(&plan->final_schema,
                       &plan->nodes[plan->n_nodes - 2].output_schema);
    } else {
        plan->final_schema.known = false;
    }

    tf_schema_free(&current);
    plan->schema_inferred = true;
    return TF_OK;
}
