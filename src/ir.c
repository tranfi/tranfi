/*
 * ir.c — IR plan construction, cloning, and memory management.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>

/* ---- Schema helpers ---- */

void tf_schema_free(tf_schema *s) {
    if (!s) return;
    if (s->col_names) {
        for (size_t i = 0; i < s->n_cols; i++) {
            free(s->col_names[i]);
        }
    }
    free(s->col_names);
    free(s->col_types);
    s->col_names = NULL;
    s->col_types = NULL;
    s->n_cols = 0;
    s->known = false;
}

void tf_schema_copy(tf_schema *dst, const tf_schema *src) {
    if (!dst) return;
    dst->known = false;
    dst->n_cols = 0;
    dst->col_names = NULL;
    dst->col_types = NULL;
    if (!src) return;

    dst->known = src->known;
    dst->n_cols = src->n_cols;
    if (src->n_cols == 0 || !src->known) return;

    dst->col_names = calloc(src->n_cols, sizeof(char *));
    dst->col_types = calloc(src->n_cols, sizeof(tf_type));
    if (!dst->col_names || !dst->col_types) {
        tf_schema_free(dst);
        return;
    }
    for (size_t i = 0; i < src->n_cols; i++) {
        dst->col_names[i] = strdup(src->col_names[i] ? src->col_names[i] : "");
        if (!dst->col_names[i]) {
            tf_schema_free(dst);
            return;
        }
        dst->col_types[i] = src->col_types[i];
    }
    dst->known = true;
}

/* ---- IR node helpers ---- */

static void ir_node_clear(tf_ir_node *node) {
    free((char *)node->op);
    if (node->args) cJSON_Delete(node->args);
    tf_schema_free(&node->input_schema);
    tf_schema_free(&node->output_schema);
}

/* ---- IR plan API ---- */

tf_ir_plan *tf_ir_plan_create(void) {
    tf_ir_plan *plan = calloc(1, sizeof(tf_ir_plan));
    if (!plan) return NULL;
    plan->capacity = 8;
    plan->nodes = calloc(plan->capacity, sizeof(tf_ir_node));
    if (!plan->nodes) { free(plan); return NULL; }
    return plan;
}

int tf_ir_plan_add_node(tf_ir_plan *plan, const char *op, cJSON *args) {
    if (!plan || !op) return -1;
    if (plan->n_nodes >= plan->capacity) {
        size_t new_cap = 0;
        if (tf_size_mul(plan->capacity, 2, &new_cap) != TF_OK) return -1;
        tf_ir_node *new_nodes = tf_reallocarray_checked(plan->nodes, new_cap,
                                                        sizeof(tf_ir_node));
        if (!new_nodes) return -1;
        memset(new_nodes + plan->capacity, 0,
               (new_cap - plan->capacity) * sizeof(tf_ir_node));
        plan->nodes = new_nodes;
        plan->capacity = new_cap;
    }

    char *op_copy = strdup(op);
    cJSON *args_copy = args ? cJSON_Duplicate(args, 1) : cJSON_CreateObject();
    if (!op_copy || !args_copy) {
        free(op_copy);
        if (args_copy) cJSON_Delete(args_copy);
        return -1;
    }

    tf_ir_node *node = &plan->nodes[plan->n_nodes];
    memset(node, 0, sizeof(tf_ir_node));
    node->op = op_copy;
    node->args = args_copy;
    node->index = plan->n_nodes;

    plan->n_nodes++;
    plan->validated = false;
    plan->schema_inferred = false;
    return 0;
}

tf_ir_plan *tf_ir_plan_clone(const tf_ir_plan *plan) {
    tf_ir_plan *clone = tf_ir_plan_create();
    if (!clone) return NULL;

    for (size_t i = 0; i < plan->n_nodes; i++) {
        const tf_ir_node *src = &plan->nodes[i];
        if (tf_ir_plan_add_node(clone, src->op, src->args) != 0) {
            tf_ir_plan_free(clone);
            return NULL;
        }
        /* Copy schemas if they were inferred */
        tf_ir_node *dst = &clone->nodes[i];
        tf_schema_copy(&dst->input_schema, &src->input_schema);
        tf_schema_copy(&dst->output_schema, &src->output_schema);
        dst->caps = src->caps;
        dst->memory_class = src->memory_class;
        dst->emit_class = src->emit_class;
        dst->schema_class = src->schema_class;
        dst->state_estimate = src->state_estimate;
    }

    tf_schema_copy(&clone->final_schema, &plan->final_schema);
    clone->plan_caps = plan->plan_caps;
    clone->validated = plan->validated;
    clone->schema_inferred = plan->schema_inferred;
    if (plan->error) clone->error = strdup(plan->error);

    return clone;
}

void tf_ir_plan_free(tf_ir_plan *plan) {
    if (!plan) return;
    for (size_t i = 0; i < plan->n_nodes; i++) {
        ir_node_clear(&plan->nodes[i]);
    }
    free(plan->nodes);
    tf_schema_free(&plan->final_schema);
    free(plan->error);
    free(plan);
}
