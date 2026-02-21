/*
 * ir_serialize.c — IR plan ↔ JSON serialization.
 *
 * JSON format (the .tfp format):
 * {
 *   "steps": [
 *     {"op": "codec.csv.decode", "args": {"delimiter": ","}},
 *     {"op": "filter", "args": {"expr": "col('age') > 25"}},
 *     {"op": "codec.csv.encode", "args": {}}
 *   ]
 * }
 */

#include "ir.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

static void set_error(char **error, const char *msg) {
    if (error) {
        free(*error);
        size_t len = strlen(msg) + 1;
        *error = malloc(len);
        if (*error) memcpy(*error, msg, len);
    }
}

tf_ir_plan *tf_ir_from_json(const char *json, size_t len, char **error) {
    if (error) *error = NULL;

    cJSON *root = cJSON_ParseWithLength(json, len);
    if (!root) {
        set_error(error, "invalid JSON in plan");
        return NULL;
    }

    cJSON *steps_arr = cJSON_GetObjectItemCaseSensitive(root, "steps");
    if (!cJSON_IsArray(steps_arr)) {
        set_error(error, "plan must have a 'steps' array");
        cJSON_Delete(root);
        return NULL;
    }

    int n = cJSON_GetArraySize(steps_arr);
    if (n == 0) {
        set_error(error, "plan has no steps");
        cJSON_Delete(root);
        return NULL;
    }

    tf_ir_plan *plan = tf_ir_plan_create();
    if (!plan) {
        set_error(error, "out of memory");
        cJSON_Delete(root);
        return NULL;
    }

    for (int i = 0; i < n; i++) {
        cJSON *step = cJSON_GetArrayItem(steps_arr, i);
        cJSON *op = cJSON_GetObjectItemCaseSensitive(step, "op");
        cJSON *args = cJSON_GetObjectItemCaseSensitive(step, "args");

        if (!cJSON_IsString(op)) {
            char buf[64];
            snprintf(buf, sizeof(buf), "step %d missing 'op' string", i);
            set_error(error, buf);
            tf_ir_plan_free(plan);
            cJSON_Delete(root);
            return NULL;
        }

        if (tf_ir_plan_add_node(plan, op->valuestring, args) != 0) {
            set_error(error, "out of memory adding node");
            tf_ir_plan_free(plan);
            cJSON_Delete(root);
            return NULL;
        }
    }

    cJSON_Delete(root);
    return plan;
}

char *tf_ir_to_json(const tf_ir_plan *plan) {
    cJSON *root = cJSON_CreateObject();
    cJSON *steps = cJSON_AddArrayToObject(root, "steps");

    for (size_t i = 0; i < plan->n_nodes; i++) {
        const tf_ir_node *node = &plan->nodes[i];
        cJSON *step = cJSON_CreateObject();
        cJSON_AddStringToObject(step, "op", node->op);
        if (node->args) {
            cJSON_AddItemToObject(step, "args", cJSON_Duplicate(node->args, 1));
        } else {
            cJSON_AddItemToObject(step, "args", cJSON_CreateObject());
        }
        cJSON_AddItemToArray(steps, step);
    }

    char *out = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    return out;
}
