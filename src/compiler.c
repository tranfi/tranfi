/*
 * compiler.c — L2 IR → L1 native target compilation.
 *
 * Iterates IR nodes, looks up each op in the registry,
 * and calls create_native() to build live decoder/steps/encoder structs.
 * This is the single native construction path for JSON IR plans.
 */

#include "ir.h"
#include "internal.h"
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

int tf_compile_native(const tf_ir_plan *plan,
                      tf_decoder **out_decoder, tf_step ***out_steps,
                      size_t *out_n_steps, tf_encoder **out_encoder,
                      char **error) {
    *out_decoder = NULL;
    *out_steps = NULL;
    *out_n_steps = 0;
    *out_encoder = NULL;
    if (error) *error = NULL;

    if (!plan || plan->n_nodes == 0) {
        set_error(error, "empty plan");
        return TF_ERROR;
    }

    /* Allocate steps array (max = n_nodes, since some are decoder/encoder) */
    tf_step **steps = calloc(plan->n_nodes, sizeof(tf_step *));
    if (!steps) {
        set_error(error, "out of memory");
        return TF_ERROR;
    }
    size_t n_steps = 0;
    tf_decoder *decoder = NULL;
    tf_encoder *encoder = NULL;

    for (size_t i = 0; i < plan->n_nodes; i++) {
        const tf_ir_node *node = &plan->nodes[i];
        const tf_op_entry *entry = tf_op_registry_find(node->op);

        if (!entry) {
            char buf[256];
            snprintf(buf, sizeof(buf), "unknown op: '%s'", node->op);
            set_error(error, buf);
            goto fail;
        }

        if (!entry->create_native) {
            if (entry->tier == TF_TIER_ECOSYSTEM) {
                /* Ecosystem stubs are skipped — host must handle */
                char buf[256];
                snprintf(buf, sizeof(buf), "op '%s' has no native target", node->op);
                set_error(error, buf);
                goto fail;
            }
            /* Core ops with NULL constructor are no-ops (e.g. flatten) */
            continue;
        }

        void *obj = entry->create_native(node->args);
        if (!obj) {
            const char *detail = tf_last_error();
            char buf[512];
            if (detail && detail[0])
                snprintf(buf, sizeof(buf), "failed to create '%s': %s", node->op, detail);
            else
                snprintf(buf, sizeof(buf), "failed to create '%s'", node->op);
            set_error(error, buf);
            goto fail;
        }

        switch (entry->kind) {
        case TF_OP_DECODER:
            if (decoder) {
                ((tf_decoder *)obj)->destroy((tf_decoder *)obj);
                set_error(error, "multiple decoders");
                goto fail;
            }
            decoder = (tf_decoder *)obj;
            break;
        case TF_OP_ENCODER:
            if (encoder) {
                ((tf_encoder *)obj)->destroy((tf_encoder *)obj);
                set_error(error, "multiple encoders");
                goto fail;
            }
            encoder = (tf_encoder *)obj;
            break;
        case TF_OP_TRANSFORM:
            steps[n_steps++] = (tf_step *)obj;
            break;
        }
    }

    if (!decoder) {
        set_error(error, "plan missing decoder");
        goto fail;
    }
    if (!encoder) {
        set_error(error, "plan missing encoder");
        goto fail;
    }

    *out_decoder = decoder;
    *out_steps = steps;
    *out_n_steps = n_steps;
    *out_encoder = encoder;
    return TF_OK;

fail:
    if (decoder) decoder->destroy(decoder);
    if (encoder) encoder->destroy(encoder);
    for (size_t i = 0; i < n_steps; i++) {
        if (steps[i]) steps[i]->destroy(steps[i]);
    }
    free(steps);
    return TF_ERROR;
}
