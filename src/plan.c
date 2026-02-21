/*
 * plan.c — Parse a JSON pipeline plan and instantiate decoder, steps, encoder.
 *
 * Plan format:
 * {
 *   "steps": [
 *     {"op": "codec.csv.decode", "args": {"delimiter": ","}},
 *     {"op": "filter", "args": {"expr": "col('age') > 25"}},
 *     {"op": "codec.csv.encode", "args": {}}
 *   ]
 * }
 */

#include "internal.h"
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

static void set_errorf(char **error, const char *fmt, const char *detail) {
    if (error) {
        char buf[256];
        snprintf(buf, sizeof(buf), fmt, detail);
        set_error(error, buf);
    }
}

int tf_plan_parse(const char *json, size_t len,
                  tf_decoder **out_decoder, tf_step ***out_steps, size_t *out_n_steps,
                  tf_encoder **out_encoder, char **error) {
    *out_decoder = NULL;
    *out_steps = NULL;
    *out_n_steps = 0;
    *out_encoder = NULL;

    /* Parse JSON */
    cJSON *root = cJSON_ParseWithLength(json, len);
    if (!root) {
        set_error(error, "invalid JSON in plan");
        return TF_ERROR;
    }

    cJSON *steps_arr = cJSON_GetObjectItemCaseSensitive(root, "steps");
    if (!cJSON_IsArray(steps_arr)) {
        set_error(error, "plan must have a 'steps' array");
        cJSON_Delete(root);
        return TF_ERROR;
    }

    int n_json_steps = cJSON_GetArraySize(steps_arr);
    if (n_json_steps == 0) {
        set_error(error, "plan has no steps");
        cJSON_Delete(root);
        return TF_ERROR;
    }

    /* Temporary arrays for transforms (max = n_json_steps) */
    tf_step **steps = calloc(n_json_steps, sizeof(tf_step *));
    size_t n_steps = 0;
    tf_decoder *decoder = NULL;
    tf_encoder *encoder = NULL;
    int err = TF_OK;

    for (int i = 0; i < n_json_steps; i++) {
        cJSON *step_json = cJSON_GetArrayItem(steps_arr, i);
        cJSON *op_json = cJSON_GetObjectItemCaseSensitive(step_json, "op");
        cJSON *args_json = cJSON_GetObjectItemCaseSensitive(step_json, "args");

        if (!cJSON_IsString(op_json)) {
            set_errorf(error, "step %d missing 'op' string", "");
            err = TF_ERROR;
            break;
        }
        const char *op = op_json->valuestring;

        /* Match op to constructor */
        if (strcmp(op, "codec.csv.decode") == 0) {
            if (decoder) {
                set_error(error, "multiple decoders not supported");
                err = TF_ERROR;
                break;
            }
            decoder = tf_csv_decoder_create(args_json);
            if (!decoder) {
                set_error(error, "failed to create CSV decoder");
                err = TF_ERROR;
                break;
            }
        } else if (strcmp(op, "codec.csv.encode") == 0) {
            if (encoder) {
                set_error(error, "multiple encoders not supported");
                err = TF_ERROR;
                break;
            }
            encoder = tf_csv_encoder_create(args_json);
            if (!encoder) {
                set_error(error, "failed to create CSV encoder");
                err = TF_ERROR;
                break;
            }
        } else if (strcmp(op, "codec.jsonl.decode") == 0) {
            if (decoder) {
                set_error(error, "multiple decoders not supported");
                err = TF_ERROR;
                break;
            }
            decoder = tf_jsonl_decoder_create(args_json);
            if (!decoder) {
                set_error(error, "failed to create JSONL decoder");
                err = TF_ERROR;
                break;
            }
        } else if (strcmp(op, "codec.jsonl.encode") == 0) {
            if (encoder) {
                set_error(error, "multiple encoders not supported");
                err = TF_ERROR;
                break;
            }
            encoder = tf_jsonl_encoder_create(args_json);
            if (!encoder) {
                set_error(error, "failed to create JSONL encoder");
                err = TF_ERROR;
                break;
            }
        } else if (strcmp(op, "filter") == 0) {
            tf_step *s = tf_filter_create(args_json);
            if (!s) {
                set_error(error, "failed to create filter step");
                err = TF_ERROR;
                break;
            }
            steps[n_steps++] = s;
        } else if (strcmp(op, "select") == 0) {
            tf_step *s = tf_select_create(args_json);
            if (!s) {
                set_error(error, "failed to create select step");
                err = TF_ERROR;
                break;
            }
            steps[n_steps++] = s;
        } else if (strcmp(op, "rename") == 0) {
            tf_step *s = tf_rename_create(args_json);
            if (!s) {
                set_error(error, "failed to create rename step");
                err = TF_ERROR;
                break;
            }
            steps[n_steps++] = s;
        } else if (strcmp(op, "head") == 0) {
            tf_step *s = tf_head_create(args_json);
            if (!s) {
                set_error(error, "failed to create head step");
                err = TF_ERROR;
                break;
            }
            steps[n_steps++] = s;
        } else {
            set_errorf(error, "unknown op: '%s'", op);
            err = TF_ERROR;
            break;
        }
    }

    cJSON_Delete(root);

    if (err != TF_OK) {
        /* Cleanup on error */
        if (decoder) decoder->destroy(decoder);
        if (encoder) encoder->destroy(encoder);
        for (size_t i = 0; i < n_steps; i++) {
            if (steps[i]) steps[i]->destroy(steps[i]);
        }
        free(steps);
        return TF_ERROR;
    }

    if (!decoder) {
        set_error(error, "plan has no decoder (need a codec.*.decode step)");
        if (encoder) encoder->destroy(encoder);
        for (size_t i = 0; i < n_steps; i++) steps[i]->destroy(steps[i]);
        free(steps);
        return TF_ERROR;
    }
    if (!encoder) {
        set_error(error, "plan has no encoder (need a codec.*.encode step)");
        decoder->destroy(decoder);
        for (size_t i = 0; i < n_steps; i++) steps[i]->destroy(steps[i]);
        free(steps);
        return TF_ERROR;
    }

    *out_decoder = decoder;
    *out_steps = steps;
    *out_n_steps = n_steps;
    *out_encoder = encoder;
    return TF_OK;
}
