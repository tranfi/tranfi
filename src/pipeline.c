/*
 * pipeline.c — Pipeline orchestrator.
 *
 * Creates a pipeline from a JSON plan, streams bytes through
 * decode → steps → encode, and routes output to channels.
 */

#include "internal.h"
#include "dsl.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#define TRANFI_VERSION "0.1.0"

static char *g_last_error = NULL;

void tf_set_last_error(const char *msg) {
    free(g_last_error);
    g_last_error = msg ? strdup(msg) : NULL;
}

const char *tf_last_error(void) {
    return g_last_error;
}

const char *tf_version(void) {
    return TRANFI_VERSION;
}

static tf_pipeline *assemble_pipeline(tf_decoder *decoder, tf_step **steps,
                                      size_t n_steps, tf_encoder *encoder) {
    tf_pipeline *p = calloc(1, sizeof(tf_pipeline));
    if (!p) {
        decoder->destroy(decoder);
        encoder->destroy(encoder);
        for (size_t i = 0; i < n_steps; i++) steps[i]->destroy(steps[i]);
        free(steps);
        tf_set_last_error("out of memory");
        return NULL;
    }

    p->decoder = decoder;
    p->steps = steps;
    p->n_steps = n_steps;
    p->encoder = encoder;

    for (int i = 0; i < TF_NUM_CHANNELS; i++) {
        tf_buffer_init(&p->output[i]);
    }

    /* Wire up side channels */
    p->side.errors = &p->output[TF_CHAN_ERRORS];
    p->side.stats = &p->output[TF_CHAN_STATS];
    p->side.samples = &p->output[TF_CHAN_SAMPLES];

    return p;
}

tf_pipeline *tf_pipeline_create(const char *plan_json, size_t len) {
    if (!plan_json || len == 0) {
        tf_set_last_error("empty plan");
        return NULL;
    }

    char *error = NULL;

    /* 1. Parse JSON → IR */
    tf_ir_plan *ir = tf_ir_from_json(plan_json, len, &error);
    if (!ir) {
        tf_set_last_error(error ? error : "failed to parse plan");
        free(error);
        return NULL;
    }

    /* 2. Validate */
    if (tf_ir_validate(ir) != TF_OK) {
        tf_set_last_error(ir->error ? ir->error : "validation failed");
        tf_ir_plan_free(ir);
        return NULL;
    }

    /* 3. Schema inference (best-effort, non-fatal) */
    tf_ir_infer_schema(ir);

    /* 4. Compile to native target */
    tf_decoder *decoder = NULL;
    tf_step **steps = NULL;
    size_t n_steps = 0;
    tf_encoder *encoder = NULL;
    if (tf_compile_native(ir, &decoder, &steps, &n_steps, &encoder, &error) != TF_OK) {
        tf_set_last_error(error ? error : "compilation failed");
        free(error);
        tf_ir_plan_free(ir);
        return NULL;
    }

    tf_ir_plan_free(ir);

    /* 5. Assemble pipeline */
    return assemble_pipeline(decoder, steps, n_steps, encoder);
}

tf_pipeline *tf_pipeline_create_from_ir(const tf_ir_plan *plan) {
    if (!plan) {
        tf_set_last_error("NULL IR plan");
        return NULL;
    }

    char *error = NULL;
    tf_decoder *decoder = NULL;
    tf_step **steps = NULL;
    size_t n_steps = 0;
    tf_encoder *encoder = NULL;

    if (tf_compile_native(plan, &decoder, &steps, &n_steps, &encoder, &error) != TF_OK) {
        tf_set_last_error(error ? error : "compilation failed");
        free(error);
        return NULL;
    }

    return assemble_pipeline(decoder, steps, n_steps, encoder);
}

/* Public IR wrappers (thin forwarding to ir.h functions) */
tf_ir_plan *tf_ir_plan_from_json(const char *json, size_t len, char **error) {
    return tf_ir_from_json(json, len, error);
}
char *tf_ir_plan_to_json(const tf_ir_plan *plan) {
    return tf_ir_to_json(plan);
}
int tf_ir_plan_validate(tf_ir_plan *plan) {
    return tf_ir_validate(plan);
}
int tf_ir_plan_infer_schema(tf_ir_plan *plan) {
    return tf_ir_infer_schema(plan);
}
void tf_ir_plan_destroy(tf_ir_plan *plan) {
    tf_ir_plan_free(plan);
}

/*
 * Process a batch through all steps, then encode.
 */
static int process_batch(tf_pipeline *p, tf_batch *batch) {
    tf_batch *current = batch;
    int batch_owned = 0; /* 0 = still owned by caller (decoder) */

    p->rows_in += current->n_rows;

    for (size_t i = 0; i < p->n_steps; i++) {
        tf_batch *next = NULL;
        int rc = p->steps[i]->process(p->steps[i], current, &next, &p->side);

        if (batch_owned) tf_batch_free(current);
        batch_owned = 1;

        if (rc != TF_OK) return TF_ERROR;
        if (!next) return TF_OK; /* filtered away entirely */
        current = next;
    }

    /* Encode */
    if (current->n_rows > 0) {
        p->rows_out += current->n_rows;
        int rc = p->encoder->encode(p->encoder, current, &p->output[TF_CHAN_MAIN]);
        if (batch_owned) tf_batch_free(current);
        return rc;
    }

    if (batch_owned) tf_batch_free(current);
    return TF_OK;
}

int tf_pipeline_push(tf_pipeline *p, const uint8_t *data, size_t len) {
    if (!p || p->finished) return TF_ERROR;

    p->bytes_in += len;

    /* Decode bytes into batches */
    tf_batch **batches = NULL;
    size_t n_batches = 0;
    int rc = p->decoder->decode(p->decoder, data, len, &batches, &n_batches);
    if (rc != TF_OK) {
        free(p->error);
        p->error = strdup("decode error");
        return TF_ERROR;
    }

    /* Process each batch */
    for (size_t i = 0; i < n_batches; i++) {
        rc = process_batch(p, batches[i]);
        tf_batch_free(batches[i]);
        if (rc != TF_OK) {
            for (size_t j = i + 1; j < n_batches; j++) tf_batch_free(batches[j]);
            free(batches);
            free(p->error);
            p->error = strdup("processing error");
            return TF_ERROR;
        }
    }
    free(batches);

    return TF_OK;
}

int tf_pipeline_finish(tf_pipeline *p) {
    if (!p || p->finished) return TF_ERROR;
    p->finished = 1;

    /* Flush decoder */
    tf_batch **batches = NULL;
    size_t n_batches = 0;
    int rc = p->decoder->flush(p->decoder, &batches, &n_batches);
    if (rc == TF_OK) {
        for (size_t i = 0; i < n_batches; i++) {
            process_batch(p, batches[i]);
            tf_batch_free(batches[i]);
        }
        free(batches);
    }

    /* Flush steps */
    for (size_t i = 0; i < p->n_steps; i++) {
        tf_batch *flushed = NULL;
        rc = p->steps[i]->flush(p->steps[i], &flushed, &p->side);
        if (rc == TF_OK && flushed) {
            /* Run flushed batch through remaining steps */
            tf_batch *current = flushed;
            int owned = 1;
            for (size_t j = i + 1; j < p->n_steps; j++) {
                tf_batch *next = NULL;
                p->steps[j]->process(p->steps[j], current, &next, &p->side);
                if (owned) tf_batch_free(current);
                owned = 1;
                if (!next) { current = NULL; break; }
                current = next;
            }
            if (current && current->n_rows > 0) {
                p->rows_out += current->n_rows;
                p->encoder->encode(p->encoder, current, &p->output[TF_CHAN_MAIN]);
            }
            if (current && owned) tf_batch_free(current);
        }
    }

    /* Flush encoder */
    p->encoder->flush(p->encoder, &p->output[TF_CHAN_MAIN]);

    /* Emit final stats */
    p->bytes_out = tf_buffer_readable(&p->output[TF_CHAN_MAIN]);
    char stats_buf[256];
    snprintf(stats_buf, sizeof(stats_buf),
             "{\"rows_in\":%zu,\"rows_out\":%zu,\"bytes_in\":%zu,\"bytes_out\":%zu}\n",
             p->rows_in, p->rows_out, p->bytes_in, p->bytes_out);
    tf_buffer_write_str(&p->output[TF_CHAN_STATS], stats_buf);

    return TF_OK;
}

size_t tf_pipeline_pull(tf_pipeline *p, int channel, uint8_t *buf, size_t buf_len) {
    if (!p || channel < 0 || channel >= TF_NUM_CHANNELS) return 0;
    return tf_buffer_read(&p->output[channel], buf, buf_len);
}

const char *tf_pipeline_error(tf_pipeline *p) {
    return p ? p->error : NULL;
}

void tf_pipeline_free(tf_pipeline *p) {
    if (!p) return;
    if (p->decoder) p->decoder->destroy(p->decoder);
    if (p->encoder) p->encoder->destroy(p->encoder);
    for (size_t i = 0; i < p->n_steps; i++) {
        if (p->steps[i]) p->steps[i]->destroy(p->steps[i]);
    }
    free(p->steps);
    for (int i = 0; i < TF_NUM_CHANNELS; i++) {
        tf_buffer_free(&p->output[i]);
    }
    free(p->error);
    free(p);
}

char *tf_compile_to_sql(const char *dsl, size_t len, char **error) {
    if (error) *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, len, error);
    if (!plan) return NULL;
    if (tf_ir_validate(plan) != TF_OK) {
        if (error) { free(*error); *error = strdup(plan->error ? plan->error : "validation failed"); }
        tf_ir_plan_destroy(plan);
        return NULL;
    }
    tf_ir_infer_schema(plan);
    char *sql = tf_ir_to_sql(plan, error);
    tf_ir_plan_destroy(plan);
    return sql;
}

char *tf_ir_plan_to_sql(const tf_ir_plan *plan, char **error) {
    return tf_ir_to_sql(plan, error);
}

char *tf_compile_dsl(const char *dsl, size_t len, char **error) {
    if (error) *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, len, error);
    if (!plan) return NULL;
    char *json = tf_ir_plan_to_json(plan);
    tf_ir_plan_destroy(plan);
    return json;
}

void tf_string_free(char *s) {
    free(s);
}
