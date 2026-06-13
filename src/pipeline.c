/*
 * pipeline.c — Pipeline orchestrator.
 *
 * Creates a pipeline from a JSON plan, streams bytes through
 * decode → steps → encode, and routes output to channels.
 */

#include "internal.h"
#include "dsl.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#ifndef _WIN32
#include <unistd.h>
#endif

#define TRANFI_VERSION "0.1.2"

enum {
    TF_FINISH_PHASE_DECODER = 0,
    TF_FINISH_PHASE_STEPS = 1,
    TF_FINISH_PHASE_ENCODER = 2,
    TF_FINISH_PHASE_STATS = 3,
    TF_FINISH_PHASE_DONE = 4
};

static char *g_last_error = NULL;

void tf_set_last_error(const char *msg) {
    free(g_last_error);
    g_last_error = msg ? strdup(msg) : NULL;
}

const char *tf_last_error(void) {
    return g_last_error;
}

static void pipeline_set_error(tf_pipeline *p, const char *fallback) {
    if (!p) return;
    const char *last = tf_last_error();
    free(p->error);
    p->error = strdup((last && *last) ? last : fallback);
}

static int pipeline_fail(tf_pipeline *p, const char *fallback) {
    pipeline_set_error(p, fallback);
    return TF_ERROR;
}

static int pipeline_fail_msg(tf_pipeline *p, const char *msg) {
    if (p) {
        free(p->error);
        p->error = strdup(msg ? msg : "pipeline error");
    }
    return TF_ERROR;
}

static void pipeline_clear_finish_batches(tf_pipeline *p) {
    if (!p || !p->finish_batches) return;
    for (size_t i = p->finish_batch_index; i < p->finish_n_batches; i++) {
        if (p->finish_batches[i]) tf_batch_free(p->finish_batches[i]);
    }
    free(p->finish_batches);
    p->finish_batches = NULL;
    p->finish_n_batches = 0;
    p->finish_batch_index = 0;
}

static int pipeline_drain_channel_to_sink(tf_pipeline *p, int channel,
                                          tf_pipeline_sink_fn sink, void *user) {
    if (!p || channel < 0 || channel >= TF_NUM_CHANNELS || !sink) return TF_ERROR;
    uint8_t buf[64 * 1024];
    for (;;) {
        size_t n = tf_buffer_read(&p->output[channel], buf, sizeof(buf));
        if (n == 0) break;
        if (sink(channel, buf, n, user) != TF_OK) {
            free(p->error);
            p->error = strdup("sink callback failed");
            return TF_ERROR;
        }
    }
    return TF_OK;
}

static int pipeline_auto_drain_sinks(tf_pipeline *p) {
    if (!p) return TF_ERROR;
    for (int channel = 0; channel < TF_NUM_CHANNELS; channel++) {
        if (p->sinks[channel]) {
            if (pipeline_drain_channel_to_sink(p, channel, p->sinks[channel],
                                               p->sink_users[channel]) != TF_OK) {
                return TF_ERROR;
            }
        }
    }
    return TF_OK;
}

const char *tf_version(void) {
    return TRANFI_VERSION;
}

static void step_stats_free(tf_step_run_stats *stats, size_t n) {
    if (!stats) return;
    for (size_t i = 0; i < n; i++) {
        free(stats[i].op);
        free(stats[i].state_estimate);
        free(stats[i].state_bytes_reason);
        free(stats[i].execution_target);
    }
    free(stats);
}

static int node_has_spill_dir(const tf_ir_node *node) {
    cJSON *spill = node && node->args ? cJSON_GetObjectItemCaseSensitive(node->args, "spill_dir") : NULL;
    return cJSON_IsString(spill) && spill->valuestring && spill->valuestring[0];
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

static int node_op_is_row_set_spillable(const tf_ir_node *node) {
    return node && node->op &&
           (strcmp(node->op, "intersect") == 0 || strcmp(node->op, "setdiff") == 0 ||
            strcmp(node->op, "intersect-all") == 0 || strcmp(node->op, "setdiff-all") == 0 ||
            strcmp(node->op, "union") == 0);
}

static int node_op_uses_native_spill(const tf_ir_node *node) {
    return node && node->op && node_has_spill_dir(node) &&
           (strcmp(node->op, "sort") == 0 ||
            strcmp(node->op, "unique") == 0 ||
            strcmp(node->op, "dedup") == 0 ||
            strcmp(node->op, "group-agg") == 0 ||
            strcmp(node->op, "pivot") == 0 ||
            node_op_is_join_spillable(node) ||
            node_op_is_row_set_spillable(node));
}

static const char *runtime_step_execution_target(const tf_ir_node *node) {
#ifdef __EMSCRIPTEN__
    (void)node;
    return "wasm";
#else
    if (node_op_uses_native_spill(node)) return "native_spill";
    return "native";
#endif
}

static int build_step_stats_from_plan(const tf_ir_plan *plan,
                                      tf_step_run_stats **out_stats,
                                      size_t *out_n) {
    if (!out_stats || !out_n) return TF_ERROR;
    *out_stats = NULL;
    *out_n = 0;
    if (!plan) return TF_OK;

    size_t count = 0;
    for (size_t i = 0; i < plan->n_nodes; i++) {
        const tf_op_entry *entry = tf_op_registry_find(plan->nodes[i].op);
        if (entry && entry->kind == TF_OP_TRANSFORM && entry->create_native) count++;
    }
    if (count == 0) return TF_OK;

    tf_step_run_stats *stats = calloc(count, sizeof(tf_step_run_stats));
    if (!stats) return TF_ERROR;

    size_t j = 0;
    for (size_t i = 0; i < plan->n_nodes; i++) {
        const tf_ir_node *node = &plan->nodes[i];
        const tf_op_entry *entry = tf_op_registry_find(node->op);
        if (!entry || entry->kind != TF_OP_TRANSFORM || !entry->create_native) continue;

        stats[j].op = strdup(node->op);
        stats[j].state_estimate = strdup(node->state_estimate ? node->state_estimate : "unknown");
        stats[j].execution_target = strdup(runtime_step_execution_target(node));
        if (!stats[j].op || !stats[j].state_estimate || !stats[j].execution_target) {
            step_stats_free(stats, count);
            return TF_ERROR;
        }
        stats[j].node_index = node->index;
        stats[j].memory_class = node->memory_class;
        stats[j].emit_class = node->emit_class;
        stats[j].schema_class = node->schema_class;
        if (node->memory_class == TF_MEM_BLOCKING) stats[j].warnings |= TF_STEP_WARN_BLOCKING;
        if (node->emit_class == TF_EMIT_ON_FLUSH) stats[j].warnings |= TF_STEP_WARN_FLUSH_LATENT;
        if (node->schema_class == TF_SCHEMA_DATA_DEPENDENT) stats[j].warnings |= TF_STEP_WARN_DATA_DEPENDENT_SCHEMA;

        char reason[192] = {0};
        size_t estimate = 0;
        if (tf_estimate_step_state_bytes(node, &estimate, reason, sizeof(reason))) {
            stats[j].has_state_bytes_estimate = 1;
            stats[j].state_bytes_estimate = estimate;
        } else if (node->memory_class == TF_MEM_KEY_STATE || node->memory_class == TF_MEM_BLOCKING) {
            stats[j].warnings |= TF_STEP_WARN_UNBOUNDED_STATE;
            stats[j].state_bytes_reason = strdup(reason[0] ? reason : "no native byte estimator");
            if (!stats[j].state_bytes_reason) {
                step_stats_free(stats, count);
                return TF_ERROR;
            }
        }
        j++;
    }

    *out_stats = stats;
    *out_n = count;
    return TF_OK;
}

static void step_stats_record_input(tf_pipeline *p, size_t step_idx, size_t rows) {
    if (!p || step_idx >= p->n_step_stats) return;
    p->step_stats[step_idx].batches_in++;
    p->step_stats[step_idx].rows_in += rows;
}

static void step_stats_record_output(tf_pipeline *p, size_t step_idx, const tf_batch *batch) {
    if (!p || step_idx >= p->n_step_stats || !batch) return;
    p->step_stats[step_idx].batches_out++;
    p->step_stats[step_idx].rows_out += batch->n_rows;
}

static int step_stats_write_warning_token(tf_pipeline *p, const char *token, int *first) {
    char buf[96];
    snprintf(buf, sizeof(buf), "%s\"%s\"", *first ? "" : ",", token);
    *first = 0;
    return tf_buffer_write_str(&p->output[TF_CHAN_STATS], buf);
}

static int step_stats_write_warnings(tf_pipeline *p, uint32_t warnings) {
    if (tf_buffer_write_str(&p->output[TF_CHAN_STATS], "\"warnings\":[") != TF_OK)
        return TF_ERROR;
    int first = 1;
    if ((warnings & TF_STEP_WARN_BLOCKING) &&
        step_stats_write_warning_token(p, "blocking", &first) != TF_OK)
        return TF_ERROR;
    if ((warnings & TF_STEP_WARN_FLUSH_LATENT) &&
        step_stats_write_warning_token(p, "flush_latent", &first) != TF_OK)
        return TF_ERROR;
    if ((warnings & TF_STEP_WARN_UNBOUNDED_STATE) &&
        step_stats_write_warning_token(p, "unbounded_state", &first) != TF_OK)
        return TF_ERROR;
    if ((warnings & TF_STEP_WARN_DATA_DEPENDENT_SCHEMA) &&
        step_stats_write_warning_token(p, "data_dependent_schema", &first) != TF_OK)
        return TF_ERROR;
    return tf_buffer_write_str(&p->output[TF_CHAN_STATS], "],");
}

static int emit_step_stats(tf_pipeline *p) {
    if (!p) return TF_ERROR;
    if (tf_buffer_write_str(&p->output[TF_CHAN_STATS], "{\"type\":\"step_stats\",\"steps\":[") != TF_OK)
        return TF_ERROR;
    for (size_t i = 0; i < p->n_step_stats; i++) {
        tf_step_run_stats *st = &p->step_stats[i];
        char buf[1024];
        snprintf(buf, sizeof(buf),
                 "%s{\"index\":%zu,\"op\":\"%s\",\"execution_target\":\"%s\","
                 "\"memory_class\":\"%s\",\"emit_class\":\"%s\","
                 "\"schema_class\":\"%s\",\"state_estimate\":\"%s\",",
                 i ? "," : "",
                 st->node_index,
                 st->op ? st->op : "unknown",
                 st->execution_target ? st->execution_target : "native",
                 tf_memory_class_name(st->memory_class),
                 tf_emit_class_name(st->emit_class),
                 tf_schema_class_name(st->schema_class),
                 st->state_estimate ? st->state_estimate : "unknown");
        if (tf_buffer_write_str(&p->output[TF_CHAN_STATS], buf) != TF_OK)
            return TF_ERROR;
        if (st->has_state_bytes_estimate) {
            snprintf(buf, sizeof(buf), "\"state_bytes_estimate\":%zu,", st->state_bytes_estimate);
        } else {
            snprintf(buf, sizeof(buf), "\"state_bytes_estimate\":null,");
        }
        if (tf_buffer_write_str(&p->output[TF_CHAN_STATS], buf) != TF_OK)
            return TF_ERROR;
        if (st->state_bytes_reason) {
            snprintf(buf, sizeof(buf), "\"state_bytes_reason\":\"%s\",", st->state_bytes_reason);
            if (tf_buffer_write_str(&p->output[TF_CHAN_STATS], buf) != TF_OK)
                return TF_ERROR;
        }
        if (step_stats_write_warnings(p, st->warnings) != TF_OK)
            return TF_ERROR;
        snprintf(buf, sizeof(buf),
                 "\"batches_in\":%zu,\"batches_out\":%zu,\"rows_in\":%zu,\"rows_out\":%zu",
                 st->batches_in, st->batches_out, st->rows_in, st->rows_out);
        if (tf_buffer_write_str(&p->output[TF_CHAN_STATS], buf) != TF_OK)
            return TF_ERROR;
        if (i < p->n_steps && p->steps[i] && p->steps[i]->append_stats &&
            p->steps[i]->append_stats(p->steps[i], &p->output[TF_CHAN_STATS]) != TF_OK)
            return TF_ERROR;
        if (tf_buffer_write_str(&p->output[TF_CHAN_STATS], "}") != TF_OK)
            return TF_ERROR;
    }
    return tf_buffer_write_str(&p->output[TF_CHAN_STATS], "]}\n");
}

static tf_pipeline *assemble_pipeline(tf_decoder *decoder, tf_step **steps,
                                      size_t n_steps,
                                      tf_step_run_stats *step_stats,
                                      size_t n_step_stats,
                                      tf_encoder *encoder) {
    tf_pipeline *p = calloc(1, sizeof(tf_pipeline));
    if (!p) {
        decoder->destroy(decoder);
        encoder->destroy(encoder);
        for (size_t i = 0; i < n_steps; i++) steps[i]->destroy(steps[i]);
        free(steps);
        step_stats_free(step_stats, n_step_stats);
        tf_set_last_error("out of memory");
        return NULL;
    }

    p->decoder = decoder;
    p->steps = steps;
    p->n_steps = n_steps;
    p->step_stats = step_stats;
    p->n_step_stats = n_step_stats;
    p->encoder = encoder;
    p->encode_output = 1;

    for (int i = 0; i < TF_NUM_CHANNELS; i++) {
        tf_buffer_init(&p->output[i]);
    }

    /* Wire up side channels */
    p->side.errors = &p->output[TF_CHAN_ERRORS];
    p->side.stats = &p->output[TF_CHAN_STATS];
    p->side.samples = &p->output[TF_CHAN_SAMPLES];
    p->side.source_name = p->source_name;

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

    /* 4. Build per-step stats metadata and compile to native target */
    tf_step_run_stats *step_stats = NULL;
    size_t n_step_stats = 0;
    if (build_step_stats_from_plan(ir, &step_stats, &n_step_stats) != TF_OK) {
        tf_set_last_error("out of memory");
        tf_ir_plan_free(ir);
        return NULL;
    }

    tf_decoder *decoder = NULL;
    tf_step **steps = NULL;
    size_t n_steps = 0;
    tf_encoder *encoder = NULL;
    if (tf_compile_native(ir, &decoder, &steps, &n_steps, &encoder, &error) != TF_OK) {
        tf_set_last_error(error ? error : "compilation failed");
        free(error);
        step_stats_free(step_stats, n_step_stats);
        tf_ir_plan_free(ir);
        return NULL;
    }

    tf_ir_plan_free(ir);

    /* 5. Assemble pipeline */
    return assemble_pipeline(decoder, steps, n_steps, step_stats, n_step_stats, encoder);
}

tf_pipeline *tf_pipeline_create_from_ir(const tf_ir_plan *plan) {
    if (!plan) {
        tf_set_last_error("NULL IR plan");
        return NULL;
    }

    char *error = NULL;
    tf_step_run_stats *step_stats = NULL;
    size_t n_step_stats = 0;
    if (build_step_stats_from_plan(plan, &step_stats, &n_step_stats) != TF_OK) {
        tf_set_last_error("out of memory");
        return NULL;
    }

    tf_decoder *decoder = NULL;
    tf_step **steps = NULL;
    size_t n_steps = 0;
    tf_encoder *encoder = NULL;

    if (tf_compile_native(plan, &decoder, &steps, &n_steps, &encoder, &error) != TF_OK) {
        tf_set_last_error(error ? error : "compilation failed");
        free(error);
        step_stats_free(step_stats, n_step_stats);
        return NULL;
    }

    return assemble_pipeline(decoder, steps, n_steps, step_stats, n_step_stats, encoder);
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

static const char *pipeline_phase_name(const tf_pipeline *p) {
    if (!p) return "unknown";
    if (p->finished || p->finish_phase == TF_FINISH_PHASE_DONE) return "done";
    if (!p->finish_started) return "push";
    switch (p->finish_phase) {
        case TF_FINISH_PHASE_DECODER: return "decoder_flush";
        case TF_FINISH_PHASE_STEPS: return "step_flush";
        case TF_FINISH_PHASE_ENCODER: return "encoder_flush";
        case TF_FINISH_PHASE_STATS: return "stats";
        case TF_FINISH_PHASE_DONE: return "done";
        default: return "unknown";
    }
}

static int pipeline_report_progress(tf_pipeline *p, int force) {
    if (!p || !p->progress_cb) return TF_OK;
    if (!force && p->progress_interval_rows > 0) {
        size_t in_delta = p->rows_in >= p->progress_last_rows_in
                            ? p->rows_in - p->progress_last_rows_in : 0;
        size_t out_delta = p->rows_out >= p->progress_last_rows_out
                             ? p->rows_out - p->progress_last_rows_out : 0;
        if (in_delta < p->progress_interval_rows && out_delta < p->progress_interval_rows) {
            return TF_OK;
        }
    }

    tf_pipeline_progress progress = {
        p->bytes_in,
        p->bytes_out,
        p->rows_in,
        p->rows_out,
        p->batches_in,
        p->batches_out,
        pipeline_phase_name(p),
        p->finished ? 1 : 0,
    };
    if (p->progress_cb(&progress, p->progress_user) != TF_OK) {
        tf_set_last_error("progress callback failed");
        free(p->error);
        p->error = strdup("progress callback failed");
        return TF_ERROR;
    }
    p->progress_last_rows_in = p->rows_in;
    p->progress_last_rows_out = p->rows_out;
    return TF_OK;
}

static int emit_output_batch(tf_pipeline *p, tf_batch *batch) {
    if (!p || !batch) return TF_OK;

    if (batch->n_rows > 0) {
        p->rows_out += batch->n_rows;
        p->batches_out++;

        if (p->batch_sink && p->batch_sink(batch, p->batch_sink_user) != TF_OK) {
            tf_set_last_error("batch sink callback failed");
            free(p->error);
            p->error = strdup("batch sink callback failed");
            return TF_ERROR;
        }
    }

    if (!p->encode_output) return TF_OK;

    tf_set_last_error(NULL);
    size_t before = tf_buffer_readable(&p->output[TF_CHAN_MAIN]);
    int rc = p->encoder->encode(p->encoder, batch, &p->output[TF_CHAN_MAIN]);
    if (rc == TF_OK) {
        size_t after = tf_buffer_readable(&p->output[TF_CHAN_MAIN]);
        if (after >= before) p->bytes_out += after - before;
    }
    return rc;
}

/*
 * Process a batch through all steps, then encode.
 */
static int process_batch(tf_pipeline *p, tf_batch *batch) {
    tf_batch *current = batch;
    int batch_owned = 0; /* 0 = still owned by caller (decoder) */

    p->rows_in += current->n_rows;
    p->batches_in++;

    for (size_t i = 0; i < p->n_steps; i++) {
        tf_batch *next = NULL;
        step_stats_record_input(p, i, current->n_rows);
        tf_set_last_error(NULL);
        int rc = p->steps[i]->process(p->steps[i], current, &next, &p->side);

        if (batch_owned) tf_batch_free(current);
        batch_owned = 1;

        if (rc != TF_OK) return TF_ERROR;
        step_stats_record_output(p, i, next);
        if (!next) return TF_OK; /* filtered away entirely */
        current = next;
    }

    /* Emit transformed output batch. */
    int rc = emit_output_batch(p, current);
    if (batch_owned) tf_batch_free(current);
    return rc;
}

static int route_flushed_batch(tf_pipeline *p, size_t source_step, tf_batch *flushed) {
    if (!flushed) return TF_OK;
    step_stats_record_output(p, source_step, flushed);

    /* Run flushed batch through remaining steps. */
    tf_batch *current = flushed;
    int owned = 1;
    int rc = TF_OK;
    for (size_t j = source_step + 1; j < p->n_steps; j++) {
        tf_batch *next = NULL;
        step_stats_record_input(p, j, current->n_rows);
        tf_set_last_error(NULL);
        rc = p->steps[j]->process(p->steps[j], current, &next, &p->side);
        if (owned) tf_batch_free(current);
        owned = 1;
        if (rc != TF_OK) return pipeline_fail(p, "processing error");
        step_stats_record_output(p, j, next);
        if (!next) { current = NULL; break; }
        current = next;
    }

    if (current) {
        rc = emit_output_batch(p, current);
        if (rc != TF_OK) {
            if (current && owned) tf_batch_free(current);
            return pipeline_fail(p, "encode error");
        }
    }
    if (current && owned) tf_batch_free(current);
    if (pipeline_auto_drain_sinks(p) != TF_OK) return TF_ERROR;
    if (pipeline_report_progress(p, 0) != TF_OK) return TF_ERROR;
    return TF_OK;
}

int tf_pipeline_push(tf_pipeline *p, const uint8_t *data, size_t len) {
    if (!p || p->finished || p->finish_started) return TF_ERROR;

    p->bytes_in += len;

    /* Decode bytes into batches */
    tf_batch **batches = NULL;
    size_t n_batches = 0;
    tf_set_last_error(NULL);
    int rc = p->decoder->decode(p->decoder, data, len, &batches, &n_batches, &p->side);
    if (rc != TF_OK) {
        return pipeline_fail(p, "decode error");
    }

    /* Process each batch */
    for (size_t i = 0; i < n_batches; i++) {
        rc = process_batch(p, batches[i]);
        tf_batch_free(batches[i]);
        if (rc != TF_OK) {
            for (size_t j = i + 1; j < n_batches; j++) tf_batch_free(batches[j]);
            free(batches);
            return p->error ? TF_ERROR : pipeline_fail(p, "processing error");
        }
        if (pipeline_auto_drain_sinks(p) != TF_OK) {
            for (size_t j = i + 1; j < n_batches; j++) tf_batch_free(batches[j]);
            free(batches);
            return TF_ERROR;
        }
        if (pipeline_report_progress(p, 0) != TF_OK) {
            for (size_t j = i + 1; j < n_batches; j++) tf_batch_free(batches[j]);
            free(batches);
            return TF_ERROR;
        }
    }
    free(batches);

    return TF_OK;
}

int tf_pipeline_finish_step(tf_pipeline *p) {
    if (!p) return TF_ERROR;
    if (p->finished || p->finish_phase == TF_FINISH_PHASE_DONE) return TF_DONE;

    if (!p->finish_started) {
        p->finish_started = 1;
        p->finish_phase = TF_FINISH_PHASE_DECODER;
        p->finish_batch_index = 0;
        p->finish_n_batches = 0;
        p->finish_batches = NULL;
        p->finish_step_index = 0;
        p->finish_step_in_next = 0;

        tf_set_last_error(NULL);
        int rc = p->decoder->flush(p->decoder, &p->finish_batches,
                                   &p->finish_n_batches, &p->side);
        if (rc != TF_OK) return pipeline_fail(p, "decode flush error");
    }

    for (;;) {
        if (p->finish_phase == TF_FINISH_PHASE_DECODER) {
            if (p->finish_batch_index < p->finish_n_batches) {
                tf_batch *batch = p->finish_batches[p->finish_batch_index];
                p->finish_batches[p->finish_batch_index] = NULL;
                p->finish_batch_index++;

                int rc = process_batch(p, batch);
                tf_batch_free(batch);
                if (rc != TF_OK) {
                    pipeline_clear_finish_batches(p);
                    return p->error ? TF_ERROR : pipeline_fail(p, "processing error");
                }
                if (pipeline_auto_drain_sinks(p) != TF_OK) {
                    pipeline_clear_finish_batches(p);
                    return TF_ERROR;
                }
                if (pipeline_report_progress(p, 0) != TF_OK) {
                    pipeline_clear_finish_batches(p);
                    return TF_ERROR;
                }
                return TF_OK;
            }
            pipeline_clear_finish_batches(p);
            p->finish_phase = TF_FINISH_PHASE_STEPS;
            continue;
        }

        if (p->finish_phase == TF_FINISH_PHASE_STEPS) {
            if (p->finish_step_index >= p->n_steps) {
                p->finish_phase = TF_FINISH_PHASE_ENCODER;
                continue;
            }

            tf_step *step = p->steps[p->finish_step_index];
            tf_batch *flushed = NULL;
            int rc = TF_OK;

            if (!p->finish_step_in_next) {
                tf_set_last_error(NULL);
                rc = step->flush(step, &flushed, &p->side);
                if (rc != TF_OK) return pipeline_fail(p, "flush error");
                p->finish_step_in_next = 1;
                if (flushed) {
                    if (route_flushed_batch(p, p->finish_step_index, flushed) != TF_OK) return TF_ERROR;
                    return TF_OK;
                }
                if (step->flush_next) continue;
                p->finish_step_index++;
                p->finish_step_in_next = 0;
                continue;
            }

            if (step->flush_next) {
                tf_set_last_error(NULL);
                rc = step->flush_next(step, &flushed, &p->side);
                if (rc != TF_OK) return pipeline_fail(p, "flush error");
                if (flushed) {
                    if (route_flushed_batch(p, p->finish_step_index, flushed) != TF_OK) return TF_ERROR;
                    return TF_OK;
                }
            }
            p->finish_step_index++;
            p->finish_step_in_next = 0;
            continue;
        }

        if (p->finish_phase == TF_FINISH_PHASE_ENCODER) {
            tf_set_last_error(NULL);
            size_t flush_before = tf_buffer_readable(&p->output[TF_CHAN_MAIN]);
            if (p->encoder->flush(p->encoder, &p->output[TF_CHAN_MAIN]) != TF_OK) {
                return pipeline_fail(p, "encode flush error");
            }
            size_t flush_after = tf_buffer_readable(&p->output[TF_CHAN_MAIN]);
            if (flush_after >= flush_before) p->bytes_out += flush_after - flush_before;
            if (pipeline_auto_drain_sinks(p) != TF_OK) return TF_ERROR;
            if (pipeline_report_progress(p, 0) != TF_OK) return TF_ERROR;
            p->finish_phase = TF_FINISH_PHASE_STATS;
            return TF_OK;
        }

        if (p->finish_phase == TF_FINISH_PHASE_STATS) {
            char stats_buf[256];
            snprintf(stats_buf, sizeof(stats_buf),
                     "{\"rows_in\":%zu,\"rows_out\":%zu,\"bytes_in\":%zu,\"bytes_out\":%zu}\n",
                     p->rows_in, p->rows_out, p->bytes_in, p->bytes_out);
            if (tf_buffer_write_str(&p->output[TF_CHAN_STATS], stats_buf) != TF_OK)
                return pipeline_fail_msg(p, "stats output error");
            if (emit_step_stats(p) != TF_OK)
                return pipeline_fail_msg(p, "stats output error");
            if (pipeline_auto_drain_sinks(p) != TF_OK) return TF_ERROR;
            p->finished = 1;
            p->finish_phase = TF_FINISH_PHASE_DONE;
            if (pipeline_report_progress(p, 1) != TF_OK) return TF_ERROR;
            return TF_DONE;
        }

        if (p->finish_phase == TF_FINISH_PHASE_DONE) {
            p->finished = 1;
            return TF_DONE;
        }

        return pipeline_fail_msg(p, "invalid finish state");
    }
}

int tf_pipeline_finish(tf_pipeline *p) {
    if (!p) return TF_ERROR;
    if (p->finished) return TF_ERROR;
    for (;;) {
        int rc = tf_pipeline_finish_step(p);
        if (rc == TF_DONE) return TF_OK;
        if (rc != TF_OK) return TF_ERROR;
    }
}

size_t tf_pipeline_pull(tf_pipeline *p, int channel, uint8_t *buf, size_t buf_len) {
    if (!p || channel < 0 || channel >= TF_NUM_CHANNELS) return 0;
    return tf_buffer_read(&p->output[channel], buf, buf_len);
}

int tf_pipeline_drain(tf_pipeline *p, int channel, tf_pipeline_sink_fn sink, void *user) {
    if (!p || channel < 0 || channel >= TF_NUM_CHANNELS || !sink) return TF_ERROR;
    return pipeline_drain_channel_to_sink(p, channel, sink, user);
}

int tf_pipeline_set_sink(tf_pipeline *p, int channel, tf_pipeline_sink_fn sink, void *user) {
    if (!p || channel < 0 || channel >= TF_NUM_CHANNELS) return TF_ERROR;
    p->sinks[channel] = sink;
    p->sink_users[channel] = sink ? user : NULL;
    if (sink && tf_buffer_readable(&p->output[channel]) > 0) {
        return pipeline_drain_channel_to_sink(p, channel, sink, user);
    }
    return TF_OK;
}

int tf_pipeline_set_batch_sink(tf_pipeline *p, tf_pipeline_batch_sink_fn sink, void *user) {
    if (!p) return TF_ERROR;
    p->batch_sink = sink;
    p->batch_sink_user = sink ? user : NULL;
    return TF_OK;
}

int tf_pipeline_set_encode_output(tf_pipeline *p, int enabled) {
    if (!p) return TF_ERROR;
    p->encode_output = enabled ? 1 : 0;
    return TF_OK;
}

int tf_pipeline_set_progress_callback(tf_pipeline *p, tf_pipeline_progress_fn cb,
                                      void *user, size_t interval_rows) {
    if (!p) return TF_ERROR;
    p->progress_cb = cb;
    p->progress_user = cb ? user : NULL;
    p->progress_interval_rows = interval_rows;
    p->progress_last_rows_in = p->rows_in;
    p->progress_last_rows_out = p->rows_out;
    return TF_OK;
}

int tf_pipeline_set_source_name(tf_pipeline *p, const char *name) {
    if (!p || p->finished || p->finish_started) return TF_ERROR;
    char *copy = NULL;
    if (name && name[0]) {
        copy = strdup(name);
        if (!copy) return pipeline_fail_msg(p, "out of memory");
    }
    free(p->source_name);
    p->source_name = copy;
    p->side.source_name = p->source_name;
    return TF_OK;
}

int tf_pipeline_flush_input(tf_pipeline *p) {
    if (!p || p->finished || p->finish_started) return TF_ERROR;

    tf_batch **batches = NULL;
    size_t n_batches = 0;
    tf_set_last_error(NULL);
    int rc = p->decoder->flush(p->decoder, &batches, &n_batches, &p->side);
    if (rc != TF_OK) return pipeline_fail(p, "decode boundary flush error");

    for (size_t i = 0; i < n_batches; i++) {
        rc = process_batch(p, batches[i]);
        tf_batch_free(batches[i]);
        if (rc != TF_OK) {
            for (size_t j = i + 1; j < n_batches; j++) tf_batch_free(batches[j]);
            free(batches);
            return p->error ? TF_ERROR : pipeline_fail(p, "processing error");
        }
        if (pipeline_auto_drain_sinks(p) != TF_OK) {
            for (size_t j = i + 1; j < n_batches; j++) tf_batch_free(batches[j]);
            free(batches);
            return TF_ERROR;
        }
        if (pipeline_report_progress(p, 0) != TF_OK) {
            for (size_t j = i + 1; j < n_batches; j++) tf_batch_free(batches[j]);
            free(batches);
            return TF_ERROR;
        }
    }
    free(batches);
    return TF_OK;
}

static int file_output_sink(int channel, const uint8_t *data, size_t len, void *user) {
    (void)channel;
    FILE *out = (FILE *)user;
    if (!out) return TF_ERROR;
    if (len == 0) return TF_OK;
    return fwrite(data, 1, len, out) == len ? TF_OK : TF_ERROR;
}

#ifndef _WIN32
static int fd_output_sink(int channel, const uint8_t *data, size_t len, void *user) {
    (void)channel;
    if (!user) return TF_ERROR;
    int fd = *(int *)user;
    if (fd < 0) return TF_ERROR;
    const uint8_t *cursor = data;
    size_t remaining = len;
    while (remaining > 0) {
        ssize_t n = write(fd, cursor, remaining);
        if (n < 0) {
            if (errno == EINTR) continue;
            return TF_ERROR;
        }
        if (n == 0) return TF_ERROR;
        cursor += (size_t)n;
        remaining -= (size_t)n;
    }
    return TF_OK;
}
#endif

int tf_pipeline_run_file(tf_pipeline *p, FILE *in, FILE *out, size_t chunk_size) {
    if (!p || !in || !out || p->finished || p->finish_started) return TF_ERROR;
    if (chunk_size == 0) chunk_size = 64 * 1024;

    uint8_t *buf = malloc(chunk_size);
    if (!buf) return pipeline_fail_msg(p, "out of memory");

    tf_pipeline_sink_fn prev_sink = p->sinks[TF_CHAN_MAIN];
    void *prev_user = p->sink_users[TF_CHAN_MAIN];
    if (tf_pipeline_set_sink(p, TF_CHAN_MAIN, file_output_sink, out) != TF_OK) {
        p->sinks[TF_CHAN_MAIN] = prev_sink;
        p->sink_users[TF_CHAN_MAIN] = prev_sink ? prev_user : NULL;
        free(buf);
        return TF_ERROR;
    }

    int rc = TF_OK;
    for (;;) {
        size_t n = fread(buf, 1, chunk_size, in);
        if (n > 0 && tf_pipeline_push(p, buf, n) != TF_OK) {
            rc = TF_ERROR;
            break;
        }
        if (n < chunk_size) {
            if (ferror(in)) rc = pipeline_fail_msg(p, "file input read error");
            break;
        }
    }

    if (rc == TF_OK && tf_pipeline_finish(p) != TF_OK) rc = TF_ERROR;
    if (rc == TF_OK && fflush(out) != 0) rc = pipeline_fail_msg(p, "file output flush error");

    p->sinks[TF_CHAN_MAIN] = prev_sink;
    p->sink_users[TF_CHAN_MAIN] = prev_sink ? prev_user : NULL;
    free(buf);
    return rc;
}

int tf_pipeline_run_fd(tf_pipeline *p, int in_fd, int out_fd, size_t chunk_size) {
#ifdef _WIN32
    (void)in_fd;
    (void)out_fd;
    (void)chunk_size;
    if (p) return pipeline_fail_msg(p, "file descriptor runner is not supported on this platform");
    tf_set_last_error("file descriptor runner is not supported on this platform");
    return TF_ERROR;
#else
    if (!p || in_fd < 0 || out_fd < 0 || p->finished || p->finish_started) return TF_ERROR;
    if (chunk_size == 0) chunk_size = 64 * 1024;

    uint8_t *buf = malloc(chunk_size);
    if (!buf) return pipeline_fail_msg(p, "out of memory");

    tf_pipeline_sink_fn prev_sink = p->sinks[TF_CHAN_MAIN];
    void *prev_user = p->sink_users[TF_CHAN_MAIN];
    if (tf_pipeline_set_sink(p, TF_CHAN_MAIN, fd_output_sink, &out_fd) != TF_OK) {
        p->sinks[TF_CHAN_MAIN] = prev_sink;
        p->sink_users[TF_CHAN_MAIN] = prev_sink ? prev_user : NULL;
        free(buf);
        return TF_ERROR;
    }

    int rc = TF_OK;
    for (;;) {
        ssize_t n = read(in_fd, buf, chunk_size);
        if (n > 0) {
            if (tf_pipeline_push(p, buf, (size_t)n) != TF_OK) {
                rc = TF_ERROR;
                break;
            }
            continue;
        }
        if (n == 0) break;
        if (errno == EINTR) continue;
        rc = pipeline_fail_msg(p, "file descriptor input read error");
        break;
    }

    if (rc == TF_OK && tf_pipeline_finish(p) != TF_OK) rc = TF_ERROR;

    p->sinks[TF_CHAN_MAIN] = prev_sink;
    p->sink_users[TF_CHAN_MAIN] = prev_sink ? prev_user : NULL;
    free(buf);
    return rc;
#endif
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
    pipeline_clear_finish_batches(p);
    step_stats_free(p->step_stats, p->n_step_stats);
    for (int i = 0; i < TF_NUM_CHANNELS; i++) {
        tf_buffer_free(&p->output[i]);
    }
    free(p->source_name);
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

    if (tf_ir_validate(plan) != TF_OK) {
        if (error) *error = strdup(plan->error ? plan->error : "validation failed");
        tf_ir_plan_destroy(plan);
        return NULL;
    }
    tf_ir_infer_schema(plan);

    char *json = tf_ir_plan_to_json(plan);
    tf_ir_plan_destroy(plan);
    return json;
}

void tf_string_free(char *s) {
    free(s);
}
