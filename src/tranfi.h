/*
 * tranfi.h — Public C API for the Tranfi streaming ETL core.
 *
 * The host streams bytes in via push(), pulls output bytes from
 * multiple channels (main, errors, stats, samples) via pull().
 * Core does: decode → typed batches → transforms → encode.
 */

#ifndef TRANFI_H
#define TRANFI_H

#include "config.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Channel IDs for pull() */
#define TF_CHAN_MAIN    0
#define TF_CHAN_ERRORS  1
#define TF_CHAN_STATS   2
#define TF_CHAN_SAMPLES 3
#define TF_NUM_CHANNELS 4

/* Return codes */
#define TF_OK    0
#define TF_DONE  1
#define TF_ERROR (-1)

/* Shared value types used by schemas and public batch accessors. */
#ifndef TF_TYPE_DEFINED
#define TF_TYPE_DEFINED
typedef enum tf_type {
    TF_TYPE_NULL = 0,
    TF_TYPE_BOOL,
    TF_TYPE_INT64,
    TF_TYPE_FLOAT64,
    TF_TYPE_STRING,
    TF_TYPE_DATE,       /* int32_t: days since 1970-01-01 */
    TF_TYPE_TIMESTAMP,  /* int64_t: microseconds since 1970-01-01T00:00:00Z */
} tf_type;
#endif

/* Opaque pipeline and columnar batch handles. */
typedef struct tf_pipeline tf_pipeline;
typedef struct tf_batch tf_batch;

/*
 * Sink callback used by tf_pipeline_set_sink() and tf_pipeline_drain().
 * Return TF_OK to continue, TF_ERROR to stop the pipeline with an error.
 * The data pointer is valid only for the duration of the callback.
 */
typedef int (*tf_pipeline_sink_fn)(int channel, const uint8_t *data, size_t len, void *user);

/*
 * Batch callback for embedders that consume transformed columnar batches before
 * encoding. The batch and any returned string pointers are valid only for the
 * duration of the callback; copy data that must outlive the callback.
 */
typedef int (*tf_pipeline_batch_sink_fn)(const tf_batch *batch, void *user);

/* Progress snapshot passed to tf_pipeline_progress_fn. */
typedef struct tf_pipeline_progress {
    size_t bytes_in;
    size_t bytes_out;
    size_t rows_in;
    size_t rows_out;
    size_t batches_in;
    size_t batches_out;
    const char *phase; /* push, decoder_flush, step_flush, encoder_flush, stats, done */
    int finished;
} tf_pipeline_progress;

/*
 * Progress callback invoked at batch/flush boundaries. Return TF_OK to continue
 * or TF_ERROR to stop the pipeline with `progress callback failed`.
 */
typedef int (*tf_pipeline_progress_fn)(const tf_pipeline_progress *progress, void *user);


/*
 * Host policy for embedded or untrusted execution surfaces.
 * File-taking pipeline args are validated before native construction; when
 * workspace_root or resolve_path is provided, accepted paths are rewritten to
 * the resolved host path in the mutable IR used for execution.
 */
typedef int (*tf_resolve_path_fn)(const char *logical, char *out, size_t out_sz, void *user);

typedef struct tf_host_policy {
    bool allow_fs;
    bool allow_net;
    bool allow_spill;
    bool allow_blocking;
    bool allow_rules_file;
    const char *workspace_root;
    tf_resolve_path_fn resolve_path;
    void *user;
} tf_host_policy;

/* Read-only columnar batch accessors for batch callbacks. */
size_t      tf_batch_num_rows(const tf_batch *b);
size_t      tf_batch_num_cols(const tf_batch *b);
const char *tf_batch_col_name(const tf_batch *b, size_t col);
tf_type     tf_batch_col_type(const tf_batch *b, size_t col);
int         tf_batch_col_index(const tf_batch *b, const char *name);
bool        tf_batch_is_null(const tf_batch *b, size_t row, size_t col);
bool        tf_batch_get_bool(const tf_batch *b, size_t row, size_t col);
int64_t     tf_batch_get_int64(const tf_batch *b, size_t row, size_t col);
double      tf_batch_get_float64(const tf_batch *b, size_t row, size_t col);
const char *tf_batch_get_string(const tf_batch *b, size_t row, size_t col);
int32_t     tf_batch_get_date(const tf_batch *b, size_t row, size_t col);
int64_t     tf_batch_get_timestamp(const tf_batch *b, size_t row, size_t col);

/*
 * Create a pipeline from a JSON plan.
 * Returns NULL on error (call tf_pipeline_error on NULL is undefined;
 * use tf_last_error() instead).
 */
tf_pipeline *tf_pipeline_create(const char *plan_json, size_t len);
tf_pipeline *tf_pipeline_create_with_host_policy(const char *plan_json, size_t len,
                                             const tf_host_policy *policy);

/* Free all resources associated with a pipeline. */
void tf_pipeline_free(tf_pipeline *p);

/*
 * Push input bytes into the pipeline.
 * Returns TF_OK on success, TF_ERROR on failure.
 */
int tf_pipeline_push(tf_pipeline *p, const uint8_t *data, size_t len);

/*
 * Signal end of input. Flushes all buffered data through the pipeline.
 * Returns TF_OK on success, TF_ERROR on failure.
 */
int tf_pipeline_finish(tf_pipeline *p);

/*
 * Advance finish processing by at most one flush boundary.
 * Returns TF_OK while more finish work remains, TF_DONE once final stats are
 * emitted, or TF_ERROR on failure. Hosts can call pull()/drain() between calls
 * to avoid retaining finish-time output in the pipeline buffer.
 */
int tf_pipeline_finish_step(tf_pipeline *p);

/*
 * Pull output bytes from a channel.
 * Writes up to buf_len bytes into buf.
 * Returns the number of bytes written (0 if nothing available).
 */
size_t tf_pipeline_pull(tf_pipeline *p, int channel, uint8_t *buf, size_t buf_len);

/*
 * Drain all currently buffered bytes from one channel to a callback.
 * Returns TF_OK on success or TF_ERROR if arguments are invalid or the callback fails.
 */
int tf_pipeline_drain(tf_pipeline *p, int channel, tf_pipeline_sink_fn sink, void *user);

/*
 * Register or clear an automatic sink for a channel. When set, the pipeline drains
 * that channel after each push()/finish() batch boundary, so embedders can write to
 * FILE*, file descriptors, sockets, or host stream adapters without retaining all
 * output in the pipeline buffer. Pass NULL as sink to clear the registered sink.
 */
int tf_pipeline_set_sink(tf_pipeline *p, int channel, tf_pipeline_sink_fn sink, void *user);

/*
 * Register or clear an automatic sink for transformed output batches before
 * encoding. This is the columnar-batch counterpart to byte sinks: it is invoked
 * at output batch boundaries during push()/finish_step()/finish(). Pass NULL as
 * sink to clear the registered batch sink.
 */
int tf_pipeline_set_batch_sink(tf_pipeline *p, tf_pipeline_batch_sink_fn sink, void *user);

/*
 * Enable or disable encoded main byte output. Enabled by default. Batch-only
 * embedders can disable it after setting a batch sink to avoid CSV/JSONL byte
 * generation while still keeping side channels and row stats available.
 */
int tf_pipeline_set_encode_output(tf_pipeline *p, int enabled);

/*
 * Register or clear a progress callback. If interval_rows is 0, the callback is
 * invoked at every batch/flush boundary; otherwise it is invoked when rows_in or
 * rows_out advances by at least interval_rows, plus once at finish.
 */
int tf_pipeline_set_progress_callback(tf_pipeline *p, tf_pipeline_progress_fn cb,
                                      void *user, size_t interval_rows);

/*
 * Set or clear the current host source name. Row-local transforms such as
 * source-name can append this value without the core opening files itself.
 * Pass NULL or an empty string to clear it.
 */
int tf_pipeline_set_source_name(tf_pipeline *p, const char *name);

/*
 * Flush only the decoder input boundary and process any decoded batches, without
 * finishing transforms, encoders, or stats. Hosts use this between sequential
 * input files so partial records and decoder batches keep the correct source
 * metadata while the pipeline state remains global.
 */
int tf_pipeline_flush_input(tf_pipeline *p);

/*
 * Run a pipeline from an input FILE* to an output FILE* using bounded chunks.
 * Main output is streamed to out; side channels remain available via pull().
 * If chunk_size is 0, a 64 KiB default is used. The pipeline is finished on
 * success and must not be pushed again.
 */
int tf_pipeline_run_file(tf_pipeline *p, FILE *in, FILE *out, size_t chunk_size);

/*
 * Run a pipeline from POSIX input/output file descriptors using bounded chunks.
 * Main output is streamed to out_fd; side channels remain available via pull().
 * If chunk_size is 0, a 64 KiB default is used. Returns TF_ERROR on
 * non-POSIX platforms.
 */
int tf_pipeline_run_fd(tf_pipeline *p, int in_fd, int out_fd, size_t chunk_size);

/* Get the last error message, or NULL if no error. */
const char *tf_pipeline_error(tf_pipeline *p);

/* Get the library version string. */
const char *tf_version(void);

/* Get the last global error (for errors before pipeline creation). */
const char *tf_last_error(void);

/* ---- IR plan API (L2 intermediate representation) ---- */

/* Opaque IR plan handle */
typedef struct tf_ir_plan tf_ir_plan;

/* Parse a JSON plan string into an IR plan. Returns NULL on error. */
tf_ir_plan *tf_ir_plan_from_json(const char *json, size_t len, char **error);

/* Serialize an IR plan back to JSON. Caller frees the returned string. */
char *tf_ir_plan_to_json(const tf_ir_plan *plan);

/* Validate an IR plan. Returns TF_OK or TF_ERROR. */
int tf_ir_plan_validate(tf_ir_plan *plan);
int tf_ir_plan_validate_with_host_policy(tf_ir_plan *plan, const tf_host_policy *policy);

/* Infer schemas through an IR plan. Best-effort, non-fatal. */
int tf_ir_plan_infer_schema(tf_ir_plan *plan);

/* Free an IR plan. */
void tf_ir_plan_destroy(tf_ir_plan *plan);

/* Create a pipeline from a pre-built IR plan. */
tf_pipeline *tf_pipeline_create_from_ir(const tf_ir_plan *plan);
tf_pipeline *tf_pipeline_create_from_ir_with_host_policy(const tf_ir_plan *plan,
                                                      const tf_host_policy *policy);

/* Compile a DSL string to a JSON recipe. Caller frees with tf_string_free(). */
char *tf_compile_dsl(const char *dsl, size_t len, char **error);
char *tf_compile_dsl_with_host_policy(const char *dsl, size_t len,
                                      const tf_host_policy *policy, char **error);

/* Compile a DSL string directly to SQL. Caller frees with tf_string_free(). */
char *tf_compile_to_sql(const char *dsl, size_t len, char **error);

/* Convert an IR plan to SQL. Caller frees with tf_string_free(). */
char *tf_ir_plan_to_sql(const tf_ir_plan *plan, char **error);

/* Free a string returned by tf_compile_dsl or tf_ir_plan_to_json. */
void tf_string_free(char *s);

/* ---- Built-in recipes ---- */

/* Number of built-in recipes. */
size_t tf_recipe_count(void);

/* Accessors by index (0-based). Return NULL if index out of range. */
const char *tf_recipe_name(size_t index);
const char *tf_recipe_dsl(size_t index);
const char *tf_recipe_description(size_t index);

/* Lookup by name (case-insensitive). Returns DSL string or NULL. */
const char *tf_recipe_find_dsl(const char *name);

#ifdef __cplusplus
}
#endif

#endif /* TRANFI_H */
