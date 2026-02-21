/*
 * tranfi.h — Public C API for the Tranfi streaming ETL core.
 *
 * The host streams bytes in via push(), pulls output bytes from
 * multiple channels (main, errors, stats, samples) via pull().
 * Core does: decode → typed batches → transforms → encode.
 */

#ifndef TRANFI_H
#define TRANFI_H

#include <stddef.h>
#include <stdint.h>

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
#define TF_ERROR (-1)

/* Opaque pipeline handle */
typedef struct tf_pipeline tf_pipeline;

/*
 * Create a pipeline from a JSON plan.
 * Returns NULL on error (call tf_pipeline_error on NULL is undefined;
 * use tf_last_error() instead).
 */
tf_pipeline *tf_pipeline_create(const char *plan_json, size_t len);

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
 * Pull output bytes from a channel.
 * Writes up to buf_len bytes into buf.
 * Returns the number of bytes written (0 if nothing available).
 */
size_t tf_pipeline_pull(tf_pipeline *p, int channel, uint8_t *buf, size_t buf_len);

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

/* Infer schemas through an IR plan. Best-effort, non-fatal. */
int tf_ir_plan_infer_schema(tf_ir_plan *plan);

/* Free an IR plan. */
void tf_ir_plan_destroy(tf_ir_plan *plan);

/* Create a pipeline from a pre-built IR plan. */
tf_pipeline *tf_pipeline_create_from_ir(const tf_ir_plan *plan);

/* Compile a DSL string to a JSON recipe. Caller frees with tf_string_free(). */
char *tf_compile_dsl(const char *dsl, size_t len, char **error);

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
