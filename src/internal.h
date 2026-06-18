/*
 * internal.h — Internal types for the Tranfi core.
 * Not part of the public API.
 */

#ifndef TF_INTERNAL_H
#define TF_INTERNAL_H

#include "tranfi.h"
#include "ir.h"
#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

typedef struct cJSON cJSON;

#define TF_MAX_BATCH_ROWS             ((size_t)64u * 1024u)
#define TF_MAX_OUTPUT_ROWS_PER_BATCH  ((size_t)1024u * 1024u)
#define TF_MAX_EXPANDING_ROWS_PER_INPUT TF_MAX_OUTPUT_ROWS_PER_BATCH
#define TF_MAX_EXPLODE_TOKENS_PER_ROW TF_MAX_OUTPUT_ROWS_PER_BATCH
#define TF_MAX_COLUMNS                ((size_t)64u * 1024u)
#define TF_MAX_RECORD_BYTES           ((size_t)1024u * 1024u * 1024u)
#define TF_MAX_CELL_BYTES             ((size_t)64u * 1024u * 1024u)
#define TF_MAX_COLUMN_NAME_BYTES      ((size_t)4096u)
#define TF_MAX_SCHEMA_BYTES           ((size_t)16u * 1024u * 1024u)
#define TF_MAX_ERROR_BYTES            ((size_t)64u * 1024u * 1024u)
#define TF_MAX_REGEX_PATTERN_BYTES    ((size_t)4096u)
#define TF_MAX_REGEX_CELL_BYTES       ((size_t)64u * 1024u)
#if SIZE_MAX > 9007199254740991ULL
#define TF_MAX_SAFE_SIZE_ARG          ((size_t)9007199254740991ULL)
#else
#define TF_MAX_SAFE_SIZE_ARG          ((size_t)SIZE_MAX)
#endif
#define TF_MAX_COUNT_ARG              TF_MAX_SAFE_SIZE_ARG
#define TF_MAX_STATE_BYTES            TF_MAX_SAFE_SIZE_ARG
#define TF_MAX_SPILL_MEMORY_BYTES     TF_MAX_SAFE_SIZE_ARG
#define TF_MAX_SPILL_RUN_ROWS         TF_MAX_COUNT_ARG
#define TF_MAX_SPILL_OUTPUT_ROWS      TF_MAX_BATCH_ROWS
#define TF_MAX_AUDIT_RECORDS          ((size_t)1000u * 1000u)
#define TF_MAX_TABLE_WIDTH            ((size_t)4096u)
#define TF_MAX_TABLE_ROWS             TF_MAX_COUNT_ARG
#define TF_MAX_WINDOW_SIZE            TF_MAX_OUTPUT_ROWS_PER_BATCH
#define TF_FLOAT64_ROUNDTRIP_FORMAT   "%.17g"

#if defined(__GNUC__) || defined(__clang__)
#define TF_WARN_UNUSED __attribute__((warn_unused_result))
#else
#define TF_WARN_UNUSED
#endif

TF_WARN_UNUSED int tf_format_float64(char *buf, size_t buf_size, double value);
TF_WARN_UNUSED int tf_size_add(size_t a, size_t b, size_t *out);
TF_WARN_UNUSED int tf_size_mul(size_t a, size_t b, size_t *out);
TF_WARN_UNUSED int tf_size_align(size_t x, size_t align, size_t *out);
TF_WARN_UNUSED int tf_size_grow_pow2(size_t current, size_t min_value, size_t min_capacity, size_t *out);
TF_WARN_UNUSED void *tf_mallocarray_checked(size_t count, size_t elem_size);
TF_WARN_UNUSED void *tf_callocarray_checked(size_t count, size_t elem_size);
TF_WARN_UNUSED void *tf_reallocarray_checked(void *ptr, size_t count, size_t elem_size);
TF_WARN_UNUSED int tf_check_byte_limit(size_t n, size_t max_value,
                                       const char *context, const char *name);
TF_WARN_UNUSED int tf_string_length_bounded(const char *s, size_t max_value,
                                            size_t *out, const char *context,
                                            const char *name);
TF_WARN_UNUSED int tf_json_get_size_arg(const cJSON *args, const char *name,
                                        size_t min_value, size_t max_value,
                                        size_t *out, const char *context);
TF_WARN_UNUSED int tf_json_size_value(const cJSON *item, const char *name,
                                      size_t min_value, size_t max_value,
                                      size_t *out, const char *context);
TF_WARN_UNUSED int tf_json_get_size_arg_any(const cJSON *args, const char *name,
                                            const char *alt_name,
                                            size_t min_value, size_t max_value,
                                            size_t *out, const char *context);

typedef int (*tf_index_compare_fn)(const void *ctx, size_t a, size_t b);
void tf_sort_indices(size_t *indices, size_t n, tf_index_compare_fn compare, const void *ctx);

/* ---- Arena allocator ---- */

typedef struct tf_arena_block {
    uint8_t *data;
    size_t   used;
    size_t   cap;
    struct tf_arena_block *next;
} tf_arena_block;

typedef struct tf_arena {
    tf_arena_block *head;
    tf_arena_block *current;
    size_t          block_size; /* default block capacity */
} tf_arena;

tf_arena *tf_arena_create(size_t block_size);
void     *tf_arena_alloc(tf_arena *a, size_t size);
char     *tf_arena_strdup(tf_arena *a, const char *s);
void      tf_arena_reset(tf_arena *a);
void      tf_arena_free(tf_arena *a);

/* ---- Growable byte buffer ---- */

typedef struct tf_buffer {
    uint8_t *data;
    size_t   len;      /* bytes written */
    size_t   cap;      /* allocated capacity */
    size_t   read_pos; /* consumer read position */
} tf_buffer;

void tf_buffer_init(tf_buffer *b);
TF_WARN_UNUSED int tf_buffer_write(tf_buffer *b, const uint8_t *data, size_t len);
size_t tf_buffer_read(tf_buffer *b, uint8_t *out, size_t len);
size_t tf_buffer_readable(const tf_buffer *b);
void tf_buffer_compact(tf_buffer *b);
TF_WARN_UNUSED int tf_buffer_write_str(tf_buffer *b, const char *s);
TF_WARN_UNUSED int tf_buffer_write_line(tf_buffer *b, const char *s);
TF_WARN_UNUSED int tf_buffer_write_json_line(tf_buffer *b, const cJSON *obj);
void tf_buffer_free(tf_buffer *b);
TF_WARN_UNUSED int tf_json_add_string(cJSON *obj, const char *name, const char *value);
TF_WARN_UNUSED int tf_json_add_number(cJSON *obj, const char *name, double value);
TF_WARN_UNUSED int tf_json_add_bool(cJSON *obj, const char *name, int value);
TF_WARN_UNUSED int tf_json_add_null(cJSON *obj, const char *name);
TF_WARN_UNUSED int tf_json_add_item(cJSON *obj, const char *name, cJSON *item);
TF_WARN_UNUSED int tf_json_add_array_item(cJSON *arr, cJSON *item);

/* ---- Columnar batch ---- */
/* (tf_type is defined in ir.h, included above) */

typedef struct tf_batch {
    char       **col_names;  /* array of column name strings (arena-allocated) */
    tf_type     *col_types;  /* per-column type */
    size_t       n_cols;
    size_t       n_rows;
    size_t       capacity;   /* allocated row slots per column */
    void       **columns;    /* array of typed column arrays */
    uint8_t    **nulls;      /* null bitmap per column (1 byte per row for simplicity) */
    tf_arena    *arena;      /* owns all memory for this batch */
    size_t       schema_name_bytes;
} tf_batch;

typedef union tf_cell_value {
    bool b;
    int64_t i64;
    double f64;
    const char *str;
    int32_t date;
} tf_cell_value;

/* Owned string variant used by spill rows before copying into a batch arena. */
typedef union tf_owned_cell_value {
    uint8_t b;
    int64_t i64;
    double f64;
    int32_t date;
    char *str;
} tf_owned_cell_value;

tf_batch *tf_batch_create(size_t n_cols, size_t capacity);
TF_WARN_UNUSED int tf_batch_set_schema(tf_batch *b, size_t col, const char *name, tf_type type);
TF_WARN_UNUSED int tf_batch_ensure_capacity(tf_batch *b, size_t min_rows);
TF_WARN_UNUSED int tf_batch_expose_row(tf_batch *b, size_t row);

/* Set a value in a specific cell. String values are copied into the arena. */
TF_WARN_UNUSED int tf_batch_set_null(tf_batch *b, size_t row, size_t col);
TF_WARN_UNUSED int tf_batch_set_bool(tf_batch *b, size_t row, size_t col, bool val);
TF_WARN_UNUSED int tf_batch_set_int64(tf_batch *b, size_t row, size_t col, int64_t val);
TF_WARN_UNUSED int tf_batch_set_float64(tf_batch *b, size_t row, size_t col, double val);
TF_WARN_UNUSED int tf_batch_set_string(tf_batch *b, size_t row, size_t col, const char *val);
TF_WARN_UNUSED int tf_batch_set_string_len(tf_batch *b, size_t row, size_t col,
                                           const char *val, size_t len);
TF_WARN_UNUSED int tf_batch_set_date(tf_batch *b, size_t row, size_t col, int32_t val);
TF_WARN_UNUSED int tf_batch_set_timestamp(tf_batch *b, size_t row, size_t col, int64_t val);
TF_WARN_UNUSED int tf_batch_set_cell_value(tf_batch *b, size_t row, size_t col,
                                           tf_type type, int is_null,
                                           const tf_cell_value *value);
TF_WARN_UNUSED int tf_batch_set_owned_cell_value(tf_batch *b, size_t row, size_t col,
                                                 tf_type type, int is_null,
                                                 const tf_owned_cell_value *value);

/* Get values from a cell. */
bool      tf_batch_is_null(const tf_batch *b, size_t row, size_t col);
bool      tf_batch_get_bool(const tf_batch *b, size_t row, size_t col);
int64_t   tf_batch_get_int64(const tf_batch *b, size_t row, size_t col);
double    tf_batch_get_float64(const tf_batch *b, size_t row, size_t col);
const char *tf_batch_get_string(const tf_batch *b, size_t row, size_t col);
int32_t   tf_batch_get_date(const tf_batch *b, size_t row, size_t col);
int64_t   tf_batch_get_timestamp(const tf_batch *b, size_t row, size_t col);

/* Find column index by name. Returns -1 if not found. */
int tf_batch_col_index(const tf_batch *b, const char *name);

/* Schema and row-copy helpers. */
typedef enum tf_cell_string_format {
    TF_CELL_STRING_ROUNDTRIP,   /* round-trip floats, ISO dates/timestamps */
    TF_CELL_STRING_HUMAN,       /* short floats, ISO dates/timestamps */
    TF_CELL_STRING_NUMERIC_TIME /* round-trip floats, numeric dates/timestamps */
} tf_cell_string_format;

TF_WARN_UNUSED int tf_batch_clone_schema(tf_batch *dst, const tf_batch *src);
TF_WARN_UNUSED int tf_batch_clone_with_extra_cols(tf_batch *dst, const tf_batch *src,
                                                  const char **names, const tf_type *types,
                                                  size_t n_extra);
TF_WARN_UNUSED int tf_batch_copy_cell(tf_batch *dst, size_t dst_row, size_t dst_col,
                                      const tf_batch *src, size_t src_row, size_t src_col);
TF_WARN_UNUSED int tf_batch_copy_cell_index(tf_batch *dst, size_t dst_row, size_t dst_col,
                                            const tf_batch *src, size_t src_row, int src_col);
TF_WARN_UNUSED int tf_batch_format_cell_as_string(const tf_batch *src, size_t src_row, size_t src_col,
                                                  tf_cell_string_format format,
                                                  char *buf, size_t buf_size,
                                                  const char **out);
TF_WARN_UNUSED int tf_batch_copy_cell_as_string(tf_batch *dst, size_t dst_row, size_t dst_col,
                                                const tf_batch *src, size_t src_row, size_t src_col);
TF_WARN_UNUSED int tf_batch_copy_selected_row(tf_batch *dst, size_t dst_row,
                                              const tf_batch *src, size_t src_row,
                                              const size_t *cols, size_t n_cols);
TF_WARN_UNUSED int tf_batch_copy_row(tf_batch *dst, size_t dst_row,
                                     const tf_batch *src, size_t src_row);
TF_WARN_UNUSED int tf_batch_clone_with_selected_extra_cols(tf_batch *dst,
                                                           const tf_batch *src,
                                                           const tf_batch *extra,
                                                           const int *extra_cols,
                                                           size_t n_extra);
TF_WARN_UNUSED int tf_batch_append_row(tf_batch *dst, const tf_batch *src,
                                       size_t src_row);
TF_WARN_UNUSED int tf_batch_append_row_with_selected_extra(tf_batch *dst,
                                                           const tf_batch *left,
                                                           size_t left_row,
                                                           const tf_batch *extra,
                                                           size_t extra_row,
                                                           const int *extra_cols,
                                                           size_t n_extra);
TF_WARN_UNUSED int tf_batch_append_row_with_null_extra(tf_batch *dst,
                                                       const tf_batch *left,
                                                       size_t left_row,
                                                       size_t n_extra);

void tf_batch_free(tf_batch *b);

/* ---- Audit / side-channel row serialization helpers ---- */

typedef struct tf_audit_options {
    int include_row;
    char **columns;
    size_t n_columns;
    char **redact_columns;
    size_t n_redact_columns;
    char **hash_columns;
    size_t n_hash_columns;
    size_t max_bytes;
    size_t max_cell_bytes;
} tf_audit_options;

void tf_audit_options_init(tf_audit_options *opts, int default_include_row);
int  tf_audit_options_parse(tf_audit_options *opts, const cJSON *args, const char *context);
void tf_audit_options_free(tf_audit_options *opts);
int  tf_audit_column_is_redacted(const tf_audit_options *opts, const char *column);
int  tf_audit_column_is_hashed(const tf_audit_options *opts, const char *column);
int  tf_audit_hash_string(const char *value, char *out, size_t out_size);
const char *tf_audit_format_string_for_column(const tf_audit_options *opts, const char *column,
                                              const char *value, char *buf, size_t buf_size);
cJSON *tf_audit_cell_to_json(const tf_batch *b, size_t row, size_t col, const tf_audit_options *opts);
cJSON *tf_audit_row_to_json(const tf_batch *b, size_t row, const tf_audit_options *opts);

/* ---- Column selector helpers ---- */

int tf_json_path_validate(const char *path);
const cJSON *tf_json_path_resolve(const cJSON *root, const char *path);

int tf_column_selector_has_syntax(const char *raw);
int tf_column_selectors_have_syntax_json(const cJSON *selectors);
int tf_column_selectors_resolve(char **selectors, size_t n_selectors,
                                char **names, const tf_type *types, size_t n_cols,
                                int **out_indices, size_t *out_n,
                                char **error);
int tf_column_selectors_resolve_json(const cJSON *selectors,
                                     char **names, const tf_type *types, size_t n_cols,
                                     int **out_indices, size_t *out_n,
                                     char **error);

int tf_estimate_step_state_bytes(const tf_ir_node *node, size_t *out,
                                 char *reason, size_t reason_size);
int tf_estimate_key_state_plan_bytes(const tf_ir_plan *ir, size_t *out,
                                     const tf_ir_node **failed_node,
                                     char *reason, size_t reason_size);

/* ---- Step interface (transforms) ---- */

/*
 * Side-channel callback: steps call this to emit data to side channels.
 * Owned by the pipeline and passed to steps during processing.
 */
typedef struct tf_side_channels {
    tf_buffer *errors;
    tf_buffer *stats;
    tf_buffer *samples;
    const char *source_name;
} tf_side_channels;

TF_WARN_UNUSED int tf_side_write_error(tf_side_channels *side, const char *msg);

typedef struct tf_step {
    /* Process one input batch, produce zero or one output batch.
     * *out is set to a new batch (caller frees) or NULL if filtered away. */
    int  (*process)(struct tf_step *self, tf_batch *in, tf_batch **out,
                    tf_side_channels *side);
    /* Flush any buffered state. */
    int  (*flush)(struct tf_step *self, tf_batch **out, tf_side_channels *side);
    /* Optional continuation for flush-latent steps that emit multiple batches. */
    int  (*flush_next)(struct tf_step *self, tf_batch **out, tf_side_channels *side);
    /* Optional JSON stats hook. Appends comma-prefixed fields to step_stats. */
    int  (*append_stats)(struct tf_step *self, tf_buffer *out);
    /* Free all resources. */
    void (*destroy)(struct tf_step *self);
    void  *state;
} tf_step;

#define TF_STEP_WARN_BLOCKING              (1u << 0)
#define TF_STEP_WARN_FLUSH_LATENT          (1u << 1)
#define TF_STEP_WARN_UNBOUNDED_STATE       (1u << 2)
#define TF_STEP_WARN_DATA_DEPENDENT_SCHEMA (1u << 3)

typedef struct tf_step_run_stats {
    char           *op;
    char           *state_estimate;
    char           *state_bytes_reason;
    char           *execution_target;
    size_t          node_index;
    size_t          state_bytes_estimate;
    int             has_state_bytes_estimate;
    uint32_t        warnings;
    tf_memory_class memory_class;
    tf_emit_class   emit_class;
    tf_schema_class schema_class;
    size_t          batches_in;
    size_t          batches_out;
    size_t          rows_in;
    size_t          rows_out;
} tf_step_run_stats;

/* ---- Decoder interface (bytes → batches) ---- */

typedef struct tf_decoder {
    /*
     * Decode bytes. May produce 0..N batches.
     * *out is set to a malloc'd array of tf_batch*, *n_out to its length.
     * Caller frees the array and each batch.
     */
    int  (*decode)(struct tf_decoder *self, const uint8_t *data, size_t len,
                   tf_batch ***out, size_t *n_out, tf_side_channels *side);
    /* Flush remaining data (e.g. last partial line). */
    int  (*flush)(struct tf_decoder *self, tf_batch ***out, size_t *n_out,
                  tf_side_channels *side);
    void (*destroy)(struct tf_decoder *self);
    void  *state;
} tf_decoder;

/* ---- Encoder interface (batches → bytes) ---- */

typedef struct tf_encoder {
    /*
     * Encode a batch to bytes. Appends to the provided buffer.
     */
    int  (*encode)(struct tf_encoder *self, tf_batch *in, tf_buffer *out);
    /* Flush any trailing data. */
    int  (*flush)(struct tf_encoder *self, tf_buffer *out);
    void (*destroy)(struct tf_encoder *self);
    void  *state;
} tf_encoder;

/* ---- Pipeline struct ---- */

struct tf_pipeline {
    tf_decoder  *decoder;
    tf_step    **steps;
    size_t       n_steps;
    tf_step_run_stats *step_stats;
    size_t       n_step_stats;
    tf_encoder  *encoder;
    tf_buffer    output[TF_NUM_CHANNELS];
    tf_pipeline_sink_fn sinks[TF_NUM_CHANNELS];
    void        *sink_users[TF_NUM_CHANNELS];
    tf_pipeline_batch_sink_fn batch_sink;
    void        *batch_sink_user;
    int          encode_output;
    tf_pipeline_progress_fn progress_cb;
    void        *progress_user;
    size_t       progress_interval_rows;
    size_t       progress_last_rows_in;
    size_t       progress_last_rows_out;
    tf_side_channels side;
    size_t       rows_in;
    size_t       rows_out;
    size_t       batches_in;
    size_t       batches_out;
    size_t       bytes_in;
    size_t       bytes_out;
    char        *source_name;
    char        *error;
    int          finished;
    int          finish_started;
    int          finish_phase;
    tf_batch   **finish_batches;
    size_t       finish_n_batches;
    size_t       finish_batch_index;
    size_t       finish_step_index;
    int          finish_step_in_next;
};

/* ---- Codec constructors (used by plan parser) ---- */

tf_decoder *tf_csv_decoder_create(const cJSON *args);
tf_encoder *tf_csv_encoder_create(const cJSON *args);
tf_decoder *tf_jsonl_decoder_create(const cJSON *args);
tf_encoder *tf_jsonl_encoder_create(const cJSON *args);
tf_decoder *tf_text_decoder_create(const cJSON *args);
tf_encoder *tf_text_encoder_create(const cJSON *args);

/* ---- Transform constructors ---- */

tf_step *tf_filter_create(const cJSON *args);
tf_step *tf_select_create(const cJSON *args);
tf_step *tf_relocate_create(const cJSON *args);
tf_step *tf_rename_create(const cJSON *args);
tf_step *tf_head_create(const cJSON *args);
tf_step *tf_skip_create(const cJSON *args);
tf_step *tf_derive_create(const cJSON *args);
tf_step *tf_source_name_create(const cJSON *args);
tf_step *tf_across_create(const cJSON *args);
tf_step *tf_stats_create(const cJSON *args);
tf_step *tf_scan_create(const cJSON *args);
tf_step *tf_unique_create(const cJSON *args);
tf_step *tf_sort_create(const cJSON *args);
tf_step *tf_validate_create(const cJSON *args);
tf_step *tf_assert_create(const cJSON *args);
tf_step *tf_quarantine_create(const cJSON *args);
tf_step *tf_schema_create(const cJSON *args);
tf_step *tf_schema_infer_create(const cJSON *args);
tf_step *tf_tee_create(const cJSON *args);
tf_step *tf_trim_create(const cJSON *args);
tf_step *tf_fill_null_create(const cJSON *args);
tf_step *tf_cast_create(const cJSON *args);
tf_step *tf_clip_create(const cJSON *args);
tf_step *tf_replace_create(const cJSON *args);
tf_step *tf_hash_create(const cJSON *args);
tf_step *tf_bin_create(const cJSON *args);
tf_step *tf_fill_down_create(const cJSON *args);
tf_step *tf_step_create(const cJSON *args);
tf_step *tf_window_create(const cJSON *args);
tf_step *tf_rolling_sum_create(const cJSON *args);
tf_step *tf_rolling_mean_create(const cJSON *args);
tf_step *tf_rolling_min_create(const cJSON *args);
tf_step *tf_rolling_max_create(const cJSON *args);
tf_step *tf_rolling_any_create(const cJSON *args);
tf_step *tf_rolling_all_create(const cJSON *args);
tf_step *tf_explode_create(const cJSON *args);
tf_step *tf_split_create(const cJSON *args);
tf_step *tf_unpivot_create(const cJSON *args);
tf_step *tf_tail_create(const cJSON *args);
tf_step *tf_top_create(const cJSON *args);
tf_step *tf_top_k_create(const cJSON *args);
tf_step *tf_bottom_k_create(const cJSON *args);
tf_step *tf_slice_min_create(const cJSON *args);
tf_step *tf_slice_max_create(const cJSON *args);
tf_step *tf_sample_create(const cJSON *args);
tf_step *tf_group_agg_create(const cJSON *args);
tf_step *tf_frequency_create(const cJSON *args);
tf_step *tf_datetime_create(const cJSON *args);
tf_step *tf_grep_create(const cJSON *args);
tf_step *tf_json_extract_create(const cJSON *args);
tf_step *tf_json_filter_create(const cJSON *args);
tf_step *tf_json_schema_create(const cJSON *args);
tf_step *tf_json_flatten_create(const cJSON *args);
tf_step *tf_pivot_create(const cJSON *args);
tf_step *tf_join_create(const cJSON *args);
tf_step *tf_semi_join_create(const cJSON *args);
tf_step *tf_anti_join_create(const cJSON *args);
tf_step *tf_intersect_create(const cJSON *args);
tf_step *tf_setdiff_create(const cJSON *args);
tf_step *tf_intersect_all_create(const cJSON *args);
tf_step *tf_setdiff_all_create(const cJSON *args);
tf_step *tf_union_create(const cJSON *args);
tf_step *tf_union_all_create(const cJSON *args);
tf_step *tf_stack_create(const cJSON *args);
tf_step *tf_lead_create(const cJSON *args);
tf_step *tf_lag_create(const cJSON *args);
tf_step *tf_shift_create(const cJSON *args);
tf_step *tf_rowid_create(const cJSON *args);
tf_step *tf_rleid_create(const cJSON *args);
tf_step *tf_date_trunc_create(const cJSON *args);
tf_step *tf_onehot_create(const cJSON *args);
tf_step *tf_label_encode_create(const cJSON *args);
tf_step *tf_ewma_create(const cJSON *args);
tf_step *tf_diff_create(const cJSON *args);
tf_step *tf_anomaly_create(const cJSON *args);
tf_step *tf_split_data_create(const cJSON *args);
tf_step *tf_interpolate_create(const cJSON *args);
tf_step *tf_normalize_create(const cJSON *args);
tf_step *tf_acf_create(const cJSON *args);

/* ---- Table encoder ---- */

tf_encoder *tf_table_encoder_create(const cJSON *args);

/* ---- Expression evaluator ---- */

typedef struct tf_expr tf_expr;

tf_expr    *tf_expr_parse(const char *text);
int         tf_expr_eval(const tf_expr *e, const tf_batch *batch, size_t row, bool *result);
int         tf_expr_eval_val(const tf_expr *e, const tf_batch *batch, size_t row,
                             tf_eval_result *result);
void        tf_expr_free(tf_expr *e);

/* ---- Global error ---- */

void tf_set_last_error(const char *msg);

#endif /* TF_INTERNAL_H */
