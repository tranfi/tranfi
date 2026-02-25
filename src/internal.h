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

void   tf_buffer_init(tf_buffer *b);
int    tf_buffer_write(tf_buffer *b, const uint8_t *data, size_t len);
size_t tf_buffer_read(tf_buffer *b, uint8_t *out, size_t len);
size_t tf_buffer_readable(const tf_buffer *b);
void   tf_buffer_compact(tf_buffer *b);
int    tf_buffer_write_str(tf_buffer *b, const char *s);
void   tf_buffer_free(tf_buffer *b);

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
} tf_batch;

tf_batch *tf_batch_create(size_t n_cols, size_t capacity);
int       tf_batch_set_schema(tf_batch *b, size_t col, const char *name, tf_type type);
int       tf_batch_ensure_capacity(tf_batch *b, size_t min_rows);

/* Set a value in a specific cell. String values are copied into the arena. */
void tf_batch_set_null(tf_batch *b, size_t row, size_t col);
void tf_batch_set_bool(tf_batch *b, size_t row, size_t col, bool val);
void tf_batch_set_int64(tf_batch *b, size_t row, size_t col, int64_t val);
void tf_batch_set_float64(tf_batch *b, size_t row, size_t col, double val);
void tf_batch_set_string(tf_batch *b, size_t row, size_t col, const char *val);
void tf_batch_set_date(tf_batch *b, size_t row, size_t col, int32_t val);
void tf_batch_set_timestamp(tf_batch *b, size_t row, size_t col, int64_t val);

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

/* Copy a single row from src to dst batch. */
int tf_batch_copy_row(tf_batch *dst, size_t dst_row,
                      const tf_batch *src, size_t src_row);

void tf_batch_free(tf_batch *b);

/* ---- Step interface (transforms) ---- */

/*
 * Side-channel callback: steps call this to emit data to side channels.
 * Owned by the pipeline and passed to steps during processing.
 */
typedef struct tf_side_channels {
    tf_buffer *errors;
    tf_buffer *stats;
    tf_buffer *samples;
} tf_side_channels;

typedef struct tf_step {
    /* Process one input batch, produce zero or one output batch.
     * *out is set to a new batch (caller frees) or NULL if filtered away. */
    int  (*process)(struct tf_step *self, tf_batch *in, tf_batch **out,
                    tf_side_channels *side);
    /* Flush any buffered state. */
    int  (*flush)(struct tf_step *self, tf_batch **out, tf_side_channels *side);
    /* Free all resources. */
    void (*destroy)(struct tf_step *self);
    void  *state;
} tf_step;

/* ---- Decoder interface (bytes → batches) ---- */

typedef struct tf_decoder {
    /*
     * Decode bytes. May produce 0..N batches.
     * *out is set to a malloc'd array of tf_batch*, *n_out to its length.
     * Caller frees the array and each batch.
     */
    int  (*decode)(struct tf_decoder *self, const uint8_t *data, size_t len,
                   tf_batch ***out, size_t *n_out);
    /* Flush remaining data (e.g. last partial line). */
    int  (*flush)(struct tf_decoder *self, tf_batch ***out, size_t *n_out);
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
    tf_encoder  *encoder;
    tf_buffer    output[TF_NUM_CHANNELS];
    tf_side_channels side;
    size_t       rows_in;
    size_t       rows_out;
    size_t       bytes_in;
    size_t       bytes_out;
    char        *error;
    int          finished;
};

/* ---- Codec constructors (used by plan parser) ---- */

typedef struct cJSON cJSON;

tf_decoder *tf_csv_decoder_create(const cJSON *args);
tf_encoder *tf_csv_encoder_create(const cJSON *args);
tf_decoder *tf_jsonl_decoder_create(const cJSON *args);
tf_encoder *tf_jsonl_encoder_create(const cJSON *args);
tf_decoder *tf_text_decoder_create(const cJSON *args);
tf_encoder *tf_text_encoder_create(const cJSON *args);

/* ---- Transform constructors ---- */

tf_step *tf_filter_create(const cJSON *args);
tf_step *tf_select_create(const cJSON *args);
tf_step *tf_rename_create(const cJSON *args);
tf_step *tf_head_create(const cJSON *args);
tf_step *tf_skip_create(const cJSON *args);
tf_step *tf_derive_create(const cJSON *args);
tf_step *tf_stats_create(const cJSON *args);
tf_step *tf_unique_create(const cJSON *args);
tf_step *tf_sort_create(const cJSON *args);
tf_step *tf_validate_create(const cJSON *args);
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
tf_step *tf_explode_create(const cJSON *args);
tf_step *tf_split_create(const cJSON *args);
tf_step *tf_unpivot_create(const cJSON *args);
tf_step *tf_tail_create(const cJSON *args);
tf_step *tf_top_create(const cJSON *args);
tf_step *tf_sample_create(const cJSON *args);
tf_step *tf_group_agg_create(const cJSON *args);
tf_step *tf_frequency_create(const cJSON *args);
tf_step *tf_datetime_create(const cJSON *args);
tf_step *tf_grep_create(const cJSON *args);
tf_step *tf_pivot_create(const cJSON *args);
tf_step *tf_join_create(const cJSON *args);
tf_step *tf_stack_create(const cJSON *args);
tf_step *tf_lead_create(const cJSON *args);
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

/* ---- Plan parser ---- */

int tf_plan_parse(const char *json, size_t len,
                  tf_decoder **decoder, tf_step ***steps, size_t *n_steps,
                  tf_encoder **encoder, char **error);

/* ---- Global error ---- */

void tf_set_last_error(const char *msg);

#endif /* TF_INTERNAL_H */
