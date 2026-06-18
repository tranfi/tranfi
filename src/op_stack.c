/*
 * op_stack.c — Vertically concatenate a second CSV file into the stream.
 *
 * Passes through all input batches, then on flush reads and appends
 * rows from a second CSV file. Optionally adds a tag column to
 * identify the source.
 *
 * Args:
 *   file (string, required) — path to CSV file to append
 *   tag (string, optional) — name of source-identifying column
 *   tag_value (string, optional) — value for tag column on appended rows
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char *file_path;
    char *tag_col;       /* NULL if no tag */
    char *tag_value;     /* value for appended rows */
    char *tag_value_in;  /* value for passthrough rows (filename or "input") */
    FILE *file;
    tf_decoder *decoder;
    tf_batch **pending_batches;
    size_t n_pending_batches;
    size_t pending_batch_index;
    int file_started;
    int file_done;
} stack_state;

static void stack_clear_pending(stack_state *st) {
    if (!st || !st->pending_batches) return;
    tf_batch_array_free_items(st->pending_batches + st->pending_batch_index,
                              st->n_pending_batches - st->pending_batch_index);
    free(st->pending_batches);
    st->pending_batches = NULL;
    st->n_pending_batches = 0;
    st->pending_batch_index = 0;
}

static void stack_close_file_reader(stack_state *st) {
    if (!st) return;
    stack_clear_pending(st);
    if (st->decoder) {
        st->decoder->destroy(st->decoder);
        st->decoder = NULL;
    }
    if (st->file) {
        fclose(st->file);
        st->file = NULL;
    }
}

static void stack_state_free(stack_state *st) {
    if (!st) return;
    stack_close_file_reader(st);
    free(st->file_path);
    free(st->tag_col);
    free(st->tag_value);
    free(st->tag_value_in);
    free(st);
}

static void stack_destroy(tf_step *self) {
    if (self) stack_state_free(self->state);
    free(self);
}

static int stack_write_error(tf_side_channels *side, const char *msg) {
    return tf_side_write_error(side, msg);
}

static int stack_open_file_reader(stack_state *st, tf_side_channels *side) {
    if (st->file_started) return TF_OK;
    st->file_started = 1;
    st->file = fopen(st->file_path, "rb");
    if (!st->file) {
        stack_write_error(side, "stack: cannot open file");
        return TF_ERROR;
    }
    st->decoder = tf_csv_decoder_create(NULL);
    if (!st->decoder) {
        stack_close_file_reader(st);
        return TF_ERROR;
    }
    return TF_OK;
}

static int stack_take_pending_batch(stack_state *st, tf_batch **out) {
    if (!st->pending_batches || st->pending_batch_index >= st->n_pending_batches) {
        stack_clear_pending(st);
        return 0;
    }
    *out = st->pending_batches[st->pending_batch_index];
    st->pending_batches[st->pending_batch_index] = NULL;
    st->pending_batch_index++;
    if (st->pending_batch_index >= st->n_pending_batches) stack_clear_pending(st);
    return 1;
}

static int stack_next_decoded_file_batch(stack_state *st, tf_batch **out,
                                         tf_side_channels *side) {
    *out = NULL;
    if (stack_take_pending_batch(st, out)) return TF_OK;
    if (st->file_done) return TF_OK;
    if (stack_open_file_reader(st, side) != TF_OK) return TF_ERROR;

    for (;;) {
        uint8_t buf[64 * 1024];
        size_t n = fread(buf, 1, sizeof(buf), st->file);
        tf_batch **batches = NULL;
        size_t n_batches = 0;
        int rc = TF_OK;

        if (n > 0) {
            rc = st->decoder->decode(st->decoder, buf, n, &batches, &n_batches, side);
        } else {
            if (ferror(st->file)) {
                stack_write_error(side, "stack: failed reading file");
                stack_close_file_reader(st);
                st->file_done = 1;
                return TF_ERROR;
            }
            rc = st->decoder->flush(st->decoder, &batches, &n_batches, side);
            st->file_done = 1;
            stack_close_file_reader(st);
        }
        if (rc != TF_OK) {
            tf_batch_array_free(batches, n_batches);
            stack_close_file_reader(st);
            st->file_done = 1;
            return TF_ERROR;
        }
        st->pending_batches = batches;
        st->n_pending_batches = n_batches;
        st->pending_batch_index = 0;
        if (stack_take_pending_batch(st, out)) return TF_OK;
        if (st->file_done) return TF_OK;
    }
}

/*
 * Add tag column to a batch. Returns a new batch with the tag column prepended.
 */
static tf_batch *add_tag_column(tf_batch *in, const char *tag_col, const char *tag_value) {
    size_t out_cols = 0;
    if (tf_size_add(in->n_cols, 1, &out_cols) != TF_OK) return NULL;
    tf_batch *out = tf_batch_create(out_cols, in->n_rows);
    if (!out) return NULL;

    if (tf_batch_set_schema(out, 0, tag_col, TF_TYPE_STRING) != TF_OK) goto fail;
    for (size_t c = 0; c < in->n_cols; c++) {
        if (tf_batch_set_schema(out, c + 1, in->col_names[c], in->col_types[c]) != TF_OK) goto fail;
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_ensure_capacity(out, r + 1) != TF_OK) goto fail;
        if (tf_batch_set_string(out, r, 0, tag_value ? tag_value : "") != TF_OK) goto fail;
        for (size_t c = 0; c < in->n_cols; c++) {
            if (tf_batch_copy_cell(out, r, c + 1, in, r, c) != TF_OK) goto fail;
        }
        if (tf_batch_expose_row(out, r) != TF_OK) goto fail;
    }
    return out;

fail:
    tf_batch_free(out);
    return NULL;
}

static int stack_process(tf_step *self, tf_batch *in, tf_batch **out,
                         tf_side_channels *side) {
    stack_state *st = self->state;
    (void)side;
    *out = NULL;

    if (st->tag_col) {
        *out = add_tag_column(in, st->tag_col, st->tag_value_in);
        return *out ? TF_OK : TF_ERROR;
    }

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) return TF_ERROR;
    if (tf_batch_clone_schema(ob, in) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }
    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }
    *out = ob;
    return TF_OK;
}

static int stack_flush_next(tf_step *self, tf_batch **out, tf_side_channels *side) {
    stack_state *st = self->state;
    *out = NULL;

    for (;;) {
        tf_batch *batch = NULL;
        if (stack_next_decoded_file_batch(st, &batch, side) != TF_OK) return TF_ERROR;
        if (!batch) return TF_OK;
        if (batch->n_rows == 0) {
            tf_batch_free(batch);
            continue;
        }
        if (!st->tag_col) {
            *out = batch;
            return TF_OK;
        }
        *out = add_tag_column(batch, st->tag_col, st->tag_value);
        tf_batch_free(batch);
        return *out ? TF_OK : TF_ERROR;
    }
}

static int stack_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    return stack_flush_next(self, out, side);
}

tf_step *tf_stack_create(const cJSON *args) {
    if (!args) return NULL;

    cJSON *file = cJSON_GetObjectItemCaseSensitive(args, "file");
    if (!cJSON_IsString(file) || !file->valuestring[0]) return NULL;

    stack_state *st = calloc(1, sizeof(stack_state));
    if (!st) return NULL;

    st->file_path = strdup(file->valuestring);
    if (!st->file_path) { stack_state_free(st); return NULL; }

    cJSON *tag = cJSON_GetObjectItemCaseSensitive(args, "tag");
    if (cJSON_IsString(tag) && tag->valuestring[0]) {
        st->tag_col = strdup(tag->valuestring);
        cJSON *tv = cJSON_GetObjectItemCaseSensitive(args, "tag_value");
        st->tag_value = strdup(cJSON_IsString(tv) ? tv->valuestring : st->file_path);
        st->tag_value_in = strdup("input");
        if (!st->tag_col || !st->tag_value || !st->tag_value_in) {
            stack_state_free(st);
            return NULL;
        }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { stack_state_free(st); return NULL; }
    step->process = stack_process;
    step->flush = stack_flush;
    step->flush_next = stack_flush_next;
    step->destroy = stack_destroy;
    step->state = st;
    return step;
}
