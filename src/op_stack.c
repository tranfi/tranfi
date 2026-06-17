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

    /* Schema from first input batch */
    char **col_names;
    tf_type *col_types;
    size_t n_cols;
    int schema_captured;
    int has_tag;         /* whether tag column was added */
} stack_state;

static void stack_clear_schema(stack_state *st) {
    if (!st) return;
    if (st->col_names) {
        for (size_t i = 0; i < st->n_cols; i++) free(st->col_names[i]);
        free(st->col_names);
    }
    free(st->col_types);
    st->col_names = NULL;
    st->col_types = NULL;
    st->n_cols = 0;
    st->schema_captured = 0;
}

static void stack_state_free(stack_state *st) {
    if (!st) return;
    free(st->file_path);
    free(st->tag_col);
    free(st->tag_value);
    free(st->tag_value_in);
    stack_clear_schema(st);
    free(st);
}

static void stack_destroy(tf_step *self) {
    if (self) stack_state_free(self->state);
    free(self);
}

static int stack_capture_schema(stack_state *st, const tf_batch *in) {
    if (!st || !in) return TF_ERROR;
    if (st->schema_captured || in->n_cols == 0) return TF_OK;

    st->n_cols = in->n_cols;
    st->col_names = calloc(in->n_cols, sizeof(char *));
    st->col_types = calloc(in->n_cols, sizeof(tf_type));
    if (!st->col_names || !st->col_types) {
        stack_clear_schema(st);
        return TF_ERROR;
    }
    for (size_t i = 0; i < in->n_cols; i++) {
        st->col_names[i] = strdup(in->col_names[i] ? in->col_names[i] : "");
        if (!st->col_names[i]) {
            stack_clear_schema(st);
            return TF_ERROR;
        }
        st->col_types[i] = in->col_types[i];
    }
    st->schema_captured = 1;
    return TF_OK;
}

static void stack_free_file_col_names(char **names, size_t n) {
    if (!names) return;
    for (size_t i = 0; i < n; i++) free(names[i]);
    free(names);
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

    if (stack_capture_schema(st, in) != TF_OK) return TF_ERROR;

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

static int stack_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    stack_state *st = self->state;
    (void)side;
    *out = NULL;

    FILE *f = fopen(st->file_path, "rb");
    if (!f) return TF_OK; /* silently skip if file not found */

    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return TF_ERROR; }
    long fsize = ftell(f);
    if (fseek(f, 0, SEEK_SET) != 0) { fclose(f); return TF_ERROR; }
    if (fsize <= 0) { fclose(f); return TF_OK; }

    char *data = malloc((size_t)fsize + 1);
    if (!data) { fclose(f); return TF_ERROR; }
    size_t nread = fread(data, 1, (size_t)fsize, f);
    int read_error = ferror(f);
    fclose(f);
    if (read_error) { free(data); return TF_ERROR; }
    data[nread] = '\0';

    char **file_col_names = NULL;
    size_t file_n_cols = 0;
    tf_batch *ob = NULL;
    int rc = TF_ERROR;

    size_t n_rows = 0;
    char *end = data + nread;

    char *p = data;
    while (p < end) {
        char *nl = memchr(p, '\n', (size_t)(end - p));
        if (!nl) nl = end;
        if (nl > p || p < end) n_rows++;
        p = (nl < end) ? nl + 1 : end;
    }
    if (n_rows == 0) { rc = TF_OK; goto done; }
    n_rows--; /* subtract header */

    char *line = data;
    char *nl = memchr(line, '\n', (size_t)(end - line));
    if (!nl) nl = end;
    size_t hdr_len = (size_t)(nl - line);
    if (hdr_len > 0 && line[hdr_len - 1] == '\r') hdr_len--;

    file_n_cols = 1;
    for (size_t i = 0; i < hdr_len; i++) {
        if (line[i] == ',') file_n_cols++;
    }

    file_col_names = calloc(file_n_cols, sizeof(char *));
    if (!file_col_names) goto done;
    size_t ci = 0;
    size_t field_start = 0;
    for (size_t i = 0; i <= hdr_len; i++) {
        if (i == hdr_len || line[i] == ',') {
            if (ci >= file_n_cols) goto done;
            size_t flen = i - field_start;
            while (flen > 0 && (line[field_start] == ' ' || line[field_start] == '\t'))
                { field_start++; flen--; }
            while (flen > 0 && (line[field_start + flen - 1] == ' ' || line[field_start + flen - 1] == '\t'))
                flen--;
            file_col_names[ci] = malloc(flen + 1);
            if (!file_col_names[ci]) goto done;
            memcpy(file_col_names[ci], line + field_start, flen);
            file_col_names[ci][flen] = '\0';
            ci++;
            field_start = i + 1;
        }
    }
    if (ci != file_n_cols) goto done;

    size_t out_cols = file_n_cols;
    size_t col_offset = 0;
    if (st->tag_col) {
        if (tf_size_add(file_n_cols, 1, &out_cols) != TF_OK) goto done;
        col_offset = 1;
    }
    ob = tf_batch_create(out_cols, n_rows > 0 ? n_rows : 1);
    if (!ob) goto done;

    if (st->tag_col) {
        if (tf_batch_set_schema(ob, 0, st->tag_col, TF_TYPE_STRING) != TF_OK) goto done;
    }
    for (size_t i = 0; i < file_n_cols; i++) {
        if (tf_batch_set_schema(ob, i + col_offset, file_col_names[i], TF_TYPE_STRING) != TF_OK) goto done;
    }

    p = (nl < end) ? nl + 1 : end;
    size_t row = 0;
    while (p < end && row < n_rows) {
        nl = memchr(p, '\n', (size_t)(end - p));
        if (!nl) nl = end;
        size_t line_len = (size_t)(nl - p);
        if (line_len > 0 && p[line_len - 1] == '\r') line_len--;
        if (line_len == 0) { p = (nl < end) ? nl + 1 : end; continue; }

        if (tf_batch_ensure_capacity(ob, row + 1) != TF_OK) goto done;

        if (st->tag_col) {
            if (tf_batch_set_string(ob, row, 0, st->tag_value ? st->tag_value : "") != TF_OK) goto done;
        }

        size_t fc = 0;
        size_t fs = 0;
        for (size_t i = 0; i <= line_len; i++) {
            if (i == line_len || p[i] == ',') {
                if (fc < file_n_cols) {
                    size_t flen = i - fs;
                    const char *fp = p + fs;
                    while (flen > 0 && (*fp == ' ' || *fp == '\t')) { fp++; flen--; }
                    while (flen > 0 && (fp[flen - 1] == ' ' || fp[flen - 1] == '\t')) flen--;
                    if (flen == 0) {
                        if (tf_batch_set_null(ob, row, fc + col_offset) != TF_OK) goto done;
                    } else {
                        if (tf_batch_set_string_len(ob, row, fc + col_offset, fp, flen) != TF_OK) goto done;
                    }
                    fc++;
                }
                fs = i + 1;
            }
        }
        for (size_t c = fc; c < file_n_cols; c++) {
            if (tf_batch_set_null(ob, row, c + col_offset) != TF_OK) goto done;
        }

        if (tf_batch_expose_row(ob, row) != TF_OK) goto done;
        row++;
        p = (nl < end) ? nl + 1 : end;
    }

    *out = ob;
    ob = NULL;
    rc = TF_OK;

done:
    if (ob) tf_batch_free(ob);
    stack_free_file_col_names(file_col_names, file_n_cols);
    free(data);
    return rc;
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
    step->destroy = stack_destroy;
    step->state = st;
    return step;
}
