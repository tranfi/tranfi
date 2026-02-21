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

static void stack_destroy(tf_step *self) {
    stack_state *st = self->state;
    if (st) {
        free(st->file_path);
        free(st->tag_col);
        free(st->tag_value);
        free(st->tag_value_in);
        if (st->col_names) {
            for (size_t i = 0; i < st->n_cols; i++) free(st->col_names[i]);
            free(st->col_names);
        }
        free(st->col_types);
        free(st);
    }
    free(self);
}

/*
 * Add tag column to a batch. Returns a new batch with the tag column prepended.
 */
static tf_batch *add_tag_column(tf_batch *in, const char *tag_col, const char *tag_value) {
    tf_batch *out = tf_batch_create(in->n_cols + 1, in->n_rows);
    if (!out) return NULL;

    /* First column is the tag */
    tf_batch_set_schema(out, 0, tag_col, TF_TYPE_STRING);
    /* Copy remaining columns */
    for (size_t c = 0; c < in->n_cols; c++) {
        tf_batch_set_schema(out, c + 1, in->col_names[c], in->col_types[c]);
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_ensure_capacity(out, r + 1);
        tf_batch_set_string(out, r, 0, tag_value);
        for (size_t c = 0; c < in->n_cols; c++) {
            if (tf_batch_is_null(in, r, c)) {
                tf_batch_set_null(out, r, c + 1);
            } else {
                switch (in->col_types[c]) {
                    case TF_TYPE_INT64:
                        tf_batch_set_int64(out, r, c + 1, tf_batch_get_int64(in, r, c));
                        break;
                    case TF_TYPE_FLOAT64:
                        tf_batch_set_float64(out, r, c + 1, tf_batch_get_float64(in, r, c));
                        break;
                    case TF_TYPE_STRING:
                        tf_batch_set_string(out, r, c + 1, tf_batch_get_string(in, r, c));
                        break;
                    case TF_TYPE_BOOL:
                        tf_batch_set_bool(out, r, c + 1, tf_batch_get_bool(in, r, c));
                        break;
                    case TF_TYPE_DATE:
                        tf_batch_set_date(out, r, c + 1, tf_batch_get_date(in, r, c));
                        break;
                    case TF_TYPE_TIMESTAMP:
                        tf_batch_set_timestamp(out, r, c + 1, tf_batch_get_timestamp(in, r, c));
                        break;
                    default:
                        tf_batch_set_null(out, r, c + 1);
                        break;
                }
            }
        }
        out->n_rows = r + 1;
    }
    return out;
}

static int stack_process(tf_step *self, tf_batch *in, tf_batch **out,
                         tf_side_channels *side) {
    stack_state *st = self->state;
    (void)side;

    /* Capture schema from first batch */
    if (!st->schema_captured && in->n_cols > 0) {
        st->n_cols = in->n_cols;
        st->col_names = malloc(in->n_cols * sizeof(char *));
        st->col_types = malloc(in->n_cols * sizeof(tf_type));
        for (size_t i = 0; i < in->n_cols; i++) {
            st->col_names[i] = strdup(in->col_names[i]);
            st->col_types[i] = in->col_types[i];
        }
        st->schema_captured = 1;
    }

    if (st->tag_col) {
        *out = add_tag_column(in, st->tag_col, st->tag_value_in);
    } else {
        /* Clone input batch */
        tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
        if (!ob) { *out = NULL; return TF_ERROR; }
        for (size_t c = 0; c < in->n_cols; c++)
            tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
        for (size_t r = 0; r < in->n_rows; r++) {
            tf_batch_ensure_capacity(ob, r + 1);
            tf_batch_copy_row(ob, r, in, r);
            ob->n_rows = r + 1;
        }
        *out = ob;
    }
    return TF_OK;
}

static int stack_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    stack_state *st = self->state;
    (void)side;
    *out = NULL;

    /* Read the file */
    FILE *f = fopen(st->file_path, "rb");
    if (!f) return TF_OK; /* silently skip if file not found */

    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (fsize <= 0) { fclose(f); return TF_OK; }

    char *data = malloc((size_t)fsize + 1);
    if (!data) { fclose(f); return TF_ERROR; }
    size_t nread = fread(data, 1, (size_t)fsize, f);
    fclose(f);
    data[nread] = '\0';

    /* Parse CSV: use a sub-pipeline to decode the file */
    /* Simple approach: parse line by line */
    char **file_col_names = NULL;
    size_t file_n_cols = 0;

    /* Count rows and parse */
    size_t n_rows = 0;
    char *line = data;
    char *end = data + nread;

    /* First pass: count data lines */
    char *p = data;
    while (p < end) {
        char *nl = memchr(p, '\n', (size_t)(end - p));
        if (!nl) nl = end;
        if (nl > p || p < end) n_rows++;
        p = nl + 1;
    }
    if (n_rows == 0) { free(data); return TF_OK; }
    n_rows--; /* subtract header */

    /* Parse header */
    line = data;
    char *nl = memchr(line, '\n', (size_t)(end - line));
    if (!nl) nl = end;
    size_t hdr_len = (size_t)(nl - line);
    if (hdr_len > 0 && line[hdr_len - 1] == '\r') hdr_len--;

    /* Count commas to get column count */
    file_n_cols = 1;
    for (size_t i = 0; i < hdr_len; i++) {
        if (line[i] == ',') file_n_cols++;
    }

    /* Parse column names */
    file_col_names = malloc(file_n_cols * sizeof(char *));
    size_t ci = 0;
    size_t field_start = 0;
    for (size_t i = 0; i <= hdr_len; i++) {
        if (i == hdr_len || line[i] == ',') {
            size_t flen = i - field_start;
            /* Trim whitespace */
            while (flen > 0 && (line[field_start] == ' ' || line[field_start] == '\t'))
                { field_start++; flen--; }
            while (flen > 0 && (line[field_start + flen - 1] == ' ' || line[field_start + flen - 1] == '\t'))
                flen--;
            file_col_names[ci] = malloc(flen + 1);
            memcpy(file_col_names[ci], line + field_start, flen);
            file_col_names[ci][flen] = '\0';
            ci++;
            field_start = i + 1;
        }
    }

    /* Create output batch — use schema from input or file */
    size_t out_cols = st->tag_col ? file_n_cols + 1 : file_n_cols;
    tf_batch *ob = tf_batch_create(out_cols, n_rows > 0 ? n_rows : 1);
    if (!ob) {
        for (size_t i = 0; i < file_n_cols; i++) free(file_col_names[i]);
        free(file_col_names);
        free(data);
        return TF_ERROR;
    }

    size_t col_offset = 0;
    if (st->tag_col) {
        tf_batch_set_schema(ob, 0, st->tag_col, TF_TYPE_STRING);
        col_offset = 1;
    }
    for (size_t i = 0; i < file_n_cols; i++) {
        tf_batch_set_schema(ob, i + col_offset, file_col_names[i], TF_TYPE_STRING);
    }

    /* Parse data rows */
    p = nl + 1; /* skip past header newline */
    size_t row = 0;
    while (p < end && row < n_rows) {
        nl = memchr(p, '\n', (size_t)(end - p));
        if (!nl) nl = end;
        size_t line_len = (size_t)(nl - p);
        if (line_len > 0 && p[line_len - 1] == '\r') line_len--;
        if (line_len == 0) { p = nl + 1; continue; }

        tf_batch_ensure_capacity(ob, row + 1);

        if (st->tag_col) {
            tf_batch_set_string(ob, row, 0, st->tag_value);
        }

        /* Parse fields */
        size_t fc = 0;
        size_t fs = 0;
        for (size_t i = 0; i <= line_len; i++) {
            if (i == line_len || p[i] == ',') {
                if (fc < file_n_cols) {
                    size_t flen = i - fs;
                    /* Trim */
                    const char *fp = p + fs;
                    while (flen > 0 && (*fp == ' ' || *fp == '\t')) { fp++; flen--; }
                    while (flen > 0 && (fp[flen - 1] == ' ' || fp[flen - 1] == '\t')) flen--;
                    if (flen == 0) {
                        tf_batch_set_null(ob, row, fc + col_offset);
                    } else {
                        char tmp[4096];
                        size_t clen = flen < sizeof(tmp) - 1 ? flen : sizeof(tmp) - 1;
                        memcpy(tmp, fp, clen);
                        tmp[clen] = '\0';
                        tf_batch_set_string(ob, row, fc + col_offset, tmp);
                    }
                    fc++;
                }
                fs = i + 1;
            }
        }
        /* Null-fill remaining columns */
        for (size_t c = fc; c < file_n_cols; c++) {
            tf_batch_set_null(ob, row, c + col_offset);
        }

        ob->n_rows = row + 1;
        row++;
        p = nl + 1;
    }

    for (size_t i = 0; i < file_n_cols; i++) free(file_col_names[i]);
    free(file_col_names);
    free(data);

    *out = ob;
    return TF_OK;
}

tf_step *tf_stack_create(const cJSON *args) {
    if (!args) return NULL;

    cJSON *file = cJSON_GetObjectItemCaseSensitive(args, "file");
    if (!cJSON_IsString(file) || !file->valuestring[0]) return NULL;

    stack_state *st = calloc(1, sizeof(stack_state));
    if (!st) return NULL;

    st->file_path = strdup(file->valuestring);

    cJSON *tag = cJSON_GetObjectItemCaseSensitive(args, "tag");
    if (cJSON_IsString(tag) && tag->valuestring[0]) {
        st->tag_col = strdup(tag->valuestring);
        cJSON *tv = cJSON_GetObjectItemCaseSensitive(args, "tag_value");
        st->tag_value = strdup(cJSON_IsString(tv) ? tv->valuestring : st->file_path);
        st->tag_value_in = strdup("input");
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { free(st->file_path); free(st->tag_col); free(st->tag_value); free(st->tag_value_in); free(st); return NULL; }
    step->process = stack_process;
    step->flush = stack_flush;
    step->destroy = stack_destroy;
    step->state = st;
    return step;
}
