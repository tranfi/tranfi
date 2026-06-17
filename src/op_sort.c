/*
 * op_sort.c -- Sort rows by column(s).
 *
 * Default mode preserves the historical in-memory/blocking sort. When the
 * args contain spill_dir, the operator writes memory-sized sorted runs to
 * temporary files and merges them at flush in bounded output batches.
 */

#include "internal.h"
#include "spill.h"
#include "cJSON.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define SORT_DEFAULT_RUN_ROWS 8192
#define SORT_DEFAULT_OUTPUT_ROWS 1024
#define SORT_MIN_RUN_ROWS 16

/* Sort spec. */
typedef struct {
    char *name;
    int   desc;
} sort_col;

typedef tf_owned_cell_value sort_cell;

typedef struct {
    uint8_t *nulls;
    sort_cell *cells;
} sort_spill_row;

typedef struct {
    FILE *file;
    sort_spill_row row;
    int has_row;
    int done;
} sort_run_reader;

typedef struct {
    tf_batch *buf;
    int       has_schema;
    char    **schema_names;
    tf_type  *schema_types;
    size_t    n_schema_cols;

    sort_col *cols;
    size_t    n_cols;
    int      *col_indices;
    int      *col_desc;

    int       use_spill;
    char     *spill_dir;
    tf_spill_session *spill;
    size_t    spill_memory_bytes;
    size_t    configured_run_rows;
    size_t    run_rows;
    size_t    output_batch_rows;

    char    **run_paths;
    size_t    n_runs;
    size_t    cap_runs;
    size_t    run_seq;
    size_t    spilled_bytes;
    size_t    spill_runs_created;
    size_t    spill_output_batches;
    size_t    spill_output_rows;

    sort_run_reader *readers;
    size_t    n_readers;
    int       merge_started;
    int       merge_done;
} sort_state;

static tf_batch *create_buffer_from_schema(const sort_state *st, size_t capacity) {
    tf_batch *b = tf_batch_create(st->n_schema_cols, capacity ? capacity : 16);
    if (!b) return NULL;
    for (size_t c = 0; c < st->n_schema_cols; c++) {
        if (tf_batch_set_schema(b, c, st->schema_names[c], st->schema_types[c]) != TF_OK) {
            tf_batch_free(b);
            return NULL;
        }
    }
    return b;
}

static size_t sort_estimated_row_bytes(const sort_state *st) {
    size_t bytes = 32;
    for (size_t c = 0; c < st->n_schema_cols; c++) {
        bytes += 1;
        switch (st->schema_types[c]) {
            case TF_TYPE_BOOL: bytes += 1; break;
            case TF_TYPE_INT64: bytes += sizeof(int64_t); break;
            case TF_TYPE_FLOAT64: bytes += sizeof(double); break;
            case TF_TYPE_STRING: bytes += sizeof(char *) + 64; break;
            case TF_TYPE_DATE: bytes += sizeof(int32_t); break;
            case TF_TYPE_TIMESTAMP: bytes += sizeof(int64_t); break;
            default: break;
        }
    }
    return bytes < 64 ? 64 : bytes;
}

static int resolve_sort_columns(sort_state *st) {
    free(st->col_indices);
    free(st->col_desc);
    st->col_indices = tf_callocarray_checked(st->n_cols ? st->n_cols : 1, sizeof(int));
    st->col_desc = tf_callocarray_checked(st->n_cols ? st->n_cols : 1, sizeof(int));
    if (!st->col_indices || !st->col_desc) return TF_ERROR;
    for (size_t k = 0; k < st->n_cols; k++) {
        int idx = tf_batch_col_index(st->buf, st->cols[k].name);
        if (idx < 0) {
            char msg[256];
            snprintf(msg, sizeof(msg), "sort: column '%s' not found",
                     st->cols[k].name ? st->cols[k].name : "");
            tf_set_last_error(msg);
            return TF_ERROR;
        }
        st->col_indices[k] = idx;
        st->col_desc[k] = st->cols[k].desc;
    }
    return TF_OK;
}

static int init_schema(sort_state *st, const tf_batch *in) {
    if (st->has_schema) return TF_OK;

    st->n_schema_cols = in->n_cols;
    st->schema_names = tf_callocarray_checked(in->n_cols ? in->n_cols : 1, sizeof(char *));
    st->schema_types = tf_callocarray_checked(in->n_cols ? in->n_cols : 1, sizeof(tf_type));
    if (!st->schema_names || !st->schema_types) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++) {
        st->schema_names[c] = strdup(in->col_names[c] ? in->col_names[c] : "");
        if (!st->schema_names[c]) return TF_ERROR;
        st->schema_types[c] = in->col_types[c];
    }

    size_t initial = in->n_rows > 0 ? in->n_rows : 16;
    if (st->use_spill) {
        if (st->configured_run_rows > 0) {
            st->run_rows = st->configured_run_rows;
        } else if (st->spill_memory_bytes > 0) {
            size_t row_bytes = sort_estimated_row_bytes(st);
            st->run_rows = st->spill_memory_bytes / (row_bytes * 3);
            if (st->run_rows < SORT_MIN_RUN_ROWS) st->run_rows = SORT_MIN_RUN_ROWS;
        } else {
            st->run_rows = SORT_DEFAULT_RUN_ROWS;
        }
        initial = st->run_rows;
    }

    st->buf = create_buffer_from_schema(st, initial);
    if (!st->buf) return TF_ERROR;
    st->has_schema = 1;
    return resolve_sort_columns(st);
}

typedef struct {
    const tf_batch *batch;
    int            *col_indices;
    int            *col_desc;
    size_t          n_sort_cols;
} sort_ctx;

static int sort_compare_rows(const sort_ctx *ctx, size_t ra, size_t rb) {
    const tf_batch *batch = ctx->batch;

    for (size_t k = 0; k < ctx->n_sort_cols; k++) {
        int ci = ctx->col_indices[k];

        int null_a = tf_batch_is_null(batch, ra, ci);
        int null_b = tf_batch_is_null(batch, rb, ci);
        if (null_a && null_b) continue;
        if (null_a) return 1;
        if (null_b) return -1;

        int cmp = 0;
        switch (batch->col_types[ci]) {
            case TF_TYPE_INT64: {
                int64_t va = tf_batch_get_int64(batch, ra, ci);
                int64_t vb = tf_batch_get_int64(batch, rb, ci);
                cmp = (va > vb) - (va < vb);
                break;
            }
            case TF_TYPE_FLOAT64: {
                double va = tf_batch_get_float64(batch, ra, ci);
                double vb = tf_batch_get_float64(batch, rb, ci);
                cmp = (va > vb) - (va < vb);
                break;
            }
            case TF_TYPE_STRING:
                cmp = strcmp(tf_batch_get_string(batch, ra, ci),
                             tf_batch_get_string(batch, rb, ci));
                break;
            case TF_TYPE_BOOL: {
                bool va = tf_batch_get_bool(batch, ra, ci);
                bool vb = tf_batch_get_bool(batch, rb, ci);
                cmp = (int)va - (int)vb;
                break;
            }
            case TF_TYPE_DATE: {
                int32_t va = tf_batch_get_date(batch, ra, ci);
                int32_t vb = tf_batch_get_date(batch, rb, ci);
                cmp = (va > vb) - (va < vb);
                break;
            }
            case TF_TYPE_TIMESTAMP: {
                int64_t va = tf_batch_get_timestamp(batch, ra, ci);
                int64_t vb = tf_batch_get_timestamp(batch, rb, ci);
                cmp = (va > vb) - (va < vb);
                break;
            }
            default:
                break;
        }

        if (cmp != 0) return ctx->col_desc[k] ? -cmp : cmp;
    }
    return 0;
}

static int sort_compare_index(const void *ctx, size_t ra, size_t rb) {
    return sort_compare_rows((const sort_ctx *)ctx, ra, rb);
}

static size_t *sort_batch_indices(const sort_state *st, const tf_batch *batch) {
    size_t n = batch->n_rows;
    size_t *indices = tf_mallocarray_checked(n ? n : 1, sizeof(size_t));
    if (!indices) return NULL;
    for (size_t i = 0; i < n; i++) indices[i] = i;
    sort_ctx ctx = {
        .batch = batch,
        .col_indices = st->col_indices,
        .col_desc = st->col_desc,
        .n_sort_cols = st->n_cols,
    };
    tf_sort_indices(indices, n, sort_compare_index, &ctx);
    return indices;
}

static int write_exact(FILE *f, const void *ptr, size_t len) {
    return fwrite(ptr, 1, len, f) == len ? TF_OK : TF_ERROR;
}

static int read_exact(FILE *f, void *ptr, size_t len) {
    return fread(ptr, 1, len, f) == len ? TF_OK : TF_ERROR;
}

static int write_cell(FILE *f, const tf_batch *b, size_t r, size_t c) {
    uint8_t is_null = tf_batch_is_null(b, r, c) ? 1 : 0;
    if (write_exact(f, &is_null, sizeof(is_null)) != TF_OK) return TF_ERROR;
    if (is_null) return TF_OK;

    switch (b->col_types[c]) {
        case TF_TYPE_BOOL: {
            uint8_t v = tf_batch_get_bool(b, r, c) ? 1 : 0;
            return write_exact(f, &v, sizeof(v));
        }
        case TF_TYPE_INT64: {
            int64_t v = tf_batch_get_int64(b, r, c);
            return write_exact(f, &v, sizeof(v));
        }
        case TF_TYPE_FLOAT64: {
            double v = tf_batch_get_float64(b, r, c);
            return write_exact(f, &v, sizeof(v));
        }
        case TF_TYPE_STRING: {
            const char *s = tf_batch_get_string(b, r, c);
            uint64_t len = s ? (uint64_t)strlen(s) : 0;
            if (write_exact(f, &len, sizeof(len)) != TF_OK) return TF_ERROR;
            return len ? write_exact(f, s, (size_t)len) : TF_OK;
        }
        case TF_TYPE_DATE: {
            int32_t v = tf_batch_get_date(b, r, c);
            return write_exact(f, &v, sizeof(v));
        }
        case TF_TYPE_TIMESTAMP: {
            int64_t v = tf_batch_get_timestamp(b, r, c);
            return write_exact(f, &v, sizeof(v));
        }
        default:
            return TF_OK;
    }
}

static int append_run_path(sort_state *st, char *path) {
    if (st->n_runs == st->cap_runs) {
        size_t need = 0;
        size_t new_cap = 0;
        if (tf_size_add(st->n_runs, 1, &need) != TF_OK ||
            tf_size_grow_pow2(st->cap_runs, need, 8, &new_cap) != TF_OK) {
            return TF_ERROR;
        }
        char **tmp = tf_reallocarray_checked(st->run_paths, new_cap, sizeof(char *));
        if (!tmp) return TF_ERROR;
        st->run_paths = tmp;
        st->cap_runs = new_cap;
    }
    st->run_paths[st->n_runs++] = path;
    st->spill_runs_created++;
    return TF_OK;
}

static int write_spill_run(sort_state *st) {
    if (!st->buf || st->buf->n_rows == 0) return TF_OK;

    size_t *indices = sort_batch_indices(st, st->buf);
    if (!indices) return TF_ERROR;

    char *path = NULL;
    FILE *f = tf_spill_open_run_file(st->spill, "sort", &path);
    if (!f) {
        free(indices);
        return TF_ERROR;
    }

    for (size_t i = 0; i < st->buf->n_rows; i++) {
        size_t r = indices[i];
        for (size_t c = 0; c < st->buf->n_cols; c++) {
            if (write_cell(f, st->buf, r, c) != TF_OK) {
                tf_set_last_error("sort spill: failed writing run file");
                fclose(f);
                remove(path);
                free(path);
                free(indices);
                return TF_ERROR;
            }
        }
    }
    long pos = ftell(f);
    if (pos > 0) st->spilled_bytes += (size_t)pos;
    if (fclose(f) != 0) {
        tf_set_last_error("sort spill: failed closing run file");
        remove(path);
        free(path);
        free(indices);
        return TF_ERROR;
    }

    free(indices);
    if (append_run_path(st, path) != TF_OK) {
        remove(path);
        free(path);
        return TF_ERROR;
    }

    tf_batch_free(st->buf);
    st->buf = create_buffer_from_schema(st, st->run_rows);
    return st->buf ? TF_OK : TF_ERROR;
}

static void spill_row_clear(sort_spill_row *row, const tf_type *types, size_t n_cols) {
    if (!row || !row->cells || !row->nulls) return;
    for (size_t c = 0; c < n_cols; c++) {
        if (!row->nulls[c] && types[c] == TF_TYPE_STRING) {
            free(row->cells[c].str);
            row->cells[c].str = NULL;
        }
        row->nulls[c] = 1;
    }
}

static int spill_row_init(sort_spill_row *row, size_t n_cols) {
    row->nulls = tf_callocarray_checked(n_cols ? n_cols : 1, sizeof(uint8_t));
    row->cells = tf_callocarray_checked(n_cols ? n_cols : 1, sizeof(sort_cell));
    if (!row->nulls || !row->cells) {
        free(row->nulls);
        free(row->cells);
        row->nulls = NULL;
        row->cells = NULL;
        return TF_ERROR;
    }
    for (size_t c = 0; c < n_cols; c++) row->nulls[c] = 1;
    return TF_OK;
}

static void spill_row_free(sort_spill_row *row, const tf_type *types, size_t n_cols) {
    if (!row) return;
    spill_row_clear(row, types, n_cols);
    free(row->nulls);
    free(row->cells);
    row->nulls = NULL;
    row->cells = NULL;
}

static int read_cell_value(FILE *f, sort_spill_row *row, const tf_type *types, size_t c) {
    switch (types[c]) {
        case TF_TYPE_BOOL: {
            uint8_t v = 0;
            if (read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR;
            row->cells[c].b = v;
            return TF_OK;
        }
        case TF_TYPE_INT64: {
            int64_t v = 0;
            if (read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR;
            row->cells[c].i64 = v;
            return TF_OK;
        }
        case TF_TYPE_FLOAT64: {
            double v = 0.0;
            if (read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR;
            row->cells[c].f64 = v;
            return TF_OK;
        }
        case TF_TYPE_STRING: {
            uint64_t len = 0;
            if (read_exact(f, &len, sizeof(len)) != TF_OK) return TF_ERROR;
            if (len > (uint64_t)SIZE_MAX - 1) return TF_ERROR;
            char *s = malloc((size_t)len + 1);
            if (!s) return TF_ERROR;
            if (len && read_exact(f, s, (size_t)len) != TF_OK) {
                free(s);
                return TF_ERROR;
            }
            s[len] = '\0';
            row->cells[c].str = s;
            return TF_OK;
        }
        case TF_TYPE_DATE: {
            int32_t v = 0;
            if (read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR;
            row->cells[c].date = v;
            return TF_OK;
        }
        case TF_TYPE_TIMESTAMP: {
            int64_t v = 0;
            if (read_exact(f, &v, sizeof(v)) != TF_OK) return TF_ERROR;
            row->cells[c].i64 = v;
            return TF_OK;
        }
        default:
            return TF_OK;
    }
}

static int reader_advance(sort_state *st, sort_run_reader *reader) {
    if (!reader || !reader->file || reader->done) return 0;
    spill_row_clear(&reader->row, st->schema_types, st->n_schema_cols);

    int first = fgetc(reader->file);
    if (first == EOF) {
        if (ferror(reader->file)) {
            tf_set_last_error("sort spill: failed reading run file");
            return -1;
        }
        reader->done = 1;
        reader->has_row = 0;
        return 0;
    }

    reader->row.nulls[0] = first ? 1 : 0;
    if (!reader->row.nulls[0] && read_cell_value(reader->file, &reader->row, st->schema_types, 0) != TF_OK) {
        tf_set_last_error("sort spill: corrupt run file");
        return -1;
    }
    for (size_t c = 1; c < st->n_schema_cols; c++) {
        uint8_t is_null = 1;
        if (read_exact(reader->file, &is_null, sizeof(is_null)) != TF_OK) {
            tf_set_last_error("sort spill: corrupt run file");
            return -1;
        }
        reader->row.nulls[c] = is_null ? 1 : 0;
        if (!reader->row.nulls[c] && read_cell_value(reader->file, &reader->row, st->schema_types, c) != TF_OK) {
            tf_set_last_error("sort spill: corrupt run file");
            return -1;
        }
    }
    reader->has_row = 1;
    return 1;
}

static int compare_spill_rows(const sort_state *st,
                              const sort_spill_row *a,
                              const sort_spill_row *b) {
    for (size_t k = 0; k < st->n_cols; k++) {
        int ci = st->col_indices[k];
        if (ci < 0) continue;
        size_t c = (size_t)ci;
        int null_a = a->nulls[c] != 0;
        int null_b = b->nulls[c] != 0;
        if (null_a && null_b) continue;
        if (null_a) return 1;
        if (null_b) return -1;

        int cmp = 0;
        switch (st->schema_types[c]) {
            case TF_TYPE_BOOL:
                cmp = (int)a->cells[c].b - (int)b->cells[c].b;
                break;
            case TF_TYPE_INT64:
            case TF_TYPE_TIMESTAMP:
                cmp = (a->cells[c].i64 > b->cells[c].i64) -
                      (a->cells[c].i64 < b->cells[c].i64);
                break;
            case TF_TYPE_FLOAT64:
                cmp = (a->cells[c].f64 > b->cells[c].f64) -
                      (a->cells[c].f64 < b->cells[c].f64);
                break;
            case TF_TYPE_STRING:
                cmp = strcmp(a->cells[c].str, b->cells[c].str);
                break;
            case TF_TYPE_DATE:
                cmp = (a->cells[c].date > b->cells[c].date) -
                      (a->cells[c].date < b->cells[c].date);
                break;
            default:
                break;
        }
        if (cmp != 0) return st->col_desc[k] ? -cmp : cmp;
    }
    return 0;
}

static int spill_row_to_batch(const sort_state *st, tf_batch *out,
                              size_t dst_row, const sort_spill_row *row) {
    if (tf_batch_ensure_capacity(out, dst_row + 1) != TF_OK) return TF_ERROR;
    for (size_t c = 0; c < st->n_schema_cols; c++) {
        if (tf_batch_set_owned_cell_value(out, dst_row, c, st->schema_types[c],
                                          row->nulls[c], &row->cells[c]) != TF_OK) {
            return TF_ERROR;
        }
    }
    return TF_OK;
}

static void close_readers(sort_state *st) {
    if (!st->readers) return;
    for (size_t i = 0; i < st->n_readers; i++) {
        if (st->readers[i].file) fclose(st->readers[i].file);
        spill_row_free(&st->readers[i].row, st->schema_types, st->n_schema_cols);
    }
    free(st->readers);
    st->readers = NULL;
    st->n_readers = 0;
}

static void remove_run_files(sort_state *st) {
    for (size_t i = 0; i < st->n_runs; i++) {
        if (st->run_paths[i]) {
            remove(st->run_paths[i]);
            free(st->run_paths[i]);
            st->run_paths[i] = NULL;
        }
    }
    free(st->run_paths);
    st->run_paths = NULL;
    st->n_runs = 0;
    st->cap_runs = 0;
}

static int begin_merge(sort_state *st) {
    if (st->merge_started) return TF_OK;
    if (st->buf && st->buf->n_rows > 0 && write_spill_run(st) != TF_OK) return TF_ERROR;
    if (st->buf) {
        tf_batch_free(st->buf);
        st->buf = NULL;
    }

    st->merge_started = 1;
    if (st->n_runs == 0) {
        st->merge_done = 1;
        return TF_OK;
    }

    st->readers = tf_callocarray_checked(st->n_runs, sizeof(sort_run_reader));
    if (!st->readers) return TF_ERROR;
    st->n_readers = st->n_runs;
    for (size_t i = 0; i < st->n_runs; i++) {
        st->readers[i].file = fopen(st->run_paths[i], "rb");
        if (!st->readers[i].file) {
            tf_set_last_error("sort spill: cannot reopen run file");
            return TF_ERROR;
        }
        if (spill_row_init(&st->readers[i].row, st->n_schema_cols) != TF_OK) return TF_ERROR;
        int rc = reader_advance(st, &st->readers[i]);
        if (rc < 0) return TF_ERROR;
    }
    return TF_OK;
}

static int next_best_reader(const sort_state *st) {
    int best = -1;
    for (size_t i = 0; i < st->n_readers; i++) {
        const sort_run_reader *r = &st->readers[i];
        if (!r->has_row || r->done) continue;
        if (best < 0) {
            best = (int)i;
            continue;
        }
        int cmp = compare_spill_rows(st, &r->row, &st->readers[best].row);
        if (cmp < 0 || (cmp == 0 && i < (size_t)best)) best = (int)i;
    }
    return best;
}

static int spill_next_batch(sort_state *st, tf_batch **out) {
    *out = NULL;
    if (begin_merge(st) != TF_OK) return TF_ERROR;
    if (st->merge_done) return TF_OK;

    tf_batch *ob = create_buffer_from_schema(st, st->output_batch_rows);
    if (!ob) return TF_ERROR;

    while (ob->n_rows < st->output_batch_rows) {
        int best = next_best_reader(st);
        if (best < 0) break;
        size_t out_row = ob->n_rows;
        if (spill_row_to_batch(st, ob, out_row, &st->readers[best].row) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, out_row) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        int rc = reader_advance(st, &st->readers[best]);
        if (rc < 0) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    if (ob->n_rows == 0) {
        tf_batch_free(ob);
        close_readers(st);
        remove_run_files(st);
        tf_spill_cleanup(st->spill);
        st->spill = NULL;
        st->merge_done = 1;
        return TF_OK;
    }

    st->spill_output_batches++;
    st->spill_output_rows += ob->n_rows;
    *out = ob;
    return TF_OK;
}

static int sort_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    sort_state *st = self->state;
    *out = NULL;

    if (init_schema(st, in) != TF_OK) return TF_ERROR;
    for (size_t r = 0; r < in->n_rows; r++) {
        size_t dst_row = st->buf->n_rows;
        if (tf_batch_copy_row(st->buf, dst_row, in, r) != TF_OK) return TF_ERROR;
        if (tf_batch_expose_row(st->buf, dst_row) != TF_OK) return TF_ERROR;
        if (st->use_spill && st->buf->n_rows >= st->run_rows) {
            if (write_spill_run(st) != TF_OK) return TF_ERROR;
        }
    }
    return TF_OK;
}

static int sort_flush_in_memory(tf_step *self, tf_batch **out) {
    sort_state *st = self->state;
    *out = NULL;
    if (!st->buf || st->buf->n_rows == 0) return TF_OK;

    size_t n = st->buf->n_rows;
    size_t *indices = sort_batch_indices(st, st->buf);
    if (!indices) return TF_ERROR;

    tf_batch *ob = create_buffer_from_schema(st, n);
    if (!ob) { free(indices); return TF_ERROR; }
    for (size_t i = 0; i < n; i++) {
        if (tf_batch_copy_row(ob, i, st->buf, indices[i]) != TF_OK) {
            free(indices);
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (tf_batch_expose_row(ob, i) != TF_OK) {
            free(indices);
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    free(indices);
    *out = ob;
    return TF_OK;
}

static int sort_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    sort_state *st = self->state;
    *out = NULL;
    if (!st->use_spill) return sort_flush_in_memory(self, out);
    return spill_next_batch(st, out);
}

static int sort_flush_next(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    sort_state *st = self->state;
    *out = NULL;
    if (!st->use_spill) return TF_OK;
    return spill_next_batch(st, out);
}

static int sort_append_stats(tf_step *self, tf_buffer *out) {
    if (!self || !out) return TF_ERROR;
    sort_state *st = self->state;
    if (!st || !st->use_spill) return TF_OK;

    char buf[256];
    snprintf(buf, sizeof(buf),
             ",\"spill_bytes\":%zu,\"spill_runs\":%zu,"
             "\"spill_output_batches\":%zu,\"spill_output_rows\":%zu",
             st->spilled_bytes, st->spill_runs_created,
             st->spill_output_batches, st->spill_output_rows);
    return tf_buffer_write_str(out, buf);
}

static void sort_state_free(sort_state *st) {
    if (!st) return;
    if (st->buf) tf_batch_free(st->buf);
    close_readers(st);
    remove_run_files(st);
    for (size_t i = 0; i < st->n_cols; i++) free(st->cols[i].name);
    for (size_t i = 0; i < st->n_schema_cols; i++) free(st->schema_names ? st->schema_names[i] : NULL);
    free(st->schema_names);
    free(st->schema_types);
    free(st->cols);
    free(st->col_indices);
    free(st->col_desc);
    tf_spill_cleanup(st->spill);
    free(st->spill_dir);
    free(st);
}

static void sort_destroy(tf_step *self) {
    if (self) sort_state_free(self->state);
    free(self);
}

tf_step *tf_sort_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *columns = cJSON_GetObjectItemCaseSensitive(args, "columns");
    if (!columns || !cJSON_IsArray(columns)) return NULL;

    int n = cJSON_GetArraySize(columns);
    if (n <= 0) return NULL;

    sort_state *st = calloc(1, sizeof(sort_state));
    if (!st) return NULL;
    st->cols = tf_callocarray_checked((size_t)n, sizeof(sort_col));
    if (!st->cols) { free(st); return NULL; }
    st->n_cols = (size_t)n;
    st->output_batch_rows = SORT_DEFAULT_OUTPUT_ROWS;

    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(columns, i);
        cJSON *name_j = cJSON_GetObjectItemCaseSensitive(item, "name");
        cJSON *desc_j = cJSON_GetObjectItemCaseSensitive(item, "desc");
        if (!cJSON_IsString(name_j) || !name_j->valuestring || name_j->valuestring[0] == '\0') {
            tf_set_last_error("sort: column names must be non-empty strings");
            sort_state_free(st);
            return NULL;
        }
        st->cols[i].name = strdup(name_j->valuestring);
        if (!st->cols[i].name) {
            sort_state_free(st);
            return NULL;
        }
        st->cols[i].desc = (desc_j && cJSON_IsBool(desc_j)) ? cJSON_IsTrue(desc_j) : 0;
    }

    cJSON *spill_dir_j = cJSON_GetObjectItemCaseSensitive(args, "spill_dir");
    if (cJSON_IsString(spill_dir_j) && spill_dir_j->valuestring && spill_dir_j->valuestring[0]) {
        st->use_spill = 1;
        st->spill_dir = strdup(spill_dir_j->valuestring);
        if (!st->spill_dir) {
            sort_state_free(st);
            return NULL;
        }
        if (tf_spill_session_create(st->spill_dir, &st->spill) != TF_OK) {
            sort_state_free(st);
            return NULL;
        }
        size_t parsed_size = 0;
        int has_spill_memory = tf_json_get_size_arg(args, "spill_memory_bytes",
                                                    1, TF_MAX_SPILL_MEMORY_BYTES,
                                                    &parsed_size, "sort");
        if (has_spill_memory < 0) { sort_state_free(st); return NULL; }
        if (has_spill_memory > 0) st->spill_memory_bytes = parsed_size;
        int has_spill_rows = tf_json_get_size_arg(args, "spill_run_rows",
                                                  1, TF_MAX_SPILL_RUN_ROWS,
                                                  &parsed_size, "sort");
        if (has_spill_rows < 0) { sort_state_free(st); return NULL; }
        if (has_spill_rows > 0) st->configured_run_rows = parsed_size;
        int has_output_rows = tf_json_get_size_arg(args, "spill_output_rows",
                                                   1, TF_MAX_SPILL_OUTPUT_ROWS,
                                                   &parsed_size, "sort");
        if (has_output_rows < 0) { sort_state_free(st); return NULL; }
        if (has_output_rows > 0) st->output_batch_rows = parsed_size;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) {
        sort_state_free(st);
        return NULL;
    }
    step->process = sort_process;
    step->flush = sort_flush;
    step->flush_next = st->use_spill ? sort_flush_next : NULL;
    step->append_stats = sort_append_stats;
    step->destroy = sort_destroy;
    step->state = st;
    return step;
}
