/*
 * op_schema_infer.c -- Bounded schema inference report.
 *
 * schema infer [rows=N] samples decoded batches without retaining rows and
 * emits one schema-report row per input column at finish(). It reports the
 * decoded runtime types rather than changing parser behavior.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>

#define SCHEMA_INFER_DEFAULT_ROWS 10000u

typedef struct {
    char *name;
    size_t missing;
    size_t counts[6]; /* bool, int, float, string, date, timestamp */
} schema_infer_col;

typedef struct {
    size_t rows_limit;
    size_t rows_seen;
    size_t rows_sampled;
    int initialized;
    int schema_changed;
    schema_infer_col *cols;
    size_t n_cols;
} schema_infer_state;

static int schema_infer_type_index(tf_type type) {
    switch (type) {
        case TF_TYPE_BOOL: return 0;
        case TF_TYPE_INT64: return 1;
        case TF_TYPE_FLOAT64: return 2;
        case TF_TYPE_STRING: return 3;
        case TF_TYPE_DATE: return 4;
        case TF_TYPE_TIMESTAMP: return 5;
        default: return -1;
    }
}

static void append_token(char *buf, size_t buf_size, int *first, const char *token) {
    if (!buf || buf_size == 0 || !token || !token[0]) return;
    size_t used = strlen(buf);
    if (used >= buf_size - 1) return;
    snprintf(buf + used, buf_size - used, "%s%s", *first ? "" : ",", token);
    *first = 0;
}

static void append_count(char *buf, size_t buf_size, int *first,
                         const char *name, size_t count) {
    if (count == 0 || !buf || buf_size == 0 || !name) return;
    size_t used = strlen(buf);
    if (used >= buf_size - 1) return;
    snprintf(buf + used, buf_size - used, "%s%s:%zu", *first ? "" : ";", name, count);
    *first = 0;
}

static int schema_infer_init(schema_infer_state *st, const tf_batch *in) {
    if (st->initialized) return TF_OK;
    size_t n_cols = in ? in->n_cols : 0;
    if (n_cols == 0) {
        st->initialized = 1;
        return TF_OK;
    }

    schema_infer_col *cols = calloc(n_cols, sizeof(schema_infer_col));
    if (!cols) return TF_ERROR;
    for (size_t c = 0; c < n_cols; c++) {
        const char *name = in->col_names[c] ? in->col_names[c] : "";
        cols[c].name = strdup(name);
        if (!cols[c].name) {
            for (size_t i = 0; i < c; i++) free(cols[i].name);
            free(cols);
            return TF_ERROR;
        }
    }

    st->cols = cols;
    st->n_cols = n_cols;
    st->initialized = 1;
    return TF_OK;
}

static int schema_infer_process(tf_step *self, tf_batch *in, tf_batch **out,
                                tf_side_channels *side) {
    (void)side;
    schema_infer_state *st = self->state;
    *out = NULL;
    if (schema_infer_init(st, in) != TF_OK) return TF_ERROR;
    if (!in) return TF_OK;
    if (st->initialized && in->n_cols != st->n_cols) st->schema_changed = 1;

    for (size_t r = 0; r < in->n_rows; r++) {
        st->rows_seen++;
        if (st->rows_sampled >= st->rows_limit) continue;
        st->rows_sampled++;
        size_t n = in->n_cols < st->n_cols ? in->n_cols : st->n_cols;
        for (size_t c = 0; c < n; c++) {
            schema_infer_col *col = &st->cols[c];
            if (tf_batch_is_null(in, r, c)) {
                col->missing++;
                continue;
            }
            int idx = schema_infer_type_index(in->col_types[c]);
            if (idx >= 0) col->counts[idx]++;
        }
    }
    return TF_OK;
}

static const char *schema_infer_inferred_type(const schema_infer_col *col,
                                              const char **warning_token) {
    size_t non_missing = 0;
    size_t nonzero = 0;
    int last = -1;
    *warning_token = NULL;
    for (int i = 0; i < 6; i++) {
        non_missing += col->counts[i];
        if (col->counts[i] > 0) { nonzero++; last = i; }
    }
    if (non_missing == 0) {
        *warning_token = "no_non_null_sample";
        return "unknown";
    }
    if (nonzero == 1) {
        static const char *names[] = {"bool", "int", "float", "string", "date", "timestamp"};
        return names[last];
    }
    if (col->counts[1] > 0 && col->counts[2] > 0 && nonzero == 2) {
        *warning_token = "mixed_numeric";
        return "float";
    }
    *warning_token = "mixed_types";
    return "string";
}

static int schema_infer_set_output_schema(tf_batch *out) {
    return tf_batch_set_schema(out, 0, "column", TF_TYPE_STRING) == TF_OK &&
           tf_batch_set_schema(out, 1, "type", TF_TYPE_STRING) == TF_OK &&
           tf_batch_set_schema(out, 2, "nullable", TF_TYPE_BOOL) == TF_OK &&
           tf_batch_set_schema(out, 3, "non_null", TF_TYPE_BOOL) == TF_OK &&
           tf_batch_set_schema(out, 4, "rows_seen", TF_TYPE_INT64) == TF_OK &&
           tf_batch_set_schema(out, 5, "rows_sampled", TF_TYPE_INT64) == TF_OK &&
           tf_batch_set_schema(out, 6, "missing", TF_TYPE_INT64) == TF_OK &&
           tf_batch_set_schema(out, 7, "non_missing", TF_TYPE_INT64) == TF_OK &&
           tf_batch_set_schema(out, 8, "observed_types", TF_TYPE_STRING) == TF_OK &&
           tf_batch_set_schema(out, 9, "warning", TF_TYPE_STRING) == TF_OK ? TF_OK : TF_ERROR;
}

static int schema_infer_flush(tf_step *self, tf_batch **out,
                              tf_side_channels *side) {
    (void)side;
    schema_infer_state *st = self->state;
    *out = NULL;
    if (!st->initialized || st->n_cols == 0) return TF_OK;

    tf_batch *ob = tf_batch_create(10, st->n_cols ? st->n_cols : 1);
    if (!ob) return TF_ERROR;
    int rc = TF_ERROR;
#define SCHEMA_INFER_WRITE(expr) do { if ((expr) != TF_OK) goto done; } while (0)

    SCHEMA_INFER_WRITE(schema_infer_set_output_schema(ob));

    static const char *type_names[] = {"bool", "int", "float", "string", "date", "timestamp"};
    for (size_t c = 0; c < st->n_cols; c++) {
        schema_infer_col *col = &st->cols[c];
        size_t non_missing = 0;
        char observed[256] = {0};
        char warning[192] = {0};
        int first_obs = 1;
        int first_warn = 1;
        for (int i = 0; i < 6; i++) {
            non_missing += col->counts[i];
            append_count(observed, sizeof(observed), &first_obs, type_names[i], col->counts[i]);
        }
        append_count(observed, sizeof(observed), &first_obs, "null", col->missing);
        if (observed[0] == '\0') snprintf(observed, sizeof(observed), "none");

        const char *type_warning = NULL;
        const char *inferred = schema_infer_inferred_type(col, &type_warning);
        if (type_warning) append_token(warning, sizeof(warning), &first_warn, type_warning);
        if (st->rows_seen > st->rows_sampled) append_token(warning, sizeof(warning), &first_warn, "sample_limited");
        if (st->schema_changed) append_token(warning, sizeof(warning), &first_warn, "schema_changed");
        if (st->rows_sampled == 0) append_token(warning, sizeof(warning), &first_warn, "no_rows_sampled");

        SCHEMA_INFER_WRITE(tf_batch_set_string(ob, c, 0, col->name ? col->name : ""));
        SCHEMA_INFER_WRITE(tf_batch_set_string(ob, c, 1, inferred));
        SCHEMA_INFER_WRITE(tf_batch_set_bool(ob, c, 2, col->missing > 0));
        SCHEMA_INFER_WRITE(tf_batch_set_bool(ob, c, 3, col->missing == 0 && st->rows_sampled > 0));
        SCHEMA_INFER_WRITE(tf_batch_set_int64(ob, c, 4, (int64_t)st->rows_seen));
        SCHEMA_INFER_WRITE(tf_batch_set_int64(ob, c, 5, (int64_t)st->rows_sampled));
        SCHEMA_INFER_WRITE(tf_batch_set_int64(ob, c, 6, (int64_t)col->missing));
        SCHEMA_INFER_WRITE(tf_batch_set_int64(ob, c, 7, (int64_t)non_missing));
        SCHEMA_INFER_WRITE(tf_batch_set_string(ob, c, 8, observed));
        SCHEMA_INFER_WRITE(tf_batch_set_string(ob, c, 9, warning));
        SCHEMA_INFER_WRITE(tf_batch_expose_row(ob, c));
    }

    *out = ob;
    ob = NULL;
    rc = TF_OK;

done:
    if (ob) tf_batch_free(ob);
#undef SCHEMA_INFER_WRITE
    return rc;
}

static void schema_infer_destroy(tf_step *self) {
    if (!self) return;
    schema_infer_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n_cols; i++) free(st->cols[i].name);
        free(st->cols);
        free(st);
    }
    free(self);
}

static int parse_rows_arg(const cJSON *args, size_t *rows) {
    *rows = SCHEMA_INFER_DEFAULT_ROWS;
    if (!args) return TF_OK;
    int has_rows = tf_json_get_size_arg(args, "rows",
                                        1, TF_MAX_COUNT_ARG,
                                        rows, "schema-infer");
    if (has_rows < 0) {
        return TF_ERROR;
    }
    if (has_rows == 0) *rows = SCHEMA_INFER_DEFAULT_ROWS;
    return TF_OK;
}

tf_step *tf_schema_infer_create(const cJSON *args) {
    schema_infer_state *st = calloc(1, sizeof(schema_infer_state));
    if (!st) return NULL;
    if (parse_rows_arg(args, &st->rows_limit) != TF_OK) {
        free(st);
        return NULL;
    }
    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { free(st); return NULL; }
    step->process = schema_infer_process;
    step->flush = schema_infer_flush;
    step->destroy = schema_infer_destroy;
    step->state = st;
    return step;
}
