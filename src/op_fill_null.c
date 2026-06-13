/*
 * op_fill_null.c — Replace nulls with default values.
 *
 * Config: {"mapping": {"col1": "default1", "col2": "0"}}
 */

#include "internal.h"
#include "cJSON.h"
#include "date_utils.h"
#include <stdlib.h>
#include <string.h>

/* Fill-null is row-local. Audit records are opt-in and capped so diagnostics
 * cannot become an unbounded second output stream. */
typedef struct {
    char **col_names;
    char **defaults;
    size_t n;
    size_t row_index;
    size_t audit_limit;
    size_t audit_emitted;
    int audit;
} fill_null_state;

static cJSON *fill_null_cell_to_json(const tf_batch *b, size_t row, size_t col) {
    if (!b || col >= b->n_cols || row >= b->n_rows || tf_batch_is_null(b, row, col)) return cJSON_CreateNull();
    switch (b->col_types[col]) {
        case TF_TYPE_BOOL: return cJSON_CreateBool(tf_batch_get_bool(b, row, col));
        case TF_TYPE_INT64: return cJSON_CreateNumber((double)tf_batch_get_int64(b, row, col));
        case TF_TYPE_FLOAT64: return cJSON_CreateNumber(tf_batch_get_float64(b, row, col));
        case TF_TYPE_STRING: return cJSON_CreateString(tf_batch_get_string(b, row, col));
        case TF_TYPE_DATE: return cJSON_CreateNumber((double)tf_batch_get_date(b, row, col));
        case TF_TYPE_TIMESTAMP: return cJSON_CreateNumber((double)tf_batch_get_timestamp(b, row, col));
        default: return cJSON_CreateNull();
    }
}

static cJSON *fill_null_row_to_json(const tf_batch *b, size_t row) {
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return NULL;
    for (size_t c = 0; c < b->n_cols; c++) {
        cJSON *value = fill_null_cell_to_json(b, row, c);
        if (!value) { cJSON_Delete(obj); return NULL; }
        cJSON_AddItemToObject(obj, b->col_names[c] ? b->col_names[c] : "", value);
    }
    return obj;
}

static int emit_fill_null_audit(fill_null_state *st, const tf_batch *b, size_t row,
                                size_t col, size_t row_no, tf_side_channels *side) {
    if (!st->audit || st->audit_emitted >= st->audit_limit || !side || !side->stats) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    cJSON_AddStringToObject(obj, "type", "audit");
    cJSON_AddStringToObject(obj, "op", "fill-null");
    cJSON_AddStringToObject(obj, "event", "null_filled");
    cJSON_AddStringToObject(obj, "reason", "null_filled");
    cJSON_AddStringToObject(obj, "channel", "audit");
    cJSON_AddStringToObject(obj, "column", b->col_names[col] ? b->col_names[col] : "");
    cJSON_AddNumberToObject(obj, "row", (double)row_no);
    cJSON_AddItemToObject(obj, "before", cJSON_CreateNull());
    cJSON *after = fill_null_cell_to_json(b, row, col);
    if (after) cJSON_AddItemToObject(obj, "after", after);
    cJSON *row_obj = fill_null_row_to_json(b, row);
    if (row_obj) cJSON_AddItemToObject(obj, "data", row_obj);
    char *line = cJSON_PrintUnformatted(obj);
    cJSON_Delete(obj);
    if (!line) return TF_ERROR;
    int rc = tf_buffer_write_str(side->stats, line);
    if (rc == TF_OK) rc = tf_buffer_write_str(side->stats, "\n");
    free(line);
    if (rc == TF_OK) st->audit_emitted++;
    return rc;
}

static int fill_null_process(tf_step *self, tf_batch *in, tf_batch **out,
                             tf_side_channels *side) {
    fill_null_state *st = self->state;
    *out = NULL;
    size_t row_base = st->row_index;

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], in->col_types[c]);
    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_copy_row(ob, r, in, r);
        ob->n_rows = r + 1;
    }

    /* Fill nulls */
    for (size_t k = 0; k < st->n; k++) {
        int ci = tf_batch_col_index(ob, st->col_names[k]);
        if (ci < 0) continue;
        for (size_t r = 0; r < ob->n_rows; r++) {
            if (!tf_batch_is_null(ob, r, (size_t)ci)) continue;
            const char *def = st->defaults[k];
            int changed = 0;
            switch (ob->col_types[ci]) {
                case TF_TYPE_STRING:
                    tf_batch_set_string(ob, r, (size_t)ci, def);
                    changed = 1;
                    break;
                case TF_TYPE_INT64: {
                    char *end;
                    int64_t v = strtoll(def, &end, 10);
                    if (*end == '\0') { tf_batch_set_int64(ob, r, (size_t)ci, v); changed = 1; }
                    break;
                }
                case TF_TYPE_FLOAT64: {
                    char *end;
                    double v = strtod(def, &end);
                    if (*end == '\0') { tf_batch_set_float64(ob, r, (size_t)ci, v); changed = 1; }
                    break;
                }
                case TF_TYPE_BOOL:
                    tf_batch_set_bool(ob, r, (size_t)ci, strcmp(def, "true") == 0);
                    changed = 1;
                    break;
                case TF_TYPE_DATE: {
                    char *end;
                    int32_t v = (int32_t)strtol(def, &end, 10);
                    if (*end == '\0') { tf_batch_set_date(ob, r, (size_t)ci, v); changed = 1; }
                    break;
                }
                case TF_TYPE_TIMESTAMP: {
                    char *end;
                    int64_t v = strtoll(def, &end, 10);
                    if (*end == '\0') { tf_batch_set_timestamp(ob, r, (size_t)ci, v); changed = 1; }
                    break;
                }
                default:
                    break;
            }
            if (changed && emit_fill_null_audit(st, ob, r, (size_t)ci, row_base + r + 1, side) != TF_OK) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
        }
    }

    st->row_index += in->n_rows;
    *out = ob;
    return TF_OK;
}

static int fill_null_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void fill_null_state_free(fill_null_state *st) {
    if (!st) return;
    for (size_t i = 0; i < st->n; i++) { free(st->col_names[i]); free(st->defaults[i]); }
    free(st->col_names);
    free(st->defaults);
    free(st);
}

static void fill_null_destroy(tf_step *self) {
    if (self) fill_null_state_free(self->state);
    free(self);
}

tf_step *tf_fill_null_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *mapping = cJSON_GetObjectItemCaseSensitive(args, "mapping");
    if (!mapping || !cJSON_IsObject(mapping)) return NULL;

    int n = cJSON_GetArraySize(mapping);
    fill_null_state *st = calloc(1, sizeof(fill_null_state));
    if (!st) return NULL;
    st->col_names = calloc((size_t)n, sizeof(char *));
    st->defaults = calloc((size_t)n, sizeof(char *));
    st->n = (size_t)n;
    st->audit_limit = 1000;
    if (!st->col_names || !st->defaults) { fill_null_state_free(st); return NULL; }

    int i = 0;
    cJSON *entry = NULL;
    cJSON_ArrayForEach(entry, mapping) {
        st->col_names[i] = strdup(entry->string);
        st->defaults[i] = strdup(cJSON_IsString(entry) ? entry->valuestring : "");
        if (!st->col_names[i] || !st->defaults[i]) { fill_null_state_free(st); return NULL; }
        i++;
    }

    cJSON *audit_j = cJSON_GetObjectItemCaseSensitive(args, "audit");
    st->audit = cJSON_IsTrue(audit_j) ? 1 : 0;
    cJSON *audit_limit_j = cJSON_GetObjectItemCaseSensitive(args, "audit_limit");
    if (!audit_limit_j) audit_limit_j = cJSON_GetObjectItemCaseSensitive(args, "auditLimit");
    if (audit_limit_j) {
        if (!cJSON_IsNumber(audit_limit_j) || audit_limit_j->valuedouble <= 0) {
            tf_set_last_error("fill-null: audit_limit must be a positive integer");
            fill_null_state_free(st);
            return NULL;
        }
        st->audit_limit = (size_t)audit_limit_j->valuedouble;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { fill_null_state_free(st); return NULL; }
    step->process = fill_null_process;
    step->flush = fill_null_flush;
    step->destroy = fill_null_destroy;
    step->state = st;
    return step;
}
