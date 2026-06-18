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
    tf_audit_options audit_opts;
} fill_null_state;

static int emit_fill_null_audit(fill_null_state *st, const tf_batch *b, size_t row,
                                size_t col, size_t row_no, tf_side_channels *side) {
    if (!st->audit || st->audit_emitted >= st->audit_limit || !side || !side->stats) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    int rc = TF_ERROR;
    if (tf_json_add_string(obj, "type", "audit") != TF_OK ||
        tf_json_add_string(obj, "op", "fill-null") != TF_OK ||
        tf_json_add_string(obj, "event", "null_filled") != TF_OK ||
        tf_json_add_string(obj, "reason", "null_filled") != TF_OK ||
        tf_json_add_string(obj, "channel", "audit") != TF_OK ||
        tf_json_add_string(obj, "column", b->col_names[col] ? b->col_names[col] : "") != TF_OK ||
        tf_json_add_number(obj, "row", (double)row_no) != TF_OK ||
        tf_json_add_null(obj, "before") != TF_OK) {
        goto done;
    }
    cJSON *after = tf_audit_cell_to_json(b, row, col, &st->audit_opts);
    if (!after || tf_json_add_item(obj, "after", after) != TF_OK) goto done;
    cJSON *row_obj = tf_audit_row_to_json(b, row, &st->audit_opts);
    if (row_obj) {
        if (tf_json_add_item(obj, "data", row_obj) != TF_OK) goto done;
    } else if (st->audit_opts.include_row) {
        goto done;
    }
    rc = tf_buffer_write_json_line(side->stats, obj);
done:
    cJSON_Delete(obj);
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
                    if (tf_batch_set_string(ob, r, (size_t)ci, def) != TF_OK) {
                        tf_batch_free(ob);
                        return TF_ERROR;
                    }
                    changed = 1;
                    break;
                case TF_TYPE_INT64: {
                    char *end;
                    int64_t v = strtoll(def, &end, 10);
                    if (*end == '\0') {
                        if (tf_batch_set_int64(ob, r, (size_t)ci, v) != TF_OK) {
                            tf_batch_free(ob);
                            return TF_ERROR;
                        }
                        changed = 1;
                    }
                    break;
                }
                case TF_TYPE_FLOAT64: {
                    char *end;
                    double v = strtod(def, &end);
                    if (*end == '\0') {
                        if (tf_batch_set_float64(ob, r, (size_t)ci, v) != TF_OK) {
                            tf_batch_free(ob);
                            return TF_ERROR;
                        }
                        changed = 1;
                    }
                    break;
                }
                case TF_TYPE_BOOL:
                    if (tf_batch_set_bool(ob, r, (size_t)ci, strcmp(def, "true") == 0) != TF_OK) {
                        tf_batch_free(ob);
                        return TF_ERROR;
                    }
                    changed = 1;
                    break;
                case TF_TYPE_DATE: {
                    char *end;
                    int32_t v = (int32_t)strtol(def, &end, 10);
                    if (*end == '\0') {
                        if (tf_batch_set_date(ob, r, (size_t)ci, v) != TF_OK) {
                            tf_batch_free(ob);
                            return TF_ERROR;
                        }
                        changed = 1;
                    }
                    break;
                }
                case TF_TYPE_TIMESTAMP: {
                    char *end;
                    int64_t v = strtoll(def, &end, 10);
                    if (*end == '\0') {
                        if (tf_batch_set_timestamp(ob, r, (size_t)ci, v) != TF_OK) {
                            tf_batch_free(ob);
                            return TF_ERROR;
                        }
                        changed = 1;
                    }
                    break;
                }
                default:
                    break;
            }
            size_t row_no = 0;
            if (changed &&
                (tf_size_add(row_base, r, &row_no) != TF_OK ||
                 tf_size_add(row_no, 1, &row_no) != TF_OK ||
                 emit_fill_null_audit(st, ob, r, (size_t)ci, row_no, side) != TF_OK)) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
        }
    }

    if (tf_size_add(st->row_index, in->n_rows, &st->row_index) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }
    *out = ob;
    return TF_OK;
}

static int fill_null_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void fill_null_state_free(fill_null_state *st) {
    if (!st) return;
    tf_audit_options_free(&st->audit_opts);
    if (st->col_names) {
        for (size_t i = 0; i < st->n; i++) free(st->col_names[i]);
    }
    if (st->defaults) {
        for (size_t i = 0; i < st->n; i++) free(st->defaults[i]);
    }
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
    tf_audit_options_init(&st->audit_opts, 1);
    st->col_names = tf_callocarray_checked(n > 0 ? (size_t)n : 1, sizeof(char *));
    st->defaults = tf_callocarray_checked(n > 0 ? (size_t)n : 1, sizeof(char *));
    st->n = (size_t)n;
    st->audit_limit = 1000;
    if (!st->col_names || !st->defaults) { fill_null_state_free(st); return NULL; }

    int i = 0;
    cJSON *entry = NULL;
    cJSON_ArrayForEach(entry, mapping) {
        if (!entry->string || !entry->string[0]) { fill_null_state_free(st); return NULL; }
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
        size_t parsed_limit = 0;
        if (tf_json_get_size_arg_any(args, "audit_limit", "auditLimit",
                                     1, TF_MAX_AUDIT_RECORDS,
                                     &parsed_limit, "fill-null") < 0) {
            fill_null_state_free(st);
            return NULL;
        }
        st->audit_limit = parsed_limit;
    }

    if (tf_audit_options_parse(&st->audit_opts, args, "fill-null") != TF_OK) {
        fill_null_state_free(st);
        return NULL;
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { fill_null_state_free(st); return NULL; }
    step->process = fill_null_process;
    step->flush = fill_null_flush;
    step->destroy = fill_null_destroy;
    step->state = st;
    return step;
}
