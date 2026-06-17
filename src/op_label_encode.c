/*
 * op_label_encode.c — Map categorical values to sequential integers.
 *
 * Config: {"column": "city", "result": "city_encoded",
 *          "categories": ["Paris", "London"],
 *          "max_categories": 1000,
 *          "max_state_bytes": 1048576,
 *          "unknown": "error" | "other" | "null"}
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef enum {
    TF_CAT_UNKNOWN_ADD,
    TF_CAT_UNKNOWN_ERROR,
    TF_CAT_UNKNOWN_OTHER,
    TF_CAT_UNKNOWN_NULL
} tf_cat_unknown_policy;

typedef struct label_entry {
    char  *value;
    int64_t label;
} label_entry;

typedef struct {
    char                  *column;
    char                  *result;
    size_t                 max_categories; /* 0 = unlimited */
    size_t                 max_state_bytes; /* 0 = unlimited */
    int                    categories_declared;
    tf_cat_unknown_policy  unknown;
    label_entry           *entries;
    size_t                 n_entries;
    size_t                 cap;
    int64_t                next_label;
} label_encode_state;

static const char *OTHER_CATEGORY = "__other__";

static size_t label_retained_state_bytes(const label_encode_state *st);

static int label_encode_expose_row(tf_batch *ob, size_t row) {
    size_t next_rows = 0;
    if (tf_size_add(row, 1, &next_rows) != TF_OK) return TF_ERROR;
    ob->n_rows = next_rows;
    return TF_OK;
}

static int label_write_error(tf_side_channels *side, const char *msg) {
    return tf_side_write_error(side, msg);
}

static int label_unknown_error(const label_encode_state *st, const char *val,
                               tf_side_channels *side) {
    char msg[256];
    snprintf(msg, sizeof(msg),
             "label-encode: unknown category '%s' for column '%s'",
             val ? val : "", st->column ? st->column : "");
    return label_write_error(side, msg);
}

static int label_limit_error(const label_encode_state *st, const char *val,
                             tf_side_channels *side) {
    char msg[256];
    snprintf(msg, sizeof(msg),
             "label-encode: max_categories=%zu exceeded while tracking category '%s'",
             st->max_categories, val ? val : "");
    return label_write_error(side, msg);
}

static int label_check_state_bytes(label_encode_state *st, tf_side_channels *side) {
    if (!st || st->max_state_bytes == 0) return TF_OK;
    size_t retained = label_retained_state_bytes(st);
    if (retained <= st->max_state_bytes) return TF_OK;
    char msg[208];
    snprintf(msg, sizeof(msg),
             "label-encode: max_state_bytes=%zu exceeded while tracking categories (%zu bytes retained)",
             st->max_state_bytes, retained);
    if (label_write_error(side, msg) != TF_OK) return TF_ERROR;
    return TF_ERROR;
}

static const char *get_string_value(const tf_batch *b, size_t r, int ci, char *buf, size_t bufsz) {
    if (tf_batch_is_null(b, r, ci)) return NULL;
    switch (b->col_types[ci]) {
        case TF_TYPE_STRING: return tf_batch_get_string(b, r, ci);
        case TF_TYPE_INT64:
            snprintf(buf, bufsz, "%lld", (long long)tf_batch_get_int64(b, r, ci));
            return buf;
        case TF_TYPE_FLOAT64:
            snprintf(buf, bufsz, "%.17g", tf_batch_get_float64(b, r, ci));
            return buf;
        case TF_TYPE_BOOL:
            return tf_batch_get_bool(b, r, ci) ? "true" : "false";
        default: return NULL;
    }
}

static int find_entry(const label_encode_state *st, const char *val) {
    for (size_t i = 0; i < st->n_entries; i++) {
        if (strcmp(st->entries[i].value, val) == 0) return (int)i;
    }
    return -1;
}

static int assign_entry(label_encode_state *st, const char *val) {
    int existing = find_entry(st, val);
    if (existing >= 0) return existing;
    if (st->n_entries >= st->cap) {
        size_t min_cap = 0, newcap = 0;
        if (tf_size_add(st->n_entries, 1, &min_cap) != TF_OK ||
            tf_size_grow_pow2(st->cap, min_cap, 16, &newcap) != TF_OK) {
            return -1;
        }
        label_entry *tmp = tf_reallocarray_checked(st->entries, newcap,
                                                   sizeof(label_entry));
        if (!tmp) return -1;
        st->entries = tmp;
        st->cap = newcap;
    }
    char *value = strdup(val);
    if (!value) return -1;
    st->entries[st->n_entries].value = value;
    st->entries[st->n_entries].label = st->next_label++;
    st->n_entries++;
    return (int)(st->n_entries - 1);
}

static int resolve_other(label_encode_state *st, int64_t *label,
                         tf_side_channels *side) {
    int other = find_entry(st, OTHER_CATEGORY);
    if (other >= 0) { *label = st->entries[other].label; return 0; }
    if (st->max_categories > 0 && st->n_entries >= st->max_categories) {
        if (label_limit_error(st, OTHER_CATEGORY, side) != TF_OK) return -1;
        return -1;
    }
    other = assign_entry(st, OTHER_CATEGORY);
    if (other < 0) return -1;
    if (label_check_state_bytes(st, side) != TF_OK) return -1;
    *label = st->entries[other].label;
    return 0;
}

static int resolve_unknown(label_encode_state *st, const char *val,
                           int64_t *label, int *is_null,
                           tf_side_channels *side) {
    switch (st->unknown) {
        case TF_CAT_UNKNOWN_NULL:
            *is_null = 1;
            return 0;
        case TF_CAT_UNKNOWN_OTHER:
            return resolve_other(st, label, side);
        case TF_CAT_UNKNOWN_ERROR:
            if (label_unknown_error(st, val, side) != TF_OK) return -1;
            return -1;
        case TF_CAT_UNKNOWN_ADD:
        default:
            break;
    }
    return 1;
}

static int resolve_label(label_encode_state *st, const char *val,
                         int64_t *label, int *is_null,
                         tf_side_channels *side) {
    *label = 0;
    *is_null = 0;
    if (!val) { *is_null = 1; return 0; }

    int existing = find_entry(st, val);
    if (existing >= 0) { *label = st->entries[existing].label; return 0; }

    if (st->categories_declared) {
        int rc = resolve_unknown(st, val, label, is_null, side);
        return rc == 0 ? 0 : -1;
    }

    if (st->max_categories > 0 && st->unknown == TF_CAT_UNKNOWN_OTHER) {
        int other = find_entry(st, OTHER_CATEGORY);
        size_t reserve = other >= 0 ? 0 : 1;
        if (st->n_entries + reserve >= st->max_categories) {
            return resolve_other(st, label, side);
        }
    }

    if (st->max_categories > 0 && st->n_entries >= st->max_categories) {
        int rc = resolve_unknown(st, val, label, is_null, side);
        if (rc == 0) return 0;
        if (label_limit_error(st, val, side) != TF_OK) return -1;
        return -1;
    }

    int added = assign_entry(st, val);
    if (added < 0) return -1;
    if (label_check_state_bytes(st, side) != TF_OK) return -1;
    *label = st->entries[added].label;
    return 0;
}

static int label_encode_process(tf_step *self, tf_batch *in, tf_batch **out,
                                tf_side_channels *side) {
    label_encode_state *st = self->state;
    *out = NULL;

    size_t out_cols = 0;
    if (tf_size_add(in->n_cols, 1, &out_cols) != TF_OK) return TF_ERROR;
    tf_batch *ob = tf_batch_create(out_cols, in->n_rows);
    if (!ob) return TF_ERROR;
    const char *extra_names[1] = { st->result };
    const tf_type extra_types[1] = { TF_TYPE_INT64 };
    if (tf_batch_clone_with_extra_cols(ob, in, extra_names, extra_types, 1) != TF_OK) {
        tf_batch_free(ob);
        return TF_ERROR;
    }

    int ci = tf_batch_col_index(in, st->column);

    for (size_t r = 0; r < in->n_rows; r++) {
        if (tf_batch_copy_row(ob, r, in, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }

        int rc = TF_OK;
        if (ci < 0 || tf_batch_is_null(in, r, ci)) {
            rc = tf_batch_set_null(ob, r, in->n_cols);
        } else {
            char buf[64];
            const char *val = get_string_value(in, r, ci, buf, sizeof(buf));
            int64_t label = 0;
            int is_null = 0;
            if (resolve_label(st, val, &label, &is_null, side) != 0) {
                tf_batch_free(ob);
                return TF_ERROR;
            }
            rc = is_null ? tf_batch_set_null(ob, r, in->n_cols)
                         : tf_batch_set_int64(ob, r, in->n_cols, label);
        }
        if (rc != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
        if (label_encode_expose_row(ob, r) != TF_OK) {
            tf_batch_free(ob);
            return TF_ERROR;
        }
    }

    *out = ob;
    return TF_OK;
}

static int label_encode_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static size_t label_category_value_bytes(const label_encode_state *st) {
    size_t total = 0;
    if (!st) return 0;
    for (size_t i = 0; i < st->n_entries; i++) {
        if (st->entries[i].value) total += strlen(st->entries[i].value) + 1;
    }
    return total;
}

static size_t label_retained_state_bytes(const label_encode_state *st) {
    if (!st) return 0;
    size_t total = st->cap * sizeof(label_entry);
    total += label_category_value_bytes(st);
    if (st->column) total += strlen(st->column) + 1;
    if (st->result) total += strlen(st->result) + 1;
    return total;
}

static int label_encode_append_stats(tf_step *self, tf_buffer *out) {
    if (!self || !self->state || !out) return TF_ERROR;
    label_encode_state *st = self->state;
    char buf[280];
    snprintf(buf, sizeof(buf),
             ",\"tracked_categories\":%zu,\"category_value_bytes\":%zu,"
             "\"retained_state_bytes\":%zu,\"max_state_bytes\":%zu",
             st->n_entries, label_category_value_bytes(st),
             label_retained_state_bytes(st), st->max_state_bytes);
    return tf_buffer_write_str(out, buf);
}

static void label_encode_state_free(label_encode_state *st) {
    if (!st) return;
    for (size_t i = 0; i < st->n_entries; i++)
        free(st->entries[i].value);
    free(st->entries);
    free(st->column);
    free(st->result);
    free(st);
}

static void label_encode_destroy(tf_step *self) {
    if (self) {
        label_encode_state_free(self->state);
        free(self);
    }
}

static int parse_unknown_policy(const cJSON *args, tf_cat_unknown_policy *policy,
                                int *specified) {
    cJSON *unknown_j = cJSON_GetObjectItemCaseSensitive(args, "unknown");
    *specified = 0;
    if (!unknown_j) return 0;
    if (!cJSON_IsString(unknown_j)) {
        tf_set_last_error("label-encode: unknown must be one of error, other, null");
        return -1;
    }
    *specified = 1;
    if (strcmp(unknown_j->valuestring, "error") == 0) *policy = TF_CAT_UNKNOWN_ERROR;
    else if (strcmp(unknown_j->valuestring, "other") == 0) *policy = TF_CAT_UNKNOWN_OTHER;
    else if (strcmp(unknown_j->valuestring, "null") == 0) *policy = TF_CAT_UNKNOWN_NULL;
    else {
        tf_set_last_error("label-encode: unknown must be one of error, other, null");
        return -1;
    }
    return 0;
}

tf_step *tf_label_encode_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j)) return NULL;

    label_encode_state *st = calloc(1, sizeof(label_encode_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    if (!st->column) { label_encode_state_free(st); return NULL; }
    st->unknown = TF_CAT_UNKNOWN_ADD;

    cJSON *res_j = cJSON_GetObjectItemCaseSensitive(args, "result");
    if (cJSON_IsString(res_j)) {
        st->result = strdup(res_j->valuestring);
    } else {
        char buf[256];
        snprintf(buf, sizeof(buf), "%s_encoded", st->column);
        st->result = strdup(buf);
    }
    if (!st->result) { label_encode_state_free(st); return NULL; }

    size_t parsed_size = 0;
    int has_max_categories = tf_json_get_size_arg(args, "max_categories",
                                                  1, TF_MAX_COUNT_ARG,
                                                  &parsed_size, "label-encode");
    if (has_max_categories < 0) { label_encode_state_free(st); return NULL; }
    if (has_max_categories > 0) st->max_categories = parsed_size;

    int has_max_state = tf_json_get_size_arg(args, "max_state_bytes",
                                             1, TF_MAX_STATE_BYTES,
                                             &parsed_size, "label-encode");
    if (has_max_state < 0) { label_encode_state_free(st); return NULL; }
    if (has_max_state > 0) st->max_state_bytes = parsed_size;

    int unknown_specified = 0;
    if (parse_unknown_policy(args, &st->unknown, &unknown_specified) != 0) {
        label_encode_state_free(st);
        return NULL;
    }

    cJSON *cats_j = cJSON_GetObjectItemCaseSensitive(args, "categories");
    if (cats_j) {
        if (!cJSON_IsArray(cats_j)) {
            tf_set_last_error("label-encode: categories must be an array");
            label_encode_state_free(st);
            return NULL;
        }
        st->categories_declared = 1;
        cJSON *item = NULL;
        cJSON_ArrayForEach(item, cats_j) {
            if (!cJSON_IsString(item)) {
                tf_set_last_error("label-encode: categories must contain strings");
                label_encode_state_free(st);
                return NULL;
            }
            if (find_entry(st, item->valuestring) < 0 &&
                st->max_categories > 0 && st->n_entries >= st->max_categories) {
                tf_set_last_error("label-encode: categories exceed max_categories");
                label_encode_state_free(st);
                return NULL;
            }
            if (assign_entry(st, item->valuestring) < 0) {
                label_encode_state_free(st);
                return NULL;
            }
            if (label_check_state_bytes(st, NULL) != TF_OK) {
                label_encode_state_free(st);
                return NULL;
            }
        }
    }

    if (st->categories_declared && !unknown_specified)
        st->unknown = TF_CAT_UNKNOWN_ERROR;

    if (st->categories_declared && st->unknown == TF_CAT_UNKNOWN_OTHER &&
        find_entry(st, OTHER_CATEGORY) < 0) {
        if (st->max_categories > 0 && st->n_entries >= st->max_categories) {
            tf_set_last_error("label-encode: max_categories leaves no room for other category");
            label_encode_state_free(st);
            return NULL;
        }
        if (assign_entry(st, OTHER_CATEGORY) < 0) {
            label_encode_state_free(st);
            return NULL;
        }
        if (label_check_state_bytes(st, NULL) != TF_OK) {
            label_encode_state_free(st);
            return NULL;
        }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { label_encode_state_free(st); return NULL; }
    step->process = label_encode_process;
    step->flush = label_encode_flush;
    step->append_stats = label_encode_append_stats;
    step->destroy = label_encode_destroy;
    step->state = st;
    return step;
}
