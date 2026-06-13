/*
 * op_onehot.c — One-hot encoding of a categorical column.
 * Expands a single column into N binary (0/1) columns.
 *
 * Config: {"column": "city", "drop": false,
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

typedef struct {
    char  *value;     /* category value string */
    char  *col_name;  /* generated column name: "column_value" */
} onehot_category;

typedef struct {
    char                  *column;
    int                    drop;    /* drop original column */
    size_t                 max_categories; /* 0 = unlimited */
    size_t                 max_state_bytes; /* 0 = unlimited */
    int                    categories_declared;
    tf_cat_unknown_policy  unknown;
    onehot_category       *cats;
    size_t                 n_cats;
    size_t                 cap;
} onehot_state;

static const char *OTHER_CATEGORY = "__other__";

static size_t onehot_retained_state_bytes(const onehot_state *st);

static void onehot_write_error(tf_side_channels *side, const char *msg) {
    tf_set_last_error(msg);
    if (side && side->errors) {
        tf_buffer_write_str(side->errors, msg);
        tf_buffer_write_str(side->errors, "\n");
    }
}

static void onehot_unknown_error(const onehot_state *st, const char *val,
                                 tf_side_channels *side) {
    char msg[256];
    snprintf(msg, sizeof(msg),
             "onehot: unknown category '%s' for column '%s'",
             val ? val : "", st->column ? st->column : "");
    onehot_write_error(side, msg);
}

static void onehot_limit_error(const onehot_state *st, const char *val,
                               tf_side_channels *side) {
    char msg[256];
    snprintf(msg, sizeof(msg),
             "onehot: max_categories=%zu exceeded while tracking category '%s'",
             st->max_categories, val ? val : "");
    onehot_write_error(side, msg);
}

static int onehot_check_state_bytes(onehot_state *st, tf_side_channels *side) {
    if (!st || st->max_state_bytes == 0) return TF_OK;
    size_t retained = onehot_retained_state_bytes(st);
    if (retained <= st->max_state_bytes) return TF_OK;
    char msg[192];
    snprintf(msg, sizeof(msg),
             "onehot: max_state_bytes=%zu exceeded while tracking categories (%zu bytes retained)",
             st->max_state_bytes, retained);
    onehot_write_error(side, msg);
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

static int find_category(const onehot_state *st, const char *val) {
    for (size_t i = 0; i < st->n_cats; i++) {
        if (strcmp(st->cats[i].value, val) == 0) return (int)i;
    }
    return -1;
}

static int add_category(onehot_state *st, const char *val) {
    int existing = find_category(st, val);
    if (existing >= 0) return existing;
    if (st->n_cats >= st->cap) {
        size_t newcap = st->cap ? st->cap * 2 : 16;
        onehot_category *tmp = realloc(st->cats, newcap * sizeof(onehot_category));
        if (!tmp) return -1;
        st->cats = tmp;
        st->cap = newcap;
    }
    char *value = strdup(val);
    if (!value) return -1;
    char namebuf[512];
    snprintf(namebuf, sizeof(namebuf), "%s_%s", st->column, val);
    char *col_name = strdup(namebuf);
    if (!col_name) { free(value); return -1; }
    st->cats[st->n_cats].value = value;
    st->cats[st->n_cats].col_name = col_name;
    st->n_cats++;
    return (int)(st->n_cats - 1);
}

static int resolve_other(onehot_state *st, int *idx, tf_side_channels *side) {
    int other = find_category(st, OTHER_CATEGORY);
    if (other >= 0) { *idx = other; return 0; }
    if (st->max_categories > 0 && st->n_cats >= st->max_categories) {
        onehot_limit_error(st, OTHER_CATEGORY, side);
        return -1;
    }
    other = add_category(st, OTHER_CATEGORY);
    if (other < 0) return -1;
    if (onehot_check_state_bytes(st, side) != TF_OK) return -1;
    *idx = other;
    return 0;
}

static int resolve_unknown(onehot_state *st, const char *val, int *idx,
                           tf_side_channels *side) {
    switch (st->unknown) {
        case TF_CAT_UNKNOWN_NULL:
            *idx = -1;
            return 0;
        case TF_CAT_UNKNOWN_OTHER:
            return resolve_other(st, idx, side);
        case TF_CAT_UNKNOWN_ERROR:
            onehot_unknown_error(st, val, side);
            return -1;
        case TF_CAT_UNKNOWN_ADD:
        default:
            break;
    }
    return 1;
}

static int resolve_category(onehot_state *st, const char *val, int *idx,
                            tf_side_channels *side) {
    *idx = -1;
    if (!val) return 0;

    int existing = find_category(st, val);
    if (existing >= 0) { *idx = existing; return 0; }

    if (st->categories_declared) {
        int rc = resolve_unknown(st, val, idx, side);
        return rc == 0 ? 0 : -1;
    }

    if (st->max_categories > 0 && st->unknown == TF_CAT_UNKNOWN_OTHER) {
        int other = find_category(st, OTHER_CATEGORY);
        size_t reserve = other >= 0 ? 0 : 1;
        if (st->n_cats + reserve >= st->max_categories) {
            return resolve_other(st, idx, side);
        }
    }

    if (st->max_categories > 0 && st->n_cats >= st->max_categories) {
        int rc = resolve_unknown(st, val, idx, side);
        if (rc == 0) return 0;
        onehot_limit_error(st, val, side);
        return -1;
    }

    int added = add_category(st, val);
    if (added < 0) return -1;
    if (onehot_check_state_bytes(st, side) != TF_OK) return -1;
    *idx = added;
    return 0;
}

static int onehot_process(tf_step *self, tf_batch *in, tf_batch **out,
                          tf_side_channels *side) {
    onehot_state *st = self->state;
    *out = NULL;

    int ci = tf_batch_col_index(in, st->column);
    int *matches = malloc((in->n_rows ? in->n_rows : 1) * sizeof(int));
    if (!matches) return TF_ERROR;
    for (size_t r = 0; r < in->n_rows; r++) matches[r] = -1;

    if (ci >= 0) {
        char buf[64];
        for (size_t r = 0; r < in->n_rows; r++) {
            const char *val = get_string_value(in, r, ci, buf, sizeof(buf));
            if (resolve_category(st, val, &matches[r], side) != 0) {
                free(matches);
                return TF_ERROR;
            }
        }
    }

    size_t input_cols = (st->drop && ci >= 0 && in->n_cols > 0) ? in->n_cols - 1 : in->n_cols;
    size_t out_cols = input_cols + st->n_cats;
    tf_batch *ob = tf_batch_create(out_cols, in->n_rows);
    if (!ob) { free(matches); return TF_ERROR; }

    size_t oc = 0;
    for (size_t c = 0; c < in->n_cols; c++) {
        if (st->drop && ci >= 0 && c == (size_t)ci) continue;
        tf_batch_set_schema(ob, oc, in->col_names[c], in->col_types[c]);
        oc++;
    }
    for (size_t i = 0; i < st->n_cats; i++) {
        tf_batch_set_schema(ob, oc + i, st->cats[i].col_name, TF_TYPE_INT64);
    }

    for (size_t r = 0; r < in->n_rows; r++) {
        oc = 0;
        for (size_t c = 0; c < in->n_cols; c++) {
            if (st->drop && ci >= 0 && c == (size_t)ci) continue;
            if (tf_batch_is_null(in, r, c)) {
                tf_batch_set_null(ob, r, oc);
            } else {
                switch (in->col_types[c]) {
                    case TF_TYPE_STRING:
                        tf_batch_set_string(ob, r, oc, tf_batch_get_string(in, r, c)); break;
                    case TF_TYPE_INT64:
                        tf_batch_set_int64(ob, r, oc, tf_batch_get_int64(in, r, c)); break;
                    case TF_TYPE_FLOAT64:
                        tf_batch_set_float64(ob, r, oc, tf_batch_get_float64(in, r, c)); break;
                    case TF_TYPE_BOOL:
                        tf_batch_set_bool(ob, r, oc, tf_batch_get_bool(in, r, c)); break;
                    case TF_TYPE_DATE:
                        tf_batch_set_date(ob, r, oc, tf_batch_get_date(in, r, c)); break;
                    case TF_TYPE_TIMESTAMP:
                        tf_batch_set_timestamp(ob, r, oc, tf_batch_get_timestamp(in, r, c)); break;
                    default: tf_batch_set_null(ob, r, oc); break;
                }
            }
            oc++;
        }

        int match = matches[r];
        for (size_t i = 0; i < st->n_cats; i++) {
            tf_batch_set_int64(ob, r, oc + i, (int)i == match ? 1 : 0);
        }
        ob->n_rows = r + 1;
    }

    free(matches);
    *out = ob;
    return TF_OK;
}

static int onehot_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static size_t onehot_category_value_bytes(const onehot_state *st) {
    size_t total = 0;
    if (!st) return 0;
    for (size_t i = 0; i < st->n_cats; i++) {
        if (st->cats[i].value) total += strlen(st->cats[i].value) + 1;
    }
    return total;
}

static size_t onehot_category_output_name_bytes(const onehot_state *st) {
    size_t total = 0;
    if (!st) return 0;
    for (size_t i = 0; i < st->n_cats; i++) {
        if (st->cats[i].col_name) total += strlen(st->cats[i].col_name) + 1;
    }
    return total;
}

static size_t onehot_retained_state_bytes(const onehot_state *st) {
    if (!st) return 0;
    size_t total = st->cap * sizeof(onehot_category);
    total += onehot_category_value_bytes(st);
    total += onehot_category_output_name_bytes(st);
    if (st->column) total += strlen(st->column) + 1;
    return total;
}

static int onehot_append_stats(tf_step *self, tf_buffer *out) {
    if (!self || !self->state || !out) return TF_ERROR;
    onehot_state *st = self->state;
    char buf[320];
    snprintf(buf, sizeof(buf),
             ",\"tracked_categories\":%zu,\"category_value_bytes\":%zu,"
             "\"category_output_name_bytes\":%zu,\"retained_state_bytes\":%zu,"
             "\"max_state_bytes\":%zu",
             st->n_cats, onehot_category_value_bytes(st),
             onehot_category_output_name_bytes(st), onehot_retained_state_bytes(st),
             st->max_state_bytes);
    return tf_buffer_write_str(out, buf);
}

static void onehot_state_free(onehot_state *st) {
    if (!st) return;
    for (size_t i = 0; i < st->n_cats; i++) {
        free(st->cats[i].value);
        free(st->cats[i].col_name);
    }
    free(st->cats);
    free(st->column);
    free(st);
}

static void onehot_destroy(tf_step *self) {
    if (self) {
        onehot_state_free(self->state);
        free(self);
    }
}

static int parse_unknown_policy(const cJSON *args, tf_cat_unknown_policy *policy,
                                int *specified) {
    cJSON *unknown_j = cJSON_GetObjectItemCaseSensitive(args, "unknown");
    *specified = 0;
    if (!unknown_j) return 0;
    if (!cJSON_IsString(unknown_j)) {
        tf_set_last_error("onehot: unknown must be one of error, other, null");
        return -1;
    }
    *specified = 1;
    if (strcmp(unknown_j->valuestring, "error") == 0) *policy = TF_CAT_UNKNOWN_ERROR;
    else if (strcmp(unknown_j->valuestring, "other") == 0) *policy = TF_CAT_UNKNOWN_OTHER;
    else if (strcmp(unknown_j->valuestring, "null") == 0) *policy = TF_CAT_UNKNOWN_NULL;
    else {
        tf_set_last_error("onehot: unknown must be one of error, other, null");
        return -1;
    }
    return 0;
}

tf_step *tf_onehot_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *col_j = cJSON_GetObjectItemCaseSensitive(args, "column");
    if (!cJSON_IsString(col_j)) return NULL;

    onehot_state *st = calloc(1, sizeof(onehot_state));
    if (!st) return NULL;
    st->column = strdup(col_j->valuestring);
    if (!st->column) { onehot_state_free(st); return NULL; }
    st->unknown = TF_CAT_UNKNOWN_ADD;

    cJSON *drop_j = cJSON_GetObjectItemCaseSensitive(args, "drop");
    st->drop = cJSON_IsBool(drop_j) && cJSON_IsTrue(drop_j) ? 1 : 0;

    cJSON *max_j = cJSON_GetObjectItemCaseSensitive(args, "max_categories");
    if (max_j) {
        if (!cJSON_IsNumber(max_j) || max_j->valuedouble <= 0) {
            tf_set_last_error("onehot: max_categories must be positive");
            onehot_state_free(st);
            return NULL;
        }
        st->max_categories = (size_t)max_j->valuedouble;
    }

    cJSON *max_state_j = cJSON_GetObjectItemCaseSensitive(args, "max_state_bytes");
    if (max_state_j) {
        if (!cJSON_IsNumber(max_state_j) || max_state_j->valuedouble <= 0) {
            tf_set_last_error("onehot: max_state_bytes must be positive");
            onehot_state_free(st);
            return NULL;
        }
        st->max_state_bytes = (size_t)max_state_j->valuedouble;
    }

    int unknown_specified = 0;
    if (parse_unknown_policy(args, &st->unknown, &unknown_specified) != 0) {
        onehot_state_free(st);
        return NULL;
    }

    cJSON *cats_j = cJSON_GetObjectItemCaseSensitive(args, "categories");
    if (cats_j) {
        if (!cJSON_IsArray(cats_j)) {
            tf_set_last_error("onehot: categories must be an array");
            onehot_state_free(st);
            return NULL;
        }
        st->categories_declared = 1;
        cJSON *item = NULL;
        cJSON_ArrayForEach(item, cats_j) {
            if (!cJSON_IsString(item)) {
                tf_set_last_error("onehot: categories must contain strings");
                onehot_state_free(st);
                return NULL;
            }
            if (find_category(st, item->valuestring) < 0 &&
                st->max_categories > 0 && st->n_cats >= st->max_categories) {
                tf_set_last_error("onehot: categories exceed max_categories");
                onehot_state_free(st);
                return NULL;
            }
            if (add_category(st, item->valuestring) < 0) {
                onehot_state_free(st);
                return NULL;
            }
            if (onehot_check_state_bytes(st, NULL) != TF_OK) {
                onehot_state_free(st);
                return NULL;
            }
        }
    }

    if (st->categories_declared && !unknown_specified)
        st->unknown = TF_CAT_UNKNOWN_ERROR;

    if (st->categories_declared && st->unknown == TF_CAT_UNKNOWN_OTHER &&
        find_category(st, OTHER_CATEGORY) < 0) {
        if (st->max_categories > 0 && st->n_cats >= st->max_categories) {
            tf_set_last_error("onehot: max_categories leaves no room for other category");
            onehot_state_free(st);
            return NULL;
        }
        if (add_category(st, OTHER_CATEGORY) < 0) {
            onehot_state_free(st);
            return NULL;
        }
        if (onehot_check_state_bytes(st, NULL) != TF_OK) {
            onehot_state_free(st);
            return NULL;
        }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { onehot_state_free(st); return NULL; }
    step->process = onehot_process;
    step->flush = onehot_flush;
    step->append_stats = onehot_append_stats;
    step->destroy = onehot_destroy;
    step->state = st;
    return step;
}
