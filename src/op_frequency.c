/*
 * op_frequency.c — Value counts. Hash map → emit sorted by count desc.
 *
 * Config: {"columns": ["city"]}
 *   or {} for frequency of all columns concatenated.
 */

#include "internal.h"
#include "cJSON.h"
#include "date_utils.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char    **keys;
    size_t   *counts;
    size_t    count;
    size_t    cap;
    size_t    key_bytes;
} freq_map;

typedef struct {
    char  **cols;
    size_t  n_cols;
    size_t  max_values;  /* 0 = unlimited */
    size_t  max_state_bytes; /* 0 = unlimited */
    int     overflow_other;
    char   *other_label;
    size_t  other_count;
    size_t  row_index;
    size_t  audit_limit;
    size_t  audit_emitted;
    int     audit;
    tf_audit_options audit_opts;
    freq_map map;
} frequency_state;

static size_t frequency_retained_state_bytes(const frequency_state *st);

static int frequency_write_error(tf_side_channels *side, const char *msg) {
    return tf_side_write_error(side, msg);
}

static int frequency_limit_error(const frequency_state *st, tf_side_channels *side) {
    char msg[160];
    snprintf(msg, sizeof(msg),
             "frequency: max_values=%zu exceeded while tracking exact counts",
             st->max_values);
    return frequency_write_error(side, msg);
}

static int frequency_check_state_bytes(frequency_state *st, tf_side_channels *side) {
    if (!st || st->max_state_bytes == 0) return 0;
    size_t retained = frequency_retained_state_bytes(st);
    if (retained <= st->max_state_bytes) return 0;
    char msg[208];
    snprintf(msg, sizeof(msg),
             "frequency: max_state_bytes=%zu exceeded while tracking exact counts (%zu bytes retained)",
             st->max_state_bytes, retained);
    if (frequency_write_error(side, msg) != TF_OK) return -1;
    return -1;
}

static int freq_buf_ensure(char **buf, size_t *cap, size_t len, size_t extra) {
    size_t need = 0;
    if (!buf || !*buf || !cap ||
        tf_size_add(len, extra, &need) != TF_OK ||
        tf_size_add(need, 1, &need) != TF_OK) {
        return TF_ERROR;
    }
    if (need <= *cap) return TF_OK;

    size_t new_cap = 0;
    if (tf_size_grow_pow2(*cap, need, 256, &new_cap) != TF_OK) return TF_ERROR;
    char *tmp = tf_reallocarray_checked(*buf, new_cap, sizeof(char));
    if (!tmp) return TF_ERROR;
    *buf = tmp;
    *cap = new_cap;
    return TF_OK;
}

static char *build_freq_key(const tf_batch *b, size_t row,
                            int *col_indices, size_t n_keys) {
    size_t buf_cap = 256;
    char *buf = tf_mallocarray_checked(buf_cap, sizeof(char));
    if (!buf) return NULL;
    size_t buf_len = 0;
    for (size_t k = 0; k < n_keys; k++) {
        int c = col_indices[k];
        if (k > 0) {
            if (freq_buf_ensure(&buf, &buf_cap, buf_len, 1) != TF_OK) {
                free(buf);
                return NULL;
            }
            buf[buf_len++] = '\x01';
        }
        char val_buf[64];
        const char *val;
        size_t val_len;
        if (c < 0 || tf_batch_is_null(b, row, c)) {
            val = "\\N"; val_len = 2;
        } else {
            switch (b->col_types[c]) {
                case TF_TYPE_STRING: val = tf_batch_get_string(b, row, c); val_len = strlen(val); break;
                case TF_TYPE_INT64:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%lld", (long long)tf_batch_get_int64(b, row, c));
                    val = val_buf; break;
                case TF_TYPE_FLOAT64:
                    if (tf_format_float64(val_buf, sizeof(val_buf), tf_batch_get_float64(b, row, c)) != TF_OK) {
                        free(buf);
                        return NULL;
                    }
                    val_len = strlen(val_buf);
                    val = val_buf; break;
                case TF_TYPE_BOOL:
                    val = tf_batch_get_bool(b, row, c) ? "T" : "F"; val_len = 1; break;
                case TF_TYPE_DATE:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%d", (int)tf_batch_get_date(b, row, c));
                    val = val_buf; break;
                case TF_TYPE_TIMESTAMP:
                    val_len = snprintf(val_buf, sizeof(val_buf), "%lld", (long long)tf_batch_get_timestamp(b, row, c));
                    val = val_buf; break;
                default: val = "\\N"; val_len = 2; break;
            }
        }
        if (freq_buf_ensure(&buf, &buf_cap, buf_len, val_len) != TF_OK) {
            free(buf);
            return NULL;
        }
        memcpy(buf + buf_len, val, val_len);
        buf_len += val_len;
    }
    buf[buf_len] = '\0';
    return buf;
}



static const char *frequency_format_key_for_audit(frequency_state *st, const char *key,
                                                  const tf_batch *b, size_t row,
                                                  char *buf, size_t buf_size) {
    const char *src = key ? key : "";
    if (!st || !buf || buf_size == 0) return src;
    int redact = 0;
    int hash = 0;
    if (b && row < b->n_rows) {
        if (st->n_cols > 0) {
            for (size_t i = 0; i < st->n_cols; i++) {
                int ci = tf_batch_col_index(b, st->cols[i]);
                if (ci < 0) continue;
                const char *name = b->col_names[ci] ? b->col_names[ci] : "";
                if (tf_audit_column_is_redacted(&st->audit_opts, name)) redact = 1;
                if (tf_audit_column_is_hashed(&st->audit_opts, name)) hash = 1;
            }
        } else {
            for (size_t c = 0; c < b->n_cols; c++) {
                const char *name = b->col_names[c] ? b->col_names[c] : "";
                if (tf_audit_column_is_redacted(&st->audit_opts, name)) redact = 1;
                if (tf_audit_column_is_hashed(&st->audit_opts, name)) hash = 1;
            }
        }
    }
    if (redact) {
        snprintf(buf, buf_size, "[REDACTED]");
        return buf;
    }
    if (hash) {
        if (tf_audit_hash_string(src, buf, buf_size) == TF_OK) return buf;
        return "[HASHED]";
    }
    if (st->audit_opts.max_cell_bytes > 0 && strlen(src) > st->audit_opts.max_cell_bytes) {
        size_t n = st->audit_opts.max_cell_bytes;
        if (n + 4 > buf_size) n = buf_size > 4 ? buf_size - 4 : 0;
        if (n > 0) memcpy(buf, src, n);
        snprintf(buf + n, buf_size - n, "...");
        return buf;
    }
    return src;
}

static int emit_frequency_overflow_audit(frequency_state *st, const char *key,
                                         const tf_batch *b, size_t row,
                                         size_t row_no, tf_side_channels *side) {
    if (!st->audit || st->audit_emitted >= st->audit_limit || !side || !side->stats) return TF_OK;
    cJSON *obj = cJSON_CreateObject();
    if (!obj) return TF_ERROR;
    cJSON_AddStringToObject(obj, "type", "audit");
    cJSON_AddStringToObject(obj, "op", "frequency");
    cJSON_AddStringToObject(obj, "event", "category_overflow");
    cJSON_AddStringToObject(obj, "reason", "max_values_overflow");
    cJSON_AddStringToObject(obj, "channel", "audit");
    cJSON_AddStringToObject(obj, "action", "other");
    char key_buf[256];
    const char *safe_key = frequency_format_key_for_audit(st, key, b, row, key_buf, sizeof(key_buf));
    cJSON_AddStringToObject(obj, "value", safe_key ? safe_key : "");
    cJSON_AddStringToObject(obj, "bucket", st->other_label ? st->other_label : "__other__");
    cJSON_AddNumberToObject(obj, "row", (double)row_no);
    cJSON_AddNumberToObject(obj, "max_values", (double)st->max_values);
    cJSON_AddNumberToObject(obj, "tracked_values", (double)st->map.count);
    cJSON *row_obj = tf_audit_row_to_json(b, row, &st->audit_opts);
    if (row_obj) cJSON_AddItemToObject(obj, "data", row_obj);
    char *line = cJSON_PrintUnformatted(obj);
    cJSON_Delete(obj);
    if (!line) return TF_ERROR;
    int rc = tf_buffer_write_line(side->stats, line);
    free(line);
    if (rc == TF_OK) st->audit_emitted++;
    return rc;
}

static int freq_find(const freq_map *map, const char *key, size_t *idx_out) {
    for (size_t i = 0; i < map->count; i++) {
        if (strcmp(map->keys[i], key) == 0) {
            if (idx_out) *idx_out = i;
            return 1;
        }
    }
    return 0;
}

static int freq_add(frequency_state *st, const char *key, const tf_batch *b, size_t row, size_t row_no, tf_side_channels *side) {
    freq_map *map = &st->map;
    if (st->overflow_other && st->other_label && strcmp(key, st->other_label) == 0) {
        if (tf_size_add(st->other_count, 1, &st->other_count) != TF_OK) return -1;
        return 0;
    }
    size_t idx = 0;
    if (freq_find(map, key, &idx)) {
        if (tf_size_add(map->counts[idx], 1, &map->counts[idx]) != TF_OK) return -1;
        return 0;
    }
    if (st->max_values > 0 && map->count >= st->max_values) {
        if (st->overflow_other) {
            if (tf_size_add(st->other_count, 1, &st->other_count) != TF_OK) return -1;
            if (emit_frequency_overflow_audit(st, key, b, row, row_no, side) != TF_OK) return -1;
            return 0;
        }
        if (frequency_limit_error(st, side) != TF_OK) return -1;
        return -1;
    }
    if (map->count >= map->cap) {
        size_t need = 0;
        size_t new_cap = 0;
        if (tf_size_add(map->count, 1, &need) != TF_OK ||
            tf_size_grow_pow2(map->cap, need, 64, &new_cap) != TF_OK) {
            return -1;
        }
        char **new_keys = tf_callocarray_checked(new_cap, sizeof(char *));
        size_t *new_counts = tf_callocarray_checked(new_cap, sizeof(size_t));
        if (!new_keys || !new_counts) {
            free(new_keys);
            free(new_counts);
            return -1;
        }
        if (map->count > 0) {
            size_t key_bytes = 0;
            size_t count_bytes = 0;
            if (tf_size_mul(map->count, sizeof(char *), &key_bytes) != TF_OK ||
                tf_size_mul(map->count, sizeof(size_t), &count_bytes) != TF_OK) {
                free(new_keys);
                free(new_counts);
                return -1;
            }
            memcpy(new_keys, map->keys, key_bytes);
            memcpy(new_counts, map->counts, count_bytes);
        }
        free(map->keys);
        free(map->counts);
        map->keys = new_keys;
        map->counts = new_counts;
        map->cap = new_cap;
    }
    char *dup = strdup(key);
    if (!dup) return -1;
    size_t key_bytes_delta = 0;
    size_t new_key_bytes = 0;
    if (tf_size_add(strlen(dup), 1, &key_bytes_delta) != TF_OK ||
        tf_size_add(map->key_bytes, key_bytes_delta, &new_key_bytes) != TF_OK) {
        free(dup);
        return -1;
    }
    map->keys[map->count] = dup;
    map->key_bytes = new_key_bytes;
    map->counts[map->count] = 1;
    map->count++;
    if (frequency_check_state_bytes(st, side) != 0) return -1;
    return 0;
}

static int frequency_process(tf_step *self, tf_batch *in, tf_batch **out,
                             tf_side_channels *side) {
    frequency_state *st = self->state;
    *out = NULL;

    size_t n_keys;
    int *col_indices;
    if (st->n_cols > 0) {
        n_keys = st->n_cols;
        col_indices = tf_mallocarray_checked(n_keys ? n_keys : 1, sizeof(int));
        if (!col_indices) return TF_ERROR;
        for (size_t k = 0; k < n_keys; k++)
            col_indices[k] = tf_batch_col_index(in, st->cols[k]);
    } else {
        n_keys = in->n_cols;
        col_indices = tf_mallocarray_checked(n_keys ? n_keys : 1, sizeof(int));
        if (!col_indices) return TF_ERROR;
        for (size_t k = 0; k < n_keys; k++) col_indices[k] = (int)k;
    }

    size_t row_base = st->row_index;
    for (size_t r = 0; r < in->n_rows; r++) {
        char *key = build_freq_key(in, r, col_indices, n_keys);
        if (!key) { free(col_indices); return TF_ERROR; }
        size_t row_no = 0;
        if (tf_size_add(row_base, r, &row_no) != TF_OK ||
            tf_size_add(row_no, 1, &row_no) != TF_OK ||
            freq_add(st, key, in, r, row_no, side) != 0) {
            free(key);
            free(col_indices);
            return TF_ERROR;
        }
        free(key);
    }

    if (tf_size_add(st->row_index, in->n_rows, &st->row_index) != TF_OK) {
        free(col_indices);
        return TF_ERROR;
    }
    free(col_indices);
    return TF_OK;
}

static int frequency_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)side;
    frequency_state *st = self->state;
    *out = NULL;

    size_t n = 0;
    if (tf_size_add(st->map.count, st->other_count > 0 ? 1u : 0u, &n) != TF_OK) return TF_ERROR;
    if (n == 0) return TF_OK;

    const char **keys = tf_mallocarray_checked(n, sizeof(char *));
    size_t *counts = tf_mallocarray_checked(n, sizeof(size_t));
    size_t *indices = tf_mallocarray_checked(n, sizeof(size_t));
    if (!keys || !counts || !indices) {
        free(keys); free(counts); free(indices);
        return TF_ERROR;
    }
    for (size_t i = 0; i < st->map.count; i++) {
        keys[i] = st->map.keys[i];
        counts[i] = st->map.counts[i];
    }
    if (st->other_count > 0) {
        keys[st->map.count] = st->other_label ? st->other_label : "__other__";
        counts[st->map.count] = st->other_count;
    }
    for (size_t i = 0; i < n; i++) indices[i] = i;

    /* Simple sort by count desc, then value asc for deterministic ties. */
    for (size_t i = 0; i < n - 1; i++) {
        for (size_t j = i + 1; j < n; j++) {
            size_t a = indices[i], b = indices[j];
            if (counts[b] > counts[a] ||
                (counts[b] == counts[a] && strcmp(keys[b], keys[a]) < 0)) {
                size_t tmp = indices[i]; indices[i] = indices[j]; indices[j] = tmp;
            }
        }
    }

    tf_batch *ob = tf_batch_create(2, n);
    if (!ob) { free(keys); free(counts); free(indices); return TF_ERROR; }
    if (tf_batch_set_schema(ob, 0, "value", TF_TYPE_STRING) != TF_OK ||
        tf_batch_set_schema(ob, 1, "count", TF_TYPE_INT64) != TF_OK) {
        tf_batch_free(ob);
        free(keys); free(counts); free(indices);
        return TF_ERROR;
    }

    for (size_t i = 0; i < n; i++) {
        if (tf_batch_ensure_capacity(ob, i + 1) != TF_OK ||
            tf_batch_set_string(ob, i, 0, keys[indices[i]]) != TF_OK ||
            tf_batch_set_int64(ob, i, 1, (int64_t)counts[indices[i]]) != TF_OK) {
            tf_batch_free(ob);
            free(keys); free(counts); free(indices);
            return TF_ERROR;
        }
        ob->n_rows = i + 1;
    }

    free(keys); free(counts); free(indices);
    *out = ob;
    return TF_OK;
}

static size_t frequency_retained_state_bytes(const frequency_state *st) {
    if (!st) return 0;
    size_t slot_width = 0;
    size_t slot_bytes = 0;
    size_t bytes = st->map.key_bytes;
    if (tf_size_add(sizeof(char *), sizeof(size_t), &slot_width) != TF_OK ||
        tf_size_mul(st->map.cap, slot_width, &slot_bytes) != TF_OK ||
        tf_size_add(bytes, slot_bytes, &bytes) != TF_OK) {
        return SIZE_MAX;
    }
    if (st->other_label) {
        size_t label_bytes = 0;
        if (tf_size_add(strlen(st->other_label), 1, &label_bytes) != TF_OK ||
            tf_size_add(bytes, label_bytes, &bytes) != TF_OK) {
            return SIZE_MAX;
        }
    }
    return bytes;
}

static int frequency_append_stats(tf_step *self, tf_buffer *out) {
    if (!self || !self->state || !out) return TF_ERROR;
    frequency_state *st = self->state;
    char buf[300];
    snprintf(buf, sizeof(buf),
             ",\"tracked_values\":%zu,\"tracked_key_bytes\":%zu,"
             "\"overflow_count\":%zu,\"retained_state_bytes\":%zu,\"max_state_bytes\":%zu",
             st->map.count, st->map.key_bytes, st->other_count,
             frequency_retained_state_bytes(st), st->max_state_bytes);
    return tf_buffer_write_str(out, buf);
}

static void frequency_state_free(frequency_state *st) {
    if (st) {
        tf_audit_options_free(&st->audit_opts);
        if (st->cols) {
            for (size_t i = 0; i < st->n_cols; i++) free(st->cols[i]);
        }
        free(st->cols);
        free(st->other_label);
        for (size_t i = 0; i < st->map.count; i++) free(st->map.keys[i]);
        free(st->map.keys); free(st->map.counts);
        free(st);
    }
}

static void frequency_destroy(tf_step *self) {
    if (self) frequency_state_free(self->state);
    free(self);
}

tf_step *tf_frequency_create(const cJSON *args) {
    frequency_state *st = calloc(1, sizeof(frequency_state));
    if (!st) return NULL;
    tf_audit_options_init(&st->audit_opts, 1);
    st->audit_limit = 1000;

    if (args) {
        size_t parsed_size = 0;
        int has_max_values = tf_json_get_size_arg(args, "max_values",
                                                  1, TF_MAX_COUNT_ARG,
                                                  &parsed_size, "frequency");
        if (has_max_values < 0) { frequency_state_free(st); return NULL; }
        if (has_max_values > 0) st->max_values = parsed_size;

        int has_max_state = tf_json_get_size_arg(args, "max_state_bytes",
                                                 1, TF_MAX_STATE_BYTES,
                                                 &parsed_size, "frequency");
        if (has_max_state < 0) { frequency_state_free(st); return NULL; }
        if (has_max_state > 0) st->max_state_bytes = parsed_size;

        cJSON *audit_j = cJSON_GetObjectItemCaseSensitive(args, "audit");
        st->audit = cJSON_IsTrue(audit_j) ? 1 : 0;
        cJSON *audit_limit_j = cJSON_GetObjectItemCaseSensitive(args, "audit_limit");
        if (!audit_limit_j) audit_limit_j = cJSON_GetObjectItemCaseSensitive(args, "auditLimit");
        if (audit_limit_j) {
            if (tf_json_get_size_arg_any(args, "audit_limit", "auditLimit",
                                         1, TF_MAX_AUDIT_RECORDS,
                                         &parsed_size, "frequency") < 0) {
                frequency_state_free(st);
                return NULL;
            }
            st->audit_limit = parsed_size;
        }

        if (tf_audit_options_parse(&st->audit_opts, args, "frequency") != TF_OK) {
            frequency_state_free(st);
            return NULL;
        }

        cJSON *overflow = cJSON_GetObjectItemCaseSensitive(args, "overflow");
        if (cJSON_IsString(overflow)) {
            if (strcmp(overflow->valuestring, "other") == 0) {
                st->overflow_other = 1;
            } else if (strcmp(overflow->valuestring, "error") != 0) {
                tf_set_last_error("frequency: overflow must be error or other");
                frequency_state_free(st);
                return NULL;
            }
        }
        cJSON *other = cJSON_GetObjectItemCaseSensitive(args, "other");
        const char *other_label = cJSON_IsString(other) ? other->valuestring : "__other__";
        if (st->overflow_other) {
            if (st->max_values == 0) {
                tf_set_last_error("frequency: overflow=other requires max_values");
                frequency_state_free(st);
                return NULL;
            }
            if (!other_label || other_label[0] == '\0') {
                tf_set_last_error("frequency: other label cannot be empty");
                frequency_state_free(st);
                return NULL;
            }
            st->other_label = strdup(other_label);
            if (!st->other_label) { frequency_state_free(st); return NULL; }
        }

        cJSON *columns = cJSON_GetObjectItemCaseSensitive(args, "columns");
        if (columns) {
            if (!cJSON_IsArray(columns)) {
                tf_set_last_error("frequency: columns must be an array");
                frequency_state_free(st);
                return NULL;
            }
            int n = cJSON_GetArraySize(columns);
            if (n > 0) {
                st->cols = tf_callocarray_checked((size_t)n, sizeof(char *));
                if (!st->cols) { frequency_state_free(st); return NULL; }
                st->n_cols = (size_t)n;
                for (int i = 0; i < n; i++) {
                    cJSON *item = cJSON_GetArrayItem(columns, i);
                    if (!cJSON_IsString(item) || !item->valuestring || !item->valuestring[0]) {
                        tf_set_last_error("frequency: column names must be non-empty strings");
                        frequency_state_free(st);
                        return NULL;
                    }
                    st->cols[i] = strdup(item->valuestring);
                    if (!st->cols[i]) { frequency_state_free(st); return NULL; }
                }
            }
        }
    }

    tf_step *step = calloc(1, sizeof(tf_step));
    if (!step) { frequency_state_free(st); return NULL; }
    step->process = frequency_process;
    step->flush = frequency_flush;
    step->append_stats = frequency_append_stats;
    step->destroy = frequency_destroy;
    step->state = st;
    return step;
}
