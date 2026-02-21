/*
 * op_cast.c — Type conversion.
 *
 * Config: {"mapping": {"age": "int", "score": "float", "active": "bool"}}
 */

#include "internal.h"
#include "date_utils.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

typedef struct {
    char    **col_names;
    tf_type  *target_types;
    size_t    n;
} cast_state;

static tf_type parse_type(const char *s) {
    if (strcmp(s, "int") == 0 || strcmp(s, "int64") == 0) return TF_TYPE_INT64;
    if (strcmp(s, "float") == 0 || strcmp(s, "float64") == 0) return TF_TYPE_FLOAT64;
    if (strcmp(s, "string") == 0 || strcmp(s, "str") == 0) return TF_TYPE_STRING;
    if (strcmp(s, "bool") == 0 || strcmp(s, "boolean") == 0) return TF_TYPE_BOOL;
    if (strcmp(s, "date") == 0) return TF_TYPE_DATE;
    if (strcmp(s, "timestamp") == 0 || strcmp(s, "datetime") == 0) return TF_TYPE_TIMESTAMP;
    return TF_TYPE_NULL;
}

static int cast_process(tf_step *self, tf_batch *in, tf_batch **out,
                        tf_side_channels *side) {
    (void)side;
    cast_state *st = self->state;
    *out = NULL;

    /* Determine output types */
    tf_type *out_types = malloc(in->n_cols * sizeof(tf_type));
    if (!out_types) return TF_ERROR;
    for (size_t c = 0; c < in->n_cols; c++) out_types[c] = in->col_types[c];
    for (size_t k = 0; k < st->n; k++) {
        int ci = tf_batch_col_index(in, st->col_names[k]);
        if (ci >= 0) out_types[ci] = st->target_types[k];
    }

    tf_batch *ob = tf_batch_create(in->n_cols, in->n_rows);
    if (!ob) { free(out_types); return TF_ERROR; }
    for (size_t c = 0; c < in->n_cols; c++)
        tf_batch_set_schema(ob, c, in->col_names[c], out_types[c]);

    for (size_t r = 0; r < in->n_rows; r++) {
        tf_batch_ensure_capacity(ob, r + 1);
        for (size_t c = 0; c < in->n_cols; c++) {
            if (tf_batch_is_null(in, r, c)) {
                tf_batch_set_null(ob, r, c);
                continue;
            }
            tf_type src_t = in->col_types[c];
            tf_type dst_t = out_types[c];

            if (src_t == dst_t) {
                /* No conversion needed */
                switch (src_t) {
                    case TF_TYPE_BOOL: tf_batch_set_bool(ob, r, c, tf_batch_get_bool(in, r, c)); break;
                    case TF_TYPE_INT64: tf_batch_set_int64(ob, r, c, tf_batch_get_int64(in, r, c)); break;
                    case TF_TYPE_FLOAT64: tf_batch_set_float64(ob, r, c, tf_batch_get_float64(in, r, c)); break;
                    case TF_TYPE_STRING: tf_batch_set_string(ob, r, c, tf_batch_get_string(in, r, c)); break;
                    case TF_TYPE_DATE: tf_batch_set_date(ob, r, c, tf_batch_get_date(in, r, c)); break;
                    case TF_TYPE_TIMESTAMP: tf_batch_set_timestamp(ob, r, c, tf_batch_get_timestamp(in, r, c)); break;
                    default: tf_batch_set_null(ob, r, c); break;
                }
                continue;
            }

            /* Conversion */
            if (dst_t == TF_TYPE_STRING) {
                char buf[64];
                switch (src_t) {
                    case TF_TYPE_INT64: snprintf(buf, sizeof(buf), "%lld", (long long)tf_batch_get_int64(in, r, c)); break;
                    case TF_TYPE_FLOAT64: snprintf(buf, sizeof(buf), "%g", tf_batch_get_float64(in, r, c)); break;
                    case TF_TYPE_BOOL: snprintf(buf, sizeof(buf), "%s", tf_batch_get_bool(in, r, c) ? "true" : "false"); break;
                    case TF_TYPE_DATE: tf_date_format(tf_batch_get_date(in, r, c), buf, sizeof(buf)); break;
                    case TF_TYPE_TIMESTAMP: tf_timestamp_format(tf_batch_get_timestamp(in, r, c), buf, sizeof(buf)); break;
                    default: buf[0] = '\0'; break;
                }
                tf_batch_set_string(ob, r, c, buf);
            } else if (dst_t == TF_TYPE_INT64) {
                int64_t v = 0;
                if (src_t == TF_TYPE_FLOAT64) v = (int64_t)tf_batch_get_float64(in, r, c);
                else if (src_t == TF_TYPE_STRING) { char *end; v = strtoll(tf_batch_get_string(in, r, c), &end, 10); }
                else if (src_t == TF_TYPE_BOOL) v = tf_batch_get_bool(in, r, c) ? 1 : 0;
                else if (src_t == TF_TYPE_TIMESTAMP) v = tf_batch_get_timestamp(in, r, c);
                tf_batch_set_int64(ob, r, c, v);
            } else if (dst_t == TF_TYPE_FLOAT64) {
                double v = 0;
                if (src_t == TF_TYPE_INT64) v = (double)tf_batch_get_int64(in, r, c);
                else if (src_t == TF_TYPE_STRING) { char *end; v = strtod(tf_batch_get_string(in, r, c), &end); }
                else if (src_t == TF_TYPE_BOOL) v = tf_batch_get_bool(in, r, c) ? 1.0 : 0.0;
                tf_batch_set_float64(ob, r, c, v);
            } else if (dst_t == TF_TYPE_BOOL) {
                bool v = false;
                if (src_t == TF_TYPE_INT64) v = tf_batch_get_int64(in, r, c) != 0;
                else if (src_t == TF_TYPE_FLOAT64) v = tf_batch_get_float64(in, r, c) != 0.0;
                else if (src_t == TF_TYPE_STRING) v = strlen(tf_batch_get_string(in, r, c)) > 0 && strcmp(tf_batch_get_string(in, r, c), "false") != 0;
                tf_batch_set_bool(ob, r, c, v);
            } else if (dst_t == TF_TYPE_DATE) {
                int32_t v = 0;
                if (src_t == TF_TYPE_STRING) {
                    const char *s = tf_batch_get_string(in, r, c);
                    int y, m, d;
                    if (sscanf(s, "%d-%d-%d", &y, &m, &d) == 3)
                        v = tf_date_from_ymd(y, m, d);
                } else if (src_t == TF_TYPE_TIMESTAMP) {
                    v = (int32_t)(tf_batch_get_timestamp(in, r, c) / (86400LL * 1000000LL));
                }
                tf_batch_set_date(ob, r, c, v);
            } else if (dst_t == TF_TYPE_TIMESTAMP) {
                int64_t v = 0;
                if (src_t == TF_TYPE_STRING) {
                    const char *s = tf_batch_get_string(in, r, c);
                    size_t slen = strlen(s);
                    int32_t dv;
                    /* Try timestamp first, then date at midnight */
                    if (slen >= 19) {
                        int y, mo, d, h, mi, se;
                        if (sscanf(s, "%d-%d-%dT%d:%d:%d", &y, &mo, &d, &h, &mi, &se) == 6)
                            v = tf_timestamp_from_parts(y, mo, d, h, mi, se, 0);
                        else if (sscanf(s, "%d-%d-%d %d:%d:%d", &y, &mo, &d, &h, &mi, &se) == 6)
                            v = tf_timestamp_from_parts(y, mo, d, h, mi, se, 0);
                    } else if (slen == 10) {
                        int y, m, d;
                        if (sscanf(s, "%d-%d-%d", &y, &m, &d) == 3) {
                            dv = tf_date_from_ymd(y, m, d);
                            v = (int64_t)dv * 86400LL * 1000000LL;
                        }
                    }
                } else if (src_t == TF_TYPE_DATE) {
                    v = (int64_t)tf_batch_get_date(in, r, c) * 86400LL * 1000000LL;
                } else if (src_t == TF_TYPE_INT64) {
                    v = tf_batch_get_int64(in, r, c);
                }
                tf_batch_set_timestamp(ob, r, c, v);
            } else {
                tf_batch_set_null(ob, r, c);
            }
        }
        ob->n_rows = r + 1;
    }

    free(out_types);
    *out = ob;
    return TF_OK;
}

static int cast_flush(tf_step *self, tf_batch **out, tf_side_channels *side) {
    (void)self; (void)side; *out = NULL; return TF_OK;
}

static void cast_destroy(tf_step *self) {
    cast_state *st = self->state;
    if (st) {
        for (size_t i = 0; i < st->n; i++) free(st->col_names[i]);
        free(st->col_names); free(st->target_types);
        free(st);
    }
    free(self);
}

tf_step *tf_cast_create(const cJSON *args) {
    if (!args) return NULL;
    cJSON *mapping = cJSON_GetObjectItemCaseSensitive(args, "mapping");
    if (!mapping || !cJSON_IsObject(mapping)) return NULL;

    int n = cJSON_GetArraySize(mapping);
    cast_state *st = calloc(1, sizeof(cast_state));
    if (!st) return NULL;
    st->col_names = calloc(n, sizeof(char *));
    st->target_types = calloc(n, sizeof(tf_type));
    st->n = n;

    int i = 0;
    cJSON *entry = NULL;
    cJSON_ArrayForEach(entry, mapping) {
        st->col_names[i] = strdup(entry->string);
        st->target_types[i] = cJSON_IsString(entry) ? parse_type(entry->valuestring) : TF_TYPE_NULL;
        i++;
    }

    tf_step *step = malloc(sizeof(tf_step));
    if (!step) { cast_destroy(&(tf_step){.state = st}); return NULL; }
    step->process = cast_process;
    step->flush = cast_flush;
    step->destroy = cast_destroy;
    step->state = st;
    return step;
}
