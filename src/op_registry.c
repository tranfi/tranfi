/*
 * op_registry.c — Declarative registry of all built-in ops.
 *
 * Replaces the hard-coded strcmp chain in plan.c.
 * Each entry describes an op's kind, capabilities, arguments,
 * schema inference callback, and native constructor.
 */

#include "ir.h"
#include "internal.h"
#include "cJSON.h"
#include <string.h>
#include <stdlib.h>

/* ---- Schema inference callbacks ---- */

/* Decoders: output schema unknown until runtime */
static int infer_schema_unknown(const tf_ir_node *node,
                                const tf_schema *in, tf_schema *out) {
    (void)node; (void)in;
    out->col_names = NULL;
    out->col_types = NULL;
    out->n_cols = 0;
    out->known = false;
    return TF_OK;
}

/* Encoders: consume schema, no output */
static int infer_schema_sink(const tf_ir_node *node,
                             const tf_schema *in, tf_schema *out) {
    (void)node; (void)in;
    out->col_names = NULL;
    out->col_types = NULL;
    out->n_cols = 0;
    out->known = false;
    return TF_OK;
}

/* filter, head, skip, unique: output schema = input schema */
static int infer_schema_passthrough(const tf_ir_node *node,
                                    const tf_schema *in, tf_schema *out) {
    (void)node;
    if (!in->known) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }
    tf_schema_copy(out, in);
    return TF_OK;
}

/* select: output schema = subset of input columns */
static int infer_schema_select(const tf_ir_node *node,
                               const tf_schema *in, tf_schema *out) {
    if (!in->known) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }

    cJSON *cols = cJSON_GetObjectItemCaseSensitive(node->args, "columns");
    if (!cols || !cJSON_IsArray(cols)) return TF_ERROR;

    int n = cJSON_GetArraySize(cols);
    out->col_names = calloc(n, sizeof(char *));
    out->col_types = calloc(n, sizeof(tf_type));
    out->n_cols = n;
    out->known = true;

    for (int i = 0; i < n; i++) {
        cJSON *item = cJSON_GetArrayItem(cols, i);
        if (!cJSON_IsString(item)) {
            tf_schema_free(out);
            return TF_ERROR;
        }
        const char *name = item->valuestring;
        /* Find column in input schema */
        bool found = false;
        for (size_t j = 0; j < in->n_cols; j++) {
            if (strcmp(in->col_names[j], name) == 0) {
                out->col_names[i] = strdup(name);
                out->col_types[i] = in->col_types[j];
                found = true;
                break;
            }
        }
        if (!found) {
            /* Column not in input — still record it, validation can catch it */
            out->col_names[i] = strdup(name);
            out->col_types[i] = TF_TYPE_NULL;
        }
    }
    return TF_OK;
}

/* rename: output schema = input schema with renamed columns */
static int infer_schema_rename(const tf_ir_node *node,
                               const tf_schema *in, tf_schema *out) {
    if (!in->known) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }

    tf_schema_copy(out, in);

    cJSON *mapping = cJSON_GetObjectItemCaseSensitive(node->args, "mapping");
    if (!mapping || !cJSON_IsObject(mapping)) return TF_OK; /* no renames */

    cJSON *entry = NULL;
    cJSON_ArrayForEach(entry, mapping) {
        if (!cJSON_IsString(entry)) continue;
        const char *old_name = entry->string;
        const char *new_name = entry->valuestring;
        for (size_t i = 0; i < out->n_cols; i++) {
            if (strcmp(out->col_names[i], old_name) == 0) {
                free(out->col_names[i]);
                out->col_names[i] = strdup(new_name);
                break;
            }
        }
    }
    return TF_OK;
}

/* derive: output schema = input + derived columns */
static int infer_schema_derive(const tf_ir_node *node,
                               const tf_schema *in, tf_schema *out) {
    if (!in->known) {
        out->known = false;
        out->col_names = NULL;
        out->col_types = NULL;
        out->n_cols = 0;
        return TF_OK;
    }

    cJSON *columns = cJSON_GetObjectItemCaseSensitive(node->args, "columns");
    int n_derived = columns ? cJSON_GetArraySize(columns) : 0;
    size_t total = in->n_cols + n_derived;

    out->col_names = calloc(total, sizeof(char *));
    out->col_types = calloc(total, sizeof(tf_type));
    out->n_cols = total;
    out->known = true;

    /* Copy input columns */
    for (size_t i = 0; i < in->n_cols; i++) {
        out->col_names[i] = strdup(in->col_names[i]);
        out->col_types[i] = in->col_types[i];
    }

    /* Add derived columns (type unknown at compile time) */
    for (int i = 0; i < n_derived; i++) {
        cJSON *item = cJSON_GetArrayItem(columns, i);
        cJSON *name_j = cJSON_GetObjectItemCaseSensitive(item, "name");
        out->col_names[in->n_cols + i] = strdup(name_j ? name_j->valuestring : "?");
        out->col_types[in->n_cols + i] = TF_TYPE_NULL;
    }
    return TF_OK;
}

/* validate: input + _valid bool column */
static int infer_schema_validate(const tf_ir_node *node,
                                 const tf_schema *in, tf_schema *out) {
    (void)node;
    if (!in->known) { out->known = false; out->col_names = NULL; out->col_types = NULL; out->n_cols = 0; return TF_OK; }
    out->n_cols = in->n_cols + 1;
    out->col_names = calloc(out->n_cols, sizeof(char *));
    out->col_types = calloc(out->n_cols, sizeof(tf_type));
    out->known = true;
    for (size_t i = 0; i < in->n_cols; i++) {
        out->col_names[i] = strdup(in->col_names[i]);
        out->col_types[i] = in->col_types[i];
    }
    out->col_names[in->n_cols] = strdup("_valid");
    out->col_types[in->n_cols] = TF_TYPE_BOOL;
    return TF_OK;
}

/* hash: input + _hash int column */
static int infer_schema_add_hash(const tf_ir_node *node,
                                 const tf_schema *in, tf_schema *out) {
    (void)node;
    if (!in->known) { out->known = false; out->col_names = NULL; out->col_types = NULL; out->n_cols = 0; return TF_OK; }
    out->n_cols = in->n_cols + 1;
    out->col_names = calloc(out->n_cols, sizeof(char *));
    out->col_types = calloc(out->n_cols, sizeof(tf_type));
    out->known = true;
    for (size_t i = 0; i < in->n_cols; i++) {
        out->col_names[i] = strdup(in->col_names[i]);
        out->col_types[i] = in->col_types[i];
    }
    out->col_names[in->n_cols] = strdup("_hash");
    out->col_types[in->n_cols] = TF_TYPE_INT64;
    return TF_OK;
}

/* frequency: output is value + count */
static int infer_schema_frequency(const tf_ir_node *node,
                                  const tf_schema *in, tf_schema *out) {
    (void)node; (void)in;
    out->n_cols = 2;
    out->col_names = calloc(2, sizeof(char *));
    out->col_types = calloc(2, sizeof(tf_type));
    out->known = true;
    out->col_names[0] = strdup("value");
    out->col_types[0] = TF_TYPE_STRING;
    out->col_names[1] = strdup("count");
    out->col_types[1] = TF_TYPE_INT64;
    return TF_OK;
}

/* group-agg: group columns (string) + agg columns (float64) */
static int infer_schema_group_agg(const tf_ir_node *node,
                                  const tf_schema *in, tf_schema *out) {
    (void)in;
    cJSON *group_by = cJSON_GetObjectItemCaseSensitive(node->args, "group_by");
    cJSON *aggs = cJSON_GetObjectItemCaseSensitive(node->args, "aggs");
    int ng = group_by ? cJSON_GetArraySize(group_by) : 0;
    int na = aggs ? cJSON_GetArraySize(aggs) : 0;
    out->n_cols = ng + na;
    out->col_names = calloc(out->n_cols, sizeof(char *));
    out->col_types = calloc(out->n_cols, sizeof(tf_type));
    out->known = true;
    for (int i = 0; i < ng; i++) {
        cJSON *item = cJSON_GetArrayItem(group_by, i);
        out->col_names[i] = strdup(cJSON_IsString(item) ? item->valuestring : "?");
        out->col_types[i] = TF_TYPE_STRING;
    }
    for (int i = 0; i < na; i++) {
        cJSON *item = cJSON_GetArrayItem(aggs, i);
        cJSON *name_j = cJSON_GetObjectItemCaseSensitive(item, "name");
        out->col_names[ng + i] = strdup(name_j && cJSON_IsString(name_j) ? name_j->valuestring : "?");
        out->col_types[ng + i] = TF_TYPE_FLOAT64;
    }
    return TF_OK;
}

/* stats: output schema is known: column, count, sum, avg, min, max */
static int infer_schema_stats(const tf_ir_node *node,
                              const tf_schema *in, tf_schema *out) {
    (void)in;
    /* Determine which stats are requested */
    int w_count=0, w_sum=0, w_avg=0, w_min=0, w_max=0;
    int w_var=0, w_stddev=0, w_median=0, w_p25=0, w_p75=0;
    int w_skewness=0, w_kurtosis=0, w_distinct=0, w_hist=0, w_sample=0;
    cJSON *stats_arr = cJSON_GetObjectItemCaseSensitive(node->args, "stats");
    if (stats_arr && cJSON_IsArray(stats_arr)) {
        int n = cJSON_GetArraySize(stats_arr);
        for (int i = 0; i < n; i++) {
            cJSON *item = cJSON_GetArrayItem(stats_arr, i);
            if (!cJSON_IsString(item)) continue;
            const char *s = item->valuestring;
            if (strcmp(s, "count") == 0) w_count = 1;
            else if (strcmp(s, "sum") == 0) w_sum = 1;
            else if (strcmp(s, "avg") == 0) w_avg = 1;
            else if (strcmp(s, "min") == 0) w_min = 1;
            else if (strcmp(s, "max") == 0) w_max = 1;
            else if (strcmp(s, "var") == 0) w_var = 1;
            else if (strcmp(s, "stddev") == 0) w_stddev = 1;
            else if (strcmp(s, "median") == 0) w_median = 1;
            else if (strcmp(s, "p25") == 0) w_p25 = 1;
            else if (strcmp(s, "p75") == 0) w_p75 = 1;
            else if (strcmp(s, "skewness") == 0) w_skewness = 1;
            else if (strcmp(s, "kurtosis") == 0) w_kurtosis = 1;
            else if (strcmp(s, "distinct") == 0) w_distinct = 1;
            else if (strcmp(s, "hist") == 0) w_hist = 1;
            else if (strcmp(s, "sample") == 0) w_sample = 1;
        }
    } else {
        /* Default: basic + variance + median */
        w_count = w_sum = w_avg = w_min = w_max = 1;
        w_var = w_stddev = w_median = 1;
    }

    size_t n_cols = 1; /* column */
    if (w_count) n_cols++;
    if (w_sum) n_cols++;
    if (w_avg) n_cols++;
    if (w_min) n_cols++;
    if (w_max) n_cols++;
    if (w_var) n_cols++;
    if (w_stddev) n_cols++;
    if (w_median) n_cols++;
    if (w_p25) n_cols++;
    if (w_p75) n_cols++;
    if (w_skewness) n_cols++;
    if (w_kurtosis) n_cols++;
    if (w_distinct) n_cols++;
    if (w_hist) n_cols++;
    if (w_sample) n_cols++;

    out->col_names = calloc(n_cols, sizeof(char *));
    out->col_types = calloc(n_cols, sizeof(tf_type));
    out->n_cols = n_cols;
    out->known = true;

    size_t ci = 0;
    out->col_names[ci] = strdup("column"); out->col_types[ci++] = TF_TYPE_STRING;
    if (w_count) { out->col_names[ci] = strdup("count"); out->col_types[ci++] = TF_TYPE_INT64; }
    if (w_sum) { out->col_names[ci] = strdup("sum"); out->col_types[ci++] = TF_TYPE_FLOAT64; }
    if (w_avg) { out->col_names[ci] = strdup("avg"); out->col_types[ci++] = TF_TYPE_FLOAT64; }
    if (w_min) { out->col_names[ci] = strdup("min"); out->col_types[ci++] = TF_TYPE_FLOAT64; }
    if (w_max) { out->col_names[ci] = strdup("max"); out->col_types[ci++] = TF_TYPE_FLOAT64; }
    if (w_var) { out->col_names[ci] = strdup("var"); out->col_types[ci++] = TF_TYPE_FLOAT64; }
    if (w_stddev) { out->col_names[ci] = strdup("stddev"); out->col_types[ci++] = TF_TYPE_FLOAT64; }
    if (w_median) { out->col_names[ci] = strdup("median"); out->col_types[ci++] = TF_TYPE_FLOAT64; }
    if (w_p25) { out->col_names[ci] = strdup("p25"); out->col_types[ci++] = TF_TYPE_FLOAT64; }
    if (w_p75) { out->col_names[ci] = strdup("p75"); out->col_types[ci++] = TF_TYPE_FLOAT64; }
    if (w_skewness) { out->col_names[ci] = strdup("skewness"); out->col_types[ci++] = TF_TYPE_FLOAT64; }
    if (w_kurtosis) { out->col_names[ci] = strdup("kurtosis"); out->col_types[ci++] = TF_TYPE_FLOAT64; }
    if (w_distinct) { out->col_names[ci] = strdup("distinct"); out->col_types[ci++] = TF_TYPE_INT64; }
    if (w_hist) { out->col_names[ci] = strdup("hist"); out->col_types[ci++] = TF_TYPE_STRING; }
    if (w_sample) { out->col_names[ci] = strdup("sample"); out->col_types[ci++] = TF_TYPE_STRING; }
    return TF_OK;
}

/* ---- Built-in ops table ---- */

static tf_arg_desc csv_decode_args[] = {
    {"delimiter", "string", false, "\",\""},
    {"header",    "bool",   false, "true"},
    {"batch_size","int",    false, "1024"},
    {"repair",    "bool",   false, "false"},
};

static tf_arg_desc csv_encode_args[] = {
    {"delimiter", "string", false, "\",\""},
    {"header",    "bool",   false, "true"},
};

static tf_arg_desc jsonl_decode_args[] = {
    {"batch_size", "int", false, "1024"},
};

static tf_arg_desc jsonl_encode_args[] = {
    {0}  /* no args */
};

static tf_arg_desc text_decode_args[] = {
    {"batch_size", "int", false, "1024"},
};

static tf_arg_desc text_encode_args[] = {
    {0}  /* no args */
};

static tf_arg_desc grep_args[] = {
    {"pattern", "string", true, NULL},
    {"invert", "bool", false, "false"},
    {"column", "string", false, "\"_line\""},
    {"regex", "bool", false, "false"},
};

static tf_arg_desc filter_args[] = {
    {"expr", "string", true, NULL},
};

static tf_arg_desc select_args[] = {
    {"columns", "string[]", true, NULL},
};

static tf_arg_desc rename_args[] = {
    {"mapping", "map", true, NULL},
};

static tf_arg_desc head_args[] = {
    {"n", "int", true, NULL},
};

static tf_arg_desc skip_args[] = {
    {"n", "int", true, NULL},
};

static tf_arg_desc derive_args[] = {
    {"columns", "map[]", true, NULL},
};

static tf_arg_desc stats_args[] = {
    {"stats", "string[]", false, NULL},
};

static tf_arg_desc unique_args[] = {
    {"columns", "string[]", false, NULL},
};

static tf_arg_desc sort_args[] = {
    {"columns", "map[]", true, NULL},
};

static tf_arg_desc validate_args[] = {
    {"expr", "string", true, NULL},
};

static tf_arg_desc trim_args[] = {
    {"columns", "string[]", false, NULL},
};

static tf_arg_desc fill_null_args[] = {
    {"mapping", "map", true, NULL},
};

static tf_arg_desc cast_args[] = {
    {"mapping", "map", true, NULL},
};

static tf_arg_desc clip_args[] = {
    {"column", "string", true, NULL},
    {"min", "float", false, NULL},
    {"max", "float", false, NULL},
};

static tf_arg_desc replace_args[] = {
    {"column", "string", true, NULL},
    {"pattern", "string", true, NULL},
    {"replacement", "string", true, NULL},
    {"regex", "bool", false, "false"},
};

static tf_arg_desc hash_args[] = {
    {"columns", "string[]", false, NULL},
};

static tf_arg_desc bin_args[] = {
    {"column", "string", true, NULL},
    {"boundaries", "float[]", true, NULL},
};

static tf_arg_desc fill_down_args[] = {
    {"columns", "string[]", false, NULL},
};

static tf_arg_desc step_args[] = {
    {"column", "string", true, NULL},
    {"func", "string", true, NULL},
    {"result", "string", false, NULL},
};

static tf_arg_desc window_args[] = {
    {"column", "string", true, NULL},
    {"size", "int", true, NULL},
    {"func", "string", true, NULL},
    {"result", "string", false, NULL},
};

static tf_arg_desc explode_args[] = {
    {"column", "string", true, NULL},
    {"delimiter", "string", false, "\",\""},
};

static tf_arg_desc split_args[] = {
    {"column", "string", true, NULL},
    {"delimiter", "string", false, "\" \""},
    {"names", "string[]", true, NULL},
};

static tf_arg_desc unpivot_args[] = {
    {"columns", "string[]", true, NULL},
};

static tf_arg_desc tail_args[] = {
    {"n", "int", true, NULL},
};

static tf_arg_desc top_args[] = {
    {"n", "int", true, NULL},
    {"column", "string", true, NULL},
    {"desc", "bool", false, "true"},
};

static tf_arg_desc sample_args[] = {
    {"n", "int", true, NULL},
};

static tf_arg_desc group_agg_args[] = {
    {"group_by", "string[]", true, NULL},
    {"aggs", "map[]", true, NULL},
};

static tf_arg_desc frequency_args[] = {
    {"columns", "string[]", false, NULL},
};

static tf_arg_desc datetime_args[] = {
    {"column", "string", true, NULL},
    {"extract", "string[]", false, NULL},
};

static tf_arg_desc pivot_args[] = {
    {"name_column", "string", true, NULL},
    {"value_column", "string", true, NULL},
    {"agg", "string", false, "\"first\""},
};

static tf_arg_desc join_args[] = {
    {"file", "string", true, NULL},
    {"on", "string", true, NULL},
    {"how", "string", false, "\"inner\""},
};

static tf_arg_desc stack_args[] = {
    {"file", "string", true, NULL},
    {"tag", "string", false, NULL},
    {"tag_value", "string", false, NULL},
};

static tf_arg_desc lead_args[] = {
    {"column", "string", true, NULL},
    {"offset", "int", false, "1"},
    {"result", "string", false, NULL},
};

static tf_arg_desc date_trunc_args[] = {
    {"column", "string", true, NULL},
    {"trunc", "string", true, NULL},
    {"result", "string", false, NULL},
};

static tf_arg_desc onehot_args[] = {
    {"column", "string", true, NULL},
    {"drop", "bool", false, "false"},
};

static tf_arg_desc label_encode_args[] = {
    {"column", "string", true, NULL},
    {"result", "string", false, NULL},
};

static tf_arg_desc ewma_args[] = {
    {"column", "string", true, NULL},
    {"alpha", "float", true, NULL},
    {"result", "string", false, NULL},
};

static tf_arg_desc diff_args[] = {
    {"column", "string", true, NULL},
    {"order", "int", false, "1"},
    {"result", "string", false, NULL},
};

static tf_arg_desc anomaly_args[] = {
    {"column", "string", true, NULL},
    {"threshold", "float", false, "3.0"},
    {"result", "string", false, NULL},
};

static tf_arg_desc split_data_args[] = {
    {"ratio", "float", false, "0.8"},
    {"result", "string", false, "\"_split\""},
    {"seed", "int", false, "42"},
};

static tf_arg_desc interpolate_args[] = {
    {"column", "string", true, NULL},
    {"method", "string", false, "\"linear\""},
};

static tf_arg_desc normalize_args[] = {
    {"columns", "string[]", true, NULL},
    {"method", "string", false, "\"minmax\""},
};

static tf_arg_desc acf_args[] = {
    {"column", "string", true, NULL},
    {"lags", "int", false, "20"},
};

static tf_arg_desc table_encode_args[] = {
    {"max_width", "int", false, "40"},
    {"max_rows", "int", false, "0"},
};

static tf_op_entry builtin_ops[] = {
    {
        .name = "codec.csv.decode",
        .kind = TF_OP_DECODER,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = csv_decode_args,
        .n_args = 4,
        .infer_schema = infer_schema_unknown,
        .create_native = (void *(*)(const cJSON *))tf_csv_decoder_create,
    },
    {
        .name = "codec.csv.encode",
        .kind = TF_OP_ENCODER,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = csv_encode_args,
        .n_args = 2,
        .infer_schema = infer_schema_sink,
        .create_native = (void *(*)(const cJSON *))tf_csv_encoder_create,
    },
    {
        .name = "codec.jsonl.decode",
        .kind = TF_OP_DECODER,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = jsonl_decode_args,
        .n_args = 1,
        .infer_schema = infer_schema_unknown,
        .create_native = (void *(*)(const cJSON *))tf_jsonl_decoder_create,
    },
    {
        .name = "codec.jsonl.encode",
        .kind = TF_OP_ENCODER,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = jsonl_encode_args,
        .n_args = 0,
        .infer_schema = infer_schema_sink,
        .create_native = (void *(*)(const cJSON *))tf_jsonl_encoder_create,
    },
    {
        .name = "codec.text.decode",
        .kind = TF_OP_DECODER,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = text_decode_args,
        .n_args = 1,
        .infer_schema = infer_schema_unknown,
        .create_native = (void *(*)(const cJSON *))tf_text_decoder_create,
    },
    {
        .name = "codec.text.encode",
        .kind = TF_OP_ENCODER,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = text_encode_args,
        .n_args = 0,
        .infer_schema = infer_schema_sink,
        .create_native = (void *(*)(const cJSON *))tf_text_encoder_create,
    },
    {
        .name = "grep",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = grep_args,
        .n_args = 4,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_grep_create,
    },
    {
        .name = "filter",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = filter_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_filter_create,
    },
    {
        .name = "select",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = select_args,
        .n_args = 1,
        .infer_schema = infer_schema_select,
        .create_native = (void *(*)(const cJSON *))tf_select_create,
    },
    {
        .name = "rename",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = rename_args,
        .n_args = 1,
        .infer_schema = infer_schema_rename,
        .create_native = (void *(*)(const cJSON *))tf_rename_create,
    },
    {
        .name = "head",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = head_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_head_create,
    },
    {
        .name = "skip",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = skip_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_skip_create,
    },
    {
        .name = "derive",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = derive_args,
        .n_args = 1,
        .infer_schema = infer_schema_derive,
        .create_native = (void *(*)(const cJSON *))tf_derive_create,
    },
    {
        .name = "stats",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = stats_args,
        .n_args = 1,
        .infer_schema = infer_schema_stats,
        .create_native = (void *(*)(const cJSON *))tf_stats_create,
    },
    {
        .name = "unique",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = unique_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_unique_create,
    },
    {
        .name = "sort",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = sort_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_sort_create,
    },
    /* ---- Aliases ---- */
    {
        .name = "reorder",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = select_args,
        .n_args = 1,
        .infer_schema = infer_schema_select,
        .create_native = (void *(*)(const cJSON *))tf_select_create,
    },
    {
        .name = "dedup",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = unique_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_unique_create,
    },
    /* ---- Simple streaming ---- */
    {
        .name = "validate",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = validate_args,
        .n_args = 1,
        .infer_schema = infer_schema_validate,
        .create_native = (void *(*)(const cJSON *))tf_validate_create,
    },
    {
        .name = "trim",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = trim_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_trim_create,
    },
    {
        .name = "fill-null",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = fill_null_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_fill_null_create,
    },
    {
        .name = "cast",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = cast_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,  /* type changes at runtime */
        .create_native = (void *(*)(const cJSON *))tf_cast_create,
    },
    {
        .name = "clip",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = clip_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_clip_create,
    },
    {
        .name = "replace",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = replace_args,
        .n_args = 4,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_replace_create,
    },
    {
        .name = "hash",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = hash_args,
        .n_args = 1,
        .infer_schema = infer_schema_add_hash,
        .create_native = (void *(*)(const cJSON *))tf_hash_create,
    },
    {
        .name = "bin",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = bin_args,
        .n_args = 2,
        .infer_schema = infer_schema_passthrough,  /* adds column at runtime */
        .create_native = (void *(*)(const cJSON *))tf_bin_create,
    },
    /* ---- Stateful streaming ---- */
    {
        .name = "fill-down",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = fill_down_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_fill_down_create,
    },
    {
        .name = "step",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = step_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,  /* adds column at runtime */
        .create_native = (void *(*)(const cJSON *))tf_step_create,
    },
    {
        .name = "window",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = window_args,
        .n_args = 4,
        .infer_schema = infer_schema_passthrough,  /* adds column at runtime */
        .create_native = (void *(*)(const cJSON *))tf_window_create,
    },
    /* ---- Row-multiplying streaming ---- */
    {
        .name = "explode",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = explode_args,
        .n_args = 2,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_explode_create,
    },
    {
        .name = "split",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = split_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,  /* adds columns at runtime */
        .create_native = (void *(*)(const cJSON *))tf_split_create,
    },
    {
        .name = "unpivot",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = unpivot_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,  /* schema changes at runtime */
        .create_native = (void *(*)(const cJSON *))tf_unpivot_create,
    },
    /* ---- Buffering ---- */
    {
        .name = "tail",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = tail_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_tail_create,
    },
    {
        .name = "top",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = top_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_top_create,
    },
    {
        .name = "sample",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE,
        .args = sample_args,
        .n_args = 1,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_sample_create,
    },
    {
        .name = "group-agg",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = group_agg_args,
        .n_args = 2,
        .infer_schema = infer_schema_group_agg,
        .create_native = (void *(*)(const cJSON *))tf_group_agg_create,
    },
    {
        .name = "frequency",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = frequency_args,
        .n_args = 1,
        .infer_schema = infer_schema_frequency,
        .create_native = (void *(*)(const cJSON *))tf_frequency_create,
    },
    /* ---- Complex ---- */
    {
        .name = "datetime",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = datetime_args,
        .n_args = 2,
        .infer_schema = infer_schema_passthrough,  /* adds columns at runtime */
        .create_native = (void *(*)(const cJSON *))tf_datetime_create,
    },
    {
        .name = "flatten",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = NULL,
        .n_args = 0,
        .infer_schema = infer_schema_passthrough,
        .create_native = NULL,  /* passthrough — no-op at runtime */
    },
    /* ---- Pivot / Join ---- */
    {
        .name = "pivot",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = pivot_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,  /* schema changes at runtime */
        .create_native = (void *(*)(const cJSON *))tf_pivot_create,
    },
    {
        .name = "join",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_FS | TF_CAP_DETERMINISTIC,
        .args = join_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,  /* schema depends on lookup file */
        .create_native = (void *(*)(const cJSON *))tf_join_create,
    },
    {
        .name = "stack",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_FS | TF_CAP_DETERMINISTIC,
        .args = stack_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_stack_create,
    },
    {
        .name = "lead",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = lead_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_lead_create,
    },
    {
        .name = "date-trunc",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = date_trunc_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_date_trunc_create,
    },
    {
        .name = "onehot",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = onehot_args,
        .n_args = 2,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_onehot_create,
    },
    {
        .name = "label-encode",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = label_encode_args,
        .n_args = 2,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_label_encode_create,
    },
    {
        .name = "ewma",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = ewma_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_ewma_create,
    },
    {
        .name = "diff",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = diff_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_diff_create,
    },
    {
        .name = "anomaly",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = anomaly_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_anomaly_create,
    },
    {
        .name = "split-data",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_STREAMING | TF_CAP_BOUNDED_MEMORY | TF_CAP_BROWSER_SAFE,
        .args = split_data_args,
        .n_args = 3,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_split_data_create,
    },
    {
        .name = "interpolate",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = interpolate_args,
        .n_args = 2,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_interpolate_create,
    },
    {
        .name = "normalize",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = normalize_args,
        .n_args = 2,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_normalize_create,
    },
    {
        .name = "acf",
        .kind = TF_OP_TRANSFORM,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = acf_args,
        .n_args = 2,
        .infer_schema = infer_schema_passthrough,
        .create_native = (void *(*)(const cJSON *))tf_acf_create,
    },
    {
        .name = "codec.table.encode",
        .kind = TF_OP_ENCODER,
        .tier = TF_TIER_CORE,
        .caps = TF_CAP_BROWSER_SAFE | TF_CAP_DETERMINISTIC,
        .args = table_encode_args,
        .n_args = 2,
        .infer_schema = infer_schema_sink,
        .create_native = (void *(*)(const cJSON *))tf_table_encoder_create,
    },
};

static const size_t n_builtin_ops = sizeof(builtin_ops) / sizeof(builtin_ops[0]);

/* ---- Public API ---- */

const tf_op_entry *tf_op_registry_find(const char *name) {
    for (size_t i = 0; i < n_builtin_ops; i++) {
        if (strcmp(builtin_ops[i].name, name) == 0)
            return &builtin_ops[i];
    }
    return NULL;
}

size_t tf_op_registry_count(void) {
    return n_builtin_ops;
}

const tf_op_entry *tf_op_registry_get(size_t index) {
    if (index >= n_builtin_ops) return NULL;
    return &builtin_ops[index];
}
