/*
 * main.c — Tranfi CLI.
 *
 * Usage:
 *   tranfi 'csv | filter "col(age) > 25" | select name,age | csv'  < in.csv
 *   tranfi -f pipeline.tf < in.csv > out.csv
 *   tranfi -j 'csv | head 5 | csv'   # compile only, output JSON
 *   tranfi --target sql --dialect duckdb 'csv | head 5 | csv'
 *   tranfi -i input.csv -o output.csv 'csv | filter "col(age) > 25" | csv'
 *
 * Channels:
 *   stdout  — main output (encoded data)
 *   stderr  — stats and errors
 */

#include "tranfi.h"
#include "internal.h"
#include "cJSON.h"
#include "ir.h"
#include "dsl.h"
#include "recipes.h"
#include "report.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <errno.h>
#include <stdint.h>
#include <strings.h>

#define READ_BUF_SIZE (64 * 1024)
#define PULL_BUF_SIZE (64 * 1024)

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s [OPTIONS] PIPELINE\n"
        "       %s [OPTIONS] -f FILE\n"
        "\n"
        "Streaming ETL with a pipe-style DSL.\n"
        "\n"
        "Examples:\n"
        "  %s 'csv | csv'                                    # passthrough\n"
        "  %s 'csv | filter \"col(age) > 25\" | csv'          # filter rows\n"
        "  %s 'csv | select name,age | csv'                  # select columns\n"
        "  %s 'csv | rename name=full_name | csv'            # rename columns\n"
        "  %s 'csv | head 10 | csv'                          # first N rows\n"
        "  %s 'csv | skip 5 | csv'                           # skip first 5 rows\n"
        "  %s 'csv | derive total=col(price)*col(qty) | csv' # computed columns\n"
        "  %s --allow-blocking 'csv | sort age | csv'        # sort known-small input\n"
        "  %s 'csv | unique name | csv'                      # deduplicate\n"
        "  %s 'csv | stats | csv'                            # aggregate stats\n"
        "  %s 'jsonl | filter \"col(x) > 0\" | jsonl'          # JSONL variant\n"
        "  %s --target sql --dialect duckdb 'csv | head 5 | csv' # print SQL\n"
        "\n"
        "Options:\n"
        "  -f FILE   Read pipeline from file instead of argument\n"
        "  -i FILE   Read input from file instead of stdin\n"
        "  -o FILE   Write output to file instead of stdout\n"
        "  -j        Output plan as JSON (compile only, don't execute)\n"
        "  --target NAME      Compile target: native, json, or sql\n"
        "  --dialect NAME     SQL dialect for --target sql (duckdb; sqlite/postgres planned)\n"
        "  --explain Output target/memory/emit/schema/state execution plan and exit\n"
        "  --memory max:SIZE  Set memory cap policy, e.g. max:64MB\n"
        "  --spill-dir DIR    Request disk spill directory for spillable plans\n"
        "  --engine NAME      Execution engine: native or duckdb\n"
        "  --stats-json FILE  Write stats side-channel NDJSON to FILE; use - for stderr\n"
        "  --allow-blocking   Explicitly permit full-input native blocking steps\n"
        "  --fail-on-blocking  Refuse native execution plans with blocking steps (default)\n"
        "  -p, --progress  Show progress on stderr\n"
        "  -q        Quiet mode (suppress stats on stderr)\n"
        "  --raw     Force raw CSV stats output (disable report formatting)\n"
        "  -v        Show version\n"
        "  -R, --recipes  List built-in recipes\n"
        "  -h        Show this help\n"
        "\n"
        "Recipes (use by name, e.g. %s profile):\n"
        "  profile, preview, schema, summary, count, cardinality,\n"
        "  distro, freq, dedup, clean, sample, head, tail, csv2json,\n"
        "  json2csv, tsv2csv, csv2tsv, histogram, hash, samples\n",
        prog, prog, prog, prog, prog, prog, prog, prog,
        prog, prog, prog, prog, prog, prog, prog);
}

static int cli_file_output_sink(int channel, const uint8_t *data, size_t len, void *user) {
    (void)channel;
    FILE *out = (FILE *)user;
    if (!out || len == 0) return out ? TF_OK : TF_ERROR;
    return fwrite(data, 1, len, out) == len ? TF_OK : TF_ERROR;
}

static char *read_file(const char *path) {
    FILE *f = fopen(path, "r");
    if (!f) return NULL;

    if (fseek(f, 0, SEEK_END) != 0) { fclose(f); return NULL; }
    long size = ftell(f);
    if (fseek(f, 0, SEEK_SET) != 0) { fclose(f); return NULL; }

    if (size <= 0) { fclose(f); return NULL; }

    size_t alloc_len;
    if (tf_size_add((size_t)size, 1, &alloc_len) != TF_OK) { fclose(f); return NULL; }
    char *buf = tf_mallocarray_checked(alloc_len, sizeof(char));
    if (!buf) { fclose(f); return NULL; }

    size_t nread = fread(buf, 1, (size_t)size, f);
    if (ferror(f)) {
        free(buf);
        fclose(f);
        return NULL;
    }
    fclose(f);
    buf[nread] = '\0';
    return buf;
}

typedef enum {
    CLI_ENGINE_NATIVE,
    CLI_ENGINE_DUCKDB,
} cli_engine;

typedef struct {
    cli_engine engine;
    int allow_blocking;
    int fail_on_blocking_seen;
    int has_memory_limit;
    size_t memory_limit_bytes;
    const char *spill_dir;
} cli_memory_policy;

static const char *engine_name(cli_engine engine) {
    switch (engine) {
        case CLI_ENGINE_NATIVE: return "native";
        case CLI_ENGINE_DUCKDB: return "duckdb";
        default: return "unknown";
    }
}

static const char *format_bytes(size_t bytes, char *buf, size_t buf_size) {
    if (bytes < 1024) {
        snprintf(buf, buf_size, "%zuB", bytes);
    } else if (bytes < 1024 * 1024) {
        snprintf(buf, buf_size, "%.1fKB", (double)bytes / 1024);
    } else if (bytes < 1024 * 1024 * 1024) {
        snprintf(buf, buf_size, "%.1fMB", (double)bytes / (1024 * 1024));
    } else {
        snprintf(buf, buf_size, "%.1fGB", (double)bytes / (1024 * 1024 * 1024));
    }
    return buf;
}

static int parse_size_bytes(const char *text, size_t *out, char *err, size_t err_size) {
    const char *p = text;
    if (!p || !*p) {
        snprintf(err, err_size, "empty size");
        return -1;
    }
    if (strncmp(p, "max:", 4) == 0 || strncmp(p, "max=", 4) == 0) p += 4;
    while (isspace((unsigned char)*p)) p++;
    if (!isdigit((unsigned char)*p)) {
        snprintf(err, err_size, "expected size such as 64MB or max:64MB");
        return -1;
    }

    errno = 0;
    char *end = NULL;
    unsigned long long value = strtoull(p, &end, 10);
    if (errno != 0 || end == p || value == 0) {
        snprintf(err, err_size, "invalid positive size '%s'", text);
        return -1;
    }
    while (isspace((unsigned char)*end)) end++;

    char suffix[8] = {0};
    size_t si = 0;
    while (*end && si + 1 < sizeof(suffix)) {
        suffix[si++] = (char)tolower((unsigned char)*end++);
    }
    while (isspace((unsigned char)*end)) end++;
    if (*end) {
        snprintf(err, err_size, "invalid size suffix in '%s'", text);
        return -1;
    }

    unsigned long long mul = 1;
    if (suffix[0] == '\0' || strcmp(suffix, "b") == 0) mul = 1;
    else if (strcmp(suffix, "k") == 0 || strcmp(suffix, "kb") == 0 || strcmp(suffix, "kib") == 0) mul = 1024ULL;
    else if (strcmp(suffix, "m") == 0 || strcmp(suffix, "mb") == 0 || strcmp(suffix, "mib") == 0) mul = 1024ULL * 1024ULL;
    else if (strcmp(suffix, "g") == 0 || strcmp(suffix, "gb") == 0 || strcmp(suffix, "gib") == 0) mul = 1024ULL * 1024ULL * 1024ULL;
    else if (strcmp(suffix, "t") == 0 || strcmp(suffix, "tb") == 0 || strcmp(suffix, "tib") == 0) mul = 1024ULL * 1024ULL * 1024ULL * 1024ULL;
    else {
        snprintf(err, err_size, "unsupported size suffix '%s'", suffix);
        return -1;
    }

    if (value > (unsigned long long)SIZE_MAX / mul) {
        snprintf(err, err_size, "size '%s' is too large", text);
        return -1;
    }
    *out = (size_t)(value * mul);
    return 0;
}

static int set_engine(cli_memory_policy *policy, const char *name, char *err, size_t err_size) {
    if (strcmp(name, "native") == 0) {
        policy->engine = CLI_ENGINE_NATIVE;
        return 0;
    }
    if (strcmp(name, "duckdb") == 0 || strcmp(name, "sql") == 0) {
        policy->engine = CLI_ENGINE_DUCKDB;
        return 0;
    }
    snprintf(err, err_size, "unknown engine '%s' (expected native or duckdb)", name);
    return -1;
}


static int is_known_sql_dialect(const char *name) {
    return name && (strcmp(name, "duckdb") == 0 ||
                    strcmp(name, "sqlite") == 0 ||
                    strcmp(name, "postgres") == 0);
}

static int sql_dialect_supported(const char *name) {
    return name && strcmp(name, "duckdb") == 0;
}

static int memory_rank(tf_memory_class cls) {
    switch (cls) {
        case TF_MEM_ROW_LOCAL: return 0;
        case TF_MEM_BOUNDED_STATE: return 1;
        case TF_MEM_KEY_STATE: return 2;
        case TF_MEM_EXTERNAL: return 3;
        case TF_MEM_BLOCKING: return 4;
        default: return 5;
    }
}

static void print_cap_item(FILE *f, int *first, const char *name) {
    fprintf(f, "%s%s", *first ? "" : ",", name);
    *first = 0;
}


typedef struct {
    char *data;
    size_t len;
    size_t cap;
} cli_string_builder;

static int sb_reserve(cli_string_builder *sb, size_t extra) {
    if (!sb) return -1;
    size_t need;
    if (tf_size_add(sb->len, extra, &need) != TF_OK ||
        tf_size_add(need, 1, &need) != TF_OK) {
        return -1;
    }
    if (need <= sb->cap) return 0;
    size_t new_cap;
    if (tf_size_grow_pow2(sb->cap, need, 256, &new_cap) != TF_OK) return -1;
    char *tmp = tf_reallocarray_checked(sb->data, new_cap, sizeof(char));
    if (!tmp) return -1;
    sb->data = tmp;
    sb->cap = new_cap;
    return 0;
}

static int sb_append_n(cli_string_builder *sb, const char *s, size_t n) {
    if (!s) return 0;
    if (sb_reserve(sb, n) != 0) return -1;
    memcpy(sb->data + sb->len, s, n);
    sb->len += n;
    sb->data[sb->len] = '\0';
    return 0;
}

static int sb_append(cli_string_builder *sb, const char *s) {
    return sb_append_n(sb, s, s ? strlen(s) : 0);
}

static int sb_append_char(cli_string_builder *sb, char ch) {
    if (sb_reserve(sb, 1) != 0) return -1;
    sb->data[sb->len++] = ch;
    sb->data[sb->len] = '\0';
    return 0;
}

static int cli_report_buffer_append(char **buf, size_t *len, size_t *cap,
                                    const uint8_t *data, size_t n) {
    if (!buf || !len || !cap || !*buf || (!data && n > 0)) return -1;
    size_t need;
    if (tf_size_add(*len, n, &need) != TF_OK) return -1;
    if (need > *cap) {
        size_t new_cap;
        if (tf_size_grow_pow2(*cap, need, PULL_BUF_SIZE, &new_cap) != TF_OK) return -1;
        char *tmp = tf_reallocarray_checked(*buf, new_cap, sizeof(char));
        if (!tmp) return -1;
        *buf = tmp;
        *cap = new_cap;
    }
    if (n > 0) memcpy(*buf + *len, data, n);
    *len = need;
    return 0;
}

static void cli_disable_report_buffer(FILE *fout, char **buf, size_t *len, size_t *cap) {
    if (!buf || !len || !cap) return;
    if (fout && *buf && *len > 0) fwrite(*buf, 1, *len, fout);
    free(*buf);
    *buf = NULL;
    *len = 0;
    *cap = 0;
}

static int cli_is_bare_dsl_token(const char *s) {
    if (!s || !*s) return 0;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        if (isspace(*p) || *p == '|' || *p == '=' || *p == '"' || *p == '\'' || *p == '\\') return 0;
    }
    return 1;
}

static const char *cli_normalized_step_name(const char *op) {
    if (!op) return "unknown";
    if (strcmp(op, "codec.csv.decode") == 0 || strcmp(op, "codec.csv.encode") == 0) return "csv";
    if (strcmp(op, "codec.jsonl.decode") == 0 || strcmp(op, "codec.jsonl.encode") == 0) return "jsonl";
    if (strcmp(op, "codec.text.decode") == 0 || strcmp(op, "codec.text.encode") == 0) return "text";
    if (strcmp(op, "codec.table.encode") == 0) return "table";
    return op;
}

static int cli_append_json_value_token(cli_string_builder *sb, const cJSON *value) {
    if (!value) return sb_append(sb, "null");
    if (cJSON_IsString(value) && value->valuestring && cli_is_bare_dsl_token(value->valuestring)) {
        return sb_append(sb, value->valuestring);
    }
    char *json = cJSON_PrintUnformatted((cJSON *)value);
    if (!json) return -1;
    int rc = sb_append(sb, json);
    free(json);
    return rc;
}

static int cli_append_normalized_arg(cli_string_builder *sb, const cJSON *arg) {
    if (!arg || !arg->string) return 0;
    if (sb_append_char(sb, ' ') != 0) return -1;
    if (sb_append(sb, arg->string) != 0) return -1;
    if (sb_append_char(sb, '=') != 0) return -1;
    return cli_append_json_value_token(sb, arg);
}

static char *cli_ir_to_normalized_dsl(const tf_ir_plan *ir) {
    if (!ir) return NULL;
    cli_string_builder sb = {0};
    for (size_t i = 0; i < ir->n_nodes; i++) {
        const tf_ir_node *node = &ir->nodes[i];
        if (i > 0 && sb_append(&sb, " | ") != 0) goto fail;
        if (sb_append(&sb, cli_normalized_step_name(node->op)) != 0) goto fail;
        if (node->args && cJSON_IsObject(node->args)) {
            for (const cJSON *arg = node->args->child; arg; arg = arg->next) {
                if (cli_append_normalized_arg(&sb, arg) != 0) goto fail;
            }
        }
    }
    if (!sb.data) {
        sb.data = strdup("");
        if (!sb.data) return NULL;
    }
    return sb.data;

fail:
    free(sb.data);
    return NULL;
}

static void print_caps(FILE *f, uint32_t caps) {
    int first = 1;
    if (caps & TF_CAP_STREAMING) print_cap_item(f, &first, "streaming");
    if (caps & TF_CAP_BOUNDED_MEMORY) print_cap_item(f, &first, "bounded_memory");
    if (caps & TF_CAP_BROWSER_SAFE) print_cap_item(f, &first, "browser_safe");
    if (caps & TF_CAP_DETERMINISTIC) print_cap_item(f, &first, "deterministic");
    if (caps & TF_CAP_FS) print_cap_item(f, &first, "fs");
    if (caps & TF_CAP_NET) print_cap_item(f, &first, "net");
    if (first) fprintf(f, "none");
}

static const tf_ir_node *first_node_with_memory_class(const tf_ir_plan *ir, tf_memory_class cls) {
    for (size_t i = 0; i < ir->n_nodes; i++) {
        if (ir->nodes[i].memory_class == cls) return &ir->nodes[i];
    }
    return NULL;
}

static const tf_ir_node *first_blocking_node(const tf_ir_plan *ir) {
    return first_node_with_memory_class(ir, TF_MEM_BLOCKING);
}

static const tf_ir_node *first_key_state_node(const tf_ir_plan *ir) {
    return first_node_with_memory_class(ir, TF_MEM_KEY_STATE);
}

static int has_sort_head_pattern(const tf_ir_plan *ir) {
    for (size_t i = 0; i + 1 < ir->n_nodes; i++) {
        if (strcmp(ir->nodes[i].op, "sort") == 0 && strcmp(ir->nodes[i + 1].op, "head") == 0)
            return 1;
    }
    return 0;
}

static int node_arg_true(const tf_ir_node *node, const char *name) {
    cJSON *item = node && node->args ? cJSON_GetObjectItemCaseSensitive(node->args, name) : NULL;
    return cJSON_IsTrue(item);
}

static int node_op_is_unique(const tf_ir_node *node) {
    return node && node->op &&
           (strcmp(node->op, "unique") == 0 || strcmp(node->op, "dedup") == 0);
}

static int node_array_arg_nonempty_main(const tf_ir_node *node, const char *name) {
    cJSON *item = node && node->args ? cJSON_GetObjectItemCaseSensitive(node->args, name) : NULL;
    return cJSON_IsArray(item) && cJSON_GetArraySize(item) > 0;
}

static int node_positive_arg_main(const tf_ir_node *node, const char *name) {
    cJSON *item = node && node->args ? cJSON_GetObjectItemCaseSensitive(node->args, name) : NULL;
    return cJSON_IsNumber(item) && item->valuedouble > 0.0;
}

static int node_op_is_pivot_spillable(const tf_ir_node *node) {
    return node && node->op && strcmp(node->op, "pivot") == 0 &&
           !node_arg_true(node, "sorted") &&
           (node_array_arg_nonempty_main(node, "categories") || node_positive_arg_main(node, "max_categories"));
}


static int node_op_is_join_spillable(const tf_ir_node *node) {
    if (!node || !node->op) return 0;
    if (strcmp(node->op, "semi-join") == 0 || strcmp(node->op, "anti-join") == 0) return 1;
    if (strcmp(node->op, "join") != 0) return 0;
    if (!node->args) return 1;
    cJSON *how = cJSON_GetObjectItemCaseSensitive(node->args, "how");
    if (!cJSON_IsString(how) || !how->valuestring) return 1;
    return strcmp(how->valuestring, "inner") == 0 || strcmp(how->valuestring, "left") == 0 ||
           strcmp(how->valuestring, "semi") == 0 || strcmp(how->valuestring, "anti") == 0;
}

static int node_op_is_row_set_spillable(const tf_ir_node *node) {
    return node && node->op &&
           (strcmp(node->op, "intersect") == 0 || strcmp(node->op, "setdiff") == 0 ||
            strcmp(node->op, "intersect-all") == 0 || strcmp(node->op, "setdiff-all") == 0 ||
            strcmp(node->op, "union") == 0);
}

static int blocking_node_has_native_spill(const tf_ir_node *node) {
    return node && node->op && (strcmp(node->op, "sort") == 0 || node_op_is_pivot_spillable(node));
}

static int key_state_node_has_native_spill(const tf_ir_node *node) {
    return ((node_op_is_unique(node) ||
             (node && node->op && strcmp(node->op, "group-agg") == 0) ||
             node_op_is_join_spillable(node) ||
             node_op_is_row_set_spillable(node)) &&
            !node_arg_true(node, "sorted"));
}

static const tf_ir_node *first_key_state_node_without_native_spill(const tf_ir_plan *ir) {
    for (size_t i = 0; i < ir->n_nodes; i++) {
        const tf_ir_node *node = &ir->nodes[i];
        if (node->memory_class == TF_MEM_KEY_STATE && !key_state_node_has_native_spill(node))
            return node;
    }
    return NULL;
}

static int node_uses_native_spill(const tf_ir_node *node) {
    return blocking_node_has_native_spill(node) || key_state_node_has_native_spill(node);
}

static const char *explain_step_target(const tf_ir_node *node, const cli_memory_policy *policy) {
    if (policy->engine == CLI_ENGINE_DUCKDB) return "duckdb_sql";
    if (policy->engine == CLI_ENGINE_NATIVE && policy->spill_dir && node_uses_native_spill(node))
        return "native_spill";
    return "native";
}

static const tf_ir_node *first_blocking_node_without_native_spill(const tf_ir_plan *ir) {
    for (size_t i = 0; i < ir->n_nodes; i++) {
        const tf_ir_node *node = &ir->nodes[i];
        if (node->memory_class == TF_MEM_BLOCKING && !blocking_node_has_native_spill(node))
            return node;
    }
    return NULL;
}

static int blocking_plan_can_use_native_spill(const tf_ir_plan *ir) {
    return first_blocking_node(ir) && !first_blocking_node_without_native_spill(ir);
}

static const char *native_spill_support_summary(void) {
    return "native spill currently supports sort, capped unsorted pivot "
           "(categories=... or max_categories=N), unsorted unique/dedup, "
           "unsorted group-agg, capped unsorted inner/left joins, unsorted "
           "semi/anti filtering joins, unsorted intersect/setdiff/"
           "intersect-all/setdiff-all, and duplicate-eliminating union";
}

static int set_json_item(cJSON *obj, const char *name, cJSON *item) {
    if (!obj || !name || !item) {
        cJSON_Delete(item);
        return -1;
    }
    if (cJSON_GetObjectItemCaseSensitive(obj, name)) {
        if (cJSON_ReplaceItemInObjectCaseSensitive(obj, name, item)) return 0;
        cJSON_Delete(item);
        return -1;
    }
    if (tf_json_add_item(obj, name, item) != TF_OK) {
        cJSON_Delete(item);
        return -1;
    }
    return 0;
}

static int set_json_string(cJSON *obj, const char *name, const char *value) {
    return set_json_item(obj, name, cJSON_CreateString(value ? value : ""));
}

static int set_json_number(cJSON *obj, const char *name, double value) {
    return set_json_item(obj, name, cJSON_CreateNumber(value));
}

static int apply_native_spill_policy(tf_ir_plan *ir, const cli_memory_policy *policy) {
    if (!policy->spill_dir || policy->engine != CLI_ENGINE_NATIVE) return 0;
    for (size_t i = 0; i < ir->n_nodes; i++) {
        tf_ir_node *node = &ir->nodes[i];
        if (!node_uses_native_spill(node)) continue;
        if (!node->args) {
            node->args = cJSON_CreateObject();
            if (!node->args) return -1;
        }
        if (set_json_string(node->args, "spill_dir", policy->spill_dir) != 0) return -1;
        if (policy->has_memory_limit &&
            set_json_number(node->args, "spill_memory_bytes", (double)policy->memory_limit_bytes) != 0)
            return -1;
    }
    return 0;
}

static int validate_memory_policy(const tf_ir_plan *ir, const cli_memory_policy *policy) {
    const tf_ir_node *blocking = first_blocking_node(ir);
    const tf_ir_node *key_state = first_key_state_node(ir);

    if (policy->engine == CLI_ENGINE_DUCKDB) return 0;

    if (policy->spill_dir) {
        const tf_ir_node *unsupported_key = first_key_state_node_without_native_spill(ir);
        if (unsupported_key) {
            fprintf(stderr,
                    "error: --spill-dir was requested, but native spill is not implemented yet for key-state step '%s'\n",
                    unsupported_key->op);
            fprintf(stderr,
                    "hint: %s; use op-specific caps or an external engine for this plan\n",
                    native_spill_support_summary());
            return 1;
        }
        const tf_ir_node *unsupported = first_blocking_node_without_native_spill(ir);
        if (unsupported) {
            fprintf(stderr,
                    "error: --spill-dir was requested, but native spill is unavailable for blocking step '%s' with the current arguments\n",
                    unsupported->op);
            fprintf(stderr,
                    "hint: %s; use --allow-blocking only for known-small data or an external engine for this plan\n",
                    native_spill_support_summary());
            return 1;
        }
    }

    if (policy->has_memory_limit && blocking &&
        !(policy->spill_dir && blocking_plan_can_use_native_spill(ir))) {
        char cap[32];
        fprintf(stderr,
                "error: --memory %s was requested, but native byte caps are not implemented for blocking step '%s' (state=%s)\n",
                format_bytes(policy->memory_limit_bytes, cap, sizeof(cap)),
                blocking->op,
                blocking->state_estimate ? blocking->state_estimate : "unknown");
        fprintf(stderr,
                "hint: rewrite to a bounded op, choose an external engine, or use --spill-dir when the step has a native spill mode and required caps\n");
        return 1;
    }

    if (policy->has_memory_limit && key_state) {
        size_t estimated = 0;
        const tf_ir_node *failed = NULL;
        char reason[192] = {0};
        if (!tf_estimate_key_state_plan_bytes(ir, &estimated, &failed, reason, sizeof(reason))) {
            char cap[32];
            fprintf(stderr,
                    "error: --memory %s was requested, but %s\n",
                    format_bytes(policy->memory_limit_bytes, cap, sizeof(cap)),
                    reason[0] ? reason : "a key-state step is not byte-bounded");
            if (failed) {
                fprintf(stderr,
                        "hint: add the op-specific cap for step '%s' or use an external/spill engine\n",
                        failed->op);
            }
            return 1;
        }
        if (estimated > policy->memory_limit_bytes) {
            char cap[32], est[32];
            fprintf(stderr,
                    "error: estimated native key-state memory %s exceeds --memory %s\n",
                    format_bytes(estimated, est, sizeof(est)),
                    format_bytes(policy->memory_limit_bytes, cap, sizeof(cap)));
            fprintf(stderr,
                    "hint: lower key/category/lookup caps or raise --memory for this known-bounded plan\n");
            return 1;
        }
    }

    if (blocking && !policy->allow_blocking &&
        !(policy->spill_dir && blocking_plan_can_use_native_spill(ir))) {
        fprintf(stderr,
                "error: blocking step '%s' requires full input in native mode (state=%s)\n",
                blocking->op,
                blocking->state_estimate ? blocking->state_estimate : "unknown");
        fprintf(stderr,
                "hint: add --allow-blocking only for known-small inputs, rewrite to a bounded op, or choose an external engine/spill mode when available\n");
        if (has_sort_head_pattern(ir)) {
            fprintf(stderr,
                    "hint: replace sort | head N with top N column when top-k semantics are acceptable\n");
        }
        return 1;
    }

    return 0;
}

static void print_explain(const tf_ir_plan *ir, const cli_memory_policy *policy) {
    tf_memory_class worst_mem = TF_MEM_ROW_LOCAL;
    const char *worst_state = "O(batch_rows * columns)";
    int has_flush = 0;
    int has_per_batch = 0;
    int has_data_schema = 0;

    for (size_t i = 0; i < ir->n_nodes; i++) {
        const tf_ir_node *node = &ir->nodes[i];
        if (memory_rank(node->memory_class) > memory_rank(worst_mem)) {
            worst_mem = node->memory_class;
            worst_state = node->state_estimate ? node->state_estimate : "unknown";
        }
        if (node->emit_class == TF_EMIT_ON_FLUSH) has_flush = 1;
        if (node->emit_class == TF_EMIT_PER_BATCH) has_per_batch = 1;
        if (node->schema_class == TF_SCHEMA_DATA_DEPENDENT) has_data_schema = 1;
    }

    char cap[32];
    printf("Tranfi execution plan\n");
    char *normalized_dsl = cli_ir_to_normalized_dsl(ir);
    printf("normalized_dsl: %s\n", normalized_dsl ? normalized_dsl : "null");
    free(normalized_dsl);
    printf("execution_target: %s\n", policy->spill_dir && policy->engine == CLI_ENGINE_NATIVE ? "native+spill" : engine_name(policy->engine));
    printf("memory_policy: %s\n", policy->spill_dir ? "spill" : (policy->allow_blocking ? "allow_blocking" : "strict"));
    printf("memory_limit: %s\n", policy->has_memory_limit ? format_bytes(policy->memory_limit_bytes, cap, sizeof(cap)) : "none");
    printf("spill_dir: %s\n", policy->spill_dir ? policy->spill_dir : "none");
    printf("memory_class: %s\n", tf_memory_class_name(worst_mem));
    printf("emit_class: %s\n", has_flush && has_per_batch ? "mixed" :
           (has_flush ? "on_flush" : "per_batch"));
    printf("schema_class: %s\n", has_data_schema ? "data_dependent" : "stable_or_parametric");
    printf("state_estimate: %s\n", worst_state);
    if (first_key_state_node(ir)) {
        size_t key_bytes = 0;
        const tf_ir_node *failed = NULL;
        char reason[192] = {0};
        if (tf_estimate_key_state_plan_bytes(ir, &key_bytes, &failed, reason, sizeof(reason))) {
            char key_est[32];
            printf("state_bytes_estimate: %s\n", format_bytes(key_bytes, key_est, sizeof(key_est)));
        } else {
            printf("state_bytes_estimate: unbounded (%s)\n", reason[0] ? reason : "missing key-state byte estimator");
        }
    } else {
        printf("state_bytes_estimate: none\n");
    }
    if (first_blocking_node(ir)) {
        if (policy->spill_dir && policy->engine == CLI_ENGINE_NATIVE && blocking_plan_can_use_native_spill(ir))
            printf("note: blocking step will use native spill files\n");
        else if (policy->engine == CLI_ENGINE_NATIVE && !policy->allow_blocking)
            printf("warning: blocking native step present; default execution will reject it unless --allow-blocking is set\n");
        else
            printf("warning: blocking step present; selected policy must provide a bounded/external execution path\n");
        if (has_sort_head_pattern(ir))
            printf("hint: replace sort | head N with top N column when top-k semantics are acceptable\n");
    }
    printf("steps:\n");
    for (size_t i = 0; i < ir->n_nodes; i++) {
        const tf_ir_node *node = &ir->nodes[i];
        printf("  %zu. %s\n", i, node->op);
        printf("     target=%s memory=%s emit=%s schema=%s state=%s caps=",
               explain_step_target(node, policy),
               tf_memory_class_name(node->memory_class),
               tf_emit_class_name(node->emit_class),
               tf_schema_class_name(node->schema_class),
               node->state_estimate ? node->state_estimate : "unknown");
        print_caps(stdout, node->caps);
        printf("\n");
        if (node->memory_class == TF_MEM_KEY_STATE) {
            size_t step_bytes = 0;
            char reason[192] = {0};
            if (tf_estimate_step_state_bytes(node, &step_bytes, reason, sizeof(reason))) {
                char step_est[32];
                printf("     state_bytes_estimate=%s\n", format_bytes(step_bytes, step_est, sizeof(step_est)));
            } else {
                printf("     state_bytes_estimate=unbounded (%s)\n", reason[0] ? reason : "missing key-state byte estimator");
            }
        }
    }

    char *ir_json = tf_ir_to_json(ir);
    if (ir_json) {
        printf("ir_json: %s\n", ir_json);
        free(ir_json);
    } else {
        printf("ir_json: null\n");
    }
}

int main(int argc, char **argv) {
    const char *pipeline_file = NULL;
    const char *pipeline_text = NULL;
    const char *input_file = NULL;
    const char *output_file = NULL;
    int json_mode = 0;
    int sql_mode = 0;
    int explain_mode = 0;
    const char *sql_dialect = "duckdb";
    cli_memory_policy policy = {0};
    policy.engine = CLI_ENGINE_NATIVE;
    int quiet = 0;
    int progress = 0;
    int raw_stats = 0;
    const char *stats_json_file = NULL;

    /* Parse options */
    int argi = 1;
    while (argi < argc && argv[argi][0] == '-') {
        const char *opt = argv[argi];
        if (strcmp(opt, "-h") == 0 || strcmp(opt, "--help") == 0) {
            usage(argv[0]);
            return 0;
        } else if (strcmp(opt, "-v") == 0 || strcmp(opt, "--version") == 0) {
            printf("tranfi %s\n", tf_version());
            return 0;
        } else if (strcmp(opt, "-R") == 0 || strcmp(opt, "--recipes") == 0) {
            size_t n = tf_recipe_count();
            printf("Built-in recipes (%zu):\n\n", n);
            for (size_t i = 0; i < n; i++) {
                printf("  %-12s %s\n", tf_recipe_name(i), tf_recipe_description(i));
                printf("  %-12s %s\n", "", tf_recipe_dsl(i));
                printf("\n");
            }
            return 0;
        } else if (strcmp(opt, "-j") == 0) {
            json_mode = 1;
            sql_mode = 0;
        } else if (strcmp(opt, "--target") == 0 || strncmp(opt, "--target=", 9) == 0) {
            const char *value = NULL;
            if (strncmp(opt, "--target=", 9) == 0) {
                value = opt + 9;
            } else {
                argi++;
                if (argi >= argc) {
                    fprintf(stderr, "error: --target requires native, json, or sql\n");
                    return 1;
                }
                value = argv[argi];
            }
            if (strcmp(value, "native") == 0) {
                json_mode = 0;
                sql_mode = 0;
            } else if (strcmp(value, "json") == 0 || strcmp(value, "ir") == 0) {
                json_mode = 1;
                sql_mode = 0;
            } else if (strcmp(value, "sql") == 0) {
                json_mode = 0;
                sql_mode = 1;
            } else {
                fprintf(stderr, "error: unknown --target '%s' (expected native, json, or sql)\n", value);
                return 1;
            }
        } else if (strcmp(opt, "--dialect") == 0 || strncmp(opt, "--dialect=", 10) == 0) {
            const char *value = NULL;
            if (strncmp(opt, "--dialect=", 10) == 0) {
                value = opt + 10;
            } else {
                argi++;
                if (argi >= argc) {
                    fprintf(stderr, "error: --dialect requires duckdb, sqlite, or postgres\n");
                    return 1;
                }
                value = argv[argi];
            }
            if (!is_known_sql_dialect(value)) {
                fprintf(stderr, "error: unknown SQL dialect '%s' (expected duckdb, sqlite, or postgres)\n", value);
                return 1;
            }
            sql_dialect = value;
        } else if (strcmp(opt, "--explain") == 0) {
            explain_mode = 1;
        } else if (strcmp(opt, "--allow-blocking") == 0) {
            policy.allow_blocking = 1;
        } else if (strcmp(opt, "--fail-on-blocking") == 0) {
            policy.fail_on_blocking_seen = 1;
        } else if (strcmp(opt, "--memory") == 0 || strncmp(opt, "--memory=", 9) == 0) {
            const char *value = NULL;
            if (strncmp(opt, "--memory=", 9) == 0) {
                value = opt + 9;
            } else {
                argi++;
                if (argi >= argc) {
                    fprintf(stderr, "error: --memory requires a size such as max:64MB\n");
                    return 1;
                }
                value = argv[argi];
            }
            char err[160];
            if (parse_size_bytes(value, &policy.memory_limit_bytes, err, sizeof(err)) != 0) {
                fprintf(stderr, "error: invalid --memory value: %s\n", err);
                return 1;
            }
            policy.has_memory_limit = 1;
        } else if (strcmp(opt, "--spill-dir") == 0 || strncmp(opt, "--spill-dir=", 12) == 0) {
            if (strncmp(opt, "--spill-dir=", 12) == 0) {
                policy.spill_dir = opt + 12;
            } else {
                argi++;
                if (argi >= argc) {
                    fprintf(stderr, "error: --spill-dir requires a directory argument\n");
                    return 1;
                }
                policy.spill_dir = argv[argi];
            }
            if (!policy.spill_dir || policy.spill_dir[0] == '\0') {
                fprintf(stderr, "error: --spill-dir requires a non-empty directory\n");
                return 1;
            }
        } else if (strcmp(opt, "--engine") == 0 || strncmp(opt, "--engine=", 9) == 0) {
            const char *value = NULL;
            if (strncmp(opt, "--engine=", 9) == 0) {
                value = opt + 9;
            } else {
                argi++;
                if (argi >= argc) {
                    fprintf(stderr, "error: --engine requires native or duckdb\n");
                    return 1;
                }
                value = argv[argi];
            }
            char err[160];
            if (set_engine(&policy, value, err, sizeof(err)) != 0) {
                fprintf(stderr, "error: %s\n", err);
                return 1;
            }
        } else if (strcmp(opt, "-q") == 0) {
            quiet = 1;
        } else if (strcmp(opt, "-p") == 0 || strcmp(opt, "--progress") == 0) {
            progress = 1;
        } else if (strcmp(opt, "--raw") == 0) {
            raw_stats = 1;
        } else if (strcmp(opt, "--stats-json") == 0 || strncmp(opt, "--stats-json=", 13) == 0) {
            if (strncmp(opt, "--stats-json=", 13) == 0) {
                stats_json_file = opt + 13;
            } else {
                argi++;
                if (argi >= argc) {
                    fprintf(stderr, "error: --stats-json requires a file path, or - for stderr\n");
                    return 1;
                }
                stats_json_file = argv[argi];
            }
            if (!stats_json_file || stats_json_file[0] == '\0') {
                fprintf(stderr, "error: --stats-json requires a non-empty file path, or - for stderr\n");
                return 1;
            }
        } else if (strcmp(opt, "-f") == 0) {
            argi++;
            if (argi >= argc) {
                fprintf(stderr, "error: -f requires a file argument\n");
                return 1;
            }
            pipeline_file = argv[argi];
        } else if (strcmp(opt, "-i") == 0) {
            argi++;
            if (argi >= argc) {
                fprintf(stderr, "error: -i requires a file argument\n");
                return 1;
            }
            input_file = argv[argi];
        } else if (strcmp(opt, "-o") == 0) {
            argi++;
            if (argi >= argc) {
                fprintf(stderr, "error: -o requires a file argument\n");
                return 1;
            }
            output_file = argv[argi];
        } else {
            fprintf(stderr, "error: unknown option '%s'\n", opt);
            return 1;
        }
        argi++;
    }

    if (policy.allow_blocking && policy.fail_on_blocking_seen) {
        fprintf(stderr, "error: --allow-blocking conflicts with --fail-on-blocking\n");
        return 1;
    }
    if (sql_mode) {
        policy.engine = CLI_ENGINE_DUCKDB;
    }

    /* Get pipeline text */
    char *file_content = NULL;
    if (pipeline_file) {
        file_content = read_file(pipeline_file);
        if (!file_content) {
            fprintf(stderr, "error: cannot read file '%s'\n", pipeline_file);
            return 1;
        }
        pipeline_text = file_content;
    } else if (argi < argc) {
        pipeline_text = argv[argi];
    } else {
        fprintf(stderr, "error: no pipeline specified\n\n");
        usage(argv[0]);
        free(file_content);
        return 1;
    }

    /* Parse pipeline: recipe name → JSON recipe → DSL */
    char *error = NULL;
    tf_ir_plan *ir = NULL;
    size_t pt_len = strlen(pipeline_text);
    /* Skip leading whitespace for detection */
    const char *pt = pipeline_text;
    while (*pt == ' ' || *pt == '\t' || *pt == '\n' || *pt == '\r') pt++;

    if (*pt == '{') {
        /* JSON recipe */
        ir = tf_ir_from_json(pipeline_text, pt_len, &error);
    } else if (!strchr(pt, '|') && !strchr(pt, ' ')) {
        /* Single word — try built-in recipe */
        const char *recipe_dsl = tf_recipe_find_dsl(pt);
        if (recipe_dsl) {
            ir = tf_dsl_parse(recipe_dsl, strlen(recipe_dsl), &error);
        } else {
            ir = tf_dsl_parse(pipeline_text, pt_len, &error);
        }
    } else {
        ir = tf_dsl_parse(pipeline_text, pt_len, &error);
    }
    free(file_content);

    if (!ir) {
        fprintf(stderr, "error: %s\n", error ? error : "failed to parse pipeline");
        free(error);
        return 1;
    }

    /* Validate */
    if (tf_ir_validate(ir) != TF_OK) {
        fprintf(stderr, "error: %s\n", ir->error ? ir->error : "validation failed");
        tf_ir_plan_free(ir);
        return 1;
    }

    /* Schema inference allows unknown runtime schemas but reports invalid known-schema plans. */
    tf_set_last_error(NULL);
    if (tf_ir_infer_schema(ir) != TF_OK) {
        const char *detail = tf_last_error();
        if (detail && detail[0])
            fprintf(stderr, "error: schema inference failed: %s\n", detail);
        else
            fprintf(stderr, "error: schema inference failed\n");
        tf_ir_plan_free(ir);
        return 1;
    }

    if (apply_native_spill_policy(ir, &policy) != 0) {
        fprintf(stderr, "error: failed to apply native spill policy\n");
        tf_ir_plan_free(ir);
        return 1;
    }

    if (tf_ir_validate(ir) != TF_OK) {
        fprintf(stderr, "error: %s\n", ir->error ? ir->error : "validation failed after applying native spill policy");
        tf_ir_plan_free(ir);
        return 1;
    }

    if (explain_mode) {
        print_explain(ir, &policy);
        tf_ir_plan_free(ir);
        return 0;
    }

    if (sql_mode) {
        if (!sql_dialect_supported(sql_dialect)) {
            fprintf(stderr,
                    "error: SQL dialect '%s' is recognized but not implemented yet; only duckdb lowering is available\n",
                    sql_dialect);
            tf_ir_plan_free(ir);
            return 1;
        }
        char *sql_error = NULL;
        char *sql = tf_ir_plan_to_sql(ir, &sql_error);
        if (!sql) {
            fprintf(stderr, "error: SQL compile failed for dialect '%s': %s\n",
                    sql_dialect, sql_error ? sql_error : "unknown error");
            free(sql_error);
            tf_ir_plan_free(ir);
            return 1;
        }
        printf("%s\n", sql);
        tf_string_free(sql);
        tf_ir_plan_free(ir);
        return 0;
    }

    /* JSON mode: print IR and exit */
    if (json_mode) {
        char *json = tf_ir_to_json(ir);
        if (json) {
            printf("%s\n", json);
            free(json);
        }
        tf_ir_plan_free(ir);
        return 0;
    }

    if (validate_memory_policy(ir, &policy) != 0) {
        tf_ir_plan_free(ir);
        return 1;
    }

    if (policy.engine == CLI_ENGINE_DUCKDB) {
        fprintf(stderr,
                "error: the C CLI cannot execute --engine duckdb directly; use Python/Node with engine='duckdb' or compile SQL with --target sql --dialect duckdb\n");
        tf_ir_plan_free(ir);
        return 1;
    }

    /* Compile to native pipeline */
    tf_pipeline *p = tf_pipeline_create_from_ir(ir);
    tf_ir_plan_free(ir);

    if (!p) {
        fprintf(stderr, "error: %s\n",
                tf_last_error() ? tf_last_error() : "failed to create pipeline");
        return 1;
    }

    /* Open I/O files */
    FILE *fin = stdin;
    FILE *fout = stdout;
    FILE *fstats = NULL;
    int close_stats = 0;

    if (input_file) {
        fin = fopen(input_file, "rb");
        if (!fin) {
            fprintf(stderr, "error: cannot open input file '%s'\n", input_file);
            tf_pipeline_free(p);
            return 1;
        }
    }

    if (output_file) {
        fout = fopen(output_file, "wb");
        if (!fout) {
            fprintf(stderr, "error: cannot open output file '%s'\n", output_file);
            if (fin != stdin) fclose(fin);
            tf_pipeline_free(p);
            return 1;
        }
    }

    if (stats_json_file) {
        if (strcmp(stats_json_file, "-") == 0) {
            fstats = stderr;
        } else {
            fstats = fopen(stats_json_file, "wb");
            if (!fstats) {
                fprintf(stderr, "error: cannot open stats JSON file '%s'\n", stats_json_file);
                if (fin != stdin) fclose(fin);
                if (fout != stdout) fclose(fout);
                tf_pipeline_free(p);
                return 1;
            }
            close_stats = 1;
        }
    }

    /* Decide whether to buffer output for report formatting.
     * When stdout is a TTY and --raw is not set, buffer main output
     * and try to render it as a rich report. Falls back to raw CSV
     * if the output doesn't look like a stats table. */
    int try_report = !raw_stats && !output_file && isatty(STDOUT_FILENO);
    if (!try_report) {
        if (tf_pipeline_set_sink(p, TF_CHAN_MAIN, cli_file_output_sink, fout) != TF_OK) {
            fprintf(stderr, "error: failed to attach output sink\n");
            if (fin != stdin) fclose(fin);
            if (fout != stdout) fclose(fout);
            if (close_stats && fstats) fclose(fstats);
            tf_pipeline_free(p);
            return 1;
        }
    }

    /* Stream input → pipeline → output */
    uint8_t read_buf[READ_BUF_SIZE];
    size_t nread;
    size_t total_bytes = 0;

    /* Output buffer (used when try_report is true) */
    size_t out_cap = PULL_BUF_SIZE;
    size_t out_len = 0;
    char *out_buf = try_report ? tf_mallocarray_checked(out_cap, sizeof(char)) : NULL;
    if (try_report && !out_buf) {
        out_cap = 0;
        try_report = 0;
    }

    while ((nread = fread(read_buf, 1, sizeof(read_buf), fin)) > 0) {
        if (tf_pipeline_push(p, read_buf, nread) != TF_OK) {
            fprintf(stderr, "error: %s\n",
                    tf_pipeline_error(p) ? tf_pipeline_error(p) : "push failed");
            free(out_buf);
            if (fin != stdin) fclose(fin);
            if (fout != stdout) fclose(fout);
            if (close_stats && fstats) fclose(fstats);
            tf_pipeline_free(p);
            return 1;
        }

        total_bytes += nread;

        /* Pull any available output immediately (streaming) */
        uint8_t pull_buf[PULL_BUF_SIZE];
        size_t n;
        while ((n = tf_pipeline_pull(p, TF_CHAN_MAIN, pull_buf, sizeof(pull_buf))) > 0) {
            if (try_report && out_buf) {
                if (cli_report_buffer_append(&out_buf, &out_len, &out_cap, pull_buf, n) != 0) {
                    cli_disable_report_buffer(fout, &out_buf, &out_len, &out_cap);
                    fwrite(pull_buf, 1, n, fout);
                    try_report = 0;
                }
            } else {
                fwrite(pull_buf, 1, n, fout);
            }
        }

        /* Show progress */
        if (progress) {
            char bytes_str[32];
            format_bytes(total_bytes, bytes_str, sizeof(bytes_str));
            fprintf(stderr, "\r%s processed", bytes_str);
        }
    }

    /* Finish */
    if (tf_pipeline_finish(p) != TF_OK) {
        fprintf(stderr, "error: %s\n",
                tf_pipeline_error(p) ? tf_pipeline_error(p) : "finish failed");
        free(out_buf);
        if (fin != stdin) fclose(fin);
        if (fout != stdout) fclose(fout);
        if (close_stats && fstats) fclose(fstats);
        tf_pipeline_free(p);
        return 1;
    }

    /* Pull remaining output */
    uint8_t pull_buf[PULL_BUF_SIZE];
    size_t n;
    while ((n = tf_pipeline_pull(p, TF_CHAN_MAIN, pull_buf, sizeof(pull_buf))) > 0) {
        if (try_report && out_buf) {
            if (cli_report_buffer_append(&out_buf, &out_len, &out_cap, pull_buf, n) != 0) {
                cli_disable_report_buffer(fout, &out_buf, &out_len, &out_cap);
                fwrite(pull_buf, 1, n, fout);
                try_report = 0;
            }
        } else {
            fwrite(pull_buf, 1, n, fout);
        }
    }

    /* Try report formatting, fall back to raw */
    if (try_report && out_buf && out_len > 0) {
        char *report = tf_report_format(out_buf, out_len, 1);
        if (report) {
            fwrite(report, 1, strlen(report), fout);
            free(report);
        } else {
            fwrite(out_buf, 1, out_len, fout);
        }
    }
    free(out_buf);
    fflush(fout);

    if (progress) {
        char bytes_str[32];
        format_bytes(total_bytes, bytes_str, sizeof(bytes_str));
        fprintf(stderr, "\r%s processed (done)\n", bytes_str);
    }

    /* Pull errors to stderr */
    while ((n = tf_pipeline_pull(p, TF_CHAN_ERRORS, pull_buf, sizeof(pull_buf))) > 0) {
        fwrite(pull_buf, 1, n, stderr);
    }

    /* Pull stats to the requested machine-readable path, or stderr unless quiet. */
    FILE *stats_out = stats_json_file ? fstats : (!quiet ? stderr : NULL);
    if (stats_out) {
        while ((n = tf_pipeline_pull(p, TF_CHAN_STATS, pull_buf, sizeof(pull_buf))) > 0) {
            fwrite(pull_buf, 1, n, stats_out);
        }
        fflush(stats_out);
    }

    /* Cleanup */
    if (fin != stdin) fclose(fin);
    if (fout != stdout) fclose(fout);
    if (close_stats && fstats) fclose(fstats);
    tf_pipeline_free(p);
    return 0;
}
