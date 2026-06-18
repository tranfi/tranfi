/*
 * test_oom.c -- allocator-failure regression tests for Tranfi.
 *
 * This binary is linked with GNU ld --wrap hooks for malloc/calloc/realloc/
 * strdup/strndup. It fails one allocation at a time while compiling and running
 * representative pipelines and SQL compile paths. Under ASan/UBSan this catches
 * invalid row counters, use-after-free, double-free, and unchecked allocation/
 * write paths.
 */

#include "tranfi.h"
#include <assert.h>
#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

void *__real_malloc(size_t size);
void *__real_calloc(size_t nmemb, size_t size);
void *__real_realloc(void *ptr, size_t size);
char *__real_strdup(const char *s);
char *__real_strndup(const char *s, size_t n);

static int oom_enabled = 0;
static size_t oom_fail_at = 0;
static size_t oom_alloc_count = 0;
static int oom_failed = 0;

static int oom_should_fail(void) {
    if (!oom_enabled) return 0;
    oom_alloc_count++;
    if (oom_alloc_count == oom_fail_at) {
        oom_failed = 1;
        return 1;
    }
    return 0;
}

void *__wrap_malloc(size_t size) {
    if (oom_should_fail()) return NULL;
    return __real_malloc(size);
}

void *__wrap_calloc(size_t nmemb, size_t size) {
    if (oom_should_fail()) return NULL;
    return __real_calloc(nmemb, size);
}

void *__wrap_realloc(void *ptr, size_t size) {
    if (size != 0 && oom_should_fail()) return NULL;
    return __real_realloc(ptr, size);
}

char *__wrap_strdup(const char *s) {
    if (oom_should_fail()) return NULL;
    return __real_strdup(s);
}

char *__wrap_strndup(const char *s, size_t n) {
    if (oom_should_fail()) return NULL;
    return __real_strndup(s, n);
}

typedef struct oom_case {
    const char *name;
    const char *dsl;
    const char *input;
    size_t max_fail_points;
} oom_case;

typedef struct oom_sql_case {
    const char *name;
    const char *dsl;
    int expect_success;
    size_t max_fail_points;
} oom_sql_case;

static void drain_all(tf_pipeline *p) {
    uint8_t buf[512];
    for (int chan = 0; chan < TF_NUM_CHANNELS; chan++) {
        while (tf_pipeline_pull(p, chan, buf, sizeof(buf)) > 0) {}
    }
}

static int run_case_once(const oom_case *tc) {
    char *error = NULL;
    char *json = tf_compile_dsl(tc->dsl, strlen(tc->dsl), &error);
    if (!json) {
        free(error);
        return TF_ERROR;
    }

    tf_pipeline *p = tf_pipeline_create(json, strlen(json));
    tf_string_free(json);
    if (!p) {
        free(error);
        return TF_ERROR;
    }

    size_t len = strlen(tc->input);
    size_t cut = len > 3 ? 3 : len;
    int rc = TF_OK;
    if (cut > 0 && tf_pipeline_push(p, (const uint8_t *)tc->input, cut) != TF_OK) rc = TF_ERROR;
    if (rc == TF_OK && len > cut &&
        tf_pipeline_push(p, (const uint8_t *)tc->input + cut, len - cut) != TF_OK) {
        rc = TF_ERROR;
    }
    if (rc == TF_OK && tf_pipeline_finish(p) != TF_OK) rc = TF_ERROR;

    drain_all(p);
    tf_pipeline_free(p);
    free(error);
    return rc;
}

static int run_capture_once(const char *dsl, const char *input, char *main_out,
                            size_t main_out_cap) {
    if (main_out_cap > 0) main_out[0] = '\0';

    char *error = NULL;
    char *json = tf_compile_dsl(dsl, strlen(dsl), &error);
    if (!json) {
        free(error);
        return TF_ERROR;
    }

    tf_pipeline *p = tf_pipeline_create(json, strlen(json));
    tf_string_free(json);
    if (!p) {
        free(error);
        return TF_ERROR;
    }

    int rc = TF_OK;
    size_t len = strlen(input);
    if (tf_pipeline_push(p, (const uint8_t *)input, len) != TF_OK) rc = TF_ERROR;
    if (rc == TF_OK && tf_pipeline_finish(p) != TF_OK) rc = TF_ERROR;

    if (main_out_cap > 0) {
        size_t off = 0;
        while (off + 1 < main_out_cap) {
            size_t n = tf_pipeline_pull(p, TF_CHAN_MAIN, (uint8_t *)main_out + off,
                                        main_out_cap - off - 1);
            if (n == 0) break;
            off += n;
        }
        main_out[off] = '\0';
    }
    drain_all(p);
    tf_pipeline_free(p);
    free(error);
    return rc;
}

static size_t count_successful_allocs(const oom_case *tc) {
    oom_enabled = 1;
    oom_fail_at = (size_t)-1;
    oom_alloc_count = 0;
    oom_failed = 0;
    int rc = run_case_once(tc);
    oom_enabled = 0;
    assert(rc == TF_OK);
    assert(!oom_failed);
    return oom_alloc_count;
}

static void run_case_with_oom(const oom_case *tc) {
    size_t allocs = count_successful_allocs(tc);
    size_t limit = allocs;
    if (tc->max_fail_points > 0 && limit > tc->max_fail_points) limit = tc->max_fail_points;
    assert(limit > 0);

    for (size_t fail_at = 1; fail_at <= limit; fail_at++) {
        oom_enabled = 1;
        oom_fail_at = fail_at;
        oom_alloc_count = 0;
        oom_failed = 0;
        (void)run_case_once(tc);
        oom_enabled = 0;
        assert(oom_failed);
    }
}

static void run_case_no_silent_output_oom(const char *name, const char *dsl,
                                          const char *input,
                                          size_t max_fail_points) {
    char baseline[8192];
    oom_enabled = 1;
    oom_fail_at = (size_t)-1;
    oom_alloc_count = 0;
    oom_failed = 0;
    int rc = run_capture_once(dsl, input, baseline, sizeof(baseline));
    oom_enabled = 0;
    assert(rc == TF_OK);
    assert(!oom_failed);
    size_t allocs = oom_alloc_count;
    size_t limit = allocs;
    if (max_fail_points > 0 && limit > max_fail_points) limit = max_fail_points;
    assert(limit > 0);

    for (size_t fail_at = 1; fail_at <= limit; fail_at++) {
        char got[8192];
        oom_enabled = 1;
        oom_fail_at = fail_at;
        oom_alloc_count = 0;
        oom_failed = 0;
        rc = run_capture_once(dsl, input, got, sizeof(got));
        oom_enabled = 0;

        if (oom_failed && rc == TF_OK && strcmp(got, baseline) != 0) {
            fprintf(stderr, "%s: silent output drift at allocation %zu\n", name, fail_at);
            fprintf(stderr, "baseline:\n%s\ngot:\n%s\n", baseline, got);
            assert(!"OOM allocation failure changed successful output");
        }
        assert(oom_failed);
    }
}

static int run_sql_case_once(const oom_sql_case *tc) {
    char *error = NULL;
    char *sql = tf_compile_to_sql(tc->dsl, strlen(tc->dsl), &error);
    int got_success = sql != NULL;
    if (sql) tf_string_free(sql);
    free(error);
    if (tc->expect_success) return got_success ? TF_OK : TF_ERROR;
    return got_success ? TF_ERROR : TF_OK;
}

static size_t count_successful_sql_allocs(const oom_sql_case *tc) {
    oom_enabled = 1;
    oom_fail_at = (size_t)-1;
    oom_alloc_count = 0;
    oom_failed = 0;
    int rc = run_sql_case_once(tc);
    oom_enabled = 0;
    assert(rc == TF_OK);
    assert(!oom_failed);
    return oom_alloc_count;
}

static void run_sql_case_with_oom(const oom_sql_case *tc) {
    size_t allocs = count_successful_sql_allocs(tc);
    size_t limit = allocs;
    if (tc->max_fail_points > 0 && limit > tc->max_fail_points) limit = tc->max_fail_points;
    assert(limit > 0);

    for (size_t fail_at = 1; fail_at <= limit; fail_at++) {
        oom_enabled = 1;
        oom_fail_at = fail_at;
        oom_alloc_count = 0;
        oom_failed = 0;
        (void)run_sql_case_once(tc);
        oom_enabled = 0;
        assert(oom_failed);
    }
}

int main(void) {
    const char *people =
        "name,age,city,score,tags,x,y,color\n"
        " Alice ,30,NY,10,a|b,1,4,red\n"
        "Bob,25,LA,20,c,2,5,blue\n"
        "Cara,35,NY,30,d|e,3,6,red\n";
    const char *sorted_people =
        "name,age,city,score,tags,x,y,color\n"
        "Bob,25,LA,20,c,2,5,blue\n"
        " Alice ,30,NY,10,a|b,1,4,red\n"
        "Cara,35,NY,30,d|e,3,6,red\n";
    const char *sorted_people_unmatched =
        "name,age,city,score,tags,x,y,color\n"
        "Bob,25,LA,20,c,2,5,blue\n"
        " Alice ,30,NY,10,a|b,1,4,red\n"
        "Cara,35,SF,30,d|e,3,6,red\n";
    const char *missing_people =
        "name,age,city,score\n"
        "Alice,30,NY,10\n"
        "Bob,25,NA,20\n"
        "Cara,35,LA,30\n";
    const char *series =
        "t,x,y\n"
        "1,1,10\n"
        "2,,20\n"
        "3,,30\n"
        "4,4,40\n";
    const char *dates =
        "name,d,ts,score\n"
        "Alice,2024-03-15,2024-03-15T12:34:56Z,10\n"
        "Bob,2023-12-25,2023-12-25T08:09:10Z,20\n";
    const char *reshape_dates =
        "name,full,d,score,tags,x,y,active\n"
        "Alice,Alice Smith,2024-03-15,10,a|b,1,4,true\n"
        "Bob,Bob Jones,2023-12-25,20,c,2,5,false\n"
        "Cara,Cara Stone,2024-01-02,30,d|e,3,6,true\n";
    const char *selector_people =
        "id,score_math,score_read,name,active,code\n"
        "1,90.2,80.7,Alice,true,OK\n"
        "2,70.4,95.6,Bob,false,BAD\n"
        "3,88.8,91.2,Cara,true,OK\n";
    const char *repair_rows =
        "a,b,c\n"
        "1,2\n"
        "3,4,5,6\n";
    const char *audit_rows =
        "name,city,note,secret\n"
        "Alice,NY,NA,111\n"
        "Bob,LA,,bad\n"
        "Cara,SF,NA,12x\n";
    const char *no_header_rows =
        "# generated by upstream\n"
        "Alice,30,NY\n"
        "Bob,NA,LA\n"
        "Cara,35,SF # trailing note\n";

    const char *union_lookup_path = "/tmp/tranfi_oom_union_lookup.csv";
    FILE *union_lookup = fopen(union_lookup_path, "wb");
    assert(union_lookup);
    fputs("name,age,city,score,tags,x,y,color\n"
          "Lookup,40,SF,40,z,7,8,green\n"
          "Dup,99,NY,99,q,9,9,red\n",
          union_lookup);
    assert(fclose(union_lookup) == 0);
    const char *sorted_lookup_path = "/tmp/tranfi_oom_sorted_lookup.csv";
    FILE *sorted_lookup = fopen(sorted_lookup_path, "wb");
    assert(sorted_lookup);
    fputs("city\nLA\nNY\n", sorted_lookup);
    assert(fclose(sorted_lookup) == 0);
    const char *join_lookup_path = "/tmp/tranfi_oom_join_lookup.csv";
    FILE *join_lookup = fopen(join_lookup_path, "wb");
    assert(join_lookup);
    fputs("city,pop,region\n"
          "NY,8000,East\n"
          "NY,8100,East2\n"
          "SF,870,West\n",
          join_lookup);
    assert(fclose(join_lookup) == 0);
    char join_silent_dsl[512];
    int join_silent_n = snprintf(join_silent_dsl, sizeof(join_silent_dsl),
                                 "csv batch_size=1 | join %s on city "
                                 "max_lookup_rows=10 max_lookup_keys=10 "
                                 "max_state_bytes=1048576 max_matches_per_row=2 "
                                 "max_output_rows=10 | csv",
                                 join_lookup_path);
    assert(join_silent_n > 0 && (size_t)join_silent_n < sizeof(join_silent_dsl));
    run_case_no_silent_output_oom(
        "hash_join_left_key_materialization",
        join_silent_dsl,
        "city,name\nNY,Alice\nSF,Cara\n",
        520);
    const char *join_sorted_lookup_path = "/tmp/tranfi_oom_join_sorted_lookup.csv";
    FILE *join_sorted_lookup = fopen(join_sorted_lookup_path, "wb");
    assert(join_sorted_lookup);
    fputs("city,pop,region\n"
          "LA,3900,West\n"
          "NY,8000,East\n"
          "NY,8100,East2\n",
          join_sorted_lookup);
    assert(fclose(join_sorted_lookup) == 0);
    const char *bag_lookup_path = "/tmp/tranfi_oom_bag_lookup.csv";
    FILE *bag_lookup = fopen(bag_lookup_path, "wb");
    assert(bag_lookup);
    fputs("city\nNY\n", bag_lookup);
    assert(fclose(bag_lookup) == 0);
    char set_silent_dsl[768];
    int set_silent_n = snprintf(set_silent_dsl, sizeof(set_silent_dsl),
                                "csv batch_size=1 | intersect %s columns=city "
                                "max_lookup_rows=10 max_lookup_keys=10 "
                                "max_output_keys=10 max_state_bytes=1048576 | csv",
                                bag_lookup_path);
    assert(set_silent_n > 0 && (size_t)set_silent_n < sizeof(set_silent_dsl));
    run_case_no_silent_output_oom(
        "set_intersect_key_materialization",
        set_silent_dsl,
        people,
        520);
    set_silent_n = snprintf(set_silent_dsl, sizeof(set_silent_dsl),
                            "csv batch_size=1 | setdiff %s columns=city "
                            "max_lookup_rows=10 max_lookup_keys=10 "
                            "max_output_keys=10 max_state_bytes=1048576 | csv",
                            bag_lookup_path);
    assert(set_silent_n > 0 && (size_t)set_silent_n < sizeof(set_silent_dsl));
    run_case_no_silent_output_oom(
        "set_setdiff_key_materialization",
        set_silent_dsl,
        people,
        520);
    set_silent_n = snprintf(set_silent_dsl, sizeof(set_silent_dsl),
                            "csv batch_size=1 | intersect-all %s columns=city "
                            "max_lookup_rows=10 max_lookup_keys=10 "
                            "max_state_bytes=1048576 | csv",
                            bag_lookup_path);
    assert(set_silent_n > 0 && (size_t)set_silent_n < sizeof(set_silent_dsl));
    run_case_no_silent_output_oom(
        "set_intersect_all_key_materialization",
        set_silent_dsl,
        people,
        520);
    set_silent_n = snprintf(set_silent_dsl, sizeof(set_silent_dsl),
                            "csv batch_size=1 | setdiff-all %s columns=city "
                            "max_lookup_rows=10 max_lookup_keys=10 "
                            "max_state_bytes=1048576 | csv",
                            bag_lookup_path);
    assert(set_silent_n > 0 && (size_t)set_silent_n < sizeof(set_silent_dsl));
    run_case_no_silent_output_oom(
        "set_setdiff_all_key_materialization",
        set_silent_dsl,
        people,
        520);
    set_silent_n = snprintf(set_silent_dsl, sizeof(set_silent_dsl),
                            "csv batch_size=1 | union %s columns=city "
                            "max_lookup_rows=10 max_lookup_bytes=4096 "
                            "max_output_keys=10 max_state_bytes=1048576 | csv",
                            union_lookup_path);
    assert(set_silent_n > 0 && (size_t)set_silent_n < sizeof(set_silent_dsl));
    run_case_no_silent_output_oom(
        "set_union_key_materialization",
        set_silent_dsl,
        people,
        520);
    const char *sorted_union_lookup_path = "/tmp/tranfi_oom_sorted_union_lookup.csv";
    FILE *sorted_union_lookup = fopen(sorted_union_lookup_path, "wb");
    assert(sorted_union_lookup);
    fputs("name,age,city,score,tags,x,y,color\n"
          "Liam,21,LA,19,m,8,8,blue\n"
          "Nora,39,NY,33,n,9,9,red\n"
          "Sam,44,SF,41,s,10,10,green\n",
          sorted_union_lookup);
    assert(fclose(sorted_union_lookup) == 0);
    const char *stack_path = "/tmp/tranfi_oom_stack.csv";
    FILE *stack_file = fopen(stack_path, "wb");
    assert(stack_file);
    fputs("name,age,city,score,tags,x,y,color\n"
          "Stacked,41,SF,40,z,7,8,green\n",
          stack_file);
    assert(fclose(stack_file) == 0);
    const char *rules_path = "/tmp/tranfi_oom_rules.json";
    FILE *rules_file = fopen(rules_path, "wb");
    assert(rules_file);
    fputs("{\"name\":\"quality_file\",\"audit\":true,\"audit_limit\":3,"
          "\"warn_failure_rate\":0.25,\"rules\":["
          "{\"name\":\"score_nonnegative\",\"expr\":\"col('score_math') >= 0\","
          "\"message\":\"score must be nonnegative\"},"
          "{\"name\":\"read_under_90\",\"expr\":\"col('score_read') < 90\"}"
          "]}",
          rules_file);
    assert(fclose(rules_file) == 0);
    const char *spill_root = "/tmp/tranfi_oom_spill_root";
    remove(spill_root);
    if (mkdir(spill_root, 0700) != 0 && errno != EEXIST) {
        assert(!"failed to create OOM spill root");
    }

    const oom_case cases[] = {
        {
            "row_local_audit_select",
            "csv batch_size=1 | filter \"col(age) >= 30\" audit audit_limit=2 | select name,age,city | csv",
            people,
            220
        },
        {
            "row_local_head_rename_split",
            "csv batch_size=1 | rename name=full_name | split full_name \" \" first,last | skip 1 | head 2 | csv",
            people,
            280
        },
        {
            "csv_codec_policies",
            "csv batch_size=1 header=false comment=# skip_empty_rows=true trim_ws=false nulls=NA quoted_nulls=false max_columns=8 n_max=3 | select col1,col2,col3 | csv",
            no_header_rows,
            360
        },
        {
            "dsl_codec_validate_audit_args",
            "csv batch_size=1 audit audit_limit=2 | validate rule=adult:col(age)>=18 audit audit_columns=name,city audit_hash_columns=name max_failure_rate=1 warn_failure_rate=0.5 name=quality_gate message=ok | csv",
            people,
            560
        },
        {
            "dsl_quality_builder_args",
            "csv batch_size=1 | tee \"col(score) >= 10\" channel=stats columns=name,city limit=2 every=1 include_row=false audit_hash_columns=name audit_max_bytes=256 | assert aggregate=missing_rate:score op=<= value=0.5 tolerance=0.01 rel=false action=warn name=score_missing audit audit_columns=name,score audit_redact=score audit_max_cell_bytes=16 | schema columns=name:string,city:string,score:number required=name values=city:NY,LA min=score:0 allow_extra_columns=false mode=annotate result=schema_ok audit audit_include_row=false audit_columns=name,city audit_hash_columns=city max_regex_pattern_bytes=64 max_regex_cell_bytes=128 | quarantine \"col(city) == 'SF'\" name=sf_rows message=blocked audit audit_include_row=false | csv",
            people,
            760
        },
        {
            "dsl_rowlocal_builder_args",
            "csv batch_size=1 | source-name source default=unknown | rename name=full_name,city=town | derive score2=col(score)*2 | across columns=full_name fn=upper replace=false names={col}_{fn} | relocate full_name_upper after=full_name | select source,full_name,full_name_upper,town,score2 | stats count,missing | csv",
            people,
            640
        },
        {
            "dsl_rank_key_builder_args",
            "csv batch_size=1 | sort -score | head 3 | bottom-k 2 age | slice-min score n=2 with_ties=false | slice-max age 2 with_ties=false | unique city,name sorted=false max_keys=8 max_state_bytes=32768 | group-agg city sum:score:total count:*:rows sorted=false max_groups=8 max_state_bytes=32768 | frequency city max_values=2 max_state_bytes=32768 overflow=other other=OTHER audit audit_limit=2 | csv",
            people,
            920
        },
        {
            "dsl_reshape_window_builder_args",
            "csv batch_size=1 | replace --regex name \"^A\" A audit audit_limit=2 | clip score min=0 max=100 | bin score 10 20 30 missing=null on_type_error=null | datetime d extract=year,month missing=null on_type_error=null | explode tags \"|\" max_tokens_per_row=4 max_token_bytes=16 max_output_rows_per_input_row=4 max_output_rows_per_batch=24 | split full \" \" first last | unpivot x y max_output_rows_per_input_row=2 max_output_rows_per_batch=64 | window score 2 sum score_win missing=null on_type_error=null | rolling-any active 2 active_any nulls=propagate | rolling-sum score 2 score_roll missing=null on_type_error=null | step score running-sum score_run | flatten | csv",
            reshape_dates,
            900
        },
        {
            "simple_suffix_constructor_args",
            "csv batch_size=1 | grep -v NOPE name | clip score min=0 max=100 | bin score 15 | lag score 1 | shift score offset=1 type=lead | csv",
            people,
            520
        },
        {
            "generated_suffix_constructor_args",
            "csv batch_size=1 | ewma score 0.5 | anomaly score 2 | diff score | step score running-sum | rolling-sum score 2 | rolling-mean score 2 | rolling-min score 2 | rolling-max score 2 | rolling-any active 2 | label-encode city max_categories=8 | csv",
            people,
            760
        },
        {
            "datetime_group_agg_generated_schema_args",
            "csv batch_size=1 | datetime d extract=year,month missing=null on_type_error=null | group-agg active sum:score max_groups=8 | csv",
            reshape_dates,
            840
        },
        {
            "csv_strict_good_rows",
            "csv batch_size=1 mode=strict max_record_bytes=96 max_columns=16 | csv",
            people,
            320
        },
        {
            "row_selection_aliases",
            "csv batch_size=1 | reorder city,name,score | dedup city max_keys=8 | slice-head n=3 | slice-tail 2 | csv",
            people,
            340
        },
        {
            "row_local_hash",
            "csv batch_size=1 | hash name,city | csv",
            people,
            220
        },
        {
            "selector_across_relocate",
            "csv batch_size=1 | across starts_with(score_) round replace=false names={col}_{fn} | select all_of(id,name),any_of(score_math_round,missing),starts_with(score_) | relocate starts_with(score_) after=name | csv",
            selector_people,
            520
        },
        {
            "derive_cast_replace",
            "csv batch_size=1 nulls=NA | trim name | fill-null city=unknown | cast age=float | clip age min=20 max=40 | replace city LA LosAngeles | derive doubled=col(score)*2 | csv",
            people,
            260
        },
        {
            "cast_fail_policy_valid",
            "csv batch_size=1 | cast age=int on_error=fail audit audit_limit=2 | csv",
            people,
            360
        },
        {
            "regex_replace_audit",
            "csv batch_size=1 | replace --regex name \"A.*e\" X audit audit_limit=2 audit_hash_columns=name | csv",
            people,
            420
        },
        {
            "fill_down_bin",
            "csv batch_size=1 nulls=NA | fill-down city | bin score 15,25 | csv",
            missing_people,
            260
        },
        {
            "bounded_state_time_series",
            "csv batch_size=1 | lag age 1 | lead age 1 | shift age 2 age_next type=lead | rolling-sum age 2 sum2 | rolling-mean score 2 mean2 | rolling-min score 2 min2 | rolling-max score 2 max2 | rolling-any score 2 any_score | ewma score 0.5 | anomaly score 2 | step score running-sum score_run | diff score | csv",
            people,
            520
        },
        {
            "boolean_rolling_aliases",
            "csv batch_size=1 | rolling-all active 2 all_active nulls=false | rolling-any active 2 any_active nulls=propagate | csv",
            selector_people,
            300
        },
        {
            "reshape_expand",
            "csv batch_size=1 | explode tags | unpivot x,y max_output_rows_per_batch=20 | csv",
            people,
            260
        },
        {
            "guarded_reshape_caps",
            "csv batch_size=1 | explode tags \"|\" max_tokens_per_row=4 max_token_bytes=16 max_output_rows_per_input_row=4 max_output_rows_per_batch=24 | unpivot x,y max_output_rows_per_input_row=2 max_output_rows_per_batch=24 | csv",
            people,
            360
        },
        {
            "key_state_ops",
            "csv batch_size=1 | rowid city | unique city max_keys=8 | group-agg city sum:score:total count:*:rows max_groups=8 | csv",
            people,
            320
        },
        {
            "key_state_unique",
            "csv batch_size=1 | unique city max_keys=8 | csv",
            people,
            260
        },
        {
            "key_state_unique_approx",
            "csv batch_size=1 | unique city mode=approx bloom_bytes=4096 bloom_hashes=3 | csv",
            people,
            300
        },
        {
            "key_state_dedup_approx",
            "csv batch_size=1 | dedup city --approx bloom_bytes=4096 bloom_hashes=3 | csv",
            people,
            300
        },
        {
            "category_ops",
            "csv batch_size=1 | frequency city max_values=8 | onehot color max_categories=8 | label-encode city city_id max_categories=8 | csv",
            people,
            320
        },
        {
            "state_byte_caps",
            "csv batch_size=1 | rowid city result=city_row max_state_bytes=4096 | frequency city max_state_bytes=4096 | onehot color max_state_bytes=4096 | label-encode city city_code max_state_bytes=4096 | csv",
            people,
            520
        },
        {
            "category_declared_unknowns",
            "csv batch_size=1 | onehot color categories=red,blue unknown=other | label-encode city city_id categories=LA,NY unknown=null | csv",
            people,
            420
        },
        {
            "data_quality_audit",
            "csv batch_size=1 | validate \"col(age) > 25\" audit audit_limit=2 | assert \"col(score) >= 20\" action=filter audit audit_limit=2 | schema name:string age:int city:string non_null=name,age min=age:0 max=age:120 values=city:NY,LA require_values_seen=true mode=filter audit audit_limit=2 | quarantine \"col(city) == 'LA'\" name=city_block message=la | csv",
            people,
            520
        },
        {
            "validate_rate_warning",
            "csv batch_size=1 | validate \"col(score) > 15\" audit audit_limit=1 warn_failure_rate=0.2 max_failure_rate=1 | csv",
            people,
            520
        },
        {
            "assert_quarantine_action",
            "csv batch_size=1 | assert \"col(score) >= 20\" action=quarantine name=score_min audit audit_limit=2 | csv",
            people,
            520
        },
        {
            "data_quality_annotate_schema",
            "csv batch_size=1 | validate \"col(age) > 0\" | assert \"col(score) >= 20\" action=annotate result=score_ok | schema name:string age:int city:string mode=annotate result=schema_ok | csv",
            people,
            560
        },
        {
            "schema_selector_rules",
            "csv batch_size=1 | schema columns=starts_with(score_):number,code:string non_null=starts_with(score_) min=where(number):0 max=starts_with(score_):100 regex=ends_with(code):^[A-Z]+$ max_regex_pattern_bytes=64 max_regex_cell_bytes=16 mode=warn | csv",
            selector_people,
            520
        },
        {
            "schema_quarantine_action",
            "csv batch_size=1 | schema name:string age:int city:string non_null=name,age min=age:0 max=age:120 values=city:NY,LA mode=quarantine name=schema_check message=schema_rule | csv",
            sorted_people_unmatched,
            560
        },
        {
            "validate_rules_file",
            "csv batch_size=1 | validate rules_file=/tmp/tranfi_oom_rules.json | csv",
            selector_people,
            560
        },
        {
            "tee_audit_selectors",
            "csv batch_size=1 | tee \"col(score_math) >= 80\" channel=audit columns=name,score_math limit=2 audit_columns=name audit_redact=name | csv",
            selector_people,
            420
        },
        {
            "csv_repair_audit",
            "csv batch_size=1 mode=repair max_error_bytes=5 audit audit_limit=1 | csv",
            repair_rows,
            360
        },
        {
            "jsonl_malformed_diagnostics",
            "jsonl batch_size=1 on_error=warn max_error_bytes=8 | csv",
            "{\"name\":\"Alice\",\"age\":30}\nnot-json-record-long\n[1,2]\n{\"name\":\"Bob\",\"age\":25}\n",
            520
        },
        {
            "audit_privacy_producers",
            "csv batch_size=1 nulls=NA | fill-null note=SECRET audit audit_columns=note audit_redact=note | cast secret=int on_error=null audit audit_columns=secret audit_redact=secret | frequency city max_values=1 overflow=other audit audit_limit=1 audit_columns=city audit_redact=city | csv",
            audit_rows,
            620
        },
        {
            "aggregate_assert_grep_passthrough",
            "csv batch_size=1 | assert aggregate=sum:score op=>= value=60 action=warn | grep missing invert=true | csv",
            people,
            420
        },
        {
            "aggregate_assert_tolerance",
            "csv batch_size=1 | assert aggregate=count op=>= value=2 tolerance=0.001 rel=false action=warn name=row_count | assert aggregate=missing_rate:score op=<= value=0.25 action=warn name=score_missing_rate | csv",
            people,
            420
        },
        {
            "metadata_time_ops",
            "csv batch_size=1 | source-name src default=oom | rleid name result=name_run | datetime d year,month | date-trunc ts month result=ts_month | relocate src after=name | csv",
            dates,
            420
        },
        {
            "typed_string_materialization",
            "csv batch_size=1 | cast d=string,ts=string | unpivot d,ts max_output_rows_per_batch=8 | csv",
            dates,
            380
        },
        {
            "json_schema_flatten",
            "text | json-schema required=user types=user:object mode=filter audit audit_limit=1 | json-flatten fields=/user/id:user_id:int,$.user.name:name:string | csv",
            "{\"user\":{\"id\":42,\"name\":\"Ada\"}}\n{\"other\":true}\n{\"user\":{\"id\":7,\"name\":\"Ben\"}}\n",
            420
        },
        {
            "json_schema_annotate",
            "text | json-schema required=user types=user:object mode=annotate result=json_ok | csv",
            "{\"user\":{\"id\":42,\"name\":\"Ada\"}}\n{\"other\":true}\n",
            360
        },
        {
            "dsl_json_builder_args",
            "text | json-extract column=_line path=/user/id result=user_id type=int | json-filter column=_line path=/user/name op=starts-with value=A type=string | json-flatten column=_line fields=/user/id:id:int,/user/name:name:string | json-schema schema=true mode=annotate result=json_ok audit audit_limit=1 audit_include_row=false audit_columns=_line audit_hash_columns=_line audit_max_bytes=256 audit_max_cell_bytes=64 | csv",
            "{\"user\":{\"id\":42,\"name\":\"Ada\"}}\n{\"user\":{\"id\":7,\"name\":\"Ben\"}}\n{\"user\":{\"id\":9,\"name\":\"Alice\"}}\n",
            620
        },
        {
            "dsl_key_sequence_builder_args",
            "csv batch_size=1 | grep -rv NOPE name | rowid city result=city_row sorted=false max_keys=8 max_state_bytes=32768 | rleid city result=city_run | shift age offset=1 result=age_prev type=lag | onehot color --drop categories=red,blue,green unknown=other max_categories=4 max_state_bytes=32768 | label-encode city city_id categories=NY,LA,SF unknown=null max_categories=4 max_state_bytes=32768 | ewma score 0.5 score_ewma missing=null on_type_error=null | anomaly score 2 score_anom missing=null on_type_error=null | diff score 1 score_diff | split-data 0.67 --seed 11 split_id | sample 3 seed=7 | csv",
            people,
            980
        },
        {
            "stack_file",
            "csv batch_size=1 | stack /tmp/tranfi_oom_stack.csv --tag src | csv",
            people,
            460
        },
        {
            "flush_report_stats",
            "csv batch_size=1 | scan | csv",
            people,
            360
        },
        {
            "stats_distinct_hist_sample",
            "csv batch_size=1 | stats distinct,hist,sample | csv",
            people,
            420
        },
        {
            "flush_report_schema_infer",
            "csv batch_size=1 | schema infer rows=3 | csv",
            people,
            320
        },
        {
            "schema_infer_sample_limited",
            "csv batch_size=1 | schema infer rows=1 | csv",
            people,
            320
        },
        {
            "bounded_top_flush",
            "csv batch_size=1 | top 2 score | csv",
            people,
            260
        },
        {
            "bounded_rank_aliases",
            "csv batch_size=1 | top-k n=3 score | bottom-k 3 score | slice-min name n=2 with_ties=false | slice-max score 2 with_ties=false | csv",
            people,
            480
        },
        {
            "blocking_sort",
            "csv batch_size=1 | sort score | csv",
            people,
            260
        },
        {
            "blocking_pivot",
            "csv batch_size=1 | pivot color score sum max_categories=4 | csv",
            people,
            420
        },
        {
            "spill_unique",
            "csv batch_size=1 | unique city spill_dir=/tmp/tranfi_oom_spill_root spill_run_rows=2 spill_output_rows=2 | csv",
            people,
            520
        },
        {
            "spill_group_agg",
            "csv batch_size=1 | group-agg city sum:score:total count:*:rows spill_dir=/tmp/tranfi_oom_spill_root spill_run_rows=2 spill_output_rows=2 | csv",
            people,
            560
        },
        {
            "spill_pivot",
            "csv batch_size=1 | pivot color score sum categories=red,blue max_categories=4 sorted=false spill_dir=/tmp/tranfi_oom_spill_root spill_memory_bytes=4096 spill_run_rows=2 spill_output_rows=2 | csv",
            people,
            640
        },
        {
            "spill_filtering_join",
            "csv batch_size=1 | semi-join /tmp/tranfi_oom_join_lookup.csv on city spill_dir=/tmp/tranfi_oom_spill_root spill_run_rows=2 spill_output_rows=2 | csv",
            people,
            560
        },
        {
            "spill_anti_join",
            "csv batch_size=1 | anti-join /tmp/tranfi_oom_join_lookup.csv on city spill_dir=/tmp/tranfi_oom_spill_root spill_run_rows=2 spill_output_rows=2 | csv",
            people,
            560
        },
        {
            "spill_mutating_join",
            "csv batch_size=1 | join /tmp/tranfi_oom_join_lookup.csv on city max_matches_per_row=2 spill_dir=/tmp/tranfi_oom_spill_root spill_memory_bytes=4096 spill_run_rows=2 spill_output_rows=2 | csv",
            people,
            680
        },
        {
            "hash_mutating_join",
            "csv batch_size=1 | join /tmp/tranfi_oom_join_lookup.csv on city max_lookup_rows=8 max_lookup_keys=8 max_lookup_bytes=4096 max_state_bytes=32768 max_matches_per_row=2 max_output_rows=16 | csv",
            people,
            580
        },
        {
            "hash_filtering_join",
            "csv batch_size=1 | semi-join /tmp/tranfi_oom_join_lookup.csv on city max_lookup_rows=8 max_lookup_keys=8 max_lookup_bytes=4096 | csv",
            people,
            460
        },
        {
            "hash_left_join_nulls",
            "csv batch_size=1 | join /tmp/tranfi_oom_join_lookup.csv on city --left max_lookup_rows=8 max_lookup_keys=8 max_lookup_bytes=4096 max_matches_per_row=2 max_output_rows=16 | csv",
            people,
            560
        },
        {
            "sorted_mutating_join",
            "csv batch_size=1 | join /tmp/tranfi_oom_join_sorted_lookup.csv on city sorted=true max_matches_per_row=2 max_output_rows=16 | csv",
            sorted_people,
            560
        },
        {
            "sorted_left_join_nulls",
            "csv batch_size=1 | join /tmp/tranfi_oom_join_sorted_lookup.csv on city --left sorted=true max_matches_per_row=2 max_output_rows=16 | csv",
            sorted_people_unmatched,
            600
        },
        {
            "sorted_filtering_joins",
            "csv batch_size=1 | semi-join /tmp/tranfi_oom_join_sorted_lookup.csv on city sorted=true | csv",
            sorted_people,
            420
        },
        {
            "sorted_anti_join",
            "csv batch_size=1 | anti-join /tmp/tranfi_oom_join_sorted_lookup.csv on city sorted=true | csv",
            sorted_people_unmatched,
            420
        },
        {
            "hash_set_schema_capture",
            "csv batch_size=1 | intersect /tmp/tranfi_oom_union_lookup.csv columns=city max_lookup_rows=8 max_lookup_keys=8 max_lookup_bytes=4096 max_output_keys=8 max_state_bytes=32768 | csv",
            people,
            420
        },
        {
            "sorted_set_schema_capture",
            "csv batch_size=1 | intersect /tmp/tranfi_oom_sorted_lookup.csv columns=city sorted=true | csv",
            sorted_people,
            340
        },
        {
            "hash_bag_set_modes",
            "csv batch_size=1 | intersect-all /tmp/tranfi_oom_bag_lookup.csv columns=city max_lookup_keys=8 max_lookup_bytes=4096 | setdiff-all /tmp/tranfi_oom_bag_lookup.csv columns=city max_lookup_keys=8 max_lookup_bytes=4096 | csv",
            people,
            420
        },
        {
            "hash_setdiff_mode",
            "csv batch_size=1 | setdiff /tmp/tranfi_oom_union_lookup.csv columns=city max_lookup_keys=8 max_output_keys=8 | csv",
            people,
            340
        },
        {
            "sorted_bag_set_modes",
            "csv batch_size=1 | intersect-all /tmp/tranfi_oom_sorted_lookup.csv columns=city sorted=true | setdiff-all /tmp/tranfi_oom_sorted_lookup.csv columns=city sorted=true | csv",
            sorted_people,
            420
        },
        {
            "sorted_setdiff_mode",
            "csv batch_size=1 | setdiff /tmp/tranfi_oom_sorted_lookup.csv columns=city sorted=true | csv",
            sorted_people,
            340
        },
        {
            "spill_set_schema_capture",
            "csv batch_size=1 | intersect /tmp/tranfi_oom_union_lookup.csv columns=city spill_dir=/tmp/tranfi_oom_spill_root spill_memory_bytes=4096 spill_run_rows=16 spill_output_rows=16 | csv",
            people,
            420
        },
        {
            "spill_setdiff_schema_capture",
            "csv batch_size=1 | setdiff /tmp/tranfi_oom_union_lookup.csv columns=city spill_dir=/tmp/tranfi_oom_spill_root spill_run_rows=16 spill_output_rows=16 | csv",
            people,
            380
        },
        {
            "spill_bag_set_schema_capture",
            "csv batch_size=1 | intersect-all /tmp/tranfi_oom_bag_lookup.csv columns=city spill_dir=/tmp/tranfi_oom_spill_root spill_run_rows=16 spill_output_rows=16 | csv",
            people,
            420
        },
        {
            "spill_bag_setdiff_schema_capture",
            "csv batch_size=1 | setdiff-all /tmp/tranfi_oom_bag_lookup.csv columns=city spill_dir=/tmp/tranfi_oom_spill_root spill_run_rows=16 spill_output_rows=16 | csv",
            people,
            420
        },
        {
            "union_schema_capture",
            "csv batch_size=1 | union /tmp/tranfi_oom_union_lookup.csv columns=city max_output_keys=8 | csv",
            people,
            320
        },
        {
            "union_all_append",
            "csv batch_size=1 | union-all /tmp/tranfi_oom_union_lookup.csv | csv",
            people,
            360
        },
        {
            "sorted_union_schema_capture",
            "csv batch_size=1 | union /tmp/tranfi_oom_sorted_union_lookup.csv columns=city sorted=true | csv",
            sorted_people,
            420
        },
        {
            "spill_union_schema_capture",
            "csv batch_size=1 | union /tmp/tranfi_oom_union_lookup.csv columns=city spill_dir=/tmp/tranfi_oom_spill_root spill_run_rows=16 spill_output_rows=16 | csv",
            people,
            380
        },
        {
            "sorted_key_state_modes",
            "csv batch_size=1 | unique city sorted=true | group-agg city sum:score:total sorted=true | csv",
            sorted_people,
            360
        },
        {
            "rowid_group_modes",
            "csv batch_size=1 | rowid city result=city_row max_state_bytes=2048 | rowid city result=city_run sorted=true | csv",
            sorted_people,
            360
        },
        {
            "json_record_ops",
            "text | json-extract /user/name user_name | json-extract /user/age user_age type=int | json-filter /user/age >= 30 type=float | csv",
            "{\"user\":{\"name\":\"Alice\",\"age\":30}}\n{\"user\":{\"name\":\"Bob\",\"age\":20}}\n",
            300
        },
        {
            "jsonpath_dot_record_ops",
            "text | json-extract $.user.name user_name | json-filter $.user.age >= 30 type=float | csv",
            "{\"user\":{\"name\":\"Alice\",\"age\":30}}\n{\"user\":{\"name\":\"Bob\",\"age\":20}}\n",
            300
        },
        {
            "json_filter_predicates",
            "text | json-filter /user/name starts-with A type=string | json-filter /tags contains vip type=string | csv",
            "{\"user\":{\"name\":\"Alice\"},\"tags\":\"vip,gold\"}\n{\"user\":{\"name\":\"Ada\"},\"tags\":\"basic\"}\n{\"user\":{\"name\":\"Bob\"},\"tags\":\"vip\"}\n",
            360
        },
        {
            "jsonl_decode_encode",
            "jsonl batch_size=4 | jsonl",
            "{\"name\":\"Alice\",\"age\":30,\"payload\":{\"city\":\"NY\"}}\n{\"name\":\"Bob\",\"age\":25.5,\"payload\":{\"city\":\"LA\"}}\n{\"name\":\"Cara\",\"age\":\"35\",\"payload\":[\"x\",\"y\"]}\n",
            360
        },
        {
            "text_grep",
            "text | grep -r error | head 3 | text",
            "ok\nerror one\nwarn\nerror two\n",
            180
        },
        {
            "text_replace_regex",
            "text batch_size=1 | replace --regex _line \"error .*\" ERR | text",
            "ok\nerror one\nwarn\nerror two\n",
            300
        },
        {
            "text_passthrough",
            "text batch_size=2 | text",
            "alpha\nbeta\ngamma\n",
            220
        },
        {
            "text_no_newline_with_record_cap",
            "text batch_size=1 max_record_bytes=64 | grep alpha | text",
            "alpha beta gamma",
            220
        },
        {
            "table_encode",
            "csv batch_size=1 | table max_rows=3 max_width=12",
            people,
            320
        },
        {
            "flush_latent_blocking",
            "csv batch_size=1 | top 2 score | tail 2 | sample 2 seed=1 | normalize score | acf score 2 | csv",
            people,
            360
        },
        {
            "blocking_data_prep",
            "csv batch_size=1 | interpolate x linear | normalize x audit audit_limit=2 | acf y 2 | csv",
            series,
            520
        },
        {
            "data_prep_policy_modes",
            "csv batch_size=1 | interpolate missing forward missing=null | interpolate name forward on_type_error=null | normalize missing2 missing=null | normalize name on_type_error=null | csv",
            people,
            620
        },
        {
            "normalize_zscore_audit",
            "csv batch_size=1 | normalize score zscore audit audit_limit=2 audit_columns=name,score audit_hash_columns=score | csv",
            people,
            520
        },
        {
            "split_data_seeded",
            "csv batch_size=1 | split-data 0.5 result=fold seed=123 | csv",
            people,
            320
        }
    };

    const oom_sql_case sql_cases[] = {
        {
            "sql_filter_derive_sort_head",
            "csv | filter \"col('age') > 20\" | derive total=col('score')+col('age') | select name,total | sort total | head 2 | csv",
            1,
            260
        },
        {
            "sql_grep_trim_literals",
            "csv | grep % name | trim name,city | csv",
            1,
            260
        },
        {
            "sql_frequency_multi",
            "csv | frequency city,color | csv",
            1,
            260
        },
        {
            "sql_join_rowid_window",
            "csv | join /tmp/tranfi_oom_join_lookup.csv on city --left | rowid city | rolling-mean score 2 mean2 | csv",
            1,
            360
        },
        {
            "sql_unique_group",
            "csv | unique city | group-agg city sum:score:total count:*:rows | csv",
            1,
            300
        },
        {
            "sql_reject_sample",
            "csv | sample 2 seed=42 | csv",
            0,
            180
        },
        {
            "sql_reject_stats",
            "csv | stats count,missing,complete_rate | csv",
            0,
            180
        },
        {
            "sql_reject_scan",
            "csv | scan | csv",
            0,
            180
        },
        {
            "sql_reject_trim_all",
            "csv | trim | csv",
            0,
            180
        }
    };

    printf("Tranfi OOM Fault-Injection Tests\n");
    printf("================================\n");
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        printf("  %-32s", cases[i].name);
        fflush(stdout);
        run_case_with_oom(&cases[i]);
        printf("PASS\n");
    }
    for (size_t i = 0; i < sizeof(sql_cases) / sizeof(sql_cases[0]); i++) {
        printf("  %-32s", sql_cases[i].name);
        fflush(stdout);
        run_sql_case_with_oom(&sql_cases[i]);
        printf("PASS\n");
    }
    remove(union_lookup_path);
    remove(sorted_lookup_path);
    remove(join_lookup_path);
    remove(join_sorted_lookup_path);
    remove(bag_lookup_path);
    remove(sorted_union_lookup_path);
    remove(stack_path);
    remove(rules_path);
    remove(spill_root);
    return 0;
}
