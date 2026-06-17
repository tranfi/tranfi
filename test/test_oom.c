/*
 * test_oom.c -- allocator-failure regression tests for Tranfi.
 *
 * This binary is linked with GNU ld --wrap hooks for malloc/calloc/realloc/
 * strdup/strndup. It fails one allocation at a time while compiling and running
 * representative pipelines. Under ASan/UBSan this catches invalid row counters,
 * use-after-free, double-free, and unchecked allocation/write paths.
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
    const char *stack_path = "/tmp/tranfi_oom_stack.csv";
    FILE *stack_file = fopen(stack_path, "wb");
    assert(stack_file);
    fputs("name,age,city,score,tags,x,y,color\n"
          "Stacked,41,SF,40,z,7,8,green\n",
          stack_file);
    assert(fclose(stack_file) == 0);
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
            "row_local_hash",
            "csv batch_size=1 | hash name,city | csv",
            people,
            220
        },
        {
            "derive_cast_replace",
            "csv batch_size=1 nulls=NA | trim name | fill-null city=unknown | cast age=float | clip age min=20 max=40 | replace city LA LosAngeles | derive doubled=col(score)*2 | csv",
            people,
            260
        },
        {
            "fill_down_bin",
            "csv batch_size=1 nulls=NA | fill-down city | bin score 15,25 | csv",
            missing_people,
            260
        },
        {
            "bounded_state_time_series",
            "csv batch_size=1 | lag age 1 | lead age 1 | rolling-sum age 2 sum2 | rolling-mean score 2 mean2 | rolling-any score 2 any_score | ewma score 0.5 | anomaly score 2 | step score running-sum score_run | diff score | csv",
            people,
            360
        },
        {
            "reshape_expand",
            "csv batch_size=1 | explode tags | unpivot x,y max_output_rows_per_batch=20 | csv",
            people,
            260
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
            "category_ops",
            "csv batch_size=1 | frequency city max_values=8 | onehot color max_categories=8 | label-encode city city_id max_categories=8 | csv",
            people,
            320
        },
        {
            "data_quality_audit",
            "csv batch_size=1 | validate \"col(age) > 25\" audit audit_limit=2 | assert \"col(score) >= 20\" action=filter audit audit_limit=2 | schema name:string age:int city:string non_null=name,age min=age:0 max=age:120 values=city:NY,LA mode=filter audit audit_limit=2 | quarantine \"col(city) == 'LA'\" name=city_block message=la | csv",
            people,
            520
        },
        {
            "aggregate_assert_grep_passthrough",
            "csv batch_size=1 | assert aggregate=sum:score op=>= value=60 action=warn | grep missing invert=true | csv",
            people,
            420
        },
        {
            "metadata_time_ops",
            "csv batch_size=1 | source-name src default=oom | datetime d year,month | date-trunc ts month result=ts_month | relocate src after=name | csv",
            dates,
            360
        },
        {
            "json_schema_flatten",
            "text | json-schema required=user types=user:object mode=filter audit audit_limit=1 | json-flatten fields=/user/id:user_id:int,$.user.name:name:string | csv",
            "{\"user\":{\"id\":42,\"name\":\"Ada\"}}\n{\"other\":true}\n{\"user\":{\"id\":7,\"name\":\"Ben\"}}\n",
            420
        },
        {
            "stack_file",
            "csv batch_size=1 | stack /tmp/tranfi_oom_stack.csv --tag src | csv",
            people,
            420
        },
        {
            "flush_report_stats",
            "csv batch_size=1 | scan | csv",
            people,
            360
        },
        {
            "flush_report_schema_infer",
            "csv batch_size=1 | schema infer rows=3 | csv",
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
            "csv batch_size=1 | pivot color score sum max_categories=4 spill_dir=/tmp/tranfi_oom_spill_root spill_run_rows=2 spill_output_rows=2 | csv",
            people,
            560
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
            "csv batch_size=1 | join /tmp/tranfi_oom_join_lookup.csv on city max_matches_per_row=2 spill_dir=/tmp/tranfi_oom_spill_root spill_run_rows=2 spill_output_rows=2 | csv",
            people,
            620
        },
        {
            "hash_set_schema_capture",
            "csv batch_size=1 | intersect /tmp/tranfi_oom_union_lookup.csv columns=city max_lookup_keys=8 max_output_keys=8 | csv",
            people,
            340
        },
        {
            "sorted_set_schema_capture",
            "csv batch_size=1 | intersect /tmp/tranfi_oom_sorted_lookup.csv columns=city sorted=true | csv",
            sorted_people,
            340
        },
        {
            "spill_set_schema_capture",
            "csv batch_size=1 | intersect /tmp/tranfi_oom_union_lookup.csv columns=city spill_dir=/tmp/tranfi_oom_spill_root spill_run_rows=16 spill_output_rows=16 | csv",
            people,
            380
        },
        {
            "union_schema_capture",
            "csv batch_size=1 | union /tmp/tranfi_oom_union_lookup.csv columns=city max_output_keys=8 | csv",
            people,
            320
        },
        {
            "spill_union_schema_capture",
            "csv batch_size=1 | union /tmp/tranfi_oom_union_lookup.csv columns=city spill_dir=/tmp/tranfi_oom_spill_root spill_run_rows=16 spill_output_rows=16 | csv",
            people,
            380
        },
        {
            "json_record_ops",
            "text | json-extract /user/name user_name | json-extract /user/age user_age type=int | json-filter /user/age >= 30 type=float | csv",
            "{\"user\":{\"name\":\"Alice\",\"age\":30}}\n{\"user\":{\"name\":\"Bob\",\"age\":20}}\n",
            300
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
            "text_passthrough",
            "text batch_size=2 | text",
            "alpha\nbeta\ngamma\n",
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
    remove(union_lookup_path);
    remove(sorted_lookup_path);
    remove(join_lookup_path);
    remove(stack_path);
    remove(spill_root);
    return 0;
}
