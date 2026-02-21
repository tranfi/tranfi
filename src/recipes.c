/*
 * recipes.c — Built-in named recipes (20 common ETL pipelines).
 */

#include "recipes.h"
#include <string.h>
#include <ctype.h>

typedef struct {
    const char *name;
    const char *dsl;
    const char *description;
} recipe_entry;

static const recipe_entry recipes[] = {
    /* ---- Data Exploration ---- */
    {"profile",     "csv | stats | csv",
     "Full data profiling (all statistics per column)"},
    {"preview",     "csv | head 10 | csv",
     "Quick preview of first 10 rows"},
    {"schema",      "csv | head 0 | csv",
     "Show column names only"},
    {"summary",     "csv | stats count,min,max,avg,stddev | csv",
     "Summary statistics"},
    {"count",       "csv | stats count | csv",
     "Row count per column"},
    {"cardinality", "csv | stats count,distinct | csv",
     "Unique value counts per column"},
    {"distro",      "csv | stats min,p25,median,p75,max | csv",
     "Five-number summary (quartiles)"},

    /* ---- Data Quality ---- */
    {"freq",        "csv | frequency | csv",
     "Value frequency distribution"},
    {"dedup",       "csv | dedup | csv",
     "Remove duplicate rows"},
    {"clean",       "csv | trim | csv",
     "Trim whitespace from all columns"},

    /* ---- Data Sampling ---- */
    {"sample",      "csv | sample 100 | csv",
     "Random sample of 100 rows"},
    {"head",        "csv | head 20 | csv",
     "First 20 rows"},
    {"tail",        "csv | tail 20 | csv",
     "Last 20 rows"},

    /* ---- Format Conversion ---- */
    {"csv2json",    "csv | jsonl",
     "Convert CSV to JSONL"},
    {"json2csv",    "jsonl | csv",
     "Convert JSONL to CSV"},
    {"tsv2csv",     "csv delimiter=\"\t\" | csv",
     "Convert TSV to CSV"},
    {"csv2tsv",     "csv | csv delimiter=\"\t\"",
     "Convert CSV to TSV"},

    /* ---- Display ---- */
    {"look",        "csv | table",
     "Pretty-print as Markdown table"},

    /* ---- Analysis ---- */
    {"histogram",   "csv | stats hist | csv",
     "Distribution histograms"},
    {"hash",        "csv | hash | csv",
     "Add row hash column for change detection"},
    {"samples",     "csv | stats sample | csv",
     "Show sample values per column"},
};

#define RECIPE_COUNT (sizeof(recipes) / sizeof(recipes[0]))

size_t tf_recipe_count(void) {
    return RECIPE_COUNT;
}

const char *tf_recipe_name(size_t index) {
    return index < RECIPE_COUNT ? recipes[index].name : NULL;
}

const char *tf_recipe_dsl(size_t index) {
    return index < RECIPE_COUNT ? recipes[index].dsl : NULL;
}

const char *tf_recipe_description(size_t index) {
    return index < RECIPE_COUNT ? recipes[index].description : NULL;
}

const char *tf_recipe_find_dsl(const char *name) {
    if (!name) return NULL;
    for (size_t i = 0; i < RECIPE_COUNT; i++) {
        /* Case-insensitive comparison */
        const char *a = name;
        const char *b = recipes[i].name;
        while (*a && *b && tolower((unsigned char)*a) == tolower((unsigned char)*b)) {
            a++;
            b++;
        }
        if (*a == '\0' && *b == '\0') return recipes[i].dsl;
    }
    return NULL;
}
