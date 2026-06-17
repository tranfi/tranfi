/*
 * fuzz_selector.c -- libFuzzer harness for column selector parsing/resolution.
 */

#include "internal.h"
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

static char *fuzz_string(const uint8_t *data, size_t size) {
    char *s = (char *)malloc(size + 1);
    if (!s) return NULL;
    memcpy(s, data, size);
    s[size] = 0;
    return s;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size > 8192) return 0;
    char *text = fuzz_string(data, size);
    if (!text) return 0;

    char *names[] = {"id", "name", "age", "score", "city", "flag", "date", "ts"};
    tf_type types[] = {
        TF_TYPE_INT64, TF_TYPE_STRING, TF_TYPE_INT64, TF_TYPE_FLOAT64,
        TF_TYPE_STRING, TF_TYPE_BOOL, TF_TYPE_DATE, TF_TYPE_TIMESTAMP
    };
    char *selectors[] = {text};
    int *indices = NULL;
    size_t n_indices = 0;
    char *error = NULL;

    (void)tf_column_selector_has_syntax(text);
    (void)tf_column_selectors_resolve(selectors, 1, names, types, 8,
                                      &indices, &n_indices, &error);

    free(indices);
    free(error);
    free(text);
    return 0;
}
