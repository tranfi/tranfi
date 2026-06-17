/*
 * fuzz_dsl.c -- libFuzzer harness for the Tranfi DSL compiler.
 */

#include "tranfi.h"
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size > 8192) return 0;
    char *dsl = (char *)malloc(size + 1);
    if (!dsl) return 0;
    memcpy(dsl, data, size);
    dsl[size] = 0;

    char *error = NULL;
    char *json = tf_compile_dsl(dsl, strlen(dsl), &error);
    tf_string_free(json);
    free(error);
    free(dsl);
    return 0;
}
