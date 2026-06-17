/*
 * fuzz_jsonpath.c -- libFuzzer harness for JSON Pointer/simple JSONPath parsing.
 */

#include "internal.h"
#include "cJSON.h"
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
    char *path = fuzz_string(data, size);
    if (!path) return 0;

    if (tf_json_path_validate(path) == TF_OK) {
        cJSON *root = cJSON_Parse(
            "{\"id\":1,\"user\":{\"name\":\"alice\",\"scores\":[1,2,3]},"
            "\"items\":[{\"city\":\"rome\"},{\"city\":\"paris\"}]}"
        );
        if (root) {
            (void)tf_json_path_resolve(root, path);
            cJSON_Delete(root);
        }
    }

    free(path);
    return 0;
}
