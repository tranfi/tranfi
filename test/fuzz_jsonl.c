/*
 * fuzz_jsonl.c -- libFuzzer harness for the JSONL decoder/encoder pipeline.
 */

#include "tranfi.h"
#include <stdint.h>
#include <stddef.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    const char *config =
        "{\"steps\":["
        "{\"type\":\"codec\",\"codec\":\"jsonl\",\"mode\":\"decode\"},"
        "{\"type\":\"codec\",\"codec\":\"jsonl\",\"mode\":\"encode\"}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(config, strlen(config));
    if (!p) return 0;

    (void)tf_pipeline_push(p, data, size);
    (void)tf_pipeline_finish(p);

    uint8_t buf[8192];
    while (tf_pipeline_pull(p, TF_CHAN_MAIN, buf, sizeof(buf)) > 0) {}
    while (tf_pipeline_pull(p, TF_CHAN_ERRORS, buf, sizeof(buf)) > 0) {}
    while (tf_pipeline_pull(p, TF_CHAN_STATS, buf, sizeof(buf)) > 0) {}

    tf_pipeline_free(p);
    return 0;
}
