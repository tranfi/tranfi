/*
 * fuzz_csv.c -- libFuzzer harness for the CSV decoder.
 *
 * Feeds arbitrary bytes through a CSV decode -> CSV encode pipeline.
 * Must never crash, never read out of bounds, never leak.
 *
 * Build:
 *   clang -std=c11 -g -O1 -fsanitize=fuzzer,address,undefined \
 *     -D_POSIX_C_SOURCE=200809L -I src \
 *     test/fuzz_csv.c src/*.c -lm -o build/fuzz_csv
 *
 * Run:
 *   mkdir -p corpus/csv
 *   ./build/fuzz_csv corpus/csv -max_len=4096 -timeout=5
 */

#include "tranfi.h"
#include <stdint.h>
#include <stddef.h>
#include <string.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    /* Build a simple CSV passthrough pipeline via JSON config */
    const char *config =
        "{\"steps\":["
        "{\"type\":\"codec\",\"codec\":\"csv\",\"mode\":\"decode\"},"
        "{\"type\":\"codec\",\"codec\":\"csv\",\"mode\":\"encode\"}"
        "]}";

    tf_pipeline *p = tf_pipeline_create(config, strlen(config));
    if (!p) return 0;

    /* Push fuzzed data -- may return TF_OK or TF_ERROR, both are fine */
    tf_pipeline_push(p, data, size);
    tf_pipeline_finish(p);

    /* Pull all output and discard */
    uint8_t buf[8192];
    while (tf_pipeline_pull(p, TF_CHAN_MAIN, buf, sizeof(buf)) > 0) {}

    tf_pipeline_free(p);
    return 0;
}
