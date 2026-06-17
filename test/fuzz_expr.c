/*
 * fuzz_expr.c -- libFuzzer harness for Tranfi expressions.
 *
 * Parses arbitrary bytes as an expression and, when parsing succeeds,
 * evaluates it against a small typed batch. Parse or evaluation failure is fine;
 * crashes, sanitizer findings, and leaks are not.
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

static tf_batch *make_batch(void) {
    tf_batch *b = tf_batch_create(8, 1);
    if (!b) return NULL;
    if (tf_batch_set_schema(b, 0, "id", TF_TYPE_INT64) != TF_OK ||
        tf_batch_set_schema(b, 1, "age", TF_TYPE_INT64) != TF_OK ||
        tf_batch_set_schema(b, 2, "score", TF_TYPE_FLOAT64) != TF_OK ||
        tf_batch_set_schema(b, 3, "name", TF_TYPE_STRING) != TF_OK ||
        tf_batch_set_schema(b, 4, "city", TF_TYPE_STRING) != TF_OK ||
        tf_batch_set_schema(b, 5, "flag", TF_TYPE_BOOL) != TF_OK ||
        tf_batch_set_schema(b, 6, "date", TF_TYPE_DATE) != TF_OK ||
        tf_batch_set_schema(b, 7, "ts", TF_TYPE_TIMESTAMP) != TF_OK) {
        tf_batch_free(b);
        return NULL;
    }
    if (tf_batch_set_int64(b, 0, 0, 1) != TF_OK ||
        tf_batch_set_int64(b, 0, 1, 42) != TF_OK ||
        tf_batch_set_float64(b, 0, 2, 3.5) != TF_OK ||
        tf_batch_set_string(b, 0, 3, "alice") != TF_OK ||
        tf_batch_set_string(b, 0, 4, "rome") != TF_OK ||
        tf_batch_set_bool(b, 0, 5, true) != TF_OK ||
        tf_batch_set_date(b, 0, 6, 19723) != TF_OK ||
        tf_batch_set_timestamp(b, 0, 7, 1700000000000000LL) != TF_OK) {
        tf_batch_free(b);
        return NULL;
    }
    b->n_rows = 1;
    return b;
}

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size > 8192) return 0;
    char *text = fuzz_string(data, size);
    if (!text) return 0;

    tf_expr *expr = tf_expr_parse(text);
    if (expr) {
        tf_batch *batch = make_batch();
        if (batch) {
            bool truth = false;
            tf_eval_result value;
            memset(&value, 0, sizeof(value));
            (void)tf_expr_eval(expr, batch, 0, &truth);
            (void)tf_expr_eval_val(expr, batch, 0, &value);
            tf_batch_free(batch);
        }
        tf_expr_free(expr);
    }

    free(text);
    return 0;
}
