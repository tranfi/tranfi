/*
 * wasm_api.c — Emscripten WASM exports for Tranfi.
 *
 * Handle-based API: JS gets integer handles instead of raw pointers.
 * Same pattern as statsim/compiler.
 */

#ifdef __EMSCRIPTEN__
#include <emscripten.h>
#endif

#include "tranfi.h"
#include "dsl.h"
#include "recipes.h"
#include <stdlib.h>
#include <string.h>

/* Handle map: slot index → tf_pipeline* */
#define MAX_HANDLES 256
static tf_pipeline *handles[MAX_HANDLES] = {0};

static int alloc_handle(tf_pipeline *p) {
    for (int i = 1; i < MAX_HANDLES; i++) {
        if (!handles[i]) {
            handles[i] = p;
            return i;
        }
    }
    return -1; /* no free slots */
}

static tf_pipeline *get_handle(int h) {
    if (h <= 0 || h >= MAX_HANDLES) return NULL;
    return handles[h];
}

#ifdef __EMSCRIPTEN__
#define EXPORT EMSCRIPTEN_KEEPALIVE
#else
#define EXPORT
#endif

EXPORT
int wasm_pipeline_create(const char *json, int len) {
    tf_pipeline *p = tf_pipeline_create(json, (size_t)len);
    if (!p) return -1;
    int h = alloc_handle(p);
    if (h < 0) {
        tf_pipeline_free(p);
        return -1;
    }
    return h;
}

EXPORT
int wasm_pipeline_push(int handle, const uint8_t *data, int len) {
    tf_pipeline *p = get_handle(handle);
    if (!p) return -1;
    return tf_pipeline_push(p, data, (size_t)len);
}

EXPORT
int wasm_pipeline_finish(int handle) {
    tf_pipeline *p = get_handle(handle);
    if (!p) return -1;
    return tf_pipeline_finish(p);
}

EXPORT
int wasm_pipeline_pull(int handle, int channel, uint8_t *buf, int buf_len) {
    tf_pipeline *p = get_handle(handle);
    if (!p) return 0;
    return (int)tf_pipeline_pull(p, channel, buf, (size_t)buf_len);
}

EXPORT
const char *wasm_pipeline_error(int handle) {
    tf_pipeline *p = get_handle(handle);
    if (!p) return tf_last_error();
    const char *err = tf_pipeline_error(p);
    return err ? err : tf_last_error();
}

EXPORT
void wasm_pipeline_free(int handle) {
    tf_pipeline *p = get_handle(handle);
    if (p) {
        tf_pipeline_free(p);
        handles[handle] = NULL;
    }
}

/*
 * wasm_compile_dsl — parse DSL string, return plan JSON.
 *
 * Returns heap-allocated JSON string (caller must free), or NULL on error.
 * On error, wasm_pipeline_error(-1) returns the message.
 */
EXPORT
char *wasm_compile_dsl(const char *dsl, int len) {
    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, (size_t)len, &error);
    if (!plan) {
        /* Store error so JS can retrieve it via wasm_pipeline_error(-1) */
        (void)error; /* error accessible via tf_last_error() */
        free(error);
        return NULL;
    }
    char *json = tf_ir_plan_to_json(plan);
    tf_ir_plan_destroy(plan);
    return json;
}

/*
 * wasm_compile_to_sql — compile DSL to SQL query string.
 *
 * Returns heap-allocated SQL string (caller must free), or NULL on error.
 */
EXPORT
char *wasm_compile_to_sql(const char *dsl, int len) {
    char *error = NULL;
    char *sql = tf_compile_to_sql(dsl, (size_t)len, &error);
    free(error);
    return sql;
}

EXPORT
const char *wasm_version(void) {
    return tf_version();
}

/* ---- Recipe API ---- */

EXPORT
int wasm_recipe_count(void) {
    return (int)tf_recipe_count();
}

EXPORT
const char *wasm_recipe_name(int index) {
    return tf_recipe_name((size_t)index);
}

EXPORT
const char *wasm_recipe_dsl(int index) {
    return tf_recipe_dsl((size_t)index);
}

EXPORT
const char *wasm_recipe_description(int index) {
    return tf_recipe_description((size_t)index);
}

EXPORT
const char *wasm_recipe_find_dsl(const char *name) {
    return tf_recipe_find_dsl(name);
}
