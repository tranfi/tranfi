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
#include "internal.h"
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

static int wasm_validate_buffer(const void *ptr, int len, const char *what) {
    if (len < 0) {
        tf_set_last_error(what ? what : "invalid WASM buffer length");
        return 0;
    }
    if (len > 0 && !ptr) {
        tf_set_last_error(what ? what : "invalid WASM buffer pointer");
        return 0;
    }
    return 1;
}

#ifdef __EMSCRIPTEN__
#define EXPORT EMSCRIPTEN_KEEPALIVE
#else
#define EXPORT
#endif

static int wasm_pipeline_create_policy(const char *json, int len,
                                       int allow_fs, int allow_spill,
                                       int allow_rules_file, const char *workspace_root) {
    if (!wasm_validate_buffer(json, len, "invalid WASM plan buffer")) return -1;
    tf_host_policy policy = {0};
    policy.allow_fs = allow_fs != 0;
    policy.allow_spill = allow_spill != 0;
    policy.allow_rules_file = allow_rules_file != 0;
    policy.allow_blocking = true;
    policy.workspace_root = workspace_root && workspace_root[0] ? workspace_root : NULL;
    tf_pipeline *p = tf_pipeline_create_with_host_policy(json, (size_t)len, &policy);
    if (!p) return -1;
    int h = alloc_handle(p);
    if (h < 0) {
        tf_pipeline_free(p);
        return -1;
    }
    return h;
}

EXPORT
int wasm_pipeline_create(const char *json, int len) {
    return wasm_pipeline_create_policy(json, len, 0, 0, 0, NULL);
}

EXPORT
int wasm_pipeline_create_with_policy(const char *json, int len, int allow_fs,
                                     int allow_spill, int allow_rules_file,
                                     const char *workspace_root) {
    return wasm_pipeline_create_policy(json, len, allow_fs, allow_spill,
                                       allow_rules_file, workspace_root);
}

EXPORT
int wasm_pipeline_push(int handle, const uint8_t *data, int len) {
    tf_pipeline *p = get_handle(handle);
    if (!p) return -1;
    if (!wasm_validate_buffer(data, len, "invalid WASM input buffer")) return -1;
    return tf_pipeline_push(p, data, (size_t)len);
}

EXPORT
int wasm_pipeline_flush_input(int handle) {
    tf_pipeline *p = get_handle(handle);
    if (!p) return -1;
    return tf_pipeline_flush_input(p);
}

EXPORT
int wasm_pipeline_set_source_name(int handle, const char *name) {
    tf_pipeline *p = get_handle(handle);
    if (!p) return -1;
    return tf_pipeline_set_source_name(p, name);
}

EXPORT
int wasm_pipeline_finish(int handle) {
    tf_pipeline *p = get_handle(handle);
    if (!p) return -1;
    return tf_pipeline_finish(p);
}

EXPORT
int wasm_pipeline_finish_step(int handle) {
    tf_pipeline *p = get_handle(handle);
    if (!p) return -1;
    return tf_pipeline_finish_step(p);
}

EXPORT
int wasm_pipeline_pull(int handle, int channel, uint8_t *buf, int buf_len) {
    tf_pipeline *p = get_handle(handle);
    if (!p) return 0;
    if (!wasm_validate_buffer(buf, buf_len, "invalid WASM output buffer")) return -1;
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
    if (!wasm_validate_buffer(dsl, len, "invalid WASM DSL buffer")) return NULL;
    char *error = NULL;
    char *json = tf_compile_dsl(dsl, (size_t)len, &error);
    if (!json) {
        tf_set_last_error(error ? error : "DSL compile failed");
        free(error);
        return NULL;
    }
    return json;
}

/*
 * wasm_compile_to_sql — compile DSL to SQL query string.
 *
 * Returns heap-allocated SQL string (caller must free), or NULL on error.
 */
EXPORT
char *wasm_compile_to_sql(const char *dsl, int len) {
    if (!wasm_validate_buffer(dsl, len, "invalid WASM DSL buffer")) return NULL;
    char *error = NULL;
    char *sql = tf_compile_to_sql(dsl, (size_t)len, &error);
    if (!sql) {
        tf_set_last_error(error ? error : "SQL compile failed");
        free(error);
        return NULL;
    }
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
