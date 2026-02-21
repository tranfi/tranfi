/*
 * napi_api.c — Node.js N-API addon for Tranfi.
 *
 * Exposes the C pipeline API as native Node functions.
 */

#include <node_api.h>
#include "tranfi.h"
#include "dsl.h"
#include "recipes.h"
#include <stdlib.h>
#include <string.h>

#define NAPI_CALL(env, call)                                    \
    do {                                                        \
        napi_status status = (call);                            \
        if (status != napi_ok) {                                \
            napi_throw_error(env, NULL, "N-API call failed");   \
            return NULL;                                        \
        }                                                       \
    } while (0)

/*ాcreate_pipeline(planJson: string) → External */
static napi_value napi_create_pipeline(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    if (argc < 1) {
        napi_throw_error(env, NULL, "createPipeline requires plan JSON string");
        return NULL;
    }

    size_t str_len;
    NAPI_CALL(env, napi_get_value_string_utf8(env, argv[0], NULL, 0, &str_len));
    char *json = malloc(str_len + 1);
    NAPI_CALL(env, napi_get_value_string_utf8(env, argv[0], json, str_len + 1, &str_len));

    tf_pipeline *p = tf_pipeline_create(json, str_len);
    free(json);

    if (!p) {
        const char *err = tf_last_error();
        napi_throw_error(env, NULL, err ? err : "failed to create pipeline");
        return NULL;
    }

    napi_value external;
    NAPI_CALL(env, napi_create_external(env, p, NULL, NULL, &external));
    return external;
}

/* push(pipeline: External, data: Buffer) → void */
static napi_value napi_push(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    tf_pipeline *p;
    NAPI_CALL(env, napi_get_value_external(env, argv[0], (void **)&p));

    uint8_t *data;
    size_t len;
    NAPI_CALL(env, napi_get_buffer_info(env, argv[1], (void **)&data, &len));

    int rc = tf_pipeline_push(p, data, len);
    if (rc != 0) {
        const char *err = tf_pipeline_error(p);
        napi_throw_error(env, NULL, err ? err : "push failed");
    }
    return NULL;
}

/* finish(pipeline: External) → void */
static napi_value napi_finish(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    tf_pipeline *p;
    NAPI_CALL(env, napi_get_value_external(env, argv[0], (void **)&p));

    int rc = tf_pipeline_finish(p);
    if (rc != 0) {
        const char *err = tf_pipeline_error(p);
        napi_throw_error(env, NULL, err ? err : "finish failed");
    }
    return NULL;
}

/* pull(pipeline: External, channel: number) → Buffer */
static napi_value napi_pull(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value argv[2];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    tf_pipeline *p;
    NAPI_CALL(env, napi_get_value_external(env, argv[0], (void **)&p));

    int32_t channel;
    NAPI_CALL(env, napi_get_value_int32(env, argv[1], &channel));

    /* Pull all available data */
    size_t total = 0;
    size_t cap = 65536;
    uint8_t *result = malloc(cap);
    uint8_t buf[65536];

    for (;;) {
        size_t n = tf_pipeline_pull(p, channel, buf, sizeof(buf));
        if (n == 0) break;
        if (total + n > cap) {
            while (cap < total + n) cap *= 2;
            result = realloc(result, cap);
        }
        memcpy(result + total, buf, n);
        total += n;
    }

    napi_value buffer;
    void *buf_data;
    NAPI_CALL(env, napi_create_buffer_copy(env, total, result, &buf_data, &buffer));
    free(result);
    return buffer;
}

/* free(pipeline: External) → void */
static napi_value napi_free_pipeline(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    tf_pipeline *p;
    NAPI_CALL(env, napi_get_value_external(env, argv[0], (void **)&p));

    tf_pipeline_free(p);
    return NULL;
}

/* version() → string */
static napi_value napi_tranfi_version(napi_env env, napi_callback_info info) {
    (void)info;
    const char *v = tf_version();
    napi_value result;
    NAPI_CALL(env, napi_create_string_utf8(env, v, strlen(v), &result));
    return result;
}

/* error(pipeline: External) → string | null */
static napi_value napi_tranfi_error(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    tf_pipeline *p;
    NAPI_CALL(env, napi_get_value_external(env, argv[0], (void **)&p));

    const char *err = tf_pipeline_error(p);
    if (!err) {
        napi_value null_val;
        napi_get_null(env, &null_val);
        return null_val;
    }

    napi_value result;
    NAPI_CALL(env, napi_create_string_utf8(env, err, strlen(err), &result));
    return result;
}

/* compileDsl(dslString: string) → string (plan JSON) */
static napi_value napi_compile_dsl(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    if (argc < 1) {
        napi_throw_error(env, NULL, "compileDsl requires DSL string");
        return NULL;
    }

    size_t str_len;
    NAPI_CALL(env, napi_get_value_string_utf8(env, argv[0], NULL, 0, &str_len));
    char *dsl = malloc(str_len + 1);
    NAPI_CALL(env, napi_get_value_string_utf8(env, argv[0], dsl, str_len + 1, &str_len));

    char *error = NULL;
    tf_ir_plan *plan = tf_dsl_parse(dsl, str_len, &error);
    free(dsl);

    if (!plan) {
        napi_throw_error(env, NULL, error ? error : "DSL parse failed");
        free(error);
        return NULL;
    }

    char *json = tf_ir_plan_to_json(plan);
    tf_ir_plan_destroy(plan);

    napi_value result;
    NAPI_CALL(env, napi_create_string_utf8(env, json, strlen(json), &result));
    free(json);
    return result;
}

/* compileToSql(dslString: string) → string (SQL query) */
static napi_value napi_compile_to_sql(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL));

    if (argc < 1) {
        napi_throw_error(env, NULL, "compileToSql requires DSL string");
        return NULL;
    }

    size_t str_len;
    NAPI_CALL(env, napi_get_value_string_utf8(env, argv[0], NULL, 0, &str_len));
    char *dsl = malloc(str_len + 1);
    NAPI_CALL(env, napi_get_value_string_utf8(env, argv[0], dsl, str_len + 1, &str_len));

    char *error = NULL;
    char *sql = tf_compile_to_sql(dsl, str_len, &error);
    free(dsl);

    if (!sql) {
        napi_throw_error(env, NULL, error ? error : "SQL compilation failed");
        free(error);
        return NULL;
    }

    napi_value result;
    NAPI_CALL(env, napi_create_string_utf8(env, sql, strlen(sql), &result));
    tf_string_free(sql);
    return result;
}

/* recipeCount() → number */
static napi_value napi_recipe_count(napi_env env, napi_callback_info info) {
    (void)info;
    napi_value result;
    NAPI_CALL(env, napi_create_uint32(env, (uint32_t)tf_recipe_count(), &result));
    return result;
}

/* recipeName(index: number) → string | null */
static napi_value napi_recipe_name(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL));
    uint32_t index;
    NAPI_CALL(env, napi_get_value_uint32(env, argv[0], &index));
    const char *s = tf_recipe_name(index);
    if (!s) { napi_value n; napi_get_null(env, &n); return n; }
    napi_value result;
    NAPI_CALL(env, napi_create_string_utf8(env, s, strlen(s), &result));
    return result;
}

/* recipeDsl(index: number) → string | null */
static napi_value napi_recipe_dsl(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL));
    uint32_t index;
    NAPI_CALL(env, napi_get_value_uint32(env, argv[0], &index));
    const char *s = tf_recipe_dsl(index);
    if (!s) { napi_value n; napi_get_null(env, &n); return n; }
    napi_value result;
    NAPI_CALL(env, napi_create_string_utf8(env, s, strlen(s), &result));
    return result;
}

/* recipeDescription(index: number) → string | null */
static napi_value napi_recipe_description(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL));
    uint32_t index;
    NAPI_CALL(env, napi_get_value_uint32(env, argv[0], &index));
    const char *s = tf_recipe_description(index);
    if (!s) { napi_value n; napi_get_null(env, &n); return n; }
    napi_value result;
    NAPI_CALL(env, napi_create_string_utf8(env, s, strlen(s), &result));
    return result;
}

/* recipeFindDsl(name: string) → string | null */
static napi_value napi_recipe_find_dsl(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value argv[1];
    NAPI_CALL(env, napi_get_cb_info(env, info, &argc, argv, NULL, NULL));
    size_t str_len;
    NAPI_CALL(env, napi_get_value_string_utf8(env, argv[0], NULL, 0, &str_len));
    char *name = malloc(str_len + 1);
    NAPI_CALL(env, napi_get_value_string_utf8(env, argv[0], name, str_len + 1, &str_len));
    const char *dsl = tf_recipe_find_dsl(name);
    free(name);
    if (!dsl) { napi_value n; napi_get_null(env, &n); return n; }
    napi_value result;
    NAPI_CALL(env, napi_create_string_utf8(env, dsl, strlen(dsl), &result));
    return result;
}

/* Module init */
static napi_value init(napi_env env, napi_value exports) {
    napi_property_descriptor props[] = {
        {"createPipeline",    NULL, napi_create_pipeline,    NULL, NULL, NULL, napi_default, NULL},
        {"push",              NULL, napi_push,               NULL, NULL, NULL, napi_default, NULL},
        {"finish",            NULL, napi_finish,             NULL, NULL, NULL, napi_default, NULL},
        {"pull",              NULL, napi_pull,               NULL, NULL, NULL, napi_default, NULL},
        {"free",              NULL, napi_free_pipeline,      NULL, NULL, NULL, napi_default, NULL},
        {"version",           NULL, napi_tranfi_version,     NULL, NULL, NULL, napi_default, NULL},
        {"error",             NULL, napi_tranfi_error,       NULL, NULL, NULL, napi_default, NULL},
        {"compileDsl",        NULL, napi_compile_dsl,        NULL, NULL, NULL, napi_default, NULL},
        {"compileToSql",      NULL, napi_compile_to_sql,     NULL, NULL, NULL, napi_default, NULL},
        {"recipeCount",       NULL, napi_recipe_count,       NULL, NULL, NULL, napi_default, NULL},
        {"recipeName",        NULL, napi_recipe_name,        NULL, NULL, NULL, napi_default, NULL},
        {"recipeDsl",         NULL, napi_recipe_dsl,         NULL, NULL, NULL, napi_default, NULL},
        {"recipeDescription", NULL, napi_recipe_description, NULL, NULL, NULL, napi_default, NULL},
        {"recipeFindDsl",     NULL, napi_recipe_find_dsl,    NULL, NULL, NULL, napi_default, NULL},
    };
    NAPI_CALL(env, napi_define_properties(env, exports, 14, props));
    return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, init)
