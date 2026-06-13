/*
 * json_path.c -- Small internal JSON Pointer / simple JSONPath resolver.
 *
 * This is intentionally not a full JSONPath implementation. It supports:
 *   - JSON Pointer: /a/b/0 with ~0 and ~1 escapes
 *   - Simple path: $.a[0].b, $['a'], $["a"], or a[0].b
 */

#include "internal.h"
#include "cJSON.h"
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#define TF_JSON_PATH_MAX_INDEX 2147483647

static char *dup_range(const char *start, size_t len) {
    char *s = malloc(len + 1);
    if (!s) return NULL;
    memcpy(s, start, len);
    s[len] = '\0';
    return s;
}

static int parse_array_index(const char *s, int *out) {
    if (!s || !*s) return 0;
    long idx = 0;
    for (const char *p = s; *p; p++) {
        if (!isdigit((unsigned char)*p)) return 0;
        idx = idx * 10 + (*p - '0');
        if (idx > TF_JSON_PATH_MAX_INDEX) return 0;
    }
    *out = (int)idx;
    return 1;
}

static char *decode_pointer_segment(const char *start, size_t len) {
    char *s = malloc(len + 1);
    if (!s) return NULL;
    size_t j = 0;
    for (size_t i = 0; i < len; i++) {
        if (start[i] == '~' && i + 1 < len) {
            if (start[i + 1] == '0') { s[j++] = '~'; i++; continue; }
            if (start[i + 1] == '1') { s[j++] = '/'; i++; continue; }
        }
        s[j++] = start[i];
    }
    s[j] = '\0';
    return s;
}

static const cJSON *descend_key_or_index(const cJSON *cur, const char *segment) {
    if (!cur || !segment) return NULL;
    if (cJSON_IsObject(cur)) {
        return cJSON_GetObjectItemCaseSensitive((cJSON *)cur, segment);
    }
    if (cJSON_IsArray(cur)) {
        int idx = -1;
        if (!parse_array_index(segment, &idx)) return NULL;
        return cJSON_GetArrayItem((cJSON *)cur, idx);
    }
    return NULL;
}

static const cJSON *resolve_json_pointer(const cJSON *root, const char *path) {
    if (!path || path[0] == '\0') return root;
    if (path[0] != '/') return NULL;

    const cJSON *cur = root;
    const char *p = path;
    while (*p == '/') {
        p++;
        const char *start = p;
        while (*p && *p != '/') p++;
        char *segment = decode_pointer_segment(start, (size_t)(p - start));
        if (!segment) return NULL;
        cur = descend_key_or_index(cur, segment);
        free(segment);
        if (!cur) return NULL;
    }
    return *p == '\0' ? cur : NULL;
}

static const cJSON *resolve_simple_path(const cJSON *root, const char *path) {
    if (!path || path[0] == '\0') return root;

    const cJSON *cur = root;
    const char *p = path;
    if (*p == '$') p++;
    if (*p == '\0') return cur;

    while (*p) {
        if (*p == '.') {
            p++;
            if (*p == '\0' || *p == '.' || *p == '[') return NULL;
            const char *start = p;
            while (*p && *p != '.' && *p != '[') p++;
            char *key = dup_range(start, (size_t)(p - start));
            if (!key) return NULL;
            cur = cJSON_IsObject(cur) ? cJSON_GetObjectItemCaseSensitive((cJSON *)cur, key) : NULL;
            free(key);
            if (!cur) return NULL;
            continue;
        }

        if (*p == '[') {
            p++;
            if (*p == '\'' || *p == '"') {
                char quote = *p++;
                const char *start = p;
                while (*p && *p != quote) p++;
                if (*p != quote) return NULL;
                char *key = dup_range(start, (size_t)(p - start));
                if (!key) return NULL;
                p++;
                if (*p != ']') { free(key); return NULL; }
                p++;
                cur = cJSON_IsObject(cur) ? cJSON_GetObjectItemCaseSensitive((cJSON *)cur, key) : NULL;
                free(key);
                if (!cur) return NULL;
                continue;
            }

            const char *start = p;
            while (*p && *p != ']') p++;
            if (*p != ']') return NULL;
            char *idx_s = dup_range(start, (size_t)(p - start));
            if (!idx_s) return NULL;
            int idx = -1;
            int ok = parse_array_index(idx_s, &idx);
            free(idx_s);
            if (!ok || !cJSON_IsArray(cur)) return NULL;
            cur = cJSON_GetArrayItem((cJSON *)cur, idx);
            if (!cur) return NULL;
            p++;
            continue;
        }

        const char *start = p;
        while (*p && *p != '.' && *p != '[') p++;
        if (p == start) return NULL;
        char *key = dup_range(start, (size_t)(p - start));
        if (!key) return NULL;
        cur = cJSON_IsObject(cur) ? cJSON_GetObjectItemCaseSensitive((cJSON *)cur, key) : NULL;
        free(key);
        if (!cur) return NULL;
    }
    return cur;
}

const cJSON *tf_json_path_resolve(const cJSON *root, const char *path) {
    if (!path) return NULL;
    if (path[0] == '\0' || path[0] == '/') return resolve_json_pointer(root, path);
    return resolve_simple_path(root, path);
}
