/*
 * path_policy.c -- Internal helpers for host-policy-approved file paths.
 *
 * The public policy pass rewrites plan-owned file args to canonical paths.
 * These helpers let operators reject later symlink/path swaps for those
 * rewritten paths while preserving normal fopen() behavior for policy-neutral
 * constructors and custom host resolvers.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if !defined(_WIN32) && !defined(__EMSCRIPTEN__)
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

const char *tf_policy_validated_path_arg(const cJSON *args, const char *path_arg) {
    if (!args || !path_arg) return NULL;
    const char *hidden = NULL;
    if (strcmp(path_arg, "file") == 0) {
        hidden = TF_POLICY_VALIDATED_FILE_PATH_ARG;
    } else if (strcmp(path_arg, "rules_file") == 0 || strcmp(path_arg, "rulesFile") == 0) {
        hidden = TF_POLICY_VALIDATED_RULES_FILE_PATH_ARG;
    }
    if (!hidden) return NULL;
    const cJSON *item = cJSON_GetObjectItemCaseSensitive((cJSON *)args, hidden);
    return cJSON_IsString(item) && item->valuestring && item->valuestring[0]
        ? item->valuestring
        : NULL;
}

#if !defined(_WIN32) && !defined(__EMSCRIPTEN__)
static int policy_fd_matches_validated_path(int fd, const char *path,
                                            const char *validated_path) {
    struct stat fd_stat;
    struct stat path_stat;
    if (fstat(fd, &fd_stat) != 0 || !S_ISREG(fd_stat.st_mode)) return TF_ERROR;

    char *resolved = realpath(path, NULL);
    if (!resolved) return TF_ERROR;
    int same_path = strcmp(resolved, validated_path) == 0;
    int stat_ok = stat(resolved, &path_stat) == 0;
    free(resolved);
    if (!same_path || !stat_ok) return TF_ERROR;
    if (fd_stat.st_dev != path_stat.st_dev || fd_stat.st_ino != path_stat.st_ino) {
        return TF_ERROR;
    }
    return TF_OK;
}
#endif

FILE *tf_policy_fopen_read(const char *path, const char *validated_path) {
    if (!path || !path[0]) return NULL;
    if (!validated_path || !validated_path[0]) return fopen(path, "rb");

#if defined(_WIN32) || defined(__EMSCRIPTEN__)
    return fopen(path, "rb");
#else
    int flags = O_RDONLY;
#ifdef O_CLOEXEC
    flags |= O_CLOEXEC;
#endif
#ifdef O_NOFOLLOW
    flags |= O_NOFOLLOW;
#endif
    int fd = open(path, flags);
    if (fd < 0) return NULL;
    if (policy_fd_matches_validated_path(fd, path, validated_path) != TF_OK) {
        close(fd);
        tf_set_last_error("policy path changed before open");
        return NULL;
    }
    FILE *f = fdopen(fd, "rb");
    if (!f) {
        close(fd);
        return NULL;
    }
    return f;
#endif
}
