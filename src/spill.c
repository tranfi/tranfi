/*
 * spill.c -- secure spill session implementation.
 */

#include "spill.h"
#include "internal.h"

#ifdef __EMSCRIPTEN__

int tf_spill_session_create(const char *root, tf_spill_session **out) {
    (void)root;
    if (out) *out = NULL;
    tf_set_last_error("spill is not supported in WASM");
    return TF_ERROR;
}

int tf_spill_open_run(tf_spill_session *s, const char *label, int *fd, char **path_out) {
    (void)s; (void)label;
    if (fd) *fd = -1;
    if (path_out) *path_out = NULL;
    tf_set_last_error("spill is not supported in WASM");
    return TF_ERROR;
}

FILE *tf_spill_open_run_file(tf_spill_session *s, const char *label, char **path_out) {
    (void)s; (void)label;
    if (path_out) *path_out = NULL;
    tf_set_last_error("spill is not supported in WASM");
    return NULL;
}

int tf_spill_cleanup(tf_spill_session *s) {
    (void)s;
    return TF_OK;
}

#else

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif
#ifndef O_DIRECTORY
#define O_DIRECTORY 0
#endif
#ifndef O_NOFOLLOW
#define O_NOFOLLOW 0
#endif

struct tf_spill_session {
    int     dirfd;
    char   *path;
    char  **names;
    size_t  n_names;
    size_t  cap_names;
    size_t  seq;
};

static void set_spill_errno(const char *prefix, const char *path) {
    char msg[512];
    snprintf(msg, sizeof(msg), "%s '%s': %s", prefix, path ? path : "", strerror(errno));
    tf_set_last_error(msg);
}

static uint64_t random_u64(void) {
    uint64_t v = 0;
    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (fd >= 0) {
        ssize_t n = read(fd, &v, sizeof(v));
        close(fd);
        if (n == (ssize_t)sizeof(v)) return v;
    }
    static atomic_uint_fast64_t counter = 0;
    uint64_t c = (uint64_t)atomic_fetch_add_explicit(&counter, 1, memory_order_relaxed) + 1u;
    v = (uint64_t)time(NULL);
    v ^= ((uint64_t)getpid()) << 32;
    v ^= (uint64_t)(uintptr_t)&v;
    v ^= c * UINT64_C(0x9e3779b97f4a7c15);
    return v;
}

static int append_name(tf_spill_session *s, char *name) {
    if (s->n_names == s->cap_names) {
        size_t need = 0;
        size_t new_cap = 0;
        if (tf_size_add(s->n_names, 1, &need) != TF_OK ||
            tf_size_grow_pow2(s->cap_names, need, 16, &new_cap) != TF_OK) {
            return TF_ERROR;
        }
        char **tmp = tf_reallocarray_checked(s->names, new_cap, sizeof(char *));
        if (!tmp) return TF_ERROR;
        s->names = tmp;
        s->cap_names = new_cap;
    }
    s->names[s->n_names++] = name;
    return TF_OK;
}

static char *join_path(const char *dir, const char *name) {
    size_t dlen = strlen(dir);
    size_t nlen = strlen(name);
    int need_sep = dlen > 0 && dir[dlen - 1] != '/';
    char *path = malloc(dlen + (need_sep ? 1u : 0u) + nlen + 1u);
    if (!path) return NULL;
    memcpy(path, dir, dlen);
    size_t off = dlen;
    if (need_sep) path[off++] = '/';
    memcpy(path + off, name, nlen + 1u);
    return path;
}

static void sanitize_label(const char *label, char out[32]) {
    size_t j = 0;
    if (!label || !label[0]) label = "run";
    for (size_t i = 0; label[i] && j < 31; i++) {
        unsigned char ch = (unsigned char)label[i];
        if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
            (ch >= '0' && ch <= '9') || ch == '-' || ch == '_') {
            out[j++] = (char)ch;
        } else {
            out[j++] = '-';
        }
    }
    if (j == 0) out[j++] = 'r';
    out[j] = '\0';
}

static int validate_root_fd(int fd, const char *root) {
    struct stat st;
    if (fstat(fd, &st) != 0) {
        set_spill_errno("spill: cannot stat root", root);
        return TF_ERROR;
    }
    if (!S_ISDIR(st.st_mode)) {
        tf_set_last_error("spill: root is not a directory");
        return TF_ERROR;
    }
#ifdef S_ISVTX
    if ((st.st_mode & S_IWOTH) && !(st.st_mode & S_ISVTX)) {
#else
    if (st.st_mode & S_IWOTH) {
#endif
        tf_set_last_error("spill: root is world-writable without sticky bit");
        return TF_ERROR;
    }
    if (st.st_uid != geteuid()) {
        tf_set_last_error("spill: root is not owned by the current user");
        return TF_ERROR;
    }
    return TF_OK;
}

int tf_spill_session_create(const char *root, tf_spill_session **out) {
    if (!out) {
        tf_set_last_error("spill: output session pointer is required");
        return TF_ERROR;
    }
    *out = NULL;
    if (!root || !root[0]) {
        tf_set_last_error("spill: root directory is required");
        return TF_ERROR;
    }

    int rootfd = open(root, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (rootfd < 0) {
        set_spill_errno("spill: cannot open root", root);
        return TF_ERROR;
    }
    if (validate_root_fd(rootfd, root) != TF_OK) {
        close(rootfd);
        return TF_ERROR;
    }

    tf_spill_session *s = calloc(1, sizeof(*s));
    if (!s) {
        close(rootfd);
        tf_set_last_error("spill: out of memory");
        return TF_ERROR;
    }
    s->dirfd = -1;

    char name[96];
    int created = 0;
    for (int attempt = 0; attempt < 128; attempt++) {
        snprintf(name, sizeof(name), "tranfi-spill-%016llx-%016llx",
                 (unsigned long long)random_u64(), (unsigned long long)random_u64());
        if (mkdirat(rootfd, name, 0700) == 0) {
            created = 1;
            break;
        }
        if (errno != EEXIST) {
            set_spill_errno("spill: cannot create private directory under", root);
            close(rootfd);
            free(s);
            return TF_ERROR;
        }
    }
    if (!created) {
        tf_set_last_error("spill: could not allocate a unique private directory");
        close(rootfd);
        free(s);
        return TF_ERROR;
    }

    s->path = join_path(root, name);
    if (!s->path) {
        unlinkat(rootfd, name, AT_REMOVEDIR);
        close(rootfd);
        free(s);
        tf_set_last_error("spill: out of memory");
        return TF_ERROR;
    }
    s->dirfd = openat(rootfd, name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    close(rootfd);
    if (s->dirfd < 0) {
        set_spill_errno("spill: cannot open private directory", s->path);
        rmdir(s->path);
        free(s->path);
        free(s);
        return TF_ERROR;
    }
    (void)fchmod(s->dirfd, 0700);
    if (out) *out = s;
    return TF_OK;
}

int tf_spill_open_run(tf_spill_session *s, const char *label, int *fd, char **path_out) {
    if (fd) *fd = -1;
    if (path_out) *path_out = NULL;
    if (!s || s->dirfd < 0) {
        tf_set_last_error("spill: invalid spill session");
        return TF_ERROR;
    }

    char clean[32];
    sanitize_label(label, clean);
    for (int attempt = 0; attempt < 128; attempt++) {
        char name_buf[128];
        snprintf(name_buf, sizeof(name_buf), "%s-%zu-%016llx.bin",
                 clean, s->seq++, (unsigned long long)random_u64());
        int runfd = openat(s->dirfd, name_buf,
                           O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW | O_CLOEXEC,
                           0600);
        if (runfd < 0) {
            if (errno == EEXIST) continue;
            set_spill_errno("spill: cannot create run", name_buf);
            return TF_ERROR;
        }
        char *tracked = strdup(name_buf);
        char *path = join_path(s->path, name_buf);
        if (!tracked || !path || append_name(s, tracked) != TF_OK) {
            close(runfd);
            unlinkat(s->dirfd, name_buf, 0);
            free(tracked);
            free(path);
            tf_set_last_error("spill: out of memory");
            return TF_ERROR;
        }
        if (fd) *fd = runfd;
        else close(runfd);
        if (path_out) *path_out = path;
        else free(path);
        return TF_OK;
    }
    tf_set_last_error("spill: could not allocate a unique run file");
    return TF_ERROR;
}

FILE *tf_spill_open_run_file(tf_spill_session *s, const char *label, char **path_out) {
    int fd = -1;
    char *path = NULL;
    if (tf_spill_open_run(s, label, &fd, &path) != TF_OK) return NULL;
    FILE *f = fdopen(fd, "wb");
    if (!f) {
        char msg[512];
        snprintf(msg, sizeof(msg), "spill: cannot fdopen run '%s': %s", path ? path : "", strerror(errno));
        tf_set_last_error(msg);
        close(fd);
        if (path) remove(path);
        free(path);
        if (path_out) *path_out = NULL;
        return NULL;
    }
    if (path_out) *path_out = path;
    else free(path);
    return f;
}

int tf_spill_cleanup(tf_spill_session *s) {
    if (!s) return TF_OK;
    int rc = TF_OK;
    if (s->dirfd >= 0) {
        for (size_t i = 0; i < s->n_names; i++) {
            if (s->names[i] && unlinkat(s->dirfd, s->names[i], 0) != 0 && errno != ENOENT) rc = TF_ERROR;
        }
        close(s->dirfd);
    }
    for (size_t i = 0; i < s->n_names; i++) free(s->names[i]);
    free(s->names);
    if (s->path && rmdir(s->path) != 0 && errno != ENOENT) rc = TF_ERROR;
    free(s->path);
    free(s);
    return rc;
}

#endif
