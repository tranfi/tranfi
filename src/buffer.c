/*
 * buffer.c — Growable byte buffer for streaming I/O.
 * Supports write (append), read (consume), and compact operations.
 */

#include "internal.h"
#include <stdlib.h>
#include <string.h>

#define INITIAL_CAP 4096

void tf_buffer_init(tf_buffer *b) {
    b->data = NULL;
    b->len = 0;
    b->cap = 0;
    b->read_pos = 0;
}

static int buffer_ensure(tf_buffer *b, size_t extra) {
    size_t needed = b->len + extra;
    if (needed <= b->cap) return TF_OK;
    size_t new_cap = b->cap ? b->cap : INITIAL_CAP;
    while (new_cap < needed) new_cap *= 2;
    uint8_t *new_data = realloc(b->data, new_cap);
    if (!new_data) return TF_ERROR;
    b->data = new_data;
    b->cap = new_cap;
    return TF_OK;
}

int tf_buffer_write(tf_buffer *b, const uint8_t *data, size_t len) {
    if (len == 0) return TF_OK;
    if (buffer_ensure(b, len) != TF_OK) return TF_ERROR;
    memcpy(b->data + b->len, data, len);
    b->len += len;
    return TF_OK;
}

int tf_buffer_write_str(tf_buffer *b, const char *s) {
    return tf_buffer_write(b, (const uint8_t *)s, strlen(s));
}

size_t tf_buffer_read(tf_buffer *b, uint8_t *out, size_t len) {
    size_t avail = b->len - b->read_pos;
    if (len > avail) len = avail;
    if (len > 0) {
        memcpy(out, b->data + b->read_pos, len);
        b->read_pos += len;
    }
    /* Auto-compact when all data consumed */
    if (b->read_pos == b->len) {
        b->read_pos = 0;
        b->len = 0;
    }
    return len;
}

size_t tf_buffer_readable(const tf_buffer *b) {
    return b->len - b->read_pos;
}

void tf_buffer_compact(tf_buffer *b) {
    if (b->read_pos == 0) return;
    size_t remaining = b->len - b->read_pos;
    if (remaining > 0) {
        memmove(b->data, b->data + b->read_pos, remaining);
    }
    b->len = remaining;
    b->read_pos = 0;
}

void tf_buffer_free(tf_buffer *b) {
    free(b->data);
    b->data = NULL;
    b->len = 0;
    b->cap = 0;
    b->read_pos = 0;
}
