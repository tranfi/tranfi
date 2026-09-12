/*
 * buffer.c — Growable byte buffer for streaming I/O.
 * Supports write (append), read (consume), and compact operations.
 */

#include "internal.h"
#include "cJSON.h"
#include <stdlib.h>
#include <string.h>

#define INITIAL_CAP 4096
#define COMPACT_AFTER_READ_POS (64 * 1024)
#define MAX_RETAINED_EMPTY_CAP (1024 * 1024)

void tf_buffer_init(tf_buffer *b) {
    b->data = NULL;
    b->len = 0;
    b->cap = 0;
    b->read_pos = 0;
}

static int buffer_ensure(tf_buffer *b, size_t needed) {
    if (needed <= b->cap) return TF_OK;
    size_t new_cap = 0;
    if (tf_size_grow_pow2(b->cap, needed, INITIAL_CAP, &new_cap) != TF_OK) return TF_ERROR;
    uint8_t *new_data = tf_reallocarray_checked(b->data, new_cap, sizeof(uint8_t));
    if (!new_data) return TF_ERROR;
    b->data = new_data;
    b->cap = new_cap;
    return TF_OK;
}

int tf_buffer_write(tf_buffer *b, const uint8_t *data, size_t len) {
    if (len == 0) return TF_OK;
    size_t new_len = 0;
    if (tf_size_add(b->len, len, &new_len) != TF_OK) return TF_ERROR;
    if (buffer_ensure(b, new_len) != TF_OK) return TF_ERROR;
    memcpy(b->data + b->len, data, len);
    b->len = new_len;
    return TF_OK;
}

int tf_buffer_write_str(tf_buffer *b, const char *s) {
    return tf_buffer_write(b, (const uint8_t *)s, strlen(s));
}

int tf_buffer_write_line(tf_buffer *b, const char *s) {
    int rc = tf_buffer_write_str(b, s);
    if (rc == TF_OK) rc = tf_buffer_write(b, (const uint8_t *)"\n", 1);
    return rc;
}

int tf_buffer_write_json_line(tf_buffer *b, const cJSON *obj) {
    if (!b || !obj) return TF_ERROR;
    char *printed = cJSON_PrintUnformatted(obj);
    if (!printed) return TF_ERROR;
    int rc = tf_buffer_write_line(b, printed);
    free(printed);
    return rc;
}

int tf_side_write_error(tf_side_channels *side, const char *msg) {
    if (msg) tf_set_last_error(msg);
    if (!side || !side->errors || !msg) return TF_OK;
    return tf_buffer_write_line(side->errors, msg);
}

int tf_json_add_string(cJSON *obj, const char *name, const char *value) {
    return cJSON_AddStringToObject(obj, name, value ? value : "") ? TF_OK : TF_ERROR;
}

int tf_json_add_number(cJSON *obj, const char *name, double value) {
    return cJSON_AddNumberToObject(obj, name, value) ? TF_OK : TF_ERROR;
}

int tf_json_add_bool(cJSON *obj, const char *name, int value) {
    return cJSON_AddBoolToObject(obj, name, value ? 1 : 0) ? TF_OK : TF_ERROR;
}

int tf_json_add_null(cJSON *obj, const char *name) {
    return cJSON_AddNullToObject(obj, name) ? TF_OK : TF_ERROR;
}

int tf_json_add_item(cJSON *obj, const char *name, cJSON *item) {
    if (!obj || !name || !item) {
        cJSON_Delete(item);
        return TF_ERROR;
    }
    if (!cJSON_AddItemToObject(obj, name, item)) {
        cJSON_Delete(item);
        return TF_ERROR;
    }
    return TF_OK;
}

int tf_json_add_array_item(cJSON *arr, cJSON *item) {
    if (!arr || !item) {
        cJSON_Delete(item);
        return TF_ERROR;
    }
    if (!cJSON_AddItemToArray(arr, item)) {
        cJSON_Delete(item);
        return TF_ERROR;
    }
    return TF_OK;
}

size_t tf_buffer_read(tf_buffer *b, uint8_t *out, size_t len) {
    size_t avail = b->len - b->read_pos;
    if (len > avail) len = avail;
    if (len > 0) {
        memcpy(out, b->data + b->read_pos, len);
        b->read_pos += len;
    }
    /* Auto-compact when all data consumed. Keep a modest scratch buffer for
     * future chunks, but release one-off large output buffers after drains. */
    if (b->read_pos == b->len) {
        b->read_pos = 0;
        b->len = 0;
        if (b->cap > MAX_RETAINED_EMPTY_CAP) {
            uint8_t *new_data = tf_reallocarray_checked(b->data, MAX_RETAINED_EMPTY_CAP,
                                                        sizeof(uint8_t));
            if (new_data) {
                b->data = new_data;
                b->cap = MAX_RETAINED_EMPTY_CAP;
            }
        }
    } else if (b->read_pos >= COMPACT_AFTER_READ_POS && b->read_pos > b->cap / 2) {
        tf_buffer_compact(b);
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
