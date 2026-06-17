/*
 * arena.c — Simple arena (bump) allocator.
 * Allocates from fixed-size blocks, grows by adding new blocks.
 */

#include "internal.h"
#include <stdlib.h>
#include <string.h>

#define DEFAULT_BLOCK_SIZE (64 * 1024) /* 64 KB */

static tf_arena_block *block_create(size_t cap) {
    tf_arena_block *blk = malloc(sizeof(tf_arena_block));
    if (!blk) return NULL;
    blk->data = malloc(cap);
    if (!blk->data) { free(blk); return NULL; }
    blk->used = 0;
    blk->cap = cap;
    blk->next = NULL;
    return blk;
}

static void block_free(tf_arena_block *blk) {
    while (blk) {
        tf_arena_block *next = blk->next;
        free(blk->data);
        free(blk);
        blk = next;
    }
}

tf_arena *tf_arena_create(size_t block_size) {
    if (block_size == 0) block_size = DEFAULT_BLOCK_SIZE;
    if (tf_size_align(block_size, 8, &block_size) != TF_OK) return NULL;
    tf_arena *a = malloc(sizeof(tf_arena));
    if (!a) return NULL;
    a->block_size = block_size;
    a->head = block_create(block_size);
    if (!a->head) { free(a); return NULL; }
    a->current = a->head;
    return a;
}

void *tf_arena_alloc(tf_arena *a, size_t size) {
    if (!a || size == 0) return NULL;
    if (tf_size_align(size, 8, &size) != TF_OK) return NULL;

    /* Try current block */
    size_t used_after = 0;
    if (tf_size_add(a->current->used, size, &used_after) == TF_OK && used_after <= a->current->cap) {
        void *ptr = a->current->data + a->current->used;
        a->current->used = used_after;
        return ptr;
    }

    /* Need a new block — at least big enough for this allocation */
    size_t new_cap = a->block_size;
    if (new_cap < size) new_cap = size;
    tf_arena_block *blk = block_create(new_cap);
    if (!blk) return NULL;

    a->current->next = blk;
    a->current = blk;
    void *ptr = blk->data;
    blk->used = size;
    return ptr;
}

char *tf_arena_strdup(tf_arena *a, const char *s) {
    if (!s) return NULL;
    size_t len = 0;
    if (tf_size_add(strlen(s), 1, &len) != TF_OK) return NULL;
    char *copy = tf_arena_alloc(a, len);
    if (copy) memcpy(copy, s, len);
    return copy;
}

void tf_arena_reset(tf_arena *a) {
    if (!a) return;
    /* Free all blocks except the first, reset the first */
    if (a->head->next) {
        block_free(a->head->next);
        a->head->next = NULL;
    }
    a->head->used = 0;
    a->current = a->head;
}

void tf_arena_free(tf_arena *a) {
    if (!a) return;
    block_free(a->head);
    free(a);
}
