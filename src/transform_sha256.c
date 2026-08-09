#include "transform_internal.h"

#include <string.h>

typedef struct tf_sha256_context {
    uint32_t state[8];
    uint64_t total_bytes;
    uint8_t block[64];
    size_t block_len;
} tf_sha256_context;

static const uint32_t sha256_round[64] = {
    0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u,
    0x3956c25bu, 0x59f111f1u, 0x923f82a4u, 0xab1c5ed5u,
    0xd807aa98u, 0x12835b01u, 0x243185beu, 0x550c7dc3u,
    0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u, 0xc19bf174u,
    0xe49b69c1u, 0xefbe4786u, 0x0fc19dc6u, 0x240ca1ccu,
    0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau,
    0x983e5152u, 0xa831c66du, 0xb00327c8u, 0xbf597fc7u,
    0xc6e00bf3u, 0xd5a79147u, 0x06ca6351u, 0x14292967u,
    0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu, 0x53380d13u,
    0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u,
    0xa2bfe8a1u, 0xa81a664bu, 0xc24b8b70u, 0xc76c51a3u,
    0xd192e819u, 0xd6990624u, 0xf40e3585u, 0x106aa070u,
    0x19a4c116u, 0x1e376c08u, 0x2748774cu, 0x34b0bcb5u,
    0x391c0cb3u, 0x4ed8aa4au, 0x5b9cca4fu, 0x682e6ff3u,
    0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u,
    0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u
};

static uint32_t rotate_right(uint32_t value, unsigned int count) {
    return (value >> count) | (value << (32u - count));
}

static uint32_t load_be32(const uint8_t *data) {
    return ((uint32_t)data[0] << 24) | ((uint32_t)data[1] << 16)
        | ((uint32_t)data[2] << 8) | (uint32_t)data[3];
}

static void store_be32(uint8_t *data, uint32_t value) {
    data[0] = (uint8_t)(value >> 24);
    data[1] = (uint8_t)(value >> 16);
    data[2] = (uint8_t)(value >> 8);
    data[3] = (uint8_t)value;
}

static void sha256_compress(tf_sha256_context *context, const uint8_t block[64]) {
    uint32_t words[64];
    uint32_t a, b, c, d, e, f, g, h;
    for (size_t i = 0; i < 16; ++i) words[i] = load_be32(block + i * 4);
    for (size_t i = 16; i < 64; ++i) {
        uint32_t s0 = rotate_right(words[i - 15], 7)
            ^ rotate_right(words[i - 15], 18) ^ (words[i - 15] >> 3);
        uint32_t s1 = rotate_right(words[i - 2], 17)
            ^ rotate_right(words[i - 2], 19) ^ (words[i - 2] >> 10);
        words[i] = words[i - 16] + s0 + words[i - 7] + s1;
    }
    a = context->state[0]; b = context->state[1];
    c = context->state[2]; d = context->state[3];
    e = context->state[4]; f = context->state[5];
    g = context->state[6]; h = context->state[7];
    for (size_t i = 0; i < 64; ++i) {
        uint32_t sum1 = rotate_right(e, 6) ^ rotate_right(e, 11) ^ rotate_right(e, 25);
        uint32_t choose = (e & f) ^ ((~e) & g);
        uint32_t temp1 = h + sum1 + choose + sha256_round[i] + words[i];
        uint32_t sum0 = rotate_right(a, 2) ^ rotate_right(a, 13) ^ rotate_right(a, 22);
        uint32_t majority = (a & b) ^ (a & c) ^ (b & c);
        uint32_t temp2 = sum0 + majority;
        h = g; g = f; f = e; e = d + temp1;
        d = c; c = b; b = a; a = temp1 + temp2;
    }
    context->state[0] += a; context->state[1] += b;
    context->state[2] += c; context->state[3] += d;
    context->state[4] += e; context->state[5] += f;
    context->state[6] += g; context->state[7] += h;
}

static void sha256_init(tf_sha256_context *context) {
    static const uint32_t initial[8] = {
        0x6a09e667u, 0xbb67ae85u, 0x3c6ef372u, 0xa54ff53au,
        0x510e527fu, 0x9b05688cu, 0x1f83d9abu, 0x5be0cd19u
    };
    memset(context, 0, sizeof(*context));
    memcpy(context->state, initial, sizeof(initial));
}

static void sha256_update(tf_sha256_context *context,
                          const uint8_t *data, size_t len) {
    context->total_bytes += len;
    while (len != 0) {
        size_t space = 64 - context->block_len;
        size_t take = len < space ? len : space;
        memcpy(context->block + context->block_len, data, take);
        context->block_len += take;
        data += take;
        len -= take;
        if (context->block_len == 64) {
            sha256_compress(context, context->block);
            context->block_len = 0;
        }
    }
}

static void sha256_final(tf_sha256_context *context, uint8_t out[32]) {
    uint64_t total_bits = context->total_bytes * UINT64_C(8);
    context->block[context->block_len++] = 0x80;
    if (context->block_len > 56) {
        memset(context->block + context->block_len, 0, 64 - context->block_len);
        sha256_compress(context, context->block);
        context->block_len = 0;
    }
    memset(context->block + context->block_len, 0, 56 - context->block_len);
    for (size_t i = 0; i < 8; ++i)
        context->block[56 + i] = (uint8_t)(total_bits >> (56u - 8u * i));
    sha256_compress(context, context->block);
    for (size_t i = 0; i < 8; ++i) store_be32(out + i * 4, context->state[i]);
    memset(context, 0, sizeof(*context));
}

void tf_transform_sha256(const uint8_t *data, size_t len, uint8_t out[32]) {
    tf_sha256_context context;
    sha256_init(&context);
    if (len != 0) sha256_update(&context, data, len);
    sha256_final(&context, out);
}

tf_transform_code tf_transform_sha256_runtime(
    const uint8_t *data, size_t len,
    const tf_transform_runtime_copy *runtime,
    uint8_t out[32], tf_transform_error **error) {
    tf_sha256_context context;
    size_t offset = 0;
    tf_transform_code code;
    if ((!data && len != 0) || !out)
        return tf_transform_set_error(
            error, TF_TRANSFORM_INVALID_ARGUMENT, "SHA-256 argument is null");
    code = tf_transform_poll_cancel(runtime, error);
    if (code != TF_TRANSFORM_OK) return code;
    sha256_init(&context);
    while (offset < len) {
        size_t remaining = len - offset;
        size_t chunk = remaining < TF_TRANSFORM_CANCEL_BYTES_V1
            ? remaining : TF_TRANSFORM_CANCEL_BYTES_V1;
        sha256_update(&context, data + offset, chunk);
        offset += chunk;
        code = tf_transform_poll_cancel(runtime, error);
        if (code != TF_TRANSFORM_OK) {
            memset(&context, 0, sizeof(context));
            return code;
        }
    }
    sha256_final(&context, out);
    return tf_transform_poll_cancel(runtime, error);
}
