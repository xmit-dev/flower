/* A freestanding test guest implementing GUEST_ABI.md by hand: no libc, no
 * SDK. Rebuild with tests/guests/build.sh. SPDX-License-Identifier: MIT */
#include <stdint.h>

#define EXPORT(name) __attribute__((export_name(name)))
#define IMPORT(name) __attribute__((import_module("flower"), import_name(name)))

IMPORT("host_call") uint64_t host_call(int32_t op, const uint8_t *payload, uint32_t length);
IMPORT("crypto_call") int32_t crypto_call(int32_t op, int32_t parameter, const void *spans,
                                          int32_t count, uint32_t *result);

enum { NUL, FALSE, TRUE, INT, FLOAT, UTF8, LATIN1, UTF16, ARRAY, MAP, KEY };
enum { SUCCESS, FAILURE };

extern unsigned char __heap_base;
static uintptr_t heap;
static int32_t initialized;
static int32_t calls;

EXPORT("flower_alloc") void *flower_alloc(uint32_t size) {
    if (!heap) heap = (uintptr_t)&__heap_base;
    uintptr_t pointer = (heap + 7) & ~(uintptr_t)7, end = pointer + size;
    uintptr_t limit = __builtin_wasm_memory_size(0) * 65536;
    if (end > limit && __builtin_wasm_memory_grow(0, (end - limit + 65535) / 65536) < 0)
        __builtin_trap();
    heap = end;
    return (void *)pointer;
}

EXPORT("flower_init") int32_t flower_init(void) {
    initialized = 42;
    return 0;
}

static uint8_t out[4096];
static uint32_t length;

static void put(uint8_t byte) { out[length++] = byte; }
static void put32(uint32_t value) {
    for (int i = 0; i < 4; ++i) put(value >> (8 * i));
}
static void text(const char *value) {
    uint32_t size = 0;
    while (value[size]) size++;
    put(LATIN1);
    put32(size);
    for (uint32_t i = 0; i < size; ++i) put(value[i]);
}
static void integer(int32_t value) {
    put(INT);
    put32((uint32_t)value);
}
static uint64_t packed(const void *pointer, uint32_t size) {
    return (uint32_t)(uintptr_t)pointer | ((uint64_t)size << 32);
}
static uint64_t done(void) { return packed(out, length); }

static uint64_t fail(const char *code, const char *message) {
    length = 0;
    put(FAILURE);
    put(MAP);
    put32(2);
    text("code");
    text(code);
    text("message");
    text(message);
    return done();
}

static int named(const char *name, uint32_t size, const char *expected) {
    uint32_t i = 0;
    for (; i < size && expected[i]; ++i)
        if (name[i] != expected[i]) return 0;
    return i == size && !expected[i];
}

static uint32_t read32(const uint8_t *bytes) {
    return bytes[0] | bytes[1] << 8 | bytes[2] << 16 | (uint32_t)bytes[3] << 24;
}

/* The host's reply to a read of counter/n: a success holding null or an int. */
static int32_t current(uint64_t reply) {
    const uint8_t *bytes = (const uint8_t *)(uintptr_t)(uint32_t)reply;
    uint32_t size = reply >> 32;
    if (size >= 6 && bytes[0] == SUCCESS && bytes[1] == INT) return (int32_t)read32(bytes + 2);
    if (size == 2 && bytes[0] == SUCCESS && bytes[1] == NUL) return 0;
    __builtin_trap();
}

/* The record counter/n: a collection reference, then the key. */
static void counter_record(void) {
    put(MAP);
    put32(2);
    text("kind");
    text("collection");
    text("name");
    text("counter");
    text("n");
}

static uint64_t read_counter(void) {
    length = 0;
    counter_record();
    return host_call(4, out, length);
}

EXPORT("flower_invoke") uint64_t flower_invoke(int32_t kind, const char *name, uint32_t name_length,
                                               const uint8_t *args, uint32_t args_length) {
    (void)kind;
    if (named(name, name_length, "echo")) {
        length = 0;
        put(SUCCESS);
        for (uint32_t i = 0; i < args_length; ++i) put(args[i]);
        return done();
    }
    if (named(name, name_length, "init") || named(name, name_length, "calls")) {
        length = 0;
        put(SUCCESS);
        integer(named(name, name_length, "init") ? initialized : ++calls);
        return done();
    }
    if (named(name, name_length, "get")) return read_counter();
    if (named(name, name_length, "count")) {
        int32_t next = current(read_counter()) + 1;
        length = 0;
        counter_record();
        integer(next);
        uint64_t reply = host_call(8, out, length);
        if (((const uint8_t *)(uintptr_t)(uint32_t)reply)[0] != SUCCESS) return reply;
        length = 0;
        put(SUCCESS);
        integer(next);
        return done();
    }
    if (named(name, name_length, "fail")) {
        length = 0;
        put(FAILURE);
        put(MAP);
        put32(3);
        text("code");
        text("CUSTOM");
        text("message");
        text("guest failure");
        text("details");
        put(ARRAY);
        put32(1);
        integer(1);
        return done();
    }
    if (named(name, name_length, "garbage")) {
        length = 0;
        put(SUCCESS);
        put(0xff);
        return done();
    }
    if (named(name, name_length, "trap")) __builtin_trap();
    if (named(name, name_length, "entropy") || named(name, name_length, "peek")) {
        uint32_t result[3];
        if (crypto_call(0, 8, 0, 0, result) != 0) __builtin_trap();
        if (result[0] != 0) return fail("ENTROPY_DENIED", "entropy is unavailable");
        length = 0;
        put(SUCCESS);
        integer((int32_t)result[2]);
        return done();
    }
    return fail("DEFINITION_MISSING", "unknown definition");
}

static void definition(const char *name, const char *kind) {
    text(name);
    put(MAP);
    put32(1);
    text("kind");
    text(kind);
}

static void alias(const char *name, const char *kind) {
    text(name);
    put(MAP);
    put32(2);
    text("name");
    text(name);
    text("kind");
    text(kind);
}

EXPORT("flower_manifest") uint64_t flower_manifest(void) {
    length = 0;
    put(SUCCESS);
    put(MAP);
    put32(2);
    text("definitions");
    put(MAP);
    put32(10);
    static const char *queries[] = {"echo", "init", "calls", "get", "fail", "garbage", "trap", "peek"};
    for (unsigned i = 0; i < sizeof(queries) / sizeof(queries[0]); ++i) definition(queries[i], "query");
    definition("count", "mutation");
    definition("entropy", "mutation");
    text("http");
    put(MAP);
    put32(2);
    alias("echo", "query");
    alias("count", "mutation");
    return done();
}
