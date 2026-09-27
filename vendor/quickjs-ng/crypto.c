/* Flower's binary-only host crypto bridge. SPDX-License-Identifier: MIT */
#include "quickjs.h"
#include <math.h>
#include <stdint.h>
#include <stdlib.h>

/* The host may inspect these bytes only for this synchronous call. It must
 * finish reading them before reentering flower_alloc to produce a result. */
typedef struct { uint32_t pointer, length; } FlowerCryptoSpan;
typedef struct { uint32_t kind, pointer, length_or_integer; } FlowerCryptoResult;
_Static_assert(sizeof(FlowerCryptoSpan) == 8, "crypto span ABI");
_Static_assert(sizeof(FlowerCryptoResult) == 12, "crypto result ABI");

__attribute__((import_module("flower"), import_name("crypto_call")))
extern int32_t flower_crypto_call(uint32_t, uint32_t, const FlowerCryptoSpan *,
                                  uint32_t, FlowerCryptoResult *);
extern void *flower_alloc(size_t);
extern void flower_free(void *);
extern int flower_crypto_view(JSContext *, JSValueConst, const uint8_t **, size_t *);

/* Only flower_invoke toggles this, immediately around its private runner call.
 * In particular, per-invocation bundle initialization still sees false. */
int flower_crypto_callback_active;
static JSClassID shared_key_class;

static JSValue shared_key_to_json(JSContext *ctx, JSValueConst this_value,
                                  int argc, JSValueConst *argv) {
    (void)this_value; (void)argc; (void)argv;
    return JS_ThrowTypeError(ctx, "Shared keys are invocation-local handles and cannot be serialized");
}

static int crypto_uint32(JSContext *ctx, JSValueConst value, uint32_t *result) {
    double number;
    if (!JS_IsNumber(value) || JS_ToFloat64(ctx, &number, value) < 0
        || !isfinite(number) || number < 0 || number > UINT32_MAX
        || trunc(number) != number) {
        JS_ThrowTypeError(ctx, "crypto operation and parameter must be unsigned 32-bit integers");
        return -1;
    }
    *result = (uint32_t)number;
    return 0;
}

/* Buffers returned by the host were allocated with flower_alloc, not QuickJS's
 * accounted arena allocator. QuickJS also uses this hook for ArrayBuffer
 * transfer/resize, so preserve realloc's failure ownership convention. */
static void *crypto_realloc(JSRuntime *rt, void *opaque, void *pointer, size_t length) {
    (void)rt;
    (void)opaque;
    if (!length) { flower_free(pointer); return NULL; }
    return realloc(pointer, length);
}

static JSValue crypto_result(JSContext *ctx, FlowerCryptoResult result) {
    uint8_t *pointer = (uint8_t *)(uintptr_t)result.pointer;
    size_t length = result.length_or_integer;
    switch (result.kind) {
    case 0: {
        /* Split adoption from typed-array creation: the first call leaves the
         * raw allocation with us on failure, while the second retains/frees
         * its own reference. The combined API cannot distinguish those cases. */
        if (!pointer) __builtin_trap();
        JSValue buffer = JS_NewArrayBuffer(ctx, pointer, length, 0,
                                           crypto_realloc, NULL, false);
        if (JS_IsException(buffer)) { flower_free(pointer); return buffer; }
        JSValue value = JS_NewTypedArray(ctx, 1, &buffer, JS_TYPED_ARRAY_UINT8);
        JS_FreeValue(ctx, buffer);
        return value;
    }
    case 1: if (pointer || length) __builtin_trap(); return JS_FALSE;
    case 2: if (pointer || length) __builtin_trap(); return JS_TRUE;
    case 3: if (pointer || length) __builtin_trap(); return JS_NULL;
    case 4: if (pointer) __builtin_trap(); return JS_NewInt32(ctx, (int32_t)result.length_or_integer);
    case 7: {
        if (pointer || !length) __builtin_trap();
        JSValue value = JS_NewObjectClass(ctx, shared_key_class);
        if (JS_IsException(value)) return value;
        /* The deterministic slot is C-private, never a JS property or random
         * token. Only this class can pass it back through the raw bridge. */
        JS_SetOpaque(value, (void *)(uintptr_t)length);
        if (JS_PreventExtensions(ctx, value) < 0) {
            JS_FreeValue(ctx, value);
            return JS_EXCEPTION;
        }
        return value;
    }
    case 5:
    case 6: {
        if (!pointer) __builtin_trap();
        JSValue text = JS_NewStringLen(ctx, (const char *)pointer, length);
        flower_free(pointer);
        if (JS_IsException(text) || result.kind == 6) return text;
        JSValue error = JS_NewError(ctx);
        if (JS_IsException(error)) { JS_FreeValue(ctx, text); return error; }
        if (JS_DefinePropertyValueStr(ctx, error, "message", text,
                JS_PROP_WRITABLE | JS_PROP_CONFIGURABLE) < 0) {
            JS_FreeValue(ctx, error);
            return JS_EXCEPTION;
        }
        return JS_Throw(ctx, error);
    }
    default: __builtin_trap();
    }
}

static JSValue crypto_host(JSContext *ctx, JSValueConst this_value,
                           int argc, JSValueConst *argv) {
    (void)this_value;
    uint32_t operation, parameter;
    if (argc < 2) return JS_ThrowTypeError(ctx, "expected crypto operation and parameter");
    if (crypto_uint32(ctx, argv[0], &operation) < 0
        || crypto_uint32(ctx, argv[1], &parameter) < 0) return JS_EXCEPTION;
    if ((!operation || (operation >= 200 && operation <= 202)) && !flower_crypto_callback_active)
        return JS_ThrowTypeError(ctx, "randomness and managed keys are unavailable during bundle initialization");
    size_t offset = 2;
    if (operation == 201 || operation == 202) {
        if (parameter || argc != 5)
            return JS_ThrowTypeError(ctx, "shared-key crypto requires a native handle and two byte inputs");
        void *handle = JS_GetOpaque2(ctx, argv[2], shared_key_class);
        if (!handle) return JS_EXCEPTION;
        parameter = (uint32_t)(uintptr_t)handle;
        offset++;
    }
    size_t count = (size_t)argc - offset;
    if (count > SIZE_MAX / sizeof(FlowerCryptoSpan))
        return JS_ThrowRangeError(ctx, "crypto arguments exceed addressable memory");
    /* Do not coerce objects or consult mutable JS prototypes/getters. All
     * positional arguments have finished evaluating before C obtains pointers. */
    for (size_t i = 0; i < count; ++i) {
        JSValueConst value = argv[i + offset];
        if (!JS_IsString(value) && JS_GetTypedArrayType(value) != JS_TYPED_ARRAY_UINT8)
            return JS_ThrowTypeError(ctx, "crypto inputs must be Uint8Array or primitive string");
    }
    FlowerCryptoSpan *spans = flower_alloc(count * sizeof(*spans));
    size_t initialized = 0;
    JSValue value = JS_EXCEPTION;
    for (size_t i = 0; i < count; ++i) {
        JSValueConst input = argv[i + offset];
        size_t length;
        const uint8_t *pointer;
        if (JS_IsString(input)) {
            pointer = (const uint8_t *)JS_ToCStringLen(ctx, &length, input);
            if (!pointer) goto done;
        } else {
            if (flower_crypto_view(ctx, input, &pointer, &length) < 0) goto done;
        }
        spans[i] = (FlowerCryptoSpan){(uint32_t)(uintptr_t)pointer, (uint32_t)length};
        initialized++;
    }
    FlowerCryptoResult result = {UINT32_MAX, 0, 0};
    if (flower_crypto_call(operation, parameter, spans, count, &result) != 0)
        __builtin_trap();
    value = crypto_result(ctx, result);
done:
    for (size_t i = 0; i < initialized; ++i)
        if (JS_IsString(argv[i + offset]))
            JS_FreeCString(ctx, (const char *)(uintptr_t)spans[i].pointer);
    flower_free(spans);
    return value;
}

int flower_crypto_init(JSContext *ctx) {
    JSClassDef definition = { .class_name = "SharedKey" };
    if (!shared_key_class) JS_NewClassID(JS_GetRuntime(ctx), &shared_key_class);
    if (JS_NewClass(JS_GetRuntime(ctx), shared_key_class, &definition) < 0) return -1;
    JSValue prototype = JS_NewObject(ctx);
    if (JS_IsException(prototype)) return -1;
    if (JS_DefinePropertyValueStr(ctx, prototype, "kind", JS_NewString(ctx, "sharedKey"), 0) < 0 ||
        JS_DefinePropertyValueStr(ctx, prototype, "toJSON",
            JS_NewCFunction(ctx, shared_key_to_json, "toJSON", 0), 0) < 0 ||
        JS_PreventExtensions(ctx, prototype) < 0) {
        JS_FreeValue(ctx, prototype);
        return -1;
    }
    JS_SetClassProto(ctx, shared_key_class, prototype);
    JSValue global = JS_GetGlobalObject(ctx);
    int status = JS_DefinePropertyValueStr(ctx, global, "__flowerCrypto",
        JS_NewCFunction(ctx, crypto_host, "__flowerCrypto", 2), 0);
    JS_FreeValue(ctx, global);
    return status;
}
