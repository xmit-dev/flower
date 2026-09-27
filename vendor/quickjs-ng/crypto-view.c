/* Flower extension; included after the byte-for-byte pinned QuickJS core.
 * Public GetTypedArrayBuffer currently reports ta->length, which does not track
 * resizable-buffer changes. Use the validated live element count instead.
 * No JS getters, coercions, proxies or callbacks execute here.
 * SPDX-License-Identifier: MIT */
int flower_crypto_view(JSContext *ctx, JSValueConst value,
                        const uint8_t **pointer, size_t *length) {
    if (JS_GetTypedArrayType(value) != JS_TYPED_ARRAY_UINT8) {
        JS_ThrowTypeError(ctx, "crypto input must be Uint8Array");
        return -1;
    }
    JSObject *object = JS_VALUE_GET_OBJ(value);
    if (typed_array_is_oob(object)) {
        JS_ThrowTypeErrorArrayBufferOOB(ctx);
        return -1;
    }
    JSTypedArray *view = object->u.typed_array;
    JSArrayBuffer *buffer = view->buffer->u.array_buffer;
    if (buffer->shared) {
        JS_ThrowTypeError(ctx, "crypto inputs must use a non-shared ArrayBuffer");
        return -1;
    }
    size_t offset = view->offset, bytes = object->u.array.count;
    if (offset > buffer->byte_length || bytes > buffer->byte_length - offset) {
        JS_ThrowRangeError(ctx, "crypto typed-array view is out of bounds");
        return -1;
    }
    *pointer = buffer->data + offset;
    *length = bytes;
    return 0;
}
