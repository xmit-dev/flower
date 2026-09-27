/* Guest ABI values (GUEST_ABI.md), included after pinned upstream/quickjs.c.
 * Encoding reads shapes, fast arrays and string buffers directly: it never
 * runs a getter, proxy trap, toJSON hook or any other application code, and
 * strings leave in QuickJS's own Latin-1 or UTF-16 representation.
 * SPDX-License-Identifier: MIT */

enum {
    FLOWER_NULL, FLOWER_FALSE, FLOWER_TRUE, FLOWER_INT, FLOWER_FLOAT,
    FLOWER_UTF8, FLOWER_LATIN1, FLOWER_UTF16, FLOWER_ARRAY, FLOWER_MAP, FLOWER_KEY,
};
enum { FLOWER_SUCCESS, FLOWER_FAILURE };
#define FLOWER_MAX_DEPTH 128
/* Per-message key interning. Slots from earlier messages are stale by
 * generation, so a message never clears the table. Interning stops at half
 * occupancy; later new keys are still numbered, just never referenced. */
#define FLOWER_KEY_SLOTS 1024

typedef struct {
    uint8_t *data;
    uint32_t length, capacity;
    uint32_t next_key, interned, generation;
    const char *invalid;
    JSObject *active[FLOWER_MAX_DEPTH + 1];
} FlowerWriter;

static FlowerWriter flower_writer;
static struct { JSAtom atom; uint32_t index, generation; } flower_key_slots[FLOWER_KEY_SLOTS];

static FlowerWriter *flower_writer_begin(void) {
    FlowerWriter *w = &flower_writer;
    w->length = 0;
    w->next_key = 0;
    w->interned = 0;
    w->generation++;
    w->invalid = NULL;
    return w;
}

static uint8_t *flower_reserve(FlowerWriter *w, uint32_t extra) {
    if (w->capacity - w->length < extra) {
        uint64_t needed = (uint64_t)w->length + extra;
        if (needed > INT32_MAX) {
            w->invalid = "values cannot exceed 2 GiB";
            return NULL;
        }
        uint64_t capacity = (uint64_t)w->capacity * 2;
        if (capacity < needed) capacity = needed;
        if (capacity < 256) capacity = 256;
        if (capacity > INT32_MAX) capacity = INT32_MAX;
        /* Allocation fails only when the host denies memory growth, which is
         * already a sticky budget failure. */
        uint8_t *data = realloc(w->data, capacity);
        if (!data) __builtin_trap();
        w->data = data;
        w->capacity = (uint32_t)capacity;
    }
    uint8_t *out = w->data + w->length;
    w->length += extra;
    return out;
}

static int flower_invalid(FlowerWriter *w, const char *message) {
    w->invalid = message;
    return -1;
}

static inline void flower_u32(uint8_t *out, uint32_t value) { memcpy(out, &value, 4); }

static int flower_head(FlowerWriter *w, uint8_t tag, uint32_t value) {
    uint8_t *out = flower_reserve(w, 5);
    if (!out) return -1;
    out[0] = tag;
    flower_u32(out + 1, value);
    return 0;
}

static int flower_write_string(FlowerWriter *w, JSString *p) {
    uint32_t bytes = p->is_wide_char ? p->len * 2 : p->len;
    uint8_t *out = flower_reserve(w, 5 + bytes);
    if (!out) return -1;
    out[0] = p->is_wide_char ? FLOWER_UTF16 : FLOWER_LATIN1;
    flower_u32(out + 1, p->len);
    memcpy(out + 5, strv(p), bytes);
    return 0;
}

/* A rope is 16-bit if any leaf is; its depth is bounded by rebalancing. */
static uint8_t *flower_copy_chars(uint8_t *out, JSValueConst value, bool wide) {
    if (JS_VALUE_GET_TAG(value) == JS_TAG_STRING_ROPE) {
        JSStringRope *rope = JS_VALUE_GET_STRING_ROPE(value);
        out = flower_copy_chars(out, rope->left, wide);
        return flower_copy_chars(out, rope->right, wide);
    }
    JSString *p = JS_VALUE_GET_STRING(value);
    if (!wide || p->is_wide_char) {
        size_t bytes = p->is_wide_char ? p->len * 2 : p->len;
        memcpy(out, strv(p), bytes);
        return out + bytes;
    }
    const uint8_t *chars = str8(p);
    for (uint32_t i = 0; i < p->len; ++i) {
        uint16_t unit = chars[i];
        memcpy(out + 2 * i, &unit, 2);
    }
    return out + 2 * p->len;
}

static int flower_write_rope(FlowerWriter *w, JSValueConst value) {
    JSStringRope *rope = JS_VALUE_GET_STRING_ROPE(value);
    uint8_t *out = flower_reserve(w, 5 + (rope->is_wide_char ? rope->len * 2 : rope->len));
    if (!out) return -1;
    out[0] = rope->is_wide_char ? FLOWER_UTF16 : FLOWER_LATIN1;
    flower_u32(out + 1, rope->len);
    flower_copy_chars(out + 5, value, rope->is_wide_char);
    return 0;
}

static int flower_write_key(FlowerWriter *w, JSContext *ctx, JSAtom atom) {
    uint32_t slot = (atom * UINT32_C(0x9e3779b1)) >> 22;
    for (;; slot = (slot + 1) & (FLOWER_KEY_SLOTS - 1)) {
        if (flower_key_slots[slot].generation != w->generation) break;
        if (flower_key_slots[slot].atom == atom)
            return flower_head(w, FLOWER_KEY, flower_key_slots[slot].index);
    }
    if (w->interned < FLOWER_KEY_SLOTS / 2) {
        flower_key_slots[slot].atom = atom;
        flower_key_slots[slot].index = w->next_key;
        flower_key_slots[slot].generation = w->generation;
        w->interned++;
    }
    w->next_key++;
    if (!__JS_AtomIsTaggedInt(atom))
        return flower_write_string(w, ctx->rt->atom_array[atom]);
    char digits[10];
    unsigned length = 0;
    for (uint32_t n = __JS_AtomToUInt32(atom); length == 0 || n; n /= 10)
        digits[sizeof(digits) - ++length] = '0' + n % 10;
    uint8_t *out = flower_reserve(w, 5 + length);
    if (!out) return -1;
    out[0] = FLOWER_LATIN1;
    flower_u32(out + 1, length);
    memcpy(out + 5, digits + sizeof(digits) - length, length);
    return 0;
}

static int flower_write_value(FlowerWriter *w, JSContext *ctx, JSValueConst value, unsigned depth);

static int flower_write_array(FlowerWriter *w, JSContext *ctx, JSObject *p, unsigned depth) {
    JSShape *shape = p->shape;
    JSShapeProperty *properties = get_shape_prop(shape);
    /* The pinned engine keeps length in slot zero of every array shape. */
    if (shape->prop_count == 0 || properties[0].atom != JS_ATOM_length
        || (properties[0].flags & JS_PROP_TMASK))
        return flower_invalid(w, "arrays must be plain arrays");
    JSValueConst length_value = p->prop[0].u.value;
    if (JS_VALUE_GET_TAG(length_value) != JS_TAG_INT || JS_VALUE_GET_INT(length_value) < 0)
        return flower_invalid(w, "arrays cannot contain holes");
    uint32_t length = (uint32_t)JS_VALUE_GET_INT(length_value);
    if (p->fast_array) {
        for (int i = 1; i < shape->prop_count; ++i)
            if (properties[i].atom != JS_ATOM_NULL)
                return flower_invalid(w, "arrays cannot contain named properties");
        if (length != p->u.array.count) return flower_invalid(w, "arrays cannot contain holes");
        if (flower_head(w, FLOWER_ARRAY, length) < 0) return -1;
        for (uint32_t i = 0; i < length; ++i)
            if (flower_write_value(w, ctx, p->u.array.u.values[i], depth + 1) < 0) return -1;
        return 0;
    }
    /* Slow arrays keep elements as indexed shape properties. */
    uint32_t elements = 0;
    for (int i = 1; i < shape->prop_count; ++i) {
        JSShapeProperty *property = &properties[i];
        if (property->atom == JS_ATOM_NULL) continue;
        if (!__JS_AtomIsTaggedInt(property->atom))
            return flower_invalid(w, "arrays cannot contain named properties");
        if (!(property->flags & JS_PROP_ENUMERABLE) || (property->flags & JS_PROP_TMASK))
            return flower_invalid(w, "values cannot contain symbols, hidden properties or accessors");
        elements++;
    }
    if (elements != length) return flower_invalid(w, "arrays cannot contain holes");
    if (flower_head(w, FLOWER_ARRAY, length) < 0) return -1;
    for (uint32_t i = 0; i < length; ++i) {
        JSProperty *property;
        if (!find_own_property(&property, p, __JS_AtomFromUInt32(i)))
            return flower_invalid(w, "arrays cannot contain holes");
        if (flower_write_value(w, ctx, property->u.value, depth + 1) < 0) return -1;
    }
    return 0;
}

static int flower_write_value(FlowerWriter *w, JSContext *ctx, JSValueConst value, unsigned depth) {
    if (depth > FLOWER_MAX_DEPTH) return flower_invalid(w, "values nest at most 128 levels");
    uint8_t *out;
    switch (JS_VALUE_GET_NORM_TAG(value)) {
    case JS_TAG_NULL:
        if (!(out = flower_reserve(w, 1))) return -1;
        out[0] = FLOWER_NULL;
        return 0;
    case JS_TAG_BOOL:
        if (!(out = flower_reserve(w, 1))) return -1;
        out[0] = JS_VALUE_GET_BOOL(value) ? FLOWER_TRUE : FLOWER_FALSE;
        return 0;
    case JS_TAG_INT:
        return flower_head(w, FLOWER_INT, (uint32_t)JS_VALUE_GET_INT(value));
    case JS_TAG_FLOAT64: {
        double number = JS_VALUE_GET_FLOAT64(value);
        if (!isfinite(number)) return flower_invalid(w, "numbers must be finite");
        if (!(out = flower_reserve(w, 9))) return -1;
        out[0] = FLOWER_FLOAT;
        memcpy(out + 1, &number, 8);
        return 0;
    }
    case JS_TAG_STRING:
        return flower_write_string(w, JS_VALUE_GET_STRING(value));
    case JS_TAG_STRING_ROPE:
        return flower_write_rope(w, value);
    case JS_TAG_OBJECT:
        break;
    default:
        return flower_invalid(w, "values must be null, booleans, finite numbers, strings, arrays or plain objects");
    }
    JSObject *p = JS_VALUE_GET_OBJ(value);
    for (unsigned i = 1; i < depth; ++i)
        if (w->active[i] == p) return flower_invalid(w, "values cannot contain cycles");
    w->active[depth] = p;
    if (p->class_id == JS_CLASS_ARRAY) return flower_write_array(w, ctx, p, depth);
    JSObject *prototype = p->shape->proto;
    if (p->class_id != JS_CLASS_OBJECT || p->is_exotic
        || (prototype && prototype != JS_VALUE_GET_OBJ(ctx->class_proto[JS_CLASS_OBJECT])))
        return flower_invalid(w, "values must be null, booleans, finite numbers, strings, arrays or plain objects");
    JSShape *shape = p->shape;
    JSShapeProperty *properties = get_shape_prop(shape);
    uint32_t head = w->length, count = 0;
    if (flower_head(w, FLOWER_MAP, 0) < 0) return -1;
    for (int i = 0; i < shape->prop_count; ++i) {
        JSShapeProperty *property = &properties[i];
        JSAtom key = property->atom;
        if (key == JS_ATOM_NULL) continue;
        if ((!__JS_AtomIsTaggedInt(key) && ctx->rt->atom_array[key]->atom_type != JS_ATOM_TYPE_STRING)
            || !(property->flags & JS_PROP_ENUMERABLE) || (property->flags & JS_PROP_TMASK))
            return flower_invalid(w, "values cannot contain symbols, hidden properties or accessors");
        if (flower_write_key(w, ctx, key) < 0
            || flower_write_value(w, ctx, p->prop[i].u.value, depth + 1) < 0) return -1;
        count++;
    }
    flower_u32(w->data + head + 1, count);
    return 0;
}

/* A failure outcome {code, message, details?}. Details come last so that
 * dropping unrepresentable details leaves no dangling key reference. */
static void flower_write_failure(FlowerWriter *w, JSContext *ctx, JSValueConst code,
                                 JSValueConst message, JSValueConst details) {
    static const char *const names[] = {"code", "message", "details"};
    uint8_t *out = flower_reserve(w, 1);
    if (!out) __builtin_trap();
    out[0] = FLOWER_FAILURE;
    uint32_t head = w->length;
    if (flower_head(w, FLOWER_MAP, 2) < 0) __builtin_trap();
    JSValueConst values[] = {code, message};
    for (unsigned i = 0; i < 2; ++i) {
        uint32_t length = (uint32_t)strlen(names[i]);
        if (flower_head(w, FLOWER_LATIN1, length) < 0 || !(out = flower_reserve(w, length)))
            __builtin_trap();
        memcpy(out, names[i], length);
        w->next_key++;
        if (flower_write_value(w, ctx, values[i], 2) < 0) __builtin_trap();
    }
    if (JS_IsUndefined(details)) return;
    uint32_t mark = w->length;
    if (flower_head(w, FLOWER_LATIN1, 7) < 0 || !(out = flower_reserve(w, 7))) __builtin_trap();
    memcpy(out, names[2], 7);
    w->next_key++;
    if (flower_write_value(w, ctx, details, 1) < 0) {
        w->length = mark;
        w->invalid = NULL;
        return;
    }
    flower_u32(w->data + head + 1, 3);
}

typedef struct {
    const uint8_t *data;
    uint32_t offset, length;
    JSAtom *keys;
    uint32_t key_count, key_capacity;
} FlowerReader;

static JSValue flower_malformed(JSContext *ctx) {
    return JS_ThrowTypeError(ctx, "malformed host value");
}

static bool flower_take(FlowerReader *r, uint32_t count, const uint8_t **bytes) {
    if (r->length - r->offset < count) return false;
    *bytes = r->data + r->offset;
    r->offset += count;
    return true;
}

static bool flower_read_u32(FlowerReader *r, uint32_t *value) {
    const uint8_t *bytes;
    if (!flower_take(r, 4, &bytes)) return false;
    memcpy(value, bytes, 4);
    return true;
}

static JSValue flower_read_string(JSContext *ctx, FlowerReader *r, uint8_t tag) {
    uint32_t length;
    const uint8_t *bytes;
    if (!flower_read_u32(r, &length) || length > JS_STRING_LEN_MAX
        || !flower_take(r, tag == FLOWER_UTF16 ? length * 2 : length, &bytes))
        return flower_malformed(ctx);
    if (length == 0) return JS_AtomToString(ctx, JS_ATOM_empty_string);
    switch (tag) {
    case FLOWER_LATIN1:
        return js_new_string8_len(ctx, (const char *)bytes, (int)length);
    case FLOWER_UTF16: {
        JSString *string = js_alloc_string(ctx, (int)length, 1);
        if (!string) return JS_EXCEPTION;
        memcpy(str16(string), bytes, length * 2);
        return JS_MKPTR(JS_TAG_STRING, string);
    }
    default:
        return JS_NewStringLen(ctx, (const char *)bytes, length);
    }
}

static bool flower_ascii(const uint8_t *bytes, uint32_t length) {
    uint8_t high = 0;
    for (uint32_t i = 0; i < length; ++i) high |= bytes[i];
    return high < 0x80;
}

/* Returns an atom borrowed from the reader's table. */
static JSAtom flower_read_key(JSContext *ctx, FlowerReader *r) {
    const uint8_t *tag;
    if (!flower_take(r, 1, &tag)) goto malformed;
    if (*tag == FLOWER_KEY) {
        uint32_t index;
        if (!flower_read_u32(r, &index) || index >= r->key_count) goto malformed;
        return r->keys[index];
    }
    if (*tag != FLOWER_UTF8 && *tag != FLOWER_LATIN1 && *tag != FLOWER_UTF16) goto malformed;
    JSAtom atom;
    uint32_t length;
    const uint8_t *bytes;
    uint32_t start = r->offset;
    if (*tag != FLOWER_UTF16 && flower_read_u32(r, &length) && flower_take(r, length, &bytes)
        && flower_ascii(bytes, length)) {
        /* Finds existing atoms without allocating. Only for ASCII: the pinned
         * lookup compares raw bytes against Latin-1 atoms. */
        atom = JS_NewAtomLen(ctx, (const char *)bytes, length);
    } else {
        r->offset = start;
        JSValue string = flower_read_string(ctx, r, *tag);
        if (JS_IsException(string)) return JS_ATOM_NULL;
        atom = JS_NewAtomStr(ctx, JS_VALUE_GET_STRING(string));
    }
    if (atom == JS_ATOM_NULL) return JS_ATOM_NULL;
    if (r->key_count == r->key_capacity) {
        uint32_t capacity = r->key_capacity ? r->key_capacity * 2 : 16;
        JSAtom *keys = realloc(r->keys, capacity * sizeof(*keys));
        if (!keys) __builtin_trap();
        r->keys = keys;
        r->key_capacity = capacity;
    }
    r->keys[r->key_count++] = atom;
    return atom;
malformed:
    flower_malformed(ctx);
    return JS_ATOM_NULL;
}

static JSValue flower_read_value(JSContext *ctx, FlowerReader *r, unsigned depth) {
    const uint8_t *bytes;
    if (depth > FLOWER_MAX_DEPTH || !flower_take(r, 1, &bytes)) return flower_malformed(ctx);
    uint8_t tag = *bytes;
    switch (tag) {
    case FLOWER_NULL: return JS_NULL;
    case FLOWER_FALSE: return JS_FALSE;
    case FLOWER_TRUE: return JS_TRUE;
    case FLOWER_INT: {
        uint32_t value;
        if (!flower_read_u32(r, &value)) return flower_malformed(ctx);
        return js_int32((int32_t)value);
    }
    case FLOWER_FLOAT: {
        double value;
        if (!flower_take(r, 8, &bytes)) return flower_malformed(ctx);
        memcpy(&value, bytes, 8);
        return js_number(value);
    }
    case FLOWER_UTF8:
    case FLOWER_LATIN1:
    case FLOWER_UTF16:
        return flower_read_string(ctx, r, tag);
    case FLOWER_ARRAY: {
        uint32_t count;
        if (!flower_read_u32(r, &count) || count > r->length - r->offset)
            return flower_malformed(ctx);
        /* Elements start as undefined, so a collection mid-decode is safe. */
        JSValue array = js_allocate_fast_array(ctx, count);
        if (JS_IsException(array)) return array;
        JSObject *p = JS_VALUE_GET_OBJ(array);
        for (uint32_t i = 0; i < count; ++i) {
            JSValue item = flower_read_value(ctx, r, depth + 1);
            if (JS_IsException(item)) {
                JS_FreeValue(ctx, array);
                return item;
            }
            p->u.array.u.values[i] = item;
        }
        return array;
    }
    case FLOWER_MAP: {
        uint32_t count;
        if (!flower_read_u32(r, &count) || count > r->length - r->offset)
            return flower_malformed(ctx);
        JSValue object = JS_NewObject(ctx);
        if (JS_IsException(object)) return object;
        for (uint32_t i = 0; i < count; ++i) {
            JSAtom key = flower_read_key(ctx, r);
            JSValue item = key == JS_ATOM_NULL ? JS_EXCEPTION : flower_read_value(ctx, r, depth + 1);
            if (JS_IsException(item)
                || JS_DefinePropertyValue(ctx, object, key, item, JS_PROP_C_W_E) < 0) {
                JS_FreeValue(ctx, object);
                return JS_EXCEPTION;
            }
        }
        return object;
    }
    default:
        return flower_malformed(ctx);
    }
}

static void flower_reader_end(JSContext *ctx, FlowerReader *r) {
    for (uint32_t i = 0; i < r->key_count; ++i) JS_FreeAtom(ctx, r->keys[i]);
    free(r->keys);
}

/* Decode one complete value (invocation arguments). */
static JSValue flower_decode(JSContext *ctx, const uint8_t *data, uint32_t length) {
    FlowerReader r = {.data = data, .length = length};
    JSValue value = flower_read_value(ctx, &r, 1);
    if (!JS_IsException(value) && r.offset != length) {
        JS_FreeValue(ctx, value);
        value = flower_malformed(ctx);
    }
    flower_reader_end(ctx, &r);
    return value;
}

static JSValue flower_error(JSContext *ctx, JSValue code, JSValue message) {
    JSValue error = JS_NewError(ctx);
    if (JS_IsException(error)) {
        JS_FreeValue(ctx, code);
        JS_FreeValue(ctx, message);
        return error;
    }
    JS_DefinePropertyValue(ctx, error, JS_ATOM_message, message,
                           JS_PROP_WRITABLE | JS_PROP_CONFIGURABLE);
    JS_DefinePropertyValueStr(ctx, error, "code", code, JS_PROP_C_W_E);
    return JS_Throw(ctx, error);
}

/* Decode a host outcome: return its value or throw its failure. */
static JSValue flower_read_outcome(JSContext *ctx, const uint8_t *data, uint32_t length) {
    if (length == 0) return flower_malformed(ctx);
    JSValue value = flower_decode(ctx, data + 1, length - 1);
    if (JS_IsException(value) || data[0] == FLOWER_SUCCESS) return value;
    if (data[0] != FLOWER_FAILURE || !JS_IsObject(value)) {
        JS_FreeValue(ctx, value);
        return flower_malformed(ctx);
    }
    /* A freshly decoded plain object: these are own data properties. */
    JSValue code = JS_GetPropertyStr(ctx, value, "code");
    JSValue message = JS_GetPropertyStr(ctx, value, "message");
    JS_FreeValue(ctx, value);
    return flower_error(ctx, code, message);
}

static JSValue flower_throw_invalid(JSContext *ctx, const char *message) {
    return flower_error(ctx, JS_NewString(ctx, "INVALID_VALUE"), JS_NewString(ctx, message));
}

__attribute__((import_module("flower"), import_name("host_call")))
extern uint64_t flower_host_call(int32_t op, const uint8_t *payload, uint32_t length);
extern int flower_crypto_callback_active;

static uint64_t flower_pack(FlowerWriter *w) {
    return (uint32_t)(uintptr_t)w->data | ((uint64_t)w->length << 32);
}

/* host(op, ...args): the only database capability, captured by the runner. */
static JSValue flower_host(JSContext *ctx, JSValueConst this_value, int argc, JSValueConst *argv) {
    (void)this_value;
    if (argc < 1 || JS_VALUE_GET_TAG(argv[0]) != JS_TAG_INT)
        return JS_ThrowTypeError(ctx, "invalid host operation");
    FlowerWriter *w = flower_writer_begin();
    for (int i = 1; i < argc; ++i)
        if (flower_write_value(w, ctx, argv[i], 1) < 0) return flower_throw_invalid(ctx, w->invalid);
    uint64_t response = flower_host_call(JS_VALUE_GET_INT(argv[0]), w->data, w->length);
    /* The host allocated the response with flower_alloc; the guest owns it. */
    uint8_t *pointer = (uint8_t *)(uintptr_t)(uint32_t)response;
    JSValue value = flower_read_outcome(ctx, pointer, (uint32_t)(response >> 32));
    free(pointer);
    return value;
}

int flower_wire_init(JSContext *ctx) {
    JSValue global = JS_GetGlobalObject(ctx);
    int result = JS_DefinePropertyValueStr(ctx, global, "__flowerHost",
        JS_NewCFunction(ctx, flower_host, "__flowerHost", 1), JS_PROP_CONFIGURABLE);
    JS_FreeValue(ctx, global);
    return result < 0 ? -1 : 0;
}

/* Encode a completed call as an outcome: its result, or its exception as
 * described by describe(kind, error) => [code, message, details]. The result
 * graph is not freed: this is the heap's last use before the host discards or
 * resets it. */
static uint64_t flower_outcome(JSContext *ctx, JSValue result, JSValueConst describe, int32_t kind) {
    FlowerWriter *w;
    if (!JS_IsException(result)) {
        w = flower_writer_begin();
        *flower_reserve(w, 1) = FLOWER_SUCCESS;
        if (flower_write_value(w, ctx, result, 1) == 0) return flower_pack(w);
        JSValue message = JS_NewString(ctx, w->invalid);
        JSValue code = JS_NewString(ctx, "INVALID_VALUE");
        w = flower_writer_begin();
        flower_write_failure(w, ctx, code, message, JS_UNDEFINED);
        return flower_pack(w);
    }
    JSValue exception[2] = {js_int32(kind), JS_GetException(ctx)};
    JSValue record = JS_Call(ctx, describe, JS_UNDEFINED, 2, exception);
    w = flower_writer_begin();
    if (JS_IsException(record)) {
        JSValue error = JS_GetException(ctx);
        JSValue message = JS_ToString(ctx, error);
        if (JS_IsException(message)) {
            JS_FreeValue(ctx, JS_GetException(ctx));
            message = JS_NewString(ctx, "unreadable QuickJS exception");
        }
        flower_write_failure(w, ctx, JS_NewString(ctx, "EVALUATION_BUDGET"), message, JS_UNDEFINED);
        return flower_pack(w);
    }
    JSObject *p = JS_VALUE_GET_OBJ(record);
    if (JS_VALUE_GET_TAG(record) != JS_TAG_OBJECT || p->class_id != JS_CLASS_ARRAY
        || !p->fast_array || p->u.array.count != 3) __builtin_trap();
    JSValue *fields = p->u.array.u.values;
    flower_write_failure(w, ctx, fields[0], fields[1], fields[2]);
    return flower_pack(w);
}

/* Run one callback and return its outcome. */
uint64_t flower_wire_invoke(JSContext *ctx, JSValueConst run, JSValueConst describe,
                            int32_t kind, const char *name, uint32_t name_length,
                            const uint8_t *args, uint32_t args_length) {
    JSValue argv[3] = {js_int32(kind), JS_NewStringLen(ctx, name, name_length), JS_UNDEFINED};
    JSValue result = JS_EXCEPTION;
    flower_crypto_callback_active = 1;
    if (!JS_IsException(argv[1])) {
        argv[2] = flower_decode(ctx, args, args_length);
        if (!JS_IsException(argv[2])) result = JS_Call(ctx, run, JS_UNDEFINED, 3, argv);
    }
    uint64_t outcome = flower_outcome(ctx, result, describe, kind);
    flower_crypto_callback_active = 0;
    return outcome;
}

/* Derived kind: manifest failures never carry details. */
uint64_t flower_wire_manifest(JSContext *ctx, JSValueConst manifest, JSValueConst describe) {
    return flower_outcome(ctx, JS_Call(ctx, manifest, JS_UNDEFINED, 0, NULL), describe, 3);
}
