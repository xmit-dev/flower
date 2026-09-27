/* Included after pinned upstream/quickjs.c and json-check.c.
 * SPDX-License-Identifier: MIT */

static FlowerJsonGuard flower_canonical_guards[17];
static unsigned flower_canonical_guard_count;
static JSAtom flower_canonical_globals[3];
static JSValue flower_canonical_object_prototype;

/* Guards 0-3 cover scalars, 0-6 arrays of scalars and all of them compound
 * values, matching the intrinsics each SDK fallback path calls. */
enum { FLOWER_CANONICAL_SCALAR = 4, FLOWER_CANONICAL_SCALARS = 7 };

static int flower_canonical_capture_atom(JSContext *ctx, JSValueConst object, JSAtom key) {
    if (flower_canonical_guard_count == countof(flower_canonical_guards)) abort();
    JSValue value = JS_GetProperty(ctx, object, key);
    if (JS_IsException(value)) return -1;
    FlowerJsonGuard *guard = &flower_canonical_guards[flower_canonical_guard_count++];
    guard->object = JS_DupValue(ctx, object);
    guard->value = value;
    guard->key = JS_DupAtom(ctx, key);
    return 0;
}

static int flower_canonical_capture(JSContext *ctx, JSValueConst object,
                                     const char *name) {
    if (flower_canonical_guard_count == countof(flower_canonical_guards)) abort();
    JSAtom key = JS_NewAtom(ctx, name);
    if (key == JS_ATOM_NULL) return -1;
    JSValue value = JS_GetProperty(ctx, object, key);
    if (JS_IsException(value)) {
        JS_FreeAtom(ctx, key);
        return -1;
    }
    FlowerJsonGuard *guard = &flower_canonical_guards[flower_canonical_guard_count++];
    guard->object = JS_DupValue(ctx, object);
    guard->value = value;
    guard->key = key;
    return 0;
}

static bool flower_canonical_intrinsics_unchanged(JSContext *ctx, unsigned count) {
    JSProperty *property;
    JSObject *lexicals = JS_VALUE_GET_OBJ(ctx->global_var_obj);
    for (unsigned i = 0; i < countof(flower_canonical_globals); ++i)
        if (find_own_property(&property, lexicals, flower_canonical_globals[i])) return false;
    /* Scalars use only JSON.stringify and Number.isFinite. Arrays additionally
     * use the descriptor/iterator intrinsics guarded by the JSON validator, and
     * objects the Map, sorting and array-building intrinsics captured here. */
    bool array = count > FLOWER_CANONICAL_SCALAR;
    if (count > flower_canonical_guard_count) {
        count = flower_canonical_guard_count;
        /* Array.from({length}) consults an inherited iterator first. */
        if (find_own_property(&property, JS_VALUE_GET_OBJ(flower_canonical_object_prototype),
                              JS_ATOM_Symbol_iterator)) return false;
    }
    for (unsigned i = 0; i < count; ++i) {
        FlowerJsonGuard *guard = &flower_canonical_guards[i];
        JSShapeProperty *shape = find_own_property(&property,
            JS_VALUE_GET_OBJ(guard->object), guard->key);
        if (!shape || (shape->flags & JS_PROP_TMASK)
            || !flower_json_same(property->u.value, guard->value)) return false;
    }
    return !array || flower_json_intrinsics_unchanged(ctx);
}

static bool flower_canonical_scalar(JSValueConst value) {
    switch (JS_VALUE_GET_NORM_TAG(value)) {
    case JS_TAG_NULL:
    case JS_TAG_BOOL:
    case JS_TAG_INT:
    case JS_TAG_STRING:
    case JS_TAG_STRING_ROPE:
        return true;
    case JS_TAG_FLOAT64:
        return isfinite(JS_VALUE_GET_FLOAT64(value));
    default:
        return false;
    }
}

/* A fast array without holes or named properties: its length slot agrees with
 * its contiguous elements and nothing else is live in its shape. */
static bool flower_canonical_plain_array(JSObject *object) {
    if (object->class_id != JS_CLASS_ARRAY || !object->fast_array) return false;
    JSShape *shape = object->shape;
    JSShapeProperty *properties = get_shape_prop(shape);
    if (shape->prop_count == 0 || properties[0].atom != JS_ATOM_length
        || (properties[0].flags & JS_PROP_TMASK)) return false;
    JSValueConst length = object->prop[0].u.value;
    if (JS_VALUE_GET_TAG(length) != JS_TAG_INT || JS_VALUE_GET_INT(length) < 0
        || (uint32_t)JS_VALUE_GET_INT(length) != object->u.array.count) return false;
    for (int i = 1; i < shape->prop_count; ++i)
        if (properties[i].atom != JS_ATOM_NULL) return false;
    return true;
}

typedef struct {
    JSString *name;
    JSValue string;
    JSValueConst value;
} FlowerCanonicalField;

/* Encode what the SDK fallback would: 0 done, 1 run the fallback instead
 * (including every error it reports), -1 engine exception. Only raw slots are
 * read, so abandoning a partial encoding has no observable effect. */
static int flower_canonical_write(JSContext *ctx, JSONStringifyContext *stringify,
                                  JSValueConst value, unsigned depth, JSObject **active) {
    if (depth > 128) return 1;
    if (flower_canonical_scalar(value))
        return js_json_to_str(ctx, stringify, JS_UNDEFINED, JS_DupValue(ctx, value), JS_UNDEFINED) ? -1 : 0;
    if (JS_VALUE_GET_TAG(value) != JS_TAG_OBJECT) return 1;
    JSObject *object = JS_VALUE_GET_OBJ(value);
    for (unsigned i = 0; i < depth; ++i)
        if (active[i] == object) return 1;
    active[depth] = object;
    StringBuffer *buffer = stringify->b;
    if (flower_canonical_plain_array(object)) {
        string_buffer_putc8(buffer, '[');
        for (uint32_t i = 0; i < object->u.array.count; ++i) {
            if (i) string_buffer_putc8(buffer, ',');
            int status = flower_canonical_write(ctx, stringify, object->u.array.u.values[i], depth + 1, active);
            if (status) return status;
        }
        string_buffer_putc8(buffer, ']');
        return 0;
    }
    if (object->class_id != JS_CLASS_OBJECT || object->is_exotic
        || (object->shape->proto && object->shape->proto != JS_VALUE_GET_OBJ(flower_canonical_object_prototype)))
        return 1;
    JSShape *shape = object->shape;
    JSShapeProperty *properties = get_shape_prop(shape);
    FlowerCanonicalField inline_fields[16], *fields = inline_fields;
    if (shape->prop_count > (int)countof(inline_fields)) {
        fields = js_malloc(ctx, sizeof(*fields) * shape->prop_count);
        if (!fields) return -1;
    }
    int count = 0, status = 0;
    for (int i = 0; i < shape->prop_count && !status; ++i) {
        JSShapeProperty *property = &properties[i];
        JSAtom key = property->atom;
        if (key == JS_ATOM_NULL) continue;
        if ((!__JS_AtomIsTaggedInt(key) && ctx->rt->atom_array[key]->atom_type != JS_ATOM_TYPE_STRING)
            || !(property->flags & JS_PROP_ENUMERABLE) || (property->flags & JS_PROP_TMASK)) {
            status = 1;
            break;
        }
        JSValue string = JS_AtomToString(ctx, key);
        if (JS_IsException(string)) {
            status = -1;
            break;
        }
        /* Insertion sort in UTF-16 code unit order, as Array.prototype.sort. */
        FlowerCanonicalField field = {JS_VALUE_GET_STRING(string), string, object->prop[i].u.value};
        int at = count++;
        while (at > 0 && js_string_compare(fields[at - 1].name, field.name) > 0) {
            fields[at] = fields[at - 1];
            at--;
        }
        fields[at] = field;
    }
    if (!status) {
        string_buffer_putc8(buffer, '{');
        for (int i = 0; i < count && !status; ++i) {
            if (i) string_buffer_putc8(buffer, ',');
            if (js_json_to_str(ctx, stringify, JS_UNDEFINED, JS_DupValue(ctx, fields[i].string), JS_UNDEFINED)) {
                status = -1;
                break;
            }
            string_buffer_putc8(buffer, ':');
            status = flower_canonical_write(ctx, stringify, fields[i].value, depth + 1, active);
        }
        if (!status) string_buffer_putc8(buffer, '}');
    }
    for (int i = 0; i < count; ++i) JS_FreeValue(ctx, fields[i].string);
    if (fields != inline_fields) js_free(ctx, fields);
    return status;
}

/* A string is a complete encoding; undefined means run the original SDK code.
 * All eligibility and intrinsic checks are raw slot reads. Fallback therefore
 * cannot run a getter, proxy trap, iterator, toJSON hook, or user conversion. */
static JSValue flower_canonical_json(JSContext *ctx, JSValueConst this_value,
                                     int argc, JSValueConst *argv) {
    (void)this_value;
    if (argc != 1) return JS_UNDEFINED;
    JSValueConst value = argv[0];
    if (flower_canonical_scalar(value)) {
        if (!flower_canonical_intrinsics_unchanged(ctx, FLOWER_CANONICAL_SCALAR)) return JS_UNDEFINED;
        int tag = JS_VALUE_GET_NORM_TAG(value);
        return tag == JS_TAG_STRING || tag == JS_TAG_STRING_ROPE
            ? JS_ToQuotedString(ctx, value) : JS_ToString(ctx, value);
    }
    if (JS_VALUE_GET_TAG(value) != JS_TAG_OBJECT) return JS_UNDEFINED;
    /* Arrays of scalars take the fallback's concatenation path; anything else
     * its Map, sort and Array.from path, with more intrinsics to guard. */
    JSObject *object = JS_VALUE_GET_OBJ(value);
    bool scalars = flower_canonical_plain_array(object);
    for (uint32_t i = 0; scalars && i < object->u.array.count; ++i)
        scalars = flower_canonical_scalar(object->u.array.u.values[i]);
    if (!flower_canonical_intrinsics_unchanged(ctx, scalars ? FLOWER_CANONICAL_SCALARS : UINT32_MAX))
        return JS_UNDEFINED;
    StringBuffer buffer;
    if (string_buffer_init(ctx, &buffer, 64)) return JS_EXCEPTION;
    JSONStringifyContext stringify = { .b = &buffer };
    /* Each ancestor slot is set before recursion; unused slots are never read.
     * The pinned engine's primitive JSON emitter gives identical number
     * spelling, negative zero and well-formed UTF-16/surrogate escaping. */
    JSObject *active[129];
    int status = flower_canonical_write(ctx, &stringify, value, 0, active);
    if (status) {
        string_buffer_free(&buffer);
        return status < 0 ? JS_EXCEPTION : JS_UNDEFINED;
    }
    return string_buffer_end(&buffer);
}

int flower_canonical_init(JSContext *ctx) {
    JSValue global = JS_GetGlobalObject(ctx);
    JSValue json = JS_GetPropertyStr(ctx, global, "JSON");
    JSValue number = JS_GetPropertyStr(ctx, global, "Number");
    JSValue object = JS_GetPropertyStr(ctx, global, "Object");
    if (JS_IsException(json) || JS_IsException(number) || JS_IsException(object)) return -1;
    for (unsigned i = 0; i < countof(flower_canonical_globals); ++i) {
        flower_canonical_globals[i] = JS_NewAtom(ctx, (const char *[]){"JSON", "Number", "Map"}[i]);
        if (flower_canonical_globals[i] == JS_ATOM_NULL) return -1;
    }
    if (flower_canonical_capture(ctx, global, "JSON") < 0
        || flower_canonical_capture(ctx, json, "stringify") < 0
        || flower_canonical_capture(ctx, global, "Number") < 0
        || flower_canonical_capture(ctx, number, "isFinite") < 0
        || flower_canonical_capture(ctx, object, "create") < 0
        || flower_canonical_capture(ctx, ctx->class_proto[JS_CLASS_REGEXP], "test") < 0
        || flower_canonical_capture(ctx, ctx->class_proto[JS_CLASS_REGEXP], "exec") < 0) return -1;
    /* The fallback's object path: new Map, set/get/keys, Array.from over the
     * key iterator, sort, map and join. */
    JSValue map = JS_GetPropertyStr(ctx, global, "Map");
    JSValue array = JS_GetPropertyStr(ctx, global, "Array");
    if (JS_IsException(map) || JS_IsException(array)) return -1;
    JSValue map_prototype = ctx->class_proto[JS_CLASS_MAP];
    JSValue array_prototype = ctx->class_proto[JS_CLASS_ARRAY];
    JSValue instance = JS_CallConstructor(ctx, map, 0, NULL);
    JSValue keys = JS_GetPropertyStr(ctx, map_prototype, "keys");
    JSValue iterator = JS_IsException(instance) || JS_IsException(keys)
        ? JS_EXCEPTION : JS_Call(ctx, keys, instance, 0, NULL);
    JS_FreeValue(ctx, keys);
    JS_FreeValue(ctx, instance);
    if (JS_IsException(iterator)) return -1;
    JSValue map_iterator = JS_GetPrototype(ctx, iterator);
    JSValue iterator_prototype = JS_GetPrototype(ctx, map_iterator);
    flower_canonical_object_prototype = JS_DupValue(ctx, ctx->class_proto[JS_CLASS_OBJECT]);
    if (JS_IsException(map_iterator) || JS_IsException(iterator_prototype)
        || flower_canonical_capture(ctx, global, "Map") < 0
        || flower_canonical_capture(ctx, map_prototype, "set") < 0
        || flower_canonical_capture(ctx, map_prototype, "get") < 0
        || flower_canonical_capture(ctx, map_prototype, "keys") < 0
        || flower_canonical_capture(ctx, array, "from") < 0
        || flower_canonical_capture(ctx, array_prototype, "sort") < 0
        || flower_canonical_capture(ctx, array_prototype, "map") < 0
        || flower_canonical_capture(ctx, array_prototype, "join") < 0
        || flower_canonical_capture(ctx, map_iterator, "next") < 0
        || flower_canonical_capture_atom(ctx, iterator_prototype, JS_ATOM_Symbol_iterator) < 0) return -1;
    JS_FreeValue(ctx, iterator);
    JS_FreeValue(ctx, map_iterator);
    JS_FreeValue(ctx, iterator_prototype);
    JS_FreeValue(ctx, map);
    JS_FreeValue(ctx, array);
    JS_FreeValue(ctx, json);
    JS_FreeValue(ctx, number);
    JS_FreeValue(ctx, object);
    /* Permanent SDK capability, installed before application code. Unlike the
     * temporary runner setup API this is intentionally callable by the SDK;
     * a nonconfigurable, nonwritable binding cannot be replaced or shadowed by
     * a later global lexical declaration. */
    int result = JS_DefinePropertyValueStr(ctx, global, "__flowerCanonicalJson",
        JS_NewCFunction(ctx, flower_canonical_json, "__flowerCanonicalJson", 1), 0);
    JS_FreeValue(ctx, global);
    return result < 0 ? -1 : 0;
}
