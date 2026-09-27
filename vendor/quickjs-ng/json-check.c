/* Included after pinned upstream/quickjs.c. Snapshots of the intrinsics that
 * the SDK's canonical JSON fallback calls: its native fast path is used only
 * while they are unchanged. SPDX-License-Identifier: MIT */

typedef struct {
    JSValue object;
    JSValue value;
    JSAtom key;
} FlowerJsonGuard;

typedef struct {
    JSValue object;
    JSValue prototype;
} FlowerPrototypeGuard;

static FlowerJsonGuard flower_json_guards[32];
static unsigned flower_json_guard_count;
static FlowerPrototypeGuard flower_json_prototypes[8];
static unsigned flower_json_prototype_count;
static JSAtom flower_json_globals[6];
static JSValue flower_json_object_prototype;

/* Kept alive by the duplicated JSValues in our process-lifetime pristine image.
 * No guard evaluates accessors, proxy traps, or application code. */
static int flower_json_capture(JSContext *ctx, JSValueConst object, JSAtom key) {
    if (flower_json_guard_count == countof(flower_json_guards)) abort();
    JSValue value = JS_GetProperty(ctx, object, key);
    if (JS_IsException(value)) return -1;
    FlowerJsonGuard *guard = &flower_json_guards[flower_json_guard_count++];
    guard->object = JS_DupValue(ctx, object);
    guard->value = value;
    guard->key = JS_DupAtom(ctx, key);
    return 0;
}

static int flower_json_capture_name(JSContext *ctx, JSValueConst object, const char *name) {
    JSAtom key = JS_NewAtom(ctx, name);
    if (key == JS_ATOM_NULL) return -1;
    int result = flower_json_capture(ctx, object, key);
    JS_FreeAtom(ctx, key);
    return result;
}

static bool flower_json_same(JSValueConst first, JSValueConst second) {
    /* Every captured guard value is an object/function. */
    return JS_VALUE_GET_TAG(first) == JS_TAG_OBJECT
        && JS_VALUE_GET_TAG(second) == JS_TAG_OBJECT
        && JS_VALUE_GET_OBJ(first) == JS_VALUE_GET_OBJ(second);
}

static bool flower_json_intrinsics_unchanged(JSContext *ctx) {
    JSProperty *property;
    /* `let Set = ...` lives in a separate lexical environment, not globalThis.
     * Any such binding conservatively selects the full original JS validator. */
    JSObject *lexicals = JS_VALUE_GET_OBJ(ctx->global_var_obj);
    for (unsigned i = 0; i < countof(flower_json_globals); ++i)
        if (find_own_property(&property, lexicals, flower_json_globals[i])) return false;
    for (unsigned i = 0; i < flower_json_guard_count; ++i) {
        FlowerJsonGuard *guard = &flower_json_guards[i];
        JSShapeProperty *shape = find_own_property(&property,
            JS_VALUE_GET_OBJ(guard->object), guard->key);
        if (!shape || (shape->flags & JS_PROP_TMASK)
            || !flower_json_same(property->u.value, guard->value)) return false;
    }
    /* The old `"value" in descriptor` observes Object.prototype pollution. */
    if (find_own_property(&property, JS_VALUE_GET_OBJ(flower_json_object_prototype),
                         JS_ATOM_value)) return false;
    /* Reflect.ownKeys is consumed with for-of. Preserve monkeypatched iterator
     * closure behavior and inherited return hooks by falling back entirely. */
    for (unsigned i = 0; i < flower_json_prototype_count; ++i) {
        FlowerPrototypeGuard *guard = &flower_json_prototypes[i];
        JSObject *object = JS_VALUE_GET_OBJ(guard->object);
        if (object->shape->proto != (JS_IsNull(guard->prototype)
                                    ? NULL : JS_VALUE_GET_OBJ(guard->prototype))) return false;
        if (find_own_property(&property, object, JS_ATOM_return)) return false;
    }
    return true;
}

int flower_json_init(JSContext *ctx) {
    JSValue global = JS_GetGlobalObject(ctx);
    const char *names[] = {"Set", "Object", "Array", "Number", "Reflect", "Error"};
    JSValue builtins[countof(names)];
    for (unsigned i = 0; i < countof(names); ++i) {
        flower_json_globals[i] = JS_NewAtom(ctx, names[i]);
        if (flower_json_globals[i] == JS_ATOM_NULL) return -1;
        if (flower_json_capture(ctx, global, flower_json_globals[i]) < 0) return -1;
        builtins[i] = JS_GetProperty(ctx, global, flower_json_globals[i]);
        if (JS_IsException(builtins[i])) return -1;
    }
    JSValue set_prototype = JS_GetPropertyStr(ctx, builtins[0], "prototype");
    flower_json_object_prototype = JS_GetPropertyStr(ctx, builtins[1], "prototype");
    JSValue array_prototype = JS_GetPropertyStr(ctx, builtins[2], "prototype");
    if (JS_IsException(set_prototype) || JS_IsException(flower_json_object_prototype)
        || JS_IsException(array_prototype)) return -1;
    for (unsigned i = 0; i < 3; ++i)
        if (flower_json_capture_name(ctx, builtins[i], "prototype") < 0) return -1;
    for (unsigned i = 0; i < 3; ++i)
        if (flower_json_capture_name(ctx, set_prototype, (const char *[]){"has", "add", "delete"}[i]) < 0) return -1;
    for (unsigned i = 0; i < 3; ++i)
        if (flower_json_capture_name(ctx, builtins[1], (const char *[]){"getPrototypeOf", "getOwnPropertyDescriptor", "hasOwn"}[i]) < 0) return -1;
    if (flower_json_capture_name(ctx, builtins[2], "isArray") < 0
        || flower_json_capture_name(ctx, builtins[3], "isFinite") < 0
        || flower_json_capture_name(ctx, builtins[4], "ownKeys") < 0
        || flower_json_capture(ctx, array_prototype, JS_ATOM_Symbol_iterator) < 0) return -1;
    JSValue empty = JS_NewArray(ctx);
    JSValue iterator = JS_Invoke(ctx, empty, JS_ATOM_Symbol_iterator, 0, NULL);
    JS_FreeValue(ctx, empty);
    if (JS_IsException(iterator)) return -1;
    JSValue prototype = JS_GetPrototype(ctx, iterator);
    JS_FreeValue(ctx, iterator);
    if (JS_IsException(prototype)) return -1;
    if (flower_json_capture_name(ctx, prototype, "next") < 0) return -1;
    while (!JS_IsNull(prototype)) {
        if (flower_json_prototype_count == countof(flower_json_prototypes)) abort();
        JSValue parent = JS_GetPrototype(ctx, prototype);
        if (JS_IsException(parent)) return -1;
        FlowerPrototypeGuard *guard = &flower_json_prototypes[flower_json_prototype_count++];
        guard->object = prototype;
        guard->prototype = JS_DupValue(ctx, parent);
        prototype = parent;
    }
    JS_FreeValue(ctx, prototype);
    JS_FreeValue(ctx, array_prototype);
    JS_FreeValue(ctx, set_prototype);
    for (unsigned i = 0; i < countof(builtins); ++i) JS_FreeValue(ctx, builtins[i]);
    JS_FreeValue(ctx, global);
    return 0;
}
