/* Immortal image blocks: flower_snapshot_prepare makes every block alive in
 * the heap immortal just before the host captures a reusable image, so that
 * evaluations started from the image stop writing reference counts into it
 * (see JS_REF_IMMORTAL in upstream/quickjs.c). Included after the engine, it
 * reads the runtime's private allocator, atom and collector state.
 * SPDX-License-Identifier: MIT */

/* The image's collectable objects, off the collector's list: they are never
 * freed, and an evaluation's collection walks only what it allocated. */
static struct list_head flower_immortal_objects = LIST_HEAD_INIT(flower_immortal_objects);

static void flower_immortal(const void *block)
{
    JS_REF_COUNT(block) = JS_REF_IMMORTAL;
}

/* Strings, ropes and big integers the image's objects hold, whatever their
 * size: large ones live outside the arenas walked below. */
static void flower_immortal_value(JSValueConst v)
{
    switch (JS_VALUE_GET_TAG(v)) {
    case JS_TAG_STRING: {
        JSString *p = JS_VALUE_GET_STRING(v);
        flower_immortal(p);
        if (p->kind == JS_STRING_KIND_SLICE)
            flower_immortal(((JSStringSlice *)&p[1])->parent);
        break;
    }
    case JS_TAG_STRING_ROPE: {
        JSStringRope *r = JS_VALUE_GET_STRING_ROPE(v);
        flower_immortal(r);
        flower_immortal_value(r->left);
        flower_immortal_value(r->right);
        break;
    }
    case JS_TAG_BIG_INT:
        flower_immortal(JS_VALUE_GET_PTR(v));
        break;
    default:
        break;
    }
}

static void flower_immortal_values(JSGCObjectHeader *gp)
{
    uint32_t i;
    switch (JS_GC_TYPE(gp)) {
    case JS_GC_OBJ_TYPE_JS_OBJECT: {
        JSObject *p = (JSObject *)gp;
        JSShapeProperty *prs = get_shape_prop(p->shape);
        for (i = 0; i < p->shape->prop_count; i++, prs++) {
            if (prs->atom != JS_ATOM_NULL && !(prs->flags & JS_PROP_TMASK))
                flower_immortal_value(p->prop[i].u.value);
        }
        if (p->fast_array && (p->class_id == JS_CLASS_ARRAY || p->class_id == JS_CLASS_ARGUMENTS)) {
            for (i = 0; i < p->u.array.count; i++)
                flower_immortal_value(p->u.array.u.values[i]);
        }
        break;
    }
    case JS_GC_OBJ_TYPE_FUNCTION_BYTECODE: {
        JSFunctionBytecode *b = (JSFunctionBytecode *)gp;
        for (i = 0; i < (uint32_t)b->cpool_count; i++)
            flower_immortal_value(b->cpool[i]);
        break;
    }
    case JS_GC_OBJ_TYPE_VAR_REF: {
        JSVarRef *var_ref = (JSVarRef *)gp;
        if (var_ref->is_detached)
            flower_immortal_value(var_ref->value);
        break;
    }
    default:
        break;
    }
}

/* Every live block becomes immortal, counted or not. A small block is live when
 * its header holds its own index (a free one links to another block or none);
 * large ones are reached as collectable objects, atoms, or strings and big
 * integers those objects hold in their properties, arrays, constants or
 * closed-over variables. Others stay counted, which is only slower. Nothing
 * reads the count of a block that is not counted, and every counted allocation
 * sets its own. */
void flower_immortalize(JSRuntime *rt)
{
    struct list_head *el, *next;
    int i;

    list_for_each_safe(el, next, &rt->gc_obj_list) {
        JSGCObjectHeader *p = list_entry(el, JSGCObjectHeader, link);
        flower_immortal(p);
        flower_immortal_values(p);
        list_del(&p->link);
        list_add_tail(&p->link, &flower_immortal_objects);
    }
    for (i = 0; i < JS_ARENA_BLOCK_SIZE_COUNT; i++) {
        unsigned int block_size = arena_block_sizes[i];
        list_for_each(el, &rt->arena_state.arena_list[i]) {
            JSArena *ar = list_entry(el, JSArena, link);
            unsigned int index;
            for (index = 0; index < ar->n_blocks; index++) {
                JSMallocBlockHeader *b = arena_get_block(ar, index, block_size);
                if (b->u.block_idx == index)
                    flower_immortal(b->user_data);
            }
        }
    }
    for (i = 0; i < rt->atom_size; i++) {
        JSAtomStruct *p = rt->atom_array[i];
        if (p && !atom_is_free(p))
            flower_immortal(p);
    }
}
