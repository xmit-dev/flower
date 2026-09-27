
(() => {
    function checkJson(value, active = new Set(), depth = 0) {
        if (depth > 128) throw new Error('JSON nesting exceeds 128');
        if (value === null || typeof value === 'string' || typeof value === 'boolean') return;
        if (typeof value === 'number' && Number.isFinite(value)) return;
        if (typeof value !== 'object') throw new Error('Return a finite JSON value');
        if (active.has(value)) throw new Error('Return values cannot contain cycles');
        if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null)
            throw new Error('Return values must be plain JSON objects or arrays');
        active.add(value);
        for (const key of Reflect.ownKeys(value)) {
            if (Array.isArray(value) && key === 'length') continue;
            const descriptor = Object.getOwnPropertyDescriptor(value, key);
            if (typeof key !== 'string' || !descriptor.enumerable || !('value' in descriptor))
                throw new Error('Return values cannot contain symbols or accessors');
            checkJson(descriptor.value, active, depth + 1);
        }
        if (Array.isArray(value)) for (let i = 0; i < value.length; ++i)
            if (!Object.hasOwn(value, i)) throw new Error('Return arrays cannot contain holes');
        active.delete(value);
    }
    function ref(value) {
        if (typeof value === 'string') return value;
        if (value && (value.kind === 'collection' || value.kind === 'derived'))
            return {kind: value.kind, name: value.name};
        return value;
    }
    function read(method, args) {
        try { checkJson(args); } catch (e) { e.code = 'INVALID_VALUE'; throw e; }
        const result = JSON.parse(__flowerRead(method, JSON.stringify(args)));
        if (!result.ok) throw Object.assign(new Error(result.error.message), {code: result.error.code});
        return result.value;
    }
    const api = {
        now: () => read('now', []),
        principal: () => read('principal', []),
        history: () => read('history', []),
        get: (target, args = null) => read('get', [ref(target), args]),
        scan: (collection, options) => {
            const target = ref(collection);
            if (options === undefined) return read('scan', [target]);
            if (target && typeof target === 'object' && target.kind === 'collection') {
                const descriptor = Object.getOwnPropertyDescriptor(collection, 'indexes');
                if (descriptor && !('value' in descriptor))
                    throw Object.assign(new Error('Collection indexes cannot be an accessor'), {code: 'INVALID_VALUE'});
                target.indexes = descriptor ? descriptor.value : {};
            }
            return read('scan', [target, options]);
        },
        query: (query) => read('query', [query]),
        range: (query) => read('range', [query])
    };
    if (__kind !== 'derived') Object.assign(api, {
        set: (collection, key, value) => read('set', [ref(collection), key, value]),
        delete: (collection, key) => read('delete', [ref(collection), key]),
        materialize: (definition, args = null) => read('materialize', [ref(definition), args]),
        unmaterialize: (definition, args = null) => read('unmaterialize', [ref(definition), args])
    });
    Object.freeze(api);
    try {
        const definitions = typeof __flowerBundle !== 'undefined' && __flowerBundle.default.definitions;
        if (!definitions || !Object.hasOwn(definitions, __name) || typeof definitions[__name].compute !== 'function')
            throw Object.assign(new Error('Unknown derived definition: ' + __name), {code: 'DEFINITION_MISSING'});
        const actualKind = definitions[__name].kind;
        const expectedKind = __kind === 'derived' ? 'derived' : __kind + 'Method';
        if (actualKind !== expectedKind)
            throw Object.assign(new Error('Definition ' + __name + ' is not a ' + __kind + ' method'), {code: 'METHOD_KIND_MISMATCH'});
        const value = definitions[__name].compute(api, JSON.parse(__argsJson));
        try { checkJson(value); } catch (e) { e.code = 'INVALID_VALUE'; throw e; }
        return JSON.stringify({ok: true, value});
    } catch (e) {
        const message = String(e && e.message || e);
        const error = {
            code: e && typeof e.code === 'string' ? e.code : /out of memory|interrupted/i.test(message) ? 'EVALUATION_BUDGET' : 'COMPUTE_ERROR',
            message
        };
        try {
            if (__kind !== 'derived' && e && typeof e === 'object' && e.details !== undefined) {
                checkJson(e.details);
                error.details = e.details;
            }
        } catch (_) {}
        return JSON.stringify({ok: false, error});
    }
})()
