/* Flower's trusted reactive state transition. No host APIs or user code live here. */
(function () {
  "use strict";

  var MAX_EVALUATIONS = 10000;
  var MAX_READS = 100000;
  var MAX_DEPTH = 128;
  var MAX_OUTPUT_BYTES = 16 * 1024 * 1024;
  var own = function (object, key) {
    return Object.prototype.hasOwnProperty.call(object, key);
  };

  function failure(code, message) {
    var error = new Error(message);
    error.code = code;
    return error;
  }

  // ctx.changesAt(time): null declares nothing; otherwise whole milliseconds.
  function changeTime(time) {
    if (time === null) return null;
    if (typeof time !== "number" || !isFinite(time)) throw failure("INVALID_VALUE", "changesAt takes a finite number of milliseconds or null");
    return Math.min(Math.ceil(time), 9007199254740991);
  }

  // Ordered positions, as src/evaluator/rust_engine/ranges.rs encodes them.
  var ORDERED_DIGITS = "-0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";
  function orderedTextKey(value) {
    var out = "";
    for (var i = 0; i < value.length; i++) {
      var unit = value.charCodeAt(i);
      if (unit <= 0x21) out += "!" + String.fromCharCode(0x40 + unit);
      else if (unit <= 0x7d) out += value[i];
      else out += "~" + unit.toString(16).padStart(4, "0");
    }
    return out;
  }
  function orderedScalar(value) {
    if (value === null) return "0";
    if (typeof value === "boolean") return value ? "11" : "10";
    if (typeof value === "string") return "3" + orderedTextKey(value) + " ";
    if (typeof value === "number" && Number.isFinite(value)) {
      var view = new DataView(new ArrayBuffer(8));
      view.setFloat64(0, value === 0 ? 0 : value);
      var bits = view.getBigUint64(0);
      bits = bits >> 63n ? (~bits & ((1n << 64n) - 1n)) : bits ^ (1n << 63n);
      var wide = bits << 2n, digits = "";
      for (var digit = 0; digit < 11; digit++) digits += ORDERED_DIGITS[Number((wide >> BigInt(60 - 6 * digit)) & 63n)];
      return "2" + digits.replace(/-+$/, "") + " ";
    }
    throw failure("INVALID_REFERENCE", "Invalid ordered index component");
  }

  function scanRecord(value, label) {
    requireRecord(value, label, "INVALID_REFERENCE");
    var result = Object.create(null);
    Reflect.ownKeys(value).forEach(function (key) {
      var descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (typeof key !== "string" || !descriptor.enumerable || !own(descriptor, "value")) {
        throw failure("INVALID_REFERENCE", label + " must contain enumerable data properties");
      }
      result[key] = descriptor.value;
    });
    return result;
  }

  // Share scalar encoding with ranges so scans have identical tuple/tie ordering.
  function orderedScan(ref, rawOptions, rows) {
    if (rawOptions === undefined) return rows;
    var options = scanRecord(rawOptions, "scan options");
    var allowed = ["index", "prefix", "gt", "gte", "lt", "lte", "limit", "offset", "reverse"];
    if (Object.keys(options).some(function (key) { return !allowed.includes(key); }) ||
        ["limit", "offset"].some(function (key) { return own(options, key) && (!Number.isSafeInteger(options[key]) || options[key] < 0); }) ||
        (own(options, "reverse") && typeof options.reverse !== "boolean")) {
      throw failure("INVALID_REFERENCE", "Invalid scan options");
    }
    var fields = null;
    if (own(options, "index")) {
      if (typeof options.index !== "string" || !options.index) throw failure("INVALID_REFERENCE", "Invalid scan index name");
      var descriptor = Object.getOwnPropertyDescriptor(ref, "indexes");
      if (descriptor && !own(descriptor, "value")) throw failure("INVALID_REFERENCE", "Collection indexes cannot be an accessor");
      var indexes = scanRecord(descriptor ? descriptor.value : {}, "collection indexes");
      if (!own(indexes, options.index)) throw failure("INVALID_REFERENCE", "Unknown scan index: " + options.index);
      fields = normalize(indexes[options.index], "INVALID_REFERENCE");
      if (!Array.isArray(fields) || !fields.length || fields.some(function (field) { return typeof field !== "string" || !field; }) ||
          new Set(fields).size !== fields.length) throw failure("INVALID_REFERENCE", "Invalid scan index fields");
    }
    var parts = own(options, "prefix") ? normalize(options.prefix, "INVALID_REFERENCE") : [];
    var dimensions = fields ? fields.length : 1;
    if (!Array.isArray(parts) || parts.length > dimensions ||
        (parts.length === dimensions && ["gt", "gte", "lt", "lte"].some(function (key) { return own(options, key); })) ||
        (own(options, "gt") && own(options, "gte")) || (own(options, "lt") && own(options, "lte"))) {
      throw failure("INVALID_REFERENCE", "Invalid scan bounds");
    }
    function component(value) {
      if (!fields && typeof value !== "string") throw failure("INVALID_REFERENCE", "Source-key scan bounds must be strings");
      return orderedScalar(value);
    }
    var base = parts.map(component).join("");
    var lower = base, upper = base + "~";
    if (own(options, "gte")) lower += component(options.gte);
    if (own(options, "gt")) lower += component(options.gt) + "~";
    if (own(options, "lt")) upper = base + component(options.lt);
    if (own(options, "lte")) upper = base + component(options.lte) + "~";
    var selected = [];
    rows.forEach(function (row) {
      var id = "";
      if (fields) {
        if (!row.value || typeof row.value !== "object" || Array.isArray(row.value)) return;
        try {
          fields.forEach(function (field) {
            if (!own(row.value, field)) throw 0;
            id += orderedScalar(row.value[field]);
          });
        } catch (_) { return; }
        id += ":" + orderedTextKey(row.key);
      } else id = orderedScalar(row.key);
      if (id >= lower && id < upper) selected.push({ id: id, row: row });
    });
    selected.sort(function (a, b) {
      return (a.id < b.id ? -1 : a.id > b.id ? 1 : 0) * (options.reverse === true ? -1 : 1);
    });
    var offset = own(options, "offset") ? options.offset : 0;
    selected = selected.slice(offset);
    if (own(options, "limit")) selected = selected.slice(0, options.limit);
    return selected.map(function (entry) { return entry.row; });
  }

  // Reference implementation only: native production uses persistent ordered keys.
  function orderedRange(ref, rows) {
    requireRecord(ref, "range reference", "INVALID_REFERENCE");
    if (ref.kind !== "range" || typeof ref.collection !== "string" || !ref.collection ||
        !Array.isArray(ref.fields) || !ref.fields.length || ref.fields.some(function (v) { return typeof v !== "string" || !v; }) || new Set(ref.fields).size !== ref.fields.length) throw failure("INVALID_REFERENCE", "Invalid range reference");
    var options = ref.options;
    requireRecord(options, "range options", "INVALID_REFERENCE");
    if (Object.keys(options).some(function (k) { return !["prefix", "gt", "gte", "lt", "lte", "limit", "after", "reverse"].includes(k); }) ||
        !Number.isSafeInteger(options.limit) || options.limit < 1 || (own(options,"reverse") && typeof options.reverse !== "boolean")) throw failure("INVALID_REFERENCE", "Invalid range options");
    var parts = own(options,"prefix") ? options.prefix : [];
    if (!Array.isArray(parts) || parts.length > ref.fields.length ||
        (parts.length === ref.fields.length && ["gt","gte","lt","lte"].some(function(k){return own(options,k);})) ||
        (own(options,"gt") && own(options,"gte")) || (own(options,"lt") && own(options,"lte"))) throw failure("INVALID_REFERENCE", "Invalid range bounds");
    var start = "ordered-entry:"+canonical([ref.collection,ref.fields])+":";
    var base = start + parts.map(orderedScalar).join("");
    var lower = base, upper = base+"~", reverse = options.reverse === true;
    if (own(options,"gte")) lower += orderedScalar(options.gte);
    if (own(options,"gt")) lower += orderedScalar(options.gt)+"~";
    if (own(options,"lt")) upper = base+orderedScalar(options.lt);
    if (own(options,"lte")) upper = base+orderedScalar(options.lte)+"~";
    var scope = canonical([ref.collection,ref.fields,lower,upper,reverse]), after = null;
    if (own(options,"after")) {
      try { var cursor = JSON.parse(options.after); } catch (_) { throw failure("INVALID_REFERENCE", "Invalid cursor"); }
      if (typeof options.after !== "string" || !cursor || cursor.version !== 1 || cursor.scope !== scope || Object.keys(cursor).length !== 3 || typeof cursor.last !== "string" || cursor.last < lower || cursor.last >= upper) throw failure("INVALID_REFERENCE", "Cursor belongs to another range");
      after = cursor.last;
    }
    var selected = [];
    rows.forEach(function(row){
      if (!row.value || typeof row.value !== "object" || Array.isArray(row.value)) return;
      var id = start;
      try { ref.fields.forEach(function(field){if(!own(row.value,field)) throw 0; id += orderedScalar(row.value[field]);}); } catch (_) {return;}
      id += ":"+orderedTextKey(row.key);
      if (id >= lower && id < upper && (after === null || (reverse ? id < after : id > after))) selected.push({id:id,row:row});
    });
    selected.sort(function(a,b){return (a.id < b.id ? -1 : a.id > b.id ? 1 : 0)*(reverse ? -1 : 1);});
    var more = selected.length > options.limit; selected = selected.slice(0,options.limit);
    return { rows: selected.map(function(v){return v.row;}), cursor: more ? canonical({version:1,scope:scope,last:selected[selected.length-1].id}) : null };
  }

  function normalize(value, code) {
    var ancestors = new Set();
    function visit(item, depth) {
      if (depth > MAX_DEPTH) throw failure(code, "JSON nesting exceeds 128 levels");
      if (item === null || typeof item === "boolean" || typeof item === "string") return item;
      if (typeof item === "number") {
        if (!Number.isFinite(item)) throw failure(code, "JSON numbers must be finite");
        return item === 0 ? 0 : item;
      }
      if (typeof item !== "object") throw failure(code, "Value is not valid JSON");
      if (Object.prototype.toString.call(item) !== "[object Object]" && !Array.isArray(item)) {
        throw failure(code, "Value is not a JSON object or array");
      }
      if (ancestors.has(item)) throw failure(code, "JSON values cannot contain cycles");
      if (Object.getOwnPropertySymbols(item).length) throw failure(code, "JSON values cannot have symbol keys");
      ancestors.add(item);
      var result;
      if (Array.isArray(item)) {
        result = [];
        for (var index = 0; index < item.length; index++) {
          var descriptor = Object.getOwnPropertyDescriptor(item, String(index));
          if (!descriptor || !own(descriptor, "value")) throw failure(code, "JSON arrays cannot contain holes or accessors");
          result.push(visit(descriptor.value, depth + 1));
        }
      } else {
        result = Object.create(null);
        var keys = Object.keys(item).sort();
        for (var i = 0; i < keys.length; i++) {
          var property = Object.getOwnPropertyDescriptor(item, keys[i]);
          if (!own(property, "value")) throw failure(code, "JSON objects cannot contain accessors");
          result[keys[i]] = visit(property.value, depth + 1);
        }
      }
      ancestors.delete(item);
      return result;
    }
    return visit(value, 0);
  }

  // JSON.stringify reorders integer-like object keys, so encode objects explicitly.
  function canonical(value) {
    if (value === null || typeof value !== "object") return JSON.stringify(value);
    // Keep traversal on the heap: recursive Array callbacks consume several
    // native QuickJS frames per JSON level, even for a tiny deeply nested value.
    var output = [];
    var frames = [];
    function append(item) {
      if (item === null || typeof item !== "object") { output.push(JSON.stringify(item)); return; }
      var array = Array.isArray(item);
      var keys = array ? null : Object.keys(item).sort();
      output.push(array ? "[" : "{");
      frames.push({ value: item, keys: keys, length: array ? item.length : keys.length, index: 0 });
    }
    append(value);
    while (frames.length) {
      var frame = frames[frames.length - 1];
      if (frame.index === frame.length) {
        output.push(frame.keys === null ? "]" : "}");
        frames.pop();
        continue;
      }
      if (frame.index) output.push(",");
      var key = frame.keys === null ? frame.index : frame.keys[frame.index];
      frame.index++;
      if (frame.keys !== null) output.push(JSON.stringify(key), ":");
      append(frame.value[key]);
    }
    return output.join("");
  }

  // Callers pass normalized JSON or trusted wrappers around normalized values.
  // Its keys already have canonical insertion order; native serialization keeps
  // the same JSON-visible ordering while avoiding another recursive encoder.
  function copy(value) { return JSON.parse(JSON.stringify(value)); }
  // Host snapshots contain only JSON, but their property insertion order comes
  // from Rust's serializer rather than JavaScript's UTF-16 string ordering.
  function canonicalCopy(value) { return JSON.parse(canonical(value)); }
  // Check only changed values. Inputs were validated in Rust; callbacks and
  // writes still pass normalize(). Offsets include their state/wire wrappers.
  function trustedDepth(value, depth) {
    var pending = [[value, depth]];
    while (pending.length) {
      var item = pending.pop();
      if (item[1] > MAX_DEPTH) throw failure("INPUT_INVALID", "JSON nesting exceeds 128 levels");
      if (item[0] === null || typeof item[0] !== "object") continue;
      var keys = Object.keys(item[0]);
      for (var i = 0; i < keys.length; i++) pending.push([item[0][keys[i]], item[1] + 1]);
    }
  }
  function cellId(name, args) { return "cell:[" + JSON.stringify(name) + "," + canonical(args) + "]"; }
  function rootId(name, args) { return "root:[" + JSON.stringify(name) + "," + canonical(args) + "]"; }
  // Both fields are strings, so native array encoding is already canonical.
  // Source identity validation runs for every stored source during a preview.
  function sourceId(collection, key) { return "source:" + JSON.stringify([collection, key]); }
  function collectionId(collection) { return "collection:" + JSON.stringify(collection); }
  // Derived equality queries depend on the bucket of their value.
  function bucketId(collection, fields, encoded) { return "index-bucket:" + canonical([collection, fields]) + ":" + encoded; }
  function bucketOf(fields, value) {
    if (value === null || typeof value !== "object" || Array.isArray(value)) return null;
    if (!fields.every(function (field) { return own(value, field); })) return null;
    return canonical(fields.length === 1 ? value[fields[0]] : fields.map(function (field) { return value[field]; }));
  }
  // The [collection, fields] JSON that starts a bucket dependency, or null.
  function bucketSpec(dep) {
    if (dep.indexOf("index-bucket:") !== 0) return null;
    var text = dep.slice(13), depth = 0, quoted = false, escaped = false;
    for (var index = 0; index < text.length; index++) {
      var char = text[index];
      if (quoted) {
        if (escaped) escaped = false;
        else if (char === "\\") escaped = true;
        else if (char === '"') quoted = false;
      } else if (char === '"') quoted = true;
      else if (char === "[") depth++;
      else if (char === "]" && --depth === 0) {
        try { return JSON.parse(text.slice(0, index + 1)); } catch (error) { return null; }
      }
    }
    return null;
  }
  // Staged maps retain unchanged immutable values by reference. In particular,
  // finalization need not serialize every untouched source, cell, and bundle.
  function equal(a, b) { return a === b || canonical(a) === canonical(b); }
  function requireString(value, label, code) {
    if (typeof value !== "string") throw failure(code || "INPUT_INVALID", label + " must be a string");
    return value;
  }
  function requireRecord(value, label, code) {
    if (value === null || typeof value !== "object" || Array.isArray(value)) {
      throw failure(code || "INPUT_INVALID", label + " must be an object");
    }
    return value;
  }
  function validateCell(id, cell) {
    requireRecord(cell, "stored cell");
    requireString(cell.name, "stored cell name");
    if (!own(cell, "args") || cellId(cell.name, cell.args) !== id) throw failure("INPUT_INVALID", "Malformed stored cell identity");
    requireRecord(cell.outcome, "stored cell outcome");
    if (cell.outcome.ok === true) {
      if (!own(cell.outcome, "value")) throw failure("INPUT_INVALID", "Missing stored cell value");
    } else if (cell.outcome.ok === false) {
      requireRecord(cell.outcome.error, "stored cell error");
      requireString(cell.outcome.error.code, "stored error code");
      requireString(cell.outcome.error.message, "stored error message");
    } else throw failure("INPUT_INVALID", "Malformed stored cell outcome");
    if (!Array.isArray(cell.deps) || cell.deps.some(function (dep) { return typeof dep !== "string"; })) {
      throw failure("INPUT_INVALID", "Malformed stored cell dependencies");
    }
  }
  function sourcePair(id) {
    var pair;
    try { pair = JSON.parse(id.slice(7)); } catch (_) { throw failure("INPUT_INVALID", "Malformed stored source key"); }
    if (!Array.isArray(pair) || pair.length !== 2 || typeof pair[0] !== "string" || typeof pair[1] !== "string" ||
        sourceId(pair[0], pair[1]) !== id) throw failure("INPUT_INVALID", "Malformed stored source key");
    return pair;
  }
  // Advancing time cannot change a complete graph with no clock readers. Reuse
  // it only after checking the same stored identities, outcomes, reachability,
  // and graph limits as normal evaluation. Anything unusual takes the existing
  // path, preserving its errors, missing-cell evaluation, and garbage collection.
  function canAdvanceClock(base) {
    var cells = new Map();
    var roots = [];
    var timed = false;
    try {
      base.forEach(function (value, id) {
        if (id.indexOf("cell:") === 0) {
          validateCell(id, value);
          if (value.deps.indexOf("clock") !== -1) timed = true;
          cells.set(id, value);
        } else if (id.indexOf("source:") === 0) {
          sourcePair(id);
        } else if (id.indexOf("root:") === 0) {
          requireRecord(value, "stored root");
          var name = requireString(value.name, "stored root.name");
          var args = own(value, "args") ? value.args : null;
          if (rootId(name, args) !== id) return roots.push(null);
          roots.push(cellId(name, args));
        }
      });
    } catch (_) { return false; }
    if (timed || roots.indexOf(null) !== -1) return false;
    var reached = new Set();
    var active = new Set();
    function visit(id, level) {
      if (active.has(id)) return false;
      if (reached.has(id)) return true;
      if (level >= MAX_DEPTH || reached.size >= MAX_EVALUATIONS || !cells.has(id)) return false;
      active.add(id);
      reached.add(id);
      var deps = cells.get(id).deps;
      for (var index = 0; index < deps.length; index++) {
        if (deps[index].indexOf("cell:") === 0 && !visit(deps[index], level + 1)) return false;
      }
      active.delete(id);
      return true;
    }
    roots.sort();
    return roots.every(function (id) { return visit(id, 0); }) && reached.size === cells.size;
  }
  function sampledTime(data, supplied) {
    var stored = own(data, "clock") ? data.clock : 0;
    if (!Number.isSafeInteger(stored) || stored < 0) throw failure("INPUT_INVALID", "Stored clock must be a nonnegative safe integer");
    if (supplied === undefined) return stored;
    if (!Number.isSafeInteger(supplied) || supplied < 0) throw failure("INPUT_INVALID", "Time must be a nonnegative safe integer");
    return Math.max(stored, supplied);
  }
  function utf8Size(value) {
    var bytes = 0;
    for (var i = 0; i < value.length; i++) {
      var unit = value.charCodeAt(i);
      if (unit < 128) bytes++;
      else if (unit < 2048) bytes += 2;
      else if (unit >= 0xd800 && unit <= 0xdbff && i + 1 < value.length &&
               value.charCodeAt(i + 1) >= 0xdc00 && value.charCodeAt(i + 1) <= 0xdfff) {
        bytes += 4;
        i++;
      } else bytes += 3;
    }
    return bytes;
  }

  globalThis.flowerEvaluate = function flowerEvaluate(data, mutation, evaluateCell) {
    return evaluateNormalized(normalize(data, "INPUT_INVALID"), normalize(mutation, "INPUT_INVALID"), evaluateCell);
  };
  function evaluateNormalized(input, command, evaluateCell, trusted) {
    requireRecord(input, "data");
    return evaluateMap(new Map(Object.keys(input).map(function (key) { return [key, input[key]]; })),
      command, evaluateCell, trusted);
  }
  // The trusted coordinator already owns a private snapshot Map. Reuse that
  // immutable base during a preview instead of rebuilding it through an object.
  function evaluateMap(base, command, evaluateCell, trusted, ordered) {
    var clone = trusted && !ordered ? canonicalCopy : copy;
    requireRecord(command, "mutation");
    requireString(command.requestId, "requestId");
    if (typeof evaluateCell !== "function") throw failure("INPUT_INVALID", "evaluateCell must be a function");
    if (own(command, "expectedRevision") && (!Number.isSafeInteger(command.expectedRevision) || command.expectedRevision < 0)) {
      throw failure("INPUT_INVALID", "expectedRevision must be a nonnegative safe integer");
    }
    var staged = new Map(base);
    var now = sampledTime(base.has("clock") ? { clock: base.get("clock") } : {}, own(command, "now") ? command.now : undefined);
    var reverse = new Map();
    var dirty = new Set();
    var complete = new Set();
    var active = new Set();
    var evaluated = [];
    var reads = 0;
    var depth = 0;
    var fatal = null;
    var sourceRows = new Map();
    var sortedCollections = new Set();
    var changedKeys = new Set();
    var cells = new Set();
    var rootKeys = new Set();

    function abort(code, message) {
      if (!fatal) fatal = failure(code, message);
      throw fatal;
    }
    function countRead(count) {
      reads += count;
      if (reads > MAX_READS) abort("EVALUATION_BUDGET", "Read budget exceeds 100000");
    }
    function list(field) {
      if (!own(command, field)) return [];
      if (!Array.isArray(command[field])) throw failure("INPUT_INVALID", field + " must be an array");
      return command[field];
    }
    function reference(value, label) {
      requireRecord(value, label);
      var name = requireString(value.name, label + ".name");
      var args = own(value, "args") ? value.args : null;
      return { name: name, args: args };
    }
    base.forEach(function (value, id) {
      if (id.indexOf("root:") === 0) rootKeys.add(id);
      if (id.indexOf("cell:") !== 0) return;
      validateCell(id, value);
      cells.add(id);
      value.deps.forEach(function (dep) {
        if (!reverse.has(dep)) reverse.set(dep, new Set());
        reverse.get(dep).add(id);
      });
    });
    var bucketFields = new Map();
    reverse.forEach(function (_, dep) {
      var spec = bucketSpec(dep);
      if (!spec) return;
      if (!bucketFields.has(spec[0])) bucketFields.set(spec[0], new Map());
      bucketFields.get(spec[0]).set(canonical(spec[1]), spec[1]);
    });

    var touched = new Set();
    list("writes").forEach(function (write) {
      requireRecord(write, "write");
      var collection = requireString(write.collection, "write.collection");
      var key = requireString(write.key, "write.key");
      var id = sourceId(collection, key);
      if (own(write, "delete") && write.delete !== true) throw failure("INPUT_INVALID", "write.delete must be true");
      if (write.delete === true) {
        if (own(write, "value")) throw failure("INPUT_INVALID", "A write cannot both set and delete a value");
        staged.delete(id);
      } else {
        if (!own(write, "value")) throw failure("INPUT_INVALID", "A write must provide a value or delete:true");
        staged.set(id, write.value);
      }
      touched.add(id);
      changedKeys.add(id);
    });

    var changed = [];
    if (own(command, "now") && (!base.has("clock") || base.get("clock") !== now)) {
      staged.set("clock", now);
      changedKeys.add("clock");
      changed.push("clock");
    }
    touched.forEach(function (id) {
      if (base.has(id) === staged.has(id) && (!base.has(id) || equal(base.get(id), staged.get(id)))) return;
      var collection = JSON.parse(id.slice(7))[0];
      changed.push(id);
      changed.push(collectionId(collection));
      (bucketFields.get(collection) || new Map()).forEach(function (fields) {
        var before = base.has(id) ? bucketOf(fields, base.get(id)) : null;
        var after = staged.has(id) ? bucketOf(fields, staged.get(id)) : null;
        if (before !== null) changed.push(bucketId(collection, fields, before));
        if (after !== null && after !== before) changed.push(bucketId(collection, fields, after));
      });
    });
    for (var cursor = 0; cursor < changed.length; cursor++) {
      var readers = reverse.get(changed[cursor]);
      if (!readers) continue;
      readers.forEach(function (id) {
        if (!dirty.has(id)) { dirty.add(id); changed.push(id); }
      });
    }

    if (own(command, "bundle")) {
      requireRecord(command.bundle, "bundle");
      requireString(command.bundle.hash, "bundle.hash");
      requireString(command.bundle.javascript, "bundle.javascript");
      if (!base.has("bundle") || !equal(base.get("bundle"), command.bundle)) {
        staged.set("bundle", command.bundle);
        changedKeys.add("bundle");
        base.forEach(function (_, id) { if (id.indexOf("cell:") === 0) dirty.add(id); });
      }
    }

    list("materialize").forEach(function (value) {
      var ref = reference(value, "materialize");
      var id = rootId(ref.name, ref.args);
      staged.set(id, ref);
      rootKeys.add(id);
      changedKeys.add(id);
    });
    list("unmaterialize").forEach(function (value) {
      var ref = reference(value, "unmaterialize");
      var id = rootId(ref.name, ref.args);
      staged.delete(id);
      rootKeys.delete(id);
      changedKeys.add(id);
    });

    // Sources do not change during evaluation. Build one safe collection lookup.
    staged.forEach(function (value, id) {
      if (id.indexOf("source:") !== 0) return;
      var pair = sourcePair(id);
      if (!sourceRows.has(pair[0])) sourceRows.set(pair[0], []);
      sourceRows.get(pair[0]).push({ key: pair[1], value: value });
    });
    // Point-only computations need no collection sorting. Keep validation above
    // unconditional, but order each collection only when a scan/query reads it.
    function collectionRows(collection) {
      var rows = sourceRows.get(collection) || [];
      if (!sortedCollections.has(collection)) {
        rows.sort(function (a, b) { return a.key < b.key ? -1 : a.key > b.key ? 1 : 0; });
        sortedCollections.add(collection);
      }
      return rows;
    }

    function ensure(name, args) {
      var id = cellId(name, args);
      if (fatal) throw fatal;
      if (active.has(id)) abort("CYCLE", "Reactive cycle at " + id);
      var previous = staged.get(id);
      if (complete.has(id) || (previous && !dirty.has(id))) return previous.outcome;
      if (evaluated.length >= MAX_EVALUATIONS) abort("EVALUATION_BUDGET", "Cell evaluation budget exceeds 10000");
      if (depth >= MAX_DEPTH) abort("EVALUATION_BUDGET", "Reactive evaluation depth exceeds 128");
      evaluated.push(id);
      active.add(id);
      depth++;
      var observed = new Set();
      var liveContext = true;
      function checkContext() {
        if (fatal) throw fatal;
        if (!liveContext) throw failure("INVALID_CONTEXT", "The evaluation context has expired");
      }
      function collectionName(ref) {
        requireRecord(ref, "collection reference", "INVALID_REFERENCE");
        if (ref.kind !== "collection") throw failure("INVALID_REFERENCE", "Expected a collection reference");
        return requireString(ref.name, "collection name", "INVALID_REFERENCE");
      }
      function scanRows(collection) {
        checkContext();
        observed.add(collectionId(collection));
        var rows = collectionRows(collection);
        countRead(1 + rows.length);
        return rows;
      }
      var context = Object.freeze({
        now: function () {
          checkContext();
          countRead(1);
          observed.add("clock");
          return now;
        },
        clock: function () {
          checkContext();
          countRead(1);
          observed.add("clock");
          return now;
        },
        changesAt: function (time) {
          checkContext();
          countRead(1);
          changeTime(time);
          return null;
        },
        get: function (ref, keyOrArgs) {
          checkContext();
          countRead(1);
          requireRecord(ref, "reference", "INVALID_REFERENCE");
          var targetName = requireString(ref.name, "reference.name", "INVALID_REFERENCE");
          if (ref.kind === "collection") {
            requireString(keyOrArgs, "source key", "INVALID_REFERENCE");
            var target = sourceId(targetName, keyOrArgs);
            observed.add(target);
            return staged.has(target) ? clone(staged.get(target)) : null;
          }
          if (ref.kind === "derived") {
            var targetArgs = normalize(keyOrArgs === undefined ? null : keyOrArgs, "INVALID_VALUE");
            observed.add(cellId(targetName, targetArgs));
            var outcome = ensure(targetName, targetArgs);
            if (!outcome.ok) throw failure(outcome.error.code, outcome.error.message);
            return clone(outcome.value);
          }
          throw failure("INVALID_REFERENCE", "Unknown reference kind");
        },
        scan: function (ref, options) {
          checkContext();
          return clone(orderedScan(ref, options, scanRows(collectionName(ref))));
        },
        range: function (ref) {
          checkContext();
          return clone(orderedRange(ref, scanRows(ref.collection)));
        },
        query: function (ref) {
          checkContext();
          requireRecord(ref, "query reference", "INVALID_REFERENCE");
          if (ref.kind !== "query") throw failure("INVALID_REFERENCE", "Expected a query reference");
          var collection = typeof ref.collection === "string" ? ref.collection : collectionName(ref.collection);
          if (!Array.isArray(ref.fields) || !ref.fields.length || ref.fields.some(function (field) { return typeof field !== "string"; })) {
            throw failure("INVALID_REFERENCE", "Query fields must be a nonempty array of strings");
          }
          var expected = normalize(ref.value, "INVALID_VALUE");
          if (ref.fields.length > 1 && (!Array.isArray(expected) || expected.length !== ref.fields.length)) {
            throw failure("INVALID_REFERENCE", "Composite query value must match the index field count");
          }
          var rows = collectionRows(collection);
          countRead(1 + rows.length);
          observed.add(bucketId(collection, ref.fields, canonical(expected)));
          var result = [];
          rows.forEach(function (row) {
            if (row.value === null || typeof row.value !== "object" || Array.isArray(row.value)) return;
            var matches = ref.fields.every(function (field, index) {
              return own(row.value, field) && equal(row.value[field], ref.fields.length === 1 ? expected : expected[index]);
            });
            if (matches) result.push(row.value);
          });
          return clone(result);
        }
      });
      var outcome;
      try {
        var value = normalize(evaluateCell(name, clone(args), context), "INVALID_VALUE");
        if (fatal) throw fatal;
        outcome = { ok: true, value: value };
      } catch (error) {
        if (fatal) throw fatal;
        var code = error && typeof error.code === "string" ? error.code : "COMPUTE_ERROR";
        var message = error && typeof error.message === "string" ? error.message : String(error);
        if (code === "CYCLE" || code === "EVALUATION_BUDGET") abort(code, message);
        outcome = { ok: false, error: { code: code, message: message } };
        if (previous) previous.deps.forEach(function (dep) { observed.add(dep); });
      } finally {
        liveContext = false;
        depth--;
        active.delete(id);
      }
      staged.set(id, { name: name, args: args, outcome: outcome, deps: Array.from(observed).sort() });
      changedKeys.add(id);
      cells.add(id);
      complete.add(id);
      return outcome;
    }

    var roots = [];
    rootKeys.forEach(function (id) {
      var ref = reference(staged.get(id), "stored root");
      if (rootId(ref.name, ref.args) !== id) throw failure("INPUT_INVALID", "Malformed stored root identity");
      roots.push(ref);
    });
    roots.sort(function (a, b) {
      var left = cellId(a.name, a.args), right = cellId(b.name, b.args);
      return left < right ? -1 : left > right ? 1 : 0;
    });
    roots.forEach(function (root) { ensure(root.name, root.args); });

    // Error outcomes retain old dependencies. Evaluate any retained dirty cells too.
    // Validate the final graph: retained error edges can create a cycle even if no
    // direct recursive call occurred in this particular execution.
    var reachable = new Set();
    var visiting = new Set();
    function visit(id, level) {
      if (visiting.has(id)) abort("CYCLE", "Reactive cycle at " + id);
      if (reachable.has(id)) return;
      if (level >= MAX_DEPTH) abort("EVALUATION_BUDGET", "Reactive graph depth exceeds 128");
      if (reachable.size >= MAX_EVALUATIONS) abort("EVALUATION_BUDGET", "Live cell budget exceeds 10000");
      var cell = staged.get(id);
      if (!cell) throw failure("INPUT_INVALID", "Missing derived dependency " + id);
      if (dirty.has(id) && !complete.has(id)) {
        ensure(cell.name, cell.args);
        cell = staged.get(id);
      }
      visiting.add(id);
      reachable.add(id);
      cell.deps.forEach(function (dep) { if (dep.indexOf("cell:") === 0) visit(dep, level + 1); });
      visiting.delete(id);
    }
    roots.forEach(function (root) { visit(cellId(root.name, root.args), 0); });
    cells.forEach(function (id) {
      if (!reachable.has(id)) {
        staged.delete(id);
        changedKeys.add(id);
      }
    });
    if (fatal) throw fatal;

    var puts = Object.create(null);
    var deletes = [];
    changedKeys.forEach(function (id) {
      if (staged.has(id)) {
        var value = staged.get(id);
        if (!base.has(id) || !equal(base.get(id), value)) puts[id] = value;
      } else if (base.has(id)) deletes.push(id);
    });
    deletes.sort();
    var result = { puts: puts, deletes: deletes, evaluated: evaluated };
    // Property order changes neither the JSON values nor their encoded size.
    if (utf8Size(JSON.stringify(result)) > MAX_OUTPUT_BYTES) abort("EVALUATION_BUDGET", "Output exceeds 16 MiB");
    return result;
  };

  // Public reads and writes enter through deployed TypeScript methods. A method
  // can observe intermediate writes; only its final state transition is durable.
  globalThis.flowerInvoke = function flowerInvoke(data, invocation, evaluateMethod, evaluateCell, now) {
    return invokeInternal(data, invocation, evaluateMethod, evaluateCell, now, false);
  };
  // Only the Rust coordinator calls this entry point, with depth-checked JSON
  // snapshots. User methods run in distinct runtimes that never load ENGINE.
  globalThis.flowerInvokeTrusted = function flowerInvokeTrusted(data, invocation, evaluateMethod, evaluateCell, now) {
    return invokeInternal(data, invocation, evaluateMethod, evaluateCell, now, true);
  };
  // Rust can order every snapshot object's keys once before JSON parsing. All
  // subsequent writes/results pass normalize(), so copies keep that ordering
  // without another interpreted canonical traversal on every context read.
  globalThis.flowerInvokeOrdered = function flowerInvokeOrdered(data, invocation, evaluateMethod, evaluateCell, now) {
    return invokeInternal(data, invocation, evaluateMethod, evaluateCell, now, true, true);
  };
  function invokeInternal(data, invocation, evaluateMethod, evaluateCell, now, trusted, ordered) {
    var clone = trusted && !ordered ? canonicalCopy : copy;
    var input = trusted ? data : normalize(data, "INPUT_INVALID");
    var call = normalize(invocation, "INPUT_INVALID");
    requireRecord(input, "data");
    requireRecord(call, "invocation");
    if (call.kind !== "query" && call.kind !== "mutation") throw failure("INPUT_INVALID", "Invocation kind must be query or mutation");
    requireString(call.name, "invocation.name");
    if (own(call, "requestId")) requireString(call.requestId, "invocation.requestId");
    if (typeof evaluateMethod !== "function" || typeof evaluateCell !== "function") {
      throw failure("INPUT_INVALID", "Evaluation callbacks must be functions");
    }
    var args = own(call, "args") ? call.args : null;
    var fixedNow = sampledTime(input, now);
    var hasTime = now !== undefined;
    var base = new Map(Object.keys(input).map(function (key) { return [key, input[key]]; }));
    var sources = new Map(base);
    var preview = new Map(base);
    var writes = new Map();
    var durableRoots = new Map();
    var temporaryRoots = new Map();
    var collectionRows = new Map();
    var evaluated = [];
    var evaluations = 0;
    var operations = 0;
    var fatal = null;
    var liveContext = true;
    var previewDirty = hasTime && (!base.has("clock") || base.get("clock") !== fixedNow);
    var clockUnprocessed = previewDirty;
    var previewUnchecked = new Set();
    var previewChanged = new Set();
    var queryCacheable = call.kind === "query";
    // ctx.now() promises no change time; ctx.changesAt() declares one.
    var queryClockPolled = false;
    var queryChangesAt = null;
    // A stored cell may read the clock unseen by this invocation.
    var graphReadsClock = false;

    base.forEach(function (value, id) {
      if (id.indexOf("root:") === 0) durableRoots.set(id, value);
      if (trusted && id.indexOf("cell:") === 0 && (!value || !Array.isArray(value.deps) || value.deps.indexOf("clock") !== -1)) {
        queryCacheable = false;
        graphReadsClock = true;
      }
    });
    function declareChange(time) {
      var at = changeTime(time);
      if (at !== null && at > fixedNow) queryChangesAt = queryChangesAt === null ? at : Math.min(queryChangesAt, at);
    }
    function abort(code, message) {
      if (!fatal) fatal = failure(code, message);
      throw fatal;
    }
    function countOperations(count) {
      operations += count;
      if (operations > MAX_READS) abort("EVALUATION_BUDGET", "Method operation budget exceeds 100000");
      if (fatal) throw fatal;
    }
    function checkContext() {
      if (fatal) throw fatal;
      if (!liveContext) throw failure("INVALID_CONTEXT", "The method context has expired");
      countOperations(1);
    }
    function writable() {
      checkContext();
      if (call.kind !== "mutation") abort("QUERY_WRITE_FORBIDDEN", "Query methods cannot write or change materialization");
    }
    function referenceName(ref, kind) {
      requireRecord(ref, "reference", "INVALID_REFERENCE");
      if (ref.kind !== kind) throw failure("INVALID_REFERENCE", "Expected a " + kind + " reference");
      return requireString(ref.name, "reference.name", "INVALID_REFERENCE");
    }
    function rowsFor(collection) {
      if (collectionRows.has(collection)) return collectionRows.get(collection);
      var rows = [];
      sources.forEach(function (value, id) {
        if (id.indexOf("source:") !== 0) return;
        var pair = JSON.parse(id.slice(7));
        if (pair[0] === collection) rows.push({ key: pair[1], value: value });
      });
      rows.sort(function (left, right) { return left.key < right.key ? -1 : left.key > right.key ? 1 : 0; });
      collectionRows.set(collection, rows);
      return rows;
    }
    function trackedCell(name, cellArgs, ctx) {
      evaluations++;
      if (evaluations > MAX_EVALUATIONS) abort("EVALUATION_BUDGET", "Method cell evaluation budget exceeds 10000");
      var tracked = Object.freeze({
        now: function () { queryCacheable = false; queryClockPolled = true; countOperations(1); return ctx.now(); },
        clock: function () { queryCacheable = false; countOperations(1); return ctx.clock(); },
        changesAt: function (time) { countOperations(1); ctx.changesAt(time); declareChange(time); return null; },
        get: function (ref, key) { countOperations(1); return ctx.get(ref, key); },
        scan: function (ref, options) {
          countOperations(1);
          var rows = ctx.scan(ref, options);
          countOperations(rows.length);
          return rows;
        },
        range: function (ref) { countOperations(1); return ctx.range(ref); },
        query: function (ref) {
          countOperations(1);
          var result = ctx.query(ref);
          var collection = typeof ref.collection === "string" ? ref.collection : ref.collection.name;
          countOperations(rowsFor(collection).length);
          return result;
        }
      });
      try { return evaluateCell(name, cellArgs, tracked); }
      catch (error) {
        if (error && (error.code === "CYCLE" || error.code === "EVALUATION_BUDGET")) abort(error.code, error.message);
        throw error;
      }
    }
    function runPreview(final) {
      if (fatal) throw fatal;
      // Defer this check until a preview is actually needed. Source-only reads
      // need no graph validation, and writes already require normal evaluation.
      if (clockUnprocessed && !writes.size) {
        clockUnprocessed = false;
        if (canAdvanceClock(preview)) {
          preview.set("clock", fixedNow);
          previewChanged.add("clock");
          previewDirty = false;
        }
      }
      var desired = new Map(durableRoots);
      if (!final) temporaryRoots.forEach(function (ref, id) { desired.set(id, ref); });
      var materialize = [];
      var unmaterialize = [];
      desired.forEach(function (ref, id) {
        if (!preview.has(id)) materialize.push(ref);
      });
      preview.forEach(function (ref, id) {
        if (id.indexOf("root:") === 0 && !desired.has(id)) unmaterialize.push(ref);
      });
      if (!previewDirty && !materialize.length && !unmaterialize.length) return;
      var result;
      try {
        var command = {
          requestId: own(call, "requestId") ? call.requestId : "__flowerInvoke__",
          writes: Array.from(writes.values()),
          materialize: materialize,
          unmaterialize: unmaterialize
        };
        if (hasTime) command.now = fixedNow;
        if (trusted) {
          previewUnchecked.forEach(function (id) { if (preview.has(id)) trustedDepth(preview.get(id), 1); });
          previewUnchecked.clear();
        }
        if (trusted) {
          result = evaluateMap(preview, normalize(command, "INPUT_INVALID"), trackedCell, true, ordered);
        } else {
          var current = Object.create(null);
          preview.forEach(function (value, id) { current[id] = value; });
          result = globalThis.flowerEvaluate(current, command, trackedCell);
        }
      } catch (error) {
        if (error && (error.code === "CYCLE" || error.code === "EVALUATION_BUDGET")) abort(error.code, error.message);
        throw error;
      }
      Object.keys(result.puts).forEach(function (id) {
        preview.set(id, result.puts[id]);
        previewChanged.add(id);
        if (trusted) previewUnchecked.add(id);
      });
      result.deletes.forEach(function (id) { preview.delete(id); previewChanged.add(id); });
      result.evaluated.forEach(function (id) { evaluated.push(id); });
      writes.clear();
      previewDirty = false;
      clockUnprocessed = false;
    }
    function rootChange(ref, rawArgs, materialize) {
      writable();
      var name = referenceName(ref, "derived");
      var cellArgs = normalize(rawArgs === undefined ? null : rawArgs, "INVALID_VALUE");
      var id = rootId(name, cellArgs);
      if (materialize) durableRoots.set(id, { name: name, args: cellArgs });
      else durableRoots.delete(id);
      previewDirty = true;
      return null;
    }
    var context = Object.freeze({
      history: function () { checkContext(); var value=input["$flower.retention"]; return value ? {database:value.database,incarnation:value.incarnation} : null; },
      now: function () { queryCacheable = false; queryClockPolled = true; checkContext(); return fixedNow; },
      clock: function () { queryCacheable = false; checkContext(); return fixedNow; },
      changesAt: function (time) { checkContext(); declareChange(time); return null; },
      get: function (ref, keyOrArgs) {
        checkContext();
        requireRecord(ref, "reference", "INVALID_REFERENCE");
        if (ref.kind === "collection") {
          var collection = referenceName(ref, "collection");
          requireString(keyOrArgs, "source key", "INVALID_REFERENCE");
          var source = sourceId(collection, keyOrArgs);
          return sources.has(source) ? clone(sources.get(source)) : null;
        }
        var name = referenceName(ref, "derived");
        var cellArgs = normalize(keyOrArgs === undefined ? null : keyOrArgs, "INVALID_VALUE");
        var root = rootId(name, cellArgs);
        // A prior read's returned copy needs no continuing subscription. Keeping
        // stale temporary roots active would evaluate values the method no
        // longer uses after a later write (and could introduce spurious errors).
        temporaryRoots.clear();
        temporaryRoots.set(root, { name: name, args: cellArgs });
        runPreview(false);
        if (graphReadsClock) queryClockPolled = true;
        var outcome = preview.get(cellId(name, cellArgs)).outcome;
        if (!outcome.ok) throw failure(outcome.error.code, outcome.error.message);
        return clone(outcome.value);
      },
      scan: function (ref, options) {
        checkContext();
        var rows = rowsFor(referenceName(ref, "collection"));
        countOperations(rows.length);
        return clone(orderedScan(ref, options, rows));
      },
      range: function (ref) {
        checkContext();
        var rows = rowsFor(ref.collection);
        countOperations(rows.length);
        return clone(orderedRange(ref, rows));
      },
      query: function (ref) {
        checkContext();
        requireRecord(ref, "query reference", "INVALID_REFERENCE");
        if (ref.kind !== "query") throw failure("INVALID_REFERENCE", "Expected a query reference");
        var collection = typeof ref.collection === "string" ? ref.collection : referenceName(ref.collection, "collection");
        if (!Array.isArray(ref.fields) || !ref.fields.length || ref.fields.some(function (field) { return typeof field !== "string"; })) {
          throw failure("INVALID_REFERENCE", "Query fields must be a nonempty array of strings");
        }
        var expected = normalize(ref.value, "INVALID_VALUE");
        if (ref.fields.length > 1 && (!Array.isArray(expected) || expected.length !== ref.fields.length)) {
          throw failure("INVALID_REFERENCE", "Composite query value must match the index field count");
        }
        var rows = rowsFor(collection);
        countOperations(rows.length);
        return clone(rows.filter(function (row) {
          return row.value !== null && typeof row.value === "object" && !Array.isArray(row.value) &&
            ref.fields.every(function (field, index) {
              return own(row.value, field) && equal(row.value[field], ref.fields.length === 1 ? expected : expected[index]);
            });
        }).map(function (row) { return row.value; }));
      },
      set: function (ref, key, rawValue) {
        writable();
        var collection = referenceName(ref, "collection");
        requireString(key, "source key", "INVALID_REFERENCE");
        var value = normalize(rawValue, "INVALID_VALUE");
        var id = sourceId(collection, key);
        sources.set(id, value);
        writes.set(id, { collection: collection, key: key, value: value });
        collectionRows.delete(collection);
        previewDirty = true;
        return null;
      },
      delete: function (ref, key) {
        writable();
        var collection = referenceName(ref, "collection");
        requireString(key, "source key", "INVALID_REFERENCE");
        var id = sourceId(collection, key);
        sources.delete(id);
        writes.set(id, { collection: collection, key: key, delete: true });
        collectionRows.delete(collection);
        previewDirty = true;
        return null;
      },
      materialize: function (ref, cellArgs) { return rootChange(ref, cellArgs, true); },
      unmaterialize: function (ref, cellArgs) { return rootChange(ref, cellArgs, false); }
    });
    var value;
    try {
      value = normalize(evaluateMethod(call.name, clone(args), context), "INVALID_VALUE");
      if (fatal) throw fatal;
    } catch (error) {
      if (fatal) throw fatal;
      throw error;
    } finally {
      liveContext = false;
    }
    var puts = Object.create(null);
    var deletes = [];
    if (call.kind === "mutation") {
      runPreview(true);
      previewChanged.forEach(function (id) {
        if (preview.has(id)) {
          var entry = preview.get(id);
          if (!base.has(id) || !equal(base.get(id), entry)) puts[id] = entry;
        } else if (base.has(id)) deletes.push(id);
      });
      deletes.sort();
    }
    if (trusted) {
      Object.keys(puts).forEach(function (id) { trustedDepth(puts[id], 3); });
      trustedDepth(value, 2);
    }
    var result = { puts: puts, deletes: deletes, evaluated: evaluated, value: value };
    if (trusted) result.query_cacheable = queryCacheable;
    if (trusted && queryClockPolled) result.query_clock_polled = true;
    if (trusted && queryChangesAt !== null) result.query_changes_at = queryChangesAt;
    if (utf8Size(JSON.stringify(result)) > MAX_OUTPUT_BYTES) abort("EVALUATION_BUDGET", "Output exceeds 16 MiB");
    return result;
  };
  // Installed only for explicit profiling. Normal execution keeps the original
  // functions and pays no per-call probe/wrapper cost. User VMs never load this.
  if (typeof __flowerProfileRegion === "function") {
    var profileRegion = __flowerProfileRegion;
    function instrument(region, operation) {
      return function () {
        profileRegion(region, true);
        try { return operation.apply(this, arguments); }
        finally { profileRegion(region, false); }
      };
    }
    normalize = instrument(0, normalize);
    canonical = instrument(1, canonical);
    sourcePair = instrument(2, sourcePair);
    validateCell = instrument(3, validateCell);
    canAdvanceClock = instrument(4, canAdvanceClock);
    evaluateMap = instrument(5, evaluateMap);
    invokeInternal = instrument(6, invokeInternal);
    utf8Size = instrument(7, utf8Size);
  }
})();
