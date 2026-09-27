// Independent JavaScript implementation of GUEST_ABI.md's value encoding, for
// maintainer checks that drive the guest without Rust.
export const [NULL, FALSE, TRUE, INT, FLOAT, UTF8, LATIN1, UTF16, ARRAY, MAP, KEY] =
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
export const SUCCESS = 0, FAILURE = 1;
const utf8 = new TextEncoder(), strict = new TextDecoder("utf-8", { fatal: true });

// Encodes like the host: ASCII strings as Latin-1, keys interned per message.
export function encode(value, prefix = []) {
  const out = [...prefix], keys = new Map();
  const u32 = (n) => out.push(n & 255, (n >>> 8) & 255, (n >>> 16) & 255, n >>> 24);
  const text = (s) => {
    const ascii = /^[\0-\x7f]*$/.test(s), bytes = utf8.encode(s);
    out.push(ascii ? LATIN1 : UTF8); u32(bytes.length); out.push(...bytes);
  };
  const walk = (v) => {
    if (v === null) out.push(NULL);
    else if (v === false || v === true) out.push(v ? TRUE : FALSE);
    else if (typeof v === "number") {
      if (Number.isInteger(v) && v >= -(2 ** 31) && v < 2 ** 31 && !Object.is(v, -0)) { out.push(INT); u32(v); }
      else { out.push(FLOAT); out.push(...new Uint8Array(new Float64Array([v]).buffer)); }
    } else if (typeof v === "string") text(v);
    else if (Array.isArray(v)) { out.push(ARRAY); u32(v.length); v.forEach(walk); }
    else {
      const entries = Object.entries(v).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
      out.push(MAP); u32(entries.length);
      for (const [key, item] of entries) {
        if (keys.has(key)) { out.push(KEY); u32(keys.get(key)); }
        else { keys.set(key, keys.size); text(key); }
        walk(item);
      }
    }
  };
  walk(value);
  return Uint8Array.from(out);
}

export const success = (value) => encode(value, [SUCCESS]);
export const failure = (code, message) => encode({ code, message }, [FAILURE]);

// Strict decoder: returns [value, bytesRead]. `seen` records string tags and
// counts key references.
export function decode(bytes, offset = 0, seen = { tags: [], references: 0 }) {
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const keys = [];
  const u32 = () => { const n = view.getUint32(offset, true); offset += 4; return n; };
  const string = (tag) => {
    const length = u32();
    seen.tags.push(tag);
    if (tag === UTF8) { const s = strict.decode(bytes.subarray(offset, offset + length)); offset += length; return s; }
    if (tag === LATIN1) { const s = String.fromCharCode(...bytes.subarray(offset, offset + length)); offset += length; return s; }
    const units = [];
    for (let i = 0; i < length; i++) units.push(view.getUint16(offset + 2 * i, true));
    offset += 2 * length;
    const s = String.fromCharCode(...units);
    if (!s.isWellFormed()) throw new Error("lone surrogate");
    return s;
  };
  const walk = (depth) => {
    if (depth > 128) throw new Error("too deep");
    const tag = bytes[offset++];
    switch (tag) {
      case NULL: return null;
      case FALSE: return false;
      case TRUE: return true;
      case INT: { const n = view.getInt32(offset, true); offset += 4; return n; }
      case FLOAT: { const n = view.getFloat64(offset, true); offset += 8; if (!Number.isFinite(n)) throw new Error("not finite"); return n; }
      case UTF8: case LATIN1: case UTF16: return string(tag);
      case ARRAY: { const n = u32(); return Array.from({ length: n }, () => walk(depth + 1)); }
      case MAP: {
        const n = u32(), result = Object.create(null);
        for (let i = 0; i < n; i++) {
          const keyTag = bytes[offset++];
          let key;
          if (keyTag === KEY) { key = keys[u32()]; seen.references++; }
          else { key = string(keyTag); keys.push(key); }
          if (key === undefined || Object.hasOwn(result, key)) throw new Error("bad key");
          result[key] = walk(depth + 1);
        }
        return { ...result };
      }
      default: throw new Error(`unknown tag ${tag}`);
    }
  };
  const value = walk(1);
  return [value, offset];
}

// An outcome: {ok: true, value} or {ok: false, error: {code, message, details?}}.
export function outcome(bytes) {
  const [value, read] = decode(bytes, 1);
  if (read !== bytes.length) throw new Error("trailing bytes");
  if (bytes[0] === SUCCESS) return { ok: true, value };
  if (bytes[0] === FAILURE) return { ok: false, error: value };
  throw new Error("bad status");
}
