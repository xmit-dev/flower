# Guest ABI v1

Application code runs as a WebAssembly *guest*. A JavaScript bundle is one kind
of guest: the pinned `quickjs.wasm` implements this ABI by running the bundle.
A Rust module built with the [`flower-sdk`](crates/flower-sdk) crate implements it
directly; [`examples/goblin-pizza-rs`](examples/goblin-pizza-rs) ports the benchmark
application that way. Rust owns everything else: the reactive graph, indexes, scans,
caching, dependency certificates, storage and replication. A guest only runs
one named callback per invocation and talks to the host through the imports
below.

Every call starts from a pristine image: the module's memory and mutable
globals as they were after initialization. Nothing a callback does survives
into another callback.

## Values

Values cross the boundary in a binary encoding designed to be cheap on both
sides: one tag byte, then little-endian fixed-width fields. Nothing is
variable-length except string bytes and container contents, so decoding never
loops over a length prefix. There is no JSON text at the boundary.

| Tag | Value | Payload |
| --- | --- | --- |
| `0x00` | null | none |
| `0x01` | false | none |
| `0x02` | true | none |
| `0x03` | integer | `i32` |
| `0x04` | number | `f64`, finite |
| `0x05` | string | `u32` byte count, UTF-8 |
| `0x06` | string | `u32` count, Latin-1 bytes (code points U+0000–U+00FF) |
| `0x07` | string | `u32` count, UTF-16 code units, well-formed |
| `0x08` | array | `u32` count, then that many values |
| `0x09` | map | `u32` count, then that many key/value pairs |
| `0x0a` | key reference | `u32` index (map keys only) |

- **Strings** travel in whichever representation the sender already holds.
  QuickJS copies its 8-bit and 16-bit strings verbatim; the host sends ASCII
  strings as Latin-1 and others as UTF-8. Lone surrogates are rejected.
- **Numbers** are JavaScript numbers. Senders may use either tag for integral
  values. The host keeps integral numbers below 2⁶⁴ (and above −2⁶³) as
  integers, exactly where parsing JavaScript's rendering of them would, and
  maps `-0` to `0`. NaN and infinities are rejected.
- **Map keys** are strings or key references. Each message numbers the key
  strings it contains in order of appearance, starting at zero; `0x0a n`
  repeats key `n`. A message may repeat a key string instead of referring to
  it. Keys are unique within a map. The host writes keys in JavaScript's
  canonical order (UTF-16 code units), which fixes their enumeration order.
- **Depth**: the root is at depth 1 and no value is deeper than 128.

## Module requirements

- A core WebAssembly module with exactly one memory, exported as `memory`:
  32-bit, not shared, not imported.
- Imports only the two functions below, from module `flower`.
- No start function. Features: MVP, SIMD, sign extension, saturating float
  conversion, multi-value and bulk memory. At most one private funcref table,
  which the module never mutates. Globals are numeric.
- Mutable globals need not be exported; the host exports them itself to reset
  them.

## Exports

```text
flower_alloc(size: i32) -> i32
flower_invoke(kind: i32, name_ptr: i32, name_len: i32, args_ptr: i32, args_len: i32) -> i64
flower_manifest() -> i64
flower_init() -> i32                        ; optional
```

- **`flower_alloc`** returns guest memory the host writes into: invocation
  inputs and host-call responses. It traps on exhaustion.
- **`flower_invoke`** runs one callback. `kind` is `0` query, `1` mutation,
  `2` transaction or `3` derived. `name` is UTF-8 and `args` one value. It
  returns `ptr | len << 32` of an *outcome* that stays valid until the next
  reset.
- **`flower_manifest`** returns the module's manifest as a value (see below).
  The host calls it once per module, without host capabilities.
- **`flower_init`**, when present, runs once while the host prepares the
  module. The resulting memory and globals become the pristine image. It
  returns `0` on success.

An outcome is one status byte followed by a value:

- `0x00`: success; the value is the callback's result.
- `0x01`: failure; the value is a map with string `code` and `message` and an
  optional `details` value, itself a root value. Derived callbacks never carry
  `details`.

A malformed outcome is the callback's `INVALID_VALUE` failure.

## Imports

```text
flower.host_call(op: i32, payload_ptr: i32, payload_len: i32) -> i64
flower.crypto_call(op: i32, parameter: i32, spans_ptr: i32, span_count: i32, result_ptr: i32) -> i32
```

`host_call` takes the operation's arguments as consecutive root values and
returns `ptr | len << 32` of an outcome that the host allocated with
`flower_alloc`. The guest owns that buffer. A failure outcome carries a
business error the callback may catch, including `INVALID_VALUE` for
malformed arguments; unknown operations and resource failures trap instead.

| op | Name | Arguments | Result | Allowed in |
| --- | --- | --- | --- | --- |
| 1 | now | none | milliseconds | all |
| 2 | principal | none | principal or null | methods |
| 3 | history | none | `{database, incarnation}` or null | methods |
| 4 | get | target, key or args | record, derived value or null | all |
| 5 | scan | collection, options? | `[{key, value}]` | all |
| 6 | range | range | `{rows: [{key, value}], cursor}` | all |
| 7 | query | query | `[value]` | all |
| 8 | set | collection, key, value | null | mutations |
| 9 | delete | collection, key | null | mutations |
| 10 | materialize | derived, args | null | mutations |
| 11 | unmaterialize | derived, args | null | mutations |
| 12 | clock | none | milliseconds | all |
| 13 | changesAt | milliseconds or null | null | all |

A `target` is a collection name, `{"kind": "collection", "name": string}` or
`{"kind": "derived", "name": string}`. Scan options, range and query shapes are
described in [INDEXES.md](INDEXES.md). Transactions plan only: every host call
fails there.

`crypto_call` passes byte spans (`{u32 ptr, u32 len}` records) and writes a
`{u32 kind, u32 ptr, u32 length_or_integer}` result; the QuickJS guest's
[README](vendor/quickjs-ng/README.md#private-abi) describes the result kinds.
Entropy (opcode 0) is available only to mutations.

## Manifest

```json
{
  "definitions": {"name": {"kind": "derived|query|mutation|transaction",
                           "consistency": "replica-local", "receipt": false,
                           "aggregate": {…}}},
  "http": {"alias": {"name": "definition", "kind": "query|mutation|transaction",
                     "consistency": "replica-local", "receipt": false}},
  "maintenance": {"name": "…", "kind": "mutation", "onError": {…}} ,
  "authorize": {"name": "…"},
  "collections": [{"name": "…", "indexes": {"index": ["field", …]}, "access": {…}}],
  "keys": [{"kind": "key", "name": "…", "algorithm": "…", "usages": ["…"]}]
}
```

Optional members may be absent. The host validates the manifest: HTTP aliases,
maintenance and authorization must name definitions of the right kind, an
alias's `consistency` (queries) and `receipt` (mutations) must match its
definition's, and
names starting with `$flower.` are reserved for SDK-generated definitions.
A collection's optional `access` holds its policy: `read`, `insert`, `update`
and `delete` rules plus per-field `read`/`write` rules, each a JSON rule tree
(`const`, `all`, `any`, `not`, `eq`, `ne`, `in`, `exists`, `lt`, `lte`, `gt`,
`gte`, `startsWith` over `{"ref": path}` and `{"value": json}` operands; paths
start with `principal`, `row`, `next` or `key`, or are exactly `["now"]`). The
host enforces it on the collection operations of queries and mutations that
have a caller; see `src/evaluator/rust_engine/access.rs`.

## Resource limits

Each invocation shares one budget with its nested callbacks: a deadline, linear
memory, nesting depth and native stack. Epoch interruption stops runaway loops.
Invocation arguments, outcomes and host-call payloads are each bounded by
`FLOWER_RESULT_MAX_BYTES`. An invocation that traps, exhausts a budget or grows
its memory is discarded; none of its effects commit.

## The QuickJS guest

`quickjs.wasm` additionally exports setup functions that only the host's image
builder calls before snapshotting: `flower_eval`, `flower_compile`,
`flower_load`, `flower_free` and `flower_snapshot_prepare`. Bundles that do not
opt into static initialization run from a shared base image and load their
bytecode with `flower_load` before `flower_invoke`.

Encoding JavaScript values reads QuickJS's own object shapes, arrays and
string buffers, so it never runs application code: no getter, proxy trap or
`toJSON` hook. Values must be plain data: `null`, booleans, finite numbers,
strings, arrays without holes or named properties, and objects whose prototype
is `Object.prototype` or `null` with only enumerable string-keyed data
properties. Shared references are copied; cycles are rejected. A callback that
returns anything else fails with `INVALID_VALUE`, and passing it to the
database throws a catchable error with that code.
