# flower-sdk for Rust

Write a Flower application in Rust and deploy it as a WebAssembly guest module ([GUEST_ABI.md](../../GUEST_ABI.md)). The server runs it through the same pool, budgets and pristine-image resets as a TypeScript bundle, without QuickJS.

The crate mirrors the TypeScript SDK's semantics rather than its syntax. Keyed collections store canonical JSON keys, `v` schemas validate arguments and records with the same messages, and mutations run triggers on the rows they change. The crate also provides `Derived::materialize`, aggregates, maintenance tasks, `scheduler` timers and `queue` leases. Each makes the same host calls in the same order and writes the same records as its TypeScript counterpart, so one database can switch between a TypeScript guest and its Rust port. [examples/goblin-pizza-rs](../../examples/goblin-pizza-rs) ports the benchmark application; `guest_parity_tests.rs` checks it against [the original](../../examples/goblin-pizza-ts/goblin-pizza.ts) step by step.

```rust
use flower_sdk::{object, schema::v, App, Collection, Consistency, Definition, Method, Value};

static COUNTERS: Collection = Collection::new("counters");
const ID: flower_sdk::schema::Schema = v::string().min(1.0).max(64.0);

static INCREMENT: Method = Method::new("internal.counter.increment", |ctx, id| {
    let count = ctx.get(&COUNTERS, &id)?.as_f64().unwrap_or(0.0) + 1.0;
    ctx.set(&COUNTERS, &id, Value::from(count))?;
    Ok(object! {"count" => count})
})
.args(&ID);

static GET: Method = Method::new("internal.counter.get", |ctx, id| ctx.get(&COUNTERS, &id)).args(&ID);

pub static APP: App = App {
    uses: &[],
    collections: &[],
    definitions: &[],
    triggers: &[],
    http: &[
        ("counter.increment", Definition::Mutation(&INCREMENT)),
        ("counter.get", Definition::Query(&GET, Consistency::Linearizable)),
    ],
};

flower_sdk::export!(APP);
```

Build a `cdylib` for `wasm32-unknown-unknown` (the `nix develop` shell provides the target) and deploy the module:

```sh
cargo build --release -p my-app --target wasm32-unknown-unknown
node sdk/cli.ts deploy target/wasm32-unknown-unknown/release/my_app.wasm
```

Values are plain JSON with JavaScript's number model: `Value::Number` is an `f64`, and objects keep insertion order. Callbacks return `Result<Value, Failure>`. `fail("CODE", message)` reaches callers as a coded failure, and `type_error` reports what a thrown `TypeError` would. The crate is `no_std` with `alloc`. Its allocator hands out a 4 MiB zeroed static region and never frees: every callback starts from a pristine image, and staying within the initial memory keeps pooled instances reusable, because an instance that grows its memory is discarded. Host tests (`cargo test -p flower-sdk`) cover the wire codec, JSON, and schemas.
