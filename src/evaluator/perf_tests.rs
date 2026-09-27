//! Invocation costs through the production evaluator, for tuning. Build the
//! bundles with the SDK, then run the ignored tests in release mode:
//!
//! ```sh
//! node -e 'import("./sdk/bundle.ts").then(async ({buildBundle}) => {
//!   const fs = await import("node:fs");
//!   fs.writeFileSync("/tmp/goblin.js", (await buildBundle("examples/goblin-pizza-ts/goblin-pizza.ts")).javascript);
//!   fs.writeFileSync("/tmp/ops.js", (await buildBundle("bench/guest-ops.ts")).javascript);
//! })'
//! FLOWER_GOBLIN_BUNDLE=/tmp/goblin.js FLOWER_MICRO_BUNDLE=/tmp/ops.js \
//!   cargo test --release --lib perf_tests -- --ignored --nocapture --test-threads 1
//! ```
//!
//! FLOWER_GOBLIN_ONLY selects one method, FLOWER_GOBLIN_CALLS sets the call
//! count, and FLOWER_MICRO_TRIVIAL=N only loops a trivial query, for sampling.
//! FLOWER_GOBLIN_BACKED=1 serves the seeded records from a stored snapshot,
//! as a replica does, instead of memory.
//! FLOWER_GOBLIN_WASM=path measures the Rust guest (examples/goblin-pizza-rs)
//! instead of the JavaScript bundle; `node bench/guests.mjs DIR` builds both.
use super::*;
use crate::consensus::Records;

fn timed(label: &str, calls: usize, mut call: impl FnMut(usize)) {
    if std::env::var("FLOWER_GOBLIN_ONLY").is_ok_and(|only| only != label) {
        return;
    }
    for index in 0..calls / 10 {
        call(usize::MAX - index);
    }
    let started = Instant::now();
    for index in 0..calls {
        call(index);
    }
    let micros = started.elapsed().as_secs_f64() * 1e6 / calls as f64;
    println!("{label:>12}: {micros:8.1} µs/call");
}

#[test]
#[ignore]
fn goblin_invocation_costs() {
    let bundle = if let Ok(path) = std::env::var("FLOWER_GOBLIN_WASM") {
        use base64::Engine as _;
        let wasm = std::fs::read(path).unwrap();
        json!({"hash": hash(&wasm), "wasm": base64::engine::general_purpose::STANDARD.encode(&wasm)})
    } else {
        let path = std::env::var("FLOWER_GOBLIN_BUNDLE").expect("FLOWER_GOBLIN_BUNDLE");
        let javascript = std::fs::read_to_string(path).unwrap();
        json!({"hash": hash(javascript.as_bytes()), "javascript": javascript})
    };
    let mut data = evaluate(
        BTreeMap::new(),
        json!({"requestId":"deploy","bundle":bundle}),
    )
    .unwrap()
    .puts;
    let setup = invoke(
        data.clone(),
        json!({"name":"internal.pizza.setup","requestId":"setup","args":{
            "tenants":["t0"],"storesPerTenant":4,"stockPerShop":1_000_000,"bakeMs":10,"leaseMs":1000
        }}),
        "mutation",
    )
    .unwrap();
    data.extend(setup.puts);
    let mut records: Records = data.into();
    if std::env::var_os("FLOWER_GOBLIN_BACKED").is_some() {
        records = records.backed_copy();
    }
    let calls = std::env::var("FLOWER_GOBLIN_CALLS").map_or(2000, |calls| calls.parse().unwrap());
    timed("order", calls, |index| {
        invoke(
            records.clone(),
            json!({"name":"internal.pizza.order","requestId":format!("o{index}"),
            "args":{"id":format!("o{index}"),"shop":["t0","store-0"],"quantity":1}}),
            "mutation",
        )
        .unwrap();
    });
    timed("tip", calls, |index| {
        invoke(
            records.clone(),
            json!({"name":"internal.pizza.tip","requestId":format!("t{index}"),
            "args":{"shop":["t0","store-1"],"amount":1}}),
            "mutation",
        )
        .unwrap();
    });
    timed("shop", calls, |_| {
        invoke(
            records.clone(),
            json!({"name":"internal.pizza.shop","args":["t0","store-2"]}),
            "query",
        )
        .unwrap();
    });
}

/// Per-operation guest costs: FLOWER_MICRO_BUNDLE=path, ops loop inside one call.
#[test]
#[ignore]
fn guest_operation_costs() {
    let path = std::env::var("FLOWER_MICRO_BUNDLE").expect("FLOWER_MICRO_BUNDLE");
    let javascript = std::fs::read_to_string(path).unwrap();
    let bundle = json!({"hash": hash(javascript.as_bytes()), "javascript": javascript});
    let mut data = evaluate(
        BTreeMap::new(),
        json!({"requestId":"deploy","bundle":bundle}),
    )
    .unwrap()
    .puts;
    data.insert(
        "source:[\"pizza.shops\",\"[\\\"t0\\\",\\\"store-0\\\"]\"]".into(),
        json!({"id":["t0","store-0"],"key":"[\"t0\",\"store-0\"]","name":"The Crispy Cauldron · store-0","initialStock":1000,"stock":900,"revenue":7,"tips":3}),
    );
    let records: Records = data.into();
    let run = |op: &str, n: usize| {
        let started = Instant::now();
        invoke(
            records.clone(),
            json!({"name":"micro","args":{"op":op,"n":n}}),
            "query",
        )
        .unwrap();
        started.elapsed().as_secs_f64()
    };
    if let Ok(calls) = std::env::var("FLOWER_MICRO_TRIVIAL") {
        for _ in 0..calls.parse::<usize>().unwrap() {
            run("noop", 1);
        }
        return;
    }
    let single = (0..2000).map(|_| run("noop", 1)).fold(f64::MAX, f64::min);
    println!("{:>16}: {:7.2} µs/call", "trivial query", single * 1e6);
    let n = 20_000;
    let base = (0..5).map(|_| run("noop", n)).fold(f64::MAX, f64::min);
    for op in [
        "canonicalTuple",
        "canonicalRecord",
        "regex",
        "identifier",
        "tuple",
        "object",
        "encodeKey",
        "spread",
        "get",
        "now",
    ] {
        let best = (0..5).map(|_| run(op, n)).fold(f64::MAX, f64::min);
        println!("{op:>16}: {:7.2} µs/op", (best - base) * 1e6 / n as f64);
    }
}
