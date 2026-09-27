//! The Rust guest port of Goblin Pizza (examples/goblin-pizza-rs) against the
//! TypeScript original, through the production evaluator at fixed clocks.
//! Every result, failure, query validity flag and stored record must match,
//! so either guest serves the same database. Build both, then run:
//!
//! ```sh
//! node bench/guests.mjs /tmp/goblin
//! FLOWER_GOBLIN_BUNDLE=/tmp/goblin/goblin-pizza.js FLOWER_GOBLIN_WASM=/tmp/goblin/goblin-pizza.wasm \
//!   cargo test --release --lib guest_parity -- --ignored --nocapture
//! ```
use super::*;
use base64::Engine as _;

struct Guest {
    label: &'static str,
    data: BTreeMap<String, Value>,
    requests: usize,
}

/// One step's observable outcome.
#[derive(Debug, PartialEq)]
struct Step {
    result: std::result::Result<Value, Value>,
    puts: BTreeMap<String, Value>,
    deletes: Vec<String>,
    evaluated: Vec<String>,
    cacheable: bool,
    clock_polled: bool,
    changes_at: Option<u64>,
    /// Records and markers a query's cache entry or a speculative mutation depends on.
    observed: Option<Vec<String>>,
}

impl Guest {
    fn deploy(label: &'static str, bundle: Value, now: u64) -> Self {
        let deployed = evaluate_at(
            BTreeMap::new(),
            json!({"requestId": "deploy", "bundle": bundle}),
            now,
        )
        .unwrap_or_else(|error| panic!("{label} deploy: {error:#}"));
        Guest {
            label,
            data: deployed.puts,
            requests: 0,
        }
    }

    fn run(&mut self, kind: &str, name: &str, args: Value, now: u64) -> Step {
        self.requests += 1;
        let mut invocation = json!({"name": name, "args": args});
        if kind != "query" {
            invocation["requestId"] = json!(format!("request-{}", self.requests));
        }
        // Mutations take the writer's speculative path, which certifies their reads.
        let evaluation = if kind == "query" {
            invoke_at(self.data.clone(), invocation, kind, now)
        } else {
            invoke_speculative_as(self.data.clone().into(), invocation, now, Value::Null)
        };
        match evaluation {
            Ok(evaluation) => {
                let observed = match (
                    &evaluation.query_certificate,
                    &evaluation.mutation_certificate,
                ) {
                    (Some(certificate), _) => Some(certificate.observed()),
                    (None, Some(certificate)) => Some(certificate.observed()),
                    (None, None) => None,
                };
                let observed = observed.map(|ids| ids.into_iter().map(str::to_owned).collect());
                if kind != "query" {
                    self.data.extend(evaluation.puts.clone());
                    for key in &evaluation.deletes {
                        self.data.remove(key);
                    }
                }
                Step {
                    result: Ok(evaluation.value),
                    puts: evaluation.puts,
                    deletes: evaluation.deletes,
                    evaluated: evaluation.evaluated,
                    cacheable: evaluation.query_cacheable,
                    clock_polled: evaluation.query_clock_polled,
                    changes_at: evaluation.query_changes_at,
                    observed,
                }
            }
            Err(error) => Step {
                result: Err(error
                    .chain()
                    .find_map(|cause| cause.downcast_ref::<rust_engine::EngineError>())
                    .map_or_else(
                        || json!({"error": format!("{error:#}")}),
                        |engine| engine.failure(),
                    )),
                puts: BTreeMap::new(),
                deletes: Vec::new(),
                evaluated: Vec::new(),
                cacheable: false,
                clock_polled: false,
                changes_at: None,
                observed: None,
            },
        }
    }
}

/// Run one invocation on both guests: kind, name, arguments, clock.
type Stepper<'a> = dyn FnMut(&str, &str, Value, u64) -> std::result::Result<Value, Value> + 'a;

fn without_bundle(data: &BTreeMap<String, Value>) -> BTreeMap<&String, &Value> {
    data.iter().filter(|(key, _)| *key != "bundle").collect()
}

#[test]
#[ignore]
fn guest_parity() {
    let javascript = std::fs::read_to_string(
        std::env::var("FLOWER_GOBLIN_BUNDLE").expect("FLOWER_GOBLIN_BUNDLE"),
    )
    .unwrap();
    let wasm =
        std::fs::read(std::env::var("FLOWER_GOBLIN_WASM").expect("FLOWER_GOBLIN_WASM")).unwrap();
    let mut js = Guest::deploy(
        "js",
        json!({"hash": hash(javascript.as_bytes()), "javascript": javascript}),
        1_000,
    );
    let mut rust = Guest::deploy(
        "wasm",
        json!({"hash": hash(&wasm), "wasm": base64::engine::general_purpose::STANDARD.encode(&wasm)}),
        1_000,
    );
    assert_eq!(
        without_bundle(&js.data),
        without_bundle(&rust.data),
        "deployment records"
    );
    let (mut steps, mut certified) = (0, 0);
    let mut step =
        |kind: &str, name: &str, args: Value, now: u64| -> std::result::Result<Value, Value> {
            let (left, right) = (
                js.run(kind, name, args.clone(), now),
                rust.run(kind, name, args.clone(), now),
            );
            steps += 1;
            certified += left.observed.as_ref().is_some_and(|ids| !ids.is_empty()) as usize;
            assert_eq!(
                left, right,
                "step {steps}: {kind} {name}({args}) at {now}: {} vs {}",
                js.label, rust.label
            );
            assert_eq!(
                without_bundle(&js.data),
                without_bundle(&rust.data),
                "step {steps}: records"
            );
            left.result
        };
    // Settle maintenance until it reports nothing due, as the writer would.
    let maintain = |step: &mut Stepper, now: u64| {
        for _ in 0..64 {
            let hint = step("mutation", "$flower.maintenance", Value::Null, now).unwrap();
            if hint["$flower"]["continue"] != true {
                return hint;
            }
        }
        panic!("maintenance did not settle");
    };

    maintain(&mut step, 1_001);
    let setup = json!({"tenants": ["t0", "t1"], "storesPerTenant": 2, "stockPerShop": 9, "bakeMs": 50, "leaseMs": 1_000});
    step("mutation", "internal.pizza.setup", setup.clone(), 1_002).unwrap();
    assert_eq!(
        step("mutation", "internal.pizza.setup", setup, 1_003).unwrap_err()["code"],
        "ALREADY_INITIALIZED"
    );
    maintain(&mut step, 1_004);

    let order = |id: &str, tenant: &str, store: &str, quantity: i64| json!({"id": id, "shop": [tenant, store], "quantity": quantity});
    for (index, (id, tenant, store, quantity)) in [
        ("pizza-1", "t0", "store-0", 4),
        ("pizza-2", "t0", "store-0", 4),
        ("pizza-3", "t0", "store-0", 4),
        ("pizza-1", "t0", "store-0", 1),
        ("pizza-4", "t0", "store-1", 5),
        ("pizza-5", "t0", "store-9", 1),
        ("pizza 6", "t0", "store-1", 1),
        ("pizza-7", "t1", "store-1", 2),
        ("pizza-8", "t1", "store-0", 3),
    ]
    .into_iter()
    .enumerate()
    {
        let _ = step(
            "mutation",
            "internal.pizza.order",
            order(id, tenant, store, quantity),
            1_010 + index as u64,
        );
    }
    for (args, now) in [
        (json!({"shop": ["t0", "store-1"], "amount": 3}), 1_020),
        (
            json!({"shop": ["t1", "store-0"], "amount": 1_000_000}),
            1_021,
        ),
        (json!({"shop": ["t1", "store-0"], "amount": 0}), 1_022),
        (json!({"shop": ["t1"], "amount": 1}), 1_023),
        (
            json!({"shop": ["t1", "store-0"], "amount": 1, "extra": true}),
            1_024,
        ),
    ] {
        let _ = step("mutation", "internal.pizza.tip", args, now);
    }
    let reads = |step: &mut Stepper, now: u64| {
        for (name, args) in [
            ("internal.pizza.shop", json!(["t0", "store-0"])),
            ("internal.pizza.shop.local", json!(["t1", "store-0"])),
            ("internal.pizza.shop", json!(["t9", "store-0"])),
            ("internal.pizza.dashboard", json!({"tenant": "t0"})),
            ("internal.pizza.dashboard", json!({"tenant": "t1"})),
            ("internal.pizza.world", Value::Null),
        ] {
            let _ = step("query", name, args, now);
        }
    };
    reads(&mut step, 1_030);

    // Ovens ring at 1_060 onward; a failed attempt backs off, then succeeds.
    maintain(&mut step, 1_059);
    let failure = json!({"error": {"code": "OVEN_JAM", "message": "jammed", "details": {"door": 1}}, "failedAt": 1_061});
    step("mutation", "$flower.maintenance.error", failure, 1_061).unwrap();
    reads(&mut step, 1_062);
    maintain(&mut step, 1_070);
    maintain(&mut step, 1_200);
    reads(&mut step, 1_201);

    // Drones: deliver one, abandon one until its lease expires, then retry it.
    let claim = step(
        "mutation",
        "internal.pizza.claim",
        json!({"tenant": "t0", "owner": "drone-0"}),
        1_300,
    )
    .unwrap();
    assert_eq!(claim["attempt"], 1);
    let identity = |claim: &Value| json!({"tenant": claim["scope"], "id": claim["id"], "owner": claim["owner"], "token": claim["token"]});
    step(
        "mutation",
        "internal.pizza.deliver",
        identity(&claim),
        1_310,
    )
    .unwrap();
    assert_eq!(
        step(
            "mutation",
            "internal.pizza.deliver",
            identity(&claim),
            1_311
        )
        .unwrap_err()["code"],
        "LEASE_LOST"
    );
    let abandoned = step(
        "mutation",
        "internal.pizza.claim",
        json!({"tenant": "t0", "owner": "drone-1", "leaseMs": 100}),
        1_320,
    )
    .unwrap();
    let _ = step(
        "mutation",
        "internal.pizza.claim",
        json!({"tenant": "t0", "owner": "drone-1", "leaseMs": 5_000}),
        1_321,
    );
    let _ = step(
        "mutation",
        "internal.pizza.claim",
        json!({"tenant": "t1", "owner": "drone-2", "leaseMs": 200}),
        1_322,
    );
    reads(&mut step, 1_330);
    maintain(&mut step, 1_500);
    reads(&mut step, 1_501);
    let reclaimed = step(
        "mutation",
        "internal.pizza.claim",
        json!({"tenant": "t0", "owner": "drone-2"}),
        1_510,
    )
    .unwrap();
    assert_eq!(reclaimed["attempt"], 2);
    assert_eq!(
        step(
            "mutation",
            "internal.pizza.deliver",
            identity(&abandoned),
            1_511
        )
        .unwrap_err()["code"],
        "LEASE_LOST"
    );
    step(
        "mutation",
        "internal.pizza.deliver",
        identity(&reclaimed),
        1_512,
    )
    .unwrap();
    let _ = step(
        "mutation",
        "internal.pizza.deliver",
        json!({"tenant": "t0", "id": "x", "owner": "drone-2", "token": 0}),
        1_513,
    );
    for owner in ["drone-3", "drone-4", "drone-5"] {
        for tenant in ["t0", "t1"] {
            if let Ok(claim) = step(
                "mutation",
                "internal.pizza.claim",
                json!({"tenant": tenant, "owner": owner}),
                1_600,
            ) && !claim.is_null()
            {
                step(
                    "mutation",
                    "internal.pizza.deliver",
                    identity(&claim),
                    1_601,
                )
                .unwrap();
            }
        }
    }
    maintain(&mut step, 3_000);
    reads(&mut step, 3_001);
    let world = step("query", "internal.pizza.world", Value::Null, 3_002).unwrap();
    assert!(
        world["orders"]
            .as_array()
            .unwrap()
            .iter()
            .all(|order| order["status"] == "delivered"),
        "{world}"
    );
    // Archive deliveries in bounded passes; the dashboard keeps their counts.
    // t0 delivered at 1_310, 1_512 and 1_601: the cutoff 1_561 takes two.
    let delivered = world["orders"].as_array().unwrap().len();
    for (args, now, archived) in [
        (
            json!({"tenant": "t0", "olderThanMs": 1_450, "limit": 1}),
            3_010,
            Some(1),
        ),
        (
            json!({"tenant": "t0", "olderThanMs": 1_450, "limit": 10}),
            3_011,
            Some(1),
        ),
        (
            json!({"tenant": "t1", "olderThanMs": 0, "limit": 1_000}),
            3_012,
            None,
        ),
        (
            json!({"tenant": "t9", "olderThanMs": 0, "limit": 1}),
            3_013,
            None,
        ),
        (
            json!({"tenant": "t0", "olderThanMs": 0, "limit": 0}),
            3_014,
            None,
        ),
    ] {
        let result = step("mutation", "internal.pizza.archive", args, now);
        if let Some(count) = archived {
            assert_eq!(result.unwrap()["archived"], count);
        }
    }
    reads(&mut step, 3_020);
    let world = step("query", "internal.pizza.world", Value::Null, 3_021).unwrap();
    assert!(
        world["orders"].as_array().unwrap().len() < delivered - 2,
        "{world}"
    );
    assert_eq!(
        step("query", "internal.pizza.missing", Value::Null, 3_003).unwrap_err()["code"],
        "DEFINITION_MISSING"
    );
    assert_eq!(
        step("query", "internal.pizza.order", Value::Null, 3_004).unwrap_err()["code"],
        "METHOD_KIND_MISMATCH"
    );
    println!(
        "{steps} steps agree, {certified} with read certificates; {} records",
        js.data.len()
    );
}
