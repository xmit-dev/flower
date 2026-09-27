use std::cell::RefCell;

use super::*;

type Host<'a> = dyn FnMut(&str, Value) -> EngineResult<Value> + 'a;
type Callback = fn(&Value, &mut Host<'_>) -> EngineResult<Value>;

struct Fixture {
    callbacks: BTreeMap<&'static str, Callback>,
    calls: RefCell<Vec<String>>,
}

impl Fixture {
    fn new(callbacks: impl IntoIterator<Item = (&'static str, Callback)>) -> Self {
        Self {
            callbacks: callbacks.into_iter().collect(),
            calls: RefCell::new(Vec::new()),
        }
    }
}

impl Executor for Fixture {
    fn execute(
        &self,
        _kind: &str,
        name: &str,
        args: &Value,
        host: &mut Host<'_>,
    ) -> EngineResult<Value> {
        self.calls.borrow_mut().push(name.into());
        self.callbacks
            .get(name)
            .ok_or_else(|| EngineError::new("DEFINITION_MISSING", format!("Missing {name}")))?(
            args, host,
        )
    }
}

fn get(host: &mut Host<'_>, kind: &str, name: &str, args: Value) -> EngineResult<Value> {
    host("get", json!([{"kind":kind,"name":name},args]))
}

fn set(host: &mut Host<'_>, collection: &str, key: &str, value: Value) -> EngineResult<Value> {
    host(
        "set",
        json!([{"kind":"collection","name":collection},key,value]),
    )
}

fn apply(data: &mut Records, result: Evaluation) {
    for (id, value) in result.puts {
        data.insert(id, value);
    }
    for id in result.deletes {
        data.remove(&id);
    }
}

fn deploy(data: &mut Records, command: Value, fixture: &Fixture) -> Evaluation {
    let mut command = command;
    command["requestId"] = json!("test");
    let result = run(data.clone(), command, "deployment", None, fixture).unwrap();
    for (id, value) in &result.puts {
        data.insert(id.clone(), value.clone());
    }
    for id in &result.deletes {
        data.remove(id);
    }
    result
}

#[test]
fn canonical_id_uses_ecmascript_numbers_and_utf16_order() {
    let value = json!({"\u{e000}": 1.0, "😀": -0.0, "1": 1e-7, "10": 1e20, "2": 1e21});
    assert_eq!(
        canonical_json(&value),
        "{\"1\":1e-7,\"10\":100000000000000000000,\"2\":1e+21,\"😀\":0,\"\u{e000}\":1}"
    );
    assert!(equal(&json!(1), &json!(1.0)));
    assert_eq!(
        canonical_json(&json!(9_007_199_254_740_993_u64)),
        "9007199254740992"
    );
    assert_eq!(
        normalize(json!(1_000_000_000_000_000_128_u64), "INPUT_INVALID").unwrap(),
        json!(1_000_000_000_000_000_100_u64)
    );
    let value = json!({"quote\"\n\u{0}":[value, "é🌺\\", true, false, null]});
    assert_eq!(encoded_len(&value), canonical_json(&value).len());
    let puts = BTreeMap::from([("x".into(), value)]);
    let deletes = vec!["y".into()];
    let evaluated = vec!["z".into()];
    assert_eq!(
        patch_bytes(&puts, &deletes, &evaluated),
        canonical_json(&json!({"puts":puts,"deletes":deletes,"evaluated":evaluated})).len()
    );
}

#[test]
fn final_patch_moves_new_json_allocations_and_preserves_retained_snapshots() {
    struct MoveExecutor {
        value: RefCell<Option<Value>>,
    }
    impl Executor for MoveExecutor {
        fn execute(&self, _: &str, _: &str, _: &Value, host: &mut Host<'_>) -> EngineResult<Value> {
            host(
                "set",
                Value::Array(vec![
                    json!({"kind":"collection","name":"payloads"}),
                    json!("one"),
                    self.value.borrow_mut().take().unwrap(),
                ]),
            )
        }
    }
    let payload = "owned output 🌸".repeat(1024);
    let allocation = payload.as_ptr();
    let executor = MoveExecutor {
        value: RefCell::new(Some(Value::String(payload))),
    };
    let mut base = Records::new();
    base.insert(source_id("payloads", "one"), json!("previous"));
    let retained = base.clone();
    let result = run(
        base,
        json!({"name":"write"}),
        "mutation",
        Some(10),
        &executor,
    )
    .unwrap();
    let output = result.puts[&source_id("payloads", "one")].as_str().unwrap();
    assert_eq!(
        output.as_ptr(),
        allocation,
        "the private overlay's JSON allocation transfers into the commit patch"
    );
    assert_eq!(output, "owned output 🌸".repeat(1024));
    assert_eq!(retained[&source_id("payloads", "one")], "previous");
    assert_eq!(result.puts["clock"], 10);
}

#[test]
fn cached_source_validation_still_rejects_malformed_stored_ids() {
    let fixture = Fixture::new([(
        "write",
        (|_, host| set(host, "cache-validation", "ok", json!(2))) as Callback,
    )]);
    let mut data = Records::default();
    data.insert(source_id("cache-validation", "ok"), json!(1));
    for _ in 0..2 {
        run(
            data.clone(),
            json!({"name":"write","requestId":"warm"}),
            "mutation",
            None,
            &fixture,
        )
        .unwrap();
    }
    for invalid in [
        r#"source:["cache-validation", "ok"]"#,
        r#"source:["cache-validation","\u006fk"]"#,
        r#"source:["cache-validation",null]"#,
    ] {
        let mut malformed = data.clone();
        malformed.insert(invalid.into(), json!(1));
        let error = run(
            malformed,
            json!({"name":"write","requestId":"invalid"}),
            "mutation",
            None,
            &fixture,
        )
        .unwrap_err();
        assert_eq!(error, source_pair(invalid).unwrap_err());
    }
}

#[test]
fn diamond_evaluates_once_and_unchanged_writes_do_not_invalidate() {
    let fixture = Fixture::new([
        (
            "child",
            (|_, host| {
                Ok(json!(
                    get(host, "collection", "input", json!("a"))?
                        .as_i64()
                        .unwrap()
                        * 2
                ))
            }) as Callback,
        ),
        (
            "left",
            (|_, host| get(host, "derived", "child", Value::Null)) as Callback,
        ),
        (
            "right",
            (|_, host| get(host, "derived", "child", Value::Null)) as Callback,
        ),
        (
            "top",
            (|_, host| {
                Ok(json!([
                    get(host, "derived", "left", Value::Null)?,
                    get(host, "derived", "right", Value::Null)?,
                ]))
            }) as Callback,
        ),
    ]);
    let mut data = Records::default();
    let result = deploy(
        &mut data,
        json!({"writes":[{"collection":"input","key":"a","value":7}],"materialize":[{"name":"top"}]}),
        &fixture,
    );
    assert_eq!(result.evaluated.len(), 4);
    assert_eq!(
        data[&cell_id("top", &Value::Null)]["outcome"]["value"],
        json!([14, 14])
    );
    let unchanged = deploy(
        &mut data,
        json!({"writes":[{"collection":"input","key":"a","value":8},{"collection":"input","key":"a","value":7}]}),
        &fixture,
    );
    assert!(unchanged.evaluated.is_empty());
    assert!(unchanged.puts.is_empty());
    let updated = deploy(
        &mut data,
        json!({"writes":[{"collection":"input","key":"a","value":9}]}),
        &fixture,
    );
    assert_eq!(updated.evaluated.len(), 4);
    assert_eq!(
        fixture
            .calls
            .borrow()
            .iter()
            .filter(|name| name.as_str() == "child")
            .count(),
        2
    );
}

#[test]
fn preview_reads_own_writes_and_collects_temporary_roots() {
    let fixture = Fixture::new([
        (
            "double",
            (|_, host| {
                Ok(json!(
                    get(host, "collection", "input", json!("a"))?
                        .as_i64()
                        .unwrap()
                        * 2
                ))
            }) as Callback,
        ),
        (
            "work",
            (|_, host| {
                set(host, "input", "a", json!(3))?;
                let first = get(host, "derived", "double", Value::Null)?;
                set(host, "input", "a", json!(4))?;
                let second = get(host, "derived", "double", Value::Null)?;
                set(host, "input", "a", json!(5))?;
                Ok(json!([first, second]))
            }) as Callback,
        ),
    ]);
    let result = run(
        Records::default(),
        json!({"name":"work","requestId":"r"}),
        "mutation",
        Some(100),
        &fixture,
    )
    .unwrap();
    assert_eq!(result.value, json!([6, 8]));
    assert_eq!(result.puts[&source_id("input", "a")], 5);
    assert_eq!(result.puts["clock"], 100);
    assert!(!result
        .puts
        .keys()
        .any(|id| id.starts_with("cell:") || id.starts_with("root:")));
    assert_eq!(result.evaluated.len(), 2);
}

#[test]
fn durable_preview_recomputes_after_last_write_and_uses_fixed_clock() {
    let fixture = Fixture::new([
        (
            "state",
            (|_, host| {
                Ok(json!([
                    get(host, "collection", "input", json!("a"))?,
                    host("now", json!([]))?
                ]))
            }) as Callback,
        ),
        (
            "work",
            (|_, host| {
                host(
                    "materialize",
                    json!([{"kind":"derived","name":"state"},null]),
                )?;
                set(host, "input", "a", json!(3))?;
                let first = get(host, "derived", "state", Value::Null)?;
                set(host, "input", "a", json!(4))?;
                Ok(json!([first, host("now", json!([]))?]))
            }) as Callback,
        ),
        (
            "read",
            (|_, host| get(host, "derived", "state", Value::Null)) as Callback,
        ),
    ]);
    let result = run(
        Records::default(),
        json!({"name":"work","requestId":"r"}),
        "mutation",
        Some(100),
        &fixture,
    )
    .unwrap();
    assert_eq!(result.value, json!([[3, 100], 100]));
    assert_eq!(
        result.puts[&cell_id("state", &Value::Null)]["outcome"]["value"],
        json!([4, 100])
    );
    let mut data = Records::default();
    apply(&mut data, result);
    let query = run(
        data.clone(),
        json!({"name":"read"}),
        "query",
        Some(200),
        &fixture,
    )
    .unwrap();
    assert_eq!(query.value, json!([4, 200]));
    assert!(!query.query_cacheable);
    assert!(query.puts.is_empty());
    assert_eq!(data["clock"], 100);
}

#[test]
fn swallowed_cycle_and_query_write_remain_fatal() {
    let fixture = Fixture::new([
        (
            "cycle",
            (|_, host| {
                let _ = get(host, "derived", "cycle", Value::Null);
                Ok(json!(42))
            }) as Callback,
        ),
        (
            "query",
            (|_, host| {
                let _ = set(host, "input", "a", json!(1));
                Ok(json!(42))
            }) as Callback,
        ),
        (
            "mutation",
            (|_, host| {
                set(host, "input", "a", json!(1))?;
                let _ = get(host, "derived", "cycle", Value::Null);
                Ok(json!(42))
            }) as Callback,
        ),
    ]);
    assert_eq!(
        run(
            Records::default(),
            json!({"name":"query"}),
            "query",
            None,
            &fixture
        )
        .unwrap_err()
        .code,
        "QUERY_WRITE_FORBIDDEN"
    );
    assert_eq!(
        run(
            Records::default(),
            json!({"name":"mutation","requestId":"r"}),
            "mutation",
            None,
            &fixture
        )
        .unwrap_err()
        .code,
        "CYCLE"
    );
}

#[test]
fn failed_cells_retain_old_dependencies_and_recover() {
    let fixture = Fixture::new([(
        "value",
        (|_, host| {
            if get(host, "collection", "input", json!("fail"))? == true {
                return Err(EngineError::new("EXPECTED", "broken"));
            }
            get(host, "collection", "input", json!("a"))
        }) as Callback,
    )]);
    let mut data = Records::default();
    deploy(
        &mut data,
        json!({"writes":[{"collection":"input","key":"a","value":1}],"materialize":[{"name":"value"}]}),
        &fixture,
    );
    deploy(
        &mut data,
        json!({"writes":[{"collection":"input","key":"fail","value":true}]}),
        &fixture,
    );
    assert_eq!(
        data[&cell_id("value", &Value::Null)]["outcome"]["ok"],
        false
    );
    let still_failed = deploy(
        &mut data,
        json!({"writes":[{"collection":"input","key":"a","value":2}]}),
        &fixture,
    );
    assert_eq!(still_failed.evaluated.len(), 1);
    deploy(
        &mut data,
        json!({"writes":[{"collection":"input","key":"fail","delete":true}]}),
        &fixture,
    );
    assert_eq!(data[&cell_id("value", &Value::Null)]["outcome"]["value"], 2);
}

#[test]
fn scans_are_utf16_ordered_and_queries_track_membership() {
    let fixture = Fixture::new([
        (
            "matches",
            (|_, host| {
                host(
                    "query",
                    json!([{"kind":"query","collection":"rows","fields":["group","shape"],"value":["a",{"x":1}]}]),
                )
            }) as Callback,
        ),
        (
            "read",
            (|_, host| host("scan", json!([{"kind":"collection","name":"rows"}]))) as Callback,
        ),
        (
            "work",
            (|_, host| {
                set(host, "rows", "😀", json!({"group":"b","shape":{"x":1}}))?;
                host("scan", json!([{"kind":"collection","name":"rows"}]))
            }) as Callback,
        ),
    ]);
    let mut data = Records::default();
    deploy(
        &mut data,
        json!({"writes":[{"collection":"rows","key":"\u{e000}","value":{"group":"a","shape":{"x":1}}},{"collection":"rows","key":"😀","value":{"group":"b","shape":{"x":1}}}],"materialize":[{"name":"matches"}]}),
        &fixture,
    );
    let read = run(
        data.clone(),
        json!({"name":"read"}),
        "query",
        None,
        &fixture,
    )
    .unwrap();
    assert_eq!(read.value[0]["key"], "😀");
    assert_eq!(read.value[1]["key"], "\u{e000}");
    let updated = deploy(
        &mut data,
        json!({"writes":[{"collection":"rows","key":"😀","value":{"group":"a","shape":{"x":1}}}]}),
        &fixture,
    );
    assert_eq!(updated.evaluated.len(), 1);
    assert_eq!(
        data[&cell_id("matches", &Value::Null)]["outcome"]["value"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    let result = run(
        data,
        json!({"name":"work","requestId":"r"}),
        "mutation",
        None,
        &fixture,
    )
    .unwrap();
    assert_eq!(result.value[0]["value"]["group"], "b");
}

#[test]
fn errors_from_retained_edges_cannot_hide_cycles() {
    let fixture = Fixture::new([
        (
            "a",
            (|_, host| {
                if get(host, "collection", "input", json!("switch"))? == true {
                    return Err(EngineError::new("EXPECTED", "failed"));
                }
                get(host, "derived", "b", Value::Null)
            }) as Callback,
        ),
        (
            "b",
            (|_, host| {
                if get(host, "collection", "input", json!("switch"))? == true {
                    let _ = get(host, "derived", "a", Value::Null);
                }
                Ok(json!(1))
            }) as Callback,
        ),
    ]);
    let mut data = Records::default();
    deploy(&mut data, json!({"materialize":[{"name":"a"}]}), &fixture);
    let before = data.clone();
    let error = run(
        data.clone(),
        json!({"requestId":"r","writes":[{"collection":"input","key":"switch","value":true}]}),
        "deployment",
        None,
        &fixture,
    )
    .unwrap_err();
    assert_eq!(error.code, "CYCLE");
    assert_eq!(
        serde_json::to_value(data).unwrap(),
        serde_json::to_value(before).unwrap()
    );
}

#[test]
fn repeated_reads_use_deadline_instead_of_a_fixed_operation_count() {
    let fixture = Fixture::new([(
        "work",
        (|_, host| {
            for _ in 0..100_001 {
                let _ = host("now", json!([]));
            }
            Ok(json!(1))
        }) as Callback,
    )]);
    assert_eq!(
        run(
            Records::default(),
            json!({"name":"work"}),
            "query",
            None,
            &fixture
        )
        .unwrap()
        .value,
        json!(1)
    );
}

#[test]
fn deep_temporary_values_are_allowed_once_but_not_committed() {
    let fixture = Fixture::new([
        (
            "deep",
            (|args, _| {
                let mut value = json!(0);
                for _ in 0..args.as_u64().unwrap() {
                    value = json!({"n":value});
                }
                Ok(value)
            }) as Callback,
        ),
        (
            "read",
            (|args, host| {
                get(host, "derived", "deep", args.clone())?;
                Ok(Value::Null)
            }) as Callback,
        ),
        (
            "retain",
            (|args, host| {
                host(
                    "materialize",
                    json!([{"kind":"derived","name":"deep"},args]),
                )?;
                Ok(Value::Null)
            }) as Callback,
        ),
    ]);
    assert!(run(
        Records::default(),
        json!({"name":"read","args":128}),
        "query",
        None,
        &fixture
    )
    .is_ok());
    assert!(run(
        Records::default(),
        json!({"name":"retain","args":123,"requestId":"r"}),
        "mutation",
        None,
        &fixture
    )
    .is_ok());
    assert_eq!(
        run(
            Records::default(),
            json!({"name":"retain","args":124,"requestId":"r"}),
            "mutation",
            None,
            &fixture
        )
        .unwrap_err()
        .code,
        "INPUT_INVALID"
    );
}

#[test]
fn rust_overlay_budget_cannot_be_bypassed_by_discarding_guest_values() {
    let fixture = Fixture::new([
        (
            "many",
            (|_, host| {
                for index in 0..40 {
                    let _ = set(host, "values", &index.to_string(), json!("x".repeat(1024)));
                }
                Ok(Value::Null)
            }) as Callback,
        ),
        (
            "replace",
            (|_, host| {
                for _ in 0..40 {
                    set(host, "values", "same", json!("x".repeat(1024)))?;
                }
                Ok(Value::Null)
            }) as Callback,
        ),
        (
            "largeResult",
            (|_, host| {
                let _ = host("scan", json!([{"kind":"collection","name":"values"}]));
                Ok(Value::Null)
            }) as Callback,
        ),
    ]);
    let error = run_with_limit(
        Records::default(),
        json!({"name":"many","requestId":"r"}),
        "mutation",
        None,
        &fixture,
        8192,
    )
    .unwrap_err();
    assert_eq!(error.code, "EVALUATION_BUDGET");
    assert!(error.message.contains("Rust overlay"));
    let result = run_with_limit(
        Records::default(),
        json!({"name":"replace","requestId":"r"}),
        "mutation",
        None,
        &fixture,
        8192,
    )
    .unwrap();
    assert_eq!(
        result.puts.len(),
        1,
        "replacing one staged key releases its old reservation"
    );
    let data = (0..12)
        .map(|index| {
            (
                source_id("values", &index.to_string()),
                json!("x".repeat(1024)),
            )
        })
        .collect();
    let error = run_with_limit(
        data,
        json!({"name":"largeResult"}),
        "query",
        None,
        &fixture,
        8192,
    )
    .unwrap_err();
    assert_eq!(error.code, "EVALUATION_BUDGET");
}

#[test]
fn physical_query_indexes_follow_staged_membership_and_missing_fields() {
    let fixture = Fixture::new([(
        "work",
        (|_, host| {
            let query =
                json!([{"kind":"query","collection":"values","fields":["match"],"value":null}]);
            let mut results = vec![host("query", query.clone())?];
            set(host, "values", "x", json!({"match":null,"version":1}))?;
            results.push(host("query", query.clone())?);
            set(host, "values", "x", json!({"other":null}))?;
            results.push(host("query", query.clone())?);
            set(host, "values", "x", json!({"match":null,"version":2}))?;
            results.push(host("query", query.clone())?);
            host("delete", json!([{"kind":"collection","name":"values"},"x"]))?;
            results.push(host("query", query)?);
            Ok(Value::Array(results))
        }) as Callback,
    )]);
    let result = run(
        Records::default(),
        json!({"name":"work","requestId":"r"}),
        "mutation",
        None,
        &fixture,
    )
    .unwrap();
    assert_eq!(
        result.value,
        json!([[],[{"match":null,"version":1}],[],[{"match":null,"version":2}],[]])
    );
    assert!(result.puts.is_empty());
}

#[test]
fn collection_indexes_do_not_copy_unrelated_collections() {
    let fixture = Fixture::new([(
        "read",
        (|_, host| host("scan", json!([{"kind":"collection","name":"small"}]))) as Callback,
    )]);
    let mut data: Records = (0..1000)
        .map(|index| (source_id("large", &index.to_string()), json!(index)))
        .collect();
    data.insert(source_id("small", "one"), json!(1));
    let result =
        run_with_limit(data, json!({"name":"read"}), "query", None, &fixture, 8192).unwrap();
    assert_eq!(result.value, json!([{"key":"one","value":1}]));
}

#[test]
fn dependency_tracking_has_a_sticky_host_allocation_budget() {
    let fixture = Fixture::new([
        (
            "large",
            (|_, host| {
                for index in 0..40 {
                    let key = format!("{index}{}", "x".repeat(1024));
                    let _ = get(host, "collection", "values", json!(key));
                }
                Ok(Value::Null)
            }) as Callback,
        ),
        (
            "read",
            (|_, host| {
                let _ = get(host, "derived", "large", Value::Null);
                Ok(Value::Null)
            }) as Callback,
        ),
    ]);
    let error = run_with_limit(
        Records::default(),
        json!({"name":"read"}),
        "query",
        None,
        &fixture,
        8192,
    )
    .unwrap_err();
    assert_eq!(error.code, "EVALUATION_BUDGET");
    assert!(error.message.contains("Observed dependencies"));
}

#[test]
fn live_graph_and_evaluation_counts_are_governed_by_memory() {
    let fixture = Fixture::new([("cell", (|args, _| Ok(args.clone())) as Callback)]);
    let references: Vec<_> = (0..10_001)
        .map(|n| json!({"name":"cell","args":n}))
        .collect();
    let command = json!({"requestId":"large-graph","materialize":references});
    let result = run(
        Records::default(),
        command.clone(),
        "deployment",
        None,
        &fixture,
    )
    .unwrap();
    assert_eq!(result.evaluated.len(), 10_001);
    let error = run_with_limit(
        Records::default(),
        command,
        "deployment",
        None,
        &fixture,
        16 * 1024,
    )
    .unwrap_err();
    assert_eq!(error.code, "EVALUATION_BUDGET");
}

mod dependencies;
mod durable;
mod managed_keys;
mod persistent_graph;
mod propagation;
mod ranges;
mod scans;
