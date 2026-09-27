//! Deployed guest modules: the same ABI as the QuickJS guest, without it.
use super::*;
use base64::Engine as _;
use std::collections::BTreeMap;

const COUNTER: &[u8] = include_bytes!("../../../../tests/guests/counter.wasm");

fn bundle(wasm: &[u8]) -> Value {
    json!({
        "hash": crate::evaluator::hash(wasm),
        "wasm": base64::engine::general_purpose::STANDARD.encode(wasm),
    })
}

fn counter() -> Arc<Prepared> {
    prepare_bundle(&bundle(COUNTER), limits()).unwrap()
}

fn call(prepared: &Prepared, name: &str, args: Value, kind: &str) -> Value {
    execute_prepared(
        prepared,
        name,
        &args,
        kind,
        &mut |_, _| Ok(Value::Null),
        limits(),
    )
    .unwrap()
}

#[test]
fn modules_snapshot_their_initialization_and_restore_every_call() {
    let prepared = counter();
    let exports: Vec<_> = prepared
        .pre
        .module()
        .exports()
        .map(|export| export.name().to_owned())
        .collect();
    assert!(
        !exports.iter().any(|name| name == "flower_init"),
        "{exports:?}"
    );
    assert!(
        exports
            .iter()
            .any(|name| name.starts_with("flower:global:")),
        "{exports:?}"
    );
    for _ in 0..3 {
        assert_eq!(
            call(&prepared, "init", Value::Null, "query"),
            json!({"ok":true,"value":42})
        );
        assert_eq!(
            call(&prepared, "calls", Value::Null, "query"),
            json!({"ok":true,"value":1})
        );
    }
    let args = json!({"text": "é🌸\0", "list": [1, 2.5, null, true], "nested": {"a": {"b": []}}});
    assert_eq!(
        call(&prepared, "echo", args.clone(), "query"),
        json!({"ok":true,"value":args})
    );
}

#[test]
fn module_outcomes_are_values_failures_or_invalid() {
    let prepared = counter();
    assert_eq!(
        call(&prepared, "fail", Value::Null, "query"),
        json!({"ok":false,"error":{"code":"CUSTOM","message":"guest failure","details":[1]}})
    );
    assert_eq!(
        call(&prepared, "garbage", Value::Null, "query")["error"]["code"],
        "INVALID_VALUE"
    );
    assert_eq!(
        call(&prepared, "missing", Value::Null, "query")["error"]["code"],
        "DEFINITION_MISSING"
    );
    let shared = limits();
    assert!(execute_prepared(
        &prepared,
        "trap",
        &Value::Null,
        "query",
        &mut |_, _| Ok(Value::Null),
        shared.clone()
    )
    .is_err());
    assert!(
        shared.check().is_err(),
        "a trap poisons the shared allowance"
    );
    assert_eq!(call(&prepared, "init", Value::Null, "query")["value"], 42);
}

#[test]
fn modules_call_the_host_and_only_mutations_draw_entropy() {
    let prepared = counter();
    let mut calls = Vec::new();
    let result = execute_prepared(
        &prepared,
        "count",
        &Value::Null,
        "mutation",
        &mut |method, args| {
            calls.push((method.to_owned(), args));
            Ok(if calls.len() == 1 {
                json!(41)
            } else {
                Value::Null
            })
        },
        limits(),
    )
    .unwrap();
    assert_eq!(result, json!({"ok":true,"value":42}));
    let target = json!({"kind":"collection","name":"counter"});
    assert_eq!(
        calls,
        [
            ("get".to_owned(), json!([target, "n"])),
            ("set".to_owned(), json!([target, "n", 42])),
        ]
    );
    assert_eq!(
        call(&prepared, "entropy", Value::Null, "mutation"),
        json!({"ok":true,"value":8})
    );
    assert_eq!(
        call(&prepared, "peek", Value::Null, "query")["error"]["code"],
        "ENTROPY_DENIED"
    );
}

#[test]
fn deployed_modules_run_through_the_engine() {
    use crate::evaluator::{evaluate, invoke};
    let bundle = bundle(COUNTER);
    let deployed = evaluate(
        BTreeMap::new(),
        json!({"requestId":"deploy","bundle":bundle}),
    )
    .unwrap();
    assert_eq!(
        deployed.puts["httpMethods"],
        json!({"count":{"name":"count","kind":"mutation"},"echo":{"name":"echo","kind":"query"}})
    );
    let mut data: BTreeMap<String, Value> = deployed.puts;
    for expected in [1, 2] {
        let result = invoke(
            data.clone(),
            json!({"name":"count","requestId":format!("count-{expected}")}),
            "mutation",
        )
        .unwrap();
        assert_eq!(result.value, expected);
        data.extend(result.puts);
    }
    assert_eq!(data["source:[\"counter\",\"n\"]"], 2);
    let echoed = invoke(
        data.clone(),
        json!({"name":"echo","args":{"x":[1]}}),
        "query",
    )
    .unwrap();
    assert_eq!(echoed.value, json!({"x":[1]}));
    let read = invoke(data, json!({"name":"get"}), "query").unwrap();
    assert_eq!(read.value, 2);
}

#[test]
fn modules_outside_the_guest_surface_are_rejected_before_running() {
    let tampered = |from: &[u8], to: &[u8]| {
        let mut wasm = COUNTER.to_vec();
        let position = wasm
            .windows(from.len())
            .position(|part| part == from)
            .unwrap();
        wasm[position..position + from.len()].copy_from_slice(to);
        wasm
    };
    for (wasm, message) in [
        (b"not wasm".to_vec(), "invalid WebAssembly"),
        (
            tampered(b"flower_manifest", b"flower_manifesT"),
            "must export flower_manifest",
        ),
        (tampered(b"crypto_call", b"crypto_cAll"), "may import only"),
    ] {
        let error = prepare_bundle(&bundle(&wasm), limits())
            .err()
            .expect(message);
        assert!(format!("{error:#}").contains(message), "{error:#}");
    }
    let mut hashed = bundle(COUNTER);
    hashed["hash"] = json!(crate::evaluator::hash(b"other"));
    assert!(crate::evaluator::evaluate(
        BTreeMap::new(),
        json!({"requestId":"deploy","bundle":hashed})
    )
    .unwrap_err()
    .to_string()
    .contains("hash"));
}
