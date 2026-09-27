use super::*;
use std::time::{Duration, Instant};

mod canonical_json;
#[cfg(any(target_os = "linux", target_os = "macos"))]
mod dirty;
mod invoke_results;
mod modules;
mod recycle;
mod scans;
mod values;

pub(super) fn limits() -> Arc<Limits> {
    Limits::new(Instant::now() + Duration::from_secs(30), MAX_MEMORY_BYTES)
}
pub(super) fn bundle(compute: &str, static_init: bool) -> String {
    format!(
        "{}var __flowerBundle={{default:{{definitions:{{test:{{name:'test',kind:'queryMethod',compute:{compute}}}}},http:{{test:{{name:'test',kind:'query'}}}}}}}};",
        if static_init { STATIC_INIT_MARKER } else { "" }
    )
}
pub(super) fn run(bundle: &str, args: Value) -> Result<Value> {
    execute(
        bundle,
        "test",
        &args,
        "query",
        &mut |_, _| Ok(Value::Null),
        limits(),
    )
}

#[test]
fn reserved_input_and_large_fallback_preserve_bindings_and_callback_isolation() {
    for static_init in [false, true] {
        let code = format!(
            "{}let count=0;const retained={};{}",
            if static_init { STATIC_INIT_MARKER } else { "" },
            serde_json::to_string(&"🌸".repeat(2048)).unwrap(),
            bundle("(ctx,args)=>[args,++count,retained.length]", false),
        );
        let prepared = prepare(&code, limits()).unwrap();
        if !static_init {
            assert!(prepared.bytecode.as_ref().unwrap().len() > 4096);
        }
        for args in [
            Value::Null,
            json!({"text":"hello\0🌺"}),
            json!("large 🌸".repeat(4096)),
            json!([1, 2, 3]),
        ] {
            let result = execute_prepared(
                &prepared,
                "test",
                &args,
                "query",
                &mut |_, _| Ok(Value::Null),
                limits(),
            )
            .unwrap();
            assert_eq!(result, json!({"ok":true,"value":[args,1,4096]}));
        }
    }
}

#[test]
fn snapshot_contexts_are_frozen_before_application_code_and_preserve_kind_surfaces() {
    for static_init in [false, true] {
        let source = format!(
            "{}Object.freeze=()=>{{throw new Error('application freeze must not run')}};var __flowerBundle={{default:{{definitions:{{
                child:{{name:'child',kind:'derived',compute:ctx=>[Object.isFrozen(ctx),typeof ctx.set,typeof ctx.principal,typeof ctx.history,typeof ctx.range]}},
                test:{{name:'test',kind:'queryMethod',compute:ctx=>[Object.isFrozen(ctx),typeof ctx.set,typeof ctx.principal,typeof ctx.history,typeof ctx.range]}}
            }},http:{{test:{{name:'test',kind:'query'}}}}}}}};",
            if static_init { STATIC_INIT_MARKER } else { "" },
        );
        for (name, kind, expected_writer) in [
            ("child", "derived", "undefined"),
            ("test", "query", "function"),
        ] {
            for _ in 0..2 {
                let result = execute(
                    &source,
                    name,
                    &Value::Null,
                    kind,
                    &mut |_, _| Ok(Value::Null),
                    limits(),
                )
                .unwrap();
                assert_eq!(
                    result["value"],
                    json!([true, expected_writer, "function", "function", "function"])
                );
            }
        }
    }
}

#[test]
fn unicode_host_values_and_business_errors_round_trip() {
    assert_eq!(
        failure_parts(anyhow::Error::new(
            super::super::rust_engine::EngineError::new("custom-code:1", "business failure")
        )),
        ("custom-code:1".into(), "business failure".into(), None)
    );
    let code = bundle(
        "(ctx,args)=>({args,record:ctx.get({kind:'collection',name:'字🌻'},'a\\u0000b')})",
        false,
    );
    let args = json!({"😀": ["a\0b", "日本語"], "\u{e000}": true});
    let mut calls = 0;
    let result = execute(
        &code,
        "test",
        &args,
        "query",
        &mut |method, payload| {
            calls += 1;
            assert_eq!(method, "get");
            assert_eq!(payload, json!([{"kind":"collection","name":"字🌻"},"a\0b"]));
            Ok(json!({"😀":"payload\0é","\u{e000}":2}))
        },
        limits(),
    )
    .unwrap();
    assert_eq!(calls, 1);
    assert_eq!(result["value"]["args"], args);
    assert_eq!(result["value"]["record"]["😀"], "payload\0é");
    let result = execute(
        &code,
        "test",
        &args,
        "query",
        &mut |_, _| anyhow::bail!("LEASE_STALE: lease no longer belongs to worker"),
        limits(),
    )
    .unwrap();
    assert_eq!(
        result["error"],
        json!({"code":"LEASE_STALE","message":"lease no longer belongs to worker"})
    );
}

#[test]
fn initialized_snapshots_isolate_closures_globals_and_prototypes_and_rebind_host() {
    let code = format!(
        "{STATIC_INIT_MARKER}let count=0;globalThis.box={{n:0}};{}",
        bundle(
            "ctx=>{const prior=Object.prototype.leak||null;Object.prototype.leak=9;return [++count,++box.n,prior,ctx.get({kind:'collection',name:'x'},'a')]}",
            false
        )
    );
    for host_value in [1, 40, 99] {
        let result = execute(
            &code,
            "test",
            &Value::Null,
            "query",
            &mut |_, _| Ok(json!(host_value)),
            limits(),
        )
        .unwrap();
        assert_eq!(result["value"], json!([1, 1, null, host_value]));
    }
    let manifest = manifest(&code, limits()).unwrap();
    assert_eq!(manifest["http"]["test"]["name"], "test");
}

#[test]
fn invocation_data_and_the_host_reach_only_callbacks() {
    let code = "const globals=[typeof __flowerHost,typeof __flowerRead,typeof __flowerSetRunner,typeof __argsJson,typeof __name,typeof __kind];var __flowerBundle={default:{definitions:{test:{name:'test',kind:'queryMethod',compute:(ctx,args)=>[args,ctx.get({kind:'collection',name:'x'},'a'),globals]}},http:{}}};";
    for static_init in ["", STATIC_INIT_MARKER] {
        for number in [1, 2] {
            let result = execute(
                &format!("{static_init}{code}"),
                "test",
                &json!(number),
                "query",
                &mut |_, _| Ok(json!(number + 10)),
                limits(),
            )
            .unwrap();
            assert_eq!(
                result["value"],
                json!([number, number + 10, vec!["undefined"; 6]])
            );
        }
    }
}

#[test]
fn nested_guests_share_budget_and_keep_callback_state_separate() {
    let code = bundle("ctx=>ctx.get({kind:'collection',name:'x'},'a')", false);
    let shared = limits();
    let result = execute(
        &code,
        "test",
        &Value::Null,
        "query",
        &mut |_, _| {
            let result = execute(
                &code,
                "test",
                &Value::Null,
                "query",
                &mut |_, _| Ok(json!("nested")),
                shared.clone(),
            )?;
            Ok(result["value"].clone())
        },
        shared.clone(),
    )
    .unwrap();
    assert_eq!(result["value"], "nested");
    shared.check().unwrap();
}

#[test]
fn deadline_stops_infinite_loop_even_when_guest_catches() {
    warmup().unwrap();
    let code = bundle("()=>{try {for(;;){}}catch(e){};return 1}", false);
    // Warm compilation separately; the measured limit covers only invocation.
    cache::runtime().unwrap().prepare(&code, limits()).unwrap();
    let start = Instant::now();
    assert!(execute(
        &code,
        "test",
        &Value::Null,
        "query",
        &mut |_, _| Ok(Value::Null),
        Limits::new(start + Duration::from_millis(50), MAX_MEMORY_BYTES)
    )
    .is_err());
    assert!(start.elapsed() < Duration::from_secs(2));
}

#[test]
fn memory_and_stack_failures_cannot_be_swallowed() {
    for compute in [
        "()=>{try{new ArrayBuffer(1024*1024*1024)}catch(e){};return 1}",
        "()=>{try{(function f(){return 1+f()})()}catch(e){};return 1}",
    ] {
        let code = bundle(compute, false);
        let shared = limits();
        assert!(
            execute(
                &code,
                "test",
                &Value::Null,
                "query",
                &mut |_, _| Ok(Value::Null),
                shared.clone()
            )
            .is_err(),
            "{compute}"
        );
        assert!(shared.check().is_err(), "resource failure must be sticky");
    }
}

#[test]
fn deep_json_is_bounded_without_rejecting_valid_envelope_depth() {
    let code = bundle("(_ctx,args)=>args", false);
    let mut value = json!("leaf");
    for _ in 0..127 {
        value = json!([value]);
    }
    assert_eq!(run(&code, value.clone()).unwrap()["value"], value);
    assert!(run(&code, json!([value])).is_err());
}

#[test]
fn aggregate_linear_memory_counts_simultaneous_nested_guests() {
    let code = bundle(
        "ctx=>{globalThis.keep=new ArrayBuffer(3*1024*1024);return ctx.get({kind:'collection',name:'x'},'a')}",
        false,
    );
    let shared = Limits::new(Instant::now() + Duration::from_secs(30), 8 * 1024 * 1024);
    assert!(execute(
        &code,
        "test",
        &Value::Null,
        "query",
        &mut |_, _| {
            execute(
                &code,
                "test",
                &Value::Null,
                "query",
                &mut |_, _| Ok(Value::Null),
                shared.clone(),
            )
        },
        shared.clone()
    )
    .is_err());
    assert!(shared.check().is_err());
}

#[test]
fn key_enumeration_matches_utf16_and_numeric_property_rules() {
    let code = bundle(
        "(ctx,args)=>[Object.keys(args),Object.keys(ctx.get({kind:'collection',name:'x'},'a'))]",
        false,
    );
    let value = json!({"😀":1,"\u{e000}":2,"10":3,"2":4});
    let result = execute(
        &code,
        "test",
        &value,
        "query",
        &mut |_, _| Ok(value.clone()),
        limits(),
    )
    .unwrap();
    assert_eq!(
        result["value"],
        json!([["2", "10", "😀", "\u{e000}"], ["2", "10", "😀", "\u{e000}"]])
    );
}

#[test]
fn large_guest_c_frames_cannot_corrupt_the_shadow_stack_or_hide_failure() {
    // Bytecode locals use variable-sized C frames in linear memory, independent
    // of Wasmtime's native-call-stack limit. Keep 240 KiB live across recursive
    // guest calls and verify that even a catch cannot turn exhaustion into success.
    let locals = (0..30_000)
        .map(|index| format!("v{index}=n"))
        .collect::<Vec<_>>()
        .join(",");
    let uses = (0..30_000)
        .map(|index| format!("v{index}"))
        .collect::<Vec<_>>()
        .join(",");
    let code = bundle(
        &format!(
            "()=>{{function recurse(n){{let {locals}; const value=n?recurse(n-1):0;return value+[{uses}].length}}try{{recurse(8)}}catch(_){{}}return 'pretend success'}}"
        ),
        false,
    );
    let shared = limits();
    assert!(execute(
        &code,
        "test",
        &Value::Null,
        "query",
        &mut |_, _| Ok(Value::Null),
        shared.clone()
    )
    .is_err());
    assert!(shared.check().is_err());
    assert_eq!(
        run(&bundle("()=>1", false), Value::Null).unwrap()["value"],
        1
    );
}

#[test]
fn minimal_guest_exposes_only_deterministic_javascript_intrinsics() {
    let code = bundle(
        "()=>{let randomBlocked=false;try{Math.random()}catch(_){randomBlocked=true}return {ambient:[typeof performance,typeof Date,typeof crypto,typeof fetch,typeof process,typeof setTimeout],randomBlocked,base64:btoa(atob('Zmxvd2Vy')),features:[String(1n+2n),new Map([['x',2]]).get('x'),/flower/.test('flower')]}}",
        true,
    );
    assert_eq!(
        run(&code, Value::Null).unwrap()["value"],
        json!({
            "ambient": ["undefined","undefined","undefined","undefined","undefined","undefined"],
            "randomBlocked":true,"base64":"Zmxvd2Vy","features":["3",2,true]
        })
    );
}

#[test]
fn snapshotted_api_functions_are_isolated_and_application_freeze_hooks_are_not_called() {
    let code = format!(
        "{STATIC_INIT_MARKER}const originalFreeze=Object.freeze;let freezes=0;Object.freeze=function(value){{++freezes;return originalFreeze(value)}};{}",
        bundle(
            "ctx=>{const before=ctx.get.marker??null;ctx.get.marker=1;return {before,freezes,frozen:Object.isFrozen(ctx),keys:Object.keys(ctx)}}",
            false
        )
    );
    // API contexts now belong to the trusted image. Application hooks must not
    // intercept their construction, and mutations to function objects remain
    // local to a single fresh Wasm invocation.
    let expected = json!({"ok":true,"value":{
        "before":null,"freezes":0,"frozen":true,
        "keys":["now","clock","changesAt","principal","history","get","scan","query","range","set","delete","materialize","unmaterialize"]
    }});
    for _ in 0..3 {
        assert_eq!(run(&code, Value::Null).unwrap(), expected);
    }
}

#[test]
fn bootstrap_bindings_cannot_be_reached_or_shadowed_by_application_code() {
    for static_init in [false, true] {
        for (declarations, visible) in [
            (
                "let __flowerHost=()=>1;let __flowerSetRunner=()=>1;",
                "function",
            ),
            (
                "var __flowerHost=()=>1;var __flowerSetRunner=()=>1;",
                "function",
            ),
            (
                "let globalThis={__flowerHost:()=>1,__flowerSetRunner:()=>1};",
                "undefined",
            ),
        ] {
            let code = format!(
                "{}{declarations}{}",
                if static_init { STATIC_INIT_MARKER } else { "" },
                bundle(
                    "ctx=>[ctx.get({kind:'collection',name:'x'},'a'),typeof __flowerHost,typeof __flowerSetRunner]",
                    false
                )
            );
            let mut calls = Vec::new();
            let actual = execute(
                &code,
                "test",
                &Value::Null,
                "query",
                &mut |method, args| {
                    calls.push((method.to_owned(), args));
                    Ok(json!("record"))
                },
                limits(),
            )
            .unwrap();
            assert_eq!(
                (actual, calls),
                (
                    json!({"ok":true,"value":["record",visible,visible]}),
                    vec![(
                        "get".to_owned(),
                        json!([{"kind":"collection","name":"x"},"a"])
                    )]
                ),
                "{static_init}: {declarations}"
            );
        }
    }
}
