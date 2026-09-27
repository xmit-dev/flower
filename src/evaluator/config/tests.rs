use super::*;
use crate::{consensus::Records, evaluator};
use serde_json::{json, Value};

#[test]
fn validates_only_budgets_and_representation_constraints() {
    let read = |pairs: &[(&str, &str)]| {
        Settings::read(|key| {
            Ok(pairs
                .iter()
                .find(|(name, _)| *name == key)
                .map(|(_, value)| (*value).into()))
        })
    };
    let defaults = read(&[]).unwrap();
    assert_eq!(defaults.evaluation_timeout, Duration::from_secs(10));
    assert_eq!(defaults.index_memory_bytes, 16 * 1024 * 1024);
    assert_eq!(defaults.wasm_recycle_bytes, 96 * 1024 * 1024);
    for key in KEYS {
        for invalid in ["", "-1", "+1", "1.5", " 1", "18446744073709551616"] {
            assert!(read(&[(key, invalid)]).is_err(), "{key}={invalid}");
        }
        if key != "FLOWER_WASM_RECYCLE_BYTES" {
            assert!(read(&[(key, "0")]).is_err(), "{key}=0");
        }
    }
    assert_eq!(read(&[(KEYS[7], "0")]).unwrap().wasm_recycle_bytes, 0);
    assert_eq!(read(&[(KEYS[7], "1")]).unwrap().wasm_recycle_bytes, 1);
    assert_eq!(
        read(&[(KEYS[7], "201326592")]).unwrap().wasm_recycle_bytes,
        192 * 1024 * 1024
    );
    assert!(read(&[(KEYS[7], &(isize::MAX as u64 + 1).to_string())]).is_err());
    assert!(read(&[(KEYS[1], "2147483647"), (KEYS[3], "4294967296")]).is_err());
    assert!(read(&[(KEYS[3], "4294967297")]).is_err());
    assert!(read(&[(KEYS[6], "4294967296")]).is_err());
    assert!(read(&[(KEYS[2], "134217729")]).is_err());
    let large = read(&[
        (KEYS[0], "90000"),
        (KEYS[1], "4194304"),
        (KEYS[2], "33554432"),
        (KEYS[3], "4294967296"),
        (KEYS[4], "536870912"),
        (KEYS[5], "67108864"),
        (KEYS[6], "1024"),
    ])
    .unwrap();
    assert_eq!(large.guest_memory_bytes, 1usize << 32);
    assert_eq!(large.pool_memory_bytes(), 1usize << 32);
    assert_eq!(large.evaluation_timeout, Duration::from_secs(90));
}

fn source(compute: &str) -> String {
    format!(
        "var __flowerBundle={{default:{{definitions:{{test:{{name:'test',kind:'queryMethod',compute:{compute}}}}},http:{{test:{{name:'test',kind:'query'}}}}}}}};"
    )
}

fn invoke(code: &str, args: Value) -> anyhow::Result<evaluator::Evaluation> {
    let mut data = Records::default();
    data.insert("bundle".into(), json!({"javascript":code}));
    evaluator::invoke(data, json!({"name":"test","args":args}), "query")
}

#[test]
fn environment_worker() {
    let Ok(case) = std::env::var("FLOWER_CONFIG_TEST_CASE") else {
        return;
    };
    if case == "invalid" {
        assert!(settings()
            .unwrap_err()
            .to_string()
            .contains("FLOWER_RESULT_MAX_BYTES"));
        assert!(evaluator::warmup().is_err());
        return;
    }
    evaluator::warmup().unwrap();
    match case.as_str() {
        "raised" => {
            let mut code = source(
                "(_ctx,args)=>args==='memory' ? new ArrayBuffer(140*1024*1024).byteLength : 'x'.repeat(17*1024*1024)",
            );
            code.push_str("/*");
            code.extend(std::iter::repeat_n(' ', 2 * 1024 * 1024));
            code.push_str("*/");
            evaluator::validate_mutation(&json!({"requestId":"x".repeat(1024),"bundle":{"hash":evaluator::hash(code.as_bytes()),"javascript":code}})).unwrap();
            assert_eq!(
                invoke(&code, json!("memory")).unwrap().value,
                json!(140 * 1024 * 1024)
            );
            assert_eq!(
                invoke(&code, Value::Null)
                    .unwrap()
                    .value
                    .as_str()
                    .unwrap()
                    .len(),
                17 * 1024 * 1024
            );
        }
        "timeout" => {
            let began = Instant::now();
            let error = invoke(&source("()=>{for(;;){}}"), Value::Null).unwrap_err();
            assert!(format!("{error:#}").contains("EVALUATION_BUDGET"));
            assert!(
                began.elapsed() < Duration::from_secs(2),
                "configured deadline was ignored"
            );
        }
        "result" => {
            let error = invoke(&source("()=> 'x'.repeat(2048)"), Value::Null).unwrap_err();
            assert!(
                format!("{error:#}").contains("limit")
                    || format!("{error:#}").contains("FLOWER_RESULT_MAX_BYTES")
            );
        }
        "memory" => {
            let error = invoke(
                &source("()=> new ArrayBuffer(32*1024*1024).byteLength"),
                Value::Null,
            )
            .unwrap_err();
            assert!(format!("{error:#}").contains("EVALUATION_BUDGET"));
        }
        "rust" => {
            let code = source("ctx=>{try {for(let i=0;i<1000;i++) ctx.get({kind:'derived',name:'cell'},i)}catch(_){}return 1}")
                .replace("definitions:{", "definitions:{cell:{name:'cell',kind:'derived',compute:()=>1},");
            let error = invoke(&code, Value::Null).unwrap_err();
            assert!(format!("{error:#}").contains("EVALUATION_BUDGET"));
        }
        "index" => {
            let code = source(
                "ctx=>ctx.query({kind:'query',collection:'items',fields:['group'],value:1})",
            );
            let mut data = Records::default();
            data.insert("bundle".into(), json!({"javascript":code}));
            for n in 0..10 {
                data.insert(
                    format!("source:[\"items\",\"{n}\"]"),
                    json!({"group":n%2,"n":n}),
                );
            }
            let result = evaluator::invoke(data, json!({"name":"test"}), "query").unwrap();
            assert_eq!(result.value.as_array().unwrap().len(), 5);
        }
        _ => panic!("unknown child case"),
    }
}

#[test]
fn environment_budgets_are_enforced_in_fresh_processes() {
    if std::env::var_os("FLOWER_CONFIG_TEST_CASE").is_some() {
        return;
    }
    for (case, settings) in [
        ("invalid", vec![(KEYS[2], "0")]),
        (
            "raised",
            vec![
                (KEYS[0], "30000"),
                (KEYS[1], "3145728"),
                (KEYS[2], "20971520"),
                (KEYS[3], "268435456"),
                (KEYS[4], "268435456"),
            ],
        ),
        ("timeout", vec![(KEYS[0], "20")]),
        ("result", vec![(KEYS[2], "1024")]),
        ("memory", vec![(KEYS[3], "16777216")]),
        ("rust", vec![(KEYS[2], "4096"), (KEYS[4], "16384")]),
        ("index", vec![(KEYS[5], "1")]),
    ] {
        let mut command = std::process::Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "evaluator::config::tests::environment_worker",
                "--nocapture",
            ])
            .env("FLOWER_CONFIG_TEST_CASE", case);
        for key in KEYS {
            command.env_remove(key);
        }
        for (key, value) in settings {
            command.env(key, value);
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{case}: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
