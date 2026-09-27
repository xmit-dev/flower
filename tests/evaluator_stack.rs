//! Exercise native-stack exhaustion in child processes: an OS stack abort must
//! fail a test without taking down the rest of the Rust test runner.
use std::{
    collections::BTreeMap,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use flower::evaluator::{hash, invoke};
use serde_json::{Value, json};

fn isolated(name: &str, operation: fn()) {
    const CHILD: &str = "FLOWER_EVALUATOR_STACK_TEST";
    if std::env::var(CHILD).as_deref() == Ok(name) {
        // Tokio's default worker stack is similarly small. The evaluator must
        // reserve native headroom instead of assuming a large main-thread stack.
        thread::Builder::new()
            .name("bounded-evaluator-test".into())
            .stack_size(2 * 1024 * 1024)
            .spawn(operation)
            .unwrap()
            .join()
            .unwrap();
        return;
    }
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture", "--test-threads=1"])
        .env(CHILD, name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // Each fresh process compiles Wasmtime before invoking application code.
    // Cold debug builds take about 17 seconds on the test host, independently
    // of the evaluator's five-second execution deadline. Leave startup room;
    // the watchdog still catches a process that hangs or cannot unwind.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if child.try_wait().unwrap().is_some() {
            let output = child.wait_with_output().unwrap();
            assert!(
                output.status.success(),
                "{name} exited with {}\nstdout: {}\nstderr: {}",
                output.status,
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
            return;
        }
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!(
                "{name} did not return within the subprocess deadline: {}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
}

fn data(definitions: &str) -> BTreeMap<String, Value> {
    let javascript = format!(
        r#"let counter = 0;
        var __flowerBundle = {{default: {{definitions: {{{definitions},
          healthy: {{name:'healthy',kind:'queryMethod',compute: () => ++counter}}
        }}, http: {{run:{{name:'run',kind:'mutation'}},healthy:{{name:'healthy',kind:'query'}}}}}}}};"#
    );
    BTreeMap::from([(
        "bundle".into(),
        json!({"hash": hash(javascript.as_bytes()), "javascript": javascript}),
    )])
}

fn call(
    data: BTreeMap<String, Value>,
    levels: u64,
) -> anyhow::Result<flower::evaluator::Evaluation> {
    invoke(
        data,
        json!({"name":"run","args":levels,"requestId":"stack-test"}),
        "mutation",
    )
}

fn chain_data() -> BTreeMap<String, Value> {
    data(
        r#"chain: {name:'chain',kind:'derived',compute:(ctx,n) => {
          const local = ++counter;
          return local + (n > 1 ? ctx.get({kind:'derived',name:'chain'},n-1) : 0);
        }},
        run: {name:'run',kind:'mutationMethod',compute:(ctx,n) => {
          const local = ++counter;
          return {method:local,chain:ctx.get({kind:'derived',name:'chain'},n)};
        }}"#,
    )
}

#[test]
fn nested_fresh_globals_fit_a_small_native_stack() {
    isolated("nested_fresh_globals_fit_a_small_native_stack", || {
        // Unoptimized Rust/FFI frames are larger. Both variants still exercise
        // fresh globals through a nontrivial chain on the same 2 MiB OS stack.
        let levels = if cfg!(debug_assertions) { 3 } else { 24 };
        let result = call(chain_data(), levels).unwrap();
        assert_eq!(result.value, json!({"method":1,"chain":levels}));
    });
}

#[test]
fn graph_depth_boundary_returns_a_value_or_a_controlled_resource_error() {
    isolated(
        "graph_depth_boundary_returns_a_value_or_a_controlled_resource_error",
        || {
            // The graph limit remains 128; the native-stack budget can reject a
            // deep chain earlier on a platform/build with larger native frames.
            match call(chain_data(), 128) {
                Ok(result) => assert_eq!(result.value, json!({"method":1,"chain":128})),
                Err(error) => assert!(
                    error.to_string().contains("EVALUATION_BUDGET"),
                    "unexpected non-budget failure: {error}"
                ),
            }
            let error = call(chain_data(), 129).unwrap_err();
            assert!(error.to_string().contains("EVALUATION_BUDGET"), "{error}");
        },
    );
}

fn padded_data(recursive: bool) -> BTreeMap<String, Value> {
    // Keep 1,536 values live across the nested call. QuickJS stores bytecode
    // locals in a native alloca frame, making aggregate consumption adversarial
    // without requesting excessive heap allocation or unbounded execution.
    let locals = (0..1_536)
        .map(|index| format!("v{index}=n"))
        .collect::<Vec<_>>()
        .join(",");
    let uses = (0..1_536)
        .map(|index| format!("v{index}"))
        .collect::<Vec<_>>()
        .join(",");
    let nested = if recursive {
        "padding > 0 ? padded(ctx,n,padding-1) : (n > 1 ? ctx.get({kind:'derived',name:'chain'},n-1) : 0)"
    } else {
        "n > 1 ? ctx.get({kind:'derived',name:'chain'},n-1) : 0"
    };
    data(&format!(
        r#"chain: {{name:'chain',kind:'derived',compute:(ctx,n) => {{
          function padded(ctx,n,padding) {{
            let {locals};
            let result;
            try {{ result = {nested}; }} catch (_) {{ result = 0; }}
            return 1 + result + [{uses}].length * 0;
          }}
          return padded(ctx,n,6);
        }}}},
        run: {{name:'run',kind:'mutationMethod',compute:(ctx,n) => {{
          ctx.set({{kind:'collection',name:'input'}},'attempted-write',true);
          try {{ ctx.get({{kind:'derived',name:'chain'}},n); }} catch (_) {{}}
          return 'pretend success';
        }}}}"#,
    ))
}

#[test]
fn caught_padded_cell_stack_exhaustion_cannot_commit() {
    isolated("caught_padded_cell_stack_exhaustion_cannot_commit", || {
        let snapshot = padded_data(false);
        let error = call(snapshot.clone(), 96).unwrap_err();
        assert!(error.to_string().contains("EVALUATION_BUDGET"), "{error}");
        assert_eq!(snapshot.len(), 1);
        let next = invoke(snapshot, json!({"name":"healthy"}), "query").unwrap();
        assert_eq!(
            next.value, 1,
            "a rejected call must not poison the next invocation"
        );
    });
}

#[test]
fn recursive_js_frames_and_nested_cells_share_one_native_stack_budget() {
    isolated(
        "recursive_js_frames_and_nested_cells_share_one_native_stack_budget",
        || {
            let snapshot = padded_data(true);
            let error = call(snapshot.clone(), 96).unwrap_err();
            assert!(error.to_string().contains("EVALUATION_BUDGET"), "{error}");
            let next = invoke(snapshot, json!({"name":"healthy"}), "query").unwrap();
            assert_eq!(next.value, 1);
        },
    );
}

#[test]
fn ordinary_js_recursion_returns_an_error_instead_of_aborting_the_process() {
    isolated(
        "ordinary_js_recursion_returns_an_error_instead_of_aborting_the_process",
        || {
            let snapshot = data(
                r#"run: {name:'run',kind:'mutationMethod',compute:() => {
                  function recurse(n) { return 1 + recurse(n+1); }
                  return recurse(0);
                }}"#,
            );
            let error = call(snapshot.clone(), 1).unwrap_err().to_string();
            assert!(
                error.to_lowercase().contains("stack") || error.contains("EVALUATION_BUDGET"),
                "{error}"
            );
            assert_eq!(
                invoke(snapshot, json!({"name":"healthy"}), "query")
                    .unwrap()
                    .value,
                1
            );
        },
    );
}

#[test]
fn caught_js_stack_exhaustion_cannot_commit() {
    isolated("caught_js_stack_exhaustion_cannot_commit", || {
        let snapshot = data(
            r#"run: {name:'run',kind:'mutationMethod',compute:(ctx) => {
              ctx.set({kind:'collection',name:'input'},'attempted-write',true);
              function recurse(n) { return 1 + recurse(n+1); }
              try { recurse(0); } catch (_) {}
              return 'pretend success';
            }}"#,
        );
        let error = call(snapshot, 1).unwrap_err().to_string();
        assert!(error.contains("EVALUATION_BUDGET"), "{error}");
    });
}

#[test]
fn prospective_large_js_frames_fail_without_aborting_the_process() {
    isolated(
        "prospective_large_js_frames_fail_without_aborting_the_process",
        || {
            // QuickJS can refuse a prospective alloca while the actual stack is
            // still below the shared allowance. Its ordinary catchable RangeError
            // permitted this mutation on the previous thread-per-cell evaluator.
            // The Wasm C-shadow-stack guard (or a shared native-stack guard) can
            // instead trap the oversized frame. That sticky budget failure must
            // reject the transaction despite this catch.
            let locals = (0..16_000)
                .map(|index| format!("v{index}=n"))
                .collect::<Vec<_>>()
                .join(",");
            let snapshot = data(&format!(
                r#"run: {{name:'run',kind:'mutationMethod',compute:(ctx) => {{
              ctx.set({{kind:'collection',name:'input'}},'attempted-write',true);
              function padded(n) {{ let {locals}; return n + padded(n+1); }}
              try {{ padded(1); }} catch (error) {{ return String(error.message); }}
              return 'unexpected completion';
            }}}}"#,
            ));
            match call(snapshot.clone(), 1) {
                Ok(result) => {
                    assert_eq!(result.value, "Maximum call stack size exceeded");
                    assert_eq!(result.puts["source:[\"input\",\"attempted-write\"]"], true);
                }
                Err(error) => assert!(error.to_string().contains("EVALUATION_BUDGET"), "{error}"),
            }
            assert_eq!(
                invoke(snapshot, json!({"name":"healthy"}), "query")
                    .unwrap()
                    .value,
                1
            );
        },
    );
}
