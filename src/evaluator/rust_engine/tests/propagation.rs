use super::*;

fn visible(result: &Evaluation) -> Value {
    json!({"puts":result.puts,"deletes":result.deletes,"value":result.value})
}
fn chain() -> Fixture {
    Fixture::new([
        (
            "leaf",
            (|_, host| {
                let n = get(host, "collection", "input", json!("n"))?;
                if n.as_i64().unwrap() < 0 {
                    return Err(EngineError::new("NEGATIVE", "negative input"));
                }
                Ok(json!(n.as_u64().unwrap() % 2))
            }) as Callback,
        ),
        (
            "middle",
            (|_, host| get(host, "derived", "leaf", Value::Null)) as Callback,
        ),
        (
            "top",
            (|_, host| {
                Ok(json!({"value":get(host,"derived","middle",Value::Null).unwrap_or(Value::Null)}))
            }) as Callback,
        ),
        (
            "read",
            (|_, host| get(host, "derived", "top", Value::Null)) as Callback,
        ),
        (
            "set",
            (|args, host| {
                set(host, "input", "n", args.clone())?;
                Ok(Value::Null)
            }) as Callback,
        ),
    ])
}
#[test]
fn equal_outcomes_stop_propagation_and_preserve_cached_root_certificates() {
    let fixture = chain();
    let mut data = Records::new();
    deploy(
        &mut data,
        json!({"writes":[{"collection":"input","key":"n","value":2}],"materialize":[{"name":"top"}]}),
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
    let certificate = read.query_certificate.unwrap();
    let result = run(
        data.clone(),
        json!({"name":"set","args":4}),
        "mutation",
        None,
        &fixture,
    )
    .unwrap();
    assert_eq!(result.evaluated, vec![cell_id("leaf", &Value::Null)]);
    apply(&mut data, result);
    assert!(certificate.valid(&data));
    let result = run(
        data.clone(),
        json!({"name":"set","args":5}),
        "mutation",
        None,
        &fixture,
    )
    .unwrap();
    assert_eq!(result.evaluated.len(), 3);
    apply(&mut data, result);
    assert!(!certificate.valid(&data));
}
#[test]
fn optimized_outcomes_match_full_propagation_across_changes_errors_and_restart() {
    let fixture = chain();
    let mut fast = Records::new();
    deploy(
        &mut fast,
        json!({"writes":[{"collection":"input","key":"n","value":2}],"materialize":[{"name":"top"}]}),
        &fixture,
    );
    let mut full = fast.clone();
    for (step, value) in [4, 6, 7, 9, -1, -2, 10, 12, 13, 15, 15, 16]
        .into_iter()
        .enumerate()
    {
        let input = json!({"name":"set","args":value});
        let actual = run(fast.clone(), input.clone(), "mutation", None, &fixture).unwrap();
        let expected =
            graph::with_full_propagation(|| run(full.clone(), input, "mutation", None, &fixture))
                .unwrap();
        assert_eq!(visible(&actual), visible(&expected), "step {step}");
        assert!(actual.evaluated.len() <= expected.evaluated.len());
        apply(&mut fast, actual);
        apply(&mut full, expected);
        assert_eq!(fast, full);
        if step == 5 {
            fast = serde_json::from_slice(&serde_json::to_vec(&fast).unwrap()).unwrap();
        }
    }
}
#[test]
fn unchanged_branch_output_updates_dependencies_and_collects_old_cells() {
    let fixture = Fixture::new([
        (
            "a",
            (|_, host| get(host, "collection", "input", json!("a"))) as Callback,
        ),
        (
            "b",
            (|_, host| get(host, "collection", "input", json!("b"))) as Callback,
        ),
        (
            "choose",
            (|_, host| {
                let branch = get(host, "collection", "input", json!("branch"))?;
                get(
                    host,
                    "derived",
                    if branch == true { "b" } else { "a" },
                    Value::Null,
                )
            }) as Callback,
        ),
        (
            "top",
            (|_, host| get(host, "derived", "choose", Value::Null)) as Callback,
        ),
    ]);
    let mut data = Records::new();
    deploy(
        &mut data,
        json!({"writes":[{"collection":"input","key":"a","value":1},{"collection":"input","key":"b","value":1},{"collection":"input","key":"branch","value":false}],"materialize":[{"name":"top"}]}),
        &fixture,
    );
    let result = deploy(
        &mut data,
        json!({"writes":[{"collection":"input","key":"branch","value":true}]}),
        &fixture,
    );
    assert!(!result.evaluated.contains(&cell_id("top", &Value::Null)));
    assert!(data.get(&cell_id("a", &Value::Null)).is_none());
    assert!(data.get(&cell_id("b", &Value::Null)).is_some());
    assert_eq!(
        data[&cell_id("choose", &Value::Null)]["deps"],
        json!([cell_id("b", &Value::Null), source_id("input", "branch")])
    );
    let result = deploy(
        &mut data,
        json!({"writes":[{"collection":"input","key":"b","value":2}]}),
        &fixture,
    );
    assert!(result.evaluated.contains(&cell_id("top", &Value::Null)));
    assert_eq!(data[&cell_id("top", &Value::Null)]["outcome"]["value"], 2);
}
#[test]
fn multiple_changed_dependencies_preserve_application_branch_order_and_cycle_semantics() {
    let fixture = Fixture::new([
        (
            "zgate",
            (|_, host| get(host, "collection", "input", json!("mode"))) as Callback,
        ),
        (
            "abranch",
            (|_, host| {
                if get(host, "collection", "input", json!("mode"))? == true {
                    get(host, "derived", "top", Value::Null)
                } else {
                    Ok(json!(1))
                }
            }) as Callback,
        ),
        (
            "top",
            (|_, host| {
                if get(host, "derived", "zgate", Value::Null)? == true {
                    Ok(json!(99))
                } else {
                    get(host, "derived", "abranch", Value::Null)
                }
            }) as Callback,
        ),
    ]);
    let mut data = Records::new();
    deploy(
        &mut data,
        json!({"writes":[{"collection":"input","key":"mode","value":false}],"materialize":[{"name":"top"}]}),
        &fixture,
    );
    // The obsolete abranch would now form a cycle if the optimizer eagerly
    // refreshed old dependencies in sorted order before application control flow.
    deploy(
        &mut data,
        json!({"writes":[{"collection":"input","key":"mode","value":true}]}),
        &fixture,
    );
    assert_eq!(data[&cell_id("top", &Value::Null)]["outcome"]["value"], 99);
    assert!(data.get(&cell_id("abranch", &Value::Null)).is_none());
}
