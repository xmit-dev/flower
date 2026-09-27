use super::*;
use crate::evaluator::rust_engine::indexes::IndexSpec;

fn schema(aggregate: bool) -> Schema {
    let index = IndexSpec {
        collection: "orders".into(),
        fields: vec!["shop".into()],
    };
    Schema {
        policies: Default::default(),
        derived_access: Default::default(),
        indexes: vec![index.clone()],
        aggregates: if aggregate {
            BTreeMap::from([("total".into(), index)])
        } else {
            BTreeMap::new()
        },
    }
}
fn deploy_schema(
    data: &mut Records,
    mut command: Value,
    schema: Schema,
    fixture: &dyn Executor,
) -> Evaluation {
    command["requestId"] = json!("deploy-indexes");
    let evaluation = run_with_schema(
        data.clone(),
        command,
        "deployment",
        None,
        fixture,
        Some(schema),
    )
    .unwrap();
    apply(
        data,
        Evaluation {
            puts: evaluation.puts.clone(),
            deletes: evaluation.deletes.clone(),
            evaluated: vec![],
            value: Value::Null,
            query_cacheable: false,
            query_clock_polled: false,
            query_changes_at: None,
            query_certificate: None,
            mutation_certificate: None,
        },
    );
    evaluation
}
fn query(host: &mut Host<'_>, group: Value) -> EngineResult<Value> {
    host(
        "query",
        json!([{"kind":"query","collection":"orders","fields":["shop"],"value":group}]),
    )
}
fn total<'a>(data: &'a Records, group: &str) -> &'a Value {
    &data[&cell_id("total", &json!(group))]["outcome"]["value"]
}
#[derive(Default)]
struct Reducer {
    payloads: RefCell<Vec<Value>>,
}
impl Executor for Reducer {
    fn execute(
        &self,
        kind: &str,
        name: &str,
        args: &Value,
        host: &mut Host<'_>,
    ) -> EngineResult<Value> {
        if kind == "derived" && name == "total" {
            self.payloads.borrow_mut().push(args.clone());
            let mut sum = if args["initialize"] == true {
                0
            } else {
                args["previous"].as_i64().unwrap()
            };
            for change in args["changes"].as_array().unwrap() {
                if let Some(row) = change.get("old") {
                    sum -= row["cents"].as_i64().unwrap();
                }
                if let Some(row) = change.get("new") {
                    if row["fail"] == true {
                        return Err(EngineError::new("BAD_ROW", "row rejected"));
                    }
                    sum += row["cents"].as_i64().unwrap();
                }
            }
            return Ok(json!(sum));
        }
        match name {
            "change" | "rollback" => {
                let mut values = vec![];
                for row in args.as_array().unwrap() {
                    if row.get("value").is_some() {
                        set(
                            host,
                            "orders",
                            row["key"].as_str().unwrap(),
                            row["value"].clone(),
                        )?;
                    } else {
                        host(
                            "delete",
                            json!([{"kind":"collection","name":"orders"},row["key"]]),
                        )?;
                    }
                    if row["preview"] == true {
                        values.push(get(host, "derived", "total", json!("a"))?);
                    }
                }
                if name == "rollback" {
                    return Err(EngineError::new("ROLLBACK", "rollback after preview"));
                }
                Ok(json!(values))
            }
            "read" => get(host, "derived", "total", args.clone()),
            _ => Err(EngineError::new("DEFINITION_MISSING", name)),
        }
    }
}

/// Index entries of either kind: scalar values have ordered entries only.
fn index_entries(data: &Records) -> Vec<String> {
    data.keys()
        .filter(|id| id.starts_with("index-entry:") || id.starts_with("ordered-entry:"))
        .collect()
}

#[test]
fn durable_index_builds_existing_rows_and_tracks_only_matching_buckets() {
    let fixture = Fixture::new([(
        "matches",
        (|args, host| query(host, args.clone())) as Callback,
    )]);
    let mut data = Records::default();
    for (key, row) in [
        ("z", json!({"shop":"a","n":1})),
        ("😀", json!({"shop":"a","n":2})),
        ("\u{e000}", json!({"shop":"a","n":3})),
        ("other", json!({"shop":"b"})),
        ("missing", json!({"other":1})),
        ("null", json!({"shop":null})),
    ] {
        data.insert(source_id("orders", key), row);
    }
    deploy_schema(
        &mut data,
        json!({"materialize":[{"name":"matches","args":"a"},{"name":"matches","args":"empty"},{"name":"matches","args":null}]}),
        schema(false),
        &fixture,
    );
    assert_eq!(index_entries(&data).len(), 5);
    assert!(!data.keys().any(|id| id.starts_with("index-entry:")));
    assert_eq!(
        data[&cell_id("matches", &json!("a"))]["outcome"]["value"],
        json!([{"shop":"a","n":1},{"shop":"a","n":2},{"shop":"a","n":3}])
    );
    assert_eq!(
        data[&cell_id("matches", &Value::Null)]["outcome"]["value"],
        json!([{"shop":null}])
    );
    let result = deploy(
        &mut data,
        json!({"writes":[{"collection":"orders","key":"other","value":{"shop":"b","n":10}}]}),
        &fixture,
    );
    assert!(
        result.evaluated.is_empty(),
        "unrelated bucket must not invalidate query"
    );
    let result = deploy(
        &mut data,
        json!({"writes":[{"collection":"orders","key":"new","value":{"shop":"empty"}}]}),
        &fixture,
    );
    assert_eq!(result.evaluated, vec![cell_id("matches", &json!("empty"))]);
    assert_eq!(
        data[&cell_id("matches", &json!("empty"))]["outcome"]["value"],
        json!([{"shop":"empty"}])
    );
}

#[test]
fn indexed_methods_overlay_pending_writes_and_remove_old_entries() {
    let fixture = Fixture::new([(
        "work",
        (|_, host| {
            set(host, "orders", "old", json!({"shop":"b"}))?;
            set(host, "orders", "new", json!({"shop":"a"}))?;
            host(
                "delete",
                json!([{"kind":"collection","name":"orders"},"deleted"]),
            )?;
            query(host, json!("a"))
        }) as Callback,
    )]);
    let mut data = Records::default();
    deploy_schema(
        &mut data,
        json!({"writes":[{"collection":"orders","key":"old","value":{"shop":"a"}},{"collection":"orders","key":"deleted","value":{"shop":"a"}}]}),
        schema(false),
        &fixture,
    );
    let result = run(
        data.clone(),
        json!({"name":"work","requestId":"overlay"}),
        "mutation",
        None,
        &fixture,
    )
    .unwrap();
    assert_eq!(result.value, json!([{"shop":"a"}]));
    apply(&mut data, result);
    assert_eq!(index_entries(&data).len(), 2);
    assert!(index_entries(&data).iter().all(|id| !id.ends_with(":deleted")));
    deploy_schema(&mut data, json!({}), Schema::default(), &fixture);
    assert!(index_entries(&data).is_empty());
    assert!(data.get("schema").is_none());
}

#[test]
fn composite_index_distinguishes_absent_and_null_and_canonicalizes_objects() {
    let fixture = Fixture::new([(
        "read",
        (|_, host| {
            host(
                "query",
                json!([{"kind":"query","collection":"orders","fields":["shop","active"],"value":[{"b":2,"a":1},null]}]),
            )
        }) as Callback,
    )]);
    let mut data = Records::default();
    let schema = Schema {
        policies: Default::default(),
        derived_access: Default::default(),
        indexes: vec![IndexSpec {
            collection: "orders".into(),
            fields: vec!["shop".into(), "active".into()],
        }],
        ..Schema::default()
    };
    deploy_schema(
        &mut data,
        json!({"writes":[{"collection":"orders","key":"match","value":{"shop":{"a":1,"b":2},"active":null}},{"collection":"orders","key":"missing","value":{"shop":{"b":2,"a":1}}}]}),
        schema,
        &fixture,
    );
    let result = run(data, json!({"name":"read"}), "query", None, &fixture).unwrap();
    assert_eq!(result.value, json!([{"shop":{"a":1,"b":2},"active":null}]));
}

#[test]
fn aggregates_apply_only_changed_rows_and_survive_serialized_state() {
    let fixture = Reducer::default();
    let mut data = Records::default();
    let writes: Vec<_> = (0..100).map(|key| json!({"collection":"orders","key":key.to_string(),"value":{"shop":"a","cents":10}})).collect();
    deploy_schema(
        &mut data,
        json!({"writes":writes,"materialize":[{"name":"total","args":"a"},{"name":"total","args":"b"}]}),
        schema(true),
        &fixture,
    );
    assert_eq!(total(&data, "a"), 1000);
    assert_eq!(total(&data, "b"), 0);
    fixture.payloads.borrow_mut().clear();
    // Round-trip the persisted record image; no evaluator-local index or
    // accumulator cache is necessary for resumed incremental maintenance.
    let stored: BTreeMap<String, Value> = serde_json::from_slice(
        &serde_json::to_vec(
            &data
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<BTreeMap<_, _>>(),
        )
        .unwrap(),
    )
    .unwrap();
    data = stored.into();
    let result = run(data.clone(), json!({"name":"change","requestId":"delta","args":[{"key":"7","value":{"shop":"a","cents":20}}]}), "mutation", None, &fixture).unwrap();
    apply(&mut data, result);
    assert_eq!(total(&data, "a"), 1010);
    let payloads = fixture.payloads.borrow();
    assert_eq!(payloads.len(), 1);
    assert_eq!(payloads[0]["initialize"], false);
    assert_eq!(payloads[0]["changes"].as_array().unwrap().len(), 1);
    assert_eq!(payloads[0]["changes"][0]["old"]["cents"], 10);
    drop(payloads);
    let result = run(data.clone(), json!({"name":"change","requestId":"move","args":[{"key":"7","value":{"shop":"b","cents":20}},{"key":"8"}]}), "mutation", None, &fixture).unwrap();
    apply(&mut data, result);
    assert_eq!(total(&data, "a"), 980);
    assert_eq!(total(&data, "b"), 20);
}

#[test]
fn aggregate_previews_do_not_double_apply_deltas_and_errors_rebuild() {
    let fixture = Reducer::default();
    let mut data = Records::default();
    deploy_schema(
        &mut data,
        json!({"materialize":[{"name":"total","args":"a"}]}),
        schema(true),
        &fixture,
    );
    let result = run(data.clone(), json!({"name":"change","requestId":"previews","args":[{"key":"1","value":{"shop":"a","cents":3},"preview":true},{"key":"1","value":{"shop":"a","cents":5},"preview":true},{"key":"2","value":{"shop":"a","cents":8}}]}), "mutation", None, &fixture).unwrap();
    assert_eq!(result.value, json!([3, 5]));
    apply(&mut data, result);
    assert_eq!(total(&data, "a"), 13);
    let before = data.clone();
    assert_eq!(run(data.clone(), json!({"name":"rollback","requestId":"rollback","args":[{"key":"1","value":{"shop":"a","cents":100},"preview":true}]}), "mutation", None, &fixture).unwrap_err().code,"ROLLBACK");
    assert_eq!(total(&before, "a"), 13);
    let result = run(data.clone(), json!({"name":"change","requestId":"bad","args":[{"key":"1","value":{"shop":"a","cents":5,"fail":true}}]}), "mutation", None, &fixture).unwrap();
    apply(&mut data, result);
    assert_eq!(
        data[&cell_id("total", &json!("a"))]["outcome"]["error"]["code"],
        "BAD_ROW"
    );
    fixture.payloads.borrow_mut().clear();
    let result = run(data.clone(), json!({"name":"change","requestId":"repair","args":[{"key":"1","value":{"shop":"a","cents":6}}]}), "mutation", None, &fixture).unwrap();
    apply(&mut data, result);
    assert_eq!(total(&data, "a"), 14);
    assert_eq!(fixture.payloads.borrow()[0]["initialize"], true);
    assert_eq!(
        fixture.payloads.borrow()[0]["changes"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    fixture.payloads.borrow_mut().clear();
    deploy_schema(
        &mut data,
        json!({"bundle":{"hash":"new-code","javascript":"new-code"}}),
        schema(true),
        &fixture,
    );
    assert_eq!(fixture.payloads.borrow()[0]["initialize"], true);
    assert_eq!(total(&data, "a"), 14);
}

#[test]
fn aggregate_context_access_is_fatal_even_if_callback_catches_it() {
    let fixture = Fixture::new([(
        "total",
        (|_, host| {
            let _ = host("now", json!([]));
            Ok(json!(0))
        }) as Callback,
    )]);
    let error = run_with_schema(
        Records::default(),
        json!({"requestId":"forbidden","materialize":[{"name":"total","args":"a"}]}),
        "deployment",
        None,
        &fixture,
        Some(schema(true)),
    )
    .unwrap_err();
    assert_eq!(error.code, "AGGREGATE_CONTEXT_FORBIDDEN");
}

#[test]
fn persisted_index_corruption_is_rejected_and_budget_failure_cannot_be_caught() {
    let fixture = Fixture::new([(
        "read",
        (|_, host| {
            let _ = query(host, json!("a"));
            Ok(Value::Null)
        }) as Callback,
    )]);
    let mut data = Records::default();
    deploy_schema(
        &mut data,
        json!({"writes":[{"collection":"orders","key":"k","value":{"shop":"a","payload":"x".repeat(4096)}}]}),
        schema(false),
        &fixture,
    );
    let error = run_with_limit(
        data.clone(),
        json!({"name":"read"}),
        "query",
        None,
        &fixture,
        2048,
    )
    .unwrap_err();
    assert_eq!(error.code, "EVALUATION_BUDGET");
    let entry = index_entries(&data).pop().unwrap();
    // An entry's ID names its source; one that names none is corrupt.
    data.remove(&entry);
    data.insert(format!("{entry}!z"), json!(0));
    let checking = Fixture::new([("read", (|_, host| query(host, json!("a"))) as Callback)]);
    let error = run(data, json!({"name":"read"}), "query", None, &checking).unwrap_err();
    assert_eq!(error.code, "INPUT_INVALID");
    assert!(error.message.contains("identity"));
}

#[test]
fn wasm_manifest_installs_schema_and_runs_incremental_callback() {
    use crate::evaluator::{evaluate, hash, invoke_at};
    let javascript = r#"var __flowerBundle={default:{
      collections:[{name:'orders',indexes:{shop:['shop']}}],
      definitions:{
        total:{kind:'derived',name:'total',aggregate:{collection:'orders',fields:['shop']},compute:(_,delta)=>{
          let sum=delta.initialize?0:delta.previous;
          for(const change of delta.changes){if('old' in change)sum-=change.old.cents;if('new' in change)sum+=change.new.cents;}
          return sum;
        }},
        change:{kind:'mutationMethod',name:'change',compute:(ctx,args)=>{
          ctx.set({kind:'collection',name:'orders'},'k',{shop:'a',cents:args});
          ctx.materialize({kind:'derived',name:'total'},'a');
          return ctx.get({kind:'derived',name:'total'},'a');
        }}
      },http:{change:{name:'change',kind:'mutation'}}}};"#;
    let deployed = evaluate(BTreeMap::new(),json!({"requestId":"manifest","bundle":{"hash":hash(javascript.as_bytes()),"javascript":javascript}})).unwrap();
    let mut data: Records = deployed.puts.into();
    assert_eq!(
        data["schema"]["aggregates"]["total"]["collection"],
        "orders"
    );
    for value in [3, 7] {
        let result = invoke_at(
            data.clone(),
            json!({"name":"change","args":value,"requestId":format!("change-{value}")}),
            "mutation",
            100,
        )
        .unwrap();
        assert_eq!(result.value, value);
        apply(&mut data, result);
        assert_eq!(total(&data, "a"), value);
        assert_eq!(index_entries(&data).len(), 1);
    }
    let invalid = javascript.replace("indexes:{shop:['shop']}", "indexes:{}");
    let error = evaluate(BTreeMap::new(),json!({"requestId":"bad-manifest","bundle":{"hash":hash(invalid.as_bytes()),"javascript":invalid}})).unwrap_err();
    assert!(format!("{error:#}").contains("Aggregate index must be declared"));
}

#[test]
fn index_schema_metadata_reserves_memory_before_execution() {
    let fixture = Fixture::new([("read", (|_, _| Ok(Value::Null)) as Callback)]);
    let mut data = Records::default();
    let oversized = Schema {
        policies: Default::default(),
        derived_access: Default::default(),
        indexes: vec![IndexSpec {
            collection: "orders".into(),
            fields: vec!["x".repeat(8192)],
        }],
        ..Schema::default()
    };
    data.insert("schema".into(), serde_json::to_value(&oversized).unwrap());
    let error =
        run_with_limit(data, json!({"name":"read"}), "query", None, &fixture, 4096).unwrap_err();
    assert_eq!(error.code, "EVALUATION_BUDGET");
    assert!(error.message.contains("schema"));
    assert!(fixture.calls.borrow().is_empty());
    let error = run_with_limit_and_schema(
        Records::default(),
        json!({"requestId":"large-schema"}),
        "deployment",
        None,
        &fixture,
        4096,
        Some(oversized),
    )
    .unwrap_err();
    assert_eq!(error.code, "EVALUATION_BUDGET");
}
