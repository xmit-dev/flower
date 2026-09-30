use super::*;
use crate::evaluator::rust_engine::indexes::IndexSpec;
use crate::evaluator::{Observation, touched};

fn schema(aggregate: bool) -> Schema {
    let index = IndexSpec {
        collection: "orders".into(),
        fields: vec!["shop".into()],
    };
    Schema {
        policies: Default::default(),
        derived_access: Default::default(),
        aggregate_versions: Default::default(),
        references: Vec::new(),
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
        host: &mut EngineHost<'_>,
    ) -> EngineResult<Value> {
        let mut host = owned(host);
        let host: &mut Host<'_> = &mut host;
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
    assert!(
        index_entries(&data)
            .iter()
            .all(|id| !id.ends_with(":deleted"))
    );
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
        aggregate_versions: Default::default(),
        references: Vec::new(),
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
fn a_query_reading_an_aggregate_depends_on_the_rows_it_counts_not_only_on_membership() {
    let fixture = Reducer::default();
    let mut data = Records::default();
    let writes: Vec<_> = (0..3).map(|key| json!({"collection":"orders","key":key.to_string(),"value":{"shop":"a","cents":10}})).collect();
    // Declared, not materialized: every query computes it from its bucket's rows.
    deploy_schema(&mut data, json!({"writes":writes}), schema(true), &fixture);
    let read = |data: &Records| {
        let result = run(
            data.clone(),
            json!({"name":"read","args":"a"}),
            "query",
            None,
            &fixture,
        )
        .unwrap();
        assert!(result.query_cacheable);
        (result.value, result.query_certificate.unwrap())
    };
    let (value, certificate) = read(&data);
    assert_eq!(value, 30);
    // A row changes inside the bucket: its membership stays, the total does not.
    let result = run(data.clone(), json!({"name":"change","requestId":"inside","args":[{"key":"1","value":{"shop":"a","cents":25}}]}), "mutation", None, &fixture).unwrap();
    let written: Vec<String> = result.puts.keys().cloned().collect();
    apply(&mut data, result);
    assert!(!certificate.valid(&data), "the cached total is stale");
    let observed: Vec<String> = certificate
        .observations()
        .map(|observation| match observation {
            Observation::Key(id) => id.to_owned(),
            Observation::Range { marker, .. } => marker.to_owned(),
        })
        .collect();
    assert!(
        written.iter().any(|key| {
            let mut ids = Vec::new();
            touched(key, |id| ids.push(id.to_owned()));
            ids.iter().any(|id| observed.contains(id))
        }),
        "a watch wakes: {written:?} touch none of {observed:?}"
    );
    let (value, certificate) = read(&data);
    assert_eq!(value, 45);
    // Another bucket's rows leave it alone.
    let result = run(data.clone(), json!({"name":"change","requestId":"elsewhere","args":[{"key":"9","value":{"shop":"b","cents":5}}]}), "mutation", None, &fixture).unwrap();
    apply(&mut data, result);
    assert!(certificate.valid(&data), "an unrelated bucket is unchanged");
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
fn aggregate_values_answer_only_the_callers_their_rule_allows() {
    // An aggregate is a derived value: its access rule, over principal and args
    // (the group), guards methods' reads like any derived value's.
    let fixture = Reducer::default();
    let mut data = Records::default();
    let writes: Vec<_> = [("1", "a", 10), ("2", "a", 20), ("3", "b", 5)]
        .into_iter()
        .map(|(key, shop, cents)| json!({"collection":"orders","key":key,"value":{"shop":shop,"cents":cents}}))
        .collect();
    let mut guarded = schema(true);
    guarded.derived_access = serde_json::from_value(json!({
        "total": {"eq":[{"ref":["args"]},{"ref":["principal","claims","shop"]}]}
    }))
    .unwrap();
    deploy_schema(
        &mut data,
        json!({"writes":writes,"materialize":[{"name":"total","args":"a"},{"name":"total","args":"b"}]}),
        guarded,
        &fixture,
    );
    let read = |group: &str, principal: Option<Value>| {
        let mut invocation = json!({"name":"read","args":group});
        if let Some(principal) = principal {
            invocation["$principal"] = principal;
        }
        run(data.clone(), invocation, "query", None, &fixture)
    };
    let manager = |shop: &str| Some(json!({"subject":"m","claims":{"shop":shop}}));
    assert_eq!(read("a", manager("a")).unwrap().value, 30);
    assert_eq!(read("b", manager("b")).unwrap().value, 5);
    for (group, principal) in [("b", manager("a")), ("a", Some(Value::Null))] {
        let error = read(group, principal).err().expect("denied");
        assert_eq!(error.code, "ACCESS_DENIED");
        assert_eq!(error.message, "Access policy denies reading total");
    }
    // Code without a caller reads every group.
    assert_eq!(read("b", None).unwrap().value, 5);
    // Mutations' reads too, after their own writes.
    let preview = |principal: Value| {
        run(
            data.clone(),
            json!({"name":"change","requestId":"preview","$principal":principal,
                "args":[{"key":"4","value":{"shop":"a","cents":1},"preview":true}]}),
            "mutation",
            None,
            &fixture,
        )
    };
    assert_eq!(
        preview(json!({"subject":"m","claims":{"shop":"a"}}))
            .unwrap()
            .value,
        json!([31])
    );
    let error = preview(json!({"subject":"m","claims":{"shop":"b"}}))
        .err()
        .expect("denied");
    assert_eq!(error.code, "ACCESS_DENIED");
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

fn versioned(version: &str) -> Schema {
    let mut schema = schema(true);
    schema.aggregate_versions = BTreeMap::from([("total".into(), version.into())]);
    schema
}

#[test]
fn a_versioned_aggregate_keeps_its_accumulators_across_deployments_until_its_version_changes() {
    let fixture = Reducer::default();
    let mut data = Records::default();
    let writes: Vec<_> = (0..50).map(|key| json!({"collection":"orders","key":key.to_string(),"value":{"shop":"a","cents":10}})).collect();
    deploy_schema(
        &mut data,
        json!({"writes":writes,"materialize":[{"name":"total","args":"a"}]}),
        versioned("1"),
        &fixture,
    );
    assert_eq!(total(&data, "a"), 500);
    let cell = |data: &Records| data[&cell_id("total", &json!("a"))].clone();
    assert_eq!(
        cell(&data)["reducer"],
        json!({"version":"1","collection":"orders","fields":["shop"]})
    );
    // New code, the same version: the group goes on from its accumulator, reading no row again.
    fixture.payloads.borrow_mut().clear();
    deploy_schema(
        &mut data,
        json!({"bundle":{"hash":"new-code","javascript":"new-code"}}),
        versioned("1"),
        &fixture,
    );
    assert!(
        fixture.payloads.borrow().is_empty(),
        "{:?}",
        fixture.payloads.borrow()
    );
    assert_eq!(total(&data, "a"), 500);
    // And deltas keep maintaining it.
    let result = run(data.clone(), json!({"name":"change","requestId":"delta","args":[{"key":"7","value":{"shop":"a","cents":20}}]}), "mutation", None, &fixture).unwrap();
    apply(&mut data, result);
    assert_eq!(total(&data, "a"), 510);
    assert_eq!(fixture.payloads.borrow()[0]["initialize"], false);
    // A new version rebuilds it from its rows.
    fixture.payloads.borrow_mut().clear();
    deploy_schema(
        &mut data,
        json!({"bundle":{"hash":"newer-code","javascript":"newer-code"}}),
        versioned("2"),
        &fixture,
    );
    assert_eq!(fixture.payloads.borrow()[0]["initialize"], true);
    assert_eq!(
        fixture.payloads.borrow()[0]["changes"]
            .as_array()
            .unwrap()
            .len(),
        50
    );
    assert_eq!(total(&data, "a"), 510);
    assert_eq!(cell(&data)["reducer"]["version"], "2");
    // Without a version, every new bundle rebuilds it, as before, and the cell records none.
    fixture.payloads.borrow_mut().clear();
    deploy_schema(
        &mut data,
        json!({"bundle":{"hash":"plain-code","javascript":"plain-code"}}),
        schema(true),
        &fixture,
    );
    assert_eq!(fixture.payloads.borrow()[0]["initialize"], true);
    assert_eq!(total(&data, "a"), 510);
    assert!(cell(&data).get("reducer").is_none());
}

#[test]
fn a_big_group_goes_to_its_reducer_a_chunk_at_a_time() {
    use crate::evaluator::rust_engine::reducers::{REDUCER_CHUNK_BYTES, REDUCER_CHUNK_ROWS};
    let fixture = Reducer::default();
    let mut data = Records::default();
    let rows = REDUCER_CHUNK_ROWS * 2 + 5;
    let writes: Vec<_> = (0..rows).map(|key| json!({"collection":"orders","key":format!("{key:05}"),"value":{"shop":"a","cents":1}})).collect();
    deploy_schema(
        &mut data,
        json!({"writes":writes,"materialize":[{"name":"total","args":"a"}]}),
        schema(true),
        &fixture,
    );
    assert_eq!(total(&data, "a"), rows as i64);
    let payloads = fixture.payloads.borrow().clone();
    let sizes: Vec<_> = payloads
        .iter()
        .map(|payload| payload["changes"].as_array().unwrap().len())
        .collect();
    assert_eq!(sizes, [REDUCER_CHUNK_ROWS, REDUCER_CHUNK_ROWS, 5]);
    let initializing: Vec<_> = payloads
        .iter()
        .map(|payload| payload["initialize"] == true)
        .collect();
    assert_eq!(initializing, [true, false, false]);
    assert_eq!(payloads[1]["previous"], REDUCER_CHUNK_ROWS);
    assert_eq!(payloads[2]["previous"], REDUCER_CHUNK_ROWS * 2);
    // Big rows make smaller chunks: about REDUCER_CHUNK_BYTES of them at a time.
    fixture.payloads.borrow_mut().clear();
    let pad = "x".repeat(20_000);
    let writes: Vec<_> = (0..200).map(|key| json!({"collection":"orders","key":format!("big{key:03}"),"value":{"shop":"b","cents":2,"pad":pad}})).collect();
    deploy_schema(
        &mut data,
        json!({"writes":writes,"materialize":[{"name":"total","args":"b"}]}),
        schema(true),
        &fixture,
    );
    assert_eq!(total(&data, "b"), 400);
    let chunks: Vec<_> = fixture
        .payloads
        .borrow()
        .iter()
        .filter(|payload| payload["group"] == "b")
        .map(|payload| payload["changes"].as_array().unwrap().len())
        .collect();
    assert!(chunks.len() >= 4, "{chunks:?}");
    assert_eq!(chunks.iter().sum::<usize>(), 200);
    assert!(
        chunks
            .iter()
            .all(|&size| size * 20_000 <= REDUCER_CHUNK_BYTES + 20_000 + 1024),
        "{chunks:?}"
    );
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
        aggregate_versions: Default::default(),
        references: Vec::new(),
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

fn speculate(data: &Records, args: Value, fixture: &Reducer) -> Evaluation {
    run(
        data.clone(),
        json!({"name":"change","args":args,"$speculate":true}),
        "mutation",
        None,
        fixture,
    )
    .unwrap()
}
fn serially(data: &Records, args: Value, fixture: &Reducer) -> Evaluation {
    run(
        data.clone(),
        json!({"name":"change","args":args}),
        "mutation",
        None,
        fixture,
    )
    .unwrap()
}
fn windows(certificate: &MutationCertificate) -> Vec<String> {
    certificate
        .observed()
        .into_iter()
        .filter(|id| id.starts_with("window:"))
        .map(str::to_owned)
        .collect()
}

#[test]
fn an_optimistic_write_to_a_kept_aggregate_stamps_its_cell_not_every_entry_of_its_bucket() {
    let fixture = Reducer::default();
    let mut data = Records::default();
    let mut writes: Vec<_> = (0..200).map(|key| json!({"collection":"orders","key":format!("a{key:03}"),"value":{"shop":"a","cents":10}})).collect();
    writes.extend((0..3).map(
        |key| json!({"collection":"orders","key":format!("b{key}"),"value":{"shop":"b","cents":1}}),
    ));
    deploy_schema(
        &mut data,
        json!({"writes":writes,"materialize":[{"name":"total","args":"a"},{"name":"total","args":"b"}]}),
        schema(true),
        &fixture,
    );
    assert_eq!(total(&data, "a"), 2000);
    // A new row of shop a, read back through the kept total.
    let args = json!([{"key":"new","value":{"shop":"a","cents":5},"preview":true}]);
    let candidate = speculate(&data, args.clone(), &fixture);
    assert_eq!(candidate.value, json!([2005]));
    let certificate = candidate.mutation_certificate.clone().unwrap();
    assert!(
        certificate
            .observed()
            .contains(&cell_id("total", &json!("a")).as_str()),
        "the kept accumulator is stamped"
    );
    assert_eq!(
        windows(&certificate),
        Vec::<String>::new(),
        "going on from the accumulator walks no bucket"
    );
    let reuse = |data: &Records| {
        let valid = certificate.valid(data);
        if valid {
            // Reusing it must be what running it now would do.
            let expected = serially(data, args.clone(), &fixture);
            assert_eq!(candidate.value, expected.value);
            assert_eq!(candidate.puts, expected.puts);
            assert_eq!(candidate.deletes, expected.deletes);
        }
        valid
    };
    // Another shop's write leaves shop a's total alone.
    let mut other = data.clone();
    apply(
        &mut other,
        serially(
            &data,
            json!([{"key":"b9","value":{"shop":"b","cents":4}}]),
            &fixture,
        ),
    );
    assert!(reuse(&other));
    // A write that changes shop a's total conflicts.
    let mut inside = data.clone();
    apply(
        &mut inside,
        serially(
            &data,
            json!([{"key":"a500","value":{"shop":"a","cents":7}}]),
            &fixture,
        ),
    );
    assert_eq!(total(&inside, "a"), 2007);
    assert!(!reuse(&inside));
    let mut removed = data.clone();
    apply(
        &mut removed,
        serially(&data, json!([{"key":"a001"}]), &fixture),
    );
    assert!(!reuse(&removed));
    // A write within shop a that leaves its total as it was, a row leaving as another of the same
    // amount comes in, does not: the result is the same either way.
    let mut same = data.clone();
    apply(
        &mut same,
        serially(
            &data,
            json!([{"key":"a002"},{"key":"a900","value":{"shop":"a","cents":10}}]),
            &fixture,
        ),
    );
    assert_eq!(total(&same, "a"), 2000);
    assert!(reuse(&same));
    // The rows it writes are still stamped.
    let mut written = data.clone();
    apply(
        &mut written,
        serially(
            &data,
            json!([{"key":"new","value":{"shop":"b","cents":1}}]),
            &fixture,
        ),
    );
    assert!(!reuse(&written));
}

#[test]
fn an_optimistic_write_building_an_aggregate_from_its_rows_stamps_its_bucket() {
    let fixture = Reducer::default();
    let mut data = Records::default();
    let mut writes: Vec<_> = (0..20).map(|key| json!({"collection":"orders","key":format!("a{key:02}"),"value":{"shop":"a","cents":10}})).collect();
    writes.push(json!({"collection":"orders","key":"b0","value":{"shop":"b","cents":1}}));
    // Declared, not kept: reading it builds shop a's group from its rows.
    deploy_schema(&mut data, json!({"writes":writes}), schema(true), &fixture);
    let args = json!([{"key":"new","value":{"shop":"a","cents":5},"preview":true}]);
    let candidate = speculate(&data, args.clone(), &fixture);
    assert_eq!(candidate.value, json!([205]));
    let certificate = candidate.mutation_certificate.clone().unwrap();
    assert_eq!(
        windows(&certificate).len(),
        1,
        "{:?}",
        certificate.observed()
    );
    // A row joining shop a without the aggregate kept changes no cell: only the bucket's entries
    // show it.
    let mut inside = data.clone();
    apply(
        &mut inside,
        serially(
            &data,
            json!([{"key":"a99","value":{"shop":"a","cents":1}}]),
            &fixture,
        ),
    );
    assert!(!certificate.valid(&inside));
    let mut other = data.clone();
    apply(
        &mut other,
        serially(
            &data,
            json!([{"key":"b1","value":{"shop":"b","cents":1}}]),
            &fixture,
        ),
    );
    assert!(
        certificate.valid(&other),
        "the window fits: another shop's row is outside it"
    );
    let expected = serially(&other, args, &fixture);
    assert_eq!(candidate.value, expected.value);
    assert_eq!(candidate.puts, expected.puts);
}

#[test]
fn optimistic_writes_to_kept_aggregates_match_serial() {
    const SHOPS: [&str; 3] = ["a", "b", "c"];
    let fixture = Reducer::default();
    let mut initial = Records::default();
    let writes: Vec<_> = (0..40).map(|key| json!({"collection":"orders","key":format!("k{key}"),"value":{"shop":(SHOPS[key % 3]),"cents":key % 4}})).collect();
    deploy_schema(
        &mut initial,
        json!({"writes":writes,"materialize":[{"name":"total","args":"a"},{"name":"total","args":"b"}]}),
        schema(true),
        &fixture,
    );
    let mut actual = initial.clone();
    let mut serial = initial;
    let (mut accepted, mut conflicts) = (0, 0);
    let mut random = 0x9e3779b97f4a7c15u64;
    let mut next = |bound: u64| {
        random = random
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (random >> 33) % bound
    };
    for _ in 0..40 {
        let mut wave = Vec::new();
        for _ in 0..6 {
            let rows: Vec<_> = (0..1 + next(2))
                .map(|_| {
                    let key = format!("k{}", next(48));
                    match next(5) {
                        0 => json!({"key":key}),
                        // Amounts repeat, so some writes leave a total as it was.
                        _ => json!({"key":key,"value":{"shop":(SHOPS[next(3) as usize]),"cents":next(3)},"preview":next(2) == 0}),
                    }
                })
                .collect();
            let args = Value::Array(rows);
            wave.push((args.clone(), speculate(&actual, args, &fixture)));
        }
        for (args, candidate) in wave {
            let expected = serially(&serial, args.clone(), &fixture);
            let selected = if candidate
                .mutation_certificate
                .as_ref()
                .unwrap()
                .valid(&actual)
            {
                accepted += 1;
                candidate
            } else {
                conflicts += 1;
                speculate(&actual, args, &fixture)
            };
            assert_eq!(selected.value, expected.value);
            assert_eq!(selected.puts, expected.puts);
            assert_eq!(selected.deletes, expected.deletes);
            apply(&mut actual, selected);
            apply(&mut serial, expected);
            assert_eq!(actual, serial);
        }
    }
    assert!(
        accepted > 40 && conflicts > 40,
        "must exercise both reuse and conflicts: {accepted} accepted, {conflicts} conflicts"
    );
}

/// What an optimistic write into a kept aggregate over a big group costs: an evaluation's thread
/// CPU and its certificate's window stamps, with the records served from a stored backing as on a
/// server (FLOWER_TEST_BACKED). Not run by default:
///
/// ```sh
/// FLOWER_AGGREGATE_BENCH_ROWS=66000 cargo test --release --lib kept_aggregate_speculation_costs -- --ignored --nocapture
/// ```
#[test]
#[ignore]
fn kept_aggregate_speculation_costs() {
    fn thread_cpu() -> f64 {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: clock_gettime writes the timespec it is given.
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) };
        time.tv_sec as f64 + time.tv_nsec as f64 * 1e-9
    }
    let setting = |name: &str, default: usize| {
        std::env::var(name).map_or(default, |value| value.parse().unwrap())
    };
    let rows = setting("FLOWER_AGGREGATE_BENCH_ROWS", 66_000);
    let calls = setting("FLOWER_AGGREGATE_BENCH_CALLS", 200);
    let fixture = Reducer::default();
    let mut data = Records::default();
    for key in 0..rows {
        data.insert(
            source_id("orders", &format!("{key:07}")),
            json!({"shop":"a","cents":1}),
        );
    }
    deploy_schema(
        &mut data,
        json!({"materialize":[{"name":"total","args":"a"}]}),
        schema(true),
        &fixture,
    );
    assert_eq!(total(&data, "a"), rows as i64);
    let data = data.backed_copy();
    let write =
        |index: usize| json!([{"key":format!("new{index}"),"value":{"shop":"a","cents":1}}]);
    let windows = speculate(&data, write(0), &fixture)
        .mutation_certificate
        .map_or(0, |certificate| windows(&certificate).len());
    for (label, speculative) in [("optimistic", true), ("serial", false)] {
        let evaluate = |index: usize| {
            if speculative {
                speculate(&data, write(index), &fixture)
            } else {
                serially(&data, write(index), &fixture)
            }
        };
        for index in 0..calls / 10 {
            evaluate(index);
        }
        let (cpu, wall) = (thread_cpu(), std::time::Instant::now());
        for index in 0..calls {
            evaluate(index);
        }
        let cpu = (thread_cpu() - cpu) * 1e3 / calls as f64;
        let wall = wall.elapsed().as_secs_f64() * 1e3 / calls as f64;
        println!(
            "{label} write into a kept group of {rows} rows: {cpu:.3} ms CPU, {wall:.3} ms wall per evaluation over {calls} ({windows} window stamps)"
        );
    }
}
