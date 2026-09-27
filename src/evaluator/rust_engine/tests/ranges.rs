use super::*;
use crate::evaluator::rust_engine::indexes::IndexSpec;

fn spec() -> IndexSpec {
    IndexSpec {
        collection: "items".into(),
        fields: vec!["tenant".into(), "score".into()],
    }
}
fn fixture() -> Fixture {
    Fixture::new([(
        "read",
        (|args, host| host("range", json!([args]))) as Callback,
    )])
}
fn reference(options: Value) -> Value {
    json!({"kind":"range","collection":"items","fields":["tenant","score"],"options":options})
}
fn install(data: &mut Records, fixture: &Fixture) {
    let result = run_with_schema(
        data.clone(),
        json!({"requestId":"schema"}),
        "deployment",
        None,
        fixture,
        Some(Schema {
            indexes: vec![spec()],
            aggregates: BTreeMap::new(),
        }),
    )
    .unwrap();
    apply(data, result);
}
fn read(data: &Records, options: Value) -> Value {
    run(
        data.clone(),
        json!({"name":"read","args":reference(options)}),
        "query",
        None,
        &fixture(),
    )
    .unwrap()
    .value
}
fn keys(value: &Value) -> Vec<&str> {
    value["rows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["key"].as_str().unwrap())
        .collect()
}

#[test]
fn ordered_ranges_have_total_scalar_order_utf16_ties_and_restart_stability() {
    let mut data = Records::default();
    for (key, score) in [
        ("null", Value::Null),
        ("false", json!(false)),
        ("true", json!(true)),
        ("negative", json!(-100)),
        ("zero", json!(-0.0)),
        ("fraction", json!(0.125)),
        ("😀", json!(2)),
        ("\u{e000}", json!(2)),
        ("short", json!("a")),
        ("long", json!("aa")),
        ("object", json!({})),
        ("array", json!([])),
    ] {
        data.insert(source_id("items", key), json!({"tenant":"a","score":score}));
    }
    install(&mut data, &fixture());
    assert_eq!(
        keys(&read(&data, json!({"prefix":["a"],"limit":99}))),
        vec![
            "null", "false", "true", "negative", "zero", "fraction", "😀", "\u{e000}", "short",
            "long"
        ]
    );
    let restored: Records = serde_json::from_slice(&serde_json::to_vec(&data).unwrap()).unwrap();
    assert_eq!(
        read(&data, json!({"prefix":["a"],"limit":99})),
        read(&restored, json!({"prefix":["a"],"limit":99}))
    );
    assert_eq!(
        keys(&read(
            &data,
            json!({"prefix":["a"],"gt":0,"lte":2,"limit":99,"reverse":true})
        )),
        vec!["\u{e000}", "😀", "fraction"]
    );
}

#[test]
fn ordered_ranges_page_compound_bounds_and_reject_mismatched_cursors() {
    let mut data = Records::default();
    for n in 0..12 {
        data.insert(
            source_id("items", &format!("key{n:02}")),
            json!({"tenant":if n==11 {"b"} else {"a"},"score":n/2}),
        );
    }
    install(&mut data, &fixture());
    for reverse in [false, true] {
        let options = json!({"prefix":["a"],"gte":1,"lt":4,"limit":2,"reverse":reverse});
        let mut all = vec![];
        let mut next = options.clone();
        loop {
            let page = read(&data, next.clone());
            all.extend(keys(&page).into_iter().map(str::to_owned));
            if page["cursor"].is_null() {
                break;
            }
            next["after"] = page["cursor"].clone();
        }
        let mut expected = (2..8).map(|n| format!("key{n:02}")).collect::<Vec<_>>();
        if reverse {
            expected.reverse();
        }
        assert_eq!(all, expected);
    }
    let page = read(&data, json!({"prefix":["a"],"limit":1}));
    let error=run(data.clone(),json!({"name":"read","args":reference(json!({"prefix":["b"],"limit":1,"after":page["cursor"]}))}),"query",None,&fixture()).unwrap_err();
    assert_eq!(error.code, "INVALID_REFERENCE");
    assert!(keys(&read(
        &data,
        json!({"prefix":["a"],"gte":7,"lt":2,"limit":1})
    ))
    .is_empty());
    assert_eq!(
        keys(&read(&data, json!({"prefix":["a",2],"limit":99}))),
        vec!["key04", "key05"]
    );
    for options in [
        json!({"limit":0}),
        json!({"limit":1,"gt":1,"gte":0}),
        json!({"limit":1,"prefix":["a",1],"lt":2}),
        json!({"limit":1,"gte":{}}),
        json!({"limit":1,"after":"no"}),
    ] {
        assert_eq!(
            run(
                data.clone(),
                json!({"name":"read","args":reference(options)}),
                "query",
                None,
                &fixture()
            )
            .unwrap_err()
            .code,
            "INVALID_REFERENCE"
        );
    }
}

#[test]
fn ordered_ranges_overlay_pending_writes_and_native_fallback_agree() {
    let fixture = Fixture::new([(
        "work",
        (|args, host| {
            set(host, "items", "one", json!({"tenant":"b","score":1}))?;
            set(host, "items", "insert", json!({"tenant":"a","score":0}))?;
            host(
                "delete",
                json!([{"kind":"collection","name":"items"},"two"]),
            )?;
            host("range", json!([args]))
        }) as Callback,
    )]);
    let mut data = Records::default();
    for n in ["one", "two", "three"] {
        data.insert(source_id("items", n), json!({"tenant":"a","score":1}));
    }
    let command = json!({"name":"work","requestId":"overlay","args":reference(json!({"prefix":["a"],"limit":1}))});
    let fallback = run(data.clone(), command.clone(), "mutation", None, &fixture).unwrap();
    install(&mut data, &fixture);
    let indexed = run(data.clone(), command, "mutation", None, &fixture).unwrap();
    assert_eq!(indexed.value, fallback.value);
    assert_eq!(keys(&indexed.value), vec!["insert"]);
    assert!(indexed.value["cursor"].is_string());
    apply(&mut data, indexed);
    assert_eq!(
        keys(&read(&data, json!({"prefix":["a"],"limit":9}))),
        vec!["insert", "three"]
    );
}

#[test]
fn ordered_ranges_track_empty_phantoms_and_value_only_changes() {
    let fixture = Fixture::new([(
        "matches",
        (|_, host| {
            host(
                "range",
                json!([reference(json!({"prefix":["a"],"lte":3,"limit":1}))]),
            )
        }) as Callback,
    )]);
    let mut data = Records::default();
    install(&mut data, &fixture);
    deploy(
        &mut data,
        json!({"materialize":[{"name":"matches"}]}),
        &fixture,
    );
    let id = cell_id("matches", &Value::Null);
    assert!(data[&id]["outcome"]["value"]["rows"]
        .as_array()
        .unwrap()
        .is_empty());
    let result = deploy(
        &mut data,
        json!({"writes":[{"collection":"unrelated","key":"x","value":1}]}),
        &fixture,
    );
    assert!(result.evaluated.is_empty());
    let result = deploy(
        &mut data,
        json!({"writes":[{"collection":"items","key":"x","value":{"tenant":"a","score":2,"label":"old"}}]}),
        &fixture,
    );
    assert_eq!(result.evaluated, vec![id.clone()]);
    let result = deploy(
        &mut data,
        json!({"writes":[{"collection":"items","key":"x","value":{"tenant":"a","score":2,"label":"new"}}]}),
        &fixture,
    );
    assert_eq!(result.evaluated, vec![id.clone()]);
    assert_eq!(
        data[&id]["outcome"]["value"]["rows"][0]["value"]["label"],
        "new"
    );
    deploy(
        &mut data,
        json!({"writes":[{"collection":"items","key":"x","delete":true}]}),
        &fixture,
    );
    assert!(data[&id]["outcome"]["value"]["rows"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn ordered_ranges_seek_only_selected_slice_and_fail_closed_on_corruption() {
    let mut data = Records::default();
    data.insert(source_id("items", "one"), json!({"tenant":"a","score":1}));
    install(&mut data, &fixture());
    // A corrupt disjoint entry is intentionally never read by this selective seek.
    let disjoint =
        super::super::ranges::entry(&spec(), "bad", &json!({"tenant":"z","score":1})).unwrap();
    data.insert(disjoint, json!({"invalid":true}));
    assert_eq!(
        keys(&read(&data, json!({"prefix":["a"],"limit":1}))),
        vec!["one"]
    );
    let error = run(
        data,
        json!({"name":"read","args":reference(json!({"prefix":["z"],"limit":1}))}),
        "query",
        None,
        &fixture(),
    )
    .unwrap_err();
    assert_eq!(error.code, "INPUT_INVALID");
}

#[test]
fn ordered_ranges_bound_retained_results_and_poison_caught_budget_failures() {
    let mut data = Records::default();
    for n in 0..200 {
        data.insert(
            source_id("items", &format!("key{n:04}")),
            json!({"tenant":"a","score":n,"payload":"x".repeat(512)}),
        );
    }
    install(&mut data, &fixture());
    let small = run_with_limit(
        data.clone(),
        json!({"name":"read","args":reference(json!({"prefix":["a"],"limit":1}))}),
        "query",
        None,
        &fixture(),
        8192,
    )
    .unwrap();
    assert_eq!(keys(&small.value), vec!["key0000"]);
    let catching = Fixture::new([(
        "catch",
        (|args, host| {
            let _ = host("range", json!([args]));
            Ok(Value::Null)
        }) as Callback,
    )]);
    let error = run_with_limit(
        data,
        json!({"name":"catch","args":reference(json!({"prefix":["a"],"limit":200}))}),
        "query",
        None,
        &catching,
        8192,
    )
    .unwrap_err();
    assert_eq!(error.code, "EVALUATION_BUDGET");
}

#[test]
fn wasm_range_context_serves_mutations_queries_and_derived_phantoms() {
    use crate::evaluator::{evaluate, hash, invoke_at};
    let javascript = r#"var __flowerBundle={default:{collections:[{name:'items',indexes:{byTenantScore:['tenant','score']}}],definitions:{
      earliest:{kind:'derived',name:'earliest',compute:(ctx)=>ctx.range({kind:'range',collection:'items',fields:['tenant','score'],options:{prefix:['a'],lte:5,limit:1}})},
      save:{kind:'mutationMethod',name:'save',compute:(ctx,args)=>{ctx.set({kind:'collection',name:'items'},args.id,{tenant:'a',score:args.score});ctx.materialize({kind:'derived',name:'earliest'},null);return ctx.get({kind:'derived',name:'earliest'},null);}},
      read:{kind:'queryMethod',name:'read',compute:(ctx)=>ctx.range({kind:'range',collection:'items',fields:['tenant','score'],options:{prefix:['a'],limit:1}})}
    },http:{save:{name:'save',kind:'mutation'},read:{name:'read',kind:'query'}}}};"#;
    let deployed=evaluate(BTreeMap::new(),json!({"requestId":"manifest","bundle":{"hash":hash(javascript.as_bytes()),"javascript":javascript}})).unwrap();
    let mut data: Records = deployed.puts.into();
    for (id, score) in [("late", 10), ("due", 3)] {
        let result = invoke_at(
            data.clone(),
            json!({"name":"save","args":{"id":id,"score":score},"requestId":id}),
            "mutation",
            100,
        )
        .unwrap();
        apply(&mut data, result);
    }
    assert_eq!(
        keys(&data[&cell_id("earliest", &Value::Null)]["outcome"]["value"]),
        vec!["due"]
    );
    let result = invoke_at(data, json!({"name":"read"}), "query", 100).unwrap();
    assert_eq!(keys(&result.value), vec!["due"]);
    assert!(result.value["cursor"].is_string());
}
