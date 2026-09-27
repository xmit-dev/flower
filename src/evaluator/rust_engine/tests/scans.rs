use super::*;
use crate::evaluator::rust_engine::indexes::IndexSpec;
use std::collections::BTreeSet;

fn collection() -> Value {
    json!({"kind":"collection","name":"items","indexes":{"rank":["tenant","score"]}})
}

fn fixture() -> Fixture {
    Fixture::new([(
        "read",
        (|args, host| host("scan", json!([collection(), args]))) as Callback,
    )])
}

fn install(data: &mut Records, fixture: &Fixture) {
    let result = run_with_schema(
        data.clone(),
        json!({"requestId":"schema"}),
        "deployment",
        None,
        fixture,
        Some(Schema {
            indexes: vec![IndexSpec {
                collection: "items".into(),
                fields: vec!["tenant".into(), "score".into()],
            }],
            aggregates: BTreeMap::new(),
        }),
    )
    .unwrap();
    apply(data, result);
}

fn read(data: &Records, options: Value) -> Value {
    run(
        data.clone(),
        json!({"name":"read","args":options}),
        "query",
        None,
        &fixture(),
    )
    .unwrap()
    .value
}

fn keys(value: &Value) -> Vec<&str> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|row| row["key"].as_str().unwrap())
        .collect()
}

#[test]
fn scans_filter_source_keys_before_reverse_offset_and_limit() {
    let data = Records::from([
        (source_id("items", "a"), Value::Null),
        (source_id("items", "b"), json!(false)),
        (source_id("items", "c"), json!([1, 2])),
        (source_id("items", "😀"), json!({"score":1})),
        (source_id("items", "\u{e000}"), json!(42)),
        (source_id("other", "b"), json!("excluded")),
    ]);
    assert_eq!(
        keys(&read(&data, json!({}))),
        ["a", "b", "c", "😀", "\u{e000}"]
    );
    assert_eq!(
        keys(&read(
            &data,
            json!({"gt":"a","lte":"😀","reverse":true,"offset":1,"limit":2})
        )),
        ["c", "b"]
    );
    assert_eq!(
        keys(&read(&data, json!({"gte":"😀","lt":"\u{e000}"}))),
        ["😀"]
    );
    assert_eq!(
        read(&data, json!({"prefix":["b"]})),
        json!([{"key":"b","value":false}])
    );
    assert!(keys(&read(&data, json!({"prefix":["missing"]}))).is_empty());
    assert!(keys(&read(&data, json!({"gt":"c","lt":"b"}))).is_empty());
    assert!(keys(&read(&data, json!({"limit":0}))).is_empty());
    assert!(keys(&read(&data, json!({"offset":5}))).is_empty());
    assert!(keys(&read(&data, json!({"offset":9_007_199_254_740_991_u64}))).is_empty());
    assert_eq!(
        read(&data, json!({"offset":0,"limit":9_007_199_254_740_991_u64})),
        read(&data, json!({}))
    );
}

#[test]
fn indexed_scans_order_scalars_and_utf16_ties_with_or_without_declared_indexes() {
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
    data.insert(source_id("items", "missing"), json!({"tenant":"a"}));
    data.insert(source_id("items", "primitive"), json!(12));
    data.insert(
        source_id("items", "other-tenant"),
        json!({"tenant":"b","score":1}),
    );
    let options = json!({"index":"rank","prefix":["a"]});
    let expected = [
        "null", "false", "true", "negative", "zero", "fraction", "😀", "\u{e000}", "short", "long",
    ];
    let fallback = read(&data, options.clone());
    assert_eq!(keys(&fallback), expected);
    install(&mut data, &fixture());
    assert_eq!(read(&data, options.clone()), fallback);
    let restored: Records = serde_json::from_slice(&serde_json::to_vec(&data).unwrap()).unwrap();
    assert_eq!(read(&restored, options), fallback);
    assert_eq!(
        keys(&read(
            &data,
            json!({"index":"rank","prefix":["a"],"gt":0,"lte":2,"reverse":true,"offset":1,"limit":1})
        )),
        ["😀"]
    );
}

#[test]
fn indexed_scans_apply_compound_prefixes_and_each_bound() {
    let mut data = Records::default();
    for n in 0..10 {
        data.insert(
            source_id("items", &format!("key{n}")),
            json!({"tenant":if n == 9 {"b"} else {"a"},"score":n/2}),
        );
    }
    for declared in [false, true] {
        if declared {
            install(&mut data, &fixture());
        }
        for (options, expected) in [
            (json!({"gt":1,"lt":3}), vec!["key4", "key5"]),
            (
                json!({"gte":1,"lte":3}),
                vec!["key2", "key3", "key4", "key5", "key6", "key7"],
            ),
            (json!({"gte":4,"lte":4}), vec!["key8"]),
            (json!({"gt":4,"lte":4}), vec![]),
            (json!({"gte":7,"lt":2}), vec![]),
        ] {
            let mut options = options;
            options["index"] = json!("rank");
            options["prefix"] = json!(["a"]);
            assert_eq!(keys(&read(&data, options)), expected, "declared={declared}");
        }
        assert_eq!(
            keys(&read(
                &data,
                json!({"index":"rank","prefix":["a",2],"reverse":true})
            )),
            ["key5", "key4"]
        );
        assert_eq!(
            keys(&read(&data, json!({"index":"rank","gte":"b"}))),
            ["key9"]
        );
        assert!(keys(&read(&data, json!({"index":"rank","limit":0}))).is_empty());
    }
}

#[test]
fn scans_reject_invalid_options_and_index_fields() {
    let fixture = Fixture::new([(
        "read",
        (|args, host| host("scan", args.clone())) as Callback,
    )]);
    for options in [
        Value::Null,
        json!([]),
        json!({"unknown":true}),
        json!({"index":"missing"}),
        json!({"index":""}),
        json!({"index":1}),
        json!({"index":null}),
        json!({"limit":-1}),
        json!({"limit":1.5}),
        json!({"limit":"1"}),
        json!({"limit":null}),
        json!({"limit":9_007_199_254_740_992_u64}),
        json!({"offset":-1}),
        json!({"offset":0.5}),
        json!({"offset":null}),
        json!({"offset":9_007_199_254_740_992_u64}),
        json!({"reverse":1}),
        json!({"prefix":"a"}),
        json!({"prefix":[1]}),
        json!({"prefix":["a","b"]}),
        json!({"prefix":["a"],"gte":"a"}),
        json!({"gte":1}),
        json!({"gt":"a","gte":"b"}),
        json!({"lt":"a","lte":"b"}),
        json!({"index":"rank","prefix":["a",1],"lt":2}),
        json!({"index":"rank","prefix":["a",1,2]}),
        json!({"index":"rank","prefix":[{}]}),
        json!({"index":"rank","gte":[]}),
    ] {
        let error = run(
            Records::default(),
            json!({"name":"read","args":[collection(),options]}),
            "query",
            None,
            &fixture,
        )
        .unwrap_err();
        assert_eq!(error.code, "INVALID_REFERENCE", "{options}");
    }
    for fields in [
        json!([]),
        json!("score"),
        json!([""]),
        json!([1]),
        json!(["score", "score"]),
    ] {
        let reference = json!({"kind":"collection","name":"items","indexes":{"rank":fields}});
        let error = run(
            Records::default(),
            json!({"name":"read","args":[reference,{"index":"rank"}]}),
            "query",
            None,
            &fixture,
        )
        .unwrap_err();
        assert_eq!(error.code, "INVALID_REFERENCE", "{fields}");
    }
}

#[test]
fn indexed_scans_overlay_inserts_deletes_and_moves_before_pagination() {
    let fixture = Fixture::new([(
        "work",
        (|_, host| {
            set(host, "items", "one", json!({"tenant":"a","score":5}))?;
            set(host, "items", "four", json!({"tenant":"a","score":1}))?;
            set(host, "items", "insert", json!({"tenant":"a","score":0}))?;
            host("delete", json!([collection(), "two"]))?;
            let all = host(
                "scan",
                json!([collection(),{"index":"rank","prefix":["a"]}]),
            )?;
            let page = host(
                "scan",
                json!([collection(),{"index":"rank","prefix":["a"],"reverse":true,"offset":1,"limit":2}]),
            )?;
            Ok(json!({"all":all,"page":page}))
        }) as Callback,
    )]);
    let mut data = Records::default();
    for (key, score) in [("one", 1), ("two", 2), ("three", 3), ("four", 4)] {
        data.insert(source_id("items", key), json!({"tenant":"a","score":score}));
    }
    let command = json!({"name":"work","requestId":"overlay"});
    let fallback = run(data.clone(), command.clone(), "mutation", None, &fixture).unwrap();
    install(&mut data, &fixture);
    let indexed = run(data.clone(), command, "mutation", None, &fixture).unwrap();
    assert_eq!(indexed.value, fallback.value);
    assert_eq!(
        keys(&indexed.value["all"]),
        ["insert", "four", "three", "one"]
    );
    assert_eq!(keys(&indexed.value["page"]), ["three", "four"]);
    apply(&mut data, indexed);
    assert_eq!(
        keys(&read(&data, json!({"index":"rank","prefix":["a"]}))),
        ["insert", "four", "three", "one"]
    );
}

#[test]
fn constrained_scans_invalidate_empty_results_and_paginated_derived_values() {
    let fixture = Fixture::new([
        (
            "read",
            (|args, host| host("scan", json!([collection(), args]))) as Callback,
        ),
        (
            "matches",
            (|_, host| {
                host(
                    "scan",
                    json!([collection(),{"index":"rank","prefix":["a"],"lte":5,"offset":1,"limit":1}]),
                )
            }) as Callback,
        ),
    ]);
    for declared in [false, true] {
        let mut data = Records::default();
        if declared {
            install(&mut data, &fixture);
        }
        deploy(
            &mut data,
            json!({"materialize":[{"name":"matches"}]}),
            &fixture,
        );
        let id = cell_id("matches", &Value::Null);
        assert!(keys(&data[&id]["outcome"]["value"]).is_empty());
        let result = run(
            data.clone(),
            json!({"name":"read","args":{"index":"rank","prefix":["a"],"lte":5,"offset":1,"limit":1}}),
            "query",
            None,
            &fixture,
        ).unwrap();
        let certificate = result.query_certificate.unwrap();
        let unrelated = deploy(
            &mut data,
            json!({"writes":[{"collection":"other","key":"x","value":1}]}),
            &fixture,
        );
        assert!(unrelated.evaluated.is_empty());
        assert!(certificate.valid(&data));
        let inserted = deploy(
            &mut data,
            json!({"writes":[
                {"collection":"items","key":"first","value":{"tenant":"a","score":1}},
                {"collection":"items","key":"second","value":{"tenant":"a","score":2,"label":"old"}}
            ]}),
            &fixture,
        );
        assert_eq!(inserted.evaluated, [id.clone()]);
        assert!(!certificate.valid(&data));
        assert_eq!(keys(&data[&id]["outcome"]["value"]), ["second"]);
        let updated = deploy(
            &mut data,
            json!({"writes":[{"collection":"items","key":"second","value":{"tenant":"a","score":2,"label":"new"}}]}),
            &fixture,
        );
        assert_eq!(updated.evaluated, [id.clone()]);
        assert_eq!(data[&id]["outcome"]["value"][0]["value"]["label"], "new");
        deploy(
            &mut data,
            json!({"writes":[{"collection":"items","key":"earlier","value":{"tenant":"a","score":0}}]}),
            &fixture,
        );
        assert_eq!(keys(&data[&id]["outcome"]["value"]), ["first"]);
        deploy(
            &mut data,
            json!({"writes":[{"collection":"items","key":"first","delete":true}]}),
            &fixture,
        );
        assert_eq!(keys(&data[&id]["outcome"]["value"]), ["second"]);
    }
}

#[test]
fn limited_scans_bound_retained_results_and_cannot_hide_budget_failures() {
    let mut data = Records::default();
    for n in 0..200 {
        data.insert(
            source_id("items", &format!("key{n:04}")),
            json!({"tenant":"a","score":n,"payload":"x".repeat(512)}),
        );
    }
    for declared in [false, true] {
        if declared {
            install(&mut data, &fixture());
        }
        let result = run_with_limit(
            data.clone(),
            json!({"name":"read","args":{"index":"rank","prefix":["a"],"offset":3,"limit":1}}),
            "query",
            None,
            &fixture(),
            8192,
        )
        .unwrap();
        assert_eq!(keys(&result.value), ["key0003"]);
        let catching = Fixture::new([(
            "catch",
            (|args, host| {
                let _ = host("scan", json!([collection(), args]));
                Ok(Value::Null)
            }) as Callback,
        )]);
        let error = run_with_limit(
            data.clone(),
            json!({"name":"catch","args":{"index":"rank","prefix":["a"],"limit":200}}),
            "query",
            None,
            &catching,
            8192,
        )
        .unwrap_err();
        assert_eq!(error.code, "EVALUATION_BUDGET");
    }
}

#[test]
fn declared_scan_offsets_do_not_retain_skipped_rows_with_pending_writes() {
    let fixture = Fixture::new([(
        "work",
        (|args, host| {
            set(
                host,
                "items",
                "key0000",
                json!({"tenant":"a","score":170.5,"payload":"m".repeat(512)}),
            )?;
            set(
                host,
                "items",
                "key0200",
                json!({"tenant":"a","score":50.5,"payload":"i".repeat(512)}),
            )?;
            host("delete", json!([collection(), "key0100"]))?;
            host(
                "scan",
                json!([collection(),{"index":"rank","prefix":["a"],"offset":150,"limit":2,"reverse":args["reverse"]}]),
            )
        }) as Callback,
    )]);
    let mut data = Records::default();
    for n in 0..200 {
        data.insert(
            source_id("items", &format!("key{n:04}")),
            json!({"tenant":"a","score":n,"payload":"x".repeat(512)}),
        );
    }
    install(&mut data, &fixture);
    for (reverse, expected) in [
        (false, ["key0151", "key0152"]),
        (true, ["key0050", "key0049"]),
    ] {
        let result = run_with_limit(
            data.clone(),
            json!({"name":"work","requestId":"skip","args":{"reverse":reverse}}),
            "mutation",
            None,
            &fixture,
            16 * 1024,
        )
        .unwrap();
        assert_eq!(keys(&result.value), expected);
    }
}

fn page(options: Value) -> Value {
    json!({"kind":"range","collection":"items","fields":["tenant","score"],"options":options})
}

fn windowed_fixture() -> Fixture {
    Fixture::new([
        (
            "read",
            (|args, host| host("scan", json!([collection(), args]))) as Callback,
        ),
        (
            "page",
            (|args, host| host("range", json!([page(args.clone())]))) as Callback,
        ),
    ])
}

fn write(key: &str, value: Value) -> Value {
    json!({"collection":"items","key":key,"value":value})
}

fn delete(key: &str) -> Value {
    json!({"collection":"items","key":key,"delete":true})
}

fn evaluated(data: &mut Records, writes: Value, fixture: &Fixture) -> BTreeSet<String> {
    // A reloaded state rebuilds its scan windows from reader records; it must
    // rerun exactly the cells the incrementally maintained one does.
    let mut reloaded: Records = serde_json::from_str(&serde_json::to_string(data).unwrap()).unwrap();
    let expected: BTreeSet<String> = deploy(&mut reloaded, json!({ "writes": writes }), fixture)
        .evaluated
        .into_iter()
        .collect();
    let actual: BTreeSet<String> = deploy(data, json!({ "writes": writes }), fixture)
        .evaluated
        .into_iter()
        .collect();
    assert_eq!(actual, expected, "rebuilt scan windows");
    assert_eq!(*data, reloaded);
    actual
}

fn cells<const N: usize>(ids: [&String; N]) -> BTreeSet<String> {
    ids.into_iter().cloned().collect()
}

#[test]
fn derived_scans_rerun_only_when_a_write_can_change_their_window() {
    let fixture = windowed_fixture();
    for declared in [false, true] {
        let mut data = Records::default();
        for n in 1..=6 {
            data.insert(
                source_id("items", &format!("s{n}")),
                json!({"tenant":"a","score":n}),
            );
        }
        data.insert(source_id("items", "b1"), json!({"tenant":"b","score":1}));
        if declared {
            install(&mut data, &fixture);
        }
        let top = json!({"index":"rank","prefix":["a"],"lte":5,"limit":2});
        let skipped = json!({"index":"rank","prefix":["a"],"reverse":true,"offset":1,"limit":1});
        let keyed = json!({"gt":"s2","lte":"s5","limit":2});
        let first = json!({"prefix":["a"],"limit":1});
        deploy(
            &mut data,
            json!({"materialize":[
                {"name":"read","args":top},
                {"name":"read","args":skipped},
                {"name":"read","args":keyed},
                {"name":"page","args":first}
            ]}),
            &fixture,
        );
        let top = cell_id("read", &top);
        let skipped = cell_id("read", &skipped);
        let keyed = cell_id("read", &keyed);
        let first = cell_id("page", &first);
        let value = |data: &Records, id: &str| data[id]["outcome"]["value"].clone();
        assert_eq!(keys(&value(&data, &top)), ["s1", "s2"]);
        assert_eq!(keys(&value(&data, &skipped)), ["s5"]);
        assert_eq!(keys(&value(&data, &keyed)), ["s3", "s4"]);
        assert_eq!(keys(&value(&data, &first)["rows"]), ["s1"]);
        let context = format!("declared={declared}");

        // Another prefix, a value past every examined row, and an insert
        // after the last examined row cannot change any result.
        assert!(
            evaluated(
                &mut data,
                json!([write("b1", json!({"tenant":"b","score":0}))]),
                &fixture
            )
            .is_empty(),
            "{context}"
        );
        assert!(
            evaluated(
                &mut data,
                json!([write("s9", json!({"tenant":"a","score":9}))]),
                &fixture
            ) == cells([&skipped]),
            "{context}"
        );
        assert_eq!(keys(&value(&data, &skipped)), ["s6"]);
        assert!(
            evaluated(
                &mut data,
                json!([write("s3", json!({"tenant":"a","score":3,"label":"x"}))]),
                &fixture
            ) == cells([&keyed]),
            "{context}"
        );
        // The page's lookahead row matters only through its existence.
        assert!(
            evaluated(
                &mut data,
                json!([write("s2", json!({"tenant":"a","score":2,"label":"x"}))]),
                &fixture
            ) == cells([&top]),
            "{context}"
        );
        // The reverse scan skipped s9 by offset: its value is irrelevant.
        assert!(
            evaluated(
                &mut data,
                json!([write("s9", json!({"tenant":"a","score":9,"label":"x"}))]),
                &fixture
            )
            .is_empty(),
            "{context}"
        );
        assert!(
            evaluated(
                &mut data,
                json!([write("s6", json!({"tenant":"a","score":6,"label":"x"}))]),
                &fixture
            ) == cells([&skipped]),
            "{context}"
        );

        // Moving a row into the examined prefix changes it.
        let moved = evaluated(
            &mut data,
            json!([write("s4", json!({"tenant":"a","score":0}))]),
            &fixture,
        );
        assert_eq!(moved, cells([&first, &top, &keyed]), "{context}");
        assert_eq!(keys(&value(&data, &top)), ["s4", "s1"]);
        assert_eq!(keys(&value(&data, &first)["rows"]), ["s4"]);
        // So does deleting the page's lookahead row.
        assert_eq!(
            evaluated(&mut data, json!([delete("s1")]), &fixture),
            cells([&first, &top]),
            "{context}"
        );
        assert_eq!(keys(&value(&data, &top)), ["s4", "s2"]);
        assert!(!value(&data, &first)["cursor"].is_null());
        // A row leaving past the examined rows does not.
        assert!(
            evaluated(&mut data, json!([delete("s5")]), &fixture).is_empty(),
            "{context}"
        );
        assert_eq!(keys(&value(&data, &keyed)), ["s3", "s4"]);

        // Reloading rebuilds the windows from the stored dependencies.
        let mut restored: Records =
            serde_json::from_slice(&serde_json::to_vec(&data).unwrap()).unwrap();
        assert!(
            evaluated(
                &mut restored,
                json!([write("s7", json!({"tenant":"a","score":7}))]),
                &fixture
            ) == cells([&skipped]),
            "{context}"
        );
        assert_eq!(
            evaluated(
                &mut restored,
                json!([write("s0", json!({"tenant":"a","score":-1}))]),
                &fixture
            ),
            cells([&first, &top]),
            "{context}"
        );
        assert_eq!(keys(&value(&restored, &top)), ["s0", "s4"]);
    }
}

#[test]
fn windowed_scans_always_match_a_fresh_scan() {
    let fixture = windowed_fixture();
    let scans = [
        json!({}),
        json!({"limit":3}),
        json!({"gt":"k03","lte":"k08","limit":2}),
        json!({"gte":"k02","reverse":true,"offset":2,"limit":2}),
        json!({"index":"rank","prefix":["a"]}),
        json!({"index":"rank","prefix":["a"],"lte":4,"limit":3}),
        json!({"index":"rank","prefix":["a"],"gt":1,"reverse":true,"offset":1,"limit":2}),
        json!({"index":"rank","gte":"b","limit":1}),
        json!({"index":"rank","offset":20}),
        json!({"index":"rank","prefix":["a",3]}),
    ];
    // Cursors continue from a fixed position whatever rows exist, so pages
    // with `after` exercise windows that start (or, reversed, end) there.
    let cursor = |options: Value| {
        let mut data = Records::default();
        for n in 0..4 {
            data.insert(
                source_id("items", &format!("k{n:02}")),
                json!({"tenant":"a","score":n}),
            );
        }
        run(
            data,
            json!({"name":"page","args":options}),
            "query",
            None,
            &fixture,
        )
        .unwrap()
        .value["cursor"]
            .clone()
    };
    let mut pages = vec![
        json!({"prefix":["a"],"limit":2}),
        json!({"prefix":["a"],"gt":2,"reverse":true,"limit":1}),
    ];
    for options in [
        json!({"prefix":["a"],"limit":2}),
        json!({"prefix":["a"],"reverse":true,"limit":1}),
    ] {
        let mut continued = options.clone();
        continued["after"] = cursor(options);
        assert!(continued["after"].is_string());
        pages.push(continued);
    }
    for declared in [false, true] {
        let mut data = Records::default();
        if declared {
            install(&mut data, &fixture);
        }
        let materialize: Vec<_> = scans
            .iter()
            .map(|args| json!({"name":"read","args":args}))
            .chain(pages.iter().map(|args| json!({"name":"page","args":args})))
            .collect();
        deploy(&mut data, json!({ "materialize": materialize }), &fixture);
        let mut state = 0x2545_f491_4f6c_dd1du64;
        let mut next = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let (mut runs, mut checks) = (0, 0);
        for step in 0..300 {
            let key = format!("k{:02}", next(12));
            let change = match next(6) {
                0 => delete(&key),
                1 => write(&key, json!(next(3))),
                _ => {
                    let tenant = ["a", "b"][next(2) as usize];
                    write(
                        &key,
                        json!({"tenant":tenant,"score":next(7),"label":next(3)}),
                    )
                }
            };
            runs += evaluated(&mut data, json!([change]), &fixture).len();
            if step % 50 == 49 {
                data = serde_json::from_slice(&serde_json::to_vec(&data).unwrap()).unwrap();
            }
            for (name, args) in scans
                .iter()
                .map(|args| ("read", args))
                .chain(pages.iter().map(|args| ("page", args)))
            {
                let fresh = run(
                    data.clone(),
                    json!({"name":name,"args":args}),
                    "query",
                    None,
                    &fixture,
                )
                .unwrap()
                .value;
                let stored = &data[&cell_id(name, args)]["outcome"]["value"];
                assert_eq!(
                    stored, &fresh,
                    "declared={declared} step={step} {name} {args}"
                );
                checks += 1;
            }
        }
        // Precision, not just correctness: most writes leave most windows alone.
        assert!(
            runs * 2 < checks,
            "declared={declared}: {runs} reruns for {checks} cells"
        );
    }
}

fn matching(fields: Value, value: Value) -> Value {
    json!([{"kind":"query","collection":"items","fields":fields,"value":value}])
}

fn query_fixture() -> Fixture {
    Fixture::new([
        (
            "matches",
            (|args, host| host("query", matching(args[0].clone(), args[1].clone()))) as Callback,
        ),
        (
            "through",
            (|args, host| get(host, "derived", "matches", args.clone())) as Callback,
        ),
    ])
}

#[test]
fn undeclared_equality_queries_rerun_only_for_their_bucket() {
    let fixture = query_fixture();
    let mut data = Records::default();
    for (key, tenant, state) in [
        ("a1", "a", "open"),
        ("a2", "a", "done"),
        ("b1", "b", "open"),
    ] {
        data.insert(
            source_id("items", key),
            json!({"tenant":tenant,"state":state}),
        );
    }
    let single = json!([["state"], "open"]);
    let pair = json!([["tenant", "state"], ["a", "open"]]);
    deploy(
        &mut data,
        json!({"materialize":[{"name":"matches","args":single},{"name":"matches","args":pair}]}),
        &fixture,
    );
    let single = cell_id("matches", &single);
    let pair = cell_id("matches", &pair);
    let count = |data: &Records, id: &str| data[id]["outcome"]["value"].as_array().unwrap().len();
    assert_eq!((count(&data, &single), count(&data, &pair)), (2, 1));
    // Rows outside both buckets, and rows lacking a queried field.
    assert!(
        evaluated(
            &mut data,
            json!([write("a2", json!({"tenant":"a","state":"done","n":1}))]),
            &fixture
        )
        .is_empty()
    );
    assert!(
        evaluated(
            &mut data,
            json!([write("c1", json!({"tenant":"c"}))]),
            &fixture
        )
        .is_empty()
    );
    assert!(evaluated(&mut data, json!([write("c2", json!(["open"]))]), &fixture).is_empty());
    // A row changing within a bucket, entering one, and leaving one.
    assert_eq!(
        evaluated(
            &mut data,
            json!([write("b1", json!({"tenant":"b","state":"open","n":1}))]),
            &fixture
        ),
        cells([&single])
    );
    assert_eq!(
        evaluated(
            &mut data,
            json!([write("a2", json!({"tenant":"a","state":"open"}))]),
            &fixture
        ),
        cells([&single, &pair])
    );
    assert_eq!((count(&data, &single), count(&data, &pair)), (3, 2));
    assert_eq!(
        evaluated(&mut data, json!([delete("a1")]), &fixture),
        cells([&single, &pair])
    );
    assert_eq!((count(&data, &single), count(&data, &pair)), (2, 1));
    // Reloading rebuilds the queried field sets from stored dependencies.
    let mut restored: Records =
        serde_json::from_slice(&serde_json::to_vec(&data).unwrap()).unwrap();
    assert!(
        evaluated(
            &mut restored,
            json!([write("b2", json!({"tenant":"b","state":"done"}))]),
            &fixture
        )
        .is_empty()
    );
    assert_eq!(
        evaluated(
            &mut restored,
            json!([write("b2", json!({"tenant":"b","state":"open"}))]),
            &fixture
        ),
        cells([&single])
    );
    // Declaring the index keeps the same dependency and precision.
    let schema = Schema {
        indexes: vec![IndexSpec {
            collection: "items".into(),
            fields: vec!["state".into()],
        }],
        aggregates: BTreeMap::new(),
    };
    let declared = run_with_schema(
        restored.clone(),
        json!({"requestId":"schema"}),
        "deployment",
        None,
        &fixture,
        Some(schema),
    )
    .unwrap();
    apply(&mut restored, declared);
    assert!(
        evaluated(
            &mut restored,
            json!([write("c3", json!({"tenant":"c","state":"done"}))]),
            &fixture
        )
        .is_empty()
    );
    assert_eq!(
        evaluated(
            &mut restored,
            json!([write("c3", json!({"tenant":"c","state":"open"}))]),
            &fixture
        ),
        cells([&single])
    );
    assert_eq!(count(&restored, &single), 4);
}

#[test]
fn undeclared_equality_queries_always_match_a_fresh_query() {
    let fixture = query_fixture();
    let queries = [
        json!([["state"], "open"]),
        json!([["state"], {"n":1}]),
        json!([["tenant", "state"], ["a", "open"]]),
        json!([["missing"], null]),
    ];
    let mut data = Records::default();
    let materialize: Vec<_> = queries
        .iter()
        .map(|args| json!({"name":"matches","args":args}))
        .collect();
    deploy(&mut data, json!({ "materialize": materialize }), &fixture);
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = move |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    let (mut runs, mut checks) = (0, 0);
    for step in 0..300 {
        let key = format!("k{:02}", next(10));
        let change = match next(6) {
            0 => delete(&key),
            1 => write(&key, json!(next(2))),
            _ => {
                let tenant = ["a", "b"][next(2) as usize];
                let state =
                    [json!("open"), json!("done"), json!({"n":1})][next(3) as usize].clone();
                write(&key, json!({"tenant":tenant,"state":state,"label":next(3)}))
            }
        };
        runs += evaluated(&mut data, json!([change]), &fixture).len();
        if step % 50 == 49 {
            data = serde_json::from_slice(&serde_json::to_vec(&data).unwrap()).unwrap();
        }
        for args in &queries {
            let fresh = run(
                data.clone(),
                json!({"name":"matches","args":args}),
                "query",
                None,
                &fixture,
            )
            .unwrap()
            .value;
            let stored = &data[&cell_id("matches", args)]["outcome"]["value"];
            assert_eq!(stored, &fresh, "step={step} {args}");
            checks += 1;
        }
    }
    assert!(runs * 2 < checks, "{runs} reruns for {checks} cells");
}

#[test]
fn query_certificates_cover_undeclared_buckets_read_through_derivations() {
    let fixture = query_fixture();
    let mut data = Records::from([(
        source_id("items", "a1"),
        json!({"tenant":"a","state":"open"}),
    )]);
    // An undeclared bucket has no marker of its own: the cached result must
    // still notice a row entering it through the unmaterialized derivation.
    let result = run(
        data.clone(),
        json!({"name":"through","args":[["state"],"open"]}),
        "query",
        None,
        &fixture,
    )
    .unwrap();
    assert_eq!(result.value.as_array().unwrap().len(), 1);
    let certificate = result.query_certificate.unwrap();
    assert!(certificate.valid(&data));
    evaluated(
        &mut data,
        json!([write("a2", json!({"tenant":"a","state":"open"}))]),
        &fixture,
    );
    assert!(!certificate.valid(&data));
}

#[test]
fn scan_certificates_hold_only_while_their_results_hold() {
    let fixture = windowed_fixture();
    let calls = [
        ("read", json!({"limit":3})),
        ("read", json!({"gt":"k03","lte":"k08","limit":2})),
        (
            "read",
            json!({"gte":"k02","reverse":true,"offset":2,"limit":2}),
        ),
        (
            "read",
            json!({"index":"rank","prefix":["a"],"lte":4,"limit":3}),
        ),
        (
            "read",
            json!({"index":"rank","prefix":["a"],"gt":1,"reverse":true,"offset":1,"limit":2}),
        ),
        ("read", json!({"index":"rank","gte":"b","limit":1})),
        ("read", json!({"index":"rank","prefix":["b",3]})),
        ("page", json!({"prefix":["a"],"limit":2})),
        (
            "page",
            json!({"prefix":["b"],"gt":2,"reverse":true,"limit":1}),
        ),
    ];
    for declared in [false, true] {
        let mut data = Records::default();
        if declared {
            install(&mut data, &fixture);
        }
        let evaluate = |data: &Records, name: &str, args: &Value| {
            let result = run(
                data.clone(),
                json!({"name":name,"args":args}),
                "query",
                None,
                &fixture,
            )
            .unwrap();
            (result.value, result.query_certificate.unwrap())
        };
        let mut cached: Vec<_> = calls
            .iter()
            .map(|(name, args)| evaluate(&data, name, args))
            .collect();
        let mut state = 0x853c_49e6_748f_ea9bu64;
        let mut next = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let (mut kept, mut checks) = (0, 0);
        for step in 0..300 {
            let key = format!("k{:02}", next(12));
            let change = match next(6) {
                0 => delete(&key),
                1 => write(&key, json!(next(3))),
                _ => {
                    let tenant = ["a", "b"][next(2) as usize];
                    write(
                        &key,
                        json!({"tenant":tenant,"score":next(7),"label":next(3)}),
                    )
                }
            };
            evaluated(&mut data, json!([change]), &fixture);
            for ((name, args), (value, certificate)) in calls.iter().zip(&mut cached) {
                checks += 1;
                let (fresh, renewed) = evaluate(&data, name, args);
                if certificate.valid(&data) {
                    assert_eq!(
                        value, &fresh,
                        "declared={declared} step={step} {name} {args}"
                    );
                    kept += 1;
                } else {
                    // A failed check must not adopt the newer index marker.
                    assert!(!certificate.valid(&data) || value == &fresh);
                    (*value, *certificate) = (fresh, renewed);
                }
            }
        }
        // Collection- and index-wide stamps kept 5% and 10% of certificates
        // through these writes; key membership and windows keep 21% and 66%.
        let (share, minimum) = (kept * 100 / checks, if declared { 50 } else { 15 });
        assert!(
            share > minimum,
            "declared={declared}: kept {kept} of {checks}"
        );
    }
}

/// Windows within one bucket of their index's first field are found among
/// the reader records under a row's bucket, the rest in memory. Together
/// they find exactly the windows a linear scan does.
#[test]
fn bucketed_and_spanning_windows_find_the_readers_a_linear_scan_does() {
    use crate::evaluator::rust_engine::windows::Window;
    let mut state = 0x5ca9_u64;
    let mut next = move |bound: u64| {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        (state >> 33) % bound
    };
    let fields = vec!["tenant".to_owned(), "score".to_owned()];
    let tenants = ["a", "b", "c", "d"];
    let row = |next: &mut dyn FnMut(u64) -> u64| {
        let value = json!({"tenant": tenants[next(4) as usize], "score": next(20)});
        let key = format!("k{}", next(50));
        let position = crate::evaluator::rust_engine::ranges::position(Some(&fields), &key, &value).unwrap();
        (key, value, position)
    };
    let mut data = Records::default();
    let mut windows = Vec::new();
    for n in 0..300 {
        let (_, _, first) = row(&mut next);
        let (_, _, second) = row(&mut next);
        let (lower, upper) = match next(6) {
            // The whole index, or everything from a position on.
            0 => (String::new(), "~".to_owned()),
            1 => (first.min(second.clone()), "~".to_owned()),
            _ if first == second => continue,
            _ => (first.clone().min(second.clone()), first.max(second)),
        };
        let values = (next(2) == 0).then(|| (lower.clone(), upper.clone()));
        let window = Window { collection: "items".into(), fields: Some(fields.clone()), lower, upper, values };
        let cell = format!("cell:[\"reader\",{n}]");
        data.insert(cell.clone(), json!({"deps": [window.dependency()]}));
        data.insert(Records::reader_key(&window.dependency(), &cell), Value::Null);
        windows.push((window, cell));
    }
    let narrow = windows.iter().filter(|(window, _)| window.bucket().is_some()).count();
    assert!(narrow >= 30 && narrow < windows.len(), "{narrow} of {} within a bucket", windows.len());
    let backed = data.backed_copy();
    for step in 0..400 {
        let (key, before, _) = row(&mut next);
        let previous = (next(3) != 0).then_some(before);
        let after = match next(3) {
            0 => None,
            1 => previous.clone(),
            _ => Some(row(&mut next).1),
        };
        let position = |value: &Option<Value>| {
            value.as_ref().and_then(|value| crate::evaluator::rust_engine::ranges::position(Some(&fields), &key, value))
        };
        let (from, to) = (position(&previous), position(&after));
        let values = from.is_some() && from == to;
        let expected: BTreeSet<&str> = windows
            .iter()
            .filter(|(window, _)| {
                [&from, &to].into_iter().flatten().any(|position| {
                    let bounds = if values {
                        window.values.as_ref().map(|(lower, upper)| (lower, upper))
                    } else {
                        Some((&window.lower, &window.upper))
                    };
                    bounds.is_some_and(|(lower, upper)| lower <= position && position < upper)
                })
            })
            .map(|(_, cell)| cell.as_str())
            .collect();
        for records in [&data, &backed] {
            let mut found = Vec::new();
            crate::evaluator::rust_engine::metadata::scan_readers(records, "items", &key, previous.as_ref(), after.as_ref(), &mut found);
            let found: BTreeSet<&str> = found.iter().map(String::as_str).collect();
            assert_eq!(found, expected, "step {step}");
        }
    }
}
