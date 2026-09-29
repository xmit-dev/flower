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
            policies: Default::default(),
            derived_access: Default::default(),
            aggregate_versions: Default::default(),
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
    assert!(
        keys(&read(
            &data,
            json!({"prefix":["a"],"gte":7,"lt":2,"limit":1})
        ))
        .is_empty()
    );
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
    assert!(
        data[&id]["outcome"]["value"]["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
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
    assert!(
        data[&id]["outcome"]["value"]["rows"]
            .as_array()
            .unwrap()
            .is_empty()
    );
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

/// The budgets e082330 applied to rows on their way to the guest as owned JSON:
/// each row sized as held (`rows_for_host`), then, for a page, the owned page
/// that `json!` built sized again (`copy_for_host`). The smallest output and
/// retained limits that let the reply through.
fn owned_reply_limits(rows: &[(String, Arc<Value>)], page: Option<Option<&str>>) -> (usize, usize) {
    let mut bytes = 2usize;
    let mut allocation = 64usize;
    for (index, (key, value)) in rows.iter().enumerate() {
        bytes += 17 + usize::from(index != 0) + string_len(key) + encoded_len(value);
        allocation += 256 + key.len() + allocation_cost(value);
    }
    let owned = Value::Array(
        rows.iter()
            .map(|(key, value)| json!({"key":key,"value":**value}))
            .collect(),
    );
    match page {
        None => (bytes, allocation),
        Some(cursor) => {
            let page = json!({"rows":owned,"cursor":cursor});
            (
                bytes.max(encoded_len(&page)),
                allocation.max(allocation_cost(&page)),
            )
        }
    }
}

/// Values as the engine holds them. With FLOWER_TEST_BACKED, the engine reads
/// a stored copy instead, whose strings have no spare capacity.
fn held_rows(data: &Records, keys: &[&str]) -> Vec<(String, Arc<Value>)> {
    keys.iter()
        .map(|key| {
            let value = data.get_shared(&source_id("items", key)).unwrap().clone();
            let value = if std::env::var_os("FLOWER_TEST_BACKED").is_some() {
                Arc::new(serde_json::from_slice(&serde_json::to_vec(&*value).unwrap()).unwrap())
            } else {
                value
            };
            ((*key).to_owned(), value)
        })
        .collect()
}

fn budget_fixture() -> Fixture {
    // Each method returns only a count, so that the reply is the one
    // allocation near the limit.
    Fixture::new([
        (
            "page",
            (|args, host| {
                let page = host("range", json!([args]))?;
                Ok(json!([
                    page["rows"].as_array().unwrap().len(),
                    page["cursor"]
                ]))
            }) as Callback,
        ),
        (
            "scanned",
            (|args, host| {
                let rows = host("scan", json!([{"kind":"collection","name":"items"}, args]))?;
                Ok(json!(rows.as_array().unwrap().len()))
            }) as Callback,
        ),
        (
            "all",
            (|_, host| {
                let rows = host("scan", json!([{"kind":"collection","name":"items"}]))?;
                Ok(json!(rows.as_array().unwrap().len()))
            }) as Callback,
        ),
        (
            "one",
            (|args, host| {
                let row = host("get", json!([{"kind":"collection","name":"items"}, args]))?;
                Ok(json!(row.is_object()))
            }) as Callback,
        ),
        (
            "matching",
            (|args, host| {
                let rows = host(
                    "query",
                    json!([{"kind":"query","collection":"items","fields":["tenant"],"value":args}]),
                )?;
                Ok(json!(rows.as_array().unwrap().len()))
            }) as Callback,
        ),
    ])
}

/// Run `name` with the retained limit just at and just below `limit`: at it the
/// reply goes through, one byte less fails with the context budget.
fn trips_just_below(data: &Records, name: &str, args: Value, limit: usize) -> Value {
    let invocation = json!({"name":name,"args":args});
    let fixture = budget_fixture();
    let value = run_with_limit(
        data.clone(),
        invocation.clone(),
        "query",
        None,
        &fixture,
        limit,
    )
    .unwrap_or_else(|error| panic!("{name} at {limit}: {error:?}"))
    .value;
    let error =
        run_with_limit(data.clone(), invocation, "query", None, &fixture, limit - 1).unwrap_err();
    assert_eq!(
        (error.code.as_str(), error.message.as_str()),
        (
            "EVALUATION_BUDGET",
            "Context result exceeds the JSON or Rust memory budget"
        ),
        "{name} at {}",
        limit - 1
    );
    value
}

fn page_args(limit: usize) -> Value {
    reference(json!({"prefix":["a"],"limit":limit}))
}

/// The cursor a page of `limit` rows returns, as `budget_fixture`'s "page" reports it.
fn page_cursor(data: &Records, limit: usize) -> Option<String> {
    let value = run(
        data.clone(),
        json!({"name":"page","args":page_args(limit)}),
        "query",
        None,
        &budget_fixture(),
    )
    .unwrap()
    .value;
    value[1].as_str().map(str::to_owned)
}

#[test]
fn shared_row_replies_trip_their_budgets_exactly_where_owned_copies_did() {
    let mut data = Records::default();
    let keys = ["k0", "k1", "k2", "k3", "k4", "k5"];
    for (n, key) in keys.iter().enumerate() {
        // Strings, numbers, nesting, escapes and non-ASCII keys: every kind of
        // size the budgets count.
        let mut row = json!({"tenant":"a","score":n,"memory":{"é":[1.5,-3,null,true,0.1]},"at":1_790_000_000_000u64+n as u64,"big":u64::MAX});
        // Moved in, not through json!, which would copy it to its length.
        let mut text = String::with_capacity(if n == 5 { 12_000 } else { 0 });
        text.push_str(&"x\"\n".repeat(700 + n));
        row["memory"]["text"] = Value::String(text);
        data.insert(source_id("items", key), row);
    }
    install(&mut data, &fixture());
    let rows = held_rows(&data, &keys);
    let held = |value: &Value| allocation_cost(value);

    // A page with a cursor: the owned page's allocation (its object, keys and
    // strings copied to their length) binds.
    let cursor = page_cursor(&data, 3).expect("a page with more");
    let (_, limit) = owned_reply_limits(&rows[..3], Some(Some(&cursor)));
    assert_eq!(
        trips_just_below(&data, "page", page_args(3), limit),
        json!([3, cursor])
    );
    // The last page, through the row with spare capacity: the rows as held bind.
    assert_eq!(page_cursor(&data, 9), None);
    let (_, limit) = owned_reply_limits(&rows, Some(None));
    if std::env::var_os("FLOWER_TEST_BACKED").is_none() {
        let (_, as_held) = owned_reply_limits(&rows, None);
        assert_eq!(limit, as_held, "the rows as held are what binds here");
    }
    assert_eq!(
        trips_just_below(&data, "page", page_args(9), limit),
        json!([6, null])
    );
    // Scans: rows as held, no page around them.
    let (_, limit) = owned_reply_limits(&rows[..4], None);
    assert_eq!(
        trips_just_below(&data, "scanned", json!({"limit":4}), limit),
        json!(4)
    );
    let (_, limit) = owned_reply_limits(&rows, None);
    assert_eq!(trips_just_below(&data, "all", Value::Null, limit), json!(6));
    // One row, and the values a query matches.
    assert_eq!(
        trips_just_below(&data, "one", json!("k5"), held(&rows[5].1)),
        json!(true)
    );
    let limit = 64 + rows.iter().map(|(_, value)| held(value)).sum::<usize>();
    assert_eq!(
        trips_just_below(&data, "matching", json!("a"), limit),
        json!(6)
    );
}

#[test]
fn shared_row_replies_trip_the_output_limit_exactly_where_owned_copies_did() {
    let limit = crate::evaluator::config::settings()
        .unwrap()
        .result_max_bytes;
    let insert = |data: &mut Records, length: usize| {
        data.insert(
            source_id("items", "k0"),
            json!({"tenant":"a","score":0,"text":format!("é\t{}", "x".repeat(length))}),
        );
    };
    let mut data = Records::default();
    // Both rows exist before the index does; k0 then changes only its text.
    insert(&mut data, 0);
    data.insert(source_id("items", "k1"), json!({"tenant":"a","score":1}));
    install(&mut data, &fixture());
    for (name, args, page) in [
        ("page", page_args(1), true),
        ("scanned", json!({"limit":1}), false),
    ] {
        // Grow k0 until the reply's JSON is exactly the limit long.
        insert(&mut data, 0);
        let cursor = page.then(|| Some(page_cursor(&data, 1).expect("a page with more")));
        let length = |data: &Records| {
            let rows = held_rows(data, &["k0"]);
            owned_reply_limits(&rows, cursor.as_ref().map(Option::as_deref)).0
        };
        let short = limit - length(&data);
        insert(&mut data, short);
        assert_eq!(length(&data), limit);
        let invocation = json!({"name":name,"args":args});
        run(
            data.clone(),
            invocation.clone(),
            "query",
            None,
            &budget_fixture(),
        )
        .unwrap_or_else(|error| panic!("{name} at the limit: {error:?}"));
        insert(&mut data, short + 1);
        assert_eq!(length(&data), limit + 1);
        let error = run(data.clone(), invocation, "query", None, &budget_fixture()).unwrap_err();
        assert_eq!(
            error.message, "Context result exceeds the JSON or Rust memory budget",
            "{name}"
        );
    }
}

/// Microbench: pages of 64 rows of about 10 KB (Ultimator session rows: a
/// memory snapshot of about half the row, then a hundred small fields) read
/// through the engine and written by the guest ABI encoder, as a guest's
/// ctx.range receives them. Reports thread CPU time per page:
///
/// ```sh
/// FLOWER_RANGE_BENCH_CALLS=2000 cargo test --release --lib range_reply_costs -- --ignored --nocapture
/// ```
#[test]
#[ignore]
fn range_reply_costs() {
    struct Encoding;
    impl Executor for Encoding {
        fn execute(
            &self,
            _: &str,
            _: &str,
            args: &Value,
            host: &mut EngineHost<'_>,
        ) -> EngineResult<Value> {
            let reply = host("range", json!([args]))?;
            let bytes = crate::evaluator::wire::success_reply(&reply).unwrap();
            Ok(json!(bytes.len()))
        }
    }
    fn thread_cpu() -> f64 {
        let mut time = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: clock_gettime writes the timespec it is given.
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut time) };
        time.tv_sec as f64 + time.tv_nsec as f64 * 1e-9
    }
    let mut data = Records::default();
    let at = 1_790_000_000_000u64;
    for n in 0..200u64 {
        let row = json!({
            "tenant": "a",
            "score": n,
            "id": format!("s{n:015}"),
            "org": "org-2qi5nxyequ5l7t3b",
            "title": format!("Session {n}: make every range reply cheaper"),
            "status": if n % 3 == 0 { "working" } else { "idle" },
            "createdAt": at + n,
            "updatedAt": at + 1000 * n,
            "memory": format!("# Memory index {n}\n\n{}", "- `notes.md` — **what a session learned, its numbers and scripts** — with a path .dev/workspace/x.\n".repeat(64)),
            "skills": (0..8).map(|i| json!({"name": format!("skill-{i}"), "description": "Load when the work matches: steps, checks and the files they need, in a sentence or two of text."})).collect::<Vec<_>>(),
            "checkpoints": (0..12).map(|i| json!({"at": at + i, "seq": i, "tokens": 123_456 + i, "cost": 0.125 * i as f64, "model": "claude"})).collect::<Vec<_>>(),
            "lastText": "The page went from three copies to one encoding. ".repeat(16),
            "tasks": (0..6).map(|i| json!({"text": format!("Task {i}: measure and land"), "status": "done", "note": null})).collect::<Vec<_>>(),
            "turn": {"started": at, "tools": 12, "model": "claude", "thinking": true, "usage": {"input": 1234, "output": 567}},
            "labels": ["flower", "cpu"],
            "assignees": ["d62qtenunf6oifeg"],
            "unread": 3,
            "archived": false,
        });
        data.insert(source_id("items", &format!("key{n:04}")), row);
    }
    install(&mut data, &fixture());
    let row = data.get(&source_id("items", "key0007")).unwrap();
    let row_bytes = encoded_len(row);
    let calls: usize =
        std::env::var("FLOWER_RANGE_BENCH_CALLS").map_or(500, |calls| calls.parse().unwrap());
    let page = |offset: usize| {
        let invocation = json!({"name":"read","args":reference(json!({"prefix":["a"],"gte":offset,"limit":64}))});
        run(data.clone(), invocation, "query", None, &Encoding)
            .unwrap()
            .value
    };
    let encoded = page(0);
    for index in 0..calls / 10 {
        page(index % 100);
    }
    let (cpu, wall) = (thread_cpu(), std::time::Instant::now());
    for index in 0..calls {
        page(index % 100);
    }
    let cpu = (thread_cpu() - cpu) * 1e6 / calls as f64;
    let wall = wall.elapsed().as_secs_f64() * 1e6 / calls as f64;
    println!(
        "64-row pages of {row_bytes}-byte rows ({encoded} wire bytes): {cpu:.1} µs CPU, {wall:.1} µs wall per page over {calls}"
    );
}
