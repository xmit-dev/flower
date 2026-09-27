//! Collection access policies, enforced on methods' host calls.
use super::*;
use crate::evaluator::rust_engine::indexes::IndexSpec;

fn notes() -> Value {
    json!({"kind":"collection","name":"notes","indexes":{"rank":["rank"]}})
}

fn fixture() -> Fixture {
    Fixture::new([
        (
            "get",
            (|args, host| host("get", json!([notes(), args]))) as Callback,
        ),
        (
            "scan",
            (|_, host| host("scan", json!([notes()]))) as Callback,
        ),
        (
            "scanRank",
            (|args, host| host("scan", json!([notes(), args]))) as Callback,
        ),
        (
            "byOwner",
            (|args, host| {
                host(
                    "query",
                    json!([{"kind":"query","collection":"notes","fields":["owner"],"value":args}]),
                )
            }) as Callback,
        ),
        (
            "bySecret",
            (|args, host| {
                host(
                    "query",
                    json!([{"kind":"query","collection":"notes","fields":["secret"],"value":args}]),
                )
            }) as Callback,
        ),
        (
            "range",
            (|args, host| {
                host(
                    "range",
                    json!([{"kind":"range","collection":"notes","fields":["rank"],"options":args}]),
                )
            }) as Callback,
        ),
        (
            "set",
            (|args, host| {
                set(
                    host,
                    "notes",
                    args["key"].as_str().unwrap(),
                    args["value"].clone(),
                )
            }) as Callback,
        ),
        (
            "delete",
            (|args, host| host("delete", json!([notes(), args]))) as Callback,
        ),
    ])
}

/// Owners read and write their notes; admins read everything; only admins
/// see `secret`, and nobody else can change it.
fn policy() -> Value {
    let owner = json!({"eq":[{"ref":["row","owner"]},{"ref":["principal","subject"]}]});
    let admin = json!({"eq":[{"ref":["principal","claims","role"]},{"value":"admin"}]});
    json!({
        "read": {"any":[owner, admin]},
        "insert": {"eq":[{"ref":["next","owner"]},{"ref":["principal","subject"]}]},
        "update": {"all":[owner, {"eq":[{"ref":["next","owner"]},{"ref":["row","owner"]}]}]},
        "delete": owner,
        "fields": {"secret": {"read": admin}},
    })
}

fn install(data: &mut Records, fixture: &Fixture) {
    install_with(data, fixture, policy(), None);
}

/// The notes fixture's rows (or `writes`) under another policy.
fn install_with(data: &mut Records, fixture: &Fixture, policy: Value, writes: Option<Value>) {
    let spec = |field: &str| IndexSpec {
        collection: "notes".into(),
        fields: vec![field.into()],
    };
    let result = run_with_schema(
        data.clone(),
        json!({"requestId":"schema","writes":writes.unwrap_or_else(|| json!([
            {"collection":"notes","key":"a1","value":{"owner":"alice","rank":1,"text":"one","secret":"s1"}},
            {"collection":"notes","key":"b1","value":{"owner":"bob","rank":2,"text":"two","secret":"s2"}},
            {"collection":"notes","key":"a2","value":{"owner":"alice","rank":3,"text":"three"}},
            {"collection":"notes","key":"b2","value":{"owner":"bob","rank":4,"text":"four"}},
            {"collection":"notes","key":"a3","value":{"owner":"alice","rank":5,"text":"five"}},
            {"collection":"notes","key":"x","value":{"rank":6,"text":"ownerless"}},
        ]))}),
        "deployment",
        None,
        fixture,
        Some(Schema {
            indexes: vec![spec("owner"), spec("rank"), spec("secret")],
            aggregates: BTreeMap::new(),
            policies: BTreeMap::from([(
                "notes".into(),
                serde_json::from_value(policy).unwrap(),
            )]),
        }),
    )
    .unwrap();
    apply(data, result);
}

fn setup() -> (Records, Fixture) {
    let fixture = fixture();
    let mut data = Records::default();
    install(&mut data, &fixture);
    (data, fixture)
}

fn alice() -> Value {
    json!({"subject":"alice"})
}
fn admin() -> Value {
    json!({"subject":"root","claims":{"role":"admin"}})
}

fn call(
    data: &Records,
    fixture: &Fixture,
    mode: &str,
    name: &str,
    args: Value,
    principal: Option<Value>,
) -> EngineResult<Evaluation> {
    let mut invocation = json!({"name":name,"args":args});
    if let Some(principal) = principal {
        invocation["$principal"] = principal;
    }
    run(data.clone(), invocation, mode, None, fixture)
}

fn query(
    data: &Records,
    fixture: &Fixture,
    name: &str,
    args: Value,
    principal: Option<Value>,
) -> Value {
    call(data, fixture, "query", name, args, principal)
        .unwrap()
        .value
}

fn keys(rows: &Value) -> Vec<&str> {
    rows.as_array()
        .unwrap()
        .iter()
        .map(|row| row["key"].as_str().unwrap())
        .collect()
}

fn texts(values: &Value) -> Vec<&str> {
    values
        .as_array()
        .unwrap()
        .iter()
        .map(|value| value["text"].as_str().unwrap())
        .collect()
}

#[test]
fn callers_see_only_rows_and_fields_their_policy_allows() {
    let (data, fixture) = setup();
    // An owner reads its rows, without the admin-only field.
    assert_eq!(
        query(&data, &fixture, "get", json!("a1"), Some(alice())),
        json!({"owner":"alice","rank":1,"text":"one"})
    );
    // Someone else's row reads as absent, like a missing one.
    assert_eq!(
        query(&data, &fixture, "get", json!("b1"), Some(alice())),
        Value::Null
    );
    assert_eq!(
        keys(&query(&data, &fixture, "scan", Value::Null, Some(alice()))),
        ["a1", "a2", "a3"]
    );
    // Admins see every row and field.
    assert_eq!(
        query(&data, &fixture, "get", json!("b1"), Some(admin())),
        json!({"owner":"bob","rank":2,"text":"two","secret":"s2"})
    );
    assert_eq!(
        keys(&query(&data, &fixture, "scan", Value::Null, Some(admin()))),
        ["a1", "a2", "a3", "b1", "b2", "x"]
    );
    // Anonymous callers, null or "$anonymous", match no owner: not even the
    // ownerless row, since comparisons with a missing side are false.
    for anonymous in [Value::Null, json!({"subject":"$anonymous"})] {
        assert_eq!(
            query(&data, &fixture, "get", json!("x"), Some(anonymous.clone())),
            Value::Null
        );
        assert_eq!(
            query(&data, &fixture, "scan", Value::Null, Some(anonymous)),
            json!([])
        );
    }
    // Invocations without a caller (maintenance, the authorization hook) see all.
    assert_eq!(
        keys(&query(&data, &fixture, "scan", Value::Null, None)),
        ["a1", "a2", "a3", "b1", "b2", "x"]
    );
    assert_eq!(
        query(&data, &fixture, "get", json!("a1"), None)["secret"],
        "s1"
    );
}

#[test]
fn queries_skip_hidden_rows_and_rows_selected_by_hidden_fields() {
    let (data, fixture) = setup();
    assert_eq!(
        texts(&query(
            &data,
            &fixture,
            "byOwner",
            json!("alice"),
            Some(alice())
        )),
        ["one", "three", "five"]
    );
    assert!(
        query(&data, &fixture, "byOwner", json!("alice"), Some(alice()))[0]
            .get("secret")
            .is_none()
    );
    assert_eq!(
        query(&data, &fixture, "byOwner", json!("bob"), Some(alice())),
        json!([])
    );
    // Finding a row by a field the caller can't read would reveal the field.
    assert_eq!(
        query(&data, &fixture, "bySecret", json!("s1"), Some(alice())),
        json!([])
    );
    assert_eq!(
        texts(&query(
            &data,
            &fixture,
            "bySecret",
            json!("s1"),
            Some(admin())
        )),
        ["one"]
    );
}

#[test]
fn range_pages_fill_with_visible_rows_and_cursors_skip_hidden_ones() {
    let (data, fixture) = setup();
    // Ranks 1..6 interleave alice's rows with bob's and the ownerless one.
    let first = query(&data, &fixture, "range", json!({"limit":2}), Some(alice()));
    assert_eq!(keys(&first["rows"]), ["a1", "a2"]);
    assert!(first["rows"][0]["value"].get("secret").is_none());
    let cursor = first["cursor"].as_str().unwrap();
    // The cursor stops at a visible row: its position encodes a2's rank.
    let position: Value = serde_json::from_str(cursor).unwrap();
    assert!(
        position["last"].as_str().unwrap().ends_with(":a2"),
        "{position}"
    );
    let second = query(
        &data,
        &fixture,
        "range",
        json!({"limit":2,"after":cursor}),
        Some(alice()),
    );
    assert_eq!(keys(&second["rows"]), ["a3"]);
    assert_eq!(second["cursor"], Value::Null);
    // Offsets and limits of indexed scans count visible rows only.
    assert_eq!(
        keys(&query(
            &data,
            &fixture,
            "scanRank",
            json!({"index":"rank","offset":1,"limit":2}),
            Some(alice())
        )),
        ["a2", "a3"]
    );
    assert_eq!(
        keys(&query(
            &data,
            &fixture,
            "scanRank",
            json!({"index":"rank","reverse":true,"limit":1}),
            Some(alice())
        )),
        ["a3"]
    );
    assert_eq!(
        keys(&query(&data, &fixture, "range", json!({"limit":3}), Some(admin()))["rows"]),
        ["a1", "b1", "a2"]
    );
}

#[test]
fn writes_follow_insert_update_and_delete_rules() {
    let (data, fixture) = setup();
    let write = |key: &str, value: Value, principal: Value| {
        call(
            &data,
            &fixture,
            "mutation",
            "set",
            json!({"key":key,"value":value}),
            Some(principal),
        )
    };
    let denied = |result: EngineResult<Evaluation>| {
        let error = result.err().expect("denied");
        assert_eq!(error.code, "ACCESS_DENIED");
        error.message
    };
    let stored = |result: EngineResult<Evaluation>, key: &str| {
        result.unwrap().puts[&source_id("notes", key)].clone()
    };
    // Insert: only as yourself.
    stored(
        write(
            "a4",
            json!({"owner":"alice","rank":7,"text":"new"}),
            alice(),
        ),
        "a4",
    );
    let insert = denied(write(
        "n",
        json!({"owner":"bob","rank":8,"text":"forged"}),
        alice(),
    ));
    // Update: only your rows, and only keeping yourself as owner. Denied
    // inserts and updates look alike, whether or not the row exists.
    let update = denied(write(
        "b1",
        json!({"owner":"alice","rank":2,"text":"mine"}),
        alice(),
    ));
    assert_eq!(insert, update);
    denied(write(
        "a1",
        json!({"owner":"bob","rank":1,"text":"gift"}),
        alice(),
    ));
    // A read-modify-write of a redacted row keeps the fields it couldn't see.
    assert_eq!(
        stored(
            write(
                "a1",
                json!({"owner":"alice","rank":1,"text":"edited"}),
                alice()
            ),
            "a1"
        ),
        json!({"owner":"alice","rank":1,"text":"edited","secret":"s1"})
    );
    // Nobody changes a field they can't read.
    denied(write(
        "a1",
        json!({"owner":"alice","rank":1,"text":"one","secret":"forged"}),
        alice(),
    ));
    denied(write(
        "a2",
        json!({"owner":"alice","rank":3,"text":"three","secret":"new"}),
        alice(),
    ));
    // Admins read the secret, but the row rules still apply to them.
    denied(write(
        "a1",
        json!({"owner":"alice","rank":1,"text":"one","secret":"s9"}),
        admin(),
    ));
    // Delete: only your rows; deleting nothing is a no-op.
    denied(call(
        &data,
        &fixture,
        "mutation",
        "delete",
        json!("b1"),
        Some(alice()),
    ));
    let deleted = call(
        &data,
        &fixture,
        "mutation",
        "delete",
        json!("a2"),
        Some(alice()),
    )
    .unwrap();
    assert!(deleted.deletes.contains(&source_id("notes", "a2")));
    call(
        &data,
        &fixture,
        "mutation",
        "delete",
        json!("missing"),
        Some(alice()),
    )
    .unwrap();
    // Without a caller, nothing is enforced.
    stored(
        call(
            &data,
            &fixture,
            "mutation",
            "set",
            json!({"key":"b1","value":{"rank":2}}),
            None,
        ),
        "b1",
    );
}

#[test]
fn policies_are_validated_before_they_are_stored() {
    let invalid = |policy: Value| {
        serde_json::from_value::<Policy>(policy)
            .map_err(|error| error.to_string())
            .and_then(|policy| policy.validate())
            .expect_err("invalid")
    };
    // `next` exists only while writing; `row` does not exist on insert.
    assert!(invalid(json!({"read":{"exists":{"ref":["next","owner"]}}})).contains("read"));
    assert!(invalid(json!({"insert":{"exists":{"ref":["row","owner"]}}})).contains("insert"));
    assert!(invalid(json!({"delete":{"exists":{"ref":["next","owner"]}}})).contains("delete"));
    assert!(
        invalid(json!({"read":{"exists":{"ref":["principal","password"]}}}))
            .contains("principal.password")
    );
    assert!(invalid(json!({"read":{"exists":{"ref":["key",""]}}})).contains("key."));
    assert!(invalid(json!({"read":{"exists":{"ref":["now","day"]}}})).contains("now.day"));
    assert!(invalid(json!({"read":{"exists":{"ref":["row"]}}})).contains("row"));
    assert!(invalid(json!({"fields":{"secret":{}}})).contains("no rule"));
    invalid(json!({"read":{"le":[{"value":1},{"value":2}]}}));
    invalid(json!({"read":{"lt":[{"value":1}]}}));
    invalid(json!({"read":true}));
    invalid(json!({"reads":{"const":true}}));
    let wide = json!({"read":{"any":vec![json!({"const":false}); 256]}});
    assert!(invalid(wide).contains("256 nodes"));
    // Unspecified operations are denied.
    let policy: Policy = serde_json::from_value(json!({"read":{"const":true}})).unwrap();
    policy.validate().unwrap();
    assert_eq!(
        policy.insert,
        crate::evaluator::rust_engine::access::Rule::Const(false)
    );
}

/// `cargo test --release --lib policy_overhead -- --ignored --nocapture`
#[test]
#[ignore]
fn policy_overhead() {
    let fixture = fixture();
    let mut data = Records::default();
    install(&mut data, &fixture);
    let writes: Vec<Value> = (0..20_000)
        .map(|index| {
            let owner = if index % 2 == 0 { "alice" } else { "bob" };
            json!({"collection":"notes","key":format!("n{index:05}"),
                "value":{"owner":owner,"rank":index,"text":"note","secret":"s"}})
        })
        .collect();
    let result = run(
        data.clone(),
        json!({"requestId":"bulk","writes":writes}),
        "deployment",
        None,
        &fixture,
    )
    .unwrap();
    apply(&mut data, result);
    for (label, principal) in [
        ("no caller (policy skipped)", None),
        ("admin (rules fold to true)", Some(admin())),
        ("alice (per-row check + redaction)", Some(alice())),
    ] {
        let mut best = std::time::Duration::MAX;
        let mut rows = 0;
        for _ in 0..5 {
            let started = std::time::Instant::now();
            let value = query(&data, &fixture, "scan", Value::Null, principal.clone());
            best = best.min(started.elapsed());
            rows = value.as_array().unwrap().len();
        }
        println!("scan 20,006 rows as {label}: {rows} returned, best of 5 {best:?}");
    }
}

fn with_policy(policy: Value, writes: Option<Value>) -> (Records, Fixture) {
    let fixture = fixture();
    let mut data = Records::default();
    install_with(&mut data, &fixture, policy, writes);
    (data, fixture)
}

fn visible_keys(policy: Value) -> Vec<String> {
    let (data, fixture) = with_policy(policy, None);
    keys(&query(&data, &fixture, "scan", Value::Null, Some(alice())))
        .into_iter()
        .map(str::to_owned)
        .collect()
}

#[test]
fn rules_order_numbers_and_strings_and_match_prefixes() {
    let rank = |op: &str, value: Value| json!({op:[{"ref":["row","rank"]},{"value":value}]});
    // Ranks: a1 1, b1 2, a2 3, b2 4, a3 5, x 6.
    assert_eq!(
        visible_keys(json!({"read":{"all":[rank("gte", json!(2)), rank("lt", json!(5))]}})),
        ["a2", "b1", "b2"]
    );
    assert_eq!(
        visible_keys(json!({"read":{"any":[rank("lte", json!(1)), rank("gt", json!(5.5))]}})),
        ["a1", "x"]
    );
    // Strings order by code point; numbers never order against strings.
    assert_eq!(
        visible_keys(json!({"read":{"gt":[{"ref":["row","text"]},{"value":"one"}]}})),
        ["a2", "b1", "x"]
    );
    assert!(visible_keys(json!({"read":rank("lt", json!("9"))})).is_empty());
    assert!(
        visible_keys(json!({"read":{"lt":[{"ref":["row","missing"]},{"value":9}]}})).is_empty()
    );
    // Prefixes of keys and strings.
    assert_eq!(
        visible_keys(json!({"read":{"startsWith":[{"ref":["key"]},{"value":"b"}]}})),
        ["b1", "b2"]
    );
    assert_eq!(
        visible_keys(json!({"read":{"startsWith":[{"ref":["row","text"]},{"value":"f"}]}})),
        ["a3", "b2"]
    );
    assert!(
        visible_keys(json!({"read":{"startsWith":[{"ref":["row","rank"]},{"value":"1"}]}}))
            .is_empty()
    );
}

#[test]
fn rules_read_parts_of_json_keys() {
    // Keys of the caller's own: reads and inserts stay inside ["<subject>", …].
    let mine = json!({"eq":[{"ref":["key","0"]},{"ref":["principal","subject"]}]});
    let writes = json!([
        {"collection":"notes","key":"[\"alice\",1]","value":{"rank":1,"text":"one"}},
        {"collection":"notes","key":"[\"bob\",1]","value":{"rank":2,"text":"two"}},
        {"collection":"notes","key":"[\"alice\",2]","value":{"rank":3,"text":"three"}},
        {"collection":"notes","key":"{\"owner\":\"alice\"}","value":{"rank":4,"text":"object"}},
        {"collection":"notes","key":"alice","value":{"rank":5,"text":"plain"}},
    ]);
    let (data, fixture) = with_policy(
        json!({"read":mine,"insert":mine,"update":mine,"delete":mine}),
        Some(writes),
    );
    assert_eq!(
        keys(&query(&data, &fixture, "scan", Value::Null, Some(alice()))),
        ["[\"alice\",1]", "[\"alice\",2]"]
    );
    let write = |key: &str| {
        call(
            &data,
            &fixture,
            "mutation",
            "set",
            json!({"key":key,"value":{"rank":9,"text":"new"}}),
            Some(alice()),
        )
    };
    write("[\"alice\",3]").unwrap();
    assert_eq!(write("[\"bob\",2]").err().unwrap().code, "ACCESS_DENIED");
    // A plain string key has no parts.
    assert_eq!(write("alice").err().unwrap().code, "ACCESS_DENIED");
    // Object keys have named parts.
    let (data, fixture) = with_policy(
        json!({"read":{"eq":[{"ref":["key","owner"]},{"ref":["principal","subject"]}]}}),
        Some(json!([
            {"collection":"notes","key":"{\"owner\":\"alice\"}","value":{"rank":4,"text":"object"}},
            {"collection":"notes","key":"{\"owner\":\"bob\"}","value":{"rank":5,"text":"other"}},
        ])),
    );
    assert_eq!(
        keys(&query(&data, &fixture, "scan", Value::Null, Some(alice()))),
        ["{\"owner\":\"alice\"}"]
    );
}

#[test]
fn rules_about_now_are_time_dependent_and_say_when_they_flip() {
    let staff = json!({"eq":[{"ref":["principal","claims","role"]},{"value":"admin"}]});
    let live = json!({"gt":[{"ref":["row","expiresAt"]},{"ref":["now"]}]});
    let (data, fixture) = with_policy(
        json!({"read":{"any":[staff, live]}}),
        Some(json!([
            {"collection":"notes","key":"gone","value":{"expiresAt":1500,"text":"gone"}},
            {"collection":"notes","key":"soon","value":{"expiresAt":2500,"text":"soon"}},
            {"collection":"notes","key":"late","value":{"expiresAt":3000.5,"text":"late"}},
            {"collection":"notes","key":"never","value":{"text":"never"}},
        ])),
    );
    let at = |now: u64, principal: Value| {
        run(
            data.clone(),
            json!({"name":"scan","args":null,"$principal":principal}),
            "query",
            Some(now),
            &fixture,
        )
        .unwrap()
    };
    let result = at(2000, alice());
    assert_eq!(keys(&result.value), ["late", "soon"]);
    assert!(result.query_clock_polled);
    assert!(!result.query_cacheable);
    // `expiresAt > now` stops holding when now reaches expiresAt.
    assert_eq!(result.query_changes_at, Some(2500));
    let result = at(2500, alice());
    assert_eq!(keys(&result.value), ["late"]);
    assert_eq!(result.query_changes_at, Some(3001));
    let result = at(3001, alice());
    assert_eq!(result.value, json!([]));
    assert_eq!(result.query_changes_at, None);
    // A rule that folds away for the caller never reads the clock.
    let result = at(2000, admin());
    assert_eq!(keys(&result.value).len(), 4);
    assert!(!result.query_clock_polled);
    assert_eq!(result.query_changes_at, None);
    // Point reads report the time dependency too.
    let point = run(
        data.clone(),
        json!({"name":"get","args":"soon","$principal":alice()}),
        "query",
        Some(2000),
        &fixture,
    )
    .unwrap();
    assert_eq!(point.value["text"], "soon");
    assert_eq!(point.query_changes_at, Some(2500));
}
