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
        (
            "definerScan",
            (|_, host| {
                host("definer", json!([true]))?;
                host("definer", json!([true]))?;
                host("definer", json!([false]))?;
                let all = host("scan", json!([notes()]))?;
                host("definer", json!([false]))?;
                let mine = host("scan", json!([notes()]))?;
                Ok(json!({"all": all, "mine": mine}))
            }) as Callback,
        ),
        (
            "definerWrite",
            (|args, host| {
                host("definer", json!([true]))?;
                host(
                    "set",
                    json!([notes(), args, {"owner":"bob","rank":0,"text":"audit"}]),
                )
            }) as Callback,
        ),
        (
            "definerUnmatched",
            (|args, host| host("definer", json!([args]))) as Callback,
        ),
        (
            // Derived: every note of an owner, whoever asks.
            "countFor",
            (|args, host| {
                let owner = args.get("owner").unwrap_or(args).clone();
                let rows = host("scan", json!([notes()]))?;
                let count = rows
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|row| row["value"]["owner"] == owner)
                    .count();
                Ok(json!(count))
            }) as Callback,
        ),
        (
            "readCount",
            (|args, host| get(host, "derived", "countFor", args.clone())) as Callback,
        ),
        (
            "total",
            (|_, host| {
                Ok(json!(
                    host("scan", json!([notes()]))?.as_array().unwrap().len()
                ))
            }) as Callback,
        ),
        (
            "readTotal",
            (|_, host| get(host, "derived", "total", Value::Null)) as Callback,
        ),
        (
            "setWith",
            (|args, host| {
                host(
                    "set",
                    json!([notes(), args["key"], args["value"], args["options"]]),
                )
            }) as Callback,
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
    install_with(data, fixture, policy(), None, None);
}

/// The notes fixture's rows (or `writes`) under another policy.
fn install_with(
    data: &mut Records,
    fixture: &Fixture,
    policy: Value,
    writes: Option<Value>,
    derived: Option<Value>,
) {
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
            derived_access: derived
                .map(|rules| serde_json::from_value(rules).unwrap())
                .unwrap_or_default(),
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
    // Delete: only your rows; deleting nothing is a no-op, and so is deleting
    // a row you can't see, so the outcome doesn't reveal that it exists.
    let hidden = call(
        &data,
        &fixture,
        "mutation",
        "delete",
        json!("b1"),
        Some(alice()),
    )
    .unwrap();
    assert!(hidden.deletes.is_empty() && hidden.puts.is_empty());
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
    install_with(&mut data, &fixture, policy, writes, None);
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

#[test]
fn clear_removes_hidden_fields_when_their_write_rule_allows() {
    let write = |data: &Records, fixture: &Fixture, value: Value, options: Value| {
        call(
            data,
            fixture,
            "mutation",
            "setWith",
            json!({"key":"a1","value":value,"options":options}),
            Some(alice()),
        )
    };
    let edited = json!({"owner":"alice","rank":1,"text":"edited"});
    // `secret` is admin-only with no write rule: its owner can't clear it.
    let (data, fixture) = setup();
    let error = write(&data, &fixture, edited.clone(), json!({"clear":["secret"]}))
        .err()
        .expect("denied");
    assert_eq!(error.code, "ACCESS_DENIED");
    // A write-only field: admins read it, owners may set or clear it.
    let mut policy = policy();
    policy["fields"]["secret"]["write"] =
        json!({"eq":[{"ref":["row","owner"]},{"ref":["principal","subject"]}]});
    let (data, fixture) = with_policy(policy, None);
    let stored =
        |result: EngineResult<Evaluation>| result.unwrap().puts[&source_id("notes", "a1")].clone();
    assert_eq!(
        stored(write(&data, &fixture, edited.clone(), Value::Null))["secret"],
        "s1"
    );
    assert_eq!(
        stored(write(&data, &fixture, edited.clone(), json!({"clear":[]})))["secret"],
        "s1"
    );
    assert_eq!(
        stored(write(
            &data,
            &fixture,
            edited.clone(),
            json!({"clear":["secret"]})
        )),
        edited
    );
    // Clearing a field the value writes, or one that isn't there, changes nothing.
    assert_eq!(
        stored(write(
            &data,
            &fixture,
            edited.clone(),
            json!({"clear":["text","gone"]})
        ))["secret"],
        "s1"
    );
    let mut rotated = edited.clone();
    rotated["secret"] = json!("s9");
    assert_eq!(
        stored(write(&data, &fixture, rotated.clone(), Value::Null))["secret"],
        "s9"
    );
    for options in [
        json!({"clear":"secret"}),
        json!({"clear":[""]}),
        json!({"clear":[1]}),
        json!({"merge":true}),
        json!(true),
    ] {
        let error = write(&data, &fixture, edited.clone(), options)
            .err()
            .expect("invalid");
        assert_eq!(error.code, "INVALID_VALUE");
    }
}

#[test]
fn denied_deletes_of_visible_rows_fail_and_write_only_rows_stay_deletable() {
    // Everyone reads, only owners delete: a visible row's denial is an error.
    let owner = json!({"eq":[{"ref":["row","owner"]},{"ref":["principal","subject"]}]});
    let (data, fixture) = with_policy(json!({"read":{"const":true},"delete":owner}), None);
    let error = call(
        &data,
        &fixture,
        "mutation",
        "delete",
        json!("b1"),
        Some(alice()),
    )
    .err()
    .expect("denied");
    assert_eq!(error.code, "ACCESS_DENIED");
    // Write-only rows (presence, say): nobody reads them, owners still delete theirs.
    let (data, fixture) = with_policy(json!({"read":{"const":false},"delete":owner}), None);
    let deleted = call(
        &data,
        &fixture,
        "mutation",
        "delete",
        json!("a1"),
        Some(alice()),
    )
    .unwrap();
    assert!(deleted.deletes.contains(&source_id("notes", "a1")));
    let skipped = call(
        &data,
        &fixture,
        "mutation",
        "delete",
        json!("b1"),
        Some(alice()),
    )
    .unwrap();
    assert!(skipped.deletes.is_empty());
}

#[test]
fn definer_brackets_act_with_the_applications_rights() {
    let (data, fixture) = setup();
    // Inside definer(true) … definer(false), nested, the caller's policy doesn't
    // apply: triggers see every row and field. Afterwards it does again.
    let both = call(
        &data,
        &fixture,
        "mutation",
        "definerScan",
        Value::Null,
        Some(alice()),
    )
    .unwrap()
    .value;
    assert_eq!(keys(&both["all"]), ["a1", "a2", "a3", "b1", "b2", "x"]);
    assert_eq!(both["all"][0]["value"]["secret"], "s1");
    assert_eq!(keys(&both["mine"]), ["a1", "a2", "a3"]);
    // Writes too: alice can't write bob's rows, but a trigger acting for the app can.
    let written = call(
        &data,
        &fixture,
        "mutation",
        "definerWrite",
        json!("audit"),
        Some(alice()),
    )
    .unwrap();
    assert!(written.puts.contains_key(&source_id("notes", "audit")));
    for (argument, message) in [
        (json!(false), "without a matching"),
        (json!(1), "true or false"),
    ] {
        let error = call(
            &data,
            &fixture,
            "mutation",
            "definerUnmatched",
            argument,
            Some(alice()),
        )
        .err()
        .expect("invalid");
        assert_eq!(error.code, "INVALID_VALUE");
        assert!(error.message.contains(message), "{}", error.message);
    }
    // Only mutations run triggers, so only they may act as the definer.
    let error = call(
        &data,
        &fixture,
        "query",
        "definerScan",
        Value::Null,
        Some(alice()),
    )
    .err()
    .expect("query");
    assert_eq!(error.code, "QUERY_WRITE_FORBIDDEN");
}

#[test]
fn derived_values_answer_only_the_callers_their_rule_allows() {
    // countFor computes with the application's rights, over every row; each
    // caller may read only its own count, by string or object arguments.
    let mine = json!({"any":[
        {"eq":[{"ref":["args"]},{"ref":["principal","subject"]}]},
        {"eq":[{"ref":["args","owner"]},{"ref":["principal","subject"]}]},
    ]});
    let fixture = fixture();
    let mut data = Records::default();
    install_with(
        &mut data,
        &fixture,
        policy(),
        None,
        Some(json!({"countFor": mine})),
    );
    let read = |args: Value, principal: Option<Value>| {
        call(&data, &fixture, "query", "readCount", args, principal)
    };
    assert_eq!(read(json!("alice"), Some(alice())).unwrap().value, 3);
    assert_eq!(
        read(json!({"owner":"alice"}), Some(alice())).unwrap().value,
        3
    );
    for (args, principal) in [
        (json!("bob"), Some(alice())),
        (json!({"owner":"bob"}), Some(alice())),
        (json!("alice"), Some(json!({"subject":"$anonymous"}))),
        (json!("alice"), Some(Value::Null)),
        (json!("alice"), Some(admin())),
    ] {
        let error = read(args, principal).err().expect("denied");
        assert_eq!(error.code, "ACCESS_DENIED");
        assert_eq!(error.message, "Access policy denies reading countFor");
    }
    // Without a caller nothing is checked, and values without a rule stay open.
    assert_eq!(read(json!("bob"), None).unwrap().value, 2);
    assert_eq!(
        call(
            &data,
            &fixture,
            "query",
            "readTotal",
            Value::Null,
            Some(alice())
        )
        .unwrap()
        .value,
        6
    );
    // Rules see principal, args and now only.
    for invalid in [
        json!({"exists":{"ref":["row","owner"]}}),
        json!({"exists":{"ref":["key"]}}),
        json!({"exists":{"ref":["args",""]}}),
    ] {
        let rule: crate::evaluator::rust_engine::Rule = serde_json::from_value(invalid).unwrap();
        assert!(rule.validate_derived().is_err());
    }
}
