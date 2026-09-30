//! Foreign keys: rows refer only to rows that exist, whichever mutation
//! writes or deletes them, and a deployment that declares one checks the rows
//! already there.
use super::*;
use crate::evaluator::rust_engine::indexes::IndexSpec;
use crate::evaluator::rust_engine::{KeyPart, ReferenceSpec};

fn reference() -> Value {
    json!({"kind":"collection","name":"ignored"})
}

/// Mutations over any collection: `[{collection, key, value?}]`, each a set,
/// or a delete without a value, in order.
fn fixture() -> Fixture {
    Fixture::new([
        (
            "write",
            (|args, host| {
                for op in args.as_array().unwrap() {
                    let target = json!({"kind":"collection","name":op["collection"]});
                    match op.get("value") {
                        Some(value) => host("set", json!([target, op["key"], value]))?,
                        None => host("delete", json!([target, op["key"]]))?,
                    };
                }
                Ok(Value::Null)
            }) as Callback,
        ),
        (
            "noop",
            (|_, host| host("get", json!([reference(), "x"]))) as Callback,
        ),
    ])
}

fn fields(collection: &str, target: &str, fields: &[&str], json: bool) -> ReferenceSpec {
    ReferenceSpec {
        collection: collection.into(),
        target: target.into(),
        fields: fields.iter().map(|field| field.to_string()).collect(),
        key: None,
        json,
    }
}

fn keyed(collection: &str, target: &str, key: KeyPart, json: bool) -> ReferenceSpec {
    ReferenceSpec {
        collection: collection.into(),
        target: target.into(),
        fields: Vec::new(),
        key: Some(key),
        json,
    }
}

/// sessions.org → orgs; events keyed [session, seq] → sessions by their
/// first component; heads keyed like sessions → sessions (one extends the
/// other); lines' shop and sku → products keyed by the JSON tuple of both.
fn schema() -> Schema {
    let references = vec![
        fields("sessions", "orgs", &["org"], false),
        keyed("events", "sessions", KeyPart::Leading(1), false),
        keyed("heads", "sessions", KeyPart::Whole(true), false),
        fields("lines", "products", &["shop", "sku"], true),
    ];
    Schema {
        indexes: references.iter().filter_map(ReferenceSpec::index).collect(),
        references,
        ..Schema::default()
    }
}

fn deploy_with(data: &Records, schema: Schema, writes: Value) -> EngineResult<Evaluation> {
    run_with_schema(
        data.clone(),
        json!({"requestId":"schema","writes":writes}),
        "deployment",
        None,
        &fixture(),
        Some(schema),
    )
}

fn setup() -> Records {
    let mut data = Records::default();
    let result = deploy_with(
        &data,
        schema(),
        json!([
            {"collection":"orgs","key":"o1","value":{"name":"one"}},
            {"collection":"orgs","key":"o2","value":{"name":"two"}},
            {"collection":"sessions","key":"s1","value":{"org":"o1"}},
            {"collection":"sessions","key":"s2","value":{"org":"o1"}},
            {"collection":"events","key":"[\"s1\",1]","value":{"text":"hi"}},
            {"collection":"heads","key":"s1","value":{"n":1}},
            {"collection":"products","key":"[\"shop\",\"sku\"]","value":{}},
            {"collection":"lines","key":"l1","value":{"shop":"shop","sku":"sku"}},
        ]),
    )
    .unwrap();
    apply(&mut data, result);
    data
}

fn write(data: &Records, ops: Value) -> EngineResult<Evaluation> {
    run(
        data.clone(),
        json!({"name":"write","args":ops}),
        "mutation",
        None,
        &fixture(),
    )
}

fn violation(result: EngineResult<Evaluation>) -> EngineError {
    let error = result.err().expect("the mutation fails");
    assert_eq!(error.code, "FOREIGN_KEY_VIOLATION", "{error}");
    error
}

#[test]
fn written_rows_refer_only_to_rows_that_exist_at_the_end_of_the_mutation() {
    let data = setup();
    let error = violation(write(
        &data,
        json!([{"collection":"sessions","key":"s3","value":{"org":"o9"}}]),
    ));
    assert_eq!(
        error.message,
        "sessions row \"s3\" refers to orgs row \"o9\", which does not exist"
    );
    assert_eq!(
        error.details,
        Some(
            json!({"collection":"sessions","key":"s3","references":{"fields":["org"]},"target":"orgs","targetKey":"o9"})
        )
    );
    // The check is the mutation's end: the row may come before what it refers to.
    write(
        &data,
        json!([
            {"collection":"events","key":"[\"s3\",1]","value":{}},
            {"collection":"heads","key":"s3","value":{}},
            {"collection":"sessions","key":"s3","value":{"org":"o3"}},
            {"collection":"orgs","key":"o3","value":{}},
        ]),
    )
    .unwrap();
    // A missing or null field refers to nothing.
    write(
        &data,
        json!([
            {"collection":"sessions","key":"s4","value":{}},
            {"collection":"sessions","key":"s5","value":{"org":null}},
            {"collection":"lines","key":"l2","value":{"shop":"shop"}},
            {"collection":"lines","key":"l3","value":{"shop":null,"sku":"sku"}},
        ]),
    )
    .unwrap();
    let error = violation(write(
        &data,
        json!([{"collection":"events","key":"[\"s9\",4]","value":{}}]),
    ));
    assert_eq!(
        error.details.unwrap()["references"],
        json!({"key": 1}),
        "{}",
        error.message
    );
    violation(write(
        &data,
        json!([{"collection":"heads","key":"s9","value":{}}]),
    ));
    // A JSON tuple key, made of several fields.
    write(
        &data,
        json!([{"collection":"lines","key":"l4","value":{"shop":"shop","sku":"sku","n":2}}]),
    )
    .unwrap();
    let error = violation(write(
        &data,
        json!([{"collection":"lines","key":"l4","value":{"shop":"shop","sku":"other"}}]),
    ));
    assert_eq!(
        error.details.unwrap()["targetKey"],
        json!("[\"shop\",\"other\"]")
    );
    // A plain key is a string: nothing else can name one.
    let error = violation(write(
        &data,
        json!([{"collection":"sessions","key":"s6","value":{"org":7}}]),
    ));
    assert_eq!(
        error.message,
        "sessions row \"s6\" refers to a key no orgs row can have"
    );
    // A row whose reference doesn't change needs nothing more; one that
    // points elsewhere must point at a row.
    write(
        &data,
        json!([{"collection":"sessions","key":"s1","value":{"org":"o1","title":"t"}}]),
    )
    .unwrap();
    write(
        &data,
        json!([{"collection":"sessions","key":"s1","value":{"org":"o2"}}]),
    )
    .unwrap();
    violation(write(
        &data,
        json!([{"collection":"sessions","key":"s1","value":{"org":"o9"}}]),
    ));
}

#[test]
fn a_row_still_referred_to_cannot_go() {
    let data = setup();
    let error = violation(write(&data, json!([{"collection":"orgs","key":"o1"}])));
    assert_eq!(
        error.message,
        "orgs row \"o1\" is deleted while sessions row \"s1\" still refers to it"
    );
    assert_eq!(
        error.details,
        Some(
            json!({"collection":"sessions","key":"s1","references":{"fields":["org"]},"target":"orgs","targetKey":"o1","deleted":true})
        )
    );
    // Nothing refers to o2.
    write(&data, json!([{"collection":"orgs","key":"o2"}])).unwrap();
    // A session with events, or with a head.
    let error = violation(write(
        &data,
        json!([
            {"collection":"heads","key":"s1"},
            {"collection":"sessions","key":"s1"},
        ]),
    ));
    assert_eq!(error.details.unwrap()["key"], json!("[\"s1\",1]"));
    let error = violation(write(
        &data,
        json!([
            {"collection":"events","key":"[\"s1\",1]"},
            {"collection":"sessions","key":"s1"},
        ]),
    ));
    assert_eq!(error.details.unwrap()["collection"], json!("heads"));
    // Gone with everything that referred to it, in any order.
    write(
        &data,
        json!([
            {"collection":"sessions","key":"s1"},
            {"collection":"sessions","key":"s2"},
            {"collection":"heads","key":"s1"},
            {"collection":"events","key":"[\"s1\",1]"},
            {"collection":"orgs","key":"o1"},
        ]),
    )
    .unwrap();
    // Or pointed elsewhere.
    write(
        &data,
        json!([
            {"collection":"sessions","key":"s1","value":{"org":"o2"}},
            {"collection":"sessions","key":"s2","value":{"org":null}},
            {"collection":"orgs","key":"o1"},
        ]),
    )
    .unwrap();
    // Deleted and made again, it is there at the end.
    write(
        &data,
        json!([
            {"collection":"sessions","key":"s1"},
            {"collection":"sessions","key":"s1","value":{"org":"o1"}},
        ]),
    )
    .unwrap();
    let error = violation(write(
        &data,
        json!([{"collection":"products","key":"[\"shop\",\"sku\"]"}]),
    ));
    assert_eq!(error.details.unwrap()["key"], json!("l1"));
    // A session whose key starts like s1's: only [s1, …] keys refer to s1.
    let mut data = data;
    let result = write(
        &data,
        json!([
            {"collection":"sessions","key":"s10","value":{"org":"o2"}},
            {"collection":"events","key":"[\"s10\",1]","value":{}},
            {"collection":"sessions","key":"s","value":{"org":"o2"}},
            {"collection":"events","key":"[\"s\",1]","value":{}},
        ]),
    )
    .unwrap();
    apply(&mut data, result);
    write(
        &data,
        json!([
            {"collection":"heads","key":"s1"},
            {"collection":"events","key":"[\"s1\",1]"},
            {"collection":"sessions","key":"s1"},
        ]),
    )
    .unwrap();
}

#[test]
fn references_in_keys_find_their_rows_whatever_the_keys_hold() {
    // Keys escape quotes, backslashes and control characters in source IDs,
    // and order by UTF-16 units: a prefix finds exactly its own rows.
    let schema = Schema {
        references: vec![keyed("events", "sessions", KeyPart::Leading(1), false)],
        ..Schema::default()
    };
    let odd = [
        "a\"b", "a\\", "a\\b", "a\nb", "😀", "\u{e000}", "a", "a,", "a]",
    ];
    let mut writes = Vec::new();
    for (index, session) in odd.iter().enumerate() {
        writes.push(json!({"collection":"sessions","key":session,"value":{}}));
        let key = canonical_json(&json!([session, index]));
        writes.push(json!({"collection":"events","key":key,"value":{}}));
    }
    let mut data = Records::default();
    let result = deploy_with(&data, schema, Value::Array(writes)).unwrap();
    apply(&mut data, result);
    for (index, session) in odd.iter().enumerate() {
        let error = violation(write(
            &data,
            json!([{"collection":"sessions","key":session}]),
        ));
        assert_eq!(
            error.details.unwrap()["key"],
            json!(canonical_json(&json!([session, index]))),
            "{session:?}"
        );
        write(
            &data,
            json!([
                {"collection":"sessions","key":session},
                {"collection":"events","key":canonical_json(&json!([session, index]))},
            ]),
        )
        .unwrap();
    }
    // A key of exactly the prefix's components refers too.
    let mut data = data;
    let result = write(
        &data,
        json!([{"collection":"events","key":"[\"a\"]","value":{}}]),
    )
    .unwrap();
    apply(&mut data, result);
    let error = violation(write(
        &data,
        json!([
            {"collection":"events","key":"[\"a\",6]"},
            {"collection":"sessions","key":"a"},
        ]),
    ));
    assert_eq!(error.details.unwrap()["key"], json!("[\"a\"]"));
}

#[test]
fn a_deployment_checks_the_rows_already_there_for_references_it_adds() {
    let fixture = fixture();
    let mut data = Records::default();
    let result = run(
        data.clone(),
        json!({"requestId":"rows","writes":[
            {"collection":"orgs","key":"o1","value":{}},
            {"collection":"sessions","key":"s1","value":{"org":"o1"}},
            {"collection":"sessions","key":"s2","value":{"org":"gone"}},
        ]}),
        "deployment",
        None,
        &fixture,
    )
    .unwrap();
    apply(&mut data, result);
    let error = violation(deploy_with(&data, schema(), json!([])));
    assert_eq!(
        error.message,
        "sessions row \"s2\" refers to orgs row \"gone\", which does not exist"
    );
    // The same deployment's writes may repair them.
    let result = deploy_with(
        &data,
        schema(),
        json!([{"collection":"sessions","key":"s2","value":{"org":null}}]),
    )
    .unwrap();
    apply(&mut data, result);
    assert!(data.get("schema").unwrap()["references"].is_array());
    write(&data, json!([{"collection":"orgs","key":"o1"}])).unwrap_err();
    // Only references it adds: those it kept held all along.
    data.insert(source_id("sessions", "s3"), json!({"org":"gone"}));
    let mut next = schema();
    next.references
        .push(fields("sessions", "people", &["owner"], false));
    next.indexes.push(IndexSpec {
        collection: "sessions".into(),
        fields: vec!["owner".into()],
    });
    let result = deploy_with(&data, next.clone(), json!([])).unwrap();
    apply(&mut data, result);
    data.insert(source_id("sessions", "s4"), json!({"owner":"nobody"}));
    // Dropping a reference checks nothing.
    let result = deploy_with(&data, schema(), json!([])).unwrap();
    apply(&mut data, result);
    let error = violation(deploy_with(&data, next, json!([])));
    assert_eq!(error.details.unwrap()["key"], json!("s4"));
}

#[test]
fn stored_schemas_and_manifests_reject_malformed_references() {
    let malformed = [
        fields("sessions", "", &["org"], false),
        fields("", "orgs", &["org"], false),
        fields("sessions", "orgs", &[], false),
        fields("sessions", "orgs", &["a", "a"], true),
        fields("sessions", "orgs", &[""], false),
        fields("sessions", "orgs", &["a", "b"], false),
        keyed("events", "sessions", KeyPart::Leading(0), true),
        keyed("events", "sessions", KeyPart::Leading(2), false),
        keyed("events", "sessions", KeyPart::Whole(false), false),
        ReferenceSpec {
            key: Some(KeyPart::Leading(1)),
            ..fields("events", "sessions", &["session"], false)
        },
    ];
    for reference in malformed {
        let malformed = Schema {
            references: vec![reference.clone()],
            ..Schema::default()
        };
        let error = Schema::load(Some(&serde_json::to_value(&malformed).unwrap())).unwrap_err();
        assert!(
            error.message.starts_with("Malformed reference"),
            "{reference:?}: {error}"
        );
    }
    // Fields need their index.
    let unindexed = Schema {
        references: vec![fields("sessions", "orgs", &["org"], false)],
        ..Schema::default()
    };
    let error = Schema::load(Some(&serde_json::to_value(&unindexed).unwrap())).unwrap_err();
    assert_eq!(
        error.message,
        "A reference's fields need their index declared"
    );
    // As a deployment stores them: validated, so in order.
    let stored = serde_json::to_value(schema().validate().unwrap()).unwrap();
    assert_eq!(
        stored["references"],
        json!([
            {"collection":"events","target":"sessions","key":1},
            {"collection":"heads","target":"sessions","key":true},
            {"collection":"lines","target":"products","fields":["shop","sku"],"json":true},
            {"collection":"sessions","target":"orgs","fields":["org"]},
        ])
    );
    assert_eq!(
        Schema::load(Some(&stored)).unwrap(),
        schema().validate().unwrap()
    );
}

fn optimistic(data: &Records, ops: Value) -> MutationCertificate {
    run(
        data.clone(),
        json!({"name":"write","args":ops,"$speculate":true}),
        "mutation",
        Some(2000),
        &fixture(),
    )
    .unwrap()
    .mutation_certificate
    .unwrap()
}

#[test]
fn optimistic_mutations_conflict_with_writes_that_change_what_their_checks_found() {
    let mut data = setup();
    data.insert("clock".into(), json!(1000));
    let changed = |ops: Value| {
        let mut next = data.clone();
        apply(&mut next, write(&data, ops).unwrap());
        next
    };
    // A row written for s2 conflicts with s2 going, not with other writes.
    let event = optimistic(
        &data,
        json!([{"collection":"events","key":"[\"s2\",1]","value":{}}]),
    );
    assert!(event.valid(&changed(json!([{"collection":"orgs","key":"o2"}]))));
    assert!(!event.valid(&changed(json!([{"collection":"sessions","key":"s2"}]))));
    // Deleting s2 conflicts with an event, head or new session row for it.
    let gone = optimistic(&data, json!([{"collection":"sessions","key":"s2"}]));
    assert!(gone.valid(&changed(
        json!([{"collection":"events","key":"[\"s1\",2]","value":{}}])
    )));
    assert!(!gone.valid(&changed(
        json!([{"collection":"events","key":"[\"s2\",1]","value":{}}])
    )));
    assert!(!gone.valid(&changed(
        json!([{"collection":"heads","key":"s2","value":{}}])
    )));
    // Deleting o2 conflicts with a session that comes to refer to it.
    let org = optimistic(&data, json!([{"collection":"orgs","key":"o2"}]));
    assert!(org.valid(&changed(
        json!([{"collection":"sessions","key":"s9","value":{"org":"o1"}}])
    )));
    assert!(!org.valid(&changed(
        json!([{"collection":"sessions","key":"s9","value":{"org":"o2"}}])
    )));
    assert!(!org.valid(&changed(
        json!([{"collection":"sessions","key":"s1","value":{"org":"o2"}}])
    )));
    // A row that keeps its reference read nothing of what it refers to.
    let kept = optimistic(
        &data,
        json!([{"collection":"sessions","key":"s1","value":{"org":"o1","n":1}}]),
    );
    assert!(
        !kept.observed().iter().any(|id| id.contains("orgs")),
        "{:?}",
        kept.observed()
    );
}

#[test]
fn queries_and_unrelated_writes_are_unchanged() {
    let data = setup();
    run(
        data.clone(),
        json!({"name":"noop","args":null}),
        "query",
        None,
        &fixture(),
    )
    .unwrap();
    write(
        &data,
        json!([{"collection":"notes","key":"n","value":{"org":"nowhere"}}]),
    )
    .unwrap();
}
