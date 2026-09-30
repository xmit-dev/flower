use super::*;
use crate::evaluator::{DependencyCertificate, Observation, touched, touches_everything};

fn query(
    data: &Records,
    name: &str,
    args: Value,
    fixture: &Fixture,
) -> (Value, DependencyCertificate) {
    let result = run(
        data.clone(),
        json!({"name":name,"args":args}),
        "query",
        None,
        fixture,
    )
    .unwrap();
    assert!(result.query_cacheable);
    (result.value, result.query_certificate.unwrap())
}
fn fixture() -> Fixture {
    Fixture::new([
        (
            "point",
            (|args, host| get(host, "collection", "items", args.clone())) as Callback,
        ),
        (
            "scan",
            (|_, host| host("scan", json!([{"kind":"collection","name":"items"}]))) as Callback,
        ),
        (
            "indexed",
            (|args, host| {
                host(
                    "query",
                    json!([{"kind":"query","collection":"items","fields":["group"],"value":args}]),
                )
            }) as Callback,
        ),
        (
            "range",
            (|_, host| {
                host(
                    "range",
                    json!([{"kind":"range","collection":"items","fields":["group"],"options":{"prefix":["a"],"limit":1}}]),
                )
            }) as Callback,
        ),
        (
            "leaf",
            (|_, host| get(host, "collection", "items", json!("first"))) as Callback,
        ),
        (
            "derived",
            (|_, host| get(host, "derived", "leaf", Value::Null)) as Callback,
        ),
        ("clock", (|_, host| host("now", json!([]))) as Callback),
    ])
}
#[test]
fn certificates_cover_points_absence_code_and_schema_without_retaining_payloads() {
    let fixture = fixture();
    let mut data = Records::from([(source_id("items", "first"), json!(42))]);
    let (_, point) = query(&data, "point", json!("first"), &fixture);
    let (_, missing) = query(&data, "point", json!("missing"), &fixture);
    let original = data.clone();
    data.insert(source_id("unrelated", "x"), json!(1));
    assert!(point.valid(&data));
    assert!(missing.valid(&data));
    data.insert(source_id("items", "missing"), json!(7));
    assert!(point.valid(&data));
    assert!(!missing.valid(&data));
    data.insert(source_id("items", "first"), json!(43));
    assert!(!point.valid(&data));
    assert!(point.valid(&original));
    for field in [
        "bundle",
        "schema",
        "keyDeclarations",
        "managedKeys",
        "authorizationMethod",
    ] {
        let mut changed = original.clone();
        changed.insert(field.into(), json!({}));
        assert!(!point.valid(&changed), "{field}");
    }
    let mut removed = original.clone();
    removed.remove(&source_id("items", "first"));
    assert!(!point.valid(&removed));
    let record = original.get_shared(&source_id("items", "first")).unwrap();
    assert_eq!(
        Arc::strong_count(record),
        1,
        "certificate must retain only Weak<Value>"
    );
}

#[test]
fn warm_queries_skip_build_progress_but_follow_physical_graph_pointer_cutover_and_source() {
    let mut fixture = fixture();
    fixture.callbacks.insert("write", |args, host| {
        set(host, "items", "first", args.clone())
    });
    let mut data = Records::from([(source_id("items", "first"), json!(7))]);
    deploy(
        &mut data,
        json!({"materialize":[{"name":"leaf"}]}),
        &fixture,
    );
    let (_, point) = query(&data, "point", json!("first"), &fixture);
    let (_, derived) = query(&data, "derived", Value::Null, &fixture);
    let mutation = optimistic(&data, "point", json!("first"), &fixture)
        .mutation_certificate
        .unwrap();
    let original = data.clone();
    let generation = "a".repeat(64);
    for phase in [
        "backfill",
        "rebuilding",
        "ready",
        "failed",
        "canceled",
        "collected",
    ] {
        data.insert(
            crate::evaluator::staging::JOB.into(),
            json!({
                "generation":generation,"phase":phase,"graphCursor":"root:[\"leaf\",null]",
                "rebuiltRoots":1,"scannedRows":12
            }),
        );
        data.insert(
            crate::evaluator::staging::INDEXES.into(),
            json!({
                "indexes":[{"collection":"items","fields":["future"]}],"aggregates":{}
            }),
        );
        // Backfilled cells and root pages belong to an unpublished graph.
        for (key, value) in original
            .iter()
            .filter(|(key, _)| key.starts_with("root:") || key.starts_with("cell:"))
        {
            data.insert(format!("graph:{generation}:{key}"), value.clone());
        }
        assert!(point.valid(&data), "point query invalidated by {phase}");
        assert!(
            derived.valid(&data),
            "materialized query invalidated by {phase}"
        );
        assert!(
            !mutation.valid(&data),
            "mutation must observe deployment lifecycle"
        );
    }
    for field in [
        crate::evaluator::staging::JOB,
        crate::evaluator::staging::INDEXES,
    ] {
        let mut changed = original.clone();
        changed.insert(field.into(), json!({}));
        assert!(!mutation.valid(&changed), "mutation must track {field}");
    }
    let mut cutover = data.clone();
    cutover.insert("reactive:active".into(), json!(generation));
    assert!(!point.valid(&cutover));
    assert!(!derived.valid(&cutover));
    let write = run(
        data.clone(),
        json!({"name":"write","args":8}),
        "mutation",
        None,
        &fixture,
    )
    .unwrap();
    apply(&mut data, write);
    assert!(!point.valid(&data));
    assert!(!derived.valid(&data));
}
#[test]
fn certificates_track_collection_and_index_phantoms_but_skip_unrelated_buckets() {
    let fixture = fixture();
    let mut data = Records::new();
    let schema = Schema {
        policies: Default::default(),
        derived_access: Default::default(),
        aggregate_versions: Default::default(),
        references: Vec::new(),
        indexes: vec![indexes::IndexSpec {
            collection: "items".into(),
            fields: vec!["group".into()],
        }],
        aggregates: BTreeMap::new(),
    };
    let result = run_with_schema(
        data.clone(),
        json!({"requestId":"schema"}),
        "deployment",
        None,
        &fixture,
        Some(schema),
    )
    .unwrap();
    apply(&mut data, result);
    let (_, scan) = query(&data, "scan", Value::Null, &fixture);
    let (_, empty) = query(&data, "indexed", json!("a"), &fixture);
    let (_, range) = query(&data, "range", Value::Null, &fixture);
    deploy(
        &mut data,
        json!({"writes":[{"collection":"items","key":"other","value":{"group":"b"}}]}),
        &fixture,
    );
    assert!(!scan.valid(&data));
    assert!(empty.valid(&data));
    // The empty page depends on its whole prefix, and only on it.
    assert!(range.valid(&data));
    deploy(
        &mut data,
        json!({"writes":[{"collection":"items","key":"first","value":{"group":"a","value":1}}]}),
        &fixture,
    );
    assert!(!empty.valid(&data));
    assert!(!range.valid(&data));
    let (_, present) = query(&data, "indexed", json!("a"), &fixture);
    let (_, range) = query(&data, "range", Value::Null, &fixture);
    deploy(
        &mut data,
        json!({"writes":[{"collection":"items","key":"other","value":{"group":"b","value":2}}]}),
        &fixture,
    );
    assert!(present.valid(&data));
    assert!(range.valid(&data));
    deploy(
        &mut data,
        json!({"writes":[{"collection":"items","key":"first","value":{"group":"a","value":2}}]}),
        &fixture,
    );
    assert!(!present.valid(&data));
    assert!(!range.valid(&data));
    // Moving a row between buckets changes index entries, not the collection.
    let (_, present) = query(&data, "indexed", json!("a"), &fixture);
    deploy(
        &mut data,
        json!({"writes":[{"collection":"items","key":"other","value":{"group":"a","value":2}}]}),
        &fixture,
    );
    assert!(!present.valid(&data), "a row moved into the bucket");
    let (_, present) = query(&data, "indexed", json!("a"), &fixture);
    let (_, other_bucket) = query(&data, "indexed", json!("c"), &fixture);
    deploy(
        &mut data,
        json!({"writes":[{"collection":"items","key":"other","value":{"group":"b","value":2}}]}),
        &fixture,
    );
    assert!(!present.valid(&data), "a row moved out of the bucket");
    assert!(
        other_bucket.valid(&data),
        "an unrelated bucket is unchanged"
    );
    let (_, present) = query(&data, "indexed", json!("a"), &fixture);
    deploy(
        &mut data,
        json!({"writes":[{"collection":"items","key":"first","delete":true}]}),
        &fixture,
    );
    assert!(!present.valid(&data));
    assert!(
        data.generation(&indexes::bucket_id("items", &["group".into()], &json!("a")))
            .is_none(),
        "empty buckets retain no generation tombstone"
    );
}
/// Whether a watch that knows only which stored keys an apply wrote wakes
/// the reader holding `certificate`.
fn wakes(certificate: &DependencyCertificate, written: &[String]) -> bool {
    written.iter().any(|key| {
        let mut ids = Vec::new();
        touched(key, |id| ids.push(id.to_owned()));
        touches_everything(key)
            || certificate
                .observations()
                .any(|observation| match observation {
                    Observation::Key(id) => ids.iter().any(|touched| touched == id),
                    Observation::Range {
                        marker,
                        lower,
                        upper,
                    } => {
                        ids.iter().any(|touched| touched == marker)
                            && lower <= key.as_str()
                            && key.as_str() < upper
                    }
                })
    })
}

#[test]
fn writes_that_invalidate_a_certificate_touch_what_it_observed() {
    let fixture = fixture();
    let mut data = Records::from([(source_id("items", "first"), json!({"group":"a"}))]);
    let schema = Schema {
        policies: Default::default(),
        derived_access: Default::default(),
        aggregate_versions: Default::default(),
        references: Vec::new(),
        indexes: vec![indexes::IndexSpec {
            collection: "items".into(),
            fields: vec!["group".into()],
        }],
        aggregates: BTreeMap::new(),
    };
    let result = run_with_schema(
        data.clone(),
        json!({"requestId":"schema"}),
        "deployment",
        None,
        &fixture,
        Some(schema),
    )
    .unwrap();
    apply(&mut data, result);
    deploy(
        &mut data,
        json!({"materialize":[{"name":"leaf"}]}),
        &fixture,
    );
    let reads = [
        ("point", json!("first")),
        ("point", json!("missing")),
        ("scan", Value::Null),
        ("indexed", json!("a")),
        ("indexed", json!("c")),
        ("range", Value::Null),
        ("derived", Value::Null),
    ];
    let mut certificates: Vec<_> = reads
        .iter()
        .map(|(name, args)| query(&data, name, args.clone(), &fixture).1)
        .collect();
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut next = |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    let (mut invalidated, mut spared) = (0, 0);
    for _ in 0..400 {
        let count = 1 + next(3);
        let writes: Vec<Value> = (0..count)
            .map(|_| {
                let collection = if next(4) == 0 { "other" } else { "items" };
                let key = ["first", "second", "missing", "x"][next(4) as usize];
                if next(5) == 0 {
                    json!({"collection":collection,"key":key,"delete":true})
                } else {
                    let group = ["a", "b", "c"][next(3) as usize];
                    json!({"collection":collection,"key":key,"value":{"group":group,"n":next(3)}})
                }
            })
            .collect();
        let result = deploy(&mut data, json!({"writes":writes}), &fixture);
        let written: Vec<String> = result
            .puts
            .keys()
            .cloned()
            .chain(result.deletes.iter().cloned())
            .collect();
        for (certificate, (name, args)) in certificates.iter_mut().zip(&reads) {
            let woken = wakes(certificate, &written);
            if !certificate.valid(&data) {
                assert!(woken, "{name}({args}) went stale, unwoken by {written:?}");
                invalidated += 1;
                *certificate = query(&data, name, args.clone(), &fixture).1;
            } else if !woken {
                spared += 1;
            }
        }
    }
    // Waking every reader for every write would pass the assertion above.
    assert!(
        invalidated > 100 && spared > 400,
        "{invalidated} invalidated, {spared} spared"
    );
}

#[test]
fn certificates_flatten_ephemeral_derives_and_guard_materialized_outcomes() {
    let fixture = fixture();
    let mut data = Records::from([(source_id("items", "first"), json!(7))]);
    let (_, ephemeral) = query(&data, "derived", Value::Null, &fixture);
    data.insert(source_id("items", "second"), json!(5));
    assert!(ephemeral.valid(&data));
    data.insert(source_id("items", "first"), json!(8));
    assert!(!ephemeral.valid(&data));
    deploy(
        &mut data,
        json!({"materialize":[{"name":"leaf"}]}),
        &fixture,
    );
    let (_, materialized) = query(&data, "derived", Value::Null, &fixture);
    deploy(
        &mut data,
        json!({"writes":[{"collection":"items","key":"second","value":6}]}),
        &fixture,
    );
    assert!(materialized.valid(&data));
    deploy(
        &mut data,
        json!({"writes":[{"collection":"items","key":"first","value":9}]}),
        &fixture,
    );
    assert!(!materialized.valid(&data));
    let clock = run(data, json!({"name":"clock"}), "query", None, &fixture).unwrap();
    assert!(!clock.query_cacheable);
    assert!(clock.query_certificate.is_none());
}
#[test]
fn markers_handle_delimiters_and_quoted_unicode_without_cross_bucket_collisions() {
    let collection = "colon:quote\"🌺";
    let fields = vec!["x:[]".into()];
    let spec = indexes::IndexSpec {
        collection: collection.into(),
        fields: fields.clone(),
    };
    let prefix = format!(
        "index-entry:{}:",
        canonical_json(&json!([collection, fields]))
    );
    let mut data = Records::new();
    let values = [
        json!(null),
        json!(false),
        json!(12),
        json!(":quote\""),
        json!([":",{"a:":1}]),
    ];
    let equality = format!(
        "index-entries:{}",
        canonical_json(&json!([collection, fields]))
    );
    let range = super::super::ranges::dependency(collection, &fields);
    // Scalar values have ordered entries only; the others equality entries.
    let entries: Vec<(String, String)> = values
        .iter()
        .map(
            |value| match super::super::ranges::entry(&spec, "row", &json!({"x:[]": value})) {
                Some(ordered) => (ordered, range.clone()),
                None => (
                    format!("{prefix}{}:\"row\"", canonical_json(value)),
                    equality.clone(),
                ),
            },
        )
        .collect();
    assert_eq!(
        entries
            .iter()
            .filter(|(_, marker)| *marker == equality)
            .count(),
        1
    );
    for (value, (id, marker)) in values.iter().zip(&entries) {
        // A bucket is stamped by the entries in its window, which must
        // contain exactly that bucket's entries.
        let bucket = indexes::bucket_id(collection, &fields, value);
        let (window_marker, lower, upper) =
            super::super::dependencies::bucket_window(&bucket).unwrap();
        assert_eq!(&window_marker, marker);
        for (other, _) in &entries {
            let inside = lower.as_str() <= other.as_str() && other.as_str() < upper.as_str();
            assert_eq!(inside, other == id, "{bucket}: {other}");
        }
        data.insert(id.clone(), json!(0));
        let inserted = data.generation(marker);
        assert!(inserted.is_some(), "{id}");
        data.remove(id);
        assert_ne!(data.generation(marker), inserted);
    }
    let ordered = super::super::ranges::entry(&spec, "row", &json!({"x:[]":1})).unwrap();
    data.insert(ordered.clone(), json!("row"));
    assert!(
        data.generation(&super::super::ranges::dependency(collection, &fields))
            .is_some()
    );
    data.insert(source_id(collection, "row"), json!(1));
    assert!(data.generation(&collection_id(collection)).is_some());
}

fn optimistic(data: &Records, name: &str, args: Value, fixture: &Fixture) -> Evaluation {
    run(
        data.clone(),
        json!({"name":name,"args":args,"$speculate":true}),
        "mutation",
        Some(2000),
        fixture,
    )
    .unwrap()
}
#[test]
fn mutation_certificates_cover_write_conflicts_negative_reads_phantoms_and_time() {
    let fixture = Fixture::new([
        (
            "blind",
            (|args, host| {
                set(
                    host,
                    "items",
                    args["key"].as_str().unwrap(),
                    args["value"].clone(),
                )
            }) as Callback,
        ),
        (
            "negative",
            (|_, host| {
                let value = get(host, "collection", "items", json!("missing"))?;
                set(host, "out", "value", value)
            }) as Callback,
        ),
        (
            "scan",
            (|_, host| {
                let rows = host("scan", json!([{"kind":"collection","name":"items"}]))?;
                set(host, "out", "value", rows)
            }) as Callback,
        ),
        (
            "range",
            (|_, host| {
                let page = host(
                    "range",
                    json!([{"kind":"range","collection":"items","fields":["score"],"options":{"gte":0,"limit":1}}]),
                )?;
                set(host, "out", "value", page)
            }) as Callback,
        ),
    ]);
    let base = Records::from([
        ("clock".into(), json!(1000)),
        (source_id("items", "a"), json!(1)),
    ]);
    let write = optimistic(&base, "blind", json!({"key":"a","value":2}), &fixture);
    let certificate = write.mutation_certificate.unwrap();
    let mut next = base.clone();
    next.insert(source_id("items", "b"), json!(7));
    next.insert("clock".into(), json!(2000));
    assert!(certificate.valid(&next));
    next.insert(source_id("items", "a"), json!(9));
    assert!(!certificate.valid(&next));
    let missing = optimistic(&base, "negative", Value::Null, &fixture)
        .mutation_certificate
        .unwrap();
    let scan = optimistic(&base, "scan", Value::Null, &fixture)
        .mutation_certificate
        .unwrap();
    let range = optimistic(&base, "range", Value::Null, &fixture)
        .mutation_certificate
        .unwrap();
    let mut next = base.clone();
    next.insert(source_id("items", "missing"), json!({"score":1}));
    assert!(!missing.valid(&next));
    assert!(!scan.valid(&next));
    assert!(!range.valid(&next));
    for field in [
        "bundle",
        "schema",
        "managedKeys",
        "keyDeclarations",
        "authorizationMethod",
    ] {
        let mut next = base.clone();
        next.insert(field.into(), Value::Null);
        assert!(!certificate.valid(&next), "{field}");
    }
    let mut next = base.clone();
    next.insert("clock".into(), json!(2001));
    assert!(!certificate.valid(&next));
}

#[test]
fn optimistic_mutations_match_serial_across_independent_hot_and_dynamic_graphs() {
    let fixture = Fixture::new([
        (
            "update",
            (|args, host| {
                let key = args["key"].as_str().unwrap();
                let old = get(host, "collection", "items", json!(key))?
                    .as_i64()
                    .unwrap_or(0);
                set(host, "items", key, json!(old + 1))?;
                Ok(json!(old + 1))
            }) as Callback,
        ),
        (
            "leaf",
            (|args, host| get(host, "collection", "items", args.clone())) as Callback,
        ),
        (
            "branch",
            (|_, host| {
                let key = get(host, "collection", "control", json!("key"))?;
                get(host, "collection", "items", key)
            }) as Callback,
        ),
    ]);
    let mut initial = Records::new();
    deploy(
        &mut initial,
        json!({"writes":[{"collection":"control","key":"key","value":"a"},{"collection":"items","key":"a","value":0},{"collection":"items","key":"b","value":0}],"materialize":[{"name":"leaf","args":"a"},{"name":"leaf","args":"b"},{"name":"branch"}]}),
        &fixture,
    );
    let candidate = optimistic(&initial, "update", json!({"key":"a"}), &fixture);
    let mut branch = initial.clone();
    deploy(
        &mut branch,
        json!({"writes":[{"collection":"control","key":"key","value":"b"}]}),
        &fixture,
    );
    assert!(
        !candidate
            .mutation_certificate
            .as_ref()
            .unwrap()
            .valid(&branch),
        "source-only edge changes must invalidate the graph shape"
    );
    let mut actual = initial.clone();
    let mut serial = initial;
    let mut accepted = 0;
    let mut conflicts = 0;
    let mut random = 0x12345678u64;
    for _ in 0..24 {
        let mut wave = Vec::new();
        for _ in 0..5 {
            random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
            let key = if random.is_multiple_of(4) {
                "a"
            } else if random % 4 == 1 {
                "b"
            } else {
                "c"
            };
            let args = json!({"key":key});
            wave.push((args.clone(), optimistic(&actual, "update", args, &fixture)));
        }
        for (args, candidate) in wave {
            let expected = run(
                serial.clone(),
                json!({"name":"update","args":args}),
                "mutation",
                Some(2000),
                &fixture,
            )
            .unwrap();
            let mut selected = if candidate
                .mutation_certificate
                .as_ref()
                .unwrap()
                .valid(&actual)
            {
                accepted += 1;
                candidate
            } else {
                conflicts += 1;
                optimistic(&actual, "update", args, &fixture)
            };
            // The ordered writer omits a redundant fixed wave-clock put.
            if selected
                .puts
                .get("clock")
                .is_some_and(|clock| actual.get("clock") == Some(clock))
            {
                selected.puts.remove("clock");
            }
            assert_eq!(selected.value, expected.value);
            assert_eq!(selected.puts, expected.puts);
            assert_eq!(selected.deletes, expected.deletes);
            apply(&mut actual, selected);
            apply(&mut serial, expected);
            assert_eq!(actual, serial);
        }
    }
    assert!(
        accepted > 24 && conflicts > 0,
        "must exercise both disjoint reuse and hot conflicts"
    );
}

#[test]
fn history_readers_survive_retention_accounting_but_not_a_new_incarnation() {
    let fixture = Fixture::new([(
        "history",
        (|_, host| host("history", json!([]))) as Callback,
    )]);
    let retention = |incarnation: &str, receipts: u64| json!({"database":"d".repeat(32),"incarnation":incarnation,"receipt_count":receipts});
    let mut data = Records::from([(
        crate::consensus::retention::KEY.to_owned(),
        retention(&"a".repeat(32), 0),
    )]);
    let read = optimistic(&data, "history", Value::Null, &fixture);
    assert_eq!(read.value["incarnation"], "a".repeat(32));
    let certificate = read.mutation_certificate.unwrap();
    // Every receipted commit rewrites the accounting.
    data.insert(
        crate::consensus::retention::KEY.into(),
        retention(&"a".repeat(32), 1),
    );
    assert!(certificate.valid(&data), "the history it read is unchanged");
    data.insert(
        crate::consensus::retention::KEY.into(),
        retention(&"b".repeat(32), 1),
    );
    assert!(!certificate.valid(&data), "a new incarnation changes it");
}

#[test]
fn a_window_stamp_holds_what_the_budget_leaves_room_for_then_falls_back_to_its_marker() {
    // A scan far into its bucket depends on every entry before it (offset), one row.
    let fixture = Fixture::new([(
        "deep",
        (|_, host| {
            host(
                "scan",
                json!([{"kind":"collection","name":"items","indexes":{"byGroup":["group"]}},{"index":"byGroup","prefix":["a"],"offset":300,"limit":1}]),
            )
        }) as Callback,
    )]);
    let mut data = Records::new();
    let schema = Schema {
        indexes: vec![indexes::IndexSpec {
            collection: "items".into(),
            fields: vec!["group".into()],
        }],
        ..Schema::default()
    };
    let rows: Vec<_> = (0..400)
        .map(|key| json!({"collection":"items","key":format!("{key:03}"),"value":{"group":"a"}}))
        .collect();
    let result = run_with_schema(
        data.clone(),
        json!({"requestId":"schema","writes":rows}),
        "deployment",
        None,
        &fixture,
        Some(schema),
    )
    .unwrap();
    apply(&mut data, result);
    let certify = |limit: usize| {
        let result = run_with_limit(
            data.clone(),
            json!({"name":"deep"}),
            "query",
            None,
            &fixture,
            limit,
        )
        .ok()?;
        result
            .query_cacheable
            .then(|| result.query_certificate.unwrap())
    };
    let windowed = |certificate: &DependencyCertificate| {
        certificate
            .observations()
            .any(|observation| matches!(observation, Observation::Range { .. }))
    };
    // The smallest budget whose certificate stamps the window.
    let (mut low, mut high) = (0, 1 << 20);
    assert!(certify(high).as_ref().is_some_and(windowed));
    while low + 1 < high {
        let middle = (low + high) / 2;
        if certify(middle).as_ref().is_some_and(windowed) {
            high = middle;
        } else {
            low = middle;
        }
    }
    let window = certify(high).unwrap();
    // One byte less, and the window's 301 entries (the 300 skipped and the row) no longer fit:
    // the certificate keeps the index's marker instead, and still certifies the scan.
    let marker = certify(high - 1).expect("the marker still fits");
    assert!(!windowed(&marker));
    let index = super::super::ranges::dependency("items", &["group".into()]);
    assert!(
        marker
            .observations()
            .any(|observation| observation == Observation::Key(&index))
    );
    assert!(window.allocation_cost() > marker.allocation_cost() + 300 * 8);
    // The window ignores another bucket of the index; the marker does not.
    deploy(
        &mut data,
        json!({"writes":[{"collection":"items","key":"b","value":{"group":"b"}}]}),
        &fixture,
    );
    assert!(window.valid(&data));
    assert!(!marker.valid(&data));
    // Both see an entry leaving the window.
    let mut inside = data.clone();
    deploy(
        &mut inside,
        json!({"writes":[{"collection":"items","key":"010","delete":true}]}),
        &fixture,
    );
    assert!(!window.valid(&inside));
}
