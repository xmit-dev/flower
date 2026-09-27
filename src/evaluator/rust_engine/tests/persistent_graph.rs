use super::*;

fn fixture() -> Fixture {
    Fixture::new([
        (
            "leaf",
            (|args, host| {
                let tenant = args.as_str().unwrap();
                let branch = get(host, "collection", "branch", args.clone())? == true;
                if get(host, "collection", "fail", args.clone())? == true {
                    return Err(EngineError::new("EXPECTED", "tenant unavailable"));
                }
                get(
                    host,
                    "collection",
                    if branch { "alternate" } else { "input" },
                    json!(tenant),
                )
            }) as Callback,
        ),
        (
            "top",
            (|args, host| get(host, "derived", "leaf", args.clone())) as Callback,
        ),
        (
            "scratch",
            (|args, host| get(host, "derived", "top", args.clone())) as Callback,
        ),
        (
            "write",
            (|args, host| {
                let tenant = args["tenant"].as_str().unwrap();
                set(host, "input", tenant, args["value"].clone())?;
                Ok(Value::Null)
            }) as Callback,
        ),
        (
            "mixed",
            (|args, host| {
                let tenant = args["tenant"].as_str().unwrap();
                match args["op"].as_u64().unwrap() {
                    0 => {
                        set(host, "input", tenant, args["value"].clone())?;
                    }
                    1 => {
                        set(host, "alternate", tenant, args["value"].clone())?;
                    }
                    2 => {
                        set(
                            host,
                            "branch",
                            tenant,
                            args["value"].as_u64().unwrap().is_multiple_of(2).into(),
                        )?;
                    }
                    3 => {
                        set(
                            host,
                            "fail",
                            tenant,
                            args["value"].as_u64().unwrap().is_multiple_of(2).into(),
                        )?;
                    }
                    4 | 5 => {
                        host(
                            if args["op"] == 4 {
                                "materialize"
                            } else {
                                "unmaterialize"
                            },
                            json!([{"kind":"derived","name":"top"}, tenant]),
                        )?;
                    }
                    6 => {
                        let first = get(host, "derived", "scratch", json!(tenant))
                            .unwrap_or_else(|error| json!(error.code));
                        set(host, "input", tenant, args["value"].clone())?;
                        return Ok(json!([
                            first,
                            get(host, "derived", "top", json!(tenant))
                                .unwrap_or_else(|error| json!(error.code))
                        ]));
                    }
                    _ => {
                        host(
                            "delete",
                            json!([{"kind":"collection","name":"input"}, tenant]),
                        )?;
                    }
                }
                Ok(get(host, "derived", "top", json!(tenant))
                    .unwrap_or_else(|error| json!(error.code)))
            }) as Callback,
        ),
        (
            "grow",
            (|args, host| {
                let start = args["start"].as_u64().unwrap();
                for tenant in start..start + args["count"].as_u64().unwrap() {
                    let tenant = tenant.to_string();
                    set(host, "input", &tenant, json!(tenant))?;
                    host(
                        "materialize",
                        json!([{"kind":"derived","name":"top"}, tenant]),
                    )?;
                }
                Ok(Value::Null)
            }) as Callback,
        ),
        ("noop", (|_, _| Ok(Value::Null)) as Callback),
        (
            "batch",
            (|args, host| {
                let top = json!({"kind":"derived","name":"top"});
                let mut observed = Vec::new();
                for op in args.as_array().unwrap() {
                    let tenant = op[1].as_str().unwrap();
                    let result = match op[0].as_u64().unwrap() {
                        0 => set(host, "input", tenant, op[2].clone()),
                        1 => host("materialize", json!([top, tenant])),
                        2 => host("unmaterialize", json!([top, tenant])),
                        3 => get(host, "derived", "top", json!(tenant)),
                        4 => get(host, "derived", "scratch", json!(tenant)),
                        5 => set(
                            host,
                            "fail",
                            tenant,
                            op[2].as_u64().unwrap().is_multiple_of(3).into(),
                        ),
                        // Too deep for a root record: this preview fails, and
                        // rolls back, before evaluating the temporary root.
                        _ => get(host, "derived", "top", nested(tenant)),
                    };
                    observed.push(result.unwrap_or_else(|error| json!(error.code)));
                }
                Ok(Value::Array(observed))
            }) as Callback,
        ),
    ])
}

/// Valid arguments whose root record `{"name","args"}` exceeds 128 levels.
fn nested(tenant: &str) -> Value {
    (0..127).fold(json!(tenant), |value, _| json!([value]))
}

fn tenants(fixture: &Fixture, count: usize) -> Records {
    let mut data = Records::new();
    deploy(
        &mut data,
        json!({
            "materialize": (0..count).map(|id| json!({"name":"top","args":id.to_string()})).collect::<Vec<_>>(),
            "writes": (0..count).map(|id| json!({"collection":"input","key":id.to_string(),"value":id})).collect::<Vec<_>>()
        }),
        fixture,
    );
    data
}

fn write(data: &mut Records, fixture: &Fixture, tenant: &str, value: u64) -> Evaluation {
    let result = run(
        data.clone(),
        json!({"name":"write","args":{"tenant":tenant,"value":value}}),
        "mutation",
        None,
        fixture,
    )
    .unwrap();
    for (id, value) in &result.puts {
        data.insert(id.clone(), value.clone());
    }
    for id in &result.deletes {
        data.remove(id);
    }
    result
}

fn comparable(result: &Evaluation) -> Value {
    json!({"puts":result.puts,"deletes":result.deletes,"evaluated":result.evaluated,
        "value":result.value,"query_cacheable":result.query_cacheable})
}

#[test]
fn quiet_tenants_share_the_graph_and_skip_global_traversal() {
    let fixture = fixture();
    let mut data = tenants(&fixture, 256);
    let prior = data.clone();
    graph::take_graph_passes();
    graph::take_graph_visits();
    let result = write(&mut data, &fixture, "1", 1001);
    assert_eq!(graph::take_graph_passes(), (1, 0));
    // Unchanged edges leave every height as stored.
    assert_eq!(graph::take_graph_visits(), (0, 0));
    assert_eq!(
        result.evaluated,
        [cell_id("leaf", &json!("1")), cell_id("top", &json!("1"))]
    );
    assert!(Arc::ptr_eq(
        prior.get_shared(&cell_id("top", &json!("200"))).unwrap(),
        data.get_shared(&cell_id("top", &json!("200"))).unwrap()
    ));
    assert_eq!(prior[&source_id("input", "1")], 1);
    assert_eq!(data[&source_id("input", "1")], 1001);
}

#[test]
fn randomized_previews_match_forced_full_traversal() {
    let fixture = fixture();
    let mut fast = tenants(&fixture, 12);
    let mut full = fast.clone();
    let mut state = 0xdad1_5115_u64;
    for step in 0..256 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let invocation = json!({"name":"mixed","args":{
            "tenant":((state >> 24) % 12).to_string(), "op":(state >> 8) % 8, "value":step
        }});
        let actual = run(
            fast.clone(),
            invocation.clone(),
            "mutation",
            Some(step + 1),
            &fixture,
        );
        let expected = graph::with_full_graph(|| {
            run(
                full.clone(),
                invocation,
                "mutation",
                Some(step + 1),
                &fixture,
            )
        });
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => {
                assert_eq!(comparable(&actual), comparable(&expected), "step {step}");
                apply(&mut fast, actual);
                apply(&mut full, expected);
            }
            (Err(actual), Err(expected)) => assert_eq!(actual, expected, "step {step}"),
            (actual, expected) => panic!("step {step}: fast {actual:?}, full {expected:?}"),
        }
        assert_eq!(fast, full, "step {step}");
        if step % 31 == 0 {
            fast = serde_json::from_str(&serde_json::to_string(&fast).unwrap()).unwrap();
        }
    }
}

#[test]
fn batched_root_edits_and_rollbacks_match_full_traversal() {
    let fixture = fixture();
    let mut fast = tenants(&fixture, 24);
    let mut full = fast.clone();
    let root = |tenant: &str| format!("root:{}", &cell_id("top", &json!(tenant))[5..]);
    let mut roots: BTreeSet<String> = (0..24).map(|id| root(&id.to_string())).collect();
    let mut state = 0x2007_5eed_u64;
    let mut ops = |count: u64, kinds: &[u64]| -> Vec<Value> {
        (0..count)
            .map(|index| {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                let kind = kinds[(state >> 40) as usize % kinds.len()];
                json!([kind, ((state >> 20) % 32).to_string(), index])
            })
            .collect()
    };
    for step in 0..96 {
        // Tenants 24 to 31 start without roots. One transaction adds and
        // removes roots, previews them temporarily and fails previews between.
        let mutation = ops(1 + step % 9, &[0, 1, 2, 3, 4, 5, 6]);
        let query = ops(1 + step % 5, &[3, 4, 6]);
        for (batch, mode) in [(mutation, "mutation"), (query, "query")] {
            let invocation = json!({"name":"batch","args":batch});
            let actual = run(
                fast.clone(),
                invocation.clone(),
                mode,
                Some(step + 1),
                &fixture,
            );
            let expected = graph::with_full_graph(|| {
                run(full.clone(), invocation, mode, Some(step + 1), &fixture)
            });
            match (actual, expected) {
                (Ok(actual), Ok(expected)) => {
                    assert_eq!(comparable(&actual), comparable(&expected), "step {step}");
                    apply(&mut fast, actual);
                    apply(&mut full, expected);
                    // Only the transaction's own materialization calls, in
                    // order, decide which roots persist.
                    for op in &batch {
                        let id = root(op[1].as_str().unwrap());
                        match op[0].as_u64().unwrap() {
                            1 => roots.insert(id),
                            2 => roots.remove(&id),
                            _ => false,
                        };
                    }
                }
                (Err(actual), Err(expected)) => assert_eq!(actual, expected, "step {step}"),
                (actual, expected) => panic!("step {step}: fast {actual:?}, full {expected:?}"),
            }
            assert_eq!(fast, full, "step {step}");
            let stored: BTreeSet<String> = fast.graph_roots().map(|(id, _)| id.into()).collect();
            assert_eq!(stored, roots, "step {step}");
        }
    }
    assert!(roots.len() != 24, "batches changed the root set");
}

#[test]
fn terminal_preview_errors_match_rollback_path_at_memory_boundaries() {
    let fixture = fixture();
    let base = tenants(&fixture, 8);
    let retained = base.clone();
    let mut successes = 0;
    let mut failures = 0;
    for budget in [
        1,
        metadata::graph_bytes(&base) / 2,
        metadata::graph_bytes(&base),
        32_768,
        131_072,
    ] {
        let invocation = json!({"name":"write","args":{"tenant":"0","value":"large".repeat(1024)}});
        let actual = run_with_limit(
            base.clone(),
            invocation.clone(),
            "mutation",
            Some(20),
            &fixture,
            budget,
        );
        let expected = graph::with_full_graph(|| {
            run_with_limit(
                base.clone(),
                invocation,
                "mutation",
                Some(20),
                &fixture,
                budget,
            )
        });
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => {
                successes += 1;
                assert_eq!(comparable(&actual), comparable(&expected));
            }
            (Err(actual), Err(expected)) => {
                failures += 1;
                assert_eq!(actual, expected);
            }
            (actual, expected) => {
                panic!("budget {budget}: final {actual:?}, rollback {expected:?}")
            }
        }
        assert_eq!(
            base, retained,
            "failed final previews never publish the private overlay"
        );
    }
    assert!(successes > 0 && failures > 0);
}

fn recover(data: &Records) -> Records {
    let recovered: Records = serde_json::from_str(&serde_json::to_string(data).unwrap()).unwrap();
    assert_eq!(data, &recovered);
    recovered
}

fn grow(start: usize, count: usize) -> Value {
    json!({"name":"grow","args":{"start":start,"count":count}})
}

#[test]
fn restart_needs_no_graph_revalidation() {
    let fixture = fixture();
    let mut original = tenants(&fixture, 16);
    write(&mut original, &fixture, "0", 1000);
    // Reader and height records survive a restart like any other record.
    let mut recovered = recover(&original);
    graph::take_graph_passes();
    graph::take_graph_visits();
    write(&mut recovered, &fixture, "0", 1001);
    assert_eq!(graph::take_graph_passes(), (1, 0));
    assert_eq!(graph::take_graph_visits(), (0, 0));
    // The first invocation after a restart pays only for what it changes.
    let recovered = recover(&original);
    run_with_limit(
        recovered.clone(),
        json!({"name":"noop"}),
        "mutation",
        Some(1000),
        &fixture,
        metadata::graph_bytes(&recovered) / 4,
    )
    .unwrap();
}

#[test]
fn transaction_budget_covers_the_graph_it_adds_not_the_graph_it_inherits() {
    let fixture = fixture();
    // The smallest budget that admits adding `count` materialized tenants.
    let needed = |data: &Records, count: usize| {
        let run = |budget| {
            run_with_limit(
                data.clone(),
                grow(10_000, count),
                "mutation",
                None,
                &fixture,
                budget,
            )
        };
        let (mut low, mut high) = (0, 1 << 26);
        run(high).unwrap();
        while low + 1 < high {
            let budget = low + (high - low) / 2;
            match run(budget) {
                Ok(_) => high = budget,
                Err(error) => {
                    assert_eq!(error.code, "EVALUATION_BUDGET");
                    low = budget;
                }
            }
        }
        high
    };
    let mut small = tenants(&fixture, 2);
    let mut large = tenants(&fixture, 512);
    write(&mut small, &fixture, "0", 1000);
    write(&mut large, &fixture, "0", 1000);
    let one = needed(&small, 1);
    let many = needed(&small, 16);
    assert!(metadata::graph_bytes(&large) > 8 * many);
    assert_eq!(needed(&large, 1), one);
    assert_eq!(needed(&large, 16), many);
    // Every added tenant is charged at least its graph entries, so large
    // transactions stay bounded.
    assert!(many - one >= 15 * metadata::graph_bytes(&large) / 512);
    let error = run_with_limit(
        large.clone(),
        grow(10_000, 16),
        "mutation",
        None,
        &fixture,
        many - 1,
    )
    .unwrap_err();
    assert_eq!(error.code, "EVALUATION_BUDGET");
}

#[test]
fn inserts_continue_after_the_graph_outgrows_the_transaction_budget() {
    let fixture = fixture();
    let budget = 128 * 1024;
    let mut data = tenants(&fixture, 1);
    let mut next = 1;
    while metadata::graph_bytes(&data) <= 8 * budget {
        let result = run_with_limit(
            data.clone(),
            grow(next, 16),
            "mutation",
            None,
            &fixture,
            budget,
        )
        .unwrap();
        apply(&mut data, result);
        next += 16;
    }
    assert_eq!(data.graph_roots().count(), next);
    // Removing a root collects its cells without a traversal, and the same
    // budget admits growth after a restart.
    graph::take_graph_passes();
    graph::take_graph_visits();
    let result = run_with_limit(
        data.clone(),
        json!({"name":"mixed","args":{"tenant":"0","op":5,"value":0}}),
        "mutation",
        None,
        &fixture,
        budget,
    )
    .unwrap();
    // It reads `top` again after unmaterializing it: two previews, neither a
    // full traversal.
    assert_eq!(graph::take_graph_passes(), (2, 0));
    assert_eq!(graph::take_graph_visits(), (0, 0));
    apply(&mut data, result);
    assert_eq!(data.graph_roots().count(), next - 1);
    let mut recovered = recover(&data);
    let result = run_with_limit(
        recovered.clone(),
        grow(next, 16),
        "mutation",
        None,
        &fixture,
        budget,
    )
    .unwrap();
    apply(&mut recovered, result);
    assert_eq!(recovered.graph_roots().count(), next + 15);
}

const GENERATION: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

fn graph_page(data: &Records, mut command: Value, fixture: &Fixture) -> EngineResult<Evaluation> {
    command["requestId"] = json!("page");
    run(
        data.graph_view(Some(GENERATION)),
        command,
        "graph",
        None,
        fixture,
    )
}

#[test]
fn committed_append_pages_visit_only_new_nodes_with_single_and_multiple_roots() {
    let fixture = fixture();
    for count in [64, 128] {
        for page_size in [1, 8] {
            let mut data = Records::new();
            graph::take_graph_visits();
            for start in (0..count).step_by(page_size) {
                let roots: Vec<_> = (start..start + page_size)
                    .map(|id| json!({"name":"top","args":id.to_string()}))
                    .collect();
                let result = graph_page(&data, json!({"materialize":roots}), &fixture).unwrap();
                assert_eq!(result.evaluated.len(), 2 * page_size);
                assert!(result.puts.keys().all(|id| id.starts_with("graph:")));
                // This is the real commit boundary: publish only physical JSON
                // records, not the evaluator's private metadata or proof.
                apply(&mut data, result);
            }
            graph_page(&data, json!({}), &fixture).unwrap();
            let (append, full) = graph::take_graph_visits();
            assert_eq!(full, 0, "{count} roots, {page_size} roots per page");
            assert_eq!(append, 2 * count, "each new cell's height is computed once");
            assert!(
                metadata::cell_count(&data) == 0,
                "the legacy graph stays isolated"
            );
        }
    }
}

#[test]
fn committed_shared_append_pages_and_removals_match_full_validation_after_restart() {
    let fixture = fixture();
    let mut fast = Records::new();
    let mut full = Records::new();
    for step in 0..72 {
        let tenant = (step % 12).to_string();
        let command = match step / 12 {
            0 => json!({"materialize":[{"name":"top","args":tenant}]}),
            1 => json!({"materialize":[{"name":"scratch","args":tenant}]}),
            2 => json!({"unmaterialize":[{"name":"top","args":tenant}]}),
            3 => json!({"writes":[{"collection":"input","key":tenant,"value":step}]}),
            4 => json!({"unmaterialize":[{"name":"scratch","args":tenant}]}),
            _ => {
                json!({"materialize":[{"name":"scratch","args":tenant},{"name":"top","args":tenant}]})
            }
        };
        let actual = graph_page(&fast, command.clone(), &fixture).unwrap();
        let expected = graph::with_full_graph(|| graph_page(&full, command, &fixture)).unwrap();
        assert_eq!(comparable(&actual), comparable(&expected), "step {step}");
        apply(&mut fast, actual);
        apply(&mut full, expected);
        assert_eq!(fast, full);
        if step % 17 == 0 {
            fast = serde_json::from_str(&serde_json::to_string(&fast).unwrap()).unwrap();
        }
    }
}

#[test]
fn committed_removal_needs_no_rebuild_before_append_pages() {
    let fixture = fixture();
    let mut data = Records::new();
    let roots: Vec<_> = (0..16)
        .map(|id| json!({"name":"top","args":id.to_string()}))
        .collect();
    let result = graph_page(&data, json!({"materialize":roots}), &fixture).unwrap();
    apply(&mut data, result);
    let result = graph_page(
        &data,
        json!({"unmaterialize":[{"name":"top","args":"0"}]}),
        &fixture,
    )
    .unwrap();
    apply(&mut data, result);
    graph::take_graph_visits();
    for id in 16..32 {
        let result = graph_page(
            &data,
            json!({"materialize":[{"name":"top","args":id.to_string()}]}),
            &fixture,
        )
        .unwrap();
        apply(&mut data, result);
    }
    graph_page(&data, json!({}), &fixture).unwrap();
    assert_eq!(graph::take_graph_visits(), (32, 0));
}

/// chain(i) reads chain(i + 1) up to chain(126), which reads per its tail
/// record: nothing, a further leaf chain(127), or the wrapper above chain(0).
fn chain_fixture() -> Fixture {
    Fixture::new([
        (
            "chain",
            (|args, host| {
                let index = args.as_u64().unwrap();
                if index < 126 {
                    return get(host, "derived", "chain", json!(index + 1));
                }
                if index > 126 {
                    return Ok(json!(1));
                }
                match get(host, "collection", "tail", json!("end"))?.as_str() {
                    Some("deeper") => get(host, "derived", "chain", json!(127)),
                    Some("loop") => get(host, "derived", "wrapper", Value::Null),
                    _ => Ok(json!(1)),
                }
            }) as Callback,
        ),
        (
            "wrapper",
            (|_, host| get(host, "derived", "chain", json!(0))) as Callback,
        ),
        (
            "wrapper2",
            (|_, host| get(host, "derived", "wrapper", Value::Null)) as Callback,
        ),
        (
            "tail",
            (|args, host| {
                set(host, "tail", "end", args.clone())?;
                Ok(Value::Null)
            }) as Callback,
        ),
    ])
}

fn chain(fixture: &Fixture) -> Records {
    let mut data = Records::new();
    deploy(
        &mut data,
        json!({"materialize":[{"name":"chain","args":0}]}),
        fixture,
    );
    assert_eq!(
        data[&Records::height_key(&cell_id("chain", &json!(0)))],
        127
    );
    data
}

#[test]
fn appended_roots_honor_stored_descendant_heights() {
    let fixture = chain_fixture();
    let mut data = chain(&fixture);
    graph::take_graph_visits();
    deploy(
        &mut data,
        json!({"materialize":[{"name":"wrapper"}]}),
        &fixture,
    );
    // Only the new cell's height is computed, from its child's stored one.
    assert_eq!(graph::take_graph_visits(), (1, 0));
    assert_eq!(
        data[&Records::height_key(&cell_id("wrapper", &Value::Null))],
        128
    );
    let command = json!({"materialize":[{"name":"wrapper2"}],"requestId":"too-deep"});
    let actual = run(
        data.clone(),
        command.clone(),
        "deployment",
        Some(1),
        &fixture,
    )
    .unwrap_err();
    let expected =
        graph::with_full_graph(|| run(data.clone(), command, "deployment", Some(1), &fixture))
            .unwrap_err();
    assert_eq!(actual, expected);
    assert_eq!(actual.code, "EVALUATION_BUDGET");
}

#[test]
fn depth_and_cycles_are_checked_through_clean_stored_cells() {
    let fixture = chain_fixture();
    let mut data = chain(&fixture);
    deploy(
        &mut data,
        json!({"materialize":[{"name":"wrapper"}]}),
        &fixture,
    );
    for (tail, code) in [("deeper", "EVALUATION_BUDGET"), ("loop", "CYCLE")] {
        let command = json!({"name":"tail","args":tail});
        let actual = run(data.clone(), command.clone(), "mutation", Some(2), &fixture).unwrap_err();
        let expected =
            graph::with_full_graph(|| run(data.clone(), command, "mutation", Some(2), &fixture))
                .unwrap_err();
        assert_eq!(actual, expected, "{tail}");
        assert_eq!(actual.code, code, "{tail}");
    }
}

#[test]
fn removing_a_root_collects_its_cells_without_a_traversal() {
    let fixture = fixture();
    let data = tenants(&fixture, 64);
    let command = json!({"name":"mixed","args":{"tenant":"7","op":5,"value":0}});
    graph::take_graph_passes();
    graph::take_graph_visits();
    let actual = run(data.clone(), command.clone(), "mutation", Some(1), &fixture).unwrap();
    // It reads `top` again after unmaterializing it: two previews, neither a
    // full traversal.
    assert_eq!(graph::take_graph_passes(), (2, 0));
    assert_eq!(graph::take_graph_visits(), (0, 0));
    let expected =
        graph::with_full_graph(|| run(data.clone(), command, "mutation", Some(1), &fixture))
            .unwrap();
    assert_eq!(comparable(&actual), comparable(&expected));
    let top = cell_id("top", &json!("7"));
    let leaf = cell_id("leaf", &json!("7"));
    assert!(actual.deletes.contains(&top));
    assert!(actual.deletes.contains(&leaf));
    assert!(actual.deletes.contains(&Records::height_key(&top)));
    assert!(actual.deletes.contains(&Records::reader_key(&leaf, &top)));
}

#[test]
fn certified_graph_still_evaluates_dirty_children_retained_by_callback_errors() {
    let fixture = Fixture::new([
        (
            "a",
            (|_, host| {
                if get(host, "collection", "input", json!("switch"))? == true {
                    return Err(EngineError::new("EXPECTED", "failed"));
                }
                get(host, "derived", "b", Value::Null)
            }) as Callback,
        ),
        (
            "b",
            (|_, host| {
                if get(host, "collection", "input", json!("switch"))? == true {
                    let _ = get(host, "derived", "a", Value::Null);
                }
                Ok(json!(1))
            }) as Callback,
        ),
    ]);
    let mut data = Records::new();
    let result = graph_page(&data, json!({"materialize":[{"name":"a"}]}), &fixture).unwrap();
    apply(&mut data, result);
    graph_page(&data, json!({}), &fixture).unwrap();
    let command = json!({"writes":[{"collection":"input","key":"switch","value":true}]});
    let actual = graph_page(&data, command.clone(), &fixture).unwrap_err();
    let expected = graph::with_full_graph(|| graph_page(&data, command, &fixture)).unwrap_err();
    assert_eq!(actual, expected);
    assert_eq!(actual.code, "CYCLE");
}

#[test]
fn root_removal_collects_only_unreachable_shared_descendants() {
    let fixture = fixture();
    let mut data = tenants(&fixture, 1);
    deploy(
        &mut data,
        json!({"materialize":[{"name":"scratch","args":"0"}]}),
        &fixture,
    );
    write(&mut data, &fixture, "0", 1000);
    deploy(
        &mut data,
        json!({"unmaterialize":[{"name":"top","args":"0"}]}),
        &fixture,
    );
    assert!(data.contains_key(&cell_id("top", &json!("0"))));
    assert!(data.contains_key(&cell_id("leaf", &json!("0"))));
    deploy(
        &mut data,
        json!({"unmaterialize":[{"name":"scratch","args":"0"}]}),
        &fixture,
    );
    assert!(metadata::cell_count(&data) == 0);
    assert_eq!(data.graph_roots().count(), 0);
    assert!(
        !data
            .keys()
            .any(|key| key.starts_with("reader:") || key.starts_with("height:"))
    );
}

#[test]
fn cached_validation_tracks_malformed_cells_roots_and_clock_dependencies() {
    let fixture = fixture();
    let mut data = tenants(&fixture, 1);
    write(&mut data, &fixture, "0", 1000);
    assert!(data.reactive().cacheable());
    let id = cell_id("leaf", &json!("0"));
    let good = data[&id].clone();
    // Reading the clock is a reader record, written with the dependency.
    let mut clock = good.clone();
    clock["deps"].as_array_mut().unwrap().push(json!("clock"));
    let reader = Records::reader_key("clock", &id);
    data.insert(id.clone(), clock);
    data.insert(reader.clone(), Value::Null);
    assert!(!data.reactive().cacheable());
    data.insert(id.clone(), good.clone());
    data.remove(&reader);
    assert!(data.reactive().cacheable());
    let mut malformed = good.clone();
    malformed["deps"] = Value::Null;
    data.insert(id.clone(), malformed);
    assert!(data.reactive().validate().is_err());
    assert_eq!(
        run(
            data.clone(),
            json!({"name":"noop"}),
            "mutation",
            Some(1),
            &fixture
        )
        .unwrap_err()
        .code,
        "INPUT_INVALID"
    );
    data.insert(id, good);
    assert!(data.reactive().validate().is_ok());
    let root = root_id("top", &json!("0"));
    data.insert(root.clone(), json!({"name":"other","args":"0"}));
    assert_eq!(
        run(
            data.clone(),
            json!({"name":"noop"}),
            "mutation",
            Some(1),
            &fixture
        )
        .unwrap_err()
        .code,
        "INPUT_INVALID"
    );
    data.insert(root, json!({"name":"top","args":"0"}));
    assert!(data.reactive().validate().is_ok());
}

#[test]
fn derived_branch_switch_collects_old_branch_in_the_same_preview() {
    let fixture = Fixture::new([
        ("left", (|_, _| Ok(json!(1))) as Callback),
        ("right", (|_, _| Ok(json!(2))) as Callback),
        (
            "choose",
            (|_, host| {
                let name = if get(host, "collection", "branch", json!("chosen"))? == true {
                    "right"
                } else {
                    "left"
                };
                get(host, "derived", name, Value::Null)
            }) as Callback,
        ),
        (
            "switch",
            (|args, host| {
                set(host, "branch", "chosen", args.clone())?;
                get(host, "derived", "choose", Value::Null)
            }) as Callback,
        ),
        ("noop", (|_, _| Ok(Value::Null)) as Callback),
    ]);
    let mut data = Records::new();
    deploy(
        &mut data,
        json!({"materialize":[{"name":"choose"}]}),
        &fixture,
    );
    for (index, switch) in [true, false, true].into_iter().enumerate() {
        run(
            data.clone(),
            json!({"name":"noop"}),
            "mutation",
            Some((index * 2 + 1) as u64),
            &fixture,
        )
        .unwrap();
        let command = json!({"name":"switch","args":switch});
        graph::take_graph_passes();
        let actual = run(
            data.clone(),
            command.clone(),
            "mutation",
            Some((index * 2 + 2) as u64),
            &fixture,
        )
        .unwrap();
        assert_eq!(
            graph::take_graph_passes(),
            (1, 0),
            "an edge change needs no full traversal"
        );
        let expected = graph::with_full_graph(|| {
            run(
                data.clone(),
                command,
                "mutation",
                Some((index * 2 + 2) as u64),
                &fixture,
            )
        })
        .unwrap();
        assert_eq!(comparable(&actual), comparable(&expected));
        assert_eq!(actual.value, if switch { json!(2) } else { json!(1) });
        apply(&mut data, actual);
        assert!(data.contains_key(&cell_id(
            if switch { "right" } else { "left" },
            &Value::Null
        )));
        assert!(!data.contains_key(&cell_id(
            if switch { "left" } else { "right" },
            &Value::Null
        )));
    }
}

#[test]
fn metadata_bounds_argument_depth_before_recursive_identity_work() {
    for kind in ["cell", "root"] {
        let args = (0..512).fold(Value::Null, |value, _| Value::Array(vec![value]));
        let value = json!({"name":"deep","args":args,"deps":[],"outcome":{"ok":true,"value":1}});
        let mut records = Records::new();
        records.insert(format!("{kind}:[]"), value);
        assert!(!records.has_valid_depth());
        let error = records.reactive().validate().unwrap_err();
        assert_eq!(error.code, "INPUT_INVALID");
        assert!(error.message.contains("nesting"));
    }
}

#[test]
fn clock_readers_follow_reader_records_through_rebuilds() {
    let mut data = Records::default();
    // A raw write may leave a cell with no dependency list or reader records.
    data.insert(r#"cell:["double",null]"#.into(), json!({"ok": true, "value": 2}));
    assert!(data.reactive().cacheable());
    // Only a write changes what a key reader read, and certificates observe
    // the catalog, so key readers leave results certifiable.
    data.insert(Records::reader_key("managedKeys", "cell:b"), Value::Null);
    assert!(data.reactive().cacheable());
    data.insert(Records::reader_key("clock", "cell:a"), Value::Null);
    data.insert(Records::reader_key("clock", "cell:c"), Value::Null);
    let decoded: Records = serde_json::from_value(serde_json::to_value(&data).unwrap()).unwrap();
    let backed = data.backed_copy();
    for mut records in [data, decoded, backed] {
        assert!(!records.reactive().cacheable());
        records.remove(r#"cell:["double",null]"#);
        records.remove(&Records::reader_key("clock", "cell:a"));
        assert!(!records.reactive().cacheable());
        records.remove(&Records::reader_key("clock", "cell:c"));
        assert!(records.reactive().cacheable());
    }
}
