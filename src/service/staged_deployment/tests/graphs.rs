use super::*;

fn graph_bundle(multiplier: u64, fail: bool) -> String {
    format!(
        r#"
const rows={{kind:'collection',name:'rows'}};
const ref=(name,args)=>({{kind:'derived',name}});
var __flowerBundle={{default:{{definitions:{{
 shared:{{kind:'derived',name:'shared',compute:ctx=>{{
   if({fail} && ctx.get(rows,'break')) return ctx.get(ref('shared'));
   return ctx.get(rows,'shared') ?? 0;
 }}}},
 item:{{kind:'derived',name:'item',compute:(ctx,id)=>ctx.get(rows,id ?? 'absent') ?? 0}},
 stable:{{kind:'derived',name:'stable',compute:(ctx,id)=>(ctx.get(ref('item'),id)+ctx.get(ref('shared')))*{multiplier}}},
 write:{{kind:'mutationMethod',name:'write',compute:(ctx,args)=>{{
   for(const [id,value] of args.values??[])ctx.set(rows,id,value);
   for(const id of args.keep??[])ctx.materialize(ref('stable'),id);
   for(const id of args.drop??[])ctx.unmaterialize(ref('stable'),id);
   return null;
 }}}},
 read:{{kind:'queryMethod',name:'read',compute:(ctx,id)=>ctx.get(ref('stable'),id)}}
}},http:{{write:{{name:'write',kind:'mutation'}},read:{{name:'read',kind:'query'}}}}}}}};
"#
    )
}

async fn read_root(app: &App, id: &str) -> Value {
    let state = app.consensus.read_query().await.unwrap();
    evaluator::invoke_at(state.data, json!({"name":"read","args":id}), "query", 1000)
        .unwrap()
        .value
}

async fn page(app: &App, id: &str) -> Value {
    administer(
        app,
        json!({"operation":"advance","requestId":id,"maxBytes":16_384}),
    )
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_pages_keep_shared_dependencies_current_and_switch_without_rebuilding() {
    let (_directory, app) = super::super::super::tests::application(graph_bundle(1, false)).await;
    write(&app,"roots",json!({"values":[["00",1],["01",2],["02",3],["shared",10]],"keep":["00","01","02"],"drop":[null]})).await;
    let started = administer(&app, stage_input("graph-v2", graph_bundle(2, false)))
        .await
        .unwrap();
    assert_eq!(started["value"]["phase"], "rebuilding");
    let generation = started["value"]["generation"].as_str().unwrap().to_owned();
    let first = page(&app, "graph-v2").await;
    assert_eq!(first["value"]["rebuiltRoots"], 1);
    assert_eq!(first["value"]["phase"], "rebuilding");
    let before = app.consensus.read_query().await.unwrap();
    let shadow = before.data.graph_view(Some(&generation));
    assert_eq!(shadow["cell:[\"stable\",\"00\"]"]["outcome"]["value"], 22);
    assert!(shadow.get("cell:[\"stable\",\"01\"]").is_none());
    assert_eq!(read_root(&app, "00").await, 11);

    // Alter built dependencies, add a root behind the traversal cursor, and
    // remove an unvisited root. The old code remains public throughout.
    write(
        &app,
        "interleave",
        json!({"values":[["00",5],["shared",20],["-1",4]],"keep":["-1"],"drop":["02"]}),
    )
    .await;
    let current = app.consensus.read_query().await.unwrap();
    let shadow = current.data.graph_view(Some(&generation));
    assert_eq!(shadow["cell:[\"stable\",\"00\"]"]["outcome"]["value"], 50);
    assert_eq!(shadow["cell:[\"stable\",\"-1\"]"]["outcome"]["value"], 48);
    assert!(shadow.get("root:[\"stable\",\"02\"]").is_none());
    assert_eq!(read_root(&app, "00").await, 25);
    finish(&app, "graph-v2").await;
    // Ready is a maintained state too: root changes after the last page must
    // require no additional advance before cutover, including behind cursor.
    write(
        &app,
        "ready-interleave",
        json!({"values":[["-2",6],["shared",21]],"keep":["-2"],"drop":["-1"]}),
    )
    .await;
    let ready = app.consensus.read_query().await.unwrap();
    assert_eq!(ready.data[staging::JOB]["phase"], "ready");
    let ready_graph = ready.data.graph_view(Some(&generation));
    assert_eq!(
        ready_graph
            .graph_roots()
            .map(|(key, _)| key)
            .collect::<Vec<_>>(),
        vec![
            "root:[\"stable\",\"-2\"]",
            "root:[\"stable\",\"00\"]",
            "root:[\"stable\",\"01\"]"
        ]
    );
    assert_eq!(
        ready_graph["cell:[\"stable\",\"-2\"]"]["outcome"]["value"],
        54
    );
    let input = json!({"requestId":"graph-v2","bundle":ready.data[PLAN]["bundle"]});
    let activation = staging::activate(ready.data.clone(), input, 1000).unwrap();
    assert!(
        activation.evaluated.is_empty(),
        "activation must reuse prepared clock-independent cells"
    );
    assert!(
        activation.puts.keys().all(|key| !key.contains(":cell:")),
        "cutover must not copy the graph"
    );
    administer(&app, json!({"operation":"activate","requestId":"graph-v2"}))
        .await
        .unwrap();
    assert_eq!(read_root(&app, "00").await, 52);
    assert_eq!(read_root(&app, "01").await, 46);
    assert_eq!(read_root(&app, "-2").await, 54);
    let switched = app.consensus.read_query().await.unwrap();
    assert_eq!(
        switched.data.graph_roots().collect::<Vec<_>>(),
        ready_graph.graph_roots().collect::<Vec<_>>()
    );
    collect_all(&app, "graph-v2").await;
    let active = app.consensus.read_query().await.unwrap();
    assert_eq!(active.data.active_graph(), Some(generation.as_str()));
    assert!(
        !active
            .data
            .keys()
            .any(|key| key.starts_with("cell:") || key.starts_with("root:"))
    );
    write(&app, "after-cutover", json!({"values":[["shared",1]]})).await;
    assert_eq!(read_root(&app, "00").await, 12);

    // A second generation cleans only its predecessor; ordinary deployment
    // also continues to work against the selected physical graph.
    administer(&app, stage_input("graph-v3", graph_bundle(3, false)))
        .await
        .unwrap();
    finish(&app, "graph-v3").await;
    administer(&app, json!({"operation":"activate","requestId":"graph-v3"}))
        .await
        .unwrap();
    collect_all(&app, "graph-v3").await;
    let active = app.consensus.read_query().await.unwrap();
    assert!(
        !active
            .data
            .keys()
            .any(|key| key.starts_with(&format!("graph:{generation}:")))
    );
    assert_eq!(read_root(&app, "00").await, 18);
    let code = graph_bundle(4, false);
    writer::submit(&app,json!({"requestId":"direct-v4","bundle":{"hash":evaluator::hash(code.as_bytes()),"javascript":code},"preparation":"blocking"}),true).await.unwrap();
    assert_eq!(read_root(&app, "00").await, 24);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shadow_failure_does_not_reject_active_writes_and_cancellation_cleans_graph() {
    let (_directory, app) = super::super::super::tests::application(graph_bundle(1, false)).await;
    write(
        &app,
        "seed-graph",
        json!({"values":[["00",2]],"keep":["00"],"drop":[null]}),
    )
    .await;
    let staged = administer(&app, stage_input("failing", graph_bundle(2, true)))
        .await
        .unwrap();
    let generation = staged["value"]["generation"].as_str().unwrap().to_owned();
    finish(&app, "failing").await;
    write(
        &app,
        "break-target",
        json!({"values":[["break",true],["00",7]]}),
    )
    .await;
    let state = app.consensus.read_query().await.unwrap();
    assert_eq!(state.data[staging::JOB]["phase"], "failed");
    assert!(
        state.data[staging::JOB]["error"]
            .as_str()
            .unwrap()
            .contains("CYCLE")
    );
    assert_eq!(read_root(&app, "00").await, 7);
    assert!(
        administer(&app, json!({"operation":"activate","requestId":"failing"}))
            .await
            .is_err()
    );
    write(&app, "still-available", json!({"values":[["00",8]]})).await;
    assert_eq!(read_root(&app, "00").await, 8);
    administer(&app, json!({"operation":"cancel","requestId":"failing"}))
        .await
        .unwrap();
    collect_all(&app, "failing").await;
    let state = app.consensus.read_query().await.unwrap();
    assert!(
        !state
            .data
            .keys()
            .any(|key| key.starts_with(&format!("graph:{generation}:")))
    );
    assert_eq!(read_root(&app, "00").await, 8);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_page_output_budget_preserves_progress_and_retries() {
    let (_directory, app) = super::super::super::tests::application(graph_bundle(1, false)).await;
    administer(&app, stage_input("bounded", graph_bundle(2, false)))
        .await
        .unwrap();
    let before = app.consensus.read_query().await.unwrap();
    assert!(
        administer(
            &app,
            json!({"operation":"advance","requestId":"bounded","maxBytes":1})
        )
        .await
        .is_err()
    );
    assert_eq!(app.consensus.read_query().await.unwrap(), before);
    let advanced = page(&app, "bounded").await;
    assert_eq!(advanced["value"]["rebuiltRoots"], 1);
    finish(&app, "bounded").await;
    administer(&app, json!({"operation":"activate","requestId":"bounded"}))
        .await
        .unwrap();
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_batch_commits_all_roots_together_and_oversized_batch_falls_back_atomically() {
    let (_directory, app) = super::super::super::tests::application(graph_bundle(1, false)).await;
    write(&app, "batch-roots", json!({"values":[["00",1],["01",2],["02",3],["03",4]],"keep":["00","01","02","03"],"drop":[null]})).await;
    administer(&app, stage_input("batch", graph_bundle(2, false)))
        .await
        .unwrap();
    let base = app.consensus.read_for_writer().await.unwrap();
    let original = base.clone();
    let job = load(&base).unwrap().unwrap();
    let timeout = Duration::from_secs(10);
    let prepare = |job, budget| {
        graph_pages::prepare_page(
            &base,
            job,
            budget,
            32 * 1024 * 1024,
            128 * 1024 * 1024,
            1000,
            timeout,
            timeout,
        )
    };
    let (_, single) = prepare(job.clone(), 65_536).unwrap();
    let puts: BTreeMap<_, _> = single
        .puts
        .iter()
        .filter(|(key, _)| key.starts_with("graph:"))
        .collect();
    let single_bytes =
        crate::consensus::encoded_json_len(&json!({"puts":puts,"deletes":single.deletes})).unwrap();
    let mut batch = job.clone();
    batch.graph_page_roots = 4;
    let (limited, first) = prepare(batch.clone(), single_bytes).unwrap();
    assert_eq!(
        limited.rebuilt_roots, 1,
        "the oversized candidate must publish only its first root"
    );
    assert_eq!(limited.graph_page_roots, 1);
    assert_eq!(limited.phase, Phase::Rebuilding);
    assert_eq!(
        first.puts,
        single
            .puts
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    if key == staging::JOB {
                        json!(limited)
                    } else {
                        value.clone()
                    },
                )
            })
            .collect()
    );
    assert_eq!(
        base, original,
        "failed candidate evaluation must not alter its immutable input"
    );

    let (timed, _) = graph_pages::prepare_page(
        &base,
        batch.clone(),
        65_536,
        32 * 1024 * 1024,
        128 * 1024 * 1024,
        1000,
        timeout,
        Duration::ZERO,
    )
    .unwrap();
    assert_eq!(
        timed.rebuilt_roots, 1,
        "an expired batch must retry only its first root with the full allowance"
    );
    assert_eq!(timed.graph_page_roots, 1);
    let single_command_bytes = crate::consensus::encoded_json_len(&single).unwrap();
    let (transaction_limited, _) = graph_pages::prepare_page(
        &base,
        batch.clone(),
        65_536,
        single_command_bytes,
        128 * 1024 * 1024,
        1000,
        timeout,
        timeout,
    )
    .unwrap();
    assert_eq!(
        transaction_limited.rebuilt_roots, 1,
        "the exact command limit must include durable progress metadata"
    );

    let (complete, command) = prepare(batch, 65_536).unwrap();
    assert_eq!(complete.rebuilt_roots, 4);
    assert_eq!(complete.phase, Phase::Ready);
    assert_eq!(
        command
            .puts
            .keys()
            .filter(|key| key.contains(":root:"))
            .count(),
        4
    );
    app.consensus.commit(command).await.unwrap();
    let ready = app.consensus.read_for_writer().await.unwrap();
    assert_eq!(ready.data[staging::JOB]["rebuiltRoots"], 4);
    assert_eq!(read_root(&app, "03").await, 4);
    administer(&app, json!({"operation":"activate","requestId":"batch"}))
        .await
        .unwrap();
    assert_eq!(read_root(&app, "03").await, 8);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn graph_progress_rejects_malformed_cursors_and_counters_but_accepts_deleted_roots() {
    let (_directory, app) = super::super::super::tests::application(graph_bundle(1, false)).await;
    administer(
        &app,
        stage_input("cursor-validation", graph_bundle(2, false)),
    )
    .await
    .unwrap();
    let original = app.consensus.read_for_writer().await.unwrap();
    let check = |field: &str, value: Value| {
        let mut state = original.clone();
        let mut job = state.data[staging::JOB].clone();
        job[field] = value;
        state.data.insert(staging::JOB.into(), job);
        assert!(progress(&state).is_err(), "malformed {field} accepted");
    };
    for cursor in [
        "zz",
        "cell:[\"stable\",null]",
        "root:not-json",
        "root:[12,null]",
        "root:[\"stable\"]",
        "root:[\"stable\",null,0]",
        "root:{}",
    ] {
        check("graphCursor", json!(cursor));
    }
    check(
        "graphCursor",
        json!(format!("graph:{}:root:[\"stable\",null]", "b".repeat(64))),
    );
    for counter in ["scannedRows", "builtEntries", "rebuiltRoots"] {
        check(counter, json!(9_007_199_254_740_992_u64));
    }
    let mut old_format = original.clone();
    let mut job = old_format.data[staging::JOB].clone();
    job.as_object_mut().unwrap().remove("graphPageRoots");
    job.as_object_mut().unwrap().remove("graphRootBytes");
    old_format.data.insert(staging::JOB.into(), job);
    let decoded = progress(&old_format).unwrap().unwrap();
    assert_eq!(decoded.graph_page_roots, 1);
    assert_eq!(decoded.graph_root_bytes, 0);

    for base_generation in [None, Some("b".repeat(64))] {
        let mut state = original.clone();
        let mut job = state.data[staging::JOB].clone();
        job["baseGeneration"] = json!(base_generation);
        let cursor = base_generation.as_ref().map_or_else(
            || "root:[\"stable\",\"deleted\"]".into(),
            |generation| format!("graph:{generation}:root:[\"stable\",\"deleted\"]"),
        );
        assert!(!state.data.contains_key(&cursor));
        job["graphCursor"] = json!(cursor);
        state.data.insert(staging::JOB.into(), job.clone());
        assert!(
            progress(&state).is_ok(),
            "deleted cursor roots remain valid"
        );
        if base_generation.is_some() {
            job["graphCursor"] = json!("root:[\"stable\",\"deleted\"]");
            state.data.insert(staging::JOB.into(), job);
            assert!(
                progress(&state).is_err(),
                "legacy cursor accepted for versioned base"
            );
        }
    }
    assert_eq!(app.consensus.read_for_writer().await.unwrap(), original);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn graph_batch_fallback_reserves_exact_partition_envelope_bytes() {
    let (_directory, app) = super::super::super::tests::application(graph_bundle(1, false)).await;
    write(
        &app,
        "scoped-budget-roots",
        json!({"keep":["00","01","02","03"],"drop":[null]}),
    )
    .await;
    administer(&app, stage_input("scoped-budget", graph_bundle(2, false)))
        .await
        .unwrap();
    let base = app.consensus.read_for_writer().await.unwrap();
    let mut job = load(&base).unwrap().unwrap();
    job.graph_page_roots = 4;
    let partition = "east \"partition\" 🌷\n";
    let epoch = 12345;
    let scoped = app.consensus.partition(partition, epoch).unwrap();
    let envelope = crate::consensus::encoded_json_len(&json!({
        "partition":partition,"epoch":epoch,"command":null
    }))
    .unwrap()
        - 4;
    assert_eq!(
        app.consensus.command_payload_limit(),
        app.consensus.limits().transaction_max_bytes
    );
    assert_eq!(
        scoped.command_payload_limit() + envelope,
        scoped.limits().transaction_max_bytes
    );
    let prepare = |limit| {
        graph_pages::prepare_page(
            &base,
            job.clone(),
            65_536,
            limit,
            128 * 1024 * 1024,
            1000,
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .unwrap()
    };
    let (_, batch) = prepare(scoped.command_payload_limit());
    let batch_bytes = crate::consensus::encoded_json_len(&batch).unwrap();
    let wire_limit = batch_bytes + envelope - 1;
    assert!(
        batch_bytes < wire_limit,
        "the unscoped batch falsely appears to fit"
    );
    let (next, command) = prepare(wire_limit - envelope);
    assert_eq!(
        next.rebuilt_roots, 1,
        "envelope reservation must trigger first-root fallback"
    );
    assert_eq!(next.phase, Phase::Rebuilding);
    let scoped_command = crate::consensus::RaftCommand::Scoped {
        partition: partition.into(),
        epoch,
        command: Box::new(crate::consensus::RaftCommand::Single(command)),
    };
    assert!(crate::consensus::encoded_json_len(&scoped_command).unwrap() <= wire_limit);
    assert_eq!(app.consensus.read_for_writer().await.unwrap(), base);
    app.consensus.shutdown().await.unwrap();
}
