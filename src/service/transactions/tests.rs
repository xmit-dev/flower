use super::*;

const JAVASCRIPT: &str = r#"
const rows={kind:'collection',name:'rows'};
const add={kind:'mutationMethod',name:'add',compute:(ctx,args)=>{
  const next=(ctx.get(rows,'count')??0)+args;
  ctx.set(rows,'count',next); return next;
}};
const read={kind:'queryMethod',name:'read',compute:ctx=>ctx.get(rows,'count')??0};
const fail={kind:'mutationMethod',name:'fail',compute:ctx=>{ctx.set(rows,'count',999); throw Error('deliberate failure');}};
const tx={kind:'transactionMethod',name:'tx',compute:(_ctx,args)=>args};
var __flowerBundle={default:{definitions:{add,read,fail,tx},http:{
 add:{name:'add',kind:'mutation'},read:{name:'read',kind:'query'},fail:{name:'fail',kind:'mutation'},tx:{name:'tx',kind:'transaction'}
}}};
"#;

async fn application() -> (tempfile::TempDir, Arc<App>) {
    let directory = tempfile::tempdir().unwrap();
    let address = "127.0.0.1:7101".to_owned();
    let consensus = Consensus::open(
        1,
        address.clone(),
        directory.path().into(),
        "transaction-test-secret".into(),
    )
    .await
    .unwrap();
    consensus
        .initialize(BTreeMap::from([(1, address.clone())]))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while consensus.read_for_writer().await.is_err() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let evaluation = evaluator::evaluate_at(
        BTreeMap::new(),
        json!({"requestId":"deploy","bundle":{
            "javascript":JAVASCRIPT,"hash":evaluator::hash(JAVASCRIPT.as_bytes())
        }}),
        1000,
    )
    .unwrap();
    consensus
        .commit(Commit {
            internal: false,
            request_id: "deploy".into(),
            fingerprint: "deploy".into(),
            expected_revision: 0,
            puts: evaluation.puts,
            deletes: evaluation.deletes,
            result: Value::Null,
        })
        .await
        .unwrap();
    let (writer_queue, _receiver) = mpsc::channel(1);
    let app = Arc::new(App {
        consensus,
        writer: Mutex::new(()),
        writer_queue,
        evaluations: Arc::new(Semaphore::new(1)),
        query_evaluations: Arc::new(Semaphore::new(1)),
        admission: crate::service::admission::Pool::configured().unwrap(),
        query_cache: query_cache::QueryCache::default(),
        watch_hubs: crate::service::watch::hubs::Registry::default(),
        admin_token: "transaction-test-secret".into(),
        clock: clock::Clock::new(),
        partition_gate: None,
        cross_group: Runtime {
            group: Some("local".into()),
            groups: BTreeMap::from([("local".into(), vec![address])]),
            client: reqwest::Client::builder()
                .http2_prior_knowledge()
                .build()
                .unwrap(),
            coordinator: Mutex::new(BTreeMap::new()),
            partitions: std::sync::OnceLock::new(),
        },
    });
    (directory, app)
}

async fn begin(app: &App, id: &str, calls: Value) -> Reference {
    let mut reference = Reference {
        history: "01".repeat(16),
        sequence: 1,
        coordinator: app.cross_group.name().unwrap().into(),
        request_id: id.into(),
        fingerprint: format!("fingerprint-{id}"),
    };
    let mut record = plan(
        app.cross_group.name().unwrap(),
        json!({"calls":calls}),
        reference.fingerprint.clone(),
        &app.cross_group.groups,
    )
    .unwrap();
    let state = app.consensus.read_for_writer().await.unwrap();
    closure::allocate(&state, &mut reference, &mut record).unwrap();
    persist(
        app,
        &state,
        BTreeMap::from([(coordinator_key(id), json!(record))]),
        vec![],
    )
    .await
    .unwrap();
    reference
}

async fn read(app: &App) -> Value {
    let state = app.consensus.read_for_writer().await.unwrap();
    evaluator::invoke_at(
        state.data,
        json!({"name":"read","args":null}),
        "query",
        1000,
    )
    .unwrap()
    .value
}

#[test]
fn registry_requires_distinct_addresses_and_names() {
    assert!(parse_registry("a", r#"{"a":["localhost:7101"],"b":["localhost:7102"]}"#).is_ok());
    for (group, input) in [
        ("missing", r#"{"a":["localhost:7101"]}"#),
        ("a", r#"{"a":[]}"#),
        ("a", r#"{"a":["localhost:7101"],"b":["localhost:7101"]}"#),
        ("a", r#"{"a":["localhost:7101/path"]}"#),
        ("a", r#"{"a":["secret@localhost:7101"]}"#),
    ] {
        assert!(parse_registry(group, input).is_err(), "{input}");
    }
}

#[test]
fn lock_gate_fails_closed_even_for_corrupt_metadata() {
    let mut state = Snapshot::default();
    assert!(ensure_unlocked(&state).is_ok());
    state.data.insert(PARTICIPANT.into(), Value::Null);
    assert_eq!(
        ensure_unlocked(&state).unwrap_err().code,
        "TRANSACTION_PREPARED"
    );
    state.data.insert(coordinator_key("claimed"), Value::Null);
    assert_eq!(
        ensure_request_id_available(&state, "claimed")
            .unwrap_err()
            .code,
        "REQUEST_ID_REUSED"
    );
}

#[test]
fn stored_patch_preserves_individual_json_depth_budget() {
    let mut value = json!(0);
    for _ in 0..125 {
        value = json!([value]);
    }
    let patch = Patch {
        puts: BTreeMap::from([("nested".into(), value.clone())]),
        deletes: vec![],
    };
    let encoded = packed(&patch).unwrap();
    let restored: Patch = unpacked(&encoded).unwrap();
    assert_eq!(restored.puts["nested"], value);
}

#[tokio::test]
async fn preparation_is_invisible_and_abort_fences_late_prepare() {
    let (_directory, app) = application().await;
    let reference = begin(
        &app,
        "abort",
        json!([
            {"group":"local","method":"add","args":2},
            {"group":"local","method":"add","args":3},
            {"group":"local","method":"read"}
        ]),
    )
    .await;
    let results = prepare(&app, &reference).await.unwrap();
    assert_eq!(
        results
            .iter()
            .map(|result| result.value.as_str())
            .collect::<Vec<_>>(),
        ["2", "5", "5"]
    );
    assert_eq!(
        read(&app).await,
        0,
        "prepared writes must remain invisible in the snapshot"
    );
    assert!(ensure_unlocked(&app.consensus.read_for_writer().await.unwrap()).is_err());
    assert!(
        finish(&app, &reference).await.is_err(),
        "a caller cannot commit without a durable decision"
    );
    let decision = decide(
        &app,
        &reference,
        Phase::Abort,
        None,
        Some("test abort".into()),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        finish_coordinator(&app, &reference, &decision)
            .await
            .unwrap_err()
            .code,
        "TRANSACTION_ABORTED"
    );
    assert!(ensure_unlocked(&app.consensus.read_for_writer().await.unwrap()).is_ok());
    assert_eq!(read(&app).await, 0);
    assert!(prepare(&app, &reference).await.is_err());
    let revision = app.consensus.read_for_writer().await.unwrap().revision;
    finish(&app, &reference).await.unwrap();
    assert_eq!(
        app.consensus.read_for_writer().await.unwrap().revision,
        revision,
        "finish retries cannot write again"
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn execute_orders_results_commits_once_and_fences_content_reuse() {
    let (_directory, app) = application().await;
    let input = json!({"name":"tx","requestId":"successful","args":{"calls":[
        {"group":"local","method":"add","args":2},
        {"group":"local","method":"read"},
        {"group":"local","method":"add","args":3}
    ],"value":{"label":"done"}}});
    let response = execute(app.clone(), input.clone()).await.unwrap();
    assert_eq!(
        response["value"],
        json!({"results":[2,2,5],"value":{"label":"done"}})
    );
    assert_eq!(response["duplicate"], false);
    let retried = execute(app.clone(), input.clone()).await.unwrap();
    assert_eq!(retried["revision"], response["revision"]);
    assert_eq!(retried["duplicate"], true);
    assert_eq!(read(&app).await, 5);
    let mut conflicting = input;
    conflicting["args"]["value"] = json!("different");
    assert_eq!(
        execute(app.clone(), conflicting).await.unwrap_err().code,
        "REQUEST_ID_REUSED"
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_participant_rolls_back_all_earlier_calls() {
    let (_directory, app) = application().await;
    let response = execute(
        app.clone(),
        json!({"name":"tx","requestId":"failure","args":{"calls":[
            {"group":"local","method":"add","args":2},
            {"group":"local","method":"fail"}
        ]}}),
    )
    .await
    .unwrap_err();
    assert_eq!(response.code, "TRANSACTION_ABORTED");
    assert_eq!(read(&app).await, 0);
    assert!(ensure_unlocked(&app.consensus.read_for_writer().await.unwrap()).is_ok());
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn recovery_aborts_preparing_but_never_reverses_commit() {
    let (_directory, app) = application().await;
    let abandoned = begin(
        &app,
        "abandoned",
        json!([{"group":"local","method":"add","args":4}]),
    )
    .await;
    prepare(&app, &abandoned).await.unwrap();
    recover(&app).await.unwrap();
    assert_eq!(
        local_status(&app, &abandoned).await.unwrap().phase,
        Phase::Abort
    );
    assert_eq!(read(&app).await, 0);
    let committed = begin(
        &app,
        "decided",
        json!([{"group":"local","method":"add","args":7}]),
    )
    .await;
    prepare(&app, &committed).await.unwrap();
    decide(
        &app,
        &committed,
        Phase::Commit,
        Some("[7]".into()),
        None,
        None,
    )
    .await
    .unwrap();
    let raced = decide(
        &app,
        &committed,
        Phase::Abort,
        None,
        Some("too late".into()),
        None,
    )
    .await
    .unwrap();
    assert_eq!(raced.phase, Phase::Commit);
    recover(&app).await.unwrap();
    assert_eq!(read(&app).await, 7);
    let status = local_status(&app, &committed).await.unwrap();
    assert_eq!(status.phase, Phase::Commit);
    assert!(status.complete);
    assert!(
        app.consensus
            .read_for_writer()
            .await
            .unwrap()
            .requests
            .get("decided")
            .is_some()
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn abort_before_prepare_fences_delayed_prepare_without_an_extra_write() {
    let (_directory, app) = application().await;
    let reference = begin(
        &app,
        "early-abort",
        json!([{"group":"local","method":"add","args":6}]),
    )
    .await;
    decide(
        &app,
        &reference,
        Phase::Abort,
        None,
        Some("prepare RPC was uncertain".into()),
        None,
    )
    .await
    .unwrap();
    let revision = app.consensus.read_for_writer().await.unwrap().revision;
    finish(&app, &reference).await.unwrap();
    assert_eq!(
        app.consensus.read_for_writer().await.unwrap().revision,
        revision,
        "an unprepared abort needs no participant revision"
    );
    assert!(prepare(&app, &reference).await.is_err());
    assert_eq!(read(&app).await, 0);
    let nested = execute(
        app.clone(),
        json!({"name":"tx","requestId":"nested","args":{"calls":[
            {"group":"local","method":"tx","args":{"calls":[]}}
        ]}}),
    )
    .await
    .unwrap_err();
    assert_eq!(nested.code, "TRANSACTION_ABORTED");
    assert!(nested.message.contains("nested transactions"));
    app.consensus.shutdown().await.unwrap();
}

#[test]
fn revision_reservations_allow_finishing_but_fence_unrelated_writes() {
    let reference = Reference {
        history: "01".repeat(16),
        sequence: 1,
        coordinator: "local".into(),
        request_id: "last".into(),
        fingerprint: "last".into(),
    };
    let mut coordinator = Coordinator {
        history: reference.history.clone(),
        sequence: reference.sequence,
        receipt_reservation: 0,
        principal: Value::Null,
        coordinator: "local".into(),
        fingerprint: "last".into(),
        calls: vec![Call {
            group: "local".into(),
            method: "add".into(),
            args: "1".into(),
        }],
        value: None,
        phase: Phase::Preparing,
        results: None,
        reason: None,
        failure: None,
        complete: false,
    };
    let mut state = Snapshot {
        revision: MAX_REVISION - 5,
        ..Default::default()
    };
    let mut begin = metadata_command(&reference, &coordinator);
    begin.expected_revision = state.revision;
    ensure_commit_capacity(&state, &mut begin).unwrap();
    writer::stage(&mut state, &begin).unwrap();
    assert_eq!(state.data[RESERVED_REVISIONS], 4);
    assert_eq!(
        ensure_write_capacity(&state).unwrap_err().code,
        "REVISION_EXHAUSTED"
    );
    let prepared = Prepared {
        transaction: reference.clone(),
        patch: "{}".into(),
        results: "[]".into(),
    };
    let mut preparation = Commit {
        puts: BTreeMap::from([(PARTICIPANT.into(), json!(prepared))]),
        expected_revision: state.revision,
        ..begin.clone()
    };
    ensure_commit_capacity(&state, &mut preparation).unwrap();
    writer::stage(&mut state, &preparation).unwrap();
    assert_eq!(state.data[RESERVED_REVISIONS], 3);
    coordinator.phase = Phase::Commit;
    let mut decision = metadata_command(&reference, &coordinator);
    decision.expected_revision = state.revision;
    ensure_commit_capacity(&state, &mut decision).unwrap();
    writer::stage(&mut state, &decision).unwrap();
    assert_eq!(state.data[RESERVED_REVISIONS], 2);
    let mut finish = Commit {
        expected_revision: state.revision,
        puts: BTreeMap::from([(
            done_key(&reference),
            json!(Done {
                transaction: reference.clone(),
                phase: Phase::Commit,
                results: Some("[]".into())
            }),
        )]),
        deletes: vec![PARTICIPANT.into()],
        ..begin.clone()
    };
    ensure_commit_capacity(&state, &mut finish).unwrap();
    writer::stage(&mut state, &finish).unwrap();
    assert_eq!(state.data[RESERVED_REVISIONS], 1);
    coordinator.complete = true;
    let mut complete = metadata_command(&reference, &coordinator);
    complete.expected_revision = state.revision;
    ensure_commit_capacity(&state, &mut complete).unwrap();
    writer::stage(&mut state, &complete).unwrap();
    assert_eq!(state.revision, MAX_REVISION);
    assert_eq!(state.data[RESERVED_REVISIONS], 0);
    let mut too_late = begin;
    assert_eq!(
        ensure_commit_capacity(&state, &mut too_late)
            .unwrap_err()
            .code,
        "REVISION_EXHAUSTED"
    );
}

#[tokio::test]
async fn rpc_authentication_checks_group_and_compatibility_contract() {
    let (_directory, app) = application().await;
    let reference = Reference {
        history: "01".repeat(16),
        sequence: 1,
        coordinator: "local".into(),
        request_id: "test".into(),
        fingerprint: "test".into(),
    };
    let mut request = Request {
        group: "local".into(),
        transaction: reference,
    };
    let mut headers = HeaderMap::new();
    assert_eq!(
        authenticate_headers(&app, &headers).unwrap_err().status,
        StatusCode::UNAUTHORIZED
    );
    headers.insert(
        "authorization",
        "Bearer transaction-test-secret".parse().unwrap(),
    );
    assert!(authenticate_headers(&app, &headers).is_err());
    headers.insert(
        COMPATIBILITY_HEADER,
        crate::consensus::compatibility()
            .contract()
            .parse()
            .unwrap(),
    );
    authenticate_headers(&app, &headers).unwrap();
    request.group = "another".into();
    assert!(validate_target(&app, &request).is_err());
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn remote_participant_stays_locked_without_coordinator_then_recovers_durable_decision() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (_directory_a, mut a) = application().await;
    let (_directory_b, mut b) = application().await;
    let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let groups = BTreeMap::from([
        (
            "a".to_owned(),
            vec![listener_a.local_addr().unwrap().to_string()],
        ),
        (
            "b".to_owned(),
            vec![listener_b.local_addr().unwrap().to_string()],
        ),
    ]);
    for (app, name) in [(&mut a, "a"), (&mut b, "b")] {
        let runtime = &mut Arc::get_mut(app).unwrap().cross_group;
        runtime.group = Some(name.into());
        runtime.groups = groups.clone();
    }
    let blocked = Arc::new(AtomicBool::new(false));
    let paused = blocked.clone();
    let router_b = router()
        .with_state(b.clone())
        .layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let paused = paused.clone();
                async move {
                    if paused.load(Ordering::SeqCst) {
                        StatusCode::SERVICE_UNAVAILABLE.into_response()
                    } else {
                        next.run(request).await
                    }
                }
            },
        ));
    let router_a = router().with_state(a.clone());
    let server_a = tokio::spawn(async move {
        axum::serve(listener_a, router_a).await.unwrap();
    });
    let server_b = tokio::spawn(async move {
        axum::serve(listener_b, router_b).await.unwrap();
    });
    let mut references = Vec::new();
    for (id, commit, expected) in [("lost-prepare-ack", false, 0), ("durable-commit", true, 4)] {
        let reference = begin(&b, id, json!([{"group":"a","method":"add","args":4}])).await;
        references.push(reference.clone());
        contact(&b, &"a".into(), "prepare", &reference)
            .await
            .unwrap();
        assert_eq!(read(&a).await, 0);
        if commit {
            decide(
                &b,
                &reference,
                Phase::Commit,
                Some("[4]".into()),
                None,
                None,
            )
            .await
            .unwrap();
        }
        // The participant must neither read hidden state nor invent an abort
        // when the durable coordinator status cannot be reached.
        blocked.store(true, Ordering::SeqCst);
        assert!(finish(&a, &reference).await.is_err());
        assert!(ensure_unlocked(&a.consensus.read_for_writer().await.unwrap()).is_err());
        assert_eq!(read(&a).await, 0);
        blocked.store(false, Ordering::SeqCst);
        recover(&b).await.unwrap();
        assert!(ensure_unlocked(&a.consensus.read_for_writer().await.unwrap()).is_ok());
        assert_eq!(read(&a).await, expected);
        assert_eq!(
            local_status(&b, &reference).await.unwrap().phase,
            if commit { Phase::Commit } else { Phase::Abort }
        );
        let revision = a.consensus.read_for_writer().await.unwrap().revision;
        contact(&b, &"a".into(), "finish", &reference)
            .await
            .unwrap();
        assert_eq!(
            a.consensus.read_for_writer().await.unwrap().revision,
            revision
        );
    }
    // Completed aborts cannot be forgotten until legacy request IDs have been
    // fenced. Then unavailable quorum proofs leave a recoverable durable intent.
    let state = b.consensus.read_for_writer().await.unwrap();
    b.consensus
        .control_retention(crate::consensus::retention::initialize(state.revision, None).unwrap())
        .await
        .unwrap();
    blocked.store(true, Ordering::SeqCst);
    let pending = administer(&b, json!({"operation":"close"})).await.unwrap();
    assert_eq!(pending["value"]["closedThrough"], 0);
    assert_eq!(pending["value"]["pending"]["through"], 2);
    assert!(
        pending["value"]["blockedReason"]
            .as_str()
            .unwrap()
            .contains("participant")
    );
    assert_eq!(
        administer(&b, json!({"operation":"collect"}))
            .await
            .unwrap()["value"]["deletedRecords"],
        0
    );
    blocked.store(false, Ordering::SeqCst);
    recover(&b).await.unwrap();
    assert_eq!(
        administer(&b, json!({"operation":"status"})).await.unwrap()["value"]["closedThrough"],
        2
    );
    administer(&b, json!({"operation":"collect"}))
        .await
        .unwrap();
    administer(&a, json!({"operation":"collect"}))
        .await
        .unwrap();
    for reference in references {
        assert_eq!(
            prepare(&a, &reference).await.unwrap_err().code,
            "TRANSACTION_CLOSED"
        );
        assert_eq!(
            finish(&a, &reference).await.unwrap_err().code,
            "TRANSACTION_CLOSED"
        );
    }
    server_a.abort();
    server_b.abort();
    a.consensus.shutdown().await.unwrap();
    b.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn decisions_reach_participants_concurrently() {
    let (_directory_a, mut a) = application().await;
    let (_directory_b, mut b) = application().await;
    let (_directory_c, mut c) = application().await;
    let listener_a = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_b = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener_c = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let groups = BTreeMap::from([
        (
            "a".to_owned(),
            vec![listener_a.local_addr().unwrap().to_string()],
        ),
        (
            "b".to_owned(),
            vec![listener_b.local_addr().unwrap().to_string()],
        ),
        (
            "c".to_owned(),
            vec![listener_c.local_addr().unwrap().to_string()],
        ),
    ]);
    for (app, name) in [(&mut a, "a"), (&mut b, "b"), (&mut c, "c")] {
        let runtime = &mut Arc::get_mut(app).unwrap().cross_group;
        runtime.group = Some(name.into());
        runtime.groups = groups.clone();
    }
    // Participant a, first in target order, receives its decision only once
    // the test releases it.
    let release = Arc::new(Semaphore::new(0));
    let held = release.clone();
    let router_a = router()
        .with_state(a.clone())
        .layer(axum::middleware::from_fn(
            move |request: axum::extract::Request, next: axum::middleware::Next| {
                let held = held.clone();
                async move {
                    if request.uri().path() == "/raft/transactions/finish" {
                        held.acquire().await.unwrap().forget();
                    }
                    next.run(request).await
                }
            },
        ));
    let mut servers = Vec::new();
    for (listener, router) in [
        (listener_a, router_a),
        (listener_b, router().with_state(b.clone())),
        (listener_c, router().with_state(c.clone())),
    ] {
        servers.push(tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        }));
    }
    let execution = tokio::spawn(execute(
        b.clone(),
        json!({"name":"tx","requestId":"fanout","args":{"calls":[
            {"group":"a","method":"add","args":4},
            {"group":"c","method":"add","args":5}
        ]}}),
    ));
    // c commits and unlocks while a still awaits the same decision.
    tokio::time::timeout(Duration::from_secs(5), async {
        while read(&c).await != json!(5)
            || ensure_unlocked(&c.consensus.read_for_writer().await.unwrap()).is_err()
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a held decision must not delay other participants");
    assert!(ensure_unlocked(&a.consensus.read_for_writer().await.unwrap()).is_err());
    assert_eq!(read(&a).await, 0);
    release.add_permits(1);
    let response = tokio::time::timeout(Duration::from_secs(5), execution)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(response["value"]["results"], json!([4, 5]));
    assert_eq!(read(&a).await, 4);
    assert!(ensure_unlocked(&a.consensus.read_for_writer().await.unwrap()).is_ok());
    for server in servers {
        server.abort();
    }
    for app in [a, b, c] {
        app.consensus.shutdown().await.unwrap();
    }
}

#[test]
fn plans_preserve_explicit_null_values() {
    let groups = BTreeMap::from([("local".into(), vec!["localhost:7101".into()])]);
    let record = plan(
        "local",
        json!({"calls":[{"group":"local","method":"read"}],"value":null}),
        "test".into(),
        &groups,
    )
    .unwrap();
    assert_eq!(record.value.as_deref(), Some("null"));
}

#[test]
fn begin_budget_must_also_fit_a_durable_abort() {
    let reference = Reference {
        history: "01".repeat(16),
        sequence: 1,
        coordinator: "local".into(),
        request_id: "budget".into(),
        fingerprint: "budget".into(),
    };
    let groups = BTreeMap::from([("local".into(), vec!["localhost:7101".into()])]);
    let mut record = plan(
        "local",
        json!({"calls":[{"group":"local","method":"read"}]}),
        "budget".into(),
        &groups,
    )
    .unwrap();
    record.history = reference.history.clone();
    record.sequence = reference.sequence;
    let mut command = metadata_command(&reference, &record);
    closure::index_envelope(&mut command).unwrap();
    command.puts.insert(
        crate::consensus::retention::RESERVED_BYTES.into(),
        json!(u64::MAX),
    );
    command
        .puts
        .insert(RESERVED_REVISIONS.into(), json!(MAX_REVISION));
    let begin_bytes = serde_json::to_vec(&command).unwrap().len();
    check_command_limit(&command, begin_bytes).unwrap();
    record.phase = Phase::Abort;
    record.reason = Some(ABORT_REASON.into());
    let abort = metadata_command(&reference, &record);
    assert_eq!(
        check_command_limit(&abort, begin_bytes).unwrap_err().code,
        "TRANSACTION_TOO_LARGE",
        "admission must reserve the terminal envelope even when begin fits exactly"
    );
    // Completion removes its active index and can be smaller than admission;
    // the pending abort envelope above is the one that must be reserved.
}

#[tokio::test]
async fn empty_plan_is_a_valid_durable_noop() {
    let (_directory, app) = application().await;
    let result = execute(
        app.clone(),
        json!({"name":"tx","requestId":"noop","args":{"calls":[],"value":null}}),
    )
    .await
    .unwrap();
    assert_eq!(result["value"], json!({"results":[],"value":null}));
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn aggregate_evaluation_deadline_discards_every_staged_call() {
    let (_directory, app) = application().await;
    let state = app.consensus.read_for_writer().await.unwrap();
    // Every callback is individually valid. A shared short test deadline must
    // still bound a large finite plan, rather than grant each call a new window.
    evaluator::invoke_at(
        state.data,
        json!({"name":"add","args":1,"requestId":"probe"}),
        "mutation",
        1000,
    )
    .unwrap();
    let calls: Vec<_> = (0..1024)
        .map(|_| json!({"group":"local","method":"add","args":1}))
        .collect();
    let reference = begin(&app, "aggregate-budget", json!(calls)).await;
    let error = prepare_with_budget(&app, &reference, Duration::from_millis(10))
        .await
        .unwrap_err();
    assert_eq!(error.code, "EVALUATION_FAILED");
    assert!(error.message.contains("EVALUATION_BUDGET"));
    assert_eq!(read(&app).await, 0);
    assert!(ensure_unlocked(&app.consensus.read_for_writer().await.unwrap()).is_ok());
    recover(&app).await.unwrap();
    assert_eq!(
        local_status(&app, &reference).await.unwrap().phase,
        Phase::Abort
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn unauthenticated_malformed_body_is_rejected_before_json_parsing() {
    let (_directory, app) = application().await;
    let request = axum::http::Request::builder()
        .header("content-type", "application/json")
        .body(axum::body::Body::from("not-json"))
        .unwrap();
    assert_eq!(
        authenticated_request(&app, request)
            .await
            .unwrap_err()
            .status,
        StatusCode::UNAUTHORIZED
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn independent_coordinators_do_not_share_a_global_execution_lock() {
    let (_directory, app) = application().await;
    let held = app.cross_group.coordinator_for("held").await;
    let _held = held.lock().await;
    let other = app.cross_group.coordinator_for("other").await;
    assert!(other.try_lock().is_ok());
    assert!(
        app.cross_group
            .coordinator_for("held")
            .await
            .try_lock()
            .is_err()
    );
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        execute(
            app.clone(),
            json!({"name":"tx","args":{"calls":[]},"requestId":"other"}),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result["value"]["results"], json!([]));
    app.consensus.shutdown().await.unwrap();
}

#[test]
fn logical_targets_are_exclusive_and_resolved_before_durable_preparation() {
    let groups = BTreeMap::from([("local".into(), vec!["127.0.0.1:1".into()])]);
    assert!(
        plan(
            "local",
            json!({"calls":[{"group":"local","partition":"tenant","method":"add"}]}),
            "x".into(),
            &groups
        )
        .is_err()
    );
    assert!(
        plan(
            "local",
            json!({"calls":[{"method":"add"}]}),
            "x".into(),
            &groups
        )
        .is_err()
    );
    let pending = plan(
        "local",
        json!({"calls":[{"partition":"tenant","method":"add"}]}),
        "x".into(),
        &groups,
    )
    .unwrap();
    assert_eq!(pending.calls[0].group.partition.as_deref(), Some("tenant"));
    assert_eq!(pending.calls[0].group.epoch, 0);
}

async fn stored_reference(app: &App, id: &str) -> Reference {
    let state = app.consensus.read_for_writer().await.unwrap();
    let record: Coordinator = decode(&state.data[&coordinator_key(id)]).unwrap();
    Reference {
        history: record.history,
        sequence: record.sequence,
        coordinator: record.coordinator,
        request_id: id.into(),
        fingerprint: record.fingerprint,
    }
}

#[tokio::test]
async fn closure_empty_plan_preserves_retry_receipt_after_collection() {
    let (_directory, app) = application().await;
    let input = json!({"name":"tx","requestId":"closed-noop","args":{"calls":[],"value":"kept"}});
    let original = execute(app.clone(), input.clone()).await.unwrap();
    let response = administer(&app, json!({"operation":"close"}))
        .await
        .unwrap();
    assert_eq!(response["value"]["closedThrough"], 1);
    assert!(response["value"]["pending"].is_null());
    let gc = administer(&app, json!({"operation":"collect"}))
        .await
        .unwrap();
    assert_eq!(gc["value"]["deletedRecords"], 2);
    let state = app.consensus.read_for_writer().await.unwrap();
    assert!(!state.data.contains_key(&coordinator_key("closed-noop")));
    assert!(closure::active_records(&state).next().is_none());
    let replay = execute(app.clone(), input).await.unwrap();
    assert_eq!(replay["value"], original["value"]);
    assert_eq!(replay["revision"], original["revision"]);
    assert_eq!(replay["duplicate"], true);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn closure_floors_reject_late_prepare_and_finish_after_decision_deletion() {
    let (_directory, app) = application().await;
    let input = json!({"name":"tx","requestId":"closed-write","args":{"calls":[{"group":"local","method":"add","args":7}]}});
    execute(app.clone(), input).await.unwrap();
    let reference = stored_reference(&app, "closed-write").await;
    assert!(
        app.consensus
            .read_for_writer()
            .await
            .unwrap()
            .data
            .contains_key(&done_key(&reference))
    );
    administer(&app, json!({"operation":"close"}))
        .await
        .unwrap();
    administer(&app, json!({"operation":"collect"}))
        .await
        .unwrap();
    let state = app.consensus.read_for_writer().await.unwrap();
    assert!(!state.data.contains_key(&coordinator_key("closed-write")));
    assert!(!state.data.contains_key(&done_key(&reference)));
    assert_eq!(
        prepare(&app, &reference).await.unwrap_err().code,
        "TRANSACTION_CLOSED"
    );
    assert_eq!(
        finish(&app, &reference).await.unwrap_err().code,
        "TRANSACTION_CLOSED"
    );
    let recovered: Snapshot = serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
    assert_eq!(
        closure::participant_open(&recovered, &reference)
            .unwrap_err()
            .code,
        "TRANSACTION_CLOSED"
    );
    assert_eq!(read(&app).await, 7);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn closure_aborts_need_permanent_retry_admission_fence() {
    let (_directory, app) = application().await;
    let input = json!({"name":"tx","requestId":"closed-abort","args":{"calls":[{"group":"local","method":"fail"}]}});
    assert_eq!(
        execute(app.clone(), input.clone()).await.unwrap_err().code,
        "TRANSACTION_ABORTED"
    );
    let reference = stored_reference(&app, "closed-abort").await;
    let blocked = administer(&app, json!({"operation":"close"}))
        .await
        .unwrap();
    assert_eq!(blocked["value"]["closedThrough"], 0);
    assert!(
        blocked["value"]["blockedReason"]
            .as_str()
            .unwrap()
            .contains("remains admissible")
    );
    let state = app.consensus.read_for_writer().await.unwrap();
    app.consensus
        .control_retention(crate::consensus::retention::initialize(state.revision, None).unwrap())
        .await
        .unwrap();
    let closed = administer(&app, json!({"operation":"close"}))
        .await
        .unwrap();
    assert_eq!(closed["value"]["closedThrough"], 1);
    administer(&app, json!({"operation":"collect"}))
        .await
        .unwrap();
    assert_eq!(
        execute(app.clone(), input).await.unwrap_err().code,
        "REQUEST_ID_SCOPE_REQUIRED"
    );
    assert_eq!(
        prepare(&app, &reference).await.unwrap_err().code,
        "TRANSACTION_CLOSED"
    );
    assert_eq!(
        finish(&app, &reference).await.unwrap_err().code,
        "TRANSACTION_CLOSED"
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn closure_collection_cursor_progresses_past_unclosed_histories() {
    let (_directory, app) = application().await;
    let mut puts = BTreeMap::new();
    let mut last = String::new();
    for number in 1..=12 {
        let reference = Reference {
            history: format!("{number:032x}"),
            sequence: 1,
            coordinator: "elsewhere".into(),
            request_id: "x".into(),
            fingerprint: "x".into(),
        };
        last = done_key(&reference);
        puts.insert(
            last.clone(),
            json!(Done {
                transaction: reference.clone(),
                phase: Phase::Abort,
                results: None
            }),
        );
        if number == 12 {
            puts.insert(format!("transaction:closed:{}",reference.history),json!({"coordinator":reference.coordinator,"history":reference.history,"through":1}));
        }
    }
    let state = app.consensus.read_for_writer().await.unwrap();
    persist(&app, &state, puts, vec![]).await.unwrap();
    for _ in 0..20 {
        administer(&app, json!({"operation":"collect","maxBytes":600}))
            .await
            .unwrap();
        if !app
            .consensus
            .read_for_writer()
            .await
            .unwrap()
            .data
            .contains_key(&last)
        {
            break;
        }
    }
    let state = app.consensus.read_for_writer().await.unwrap();
    assert!(
        !state.data.contains_key(&last),
        "bounded cursor must eventually reach the final history"
    );
    assert_eq!(
        state
            .data
            .iter()
            .filter(|(key, _)| key.starts_with(COMPLETED))
            .count(),
        11,
        "unclosed participant outcomes remain retained"
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn staged_and_transaction_coordinators_reserve_request_identity_in_both_directions() {
    let (_directory, app) = application().await;
    let stage = |id: &str| json!({"operation":"stage","requestId":id,"bundle":{"hash":evaluator::hash(JAVASCRIPT.as_bytes()),"javascript":JAVASCRIPT}});
    super::super::staged_deployment::administer(&app, stage("stage-owned"))
        .await
        .unwrap();
    assert_eq!(
        execute(
            app.clone(),
            json!({"name":"tx","requestId":"stage-owned","args":{"calls":[]}})
        )
        .await
        .unwrap_err()
        .code,
        "REQUEST_ID_REUSED"
    );
    super::super::staged_deployment::administer(
        &app,
        json!({"operation":"cancel","requestId":"stage-owned"}),
    )
    .await
    .unwrap();
    super::super::staged_deployment::administer(
        &app,
        json!({"operation":"collect","requestId":"stage-owned"}),
    )
    .await
    .unwrap();
    let reference = begin(&app, "coordinator-owned", json!([])).await;
    assert_eq!(
        super::super::staged_deployment::administer(&app, stage("coordinator-owned"))
            .await
            .unwrap_err()
            .code,
        "REQUEST_ID_REUSED"
    );
    let current = decide(
        &app,
        &reference,
        Phase::Abort,
        None,
        Some("test cancellation".into()),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        finish_coordinator(&app, &reference, &current)
            .await
            .unwrap_err()
            .code,
        "TRANSACTION_ABORTED"
    );
    app.consensus.shutdown().await.unwrap();
}
