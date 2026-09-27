use super::*;
use std::collections::BTreeMap;
use tempfile::TempDir;
use tokio::task::JoinHandle;

fn application_bundle(expose_increment: bool) -> String {
    let aliases = if expose_increment {
        "'counter.add':{name:'increment',kind:'mutation'},"
    } else {
        ""
    };
    format!(
        r#"
        const records = {{kind:'collection',name:'records'}};
        const doubled = {{kind:'derived',name:'doubled'}};
        let calls = 0;
        var __flowerBundle = {{default:{{definitions:{{
            increment:{{name:'increment',kind:'mutationMethod',compute:(ctx,args)=>{{
                const previous = ctx.get(records,'value') || 0;
                ctx.set(records,'value',previous + args.by);
                return {{previous,value:ctx.get(records,'value'),doubled:ctx.get(doubled),calls:++calls}};
            }}}},
            doubled:{{name:'doubled',kind:'derived',compute:(ctx)=>2*(ctx.get(records,'value') || 0)}},
            fail:{{name:'fail',kind:'mutationMethod',compute:(ctx)=>{{
                ctx.set(records,'value',999); ctx.set(records,'partial',true);
                throw new Error('deliberate method failure');
            }}}},
            read:{{name:'read',kind:'queryMethod',compute:(ctx)=>ctx.get(records,'value')}}
        }},http:{{{aliases}'counter.fail':{{name:'fail',kind:'mutation'}},'counter.read':{{name:'read',kind:'query'}}}}}}}};
        "#,
    )
}

struct Fixture {
    _directory: TempDir,
    app: Arc<App>,
    worker: Option<JoinHandle<()>>,
    receiver: Option<mpsc::Receiver<Pending>>,
}

impl Fixture {
    async fn new(capacity: usize, run_worker: bool) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let address = "127.0.0.1:7101".to_owned();
        let consensus = Consensus::open(
            1,
            address.clone(),
            directory.path().into(),
            "test-only-secret".into(),
        )
        .await
        .unwrap();
        consensus
            .initialize(BTreeMap::from([(1, address)]))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            while consensus.read().await.is_err() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let javascript = application_bundle(true);
        let evaluation = evaluator::evaluate_at(
            BTreeMap::new(),
            json!({
                "requestId":"seed",
                "bundle":{"hash":evaluator::hash(javascript.as_bytes()),"javascript":javascript},
                "writes":[{"collection":"records","key":"value","value":0}],
                "materialize":[{"name":"doubled","args":null}],
            }),
            1000,
        )
        .unwrap();
        consensus
            .commit(Commit {
                internal: false,
                request_id: "seed".into(),
                fingerprint: "seed".into(),
                expected_revision: 0,
                puts: evaluation.puts,
                deletes: evaluation.deletes,
                result: Value::Null,
            })
            .await
            .unwrap();
        let (writer_queue, receiver) = mpsc::channel(capacity);
        let app = Arc::new(App {
            consensus,
            writer: Mutex::new(()),
            writer_queue,
            evaluations: Arc::new(Semaphore::new(1)),
            query_evaluations: Arc::new(Semaphore::new(4)),
            admission: crate::service::admission::Pool::configured().unwrap(),
            query_cache: query_cache::QueryCache::default(),
            watch_hubs: crate::service::watch::hubs::Registry::default(),
            admin_token: "test-only-secret".into(),
            clock: clock::Clock::new(),
            cross_group: transactions::Runtime::new().unwrap(),
            partition_gate: None,
        });
        let (worker, receiver) = if run_worker {
            (
                Some(tokio::spawn(run(Arc::downgrade(&app), receiver))),
                None,
            )
        } else {
            (None, Some(receiver))
        };
        Self {
            _directory: directory,
            app,
            worker,
            receiver,
        }
    }

    async fn close(mut self) {
        self.app.consensus.shutdown().await.unwrap();
        self.receiver.take();
        drop(self.app);
        if let Some(worker) = self.worker {
            tokio::time::timeout(Duration::from_secs(2), worker)
                .await
                .unwrap()
                .unwrap();
        }
    }
}

fn increment(request_id: &str, by: i32, revision: Option<u64>) -> Value {
    let mut input = json!({"requestId":request_id,"name":"counter.add","args":{"by":by}});
    if let Some(revision) = revision {
        input["expectedRevision"] = json!(revision);
    }
    input
}

fn deployment(request_id: &str, expose: bool) -> Value {
    let javascript = application_bundle(expose);
    json!({"requestId":request_id,"bundle":{"hash":evaluator::hash(javascript.as_bytes()),"javascript":javascript}})
}

#[test]
fn borrowed_commit_verification_skips_errors_replays_and_rejects_divergent_acks() {
    fn response(revision: u64, value: Value, duplicate: bool) -> Result<Prepared, ApiError> {
        Ok(Prepared {
            response: json!({"revision":revision,"value":value,"duplicate":duplicate}),
            command: None,
            permit_wait_us: 0,
            evaluation_us: 0,
            _retained: None,
        })
    }
    let first = json!({"array":[1,"🌸",null],"key":"value"});
    let second = json!([true, 7]);
    let prepared = [
        response(9, first.clone(), false),
        Err(ApiError::new(
            StatusCode::CONFLICT,
            "REVISION_CONFLICT",
            "expected failure".into(),
        )),
        response(3, json!("historical"), true),
        response(10, second.clone(), false),
    ];
    let commit = |revision, result, duplicate| {
        ApplyResult::Committed(crate::consensus::CommitResult {
            revision,
            result,
            duplicate,
        })
    };
    let exact = || {
        vec![
            commit(9, first.clone(), false),
            commit(10, second.clone(), false),
        ]
    };
    assert!(committed_matches(&exact(), 2, &prepared));
    assert!(!committed_matches(&exact(), 1, &prepared));
    assert!(!committed_matches(&exact()[..1], 1, &prepared));
    assert!(!committed_matches(&exact(), 2, &prepared[..3]));
    let mut changed = exact();
    changed[0] = commit(9, json!("wrong result"), false);
    assert!(!committed_matches(&changed, 2, &prepared));
    changed[0] = commit(8, first.clone(), false);
    assert!(!committed_matches(&changed, 2, &prepared));
    changed[0] = commit(9, first, true);
    assert!(!committed_matches(&changed, 2, &prepared));
    assert!(!committed_matches(
        &[ApplyResult::Internal],
        1,
        &prepared[..1]
    ));
    assert!(committed_matches(&[], 0, &prepared[1..3]));
}

#[tokio::test]
async fn early_drain_runs_maintenance_before_saturated_queues_without_a_timer_wakeup() {
    use futures_util::FutureExt;

    fn pending(id: &str) -> Pending {
        let (reply, _receive) = oneshot::channel();
        Pending {
            input: increment(id, 1, None).into(),
            deployment: false,
            enqueued: Instant::now(),
            reply,
        }
    }
    let (sender, mut receiver) = mpsc::channel(2);
    sender
        .try_send(pending("queued-first"))
        .unwrap_or_else(|_| panic!("test queue has sufficient capacity"));
    sender
        .try_send(pending("queued-second"))
        .unwrap_or_else(|_| panic!("test queue has sufficient capacity"));
    let mut deferred = VecDeque::from([pending("deferred-first")]);
    // Exercise the production actor's selection with maintenance scheduled a
    // minute out. Early drains must not rely on another poll or a clock tick.
    let mut progress = None;
    let later = Some(Instant::now() + Duration::from_secs(60));
    for _ in 0..3 {
        assert!(matches!(
            next_work(&mut receiver, &mut deferred, &mut progress, later, true, true).now_or_never(),
            Some(Work::Maintenance)
        ));
        assert_eq!(receiver.len(), 2);
        assert_eq!(deferred.len(), 1);
    }
    // Clearing the serviced intent admits customers in their original order;
    // explicitly disabled automatic maintenance also ignores a drain intent.
    for (id, due, enabled) in [
        ("deferred-first", false, true),
        ("queued-first", false, true),
        ("queued-second", true, false),
    ] {
        let Some(Work::Request(request)) =
            next_work(&mut receiver, &mut deferred, &mut progress, later, due, enabled).now_or_never()
        else {
            panic!("queued customer work must be ready after maintenance");
        };
        assert_eq!(request.input.request_id().as_deref(), Some(id));
    }
    drop(sender);
    assert!(matches!(
        next_work(&mut receiver, &mut deferred, &mut progress, later, false, true).now_or_never(),
        Some(Work::Closed)
    ));
}

fn staging_policy() -> crate::consensus::retention::State {
    crate::consensus::retention::State {
        database: "a".repeat(32),
        incarnation: "b".repeat(32),
        current_epoch: 0,
        min_epoch: 0,
        receipt_bytes: 0,
        receipt_count: 0,
        session_bytes: 0,
        session_count: 0,
        max_receipt_bytes: None,
        gc_cursor: None,
        gc_complete: false,
        gc_receipts_complete: false,
        gc_session_cursor: None,
        rotation: None,
    }
}

fn staging_command(request_id: String, value: Value) -> Commit {
    Commit {
        internal: false,
        request_id,
        fingerprint: "fingerprint".into(),
        expected_revision: 0,
        puts: BTreeMap::from([("value".into(), value.clone())]),
        deletes: vec!["old".into()],
        result: value,
    }
}

#[test]
fn staging_repeated_keys_preserves_older_snapshots_with_and_without_retention() {
    use crate::consensus::retention as protocol;
    for retained in [false, true] {
        let policy = staging_policy();
        let mut state = Snapshot::default();
        state.data.insert("value".into(), json!(0));
        state.data.insert("old".into(), json!(true));
        if retained {
            state.data.insert(protocol::KEY.into(), json!(policy));
        }
        let mut earlier = Vec::new();
        for counter in 1..=3 {
            earlier.push(state.clone());
            let id = if retained {
                protocol::scope_request_id(&policy, &counter.to_string())
            } else {
                counter.to_string()
            };
            let mut command = staging_command(id.clone(), json!(counter));
            command.deletes.push("value".into()); // Puts win after deletes.
            stage(&mut state, &command).unwrap();
            assert_eq!(state.data["value"], counter);
            assert_eq!(state.requests[&id].revision, counter);
            assert_eq!(state.requests[&id].result, counter);
            assert_eq!(state.revision, counter);
            assert!(!state.data.contains_key("old"));
        }
        for (index, previous) in earlier.iter().enumerate() {
            assert_eq!(previous.revision, index as u64);
            assert_eq!(previous.data["value"], index);
            assert_eq!(previous.requests.len(), index);
            assert_eq!(previous.data.contains_key("old"), index == 0);
        }
        if retained {
            let accounting = protocol::status(&state).unwrap().unwrap();
            let expected: u64 = state
                .requests
                .iter()
                .map(|(id, receipt)| protocol::receipt_bytes(id, receipt).unwrap())
                .sum();
            assert_eq!(accounting.receipt_count, 3);
            assert_eq!(accounting.receipt_bytes, expected);
        }
    }
}

#[test]
fn staging_failure_is_atomic_for_retained_and_initially_unretained_state() {
    use crate::consensus::retention as protocol;
    for retained in [false, true] {
        let mut policy = staging_policy();
        policy.max_receipt_bytes = Some(0);
        let mut state = Snapshot::default();
        state.data.insert("value".into(), json!("before"));
        state.data.insert("old".into(), json!(true));
        if retained {
            state.data.insert(protocol::KEY.into(), json!(policy));
        }
        let before = state.clone();
        let mut command = staging_command(
            protocol::scope_request_id(&policy, "too-large"),
            json!("after"),
        );
        // Initializing retention within the overlay must be charged too.
        command.puts.insert(protocol::KEY.into(), json!(policy));
        assert_eq!(
            stage(&mut state, &command).unwrap_err().code,
            "RECEIPT_BUDGET_EXCEEDED"
        );
        assert_eq!(state, before);
        assert!(state.data.ptr_eq(&before.data));

        command
            .puts
            .insert(protocol::KEY.into(), json!({"invalid": true}));
        assert_eq!(
            stage(&mut state, &command).unwrap_err().code,
            "RETENTION_CONFLICT"
        );
        assert_eq!(state, before);
        assert!(state.data.ptr_eq(&before.data));
    }
}

#[test]
fn staging_accounting_observes_overlay_sessions_and_reservations() {
    use crate::consensus::retention as protocol;
    let mut policy = staging_policy();
    let mut state = Snapshot::default();
    state.data.insert(protocol::KEY.into(), json!(policy));
    state
        .data
        .insert(protocol::RESERVED_BYTES.into(), json!(u64::MAX));
    policy.current_epoch = 1;
    let mut session = protocol::Session {
        id: "c".repeat(32),
        incarnation: policy.incarnation.clone(),
        owner: "d".repeat(64),
        epoch: 1,
        acknowledged_through: 0,
        closed: false,
    };
    let session_key = protocol::session_key(&session.id);
    let id = protocol::session_request_id(&policy, &session, 1).unwrap();
    let mut command = staging_command(id, json!(1));
    command.puts.insert(protocol::KEY.into(), json!(policy));
    command.puts.insert(session_key.clone(), json!(session));
    command.deletes.extend([
        protocol::KEY.into(),
        session_key.clone(),
        protocol::RESERVED_BYTES.into(),
    ]);
    stage(&mut state, &command).unwrap();
    assert_eq!(protocol::status(&state).unwrap().unwrap().receipt_count, 1);
    assert!(!state.data.contains_key(protocol::RESERVED_BYTES));

    let before = state.clone();
    let id = protocol::session_request_id(&policy, &session, 2).unwrap();
    let mut command = staging_command(id, json!(2));
    session.acknowledged_through = 2;
    command.puts.insert(session_key.clone(), json!(session));
    assert_eq!(
        stage(&mut state, &command).unwrap_err().code,
        "ALREADY_ACKNOWLEDGED"
    );
    assert_eq!(state, before);
    command.puts.remove(&session_key);
    command.deletes.push(session_key);
    assert_eq!(
        stage(&mut state, &command).unwrap_err().code,
        "RETRY_SESSION_UNKNOWN"
    );
    assert_eq!(state, before);

    command.deletes.clear();
    command
        .puts
        .insert(protocol::RESERVED_BYTES.into(), json!(u64::MAX));
    assert_eq!(
        stage(&mut state, &command).unwrap_err().code,
        "RECEIPT_BUDGET_EXCEEDED"
    );
    assert_eq!(state, before);

    // Internal commits change metadata without creating or charging a receipt.
    command.internal = true;
    stage(&mut state, &command).unwrap();
    assert_eq!(state.requests, before.requests);
    assert_eq!(state.revision, before.revision + 1);
    assert_eq!(state.data[protocol::KEY], before.data[protocol::KEY]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn online_deployment_prepares_off_lane_conflicts_atomically_and_blocking_retry_replays() {
    let fixture = Fixture::new(16, true).await;
    let javascript = format!(
        r#"{}
        __flowerBundle.default.collections=[{{name:'orders',indexes:{{shop:['shop']}}}}];
        __flowerBundle.default.definitions.authorize={{kind:'queryMethod',name:'authorize',compute:()=>{{throw new Error('new authorization');}}}};
        __flowerBundle.default.authorize={{name:'authorize'}};
    "#,
        application_bundle(false)
    );
    let mut invocation = json!({"requestId":"online-cutover","bundle":{"hash":evaluator::hash(javascript.as_bytes()),"javascript":javascript},
        "writes":[{"collection":"orders","key":"one","value":{"shop":"north"}}]});
    let pending = PendingInput::from(invocation.clone());
    let before = fixture.app.consensus.read().await.unwrap();
    // Online work must finish even while the serial writer and its evaluation
    // semaphore are occupied. Existing durable code stays visible throughout.
    let writer = fixture.app.writer.lock().await;
    let evaluations = fixture
        .app
        .evaluations
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_secs(10),
            deployment::stage(&fixture.app, &pending)
        )
        .await
        .unwrap()
        .unwrap()
        .is_none()
    );
    assert_eq!(fixture.app.consensus.read().await.unwrap(), before);
    drop(evaluations);
    drop(writer);
    let old_write = submit(&fixture.app, increment("during-online", 3, None), false)
        .await
        .unwrap();
    assert_eq!(old_write["value"]["value"], 3);
    let changed = fixture.app.consensus.read().await.unwrap();
    let conflict = prepare_candidate(&fixture.app, &changed, &pending, true, None)
        .await
        .err()
        .unwrap();
    assert_eq!(conflict.code, "DEPLOYMENT_CONFLICT");
    assert_eq!(fixture.app.consensus.read().await.unwrap(), changed);
    assert!(
        !changed
            .data
            .keys()
            .any(|key| key.starts_with("index-entry:") || key.starts_with("ordered-entry:"))
    );
    // Scheduling mode is not part of the business fingerprint: the same ID
    // can choose an exclusive preparation window after a clean conflict.
    invocation["preparation"] = json!("blocking");
    let receipt = submit(&fixture.app, invocation.clone(), true)
        .await
        .unwrap();
    assert_eq!(receipt["duplicate"], false);
    let after = fixture.app.consensus.read().await.unwrap();
    assert_eq!(after.data["schema"]["indexes"][0]["collection"], "orders");
    // Scalar values have ordered entries only.
    assert_eq!(
        after
            .data
            .keys()
            .filter(|key| key.starts_with("index-entry:") || key.starts_with("ordered-entry:"))
            .count(),
        1
    );
    assert_eq!(
        http_method(&after, "counter.add", Some(MethodKind::Mutation))
            .err()
            .unwrap()
            .code,
        "METHOD_NOT_FOUND"
    );
    let denied = submit(
        &fixture.app,
        json!({"requestId":"new-policy","name":"counter.fail","args":null}),
        false,
    )
    .await
    .err()
    .unwrap();
    assert_eq!(denied.code, "FORBIDDEN");
    invocation["preparation"] = json!("online");
    let duplicate = submit(&fixture.app, invocation, true).await.unwrap();
    assert_eq!(duplicate["duplicate"], true);
    assert_eq!(duplicate["revision"], receipt["revision"]);
    assert_eq!(fixture.app.consensus.read().await.unwrap(), after);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn online_deployment_cutover_publishes_once_from_unchanged_base() {
    let fixture = Fixture::new(16, true).await;
    let canceled = PendingInput::from(deployment("online-canceled", false));
    deployment::stage(&fixture.app, &canceled).await.unwrap();
    assert!(canceled.state.lock().unwrap().staged.is_some());
    canceled.cancel();
    {
        let state = canceled.state.lock().unwrap();
        assert!(state.body.is_none());
        assert!(
            state.staged.is_none(),
            "Cancellation promptly releases queued candidate bytes"
        );
    }
    let input = deployment("online-success", false);
    let receipt = submit(&fixture.app, input.clone(), true).await.unwrap();
    assert_eq!(receipt["revision"], 2);
    assert_eq!(receipt["duplicate"], false);
    let duplicate = submit(&fixture.app, input, true).await.unwrap();
    assert_eq!(duplicate["revision"], 2);
    assert_eq!(duplicate["duplicate"], true);
    let state = fixture.app.consensus.read().await.unwrap();
    assert_eq!(
        http_method(&state, "counter.add", Some(MethodKind::Mutation))
            .err()
            .unwrap()
            .code,
        "METHOD_NOT_FOUND"
    );
    fixture.close().await;
}

async fn ordered(
    app: &Arc<App>,
    inputs: Vec<(Value, bool)>,
) -> Vec<oneshot::Receiver<Result<Value, ApiError>>> {
    // Hold the writer guard until every input is enqueued, making arrival order
    // deterministic while exercising the real background actor and its queue.
    let guard = app.writer.lock().await;
    let mut receivers = Vec::new();
    for (input, deployment) in inputs {
        let (reply, receive) = oneshot::channel();
        app.writer_queue
            .try_send(Pending {
                input: input.into(),
                deployment,
                enqueued: Instant::now(),
                reply,
            })
            .unwrap_or_else(|_| panic!("test queue has sufficient capacity"));
        receivers.push(receive);
    }
    for receive in &mut receivers {
        assert!(matches!(
            receive.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }
    drop(guard);
    receivers
}

async fn responses(
    receivers: Vec<oneshot::Receiver<Result<Value, ApiError>>>,
) -> Vec<Result<Value, ApiError>> {
    let mut results = Vec::new();
    for receiver in receivers {
        results.push(
            tokio::time::timeout(Duration::from_secs(10), receiver)
                .await
                .unwrap()
                .unwrap(),
        );
    }
    results
}

#[tokio::test]
async fn ordered_queue_stages_sources_derived_values_receipts_and_cas_without_error_leaks() {
    let fixture = Fixture::new(128, true).await;
    let first = increment("first", 1, Some(1));
    let second = increment("second", 2, Some(2));
    let before = fixture.app.consensus.metrics().last_applied.unwrap().index;
    let pending = ordered(
        &fixture.app,
        vec![
            (first.clone(), false),
            (first, false),
            (increment("first", 8, Some(1)), false),
            (
                json!({"requestId":"failed","name":"counter.fail","args":null}),
                false,
            ),
            (increment("stale", 10, Some(1)), false),
            (second.clone(), false),
            (second, false),
        ],
    )
    .await;
    let results = responses(pending).await;
    assert_eq!(
        results[0].as_ref().unwrap(),
        &json!({
            "revision":2,"value":{"previous":0,"value":1,"doubled":2,"calls":1},"duplicate":false,
        })
    );
    assert_eq!(results[1].as_ref().unwrap()["revision"], 2);
    assert_eq!(results[1].as_ref().unwrap()["duplicate"], true);
    assert_eq!(
        results[1].as_ref().unwrap()["value"],
        results[0].as_ref().unwrap()["value"]
    );
    assert_eq!(results[2].as_ref().unwrap_err().code, "REQUEST_ID_REUSED");
    assert_eq!(results[3].as_ref().unwrap_err().code, "EVALUATION_FAILED");
    assert_eq!(results[4].as_ref().unwrap_err().code, "REVISION_CONFLICT");
    assert_eq!(
        results[5].as_ref().unwrap(),
        &json!({
            "revision":3,"value":{"previous":1,"value":3,"doubled":6,"calls":1},"duplicate":false,
        })
    );
    assert_eq!(results[6].as_ref().unwrap()["revision"], 3);
    assert_eq!(results[6].as_ref().unwrap()["duplicate"], true);
    let committed = fixture.app.consensus.local_snapshot().await;
    assert_eq!(committed.revision, 3);
    assert_eq!(committed.data["source:[\"records\",\"value\"]"], 3);
    assert!(
        !committed
            .data
            .contains_key("source:[\"records\",\"partial\"]")
    );
    assert_eq!(committed.requests.len(), 3);
    assert_eq!(
        committed.requests["first"].result,
        results[0].as_ref().unwrap()["value"]
    );
    assert_eq!(
        committed.requests["second"].result,
        results[5].as_ref().unwrap()["value"]
    );
    assert!(fixture.app.consensus.metrics().last_applied.unwrap().index - before <= 2);
    fixture.close().await;
}

#[tokio::test]
async fn queued_deployments_change_alias_visibility_in_order_even_for_receipt_replays() {
    let fixture = Fixture::new(128, true).await;
    let first = increment("first", 1, None);
    let pending = ordered(
        &fixture.app,
        vec![
            (first.clone(), false),
            (deployment("remove-alias", false), true),
            (first.clone(), false),
            (deployment("restore-alias", true), true),
            (first, false),
            (increment("second", 2, Some(4)), false),
        ],
    )
    .await;
    let results = responses(pending).await;
    assert_eq!(results[0].as_ref().unwrap()["revision"], 2);
    assert_eq!(results[1].as_ref().unwrap()["revision"], 3);
    assert_eq!(results[2].as_ref().unwrap_err().code, "METHOD_NOT_FOUND");
    assert_eq!(results[3].as_ref().unwrap()["revision"], 4);
    assert_eq!(results[4].as_ref().unwrap()["revision"], 2);
    assert_eq!(results[4].as_ref().unwrap()["duplicate"], true);
    assert_eq!(results[5].as_ref().unwrap()["revision"], 5);
    assert_eq!(results[5].as_ref().unwrap()["value"]["previous"], 1);
    assert_eq!(fixture.app.consensus.local_snapshot().await.revision, 5);
    fixture.close().await;
}

#[tokio::test]
async fn full_or_closed_queue_returns_unavailable_without_retaining_waiting_requests() {
    let mut fixture = Fixture::new(2, false).await;
    let held = ordered(
        &fixture.app,
        vec![
            (increment("a", 1, None), false),
            (increment("b", 1, None), false),
        ],
    )
    .await;
    let error = tokio::time::timeout(
        Duration::from_millis(100),
        submit(&fixture.app, increment("c", 1, None), false),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(error.code, "UNAVAILABLE");
    assert!(error.message.contains("queue full"));
    assert_eq!(fixture.app.writer_queue.capacity(), 0);
    assert_eq!(fixture.app.consensus.local_snapshot().await.revision, 1);
    fixture.receiver.take();
    let error = submit(&fixture.app, increment("d", 1, None), false)
        .await
        .unwrap_err();
    assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(error.message.contains("queue closed"));
    for receiver in held {
        assert!(receiver.await.is_err());
    }
    fixture.close().await;
}

#[tokio::test]
async fn bounded_groups_preserve_fifo_across_deferred_and_newly_received_work() {
    let fixture = Fixture::new(128, true).await;
    let pending = ordered(
        &fixture.app,
        (0..70)
            .map(|index| {
                (
                    increment(&format!("call-{index}"), 1, Some(index + 1)),
                    false,
                )
            })
            .collect(),
    )
    .await;
    let results = responses(pending).await;
    for (index, result) in results.iter().enumerate() {
        let value = result.as_ref().unwrap();
        assert_eq!(value["revision"], index + 2);
        assert_eq!(value["value"]["previous"], index);
        assert_eq!(value["value"]["value"], index + 1);
        assert_eq!(value["value"]["doubled"], (index + 1) * 2);
    }
    let state = fixture.app.consensus.local_snapshot().await;
    assert_eq!(state.revision, 71);
    assert_eq!(state.requests.len(), 71);
    fixture.close().await;
}

pub(super) enum Decision {
    Apply,
    RejectBefore,
    RejectAfter,
}

enum Event {
    Extended {
        requests: usize,
    },
    Prepared {
        requests: usize,
        proceed: oneshot::Sender<()>,
    },
    Committing {
        proceed: oneshot::Sender<Decision>,
    },
}

pub(super) struct Hooks {
    events: mpsc::UnboundedSender<Event>,
}

impl Hooks {
    pub(super) fn extended(&self, requests: usize) {
        let _ = self.events.send(Event::Extended { requests });
    }

    pub(super) async fn prepared(&self, requests: usize) {
        let (proceed, wait) = oneshot::channel();
        let _ = self.events.send(Event::Prepared { requests, proceed });
        let _ = wait.await;
    }

    pub(super) async fn committing(&self, _group: &Group) -> Decision {
        let (proceed, wait) = oneshot::channel();
        let _ = self.events.send(Event::Committing { proceed });
        wait.await.unwrap_or(Decision::Apply)
    }
}

async fn raw_event(events: &mut mpsc::UnboundedReceiver<Event>) -> Event {
    tokio::time::timeout(Duration::from_secs(10), events.recv())
        .await
        .unwrap()
        .unwrap()
}

async fn next_event(events: &mut mpsc::UnboundedReceiver<Event>) -> Event {
    loop {
        let event = raw_event(events).await;
        if !matches!(event, Event::Extended { .. }) {
            return event;
        }
    }
}

// The gates pause actual submission/preparation, not a mock database. This
// proves private preparation overlaps real Raft application without publishing
// speculative roots or delaying already-durable acknowledgements.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_overlaps_preparation_and_acks_without_waiting_for_successor() {
    let mut fixture = Fixture::new(256, false).await;
    let inputs = (0..130)
        .map(|i| (increment(&format!("overlap-{i}"), 1, None), false))
        .collect();
    let mut replies = ordered(&fixture.app, inputs).await;
    let mut receiver = fixture.receiver.take().unwrap();
    let (events, mut events_rx) = mpsc::unbounded_channel();
    let app = fixture.app.clone();
    let task = tokio::spawn(async move {
        let hooks = Hooks { events };
        let mut deferred = VecDeque::new();
        pipeline(&app, &mut receiver, &mut deferred, Some(&hooks)).await;
        (receiver, deferred)
    });
    let Event::Prepared {
        requests: first_count,
        proceed,
    } = next_event(&mut events_rx).await
    else {
        panic!("expected first preparation")
    };
    assert!(first_count > 0 && first_count <= 64);
    proceed.send(()).unwrap();
    let mut commit_gate = None;
    let mut prepared_gate = None;
    for _ in 0..2 {
        match next_event(&mut events_rx).await {
            Event::Extended { .. } => unreachable!(),
            Event::Committing { proceed } => commit_gate = Some(proceed),
            Event::Prepared { proceed, .. } => prepared_gate = Some(proceed),
        }
    }
    assert_eq!(
        fixture
            .app
            .consensus
            .read_for_writer()
            .await
            .unwrap()
            .revision,
        1
    );
    for reply in &mut replies {
        assert!(matches!(
            reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }
    commit_gate.unwrap().send(Decision::Apply).ok().unwrap();
    // Successor remains paused; first group still replies promptly.
    for (i, reply) in replies.iter_mut().take(first_count).enumerate() {
        let response = tokio::time::timeout(Duration::from_secs(2), reply)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(response["revision"], i + 2);
        assert_eq!(response["value"]["value"], i + 1);
    }
    let durable = fixture.app.consensus.read_for_writer().await.unwrap();
    assert_eq!(durable.revision, first_count as u64 + 1);
    assert_eq!(durable.requests.len(), first_count + 1);
    assert!(matches!(
        replies[first_count].try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    // Dropping the event receiver releases later hooks automatically.
    drop(events_rx);
    prepared_gate.unwrap().send(()).unwrap();
    let (receiver, deferred) = task.await.unwrap();
    drop(receiver);
    drop(deferred);
    fixture.close().await;
}

async fn uncertain_predecessor(decision: Decision, applied: bool) {
    let mut fixture = Fixture::new(256, false).await;
    let inputs = (0..130)
        .map(|i| (increment(&format!("uncertain-{i}"), 1, None), false))
        .collect();
    let mut replies = ordered(&fixture.app, inputs).await;
    let mut receiver = fixture.receiver.take().unwrap();
    let (events, mut events_rx) = mpsc::unbounded_channel();
    let app = fixture.app.clone();
    let task = tokio::spawn(async move {
        let hooks = Hooks { events };
        let mut deferred = VecDeque::new();
        pipeline(&app, &mut receiver, &mut deferred, Some(&hooks)).await;
        (receiver, deferred)
    });
    let Event::Prepared {
        requests: first_count,
        proceed,
    } = next_event(&mut events_rx).await
    else {
        panic!("expected first preparation")
    };
    proceed.send(()).unwrap();
    let mut commit_gate = None;
    let mut prepared_gate = None;
    let mut second_count = 0;
    for _ in 0..2 {
        match next_event(&mut events_rx).await {
            Event::Extended { .. } => unreachable!(),
            Event::Committing { proceed } => commit_gate = Some(proceed),
            Event::Prepared { requests, proceed } => {
                second_count = requests;
                prepared_gate = Some(proceed);
            }
        }
    }
    commit_gate.unwrap().send(decision).ok().unwrap();
    prepared_gate.unwrap().send(()).unwrap();
    let (mut receiver, mut deferred) = task.await.unwrap();
    assert!(
        events_rx.try_recv().is_err(),
        "successor must never be submitted"
    );
    for reply in replies.iter_mut().take(first_count + second_count) {
        assert_eq!(reply.await.unwrap().unwrap_err().code, "UNAVAILABLE");
    }
    let durable = fixture.app.consensus.read_for_writer().await.unwrap();
    assert_eq!(
        durable.revision,
        if applied { first_count as u64 + 1 } else { 1 }
    );
    // Throw away unprepared calls, then retry the same IDs against a fresh
    // barrier. A lost commit acknowledgement must never double-apply writes.
    deferred.clear();
    while receiver.try_recv().is_ok() {}
    let (reply, receive) = oneshot::channel();
    deferred.push_back(Pending {
        input: increment("uncertain-0", 1, None).into(),
        deployment: false,
        enqueued: Instant::now(),
        reply,
    });
    pipeline(&fixture.app, &mut receiver, &mut deferred, None).await;
    let response = receive.await.unwrap().unwrap();
    assert_eq!(response["duplicate"], applied);
    assert_eq!(response["value"]["value"], 1);
    assert_eq!(response["revision"], 2);
    assert_eq!(
        fixture
            .app
            .consensus
            .read_for_writer()
            .await
            .unwrap()
            .revision,
        if applied { first_count as u64 + 1 } else { 2 }
    );
    drop(receiver);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_discards_successor_when_predecessor_does_not_apply() {
    uncertain_predecessor(Decision::RejectBefore, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_recovers_receipts_after_lost_commit_acknowledgement() {
    uncertain_predecessor(Decision::RejectAfter, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn empty_groups_recheck_durable_revision_before_replaying_or_exposing_validation_errors() {
    for conflicting_fingerprint in [false, true] {
        let mut fixture = Fixture::new(16, false).await;
        let original = increment("historical", 1, None);
        let initial_replies = ordered(&fixture.app, vec![(original.clone(), false)]).await;
        let mut receiver = fixture.receiver.take().unwrap();
        let mut deferred = VecDeque::new();
        pipeline(&fixture.app, &mut receiver, &mut deferred, None).await;
        let initial_response = responses(initial_replies).await.remove(0).unwrap();
        assert_eq!(initial_response["revision"], 2);

        let input = if conflicting_fingerprint {
            increment("historical", 9, None)
        } else {
            original.clone()
        };
        let mut replies = ordered(&fixture.app, vec![(input, false)]).await;
        let (events, mut events_rx) = mpsc::unbounded_channel();
        let app = fixture.app.clone();
        let task = tokio::spawn(async move {
            let hooks = Hooks { events };
            pipeline(&app, &mut receiver, &mut deferred, Some(&hooks)).await;
            (receiver, deferred)
        });
        let Event::Prepared { requests, proceed } = next_event(&mut events_rx).await else {
            panic!("expected replay/validation preparation")
        };
        assert_eq!(requests, 1);
        proceed.send(()).unwrap();
        let Event::Committing { proceed } = next_event(&mut events_rx).await else {
            panic!("expected empty group commit barrier")
        };
        assert!(matches!(
            replies[0].try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        // Simulate another durable writer winning after this group's initial
        // barrier. Revoking the alias also makes the old validation answer
        // inappropriate: exposure is checked before historical receipt lookup.
        let state = fixture.app.consensus.read_for_writer().await.unwrap();
        let (reply, _) = oneshot::channel();
        let revoke = Pending {
            input: deployment("revoke-between-prepare-and-commit", false).into(),
            deployment: true,
            enqueued: Instant::now(),
            reply,
        };
        let prepared = prepare(&fixture.app, &state, &revoke).await.unwrap();
        fixture
            .app
            .consensus
            .commit(prepared.command.unwrap())
            .await
            .unwrap();
        let after_revoke = fixture.app.consensus.read_for_writer().await.unwrap();
        assert_eq!(after_revoke.revision, 3);
        assert_eq!(after_revoke.requests["historical"].revision, 2);

        proceed.send(Decision::Apply).ok().unwrap();
        let (receiver, deferred) = tokio::time::timeout(Duration::from_secs(10), task)
            .await
            .unwrap()
            .unwrap();
        let error = responses(replies).await.remove(0).unwrap_err();
        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.code, "UNAVAILABLE");
        assert!(error.message.contains("snapshot changed"));
        assert_eq!(
            fixture.app.consensus.read_for_writer().await.unwrap(),
            after_revoke
        );
        assert!(deferred.is_empty());

        let (reply, _) = oneshot::channel();
        let retry = Pending {
            input: original.into(),
            deployment: false,
            enqueued: Instant::now(),
            reply,
        };
        let error = prepare(&fixture.app, &after_revoke, &retry)
            .await
            .err()
            .unwrap();
        assert_eq!(error.code, "METHOD_NOT_FOUND");
        drop(receiver);
        fixture.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_predecessor_hides_successor_replays_and_all_speculative_validation_errors() {
    let mut fixture = Fixture::new(16, false).await;
    let original = increment("predecessor", 1, Some(1));
    let successor_inputs = vec![
        (original.clone(), false),
        (increment("predecessor", 9, Some(1)), false),
        (increment("stale-successor", 1, Some(1)), false),
        (
            json!({"requestId":"failed-successor","name":"counter.fail","args":null}),
            false,
        ),
    ];
    let initial = fixture.app.consensus.read_for_writer().await.unwrap();

    // Establish that the successor cases really produce four different
    // outcomes against the predecessor's private state, with no durable write.
    let mut staged = initial.clone();
    let (reply, _) = oneshot::channel();
    let predecessor = Pending {
        input: original.clone().into(),
        deployment: false,
        enqueued: Instant::now(),
        reply,
    };
    let prepared = prepare(&fixture.app, &staged, &predecessor).await.unwrap();
    stage(&mut staged, &prepared.command.unwrap()).unwrap();
    for (index, (input, deployment)) in successor_inputs.iter().enumerate() {
        let (reply, _) = oneshot::channel();
        let pending = Pending {
            input: input.clone().into(),
            deployment: *deployment,
            enqueued: Instant::now(),
            reply,
        };
        let outcome = prepare(&fixture.app, &staged, &pending).await;
        if index == 0 {
            let replay = outcome.unwrap();
            assert_eq!(replay.response["duplicate"], true);
            assert_eq!(replay.response["revision"], 2);
            assert!(replay.command.is_none());
        } else {
            assert_eq!(
                outcome.err().unwrap().code,
                [
                    "REQUEST_ID_REUSED",
                    "REVISION_CONFLICT",
                    "EVALUATION_FAILED"
                ][index - 1]
            );
        }
    }
    assert_eq!(
        fixture.app.consensus.read_for_writer().await.unwrap(),
        initial
    );

    // Only the predecessor exists when the first group is collected. Enqueue
    // successors while its preparation gate is held, guaranteeing the boundary
    // without relying on group count, timing, or evaluator throughput.
    let mut replies = ordered(&fixture.app, vec![(original, false)]).await;
    let mut receiver = fixture.receiver.take().unwrap();
    let (events, mut events_rx) = mpsc::unbounded_channel();
    let app = fixture.app.clone();
    let task = tokio::spawn(async move {
        let hooks = Hooks { events };
        let mut deferred = VecDeque::new();
        pipeline(&app, &mut receiver, &mut deferred, Some(&hooks)).await;
        (receiver, deferred)
    });
    let Event::Prepared { requests, proceed } = next_event(&mut events_rx).await else {
        panic!("expected predecessor preparation")
    };
    assert_eq!(requests, 1);
    replies.extend(ordered(&fixture.app, successor_inputs).await);
    proceed.send(()).unwrap();
    let mut commit_gate = None;
    let mut successor_gate = None;
    for _ in 0..2 {
        match next_event(&mut events_rx).await {
            Event::Extended { .. } => unreachable!(),
            Event::Committing { proceed } => commit_gate = Some(proceed),
            Event::Prepared { requests, proceed } => {
                assert_eq!(requests, 4);
                successor_gate = Some(proceed);
            }
        }
    }
    for reply in &mut replies {
        assert!(matches!(
            reply.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
    }
    commit_gate
        .unwrap()
        .send(Decision::RejectBefore)
        .ok()
        .unwrap();
    successor_gate.unwrap().send(()).unwrap();
    let (receiver, deferred) = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap()
        .unwrap();
    for result in responses(replies).await {
        let error = result.unwrap_err();
        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.code, "UNAVAILABLE");
        assert!(
            error
                .message
                .contains("injected unknown commit before apply")
        );
    }
    assert!(
        events_rx.try_recv().is_err(),
        "successor must never be submitted"
    );
    assert!(deferred.is_empty());
    assert_eq!(
        fixture.app.consensus.read_for_writer().await.unwrap(),
        initial
    );
    drop(receiver);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_accepts_arrivals_while_an_initially_empty_queue_is_committing() {
    let mut fixture = Fixture::new(16, false).await;
    let mut replies = ordered(&fixture.app, vec![(increment("early", 1, None), false)]).await;
    let mut receiver = fixture.receiver.take().unwrap();
    let (events, mut events_rx) = mpsc::unbounded_channel();
    let app = fixture.app.clone();
    let task = tokio::spawn(async move {
        let hooks = Hooks { events };
        let mut deferred = VecDeque::new();
        pipeline(&app, &mut receiver, &mut deferred, Some(&hooks)).await;
        (receiver, deferred)
    });
    let Event::Prepared { requests, proceed } = next_event(&mut events_rx).await else {
        panic!("expected initial preparation")
    };
    assert_eq!(requests, 1);
    proceed.send(()).unwrap();
    let Event::Committing { proceed: commit } = next_event(&mut events_rx).await else {
        panic!("expected committing group")
    };
    // The queue was empty when commit started. This new call must be prepared
    // before the held commit is released, still against its private successor.
    replies.extend(ordered(&fixture.app, vec![(increment("late", 2, Some(2)), false)]).await);
    let Event::Prepared { requests, proceed } = next_event(&mut events_rx).await else {
        panic!("expected overlapping late preparation")
    };
    assert_eq!(requests, 1);
    assert_eq!(
        fixture
            .app
            .consensus
            .read_for_writer()
            .await
            .unwrap()
            .revision,
        1
    );
    commit.send(Decision::Apply).ok().unwrap();
    proceed.send(()).unwrap();
    drop(events_rx);
    let (receiver, deferred) = task.await.unwrap();
    let results = responses(replies).await;
    assert_eq!(results[0].as_ref().unwrap()["value"]["value"], 1);
    assert_eq!(results[1].as_ref().unwrap()["value"]["value"], 3);
    assert_eq!(
        fixture
            .app
            .consensus
            .read_for_writer()
            .await
            .unwrap()
            .revision,
        3
    );
    assert!(deferred.is_empty());
    drop(receiver);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn successor_fills_with_later_arrivals_during_the_same_commit() {
    let mut fixture = Fixture::new(16, false).await;
    Arc::get_mut(&mut fixture.app).unwrap().admission =
        admission::Pool::new([1, 1], [1, 1], [1 << 20, 1 << 20], 1);
    let mut replies = ordered(
        &fixture.app,
        vec![(increment("predecessor", 1, None), false)],
    )
    .await;
    let mut receiver = fixture.receiver.take().unwrap();
    let (events, mut events_rx) = mpsc::unbounded_channel();
    let app = fixture.app.clone();
    let task = tokio::spawn(async move {
        let hooks = Hooks { events };
        let mut deferred = VecDeque::new();
        pipeline(&app, &mut receiver, &mut deferred, Some(&hooks)).await;
        (receiver, deferred)
    });
    let Event::Prepared { proceed, .. } = next_event(&mut events_rx).await else {
        panic!("expected predecessor")
    };
    proceed.send(()).unwrap();
    let Event::Committing { proceed: commit } = next_event(&mut events_rx).await else {
        panic!("expected held commit")
    };
    assert_eq!(fixture.app.admission.metrics()["classes"][0]["active"], 0);
    replies.extend(
        ordered(
            &fixture.app,
            vec![(increment("first-successor", 2, Some(2)), false)],
        )
        .await,
    );
    let Event::Prepared { requests, proceed } = next_event(&mut events_rx).await else {
        panic!("expected successor prefix")
    };
    assert_eq!(requests, 1);
    proceed.send(()).unwrap();
    let competitor = tokio::time::timeout(
        Duration::from_secs(5),
        admission::acquire(&fixture.app, admission::Class::User, &Value::Null),
    )
    .await
    .expect("successor releases its pass slot while waiting for arrivals")
    .unwrap();
    drop(competitor);
    for (index, by) in [3, 4].into_iter().enumerate() {
        replies.extend(
            ordered(
                &fixture.app,
                vec![(
                    increment(&format!("later-{index}"), by, Some(index as u64 + 3)),
                    false,
                )],
            )
            .await,
        );
        let Event::Extended { requests } = raw_event(&mut events_rx).await else {
            panic!("expected same group to extend")
        };
        assert_eq!(requests, index + 2);
    }
    assert_eq!(
        fixture
            .app
            .consensus
            .read_for_writer()
            .await
            .unwrap()
            .revision,
        1
    );
    commit.send(Decision::Apply).ok().unwrap();
    let Event::Committing { proceed } = next_event(&mut events_rx).await else {
        panic!("expected one successor commit")
    };
    proceed.send(Decision::Apply).ok().unwrap();
    drop(events_rx);
    let (receiver, deferred) = task.await.unwrap();
    let results = responses(replies).await;
    for (index, expected) in [1, 3, 6, 10].into_iter().enumerate() {
        assert_eq!(results[index].as_ref().unwrap()["revision"], index + 2);
        assert_eq!(results[index].as_ref().unwrap()["value"]["value"], expected);
    }
    assert!(deferred.is_empty());
    drop(receiver);
    fixture.close().await;
}

// A learned adaptive target of one call must not close a successor while its
// predecessor is still committing. Stopping there would make every later
// arrival wait for another whole durable round.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adaptive_successor_fills_past_its_targets_until_the_predecessor_completes() {
    let mut fixture = Fixture::new(16, false).await;
    let mut replies = ordered(
        &fixture.app,
        vec![(increment("predecessor", 1, None), false)],
    )
    .await;
    let before = fixture.app.consensus.metrics().last_applied.unwrap().index;
    let mut receiver = fixture.receiver.take().unwrap();
    let (events, mut events_rx) = mpsc::unbounded_channel();
    let app = fixture.app.clone();
    let task = tokio::spawn(async move {
        let hooks = Hooks { events };
        let mut deferred = VecDeque::new();
        let mut controller = Controller::new(tuning::settings().unwrap());
        controller.adaptive_for_test(16, Duration::from_millis(50));
        // One call already fills the learned preparation target.
        controller.observe(1, Duration::from_millis(40), Duration::from_millis(1), true);
        assert_eq!(controller.decide(1).count, 1);
        pipeline_with_controller(
            &app,
            &mut receiver,
            &mut deferred,
            &mut controller,
            Some(&hooks),
        )
        .await;
        (receiver, deferred)
    });
    let Event::Prepared { proceed, .. } = next_event(&mut events_rx).await else {
        panic!("expected predecessor")
    };
    proceed.send(()).unwrap();
    let Event::Committing { proceed: commit } = next_event(&mut events_rx).await else {
        panic!("expected held commit")
    };
    replies.extend(
        ordered(
            &fixture.app,
            (0..3)
                .map(|index| (increment(&format!("successor-{index}"), 1, None), false))
                .collect(),
        )
        .await,
    );
    let Event::Prepared { requests, proceed } = next_event(&mut events_rx).await else {
        panic!("expected successor prefix")
    };
    assert_eq!(requests, 1, "the adaptive count target collects one call");
    proceed.send(()).unwrap();
    for requests in [2, 3] {
        let Event::Extended { requests: extended } = raw_event(&mut events_rx).await else {
            panic!("expected the successor to keep filling")
        };
        assert_eq!(extended, requests);
    }
    commit.send(Decision::Apply).ok().unwrap();
    let Event::Committing { proceed } = next_event(&mut events_rx).await else {
        panic!("expected one successor commit")
    };
    proceed.send(Decision::Apply).ok().unwrap();
    drop(events_rx);
    let (receiver, deferred) = task.await.unwrap();
    for (index, result) in responses(replies).await.into_iter().enumerate() {
        let result = result.unwrap();
        assert_eq!(result["revision"], index + 2);
        assert_eq!(result["value"]["value"], index + 1);
    }
    assert!(deferred.is_empty());
    assert_eq!(
        fixture.app.consensus.metrics().last_applied.unwrap().index - before,
        2,
        "one predecessor and one successor Raft entry"
    );
    drop(receiver);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn adaptive_backlog_preserves_fifo_cas_and_receipts_across_preparation_windows() {
    let mut fixture = Fixture::new(128, false).await;
    let mut inputs = Vec::new();
    let mut expected = Vec::new();
    for index in 0..96u64 {
        let input = increment(&format!("adaptive-{index}"), 1, Some(index + 1));
        inputs.push((input.clone(), false));
        expected.push((index + 2, index + 1, false));
        if index % 16 == 0 {
            inputs.push((input, false));
            expected.push((index + 2, index + 1, true));
        }
    }
    let replies = ordered(&fixture.app, inputs).await;
    let mut receiver = fixture.receiver.take().unwrap();
    let mut deferred = VecDeque::new();
    let mut controller = Controller::new(tuning::settings().unwrap());
    controller.adaptive_for_test(128, Duration::from_millis(1));
    // Begin with a real learned decision, then feed subsequent real Raft and
    // evaluation samples back through the same controller across writer turns.
    controller.observe(10, Duration::from_millis(1), Duration::from_millis(2), true);
    let before = fixture.app.consensus.metrics().last_applied.unwrap().index;
    while !receiver.is_empty() || !deferred.is_empty() {
        pipeline_with_controller(
            &fixture.app,
            &mut receiver,
            &mut deferred,
            &mut controller,
            None,
        )
        .await;
    }
    for (response, (revision, value, duplicate)) in
        responses(replies).await.into_iter().zip(expected)
    {
        let response = response.unwrap();
        assert_eq!(response["revision"], revision);
        assert_eq!(response["value"]["value"], value);
        assert_eq!(response["duplicate"], duplicate);
        assert_eq!(
            response["value"]["calls"], 1,
            "each invocation retains isolated JS state"
        );
    }
    let state = fixture.app.consensus.read_for_writer().await.unwrap();
    assert_eq!(state.revision, 97);
    assert_eq!(state.requests.len(), 97);
    assert!(fixture.app.consensus.metrics().last_applied.unwrap().index > before + 1);
    assert_eq!(controller.decide(128).mode, "adaptive");
    assert!(controller.decide(128).budget <= Duration::from_millis(1));
    drop(receiver);
    fixture.close().await;
}

#[test]
fn canceled_queue_entries_release_payloads_but_started_work_keeps_its_reservation() {
    let pool = crate::service::admission::Pool::configured().unwrap();
    let retained = pool.retain(admission::Class::User, 4096).unwrap();
    let input = PendingInput::new(
        json!({"requestId":"cancel","args":"payload"}),
        Some(retained),
    );
    let submitted = Submitted(input.clone());
    drop(submitted);
    assert!(input.request_id().is_none());
    assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 0);

    let retained = pool.retain(admission::Class::User, 4096).unwrap();
    let input = PendingInput::new(json!({"requestId":"committing"}), Some(retained));
    let submitted = Submitted(input.clone());
    let executing = input.begin().unwrap();
    drop(submitted);
    drop(input);
    assert_eq!(executing.value["requestId"], "committing");
    assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 4096);
    drop(executing);
    assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 0);
}

fn speculative_bundle() -> String {
    r#"
    const records={kind:'collection',name:'records'};
    var __flowerBundle={default:{definitions:{
      stable:{name:'stable',kind:'derived',compute:()=>0},
      authorize:{name:'authorize',kind:'queryMethod',compute:ctx=>({subject:'same-owner',claims:{role:ctx.get(records,'role')||'reader'}})},
      write:{name:'write',kind:'mutationMethod',compute:(ctx,args)=>{
        const old=ctx.get(records,args.key)||0; ctx.set(records,args.key,old+1);
        return {old,value:old+1,principal:ctx.principal()};
      }}
    },authorize:{name:'authorize'},http:{write:{name:'write',kind:'mutation'}}}};
    "#.into()
}
fn speculative_input(id: &str, key: &str) -> PendingInput {
    json!({"name":"write","args":{"key":key},"requestId":id}).into()
}

#[tokio::test]
async fn speculative_candidates_recheck_order_principal_receipts_cas_and_transaction_locks() {
    let (_directory, app) = super::super::tests::application(speculative_bundle()).await;
    let mut state = app.consensus.read_for_writer().await.unwrap();
    let now = app.clock.sample(&state).unwrap();
    let first = speculative_input("first", "a");
    let second = speculative_input("second", "b");
    let mut a = prepare_candidate(&app, &state, &first, false, Some(now))
        .await
        .unwrap();
    let mut b = prepare_candidate(&app, &state, &second, false, Some(now))
        .await
        .unwrap();
    assert!(a.validate(&app, &state, &first).await.unwrap());
    stage(&mut state, a.prepared.command.as_ref().unwrap()).unwrap();
    drop(a);
    assert!(
        b.validate(&app, &state, &second).await.unwrap(),
        "disjoint writes must reuse"
    );
    assert_eq!(
        b.prepared.command.as_ref().unwrap().expected_revision,
        state.revision
    );
    assert_eq!(b.prepared.response["revision"], state.revision + 1);
    stage(&mut state, b.prepared.command.as_ref().unwrap()).unwrap();
    drop(b);
    let input = speculative_input("principal", "c");
    let mut candidate = prepare_candidate(&app, &state, &input, false, Some(now))
        .await
        .unwrap();
    let mut changed = state.clone();
    changed.data.insert(
        "source:[\"records\",\"role\"]".into(),
        json!("administrator"),
    );
    changed.revision += 1;
    assert!(
        candidate.certificate.as_ref().unwrap().valid(&changed.data),
        "auth reads are deliberately rechecked separately"
    );
    let access = candidate.access.as_ref().unwrap();
    assert!(access.holds(&app, &state), "an unchanged decision is reused");
    assert!(!access.holds(&app, &changed), "authorization read the role");
    assert!(
        !candidate.validate(&app, &changed, &input).await.unwrap(),
        "same owner but different claims must recompute"
    );
    let mut replay = state.clone();
    replay.requests.insert(
        "principal".into(),
        Receipt {
            fingerprint: candidate
                .prepared
                .command
                .as_ref()
                .unwrap()
                .fingerprint
                .clone(),
            revision: 99,
            result: json!("historical"),
            epoch: None,
        },
    );
    assert!(!candidate.validate(&app, &replay, &input).await.unwrap());
    let mut locked = state.clone();
    locked
        .data
        .insert("transaction:participant".into(), json!({"id":"held"}));
    assert!(candidate.validate(&app, &locked, &input).await.is_err());
    drop(candidate);
    let input:PendingInput=json!({"name":"write","args":{"key":"d"},"requestId":"cas","expectedRevision":state.revision}).into();
    let mut candidate = prepare_candidate(&app, &state, &input, false, Some(now))
        .await
        .unwrap();
    state.revision += 1;
    assert!(!candidate.validate(&app, &state, &input).await.unwrap());
    drop(candidate);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn speculative_wave_drains_under_a_single_shared_admission_slot() {
    let (_directory, app) = super::super::tests::application(speculative_bundle()).await;
    let mut owned = Arc::try_unwrap(app)
        .ok()
        .expect("fixture has one strong owner");
    owned.admission = admission::Pool::new([1, 1], [1, 1], [1 << 20, 1 << 20], 1);
    let app = Arc::new(owned);
    let mut state = app.consensus.read_for_writer().await.unwrap();
    let now = app.clock.sample(&state).unwrap();
    let pending = (0..4)
        .map(|n| {
            let (reply, _) = oneshot::channel();
            Pending {
                input: speculative_input(&format!("wave{n}"), &format!("key{n}")),
                deployment: false,
                enqueued: Instant::now(),
                reply,
            }
        })
        .collect::<Vec<_>>();
    let mut wave = speculation::Wave::new(&app, &state, &pending, now);
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut first = wave.next().await.unwrap().unwrap();
        assert!(
            first
                .validate(&app, &state, &pending[0].input)
                .await
                .unwrap()
        );
        stage(&mut state, first.prepared.command.as_ref().unwrap()).unwrap();
        drop(first);
        let mut second = wave.next().await.unwrap().unwrap();
        assert!(
            second
                .validate(&app, &state, &pending[1].input)
                .await
                .unwrap()
        );
        drop(second);
        wave.drain().await;
    })
    .await
    .expect("completed candidates must release permits before later queued siblings");
    assert!(!pending[2].input.state.lock().unwrap().begun);
    assert!(
        !pending[3].input.state.lock().unwrap().begun,
        "draining must not start queued siblings"
    );
    assert_eq!(app.admission.metrics()["classes"][0]["active"], 0);
    assert_eq!(
        app.admission.metrics()["classes"][0]["retainedInputBytes"],
        0
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn serial_and_replayed_results_keep_output_bytes_reserved() {
    let fixture = Fixture::new(4, false).await;
    let (reply, _receive) = oneshot::channel();
    let pending = Pending {
        input: increment("retained-output", 1, None).into(),
        deployment: false,
        enqueued: Instant::now(),
        reply,
    };
    let state = fixture.app.consensus.read_for_writer().await.unwrap();
    let mut prepared = prepare(&fixture.app, &state, &pending).await.unwrap();
    let bytes = fixture.app.admission.metrics()["classes"][0]["retainedInputBytes"]
        .as_u64()
        .unwrap();
    assert!(bytes > 0);
    assert!(prepared._retained.is_some());
    fixture
        .app
        .consensus
        .commit(prepared.command.take().unwrap())
        .await
        .unwrap();
    assert_eq!(
        fixture.app.admission.metrics()["classes"][0]["retainedInputBytes"],
        bytes
    );
    drop(prepared);
    assert_eq!(
        fixture.app.admission.metrics()["classes"][0]["retainedInputBytes"],
        0
    );
    let state = fixture.app.consensus.read_for_writer().await.unwrap();
    let replay = prepare(&fixture.app, &state, &pending).await.unwrap();
    assert_eq!(replay.response["duplicate"], true);
    assert!(replay._retained.is_some());
    assert!(
        fixture.app.admission.metrics()["classes"][0]["retainedInputBytes"]
            .as_u64()
            .unwrap()
            > 0
    );
    drop(replay);
    assert_eq!(
        fixture.app.admission.metrics()["classes"][0]["retainedInputBytes"],
        0
    );
    fixture.close().await;
}

fn serial_pass_inputs(count: usize) -> Vec<Pending> {
    (0..count)
        .map(|index| {
            let (reply, _) = oneshot::channel();
            Pending {
                input: increment(&format!("pass-{index}"), 1, None).into(),
                deployment: false,
                enqueued: Instant::now(),
                reply,
            }
        })
        .collect()
}

async fn wait_for_queued_query(app: &App) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while app.admission.metrics()["classes"][0]["queued"] != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("competing query must reach worker admission");
}

#[tokio::test]
async fn serial_worker_matches_dispatch_for_fifo_cas_receipts_errors_and_isolation() {
    let fixture = Fixture::new(16, false).await;
    let mut original = fixture.app.consensus.read_for_writer().await.unwrap();
    let durable_clock = original.data["clock"].clone();
    // Pin the logical floor above wall time so both execution paths use the
    // same clock while retaining each method's ordinary sampling behavior.
    original
        .data
        .insert("clock".into(), json!(4_000_000_000_000u64));
    let revision = original.revision;
    let inputs = [
        increment("first", 1, Some(revision)),
        increment("first", 1, Some(revision)),
        increment("stale-cas", 9, Some(revision)),
        json!({"requestId":"failure","name":"counter.fail","args":null}),
        increment("after-failure", 3, Some(revision + 1)),
        increment("canceled", 7, None),
        increment("last", 4, None),
    ];
    let pending = || {
        inputs
            .iter()
            .enumerate()
            .map(|(index, value)| {
                let (reply, _) = oneshot::channel();
                let input = PendingInput::from(value.clone());
                if index == 5 {
                    input.cancel();
                }
                Pending {
                    input,
                    deployment: false,
                    enqueued: Instant::now(),
                    reply,
                }
            })
            .collect::<Vec<_>>()
    };
    let mut dispatched = original.clone();
    let mut commands = Vec::new();
    let mut bytes = 0;
    let mut admission = None;
    let mut expected = Vec::new();
    for input in pending() {
        let prepared = prepare_serial(&fixture.app, &dispatched, &input, &mut admission).await;
        expected.push(
            stage_prepared(
                &fixture.app,
                &mut dispatched,
                prepared,
                &mut commands,
                &mut bytes,
            )
            .unwrap(),
        );
    }
    drop(admission);
    let pending = pending();
    let admission = admit(&fixture.app, &pending[0].input, false).await.unwrap();
    let mut controller = Controller::new(tuning::settings().unwrap());
    controller.fixed_for_test(inputs.len());
    controller.speculation = speculation::Policy::new(1, 16);
    let mut decision = controller.decide(inputs.len());
    decision.budget = Duration::from_secs(10);
    let (mut group, deferred) = prepare_group(
        &fixture.app,
        &mut original,
        pending,
        Preparation {
            read_us: 0,
            decision,
            admission: Some(admission),
        },
        &mut controller.speculation,
        None,
        None,
    )
    .await;
    assert!(deferred.is_empty());
    assert_eq!(group.serial_worker_jobs, 1);
    assert_eq!(group.serial_worker_requests, inputs.len());
    assert_eq!(group.commands, commands);
    assert_eq!(original, dispatched);
    for (actual, expected) in group.results.iter().zip(expected) {
        match (actual, expected) {
            (Ok(actual), Ok(expected)) => {
                assert_eq!(actual.response, expected.response);
                assert_eq!(
                    actual.response["value"]["calls"], 1,
                    "each method still gets isolated guest globals"
                );
            }
            (Err(actual), Err(expected)) => {
                assert_eq!(
                    (actual.status, actual.code),
                    (expected.status, expected.code)
                )
            }
            _ => panic!("worker and individual dispatch disagree"),
        }
    }
    assert_eq!(original.data["source:[\"records\",\"value\"]"], 8);
    assert!(
        !original
            .data
            .contains_key("source:[\"records\",\"partial\"]")
    );
    assert_eq!(
        group.results[1].as_ref().unwrap().response["duplicate"],
        true
    );
    assert_eq!(
        group.results[2].as_ref().err().unwrap().code,
        "REVISION_CONFLICT"
    );
    // The shared queue must submit the same compact overlay and preserve every
    // intermediate receipt, including around failed calls and staged replays.
    let retry = SharedCommit::compact(group.commands.clone()).unwrap();
    commit_prepared_group(
        &fixture.app,
        std::mem::take(&mut group.commands),
        &group.results,
    )
    .await
    .unwrap();
    let durable = fixture.app.consensus.read().await.unwrap();
    // The clock pin above exists only in the two private test snapshots. These
    // methods do not read or change clock, so no command persists that pin.
    original.data.insert("clock".into(), durable_clock);
    assert_eq!(durable, original);
    for id in ["stale-cas", "failure", "canceled"] {
        assert!(!durable.requests.contains_key(id));
    }
    let applied = fixture.app.consensus.commit_compact(retry).await.unwrap();
    assert_eq!(applied.len(), 3);
    for (index, result) in applied.iter().enumerate() {
        assert!(matches!(result, ApplyResult::Committed(result)
            if result.duplicate && result.revision == revision + index as u64 + 1));
    }
    assert_eq!(fixture.app.consensus.read().await.unwrap(), durable);
    drop(group);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn serial_worker_waiting_for_evaluation_does_not_delay_predecessor_ack() {
    let fixture = Fixture::new(8, false).await;
    let mut state = fixture.app.consensus.read_for_writer().await.unwrap();
    let mut first = serial_pass_inputs(1);
    let (reply, receive) = oneshot::channel();
    first[0].reply = reply;
    let mut controller = Controller::new(tuning::settings().unwrap());
    controller.fixed_for_test(1);
    let (group, _) = prepare_group(
        &fixture.app,
        &mut state,
        first,
        Preparation {
            read_us: 0,
            decision: controller.decide(1),
            admission: None,
        },
        &mut controller.speculation,
        None,
        None,
    )
    .await;
    let pending = serial_pass_inputs(3)
        .into_iter()
        .skip(1)
        .collect::<Vec<_>>();
    let observed = pending[0].input.clone();
    let admission = admit(&fixture.app, &observed, false).await.unwrap();
    let gate = fixture
        .app
        .evaluations
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    let app = fixture.app.clone();
    let task = tokio::spawn(async move {
        let mut controller = Controller::new(tuning::settings().unwrap());
        controller.fixed_for_test(2);
        controller.speculation = speculation::Policy::new(1, 8);
        let mut decision = controller.decide(2);
        decision.budget = Duration::from_secs(10);
        tokio::join!(
            group.commit_and_respond(&app, None),
            prepare_group(
                &app,
                &mut state,
                pending,
                Preparation {
                    read_us: 0,
                    decision,
                    admission: Some(admission)
                },
                &mut controller.speculation,
                None,
                None
            )
        )
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !observed.state.lock().unwrap().begun {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("batch worker must reach its held evaluation gate");
    let response = tokio::time::timeout(Duration::from_secs(5), receive)
        .await
        .expect("durable predecessor acknowledgement must not wait for batch worker")
        .unwrap()
        .unwrap();
    assert_eq!(response["value"]["value"], 1);
    assert!(!task.is_finished());
    assert_eq!(
        fixture
            .app
            .consensus
            .read_for_writer()
            .await
            .unwrap()
            .revision,
        2
    );
    drop(gate);
    let (outcome, (successor, deferred)) = task.await.unwrap();
    outcome.result.unwrap();
    assert!(deferred.is_empty());
    assert_eq!(successor.serial_worker_jobs, 1);
    assert_eq!(successor.serial_worker_requests, 2);
    assert_eq!(
        successor.results[1].as_ref().unwrap().response["value"]["value"],
        3
    );
    drop(successor);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_serial_worker_releases_admission_while_evaluation_gate_is_held() {
    let mut fixture = Fixture::new(8, false).await;
    Arc::get_mut(&mut fixture.app).unwrap().admission =
        admission::Pool::new([1, 1], [1, 1], [1 << 20, 1 << 20], 1);
    let mut state = fixture.app.consensus.read_for_writer().await.unwrap();
    let pending = serial_pass_inputs(3);
    let observed = pending
        .iter()
        .map(|entry| entry.input.clone())
        .collect::<Vec<_>>();
    let admission = admit(&fixture.app, &observed[0], false).await.unwrap();
    let gate = fixture
        .app
        .evaluations
        .clone()
        .acquire_owned()
        .await
        .unwrap();
    let app = fixture.app.clone();
    let task = tokio::spawn(async move {
        let mut controller = Controller::new(tuning::settings().unwrap());
        controller.fixed_for_test(3);
        controller.speculation = speculation::Policy::new(1, 8);
        prepare_group(
            &app,
            &mut state,
            pending,
            Preparation {
                read_us: 0,
                decision: controller.decide(3),
                admission: Some(admission),
            },
            &mut controller.speculation,
            None,
            None,
        )
        .await
    });
    tokio::time::timeout(Duration::from_secs(5), async {
        while !observed[0].state.lock().unwrap().begun {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(matches!(task.await, Err(error) if error.is_cancelled()));
    tokio::time::timeout(Duration::from_secs(5), async {
        while fixture.app.admission.metrics()["classes"][0]["active"] != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancellation must wake an evaluation-gate wait without opening the gate");
    assert!(!observed[1].state.lock().unwrap().begun);
    assert!(!observed[2].state.lock().unwrap().begun);
    assert_eq!(
        fixture
            .app
            .consensus
            .read_for_writer()
            .await
            .unwrap()
            .revision,
        1
    );
    drop(gate);
    fixture.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn serial_preparation_pass_reuses_one_slot_and_releases_it_before_commit() {
    for workers in [1, 4] {
        let mut fixture = Fixture::new(8, false).await;
        Arc::get_mut(&mut fixture.app).unwrap().admission =
            admission::Pool::new([workers, 1], [workers, 1], [1 << 20, 1 << 20], 1);
        let mut occupied = Vec::new();
        for _ in 1..workers {
            occupied.push(
                admission::acquire(&fixture.app, admission::Class::User, &Value::Null)
                    .await
                    .unwrap(),
            );
        }
        let pending = serial_pass_inputs(3);
        let admission = admit(&fixture.app, &pending[0].input, false).await.unwrap();
        let mut state = fixture.app.consensus.read_for_writer().await.unwrap();
        let reader_app = fixture.app.clone();
        let reader = tokio::spawn(async move {
            read_query(&reader_app, json!({"name":"counter.read","args":null})).await
        });
        wait_for_queued_query(&fixture.app).await;
        let mut controller = Controller::new(tuning::settings().unwrap());
        controller.fixed_for_test(3);
        controller.speculation = speculation::Policy::new(1, 8);
        let (mut group, deferred) = tokio::time::timeout(
            Duration::from_secs(10),
            prepare_group(
                &fixture.app,
                &mut state,
                pending,
                Preparation {
                    read_us: 0,
                    decision: controller.decide(3),
                    admission: Some(admission),
                },
                &mut controller.speculation,
                None,
                None,
            ),
        )
        .await
        .expect("reusing a one-slot pass must not reacquire its own slot");
        assert!(deferred.is_empty());
        assert_eq!(group.commands.len(), 3);
        assert_eq!(group.serial_worker_jobs, 1);
        assert_eq!(group.serial_worker_requests, 3);
        let result = tokio::time::timeout(Duration::from_secs(10), reader)
            .await
            .expect("query proceeds while prepared output awaits commit")
            .unwrap()
            .unwrap();
        assert_eq!(result.value, 0, "uncommitted mutations remain invisible");
        assert_eq!(
            fixture.app.admission.metrics()["admitted"],
            workers + 1,
            "all three serial methods share exactly one admission"
        );
        assert_eq!(
            fixture.app.admission.metrics()["classes"][0]["active"],
            workers - 1
        );
        commit_prepared_group(
            &fixture.app,
            std::mem::take(&mut group.commands),
            &group.results,
        )
        .await
        .unwrap();
        drop(group);
        drop(occupied);
        assert_eq!(fixture.app.admission.metrics()["classes"][0]["active"], 0);
        assert_eq!(
            fixture.app.admission.metrics()["classes"][0]["retainedInputBytes"],
            0
        );
        fixture.close().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canceled_preparation_pass_releases_its_slot_and_staged_output() {
    let mut fixture = Fixture::new(8, false).await;
    Arc::get_mut(&mut fixture.app).unwrap().admission =
        admission::Pool::new([1, 1], [1, 1], [1 << 20, 1 << 20], 1);
    let pending = serial_pass_inputs(3);
    let admission = admit(&fixture.app, &pending[0].input, false).await.unwrap();
    let mut state = fixture.app.consensus.read_for_writer().await.unwrap();
    let reader_app = fixture.app.clone();
    let reader = tokio::spawn(async move {
        read_query(&reader_app, json!({"name":"counter.read","args":null})).await
    });
    wait_for_queued_query(&fixture.app).await;
    let (events, mut event_rx) = mpsc::unbounded_channel();
    let app = fixture.app.clone();
    let preparing = tokio::spawn(async move {
        let hooks = Hooks { events };
        let mut controller = Controller::new(tuning::settings().unwrap());
        controller.fixed_for_test(3);
        prepare_group(
            &app,
            &mut state,
            pending,
            Preparation {
                read_us: 0,
                decision: controller.decide(3),
                admission: Some(admission),
            },
            &mut controller.speculation,
            None,
            Some(&hooks),
        )
        .await
    });
    let Event::Prepared { requests, proceed } = next_event(&mut event_rx).await else {
        panic!("expected fully prepared pass");
    };
    assert_eq!(requests, 3);
    assert!(!reader.is_finished());
    assert_eq!(fixture.app.admission.metrics()["admitted"], 1);
    preparing.abort();
    assert!(matches!(preparing.await, Err(error) if error.is_cancelled()));
    drop(proceed);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(10), reader)
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .value,
        0
    );
    assert_eq!(fixture.app.admission.metrics()["classes"][0]["active"], 0);
    assert_eq!(
        fixture.app.admission.metrics()["classes"][0]["retainedInputBytes"],
        0
    );
    fixture.close().await;
}

#[tokio::test]
async fn preparation_pass_transfers_its_slot_through_a_conflicting_wave() {
    let mut fixture = Fixture::new(8, false).await;
    Arc::get_mut(&mut fixture.app).unwrap().admission =
        admission::Pool::new([1, 1], [1, 1], [1 << 20, 1 << 20], 1);
    let pending = serial_pass_inputs(4);
    let admission = admit(&fixture.app, &pending[0].input, false).await.unwrap();
    let mut state = fixture.app.consensus.read_for_writer().await.unwrap();
    let mut controller = Controller::new(tuning::settings().unwrap());
    controller.fixed_for_test(4);
    controller.speculation = speculation::Policy::new(4, 8);
    let (group, deferred) = tokio::time::timeout(
        Duration::from_secs(10),
        prepare_group(
            &fixture.app,
            &mut state,
            pending,
            Preparation {
                read_us: 0,
                decision: controller.decide(4),
                admission: Some(admission),
            },
            &mut controller.speculation,
            None,
            None,
        ),
    )
    .await
    .expect("wave siblings must not wait behind a spare pass lease");
    assert!(deferred.is_empty());
    assert_eq!(group.commands.len(), 4);
    assert!(group.speculative_candidates > group.speculative_reused);
    for (index, result) in group.results.iter().enumerate() {
        assert_eq!(
            result.as_ref().unwrap().response["value"]["value"],
            index + 1
        );
    }
    assert_eq!(fixture.app.admission.metrics()["classes"][0]["active"], 0);
    drop(group);
    assert_eq!(
        fixture.app.admission.metrics()["classes"][0]["retainedInputBytes"],
        0
    );
    fixture.close().await;
}

async fn stage_writer_graph(app: &App, phase: &str, with_plan: bool) {
    let mut state = app.consensus.read_for_writer().await.unwrap();
    let generation = "a".repeat(64);
    let bundle = state.data["bundle"].clone();
    let mut puts: BTreeMap<String, Value> = BTreeMap::from([(
        evaluator::staging::JOB.into(),
        json!({"requestId":"writer-build","generation":generation,"phase":phase,"bundleHash":bundle["hash"]}),
    )]);
    if with_plan {
        puts.insert("deployment:plan".into(), json!({"bundle":bundle}));
        for (key, value) in &puts {
            state.data.insert(key.clone(), value.clone());
        }
        let roots = state
            .data
            .graph_roots()
            .map(|(_, root)| root.clone())
            .collect();
        let page = evaluator::staging::graph_page(
            state.data.clone(),
            json!({"requestId":"writer-build","bundle":bundle}),
            &generation,
            roots,
            1000,
        )
        .unwrap();
        puts.extend(page.puts);
    }
    app.consensus
        .commit(Commit {
            internal: false,
            request_id: "stage-writer-build".into(),
            fingerprint: "stage-writer-build".into(),
            expected_revision: state.revision,
            puts,
            deletes: vec![],
            result: Value::Null,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn graph_maintenance_skips_all_speculation_and_keeps_serial_batch_group_commit() {
    for phase in ["rebuilding", "ready"] {
        let fixture = Fixture::new(8, false).await;
        stage_writer_graph(&fixture.app, phase, true).await;
        let mut state = fixture.app.consensus.read_for_writer().await.unwrap();
        let revision = state.revision;
        let pending = serial_pass_inputs(4);
        let admission = admit(&fixture.app, &pending[0].input, false).await.unwrap();
        let mut controller = Controller::new(tuning::settings().unwrap());
        controller.fixed_for_test(4);
        controller.speculation = speculation::Policy::new(4, 8);
        controller.speculation.accepted();
        let mut decision = controller.decide(4);
        decision.budget = Duration::from_secs(10);
        let (mut group, deferred) = prepare_group(
            &fixture.app,
            &mut state,
            pending,
            Preparation {
                read_us: 0,
                decision,
                admission: Some(admission),
            },
            &mut controller.speculation,
            None,
            None,
        )
        .await;
        assert!(deferred.is_empty());
        assert_eq!(group.speculative_candidates, 0, "{phase}");
        assert_eq!(group.speculative_reused, 0, "{phase}");
        assert_eq!(group.serial_worker_jobs, 1, "{phase}");
        assert_eq!(group.serial_worker_requests, 4, "{phase}");
        assert_eq!(group.commands.len(), 4);
        assert_eq!(state.data[evaluator::staging::JOB]["phase"], phase);
        for (index, result) in group.results.iter().enumerate() {
            let response = &result.as_ref().unwrap().response;
            assert_eq!(response["value"]["value"], index + 1);
            assert_eq!(response["revision"], revision + index as u64 + 1);
        }
        commit_prepared_group(
            &fixture.app,
            std::mem::take(&mut group.commands),
            &group.results,
        )
        .await
        .unwrap();
        let durable = fixture.app.consensus.read_for_writer().await.unwrap();
        assert_eq!(durable, state);
        assert_eq!(
            durable.data["cell:[\"doubled\",null]"]["outcome"]["value"],
            8
        );
        assert_eq!(
            durable.data[&format!("graph:{}:cell:[\"doubled\",null]", "a".repeat(64))]["outcome"]["value"],
            8
        );
        drop(group);
        fixture.close().await;
    }
}

#[tokio::test]
async fn failed_graph_replay_restores_speculation_inside_the_same_preparation_group() {
    let fixture = Fixture::new(8, false).await;
    // A missing retained plan causes the first target replay to fail while
    // preserving the active mutation; the next request should probe normally.
    stage_writer_graph(&fixture.app, "rebuilding", false).await;
    let mut state = fixture.app.consensus.read_for_writer().await.unwrap();
    let pending = serial_pass_inputs(4);
    let admission = admit(&fixture.app, &pending[0].input, false).await.unwrap();
    let mut controller = Controller::new(tuning::settings().unwrap());
    controller.fixed_for_test(4);
    controller.speculation = speculation::Policy::new(4, 8);
    let mut decision = controller.decide(4);
    decision.budget = Duration::from_secs(10);
    let (group, deferred) = prepare_group(
        &fixture.app,
        &mut state,
        pending,
        Preparation {
            read_us: 0,
            decision,
            admission: Some(admission),
        },
        &mut controller.speculation,
        None,
        None,
    )
    .await;
    assert!(deferred.is_empty());
    assert_eq!(state.data[evaluator::staging::JOB]["phase"], "failed");
    assert_eq!(group.serial_worker_jobs, 1);
    assert_eq!(group.serial_worker_requests, 1);
    assert!(group.speculative_candidates >= 2);
    assert_eq!(group.commands.len(), 4);
    assert_eq!(state.data["source:[\"records\",\"value\"]"], 4);
    assert!(group.results.iter().all(Result::is_ok));
    drop(group);
    fixture.close().await;
}

#[tokio::test]
async fn conflicting_wave_preserves_independent_candidates_clock_and_order() {
    for workers in [1, 4] {
        let javascript = speculative_bundle().replace(
            "return {old,value:old+1,principal:ctx.principal()};",
            "return {old,value:old+1,principal:ctx.principal(),now:ctx.now()};",
        );
        let (_directory, app) = super::super::tests::application(javascript).await;
        let mut owned = Arc::try_unwrap(app).ok().expect("fixture has one owner");
        owned.admission = admission::Pool::new([workers, 1], [workers, 1], [1 << 20, 1 << 20], 1);
        let app = Arc::new(owned);
        let mut state = app.consensus.read_for_writer().await.unwrap();
        let revision = state.revision;
        let pending = ["a", "a", "b", "c"]
            .into_iter()
            .enumerate()
            .map(|(index, key)| {
                let (reply, _) = oneshot::channel();
                Pending {
                    input: speculative_input(&format!("salvage-{index}"), key),
                    deployment: false,
                    enqueued: Instant::now(),
                    reply,
                }
            })
            .collect();
        let mut controller = Controller::new(tuning::settings().unwrap());
        controller.fixed_for_test(4);
        controller.speculation = speculation::Policy::new(4, 8);
        controller.speculation.accepted();
        let mut decision = controller.decide(4);
        decision.budget = Duration::from_secs(10);
        let (group, deferred) = tokio::time::timeout(
            Duration::from_secs(10),
            prepare_group(
                &app,
                &mut state,
                pending,
                Preparation {
                    read_us: 0,
                    decision,
                    admission: None,
                },
                &mut controller.speculation,
                None,
                None,
            ),
        )
        .await
        .expect("conflict retries must release their lease before queued siblings");
        assert!(deferred.is_empty());
        assert_eq!(group.speculative_candidates, 4);
        assert_eq!(group.speculative_reused, 3);
        assert_eq!(group.commands.len(), 4);
        let now = group.results[0].as_ref().unwrap().response["value"]["now"].clone();
        for (index, result) in group.results.iter().enumerate() {
            let response = &result.as_ref().unwrap().response;
            assert_eq!(response["revision"], revision + index as u64 + 1);
            assert_eq!(response["value"]["old"], u64::from(index == 1));
            assert_eq!(response["value"]["now"], now);
        }
        assert_eq!(state.data["clock"], now);
        assert_eq!(state.data["source:[\"records\",\"a\"]"], 2);
        assert_eq!(state.data["source:[\"records\",\"b\"]"], 1);
        assert_eq!(state.data["source:[\"records\",\"c\"]"], 1);
        assert_eq!(app.admission.metrics()["classes"][0]["active"], 0);
        let outcome = group.commit_and_respond(&app, None).await;
        outcome.result.unwrap();
        let committed = app.consensus.read_for_writer().await.unwrap();
        assert_eq!(committed.revision, revision + 4);
        assert_eq!(committed.requests["salvage-1"].result["old"], 1);
        assert_eq!(
            app.admission.metrics()["classes"][0]["retainedInputBytes"],
            0
        );
        app.consensus.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn salvaged_wave_rechecks_authorization_cas_and_exact_replays() {
    for scenario in ["authorization", "cas", "replay"] {
        let (_directory, app) = super::super::tests::application(speculative_bundle()).await;
        let mut state = app.consensus.read_for_writer().await.unwrap();
        let revision = state.revision;
        let keys = if scenario == "authorization" {
            ["a", "role", "b", "c"]
        } else {
            ["a", "a", "b", "c"]
        };
        let pending = keys
            .into_iter()
            .enumerate()
            .map(|(index, key)| {
                let id = if scenario == "replay" && index == 1 {
                    0
                } else {
                    index
                };
                let mut input =
                    json!({"name":"write","args":{"key":key},"requestId":format!("guard-{id}")});
                if scenario == "cas" && index == 1 {
                    input["expectedRevision"] = json!(revision);
                }
                let (reply, _) = oneshot::channel();
                Pending {
                    input: input.into(),
                    deployment: false,
                    enqueued: Instant::now(),
                    reply,
                }
            })
            .collect();
        let mut controller = Controller::new(tuning::settings().unwrap());
        controller.fixed_for_test(4);
        controller.speculation = speculation::Policy::new(4, 8);
        controller.speculation.accepted();
        let mut decision = controller.decide(4);
        decision.budget = Duration::from_secs(10);
        let (group, deferred) = tokio::time::timeout(
            Duration::from_secs(10),
            prepare_group(
                &app,
                &mut state,
                pending,
                Preparation {
                    read_us: 0,
                    decision,
                    admission: None,
                },
                &mut controller.speculation,
                None,
                None,
            ),
        )
        .await
        .expect("guard rechecks cannot strand remaining wave admissions");
        assert!(deferred.is_empty());
        assert_eq!(group.speculative_candidates, 4);
        match scenario {
            "authorization" => {
                assert_eq!(group.speculative_reused, 2);
                for result in &group.results[2..] {
                    assert_eq!(
                        result.as_ref().unwrap().response["value"]["principal"]["claims"]["role"],
                        1
                    );
                }
            }
            "cas" => {
                assert_eq!(group.speculative_reused, 3);
                assert_eq!(
                    group.results[1].as_ref().err().unwrap().code,
                    "REVISION_CONFLICT"
                );
                assert_eq!(group.commands.len(), 3);
                assert_eq!(state.data["source:[\"records\",\"a\"]"], 1);
            }
            "replay" => {
                assert_eq!(group.speculative_reused, 3);
                let replay = &group.results[1].as_ref().unwrap().response;
                assert_eq!(replay["duplicate"], true);
                assert_eq!(replay["revision"], revision + 1);
                assert_eq!(replay["value"]["old"], 0);
                assert_eq!(group.commands.len(), 3);
                assert_eq!(state.data["source:[\"records\",\"a\"]"], 1);
            }
            _ => unreachable!(),
        }
        assert_eq!(app.admission.metrics()["classes"][0]["active"], 0);
        drop(group);
        assert_eq!(
            app.admission.metrics()["classes"][0]["retainedInputBytes"],
            0
        );
        app.consensus.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn maintenance_schedule_follows_hints_and_never_runs_twice_within_its_spacing() {
    let (_directory, app) = crate::service::tests::application("var __flowerBundle={default:{definitions:{},http:{}}};".into()).await;
    let spacing = Duration::from_millis(250);
    let mut schedule = Schedule::new(spacing);
    assert!(schedule.next.unwrap() <= Instant::now(), "the first run is immediate");
    schedule.ran(&app, Some(Ok(NextRun::Idle)));
    let last = schedule.last.unwrap();
    assert_eq!(schedule.next, None, "idle maintenance has no timer");
    schedule.soon();
    assert_eq!(schedule.next, Some(last + spacing), "a write reschedules, paced");
    schedule.ran(&app, Some(Ok(NextRun::Now)));
    assert_eq!(schedule.next, Some(schedule.last.unwrap() + spacing), "more work waits one spacing");
    for outcome in [Some(Ok(NextRun::Unknown)), Some(Err(anyhow::anyhow!("no quorum")))] {
        schedule.ran(&app, outcome);
        assert_eq!(schedule.next, Some(schedule.last.unwrap() + spacing), "polled and retried");
    }
    schedule.ran(&app, None);
    assert_eq!(schedule.next, None, "followers wait to lead");
    let now = app.clock.sample_after(0).unwrap();
    schedule.ran(&app, Some(Ok(NextRun::At(now + 10_000))));
    let delay = schedule.next.unwrap() - schedule.last.unwrap();
    assert!(delay > Duration::from_millis(9_900) && delay < Duration::from_millis(10_100), "{delay:?}");
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn mutations_that_write_nothing_keep_no_receipt_and_run_again() {
    let fixture = Fixture::new(16, true).await;
    // Adding zero rewrites nothing: no receipt, and a retry runs again.
    let unchanged = submit(&fixture.app, increment("unchanged", 0, None), false)
        .await
        .unwrap();
    assert_eq!(unchanged["value"]["value"], 0);
    let state = fixture.app.consensus.read().await.unwrap();
    assert!(!state.requests.contains_key("unchanged"));
    let again = submit(&fixture.app, increment("unchanged", 0, None), false)
        .await
        .unwrap();
    assert_eq!(again["duplicate"], false);
    assert!(again["revision"].as_u64() > unchanged["revision"].as_u64());
    // The same request ID may later carry a write, which is receipted.
    let written = submit(&fixture.app, increment("unchanged", 2, None), false)
        .await
        .unwrap();
    assert_eq!(written["value"]["value"], 2);
    let replay = submit(&fixture.app, increment("unchanged", 2, None), false)
        .await
        .unwrap();
    assert_eq!(replay["duplicate"], true);
    assert_eq!(replay["value"], written["value"]);
    let state = fixture.app.consensus.read().await.unwrap();
    assert_eq!(state.requests["unchanged"].revision, written["revision"]);
    fixture.close().await;
}

#[tokio::test]
async fn mutations_declared_without_receipts_run_again_on_retry() {
    let fixture = Fixture::new(16, true).await;
    let javascript = r#"
        const records = {kind:'collection',name:'records'};
        var __flowerBundle = {default:{definitions:{
            append:{name:'append',kind:'mutationMethod',receipt:false,compute:(ctx)=>{
                const count = (ctx.get(records,'count') || 0) + 1;
                ctx.set(records,'count',count);
                return count;
            }}
        },http:{'log.append':{name:'append',kind:'mutation',receipt:false}}}};
    "#;
    let deployed = submit(
        &fixture.app,
        json!({"requestId":"deploy-unreceipted","bundle":{"hash":evaluator::hash(javascript.as_bytes()),"javascript":javascript}}),
        true,
    )
    .await
    .unwrap();
    assert_eq!(deployed["duplicate"], false);
    let call = json!({"requestId":"append-1","name":"log.append"});
    let first = submit(&fixture.app, call.clone(), false).await.unwrap();
    let again = submit(&fixture.app, call, false).await.unwrap();
    assert_eq!((first["value"].clone(), again["value"].clone()), (json!(1), json!(2)));
    assert_eq!(again["duplicate"], false);
    let state = fixture.app.consensus.read().await.unwrap();
    assert!(!state.requests.contains_key("append-1"));
    assert_eq!(
        http_method(&state, "log.append", Some(MethodKind::Mutation)).unwrap().receipt,
        false
    );
    fixture.close().await;
}

#[tokio::test]
async fn candidates_a_group_does_not_reach_carry_into_the_next() {
    let (_directory, app) = super::super::tests::application(speculative_bundle()).await;
    let mut state = app.consensus.read_for_writer().await.unwrap();
    let pending: Vec<_> = (0..6)
        .map(|index| {
            let (reply, _) = oneshot::channel();
            Pending {
                input: speculative_input(&format!("carry-{index}"), &format!("key{index}")),
                deployment: false,
                enqueued: Instant::now(),
                reply,
            }
        })
        .collect();
    let mut controller = Controller::new(tuning::settings().unwrap());
    controller.fixed_for_test(6);
    controller.speculation = speculation::Policy::new(8, 16);
    controller.speculation.accepted();
    controller.speculation.accepted();
    let mut carried = None;
    // No time to prepare: the group stops after its first call.
    let mut decision = controller.decide(6);
    decision.budget = Duration::ZERO;
    let (first, rest) = prepare_group_carried(
        &app,
        &mut state,
        pending,
        Preparation {
            read_us: 0,
            decision,
            admission: None,
        },
        &mut controller.speculation,
        None,
        &mut carried,
        None,
    )
    .await;
    assert_eq!(first.results.len(), 1);
    assert_eq!(first.speculative_candidates, 6);
    assert_eq!(rest.len(), 5);
    assert_eq!(carried.as_ref().map(speculation::Wave::len), Some(5));
    let mut decision = controller.decide(5);
    decision.budget = Duration::from_secs(10);
    let (second, rest) = prepare_group_carried(
        &app,
        &mut state,
        rest,
        Preparation {
            read_us: 0,
            decision,
            admission: None,
        },
        &mut controller.speculation,
        None,
        &mut carried,
        None,
    )
    .await;
    assert!(rest.is_empty());
    assert!(carried.is_none());
    assert_eq!(second.speculative_candidates, 0, "nothing prepared twice");
    assert_eq!(second.speculative_reused, 5);
    assert_eq!(
        second.revision,
        first.revision + 5,
        "the successor follows its predecessor's writes"
    );
    app.consensus.shutdown().await.unwrap();
}
