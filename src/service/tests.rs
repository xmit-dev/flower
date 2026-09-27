use std::{collections::BTreeMap, time::Instant};

use tempfile::TempDir;

use super::*;

mod cache_http;
mod mutation_dispatch;

#[test]
fn http_registry_borrowed_entries_preserve_validation_and_resolution() {
    let mut state = Snapshot::default();
    state.data.insert(
        "httpMethods".into(),
        json!({
            "alias": {"name":"resolved", "kind":"query", "consistency":"replica-local"},
            "fresh": {"name":"resolved", "kind":"query"},
            "write": {"name":"resolved", "kind":"mutation"},
            "unknown": {"name":"resolved", "kind":"query", "extra":true},
            "null_consistency": {"name":"resolved", "kind":"query", "consistency":null},
            "write_consistency": {"name":"resolved", "kind":"mutation", "consistency":"linearizable"},
            "bad_name": {"name":3, "kind":"query"},
            "bad_kind": {"name":"resolved", "kind":"other"}
        }),
    );
    let before = state.data.clone();
    let method = http_method(&state, "alias", Some(MethodKind::Query)).unwrap();
    assert_eq!(method.name, "resolved");
    assert_eq!(method.consistency, QueryConsistency::ReplicaLocal);
    assert_eq!(
        http_method(&state, "fresh", None).unwrap().consistency,
        QueryConsistency::Linearizable
    );
    assert_eq!(
        http_method(&state, "write", Some(MethodKind::Query))
            .unwrap_err()
            .code,
        "METHOD_KIND_MISMATCH"
    );
    assert_eq!(
        http_method(&state, "missing", None).unwrap_err().code,
        "METHOD_NOT_FOUND"
    );
    for alias in [
        "unknown",
        "null_consistency",
        "write_consistency",
        "bad_name",
        "bad_kind",
    ] {
        let error = http_method(&state, alias, None).unwrap_err();
        assert_eq!(error.status, StatusCode::INTERNAL_SERVER_ERROR, "{alias}");
        assert_eq!(error.code, "INVALID_METHOD_REGISTRY", "{alias}");
    }
    assert_eq!(state.data, before);
}

fn bundle(run: &str, on_error: Option<&str>) -> String {
    let mut javascript = String::from(
        r#"const records = {kind:'collection',name:'records'};
        const stable = {kind:'derived',name:'stable'};
        const transient = {kind:'derived',name:'transient'};
        var __flowerBundle = {default:{definitions:{
            stable:{name:'stable',kind:'derived',compute:(ctx)=>ctx.get(records,'value') * 2},
            transient:{name:'transient',kind:'derived',compute:(ctx)=>ctx.now()},
            run:{name:'run',kind:'mutationMethod',compute:(ctx,args)=>{"#,
    );
    javascript.push_str(run);
    javascript.push_str("}}, recover:{name:'recover',kind:'mutationMethod',compute:(ctx,args)=>{");
    javascript.push_str(on_error.unwrap_or("throw new Error('Unexpected recovery');"));
    javascript.push_str("}}},http:{},maintenance:{name:'run',kind:'mutation'");
    if on_error.is_some() {
        javascript.push_str(",onError:{name:'recover',kind:'mutation'}");
    }
    javascript.push_str("}}};");
    javascript
}

pub(super) async fn application(javascript: String) -> (TempDir, Arc<App>) {
    application_with_admission(javascript, None).await
}

async fn application_with_admission(
    javascript: String,
    pool: Option<Arc<admission::Pool>>,
) -> (TempDir, Arc<App>) {
    build_application(javascript, pool, false).await
}

/// The production writer actor, which schedules maintenance on its own.
pub(super) async fn application_with_maintenance(javascript: String) -> (TempDir, Arc<App>) {
    build_application(javascript, None, true).await
}

async fn build_application(
    javascript: String,
    pool: Option<Arc<admission::Pool>>,
    automatic_maintenance: bool,
) -> (TempDir, Arc<App>) {
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
        loop {
            if consensus.read().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let evaluation = evaluator::evaluate_at(
        BTreeMap::new(),
        json!({
            "requestId":"seed",
            "bundle":{"hash":evaluator::hash(javascript.as_bytes()),"javascript":javascript},
            "writes":[
                {"collection":"records","key":"value","value":6},
                {"collection":"records","key":"retained","value":true},
            ],
            "materialize":[{"name":"stable","args":null}],
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
    let (writer_queue, receiver) = mpsc::channel(128);
    let app = Arc::new(App {
        consensus,
        writer: Mutex::new(()),
        writer_queue,
        evaluations: Arc::new(Semaphore::new(1)),
        query_evaluations: Arc::new(Semaphore::new(4)),
        admission: pool.unwrap_or_else(|| crate::service::admission::Pool::configured().unwrap()),
        query_cache: query_cache::QueryCache::default(),
        watch_hubs: crate::service::watch::hubs::Registry::default(),
        admin_token: "test-only-secret".into(),
        clock: clock::Clock::new(),
        cross_group: transactions::Runtime::new().unwrap(),
        partition_gate: None,
    });
    if automatic_maintenance {
        tokio::spawn(writer::run(Arc::downgrade(&app), receiver));
    } else {
        tokio::spawn(writer::run_without_maintenance(
            Arc::downgrade(&app),
            receiver,
        ));
    }
    (directory, app)
}

#[test]
fn maintenance_hints_say_when_to_run_again() {
    for (value, next) in [
        (json!({"$flower":{"continue":true,"next":5_000}}), NextRun::Now),
        (json!({"$flower":{"continue":false,"next":null}}), NextRun::Idle),
        (json!({"$flower":{"next":2_000}}), NextRun::At(2_000)),
        (json!({"$flower":{"next":1_500.2}}), NextRun::At(1_501)),
        (json!({"$flower":{"next":1_000}}), NextRun::Now),
        (json!({"$flower":{"next":"soon"}}), NextRun::Unknown),
        (json!({"task":"x"}), NextRun::Unknown),
        (Value::Null, NextRun::Unknown),
    ] {
        assert_eq!(NextRun::of(&value, 1_000), next, "{value}");
    }
}

#[tokio::test]
async fn maintenance_sleeps_until_the_declared_due_time() {
    // Runs only once records.due has passed, and says when that is.
    let javascript = bundle(
        r#"
        const now=ctx.now(), due=ctx.get(records,'due');
        if(due===null || ctx.get(records,'ran')!==null) return {$flower:{next:null}};
        if(now<due) return {$flower:{next:due}};
        ctx.set(records,'ran',now);
        return {$flower:{next:null}};
        "#,
        None,
    );
    let (_directory, app) = application_with_maintenance(javascript).await;
    let state = app.consensus.read().await.unwrap();
    let due = app.clock.sample(&state).unwrap() + 700;
    app.consensus
        .commit(Commit {
            internal: true,
            request_id: "due".into(),
            fingerprint: String::new(),
            expected_revision: state.revision,
            puts: BTreeMap::from([("source:[\"records\",\"due\"]".to_owned(), json!(due))]),
            deletes: vec![],
            result: Value::Null,
        })
        .await
        .unwrap();
    let ran = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let state = app.consensus.read().await.unwrap();
            if let Some(ran) = state.data.get("source:[\"records\",\"ran\"]") {
                return ran.as_u64().unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(ran >= due, "ran at {ran}, due {due}");
    assert!(ran < due + 150, "ran {} ms late", ran - due);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn maintenance_failure_rolls_back_sources_previews_and_roots_before_recovery() {
    let javascript = bundle(
        r#"
        ctx.set(records,'value',99);
        ctx.set(records,'partial',ctx.now());
        ctx.delete(records,'retained');
        if(ctx.get(stable)!==198) throw new Error('preview did not observe staged writes');
        ctx.unmaterialize(stable);
        ctx.materialize(transient);
        ctx.get(transient);
        throw new Error('business failure');
        "#,
        Some(
            r#"
            if(ctx.get(records,'value')!==6 || ctx.get(stable)!==12 ||
               ctx.get(records,'partial')!==null || ctx.get(records,'retained')!==true)
                throw new Error('failed transaction leaked');
            ctx.set(records,'recovered',{now:ctx.now(),args});
            return null;
            "#,
        ),
    );
    let (_directory, app) = application(javascript).await;
    let before = app.consensus.read().await.unwrap();
    maintain(&app).await.unwrap();
    let after = app.consensus.read().await.unwrap();
    assert_eq!(after.revision, before.revision + 1);
    assert_eq!(after.requests, before.requests);
    assert_eq!(after.data["source:[\"records\",\"value\"]"], 6);
    assert_eq!(after.data["source:[\"records\",\"retained\"]"], true);
    assert!(!after.data.contains_key("source:[\"records\",\"partial\"]"));
    for key in ["root:[\"stable\",null]", "cell:[\"stable\",null]"] {
        assert_eq!(after.data.get(key), before.data.get(key));
        assert!(after.data.contains_key(key));
    }
    assert!(!after.data.contains_key("root:[\"transient\",null]"));
    assert!(!after.data.contains_key("cell:[\"transient\",null]"));
    let recovery = &after.data["source:[\"records\",\"recovered\"]"];
    assert_eq!(after.data["clock"], recovery["now"]);
    assert!(recovery["now"].as_u64().unwrap() > 1000);
    assert_eq!(recovery["args"]["error"]["code"], "COMPUTE_ERROR");
    assert!(
        recovery["args"]["error"]["message"]
            .as_str()
            .unwrap()
            .contains("business failure")
    );
    assert!(recovery["args"]["failedAt"].as_u64().unwrap() >= recovery["now"].as_u64().unwrap());
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn failure_in_recovery_rolls_back_everything_and_allows_redeployment() {
    let (_directory, app) = application(bundle(
        "ctx.set(records,'partial',true); throw new Error('run failed');",
        Some("ctx.set(records,'recoveryPartial',true); throw new Error('recovery failed');"),
    ))
    .await;
    let before = app.consensus.read().await.unwrap();
    let error = maintain(&app).await.unwrap_err();
    assert!(error.to_string().contains("recovery failed"));
    assert_eq!(app.consensus.read().await.unwrap(), before);

    let repaired = bundle("ctx.set(records,'fixed',true); return null;", None);
    let _ = commit_method(
        app.clone(),
        json!({"requestId":"repair","bundle":{"hash":evaluator::hash(repaired.as_bytes()),"javascript":repaired}}),
        true,
    )
    .await
    .unwrap_or_else(|error| panic!("{}: {}", error.code, error.message));
    maintain(&app).await.unwrap();
    let after = app.consensus.read().await.unwrap();
    assert_eq!(after.data["source:[\"records\",\"fixed\"]"], true);
    assert!(!after.data.contains_key("source:[\"records\",\"partial\"]"));
    assert!(
        !after
            .data
            .contains_key("source:[\"records\",\"recoveryPartial\"]")
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn successful_and_legacy_maintenance_never_invoke_recovery_or_commit_idle_ticks() {
    for on_error in [None, Some("throw new Error('must not be invoked');")] {
        let (_directory, app) =
            application(bundle("ctx.set(records,'value',7); return null;", on_error)).await;
        maintain(&app).await.unwrap();
        let after = app.consensus.read().await.unwrap();
        assert_eq!(after.revision, 2);
        assert_eq!(after.data["source:[\"records\",\"value\"]"], 7);
        assert_eq!(after.requests.len(), 1);
        maintain(&app).await.unwrap();
        assert_eq!(app.consensus.read().await.unwrap(), after);
        app.consensus.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn exhausted_callback_has_fresh_recovery_budget_and_original_time() {
    let (_directory, app) = application(bundle(
        "ctx.set(records,'partial',ctx.now()); while(true) {}",
        Some(
            "ctx.set(records,'recovered',{now:ctx.now(),args,partial:ctx.get(records,'partial')}); return null;",
        ),
    ))
    .await;
    let before = app.consensus.read().await.unwrap();
    let budget = crate::evaluator::config::settings()
        .unwrap()
        .evaluation_timeout;
    let started = Instant::now();
    maintain(&app).await.unwrap();
    // The loop spends one whole budget; the recovery runs on a fresh one and returns quickly.
    assert!(started.elapsed() >= budget * 4 / 5);
    assert!(started.elapsed() < budget * 2);
    let after = app.consensus.read().await.unwrap();
    assert_eq!(after.revision, before.revision + 1);
    assert_eq!(after.requests, before.requests);
    assert!(!after.data.contains_key("source:[\"records\",\"partial\"]"));
    let recovery = &after.data["source:[\"records\",\"recovered\"]"];
    assert_eq!(recovery["partial"], Value::Null);
    assert_eq!(recovery["args"]["error"]["code"], "EVALUATION_BUDGET");
    assert!(
        recovery["args"]["failedAt"].as_u64().unwrap() >= recovery["now"].as_u64().unwrap() + 4000
    );
    assert_eq!(after.data["clock"], recovery["now"]);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn maintenance_continuation_is_opt_in_bounded_and_stops_when_idle() {
    let (_directory, app) = application(bundle(
        "ctx.set(records,'value',ctx.get(records,'value')+1); return {$flower:{continue:true}};",
        None,
    ))
    .await;
    let before = app.consensus.read().await.unwrap();
    maintain(&app).await.unwrap();
    let after = app.consensus.read().await.unwrap();
    let calls = after.revision - before.revision;
    assert!(calls >= 1);
    assert_eq!(after.data["source:[\"records\",\"value\"]"], 6 + calls);
    assert_eq!(after.requests, before.requests);
    app.consensus.shutdown().await.unwrap();

    let (_directory, app) = application(bundle("return {$flower:{continue:true}};", None)).await;
    let before = app.consensus.read().await.unwrap();
    assert!(!maintain_one(&app).await.unwrap());
    assert_eq!(app.consensus.read().await.unwrap(), before);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn catch_up_failure_cannot_undo_a_preceding_callback_commit() {
    let (_directory, app) = application(bundle(
        "if(ctx.get(records,'value')===6){ctx.set(records,'value',7); return {$flower:{continue:true}};} ctx.set(records,'partial',true); throw new Error('second callback failed');",
        Some("ctx.set(records,'recovered',true); return {$flower:{continue:false}};"),
    )).await;
    let before = app.consensus.read().await.unwrap();
    assert!(maintain_one(&app).await.unwrap());
    assert!(!maintain_one(&app).await.unwrap());
    let after = app.consensus.read().await.unwrap();
    assert_eq!(after.revision, before.revision + 2);
    assert_eq!(after.requests, before.requests);
    assert_eq!(after.data["source:[\"records\",\"value\"]"], 7);
    assert_eq!(after.data["source:[\"records\",\"recovered\"]"], true);
    assert!(!after.data.contains_key("source:[\"records\",\"partial\"]"));
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn catch_up_group_flushes_successful_prefix_when_a_later_callback_fails() {
    let (_directory, app) = application(bundle(
        "if(ctx.get(records,'value')===6){ctx.set(records,'value',7); return {$flower:{continue:true}};} ctx.set(records,'partial',true); throw new Error('second callback failed');",
        None,
    )).await;
    let before = app.consensus.read().await.unwrap();
    // Slow test machines can end the bounded burst after its first callback.
    // In that case, the next burst observes that same durable prefix.
    if maintain(&app).await.is_ok() {
        assert!(maintain(&app).await.is_err());
    }
    let after = app.consensus.read().await.unwrap();
    assert_eq!(after.revision, before.revision + 1);
    assert_eq!(after.requests, before.requests);
    assert_eq!(after.data["source:[\"records\",\"value\"]"], 7);
    assert!(!after.data.contains_key("source:[\"records\",\"partial\"]"));
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn query_certificates_require_quorum_admission_and_clock_independence() {
    let javascript = r#"
        const records = {kind:'collection',name:'records'};
        const transient = {kind:'derived',name:'transient'};
        var __flowerBundle = {default:{definitions:{
            stable:{name:'stable',kind:'derived',compute:(ctx)=>ctx.get(records,'value')},
            transient:{name:'transient',kind:'derived',compute:(ctx)=>ctx.now()},
            plain:{name:'plain',kind:'queryMethod',compute:(ctx)=>ctx.get(records,'value')},
            clock:{name:'clock',kind:'queryMethod',compute:(ctx)=>ctx.now()},
            derivedClock:{name:'derivedClock',kind:'queryMethod',compute:(ctx)=>ctx.get(transient)},
            unrelated:{name:'unrelated',kind:'mutationMethod',compute:(ctx,args)=>{ctx.set(records,'ignored',args);return args;}},
            change:{name:'change',kind:'mutationMethod',compute:(ctx,args)=>{ctx.set(records,'value',args); return args;}}
        },http:{
            plain:{name:'plain',kind:'query'},clock:{name:'clock',kind:'query'},
            derivedClock:{name:'derivedClock',kind:'query'},unrelated:{name:'unrelated',kind:'mutation'},change:{name:'change',kind:'mutation'}
        }}};
    "#;
    let (_directory, app) = application(javascript.into()).await;
    let plain = json!({"name":"plain","args":null});
    let key = serde_json::to_string(&json!({"invocation":plain,"principal":null})).unwrap();
    let first = query(State(app.clone()), Json(plain.clone()))
        .await
        .unwrap()
        .0;
    assert_eq!(first["value"], 6);
    assert_eq!(
        app.query_cache
            .get(1, &key, &app.consensus.read_query().await.unwrap().data),
        Some(json!(6))
    );
    // An unrelated committed source update preserves the certificate.
    let _ = commit_method(
        app.clone(),
        json!({"name":"unrelated","args":42,"requestId":"unrelated"}),
        false,
    )
    .await
    .unwrap();
    let snapshot = app.consensus.read_query().await.unwrap();
    assert_eq!(snapshot.revision, 2);
    assert_eq!(app.query_cache.get(2, &key, &snapshot.data), Some(json!(6)));
    drop(snapshot);
    // Decoded evaluations (including watches and authorization) still pass full
    // admission before capturing roots. HTTP's bounded public-hit path has
    // separate coverage below and never queues while retaining a snapshot.
    let capacity = app.admission.metrics()["classes"][0]["workerBudget"]
        .as_u64()
        .unwrap();
    let mut permits = Vec::new();
    for _ in 0..capacity {
        permits.push(
            admission::acquire(&app, admission::Class::User, &Value::Null)
                .await
                .unwrap(),
        );
    }
    let mut cached = Box::pin(query(State(app.clone()), Json(plain.clone())));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut cached)
            .await
            .is_err()
    );
    assert_eq!(app.admission.metrics()["classes"][0]["queued"], 1);
    drop(permits);
    assert_eq!(cached.await.unwrap().0["value"], 6);
    for name in ["clock", "derivedClock"] {
        let input = json!({"name":name,"args":null});
        let before = query(State(app.clone()), Json(input.clone()))
            .await
            .unwrap()
            .0;
        tokio::time::sleep(Duration::from_millis(2)).await;
        let after = query(State(app.clone()), Json(input.clone()))
            .await
            .unwrap()
            .0;
        assert!(after["value"].as_u64().unwrap() > before["value"].as_u64().unwrap());
        assert_eq!(
            app.query_cache.get(
                1,
                &serde_json::to_string(&json!({"invocation":input,"principal":null})).unwrap(),
                &app.consensus.read_query().await.unwrap().data
            ),
            None
        );
    }
    let _ = commit_method(
        app.clone(),
        json!({"requestId":"change","name":"change","args":17}),
        false,
    )
    .await
    .unwrap();
    let changed = query(State(app.clone()), Json(plain.clone()))
        .await
        .unwrap()
        .0;
    assert_eq!(changed["revision"], 3);
    assert_eq!(changed["value"], 17);
    assert_eq!(
        app.query_cache
            .get(3, &key, &app.consensus.read_query().await.unwrap().data),
        Some(json!(17))
    );
    app.consensus.shutdown().await.unwrap();
    assert_eq!(
        query(State(app), Json(plain)).await.unwrap_err().status,
        StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn reads_and_writes_have_independent_execution_capacity() {
    let javascript = r#"
        const records = {kind:'collection',name:'records'};
        var __flowerBundle = {default:{definitions:{
            stable:{name:'stable',kind:'derived',compute:(ctx)=>ctx.get(records,'value')},
            read:{name:'read',kind:'queryMethod',compute:(ctx,args)=>({value:ctx.get(records,'value'),args})},
            change:{name:'change',kind:'mutationMethod',compute:(ctx,args)=>{ctx.set(records,'value',args);return args;}}
        },http:{read:{name:'read',kind:'query'},change:{name:'change',kind:'mutation'}}}};
    "#;
    let (_directory, app) = application(javascript.into()).await;
    let writers = app.evaluations.clone().acquire_owned().await.unwrap();
    let reads = (0..8).map(|args| read_query(&app, json!({"name":"read","args":args})));
    let results = tokio::time::timeout(
        Duration::from_secs(2),
        futures_util::future::join_all(reads),
    )
    .await
    .unwrap();
    for (args, result) in results.into_iter().enumerate() {
        assert_eq!(result.unwrap().value, json!({"value":6,"args":args}));
    }
    drop(writers);
    let readers = app
        .query_evaluations
        .clone()
        .acquire_many_owned(4)
        .await
        .unwrap();
    let write = tokio::time::timeout(
        Duration::from_secs(2),
        commit_method(
            app.clone(),
            json!({"name":"change","args":9,"requestId":"independent-writer"}),
            false,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(write.0["value"], 9);
    drop(readers);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn coalesced_clock_queries_resample_after_reacquiring_the_snapshot() {
    let javascript = r#"
        const records = {kind:'collection',name:'records'};
        var __flowerBundle = {default:{definitions:{
            stable:{name:'stable',kind:'derived',compute:(ctx)=>ctx.get(records,'value')},
            time:{name:'time',kind:'queryMethod',compute:(ctx)=>ctx.now()}
        },http:{time:{name:'time',kind:'query'}}}};
    "#;
    let (_directory, app) = application(javascript.into()).await;
    let state = app.consensus.read_query().await.unwrap();
    let method = http_method(&state, "time", Some(MethodKind::Query)).unwrap();
    let calls = (1001..1017).map(|now| {
        let app = app.clone();
        let state = state.clone();
        let method = method.clone();
        async move {
            let input = json!({"name":"time"});
            let permit = admission::acquire(&app, admission::Class::User, &input)
                .await
                .unwrap();
            evaluate_query(&app, state, input, method, now, permit).await
        }
    });
    let results = futures_util::future::join_all(calls).await;
    for (result, now) in results.into_iter().zip(1001..1017) {
        let result = result.unwrap();
        let observed = result.value.as_u64().unwrap();
        assert!(
            observed == now || observed > 1017,
            "A retried query uses a fresh host-sampled clock, never another waiter's clock"
        );
        assert_eq!(result.validity, Validity::Polled, "ctx.now() promises no change time");
    }
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coalesced_query_waiters_release_workers_and_reauthorize_fresh_state() {
    let javascript = r#"
        const records = {kind:'collection',name:'records'};
        var __flowerBundle = {default:{definitions:{
          stable:{kind:'derived',name:'stable',compute:ctx=>ctx.get(records,'value')},
          authorize:{kind:'queryMethod',name:'authorize',compute:(ctx,args)=>{
            if(args.credentials==='expired' && ctx.get(records,'blocked')) throw new Error('revoked');
            return {subject:'shared'};
          }},
          read:{kind:'queryMethod',name:'read',compute:ctx=>ctx.get(records,'value')},
          change:{kind:'mutationMethod',name:'change',compute:(ctx,args)=>{
            ctx.set(records,'value',args);ctx.set(records,'blocked',true);return args;
          }}
        },authorize:{name:'authorize'},http:{read:{name:'read',kind:'query'},change:{name:'change',kind:'mutation'}}}};
    "#;
    let (_directory, app) = application_with_admission(
        javascript.into(),
        Some(admission::Pool::new([2, 1], [2, 1], [1 << 20, 1 << 20], 1)),
    )
    .await;
    // Model a producer paused in native work while retaining its own one slot.
    let producer = admission::acquire(&app, admission::Class::User, &Value::Null)
        .await
        .unwrap();
    let key = serde_json::to_string(
        &json!({"invocation":{"name":"read","args":null},"principal":{"subject":"shared"}}),
    )
    .unwrap();
    let flight = app.query_cache.flight(1, &key).unwrap();
    let guard = flight.lock().await;
    let waiters = (0..4).map(|index| {
        let app = app.clone();
        tokio::spawn(async move { read_query(&app,json!({"name":"read","args":null,"credentials":if index<2 {"expired"} else {"live"}})).await })
    }).collect::<Vec<_>>();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let metrics = app.admission.metrics();
            if metrics["admitted"].as_u64().unwrap() >= 5 && metrics["classes"][0]["active"] == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("Coalescing waiters must release execution slots");
    assert!(waiters.iter().all(|waiter| !waiter.is_finished()));
    assert!(
        app.admission.metrics()["classes"][0]["retainedInputBytes"]
            .as_u64()
            .unwrap()
            > 512,
        "Waiting inputs remain byte-accounted"
    );
    let unrelated = tokio::time::timeout(
        Duration::from_secs(5),
        read_query(
            &app,
            json!({"name":"read","args":"unrelated","credentials":"live"}),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(unrelated.value, 6);
    let _ = tokio::time::timeout(
        Duration::from_secs(5),
        commit_method(
            app.clone(),
            json!({"name":"change","args":9,"requestId":"during-flight","credentials":"live"}),
            false,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    // A new producer for the newly committed revision must not make these
    // requests chase another moving snapshot after their first coalesced wait.
    let newer_flight = app.query_cache.flight(2, &key).unwrap();
    let newer_guard = newer_flight.lock().await;
    drop(guard);
    drop(producer);
    for (index, waiter) in waiters.into_iter().enumerate() {
        let result = tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .unwrap()
            .unwrap();
        if index < 2 {
            assert_eq!(result.err().unwrap().code, "FORBIDDEN");
        } else {
            let result = result.unwrap();
            assert_eq!(result.revision, 2);
            assert_eq!(result.value, 9);
        }
    }
    assert_eq!(app.admission.metrics()["classes"][0]["active"], 0);
    drop(newer_guard);
    app.consensus.shutdown().await.unwrap();
}
