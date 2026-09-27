use super::*;

mod graphs;

fn bundle(indexed: bool, denied: bool, root: &str) -> String {
    let indexes = if indexed {
        "{kind:['kind'],shop:['shop']}"
    } else {
        "{kind:['kind']}"
    };
    let authorization = if denied {
        ",authorize:{name:'authorize'}"
    } else {
        ""
    };
    format!(
        r#"
const rows={{kind:'collection',name:'rows'}};
const query={{kind:'query',collection:'rows',fields:['shop'],value:'north'}};
var __flowerBundle={{default:{{collections:[{{name:'rows',indexes:{indexes}}}],definitions:{{
 stable:{{kind:'derived',name:'stable',compute:ctx=>{{{root}}}}},
 write:{{kind:'mutationMethod',name:'write',compute:(ctx,args)=>{{for(const row of args){{if(row.remove)ctx.delete(rows,row.key);else ctx.set(rows,row.key,row.value);}}return ctx.query(query).length;}}}},
 read:{{kind:'queryMethod',name:'read',compute:ctx=>({{rows:ctx.query(query),derived:ctx.get({{kind:'derived',name:'stable'}})}})}},
 authorize:{{kind:'queryMethod',name:'authorize',compute:()=>{{throw new Error('new policy');}}}}
}},http:{{write:{{name:'write',kind:'mutation'}},read:{{name:'read',kind:'query'}}}}{authorization}}}}};
"#
    )
}
fn stage_input(id: &str, code: String) -> Value {
    json!({"operation":"stage","requestId":id,"bundle":{"hash":evaluator::hash(code.as_bytes()),"javascript":code}})
}
async fn write(app: &App, id: &str, args: Value) -> Value {
    writer::submit(
        app,
        json!({"requestId":id,"name":"write","args":args}),
        false,
    )
    .await
    .unwrap()
}
async fn query(app: &App) -> Value {
    let state = app.consensus.read_query().await.unwrap();
    evaluator::invoke_at(
        state.data,
        json!({"name":"read","args":null}),
        "query",
        1000,
    )
    .unwrap()
    .value
}
async fn finish(app: &App, id: &str) -> Value {
    for _ in 0..100 {
        let state = administer(
            app,
            json!({"operation":"advance","requestId":id,"maxBytes":16_384}),
        )
        .await
        .unwrap();
        if state["value"]["phase"] == "ready" {
            return state;
        }
    }
    panic!("backfill did not converge");
}
async fn collect_all(app: &App, id: &str) {
    for _ in 0..100 {
        let state = administer(
            app,
            json!({"operation":"collect","requestId":id,"maxBytes":16_384}),
        )
        .await
        .unwrap();
        if state["value"]["phase"] == "collected" {
            return;
        }
    }
    panic!("cleanup did not converge");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_backfill_dual_maintenance_and_atomic_policy_cutover() {
    let code = bundle(false, false, "return ctx.query(query).length;");
    let (_directory, app) = super::super::tests::application(code).await;
    let initial: Vec<_> = (0..8)
        .map(|n| json!({"key":format!("row-{n}"),"value":{"kind":"pizza","shop":"north","n":n}}))
        .collect();
    write(&app, "seed-rows", json!(initial)).await;
    let before = app.consensus.read_for_writer().await.unwrap();
    let speculative=evaluator::invoke_speculative_as(before.data.clone(),json!({"requestId":"old-candidate","name":"write","args":[{"key":"old","value":{"kind":"pizza","shop":"north"}}]}),1000,Value::Null).unwrap();
    let input = stage_input(
        "build",
        bundle(true, true, "return 10*ctx.query(query).length;"),
    );
    let started = administer(&app, input.clone()).await.unwrap();
    assert_eq!(started["value"]["phase"], "backfill");
    let admitted = app.consensus.read_for_writer().await.unwrap();
    assert!(
        !speculative
            .mutation_certificate
            .unwrap()
            .valid(&admitted.data),
        "pre-stage candidates must re-evaluate with dual maintenance"
    );
    assert_eq!(admitted.data["schema"], before.data["schema"]);
    assert_eq!(admitted.data["bundle"], before.data["bundle"]);
    let partial = administer(
        &app,
        json!({"operation":"advance","requestId":"build","maxBytes":900}),
    )
    .await
    .unwrap();
    assert_eq!(partial["value"]["phase"], "backfill");
    assert!(partial["value"]["scannedRows"].as_u64().unwrap() > 0);
    write(
        &app,
        "interleaved",
        json!([
            {"key":"row-0","value":{"kind":"pizza","shop":"south"}},
            {"key":"row-6","remove":true},
            {"key":"row-2","value":{"kind":"pizza","shop":"north","n":20}},
            {"key":"row--insert-before-cursor","value":{"kind":"pizza","shop":"north"}},
            {"key":"zz-after-cursor","value":{"kind":"pizza","shop":"south"}}
        ]),
    )
    .await;
    assert_eq!(query(&app).await["derived"], 7);
    finish(&app, "build").await;
    let ready = app.consensus.read_for_writer().await.unwrap();
    let expected: Vec<_> = ready
        .data
        .iter()
        .filter(|(key, _)| key.starts_with("source:[\"rows\","))
        .flat_map(|(key, value)| {
            staging::entries(
                &IndexSpec {
                    collection: "rows".into(),
                    fields: vec!["shop".into()],
                },
                key,
                value,
            )
            .unwrap()
        })
        .collect();
    for (key, value) in expected {
        assert_eq!(ready.data.get(&key), Some(&value));
    }
    assert!(ready.data["authorizationMethod"].is_null());
    let activated = administer(&app, json!({"operation":"activate","requestId":"build"}))
        .await
        .unwrap();
    assert_eq!(activated["value"]["phase"], "active");
    let after = app.consensus.read_for_writer().await.unwrap();
    assert_eq!(after.data["bundle"], input["bundle"]);
    assert_eq!(after.data["authorizationMethod"]["name"], "authorize");
    assert_eq!(query(&app).await["derived"], 70);
    assert!(!after.data.contains_key(staging::INDEXES));
    let denied = writer::submit(
        &app,
        json!({"requestId":"denied","name":"write","args":[]}),
        false,
    )
    .await
    .unwrap_err();
    assert_eq!(denied.code, "FORBIDDEN");
    let replay = administer(&app, input).await.unwrap();
    assert_eq!(replay["duplicate"], true);
    assert_eq!(replay["revision"], activated["revision"]);
    collect_all(&app, "build").await;
    assert!(
        !app.consensus
            .read_query()
            .await
            .unwrap()
            .data
            .contains_key(PLAN)
    );
    assert_eq!(query(&app).await["derived"], 70);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_cancel_preserves_shared_indexes_and_request_fence() {
    let (_directory, app) =
        super::super::tests::application(bundle(false, false, "return ctx.query(query).length;"))
            .await;
    write(&app,"seed-rows",json!([{ "key":"one","value":{"kind":"pizza","shop":"north"}},{"key":"two","value":{"kind":"pizza","shop":"south"}}])).await;
    let input = stage_input(
        "canceled-build",
        bundle(true, false, "return 20*ctx.query(query).length;"),
    );
    administer(&app, input.clone()).await.unwrap();
    administer(
        &app,
        json!({"operation":"advance","requestId":"canceled-build","maxBytes":900}),
    )
    .await
    .unwrap();
    let before = app.consensus.read_query().await.unwrap();
    // Scalar values have ordered entries only.
    assert!(
        before
            .data
            .keys()
            .any(|key| key.starts_with("ordered-entry:[\"rows\",[\"shop\"]]"))
    );
    let competing = writer::submit(
        &app,
        json!({"requestId":"competing","bundle":input["bundle"],"preparation":"blocking"}),
        true,
    )
    .await
    .unwrap_err();
    assert!(competing.message.contains("staged deployment"));
    administer(
        &app,
        json!({"operation":"cancel","requestId":"canceled-build"}),
    )
    .await
    .unwrap();
    write(
        &app,
        "after-cancel",
        json!([{ "key":"one","value":{"kind":"bread","shop":"south"}}]),
    )
    .await;
    collect_all(&app, "canceled-build").await;
    let state = app.consensus.read_query().await.unwrap();
    assert!(
        !state
            .data
            .keys()
            .any(|key| key.starts_with("index-entry:[\"rows\",[\"shop\"]]"))
    );
    assert!(
        !state
            .data
            .keys()
            .any(|key| key.starts_with("ordered-entry:[\"rows\",[\"shop\"]]"))
    );
    let expected = staging::entries(
        &IndexSpec {
            collection: "rows".into(),
            fields: vec!["kind".into()],
        },
        "source:[\"rows\",\"one\"]",
        &json!({"kind":"bread","shop":"south"}),
    )
    .unwrap();
    for (key, value) in expected {
        assert_eq!(state.data.get(&key), Some(&value));
    }
    let replay = administer(&app, input).await.unwrap();
    assert_eq!(replay["value"]["phase"], "canceled");
    assert_eq!(replay["duplicate"], true);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_graph_budget_failure_preserves_old_deployment_and_progress() {
    let (_directory, app) =
        super::super::tests::application(bundle(false, false, "return ctx.query(query).length;"))
            .await;
    let input = stage_input("bad-root", bundle(true, true, "for(;;){}"));
    administer(&app, input).await.unwrap();
    administer(&app, json!({"operation":"advance","requestId":"bad-root"}))
        .await
        .unwrap();
    let before = app.consensus.read_query().await.unwrap();
    assert!(
        administer(&app, json!({"operation":"advance","requestId":"bad-root"}))
            .await
            .is_err()
    );
    assert_eq!(app.consensus.read_query().await.unwrap(), before);
    assert_eq!(load(&before).unwrap().unwrap().phase, Phase::Rebuilding);
    write(
        &app,
        "still-old",
        json!([{ "key":"one","value":{"kind":"pizza","shop":"north"}}]),
    )
    .await;
    assert_eq!(query(&app).await["derived"], 1);
    administer(&app, json!({"operation":"cancel","requestId":"bad-root"}))
        .await
        .unwrap();
    collect_all(&app, "bad-root").await;
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_cancellation_after_retry_retirement_releases_dual_maintenance() {
    let (_directory, app) =
        super::super::tests::application(bundle(false, false, "return ctx.query(query).length;"))
            .await;
    administer(
        &app,
        stage_input(
            "retired-build",
            bundle(true, false, "return 2*ctx.query(query).length;"),
        ),
    )
    .await
    .unwrap();
    let state = app.consensus.read_for_writer().await.unwrap();
    app.consensus
        .control_retention(crate::consensus::retention::initialize(state.revision, None).unwrap())
        .await
        .unwrap();
    let canceled = administer(
        &app,
        json!({"operation":"cancel","requestId":"retired-build"}),
    )
    .await
    .unwrap();
    assert_eq!(canceled["value"]["phase"], "canceled");
    collect_all(&app, "retired-build").await;
    let state = app.consensus.read_query().await.unwrap();
    assert!(!state.data.contains_key(staging::INDEXES));
    assert!(!state.data.contains_key(PLAN));
    assert_eq!(
        crate::consensus::retention::require_scoped(&state, "retired-build")
            .unwrap_err()
            .to_string()
            .split(':')
            .next(),
        Some("REQUEST_ID_SCOPE_REQUIRED")
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_active_cleanup_removes_only_obsolete_index_definitions() {
    let (_directory, app) =
        super::super::tests::application(bundle(true, false, "return ctx.query(query).length;"))
            .await;
    write(
        &app,
        "seed-indexes",
        json!([{ "key":"one","value":{"kind":"pizza","shop":"north"}}]),
    )
    .await;
    let next = bundle(true, false, "return 3*ctx.query(query).length;")
        .replace("{kind:['kind'],shop:['shop']}", "{shop:['shop']}");
    let staged = administer(&app, stage_input("drop-index", next))
        .await
        .unwrap();
    assert_eq!(staged["value"]["phase"], "rebuilding");
    finish(&app, "drop-index").await;
    administer(
        &app,
        json!({"operation":"activate","requestId":"drop-index"}),
    )
    .await
    .unwrap();
    write(&app,"post-cutover",json!([{ "key":"one","value":{"kind":"bread","shop":"south"}},{"key":"two","value":{"kind":"pizza","shop":"north"}}])).await;
    collect_all(&app, "drop-index").await;
    let state = app.consensus.read_query().await.unwrap();
    assert!(
        !state
            .data
            .keys()
            .any(|key| key.starts_with("index-entry:[\"rows\",[\"kind\"]]")
                || key.starts_with("ordered-entry:[\"rows\",[\"kind\"]]"))
    );
    assert_eq!(query(&app).await["derived"], 3);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_activation_replay_checks_retirement_before_retained_receipt() {
    use crate::consensus::retention as protocol;
    let (_directory, app) =
        super::super::tests::application(bundle(false, false, "return ctx.query(query).length;"))
            .await;
    let snapshot = app.consensus.read_for_writer().await.unwrap();
    app.consensus
        .control_retention(protocol::initialize(snapshot.revision, None).unwrap())
        .await
        .unwrap();
    let snapshot = app.consensus.read_for_writer().await.unwrap();
    let retention = protocol::status(&snapshot).unwrap().unwrap();
    let id = protocol::scope_request_id(&retention, "activated-at-epoch-zero");
    administer(
        &app,
        stage_input(
            &id,
            bundle(false, false, "return 2*ctx.query(query).length;"),
        ),
    )
    .await
    .unwrap();
    finish(&app, &id).await;
    administer(&app, json!({"operation":"activate","requestId":id}))
        .await
        .unwrap();
    let snapshot = app.consensus.read_for_writer().await.unwrap();
    app.consensus
        .control_retention(protocol::Command {
            expected_revision: snapshot.revision,
            action: protocol::Action::Advance {
                incarnation: retention.incarnation,
                current_epoch: 1,
                min_epoch: 1,
            },
        })
        .await
        .unwrap();
    assert!(
        app.consensus
            .read_for_writer()
            .await
            .unwrap()
            .requests
            .contains_key(&id)
    );
    assert_eq!(
        administer(&app, json!({"operation":"activate","requestId":id}))
            .await
            .unwrap_err()
            .code,
        "RETRY_WINDOW_EXPIRED"
    );
    collect_all(&app, &id).await;
    assert!(
        app.consensus
            .read_for_writer()
            .await
            .unwrap()
            .requests
            .contains_key(&id)
    );
    assert_eq!(
        administer(&app, json!({"operation":"activate","requestId":id}))
            .await
            .unwrap_err()
            .code,
        "RETRY_WINDOW_EXPIRED"
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_request_identity_is_reserved_from_mutations_and_key_operations() {
    let (_directory, app) =
        super::super::tests::application(bundle(false, false, "return ctx.query(query).length;"))
            .await;
    administer(
        &app,
        stage_input(
            "reserved-build",
            bundle(true, false, "return 2*ctx.query(query).length;"),
        ),
    )
    .await
    .unwrap();
    let state = app.consensus.read_for_writer().await.unwrap();
    let ordinary = writer::submit(
        &app,
        json!({"requestId":"reserved-build","name":"write","args":[]}),
        false,
    )
    .await
    .unwrap_err();
    assert_eq!(ordinary.code, "REQUEST_ID_REUSED");
    let keys = super::super::keys::commit(
        app.clone(),
        json!({"operation":"unbind","name":"unused","requestId":"reserved-build"}),
    )
    .await
    .unwrap_err();
    assert_eq!(keys.code, "REQUEST_ID_REUSED");
    assert_eq!(app.consensus.read_for_writer().await.unwrap(), state);
    finish(&app, "reserved-build").await;
    administer(
        &app,
        json!({"operation":"activate","requestId":"reserved-build"}),
    )
    .await
    .unwrap();
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_cancel_after_retired_session_collection_uses_permanent_epoch_fence() {
    use crate::consensus::retention as protocol;
    let (_directory, app) =
        super::super::tests::application(bundle(false, false, "return ctx.query(query).length;"))
            .await;
    let snapshot = app.consensus.read_for_writer().await.unwrap();
    app.consensus
        .control_retention(protocol::initialize(snapshot.revision, None).unwrap())
        .await
        .unwrap();
    let snapshot = app.consensus.read_for_writer().await.unwrap();
    let history = protocol::status(&snapshot).unwrap().unwrap();
    let opened = app
        .consensus
        .control_retention(
            protocol::open_session(&snapshot, &authorization::owner(&Value::Null, true)).unwrap(),
        )
        .await
        .unwrap();
    let session: protocol::Session =
        serde_json::from_value(opened.result["session"].clone()).unwrap();
    let id = protocol::session_request_id(&history, &session, 1).unwrap();
    administer(
        &app,
        stage_input(
            &id,
            bundle(true, false, "return 2*ctx.query(query).length;"),
        ),
    )
    .await
    .unwrap();
    let snapshot = app.consensus.read_for_writer().await.unwrap();
    app.consensus
        .control_retention(protocol::Command {
            expected_revision: snapshot.revision,
            action: protocol::Action::Advance {
                incarnation: history.incarnation.clone(),
                current_epoch: 1,
                min_epoch: 1,
            },
        })
        .await
        .unwrap();
    let snapshot = app.consensus.read_for_writer().await.unwrap();
    app.consensus
        .control_retention(protocol::Command {
            expected_revision: snapshot.revision,
            action: protocol::Action::Collect {
                incarnation: history.incarnation,
                limit: 100,
            },
        })
        .await
        .unwrap();
    let snapshot = app.consensus.read_for_writer().await.unwrap();
    assert!(protocol::session(&snapshot, &session.id).unwrap().is_none());
    assert!(
        protocol::validate_request_owner(&snapshot, &id, &authorization::owner(&Value::Null, true))
            .unwrap_err()
            .to_string()
            .starts_with("RETRY_SESSION_FORBIDDEN")
    );
    assert_eq!(
        administer(&app, json!({"operation":"cancel","requestId":id}))
            .await
            .unwrap()["value"]["phase"],
        "canceled"
    );
    collect_all(&app, &id).await;
    let snapshot = app.consensus.read_for_writer().await.unwrap();
    assert!(!snapshot.data.contains_key(staging::INDEXES));
    assert!(!snapshot.data.contains_key(PLAN));
    assert!(
        !snapshot.requests.contains_key(&id),
        "retired cancellation needs no new retry receipt"
    );
    app.consensus.shutdown().await.unwrap();
}
