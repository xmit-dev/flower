use super::*;

fn public_bundle() -> String {
    r#"
      const records={kind:'collection',name:'records'};
      var __flowerBundle={default:{definitions:{
        stable:{name:'stable',kind:'derived',compute:ctx=>ctx.get(records,'value')},
        add:{name:'add',kind:'mutationMethod',compute:(ctx,args)=>{
          const value=ctx.get(records,'value')+args;
          ctx.set(records,'value',value); return value;
        }},
        multiply:{name:'multiply',kind:'mutationMethod',compute:(ctx,args)=>{
          const value=ctx.get(records,'value')*args;
          ctx.set(records,'value',value); return value;
        }},
        read:{name:'read',kind:'queryMethod',compute:ctx=>ctx.get(records,'value')},
        plan:{name:'plan',kind:'transactionMethod',compute:(_ctx,args)=>args},
        deny:{name:'deny',kind:'queryMethod',compute:()=>null}
      },http:{change:{name:'add',kind:'mutation'}}}};
    "#
    .into()
}

async fn decode(response: Response) -> Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn metadata(app: &App, puts: BTreeMap<String, Value>) {
    let state = app.consensus.read().await.unwrap();
    app.consensus
        .commit(Commit {
            internal: true,
            request_id: format!("test-metadata-{}", state.revision),
            fingerprint: "test-metadata".into(),
            expected_revision: state.revision,
            puts,
            deletes: vec![],
            result: Value::Null,
        })
        .await
        .unwrap();
}

async fn change_while_queued(
    app: &Arc<App>,
    input: Value,
    puts: BTreeMap<String, Value>,
) -> Result<Response, ApiError> {
    let writer = app.writer.lock().await;
    let mut pending = Box::pin(call(State(app.clone()), Json(input)));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut pending)
            .await
            .is_err()
    );
    metadata(app, puts).await;
    drop(writer);
    tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .expect("changed aliases must finish or reject without a dispatch loop")
}

#[tokio::test]
async fn mutation_dispatch_queues_without_a_separate_preparation_lease() {
    let (_directory, app) = application_with_admission(
        public_bundle(),
        Some(admission::Pool::new([1, 1], [1, 1], [1 << 20, 1 << 20], 1)),
    )
    .await;
    let admitted = admission::acquire(&app, admission::Class::User, &Value::Null)
        .await
        .unwrap();
    let writer = app.writer.lock().await;
    let mut pending = Box::pin(call(
        State(app.clone()),
        Json(json!({"name":"change","requestId":"queued","args":2})),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut pending)
            .await
            .is_err()
    );
    assert_eq!(
        app.admission.metrics()["classes"][0]["queued"],
        0,
        "Public mutation dispatch must enqueue at the writer before admission"
    );
    assert_eq!(app.query_evaluations.available_permits(), 4);
    drop(writer);
    drop(admitted);
    let response = tokio::time::timeout(Duration::from_secs(5), pending)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(decode(response).await["value"], 8);
    assert_eq!(
        app.admission.metrics()["classes"][0]["retainedInputBytes"],
        0
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn mutation_dispatch_resolves_changed_targets_and_query_kinds_before_execution() {
    let (_directory, app) = application(public_bundle()).await;
    let response = change_while_queued(
        &app,
        json!({"name":"change","requestId":"new-target","args":3}),
        BTreeMap::from([(
            "httpMethods".into(),
            json!({"change":{"name":"multiply","kind":"mutation"}}),
        )]),
    )
    .await
    .unwrap();
    assert_eq!(decode(response).await["value"], 18);
    let response = change_while_queued(
        &app,
        json!({"name":"change","requestId":"now-a-query","args":100}),
        BTreeMap::from([(
            "httpMethods".into(),
            json!({"change":{"name":"read","kind":"query"}}),
        )]),
    )
    .await
    .unwrap();
    assert_eq!(decode(response).await["value"], 18);
    let state = app.consensus.read().await.unwrap();
    assert!(!state.requests.contains_key("now-a-query"));
    assert_eq!(state.data["source:[\"records\",\"value\"]"], 18);

    // A current query needs no mutation ID, even if callers use generic RPC.
    assert_eq!(
        decode(
            call(State(app.clone()), Json(json!({"name":"change"})))
                .await
                .unwrap()
        )
        .await["value"],
        18
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn mutation_dispatch_can_reclassify_an_alias_as_a_transaction() {
    const CHILD: &str = "FLOWER_TEST_MUTATION_DISPATCH_TRANSACTION_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = tokio::task::spawn_blocking(|| {
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "service::tests::mutation_dispatch::mutation_dispatch_can_reclassify_an_alias_as_a_transaction",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .env("FLOWER_GROUP", "local")
                .env("FLOWER_GROUPS", r#"{"local":["127.0.0.1:7101"]}"#)
                .output()
                .unwrap()
        })
        .await
        .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    let (_directory, app) = application(public_bundle()).await;
    let response = change_while_queued(
        &app,
        json!({"name":"change","requestId":"now-a-transaction","args":{"calls":[],"value":"planned"}}),
        BTreeMap::from([("httpMethods".into(), json!({"change":{"name":"plan","kind":"transaction"}}))]),
    )
    .await
    .unwrap();
    assert_eq!(
        decode(response).await["value"],
        json!({"results":[],"value":"planned"})
    );
    let state = app.consensus.read().await.unwrap();
    assert!(state.requests.contains_key("now-a-transaction"));
    assert_eq!(state.data["source:[\"records\",\"value\"]"], 6);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn mutation_dispatch_rechecks_revocation_and_authorization_before_receipt_replay() {
    let (_directory, app) = application(public_bundle()).await;
    let input = json!({"name":"change","requestId":"retry","args":2});
    assert_eq!(
        decode(call(State(app.clone()), Json(input.clone())).await.unwrap()).await["value"],
        8
    );
    let error = change_while_queued(
        &app,
        input.clone(),
        BTreeMap::from([("authorizationMethod".into(), json!({"name":"deny"}))]),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "FORBIDDEN");
    metadata(
        &app,
        BTreeMap::from([("authorizationMethod".into(), Value::Null)]),
    )
    .await;
    let error = change_while_queued(
        &app,
        input,
        BTreeMap::from([("httpMethods".into(), json!({}))]),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code, "METHOD_NOT_FOUND");
    assert_eq!(
        app.consensus.read().await.unwrap().data["source:[\"records\",\"value\"]"],
        8
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn mutation_dispatch_retains_cas_retry_and_input_validation_contracts() {
    let (_directory, app) = application(public_bundle()).await;
    let missing = call(State(app.clone()), Json(json!({"name":"change","args":2})))
        .await
        .unwrap_err();
    assert_eq!(missing.code, "INPUT_INVALID");
    let input = json!({"name":"change","requestId":"once","expectedRevision":1,"args":2});
    let first = decode(call(State(app.clone()), Json(input.clone())).await.unwrap()).await;
    let duplicate = decode(call(State(app.clone()), Json(input.clone())).await.unwrap()).await;
    assert_eq!(duplicate["revision"], first["revision"]);
    assert_eq!(duplicate["duplicate"], true);
    assert_eq!(duplicate["value"], 8);
    let mut changed = input.clone();
    changed["args"] = json!(3);
    assert_eq!(
        call(State(app.clone()), Json(changed))
            .await
            .unwrap_err()
            .code,
        "REQUEST_ID_REUSED"
    );
    let mut stale = input;
    stale["requestId"] = json!("stale");
    assert_eq!(
        call(State(app.clone()), Json(stale))
            .await
            .unwrap_err()
            .code,
        "REVISION_CONFLICT"
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn mutation_dispatch_only_retries_exact_kind_rejections_and_preserves_forwarded_bodies() {
    let (_directory, app) = application(public_bundle()).await;
    let mismatch = ApiError::new(
        StatusCode::UNPROCESSABLE_ENTITY,
        "METHOD_KIND_MISMATCH",
        "changed".into(),
    );
    assert!(
        mutation_hint_response(&app, Err(mismatch.clone()))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        mutation_hint_response(&app, Ok(mismatch.into_response()))
            .await
            .unwrap()
            .is_none()
    );
    for (status, body) in [
        (
            StatusCode::UNPROCESSABLE_ENTITY,
            "{ \"error\": {\"code\":\"EVALUATION_FAILED\",\"message\":\"METHOD_KIND_MISMATCH\"} }\n",
        ),
        (StatusCode::UNPROCESSABLE_ENTITY, "not JSON"),
        (
            StatusCode::CONFLICT,
            "{\"error\":{\"code\":\"METHOD_KIND_MISMATCH\"}}",
        ),
        (
            StatusCode::OK,
            "{\"value\":{\"error\":{\"code\":\"METHOD_KIND_MISMATCH\"}}}",
        ),
    ] {
        let response = Response::builder()
            .status(status)
            .header("x-test", "preserved")
            .body(axum::body::Body::from(body))
            .unwrap();
        let response = mutation_hint_response(&app, Ok(response))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(response.status(), status);
        assert_eq!(response.headers()["x-test"], "preserved");
        assert_eq!(
            axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap(),
            body
        );
    }
    app.consensus.shutdown().await.unwrap();
}
