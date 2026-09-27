use super::*;

fn public_bundle() -> String {
    r#"
      const records={kind:'collection',name:'records'};
      var __flowerBundle={default:{definitions:{
        stable:{name:'stable',kind:'derived',compute:ctx=>ctx.get(records,'value')},
        read:{name:'read',kind:'queryMethod',consistency:'replica-local',compute:(ctx,args)=>({value:ctx.get(records,'value'),args})},
        fresh:{name:'fresh',kind:'queryMethod',compute:(ctx,args)=>({value:ctx.get(records,'value'),args})}
      },http:{
        read:{name:'read',kind:'query',consistency:'replica-local'},
        fresh:{name:'fresh',kind:'query'}
      }}};
    "#
    .into()
}

async fn decode(response: Response) -> Value {
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

async fn metadata(app: &App, puts: BTreeMap<String, Value>, deletes: Vec<String>) {
    let state = app.consensus.read().await.unwrap();
    app.consensus
        .commit(Commit {
            internal: true,
            request_id: format!("test-metadata-{}", state.revision),
            fingerprint: "test-metadata".into(),
            expected_revision: state.revision,
            puts,
            deletes,
            result: Value::Null,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn hot_http_cache_hits_do_not_queue_behind_writers_and_misses_recapture() {
    let (_directory, app) = application_with_admission(
        public_bundle(),
        Some(admission::Pool::new([1, 1], [1, 1], [1 << 20, 1 << 20], 1)),
    )
    .await;
    let input = json!({"name":"read","args":null});
    read_query(&app, input.clone()).await.unwrap();
    let writer = admission::acquire(&app, admission::Class::User, &Value::Null)
        .await
        .unwrap();
    let hit = tokio::time::timeout(
        Duration::from_secs(1),
        query_http(State(app.clone()), Json(input)),
    )
    .await
    .expect("A cached public read must not wait for the writer's worker")
    .unwrap();
    assert_eq!(decode(hit).await["value"]["value"], 6);
    let mut miss = Box::pin(query_http(
        State(app.clone()),
        Json(json!({"name":"read","args":"new cache key"})),
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut miss)
            .await
            .is_err()
    );
    assert_eq!(app.admission.metrics()["classes"][0]["queued"], 1);
    assert_eq!(app.query_evaluations.available_permits(), 4);
    metadata(
        &app,
        BTreeMap::from([("source:[\"records\",\"value\"]".into(), json!(19))]),
        vec![],
    )
    .await;
    drop(writer);
    let value = decode(miss.await.unwrap()).await;
    assert_eq!(value["revision"], 2);
    assert_eq!(
        value["value"]["value"], 19,
        "Queued misses must recapture the current root"
    );
    assert_eq!(
        app.admission.metrics()["classes"][0]["retainedInputBytes"],
        0
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn cached_http_responses_keep_output_accounted_after_replacement_and_transport_poll() {
    use futures_util::StreamExt;

    let (_directory, app) = application_with_admission(
        public_bundle(),
        Some(admission::Pool::new([1, 1], [1, 1], [1 << 20, 1 << 20], 1)),
    )
    .await;
    let input = json!({"name":"read","args":"x".repeat(8192)});
    read_query(&app, input.clone()).await.unwrap();
    let response = query_http(State(app.clone()), Json(input.clone()))
        .await
        .unwrap();
    let key = serde_json::to_string(&json!({"invocation":input,"principal":null})).unwrap();
    app.query_cache
        .insert(2, key, Value::Null, Default::default());
    assert_eq!(app.admission.metrics()["classes"][0]["active"], 0);
    assert_eq!(app.query_evaluations.available_permits(), 4);
    assert!(
        app.admission.metrics()["classes"][0]["retainedInputBytes"]
            .as_u64()
            .unwrap()
            > 8192
    );
    let mut stream = response.into_body().into_data_stream();
    let bytes = stream.next().await.unwrap().unwrap();
    drop(stream);
    let held = app.admission.metrics()["classes"][0]["retainedInputBytes"]
        .as_u64()
        .unwrap();
    assert!(
        held > 8192,
        "Transport-held bytes retain their own reservation"
    );
    let clone = bytes.clone();
    drop(bytes);
    assert_eq!(
        app.admission.metrics()["classes"][0]["retainedInputBytes"],
        held
    );
    drop(clone);
    assert_eq!(
        app.admission.metrics()["classes"][0]["retainedInputBytes"],
        0
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_http_cache_still_checks_registry_transactions_and_current_policy() {
    let (_directory, app) = application(public_bundle()).await;
    let input = json!({"name":"read","args":null});
    read_query(&app, input.clone()).await.unwrap();
    let original = app.consensus.read().await.unwrap().data["httpMethods"].clone();
    metadata(
        &app,
        BTreeMap::from([("httpMethods".into(), json!({}))]),
        vec![],
    )
    .await;
    assert_eq!(
        query_http(State(app.clone()), Json(input.clone()))
            .await
            .unwrap_err()
            .code,
        "METHOD_NOT_FOUND"
    );
    metadata(
        &app,
        BTreeMap::from([
            ("httpMethods".into(), original),
            ("transaction:participant".into(), Value::Null),
        ]),
        vec![],
    )
    .await;
    assert_eq!(
        query_http(State(app.clone()), Json(input.clone()))
            .await
            .unwrap_err()
            .code,
        "TRANSACTION_PREPARED"
    );
    metadata(
        &app,
        BTreeMap::from([("keyDeclarations".into(), json!([{"name":"key"}]))]),
        vec!["transaction:participant".into()],
    )
    .await;
    assert!(
        cached_query_response(&app, &input, Some(MethodKind::Query))
            .await
            .unwrap()
            .is_none()
    );
    metadata(
        &app,
        BTreeMap::from([(
            "authorizationMethod".into(),
            json!({"name":"missing-authorizer"}),
        )]),
        vec!["keyDeclarations".into()],
    )
    .await;
    assert!(
        cached_query_response(&app, &input, Some(MethodKind::Query))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        query_http(State(app.clone()), Json(input))
            .await
            .unwrap_err()
            .code,
        "FORBIDDEN"
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn public_http_cache_preserves_fresh_and_replica_local_read_fences() {
    let (_directory, app) = application(public_bundle()).await;
    for name in ["read", "fresh"] {
        query_http(State(app.clone()), Json(json!({"name":name})))
            .await
            .unwrap();
    }
    let invalid = query_http(
        State(app.clone()),
        Json(json!({"name":"read","expectedRevision":1})),
    )
    .await;
    assert_eq!(invalid.unwrap_err().code, "INPUT_INVALID");
    app.consensus.shutdown().await.unwrap();
    assert_eq!(
        query_http(State(app.clone()), Json(json!({"name":"fresh"})))
            .await
            .unwrap_err()
            .status,
        StatusCode::SERVICE_UNAVAILABLE
    );
    let local = query_http(State(app), Json(json!({"name":"read"})))
        .await
        .unwrap();
    assert_eq!(decode(local).await["value"]["value"], 6);
}

#[tokio::test]
async fn saturated_cache_lookup_falls_back_to_fair_admission_and_cancels_cleanly() {
    let (_directory, app) = application_with_admission(
        public_bundle(),
        Some(admission::Pool::new([1, 1], [1, 1], [1 << 20, 1 << 20], 1)),
    )
    .await;
    let input = json!({"name":"read"});
    read_query(&app, input.clone()).await.unwrap();
    let writer = admission::acquire(&app, admission::Class::User, &Value::Null)
        .await
        .unwrap();
    let original_bytes = app.admission.metrics()["classes"][0]["retainedInputBytes"].clone();
    let lookups = app
        .query_evaluations
        .clone()
        .acquire_many_owned(4)
        .await
        .unwrap();
    let mut queued = Box::pin(query_http(State(app.clone()), Json(input.clone())));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut queued)
            .await
            .is_err()
    );
    assert_eq!(app.admission.metrics()["classes"][0]["queued"], 1);
    drop(queued);
    assert_eq!(app.admission.metrics()["classes"][0]["queued"], 0);
    assert_eq!(
        app.admission.metrics()["classes"][0]["retainedInputBytes"],
        original_bytes
    );
    drop(lookups);
    let hit = tokio::time::timeout(
        Duration::from_secs(1),
        query_http(State(app.clone()), Json(input)),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(decode(hit).await["value"]["value"], 6);
    drop(writer);
    assert_eq!(
        app.admission.metrics()["classes"][0]["retainedInputBytes"],
        0
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn cached_http_output_must_fit_the_node_byte_budget() {
    let (_directory, app) = application_with_admission(
        public_bundle(),
        Some(admission::Pool::new([1, 1], [1, 1], [8192, 8192], 1)),
    )
    .await;
    metadata(
        &app,
        BTreeMap::from([(
            "source:[\"records\",\"value\"]".into(),
            json!("x".repeat(9000)),
        )]),
        vec![],
    )
    .await;
    let input = json!({"name":"read"});
    read_query(&app, input.clone()).await.unwrap();
    let error = query_http(State(app.clone()), Json(input))
        .await
        .unwrap_err();
    assert_eq!(error.code, "ADMISSION_OVERLOADED");
    assert_eq!(
        app.admission.metrics()["classes"][0]["retainedInputBytes"],
        0
    );
    assert_eq!(app.admission.metrics()["classes"][0]["active"], 0);
    assert_eq!(app.query_evaluations.available_permits(), 4);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn optional_cache_scratch_budget_does_not_reject_an_admissible_query() {
    let (_directory, app) = application_with_admission(
        public_bundle(),
        Some(admission::Pool::new([1, 1], [1, 1], [8192, 8192], 1)),
    )
    .await;
    let input = json!({"name":"read", "args":"x".repeat(1500)});
    assert!(admission::input_bytes(&input) < 8192);
    assert!(admission::input_bytes(&input).saturating_mul(7) > 8192);
    let result = query_http(State(app.clone()), Json(input.clone()))
        .await
        .unwrap();
    assert_eq!(decode(result).await["value"]["args"], input["args"]);
    assert_eq!(
        app.admission.metrics()["classes"][0]["retainedInputBytes"],
        0
    );
    app.consensus.shutdown().await.unwrap();
}
