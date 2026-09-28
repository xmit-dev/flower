use super::*;

#[test]
fn streamed_fingerprints_match_original_bytes_and_hashes() {
    fn original(input: &Value, deployment: bool, principal: &Value) -> Value {
        let mut request = input.clone();
        if let Some(object) = request.as_object_mut() {
            object.remove("credentials");
            object.remove("preparation");
        }
        let mut intent = json!({"deployment":deployment,"request":request});
        if !principal.is_null() {
            intent["owner"] = json!({"subject":principal["subject"],"tenant":principal["tenant"]});
        }
        intent
    }
    for input in [
        Value::Null,
        json!(["non-object compatibility", {"credentials":"nested stays"}]),
        json!({}),
        json!({"credentials":{"token":"ignored"},"preparation":"blocking"}),
        json!({"requestId":"a\0🌸","name":"m","expectedRevision":42,
            "args":{"2":1e21,"10":1e-7,"😀":-0.0,"\u{e000}":9007199254740993u64,
                "preparation":"nested stays","credentials":[null,false,true,"\\\"\n"]},
            "credentials":"removed","preparation":"online"}),
        json!({"bundle":{"javascript":"a🌺\n".repeat(4096),"hash":"bundle"},
            "writes":[{"collection":"c","key":"k","value":1.0}],"requestId":"deploy"}),
    ] {
        for principal in [
            Value::Null,
            json!({}),
            json!({"subject":"alice"}),
            json!({"subject":"a\0😀","tenant":"\u{e000}","claims":{"role":"admin"}}),
        ] {
            for deployment in [false, true] {
                let expected =
                    serde_json::to_vec(&original(&input, deployment, &principal)).unwrap();
                let borrowed = Intent {
                    deployment,
                    owner: (!principal.is_null()).then(|| IntentOwner {
                        subject: &principal["subject"],
                        tenant: &principal["tenant"],
                    }),
                    request: BusinessInput(&input),
                };
                assert_eq!(serde_json::to_vec(&borrowed).unwrap(), expected);
                // The first 128 bits of the SHA-256, in unpadded base64url.
                use base64::Engine as _;
                let digest = sha2::Sha256::digest(&expected);
                let short = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&digest[..16]);
                assert_eq!(fingerprint(&input, deployment, &principal), short);
                assert_eq!(short.len(), 22);
            }
        }
    }
}

fn bundle() -> String {
    r#"
    const records={kind:'collection',name:'records'};
    var __flowerBundle={default:{definitions:{
      stable:{name:'stable',kind:'derived',compute:()=>0},
      authorize:{name:'authorize',kind:'queryMethod',compute:(ctx,req)=>{
        const c=req.credentials;
        if(!c || c.expires<=ctx.now() || ctx.get(records,'revoked')) return null;
        return {subject:c.subject,claims:{role:c.role}};
      }},
      update:{name:'update',kind:'mutationMethod',compute:(ctx,args)=>{
        const n=(ctx.get(records,'count')||0)+1;
        ctx.set(records,'count',n);
        if(args.revoke) ctx.set(records,'revoked',true);
        return {count:n,principal:ctx.principal()};
      }},
      read:{name:'read',kind:'queryMethod',compute:ctx=>ctx.principal()}
    },authorize:{name:'authorize'},http:{
      update:{name:'update',kind:'mutation'},read:{name:'read',kind:'query'}
    }}};
    "#
    .into()
}

fn credentials(subject: &str, role: &str) -> Value {
    json!({"subject":subject,"role":role,"expires":9_007_199_254_000_000_u64})
}

#[tokio::test]
async fn retry_authorizes_current_credentials_and_preserves_original_result() {
    let (_directory, app) = super::super::tests::application(bundle()).await;
    let input = json!({"name":"update","args":{},"requestId":"same-intent",
        "credentials":credentials("alice","old")});
    let original = writer::submit(&app, input.clone(), false).await.unwrap();
    assert_eq!(original["value"]["count"], 1);
    let mut retry = input.clone();
    retry["credentials"] = credentials("alice", "new");
    let duplicate = writer::submit(&app, retry.clone(), false).await.unwrap();
    assert_eq!(duplicate["duplicate"], true);
    assert_eq!(duplicate["value"], original["value"]);
    retry["credentials"]["expires"] = json!(0);
    assert_eq!(
        writer::submit(&app, retry.clone(), false)
            .await
            .unwrap_err()
            .code,
        "FORBIDDEN"
    );
    retry["credentials"] = credentials("bob", "new");
    assert_eq!(
        writer::submit(&app, retry, false).await.unwrap_err().code,
        "REQUEST_ID_REUSED"
    );
    let mut revoke = input.clone();
    revoke["requestId"] = json!("revoke");
    revoke["args"] = json!({"revoke":true});
    writer::submit(&app, revoke, false).await.unwrap();
    assert_eq!(
        writer::submit(&app, input, false).await.unwrap_err().code,
        "FORBIDDEN"
    );
    let state = app.consensus.read().await.unwrap();
    assert_eq!(
        state.requests.get("same-intent").unwrap().result,
        original["value"]
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn cached_queries_still_authorize_and_scope_principals() {
    let (_directory, app) = super::super::tests::application(bundle()).await;
    let mut input = json!({"name":"read","credentials":credentials("alice","one")});
    let first = read_query(&app, input.clone()).await.unwrap();
    assert_eq!(first.value["subject"], "alice");
    // Access has its own validity; the result itself holds at its revision.
    assert_eq!(first.validity, Validity::Stable);
    input["credentials"] = credentials("bob", "two");
    assert_eq!(
        read_query(&app, input.clone()).await.unwrap().value["subject"],
        "bob"
    );
    input["credentials"]["expires"] = json!(0);
    assert!(matches!(
        read_query(&app, input).await,
        Err(ApiError {
            code: "FORBIDDEN",
            ..
        })
    ));
    app.consensus.shutdown().await.unwrap();
}

#[test]
fn credentials_and_mutable_claims_do_not_change_owned_intent() {
    let one = json!({"name":"method","args":{"x":1},"requestId":"id","credentials":"old"});
    let mut two = one.clone();
    two["credentials"] = json!("new");
    let owner = json!({"subject":"alice","tenant":"store","claims":{"v":1}});
    let mut changed = owner.clone();
    changed["claims"] = json!({"v":2});
    assert_eq!(
        fingerprint(&one, false, &owner),
        fingerprint(&two, false, &changed)
    );
    changed["subject"] = json!("bob");
    assert_ne!(
        fingerprint(&one, false, &owner),
        fingerprint(&two, false, &changed)
    );
    assert!(
        evaluator::validate_invocation(&json!({"name":"read","$principal":owner}), "query")
            .is_err()
    );
}

/// A hook that reports decisions: `guarded` reads the argument `owner`
/// (the caller's), `opaque` all of them, every other method none.
fn decision_bundle(claims: &str) -> String {
    r#"
    const records={kind:'collection',name:'records'};
    var __flowerBundle={default:{definitions:{
      stable:{name:'stable',kind:'derived',compute:()=>0},
      authorize:{name:'authorize',kind:'queryMethod',compute:(ctx,req)=>{
        const c=req.credentials, now=ctx.clock();
        if(!c || c.expires<=now || ctx.get(records,'revoked')) return {principal:null,readArgs:false};
        ctx.changesAt(c.expires);
        if(req.method==='guarded') return {principal:req.args.owner===c.subject?{subject:c.subject}:null,readArgs:['owner']};
        if(req.method==='opaque') return {principal:{subject:c.subject},readArgs:true};
        if(req.method==='malformed') return {subject:c.subject};
        return {principal:{subject:c.subject,claims:CLAIMS},readArgs:false};
      }},
      update:{name:'update',kind:'mutationMethod',compute:(ctx,args)=>{
        const n=(ctx.get(records,'count')||0)+1;
        ctx.set(records,'count',n);
        if(args.revoke) ctx.set(records,'revoked',true);
        return {count:n,principal:ctx.principal()};
      }},
      read:{name:'read',kind:'queryMethod',compute:(ctx,args)=>({principal:ctx.principal(),args})},
      guarded:{name:'guarded',kind:'queryMethod',compute:ctx=>ctx.principal()},
      opaque:{name:'opaque',kind:'queryMethod',compute:ctx=>ctx.principal()},
      malformed:{name:'malformed',kind:'queryMethod',compute:ctx=>ctx.principal()}
    },authorize:{name:'authorize',result:'decision'},http:{
      update:{name:'update',kind:'mutation'},read:{name:'read',kind:'query'},
      guarded:{name:'guarded',kind:'query'},opaque:{name:'opaque',kind:'query'},
      malformed:{name:'malformed',kind:'query'}
    }}};
    "#
    .replace("CLAIMS", claims)
}

fn counts(app: &App) -> (u64, u64) {
    let metrics = app.authorizations.metrics();
    (
        metrics["evaluated"].as_u64().unwrap(),
        metrics["reused"].as_u64().unwrap(),
    )
}

async fn now(app: &App) -> u64 {
    app.clock
        .sample(&app.consensus.read_query().await.unwrap())
        .unwrap()
}

#[tokio::test]
async fn decisions_that_ignore_arguments_are_reused_across_arguments_and_kinds() {
    let (_directory, app) = super::super::tests::application(decision_bundle("{role:'one'}")).await;
    let credentials = json!({"subject":"alice","expires":now(&app).await + 60_000});
    for x in 0..3 {
        let read = read_query(
            &app,
            json!({"name":"read","args":{"x":x},"credentials":credentials}),
        )
        .await
        .unwrap();
        assert_eq!(read.value["principal"]["subject"], "alice");
        assert_eq!(read.value["args"]["x"], x);
    }
    assert_eq!(counts(&app), (1, 2), "one evaluation for three reads");
    // Another method is another decision; then it is reused too.
    for x in 0..2 {
        let update = writer::submit(
            &app,
            json!({"name":"update","args":{"x":x},"requestId":format!("u{x}"),"credentials":credentials}),
            false,
        )
        .await
        .unwrap();
        assert_eq!(update["value"]["principal"]["claims"]["role"], "one");
    }
    assert_eq!(counts(&app).0, 2);
    // Other credentials, another decision.
    let bob = json!({"subject":"bob","expires":now(&app).await + 60_000});
    let read = read_query(&app, json!({"name":"read","args":{},"credentials":bob}))
        .await
        .unwrap();
    assert_eq!(read.value["principal"]["subject"], "bob");
    assert_eq!(counts(&app).0, 3);
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn decisions_that_read_arguments_hold_only_for_arguments_that_agree_on_them() {
    let (_directory, app) = super::super::tests::application(decision_bundle("{}")).await;
    let credentials = json!({"subject":"alice","expires":now(&app).await + 60_000});
    let call =
        |method: &str, args: Value| json!({"name":method,"args":args,"credentials":credentials});
    let guarded = |args: Value| call("guarded", args);
    assert_eq!(
        read_query(&app, guarded(json!({"owner":"alice"})))
            .await
            .unwrap()
            .value["subject"],
        "alice"
    );
    // The same owner, whatever else: the decision read only the owner.
    for args in [json!({"owner":"alice"}), json!({"owner":"alice","page":2})] {
        assert_eq!(
            read_query(&app, guarded(args)).await.unwrap().value["subject"],
            "alice"
        );
    }
    assert_eq!(counts(&app), (1, 2));
    for args in [
        json!({"owner":"bob"}),
        json!({"owner":null}),
        json!({}),
        json!(["alice"]),
        json!("alice"),
    ] {
        assert_eq!(
            read_query(&app, guarded(args.clone()))
                .await
                .err()
                .unwrap()
                .code,
            "FORBIDDEN",
            "an allowed call never lets other arguments through: {args}"
        );
    }
    assert_eq!(counts(&app), (6, 2));
    // A decision that read more than fields by name is never reused.
    for _ in 0..2 {
        read_query(&app, call("opaque", json!({"x":1})))
            .await
            .unwrap();
    }
    assert_eq!(counts(&app), (8, 2));
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn reused_decisions_end_with_what_they_read_their_time_and_their_code() {
    let (_directory, app) = super::super::tests::application(decision_bundle("{role:'one'}")).await;
    // A write to a record the hook read.
    let credentials = json!({"subject":"alice","expires":now(&app).await + 60_000});
    let read = json!({"name":"read","args":{},"credentials":credentials});
    read_query(&app, read.clone()).await.unwrap();
    read_query(&app, read.clone()).await.unwrap();
    assert_eq!(counts(&app), (1, 1));
    writer::submit(
        &app,
        json!({"name":"update","args":{},"requestId":"count","credentials":credentials}),
        false,
    )
    .await
    .unwrap();
    // Writes elsewhere leave it alone.
    read_query(&app, read.clone()).await.unwrap();
    assert_eq!(counts(&app), (2, 2), "update's own decision, then reuse");
    // A redeployment: the hook's code is among what every decision read.
    writer::submit(
        &app,
        json!({"requestId":"redeploy","bundle":{
            "hash":evaluator::hash(decision_bundle("{role:'two'}").as_bytes()),
            "javascript":decision_bundle("{role:'two'}")}}),
        true,
    )
    .await
    .unwrap();
    let redeployed = read_query(&app, read.clone()).await.unwrap();
    assert_eq!(redeployed.value["principal"]["claims"]["role"], "two");
    assert_eq!(counts(&app).0, 3);
    writer::submit(
        &app,
        json!({"name":"update","args":{"revoke":true},"requestId":"revoke","credentials":credentials}),
        false,
    )
    .await
    .unwrap();
    assert_eq!(
        read_query(&app, read).await.err().unwrap().code,
        "FORBIDDEN",
        "the hook read the revocation record"
    );
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn reused_decisions_expire_with_the_time_they_declared() {
    let (_directory, app) = super::super::tests::application(decision_bundle("{}")).await;
    let credentials = json!({"subject":"alice","expires":now(&app).await + 400});
    let read = json!({"name":"read","args":{},"credentials":credentials});
    read_query(&app, read.clone()).await.unwrap();
    read_query(&app, read.clone()).await.unwrap();
    assert_eq!(counts(&app), (1, 1));
    tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    assert_eq!(
        read_query(&app, read).await.err().unwrap().code,
        "FORBIDDEN"
    );
    assert_eq!(counts(&app), (2, 1));
    app.consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn hooks_without_decisions_run_for_every_call_and_malformed_decisions_deny() {
    let (_directory, app) = super::super::tests::application(bundle()).await;
    let read = json!({"name":"read","credentials":credentials("alice","one")});
    read_query(&app, read.clone()).await.unwrap();
    read_query(&app, read).await.unwrap();
    assert_eq!(counts(&app), (2, 0));
    app.consensus.shutdown().await.unwrap();

    let (_directory, app) = super::super::tests::application(decision_bundle("{}")).await;
    let credentials = json!({"subject":"alice","expires":now(&app).await + 60_000});
    let error = read_query(&app, json!({"name":"malformed","credentials":credentials}))
        .await
        .err()
        .unwrap();
    assert_eq!(
        (error.code, error.message.as_str()),
        ("FORBIDDEN", "Authorization returned an invalid decision")
    );
    app.consensus.shutdown().await.unwrap();
}
