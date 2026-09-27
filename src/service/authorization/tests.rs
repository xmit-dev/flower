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
