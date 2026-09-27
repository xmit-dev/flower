use super::tests::{bundle, limits, run};
use super::*;

#[test]
fn managed_keys_cannot_resolve_during_initialization_or_from_application_code() {
    let call = "__flowerCrypto(200,0,JSON.stringify({operation:'key.publicKey',key:{kind:'key',name:'x',algorithm:'Ed25519',usages:['publicKey']}}),'','','')";
    for marker in ["", STATIC_INIT_MARKER] {
        let code = format!(
            "{marker}const forbidden={call};{}",
            bundle("()=>null", false)
        );
        let mut calls = 0;
        let outcome = execute(
            &code,
            "test",
            &Value::Null,
            "query",
            &mut |_, _| {
                calls += 1;
                Ok(Value::Null)
            },
            limits(),
        );
        assert!(outcome.is_err(), "managed key escaped initialization guard");
        assert_eq!(calls, 0);
    }
    // Application code has no generic host capability through which to name
    // a native-only operation; the runner's numbered operations exclude it.
    let code = bundle("()=>[typeof __flowerRead,typeof __flowerHost]", true);
    let mut calls = 0;
    let result = execute(
        &code,
        "test",
        &Value::Null,
        "query",
        &mut |_, _| {
            calls += 1;
            Ok(Value::Null)
        },
        limits(),
    )
    .unwrap();
    assert_eq!(result["value"], json!(["undefined", "undefined"]));
    assert_eq!(calls, 0);
}

#[test]
fn managed_request_shape_and_foreign_shared_handles_fail_before_resolution() {
    for request in [
        json!({"operation":"key.publicKey","key":{},"unexpected":true}),
        json!({"operation":"key.publicKey","key":{},"options":[]}),
        json!({"operation":"nacl.secretbox","key":{"kind":"sharedKey","token":"prior-invocation"}}),
        json!({"operation":"jwt.sign","key":{"kind":"sharedKey","token":"prior-invocation"}}),
    ] {
        let code = bundle(
            "(_ctx,args)=>__flowerCrypto(200,0,JSON.stringify(args),'','','')",
            true,
        );
        let mut calls = 0;
        let result = execute(
            &code,
            "test",
            &request,
            "query",
            &mut |_, _| {
                calls += 1;
                Ok(Value::Null)
            },
            limits(),
        )
        .unwrap();
        assert_eq!(result["ok"], false, "{result}");
        assert_eq!(
            calls, 0,
            "rejected malformed input must not resolve a capability"
        );
    }
}

#[test]
fn opaque_shared_bridge_rejects_numeric_copied_and_proxy_handles_without_resolution() {
    for source in [
        "()=>__flowerCrypto(201,0,1,new Uint8Array(),new Uint8Array())",
        "()=>__flowerCrypto(202,0,{kind:'sharedKey',token:'1'},new Uint8Array(),new Uint8Array())",
        "()=>__flowerCrypto(201,1,{},new Uint8Array(),new Uint8Array())",
        "()=>__flowerCrypto(201,0,new Proxy({kind:'sharedKey'},{}),new Uint8Array(),new Uint8Array())",
    ] {
        let mut calls = 0;
        let result = execute(
            &bundle(source, true),
            "test",
            &Value::Null,
            "query",
            &mut |_, _| {
                calls += 1;
                Ok(Value::Null)
            },
            limits(),
        )
        .unwrap();
        assert_eq!(result["ok"], false, "{result}");
        assert_eq!(
            calls, 0,
            "Forged handle must fail before invoking a host callback"
        );
    }
}

#[test]
fn managed_jwt_routing_extracts_only_a_scoped_selector() {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"EdDSA","kid":"flower.key-a.3"}"#);
    let args = json!({"operation":"jwt.verify","key":{"kind":"key","name":"sessions","algorithm":"Ed25519","usages":["verify"]}});
    let code = bundle(
        &format!("(_ctx,args)=>__flowerCrypto(200,0,JSON.stringify(args),'{header}.e30.AA','','')"),
        true,
    );
    let mut calls = Vec::new();
    let result = execute(
        &code,
        "test",
        &args,
        "query",
        &mut |operation, arguments| {
            calls.push((operation.to_owned(), arguments));
            anyhow::bail!("KEY_FORBIDDEN: test policy rejection")
        },
        limits(),
    )
    .unwrap();
    assert_eq!(result["ok"], false);
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "managedKey");
    assert_eq!(calls[0].1[0]["kid"], "flower.key-a.3");
    assert_eq!(calls[0].1[0]["key"], args["key"]);
}

#[test]
fn managed_jwt_validation_declares_expiry_instead_of_polling() {
    // The mounted wrapping key is read once per process.
    const CHILD: &str = "FLOWER_TEST_MANAGED_JWT_CLOCK_CHILD";
    if std::env::var_os(CHILD).is_none() {
        use std::io::Write;
        let mut wrapping = tempfile::NamedTempFile::new().unwrap();
        wrapping.write_all(&[31; 32]).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "evaluator::wasm::crypto_tests::managed_jwt_validation_declares_expiry_instead_of_polling",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .env("FLOWER_KEYRING_FILE", wrapping.path())
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    use crate::crypto::managed;
    let code = bundle(
        r#"(_ctx,{key,seal,open})=>{
        const call=(operation,...inputs)=>__flowerCrypto(200,0,JSON.stringify({operation,key}),...inputs);
        const token=call(seal,JSON.stringify({sub:'a',exp:20}),new Uint8Array(12),'');
        return JSON.parse(call(open,token,'',''));
    }"#,
        true,
    );
    let mut catalog = None;
    for (algorithm, usages, seal, open) in [
        ("Ed25519", ["sign", "verify"], "jwt.sign", "jwt.verify"),
        (
            "A256GCM",
            ["encrypt", "decrypt"],
            "jwt.encrypt",
            "jwt.decrypt",
        ),
    ] {
        let (keys, _) = managed::prepare(
            catalog.as_ref(),
            &json!({"operation":"generate","name":algorithm,"algorithm":algorithm}),
        )
        .unwrap();
        let (keys, _) = managed::prepare(
            Some(&keys),
            &json!({"operation":"bind","name":open,"key":algorithm,"usages":usages}),
        )
        .unwrap();
        let declaration = json!({"kind":"key","name":open,"algorithm":algorithm,"usages":usages});
        let mut calls = Vec::new();
        let result = execute(
            &code,
            "test",
            &json!({"key":declaration,"seal":seal,"open":open}),
            "query",
            &mut |operation, arguments| {
                calls.push((operation.to_owned(), arguments.clone()));
                Ok(match operation {
                    "managedKey" => managed::resolve(
                        &keys,
                        &declaration,
                        arguments[0]["operation"].as_str().unwrap(),
                        arguments[0]["kid"].as_str(),
                    )?,
                    "clock" | "now" => json!(5_000),
                    _ => Value::Null,
                })
            },
            limits(),
        )
        .unwrap();
        assert_eq!(result["ok"], true, "{open}: {result}");
        assert_eq!(result["value"]["claims"]["sub"], "a", "{open}: {result}");
        let names: Vec<_> = calls.iter().map(|(name, _)| name.as_str()).collect();
        // ctx.clock() keeps the result's certificate; ctx.now() would poll.
        assert_eq!(
            names,
            ["managedKey", "managedKey", "clock", "changesAt"],
            "{open}"
        );
        assert_eq!(calls[3].1, json!([20_000]), "{open}");
        catalog = Some(keys);
    }
}

#[test]
fn native_bytes_round_trip_views_empty_inputs_and_authentication() {
    let code = bundle(
        r#"() => {
        const call = (op,...args) => __flowerCrypto(op,0,...args);
        const backing = new Uint8Array([91,1,2,3,92]);
        const message = backing.subarray(1,4), nonce = new Uint8Array(24), key = new Uint8Array(32);
        const sealed = call(1,message,nonce,key);
        const plain = call(2,sealed,nonce,key);
        const pair = call(12,key), signature = call(10,message,pair.subarray(32));
        const valid = call(11,message,signature,pair.subarray(0,32));
        signature[0] ^= 1; sealed[0] ^= 1;
        const invalid = call(11,message,signature,pair.subarray(0,32));
        const empty = call(2,call(1,new Uint8Array(0),nonce,key),nonce,key);
        return {plain:Array.from(plain),valid,invalid,tampered:call(2,sealed,nonce,key),
            empty:Array.from(empty),emptyEquals:call(16,empty,empty),digest:Array.from(call(15,message)),
            view:plain instanceof Uint8Array};
    }"#,
        true,
    );
    let result = run(&code, Value::Null).unwrap();
    assert_eq!(result["ok"], true, "{result}");
    let value = &result["value"];
    assert_eq!(value["plain"], json!([1, 2, 3]));
    assert_eq!(value["valid"], true);
    assert_eq!(value["invalid"], false);
    assert_eq!(value["tampered"], Value::Null);
    assert_eq!(value["empty"], json!([]));
    assert_eq!(value["emptyEquals"], false);
    assert_eq!(value["digest"].as_array().unwrap().len(), 64);
    assert_eq!(value["view"], true);
}

#[test]
fn entropy_is_mutation_only_and_not_frozen_in_snapshots() {
    let compute = "()=>Array.from(__flowerCrypto(0,32))";
    let query = bundle(compute, true);
    let rejected = run(&query, Value::Null).unwrap();
    assert_eq!(rejected["ok"], false);
    assert!(
        rejected["error"]["message"]
            .as_str()
            .unwrap()
            .contains("only in mutations")
    );
    let mutation = query
        .replace("kind:'queryMethod'", "kind:'mutationMethod'")
        .replace("kind:'query'", "kind:'mutation'");
    let mut samples = Vec::new();
    for _ in 0..3 {
        let result = execute(
            &mutation,
            "test",
            &Value::Null,
            "mutation",
            &mut |_, _| Ok(Value::Null),
            limits(),
        )
        .unwrap();
        assert_eq!(result["ok"], true, "{result}");
        samples.push(result["value"].clone());
    }
    assert_ne!(samples[0], samples[1]);
    assert_ne!(samples[1], samples[2]);
    for kind in ["derived", "transaction"] {
        let definition_kind = if kind == "derived" {
            "derived"
        } else {
            "transactionMethod"
        };
        let code = query.replace("kind:'queryMethod'", &format!("kind:'{definition_kind}'"));
        let rejected = execute(
            &code,
            "test",
            &Value::Null,
            kind,
            &mut |_, _| Ok(Value::Null),
            limits(),
        )
        .unwrap();
        assert_eq!(rejected["ok"], false);
        assert!(
            rejected["error"]["message"]
                .as_str()
                .unwrap()
                .contains("only in mutations")
        );
    }
    for marker in ["", STATIC_INIT_MARKER] {
        let code = format!("{marker}const secret=__flowerCrypto(0,32);{mutation}");
        let rejected = execute(
            &code,
            "test",
            &Value::Null,
            "mutation",
            &mut |_, _| Ok(Value::Null),
            limits(),
        );
        assert!(
            rejected.is_err(),
            "entropy was permitted during initialization"
        );
    }
}

#[test]
fn nested_derived_callback_does_not_inherit_mutation_entropy_rights() {
    let outer = bundle("ctx=>ctx.get({kind:'derived',name:'child'},null)", true)
        .replace("kind:'queryMethod'", "kind:'mutationMethod'")
        .replace("kind:'query'", "kind:'mutation'");
    let child = bundle("()=>Array.from(__flowerCrypto(0,32))", true)
        .replace("kind:'queryMethod'", "kind:'derived'");
    let shared = limits();
    let result = execute(
        &outer,
        "test",
        &Value::Null,
        "mutation",
        &mut |method, _| {
            assert_eq!(method, "get");
            execute(
                &child,
                "test",
                &Value::Null,
                "derived",
                &mut |_, _| Ok(Value::Null),
                shared.clone(),
            )
        },
        shared.clone(),
    )
    .unwrap();
    assert_eq!(result["ok"], true);
    assert_eq!(result["value"]["ok"], false);
    assert!(
        result["value"]["error"]["message"]
            .as_str()
            .unwrap()
            .contains("only in mutations")
    );
}

#[test]
fn jwt_bridge_uses_tracked_invocation_clock_and_standard_tokens() {
    let code = bundle(
        r#"(_ctx,args)=>{
        const key=new Uint8Array(32).fill(42);
        const claims=JSON.stringify({sub:'🌻',exp:100,nbf:1});
        const sign=JSON.stringify({algorithm:'HS256',keyFormat:'raw'});
        const verify=JSON.stringify({algorithms:['HS256'],keyFormat:'raw'});
        const token=__flowerCrypto(100,0,claims,key,sign);
        const encrypted=__flowerCrypto(102,0,claims,key,new Uint8Array(12),'{}');
        return {token,encrypted,verified:JSON.parse(__flowerCrypto(101,0,token,key,verify)),
            decrypted:JSON.parse(__flowerCrypto(103,0,encrypted,key,'{}'))};
    }"#,
        true,
    );
    let mut calls = Vec::new();
    let result = execute(
        &code,
        "test",
        &Value::Null,
        "query",
        &mut |method, args| {
            calls.push((method.to_owned(), args));
            Ok(if method == "changesAt" {
                Value::Null
            } else {
                json!(99_999)
            })
        },
        limits(),
    )
    .unwrap();
    assert_eq!(result["ok"], true, "{result}");
    // Verification and decryption declare the expiry (exp 100 s) instead of polling.
    assert_eq!(
        calls,
        [
            ("clock".to_owned(), json!([])),
            ("changesAt".to_owned(), json!([100_000])),
            ("clock".to_owned(), json!([])),
            ("changesAt".to_owned(), json!([100_000])),
        ]
    );
    assert_eq!(result["value"]["verified"]["claims"]["sub"], "🌻");
    assert_eq!(
        result["value"]["decrypted"]["claims"],
        result["value"]["verified"]["claims"]
    );
    let expired = execute(
        &code,
        "test",
        &Value::Null,
        "query",
        &mut |_, _| Ok(json!(100_000)),
        limits(),
    )
    .unwrap();
    assert_eq!(expired["ok"], false);
    assert!(
        expired["error"]["message"]
            .as_str()
            .unwrap()
            .contains("JWT expired")
    );
}

#[test]
fn failed_jwt_decryption_reads_the_clock_as_ctx_now_does() {
    // Only the key opens the claims, so a failure can't say when it would
    // succeed: this token becomes active at 200 s.
    let code = bundle(
        r#"(_ctx,claims)=>{
        const key=new Uint8Array(32).fill(42);
        const encrypted=__flowerCrypto(102,0,JSON.stringify(claims),key,new Uint8Array(12),'{}');
        try { __flowerCrypto(103,0,encrypted,key,'{}'); return 'opened'; }
        catch (error) { return String(error.message); }
    }"#,
        true,
    );
    let mut calls = Vec::new();
    let result = execute(
        &code,
        "test",
        &json!({"sub":"a","nbf":200,"exp":300}),
        "query",
        &mut |method, _| {
            calls.push(method.to_owned());
            Ok(json!(100_000))
        },
        limits(),
    )
    .unwrap();
    assert!(
        result["value"].as_str().unwrap().contains("not active yet"),
        "{result}"
    );
    assert_eq!(calls, ["clock", "now"]);
}

#[test]
fn native_crypto_budget_cannot_be_caught_and_ignored() {
    let code = bundle(
        "()=>{try{__flowerCrypto(0,0x7fffffff)}catch(e){}return 1}",
        true,
    )
    .replace("kind:'queryMethod'", "kind:'mutationMethod'")
    .replace("kind:'query'", "kind:'mutation'");
    let shared = limits();
    let result = execute(
        &code,
        "test",
        &Value::Null,
        "mutation",
        &mut |_, _| Ok(Value::Null),
        shared.clone(),
    );
    assert!(result.is_err());
    assert!(shared.check().is_err());
}

#[test]
fn native_validation_errors_are_catchable_without_panic_or_key_disclosure() {
    let code = bundle(
        r#"()=>{
        const rejected=[];
        for (const invoke of [
            ()=>__flowerCrypto(4000,0),
            ()=>__flowerCrypto(1,0,new Uint8Array(8)),
            ()=>__flowerCrypto(1,0,new Uint8Array(8),new Uint8Array(23),new Uint8Array(32)),
            ()=>__flowerCrypto(15,0,new Uint16Array(4)),
            ()=>__flowerCrypto(100,0,'{"sub":1,"sub":2}',new Uint8Array(32),'{"algorithm":"HS256","keyFormat":"raw"}'),
        ]) { try {invoke();rejected.push(false)} catch(e) {rejected.push(true)} }
        return rejected;
    }"#,
        true,
    );
    assert_eq!(
        run(&code, Value::Null).unwrap()["value"],
        json!([true, true, true, true, true])
    );
}

#[test]
fn sha256_and_webauthn_verification_are_reachable_from_the_guest() {
    let code = bundle(
        r#"()=>{
        const digest = Array.from(__flowerCrypto(17,0,'abc'));
        const expected = JSON.stringify({challenge:'AAECAwQFBgcICQoLDA0ODw',origins:['https://example.com'],
            rpId:'example.com',userVerification:'required',algorithms:[-7]});
        let refusal = null, arity = false;
        try { __flowerCrypto(110,0,'{}',expected) } catch (e) { refusal = e.message }
        try { __flowerCrypto(111,0,'{}') } catch (e) { arity = true }
        return {digest, refusal, arity};
    }"#,
        true,
    );
    let value = &run(&code, Value::Null).unwrap()["value"];
    use sha2::Digest;
    assert_eq!(
        value["digest"],
        json!(sha2::Sha256::digest(b"abc").to_vec())
    );
    assert_eq!(
        value["refusal"],
        "CRYPTO_ERROR: WebAuthn response type must be public-key"
    );
    assert_eq!(value["arity"], true);
}
