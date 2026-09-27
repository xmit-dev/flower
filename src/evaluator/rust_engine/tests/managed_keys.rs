use super::*;

fn declaration() -> Value {
    json!({"kind":"key","name":"sessions","algorithm":"Ed25519","usages":["sign","verify"]})
}

// The coordinator tests only policy/dependency tracking; envelope authentication
// and real private key use are covered by native and three-node bridge tests.
fn catalog(version: u64, bound: bool, revoked: bool) -> Value {
    let envelope = json!({"provider":"mounted","wrappingId":"test","wrappedDek":"","wrappingNonce":"","ciphertext":"","nonce":""});
    json!({
        "domain":"tenant-a","revision":version,
        "keys":{"signing":{"id":"key-a","algorithm":"Ed25519","activeVersion":version,
            "versions":{version.to_string():{"envelope":envelope,"revoked":revoked}}}},
        "bindings": if bound {json!({"sessions":{"key":"signing","usages":["sign","verify","publicKey"]}})} else {json!({})}
    })
}

fn fixture() -> Fixture {
    Fixture::new([
        (
            "version",
            (|_, host| {
                let resolved = host(
                    "managedKey",
                    json!([{"key":declaration(),"operation":"jwt.sign"}]),
                )?;
                Ok(resolved["version"].clone())
            }) as Callback,
        ),
        (
            "parent",
            (|_, host| get(host, "derived", "version", Value::Null)) as Callback,
        ),
        ("unrelated", (|_, _| Ok(json!(99))) as Callback),
        (
            "readParent",
            (|_, host| get(host, "derived", "parent", Value::Null)) as Callback,
        ),
        (
            "readUnrelated",
            (|_, host| get(host, "derived", "unrelated", Value::Null)) as Callback,
        ),
        (
            "forged",
            (|args, host| host("managedKey", json!([{"key":args,"operation":"jwt.sign"}])))
                as Callback,
        ),
    ])
}

#[test]
fn policy_updates_repair_errors_invalidate_dependents_and_persist_revocation() {
    let fixture = fixture();
    let mut data = Records::default();
    data.insert("managedKeys".into(), catalog(1, false, false));
    deploy(
        &mut data,
        json!({"keyDeclarations":[declaration()],"materialize":[{"name":"parent"},{"name":"unrelated"}]}),
        &fixture,
    );
    let child = cell_id("version", &Value::Null);
    let parent = cell_id("parent", &Value::Null);
    assert_eq!(data[&child]["outcome"]["ok"], false);
    assert_eq!(data[&child]["deps"], json!(["managedKeys"]));
    assert!(
        data.reactive().cacheable(),
        "key readers are not clock readers"
    );
    for (version, revoked) in [(1, false), (2, false), (2, true)] {
        fixture.calls.borrow_mut().clear();
        let result = run(
            data.clone(),
            json!({"catalog":catalog(version,true,revoked)}),
            "keyUpdate",
            None,
            &fixture,
        )
        .unwrap();
        assert!(result.puts.contains_key("managedKeys"));
        assert!(
            !fixture
                .calls
                .borrow()
                .iter()
                .any(|name| name == "unrelated")
        );
        apply(&mut data, result);
        assert_eq!(data[&child]["outcome"]["ok"], !revoked);
        assert_eq!(data[&parent]["outcome"]["ok"], !revoked);
        if !revoked {
            assert_eq!(data[&parent]["outcome"]["value"], version);
        } else {
            assert!(
                data[&parent]["outcome"]["error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("revoked")
            );
        }
    }
}

#[test]
fn code_cannot_use_an_undeclared_or_expanded_capability() {
    let fixture = fixture();
    let mut data = Records::default();
    data.insert("managedKeys".into(), catalog(1, true, false));
    data.insert("keyDeclarations".into(), json!([declaration()]));
    let good = run(
        data.clone(),
        json!({"name":"forged","args":declaration()}),
        "query",
        None,
        &fixture,
    )
    .unwrap();
    assert_eq!(good.value["version"], 1);
    assert!(!good.query_cacheable);
    let mut different = declaration();
    different["name"] = json!("other");
    let mut expanded = declaration();
    expanded["usages"]
        .as_array_mut()
        .unwrap()
        .push(json!("publicKey"));
    for value in [different, expanded, Value::Null] {
        let error = run(
            data.clone(),
            json!({"name":"forged","args":value}),
            "query",
            None,
            &fixture,
        )
        .unwrap_err();
        assert_eq!(error.code, "KEY_FORBIDDEN");
    }
    let denied = run(
        data,
        json!({"name":"forged","args":declaration()}),
        "transaction",
        None,
        &fixture,
    )
    .unwrap_err();
    assert_eq!(denied.code, "TRANSACTION_PLAN_ONLY");
}

#[test]
fn queries_resolving_keys_stay_certified_so_watches_need_no_timer() {
    // Reusing a derived value that resolved a key unwraps the catalog, and the
    // mounted wrapping key is read once per process.
    const CHILD: &str = "FLOWER_TEST_CERTIFIED_KEYS_CHILD";
    if std::env::var_os(CHILD).is_none() {
        use std::io::Write;
        let mut wrapping = tempfile::NamedTempFile::new().unwrap();
        wrapping.write_all(&[37; 32]).unwrap();
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "evaluator::rust_engine::tests::managed_keys::queries_resolving_keys_stay_certified_so_watches_need_no_timer",
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
    let (keys, _) = managed::prepare(
        None,
        &json!({"operation":"generate","name":"signing","algorithm":"Ed25519"}),
    )
    .unwrap();
    let (keys, _) = managed::prepare(
        Some(&keys),
        &json!({"operation":"bind","name":"sessions","key":"signing","usages":["sign","verify"]}),
    )
    .unwrap();
    let fixture = fixture();
    let mut data = Records::default();
    data.insert("managedKeys".into(), keys);
    data.insert("keyDeclarations".into(), json!([declaration()]));
    // Directly, and through a derived value this query computes itself.
    for name in ["version", "parent"] {
        let result = run(
            data.clone(),
            json!({"name":name,"args":null}),
            "query",
            None,
            &fixture,
        )
        .unwrap();
        assert_eq!(result.value, 1, "{name}");
        assert!(!result.query_cacheable, "{name}");
        assert!(!result.query_clock_polled, "{name}");
        let certificate = result.query_certificate.expect(name);
        assert!(certificate.valid(&data), "{name}");
        let mut revoked = data.clone();
        revoked.insert("managedKeys".into(), catalog(1, true, true));
        assert!(!certificate.valid(&revoked), "{name}");
    }
    // Stored values that resolved a key leave other results cacheable, and
    // results that read them certified.
    deploy(
        &mut data,
        json!({"keyDeclarations":[declaration()],"materialize":[{"name":"parent"},{"name":"unrelated"}]}),
        &fixture,
    );
    assert!(data.reactive().cacheable());
    for (name, value, cacheable) in [("readParent", 1, false), ("readUnrelated", 99, true)] {
        let result = run(
            data.clone(),
            json!({"name":name,"args":null}),
            "query",
            None,
            &fixture,
        )
        .unwrap();
        assert_eq!(result.value, value, "{name}");
        assert_eq!(result.query_cacheable, cacheable, "{name}");
        assert!(!result.query_clock_polled, "{name}");
        let certificate = result.query_certificate.expect(name);
        assert!(certificate.valid(&data), "{name}");
        let mut revoked = data.clone();
        revoked.insert("managedKeys".into(), catalog(1, true, true));
        assert!(!certificate.valid(&revoked), "{name}");
    }
}

#[test]
fn key_update_without_a_bundle_produces_a_durable_catalog_only() {
    let result = run(
        Records::default(),
        json!({"catalog":catalog(1,false,false)}),
        "keyUpdate",
        None,
        &fixture(),
    )
    .unwrap();
    assert_eq!(result.puts.len(), 1);
    assert_eq!(result.puts["managedKeys"], catalog(1, false, false));
    assert!(result.evaluated.is_empty());
}

#[test]
fn key_declaration_removal_recomputes_previously_materialized_values() {
    let mut data = Records::default();
    let fixture = fixture();
    data.insert("managedKeys".into(), catalog(1, true, false));
    deploy(
        &mut data,
        json!({"keyDeclarations":[declaration()],"materialize":[{"name":"parent"}]}),
        &fixture,
    );
    // Real declarations only change with a deployed bundle; its changed hash
    // triggers the same dependency recomputation as any code deployment.
    deploy(
        &mut data,
        json!({"keyDeclarations":[],"bundle":{"hash":"new","javascript":"new"}}),
        &fixture,
    );
    assert_eq!(
        data[&cell_id("parent", &Value::Null)]["outcome"]["error"]["code"],
        "KEY_FORBIDDEN"
    );
}
