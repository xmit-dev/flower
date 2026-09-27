//! Integration against the real Flower server: spawns `$FLOWER_BIN` (default
//! `~/src/flower/target/release/flower`), initializes Raft, builds and deploys a small app with the
//! SDK's own `buildBundle`, then exercises query/mutate/subscribe/partitions/admin. Skips (passing)
//! when the binary, Node or an SDK with esbuild is missing.

mod common;

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use common::*;
use flower_client::admin::uninitialized;
use flower_client::{
    BundleOptions, Credentials, DeployOptions, ErrorKind, FlowerAdmin, FlowerClient, FlowerError,
    Initialization, MutationOptions, Reconnect, RequestOptions, RetentionAction, SubscribeOptions,
    WatchDelta, WatchOptions, build_bundle,
};
use futures_util::StreamExt;
use serde_json::{Value, json};

struct Flower {
    child: std::sync::Mutex<Child>,
    url: String,
    address: String,
    token: String,
    _data: tempfile::TempDir,
    log: PathBuf,
}

impl Drop for Flower {
    fn drop(&mut self) {
        let child = self.child.get_mut().unwrap();
        let _ = child.kill();
        let _ = child.wait();
    }
}

impl Flower {
    fn exited(&self) -> Option<std::process::ExitStatus> {
        self.child.lock().unwrap().try_wait().unwrap()
    }

    fn logs(&self) -> String {
        let text = std::fs::read_to_string(&self.log).unwrap_or_default();
        text[text.len().saturating_sub(4000)..].to_owned()
    }
}

fn binary() -> Option<PathBuf> {
    let path = std::env::var_os("FLOWER_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var("HOME").unwrap_or_default())
                .join("src/flower/target/release/flower")
        });
    path.is_file().then_some(path)
}

/// An SDK directory whose `bundle.ts` can import esbuild.
fn sdk() -> Option<PathBuf> {
    let home = PathBuf::from(std::env::var("HOME").unwrap_or_default());
    let candidates = [
        std::env::var_os("FLOWER_SDK_DIR").map(PathBuf::from),
        Some(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../sdk")),
        Some(home.join("src/flower/sdk")),
    ];
    candidates
        .into_iter()
        .flatten()
        .find(|sdk| {
            sdk.join("bundle.ts").is_file()
                && sdk.join("../node_modules/esbuild/package.json").is_file()
        })
        .map(|sdk| std::fs::canonicalize(sdk).unwrap())
}

fn node() -> bool {
    Command::new("node")
        .arg("--version")
        .output()
        .is_ok_and(|output| output.status.success())
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn random_hex(bytes: usize) -> String {
    (0..bytes)
        .map(|_| format!("{:02x}", fastrand::u8(..)))
        .collect()
}

/// One node that is also its own partition catalog (group "main").
fn start(binary: &Path) -> Flower {
    let data = tempfile::tempdir().unwrap();
    let keyring = data.path().join("keyring");
    let mut file = std::fs::File::create(&keyring).unwrap();
    file.write_all(&(0..32).map(|_| fastrand::u8(..)).collect::<Vec<_>>())
        .unwrap();
    std::fs::set_permissions(
        &keyring,
        std::os::unix::fs::PermissionsExt::from_mode(0o600),
    )
    .unwrap();
    let port = free_port();
    let address = format!("127.0.0.1:{port}");
    let token = random_hex(16);
    let log = data.path().join("flower.log");
    let output = std::fs::File::create(&log).unwrap();
    let mut command = Command::new(binary);
    for (name, _) in std::env::vars_os() {
        if name.to_string_lossy().starts_with("FLOWER_") {
            command.env_remove(name);
        }
    }
    let child = command
        .args(["--id", "1", "--listen", &address, "--data"])
        .arg(data.path().join("db"))
        .env("FLOWER_ADMIN_TOKEN", &token)
        .env("FLOWER_KEYRING_FILE", &keyring)
        .env("FLOWER_GROUP", "main")
        .env("FLOWER_CATALOG_GROUP", "main")
        .env("FLOWER_GROUPS", json!({ "main": [address] }).to_string())
        .env("RUST_LOG", "warn")
        .stdin(Stdio::null())
        .stdout(output.try_clone().unwrap())
        .stderr(output)
        .spawn()
        .unwrap();
    Flower {
        child: child.into(),
        url: format!("http://{address}"),
        address,
        token,
        _data: data,
        log,
    }
}

const APP: &str = r#"
import { collection, define, fail, mutation, query, v } from "SDK/index.ts";
const counters = collection("counters", v.object({ n: v.int() }));
const count = query("count", (ctx) => ctx.get(counters, "main")?.n ?? 0);
const echo = query("echo", (_ctx, args: unknown) => args);
const add = mutation("add", { args: v.object({ by: v.int({ min: 1 }) }) }, (ctx, { by }) => {
  const n = (ctx.get(counters, "main")?.n ?? 0) + by;
  ctx.set(counters, "main", { n });
  return n;
});
const boom = mutation("boom", () => fail("NOPE", "not today", { why: 1 }));
export default define({ http: { count, echo, add, boom } });
"#;

async fn until<T>(
    label: &str,
    flower: &Flower,
    mut probe: impl AsyncFnMut() -> Result<Option<T>, FlowerError>,
) -> T {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let mut last = None;
    while std::time::Instant::now() < deadline {
        match probe().await {
            Ok(Some(value)) => return value,
            Ok(None) => {}
            Err(error) => last = Some(error),
        }
        if let Some(status) = flower.exited() {
            panic!("{label}: flower exited with {status}\n{}", flower.logs());
        }
        tokio::time::sleep(ms(100)).await;
    }
    panic!("{label} timed out: {last:?}\n{}", flower.logs());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn against_the_real_server() {
    let (Some(binary), Some(sdk), true) = (binary(), sdk(), node()) else {
        eprintln!(
            "skipping: needs $FLOWER_BIN (or ~/src/flower/target/release/flower), node and an SDK with esbuild"
        );
        return;
    };
    let flower = start(&binary);
    let admin = FlowerAdmin::builder(&flower.url)
        .admin_token(&flower.token)
        .build()
        .unwrap();

    // Raft: initialize a one-member cluster once, then wait until it leads (scripts/init.ts).
    let metrics = until("metrics", &flower, async || {
        admin.raft_metrics().await.map(Some)
    })
    .await;
    assert!(uninitialized(&metrics), "{metrics}");
    admin
        .initialize(&BTreeMap::from([("1".to_owned(), flower.address.clone())]))
        .await
        .unwrap();
    let leader = admin
        .wait_for_leader(Duration::from_secs(30), ms(200))
        .await
        .unwrap();
    assert_eq!(leader["state"], json!("Leader"));
    assert!(!uninitialized(&leader));
    let unauthorized = FlowerAdmin::new(&flower.url, Some("wrong".into()))
        .unwrap()
        .raft_metrics()
        .await
        .unwrap_err();
    assert!(matches!(unauthorized.status, 401 | 403), "{unauthorized:?}");

    // Bundle with the SDK's buildBundle, deploy, and redeploy idempotently.
    let build = tempfile::tempdir().unwrap();
    let entry = build.path().join("entry.ts");
    std::fs::write(&entry, APP.replace("SDK", &sdk.display().to_string())).unwrap();
    let options = BundleOptions {
        sdk_dir: Some(sdk.clone()),
        ..Default::default()
    };
    let bundle = build_bundle(&entry, options.clone()).await.unwrap();
    assert!(bundle.javascript.starts_with("/* flower:static-init */\n"));
    assert_eq!(
        bundle.hash,
        flower_client::bundle::sha256_hex(bundle.bytes())
    );
    assert_eq!(
        build_bundle(&entry, options.clone()).await.unwrap(),
        bundle,
        "builds are deterministic"
    );
    let per_call = build_bundle(
        &entry,
        BundleOptions {
            initialization: Initialization::PerInvocation,
            ..options.clone()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        format!("/* flower:static-init */\n{}", per_call.javascript),
        bundle.javascript
    );
    let broken = build.path().join("broken.ts");
    std::fs::write(&broken, "export default {;\n").unwrap();
    assert_eq!(
        build_bundle(&broken, options.clone())
            .await
            .unwrap_err()
            .code,
        "BUNDLE_FAILED"
    );

    let request_id = format!("deploy::{}", bundle.hash);
    let deployable: flower_client::Bundle = bundle.clone().into();
    let deploy = || {
        admin.deploy(
            &deployable,
            DeployOptions {
                request_id: Some(request_id.clone()),
                preparation: None,
            },
        )
    };
    let receipt = until("deployment", &flower, async || deploy().await.map(Some)).await;
    let again = deploy().await.unwrap();
    assert!(again.duplicate, "{again:?}");
    assert_eq!(again.revision, receipt.revision);

    // Queries and mutations, typed and untyped, with stable request IDs.
    let client = FlowerClient::new(&flower.url).unwrap();
    assert_eq!(
        client
            .query::<_, u64>("count", &(), RequestOptions::retrying())
            .await
            .unwrap()
            .value,
        0
    );
    let added = client
        .mutate::<_, u64>(
            "add",
            &json!({"by": 2}),
            MutationOptions::retrying().request_id("add-1"),
        )
        .await
        .unwrap();
    assert_eq!((added.value, added.duplicate), (2, false));
    let replayed = client
        .mutate::<_, u64>(
            "add",
            &json!({"by": 2}),
            MutationOptions::retrying().request_id("add-1"),
        )
        .await
        .unwrap();
    assert_eq!(
        (replayed.value, replayed.duplicate, replayed.revision),
        (2, true, added.revision)
    );
    let reused = client
        .mutate_value(
            "add",
            &json!({"by": 3}),
            MutationOptions::retrying().request_id("add-1"),
        )
        .await
        .unwrap_err();
    assert_eq!(
        (reused.status, reused.code.as_str(), reused.is_transient()),
        (409, "REQUEST_ID_REUSED", false)
    );
    let counted = client
        .query::<_, u64>("count", &(), RequestOptions::new())
        .await
        .unwrap();
    assert_eq!(counted.value, 2);
    assert!(counted.revision >= added.revision);

    // Errors: the method's failure, unknown aliases, kind mismatches, invalid input.
    let failed = client
        .mutate_value("boom", &(), MutationOptions::retrying())
        .await
        .unwrap_err();
    assert_eq!(
        (failed.status, failed.code.as_str()),
        (422, "EVALUATION_FAILED")
    );
    let failure = failed.failure.as_deref().unwrap();
    assert_eq!(
        (
            failure.code.as_str(),
            failure.message.as_str(),
            &failure.details
        ),
        ("NOPE", "not today", &Some(json!({"why": 1})))
    );
    assert!(!failed.is_transient());
    let missing = client
        .query_value("nope", &(), RequestOptions::retrying())
        .await
        .unwrap_err();
    assert_eq!(
        (missing.status, missing.code.as_str()),
        (404, "METHOD_NOT_FOUND")
    );
    let kind = client
        .query_value("add", &json!({"by": 1}), RequestOptions::new())
        .await
        .unwrap_err();
    assert_eq!(
        (kind.status, kind.code.as_str()),
        (422, "METHOD_KIND_MISMATCH")
    );
    let invalid = client
        .mutate_value("add", &json!({"by": 0}), MutationOptions::new())
        .await
        .unwrap_err();
    assert_eq!(invalid.status, 422, "{invalid:?}");

    // JSON round trip through the app: JS numbers, key order, astral characters.
    let odd = json!({"b": [1e21, 0.1, -0.0, 1e-7, 9_007_199_254_740_993u64], "a": "😀\u{2028}\u{0}", "10": {}, "2": []});
    let echoed = client
        .query_value("echo", &odd, RequestOptions::new())
        .await
        .unwrap()
        .value;
    assert_eq!(
        flower_client::canonical_json(&echoed),
        flower_client::canonical_json(&odd)
    );
    assert_eq!(
        flower_client::canonical_json(&echoed),
        "{\"10\":{},\"2\":[],\"a\":\"😀\u{2028}\\u0000\",\"b\":[1e+21,0.1,0,1e-7,9007199254740992]}"
    );

    // Credentials travel in the body; an app without an auth hook admits anyone.
    let credentialed = client.with_credentials(Credentials::token("anything"));
    assert_eq!(
        credentialed
            .query::<_, u64>("count", &(), RequestOptions::new())
            .await
            .unwrap()
            .value,
        2
    );

    // Watches: raw deltas start with a snapshot; subscribe follows mutations.
    let mut deltas = client.watch_deltas("count", &(), WatchOptions::default());
    match bounded(deltas.next()).await.unwrap().unwrap() {
        WatchDelta::Snapshot { value, .. } => assert_eq!(value, json!(2)),
        other => panic!("{other:?}"),
    }
    drop(deltas);
    let mut updates = client.subscribe::<_, u64>("count", &(), SubscribeOptions::new());
    let first = bounded(updates.next()).await.unwrap().unwrap();
    assert_eq!((first.value, first.reset), (2, true));
    client
        .mutate_value("add", &json!({"by": 5}), MutationOptions::retrying())
        .await
        .unwrap();
    let next = bounded(updates.next()).await.unwrap().unwrap();
    assert_eq!((next.value, next.reset), (7, false));
    assert!(next.revision > first.revision);
    let waiter = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .wait_until("count", &(), |n: &u64| *n >= 10, SubscribeOptions::new())
                .await
        }
    });
    tokio::time::sleep(ms(50)).await;
    client
        .mutate_value("add", &json!({"by": 3}), MutationOptions::retrying())
        .await
        .unwrap();
    assert_eq!(bounded(waiter).await.unwrap().unwrap().value, 10);
    let gone = client.subscribe::<_, Value>(
        "nope",
        &(),
        SubscribeOptions::new().reconnect(Reconnect::default()),
    );
    let items: Vec<_> = gone.collect().await;
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].as_ref().unwrap_err().code, "METHOD_NOT_FOUND");
    drop(updates);

    // Several connections, many concurrent calls.
    let wide = FlowerClient::builder(&flower.url)
        .connections(3)
        .build()
        .unwrap();
    let calls = (0..64).map(|index| {
        let wide = wide.clone();
        async move {
            wide.query_value("echo", &json!({"index": index}), RequestOptions::retrying())
                .await
        }
    });
    for (index, result) in futures_util::future::join_all(calls)
        .await
        .into_iter()
        .enumerate()
    {
        assert_eq!(result.unwrap().value, json!({"index": index}));
    }

    // Managed keys (FLOWER_KEYRING_FILE) and retention, as workers/deploy.ts drives them.
    let catalog = admin.key_list().await.unwrap().value;
    assert!(
        catalog.keys.is_empty() && catalog.bindings.is_empty(),
        "{catalog:?}"
    );
    admin
        .key_generate("test-tokens", "Ed25519", None, Some("key::test-tokens"))
        .await
        .unwrap();
    let bound = admin
        .key_bind(
            "tokens",
            "test-tokens",
            &["sign", "verify"],
            Some("bind::tokens"),
        )
        .await
        .unwrap()
        .value;
    assert_eq!(bound.keys["test-tokens"].algorithm, "Ed25519");
    assert_eq!(bound.bindings["tokens"].key, "test-tokens");
    assert_eq!(bound.bindings["tokens"].usages, ["sign", "verify"]);
    let status = admin.retention_status().await.unwrap();
    assert!(status.value.is_none(), "{status:?}");
    let action = RetentionAction::Initialize {
        database: random_hex(16),
        incarnation: random_hex(16),
        max_receipt_bytes: None,
    };
    admin
        .control_retention(status.revision, &action)
        .await
        .unwrap();
    let status = admin.retention_status().await.unwrap();
    let state = status.value.clone().unwrap();
    assert!(state.rotation.is_none());
    let rotate = RetentionAction::Rotate {
        incarnation: state.incarnation.clone(),
        epoch_ms: Some(3_600_000),
        keep_epochs: 24,
    };
    admin
        .control_retention(status.revision, &rotate)
        .await
        .unwrap();
    let rotation = admin
        .retention_status()
        .await
        .unwrap()
        .value
        .unwrap()
        .rotation
        .unwrap();
    assert_eq!((rotation.epoch_ms, rotation.keep_epochs), (3_600_000, 24));
    let conflict = admin
        .control_retention(status.revision, &rotate)
        .await
        .unwrap_err();
    assert_eq!(conflict.code, "RETENTION_CONFLICT", "{conflict:?}");

    // Partitions: register the group, create p1, deploy there; its state is its own.
    admin
        .admin::<_, Value>("/admin/partitions/catalog", &json!({"action": "register_group", "group": {"id": "main", "addresses": [flower.address]}}))
        .await
        .unwrap();
    admin
        .admin::<_, Value>("/admin/partitions/catalog", &json!({"action": "create", "partition": "p1", "group": "main", "operation": "create-p1"}))
        .await
        .unwrap();
    until("p1 active", &flower, async || {
        let placement: Value = admin
            .admin(
                "/admin/partitions/catalog",
                &json!({"action": "resolve", "partition": "p1"}),
            )
            .await?;
        Ok((placement["status"] == "active").then_some(()))
    })
    .await;
    let p1 = admin.partition("p1");
    assert_eq!(p1.url(), format!("{}/partitions/p1", flower.url));
    until("p1 deployment", &flower, async || {
        p1.deploy(
            &deployable,
            DeployOptions {
                request_id: Some(format!("deploy:p1:{}", bundle.hash)),
                preparation: None,
            },
        )
        .await
        .map(Some)
    })
    .await;
    let tenant = client.partition("p1");
    assert_eq!(
        tenant
            .query::<_, u64>("count", &(), RequestOptions::retrying())
            .await
            .unwrap()
            .value,
        0
    );
    assert_eq!(
        tenant
            .mutate::<_, u64>("add", &json!({"by": 4}), MutationOptions::retrying())
            .await
            .unwrap()
            .value,
        4
    );
    assert_eq!(
        client
            .query::<_, u64>("count", &(), RequestOptions::new())
            .await
            .unwrap()
            .value,
        10
    );
    let mut tenant_updates = tenant.subscribe::<_, u64>("count", &(), SubscribeOptions::new());
    assert_eq!(
        bounded(tenant_updates.next()).await.unwrap().unwrap().value,
        4
    );
    assert!(
        p1.key_list().await.unwrap().value.keys.is_empty(),
        "keys are per partition"
    );
    let unknown = client
        .partition("nowhere")
        .query_value("count", &(), RequestOptions::new())
        .await
        .unwrap_err();
    assert!(unknown.status >= 400, "{unknown:?}");

    // A dead server: calls fail with transient transport errors; retries give up at `attempts`.
    drop(tenant_updates);
    let url = flower.url.clone();
    drop(flower);
    let dead = FlowerClient::new(&url).unwrap();
    let error = dead
        .query_value(
            "count",
            &(),
            RequestOptions::new().retry(
                flower_client::RetryPolicy::default()
                    .attempts(2)
                    .initial_delay(ms(1)),
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(
        (error.kind, error.is_transient()),
        (ErrorKind::Transport, true),
        "{error:?}"
    );
}
