//! The worker against the real Flower server: spawns `$FLOWER_BIN` (default
//! `~/src/flower/target/release/flower`) on a free local port, initializes Raft, deploys a queue
//! and an external value bundled by the SDK's own `buildBundle`, then runs `run_queue_worker` and
//! `reconcile` over a real `FlowerClient`. Skips (passing) when the binary, Node or an SDK with
//! esbuild is missing.
#![cfg(feature = "flower-client")]

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use flower_client::{BundleOptions, DeployOptions, FlowerAdmin, FlowerClient, MutationOptions, RequestOptions, build_bundle};
use flower_worker::{
    Claim, Concurrency, ExternalWork, QueueWorkerEvent, QueueWorkerOptions, ReconcileEvent, ReconcileOptions, WorkError,
    reconcile, run_queue_worker,
};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use tokio_util::sync::CancellationToken;

struct Flower {
    child: Child,
    url: String,
    address: String,
    token: String,
    log: PathBuf,
    _data: tempfile::TempDir,
}

impl Drop for Flower {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Flower {
    fn logs(&self) -> String {
        let text = std::fs::read_to_string(&self.log).unwrap_or_default();
        text[text.len().saturating_sub(4000)..].to_owned()
    }
}

fn home() -> PathBuf {
    PathBuf::from(std::env::var("HOME").unwrap_or_default())
}

fn binary() -> Option<PathBuf> {
    let path = std::env::var_os("FLOWER_BIN")
        .map(PathBuf::from)
        .unwrap_or_else(|| home().join("src/flower/target/release/flower"));
    path.is_file().then_some(path)
}

/// An SDK directory whose `bundle.ts` can import esbuild.
fn sdk() -> Option<PathBuf> {
    [
        std::env::var_os("FLOWER_SDK_DIR").map(PathBuf::from),
        Some(Path::new(env!("CARGO_MANIFEST_DIR")).join("../../sdk")),
        Some(home().join("src/flower/sdk")),
    ]
    .into_iter()
    .flatten()
    .find(|sdk| sdk.join("bundle.ts").is_file() && sdk.join("../node_modules/esbuild/package.json").is_file())
    .map(|sdk| std::fs::canonicalize(sdk).unwrap())
}

fn node() -> bool {
    Command::new("node").arg("--version").output().is_ok_and(|output| output.status.success())
}

fn start(binary: &Path) -> Flower {
    let data = tempfile::tempdir().unwrap();
    let keyring = data.path().join("keyring");
    let mut file = std::fs::File::create(&keyring).unwrap();
    file.write_all(&(0..32).map(|_| fastrand::u8(..)).collect::<Vec<_>>()).unwrap();
    std::fs::set_permissions(&keyring, std::os::unix::fs::PermissionsExt::from_mode(0o600)).unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let address = format!("127.0.0.1:{port}");
    let token: String = (0..16).map(|_| format!("{:02x}", fastrand::u8(..))).collect();
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
        child,
        url: format!("http://{address}"),
        address,
        token,
        log,
        _data: data,
    }
}

/// `examples/workers.ts`'s queue and `docs/reactive-worker.ts`'s digest in one app.
const APP: &str = r#"
import { collection, define, external, mutation, query, v } from "SDK/index.ts";
import { queue } from "SDK/temporal.ts";
const jobs = queue("workerJobs", { lease: { defaultMs: 10_000, maxMs: 30_000 }, retry: { maxAttempts: 5 } });
const documentId = v.string({ min: 1, max: 256 });
const documents = collection("documents", v.object({ text: v.string({ max: 100_000 }) }));
const digest = external("digest", {
  input: (ctx, id: string) => {
    const document = ctx.get(documents, id);
    return document && { recipe: "sha256-v1", text: document.text };
  },
  result: v.string({ pattern: /^[0-9a-f]{64}$/ }),
  each: documents,
});
const put = mutation("document.put", { args: v.object({ id: documentId, text: v.string({ max: 100_000 }) }) }, (ctx, input) => {
  ctx.set(documents, input.id, { text: input.text });
  return null;
});
const get = query("document.get", { args: documentId }, (ctx, id) => {
  const document = ctx.get(documents, id);
  return document && { text: document.text, digest: ctx.get(digest, id) };
});
export default define({
  uses: [jobs, digest],
  http: {
    ...jobs.http("jobs", { methods: ["enqueue", "claim", "renew", "complete", "fail", "release", "retry", "get", "ready", "stats"] }),
    "document.put": put, "document.get": get, ...digest.http("digest"),
  },
});
"#;

async fn until<T, F: Future<Output = Option<T>>>(label: &str, flower: &Flower, mut probe: impl FnMut() -> F) -> T {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if let Some(value) = probe().await {
            return value;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("{label} timed out\n{}", flower.logs());
}

fn sha(text: &str) -> String {
    Sha256::digest(text.as_bytes()).iter().map(|byte| format!("{byte:02x}")).collect()
}

#[derive(Deserialize)]
struct Payload {
    n: u64,
}

#[derive(Clone, Deserialize)]
struct Input {
    text: String,
}

async fn put(client: &FlowerClient, id: &str, text: &str) {
    client
        .mutate::<_, Value>("document.put", &json!({ "id": id, "text": text }), MutationOptions::retrying())
        .await
        .unwrap();
}

async fn digest_ready(client: &FlowerClient, id: &str, text: &str) -> bool {
    let document = client
        .query::<_, Value>("document.get", &json!(id), RequestOptions::retrying())
        .await
        .unwrap()
        .value;
    document["digest"] == json!({ "status": "ready", "value": sha(text) })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_against_the_real_server() {
    let (Some(binary), Some(sdk), true) = (binary(), sdk(), node()) else {
        eprintln!("skipping: needs $FLOWER_BIN (or ~/src/flower/target/release/flower), node and an SDK with esbuild");
        return;
    };
    let flower = start(&binary);
    let admin = FlowerAdmin::builder(&flower.url).admin_token(&flower.token).build().unwrap();
    until("metrics", &flower, || async { admin.raft_metrics().await.ok() }).await;
    admin
        .initialize(&BTreeMap::from([("1".to_owned(), flower.address.clone())]))
        .await
        .unwrap();
    admin.wait_for_leader(Duration::from_secs(30), Duration::from_millis(100)).await.unwrap();
    let build = tempfile::tempdir().unwrap();
    let entry = build.path().join("entry.ts");
    std::fs::write(&entry, APP.replace("SDK", &sdk.display().to_string())).unwrap();
    let options = BundleOptions {
        sdk_dir: Some(sdk.clone()),
        ..Default::default()
    };
    let bundle: flower_client::Bundle = build_bundle(&entry, options).await.unwrap().into();
    let request_id = "deploy::workers".to_owned();
    until("deployment", &flower, || {
        let options = DeployOptions {
            request_id: Some(request_id.clone()),
            preparation: None,
        };
        let deploy = admin.deploy(&bundle, options);
        async move { deploy.await.ok() }
    })
    .await;
    let client = FlowerClient::new(&flower.url).unwrap();

    // A queue worker: waiting in line, chaining claims into reports, adaptive, batched.
    let mut ids: Vec<String> = (0..24).map(|n| format!("job-{n}")).collect();
    ids.push("flaky".into());
    for (n, id) in ids.iter().enumerate() {
        let args = json!({ "id": id, "payload": { "n": n } });
        client.mutate::<_, Value>("jobs.enqueue", &args, MutationOptions::retrying()).await.unwrap();
    }
    let events = Arc::new(Mutex::new(Vec::<QueueWorkerEvent>::new()));
    let recorded = events.clone();
    let stop = CancellationToken::new();
    let options = QueueWorkerOptions {
        owner: Some("rust-worker".into()),
        concurrency: Some(Concurrency::adaptive(1, 2, 8)),
        claimers: Some(2),
        batch: Some(4),
        wait: true,
        wait_ms: Some(10_000),
        chain: true,
        lease_ms: Some(10_000),
        drain_ms: Some(5_000),
        release: true,
        adjust_every_ms: Some(50),
        ..QueueWorkerOptions::new("jobs", stop.clone())
    }
    .on_event(move |event| recorded.lock().push(event));
    let worker = tokio::spawn(run_queue_worker(client.clone(), options, |job: Claim<Payload>, _, _| async move {
        tokio::time::sleep(Duration::from_millis(5)).await;
        if job.id == "flaky" && job.attempt == 1 {
            return Err(WorkError::new("flaky"));
        }
        Ok(json!({ "double": job.payload.n * 2 }))
    }));
    let completed = || events.lock().iter().filter(|event| event.kind() == "completed").count();
    until("the backlog", &flower, || {
        let done = completed() == ids.len();
        async move { done.then_some(()) }
    })
    .await;
    // Idle in line, the worker wakes for new work.
    let args = json!({ "id": "late", "payload": { "n": 100 } });
    client.mutate::<_, Value>("jobs.enqueue", &args, MutationOptions::retrying()).await.unwrap();
    until("the late job", &flower, || {
        let done = completed() == ids.len() + 1;
        async move { done.then_some(()) }
    })
    .await;
    stop.cancel();
    worker.await.unwrap().unwrap();
    for (n, id) in ids.iter().enumerate() {
        let job = client
            .query::<_, Value>("jobs.get", &json!({ "id": id }), RequestOptions::retrying())
            .await
            .unwrap()
            .value;
        assert_eq!((&job["state"], &job["result"]), (&json!("completed"), &json!({ "double": n * 2 })), "{job}");
        assert_eq!(job["attempts"], json!(if id == "flaky" { 2 } else { 1 }), "{job}");
    }
    let events = events.lock().clone();
    assert!(events.iter().any(|event| matches!(event, QueueWorkerEvent::Failed { id, error } if id == "flaky" && error == "flaky")));
    assert!(
        !events.iter().any(|event| matches!(event.kind(), "lost" | "unreported" | "waiting")),
        "{events:?}"
    );

    // Leased reconcile pools keep every digest current.
    let documents: Vec<(String, String)> = (0..6).map(|n| (format!("doc-{n}"), format!("text {n}"))).collect();
    for (id, text) in &documents {
        put(&client, id, text).await;
    }
    let published = Arc::new(Mutex::new(Vec::<ReconcileEvent>::new()));
    let recorded = published.clone();
    let stop = CancellationToken::new();
    let options = ReconcileOptions {
        lease: true,
        owner: Some("rust-reconciler".into()),
        concurrency: Some(Concurrency::Fixed(3)),
        ..ReconcileOptions::new("digest", stop.clone()).on_event(move |event| recorded.lock().push(event))
    };
    let pool = tokio::spawn(reconcile(client.clone(), options, |input: Input, _: ExternalWork<String, Input>, _| async move {
        Ok(sha(&input.text))
    }));
    for (id, text) in &documents {
        until("a digest", &flower, || digest_ready(&client, id, text).then(|ready| ready.then_some(()))).await;
    }
    put(&client, "doc-0", "edited").await;
    until("the edited digest", &flower, || digest_ready(&client, "doc-0", "edited").then(|ready| ready.then_some(()))).await;
    stop.cancel();
    pool.await.unwrap().unwrap();
    let published = published.lock().clone();
    assert_eq!(published.iter().filter(|event| event.kind() == "published").count(), documents.len() + 1, "{published:?}");
}

trait Then: Future + Sized {
    fn then<T>(self, map: impl FnOnce(Self::Output) -> T) -> impl Future<Output = T> {
        async move { map(self.await) }
    }
}

impl<F: Future> Then for F {}
