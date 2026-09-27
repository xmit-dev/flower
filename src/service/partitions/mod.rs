//! Durable placement and roll-forward movement of independently scoped applications.
mod budgets;
pub(super) mod catalog;
pub(super) mod coordinator;
pub(super) mod router;

use crate::consensus::Consensus;
use anyhow::{Context, ensure};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};
use tokio::sync::Mutex;

pub(super) const COMPATIBILITY_HEADER: &str = "x-flower-compatibility";
pub(super) const CATALOG_PATH: &str = "/raft/partitions/catalog";
pub(super) const CONTROL_PATH: &str = "/raft/partitions/control";

#[derive(Clone, Debug)]
pub(super) struct Config {
    pub local_group: String,
    pub catalog_group: String,
    pub catalog_peers: Vec<String>,
    pub base_max_bytes: Option<u64>,
    pub tail_max_bytes: Option<u64>,
}

impl Config {
    pub fn load() -> anyhow::Result<Option<Self>> {
        let Some(catalog_group) = std::env::var_os("FLOWER_CATALOG_GROUP") else {
            return Ok(None);
        };
        let catalog_group = catalog_group
            .into_string()
            .map_err(|_| anyhow::anyhow!("invalid FLOWER_CATALOG_GROUP"))?;
        let local_group =
            std::env::var("FLOWER_GROUP").context("partitions require FLOWER_GROUP")?;
        let groups: BTreeMap<String, Vec<String>> = serde_json::from_str(
            &std::env::var("FLOWER_GROUPS")
                .context("partitions require FLOWER_GROUPS bootstrap peers")?,
        )?;
        validate_name(&local_group)?;
        validate_name(&catalog_group)?;
        ensure!(
            groups.contains_key(&local_group),
            "FLOWER_GROUP is absent from FLOWER_GROUPS"
        );
        let catalog_peers = groups
            .get(&catalog_group)
            .context("FLOWER_CATALOG_GROUP is absent from FLOWER_GROUPS")?
            .clone();
        validate_addresses(&catalog_peers)?;
        Ok(Some(Self {
            local_group,
            catalog_group,
            catalog_peers,
            base_max_bytes: byte_budget("FLOWER_PARTITION_BASE_MAX_BYTES")?,
            tail_max_bytes: byte_budget("FLOWER_PARTITION_TAIL_MAX_BYTES")?,
        }))
    }
}

fn byte_budget(name: &str) -> anyhow::Result<Option<u64>> {
    std::env::var(name)
        .ok()
        .map(|value| {
            value
                .parse::<u64>()
                .with_context(|| format!("{name} must be an unsigned byte budget"))
        })
        .transpose()
}

pub(super) fn validate_name(value: &str) -> anyhow::Result<()> {
    ensure!(
        !value.trim().is_empty() && !value.chars().any(char::is_control),
        "names must be nonempty and contain no control characters"
    );
    Ok(())
}

pub(super) fn validate_addresses(addresses: &[String]) -> anyhow::Result<()> {
    ensure!(!addresses.is_empty(), "a group requires peer addresses");
    let mut distinct = BTreeSet::new();
    for address in addresses {
        let url = reqwest::Url::parse(&format!("http://{address}"))?;
        ensure!(
            url.host_str().is_some()
                && url.port().is_some_and(|port| port > 0)
                && url.username().is_empty()
                && url.password().is_none()
                && url.path() == "/"
                && url.query().is_none()
                && url.fragment().is_none(),
            "group peers must be host:port addresses"
        );
        ensure!(
            distinct.insert(url.to_string()),
            "group peer addresses must be distinct"
        );
    }
    Ok(())
}

pub(super) struct Runtime {
    pub consensus: Consensus,
    pub config: Config,
    pub token: String,
    client: reqwest::Client,
    pub catalog_writer: Mutex<()>,
    export: Mutex<Option<coordinator::ExportCache>>,
    admission: Arc<super::admission::Pool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PeerRequest<T> {
    pub group: String,
    pub body: T,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(super) struct PeerResponse<T> {
    pub group: String,
    pub body: T,
}

impl Runtime {
    pub fn new(
        consensus: Consensus,
        token: String,
        admission: Arc<super::admission::Pool>,
    ) -> anyhow::Result<Option<Arc<Self>>> {
        let Some(config) = Config::load()? else {
            return Ok(None);
        };
        let limits = consensus.limits();
        let client = crate::transport::client_builder()?
            .connect_timeout(limits.peer_connect_timeout)
            .pool_idle_timeout(limits.peer_idle_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .build()?;
        Ok(Some(Arc::new(Self {
            consensus,
            config,
            token,
            client,
            catalog_writer: Mutex::new(()),
            export: Mutex::new(None),
            admission,
        })))
    }

    pub fn authenticate(&self, headers: &HeaderMap, group: &str) -> anyhow::Result<()> {
        ensure!(
            headers.get("authorization").and_then(|v| v.to_str().ok())
                == Some(format!("Bearer {}", self.consensus.peer_token()).as_str()),
            "peer bearer token required"
        );
        ensure!(
            headers
                .get(COMPATIBILITY_HEADER)
                .and_then(|v| v.to_str().ok())
                == Some(crate::consensus::compatibility().contract().as_str()),
            "partition compatibility contract mismatch"
        );
        ensure!(
            group == self.config.local_group,
            "partition control reached the wrong physical group"
        );
        Ok(())
    }

    async fn catalog(&self, request: catalog::CatalogRequest) -> anyhow::Result<serde_json::Value> {
        // Catalog reads can be served by any caught-up catalog replica. Writes
        // fall through to the configured peers when this process is a follower.
        if self.config.local_group == self.config.catalog_group {
            let result = catalog::apply(self, request.clone()).await;
            if result.is_ok() || self.consensus.metrics().state == openraft::ServerState::Leader {
                return result;
            }
        }
        self.send(
            &catalog::Group {
                id: self.config.catalog_group.clone(),
                addresses: self.config.catalog_peers.clone(),
            },
            CATALOG_PATH,
            &request,
        )
        .await
    }

    pub(in crate::service) async fn resolve(
        &self,
        partition: &str,
    ) -> anyhow::Result<catalog::Placement> {
        serde_json::from_value(
            self.catalog(catalog::CatalogRequest::Resolve {
                partition: partition.into(),
            })
            .await?,
        )
        .map_err(Into::into)
    }

    pub(in crate::service) async fn send<Q: Serialize + Sync, R: DeserializeOwned>(
        &self,
        group: &catalog::Group,
        path: &str,
        body: &Q,
    ) -> anyhow::Result<R> {
        let mut last = String::from("no peer answered");
        let mut attempted = BTreeSet::new();
        for address in &group.addresses {
            if attempted.insert(address.clone()) {
                match self.send_to(group, address, path, body).await {
                    Ok(result) => return Ok(result),
                    Err(error) if super::engine_failure(&error).is_some() => return Err(error),
                    Err(error) => last = error.to_string(),
                }
            }
            // Registered addresses are seeds, not a frozen membership list.
            // Ask each seed for at most one quorum-confirmed leader hint; never
            // recursively follow hints, and never resend to an attempted peer.
            let located: anyhow::Result<coordinator::LeaderLocation> = self
                .send_to(
                    group,
                    address,
                    CONTROL_PATH,
                    &coordinator::ControlRequest::Locate,
                )
                .await;
            if let Ok(leader) = located {
                validate_addresses(std::slice::from_ref(&leader.address))?;
                if attempted.insert(leader.address.clone()) {
                    match self.send_to(group, &leader.address, path, body).await {
                        Ok(result) => return Ok(result),
                        Err(error) if super::engine_failure(&error).is_some() => return Err(error),
                        Err(error) => last = error.to_string(),
                    }
                }
            }
        }
        anyhow::bail!("partition group {} unavailable: {last}", group.id)
    }

    async fn send_to<Q: Serialize + Sync, R: DeserializeOwned>(
        &self,
        group: &catalog::Group,
        address: &str,
        path: &str,
        body: &Q,
    ) -> anyhow::Result<R> {
        let limits = self.consensus.limits();
        let timeout = limits.read_timeout.saturating_add(limits.commit_timeout);
        let mut response = self
            .client
            .post(crate::transport::peer_url(address, path))
            .bearer_auth(self.consensus.peer_token())
            .header(
                COMPATIBILITY_HEADER,
                crate::consensus::compatibility().contract(),
            )
            .timeout(timeout)
            .json(&PeerRequest {
                group: group.id.clone(),
                body,
            })
            .send()
            .await?;
        let status = response.status();
        let compatible = response
            .headers()
            .get(COMPATIBILITY_HEADER)
            .and_then(|v| v.to_str().ok())
            == Some(crate::consensus::compatibility().contract().as_str());
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            ensure!(
                bytes.len().saturating_add(chunk.len()) <= limits.rpc_max_bytes,
                "partition control response exceeds FLOWER_RPC_MAX_BYTES"
            );
            bytes.extend_from_slice(&chunk);
        }
        if !status.is_success() {
            if matches!(status, reqwest::StatusCode::UNPROCESSABLE_ENTITY | reqwest::StatusCode::FORBIDDEN)
                && let Some(failure) = super::remote_failure(&bytes)
            {
                return Err(failure);
            }
            anyhow::bail!("{}", String::from_utf8_lossy(&bytes));
        }
        ensure!(
            compatible,
            "partition control response compatibility mismatch"
        );
        let response: PeerResponse<R> = serde_json::from_slice(&bytes)?;
        ensure!(
            response.group == group.id,
            "partition control response reached a different group"
        );
        Ok(response.body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json,
        extract::State,
        http::StatusCode,
        response::{IntoResponse, Response},
        routing::post,
    };
    use serde_json::{Value, json};
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[derive(Clone)]
    struct Peer {
        leader: String,
        seed: bool,
        calls: Arc<AtomicUsize>,
        wrong_group: Arc<AtomicBool>,
        unavailable: Arc<AtomicBool>,
    }
    async fn peer(
        State(peer): State<Peer>,
        headers: HeaderMap,
        Json(input): Json<Value>,
    ) -> Response {
        peer.calls.fetch_add(1, Ordering::Relaxed);
        assert_eq!(headers["authorization"], "Bearer test-partition-routing");
        assert_eq!(
            headers[COMPATIBILITY_HEADER].to_str().unwrap(),
            crate::consensus::compatibility().contract()
        );
        assert_eq!(input["group"], "🌷");
        if input["body"]["action"] != "locate"
            && (peer.seed || peer.unavailable.load(Ordering::Relaxed))
        {
            return StatusCode::SERVICE_UNAVAILABLE.into_response();
        }
        let body = if input["body"]["action"] == "locate" {
            json!({"address": peer.leader})
        } else {
            input["body"].clone()
        };
        let group = if peer.wrong_group.load(Ordering::Relaxed) {
            "wrong-group"
        } else {
            "🌷"
        };
        let mut result = Json(json!({"group":group, "body":body})).into_response();
        result.headers_mut().insert(
            COMPATIBILITY_HEADER,
            axum::http::HeaderValue::from_str(&crate::consensus::compatibility().contract())
                .unwrap(),
        );
        result
    }

    #[tokio::test]
    async fn partition_control_discovers_unlisted_leader_without_loops_or_losing_group_proof() {
        let leader_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let leader = leader_listener.local_addr().unwrap().to_string();
        let seed = seed_listener.local_addr().unwrap().to_string();
        let leader_peer = Peer {
            leader: leader.clone(),
            seed: false,
            calls: Arc::new(AtomicUsize::new(0)),
            wrong_group: Arc::new(AtomicBool::new(false)),
            unavailable: Arc::new(AtomicBool::new(false)),
        };
        let seed_peer = Peer {
            seed: true,
            calls: Arc::new(AtomicUsize::new(0)),
            wrong_group: Arc::new(AtomicBool::new(false)),
            ..leader_peer.clone()
        };
        let leader_router = axum::Router::new()
            .route(CATALOG_PATH, post(peer))
            .route(CONTROL_PATH, post(peer))
            .with_state(leader_peer.clone());
        let seed_router = axum::Router::new()
            .route(CATALOG_PATH, post(peer))
            .route(CONTROL_PATH, post(peer))
            .with_state(seed_peer.clone());
        let leader_task = tokio::spawn(async move {
            axum::serve(leader_listener, leader_router).await.unwrap();
        });
        let seed_task = tokio::spawn(async move {
            axum::serve(seed_listener, seed_router).await.unwrap();
        });
        let directory = tempfile::tempdir().unwrap();
        let consensus = Consensus::open(
            99,
            seed.clone(),
            directory.path().into(),
            "test-partition-routing".into(),
        )
        .await
        .unwrap();
        let runtime = Runtime {
            consensus: consensus.clone(),
            config: Config {
                local_group: "caller".into(),
                catalog_group: "🌷".into(),
                catalog_peers: vec![seed.clone()],
                base_max_bytes: None,
                tail_max_bytes: None,
            },
            token: "test-partition-routing".into(),
            client: reqwest::Client::builder()
                .http2_prior_knowledge()
                .no_proxy()
                .build()
                .unwrap(),
            catalog_writer: Mutex::new(()),
            export: Mutex::new(None),
            admission: super::super::admission::Pool::configured().unwrap(),
        };
        let group = catalog::Group {
            id: "🌷".into(),
            addresses: vec![seed],
        };
        let command = json!({"action":"create", "partition":"tenant", "operation":"same-identity"});
        let received: Value = runtime.send(&group, CATALOG_PATH, &command).await.unwrap();
        assert_eq!(received, command);
        assert_eq!(seed_peer.calls.load(Ordering::Relaxed), 2);
        assert_eq!(leader_peer.calls.load(Ordering::Relaxed), 1);
        leader_peer.wrong_group.store(true, Ordering::Relaxed);
        // Keep the seed's identity valid: its proof binds the discovered address,
        // but the target must independently prove the expected group as well.
        let error = runtime
            .send::<_, Value>(&group, CATALOG_PATH, &command)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("different group"));
        leader_peer.wrong_group.store(false, Ordering::Relaxed);
        leader_peer.unavailable.store(true, Ordering::Relaxed);
        let before = leader_peer.calls.load(Ordering::Relaxed);
        assert!(
            runtime
                .send::<_, Value>(&group, CATALOG_PATH, &command)
                .await
                .is_err()
        );
        assert_eq!(
            leader_peer.calls.load(Ordering::Relaxed),
            before + 1,
            "a discovered peer is attempted once and never recursively resolves another hint"
        );
        consensus.shutdown().await.unwrap();
        leader_task.abort();
        seed_task.abort();
        let _ = leader_task.await;
        let _ = seed_task.await;
    }
}
