//! `FlowerAdmin`: the operator endpoints Trinity uses (`sdk/client.ts`'s `FlowerAdmin` subset, plus
//! `GET /raft/metrics` and waiting for a leader as `scripts/init.ts` does), and a generic call for
//! the rest. Admin calls send `authorization: Bearer <admin token>` and never retry.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http::Method;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::time::Instant;

use crate::bundle::Bundle;
use crate::client::{
    self, DEFAULT_URL, MutationResult, QueryResult, new_request_id, partition_path,
};
use crate::error::FlowerError;
use crate::json;
use crate::transport::{HttpTransport, Lane, Transport, TransportOptions};

/// `preparation` of a deployment.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Preparation {
    /// Preserves writes but may conflict (default on the server).
    Online,
    /// Guarantees an exclusive window.
    Blocking,
}

/// Options of [`FlowerAdmin::deploy`].
#[derive(Clone, Debug, Default)]
pub struct DeployOptions {
    /// A random UUID when absent. Keep it across uncertain retries.
    pub request_id: Option<String>,
    pub preparation: Option<Preparation>,
}

/// Retention control actions (`RetentionAction`); `operation` comes first on the wire.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case")]
pub enum RetentionAction {
    Initialize {
        database: String,
        incarnation: String,
        max_receipt_bytes: Option<u64>,
    },
    Advance {
        incarnation: String,
        current_epoch: u64,
        min_epoch: u64,
    },
    Collect {
        incarnation: String,
        limit: u64,
    },
    SetBudget {
        incarnation: String,
        max_receipt_bytes: Option<u64>,
    },
    Rotate {
        incarnation: String,
        epoch_ms: Option<u64>,
        keep_epochs: u64,
    },
    Reincarnate {
        incarnation: String,
        new_incarnation: String,
        fence_attestation: String,
    },
}

/// `RetentionState.rotation`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Rotation {
    pub epoch_ms: u64,
    pub keep_epochs: u64,
}

/// `RetentionState`; fields Trinity reads are typed, everything is kept in `extra`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetentionState {
    pub database: String,
    pub incarnation: String,
    pub current_epoch: u64,
    pub min_epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub epoch_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotation: Option<Rotation>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

/// One managed key's metadata.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManagedKey {
    pub id: String,
    pub algorithm: String,
    pub active_version: u64,
    #[serde(default)]
    pub versions: Vec<Value>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

/// A binding of a managed key to usages.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KeyBinding {
    pub key: String,
    pub usages: Vec<String>,
}

/// `ManagedKeyCatalog`: operator metadata, never private material.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ManagedKeyCatalog {
    #[serde(default)]
    pub domain: Option<String>,
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub keys: BTreeMap<String, ManagedKey>,
    #[serde(default)]
    pub bindings: BTreeMap<String, KeyBinding>,
}

/// Operator endpoints. Cheap to clone.
#[derive(Clone)]
pub struct FlowerAdmin {
    root: String,
    url: String,
    token: Option<String>,
    transport: Arc<dyn Transport>,
}

impl fmt::Debug for FlowerAdmin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FlowerAdmin")
            .field("url", &self.url)
            .field("token", &self.token.as_ref().map(|_| ".."))
            .finish()
    }
}

/// Builds a [`FlowerAdmin`].
#[must_use]
pub struct AdminBuilder {
    url: String,
    token: Option<String>,
    transport: Option<Arc<dyn Transport>>,
    options: TransportOptions,
}

impl AdminBuilder {
    pub fn admin_token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    pub fn transport(mut self, transport: Arc<dyn Transport>) -> Self {
        self.transport = Some(transport);
        self
    }

    pub fn transport_options(mut self, options: TransportOptions) -> Self {
        self.options = options;
        self
    }

    pub fn build(self) -> Result<FlowerAdmin, FlowerError> {
        let url = client::http_url(&self.url, "Flower URL")?;
        let transport = match self.transport {
            Some(transport) => transport,
            None => Arc::new(HttpTransport::new(self.options)?),
        };
        Ok(FlowerAdmin {
            root: url.clone(),
            url,
            token: self.token,
            transport,
        })
    }
}

#[derive(Serialize)]
struct Deploy<'a> {
    #[serde(rename = "requestId")]
    request_id: &'a str,
    bundle: &'a Bundle,
    #[serde(skip_serializing_if = "Option::is_none")]
    preparation: Option<Preparation>,
}

#[derive(Serialize)]
struct KeyRequest<'a> {
    operation: &'a str,
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    algorithm: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    key: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usages: Option<&'a [&'a str]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    bits: Option<u32>,
    #[serde(rename = "requestId")]
    request_id: &'a str,
}

#[derive(Serialize)]
struct Operation<'a> {
    operation: &'a str,
}

#[derive(Serialize)]
struct ControlRetention<'a> {
    expected_revision: u64,
    action: &'a RetentionAction,
}

impl FlowerAdmin {
    pub fn builder(url: impl Into<String>) -> AdminBuilder {
        AdminBuilder {
            url: url.into(),
            token: None,
            transport: None,
            options: TransportOptions::default(),
        }
    }

    /// An admin client with default transport options.
    pub fn new(url: impl Into<String>, admin_token: Option<String>) -> Result<Self, FlowerError> {
        let mut builder = Self::builder(url);
        builder.token = admin_token;
        builder.build()
    }

    /// `http://127.0.0.1:7101` with this token.
    pub fn local(admin_token: impl Into<String>) -> Result<Self, FlowerError> {
        Self::builder(DEFAULT_URL).admin_token(admin_token).build()
    }

    /// The base URL, including any partition path.
    pub fn url(&self) -> &str {
        &self.url
    }

    pub fn transport(&self) -> &Arc<dyn Transport> {
        &self.transport
    }

    /// The same operations against one named logical database.
    ///
    /// # Panics
    /// When the name is invalid; see [`FlowerAdmin::try_partition`].
    pub fn partition(&self, name: &str) -> FlowerAdmin {
        self.try_partition(name)
            .unwrap_or_else(|error| panic!("{error}"))
    }

    pub fn try_partition(&self, name: &str) -> Result<FlowerAdmin, FlowerError> {
        Ok(FlowerAdmin {
            url: format!("{}{}", self.url, partition_path(name)?),
            ..self.clone()
        })
    }

    /// POST an admin JSON body to `path` under this admin's URL (partition-scoped when scoped).
    pub async fn admin<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, FlowerError> {
        let body = Bytes::from(json::to_js_vec(body)?);
        self.send(Method::POST, format!("{}{path}", self.url), body)
            .await
    }

    /// GET `path` under the root URL (never partition-scoped), with the admin token.
    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, FlowerError> {
        self.send(Method::GET, format!("{}{path}", self.root), Bytes::new())
            .await
    }

    async fn send<T: DeserializeOwned>(
        &self,
        method: Method,
        url: String,
        body: Bytes,
    ) -> Result<T, FlowerError> {
        let request = client::request(method, url, body, Lane::Unary, self.token.as_deref())?;
        let response = self.transport.send(request).await?;
        client::decode_reply(response).await
    }

    /// `POST /raft/initialize` with `{"<id>": "<host:port>"}`.
    pub async fn initialize(&self, members: &BTreeMap<String, String>) -> Result<(), FlowerError> {
        let _: Value = self.admin("/raft/initialize", members).await?;
        Ok(())
    }

    /// `POST /admin/deploy {requestId, bundle, preparation?}`.
    pub async fn deploy(
        &self,
        bundle: &Bundle,
        options: DeployOptions,
    ) -> Result<MutationResult<Value>, FlowerError> {
        let request_id = options.request_id.unwrap_or_else(new_request_id);
        self.admin(
            "/admin/deploy",
            &Deploy {
                request_id: &request_id,
                bundle,
                preparation: options.preparation,
            },
        )
        .await
    }

    /// Operator key metadata.
    pub async fn key_list(&self) -> Result<MutationResult<ManagedKeyCatalog>, FlowerError> {
        self.admin("/admin/keys", &Operation { operation: "list" })
            .await
    }

    /// Generate a managed key (`bits` only for RSA).
    pub async fn key_generate(
        &self,
        name: &str,
        algorithm: &str,
        bits: Option<u32>,
        request_id: Option<&str>,
    ) -> Result<MutationResult<ManagedKeyCatalog>, FlowerError> {
        let request_id = request_id.map(str::to_owned).unwrap_or_else(new_request_id);
        self.admin(
            "/admin/keys",
            &KeyRequest {
                operation: "generate",
                name,
                algorithm: Some(algorithm),
                key: None,
                usages: None,
                bits,
                request_id: &request_id,
            },
        )
        .await
    }

    /// Bind `key` to the binding `name` with `usages`.
    pub async fn key_bind(
        &self,
        name: &str,
        key: &str,
        usages: &[&str],
        request_id: Option<&str>,
    ) -> Result<MutationResult<ManagedKeyCatalog>, FlowerError> {
        let request_id = request_id.map(str::to_owned).unwrap_or_else(new_request_id);
        self.admin(
            "/admin/keys",
            &KeyRequest {
                operation: "bind",
                name,
                algorithm: None,
                key: Some(key),
                usages: Some(usages),
                bits: None,
                request_id: &request_id,
            },
        )
        .await
    }

    /// `{operation: "status"}`: `value` is null before retention is initialized.
    pub async fn retention_status(
        &self,
    ) -> Result<QueryResult<Option<RetentionState>>, FlowerError> {
        self.admin(
            "/admin/retention",
            &Operation {
                operation: "status",
            },
        )
        .await
    }

    /// Revision-conditional control; the value is `{state, collected}`.
    pub async fn control_retention(
        &self,
        expected_revision: u64,
        action: &RetentionAction,
    ) -> Result<MutationResult<Value>, FlowerError> {
        self.admin(
            "/admin/retention",
            &ControlRetention {
                expected_revision,
                action,
            },
        )
        .await
    }

    /// `GET /raft/metrics`: `{state, current_leader, membership_config, ...}`.
    pub async fn raft_metrics(&self) -> Result<Value, FlowerError> {
        self.get("/raft/metrics").await
    }

    /// Poll `/raft/metrics` every `interval` until `state` is `"Leader"`, returning those metrics.
    /// Transient errors (a node still starting) are retried until `timeout`; others fail at once.
    pub async fn wait_for_leader(
        &self,
        timeout: Duration,
        interval: Duration,
    ) -> Result<Value, FlowerError> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.raft_metrics().await {
                Ok(metrics) if metrics.get("state").and_then(Value::as_str) == Some("Leader") => {
                    return Ok(metrics);
                }
                Ok(_) => {}
                Err(error) if error.is_transient() && Instant::now() < deadline => {}
                Err(error) => return Err(error),
            }
            if Instant::now() + interval > deadline {
                return Err(FlowerError::new(
                    format!(
                        "The node did not become leader within {} seconds",
                        timeout.as_secs()
                    ),
                    0,
                    "LEADER_WAIT_TIMEOUT",
                ));
            }
            tokio::time::sleep(interval).await;
        }
    }
}

/// Whether raft metrics show no membership yet (`scripts/init.ts`'s check before `initialize`).
pub fn uninitialized(metrics: &Value) -> bool {
    metrics
        .pointer("/membership_config/membership/configs")
        .and_then(Value::as_array)
        .is_some_and(Vec::is_empty)
}
