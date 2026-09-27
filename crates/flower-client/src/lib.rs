//! A client for Flower, the Rust twin of `@flower-js/sdk`'s client, watch, admin and bundle
//! modules (the queue worker lives in `flower-worker`). See `API.md` for a summary and
//! `DEVIATIONS.md` for where it differs from the TypeScript.
//!
//! ```no_run
//! # async fn demo() -> Result<(), flower_client::FlowerError> {
//! use flower_client::{Credentials, FlowerClient, MutationOptions, RequestOptions};
//! use serde_json::{json, Value};
//!
//! let client = FlowerClient::builder("http://127.0.0.1:7101")
//!     .credentials(Credentials::token("..."))
//!     .partition("acme")
//!     .build()?;
//! let count = client.query::<_, u64>("count", &(), RequestOptions::retrying()).await?;
//! let added = client
//!     .mutate::<_, Value>("add", &json!({"by": 1}), MutationOptions::retrying().request_id("add:1"))
//!     .await?;
//! # let _ = (count, added);
//! # Ok(()) }
//! ```

pub mod admin;
pub mod bundle;
pub mod client;
pub mod error;
pub mod json;
pub mod retry;
pub mod sse;
pub mod testing;
pub mod transport;
pub mod watch;

pub use admin::{
    AdminBuilder, DeployOptions, FlowerAdmin, ManagedKeyCatalog, Preparation, RetentionAction,
    RetentionState,
};
pub use bundle::{Bundle, BundleOptions, Initialization, JavaScriptBundle, build_bundle};
pub use client::{
    ClientBuilder, CredentialProvider, Credentials, DEFAULT_URL, FlowerClient, MutationOptions,
    MutationResult, QueryResult, RequestOptions, new_request_id,
};
pub use error::{ErrorKind, Failure, FlowerError};
pub use json::canonical_json;
pub use retry::{Retry, RetryPolicy, backoff};
pub use transport::{
    HttpRequest, HttpResponse, HttpTransport, Lane, Protocol, ResponseBody, Transport,
    TransportOptions,
};
pub use watch::{
    PatchOperation, Reconnect, SubscribeOptions, Subscription, Update, WatchBudgets, WatchDelta,
    WatchOptions,
};

/// Re-exported so callers can cancel without depending on tokio-util themselves.
pub use tokio_util::sync::CancellationToken;
