use super::{Config, State};
use anyhow::{Context, Result, bail};
use futures_util::{StreamExt, stream::FuturesUnordered};
use serde_json::{Value, json};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

#[derive(Debug)]
pub struct CallError {
    pub message: String,
    pub code: String,
}
#[derive(Clone, Debug, PartialEq)]
pub struct Response {
    pub revision: u64,
    pub value: Value,
    pub duplicate: bool,
}
pub struct Client {
    pub config: Config,
    pub state: Mutex<State>,
    method: reqwest::Client,
    admin: reqwest::Client,
    discovery: tokio::sync::Mutex<()>,
    route: Mutex<Routing>,
}
struct Routing {
    leader: usize,
    discovery_checked: Option<Instant>,
    cursor: usize,
    cooldown: Vec<Option<Instant>>,
    nodes: Vec<Route>,
}
#[derive(Default)]
struct Route {
    attempts: u64,
    completed: u64,
    failures: u64,
}
impl Client {
    pub fn new(config: Config) -> Result<Arc<Self>> {
        let mut method = reqwest::Client::builder().pool_idle_timeout(Duration::from_secs(30));
        if config.http2 {
            method = method.http2_prior_knowledge();
        } else {
            method = method.http1_only();
        }
        let leader = config
            .members
            .iter()
            .position(|member| member.id == config.leader_id)
            .context("Unknown initial leader")?;
        let size = config.members.len();
        Ok(Arc::new(Self {
            config,
            state: Mutex::new(State::default()),
            method: method.build()?,
            admin: reqwest::Client::builder().http1_only().build()?,
            discovery: tokio::sync::Mutex::new(()),
            route: Mutex::new(Routing {
                leader,
                discovery_checked: None,
                cursor: 0,
                cooldown: vec![None; size],
                nodes: (0..size).map(|_| Route::default()).collect(),
            }),
        }))
    }
    pub fn next_id(&self) -> String {
        let mut state = self.state.lock().unwrap();
        state.sequence += 1;
        format!("rust-{}", state.sequence)
    }
    pub fn routing(&self) -> Value {
        let route = self.route.lock().unwrap();
        json!({"mode": self.config.query_routing, "consistency": self.config.read_consistency, "auditConsistency": "fresh",
            "nodes": self.config.members.iter().zip(&route.nodes).map(|(member, counts)| json!({
                "id": member.id, "url": member.url, "attempts": counts.attempts, "completed": counts.completed, "failures": counts.failures,
            })).collect::<Vec<_>>()})
    }
    // Establish one multiplexed connection per replica before the synchronized
    // measured start. These harmless preview requests are reported separately.
    pub async fn warm_connections(&self) -> Result<()> {
        for member in &self.config.members {
            let response = self.method.post(format!("{}/v1/query", member.url))
                .timeout(Duration::from_millis(self.config.request_timeout_ms))
                .json(&json!({"name": self.read_method(), "args": [&self.config.tenant_ids[0], "store-0"]})).send().await?;
            if !response.status().is_success() {
                bail!("Driver warmup HTTP {}", response.status());
            }
            let value: Value = response.json().await?;
            if value["revision"].as_u64().is_none() || value.get("value").is_none() {
                bail!("Malformed warmup response");
            }
        }
        Ok(())
    }
    pub fn read_method(&self) -> &'static str {
        if self.config.read_consistency == "replica-local" {
            "pizza.shop.local"
        } else {
            "pizza.shop"
        }
    }
    fn target(&self, distributed: bool, excluded: &BTreeSet<usize>) -> Option<usize> {
        let mut route = self.route.lock().unwrap();
        if !distributed {
            return Some(route.leader);
        }
        let now = Instant::now();
        for offset in 0..self.config.members.len() {
            let index = (route.cursor + offset) % self.config.members.len();
            if excluded.contains(&index) || route.cooldown[index].is_some_and(|until| until > now) {
                continue;
            }
            route.cursor = (index + 1) % self.config.members.len();
            return Some(index);
        }
        None
    }
    async fn discover(&self, attempted: usize, deadline: Instant) {
        let Ok(_guard) = tokio::time::timeout(
            deadline.saturating_duration_since(Instant::now()),
            self.discovery.lock(),
        )
        .await
        else {
            return;
        };
        {
            let mut route = self.route.lock().unwrap();
            // Coalesce failed discovery rounds too: hundreds of callers waiting
            // on the same election must not issue hundreds of metrics rounds.
            if route.leader != attempted
                || route
                    .discovery_checked
                    .is_some_and(|last| last.elapsed() < Duration::from_millis(25))
            {
                return;
            }
            route.discovery_checked = Some(Instant::now());
        }
        let timeout = Duration::from_millis(self.config.request_timeout_ms)
            .min(deadline.saturating_duration_since(Instant::now()));
        if timeout.is_zero() {
            return;
        }
        let mut pending = FuturesUnordered::new();
        for (index, member) in self.config.members.iter().enumerate() {
            pending.push(async move {
                let response = self
                    .admin
                    .get(format!("{}/raft/metrics", member.url))
                    .bearer_auth(&self.config.admin_token)
                    .timeout(timeout)
                    .send()
                    .await
                    .ok()?;
                if !response.status().is_success() {
                    return None;
                }
                let value: Value = response.json().await.ok()?;
                (value["state"] == "Leader" && value["current_leader"].as_u64() == Some(member.id))
                    .then_some(index)
            });
        }
        while let Some(result) = pending.next().await {
            if let Some(index) = result {
                let mut route = self.route.lock().unwrap();
                route.leader = index;
                route.discovery_checked = Some(Instant::now());
                return;
            }
        }
        self.route.lock().unwrap().discovery_checked = Some(Instant::now());
    }
    pub async fn call(
        &self,
        name: &str,
        args: Value,
        id: Option<&str>,
        replay: bool,
    ) -> Result<Response, CallError> {
        self.call_at(name, args, id, replay, None).await
    }
    pub async fn call_at(
        &self,
        name: &str,
        args: Value,
        id: Option<&str>,
        replay: bool,
        arrival: Option<Instant>,
    ) -> Result<Response, CallError> {
        let started = Instant::now();
        let logical_started = arrival.unwrap_or(started);
        let query = id.is_none();
        let body = if let Some(id) = id {
            json!({"name":name,"args":args,"requestId":id})
        } else {
            json!({"name":name,"args":args})
        };
        // Keep the exact serialized bytes across all uncertain retries.
        let serialized = serde_json::to_string(&body).unwrap();
        let result = self.request(name, &serialized, query, started).await;
        self.state.lock().unwrap().stats.operation(
            &format!("{name}{}", if replay { ".replay" } else { "" }),
            logical_started.elapsed().as_secs_f64() * 1000.0,
            result.is_ok(),
            result.as_ref().is_ok_and(|value| value.duplicate),
        );
        result
    }
    async fn request(
        &self,
        name: &str,
        body: &str,
        query: bool,
        started: Instant,
    ) -> Result<Response, CallError> {
        let deadline = started + Duration::from_millis(self.config.retry_budget_ms);
        let distributed = query && self.config.query_routing == "replicas";
        let mut excluded = BTreeSet::new();
        let mut attempts = 0_u64;
        let mut last_error = String::from("No reachable endpoint");
        while Instant::now() < deadline {
            let Some(index) = self.target(distributed, &excluded) else {
                pause((25 * attempts).clamp(25, 250), deadline).await;
                excluded.clear();
                continue;
            };
            let attempted_leader = self.route.lock().unwrap().leader;
            let attempt_started = Instant::now();
            let timeout = Duration::from_millis(self.config.request_timeout_ms)
                .min(deadline.saturating_duration_since(attempt_started));
            let path = if query { "/v1/query" } else { "/v1/call" };
            let request = async {
                let response = self
                    .method
                    .post(format!("{}{}", self.config.members[index].url, path))
                    .header("content-type", "application/json")
                    .body(body.to_owned())
                    .send()
                    .await?;
                let status = response.status().as_u16();
                let bytes = response.bytes().await?;
                Ok::<_, reqwest::Error>((status, serde_json::from_slice::<Value>(&bytes).ok()))
            };
            let mut status = "network".to_owned();
            let mut retryable = true;
            let mut code = "NETWORK".to_owned();
            let mut response = None;
            match tokio::time::timeout(timeout, request).await {
                Err(_) => {
                    status = "timeout".into();
                    last_error = "Request timed out".into();
                }
                Ok(Err(error)) => {
                    last_error = error.to_string();
                }
                Ok(Ok((http, value))) => {
                    status = format!("HTTP_{http}");
                    let mut value = value.unwrap_or(Value::Null);
                    if (200..300).contains(&http) {
                        let revision = value["revision"]
                            .as_u64()
                            .filter(|revision| *revision <= 9_007_199_254_740_991);
                        let duplicate = value["duplicate"] == true;
                        if let (Some(revision), Some(result)) = (revision, value.get_mut("value")) {
                            response = Some(Response {
                                revision,
                                value: result.take(),
                                duplicate,
                            });
                        } else {
                            retryable = false;
                            code = "INVALID_RESPONSE".into();
                            last_error = "Malformed successful Flower response".into();
                        }
                    } else {
                        retryable = [502, 503, 504].contains(&http);
                        last_error = value["error"]["message"]
                            .as_str()
                            .unwrap_or("HTTP request failed")
                            .to_owned();
                        code = value["error"]["code"]
                            .as_str()
                            .unwrap_or("HTTP_ERROR")
                            .to_owned();
                    }
                }
            }
            let ok = response.is_some();
            self.state.lock().unwrap().stats.attempt(
                name,
                attempt_started.elapsed().as_secs_f64() * 1000.0,
                ok,
                &status,
                attempts > 0,
                response.as_ref().is_some_and(|result| result.duplicate),
            );
            if query {
                let mut route = self.route.lock().unwrap();
                route.nodes[index].attempts += 1;
                if ok {
                    route.nodes[index].completed += 1;
                } else {
                    route.nodes[index].failures += 1;
                }
            }
            attempts += 1;
            if let Some(value) = response {
                return Ok(value);
            }
            if !retryable {
                return Err(CallError {
                    message: last_error,
                    code,
                });
            }
            if distributed {
                excluded.insert(index);
                self.route.lock().unwrap().cooldown[index] =
                    Some(Instant::now() + Duration::from_millis((25 * attempts).min(250)));
            } else {
                self.discover(attempted_leader, deadline).await;
                if self.route.lock().unwrap().leader == attempted_leader {
                    pause((25 * attempts).min(250), deadline).await;
                }
            }
        }
        Err(CallError {
            message: format!("Retry budget exhausted for {name}: {last_error}"),
            code: "RETRY_EXHAUSTED".into(),
        })
    }
}
async fn pause(ms: u64, deadline: Instant) {
    tokio::time::sleep(
        Duration::from_millis(ms).min(deadline.saturating_duration_since(Instant::now())),
    )
    .await;
}
