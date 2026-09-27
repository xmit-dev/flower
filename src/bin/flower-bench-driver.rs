//! Optional benchmark customer driver. Node retains setup, workers, faults and
//! the independent audit. Stdin contains one private config line, then a common
//! start/deadline line. Stdout contains ready, disjoint metric intervals, and done.
#[path = "bench_driver/arrival.rs"]
mod arrival;
#[path = "bench_driver/client.rs"]
mod client;
#[path = "bench_driver/metrics.rs"]
mod metrics;

use anyhow::{Context, Result, ensure};
use client::{CallError, Client, Response};
use metrics::{Random, Stats};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io::{BufRead, Write},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Member {
    id: u64,
    url: String,
}
#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Config {
    schema_version: u32,
    members: Vec<Member>,
    leader_id: u64,
    admin_token: String,
    concurrency: usize,
    #[serde(default)]
    offered_rate: f64,
    tenant_ids: Vec<String>,
    shops: usize,
    hot_shops: usize,
    hot_probability: f64,
    max_orders: u64,
    duplicate_rate: f64,
    poll_ms: u64,
    request_timeout_ms: u64,
    retry_budget_ms: u64,
    http2: bool,
    query_routing: String,
    read_consistency: String,
    seed: String,
}
impl Config {
    fn validate(&self) -> Result<()> {
        ensure!(self.schema_version == 1, "Unsupported driver protocol");
        ensure!(
            self.offered_rate.is_finite() && self.offered_rate >= 0.0,
            "Invalid offered rate"
        );
        ensure!(
            !self.members.is_empty() && !self.tenant_ids.is_empty() && !self.admin_token.is_empty(),
            "Missing members, tenants or admin token"
        );
        ensure!(
            self.concurrency > 0
                && self.shops > 0
                && self.hot_shops > 0
                && self.hot_shops <= self.shops,
            "Invalid concurrency or shop count"
        );
        ensure!(
            self.max_orders > 0
                && self.poll_ms > 0
                && self.request_timeout_ms > 0
                && self.retry_budget_ms >= self.request_timeout_ms,
            "Invalid order or timeout settings"
        );
        ensure!(
            self.hot_probability.is_finite()
                && (0.0..=1.0).contains(&self.hot_probability)
                && self.duplicate_rate.is_finite()
                && (0.0..=1.0).contains(&self.duplicate_rate),
            "Invalid probabilities"
        );
        ensure!(
            ["replicas", "leader"].contains(&self.query_routing.as_str())
                && ["fresh", "replica-local"].contains(&self.read_consistency.as_str()),
            "Invalid read routing or policy"
        );
        let mut ids = std::collections::BTreeSet::new();
        for member in &self.members {
            let url = reqwest::Url::parse(&member.url)?;
            ensure!(
                member.id > 0
                    && ids.insert(member.id)
                    && url.scheme() == "http"
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.path() == "/"
                    && url.query().is_none(),
                "Invalid or duplicate member"
            );
        }
        ensure!(
            self.tenant_ids.iter().all(|id| !id.is_empty()),
            "Empty tenant ID"
        );
        Ok(())
    }
}
#[derive(Default)]
struct State {
    stats: Stats,
    offered: arrival::Counters,
    sequence: u64,
    issued_orders: u64,
    emitted_issued: u64,
    emitted_ms: f64,
    orders: Vec<Value>,
    tips: BTreeMap<String, u64>,
    replay_checks: u64,
    failure_count: u64,
    failures: Vec<Value>,
    retained_failures: usize,
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Start {
    r#type: String,
    deadline_unix_ms: u64,
}

fn emit(value: &Value) -> Result<()> {
    let mut output = std::io::stdout().lock();
    serde_json::to_writer(&mut output, value)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}
fn emit_interval(client: &Client, elapsed: f64, final_interval: bool) -> Result<()> {
    let mut state = client.state.lock().unwrap();
    let stats = std::mem::take(&mut state.stats);
    let offered = std::mem::take(&mut state.offered).snapshot();
    let duration_ms = elapsed - state.emitted_ms;
    state.emitted_ms = elapsed;
    let issued = state.issued_orders - state.emitted_issued;
    state.emitted_issued = state.issued_orders;
    let orders = std::mem::take(&mut state.orders);
    let tips = std::mem::take(&mut state.tips);
    let replay_checks = std::mem::take(&mut state.replay_checks);
    let failure_count = std::mem::take(&mut state.failure_count);
    let failures = std::mem::take(&mut state.failures);
    drop(state);
    emit(
        &json!({"type": if final_interval { "done" } else { "interval" }, "elapsedMs": elapsed,
        "stats": stats.snapshot(duration_ms), "offered": offered, "orders": orders, "tips": tips,
        "counters": {"issuedOrders": issued, "replayChecks": replay_checks},
        "failureCount": failure_count, "failures": failures,
        "queryRouting": final_interval.then(|| client.routing())}),
    )
}
async fn replay(
    client: &Client,
    name: &str,
    args: &Value,
    id: &str,
    original: &Response,
    random: &mut Random,
) -> Result<(), CallError> {
    if random.next() >= client.config.duplicate_rate {
        return Ok(());
    }
    let repeated = client.call(name, args.clone(), Some(id), true).await?;
    if !repeated.duplicate
        || repeated.revision != original.revision
        || repeated.value != original.value
    {
        return Err(CallError {
            message: format!("{name}: replay did not preserve receipt"),
            code: "INVALID_REPLAY".into(),
        });
    }
    client.state.lock().unwrap().replay_checks += 1;
    Ok(())
}
async fn customer(
    client: Arc<Client>,
    index: usize,
    random: &mut Random,
    arrival: Option<Instant>,
) -> bool {
    let choice = random.next();
    let shops = if random.next() < client.config.hot_probability {
        client.config.hot_shops
    } else {
        client.config.shops
    };
    let tenant =
        &client.config.tenant_ids[(random.next() * client.config.tenant_ids.len() as f64) as usize];
    let shop = json!([
        tenant,
        format!("store-{}", (random.next() * shops as f64) as usize)
    ]);
    let order = if choice < 0.30 {
        let mut state = client.state.lock().unwrap();
        if state.issued_orders < client.config.max_orders {
            state.issued_orders += 1;
            Some(state.issued_orders)
        } else {
            None
        }
    } else {
        None
    };
    let result: Result<(), CallError> = async {
            if let Some(sequence) = order {
                let args = json!({"id":format!("pizza-{sequence:06}"),"shop":shop,"quantity":1 + (random.next() * 4.0) as u64});
                let id = client.next_id();
                let response = client.call_at("pizza.order", args.clone(), Some(&id), false,arrival).await?;
                client.state.lock().unwrap().orders.push(args.clone());
                replay(&client, "pizza.order", &args, &id, &response, random).await?;
            } else if choice < 0.70 {
                client.call_at(client.read_method(), shop, None, false,arrival).await?;
            } else {
                let amount = 1 + (random.next() * 3.0) as u64;
                let key = serde_json::to_string(&shop).unwrap();
                let args = json!({"shop":shop,"amount":amount});
                let id = client.next_id();
                let response = client.call_at("pizza.tip", args.clone(), Some(&id), false,arrival).await?;
                *client.state.lock().unwrap().tips.entry(key).or_default() += amount;
                replay(&client, "pizza.tip", &args, &id, &response, random).await?;
            }
            Ok(())
        }.await;
    let success = result.is_ok();
    if let Err(error) = result {
        {
            let mut state = client.state.lock().unwrap();
            state.failure_count += 1;
            if state.retained_failures < 50 {
                state.retained_failures += 1;
                state.failures.push(json!({"context":format!("Rust customer {index}"), "message":error.message.chars().take(2000).collect::<String>(), "code":error.code}));
            }
        }
        tokio::time::sleep(Duration::from_millis(client.config.poll_ms)).await;
    }
    success
}
async fn producer(client: Arc<Client>, index: usize, deadline: Instant) {
    let mut random = Random::new(&format!("{}:customer:{index}", client.config.seed));
    while Instant::now() < deadline {
        customer(client.clone(), index, &mut random, None).await;
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let mut input = std::io::stdin().lock();
    let mut line = String::new();
    ensure!(
        input.read_line(&mut line)? > 0,
        "Missing driver configuration"
    );
    let config: Config = serde_json::from_str(&line).context("Invalid driver configuration")?;
    config.validate()?;
    let client = Client::new(config)?;
    client.warm_connections().await?;
    emit(
        &json!({"type":"ready", "schemaVersion":1, "warmupRequests":client.config.members.len(), "pid":std::process::id()}),
    )?;
    line.clear();
    ensure!(
        input.read_line(&mut line)? > 0,
        "Missing synchronized start"
    );
    let start: Start = serde_json::from_str(&line)?;
    ensure!(start.r#type == "start", "Expected synchronized start");
    drop(input);
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis();
    ensure!(
        u128::from(start.deadline_unix_ms) > now,
        "Measurement deadline already passed"
    );
    let started = Instant::now();
    let deadline = started + Duration::from_millis(start.deadline_unix_ms - now as u64);
    let mut tasks = tokio::task::JoinSet::new();
    if client.config.offered_rate > 0.0 {
        tasks.spawn(arrival::run(client.clone(), started, deadline));
    } else {
        for index in 0..client.config.concurrency {
            tasks.spawn(producer(client.clone(), index, deadline));
        }
    }
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.tick().await;
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    while !tasks.is_empty() {
        tokio::select! {
            result = tasks.join_next() => { if let Some(result) = result { result?; } },
            _ = interval.tick() => emit_interval(&client, started.elapsed().as_secs_f64() * 1000.0, false)?,
        }
    }
    emit_interval(&client, started.elapsed().as_secs_f64() * 1000.0, true)?;
    Ok(())
}
