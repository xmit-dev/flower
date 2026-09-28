//! Ephemeral watches with shared authorized producers and bounded subscribers.
pub(super) mod hubs;
mod json_patch;
#[cfg(test)]
mod tests;
mod wakes;

use std::{
    convert::Infallible,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    Json,
    body::{Body, Bytes},
    extract::State,
    http::{HeaderValue, StatusCode, header},
    response::Response,
};
use futures_util::stream;
use serde_json::{Value, json};
use tokio::sync::{mpsc, oneshot, watch as notifications};

use super::{
    ApiError, App, Validity, admission, authorization, clock, invalid, query_snapshot, unavailable,
};
use crate::consensus::Progress;
use crate::evaluator::MethodKind;
use hubs::{Authorized, Frame, Hub, Subscriber};

fn query_timeout(app: &App) -> Result<Duration, ApiError> {
    super::tuning::watch_timeout(
        app.consensus.limits().read_timeout,
        crate::evaluator::config::settings()
            .map_err(unavailable)?
            .evaluation_timeout,
    )
    .map_err(unavailable)
}

fn failure(code: &'static str, message: &str) -> ApiError {
    ApiError::new(StatusCode::SERVICE_UNAVAILABLE, code, message.into())
}

fn error_event(error: ApiError) -> Bytes {
    let mut body = error.body();
    body["error"]["status"] = json!(error.status.as_u16());
    Bytes::from(format!("event: error\ndata: {body}\n\n"))
}

fn ensure_running(progress: &Progress) -> Result<(), ApiError> {
    if !progress.running {
        return Err(failure(
            "UNAVAILABLE",
            "watch replica stopped; reconnect for a fresh snapshot",
        ));
    }
    Ok(())
}

// A queued item owns its frame's retention reservation. Encoded Bytes and the
// immutable value are shared with other subscribers, without per-client diffs.
struct Wire {
    bytes: Bytes,
    _frame: Arc<Frame>,
}

impl AsRef<[u8]> for Wire {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn wire(frame: Arc<Frame>, previous: Option<u64>) -> Option<Wire> {
    frame.bytes_after(previous).map(|bytes| Wire {
        bytes,
        _frame: frame,
    })
}

/// A subscriber's current frame, and when it must look again with no Raft event.
struct Refreshed {
    hub: Arc<Hub>,
    frame: Arc<Frame>,
    wake: Option<tokio::time::Instant>,
    /// The revision access was decided at.
    access: u64,
}

/// Idle watches have no timer. Commits that write what a result or its
/// access decision read raise their signals, and freshness doubts every
/// watch's; time changes either only at the instant its evaluation declared.
/// Only a bare ctx.now() read makes a watch poll.
fn wake_at(
    app: &App,
    validity: Validity,
    committed: u64,
) -> Result<Option<tokio::time::Instant>, ApiError> {
    Ok(match validity {
        Validity::Stable => None,
        Validity::Polled => Some(
            tokio::time::Instant::now()
                + super::tuning::settings()
                    .map_err(unavailable)?
                    .watch_refresh,
        ),
        Validity::Until(time) => {
            let now = app.clock.sample_after(committed).map_err(unavailable)?;
            // One more millisecond: the clock samples whole milliseconds.
            let delay = Duration::from_millis(time.saturating_sub(now).saturating_add(1));
            tokio::time::Instant::now().checked_add(delay)
        }
    })
}

async fn refresh(
    app: &App,
    input: &Value,
    expected_scope: Option<&str>,
    joined: Option<Instant>,
    subscriber: Option<&Subscriber>,
) -> Result<Refreshed, ApiError> {
    let mut coalesce = true;
    loop {
        // Every subscriber independently enters admission before capturing a
        // root, proves its consistency fence, and authorizes on every wake.
        let (state, method, permit) = query_snapshot(app, input, Some(MethodKind::Query)).await?;
        let authorization::Access {
            principal,
            validity: access,
            observed,
        } = authorization::authorize_watch(app, &state, input, &permit).await?;
        let decided = state.revision;
        if let Some(subscriber) = subscriber {
            subscriber.observe(decided, observed.as_deref());
        }
        let committed = clock::committed(&state);
        let authorized = Authorized {
            state,
            method,
            principal,
            permit,
            input: json!({"name":input["name"],"args":input.get("args").unwrap_or(&Value::Null)}),
        };
        let scope = authorized.scope();
        if expected_scope.is_some_and(|expected| expected != scope) {
            return Err(failure(
                "WATCH_SCOPE_CHANGED",
                "watch deployment, policy, or principal changed; reconnect for a fresh snapshot",
            ));
        }
        let hub = app.watch_hubs.get(app, &scope)?;
        if let Some(frame) = hub.refresh(app, authorized, joined, coalesce).await? {
            let wake = wake_at(app, frame.query.validity.and(access), committed)?;
            return Ok(Refreshed {
                hub,
                frame,
                wake,
                access: decided,
            });
        }
        coalesce = false;
        // A concurrently admitted subscriber advanced beyond our policy view.
        // Retry under the enclosing deadline rather than serving its result.
    }
}

pub(super) async fn watch(
    State(app): State<Arc<App>>,
    Json(input): Json<Value>,
) -> Result<Response, ApiError> {
    crate::evaluator::validate_invocation(&input, "query").map_err(invalid)?;
    if input.get("expectedRevision").is_some() {
        return Err(invalid(anyhow::anyhow!(
            "expectedRevision applies only to mutation methods"
        )));
    }
    let retained = app.watch_hubs.retain(admission::input_bytes(&input))?;
    let everything = app.watch_hubs.everything(&app);
    let progress = app.consensus.progress();
    let subscriber = app.watch_hubs.subscriber();
    let Refreshed {
        hub,
        frame,
        wake,
        access,
    } = tokio::time::timeout(
        query_timeout(&app)?,
        refresh(&app, &input, None, Some(Instant::now()), Some(&subscriber)),
    )
    .await
    .map_err(|_| failure("UNAVAILABLE", "initial watch query timed out"))??;
    ensure_running(&progress.borrow())?;
    let sequence = frame.sequence;
    let revision = frame.query.revision;
    let (sender, receiver) = mpsc::channel(1);
    sender
        .try_send(wire(frame, None).expect("initial snapshot exists"))
        .unwrap_or_else(|_| unreachable!("empty watch queue"));
    let (terminal, terminal_receiver) = oneshot::channel();
    let draining = app.consensus.draining();
    tokio::spawn(async move {
        let _retained = retained;
        let watched = Watched {
            sequence,
            revision,
            access,
            wake,
        };
        let subscribed = (subscriber, everything, progress);
        if let Err(error) = produce(app, input, hub, watched, subscribed, &sender).await {
            let _ = terminal.send(error_event(error));
        }
    });
    let mut response = Response::new(Body::from_stream(events(
        receiver,
        terminal_receiver,
        draining,
    )));
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/event-stream"),
    );
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    response
        .headers_mut()
        .insert("x-accel-buffering", HeaderValue::from_static("no"));
    Ok(response)
}

fn events(
    receiver: mpsc::Receiver<Wire>,
    terminal: oneshot::Receiver<Bytes>,
    draining: tokio::sync::watch::Receiver<bool>,
) -> impl futures_util::Stream<Item = Result<Bytes, Infallible>> {
    struct StreamBody {
        receiver: mpsc::Receiver<Wire>,
        terminal: oneshot::Receiver<Bytes>,
        draining: tokio::sync::watch::Receiver<bool>,
        heartbeat: tokio::time::Interval,
        first: bool,
        finished: bool,
    }
    let interval = super::tuning::settings()
        .expect("validated SSE heartbeat")
        .watch_keepalive;
    let mut heartbeat = tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    stream::unfold(
        StreamBody {
            receiver,
            terminal,
            draining,
            heartbeat,
            first: true,
            finished: false,
        },
        |mut body| async move {
            if body.finished {
                return None;
            }
            let bytes = if body.first {
                body.first = false;
                Bytes::from_owner(body.receiver.recv().await?)
            } else {
                tokio::select! {
                    biased;
                    terminal = &mut body.terminal => {
                        body.finished = true;
                        terminal.unwrap_or_else(|_| error_event(failure("UNAVAILABLE", "watch producer stopped")))
                    }
                    // A shutdown drain waits for every response to end.
                    _ = body.draining.wait_for(|draining| *draining) => {
                        body.finished = true;
                        error_event(failure("UNAVAILABLE", "server is shutting down; reconnect for a fresh snapshot"))
                    }
                    event = body.receiver.recv() => Bytes::from_owner(event?),
                    _ = body.heartbeat.tick() => Bytes::from_static(b": flower\n\n"),
                }
            };
            Some((Ok(bytes), body))
        },
    )
}

async fn lapsed(lapse: &mut Option<notifications::Receiver<()>>) {
    match lapse {
        Some(receiver) => {
            if receiver.changed().await.is_err() {
                *lapse = None;
            }
        }
        None => std::future::pending().await,
    }
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

async fn reserve(
    sender: &mpsc::Sender<Wire>,
    timeout: Duration,
) -> Result<mpsc::Permit<'_, Wire>, ApiError> {
    tokio::time::timeout(timeout, sender.reserve())
        .await
        .map_err(|_| {
            ApiError::new(
                StatusCode::REQUEST_TIMEOUT,
                "WATCH_SLOW_CONSUMER",
                format!(
                    "watch consumer did not accept an update within {} ms",
                    timeout.as_millis()
                ),
            )
        })?
        .map_err(|_| failure("WATCH_CLOSED", "watch consumer disconnected"))
}

/// What a subscriber last refreshed: its sequence, the revisions of that
/// result and of its access decision, and when time next changes either.
struct Watched {
    sequence: u64,
    revision: u64,
    access: u64,
    wake: Option<tokio::time::Instant>,
}

/// A subscriber's own wakes, those of every watch, and Raft's state.
type Subscribed = (
    Subscriber,
    notifications::Receiver<u64>,
    notifications::Receiver<Progress>,
);

async fn produce(
    app: Arc<App>,
    input: Value,
    hub: Arc<Hub>,
    watched: Watched,
    (subscriber, mut everything, progress): Subscribed,
    sender: &mpsc::Sender<Wire>,
) -> Result<(), ApiError> {
    let settings = super::tuning::settings().map_err(unavailable)?;
    let Watched {
        mut sequence,
        mut revision,
        mut access,
        mut wake,
    } = watched;
    // Only a write the result or its access decision observed raises these
    // signals, or any write when that is not known: unrelated commits cost
    // nothing.
    let mut signal = hub.signal();
    let mut access_signal = subscriber.signal();
    let mut lapse = None;
    loop {
        if lapse.is_none() {
            lapse = app.partition_gate.as_ref().map(|gate| gate.lapses());
        }
        let mut written = false;
        tokio::select! {
            biased;
            _ = sender.closed() => return Ok(()),
            changed = everything.changed() => {
                changed.map_err(|_| failure("UNAVAILABLE", "Raft notifications stopped"))?;
            }
            woken = signal.wait_for(|woken| *woken > revision) => {
                woken.map_err(|_| failure("UNAVAILABLE", "watch notifications stopped"))?;
                written = true;
            }
            woken = access_signal.wait_for(|woken| *woken > access) => {
                woken.map_err(|_| failure("UNAVAILABLE", "watch notifications stopped"))?;
            }
            _ = sleep_until(wake) => {},
            // A partition's owner lost its claim: the next refresh fails its gate.
            _ = lapsed(&mut lapse) => {},
        };
        // A write to what the result read refreshes it no more often than its
        // cost allows (FLOWER_WATCH_DUTY_PERCENT of the time), so that a result
        // that reads much, watched while what it read keeps changing, does not
        // keep a worker busy; later writes join the refresh that follows.
        // Access, code and time changes refresh at once.
        if written && let Some(until) = hub.paced_until() {
            tokio::select! {
                biased;
                _ = sender.closed() => return Ok(()),
                _ = tokio::time::sleep_until(until) => {}
            }
        }
        everything.borrow_and_update();
        ensure_running(&progress.borrow())?;
        let mut capacity = None;
        loop {
            let refreshed = tokio::select! {
                biased;
                _ = sender.closed() => return Ok(()),
                result = tokio::time::timeout(query_timeout(&app)?, refresh(&app, &input, Some(&hub.scope), None, Some(&subscriber))) =>
                    result.map_err(|_| failure("UNAVAILABLE", "watch query timed out"))??,
            };
            wake = refreshed.wake;
            access = refreshed.access;
            let frame = refreshed.frame;
            revision = frame.query.revision;
            ensure_running(&progress.borrow())?;
            if frame.sequence <= sequence {
                break;
            }
            let permit = match capacity.take() {
                Some(permit) => permit,
                None => match sender.try_reserve() {
                    Ok(permit) => permit,
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        // Batch commits while the transport is backpressured.
                        // Keep no pending value or admission slot: once writable,
                        // authorize and evaluate the latest state, not this frame.
                        drop(frame);
                        capacity = Some(reserve(sender, settings.watch_send_timeout).await?);
                        continue;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
                },
            };
            let next_sequence = frame.sequence;
            permit.send(wire(frame, Some(sequence)).expect("watch sequence advanced"));
            sequence = next_sequence;
            break;
        }
    }
}
