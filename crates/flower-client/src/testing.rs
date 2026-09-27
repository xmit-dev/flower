//! A scripted in-memory [`Transport`] for tests (the Rust twin of `client.test.ts`'s `scripted`
//! fetch): replies in order, records every request, and once the script is exhausted, requests
//! hang until dropped, like an unresponsive server.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::Stream;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::error::FlowerError;
use crate::transport::{HttpRequest, HttpResponse, Lane, ResponseBody, Transport};

/// A reply body.
pub enum MockBody {
    Full(Bytes),
    Stream(MockStream),
}

/// A scripted reply.
pub struct MockReply {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: MockBody,
}

impl MockReply {
    /// A JSON reply.
    pub fn json(status: u16, value: &Value) -> Self {
        MockReply {
            status,
            headers: vec![("content-type".into(), "application/json".into())],
            body: MockBody::Full(Bytes::from(serde_json::to_vec(value).expect("JSON"))),
        }
    }

    /// `{revision: 1, value, duplicate: false}`.
    pub fn ok(value: Value) -> Self {
        Self::result(1, value, false)
    }

    pub fn result(revision: u64, value: Value, duplicate: bool) -> Self {
        Self::json(
            200,
            &json!({ "revision": revision, "value": value, "duplicate": duplicate }),
        )
    }

    /// `{error: {code, message, ...extra}}` with `status`.
    pub fn failure(status: u16, code: &str, message: &str, extra: Value) -> Self {
        let mut error = json!({ "code": code, "message": message });
        if let (Some(error), Value::Object(extra)) = (error.as_object_mut(), extra) {
            error.extend(extra);
        }
        Self::json(status, &json!({ "error": error }))
    }

    /// A body without a content type.
    pub fn text(status: u16, body: impl Into<Bytes>) -> Self {
        MockReply {
            status,
            headers: Vec::new(),
            body: MockBody::Full(body.into()),
        }
    }

    /// A complete `text/event-stream` reply.
    pub fn events(text: &str) -> Self {
        MockReply {
            status: 200,
            headers: vec![("content-type".into(), "text/event-stream".into())],
            body: MockBody::Full(Bytes::from(text.to_owned())),
        }
    }

    /// A streaming reply starting with `text`; with `open`, more can be pushed through the handle
    /// until it is closed or dropped.
    pub fn stream(
        status: u16,
        content_type: Option<&str>,
        text: &str,
        open: bool,
    ) -> (Self, StreamHandle) {
        let (stream, handle) = MockStream::new(text, open);
        let headers = content_type
            .map(|value| vec![("content-type".to_owned(), value.to_owned())])
            .unwrap_or_default();
        (
            MockReply {
                status,
                headers,
                body: MockBody::Stream(stream),
            },
            handle,
        )
    }

    /// An event stream (see [`MockReply::stream`]).
    pub fn event_stream(text: &str, open: bool) -> (Self, StreamHandle) {
        Self::stream(200, Some("text/event-stream"), text, open)
    }

    pub fn with_header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_owned(), value.to_owned()));
        self
    }
}

/// A streaming body fed through a [`StreamHandle`].
pub struct MockStream {
    receiver: mpsc::UnboundedReceiver<Result<Bytes, FlowerError>>,
    cancelled: Arc<AtomicUsize>,
    aborted: Option<Arc<AtomicBool>>,
    ended: bool,
}

type Sender = mpsc::UnboundedSender<Result<Bytes, FlowerError>>;

/// Feeds a [`MockStream`] and observes its cancellation.
#[derive(Clone)]
pub struct StreamHandle {
    sender: Arc<Mutex<Option<Sender>>>,
    cancelled: Arc<AtomicUsize>,
}

impl MockStream {
    pub fn new(text: &str, open: bool) -> (MockStream, StreamHandle) {
        let (sender, receiver) = mpsc::unbounded_channel();
        if !text.is_empty() {
            let _ = sender.send(Ok(Bytes::from(text.to_owned())));
        }
        let cancelled = Arc::new(AtomicUsize::new(0));
        let handle = StreamHandle {
            sender: Arc::new(Mutex::new(open.then_some(sender))),
            cancelled: cancelled.clone(),
        };
        (
            MockStream {
                receiver,
                cancelled,
                aborted: None,
                ended: false,
            },
            handle,
        )
    }
}

impl StreamHandle {
    /// Send more bytes (ignored once closed).
    pub fn push(&self, text: &str) {
        if let Some(sender) = self.sender.lock().unwrap().as_ref() {
            let _ = sender.send(Ok(Bytes::from(text.to_owned())));
        }
    }

    pub fn push_bytes(&self, bytes: &[u8]) {
        if let Some(sender) = self.sender.lock().unwrap().as_ref() {
            let _ = sender.send(Ok(Bytes::copy_from_slice(bytes)));
        }
    }

    /// Fail the body.
    pub fn error(&self, error: FlowerError) {
        if let Some(sender) = self.sender.lock().unwrap().take() {
            let _ = sender.send(Err(error));
        }
    }

    /// End the body cleanly.
    pub fn close(&self) {
        self.sender.lock().unwrap().take();
    }

    /// How many times the reader dropped the body before its end (TS: `cancel()` calls).
    pub fn cancelled(&self) -> usize {
        self.cancelled.load(Ordering::SeqCst)
    }
}

impl Stream for MockStream {
    type Item = Result<Bytes, FlowerError>;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.receiver.poll_recv(context) {
            Poll::Ready(None) => {
                self.ended = true;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                self.ended = true;
                Poll::Ready(Some(Err(error)))
            }
            other => other,
        }
    }
}

impl Drop for MockStream {
    fn drop(&mut self) {
        if !self.ended {
            self.cancelled.fetch_add(1, Ordering::SeqCst);
            if let Some(aborted) = &self.aborted {
                aborted.store(true, Ordering::SeqCst);
            }
        }
    }
}

type Handler =
    Box<dyn FnOnce(&HttpRequest) -> BoxFuture<'static, Result<HttpResponse, FlowerError>> + Send>;

/// One scripted step.
pub enum Step {
    Reply(MockReply),
    Fail(FlowerError),
    /// Never answer; the request is marked aborted when its future is dropped.
    Hang,
    /// Answer with custom code.
    Handle(Handler),
}

impl From<MockReply> for Step {
    fn from(reply: MockReply) -> Self {
        Step::Reply(reply)
    }
}

impl From<FlowerError> for Step {
    fn from(error: FlowerError) -> Self {
        Step::Fail(error)
    }
}

/// A request the mock received.
#[derive(Clone, Debug)]
pub struct Recorded {
    pub method: Method,
    pub url: String,
    pub headers: HeaderMap,
    pub body: Bytes,
    /// The body parsed as JSON (`null` when it isn't).
    pub json: Value,
    pub lane: Lane,
    aborted: Arc<AtomicBool>,
}

impl Recorded {
    /// Whether the client dropped this request (its pending future or unfinished body).
    pub fn aborted(&self) -> bool {
        self.aborted.load(Ordering::SeqCst)
    }

    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(|value| value.to_str().ok())
    }
}

#[derive(Default)]
struct State {
    steps: VecDeque<Step>,
    requests: Vec<Recorded>,
}

/// The scripted transport. Share it with `Arc` and pass it to `ClientBuilder::transport`.
#[derive(Default)]
pub struct MockTransport {
    state: Mutex<State>,
}

struct Hang(Arc<AtomicBool>);

impl Drop for Hang {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

impl MockTransport {
    pub fn new(steps: impl IntoIterator<Item = Step>) -> Arc<Self> {
        Arc::new(MockTransport {
            state: Mutex::new(State {
                steps: steps.into_iter().collect(),
                requests: Vec::new(),
            }),
        })
    }

    pub fn push(&self, step: impl Into<Step>) {
        self.state.lock().unwrap().steps.push_back(step.into());
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.state.lock().unwrap().requests.clone()
    }

    pub fn count(&self) -> usize {
        self.state.lock().unwrap().requests.len()
    }
}

fn response(reply: MockReply, aborted: Arc<AtomicBool>) -> Result<HttpResponse, FlowerError> {
    let mut headers = HeaderMap::new();
    for (name, value) in reply.headers {
        headers.append(
            HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| FlowerError::invalid("bad mock header"))?,
            HeaderValue::from_str(&value).map_err(|_| FlowerError::invalid("bad mock header"))?,
        );
    }
    let body = match reply.body {
        MockBody::Full(bytes) if bytes.is_empty() => ResponseBody::Empty,
        MockBody::Full(bytes) => ResponseBody::Full(bytes),
        MockBody::Stream(mut stream) => {
            stream.aborted = Some(aborted);
            ResponseBody::Stream(Box::pin(stream))
        }
    };
    Ok(HttpResponse {
        status: StatusCode::from_u16(reply.status)
            .map_err(|_| FlowerError::invalid("bad mock status"))?,
        headers,
        body,
    })
}

impl Transport for MockTransport {
    fn send(&self, request: HttpRequest) -> BoxFuture<'static, Result<HttpResponse, FlowerError>> {
        let aborted = Arc::new(AtomicBool::new(false));
        let step = {
            let mut state = self.state.lock().unwrap();
            state.requests.push(Recorded {
                method: request.method.clone(),
                url: request.url.clone(),
                headers: request.headers.clone(),
                body: request.body.clone(),
                json: serde_json::from_slice(&request.body).unwrap_or(Value::Null),
                lane: request.lane,
                aborted: aborted.clone(),
            });
            state.steps.pop_front().unwrap_or(Step::Hang)
        };
        match step {
            Step::Reply(reply) => Box::pin(async move { response(reply, aborted) }),
            Step::Fail(error) => Box::pin(async move { Err(error) }),
            Step::Hang => Box::pin(async move {
                let _hang = Hang(aborted);
                std::future::pending::<Result<HttpResponse, FlowerError>>().await
            }),
            Step::Handle(handler) => handler(&request),
        }
    }
}
