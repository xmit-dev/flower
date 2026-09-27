//! Ports of `sdk/http2.test.ts` against in-process hyper servers (h2c prior knowledge and HTTP/1.1
//! on one port, like Flower; TLS with ALPN; a raw h2 server for stream resets).

mod common;

use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use common::*;
use flower_client::{
    Bundle, DeployOptions, ErrorKind, FlowerAdmin, FlowerClient, HttpRequest, HttpTransport,
    JavaScriptBundle, Lane, MutationOptions, Protocol, RequestOptions, ResponseBody, Transport,
    TransportOptions,
};
use futures_util::future::BoxFuture;
use futures_util::stream::StreamExt;
use http::{HeaderMap, HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper_util::rt::{TokioExecutor, TokioIo};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{Notify, mpsc};
use tokio::task::AbortHandle;

type Body = BoxBody<Bytes, io::Error>;

/// Per-connection controls a handler can use.
#[derive(Clone)]
struct Conn {
    goaway: Arc<Notify>,
    kill: Arc<Notify>,
}

type Handler =
    Arc<dyn Fn(Request<Incoming>, Conn) -> BoxFuture<'static, Response<Body>> + Send + Sync>;

struct Server {
    url: String,
    connections: Arc<AtomicUsize>,
    tasks: Arc<Mutex<Vec<AbortHandle>>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Auto,
    Http1Only,
}

async fn serve(
    mode: Mode,
    handler: impl Fn(Request<Incoming>, Conn) -> BoxFuture<'static, Response<Body>>
    + Send
    + Sync
    + 'static,
) -> Server {
    let handler: Handler = Arc::new(handler);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address: SocketAddr = listener.local_addr().unwrap();
    let connections = Arc::new(AtomicUsize::new(0));
    let tasks = Arc::new(Mutex::new(Vec::new()));
    let (count, registry) = (connections.clone(), tasks.clone());
    let accept = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            count.fetch_add(1, Ordering::SeqCst);
            let conn = Conn {
                goaway: Arc::new(Notify::new()),
                kill: Arc::new(Notify::new()),
            };
            let handler = handler.clone();
            let control = conn.clone();
            let service = hyper::service::service_fn(move |request| {
                let future = handler(request, conn.clone());
                async move { Ok::<_, Infallible>(future.await) }
            });
            let task = tokio::spawn(async move {
                let io = TokioIo::new(stream);
                match mode {
                    Mode::Auto => {
                        let builder =
                            hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
                        let connection = builder.serve_connection(io, service);
                        tokio::pin!(connection);
                        tokio::select! {
                            _ = connection.as_mut() => {}
                            _ = control.goaway.notified() => {
                                connection.as_mut().graceful_shutdown();
                                tokio::select! { _ = connection => {} _ = control.kill.notified() => {} }
                            }
                            _ = control.kill.notified() => {}
                        }
                    }
                    Mode::Http1Only => {
                        let connection = hyper::server::conn::http1::Builder::new()
                            .serve_connection(io, service);
                        tokio::select! { _ = connection => {} _ = control.kill.notified() => {} }
                    }
                }
            });
            registry.lock().unwrap().push(task.abort_handle());
        }
    });
    tasks.lock().unwrap().push(accept.abort_handle());
    Server {
        url: format!("http://{address}"),
        connections,
        tasks,
    }
}

fn full(status: u16, headers: &[(&str, &str)], body: impl Into<Bytes>) -> Response<Body> {
    let mut response = Response::builder().status(status);
    for (name, value) in headers {
        response = response.header(*name, *value);
    }
    response
        .body(
            Full::new(body.into())
                .map_err(|never| match never {})
                .boxed(),
        )
        .unwrap()
}

fn reply(value: Value) -> Response<Body> {
    full(
        200,
        &[("content-type", "application/json")],
        json!({"revision": 1, "value": value, "duplicate": false}).to_string(),
    )
}

type Chunks = mpsc::Sender<Result<Bytes, io::Error>>;

/// A streaming response fed through the returned sender (capacity 1, so writes backpressure).
fn streaming(status: u16, headers: &[(&str, &str)]) -> (Response<Body>, Chunks) {
    let (sender, receiver) = mpsc::channel::<Result<Bytes, io::Error>>(1);
    let stream = futures_util::stream::unfold(receiver, |mut receiver| async move {
        let item = receiver.recv().await?;
        Some((item.map(Frame::data), receiver))
    });
    let mut response = Response::builder().status(status);
    for (name, value) in headers {
        response = response.header(*name, *value);
    }
    (
        response
            .body(BodyExt::boxed(StreamBody::new(stream)))
            .unwrap(),
        sender,
    )
}

async fn body_json(request: Request<Incoming>) -> (http::request::Parts, Value) {
    let (parts, body) = request.into_parts();
    let bytes = body.collect().await.unwrap().to_bytes();
    (parts, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

/// Test defaults: a 1 s deadline and no CA environment.
fn opts() -> TransportOptions {
    TransportOptions {
        request_timeout: Duration::from_secs(1),
        ca_from_env: false,
        ..Default::default()
    }
}

fn transport(options: TransportOptions) -> Arc<HttpTransport> {
    Arc::new(HttpTransport::new(options).unwrap())
}

fn client_for(server: &Server, transport: &Arc<HttpTransport>) -> FlowerClient {
    FlowerClient::builder(&server.url)
        .transport(transport.clone())
        .build()
        .unwrap()
}

fn watch_request(url: String) -> HttpRequest {
    let mut headers = HeaderMap::new();
    headers.insert("accept", HeaderValue::from_static("text/event-stream"));
    headers.insert("content-type", HeaderValue::from_static("application/json"));
    HttpRequest {
        method: Method::POST,
        url,
        headers,
        body: Bytes::from_static(b"{}"),
        lane: Lane::Watch,
        timeout: None,
    }
}

fn unary_request(url: String) -> HttpRequest {
    HttpRequest {
        lane: Lane::Unary,
        ..watch_request(url)
    }
}

async fn text(body: &mut ResponseBody) -> String {
    String::from_utf8(bounded(body.chunk()).await.unwrap().unwrap().to_vec()).unwrap()
}

#[tokio::test]
async fn sse_headers_return_immediately_outlive_the_header_deadline_and_run_beside_ordinary_calls()
{
    let sse: Arc<Mutex<Option<Chunks>>> = Arc::default();
    let slot = sse.clone();
    let server = serve(Mode::Auto, move |request, _| {
        let slot = slot.clone();
        Box::pin(async move {
            if request
                .headers()
                .get("accept")
                .is_some_and(|value| value == "text/event-stream")
            {
                let (response, sender) = streaming(200, &[("content-type", "text/event-stream")]);
                sender
                    .send(Ok(Bytes::from_static(b": connected\n\n")))
                    .await
                    .unwrap();
                *slot.lock().unwrap() = Some(sender);
                response
            } else {
                body_json(request).await;
                reply(json!("ok"))
            }
        })
    })
    .await;
    let transport = transport(TransportOptions {
        request_timeout: ms(40),
        ..opts()
    });
    let client = client_for(&server, &transport);
    let mut response = bounded(transport.send(watch_request(format!("{}/v1/watch", server.url))))
        .await
        .unwrap();
    assert!(text(&mut response.body).await.contains("connected"));
    tokio::time::sleep(ms(80)).await;
    assert_eq!(
        client
            .query_value("still-usable", &(), RequestOptions::new())
            .await
            .unwrap()
            .value,
        json!("ok")
    );
    let sender = sse.lock().unwrap().take().unwrap();
    sender
        .send(Ok(Bytes::from_static(b"event: snapshot\ndata: {}\n\n")))
        .await
        .unwrap();
    assert!(text(&mut response.body).await.contains("event: snapshot"));
    drop(response);
    bounded(sender.closed()).await;
    assert_eq!(
        client
            .query_value("after-cancel", &(), RequestOptions::new())
            .await
            .unwrap()
            .value,
        json!("ok")
    );
    assert_eq!(
        server.connections.load(Ordering::SeqCst),
        2,
        "one watch connection and one unary connection"
    );
}

#[tokio::test]
async fn sse_bounds_non_streaming_error_bodies_and_times_out_missing_headers() {
    let held: Arc<Mutex<Vec<Chunks>>> = Arc::default();
    let keep = held.clone();
    let server = serve(Mode::Auto, move |request, _| {
        let keep = keep.clone();
        Box::pin(async move {
            match request.uri().path() {
                "/error" => full(
                    400,
                    &[("content-type", "application/json")],
                    "x".repeat(1024),
                ),
                "/sse-error" => {
                    let (response, sender) =
                        streaming(503, &[("content-type", "text/event-stream")]);
                    sender
                        .send(Ok(Bytes::from_static(b": unavailable\n\n")))
                        .await
                        .unwrap();
                    keep.lock().unwrap().push(sender);
                    response
                }
                _ => std::future::pending().await,
            }
        })
    })
    .await;
    let transport = transport(TransportOptions {
        max_response_bytes: 128,
        request_timeout: ms(40),
        ..opts()
    });
    let response = transport
        .send(watch_request(format!("{}/error", server.url)))
        .await
        .unwrap();
    assert_eq!(response.status, 400);
    let error = response
        .body
        .collect(usize::MAX, || unreachable!())
        .await
        .unwrap_err();
    assert_eq!(error.code, "H2_RESPONSE_TOO_LARGE");
    let response = transport
        .send(watch_request(format!("{}/sse-error", server.url)))
        .await
        .unwrap();
    let error = response
        .body
        .collect(usize::MAX, || unreachable!())
        .await
        .unwrap_err();
    assert!(error.is_timeout(), "{error:?}");
    let error = bounded(transport.send(watch_request(format!("{}/stall", server.url))))
        .await
        .unwrap_err();
    assert!(error.is_timeout() && error.is_transient(), "{error:?}");
}

#[tokio::test]
async fn an_unread_event_stream_is_backpressured_instead_of_buffered_whole() {
    let written = Arc::new(AtomicUsize::new(0));
    let counter = written.clone();
    let server = serve(Mode::Auto, move |_, _| {
        let counter = counter.clone();
        Box::pin(async move {
            let (response, sender) = streaming(200, &[("content-type", "text/event-stream")]);
            tokio::spawn(async move {
                while counter.load(Ordering::SeqCst) < 16 << 20 {
                    if sender
                        .send(Ok(Bytes::from(vec![b':'; 16 * 1024])))
                        .await
                        .is_err()
                    {
                        return;
                    }
                    counter.fetch_add(16 * 1024, Ordering::SeqCst);
                }
            });
            response
        })
    })
    .await;
    let transport = transport(opts());
    let response = transport
        .send(watch_request(server.url.clone()))
        .await
        .unwrap();
    tokio::time::sleep(ms(100)).await;
    let seen = written.load(Ordering::SeqCst);
    assert!(seen < 4 << 20, "an unread watch consumed {seen} bytes");
    drop(response);
}

#[tokio::test]
async fn sdk_calls_multiplex_on_one_connection_and_preserve_methods_headers_and_request_ids() {
    let concurrent = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let received: Arc<Mutex<Vec<(String, Value, Option<String>, http::Version)>>> = Arc::default();
    let (now, top, seen) = (concurrent.clone(), peak.clone(), received.clone());
    let server = serve(Mode::Auto, move |request, _| {
        let (now, top, seen) = (now.clone(), top.clone(), seen.clone());
        Box::pin(async move {
            assert_eq!(request.method(), Method::POST);
            let (parts, data) = body_json(request).await;
            let token = parts
                .headers
                .get("authorization")
                .map(|value| value.to_str().unwrap().to_owned());
            seen.lock().unwrap().push((
                parts.uri.path().to_owned(),
                data.clone(),
                token,
                parts.version,
            ));
            let running = now.fetch_add(1, Ordering::SeqCst) + 1;
            top.fetch_max(running, Ordering::SeqCst);
            tokio::time::sleep(ms(15)).await;
            now.fetch_sub(1, Ordering::SeqCst);
            reply(data.get("args").cloned().unwrap_or(Value::Null))
        })
    })
    .await;
    let transport = transport(opts());
    let client = client_for(&server, &transport);
    let admin = FlowerAdmin::builder(&server.url)
        .admin_token("test-token")
        .transport(transport.clone())
        .build()
        .unwrap();
    let calls = (0..12).map(|index| {
        let client = client.clone();
        async move {
            client
                .call::<_, Value>(
                    "test",
                    &json!({"index": index}),
                    MutationOptions::new().request_id(format!("same-{index}")),
                )
                .await
        }
    });
    let values = futures_util::future::join_all(calls).await;
    assert!(values.iter().all(Result::is_ok));
    assert!(
        peak.load(Ordering::SeqCst) > 1,
        "requests overlap on the HTTP/2 connection"
    );
    assert_eq!(server.connections.load(Ordering::SeqCst), 1);
    let mut ids: Vec<_> = received
        .lock()
        .unwrap()
        .iter()
        .map(|(_, data, _, _)| data["requestId"].as_str().unwrap().to_owned())
        .collect();
    ids.sort();
    let mut expected: Vec<_> = (0..12).map(|index| format!("same-{index}")).collect();
    expected.sort();
    assert_eq!(ids, expected);
    client
        .query_value("read", &(), RequestOptions::new())
        .await
        .unwrap();
    client
        .mutate_value(
            "write",
            &(),
            MutationOptions::new()
                .request_id("stable")
                .expected_revision(1),
        )
        .await
        .unwrap();
    let bundle = Bundle::from(JavaScriptBundle {
        hash: "hash".into(),
        javascript: "source".into(),
    });
    admin
        .deploy(
            &bundle,
            DeployOptions {
                request_id: Some("deploy".into()),
                preparation: None,
            },
        )
        .await
        .unwrap();
    admin
        .initialize(&[("1".to_owned(), "127.0.0.1:1".to_owned())].into())
        .await
        .unwrap();
    assert_eq!(
        server.connections.load(Ordering::SeqCst),
        1,
        "sequential requests reuse the connection too"
    );
    let received = received.lock().unwrap();
    let paths: Vec<_> = received[12..]
        .iter()
        .map(|(path, ..)| path.as_str())
        .collect();
    assert_eq!(
        paths,
        [
            "/v1/query",
            "/v1/mutate",
            "/admin/deploy",
            "/raft/initialize"
        ]
    );
    assert!(
        received
            .iter()
            .all(|(.., version)| *version == http::Version::HTTP_2)
    );
    assert_eq!(received[15].2.as_deref(), Some("Bearer test-token"));
    assert_eq!(received[14].1["requestId"], json!("deploy"));
    assert_eq!(received[13].1["expectedRevision"], json!(1));
    assert!(
        received[..14]
            .iter()
            .all(|(_, _, token, _)| token.is_none()),
        "method calls never carry the operator token"
    );
}

#[tokio::test]
async fn a_caller_deadline_cancels_a_stalled_body_without_cancelling_other_streams() {
    let held: Arc<Mutex<Vec<Chunks>>> = Arc::default();
    let keep = held.clone();
    let server = serve(Mode::Auto, move |request, _| {
        let keep = keep.clone();
        Box::pin(async move {
            let (_, data) = body_json(request).await;
            if data["name"] == "stall" {
                let (response, sender) = streaming(200, &[]);
                sender
                    .send(Ok(Bytes::from_static(b"{\"revision\":")))
                    .await
                    .unwrap();
                keep.lock().unwrap().push(sender);
                response
            } else {
                reply(json!("ok"))
            }
        })
    })
    .await;
    let transport = transport(opts());
    let client = client_for(&server, &transport);
    let cancel = flower_client::CancellationToken::new();
    let blocked = tokio::spawn({
        let client = client.clone();
        let cancel = cancel.clone();
        async move {
            client
                .query_value("stall", &(), RequestOptions::new().cancel(cancel))
                .await
        }
    });
    assert_eq!(
        client
            .query_value("other", &(), RequestOptions::new())
            .await
            .unwrap()
            .value,
        json!("ok")
    );
    tokio::time::sleep(ms(40)).await;
    cancel.cancel();
    assert!(bounded(blocked).await.unwrap().unwrap_err().is_aborted());
    let sender = held.lock().unwrap().pop().unwrap();
    bounded(sender.closed()).await;
    assert_eq!(
        client
            .query_value("after-abort", &(), RequestOptions::new())
            .await
            .unwrap()
            .value,
        json!("ok")
    );
    assert_eq!(server.connections.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn the_default_deadline_covers_body_completion_and_cancelled_calls_open_no_connection() {
    let held: Arc<Mutex<Vec<Chunks>>> = Arc::default();
    let keep = held.clone();
    let server = serve(Mode::Auto, move |_, _| {
        let keep = keep.clone();
        Box::pin(async move {
            let (response, sender) = streaming(200, &[]);
            sender.send(Ok(Bytes::from_static(b"{"))).await.unwrap();
            keep.lock().unwrap().push(sender);
            response
        })
    })
    .await;
    let transport = transport(TransportOptions {
        request_timeout: ms(40),
        ..opts()
    });
    let client = client_for(&server, &transport);
    let cancel = flower_client::CancellationToken::new();
    cancel.cancel();
    let error = client
        .call::<_, Value>("cancelled", &(), MutationOptions::new().cancel(cancel))
        .await
        .unwrap_err();
    assert!(error.is_aborted());
    assert_eq!(server.connections.load(Ordering::SeqCst), 0);
    let started = std::time::Instant::now();
    let error = client
        .call::<_, Value>("stall", &(), MutationOptions::new())
        .await
        .unwrap_err();
    assert!(error.is_timeout(), "{error:?}");
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn request_and_response_byte_limits_fail_without_unbounded_buffering() {
    let requests = Arc::new(AtomicUsize::new(0));
    let counter = requests.clone();
    let server = serve(Mode::Auto, move |request, _| {
        let counter = counter.clone();
        Box::pin(async move {
            counter.fetch_add(1, Ordering::SeqCst);
            let (_, data) = body_json(request).await;
            if data["name"] == "huge" {
                full(200, &[], "x".repeat(1024))
            } else {
                reply(json!("ok"))
            }
        })
    })
    .await;
    let transport = transport(TransportOptions {
        max_request_bytes: 256,
        max_response_bytes: 128,
        ..opts()
    });
    let client = client_for(&server, &transport);
    let error = client
        .call::<_, Value>("large-request", &"x".repeat(300), MutationOptions::new())
        .await
        .unwrap_err();
    assert_eq!(
        (error.code.as_str(), error.is_transient()),
        ("H2_REQUEST_TOO_LARGE", false)
    );
    assert_eq!(requests.load(Ordering::SeqCst), 0);
    assert_eq!(server.connections.load(Ordering::SeqCst), 0);
    let error = client
        .query_value("huge", &(), RequestOptions::new())
        .await
        .unwrap_err();
    assert_eq!(
        (error.code.as_str(), error.is_transient()),
        ("H2_RESPONSE_TOO_LARGE", false)
    );
    assert_eq!(
        client
            .query_value("small", &(), RequestOptions::new())
            .await
            .unwrap()
            .value,
        json!("ok")
    );
}

#[tokio::test]
async fn goaway_drains_accepted_streams_and_sends_later_calls_on_a_new_connection() {
    let first = Arc::new(AtomicUsize::new(0));
    let counter = first.clone();
    let server = serve(Mode::Auto, move |request, conn| {
        let counter = counter.clone();
        Box::pin(async move {
            body_json(request).await;
            if counter.fetch_add(1, Ordering::SeqCst) == 0 {
                conn.goaway.notify_one();
                tokio::time::sleep(ms(10)).await;
            }
            reply(json!("ok"))
        })
    })
    .await;
    let transport = transport(opts());
    let client = client_for(&server, &transport);
    assert_eq!(
        client
            .query_value("first", &(), RequestOptions::new())
            .await
            .unwrap()
            .value,
        json!("ok")
    );
    tokio::time::sleep(ms(20)).await;
    assert_eq!(
        client
            .query_value("second", &(), RequestOptions::new())
            .await
            .unwrap()
            .value,
        json!("ok")
    );
    assert_eq!(server.connections.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn lost_connections_fail_once_without_implicit_replay_and_later_calls_reconnect() {
    let received: Arc<Mutex<Vec<Value>>> = Arc::default();
    let seen = received.clone();
    let server = serve(Mode::Auto, move |request, conn| {
        let seen = seen.clone();
        Box::pin(async move {
            let (_, data) = body_json(request).await;
            let count = {
                let mut seen = seen.lock().unwrap();
                seen.push(data);
                seen.len()
            };
            if count == 1 {
                conn.kill.notify_one();
                std::future::pending::<()>().await;
            }
            reply(json!("ok"))
        })
    })
    .await;
    let transport = transport(opts());
    let client = client_for(&server, &transport);
    let error = client
        .call::<_, Value>(
            "write",
            &json!({"amount": 1}),
            MutationOptions::new().request_id("keep-me"),
        )
        .await
        .unwrap_err();
    assert!(error.is_transient(), "{error:?}");
    assert_eq!(
        received.lock().unwrap().len(),
        1,
        "the transport does not replay an uncertain mutation"
    );
    let result = client
        .call::<_, Value>(
            "write",
            &json!({"amount": 1}),
            MutationOptions::new().request_id("keep-me"),
        )
        .await
        .unwrap();
    assert_eq!(result.value, json!("ok"));
    assert_eq!(server.connections.load(Ordering::SeqCst), 2);
    let received = received.lock().unwrap();
    assert_eq!(received[0], received[1]);
}

#[tokio::test]
async fn a_connection_lost_after_response_headers_rejects_incomplete_bodies_instead_of_returning_200()
 {
    for declared in [false, true] {
        for partial in ["", "{\"revision\":1,\"value\":\"🌸"] {
            let killed = Arc::new(AtomicUsize::new(0));
            let flag = killed.clone();
            let server = serve(Mode::Auto, move |request, conn| {
                let flag = flag.clone();
                Box::pin(async move {
                    body_json(request).await;
                    if flag.load(Ordering::SeqCst) > 0 {
                        return reply(json!("ok"));
                    }
                    let mut headers = vec![("content-type", "application/json")];
                    if declared {
                        headers.push(("content-length", "100"));
                    }
                    let (response, sender) = streaming(200, &headers);
                    tokio::spawn(async move {
                        if !partial.is_empty() {
                            sender.send(Ok(Bytes::from(partial))).await.unwrap();
                        }
                        tokio::time::sleep(ms(20)).await;
                        flag.fetch_add(1, Ordering::SeqCst);
                        conn.kill.notify_one();
                        std::future::pending::<()>().await;
                        drop(sender);
                    });
                    response
                })
            })
            .await;
            let transport = transport(opts());
            let error = bounded(transport.send(unary_request(server.url.clone())))
                .await
                .unwrap_err();
            assert_eq!(
                error.kind,
                ErrorKind::Transport,
                "{declared} {partial:?}: {error:?}"
            );
            assert!(error.is_transient(), "{declared} {partial:?}: {error:?}");
            assert_eq!(killed.load(Ordering::SeqCst), 1);
            let retry = bounded(transport.send(unary_request(server.url.clone())))
                .await
                .unwrap();
            let ResponseBody::Full(bytes) = retry.body else {
                panic!("unary bodies are buffered")
            };
            assert_eq!(
                serde_json::from_slice::<Value>(&bytes).unwrap()["value"],
                json!("ok")
            );
            assert_eq!(server.connections.load(Ordering::SeqCst), 2);
        }
    }
}

#[tokio::test]
async fn content_length_counts_bytes_and_cleanly_completed_malformed_json_is_an_application_error()
{
    let server = serve(Mode::Auto, |request, _| {
        Box::pin(async move {
            let (_, data) = body_json(request).await;
            let text = if data["name"] == "malformed" {
                "{".to_owned()
            } else {
                json!({"revision": 1, "value": "🌸 café", "duplicate": false}).to_string()
            };
            let length = text.len().to_string();
            full(
                200,
                &[
                    ("content-type", "application/json"),
                    ("content-length", &length),
                ],
                text,
            )
        })
    })
    .await;
    let transport = transport(opts());
    let client = client_for(&server, &transport);
    assert_eq!(
        client
            .query_value("unicode", &(), RequestOptions::new())
            .await
            .unwrap()
            .value,
        json!("🌸 café")
    );
    let mut request = unary_request(server.url.clone());
    request.body = Bytes::from_static(br#"{"name":"malformed"}"#);
    let response = transport.send(request).await.unwrap();
    assert_eq!(response.status, StatusCode::OK);
    let ResponseBody::Full(bytes) = response.body else {
        panic!()
    };
    assert_eq!(
        &bytes[..],
        b"{",
        "the transport must not reinterpret an intact invalid reply"
    );
    let error = client
        .query_value("malformed", &(), RequestOptions::new())
        .await
        .unwrap_err();
    assert_eq!(
        (error.kind, error.is_transient()),
        (ErrorKind::Decode, false)
    );
}

#[tokio::test]
async fn idle_connections_expire_and_compressed_replies_fail_explicitly() {
    let server = serve(Mode::Auto, |request, _| {
        Box::pin(async move {
            let (_, data) = body_json(request).await;
            if data["name"] == "compressed" {
                full(200, &[("content-encoding", "gzip")], "compressed")
            } else {
                reply(json!("ok"))
            }
        })
    })
    .await;
    let transport = transport(TransportOptions {
        pool_idle_timeout: ms(15),
        ..opts()
    });
    let client = client_for(&server, &transport);
    assert_eq!(
        client
            .query_value("first", &(), RequestOptions::new())
            .await
            .unwrap()
            .value,
        json!("ok")
    );
    tokio::time::sleep(ms(200)).await;
    assert_eq!(
        client
            .query_value("second", &(), RequestOptions::new())
            .await
            .unwrap()
            .value,
        json!("ok")
    );
    assert_eq!(server.connections.load(Ordering::SeqCst), 2);
    let error = client
        .query_value("compressed", &(), RequestOptions::new())
        .await
        .unwrap_err();
    assert_eq!(
        (error.code.as_str(), error.is_transient()),
        ("H2_UNSUPPORTED_ENCODING", false)
    );
}

#[tokio::test]
async fn round_robin_connections_and_protocol_choices() {
    let server = serve(Mode::Auto, |request, _| {
        Box::pin(async move {
            let version = format!("{:?}", request.version());
            body_json(request).await;
            reply(json!(version))
        })
    })
    .await;
    let transport = transport(TransportOptions {
        connections: 3,
        ..opts()
    });
    let client = client_for(&server, &transport);
    for _ in 0..6 {
        assert_eq!(
            client
                .query_value("v", &(), RequestOptions::new())
                .await
                .unwrap()
                .value,
            json!("HTTP/2.0")
        );
    }
    assert_eq!(server.connections.load(Ordering::SeqCst), 3);

    let old = serve(Mode::Http1Only, |request, _| {
        Box::pin(async move {
            let version = format!("{:?}", request.version());
            body_json(request).await;
            reply(json!(version))
        })
    })
    .await;
    let http1 = FlowerClient::builder(&old.url).http1().build().unwrap();
    assert_eq!(
        http1
            .query_value("v", &(), RequestOptions::new())
            .await
            .unwrap()
            .value,
        json!("HTTP/1.1")
    );
    let h2c = FlowerClient::builder(&old.url)
        .transport(transport.clone())
        .build()
        .unwrap();
    assert!(
        h2c.query_value("v", &(), RequestOptions::new())
            .await
            .unwrap_err()
            .is_transient()
    );
    let _ = Protocol::H2c;
    for url in ["ftp://localhost/", "http://user:secret@localhost/"] {
        let error = transport
            .send(unary_request(url.to_owned()))
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::Invalid, "{url}");
    }
    let refused = FlowerClient::builder("http://127.0.0.1:1")
        .transport(transport.clone())
        .build()
        .unwrap();
    let error = refused
        .query_value("v", &(), RequestOptions::new())
        .await
        .unwrap_err();
    assert_eq!(
        (error.code.as_str(), error.is_transient()),
        ("ECONNREFUSED", true),
        "{error:?}"
    );
}

/// A raw h2 server that resets every stream with REFUSED_STREAM.
#[tokio::test]
async fn refused_streams_are_transient_http2_stream_errors() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut connection = h2::server::handshake(socket).await.unwrap();
        while let Some(Ok((_, mut respond))) = connection.accept().await {
            respond.send_reset(h2::Reason::REFUSED_STREAM);
        }
    });
    let client = FlowerClient::builder(&url).build().unwrap();
    let error = client
        .query_value("v", &(), RequestOptions::new())
        .await
        .unwrap_err();
    assert_eq!(error.code, "ERR_HTTP2_STREAM_ERROR", "{error:?}");
    assert_eq!(
        error.message,
        "Stream closed with error code NGHTTP2_REFUSED_STREAM"
    );
    assert!(error.is_transient());
    server.abort();
}

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../tests/fixtures/tls")
}

#[tokio::test]
async fn https_verifies_custom_roots_and_hostnames_while_pooling() {
    use rustls_pki_types::pem::PemObject;
    use rustls_pki_types::{CertificateDer, PrivateKeyDer};
    use tokio_rustls::rustls;
    let certificates: Vec<CertificateDer<'static>> =
        CertificateDer::pem_file_iter(fixtures().join("localhost.crt"))
            .unwrap()
            .map(Result::unwrap)
            .collect();
    let key = PrivateKeyDer::from_pem_file(fixtures().join("localhost.key")).unwrap();
    let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_no_client_auth()
    .with_single_cert(certificates, key)
    .unwrap();
    config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let connections = Arc::new(AtomicUsize::new(0));
    let count = connections.clone();
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let acceptor = acceptor.clone();
            let count = count.clone();
            tokio::spawn(async move {
                let Ok(stream) = acceptor.accept(socket).await else {
                    return;
                };
                count.fetch_add(1, Ordering::SeqCst);
                let service = hyper::service::service_fn(|request: Request<Incoming>| async move {
                    let version = format!("{:?}", request.version());
                    body_json(request).await;
                    Ok::<_, Infallible>(reply(json!(version)))
                });
                let _ = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    let trusted = FlowerClient::builder(format!("https://localhost:{port}"))
        .ca_file(fixtures().join("ca.crt"))
        .build()
        .unwrap();
    assert_eq!(
        trusted
            .query_value("value", &(), RequestOptions::new())
            .await
            .unwrap()
            .value,
        json!("HTTP/2.0")
    );
    assert_eq!(
        trusted
            .query_value("value", &(), RequestOptions::new())
            .await
            .unwrap()
            .value,
        json!("HTTP/2.0")
    );
    assert_eq!(connections.load(Ordering::SeqCst), 1);
    let untrusted = FlowerClient::builder(format!("https://localhost:{port}"))
        .transport_options(TransportOptions {
            ca_from_env: false,
            ..Default::default()
        })
        .build()
        .unwrap();
    let error = untrusted
        .query_value("untrusted", &(), RequestOptions::new())
        .await
        .unwrap_err();
    assert!(
        !error.is_transient(),
        "certificate failures are permanent: {error:?}"
    );
    let wrong_name = trusted.with_credentials(flower_client::Credentials::None);
    let wrong_name = FlowerClient::builder(format!("https://127.0.0.1:{port}"))
        .transport(wrong_name.transport().clone())
        .build()
        .unwrap();
    assert!(
        wrong_name
            .query_value("wrong-name", &(), RequestOptions::new())
            .await
            .is_err()
    );
    let only = FlowerClient::builder(format!("https://localhost:{port}"))
        .transport_options(TransportOptions {
            ca_files: vec![fixtures().join("ca.crt")],
            roots_only: true,
            ..Default::default()
        })
        .build()
        .unwrap();
    assert!(
        only.query_value("value", &(), RequestOptions::new())
            .await
            .is_ok()
    );
    let error = HttpTransport::new(TransportOptions {
        ca_files: vec![fixtures().join("missing.crt")],
        ..Default::default()
    })
    .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Invalid);
    server.abort();
}

#[tokio::test]
async fn subscribe_over_real_http2_reconnects_after_the_server_ends_a_stream() {
    let opened = Arc::new(AtomicUsize::new(0));
    let counter = opened.clone();
    let server = serve(Mode::Auto, move |request, _| {
        let counter = counter.clone();
        Box::pin(async move {
            let (_, data) = body_json(request).await;
            assert_eq!(data["name"], json!("counter"));
            let n = counter.fetch_add(1, Ordering::SeqCst) as u64;
            let (response, sender) = streaming(200, &[("content-type", "text/event-stream")]);
            tokio::spawn(async move {
                let first = snapshot(json!({"n": n * 10}), n * 10 + 1, 5);
                let _ = sender.send(Ok(Bytes::from(first))).await;
                let _ = sender
                    .send(Ok(Bytes::from(patch(
                        json!(n * 10 + 1),
                        n * 10 + 2,
                        6,
                        "/n",
                    ))))
                    .await;
                if n == 0 {
                    // End the first stream abruptly: an error resets the HTTP/2 stream.
                    let _ = sender.send(Err(io::Error::other("gone"))).await;
                } else {
                    std::future::pending::<()>().await;
                }
            });
            response
        })
    })
    .await;
    let client = FlowerClient::new(&server.url).unwrap();
    let options = flower_client::SubscribeOptions::new().reconnect(flower_client::Reconnect::On {
        initial_delay: ms(1),
        max_delay: ms(5),
    });
    let mut updates = client.subscribe::<_, Value>("counter", &(), options);
    let got: Vec<_> = take(&mut updates, 4)
        .await
        .into_iter()
        .map(|item| {
            item.map(|u| (u.revision, u.value["n"].clone(), u.reset))
                .unwrap()
        })
        .collect();
    assert_eq!(
        got,
        [
            (1, json!(0), true),
            (2, json!(1), false),
            (11, json!(10), true),
            (12, json!(11), false)
        ]
    );
    assert_eq!(opened.load(Ordering::SeqCst), 2);
    drop(updates);
    let _ = StreamExt::boxed(futures_util::stream::empty::<()>());
}
