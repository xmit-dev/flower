use std::{ffi::OsStr, net::SocketAddr, sync::Arc, time::Duration};

use axum::{Router, http::Version, routing::get};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Notify, Semaphore, oneshot},
    task::JoinHandle,
    time::timeout,
};

use super::{Config, serve};

const DEADLINE: Duration = Duration::from_secs(5);

fn client(http2: bool) -> reqwest::Client {
    let builder = reqwest::Client::builder().no_proxy().timeout(DEADLINE);
    if http2 {
        builder.http2_prior_knowledge()
    } else {
        builder.http1_only()
    }
    .build()
    .unwrap()
}

fn spawn_server(
    listener: TcpListener,
    app: Router,
    max_streams: Option<u32>,
) -> (oneshot::Sender<()>, JoinHandle<std::io::Result<()>>) {
    spawn_configured_server(
        listener,
        app,
        Config {
            http2_max_streams: max_streams,
            ..Config::default()
        },
    )
}

fn spawn_configured_server(
    listener: TcpListener,
    app: Router,
    config: Config,
) -> (oneshot::Sender<()>, JoinHandle<std::io::Result<()>>) {
    let (stop, stopped) = oneshot::channel();
    let task = tokio::spawn(serve(listener, app, config, async move {
        let _ = stopped.await;
    }));
    (stop, task)
}

#[test]
fn header_budgets_accept_supported_ranges_and_reject_invalid_startup_settings() {
    let parse =
        |name: &str, value: &str| Config::parse_options(|key| (key == name).then(|| value.into()));
    let defaults = Config::default();
    assert_eq!(defaults.http1_max_headers, None);
    assert_eq!(defaults.http1_max_buffer_bytes, 417792);
    assert_eq!(defaults.http2_max_header_list_bytes, 16384);
    assert_eq!(
        parse(super::H1_HEADERS_OPTION, "24576")
            .unwrap()
            .http1_max_headers,
        Some(super::H1_HEADER_CAPACITY)
    );
    assert!(axum::http::HeaderMap::<()>::try_with_capacity(super::H1_HEADER_CAPACITY).is_ok());
    assert!(axum::http::HeaderMap::<()>::try_with_capacity(super::H1_HEADER_CAPACITY + 1).is_err());
    assert_eq!(
        parse(super::H1_BUFFER_OPTION, "8192")
            .unwrap()
            .http1_max_buffer_bytes,
        8192
    );
    assert_eq!(
        parse(super::H1_BUFFER_OPTION, &isize::MAX.to_string())
            .unwrap()
            .http1_max_buffer_bytes,
        isize::MAX as usize
    );
    assert_eq!(
        parse(super::H2_HEADERS_OPTION, "4294967295")
            .unwrap()
            .http2_max_header_list_bytes,
        u32::MAX
    );
    for (name, value) in [
        (super::H1_HEADERS_OPTION, "24577"),
        (super::H1_BUFFER_OPTION, "8191"),
        (super::H2_HEADERS_OPTION, "4294967296"),
    ] {
        assert!(parse(name, value).unwrap_err().to_string().contains(name));
    }
    for name in [
        super::H1_HEADERS_OPTION,
        super::H1_BUFFER_OPTION,
        super::H2_HEADERS_OPTION,
    ] {
        for value in ["", "0", "-1", "1.5", " 8192", "bad"] {
            assert!(parse(name, value).unwrap_err().to_string().contains(name));
        }
    }
    assert!(parse(super::H1_BUFFER_OPTION, &usize::MAX.to_string()).is_err());
}

#[test]
fn stream_configuration_has_no_default_ceiling_and_accepts_positive_protocol_range() {
    assert_eq!(Config::parse(None).unwrap().http2_max_streams, None);
    for value in ["1", "200", "1024", "4096", "4097", "4294967295"] {
        assert_eq!(
            Config::parse(Some(OsStr::new(value)))
                .unwrap()
                .http2_max_streams,
            Some(value.parse::<u32>().unwrap())
        );
    }
    for value in ["", "0", "-1", "1.5", "200 ", " 200", "NaN", "4294967296"] {
        assert!(
            Config::parse(Some(OsStr::new(value)))
                .unwrap_err()
                .to_string()
                .contains("must be a positive 32-bit integer")
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        assert!(Config::parse(Some(OsStr::from_bytes(b"\xff"))).is_err());
    }
}

#[test]
fn shutdown_deadline_is_operator_configurable_and_rejects_invalid_values() {
    assert_eq!(Config::default().shutdown_timeout, Duration::from_secs(30));
    let parse = |value: &str| {
        Config::parse_options(|key| (key == super::SHUTDOWN_OPTION).then(|| value.into()))
    };
    assert_eq!(
        parse("17").unwrap().shutdown_timeout,
        Duration::from_millis(17)
    );
    for value in ["0", "-1", "", "1.5", "never", " 15"] {
        assert!(
            parse(value)
                .unwrap_err()
                .to_string()
                .contains(super::SHUTDOWN_OPTION)
        );
    }
}

#[tokio::test]
async fn graceful_shutdown_closes_live_sse_after_configured_drain_deadline() {
    for http2 in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let dropped = Arc::new(Notify::new());
        struct Closed(Arc<Notify>);
        impl Drop for Closed {
            fn drop(&mut self) {
                self.0.notify_one();
            }
        }
        let guard = dropped.clone();
        let app = Router::new().route(
            "/watch",
            get(move || {
                let guard = Closed(guard.clone());
                async move {
                    let body = futures_util::stream::unfold(
                        (Some(guard), true),
                        |(guard, first)| async move {
                            if !first {
                                std::future::pending::<()>().await;
                            }
                            Some((
                                Ok::<_, std::convert::Infallible>(axum::body::Bytes::from_static(
                                    b"data: live\n\n",
                                )),
                                (guard, false),
                            ))
                        },
                    );
                    (
                        [(axum::http::header::CONTENT_TYPE, "text/event-stream")],
                        axum::body::Body::from_stream(body),
                    )
                }
            }),
        );
        let (stop, task) = spawn_configured_server(
            listener,
            app,
            Config {
                shutdown_timeout: Duration::from_millis(50),
                ..Config::default()
            },
        );
        let mut response = client(http2)
            .get(format!("http://{address}/watch"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.chunk().await.unwrap().unwrap(), "data: live\n\n");
        let began = std::time::Instant::now();
        stop.send(()).unwrap();
        timeout(DEADLINE, task).await.unwrap().unwrap().unwrap();
        assert!(began.elapsed() >= Duration::from_millis(40));
        assert!(TcpStream::connect(address).await.is_err());
        // Observe both the transport close and the body drop: an HTTP/2 stream
        // task must not retain its watch admission slot after its socket closes.
        assert!(!matches!(
            timeout(DEADLINE, response.chunk()).await.unwrap(),
            Ok(Some(_))
        ));
        timeout(DEADLINE, dropped.notified()).await.unwrap();
    }
}

/// Read the peer's wire SETTINGS, the same value exposed as remoteSettings by
/// an HTTP/2 client. No request/client pool can hide a missing server setting.
async fn advertised_setting(address: SocketAddr, identifier: u16) -> Option<u32> {
    let mut socket = TcpStream::connect(address).await.unwrap();
    socket
        .write_all(b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n\x00\x00\x00\x04\x00\x00\x00\x00\x00")
        .await
        .unwrap();
    loop {
        let mut header = [0; 9];
        socket.read_exact(&mut header).await.unwrap();
        let length = u32::from_be_bytes([0, header[0], header[1], header[2]]) as usize;
        assert!(length <= 16_384, "unexpected initial HTTP/2 frame size");
        let mut payload = vec![0; length];
        socket.read_exact(&mut payload).await.unwrap();
        if header[3] == 4 && header[4] & 1 == 0 {
            assert_eq!(&header[5..], &[0; 4]);
            assert_eq!(payload.len() % 6, 0);
            for setting in payload.as_chunks::<6>().0 {
                if u16::from_be_bytes([setting[0], setting[1]]) == identifier {
                    return Some(u32::from_be_bytes(setting[2..6].try_into().unwrap()));
                }
            }
            return None;
        }
    }
}

#[tokio::test]
async fn header_count_and_buffer_budgets_change_http1_acceptance() {
    for (headers, buffer, count, value_bytes, expected_status) in [
        (100, 417792, 120, 1, 431),
        (200, 417792, 120, 1, 200),
        (100, 8192, 1, 9000, 431),
        (100, 16384, 1, 9000, 200),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, task) = spawn_configured_server(
            listener,
            Router::new().route("/", get(|| async { "ok" })),
            Config {
                http1_max_headers: Some(headers),
                http1_max_buffer_bytes: buffer,
                ..Config::default()
            },
        );
        let mut request = "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n".to_owned();
        for index in 0..count {
            request.push_str(&format!(
                "x-budget-{index}: {}\r\n",
                "a".repeat(value_bytes)
            ));
        }
        request.push_str("\r\n");
        let response = timeout(DEADLINE, async {
            let mut socket = TcpStream::connect(address).await.unwrap();
            socket.write_all(request.as_bytes()).await.unwrap();
            let mut response = String::new();
            // The server can reset a rejected connection with unread request
            // bytes after sending its status. Read the status before EOF.
            tokio::io::AsyncBufReadExt::read_line(
                &mut tokio::io::BufReader::new(socket),
                &mut response,
            )
            .await
            .unwrap();
            response
        })
        .await
        .unwrap();
        assert!(
            response.starts_with(&format!("HTTP/1.1 {expected_status} ")),
            "{response}"
        );
        stop.send(()).unwrap();
        timeout(DEADLINE, task).await.unwrap().unwrap().unwrap();
    }
}

#[tokio::test]
async fn wire_settings_advertise_configured_http2_header_budgets() {
    for maximum in [1024, 65536, u32::MAX] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, task) = spawn_configured_server(
            listener,
            Router::new(),
            Config {
                http2_max_header_list_bytes: maximum,
                ..Config::default()
            },
        );
        assert_eq!(
            timeout(DEADLINE, advertised_setting(address, 6))
                .await
                .unwrap(),
            Some(maximum)
        );
        stop.send(()).unwrap();
        timeout(DEADLINE, task).await.unwrap().unwrap().unwrap();
    }
}

#[tokio::test]
async fn wire_settings_advertise_default_and_configured_stream_limits() {
    for max_streams in [None, Some(1024), Some(u32::MAX)] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, task) = spawn_server(listener, Router::new(), max_streams);
        assert_eq!(
            timeout(DEADLINE, advertised_setting(address, 3))
                .await
                .unwrap(),
            max_streams
        );
        stop.send(()).unwrap();
        timeout(DEADLINE, task).await.unwrap().unwrap().unwrap();
    }
}

#[tokio::test]
async fn same_listener_serves_http1_http2_and_preserves_operator_auth() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let directory = tempfile::tempdir().unwrap();
    let token = "transport-test-token";
    let consensus = flower::consensus::Consensus::open(
        1,
        address.to_string(),
        directory.path().into(),
        token.into(),
    )
    .await
    .unwrap();
    let app = flower::service::router(consensus.clone(), token.into());
    let (stop, task) = spawn_server(listener, app, Some(1024));
    let base = format!("http://{address}");

    for http2 in [false, true] {
        let client = client(http2);
        let expected_version = if http2 {
            Version::HTTP_2
        } else {
            Version::HTTP_11
        };
        let response = client.get(format!("{base}/health")).send().await.unwrap();
        assert_eq!(response.version(), expected_version);
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.json::<serde_json::Value>().await.unwrap()["service"],
            "flower"
        );
        for bearer in [None, Some("wrong-token"), Some(token)] {
            let mut request = client.get(format!("{base}/raft/metrics"));
            if let Some(bearer) = bearer {
                request = request.bearer_auth(bearer);
            }
            let response = request.send().await.unwrap();
            assert_eq!(response.version(), expected_version);
            assert_eq!(
                response.status(),
                if bearer == Some(token) { 200 } else { 401 }
            );
            let value = response.json::<serde_json::Value>().await.unwrap();
            if bearer == Some(token) {
                assert_eq!(value["id"], 1);
            }
        }
        let response = client
            .post(format!("{base}/admin/deploy"))
            .json(&serde_json::json!({}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.version(), expected_version);
        assert_eq!(response.status(), 401);
        response.bytes().await.unwrap();
    }

    stop.send(()).unwrap();
    timeout(DEADLINE, task).await.unwrap().unwrap().unwrap();
    consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn graceful_shutdown_stops_accepting_and_finishes_active_requests() {
    for http2 in [false, true] {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let app = Router::new().route(
            "/slow",
            get({
                let entered = entered.clone();
                let release = release.clone();
                move || {
                    let entered = entered.clone();
                    let release = release.clone();
                    async move {
                        entered.notify_one();
                        let _permit = release.acquire().await.unwrap();
                        "finished request"
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, mut server) = spawn_server(listener, app, Some(1024));
        let request = tokio::spawn(async move {
            let response = client(http2)
                .get(format!("http://{address}/slow"))
                .send()
                .await
                .unwrap();
            assert_eq!(
                response.version(),
                if http2 {
                    Version::HTTP_2
                } else {
                    Version::HTTP_11
                }
            );
            response.text().await.unwrap()
        });
        timeout(DEADLINE, entered.notified()).await.unwrap();
        stop.send(()).unwrap();
        assert!(
            timeout(Duration::from_millis(20), &mut server)
                .await
                .is_err()
        );
        timeout(DEADLINE, async {
            loop {
                match TcpStream::connect(address).await {
                    Err(error) if error.kind() == std::io::ErrorKind::ConnectionRefused => break,
                    Err(error) => panic!("unexpected connection error: {error}"),
                    Ok(socket) => drop(socket),
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        release.add_permits(1);
        assert_eq!(
            timeout(DEADLINE, request).await.unwrap().unwrap(),
            "finished request"
        );
        timeout(DEADLINE, server).await.unwrap().unwrap().unwrap();
    }
}

#[tokio::test]
async fn native_tls_negotiates_h2_and_h1_and_rejects_untrusted_or_wrong_names() {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/tls");
    let tls = flower::transport::Tls::from_files(
        &fixtures.join("localhost.crt"),
        &fixtures.join("localhost.key"),
        &fixtures.join("ca.crt"),
        Duration::from_millis(100),
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = oneshot::channel();
    let server_tls = tls.clone();
    let server = tokio::spawn(super::serve_with_tls(
        listener,
        Router::new().route("/", get(|| async { "tls flower" })),
        Config::default(),
        Some(server_tls),
        async move {
            let _ = stopped.await;
        },
    ));
    let url = format!("https://localhost:{}/", address.port());
    for h2 in [false, true] {
        let builder = tls.client_builder().no_proxy().timeout(DEADLINE);
        let client = if h2 {
            builder.http2_prior_knowledge()
        } else {
            builder.http1_only()
        }
        .build()
        .unwrap();
        let response = client.get(&url).send().await.unwrap();
        assert_eq!(
            response.version(),
            if h2 {
                Version::HTTP_2
            } else {
                Version::HTTP_11
            }
        );
        assert_eq!(response.text().await.unwrap(), "tls flower");
        assert!(
            client
                .get(format!("https://{address}/"))
                .send()
                .await
                .is_err(),
            "IP SAN mismatch must fail"
        );
        assert!(
            client
                .get(format!("http://{address}/"))
                .send()
                .await
                .is_err(),
            "configured client must not downgrade"
        );
    }
    assert!(
        reqwest::Client::builder()
            .no_proxy()
            .timeout(DEADLINE)
            .build()
            .unwrap()
            .get(&url)
            .send()
            .await
            .is_err(),
        "untrusted test CA must fail"
    );
    let mut stalled = TcpStream::connect(address).await.unwrap();
    let mut byte = [0];
    assert_eq!(
        timeout(DEADLINE, stalled.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0,
        "TLS handshake deadline releases silent connections"
    );
    stop.send(()).unwrap();
    timeout(DEADLINE, server).await.unwrap().unwrap().unwrap();
}
