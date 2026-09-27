//! Bound aggregate incoming bodies before JSON decoding. These byte leases do
//! not acquire evaluation slots: peer replication must progress while an
//! admitted writer waits for quorum. The handler retains the raw-byte reservation
//! until response headers, including route discovery and forwarding waits. Local
//! evaluation separately charges its decoded input; this overlap is conservative.
use super::*;
use axum::{
    body::{Body, Bytes},
    extract::{Request, State},
    http::{Method, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;
use std::time::Duration;

#[derive(Clone)]
pub(in crate::service) struct Budget {
    pool: Arc<Pool>,
    peer_token: String,
    operator_token: String,
    http_limit: usize,
    rpc_limit: usize,
    timeout: Duration,
}
impl Budget {
    pub(in crate::service) fn new(app: &App) -> Self {
        Self {
            pool: app.admission.clone(),
            peer_token: app.consensus.peer_token().into(),
            operator_token: app.admin_token.clone(),
            http_limit: tuning::settings()
                .expect("validated HTTP budget")
                .http_max_body_bytes,
            rpc_limit: app.consensus.limits().rpc_max_bytes,
            timeout: app.consensus.limits().read_timeout,
        }
    }
    fn classify(&self, request: &Request) -> Result<(Class, usize), ApiError> {
        let path = request.uri().path();
        let raft = path.starts_with("/raft/");
        let operator = path.starts_with("/admin/")
            || path.contains("/admin/")
            || matches!(
                path,
                "/raft/initialize" | "/raft/membership" | "/raft/metrics"
            );
        if raft || operator {
            let expected = if operator {
                &self.operator_token
            } else {
                &self.peer_token
            };
            let token = request
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.strip_prefix("Bearer "));
            if token != Some(expected.as_str()) {
                return Err(ApiError::new(
                    StatusCode::UNAUTHORIZED,
                    "UNAUTHORIZED",
                    "valid control-plane bearer token required".into(),
                ));
            }
            Ok((
                Class::Control,
                if raft {
                    self.rpc_limit
                } else {
                    self.http_limit
                },
            ))
        } else {
            Ok((Class::User, self.http_limit))
        }
    }
}

struct Buffered {
    bytes: Vec<u8>,
    _retained: Arc<Input>,
}
impl AsRef<[u8]> for Buffered {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

fn too_large(limit: usize) -> ApiError {
    ApiError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        "BODY_TOO_LARGE",
        format!("request body exceeds configured {limit}-byte transport limit"),
    )
}

async fn collect(
    pool: &Arc<Pool>,
    class: Class,
    limit: usize,
    body: Body,
) -> Result<(Bytes, Arc<Input>), ApiError> {
    // Reserve a waiting request even before its first body chunk arrives.
    let mut retained = pool.retain(class, input_bytes(&Value::Null))?;
    let mut bytes = Vec::new();
    let mut chunks = body.into_data_stream();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(|error| {
            ApiError::new(StatusCode::BAD_REQUEST, "INVALID_BODY", error.to_string())
        })?;
        let needed = bytes
            .len()
            .checked_add(chunk.len())
            .filter(|size| *size <= limit)
            .ok_or_else(|| too_large(limit))?;
        if needed > bytes.capacity() {
            let capacity = needed.max(bytes.capacity().saturating_mul(2)).min(limit);
            retained.grow(capacity - bytes.capacity())?;
            bytes
                .try_reserve_exact(capacity - bytes.len())
                .map_err(|_| overloaded("request body allocation failed"))?;
            if bytes.capacity() > capacity {
                retained.grow(bytes.capacity() - capacity)?;
            }
        }
        bytes.extend_from_slice(&chunk);
    }
    let retained = Arc::new(retained);
    Ok((
        Bytes::from_owner(Buffered {
            bytes,
            _retained: retained.clone(),
        }),
        retained,
    ))
}

pub(in crate::service) async fn handle(
    State(budget): State<Budget>,
    request: Request,
    next: Next,
) -> Response {
    if request.method() != Method::POST {
        return next.run(request).await;
    }
    let (class, limit) = match budget.classify(&request) {
        Ok(value) => value,
        Err(error) => return error.into_response(),
    };
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<u64>().ok())
        .is_some_and(|length| length > limit as u64)
    {
        return too_large(limit).into_response();
    }
    let (parts, body) = request.into_parts();
    let body = crate::telemetry::http_stage("body_receive", async {
        tokio::time::timeout(budget.timeout, collect(&budget.pool, class, limit, body))
            .await
            .map_err(|_| {
                ApiError::new(
                    StatusCode::REQUEST_TIMEOUT,
                    "BODY_TIMEOUT",
                    "request body exceeded FLOWER_READ_TIMEOUT_MS".into(),
                )
            })?
    })
    .await;
    match body {
        Ok((bytes, _retained)) => {
            next.run(Request::from_parts(parts, Body::from(bytes)))
                .await
        }
        Err(error) => error.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pool() -> Arc<Pool> {
        Pool::new([1, 1], [1, 1], [1024, 1024], 1)
    }

    #[tokio::test]
    async fn raw_buffer_retention_does_not_consume_worker_slots() {
        let pool = pool();
        let (bytes, retained) = collect(&pool, Class::User, 16, Body::from("12345678"))
            .await
            .unwrap();
        drop(retained);
        assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 520);
        assert_eq!(pool.metrics()["classes"][0]["active"], 0);
        let clone = bytes.clone();
        drop(bytes);
        assert!(
            collect(&pool, Class::User, 16, Body::from("123456789"))
                .await
                .is_err()
        );
        assert!(
            collect(&pool, Class::Control, 16, Body::from("123456789"))
                .await
                .is_ok()
        );
        drop(clone);
        assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 0);
        assert!(
            collect(&pool, Class::User, 16, Body::from("123456789"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn chunked_overflow_and_cancellation_release_buffers() {
        let pool = pool();
        let body = Body::from_stream(futures_util::stream::iter([
            Ok::<_, std::io::Error>(Bytes::from_static(b"12345678")),
            Ok(Bytes::from_static(b"123456789")),
        ]));
        assert_eq!(
            collect(&pool, Class::User, 16, body)
                .await
                .err()
                .unwrap()
                .status,
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 0);
        let mut empty = Box::pin(collect(
            &pool,
            Class::User,
            16,
            Body::from_stream(futures_util::stream::pending::<Result<Bytes, std::io::Error>>()),
        ));
        assert!(futures_util::poll!(&mut empty).is_pending());
        assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 512);
        drop(empty);
        let stream =
            futures_util::stream::iter([Ok::<_, std::io::Error>(Bytes::from_static(b"12345678"))])
                .chain(futures_util::stream::pending());
        let mut collecting = Box::pin(collect(&pool, Class::User, 16, Body::from_stream(stream)));
        assert!(futures_util::poll!(&mut collecting).is_pending());
        assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 520);
        drop(collecting);
        assert_eq!(pool.metrics()["classes"][0]["retainedInputBytes"], 0);
    }

    #[test]
    fn reserved_control_budget_requires_the_correct_authenticated_scope() {
        let budget = Budget {
            pool: pool(),
            peer_token: "peer".into(),
            operator_token: "operator".into(),
            http_limit: 8,
            rpc_limit: 16,
            timeout: Duration::from_secs(1),
        };
        for (path, token, class) in [
            ("/v1/query", None, Some(Class::User)),
            ("/admin/keys", None, None),
            ("/admin/keys", Some("peer"), None),
            ("/admin/keys", Some("operator"), Some(Class::Control)),
            ("/raft/append", Some("operator"), None),
            ("/raft/append", Some("peer"), Some(Class::Control)),
            ("/raft/membership", Some("operator"), Some(Class::Control)),
            (
                "/partitions/tenant/admin/deploy",
                Some("operator"),
                Some(Class::Control),
            ),
        ] {
            let mut request = Request::builder().method(Method::POST).uri(path);
            if let Some(token) = token {
                request = request.header(header::AUTHORIZATION, format!("Bearer {token}"));
            }
            assert_eq!(
                budget
                    .classify(&request.body(Body::empty()).unwrap())
                    .ok()
                    .map(|value| value.0),
                class,
                "{path}"
            );
        }
    }
}
