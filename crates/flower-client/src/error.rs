//! `FlowerError` and `is_transient`, the twins of `sdk/client.ts`'s `FlowerError`/`isTransient`.

use std::error::Error as StdError;
use std::fmt;
use std::io;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::json;

/// A method's own failure: `fail(code, message, details)` or a runtime evaluation error.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Failure {
    pub code: String,
    pub message: String,
    /// `Some(Value::Null)` when the server sent `"details": null`, `None` when it sent none.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "present"
    )]
    pub details: Option<Value>,
}

fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

impl Failure {
    /// `failureOf`: an object with string `code` and `message`, else nothing.
    pub fn from_value(value: Option<&Value>) -> Option<Failure> {
        let object = value?.as_object()?;
        let code = object.get("code")?.as_str()?;
        let message = object.get("message")?.as_str()?;
        Some(Failure {
            code: code.to_owned(),
            message: message.to_owned(),
            details: object.get("details").cloned(),
        })
    }
}

/// What produced an error. The TS SDK tells these apart by class (`FlowerError`, `DOMException`,
/// Node errors with a `code`); Rust has one type, so the kind carries that distinction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// A `FlowerError` in the TS sense: an HTTP error answer, a watch error event, or an SDK-level
    /// condition with status 0 (`WATCH_STALLED`, `WATCH_ENDED`, `WATCH_PROTOCOL_ERROR`, ...).
    Flower,
    /// A deadline passed (TS: `DOMException` `TimeoutError`). Transient.
    Timeout,
    /// The caller cancelled (TS: `AbortError` or the signal's reason). Never transient.
    Aborted,
    /// Network or HTTP/2 trouble, with a Node-style `code` (`ECONNREFUSED`, `ERR_HTTP2_GOAWAY_SESSION`,
    /// `H2_RESPONSE_TOO_LARGE`, ...). Transient exactly when the TS code regex says so.
    Transport,
    /// A reply that could not be decoded into the requested type. Never transient.
    Decode,
    /// A bad argument detected before sending (TS: `TypeError`). Never transient.
    Invalid,
}

/// The error of every client operation. `status` is the HTTP status (0 when there was none),
/// `code` the server's `error.code` (or an SDK/transport code), `failure` the method's own failure.
#[derive(Clone)]
pub struct FlowerError {
    pub status: u16,
    pub code: String,
    pub message: String,
    /// Boxed to keep `Result<_, FlowerError>` small.
    pub failure: Option<Box<Failure>>,
    pub kind: ErrorKind,
    source: Option<Arc<dyn StdError + Send + Sync>>,
}

/// Node's message for `AbortSignal.timeout`.
pub const TIMEOUT_MESSAGE: &str = "The operation was aborted due to timeout";
/// Node's message for a bare `AbortController.abort()`.
pub const ABORT_MESSAGE: &str = "This operation was aborted";

impl FlowerError {
    /// `new FlowerError(message, status, code)`: kind [`ErrorKind::Flower`].
    pub fn new(message: impl Into<String>, status: u16, code: impl Into<String>) -> Self {
        FlowerError {
            status,
            code: code.into(),
            message: message.into(),
            failure: None,
            kind: ErrorKind::Flower,
            source: None,
        }
    }

    pub fn with_failure(mut self, failure: Option<Failure>) -> Self {
        self.failure = failure.map(Box::new);
        self
    }

    pub fn with_source(mut self, source: impl StdError + Send + Sync + 'static) -> Self {
        self.source = Some(Arc::new(source));
        self
    }

    fn of_kind(kind: ErrorKind, code: &str, message: impl Into<String>) -> Self {
        FlowerError {
            kind,
            ..FlowerError::new(message, 0, code)
        }
    }

    /// A deadline passed: transient.
    pub fn timeout() -> Self {
        Self::of_kind(ErrorKind::Timeout, "TIMEOUT", TIMEOUT_MESSAGE)
    }

    /// The caller cancelled: never transient.
    pub fn aborted() -> Self {
        Self::of_kind(ErrorKind::Aborted, "ABORTED", ABORT_MESSAGE)
    }

    /// A transport failure with a Node-style code (see [`transient_code`]).
    pub fn transport(code: impl Into<String>, message: impl Into<String>) -> Self {
        FlowerError {
            kind: ErrorKind::Transport,
            ..FlowerError::new(message, 0, code)
        }
    }

    /// A reply that did not decode. `status` is the reply's HTTP status.
    pub fn decode(message: impl Into<String>, status: u16) -> Self {
        FlowerError {
            kind: ErrorKind::Decode,
            ..FlowerError::new(message, status, "DECODE_ERROR")
        }
    }

    /// A bad argument (TS: `TypeError`).
    pub fn invalid(message: impl Into<String>) -> Self {
        Self::of_kind(ErrorKind::Invalid, "TYPE_ERROR", message)
    }

    /// Network failures, timeouts, stalls, 408, 425, 429 and 5xx; never a method's own failure.
    pub fn is_transient(&self) -> bool {
        match self.kind {
            ErrorKind::Flower => {
                if self.failure.is_some() {
                    false
                } else if self.status == 0 {
                    self.code == "WATCH_STALLED" || self.code == "WATCH_ENDED"
                } else {
                    matches!(self.status, 408 | 425 | 429) || self.status >= 500
                }
            }
            ErrorKind::Timeout => true,
            ErrorKind::Transport => transient_code(&self.code),
            ErrorKind::Aborted | ErrorKind::Decode | ErrorKind::Invalid => false,
        }
    }

    pub fn is_timeout(&self) -> bool {
        self.kind == ErrorKind::Timeout
    }

    pub fn is_aborted(&self) -> bool {
        self.kind == ErrorKind::Aborted
    }

    /// `failure.code`, when the method failed.
    pub fn failure_code(&self) -> Option<&str> {
        self.failure.as_ref().map(|failure| failure.code.as_str())
    }

    /// The error of a non-2xx unary reply (`Connection.request` in `client.ts`).
    pub fn from_response(status: u16, status_text: &str, body: &[u8]) -> Self {
        let text = String::from_utf8_lossy(body);
        let data = parse_body(&text);
        let error = field(data.as_ref(), "error");
        let message = match error {
            Some(Value::String(message)) => message.clone(),
            _ => field(error, "message")
                .or_else(|| field(data.as_ref(), "message"))
                .map(js_string)
                .unwrap_or_else(|| text_or(&text, status_text)),
        };
        let code = field(error, "code")
            .or_else(|| field(data.as_ref(), "code"))
            .map(js_string)
            .unwrap_or_else(|| "HTTP_ERROR".to_owned());
        FlowerError::new(message, status, code)
            .with_failure(Failure::from_value(field(error, "failure")))
    }

    /// The error of a non-2xx watch reply (`deltas` in `client.ts`), which reads fewer fallbacks.
    pub fn from_watch_response(status: u16, status_text: &str, body: &[u8]) -> Self {
        let text = String::from_utf8_lossy(body);
        let data = parse_body(&text);
        let error = field(data.as_ref(), "error");
        let message = field(error, "message")
            .map(js_string)
            .unwrap_or_else(|| text_or(&text, status_text));
        let code = field(error, "code")
            .map(js_string)
            .unwrap_or_else(|| "HTTP_ERROR".to_owned());
        FlowerError::new(message, status, code)
            .with_failure(Failure::from_value(field(error, "failure")))
    }

    /// Map a reqwest/hyper/h2/io error onto the Node error codes `isTransient` knows.
    pub fn from_reqwest(error: reqwest::Error) -> Self {
        if error.is_timeout() {
            return FlowerError::timeout().with_source(error);
        }
        if error.is_builder() {
            let message = chain(&error);
            return FlowerError::invalid(message).with_source(error);
        }
        let (code, detail) = classify(&error);
        let message = detail.unwrap_or_else(|| format!("fetch failed: {}", chain(&error)));
        FlowerError::transport(code, message).with_source(error)
    }

    /// Map an I/O error (for example from a custom transport) onto a Node error code.
    pub fn from_io(error: io::Error) -> Self {
        let code = io_code(&error).unwrap_or("UND_ERR_SOCKET");
        FlowerError::transport(code, error.to_string()).with_source(error)
    }
}

/// `^(?:H2_|UND_ERR_|ERR_HTTP2_|ECONNRESET$|...)` minus the three permanent H2 codes.
pub fn transient_code(code: &str) -> bool {
    let transient = code.starts_with("H2_")
        || code.starts_with("UND_ERR_")
        || code.starts_with("ERR_HTTP2_")
        || matches!(
            code,
            "ECONNRESET"
                | "ECONNREFUSED"
                | "ECONNABORTED"
                | "EPIPE"
                | "ETIMEDOUT"
                | "EHOSTUNREACH"
                | "ENETUNREACH"
                | "ENETDOWN"
                | "EAI_AGAIN"
        );
    transient
        && !matches!(
            code,
            "H2_REQUEST_TOO_LARGE" | "H2_UNSUPPORTED_ENCODING" | "H2_RESPONSE_TOO_LARGE"
        )
}

fn parse_body(text: &str) -> Option<Value> {
    if text.is_empty() {
        None
    } else {
        json::from_slice_deep::<Value>(text.as_bytes()).ok()
    }
}

fn field<'a>(value: Option<&'a Value>, name: &str) -> Option<&'a Value> {
    match value?.as_object()?.get(name)? {
        Value::Null => None,
        found => Some(found),
    }
}

fn text_or(text: &str, status_text: &str) -> String {
    if text.is_empty() {
        status_text.to_owned()
    } else {
        text.to_owned()
    }
}

/// JavaScript's `String(value)` for what `new Error(value)` would store.
fn js_string(value: &Value) -> String {
    match value {
        Value::Null => "null".to_owned(),
        Value::Bool(value) => value.to_string(),
        Value::Number(_) => json::canonical_json(value),
        Value::String(value) => value.clone(),
        Value::Array(values) => values
            .iter()
            .map(|value| match value {
                Value::Null => String::new(),
                other => js_string(other),
            })
            .collect::<Vec<_>>()
            .join(","),
        Value::Object(_) => "[object Object]".to_owned(),
    }
}

fn chain(error: &(dyn StdError + 'static)) -> String {
    let mut message = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let text = cause.to_string();
        if !message.contains(&text) {
            message.push_str(": ");
            message.push_str(&text);
        }
        source = cause.source();
    }
    message
}

fn io_code(error: &io::Error) -> Option<&'static str> {
    use io::ErrorKind::*;
    Some(match error.kind() {
        ConnectionRefused => "ECONNREFUSED",
        ConnectionReset => "ECONNRESET",
        ConnectionAborted => "ECONNABORTED",
        BrokenPipe => "EPIPE",
        TimedOut => "ETIMEDOUT",
        HostUnreachable => "EHOSTUNREACH",
        NetworkUnreachable => "ENETUNREACH",
        NetworkDown => "ENETDOWN",
        NotConnected => "ENOTCONN",
        AddrNotAvailable => "EADDRNOTAVAIL",
        UnexpectedEof => "UND_ERR_SOCKET",
        InvalidData => "ERR_TLS_CERT_INVALID",
        _ => {
            let text = error.to_string();
            if text.contains("lookup address") || text.contains("dns error") {
                if text.contains("emporary") || text.contains("try again") {
                    "EAI_AGAIN"
                } else {
                    "ENOTFOUND"
                }
            } else {
                return None;
            }
        }
    })
}

fn h2_code(error: &h2::Error) -> (&'static str, Option<String>) {
    if error.is_go_away() {
        return ("ERR_HTTP2_GOAWAY_SESSION", None);
    }
    if let Some(io) = error.get_io() {
        return (io_code(io).unwrap_or("ERR_HTTP2_ERROR"), None);
    }
    match error.reason() {
        Some(reason) => (
            "ERR_HTTP2_STREAM_ERROR",
            Some(format!(
                "Stream closed with error code {}",
                nghttp2_name(reason)
            )),
        ),
        None => ("ERR_HTTP2_ERROR", None),
    }
}

fn nghttp2_name(reason: h2::Reason) -> String {
    let name = match reason {
        h2::Reason::NO_ERROR => "NO_ERROR",
        h2::Reason::PROTOCOL_ERROR => "PROTOCOL_ERROR",
        h2::Reason::INTERNAL_ERROR => "INTERNAL_ERROR",
        h2::Reason::FLOW_CONTROL_ERROR => "FLOW_CONTROL_ERROR",
        h2::Reason::SETTINGS_TIMEOUT => "SETTINGS_TIMEOUT",
        h2::Reason::STREAM_CLOSED => "STREAM_CLOSED",
        h2::Reason::FRAME_SIZE_ERROR => "FRAME_SIZE_ERROR",
        h2::Reason::REFUSED_STREAM => "REFUSED_STREAM",
        h2::Reason::CANCEL => "CANCEL",
        h2::Reason::COMPRESSION_ERROR => "COMPRESSION_ERROR",
        h2::Reason::CONNECT_ERROR => "CONNECT_ERROR",
        h2::Reason::ENHANCE_YOUR_CALM => "ENHANCE_YOUR_CALM",
        h2::Reason::INADEQUATE_SECURITY => "INADEQUATE_SECURITY",
        h2::Reason::HTTP_1_1_REQUIRED => "HTTP_1_1_REQUIRED",
        other => return format!("{}", u32::from(other)),
    };
    format!("NGHTTP2_{name}")
}

/// The Node-style code for a reqwest error, walking its source chain.
fn classify(error: &reqwest::Error) -> (String, Option<String>) {
    let mut source: Option<&(dyn StdError + 'static)> = Some(error);
    let mut io_fallback = None;
    while let Some(cause) = source {
        if let Some(h2) = cause.downcast_ref::<h2::Error>() {
            let (code, detail) = h2_code(h2);
            return (code.to_owned(), detail);
        }
        if let Some(io) = cause.downcast_ref::<io::Error>() {
            if let Some(code) = io_code(io) {
                io_fallback.get_or_insert(code);
            }
            // `io::Error::source` skips the wrapped error itself (an h2 or rustls error, or
            // another io::Error), so descend into it explicitly.
            if let Some(inner) = io.get_ref() {
                source = Some(inner as &(dyn StdError + 'static));
                continue;
            }
        }
        if let Some(hyper) = cause.downcast_ref::<hyper::Error>() {
            if hyper.is_timeout() {
                return ("ETIMEDOUT".to_owned(), None);
            }
            if hyper.is_parse() {
                return ("HPE_INVALID_CONSTANT".to_owned(), None);
            }
            if hyper.is_user() {
                return ("ERR_INVALID_ARG_VALUE".to_owned(), None);
            }
            if io_fallback.is_none() && cause.source().is_none() {
                let code = if hyper.is_canceled() {
                    "ERR_HTTP2_STREAM_CANCEL"
                } else if hyper.is_closed() {
                    "UND_ERR_CLOSED"
                } else {
                    "UND_ERR_SOCKET"
                };
                return (code.to_owned(), None);
            }
        }
        source = cause.source();
    }
    if let Some(code) = io_fallback {
        return (code.to_owned(), None);
    }
    let code = if error.is_connect() {
        "UND_ERR_CONNECT"
    } else {
        "UND_ERR_SOCKET"
    };
    (code.to_owned(), None)
}

impl fmt::Display for FlowerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl fmt::Debug for FlowerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut debug = f.debug_struct("FlowerError");
        debug
            .field("status", &self.status)
            .field("code", &self.code)
            .field("message", &self.message)
            .field("kind", &self.kind);
        if let Some(failure) = &self.failure {
            debug.field("failure", failure);
        }
        if let Some(source) = &self.source {
            debug.field("source", source);
        }
        debug.finish()
    }
}

impl StdError for FlowerError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

impl From<reqwest::Error> for FlowerError {
    fn from(error: reqwest::Error) -> Self {
        FlowerError::from_reqwest(error)
    }
}
