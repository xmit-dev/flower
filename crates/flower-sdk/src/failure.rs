use crate::Value;
use alloc::string::String;

/// A structured failure. Callers receive its code, message and details intact.
#[derive(Clone, Debug, PartialEq)]
pub struct Failure {
    pub code: String,
    pub message: String,
    pub details: Option<Value>,
}

pub type Result<T, E = Failure> = core::result::Result<T, E>;

impl Failure {
    pub fn new(code: &str, message: impl Into<String>) -> Self {
        debug_assert!(
            code.bytes().next().is_some_and(|c| c.is_ascii_uppercase())
                && code
                    .bytes()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == b'_'),
            "Failure codes use UPPER_SNAKE_CASE"
        );
        Failure {
            code: code.into(),
            message: message.into(),
            details: None,
        }
    }
    pub fn with_details(mut self, details: Value) -> Self {
        self.details = Some(details);
        self
    }
}

/// Abort with a structured failure: `fail("OUT_OF_STOCK", "…")?`.
pub fn fail<T>(code: &str, message: impl Into<String>) -> Result<T> {
    Err(Failure::new(code, message))
}

/// Abort with a failure carrying JSON details.
pub fn fail_with<T>(code: &str, message: impl Into<String>, details: Value) -> Result<T> {
    Err(Failure::new(code, message).with_details(details))
}

/// A programming error, as a thrown JavaScript TypeError reaches callers.
pub fn type_error<T>(message: impl Into<String>) -> Result<T> {
    fail("COMPUTE_ERROR", message)
}
