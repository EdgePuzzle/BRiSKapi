//! What can go wrong: a recording refused, with the reason the archive tells its uploader, or a fault of ours.
use std::fmt;

#[derive(Debug)]
pub enum Failure {
    /// The recording breaks a rule; the message is the one `briskapi` shows (it is part of the API).
    Refused(String),
    /// Something else: a missing tool, a full disk.
    Fault(anyhow::Error),
}

pub type Res<T> = Result<T, Failure>;

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Failure::Refused(message) => f.write_str(message),
            Failure::Fault(error) => write!(f, "{error:#}"),
        }
    }
}

impl From<std::io::Error> for Failure {
    fn from(error: std::io::Error) -> Self {
        Failure::Fault(error.into())
    }
}

// A table that cannot be decoded is the uploader's, not ours.
impl From<parquet::errors::ParquetError> for Failure {
    fn from(error: parquet::errors::ParquetError) -> Self {
        Failure::Refused(format!("Unreadable table: {error}"))
    }
}

impl From<arrow_schema::ArrowError> for Failure {
    fn from(error: arrow_schema::ArrowError) -> Self {
        Failure::Refused(format!("Unreadable table: {error}"))
    }
}

pub fn refuse<T>(message: impl Into<String>) -> Res<T> {
    Err(Failure::Refused(message.into()))
}

/// `require!(condition, "message")` returns the refusal unless the condition holds.
#[macro_export]
macro_rules! require {
    ($condition:expr, $message:expr) => {
        if !($condition) {
            return $crate::error::refuse($message);
        }
    };
}
