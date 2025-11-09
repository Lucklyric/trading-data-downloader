//! Download orchestration and rate limiting

pub mod config;
pub mod executor;
pub mod job;
pub mod rate_limit;

pub use executor::DownloadExecutor;
pub use job::{DownloadJob, JobProgress, JobStatus, JobType};
pub use rate_limit::{RateLimitError, RateLimiter};

/// Download errors (T035)
#[derive(Debug, thiserror::Error)]
pub enum DownloadError {
    /// Rate limit error
    #[error("rate limit error: {0}")]
    RateLimitError(#[from] RateLimitError),

    /// Network error
    #[error("network error: {0}")]
    NetworkError(String),

    /// IO error
    #[error("IO error: {0}")]
    IoError(String),

    /// Parse error
    #[error("parse error: {0}")]
    ParseError(String),

    /// Validation error
    #[error("validation error: {0}")]
    ValidationError(String),

    /// Fetcher error
    #[error("fetcher error: {0}")]
    FetcherError(String),

    /// Output error
    #[error("output error: {0}")]
    OutputError(String),
}
