//! Download orchestration and rate limiting
//!
//! This module provides the core download execution engine with comprehensive
//! error handling, retry logic, and rate limiting capabilities.
//!
//! # Overview
//!
//! The downloader orchestrates the complete download workflow:
//!
//! 1. **Job Creation**: Define what to download using [`job::DownloadJob`]
//! 2. **Execution**: Process the job using [`executor::DownloadExecutor`]
//! 3. **Rate Limiting**: Automatic throttling via [`rate_limit::RateLimiter`]
//! 4. **Progress Tracking**: Monitor status with [`job::JobStatus`] and [`job::JobProgress`]
//! 5. **Resume Support**: Automatic checkpointing for interrupted downloads
//!
//! # Quick Start
//!
//! ```no_run
//! use trading_data_downloader::downloader::{DownloadJob, DownloadExecutor};
//! use trading_data_downloader::identifier::ExchangeIdentifier;
//! use trading_data_downloader::Interval;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a download job
//! let id = ExchangeIdentifier::parse("BINANCE:BTC/USDT:USDT")?;
//! let job = DownloadJob::bars(
//!     id,
//!     "BTCUSDT".to_string(),
//!     Interval::OneHour,
//!     1640995200000, // 2022-01-01
//!     1672531200000, // 2023-01-01
//!     "./btc_bars.csv".into()
//! );
//!
//! // Execute with resume support
//! let executor = DownloadExecutor::with_resume("./resume_dir");
//! let result = executor.execute(job).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Components
//!
//! - [`executor`] - Main download executor with retry and resume logic
//! - [`job`] - Job specifications and status tracking
//! - [`rate_limit`] - Rate limiting implementation
//! - [`config`] - Configuration constants and backoff calculation
//!
//! # Error Handling
//!
//! All operations return `Result<T, DownloadError>`. Errors are categorized by type:
//! - Network errors (retried automatically)
//! - Rate limit errors (with backoff)
//! - IO errors (resume on next run)
//! - Validation errors (not retried)
//!
//! # Related Modules
//!
//! - [`crate::fetcher`] - Data fetching from exchanges
//! - [`crate::output`] - Output writing to files
//! - [`crate::resume`] - Resume state management

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
