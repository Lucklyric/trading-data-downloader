//! CLI error types and conversions

use crate::downloader::DownloadError;
use crate::fetcher::FetcherError;
use crate::identifier::IdentifierError;
use crate::output::OutputError;
use crate::registry::RegistryError;
use crate::resume::ResumeError;

/// CLI errors (T036)
#[derive(Debug, thiserror::Error)]
pub enum CliError {
    /// Identifier error
    #[error("identifier error: {0}")]
    IdentifierError(#[from] IdentifierError),

    /// Registry error
    #[error("registry error: {0}")]
    RegistryError(#[from] RegistryError),

    /// Download error
    #[error("download error: {0}")]
    DownloadError(#[from] DownloadError),

    /// Fetcher error
    #[error("fetcher error: {0}")]
    FetcherError(#[from] FetcherError),

    /// Output error
    #[error("output error: {0}")]
    OutputError(#[from] OutputError),

    /// Resume error
    #[error("resume error: {0}")]
    ResumeError(#[from] ResumeError),

    /// Invalid argument
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Configuration error
    #[error("configuration error: {0}")]
    ConfigurationError(String),
}
