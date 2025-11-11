//! Data fetcher implementations for exchanges
//!
//! This module provides the core abstractions and implementations for fetching
//! trading data from various exchanges. Each exchange may have multiple fetchers
//! depending on the market type (e.g., spot, futures USDT-margined, futures COIN-margined).
//!
//! # Architecture
//!
//! The module is organized around the [`DataFetcher`] trait, which defines a
//! uniform interface for retrieving different types of trading data:
//!
//! - OHLCV bars via [`DataFetcher::fetch_bars_stream`]
//! - Aggregate trades via [`DataFetcher::fetch_aggtrades_stream`]
//! - Funding rates via [`DataFetcher::fetch_funding_stream`]
//! - Symbol metadata via [`DataFetcher::list_symbols`]
//!
//! # Supported Exchanges
//!
//! ## Binance Futures
//!
//! - **USDT-margined** ([`binance_futures_usdt`]): For USDT and BUSD settlement
//! - **COIN-margined** ([`binance_futures_coin`]): For BTC, ETH, and USD settlement
//!
//! # Factory Function
//!
//! Use [`create_fetcher`] to automatically route to the correct fetcher based on
//! an [`ExchangeIdentifier`]:
//!
//! ```no_run
//! use trading_data_downloader::identifier::ExchangeIdentifier;
//! use trading_data_downloader::fetcher::create_fetcher;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let id = ExchangeIdentifier::parse("BINANCE:BTC/USDT:USDT")?;
//! let fetcher = create_fetcher(&id)?;
//! // fetcher is BinanceFuturesUsdtFetcher
//! # Ok(())
//! # }
//! ```
//!
//! # Error Handling
//!
//! All fetcher operations return [`FetcherResult<T>`] which wraps [`FetcherError`].
//! Errors are categorized by type (HTTP, parse, API, rate limit, etc.) for appropriate
//! error handling and retry logic.
//!
//! # Related Modules
//!
//! - [`binance_http`] - HTTP client with retry and rate limiting
//! - [`binance_parser`] - Response parsing helpers
//! - [`binance_config`] - Market configuration and endpoints
//! - [`pagination`] - Pagination helpers for chunked requests
//! - [`archive`] - Historical archive download support

use crate::identifier::ExchangeIdentifier;
use crate::{AggTrade, Bar, FundingRate, Interval, Symbol};
use async_trait::async_trait;
use futures_util::Stream;
use std::pin::Pin;

pub mod archive;
pub mod binance_config;
pub mod binance_futures_coin;
pub mod binance_futures_usdt;
pub mod binance_http;
pub mod binance_parser;
pub mod pagination;
pub mod shared_resources;

/// Fetcher errors (T033)
#[derive(Debug, thiserror::Error)]
pub enum FetcherError {
    /// HTTP request error
    #[error("HTTP error: {0}")]
    HttpError(String),

    /// Response parse error
    #[error("parse error: {0}")]
    ParseError(String),

    /// API error response
    #[error("API error: {0}")]
    ApiError(String),

    /// Rate limit exceeded
    #[error("rate limit exceeded")]
    RateLimitExceeded,

    /// Invalid response
    #[error("invalid response: {0}")]
    InvalidResponse(String),

    /// Network error
    #[error("network error: {0}")]
    NetworkError(String),

    /// Archive error
    #[error("archive error: {0}")]
    ArchiveError(String),

    /// Checksum validation failed
    #[error("checksum validation failed: expected {expected}, got {actual}")]
    ChecksumMismatch {
        /// Expected checksum value
        expected: String,
        /// Actual checksum value
        actual: String
    },

    /// Unsupported settlement asset
    #[error("unsupported settlement asset: {0}")]
    UnsupportedAsset(String),

    /// Unsupported exchange
    #[error("unsupported exchange: {0}")]
    UnsupportedExchange(String),
}

/// Result type for fetcher operations
pub type FetcherResult<T> = Result<T, FetcherError>;

/// Stream of bars from a data fetcher
pub type BarStream = Pin<Box<dyn Stream<Item = FetcherResult<Bar>> + Send>>;

/// Stream of aggregate trades from a data fetcher
pub type AggTradeStream = Pin<Box<dyn Stream<Item = FetcherResult<AggTrade>> + Send>>;

/// Stream of funding rates from a data fetcher
pub type FundingStream = Pin<Box<dyn Stream<Item = FetcherResult<FundingRate>> + Send>>;

/// Data fetcher trait for retrieving OHLCV bars (T049)
#[async_trait]
pub trait DataFetcher: Send + Sync {
    /// Fetch OHLCV bars as a stream
    ///
    /// # Arguments
    /// * `symbol` - Trading symbol (e.g., "BTCUSDT")
    /// * `interval` - Time interval for bars
    /// * `start_time` - Start time (Unix timestamp in milliseconds)
    /// * `end_time` - End time (Unix timestamp in milliseconds)
    ///
    /// # Returns
    /// Stream of Bar results
    async fn fetch_bars_stream(
        &self,
        symbol: &str,
        interval: Interval,
        start_time: i64,
        end_time: i64,
    ) -> FetcherResult<BarStream>;

    /// List all tradable symbols from the exchange (T096)
    ///
    /// # Returns
    /// Vector of Symbol metadata
    async fn list_symbols(&self) -> FetcherResult<Vec<Symbol>>;

    /// Fetch aggregate trades as a stream (T114)
    ///
    /// # Arguments
    /// * `symbol` - Trading symbol (e.g., "BTCUSDT")
    /// * `start_time` - Start time (Unix timestamp in milliseconds)
    /// * `end_time` - End time (Unix timestamp in milliseconds)
    ///
    /// # Returns
    /// Stream of AggTrade results
    ///
    /// # Note
    /// Implementation must handle 1-hour window constraint by chunking requests
    async fn fetch_aggtrades_stream(
        &self,
        symbol: &str,
        start_time: i64,
        end_time: i64,
    ) -> FetcherResult<AggTradeStream>;

    /// Fetch funding rates as a stream (T138)
    ///
    /// # Arguments
    /// * `symbol` - Trading symbol (e.g., "BTCUSDT")
    /// * `start_time` - Start time (Unix timestamp in milliseconds)
    /// * `end_time` - End time (Unix timestamp in milliseconds)
    ///
    /// # Returns
    /// Stream of FundingRate results
    ///
    /// # Note
    /// Funding rates occur at 8-hour intervals (00:00, 08:00, 16:00 UTC)
    async fn fetch_funding_stream(
        &self,
        symbol: &str,
        start_time: i64,
        end_time: i64,
    ) -> FetcherResult<FundingStream>;

    /// Get the base URL for this fetcher
    fn base_url(&self) -> &str;
}

/// Create a fetcher based on the exchange identifier (T163-T165)
///
/// Routes to the appropriate fetcher implementation based on settlement asset:
/// - USDT/BUSD → Binance Futures USDT-margined (FAPI)
/// - BTC/ETH/USD → Binance Futures COIN-margined (DAPI)
///
/// # Arguments
/// * `identifier` - Parsed exchange identifier
///
/// # Returns
/// Boxed DataFetcher implementation
///
/// # Errors
/// Returns error if the settlement asset or exchange is not supported
pub fn create_fetcher(identifier: &ExchangeIdentifier) -> FetcherResult<Box<dyn DataFetcher>> {
    // Validate exchange
    if identifier.exchange() != "BINANCE" {
        return Err(FetcherError::UnsupportedExchange(
            identifier.exchange().to_string(),
        ));
    }

    // Route based on settlement asset
    match identifier.settle() {
        "USDT" | "BUSD" => Ok(Box::new(binance_futures_usdt::BinanceFuturesUsdtFetcher::new())),
        "BTC" | "ETH" | "USD" => Ok(Box::new(binance_futures_coin::BinanceFuturesCoinFetcher::new())),
        _ => Err(FetcherError::UnsupportedAsset(
            identifier.settle().to_string(),
        )),
    }
}
