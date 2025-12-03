//! Pagination helper module for Binance API requests (T212-T222)
//!
//! Provides unified pagination logic for all data types:
//! - Klines: startTime-based pagination
//! - AggTrades: fromId-based pagination with time window constraints
//! - Funding Rates: startTime-based pagination (typically single page)
//!
//! Includes safety mechanisms:
//! - Maximum iteration limits to prevent infinite loops
//! - Empty response detection
//! - Error handling and retry integration

use crate::fetcher::binance_http::BinanceHttpClient;
use crate::fetcher::{FetcherError, FetcherResult};
use crate::{AggTrade, Bar, FundingRate};
use std::future::Future;
use tracing::debug;

/// Maximum number of pagination iterations to prevent infinite loops (T222)
const MAX_ITERATIONS: usize = 10_000;

/// Default page limit for Binance API requests
const PAGE_LIMIT: usize = 1000;

/// Pagination helper for Binance API requests
pub struct PaginationHelper;

impl PaginationHelper {
    /// Paginate klines using startTime-based pagination (T214)
    ///
    /// # Arguments
    /// * `http_client` - HTTP client for API requests
    /// * `endpoint` - API endpoint path (e.g., "/fapi/v1/klines")
    /// * `symbol` - Trading symbol
    /// * `interval` - Time interval (e.g., "1m", "1h")
    /// * `start_time` - Start timestamp in milliseconds
    /// * `end_time` - End timestamp in milliseconds
    /// * `fetch_fn` - Async function to fetch a single page
    ///
    /// # Returns
    /// All bars across all pages
    ///
    /// # Errors
    /// Returns error if max iterations exceeded or fetch fails
    pub async fn paginate_klines<F, Fut>(
        http_client: &BinanceHttpClient,
        endpoint: &str,
        symbol: &str,
        interval: &str,
        start_time: i64,
        end_time: i64,
        fetch_fn: F,
    ) -> FetcherResult<Vec<Bar>>
    where
        F: for<'a> Fn(&'a BinanceHttpClient, &'a str, &'a [(&'a str, String)]) -> Fut,
        Fut: Future<Output = FetcherResult<Vec<Bar>>> + Send,
    {
        let mut all_bars = Vec::new();
        let mut current_start = start_time;
        let mut iteration = 0;

        loop {
            // Safety check: prevent infinite loops (T222)
            if iteration >= MAX_ITERATIONS {
                return Err(FetcherError::ApiError(format!(
                        "Max iterations ({MAX_ITERATIONS}) exceeded for symbol {symbol} - possible infinite loop. Last timestamp: {current_start}"
                    )));
            }

            // Check if we've reached the end
            if current_start >= end_time {
                debug!(
                    "Pagination complete: reached end_time. Total bars: {}",
                    all_bars.len()
                );
                break;
            }

            // Build parameters for this page
            let params = [
                ("symbol", symbol.to_string()),
                ("interval", interval.to_string()),
                ("startTime", current_start.to_string()),
                ("endTime", end_time.to_string()),
                ("limit", PAGE_LIMIT.to_string()),
            ];

            debug!(
                "Fetching klines page {} for {} from {} to {}",
                iteration + 1,
                symbol,
                current_start,
                end_time
            );

            // Fetch page
            let page = fetch_fn(http_client, endpoint, &params).await?;

            // Empty response handling (T220)
            if page.is_empty() {
                debug!(
                    "Empty page received at iteration {}. Total bars collected: {}",
                    iteration + 1,
                    all_bars.len()
                );
                break;
            }

            debug!("Received {} bars in page {}", page.len(), iteration + 1);

            // Get last bar's close_time to advance pagination
            // SAFETY: unwrap() is safe because we break early on empty page (line 100)
            let last_close_time = page.last().unwrap().close_time;

            // Extend results
            all_bars.extend(page);

            // Advance to next page: start from (last_close_time + 1)
            current_start = last_close_time + 1;

            iteration += 1;
        }

        debug!(
            "Pagination completed after {} iterations. Total bars: {}",
            iteration,
            all_bars.len()
        );

        Ok(all_bars)
    }

    /// Paginate aggTrades using fromId-based pagination (T217)
    ///
    /// # Arguments
    /// * `http_client` - HTTP client for API requests
    /// * `endpoint` - API endpoint path (e.g., "/fapi/v1/aggTrades")
    /// * `symbol` - Trading symbol
    /// * `start_time` - Start timestamp in milliseconds
    /// * `end_time` - End timestamp in milliseconds
    /// * `from_id` - Optional starting trade ID for pagination
    /// * `fetch_fn` - Async function to fetch a single page
    ///
    /// # Returns
    /// All aggregate trades across all pages
    ///
    /// # Errors
    /// Returns error if max iterations exceeded or fetch fails
    pub async fn paginate_aggtrades<F, Fut>(
        http_client: &BinanceHttpClient,
        endpoint: &str,
        symbol: &str,
        start_time: i64,
        end_time: i64,
        from_id: Option<i64>,
        fetch_fn: F,
    ) -> FetcherResult<Vec<AggTrade>>
    where
        F: for<'a> Fn(&'a BinanceHttpClient, &'a str, &'a [(&'a str, String)]) -> Fut,
        Fut: Future<Output = FetcherResult<Vec<AggTrade>>> + Send,
    {
        let mut all_trades = Vec::new();
        let mut current_from_id = from_id;
        let mut iteration = 0;

        loop {
            // Safety check: prevent infinite loops (T222)
            if iteration >= MAX_ITERATIONS {
                return Err(FetcherError::ApiError(format!(
                        "Max iterations ({MAX_ITERATIONS}) exceeded for symbol {symbol} - possible infinite loop. Last trade ID: {current_from_id:?}"
                    )));
            }

            // Build parameters for this page
            let mut params = vec![
                ("symbol", symbol.to_string()),
                ("startTime", start_time.to_string()),
                ("endTime", end_time.to_string()),
                ("limit", PAGE_LIMIT.to_string()),
            ];

            // Add fromId if available (takes precedence over time-based pagination)
            if let Some(id) = current_from_id {
                params.push(("fromId", id.to_string()));
            }

            debug!(
                "Fetching aggTrades page {} for {} from_id={:?}",
                iteration + 1,
                symbol,
                current_from_id
            );

            // Fetch page
            let page = fetch_fn(http_client, endpoint, &params).await?;

            // Empty response handling (T220)
            if page.is_empty() {
                debug!(
                    "Empty page received at iteration {}. Total trades collected: {}",
                    iteration + 1,
                    all_trades.len()
                );
                break;
            }

            debug!("Received {} trades in page {}", page.len(), iteration + 1);

            // Get last trade's ID to advance pagination
            // SAFETY: unwrap() is safe because we break early on empty page (line 197)
            let last_trade_id = page.last().unwrap().agg_trade_id;

            // Extend results
            all_trades.extend(page);

            // Advance to next page: start from (last_trade_id + 1)
            current_from_id = Some(last_trade_id + 1);

            iteration += 1;
        }

        debug!(
            "Pagination completed after {} iterations. Total trades: {}",
            iteration,
            all_trades.len()
        );

        Ok(all_trades)
    }

    /// Paginate funding rates using startTime-based pagination (T219)
    ///
    /// # Arguments
    /// * `http_client` - HTTP client for API requests
    /// * `endpoint` - API endpoint path (e.g., "/fapi/v1/fundingRate")
    /// * `symbol` - Trading symbol
    /// * `start_time` - Start timestamp in milliseconds
    /// * `end_time` - End timestamp in milliseconds
    /// * `fetch_fn` - Async function to fetch a single page
    ///
    /// # Returns
    /// All funding rates across all pages
    ///
    /// # Errors
    /// Returns error if max iterations exceeded or fetch fails
    pub async fn paginate_funding_rates<F, Fut>(
        http_client: &BinanceHttpClient,
        endpoint: &str,
        symbol: &str,
        start_time: i64,
        end_time: i64,
        fetch_fn: F,
    ) -> FetcherResult<Vec<FundingRate>>
    where
        F: for<'a> Fn(&'a BinanceHttpClient, &'a str, &'a [(&'a str, String)]) -> Fut,
        Fut: Future<Output = FetcherResult<Vec<FundingRate>>> + Send,
    {
        let mut all_rates = Vec::new();
        let mut current_start = start_time;
        let mut iteration = 0;

        loop {
            // Safety check: prevent infinite loops (T222)
            if iteration >= MAX_ITERATIONS {
                return Err(FetcherError::ApiError(format!(
                        "Max iterations ({MAX_ITERATIONS}) exceeded for symbol {symbol} - possible infinite loop. Last timestamp: {current_start}"
                    )));
            }

            // Check if we've reached the end
            if current_start >= end_time {
                debug!(
                    "Pagination complete: reached end_time. Total funding rates: {}",
                    all_rates.len()
                );
                break;
            }

            // Build parameters for this page
            let params = [
                ("symbol", symbol.to_string()),
                ("startTime", current_start.to_string()),
                ("endTime", end_time.to_string()),
                ("limit", PAGE_LIMIT.to_string()),
            ];

            debug!(
                "Fetching funding rates page {} for {} from {} to {}",
                iteration + 1,
                symbol,
                current_start,
                end_time
            );

            // Fetch page
            let page = fetch_fn(http_client, endpoint, &params).await?;

            // Empty response handling (T220)
            if page.is_empty() {
                debug!(
                    "Empty page received at iteration {}. Total rates collected: {}",
                    iteration + 1,
                    all_rates.len()
                );
                break;
            }

            debug!(
                "Received {} funding rates in page {}",
                page.len(),
                iteration + 1
            );

            // Get last funding_time to advance pagination
            // SAFETY: unwrap() is safe because we break early on empty page (line 297)
            let last_funding_time = page.last().unwrap().funding_time;

            // Extend results
            all_rates.extend(page);

            // Advance to next page: start from (last_funding_time + 1)
            current_start = last_funding_time + 1;

            iteration += 1;
        }

        debug!(
            "Pagination completed after {} iterations. Total funding rates: {}",
            iteration,
            all_rates.len()
        );

        Ok(all_rates)
    }
}
