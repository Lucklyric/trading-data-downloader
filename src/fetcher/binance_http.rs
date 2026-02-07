//! Binance HTTP client helper module (T198-T202)
//!
//! Provides unified HTTP client for all Binance API interactions with:
//! - Generic request/response handling
//! - Rate limit integration
//! - Retry logic with exponential backoff
//! - Weight header parsing

use reqwest::Client;
use serde::de::DeserializeOwned;
use std::{sync::Arc, time::Duration};
use tracing::{debug, error, info, warn};

use crate::downloader::config::calculate_backoff;
use crate::downloader::rate_limit::RateLimiter;
use crate::fetcher::retry_formatter::{extract_error_type, RetryContext, RetryErrorType};
use crate::fetcher::{FetcherError, FetcherResult};
use crate::shutdown::{self, SharedShutdown};

/// Unified HTTP client for all Binance API interactions (T198)
pub struct BinanceHttpClient {
    client: Arc<Client>,
    base_url: String,
    rate_limiter: Arc<RateLimiter>,
    shutdown: Option<SharedShutdown>,
    max_retries: u32,
}

impl BinanceHttpClient {
    /// Create new HTTP client (T199)
    ///
    /// # Arguments
    /// * `client` - Shared HTTP client (Arc for cheap cloning)
    /// * `base_url` - Base URL for API endpoints (e.g., "<https://fapi.binance.com>")
    /// * `rate_limiter` - Shared rate limiter (Arc for global quota enforcement)
    /// * `max_retries` - Maximum number of retry attempts for failed requests
    pub fn new(
        client: impl Into<Arc<Client>>,
        base_url: impl Into<String>,
        rate_limiter: Arc<RateLimiter>,
        max_retries: u32,
    ) -> Self {
        Self {
            client: client.into(),
            base_url: base_url.into(),
            rate_limiter,
            shutdown: shutdown::get_global_shutdown(),
            max_retries,
        }
    }

    /// Get the configured max_retries value
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Execute GET request with generic deserialization (T200)
    ///
    /// # Arguments
    /// * `endpoint` - API endpoint path (e.g., "/fapi/v1/klines")
    /// * `params` - Query parameters as key-value pairs
    /// * `weight` - Rate limit weight for this endpoint (F037)
    ///
    /// # Returns
    /// Deserialized response of type T
    ///
    /// # Errors
    /// Returns FetcherError on network, parse, or API errors
    ///
    /// # Rate Limiting (F037, C2)
    ///
    /// The weight parameter is used to properly consume rate limit quota.
    /// Rate limiter is acquired before each attempt (including retries) to
    /// ensure quota is properly tracked. Using incorrect weights can lead
    /// to HTTP 429 errors. Endpoint weights are defined in `BinanceMarketConfig`.
    pub async fn get<T>(
        &self,
        endpoint: &str,
        params: &[(&str, String)],
        weight: u32,
    ) -> FetcherResult<T>
    where
        T: DeserializeOwned,
    {
        // 1. Build full URL
        let url = format!("{}{}", self.base_url, endpoint);

        debug!(
            "Making GET request to: {} with {} params (weight: {})",
            url,
            params.len(),
            weight
        );

        // 2. Execute request with retry logic (rate limiter acquired per attempt - C2)
        self.request_with_retry(&url, params, weight).await
    }

    /// Implement retry logic with exponential backoff (T201)
    ///
    /// Retries on:
    /// - Network errors (timeout, connection refused)
    /// - 5xx server errors
    /// - 429 rate limit errors
    ///
    /// Does not retry on:
    /// - 4xx client errors (except 429)
    /// - Successful responses
    ///
    /// # Rate Limiting (C2)
    ///
    /// Rate limiter is acquired before each attempt (including retries) to
    /// ensure quota is properly tracked across all HTTP requests.
    async fn request_with_retry<T>(
        &self,
        url: &str,
        params: &[(&str, String)],
        weight: u32,
    ) -> FetcherResult<T>
    where
        T: DeserializeOwned,
    {
        let mut last_error = None;
        let mut last_error_type: Option<RetryErrorType> = None;

        // Extract endpoint for logging (remove base URL)
        let endpoint = url.strip_prefix(&self.base_url).unwrap_or(url);
        let symbol = extract_symbol_from_params(params);
        let date_range = extract_time_range_from_params(params);
        // Loop performs max_retries + 1 total attempts (0..=max_retries)
        let max_attempts = (self.max_retries as usize) + 1;

        'attempts: for attempt in 0..=self.max_retries {
            if self.shutdown_requested() {
                last_error = Some(FetcherError::NetworkError("Shutdown requested".to_string()));
                break;
            }

            // Acquire rate limiter BEFORE each attempt including retries (C2)
            // This ensures quota is properly tracked for all HTTP requests
            if let Err(e) = self.rate_limiter.acquire(weight as usize).await {
                last_error = Some(FetcherError::NetworkError(format!("Rate limiter error: {e}")));
                break 'attempts;
            }

            // Execute the HTTP request with shutdown awareness (P2-8)
            let send_result = if let Some(shutdown) = &self.shutdown {
                tokio::select! {
                    result = self.client.get(url).query(params).send() => result,
                    _ = shutdown.wait_for_shutdown() => {
                        last_error = Some(FetcherError::NetworkError("Shutdown requested".to_string()));
                        break 'attempts;
                    }
                }
            } else {
                self.client.get(url).query(params).send().await
            };
            let response = match send_result {
                Ok(resp) => resp,
                Err(e) => {
                    warn!(
                        "Network error on attempt {}/{}: {}",
                        attempt + 1,
                        self.max_retries,
                        e
                    );
                    let error_text = e.to_string();
                    last_error = Some(FetcherError::NetworkError(error_text.clone()));
                    let error_type = extract_error_type(None, Some(&e));
                    last_error_type = Some(error_type);

                    // Retry on network errors
                    if attempt < self.max_retries {
                        let backoff = calculate_backoff(attempt);
                        debug!("Retrying after {:?}", backoff);
                        let ctx = RetryContext::new(
                            (attempt + 1) as usize,
                            max_attempts,
                            error_type,
                            backoff,
                            symbol.clone(),
                            date_range,
                            error_text.clone(),
                            endpoint.to_string(),
                        );
                        log_retry_attempt(ctx);
                        if !self.wait_backoff_or_shutdown(backoff).await {
                            last_error =
                                Some(FetcherError::NetworkError("Shutdown requested".to_string()));
                            break 'attempts;
                        }
                        continue;
                    }
                    break;
                }
            };

            let status = response.status();

            // Check for rate limit error (429)
            if status.as_u16() == 429 {
                warn!(
                    "Rate limit error (429) on attempt {}/{}",
                    attempt + 1,
                    self.max_retries
                );
                last_error = Some(FetcherError::RateLimitExceeded);
                let error_text = "Rate limit exceeded (429)".to_string();
                let error_type = extract_error_type(Some(status), None);
                last_error_type = Some(error_type);

                if attempt < self.max_retries {
                    // Honor Retry-After header if present, otherwise use exponential backoff (M7)
                    let backoff = self
                        .parse_retry_after_header(response.headers())
                        .unwrap_or_else(|| calculate_backoff(attempt));
                    debug!("Retrying after {:?}", backoff);
                    let ctx = RetryContext::new(
                        (attempt + 1) as usize,
                        max_attempts,
                        error_type,
                        backoff,
                        symbol.clone(),
                        date_range,
                        error_text.clone(),
                        endpoint.to_string(),
                    );
                    log_retry_attempt(ctx);
                    if !self.wait_backoff_or_shutdown(backoff).await {
                        last_error =
                            Some(FetcherError::NetworkError("Shutdown requested".to_string()));
                        break 'attempts;
                    }
                    continue;
                }
                break;
            }

            // Check for server errors (5xx)
            if status.is_server_error() {
                warn!(
                    "Server error {} on attempt {}/{}",
                    status,
                    attempt + 1,
                    self.max_retries
                );
                let error_text = format!("Server error: {status}");
                last_error = Some(FetcherError::HttpError(error_text.clone()));
                let error_type = extract_error_type(Some(status), None);
                last_error_type = Some(error_type);

                if attempt < self.max_retries {
                    let backoff = calculate_backoff(attempt);
                    debug!("Retrying after {:?}", backoff);
                    let ctx = RetryContext::new(
                        (attempt + 1) as usize,
                        max_attempts,
                        error_type,
                        backoff,
                        symbol.clone(),
                        date_range,
                        error_text.clone(),
                        endpoint.to_string(),
                    );
                    log_retry_attempt(ctx);
                    if !self.wait_backoff_or_shutdown(backoff).await {
                        last_error =
                            Some(FetcherError::NetworkError("Shutdown requested".to_string()));
                        break 'attempts;
                    }
                    continue;
                }
                break;
            }

            // Don't retry on client errors (4xx, except 429)
            if status.is_client_error() {
                let error_text = response
                    .text()
                    .await
                    .unwrap_or_else(|_| "Unknown error".to_string());
                let formatted_error = format!("Client error {status}: {error_text}");
                let error_type = extract_error_type(Some(status), None);
                log_failure_summary(
                    (attempt + 1) as usize,
                    max_attempts,
                    &symbol,
                    date_range,
                    endpoint,
                    error_type,
                    &formatted_error,
                );
                return Err(FetcherError::HttpError(formatted_error));
            }

            // Parse weight headers before consuming response (T202)
            // NOTE: Clone is necessary here because response.json() consumes the response,
            // but we need to read the rate limit headers before that happens.
            // The alternative (response.bytes() + manual parsing) would be more complex.
            let headers = response.headers().clone();
            if let Some(weight) = self.parse_weight_header(&headers) {
                debug!("Response weight: {}", weight);
            }

            // Success - read body as text first to preserve it on parse error (M1)
            let body_text = match response.text().await {
                Ok(text) => text,
                Err(e) => {
                    return Err(FetcherError::NetworkError(format!(
                        "Failed to read response body: {e}"
                    )));
                }
            };

            // Deserialize from text, preserving body snippet on error for debugging
            match serde_json::from_str::<T>(&body_text) {
                Ok(data) => {
                    debug!("Request succeeded on attempt {}", attempt + 1);
                    log_retry_success(attempt, max_attempts, &symbol, date_range, endpoint);
                    return Ok(data);
                }
                Err(e) => {
                    // Include body snippet in error for debugging (M1)
                    let snippet: String = body_text.chars().take(500).collect();
                    let truncated = if body_text.len() > 500 { "..." } else { "" };
                    return Err(FetcherError::ParseError(format!(
                        "Failed to deserialize response: {e}. Body snippet: {snippet}{truncated}"
                    )));
                }
            }
        }

        // All retries exhausted
        let final_error = last_error
            .unwrap_or_else(|| FetcherError::NetworkError("All retries exhausted".to_string()));
        let final_message = final_error.to_string();
        let failure_type = last_error_type.unwrap_or(RetryErrorType::NetworkGeneric);
        log_failure_summary(
            max_attempts,
            max_attempts,
            &symbol,
            date_range,
            endpoint,
            failure_type,
            &final_message,
        );
        Err(final_error)
    }

    /// Parse rate limit headers (T202)
    ///
    /// Extracts the X-MBX-USED-WEIGHT-1M header to track API weight usage
    ///
    /// # Arguments
    /// * `headers` - Response headers from Binance API
    ///
    /// # Returns
    /// Some(weight) if header is present and valid, None otherwise
    fn parse_weight_header(&self, headers: &reqwest::header::HeaderMap) -> Option<u32> {
        // Try to get the X-MBX-USED-WEIGHT-1M header
        let weight_str = headers.get("X-MBX-USED-WEIGHT-1M")?.to_str().ok()?;

        // Parse as u32
        match weight_str.parse::<u32>() {
            Ok(weight) => {
                debug!("Parsed weight from header: {}", weight);
                Some(weight)
            }
            Err(e) => {
                warn!("Failed to parse weight header '{}': {}", weight_str, e);
                None
            }
        }
    }

    /// Parse Retry-After header for 429 responses (M7)
    ///
    /// Binance may send a Retry-After header indicating how long to wait.
    /// Falls back to None if header is missing or invalid.
    ///
    /// # Safety Cap
    ///
    /// Parsed values are capped at 5 minutes to prevent indefinite hangs
    /// from malicious or buggy server responses.
    ///
    /// # Arguments
    /// * `headers` - Response headers from Binance API
    ///
    /// # Returns
    /// Some(Duration) if Retry-After header is present and valid, None otherwise
    fn parse_retry_after_header(&self, headers: &reqwest::header::HeaderMap) -> Option<Duration> {
        // Maximum retry delay to prevent indefinite hangs (5 minutes)
        const MAX_RETRY_AFTER: Duration = Duration::from_secs(300);

        // Try standard Retry-After header (seconds)
        if let Some(retry_after) = headers.get("Retry-After") {
            if let Ok(seconds_str) = retry_after.to_str() {
                if let Ok(seconds) = seconds_str.trim().parse::<u64>() {
                    let duration = Duration::from_secs(seconds).min(MAX_RETRY_AFTER);
                    debug!("Using Retry-After header: {} seconds (capped: {:?})", seconds, duration);
                    return Some(duration);
                }
            }
        }

        // Try Binance-specific X-Mbx-Retry-After header (milliseconds)
        if let Some(retry_after) = headers.get("X-Mbx-Retry-After") {
            if let Ok(ms_str) = retry_after.to_str() {
                if let Ok(ms) = ms_str.trim().parse::<u64>() {
                    let duration = Duration::from_millis(ms).min(MAX_RETRY_AFTER);
                    debug!("Using X-Mbx-Retry-After header: {} ms (capped: {:?})", ms, duration);
                    return Some(duration);
                }
            }
        }

        None
    }

    fn shutdown_requested(&self) -> bool {
        self.shutdown
            .as_ref()
            .map(|s| s.is_shutdown_requested())
            .unwrap_or(false)
    }

    async fn wait_backoff_or_shutdown(&self, backoff: Duration) -> bool {
        if let Some(shutdown) = &self.shutdown {
            tokio::select! {
                _ = tokio::time::sleep(backoff) => true,
                _ = shutdown.wait_for_shutdown() => false,
            }
        } else {
            tokio::time::sleep(backoff).await;
            true
        }
    }
}

fn extract_symbol_from_params(params: &[(&str, String)]) -> String {
    params
        .iter()
        .find_map(|(key, value)| (*key == "symbol").then(|| value.clone()))
        .unwrap_or_default()
}

fn extract_time_range_from_params(params: &[(&str, String)]) -> Option<(i64, i64)> {
    let start = parse_param_i64(params, "startTime");
    let end = parse_param_i64(params, "endTime");
    match (start, end) {
        (Some(start), Some(end)) if end >= start => Some((start, end)),
        _ => None,
    }
}

fn parse_param_i64(params: &[(&str, String)], key: &str) -> Option<i64> {
    params
        .iter()
        .find(|(name, _)| *name == key)
        .and_then(|(_, value)| value.parse::<i64>().ok())
}

fn log_retry_attempt(ctx: RetryContext) {
    info!("{}", ctx.format_retry());
}

fn log_retry_success(
    attempt_index: u32,
    max_attempts: usize,
    symbol: &str,
    date_range: Option<(i64, i64)>,
    endpoint: &str,
) {
    if attempt_index == 0 {
        return;
    }

    let ctx = RetryContext::new(
        (attempt_index + 1) as usize,
        max_attempts,
        RetryErrorType::NetworkGeneric,
        Duration::from_secs(0),
        symbol.to_string(),
        date_range,
        "Retry successful".to_string(),
        endpoint.to_string(),
    );
    info!("{}", ctx.format_success());
}

fn log_failure_summary(
    attempt: usize,
    max_attempts: usize,
    symbol: &str,
    date_range: Option<(i64, i64)>,
    endpoint: &str,
    error_type: RetryErrorType,
    error_message: &str,
) {
    let ctx = RetryContext::new(
        attempt,
        max_attempts,
        error_type,
        Duration::from_secs(0),
        symbol.to_string(),
        date_range,
        error_message.to_string(),
        endpoint.to_string(),
    );
    error!("{}", ctx.format_failure());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_binance_http_client_creation() {
        let client = Arc::new(Client::new());
        let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
        let http_client =
            BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter, 5);

        assert_eq!(http_client.base_url, "https://fapi.binance.com");
    }

    #[test]
    fn test_parse_weight_header_valid() {
        let client = Arc::new(Client::new());
        let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
        let http_client =
            BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter, 5);

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "X-MBX-USED-WEIGHT-1M",
            reqwest::header::HeaderValue::from_static("42"),
        );

        let weight = http_client.parse_weight_header(&headers);
        assert_eq!(weight, Some(42));
    }

    #[test]
    fn test_parse_weight_header_missing() {
        let client = Arc::new(Client::new());
        let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
        let http_client =
            BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter, 5);

        let headers = reqwest::header::HeaderMap::new();

        let weight = http_client.parse_weight_header(&headers);
        assert_eq!(weight, None);
    }

    #[test]
    fn test_parse_weight_header_invalid() {
        let client = Arc::new(Client::new());
        let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
        let http_client =
            BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter, 5);

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "X-MBX-USED-WEIGHT-1M",
            reqwest::header::HeaderValue::from_static("invalid"),
        );

        let weight = http_client.parse_weight_header(&headers);
        assert_eq!(weight, None);
    }
}
