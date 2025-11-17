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
use crate::metrics::{record_api_weight, record_retry_backoff, HttpRequestMetrics};
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
    ///
    /// # Returns
    /// Deserialized response of type T
    ///
    /// # Errors
    /// Returns FetcherError on network, parse, or API errors
    pub async fn get<T>(&self, endpoint: &str, params: &[(&str, String)]) -> FetcherResult<T>
    where
        T: DeserializeOwned,
    {
        // 1. Build full URL
        let url = format!("{}{}", self.base_url, endpoint);

        // 2. Consult rate limiter before making request
        // Use weight of 1 as default - will be updated from response headers
        self.rate_limiter
            .acquire(1)
            .await
            .map_err(|e| FetcherError::NetworkError(format!("Rate limiter error: {e}")))?;

        debug!(
            "Making GET request to: {} with {} params",
            url,
            params.len()
        );

        // 3. Execute request with retry logic
        self.request_with_retry(&url, params).await
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
    async fn request_with_retry<T>(&self, url: &str, params: &[(&str, String)]) -> FetcherResult<T>
    where
        T: DeserializeOwned,
    {
        let mut last_error = None;
        let mut last_error_type: Option<RetryErrorType> = None;

        // Extract endpoint for metrics (remove base URL)
        let endpoint = url.strip_prefix(&self.base_url).unwrap_or(url);
        let symbol = extract_symbol_from_params(params);
        let date_range = extract_time_range_from_params(params);
        let max_attempts = self.max_retries as usize;

        'attempts: for attempt in 0..=self.max_retries {
            if self.shutdown_requested() {
                last_error = Some(FetcherError::NetworkError("Shutdown requested".to_string()));
                break;
            }
            // Start metrics collection for this request
            let request_metrics = HttpRequestMetrics::start(endpoint, attempt).await;

            // Execute the HTTP request
            let response = match self.client.get(url).query(params).send().await {
                Ok(resp) => resp,
                Err(e) => {
                    warn!(
                        "Network error on attempt {}/{}: {}",
                        attempt + 1,
                        self.max_retries,
                        e
                    );
                    request_metrics.record_network_error();
                    let error_text = e.to_string();
                    last_error = Some(FetcherError::NetworkError(error_text.clone()));
                    let error_type = extract_error_type(None, Some(&e));
                    last_error_type = Some(error_type);

                    // Retry on network errors
                    if attempt < self.max_retries {
                        let backoff = calculate_backoff(attempt);
                        debug!("Retrying after {:?}", backoff);
                        record_retry_backoff(backoff, attempt);
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
                request_metrics.record_complete(429);
                last_error = Some(FetcherError::RateLimitExceeded);
                let error_text = "Rate limit exceeded (429)".to_string();
                let error_type = extract_error_type(Some(status), None);
                last_error_type = Some(error_type);

                if attempt < self.max_retries {
                    let backoff = calculate_backoff(attempt);
                    debug!("Retrying after {:?}", backoff);
                    record_retry_backoff(backoff, attempt);
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
                    self.max_retries + 1
                );
                request_metrics.record_complete(status.as_u16());
                let error_text = format!("Server error: {status}");
                last_error = Some(FetcherError::HttpError(error_text.clone()));
                let error_type = extract_error_type(Some(status), None);
                last_error_type = Some(error_type);

                if attempt < self.max_retries {
                    let backoff = calculate_backoff(attempt);
                    debug!("Retrying after {:?}", backoff);
                    record_retry_backoff(backoff, attempt);
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
                request_metrics.record_complete(status.as_u16());
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
            let headers = response.headers().clone();
            if let Some(weight) = self.parse_weight_header(&headers) {
                debug!("Response weight: {}", weight);
                // Update metrics with actual weight consumed
                // Binance weight limit is 2400 per minute for futures
                record_api_weight(weight, 2400);
            }

            // Success - deserialize response
            match response.json::<T>().await {
                Ok(data) => {
                    debug!("Request succeeded on attempt {}", attempt + 1);
                    request_metrics.record_complete(status.as_u16());
                    log_retry_success(attempt, max_attempts, &symbol, date_range, endpoint);
                    return Ok(data);
                }
                Err(e) => {
                    request_metrics.record_complete(status.as_u16());
                    return Err(FetcherError::ParseError(format!(
                        "Failed to deserialize response: {e}"
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
        let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter, 5);

        assert_eq!(http_client.base_url, "https://fapi.binance.com");
    }

    #[test]
    fn test_parse_weight_header_valid() {
        let client = Arc::new(Client::new());
        let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
        let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter, 5);

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
        let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter, 5);

        let headers = reqwest::header::HeaderMap::new();

        let weight = http_client.parse_weight_header(&headers);
        assert_eq!(weight, None);
    }

    #[test]
    fn test_parse_weight_header_invalid() {
        let client = Arc::new(Client::new());
        let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
        let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter, 5);

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "X-MBX-USED-WEIGHT-1M",
            reqwest::header::HeaderValue::from_static("invalid"),
        );

        let weight = http_client.parse_weight_header(&headers);
        assert_eq!(weight, None);
    }
}
