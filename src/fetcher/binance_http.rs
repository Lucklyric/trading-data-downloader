//! Binance HTTP client helper module (T198-T202)
//!
//! Provides unified HTTP client for all Binance API interactions with:
//! - Generic request/response handling
//! - Rate limit integration
//! - Retry logic with exponential backoff
//! - Weight header parsing

use reqwest::Client;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use tracing::{debug, warn};

use crate::downloader::config::{calculate_backoff, MAX_RETRIES};
use crate::downloader::rate_limit::RateLimiter;
use crate::fetcher::{FetcherError, FetcherResult};

/// Unified HTTP client for all Binance API interactions (T198)
pub struct BinanceHttpClient {
    client: Arc<Client>,
    base_url: String,
    rate_limiter: Arc<RateLimiter>,
}

impl BinanceHttpClient {
    /// Create new HTTP client (T199)
    ///
    /// # Arguments
    /// * `client` - Shared HTTP client (Arc for cheap cloning)
    /// * `base_url` - Base URL for API endpoints (e.g., "<https://fapi.binance.com>")
    /// * `rate_limiter` - Shared rate limiter (Arc for global quota enforcement)
    pub fn new(client: Arc<Client>, base_url: impl Into<String>, rate_limiter: Arc<RateLimiter>) -> Self {
        Self {
            client,
            base_url: base_url.into(),
            rate_limiter,
        }
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
            .map_err(|e| FetcherError::NetworkError(format!("Rate limiter error: {}", e)))?;

        debug!("Making GET request to: {} with {} params", url, params.len());

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

        for attempt in 0..=MAX_RETRIES {
            // Execute the HTTP request
            let response = match self.client.get(url).query(params).send().await {
                Ok(resp) => resp,
                Err(e) => {
                    warn!(
                        "Network error on attempt {}/{}: {}",
                        attempt + 1,
                        MAX_RETRIES + 1,
                        e
                    );
                    last_error = Some(FetcherError::NetworkError(e.to_string()));

                    // Retry on network errors
                    if attempt < MAX_RETRIES {
                        let backoff = calculate_backoff(attempt);
                        debug!("Retrying after {:?}", backoff);
                        tokio::time::sleep(backoff).await;
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
                    MAX_RETRIES + 1
                );
                last_error = Some(FetcherError::RateLimitExceeded);

                if attempt < MAX_RETRIES {
                    let backoff = calculate_backoff(attempt);
                    debug!("Retrying after {:?}", backoff);
                    tokio::time::sleep(backoff).await;
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
                    MAX_RETRIES + 1
                );
                last_error = Some(FetcherError::HttpError(format!(
                    "Server error: {}",
                    status
                )));

                if attempt < MAX_RETRIES {
                    let backoff = calculate_backoff(attempt);
                    debug!("Retrying after {:?}", backoff);
                    tokio::time::sleep(backoff).await;
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
                return Err(FetcherError::HttpError(format!(
                    "Client error {}: {}",
                    status, error_text
                )));
            }

            // Parse weight headers before consuming response (T202)
            let headers = response.headers().clone();
            if let Some(weight) = self.parse_weight_header(&headers) {
                debug!("Response weight: {}", weight);
                // Note: In a more sophisticated implementation, we would update
                // the rate limiter with the actual weight used. For now, we just log it.
            }

            // Success - deserialize response
            match response.json::<T>().await {
                Ok(data) => {
                    debug!("Request succeeded on attempt {}", attempt + 1);
                    return Ok(data);
                }
                Err(e) => {
                    return Err(FetcherError::ParseError(format!(
                        "Failed to deserialize response: {}",
                        e
                    )));
                }
            }
        }

        // All retries exhausted
        Err(last_error.unwrap_or_else(|| {
            FetcherError::NetworkError("All retries exhausted".to_string())
        }))
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_binance_http_client_creation() {
        let client = Arc::new(Client::new());
        let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
        let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter);

        assert_eq!(http_client.base_url, "https://fapi.binance.com");
    }

    #[test]
    fn test_parse_weight_header_valid() {
        let client = Arc::new(Client::new());
        let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
        let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter);

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
        let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter);

        let headers = reqwest::header::HeaderMap::new();

        let weight = http_client.parse_weight_header(&headers);
        assert_eq!(weight, None);
    }

    #[test]
    fn test_parse_weight_header_invalid() {
        let client = Arc::new(Client::new());
        let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));
        let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter);

        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            "X-MBX-USED-WEIGHT-1M",
            reqwest::header::HeaderValue::from_static("invalid"),
        );

        let weight = http_client.parse_weight_header(&headers);
        assert_eq!(weight, None);
    }
}
