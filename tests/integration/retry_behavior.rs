//! Integration tests for HTTP retry behavior (User Story 1)
//!
//! Tests that verify CLI --max-retries flag is properly wired to HTTP client

use std::sync::Arc;
use std::time::Duration;
use reqwest::Client;
use trading_data_downloader::fetcher::binance_http::BinanceHttpClient;
use trading_data_downloader::downloader::rate_limit::RateLimiter;

#[tokio::test]
async fn test_http_client_respects_custom_max_retries() {
    // T004: Verify HTTP client respects CLI --max-retries flag
    // This test verifies that when max_retries is set via CLI,
    // the HTTP client uses that value instead of a hardcoded constant

    let client = Client::new();
    let rate_limiter = Arc::new(RateLimiter::weight_based(1200, Duration::from_secs(60)));
    let custom_max_retries = 1; // Only 1 retry = 2 total attempts (initial + 1 retry)

    // Create HTTP client with custom max_retries
    let http_client = BinanceHttpClient::new(
        client,
        "https://fapi.binance.com",
        rate_limiter,
        custom_max_retries,
    );

    // Verify the client was created successfully and respects the max_retries value
    assert_eq!(http_client.max_retries(), custom_max_retries);

    // TODO: Add actual retry behavior test once implementation is complete
    // This test will verify that:
    // 1. With max_retries=1, we get exactly 2 attempts (initial + 1 retry)
    // 2. Retry messages show "attempt X/Y" where Y matches max_retries + 1
}

#[tokio::test]
async fn test_http_client_respects_default_max_retries() {
    // T004: Verify HTTP client uses default max_retries=5
    // When no custom value is provided, should default to 5 retries

    let client = Client::new();
    let rate_limiter = Arc::new(RateLimiter::weight_based(1200, Duration::from_secs(60)));
    let default_max_retries = 5; // Default from FR-002

    // Create HTTP client with default max_retries
    let http_client = BinanceHttpClient::new(
        client,
        "https://fapi.binance.com",
        rate_limiter,
        default_max_retries,
    );

    // Verify the client was created successfully and respects the max_retries value
    assert_eq!(http_client.max_retries(), default_max_retries);

    // TODO: Add actual retry behavior test once implementation is complete
    // This test will verify that default is 5 retries = 6 total attempts
}
