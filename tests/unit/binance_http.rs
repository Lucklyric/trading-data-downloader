//! Unit tests for BinanceHttpClient (T195-T197)

use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;
use trading_data_downloader::downloader::rate_limit::RateLimiter;
use trading_data_downloader::fetcher::binance_http::BinanceHttpClient;

/// Mock response structure for testing
#[derive(serde::Deserialize, Debug, PartialEq)]
struct MockResponse {
    status: String,
}

/// T195: Test BinanceHttpClient GET request with rate limiting
#[tokio::test]
async fn test_binance_http_client_get_request() {
    // Create a real HTTP client
    let client = Client::new();

    // Create a rate limiter with generous limits for testing
    let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));

    // Create BinanceHttpClient with real Binance API
    let http_client =
        BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter.clone(), 5);

    // Test a real GET request to Binance API (exchangeInfo endpoint)
    // This endpoint has low weight and should always succeed
    let params = vec![];
    let result: Result<serde_json::Value, _> =
        http_client.get("/fapi/v1/exchangeInfo", &params).await;

    // Verify the request succeeded
    assert!(result.is_ok(), "GET request should succeed");

    let response = result.unwrap();

    // Verify response has expected structure
    assert!(
        response.get("symbols").is_some(),
        "Response should contain symbols"
    );
    assert!(
        response.get("timezone").is_some(),
        "Response should contain timezone"
    );
}

/// T196: Test retry logic on network errors
#[tokio::test]
async fn test_binance_http_client_retry_logic() {
    // Create HTTP client
    let client = Client::new();

    // Create rate limiter
    let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));

    // Create BinanceHttpClient with invalid base URL to trigger network errors
    let http_client = BinanceHttpClient::new(
        client,
        "http://invalid-hostname-that-does-not-exist-12345.com",
        rate_limiter,
        5,
    );

    // Attempt GET request - should fail after retries
    let params = vec![];
    let result: Result<serde_json::Value, _> = http_client.get("/api/endpoint", &params).await;

    // Verify the request failed after retries
    assert!(result.is_err(), "Request should fail on network error");

    // Verify error message indicates network issue
    let error = result.unwrap_err();
    let error_msg = error.to_string().to_lowercase();
    assert!(
        error_msg.contains("network") || error_msg.contains("http"),
        "Error should indicate network/HTTP issue, got: {}",
        error
    );
}

/// T197: Test weight-based rate limit tracking
#[tokio::test]
async fn test_binance_http_client_weight_tracking() {
    // Create HTTP client
    let client = Client::new();

    // Create rate limiter with weight tracking
    let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));

    // Verify rate limiter is weight-based
    assert!(
        rate_limiter.is_weight_based(),
        "Rate limiter should be weight-based"
    );

    // Create BinanceHttpClient with real Binance API
    let http_client =
        BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter.clone(), 5);

    // Make a real request to Binance API
    let params = vec![];
    let result: Result<serde_json::Value, _> =
        http_client.get("/fapi/v1/exchangeInfo", &params).await;

    // Verify the request succeeded
    assert!(
        result.is_ok(),
        "GET request should succeed for weight tracking test"
    );

    // Make multiple requests to verify weight accumulation doesn't block
    // (with generous limits, requests should succeed)
    for _ in 0..3 {
        let result: Result<serde_json::Value, _> =
            http_client.get("/fapi/v1/exchangeInfo", &params).await;

        assert!(result.is_ok(), "Subsequent requests should succeed");
    }
}

/// Test BinanceHttpClient with query parameters
#[tokio::test]
async fn test_binance_http_client_with_params() {
    // Create HTTP client
    let client = Client::new();

    // Create rate limiter
    let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));

    // Create BinanceHttpClient
    let http_client = BinanceHttpClient::new(client, "https://fapi.binance.com", rate_limiter, 5);

    // Test request with query parameters
    let params = vec![
        ("symbol", "BTCUSDT".to_string()),
        ("limit", "100".to_string()),
    ];

    let result: Result<serde_json::Value, _> =
        http_client.get("/fapi/v1/fundingRate", &params).await;

    // Verify the request succeeded
    assert!(result.is_ok(), "GET request with params should succeed");

    let response = result.unwrap();

    // Verify response is an array
    assert!(
        response.is_array(),
        "Response should be an array of funding rates"
    );
}

/// T005: Test BinanceHttpClient constructor accepts max_retries parameter
#[test]
fn test_binance_http_client_constructor_accepts_max_retries() {
    // Create HTTP client
    let client = Client::new();

    // Create rate limiter
    let rate_limiter = Arc::new(RateLimiter::weight_based(1000, Duration::from_secs(60)));

    // Verify constructor accepts max_retries parameter
    let custom_max_retries = 10u32;
    let http_client = BinanceHttpClient::new(
        client,
        "https://fapi.binance.com",
        rate_limiter,
        custom_max_retries,
    );

    // Verify client was created successfully and max_retries is set correctly
    assert_eq!(http_client.max_retries(), custom_max_retries);
}
