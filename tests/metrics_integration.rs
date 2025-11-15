//! Integration tests for metrics system
//!
//! These tests verify that metrics are correctly emitted during various scenarios
//! including 429 errors, retries, and successful downloads.

use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;
use trading_data_downloader::metrics;

/// Helper to check if metrics endpoint is responding
async fn check_metrics_endpoint(addr: &str) -> bool {
    let url = format!("http://{}/metrics", addr);
    match reqwest::get(&url).await {
        Ok(resp) => resp.status().is_success(),
        Err(_) => false,
    }
}

/// Helper to fetch metrics text from endpoint
async fn fetch_metrics_text(addr: &str) -> Result<String, Box<dyn std::error::Error>> {
    let url = format!("http://{}/metrics", addr);
    let resp = reqwest::get(&url).await?;
    Ok(resp.text().await?)
}

#[tokio::test]
async fn test_metrics_initialization() {
    // Initialize metrics on a random port
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();

    // Should succeed first time
    assert!(metrics::init_metrics(addr).await.is_ok());

    // Should be idempotent (no error on second call)
    assert!(metrics::init_metrics(addr).await.is_ok());

    // Verify initialized flag
    assert!(metrics::is_initialized().await);
}

#[tokio::test]
async fn test_metrics_endpoint_accessible() {
    // Use a fixed port for this test
    let addr: SocketAddr = "127.0.0.1:19090".parse().unwrap();

    // Initialize metrics
    metrics::init_metrics(addr).await.unwrap();

    // Give the server time to start
    sleep(Duration::from_millis(100)).await;

    // Check endpoint is accessible
    assert!(check_metrics_endpoint("127.0.0.1:19090").await);

    // Fetch metrics text
    let metrics_text = fetch_metrics_text("127.0.0.1:19090").await.unwrap();

    // Should contain standard Prometheus format
    assert!(metrics_text.contains("# TYPE"));
    assert!(metrics_text.contains("# HELP"));
}

#[tokio::test]
async fn test_http_request_metrics() {
    use trading_data_downloader::metrics::HttpRequestMetrics;

    // Initialize metrics
    let addr: SocketAddr = "127.0.0.1:19091".parse().unwrap();
    metrics::init_metrics(addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Record some requests
    let metrics1 = HttpRequestMetrics::start("/fapi/v1/klines", 0).await;
    sleep(Duration::from_millis(10)).await;
    metrics1.record_complete(200);

    let metrics2 = HttpRequestMetrics::start("/fapi/v1/aggTrades", 1).await;
    sleep(Duration::from_millis(5)).await;
    metrics2.record_complete(429);

    let metrics3 = HttpRequestMetrics::start("/fapi/v1/fundingRate", 0).await;
    metrics3.record_network_error();

    // Fetch metrics and verify
    let metrics_text = fetch_metrics_text("127.0.0.1:19091").await.unwrap();

    // Should contain our metrics
    assert!(metrics_text.contains("http_requests_total"));
    assert!(metrics_text.contains("http_429_errors_total"));
    assert!(metrics_text.contains("http_request_duration_seconds"));
}

#[tokio::test]
async fn test_retry_backoff_metrics() {
    use trading_data_downloader::metrics::record_retry_backoff;

    // Initialize metrics
    let addr: SocketAddr = "127.0.0.1:19092".parse().unwrap();
    metrics::init_metrics(addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Record some retries
    record_retry_backoff(Duration::from_millis(1000), 0);
    record_retry_backoff(Duration::from_millis(2000), 1);
    record_retry_backoff(Duration::from_millis(4000), 2);

    // Fetch metrics and verify
    let metrics_text = fetch_metrics_text("127.0.0.1:19092").await.unwrap();

    assert!(metrics_text.contains("http_retries_total"));
    assert!(metrics_text.contains("retry_backoff_duration_seconds"));
}

#[tokio::test]
async fn test_api_weight_metrics() {
    use trading_data_downloader::metrics::record_api_weight;

    // Initialize metrics
    let addr: SocketAddr = "127.0.0.1:19093".parse().unwrap();
    metrics::init_metrics(addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Record weight consumption
    record_api_weight(100, 2400);
    record_api_weight(500, 2400);
    record_api_weight(2000, 2400); // Above 80% threshold

    // Fetch metrics and verify
    let metrics_text = fetch_metrics_text("127.0.0.1:19093").await.unwrap();

    assert!(metrics_text.contains("api_weight_consumed"));
    assert!(metrics_text.contains("api_weight_remaining"));
}

#[tokio::test]
async fn test_rate_limiter_metrics() {
    use trading_data_downloader::metrics::RateLimiterMetrics;

    // Initialize metrics
    let addr: SocketAddr = "127.0.0.1:19094".parse().unwrap();
    metrics::init_metrics(addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Simulate rate limiter operations
    let mut limiter_metrics = RateLimiterMetrics::new();

    limiter_metrics.start_acquire();
    sleep(Duration::from_millis(50)).await; // Simulate wait
    limiter_metrics.record_acquired(5);
    limiter_metrics.update_available_permits(95);

    // Fetch metrics and verify
    let metrics_text = fetch_metrics_text("127.0.0.1:19094").await.unwrap();

    assert!(metrics_text.contains("rate_limit_permits_acquired_total"));
    assert!(metrics_text.contains("rate_limit_permits_available"));
    assert!(metrics_text.contains("rate_limit_queue_wait_seconds"));
}

#[tokio::test]
async fn test_download_metrics() {
    use trading_data_downloader::metrics::DownloadMetrics;

    // Initialize metrics
    let addr: SocketAddr = "127.0.0.1:19095".parse().unwrap();
    metrics::init_metrics(addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Record successful download
    let metrics1 = DownloadMetrics::start("bars", "BTCUSDT");
    sleep(Duration::from_millis(100)).await;
    metrics1.record_success(1000);

    // Record failed download
    let metrics2 = DownloadMetrics::start("aggtrades", "ETHUSDT");
    sleep(Duration::from_millis(50)).await;
    metrics2.record_failure("Network timeout");

    // Fetch metrics and verify
    let metrics_text = fetch_metrics_text("127.0.0.1:19095").await.unwrap();

    assert!(metrics_text.contains("downloads_completed_total"));
    assert!(metrics_text.contains("downloads_failed_total"));
}

#[tokio::test]
async fn test_correlation_id_uniqueness() {
    use trading_data_downloader::metrics::generate_correlation_id;

    // Generate multiple IDs
    let id1 = generate_correlation_id().await;
    let id2 = generate_correlation_id().await;
    let id3 = generate_correlation_id().await;

    // All should be unique
    assert_ne!(id1, id2);
    assert_ne!(id2, id3);
    assert_ne!(id1, id3);

    // Should follow format
    assert!(id1.starts_with("req-"));
    assert!(id2.starts_with("req-"));
    assert!(id3.starts_with("req-"));
}

#[tokio::test]
async fn test_metrics_performance_overhead() {
    use std::time::Instant;
    use trading_data_downloader::metrics::HttpRequestMetrics;

    // Initialize metrics
    let addr: SocketAddr = "127.0.0.1:19096".parse().unwrap();
    metrics::init_metrics(addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Measure time for 1000 metric emissions
    let start = Instant::now();

    for i in 0..1000 {
        let metrics = HttpRequestMetrics::start("/test", i % 5).await;
        metrics.record_complete(200);
    }

    let elapsed = start.elapsed();

    // Should complete in under 100ms (0.1ms per metric)
    assert!(
        elapsed.as_millis() < 100,
        "Metrics overhead too high: {}ms for 1000 emissions",
        elapsed.as_millis()
    );
}

#[tokio::test]
async fn test_metrics_graceful_degradation() {
    // Try to initialize on a port that's likely in use
    let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

    // Should fail gracefully (permission denied on port 1)
    let result = metrics::init_metrics(addr).await;
    assert!(result.is_err());

    // But metrics operations should not panic
    use trading_data_downloader::metrics::HttpRequestMetrics;

    let metrics = HttpRequestMetrics::start("/test", 0).await;
    metrics.record_complete(200); // Should not panic even if metrics not initialized
}

/// Test that metrics work correctly in a concurrent environment
#[tokio::test]
async fn test_metrics_concurrency() {
    use futures::future::join_all;
    use trading_data_downloader::metrics::HttpRequestMetrics;

    // Initialize metrics
    let addr: SocketAddr = "127.0.0.1:19097".parse().unwrap();
    metrics::init_metrics(addr).await.unwrap();
    sleep(Duration::from_millis(100)).await;

    // Spawn 100 concurrent tasks emitting metrics
    let tasks: Vec<_> = (0..100)
        .map(|i| {
            tokio::spawn(async move {
                let endpoint = format!("/endpoint/{}", i % 10);
                let metrics = HttpRequestMetrics::start(endpoint, i % 3).await;
                sleep(Duration::from_millis((i % 10) as u64)).await;

                if i % 10 == 0 {
                    metrics.record_complete(429);
                } else if i % 5 == 0 {
                    metrics.record_network_error();
                } else {
                    metrics.record_complete(200);
                }
            })
        })
        .collect();

    // Wait for all to complete
    join_all(tasks).await;

    // Verify metrics were recorded
    let metrics_text = fetch_metrics_text("127.0.0.1:19097").await.unwrap();

    assert!(metrics_text.contains("http_requests_total"));
    assert!(metrics_text.contains("http_429_errors_total"));
}
