//! Example demonstrating metrics collection
//!
//! Run with:
//! ```bash
//! cargo run --example metrics_demo
//! ```
//!
//! Then check metrics at http://localhost:9090/metrics

use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;
use trading_data_downloader::metrics::{
    self, record_api_weight, record_retry_backoff, DownloadMetrics, HttpRequestMetrics,
    RateLimiterMetrics,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("trading_data_downloader=debug")
        .init();

    // Initialize metrics on port 9090
    let addr: SocketAddr = "0.0.0.0:9090".parse()?;
    println!("Initializing metrics on {}", addr);
    metrics::init_metrics(addr).await?;

    println!("\n=== Metrics Demo Started ===");
    println!("View metrics at: http://localhost:9090/metrics\n");

    // Simulate various operations
    println!("Simulating HTTP requests...");
    simulate_http_requests().await;

    println!("\nSimulating 429 errors and retries...");
    simulate_429_errors().await;

    println!("\nSimulating API weight consumption...");
    simulate_api_weight().await;

    println!("\nSimulating rate limiter operations...");
    simulate_rate_limiter().await;

    println!("\nSimulating download jobs...");
    simulate_downloads().await;

    println!("\n=== Demo Complete ===");
    println!("Metrics server will continue running. Press Ctrl+C to exit.");
    println!("Check metrics at: http://localhost:9090/metrics");

    // Keep the server running
    loop {
        sleep(Duration::from_secs(60)).await;
    }
}

async fn simulate_http_requests() {
    // Successful requests
    for i in 0..5 {
        let metrics = HttpRequestMetrics::start("/fapi/v1/klines", 0).await;
        sleep(Duration::from_millis(50 + i * 10)).await;
        metrics.record_complete(200);
        println!("  ✓ Request {} completed (200 OK)", i + 1);
    }

    // Mixed status codes
    let endpoints = [
        ("/fapi/v1/aggTrades", 200),
        ("/fapi/v1/fundingRate", 404),
        ("/fapi/v1/exchangeInfo", 200),
        ("/fapi/v1/ticker/24hr", 500),
    ];

    for (endpoint, status) in endpoints {
        let metrics = HttpRequestMetrics::start(endpoint, 0).await;
        sleep(Duration::from_millis(30)).await;
        metrics.record_complete(status);
        println!("  → {} returned {}", endpoint, status);
    }
}

async fn simulate_429_errors() {
    // Simulate rate limit errors with retries
    for attempt in 0..3 {
        let metrics = HttpRequestMetrics::start("/fapi/v1/klines", attempt).await;
        sleep(Duration::from_millis(20)).await;

        if attempt < 2 {
            metrics.record_complete(429);
            println!("  ⚠ Rate limit hit (429), attempt {}", attempt + 1);

            let backoff = Duration::from_secs(1 << attempt);
            record_retry_backoff(backoff, attempt);
            println!("  ⏳ Backing off for {:?}", backoff);
            sleep(backoff).await;
        } else {
            metrics.record_complete(200);
            println!("  ✓ Request succeeded after {} retries", attempt);
        }
    }
}

async fn simulate_api_weight() {
    // Simulate progressive weight consumption
    let limits = [
        (100, "Low usage"),
        (500, "Moderate usage"),
        (1500, "High usage"),
        (2000, "Very high usage (>80% threshold)"),
        (2300, "Critical usage (>95% threshold)"),
    ];

    for (weight, description) in limits {
        record_api_weight(weight, 2400);
        println!("  📊 API weight: {}/2400 - {}", weight, description);
        sleep(Duration::from_millis(500)).await;
    }
}

async fn simulate_rate_limiter() {
    let mut metrics = RateLimiterMetrics::new();

    // Simulate acquiring permits with varying wait times
    for weight in [1, 5, 10, 20] {
        metrics.start_acquire();
        println!("  🔒 Requesting {} permits...", weight);

        // Simulate wait time
        sleep(Duration::from_millis(weight as u64 * 10)).await;

        metrics.record_acquired(weight);
        metrics.update_available_permits(100 - weight);
        println!(
            "  ✓ Acquired {} permits, {} remaining",
            weight,
            100 - weight
        );

        sleep(Duration::from_millis(200)).await;
    }
}

async fn simulate_downloads() {
    // Successful download
    let metrics1 = DownloadMetrics::start("bars", "BTCUSDT");
    println!("  📥 Starting BTCUSDT bars download...");
    sleep(Duration::from_secs(2)).await;
    metrics1.record_success(50000);
    println!("  ✓ BTCUSDT download complete: 50,000 bars");

    sleep(Duration::from_millis(500)).await;

    // Failed download
    let metrics2 = DownloadMetrics::start("aggtrades", "ETHUSDT");
    println!("  📥 Starting ETHUSDT trades download...");
    sleep(Duration::from_secs(1)).await;
    metrics2.record_failure("Connection timeout");
    println!("  ✗ ETHUSDT download failed: Connection timeout");

    sleep(Duration::from_millis(500)).await;

    // Another successful download
    let metrics3 = DownloadMetrics::start("funding", "SOLUSDT");
    println!("  📥 Starting SOLUSDT funding rates download...");
    sleep(Duration::from_millis(800)).await;
    metrics3.record_success(365);
    println!("  ✓ SOLUSDT download complete: 365 funding rates");
}
