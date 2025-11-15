//! Production observability metrics for trading data downloader
//!
//! This module provides comprehensive metrics collection for monitoring
//! 429 errors, retry behavior, rate limiter health, and API weight consumption.
//!
//! ## Architecture
//!
//! - Uses `metrics` crate for low-overhead metric collection
//! - Prometheus exporter for scraping endpoint (:9090/metrics)
//! - Async emission to avoid blocking HTTP requests
//! - Graceful degradation if metrics sink unavailable

use metrics::{
    counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram, Unit,
};
use metrics_exporter_prometheus::PrometheusBuilder;
use once_cell::sync::Lazy;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Global metrics registry initialization flag
static METRICS_INITIALIZED: Lazy<Arc<RwLock<bool>>> = Lazy::new(|| Arc::new(RwLock::new(false)));

/// Correlation ID generator for request tracing
static CORRELATION_COUNTER: Lazy<Arc<RwLock<u64>>> = Lazy::new(|| Arc::new(RwLock::new(0)));

/// Initialize metrics system with Prometheus exporter
///
/// This should be called once at application startup, typically in main().
/// The function is idempotent and will not reinitialize if already called.
///
/// # Arguments
/// * `addr` - Socket address to bind Prometheus scrape endpoint (e.g., "0.0.0.0:9090")
///
/// # Returns
/// Ok(()) if metrics initialized successfully, Err if binding fails
pub async fn init_metrics(addr: SocketAddr) -> Result<(), Box<dyn std::error::Error>> {
    let mut initialized = METRICS_INITIALIZED.write().await;
    if *initialized {
        debug!("Metrics already initialized, skipping");
        return Ok(());
    }

    info!("Initializing metrics system on {}", addr);

    // Configure Prometheus exporter
    PrometheusBuilder::new()
        .with_http_listener(addr)
        .install()
        .map_err(|e| format!("Failed to install Prometheus exporter: {e}"))?;

    // Register metric descriptions for better Prometheus integration
    describe_counter!(
        "http_requests_total",
        Unit::Count,
        "Total number of HTTP requests made to Binance API"
    );

    describe_counter!(
        "http_429_errors_total",
        Unit::Count,
        "Total number of 429 rate limit errors received"
    );

    describe_counter!(
        "http_retries_total",
        Unit::Count,
        "Total number of retry attempts"
    );

    describe_histogram!(
        "http_request_duration_seconds",
        Unit::Seconds,
        "HTTP request duration in seconds"
    );

    describe_histogram!(
        "retry_backoff_duration_seconds",
        Unit::Seconds,
        "Duration of retry backoff in seconds"
    );

    describe_gauge!(
        "api_weight_consumed",
        Unit::Count,
        "Current API weight consumed in the rolling window"
    );

    describe_gauge!(
        "api_weight_remaining",
        Unit::Count,
        "Remaining API weight in the rolling window"
    );

    describe_counter!(
        "rate_limit_permits_acquired_total",
        Unit::Count,
        "Total number of rate limit permits acquired"
    );

    describe_gauge!(
        "rate_limit_permits_available",
        Unit::Count,
        "Currently available rate limit permits"
    );

    describe_histogram!(
        "rate_limit_queue_wait_seconds",
        Unit::Seconds,
        "Time spent waiting for rate limit permits"
    );

    describe_counter!(
        "downloads_completed_total",
        Unit::Count,
        "Total number of successful downloads completed"
    );

    describe_counter!(
        "downloads_failed_total",
        Unit::Count,
        "Total number of failed downloads"
    );

    *initialized = true;
    info!("Metrics system initialized successfully on {}", addr);
    Ok(())
}

/// Generate a new correlation ID for request tracing
pub async fn generate_correlation_id() -> String {
    let mut counter = CORRELATION_COUNTER.write().await;
    *counter += 1;
    format!("req-{:08x}", *counter)
}

/// Record an HTTP request with timing
pub struct HttpRequestMetrics {
    endpoint: String,
    start_time: Instant,
    correlation_id: String,
    attempt: u32,
}

impl HttpRequestMetrics {
    /// Start recording a new HTTP request
    pub async fn start(endpoint: impl Into<String>, attempt: u32) -> Self {
        let endpoint = endpoint.into();
        let correlation_id = generate_correlation_id().await;

        debug!(
            correlation_id = %correlation_id,
            endpoint = %endpoint,
            attempt = attempt,
            "Starting HTTP request metrics"
        );

        Self {
            endpoint,
            start_time: Instant::now(),
            correlation_id,
            attempt,
        }
    }

    /// Record completion of the HTTP request
    pub fn record_complete(&self, status_code: u16) {
        let duration = self.start_time.elapsed();

        // Record request counter with labels
        counter!(
            "http_requests_total",
            "endpoint" => self.endpoint.clone(),
            "status" => status_code.to_string(),
            "attempt" => self.attempt.to_string(),
        )
        .increment(1);

        // Record request duration
        histogram!(
            "http_request_duration_seconds",
            "endpoint" => self.endpoint.clone(),
        )
        .record(duration.as_secs_f64());

        // Special handling for 429 errors
        if status_code == 429 {
            counter!(
                "http_429_errors_total",
                "endpoint" => self.endpoint.clone(),
            )
            .increment(1);

            warn!(
                correlation_id = %self.correlation_id,
                endpoint = %self.endpoint,
                attempt = self.attempt,
                duration_ms = duration.as_millis(),
                "Rate limit error (429) recorded"
            );
        }

        debug!(
            correlation_id = %self.correlation_id,
            endpoint = %self.endpoint,
            status = status_code,
            duration_ms = duration.as_millis(),
            "HTTP request completed"
        );
    }

    /// Record a network error (no status code)
    pub fn record_network_error(&self) {
        let duration = self.start_time.elapsed();

        counter!(
            "http_requests_total",
            "endpoint" => self.endpoint.clone(),
            "status" => "network_error",
            "attempt" => self.attempt.to_string(),
        )
        .increment(1);

        histogram!(
            "http_request_duration_seconds",
            "endpoint" => self.endpoint.clone(),
        )
        .record(duration.as_secs_f64());

        warn!(
            correlation_id = %self.correlation_id,
            endpoint = %self.endpoint,
            attempt = self.attempt,
            duration_ms = duration.as_millis(),
            "Network error recorded"
        );
    }

    /// Get the correlation ID for this request
    pub fn correlation_id(&self) -> &str {
        &self.correlation_id
    }
}

/// Record retry backoff duration
pub fn record_retry_backoff(duration: Duration, attempt: u32) {
    counter!(
        "http_retries_total",
        "attempt" => attempt.to_string(),
    )
    .increment(1);

    histogram!(
        "retry_backoff_duration_seconds",
        "attempt" => attempt.to_string(),
    )
    .record(duration.as_secs_f64());

    debug!(
        attempt = attempt,
        backoff_ms = duration.as_millis(),
        "Retry backoff recorded"
    );
}

/// Record API weight consumption from response headers
pub fn record_api_weight(consumed: u32, limit: u32) {
    let remaining = limit.saturating_sub(consumed);

    gauge!("api_weight_consumed").set(consumed as f64);
    gauge!("api_weight_remaining").set(remaining as f64);

    // Emit warning if approaching limit
    let usage_percent = (consumed as f64 / limit as f64) * 100.0;
    if usage_percent >= 80.0 {
        warn!(
            consumed = consumed,
            limit = limit,
            usage_percent = usage_percent,
            "API weight usage exceeds 80% threshold"
        );
    }

    debug!(
        consumed = consumed,
        remaining = remaining,
        usage_percent = usage_percent,
        "API weight recorded"
    );
}

/// Rate limiter metrics helper
pub struct RateLimiterMetrics {
    start_time: Option<Instant>,
}

impl Default for RateLimiterMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimiterMetrics {
    /// Create a new rate limiter metrics instance
    pub fn new() -> Self {
        Self { start_time: None }
    }

    /// Start measuring queue wait time
    pub fn start_acquire(&mut self) {
        self.start_time = Some(Instant::now());
    }

    /// Record successful permit acquisition
    pub fn record_acquired(&mut self, weight: u32) {
        if let Some(start) = self.start_time.take() {
            let wait_duration = start.elapsed();

            histogram!("rate_limit_queue_wait_seconds").record(wait_duration.as_secs_f64());

            counter!(
                "rate_limit_permits_acquired_total",
                "weight" => weight.to_string(),
            )
            .increment(1);

            if wait_duration.as_millis() > 100 {
                debug!(
                    weight = weight,
                    wait_ms = wait_duration.as_millis(),
                    "Rate limit permit acquired after wait"
                );
            }
        }
    }

    /// Update available permits gauge
    pub fn update_available_permits(&self, available: u32) {
        gauge!("rate_limit_permits_available").set(available as f64);
    }
}

/// Download job metrics
pub struct DownloadMetrics {
    job_type: String,
    symbol: String,
    start_time: Instant,
}

impl DownloadMetrics {
    /// Start tracking a download job
    pub fn start(job_type: impl Into<String>, symbol: impl Into<String>) -> Self {
        let job_type = job_type.into();
        let symbol = symbol.into();

        info!(
            job_type = %job_type,
            symbol = %symbol,
            "Download job started"
        );

        Self {
            job_type,
            symbol,
            start_time: Instant::now(),
        }
    }

    /// Record successful download completion
    pub fn record_success(&self, items_count: u64) {
        let duration = self.start_time.elapsed();

        counter!(
            "downloads_completed_total",
            "job_type" => self.job_type.clone(),
            "symbol" => self.symbol.clone(),
        )
        .increment(1);

        info!(
            job_type = %self.job_type,
            symbol = %self.symbol,
            items_count = items_count,
            duration_secs = duration.as_secs(),
            "Download completed successfully"
        );
    }

    /// Record failed download
    pub fn record_failure(&self, error: &str) {
        let duration = self.start_time.elapsed();

        counter!(
            "downloads_failed_total",
            "job_type" => self.job_type.clone(),
            "symbol" => self.symbol.clone(),
            "error" => error.to_string(),
        )
        .increment(1);

        error!(
            job_type = %self.job_type,
            symbol = %self.symbol,
            error = %error,
            duration_secs = duration.as_secs(),
            "Download failed"
        );
    }
}

/// Check if metrics system is initialized
pub async fn is_initialized() -> bool {
    *METRICS_INITIALIZED.read().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_correlation_id_generation() {
        let id1 = generate_correlation_id().await;
        let id2 = generate_correlation_id().await;

        assert_ne!(id1, id2);
        assert!(id1.starts_with("req-"));
        assert!(id2.starts_with("req-"));
    }

    #[tokio::test]
    async fn test_http_request_metrics_lifecycle() {
        let metrics = HttpRequestMetrics::start("/fapi/v1/klines", 1).await;
        assert!(!metrics.correlation_id.is_empty());

        // Simulate some work
        tokio::time::sleep(Duration::from_millis(10)).await;

        metrics.record_complete(200);
    }

    #[test]
    fn test_rate_limiter_metrics() {
        let mut metrics = RateLimiterMetrics::new();
        metrics.start_acquire();
        metrics.record_acquired(5);
        metrics.update_available_permits(95);
    }

    #[test]
    fn test_download_metrics() {
        let metrics = DownloadMetrics::start("bars", "BTCUSDT");
        metrics.record_success(1000);

        let metrics2 = DownloadMetrics::start("aggtrades", "ETHUSDT");
        metrics2.record_failure("Network timeout");
    }
}
