//! Integration tests for rate limiting functionality

use std::time::Duration;
use trading_data_downloader::downloader::RateLimiter;

// T028: Weight-based rate limiting

#[test]
fn test_rate_limiter_weight_based_initialization() {
    let limiter = RateLimiter::weight_based(2400, Duration::from_secs(60));
    assert!(limiter.is_weight_based());
}

#[test]
fn test_rate_limiter_request_based_initialization() {
    let limiter = RateLimiter::request_based(50, Duration::from_secs(10));
    assert!(!limiter.is_weight_based());
}

#[tokio::test]
async fn test_rate_limiter_acquire_single_weight() {
    let limiter = RateLimiter::weight_based(10, Duration::from_millis(100));

    // Acquire 1 weight unit
    limiter.acquire(1).await.unwrap();

    // Should succeed immediately (weight available)
    limiter.acquire(1).await.unwrap();
}

#[tokio::test]
async fn test_rate_limiter_acquire_multiple_weights() {
    let limiter = RateLimiter::weight_based(100, Duration::from_secs(1));

    // Acquire 10 weight units
    limiter.acquire(10).await.unwrap();

    // Acquire another 20 weight units
    limiter.acquire(20).await.unwrap();

    // Total 30 weight units acquired (still within 100 limit)
}

#[tokio::test]
async fn test_rate_limiter_request_based_acquire() {
    let limiter = RateLimiter::request_based(10, Duration::from_secs(1));

    // Each acquire counts as 1 request
    limiter.acquire(1).await.unwrap();
    limiter.acquire(1).await.unwrap();
    limiter.acquire(1).await.unwrap();
}

// T029: Exponential backoff on 429 responses

#[tokio::test]
async fn test_rate_limiter_handle_429_exponential_backoff() {
    let limiter = RateLimiter::weight_based(2400, Duration::from_secs(60));

    // Simulate first 429 response
    let delay1 = limiter.handle_rate_limit_error(1).await;
    assert!(delay1 > Duration::from_secs(0));

    // Simulate second 429 response (should be longer)
    let delay2 = limiter.handle_rate_limit_error(2).await;
    assert!(
        delay2 > delay1,
        "Second delay should be longer (exponential)"
    );

    // Simulate third 429 response (should be even longer)
    let delay3 = limiter.handle_rate_limit_error(3).await;
    assert!(delay3 > delay2, "Third delay should be even longer");
}

#[tokio::test]
async fn test_rate_limiter_backoff_reset_on_success() {
    let mut limiter = RateLimiter::weight_based(2400, Duration::from_secs(60));

    // Simulate 429 response
    let delay1 = limiter.handle_rate_limit_error(1).await;
    assert!(delay1 > Duration::from_secs(0));

    // Reset backoff after successful request
    limiter.reset_backoff();

    // Next 429 should start from base delay again
    let delay2 = limiter.handle_rate_limit_error(1).await;
    assert_eq!(delay1, delay2, "Delay should reset to base after success");
}

#[tokio::test]
async fn test_rate_limiter_max_backoff_cap() {
    let limiter = RateLimiter::weight_based(2400, Duration::from_secs(60));

    // Simulate many 429 responses
    let mut last_delay = Duration::from_secs(0);
    for attempt in 1..=10 {
        let delay = limiter.handle_rate_limit_error(attempt).await;
        assert!(delay >= last_delay);
        last_delay = delay;
    }

    // Verify there's a reasonable maximum (e.g., < 5 minutes)
    assert!(
        last_delay < Duration::from_secs(300),
        "Backoff should have a reasonable maximum cap"
    );
}
