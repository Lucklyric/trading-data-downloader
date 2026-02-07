//! Shared resources for all fetcher instances
//!
//! This module provides global singleton instances of HTTP client and rate limiters
//! to ensure rate limits are properly shared across all concurrent download operations.
//!
//! # Critical for Production
//!
//! Binance enforces rate limits per IP address (2400 weight/60s for Futures endpoints).
//! If each fetcher instance creates its own rate limiter, concurrent downloads will
//! bypass the shared quota and risk IP bans.

use once_cell::sync::Lazy;
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;

use crate::downloader::rate_limit::RateLimiter;

/// HTTP connect timeout (seconds) - time to establish TCP connection
const HTTP_CONNECT_TIMEOUT_SECS: u64 = 10;
/// HTTP request timeout (seconds) - overall time for the entire request
const HTTP_REQUEST_TIMEOUT_SECS: u64 = 30;

/// Global HTTP client shared by all fetcher instances
///
/// The reqwest::Client is designed to be cloned cheaply (uses Arc internally),
/// but we explicitly use a global instance to ensure connection pooling works
/// optimally across all download operations.
///
/// Configured with explicit timeouts to prevent indefinite hangs:
/// - Connect timeout: 10 seconds
/// - Request timeout: 30 seconds
pub static GLOBAL_HTTP_CLIENT: Lazy<Arc<Client>> = Lazy::new(|| {
    Arc::new(
        Client::builder()
            .connect_timeout(Duration::from_secs(HTTP_CONNECT_TIMEOUT_SECS))
            .timeout(Duration::from_secs(HTTP_REQUEST_TIMEOUT_SECS))
            .build()
            .unwrap_or_else(|e| {
                panic!("FATAL: Failed to build HTTP client: {}. Check system TLS configuration.", e);
            }),
    )
});

/// Global rate limiter for Binance Futures endpoints (both USDT and COIN)
///
/// Binance Futures uses a unified rate limit across both FAPI (USDT-margined) and
/// DAPI (COIN-margined) endpoints:
/// - 2400 weight per 1 minute
///
/// All fetcher instances MUST share this limiter to avoid exceeding quotas.
pub static GLOBAL_BINANCE_RATE_LIMITER: Lazy<Arc<RateLimiter>> =
    Lazy::new(|| Arc::new(RateLimiter::weight_based(2400, Duration::from_secs(60))));

/// Get the global HTTP client
///
/// Returns a clone of the Arc, which is cheap (just increments ref count)
pub fn global_http_client() -> Arc<Client> {
    GLOBAL_HTTP_CLIENT.clone()
}

/// Get the global Binance rate limiter
///
/// Returns a clone of the Arc, which is cheap (just increments ref count)
pub fn global_binance_rate_limiter() -> Arc<RateLimiter> {
    GLOBAL_BINANCE_RATE_LIMITER.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_client_is_shared() {
        let client1 = global_http_client();
        let client2 = global_http_client();

        // Verify they're the same Arc (same allocation)
        assert!(Arc::ptr_eq(&client1, &client2));
    }

    #[test]
    fn test_global_rate_limiter_is_shared() {
        let limiter1 = global_binance_rate_limiter();
        let limiter2 = global_binance_rate_limiter();

        // Verify they're the same Arc (same allocation)
        assert!(Arc::ptr_eq(&limiter1, &limiter2));
    }
}
