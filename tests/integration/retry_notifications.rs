use std::time::Duration;

use trading_data_downloader::fetcher::retry_formatter::{RetryContext, RetryErrorType};

#[test]
fn retry_messages_match_spec_format() {
    let ctx = RetryContext::new(
        1,
        5,
        RetryErrorType::NetworkTimeout,
        Duration::from_secs(2),
        "ETHUSDT",
        Some((1_701_596_800_000, 1_701_683_200_000)),
        "network timeout",
        "/fapi/v1/klines",
    );
    let retry = ctx.format_retry();
    assert!(retry.contains("Retrying"));
    assert!(retry.contains("attempt 1/5"));
    assert!(retry.contains("2.0 seconds"));
    assert!(retry.contains("ETHUSDT"));

    let success = ctx.format_success();
    assert!(success.contains("Retry attempt 1/5 succeeded"));
}

#[test]
fn failure_messages_include_suggestions() {
    let ctx = RetryContext::new(
        5,
        5,
        RetryErrorType::AuthFailed(401),
        Duration::from_secs(0),
        "BTCUSDT",
        None,
        "auth failure",
        "/fapi/v1/account",
    );
    let failure = ctx.format_failure();
    assert!(failure.contains("Download failed"));
    assert!(failure.contains("Authentication failed"));
    assert!(failure.contains("--max-retries"));
}
