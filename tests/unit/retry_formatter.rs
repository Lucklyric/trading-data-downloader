use std::time::Duration;

use reqwest::StatusCode;
use trading_data_downloader::fetcher::retry_formatter::{
    extract_error_type, RetryContext, RetryErrorType,
};

fn sample_context(error_type: RetryErrorType) -> RetryContext {
    RetryContext::new(
        2,
        5,
        error_type,
        Duration::from_secs(4),
        "BTCUSDT",
        Some((1_701_665_600_000, 1_701_752_000_000)),
        "network timeout",
        "/fapi/v1/klines",
    )
}

#[test]
fn format_retry_captures_attempt_and_wait() {
    let ctx = sample_context(RetryErrorType::RateLimit);
    let message = ctx.format_retry();
    assert!(message.contains("attempt 2/5"));
    assert!(message.contains("rate limit exceeded"));
    assert!(message.contains("4.0 seconds"));
    assert!(message.contains("BTCUSDT"));
    assert!(message.contains("2024-01-"));
}

#[test]
fn format_success_includes_symbol_context() {
    let ctx = sample_context(RetryErrorType::NetworkTimeout);
    let message = ctx.format_success();
    assert!(message.contains("Retry attempt 2/5 succeeded"));
    assert!(message.contains("BTCUSDT"));
}

#[test]
fn format_failure_lists_suggestions() {
    let ctx = sample_context(RetryErrorType::ServerError(502));
    let output = ctx.format_failure();
    assert!(output.contains("Download failed after 5 attempts"));
    assert!(output.contains("bad gateway"));
    assert!(output.contains("Check your network connection"));
    assert!(output.contains("--max-retries"));
}

#[test]
fn extract_error_type_classifies_status_codes() {
    let invalid = extract_error_type(Some(StatusCode::BAD_REQUEST), None);
    assert!(matches!(invalid, RetryErrorType::InvalidRequest));

    let auth = extract_error_type(Some(StatusCode::UNAUTHORIZED), None);
    assert!(matches!(auth, RetryErrorType::AuthFailed(401)));

    let rate_limit = extract_error_type(Some(StatusCode::TOO_MANY_REQUESTS), None);
    assert_eq!(rate_limit, RetryErrorType::RateLimit);

    let server = extract_error_type(Some(StatusCode::INTERNAL_SERVER_ERROR), None);
    assert_eq!(server, RetryErrorType::ServerError(500));

    let generic = extract_error_type(None, None);
    assert_eq!(generic, RetryErrorType::NetworkGeneric);
}
