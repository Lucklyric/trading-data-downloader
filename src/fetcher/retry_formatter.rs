//! Retry message formatting utilities for Binance HTTP client enhancements.
//!
//! Feature 002 introduces dedicated data structures for managing retry context and
//! formatting consistent, user-friendly log messages. Phase 2 focuses on defining
//! the foundational types so later phases can add the actual formatting logic.

use chrono::{DateTime, Utc};
use reqwest::{Error as ReqwestError, StatusCode};
use std::time::Duration;

/// Classification of retry errors for user messaging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryErrorType {
    /// Network timeout or connection stalled long enough to trigger a timeout
    NetworkTimeout,
    /// Connection refused, DNS failure, or other offline scenarios
    NetworkOffline,
    /// HTTP 429 rate limit exceeded
    RateLimit,
    /// HTTP 5xx server error
    ServerError(u16),
    /// HTTP 400 invalid request / bad symbol
    InvalidRequest,
    /// Authentication failures (401/403)
    AuthFailed(u16),
    /// Other client errors (4xx, except 429)
    ClientError(u16),
    /// Generic fallback when no better classification fits
    NetworkGeneric,
}

impl RetryErrorType {
    /// User-friendly description string used inside retry log messages.
    pub fn description(&self) -> &'static str {
        match self {
            Self::NetworkTimeout => "network timeout",
            Self::NetworkOffline => "connection failed",
            Self::RateLimit => "rate limit exceeded",
            Self::ServerError(code) => match code {
                500 => "internal server error",
                502 => "bad gateway",
                503 => "service unavailable",
                504 => "gateway timeout",
                _ => "server error",
            },
            Self::InvalidRequest => "invalid request",
            Self::AuthFailed(code) => match code {
                401 => "authentication failed (401)",
                403 => "authentication failed (403)",
                _ => "authentication failed",
            },
            Self::ClientError(code) => match code {
                404 => "resource not found",
                451 => "unavailable due to restrictions",
                _ => "client error",
            },
            Self::NetworkGeneric => "network error",
        }
    }

    /// Suggested remediation presented with actionable guidance after failures.
    pub fn suggestion(&self) -> &'static str {
        match self {
            Self::NetworkTimeout => "Check your network connection and firewall settings",
            Self::NetworkOffline => "Verify internet connectivity and DNS resolution",
            Self::RateLimit => "Consider reducing request rate or waiting longer",
            Self::ServerError(_) => "Exchange may be experiencing issues, try again later",
            Self::InvalidRequest => "Check symbol, interval, and date range arguments for typos",
            Self::AuthFailed(_) => "Verify your API key, secret, and permissions",
            Self::ClientError(_) => "Review request parameters or consult Binance API docs",
            Self::NetworkGeneric => "Check network connectivity and try again",
        }
    }

    /// Determine whether the error type is typically retryable.
    pub fn is_retryable(&self) -> bool {
        !matches!(
            self,
            RetryErrorType::InvalidRequest
                | RetryErrorType::AuthFailed(_)
                | RetryErrorType::ClientError(_)
        )
    }
}

/// Context for formatting retry messages.
#[derive(Debug, Clone)]
pub struct RetryContext {
    /// Current attempt number (1-based)
    pub attempt: usize,
    /// Maximum number of attempts configured
    pub max_attempts: usize,
    /// Type of error that triggered retry
    pub error_type: RetryErrorType,
    /// Backoff duration until next attempt
    pub backoff_duration: Duration,
    /// Symbol being downloaded (e.g., "BTCUSDT")
    pub symbol: String,
    /// Date range for context (start, end) in millis
    pub date_range: Option<(i64, i64)>,
    /// Original error message for details
    pub error_message: String,
    /// URL or endpoint that failed
    pub endpoint: String,
}

impl RetryContext {
    /// Convenience constructor used throughout the retry logic.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        attempt: usize,
        max_attempts: usize,
        error_type: RetryErrorType,
        backoff_duration: Duration,
        symbol: impl Into<String>,
        date_range: Option<(i64, i64)>,
        error_message: impl Into<String>,
        endpoint: impl Into<String>,
    ) -> Self {
        Self {
            attempt,
            max_attempts,
            error_type,
            backoff_duration,
            symbol: symbol.into(),
            date_range,
            error_message: error_message.into(),
            endpoint: endpoint.into(),
        }
    }

    /// Format standardized retry message with attempt counters and context.
    pub fn format_retry(&self) -> String {
        let mut message = format!(
            "Retrying (attempt {}/{}) after {} - waiting {:.1} seconds...",
            self.attempt,
            self.max_attempts,
            self.error_type.description(),
            self.backoff_duration.as_secs_f64()
        );

        append_symbol_and_range(&mut message, &self.symbol, self.date_range);
        message
    }

    /// Format retry success message when a previous attempt eventually works.
    pub fn format_success(&self) -> String {
        let mut message = format!(
            "Retry attempt {}/{} succeeded - resuming download",
            self.attempt, self.max_attempts
        );
        append_symbol_and_range(&mut message, &self.symbol, self.date_range);
        message
    }

    /// Format final failure summary with actionable suggestions.
    pub fn format_failure(&self) -> String {
        let mut lines = Vec::new();
        lines.push(format!(
            "[FAILED] Download failed after {} attempts",
            self.max_attempts
        ));
        lines.push(format!("  Last error: {}", self.error_message));

        let symbol_display = if self.symbol.is_empty() {
            "unknown"
        } else {
            &self.symbol
        };
        lines.push(format!("  Symbol: {symbol_display}"));

        let range_display = self
            .date_range
            .map(|(start, end)| format!("{} to {}", format_timestamp(start), format_timestamp(end)))
            .unwrap_or_else(|| "unknown".to_string());
        lines.push(format!("  Date range: {range_display}"));
        lines.push(format!("  Endpoint: {}", self.endpoint));
        lines.push("  Suggestions:".to_string());

        for suggestion in self.format_suggestions() {
            lines.push(format!("    - {suggestion}"));
        }

        lines.join("\n")
    }

    /// Derive suggestions tailored to the current retry context.
    pub fn format_suggestions(&self) -> Vec<String> {
        let mut suggestions = vec![self.error_type.suggestion().to_string()];
        suggestions.push(format!(
            "Try increasing --max-retries (current: {})",
            self.max_attempts
        ));
        suggestions.push("Check exchange status at https://binance.com/status".to_string());
        suggestions
    }
}

/// Extract a [`RetryErrorType`] from an HTTP status or reqwest error.
pub fn extract_error_type(
    status: Option<StatusCode>,
    err: Option<&ReqwestError>,
) -> RetryErrorType {
    if let Some(status) = status {
        match status.as_u16() {
            400 => return RetryErrorType::InvalidRequest,
            401 | 403 => return RetryErrorType::AuthFailed(status.as_u16()),
            429 => return RetryErrorType::RateLimit,
            _ => {}
        }

        if status.is_server_error() {
            return RetryErrorType::ServerError(status.as_u16());
        }

        if status.is_client_error() {
            return RetryErrorType::ClientError(status.as_u16());
        }
    }

    if let Some(err) = err {
        if err.is_timeout() {
            return RetryErrorType::NetworkTimeout;
        }

        if err.is_connect() {
            return RetryErrorType::NetworkOffline;
        }
    }

    RetryErrorType::NetworkGeneric
}

fn append_symbol_and_range(buffer: &mut String, symbol: &str, date_range: Option<(i64, i64)>) {
    if !symbol.is_empty() {
        buffer.push(' ');
        buffer.push('(');
        buffer.push_str(symbol);
        buffer.push(')');
    }

    if let Some((start, end)) = date_range {
        buffer.push(' ');
        buffer.push_str(&format!(
            "{} to {}",
            format_timestamp(start),
            format_timestamp(end)
        ));
    }
}

fn format_timestamp(millis: i64) -> String {
    DateTime::<Utc>::from_timestamp_millis(millis)
        .unwrap_or_else(Utc::now)
        .format("%Y-%m-%d")
        .to_string()
}
