//! Download configuration constants

use std::time::Duration;

/// Maximum number of retries for failed downloads.
/// 5 retries with exponential backoff allows recovery from transient network issues
/// while avoiding infinite loops on persistent failures (max total wait ~1 minute).
pub const MAX_RETRIES: u32 = 5;

/// Initial backoff delay in milliseconds.
/// 1 second is long enough for rate limit windows to reset but short enough
/// to not overly delay recovery from transient errors.
pub const INITIAL_BACKOFF_MS: u64 = 1000; // 1 second

/// Maximum backoff delay in milliseconds.
/// 30 seconds caps exponential backoff to prevent excessive wait times
/// (retry 5 = 32s capped to 30s, total max wait with 5 retries ~63s).
pub const MAX_BACKOFF_MS: u64 = 30000; // 30 seconds

/// Checkpoint interval for bars (save state every N bars).
/// 10,000 bars balances resume granularity vs checkpoint I/O overhead.
/// At 1-minute interval, this is ~7 days of data per checkpoint.
pub const CHECKPOINT_INTERVAL_BARS: u64 = 10_000;

/// Checkpoint interval for aggregate trades (save state every N trades).
/// 10,000 trades balances resume granularity vs checkpoint I/O overhead.
/// Typical trading activity produces 10K trades every few minutes to hours.
pub const CHECKPOINT_INTERVAL_TRADES: u64 = 10_000;

/// Checkpoint interval for funding rates (save state every N rates).
/// 1,000 rates is lower than bars/trades since funding rates are less frequent
/// (3 per day = ~1 year of data per checkpoint).
pub const CHECKPOINT_INTERVAL_FUNDING: u64 = 1_000;

/// Flush interval for output writers (flush every N items)
pub const FLUSH_INTERVAL: usize = 1_000;

/// Calculate exponential backoff delay
pub fn calculate_backoff(retry_count: u32) -> Duration {
    let delay_ms = INITIAL_BACKOFF_MS * 2u64.pow(retry_count);
    let delay_ms = delay_ms.min(MAX_BACKOFF_MS);
    Duration::from_millis(delay_ms)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_calculation() {
        assert_eq!(calculate_backoff(0), Duration::from_millis(1000));
        assert_eq!(calculate_backoff(1), Duration::from_millis(2000));
        assert_eq!(calculate_backoff(2), Duration::from_millis(4000));
        assert_eq!(calculate_backoff(3), Duration::from_millis(8000));
        assert_eq!(calculate_backoff(4), Duration::from_millis(16000));
        // Should cap at MAX_BACKOFF_MS
        assert_eq!(calculate_backoff(10), Duration::from_millis(MAX_BACKOFF_MS));
    }
}
