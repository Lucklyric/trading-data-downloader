//! Download configuration constants

use std::time::Duration;

/// Maximum number of retries for failed downloads
pub const MAX_RETRIES: u32 = 5;

/// Initial backoff delay in milliseconds
pub const INITIAL_BACKOFF_MS: u64 = 1000; // 1 second

/// Maximum backoff delay in milliseconds
pub const MAX_BACKOFF_MS: u64 = 30000; // 30 seconds

/// Checkpoint interval for bars (save state every N bars)
pub const CHECKPOINT_INTERVAL_BARS: u64 = 10_000;

/// Checkpoint interval for aggregate trades (save state every N trades)
pub const CHECKPOINT_INTERVAL_TRADES: u64 = 10_000;

/// Checkpoint interval for funding rates (save state every N rates)
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
