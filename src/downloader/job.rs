//! Download job structures and status tracking (T046-T047)

use crate::Interval;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Job type specifies what kind of data to download
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobType {
    /// OHLCV bars
    Bars {
        /// Time interval for candlestick aggregation
        interval: Interval,
    },
    /// Aggregate trades
    AggTrades,
    /// Funding rates
    FundingRates,
}

/// Download job specification (T046)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadJob {
    /// Exchange identifier (e.g., "BINANCE:BTC/USDT:USDT")
    pub identifier: String,
    /// Trading symbol (e.g., "BTCUSDT")
    pub symbol: String,
    /// Job type (bars, aggtrades, funding)
    pub job_type: JobType,
    /// Start time (Unix timestamp in milliseconds)
    pub start_time: i64,
    /// End time (Unix timestamp in milliseconds)
    pub end_time: i64,
    /// Output file path
    pub output_path: PathBuf,
    /// Resume directory (optional)
    pub resume_dir: Option<PathBuf>,
    /// Current job status
    #[serde(default)]
    pub status: JobStatus,
    /// Job progress tracking
    #[serde(default)]
    pub progress: JobProgress,
}

impl DownloadJob {
    /// Create a new download job for bars
    pub fn new_bars(
        identifier: String,
        symbol: String,
        interval: Interval,
        start_time: i64,
        end_time: i64,
        output_path: PathBuf,
    ) -> Self {
        Self {
            identifier,
            symbol,
            job_type: JobType::Bars { interval },
            start_time,
            end_time,
            output_path,
            resume_dir: None,
            status: JobStatus::Pending,
            progress: JobProgress::default(),
        }
    }

    /// Create a new download job for aggregate trades
    pub fn new_aggtrades(
        identifier: String,
        symbol: String,
        start_time: i64,
        end_time: i64,
        output_path: PathBuf,
    ) -> Self {
        Self {
            identifier,
            symbol,
            job_type: JobType::AggTrades,
            start_time,
            end_time,
            output_path,
            resume_dir: None,
            status: JobStatus::Pending,
            progress: JobProgress::default(),
        }
    }

    /// Create a new download job for funding rates
    pub fn new_funding(
        identifier: String,
        symbol: String,
        start_time: i64,
        end_time: i64,
        output_path: PathBuf,
    ) -> Self {
        Self {
            identifier,
            symbol,
            job_type: JobType::FundingRates,
            start_time,
            end_time,
            output_path,
            resume_dir: None,
            status: JobStatus::Pending,
            progress: JobProgress::default(),
        }
    }

    /// Legacy constructor for backwards compatibility
    pub fn new(
        identifier: String,
        symbol: String,
        interval: Interval,
        start_time: i64,
        end_time: i64,
        output_path: PathBuf,
    ) -> Self {
        Self::new_bars(
            identifier,
            symbol,
            interval,
            start_time,
            end_time,
            output_path,
        )
    }

    /// Validate job parameters
    pub fn validate(&self) -> Result<(), String> {
        if self.end_time <= self.start_time {
            return Err(format!(
                "End time ({}) must be after start time ({})",
                self.end_time, self.start_time
            ));
        }

        if self.symbol.is_empty() {
            return Err("Symbol cannot be empty".to_string());
        }

        if self.identifier.is_empty() {
            return Err("Identifier cannot be empty".to_string());
        }

        Ok(())
    }
}

/// Job execution status (T047)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum JobStatus {
    /// Job has not started yet
    #[default]
    Pending,
    /// Job is currently running
    InProgress,
    /// Job completed successfully
    Completed,
    /// Job failed with error
    Failed,
    /// Job was cancelled
    Cancelled,
}

/// Job progress tracking (T047)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct JobProgress {
    /// Total number of bars expected
    pub total_bars: Option<u64>,
    /// Number of bars downloaded so far
    pub downloaded_bars: u64,
    /// Number of bars written to output
    pub written_bars: u64,
    /// Current position (timestamp in milliseconds)
    pub current_position: Option<i64>,
    /// Number of API requests made
    pub api_requests: u64,
    /// Number of retries attempted
    pub retries: u64,
    /// Error message if job failed
    pub error: Option<String>,
}

impl JobProgress {
    /// Calculate download percentage (0.0 to 100.0)
    pub fn percentage(&self) -> Option<f64> {
        self.total_bars.map(|total| {
            if total == 0 {
                100.0
            } else {
                (self.downloaded_bars as f64 / total as f64) * 100.0
            }
        })
    }

    /// Check if download is complete
    pub fn is_complete(&self) -> bool {
        if let Some(total) = self.total_bars {
            self.downloaded_bars >= total
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_download_job_creation() {
        let job = DownloadJob::new(
            "BINANCE:BTC/USDT:USDT".to_string(),
            "BTCUSDT".to_string(),
            Interval::from_str("1m").unwrap(),
            1704067200000,
            1704153600000,
            PathBuf::from("/tmp/output.csv"),
        );

        assert_eq!(job.identifier, "BINANCE:BTC/USDT:USDT");
        assert_eq!(job.symbol, "BTCUSDT");
        if let JobType::Bars { interval } = job.job_type {
            assert_eq!(interval.to_string(), "1m");
        } else {
            panic!("Expected Bars job type");
        }
        assert_eq!(job.status, JobStatus::Pending);
        assert_eq!(job.progress.downloaded_bars, 0);
    }

    #[test]
    fn test_download_job_validation() {
        let job = DownloadJob::new(
            "BINANCE:BTC/USDT:USDT".to_string(),
            "BTCUSDT".to_string(),
            Interval::from_str("1m").unwrap(),
            1704067200000,
            1704153600000,
            PathBuf::from("/tmp/output.csv"),
        );

        assert!(job.validate().is_ok());

        // Invalid time range
        let invalid_job = DownloadJob::new(
            "BINANCE:BTC/USDT:USDT".to_string(),
            "BTCUSDT".to_string(),
            Interval::from_str("1m").unwrap(),
            1704153600000,
            1704067200000,
            PathBuf::from("/tmp/output.csv"),
        );

        assert!(invalid_job.validate().is_err());
    }

    #[test]
    fn test_job_progress_percentage() {
        let mut progress = JobProgress::default();
        assert_eq!(progress.percentage(), None);

        progress.total_bars = Some(100);
        assert_eq!(progress.percentage(), Some(0.0));

        progress.downloaded_bars = 50;
        assert_eq!(progress.percentage(), Some(50.0));

        progress.downloaded_bars = 100;
        assert_eq!(progress.percentage(), Some(100.0));
        assert!(progress.is_complete());
    }
}
