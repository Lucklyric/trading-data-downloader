//! Download executor with retry and resume capability (T074-T080)
//! Refactored to eliminate code duplication and use fetcher factory

use crate::downloader::config::{
    calculate_backoff, CHECKPOINT_INTERVAL_BARS, CHECKPOINT_INTERVAL_FUNDING,
    CHECKPOINT_INTERVAL_TRADES, MAX_RETRIES,
};
use crate::downloader::{DownloadError, DownloadJob, JobProgress, JobStatus};
use crate::fetcher::{create_fetcher, DataFetcher};
use crate::identifier::ExchangeIdentifier;
use crate::output::csv::{CsvAggTradesWriter, CsvBarsWriter, CsvFundingWriter};
use crate::output::{AggTradesWriter, BarsWriter, FundingWriter, OutputWriter};
use crate::resume::checkpoint::{Checkpoint, CheckpointType};
use crate::resume::state::ResumeState;
use crate::{AggTrade, Bar, FundingRate};
use futures_util::StreamExt;
use std::path::PathBuf;
use tracing::{debug, error, info, warn};

/// Safe enum wrapper for different writer types
/// This replaces unsafe type erasure with a type-safe enum dispatch pattern
enum WriterType {
    Bars(CsvBarsWriter),
    AggTrades(CsvAggTradesWriter),
    Funding(CsvFundingWriter),
}

impl WriterType {
    /// Write a bar to the bars writer
    fn write_bar(&mut self, bar: &Bar) -> Result<(), DownloadError> {
        match self {
            WriterType::Bars(writer) => writer
                .write_bar(bar)
                .map_err(|e| DownloadError::OutputError(e.to_string())),
            _ => Err(DownloadError::ValidationError(
                "Attempted to write Bar to non-Bars writer".to_string(),
            )),
        }
    }

    /// Write an aggregate trade to the trades writer
    fn write_aggtrade(&mut self, trade: &AggTrade) -> Result<(), DownloadError> {
        match self {
            WriterType::AggTrades(writer) => writer
                .write_aggtrade(trade)
                .map_err(|e| DownloadError::OutputError(e.to_string())),
            _ => Err(DownloadError::ValidationError(
                "Attempted to write AggTrade to non-AggTrades writer".to_string(),
            )),
        }
    }

    /// Write a funding rate to the funding writer
    fn write_funding(&mut self, rate: &FundingRate) -> Result<(), DownloadError> {
        match self {
            WriterType::Funding(writer) => writer
                .write_funding(rate)
                .map_err(|e| DownloadError::OutputError(e.to_string())),
            _ => Err(DownloadError::ValidationError(
                "Attempted to write FundingRate to non-Funding writer".to_string(),
            )),
        }
    }

    /// Close the writer
    fn close(self) -> Result<(), DownloadError> {
        match self {
            WriterType::Bars(writer) => writer
                .close()
                .map_err(|e| DownloadError::OutputError(e.to_string())),
            WriterType::AggTrades(writer) => writer
                .close()
                .map_err(|e| DownloadError::OutputError(e.to_string())),
            WriterType::Funding(writer) => writer
                .close()
                .map_err(|e| DownloadError::OutputError(e.to_string())),
        }
    }

    /// Get the count of items written
    fn items_written(&self) -> u64 {
        match self {
            WriterType::Bars(_) => 0, // CsvBarsWriter doesn't track count
            WriterType::AggTrades(writer) => writer.trades_written(),
            WriterType::Funding(writer) => writer.rates_written(),
        }
    }
}

/// Download executor orchestrates the complete download workflow (T075)
pub struct DownloadExecutor {
    resume_dir: Option<PathBuf>,
    max_retries: u32,
}

impl DownloadExecutor {
    /// Create a new download executor without resume capability
    pub fn new() -> Self {
        Self {
            resume_dir: None,
            max_retries: MAX_RETRIES,
        }
    }

    /// Create a new download executor with resume capability
    pub fn new_with_resume<P: Into<PathBuf>>(resume_dir: P) -> Self {
        let resume_dir = resume_dir.into();
        std::fs::create_dir_all(&resume_dir).ok();

        Self {
            resume_dir: Some(resume_dir),
            max_retries: MAX_RETRIES,
        }
    }

    /// Set maximum number of retries
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Generate resume state file path for a job
    fn get_resume_state_path(&self, job: &DownloadJob) -> Option<PathBuf> {
        self.resume_dir.as_ref().map(|dir| {
            let data_type_str = match job.job_type {
                crate::downloader::JobType::Bars { interval } => {
                    format!("bars_{}", interval)
                }
                crate::downloader::JobType::AggTrades => "aggtrades".to_string(),
                crate::downloader::JobType::FundingRates => "funding".to_string(),
            };
            let filename = format!(
                "{}_{}_{}_{}.json",
                job.identifier.replace([':', '/'], "_"),
                data_type_str,
                job.start_time,
                job.end_time
            );
            dir.join(filename)
        })
    }

    /// Load existing resume state if available (T079)
    fn load_resume_state(&self, job: &DownloadJob) -> Result<Option<i64>, DownloadError> {
        let path = match self.get_resume_state_path(job) {
            Some(p) => p,
            None => return Ok(None),
        };

        if !path.exists() {
            debug!("No resume state found at {}", path.display());
            return Ok(None);
        }

        match ResumeState::load(&path) {
            Ok(state) => {
                // Find latest checkpoint
                if let Some(checkpoint) = state.checkpoints().last() {
                    // Extract end_time from checkpoint type
                    if let CheckpointType::TimeWindow { end_time, .. } =
                        checkpoint.checkpoint_type()
                    {
                        info!("Resuming from checkpoint: timestamp={}", end_time);
                        return Ok(Some(*end_time));
                    }
                }
                Ok(None)
            }
            Err(e) => {
                warn!("Failed to load resume state: {}", e);
                Ok(None)
            }
        }
    }

    /// Save checkpoint during download (T080)
    async fn save_checkpoint(
        &self,
        job: &DownloadJob,
        checkpoint: Checkpoint,
    ) -> Result<(), DownloadError> {
        if let Some(path) = self.get_resume_state_path(job) {
            let mut state = if path.exists() {
                ResumeState::load(&path).map_err(|e| {
                    DownloadError::IoError(format!("Failed to load state: {}", e))
                })?
            } else {
                ResumeState::new(
                    job.identifier.clone(),
                    job.symbol.clone(),
                    match job.job_type {
                        crate::downloader::JobType::Bars { interval } => {
                            Some(interval.to_string())
                        }
                        _ => None,
                    },
                    match job.job_type {
                        crate::downloader::JobType::Bars { .. } => "bars".to_string(),
                        crate::downloader::JobType::AggTrades => "aggtrades".to_string(),
                        crate::downloader::JobType::FundingRates => "funding".to_string(),
                    },
                )
            };

            state.add_checkpoint(checkpoint);

            state.save(&path).map_err(|e| {
                DownloadError::IoError(format!("Failed to save checkpoint: {}", e))
            })?;

            debug!("Checkpoint saved to {}", path.display());
        }

        Ok(())
    }

    /// Clean up resume state after successful completion
    fn cleanup_resume_state(&self, job: &DownloadJob) {
        if let Some(path) = self.get_resume_state_path(job) {
            if path.exists() {
                std::fs::remove_file(&path).ok();
                debug!("Removed resume state file after successful completion");
            }
        }
    }

    /// Create fetcher from job identifier
    fn create_fetcher_from_job(
        &self,
        job: &DownloadJob,
    ) -> Result<Box<dyn DataFetcher>, DownloadError> {
        let identifier = ExchangeIdentifier::parse(&job.identifier)
            .map_err(|e| DownloadError::ValidationError(e.to_string()))?;

        create_fetcher(&identifier)
            .map_err(|e| DownloadError::FetcherError(format!("Failed to create fetcher: {}", e)))
    }

    /// Retry with exponential backoff
    async fn retry_with_backoff(&self, retry_count: u32, job: &mut DownloadJob) -> bool {
        job.progress.retries = retry_count as u64;

        if retry_count > self.max_retries {
            return false;
        }

        let backoff = calculate_backoff(retry_count);
        warn!(
            "Retry {}/{} after {:?} delay",
            retry_count, self.max_retries, backoff
        );
        tokio::time::sleep(backoff).await;

        true
    }

    /// Execute bars download job (T076)
    pub async fn execute_bars_job(
        &self,
        mut job: DownloadJob,
    ) -> Result<JobProgress, DownloadError> {
        info!("Starting bars download job: {:?}", job.identifier);

        // Validate job
        job.validate()
            .map_err(|e| DownloadError::ValidationError(e))?;

        // Extract interval from job type
        let interval = match job.job_type {
            crate::downloader::JobType::Bars { interval } => interval,
            _ => {
                return Err(DownloadError::ValidationError(
                    "Job type must be Bars for execute_bars_job".to_string(),
                ));
            }
        };

        // Load resume state
        let resume_time = self.load_resume_state(&job)?;
        let mut start_time = resume_time.unwrap_or(job.start_time);
        if resume_time.is_some() {
            job.progress.current_position = Some(start_time);
        }

        // Calculate expected total bars
        let interval_ms = interval.to_milliseconds();
        let total_duration = job.end_time - job.start_time;
        let total_bars = ((total_duration + interval_ms - 1) / interval_ms) as u64;
        job.progress.total_bars = Some(total_bars);

        info!(
            "Expected {} bars for range [{}, {})",
            total_bars, job.start_time, job.end_time
        );

        job.status = JobStatus::InProgress;

        // Create fetcher and writer
        let fetcher = self.create_fetcher_from_job(&job)?;
        let writer = CsvBarsWriter::new(&job.output_path)
            .map_err(|e| DownloadError::OutputError(e.to_string()))?;
        let mut writer = WriterType::Bars(writer);

        // Download with retry
        let result = self
            .download_bars_with_retry(
                &mut job,
                &*fetcher,
                &mut writer,
                &mut start_time,
                interval,
            )
            .await;

        // Finalize
        writer.close()?;

        if job.status == JobStatus::Completed {
            self.cleanup_resume_state(&job);
        }

        info!(
            "Job completed: status={:?}, bars_downloaded={}",
            job.status, job.progress.downloaded_bars
        );

        result
    }

    /// Execute aggTrades download job (T127)
    pub async fn execute_aggtrades_job(
        &self,
        mut job: DownloadJob,
    ) -> Result<JobProgress, DownloadError> {
        info!("Starting aggTrades download job: {:?}", job.identifier);

        // Validate job
        job.validate()
            .map_err(|e| DownloadError::ValidationError(e))?;

        // Load resume state
        let resume_time = self.load_resume_state(&job)?;
        let mut start_time = resume_time.unwrap_or(job.start_time);
        if resume_time.is_some() {
            job.progress.current_position = Some(start_time);
        }

        info!(
            "Fetching aggTrades for range [{}, {})",
            job.start_time, job.end_time
        );

        job.status = JobStatus::InProgress;

        // Create fetcher and writer
        let fetcher = self.create_fetcher_from_job(&job)?;
        let writer = CsvAggTradesWriter::new(&job.output_path)
            .map_err(|e| DownloadError::OutputError(e.to_string()))?;
        let mut writer = WriterType::AggTrades(writer);

        // Download with retry
        let result = self
            .download_aggtrades_with_retry(
                &mut job,
                &*fetcher,
                &mut writer,
                &mut start_time,
            )
            .await;

        // Finalize
        writer.close()?;

        if job.status == JobStatus::Completed {
            self.cleanup_resume_state(&job);
        }

        info!(
            "Job completed: status={:?}, trades_downloaded={}",
            job.status, job.progress.downloaded_bars
        );

        result
    }

    /// Execute funding rates download job (T149)
    pub async fn execute_funding_job(
        &self,
        mut job: DownloadJob,
    ) -> Result<JobProgress, DownloadError> {
        info!("Starting funding rates download job: {:?}", job.identifier);

        // Validate job
        job.validate()
            .map_err(|e| DownloadError::ValidationError(e))?;

        // Load resume state
        let resume_time = self.load_resume_state(&job)?;
        let mut start_time = resume_time.unwrap_or(job.start_time);
        if resume_time.is_some() {
            job.progress.current_position = Some(start_time);
        }

        info!(
            "Fetching funding rates for range [{}, {})",
            job.start_time, job.end_time
        );

        job.status = JobStatus::InProgress;

        // Create fetcher and writer
        let fetcher = self.create_fetcher_from_job(&job)?;
        let writer = CsvFundingWriter::new(&job.output_path)
            .map_err(|e| DownloadError::OutputError(e.to_string()))?;
        let mut writer = WriterType::Funding(writer);

        // Download with retry
        let result = self
            .download_funding_with_retry(
                &mut job,
                &*fetcher,
                &mut writer,
                &mut start_time,
            )
            .await;

        // Finalize
        writer.close()?;

        if job.status == JobStatus::Completed {
            self.cleanup_resume_state(&job);
        }

        info!(
            "Job completed: status={:?}, funding_rates_downloaded={}",
            job.status, job.progress.downloaded_bars
        );

        result
    }

    /// Download bars with retry logic - type-safe version for bars
    async fn download_bars_with_retry(
        &self,
        job: &mut DownloadJob,
        fetcher: &dyn DataFetcher,
        writer: &mut WriterType,
        start_time: &mut i64,
        interval: crate::Interval,
    ) -> Result<JobProgress, DownloadError> {
        let mut retry_count = 0;
        let mut last_error = None;

        loop {
            match fetcher.fetch_bars_stream(&job.symbol, interval, *start_time, job.end_time).await {
                Ok(mut stream) => {
                    // Process stream
                    while let Some(result) = stream.next().await {
                        match result {
                            Ok(bar) => {
                                // Write bar using safe enum dispatch
                                writer.write_bar(&bar)?;
                                job.progress.downloaded_bars += 1;
                                job.progress.written_bars += 1;

                                let timestamp = bar.close_time;
                                job.progress.current_position = Some(timestamp);

                                // Save checkpoint at intervals
                                if job.progress.downloaded_bars % CHECKPOINT_INTERVAL_BARS == 0 {
                                    let checkpoint = Checkpoint::time_window(
                                        job.start_time,
                                        timestamp,
                                        job.progress.downloaded_bars,
                                        0,
                                    );
                                    self.save_checkpoint(job, checkpoint).await?;
                                }
                            }
                            Err(e) => {
                                error!("Stream error: {}", e);
                                last_error = Some(e);
                                break;
                            }
                        }
                    }

                    // If we got here without error, download is complete
                    if last_error.is_none() {
                        job.status = JobStatus::Completed;
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to create stream: {}", e);
                    last_error = Some(e);
                }
            }

            // Retry logic
            retry_count += 1;
            if !self.retry_with_backoff(retry_count, job).await {
                job.status = JobStatus::Failed;
                let error_msg = last_error
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "Unknown error".to_string());
                job.progress.error = Some(error_msg.clone());
                return Err(DownloadError::NetworkError(format!(
                    "Max retries ({}) exceeded: {}",
                    self.max_retries, error_msg
                )));
            }

            // Continue from last known position
            if let Some(pos) = job.progress.current_position {
                *start_time = pos + 1;
            }
        }

        Ok(job.progress.clone())
    }

    /// Download aggTrades with retry logic - type-safe version for trades
    async fn download_aggtrades_with_retry(
        &self,
        job: &mut DownloadJob,
        fetcher: &dyn DataFetcher,
        writer: &mut WriterType,
        start_time: &mut i64,
    ) -> Result<JobProgress, DownloadError> {
        let mut retry_count = 0;
        let mut last_error = None;

        loop {
            match fetcher.fetch_aggtrades_stream(&job.symbol, *start_time, job.end_time).await {
                Ok(mut stream) => {
                    // Process stream
                    while let Some(result) = stream.next().await {
                        match result {
                            Ok(trade) => {
                                // Write trade using safe enum dispatch
                                writer.write_aggtrade(&trade)?;
                                job.progress.downloaded_bars += 1;
                                job.progress.written_bars = writer.items_written();

                                let timestamp = trade.timestamp;
                                job.progress.current_position = Some(timestamp);

                                // Save checkpoint at intervals
                                if job.progress.downloaded_bars % CHECKPOINT_INTERVAL_TRADES == 0 {
                                    let checkpoint = Checkpoint::time_window(
                                        job.start_time,
                                        timestamp,
                                        job.progress.downloaded_bars,
                                        0,
                                    );
                                    self.save_checkpoint(job, checkpoint).await?;
                                }
                            }
                            Err(e) => {
                                error!("Stream error: {}", e);
                                last_error = Some(e);
                                break;
                            }
                        }
                    }

                    // If we got here without error, download is complete
                    if last_error.is_none() {
                        job.status = JobStatus::Completed;
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to create stream: {}", e);
                    last_error = Some(e);
                }
            }

            // Retry logic
            retry_count += 1;
            if !self.retry_with_backoff(retry_count, job).await {
                job.status = JobStatus::Failed;
                let error_msg = last_error
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "Unknown error".to_string());
                job.progress.error = Some(error_msg.clone());
                return Err(DownloadError::NetworkError(format!(
                    "Max retries ({}) exceeded: {}",
                    self.max_retries, error_msg
                )));
            }

            // Continue from last known position
            if let Some(pos) = job.progress.current_position {
                *start_time = pos + 1;
            }
        }

        Ok(job.progress.clone())
    }

    /// Download funding rates with retry logic - type-safe version for funding
    async fn download_funding_with_retry(
        &self,
        job: &mut DownloadJob,
        fetcher: &dyn DataFetcher,
        writer: &mut WriterType,
        start_time: &mut i64,
    ) -> Result<JobProgress, DownloadError> {
        let mut retry_count = 0;
        let mut last_error = None;

        loop {
            match fetcher.fetch_funding_stream(&job.symbol, *start_time, job.end_time).await {
                Ok(mut stream) => {
                    // Process stream
                    while let Some(result) = stream.next().await {
                        match result {
                            Ok(rate) => {
                                // Write funding rate using safe enum dispatch
                                writer.write_funding(&rate)?;
                                job.progress.downloaded_bars += 1;
                                job.progress.written_bars = writer.items_written();

                                let timestamp = rate.funding_time;
                                job.progress.current_position = Some(timestamp);

                                // Save checkpoint at intervals
                                if job.progress.downloaded_bars % CHECKPOINT_INTERVAL_FUNDING == 0 {
                                    let checkpoint = Checkpoint::time_window(
                                        job.start_time,
                                        timestamp,
                                        job.progress.downloaded_bars,
                                        0,
                                    );
                                    self.save_checkpoint(job, checkpoint).await?;
                                }
                            }
                            Err(e) => {
                                error!("Stream error: {}", e);
                                last_error = Some(e);
                                break;
                            }
                        }
                    }

                    // If we got here without error, download is complete
                    if last_error.is_none() {
                        job.status = JobStatus::Completed;
                        break;
                    }
                }
                Err(e) => {
                    error!("Failed to create stream: {}", e);
                    last_error = Some(e);
                }
            }

            // Retry logic
            retry_count += 1;
            if !self.retry_with_backoff(retry_count, job).await {
                job.status = JobStatus::Failed;
                let error_msg = last_error
                    .map(|e| e.to_string())
                    .unwrap_or_else(|| "Unknown error".to_string());
                job.progress.error = Some(error_msg.clone());
                return Err(DownloadError::NetworkError(format!(
                    "Max retries ({}) exceeded: {}",
                    self.max_retries, error_msg
                )));
            }

            // Continue from last known position
            if let Some(pos) = job.progress.current_position {
                *start_time = pos + 1;
            }
        }

        Ok(job.progress.clone())
    }
}

impl Default for DownloadExecutor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_executor_creation() {
        let executor = DownloadExecutor::new();
        assert!(executor.resume_dir.is_none());
        assert_eq!(executor.max_retries, MAX_RETRIES);
    }

    #[test]
    fn test_executor_with_resume() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let executor = DownloadExecutor::new_with_resume(temp_dir.path());
        assert!(executor.resume_dir.is_some());
    }
}
