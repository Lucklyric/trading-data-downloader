//! Download executor with retry and resume capability (T074-T080)
//! Refactored to eliminate code duplication and use fetcher factory

use crate::downloader::config::{
    calculate_backoff, CHECKPOINT_INTERVAL_BARS, CHECKPOINT_INTERVAL_FUNDING,
    CHECKPOINT_INTERVAL_TRADES, MAX_RETRIES,
};
use crate::downloader::progress::{DownloadItemType, ProgressState, ProgressTracker};
use crate::downloader::{DownloadError, DownloadJob, JobProgress, JobStatus};
use crate::fetcher::{create_fetcher, DataFetcher};
use crate::identifier::ExchangeIdentifier;
use crate::metrics::DownloadMetrics;
use crate::output::csv::{read_time_range, CsvAggTradesWriter, CsvBarsWriter, CsvFundingWriter};
use crate::output::{AggTradesWriter, BarsWriter, FundingWriter, OutputWriter};
use crate::resume::checkpoint::{Checkpoint, CheckpointType};
use crate::resume::state::ResumeState;
use crate::shutdown::{self, SharedShutdown};
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
    force: bool,
    progress_tracker: ProgressTracker,
    shutdown: Option<SharedShutdown>,
}

impl DownloadExecutor {
    /// Create a new download executor without resume capability
    pub fn new() -> Self {
        Self {
            resume_dir: None,
            max_retries: MAX_RETRIES,
            force: false,
            progress_tracker: ProgressTracker::default(),
            shutdown: shutdown::get_global_shutdown(),
        }
    }

    /// Create a new download executor with resume capability
    pub fn new_with_resume<P: Into<PathBuf>>(resume_dir: P) -> Self {
        let resume_dir = resume_dir.into();
        std::fs::create_dir_all(&resume_dir).ok();

        Self {
            resume_dir: Some(resume_dir),
            max_retries: MAX_RETRIES,
            force: false,
            progress_tracker: ProgressTracker::default(),
            shutdown: shutdown::get_global_shutdown(),
        }
    }

    /// Set maximum number of retries
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Enable or disable forcing re-download regardless of existing files
    pub fn with_force(mut self, force: bool) -> Self {
        self.force = force;
        self
    }

    /// Attach a shared shutdown handle for graceful cancellation.
    pub fn with_shutdown(mut self, shutdown: SharedShutdown) -> Self {
        self.shutdown = Some(shutdown);
        self
    }

    /// Override progress tracking configuration.
    pub fn with_progress_tracker(mut self, tracker: ProgressTracker) -> Self {
        self.progress_tracker = tracker;
        self
    }

    /// Generate resume state file path for a job
    fn get_resume_state_path(&self, job: &DownloadJob) -> Option<PathBuf> {
        self.resume_dir.as_ref().map(|dir| {
            let data_type_str = match job.job_type {
                crate::downloader::JobType::Bars { interval } => {
                    format!("bars_{interval}")
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

    /// Load existing resume state if available (T079, T168)
    fn load_resume_state(&self, job: &DownloadJob) -> Result<Option<i64>, DownloadError> {
        let path = match self.get_resume_state_path(job) {
            Some(p) => p,
            None => {
                debug!("Resume capability not enabled");
                return Ok(None);
            }
        };

        if !path.exists() {
            debug!("No resume state found, starting fresh download");
            return Ok(None);
        }

        match ResumeState::load(&path) {
            Ok(state) => {
                info!("Found existing resume state");
                // Find latest checkpoint
                if let Some(checkpoint) = state.checkpoints().last() {
                    // Extract end_time from checkpoint type
                    if let CheckpointType::TimeWindow { end_time, .. } =
                        checkpoint.checkpoint_type()
                    {
                        info!(
                            resume_timestamp = end_time,
                            checkpoint_count = state.checkpoints().len(),
                            "Resuming from checkpoint"
                        );
                        return Ok(Some(*end_time));
                    }
                }
                Ok(None)
            }
            Err(e) => {
                warn!(error = %e, "Failed to load resume state, starting fresh");
                Ok(None)
            }
        }
    }

    /// Save checkpoint during download (T080, T168)
    async fn save_checkpoint(
        &self,
        job: &DownloadJob,
        checkpoint: Checkpoint,
    ) -> Result<(), DownloadError> {
        if let Some(path) = self.get_resume_state_path(job) {
            let mut state = if path.exists() {
                ResumeState::load(&path)
                    .map_err(|e| DownloadError::IoError(format!("Failed to load state: {e}")))?
            } else {
                ResumeState::new(
                    job.identifier.clone(),
                    job.symbol.clone(),
                    match job.job_type {
                        crate::downloader::JobType::Bars { interval } => Some(interval.to_string()),
                        _ => None,
                    },
                    match job.job_type {
                        crate::downloader::JobType::Bars { .. } => "bars".to_string(),
                        crate::downloader::JobType::AggTrades => "aggtrades".to_string(),
                        crate::downloader::JobType::FundingRates => "funding".to_string(),
                    },
                )
            };

            // Log checkpoint info before moving it
            debug!(
                checkpoint_type = ?checkpoint.checkpoint_type(),
                "Saving checkpoint to resume state"
            );

            state.add_checkpoint(checkpoint);

            state
                .save(&path)
                .map_err(|e| DownloadError::IoError(format!("Failed to save checkpoint: {e}")))?;

            debug!(
                total_checkpoints = state.checkpoints().len(),
                "Checkpoint saved"
            );
        }

        Ok(())
    }

    /// Clean up resume state after successful completion (T168)
    fn cleanup_resume_state(&self, job: &DownloadJob) {
        if let Some(path) = self.get_resume_state_path(job) {
            if path.exists() {
                match std::fs::remove_file(&path) {
                    Ok(_) => info!("Removed resume state file after successful completion"),
                    Err(e) => warn!(error = %e, "Failed to remove resume state file"),
                }
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
            .map_err(|e| DownloadError::FetcherError(format!("Failed to create fetcher: {e}")))
    }

    /// Retry with exponential backoff (T168)
    async fn retry_with_backoff(&self, retry_count: u32, job: &mut DownloadJob) -> bool {
        job.progress.retries = retry_count as u64;

        if retry_count > self.max_retries {
            error!(
                retry_count = retry_count,
                max_retries = self.max_retries,
                "Max retries exceeded"
            );
            return false;
        }

        let backoff = calculate_backoff(retry_count);
        warn!(
            retry_count = retry_count,
            max_retries = self.max_retries,
            backoff_ms = backoff.as_millis(),
            "Retrying after backoff delay"
        );
        if self.shutdown_requested() {
            return false;
        }
        if let Some(shutdown) = &self.shutdown {
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {},
                _ = shutdown.wait_for_shutdown() => return false,
            }
        } else {
            tokio::time::sleep(backoff).await;
        }

        true
    }

    fn shutdown_requested(&self) -> bool {
        self.shutdown
            .as_ref()
            .map(|s| s.is_shutdown_requested())
            .unwrap_or(false)
    }

    async fn abort_due_to_shutdown(
        &self,
        job: &mut DownloadJob,
    ) -> Result<JobProgress, DownloadError> {
        job.status = JobStatus::Cancelled;
        job.progress.error = Some("Shutdown requested".to_string());
        info!("Shutdown requested - saving progress before exiting");
        self.save_shutdown_checkpoint(job).await?;
        Err(DownloadError::NetworkError(
            "Shutdown requested".to_string(),
        ))
    }

    async fn save_shutdown_checkpoint(&self, job: &mut DownloadJob) -> Result<(), DownloadError> {
        if let Some(timestamp) = job.progress.current_position {
            let checkpoint =
                Checkpoint::time_window(job.start_time, timestamp, job.progress.downloaded_bars, 0);
            self.save_checkpoint(job, checkpoint).await?;
        }
        Ok(())
    }

    /// Execute bars download job (T076, T168)
    pub async fn execute_bars_job(
        &self,
        mut job: DownloadJob,
    ) -> Result<JobProgress, DownloadError> {
        // Create structured logging span for download operation
        let span = tracing::info_span!(
            "execute_bars_job",
            identifier = %job.identifier,
            symbol = %job.symbol,
            start_time = job.start_time,
            end_time = job.end_time
        );
        let _enter = span.enter();

        info!("Starting bars download job");

        // Start metrics tracking for this download
        let download_metrics = DownloadMetrics::start("bars", &job.symbol);

        // Validate job
        job.validate().map_err(DownloadError::ValidationError)?;

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

        // P0-3: If output exists and not forcing, skip/adjust to avoid redundant downloads
        if !self.force && job.output_path.exists() {
            if let Ok(Some((_min_ts, max_ts))) = read_time_range(&job.output_path) {
                let adjusted = max_ts + 1;
                if adjusted >= job.end_time {
                    info!("Output already fully covers requested bars range; skipping download");
                    job.status = JobStatus::Completed;
                    job.progress.current_position = Some(max_ts);
                    return Ok(job.progress.clone());
                }
                if adjusted > start_time {
                    info!(
                        start_time = adjusted,
                        "Adjusting bars start_time to existing coverage end + 1"
                    );
                    start_time = adjusted;
                    job.progress.current_position = Some(start_time);
                }
            }
        }

        // Calculate expected total bars
        let interval_ms = interval.to_milliseconds();
        let total_duration = job.end_time - job.start_time;
        let total_bars = ((total_duration + interval_ms - 1) / interval_ms) as u64;
        job.progress.total_bars = Some(total_bars);

        info!(total_bars = total_bars, "Expected bars for time range");

        job.status = JobStatus::InProgress;

        // Create fetcher and writer
        let fetcher = self.create_fetcher_from_job(&job)?;
        let writer = CsvBarsWriter::new(&job.output_path)
            .map_err(|e| DownloadError::OutputError(e.to_string()))?;
        let mut writer = WriterType::Bars(writer);

        let total_estimated = job.progress.total_bars.or_else(|| {
            ProgressState::estimate_total_from_range(
                DownloadItemType::Bars,
                job.start_time,
                job.end_time,
                Some(interval_ms),
            )
        });
        let mut progress_state = self.progress_tracker.create_state(
            total_estimated,
            DownloadItemType::Bars,
            Some((job.start_time, job.end_time)),
        );
        progress_state.items_downloaded = job.progress.downloaded_bars;
        progress_state.last_position = job.progress.current_position;

        // Download with retry
        let result = self
            .download_bars_with_retry(
                &mut job,
                &*fetcher,
                &mut writer,
                &mut start_time,
                interval,
                &mut progress_state,
            )
            .await;

        // Finalize
        writer.close()?;

        if job.status == JobStatus::Completed {
            self.cleanup_resume_state(&job);
            // Record successful download in metrics
            download_metrics.record_success(job.progress.downloaded_bars);
        } else {
            // Record failure in metrics
            let error_msg = match &result {
                Err(e) => e.to_string(),
                Ok(_) => "Unknown error".to_string(),
            };
            download_metrics.record_failure(&error_msg);
        }

        info!(
            status = ?job.status,
            bars_downloaded = job.progress.downloaded_bars,
            "Bars download job completed"
        );

        result
    }

    /// Execute aggTrades download job (T127, T168)
    pub async fn execute_aggtrades_job(
        &self,
        mut job: DownloadJob,
    ) -> Result<JobProgress, DownloadError> {
        // Create structured logging span for download operation
        let span = tracing::info_span!(
            "execute_aggtrades_job",
            identifier = %job.identifier,
            symbol = %job.symbol,
            start_time = job.start_time,
            end_time = job.end_time
        );
        let _enter = span.enter();

        info!("Starting aggTrades download job");

        // Start metrics tracking for this download
        let download_metrics = DownloadMetrics::start("aggtrades", &job.symbol);

        // Validate job
        job.validate().map_err(DownloadError::ValidationError)?;

        // Load resume state
        let resume_time = self.load_resume_state(&job)?;
        let mut start_time = resume_time.unwrap_or(job.start_time);
        if resume_time.is_some() {
            job.progress.current_position = Some(start_time);
        }

        // P0-3: If output exists and not forcing, skip/adjust to avoid redundant downloads
        if !self.force && job.output_path.exists() {
            if let Ok(Some((_min_ts, max_ts))) = read_time_range(&job.output_path) {
                let adjusted = max_ts + 1;
                if adjusted >= job.end_time {
                    info!("Output fully covers requested aggTrades; skipping");
                    job.status = JobStatus::Completed;
                    job.progress.current_position = Some(max_ts);
                    return Ok(job.progress.clone());
                }
                if adjusted > start_time {
                    info!(
                        start_time = adjusted,
                        "Adjusting aggTrades start_time to coverage end + 1"
                    );
                    start_time = adjusted;
                    job.progress.current_position = Some(start_time);
                }
            }
        }

        debug!("Fetching aggTrades for time range");

        job.status = JobStatus::InProgress;

        // Create fetcher and writer
        let fetcher = self.create_fetcher_from_job(&job)?;
        let writer = CsvAggTradesWriter::new(&job.output_path)
            .map_err(|e| DownloadError::OutputError(e.to_string()))?;
        let mut writer = WriterType::AggTrades(writer);
        let mut progress_state = self.progress_tracker.create_state(
            ProgressState::estimate_total_from_range(
                DownloadItemType::AggTrades,
                job.start_time,
                job.end_time,
                None,
            ),
            DownloadItemType::AggTrades,
            Some((job.start_time, job.end_time)),
        );
        progress_state.items_downloaded = job.progress.downloaded_bars;
        progress_state.last_position = job.progress.current_position;

        // Download with retry
        let result = self
            .download_aggtrades_with_retry(
                &mut job,
                &*fetcher,
                &mut writer,
                &mut start_time,
                &mut progress_state,
            )
            .await;

        // Finalize
        writer.close()?;

        if job.status == JobStatus::Completed {
            self.cleanup_resume_state(&job);
            // Record successful download in metrics
            download_metrics.record_success(job.progress.downloaded_bars);
        } else {
            // Record failure in metrics
            let error_msg = match &result {
                Err(e) => e.to_string(),
                Ok(_) => "Unknown error".to_string(),
            };
            download_metrics.record_failure(&error_msg);
        }

        info!(
            status = ?job.status,
            trades_downloaded = job.progress.downloaded_bars,
            "AggTrades download job completed"
        );

        result
    }

    /// Execute funding rates download job (T149, T168)
    pub async fn execute_funding_job(
        &self,
        mut job: DownloadJob,
    ) -> Result<JobProgress, DownloadError> {
        // Create structured logging span for download operation
        let span = tracing::info_span!(
            "execute_funding_job",
            identifier = %job.identifier,
            symbol = %job.symbol,
            start_time = job.start_time,
            end_time = job.end_time
        );
        let _enter = span.enter();

        info!("Starting funding rates download job");

        // Start metrics tracking for this download
        let download_metrics = DownloadMetrics::start("funding", &job.symbol);

        // Validate job
        job.validate().map_err(DownloadError::ValidationError)?;

        // Load resume state
        let resume_time = self.load_resume_state(&job)?;
        let mut start_time = resume_time.unwrap_or(job.start_time);
        if resume_time.is_some() {
            job.progress.current_position = Some(start_time);
        }

        // P0-3: If output exists and not forcing, skip/adjust to avoid redundant downloads
        if !self.force && job.output_path.exists() {
            if let Ok(Some((_min_ts, max_ts))) = read_time_range(&job.output_path) {
                let adjusted = max_ts + 1;
                if adjusted >= job.end_time {
                    info!("Output fully covers requested funding; skipping");
                    job.status = JobStatus::Completed;
                    job.progress.current_position = Some(max_ts);
                    return Ok(job.progress.clone());
                }
                if adjusted > start_time {
                    info!(
                        start_time = adjusted,
                        "Adjusting funding start_time to coverage end + 1"
                    );
                    start_time = adjusted;
                    job.progress.current_position = Some(start_time);
                }
            }
        }

        debug!("Fetching funding rates for time range");

        job.status = JobStatus::InProgress;

        // Create fetcher and writer
        let fetcher = self.create_fetcher_from_job(&job)?;
        let writer = CsvFundingWriter::new(&job.output_path)
            .map_err(|e| DownloadError::OutputError(e.to_string()))?;
        let mut writer = WriterType::Funding(writer);
        let mut progress_state = self.progress_tracker.create_state(
            ProgressState::estimate_total_from_range(
                DownloadItemType::FundingRates,
                job.start_time,
                job.end_time,
                None,
            ),
            DownloadItemType::FundingRates,
            Some((job.start_time, job.end_time)),
        );
        progress_state.items_downloaded = job.progress.downloaded_bars;
        progress_state.last_position = job.progress.current_position;

        // Download with retry
        let result = self
            .download_funding_with_retry(
                &mut job,
                &*fetcher,
                &mut writer,
                &mut start_time,
                &mut progress_state,
            )
            .await;

        // Finalize
        writer.close()?;

        if job.status == JobStatus::Completed {
            self.cleanup_resume_state(&job);
            // Record successful download in metrics
            download_metrics.record_success(job.progress.downloaded_bars);
        } else {
            // Record failure in metrics
            let error_msg = match &result {
                Err(e) => e.to_string(),
                Ok(_) => "Unknown error".to_string(),
            };
            download_metrics.record_failure(&error_msg);
        }

        info!(
            status = ?job.status,
            funding_rates_downloaded = job.progress.downloaded_bars,
            "Funding rates download job completed"
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
        progress_state: &mut ProgressState,
    ) -> Result<JobProgress, DownloadError> {
        let mut retry_count = 0;
        let mut last_error = None;

        loop {
            if self.shutdown_requested() {
                return self.abort_due_to_shutdown(job).await;
            }
            match fetcher
                .fetch_bars_stream(&job.symbol, interval, *start_time, job.end_time)
                .await
            {
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

                                progress_state.update(1, Some(timestamp));
                                if progress_state.should_emit_update() {
                                    info!(
                                        symbol = %job.symbol,
                                        "{}",
                                        progress_state.format_progress()
                                    );
                                    progress_state.mark_emitted();
                                }

                                if self.shutdown_requested() {
                                    return self.abort_due_to_shutdown(job).await;
                                }

                                // Save checkpoint at intervals
                                if job.progress.downloaded_bars % CHECKPOINT_INTERVAL_BARS == 0 {
                                    debug!(
                                        bars_downloaded = job.progress.downloaded_bars,
                                        current_timestamp = timestamp,
                                        "Download progress"
                                    );
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
                if self.shutdown_requested() {
                    return self.abort_due_to_shutdown(job).await;
                }
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
        progress_state: &mut ProgressState,
    ) -> Result<JobProgress, DownloadError> {
        let mut retry_count = 0;
        let mut last_error = None;

        loop {
            if self.shutdown_requested() {
                return self.abort_due_to_shutdown(job).await;
            }
            match fetcher
                .fetch_aggtrades_stream(&job.symbol, *start_time, job.end_time)
                .await
            {
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

                                progress_state.update(1, Some(timestamp));
                                if progress_state.should_emit_update() {
                                    info!(
                                        symbol = %job.symbol,
                                        "{}",
                                        progress_state.format_progress()
                                    );
                                    progress_state.mark_emitted();
                                }

                                if self.shutdown_requested() {
                                    return self.abort_due_to_shutdown(job).await;
                                }

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
                if self.shutdown_requested() {
                    return self.abort_due_to_shutdown(job).await;
                }
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
        progress_state: &mut ProgressState,
    ) -> Result<JobProgress, DownloadError> {
        let mut retry_count = 0;
        let mut last_error = None;

        loop {
            if self.shutdown_requested() {
                return self.abort_due_to_shutdown(job).await;
            }
            match fetcher
                .fetch_funding_stream(&job.symbol, *start_time, job.end_time)
                .await
            {
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

                                progress_state.update(1, Some(timestamp));
                                if progress_state.should_emit_update() {
                                    info!(
                                        symbol = %job.symbol,
                                        "{}",
                                        progress_state.format_progress()
                                    );
                                    progress_state.mark_emitted();
                                }

                                if self.shutdown_requested() {
                                    return self.abort_due_to_shutdown(job).await;
                                }

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
                if self.shutdown_requested() {
                    return self.abort_due_to_shutdown(job).await;
                }
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
