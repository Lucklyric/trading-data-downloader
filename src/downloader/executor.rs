//! Download executor with retry and resume capability (T074-T080)
//! Refactored to eliminate code duplication and use fetcher factory

use crate::downloader::config::{
    calculate_backoff, CHECKPOINT_INTERVAL_BARS, CHECKPOINT_INTERVAL_FUNDING,
    CHECKPOINT_INTERVAL_TRADES, MAX_RETRIES,
};
use crate::downloader::progress::{DownloadItemType, ProgressState, ProgressTracker};
use crate::downloader::{DownloadError, DownloadJob, JobProgress, JobStatus, JobType};
use crate::fetcher::{create_fetcher, DataFetcher, FetcherError};
use crate::identifier::ExchangeIdentifier;
use crate::output::csv::{read_time_range, CsvAggTradesWriter, CsvBarsWriter, CsvFundingWriter};
use crate::output::{AggTradesWriter, BarsWriter, FundingWriter, OutputWriter};
use crate::resume::checkpoint::{Checkpoint, CheckpointType};
use crate::resume::state::ResumeState;
use crate::shutdown::{self, SharedShutdown};
use crate::{AggTrade, Bar, FundingRate};
use futures_util::StreamExt;
use indicatif::ProgressBar;
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
    /// Returns `Ok(true)` if bar was written, `Ok(false)` if duplicate was skipped
    fn write_bar(&mut self, bar: &Bar) -> Result<bool, DownloadError> {
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
            WriterType::Bars(writer) => writer.bars_written(),
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
        if let Err(e) = std::fs::create_dir_all(&resume_dir) {
            warn!(error = %e, path = %resume_dir.display(), "Failed to create resume directory");
        }

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
            // Sanitize identifier to prevent path traversal (M4)
            // Replace all directory separators and parent references
            let safe_identifier = job
                .identifier
                .replace("..", "__")
                .replace([':', '/', '\\'], "_");
            let safe_symbol = job
                .symbol
                .replace("..", "__")
                .replace([':', '/', '\\'], "_");
            let filename = format!(
                "{}_{}_{}_{}_{}.json",
                safe_identifier,
                safe_symbol,
                data_type_str,
                job.start_time,
                job.end_time
            );
            dir.join(filename)
        })
    }

    /// Load existing resume state if available (T079, T168)
    ///
    /// Wraps blocking file I/O in `spawn_blocking` to avoid blocking the
    /// async runtime (P1-7).
    async fn load_resume_state(&self, job: &DownloadJob) -> Result<Option<i64>, DownloadError> {
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

        let path_clone = path.clone();
        let load_result = tokio::task::spawn_blocking(move || ResumeState::load(&path_clone))
            .await
            .map_err(|e| DownloadError::IoError(format!("spawn_blocking join error: {e}")))?;

        match load_result {
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
                // Handle schema version mismatch as a clear error (T044/F033)
                if let crate::resume::state::ResumeError::SchemaVersionMismatch { expected, found } = &e {
                    return Err(DownloadError::IncompatibleResumeState {
                        expected: expected.clone(),
                        found: found.clone(),
                        path: path.display().to_string(),
                    });
                }
                // Other errors (IO, deserialization) - fail hard to prevent silent data loss (H2)
                // Users should explicitly use --resume reset to clear corrupt state
                error!(
                    error = %e,
                    path = %path.display(),
                    "CRITICAL: Resume state is corrupt or unreadable. \
                     Use --resume reset to clear and start fresh, or delete the file manually."
                );
                Err(DownloadError::IoError(format!(
                    "Corrupt resume state at {}: {}. Use --resume reset to clear.",
                    path.display(),
                    e
                )))
            }
        }
    }

    /// Load existing resume state for aggtrades, returning the last agg_trade_id if available
    ///
    /// Returns `(Option<trade_id>, Option<timestamp>)`:
    /// - First element: trade ID from "aggtrade_id:{id}" cursor (new format)
    /// - Second element: timestamp from "timestamp:{ts}" cursor (legacy migrated format)
    ///
    /// Wraps blocking file I/O in `spawn_blocking` to avoid blocking the async runtime.
    async fn load_aggtrades_resume_state(
        &self,
        job: &DownloadJob,
    ) -> Result<(Option<i64>, Option<i64>), DownloadError> {
        let path = match self.get_resume_state_path(job) {
            Some(p) => p,
            None => {
                debug!("Resume capability not enabled");
                return Ok((None, None));
            }
        };

        if !path.exists() {
            debug!("No resume state found, starting fresh download");
            return Ok((None, None));
        }

        let path_clone = path.clone();
        let load_result = tokio::task::spawn_blocking(move || ResumeState::load(&path_clone))
            .await
            .map_err(|e| DownloadError::IoError(format!("spawn_blocking join error: {e}")))?;

        match load_result {
            Ok(state) => {
                info!("Found existing aggtrades resume state");
                if let Some(checkpoint) = state.checkpoints().last() {
                    match checkpoint.checkpoint_type() {
                        CheckpointType::Cursor { cursor } => {
                            if let Some(id_str) = cursor.strip_prefix("aggtrade_id:") {
                                match id_str.parse::<i64>() {
                                    Ok(id) => {
                                        info!(
                                            resume_aggtrade_id = id,
                                            "Resuming aggtrades from trade ID"
                                        );
                                        return Ok((Some(id), None));
                                    }
                                    Err(e) => {
                                        warn!(
                                            cursor = %cursor,
                                            error = %e,
                                            "Failed to parse aggtrade_id from cursor"
                                        );
                                    }
                                }
                            } else if let Some(ts_str) = cursor.strip_prefix("timestamp:") {
                                // Legacy migrated cursor from 1.0.0
                                match ts_str.parse::<i64>() {
                                    Ok(ts) => {
                                        warn!(
                                            timestamp = ts,
                                            "Legacy timestamp-based cursor from migration; \
                                             falling back to timestamp-based resume"
                                        );
                                        return Ok((None, Some(ts)));
                                    }
                                    Err(e) => {
                                        warn!(
                                            cursor = %cursor,
                                            error = %e,
                                            "Failed to parse timestamp from legacy cursor"
                                        );
                                    }
                                }
                            } else {
                                warn!(
                                    cursor = %cursor,
                                    "Unrecognized cursor format in aggtrades resume state"
                                );
                            }
                        }
                        CheckpointType::TimeWindow { end_time, .. } => {
                            // Shouldn't happen after migration but handle gracefully
                            warn!(
                                end_time = end_time,
                                "Found TimeWindow checkpoint for aggtrades (pre-migration); \
                                 falling back to timestamp-based resume"
                            );
                            return Ok((None, Some(*end_time)));
                        }
                        _ => {
                            warn!("Unexpected checkpoint type for aggtrades resume state");
                        }
                    }
                }
                Ok((None, None))
            }
            Err(e) => {
                if let crate::resume::state::ResumeError::SchemaVersionMismatch {
                    expected,
                    found,
                } = &e
                {
                    return Err(DownloadError::IncompatibleResumeState {
                        expected: expected.clone(),
                        found: found.clone(),
                        path: path.display().to_string(),
                    });
                }
                error!(
                    error = %e,
                    path = %path.display(),
                    "CRITICAL: Resume state is corrupt or unreadable. \
                     Use --resume reset to clear and start fresh, or delete the file manually."
                );
                Err(DownloadError::IoError(format!(
                    "Corrupt resume state at {}: {}. Use --resume reset to clear.",
                    path.display(),
                    e
                )))
            }
        }
    }

    /// Save checkpoint during download (T080, T168)
    ///
    /// Wraps blocking file I/O in `spawn_blocking` to avoid blocking the
    /// async runtime (P1-7).
    async fn save_checkpoint(
        &self,
        job: &DownloadJob,
        checkpoint: Checkpoint,
    ) -> Result<(), DownloadError> {
        if let Some(path) = self.get_resume_state_path(job) {
            let identifier = job.identifier.clone();
            let symbol = job.symbol.clone();
            let job_type = job.job_type;
            let path_clone = path.clone();

            tokio::task::spawn_blocking(move || {
                let mut state = if path_clone.exists() {
                    ResumeState::load(&path_clone)
                        .map_err(|e| DownloadError::IoError(format!("Failed to load state: {e}")))?
                } else {
                    ResumeState::new(
                        identifier,
                        symbol,
                        match job_type {
                            crate::downloader::JobType::Bars { interval } => {
                                Some(interval.to_string())
                            }
                            _ => None,
                        },
                        match job_type {
                            crate::downloader::JobType::Bars { .. } => "bars".to_string(),
                            crate::downloader::JobType::AggTrades => "aggtrades".to_string(),
                            crate::downloader::JobType::FundingRates => "funding".to_string(),
                        },
                    )
                };

                debug!(
                    checkpoint_type = ?checkpoint.checkpoint_type(),
                    "Saving checkpoint to resume state"
                );

                state.add_checkpoint(checkpoint);

                state.save(&path_clone).map_err(|e| {
                    DownloadError::IoError(format!("Failed to save checkpoint: {e}"))
                })?;

                debug!(
                    total_checkpoints = state.checkpoints().len(),
                    "Checkpoint saved"
                );

                Ok::<(), DownloadError>(())
            })
            .await
            .map_err(|e| DownloadError::IoError(format!("spawn_blocking join error: {e}")))??;
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

        create_fetcher(&identifier, self.max_retries)
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

        // Fix off-by-one: calculate_backoff expects 0-indexed, but retry_count is 1-indexed (L2)
        let backoff = calculate_backoff(retry_count.saturating_sub(1));
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
        last_aggtrade_id: Option<i64>,
    ) -> Result<JobProgress, DownloadError> {
        job.status = JobStatus::Cancelled;
        job.progress.error = Some("Shutdown requested".to_string());
        info!("Shutdown requested - saving progress before exiting");
        self.save_shutdown_checkpoint(job, last_aggtrade_id).await?;
        Err(DownloadError::NetworkError(
            "Shutdown requested".to_string(),
        ))
    }

    async fn save_shutdown_checkpoint(
        &self,
        job: &mut DownloadJob,
        last_aggtrade_id: Option<i64>,
    ) -> Result<(), DownloadError> {
        if let Some(timestamp) = job.progress.current_position {
            let checkpoint = match (&job.job_type, last_aggtrade_id) {
                // AggTrades with a known trade ID: save cursor-based checkpoint
                (JobType::AggTrades, Some(trade_id)) => Checkpoint::cursor(
                    format!("aggtrade_id:{trade_id}"),
                    job.progress.downloaded_bars,
                    0,
                ),
                // All other cases: time-window checkpoint
                _ => Checkpoint::time_window(
                    job.start_time,
                    timestamp,
                    job.progress.downloaded_bars,
                    0,
                ),
            };
            self.save_checkpoint(job, checkpoint).await?;
        }
        Ok(())
    }

    /// Execute bars download job (T076, T168)
    ///
    /// # Arguments
    /// * `job` - The download job specification
    /// * `progress_bar` - Optional indicatif ProgressBar for visual updates
    pub async fn execute_bars_job(
        &self,
        mut job: DownloadJob,
        progress_bar: Option<ProgressBar>,
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
        let resume_time = self.load_resume_state(&job).await?;
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
                progress_bar.as_ref(),
            )
            .await;

        // Finalize
        writer.close()?;

        if job.status == JobStatus::Completed {
            self.cleanup_resume_state(&job);
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

        // Validate job
        job.validate().map_err(DownloadError::ValidationError)?;

        // Load resume state (aggtrades uses cursor-based resume with trade IDs)
        let (resume_trade_id, resume_timestamp) = self.load_aggtrades_resume_state(&job).await?;
        let mut from_id: Option<i64> = resume_trade_id.map(|id| id + 1);
        let mut start_time = resume_timestamp.unwrap_or(job.start_time);
        if resume_trade_id.is_some() || resume_timestamp.is_some() {
            job.progress.current_position = resume_timestamp.or(Some(job.start_time));
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
                if adjusted > start_time && from_id.is_none() {
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
                &mut from_id,
                &mut progress_state,
            )
            .await;

        // Finalize
        writer.close()?;

        if job.status == JobStatus::Completed {
            self.cleanup_resume_state(&job);
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

        // Validate job
        job.validate().map_err(DownloadError::ValidationError)?;

        // Load resume state
        let resume_time = self.load_resume_state(&job).await?;
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
        }

        info!(
            status = ?job.status,
            funding_rates_downloaded = job.progress.downloaded_bars,
            "Funding rates download job completed"
        );

        result
    }

    /// Download bars with retry logic - type-safe version for bars
    ///
    /// # Sequential Stream Processing (T050/F030)
    ///
    /// Items are processed sequentially from the stream rather than in parallel for these reasons:
    /// 1. **Ordering**: Bars must be written to CSV in chronological order for data integrity
    /// 2. **Checkpointing**: Resume state tracks the last processed timestamp, which requires
    ///    sequential advancement to avoid gaps on restart
    /// 3. **Rate limiting**: The rate limiter and backoff logic depend on processing one
    ///    request at a time to respect exchange API limits
    /// 4. **Memory efficiency**: Sequential processing with streaming avoids loading all
    ///    data into memory at once
    #[allow(clippy::too_many_arguments)]
    async fn download_bars_with_retry(
        &self,
        job: &mut DownloadJob,
        fetcher: &dyn DataFetcher,
        writer: &mut WriterType,
        start_time: &mut i64,
        interval: crate::Interval,
        progress_state: &mut ProgressState,
        progress_bar: Option<&ProgressBar>,
    ) -> Result<JobProgress, DownloadError> {
        let mut retry_count = 0;
        let mut last_error: Option<FetcherError>;

        loop {
            // Reset last_error at start of each attempt so successful retries complete properly
            last_error = None;

            if self.shutdown_requested() {
                return self.abort_due_to_shutdown(job, None).await;
            }
            match fetcher
                .fetch_bars_stream(&job.symbol, interval, *start_time, job.end_time)
                .await
            {
                Ok(mut stream) => {
                    // Process stream with shutdown awareness (P2-1)
                    loop {
                        let next_item = if let Some(shutdown) = &self.shutdown {
                            tokio::select! {
                                item = stream.next() => item,
                                _ = shutdown.wait_for_shutdown() => {
                                    return self.abort_due_to_shutdown(job, None).await;
                                }
                            }
                        } else {
                            stream.next().await
                        };

                        let result = match next_item {
                            Some(r) => r,
                            None => break,
                        };

                        match result {
                            Ok(bar) => {
                                // Write bar using safe enum dispatch
                                let was_written = writer.write_bar(&bar)?;
                                job.progress.downloaded_bars += 1;
                                if was_written {
                                    job.progress.written_bars += 1;
                                }

                                let timestamp = bar.close_time;
                                job.progress.current_position = Some(timestamp);

                                progress_state.update(1, Some(timestamp));

                                // Update visual progress bar if provided
                                if let Some(pb) = progress_bar {
                                    pb.inc(1);
                                }

                                if progress_state.should_emit_update() {
                                    info!(
                                        symbol = %job.symbol,
                                        "{}",
                                        progress_state.format_progress()
                                    );
                                    progress_state.mark_emitted();
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
                    return self.abort_due_to_shutdown(job, None).await;
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
    ///
    /// Uses agg_trade_id-based checkpointing for precise resume capability.
    /// The `from_id` parameter allows resuming from a specific trade ID.
    async fn download_aggtrades_with_retry(
        &self,
        job: &mut DownloadJob,
        fetcher: &dyn DataFetcher,
        writer: &mut WriterType,
        start_time: &mut i64,
        from_id: &mut Option<i64>,
        progress_state: &mut ProgressState,
    ) -> Result<JobProgress, DownloadError> {
        let mut retry_count = 0;
        let mut last_error: Option<FetcherError>;
        // Track last processed trade ID for cursor-based shutdown checkpoints
        let mut last_aggtrade_id: Option<i64> = None;

        loop {
            // Reset last_error at start of each attempt so successful retries complete properly
            last_error = None;

            if self.shutdown_requested() {
                return self.abort_due_to_shutdown(job, last_aggtrade_id).await;
            }
            match fetcher
                .fetch_aggtrades_stream(&job.symbol, *start_time, job.end_time, *from_id)
                .await
            {
                Ok(mut stream) => {
                    // Process stream with shutdown awareness (P2-1)
                    loop {
                        let next_item = if let Some(shutdown) = &self.shutdown {
                            tokio::select! {
                                item = stream.next() => item,
                                _ = shutdown.wait_for_shutdown() => {
                                    return self.abort_due_to_shutdown(job, last_aggtrade_id).await;
                                }
                            }
                        } else {
                            stream.next().await
                        };

                        let result = match next_item {
                            Some(r) => r,
                            None => break,
                        };

                        match result {
                            Ok(trade) => {
                                // Write trade using safe enum dispatch
                                writer.write_aggtrade(&trade)?;
                                job.progress.downloaded_bars += 1;
                                job.progress.written_bars = writer.items_written();

                                let timestamp = trade.timestamp;
                                job.progress.current_position = Some(timestamp);
                                last_aggtrade_id = Some(trade.agg_trade_id);

                                progress_state.update(1, Some(timestamp));
                                if progress_state.should_emit_update() {
                                    info!(
                                        symbol = %job.symbol,
                                        "{}",
                                        progress_state.format_progress()
                                    );
                                    progress_state.mark_emitted();
                                }

                                // Save cursor-based checkpoint at intervals using agg_trade_id
                                if job.progress.downloaded_bars % CHECKPOINT_INTERVAL_TRADES == 0 {
                                    let checkpoint = Checkpoint::cursor(
                                        format!("aggtrade_id:{}", trade.agg_trade_id),
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
                    return self.abort_due_to_shutdown(job, last_aggtrade_id).await;
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

            // Continue from last known trade ID if available, else fall back to timestamp
            if let Some(trade_id) = last_aggtrade_id {
                *from_id = Some(trade_id + 1);
            } else if let Some(pos) = job.progress.current_position {
                *start_time = pos + 1;
                *from_id = None;
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
        let mut last_error: Option<FetcherError>;

        loop {
            // Reset last_error at start of each attempt so successful retries complete properly
            last_error = None;

            if self.shutdown_requested() {
                return self.abort_due_to_shutdown(job, None).await;
            }
            match fetcher
                .fetch_funding_stream(&job.symbol, *start_time, job.end_time)
                .await
            {
                Ok(mut stream) => {
                    // Process stream with shutdown awareness (P2-1)
                    loop {
                        let next_item = if let Some(shutdown) = &self.shutdown {
                            tokio::select! {
                                item = stream.next() => item,
                                _ = shutdown.wait_for_shutdown() => {
                                    return self.abort_due_to_shutdown(job, None).await;
                                }
                            }
                        } else {
                            stream.next().await
                        };

                        let result = match next_item {
                            Some(r) => r,
                            None => break,
                        };

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
                    return self.abort_due_to_shutdown(job, None).await;
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
