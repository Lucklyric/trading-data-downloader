//! Download command implementation (T082-T087)

use crate::downloader::{DownloadExecutor, DownloadJob, JobProgress};
use crate::shutdown::SharedShutdown;
use crate::Interval;
use chrono::{DateTime, NaiveDate};
use clap::{Parser, Subcommand};
use futures::stream::{self, StreamExt};
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;
use std::path::PathBuf;
use std::str::FromStr;
use tracing::{error, info};

use super::CliError;

/// Maximum allowed concurrency to prevent self-inflicted rate limiting (L3)
const MAX_CONCURRENCY: usize = 32;

/// Try to parse datetime from RFC3339 format (M6)
///
/// Handles both inputs with and without timezone designators:
/// - "2024-01-01T00:00:00Z" - explicit UTC
/// - "2024-01-01T00:00:00+01:00" - explicit offset
/// - "2024-01-01T00:00:00" - no timezone, assumed UTC
///
/// Returns timestamp in milliseconds, or None if parsing fails.
fn try_parse_datetime_rfc3339(input: &str) -> Option<i64> {
    // Trim whitespace to handle user input with leading/trailing spaces
    let input = input.trim();

    // First try parsing as-is (may have timezone)
    if let Ok(dt) = DateTime::parse_from_rfc3339(input) {
        return Some(dt.timestamp_millis());
    }

    // If that fails, try appending 'Z' for inputs without timezone
    // This handles "2024-01-01T00:00:00" format
    if let Ok(dt) = DateTime::parse_from_rfc3339(&format!("{input}Z")) {
        return Some(dt.timestamp_millis());
    }

    None
}

/// Parse a start time from YYYY-MM-DD or RFC3339 datetime format.
///
/// For date-only format, uses start-of-day (00:00:00 UTC).
/// For RFC3339 format, uses the exact time specified.
fn parse_start_time_flexible(input: &str) -> Result<i64, CliError> {
    if let Some(ts) = try_parse_datetime_rfc3339(input) {
        return Ok(ts);
    }

    let date = NaiveDate::parse_from_str(input, "%Y-%m-%d")
        .map_err(|e| CliError::InvalidArgument(format!("Invalid start time: {e}")))?;
    let datetime = date
        .and_hms_opt(0, 0, 0)
        .ok_or_else(|| CliError::InvalidArgument("Invalid start time".to_string()))?;
    Ok(datetime.and_utc().timestamp_millis())
}

/// Parse an end time from YYYY-MM-DD or RFC3339 datetime format.
///
/// For date-only format, uses end-of-day (23:59:59.999 UTC) so the specified date
/// is fully included. For RFC3339 format, uses the exact time specified.
fn parse_end_time_flexible(input: &str) -> Result<i64, CliError> {
    if let Some(ts) = try_parse_datetime_rfc3339(input) {
        return Ok(ts);
    }

    let date = NaiveDate::parse_from_str(input, "%Y-%m-%d")
        .map_err(|e| CliError::InvalidArgument(format!("Invalid end time: {e}")))?;
    let datetime = date
        .and_hms_milli_opt(23, 59, 59, 999)
        .ok_or_else(|| CliError::InvalidArgument("Invalid end time".to_string()))?;
    Ok(datetime.and_utc().timestamp_millis())
}

/// Parse and validate concurrency value (L3)
fn parse_concurrency(s: &str) -> Result<usize, String> {
    let value: usize = s
        .parse()
        .map_err(|_| format!("'{s}' is not a valid number"))?;

    if value == 0 {
        return Err("concurrency must be at least 1".to_string());
    }
    if value > MAX_CONCURRENCY {
        return Err(format!(
            "concurrency {value} exceeds maximum of {MAX_CONCURRENCY}"
        ));
    }
    Ok(value)
}

/// Resume modes (T175)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeMode {
    /// Resume mode disabled
    Off,
    /// Resume from checkpoints if available
    On,
    /// Reset resume state and start fresh
    Reset,
    /// Verify resume state integrity before starting
    Verify,
}

impl FromStr for ResumeMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "off" => Ok(ResumeMode::Off),
            "on" => Ok(ResumeMode::On),
            "reset" => Ok(ResumeMode::Reset),
            "verify" => Ok(ResumeMode::Verify),
            _ => Err(format!(
                "Invalid resume mode: {s}. Valid options: on, off, reset, verify"
            )),
        }
    }
}

/// Handle Reset mode: delete existing resume directory
fn handle_resume_reset(resume_dir: &PathBuf) -> Result<(), CliError> {
    if resume_dir.exists() {
        info!("Reset mode: deleting existing resume directory: {:?}", resume_dir);
        std::fs::remove_dir_all(resume_dir).map_err(|e| {
            CliError::InvalidArgument(format!(
                "Failed to delete resume directory {resume_dir:?}: {e}"
            ))
        })?;
        info!("Resume directory deleted successfully");
    }
    Ok(())
}

/// Handle Verify mode: check resume state integrity
fn handle_resume_verify(resume_dir: &PathBuf) -> Result<(), CliError> {
    use crate::resume::ResumeState;

    if !resume_dir.exists() {
        info!("Verify mode: resume directory does not exist, nothing to verify");
        return Ok(());
    }

    // Check for any .json files in resume directory and validate them
    let entries = std::fs::read_dir(resume_dir).map_err(|e| {
        CliError::InvalidArgument(format!(
            "Failed to read resume directory {resume_dir:?}: {e}"
        ))
    })?;

    let mut valid_count = 0;
    let mut error_count = 0;

    for entry_result in entries {
        let entry = entry_result.map_err(|e| {
            CliError::InvalidArgument(format!("Failed to read directory entry: {e}"))
        })?;
        let path = entry.path();
        if path.extension().is_some_and(|ext| ext == "json") {
            match ResumeState::load(&path) {
                Ok(_state) => {
                    info!("Valid resume state: {:?}", path);
                    valid_count += 1;
                }
                Err(e) => {
                    error!("Invalid resume state: {:?}: {}", path, e);
                    error_count += 1;
                }
            }
        }
    }

    if error_count > 0 {
        return Err(CliError::InvalidArgument(format!(
            "Verify failed: {error_count} invalid resume state file(s) found. Use --resume reset to clear."
        )));
    }

    info!("Verify passed: {} valid resume state file(s)", valid_count);
    Ok(())
}

/// Configuration needed for concurrent job execution (F040)
///
/// This struct holds a copy of the cloneable CLI fields needed by the
/// helper functions for concurrent job execution. This avoids requiring
/// Clone on the entire Cli struct (which contains Commands enum).
#[derive(Clone)]
struct JobConfig {
    resume: ResumeMode,
    resume_dir: Option<PathBuf>,
    max_retries: u32,
    force: bool,
    output_format: OutputFormat,
}

impl JobConfig {
    fn from_cli(cli: &Cli) -> Self {
        Self {
            resume: cli.resume,
            resume_dir: cli.resume_dir.clone(),
            max_retries: cli.max_retries,
            force: cli.force,
            output_format: cli.output_format,
        }
    }

    /// Create a configured executor from this job config.
    ///
    /// Resume mode Reset is handled identically to On here because the
    /// actual directory reset happens once in the caller's `execute()` method
    /// before any concurrent jobs are spawned.
    fn create_executor(&self, shutdown: SharedShutdown) -> DownloadExecutor {
        match self.resume {
            ResumeMode::Off => DownloadExecutor::new(),
            ResumeMode::On | ResumeMode::Verify | ResumeMode::Reset => {
                let resume_dir = self
                    .resume_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from(".resume"));
                DownloadExecutor::new_with_resume(resume_dir)
            }
        }
        .with_max_retries(self.max_retries)
        .with_force(self.force)
        .with_shutdown(shutdown)
    }
}

/// Trading Data Downloader CLI (T082)
#[derive(Parser, Debug)]
#[command(name = "trading-data-downloader")]
#[command(about = "Download crypto trading data from supported exchanges", long_about = None)]
#[command(version)]
pub struct Cli {
    /// Command to execute
    #[command(subcommand)]
    pub command: Commands,

    /// Output format (json or human)
    #[arg(long, global = true, default_value = "human")]
    pub output_format: OutputFormat,

    /// Resume mode: on, off, reset, or verify
    #[arg(long, global = true, default_value = "off")]
    pub resume: ResumeMode,

    /// Resume state directory
    #[arg(long, global = true)]
    pub resume_dir: Option<PathBuf>,

    /// Number of concurrent downloads (default: 4, max: 32)
    ///
    /// Higher values increase throughput for large date ranges by downloading
    /// multiple months in parallel. The rate limiter coordinates all concurrent
    /// requests to stay within Binance API limits.
    ///
    /// Recommended: 4-8 for most use cases, up to 16 for very large backfills.
    #[arg(long, global = true, default_value = "4", value_parser = parse_concurrency)]
    pub concurrency: usize,

    /// Maximum number of retries for failed requests (default: 5, range: 1-20)
    #[arg(long, global = true, default_value = "5", value_parser = clap::value_parser!(u32).range(1..=20))]
    pub max_retries: u32,

    /// Force re-download even if output file already covers the requested range
    #[arg(long, global = true, default_value_t = false)]
    pub force: bool,

    /// Data root directory for hierarchical file organization (default: "data")
    #[arg(long, global = true)]
    pub data_dir: Option<PathBuf>,
}

/// CLI commands
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Download trading data
    Download(DownloadArgs),

    /// List available data sources and symbols
    Sources(super::SourcesCommand),

    /// Validate identifiers or resume state
    Validate(super::ValidateCommand),
}

/// Download command arguments (T083-T084)
#[derive(Parser, Debug)]
pub struct DownloadArgs {
    /// Data type to download
    #[command(subcommand)]
    pub data_type: DataType,
}

/// Data types available for download
#[derive(Subcommand, Debug)]
pub enum DataType {
    /// Download OHLCV bars
    Bars(BarsArgs),
    /// Download aggregate trades
    AggTrades(AggTradesArgs),
    /// Download funding rates
    Funding(FundingArgs),
}

/// Arguments for downloading bars (T084)
#[derive(Parser, Debug)]
pub struct BarsArgs {
    /// Exchange identifier (e.g., BINANCE:BTC/USDT:USDT)
    #[arg(long)]
    pub identifier: String,

    /// Trading symbol (e.g., BTCUSDT)
    #[arg(long)]
    pub symbol: String,

    /// Time interval (e.g., 1m, 1h, 1d)
    #[arg(long)]
    pub interval: String,

    /// Start time (YYYY-MM-DD format)
    #[arg(long)]
    pub start_time: String,

    /// End time (YYYY-MM-DD format)
    #[arg(long)]
    pub end_time: String,
}

/// Arguments for downloading aggregate trades (T129)
#[derive(Parser, Debug)]
pub struct AggTradesArgs {
    /// Exchange identifier (e.g., BINANCE:BTC/USDT:USDT)
    #[arg(long)]
    pub identifier: String,

    /// Trading symbol (e.g., BTCUSDT)
    #[arg(long)]
    pub symbol: String,

    /// Start time (YYYY-MM-DD format or YYYY-MM-DDTHH:MM:SS)
    #[arg(long)]
    pub start_time: String,

    /// End time (YYYY-MM-DD format or YYYY-MM-DDTHH:MM:SS)
    #[arg(long)]
    pub end_time: String,
}

/// Arguments for downloading funding rates (T151)
#[derive(Parser, Debug)]
pub struct FundingArgs {
    /// Exchange identifier (e.g., BINANCE:BTC/USDT:USDT)
    #[arg(long)]
    pub identifier: String,

    /// Trading symbol (e.g., BTCUSDT)
    #[arg(long)]
    pub symbol: String,

    /// Start time (YYYY-MM-DD format or YYYY-MM-DDTHH:MM:SS)
    #[arg(long)]
    pub start_time: String,

    /// End time (YYYY-MM-DD format or YYYY-MM-DDTHH:MM:SS)
    #[arg(long)]
    pub end_time: String,
}

/// Output format options (T086)
#[derive(Debug, Clone, Copy)]
pub enum OutputFormat {
    /// JSON output
    Json,
    /// Human-readable output
    Human,
}

impl FromStr for OutputFormat {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "json" => Ok(OutputFormat::Json),
            "human" => Ok(OutputFormat::Human),
            _ => Err(format!("Invalid output format: {s}")),
        }
    }
}

/// Download result for JSON output
#[derive(Debug, Serialize)]
struct DownloadResult {
    success: bool,
    identifier: String,
    symbol: String,
    interval: String,
    start_time: i64,
    end_time: i64,
    output_path: String,
    bars_downloaded: u64,
    bars_written: u64,
    api_requests: u64,
    retries: u64,
    error: Option<String>,
}
impl BarsArgs {
    fn parse_start_time(&self) -> Result<i64, CliError> {
        parse_start_time_flexible(&self.start_time)
    }

    fn parse_end_time(&self) -> Result<i64, CliError> {
        parse_end_time_flexible(&self.end_time)
    }

    /// Execute bars download (T086-T087)
    pub async fn execute(&self, cli: &Cli, shutdown: SharedShutdown) -> Result<(), CliError> {
        let start_time = self.parse_start_time()?;
        let end_time = self.parse_end_time()?;
        let interval = Interval::from_str(&self.interval)
            .map_err(|e| CliError::InvalidArgument(format!("Invalid interval: {e}")))?;

        // Handle Reset/Verify modes before starting downloads
        let resume_dir = cli.resume_dir.clone().unwrap_or_else(|| PathBuf::from(".resume"));
        match cli.resume {
            ResumeMode::Reset => handle_resume_reset(&resume_dir)?,
            ResumeMode::Verify => handle_resume_verify(&resume_dir)?,
            _ => {}
        }

        // Always use hierarchical structure - no exceptions
        use crate::output::{split_into_month_ranges, DataType, OutputPathBuilder};

        let root = cli
            .data_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from("data"));

        // Ensure base directories exist
        let builder = OutputPathBuilder::new(root.clone(), &self.identifier, &self.symbol)
            .with_data_type(DataType::Bars);
        builder
            .ensure_directories()
            .map_err(|e| CliError::InvalidArgument(format!("Failed to create directories: {e}")))?;

        // Split time range into monthly chunks
        let month_ranges = split_into_month_ranges(start_time, end_time);

        info!(
            "Downloading {} months of data: {} {} from {} to {}",
            month_ranges.len(),
            self.symbol,
            self.interval,
            self.start_time,
            self.end_time
        );

        // Pre-build job configs for concurrent execution (F040)
        let job_configs: Vec<_> = month_ranges
            .into_iter()
            .map(|month_range| {
                let path_builder =
                    OutputPathBuilder::new(root.clone(), &self.identifier, &self.symbol)
                        .with_data_type(DataType::Bars)
                        .with_interval(interval)
                        .with_month(month_range.month);

                let output_path = path_builder.build().map_err(|e| {
                    CliError::InvalidArgument(format!("Failed to build output path: {e}"))
                })?;
                Ok((month_range, output_path))
            })
            .collect::<Result<Vec<_>, CliError>>()?;

        // Execute jobs with configurable concurrency (F040)
        let concurrency = cli.concurrency.max(1);
        let config = JobConfig::from_cli(cli);
        info!(
            "Processing {} months with concurrency {}",
            job_configs.len(),
            concurrency
        );

        let results: Vec<Result<(), CliError>> = stream::iter(job_configs)
            .map(|(month_range, output_path)| {
                let identifier = self.identifier.clone();
                let symbol = self.symbol.clone();
                let config_clone = config.clone();
                let shutdown_clone = shutdown.clone();

                async move {
                    info!(
                        "Processing month {}: {} to {}",
                        month_range.month, month_range.start_ms, month_range.end_ms
                    );

                    execute_bars_job(
                        &identifier,
                        &symbol,
                        &config_clone,
                        shutdown_clone,
                        interval,
                        month_range.start_ms,
                        month_range.end_ms,
                        output_path,
                    )
                    .await
                }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;

        // Check for errors
        for result in results {
            result?;
        }

        Ok(())
    }
}

/// Execute a bars download job with given parameters (F040 helper)
///
/// This standalone function allows concurrent execution of multiple bar download jobs.
#[allow(clippy::too_many_arguments)]
async fn execute_bars_job(
    identifier: &str,
    symbol: &str,
    config: &JobConfig,
    shutdown: SharedShutdown,
    interval: crate::Interval,
    start_time: i64,
    end_time: i64,
    output_path: PathBuf,
) -> Result<(), CliError> {
    // Create download job
    let job = DownloadJob::new(
        identifier.to_string(),
        symbol.to_string(),
        interval,
        start_time,
        end_time,
        output_path.clone(),
    );

    let executor = config.create_executor(shutdown);

    // Create progress bar
    let progress = create_progress_bar(&job);

    // Execute download
    info!(
        "Starting download: {} {} from {} to {}",
        symbol, interval, start_time, end_time
    );

    let result = executor
        .execute_bars_job(job.clone(), Some(progress.clone()))
        .await;

    progress.finish_and_clear();

    // Format output
    match config.output_format {
        OutputFormat::Json => {
            output_json_result(&job, &result);
        }
        OutputFormat::Human => {
            output_human_result(&job, &result);
        }
    }

    result.map(|_| ()).map_err(CliError::DownloadError)
}

/// Helper function for concurrent aggTrades job execution (F040)
async fn execute_aggtrades_job(
    identifier: &str,
    symbol: &str,
    config: &JobConfig,
    shutdown: SharedShutdown,
    start_time: i64,
    end_time: i64,
    output_path: PathBuf,
) -> Result<(), CliError> {
    // Create download job
    let job = DownloadJob::new_aggtrades(
        identifier.to_string(),
        symbol.to_string(),
        start_time,
        end_time,
        output_path.clone(),
    );

    let executor = config.create_executor(shutdown);

    // Create progress bar
    let progress = ProgressBar::new_spinner();
    progress.set_message(format!("Downloading {symbol} aggTrades"));

    // Execute download
    info!(
        "Starting aggTrades download: {} from {} to {}",
        symbol, start_time, end_time
    );

    let result = executor.execute_aggtrades_job(job.clone()).await;

    progress.finish_and_clear();

    // Format output
    match config.output_format {
        OutputFormat::Json => {
            output_aggtrades_json_result(&job, &result);
        }
        OutputFormat::Human => {
            output_aggtrades_human_result(&job, &result);
        }
    }

    result.map(|_| ()).map_err(CliError::DownloadError)
}

impl AggTradesArgs {
    fn parse_start_time(&self) -> Result<i64, CliError> {
        parse_start_time_flexible(&self.start_time)
    }

    fn parse_end_time(&self) -> Result<i64, CliError> {
        parse_end_time_flexible(&self.end_time)
    }

    /// Execute aggTrades download (T129)
    pub async fn execute(&self, cli: &Cli, shutdown: SharedShutdown) -> Result<(), CliError> {
        let start_time = self.parse_start_time()?;
        let end_time = self.parse_end_time()?;

        // Handle Reset/Verify modes before starting downloads
        let resume_dir = cli.resume_dir.clone().unwrap_or_else(|| PathBuf::from(".resume"));
        match cli.resume {
            ResumeMode::Reset => handle_resume_reset(&resume_dir)?,
            ResumeMode::Verify => handle_resume_verify(&resume_dir)?,
            _ => {}
        }

        // Always use hierarchical structure - no exceptions
        use crate::output::{split_into_month_ranges, DataType, OutputPathBuilder};

        let root = cli
            .data_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from("data"));

        // Ensure base directories exist
        let builder = OutputPathBuilder::new(root.clone(), &self.identifier, &self.symbol)
            .with_data_type(DataType::AggTrades);
        builder
            .ensure_directories()
            .map_err(|e| CliError::InvalidArgument(format!("Failed to create directories: {e}")))?;

        // Split time range into monthly chunks
        let month_ranges = split_into_month_ranges(start_time, end_time);

        info!(
            "Downloading {} months of aggTrades data: {} from {} to {}",
            month_ranges.len(),
            self.symbol,
            self.start_time,
            self.end_time
        );

        // Pre-build job configs for concurrent execution (F040)
        let job_configs: Vec<_> = month_ranges
            .into_iter()
            .map(|month_range| {
                let path_builder =
                    OutputPathBuilder::new(root.clone(), &self.identifier, &self.symbol)
                        .with_data_type(DataType::AggTrades)
                        .with_month(month_range.month);

                let output_path = path_builder.build().map_err(|e| {
                    CliError::InvalidArgument(format!("Failed to build output path: {e}"))
                })?;
                Ok((month_range, output_path))
            })
            .collect::<Result<Vec<_>, CliError>>()?;

        // Execute jobs with configurable concurrency (F040)
        let concurrency = cli.concurrency.max(1);
        let config = JobConfig::from_cli(cli);
        info!(
            "Processing {} months with concurrency {}",
            job_configs.len(),
            concurrency
        );

        let results: Vec<Result<(), CliError>> = stream::iter(job_configs)
            .map(|(month_range, output_path)| {
                let identifier = self.identifier.clone();
                let symbol = self.symbol.clone();
                let config_clone = config.clone();
                let shutdown_clone = shutdown.clone();

                async move {
                    info!(
                        "Processing month {}: {} to {}",
                        month_range.month, month_range.start_ms, month_range.end_ms
                    );

                    execute_aggtrades_job(
                        &identifier,
                        &symbol,
                        &config_clone,
                        shutdown_clone,
                        month_range.start_ms,
                        month_range.end_ms,
                        output_path,
                    )
                    .await
                }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;

        // Check for errors
        for result in results {
            result?;
        }

        Ok(())
    }
}

/// Create progress bar with style (T087)
fn create_progress_bar(job: &DownloadJob) -> ProgressBar {
    let (total_bars, message) = match job.job_type {
        crate::downloader::JobType::Bars { interval } => {
            let interval_ms = interval.to_milliseconds();
            let total_duration = job.end_time - job.start_time;
            let total_bars = ((total_duration + interval_ms - 1) / interval_ms) as u64;
            (
                total_bars,
                format!("Downloading {} {}", job.symbol, interval),
            )
        }
        _ => (0, format!("Downloading {}", job.symbol)),
    };

    let pb = ProgressBar::new(total_bars);
    pb.set_style(
        ProgressStyle::default_bar()
            // SAFETY: Template is a compile-time constant that has been tested.
            // expect() is safe here as the template format is validated at compile time.
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
            .expect("hardcoded template is valid")
            .progress_chars("#>-"),
    );
    pb.set_message(message);
    pb
}

/// Output result as JSON (T086)
fn output_json_result(
    job: &DownloadJob,
    result: &Result<JobProgress, crate::downloader::DownloadError>,
) {
    let interval_str = match job.job_type {
        crate::downloader::JobType::Bars { interval } => interval.to_string(),
        _ => "N/A".to_string(),
    };

    let output = match result {
        Ok(progress) => DownloadResult {
            success: true,
            identifier: job.identifier.clone(),
            symbol: job.symbol.clone(),
            interval: interval_str,
            start_time: job.start_time,
            end_time: job.end_time,
            output_path: job.output_path.display().to_string(),
            bars_downloaded: progress.downloaded_bars,
            bars_written: progress.written_bars,
            api_requests: progress.api_requests,
            retries: progress.retries,
            error: None,
        },
        Err(e) => DownloadResult {
            success: false,
            identifier: job.identifier.clone(),
            symbol: job.symbol.clone(),
            interval: interval_str,
            start_time: job.start_time,
            end_time: job.end_time,
            output_path: job.output_path.display().to_string(),
            bars_downloaded: 0,
            bars_written: 0,
            api_requests: 0,
            retries: 0,
            error: Some(e.to_string()),
        },
    };

    println!("{}", serde_json::to_string(&output).unwrap());
}

/// Output result in human-readable format (T086)
fn output_human_result(
    job: &DownloadJob,
    result: &Result<JobProgress, crate::downloader::DownloadError>,
) {
    match result {
        Ok(progress) => {
            println!("\nDownload completed successfully!");
            if let crate::downloader::JobType::Bars { interval } = job.job_type {
                println!("Symbol: {} {}", job.symbol, interval);
            }
            println!("Output: {}", job.output_path.display());
            println!("Bars downloaded: {}", progress.downloaded_bars);
            println!("Bars written: {}", progress.written_bars);
            if progress.retries > 0 {
                println!("Retries: {}", progress.retries);
            }
        }
        Err(e) => {
            eprintln!("\nDownload failed!");
            eprintln!("Error: {e}");
            error!("Download failed: {}", e);
        }
    }
}

/// Output aggTrades result as JSON
fn output_aggtrades_json_result(
    job: &DownloadJob,
    result: &Result<JobProgress, crate::downloader::DownloadError>,
) {
    let output = match result {
        Ok(progress) => serde_json::json!({
            "success": true,
            "identifier": job.identifier,
            "symbol": job.symbol,
            "data_type": "aggtrades",
            "start_time": job.start_time,
            "end_time": job.end_time,
            "output_path": job.output_path.display().to_string(),
            "trades_downloaded": progress.downloaded_bars,
            "trades_written": progress.written_bars,
            "api_requests": progress.api_requests,
            "retries": progress.retries,
            "error": null,
        }),
        Err(e) => serde_json::json!({
            "success": false,
            "identifier": job.identifier,
            "symbol": job.symbol,
            "data_type": "aggtrades",
            "start_time": job.start_time,
            "end_time": job.end_time,
            "output_path": job.output_path.display().to_string(),
            "trades_downloaded": 0,
            "trades_written": 0,
            "api_requests": 0,
            "retries": 0,
            "error": e.to_string(),
        }),
    };

    println!("{}", serde_json::to_string(&output).unwrap());
}

/// Output aggTrades result in human-readable format
fn output_aggtrades_human_result(
    job: &DownloadJob,
    result: &Result<JobProgress, crate::downloader::DownloadError>,
) {
    match result {
        Ok(progress) => {
            println!("\nAggTrades download completed successfully!");
            println!("Symbol: {}", job.symbol);
            println!("Output: {}", job.output_path.display());
            println!("Trades downloaded: {}", progress.downloaded_bars);
            println!("Trades written: {}", progress.written_bars);
            if progress.retries > 0 {
                println!("Retries: {}", progress.retries);
            }
        }
        Err(e) => {
            eprintln!("\nAggTrades download failed!");
            eprintln!("Error: {e}");
            error!("Download failed: {}", e);
        }
    }
}

impl FundingArgs {
    fn parse_start_time(&self) -> Result<i64, CliError> {
        parse_start_time_flexible(&self.start_time)
    }

    fn parse_end_time(&self) -> Result<i64, CliError> {
        parse_end_time_flexible(&self.end_time)
    }

    /// Execute funding rates download (T151)
    pub async fn execute(&self, cli: &Cli, shutdown: SharedShutdown) -> Result<(), CliError> {
        let start_time = self.parse_start_time()?;
        let end_time = self.parse_end_time()?;

        // Handle Reset/Verify modes before starting downloads
        let resume_dir = cli.resume_dir.clone().unwrap_or_else(|| PathBuf::from(".resume"));
        match cli.resume {
            ResumeMode::Reset => handle_resume_reset(&resume_dir)?,
            ResumeMode::Verify => handle_resume_verify(&resume_dir)?,
            _ => {}
        }

        // Always use hierarchical structure - no exceptions
        use crate::output::{split_into_month_ranges, DataType, OutputPathBuilder};

        let root = cli
            .data_dir
            .clone()
            .unwrap_or_else(|| PathBuf::from("data"));

        // Ensure base directories exist
        let builder = OutputPathBuilder::new(root.clone(), &self.identifier, &self.symbol)
            .with_data_type(DataType::Funding);
        builder
            .ensure_directories()
            .map_err(|e| CliError::InvalidArgument(format!("Failed to create directories: {e}")))?;

        // Split time range into monthly chunks
        let month_ranges = split_into_month_ranges(start_time, end_time);

        info!(
            "Downloading {} months of funding data: {} from {} to {}",
            month_ranges.len(),
            self.symbol,
            self.start_time,
            self.end_time
        );

        // Pre-build job configs for concurrent execution (F040)
        let job_configs: Vec<_> = month_ranges
            .into_iter()
            .map(|month_range| {
                let path_builder =
                    OutputPathBuilder::new(root.clone(), &self.identifier, &self.symbol)
                        .with_data_type(DataType::Funding)
                        .with_month(month_range.month);

                let output_path = path_builder.build().map_err(|e| {
                    CliError::InvalidArgument(format!("Failed to build output path: {e}"))
                })?;
                Ok((month_range, output_path))
            })
            .collect::<Result<Vec<_>, CliError>>()?;

        // Execute jobs with configurable concurrency (F040)
        let concurrency = cli.concurrency.max(1);
        let config = JobConfig::from_cli(cli);
        info!(
            "Processing {} months with concurrency {}",
            job_configs.len(),
            concurrency
        );

        let results: Vec<Result<(), CliError>> = stream::iter(job_configs)
            .map(|(month_range, output_path)| {
                let identifier = self.identifier.clone();
                let symbol = self.symbol.clone();
                let config_clone = config.clone();
                let shutdown_clone = shutdown.clone();

                async move {
                    info!(
                        "Processing month {}: {} to {}",
                        month_range.month, month_range.start_ms, month_range.end_ms
                    );

                    execute_funding_job(
                        &identifier,
                        &symbol,
                        &config_clone,
                        shutdown_clone,
                        month_range.start_ms,
                        month_range.end_ms,
                        output_path,
                    )
                    .await
                }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;

        // Check for errors
        for result in results {
            result?;
        }

        Ok(())
    }
}

/// Helper function for concurrent funding job execution (F040)
async fn execute_funding_job(
    identifier: &str,
    symbol: &str,
    config: &JobConfig,
    shutdown: SharedShutdown,
    start_time: i64,
    end_time: i64,
    output_path: PathBuf,
) -> Result<(), CliError> {
    // Create download job
    let job = DownloadJob::new_funding(
        identifier.to_string(),
        symbol.to_string(),
        start_time,
        end_time,
        output_path.clone(),
    );

    let executor = config.create_executor(shutdown);

    // Create progress bar
    let progress = ProgressBar::new_spinner();
    progress.set_message(format!("Downloading {symbol} funding rates"));

    // Execute download
    info!(
        "Starting funding rates download: {} from {} to {}",
        symbol, start_time, end_time
    );

    let result = executor.execute_funding_job(job.clone()).await;

    progress.finish_and_clear();

    // Format output
    match config.output_format {
        OutputFormat::Json => {
            output_funding_json_result(&job, &result);
        }
        OutputFormat::Human => {
            output_funding_human_result(&job, &result);
        }
    }

    result.map(|_| ()).map_err(CliError::DownloadError)
}

/// Output funding result as JSON
fn output_funding_json_result(
    job: &DownloadJob,
    result: &Result<JobProgress, crate::downloader::DownloadError>,
) {
    let output = match result {
        Ok(progress) => serde_json::json!({
            "success": true,
            "identifier": job.identifier,
            "symbol": job.symbol,
            "data_type": "funding",
            "start_time": job.start_time,
            "end_time": job.end_time,
            "output_path": job.output_path.display().to_string(),
            "rates_downloaded": progress.downloaded_bars,
            "rates_written": progress.written_bars,
            "api_requests": progress.api_requests,
            "retries": progress.retries,
            "error": null,
        }),
        Err(e) => serde_json::json!({
            "success": false,
            "identifier": job.identifier,
            "symbol": job.symbol,
            "data_type": "funding",
            "start_time": job.start_time,
            "end_time": job.end_time,
            "output_path": job.output_path.display().to_string(),
            "rates_downloaded": 0,
            "rates_written": 0,
            "api_requests": 0,
            "retries": 0,
            "error": e.to_string(),
        }),
    };

    println!("{}", serde_json::to_string(&output).unwrap());
}

/// Output funding result in human-readable format
fn output_funding_human_result(
    job: &DownloadJob,
    result: &Result<JobProgress, crate::downloader::DownloadError>,
) {
    match result {
        Ok(progress) => {
            println!("\nFunding rates download completed successfully!");
            println!("Symbol: {}", job.symbol);
            println!("Output: {}", job.output_path.display());
            println!("Rates downloaded: {}", progress.downloaded_bars);
            println!("Rates written: {}", progress.written_bars);
            if progress.retries > 0 {
                println!("Retries: {}", progress.retries);
            }
        }
        Err(e) => {
            eprintln!("\nFunding rates download failed!");
            eprintln!("Error: {e}");
            error!("Download failed: {}", e);
        }
    }
}
