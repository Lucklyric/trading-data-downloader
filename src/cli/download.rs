//! Download command implementation (T082-T087)

use crate::downloader::{DownloadExecutor, DownloadJob, JobProgress};
use crate::shutdown::SharedShutdown;
use crate::Interval;
use chrono::{DateTime, NaiveDate};
use clap::{Parser, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use serde::Serialize;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use tracing::{error, info};

use super::CliError;

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

    /// Number of concurrent downloads (default: 1)
    #[arg(long, global = true, default_value = "1")]
    pub concurrency: usize,

    /// Maximum number of retries for failed requests (default: 5, range: 1-20)
    #[arg(long, global = true, default_value = "5", value_parser = clap::value_parser!(u32).range(1..=20))]
    pub max_retries: u32,

    /// Force re-download even if output file already covers the requested range
    #[arg(long, global = true, default_value_t = false)]
    pub force: bool,

    /// Enable Prometheus metrics server on localhost:9090
    #[arg(long, global = true, default_value_t = false)]
    pub metrics: bool,
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

    /// Output file path
    #[arg(long, short = 'o')]
    pub output: Option<PathBuf>,
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

    /// Output file path
    #[arg(long, short = 'o')]
    pub output: Option<PathBuf>,
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

    /// Output file path
    #[arg(long, short = 'o')]
    pub output: Option<PathBuf>,
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

/// Validate that a path is safe to delete (guards against catastrophic data loss)
fn validate_safe_delete_path(path: &Path) -> Result<(), CliError> {
    let path_str = path.to_string_lossy();

    // 1. Check for dangerous Unix absolute paths
    #[cfg(unix)]
    {
        if path_str == "/"
            || path_str == "//"
            || (path_str.starts_with("/Users") && path_str.matches('/').count() <= 2)
            || (path_str.starts_with("/home") && path_str.matches('/').count() <= 2)
            || path_str.starts_with("/etc")
            || path_str.starts_with("/var")
            || path_str.starts_with("/usr")
            || path_str.starts_with("/bin")
            || path_str.starts_with("/sbin")
            || path_str.starts_with("/lib")
        {
            return Err(CliError::InvalidArgument(format!(
                "Refusing to delete dangerous system path: {path_str}. Resume directories should be in the current project."
            )));
        }
    }

    // 2. Check for dangerous Windows paths
    #[cfg(windows)]
    {
        // Check for drive roots (C:\, D:\, etc.)
        if path_str.len() <= 3 && path_str.contains(":\\") {
            return Err(CliError::InvalidArgument(format!(
                "Refusing to delete drive root: {path_str}. Resume directories should be in the current project."
            )));
        }

        // Check for Windows system directories
        let lower = path_str.to_lowercase();
        if lower.starts_with("c:\\windows")
            || lower.starts_with("c:\\program files")
            || (lower.starts_with("c:\\users") && lower.matches('\\').count() <= 2)
            || lower.starts_with("\\\\")
        {
            // UNC paths
            return Err(CliError::InvalidArgument(format!(
                "Refusing to delete dangerous Windows path: {path_str}. Resume directories should be in the current project."
            )));
        }
    }

    // 3. Check for parent directory references (cross-platform)
    if path_str.contains("..") {
        return Err(CliError::InvalidArgument(
            "Path contains '..' which could escape the intended directory. Use absolute paths or simple relative paths.".to_string()
        ));
    }

    // 4. Check if path is a symlink (before canonicalization)
    if let Ok(metadata) = path.symlink_metadata() {
        if metadata.file_type().is_symlink() {
            return Err(CliError::InvalidArgument(format!(
                "Refusing to delete symlink: {path_str}. Delete the actual directory, not the symlink."
            )));
        }
    }

    // 5. Resolve and validate canonical path
    if let Ok(canonical) = path.canonicalize() {
        let canonical_str = canonical.to_string_lossy();
        // Ensure the canonical path is still within a reasonable scope
        #[cfg(unix)]
        if canonical_str == "/" || canonical_str == "//" {
            return Err(CliError::InvalidArgument(format!(
                "Path {path_str} resolves to dangerous location: {canonical_str}"
            )));
        }

        #[cfg(windows)]
        if canonical_str.len() <= 3 && canonical_str.contains(":\\") {
            return Err(CliError::InvalidArgument(format!(
                "Path {path_str} resolves to drive root: {canonical_str}"
            )));
        }
    }

    Ok(())
}

/// Prompt user for confirmation before deleting resume directory
fn confirm_resume_reset(resume_dir: &Path) -> Result<(), CliError> {
    eprintln!("\n!!! WARNING !!!");
    eprintln!("About to delete resume directory: {}", resume_dir.display());
    eprintln!("This will remove all checkpoint data and cannot be undone.");
    eprintln!("\nType 'yes' to confirm deletion: ");

    let mut input = String::new();
    std::io::stdin()
        .read_line(&mut input)
        .map_err(|e| CliError::InvalidArgument(format!("Failed to read confirmation: {e}")))?;

    if input.trim() != "yes" {
        return Err(CliError::InvalidArgument(
            "Reset cancelled by user.".to_string(),
        ));
    }

    Ok(())
}

impl BarsArgs {
    /// Parse start time from YYYY-MM-DD format
    fn parse_start_time(&self) -> Result<i64, CliError> {
        let date = NaiveDate::parse_from_str(&self.start_time, "%Y-%m-%d")
            .map_err(|e| CliError::InvalidArgument(format!("Invalid start time: {e}")))?;

        let datetime = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| CliError::InvalidArgument("Invalid start time".to_string()))?;

        Ok(datetime.and_utc().timestamp_millis())
    }

    /// Parse end time from YYYY-MM-DD format
    fn parse_end_time(&self) -> Result<i64, CliError> {
        let date = NaiveDate::parse_from_str(&self.end_time, "%Y-%m-%d")
            .map_err(|e| CliError::InvalidArgument(format!("Invalid end time: {e}")))?;

        let datetime = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| CliError::InvalidArgument("Invalid end time".to_string()))?;

        Ok(datetime.and_utc().timestamp_millis())
    }

    /// Get output path or generate default
    fn get_output_path(&self) -> PathBuf {
        self.output.clone().unwrap_or_else(|| {
            PathBuf::from(format!(
                "{}_{}.csv",
                self.symbol.to_lowercase(),
                self.interval
            ))
        })
    }

    /// Execute bars download (T086-T087)
    pub async fn execute(&self, cli: &Cli, shutdown: SharedShutdown) -> Result<(), CliError> {
        let start_time = self.parse_start_time()?;
        let end_time = self.parse_end_time()?;
        let output_path = self.get_output_path();

        let interval = Interval::from_str(&self.interval)
            .map_err(|e| CliError::InvalidArgument(format!("Invalid interval: {e}")))?;

        // Create download job
        let job = DownloadJob::new(
            self.identifier.clone(),
            self.symbol.clone(),
            interval,
            start_time,
            end_time,
            output_path.clone(),
        );

        // Create executor
        let executor = match cli.resume {
            ResumeMode::Off => DownloadExecutor::new(),
            ResumeMode::On => {
                let resume_dir = cli
                    .resume_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from(".resume"));
                DownloadExecutor::new_with_resume(resume_dir)
            }
            ResumeMode::Reset => {
                let resume_dir = cli
                    .resume_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from(".resume"));
                // CRITICAL SAFETY GUARDS: Prevent catastrophic data loss
                if resume_dir.exists() {
                    validate_safe_delete_path(&resume_dir)?;
                    confirm_resume_reset(&resume_dir)?;
                    std::fs::remove_dir_all(&resume_dir).map_err(|e| {
                        CliError::InvalidArgument(format!("Failed to reset resume state: {e}"))
                    })?;
                    eprintln!("Resume state deleted successfully.\n");
                }
                DownloadExecutor::new_with_resume(resume_dir)
            }
            ResumeMode::Verify => {
                let resume_dir = cli
                    .resume_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from(".resume"));
                // TODO: Add resume state verification logic
                // For now, just use normal resume
                DownloadExecutor::new_with_resume(resume_dir)
            }
        };

        // Apply CLI flags to executor (P0-6 fix)
        let executor = executor
            .with_max_retries(cli.max_retries)
            .with_force(cli.force)
            .with_shutdown(shutdown);

        // Create progress bar (T087)
        let progress = create_progress_bar(&job);

        // Execute download
        info!(
            "Starting download: {} {} from {} to {}",
            self.symbol, self.interval, self.start_time, self.end_time
        );

        let result = executor.execute_bars_job(job.clone()).await;

        progress.finish_and_clear();

        // Format output (T086)
        match cli.output_format {
            OutputFormat::Json => {
                output_json_result(&job, &result);
            }
            OutputFormat::Human => {
                output_human_result(&job, &result);
            }
        }

        result.map(|_| ()).map_err(CliError::DownloadError)
    }
}

impl AggTradesArgs {
    /// Parse start time from YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format
    fn parse_start_time(&self) -> Result<i64, CliError> {
        // Try parsing as full datetime first
        if let Ok(dt) = DateTime::parse_from_rfc3339(&format!("{}Z", self.start_time)) {
            return Ok(dt.timestamp_millis());
        }

        // Fall back to date-only format
        let date = NaiveDate::parse_from_str(&self.start_time, "%Y-%m-%d")
            .map_err(|e| CliError::InvalidArgument(format!("Invalid start time: {e}")))?;

        let datetime = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| CliError::InvalidArgument("Invalid start time".to_string()))?;

        Ok(datetime.and_utc().timestamp_millis())
    }

    /// Parse end time from YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format
    fn parse_end_time(&self) -> Result<i64, CliError> {
        // Try parsing as full datetime first
        if let Ok(dt) = DateTime::parse_from_rfc3339(&format!("{}Z", self.end_time)) {
            return Ok(dt.timestamp_millis());
        }

        // Fall back to date-only format
        let date = NaiveDate::parse_from_str(&self.end_time, "%Y-%m-%d")
            .map_err(|e| CliError::InvalidArgument(format!("Invalid end time: {e}")))?;

        let datetime = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| CliError::InvalidArgument("Invalid end time".to_string()))?;

        Ok(datetime.and_utc().timestamp_millis())
    }

    /// Get output path or generate default
    fn get_output_path(&self) -> PathBuf {
        self.output
            .clone()
            .unwrap_or_else(|| PathBuf::from(format!("{}_trades.csv", self.symbol.to_lowercase())))
    }

    /// Execute aggTrades download (T129)
    pub async fn execute(&self, cli: &Cli, shutdown: SharedShutdown) -> Result<(), CliError> {
        let start_time = self.parse_start_time()?;
        let end_time = self.parse_end_time()?;
        let output_path = self.get_output_path();

        // Create download job
        let job = DownloadJob::new_aggtrades(
            self.identifier.clone(),
            self.symbol.clone(),
            start_time,
            end_time,
            output_path.clone(),
        );

        // Create executor
        let executor = match cli.resume {
            ResumeMode::Off => DownloadExecutor::new(),
            ResumeMode::On => {
                let resume_dir = cli
                    .resume_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from(".resume"));
                DownloadExecutor::new_with_resume(resume_dir)
            }
            ResumeMode::Reset => {
                let resume_dir = cli
                    .resume_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from(".resume"));
                // CRITICAL SAFETY GUARDS: Prevent catastrophic data loss
                if resume_dir.exists() {
                    validate_safe_delete_path(&resume_dir)?;
                    confirm_resume_reset(&resume_dir)?;
                    std::fs::remove_dir_all(&resume_dir).map_err(|e| {
                        CliError::InvalidArgument(format!("Failed to reset resume state: {e}"))
                    })?;
                    eprintln!("Resume state deleted successfully.\n");
                }
                DownloadExecutor::new_with_resume(resume_dir)
            }
            ResumeMode::Verify => {
                let resume_dir = cli
                    .resume_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from(".resume"));
                // TODO: Add resume state verification logic
                // For now, just use normal resume
                DownloadExecutor::new_with_resume(resume_dir)
            }
        };

        // Apply CLI flags to executor (P0-6 fix)
        let executor = executor
            .with_max_retries(cli.max_retries)
            .with_force(cli.force)
            .with_shutdown(shutdown);

        // Create progress bar
        let progress = ProgressBar::new_spinner();
        progress.set_message(format!("Downloading {} aggTrades", self.symbol));

        // Execute download
        info!(
            "Starting aggTrades download: {} from {} to {}",
            self.symbol, self.start_time, self.end_time
        );

        let result = executor.execute_aggtrades_job(job.clone()).await;

        progress.finish_and_clear();

        // Format output
        match cli.output_format {
            OutputFormat::Json => {
                output_aggtrades_json_result(&job, &result);
            }
            OutputFormat::Human => {
                output_aggtrades_human_result(&job, &result);
            }
        }

        result.map(|_| ()).map_err(CliError::DownloadError)
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
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) {msg}")
            .expect("Failed to create progress style")
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

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
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

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
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
    /// Parse start time from YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format
    fn parse_start_time(&self) -> Result<i64, CliError> {
        // Try parsing as full datetime first
        if let Ok(dt) = DateTime::parse_from_rfc3339(&format!("{}Z", self.start_time)) {
            return Ok(dt.timestamp_millis());
        }

        // Fall back to date-only format
        let date = NaiveDate::parse_from_str(&self.start_time, "%Y-%m-%d")
            .map_err(|e| CliError::InvalidArgument(format!("Invalid start time: {e}")))?;

        let datetime = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| CliError::InvalidArgument("Invalid start time".to_string()))?;

        Ok(datetime.and_utc().timestamp_millis())
    }

    /// Parse end time from YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format
    fn parse_end_time(&self) -> Result<i64, CliError> {
        // Try parsing as full datetime first
        if let Ok(dt) = DateTime::parse_from_rfc3339(&format!("{}Z", self.end_time)) {
            return Ok(dt.timestamp_millis());
        }

        // Fall back to date-only format
        let date = NaiveDate::parse_from_str(&self.end_time, "%Y-%m-%d")
            .map_err(|e| CliError::InvalidArgument(format!("Invalid end time: {e}")))?;

        let datetime = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| CliError::InvalidArgument("Invalid end time".to_string()))?;

        Ok(datetime.and_utc().timestamp_millis())
    }

    /// Get output path or generate default
    fn get_output_path(&self) -> PathBuf {
        self.output
            .clone()
            .unwrap_or_else(|| PathBuf::from(format!("{}_funding.csv", self.symbol.to_lowercase())))
    }

    /// Execute funding rates download (T151)
    pub async fn execute(&self, cli: &Cli, shutdown: SharedShutdown) -> Result<(), CliError> {
        let start_time = self.parse_start_time()?;
        let end_time = self.parse_end_time()?;
        let output_path = self.get_output_path();

        // Create download job
        let job = DownloadJob::new_funding(
            self.identifier.clone(),
            self.symbol.clone(),
            start_time,
            end_time,
            output_path.clone(),
        );

        // Create executor
        let executor = match cli.resume {
            ResumeMode::Off => DownloadExecutor::new(),
            ResumeMode::On => {
                let resume_dir = cli
                    .resume_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from(".resume"));
                DownloadExecutor::new_with_resume(resume_dir)
            }
            ResumeMode::Reset => {
                let resume_dir = cli
                    .resume_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from(".resume"));
                // CRITICAL SAFETY GUARDS: Prevent catastrophic data loss
                if resume_dir.exists() {
                    validate_safe_delete_path(&resume_dir)?;
                    confirm_resume_reset(&resume_dir)?;
                    std::fs::remove_dir_all(&resume_dir).map_err(|e| {
                        CliError::InvalidArgument(format!("Failed to reset resume state: {e}"))
                    })?;
                    eprintln!("Resume state deleted successfully.\n");
                }
                DownloadExecutor::new_with_resume(resume_dir)
            }
            ResumeMode::Verify => {
                let resume_dir = cli
                    .resume_dir
                    .clone()
                    .unwrap_or_else(|| PathBuf::from(".resume"));
                // TODO: Add resume state verification logic
                // For now, just use normal resume
                DownloadExecutor::new_with_resume(resume_dir)
            }
        };

        // Apply CLI flags to executor (P0-6 fix)
        let executor = executor
            .with_max_retries(cli.max_retries)
            .with_force(cli.force)
            .with_shutdown(shutdown);

        // Create progress bar
        let progress = ProgressBar::new_spinner();
        progress.set_message(format!("Downloading {} funding rates", self.symbol));

        // Execute download
        info!(
            "Starting funding rates download: {} from {} to {}",
            self.symbol, self.start_time, self.end_time
        );

        let result = executor.execute_funding_job(job.clone()).await;

        progress.finish_and_clear();

        // Format output
        match cli.output_format {
            OutputFormat::Json => {
                output_funding_json_result(&job, &result);
            }
            OutputFormat::Human => {
                output_funding_human_result(&job, &result);
            }
        }

        result.map(|_| ()).map_err(CliError::DownloadError)
    }
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

    println!("{}", serde_json::to_string_pretty(&output).unwrap());
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
