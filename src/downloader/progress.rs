//! Progress tracking structures for long-running downloads.
//!
//! Feature 002 introduces periodic user-facing updates for download progress.
//! This module defines the data structures and helpers responsible for
//! calculating percentages, estimating remaining time, and formatting the
//! progress strings that surface in the executor loops.

use std::time::{Duration, Instant};

const DEFAULT_UPDATE_INTERVAL: Duration = Duration::from_secs(60);
const MIN_DOWNLOAD_DURATION: Duration = Duration::from_secs(30);
const FUNDING_INTERVAL_MS: i64 = 8 * 60 * 60 * 1000;

/// Type of items being downloaded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DownloadItemType {
    /// OHLCV bar downloads.
    Bars,
    /// Aggregate trade downloads.
    AggTrades,
    /// Funding rate downloads.
    FundingRates,
}

impl DownloadItemType {
    /// Human-friendly lowercase singular label ("bar", "trade", ...).
    pub fn singular(&self) -> &'static str {
        match self {
            Self::Bars => "bar",
            Self::AggTrades => "trade",
            Self::FundingRates => "funding rate",
        }
    }

    /// Human-friendly lowercase plural label ("bars", "trades", ...).
    pub fn plural(&self) -> &'static str {
        match self {
            Self::Bars => "bars",
            Self::AggTrades => "trades",
            Self::FundingRates => "funding rates",
        }
    }
}

/// Lightweight builder that controls update cadence.
#[derive(Debug, Clone)]
pub struct ProgressTracker {
    update_interval: Duration,
    min_percentage_step: f64,
}

impl ProgressTracker {
    /// Create a tracker with custom interval and percentage step.
    pub fn new(update_interval: Duration, min_percentage_step: f64) -> Self {
        Self {
            update_interval,
            min_percentage_step,
        }
    }

    /// Build a [`ProgressState`] configured with the tracker defaults.
    pub fn create_state(
        &self,
        total_expected: Option<u64>,
        item_type: DownloadItemType,
        range: Option<(i64, i64)>,
    ) -> ProgressState {
        let mut state = ProgressState::new(total_expected, item_type);
        state.update_interval = self.update_interval;
        state.min_percentage_step = self.min_percentage_step;
        state.range = range;
        state
    }
}

impl Default for ProgressTracker {
    fn default() -> Self {
        Self::new(DEFAULT_UPDATE_INTERVAL, 10.0)
    }
}

/// Progress tracking state for long-running downloads.
#[derive(Debug, Clone)]
pub struct ProgressState {
    /// Number of items downloaded so far.
    pub items_downloaded: u64,
    /// Total expected items (if known).
    pub total_expected: Option<u64>,
    /// Timestamp when download started.
    pub start_time: Instant,
    /// Last time progress was reported.
    pub last_update: Instant,
    /// Minimum interval between progress updates.
    pub update_interval: Duration,
    /// Current download rate (items per second).
    pub current_rate: f64,
    /// Type of items being downloaded.
    pub item_type: DownloadItemType,
    /// Current phase or chunk being processed.
    pub current_phase: Option<String>,
    /// Last reported completion percentage (0-100) for 10% emission logic.
    pub last_reported_percentage: f64,
    /// Minimum percentage delta required to emit a new update.
    pub min_percentage_step: f64,
    /// Optional time range (start, end) for timeline-based estimates.
    pub range: Option<(i64, i64)>,
    /// Last processed timestamp for timeline-based estimates.
    pub last_position: Option<i64>,
}

impl ProgressState {
    /// Create a new progress tracker with default intervals.
    pub fn new(total_expected: Option<u64>, item_type: DownloadItemType) -> Self {
        let now = Instant::now();
        Self {
            items_downloaded: 0,
            total_expected,
            start_time: now,
            last_update: now,
            update_interval: DEFAULT_UPDATE_INTERVAL,
            current_rate: 0.0,
            item_type,
            current_phase: None,
            last_reported_percentage: 0.0,
            min_percentage_step: 10.0,
            range: None,
            last_position: None,
        }
    }

    /// Update internal counters when new items are written to disk.
    pub fn update(&mut self, new_items: u64, latest_position: Option<i64>) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        self.items_downloaded = self.items_downloaded.saturating_add(new_items);
        if elapsed > 0.0 {
            self.current_rate = self.items_downloaded as f64 / elapsed;
        }
        if let Some(position) = latest_position {
            self.last_position = Some(position);
        }
    }

    /// Whether a progress update should be emitted based on time or percentage.
    pub fn should_emit_update(&self) -> bool {
        if self.items_downloaded == 0 {
            return false;
        }

        let percentage_jump = self
            .percentage()
            .map(|pct| pct - self.last_reported_percentage >= self.min_percentage_step)
            .unwrap_or(false);

        if percentage_jump {
            return true;
        }

        self.start_time.elapsed() >= MIN_DOWNLOAD_DURATION
            && self.last_update.elapsed() >= self.update_interval
    }

    /// Call after emitting a progress log to reset timers and cached percentage.
    pub fn mark_emitted(&mut self) {
        self.last_update = Instant::now();
        if let Some(pct) = self.percentage() {
            self.last_reported_percentage = pct;
        }
    }

    /// Set descriptive phase label (e.g., "Day 3/30").
    pub fn set_phase<S: Into<String>>(&mut self, phase: Option<S>) {
        self.current_phase = phase.map(|s| s.into());
    }

    /// Assign a time range (start/end millis) for timeline-based estimates.
    pub fn set_range(&mut self, range: Option<(i64, i64)>) {
        self.range = range;
    }

    /// Calculate download percentage (0-100) using counts or timeline.
    pub fn percentage(&self) -> Option<f64> {
        if let Some(total) = self.total_expected {
            if total == 0 {
                return Some(100.0);
            }
            return Some((self.items_downloaded as f64 / total as f64) * 100.0);
        }

        self.timeline_ratio().map(|ratio| ratio * 100.0)
    }

    /// Estimate remaining time based on counts or timeline.
    pub fn estimate_remaining(&self) -> Option<Duration> {
        if self.current_rate > 0.0 {
            if let Some(total) = self.total_expected {
                let remaining = total.saturating_sub(self.items_downloaded);
                if remaining > 0 {
                    return Some(Duration::from_secs_f64(
                        remaining as f64 / self.current_rate,
                    ));
                }
            }
        }

        if let Some(ratio) = self.timeline_ratio() {
            if (0.0..1.0).contains(&ratio) {
                let elapsed = self.start_time.elapsed().as_secs_f64();
                if elapsed > 0.0 {
                    let total_secs = elapsed / ratio;
                    let remaining_secs = (total_secs - elapsed).max(0.0);
                    return Some(Duration::from_secs_f64(remaining_secs));
                }
            }
        }

        None
    }

    /// Human-readable progress string for logging.
    pub fn format_progress(&self) -> String {
        let mut parts = vec![format!(
            "[PROGRESS] Downloaded {} {}",
            self.items_downloaded,
            self.item_type.plural()
        )];

        if let Some(pct) = self.percentage() {
            parts.push(format!("- {pct:.1}% complete"));
        }

        if let Some(phase) = &self.current_phase {
            parts.push(format!("({phase})"));
        }

        if self.current_rate > 0.0 {
            parts.push(format!(
                "at {:.0} {}/sec",
                self.current_rate,
                self.item_type.plural()
            ));
        }

        if let Some(remaining) = self.estimate_remaining() {
            parts.push(format!("- ~{} remaining", format_duration(remaining)));
        }

        parts.join(" ")
    }

    fn timeline_ratio(&self) -> Option<f64> {
        let (start, end) = self.range?;
        let position = self.last_position?;
        let span = (end - start) as f64;
        if span <= 0.0 {
            return None;
        }
        let clamped = position.clamp(start, end);
        let completed = (clamped - start) as f64;
        Some((completed / span).clamp(0.0, 1.0))
    }

    /// Estimate total expected items using only time range information.
    pub fn estimate_total_from_range(
        item_type: DownloadItemType,
        start: i64,
        end: i64,
        interval_ms: Option<i64>,
    ) -> Option<u64> {
        if end <= start {
            return Some(0);
        }
        let duration_ms = (end - start) as u64;
        match item_type {
            DownloadItemType::Bars => interval_ms.and_then(|ms| {
                if ms <= 0 {
                    None
                } else {
                    let interval = ms as u64;
                    Some(duration_ms.div_ceil(interval))
                }
            }),
            DownloadItemType::FundingRates => {
                let interval = FUNDING_INTERVAL_MS as u64;
                Some(duration_ms.div_ceil(interval))
            }
            DownloadItemType::AggTrades => None,
        }
    }
}

fn format_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m", secs / 60)
    } else {
        format!("{:.1}h", secs as f64 / 3600.0)
    }
}
