//! Resume capability for download jobs
//!
//! Provides persistent state management with atomic writes and file locking
//! to enable safe resumption of interrupted downloads.
//!
//! # Overview
//!
//! When downloading large datasets, network failures or interruptions can cause
//! data loss. This module implements a checkpointing system that periodically
//! saves progress to disk, allowing downloads to resume from the last successful
//! checkpoint rather than starting over.
//!
//! # Key Features
//!
//! - **Atomic Writes**: State updates use atomic file operations to prevent corruption
//! - **File Locking**: Prevents multiple processes from resuming the same job
//! - **Periodic Checkpointing**: Saves progress at configurable intervals
//! - **Type-Specific State**: Different checkpoint types for bars, trades, and funding rates
//!
//! # Components
//!
//! - [`state`] - Main resume state management with [`ResumeState`]
//! - [`checkpoint`] - Checkpoint data structures (last timestamp/ID processed)
//! - [`lock`] - File-based locking with [`ResumeLock`]
//!
//! # Usage Example
//!
//! ```no_run
//! use trading_data_downloader::resume::{ResumeState, Checkpoint, CheckpointType};
//! use std::path::Path;
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let resume_dir = Path::new("./resume");
//! let job_id = "binance_btcusdt_bars_1h";
//!
//! // Initialize or load resume state
//! let mut state = ResumeState::load_or_create(resume_dir, job_id)?;
//!
//! // Check if we're resuming
//! if let Some(checkpoint) = state.last_checkpoint() {
//!     match checkpoint {
//!         CheckpointType::LastTimestamp(ts) => {
//!             println!("Resuming from timestamp: {}", ts);
//!             // Continue download from ts + 1
//!         }
//!         CheckpointType::LastId(id) => {
//!             println!("Resuming from ID: {}", id);
//!             // Continue download from id + 1
//!         }
//!     }
//! }
//!
//! // During download, periodically save checkpoints
//! state.save_checkpoint(CheckpointType::LastTimestamp(1640995200000))?;
//!
//! // Mark as complete when done
//! state.mark_complete()?;
//! # Ok(())
//! # }
//! ```
//!
//! # Checkpoint Strategy
//!
//! Checkpoints are saved at different intervals depending on data type:
//! - **Bars**: Every 1000 records
//! - **Aggregate Trades**: Every 10,000 records
//! - **Funding Rates**: Every 100 records
//!
//! This balances checkpoint overhead with recovery granularity.
//!
//! # Related Modules
//!
//! - [`crate::downloader::executor`] - Uses resume state during download execution

pub mod checkpoint;
pub mod lock;
pub mod state;

pub use checkpoint::{Checkpoint, CheckpointType};
pub use lock::ResumeLock;
pub use state::{ResumeError, ResumeState, StateMetadata};
