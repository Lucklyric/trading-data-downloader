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
//! // Create a new resume state
//! let mut state = ResumeState::new(
//!     "BINANCE:BTC/USDT:USDT".to_string(),
//!     "BTCUSDT".to_string(),
//!     Some("1h".to_string()),
//!     "bars".to_string(),
//! );
//!
//! // Add a checkpoint during download
//! let checkpoint = Checkpoint::time_window(
//!     1640995200000, // start_time
//!     1641081600000, // end_time
//!     1440,          // record_count
//!     65536,         // bytes_written
//! );
//! state.add_checkpoint(checkpoint);
//!
//! // Save state to disk
//! state.save(Path::new("./resume/btcusdt_bars.json")).unwrap();
//!
//! // Load state on resume
//! let loaded = ResumeState::load(Path::new("./resume/btcusdt_bars.json")).unwrap();
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
