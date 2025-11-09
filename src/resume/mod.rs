//! Resume capability for download jobs
//!
//! Provides persistent state management with atomic writes and file locking.

pub mod checkpoint;
pub mod lock;
pub mod state;

pub use checkpoint::{Checkpoint, CheckpointType};
pub use lock::ResumeLock;
pub use state::{ResumeError, ResumeState, StateMetadata};
