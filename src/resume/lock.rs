//! File locking for concurrent resume state access
//!
//! Implements advisory file locking using fd-lock (FR-037)

use super::state::ResumeError;
use fd_lock::RwLock;
use std::fs::{File, OpenOptions};
use std::path::Path;

/// Resume state lock wrapper
pub struct ResumeLock {
    #[allow(dead_code)]
    lock: RwLock<File>,
}

impl ResumeLock {
    /// Acquire an exclusive lock on the resume state file
    ///
    /// Blocks until the lock is available.
    pub fn acquire(path: &Path) -> Result<Self, ResumeError> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| ResumeError::IoError(e.to_string()))?;
        }

        // Open/create lock file
        let lock_path = path.with_extension("lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| ResumeError::LockError(format!("Failed to open lock file: {e}")))?;

        let mut lock = RwLock::new(file);

        // Acquire exclusive lock (blocks)
        let _ = lock
            .write()
            .map_err(|e| ResumeError::LockError(format!("Failed to acquire lock: {e}")))?;

        Ok(Self { lock })
    }

    /// Try to acquire an exclusive lock without blocking
    ///
    /// Returns an error immediately if the lock is held.
    pub fn try_acquire(path: &Path) -> Result<Self, ResumeError> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| ResumeError::IoError(e.to_string()))?;
        }

        // Open/create lock file
        let lock_path = path.with_extension("lock");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| ResumeError::LockError(format!("Failed to open lock file: {e}")))?;

        let mut lock = RwLock::new(file);

        // Try to acquire exclusive lock (non-blocking)
        let _ = lock
            .try_write()
            .map_err(|e| ResumeError::LockError(format!("Failed to acquire lock: {e}")))?;

        Ok(Self { lock })
    }
}

// Lock is automatically released when ResumeLock is dropped
