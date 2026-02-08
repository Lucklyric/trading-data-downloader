//! File locking for concurrent resume state access
//!
//! Implements advisory file locking using fd-lock (FR-037)
//!
//! # Usage
//!
//! ```ignore
//! let mut resume_lock = ResumeLock::new(path)?;
//! let _guard = resume_lock.write()?;  // Lock held while _guard exists
//! // ... do protected work ...
//! // Lock released when _guard is dropped
//! ```

use super::state::ResumeError;
use fd_lock::{RwLock, RwLockWriteGuard};
use std::fs::{File, OpenOptions};
use std::path::Path;

/// Resume state lock wrapper
///
/// The lock is not held until you call [`write()`](Self::write) and hold
/// the returned guard. The lock is released when the guard is dropped.
pub struct ResumeLock {
    lock: RwLock<File>,
}

impl ResumeLock {
    /// Create a new lock handle for the given path
    ///
    /// This creates/opens the lock file but does NOT acquire the lock.
    /// Call [`write()`](Self::write) or [`try_write()`](Self::try_write)
    /// to acquire the lock.
    pub fn new(path: &Path) -> Result<Self, ResumeError> {
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

        Ok(Self {
            lock: RwLock::new(file),
        })
    }

    /// Acquire an exclusive write lock, blocking until available
    ///
    /// Returns a guard that must be held for the duration of protected work.
    /// The lock is released when the guard is dropped.
    pub fn write(&mut self) -> Result<RwLockWriteGuard<'_, File>, ResumeError> {
        self.lock
            .write()
            .map_err(|e| ResumeError::LockError(format!("Failed to acquire lock: {e}")))
    }

    /// Try to acquire an exclusive write lock without blocking
    ///
    /// Returns a guard if successful, or an error if the lock is held.
    /// The lock is released when the guard is dropped.
    pub fn try_write(&mut self) -> Result<RwLockWriteGuard<'_, File>, ResumeError> {
        self.lock
            .try_write()
            .map_err(|e| ResumeError::LockError(format!("Failed to acquire lock: {e}")))
    }

}
