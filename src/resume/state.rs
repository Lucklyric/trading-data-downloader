//! Resume state persistence and management
//!
//! Implements atomic file writes (FR-039) and schema versioning (FR-038)

use super::checkpoint::Checkpoint;
use fd_lock::RwLock;
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;

/// Current resume state schema version
const SCHEMA_VERSION: &str = "1.0.0";

/// Resume state for a download job
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResumeState {
    schema_version: String,
    identifier: String,
    symbol: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    interval: Option<String>,
    data_type: String,
    checkpoints: Vec<Checkpoint>,
    metadata: StateMetadata,
    created_at: i64,
    updated_at: i64,
}

impl ResumeState {
    /// Create a new resume state
    pub fn new(
        identifier: String,
        symbol: String,
        interval: Option<String>,
        data_type: String,
    ) -> Self {
        let now = chrono::Utc::now().timestamp_millis();
        Self {
            schema_version: SCHEMA_VERSION.to_string(),
            identifier,
            symbol,
            interval,
            data_type,
            checkpoints: Vec::new(),
            metadata: StateMetadata::default(),
            created_at: now,
            updated_at: now,
        }
    }

    /// Get the identifier
    pub fn identifier(&self) -> &str {
        &self.identifier
    }

    /// Get the symbol
    pub fn symbol(&self) -> &str {
        &self.symbol
    }

    /// Get the interval
    pub fn interval(&self) -> Option<&str> {
        self.interval.as_deref()
    }

    /// Get the data type
    pub fn data_type(&self) -> &str {
        &self.data_type
    }

    /// Get all checkpoints
    pub fn checkpoints(&self) -> &[Checkpoint] {
        &self.checkpoints
    }

    /// Get the metadata
    pub fn metadata(&self) -> &StateMetadata {
        &self.metadata
    }

    /// Add a checkpoint and update metadata
    pub fn add_checkpoint(&mut self, checkpoint: Checkpoint) {
        self.metadata.total_checkpoints += 1;
        self.metadata.total_records += checkpoint.record_count();
        self.metadata.total_bytes += checkpoint.byte_count();
        self.checkpoints.push(checkpoint);
        self.updated_at = chrono::Utc::now().timestamp_millis();
    }

    /// Validate schema version
    pub fn validate_schema_version(&self) -> Result<(), ResumeError> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(ResumeError::SchemaVersionMismatch {
                expected: SCHEMA_VERSION.to_string(),
                found: self.schema_version.clone(),
            });
        }
        Ok(())
    }

    /// Save state to file with atomic writes and file locking
    ///
    /// Uses tempfile::NamedTempFile for atomic writes (FR-039)
    /// Uses fd-lock for concurrency safety (FR-037)
    pub fn save(&self, path: &Path) -> Result<(), ResumeError> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| ResumeError::IoError(e.to_string()))?;
        }

        // Serialize to JSON
        let json = serde_json::to_string_pretty(self)
            .map_err(|e| ResumeError::SerializationError(e.to_string()))?;

        // Create a lock file for coordinating concurrent access
        let lock_path = path.with_extension("lock");
        let lock_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| ResumeError::LockError(format!("Failed to create lock file: {}", e)))?;

        // Acquire exclusive lock for writing
        let mut lock = RwLock::new(lock_file);
        let _guard = lock.write()
            .map_err(|e| ResumeError::LockError(format!("Failed to acquire write lock: {}", e)))?;

        // Use NamedTempFile for atomic write
        // tempfile::NamedTempFile automatically handles creation in same directory
        let parent_dir = path.parent().unwrap_or_else(|| Path::new("."));
        let mut temp_file = tempfile::NamedTempFile::new_in(parent_dir)
            .map_err(|e| ResumeError::IoError(format!("Failed to create temp file: {}", e)))?;

        // Write JSON to temp file
        temp_file
            .write_all(json.as_bytes())
            .map_err(|e| ResumeError::IoError(format!("Failed to write to temp file: {}", e)))?;

        // Sync to ensure data is on disk before rename
        temp_file
            .flush()
            .map_err(|e| ResumeError::IoError(format!("Failed to flush temp file: {}", e)))?;

        // Atomically replace the target file (persists temp file to target path)
        temp_file
            .persist(path)
            .map_err(|e| ResumeError::IoError(format!("Failed to persist temp file: {}", e)))?;

        // Lock is automatically released when it goes out of scope
        Ok(())
    }

    /// Load state from file with locking
    ///
    /// Uses fd-lock for concurrency safety (FR-037)
    pub fn load(path: &Path) -> Result<Self, ResumeError> {
        // Create/open lock file for coordinating concurrent access
        let lock_path = path.with_extension("lock");
        let lock_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| ResumeError::LockError(format!("Failed to create lock file: {}", e)))?;

        // Acquire shared lock for reading
        let mut lock = RwLock::new(lock_file);
        let _guard = lock.read()
            .map_err(|e| ResumeError::LockError(format!("Failed to acquire read lock: {}", e)))?;

        // Read the state file while holding the lock
        let contents =
            std::fs::read_to_string(path).map_err(|e| ResumeError::IoError(e.to_string()))?;

        let state: ResumeState = serde_json::from_str(&contents)
            .map_err(|e| ResumeError::DeserializationError(e.to_string()))?;

        // Validate schema version
        state.validate_schema_version()?;

        // Lock is automatically released when it goes out of scope
        Ok(state)
    }
}

/// Metadata about resume state
#[derive(Debug, Clone, Serialize, Deserialize)]
#[derive(Default)]
pub struct StateMetadata {
    total_checkpoints: u64,
    total_records: u64,
    total_bytes: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_etag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_content_length: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lock_pid: Option<u32>,
}


impl StateMetadata {
    /// Get total checkpoints
    pub fn total_checkpoints(&self) -> u64 {
        self.total_checkpoints
    }

    /// Get total records
    pub fn total_records(&self) -> u64 {
        self.total_records
    }

    /// Get total bytes
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Get last ETag
    pub fn last_etag(&self) -> Option<&str> {
        self.last_etag.as_deref()
    }

    /// Get last content length
    pub fn last_content_length(&self) -> Option<u64> {
        self.last_content_length
    }

    /// Get lock PID
    pub fn lock_pid(&self) -> Option<u32> {
        self.lock_pid
    }
}

/// Errors related to resume state
#[derive(Debug, thiserror::Error)]
pub enum ResumeError {
    /// Schema version mismatch
    #[error("schema version mismatch: expected {expected}, found {found}")]
    SchemaVersionMismatch {
        /// Expected schema version
        expected: String,
        /// Found schema version
        found: String,
    },

    /// IO error
    #[error("IO error: {0}")]
    IoError(String),

    /// Serialization error
    #[error("serialization error: {0}")]
    SerializationError(String),

    /// Deserialization error
    #[error("deserialization error: {0}")]
    DeserializationError(String),

    /// Lock error
    #[error("lock error: {0}")]
    LockError(String),
}
