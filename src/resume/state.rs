//! Resume state persistence and management (T170)
//!
//! Implements atomic file writes (FR-039) and schema versioning (FR-038)

use super::checkpoint::{Checkpoint, CheckpointType};
use fd_lock::RwLock;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use tracing::{debug, info, warn};

/// Current resume state schema version
const SCHEMA_VERSION: &str = "1.1.0";

/// Previous schema version that can be migrated
const SCHEMA_VERSION_1_0_0: &str = "1.0.0";

/// Maximum allowed state file size (10 MB) to prevent memory exhaustion
pub const MAX_STATE_FILE_SIZE: u64 = 10 * 1024 * 1024;

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

    /// Add a checkpoint and update metadata (T170)
    ///
    /// Prunes old checkpoints to prevent unbounded growth (H1).
    /// Only the most recent 5 checkpoints are retained to bound
    /// state file size below MAX_STATE_FILE_SIZE.
    pub fn add_checkpoint(&mut self, checkpoint: Checkpoint) {
        self.metadata.total_checkpoints += 1;
        self.metadata.total_records += checkpoint.record_count();
        self.metadata.total_bytes += checkpoint.byte_count();

        debug!(
            checkpoint_type = ?checkpoint.checkpoint_type(),
            total_checkpoints = self.metadata.total_checkpoints,
            total_records = self.metadata.total_records,
            "Adding checkpoint to resume state"
        );

        self.checkpoints.push(checkpoint);

        // Prune old checkpoints to prevent unbounded growth (H1)
        // Keep only the last 5 checkpoints - only the most recent is needed for resume
        const MAX_CHECKPOINTS: usize = 5;
        if self.checkpoints.len() > MAX_CHECKPOINTS {
            let drain_count = self.checkpoints.len() - MAX_CHECKPOINTS;
            self.checkpoints.drain(0..drain_count);
            debug!(
                retained_checkpoints = self.checkpoints.len(),
                "Pruned old checkpoints to prevent unbounded growth"
            );
        }

        self.updated_at = chrono::Utc::now().timestamp_millis();
    }

    /// Validate schema version (accepts current version only)
    pub fn validate_schema_version(&self) -> Result<(), ResumeError> {
        if self.schema_version != SCHEMA_VERSION {
            return Err(ResumeError::SchemaVersionMismatch {
                expected: SCHEMA_VERSION.to_string(),
                found: self.schema_version.clone(),
            });
        }
        Ok(())
    }

    /// Migrate state from schema version 1.0.0 to 1.1.0
    ///
    /// For aggtrades data type: converts TimeWindow checkpoints to Cursor checkpoints
    /// with format "timestamp:{end_time}" so the new resume logic can handle them.
    /// For other data types (bars, funding): no changes needed.
    fn migrate_from_1_0_0(&mut self) {
        if self.data_type == "aggtrades" {
            let migrated: Vec<Checkpoint> = self
                .checkpoints
                .iter()
                .map(|cp| {
                    if let CheckpointType::TimeWindow { end_time, .. } = cp.checkpoint_type() {
                        Checkpoint::cursor(
                            format!("timestamp:{end_time}"),
                            cp.record_count(),
                            cp.byte_count(),
                        )
                    } else {
                        cp.clone()
                    }
                })
                .collect();
            self.checkpoints = migrated;
            info!("Migrated aggtrades checkpoints from TimeWindow to Cursor format");
        }
        self.schema_version = SCHEMA_VERSION.to_string();
        info!(
            old_version = SCHEMA_VERSION_1_0_0,
            new_version = SCHEMA_VERSION,
            "Migrated resume state schema"
        );
    }

    /// Save state to file with atomic writes and file locking (T170)
    ///
    /// Uses tempfile::NamedTempFile for atomic writes (FR-039)
    /// Uses fd-lock for concurrency safety (FR-037)
    pub fn save(&self, path: &Path) -> Result<(), ResumeError> {
        debug!(
            path = %path.display(),
            checkpoints = self.checkpoints.len(),
            "Saving resume state"
        );

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
            .map_err(|e| ResumeError::LockError(format!("Failed to create lock file: {e}")))?;

        // Acquire exclusive lock for writing
        debug!("Acquiring write lock for resume state");
        let mut lock = RwLock::new(lock_file);
        let _guard = lock
            .write()
            .map_err(|e| ResumeError::LockError(format!("Failed to acquire write lock: {e}")))?;

        // Use NamedTempFile for atomic write
        // tempfile::NamedTempFile automatically handles creation in same directory
        let parent_dir = path.parent().unwrap_or_else(|| Path::new("."));
        let mut temp_file = tempfile::NamedTempFile::new_in(parent_dir)
            .map_err(|e| ResumeError::IoError(format!("Failed to create temp file: {e}")))?;

        // Write JSON to temp file
        temp_file
            .write_all(json.as_bytes())
            .map_err(|e| ResumeError::IoError(format!("Failed to write to temp file: {e}")))?;

        // Flush buffer to OS and sync to disk for durability before atomic rename
        temp_file
            .flush()
            .map_err(|e| ResumeError::IoError(format!("Failed to flush temp file: {e}")))?;
        temp_file
            .as_file()
            .sync_all()
            .map_err(|e| ResumeError::IoError(format!("Failed to sync temp file: {e}")))?;

        // Atomically replace the target file (persists temp file to target path)
        temp_file
            .persist(path)
            .map_err(|e| ResumeError::IoError(format!("Failed to persist temp file: {e}")))?;

        // Fsync parent directory to ensure the rename is durable
        if let Some(parent) = path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }

        info!(
            path = %path.display(),
            checkpoints = self.checkpoints.len(),
            total_records = self.metadata.total_records,
            "Resume state saved successfully"
        );

        // Lock is automatically released when it goes out of scope
        Ok(())
    }

    /// Load state from file with locking (T170)
    ///
    /// Uses fd-lock for concurrency safety (FR-037)
    pub fn load(path: &Path) -> Result<Self, ResumeError> {
        debug!(
            path = %path.display(),
            "Loading resume state"
        );

        // Create/open lock file for coordinating concurrent access
        let lock_path = path.with_extension("lock");
        let lock_file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .map_err(|e| ResumeError::LockError(format!("Failed to create lock file: {e}")))?;

        // Acquire shared lock for reading
        debug!("Acquiring read lock for resume state");
        let lock = RwLock::new(lock_file);
        let _guard = lock
            .read()
            .map_err(|e| ResumeError::LockError(format!("Failed to acquire read lock: {e}")))?;

        // Check file size before reading to prevent memory exhaustion
        let metadata = std::fs::metadata(path).map_err(|e| ResumeError::IoError(e.to_string()))?;
        if metadata.len() > MAX_STATE_FILE_SIZE {
            return Err(ResumeError::StateTooLarge {
                size: metadata.len(),
                max: MAX_STATE_FILE_SIZE,
            });
        }

        // Read the state file while holding the lock
        let contents =
            std::fs::read_to_string(path).map_err(|e| ResumeError::IoError(e.to_string()))?;

        let mut state: ResumeState = serde_json::from_str(&contents).map_err(|e| {
            warn!(error = %e, "Failed to deserialize resume state");
            ResumeError::DeserializationError(e.to_string())
        })?;

        // Handle schema version: current passes through, 1.0.0 gets migrated, others fail
        if state.schema_version == SCHEMA_VERSION {
            info!(
                checkpoints = state.checkpoints.len(),
                total_records = state.metadata.total_records,
                schema_version = %state.schema_version,
                "Resume state loaded successfully"
            );
        } else if state.schema_version == SCHEMA_VERSION_1_0_0 {
            info!(
                found_version = %state.schema_version,
                target_version = SCHEMA_VERSION,
                "Migrating resume state from 1.0.0 to 1.1.0"
            );
            state.migrate_from_1_0_0();
        } else {
            warn!(
                found_version = %state.schema_version,
                expected_version = SCHEMA_VERSION,
                "Resume state schema version mismatch"
            );
            return Err(ResumeError::SchemaVersionMismatch {
                expected: SCHEMA_VERSION.to_string(),
                found: state.schema_version.clone(),
            });
        }

        // Lock is automatically released when it goes out of scope
        Ok(state)
    }
}

/// Metadata about resume state
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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

    /// State file too large
    #[error("state file too large: {size} bytes (max: {max} bytes)")]
    StateTooLarge {
        /// Actual file size
        size: u64,
        /// Maximum allowed size
        max: u64,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resume::checkpoint::{Checkpoint, CheckpointType};

    /// Helper: create a v1.0.0 aggtrades state with TimeWindow checkpoints and save it
    fn create_v1_0_0_aggtrades_state(path: &std::path::Path) {
        let mut state = ResumeState::new(
            "test_id".to_string(),
            "BTCUSDT".to_string(),
            None,
            "aggtrades".to_string(),
        );
        state.schema_version = "1.0.0".to_string();
        state.add_checkpoint(Checkpoint::time_window(1000, 2000, 100, 0));
        state.add_checkpoint(Checkpoint::time_window(1000, 3000, 200, 0));
        // Override schema version back since add_checkpoint doesn't change it
        state.schema_version = "1.0.0".to_string();
        state.save(path).unwrap();
    }

    /// Helper: create a v1.0.0 bars state with TimeWindow checkpoints
    fn create_v1_0_0_bars_state(path: &std::path::Path) {
        let mut state = ResumeState::new(
            "test_id".to_string(),
            "BTCUSDT".to_string(),
            Some("1h".to_string()),
            "bars".to_string(),
        );
        state.schema_version = "1.0.0".to_string();
        state.add_checkpoint(Checkpoint::time_window(1000, 5000, 50, 0));
        state.schema_version = "1.0.0".to_string();
        state.save(path).unwrap();
    }

    #[test]
    fn test_schema_migration_aggtrades_1_0_0_to_1_1_0() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("aggtrades_state.json");
        create_v1_0_0_aggtrades_state(&path);

        // Load should trigger migration
        let state = ResumeState::load(&path).unwrap();
        assert_eq!(state.schema_version, "1.1.0");
        assert_eq!(state.data_type, "aggtrades");

        // Checkpoints should be converted to Cursor type
        for cp in state.checkpoints() {
            match cp.checkpoint_type() {
                CheckpointType::Cursor { cursor } => {
                    assert!(cursor.starts_with("timestamp:"), "cursor should have timestamp prefix: {cursor}");
                }
                other => panic!("Expected Cursor checkpoint, got {:?}", other),
            }
        }

        // Verify the last checkpoint has the correct timestamp
        let last = state.checkpoints().last().unwrap();
        if let CheckpointType::Cursor { cursor } = last.checkpoint_type() {
            assert_eq!(cursor, "timestamp:3000");
        }
    }

    #[test]
    fn test_schema_migration_bars_is_noop() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("bars_state.json");
        create_v1_0_0_bars_state(&path);

        let state = ResumeState::load(&path).unwrap();
        assert_eq!(state.schema_version, "1.1.0");
        assert_eq!(state.data_type, "bars");

        // Bars checkpoints should remain as TimeWindow (migration only converts aggtrades)
        for cp in state.checkpoints() {
            match cp.checkpoint_type() {
                CheckpointType::TimeWindow { .. } => {}
                other => panic!("Expected TimeWindow checkpoint for bars, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_schema_migration_funding_is_noop() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("funding_state.json");

        let mut state = ResumeState::new(
            "test_id".to_string(),
            "BTCUSDT".to_string(),
            None,
            "funding".to_string(),
        );
        state.schema_version = "1.0.0".to_string();
        state.add_checkpoint(Checkpoint::time_window(1000, 4000, 10, 0));
        state.schema_version = "1.0.0".to_string();
        state.save(&path).unwrap();

        let state = ResumeState::load(&path).unwrap();
        assert_eq!(state.schema_version, "1.1.0");

        for cp in state.checkpoints() {
            match cp.checkpoint_type() {
                CheckpointType::TimeWindow { .. } => {}
                other => panic!("Expected TimeWindow checkpoint for funding, got {:?}", other),
            }
        }
    }

    #[test]
    fn test_unknown_schema_version_rejected() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("bad_state.json");

        let mut state = ResumeState::new(
            "test_id".to_string(),
            "BTCUSDT".to_string(),
            None,
            "aggtrades".to_string(),
        );
        state.schema_version = "2.0.0".to_string();
        state.save(&path).unwrap();

        let result = ResumeState::load(&path);
        assert!(result.is_err());
        match result.unwrap_err() {
            ResumeError::SchemaVersionMismatch { expected, found } => {
                assert_eq!(expected, "1.1.0");
                assert_eq!(found, "2.0.0");
            }
            other => panic!("Expected SchemaVersionMismatch, got {:?}", other),
        }
    }

    #[test]
    fn test_current_version_loads_without_migration() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("current_state.json");

        let mut state = ResumeState::new(
            "test_id".to_string(),
            "BTCUSDT".to_string(),
            None,
            "aggtrades".to_string(),
        );
        state.add_checkpoint(Checkpoint::cursor(
            "aggtrade_id:12345".to_string(),
            100,
            0,
        ));
        state.save(&path).unwrap();

        let loaded = ResumeState::load(&path).unwrap();
        assert_eq!(loaded.schema_version, "1.1.0");

        let last = loaded.checkpoints().last().unwrap();
        if let CheckpointType::Cursor { cursor } = last.checkpoint_type() {
            assert_eq!(cursor, "aggtrade_id:12345");
        } else {
            panic!("Expected Cursor checkpoint");
        }
    }

    #[test]
    fn test_cursor_parsing_aggtrade_id() {
        // Test parsing "aggtrade_id:12345"
        let cursor = "aggtrade_id:12345";
        let id_str = cursor.strip_prefix("aggtrade_id:").unwrap();
        let id: i64 = id_str.parse().unwrap();
        assert_eq!(id, 12345);
    }

    #[test]
    fn test_cursor_parsing_legacy_timestamp() {
        // Test parsing "timestamp:1234" returns the timestamp
        let cursor = "timestamp:1234";
        let ts_str = cursor.strip_prefix("timestamp:").unwrap();
        let ts: i64 = ts_str.parse().unwrap();
        assert_eq!(ts, 1234);

        // But strip_prefix("aggtrade_id:") should return None
        assert!(cursor.strip_prefix("aggtrade_id:").is_none());
    }

    #[test]
    fn test_cursor_parsing_invalid_format() {
        // Test that unrecognized cursor formats don't match either prefix
        let cursor = "unknown:value";
        assert!(cursor.strip_prefix("aggtrade_id:").is_none());
        assert!(cursor.strip_prefix("timestamp:").is_none());
    }
}
