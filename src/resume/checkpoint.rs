//! Checkpoint types for resume capability
//!
//! Implements checkpoint semantics according to FR-036

use serde::{Deserialize, Serialize};

/// A checkpoint representing completed work
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    checkpoint_type: CheckpointType,
    record_count: u64,
    byte_count: u64,
    completed_at: i64,
}

impl Checkpoint {
    /// Create a time window checkpoint (end-exclusive semantics)
    pub fn time_window(start_time: i64, end_time: i64, record_count: u64, byte_count: u64) -> Self {
        Self {
            checkpoint_type: CheckpointType::TimeWindow { start_time, end_time },
            record_count,
            byte_count,
            completed_at: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Create an archive file checkpoint
    pub fn archive_file(
        cursor: String,
        record_count: u64,
        byte_count: u64,
        checksum: Option<String>,
    ) -> Self {
        Self {
            checkpoint_type: CheckpointType::ArchiveFile { cursor, checksum },
            record_count,
            byte_count,
            completed_at: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Create a cursor-based checkpoint
    pub fn cursor(cursor: String, record_count: u64, byte_count: u64) -> Self {
        Self {
            checkpoint_type: CheckpointType::Cursor { cursor },
            record_count,
            byte_count,
            completed_at: chrono::Utc::now().timestamp_millis(),
        }
    }

    /// Get the checkpoint type
    pub fn checkpoint_type(&self) -> &CheckpointType {
        &self.checkpoint_type
    }

    /// Get the record count
    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    /// Get the byte count
    pub fn byte_count(&self) -> u64 {
        self.byte_count
    }

    /// Get the completion timestamp
    pub fn completed_at(&self) -> i64 {
        self.completed_at
    }
}

/// Type of checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "checkpoint_type", rename_all = "snake_case")]
pub enum CheckpointType {
    /// Time window checkpoint with end-exclusive semantics: [start_time, end_time)
    TimeWindow {
        /// Start time (inclusive) in Unix milliseconds
        start_time: i64,
        /// End time (exclusive) in Unix milliseconds
        end_time: i64,
    },
    /// Archive file checkpoint
    ArchiveFile {
        /// File identifier/cursor
        cursor: String,
        /// Optional SHA-256 checksum
        #[serde(skip_serializing_if = "Option::is_none")]
        checksum: Option<String>,
    },
    /// Cursor-based checkpoint
    Cursor {
        /// Cursor value (e.g., "lastTradeId:12345678")
        cursor: String,
    },
}
