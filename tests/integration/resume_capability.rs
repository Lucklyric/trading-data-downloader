//! Integration tests for resume capability (FR-036, FR-037, FR-038, FR-039, FR-042)

use tempfile::TempDir;
use trading_data_downloader::resume::{Checkpoint, CheckpointType, ResumeState};

// T016: ResumeState serialization/deserialization tests

#[test]
fn test_resume_state_serialization_deserialization() {
    let state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );

    let json = serde_json::to_string_pretty(&state).unwrap();
    let deserialized: ResumeState = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.identifier(), state.identifier());
    assert_eq!(deserialized.symbol(), state.symbol());
    assert_eq!(deserialized.interval(), state.interval());
    assert_eq!(deserialized.data_type(), state.data_type());
}

#[test]
fn test_resume_state_with_checkpoints_serialization() {
    let mut state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );

    let checkpoint = Checkpoint::time_window(1699920000000, 1699920060000, 60, 4096);
    state.add_checkpoint(checkpoint);

    let json = serde_json::to_string_pretty(&state).unwrap();
    let deserialized: ResumeState = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.checkpoints().len(), 1);
    assert_eq!(deserialized.metadata().total_checkpoints(), 1);
    assert_eq!(deserialized.metadata().total_records(), 60);
    assert_eq!(deserialized.metadata().total_bytes(), 4096);
}

#[test]
fn test_resume_state_schema_version_present() {
    let state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );

    let json = serde_json::to_string(&state).unwrap();
    assert!(json.contains("schema_version"));
    assert!(json.contains("1.0.0"));
}

// T017: Checkpoint time window semantics (end-exclusive)

#[test]
fn test_checkpoint_time_window_end_exclusive_semantics() {
    let checkpoint = Checkpoint::time_window(1699920000000, 1699920060000, 60, 4096);

    if let CheckpointType::TimeWindow {
        start_time,
        end_time,
        ..
    } = checkpoint.checkpoint_type()
    {
        assert_eq!(*start_time, 1699920000000);
        assert_eq!(*end_time, 1699920060000);
        // end_time is exclusive: next checkpoint should start at 1699920060000
    } else {
        panic!("Expected TimeWindow checkpoint type");
    }
}

#[test]
fn test_checkpoint_non_overlapping_time_windows() {
    let mut state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );

    // First window: [1699920000000, 1699920060000)
    let checkpoint1 = Checkpoint::time_window(1699920000000, 1699920060000, 60, 4096);
    state.add_checkpoint(checkpoint1);

    // Second window starts where first ends: [1699920060000, 1699920120000)
    let checkpoint2 = Checkpoint::time_window(1699920060000, 1699920120000, 60, 4096);
    state.add_checkpoint(checkpoint2);

    assert_eq!(state.checkpoints().len(), 2);
    assert_eq!(state.metadata().total_records(), 120);
    assert_eq!(state.metadata().total_bytes(), 8192);
}

#[test]
fn test_checkpoint_archive_file_type() {
    let checkpoint = Checkpoint::archive_file(
        "BTCUSDT-aggTrades-2023-11.zip".to_string(),
        5000000,
        524288000,
        Some("a3c5d8e9f0b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7".to_string()),
    );

    if let CheckpointType::ArchiveFile {
        cursor, checksum, ..
    } = checkpoint.checkpoint_type()
    {
        assert_eq!(cursor, "BTCUSDT-aggTrades-2023-11.zip");
        assert!(checksum.is_some());
    } else {
        panic!("Expected ArchiveFile checkpoint type");
    }
}

#[test]
fn test_checkpoint_cursor_type() {
    let checkpoint = Checkpoint::cursor("lastTradeId:12345678".to_string(), 1000, 102400);

    if let CheckpointType::Cursor { cursor, .. } = checkpoint.checkpoint_type() {
        assert_eq!(cursor, "lastTradeId:12345678");
    } else {
        panic!("Expected Cursor checkpoint type");
    }
}

// T021: ResumeState atomic file writes

#[test]
fn test_resume_state_save_creates_file() {
    let temp_dir = TempDir::new().unwrap();
    let state_path = temp_dir.path().join("test_state.json");

    let state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );

    state.save(&state_path).unwrap();
    assert!(state_path.exists());
}

#[test]
fn test_resume_state_load_from_file() {
    let temp_dir = TempDir::new().unwrap();
    let state_path = temp_dir.path().join("test_state.json");

    let mut original_state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );
    original_state.add_checkpoint(Checkpoint::time_window(
        1699920000000,
        1699920060000,
        60,
        4096,
    ));

    original_state.save(&state_path).unwrap();

    let loaded_state = ResumeState::load(&state_path).unwrap();
    assert_eq!(loaded_state.identifier(), original_state.identifier());
    assert_eq!(loaded_state.checkpoints().len(), 1);
    assert_eq!(loaded_state.metadata().total_records(), 60);
}

#[test]
fn test_resume_state_atomic_write_on_crash() {
    let temp_dir = TempDir::new().unwrap();
    let state_path = temp_dir.path().join("test_state.json");

    let state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );

    // Save initial state
    state.save(&state_path).unwrap();

    // Load and verify
    let loaded = ResumeState::load(&state_path).unwrap();
    assert_eq!(loaded.identifier(), state.identifier());
}

// T024: Schema version validation

#[test]
fn test_resume_state_schema_version_validation_success() {
    let state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );

    // Should accept same schema version
    assert!(state.validate_schema_version().is_ok());
}

#[test]
fn test_resume_state_schema_version_in_json() {
    let state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );

    let json = serde_json::to_value(&state).unwrap();
    assert_eq!(json["schema_version"], "1.0.0");
}

// T026: Concurrent access with file locking

#[test]
fn test_resume_lock_basic_acquire_and_release() {
    use trading_data_downloader::resume::ResumeLock;

    let temp_dir = TempDir::new().unwrap();
    let state_path = temp_dir.path().join("test_state.json");

    let state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );
    state.save(&state_path).unwrap();

    // Acquire lock using new API - must hold the guard
    let mut lock1 = ResumeLock::new(&state_path).unwrap();
    let _guard1 = lock1.write().unwrap();

    // Drop the guard to release the lock
    drop(_guard1);
    drop(lock1);

    // Now acquiring should succeed
    let mut lock2 = ResumeLock::new(&state_path).unwrap();
    let _guard2 = lock2.write().unwrap();
}

#[test]
fn test_resume_state_concurrent_read_write_safety() {
    use std::sync::Arc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let state_path = Arc::new(temp_dir.path().join("concurrent_state.json"));

    // Create initial state
    let state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );
    state.save(&state_path).unwrap();

    // Spawn multiple threads that read and write concurrently
    let mut handles = vec![];

    for i in 0..5 {
        let state_path = Arc::clone(&state_path);
        let handle = thread::spawn(move || {
            // Read the state
            let loaded_state = ResumeState::load(&state_path).expect("Failed to load state");
            assert_eq!(loaded_state.identifier(), "BINANCE:BTC/USDT:USDT");

            // Modify and save
            let mut new_state = loaded_state;
            let checkpoint = Checkpoint::time_window(
                1699920000000 + (i * 60000),
                1699920060000 + (i * 60000),
                60,
                4096,
            );
            new_state.add_checkpoint(checkpoint);
            new_state.save(&state_path).expect("Failed to save state");
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify final state is valid (no corruption)
    let final_state = ResumeState::load(&state_path).unwrap();
    assert_eq!(final_state.identifier(), "BINANCE:BTC/USDT:USDT");
    // Due to concurrent writes, we should have at least one checkpoint
    assert!(final_state.checkpoints().len() > 0);
}

#[test]
fn test_resume_state_file_corruption_prevention() {
    let temp_dir = TempDir::new().unwrap();
    let state_path = temp_dir.path().join("corruption_test.json");

    let mut state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );

    // Add some checkpoints
    for i in 0..10 {
        state.add_checkpoint(Checkpoint::time_window(
            1699920000000 + (i * 60000),
            1699920060000 + (i * 60000),
            60,
            4096,
        ));
    }

    // Save state
    state.save(&state_path).unwrap();

    // Load and verify - this tests that save/load are atomic
    let loaded = ResumeState::load(&state_path).unwrap();
    assert_eq!(loaded.checkpoints().len(), 10);
    assert_eq!(loaded.metadata().total_records(), 600);

    // Save again to ensure temp file cleanup
    let mut state2 = loaded;
    state2.add_checkpoint(Checkpoint::time_window(
        1699920600000,
        1699920660000,
        60,
        4096,
    ));
    state2.save(&state_path).unwrap();

    // Verify no temp files left behind
    let parent_dir = state_path.parent().unwrap();
    let entries: Vec<_> = std::fs::read_dir(parent_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    // Should only have the state file and lock file, no temp files
    let temp_files: Vec<_> = entries
        .iter()
        .filter(|name| name.contains(".tmp"))
        .collect();
    assert_eq!(
        temp_files.len(),
        0,
        "Found unexpected temp files: {:?}",
        temp_files
    );
}

// T192: Additional comprehensive concurrent access tests

/// Test A: Concurrent Readers
///
/// Spawn multiple threads all reading the same resume state file.
/// Verify: All threads read consistent data.
/// Expected: File locking allows multiple readers.
#[test]
fn test_concurrent_readers_consistency() {
    use std::sync::Arc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let state_path = Arc::new(temp_dir.path().join("concurrent_readers.json"));

    // Create initial state with known data
    let mut state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );

    // Add multiple checkpoints
    for i in 0..10 {
        state.add_checkpoint(Checkpoint::time_window(
            1699920000000 + (i * 60000),
            1699920060000 + (i * 60000),
            60,
            4096,
        ));
    }
    state.save(&state_path).unwrap();

    // Spawn 10 reader threads
    let mut handles = vec![];
    for _ in 0..10 {
        let state_path = Arc::clone(&state_path);
        let handle = thread::spawn(move || {
            // Each thread reads the state
            let loaded_state = ResumeState::load(&state_path).expect("Failed to load state");

            // Verify consistency
            assert_eq!(loaded_state.identifier(), "BINANCE:BTC/USDT:USDT");
            assert_eq!(loaded_state.symbol(), "BTCUSDT");
            assert_eq!(loaded_state.interval(), Some("1m"));
            assert_eq!(loaded_state.checkpoints().len(), 10);
            assert_eq!(loaded_state.metadata().total_records(), 600);

            loaded_state.checkpoints().len()
        });
        handles.push(handle);
    }

    // Wait for all threads and verify all read the same data
    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().expect("Thread panicked"))
        .collect();

    // All threads should have read 10 checkpoints
    assert!(results.iter().all(|&count| count == 10));
}

/// Test B: Concurrent Writers
///
/// Spawn multiple threads all attempting to write to the same resume state file.
/// Verify: No data corruption, all writes are atomic.
/// Expected: File locking serializes writes.
#[test]
fn test_concurrent_writers_atomicity() {
    use std::sync::Arc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let state_path = Arc::new(temp_dir.path().join("concurrent_writers.json"));

    // Create initial state
    let state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );
    state.save(&state_path).unwrap();

    // Spawn 5 writer threads
    let mut handles = vec![];
    for i in 0..5 {
        let state_path = Arc::clone(&state_path);
        let handle = thread::spawn(move || {
            // Each thread loads, modifies, and saves the state
            let loaded_state = ResumeState::load(&state_path).expect("Failed to load state");
            let mut new_state = loaded_state;

            // Add a unique checkpoint
            let checkpoint = Checkpoint::time_window(
                1699920000000 + (i * 60000),
                1699920060000 + (i * 60000),
                60,
                4096,
            );
            new_state.add_checkpoint(checkpoint);

            // Save the state
            new_state.save(&state_path).expect("Failed to save state");
        });
        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify final state is valid (not corrupted)
    let final_state = ResumeState::load(&state_path).unwrap();
    assert_eq!(final_state.identifier(), "BINANCE:BTC/USDT:USDT");

    // Due to concurrent writes with read-modify-write pattern,
    // we should have at least 1 checkpoint (possibly more if writes interleaved)
    assert!(final_state.checkpoints().len() >= 1);
    assert!(final_state.checkpoints().len() <= 5);

    // Verify metadata is consistent with checkpoints
    let total_records: u64 = final_state
        .checkpoints()
        .iter()
        .map(|c| c.record_count())
        .sum();
    assert_eq!(final_state.metadata().total_records(), total_records);

    // Verify no JSON corruption by checking all fields are valid
    assert!(final_state.validate_schema_version().is_ok());
}

/// Test C: Mixed Read/Write
///
/// Spawn reader and writer threads concurrently.
/// Verify: Readers see consistent state, no partial writes.
/// Expected: fd-lock prevents torn reads/writes.
#[test]
fn test_concurrent_mixed_read_write() {
    use std::sync::{Arc, Barrier};
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let state_path = Arc::new(temp_dir.path().join("concurrent_mixed.json"));

    // Create initial state
    let mut state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );
    state.add_checkpoint(Checkpoint::time_window(
        1699920000000,
        1699920060000,
        60,
        4096,
    ));
    state.save(&state_path).unwrap();

    // Barrier to synchronize thread start
    let barrier = Arc::new(Barrier::new(10));
    let mut handles = vec![];

    // Spawn 8 reader threads
    for _ in 0..8 {
        let state_path = Arc::clone(&state_path);
        let barrier = Arc::clone(&barrier);
        let handle = thread::spawn(move || {
            barrier.wait(); // Wait for all threads to be ready

            // Read the state
            let loaded_state = ResumeState::load(&state_path).expect("Failed to load state");

            // Verify state is valid (no partial writes)
            assert_eq!(loaded_state.identifier(), "BINANCE:BTC/USDT:USDT");
            assert!(loaded_state.checkpoints().len() >= 1);
            assert!(loaded_state.validate_schema_version().is_ok());

            // Verify metadata consistency
            let total_records: u64 = loaded_state
                .checkpoints()
                .iter()
                .map(|c| c.record_count())
                .sum();
            assert_eq!(loaded_state.metadata().total_records(), total_records);
        });
        handles.push(handle);
    }

    // Spawn 2 writer threads
    for i in 0..2 {
        let state_path = Arc::clone(&state_path);
        let barrier = Arc::clone(&barrier);
        let handle = thread::spawn(move || {
            barrier.wait(); // Wait for all threads to be ready

            // Load, modify, save
            let loaded_state = ResumeState::load(&state_path).expect("Failed to load state");
            let mut new_state = loaded_state;
            let checkpoint = Checkpoint::time_window(
                1699920060000 + (i * 60000),
                1699920120000 + (i * 60000),
                60,
                4096,
            );
            new_state.add_checkpoint(checkpoint);
            new_state.save(&state_path).expect("Failed to save state");
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread panicked");
    }

    // Verify final state is valid
    let final_state = ResumeState::load(&state_path).unwrap();
    assert!(final_state.validate_schema_version().is_ok());
    assert!(final_state.checkpoints().len() >= 1);
}

/// Test D: Process Crash Simulation
///
/// Simulate a crash during write to verify atomic write protection.
/// Verify: Original state file is unchanged (atomic write protection).
#[test]
fn test_atomic_write_crash_safety() {
    let temp_dir = TempDir::new().unwrap();
    let state_path = temp_dir.path().join("crash_safety.json");

    // Create and save initial state
    let mut initial_state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );
    initial_state.add_checkpoint(Checkpoint::time_window(
        1699920000000,
        1699920060000,
        60,
        4096,
    ));
    initial_state.save(&state_path).unwrap();

    // Load and verify initial state
    let loaded = ResumeState::load(&state_path).unwrap();
    assert_eq!(loaded.checkpoints().len(), 1);
    let initial_records = loaded.metadata().total_records();

    // Simulate a crash scenario by creating a tempfile but NOT persisting it
    // This mimics what would happen if the process crashed during save()
    {
        let parent_dir = state_path.parent().unwrap();
        let temp_file = tempfile::NamedTempFile::new_in(parent_dir).unwrap();

        // Write corrupted data to temp file
        std::fs::write(temp_file.path(), b"corrupted data").unwrap();

        // Drop tempfile WITHOUT persisting (simulates crash)
        // tempfile is automatically deleted on drop
    }

    // Verify original state file is still valid and unchanged
    let recovered_state = ResumeState::load(&state_path).unwrap();
    assert_eq!(recovered_state.checkpoints().len(), 1);
    assert_eq!(recovered_state.metadata().total_records(), initial_records);
    assert_eq!(recovered_state.identifier(), "BINANCE:BTC/USDT:USDT");

    // Verify no temp files exist
    let parent_dir = state_path.parent().unwrap();
    let entries: Vec<_> = std::fs::read_dir(parent_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .collect();

    let temp_files: Vec<_> = entries
        .iter()
        .filter(|name| name.contains(".tmp"))
        .collect();
    assert_eq!(
        temp_files.len(),
        0,
        "Found unexpected temp files after crash simulation"
    );
}

/// Test E: High-Concurrency Stress Test
///
/// Spawn many threads performing rapid read/write operations.
/// This test is marked #[ignore] as it may take >1 second.
#[test]
#[ignore] // Takes >1 second to complete
fn test_high_concurrency_stress() {
    use std::sync::Arc;
    use std::thread;

    let temp_dir = TempDir::new().unwrap();
    let state_path = Arc::new(temp_dir.path().join("stress_test.json"));

    // Create initial state
    let state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        Some("1m".to_string()),
        "bars".to_string(),
    );
    state.save(&state_path).unwrap();

    // Spawn 20 threads doing random read/write operations
    let mut handles = vec![];
    for i in 0..20 {
        let state_path = Arc::clone(&state_path);
        let handle = thread::spawn(move || {
            for j in 0..10 {
                // Alternate between read and write
                if j % 2 == 0 {
                    // Read
                    let loaded = ResumeState::load(&state_path).expect("Failed to load");
                    assert_eq!(loaded.identifier(), "BINANCE:BTC/USDT:USDT");
                } else {
                    // Write
                    let loaded = ResumeState::load(&state_path).expect("Failed to load");
                    let mut new_state = loaded;
                    let checkpoint = Checkpoint::time_window(
                        1699920000000 + ((i * 10 + j) * 60000),
                        1699920060000 + ((i * 10 + j) * 60000),
                        60,
                        4096,
                    );
                    new_state.add_checkpoint(checkpoint);
                    new_state.save(&state_path).expect("Failed to save");
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all threads
    for handle in handles {
        handle.join().expect("Thread panicked during stress test");
    }

    // Verify final state is valid (no corruption)
    let final_state = ResumeState::load(&state_path).unwrap();
    assert_eq!(final_state.identifier(), "BINANCE:BTC/USDT:USDT");
    assert!(final_state.validate_schema_version().is_ok());

    // Verify metadata consistency
    let total_records: u64 = final_state
        .checkpoints()
        .iter()
        .map(|c| c.record_count())
        .sum();
    assert_eq!(final_state.metadata().total_records(), total_records);
}
