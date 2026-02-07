use std::time::Duration;

use trading_data_downloader::shutdown::ShutdownCoordinator;

#[tokio::test]
async fn shutdown_notifies_waiters() {
    let shutdown = ShutdownCoordinator::shared();
    let waiter = {
        let handle = shutdown.clone();
        tokio::spawn(async move {
            handle.wait_for_shutdown().await;
            true
        })
    };

    // Give the task time to start waiting
    tokio::time::sleep(Duration::from_millis(50)).await;
    shutdown.request_shutdown();

    let result = tokio::time::timeout(Duration::from_secs(1), waiter).await;
    assert!(result.is_ok());
}

/// P1-6: Test that the TOCTOU race in wait_for_shutdown is fixed.
/// Calls request_shutdown() immediately before wait_for_shutdown() to verify
/// no deadlock occurs when shutdown is requested between the check and the await.
#[tokio::test]
async fn shutdown_race_condition_no_deadlock() {
    let shutdown = ShutdownCoordinator::shared();

    // Request shutdown BEFORE spawning the waiter - this exercises the
    // race-free pattern where notified() is created before the flag check
    shutdown.request_shutdown();

    let handle = shutdown.clone();
    let waiter = tokio::spawn(async move {
        handle.wait_for_shutdown().await;
        true
    });

    // Should complete immediately since shutdown was already requested
    let result = tokio::time::timeout(Duration::from_secs(1), waiter).await;
    assert!(result.is_ok(), "wait_for_shutdown() deadlocked despite shutdown already requested");
}

/// P1-6: Test concurrent shutdown request and wait - no missed notifications.
/// Multiple tasks call wait_for_shutdown() concurrently while another task
/// requests shutdown. All waiters should be notified.
#[tokio::test]
async fn shutdown_concurrent_waiters_all_notified() {
    let shutdown = ShutdownCoordinator::shared();

    let mut waiters = Vec::new();
    for _ in 0..10 {
        let handle = shutdown.clone();
        waiters.push(tokio::spawn(async move {
            handle.wait_for_shutdown().await;
        }));
    }

    // Small delay to let all tasks start waiting
    tokio::time::sleep(Duration::from_millis(10)).await;

    // Request shutdown once - all waiters should be notified
    shutdown.request_shutdown();

    for waiter in waiters {
        let result = tokio::time::timeout(Duration::from_secs(1), waiter).await;
        assert!(result.is_ok(), "A waiter was not notified of shutdown");
    }
}

/// P1-6: Verify wait_for_shutdown returns immediately when called after shutdown.
#[tokio::test]
async fn shutdown_wait_returns_immediately_when_already_set() {
    let shutdown = ShutdownCoordinator::shared();
    shutdown.request_shutdown();

    // Should return immediately without any delay
    let start = tokio::time::Instant::now();
    shutdown.wait_for_shutdown().await;
    let elapsed = start.elapsed();

    assert!(elapsed < Duration::from_millis(10), "wait_for_shutdown took too long: {:?}", elapsed);
}

/// Verify that aggTrades shutdown checkpoints use cursor format (not TimeWindow).
/// This ensures resume after Ctrl+C uses agg_trade_id for precise resume.
#[tokio::test]
async fn aggtrades_shutdown_saves_cursor_checkpoint() {
    use trading_data_downloader::resume::checkpoint::{Checkpoint, CheckpointType};
    use trading_data_downloader::resume::state::ResumeState;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("aggtrades_shutdown.json");

    // Simulate what save_shutdown_checkpoint does for aggTrades with a known trade ID
    let trade_id: i64 = 987654;
    let checkpoint = Checkpoint::cursor(
        format!("aggtrade_id:{}", trade_id),
        500,
        0,
    );

    let mut state = ResumeState::new(
        "BINANCE:BTC/USDT:USDT".to_string(),
        "BTCUSDT".to_string(),
        None,
        "aggtrades".to_string(),
    );
    state.add_checkpoint(checkpoint);
    state.save(&path).unwrap();

    // Load and verify it's a Cursor checkpoint with aggtrade_id prefix
    let loaded = ResumeState::load(&path).unwrap();
    let cp = &loaded.checkpoints()[0];
    match cp.checkpoint_type() {
        CheckpointType::Cursor { cursor } => {
            assert!(cursor.starts_with("aggtrade_id:"), "Expected aggtrade_id cursor, got: {cursor}");
            let id_str = cursor.strip_prefix("aggtrade_id:").unwrap();
            assert_eq!(id_str.parse::<i64>().unwrap(), trade_id);
        }
        other => panic!("Expected Cursor checkpoint for aggTrades shutdown, got: {other:?}"),
    }
}

/// P1-7: Test async checkpoint save via spawn_blocking
#[tokio::test]
async fn checkpoint_save_async_no_runtime_block() {
    use trading_data_downloader::resume::checkpoint::Checkpoint;
    use trading_data_downloader::resume::state::ResumeState;
    use tempfile::TempDir;

    let temp_dir = TempDir::new().unwrap();
    let path = temp_dir.path().join("test_checkpoint.json");

    // Create and save a state using spawn_blocking (simulating what executor does)
    let path_clone = path.clone();
    let result = tokio::task::spawn_blocking(move || {
        let mut state = ResumeState::new(
            "BINANCE:BTC/USDT:USDT".to_string(),
            "BTCUSDT".to_string(),
            Some("1m".to_string()),
            "bars".to_string(),
        );

        let checkpoint = Checkpoint::time_window(1000, 2000, 100, 0);
        state.add_checkpoint(checkpoint);
        state.save(&path_clone)
    })
    .await;

    assert!(result.is_ok(), "spawn_blocking join failed");
    assert!(result.unwrap().is_ok(), "checkpoint save failed");
    assert!(path.exists(), "checkpoint file was not created");

    // Verify the state can be loaded back via spawn_blocking
    let path_clone = path.clone();
    let load_result = tokio::task::spawn_blocking(move || ResumeState::load(&path_clone)).await;
    assert!(load_result.is_ok(), "spawn_blocking join failed on load");
    let state = load_result.unwrap().unwrap();
    assert_eq!(state.checkpoints().len(), 1);
}
