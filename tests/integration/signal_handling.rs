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
