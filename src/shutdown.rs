//! Graceful shutdown coordination utilities.
//!
//! Provides a lightweight [`ShutdownCoordinator`] that can be shared across
//! tasks to detect Ctrl+C and request early termination without corrupting
//! resume state or partially written files.

use once_cell::sync::OnceCell;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

/// Shared handle to a shutdown coordinator.
pub type SharedShutdown = Arc<ShutdownCoordinator>;

static GLOBAL_SHUTDOWN: OnceCell<SharedShutdown> = OnceCell::new();

/// Register a global shutdown handle so subsystems can discover it lazily.
pub fn set_global_shutdown(handle: SharedShutdown) {
    let _ = GLOBAL_SHUTDOWN.set(handle);
}

/// Retrieve the registered global shutdown handle, if available.
pub fn get_global_shutdown() -> Option<SharedShutdown> {
    GLOBAL_SHUTDOWN.get().cloned()
}

/// Coordinates graceful shutdown across async tasks.
#[derive(Debug, Default)]
pub struct ShutdownCoordinator {
    is_shutdown: AtomicBool,
    notify: Notify,
}

impl ShutdownCoordinator {
    /// Create a new coordinator.
    pub fn new() -> Self {
        Self {
            is_shutdown: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    /// Create a new shared coordinator wrapped in [`Arc`].
    pub fn shared() -> SharedShutdown {
        Arc::new(Self::new())
    }

    /// Request shutdown. Notifies all registered waiters exactly once.
    pub fn request_shutdown(&self) {
        if !self.is_shutdown.swap(true, Ordering::SeqCst) {
            self.notify.notify_waiters();
        }
    }

    /// Whether shutdown has been requested.
    pub fn is_shutdown_requested(&self) -> bool {
        self.is_shutdown.load(Ordering::SeqCst)
    }

    /// Wait until shutdown is requested. Returns immediately if already set.
    pub async fn wait_for_shutdown(&self) {
        if self.is_shutdown_requested() {
            return;
        }
        self.notify.notified().await;
    }
}
