//! Rate limiting with exponential backoff
//!
//! Implements weight-based and request-based rate limiting with 429 response handling

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::sleep;

/// Rate limiter with weight-based or request-based strategies
#[derive(Clone)]
pub struct RateLimiter {
    limiter_type: RateLimiterType,
    semaphore: Arc<Semaphore>,
    window: Duration,
}

#[derive(Clone)]
enum RateLimiterType {
    WeightBased {
        #[allow(dead_code)]
        max_weight: usize,
    },
    RequestBased {
        #[allow(dead_code)]
        max_requests: usize,
    },
}

impl RateLimiter {
    /// Create a weight-based rate limiter
    ///
    /// # Arguments
    /// * `max_weight` - Maximum weight units per window
    /// * `window` - Time window for rate limit
    pub fn weight_based(max_weight: usize, window: Duration) -> Self {
        Self {
            limiter_type: RateLimiterType::WeightBased { max_weight },
            semaphore: Arc::new(Semaphore::new(max_weight)),
            window,
        }
    }

    /// Create a request-based rate limiter
    ///
    /// # Arguments
    /// * `max_requests` - Maximum requests per window
    /// * `window` - Time window for rate limit
    pub fn request_based(max_requests: usize, window: Duration) -> Self {
        Self {
            limiter_type: RateLimiterType::RequestBased { max_requests },
            semaphore: Arc::new(Semaphore::new(max_requests)),
            window,
        }
    }

    /// Check if this is a weight-based limiter
    pub fn is_weight_based(&self) -> bool {
        matches!(self.limiter_type, RateLimiterType::WeightBased { .. })
    }

    /// Acquire permits for a request
    ///
    /// # Arguments
    /// * `weight` - Number of weight units to acquire
    pub async fn acquire(&self, weight: usize) -> Result<(), RateLimitError> {
        // Acquire semaphore permits
        let _permit = self
            .semaphore
            .acquire_many(weight as u32)
            .await
            .map_err(|e| RateLimitError::AcquireError(e.to_string()))?;

        // Release permits after window duration
        let sem_clone = self.semaphore.clone();
        let window = self.window;
        tokio::spawn(async move {
            sleep(window).await;
            sem_clone.add_permits(weight);
        });

        Ok(())
    }

    /// Handle a rate limit error (429 response) with exponential backoff
    ///
    /// # Arguments
    /// * `attempt` - Attempt number (1-indexed)
    ///
    /// # Returns
    /// The delay duration that was applied
    pub async fn handle_rate_limit_error(&self, attempt: u32) -> Duration {
        let base_delay = Duration::from_secs(1);
        let max_delay = Duration::from_secs(120); // 2 minutes cap

        // Exponential backoff: base_delay * 2^(attempt-1)
        let delay_secs = base_delay.as_secs() * 2_u64.pow(attempt.saturating_sub(1));
        let delay = Duration::from_secs(delay_secs).min(max_delay);

        sleep(delay).await;
        delay
    }

    /// Reset backoff state after successful request
    pub fn reset_backoff(&mut self) {
        // In a real implementation, this would reset internal backoff state
        // For now, the backoff is stateless (based on attempt number)
    }
}

/// Rate limiter errors
#[derive(Debug, thiserror::Error)]
pub enum RateLimitError {
    /// Failed to acquire permits
    #[error("failed to acquire rate limit permits: {0}")]
    AcquireError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rate_limiter_creation() {
        let limiter = RateLimiter::weight_based(100, Duration::from_secs(60));
        assert!(limiter.is_weight_based());

        let limiter2 = RateLimiter::request_based(50, Duration::from_secs(10));
        assert!(!limiter2.is_weight_based());
    }

    #[tokio::test]
    async fn test_acquire_basic() {
        let limiter = RateLimiter::weight_based(10, Duration::from_millis(100));
        limiter.acquire(1).await.unwrap();
    }
}
