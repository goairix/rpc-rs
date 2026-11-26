use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::{Instant, sleep};

/// Shared token bucket. Capacity is the initial and maximum burst size.
#[derive(Clone)]
pub struct TokenBucket {
    state: Arc<Mutex<Balance>>,
    /// Refill rate in tokens per second, clamped to at least one.
    rate: f64,
    capacity: u32,
}

struct Balance {
    tokens: f64,
    /// Monotonic instant through which refill has already been accounted.
    updated: Instant,
}

impl TokenBucket {
    /// Create a full bucket. A capacity of zero permanently rejects admissions.
    pub fn new(rate_per_second: f64, capacity: u32) -> Self {
        Self {
            state: Arc::new(Mutex::new(Balance {
                tokens: f64::from(capacity),
                updated: Instant::now(),
            })),
            rate: rate_per_second.clamp(1.0, f64::MAX).max(1.0),
            capacity,
        }
    }

    /// Immediately consume one token if available.
    pub fn allow(&self) -> bool {
        self.take().is_ok()
    }

    fn take(&self) -> Result<(), Duration> {
        let mut balance = self.state.lock().unwrap();
        let now = Instant::now();
        balance.tokens = (balance.tokens + (now - balance.updated).as_secs_f64() * self.rate)
            .min(f64::from(self.capacity));
        balance.updated = now;
        if balance.tokens >= 1.0 {
            balance.tokens -= 1.0;
            Ok(())
        } else {
            Err(Duration::from_secs_f64((1.0 - balance.tokens) / self.rate))
        }
    }

    /// Wait for admission. Dropping this future while waiting consumes no tokens.
    pub async fn wait(&self) {
        if self.capacity == 0 {
            std::future::pending::<()>().await;
        }
        while let Err(delay) = self.take() {
            sleep(delay).await;
        }
    }
}
