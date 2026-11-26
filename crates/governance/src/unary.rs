use crate::CircuitBreaker;
use std::{future::Future, time::Duration};
use tokio::time::{Instant, sleep, timeout_at};
use tonic::{Code, Request, Status};

/// Exponential backoff for a caller-selected idempotent unary method.
#[derive(Clone, Debug)]
pub struct RetryPolicy {
    /// Total attempts including the initial call. Zero still permits one call.
    pub max_attempts: usize,
    /// Delay after the first retryable failure.
    pub initial_backoff: Duration,
    /// Upper bound on the delay between two attempts.
    pub max_backoff: Duration,
    /// Standard statuses eligible for replay.
    pub retryable_codes: Vec<Code>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
            retryable_codes: vec![
                Code::Unavailable,
                Code::DeadlineExceeded,
                Code::ResourceExhausted,
                Code::Internal,
            ],
        }
    }
}

/// Typed unary call wrapper. By default each invocation immediately attempts one RPC.
#[derive(Clone, Default)]
pub struct UnaryPolicy {
    retry: Option<RetryPolicy>,
    breaker: Option<CircuitBreaker>,
}

impl UnaryPolicy {
    /// Enable replay. The caller is responsible for method idempotency.
    pub fn with_retry(mut self, retry: RetryPolicy) -> Self {
        self.retry = Some(retry);
        self
    }

    /// Share a circuit across calls to this upstream.
    pub fn with_breaker(mut self, breaker: CircuitBreaker) -> Self {
        self.breaker = Some(breaker);
        self
    }

    /// Invoke a generated client's unary method with a single absolute deadline.
    /// Dropping the future cancels any in-flight call or pending backoff.
    pub async fn call<M: Clone, R, F, Fut>(
        &self,
        request: Request<M>,
        deadline: Option<Instant>,
        mut invoke: F,
    ) -> Result<R, Status>
    where
        F: FnMut(Request<M>) -> Fut,
        Fut: Future<Output = Result<R, Status>>,
    {
        let permit = self
            .breaker
            .as_ref()
            .map(CircuitBreaker::enter)
            .transpose()?;
        let operation = async {
            let (metadata, extensions, message) = request.into_parts();
            let mut attempts_left = self.retry.as_ref().map_or(1, |r| r.max_attempts.max(1));
            let mut backoff = self
                .retry
                .as_ref()
                .map_or(Duration::ZERO, |r| r.initial_backoff.min(r.max_backoff));
            loop {
                let mut request =
                    Request::from_parts(metadata.clone(), extensions.clone(), message.clone());
                if let Some(end) = deadline {
                    let remaining = end.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        return Err(expired());
                    }
                    request.set_timeout(remaining);
                }
                let result = invoke(request).await;
                attempts_left -= 1;
                let Some(retry) = &self.retry else {
                    return result;
                };
                if attempts_left == 0
                    || !result
                        .as_ref()
                        .is_err_and(|e| retry.retryable_codes.contains(&e.code()))
                {
                    return result;
                }
                sleep(backoff).await;
                backoff = backoff.saturating_mul(2).min(retry.max_backoff);
            }
        };
        let result = match deadline {
            Some(end) => timeout_at(end, operation)
                .await
                .unwrap_or_else(|_| Err(expired())),
            None => operation.await,
        };
        if let Some(permit) = permit {
            permit.finish(result.as_ref().err().map(Status::code));
        }
        result
    }
}

fn expired() -> Status {
    Status::deadline_exceeded("unary call deadline exceeded")
}
