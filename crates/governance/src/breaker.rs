use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;
use tonic::{Code, Status};

/// Thresholds and open duration shared by every clone of a circuit.
#[derive(Clone, Copy, Debug)]
pub struct BreakerConfig {
    /// Counted failures before opening. Zero opens on the first counted failure.
    pub failure_threshold: u32,
    /// Successful half-open probes before closing. Zero closes on the first success.
    pub success_threshold: u32,
    /// Minimum time spent open before the next call may become a probe.
    pub open_interval: Duration,
}

impl Default for BreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            open_interval: Duration::from_secs(10),
        }
    }
}

/// Shared circuit state for one logical upstream. Clones share failure history.
#[derive(Clone)]
pub struct CircuitBreaker {
    config: BreakerConfig,
    state: Arc<Mutex<State>>,
    /// Application-defined classification, run outside the state lock.
    failed: Arc<dyn Fn(Code) -> bool + Send + Sync>,
}

struct State {
    /// Incremented when changing phases to ignore requests admitted before the transition.
    generation: u64,
    phase: Phase,
}

enum Phase {
    Closed {
        consecutive_failures: u32,
    },
    Open {
        /// Monotonic instant of the latest transition to open.
        opened_at: Instant,
    },
    HalfOpen {
        successes: u32,
        probe_in_flight: bool,
    },
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new(|code| code != Code::Ok)
    }
}

impl CircuitBreaker {
    /// Count errors selected by the classifier. Five consecutive failures open the circuit.
    pub fn new(failed: impl Fn(Code) -> bool + Send + Sync + 'static) -> Self {
        Self::with_config(BreakerConfig::default(), failed)
    }

    /// Create a circuit with application thresholds and an error classifier.
    /// Excluded errors preserve counters and release any half-open probe.
    pub fn with_config(
        config: BreakerConfig,
        failed: impl Fn(Code) -> bool + Send + Sync + 'static,
    ) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(State {
                generation: 0,
                phase: Phase::Closed {
                    consecutive_failures: 0,
                },
            })),
            failed: Arc::new(failed),
        }
    }

    /// Admit one whole logical operation, including all retry attempts.
    pub(crate) fn enter(&self) -> Result<Permit<'_>, Status> {
        let mut state = self.state.lock().unwrap();
        if let Phase::Open { opened_at } = state.phase {
            if opened_at.elapsed() < self.config.open_interval {
                return Err(Status::unavailable("circuit is open"));
            }
            state.generation += 1;
            state.phase = Phase::HalfOpen {
                successes: 0,
                probe_in_flight: false,
            };
        }
        if let Phase::HalfOpen {
            probe_in_flight, ..
        } = &mut state.phase
        {
            if *probe_in_flight {
                return Err(Status::unavailable("circuit probe in progress"));
            }
            *probe_in_flight = true;
        }
        Ok(Permit {
            breaker: self,
            generation: state.generation,
            finished: false,
        })
    }
}

pub(crate) struct Permit<'a> {
    breaker: &'a CircuitBreaker,
    generation: u64,
    finished: bool,
}

impl Permit<'_> {
    pub(crate) fn finish(mut self, error: Option<Code>) {
        // Dropping a neutral permit still releases its probe slot.
        if error.is_some_and(|code| !(self.breaker.failed)(code)) {
            return;
        }
        let failed = error.is_some();
        let mut state = self.breaker.state.lock().unwrap();
        self.finished = true;
        if self.generation != state.generation {
            return;
        }
        let next = match &mut state.phase {
            Phase::Closed {
                consecutive_failures,
            } => {
                if !failed {
                    *consecutive_failures = 0;
                    return;
                }
                *consecutive_failures += 1;
                if *consecutive_failures < self.breaker.config.failure_threshold {
                    return;
                }
                Phase::Open {
                    opened_at: Instant::now(),
                }
            }
            Phase::HalfOpen {
                successes,
                probe_in_flight,
            } => {
                *probe_in_flight = false;
                if failed {
                    Phase::Open {
                        opened_at: Instant::now(),
                    }
                } else {
                    *successes += 1;
                    if *successes < self.breaker.config.success_threshold {
                        return;
                    }
                    Phase::Closed {
                        consecutive_failures: 0,
                    }
                }
            }
            Phase::Open { .. } => return,
        };
        state.generation += 1;
        state.phase = next;
    }
}

impl Drop for Permit<'_> {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut state = self.breaker.state.lock().unwrap();
        if state.generation == self.generation
            && let Phase::HalfOpen {
                probe_in_flight, ..
            } = &mut state.phase
        {
            *probe_in_flight = false;
        }
    }
}
