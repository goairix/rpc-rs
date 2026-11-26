use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;
use tonic::{Code, Status};

/// Shared circuit state for one logical upstream. Clones share failure history.
#[derive(Clone)]
pub struct CircuitBreaker {
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
        probe_after: Instant,
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
        Self {
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
        if let Phase::Open { probe_after } = state.phase {
            if Instant::now() < probe_after {
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
        let failed = error.is_some_and(|code| (self.breaker.failed)(code));
        let mut state = self.breaker.state.lock().unwrap();
        self.finished = true;
        if self.generation != state.generation {
            return;
        }
        let next = match &mut state.phase {
            Phase::Closed {
                consecutive_failures,
            } => {
                *consecutive_failures = if failed { *consecutive_failures + 1 } else { 0 };
                if *consecutive_failures < 5 {
                    return;
                }
                Phase::Open {
                    probe_after: Instant::now() + Duration::from_secs(10),
                }
            }
            Phase::HalfOpen {
                successes,
                probe_in_flight,
            } => {
                *probe_in_flight = false;
                if failed {
                    Phase::Open {
                        probe_after: Instant::now() + Duration::from_secs(10),
                    }
                } else {
                    *successes += 1;
                    if *successes < 3 {
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
