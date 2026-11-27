use std::sync::atomic::{AtomicUsize, Ordering};

/// A policy only sees established connections to instances whose registry status is up.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LbPolicy {
    /// Prefer the incumbent until it leaves the available set.
    PickFirst,
    /// Cycle through all established connections equally.
    #[default]
    RoundRobin,
    /// Use smooth weighted round robin for short periods and proportional sampling otherwise.
    WeightedRoundRobin,
}

pub(crate) struct Picker {
    schedule: Schedule,
    /// Next ticket within this immutable membership generation.
    cursor: AtomicUsize,
}

/// Only weighted, unequal instances need a precomputed table.
enum Schedule {
    Empty,
    First,
    RoundRobin(usize),
    /// One exact smooth cycle of channel indices.
    Smooth(Vec<usize>),
    /// Inclusive cumulative weights for proportional sampling.
    Sampled(Vec<u128>),
}

/// Go registrations use the default weight of 100 for missing or nonpositive values.
pub(crate) fn effective_weight(weight: i64) -> i64 {
    if weight > 0 { weight } else { 100 }
}

impl Picker {
    pub(crate) fn new(policy: LbPolicy, weights: &[i64]) -> Self {
        let schedule = match (weights.first(), policy) {
            (None, _) => Schedule::Empty,
            (_, LbPolicy::PickFirst) => Schedule::First,
            (_, LbPolicy::RoundRobin) => Schedule::RoundRobin(weights.len()),
            (Some(&first), LbPolicy::WeightedRoundRobin) => {
                let first = effective_weight(first);
                if weights.iter().all(|&w| effective_weight(w) == first) {
                    Schedule::RoundRobin(weights.len())
                } else {
                    Self::weighted_schedule(weights)
                }
            }
        };
        Self {
            schedule,
            cursor: AtomicUsize::new(0),
        }
    }

    fn weighted_schedule(weights: &[i64]) -> Schedule {
        let mut sum = 0_u128;
        let mut divisor = 0;
        for &raw in weights {
            let weight = effective_weight(raw) as u128;
            sum += weight;
            divisor = gcd(divisor, weight);
        }
        if sum / divisor <= 512 {
            let weights: Vec<i64> = weights
                .iter()
                .map(|&w| ((effective_weight(w) as u128) / divisor) as i64)
                .collect();
            let total: i64 = weights.iter().sum();
            let mut cycle = Vec::with_capacity(total as usize);
            let mut credit = vec![0; weights.len()];
            for _ in 0..total {
                let mut best = 0;
                for (i, weight) in weights.iter().enumerate() {
                    credit[i] += weight;
                    if credit[i] > credit[best] {
                        best = i;
                    }
                }
                credit[best] -= total;
                cycle.push(best);
            }
            Schedule::Smooth(cycle)
        } else {
            let mut total = 0;
            Schedule::Sampled(
                weights
                    .iter()
                    .map(|&w| {
                        total += effective_weight(w) as u128;
                        total
                    })
                    .collect(),
            )
        }
    }

    /// Choose without synchronizing unrelated requests on a mutex.
    pub(crate) fn pick(&self) -> Option<usize> {
        Some(match &self.schedule {
            Schedule::Empty => return None,
            Schedule::First => 0,
            Schedule::RoundRobin(len) => self.cursor.fetch_add(1, Ordering::Relaxed) % len,
            Schedule::Smooth(cycle) => {
                cycle[self.cursor.fetch_add(1, Ordering::Relaxed) % cycle.len()]
            }
            Schedule::Sampled(cumulative) => {
                let ticket = fastrand::u128(0..*cumulative.last()?);
                cumulative.partition_point(|w| *w <= ticket)
            }
        })
    }
}

fn gcd(mut a: u128, mut b: u128) -> u128 {
    while b != 0 {
        (a, b) = (b, a % b);
    }
    a
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn concurrent_round_robin_shares_one_cursor() {
        let picker = Picker::new(LbPolicy::default(), &[1, 1, 1]);
        std::thread::scope(|scope| {
            let joins: Vec<_> = (0..8)
                .map(|_| {
                    scope.spawn(|| {
                        let mut counts = [0; 3];
                        for _ in 0..300 {
                            counts[picker.pick().unwrap()] += 1;
                        }
                        counts
                    })
                })
                .collect();
            let mut total = [0; 3];
            for join in joins {
                for (sum, count) in total.iter_mut().zip(join.join().unwrap()) {
                    *sum += count;
                }
            }
            assert_eq!(total, [800; 3]);
        });
    }

    #[test]
    fn equal_effective_weights_use_rotation_without_a_table() {
        for weights in [vec![0, 100, -1], vec![7; 512], vec![7; 513]] {
            let picker = Picker::new(LbPolicy::WeightedRoundRobin, &weights);
            assert!(matches!(picker.schedule, Schedule::RoundRobin(_)));
            for index in 0..weights.len() * 2 {
                assert_eq!(picker.pick(), Some(index % weights.len()));
            }
        }
    }

    #[test]
    fn weights_and_empty_membership() {
        for weights in [[500, 0], [5, 1], [500, -99]] {
            let picker = Picker::new(LbPolicy::WeightedRoundRobin, &weights);
            assert_eq!((0..60).filter(|_| picker.pick() == Some(1)).count(), 10);
        }
        assert_eq!(Picker::new(LbPolicy::RoundRobin, &[]).pick(), None);
        let picker = Picker::new(
            LbPolicy::WeightedRoundRobin,
            &[i64::MAX, i64::MAX - 1, i64::MAX - 2],
        );
        let Schedule::Sampled(ref cumulative) = picker.schedule else {
            panic!("expected sampling");
        };
        assert_eq!(cumulative[2], i64::MAX as u128 * 3 - 3);
        assert!(picker.pick().unwrap() < 3);
    }
}
