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
    pub(crate) policy: LbPolicy,
    /// Next ticket within this immutable membership generation.
    cursor: AtomicUsize,
    /// Indices for one exact smooth cycle (bounded in both construction cost and size).
    cycle: Vec<usize>,
    /// Inclusive cumulative weights, allowing ratios beyond a machine word.
    cumulative: Vec<u128>,
}

impl Picker {
    pub(crate) fn new(policy: LbPolicy, weights: &[i64]) -> Self {
        let mut cumulative = Vec::with_capacity(weights.len());
        let mut sum = 0_u128;
        let mut divisor = 0;
        for &raw in weights {
            let weight = if raw > 0 { raw as u128 } else { 100 };
            sum += weight;
            cumulative.push(sum);
            divisor = gcd(divisor, weight);
        }
        let mut cycle = Vec::new();
        if policy == LbPolicy::WeightedRoundRobin && divisor > 0 && sum / divisor <= 512 {
            let weights: Vec<i64> = weights
                .iter()
                .map(|&w| ((if w > 0 { w as u128 } else { 100 }) / divisor) as i64)
                .collect();
            let total: i64 = weights.iter().sum();
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
        }
        Self {
            policy,
            cumulative,
            cycle,
            cursor: AtomicUsize::new(0),
        }
    }

    /// Choose without synchronizing unrelated requests on a mutex.
    pub(crate) fn pick(&self) -> Option<usize> {
        let total = *self.cumulative.last()?;
        Some(match self.policy {
            LbPolicy::PickFirst => 0,
            LbPolicy::RoundRobin => {
                self.cursor.fetch_add(1, Ordering::Relaxed) % self.cumulative.len()
            }
            LbPolicy::WeightedRoundRobin if !self.cycle.is_empty() => {
                self.cycle[self.cursor.fetch_add(1, Ordering::Relaxed) % self.cycle.len()]
            }
            LbPolicy::WeightedRoundRobin => {
                let ticket = fastrand::u128(0..total);
                self.cumulative.partition_point(|w| *w <= ticket)
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
        assert!(picker.cycle.is_empty());
        assert_eq!(picker.cumulative[2], i64::MAX as u128 * 3 - 3);
        assert!(picker.pick().unwrap() < 3);
    }
}
