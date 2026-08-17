//! Reconnect/backoff state kept inside an agent actor.

use std::time::Duration;

use rand::{Rng, RngExt};

use crate::config::ReconnectConfig;

/// Exponential reconnect scheduler with bounded symmetric jitter.
#[derive(Clone, Debug)]
pub struct ReconnectBackoff {
    policy: ReconnectConfig,
    attempt: u32,
}

impl ReconnectBackoff {
    /// Start a new backoff sequence.
    pub const fn new(policy: ReconnectConfig) -> Self {
        Self { policy, attempt: 0 }
    }

    /// Return the next delay using the supplied random source.
    pub fn next_delay(&mut self, rng: &mut impl Rng) -> Duration {
        let exponent = i32::try_from(self.attempt).unwrap_or(i32::MAX);
        let factor = self.policy.multiplier.powi(exponent);
        let deterministic =
            ((self.policy.initial_delay_ms as f64) * factor).min(self.policy.max_delay_ms as f64);
        let jitter = self.policy.jitter;
        let jitter_factor = if jitter == 0.0 {
            1.0
        } else {
            1.0 + rng.random_range(-jitter..=jitter)
        };
        let delay_ms = (deterministic * jitter_factor)
            .clamp(0.0, self.policy.max_delay_ms as f64)
            .round() as u64;
        self.attempt = self.attempt.saturating_add(1);
        Duration::from_millis(delay_ms)
    }

    /// Number of delays already issued in the current sequence.
    pub const fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Clear attempt state after a stable ready connection.
    pub const fn reset(&mut self) {
        self.attempt = 0;
    }
}

#[cfg(test)]
mod tests {
    use rand::{SeedableRng, rngs::StdRng};

    use super::*;

    #[test]
    fn deterministic_portion_is_exponential_and_capped() {
        let mut backoff = ReconnectBackoff::new(ReconnectConfig {
            initial_delay_ms: 10,
            max_delay_ms: 25,
            multiplier: 2.0,
            jitter: 0.0,
        });
        let mut rng = StdRng::seed_from_u64(1);
        assert_eq!(backoff.next_delay(&mut rng), Duration::from_millis(10));
        assert_eq!(backoff.next_delay(&mut rng), Duration::from_millis(20));
        assert_eq!(backoff.next_delay(&mut rng), Duration::from_millis(25));
        assert_eq!(backoff.attempt(), 3);
        backoff.reset();
        assert_eq!(backoff.attempt(), 0);
    }

    #[test]
    fn jitter_never_exceeds_policy_cap() {
        let mut backoff = ReconnectBackoff::new(ReconnectConfig {
            initial_delay_ms: 100,
            max_delay_ms: 100,
            multiplier: 2.0,
            jitter: 1.0,
        });
        let mut rng = StdRng::seed_from_u64(7);
        for _ in 0..32 {
            assert!(backoff.next_delay(&mut rng) <= Duration::from_millis(100));
        }
    }
}
