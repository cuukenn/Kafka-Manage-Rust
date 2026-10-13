//! Fault and latency injection so the UI can be exercised against failures it would
//! otherwise never see: connect refused, timeouts, leaderless partitions, broker errors.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use kafka_core::error::DriverError;

use super::state::tp_key;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct LatencyRange {
    #[serde(default)]
    pub min_ms: u64,
    #[serde(default)]
    pub max_ms: u64,
}

impl LatencyRange {
    pub fn is_none(&self) -> bool {
        self.min_ms == 0 && self.max_ms == 0
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct BrokerFault {
    #[serde(default)]
    pub code: i16,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub message: String,
}

/// All fault knobs for one mock cluster. Every field defaults to "off".
/// Deliberately **not** `Eq`: `fail_probability` and `timeout_probability` are `f32`,
/// and a probability of `0.1` is not bit-equal to one computed as `1.0 / 10.0`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct FaultInjection {
    /// `connect` always fails.
    #[serde(default)]
    pub fail_connect: bool,
    #[serde(default)]
    pub connect_message: String,
    /// Artificial latency applied to every driver call.
    #[serde(default)]
    pub latency: LatencyRange,
    /// Probability in `0.0..=1.0` that a call fails outright.
    #[serde(default)]
    pub fail_probability: f32,
    /// Restrict random failures to one operation (`produce`, `subscribe`, ...);
    /// `None` means every operation.
    #[serde(default)]
    pub fail_operation: Option<String>,
    #[serde(default)]
    pub fail_message: String,
    /// Probability that a call fails with a timeout instead of a plain error.
    #[serde(default)]
    pub timeout_probability: f32,
    #[serde(default)]
    pub timeout_ms: u64,
    /// Partitions forced leaderless, keyed `"{topic}#{partition}"`.
    #[serde(default)]
    pub leader_down: BTreeSet<String>,
    /// Every call fails with this broker error code.
    #[serde(default)]
    pub broker_error: Option<BrokerFault>,
}

impl Default for FaultInjection {
    fn default() -> Self {
        Self {
            fail_connect: false,
            connect_message: String::new(),
            latency: LatencyRange::default(),
            fail_probability: 0.0,
            fail_operation: None,
            fail_message: "injected mock failure".to_string(),
            timeout_probability: 0.0,
            timeout_ms: 5_000,
            leader_down: BTreeSet::new(),
            broker_error: None,
        }
    }
}

impl FaultInjection {
    pub fn all_off(&self) -> bool {
        !self.fail_connect
            && self.latency.is_none()
            && self.fail_probability <= 0.0
            && self.timeout_probability <= 0.0
            && self.leader_down.is_empty()
            && self.broker_error.is_none()
    }

    pub fn set_leader_down(&mut self, topic: &str, partition: u32, down: bool) {
        if down {
            self.leader_down.insert(tp_key(topic, partition));
        } else {
            self.leader_down.remove(&tp_key(topic, partition));
        }
    }

    pub fn is_leader_down(&self, topic: &str, partition: u32) -> bool {
        self.leader_down.contains(&tp_key(topic, partition))
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }

    /// Deterministic at the extremes (`0.0` never fails, `1.0` always fails) so
    /// tests do not depend on the seed.
    fn roll(probability: f32, rng: &mut Rng) -> bool {
        if probability <= 0.0 {
            false
        } else if probability >= 1.0 {
            true
        } else {
            rng.next_f32() < probability
        }
    }

    /// Returns an injected failure for `op`, if any applies.
    pub fn check(&self, op: &str, rng: &mut Rng) -> Option<DriverError> {
        if let Some(broker) = &self.broker_error {
            return Some(DriverError::Broker {
                code: broker.code,
                name: if broker.name.is_empty() { "INJECTED".into() } else { broker.name.clone() },
                message: if broker.message.is_empty() { format!("injected broker error for '{op}'") } else { broker.message.clone() },
            });
        }
        let op_matches = self.fail_operation.as_deref().is_none_or(|want| want == op);
        if op_matches {
            if Self::roll(self.timeout_probability, rng) {
                return Some(DriverError::timeout(op, self.timeout_ms.max(1)));
            }
            if Self::roll(self.fail_probability, rng) {
                return Some(DriverError::Connection {
                    server: self.fail_operation.clone().unwrap_or_else(|| op.to_string()),
                    message: if self.fail_message.is_empty() { "injected mock failure".into() } else { self.fail_message.clone() },
                });
            }
        }
        None
    }

    /// Milliseconds to sleep before doing real work.
    pub fn latency_ms(&self, rng: &mut Rng) -> u64 {
        let min = self.latency.min_ms;
        let max = self.latency.max_ms.max(min);
        if max == 0 {
            return 0;
        }
        min + rng.next_below(max - min + 1)
    }
}

/// Tiny SplitMix64 PRNG: no extra dependency, deterministic when seeded.
#[derive(Debug, Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    pub fn from_clock() -> Self {
        Self::new(kafka_core::protocol::now_ms() as u64)
    }

    pub fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    pub fn next_below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            0
        } else {
            self.next_u64() % bound
        }
    }

    pub fn next_f32(&mut self) -> f32 {
        (self.next_u64() >> 40) as f32 / (1u64 << 24) as f32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_inject_nothing() {
        let faults = FaultInjection::default();
        assert!(faults.all_off());
        assert!(faults.check("produce", &mut Rng::new(1)).is_none());
        assert_eq!(faults.latency_ms(&mut Rng::new(1)), 0);
    }

    #[test]
    fn probability_one_always_fails_and_zero_never() {
        let mut faults = FaultInjection { fail_probability: 1.0, ..Default::default() };
        for seed in 0..8 {
            assert!(faults.check("produce", &mut Rng::new(seed)).is_some(), "seed {seed} should fail");
        }
        faults.fail_probability = 0.0;
        for seed in 0..8 {
            assert!(faults.check("produce", &mut Rng::new(seed)).is_none());
        }
    }

    #[test]
    fn failure_can_be_scoped_to_one_operation() {
        let faults = FaultInjection {
            fail_probability: 1.0,
            fail_operation: Some("produce".into()),
            ..Default::default()
        };
        assert!(faults.check("produce", &mut Rng::new(7)).is_some());
        assert!(faults.check("list_topics", &mut Rng::new(7)).is_none());
    }

    #[test]
    fn timeout_wins_over_plain_failure() {
        let faults = FaultInjection { timeout_probability: 1.0, timeout_ms: 250, ..Default::default() };
        let err = faults.check("subscribe", &mut Rng::new(3)).expect("injected");
        assert!(matches!(err, DriverError::Timeout { .. }));
        assert!(err.is_retryable());
    }

    #[test]
    fn leader_down_set_tracks_partitions() {
        let mut faults = FaultInjection::default();
        faults.set_leader_down("orders", 2, true);
        assert!(faults.is_leader_down("orders", 2));
        assert!(!faults.is_leader_down("orders", 3));
        faults.set_leader_down("orders", 2, false);
        assert!(!faults.is_leader_down("orders", 2));
    }

    #[test]
    fn latency_stays_inside_the_range() {
        let faults = FaultInjection { latency: LatencyRange { min_ms: 5, max_ms: 9 }, ..Default::default() };
        for seed in 0..32 {
            let ms = faults.latency_ms(&mut Rng::new(seed));
            assert!((5..=9).contains(&ms), "seed {seed} gave {ms}ms");
        }
    }

    #[test]
    fn rng_is_deterministic_per_seed() {
        let a: Vec<u64> = (0..4).map(|_| Rng::new(42).next_u64()).collect();
        let b: Vec<u64> = (0..4).map(|_| Rng::new(42).next_u64()).collect();
        assert_eq!(a, b);
    }
}
