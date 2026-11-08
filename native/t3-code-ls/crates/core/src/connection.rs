//! Retry decisions from packages/client-runtime/src/connection/supervisor.ts and wakeups.ts.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.
//!
//! The caller owns the connection, timer, and network observer. This module owns
//! the retry ladder, including the difference between failing to connect and
//! losing an established session. A successful open alone does not reset the
//! ladder: the session must remain up for BACKOFF_RESET_AFTER_MS. Application
//! bytes must never be replayed when applying these connection retry decisions.

pub const RETRY_BASE_DELAY_MS: u64 = 1_000;
pub const RETRY_MAX_DELAY_MS: u64 = 300_000;
pub const SESSION_LOSS_RETRY_DELAY_MS: u64 = 250;
pub const BACKOFF_RESET_AFTER_MS: u64 = 30_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wakeup {
    ApplicationActive,
    ApplicationActiveProbe,
    ApplicationActiveLong,
    CredentialsChanged,
    NetworkChanged,
}

impl Wakeup {
    pub fn resets_retry_backoff(self) -> bool {
        self != Self::CredentialsChanged
    }

    pub fn should_resubscribe(self) -> bool {
        matches!(
            self,
            Self::ApplicationActive | Self::ApplicationActiveProbe | Self::ApplicationActiveLong
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AttemptEnd {
    TransientFailure,
    BlockedFailure,
    Interrupted { reset_retry: bool },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AttemptOutcome {
    pub established: bool,
    pub stable: bool,
    pub probe_failed: bool,
    pub end: AttemptEnd,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryDecision {
    RetryAfter(u64),
    RetryNow,
    AwaitSignal,
}

/// A bounded retry counter. Counts and attempt numbers saturate at u32::MAX;
/// the delay is already capped from failure 8 onward. A reset still restores
/// the full upstream initial-session behavior after saturation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetryPolicy {
    failure_count: u32,
}

impl RetryPolicy {
    pub fn new() -> Self {
        Self { failure_count: 0 }
    }

    pub fn failure_count(&self) -> u32 {
        self.failure_count
    }

    pub fn next_attempt(&self) -> u32 {
        self.failure_count.saturating_add(1)
    }

    /// Explicit retry and disconnect both clear the upstream retry ladder.
    pub fn reset(&mut self) {
        self.failure_count = 0;
    }

    /// Returns whether the caller should discard its current retry deadline.
    pub fn wakeup(&mut self, reason: Wakeup) -> bool {
        if reason.resets_retry_backoff() {
            self.reset();
            true
        } else {
            false
        }
    }

    /// Apply a completed attempt. `random` has the same [0, 1] domain as the
    /// upstream injected Random.next, with 1 accepted for boundary testing.
    ///
    /// Blocked failures wait for a signal without climbing the retry ladder.
    /// A failed liveness probe retries immediately. A short-lived session still
    /// counts as a failure so repeated disconnects cannot spin at 125-250ms.
    pub fn after_attempt(&mut self, outcome: AttemptOutcome, random: f64) -> RetryDecision {
        if outcome.established && outcome.stable {
            self.reset();
        }
        if let AttemptEnd::Interrupted { reset_retry } = outcome.end {
            if reset_retry {
                self.reset();
            }
            return RetryDecision::RetryNow;
        }
        if outcome.end == AttemptEnd::BlockedFailure {
            return RetryDecision::AwaitSignal;
        }
        if outcome.probe_failed {
            self.reset();
            return RetryDecision::RetryNow;
        }
        let first_loss = outcome.established && self.failure_count == 0;
        let previous_failures = self.failure_count;
        self.failure_count = self.failure_count.saturating_add(1);
        RetryDecision::RetryAfter(if first_loss {
            session_loss_retry_delay_ms(random)
        } else {
            retry_delay_ms(previous_failures, random)
        })
    }
}

/// Equal jitter: half of the capped exponential ceiling, plus up to half more.
/// The arithmetic order matches JavaScript's Math.round for these positive values.
// Pinned Lust rejects floating-point ranges (L0641).
#[allow(clippy::manual_range_contains)]
pub fn retry_delay_ms(failure_count: u32, random: f64) -> u64 {
    assert!(
        random >= 0.0 && random <= 1.0,
        "retry jitter must be in [0, 1]"
    );
    // At failure 8 the ceiling is already capped. Avoid an unbounded exponent
    // or shift while preserving the upstream ceiling for every larger count.
    let ceiling = if failure_count >= 8 {
        RETRY_MAX_DELAY_MS
    } else {
        RETRY_BASE_DELAY_MS * (1_u64 << (failure_count + 1))
    };
    let half = ceiling as f64 / 2.0;
    (half + half * random).round() as u64
}

// Pinned Lust rejects floating-point ranges (L0641).
#[allow(clippy::manual_range_contains)]
pub fn session_loss_retry_delay_ms(random: f64) -> u64 {
    assert!(
        random >= 0.0 && random <= 1.0,
        "retry jitter must be in [0, 1]"
    );
    (SESSION_LOSS_RETRY_DELAY_MS as f64 * (0.5 + random / 2.0)).round() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn failure(established: bool, stable: bool) -> AttemptOutcome {
        AttemptOutcome {
            established,
            stable,
            probe_failed: false,
            end: AttemptEnd::TransientFailure,
        }
    }

    #[test]
    fn initial_failure_and_capped_jitter_match_upstream() {
        assert_eq!(retry_delay_ms(0, 0.0), 1_000);
        assert_eq!(retry_delay_ms(0, 1.0), 2_000);
        assert_eq!(retry_delay_ms(7, 0.0), 128_000);
        assert_eq!(retry_delay_ms(8, 0.0), 150_000);
        assert_eq!(retry_delay_ms(u32::MAX, 1.0), 300_000);
        assert_eq!(retry_delay_ms(0, 0.0005), 1_001);
        assert_eq!(session_loss_retry_delay_ms(0.5), 188);
    }

    #[test]
    fn exhausted_counter_preserves_capped_delays_and_can_reset() {
        let mut retry = RetryPolicy {
            failure_count: u32::MAX,
        };
        assert_eq!(retry.next_attempt(), u32::MAX);
        assert_eq!(
            retry.after_attempt(failure(false, false), 1.0),
            RetryDecision::RetryAfter(300_000)
        );
        assert_eq!(retry.failure_count(), u32::MAX);
        assert_eq!(
            retry.after_attempt(failure(true, true), 0.0),
            RetryDecision::RetryAfter(125)
        );
        assert_eq!(retry.failure_count(), 1);
    }

    #[test]
    fn a_repeatedly_lost_session_climbs_the_ladder() {
        let mut retry = RetryPolicy::new();
        assert_eq!(
            retry.after_attempt(failure(true, false), 0.0),
            RetryDecision::RetryAfter(125)
        );
        assert_eq!(
            retry.after_attempt(failure(true, false), 0.0),
            RetryDecision::RetryAfter(2_000)
        );
        assert_eq!(
            retry.after_attempt(failure(false, false), 1.0),
            RetryDecision::RetryAfter(8_000)
        );
        assert_eq!(retry.failure_count(), 3);
        assert_eq!(retry.next_attempt(), 4);
    }

    #[test]
    fn only_an_established_stable_session_resets_the_ladder() {
        let mut retry = RetryPolicy::new();
        retry.after_attempt(failure(false, false), 0.5);
        assert_eq!(
            retry.after_attempt(failure(false, true), 0.0),
            RetryDecision::RetryAfter(2_000)
        );
        assert_eq!(
            retry.after_attempt(failure(true, true), 0.0),
            RetryDecision::RetryAfter(125)
        );
        assert_eq!(retry.failure_count(), 1);
    }

    #[test]
    fn blocked_failure_wins_over_probe_failure_without_incrementing() {
        let mut retry = RetryPolicy::new();
        retry.after_attempt(failure(false, false), 0.5);
        let blocked = AttemptOutcome {
            probe_failed: true,
            end: AttemptEnd::BlockedFailure,
            ..failure(false, false)
        };
        assert_eq!(
            retry.after_attempt(blocked, 0.5),
            RetryDecision::AwaitSignal
        );
        assert_eq!(retry.failure_count(), 1);
    }

    #[test]
    fn unanswered_probe_replaces_connection_immediately() {
        let mut retry = RetryPolicy::new();
        retry.after_attempt(failure(false, false), 0.5);
        let probe = AttemptOutcome {
            probe_failed: true,
            ..failure(true, false)
        };
        assert_eq!(retry.after_attempt(probe, 0.5), RetryDecision::RetryNow);
        assert_eq!(retry.failure_count(), 0);
    }

    #[test]
    fn interruption_respects_the_reset_flag() {
        let mut retry = RetryPolicy::new();
        retry.after_attempt(failure(false, false), 0.5);
        let keep = AttemptOutcome {
            end: AttemptEnd::Interrupted { reset_retry: false },
            ..failure(false, false)
        };
        let reset = AttemptOutcome {
            end: AttemptEnd::Interrupted { reset_retry: true },
            ..keep
        };
        assert_eq!(retry.after_attempt(keep, 0.5), RetryDecision::RetryNow);
        assert_eq!(retry.failure_count(), 1);
        assert_eq!(retry.after_attempt(reset, 0.5), RetryDecision::RetryNow);
        assert_eq!(retry.failure_count(), 0);
    }

    #[test]
    fn wakeups_match_resume_and_network_reset_semantics() {
        for reason in [
            Wakeup::ApplicationActive,
            Wakeup::ApplicationActiveProbe,
            Wakeup::ApplicationActiveLong,
            Wakeup::NetworkChanged,
            Wakeup::CredentialsChanged,
        ] {
            let mut retry = RetryPolicy::new();
            retry.after_attempt(failure(false, false), 0.0);
            assert_eq!(retry.wakeup(reason), reason != Wakeup::CredentialsChanged);
            assert_eq!(
                retry.failure_count(),
                if reason == Wakeup::CredentialsChanged {
                    1
                } else {
                    0
                }
            );
            assert_eq!(
                reason.should_resubscribe(),
                reason != Wakeup::CredentialsChanged && reason != Wakeup::NetworkChanged
            );
        }
    }
}
