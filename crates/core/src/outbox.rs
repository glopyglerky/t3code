//! Durable effect lifecycle from orchestration-v2/EffectOutbox.ts.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.
//!
//! The store owns transactions, payload validation, clocks, and notifications.
//! This policy owns scheduling and every persisted lifecycle transition. Times
//! are canonical UTC millisecond strings supplied by the host, matching SQLite's
//! binary text comparisons. A running effect never expires merely with time.

use std::collections::HashMap;

pub const MAX_COUNTER: u64 = 9_007_199_254_740_991;
pub const PRUNE_BATCH_SIZE: usize = 500;
pub const PROCESS_LOSS_CANCEL_REASON: &str =
    "Cancelled because the server process ended before the effect completed.";
pub const PROCESS_LOSS_REQUEUE_REASON: &str = "Requeued after the previous server process ended.";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectType {
    ProviderRuntimeContinue,
    ProviderSessionDetach,
    ProviderTurnStart,
    ProviderTurnInterrupt,
    ProviderTurnSteer,
    ProviderTurnRestart,
    RuntimeRequestRespond,
    ProviderThreadRollback,
    CheckpointCapture,
    ScheduledTasksCleanup,
    TerminalCleanup,
    PreviewCleanup,
    AttachmentCleanup,
    ThreadTitleGenerate,
    DelegatedTasksStop,
}

impl EffectType {
    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "provider-runtime.continue" => Some(Self::ProviderRuntimeContinue),
            "provider-session.detach" => Some(Self::ProviderSessionDetach),
            "provider-turn.start" => Some(Self::ProviderTurnStart),
            "provider-turn.interrupt" => Some(Self::ProviderTurnInterrupt),
            "provider-turn.steer" => Some(Self::ProviderTurnSteer),
            "provider-turn.restart" => Some(Self::ProviderTurnRestart),
            "runtime-request.respond" => Some(Self::RuntimeRequestRespond),
            "provider-thread.rollback" => Some(Self::ProviderThreadRollback),
            "checkpoint.capture" => Some(Self::CheckpointCapture),
            "scheduled-tasks.cleanup" => Some(Self::ScheduledTasksCleanup),
            "terminal.cleanup" => Some(Self::TerminalCleanup),
            "preview.cleanup" => Some(Self::PreviewCleanup),
            "attachment.cleanup" => Some(Self::AttachmentCleanup),
            "thread-title.generate" => Some(Self::ThreadTitleGenerate),
            "delegated-tasks.stop" => Some(Self::DelegatedTasksStop),
            _ => None,
        }
    }

    pub fn as_wire(self) -> &'static str {
        match self {
            Self::ProviderRuntimeContinue => "provider-runtime.continue",
            Self::ProviderSessionDetach => "provider-session.detach",
            Self::ProviderTurnStart => "provider-turn.start",
            Self::ProviderTurnInterrupt => "provider-turn.interrupt",
            Self::ProviderTurnSteer => "provider-turn.steer",
            Self::ProviderTurnRestart => "provider-turn.restart",
            Self::RuntimeRequestRespond => "runtime-request.respond",
            Self::ProviderThreadRollback => "provider-thread.rollback",
            Self::CheckpointCapture => "checkpoint.capture",
            Self::ScheduledTasksCleanup => "scheduled-tasks.cleanup",
            Self::TerminalCleanup => "terminal.cleanup",
            Self::PreviewCleanup => "preview.cleanup",
            Self::AttachmentCleanup => "attachment.cleanup",
            Self::ThreadTitleGenerate => "thread-title.generate",
            Self::DelegatedTasksStop => "delegated-tasks.stop",
        }
    }

    pub fn is_title(self) -> bool {
        self == Self::ThreadTitleGenerate
    }

    pub fn is_process_bound(self) -> bool {
        matches!(
            self,
            Self::ProviderTurnStart
                | Self::ProviderTurnInterrupt
                | Self::ProviderTurnSteer
                | Self::ProviderTurnRestart
                | Self::RuntimeRequestRespond
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectStatus {
    Pending,
    Running,
    Succeeded,
    Failed,
    Cancelled,
}

impl EffectStatus {
    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    pub fn as_wire(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
        }
    }

    pub fn is_unsettled(self) -> bool {
        matches!(self, Self::Pending | Self::Running)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Row {
    pub id: String,
    pub thread_id: String,
    pub effect_type: EffectType,
    pub status: EffectStatus,
    pub insertion_ordinal: u64,
    pub attempt_count: u64,
    pub available_at: String,
    pub lease_owner: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub completed_at: Option<String>,
    pub last_error: Option<String>,
}

/// Check a candidate against the complete indexed lane visible in its write transaction.
pub fn claimable(
    candidate: &Row,
    lane_rows: &[Row],
    now: Option<&str>,
    exclude_restart_continuations: bool,
) -> bool {
    if candidate.status != EffectStatus::Pending
        || now.is_some_and(|time| candidate.available_at.as_str() > time)
        || (exclude_restart_continuations
            && candidate.effect_type == EffectType::ProviderRuntimeContinue)
    {
        return false;
    }
    !lane_rows.iter().any(|active| {
        active.thread_id == candidate.thread_id
            && active.effect_type.is_title() == candidate.effect_type.is_title()
            && (active.status == EffectStatus::Running
                || (active.status == EffectStatus::Pending
                    && active.insertion_ordinal < candidate.insertion_ordinal
                    && !(exclude_restart_continuations
                        && active.effect_type == EffectType::ProviderRuntimeContinue)))
    })
}

#[derive(Default)]
struct Lane {
    running: bool,
    first_pending: Option<u64>,
}

fn lanes(rows: &[Row], title: bool, exclude: bool) -> HashMap<String, Lane> {
    let mut result: HashMap<String, Lane> = HashMap::new();
    for row in rows {
        if row.effect_type.is_title() != title || !row.status.is_unsettled() {
            continue;
        }
        let lane = result.entry(row.thread_id.clone()).or_default();
        if row.status == EffectStatus::Running {
            lane.running = true;
        } else if !(exclude && row.effect_type == EffectType::ProviderRuntimeContinue) {
            lane.first_pending = Some(
                lane.first_pending
                    .map_or(row.insertion_ordinal, |old| old.min(row.insertion_ordinal)),
            );
        }
    }
    result
}

fn select(rows: &[Row], now: Option<&str>, exclude: bool) -> Option<usize> {
    // Build lane heads once so a standalone bounded policy frame is linear in row count.
    let regular = lanes(rows, false, exclude);
    let titles = lanes(rows, true, exclude);
    let mut selected: Option<usize> = None;
    for (index, row) in rows.iter().enumerate() {
        if row.status != EffectStatus::Pending
            || now.is_some_and(|time| row.available_at.as_str() > time)
            || (exclude && row.effect_type == EffectType::ProviderRuntimeContinue)
        {
            continue;
        }
        let lane = if row.effect_type.is_title() {
            titles.get(&row.thread_id)
        } else {
            regular.get(&row.thread_id)
        };
        if !lane
            .is_some_and(|lane| !lane.running && lane.first_pending == Some(row.insertion_ordinal))
        {
            continue;
        }
        let before = selected.is_none_or(|previous| {
            let other = &rows[previous];
            (&row.available_at, &row.created_at, &row.id)
                < (&other.available_at, &other.created_at, &other.id)
        });
        if before {
            selected = Some(index);
        }
    }
    selected
}

pub fn claim_next(rows: &[Row], now: &str, exclude_restart_continuations: bool) -> Option<usize> {
    select(rows, Some(now), exclude_restart_continuations)
}

/// Unlike the startup claim filter, upstream's wake deadline includes restart continuations.
pub fn next_claimable_at(rows: &[Row]) -> Option<String> {
    select(rows, None, false).map(|index| rows[index].available_at.clone())
}

pub fn owned_running(row: &Row, worker_id: &str) -> bool {
    row.status == EffectStatus::Running && row.lease_owner.as_deref() == Some(worker_id)
}

pub fn cancel_matches(row: &Row, thread_id: &str, effect_types: &[EffectType]) -> bool {
    row.thread_id == thread_id
        && row.status.is_unsettled()
        && effect_types.contains(&row.effect_type)
}

pub fn retry_delay_ms(delay_ms: i64) -> u64 {
    if delay_ms < 0 { 0 } else { delay_ms as u64 }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Action {
    Claim {
        worker_id: String,
    },
    Succeed {
        worker_id: String,
    },
    Retry {
        worker_id: String,
        error: String,
        available_at: String,
    },
    Fail {
        worker_id: String,
        error: String,
    },
    Cancel {
        reason: String,
    },
    Reconcile,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    CounterOverflow,
}

/// None means the SQL predicate would update no row. Claim callers first check lane eligibility.
pub fn transition(row: &Row, action: Action, now: &str) -> Result<Option<Row>, Error> {
    let mut next = row.clone();
    match action {
        Action::Claim { worker_id } => {
            if row.status != EffectStatus::Pending {
                return Ok(None);
            }
            if row.attempt_count >= MAX_COUNTER {
                return Err(Error::CounterOverflow);
            }
            next.status = EffectStatus::Running;
            next.attempt_count += 1;
            next.lease_owner = Some(worker_id);
            next.last_error = None;
        }
        Action::Succeed { worker_id } => {
            if !owned_running(row, &worker_id) {
                return Ok(None);
            }
            next.status = EffectStatus::Succeeded;
            next.lease_owner = None;
            next.completed_at = Some(now.to_string());
            next.last_error = None;
        }
        Action::Retry {
            worker_id,
            error,
            available_at,
        } => {
            if !owned_running(row, &worker_id) {
                return Ok(None);
            }
            next.status = EffectStatus::Pending;
            next.available_at = available_at;
            next.lease_owner = None;
            next.last_error = Some(error);
        }
        Action::Fail { worker_id, error } => {
            if !owned_running(row, &worker_id) {
                return Ok(None);
            }
            next.status = EffectStatus::Failed;
            next.lease_owner = None;
            next.completed_at = Some(now.to_string());
            next.last_error = Some(error);
        }
        Action::Cancel { reason } => {
            if !row.status.is_unsettled() {
                return Ok(None);
            }
            next.status = EffectStatus::Cancelled;
            next.lease_owner = None;
            next.completed_at = Some(now.to_string());
            next.last_error = Some(reason);
        }
        Action::Reconcile => {
            if row.effect_type.is_process_bound() && row.status.is_unsettled() {
                return transition(
                    row,
                    Action::Cancel {
                        reason: PROCESS_LOSS_CANCEL_REASON.to_string(),
                    },
                    now,
                );
            }
            if row.effect_type.is_process_bound() || row.status != EffectStatus::Running {
                return Ok(None);
            }
            next.status = EffectStatus::Pending;
            next.lease_owner = None;
            next.available_at = now.to_string();
            next.last_error = Some(PROCESS_LOSS_REQUEUE_REASON.to_string());
        }
    }
    next.updated_at = now.to_string();
    Ok(Some(next))
}

pub fn prunable(row: &Row, cutoff: &str) -> bool {
    matches!(
        row.status,
        EffectStatus::Succeeded | EffectStatus::Cancelled
    ) && row
        .completed_at
        .as_deref()
        .is_some_and(|time| time < cutoff)
}

pub fn prune_indices(rows: &[Row], cutoff: &str, limit: usize) -> Vec<usize> {
    let mut eligible: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter_map(|(index, row)| prunable(row, cutoff).then_some(index))
        .collect();
    eligible.sort_by_key(|index| rows[*index].insertion_ordinal);
    eligible.truncate(limit.min(PRUNE_BATCH_SIZE));
    eligible
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: &str, thread: &str, order: u64, kind: EffectType) -> Row {
        Row {
            id: id.to_string(),
            thread_id: thread.to_string(),
            effect_type: kind,
            status: EffectStatus::Pending,
            insertion_ordinal: order,
            attempt_count: 0,
            available_at: "2026-11-09T00:00:00.000Z".to_string(),
            lease_owner: None,
            created_at: "2026-11-09T00:00:00.000Z".to_string(),
            updated_at: "2026-11-09T00:00:00.000Z".to_string(),
            completed_at: None,
            last_error: None,
        }
    }

    const NOW: &str = "2026-11-09T01:00:00.000Z";

    #[test]
    fn retry_head_blocks_later_work_but_not_another_thread_or_title() {
        let mut head = row("head", "a", 1, EffectType::ProviderThreadRollback);
        head.available_at = "2026-11-10T00:00:00.000Z".to_string();
        let later = row("later", "a", 2, EffectType::ProviderTurnStart);
        let title = row("title", "a", 3, EffectType::ThreadTitleGenerate);
        let other = row("other", "b", 4, EffectType::ProviderTurnStart);
        let rows = vec![head, later, title, other];
        assert!(!claimable(&rows[1], &rows, Some(NOW), false));
        assert_eq!(claim_next(&rows, NOW, false), Some(3));
        assert!(claimable(&rows[2], &rows, Some(NOW), false));
        assert_eq!(next_claimable_at(&rows), Some(rows[3].available_at.clone()));
    }

    #[test]
    fn running_lanes_do_not_expire_or_block_the_other_lane() {
        let mut running = row("running", "a", 2, EffectType::AttachmentCleanup);
        running.status = EffectStatus::Running;
        let pending = row("pending", "a", 1, EffectType::TerminalCleanup);
        let title = row("title", "a", 3, EffectType::ThreadTitleGenerate);
        let rows = vec![running, pending, title];
        assert_eq!(
            claim_next(&rows, "2099-01-01T00:00:00.000Z", false),
            Some(2)
        );
        assert!(!claimable(&rows[1], &rows, None, false));
    }

    #[test]
    fn excluded_pending_continuation_does_not_block_but_running_one_does() {
        let first = row("continue", "a", 1, EffectType::ProviderRuntimeContinue);
        let second = row("start", "a", 2, EffectType::ProviderTurnStart);
        let mut rows = vec![first, second];
        assert_eq!(claim_next(&rows, NOW, false), Some(0));
        assert_eq!(claim_next(&rows, NOW, true), Some(1));
        assert_eq!(next_claimable_at(&rows), Some(rows[0].available_at.clone()));
        rows[0].status = EffectStatus::Running;
        assert_eq!(claim_next(&rows, NOW, true), None);
    }

    #[test]
    fn selection_uses_available_then_created_then_id_not_insertion_order() {
        let mut first = row("z", "a", 1, EffectType::PreviewCleanup);
        let mut second = row("a", "b", 2, EffectType::PreviewCleanup);
        assert_eq!(
            claim_next(&[first.clone(), second.clone()], NOW, false),
            Some(1)
        );
        second.created_at = "2026-11-09T00:00:00.001Z".to_string();
        assert_eq!(
            claim_next(&[first.clone(), second.clone()], NOW, false),
            Some(0)
        );
        first.available_at = "2026-11-09T00:00:00.001Z".to_string();
        assert_eq!(claim_next(&[first, second], NOW, false), Some(1));
    }

    #[test]
    fn claim_and_lease_guards_preserve_unmodified_fields() {
        let original = row("a", "a", 1, EffectType::TerminalCleanup);
        let running = transition(
            &original,
            Action::Claim {
                worker_id: "w".to_string(),
            },
            NOW,
        )
        .unwrap()
        .unwrap();
        assert_eq!(running.attempt_count, 1);
        assert_eq!(running.available_at, original.available_at);
        assert_eq!(
            transition(
                &running,
                Action::Succeed {
                    worker_id: "wrong".to_string()
                },
                NOW
            ),
            Ok(None)
        );
        let succeeded = transition(
            &running,
            Action::Succeed {
                worker_id: "w".to_string(),
            },
            NOW,
        )
        .unwrap()
        .unwrap();
        assert_eq!(succeeded.status, EffectStatus::Succeeded);
        assert_eq!(succeeded.completed_at.as_deref(), Some(NOW));
        assert_eq!(succeeded.lease_owner, None);
        assert_eq!(
            transition(
                &succeeded,
                Action::Succeed {
                    worker_id: "w".to_string()
                },
                NOW
            ),
            Ok(None)
        );
    }

    #[test]
    fn retry_preserves_attempt_count_and_failure_is_terminal() {
        let pending = row("a", "a", 1, EffectType::CheckpointCapture);
        let running = transition(
            &pending,
            Action::Claim {
                worker_id: "w".to_string(),
            },
            NOW,
        )
        .unwrap()
        .unwrap();
        let retried = transition(
            &running,
            Action::Retry {
                worker_id: "w".to_string(),
                error: "busy".to_string(),
                available_at: NOW.to_string(),
            },
            NOW,
        )
        .unwrap()
        .unwrap();
        assert_eq!(retried.status, EffectStatus::Pending);
        assert_eq!(retried.attempt_count, 1);
        assert_eq!(retried.last_error.as_deref(), Some("busy"));
        let running = transition(
            &retried,
            Action::Claim {
                worker_id: "next".to_string(),
            },
            NOW,
        )
        .unwrap()
        .unwrap();
        assert_eq!(running.attempt_count, 2);
        assert_eq!(running.last_error, None);
        let failed = transition(
            &running,
            Action::Fail {
                worker_id: "next".to_string(),
                error: "failed".to_string(),
            },
            NOW,
        )
        .unwrap()
        .unwrap();
        assert_eq!(failed.status, EffectStatus::Failed);
        assert!(!prunable(&failed, "2099-01-01T00:00:00.000Z"));
        assert_eq!(retry_delay_ms(-1), 0);
        assert_eq!(retry_delay_ms(12), 12);
    }

    #[test]
    fn reconciliation_only_requeues_replay_safe_running_rows() {
        for kind in [
            EffectType::ProviderTurnStart,
            EffectType::ProviderTurnInterrupt,
            EffectType::ProviderTurnSteer,
            EffectType::ProviderTurnRestart,
            EffectType::RuntimeRequestRespond,
        ] {
            let pending = row("a", "a", 1, kind);
            let cancelled = transition(&pending, Action::Reconcile, NOW)
                .unwrap()
                .unwrap();
            assert_eq!(cancelled.status, EffectStatus::Cancelled);
            assert_eq!(
                cancelled.last_error.as_deref(),
                Some(PROCESS_LOSS_CANCEL_REASON)
            );
        }
        let pending = row("a", "a", 1, EffectType::ProviderRuntimeContinue);
        assert_eq!(transition(&pending, Action::Reconcile, NOW), Ok(None));
        let running = transition(
            &pending,
            Action::Claim {
                worker_id: "dead".to_string(),
            },
            NOW,
        )
        .unwrap()
        .unwrap();
        let requeued = transition(&running, Action::Reconcile, NOW)
            .unwrap()
            .unwrap();
        assert_eq!(requeued.status, EffectStatus::Pending);
        assert_eq!(requeued.attempt_count, 1);
        assert_eq!(
            requeued.last_error.as_deref(),
            Some(PROCESS_LOSS_REQUEUE_REASON)
        );
        assert_eq!(transition(&requeued, Action::Reconcile, NOW), Ok(None));
    }

    #[test]
    fn cancellation_filters_and_pruning_use_strict_cutoff() {
        let pending = row("a", "a", 1, EffectType::AttachmentCleanup);
        assert!(cancel_matches(
            &pending,
            "a",
            &[EffectType::AttachmentCleanup]
        ));
        assert!(!cancel_matches(
            &pending,
            "b",
            &[EffectType::AttachmentCleanup]
        ));
        let cancelled = transition(
            &pending,
            Action::Cancel {
                reason: "stop".to_string(),
            },
            NOW,
        )
        .unwrap()
        .unwrap();
        assert!(!prunable(&cancelled, NOW));
        assert!(prunable(&cancelled, "2026-11-09T01:00:00.001Z"));
        assert!(!cancel_matches(
            &cancelled,
            "a",
            &[EffectType::AttachmentCleanup]
        ));
    }

    #[test]
    fn claim_fails_closed_at_the_safe_counter_limit() {
        let mut pending = row("a", "a", 1, EffectType::TerminalCleanup);
        pending.attempt_count = MAX_COUNTER;
        assert_eq!(
            transition(
                &pending,
                Action::Claim {
                    worker_id: "w".to_string()
                },
                NOW
            ),
            Err(Error::CounterOverflow)
        );
    }
}
