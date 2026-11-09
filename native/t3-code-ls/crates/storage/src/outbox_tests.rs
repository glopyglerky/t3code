use super::*;
use crate::test_support::TestFixture;
use serde_json::json;

const NOW: &str = "2026-11-09T10:00:00.000Z";
const LATER: &str = "2026-11-09T10:01:00.000Z";
const FUTURE: &str = "2026-11-09T11:00:00.000Z";

fn effect(id: &str, thread: &str, kind: EffectType) -> EnqueueEffect {
    let request = match kind {
        EffectType::ProviderRuntimeContinue => json!({"type":kind.as_wire(), "sourceRunId":"run"}),
        EffectType::ProviderSessionDetach => {
            json!({"type":kind.as_wire(), "providerSessionId":"session"})
        }
        EffectType::ProviderTurnStart => json!({"type":kind.as_wire(), "runId":"run"}),
        EffectType::ProviderTurnInterrupt => {
            json!({"type":kind.as_wire(), "providerSessionId":"session", "providerThreadId":"provider-thread", "providerTurnId":"provider-turn"})
        }
        EffectType::ProviderTurnSteer => {
            json!({"type":kind.as_wire(), "providerSessionId":"session", "providerThreadId":"provider-thread", "providerTurnId":"provider-turn", "messageId":"message"})
        }
        EffectType::ProviderTurnRestart => {
            json!({"type":kind.as_wire(), "providerSessionId":"session", "providerThreadId":"provider-thread", "providerTurnId":"provider-turn", "interruptedAttemptId":"attempt", "runId":"run"})
        }
        EffectType::RuntimeRequestRespond => {
            json!({"type":kind.as_wire(), "providerSessionId":"session", "requestId":"request"})
        }
        EffectType::ProviderThreadRollback => {
            json!({"type":kind.as_wire(), "providerThreadId":"provider-thread", "checkpointId":"checkpoint", "scopeId":"scope"})
        }
        EffectType::CheckpointCapture => {
            json!({"type":kind.as_wire(), "runId":"run", "scopeId":"scope"})
        }
        EffectType::AttachmentCleanup => {
            json!({"type":kind.as_wire(), "attachmentIds":["attachment"]})
        }
        EffectType::ThreadTitleGenerate => {
            json!({"type":kind.as_wire(), "kind":{"type":"regenerate"}})
        }
        _ => json!({"type":kind.as_wire()}),
    };
    EnqueueEffect {
        id: id.into(),
        command_id: "command".into(),
        thread_id: thread.into(),
        request,
        available_at: None,
    }
}

fn claim(worker: &str, now: &str) -> Claim {
    Claim {
        worker_id: worker.into(),
        now: now.into(),
        exclude_restart_continuations: false,
    }
}

fn settle(id: &str, worker: &str, now: &str) -> Settle {
    Settle {
        effect_id: id.into(),
        worker_id: worker.into(),
        now: now.into(),
    }
}

async fn row(store: &Store, id: &str) -> OutboxEffect {
    store.outbox_get(id).await.unwrap().unwrap()
}

#[tokio::test]
async fn enqueue_replays_effect_ids_without_replacing_payload_or_consuming_ordinals() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let first = effect("same", "thread", EffectType::TerminalCleanup);
    let mut duplicate = effect("same", "other-thread", EffectType::PreviewCleanup);
    duplicate.command_id = "different-command".into();
    assert_eq!(
        store
            .outbox_enqueue(&[first.clone(), duplicate.clone()], NOW)
            .await
            .unwrap(),
        1
    );
    let original = row(&store, "same").await;
    assert_eq!(store.outbox_enqueue(&[duplicate], LATER).await.unwrap(), 0);
    assert_eq!(row(&store, "same").await, original);
    assert_eq!(
        store
            .outbox_list_by_command("different-command")
            .await
            .unwrap(),
        vec![]
    );
    store
        .outbox_enqueue(
            &[effect("next", "thread", EffectType::TerminalCleanup)],
            LATER,
        )
        .await
        .unwrap();
    assert_eq!(row(&store, "next").await.insertion_ordinal, 2);
    assert_eq!(original.request, first.request);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn claim_orders_deadlines_then_creation_then_id_without_bypassing_fifo() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut blocked = effect(
        "first-in-thread",
        "same-thread",
        EffectType::TerminalCleanup,
    );
    blocked.available_at = Some(FUTURE.into());
    let same_lane = effect(
        "earlier-deadline",
        "same-thread",
        EffectType::PreviewCleanup,
    );
    let title = effect("title", "same-thread", EffectType::ThreadTitleGenerate);
    let a = effect("a", "thread-a", EffectType::TerminalCleanup);
    let b = effect("b", "thread-b", EffectType::TerminalCleanup);
    store
        .outbox_enqueue(&[blocked, same_lane, title, b, a], NOW)
        .await
        .unwrap();
    assert_eq!(
        store
            .outbox_claim_next(&claim("worker-a", NOW))
            .await
            .unwrap()
            .unwrap()
            .id,
        "a"
    );
    assert_eq!(
        store
            .outbox_claim_next(&claim("worker-b", NOW))
            .await
            .unwrap()
            .unwrap()
            .id,
        "b"
    );
    assert_eq!(
        store
            .outbox_claim_next(&claim("worker-title", NOW))
            .await
            .unwrap()
            .unwrap()
            .id,
        "title"
    );
    assert!(
        store
            .outbox_claim_next(&claim("worker", NOW))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.outbox_next_claimable_at().await.unwrap(),
        Some(FUTURE.into())
    );
    assert_eq!(
        store
            .outbox_claim_next(&claim("worker", FUTURE))
            .await
            .unwrap()
            .unwrap()
            .id,
        "first-in-thread"
    );
    assert_eq!(store.outbox_next_claimable_at().await.unwrap(), None);
    assert!(
        store
            .outbox_succeed(&settle("first-in-thread", "worker", FUTURE))
            .await
            .unwrap()
    );
    assert_eq!(
        store
            .outbox_claim_next(&claim("worker", FUTURE))
            .await
            .unwrap()
            .unwrap()
            .id,
        "earlier-deadline"
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn continuation_exclusion_ignores_pending_continuations_but_not_running_ones() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    store
        .outbox_enqueue(
            &[
                effect("continue", "thread", EffectType::ProviderRuntimeContinue),
                effect("cleanup", "thread", EffectType::TerminalCleanup),
            ],
            NOW,
        )
        .await
        .unwrap();
    let mut excluded = claim("worker", NOW);
    excluded.exclude_restart_continuations = true;
    assert_eq!(
        store
            .outbox_claim_next(&excluded)
            .await
            .unwrap()
            .unwrap()
            .id,
        "cleanup"
    );
    assert!(
        store
            .outbox_succeed(&settle("cleanup", "worker", LATER))
            .await
            .unwrap()
    );
    assert!(store.outbox_claim_next(&excluded).await.unwrap().is_none());
    assert_eq!(
        store
            .outbox_claim_next(&claim("continue-worker", NOW))
            .await
            .unwrap()
            .unwrap()
            .id,
        "continue"
    );
    store
        .outbox_enqueue(
            &[effect("after", "thread", EffectType::TerminalCleanup)],
            LATER,
        )
        .await
        .unwrap();
    excluded.now = FUTURE.into();
    assert!(store.outbox_claim_next(&excluded).await.unwrap().is_none());
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn owner_guards_and_terminal_states_do_not_accept_stale_workers() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    store
        .outbox_enqueue(
            &[effect("effect", "thread", EffectType::TerminalCleanup)],
            NOW,
        )
        .await
        .unwrap();
    let pending = row(&store, "effect").await;
    assert!(
        !store
            .outbox_succeed(&settle("effect", "worker", NOW))
            .await
            .unwrap()
    );
    assert_eq!(row(&store, "effect").await, pending);
    let claimed = store
        .outbox_claim_next(&claim("owner", NOW))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.attempt_count, 1);
    assert_eq!(claimed.lease_expires_at, None);
    assert!(
        !store
            .outbox_succeed(&settle("effect", "wrong", LATER))
            .await
            .unwrap()
    );
    assert!(
        !store
            .outbox_fail(&Failure {
                effect_id: "effect".into(),
                worker_id: "wrong".into(),
                now: LATER.into(),
                error: "wrong".into()
            })
            .await
            .unwrap()
    );
    assert_eq!(row(&store, "effect").await, claimed);
    assert!(
        store
            .outbox_succeed(&settle("effect", "owner", LATER))
            .await
            .unwrap()
    );
    assert!(
        !store
            .outbox_succeed(&settle("effect", "owner", FUTURE))
            .await
            .unwrap()
    );
    let completed = row(&store, "effect").await;
    assert_eq!(completed.status, EffectStatus::Succeeded);
    assert_eq!(completed.completed_at, Some(LATER.into()));
    assert_eq!(completed.lease_owner, None);
    assert_eq!(completed.last_error, None);
    assert!(
        !store
            .outbox_succeed(&settle("missing", "owner", LATER))
            .await
            .unwrap()
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn retry_preserves_fifo_and_reclaims_only_after_its_explicit_deadline() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    store
        .outbox_enqueue(
            &[
                effect("first", "thread", EffectType::TerminalCleanup),
                effect("second", "thread", EffectType::PreviewCleanup),
            ],
            NOW,
        )
        .await
        .unwrap();
    store
        .outbox_claim_next(&claim("owner", NOW))
        .await
        .unwrap()
        .unwrap();
    let mut retry = Retry {
        effect_id: "first".into(),
        worker_id: "wrong".into(),
        now: LATER.into(),
        available_at: FUTURE.into(),
        error: "backoff".into(),
    };
    assert!(!store.outbox_retry(&retry).await.unwrap());
    retry.worker_id = "owner".into();
    assert!(store.outbox_retry(&retry).await.unwrap());
    let pending = row(&store, "first").await;
    assert_eq!(pending.status, EffectStatus::Pending);
    assert_eq!(pending.attempt_count, 1);
    assert_eq!(pending.lease_owner, None);
    assert_eq!(pending.last_error, Some("backoff".into()));
    assert!(
        store
            .outbox_claim_next(&claim("second-owner", LATER))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store.outbox_next_claimable_at().await.unwrap(),
        Some(FUTURE.into())
    );
    let running = store
        .outbox_claim_next(&claim("second-owner", FUTURE))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(running.id, "first");
    assert_eq!(running.attempt_count, 2);
    assert_eq!(running.last_error, None);
    assert!(
        !store
            .outbox_succeed(&settle("first", "owner", FUTURE))
            .await
            .unwrap()
    );
    retry.now = FUTURE.into();
    retry.available_at = NOW.into();
    retry.worker_id = "second-owner".into();
    assert!(matches!(
        store.outbox_retry(&retry).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(row(&store, "first").await, running);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_is_thread_and_type_scoped_and_preserves_failed_rows() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    store
        .outbox_enqueue(
            &[
                effect("running", "thread", EffectType::TerminalCleanup),
                effect("pending", "thread", EffectType::PreviewCleanup),
                effect("title", "thread", EffectType::ThreadTitleGenerate),
                effect("outside", "outside", EffectType::TerminalCleanup),
            ],
            NOW,
        )
        .await
        .unwrap();
    // Global tie ordering gives outside, then running. The title lane is independent.
    assert_eq!(
        store
            .outbox_claim_next(&claim("outside-worker", NOW))
            .await
            .unwrap()
            .unwrap()
            .id,
        "outside"
    );
    assert_eq!(
        store
            .outbox_claim_next(&claim("worker", NOW))
            .await
            .unwrap()
            .unwrap()
            .id,
        "running"
    );
    assert_eq!(
        store
            .outbox_claim_next(&claim("title-worker", NOW))
            .await
            .unwrap()
            .unwrap()
            .id,
        "title"
    );
    assert!(
        store
            .outbox_fail(&Failure {
                effect_id: "title".into(),
                worker_id: "title-worker".into(),
                now: LATER.into(),
                error: "keep this".into()
            })
            .await
            .unwrap()
    );
    let cancel = CancelUnsettled {
        thread_id: "thread".into(),
        effect_types: vec![
            EffectType::TerminalCleanup,
            EffectType::PreviewCleanup,
            EffectType::ThreadTitleGenerate,
        ],
        reason: "deleted".into(),
        now: LATER.into(),
    };
    assert_eq!(
        store.outbox_cancel_unsettled(&cancel).await.unwrap(),
        vec!["running", "pending"]
    );
    assert_eq!(
        store.outbox_cancel_unsettled(&cancel).await.unwrap(),
        Vec::<String>::new()
    );
    for id in ["running", "pending"] {
        let value = row(&store, id).await;
        assert_eq!(value.status, EffectStatus::Cancelled);
        assert_eq!(value.completed_at, Some(LATER.into()));
        assert_eq!(value.lease_owner, None);
        assert_eq!(value.last_error, Some("deleted".into()));
    }
    assert_eq!(row(&store, "title").await.status, EffectStatus::Failed);
    assert_eq!(row(&store, "outside").await.status, EffectStatus::Running);
    assert!(
        !store
            .outbox_succeed(&settle("running", "worker", FUTURE))
            .await
            .unwrap()
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn restart_reconciliation_matches_all_fifteen_effect_type_policies() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let kinds = [
        EffectType::ProviderRuntimeContinue,
        EffectType::ProviderSessionDetach,
        EffectType::ProviderTurnStart,
        EffectType::ProviderTurnInterrupt,
        EffectType::ProviderTurnSteer,
        EffectType::ProviderTurnRestart,
        EffectType::RuntimeRequestRespond,
        EffectType::ProviderThreadRollback,
        EffectType::CheckpointCapture,
        EffectType::ScheduledTasksCleanup,
        EffectType::TerminalCleanup,
        EffectType::PreviewCleanup,
        EffectType::AttachmentCleanup,
        EffectType::ThreadTitleGenerate,
        EffectType::DelegatedTasksStop,
    ];
    let mut all = Vec::new();
    for (index, kind) in kinds.iter().enumerate() {
        let running = format!("r-{index:02}");
        all.push(effect(&running, &running, *kind));
    }
    store.outbox_enqueue(&all, NOW).await.unwrap();
    for _ in 0..kinds.len() {
        store
            .outbox_claim_next(&claim("owner", NOW))
            .await
            .unwrap()
            .unwrap();
    }
    let pending: Vec<_> = kinds
        .iter()
        .enumerate()
        .map(|(index, kind)| {
            let id = format!("p-{index:02}");
            effect(&id, &id, *kind)
        })
        .collect();
    store.outbox_enqueue(&pending, NOW).await.unwrap();
    let outcome = store
        .outbox_reconcile(&Reconcile { now: LATER.into() })
        .await
        .unwrap();
    assert_eq!(
        outcome,
        ReconcileOutcome {
            requeued: 10,
            cancelled: 10
        }
    );
    for (index, kind) in kinds.iter().enumerate() {
        let process_bound = policy::EffectType::from_wire(kind.as_wire())
            .unwrap()
            .is_process_bound();
        for prefix in ["p", "r"] {
            let value = row(&store, &format!("{prefix}-{index:02}")).await;
            assert_eq!(
                value.status,
                if process_bound {
                    EffectStatus::Cancelled
                } else {
                    EffectStatus::Pending
                }
            );
            assert_eq!(value.attempt_count, u64::from(prefix == "r"));
            assert_eq!(value.lease_owner, None);
            if process_bound {
                assert_eq!(
                    value.last_error.as_deref(),
                    Some(policy::PROCESS_LOSS_CANCEL_REASON)
                );
                assert_eq!(value.completed_at, Some(LATER.into()));
            } else if prefix == "r" {
                assert_eq!(
                    value.last_error.as_deref(),
                    Some(policy::PROCESS_LOSS_REQUEUE_REASON)
                );
                assert_eq!(value.available_at, LATER);
            } else {
                assert_eq!(value.updated_at, NOW);
                assert_eq!(value.last_error, None);
            }
        }
    }
    assert_eq!(
        store
            .outbox_reconcile(&Reconcile { now: FUTURE.into() })
            .await
            .unwrap(),
        ReconcileOutcome {
            requeued: 0,
            cancelled: 0
        }
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn pruning_keeps_failed_unsettled_and_exact_cutoff_rows() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    store
        .outbox_enqueue(
            &[
                effect("a-success", "a", EffectType::TerminalCleanup),
                effect("b-failed", "b", EffectType::TerminalCleanup),
                effect("c-cancelled", "c", EffectType::TerminalCleanup),
                effect("d-pending", "d", EffectType::TerminalCleanup),
            ],
            NOW,
        )
        .await
        .unwrap();
    store
        .outbox_claim_next(&claim("a", NOW))
        .await
        .unwrap()
        .unwrap();
    store
        .outbox_succeed(&settle("a-success", "a", LATER))
        .await
        .unwrap();
    store
        .outbox_claim_next(&claim("b", NOW))
        .await
        .unwrap()
        .unwrap();
    store
        .outbox_fail(&Failure {
            effect_id: "b-failed".into(),
            worker_id: "b".into(),
            now: LATER.into(),
            error: "retained".into(),
        })
        .await
        .unwrap();
    store
        .outbox_cancel_unsettled(&CancelUnsettled {
            thread_id: "c".into(),
            effect_types: vec![EffectType::TerminalCleanup],
            reason: "cancel".into(),
            now: LATER.into(),
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .outbox_prune(&Prune {
                before: LATER.into(),
                limit: 500
            })
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        store
            .outbox_prune(&Prune {
                before: FUTURE.into(),
                limit: 1
            })
            .await
            .unwrap(),
        1
    );
    assert!(store.outbox_get("a-success").await.unwrap().is_none());
    assert_eq!(
        store
            .outbox_prune(&Prune {
                before: FUTURE.into(),
                limit: 500
            })
            .await
            .unwrap(),
        1
    );
    assert!(store.outbox_get("c-cancelled").await.unwrap().is_none());
    assert_eq!(row(&store, "b-failed").await.status, EffectStatus::Failed);
    assert_eq!(row(&store, "d-pending").await.status, EffectStatus::Pending);
    assert_eq!(
        store.outbox_list_by_command("command").await.unwrap().len(),
        2
    );
    store
        .outbox_enqueue(
            &[effect("a-success", "a", EffectType::TerminalCleanup)],
            FUTURE,
        )
        .await
        .unwrap();
    assert_eq!(row(&store, "a-success").await.insertion_ordinal, 5);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn independent_writers_cannot_claim_the_same_snapshot_effect() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    store
        .outbox_enqueue(&[effect("one", "thread", EffectType::TerminalCleanup)], NOW)
        .await
        .unwrap();
    let other = fixture.connect().await;
    let mut a = store.db.begin().await.unwrap();
    let mut b = other.db.begin().await.unwrap();
    let a_row = candidate(&mut a, Some(NOW), false).await.unwrap().unwrap();
    let b_row = candidate(&mut b, Some(NOW), false).await.unwrap().unwrap();
    transition(
        &mut a,
        &a_row,
        policy::Action::Claim {
            worker_id: "a".into(),
        },
        NOW,
    )
    .await
    .unwrap();
    transition(
        &mut b,
        &b_row,
        policy::Action::Claim {
            worker_id: "b".into(),
        },
        NOW,
    )
    .await
    .unwrap();
    a.commit().await.unwrap();
    assert!(matches!(b.commit().await, Err(Error::TransactionConflict)));
    assert_eq!(row(&other, "one").await.lease_owner, Some("a".into()));
    other.shutdown().await.unwrap();
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn a_concurrent_enqueue_changes_the_claim_range_and_conflicts() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    store
        .outbox_enqueue(&[effect("z", "z-thread", EffectType::TerminalCleanup)], NOW)
        .await
        .unwrap();
    let mut transaction = store.db.begin().await.unwrap();
    let selected = candidate(&mut transaction, Some(NOW), false)
        .await
        .unwrap()
        .unwrap();
    transition(
        &mut transaction,
        &selected,
        policy::Action::Claim {
            worker_id: "stale".into(),
        },
        NOW,
    )
    .await
    .unwrap();
    store
        .outbox_enqueue(&[effect("a", "a-thread", EffectType::TerminalCleanup)], NOW)
        .await
        .unwrap();
    assert!(matches!(
        transaction.commit().await,
        Err(Error::TransactionConflict)
    ));
    assert_eq!(row(&store, "z").await.status, EffectStatus::Pending);
    assert_eq!(
        store
            .outbox_claim_next(&claim("owner", NOW))
            .await
            .unwrap()
            .unwrap()
            .id,
        "a"
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn staged_cancellation_and_enqueue_disappear_on_rollback() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    store
        .outbox_enqueue(&[effect("old", "thread", EffectType::TerminalCleanup)], NOW)
        .await
        .unwrap();
    let before = row(&store, "old").await;
    let mut tx = store.db.begin().await.unwrap();
    stage_enqueue(
        &mut tx,
        &[effect("new", "thread", EffectType::PreviewCleanup)],
        LATER,
    )
    .await
    .unwrap();
    assert_eq!(
        stage_cancel_unsettled(
            &mut tx,
            &CancelUnsettled {
                thread_id: "thread".into(),
                effect_types: vec![EffectType::TerminalCleanup, EffectType::PreviewCleanup],
                reason: "abort".into(),
                now: LATER.into()
            }
        )
        .await
        .unwrap(),
        vec!["old", "new"]
    );
    tx.rollback().await.unwrap();
    assert_eq!(row(&store, "old").await, before);
    assert!(store.outbox_get("new").await.unwrap().is_none());
    store
        .outbox_enqueue(
            &[effect("new", "thread", EffectType::PreviewCleanup)],
            LATER,
        )
        .await
        .unwrap();
    assert_eq!(row(&store, "new").await.insertion_ordinal, 2);
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn late_outbox_exhaustion_rolls_back_events_projections_receipts_and_first_enqueue() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut tx = store.db.begin().await.unwrap();
    tx.put(key("counter"), crate::encode(&(MAX_COUNTER - 1)).unwrap())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let batch = crate::CommitBatch {
        command_id: "atomic-overflow".into(),
        request_digest: crate::request_digest(&json!({"command":"atomic-overflow"})),
        expected_revision: 0,
        events: vec![crate::Record {
            id: "event".into(),
            payload: json!({"test":true}),
        }],
        projections: vec![crate::ProjectionMutation::Put {
            key: "test/projection".into(),
            payload: json!({"test":true}),
        }],
        effects: vec![],
        result: json!({"accepted":true}),
    };
    let outbox = crate::OutboxBatch {
        enqueue: vec![
            effect("first", "thread", EffectType::TerminalCleanup),
            effect("overflow", "other", EffectType::PreviewCleanup),
        ],
        enqueue_at: NOW.into(),
        cancel: vec![],
    };
    assert!(matches!(
        store.commit_outbox(&batch, &outbox).await,
        Err(Error::Invalid(_))
    ));
    let snapshot = store.snapshot().await.unwrap();
    assert_eq!(snapshot.revision, 0);
    assert_eq!(snapshot.last_sequence, 0);
    assert!(snapshot.projections.is_empty());
    assert!(store.events(0, 100).await.unwrap().is_empty());
    assert!(
        store
            .read_receipt("atomic-overflow")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .read_outbox_receipt("atomic-overflow")
            .await
            .unwrap()
            .is_none()
    );
    assert!(store.outbox_get("first").await.unwrap().is_none());
    assert!(store.outbox_get("overflow").await.unwrap().is_none());
    assert_eq!(
        crate::decode::<u64>(&store.db.read(&key("counter")).await.unwrap().unwrap()).unwrap(),
        MAX_COUNTER - 1
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn reopen_preserves_owner_and_time_alone_never_reclaims_running_work() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    store
        .outbox_enqueue(
            &[effect("effect", "thread", EffectType::TerminalCleanup)],
            NOW,
        )
        .await
        .unwrap();
    let claimed = store
        .outbox_claim_next(&claim("owner", NOW))
        .await
        .unwrap()
        .unwrap();
    store.shutdown().await.unwrap();
    let reopened = fixture.connect().await;
    assert_eq!(row(&reopened, "effect").await, claimed);
    assert!(
        reopened
            .outbox_claim_next(&claim("new-owner", "2028-11-09T00:00:00.000Z"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        reopened
            .outbox_reconcile(&Reconcile { now: FUTURE.into() })
            .await
            .unwrap(),
        ReconcileOutcome {
            requeued: 1,
            cancelled: 0
        }
    );
    assert_eq!(
        reopened
            .outbox_claim_next(&claim("new-owner", FUTURE))
            .await
            .unwrap()
            .unwrap()
            .attempt_count,
        2
    );
    assert!(
        !reopened
            .outbox_succeed(&settle("effect", "owner", FUTURE))
            .await
            .unwrap()
    );
    reopened.shutdown().await.unwrap();
}

#[tokio::test]
async fn command_indexes_preserve_binary_unicode_order_and_escape_component_boundaries() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let ids = ["😀", "é", "a/b", "a", "a0", "a/../b"];
    let mut effects: Vec<_> = ids
        .iter()
        .map(|id| effect(id, id, EffectType::TerminalCleanup))
        .collect();
    effects[0].command_id = "command/child".into();
    store.outbox_enqueue(&effects, NOW).await.unwrap();
    let rows = store.outbox_list_by_command("command").await.unwrap();
    let mut expected = ids[1..].to_vec();
    expected.sort();
    assert_eq!(
        rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        expected
    );
    assert_eq!(
        store
            .outbox_list_by_command("command/child")
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        store
            .outbox_claim_next(&claim("worker", NOW))
            .await
            .unwrap()
            .unwrap()
            .id,
        "a"
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn indexed_pages_do_not_skip_rows_when_cancellation_and_pruning_delete_entries() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let effects: Vec<_> = (0..(PAGE_SIZE + 3))
        .map(|index| {
            effect(
                &format!("effect-{index:03}"),
                "thread",
                EffectType::TerminalCleanup,
            )
        })
        .collect();
    store.outbox_enqueue(&effects, NOW).await.unwrap();
    assert_eq!(
        store.outbox_list_by_command("command").await.unwrap().len(),
        effects.len()
    );
    let cancelled = store
        .outbox_cancel_unsettled(&CancelUnsettled {
            thread_id: "thread".into(),
            effect_types: vec![EffectType::TerminalCleanup],
            reason: "delete".into(),
            now: LATER.into(),
        })
        .await
        .unwrap();
    assert_eq!(cancelled.len(), effects.len());
    assert_eq!(
        store
            .outbox_prune(&Prune {
                before: FUTURE.into(),
                limit: 500
            })
            .await
            .unwrap(),
        effects.len()
    );
    assert!(
        store
            .outbox_list_by_command("command")
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .outbox_claim_next(&claim("worker", FUTURE))
            .await
            .unwrap()
            .is_none()
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn corrupt_index_rows_and_attempt_exhaustion_fail_without_partial_mutations() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    store
        .outbox_enqueue(
            &[effect("effect", "thread", EffectType::TerminalCleanup)],
            NOW,
        )
        .await
        .unwrap();
    let original = row(&store, "effect").await;
    let mut at_limit = original.clone();
    at_limit.attempt_count = MAX_COUNTER;
    let mut tx = store.db.begin().await.unwrap();
    save_row(&mut tx, Some(&original), &at_limit).await.unwrap();
    tx.commit().await.unwrap();
    assert!(matches!(
        store.outbox_claim_next(&claim("worker", NOW)).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(row(&store, "effect").await, at_limit);
    let mut tx = store.db.begin().await.unwrap();
    let pending_key = index_keys(&at_limit)
        .unwrap()
        .into_iter()
        .find(|key| key.starts_with(&super::key("pending/")))
        .unwrap();
    tx.put(pending_key, b"missing".to_vec()).await.unwrap();
    tx.commit().await.unwrap();
    assert!(matches!(
        store.outbox_claim_next(&claim("worker", NOW)).await,
        Err(Error::IncompatibleStore)
    ));
    store.shutdown().await.unwrap();
}

#[test]
fn rejects_noncanonical_or_impossible_times_but_accepts_real_leap_days() {
    for invalid in [
        "2026-11-09T10:00:00Z",
        "2026-11-09T10:00:00.000+00:00",
        "2026-02-29T10:00:00.000Z",
        "1900-02-29T10:00:00.000Z",
        "2026-04-31T10:00:00.000Z",
        "2026-13-01T10:00:00.000Z",
        "2026-01-00T10:00:00.000Z",
        "2026-11-09T24:00:00.000Z",
        "2026-11-09T10:00:60.000Z",
        "",
        "😀",
    ] {
        assert!(validate_timestamp(invalid).is_err(), "{invalid}");
    }
    for valid in [
        NOW,
        "2000-02-29T10:00:00.000Z",
        "2028-02-29T23:59:59.999Z",
        "0000-01-01T00:00:00.000Z",
    ] {
        validate_timestamp(valid).unwrap();
    }
}

#[tokio::test]
async fn malformed_payloads_and_bounded_inputs_leave_database_unchanged() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut input = effect("effect", "thread", EffectType::TerminalCleanup);
    for request in [
        json!({"type":"unknown"}),
        json!({"type":"provider-turn.start"}),
        json!({"type":"provider-turn.start","runId":null}),
        json!({"type":"attachment.cleanup","attachmentIds":[1]}),
        json!({"type":"thread-title.generate","kind":{"type":"initial"}}),
        json!({"type":"runtime-request.respond","providerSessionId":"session","requestId":"request","decision":"approve"}),
    ] {
        input.request = request;
        assert!(matches!(
            store.outbox_enqueue(&[input.clone()], NOW).await,
            Err(Error::Invalid(_))
        ));
    }
    for decision in [
        "accept",
        "acceptForSession",
        "acceptAlways",
        "decline",
        "cancel",
    ] {
        input.request = json!({"type":"runtime-request.respond","providerSessionId":"session","requestId":"request","decision":decision});
        validate_enqueue(&[input.clone()], NOW).unwrap();
    }
    input.id = "x".repeat(crate::MAX_ID_BYTES + 1);
    assert!(validate_enqueue(&[input], NOW).is_err());
    assert!(
        store
            .outbox_prune(&Prune {
                before: NOW.into(),
                limit: 0
            })
            .await
            .is_err()
    );
    assert!(
        store
            .outbox_cancel_unsettled(&CancelUnsettled {
                thread_id: "thread".into(),
                effect_types: vec![],
                reason: "x".repeat(MAX_ERROR_BYTES + 1),
                now: NOW.into()
            })
            .await
            .is_err()
    );
    assert!(store.outbox_get("effect").await.unwrap().is_none());
    assert!(store.db.read(&key("counter")).await.unwrap().is_none());
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn request_encoding_strips_extras_trims_only_branded_ids_and_preserves_answer_values() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let requests = [
        (
            json!({"type":"terminal.cleanup", "detail":null, "runId":12}),
            json!({"type":"terminal.cleanup"}),
        ),
        (
            json!({"type":"provider-session.detach", "providerSessionId":"\u{feff} session\u{a0}", "detail":"  keep  ", "extra":true}),
            json!({"type":"provider-session.detach", "providerSessionId":"session", "detail":"  keep  "}),
        ),
        (
            json!({"type":"provider-turn.start", "runId":"\u{85}run\u{85}"}),
            json!({"type":"provider-turn.start", "runId":"\u{85}run\u{85}"}),
        ),
        (
            json!({"type":"provider-turn.interrupt", "providerSessionId":" session ", "providerThreadId":" thread ", "providerTurnId":" turn ", "subagent":{"id":"\u{feff} node ","nativeTaskId":"  raw task  ","extra":null}}),
            json!({"type":"provider-turn.interrupt", "providerSessionId":"session", "providerThreadId":"thread", "providerTurnId":"turn", "subagent":{"id":"node","nativeTaskId":"  raw task  "}}),
        ),
        (
            json!({"type":"provider-turn.restart", "providerSessionId":"s", "providerThreadId":"t", "providerTurnId":"p", "interruptedAttemptId":"a", "runId":"r", "sessionTransition":{"type":"replace","replacementProviderSessionId":"\u{feff} next\u{a0}","extra":"discard"}}),
            json!({"type":"provider-turn.restart", "providerSessionId":"s", "providerThreadId":"t", "providerTurnId":"p", "interruptedAttemptId":"a", "runId":"r", "sessionTransition":{"type":"replace","replacementProviderSessionId":"next"}}),
        ),
        (
            json!({"type":"thread-title.generate", "kind":{"type":"initial","messageId":"\u{feff} message ","extra":false}}),
            json!({"type":"thread-title.generate", "kind":{"type":"initial","messageId":"message"}}),
        ),
        (
            json!({"type":"thread-title.generate", "kind":{"type":"regenerate","messageId":null}}),
            json!({"type":"thread-title.generate", "kind":{"type":"regenerate"}}),
        ),
        (
            json!({"type":"attachment.cleanup", "attachmentIds":["  raw  ","\u{feff}",""]}),
            json!({"type":"attachment.cleanup", "attachmentIds":["  raw  ","\u{feff}",""]}),
        ),
        (
            json!({"type":"runtime-request.respond", "providerSessionId":" s ", "requestId":" r ", "answers":{" arbitrary ":{"type":"unknown","detail":null,"values":[null,42,"  unchanged  "]}}, "extra":false}),
            json!({"type":"runtime-request.respond", "providerSessionId":"s", "requestId":"r", "answers":{" arbitrary ":{"type":"unknown","detail":null,"values":[null,42,"  unchanged  "]}}}),
        ),
    ];
    for (index, (request, expected)) in requests.into_iter().enumerate() {
        let mut input = effect(
            &format!("case-{index}"),
            "thread",
            EffectType::TerminalCleanup,
        );
        input.request = request;
        store.outbox_enqueue(&[input.clone()], NOW).await.unwrap();
        assert_eq!(row(&store, &input.id).await.request, expected);
    }
    let mut blank = effect("blank", "thread", EffectType::ProviderTurnStart);
    blank.request["runId"] = json!("\u{feff} \u{a0}");
    assert!(validate_enqueue(&[blank], NOW).is_err());
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn cancellation_reason_amplification_is_bounded_and_rolls_back_every_row() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let effects: Vec<_> = (0..300)
        .map(|index| {
            effect(
                &format!("effect-{index:03}"),
                "thread",
                if index < 150 {
                    EffectType::TerminalCleanup
                } else {
                    EffectType::PreviewCleanup
                },
            )
        })
        .collect();
    store.outbox_enqueue(&effects, NOW).await.unwrap();
    let before = store.outbox_list_by_command("command").await.unwrap();
    let outcome = store
        .outbox_cancel_unsettled(&CancelUnsettled {
            thread_id: "thread".into(),
            effect_types: vec![EffectType::TerminalCleanup, EffectType::PreviewCleanup],
            reason: "x".repeat(MAX_ERROR_BYTES),
            now: LATER.into(),
        })
        .await;
    assert!(matches!(outcome, Err(Error::Invalid(_))));
    assert_eq!(
        store.outbox_list_by_command("command").await.unwrap(),
        before
    );
    let batch = crate::CommitBatch {
        command_id: "amplified-command".into(),
        request_digest: crate::request_digest(&json!({"amplification": true})),
        expected_revision: 0,
        events: vec![crate::Record {
            id: "amplified-event".into(),
            payload: json!({"type": "test"}),
        }],
        projections: vec![],
        effects: vec![],
        result: json!(null),
    };
    let outbox = crate::OutboxBatch {
        enqueue: vec![],
        enqueue_at: NOW.into(),
        cancel: [EffectType::TerminalCleanup, EffectType::PreviewCleanup]
            .into_iter()
            .map(|kind| CancelUnsettled {
                thread_id: "thread".into(),
                effect_types: vec![kind],
                reason: "x".repeat(MAX_ERROR_BYTES),
                now: LATER.into(),
            })
            .collect(),
    };
    assert!(matches!(
        store.commit_outbox(&batch, &outbox).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(
        store.outbox_list_by_command("command").await.unwrap(),
        before
    );
    assert_eq!(store.snapshot().await.unwrap().revision, 0);
    assert!(store.events(0, 100).await.unwrap().is_empty());
    assert!(
        store
            .read_receipt("amplified-command")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .outbox_claim_next(&claim("owner", NOW))
            .await
            .unwrap()
            .unwrap()
            .id,
        "effect-000"
    );
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn combined_cancellations_share_the_transaction_read_budget() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    for index in 0..2 {
        let mut entry = effect(
            &format!("large-effect-{index}"),
            &format!("large-thread-{index}"),
            EffectType::ProviderSessionDetach,
        );
        entry.request["detail"] = json!("x".repeat(9 * 1024 * 1024));
        assert_eq!(store.outbox_enqueue(&[entry], NOW).await.unwrap(), 1);
    }
    let before = store.snapshot().await.unwrap();
    let batch = crate::CommitBatch {
        command_id: "read-budget-command".into(),
        request_digest: crate::request_digest(&json!({"read_budget": true})),
        expected_revision: before.revision,
        events: vec![crate::Record {
            id: "read-budget-event".into(),
            payload: json!({"type": "test"}),
        }],
        projections: vec![crate::ProjectionMutation::Put {
            key: "read-budget/projection".into(),
            payload: json!({"changed": true}),
        }],
        effects: vec![],
        result: json!(null),
    };
    let outbox = crate::OutboxBatch {
        enqueue: vec![],
        enqueue_at: NOW.into(),
        cancel: (0..2)
            .map(|index| CancelUnsettled {
                thread_id: format!("large-thread-{index}"),
                effect_types: vec![EffectType::TerminalCleanup],
                reason: "No matching effects".into(),
                now: LATER.into(),
            })
            .collect(),
    };
    assert!(matches!(
        store.commit_outbox(&batch, &outbox).await,
        Err(Error::Invalid(_))
    ));
    assert_eq!(store.snapshot().await.unwrap(), before);
    assert!(store.events(0, 100).await.unwrap().is_empty());
    assert!(
        store
            .read_receipt(&batch.command_id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .read_outbox_receipt(&batch.command_id)
            .await
            .unwrap()
            .is_none()
    );
    for index in 0..2 {
        let persisted = row(&store, &format!("large-effect-{index}")).await;
        assert_eq!(persisted.status, EffectStatus::Pending);
        assert_eq!(persisted.updated_at, NOW);
        assert_eq!(
            persisted.request["detail"].as_str().unwrap().len(),
            9 * 1024 * 1024
        );
    }
    store.shutdown().await.unwrap();
}

#[tokio::test]
async fn one_large_valid_effect_can_be_scheduled_and_claimed() {
    let fixture = TestFixture::start().await;
    let store = fixture.initialize().await;
    let mut entry = effect(
        "large-effect",
        "large-thread",
        EffectType::ProviderSessionDetach,
    );
    entry.request["detail"] = json!("x".repeat(9 * 1024 * 1024));
    assert_eq!(store.outbox_enqueue(&[entry], NOW).await.unwrap(), 1);
    assert_eq!(
        store.outbox_next_claimable_at().await.unwrap().as_deref(),
        Some(NOW)
    );
    let claimed = store
        .outbox_claim_next(&claim("large-worker", NOW))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(claimed.id, "large-effect");
    assert_eq!(claimed.status, EffectStatus::Running);
    assert_eq!(claimed.attempt_count, 1);
    assert_eq!(claimed.lease_owner.as_deref(), Some("large-worker"));
    assert_eq!(
        claimed.request["detail"].as_str().unwrap().len(),
        9 * 1024 * 1024
    );
    assert!(
        store
            .outbox_succeed(&settle("large-effect", "large-worker", LATER))
            .await
            .unwrap()
    );
    assert_eq!(
        row(&store, "large-effect").await.status,
        EffectStatus::Succeeded
    );
    store.shutdown().await.unwrap();
}
