use t3_code_ls_core::connection::{
    AttemptEnd, AttemptOutcome, BACKOFF_RESET_AFTER_MS, RetryDecision, RetryPolicy, Wakeup,
    retry_delay_ms, session_loss_retry_delay_ms,
};

fn outcome(index: u32, established: bool, stable: bool, probe_failed: bool) -> AttemptOutcome {
    AttemptOutcome {
        established,
        stable,
        probe_failed,
        end: match index {
            0 => AttemptEnd::TransientFailure,
            1 => AttemptEnd::BlockedFailure,
            2 => AttemptEnd::Interrupted { reset_retry: false },
            3 => AttemptEnd::Interrupted { reset_retry: true },
            4 => AttemptEnd::ApprovalPendingFailure,
            _ => panic!("unknown fixture outcome"),
        },
    }
}

fn seeded(failures: u32, approval_waits: u32) -> RetryPolicy {
    let mut retry = RetryPolicy::new();
    for _ in 0..failures {
        retry.after_attempt(outcome(0, false, false, false), 0.0);
    }
    for _ in 0..approval_waits {
        retry.after_attempt(outcome(4, false, false, false), 0.0);
    }
    retry
}

fn print_decision(retry: RetryPolicy, decision: RetryDecision) {
    let (action, delay) = match decision {
        RetryDecision::RetryAfter(delay) => ("after", delay),
        RetryDecision::RetryNow => ("now", 0),
        RetryDecision::AwaitSignal => ("signal", 0),
    };
    println!(
        "{}\t{}\t{}\t{}\t{}\t{}",
        action,
        delay,
        retry.failure_count(),
        retry.next_attempt(),
        retry.backoff_failures(),
        retry.approval_waits()
    );
}

fn main() {
    println!("stable\t{}", BACKOFF_RESET_AFTER_MS);
    for failure_count in 0..=24 {
        for sample in 0_u32..=2_000 {
            println!(
                "delay\t{}",
                retry_delay_ms(failure_count, sample as f64 / 2_000.0)
            );
        }
    }
    for sample in 0_u32..=2_000 {
        println!(
            "loss\t{}",
            session_loss_retry_delay_ms(sample as f64 / 2_000.0)
        );
    }
    let histories = [
        (0, 0),
        (1, 0),
        (2, 0),
        (7, 0),
        (8, 0),
        (25, 0),
        (0, 1),
        (0, 149),
        (0, 150),
        (0, 151),
        (8, 149),
        (8, 150),
        (8, 151),
    ];
    for (failures, approval_waits) in histories {
        for established in [false, true] {
            for stable in [false, true] {
                for probe_failed in [false, true] {
                    for end in 0..5 {
                        for sample in 0_u32..=16 {
                            let mut retry = seeded(failures, approval_waits);
                            let decision = retry.after_attempt(
                                outcome(end, established, stable, probe_failed),
                                sample as f64 / 16.0,
                            );
                            print_decision(retry, decision);
                        }
                    }
                }
            }
        }
    }
    for (failures, approval_waits) in histories {
        for reason in [
            Wakeup::ApplicationActive,
            Wakeup::ApplicationActiveProbe,
            Wakeup::ApplicationActiveLong,
            Wakeup::ApplicationFocused,
            Wakeup::CredentialsChanged,
            Wakeup::NetworkChanged,
        ] {
            let mut retry = seeded(failures, approval_waits);
            let reset = retry.wakeup(reason);
            println!(
                "wakeup\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                reset,
                reason.should_resubscribe(),
                reason.should_relist_account(),
                reason.reaches_connection_supervisor(),
                retry.failure_count(),
                retry.backoff_failures(),
                retry.approval_waits()
            );
        }
    }
    // A path through the real retry ladder catches accidental state resetting
    // between decisions, which a table of isolated delays would miss.
    let mut retry = RetryPolicy::new();
    for index in 0_u32..360 {
        let end = if index < 160 {
            4
        } else if index % 11 == 0 {
            1
        } else if index % 5 == 0 {
            2
        } else if index % 17 == 0 {
            3
        } else if index % 3 == 0 {
            4
        } else {
            0
        };
        let established = index >= 160 && index % 3 == 0;
        let stable = index >= 160 && index % 53 == 0;
        let probe_failed = index >= 160 && index % 19 == 0;
        let decision = retry.after_attempt(
            outcome(end, established, stable, probe_failed),
            (index % 13) as f64 / 13.0,
        );
        print_decision(retry, decision);
        if index >= 160 && index % 23 == 0 {
            retry.wakeup(Wakeup::NetworkChanged);
        }
        if index >= 160 && index % 29 == 0 {
            retry.wakeup(Wakeup::CredentialsChanged);
        }
        if index >= 160 && index % 31 == 0 {
            retry.wakeup(Wakeup::ApplicationFocused);
        }
    }
}
