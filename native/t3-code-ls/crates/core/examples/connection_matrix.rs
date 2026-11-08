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
            _ => AttemptEnd::Interrupted { reset_retry: true },
        },
    }
}

fn seeded(failures: u32) -> RetryPolicy {
    let mut retry = RetryPolicy::new();
    for _ in 0..failures {
        retry.after_attempt(outcome(0, false, false, false), 0.0);
    }
    retry
}

fn print_decision(retry: RetryPolicy, decision: RetryDecision) {
    match decision {
        RetryDecision::RetryAfter(delay) => println!(
            "after\t{}\t{}\t{}",
            delay,
            retry.failure_count(),
            retry.next_attempt()
        ),
        RetryDecision::RetryNow => println!(
            "now\t0\t{}\t{}",
            retry.failure_count(),
            retry.next_attempt()
        ),
        RetryDecision::AwaitSignal => println!(
            "signal\t0\t{}\t{}",
            retry.failure_count(),
            retry.next_attempt()
        ),
    }
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
    for failures in [0, 1, 2, 7, 8, 25] {
        for established in [false, true] {
            for stable in [false, true] {
                for probe_failed in [false, true] {
                    for end in 0..4 {
                        for sample in 0_u32..=16 {
                            let mut retry = seeded(failures);
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
    for failures in [0, 1, 2, 7, 8, 25] {
        for reason in [
            Wakeup::ApplicationActive,
            Wakeup::ApplicationActiveProbe,
            Wakeup::ApplicationActiveLong,
            Wakeup::CredentialsChanged,
            Wakeup::NetworkChanged,
        ] {
            let mut retry = seeded(failures);
            let reset = retry.wakeup(reason);
            println!(
                "wakeup\t{}\t{}\t{}",
                reset,
                reason.should_resubscribe(),
                retry.failure_count()
            );
        }
    }
    // A path through the real retry ladder catches accidental state resetting
    // between decisions, which a table of isolated delays would miss.
    let mut retry = RetryPolicy::new();
    for index in 0_u32..96 {
        let end = if index % 11 == 0 { 1 } else { 0 };
        let established = index % 3 == 0;
        let stable = index % 17 == 0;
        let probe_failed = index % 19 == 0;
        let decision = retry.after_attempt(
            outcome(end, established, stable, probe_failed),
            (index % 13) as f64 / 13.0,
        );
        print_decision(retry, decision);
        if index % 23 == 0 {
            retry.wakeup(Wakeup::NetworkChanged);
        }
        if index % 29 == 0 {
            retry.wakeup(Wakeup::CredentialsChanged);
        }
    }
}
