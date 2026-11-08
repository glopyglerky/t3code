//! Delivery decisions from orchestration-v2/CommandPolicy.ts.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.

fn flag(value: &str) -> Result<bool, &'static str> {
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err("expected a boolean encoded as 0 or 1"),
    }
}

fn turns(value: &str) -> Result<Option<u8>, &'static str> {
    if value == "-" {
        return Ok(None);
    }
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("invalid turn capability mask");
    }
    let bits: u8 = value.parse().map_err(|_| "invalid turn capability mask")?;
    if bits > 15 {
        return Err("unknown turn capability bits");
    }
    Ok(Some(bits))
}

fn active(status: &str) -> bool {
    matches!(status, "preparing" | "starting" | "running" | "waiting")
}

fn status(value: &str) -> Result<&str, &'static str> {
    match value {
        "-" | "inactive" | "preparing" | "starting" | "running" | "waiting" => Ok(value),
        _ => Err("invalid projected run status"),
    }
}

fn supports(capabilities: Option<u8>, bit: u8) -> bool {
    match capabilities {
        Some(bits) => bits & bit != 0,
        None => false,
    }
}

/// Projection joins stay at the adapter boundary. All delivery precedence is here.
/// Turn bits: queued=1, active steering=2, interrupt=4, restart steering=8.
pub fn evaluate(frame: &str) -> Result<&'static str, &'static str> {
    if frame.len() > 256 || !frame.is_ascii() {
        return Err("invalid dispatch frame size or encoding");
    }
    let fields: Vec<&str> = frame.split('\t').collect();
    if fields.first() == Some(&"t3ls-goal-v1") {
        if fields.len() != 3 {
            return Err("wrong goal field count");
        }
        let goal_active = flag(fields[2])?;
        let stops = match fields[1] {
            "set" | "resume" => true,
            "clear" | "pause" => goal_active,
            "show" => false,
            _ => return Err("unknown goal command"),
        };
        return Ok(if stops { "true" } else { "false" });
    }
    if fields.len() != 12 || fields[0] != "t3ls-dispatch-v1" {
        return Err("unsupported dispatch protocol or field count");
    }
    if !matches!(fields[1], "steer_active" | "other") {
        return Err("invalid requested dispatch kind");
    }
    if !matches!(fields[2], "-" | "auto" | "steer" | "restart") {
        return Err("invalid delivery intent");
    }
    let active_status = status(fields[3])?;
    if active_status == "inactive" {
        return Err("selected active run is inactive");
    }
    let active_turns = turns(fields[4])?;
    let running_turn = flag(fields[5])?;
    let requested_status = status(fields[6])?;
    let requested_turns = turns(fields[7])?;
    let requested_maintenance = flag(fields[8])?;
    let target_status = status(fields[9])?;
    let target_turns = turns(fields[10])?;
    let target_maintenance = flag(fields[11])?;
    let from_active = fields[2] != "-";
    let mode = if !from_active {
        fields[1]
    } else if active_status == "-" {
        "start_immediately"
    } else if fields[2] == "steer" {
        "steer_active"
    } else if fields[2] == "restart" {
        "restart_active"
    } else if matches!(active_status, "preparing" | "starting") || !running_turn {
        "queue_after_active"
    } else if supports(active_turns, 2) {
        "steer_active"
    } else if supports(active_turns, 1) {
        "queue_after_active"
    } else if supports(active_turns, 8) {
        // Upstream chooses restart here even without interrupt support; the
        // subsequent capability check owns the rejection, not this resolver.
        "restart_active"
    } else {
        "queue_after_active"
    };
    if mode == "steer_active" {
        let (target, capabilities, maintenance) = if from_active {
            (target_status, target_turns, target_maintenance)
        } else {
            (requested_status, requested_turns, requested_maintenance)
        };
        if maintenance
            || (active(target)
                && capabilities.is_some()
                && !supports(capabilities, 2)
                && !(supports(capabilities, 4) && supports(capabilities, 8))
                && supports(capabilities, 1))
        {
            return Ok("queue_after_active");
        }
    }
    if !from_active {
        return Ok("requested");
    }
    match mode {
        "start_immediately" => Ok("start_immediately"),
        "queue_after_active" => Ok("queue_after_active"),
        "steer_active" => Ok("steer_active"),
        "restart_active" => Ok("restart_active"),
        _ => Err("invalid dispatch result"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn automatic_delivery_preserves_steer_queue_restart_precedence() {
        for (mask, expected) in [(15, "steer_active"), (9, "queue_after_active"),
            (8, "restart_active"), (0, "queue_after_active")] {
            let frame = format!("t3ls-dispatch-v1\tother\tauto\trunning\t{mask}\t1\t-\t-\t0\trunning\t{mask}\t0");
            assert_eq!(evaluate(&frame), Ok(expected));
        }
    }

    #[test]
    fn preparation_and_missing_provider_turn_queue_only_automatic_delivery() {
        for state in ["preparing", "starting", "running", "waiting"] {
            for intent in ["auto", "steer", "restart"] {
                let frame = format!("t3ls-dispatch-v1\tother\t{intent}\t{state}\t15\t0\t-\t-\t0\t{state}\t15\t0");
                let expected = match intent {
                    "auto" => "queue_after_active",
                    "steer" => "steer_active",
                    _ => "restart_active",
                };
                assert_eq!(evaluate(&frame), Ok(expected));
            }
        }
    }

    #[test]
    fn absent_intent_preserves_the_requested_target_not_the_last_active_run() {
        assert_eq!(evaluate("t3ls-dispatch-v1\tsteer_active\t-\trunning\t15\t1\trunning\t-\t0\trunning\t1\t1"), Ok("requested"));
        assert_eq!(evaluate("t3ls-dispatch-v1\tsteer_active\t-\trunning\t15\t1\trunning\t1\t0\trunning\t15\t0"), Ok("queue_after_active"));
        assert_eq!(evaluate("t3ls-dispatch-v1\tother\t-\t-\t-\t0\t-\t-\t0\t-\t-\t0"), Ok("requested"));
    }

    #[test]
    fn maintenance_blocks_steering_but_not_explicit_restart() {
        assert_eq!(evaluate("t3ls-dispatch-v1\tother\tsteer\trunning\t15\t1\t-\t-\t0\trunning\t15\t1"), Ok("queue_after_active"));
        assert_eq!(evaluate("t3ls-dispatch-v1\tother\trestart\trunning\t15\t1\t-\t-\t0\trunning\t15\t1"), Ok("restart_active"));
    }

    #[test]
    fn absent_active_run_starts_and_missing_capabilities_do_not_invent_support() {
        for intent in ["auto", "steer", "restart"] {
            let frame = format!("t3ls-dispatch-v1\tsteer_active\t{intent}\t-\t-\t0\t-\t-\t0\t-\t-\t0");
            assert_eq!(evaluate(&frame), Ok("start_immediately"));
        }
        assert_eq!(evaluate("t3ls-dispatch-v1\tother\tauto\trunning\t-\t1\t-\t-\t0\trunning\t-\t0"), Ok("queue_after_active"));
    }

    #[test]
    fn goal_stop_policy_preserves_active_goal_requirement() {
        for command in ["set", "resume", "clear", "pause", "show"] {
            for value in [0, 1] {
                let frame = format!("t3ls-goal-v1\t{command}\t{value}");
                let expected = matches!(command, "set" | "resume")
                    || (matches!(command, "clear" | "pause") && value == 1);
                assert_eq!(evaluate(&frame), Ok(if expected { "true" } else { "false" }));
            }
        }
    }

    #[test]
    fn malformed_frames_are_not_decisions() {
        for frame in ["", "t3ls-goal-v1\tshow\ttrue", "t3ls-goal-v1\tunknown\t0",
            "t3ls-goal-v1\tshow\t0\textra", "t3ls-dispatch-v2\tother",
            "t3ls-dispatch-v1\tother\tauto\trunning\t16\t1\t-\t-\t0\t-\t-\t0",
            "t3ls-dispatch-v1\tother\tauto\tinactive\t0\t1\t-\t-\t0\t-\t-\t0",
            "t3ls-dispatch-v1\tother\tauto\trunning\t+1\t1\t-\t-\t0\t-\t-\t0"] {
            assert!(evaluate(frame).is_err());
        }
    }
}
