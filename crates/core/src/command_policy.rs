//! Provider capability decisions from orchestration-v2/CommandPolicy.ts.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.

use std::io::{self, BufRead};

pub const PROTOCOL: &str = "t3ls-policy-v1";
pub const MAX_FRAME_BYTES: usize = 256;
pub const CAPABILITY_BITS: u16 = (1 << 14) - 1;

const QUEUED_MESSAGES: u32 = 0;
const ACTIVE_STEERING: u32 = 1;
const INTERRUPT: u32 = 2;
const INTERRUPT_RESTART: u32 = 3;
const NATIVE_FORK: u32 = 4;
const FORK_FROM_TURN: u32 = 5;
const STRONG_THREAD_IDS: u32 = 6;
const ROLLBACK_THREAD: u32 = 7;
const ROLLBACK_CONVERSATION: u32 = 8;
const ROLLBACK_SNAPSHOT: u32 = 9;
const HANDOFF_SUMMARIES: u32 = 10;
const SYNTHETIC_CONTEXT: u32 = 11;
const DELTA_HANDOFF: u32 = 12;
const FULL_THREAD_HANDOFF: u32 = 13;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Capabilities {
    bits: u16,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Unsupported {
    pub capability: &'static str,
    pub detail: &'static str,
}

pub type Decision = Result<&'static str, Unsupported>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Handoff {
    ForkDelta,
    Delta,
    FullThread,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ForkInput {
    pub same_provider: bool,
    pub has_strong_native_source: bool,
    pub source_completed_or_waiting: bool,
    pub from_specific_turn: bool,
    pub source_busy: bool,
}

fn unsupported(capability: &'static str, detail: &'static str) -> Decision {
    Err(Unsupported { capability, detail })
}

impl Capabilities {
    /// Wire bits are fixed by protocol v1, not by upstream object iteration order.
    pub fn from_bits(bits: u16) -> Option<Self> {
        if bits & !CAPABILITY_BITS == 0 {
            Some(Self { bits })
        } else {
            None
        }
    }

    fn has(self, bit: u32) -> bool {
        self.bits & (1 << bit) != 0
    }

    pub fn ensure_queued_messages(self) -> Decision {
        if self.has(QUEUED_MESSAGES) {
            Ok("")
        } else {
            unsupported(
                "queued_messages",
                "providerInstanceId does not support app-owned queued turns",
            )
        }
    }

    pub fn decide_steering(self, force_restart: bool) -> Decision {
        if !force_restart && self.has(ACTIVE_STEERING) {
            return Ok("active_steering");
        }
        if self.has(INTERRUPT) && self.has(INTERRUPT_RESTART) {
            return Ok("interrupt_restart");
        }
        unsupported(
            if force_restart || self.has(INTERRUPT) {
                "interrupt_restart_steering"
            } else {
                "active_steering"
            },
            if force_restart {
                "providerInstanceId cannot satisfy a required interrupt-and-restart"
            } else {
                "providerInstanceId cannot steer active turns directly or by interrupt-and-restart"
            },
        )
    }

    pub fn ensure_interrupt(self) -> Decision {
        if self.has(INTERRUPT) {
            Ok("")
        } else {
            unsupported(
                "interrupt",
                "providerInstanceId does not support turn interrupts",
            )
        }
    }

    pub fn ensure_native_fork(self, from_specific_turn: bool) -> Decision {
        if !self.has(NATIVE_FORK) {
            return unsupported(
                "native_fork",
                "providerInstanceId does not support native thread forks",
            );
        }
        if from_specific_turn && !self.has(FORK_FROM_TURN) {
            return unsupported(
                "fork_from_turn",
                "providerInstanceId cannot fork from a specific completed turn",
            );
        }
        if !self.has(STRONG_THREAD_IDS) {
            return unsupported(
                "native_fork",
                "providerInstanceId does not expose strong native thread ids",
            );
        }
        Ok("")
    }

    pub fn ensure_rollback(self) -> Decision {
        if !self.has(ROLLBACK_THREAD) || !self.has(ROLLBACK_CONVERSATION) {
            return unsupported(
                "rollback",
                "providerInstanceId conversation rollback is unavailable",
            );
        }
        if !self.has(ROLLBACK_SNAPSHOT) {
            return unsupported(
                "rollback_snapshot",
                "rollback must return a providerInstanceId thread snapshot",
            );
        }
        Ok("")
    }

    pub fn ensure_handoff(self, strategy: Handoff) -> Decision {
        if !self.has(HANDOFF_SUMMARIES) {
            return unsupported(
                "context_handoff",
                "providerInstanceId cannot consume handoff summaries",
            );
        }
        if !self.has(SYNTHETIC_CONTEXT) {
            return unsupported(
                "context_handoff",
                "providerInstanceId cannot receive synthetic user context",
            );
        }
        if strategy != Handoff::FullThread && !self.has(DELTA_HANDOFF) {
            return unsupported(
                "context_handoff",
                "providerInstanceId does not support delta handoff",
            );
        }
        if strategy == Handoff::FullThread && !self.has(FULL_THREAD_HANDOFF) {
            return unsupported(
                "context_handoff",
                "providerInstanceId does not support full-thread handoff",
            );
        }
        Ok("")
    }

    pub fn decide_fork(self, input: ForkInput) -> Decision {
        if input.source_completed_or_waiting
            && input.same_provider
            && input.has_strong_native_source
            && !input.source_busy
            && self.has(NATIVE_FORK)
            && (!input.from_specific_turn || self.has(FORK_FROM_TURN))
            && self.has(STRONG_THREAD_IDS)
        {
            return Ok("native_fork");
        }
        self.ensure_handoff(Handoff::FullThread)?;
        Ok("portable_context")
    }
}

fn flag(value: &str) -> Result<bool, &'static str> {
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err("expected a boolean encoded as 0 or 1"),
    }
}

/// The boundary carries capability facts, never prompts, credentials, or shell commands.
pub fn evaluate(frame: &str) -> Result<Decision, &'static str> {
    if frame.len() > MAX_FRAME_BYTES || !frame.is_ascii() {
        return Err("invalid policy frame size or encoding");
    }
    let fields: Vec<&str> = frame.split('\t').collect();
    if fields.len() < 3 || fields[0] != PROTOCOL {
        return Err("unsupported policy protocol");
    }
    if fields[2].is_empty() || !fields[2].bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("invalid capability mask");
    }
    let bits: u16 = fields[2].parse().map_err(|_| "invalid capability mask")?;
    let caps = Capabilities::from_bits(bits).ok_or("unknown capability bits")?;
    match (fields[1], fields.len()) {
        ("queued", 3) => Ok(caps.ensure_queued_messages()),
        ("steering", 4) => Ok(caps.decide_steering(flag(fields[3])?)),
        ("interrupt", 3) => Ok(caps.ensure_interrupt()),
        ("native-fork", 4) => Ok(caps.ensure_native_fork(flag(fields[3])?)),
        ("rollback", 3) => Ok(caps.ensure_rollback()),
        ("handoff", 4) => {
            let strategy = match fields[3] {
                "fork_delta_context" => Handoff::ForkDelta,
                "delta_context" => Handoff::Delta,
                "full_thread_summary" => Handoff::FullThread,
                _ => return Err("invalid context handoff strategy"),
            };
            Ok(caps.ensure_handoff(strategy))
        }
        ("fork", 8) => Ok(caps.decide_fork(ForkInput {
            same_provider: flag(fields[3])?,
            has_strong_native_source: flag(fields[4])?,
            source_completed_or_waiting: flag(fields[5])?,
            from_specific_turn: flag(fields[6])?,
            source_busy: flag(fields[7])?,
        })),
        _ => Err("unknown operation or wrong field count"),
    }
}

pub fn response(decision: Result<Decision, &'static str>) -> String {
    match decision {
        Ok(Ok("")) => "ok\t-\n".to_owned(),
        Ok(Ok(value)) => format!("ok\t{value}\n"),
        Ok(Err(error)) => format!("unsupported\t{}\t{}\n", error.capability, error.detail),
        Err(error) => format!("invalid\t{error}\n"),
    }
}

/// Bounds allocation before reading a complete frame. A partial EOF is not a request.
pub fn read_frame<R: BufRead>(reader: &mut R) -> io::Result<Option<String>> {
    let mut bytes = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return if bytes.is_empty() {
                Ok(None)
            } else {
                Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated policy frame",
                ))
            };
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let count = newline.map_or(available.len(), |index| index + 1);
        if bytes.len() + count > MAX_FRAME_BYTES + 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "policy frame too large",
            ));
        }
        bytes.extend_from_slice(&available[..count]);
        reader.consume(count);
        if newline.is_some() {
            bytes.pop();
            return String::from_utf8(bytes).map(Some).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "policy frame is not UTF-8")
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;

    fn caps(bits: u16) -> Capabilities {
        Capabilities::from_bits(bits).unwrap()
    }

    #[test]
    fn force_restart_never_falls_back_to_active_steering() {
        assert_eq!(caps(2).decide_steering(false), Ok("active_steering"));
        let failure = caps(2).decide_steering(true).unwrap_err();
        assert_eq!(failure.capability, "interrupt_restart_steering");
        assert_eq!(caps(14).decide_steering(true), Ok("interrupt_restart"));
        assert_eq!(caps(14).decide_steering(false), Ok("active_steering"));
    }

    #[test]
    fn native_fork_checks_have_upstream_error_precedence() {
        assert_eq!(
            caps(0).ensure_native_fork(true).unwrap_err().detail,
            "providerInstanceId does not support native thread forks"
        );
        assert_eq!(
            caps(16).ensure_native_fork(true).unwrap_err().capability,
            "fork_from_turn"
        );
        assert_eq!(
            caps(48).ensure_native_fork(true).unwrap_err().detail,
            "providerInstanceId does not expose strong native thread ids"
        );
        assert_eq!(caps(112).ensure_native_fork(true), Ok(""));
    }

    #[test]
    fn busy_or_unsuccessful_source_requires_a_real_portable_handoff() {
        let input = ForkInput {
            same_provider: true,
            has_strong_native_source: true,
            source_completed_or_waiting: true,
            from_specific_turn: true,
            source_busy: false,
        };
        assert_eq!(caps(112).decide_fork(input), Ok("native_fork"));
        assert_eq!(
            caps(112)
                .decide_fork(ForkInput {
                    source_busy: true,
                    ..input
                })
                .unwrap_err()
                .capability,
            "context_handoff"
        );
        assert_eq!(
            caps(112 | 1024 | 2048 | 8192).decide_fork(ForkInput {
                source_completed_or_waiting: false,
                ..input
            }),
            Ok("portable_context")
        );
    }

    #[test]
    fn rollback_and_handoff_keep_their_own_capability_boundaries() {
        assert_eq!(
            caps(512).ensure_rollback().unwrap_err().capability,
            "rollback"
        );
        assert_eq!(
            caps(128 | 256).ensure_rollback().unwrap_err().capability,
            "rollback_snapshot"
        );
        assert_eq!(caps(128 | 256 | 512).ensure_rollback(), Ok(""));
        assert_eq!(
            caps(1024 | 2048 | 4096).ensure_handoff(Handoff::Delta),
            Ok("")
        );
        assert!(
            caps(1024 | 2048 | 4096)
                .ensure_handoff(Handoff::FullThread)
                .is_err()
        );
        assert_eq!(
            caps(1024 | 2048 | 8192).ensure_handoff(Handoff::FullThread),
            Ok("")
        );
        assert!(
            caps(1024 | 2048 | 8192)
                .ensure_handoff(Handoff::ForkDelta)
                .is_err()
        );
    }

    #[test]
    fn malformed_frames_never_become_successful_decisions() {
        for frame in [
            "",
            "t3ls-policy-v2\tqueued\t1",
            "t3ls-policy-v1\tqueued\t+1",
            "t3ls-policy-v1\tqueued\t16384",
            "t3ls-policy-v1\tqueued\t65536",
            "t3ls-policy-v1\tqueued\t1\textra",
            "t3ls-policy-v1\tsteering\t14\ttrue",
            "t3ls-policy-v1\tfork\t112\t1\t1\t1\t0",
            "t3ls-policy-v1\thandoff\t0\tother",
            "t3ls-policy-v1\tunknown\t0",
            "t3ls-policy-v1\tqueued\t１",
        ] {
            assert!(evaluate(frame).is_err(), "{frame}");
        }
        assert_eq!(response(evaluate("t3ls-policy-v1\tqueued\t1")), "ok\t-\n");
        assert!(response(evaluate("t3ls-policy-v1\tqueued\t0")).starts_with("unsupported\t"));
    }

    #[test]
    fn framing_handles_fragmentation_and_refuses_unbounded_or_truncated_input() {
        let source = b"t3ls-policy-v1\tqueued\t1\nt3ls-policy-v1\tinterrupt\t4\n";
        let mut reader = BufReader::with_capacity(3, &source[..]);
        assert_eq!(
            read_frame(&mut reader).unwrap().unwrap(),
            "t3ls-policy-v1\tqueued\t1"
        );
        assert_eq!(
            read_frame(&mut reader).unwrap().unwrap(),
            "t3ls-policy-v1\tinterrupt\t4"
        );
        assert_eq!(read_frame(&mut reader).unwrap(), None);
        let oversized = vec![b'x'; MAX_FRAME_BYTES + 2];
        let mut reader = oversized.as_slice();
        assert_eq!(
            read_frame(&mut reader).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
        let mut reader = &b"t3ls-policy-v1\tqueued\t1"[..];
        assert_eq!(
            read_frame(&mut reader).unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
        let invalid = [255_u8, b'\n'];
        let mut reader = &invalid[..];
        assert_eq!(
            read_frame(&mut reader).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
