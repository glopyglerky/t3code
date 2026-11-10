//! Direct ports of pinned T3 Code orchestration and shared modules.
//!
//! These modules preserve upstream field names and record/event contracts.
//! They do not define a replacement application protocol. See docs/port-map.md.

#![forbid(unsafe_code)]

pub mod bounded_snapshot;
pub mod deduped_bodies;
pub mod js;
pub mod tool_output;
pub mod types;
pub mod wire_projection;
