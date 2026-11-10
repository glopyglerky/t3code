# Existing T3 Code to Rust

The target is the existing application, not a replacement designed around the
parts already implemented. Its upstream interfaces, workflows, clients, errors,
ordering and persistence semantics remain the requirements. Redb and Iroh are
requested implementation changes, not permission to change unrelated behavior.

Lust accepts its covered Rust subset. Lustr coordinates compilation and BlueFe
handles the remaining Rust. This does not mean rewriting the application to fit
only the native Lust subset. The GitHub workflow qualifies these crate ports
with produced BlueFe through Lustr; it does not claim native-Lust acceptance of
the new orchestration crate.

## Direct translations in this pass

All sources below are from `pingdotgg/t3code` at
`521ac7c218b3faa74281fb72fc36ce74a0951fb3`, with its MIT license preserved.
`port-sources.json` records the source and test blob identities. The Rust crate
is `crates/orchestration`; it retains the original JSON field names, event names
and omission markers. It introduces no application RPC method or conversation
schema. Rust `Arc` identity models the shared objects needed by compaction.

| Existing upstream source | Rust translation | Port boundary |
| --- | --- | --- |
| `packages/shared/src/orchestrationV2DedupedBodies.ts` | `src/deduped_bodies.rs` | Message, checkpoint and subagent body deduplication/restoration; lazy indexes; shared local items; inherited rows untouched. |
| `packages/shared/src/orchestrationV2BoundedSnapshot.ts` | `src/bounded_snapshot.rs` | Exact local-prefix eligibility by identity, flag-gated restoration, then body restoration. |
| `apps/server/src/orchestration-v2/WireProjection.ts` | `src/wire_projection.rs` | Wire/detail projection, checkpoint caps, event projection/cache and per-subscription live body deduplication. |
| `packages/shared/src/toolOutput.ts` | `src/tool_output.rs` | Tool-result envelope reads, metadata/image limits, reference validation, image-data omission and failure detection. Required reference readers are translated from the pinned HTML/MCP helper modules. |

Tests translate the applicable upstream cases and add boundary tests. The
`threadShellFromProjection` assertion and async coalescer execution imported by
upstream's wire-projection test file are not claimed here: those modules have
not been ported in this pass. A live-deduper test begins at their already-
coalesced output. These distinctions matter when counting tests as evidence.

These modules are compiled workspace components, not a completed client/server
cutover. The upstream event sink, live-stream subscription ownership/budgets,
full contract validators and client reducers still need to call the Rust ports
as their own modules are translated. The temporary native host is not proof of
that integration. The `types` module retains unmapped fields; it is an ownership
representation, not the complete upstream schema validator.

## JavaScript compatibility still requiring work

The port preserves ECMAScript whitespace, UTF-16 lengths, JSON numeric formatting
and property ordering where they affect output. UTF-8 truncation includes the
upstream marker in its byte cap. It does not silently substitute Rust's string
or numeric behavior for JavaScript's.

The current `String`/`serde_json::Value` representation cannot hold lone UTF-16
surrogates or non-finite JSON numbers. JavaScript can also parse nesting beyond
Serde's safe recursion limit. A slice
that would split a surrogate pair, an escaped lone surrogate in a JSON envelope,
a numeric-overflow envelope, or a parser-depth boundary returns an explicit
representation error. Those inputs are **not parity-qualified**. Stack-safe
parsing and a lossless JavaScript-string representation remain required; these
errors must not become fabricated successful results or discarded metadata.

## Remaining application port

The server orchestration, command dispatch, provider adapters, event/persistence
and migration behavior need full source-to-port coverage. Web, desktop and
mobile clients and their platform integrations remain required, along with
attachments, terminals, scheduling, checkpoints, guest sharing, Connect/pairing
behavior over Iroh and packaging. Earlier removal of their implementation did
not satisfy any of those requirements.

The current native host, partial command services and component tests must not
be presented as an end-to-end T3 Code port. Keep the original source available
through the immutable upstream reference and Git history, keep working Rust
ports, and record remaining differences until the existing workflows are proven.
