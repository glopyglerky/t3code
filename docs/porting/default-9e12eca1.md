# Default-branch 9e12eca1 and provider update verification

This integration advances the trusted upstream source from
`4c361199f47f6ea32a2c14835df392640d0f7c63` to
`9e12eca12526c842f97cfcbc6f3cd2149259c776`, with tree
`69d0dcaa463b25fd8d25698b07916f925086c391`. The fork base is
`2a439db0d3a4918b72764fa3de45f376041b7416`.

## Source integration

The complete upstream difference contains 17 modified paths. Sixteen are imported
at their exact upstream blob identities. `Orchestrator.ts` combines upstream's
mutable model selection with the fork's awaited Rust dispatch-policy call. The
remaining upstream hunks merge around the existing substitutions. Comparing the
fork's changes against both upstream baselines preserves all 202 added/deleted
fork lines in that file, including guest-share and recovery calls.

| Upstream area | Paths | Disposition |
| --- | ---: | --- |
| Managed endpoint runtime and tests | 2 | Exact upstream |
| Notification mailbox | 1 | Exact upstream |
| Orchestrator | 1 | Merged, preserving native service calls |
| Provider session manager and tests | 2 | Exact upstream |
| Steering completion and runtime integration tests | 2 | Exact upstream |
| Browser page evaluation and tests | 2 | Exact upstream |
| Database storage notice | 1 | Exact upstream |
| OpenCode adapter and tests | 2 | Exact upstream |
| Markdown links and tests | 2 | Exact upstream |
| SSH tunnels and tests | 2 | Exact upstream |

Queued messages without an explicit model now follow the last applicable queued
model selection. Upstream keeps the special handling for goal commands that stop
the active run, buffered provider notifications and messages following the current
conversation. The update also handles detached-session requests, endpoint
shutdown, browser top-level await retries, OpenCode command admission, Windows
relative Markdown paths and SSH listener readiness.

The five-entry source history retains every earlier integration. All 51 reviewed
upstream-removal records remain intact. The README body, dependency locks and
compiler pins are unchanged. The `8226715` nightly reference remains separate from
this default-branch snapshot.

## Rust verification in the maintenance runner

The existing `t3-code-ls-maintenance` helper now decides whether a refreshed
provider is already current and whether a completed update succeeded. The pure
algorithm lives in its owning application domain at
`apps/server/src/provider/provider_update_verification.rs` and is compiled by the
existing core crate.

The real `ProviderMaintenanceRunner` sends bounded provider observations to the
helper before an unpinned update and after a successful installer command. The
host retains compatibility checks, installation ownership, command locks,
executable and environment selection, refreshes, cache invalidation, timestamps
and publication of update state. The helper receives no credentials or executable
paths in these requests.

The precheck preserves every upstream condition: advisory enrichment succeeded,
a prior provider exists, the refresh contains providers, each timestamp changed,
and every provider is installed, has no error status and reports a current
version. The postcheck preserves missing versus explicit-null installed versions,
the unauthenticated-provider exception, removal of exactly one lowercase `v` for
a targeted version, and the precedence of inability to verify over an outdated
advisory. Missing version data alone does not fail an unpinned update.

Each verification operation has a single terminal response. Strict validation
rejects unexpected fields, modes, status/message combinations, observations,
extra output and failed child processes. A helper failure before installation
stops the update. A failure afterward records the existing failed state without
rerunning the installer or switching implementations.

## Qualification and remaining scope

The existing maintenance gates cover Rust unit behavior, process framing,
immutable-upstream parity and the real runner with controlled installers and
refreshes. The upstream runner itself is added to the port's source mapping, so
future changes to its verification logic remain visible to the porting planner.
The existing native application flow qualifies the combined upstream integration.

This change adds a mapped Rust input and modifies the fork's orchestrator, so the
generated plan selects full fork qualification. Main may advance only after all
selected gates pass on the candidate commit. Compiler and dependency pins, CI jobs
and schedules stay unchanged.

The maintenance port remains `integrated`. Retained platforms and explicit
upstream mode still use the original decisions selected before execution. This
integration grants no cross-platform retirement acceptance. Production SQLite
and Connect remain authoritative; no original implementation is retired.
