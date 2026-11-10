# t3code-ls

## Port the existing application

This is a behavior-preserving Rust port of the existing T3 Code implementation,
not a new application built from scratch or a cleanroom replacement. Preserve
upstream contracts, features, UI behavior, ordering, errors and persistence
semantics. Port the upstream modules and their tests; record source mappings and
remaining differences in `docs/port-map.md`. Do not invent a replacement protocol
and count it as the upstream protocol's port.

Lust compiles its supported Rust subset. Lustr coordinates that path with BlueFe
for the rest of Rust. Compiler coverage is not a reason to remove features or
redesign the product to fit a smaller language subset. Qualification must identify
which compiler path ran, without calling a BlueFe build a native-Lust build.

Previously removed source is a porting gap, not authorization to discard its
functionality. Web, desktop, mobile and server behavior remain required. The
existing native API is a temporary implementation/qualification host with a
different protocol, not the completed application's contract. Do not expand that
different protocol as a substitute for porting the original.

The default tree contains Rust implementation only. Preserve licenses, provenance,
passive assets, data fixtures and Git history. Do not restore foreign executable
code, embed it in Rust strings, or use language-classification overrides.

The root Cargo workspace contains an authenticated native API/CLI, durable
project/thread services, Codex sessions, decision/protocol tools, Unix structured
generation, redb storage and Iroh transport. It is not a complete application.
Web, Electron, mobile, Node server and Windows provider source is absent from
the current tree; porting their behavior remains required. The native service
subset does not replace their full behavior. Never claim full application,
platform or whole-stack memory-safety qualification from component tests.

Build with the pinned qualified Lustr coordinator and produced BlueFe rustc and
rustdoc. Cargo is bootstrap Cargo. Preserve caller flags and select the matching
produced target sysroot. Do not substitute stock Clippy or compiler drivers.
Keep generated output outside canonical source and clean owned disposable output.

Run workspace tests and `t3code-ls-tools check-source` and
`check-upstream-readme` and `check-upstream-ports` before publishing. Build helpers
are Rust too. Do not run removed Node tooling or recreate vendored source caches.

Original behavior is available from `pingdotgg/t3code` at the immutable commit
and tree in `upstream.lock.json`. Use `git show <commit>:<path>` after fetching
that exact commit, or read its pinned GitHub URL. The unchanged upstream README
body is wrapped in `upstream-README.md`; its wrapper is fork prose. Historical
fork source, including retired adapters and reference snapshots, stays in Git
history. Reference pins are retained as passive metadata, never runtime imports.

Keep repository identity 1352693431, fork relationship, license and separate
archived-thread/mobile-backup branches. Publish main only through fresh-head,
non-forced fast-forward updates when authorized. No PR is implied. External
GitHub submissions require verified model/harness attribution under the user's
session rule. Never modify installed T3 state, services or user data for tests.

## Mandatory external GitHub attribution

Before publishing agent-authored text or code on GitHub outside verified user
namespaces `glopyglerky` and `omnidia-dev`, append the visible footer below at the
very end of the submission body (or commit message). Classify a PR by its base
repository. Verify any additional claimed ownership. This applies to PRs, issues,
comments, reviews, discussions and release text; it does not authorize publication.

Use runtime evidence for exact author models, roles and actual harness chain;
distinguish reviewers. Never infer historical authorship from the current editor,
configured defaults or commit names. If provenance cannot be verified, keep a
private draft and report the missing evidence. Exclude private paths, thread IDs,
transcripts and credentials from public attribution.

For retrospective corrections, amend only the user's existing open PR or issue
with verifiable original provenance. Recheck open state immediately before editing,
preserve its body and append the footer. Never reopen or modify closed submissions
or add speculative attribution comments. Carry this rule into worker handoffs.

```md
---
**AI attribution**
- Models: <verified exact models and roles>
- Harness: <verified actual harness chain>
```

## Runtime and storage safety

- Use GitHub repositories and the hosted workspace for implementation and qualification.
- Use controlled provider fixtures for qualification, without real provider turns or personal credentials.
- Use the session's verified model/settings; reasoning above xhigh and non-Standard speed are prohibited.
- Run authorized workers through T3's supported lifecycle APIs. Do not spawn workers for a bounded task assigned to work alone.
- Never kill processes by name/path pattern. Stop only owned processes identified when spawned.
- Never run tests against an installed application's state or remove user data.
- Build the pinned Lustr and BlueFe sources in isolated hosted directories. Bootstrap compilers may build the toolchain; application qualification must use the produced drivers and matching sysroot.
- Keep generated output outside the source checkout and bound build concurrency to the hosted runner's capacity. Do not touch another owner's source or builds.
- Own generated output through completion/failure/cancellation. Retain source/history, a current deliverable, one useful rollback and concise hashes/logs. Remove disposable targets, dependencies and staging after checking actual processes, descriptors, mounts and Cargo locks; record available-space change. Do not clean another owner's scope.
- Do not resume unrelated work, schedules or agents to perform housekeeping. Preserve source revisions, first failures, receipts and rollback boundaries.
- Do not publish, deploy, rename repositories, rewrite history or change visibility/license unless the user has authorized that exact scope. Do not send messages to others without authorization.
