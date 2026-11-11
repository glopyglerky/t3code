# Upstream a536ff7a integration and omdb compatibility

This change integrates `pingdotgg/t3code` default-branch snapshot
`a536ff7ac0c080190f098965d0a0d244caf5b211`, following
`7171fdd7716b05dc1b4be40c26ac78e8bb35f80c`. The immutable snapshot contains 125
additional upstream commits. It is not a new nightly release claim.

## Retained application and reviewed adapters

The upstream web, desktop, mobile and Node application remains at its original
paths. The merge preserves the production Rust adapters and backend-neutral
transaction/row interfaces. In particular:

- Project commands retain the native planner while adding upstream directory
  scanning and managed-worktree selection.
- Thread-find retains the injected row interface and adds upstream's idle cache
  lifecycle. Search uses schema-81 readable citation text.
- Projection lookups add `getRun` ownership/error semantics; shell/awareness rows
  retain background-wake status, including blocked usage-limit runs.
- Unanchored history windows include the latest presented run without treating
  held queued prompts or silent compaction as a new presented run. Anchored and
  explicitly required windows keep their prior boundaries.
- Pull-request thread selection uses canonical archive state. Query-only
  optimizations do not replace the fork's native row boundary.
- Peer-link and Codex adapter upstream changes retain their native persistence
  and process integration points.

Four upstream removals are recorded with exact previous blobs and replacement
paths in `port-map.json`: `MorphIcon.tsx`, `useTerminalFocus.ts`, and the moved
embedded-script implementation/test. The prior fork copies matched upstream;
no fork behavior is discarded. No Rust retirement or mobile source removal is
authorized by this integration.

## Schema 81

The checked schema is generated from all 81 retained migrations, including
`ProjectionMessageSearchText`. New SQLite imports run the retained migrations on
an owned snapshot, preserving the original source. Existing omdb schema-80 homes
must upgrade natively: bounded, committed backfill pages preserve native-only
writes and the contentless search index's historical postings. The upgrade fences
old readers/writers, resumes from its durable cursor after interruption, and
publishes schema 81 only when complete. It never reimports an old SQLite backup.

Before fencing, the reserved writer validates every transformed row and its real
index-mutation budget through a bounded read-only snapshot. Expansion beyond the
existing native row/transaction limits is refused without changing schema 80.
An unactivated schema-80 import first compares a schema-preserving export of its
retained source against the original receipt; new migration timestamps cannot
invalidate an otherwise unchanged source. Changed sources still fail closed.

The file-access guard now treats the canonical omdb path and native backups as
server-internal files, including configured database paths and symlink aliases.
Ordinary unrelated workspace files with an `.omdb` extension remain accessible.

## Compilers and platform evidence

Per the user's temporary compiler policy, eligible helpers use the unchanged
pinned native Lust compiler. Code outside that subset uses stock Rust 1.99.1.
Lustr and blueFe source is not changed. Build artifacts record their actual
compiler routes and hashes.

The pinned native Lust frontend rejects the current core crate's iterator
inference and `String.replace_range` operations (`L0610` and `L0104`). The explicit
fallback verifies that same source with pinned rustc and records an artifact-free
`unsupported` Lust receipt. Compiler-integrity, runtime and parity failures remain
hard failures. This candidate therefore does not claim native-Lust application
artifacts merely because the compiler itself built successfully.

Database-only bundles decouple the portable omdb helper from Unix-only service
helpers. Actual Linux/macOS/Windows x86_64/ARM64 qualification, packaged selector
checks and exact-source receipts are required before those bundles are selected.
Automatic builds run the package probe before caching or staging. Ordinary
selection rejects lifecycle-only candidates; the package receipt must match the
source, upstream revision, target and database executable hash. The owned probe
first executes each layout directly, then checks normal default selection and
reopen using provisional receipts inside its owned copies. Only after all checks
pass does it publish the source bundle's receipt; interruption cannot leave an
unqualified source bundle marked as passed.
The full fork workflow must pass on the candidate before main can be advanced.

This record describes implementation and required qualification, not an invented
passing result. The candidate's GitHub qualification run is the execution record.
Mobile-local Expo SQLite caches/preferences and a fully native mobile bridge
remain outside this server-persistence cutover. A server test does not establish
all-platform UI or mobile storage compatibility.
