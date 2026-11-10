# Hybrid application recovery

## Source and product boundary

The recovery starts from `0be60f85268daece83e616b97f6fc421ec83ee6b` and restores
7,221 missing application, platform, asset, build and test files from
`d29a94f3a175eac1f26c548023fedd8695cedfaf`. The source checkpoint is
[241276b](https://github.com/glopyglerky/t3code-ls/commit/241276bf28844d3fdc12b96d44865b58cd63b075).
The overlay preserves every current Rust implementation file, including the
later storage and experimental native application work. It does not reset main
or alter the separate archived-thread, mobile-backup or orchestration branches.

The original Node server, React web app, Electron shell, React Native clients,
shared contracts, provider packages and Connect infrastructure occupy their
upstream paths. Root Cargo and pnpm workspaces coexist. `crates/` remains an
explicitly mapped migration input; [the port map](../../port-map.json) assigns
domain owners. Moving Rust source is a separate step from this recovery and
behavior selection. The experimental native API remains component work and is
not the normal clients' server.

Required original algorithms that had been removed before the large deletion
are also recovered from `3ba083e4c2532014ad9b740c26b69b0183916ecb` at their owning
paths. Native adapters select before execution. The default native host is
Linux GNU x86_64 with GLIBC 2.39 or newer. Other hosts keep the original
implementation and can build without the Linux compiler. An explicit
`T3_CODE_LS_IMPLEMENTATION=upstream` chooses that original backend for controlled
comparisons. A component override selects native and retains its existing typed
validation errors. No uncertain action retries through another implementation.

Native and original-only artifact manifests make the same decision during
server, SEA and desktop staging. Missing native components, mismatched targets
and modified native bytes fail the native build. An original-only manifest
cannot silently satisfy a requested native build.

## Compiler route

[toolchain.lock.json](../../toolchain.lock.json) pins all six assets of
BlueFe `preview-2026.11.09-2`, compiler source
`1b74388e196ee8813f38ba9eeb6d4ab43b6b1071` and Lustr coordinator
`07470b20464bf097c3c88983124aff56c5e44f9a`. The application uses rustc-derived
BlueFe through the Lustr compatibility coordinator. It does not rebuild the
compiler. Native Lust remains separately pinned in
[lust-toolchain.json](../../lust-toolchain.json); the compatibility package
contains no native Lust compiler.

Initial execution exposed a missing producer ELF interpreter path,
`/usr/lib/ld-linux-x86-64.so.2`. The installer verifies all package bytes, then
creates a separate recorded host adapter when that path is absent. It uses the
host GNU loader and the same packaged compiler and linker. The package bytes
and global loader paths remain unchanged. Linker dispatch follows the pinned
upstream wrapper's flavor mapping. Bash launchers preserve Cargo environment
keys containing hyphens, including `CARGO_BIN_EXE_*`. Both initial failures
precede the final successful test run; they are not reported as passes.

## Recovery defects found by fresh checks

The historical comparison helpers lacked declared workspace dependencies.
`tools/oracles/package.json` declares their existing Effect/contracts/shared
dependencies. The reviewed pnpm lock diff only adds that importer; dependency
versions stay fixed. Cargo.lock remains unchanged. Frozen and locked gates stay
in place.

Two recovery test files chained independent Effect layer provisions, which the
pinned checker reported. They now provide the merged layers once. The restart
fixtures also bypassed their types and omitted mandatory provider-turn,
subagent and turn-item fields. Typed fixture constructors restore those fields.
Assertions remain, and the Rust decoder keeps rejecting incomplete requests.

The historical invalid-component-override path threw too early and bypassed the
service's typed error contract. Selection now preserves the explicit choice for
the existing adapter to validate before effects. The restored original-mode
selector also makes differential tests compare original and Rust execution,
instead of accidentally running the native backend twice.

Independent review also found an original-only desktop bundle waiting for absent
native helpers, and provider diagnostics using an obsolete platform heuristic.
Readiness now reads the validated staged manifest. Provider status records the
same backend choice as process launch before probing. Failed original probes
retain their version diagnostic; failed native probes do not retry through the
original implementation. Regression fixtures exercise both cases.

Acceptance receipts include the consolidated compiler lock and all tracked
inputs under each owning Rust crate and its local crate dependencies. Changing
a shared decoder or compiler pin invalidates the receipt, even when the named
feature module is unchanged.

## Evidence and limits

The recovered workspace passed a frozen pnpm install with Node 26.12.1 and pnpm
12.14.0. Local checks on the recovered candidate include:

| Check | Observed result |
| --- | --- |
| Produced-toolchain Rust workspace, `--locked --workspace --all-targets` | 219 passed, zero failed or ignored |
| Migration source guard | 7,396 protected recovery files and 10 mapped ports |
| Immutable upstream README | Exact match to 521ac7c, 5,256 bytes |
| Contracts, client runtime and web typechecks | Passed |
| Server typecheck after fixture/dependency repairs | Passed |
| Persistence, restart, project and subagent regression files | 120 passed, zero skipped |
| WebSocket, replay, cancellation, reconnect and client-state files | 337 passed |
| Original/native provider-process scenarios after review fixes | 29 passed |
| Compiler installer and packaging regressions | 6 passed |
| Original server and web bundles | Built successfully |

[Hybrid CI](../../.github/workflows/hybrid.yml) repeats the gates on the published
candidate and retains exact-run JSON receipts and compiler/artifact identities.
The application job also checks WebSocket, replay-provider, cancellation,
connection and durable-recovery behavior and boots the built client in Chromium.
It then runs the original client with original and Rust services, creates a local
project, approves a free replay turn, reloads, and restarts the server against the
same state. A fresh browser context carrying only the existing session cookies
must recover the completed thread. This gate retains screenshots, accessible
snapshots and sanitized logs along with its pass/fail receipts.
Historical adapter proof strings now say native executable; aggregate receipts
record the actual BlueFe route instead of mislabeling it native Lust.

Local browser qualification was blocked by a truncated browser download and
this execution environment's browser socket restriction. This is not a passing
browser result. GitHub-hosted browser checks and their retained receipts are
the acceptance evidence for that part of the recovery.

These bounded Linux checks do not establish complete macOS/Windows/mobile
release acceptance, accessibility parity, redb production cutover, full Connect
replacement or an all-Rust product. The port map keeps source retirement blocked
until feature and platform evidence is explicitly accepted. Original
implementations remain available while those gates are incomplete.

## Upstream currency

The integrated source baseline remains nightly
`v0.1.8-nightly.20261109.4392`, commit
`521ac7c218b3faa74281fb72fc36ce74a0951fb3`. Detection records the newer published
nightly `v0.1.9-nightly.20261110.4452`, commit
`822671581383a357c5dc8d3ea9984b408c3e03c6`, without pretending it is integrated or
qualified. A subsequent nightly update must review mapped translated paths and
contracts, merge unported source, translate affected behavior and rerun the
relevant acceptance gates. No source percentage or changed pin establishes parity.
