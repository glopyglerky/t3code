# CI and comparison references, 2026-11-14

This audit selects practices for comparing the fork with stock T3 Code and
keeping qualified changes on main. It covers T3 Code, canonical Lust, the
Lustr/BlueFe toolchain this application consumes, and the relevant ts-rust
downstream. It is not a survey of every ecosystem project. The decisions below
describe the intended comparison and qualification contract; their implementation
and passing evidence must be established by the fork's own candidate workflow.

## Source and compiler identities

| Source                                                                                                                                  | Audited revision or artifact               | Meaning for this fork                                                                                                                                                                                                                                         |
| --------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [T3 Code](https://github.com/pingdotgg/t3code/tree/913d7fc45ec5814fff25c819cfe4de193ea42d63)                                            | `913d7fc45ec5814fff25c819cfe4de193ea42d63` | The integrated upstream oracle. Comparisons must use this immutable source, not a later moving main or the fork's retained SQL copy.                                                                                                                          |
| [Canonical Lust](https://github.com/t3dotgg/lust-lang/tree/240e08d91d4aed0299639f6f11fd522912b248f9)                                    | `240e08d91d4aed0299639f6f11fd522912b248f9` | An experimental Rust-subset compiler. Its warm-daemon methods are useful references, but it is not the compiler used for the full application build.                                                                                                          |
| [Lustr coordinator](https://github.com/glopyglerky/lustr/commit/07470b20464bf097c3c88983124aff56c5e44f9a)                               | `07470b20464bf097c3c88983124aff56c5e44f9a` | The coordinator revision packaged with the application's pinned BlueFe release.                                                                                                                                                                               |
| [Lustr measurement documentation](https://github.com/glopyglerky/lustr/tree/82d77539680fec9cf822ca8231d1abb985069c48/docs/compiler-set) | `82d77539680fec9cf822ca8231d1abb985069c48` | A separately identified documentation snapshot; reading it does not update the application's coordinator pin.                                                                                                                                                 |
| [BlueFe preview](https://github.com/glopyglerky/blueFe/releases/tag/preview-2026.11.09-2)                                               | `preview-2026.11.09-2`                     | Compiler source `1b74388e196ee8813f38ba9eeb6d4ab43b6b1071`, coordinator `07470b20464bf097c3c88983124aff56c5e44f9a`, LLVM `0a5579eb649d3634a9c30ac873300cc2944fa070`. The release tag records an assembled package, not a substitute compiler-source identity. |
| [ts-rust](https://github.com/pingdotgg/ts-rust/tree/91f3d021c4f1f102cebee704ac285e85ae0c8ff5)                                           | `91f3d021c4f1f102cebee704ac285e85ae0c8ff5` | A relevant compiler-port example with explicit oracle comparisons and separate candidate qualification. Its current benchmark workflow is a method reference.                                                                                                 |

The [BlueFe release description](https://github.com/glopyglerky/blueFe/releases/tag/preview-2026.11.09-2)
states that the package includes produced compiler tools and sysroots, identified
bootstrap Cargo, and the Lustr coordinator. Native Lust/TCC is absent. The
application's separate `lust-toolchain.json` subset pin must therefore remain
distinct from the full-Rust compatibility build. GitHub's
[old `glopyglerky/lust-lang` endpoint](https://api.github.com/repos/glopyglerky/lust-lang)
redirected to repository ID `1450430441`, the same repository now named
[`glopyglerky/lustr`](https://api.github.com/repos/glopyglerky/lustr).

## Decisions to adopt or retain

### Require complete qualification before promotion

Adopt T3's single final qualification check. Its pinned
[CI workflow](https://github.com/pingdotgg/t3code/blob/913d7fc45ec5814fff25c819cfe4de193ea42d63/.github/workflows/ci.yml)
runs `Check` with `always()`, includes every gate in its dependencies, and rejects
failure, cancellation, and unexplained skipped jobs. A skip is allowed only when
change detection explicitly marked the represented work unnecessary. Server
test execution also requires its measurement artifact. Change detection widens
to all work when the diff is unavailable; main pushes run all Linux jobs.

For this fork, the aggregate must cover comparison correctness, report validity,
the declared performance decision, and existing application qualification.
Promotion must bind the successful run to the exact candidate commit/tree and
current CI policy. A green individual job or an artifact from another commit is
insufficient. Repository rules must enforce the required check for all allowed
main updates; defining a workflow alone does not prevent an unguarded push.

Retain dependency-aware planning to avoid unrelated work, but do not treat an
unknown dependency, missing baseline, or failed detector as permission to skip.
Compiler and CI-policy changes still require the broader fork qualification.

### Compare the active application path

Adopt T3's [v2 regression methodology](https://github.com/pingdotgg/t3code/blob/913d7fc45ec5814fff25c819cfe4de193ea42d63/docs/internals/performance-regressions.md).
It warns that old v1 fixtures can give attractive timings without exercising the
current transport. The existing v2 workloads use real projection and client
reducers, large turns, deferred work, history paging, shell snapshots, and resume
behavior. [Commit 476fc9b](https://github.com/pingdotgg/t3code/commit/476fc9be6691ab0d81e936f4cc674983cf3913be)
updated budget fixtures to match current client requests; [commit 3e03072](https://github.com/pingdotgg/t3code/commit/3e03072eb634a0cc5e6b3b6429d5f7656970fe68)
bounded thread-list work independently of long thread histories.

The next comparison should exercise the production native projection/search
services against their immutable upstream implementations, using the same
logical data and requests. Exact result, error, ordering, pagination, and
restart checks precede timing. Existing deterministic row and wire-byte budgets
remain separate gates. This measures those application service boundaries;
it does not establish browser interaction latency or the speed of the whole app.

### Pair measurements and keep the complete evidence

Adopt the [Lustr measurement contract](https://github.com/glopyglerky/lustr/blob/82d77539680fec9cf822ca8231d1abb985069c48/docs/compiler-set/benchmark-method.md):
frozen inputs, explicit configuration, excluded warmups, fixed sample counts
and seed, balanced order, and raw observations. Its two-lane studies use at
least eight even pairs. Larger lane sets need position balancing rather than
only forward/reverse order. The [paired implementation](https://github.com/glopyglerky/lustr/blob/82d77539680fec9cf822ca8231d1abb985069c48/crates/lustr/bench/paired.py)
operates on log timing ratios and retains seeded uncertainty estimates.

Run baseline and candidate sequentially on the same runner. Keep preparation,
installation, seeding, correctness checks, and hashing outside the stated timed
interval. Separate process startup and steady-state API batches. Record all
samples, order, median, p95, worst, paired ratio, and interval. Declare sample
counts, regression limits, and aggregation weights before inspecting results.
Do not discard outliers, retry slow workloads until they pass, or stop early
after a favorable result. A failed correctness or source check must leave a
failed report with no accepted speed summary.

Record upstream and candidate revisions, fixture identity, helper/toolchain
hashes, selected backend, runtime version, and relevant host observations. Any
normalization must be narrow and documented; raw outputs must remain available.
Bootstrap intervals on a shared host do not eliminate scheduling, thermal,
cache, or carryover effects. A process-cold run does not flush OS caches.

### Preserve failure and restart coverage

Retain the oracle-first approach in canonical Lust's
[gate](https://github.com/t3dotgg/lust-lang/blob/240e08d91d4aed0299639f6f11fd522912b248f9/scripts/ci.sh)
and [speed methodology](https://github.com/t3dotgg/lust-lang/blob/240e08d91d4aed0299639f6f11fd522912b248f9/docs/speed.md).
They distinguish accepted/rejected inputs, warm/cold results, and runtime output
parity. [Commit b97ce46](https://github.com/t3dotgg/lust-lang/commit/b97ce4616695648dbf6c935cf2b1210225f228b9)
added varied real edit types with rejection states and cold-oracle checks.

For the application, preserve transaction failure, interruption, restart, stale
state, and uncertain-commit tests alongside the timing comparison. No failed
native write may retry through SQLite. The [ts-rust candidate pipeline](https://github.com/pingdotgg/ts-rust/blob/91f3d021c4f1f102cebee704ac285e85ae0c8ff5/scripts/goport/candidate.sh)
is a useful evidence-management precedent: source/pin-bound reuse, comparison
against an accepted baseline, and retained failed attempts. Reuse those principles,
not its project-specific infrastructure or approval process.

### Keep build and report checks independent

T3's [CI workflow](https://github.com/pingdotgg/t3code/blob/913d7fc45ec5814fff25c819cfe4de193ea42d63/.github/workflows/ci.yml)
boots built web assets and opens an old-state fixture in the built desktop app.
Retain the fork's client and startup qualification: source-level API equality
cannot catch every bundling, migration, or client lifecycle failure.

The [ts-rust application benchmark workflow](https://github.com/pingdotgg/ts-rust/blob/91f3d021c4f1f102cebee704ac285e85ae0c8ff5/.github/workflows/bench-apps.yml)
builds verified tools once, prepares inputs separately, keeps each workload's
competing lanes on one runner, and preserves result artifacts after failures.
Its [verifier](https://github.com/pingdotgg/ts-rust/blob/91f3d021c4f1f102cebee704ac285e85ae0c8ff5/scripts/bench-apps/verify.py)
checks diagnostics before publishing tables. Apply the same separation to
application result parity and measurement reporting. Missing expected artifacts
or scenarios must fail, not silently shrink the comparison.

## Decisions to defer or reject

| Decision                                                                                                 | Reason                                                                                                                                                                                                                                                                                                                          |
| -------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Do not rebuild or retune compilers in application comparison CI.                                         | Consume the verified pinned package. Compiler construction and qualification belong to the compiler repositories; rebuilding adds an unrelated treatment and cost.                                                                                                                                                              |
| Do not replace the current full-Rust compiler with canonical native Lust based on its benchmark results. | Its accepted language subset and daemon workloads differ from the complete application and dependencies. The current release does not contain it.                                                                                                                                                                               |
| Do not turn compiler-check speedups into application-runtime claims.                                     | Compiler latency, service latency, UI responsiveness, and full startup are separate operations. Each requires its own measured workload.                                                                                                                                                                                        |
| Do not claim platform acceptance from Linux results.                                                     | Windows, macOS, mobile, and their lifecycle behavior need their own evidence. Retained implementations remain required.                                                                                                                                                                                                         |
| Do not call JSON bytes or one process's RSS total app memory.                                            | Wire size, parent RSS, helper RSS, sampled summed RSS, and simultaneous process-tree peaks have different meanings. Report only the observed scope.                                                                                                                                                                             |
| Do not copy upstream's write-capable PR comment publisher into the benchmark job.                        | Read-only CI artifacts and job summaries suffice. T3's [publisher](https://github.com/pingdotgg/t3code/blob/913d7fc45ec5814fff25c819cfe4de193ea42d63/.github/workflows/thread-transfer-report.yml) separately uses trusted default-branch code and validates run/artifact identity. Any future publisher needs that separation. |
| Do not treat open demonstrations, source guards, or a few green component jobs as a green whole product. | Adoption, exact-input correctness, performance, and platform acceptance are separate claims. The evidence below illustrates the difference.                                                                                                                                                                                     |

## Verified evidence and its limits

| Evidence                                                                                                                                                     | What was verified                                                                                                                                        | What it does not establish                                                                                                       |
| ------------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| [T3 CI run 43590906566](https://github.com/pingdotgg/t3code/actions/runs/43590906566), `913d7fc45ec5814fff25c819cfe4de193ea42d63`                            | Seventeen jobs, including the final `Check`, succeeded; mobile static analysis was intentionally skipped.                                                | Acceptance of this fork's native replacements.                                                                                   |
| [ts-rust CI run 40644231445](https://github.com/pingdotgg/ts-rust/actions/runs/40644231445), `0c822c91504b9e2b58b2b1da4e0e4273e43100b2`                      | Linux x64/ARM64, macOS, Windows, wasm, and lint/codegen jobs all succeeded.                                                                              | A successful run of the separately audited newer benchmark workflow, or adoption of Lust.                                        |
| [ts-rust Lust PR 93](https://github.com/pingdotgg/ts-rust/pull/93), head `8f27e56ed1428c2a266861c9b8546cd912b45994` at audit                                 | The proposal and its bounded fixture measurements were readable; the PR was open and unmerged.                                                           | Merged downstream adoption or production qualification.                                                                          |
| [Canonical Lust successful-run query](https://api.github.com/repos/t3dotgg/lust-lang/actions/runs?status=success&per_page=3)                                 | The API returned no successful Actions runs at audit time.                                                                                               | That its documented local measurements were independently reproduced here, or that a workflow definition is a passing CI record. |
| [Pinned Lustr run 42583006080](https://github.com/glopyglerky/lustr/actions/runs/42583006080), `07470b20464bf097c3c88983124aff56c5e44f9a`                    | Linux/macOS compatibility, native subset, Cranelift, semantic, and minimum-Rust jobs succeeded. Windows compatibility failed, so the overall run failed. | A green whole-toolchain workflow or Windows acceptance.                                                                          |
| [BlueFe pinned-commit run query](https://api.github.com/repos/glopyglerky/blueFe/actions/runs?head_sha=1b74388e196ee8813f38ba9eeb6d4ab43b6b1071&per_page=20) | No Actions runs were returned for that exact compiler source. Package qualification is described in the separately published release evidence.           | An inferred green compiler CI run.                                                                                               |

The published [three-project fixed-workload study](https://github.com/glopyglerky/lustr/blob/82d77539680fec9cf822ca8231d1abb985069c48/docs/compiler-set/measurements/2026-11-09-cgu16-thinlto-v3/README.md)
is a useful example of a bounded decision: exact t3code-ls, gitr, and bluefin
revisions; before/after tests; preregistered per-case and aggregate limits; all
observations; and independent arithmetic review. It reports metadata-only root
checks against a matched compiler control, while explicitly retaining slower
results against stock Rust. It does not establish application execution speed,
full-build acceleration, complete project acceptance, or independent performance
replication. This audit read that evidence; it did not rerun the study.

The fork should retain the same distinctions in its own report: what ran, what
matched, what was measured, what failed, and exactly which promotion decision
the evidence supports.
