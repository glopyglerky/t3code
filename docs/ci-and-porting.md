# CI and the Rust porting queue

Upstream `pingdotgg/t3code`'s default branch is trusted as working. CI qualifies our Rust replacements, their production adapters, and their integration with the retained application. It also compares fixed performance and reliability workloads against immutable stock services. These comparisons and behavior oracles are the explicit exception to the rule against rerunning unrelated pristine upstream suites or the original-only client flow.

## What each run checks

The qualification planner compares the current commit with a verified successful ancestor. It reads Cargo dependencies and [the port map](../port-map.json) to select work.

| Change                                                                             | Qualification                                                                                                                                                                         |
| ---------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Rust implementation                                                                | Changed crates and their reverse dependency consumers, plus affected production adapters, the native client flow and paired performance/reliability when production execution changes |
| Mapped adapter or shared contract                                                  | Affected Rust ports, adapter typechecks and differential checks, client/server integration and paired performance/reliability for production changes                                  |
| Port tests only                                                                    | Relevant Rust, mapped component acceptance, or adapter tests without rebuilding the client solely for a test change                                                                   |
| Unmodified upstream implementation imported at its exact Git identity              | No independent upstream test run; mapped contracts and fork integration still select their checks                                                                                     |
| Port-map changes limited to integrated and reviewed commits and source history     | Source guard; every other mapping value must be unchanged, and actual runtime changes still select their checks                                                                       |
| Documentation only                                                                 | Planning and lightweight tooling checks                                                                                                                                               |
| Shared locks, compiler pins, CI policy, or an unclassified fork integration change | Full fork qualification                                                                                                                                                               |
| Immutable stock comparison advances                                                | Full fork qualification against the new source; earlier measurements cannot qualify a different oracle                                                                                |

Full fork qualification covers four gates: native Rust and component checks, production adapters, the retained client with Rust services, and paired performance/reliability. Storage is a production boundary, so storage-only runtime changes select the client and comparison gates too. Adapter checks use controlled fixtures and the trusted upstream implementation as a parity reference. The client flow checks onboarding, a provider approval and completed response, reload, and recovery after server restart. It uses no paid provider turns.

Mapped component acceptance scripts have explicit executable requirements. CI builds those targets with the pinned compiler and runs changed scripts; a full qualification runs all mapped component checks. Unknown test mappings fail planning. Builds retain the verified compiler package and locked Cargo and pnpm dependency gates. Expensive jobs run only when selected.

The final `Record verified qualification` job always runs and explicitly depends on all four gates and the plan. It rejects failure, cancellation and unexplained skips. A gate may be skipped only when the generated plan says it is unnecessary. A successful performance job also needs its exact report artifact and digest; a green job label without that evidence cannot issue a receipt.

When a required comparison fails, qualification reports that failed job directly. It downloads a comparison report only from a successful selected job, and the receipt validator checks job results before opening evidence files. Missing artifacts cannot hide the originating failure behind an empty download name. Failed comparison artifacts remain available separately; no failure can issue a qualification receipt.

## Comparing speed and reliability

The [fixed contract](../tools/performance-contract.json) registers eleven required rows before measurement. It uses 256 navigation threads, a separate write thread, 3,000 messages, a 1,500-item history and fixed payload sizes. Every attempt runs eight process pairs in a seeded, balanced order. Each process performs three excluded warmup cycles and twelve measured cycles. The pair order, seed, sample counts and 10,000 bootstrap resamples cannot change within qualification.

| Workload                          | Recorded and gated timings                                                                      |
| --------------------------------- | ----------------------------------------------------------------------------------------------- |
| Service open and first operations | Service acquisition plus the first complete API cycle, including native process startup and IPC |
| Sidebar shell                     | Median and p95 latency for active navigation                                                    |
| History window                    | Median and p95 latency for a bounded view of a long completed turn                              |
| Message search                    | Median and p95 latency for indexed message matching                                             |
| Accent search                     | Median and p95 latency for upstream accent and substring/GLOB semantics                         |
| Visit commit                      | Median and p95 latency for a committed partial visit mutation and transactional readback        |

The stock side runs from a full export of the commit and tree in [upstream.lock.json](../upstream.lock.json). Relative and workspace imports stay inside that export, including the original SQLite wrapper. Hosted comparisons install the same pinned Node, pnpm and frozen workspace lock as the application job. The small comparison-import linker uses that installation's exact package instances; it cannot substitute an isolated npm test runtime. Each stock workspace preserves its corresponding production dependency and peer resolution while directing workspace imports into the immutable export.

Qualification verifies the installed pnpm lock against the application lock, checks the package manager and isolated virtual store, and rejects external package links outside that store. It records the installed lock, pnpm metadata, Node executable, export archive, source locks and package hashes. Matching a few direct dependency versions alone is insufficient to establish the full runtime provenance. Component and adapter tests may continue to use their separate locked fixture runtime; that runtime does not qualify the stock/native speed comparison.

The native side uses the production Omdb service and projection/search adapters, with the exact release helper produced by the same workflow run. The native manifest records the source commit and tree, clean state, toolchain-lock hash, compiler route, release profile and build run. Both paths include their actual service, serialization and database work. SQLite retains its production WAL/NORMAL settings and omdb retains Immediate commits. The report names that durability difference, and the visit workload includes its cost.

Correctness runs precede all timing. Both implementations must return the same complete results and order, perform a visible transactional mutation, roll back a deliberate failed transaction, and preserve a committed mutation across service close and reopen. Explicit nonempty cardinalities prevent a fast empty fixture from passing. Every measured response is checked against those validated results. A correctness failure stops the comparison and suppresses speed ratios. Close/reopen and rollback evidence do not establish recovery from power loss, disk corruption or every operating-system failure.

Each row reports the geometric mean of its paired native/stock latency ratios and a 95% bootstrap interval over whole process pairs. A ratio below 1 means lower native latency for that workload. Every row must have its whole interval at or below 1 to pass. An interval wholly above 1 is a regression; an interval crossing 1 is inconclusive and also blocks qualification. There is no aggregate pass that can hide a slow row, no allowed slowdown budget, no outlier removal and no discarded retry. Wide intervals produce an additional precision warning, not a relaxed pass.

The report retains all request timings, process-pair order, median, p95, maximum and per-row ratios. Process-tree RSS and GC observations are advisory because runtime and operating-system accounting differ. The cold row means a fresh service/process and first API cycle; it does not mean the operating-system page cache was flushed. The suite currently measures these persistence APIs on the qualified Linux route. It does not establish that every app operation, client, network path or platform is faster.

The [persistence measurement record](performance/persistence-api.md) tracks the method and decisions. The [first complete baseline](performance/measurements/2026-11-14-baseline.md) records every observed slowdown and its raw evidence. Its sandbox could not enumerate descendant processes, so the initial zero RSS readings mean unavailable data and establish no memory improvement. That report retains its original numbers with an explicit erratum; later attempts must report unavailable memory observations as such.

The [readiness-cache comparison](performance/measurements/2026-11-14-readiness.md) records the first isolated optimization and its full fixed rerun. Read paths improved against the earlier native implementation, but all eleven rows still regressed against stock. Writer behavior was unchanged; differences between those runs do not establish a write optimization.

The [dispatcher comparison](performance/measurements/2026-11-14-dispatcher.md) and [hosted readiness comparison](performance/measurements/2026-11-14-hosted-readiness.md) preserve the subsequent measurements and remaining regressions. The dispatcher attempt used a fresh owned directory on the workspace filesystem because the shared temporary filesystem lacked capacity. Both implementations used that same medium and verified frozen dependencies. Its differences from earlier local attempts cannot be attributed solely to the dispatcher change.

These measured persistence API slowdowns make the existing native persistence path the next porting priority. New transport and platform cutovers wait behind correcting and qualifying that path. Each optimization is measured separately against the unchanged workload, and every remaining slowdown continues to block promotion. The measurements compare this fork with stock; they do not establish that the upstream sync introduced the regressions.

The dispatcher correction adds one explicitly joined OS thread with an 8 MiB stack for the serial request loop, while retaining its two Tokio workers. Existing bounded point, range and observation reads can execute directly on that dispatcher; ordinary library calls and spawned or local Tokio tasks continue to offload them. Writer reservation, commit, rollback and lifecycle operations retain their workers, and Immediate durability stays unchanged. This removes repeated worker scheduling without changing the read algorithms. The extra thread and reserved stack are a deliberate resource cost, not a memory improvement. Actual-helper tests cover the 16 MiB frame and 64-level nesting limits and clean rejection beyond them. Its recorded comparison still fails the performance gate.

The next isolated correction retains a lazily opened engine table handle within each immutable read transaction. Only a successful first open can populate it; an old transaction continues to read its original snapshot after a later commit or catalog change. Each active read transaction retains one table handle and its root page until it ends. There is no row cache, database-wide cache or writer cache. The change avoids repeated catalog/type checks and root-handle construction while preserving value validation, read order, byte limits and integrity checks when opening the database. Its speed and resource effects remain unqualified until the unchanged paired workload runs again.

The receipt validator independently reconstructs cold, median and p95 pair values from the retained worker samples, checks result digests and fixture cardinalities, and recomputes the bootstrap verdict. It binds the report hash to the current job output, source/tree, policy digest, immutable upstream identity, executable and compiler-lock hashes, workflow run and successful attempt. A partial, stale, debug, dirty-source or diagnostic report cannot qualify.

Build, measurement and receipt-writing attempts are recorded separately. Rerunning a failed comparison may reuse the same run's successful release build. Rerunning a failed application job may reuse its successful comparison from an earlier attempt. Qualification downloads the exact artifact named by the successful performance job's attempt output and validates its digest. A later receipt-writing attempt never relabels those measurements as newly collected.

For local diagnosis, run the same suite with an optimized helper and a new output directory outside the checkout:

```sh
node tools/performance-persistence.mjs \
  --database /absolute/release/t3-code-ls-db \
  --manifest /absolute/release/manifest.json \
  --output /absolute/new-performance-attempt \
  --mode diagnostic
```

Diagnostics preserve evidence but cannot issue a qualification receipt. CI supplies the exact plan and uses `--mode qualify`. Avoid concurrent builds or unrelated load during a local timing attempt. Never reinterpret a debug or locally modified build as the hosted release result.

### Recording every outcome and decision

Every attempt writes `performance.json`, `performance.md` and raw worker evidence. The output directory must be new. CI uploads `hybrid-performance-proof-<run-attempt>` even after failure and keeps each attempt separate for 30 days. A rerun cannot overwrite an earlier failed or inconclusive artifact. The job summary lists every measured row, including slowdowns and uncertainty.

For a release or main promotion, preserve the generated measured Markdown and compact raw JSON under `docs/performance/`. Record the measured source SHA, policy and input hashes, all relevant run/attempt links, and the exact successful report digest. Keep failed and inconclusive attempts accessible, with their failure reason and subsequent change. Record each deliberate divergence from upstream, the reason for retaining it, its observed cost or benefit, and the scope of the evidence. Do not claim a speedup before it is measured or omit a slowdown because another operation improved.

A report commit names the source that was measured. It cannot measure its own later commit ID. A documentation-only follow-up may retain the earlier measurement reference only when the planner proves that every intervening change leaves the runtime, comparison oracle and policy unchanged. A new production or policy change requires a new comparison. The [CI reference audit](porting/ci-reference-audit-2026-11-14.md) records the upstream and compiler projects consulted, which practices this workflow adopts, and why narrower compiler evidence cannot establish application speed or platform acceptance.

## Reusing qualification

CI only accepts a qualification receipt from a completed successful run of this repository's qualification workflow. The receipt must match the source commit, Git tree, and current CI policy digest, and every selected gate must have succeeded.

A matching receipt for the exact current commit lets a repeated run reuse qualification. A receipt for an ancestor anchors the changed-file comparison. A previous push SHA alone is insufficient: a canceled or failed intervening push must not hide its changes.

Missing, expired, malformed, inaccessible, or incompatible receipts select full fork qualification. Changed CI policy also prevents reuse. This fallback costs a full run rather than silently skipping checks. Restoring a fork modification to pristine upstream still qualifies the changed integration. Edits or deletions of recorded platform receipts always run the source guard, including receipts stored under `docs/`.

Performance references retain their original measured commit, run, attempt and report digest when a documentation-only descendant reuses them. The receipt writer checks the full interval from that measured source, so a chain of documentation receipts cannot conceal an intervening runtime or oracle change. Exact-source qualification of the candidate is still required before promotion.

## Keeping main qualified

Build and qualify a candidate branch first. Do not advance `main` while any required gate is failed, cancelled, missing or inconclusive. Keep the last qualified main available while repairing the candidate. The first failed sync run and the resulting connection repair are recorded in [the 913d7fc4 integration report](porting/default-913d7fc4.md).

After the complete candidate workflow succeeds, fetch both refs and check out that exact candidate. With a token that can read Actions evidence, run:

```sh
python3 tools/ci_promotion.py \
  --candidate "$(git rev-parse HEAD)" \
  --expected-main <fresh-full-main-SHA> \
  --output /absolute/promotion-proof.json
```

The helper is read-only. It requires a clean checkout, a completed successful exact-candidate receipt under the current policy, valid performance coverage and fast-forward ancestry from the live remote main. It reads main again before returning. The resulting `update_ref` request contains the candidate SHA, expected old main SHA and `force: false`. Submit only that fresh request through a compare-and-set ref update, then verify the resulting remote main. If main moves, fetch, integrate as needed and requalify the new candidate; do not force the old request.

The intended required repository check is `Record verified qualification`. At the 2026-11-14 inspection, GitHub reported `main` as unprotected. The available connector had no administration-write operation and the browser had no authenticated administration session, so no branch protection was enabled. The workflow and promotion verifier enforce this project's publication procedure; they cannot prevent an administrator or another client from bypassing it while the repository rule is absent. Enable the required check and block force pushes in GitHub's repository settings when administration access is available.

## Reading the porting queue

Each qualification run generates a JSON porting plan and a job summary. This uses the existing CI triggers; it adds no schedule. The planner resolves the advertised upstream default branch once, records its immutable commit, and fetches Git tree metadata. It compares three committed snapshots: the integrated upstream baseline, the detected upstream commit, and the fork's `HEAD`.

The queue orders work as follows:

1. Adapt mapped Rust ports to upstream changes, including shared contracts, adapters, and originals already recorded as retired.
2. Resolve fork conflicts, preserve fork policy, account for upstream removals, and review build or dependency boundaries.
3. Import clean upstream changes whose fork copies still match the integrated baseline.
4. Complete acceptance for integrated ports, then integrate candidate ports.

Each port item names its owner, Rust paths and crates, tests, compiler routes, and missing platform evidence. File changes include exact modes and object IDs. Rename suggestions require identical Git objects; they do not infer equivalent behavior.

The queue is a work plan. It does not generate Rust implementations, merge changes, update pins, accept a port, or delete originals. Mechanical import candidates still belong in a complete integration change with its affected Rust work.

Detected, integrated, and qualified revisions remain separate. Discovering a new default-branch commit changes neither the integrated pin nor qualification. CI receipts for this fork also do not replace the per-platform acceptance receipts required to retire original code. The source guard validates those receipts against their current inputs.

## Default-branch snapshots and nightlies

The integrated source can advance to an immutable default-branch commit before a nightly release contains it. Such a lock records `channel: "default-branch"`, the branch name, exact commit and tree, and a commit URL. Nightly tags and release identifiers belong to the separate `nightly_reference`, not that source identity.

The nightly checker compares Git ancestry as well as trees. An equal revision is current; an ancestor nightly is already integrated and creates no reverse-import or removal work. A newer or divergent nightly requires review. Complete tree differences remain in the report, and missing shallow ancestry fails explicitly. Nightly source locks retain their moved-tag and publication rollback checks. None of these reports changes the source pin or qualification.

## Generate plans locally

Run from the repository root. Reports must stay outside the source checkout.

```sh
t3code_report_dir="$(mktemp -d /tmp/t3code-ci.XXXXXX)"
python3 tools/plan-porting.py --repo-root "$PWD" --fetch-default \
  --output "$t3code_report_dir/porting-plan.json" \
  --summary "$t3code_report_dir/porting-plan.md"
python3 tools/ci_plan.py --repo-root "$PWD" --offline \
  --output "$t3code_report_dir/ci-plan.json"
```

For an already fetched upstream snapshot, replace `--fetch-default` with `--upstream-commit <full-commit-SHA>`. The two options are mutually exclusive. The porting planner reads committed source and requires committed, unchanged lock and map files.

`ci_plan.py --offline` skips remote receipt lookup and plans full fork qualification. `--full` explicitly forces that scope even when reusable evidence exists. These commands write plans; the workflow executes the selected checks.

The current integrated upstream pin is recorded in [upstream.lock.json](../upstream.lock.json). The [913d7fc4 integration](porting/default-913d7fc4.md) updates schema 91, auth and projection APIs, search, provider maintenance, Codex generation and connection behavior. Application Rust still uses the pinned rustc-derived BlueFe package through Lustr's compatibility coordinator. Native Lust and platform retirement acceptance remain separately recorded in the [toolchain lock](../toolchain.lock.json) and [port map](../port-map.json).
