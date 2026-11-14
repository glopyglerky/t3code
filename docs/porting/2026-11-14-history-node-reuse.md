# History node-root reuse candidate

This is an unqualified, isolated experiment on top of
`ee178c237d5fad5842f950871003dc898ab26990`. It keeps the immutable stock comparison
at `5fe2f7e4567d9e530ded0dabc9d706db27d77b84` and does not incorporate the later
f751 integration. There is no measured speedup for this change. The separate
`ci/t3code-ls-history-node-reuse-20261114` branch must complete the normal hosted
hook, exact-source Rust and component tests, adapters, original client flow and
unchanged performance policy before it can be considered for promotion.

The existing main branch remains unchanged. The
[ee178 functional report](2026-11-14-ee178-functional-acceptance.md) establishes
the prior candidate's scoped Linux behavior; its eleven failed performance rows
still block qualification. Those results do not qualify this experiment.

## Why this is the next experiment

The retained [a2fb attribution run](../performance/measurements/2026-11-14-attribution-a2fb16d.md)
used the same fixed persistence workload with instrumentation confined to a
separate build. Across twelve measured history cycles, the API mean was
135.35 ms and the helper evaluation mean was 131.02 ms. Each cycle made about
6,059 point reads returning 6.35 MB and 61 scans. Most of that work belonged to
`application_projections_records`, which calls the retained-window selection
routine. The similarly named `application_projection_window_query` was small
for that fixture.

The get and scan wrappers accounted for inclusive means of 8.37 ms and 1.79 ms.
Those measurements identify repeated work; they do not establish a specific
JSON-parser or allocation cause. No sampled CPU profile was available. The
earlier instrumented result is diagnostic evidence, not a speed qualification,
and its source remains a2fb rather than the later ee178 repair.

The selected change removes only repeated node-root lookups within one
retained-window query. Metadata-page reuse, row cloning, JSON decoding,
relationship scans, global run-item loads, writer mutations and startup service
construction remain separate possible changes. Keeping them separate allows the
next uninstrumented comparison to assess this experiment on its own.

## Production scope and limits

`apps/server/src/persistence/redb_projection_window.rs` now creates a local
`NodeRoots` map inside `retained()`. After the original eligibility checks, a
lookup may retain a successfully validated root or a missing-node result. The
map is available only to an explicit immutable read session. Every hit still
passes the existing readiness and active-transaction check.

The first lookup uses the same compact metadata reader, table and physical
primary validation, root cell decoding and error order. Errors are not retained.
A missing node still falls back to the turn item's node ID; a node on another
thread is still resolved by its global primary ID. This is query-local state,
so another query or snapshot cannot inherit an old root or missing result.

The limits are 256 entries and 64 KiB of retained ID/root string bytes, plus
bounded map overhead. The string-byte limit is not a claim about total heap
usage. Entries that do not fit use the original read path. Writers retain their
staged overlay, observations, savepoint behavior and read-byte limits without
using the cache.

The actual history records path reaches this routine. The inherited-count
operation also uses it; the independent timeline lookup is unchanged. All three
existing global run-item consumers remain intact, including their cross-thread
semantics. Durable commit settings and protocol framing do not change.

## New regression coverage

Six tests exercise the affected behavior through real storage sessions and,
where relevant, the public application transaction boundary:

- Repeated shared and foreign roots, missing-node fallback, and correct reads
  after the entry limit is reached.
- Oversized roots and correct uncached reads after the string-byte limit is
  reached.
- Nonempty public history records that fold two completed-work groups using
  missing and foreign node roots, followed by exact rejection after both commit
  and rollback close the session.
- Eligibility before node access, first-use table/primary/root corruption, and
  failure results that are not cached.
- Old versus fresh snapshots across root updates, deletion and corruption.
- Writer staged updates, savepoint restoration, corruption rejection and the
  existing observation-byte limit, with no retained writer roots.

The public closure test is deliberately at the application boundary. A closed
read transaction has been consumed, so constructing an impossible closed
internal `Session` would not test a supported lifecycle.

## Local attempts, including failures

Both attempts invoked the actual pinned Lustr coordinator and its recorded
BlueFe toolchain. They used Cargo's locked dependencies, one CPU and one build
job, no incremental compilation, and zero development/test debug information.
The owned supervisor bounded time and log size and recorded source, working-tree
and tool hashes before and after execution. Fixtures used an owned overlay
directory; that directory was empty after both attempts.

```text
lustr --bluefe-root <verified-package>
  test --locked -p t3-code-ls-storage --all-targets -- --test-threads=1
```

| Attempt | Observed result                                                                                                                                                                                | What it establishes                                                                                                                                                           |
| ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 01      | Compilation completed. The storage library ran 208 tests: 207 passed and one new fixture failed during seeding. The process exited 101 after 143.525 seconds; the tests ran for 58.44 seconds. | The five other new tests passed, as did the existing 202 library tests. The all-target invocation stopped at the failed library suite; later target execution is not claimed. |
| 02      | Compilation received `SIGKILL` before test execution. The process exited 101 after 72.799 seconds, with no Rust diagnostic or failed assertion.                                                | The corrected fixture and current source have not completed the Rust suite locally. This is not a test pass.                                                                  |

Attempt 01 supplied `providerThreadId: null` in a run-attempt fixture.
`OrchestrationV2RunAttempt.providerThreadId` and the native attempt table both
require text. The real writer correctly returned
`Invalid("projection payload requires a text field")`. The correction changes
only that fixture value to `provider-{index}`. The production cache bytes are
identical between attempts; the public records and closure assertions remain.

Attempt 02 used the corrected source. Available memory was lower and shared
memory use higher than in attempt 01, but the available counters do not establish
the cause of the kill. The process was not rerun blindly, no unrelated temporary
files were removed, and no scope-reducing compiler or test option was introduced.

| Recorded quantity            | Attempt 01 | Attempt 02 |
| ---------------------------- | ---------: | ---------: |
| Child maximum RSS, KiB       |  1,589,288 |  1,145,424 |
| Available memory before, KiB |  3,552,328 |  2,772,500 |
| Shared memory before, KiB    |  5,050,284 |  5,790,644 |
| Available memory after, KiB  |  3,316,500 |  2,404,920 |
| Shared memory after, KiB     |  5,180,668 |  6,016,172 |

These are resource observations, not proof of an out-of-memory termination.
Both receipts report unchanged source and toolchain hashes. No local release
helper was built for this experiment, and no local latency benchmark started.

## Exact source and retained evidence

The original attempt used working tree
`f7c1364f9d1d17db392cf2cba246ec3289b5df5e` and 157-input digest
`1bdcede3a578c189c0b7557bb2e12b77d4ac07c2f807bd8e657440685e231d85`.
The corrected attempt used working tree
`e2f6290f849ca0f489f3f1ca0b939a2654e68bba` and input digest
`95249eb82b132f6291e4286a6b454e48172a9b3f3bd43b53ae59772b3a9a922e`.
Those trees precede this documentation addition. The corrected Rust file has
SHA256 `9421434f8db63a11493ddb5673800928d344fa32fa666471d68661455389b153`.

The [local evidence archive](evidence/2026-11-14-history-node-reuse-local.raw.tar.gz)
retains both complete logs, receipts, immutable input plans, supervisors, source
versions and patches. Its 14 regular entries have archive SHA256
`b582371d2afc8bb7851cf536970cc53aafd42f320f9054a8feac69f5ee03fa7a`.
An internal manifest binds the size and hash of every payload.

Hosted execution is the next gate because the local corrected test compilation
did not finish. It must test the exact published source and retain failures.
The fixed eight balanced pairs, three warmups, twelve samples, eleven individual
latency rows and confidence threshold remain as documented in the
[comparison method](../performance/persistence-api.md). Functional success and
an attractive individual timing cannot substitute for the complete policy.

## Subsequent hosted outcome

The preparation and local attempts above are historical records. Published
candidate `02296614b7d5989475fed25e86ebb5116766645f` completed
[run 43618303547](https://github.com/glopyglerky/t3code-ls/actions/runs/43618303547).
All 425 Rust tests passed, including the corrected node fixture, together with
29 component typechecks, 49 mapped scripts, production adapters, the normal
conditional hook and the controlled Linux client/recovery flow. Every one of
the eleven latency rows still regressed against stock, so performance and final
qualification failed. Main remains unchanged.

The [complete hosted report](../performance/measurements/2026-11-14-history-node-reuse.md)
retains all rows, medians, p95s, worst values, raw artifacts, exact identities and
the hook's actual conditional scope. No causal speedup is established by
comparison with earlier runs on other hosts. The local failed and incomplete
attempts above remain unmodified; successful hosted execution does not relabel
their outcomes.
