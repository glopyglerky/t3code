# Persistence API comparison

This suite compares the selected native persistence services with stock T3 Code at
the immutable revision in `upstream.lock.json`. It measures the real v2 projection
and search boundaries, including native IPC, serialization and transaction
commits. It is one qualification requirement; it does not establish whole-app,
browser, network, provider, or other-platform performance.

The fixed protocol is [performance-contract.json](../../tools/performance-contract.json).
The runner is [performance-persistence.mjs](../../tools/performance-persistence.mjs).
The [reference audit](../porting/ci-reference-audit-2026-11-14.md) records the
upstream v2 regression, paired measurement and qualification sources used to
choose this method. Both the [first complete baseline](measurements/2026-11-14-baseline.md)
and the [readiness-cache comparison](measurements/2026-11-14-readiness.md)
failed every latency row and remain part of the evidence.

## What runs

Both implementations receive the same synthetic application state: 256 normal
threads, one separate archived write thread, 3,000 search messages, one input
message, and a completed 1,500-item turn with a real user-message timeline row.
The remaining 1,499 items carry reasoning text with a 1,024-byte repeated payload.
The run, attempt, node, provider session, provider thread and provider turn have
coherent references. Every eighth normal thread is archived, leaving 224 active
threads. No provider, network account, credential or user database is involved.

The fixture is created through the pinned upstream SQLite and projection APIs.
The native template is imported and prepared through the production `Omdb.make`
startup path. Each validation and measurement worker gets its own file copy.
Seeding, copying, migration, index preparation and file hashing occur outside the
timed API calls. The source fixture is checked for mutation after the run.

| Workload         | Production request and purpose                                                                                                                                                   |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sidebar-shell`  | Active `getShellSnapshot` over the complete thread fixture.                                                                                                                      |
| `history-window` | `getThreadSnapshotWindow` with row limit 75, user-turn limit 10 and deferred work enabled, over the completed long turn. The result may be smaller because completed work folds. |
| `message-search` | Search for `benchmark needle`, limit 20, using the real message index and unchanged application ranker.                                                                          |
| `accent-search`  | Search for `ecole fer`, limit 20, exercising the upstream Latin accent substring/GLOB behavior.                                                                                  |
| `visit-commit`   | Apply a distinct `thread.visit-recorded` mutation to the archived write thread, read that row inside the same transaction, and wait for commit acknowledgement.                  |

The write thread is separate so each commit can be timed without changing the
read workloads. Every committed timestamp changes. Each returned write row must
equal the expected full row for that timestamp; a no-op cannot pass by returning
an unchanged digest. The write fixture resets between process pairs.

Stock SQLite uses its real WAL/NORMAL defaults with reader and writer workers.
Native omdb retains `Immediate` commit durability. This is an intentional
production durability difference. Its cost stays in the timed write result;
the suite neither weakens native commits nor changes the stock baseline to FULL
to manufacture parity. Rollback and reopen tests exercise application failure
semantics, but are not power-loss or hardware-failure tests.

## Establish stock identity and correctness first

The baseline is a complete `git archive` of the locked upstream commit, verified
against the locked tree. Relative and workspace imports resolve inside that
export. This avoids accidentally running a pinned top-level module with modified
fork dependencies. Each exported workspace receives its original importer's
installed external links, preserving dependency and peer instances; every
workspace alias is redirected to the stock export. External dependencies resolve
to the same installed packages as the corresponding production workspace.

All settings, patches, package/snapshot resolutions and shared importers in the
fork and upstream pnpm locks must agree. Only the exact existing `tools/oracles`
importer is allowed as an additional harness importer. Resolved direct server and
shared dependency versions are checked against the upstream production lock;
Effect and platform-node are required. Reports retain installed package manifest
hashes, the SQLite wrapper hash, the full source archive hash, both lock hashes,
Node version, native executable hash and compiler/toolchain identities.

Qualification also requires the installed pnpm application lock to match the
complete source application document, including transitive package/snapshot
resolutions. Installed package-manager metadata must match the source pin and
isolated virtual-store layout; the Node engine must match its exact application
pin. Every external target must resolve inside that source workspace's frozen
`.pnpm` tree. Reports retain the installed-lock, module-metadata and Node-executable
hashes, every external importer/name/version/manifest/path descriptor, and a
hash of their explicit field tuples. Missing or incompatible provenance prevents
qualification. Diagnostic mode records the issue without claiming a production
runtime qualification. The first two timing attempts predate this additional
attestation; their original diagnostic limits remain documented.

Before any speed verdict, both backends must pass:

1. Deep equality of every initial result, including array ordering.
2. Cardinality witnesses: 224 active threads, bounded nonempty history with run
   and node rows, a history JSON upper bound, and bounded nonempty results from
   both searches.
3. Same-transaction read-after-write visibility and the exact deliberate rollback
   sentinel. An unrelated exception or failed assertion cannot count as rollback
   evidence.
4. Exact restoration of the prior row after rollback.
5. Persistence of the committed visit after closing and reopening each production
   persistence service.
6. Equality of every cold, warmup and measured response to the validated result,
   including the changing write timestamp.

Correctness, durability-behavior or source-identity failure makes the attempt
failed and sets its speed summaries to null. Timing cannot compensate for an
incorrect or empty result. The benchmark retains the failure instead of reducing
the workload or silently using a fallback backend.

## Fixed paired measurements

Qualification uses eight process pairs, three excluded warmup cycles and twelve
measured cycles per worker. Seed `1949524847` produces four native-first and four
stock-first pairs in a fixed balanced order. Backends run sequentially on the
same host. Each pair keeps the same fixture and operation order; the five warm
operations rotate position within each measured cycle. All durations, including
slow observations, remain in the report.

The cold row measures service acquisition plus the first call to all five
operations in a fresh worker. The worker records service acquisition and each
first-call duration separately. Module loading and result fingerprinting are
outside that row's timed sum. Fresh process does not mean cold OS page cache:
the suite does not flush the host page cache. Warm API measurements include the
selected production implementation, worker/native IPC, serialization and commit
acknowledgement; result assertions and fingerprints run after each timer stops.

Each warm workload has independent median and p95 verdict rows. The cold row
uses one elapsed sample per process. For every row, the suite computes each
pair's native/stock ratio, averages the log ratios, and exponentiates that mean.
A seeded 10,000-resample bootstrap resamples whole process pairs, preserving the
relationship between requests within each worker. The reported interval uses
the 2.5th and 97.5th percentiles. Aggregate timing columns retain median, p95 and
worst observed durations; the paired ratio can differ from the quotient of the
two pooled timing columns.

| Evidence                                                        | Decision                                            |
| --------------------------------------------------------------- | --------------------------------------------------- |
| Entire 95% paired ratio interval is above 1.0                   | Measured slowdown; reject qualification.            |
| Entire interval is at or below 1.0                              | This row passes for the measured workload and host. |
| Interval overlaps 1.0, or fewer than eight pairs ran            | Inconclusive; cannot qualify or claim equivalence.  |
| Any row is missing, duplicated, changed, failed or inconclusive | The suite cannot qualify the candidate.             |

There is no allowed 10% slowdown. Relative interval width above 10% is only a
precision warning. Every fixed row gates separately, so a speedup elsewhere
cannot hide a regression. The signed latency delta is `(ratio - 1) * 100`:
negative is faster, positive is slower. The JSON's
`sequential_requests_per_second` is the inverse mean service time, not measured
concurrent throughput or whole-app capacity.

## Reliability, noise and memory scope

Workers have a 15-minute timeout and a 16 MiB stdout/stderr limit. Cancellation
or failure terminates only the owned process group, escalates to SIGKILL after
two seconds, and waits for that worker to close before fixture cleanup. Each
attempt uses a fresh output directory; existing evidence is never overwritten.
All individual worker configs, outputs and reports are retained. A rerun is a
new complete attempt with a recorded reason, not a replacement for a slow pair.

Reports include host CPU, available parallelism, OS, Node version, load before
and after, observed GC count/duration, and raw sample distributions. Shared host
load, CPU frequency, page-cache carryover, scheduling and finite tail samples
still limit inference. No forced GC or discarded outlier is used to make a
result pass.

Whole-process RSS is sampled every 25 ms by adding the worker and its visible
descendants. It can miss short peaks and count shared pages more than once. If
the process tree cannot be enumerated, the metric is null with an explicit
unavailable status and reason. Node's separately labeled peak RSS excludes native
helpers. Neither field gates qualification or supports a whole-app memory or
memory-safety claim. The first baseline's zero-valued process-tree observations
were an instrumentation error; its retained report contains an explicit erratum.

## Run and retain evidence

Diagnostic execution accepts an explicitly supplied helper and can measure a
dirty source tree, but cannot qualify a commit:

```sh
node tools/performance-persistence.mjs \
  --database /absolute/path/to/t3-code-ls-db \
  --output /absolute/path/to/new-attempt-directory \
  --mode diagnostic
```

Use `--phase validation` to debug fixture and correctness setup without a speed
claim. Diagnostic sample overrides remain visible in the report; qualification
requires the exact fixed contract. The diagnostic runner exits successfully
after a completed measurement even when its recorded speed verdict regresses.
Consumers must read `status` and `qualifies`, not infer acceptance from that exit.

CI qualification adds `--mode qualify --manifest /absolute/bin/manifest.json
--plan /absolute/ci-plan.json`. It requires a clean exact source tree, the
same-run release artifact, matching compiler/toolchain/helper hashes, and the
current policy and immutable upstream identities. Source cleanliness and
identity are checked again after measurements. Qualification exits nonzero for
a regression, failure or inconclusive result.

The supplied native executable and release-manifest hashes are rechecked after
the workers finish, including in diagnostic mode. A concurrent binary replacement
cannot attach mixed measurements to the starting executable's identity.

Build, measurement and receipt attempts may differ when a failed-job retry
reuses a successful dependency from the same exact workflow run and source.
The report's `ci_run_attempt` is the measurement attempt;
`native_build_run_attempt` records the build attempt. Receipt-writing provenance
is separate. Reuse must preserve run, source/tree, policy, compiler and executable
identity and the original successful job/artifact identity; it must not relabel
older evidence as a new measurement.

Each attempt writes `performance.json`, a readable `performance.md`, and the
worker evidence. CI validates the fixed row set, pair order, raw timings,
cardinalities, digests, ratios and confidence calculations independently before
using the result for promotion. Source and measurement artifacts must remain
available for both successful and rejected candidates. The application startup,
browser, type, unit, recovery and platform gates remain separate requirements.
