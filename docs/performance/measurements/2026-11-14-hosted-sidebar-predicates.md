# Hosted sidebar predicate correction, 2026-11-14

**Decision: reject qualification. All eleven fixed latency rows regressed.**
The expanded sidebar differential, all selected Rust/component/adapter checks,
the actual normal hook and the controlled Linux browser/restart flow passed.
Those results establish the recorded behaviors; they do not establish complete
application parity or acceptable stock-relative performance. Main was not promoted.

[Workflow run 43619734102](https://github.com/glopyglerky/t3code-ls/actions/runs/43619734102)
measured `4b5719f34a8b76a23f3c77982940cea67234186c` against immutable upstream
`f751c8488f7ca1e659e44ef12b05861c1bcf9bfb`. The
[unchanged performance ZIP](2026-11-14-hosted-sidebar-predicates.raw.zip)
contains all eighteen workers, raw samples, configuration, logs and the original
JSON/Markdown verdict. The [functional archive](2026-11-14-hosted-sidebar-predicates.functional.raw.tar.gz)
retains the full decoded native job log, unchanged component/adapter/application
ZIPs, public workflow metadata, a scoped summary and a payload hash manifest.

## Correctness change and its scope

The [preceding f751 run](2026-11-14-hosted-f751.md) passed its existing functional
checks. A later [18-case production-boundary differential](../../porting/sidebar-predicate-parity-f751.md)
then exposed two uncovered SQL predicates. This candidate fixes those specific
behaviors: background wake suppression uses authored messages with the same run
ID across threads, and active-sidebar remote presence tests the first object-valued
`remoteChild` independently of remote-task openness. Canonical first JSON members
preserve SQLite duplicate-member behavior. The RemoteTasks openness predicate,
row/index format and ownership markers are unchanged.

The hosted shell proof now binds the exact expanded fixture and native source
and records that predicate check passing. It executes all six wake cases and all
twelve remote cases, not only the benchmark's ordinary fixture:

| Group                  | Cases | Source distinctions exercised                                                                                                                                                         |
| ---------------------- | ----: | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Background wake        |     6 | Foreign/local authored user message, agent message, different run, and both first-member duplicate-createdBy orders                                                                   |
| Active remote presence |    12 | Completed/cancelled/interrupted state; stopped/unstopped/absent stoppedThere; object/null/array/scalar; both duplicate-remoteChild orders; provider-native and local-child exclusions |

The component proof reports fourteen shell check groups rather than a separate
18-case counter. Its committed fixture SHA below binds those two complete arrays.
The earlier green checks did not cover these cases; this later success cannot
turn the prior result into broad parity evidence.

These are correctness changes, not a controlled speed experiment. Canonical-row
lookups can add work for wake and remote-child cases. This run measures the
corrected candidate against stock, but a different hosted execution cannot
establish the correction's causal latency cost or claim that cost is zero.
No history/search optimization or new project-event integration is included in
this measured commit.

## Every fixed latency row

Times are milliseconds. Warm absolute columns use the named percentile over
all 96 measured calls; cold uses the median of eight fresh-worker observations.
Paired ratios are geometric means of per-pair ratios with 95% whole-pair bootstrap
intervals, so they need not equal the quotient of the pooled absolute columns.
Positive latency change means slower than stock. Every interval is wholly above
the unchanged strict ratio of 1.0.

| Scenario                            | Metric  | Stock ms | Native ms | Paired native/stock | 95% paired interval | Latency change | Decision  |
| ----------------------------------- | ------- | -------- | --------- | ------------------- | ------------------- | -------------- | --------- |
| `service-open-and-first-operations` | elapsed | 117.216  | 559.586   | 4.824               | [4.753, 4.904]      | +382.4%        | Regressed |
| `sidebar-shell`                     | median  | 9.590    | 120.101   | 12.524              | [12.375, 12.689]    | +1,152.4%      | Regressed |
| `sidebar-shell.p95`                 | p95     | 11.038   | 123.805   | 11.272              | [10.917, 11.628]    | +1,027.2%      | Regressed |
| `history-window`                    | median  | 20.085   | 115.305   | 5.707               | [5.649, 5.759]      | +470.7%        | Regressed |
| `history-window.p95`                | p95     | 21.801   | 119.314   | 5.522               | [5.403, 5.655]      | +452.2%        | Regressed |
| `message-search`                    | median  | 4.921    | 37.824    | 7.681               | [7.593, 7.781]      | +668.1%        | Regressed |
| `message-search.p95`                | p95     | 5.531    | 39.502    | 7.202               | [7.023, 7.385]      | +620.2%        | Regressed |
| `accent-search`                     | median  | 3.655    | 32.614    | 8.942               | [8.853, 9.034]      | +794.2%        | Regressed |
| `accent-search.p95`                 | p95     | 4.265    | 34.753    | 8.284               | [8.119, 8.431]      | +728.4%        | Regressed |
| `visit-commit`                      | median  | 1.103    | 4.074     | 3.687               | [3.592, 3.782]      | +268.7%        | Regressed |
| `visit-commit.p95`                  | p95     | 1.623    | 5.190     | 3.198               | [2.945, 3.498]      | +219.8%        | Regressed |

Independent offline review reconstructed the eighteen raw workers and checked
the fixed configuration/order, runtime/cardinality witnesses, eleven pooled
statistics, paired ratios and each 10,000-resample interval. The original verdict
is retained: `status=regressed`, `qualifies=false`.

Only visit p95 raises the 10% precision diagnostic. Its relative interval width
is 17.3%, with interval
`[2.945, 3.498]`.
The lower bound remains above 1.0, so this is a failed row. The diagnostic is not
an allowed slowdown and does not justify discarding its tails or changing the gate.

## Full distributions and write tails

Cold has eight samples per backend and every warm workload has 96. These pooled
summaries are descriptive; all individual observations remain in the raw ZIP.

| Workload                            | Stock median | Stock p95 | Stock worst | Native median | Native p95 | Native worst |
| ----------------------------------- | ------------ | --------- | ----------- | ------------- | ---------- | ------------ |
| `service-open-and-first-operations` | 117.216      | 118.236   | 118.346     | 559.586       | 570.661    | 574.101      |
| `sidebar-shell`                     | 9.590        | 11.038    | 12.539      | 120.101       | 123.805    | 124.726      |
| `history-window`                    | 20.085       | 21.801    | 22.655      | 115.305       | 119.314    | 125.884      |
| `message-search`                    | 4.921        | 5.531     | 6.220       | 37.824        | 39.502     | 39.976       |
| `accent-search`                     | 3.655        | 4.265     | 4.638       | 32.614        | 34.753     | 35.324       |
| `visit-commit`                      | 1.103        | 1.623     | 1.865       | 4.074         | 5.190      | 6.092        |

Visit commits retain stock WAL/NORMAL and native Immediate durability. The
write row changes its timestamp, reads the complete row inside the same
transaction and waits for commit acknowledgement. Neither durability nor the
workload was weakened. Each worker's p95 and worst observation follows; no tail
was omitted, retried or attributed to a cause without evidence.

| Pair | Execution order   | Stock p95 | Stock worst | Native p95 | Native worst | p95 ratio |
| ---- | ----------------- | --------- | ----------- | ---------- | ------------ | --------- |
| 0    | native → upstream | 1.388     | 1.492       | 4.467      | 4.489        | 3.218     |
| 1    | upstream → native | 1.466     | 1.608       | 5.821      | 6.092        | 3.969     |
| 2    | upstream → native | 1.447     | 1.572       | 5.502      | 5.795        | 3.801     |
| 3    | upstream → native | 1.395     | 1.622       | 4.404      | 4.638        | 3.156     |
| 4    | native → upstream | 1.687     | 1.865       | 4.644      | 4.861        | 2.753     |
| 5    | upstream → native | 1.715     | 1.827       | 5.038      | 5.530        | 2.938     |
| 6    | native → upstream | 1.702     | 1.775       | 4.752      | 5.166        | 2.792     |
| 7    | native → upstream | 1.390     | 1.571       | 4.392      | 4.480        | 3.159     |

## Workload, correctness and reliability

The unchanged [protocol](../../../tools/performance-contract.json) uses eight
balanced process pairs, seed `1949524847`, three excluded warmup cycles and
twelve measured cycles per worker. The fixture has 256 normal threads, one
archived write thread, 3,000 search messages, a completed 1,500-item turn and
1,024-byte reasoning payloads. Setup, migration, file copying and index
preparation occur outside timed workers. Warm requests include ordinary native
IPC/serialization and stock worker messaging; result fingerprinting is outside
the timed API calls.

Cold means fresh-process service acquisition plus the first call to each of the
five APIs. It does not mean cold OS page cache or cold hardware. Module loading
and common result fingerprinting are outside that sum. Service acquisition alone
had observed medians of 32.214 ms stock and 182.594 ms native; these are descriptive
subcomponents, not extra qualifying rows.

Initial deep equality and array order, nonempty cardinalities, same-transaction
readback, the exact deliberate rollback sentinel, restoration, committed reopen
and every warmup/measured response digest passed. The unchanged cardinality
witnesses were:

| Witness            | Retained value |
| ------------------ | -------------- |
| active_threads     | 224            |
| history_items      | 3              |
| history_runs       | 1              |
| history_nodes      | 1              |
| history_json_bytes | 13,976         |
| message_matches    | 20             |
| accent_matches     | 20             |

Rollback and reopen exercise application failure semantics. They do not prove
power-loss durability, real-provider behavior, whole-app speed, every corruption
case or another platform. Equality on this fixture cannot compensate for an
incorrect application path outside it.

## Hosted functional outcome

| Job                                                 | Result  | Job ID                                                                                             |
| --------------------------------------------------- | ------- | -------------------------------------------------------------------------------------------------- |
| Select fork checks and porting work                 | success | [132522451224](https://github.com/glopyglerky/t3code-ls/actions/runs/43619734102/job/132522451224) |
| Affected Rust packages and source guards / qualify  | success | [132522701988](https://github.com/glopyglerky/t3code-ls/actions/runs/43619734102/job/132522701988) |
| Paired stock and native performance and reliability | failure | [132525929251](https://github.com/glopyglerky/t3code-ls/actions/runs/43619734102/job/132525929251) |
| Rust integration through the application            | success | [132525929256](https://github.com/glopyglerky/t3code-ls/actions/runs/43619734102/job/132525929256) |
| Affected production adapters                        | success | [132525929273](https://github.com/glopyglerky/t3code-ls/actions/runs/43619734102/job/132525929273) |
| Record verified qualification                       | failure | [132526829165](https://github.com/glopyglerky/t3code-ls/actions/runs/43619734102/job/132526829165) |

The full native log contains **419 passed, zero failed and zero ignored tests**
across 37 result groups, 18 nonempty. Mapped acceptance records two builds,
29 production typechecks and 49 component checks. Adapter acceptance records
15 checks. The expanded shell gate and its source/helper hashes are preserved
inside the unchanged component artifact.

The application job ran the actual committed `git hook run pre-commit` once
against the candidate's staged first-parent delta. Its proof records a 27.750-second
success and restoration of the exact detached candidate/tree with a clean index
and worktree. The normal formatter, linter and selected Knip work were not replaced
by a hand-maintained subset.

The original Linux browser flow records seven completed steps and three matching
turn-identity snapshots: initial completion, client reload and same-state server
restart. The snapshot sequences are 79, 80 and 82, with the same project/thread/
run/attempt/provider-turn/assistant-message identities and completion timestamps.
Read-only native inspection checks canonical rows and event heads without an
application SQLite database; the closed-store inspection records unchanged bytes.
The original screenshots and scripted replay-provider fixture remain in the ZIP.

The browser proof also retains **three resource errors**: two connection refusals
and one HTTP 502 response. Its overall error field is null and the seven state
checks passed. The error array lacks phase timestamps, so this report does not
attribute those messages to startup/restart or claim an error-free browser session.

This qualifies neither real providers nor desktop/mobile distribution, other
operating systems or native Lust. It is scoped functional evidence for the exact
Linux candidate. Final qualification failed because the required performance job
failed, and main remained at the earlier accepted commit.

## Runtime, host and memory limits

The immutable full stock archive and per-workspace source/dependency resolution
passed verification. Frozen pnpm 12.14.0, Node 26.12.1, installed application lock,
package-manager metadata and all 440 external dependency descriptors were checked.
Runtime provenance is `verified` with no issues; external packages remained in
the frozen pnpm store and workspace aliases resolved to their own source trees.
The only extra lock importer is the exact existing `tools/oracles` harness.

The performance report records `compiler_route=bluefe-rustc-via-lustr` with
`compiler_provenance=verified-ci-manifest`. Its top-level `compiler_identity`
field remains null in the original JSON; it is not filled from the expected pin.
The expected toolchain is the produced BlueFe compatibility package
`preview-2026.11.09-2`, compiler revision
`1b74388e196ee8813f38ba9eeb6d4ab43b6b1071`, with coordinator revision
`07470b20464bf097c3c88983124aff56c5e44f9a`. The bound native manifest, helper hash,
selected route and native job log provide the retained build evidence. Native Lust
is not present. The component/debug helper differs from the optimized release
helper used for timing; both identities below keep those scopes distinct.

The host reported Intel Xeon 6975P-C, four CPUs, Linux 7.0.0-1014-aws and
16,464,297,984 memory bytes. Load averages changed from `[2.33, 0.73, 0.26]`
to `[1.64, 0.90, 0.37]`. These observations do not identify the cause of a latency
change. Scheduling, CPU frequency, shared filesystem/page cache and finite tail
sampling remain limits on comparisons between hosted runs.

The fixture volume reported type `0xef53`, no assigned observer name and
4,096-byte blocks/fragments. These outside-timing observations describe available
space, not a physical-device or power-loss guarantee:

| Observation   | Available blocks | Bytes at reported block size |
| ------------- | ---------------- | ---------------------------- |
| before_setup  | 21,915,556       | 89,766,117,376               |
| after_workers | 21,138,423       | 86,582,980,608               |
| after_cleanup | 21,914,575       | 89,762,099,200               |

All sixteen measured workers returned advisory process-tree RSS observations.

| Backend  | Process-tree peak KiB | Node-only peak KiB | GC count per worker | GC duration ms per worker |
| -------- | --------------------- | ------------------ | ------------------- | ------------------------- |
| upstream | 330,844–332,116       | 331,056–336,208    | 8–8                 | 8.609–11.513              |
| native   | 441,676–443,316       | 314,444–336,208    | 12–12               | 20.577–24.034             |

The 25 ms sampler can miss short peaks and count shared pages more than once.
Node-only high-water values exclude helper children; they need not equal the
sampled process-tree peak. Neither measure is heap size, memory-safety proof or
whole-application memory acceptance. No forced GC or sample removal was used.

## Exact identities and retained evidence

| Evidence                            | Identity                                                           |
| ----------------------------------- | ------------------------------------------------------------------ |
| Source commit                       | `4b5719f34a8b76a23f3c77982940cea67234186c`                         |
| Source tree                         | `3cf58d876b6e189f6efe38eaba0b438d2d89ec14`                         |
| Sole parent                         | `192007cc03f418763b1f89eac26211964fc3c1c1`                         |
| Pinned upstream                     | `f751c8488f7ca1e659e44ef12b05861c1bcf9bfb`                         |
| Upstream tree                       | `a8157c768c81bdb0564612e2adca54dde1411a22`                         |
| Attempt                             | `2026-11-14T17-24-20.158Z-b6256e37`                                |
| Workflow / measurement attempt      | `43619734102 / 1`                                                  |
| Native build attempt                | `1`                                                                |
| Release helper SHA-256              | `695bd841d4d7855d59b927f8d31461a38992588fc0d9f5b5e00ec33fbeba3ace` |
| Native manifest SHA-256             | `e5e5e115e94e310b6f563bb08cbb9def38298bb7dcaadea1496072a53969b2ce` |
| Toolchain lock SHA-256              | `2cddf6970e38ab122d6d62e4543cfb3d1fd23876d4a813a4b4421babc033d1bf` |
| Policy digest                       | `d8c2b716d3c6b3d6d47d64d2f1ebd5d83c82ba09821f9d7a1f8f172d57ec6149` |
| Stock source archive SHA-256        | `671f49449dc5845aebc52e408ca9f719b6fdfa2d587646f73a132b8715e8615d` |
| Runtime external-resolution SHA-256 | `215febeb42c657ec13ce66c7da4ce621ee7f87a4eefcf0a4a521955a085e897f` |
| Performance JSON SHA-256            | `8f6623885ded594c1ac7ef466bc6a179a9d123e7b943e5de4608c81c10ef01cb` |
| Unchanged performance ZIP SHA-256   | `e9c7e6955dcfc3c63f122914f4dfcad1cde595663a31b82cf66a728e9177a2b2` |
| Functional archive SHA-256          | `605d8a5110624aa357c28254074a7c69a710784102497616b4dd1c2eb4778d5e` |
| Decoded native job log SHA-256      | `793389b77c0b0f617fdacb79b159c91373dab5195262c0b9a466aa28490c41ef` |
| Shell gate proof SHA-256            | `a93dd82dc794df7525f56c1602d58d542ad17f76ac833984438c30a7551f595d` |
| Expanded shell fixture SHA-256      | `1dde2bc4d062adb80a7a2b719b3b296992deee6a9f130435fd1b7700b731a3eb` |
| Native shell implementation SHA-256 | `6c140918578439dae756d459628d234fa0f47f9a5b5235361ba052df256fc336` |
| Component/debug helper SHA-256      | `4deaa29ba3a7bb84129ca7db85fc171a0bfc26e7c946c555d2a655a2d5b1a6fd` |
| Normal-hook proof SHA-256           | `3887ae2729f6a05cd9edb3cb33e13f8c832a88409f3cebb736784f2b9b6fdd44` |
| Browser proof SHA-256               | `4fc70d600f6eb2d5b3601488f4ffc852f4627361b057f1ce0603335f662a05ee` |

| Artifact           | GitHub artifact ID                                                                                     | Unchanged ZIP SHA-256                                            |
| ------------------ | ------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------- |
| Paired performance | [14124635038](https://github.com/glopyglerky/t3code-ls/actions/runs/43619734102/artifacts/14124635038) | e9c7e6955dcfc3c63f122914f4dfcad1cde595663a31b82cf66a728e9177a2b2 |
| Components         | [14123233358](https://github.com/glopyglerky/t3code-ls/actions/runs/43619734102/artifacts/14123233358) | 3d8f6f2795ed4f34953acde8ea8a2c8260aa42b8f06dffa8fe5fb618f71a74a3 |
| Adapters           | [14122984445](https://github.com/glopyglerky/t3code-ls/actions/runs/43619734102/artifacts/14122984445) | 5f838f2bc5304b6918af141b6a28dff6e59259c0ca1992959d03d98d73a566fd |
| Application        | [14123378794](https://github.com/glopyglerky/t3code-ls/actions/runs/43619734102/artifacts/14123378794) | 651894686aac0d6c37c78b89e80a41cfd6f92173c63982500cf3c0468d672988 |

The functional archive has six payloads plus its manifest: the three unchanged
artifact ZIPs, full decoded UTF-8 native log, selected public workflow metadata
and a scoped summary. The summary hashes the six relevant inner proof files and
records their exact counts. Ephemeral download credentials/URLs are omitted.
The performance ZIP is byte-for-byte the original artifact, including all 74
entries. These retained bytes preserve the failed verdict after hosted artifact
expiry.

## Decision

Keep the two source-proven sidebar corrections and their expanded fixtures in
the unqualified integration branch. Keep every failed latency row, both raw
archives and the existing 1.0 upper-bound contract. Use separately reviewed,
measured changes to address the remaining persistence gap. This outcome is not a
reason to retire retained source, declare broad parity or promote main.
