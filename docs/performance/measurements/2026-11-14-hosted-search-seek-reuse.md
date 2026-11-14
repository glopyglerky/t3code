# Hosted search seek reuse experiment, 2026-11-14

**Decision: reject qualification. All eleven fixed latency rows regressed.**
The five new Rust search cases, existing component and adapter checks, normal
hook and controlled Linux application recovery flow passed. Those functional
results do not establish stock performance or complete application parity.
This isolated experiment remains outside main and the later upstream candidate.

[Workflow run 43618640861](https://github.com/glopyglerky/t3code-ls/actions/runs/43618640861)
measured `7cdb4d3176d5199cd236d78617d74bc01d3c9b72` against immutable upstream
`5fe2f7e4567d9e530ded0dabc9d706db27d77b84`. The
[unchanged performance ZIP](2026-11-14-hosted-search-seek-reuse.raw.zip)
contains both validation workers, all sixteen measured workers, every raw
sample and the original JSON/Markdown verdict. The separate
[functional archive](2026-11-14-hosted-search-seek-reuse.functional.raw.tar.gz)
retains the full native job log, workflow metadata and unchanged component,
adapter and application artifact ZIPs with an indexed hash manifest.

## Experiment and interpretation

The [source proposal and review](../experiments/2026-11-14-search-seek-reuse.md)
describe one bounded query-local optimization. An immutable read snapshot may
reuse a completed seek's scalar result only within its proven maximum interval.
The successful-hit rule is `found <= newMaximum <= searchedMaximum`; an exhausted
seek is reusable only when `newMaximum <= searchedMaximum`. Misses retain the
same legacy, direct and bucket checks in the same order. Writers keep their
existing observations and savepoint behavior. The scalar state has fixed
256-entry and 8,192-byte bounds; excess queries use the original algorithm.

This candidate has sole parent `ee178c237d5fad5842f950871003dc898ab26990` and
the same frozen 5fe oracle as that earlier hosted report. Its native production
change is confined to search; the separate history experiment and f751 sidebar
corrections are absent. No per-source cursor, document or position reuse was added.

The comparison below establishes regressions against stock within this run.
It does not establish a causal improvement over the earlier native helper.
Different hosted executions, CPU/kernel observations, scheduling and cache state
prevent treating changes between reports as a controlled before/after result.
No sample was removed, retried or recalibrated, and no strict threshold changed.

## Every fixed row

Times are milliseconds. Warm absolute columns use the named percentile over
all 96 measured calls; cold columns use the median of eight fresh-worker
observations. Paired ratios are geometric means of per-pair ratios with 95%
whole-pair bootstrap intervals. They need not equal the quotient of pooled
absolute columns. Positive latency change means slower than stock. All eleven
intervals are wholly above the strict comparison ratio of 1.0.

| Scenario                            | Metric  | Stock ms | Native ms | Paired native/stock | 95% paired interval | Latency change | Decision  |
| ----------------------------------- | ------- | -------: | --------: | ------------------: | ------------------- | -------------: | --------- |
| `service-open-and-first-operations` | elapsed |  117.257 |   560.403 |               4.778 | [4.710, 4.842]      |        +377.8% | Regressed |
| `sidebar-shell`                     | median  |    9.657 |   120.026 |              12.514 | [12.405, 12.620]    |      +1,151.4% | Regressed |
| `sidebar-shell.p95`                 | p95     |   11.062 |   123.861 |              11.502 | [11.134, 11.889]    |      +1,050.2% | Regressed |
| `history-window`                    | median  |   20.405 |   115.920 |               5.735 | [5.636, 5.841]      |        +473.5% | Regressed |
| `history-window.p95`                | p95     |   21.704 |   121.081 |               5.589 | [5.520, 5.659]      |        +458.9% | Regressed |
| `message-search`                    | median  |    4.982 |    34.192 |               6.865 | [6.719, 7.015]      |        +586.5% | Regressed |
| `message-search.p95`                | p95     |    5.860 |    36.044 |               6.417 | [6.158, 6.680]      |        +541.7% | Regressed |
| `accent-search`                     | median  |    3.680 |    30.329 |               8.253 | [8.080, 8.405]      |        +725.3% | Regressed |
| `accent-search.p95`                 | p95     |    4.155 |    32.682 |               7.910 | [7.728, 8.101]      |        +691.0% | Regressed |
| `visit-commit`                      | median  |    1.117 |     4.042 |               3.590 | [3.470, 3.697]      |        +259.0% | Regressed |
| `visit-commit.p95`                  | p95     |    1.565 |     5.216 |               3.451 | [3.095, 3.818]      |        +245.1% | Regressed |

Both targeted search workloads remain substantially slower than stock.
Message-search median is 34.192 ms native versus 4.982 ms stock, with a paired
ratio of 6.865. Accent-search median is 30.329 ms versus 3.680 ms, with a paired
ratio of 8.253. Their p95 rows also fail; no search speed parity is established.

Independent Python validation reconstructed all eighteen raw workers, the
fixed order, all eleven pooled summaries and each 10,000-resample interval.
Each separately retained worker agrees with the original report. Result and
array-order equality, nonempty cardinality witnesses, same-transaction
readback, exact deliberate rollback, restoration and committed reopen passed.
Each warmup and measured output matched its validated result, including the
changing committed visit timestamp. Frozen production runtime provenance passed.

Only visit p95 raises the 10% precision diagnostic: its relative interval
width is 20.9%, with interval `[3.095, 3.818]`. The lower bound still exceeds
1.0, so it fails. A noise diagnostic is neither an allowed regression nor a
reason to omit a row. No workload passed, and no aggregate can hide a slow row.

## Full distributions and write tail

Cold has eight samples per backend; every warm workload has 96. These are
descriptive pooled distributions. Every individual sample and paired statistic
remains in the unchanged raw ZIP.

| Workload                            | Stock median | Stock p95 | Stock worst | Native median | Native p95 | Native worst |
| ----------------------------------- | -----------: | --------: | ----------: | ------------: | ---------: | -----------: |
| `service-open-and-first-operations` |      117.257 |   120.252 |     120.805 |       560.403 |    570.197 |      571.871 |
| `sidebar-shell`                     |        9.657 |    11.062 |      12.123 |       120.026 |    123.861 |      126.644 |
| `history-window`                    |       20.405 |    21.704 |      22.522 |       115.920 |    121.081 |      125.049 |
| `message-search`                    |        4.982 |     5.860 |       6.342 |        34.192 |     36.044 |       36.674 |
| `accent-search`                     |        3.680 |     4.155 |       4.452 |        30.329 |     32.682 |       34.004 |
| `visit-commit`                      |        1.117 |     1.565 |       1.842 |         4.042 |      5.216 |        5.752 |

The write path was unchanged by this search experiment. Native visit commits
had median 4.042 ms, p95 5.216 ms and worst 5.752 ms; stock had 1.117 ms,
1.565 ms and 1.842 ms respectively. The following table retains each worker's
tail. These observations neither identify a storage cause nor explain away
the larger write tails preserved in previous hosted attempts.

| Pair | Execution order   | Stock p95 | Stock worst | Native p95 | Native worst |
| ---: | ----------------- | --------: | ----------: | ---------: | -----------: |
|    0 | native → upstream |     1.149 |       1.173 |      4.520 |        4.778 |
|    1 | upstream → native |     1.490 |       1.575 |      5.454 |        5.752 |
|    2 | upstream → native |     1.301 |       1.420 |      5.534 |        5.551 |
|    3 | upstream → native |     1.482 |       1.765 |      4.726 |        5.233 |
|    4 | native → upstream |     1.283 |       1.348 |      5.050 |        5.441 |
|    5 | upstream → native |     1.688 |       1.842 |      4.543 |        4.920 |
|    6 | native → upstream |     1.607 |       1.612 |      4.561 |        4.834 |
|    7 | native → upstream |     1.280 |       1.368 |      4.383 |        4.592 |

## Fixed workload and reliability scope

The protocol retained eight balanced process pairs, seed `1949524847`, three
excluded warmups and twelve measured cycles per worker. The unchanged fixture
contains 256 normal threads, one archived write thread, 3,000 messages, 1,500
history items and 1,024-byte reasoning payloads. Setup, migration and index
preparation are outside timed workers. Cold means fresh-process service
acquisition and the first five operations; module loading and result
fingerprinting are outside that sum. No OS page-cache flush or cold-device
claim is made. Warm APIs include normal IPC, serialization and worker messaging.

The equal-result proof also required these nonempty, bounded witnesses:

| Witness            | Retained value |
| ------------------ | -------------: |
| active_threads     |            224 |
| history_items      |              3 |
| history_runs       |              1 |
| history_nodes      |              1 |
| history_json_bytes |         13,976 |
| message_matches    |             20 |
| accent_matches     |             20 |

Stock SQLite retained WAL/NORMAL with its production worker configuration.
Native omdb retained Immediate durability. Every timed visit changed its
timestamp, read back the complete row inside the transaction and awaited
commit acknowledgement. The stronger native setting was not weakened to obtain
a pass. Rollback and committed reopen are scoped reliability checks, not a
power-loss test. Equal fixture outputs do not cover all application behaviors.

## Hosted functional evidence

| Job                                    | Result | Recorded scope                                                                                             |
| -------------------------------------- | ------ | ---------------------------------------------------------------------------------------------------------- |
| Plan/source selection                  | Passed | Exact candidate and fixed policy selection                                                                 |
| Native packages and source guards      | Passed | 207 storage library tests, including all five new seek cases; other selected Rust tests and source guards  |
| Mapped component acceptance            | Passed | 2 builds, 29 production typechecks and 49 checks recorded in acceptance.json                               |
| Production adapters                    | Passed | 15 recorded typecheck/adapter checks                                                                       |
| Normal hook and controlled application | Passed | Actual pre-commit hook, client/server build and original Linux browser approval/reload/server-restart flow |
| Paired performance                     | Failed | All 11 required rows regressed                                                                             |
| Final qualification                    | Failed | The required performance job failed                                                                        |

| New Rust case                                                               | Behavior exercised                                                                                        |
| --------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| mixed_sources_keep_phrase_positions_suppression_order_and_signed_rowids     | Mixed legacy/direct/bucket sources retain phrase positions, suppression, ordering and signed row IDs      |
| non_winning_source_corruption_never_installs_a_partial_seek_proof           | A corrupt non-winning source cannot install a partial proof                                               |
| scalar_state_bounds_fall_back_without_rejecting_large_queries               | Both scalar limits fall back without rejecting larger queries                                             |
| seek_bounds_and_read_snapshot_survive_new_commits_without_cross_query_state | Maximum intervals and an existing read snapshot survive new commits without state leaking between queries |
| writer_search_keeps_range_observation_cost_and_savepoint_visibility         | Writer range-observation cost and savepoint visibility remain intact                                      |

The native job log explicitly records each new case passing. The application
artifact binds source/tree and the normal `git hook run pre-commit` result,
then proves restoration to the clean measured candidate. The browser proof
records normal setup, local-project creation, controlled replay-provider approval,
completed turn, client reload and same-state server restart. Read-only native
inspection confirms canonical rows/event heads without application SQLite files.
The archive retains the original screenshots and scripted fixture evidence.

The browser proof also retains four console resource errors: two connection
refusals and two HTTP 502 responses. Its overall error field is null and all
seven state checks passed, but the error array has no phase timestamps. This
report does not attribute those messages to startup or restart, and does not
claim an error-free browser session. They remain available for follow-up in
the original artifact.

These checks cover the selected Linux scenario and fixture boundaries. They do
not qualify real providers, desktop/mobile distribution, other operating systems
or native Lust. Separately expanded sidebar fixtures found two behaviors absent
from the older test coverage, later corrected on the f751 integration branch;
those corrections are not in this 5fe search-only candidate. Passing this run's
functional checks must not be described as complete application parity.

## Runtime, host and memory limits

Immutable stock export, per-workspace resolution and the actual production
dependency graph passed verification. Frozen pnpm 12.14.0, Node 26.12.1, installed
lock and package-manager metadata, and all 440 external dependency descriptors
were checked without issues. The optional root Cursor platform package is
included. External targets remained within the frozen pnpm store; workspace
aliases resolved to their own stock or native source trees. Critical imported
dependencies matched their pinned production versions.

The host reported Intel Xeon 6975P-C, four CPUs, Linux 7.0.0-1014-aws and
16,464,293,888 memory bytes. Load averages changed from `[1.70, 0.62, 0.23]`
to `[1.28, 0.79, 0.33]`. This is host context, not a causal latency diagnosis.
Scheduling, frequency, filesystem cache and finite tail sampling remain limits.

The fixture filesystem reported type `0xef53` with no assigned observer name,
and 4,096-byte block/fragment sizes. The observations below are outside the
timed intervals and describe the volume, not a physical device or power-loss
guarantee.

| Observation   | Available blocks | Bytes at reported block size |
| ------------- | ---------------: | ---------------------------: |
| before_setup  |       21,858,284 |               89,531,531,264 |
| after_workers |       21,081,251 |               86,348,804,096 |
| after_cleanup |       21,857,303 |               89,527,513,088 |

All sixteen measured workers returned advisory process-tree RSS samples.
The ranges below are retained independently of the latency verdict.

| Backend  | Process-tree peak KiB | Node-only peak KiB | GC count per worker | GC duration ms per worker |
| -------- | --------------------: | -----------------: | ------------------: | ------------------------: |
| upstream |       331,180–332,836 |    331,552–338,488 |                 8–9 |              7.761–13.449 |
| native   |       439,252–445,352 |    311,332–338,488 |               12–12 |             21.739–26.248 |

The 25 ms sampler can miss short peaks and double-count shared pages.
Node-only high-water values exclude helper children and need not equal the
sampled process-tree peak. Neither is heap size, whole-application memory
qualification or a memory-safety guarantee. No forced collection or outlier
removal was used; every worker's RSS/GC observations remain in the raw archive.

## Exact identities

| Evidence                                | Identity                                                           |
| --------------------------------------- | ------------------------------------------------------------------ |
| Attempt                                 | `2026-11-14T17-20-00.581Z-12374346`                                |
| Source commit                           | `7cdb4d3176d5199cd236d78617d74bc01d3c9b72`                         |
| Source tree                             | `2508800608c7fb069a13054746aec947f3256db3`                         |
| Sole parent                             | `ee178c237d5fad5842f950871003dc898ab26990`                         |
| Pinned upstream                         | `5fe2f7e4567d9e530ded0dabc9d706db27d77b84`                         |
| Upstream tree                           | `5d1b0d638e32a15f4ea85924c1e751b7da12a92a`                         |
| Workflow run / measurement attempt      | `43618640861 / 1`                                                  |
| Native build attempt                    | `1`                                                                |
| Release helper SHA-256                  | `a3cfcd67d7b8af31950873144447f8acb5871305ea18787c00dfe27185abd2bc` |
| Native manifest SHA-256                 | `0cc722981960bcaf799a16800441891179a1c6e94972787f9de0cbe913fb91e3` |
| Toolchain lock SHA-256                  | `2cddf6970e38ab122d6d62e4543cfb3d1fd23876d4a813a4b4421babc033d1bf` |
| Policy digest                           | `d8c2b716d3c6b3d6d47d64d2f1ebd5d83c82ba09821f9d7a1f8f172d57ec6149` |
| Original report SHA-256                 | `b4d825d6c35f13e384d5561b771e7260dfdac3567fe3ea2ae396ad871af50142` |
| Original performance ZIP SHA-256        | `455fa4786026fd641ce393ec8a398c7a4be8b1ca66c913559fe9ae35d3fd89f8` |
| Performance ZIP bytes / regular entries | `219187 / 74`                                                      |
| Performance ZIP uncompressed bytes      | `3981473`                                                          |
| Functional archive SHA-256              | `0d07741434105b0175a5d85d1ccce8c9cd94dcc4b3a7881d2b8bdfb3033eccac` |
| Native job log SHA-256                  | `3a348641538fda4acdeda8c229951c3eeb8fa7f2b801dc9d87f0e7530a84039e` |
| Component artifact SHA-256              | `2224ad11bc01891be520c5ac60cba90f5469b1d702046a68699b517d9331319c` |
| Application artifact SHA-256            | `2ce82a278cb74c83204305aea4aff82beb0da5803a992deb4075b0546f5717e5` |
| Adapter artifact SHA-256                | `8d12e7905038bb44078217e313bb517c28dc399fd75e8c8e219d1f01b68eded9` |
| Immutable stock export SHA-256          | `081dfe6c4c30b698abff23808a46b6bac14bd79dd530b6f1b67d7c43b848ecd5` |
| Stock SQLite wrapper SHA-256            | `c79a07827b4a546cc5bfb2c47c55697d33ef2d032c5e666b116c832bace3ee4d` |
| Filesystem observer SHA-256             | `c9557b4c5d9d5aa2a8886ce7c4a8b16d47acb2c165e3238fa1c2ea07cb954fb9` |
| Installed lock SHA-256                  | `01d0fc49af1ed2656e20b7ec900232139f860043fe806cbb06d4ee33941254d1` |
| Modules metadata SHA-256                | `89aad33db7d3b608d2fd3262eb7435c4f218d72fb0e6385ecf7106dc0d3fd00a` |
| Node executable SHA-256                 | `08f12d42fb304468e8ee86247fe686aae5b225836ea65e9d6566c11ae6ba2e8e` |
| External resolution SHA-256             | `215febeb42c657ec13ce66c7da4ce621ee7f87a4eefcf0a4a521955a085e897f` |
| SQLite fixture SHA-256                  | `f183bac7d05db84ad61ff1c010eefd9dc64e5317d1e987120d5a4c64f9835035` |
| Canonical export SHA-256                | `85e9bed86bc6eea1e0754d1906d1351ea49018e76218b82ba15fc52422f1cca0` |
| Native template SHA-256                 | `db20b967ca576fd1bbea3048336cdf0a198be75abe778bbe20fe7ac2fe6014a7` |
| Schema SHA-256                          | `395b45dba23c8c59851ff0af055023d7536b0e76059fc613f1b96b6e5244a502` |

The source was clean before and after measurement. The report records
`compiler_provenance: verified-ci-manifest`, manifest-bound
`bluefe-rustc-via-lustr` and release profile with exact source/tree, same-run
and executable/lock hashes. Observed `compiler_identity` is null because the
manifest did not report it; expected pinned identity stays separate. Manifest
verification does not independently observe an unreported compiler identity.

The exact recorded measurement command is:

```text
/opt/hostedtoolcache/node/26.12.1/x64/bin/node /home/runner/work/t3code-ls/t3code-ls/tools/performance-persistence.mjs --database /home/runner/work/t3code-ls/t3code-ls/bin/t3-code-ls-db --manifest /home/runner/work/t3code-ls/t3code-ls/bin/manifest.json --plan /home/runner/work/_temp/performance-plan/ci-plan.json --output /home/runner/work/_temp/performance-proof-1 --mode qualify
```

All runner, worker, statistics, contract, observer and adapter hashes remain
in `performance.json`. Fixture hashes bind the controlled data; no database
file or helper executable is added to either evidence archive.

## Next decision

Retain this isolated result and its source review without advancing main or
claiming an optimization gain. The run shows that the bounded seek experiment
preserves the tested semantics but does not reach stock performance. Deciding
whether to carry the implementation into current upstream needs separately
controlled evidence; a lower number from another hosted run is insufficient.

The approved finer diagnostic on the fixed a2fb source separates named decode,
copy, hashing/hex, sort and selection spans while leaving algorithms and the
qualification workload unchanged. Its nested timings will remain diagnostic,
not a substitute for the unchanged eight-pair stock comparison. Existing main
remains unchanged and the strict performance gate continues to block promotion.
