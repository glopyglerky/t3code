# Readiness-cache measurement, 2026-11-14

> Later compiler-provenance correction: this diagnostic supplied no native build
> manifest. Its compiler route and identity fields describe expected pins, not
> verified build evidence. See the [erratum](2026-11-14-compiler-provenance-erratum.md).
> Original raw evidence, hashes and timing results are unchanged.

**Decision: retain the correctness-preserving readiness fix, continue optimization,
and reject performance qualification.** All eleven fixed latency rows still have
their entire paired 95% native/stock interval above 1.0. Correctness, ordering,
cardinality, transaction readback, deliberate rollback and committed reopen pass.
The source tree stayed clean for the complete eight-pair attempt.

The [methodology](../persistence-api.md) and
[first baseline](2026-11-14-baseline.md) define the unchanged workloads. The
[complete raw archive](2026-11-14-readiness.raw.tar.gz) preserves the original
report, both validation workers, all sixteen measurement workers and their
configs/output/timings, exact benchmark sources, and the committed readiness
implementation and lock files. It contains no database files or executables.

## Paired comparisons with stock within this attempt

Times are milliseconds. Warm columns use the named percentile over all 96
observations; cold columns use the median of eight fresh-worker elapsed values.
Ratios and intervals use whole process pairs. Positive latency change means
slower than the pinned stock baseline. Every row independently fails.

| Scenario                            | Metric  | Stock ms | Native ms | Paired native/stock | 95% paired interval | Latency change | Decision  |
| ----------------------------------- | ------- | -------: | --------: | ------------------: | ------------------- | -------------: | --------- |
| `service-open-and-first-operations` | elapsed |  225.702 | 1,664.905 |               7.181 | [6.782, 7.615]      |        +618.1% | Regressed |
| `sidebar-shell`                     | median  |   11.960 |   496.026 |              41.026 | [39.729, 42.315]    |      +4,002.6% | Regressed |
| `sidebar-shell.p95`                 | p95     |   14.440 |   576.220 |              40.731 | [37.087, 44.568]    |      +3,973.1% | Regressed |
| `history-window`                    | median  |   23.518 |   306.730 |              13.102 | [12.603, 13.586]    |      +1,210.2% | Regressed |
| `history-window.p95`                | p95     |   27.868 |   384.382 |              13.247 | [12.189, 14.263]    |      +1,224.7% | Regressed |
| `message-search`                    | median  |    6.090 |   317.866 |              52.015 | [49.777, 54.348]    |      +5,101.5% | Regressed |
| `message-search.p95`                | p95     |    7.836 |   415.917 |              51.916 | [45.795, 59.283]    |      +5,091.6% | Regressed |
| `accent-search`                     | median  |    4.732 |   192.199 |              40.419 | [38.818, 42.204]    |      +3,941.9% | Regressed |
| `accent-search.p95`                 | p95     |    6.555 |   273.646 |              38.177 | [31.646, 44.973]    |      +3,717.7% | Regressed |
| `visit-commit`                      | median  |    1.344 |     6.039 |               4.420 | [4.198, 4.717]      |        +342.0% | Regressed |
| `visit-commit.p95`                  | p95     |    2.214 |     9.706 |               4.214 | [3.383, 5.270]      |        +321.4% | Regressed |

No threshold, row, sample, warmup or fixture was changed after seeing the first
baseline. Seed `1949524847`, eight balanced pairs, three excluded warmups and
twelve measured cycles per worker remained fixed. SQLite retained WAL/NORMAL;
native retained Immediate durability. The input and result digests agree with
the original timed baseline, including the folded three-item history result,
224 active threads and 20 hits in each search.

## Observed native changes across the two attempts

This table is descriptive. It compares pooled native statistics from separate
attempts; it is **not** a paired estimate of the optimization's treatment effect.
The formal acceptance evidence is the within-attempt stock comparison above.
Host scheduling, cache and frequency differences can affect these figures.

| Scenario                            | Prior native ms | Readiness native ms | Observed latency change |
| ----------------------------------- | --------------: | ------------------: | ----------------------: |
| `service-open-and-first-operations` |       6,161.366 |           1,664.905 |                  -73.0% |
| `sidebar-shell`                     |       3,084.128 |             496.026 |                  -83.9% |
| `sidebar-shell.p95`                 |       3,650.978 |             576.220 |                  -84.2% |
| `history-window`                    |       2,042.893 |             306.730 |                  -85.0% |
| `history-window.p95`                |       2,527.209 |             384.382 |                  -84.8% |
| `message-search`                    |         410.056 |             317.866 |                  -22.5% |
| `message-search.p95`                |         536.819 |             415.917 |                  -22.5% |
| `accent-search`                     |         290.054 |             192.199 |                  -33.7% |
| `accent-search.p95`                 |         379.884 |             273.646 |                  -28.0% |
| `visit-commit`                      |           6.657 |               6.039 |                   -9.3% |
| `visit-commit.p95`                  |          11.285 |               9.706 |                  -14.0% |

The large sidebar/history reductions are consistent with removal of repeated
readiness-marker work identified in source inspection. The patch only caches a
successful preparation proof inside one immutable read transaction and batches
its first eighteen owner-marker reads. Writers and savepoint validation stay
ordered and uncached. Three focused snapshot/corruption/savepoint tests passed
before the optimized helper was rebuilt.

The apparent visit median reduction is about 9.3%, but writer behavior was
unchanged. This is an observed between-run difference, not evidence of a write
optimization by this patch. The same limitation applies to every descriptive
before/after percentage, including the observed p95 changes. The write path
remains clearly slower than stock in this attempt.

## Identity and limits

| Identity                    | Value                                                              |
| --------------------------- | ------------------------------------------------------------------ |
| Attempt                     | `2026-11-14T12-55-23.587Z-844e3a67`                                |
| Source commit               | `bc0d46941ccac37a97062fb135544c62381cf453`                         |
| Source tree                 | `ae264726ef695ccb96006236c4745f8e3c915360`                         |
| Dirty before / after        | `false` / `false`                                                  |
| Pinned upstream             | `913d7fc45ec5814fff25c819cfe4de193ea42d63`                         |
| Pinned upstream tree        | `95faedbd2644d046dac476ef9dcab2eb38a7eb97`                         |
| Helper SHA256               | `2af9df238dddbe380396eff14098a1321703d212db3c3c0de2fa1d7cfb31e782` |
| Toolchain lock SHA256       | `2cddf6970e38ab122d6d62e4543cfb3d1fd23876d4a813a4b4421babc033d1bf` |
| Original report SHA256      | `23134c5505b452de830d5a0c6c0281bbb1adf8fc225d8ff7a22098254388b588` |
| Raw evidence archive SHA256 | `db31a1d6f432d1de591a17c05e13f9041e73f197c6b372be24b41c0622e71674` |

The optimized helper was built through the pinned Lustr/BlueFe release route.
The local runner received no CI artifact manifest or plan, so it correctly
records `unverified-diagnostic` and `qualifies: false`. A clean worktree alone
does not convert this diagnostic into same-run CI release qualification.

The first two timing attempts used a complete immutable stock source export,
the same installed external dependencies for both implementations, source-lock
comparison, and actual direct dependency version checks. They did not yet attest
the complete installed pnpm graph or reproduce every workspace's own external
peer-instance links. They remain same-environment diagnostic comparisons;
neither is a fully attested production-runtime qualification. That limitation
is preserved rather than retroactively relabeling their evidence.

The host and raw GC/load observations remain in the original report. Fresh
process does not imply cold OS page cache. The measurement covers persistence
APIs and selected service acquisition, not the full UI/provider/network path.
All whole-process RSS fields correctly report unavailable in this sandbox; the
separate Node-only peak values cannot establish total memory use or parity.

The recorded fixture paths were under `/tmp`. During preparation for the later
[dispatcher attempt](2026-11-14-dispatcher.md), this workspace's `/tmp` mount was
observed as `tmpfs`. This earlier report did not record historical filesystem
counters, so that later observation cannot supply its missing free-space data.
These local commit timings must not be presented as physical-device fsync or
power-loss tests. The dispatcher report documents the subsequent move to an
overlay fixture directory, its capacity reason and the resulting comparison
limits. The original readiness report and archive remain unchanged.

## Decisions after this attempt

The next native investigation targets blocking task dispatch around individual
storage lookups and the remaining metadata/posting lookup work. The source
inspection and this complete measurement justify that priority; they do not
establish a future speedup. A separate review identified avoidable visit-index
churn, but no write optimization is attributed to the readiness change.

CI now configures the application's pinned frozen pnpm installation. The exported
stock tree reproduces each workspace importer's installed external links while
mapping workspace aliases to stock source. Qualification requires the installed
application lock, package-manager metadata, Node executable, exact external
targets and source-matched virtual store to be attested.

The complete validation-only suite passed with just the declared root
dependencies and the linker's five benchmark aliases: seventy extra generated
root aliases were moved aside, both production validation workers completed,
and every alias was restored. The report records verified provenance, no issues,
the exact installed application-lock match, per-workspace resolution, and 439
external descriptors inside the frozen pnpm virtual store. An independent Python
validator accepted that retained resolution evidence. The same nonempty results,
transaction readback, rollback and reopen checks passed; no timing verdict was
produced. This provenance correction leaves the fixed latency workloads and
thresholds unchanged and is a distinct intervening change.

The [runtime-validation archive](2026-11-14-runtime-validation.raw.tar.gz) retains
both workers, the full report, logs/configs and exact benchmark source hashes.
Its SHA256 is `85f925184b2ad951fc656da0da0471c82fefc0116168335fc460c7967e742e35`;
the original report SHA256 is
`21f82ffaecd098c1b042c3b6530a0ae37b8946b7ffa2e0b8d2bcb59a8869e1dc`.

The runner also checks the helper and manifest hashes again after all workers.
Replacing an external executable during a run must fail the attempt rather
than attach mixed results to its initial hash. Future attempts keep separate
artifacts and reasons, including remaining regressions or inconclusive results.
