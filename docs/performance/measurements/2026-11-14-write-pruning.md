# Observed-value write-pruning measurement, 2026-11-14

> Later compiler-provenance correction: this diagnostic supplied no native build
> manifest. Its compiler route and identity fields describe expected pins, not
> verified build evidence. See the [erratum](2026-11-14-compiler-provenance-erratum.md).
> Original raw evidence, hashes and timing results are unchanged.

**Decision: reject qualification. No write speedup is demonstrated.** All eleven
fixed latency rows still regress against the pinned stock baseline. The native
visit median is essentially unchanged from the preceding attempt: 5.712 ms to
5.780 ms, a descriptive increase of about 1.2%. The favorable visit p95 change
cannot replace the failed median or tail comparison with stock.

The subsequent [hosted write-pruning result](2026-11-14-hosted-write-pruning.md)
also failed all eleven rows and recorded a much larger visit tail: 191.083 ms
p95, with 14 of 96 native requests above 100 ms. That separate host result is
retained without attributing the stalls to this patch or overriding it with the
smaller local tail.

The [complete raw archive](2026-11-14-write-pruning.raw.tar.gz) retains both
validation workers, all sixteen measured workers, every config/output/timing,
the original reports, filesystem observations, experiment supervisor, and exact
benchmark/implementation/lock sources. Archived source bytes were read from the
immutable measured commit, never the later upstream merge working tree, and
verified against the report. No database or executable payload is included.

## Every paired comparison with stock

Times are milliseconds. Warm absolute columns use the named percentile over all
96 measured requests. Cold columns use the median of eight fresh-worker elapsed
values. The paired ratio and interval resample whole process pairs; they need
not equal the quotient of pooled columns. Positive change means slower than
stock. Every interval's lower bound is above 1.0, so every row fails.

| Scenario                            | Metric  | Stock ms | Native ms | Paired native/stock | 95% paired interval | Latency change | Decision  |
| ----------------------------------- | ------- | -------: | --------: | ------------------: | ------------------- | -------------: | --------- |
| `service-open-and-first-operations` | elapsed |  198.334 |   784.657 |               3.952 | [3.613, 4.247]      |        +295.2% | Regressed |
| `sidebar-shell`                     | median  |   11.938 |   153.415 |              12.737 | [12.543, 13.018]    |      +1,173.7% | Regressed |
| `sidebar-shell.p95`                 | p95     |   15.266 |   194.175 |              13.018 | [10.947, 15.003]    |      +1,201.8% | Regressed |
| `history-window`                    | median  |   24.279 |   113.825 |               4.629 | [4.409, 4.901]      |        +362.9% | Regressed |
| `history-window.p95`                | p95     |   33.527 |   134.598 |               4.038 | [3.336, 4.633]      |        +303.8% | Regressed |
| `message-search`                    | median  |    6.115 |    39.959 |               6.602 | [6.383, 6.845]      |        +560.2% | Regressed |
| `message-search.p95`                | p95     |    7.439 |    46.892 |               6.138 | [5.619, 6.660]      |        +513.8% | Regressed |
| `accent-search`                     | median  |    4.711 |    35.292 |               7.492 | [7.249, 7.727]      |        +649.2% | Regressed |
| `accent-search.p95`                 | p95     |    6.123 |    40.863 |               5.611 | [3.565, 7.334]      |        +461.1% | Regressed |
| `visit-commit`                      | median  |    1.397 |     5.780 |               4.093 | [3.814, 4.380]      |        +309.3% | Regressed |
| `visit-commit.p95`                  | p95     |    2.346 |     7.426 |               3.386 | [1.664, 6.582]      |        +238.6% | Regressed |

Correctness passed for the fixed fixture: exact data and ordering, nonempty
cardinality witnesses, same-transaction readback, the deliberate rollback,
restored prior rows and committed reopen. Every measured result matched its
validated digest. The digests equal the preceding table-handle attempt. This
bounded result does not waive the separately discovered sidebar parity fix.

Eight balanced process pairs, seed `1949524847`, three excluded warmups, twelve
measured cycles and all eleven rows remained unchanged. SQLite retained
WAL/NORMAL and native retained Immediate. Every timed visit used a new timestamp,
read the row inside its transaction, and waited for commit acknowledgement.

## What the write patch changes

The backend can omit a mutation when its proposed bytes exactly match the value
already observed by that transaction. It does not issue extra comparison reads
or use hash-only equality to decide that bytes are unchanged. The patch keeps
conflict observations, savepoint behavior, reservation and commit guarantees.
Read/count query algorithms and the previously tested table-handle design were
held fixed. Forty-five affected Rust tests and all 34 real RedbClient transaction
cases passed before the release build.

The implementation removes a class of redundant writes when that exact observed
value is available. This benchmark does not establish that such writes dominate
the committed-visit cost, and the measured total request did not become faster
at the median. No count or phase profile was collected that would justify a
stronger causal explanation.

## Descriptive native changes across attempts

The preceding [table-handle attempt](2026-11-14-table-handle.md) used the same
overlay fixture medium and frozen dependency layout. All 439 external dependency
descriptors and their digest remain identical. Separate runs still have different
cache, scheduling, frequency and shared-host load. These pooled native changes
are descriptive; they are not paired estimates of this write patch's effect.
Every increase and decrease remains visible.

| Scenario                            | Table-handle native ms | Write-pruning native ms | Observed latency change |
| ----------------------------------- | ---------------------: | ----------------------: | ----------------------: |
| `service-open-and-first-operations` |                832.349 |                 784.657 |                   -5.7% |
| `sidebar-shell`                     |                157.948 |                 153.415 |                   -2.9% |
| `sidebar-shell.p95`                 |                193.416 |                 194.175 |                   +0.4% |
| `history-window`                    |                117.014 |                 113.825 |                   -2.7% |
| `history-window.p95`                |                128.856 |                 134.598 |                   +4.5% |
| `message-search`                    |                 40.174 |                  39.959 |                   -0.5% |
| `message-search.p95`                |                 46.123 |                  46.892 |                   +1.7% |
| `accent-search`                     |                 35.749 |                  35.292 |                   -1.3% |
| `accent-search.p95`                 |                 39.279 |                  40.863 |                   +4.0% |
| `visit-commit`                      |                  5.712 |                   5.780 |                   +1.2% |
| `visit-commit.p95`                  |                  8.526 |                   7.426 |                  -12.9% |

The visit median increase and wider paired tail uncertainty remain part of the
record. The native p95 decreased across attempts, but its within-attempt ratio
interval is still entirely above stock. Read medians also moved even though
this candidate did not change read algorithms, which reinforces the need to
avoid attributing every between-run difference to the patch.

## Host and filesystem

The host reports Intel Xeon Platinum 8573C, nine available CPUs, Linux 6.18.57,
Node v26.12.1 and 10,451,423,232 memory bytes. The report retains load before and
after, all GC observations and every request duration. No overlapping local
build or heavy test was scheduled by this task during the fixed run; unobserved
shared-host work remains a limitation.

A fresh owned `TMPDIR` under
`/workspace/scratch/fcf5fdf8376a/t3-performance-write-pruning-fixtures` used the
same overlay filesystem as the previous two local attempts. The bounded observer
recorded `overlayfs`, type `0x794c7630`, with 4,096-byte block and fragment sizes.
All three observations were outside the timed APIs.

| Observation   | Available filesystem blocks | Available bytes on this reported block size |
| ------------- | --------------------------: | ------------------------------------------: |
| Before setup  |                  10,397,846 |                              42,589,577,216 |
| After workers |                   9,620,714 |                              39,406,444,544 |
| After cleanup |                  10,396,842 |                              42,585,464,832 |

The owned fixture directory was empty after completion and all 70 moved root
aliases were restored. Volume counters are not process memory or fixture
allocation. The overlay name does not establish a physical device or power-loss
guarantee. Whole-process RSS remains explicitly unavailable in this sandbox;
Node-only RSS is retained with its narrower scope. Fresh process does not imply
cold OS page cache.

## Exact identity

| Evidence                   | Identity                                                           |
| -------------------------- | ------------------------------------------------------------------ |
| Attempt                    | `2026-11-14T14-13-43.065Z-7ad9e68e`                                |
| Source commit              | `d661dbb2bf693b88c8969fcda9a0f5b98e3a2cca`                         |
| Source tree                | `549bff502c033739693b562a854437c0292a5019`                         |
| Dirty before / after       | `false / false`                                                    |
| Pinned upstream            | `913d7fc45ec5814fff25c819cfe4de193ea42d63`                         |
| Pinned upstream tree       | `95faedbd2644d046dac476ef9dcab2eb38a7eb97`                         |
| Native helper SHA256       | `1d04aae57b316d02eba6452c04b81bc616ab6d554f9686c76e973aa58068b60c` |
| Toolchain lock SHA256      | `2cddf6970e38ab122d6d62e4543cfb3d1fd23876d4a813a4b4421babc033d1bf` |
| Original report SHA256     | `dfd3c8b873fa542ab8edffbb35a3e4444a39e08eabbf005e19c325bc8b6e5aeb` |
| Raw archive SHA256         | `e481fba3d9f45511017469715d48273b8654c7db0a365e512cc95654a325a217` |
| installed lock sha256      | `f155f609971b8e9c031c4e5ebac811211c0b87f51adca7f60046ba0f5a9c5ceb` |
| modules metadata sha256    | `46485e3ab57d643215b46858291822b614d8d681f3c617c78113c250910ea4f4` |
| node executable sha256     | `08f12d42fb304468e8ee86247fe686aae5b225836ea65e9d6566c11ae6ba2e8e` |
| external resolution sha256 | `9bbe4c5be382f5ac67d8e7106161bd29474e21ad2487561746b167dd88a93b45` |

The helper was rebuilt through the pinned Lustr/BlueFe release route. This local
diagnostic supplied no same-run CI manifest or plan, so the report correctly
retains `build_profile: unverified-diagnostic` and `qualifies: false`. Installed
production-runtime provenance is verified with no issues. Source and executable
identities were checked at the end and the tree remained clean.

## Comparator cutoff and next decisions

This entire attempt ran against immutable upstream
`913d7fc45ec5814fff25c819cfe4de193ea42d63`. While it was running, upstream advanced
to `5fe2f7e4567d9e530ded0dabc9d706db27d77b84`, 63 commits later. The current run
was completed unchanged; no new source, dependency or oracle was mixed into
its samples. Its result remains an accurate record against the stated older
comparator, not a measurement of the new upstream revision.

The new upstream changes include `nodeSqliteClient` and the pnpm lock. Future
production/oracle measurements must attest that new source and installed
runtime explicitly. Even if the fixed fixture and requests remain unchanged,
stock implementation or dependency changes limit comparisons across this
cutoff. Earlier samples will not be relabeled, pooled with the new baseline, or
replaced. A new correctness validation must precede its speed verdict.

Keep main behind complete functional and speed qualification. Finish the
upstream sync and the separately scoped sidebar parity/query work, then measure
the resulting candidate against its own pinned stock baseline. The unchanged
write median means this patch cannot be presented as solving the remaining
write gap. The [hosted table-handle report](2026-11-14-hosted-table-handle.md)
also retains every failed row, its distinct host/filesystem and the passed
functional checks without converting them into speed qualification.
