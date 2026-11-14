# Hosted write-pruning comparison, 2026-11-14

> Later compiler-provenance correction: the supplied release manifest bound the
> reported route/profile, but the raw `compiler_identity` field came from the
> expected lock. See the [erratum](2026-11-14-compiler-provenance-erratum.md).
> Original raw evidence, hashes and failed timing verdicts are unchanged.

**Decision: reject qualification.** All eleven fixed latency rows regressed
against stock. The committed-write tail also needs investigation: native visit
p95 was 191.083 ms, the maximum was 355.931 ms, and 14 of 96 measured visits took
more than 100 ms. These samples remain in the report and verdict. This run does
not demonstrate a write speedup or identify why those requests were slow.

[Workflow run 43603651670](https://github.com/glopyglerky/t3code-ls/actions/runs/43603651670)
produced performance artifact `14117353393`. The
[unaltered ZIP](2026-11-14-hosted-write-pruning.raw.zip) retains both validation
workers, all sixteen measured workers, every config/output/raw timing, and the
original JSON and Markdown reports. Its 74 entries occupy 219,437 compressed
bytes and 3,980,892 uncompressed bytes. The ZIP and report hashes were verified
before archiving. No retry, outlier exclusion or workload change was used to
replace this result.

## Every fixed row

Times are milliseconds. Warm absolute columns use the named percentile over all
96 measured requests. Cold columns use the median of eight fresh-worker elapsed
values. The ratio and interval use whole process pairs, so the ratio need not
equal the quotient of the pooled absolute columns. Positive latency change
means slower than stock. Every paired interval's lower bound exceeds 1.0.

| Scenario                            | Metric  | Stock ms | Native ms | Paired native/stock | 95% paired interval | Latency change | Decision  |
| ----------------------------------- | ------- | -------: | --------: | ------------------: | ------------------- | -------------: | --------- |
| `service-open-and-first-operations` | elapsed |  130.033 |   724.114 |               5.763 | [5.359, 6.257]      |        +476.3% | Regressed |
| `sidebar-shell`                     | median  |   10.678 |   155.119 |              14.606 | [14.198, 15.045]    |      +1,360.6% | Regressed |
| `sidebar-shell.p95`                 | p95     |   12.348 |   169.259 |              13.654 | [13.037, 14.250]    |      +1,265.4% | Regressed |
| `history-window`                    | median  |   24.975 |   127.377 |               5.143 | [5.020, 5.277]      |        +414.3% | Regressed |
| `history-window.p95`                | p95     |   26.783 |   141.644 |               5.226 | [5.040, 5.433]      |        +422.6% | Regressed |
| `message-search`                    | median  |    5.590 |    36.198 |               6.480 | [6.311, 6.644]      |        +548.0% | Regressed |
| `message-search.p95`                | p95     |    6.218 |    38.943 |               6.111 | [5.396, 6.633]      |        +511.1% | Regressed |
| `accent-search`                     | median  |    3.851 |    31.291 |               8.259 | [8.046, 8.461]      |        +725.9% | Regressed |
| `accent-search.p95`                 | p95     |    4.525 |    34.952 |               7.521 | [6.959, 8.132]      |        +652.1% | Regressed |
| `visit-commit`                      | median  |    1.147 |     6.651 |              10.735 | [5.754, 21.221]     |        +973.5% | Regressed |
| `visit-commit.p95`                  | p95     |    1.577 |   191.083 |              38.435 | [17.489, 82.700]    |      +3,743.5% | Regressed |

Independent Python validation reconstructed all eighteen worker results, every
paired summary and all eleven 10,000-resample confidence intervals. Data and
ordering equality, nonempty cardinality witnesses, same-transaction readback,
the exact deliberate rollback, prior-row restoration and committed reopen
passed. All measured result digests matched the validated fixture. Installed
production-runtime provenance and all 440 external descriptors also passed.

Eight balanced pairs, seed `1949524847`, three excluded warmups, twelve measured
cycles and the fixed fixture remained unchanged. SQLite used its stock
WAL/NORMAL settings; native used Immediate. Each timed visit changed its
timestamp, read back the row inside the transaction and waited for commit
acknowledgement. The stronger native durability was retained and its cost is
included. No durability adjustment was made to improve the comparison.

## Committed-write dispersion

The slow visits occurred in several native processes. This table shows every
native pair's twelve measured visits; the ZIP retains every individual value.

| Pair | Native visit median ms | Native visit maximum ms | Visits above 100 ms |
| ---- | ---------------------: | ----------------------: | ------------------: |
| 0    |                 18.970 |                 218.849 |                   4 |
| 1    |                  3.490 |                  19.823 |                   0 |
| 2    |                 15.483 |                 355.931 |                   3 |
| 3    |                  7.216 |                  99.441 |                   0 |
| 4    |                  5.618 |                  69.454 |                   0 |
| 5    |                  5.307 |                  66.477 |                   0 |
| 6    |                 55.331 |                 278.313 |                   4 |
| 7    |                 43.701 |                 188.939 |                   3 |

The wide visit intervals report this variability; their lower bounds still
exceed 1.0. The result therefore fails the strict non-regression rule. No phase
profile or controlled before/after experiment was collected that can attribute
these stalls to observed-value write pruning, filesystem sync, garbage
collection or another specific cause. Treat the tail as a measured problem to
investigate, while preserving the full distribution and current verdict.

## Descriptive comparison with the previous hosted run

The [hosted table-handle attempt](2026-11-14-hosted-table-handle.md) used the same
fixed suite and upstream revision. Its native helper did not include the
observed-value write-pruning change. Both attempts verified the same 440
external dependency descriptors and resolution digest. They ran on separate
hosted machines with different reported CPU models, cache state and load; this
table does not isolate the patch's effect. Every fixed row is retained.

| Scenario                            | Prior hosted native ms | Write-pruning hosted native ms | Observed latency change |
| ----------------------------------- | ---------------------: | -----------------------------: | ----------------------: |
| `service-open-and-first-operations` |                588.521 |                        724.114 |                  +23.0% |
| `sidebar-shell`                     |                134.092 |                        155.119 |                  +15.7% |
| `sidebar-shell.p95`                 |                147.586 |                        169.259 |                  +14.7% |
| `history-window`                    |                117.208 |                        127.377 |                   +8.7% |
| `history-window.p95`                |                131.279 |                        141.644 |                   +7.9% |
| `message-search`                    |                 29.704 |                         36.198 |                  +21.9% |
| `message-search.p95`                |                 32.749 |                         38.943 |                  +18.9% |
| `accent-search`                     |                 25.595 |                         31.291 |                  +22.3% |
| `accent-search.p95`                 |                 29.289 |                         34.952 |                  +19.3% |
| `visit-commit`                      |                  3.386 |                          6.651 |                  +96.4% |
| `visit-commit.p95`                  |                  7.883 |                        191.083 |               +2,324.1% |

The [local write-pruning attempt](2026-11-14-write-pruning.md) also failed every
paired stock comparison and showed essentially unchanged native write median.
Its smaller write tail does not override this hosted failure. Different local
storage media and hosts further limit comparisons across those attempts.

## Host, filesystem and memory scope

This runner reported AMD EPYC 9V74 80-Core Processor, four available CPUs,
Linux 7.0.0-1017-azure, Node v26.12.1 and 16,760,348,672 memory bytes. The preceding
hosted table-handle run reported EPYC 9V45. Load averages moved from
`[2.87, 0.93, 0.33]` to `[1.53, 1.11, 0.48]`. These observations describe the
environment; they do not explain the write-tail stalls.

The bounded filesystem observer recorded type `0xef53`, `type_name: null`, and
4,096-byte block and fragment sizes for the `/tmp` fixture. All three observations
were outside timed APIs. The type ID does not establish a physical device or a
power-loss guarantee.

| Observation   | Available filesystem blocks | Available bytes on this reported block size |
| ------------- | --------------------------: | ------------------------------------------: |
| Before setup  |                  21,074,590 |                              86,321,520,640 |
| After workers |                  20,297,618 |                              83,139,043,328 |
| After cleanup |                  21,073,609 |                              86,317,502,464 |

Volume counters are not fixture allocation or memory usage. The process-tree
sampler succeeded on this host: measured native peaks ranged from 439,516 to
446,800 KiB, and stock peaks from 331,016 to 332,508 KiB. These advisory sampled
worker-tree observations can miss peaks and do not measure complete-app memory
or establish a memory qualification. Node-only RSS and GC observations remain
separately scoped in the raw report. Fresh workers do not imply cold OS cache.

## Exact identity

| Evidence                           | Identity                                                           |
| ---------------------------------- | ------------------------------------------------------------------ |
| Attempt                            | `2026-11-14T14-27-13.795Z-4d593cd9`                                |
| Source commit                      | `d661dbb2bf693b88c8969fcda9a0f5b98e3a2cca`                         |
| Source tree                        | `549bff502c033739693b562a854437c0292a5019`                         |
| Pinned upstream                    | `913d7fc45ec5814fff25c819cfe4de193ea42d63`                         |
| Pinned upstream tree               | `95faedbd2644d046dac476ef9dcab2eb38a7eb97`                         |
| Measurement / native build attempt | `1 / 1`                                                            |
| Native helper SHA256               | `9a084d4a821fcfe0e729e72f757c1a15762ca2a2b9c86c440632841318be5cc3` |
| Native manifest SHA256             | `0da1f24735bfc2351cc1bc4283ad20b08f7620ccea548a49203d7cca73f695da` |
| Toolchain lock SHA256              | `2cddf6970e38ab122d6d62e4543cfb3d1fd23876d4a813a4b4421babc033d1bf` |
| Policy digest                      | `012e41ab98ad7322663f9c93b279b66064bba383464e26410eb42d9895d8424c` |
| Original report SHA256             | `16718348f56567ce198e086909db063b013f1a8aa53df7e638b8e6851bed480b` |
| Original ZIP SHA256                | `9e404c97003a3d84c760ebe8c6b29f783b38c76e2c084b7970113e2181182663` |
| Filesystem observer SHA256         | `c9557b4c5d9d5aa2a8886ce7c4a8b16d47acb2c165e3238fa1c2ea07cb954fb9` |
| Installed lock SHA256              | `f155f609971b8e9c031c4e5ebac811211c0b87f51adca7f60046ba0f5a9c5ceb` |
| Modules metadata SHA256            | `e579435d7d216c3cf4591889db41dfd545f3b4b26fe1bee5bf6fbf3e56f3fb4d` |
| Node executable SHA256             | `08f12d42fb304468e8ee86247fe686aae5b225836ea65e9d6566c11ae6ba2e8e` |
| External resolution SHA256         | `215febeb42c657ec13ce66c7da4ce621ee7f87a4eefcf0a4a521955a085e897f` |

The source tree was clean before and after measurement. Qualify mode verified a
same-run release helper built through the pinned Lustr/BlueFe route. Installed
pnpm lock, metadata, Node executable and per-workspace external resolution
checks passed without issues. `qualifies` remains false because speed regressed.

## Application result and next decisions

Native, adapter and application gates passed, including the original Linux
browser and restart flow. Application artifact `14117637443` has ZIP SHA256
`65924edff9a2407e42f22519eefaa902d96f99ec853a9b77ceb0ee6622c9b913`.
This functional evidence is scoped to those checks. Performance and final
qualification failed, so the candidate remains blocked from promotion.

The next work includes the separately identified sidebar parity correction and
its bounded count path. The observed write tail remains unresolved and must be
retained in subsequent decisions. The upcoming upstream synchronization to
`5fe2f7e4567d9e530ded0dabc9d706db27d77b84` changes the SQLite client and dependency
lock. Future stock comparisons must identify that new oracle and actual runtime
explicitly; their timing changes cannot be attributed solely to a native patch.
The fixed workloads, paired protocol and strict thresholds remain unchanged.
