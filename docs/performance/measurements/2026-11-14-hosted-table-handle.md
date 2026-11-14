# Hosted table-handle comparison, 2026-11-14

> Later compiler-provenance correction: the supplied release manifest bound the
> reported route/profile, but the raw `compiler_identity` field came from the
> expected lock. See the [erratum](2026-11-14-compiler-provenance-erratum.md).
> Original raw evidence, hashes and failed timing verdicts are unchanged.

**Decision: reject qualification.** Correctness and verified production-runtime
provenance passed, but every fixed latency row regressed. All eleven paired 95%
intervals have lower bounds above 1.0. The qualification log identifies the
failed performance gate directly; passing functional gates cannot override it.

[Workflow run 43602183576](https://github.com/glopyglerky/t3code-ls/actions/runs/43602183576)
produced performance artifact `14116679404`. The
[unaltered ZIP](2026-11-14-hosted-table-handle.raw.zip) preserves the original
JSON/Markdown reports, both validation workers and all sixteen measured workers
with configs/output/raw timings. Its 74 entries occupy 219,271 bytes. Hashes were
verified before reading or archiving it.

## Every fixed row

Times are milliseconds. Warm absolute columns use the named percentile over all
96 measured requests; cold columns use the median of eight fresh-worker elapsed
values. The ratio and confidence interval use whole process pairs. Positive
latency change means slower than stock, and every row independently fails.

| Scenario                            | Metric  | Stock ms | Native ms | Paired native/stock | 95% paired interval | Latency change | Decision  |
| ----------------------------------- | ------- | -------: | --------: | ------------------: | ------------------- | -------------: | --------- |
| `service-open-and-first-operations` | elapsed |  108.568 |   588.521 |               5.499 | [5.258, 5.762]      |        +449.9% | Regressed |
| `sidebar-shell`                     | median  |    8.429 |   134.092 |              15.969 | [15.614, 16.388]    |      +1,496.9% | Regressed |
| `sidebar-shell.p95`                 | p95     |   10.091 |   147.586 |              14.784 | [14.388, 15.143]    |      +1,378.4% | Regressed |
| `history-window`                    | median  |   19.969 |   117.208 |               5.861 | [5.728, 6.047]      |        +486.1% | Regressed |
| `history-window.p95`                | p95     |   22.830 |   131.279 |               5.892 | [5.462, 6.630]      |        +489.2% | Regressed |
| `message-search`                    | median  |    4.680 |    29.704 |               6.401 | [6.239, 6.671]      |        +540.1% | Regressed |
| `message-search.p95`                | p95     |    5.462 |    32.749 |               5.997 | [5.403, 6.691]      |        +499.7% | Regressed |
| `accent-search`                     | median  |    3.330 |    25.595 |               7.723 | [7.436, 8.083]      |        +672.3% | Regressed |
| `accent-search.p95`                 | p95     |    3.897 |    29.289 |               6.994 | [5.916, 7.937]      |        +599.4% | Regressed |
| `visit-commit`                      | median  |    1.061 |     3.386 |               3.203 | [3.063, 3.364]      |        +220.3% | Regressed |
| `visit-commit.p95`                  | p95     |    1.587 |     7.883 |               4.883 | [2.744, 9.715]      |        +388.3% | Regressed |

Independent Python validation reconstructed every raw pair and all eleven
10,000-resample intervals. It also verified the complete runtime evidence and
440 external descriptors. Data/order equality, nonempty cardinality witnesses,
readback, deliberate rollback, restoration and committed reopen passed. Every
measured response matched the validated fixture result. Eight balanced pairs,
seed `1949524847`, three excluded warmups, twelve measured cycles, SQLite
WAL/NORMAL and native Immediate were unchanged. The broad visit p95 interval
remains visible; its lower bound still exceeds 1.0, so it is a failure rather
than a reason to discard the slow samples.

## Host and filesystem limits

This runner reports AMD EPYC 9V45, four available CPUs, Linux 7.0.0-1017-azure,
Node v26.12.1 and 16,760,344,576 memory bytes. Load averages changed from
`[1.68, 0.72, 0.29]` to `[2.66, 1.2, 0.5]`. Its CPU model differs from the earlier
hosted EPYC 7763 runs and the local Intel host. Absolute timing changes across
these jobs cannot isolate a table-cache effect. The within-attempt paired
stock comparison is the acceptance evidence and it fails.

The bounded observer reports filesystem type `0xef53` for the `/tmp` fixture,
with `type_name: null` and 4,096-byte block and fragment sizes. It records exact
block counts before setup, after workers and after cleanup. The type ID is
retained without inventing a physical-device classification or power-loss
promise. This hosted `/tmp` observation is different from the local tmpfs and
overlay facts; matching path names do not establish matching storage media.

| Observation   | Available blocks | Available bytes on this reported block size |
| ------------- | ---------------: | ------------------------------------------: |
| Before setup  |       21,075,071 |                              86,323,490,816 |
| After workers |       20,298,099 |                              83,141,013,504 |
| After cleanup |       21,074,090 |                              86,319,472,640 |

Volume free-space counters are not fixture allocation or process memory. Fresh
processes do not imply cold OS page cache, and the report's separately scoped
process-tree and Node-only RSS observations do not establish complete-app memory
parity. All GC, load and raw duration observations remain in the ZIP.

## Exact identity

| Evidence                   | Identity                                                           |
| -------------------------- | ------------------------------------------------------------------ |
| Source commit              | `b5250fe811e64ef06dd6b79963191ee48a91fe8f`                         |
| Source tree                | `85230b3187cd32bbeafb88b32d14b91f58363b27`                         |
| Pinned upstream            | `913d7fc45ec5814fff25c819cfe4de193ea42d63`                         |
| Pinned upstream tree       | `95faedbd2644d046dac476ef9dcab2eb38a7eb97`                         |
| Measurement attempt        | `1`                                                                |
| Native build attempt       | `1`                                                                |
| Native helper SHA256       | `3295cedd6bb7f31221ce7619ccf3cbe957c9eebe904bd0a24fbf1d0e5fd74ed2` |
| Native manifest SHA256     | `24c273c63523e68c463bee72db952d475fa67837e3a9b86f6a933991d8ff28e8` |
| Toolchain lock SHA256      | `2cddf6970e38ab122d6d62e4543cfb3d1fd23876d4a813a4b4421babc033d1bf` |
| Policy digest              | `012e41ab98ad7322663f9c93b279b66064bba383464e26410eb42d9895d8424c` |
| Original report SHA256     | `25ccb75bca9a7bd0e3a66c564f6384b025132dc33490748a0a86c529dc42ebd0` |
| Original ZIP SHA256        | `f2bedebffe23f094a82cad381a392e375aa8e66f5dc14484912f8571e339b7f3` |
| Filesystem observer SHA256 | `c9557b4c5d9d5aa2a8886ce7c4a8b16d47acb2c165e3238fa1c2ea07cb954fb9` |
| installed lock sha256      | `f155f609971b8e9c031c4e5ebac811211c0b87f51adca7f60046ba0f5a9c5ceb` |
| modules metadata sha256    | `d4e110a99aea49e5e98b7dccf3509e5dd9fbe142547cb03fc50071d140bcee76` |
| node executable sha256     | `08f12d42fb304468e8ee86247fe686aae5b225836ea65e9d6566c11ae6ba2e8e` |
| external resolution sha256 | `215febeb42c657ec13ce66c7da4ce621ee7f87a4eefcf0a4a521955a085e897f` |

The source tree was clean before and after the attempt. The report used qualify
mode and a same-run release helper from the pinned Lustr/BlueFe route. Installed
pnpm lock, metadata, Node executable and workspace-specific external resolution
checks passed with no issues. `qualifies` remains false because speed regressed.

## Application result and next decision

All non-performance gates in this workflow passed, including the original Linux
browser and restart flow. Application artifact `14117400950` has ZIP SHA256
`721cb125ab463e2c130635dbf09688e7c23ce391207a9526c4bd6047975c606e`.
That functional result establishes its tested scope; it cannot replace the
failed persistence-speed requirement or the separate newly discovered sidebar
parity regression test.

Retain the tested table-handle design provisionally as documented in the
[local attempt](2026-11-14-table-handle.md), and keep the next write-pruning
experiment isolated. Preserve the same fixed workloads and thresholds. The
[earlier hosted dispatcher failure](2026-11-14-hosted-dispatcher.md) also remains
available with its distinct host and every result row. No favorable native-only
comparison is substituted for the required paired stock result.
