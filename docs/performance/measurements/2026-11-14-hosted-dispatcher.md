# Hosted dispatcher comparison, 2026-11-14

**Decision: reject qualification.** Every fixed latency row regressed against the
pinned stock baseline, with all eleven 95% paired intervals entirely above 1.0.
Correctness passed, the source remained clean, the helper was a same-run release
artifact, and complete frozen production-runtime provenance was verified. This
failure cannot be attributed to the earlier missing runtime attestation.

[Workflow run 43600673237](https://github.com/glopyglerky/t3code-ls/actions/runs/43600673237)
produced performance artifact `14115933987`. The
[unaltered performance ZIP](2026-11-14-hosted-dispatcher.raw.zip) retains both
validation workers, all sixteen measured workers, their configs/output/timings,
and the original JSON/Markdown reports. Its 74 entries occupy 218,332 bytes.
No row or slow sample has been removed.

## All eleven paired results

Times are milliseconds. Warm absolute columns use the named percentile over all
96 observations; cold columns use the median of eight fresh-worker elapsed
values. Ratios and confidence intervals use whole process pairs, not the
quotient of pooled columns. Positive change means slower than stock.

| Scenario                            | Metric  | Stock ms | Native ms | Paired native/stock | 95% paired interval | Latency change | Decision  |
| ----------------------------------- | ------- | -------: | --------: | ------------------: | ------------------- | -------------: | --------- |
| `service-open-and-first-operations` | elapsed |  170.868 |   816.177 |               4.725 | [4.609, 4.827]      |        +372.5% | Regressed |
| `sidebar-shell`                     | median  |   14.868 |   197.547 |              13.277 | [13.048, 13.484]    |      +1,227.7% | Regressed |
| `sidebar-shell.p95`                 | p95     |   16.844 |   205.709 |              12.357 | [12.113, 12.598]    |      +1,135.7% | Regressed |
| `history-window`                    | median  |   30.861 |   147.961 |               4.791 | [4.738, 4.835]      |        +379.1% | Regressed |
| `history-window.p95`                | p95     |   32.175 |   155.873 |               4.862 | [4.773, 4.947]      |        +386.2% | Regressed |
| `message-search`                    | median  |    7.108 |    50.110 |               7.053 | [6.930, 7.171]      |        +605.3% | Regressed |
| `message-search.p95`                | p95     |    7.683 |    52.703 |               6.843 | [6.636, 6.995]      |        +584.3% | Regressed |
| `accent-search`                     | median  |    5.133 |    43.240 |               8.427 | [8.328, 8.517]      |        +742.7% | Regressed |
| `accent-search.p95`                 | p95     |    5.724 |    46.609 |               8.014 | [7.733, 8.303]      |        +701.4% | Regressed |
| `visit-commit`                      | median  |    1.506 |     5.078 |               3.355 | [3.252, 3.469]      |        +235.5% | Regressed |
| `visit-commit.p95`                  | p95     |    1.891 |     5.935 |               3.044 | [2.915, 3.208]      |        +204.4% | Regressed |

Independent Python validation reconstructed every raw pair, ratio and seeded
10,000-resample interval and confirmed every regression. Data/order equality,
nonempty cardinalities, transaction readback, deliberate rollback, restoration
and committed reopen passed. Eight balanced pairs, seed `1949524847`, three
excluded warmups and twelve measured cycles per worker remained fixed. SQLite
retained WAL/NORMAL and native retained Immediate; the distinct committed-write
scenario remained in both the median and p95 verdicts.

## Runtime, host and comparison limits

This job used the pinned frozen pnpm production installation and preserved each
stock workspace's dependency and peer instance. The installed application lock
matched its complete source document, every external target was within the
virtual store, and the report records no provenance issue. Its 440 retained
external descriptors have one difference from the local dispatcher attempt:
root optional `@cursor/sdk-linux-x64@1.2.0` is also installed on the hosted runner.
Every shared descriptor, including version, installed path and manifest hash,
is identical. This difference is retained instead of calling the two installed
trees byte-identical.

The host was Linux x64, kernel 7.0.0-1017-azure, AMD EPYC 7763, four available
CPUs, Node v26.12.1 and 16,760,344,576 reported memory bytes. Load averages changed
from `[1.76, 0.53, 0.18]` to `[1.81, 0.93, 0.38]`. This is a separate host from the
local Intel/nine-CPU attempt. Its original report predates the bounded filesystem
observer and does not establish the filesystem type or physical storage medium.
Those facts must not be inferred from the later local overlay observations.

The [local dispatcher report](2026-11-14-dispatcher.md) and
[earlier hosted report](2026-11-14-hosted-readiness.md) remain separate evidence.
Their native-only differences are not controlled estimates of this patch's
effect. The earlier hosted attempt also had weaker runtime provenance. The
current within-attempt stock comparison is the qualification evidence, and it
fails. Fresh processes do not imply cold OS page cache; advisory process-tree
RSS and Node-only RSS have different scopes. No memory-parity or complete-app
performance claim follows from these persistence measurements.

## Exact identity

| Evidence                   | Identity                                                           |
| -------------------------- | ------------------------------------------------------------------ |
| Source commit              | `c3f8245ed7daf3c16dcfcfbc33682811728bc5bf`                         |
| Source tree                | `04386131068654c883f2cca8689b5f5853c74eb3`                         |
| Pinned upstream            | `913d7fc45ec5814fff25c819cfe4de193ea42d63`                         |
| Pinned upstream tree       | `95faedbd2644d046dac476ef9dcab2eb38a7eb97`                         |
| Measurement attempt        | `1`                                                                |
| Native build attempt       | `1`                                                                |
| Native helper SHA256       | `c447c43876dc23aac68d40d5438334f13c0c9dba7ae7ef4db1efc5136b6ad22a` |
| Native manifest SHA256     | `05843ec6bb1b9800b9407809400733a5889aadea6aad6ad28bbe243cfa9ce0ff` |
| Toolchain lock SHA256      | `2cddf6970e38ab122d6d62e4543cfb3d1fd23876d4a813a4b4421babc033d1bf` |
| Policy digest              | `d788ac6ea1f5fac1cc31b8d80a1c4334cd40786ea750b3495d36d8e1f9fb9434` |
| Original report SHA256     | `8f7d104a13d3d7e69267ac16a40d036b255be196bea959d51bc8bdf04f5fb597` |
| Original ZIP SHA256        | `f25a8a34f89624a05dde0a922ee3f3acb187ff1ee323dda8a9c10b1c00c3c4c3` |
| installed lock sha256      | `f155f609971b8e9c031c4e5ebac811211c0b87f51adca7f60046ba0f5a9c5ceb` |
| modules metadata sha256    | `844df7b8c0f12aa0900667357d0d24cf19b7ce617f6a7911e6b6b15110da4b8f` |
| node executable sha256     | `08f12d42fb304468e8ee86247fe686aae5b225836ea65e9d6566c11ae6ba2e8e` |
| external resolution sha256 | `215febeb42c657ec13ce66c7da4ce621ee7f87a4eefcf0a4a521955a085e897f` |

## Application result and next decision

The same workflow's original Linux browser and restart flow passed. Its
application artifact is `14116622101`, with ZIP SHA256
`e4e50ddc5efaa221c0be35ff9eeb559e0eb2b2ff4915f16378316b581d7db474`.
The passed application flow and other non-performance gates do not override any
failed speed row. They establish their own tested behavior and scope.

Keep main behind the complete qualification gate. Continue the bounded immutable
read-transaction table-handle experiment and repeat the same full contract.
Neither a passing functional test nor an improvement over a previous native
attempt establishes parity with stock. Every later failure or inconclusive
attempt must retain its own evidence and decision.
