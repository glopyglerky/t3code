# Immutable table-handle measurement, 2026-11-14

**Decision: reject qualification and retain the tested table-handle design
provisionally while isolating the next write experiment.** All eleven latency
rows still regress against stock. The native warm medians changed only modestly
from the previous attempt, and the observed cold median increased. No large
speedup, causal treatment effect or stock parity is established by this result.

The [complete raw archive](2026-11-14-table-handle.raw.tar.gz) retains both
validation workers, all sixteen measured workers, every config/output/timing,
the original report, filesystem observations, experiment supervisor, exact
benchmark and observer sources, and committed implementation/lock sources. Its
manifest verifies every retained file. It contains no database or executable
payload, and the original report remains unchanged.

## Every paired comparison with stock

Times are milliseconds. Warm absolute columns use the named percentile over all
96 requests; cold columns use the median of eight fresh-worker elapsed values.
The paired ratio and interval use complete process pairs rather than the
quotient of pooled columns. Positive latency change means slower than stock.
All eleven interval lower bounds exceed 1.0, so every row independently fails.

| Scenario                            | Metric  | Stock ms | Native ms | Paired native/stock | 95% paired interval | Latency change | Decision  |
| ----------------------------------- | ------- | -------: | --------: | ------------------: | ------------------- | -------------: | --------- |
| `service-open-and-first-operations` | elapsed |  200.751 |   832.349 |               4.078 | [3.694, 4.490]      |        +307.8% | Regressed |
| `sidebar-shell`                     | median  |   11.531 |   157.948 |              13.740 | [13.450, 14.037]    |      +1,274.0% | Regressed |
| `sidebar-shell.p95`                 | p95     |   13.727 |   193.416 |              11.070 | [8.345, 13.828]     |      +1,007.0% | Regressed |
| `history-window`                    | median  |   23.124 |   117.014 |               5.022 | [4.906, 5.151]      |        +402.2% | Regressed |
| `history-window.p95`                | p95     |   27.971 |   128.856 |               4.655 | [4.260, 5.033]      |        +365.5% | Regressed |
| `message-search`                    | median  |    5.919 |    40.174 |               6.828 | [6.597, 7.135]      |        +582.8% | Regressed |
| `message-search.p95`                | p95     |    7.587 |    46.123 |               6.182 | [4.939, 7.579]      |        +518.2% | Regressed |
| `accent-search`                     | median  |    4.545 |    35.749 |               7.871 | [7.708, 8.023]      |        +687.1% | Regressed |
| `accent-search.p95`                 | p95     |    5.714 |    39.279 |               6.599 | [5.698, 7.477]      |        +559.9% | Regressed |
| `visit-commit`                      | median  |    1.411 |     5.712 |               4.132 | [3.745, 4.464]      |        +313.2% | Regressed |
| `visit-commit.p95`                  | p95     |    2.372 |     8.526 |               3.122 | [2.675, 3.591]      |        +212.2% | Regressed |

Both production validation workers passed data/order equality, nonempty
cardinality witnesses, same-transaction readback, exact deliberate rollback,
restoration and committed reopen. Every later response matched its validated
result. The result digests equal the preceding dispatcher attempt, including
224 active threads, the folded bounded history with run/node rows and 20 hits
in each search. These checks cover this fixed fixture; they do not establish
correctness for every application state.

The complete eight balanced pairs, seed `1949524847`, three excluded warmups and
twelve measured cycles remained fixed. SQLite retained WAL/NORMAL; native omdb
retained Immediate commits. The distinct committed visit remained a timed
workload with independent median and p95 decisions.

## The isolated implementation change

A read-only transaction now opens its engine table handle lazily and reuses that
handle within the same immutable snapshot. It does not share the handle across
transaction roots or with a writable transaction. Writer overlays, conflict
observations, reservation, commit and rollback behavior retain their existing
paths. The change does not alter query algorithms, startup preparation or
commit durability. Forty focused/affected Rust tests, including the five new
handle-lifetime and snapshot cases, passed before the release rebuild.

The design is provisionally retained because one table handle owned by one
immutable transaction is a bounded lifetime that avoids repeated opens without
crossing snapshot ownership. That is an implementation rationale, not a measured
stock-speed result. Keeping it fixed also prevents a simultaneous cache reversal
from confounding the next write experiment. Its performance value must not be
claimed from the modest changes below.

## Descriptive native changes across attempts

The overlay fixture medium and frozen dependency layout match
[dispatcher-01](2026-11-14-dispatcher.md). All 439 external resolution descriptors
and their tuple digest remain identical. This removes the explicit media/runtime
changes that complicated the earlier comparison, but separate attempts still
have different cache, scheduling, frequency and shared-host load. These pooled
native-only changes are descriptive, not paired estimates of the patch's effect.

| Scenario                            | Dispatcher native ms | Table-handle native ms | Observed latency change |
| ----------------------------------- | -------------------: | ---------------------: | ----------------------: |
| `service-open-and-first-operations` |              721.481 |                832.349 |                  +15.4% |
| `sidebar-shell`                     |              159.468 |                157.948 |                   -1.0% |
| `sidebar-shell.p95`                 |              250.417 |                193.416 |                  -22.8% |
| `history-window`                    |              119.558 |                117.014 |                   -2.1% |
| `history-window.p95`                |              151.817 |                128.856 |                  -15.1% |
| `message-search`                    |               43.329 |                 40.174 |                   -7.3% |
| `message-search.p95`                |               48.116 |                 46.123 |                   -4.1% |
| `accent-search`                     |               37.201 |                 35.749 |                   -3.9% |
| `accent-search.p95`                 |               40.856 |                 39.279 |                   -3.9% |
| `visit-commit`                      |                5.970 |                  5.712 |                   -4.3% |
| `visit-commit.p95`                  |                8.416 |                  8.526 |                   +1.3% |

The cold increase remains visible. Warm medians moved by only a few percent,
while individual tail changes were larger and noisy. Neither the favorable
p95 observations nor the small median changes offset any failed stock-comparison
row. Commit behavior was unchanged; the observed visit differences do not prove
a write optimization. The next decision is based on the remaining complete
within-attempt gaps, not a favorable subset of this table.

## Host, filesystem and measurement scope

The host reports Intel Xeon Platinum 8573C, nine available CPUs, Linux 6.18.57,
Node v26.12.1 and 10,451,423,232 memory bytes. Load averages changed from
`[0.52, 1.09, 1.14]` to `[1.31, 1.21, 1.17]`. No overlapping local build or heavy
test was scheduled by this task during measurement. That does not exclude
unobserved shared-host work.

A fresh task-owned `TMPDIR` under
`/workspace/scratch/fcf5fdf8376a/t3-performance-table-handle-fixtures` kept the
same overlay medium as dispatcher-01. Node's `os.tmpdir()` was verified before
execution. The new bounded `statfs` observer recorded type `0x794c7630`,
`overlayfs`, with both reported block and fragment size equal to 4,096 bytes.
It ran before setup, after workers and after cleanup, outside every API timer.

| Observation                           | Available filesystem blocks | Available bytes on this reported 4,096-byte block size |
| ------------------------------------- | --------------------------: | -----------------------------------------------------: |
| Before setup                          |                  10,498,386 |                                         43,001,389,056 |
| After workers, before fixture cleanup |                   9,721,276 |                                         39,818,346,496 |
| After cleanup                         |                  10,497,404 |                                         42,997,366,784 |

These counters describe the whole volume, not fixture allocation or memory.
The owned fixture directory was empty when the run finished. The overlay name
does not identify physical storage or prove power-loss behavior. The observer
retains exact decimal strings, known type IDs/names and explicit unavailable
states; it changes reporting only and its exact source hash is recorded below.

Whole-process RSS remains unavailable in this sandbox because descendant
process enumeration is hidden. Separately labeled Node-only RSS and raw GC
observations remain in each worker. Fresh process does not mean cold OS cache,
and these persistence/API measurements do not establish whole-app performance.

## Exact identity and qualification limits

| Evidence                   | Identity                                                           |
| -------------------------- | ------------------------------------------------------------------ |
| Attempt                    | `2026-11-14T13-56-21.074Z-499e79b7`                                |
| Source commit              | `b5250fe811e64ef06dd6b79963191ee48a91fe8f`                         |
| Source tree                | `85230b3187cd32bbeafb88b32d14b91f58363b27`                         |
| Dirty before / after       | `false / false`                                                    |
| Pinned upstream            | `913d7fc45ec5814fff25c819cfe4de193ea42d63`                         |
| Pinned upstream tree       | `95faedbd2644d046dac476ef9dcab2eb38a7eb97`                         |
| Native helper SHA256       | `8995a46496c71d7619f1209048e17a7866990d8b328b62545a8d60f5d0435659` |
| Toolchain lock SHA256      | `2cddf6970e38ab122d6d62e4543cfb3d1fd23876d4a813a4b4421babc033d1bf` |
| Filesystem observer SHA256 | `c9557b4c5d9d5aa2a8886ce7c4a8b16d47acb2c165e3238fa1c2ea07cb954fb9` |
| Original report SHA256     | `8da52a79c854aa11e03b8191b3a7154ce94beddf40f93ef24b6b5c8ba84bb7aa` |
| Raw archive SHA256         | `a23d015ec2bbfe053e1c786e73c3fb45139e3b12bd649b5ff671f982e925d0ed` |
| installed lock sha256      | `f155f609971b8e9c031c4e5ebac811211c0b87f51adca7f60046ba0f5a9c5ceb` |
| modules metadata sha256    | `46485e3ab57d643215b46858291822b614d8d681f3c617c78113c250910ea4f4` |
| node executable sha256     | `08f12d42fb304468e8ee86247fe686aae5b225836ea65e9d6566c11ae6ba2e8e` |
| external resolution sha256 | `9bbe4c5be382f5ac67d8e7106161bd29474e21ad2487561746b167dd88a93b45` |

The helper was built in release mode through the pinned Lustr/BlueFe route.
This local diagnostic supplied no same-run CI manifest or plan, so it correctly
retains `build_profile: unverified-diagnostic` and `qualifies: false`. The installed
production runtime is verified with no issues, but that does not convert a local
diagnostic into hosted release qualification. Source and executable identities
were checked again at the end, and all 70 temporarily moved root aliases were
restored.

## Next decision

Keep main behind the complete application and strict speed gates. The next
isolated production experiment prunes writes whose new bytes equal the exact
value already observed by the transaction. It must preserve conflict evidence,
savepoint behavior and commit guarantees, pass focused tests, and then repeat
this same full contract and environment. No result is assumed before that run.

A separately discovered queued-input/cross-thread sidebar parity issue is being
fixed with its own regression coverage and compact query work. It is kept out
of the next write experiment so unrelated query changes cannot obscure the
write result. Passing this fixture does not waive that correctness fix.

The [hosted dispatcher report](2026-11-14-hosted-dispatcher.md) preserves its
independent eleven-row failure, verified production runtime and passed original
Linux browser/restart flow. Functional passes and previous native improvements
cannot replace the required stock-speed result.
