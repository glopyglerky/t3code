# Owned-dispatcher measurement, 2026-11-14

**Decision: reject performance qualification and continue the bounded read-path
optimization.** Correctness passed, but every one of the eleven fixed latency
rows has its entire paired 95% native/stock interval above 1.0. The full eight
pairs completed with a clean source tree and verified frozen production runtime.
No workload, warmup, sample or threshold was changed to obtain this result.

The [raw archive](2026-11-14-dispatcher.raw.tar.gz) retains the original report,
both validation workers, all sixteen measurement workers and their complete
configs/output/timings, filesystem observations, experiment supervisor, exact
benchmark sources, and the committed dispatcher and lock sources. It contains
no database files or executable binaries. The archive manifest verifies every
retained file. The original report is unchanged.

## Every paired comparison with stock

Times are milliseconds. Warm absolute columns use the named percentile over all
96 observations. Cold columns use the median of eight fresh-worker elapsed
values. Ratios and intervals resample whole process pairs, so the ratio need not
equal the quotient of the two pooled timing columns. Positive change means
slower than stock. Every row independently fails.

| Scenario                            | Metric  | Stock ms | Native ms | Paired native/stock | 95% paired interval | Latency change | Decision  |
| ----------------------------------- | ------- | -------: | --------: | ------------------: | ------------------- | -------------: | --------- |
| `service-open-and-first-operations` | elapsed |  218.328 |   721.481 |               3.520 | [3.136, 3.968]      |        +252.0% | Regressed |
| `sidebar-shell`                     | median  |   11.788 |   159.468 |              13.454 | [13.056, 13.793]    |      +1,245.4% | Regressed |
| `sidebar-shell.p95`                 | p95     |   15.080 |   250.417 |              14.899 | [12.624, 18.312]    |      +1,389.9% | Regressed |
| `history-window`                    | median  |   23.788 |   119.558 |               4.978 | [4.670, 5.229]      |        +397.8% | Regressed |
| `history-window.p95`                | p95     |   34.669 |   151.817 |               4.762 | [4.038, 5.425]      |        +376.2% | Regressed |
| `message-search`                    | median  |    6.057 |    43.329 |               7.070 | [6.808, 7.321]      |        +607.0% | Regressed |
| `message-search.p95`                | p95     |    8.214 |    48.116 |               6.352 | [5.616, 7.080]      |        +535.2% | Regressed |
| `accent-search`                     | median  |    4.770 |    37.201 |               7.791 | [7.410, 8.145]      |        +679.1% | Regressed |
| `accent-search.p95`                 | p95     |    6.652 |    40.856 |               7.042 | [5.945, 8.306]      |        +604.2% | Regressed |
| `visit-commit`                      | median  |    1.447 |     5.970 |               4.213 | [3.824, 4.649]      |        +321.3% | Regressed |
| `visit-commit.p95`                  | p95     |    2.156 |     8.416 |               4.389 | [3.458, 5.782]      |        +338.9% | Regressed |

The fixture still produces 224 active threads, a bounded nonempty folded history
with run and node rows, and 20 hits from each search. Initial data and order,
same-transaction readback, the exact deliberate rollback, restored prior data,
and committed reopen all passed. Every measured response matched its validated
result. Independent Python validation reconstructed all eleven ratios and
10,000-resample confidence intervals from the raw samples and confirmed every
regression.

SQLite retained its production WAL/NORMAL settings. Native omdb retained
Immediate commits. The timed visit updates a distinct timestamp, reads it within
the same transaction, and waits for commit acknowledgement. Neither the write
scenario nor the stronger native setting was omitted to improve the result.

## What changed since the readiness attempt

The helper now runs its serial database dispatcher on one owned, joined OS
thread. Direct bounded storage reads on that thread can execute without a
separate blocking-task dispatch for every lookup. Normal library calls and
spawned Tokio tasks keep their offloaded reads. Writer reservation, commit,
lifecycle operations, resource bounds, panic/error classification and snapshot
semantics retain their existing behavior. Focused execution-scope/storage tests
and all 34 production RedbClient transaction checks passed before measurement.

Two environment changes also occurred, and they limit attribution:

- The complete installed pnpm production graph is now verified. The exported
  stock tree preserves each workspace's dependency and peer instance, and all
  439 retained external descriptors resolve inside the frozen virtual store.
  The earlier local attempts had weaker runtime attestation and flattened root
  aliases. This attempt used only the declared root dependencies and the five
  benchmark links during measurement; all 70 extra generated aliases were
  restored afterward.
- Fixtures moved from the earlier `/tmp` paths to a new owned `TMPDIR` on the
  workspace overlay filesystem. Node's `os.tmpdir()` was checked before the
  attempt. The earlier location had insufficient available space when checked;
  a controlled sparse-copy probe showed that Node expands each 272,896,000-byte
  native copy fully. Nine copies alone require 2,456,064,000 bytes before the
  template, SQLite files, stock export and other fixture data. No unrelated
  temporary directories were removed and no incomplete timing run was started.

The paired native/stock comparison remains a comparison on one host, one medium
and the same frozen dependency graph. The following native-only before/after
numbers are descriptive across separate attempts. They cannot isolate the
dispatcher's effect from the dependency, storage-medium, cache, scheduling or
frequency changes.

| Scenario                            | Prior native ms | Dispatcher native ms | Observed latency change |
| ----------------------------------- | --------------: | -------------------: | ----------------------: |
| `service-open-and-first-operations` |       1,664.905 |              721.481 |                  -56.7% |
| `sidebar-shell`                     |         496.026 |              159.468 |                  -67.9% |
| `sidebar-shell.p95`                 |         576.220 |              250.417 |                  -56.5% |
| `history-window`                    |         306.730 |              119.558 |                  -61.0% |
| `history-window.p95`                |         384.382 |              151.817 |                  -60.5% |
| `message-search`                    |         317.866 |               43.329 |                  -86.4% |
| `message-search.p95`                |         415.917 |               48.116 |                  -88.4% |
| `accent-search`                     |         192.199 |               37.201 |                  -80.6% |
| `accent-search.p95`                 |         273.646 |               40.856 |                  -85.1% |
| `visit-commit`                      |           6.039 |                5.970 |                   -1.1% |
| `visit-commit.p95`                  |           9.706 |                8.416 |                  -13.3% |

The lower observed read latencies support continuing the investigation, but they
do not establish stock parity. Sidebar median is still about 13.5 times stock,
and its p95 is about 14.9 times stock. The write result remains about 4.2 times
stock at the median. The small cross-attempt visit change is not evidence of a
commit optimization; commit policy did not change and the environmental limits
above apply to that observation too.

## Filesystem and host evidence

`filesystem-observations.json` in the raw archive records `df -B1 -T` and exact
`statvfs` fields before and after the attempt. The selected fixture parent was
`/workspace/scratch/fcf5fdf8376a/t3-performance-dispatcher-fixtures` on `overlay`.
The volume reported 67,586,535,424 total bytes, with 43,070,238,720 available
before the run and 43,066,028,032 after fixture cleanup. The owned directory was
empty at completion. These are volume observations, not the fixture's allocated
bytes or process memory.

The same observation identifies `/tmp` as `tmpfs`. Its available space changed
between the preflight capacity probe and the actual run, without this experiment
removing unrelated state. The earlier reports did not collect historical
filesystem counters. Their recorded `/tmp` fixture paths and this later mount
observation must not be presented as physical-device fsync or power-loss tests.
The current container overlay also does not establish a physical device or its
persistence guarantees.

The host reports Intel Xeon Platinum 8573C, nine available CPUs, Linux 6.18.57,
Node v26.12.1 and 10,451,423,232 memory bytes. Load averages changed from
`[0.98, 0.89, 1.2]` to `[2.24, 1.41, 1.36]`. Whole-process RSS remains explicitly
unavailable because descendant enumeration is not exposed in this sandbox.
Separate Node-only peak RSS and GC observations remain in every worker report;
neither proves total memory parity. Fresh process does not imply cold OS cache.

## Exact identity

| Evidence                   | Identity                                                           |
| -------------------------- | ------------------------------------------------------------------ |
| Attempt                    | `2026-11-14T13-38-19.057Z-91e86ad1`                                |
| Source commit              | `c3f8245ed7daf3c16dcfcfbc33682811728bc5bf`                         |
| Source tree                | `04386131068654c883f2cca8689b5f5853c74eb3`                         |
| Dirty before / after       | `false / false`                                                    |
| Pinned upstream            | `913d7fc45ec5814fff25c819cfe4de193ea42d63`                         |
| Pinned upstream tree       | `95faedbd2644d046dac476ef9dcab2eb38a7eb97`                         |
| Native helper SHA256       | `9700ef49aa1131364ac5a220cfb5004e4ff4244f135e7d95bf305525606c077e` |
| Toolchain lock SHA256      | `2cddf6970e38ab122d6d62e4543cfb3d1fd23876d4a813a4b4421babc033d1bf` |
| Original report SHA256     | `055926c764a88d45ac5957a6b6e91316f0ef1ce0aae8b3d4fc84aa5ce46dfc27` |
| Raw archive SHA256         | `de2a0b53769749dd63775eba5ea5f0137ffc820047b80cf7bbf9c9db85cc8b89` |
| installed lock sha256      | `f155f609971b8e9c031c4e5ebac811211c0b87f51adca7f60046ba0f5a9c5ceb` |
| modules metadata sha256    | `46485e3ab57d643215b46858291822b614d8d681f3c617c78113c250910ea4f4` |
| node executable sha256     | `08f12d42fb304468e8ee86247fe686aae5b225836ea65e9d6566c11ae6ba2e8e` |
| external resolution sha256 | `9bbe4c5be382f5ac67d8e7106161bd29474e21ad2487561746b167dd88a93b45` |

The helper was rebuilt in release mode through the pinned Lustr/BlueFe route.
This local diagnostic supplied no same-run CI release manifest or policy plan;
the report therefore retains `build_profile: unverified-diagnostic` and
`qualifies: false`. Verified runtime provenance and a clean source tree do not
turn it into a hosted release qualification.

The complete command and supervisor are retained in the archive. The command
used the pinned Node executable, `--mode diagnostic`, the release helper with
the hash above, and a fresh output directory. Seed `1949524847`, eight balanced
pairs, three excluded warmups, twelve measured cycles and all eleven rows were
unchanged. The source, helper and relevant hashes were checked again at the end.

## Decisions after this attempt

Keep main behind the full application and strict speed qualification gates.
The next isolated change will reuse an engine table handle only within an
immutable read transaction. It must preserve writer and snapshot behavior and
pass focused correctness checks before another complete measurement. The next
attempt will keep this overlay fixture medium and frozen dependency layout so
its descriptive before/after comparison has fewer environmental differences.

A separate bounded filesystem observer was added after this attempt. It
records Node `statfs` type, block sizes and exact available block counts before
setup, after workers, and after cleanup, outside all timed intervals. Its source
hash will be retained in future reports. It does not alter the fixed requests,
statistical policy or any accepted/rejected sample. This attempt's original
report and supplemental filesystem observations remain intact.

The [earlier hosted attempt](2026-11-14-hosted-readiness.md) also retains all eleven
regressions, its distinct host, and its earlier runtime-provenance limitation.
Neither an earlier failure nor a future inconclusive result is replaced by a
more favorable attempt.
