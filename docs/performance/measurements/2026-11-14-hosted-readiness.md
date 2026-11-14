# Hosted readiness comparison, 2026-11-14

**Decision: reject qualification.** The first hosted performance attempt passed
its correctness, ordering, nonempty-cardinality, rollback and committed-reopen
checks, but every fixed latency row regressed against the pinned stock source.
No hosted result row has been dropped or averaged away. This is a separate
hosted observation, not a substitute for or continuation of the local samples.

[Workflow run 43597075132](https://github.com/glopyglerky/t3code-ls/actions/runs/43597075132)
produced performance artifact `14114719436`. The
[unaltered hosted ZIP](2026-11-14-hosted-readiness.raw.zip) retains the original
JSON/Markdown reports, both validation workers and all sixteen measurement
workers/configs/logs/raw timings. The ZIP SHA256 was verified before reading it.

## All eleven measured rows

Times are milliseconds. Warm absolute columns use each named percentile over all
96 observations; cold columns use the median of eight fresh-worker elapsed
samples. Ratios and confidence intervals use whole process pairs. Positive
latency change means slower than stock. Every row independently fails.

| Scenario                            | Metric  | Stock ms | Native ms | Paired native/stock | 95% paired interval | Latency change | Decision  |
| ----------------------------------- | ------- | -------: | --------: | ------------------: | ------------------- | -------------: | --------- |
| `service-open-and-first-operations` | elapsed |  172.136 | 1,557.143 |               8.905 | [8.691, 9.097]      |        +790.5% | Regressed |
| `sidebar-shell`                     | median  |   14.958 |   436.176 |              28.964 | [28.428, 29.595]    |      +2,796.4% | Regressed |
| `sidebar-shell.p95`                 | p95     |   17.419 |   473.357 |              27.215 | [26.299, 28.163]    |      +2,621.5% | Regressed |
| `history-window`                    | median  |   31.272 |   294.415 |               9.331 | [9.131, 9.519]      |        +833.1% | Regressed |
| `history-window.p95`                | p95     |   33.280 |   318.356 |               9.580 | [9.383, 9.790]      |        +858.0% | Regressed |
| `message-search`                    | median  |    7.240 |   274.818 |              38.327 | [37.050, 39.566]    |      +3,732.7% | Regressed |
| `message-search.p95`                | p95     |    8.297 |   311.390 |              37.476 | [35.705, 39.361]    |      +3,647.6% | Regressed |
| `accent-search`                     | median  |    5.112 |   165.522 |              32.192 | [31.139, 33.233]    |      +3,119.2% | Regressed |
| `accent-search.p95`                 | p95     |    5.882 |   192.781 |              32.499 | [31.488, 33.585]    |      +3,149.9% | Regressed |
| `visit-commit`                      | median  |    1.532 |     5.971 |               3.872 | [3.731, 4.012]      |        +287.2% | Regressed |
| `visit-commit.p95`                  | p95     |    2.029 |     7.773 |               3.924 | [3.642, 4.297]      |        +292.4% | Regressed |

## Identity and environment

| Evidence                     | Identity                                                           |
| ---------------------------- | ------------------------------------------------------------------ |
| Source commit                | `bc0d46941ccac37a97062fb135544c62381cf453`                         |
| Source tree                  | `ae264726ef695ccb96006236c4745f8e3c915360`                         |
| Pinned upstream              | `913d7fc45ec5814fff25c819cfe4de193ea42d63`                         |
| Pinned upstream tree         | `95faedbd2644d046dac476ef9dcab2eb38a7eb97`                         |
| Measurement attempt          | `1`                                                                |
| Native build attempt         | `1`                                                                |
| Native helper SHA256         | `d19684d67a953a82d8c41eafa89e08f2575a0fdde30b3f3d8f45c2605b39814f` |
| Native manifest SHA256       | `f041e295b2cae9aa200df7239604e83b603646c69a56aa55d6998c4d46502d86` |
| Toolchain lock SHA256        | `2cddf6970e38ab122d6d62e4543cfb3d1fd23876d4a813a4b4421babc033d1bf` |
| Policy digest at measurement | `cd9ab0e80ebad5d66a26d75767f7fc44bcaf623329974eef36a5161ad49d2f69` |
| Original report SHA256       | `359e09fa4e234e09ed4c68c208b80c1dac034dc3fb5992384b61a73411dc2c6b` |
| Original ZIP SHA256          | `360b5f8eeafbcbd46ba69c393bcd22a4178a5d6202433808469e1e8944ac9c84` |

The original report records qualify mode, a clean source tree, an attested release
helper from the pinned Lustr/BlueFe route, and `qualifies: false` because speed
regressed. Its host was Linux x64, kernel 7.0.0-1017-azure, AMD EPYC 7763, four
available CPUs, Node v26.12.1, and 16,760,344,576 reported memory bytes. Load
averages changed from `[1.35, 0.33, 0.11]` to `[1.51, 0.98, 0.42]`. This differs
from the local Intel/nine-CPU host. Cross-host absolute changes are not a paired
optimization effect and are not used to qualify the implementation.

Eight balanced pairs, seed `1949524847`, three excluded warmups and twelve
measured cycles per worker were unchanged. SQLite used production WAL/NORMAL;
native retained Immediate durability. The raw reports retain the individual
request distributions, cold service and first-call components, GC and the
explicit memory scope. Fresh process does not imply cold OS page cache, and
persistence/API measurements do not establish whole-app responsiveness.

## Known runtime-provenance limitation

This hosted job ran the earlier policy, which provisioned the separate npm
component runtime. It verified complete pinned upstream source, source lock
compatibility and actual direct dependency versions, but did not attest the
complete installed pnpm transitive graph or preserve every production workspace's
peer-instance resolution. Both compared implementations used the supplied runtime;
that is a bounded same-environment comparison, not the stronger production-runtime
qualification required by the later policy.

This limitation is retained explicitly. The failed report is not rewritten,
removed, reclassified as a pass, or presented as proof that the whole native app
matches stock speed. The later CI change installs the pinned frozen pnpm graph,
retains workspace-specific external targets, and independently verifies installed
lock/metadata/Node hashes and every external resolution descriptor. The
[readiness report](2026-11-14-readiness.md) preserves the successful clean-layout
validation of that stronger path. All eleven speed rows remain useful failure
evidence from this hosted environment, and all remain visible above.

## Resulting decision

Keep main behind the complete qualification gate. Continue the bounded storage
optimization and repeat the same latency contract under the strengthened runtime
provenance. The next local dispatcher attempt is separate evidence with a new
helper identity; neither the hosted environment nor the runtime-policy change
will be used to attribute an unpaired speed difference to that patch.
