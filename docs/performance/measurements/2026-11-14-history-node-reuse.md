# Hosted history node-root experiment, 2026-11-14

**Decision: reject qualification.** All eleven fixed latency rows regressed
against stock T3 Code. Exact-source Rust, mapped components, production adapters,
the normal conditional commit hook and the controlled Linux application/recovery
flow passed. The performance job and final qualification failed; main remains
unchanged.

[Run 43618303547](https://github.com/glopyglerky/t3code-ls/actions/runs/43618303547)
measured node-root candidate `02296614b7d5989475fed25e86ebb5116766645f` against
immutable upstream `5fe2f7e4567d9e530ded0dabc9d706db27d77b84`. This is an isolated
5fe experiment, separate from the later f751 integration and sidebar correctness
work. Its [implementation record](../../porting/2026-11-14-history-node-reuse.md)
retains the original local fixture failure and incomplete compiler attempt.

The [unchanged performance ZIP](2026-11-14-history-node-reuse.raw.zip) retains
both validation workers, all sixteen measured workers, their complete configs,
outputs and raw timings, and the original JSON and Markdown. It contains 74
regular entries, 219,421 compressed bytes and 3,981,384 uncompressed bytes.
The separate [functional and verification archive](../../porting/evidence/2026-11-14-history-node-reuse-hosted.raw.tar.gz)
retains the original application/component ZIPs, complete native and qualification
job logs, final job metadata, verification script/result and exact validator inputs.
Its internal manifest binds all ten payloads. Neither archive contains a database
or native executable.

## Every fixed row

All times are milliseconds. Warm absolute columns are the named percentile over
all 96 measured calls. The cold absolute columns are the median of eight fresh
worker elapsed observations. Each ratio is the geometric mean of the eight
per-pair native/stock ratios; 95% intervals resample complete process pairs.
Consequently a paired ratio need not equal the quotient of pooled columns.
Positive latency change means slower than stock. Every interval is wholly above
1.0; no row passed and no aggregate can offset a slow workload.

| Scenario                            | Metric  | Stock ms | Native ms | Paired native/stock | 95% paired interval | Latency change | Decision  |
| ----------------------------------- | ------- | -------: | --------: | ------------------: | ------------------- | -------------: | --------- |
| `service-open-and-first-operations` | elapsed |  167.263 |   734.006 |               4.363 | [4.271, 4.441]      |        +336.3% | Regressed |
| `sidebar-shell`                     | median  |   14.574 |   158.574 |              10.920 | [10.810, 11.029]    |        +992.0% | Regressed |
| `sidebar-shell.p95`                 | p95     |   17.033 |   167.840 |              10.120 | [9.887, 10.359]     |        +912.0% | Regressed |
| `history-window`                    | median  |   30.316 |   122.902 |               4.051 | [4.013, 4.089]      |        +305.1% | Regressed |
| `history-window.p95`                | p95     |   31.771 |   128.670 |               4.025 | [3.953, 4.095]      |        +302.5% | Regressed |
| `message-search`                    | median  |    6.876 |    49.139 |               7.142 | [7.084, 7.197]      |        +614.2% | Regressed |
| `message-search.p95`                | p95     |    7.356 |    51.094 |               7.190 | [6.964, 7.465]      |        +619.0% | Regressed |
| `accent-search`                     | median  |    5.052 |    42.114 |               8.347 | [8.283, 8.428]      |        +734.7% | Regressed |
| `accent-search.p95`                 | p95     |    5.550 |    45.492 |               8.246 | [7.938, 8.613]      |        +724.6% | Regressed |
| `visit-commit`                      | median  |    1.441 |     4.800 |               3.318 | [3.257, 3.384]      |        +231.8% | Regressed |
| `visit-commit.p95`                  | p95     |    1.824 |     5.708 |               3.006 | [2.723, 3.262]      |        +200.6% | Regressed |

Independent verification reconstructed all eighteen raw worker records, the
fixed row and backend order, every pooled statistic and all eleven
10,000-resample intervals. The eight pairs, seed `1949524847`, three excluded
warmups and twelve measured cycles per worker were unchanged. Every raw sample
is retained; no slow sample was removed or retried.

The visit p95 interval has relative width 17.9%, the only row above the 10%
precision-warning level. Its lower bound is still 2.723, so the result is a
regression; an inconclusive result would also block qualification. The warning is not a slowdown
allowance. Qualification still requires each row's upper confidence bound to
be at most 1.0.

## Full distributions and tails

Cold has eight samples per backend; every warm workload has 96. These pooled
statistics retain the worst observed values as well as medians and p95s.

| Workload                            | Stock median | Stock p95 | Stock worst | Native median | Native p95 | Native worst |
| ----------------------------------- | -----------: | --------: | ----------: | ------------: | ---------: | -----------: |
| `service-open-and-first-operations` |      167.263 |   176.109 |     178.346 |       734.006 |    741.920 |      743.729 |
| `sidebar-shell`                     |       14.574 |    17.033 |      18.624 |       158.574 |    167.840 |      174.385 |
| `history-window`                    |       30.316 |    31.771 |      34.628 |       122.902 |    128.670 |      135.786 |
| `message-search`                    |        6.876 |     7.356 |       7.525 |        49.139 |     51.094 |       66.773 |
| `accent-search`                     |        5.052 |     5.550 |       5.968 |        42.114 |     45.492 |       53.864 |
| `visit-commit`                      |        1.441 |     1.824 |       3.156 |         4.800 |      5.708 |        6.246 |

All 96 native sidebar calls and all 96 native history calls exceeded 100 ms;
none of the corresponding stock calls did. Native message search had 22 calls
above 50 ms and accent search had one; neither backend had a search above
100 ms in this attempt. Native visits had 28 calls above 5 ms, with a maximum
of 6.246 ms; stock had none above 5 ms and a maximum of 3.156 ms. Neither visit
series exceeded 10 ms. These are descriptive counts, not additional pass rules.
They do not erase the severe write tails retained in earlier hosted attempts.

The cold aggregate contains service acquisition plus the first call to each
operation. The following are medians of its separately recorded components;
component medians do not necessarily sum to the aggregate median.

| Cold component    | Stock median ms | Native median ms |
| ----------------- | --------------: | ---------------: |
| `service_open_ms` |          47.355 |          235.092 |
| `sidebar-shell`   |          50.427 |          217.804 |
| `history-window`  |          42.551 |          167.128 |
| `message-search`  |          13.067 |           58.296 |
| `accent-search`   |          10.081 |           46.758 |
| `visit-commit`    |           4.052 |            7.740 |

Module loading and result fingerprinting are outside those API timers. Native
IPC and serialization and stock reader/writer-worker messaging remain inside
the timed operations. A fresh process does not imply a cold OS page cache; the
suite does not flush or claim to control that cache.

## Correctness, durability and runtime

The fixture and public requests remain those in the [fixed method](../persistence-api.md):
256 normal threads, one archived write thread, 3,000 search messages, 1,500
history items and 1,024-byte reasoning payloads. Validation retained 224 active
threads, three folded history items, one run, one node, a 13,976-byte history
projection, and twenty matches for each search. These counts were independently
derived from the retained result values, not merely trusted as reported numbers.

Initial results, including array order, matched immutable stock. Same-transaction
visit readback saw the changed row; the deliberate rollback restored the prior
row exactly; committed visits survived closing and reopening both production
services. Every warmup and measured response matched its validated result
identity. Each visit changed its timestamp, read back the complete row and
waited for commit acknowledgement.

Stock retained production SQLite WAL/NORMAL with reader and writer workers.
Native retained omdb Immediate durability. That intentional durability difference
and any latency cost remain in the write comparison. Rollback and reopen checks
do not simulate power failure or establish whole-app reliability.

The stock source was a full immutable export with relative and workspace imports
inside that export. Frozen pnpm 12.14.0 and Node 26.12.1 passed installed-lock,
per-workspace resolution and dependency validation with no issues. All 440
external dependency descriptors were retained and bound to the frozen virtual
store. Both source trees used their corresponding workspace aliases and the
verified external dependency graph.

## Functional results on this exact source

| Required job            | Job ID         | Outcome and retained scope                                                                                                         |
| ----------------------- | -------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| Plan                    | `132518114548` | Passed; exact-source affected qualification selected.                                                                              |
| Native and source       | `132518310442` | Passed: 425 Rust tests, zero failed/ignored, 37 result groups; source guards; 29 component typechecks and 49 mapped scripts.       |
| Production adapters     | `132522055625` | Passed in this workflow.                                                                                                           |
| Application             | `132522055597` | Passed: normal conditional hook, native-selection tests, original client/server builds and controlled Linux browser/recovery flow. |
| Performance/reliability | `132522055596` | Correctness/runtime passed; all eleven latency rows regressed.                                                                     |
| Qualification           | `132522966742` | Failed because the planned performance job failed. No qualifying receipt.                                                          |

The storage library passed all 208 tests, including all six new node-root tests:
missing/foreign roots, entry and string-byte caps, eligibility before corruption,
old/fresh snapshots, the real public records boundary after transaction closure,
and writer/savepoint/observation behavior. This hosted execution covers the
corrected fixture that could not finish compiling locally. The earlier local
207-pass/one-fixture-failure run and later SIGKILL before test execution remain
unchanged in their original archive; they are not relabeled as passes.

The real `git hook run pre-commit` completed in 2.520 seconds. Its receipt binds
the candidate-versus-first-parent diff and confirms exact clean restoration of
HEAD and index. The normal conditional selection reported formatter and linter
success for this Rust/docs-only diff. It did **not** rerun Knip; the configured
Knip tasks remain recorded, and the earlier ee178 hook's full scan evidence is
retained in its [functional report](../../porting/2026-11-14-ee178-functional-acceptance.md).
This records the hook's actual scope rather than inferring every configured task
ran on every commit.

The application built the original client and server with native selection. The
Linux x64 browser flow completed normal setup and local project creation, a
controlled free ACP approval and completed turn, client reload, same-state server
restart in a fresh client, and read-only inspection of canonical omdb rows and
event heads with no application SQLite files. Three completed snapshots retained
the same thread/run/attempt/provider-turn identity. The closed database remained
unchanged by read-only inspection. The proof retains two connection-refused
network messages and one HTTP 502 while the controlled flow completed; it is
not a zero-console-error claim.

This is one controlled original-client path on Linux. It does not qualify real
provider accounts, all UI features, native desktop/mobile distribution, macOS,
Windows, Iroh cutover or native Lust compilation. Those limitations and the
failed speed policy remain material even though this browser flow passed.

## Host, filesystem and advisory memory

The host reported an AMD EPYC 7763, four available CPUs, Linux 7.0.0-1017-azure,
and 16,760,344,576 memory bytes. Load averages moved from `[2.87, 0.87, 0.3]` to
`[1.7, 1.08, 0.45]`. The fixture filesystem reported type `0xef53` with an unknown
name in the bounded observer and 4,096-byte block/fragment sizes. Available
volume-wide blocks were 21,017,810 before setup, 20,240,775 after workers and
21,016,828 after cleanup. Those outside-timer observations are neither fixture
allocation measurements nor proof of a physical device or power-loss behavior.

Every measured worker returned advisory sampled process-tree RSS. Native peaks
ranged from 440,924 to 444,896 KiB; stock peaks ranged from 330,604 to 332,184 KiB.
The 25 ms sampler can miss short peaks and double-count shared pages. Separate
Node-only peaks and GC observations remain in the raw records. No forced GC was
used, and these observations do not establish whole-app memory or memory-safety
qualification.

## Interpretation and next decision

This attempt establishes a slowdown against stock for every fixed row. It does
not establish a causal before/after benefit from node-root reuse. The preceding
[ee178 comparison](2026-11-14-hosted-5fe.md) used a different CPU/host and had
native history median 108.544 ms against stock 19.312 ms. This attempt observed
122.902 ms against stock 30.316 ms. The smaller relative ratio here and the larger
absolute times cannot be attributed to this one code change across those hosts.
The separate instrumented attribution run is also not a substitute control.

The node experiment remains isolated and unqualified. Its immutable read-session
lookup bounds and validation behavior passed the affected tests, but no production
speed claim or main promotion follows. The next prepared
[eligible-row borrowing experiment](../../porting/2026-11-14-history-eligible-borrow.md)
removes only the first deep clone of each eligible compact row. Source predicts
1,500 fewer such clones for this fixture and no I/O-wrapper count reduction.
Its latency and Rust lifetime acceptance are still unmeasured/unexecuted at the
preparation checkpoint. It requires its own full exact-source hosted checks and
the unchanged comparison policy. Broader metadata caching, changed global
run-item queries and weakened corruption checks are outside that experiment.

## Exact retained identities

| Evidence                                  | Identity                                                           |
| ----------------------------------------- | ------------------------------------------------------------------ |
| Attempt                                   | `2026-11-14T17-10-57.475Z-2c988ab5`                                |
| Source commit                             | `02296614b7d5989475fed25e86ebb5116766645f`                         |
| Source tree                               | `77c2d5ad2ca09755a481a077ac058c1006a9ea80`                         |
| First parent                              | `ee178c237d5fad5842f950871003dc898ab26990`                         |
| Pinned stock                              | `5fe2f7e4567d9e530ded0dabc9d706db27d77b84`                         |
| Stock tree                                | `5d1b0d638e32a15f4ea85924c1e751b7da12a92a`                         |
| Run / measurement attempt / build attempt | `43618303547 / 1 / 1`                                              |
| Native helper SHA-256                     | `5738cef601d6e27a63d86b51918ed3bcfaf42ea23e9f741fa8c9fef1924a1b92` |
| Native manifest SHA-256                   | `fe01df8d6470ccf8960a54ab21f887e0e0d4361fe8899a5c63932e4c34ceeef9` |
| Policy digest                             | `d8c2b716d3c6b3d6d47d64d2f1ebd5d83c82ba09821f9d7a1f8f172d57ec6149` |
| Toolchain lock SHA-256                    | `2cddf6970e38ab122d6d62e4543cfb3d1fd23876d4a813a4b4421babc033d1bf` |
| Performance artifact                      | `14123692961`                                                      |
| Original performance ZIP SHA-256          | `5da9de275d87a2c0e19a70df53710b2a7df86b46e2b7f71dd85e834f6813447e` |
| Original performance report SHA-256       | `fddc4ea5ef86e9ebd5a941126ba668aa200180fe5760d0e1ae738194a017a98b` |
| Application artifact                      | `14123132463`                                                      |
| Original application ZIP SHA-256          | `72b2fd406958d7c7b85fa03f294a2a45228c8a65d4f337b3b1cc8f7c1f6cbfce` |
| Hook proof SHA-256                        | `2628e0c1dec5d2f370e11008986867d959e607e718bd9a412deb2241f9fb875a` |
| Browser proof SHA-256                     | `3d688b4084ec11e1f158febabf2857c88eec70af66eb834743af12691c296e3b` |
| Component artifact                        | `14124055016`                                                      |
| Original component ZIP SHA-256            | `fdf4001f4661993717beb1cea9f995dc275e17241c9fdf45fe17e9ecb482d37f` |
| Native job log SHA-256                    | `2a9259dfd6281513029eed1161f601d22473100e8a849052ef28dd7b5e391a65` |
| Verification result SHA-256               | `6c657d9ee8f11aea7a6c04c5cebdbe5482e080b04a22a49aa7752de3fcc14eb5` |
| Functional/verification archive SHA-256   | `23a73ffad0b2f1d33e9a6f942b20c111eb3cccc1954c0b868fb909f999f85637` |

Source remained clean before and after the measurement. Compiler provenance is
`verified-ci-manifest`, binding the release profile and required
`bluefe-rustc-via-lustr` route to source/tree, run and helper/toolchain hashes.
Observed `compiler_identity` remains null because the manifest did not supply
that field; expected lock identity remains separate. Manifest verification does
not infer an unreported compiler identity from executable bytes. The complete
runtime/source hashes, raw samples and failed verdict remain in the unchanged
performance ZIP. The verification result's `verification_status: passed` means
those retained claims and calculations matched; its `qualifies: false` preserves
the rejected performance result.
