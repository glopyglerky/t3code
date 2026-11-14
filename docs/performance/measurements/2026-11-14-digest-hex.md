# Hosted digest hex experiment

**All eleven latency rows regressed against stock T3 Code.** Correctness,
rollback/readback and reopen checks passed. Final qualification failed; main
remained unchanged. An inconclusive result would also block qualification.

Measured source: `a0eee4f2a0e47bf080b0d1514dfb3cbcb51fefce`, tree
`86a8fe7990cc663fe226637b39b51ad1f07e45ed`, sole parent
`4b5719f34a8b76a23f3c77982940cea67234186c`. The frozen oracle is upstream
`f751c8488f7ca1e659e44ef12b05861c1bcf9bfb`, tree `a8157c768c81bdb0564612e2adca54dde1411a22`.
Run: [43625465013, attempt 1](https://github.com/glopyglerky/t3code-ls/actions/runs/43625465013).

The sole Rust production change from the sidebar-correctness parent is the
final lowercase hex encoding in `request_digest`. Canonical hash inputs, stored
keys, request receipts, schema, durability and limits remain unchanged. The
[decision and local compatibility evidence](../../porting/2026-11-14-request-digest-hex.md)
remain separate from this hosted result. Browser diagnostics were also added
for later failure investigation; they do not change the timed API workload.

This is a comparison against stock, not a controlled before/after digest test.
The parent was measured on another host. A smaller stock/native ratio across
different runs cannot prove a causal improvement from the hex change.

## Exact retained evidence

- [Original performance ZIP](2026-11-14-digest-hex.raw.zip): artifact
  `14126122080`, 219,423 bytes, SHA-256
  `26dbe0c3fae812ad4ada760ac8ebc9e3f7239437d8343682a3e704eb7ec521cc`. All 74 original entries are unchanged.
- Report SHA-256: `1117a9dd9075f54e40ce623b2df0c9f835ebb105c7c4f0841a8dbbebbce75635`.
- [Functional evidence and decoded job logs](../../porting/evidence/2026-11-14-digest-hook-failure.raw.tar.gz):
  148,873 bytes, 10 regular entries, SHA-256
  `74837f4d2122a599747d6ebdbc35428ad915cf30632a46804b79b1004bf4b017`. Its manifest binds every payload.
- Original application artifact `14126053422`: 2,706 bytes,
  SHA-256 `96f828be019c3afa5f52e3fd1bd9547cd8106d5c28413fcc1f4986c410f64442`.

Verification matched all eighteen raw worker files to the embedded records,
derived retained correctness/cardinality witnesses, recomputed every pooled
statistic and paired ratio, and reproduced every 10,000-resample interval. The
verification script and result are retained with the functional evidence.
This verifies the failed evidence; it does not create a qualification receipt.

## Fixed method and production identity

Eight balanced pairs use seed `1949524847`, three excluded warmups and twelve
measured calls per warm workload. All eleven rows gate independently at native
/ stock ratio 1.0 with paired 95% confidence. Warm distributions each contain
96 calls; cold service acquisition plus the first API cycle contains eight.
The fixed fixture has 256 normal threads plus one archived write thread, 3,000
search messages plus one input, 1,500 turn items and 1,024-byte reasoning payloads.
The pristine stock export and native adapters use the same frozen production
pnpm graph; 440 external dependency descriptors and per-workspace resolutions
were verified. Source was clean before and after every complete attempt.

The actual `Omdb.make` / native projection and search services include IPC and
serialization. Native commits retain Immediate acknowledgement; stock retains
its normal SQLite WAL/NORMAL settings. Result verification, rollback and reopen
run unchanged. Warm fingerprint checks remain outside each API timer. Cold
means a fresh process/service, not a flushed OS page cache.

Compiler provenance is `verified-ci-manifest`; producer and measurement attempts
are both 1. That status binds the actual source/run/helper/lock/manifest fields,
not a compiler origin inferred from arbitrary executable bytes. Observed
`compiler_identity` remains null and expected pinned identity remains separate.

- Helper SHA-256: `1758f86962de09120a5b72005469809c5e6c7da3ada26585f4e0a41666c7351c`.
- Native release manifest SHA-256: `8511f61dd820884d67d6ce9cdf3398c41e85272794e3278d5f50fa21550412b1`.
- Toolchain lock SHA-256: `2cddf6970e38ab122d6d62e4543cfb3d1fd23876d4a813a4b4421babc033d1bf`.
- Qualification policy digest: `2845573b3e4c1079f953de83094f35df09d21c92d0eb28f7602af9dca180bbf7`.

## Every qualification row

Values are pooled median or p95 milliseconds as labeled. Ratios are geometric
means of paired per-worker ratios and need not equal the ratio of pooled values.

| Workload / statistic              |   Stock ms |  Native ms | Paired ratio | 95% interval         | Verdict   |
| --------------------------------- | ---------: | ---------: | -----------: | -------------------- | --------- |
| service-open-and-first-operations | 166.909158 | 687.217689 |     4.112704 | [4.057145, 4.164453] | regressed |
| sidebar-shell                     |  14.601324 | 125.452164 |     8.597677 | [8.479478, 8.711188] | regressed |
| sidebar-shell.p95                 |  16.914777 | 130.996871 |     8.016528 | [7.679505, 8.382483] | regressed |
| history-window                    |  30.392782 | 119.421988 |     3.930837 | [3.886037, 3.974275] | regressed |
| history-window.p95                |  31.764749 | 124.623766 |     3.889091 | [3.770383, 4.004958] | regressed |
| message-search                    |   6.980783 |  43.997327 |     6.299858 | [6.244519, 6.360422] | regressed |
| message-search.p95                |   7.477113 |  46.508425 |     6.248291 | [6.063335, 6.417347] | regressed |
| accent-search                     |   5.062142 |  36.129252 |     7.107984 | [7.020106, 7.213340] | regressed |
| accent-search.p95                 |   5.586394 |  38.700684 |     6.913050 | [6.754697, 7.099408] | regressed |
| visit-commit                      |   1.488063 |   4.784089 |     3.220719 | [3.151497, 3.286552] | regressed |
| visit-commit.p95                  |   1.910253 |   5.850988 |     3.112485 | [2.922080, 3.317444] | regressed |

Precision warnings remain on `visit-commit.p95` (12.703% relative width). They do not waive a row; every interval is wholly above 1.0.

## Complete pooled distributions and tails

| Workload                          | Stock median / p95 / max ms          | Native median / p95 / max ms         |
| --------------------------------- | ------------------------------------ | ------------------------------------ |
| service-open-and-first-operations | 166.909158 / 171.170393 / 171.590474 | 687.217689 / 698.544234 / 698.916718 |
| sidebar-shell                     | 14.601324 / 16.914777 / 18.157341    | 125.452164 / 130.996871 / 139.728898 |
| history-window                    | 30.392782 / 31.764749 / 36.651795    | 119.421988 / 124.623766 / 133.915656 |
| message-search                    | 6.980783 / 7.477113 / 10.000277      | 43.997327 / 46.508425 / 54.784815    |
| accent-search                     | 5.062142 / 5.586394 / 5.858135       | 36.129252 / 38.700684 / 39.575917    |
| visit-commit                      | 1.488063 / 1.910253 / 2.342383       | 4.784089 / 5.850988 / 7.257923       |

Each count below is stock / native calls strictly above the threshold. Cold has
eight samples per side; every warm workload has 96. No outlier is removed.

| Workload                          |   >5 ms |  >10 ms | >50 ms | >100 ms |
| --------------------------------- | ------: | ------: | -----: | ------: |
| service-open-and-first-operations |   8 / 8 |   8 / 8 |  8 / 8 |   8 / 8 |
| sidebar-shell                     | 96 / 96 | 96 / 96 | 0 / 96 |  0 / 96 |
| history-window                    | 96 / 96 | 96 / 96 | 0 / 96 |  0 / 96 |
| message-search                    | 96 / 96 |  1 / 96 |  0 / 2 |   0 / 0 |
| accent-search                     | 52 / 96 |  0 / 96 |  0 / 0 |   0 / 0 |
| visit-commit                      |  0 / 33 |   0 / 0 |  0 / 0 |   0 / 0 |

## Cold service components

Component medians do not sum exactly to the median cold cycle.

| Component       | Stock median ms | Native median ms |
| --------------- | --------------: | ---------------: |
| service_open_ms |       46.752333 |       237.933785 |
| sidebar-shell   |       49.908085 |       183.744201 |
| history-window  |       42.658122 |       163.617774 |
| message-search  |       13.209940 |        53.597281 |
| accent-search   |        8.137248 |        40.658236 |
| visit-commit    |        4.165138 |         7.712826 |

## Host and advisory observations

The runner reported AMD EPYC 7763 64-Core Processor, 4 CPUs,
Linux 7.0.0-1017-azure, 16,760,344,576 bytes of memory and
Node v26.12.1. Load observations were `[2.39, 0.86, 0.31]` before and
`[2.01, 1.18, 0.49]` after. Filesystem type was `0xef53`
with no reported friendly name and 4,096-byte blocks.
Available blocks were 21,015,898 before setup,
20,238,764 after workers and
21,014,915 after cleanup. These observations
do not establish an otherwise idle host, physical device or power-loss guarantee.

| Advisory observation over eight workers   | Stock           | Native          |
| ----------------------------------------- | --------------- | --------------- |
| Sampled process-tree peak RSS range (KiB) | 330,412–332,400 | 442,152–446,252 |
| Node peak RSS range (KiB)                 | 330,144–337,140 | 312,924–337,136 |
| Observed GC total (ms)                    | 74.105075       | 206.274407      |

The 25 ms process-tree samples may miss peaks and double-count shared pages.
Some samples observe processes during exit and report unavailable reads; those
reasons remain in the raw evidence. Node peak RSS uses a separate method and
excludes helper memory. Do not subtract the two series to infer exact helper
usage. Memory/GC values are advisory, not memory or safety qualification.

## Functional gates and remaining scope

The native/source/component and production adapter jobs passed. The native log
contains 421 passed Rust tests across 37 result groups, including 203 storage
unit tests, with zero failures and zero ignored tests.

The application job stopped at its mandatory normal hook. Formatting passed;
lint reported eight findings in Node namespace imports and platform/architecture
access. Knip was started but did not complete successfully. The hook exited 1
after 9.672 seconds without timeout, then restored the
exact clean detached candidate HEAD/index/tree. Hook proof SHA-256:
`7d28bfe6a90423589bb9e3e0f77a90f560440e016d347460d0973e764835fc7e`.

No client typecheck, bundle or browser flow ran for this source. The separately
noticed optional-property type issue was a source-review finding, not the
observed failure. The [hook failure and correction record](../../porting/2026-11-14-inherited-hook-coverage.md)
preserves the eight actual findings and explains why a small successor's
first-parent hook is insufficient to cover inherited failures.

| Job                                                 | ID                                                                                                 | Result  |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------- | ------- |
| Select fork checks and porting work                 | [132540180798](https://github.com/glopyglerky/t3code-ls/actions/runs/43625465013/job/132540180798) | success |
| Affected Rust packages and source guards / qualify  | [132540384838](https://github.com/glopyglerky/t3code-ls/actions/runs/43625465013/job/132540384838) | success |
| Affected production adapters                        | [132544147653](https://github.com/glopyglerky/t3code-ls/actions/runs/43625465013/job/132544147653) | success |
| Rust integration through the application            | [132544147654](https://github.com/glopyglerky/t3code-ls/actions/runs/43625465013/job/132544147654) | failure |
| Paired stock and native performance and reliability | [132544147682](https://github.com/glopyglerky/t3code-ls/actions/runs/43625465013/job/132544147682) | failure |
| Record verified qualification                       | [132545124254](https://github.com/glopyglerky/t3code-ls/actions/runs/43625465013/job/132545124254) | failure |

The final receipt rejected the failed planned application job. No qualified
receipt or main promotion followed. Every prior slowdown and failed attempt
remains retained. The fixed workload, threshold, durability settings and sample
count were not changed to obtain acceptance. A separate same-host before/after
design is required to decide whether the digest experiment merits retention;
this report makes no such causal speed claim.
