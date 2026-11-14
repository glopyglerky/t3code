# Hosted browser type successor

**All eleven latency rows regressed against stock T3 Code.** Correctness,
rollback/readback and reopen checks passed. Final qualification failed; main
remained unchanged. An inconclusive result would also block qualification.

Measured source: `b240bdc11b11237dba2c0134401eac5e0e4a096f`, tree
`2211c123d4a5a255ce03948c448b5eea6a4c5581`, sole parent
`a0eee4f2a0e47bf080b0d1514dfb3cbcb51fefce`. The frozen oracle is upstream
`f751c8488f7ca1e659e44ef12b05861c1bcf9bfb`, tree `a8157c768c81bdb0564612e2adca54dde1411a22`.
Run: [43626660476, attempt 1](https://github.com/glopyglerky/t3code-ls/actions/runs/43626660476).

The only change from a0 is the browser diagnostic optional-property type and its note.
The release database helper is byte-for-byte identical to a0. The two jobs ran
on different CPU models, so their differences do not establish a speed effect
from that type change or a causal gain from the earlier digest optimization.

## Exact retained evidence

- [Original performance ZIP](2026-11-14-browser-type.raw.zip): artifact
  `14126779134`, 219,290 bytes, SHA-256
  `bdc793bf9fdbb4df16e047fe7713d6c9d4d433986820b37a8be3751c1454b931`. All 74 original entries are unchanged.
- Report SHA-256: `e99450bea939906dac4bea237cfc41a34286e3aa351a69965dc0556390b78a23`.
- [Functional evidence and decoded job logs](../../porting/evidence/2026-11-14-browser-type-hosted.raw.tar.gz):
  849,650 bytes, 9 regular entries, SHA-256
  `3fb5816620fa64a23d31f3c2b6e808a58bed7f0ba3036abd6796ef47d493ff4f`. Its manifest binds every payload.
- Original application artifact `14126278678`: 661,827 bytes,
  SHA-256 `ab86a7901487fd8059c1d67c375f0a31c1d1fc7300ec52698c7fbcf411189b48`.

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
- Native release manifest SHA-256: `6dbc08b7e4e97a3b87380e9b084f634b96cd936dfb1038520c0741ce79193b9c`.
- Toolchain lock SHA-256: `2cddf6970e38ab122d6d62e4543cfb3d1fd23876d4a813a4b4421babc033d1bf`.
- Qualification policy digest: `2845573b3e4c1079f953de83094f35df09d21c92d0eb28f7602af9dca180bbf7`.

## Every qualification row

Values are pooled median or p95 milliseconds as labeled. Ratios are geometric
means of paired per-worker ratios and need not equal the ratio of pooled values.

| Workload / statistic              |   Stock ms |  Native ms | Paired ratio | 95% interval         | Verdict   |
| --------------------------------- | ---------: | ---------: | -----------: | -------------------- | --------- |
| service-open-and-first-operations | 149.274479 | 617.495508 |     3.831870 | [3.246239, 4.202363] | regressed |
| sidebar-shell                     |  12.149682 | 118.257074 |     9.736717 | [9.599945, 9.856505] | regressed |
| sidebar-shell.p95                 |  14.123277 | 122.989028 |     9.066643 | [8.821895, 9.288044] | regressed |
| history-window                    |  25.780355 | 115.127262 |     4.460359 | [4.362466, 4.543808] | regressed |
| history-window.p95                |  28.754717 | 119.838393 |     4.059491 | [3.693189, 4.412065] | regressed |
| message-search                    |   6.286568 |  42.336963 |     6.766407 | [6.627620, 6.902494] | regressed |
| message-search.p95                |   6.873604 |  44.501556 |     6.546847 | [6.404782, 6.664741] | regressed |
| accent-search                     |   4.757079 |  34.561645 |     7.262530 | [7.124458, 7.383859] | regressed |
| accent-search.p95                 |   5.429206 |  37.465078 |     6.949329 | [6.793941, 7.137903] | regressed |
| visit-commit                      |   1.416653 |   5.093045 |     3.627396 | [3.450118, 3.802805] | regressed |
| visit-commit.p95                  |   2.097795 |   6.577490 |     3.563084 | [2.906468, 4.519716] | regressed |

Precision warnings remain on `service-open-and-first-operations` (24.952% relative width), `history-window.p95` (17.709% relative width), `visit-commit.p95` (45.277% relative width). They do not waive a row; every interval is wholly above 1.0.

## Complete pooled distributions and tails

| Workload                          | Stock median / p95 / max ms          | Native median / p95 / max ms         |
| --------------------------------- | ------------------------------------ | ------------------------------------ |
| service-open-and-first-operations | 149.274479 / 241.287415 / 289.792649 | 617.495508 / 629.197906 / 632.290795 |
| sidebar-shell                     | 12.149682 / 14.123277 / 14.755259    | 118.257074 / 122.989028 / 127.262388 |
| history-window                    | 25.780355 / 28.754717 / 45.453321    | 115.127262 / 119.838393 / 124.307838 |
| message-search                    | 6.286568 / 6.873604 / 7.321444       | 42.336963 / 44.501556 / 48.842973    |
| accent-search                     | 4.757079 / 5.429206 / 5.719115       | 34.561645 / 37.465078 / 38.491636    |
| visit-commit                      | 1.416653 / 2.097795 / 8.002754       | 5.093045 / 6.577490 / 22.204481      |

Each count below is stock / native calls strictly above the threshold. Cold has
eight samples per side; every warm workload has 96. No outlier is removed.

| Workload                          |   >5 ms |  >10 ms | >50 ms | >100 ms |
| --------------------------------- | ------: | ------: | -----: | ------: |
| service-open-and-first-operations |   8 / 8 |   8 / 8 |  8 / 8 |   8 / 8 |
| sidebar-shell                     | 96 / 96 | 96 / 96 | 0 / 96 |  0 / 96 |
| history-window                    | 96 / 96 | 96 / 96 | 0 / 96 |  0 / 96 |
| message-search                    | 96 / 96 |  0 / 96 |  0 / 0 |   0 / 0 |
| accent-search                     | 25 / 96 |  0 / 96 |  0 / 0 |   0 / 0 |
| visit-commit                      |  1 / 51 |   0 / 3 |  0 / 0 |   0 / 0 |

## Cold service components

Component medians do not sum exactly to the median cold cycle.

| Component       | Stock median ms | Native median ms |
| --------------- | --------------: | ---------------: |
| service_open_ms |       42.402451 |       217.776269 |
| sidebar-shell   |       42.786100 |       156.970203 |
| history-window  |       38.154906 |       144.578784 |
| message-search  |       12.462330 |        51.728207 |
| accent-search   |        7.575835 |        39.110203 |
| visit-commit    |        4.451616 |         8.229030 |

## Host and advisory observations

The runner reported INTEL(R) XEON(R) PLATINUM 8573C, 4 CPUs,
Linux 7.0.0-1017-azure, 16,759,300,096 bytes of memory and
Node v26.12.1. Load observations were `[3.22, 0.94, 0.32]` before and
`[2.25, 1.32, 0.54]` after. Filesystem type was `0xef53`
with no reported friendly name and 4,096-byte blocks.
Available blocks were 21,016,272 before setup,
20,239,137 after workers and
21,015,290 after cleanup. These observations
do not establish an otherwise idle host, physical device or power-loss guarantee.

| Advisory observation over eight workers   | Stock           | Native          |
| ----------------------------------------- | --------------- | --------------- |
| Sampled process-tree peak RSS range (KiB) | 330,924–332,580 | 440,100–442,984 |
| Node peak RSS range (KiB)                 | 330,808–336,912 | 312,520–336,616 |
| Observed GC total (ms)                    | 79.253435       | 202.968061      |

The 25 ms process-tree samples may miss peaks and double-count shared pages.
Some samples observe processes during exit and report unavailable reads; those
reasons remain in the raw evidence. Node peak RSS uses a separate method and
excludes helper memory. Do not subtract the two series to infer exact helper
usage. Memory/GC values are advisory, not memory or safety qualification.

## Functional gates and remaining scope

The native/source/component and adapter jobs passed. The native log contains
421 passed Rust tests across 37 result groups, including 203 storage unit tests,
with zero failures and zero ignored tests. The normal pre-commit hook passed in
32.111 seconds and its stderr explicitly completed fmt,
lint and Knip. Hook proof SHA-256:
`9053576c48918a49660e43cf356cc2c653553d9edd65808dda20f11278606cbd`.

That hook staged only two first-parent paths: the diagnostic helper and the
type-fix note. It did not stage the inherited diagnostic test or acceptance
runner containing a0's eight lint findings. Successful Knip and the application
flow do not establish lint cleanliness for those omitted files. The
[inherited-hook correction](../../porting/2026-11-14-inherited-hook-coverage.md)
records this coverage gap and the required broader source selection.

The application typecheck, bundle/native selection and original Linux browser
flow passed. Setup, local project creation, controlled ACP approval/completed
turn, client reload and same-state server restart completed through the normal
client. All three snapshots retain identical completed run/attempt/provider-turn
identities and completion timestamps. Read-only native inspection checked omdb
rows/event heads and the absence of application SQLite files; the closed-state
inspection reported unchanged database bytes. Browser proof SHA-256:
`d21c4cfb725ff708718bcc95bbc58c5b43a3dc2fd26fb5789ee00aa67811d9a0`.

The persisted browser evidence contains six legacy error entries, twelve bounded
structured events and eleven phases. No cap was reached or entry dropped. Initial
setup includes a WebSocket HTTP-authentication failure and a shell GET abort.
The server-restart-starting phase records three connection-refused errors and
two HTTP 502 entries, with environment GET and traces POST events between
18:46:14.975Z and the ready marker at 18:46:21.738Z. These are phase associations,
not established causes or deduplicated incident counts. No error event is
recorded after ready within the persisted window. Cleanup remains outside that
window. This is neither an error-free claim nor Windows/macOS/mobile or
every-provider acceptance.

| Job                                                 | ID                                                                                                 | Result  |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------- | ------- |
| Select fork checks and porting work                 | [132543913397](https://github.com/glopyglerky/t3code-ls/actions/runs/43626660476/job/132543913397) | success |
| Affected Rust packages and source guards / qualify  | [132544156723](https://github.com/glopyglerky/t3code-ls/actions/runs/43626660476/job/132544156723) | success |
| Affected production adapters                        | [132548643709](https://github.com/glopyglerky/t3code-ls/actions/runs/43626660476/job/132548643709) | success |
| Rust integration through the application            | [132548643739](https://github.com/glopyglerky/t3code-ls/actions/runs/43626660476/job/132548643739) | success |
| Paired stock and native performance and reliability | [132548643763](https://github.com/glopyglerky/t3code-ls/actions/runs/43626660476/job/132548643763) | failure |
| Record verified qualification                       | [132549650117](https://github.com/glopyglerky/t3code-ls/actions/runs/43626660476/job/132549650117) | failure |

The final receipt rejected the failed planned performance job. No qualified
receipt or main promotion followed. Every prior slowdown and failed attempt
remains retained. The fixed workload, threshold, durability settings and sample
count were not changed to obtain acceptance. A separate same-host before/after
design is required to decide whether the digest experiment merits retention;
this report makes no such causal speed claim.
