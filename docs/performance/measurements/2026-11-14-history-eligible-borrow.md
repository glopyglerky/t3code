# Hosted history borrowing experiment

**All eleven latency rows regressed against stock T3 Code.** Correctness,
reopen/rollback, compiled Rust, mapped components, production adapters and the
original Linux application flow passed. Final qualification failed and main
remained unchanged. An inconclusive speed result would also block qualification.

The measured source is `311ba836d74308b3ab89493c0b745e5296c0d5e6`, tree
`f36c074e3306ca2906137aa51e6f4e17d341982a`, sole parent
`02296614b7d5989475fed25e86ebb5116766645f`. This isolated branch retains the
frozen upstream `5fe2f7e4567d9e530ded0dabc9d706db27d77b84`, tree
`5d1b0d638e32a15f4ea85924c1e751b7da12a92a`. It is historical evidence in a
later f751/digest preparation package, not a measurement of that later source.

Run: [43621254851, attempt 1](https://github.com/glopyglerky/t3code-ls/actions/runs/43621254851).
The sole production change from its node-reuse parent replaces the first
Eligible IndexedRow deep clone with a scoped borrow. Reads, scans, JSON decoding,
error order, all three global run-item consumers and later owned output remain.
The source predicts 1,500 first clones avoided for the fixture; this run does
not measure allocation counts or isolate a causal latency benefit.

The parent ran on a different CPU/kernel (AMD EPYC versus this Intel Xeon).
Absolute timings and stock/native ratios across those runs cannot establish
whether borrowing helped. The experiment remains separate pending controlled
same-host evidence; no borrowed production change is carried into the digest
candidate. No threshold, workload, durability mode or policy changed.

## Exact evidence

- [Original performance ZIP](2026-11-14-history-eligible-borrow.raw.zip): artifact
  `14125305813`, 218,970 bytes, SHA-256
  `bbd1463211a2a8891e2a3491d5d4cb11c328993fe0ebc7b2a4a0352f80b07107`.
  Its 74 safe entries retain 3,981,169 uncompressed bytes, all worker samples
  and the original report. Report SHA-256:
  `4843498c53a82d0cacbf505695df0d5626c52b1fd9f9638219702934e04f8c63`.
- [Functional and independent verification archive](../../porting/evidence/2026-11-14-history-eligible-borrow-hosted.raw.tar.gz)
  is 1,019,997 bytes, 9 safe regular entries,
  SHA-256 `93d334672e164f4523e278e8299fb72fe97f8d7fe37904a2eb906e96f006ec8a`.
  Its manifest binds every original ZIP/log, final job list and the verifier/result.
- Application artifact `14124539115`, 660,612 bytes:
  `6386adfc3d2b9d5b3fd9100b3e4bc661aeae441ded1465b2ed8c54df20167cd3`.
- Component artifact `14124473342`, 58,995 bytes:
  `068ec5d653ee95835d888b5faf91bb1165f6cf34524a56ab03c9cd5040f3e543`.

Verification matched all eighteen retained worker files to the report, derived
cardinalities/digests from retained results, reconstructed every pooled statistic
and paired ratio/10,000-resample interval, and checked exact source/runtime
identity. This verifies the evidence; it does not turn its failed verdict green.

## Fixed method and production identity

Eight balanced pairs use seed `1949524847`, three excluded warmups and twelve
measured calls per warm workload. All eleven rows gate independently at native
/ stock ratio 1.0 with paired 95% confidence. Each warm distribution has 96
request samples; cold service acquisition plus the first API cycle has eight.
The immutable stock export and native production services use the same frozen
pnpm graph; 440 external dependency descriptors and per-workspace resolutions
were verified. Source was clean before and after. Both validation workers passed
correctness, committed-write/readback, rollback and reopen checks.

The fixture has 256 normal threads plus an archived write thread, 3,000 search
messages plus one input, 1,500 turn items and 1,024-byte repeated reasoning
payloads. Retained output witnesses are 224 active threads; a history window
with three items, one run and one node, 13,976 serialized bytes; and twenty
results for each search. Reduced history output reflects the real completed
turn folding behavior, not an empty fixture.

The helper is the real release `Omdb.make` / native projection and search path,
including IPC and normal Immediate commit acknowledgement. Stock preserves its
normal SQLite configuration. Producer and measurement attempts are both 1.
Compiler provenance is `verified-ci-manifest`; this verifies the manifest
binding, not the compiler inferred from arbitrary executable contents.
`compiler_identity` is null; expected compiler pins remain separate.

- Helper SHA-256: `280b1868dbeb397c05a1d0ffdd6a9efca932e4c5309e2e1058ec3a3df526869e`.
- Release manifest SHA-256: `bb50829e01548e95b7d7f075f436ef58bb262534966798311858ced866a08652`.
- Toolchain lock SHA-256: `2cddf6970e38ab122d6d62e4543cfb3d1fd23876d4a813a4b4421babc033d1bf`.
- Qualification policy digest: `d8c2b716d3c6b3d6d47d64d2f1ebd5d83c82ba09821f9d7a1f8f172d57ec6149`.

## Every qualification row

Values are pooled median or p95 milliseconds as labeled. Ratios are the
geometric mean of paired per-worker ratios, so they need not equal the ratio
of pooled values. Every lower confidence bound exceeds 1.0.

| Workload / statistic              |   Stock ms |  Native ms | Paired ratio | 95% interval           | Verdict   |
| --------------------------------- | ---------: | ---------: | -----------: | ---------------------- | --------- |
| service-open-and-first-operations | 114.382274 | 519.936415 |     4.269507 | [3.745056, 4.594062]   | regressed |
| sidebar-shell                     |   9.572704 | 118.903470 |    12.461985 | [12.377561, 12.544663] | regressed |
| sidebar-shell.p95                 |  10.862894 | 122.093952 |    11.498495 | [11.195106, 11.812771] | regressed |
| history-window                    |  19.893733 |  86.121520 |     4.336773 | [4.288960, 4.377636]   | regressed |
| history-window.p95                |  21.237938 |  88.702352 |     4.205979 | [4.175558, 4.233663]   | regressed |
| message-search                    |   4.827483 |  37.296510 |     7.752347 | [7.675641, 7.839283]   | regressed |
| message-search.p95                |   5.379959 |  39.241325 |     7.381696 | [7.254332, 7.506113]   | regressed |
| accent-search                     |   3.633328 |  32.082549 |     8.803271 | [8.687205, 8.914507]   | regressed |
| accent-search.p95                 |   4.154784 |  34.592477 |     8.439382 | [8.128569, 8.726324]   | regressed |
| visit-commit                      |   1.081849 |   3.949135 |     3.652947 | [3.582760, 3.723695]   | regressed |
| visit-commit.p95                  |   1.522883 |   5.684811 |     3.705536 | [3.294698, 4.148140]   | regressed |

The cold-cycle interval has relative width 19.885% and visit-commit p95 has
relative width 23.032%; both retain the report's precision warning. All other
rows have no such warning. Both warned intervals remain entirely above 1.0,
so neither warning changes the regression verdict or waives the row.

## Complete pooled distributions and tails

| Workload                          | Stock median / p95 / max ms          | Native median / p95 / max ms         |
| --------------------------------- | ------------------------------------ | ------------------------------------ |
| service-open-and-first-operations | 114.382274 / 165.107592 / 189.799427 | 519.936415 / 525.194628 / 525.333312 |
| sidebar-shell                     | 9.572704 / 10.862894 / 11.806636     | 118.903470 / 122.093952 / 136.261782 |
| history-window                    | 19.893733 / 21.237938 / 21.628303    | 86.121520 / 88.702352 / 90.134212    |
| message-search                    | 4.827483 / 5.379959 / 5.802587       | 37.296510 / 39.241325 / 40.400911    |
| accent-search                     | 3.633328 / 4.154784 / 4.677781       | 32.082549 / 34.592477 / 36.913643    |
| visit-commit                      | 1.081849 / 1.522883 / 1.700717       | 3.949135 / 5.684811 / 9.050651       |

Each cell below is stock / native sample count strictly above the threshold.
Cold has eight samples per side; each warm workload has 96.

| Workload                          |   >5 ms |  >10 ms | >50 ms | >100 ms |
| --------------------------------- | ------: | ------: | -----: | ------: |
| service-open-and-first-operations |   8 / 8 |   8 / 8 |  8 / 8 |   8 / 8 |
| sidebar-shell                     | 96 / 96 | 23 / 96 | 0 / 96 |  0 / 96 |
| history-window                    | 96 / 96 | 96 / 96 | 0 / 96 |   0 / 0 |
| message-search                    | 27 / 96 |  0 / 96 |  0 / 0 |   0 / 0 |
| accent-search                     |  0 / 96 |  0 / 96 |  0 / 0 |   0 / 0 |
| visit-commit                      |   0 / 8 |   0 / 0 |  0 / 0 |   0 / 0 |

The native sidebar exceeds 100 ms in all 96 calls. Native history is between
50 and 100 ms in all 96 calls. Neither search exceeds 50 ms here; every native
search still exceeds 10 ms. Eight native commits exceed 5 ms, none exceed
10 ms, and the native maximum is 9.051 ms. Earlier attempts with much larger
write tails remain retained; their absence here does not erase them or prove
a causal change in write behavior.

## Cold service components

These pooled component medians do not sum exactly to the median cold cycle.

| Component       | Stock median ms | Native median ms |
| --------------- | --------------: | ---------------: |
| service_open_ms |       31.860104 |       182.134979 |
| sidebar-shell   |       32.760824 |       148.049482 |
| history-window  |       29.646765 |       103.557028 |
| message-search  |        9.812291 |        44.663338 |
| accent-search   |        5.881836 |        35.066625 |
| visit-commit    |        3.265967 |         6.085021 |

## Host and advisory memory

The runner reported Intel Xeon 6975P-C, four CPUs, Linux 7.0.0-1014-aws,
16,464,302,080 bytes of memory and Node v26.12.1. Load observations were
`[2.51, 0.81, 0.29]` before and `[1.78, 0.99, 0.4]` after. Filesystem type was
`0xef53` with no reported friendly name and 4,096-byte blocks. Available blocks
were 21,857,413 before setup, 21,080,378 after workers and 21,856,431 after
cleanup. These are observations, not a claim of an otherwise idle host.

Sampled process-tree peak RSS ranges were 331,256–332,856 KiB for stock and
421,736–424,820 KiB for native. Node peak RSS ranges were 331,112–338,504 and
313,752–338,500 KiB respectively. The process-tree sample and Node peak use
different observation methods; do not subtract them to infer exact helper
memory. GC totals over measured workers were 73.493 ms stock and 181.552 ms
native. These observations are advisory and are not a memory qualification.

## Functional gates and their exact scope

The native job passed 425 Rust tests across 37 result groups (208 in the
storage library), with zero failures and zero ignored tests. This includes
the six inherited node-root cases. The borrowing lifetime/Send boundary
compiled under the real pinned toolchain. All 29 mapped typechecks and 49
component scripts passed, followed by production adapter acceptance.

The real normal conditional pre-commit hook passed in 2.937 seconds, reported
fmt and lint, and restored the exact clean candidate HEAD/index/tree. Knip was
not selected by this Rust/docs-only diff; this is not a claim of a new Knip
execution. Hook proof SHA-256:
`4121b94143baf7dcc6e0cd09dc27f35f40d0a9bfd7d7cca6be4d7d65cbf13003`.

The built original Linux web client completed setup, local project creation,
a controlled ACP approval/completed turn, client reload and same-state server
restart in a fresh client. All three snapshots have the same completed
run/attempt/provider-turn identities and statuses. Native read-only inspection
verified canonical omdb rows/event heads and no application SQLite files;
closed-database inspection reported unchanged state. Browser proof SHA-256:
`0ca83382a0c6e911c61515a7b0d67d54960878efae3c3f67f0fec9894779cfd3`.

The raw browser proof retains two connection-refused and three HTTP 502 entries
during the lifecycle exercise. The defined recovery flow passed; no
console-error-free, every-provider, Windows, macOS or mobile claim follows.

| Job                                                 | ID                                                                                                 | Result  |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------- | ------- |
| Select fork checks and porting work                 | [132527235534](https://github.com/glopyglerky/t3code-ls/actions/runs/43621254851/job/132527235534) | success |
| Affected Rust packages and source guards / qualify  | [132527471208](https://github.com/glopyglerky/t3code-ls/actions/runs/43621254851/job/132527471208) | success |
| Paired stock and native performance and reliability | [132532570006](https://github.com/glopyglerky/t3code-ls/actions/runs/43621254851/job/132532570006) | failure |
| Rust integration through the application            | [132532570012](https://github.com/glopyglerky/t3code-ls/actions/runs/43621254851/job/132532570012) | success |
| Affected production adapters                        | [132532570028](https://github.com/glopyglerky/t3code-ls/actions/runs/43621254851/job/132532570028) | success |
| Record verified qualification                       | [132533425691](https://github.com/glopyglerky/t3code-ls/actions/runs/43621254851/job/132533425691) | failure |

Final receipt validation rejected the failed planned performance job:
`Qualification receipt rejected: Job performance did not succeed or skip ('failure')`.
No qualified receipt or main promotion followed. The strict gate worked as
intended while functional success and unresolved performance remained distinct.
