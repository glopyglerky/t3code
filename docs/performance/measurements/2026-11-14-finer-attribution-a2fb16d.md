# Finer persistence attribution at a2fb16d, v3

**The diagnostic completed. It establishes no stock-performance qualification
or causal optimization gain.** Both stock/native correctness workers passed,
followed by one native probes-disabled worker and one probes-enabled worker.
The named spans locate substantial observed time in compact metadata decoding
and the existing digest finalization/hex expression. Copies and sorting are
smaller measured regions in this fixture. These findings justify bounded source
experiments; they do not predict the gain those experiments will achieve.

The [complete raw archive](2026-11-14-finer-attribution-a2fb16d.raw.tar.gz)
retains full v2/v3 patches, the incremental diff, source/build identities,
both v3 static preflights, all four raw workers, every helper/client profile,
failed derived-analysis draft and complete validated aggregates. The
[previous v2 diagnostic](2026-11-14-attribution-a2fb16d.md) remains unchanged.
The fixed eight-pair stock/native qualification workload and thresholds also
remain unchanged. Existing main has not advanced on this evidence.

## Observed regions and next hypotheses

The table uses the probes-enabled worker's twelve complete measured API calls.
Every duration is inclusive wall time; the columns overlap and must not be added.
`canonical_digest` contains `digest_hex`, key construction contains digests, and
selection/assembly contain reads, decoding, copies and sorting. `digest_hex`
includes SHA-256 finalization and the unchanged hex rendering expression.

| API            | API mean ms | Helper evaluate ms | Compact decode ms | Key build ms | Digest ms | Nested hex ms | Digest calls/API |
| -------------- | ----------: | -----------------: | ----------------: | -----------: | --------: | ------------: | ---------------: |
| sidebar-shell  |     179.706 |            148.263 |            33.714 |       42.660 |    35.788 |        30.601 |           20,587 |
| history-window |     137.419 |            133.814 |            42.027 |       34.451 |    25.807 |        22.034 |           15,169 |
| message-search |      57.976 |             46.478 |             3.964 |        8.940 |     5.969 |         5.113 |            3,472 |
| accent-search  |      47.785 |             38.640 |             5.070 |       11.379 |     7.934 |         6.900 |            3,966 |
| visit-commit   |       7.536 |              2.871 |             0.000 |        0.132 |     0.085 |         0.074 |               34 |

The source and measurements support this priority order:

1. **Test an exact-output hex encoder independently.** The observed hex span
   averages 30.601 ms/sidebar API and 22.034 ms/history API. Source inspection
   shows the existing expression calls `format!("{byte:02x}")` for each of 32
   digest bytes, then collects the strings. At the observed 20,587 and 15,169
   digest calls per API, that implies 658,784 and 485,408 executions of the
   formatting expression. This is a source-derived execution count, not an
   observed allocation count. A replacement must retain every output byte and
   existing key/digest compatibility. This diagnostic contains no such change.
2. **Investigate bounded immutable-session metadata reuse.** Compact decoding
   averages 33.714 ms/sidebar and 42.027 ms/history, with 5,890 and 6,042 decode
   calls and 6,020,160 and 6,325,407 input bytes per API. Repeated bytes may
   describe the same rows, but the observer records no keys, so row identity
   and safe reuse require a separate source audit. All witness scans, ordering,
   discovery/corruption checks, snapshot semantics and writer observations
   must be preserved. No broad cache or disk-format change is justified here.
3. **Keep key construction visible without double-counting its children.**
   Its 42.660 ms/sidebar and 34.451 ms/history spans include digest work and
   other key assembly. Subtracting overlapping aggregate timers does not
   isolate a causal canonical-JSON, allocator or CPU cost.
4. **Defer broader clone/sort refactoring.** History has 1,510 observed row-clone
   calls averaging 3.163 ms/API, 22 instrumented sorts averaging 0.178 ms,
   and 3,032 selected payload parses averaging 1.055 ms. The source sites are
   real costs, but their measured scope is smaller than compact decoding and
   the nested digest expression. Payload parsing is distinct from the compact
   metadata decoder; neither counter measures every parse in the application.

The history eligibility span averages 25.777 ms and selection 15.103 ms; sidebar
thread assembly averages 99.766 ms across 224 rows. Those parent spans contain
the child work above. They do not expose a new disjoint cost of those sizes.
The earlier search observation remains relevant: message search executes 6,398
instrumented scans/API, but this diagnostic has no seek-reuse production patch.

Observed facts are named calls, lengths and wall intervals. The source-derived
explanations above identify candidates, not proven causes or guaranteed savings.
Raw-reader wrapper time, helper evaluation and client wait must not be combined
to label an unmeasured residual as JSON, physical I/O, CPU or pure IPC.

## Exact scope and preparation

The fixed source is `a2fb16d84f089e2a7ca681e431e6acff083695bf`, tree
`9aca39040e56c362ed1edba9e69d49602b5feee8`, against stock correctness oracle
`5fe2f7e4567d9e530ded0dabc9d706db27d77b84`. The v3 patch changes only private
observation boundaries and diagnostic metadata. All original algorithms,
source queries, validation/error order, transaction/close semantics and
durability remain unchanged. The hex expression was deliberately preserved.

The driver checks six byte-identical sections against the original runner and
worker before executing: supervision, seed, canonicalization, fixture setup,
selected services and operations/correctness. Their hashes and original source
are retained. All fifteen observer source hashes and the complete v2-to-v3
diff were independently reviewed before a fresh release build.

The build used the pinned produced BlueFe compiler through the actual Lustr
coordinator, with one CPU/job, unchanged optimized settings, a fresh private
target/output and no compiler flag override. It completed in 260.338 seconds.
All 161 source/build inputs and six observed tool hashes stayed unchanged.
The v2 helper, target, source snapshot and result remain preserved. This receipt
is `manifest-reported` local diagnostic evidence, not a `verified-ci-manifest`
qualification. Observed compiler identity stays null; expected policy identity
is recorded separately.

The first v3 static preflight failed before any application worker ran. Its
complete 440-descriptor frozen dependency graph passed, but an added historical
alias loop expected a root workspace link removed during the earlier approved
clean-root normalization. The failed script/receipt is retained. The corrected
script hash-binds both historical receipts, requires all 69 removed root links
absent using `lstat` (including dangling links), and verifies the remaining 170
workspace aliases against exact private-source targets. The second preflight
passed without any package, alias, source or workload mutation. Both temporary
stock exports were removed.

Frozen pnpm 12.14.0, Node 26.12.1, per-workspace resolution, the installed lock
and all 440 external descriptors passed with no issues. The optional root Cursor
platform package remains included. The full stock export has its own workspace
aliases; it does not import live fork-relative source. The dependency-resolution
tuple hash is unchanged from v2.

## Workload and correctness

The fixture retained 256 normal threads, one archived write thread, 3,000 search
messages, a completed 1,500-item history run and 1,024-byte reasoning payloads.
Setup, export, migration, index preparation and file hashing stay outside the
API timers. Both correctness workers leave observers unconfigured, preserving
the deliberate rollback/discard lifecycle. The measurement workers still require
strict successful-close profiles from every non-owner helper connection.

Data and array order, same-transaction readback, the exact deliberate rollback
sentinel, restored state and committed reopen passed. Every cold, warmup and
measured result matched its validated result, including changing visit timestamps.
Nonempty witnesses were 224 active threads, three retained history items, one run,
one node, 13,976 history JSON bytes, and 20 results from each search.

Stock SQLite retained WAL/NORMAL and native omdb retained Immediate durability.
Logical fixture lengths were 7,168,000 SQLite bytes and 272,896,000 native bytes;
those are not allocated disk bytes, memory usage or storage traffic. Rollback
and reopen checks establish no power-loss, other-platform or real-provider proof.

## Every probes-off/on observation

The same instrumented release helper ran with probes disabled first and enabled
second, one process per condition, three excluded warmups and twelve measured
cycles. **V3 explicitly used CPU affinity `[0]`; v2 used nine available CPUs.**
Cross-version timing differences cannot isolate observer or algorithm effects.
Even within v3, fixed order, one process and host variation prevent a causal
overhead estimate. The disabled helper still contains the compiled probes.

Times are milliseconds. Cold has one observation per condition; its identical
median, p95 and worst are not independent samples or a tail estimate. Every
observed positive and negative change is retained below, without a significance
or speedup claim.

| Workload                          | Off median |   Off p95 | Off worst | On median |    On p95 |  On worst |
| --------------------------------- | ---------: | --------: | --------: | --------: | --------: | --------: |
| service-open-and-first-operations |  1,052.021 | 1,052.021 | 1,052.021 | 1,013.593 | 1,013.593 | 1,013.593 |
| sidebar-shell                     |    148.998 |   177.625 |   191.107 |   153.969 |   301.370 |   355.419 |
| history-window                    |    127.428 |   216.028 |   296.369 |   134.309 |   161.022 |   162.505 |
| message-search                    |     48.739 |    99.042 |   137.442 |    48.229 |   101.911 |   162.159 |
| accent-search                     |     39.793 |    47.191 |    48.301 |    42.379 |    71.655 |    77.367 |
| visit-commit                      |      6.729 |    13.287 |    15.744 |     7.036 |    10.969 |    12.321 |

| Workload                          | Median change | p95 change | Worst change |
| --------------------------------- | ------------: | ---------: | -----------: |
| service-open-and-first-operations |        -3.65% |     -3.65% |       -3.65% |
| sidebar-shell                     |        +3.34% |    +69.67% |      +85.98% |
| history-window                    |        +5.40% |    -25.46% |      -45.17% |
| message-search                    |        -1.04% |     +2.90% |      +17.98% |
| accent-search                     |        +6.50% |    +51.84% |      +60.18% |
| visit-commit                      |        +4.56% |    -17.44% |      -21.74% |

The mixed tail directions are material: enabled sidebar worst is 355.419 ms,
while disabled history worst is 296.369 ms and enabled message-search worst is
162.159 ms. Neither condition is uniformly faster. No tail was censored or
retried. These single-worker observations cannot be used to calibrate away a
qualification regression or select only favorable probe costs.

## Cold/open attribution

The enabled service-open marker lasted 295.578 ms. The primary helper's outside-
request store-open span was 168.445 ms, containing backend-open 168.184 ms and
checked-open 168.057 ms. These nested spans are not three independent durations.
The separate read-only startup helper's store-open span was 0.452 ms. Their
complete process lifetimes include waiting and must not be treated as open time.

The enabled cold phases are retained below. Each is a single observation.
Module loading and fixture work remain outside the reported cold API sum;
neither OS page cache nor storage device state is claimed cold.

| Phase/API           | Marker ms | Helper requests | Helper wall ms | Client total ms |
| ------------------- | --------: | --------------: | -------------: | --------------: |
| open:service-open   |   295.578 |              45 |         35.992 |         249.370 |
| cold:sidebar-shell  |   367.726 |               7 |        303.572 |         330.526 |
| cold:history-window |   218.155 |               5 |        211.188 |         213.149 |
| cold:message-search |    63.273 |               7 |         47.664 |          51.511 |
| cold:accent-search  |    58.292 |               7 |         38.128 |          48.349 |
| cold:visit-commit   |    10.657 |               7 |          3.482 |           7.176 |

## All 31 metric scopes, every measured workload

Each row aggregates all helper requests belonging to one complete API marker,
then summarizes its twelve measured observations. Time columns are inclusive
milliseconds. Calls and byte columns are means per API. All count/unit/byte
raw values, sums, means, medians, p95 and worst values are retained in
`analysis/all-scopes.json`; every request and phase remains in `analysis/attribution.json`.

`—` means that a unit/byte field is unobserved, with a null distribution in the
derived data. It does not mean zero bytes or allocations. A row with zero calls
means that this named probe did not execute in that workload; it does not measure
uninstrumented paths elsewhere. Counter bytes are argument/return lengths at
the indicated boundary, not total copying, heap size, disk traffic or allocations.

### sidebar-shell

| Metric                          | Calls/API | Mean ms | Median ms |  p95 ms | Worst ms | Input bytes/API | Output bytes/API |
| ------------------------------- | --------: | ------: | --------: | ------: | -------: | --------------: | ---------------: |
| `backend_open`                  |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `backend_open_read_only`        |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `backend_open_exclusive`        |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `checked_open`                  |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `begin_snapshot`                |         1 |   0.055 |     0.043 |   0.107 |    0.125 |               — |                — |
| `begin_reserved`                |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `get`                           |     6,351 |  11.198 |     9.435 |  18.496 |   24.851 |       1,491,167 |        6,375,065 |
| `get_many`                      |         1 |   0.015 |     0.012 |   0.029 |    0.047 |           3,422 |              396 |
| `scan`                          |     2,265 |   4.851 |     4.502 |   6.561 |    7.649 |       1,084,500 |        3,085,456 |
| `observe_mutation`              |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               0 |                — |
| `commit_staged`                 |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               0 |                — |
| `commit_actual`                 |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `store_open`                    |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `decode_frame`                  |         7 |   0.270 |     0.233 |   0.483 |    0.621 |               — |                — |
| `evaluate`                      |         7 | 148.263 |   132.580 | 228.698 |  238.508 |               — |                — |
| `encode_frame`                  |         7 |   9.796 |     9.073 |  15.544 |   16.587 |               — |                — |
| `flush_frame`                   |         7 |   0.001 |     0.001 |   0.001 |    0.001 |               — |                — |
| `index_witness_decode`          |     5,887 |   4.916 |     4.776 |   5.713 |    6.196 |       1,297,133 |                — |
| `compact_metadata_decode`       |     5,890 |  33.714 |    30.736 |  45.622 |   54.118 |       6,020,160 |                — |
| `canonical_row_decode_validate` |       228 |   3.617 |     3.397 |   4.218 |    4.469 |         343,506 |                — |
| `lookup_key_build`              |    12,233 |  42.660 |    41.571 |  49.477 |   52.389 |               — |        3,287,918 |
| `canonical_digest`              |    20,587 |  35.788 |    35.099 |  39.697 |   39.731 |               — |        1,317,568 |
| `digest_hex`                    |    20,587 |  30.601 |    30.123 |  33.294 |   33.385 |         658,784 |        1,317,568 |
| `payload_json_parse`            |     5,857 |   4.180 |     3.952 |   5.519 |    5.829 |       1,386,092 |                — |
| `owned_text_copy`               |     6,597 |   0.541 |     0.473 |   0.888 |    0.935 |       1,555,709 |        1,555,709 |
| `indexed_row_clone`             |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `sort_key_build`                |       224 |   0.008 |     0.008 |   0.008 |    0.009 |               — |                — |
| `sort_compare`                  |       675 |   0.367 |     0.329 |   0.533 |    0.728 |               — |                — |
| `window_eligibility`            |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `window_selection`              |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `shell_thread_assembly`         |       224 |  99.766 |    90.444 | 147.478 |  178.078 |               — |                — |

### history-window

| Metric                          | Calls/API | Mean ms | Median ms |  p95 ms | Worst ms | Input bytes/API | Output bytes/API |
| ------------------------------- | --------: | ------: | --------: | ------: | -------: | --------------: | ---------------: |
| `backend_open`                  |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `backend_open_read_only`        |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `backend_open_exclusive`        |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `checked_open`                  |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `begin_snapshot`                |         1 |   0.181 |     0.061 |   0.700 |    0.877 |               — |                — |
| `begin_reserved`                |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `get`                           |     6,059 |   7.410 |     7.273 |   8.241 |    8.487 |       1,427,913 |        6,353,951 |
| `get_many`                      |         1 |   0.017 |     0.013 |   0.035 |    0.058 |           3,422 |              396 |
| `scan`                          |        61 |   1.500 |     1.421 |   1.906 |    1.975 |          31,635 |        2,372,915 |
| `observe_mutation`              |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               0 |                — |
| `commit_staged`                 |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               0 |                — |
| `commit_actual`                 |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `store_open`                    |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |
| `decode_frame`                  |         5 |   0.063 |     0.052 |   0.112 |    0.137 |               — |                — |
| `evaluate`                      |         5 | 133.814 |   131.057 | 155.439 |  158.450 |               — |                — |
| `encode_frame`                  |         5 |   2.692 |     3.180 |   4.614 |    5.159 |               — |                — |
| `flush_frame`                   |         5 |   0.001 |     0.001 |   0.001 |    0.001 |               — |                — |
| `index_witness_decode`          |     4,538 |   3.686 |     3.692 |   3.906 |    3.960 |         993,342 |                — |
| `compact_metadata_decode`       |     6,042 |  42.027 |    39.282 |  57.497 |   61.508 |       6,325,407 |                — |
| `canonical_row_decode_validate` |        11 |   0.234 |     0.214 |   0.298 |    0.317 |          17,374 |                — |
| `lookup_key_build`              |    10,602 |  34.451 |    34.385 |  35.367 |   35.795 |               — |        2,809,453 |
| `canonical_digest`              |    15,169 |  25.807 |    25.705 |  26.552 |   26.837 |               — |          970,816 |
| `digest_hex`                    |    15,169 |  22.034 |    21.911 |  22.662 |   22.998 |         485,408 |          970,816 |
| `payload_json_parse`            |     3,032 |   1.055 |     1.022 |   1.215 |    1.279 |         524,525 |                — |
| `owned_text_copy`               |    34,636 |   1.797 |     1.673 |   2.400 |    3.157 |         976,915 |          976,915 |
| `indexed_row_clone`             |     1,510 |   3.163 |     3.184 |   3.407 |    3.531 |               — |                — |
| `sort_key_build`                |        19 |   1.116 |     1.087 |   1.300 |    1.323 |               — |                — |
| `sort_compare`                  |        22 |   0.178 |     0.160 |   0.261 |    0.265 |               — |                — |
| `window_eligibility`            |         1 |  25.777 |    25.485 |  30.821 |   31.325 |               — |                — |
| `window_selection`              |         1 |  15.103 |    14.828 |  17.927 |   18.350 |               — |                — |
| `shell_thread_assembly`         |         0 |   0.000 |     0.000 |   0.000 |    0.000 |               — |                — |

### message-search

| Metric                          | Calls/API | Mean ms | Median ms | p95 ms | Worst ms | Input bytes/API | Output bytes/API |
| ------------------------------- | --------: | ------: | --------: | -----: | -------: | --------------: | ---------------: |
| `backend_open`                  |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `backend_open_read_only`        |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `backend_open_exclusive`        |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `checked_open`                  |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `begin_snapshot`                |         2 |   0.780 |     0.095 |  3.814 |    8.048 |               — |                — |
| `begin_reserved`                |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `get`                           |     4,199 |   6.511 |     4.976 | 13.318 |   22.716 |         730,706 |        1,388,663 |
| `get_many`                      |         2 |   0.038 |     0.025 |  0.099 |    0.136 |           6,844 |              792 |
| `scan`                          |     6,398 |   7.434 |     6.598 | 11.223 |   15.193 |       1,854,472 |        1,003,021 |
| `observe_mutation`              |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               0 |                — |
| `commit_staged`                 |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               0 |                — |
| `commit_actual`                 |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `store_open`                    |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `decode_frame`                  |         7 |   0.184 |     0.092 |  0.591 |    1.192 |               — |                — |
| `evaluate`                      |         7 |  46.478 |    42.397 | 65.957 |   92.512 |               — |                — |
| `encode_frame`                  |         7 |   3.417 |     3.223 |  6.570 |    7.076 |               — |                — |
| `flush_frame`                   |         7 |   0.001 |     0.001 |  0.001 |    0.001 |               — |                — |
| `index_witness_decode`          |       846 |   0.666 |     0.642 |  0.738 |    0.757 |         188,502 |                — |
| `compact_metadata_decode`       |       846 |   3.964 |     3.993 |  4.293 |    4.342 |         789,747 |                — |
| `canonical_row_decode_validate` |       390 |   5.188 |     4.816 |  6.676 |    8.152 |         427,484 |                — |
| `lookup_key_build`              |     2,472 |   8.940 |     8.822 | 10.157 |   11.374 |               — |          639,176 |
| `canonical_digest`              |     3,472 |   5.969 |     5.935 |  6.673 |    7.219 |               — |          222,208 |
| `digest_hex`                    |     3,472 |   5.113 |     5.046 |  5.751 |    6.134 |         111,104 |          222,208 |
| `payload_json_parse`            |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               0 |                — |
| `owned_text_copy`               |     3,144 |   0.165 |     0.154 |  0.214 |    0.248 |         193,381 |          193,381 |
| `indexed_row_clone`             |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `sort_key_build`                |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `sort_compare`                  |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `window_eligibility`            |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `window_selection`              |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `shell_thread_assembly`         |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |

### accent-search

| Metric                          | Calls/API | Mean ms | Median ms | p95 ms | Worst ms | Input bytes/API | Output bytes/API |
| ------------------------------- | --------: | ------: | --------: | -----: | -------: | --------------: | ---------------: |
| `backend_open`                  |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `backend_open_read_only`        |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `backend_open_exclusive`        |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `checked_open`                  |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `begin_snapshot`                |         2 |   0.099 |     0.086 |  0.142 |    0.161 |               — |                — |
| `begin_reserved`                |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `get`                           |     2,830 |   4.703 |     4.444 |  6.200 |    7.436 |         543,722 |        1,564,106 |
| `get_many`                      |         2 |   0.025 |     0.025 |  0.029 |    0.031 |           6,844 |              792 |
| `scan`                          |     2,717 |   3.002 |     2.924 |  3.349 |    3.484 |         797,317 |          739,881 |
| `observe_mutation`              |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               0 |                — |
| `commit_staged`                 |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               0 |                — |
| `commit_actual`                 |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `store_open`                    |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `decode_frame`                  |         7 |   0.185 |     0.090 |  0.616 |    1.064 |               — |                — |
| `evaluate`                      |         7 |  38.640 |    36.150 | 51.402 |   57.431 |               — |                — |
| `encode_frame`                  |         7 |   3.955 |     3.862 |  7.087 |    7.281 |               — |                — |
| `flush_frame`                   |         7 |   0.004 |     0.001 |  0.019 |    0.041 |               — |                — |
| `index_witness_decode`          |       966 |   0.792 |     0.740 |  1.015 |    1.135 |         215,235 |                — |
| `compact_metadata_decode`       |       966 |   5.070 |     4.397 |  8.403 |   11.548 |         901,702 |                — |
| `canonical_row_decode_validate` |       446 |   5.778 |     5.667 |  6.514 |    6.975 |         488,382 |                — |
| `lookup_key_build`              |     2,824 |  11.379 |     9.941 | 17.511 |   22.430 |               — |          730,184 |
| `canonical_digest`              |     3,966 |   7.934 |     6.884 | 13.354 |   19.080 |               — |          253,824 |
| `digest_hex`                    |     3,966 |   6.900 |     5.869 | 12.132 |   18.055 |         126,912 |          253,824 |
| `payload_json_parse`            |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               0 |                — |
| `owned_text_copy`               |     3,590 |   0.203 |     0.182 |  0.285 |    0.320 |         220,202 |          220,202 |
| `indexed_row_clone`             |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `sort_key_build`                |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `sort_compare`                  |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `window_eligibility`            |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `window_selection`              |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `shell_thread_assembly`         |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |

### visit-commit

| Metric                          | Calls/API | Mean ms | Median ms | p95 ms | Worst ms | Input bytes/API | Output bytes/API |
| ------------------------------- | --------: | ------: | --------: | -----: | -------: | --------------: | ---------------: |
| `backend_open`                  |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `backend_open_read_only`        |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `backend_open_exclusive`        |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `checked_open`                  |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `begin_snapshot`                |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `begin_reserved`                |         1 |   0.091 |     0.059 |  0.237 |    0.321 |               — |                — |
| `get`                           |        79 |   0.180 |     0.170 |  0.235 |    0.240 |          15,957 |           24,733 |
| `get_many`                      |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               0 |                0 |
| `scan`                          |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               0 |                0 |
| `observe_mutation`              |        15 |   0.002 |     0.002 |  0.003 |    0.003 |           3,943 |                — |
| `commit_staged`                 |         1 |   1.634 |     1.661 |  2.053 |    2.147 |           5,968 |                — |
| `commit_actual`                 |         1 |   1.510 |     1.549 |  1.938 |    2.032 |               — |                — |
| `store_open`                    |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `decode_frame`                  |         7 |   0.085 |     0.074 |  0.133 |    0.155 |               — |                — |
| `evaluate`                      |         7 |   2.871 |     2.886 |  3.246 |    3.398 |               — |                — |
| `encode_frame`                  |         7 |   1.149 |     0.975 |  2.971 |    3.017 |               — |                — |
| `flush_frame`                   |         7 |   0.001 |     0.001 |  0.001 |    0.001 |               — |                — |
| `index_witness_decode`          |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               0 |                — |
| `compact_metadata_decode`       |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               0 |                — |
| `canonical_row_decode_validate` |         6 |   0.146 |     0.136 |  0.188 |    0.201 |           9,636 |                — |
| `lookup_key_build`              |        21 |   0.132 |     0.105 |  0.257 |    0.408 |               — |            5,407 |
| `canonical_digest`              |        34 |   0.085 |     0.060 |  0.198 |    0.364 |               — |            2,176 |
| `digest_hex`                    |        34 |   0.074 |     0.048 |  0.186 |    0.352 |           1,088 |            2,176 |
| `payload_json_parse`            |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               0 |                — |
| `owned_text_copy`               |        34 |   0.003 |     0.003 |  0.003 |    0.003 |          10,916 |           10,916 |
| `indexed_row_clone`             |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `sort_key_build`                |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `sort_compare`                  |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `window_eligibility`            |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `window_selection`              |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |
| `shell_thread_assembly`         |         0 |   0.000 |     0.000 |  0.000 |    0.000 |               — |                — |

### Static unit and byte definitions

| Metric                          | Input / output units                  | Input / output byte scope                          |
| ------------------------------- | ------------------------------------- | -------------------------------------------------- |
| `backend_open`                  | unobserved / unobserved               | unobserved / unobserved                            |
| `backend_open_read_only`        | unobserved / unobserved               | unobserved / unobserved                            |
| `backend_open_exclusive`        | unobserved / unobserved               | unobserved / unobserved                            |
| `checked_open`                  | unobserved / unobserved               | unobserved / unobserved                            |
| `begin_snapshot`                | unobserved / unobserved               | unobserved / unobserved                            |
| `begin_reserved`                | unobserved / unobserved               | unobserved / unobserved                            |
| `get`                           | keys / present_values                 | key_lengths / value_lengths                        |
| `get_many`                      | keys / value_slots                    | key_lengths / present_value_lengths                |
| `scan`                          | ranges / rows                         | range_boundary_lengths / row_key_and_value_lengths |
| `observe_mutation`              | keys / unobserved                     | key_lengths / unobserved                           |
| `commit_staged`                 | writes / unobserved                   | write_key_and_value_lengths / unobserved           |
| `commit_actual`                 | unobserved / unobserved               | unobserved / unobserved                            |
| `store_open`                    | unobserved / unobserved               | unobserved / unobserved                            |
| `decode_frame`                  | unobserved / unobserved               | unobserved / unobserved                            |
| `evaluate`                      | unobserved / unobserved               | unobserved / unobserved                            |
| `encode_frame`                  | unobserved / unobserved               | unobserved / unobserved                            |
| `flush_frame`                   | unobserved / unobserved               | unobserved / unobserved                            |
| `index_witness_decode`          | encoded_witnesses / decoded_witnesses | encoded_witness_lengths / unobserved               |
| `compact_metadata_decode`       | encoded_metadata / decoded_metadata   | encoded_metadata_lengths / unobserved              |
| `canonical_row_decode_validate` | encoded_rows / validated_rows         | encoded_row_lengths / unobserved                   |
| `lookup_key_build`              | locations / keys                      | unobserved / encoded_key_lengths                   |
| `canonical_digest`              | values / digests                      | unobserved / utf8_hex_string_lengths               |
| `digest_hex`                    | digests / hex_strings                 | digest_byte_lengths / utf8_hex_string_lengths      |
| `payload_json_parse`            | json_strings / parsed_values          | utf8_json_lengths / unobserved                     |
| `owned_text_copy`               | strings / strings                     | utf8_string_lengths / utf8_string_lengths          |
| `indexed_row_clone`             | row_fields / row_fields               | unobserved / unobserved                            |
| `sort_key_build`                | rows / keyed_or_validated_rows        | unobserved / unobserved                            |
| `sort_compare`                  | rows / rows                           | unobserved / unobserved                            |
| `window_eligibility`            | items / eligible_items                | unobserved / unobserved                            |
| `window_selection`              | eligible_items / retained_items       | unobserved / unobserved                            |
| `shell_thread_assembly`         | threads / rows                        | unobserved / unobserved                            |

`get` includes cached writer observations; `get_many` counts returned slots,
including missing values, while its bytes count only present values. `begin_snapshot`
covers its named wrapper, not every engine snapshot. `sort_compare` covers a
whole instrumented sort, including its comparator/key closure; it is not a
per-comparison counter. `canonical_row_decode_validate` includes row validation
and key checking; it is broader than serde parsing alone. The digest input-byte
field is unobserved; only the nested hex span uses the fixed 32-byte digest length.

## Raw verification, limits and retained failures

Independent reconstruction matched all four worker JSONs to the report, six
observer files to their hashes, 574 client/helper requests by ordinal, static
operation kind, input frame length and success, all 81 finished markers and
17 phase groups. All 31 scope definitions, exact counter fields, safe integer
bounds, digest/hex call equality and 32/64-byte hex input/output relationships
were checked. Every emitted helper proved actual store closure, zero active
spans and no incompleteness. Disabled profiles retain null request/marker data
rather than invented measurements. The owner-lock connection intentionally has
no helper profile, as recorded in the unchanged protocol.

The first all-scope metadata analyzer incorrectly tried to take the length of
disabled `markers: null`; it failed before producing derived data. Its source
and failure note remain in the archive. The corrected analyzer explicitly
requires null for disabled markers and validates the enabled marker list. This
did not alter any raw evidence, measured source, workload or application run.

All observer labels are static. Probes retain no keys, payloads, identifiers,
headers, network responses or user state. Helper output is limited to 2,048
request rows and 8 MiB including its newline; client output retains the existing
32-connection, 2,048-request-per-connection and 256-marker limits. Checked counter
overflow or late/unclosed work makes a profile incomplete. Observer output is
private and emitted after proven successful closure, with transport unchanged.
At maximum row count, 31 logical counter arrays occupy 3,047,424 bytes before
container overhead; that calculation is not measured RSS. High-frequency timers
add clock reads, atomics and aggregation; their overhead is not assumed small.

The owned supervisor completed in 60.059 seconds with exit 0, unchanged source
and helper, empty fixture directory and no remaining compiler/helper process.
It retained bounded logs, timeout and owned-process cleanup. There was exactly
one v3 application diagnostic; no result-driven rerun occurred.

## Host, filesystem and advisory memory

The host reported Intel Xeon Platinum 8573C, Linux 6.18.57, one available CPU
under the explicit affinity and 10,451,423,232 memory bytes. Load averages changed
from `[1.30, 1.25, 1.26]` to `[1.99, 1.46, 1.33]`. Supervisor preflight found
no active compiler/helper process and 4,432,856 KiB available memory. These are
context observations, not an attribution of tails to memory or scheduling.

Fixtures used the task-owned overlayfs directory, not `/tmp` tmpfs. The observer
reported type `0x794c7630` and 4,096-byte block/fragment sizes. Volume-wide block
counts below are neither fixture allocation nor a physical-device or durability
guarantee. They were sampled outside timed operations.

| Observation   | Available blocks | Bytes at reported block size |
| ------------- | ---------------: | ---------------------------: |
| before_setup  |        7,893,933 |               32,333,549,568 |
| after_workers |        7,530,408 |               30,844,551,168 |
| after_cleanup |        7,892,828 |               32,329,023,488 |

| Probes | Sampled process-tree peak KiB | Node-only peak KiB | GC count | GC duration ms |
| ------ | ----------------------------: | -----------------: | -------: | -------------: |
| off    |                   Unavailable |             305556 |       12 |         66.164 |
| on     |                   Unavailable |             313228 |       12 |        111.682 |

Process-tree RSS is unavailable in both workers: the sampler recorded
`Process or descendant enumeration unavailable (ENOENT)`, with zero valid samples
in 398 probes-off and 407 probes-on attempts. The retained peak is null, never
zero memory or a Node-only substitute. No process-tree memory comparison is made.
Node-only high-water data remains separately measured and excludes helper children.

When available, the 25 ms process-tree sampler can miss short peaks and double-count
shared pages. No forced collection
was used. RSS is advisory, not heap size, allocation accounting, whole-app memory
qualification or memory safety. Raw GC/RSS fields remain unchanged in each worker.

## Exact evidence identity

| Evidence                          | Identity                                                           |
| --------------------------------- | ------------------------------------------------------------------ |
| Base commit                       | `a2fb16d84f089e2a7ca681e431e6acff083695bf`                         |
| Base tree                         | `9aca39040e56c362ed1edba9e69d49602b5feee8`                         |
| Stock oracle                      | `5fe2f7e4567d9e530ded0dabc9d706db27d77b84`                         |
| Full v3 patch SHA-256             | `b26cf8c7e0ac9bbecc6e2f4cd0cb9704597f42034d64ce1f2707b791c5918898` |
| Incremental v2-to-v3 SHA-256      | `76ecc39012c89068d26b169d4a1f74b5de37a8b3292452719295e93a6c3b91d1` |
| Source-review manifest SHA-256    | `0129a0b06c9458557c85ddd66cdf079fdf933202b2dd2d74da5cb84c6a91511b` |
| Helper SHA-256                    | `9d985f08e3c883428031810a8ec8d315e66347c9b8d849ee443130cfa4fab446` |
| Build receipt SHA-256             | `1eb5d3da00e634f8bd579cca9d0d4a11f66c56561af7cfeca92b57b2054187f7` |
| Build log SHA-256                 | `9a40cba56c81c1afd5e2f57e0f589cd80f3bdfe8a38cdfd1ea85a1317dede821` |
| Failed static preflight01 SHA-256 | `c53dfef484e8fd6200f439a6ec3fab48a4cd5ee18f6a937e4f53e348a878f3e4` |
| Passed static preflight02 SHA-256 | `b6a06608f5d4697ea8bbec2f91289c755005dacaed4f278be993997fb6e756d8` |
| External resolution SHA-256       | `215febeb42c657ec13ce66c7da4ce621ee7f87a4eefcf0a4a521955a085e897f` |
| Original report SHA-256           | `fb27430b6aa656419ba4c6cde25ce3831779762923c41a6b6f83fc8bd76a334e` |
| Supervisor SHA-256                | `17adb32445f74cad7d94bafa4528f73d501d6220926362513354a03d938b78a5` |
| Per-request attribution SHA-256   | `6c7bfff0d78f9f38dc119706d698ea5e5918b58c6814bdd5fd15858549019486` |
| All-scope attribution SHA-256     | `26db314889905658c35a4378348ff4dec4f14b23516d279a6fab63b2130b1cfc` |
| Raw archive SHA-256               | `a4d106990b551219901f8cbab1c5b36a0da491ace123ad3904ce7b0a8a73a569` |
| Private worker SHA-256            | `2bf3d8e038f049a89fdc77f85cd90f0d2c18f4db5719e33c3113a9e7c6715e5b` |
| Toolchain lock SHA-256            | `2cddf6970e38ab122d6d62e4543cfb3d1fd23876d4a813a4b4421babc033d1bf` |

The exact invocation was:

```text
/workspace/scratch/fcf5fdf8376a/t3-runtime/node_modules/node/bin/node /workspace/scratch/fcf5fdf8376a/t3-persistence-profile-a2fb16d/tools/performance-profile.mjs --database /workspace/scratch/fcf5fdf8376a/t3-profile-helper-a2fb16d-v3-build-01/t3-code-ls-db --output /workspace/scratch/fcf5fdf8376a/t3-persistence-profile-v3-01 --build-receipt /workspace/scratch/fcf5fdf8376a/t3-profile-helper-a2fb16d-v3-build-01/build-receipt.json
```

The readable report was prepared in a separate documentation worktree based
on `4b5719f34a8b76a23f3c77982940cea67234186c`. That integration source and its
later sidebar corrections did not enter this fixed a2fb diagnostic. The archive
binds the actual measured bytes and actual helper, not the documentation parent.

## Decision

Retain the complete diagnostic and failures. Test the exact-output digest encoder
as a separate bounded change, then audit successful metadata reuse without
weakening witness/error checks or writer observations. The evidence supports
that order because the named decode/digest regions are much larger than the
observed selected clone/sort regions. No production change, promised gain or
threshold adjustment follows automatically from these timings. Required source,
native, adapter, full controlled app and unchanged stock-performance gates still
decide whether a later candidate can advance.
