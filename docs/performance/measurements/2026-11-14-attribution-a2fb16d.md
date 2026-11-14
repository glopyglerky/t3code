# Persistence request attribution at a2fb16d

**This diagnostic completed; it does not qualify performance or the application.**
Both stock/native correctness workers passed, followed by one native worker with
probes disabled and one with probes enabled. The observations support testing a
bounded history lookup reuse change: the helper repeatedly reads and processes
substantial metadata for a small returned history window. They do not identify
the unmeasured time as JSON parsing, establish a causal optimization target, or
promise a speedup. Every earlier fixed stock/native comparison remains failed.

The [raw evidence archive](2026-11-14-attribution-a2fb16d.raw.tar.gz) retains the
complete preparation history, exact instrumented patch, build, runtime
attestation, all four workers, both sets of client/helper profiles and derived
request aggregates. The observer is an isolated diagnostic patch, not production
code added to this repository. The [qualification method](../persistence-api.md)
and its eight-pair contract, workloads and thresholds remain unchanged.

## Exact scope and identities

| Identity                           | Value                                                              |
| ---------------------------------- | ------------------------------------------------------------------ |
| Measured base commit               | `a2fb16d84f089e2a7ca681e431e6acff083695bf`                         |
| Measured base tree                 | `9aca39040e56c362ed1edba9e69d49602b5feee8`                         |
| Pinned stock correctness oracle    | `5fe2f7e4567d9e530ded0dabc9d706db27d77b84`                         |
| Built observer patch SHA-256       | `fd598676c15bdeeb2c503b01a7f9fb48f161bb0d008d84119162234d180563d1` |
| Preserved optimized helper SHA-256 | `e29ae6419d7b6138a43eacdcf86803c410b2522600fbff20d3219e1bd42369b4` |
| Actual build receipt SHA-256       | `94b532e63376cd235165acab975e3ed5465f66a09d13f8b405fbe0a8cd65a7ea` |
| Original diagnostic report SHA-256 | `76849ba6bbecb3413995c6c1164142209c5cfd2a3ef6c182bfa1041c61123da9` |
| Derived attribution JSON SHA-256   | `1082856542464cd3406882baf48de81d35c5fa7fc4f458fad6010c5373c2fe15` |
| Raw archive SHA-256                | `43a0e91322efaefb659ee2593fd1d3a9253e669e23072a893ac927e29dd3e4f6` |

The report was prepared in a separate documentation worktree based on
`ee178c237d5fad5842f950871003dc898ab26990` (tree
`68ff57f62a644665e401c6d9a046b4c5129eca7a`). That later commit repairs an
Orchestrator adapter call. Its bytes did not enter this a2fb persistence run.
This diagnostic neither executes nor qualifies that call, the browser, providers,
other platforms, or the complete application. Existing main remains unchanged.

The build receipt records the actual pinned Lustr coordinator invocation,
produced tools and hashes, 161 source/build inputs, the full observer patch,
release profile, source stability and the preserved executable hash. The build
used a separate target/output directory, one CPU and one Cargo job, and completed
successfully in 281.907 seconds. The build log SHA-256 is
`b6d208210adabd3dead664ebc3acbcc1d3f6ce15203199deccdfb40978fbb145`.
The recorded build command was:

```sh
lustr --bluefe-root <pinned-host-tools> build --locked --release   -p t3-code-ls-storage --bin t3-code-ls-db
```

The archive preserves the actual absolute paths and environment. Compiler
provenance is explicitly `manifest-reported` diagnostic evidence, with observed
`compiler_identity: null` and the expected lock identity separate. This local
receipt is not a `verified-ci-manifest` qualification receipt. No instrumentation
or build replaced the production release helper.

## Preparation, failures and unchanged workload

The first observer draft was reviewed as source only and was never built or
run. Review found that the unchanged deliberate rollback test normally discards
its poisoned helper. Such a helper cannot truthfully provide a successful-close
profile. The corrected v2 patch therefore leaves observers entirely unconfigured
during both correctness/rollback/reopen workers. Measurement workers retain the
strict profile and closure requirements. No database lifecycle or failure
semantics changed to obtain a profile. Both complete drafts and their file hashes
are retained, including v1 patch SHA-256
`eb47af3803994cd86d018020f9ee6e826ea3b080f8e467b64587a3f07e5fa032`.

The first static runtime preflight also failed before any application worker ran:
24 inherited root test aliases resolved outside the frozen production virtual
store. The private root was normalized to its declared dependencies and required
benchmark links, removing 69 generated symlinks while preserving the known
optional Cursor platform package. Package files, per-workspace dependency links
and the active checkout were unchanged. The archive retains the failed preflight
and every alias change; it contains no speed result for that setup failure.

The second static preflight passed. It verified the complete frozen pnpm 12.14.0
graph, Node 26.12.1, per-importer external resolution and all workspace source
aliases in the isolated a2fb checkout. Its 440 retained dependency descriptors
include the optional root `@cursor/sdk-linux-x64`; the descriptor tuple SHA-256 is
`215febeb42c657ec13ce66c7da4ce621ee7f87a4eefcf0a4a521955a085e897f`.
The passed static report SHA-256 is
`fc2f6b28dce3b9c9bf619d5466cf3af32f4796200d898bb78719d84f565f8c91`.
The stock oracle was a complete immutable export with its own workspace aliases,
not a pinned top-level file importing live fork-relative source.

The private driver verifies six copied sections against the original runner and
worker before execution: worker supervision, fixture seeding, canonicalization,
fixture setup/preparation, selected services, and operations/correctness. Their
hashes, original source bytes and full private-file deltas are retained in
`source/`. Changes are confined to observer hooks, metadata output and the
separately named diagnostic process sequence.

The fixture still contains 256 normal threads, one archived write thread, 3,000
search messages and a completed 1,500-item turn, with 1,024-byte reasoning
payloads. Returned witnesses were 224 active threads, three retained history
items, one history run, one history node, 13,976 history JSON bytes, and 20 results
from each search. Both backends passed exact data/order equality, same-transaction
readback, the exact deliberate rollback sentinel, restored state, and committed
reopen. Every cold, warmup and measured result matched the validated result,
including each changing committed visit timestamp.

Stock SQLite retained WAL/NORMAL and native omdb retained Immediate durability.
Fixture seeding, file copying, migration, preparation and hashing stayed outside
the timed API calls. The SQLite fixture's logical file size was 7,168,000 bytes;
the native template's was 272,896,000 bytes. These are file lengths, not allocated
disk bytes, memory consumption or I/O traffic.

## All probes-off/on distributions

Both workers use the same instrumented optimized executable and private worker.
The probes-disabled worker ran first; the probes-enabled worker ran second.
Each retained three excluded warmup cycles and twelve measured cycles for all
five APIs. There is only one process per condition and no randomized balanced
stock/native pairing. These are descriptive probe-overhead observations, not a
causal overhead estimate or an uninstrumented production speed comparison.

All times below are milliseconds. The cold row has one observation per worker;
its identical median, p95 and worst are not three independent observations or a
tail estimate. Percentiles use linear interpolation between sorted samples.

| Workload                          | Off median | Off p95 | Off worst | On median |  On p95 | On worst |
| --------------------------------- | ---------: | ------: | --------: | --------: | ------: | -------: |
| service-open-and-first-operations |    715.774 | 715.774 |   715.774 |   712.328 | 712.328 |  712.328 |
| sidebar-shell                     |    139.031 | 175.969 |   201.361 |   139.049 | 170.510 |  183.978 |
| history-window                    |    126.904 | 153.935 |   172.427 |   123.738 | 188.932 |  237.322 |
| message-search                    |     45.596 |  51.923 |    54.864 |    44.448 |  55.958 |   57.247 |
| accent-search                     |     40.024 |  43.812 |    44.157 |    37.468 |  44.112 |   45.582 |
| visit-commit                      |      5.630 |   6.343 |     6.909 |     6.003 |   7.460 |    8.137 |

The following signed changes are `(on / off - 1) × 100`, computed separately
for each displayed statistic. Positive means the observed enabled-probe latency
was longer. Negative values do not establish a speedup: process order, shared
host scheduling, cache state and finite samples are uncontrolled here.

| Workload                          | Median change | p95 change | Worst change |
| --------------------------------- | ------------: | ---------: | -----------: |
| service-open-and-first-operations |        -0.48% |     -0.48% |       -0.48% |
| sidebar-shell                     |        +0.01% |     -3.10% |       -8.63% |
| history-window                    |        -2.50% |    +22.74% |      +37.64% |
| message-search                    |        -2.52% |     +7.77% |       +4.34% |
| accent-search                     |        -6.39% |     +0.69% |       +3.23% |
| visit-commit                      |        +6.62% |    +17.61% |      +17.77% |

History's enabled-probe median was lower, but its p95 was 22.74% higher and
its worst sample 37.64% higher. Visit median, p95 and worst were all higher with
probes enabled. These observations remain in the record. They do not justify
subtracting a fixed observer cost, dropping tails, or adjusting qualification
thresholds. The earlier hosted visit tail is a different attempt and cannot be
explained by these twelve diagnostic samples.

### Cold service acquisition and first calls

The cold total is service acquisition plus the first call to each API. Module
loading and result fingerprinting are excluded. A fresh worker does not imply
a cold OS page cache; no cache flush was performed.

| Component           | Probes off ms | Probes on ms |
| ------------------- | ------------: | -----------: |
| Service acquisition |       253.939 |      257.623 |
| sidebar-shell       |       190.944 |      179.617 |
| history-window      |       164.985 |      170.197 |
| message-search      |        53.903 |       52.487 |
| accent-search       |        43.356 |       41.437 |
| visit-commit        |         8.647 |       10.966 |

## Helper and client attribution

These are means over the twelve measured API calls with probes enabled.
`get` and `scan` name application-wrapper calls. They do not count physical
storage operations: `get` can include a writer's cached observation, and the
named snapshot entry point does not cover every engine snapshot. Returned bytes
count repeated values repeatedly and are not memory use or device traffic.

Times are inclusive wall intervals. API, client wait, helper request, evaluate,
open, `get_many`/`get`, and staged/actual commit spans overlap. They must not be
added as independent costs or subtracted together to label a residual JSON,
CPU, engine or pure IPC time.

| API            | API mean ms | Helper evaluate ms | Requests/API | Gets/API | Get ms/API | Scans/API | Scan ms/API |
| -------------- | ----------: | -----------------: | -----------: | -------: | ---------: | --------: | ----------: |
| sidebar-shell  |     144.280 |            128.562 |            7 |    6,351 |     10.062 |     2,265 |       4.786 |
| history-window |     135.347 |            131.017 |            5 |    6,059 |      8.372 |        61 |       1.794 |
| message-search |      47.944 |             42.851 |            7 |    4,199 |      5.300 |     6,398 |       7.091 |
| accent-search  |      39.400 |             34.890 |            7 |    2,830 |      4.586 |     2,717 |       2.963 |
| visit-commit   |       6.197 |              3.134 |            7 |       79 |      0.122 |         0 |       0.000 |

| API            | Get returned bytes/API | Scan rows/API | Scan returned bytes/API | Client encode ms/API | Client wait ms/API | Client decode ms/API |
| -------------- | ---------------------: | ------------: | ----------------------: | -------------------: | -----------------: | -------------------: |
| sidebar-shell  |              6,375,065 |         5,888 |               3,085,456 |                0.144 |            132.933 |                2.901 |
| history-window |              6,353,951 |         4,538 |               2,372,915 |                0.090 |            133.029 |                0.717 |
| message-search |              1,388,663 |         3,555 |               1,003,021 |                0.105 |             45.364 |                0.676 |
| accent-search  |              1,564,106 |         2,094 |                 739,881 |                0.103 |             36.654 |                0.657 |
| visit-commit   |                 24,733 |             0 |                       0 |                0.093 |              4.518 |                0.499 |

Client wait includes request writing, helper execution, transport, scheduling
and UTF-8 framing. Client decode includes response-envelope and result
validation. The six source-equivalence witnesses establish that API timers
still wrap the original selected production calls; marker hooks are outside
those original timer boundaries. Probe work within the helper and client is
still part of the observed API duration.

### History: why bounded node-root reuse is the next experiment

The dominant history request is `application_projections_records`, which invokes
the retained-window path. In the first measured history sample it accounted for
117.510 ms, 6,055 gets and 60 scans. The separate
`application_projection_window_query` request took 0.060 ms and one empty scan.
All five history protocol requests remain available individually in the archive.
Optimizing only that latter query would target little of this observed work.

The immutable [history source at a2fb](https://github.com/glopyglerky/t3code-ls/blob/a2fb16d84f089e2a7ca681e431e6acff083695bf/apps/server/src/persistence/redb_projection_window.rs)
reads thread item metadata, related run item metadata, and node metadata while
deciding retained rows. This fixture has 1,500 items sharing one run and one node.
Source inspection therefore suggests a bounded, per-query reuse of already
validated node-root lookups. The observed repeated wrapper reads and substantial
helper evaluation time justify testing that hypothesis. The probes contain no
keys or parser attribution, so they do not measure the share attributable to
that node or prove that parsing causes the remaining duration.

Any experiment must preserve all three consumers of run-wide metadata:
same-thread interrupt-result handling, global first user-message/notification
anchors, and final interrupt-request inclusion with its different thread scope.
It must also preserve ordering, malformed/missing metadata errors, snapshot
visibility and writer observation budgets. No range scan should disappear
merely because a repeated key is reusable. The decision is to test one bounded
lookup reuse change separately, require correctness first, and then run the
unchanged fixed comparison. A gain remains unproven.

### Search, startup and committed writes

Message search averaged 6,398 scans returning 3,555 rows; accent search averaged
2,717 scans returning 2,094 rows. In the first measured message-search API, the
three `application_thread_search_read` requests respectively made 0, 6,326 and
72 scans. The corresponding accent-search counts were 0, 2,635 and 82. This
concentration makes repeated search seeks a narrow source-review target.
The static operation labels deliberately do not record query payloads, and these
counts alone do not prove that any seek is redundant. Search ordering, candidate
limits, stale-document handling, corruption errors and transaction observations
must be traced before proposing reuse.

Enabled-probe service acquisition took 257.623 ms. Its 45 protocol requests
totaled 24.587 ms of helper request wall time and 23.743 ms of evaluate time.
Separately, the writable helper's store-open span was 165.524 ms, containing
backend-open at 165.352 ms and checked-open at 165.031 ms. Those three nested
open intervals describe the same startup region and must not be added together.
The read-only helper's store-open span was 1.265 ms. Startup preparation requests
and outside-request open rows are retained separately from measured APIs.

Visit commits averaged 6.197 ms API time. Actual commit averaged 1.866 ms inside
the 1.998 ms staged-commit span. Each API issued seven protocol requests and
recorded fifteen mutation observations for eight distinct staged writes.
Immediate durability and transaction acknowledgement remain intact. This
diagnostic does not establish power-loss behavior or explain earlier hosted
write tails.

### Complete phase totals

The table below includes startup, every first call, all excluded warmups, all
measured cycles and shutdown outside an API marker. Counts are totals within
each phase, not per-call means. The archive's `analysis/attribution.json` retains
all 574 joined client/helper requests and 81 API markers behind these 17 groups.

| Phase/API             | Requests | Helper request ms | Evaluate ms |   Gets |  Scans |
| --------------------- | -------: | ----------------: | ----------: | -----: | -----: |
| open:service-open     |       45 |            24.587 |      23.743 |    552 |      2 |
| cold:sidebar-shell    |        7 |           154.511 |     152.848 |  6,351 |  2,265 |
| cold:history-window   |        5 |           164.463 |     164.321 |  6,059 |     61 |
| cold:message-search   |        7 |            41.702 |      41.424 |  4,199 |  6,398 |
| cold:accent-search    |        7 |            33.775 |      33.493 |  2,830 |  2,717 |
| cold:visit-commit     |        7 |             2.688 |       2.580 |     79 |      0 |
| warmup:sidebar-shell  |       21 |           356.041 |     350.941 | 19,053 |  6,795 |
| warmup:history-window |       15 |           419.558 |     419.029 | 18,177 |    183 |
| warmup:message-search |       21 |           120.963 |     120.009 | 12,597 | 19,194 |
| warmup:accent-search  |       21 |           100.377 |      99.634 |  8,490 |  8,151 |
| warmup:visit-commit   |       21 |             9.928 |       9.374 |    237 |      0 |
| sample:sidebar-shell  |       84 |          1569.685 |    1542.745 | 76,212 | 27,180 |
| sample:history-window |       60 |          1574.318 |    1572.207 | 72,708 |    732 |
| sample:message-search |       84 |           517.786 |     514.218 | 50,388 | 76,776 |
| sample:accent-search  |       84 |           422.330 |     418.679 | 33,960 | 32,604 |
| sample:visit-commit   |       84 |            39.214 |      37.612 |    948 |      0 |
| outside-api           |        1 |             2.923 |       2.900 |      0 |      0 |

## Observer integrity and host limits

Observers record only allowed static operation labels, ordinals, counts, lengths
and elapsed times. There are no added database keys, user identifiers or payload
values in probe files. Full validation outputs contain only the unchanged
controlled synthetic fixture. No real provider or personal state is involved.

The native observer has fixed request/byte caps, checked aggregate arithmetic,
late/unfinished-span detection and a proven backend-close witness. It writes
private files only after successful closure, with no new helper stdout/stderr.
The client observer has fixed connection/request/marker caps. The driver required
matching request ordinals, operation kinds, frame lengths, complete success,
zero active spans and both expected helper files; overflow or incomplete closure
would reject the diagnostic. The normal owner-lock process retains its existing
lifecycle and is explicitly excluded. Both measured workers passed these checks.

The entire diagnostic ran from 16:19:51 to 16:20:40 UTC on 2026-11-14, taking
49.697 seconds. The host reported Linux 6.18.57, an Intel Xeon Platinum 8573C,
nine available CPUs and Node 26.12.1. Owned heavy work was serialized for the
measurement window. Shared-host load and scheduling still limit inference.
Load averages changed from 0.24/0.50/0.56 to 1.02/0.68/0.62; these observations
do not establish their cause.

The fixture volume was overlayfs (`0x794c7630`) with 4,096-byte blocks. It had
9,016,959 available blocks before setup and 9,015,067 after cleanup. Volume-wide
counters are not per-fixture usage or physical-device evidence. The owned fixture
directory was empty after cleanup, and source/helper identities were rechecked.

Both measurement workers observed twelve GC events: 25.579 ms total with probes
off and 25.046 ms with probes on, across the whole worker. No forced GC or outlier
removal was used. Node-only peak RSS was 313,440 KiB off and 303,232 KiB on.
Whole-process-tree RSS was unavailable because descendant enumeration returned
ENOENT; it is null, not zero, and the Node value does not substitute for it.
The supervisor's 665,292 KiB child maximum has a separate process-maximum scope
and is not a sum of simultaneous application memory.

## Retained evidence and decision

The archive is 425,716 compressed bytes with 55 regular-file entries and
6,548,317 uncompressed bytes. `SHA256SUMS.json` binds every other entry. It contains
no database, executable, build target, dependency tree, credential or user state.
Its structure is:

| Directory   | Retained evidence                                                                                                                                                                |
| ----------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `source/`   | Both source-review drafts, full patches and private runner/worker deltas, allowed labels, original runner/worker bytes, exact source hashes and six unchanged-section witnesses. |
| `build/`    | Actual coordinator command, supervisor source, build receipt with source/tool hashes and complete build log.                                                                     |
| `runtime/`  | Dependency-copy/source-alias receipt, both static preflights, exact private alias normalization and verified frozen dependency descriptors.                                      |
| `run/`      | Original report and Markdown, all four configs/results/stdout/stderr logs, all off/on observer files, supervisor outcome and cleanup facts.                                      |
| `analysis/` | The deterministic aggregation source and full per-request, marker, phase, startup and off/on derived data.                                                                       |

The completed status means this bounded diagnostic and its correctness checks
finished. It provides no stock latency verdict, confidence interval or application
qualification. The chosen next experiment is bounded history lookup reuse,
followed by correctness checks and the unchanged paired suite. Search seek reuse
remains a source-review proposal. Neither decision permits a threshold change,
omission of a slow workload, or promotion of the current candidate.
