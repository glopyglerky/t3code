# Upstream integration f751c848

This candidate integrates the fixed upstream revision
[`f751c8488f7ca1e659e44ef12b05861c1bcf9bfb`](https://github.com/pingdotgg/t3code/commit/f751c8488f7ca1e659e44ef12b05861c1bcf9bfb),
tree `a8157c768c81bdb0564612e2adca54dde1411a22`. The
[complete upstream comparison](https://github.com/pingdotgg/t3code/compare/5fe2f7e4567d9e530ded0dabc9d706db27d77b84...f751c8488f7ca1e659e44ef12b05861c1bcf9bfb)
contains 27 commits and 105 changed paths after the
[5fe2f7e4 integration](default-5fe2f7e4.md). Later upstream movement is a separate
detection and integration decision.

The fork parent is the initial-message repair
[`ee178c237d5fad5842f950871003dc898ab26990`](https://github.com/glopyglerky/t3code-ls/commit/ee178c237d5fad5842f950871003dc898ab26990),
tree `68ff57f62a644665e401c6d9a046b4c5129eca7a`. That repair routes an initial
thread message through the selected guest-share decider. This merge keeps that
call and the native transaction owner. It does not modify the earlier a2fb16d
profiling checkout, its upstream reference, the performance contract or recorded
measurements.

## Native boundaries and retained behavior

Only three changed upstream files already contained fork adaptations:
`ProjectionStore.ts`, `CodexAdapterV2.ts` and `Orchestrator.ts`. The other 102
changed source and asset paths retain their exact upstream blobs. The following
decisions preserve the existing port boundaries.

| Boundary                     | Integration and reason                                                                                                                                                                                                                                                                                                     |
| ---------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Agent awareness              | Add `snoozedUntil`, `snoozedAt` and `pendingGuestMessageCount` to the shared awareness type and mapper. Native and SQLite row services both use that mapper; native thread rows already carry the complete stored payload. A second Rust implementation of this presentation rule or a new database column is unnecessary. |
| Snoozed completion           | Retain the upstream relay rule that evaluates snooze at completion time, including a completion processed after wake time. The new awareness fields must arrive together with that rule; waiting guest messages still wake a snoozed thread.                                                                               |
| Codex retry deadline         | Retain the shared provider-core retry budget and compute each delay from the first retry's start time. Repeated notices cannot extend that deadline. The adapter still selects the Rust process transport before execution; retry policy remains in the shared orchestration adapter.                                      |
| Settled usage-limit recovery | Retain the specific refusal to arm recovery on a settled thread and clear armed auto-resume/snooze flags when settling. This is shared orchestration state handling; the native command-policy interface does not gain a new rule in this tail.                                                                            |
| Rendered thread find         | Keep the exact shared Markdown parser update for Arabic comma, semicolon, question mark and full stop. Native find consumes this retained rendered-text normalization. The change does not define a new Rust lexical-search tokenization rule.                                                                             |
| Message search               | The mapped `messageSearchIndex.ts` change is a comment only. Refresh its source identity; no index rebuild or query change follows from that comment.                                                                                                                                                                      |

The projection merge conflict arose because upstream's awareness mapper remains
inside its SQLite layer while the fork has extracted shared presentation from
database row access. The resolution adds the exact three type fields and three
mapping expressions to the existing shared assembly. It keeps the native read
services and avoids restoring the duplicated SQLite implementation block.

Primary changes: [awareness and snooze](https://github.com/pingdotgg/t3code/commit/4954322f02f7b569bd0369caa4e1552393635a1c),
[provider retry deadlines](https://github.com/pingdotgg/t3code/commit/66a40c7ca8223b2126a2a4baf94e7374169c0703),
[settled recovery](https://github.com/pingdotgg/t3code/commit/cc578d5f9a3899e6aa41e12140e3e623de832b9c),
and [Arabic punctuation](https://github.com/pingdotgg/t3code/commit/e3305931412d82f8cf09474b23c7e5409ee360b7).

## Retained application changes

The full upstream tail is retained, including Claude retry exhaustion, ACP
failure classification and bounded subagent-text batching with terminal flushes,
Cursor transient-log filtering, deleted Git-upstream publication, ident-keyword
review handling, Windows home-relative paths, normalized folder matching,
browser title observation, desktop controller ownership and screencast pacing.
Web and mobile changes retain notification, terminal lifecycle, queued-edit
cleanup, project ordering, document preview and review behavior at their
upstream paths. These implementations are required by the working clients and
have not been replaced or retired by this native persistence integration.

The [Ghostty update](https://github.com/pingdotgg/t3code/commit/3908c4eab3ac33eb990e3b961738fbbb3951e0ff)
advances the retained vendor revision from
`9f62873bf195e4d8a762d768a1405a5f2f7b1697` to
`3ff6d08fad56f2d6b81420a27bdc56ee707b453e`. Its headers, version, Android build
pin, notices and five binary artifacts move together. The imported Git blobs are:

| Artifact            | Git blob                                   |
| ------------------- | ------------------------------------------ |
| Web WASM            | `80b269e8699a1474a460a626623af93f2ff669b2` |
| Android arm64-v8a   | `9e3ed70cf88adb0c21aa84defde455c9620de070` |
| Android armeabi-v7a | `9174c0be80f445a6c27cf07d3e1f4a2d47edc4ab` |
| Android x86         | `db7667ea3559ba6c650125ef04fd1ed6ea099f80` |
| Android x86_64      | `0a1f011f8c1339530011e87acd03b030b49cb1e4` |

Exact source retention does not establish Android, browser or desktop native
acceptance. There are no application migrations, dependency manifest changes or
lockfile changes in this cutoff. The canonical application schema remains 91;
the count ownership fence and source-preserving upgrade behavior documented for
5fe2f7e4 remain required.

## Verification and evidence limits

`upstream.lock.json` records the integrated source and inventory as unqualified.
`port-map.json` appends the revision to the complete source history, refreshes
the four changed mapped blobs and advances all ten port review revisions. It
preserves existing platform states and acceptance requirements. The event-store
differential's explicit commit guard advances to the new pin; its oracle blob
is unchanged. No qualification receipt is inherited merely because that service
has identical source.

The shell differential now exercises nonempty snooze timestamps, absent-value
defaults and a mixture of pending, sent and dismissed guest messages through
the native API and immutable upstream service. Both global and requested-thread
awareness must retain the exact new fields. Rendered-find differential cases
exercise Arabic punctuation around bare and emphasized URLs, including the
absence of Markdown emphasis delimiters in displayed text. The parser's exact
immutable-byte guard remains in place.

Required affected checks include the projection shell/count, rendered-find,
event-store and provider adapter gates, plus production TypeScript checking and
the existing replay case for a second Codex retry notice five minutes into the
same deadline. The repaired initial-message launch cases remain relevant to the
shared orchestrator. Source-retention checks, the actual normal commit hook,
selected native application flow and the strict stock/native performance and
reliability comparison remain required for the final exact source. Unrelated
pristine upstream suites are not separately rerun as fork qualification.

### Completed local diagnostics

Formatting, lint of the three adapted production files and changed helpers,
source retention and the immutable README guard passed. The focused runtime
checks below ran sequentially with one-CPU affinity against implementation tree
`b9d1886f4a80ba1cf158a4e35d309b6e539d3075`. Subsequent changes add this evidence
record and its archive; final hosted qualification must still test its exact
published tree.

| Check                                                       | Result                   |  Elapsed |
| ----------------------------------------------------------- | ------------------------ | -------: |
| Native projection shells/counts, including awareness fields | 13 check groups passed   | 10.335 s |
| Native rendered thread find, including both Arabic cases    | 7 check groups passed    |  9.137 s |
| Native event store against the f751 pin                     | 12 check groups passed   | 10.792 s |
| Native provider adapter                                     | 29 scenarios passed      |  6.180 s |
| Repeated Codex retry notice at the original deadline        | 1 selected replay passed | 13.959 s |
| Initial-message launch, rejection and lost-result recovery  | 3 selected cases passed  |  7.325 s |

The Codex test uses its existing in-process replay and fake clock; the real
provider process route is exercised separately by the adapter gate. The launch
tests retain their existing SQLite harness and explicitly select the real
native guest-share decider, which was invoked 13 times. They do not establish
full native-database application acceptance. The 171 unselected Codex tests and
58 unselected launch tests were skipped, not counted as passes.

The database helper is the preserved Lustr-built debug executable, SHA256
`17b743ce8bca3b9dc0bd9accc0055b843017c60f06faf44b9bbba44bab25e6a1`.
All 148 recorded Rust/Cargo/toolchain inputs and all four embedded schemas match
its attested build inputs. The earlier broad manifest also included an older
upstream lock; that entire manifest is not claimed unchanged. The f751 oracle
and adapter inputs are recorded separately. Provider, guest-share and source
guard helper hashes are retained, with their compilation routes explicitly
unverified by these focused invocations. Final hosted builds retain the pinned
Lustr/BlueFe requirement.

Two unsuccessful attempts remain in the record. Production type checking ended
with SIGKILL after 19.080 seconds, without a compiler diagnostic or a wrapper
timeout. Its cause is not established, and it is not a typecheck pass. The first
provider-gate invocation failed because the external runner forced upstream
selection while that gate checks the normal native default. Removing that
unsolicited runner setting allowed the unchanged production source to pass the
complete adapter gate. Neither result is hidden or replaced by its successor.

The [raw receipts, proofs and logs](evidence/2026-11-14-f751-verification.raw.tar.gz)
contain 31 entries. The archive SHA256 is
`f27f79ffe9860ee2277966443167d01b75ecf6f2d41b348c28630896b9ae0513`;
the aggregate receipt SHA256 is
`7a14ca8bc5641e28e5d0026615fe28a7ab465f08c6f5900aa49fd52ae3797396`.
Source and helper hashes remained unchanged during every recorded check.

A successful production typecheck, the actual normal commit hook, full
exact-source native/application qualification and the strict stock/native
comparison remain required. The earlier count/fence, initial-message and
performance results retain their original commits, helpers and upstream
references. This integration does not claim a speed improvement or change a
failing threshold. Main remains unpromoted; further history or query
optimization requires measured evidence and a separate review. No additional
original source is retired here.
