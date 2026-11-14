# Search seek reuse experiment, 2026-11-14

**Status: implemented and reviewed; hosted validation is pending.** This is an
isolated, unqualified candidate on branch
`ci/t3code-ls-search-seek-reuse-20261114`. It does not establish a speed gain or
authorize main promotion. Existing main remains unchanged.

The candidate starts from `ee178c237d5fad5842f950871003dc898ab26990`, tree
`68ff57f62a644665e401c6d9a046b4c5129eca7a`, and keeps the upstream oracle at
`5fe2f7e4567d9e530ded0dabc9d706db27d77b84`. It is separate from the later f751
integration and the history node-read experiment. This fixed base permits a
scoped search experiment without combining those production changes.

## Why this change

The [a2fb request profile](../measurements/2026-11-14-attribution-a2fb16d.md)
recorded 6,326 scans in each measured message candidate request and 2,635 in each
accent candidate request. Source inspection found that the phrase-alignment loop
can repeat a fully validated seek against the same immutable snapshot. These
counts justify testing reuse; they do not identify parsing cost or establish how
much elapsed time a change will save. The later thread-opens request remains a
separate substantial cost.

The [hosted 5fe comparison](../measurements/2026-11-14-hosted-5fe.md) passed its
functional gates and failed all eleven performance rows. This experiment does
not replace that result. Its baseline, raw samples and strict thresholds remain
unchanged.

## Production scope

Only `apps/server/src/persistence/redb_thread_search.rs` changes production
behavior. A query-local vector retains scalar results from completed term seeks,
up to 256 entries and 8,192 logical state bytes. Larger queries use the existing
algorithm without a new query-size error. There are no cached documents,
postings, positions or per-source cursors.

Reuse is enabled only when `session.read_snapshot_validation.is_some()`, which
identifies a real read-only root snapshot. A nested read can inherit a writer,
so a read-shaped operation alone does not enable reuse. Writers keep the original
seek path and its range observations, budgets and savepoint behavior.

A cached hit is valid only when `found <= new maximum <= searched maximum`.
Cached exhaustion is valid only at or below its searched maximum. Reuse still
calls `ensure_active`. Every miss calls the unchanged legacy, direct and bucket
source checks in their original order, including non-winning-source validation.
Only a completely successful seek installs a proof. Position extraction,
intersection, signed rowid termination, candidate/hit limits and ranking remain
unchanged. All state is dropped when that one query ends.

The [retained proposal](2026-11-14-search-seek-reuse-proposal.md) explains the
interval proof, exact predicate, bounds and failure cases. Five new real-database
tests cover mixed sources and signed/phrase positions; interval bounds and old
versus fresh snapshots; corruption in a non-winning source; oversized-query
fallback; and public writer-search observation cost with savepoint visibility.

## Evidence and pending gates

| Check                                    | Current evidence                                                                                               |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------- |
| Independent source review                | Two reviews found no concrete production blocker.                                                              |
| Formatting and patch hygiene             | Pinned packaged rustfmt and `git diff --check` passed.                                                         |
| Unchanged source validators              | Legacy/direct/bucket seek, combined seek, positions and intersection functions are byte-identical to the base. |
| New and existing Rust tests              | Pending hosted execution; no local search build or test ran.                                                   |
| Optimized helper provenance              | Pending exact-source release build through the pinned Lustr/BlueFe route and bound manifest.                   |
| Native/stock differential and adapters   | Pending mapped acceptance, including the existing full thread-search fixture.                                  |
| Normal commit hook                       | Pending actual hook execution on the exact hosted candidate tree.                                              |
| Controlled application/recovery flow     | Pending the complete Linux application gate.                                                                   |
| Fixed performance/reliability comparison | Pending all eight balanced pairs, unchanged warmups/samples, every row and strict 1.0 comparison.              |

A preceding, separate local lib-test compilation was killed before test
execution. Local resources have not been established as sufficient for another
such build, so this candidate goes to the mandatory hosted gates without a
similar local retry. That event is not a search test failure or a test pass.

The [source-review archive](2026-11-14-search-seek-reuse.source-review.tar.gz)
retains the original proposal, complete implementation/test patch and source
review manifest. It contains five regular entries, 10,632 compressed bytes and
no executable, database or runtime dependencies.

| Evidence                       | SHA-256                                                            |
| ------------------------------ | ------------------------------------------------------------------ |
| Original reviewed proposal     | `635b2513ea5866e1baf030ca8e8a607acebb92c089691386fbd872a34a926b87` |
| Complete reviewed source patch | `3b7a0643ca727311c19ca145e95dcdda4a151d83f32d3fe638b9354bf788063f` |
| Changed Rust source            | `e7fe8bbd88a0e27278ad37cbbc63c8fac4a34de5bb79dc25bfb7ffec16da7ee9` |
| Source-review archive          | `85633fe38af7fa0dc22da687f1cc69269c07f16c96fd3ae9be10eb0dda0b989d` |

## Decision rule

Retain every hosted failure, inconclusive result and timing sample. Correctness
and durability errors block the experiment immediately. A gain must be measured
on the exact source, runtime, helper and stock oracle; no parent-marker residual
will be labeled parser or IPC savings. All eleven fixed rows still gate
separately, and any failed or inconclusive row prevents qualification. Functional
success alone cannot promote this candidate or justify weakening a threshold.
