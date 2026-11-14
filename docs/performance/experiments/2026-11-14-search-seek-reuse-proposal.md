> Retained source proposal, reviewed before implementation. This readable copy
> is formatted for the repository; its original bytes and hash remain in the
> [source-review archive](2026-11-14-search-seek-reuse.source-review.tar.gz).
> See the [experiment status](2026-11-14-search-seek-reuse.md) for current evidence.

# Bounded search seek reuse proposal

Status: source review only. No production edit, build, test or new timing run is
part of this proposal. Keep the current history experiment separate. If approved,
implement this as one isolated change after verifying the same functions against
the final candidate and its pinned upstream oracle.

## Measured target

The completed a2fb attribution report is
`76849ba6bbecb3413995c6c1164142209c5cfd2a3ef6c182bfa1041c61123da9`;
the full request aggregation is
`1082856542464cd3406882baf48de81d35c5fa7fc4f458fad6010c5373c2fe15`.
Both are retained in the reviewed archive `43a0e91322efaefb659ee2593fd1d3a9253e669e23072a893ac927e29dd3e4f6`.

The following are means across all twelve measured enabled-probe calls. The
request counts are identical in every measured cycle. Static probes identify
the three search-read requests by order, not by logging their query payloads.
Their purpose follows from the source sequence in `ThreadSearch.makeWithRows`
and `NativeThreadSearch.makeRows`.

| API / request                  | Helper request wall ms | Evaluate ms |  Gets | Scans | Scan rows |
| ------------------------------ | ---------------------: | ----------: | ----: | ----: | --------: |
| Message search: `has_index`    |                  0.371 |       0.349 |     3 |     0 |         0 |
| Message search: `message_hits` |                 25.476 |      25.358 | 3,274 | 6,326 |     2,709 |
| Message search: `thread_opens` |                 16.249 |      16.172 |   920 |    72 |       846 |
| Accent search: `has_index`     |                  0.362 |       0.337 |     3 |     0 |         0 |
| Accent search: `message_hits`  |                 15.591 |      15.467 | 1,775 | 2,635 |     1,128 |
| Accent search: `thread_opens`  |                 18.329 |      18.261 | 1,050 |    82 |       966 |

Message search's full API mean was 47.944 ms, with 6,398 scans across seven
protocol requests. Accent search averaged 39.400 ms, with 2,717 scans. This
proposal targets only the candidate request. In particular, it cannot remove
the substantial later `thread_opens` cost. Neither scan counts nor the gap
between wrapper and evaluate timings identifies a parser or physical I/O cost;
the spans are inclusive and overlapping.

## Existing behavior and the repeated work

In `redb_thread_search.rs`, `matching_documents` derives a sorted set of distinct
trigram terms from the quoted-phrase grammar. It decreases an inclusive maximum
rowid until every term agrees, validates positional phrase matches, and collects
at most 10,000 candidates in descending rowid order. Candidate membership,
including orphan postings, precedes message/thread eligibility filtering and the
2,000-hit limit. Nonindexed searches use their separate 20,000-rowid-distance
path and are outside this proposal.

Each `next_document(term, maximum)` fully evaluates **legacy, then direct, then
bucket** sources, and returns their largest valid rowid at or below `maximum`.
Those functions perform their existing key, canonical rowid, document-membership,
suppression/survivor and position checks before returning. A non-winning source
may still expose corruption; it must not be skipped merely because an earlier
source already found the current maximum.

When one term lowers the maximum, the outer alignment loop repeats. Terms whose
previous successful seek already covers that smaller interval are queried again
against the same immutable snapshot. The fixture's query shapes explain why
alignment is a plausible source of repeated calls: `benchmark`/`needle` has
eleven distinct trigrams, and `ecole`/`fer` has four. The benchmark fixture has
82 needle-bearing messages and 94 accent-bearing messages. These are properties
of the fixed seed, not per-key observations from the profiler. The profiling
counts are consistent with repeated seeks but do not establish a timing gain
from avoiding them.

## Proposed change

Keep `next_document`, all three source functions and `positions` unchanged.
Inside one `matching_documents` call, retain at most one scalar seek proof for
each term in its existing sorted iteration order. The proof has an unknown state
or a fully successful result containing:

- The maximum passed to the completed seek.
- Its optional found rowid.

For a found rowid, reuse only when:

`found <= new_maximum <= previously_searched_maximum`.

For a successful `None`, reuse only when:

`new_maximum <= previously_searched_maximum`.

The current aggregate-`None` path ends the whole matching query immediately, so
caching that result has no repeated-use benefit today. Its bound must still be
correct if represented. Do not introduce separate per-source cursors or reuse
an exhausted result above the maximum it proved.

On a miss, call the original `next_document` in its existing position in the
term loop. It still checks legacy, direct and bucket in the same order. Install
a proof only after the entire call returns successfully. Never cache an error
or a partial source result. Keep the original aligned flag, maximum changes,
phrase-position reads/intersections, result append, limits and `checked_sub`
termination unchanged. In particular, a term whose cached hit is greater than
the new maximum must seek again; reuse of only the last rowid would be wrong.

### Exact immutable-transaction predicate

Enable reuse only when `session.read_snapshot_validation.is_some()` at the
`message_hits` call site. The current `ApplicationTransactions::begin_mode`
constructs that field only for `ApplicationTransactionMode::Read`, which creates
`Backend::begin_read_only`. That transaction holds an immutable engine snapshot,
and `require_write` rejects mutation. Pass the resulting predicate to this
private query helper; the cache itself remains local to one call and is dropped
on every completion, error or cancellation. Do not place it in the session's
readiness flags or any global/store state.

Do not infer immutability from a read-shaped protocol operation or from
`!Transaction::owns_writer()`. The latter would also include an unreserved
optimistic writer. `RedbClient.withReadTransaction` can inherit a parent's writer
and its staged overlay, so a nested read is not sufficient proof of a read-only
root. All writer sessions take the existing uncached path, preserving their
range observations, conflict detection, savepoint behavior and observation
budget failures exactly.

### Entry and byte bound

Use a vector of scalar proofs indexed alongside the already existing sorted
term set; do not clone term strings, posting rows, documents or position lists.
Allow at most **256 entries** and at most **8,192 logical scalar-state bytes**,
checked using `term_count.checked_mul(size_of::<SeekProof>())`. If either limit
would be exceeded, execute the original uncached algorithm for the whole query.
The byte bound excludes the one vector header and allocator rounding; it is not
a claim about whole-process RSS. A proposed state enum with a maximum and
optional result fits this bound on the qualified target, but the runtime size
check keeps the rule explicit on other targets.

This bound limits optimization state only. It does not reject a previously valid
large search, change accepted query syntax or raise an application/resource
limit. It is unrelated to the fixed benchmark's timing thresholds or workload.

## Why the proof holds, and where it stops

For one immutable snapshot, a successful seek establishes the greatest eligible
rowid at or below its searched maximum across all three sources. If the new
maximum remains between that rowid and the searched maximum, no newly eligible
row can appear inside the already validated interval. A `None` result proves
exhaustion only at or below its searched maximum. The bound checks express
these facts directly; signed comparisons require no rowid arithmetic.

The proof does not justify skipping first-time source validation, using a result
above its bound, retaining it after a query, or using it on a mutable overlay.
No source error is converted into an empty result. Position extraction still
runs at its existing point for every aligned candidate, preserving stale FTS,
delete-survivor and phrase-offset semantics. No parser or query-language
normalization changes are proposed.

This narrow first step also leaves document reuse, per-source exhausted cursors,
scan batching, source ordering, early branch exits, thread-opens optimization,
candidate/hit limits and ranking unchanged. They would need separate evidence
and semantic review rather than being bundled into this experiment.

## Required correctness evidence before measurement

Use real database transactions and the production search boundary for the
following risks; retain the existing immutable upstream differential fixture.

1. Mixed legacy/direct/bucket postings, gaps and signed rowids must return the
   same descending candidates and positional matches with reuse enabled and the
   original path. Include a lower maximum below a cached hit and a higher
   maximum than an earlier proof to exercise both miss conditions.
2. A malformed direct or bucket record must still fail when an earlier legacy
   source already found a winning rowid. A failed first seek must never install
   a reusable proof. Keep canonical key/rowid, membership and position checks.
3. An old read snapshot must retain its result across another writer's commit,
   while a newly opened snapshot sees the new postings. There is no cache shared
   between calls or snapshots.
4. Search invoked under an existing writer must preserve staged-write readback,
   rollback/savepoint behavior and the original repeated range-observation
   budget. The writer path must remain uncached even through the TS nested-read
   API.
5. Queries at and above the 256-entry/8,192-byte optimization bounds must preserve
   results and error behavior, with larger queries falling back. Existing raw
   query syntax, Unicode/GLOB, stale/orphan generation, 10,000-candidate,
   2,000-hit and 20,000-rowid-distance cases remain required.

Only after those affected checks pass, produce a separately preserved optimized
helper through the actual pinned Lustr route. Run the unchanged eight-pair
stock/native suite on the exact candidate and oracle, retaining every sample,
slowdown and source/runtime/compiler identity. A separately named bounded
observer run may verify fewer wrapper calls if necessary, but it cannot replace
the suite or turn this source argument into a speed claim. Keep the history
change's measurement separate so attribution remains possible.

## Reviewed source identity

All files below were verified byte-identical between the measured a2fb base and
the ee178 documentation base. The current profile's observer changes to backend
and NativeDatabase are separate and are not an implementation base for this
production proposal.

| Path                                                     | SHA-256 at measured base                                           |
| -------------------------------------------------------- | ------------------------------------------------------------------ |
| `apps/server/src/persistence/redb_thread_search.rs`      | `248cd55f5b2a3f2561eb757ec04dc5b93044065d028bbc947d77f468f34a3c8e` |
| `apps/server/src/persistence/redb_transactions.rs`       | `3d66b58ad52fb39a67e150a9390329b3f40b87eedd20bb20b7c21f1d13d2d270` |
| `crates/storage/src/backend.rs`                          | `71ab55cb6ae7b151a5ea87620adcc1e839732731bc4d1bd179162fac96cd91bf` |
| `apps/server/src/orchestration-v2/ThreadSearch.ts`       | `cced0ba9a9e0412389a6c7bfbbec6a56e2dfbcaa58a75a74c4a881008e20d6b9` |
| `apps/server/src/orchestration-v2/NativeThreadSearch.ts` | `ebdea6dfe28aebfbd02eeee8afbabce8be64ccdb277de2f646d2d181429deddd` |
| `apps/server/src/persistence/RedbClient.ts`              | `ecb992d21ca19977c2c0fd1c1b90ea96bebe40d89b3f5b28cbee4a6b399e0c4e` |
| `tools/check-redb-thread-search.mjs`                     | `9098be653b1cc38f39f69c07a05648032ef77f5b6619d335cd0837f195d86111` |

Expected production ownership is `redb_thread_search.rs` only, plus focused
acceptance coverage if an existing fixture cannot express a required edge case.
No shared backend/session implementation change is necessary for the proposed
predicate or query-local state.
