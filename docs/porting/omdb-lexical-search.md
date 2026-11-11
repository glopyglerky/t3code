# Native lexical search and in-thread find

The omdb adapters replace storage queries behind the existing `ThreadSearch` and
`ThreadFind` services. They do not substitute vector similarity for lexical
matching, change the palette ranker, or create another application protocol.
The TypeScript ranking, snippet, find-text parsing, navigation and cache logic
remain shared with the retained application.

## Exact import semantics

SQLite export version 2 records the actual signed message rowid and the FTS5
instance vocabulary: term, document rowid and position. It does not reconstruct
the index from current message text. This preserves stale contentless-index
entries, orphan documents, legacy placeholders and the distinction between an
absent index and an empty index. Older version 1 exports cannot supply those
facts and cannot qualify search preparation.

An indexed export also carries a checksum-covered `searchDeleteWitness`: the
current FTS document identities and the exact positional vocabulary remaining
after each current document is explicitly deleted on an owned SQLite snapshot.
The original source is never mutated. This records which historical terms survive
the next contentless DELETE, including duplicate insertions and empty-token
generations. Native preparation rejects indexed imports without this witness
before activation; guessing from current message text would lose source behavior.

The native rowid owner preserves an existing message's identity on update and
implements the source allocation rules for inserts. A source export ordinal is
not a rowid. Candidate selection preserves the original 10,000 matching-document
cutoff, including orphan documents before application filters; the 2,000 hit
limit follows thread/project/message filters. The no-index path uses the newest
20,000 units of numeric rowid distance, not the newest 20,000 physical rows.
Matching identities outside the JavaScript safe-integer range fail the public
query, as the existing Node SQLite row decoder does, rather than rounding them.

## Trigrams and positions

The accepted native query grammar is the application's generated conjunction of
quoted phrases, with doubled-quote escaping. Positional intersections implement
phrase matching. This is not a SQL parser or a general FTS query language.

Case and diacritic folding are translated into safe Rust from SQLite's
public-domain source, not replaced by Rust's changing Unicode lowercase or
general Unicode normalization:

| Source | Immutable Git blob | Use |
| --- | --- | --- |
| [sqlite/sqlite: ext/fts5/fts5_unicode2.c](https://github.com/sqlite/sqlite/blob/master/ext/fts5/fts5_unicode2.c) | `2133d5d5b894c35fbbe642a32ee027ca15ddab33` | Legacy Unicode fold and complex Latin diacritic tables |
| [sqlite/sqlite: ext/fts5/fts5_tokenize.c](https://github.com/sqlite/sqlite/blob/master/ext/fts5/fts5_tokenize.c) | `94132272fb6e3a7a04c4108f7cd4ddd2a1e4c42d` | Trigram positions |
| [sqlite/sqlite: src/utf.c](https://github.com/sqlite/sqlite/blob/version-3.54.0/src/utf.c) | `e8bb63979703241b038cac37a686de2fbe9616b2` | Scalar decoding used by the pinned Node 26 SQLite 3.54 amalgamation |

The links identify the upstream files; the Git blob IDs identify the exact
translated bytes. SQLite's blessing is retained in `redb_search_unicode.rs`:

> May you do good and not evil.
> May you find forgiveness for yourself and forgive others.
> May you share freely, never taking more than you give.

In particular, trigram `remove_diacritics=1` uses SQLite's fold mode 2. Dropped
combining marks do not consume trigram positions. The pinned SQLite 3.54
amalgamation preserves U+FFFE and U+FFFF as distinct scalars, as well as other
planes' noncharacters. Rust strings already exclude malformed UTF-8 and surrogates. The
application's curly-apostrophe replacement occurs before indexing.

## Live writes and bounded index size

Live writes share the projection mutation's omdb transaction. Delta/run varints
encode positional ranges, collapsing repeats and avoiding duplicated absolute
offsets for nonconsecutive occurrences. Ordinary documents use one record per distinct
term; documents exceeding 4,096 distinct terms use at most 256 exact term-map
buckets. Bucket collisions only co-locate records: terms and positions are
checked before counting candidates. Normal documents retain direct term lookup;
only high-cardinality documents require bucket filtering.

A document manifest tracks the current insertion generation and selects the
original or delete-survivor source vocabulary atomically. An edit or deletion
therefore does not stage two key deletions per historical occurrence. The
imported witnesses remain available as source evidence. An insertion replaces
its terms' positions while retaining older terms absent from that insertion.
DELETE removes only the latest generation's terms; it neither removes all older
terms nor resurrects earlier positions for an overwritten term. Empty-token
insertions still own a generation. Unchanged-text updates do not rewrite the
index. Savepoint rollback restores both row and lexical state.

Search snapshots use the actual read-only omdb transaction. They do not retain
the optimistic conflict observations required by mutable transactions. Recency
ranking reads compact projection metadata rather than historical message bodies.

## In-thread find

Native find uses the shared projected timeline index, including inherited run
forks, visibility and synthetic fork markers. Payloads are fetched in batches of
128 within the same read snapshot. Its revision includes source threads and all
run-fork ancestors, so a parent-only event invalidates a descendant scan cache.
The read owner is released before text parsing and result-frame emission.

## Reproducible acceptance

`tools/check-redb-thread-search.mjs` and `tools/check-redb-thread-find.mjs` execute
the immutable integrated upstream source as the SQLite oracle. They compare the
retained adapters and native adapters using actual databases and application
services. The generated receipts record source and executable hashes. Run them
against the exact newly built helper; an earlier receipt does not qualify a
later executable.

The gates cover indexed and no-index search, signed rowid gaps, exact candidate
and hit limits, Unicode fold-rule boundaries, long repeated and high-cardinality
output, source placeholders and orphan postings, live edits, rollback, restart,
fork ancestry and find navigation. They qualify these persistence boundaries;
they do not independently establish platform-wide UI or packaging acceptance.
