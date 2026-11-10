# Default-branch 934c7f8 integration

This change integrates `pingdotgg/t3code` commit
`934c7f8d8e7a012aa55be9c0b17498979040fe02`, observed on its trusted default
branch, into fork base `0b3f496ad28cae721f0a3b7aa40285d68e69227e`. It advances
the integrated source from nightly commit
`822671581383a357c5dc8d3ea9984b408c3e03c6`.

## Source accounting

The delta contains the 20 paths previously detected at `6c9adfb` and two later
one-line pull-request UI spacing fixes.

| Disposition | Paths | Treatment |
| --- | ---: | --- |
| Exact upstream content | 20 | Preserve upstream blobs and modes at their existing paths. |
| Merged with fork changes | 1 | Add six ended-watch metadata lines to `Orchestrator.ts` while retaining its Rust service calls. |
| Existing fork CI policy | 1 | Keep `mobile-eas-production.yml` absent; this fork does not run upstream's store-release infrastructure. |

There are no application source removals. The existing 51 protected removal
records remain unchanged. Source history now contains all three integrated
revisions, and every mapped port records the new review revision. The only
changed mapped upstream blob is `EventSink.ts`, owned by the candidate redb port.
The upstream README body is unchanged and still matches its pinned source.

The source lock identifies this as a default-branch snapshot. The published
`v0.1.9-nightly.20261110.4452` identity remains separately recorded under
`nightly_reference`; it still names `8226715`.

## Behavior and Rust boundaries

The retained streaming implementation now schedules idle flushes through control
items accepted by `EventSink` and `ProviderCommitBatch`. The Rust provider
process continues carrying the same JSON-RPC envelopes. No Rust algorithm or wire
format changes are needed for that scheduling change.

Pull-request checks can identify placeholder entries, and active or ended watches
can retain approval holds and whether those holds were announced. Evaluation of
that state remains in the retained pull-request implementation. The existing
recovery adapter fixtures now include nonempty holds, empty holds, explicit
`false`, and omitted fields in both watch forms. Existing assertions check the
whole durable thread after Rust-backed recovery, SQLite persistence, database
reopening, replay and repeated recovery. This verifies preservation around the
Rust recovery planner; its request protocol does not parse PR-watch metadata.
No extra recovery scenario or process invocation is added.

The mobile changes and PR display adjustments retain their upstream
implementations. The maintenance update-selection port, compiler routes,
dependency locks and CI jobs from the fork base remain unchanged.

## Tracking and qualification

The nightly checker now accepts immutable default-branch source locks. It reports
an ancestor nightly as already integrated, with no reverse-import or source-removal
work. Equal, newer and divergent revisions remain distinct. Complete tree
comparisons and detected, integrated and qualified identities remain available.
Existing nightly locks keep their tag-movement and publication rollback checks.
Four regression tests extend the existing tracker suite.

The candidate must pass the existing Rust, source-retention, component, production
adapter and Rust-backed client gates before `main` advances. The existing scoped
typechecks exercise the changed shared types through the fork's adapters. Upstream's
pristine streaming and PR-watch suites remain trusted. Compiler pins and compiler
checks are not expanded.

This source integration does not promote platform acceptance or retire an original
implementation. SQLite remains authoritative; redb and Iroh cutovers remain
separate work.
