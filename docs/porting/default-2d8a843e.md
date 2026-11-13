# Upstream integration 2d8a843e

The source advances from `4aec277f79b2d4dabd88f13cfab264d9f80cbcef` to
`2d8a843e6be9ca170f3bce98cc92ad76617b00c4`, the chosen upstream cutoff for
this qualification. The merge retains both histories, the preceding project API
repair and the fork's selected native execution paths. Source inventories and
mapped implementation identities refer to this exact revision.

## Shell reads and migration 89

One upstream commit changes five files. It adds migration 89 and narrows shell
queries without changing public contracts, providers, clients or dependencies.
The migration adds expression indexes for worktree paths and family roots, plus
a covering run-status index. Its table definitions match schema 88 exactly.

The fork retains the extracted `SqliteProjectionShellRows` repository. All
upstream query changes are applied there rather than restoring the duplicate SQL
inside `ProjectionStore`. Terminal-failure lookup uses the already-selected run;
shell archive selection uses the stored archive column; worktree and family
filters guard malformed JSON before selecting indexed candidates. Item counts
read the covering status index instead of loading complete run rows. The shared
projection helpers and native repository injection remain intact.

The audit also corrects the extracted pull-request snapshot filter to use the
archive column, as its immutable upstream counterpart already did. Other
upstream APIs that still filter the payload's archive field keep that behavior.

The native shell repository uses worktree and family equality indexes before
decoding candidate payloads. A narrow read therefore avoids unrelated thread
payloads, including malformed ones excluded by the upstream indexed predicate.
Selected malformed data still fails validation. The archive-column changes are
applied to the same native queries as their SQL counterparts. Run counts already
use compact native status metadata, and terminal-failure lookup retains the
presented run's identity and root node.

## Native upgrade and activation

Migration 89 changes indexes and history without changing table definitions.
Checking only table equality would leave schema-88 homes unable to satisfy the
new migration head. The native bootstrap therefore recognizes the exact known
schema-88 history and upgrades it under exclusive ownership. Schema-80 homes
retain their backfills and can advance directly to the current history.

Both paths stage a separate generation in bounded batches and publish its
catalog, prepared read indexes, migration history and durable lineage together.
The prior generation and original import archive remain intact. Activated homes
retain native writes and do not reimport their older SQLite backup. Unactivated
imports must still match a fresh canonical snapshot of the retained source
before writable upgrade or activation. Receipt validation supports the known
schema-80-to-88 lineage followed by the schema-88-to-89 transition, while
unsupported histories, missing lineage and read-only upgrade attempts remain
errors.

## Schema provenance and qualification

The current native schema is regenerated from all 89 retained migrations. The
schema-80 fixture is unchanged, and `omdb-schema-88.json` preserves the complete
prior definition and migration-source identities. JSON formatting does not alter
the preserved schema or history. The current table definitions and the first 88
migration entries are checked against that fixture before advancing the native
manifest.

The targeted acceptance cases cover activated and unactivated schema-80/schema-88
homes, retained native edits, repeated startup, interruption and changed-source
rejection. Shell comparisons execute immutable upstream, the extracted SQLite
reader and the native reader over the same fixtures. They include archive-column
disagreements, missing and duplicate filters, deleted family roots, malformed
unrelated and selected rows, query plans, index changes within transactions and
restart after commit. Native regressions additionally check bounded candidate
selection and the exact supported upgrade histories.

The event-store acceptance oracle is pinned to the same upstream revision and
continues to assert its independent Git blob identity. Source retention and
upstream README checks remain separate from behavioral qualification. This
integration retires no additional platform implementation and requires fresh
native, adapter and full application acceptance for its final source tree.
