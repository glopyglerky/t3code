# redb cutover

The production server and CLI still use SQLite. The native implementation now
includes application transactions, SQLite row import and the complete shared
event-log service. Production selects the SQLite event-row adapter. No SQL
repository or upstream source is retired by this candidate.

## Implemented boundary

`apps/server/src/persistence/redb_transactions.rs` is compiled by the existing
storage crate. Its application namespace preserves table and column names with
explicit primary keys. Rows and schemas are validated before storage. Native
transactions support get, put, delete and bounded cursor scans, with reads of
staged writes and a consistent snapshot. A write transaction acquires the redb
writer before acknowledging begin. It commits through that reserved writer with
immediate durability, so stateful provider normalization cannot encounter an
optimistic writer conflict only after its callback has run. Existing standalone
component transactions retain their optimistic key and range conflict checks.

A helper owns up to 16 transaction sessions. A reserved writer expires after five
idle minutes even while the helper waits for input. Helper termination drops its
uncommitted writes. Reads and staged writes retain the backend's separate 16 MiB
budgets. Up to 64 nested savepoints share another 16 MiB checkpoint budget. Cursor
pages contain at most 4,096 rows. Generic row cursors describe storage order, not
SQL ordering. Primary-key values remain stored beside their hash and are checked
on reads and writes, so a mismatched hash cannot overwrite another row.

`RedbClient.ts` supplies an injectable Effect service. `withTransaction` holds
the native connection exclusively for the complete callback. Same-client,
same-fiber nested transactions use savepoints. A failed nested body, defect or
interruption rolls back its own work; a successful release leaves durability to
the outer commit. Recoverable native statement errors poison that scope until
rollback. Transport uncertainty, incompatible storage and other fatal errors
invalidate the whole transaction even if application code catches the error.

Standalone reads use read-only snapshots; writes reserve the writer before the
callback runs. Another client's transaction, child-fiber reuse, captured parent
scope reuse, escaped sessions and mixed SQL/redb ownership reject before execution.
A missing commit acknowledgement remains an unknown outcome, with no automatic
retry or fallback to SQLite. Writer acquisition has a bounded timeout and reports
`writer_busy` before running the body.
While a session owns the writer, the helper also rejects unscoped legacy commits,
outbox mutations, imports and index preparation. They cannot wait on the same
helper's writer and prevent it from receiving the owner's rollback.

Opening a writable redb handle itself needs its writer lock. An uncontended open
performs the pinned integrity check. Proven contention permits a read-only handle
to open committed snapshots; corruption and other open failures do not. Before a
write begins, that handle must promote through a checked writable open within the
acquisition deadline. Held file identities prevent pathname replacement from
silently switching the database during promotion. The full open integrity scan is
deferred while a peer writer prevents promotion. Read-only access retains the
application's table/catalog validation; full-file checksum verification runs on
checked writable promotion. Startup allows a bounded window for a peer's crash
recovery before opening a read-only snapshot. Once the initial writer lock is held,
opening preserves redb's internal header-lock waits and recovery protocol.

The existing component event/outbox store and the application table namespace are
separate. Their transaction APIs must not be treated as interchangeable. The next
repository ports must commit all related application rows through one owner.

## Shared application event log

`OrchestrationEventStore.ts` keeps one implementation of event encoding, replay,
live subscriptions, unknown-type handling and publication. Its storage queries
are behind `EventRows`. `SqliteEventRows.ts` preserves the production SQL path;
`NativeEventRows.ts` translates the same operations to the Rust repository in
`redb_event_rows.rs`. The injected native service implements every existing public
event-store member without a substitute event stream or fake repository.

Native writes use the same tagged cells and primary keys as imported rows. They
allocate the application sequence and stream version in the caller's transaction,
enforce unique event IDs and stream versions, and update ordered indexes together
with the row. The imported SQLite sequence high-water mark remains authoritative
for allocation even when its newest event was deleted before export. A rolled-back
write or savepoint also rolls back the counters and indexes.

Preparation builds the indexes in bounded, resumable batches. It reserves the
event table for domain writes as soon as indexing starts, so generic row mutations
cannot bypass uniqueness or leave stale indexes. Queries require a completely
prepared index. The durable preparation cursor and completed marker let a retry
resume without duplicating counts or rebuilding later live events. Event identities
require ordinary UTF-8 text cells; byte-encoded SQLite text stays intact in the
import but fails event preparation instead of being mistaken for a base64 identity.

Ordered indexes cover application/agent selection and combinations of stream,
command and event type. Replay counters use a Fenwick aggregate index, so counts
and UTF-8 byte totals do not load the whole event history. Capped agent statistics
select the first `maxEvents + 1` events in the requested sequence range. Prompt
anchors reproduce the existing subagent and turn-item deduplication, including
restored payload byte counts and JSON number/string lexemes. Ordinary payload
strings remain opaque during statistics, including malformed historical JSON.
An invalid REAL-valued prompt anchor is refused rather than converted to a
different scalar representation; valid domain prompts are strings. Arrays,
objects, booleans and integer anchors retain SQLite's JSON types and coercions,
so malformed prompts cannot turn into valid text during restoration.

The event repository does not prune event rows or manage prompt-anchor retention.
Those operations must update its indexes in the future native maintenance port.
The existing 16 MiB protocol/read/write limits still apply to individual operations;
an oversized request or replay page fails explicitly. Event-log integration alone
does not port EventSink's projection, receipt, position and outbox transaction.

## Active row import

`redb_import.rs` first obtains a validated, content-addressed SQLite export archive.
It streams the immutable archived chunks into bounded staging transactions. Each
generation is identified by its export digest. Repeating a partial import checks
existing staged values and resumes without duplicating rows. A row's source ordinal
witness rejects duplicate primary keys even when duplicate row values are identical.

Each staging transaction observes that no application catalog is active. The final
transaction publishes the complete catalog and its durable import receipt together.
Consequently, readers cannot see a partial generation and a competing importer
cannot modify that generation after publication. Repeating a completed import
returns its receipt and preserves all subsequent application-row changes. A
different import cannot replace an active database. The SQLite source is never
modified.

Imported cells preserve SQLite storage classes explicitly:

| Source value | Native row value |
| --- | --- |
| NULL | `{ "type": "null" }` |
| Signed 64-bit integer | `{ "type": "integer", "value": "9007199254740993" }` |
| REAL | Tagged IEEE-754 bits from the validated export |
| Text | Tagged text, with base64 for bytes that are not valid UTF-8 |
| Blob | Tagged canonical base64 |

This representation avoids JavaScript integer truncation, text/blob confusion and
JSON coercion. Repository adapters must use the explicit representation for
imported rows; ordinary untagged values are not equal keys. Column defaults,
constraints, indexes, triggers, source migration records and sequence metadata are
preserved in the archive and generation metadata. Preserving them does not execute
their SQL behavior. The event repository implements its sequence and uniqueness
rules; the other domains still need their own native constraint implementations.

Tables without a primary key and rows with a null primary-key component require
an explicit identity mapping and fail before publication. Known derived FTS tables
remain excluded and recorded in the receipt. Search must rebuild its native index
before production cutover. Failed or interrupted generations remain unreachable;
automatic reclamation of unpublished generations is not implemented.

## Native helper protocol

The existing bounded JSON-lines protocol now includes:

| Operation | Result or effect |
| --- | --- |
| `application_init` | Initialize an exact schema once, or verify the existing schema |
| `application_schema` | Read the validated active schema, or null |
| `application_begin` | Reserve a writer before execution, or open a read-only snapshot |
| `application_get`, `application_scan` | Read rows inside that transaction |
| `application_put`, `application_delete` | Stage validated row mutations |
| `application_commit`, `application_rollback` | End the transaction |
| `application_savepoint`, `application_rollback_to`, `application_release` | Control nested scopes without committing the outer transaction |
| `application_events_prepare` | Build one resumable batch of event indexes outside an application session |
| `application_events_append` | Append a shared-log event and its indexes within the caller's transaction |
| `application_events_read`, `application_events_stats`, `application_events_latest`, `application_events_pruned_through` | Query the indexed event log through the same transaction owner |
| `import_sqlite_application` | Stage an export and atomically publish its native row catalog |
| `application_import` | Resolve a completed import through its durable receipt |

`t3-code-ls-db` is included in the verified native helper bundle. Packaging a helper
does not make it authoritative. The existing Linux compiler/platform qualification
scope still applies; this change adds no macOS or Windows database acceptance.

## Work required for production cutover

| Boundary | Required work |
| --- | --- |
| Atomic event execution | Connect the native event-log service to `EventSink`, then port its direct SQL guards, `CommandReceiptStore`, `TurnItemPositionStore`, `ProjectStore`, `ProjectionStore` mutations and `EffectOutbox` into that same transaction. Preserve rejected command receipts, per-snapshot savepoints, deferred publications and commit-before-publication. |
| Read models | Replace the remaining projection queries, joins, pagination, provider ownership, timeline windows, summaries and recovery queries. Use explicit indexes rather than loading a whole database snapshot. |
| Authentication | Port sessions and pairing links together; pairing consumption and session creation must stay atomic. |
| Scheduling and webhooks | Port task claims, compare-and-set updates, dispatch state, run counters, delivery queues and deduplication. |
| Other state | Port provider runtime, peer links, review guides, viewed files, MCP app context, usage history, workspace discovery, settings and cleanup consumers. |
| Search | Replace trigram FTS, JSON predicates and related ordering behavior, and rebuild derived indexes from imported message rows. |
| Startup and maintenance | Select one store for server and CLI, migrate before accepting new writes, replace SQLite maintenance/storage reporting and the separate server-ownership lock. |
| Legacy import and rollback | Keep lazy V1 transcript import working and define backup/restore and rollback after redb has accepted writes. The immutable original export alone is not a current backup. |
| Application/platform acceptance | Run fresh and migrated databases through the real client/provider fixtures, CLI concurrency, crash/restart and packaging gates on every supported target. Assert the database engine and that new application SQLite/WAL files are absent. |

Provider-owned databases inspected by integrations, desktop browser cookie imports,
and mobile-local caches are separate from the server-owned persistence cutover.

## Verification

Native tests cover transaction atomicity, snapshot reads, key and phantom conflicts,
row identity, rollback, expiry and resource bounds. Import tests cover durable
unpublished staging followed by reopen/resume, duplicate keys after a committed
staging batch, stale importers after activation and imports larger than one
transaction's budget.

The real Effect adapter gate compares nested commit/rollback behavior with SQLite
and checks failures, interruption, client/fiber/scope isolation, concurrent writer
ownership, read-only snapshots, pagination, invalid JSON and deliberately lost
savepoint, release, rollback and commit acknowledgements.
The import gate checks exact cells, composite keys, migration/sequence preservation,
the checked-in application SQLite fixture, receipt recovery after restart,
unsupported sources, concurrent importers and post-import writes.

The event gate runs the complete service from the pinned immutable upstream source,
the refactored SQLite service and the redb service through the same cases. It checks
project/agent sequencing, filtering and pagination, imported sequence high-water
marks, replay budgets, more than one page of unknown event types, malformed
ordinary payloads, both prompt-deduplication paths, rollback, explicit committed
publication, live filtering and reopen. Native tests cover resumable index
preparation, typed identities, sparse sequence ranges, all filter combinations,
savepoint rollback of event indexes and history exceeding one transaction budget.

`tools/acceptance-checks.json` also now schedules the existing full database
durability and differential outbox gates. Multiple native executable arguments
are built and hash-recorded in order. Changes to a candidate's runtime select its
mapped acceptance checks, even when the test script itself is unchanged.

All of this is component evidence. The native client flow still exercises the
application's current SQLite database and does not establish redb cutover.
