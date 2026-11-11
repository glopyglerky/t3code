# redb cutover

The native storage crate now resolves the user's `omdb` fork at the immutable
revision recorded in `database.lock.json` and `Cargo.lock`. `omdb` currently
keeps the `redb` Cargo package name and version 4.5.0, so existing Rust API and
on-disk compatibility contracts are unchanged. CI checks the exact Git source,
including the lockfile, to prevent a silent fallback to crates.io's `redb`.
Pinning the engine is independent of making it authoritative for the T3 server.

The production server and CLI still use SQLite. The native candidate now includes
SQLite row import and an injectable `NativeEventSink` composition: events,
receipts, positions, projects, projection mutations and the application outbox
share one redb transaction. Production still selects the SQLite services. Full
projection read models and the other server persistence domains remain unported;
no SQL repository or upstream source is retired by this candidate.

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
separate. `NativeEventSink` uses only application repositories through its supplied
`RedbClient`; it does not combine independent component commits with an application
transaction.

## Atomic EventSink composition

`NativeEventSink.ts` supplies the complete existing `EventSink` service without a
`SqlClient`. `PersistenceTransactions` and `ProjectionCommitStore` expose the
transaction and projection operations needed by the existing orchestration
algorithm. The retained SQLite layers implement the same boundaries and keep the
production composition unchanged.

The native composition joins the following work to the caller's reserved writer:

| Repository | Native scope |
| --- | --- |
| Shared event log | Application and agent events, sequence/version allocation, replay and live publication through the existing service |
| Command receipts | All application receipt operations and all four V2 receipt methods, including accepted/rejected replay and the global project/thread command namespace |
| Turn-item positions | Both public methods, stable existing-item lookup, supplied/run-derived ordinal selection and unique per-thread allocation |
| Projects | All six `ProjectStore` members, including selection, event application and shell views |
| Projection commit store | Mutations for all 16 entity families, provider-session bindings, applied cursor, transactional thread/run/request reads and ownership/version guards |
| Application outbox | All 17 `EffectOutbox` members, including enqueue, cancellation, claims, settlement, retries, recovery, pruning and worker notifications |

Provider snapshots retain a savepoint for each nested commit. A failed snapshot
rolls back its events, receipts, positions, projects, projections and outbox work,
while a successful sibling remains part of the outer transaction. Guard reads
and their resulting writes run under the same writer reservation. The
`ProjectionCommitStore` boundary is the mutation side and the reads required by
`EventSink`; it does not replace the complete `ProjectionStore` read API.

`RedbClient.afterCommit` attaches callbacks to the current transaction scope.
Successful savepoint release transfers them to the parent in registration order;
rollback, defects and interruption discard that scope's callbacks. Only an
acknowledged root commit runs them, after releasing the database session. An
unknown commit outcome does not publish. The same client's write/commit lane stays
held until its callbacks finish, preserving publication order while permitting
callbacks to read committed rows. A callback cannot reenter that client's writer.
Consequently, an `EventSink` call inside an existing native transaction defers live
events and outbox signals until the actual owner commits. SQLite callers retain
their existing publication behavior.

### Managed domains and shared normalization

Domain preparation validates exact source schemas and builds required indexes in
bounded, resumable transactions. A managed-table marker prevents generic row
mutations from bypassing domain constraints from the first preparation batch.
Repositories require completed preparation; a failed attempt can resume from its
durable cursor. Index entries retain and check the row identity behind their hash.
Rows, indexes and allocation metadata roll back together at a savepoint or root
transaction boundary.

Receipts keep the original command primary key and all receipt fields. Positions
maintain ordinal uniqueness and per-run allocation ranges. Project indexes cover
creation order, workspace equality and active/deleted selection, including queries
larger than one index page. Projection indexes implement the uniqueness and native
provider ownership checks needed during event commits. The application outbox
retains imported FIFO order, payloads and lease cells; its existing Rust state
machine operates over the application repository. It does not substitute the
separate component outbox. An uncertain standalone claim can be resolved through
its durable claim receipt; a claim inside an application transaction cannot recover
by starting a second owner.

`redb_application_rows.rs` shares tagged-cell encoding, exact schema validation and
witnessed row access across the new repositories. `redb_json.rs` shares the JSON
edits that preserve existing number/string lexemes and object order. Application
normalization remains shared: project defaults and omitted/null updates stay in
`ProjectStore`, turn-item normalization stays in `TurnItemPositionStore`, and pull
request identity normalization uses the existing source-control implementation.
For provider updates that require a domain codec round trip, Rust selects the old
row, the adapter applies the existing codec, and Rust checks the raw preimage before
applying the mutation. Mutation and index algorithms remain in Rust.

These repositories implement their typed application contracts, not arbitrary SQL
coercion. In particular, a new position requires a nonnegative safe integer run
ordinal, checked after the existing-item lookup. Fractional transport values and
REAL-valued imported positions are not accepted as integer domain values. The
immutable import still preserves those cells exactly; preparation rejects an
unsupported domain representation instead of changing it.

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
an oversized request or replay page fails explicitly. The event repository now
participates in the native EventSink transaction described above.

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
their SQL behavior. The managed EventSink repositories implement the constraints
and indexes needed by their operations. Unported domains still require explicit
native behavior before they can become authoritative.

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
| `application_receipts_prepare`, `application_positions_prepare`, `application_projects_prepare`, `application_projections_prepare`, `application_outbox_prepare` | Validate and prepare one resumable domain batch outside an application session |
| `application_receipts_insert`, `application_receipts_upsert`, `application_receipts_get` | Read and write the shared command receipt namespace |
| `application_positions_allocate` | Reuse or allocate a witnessed turn-item position |
| `application_projects_select`, `application_projects_upsert` | Select indexed project records and replace rows with their indexes |
| `application_projections_apply`, `application_projections_normalization`, `application_projections_read`, `application_projections_guard`, `application_projections_mark_applied` | Apply projection mutations, obtain codec input, read guarded state and advance the cursor inside the owner |
| `application_outbox_*` session operations | Run the complete outbox repository inside the application owner, including durable standalone claim-recovery receipts |
| `import_sqlite_application` | Stage an export and atomically publish its native row catalog |
| `application_import` | Resolve a completed import through its durable receipt |

`t3-code-ls-db` is included in the verified native helper bundle. Packaging a helper
does not make it authoritative. The existing Linux compiler/platform qualification
scope still applies; this change adds no macOS or Windows database acceptance.

## Work required for production cutover

| Boundary | Required work |
| --- | --- |
| Whole-server ownership | Integrate the candidate EventSink composition only when its surrounding readers, services and maintenance operations use the same selected store. Production must not split related application authority between SQLite and redb. |
| Read models | Replace the remaining full `ProjectionStore` queries, joins, pagination, provider discovery, timeline windows, summaries and recovery queries. The transaction guards already ported do not implement these broader APIs. Use explicit indexes rather than loading a whole database snapshot. |
| Authentication | Port sessions and pairing links together; pairing consumption and session creation must stay atomic. |
| Scheduling and webhooks | Port task claims, compare-and-set updates, dispatch state, run counters, delivery queues and deduplication. |
| Other state | Review guides and viewed files now have candidate native omdb transaction operations, a bounded viewed-files index, and opt-in Effect repository adapters; SQLite remains the production authority. Qualify those adapters against upstream before activating them. Port provider runtime, peer links, MCP app context, usage history, workspace discovery, settings and cleanup consumers. |
| Search | Replace trigram FTS, JSON predicates and related ordering behavior, and rebuild derived indexes from imported message rows. |
| Startup and maintenance | Select one store for server and CLI, prepare managed domains before accepting writes, replace SQLite maintenance/storage reporting and the separate server-ownership lock, and port event pruning and prompt-anchor retention with their indexes. |
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
savepoint, release, rollback and commit acknowledgements. Its callback cases assert
root and nested rollback behavior, registration order, delayed publication,
committed reads during publication, write-reentry rejection and no callbacks after
an unknown commit.
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

The additional registered gates specify these application-boundary checks:

| Gate | Assertions |
| --- | --- |
| `check-redb-receipts-positions.mjs` | All receipt and position methods against immutable upstream SQLite; imported null/empty fields and global command identity; run ownership and ordinal precedence; existing-item reuse; nested/root rollback; range overflow; restart; integer-domain rejection; lost commit acknowledgement |
| `check-redb-projects.mjs` | All six public members against immutable upstream and retained SQLite; more than one index page; ordering and filters; raw JSON and legacy decoding; omitted/null/default updates; index replacement, rollback and reopen |
| `check-redb-application-outbox.mjs` | All 17 members against immutable upstream SQLite; all effect types and statuses; imported FIFO, payload and lease preservation; transactional cancellation/enqueue; worker ownership, backoff, recovery, pruning, uncertain acknowledgements and the real EffectWorker |
| `check-redb-event-sink.mjs` | The complete shared service against immutable upstream and retained SQLite; all 16 projection families and bindings; command replay and guards; shared normalization; atomic provider-batch rollback; ordered publication; the actual provider snapshot coordinator; replay, live streams and reopen |

The receipt/position, project and EventSink fixtures run the actual application
migrations before exporting/importing their SQLite source. Native compositions
receive no `SqlClient`; retained SQLite and immutable upstream implementations
remain separate oracles. Fixtures and proofs record source and helper hashes.
Registration and the assertions described here are not a declaration that a
particular committed head has passed. Qualification must run the mapped checks
against that exact head and its packaged helpers.

`tools/acceptance-checks.json` schedules these four gates and their scoped
typechecks, together with the existing transaction, import, event-log, database
durability and differential component-outbox gates. Multiple native executable
arguments are built and hash-recorded in order. Changes to a candidate's runtime
select its mapped acceptance checks, even when the test script itself is unchanged.

All of this is component evidence. The native client flow still exercises the
application's current SQLite database and does not establish redb cutover.
