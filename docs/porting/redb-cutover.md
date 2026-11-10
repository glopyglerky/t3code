# redb cutover

The production server and CLI still use SQLite. This change supplies the shared
native transaction and data-import boundary required to replace their repositories.
It does not select redb as the application's authoritative database, and it does
not retire any SQL repository or upstream source.

## Implemented boundary

`apps/server/src/persistence/redb_transactions.rs` is compiled by the existing
storage crate. Its application namespace preserves table and column names with
explicit primary keys. Rows and schemas are validated before storage. Native
transactions support get, put, delete and bounded cursor scans, with reads of
staged writes and a consistent snapshot. The existing backend checks changed keys
and scanned ranges before committing with immediate redb durability.

A helper owns up to 16 transaction sessions. A failed row operation discards the
session; helper termination discards all uncommitted sessions. Idle sessions expire
after five minutes. Reads and staged writes retain the backend's 16 MiB transaction
budgets. Cursor pages contain at most 4,096 rows. Cursors describe storage order,
not SQL ordering. Primary-key values remain stored beside their hash and are
checked on reads and writes, so a mismatched hash cannot overwrite another row.

`RedbClient.ts` supplies an injectable Effect service. `withTransaction` holds
the native connection exclusively for the complete callback. Standalone row
operations start their own transaction; operations inside the callback share its
session. Nested transactions, another client's transaction, child-fiber reuse,
escaped sessions and mixed SQL/redb transaction ownership reject before execution.
Exceptions, defects and cancellation roll back. A missing commit acknowledgement
remains an unknown outcome, with no automatic retry or fallback to SQLite.

The existing component event/outbox store and the application table namespace are
separate. Their transaction APIs must not be treated as interchangeable. The next
repository ports must commit all related application rows through one owner.

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
their SQL behavior. Native sequence allocation and constraint enforcement still
belong in the owning repository ports.

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
| `application_begin` | Open a snapshot transaction identified by the caller |
| `application_get`, `application_scan` | Read rows inside that transaction |
| `application_put`, `application_delete` | Stage validated row mutations |
| `application_commit`, `application_rollback` | End the transaction |
| `import_sqlite_application` | Stage an export and atomically publish its native row catalog |
| `application_import` | Resolve a completed import through its durable receipt |

`t3-code-ls-db` is included in the verified native helper bundle. Packaging a helper
does not make it authoritative. The existing Linux compiler/platform qualification
scope still applies; this change adds no macOS or Windows database acceptance.

## Work required for production cutover

| Boundary | Required work |
| --- | --- |
| Atomic event execution | Port `EventSink`, `OrchestrationEventStore`, `CommandReceiptStore`, `TurnItemPositionStore`, `ProjectStore`, `ProjectionStore` mutations and `EffectOutbox` into one native transaction. Preserve stream/application sequences, rejected command receipts and commit-before-publication. |
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

The real Effect adapter gate checks rollback on failures and interruption, nested
and mixed-store rejection, client/fiber isolation, concurrent access, pagination,
invalid JSON values and a deliberately discarded native commit acknowledgement.
The import gate checks exact cells, composite keys, migration/sequence preservation,
the checked-in application SQLite fixture, receipt recovery after restart,
unsupported sources, concurrent importers and post-import writes.

`tools/acceptance-checks.json` also now schedules the existing full database
durability and differential outbox gates. Multiple native executable arguments
are built and hash-recorded in order. Changes to a candidate's runtime select its
mapped acceptance checks, even when the test script itself is unchanged.

All of this is component evidence. The native client flow still exercises the
application's current SQLite database and does not establish redb cutover.
