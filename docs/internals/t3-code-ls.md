# t3 code ls boundaries

The requested end state is T3 Code's current nightly and its mobile applications
entirely in Theo Browne's Lust language, named t3 code ls, with Iroh carrying
Connect traffic and the useful Iroh suite fully integrated. This includes server,
web, desktop, iOS, and Android. Source currency,
language compatibility, application parity, and platform qualification are
separate claims. None follows automatically from another.

The current code preserves the complete upstream nightly as the reference and
adds a native workspace under `native/t3-code-ls`. The source pin identifies a
published nightly, not whatever happens to be on `main`. The fork's older fix
branches remain independent; their existence is not evidence that those fixes
have been incorporated into the native port.

## Compiler boundary

Lust accepts a subset of ordinary Rust. Its upstream contract uses rustc as the
semantic oracle and Cargo for production builds. Rust syntax alone is not proof
that a file works with Lust. Application components must pass an actual pinned Lust
invocation and behavior comparison. The build must fail when that invocation
fails; silently falling back to Cargo would hide a regression.

Iroh depends on async Rust and crates beyond Lust's adapters. Mobile native entry
points and platform bindings also require functionality current Lust rejects.
The transport adapter therefore remains Cargo-built Rust, and the original UI
remains in its current languages. Replacing those boundaries requires compiler
or application work with its own runtime evidence. Renaming `.rs` files, wrapping
the TypeScript app, or accepting a Cargo build cannot establish an all-Lust port.

The verified compiler is the current `next` implementation, pinned in
`lust-toolchain.json`. Its source also contains a C/zig release path for selected
Linux, macOS, and Windows targets. Those targets do not include iOS, Android, or
browser WebAssembly. This fork has not qualified that release path and continues
to use Cargo for release artifacts.

Synchronous standard-library networking, child processes, threads, and files do
run through this compiler. The provider process runtime uses those capabilities
directly. Its Unix socket pairs let shutdown unblock owned readers and writers
even if a descendant retains inherited descriptors. It terminates and reaps only
the child it spawned. The initial implementation is Unix-only and is not evidence
of a Windows or mobile provider host.

The provider JSON codec preserves number text so large identifiers survive a
decode/encode cycle. It bounds input bytes, nesting, and value count. Strings
must contain Unicode scalar values, so unpaired escaped surrogates are rejected;
JavaScript's JSON parser accepts them. This is an explicit input constraint at
the native boundary. Invalid UTF-8 and non-finite numeric request IDs are also
rejected. Finite numeric IDs use ECMAScript formatting through Lust's pinned
`ryu-js` adapter; string IDs remain strings. The raw protocol runtime does not
replace Codex's generated method schemas or T3's provider-to-orchestration event
mapping.

`T3_CODE_LS_PROVIDER_EXECUTABLE` selects the Lust process runtime in both Codex
launch paths: the provider/account/skills probes and the live session factory.
Selection comes from the server environment, before the existing agent process
scope is applied. Provider-specific environment values cannot replace the
selected executable. The wrapper inherits the environment already chosen by the
server, preserving account overlays and an absent `HOME` without exposing values
in command arguments. The separate `codex exec` text-generation path is unchanged.

The parent Effect client still owns generated payload schemas, request
correlation, cancellation, and deadlines. Parent-protocol mode therefore keeps
no native pending-request or approval table. Effect removes cancelled requests
without sending a wire cancellation; retaining a second table would eventually
exhaust its limit on a healthy shared session. Native frame and queue limits
remain, provider exit codes and stderr reach the original diagnostics, and
native framing failures terminate the connection. Invalid configured selection
also disables the original executable's diagnostic version fallback.

Recovery event planning is a separate Lust port of the two upstream completion
functions in `subagentProjection.ts`. It preserves event order and allocation
failures while retaining payload fields outside its model. The environment
selector `T3_CODE_LS_RECOVERY_EXECUTABLE` routes startup/shutdown reconciliation
and both live orchestration flows through the same adapter. The executable
returns drafts without IDs, and the adapter validates the entire response before
using the caller's allocator. JSON extensions survive schema decoding; known
timestamp fields become the actual contract's DateTime values. The caller still
owns authoritative event storage and transactional commit.

Reconciliation combines orphaned subagents and root turns in one process per
thread so a shutdown transaction does not start one process per record. Native
failure aborts the existing transaction, including earlier thread writes in the
same shutdown group. Interruption awaits child cleanup. The process receives an
empty environment, bounded JSON over standard input, and no shell. Its fixed
limits are 128 MiB per request/response, 65,536 output events, and 30 seconds.
The SQLite integration gate exercises persistence, reopen/replay, idempotence,
group rollback, and cancellation. This establishes recovery integration, while
the event store, projections, and outbox still execute in TypeScript.

`T3_CODE_LS_GUEST_SHARE_EXECUTABLE` selects the guest-share decision port at all
orchestrator call sites, including queued edits and attachment release during
ordinary thread mutations. Its response carries an explicit change marker.
The adapter returns the original object for an unchanged result because the
caller uses identity to suppress events. Comparing values would lose real
updates that happen to produce equal data at the same timestamp. Unrelated
thread, share, message, and attachment fields survive the native round trip.

Guest-share domain refusals retain their exact upstream strings because the
guest service maps those strings to public reasons. Native process and protocol
failures remain separate errors. The event sink still records rejected command
receipts, while domain state and attachment-cleanup effects commit only on
success. The guest service retains upload rollback and the effect worker retains
file deletion after commit. Expiration parsing uses the original `Date.parse`
result, including invalid dates, while conversion of DateTime fields is explicit.
This ports decisions; credentials, HTTP/SSE, file storage, and Connect hostname
acquisition remain in the existing guest service.

`T3_CODE_LS_PROJECT_EXECUTABLE` moves project command eligibility and event
planning into Lust under the project service's existing project/workspace locks.
The service still allocates an event ID before planning and stores domain
rejections through its original command-receipt path. Replayed commands retain
their first durable outcome. Process and protocol failures remain infrastructure
errors and never trigger the TypeScript planner as a fallback. Complete response
validation checks command association and copied payload data before persistence;
it does not replace native eligibility with a second TypeScript planner.

Monogram length uses the host's actual `Intl.Segmenter` result as an explicit
input because the pinned Lust core has no equivalent Unicode grapheme library.
Script IDs use the nightly contract's rules and JavaScript UTF-16 length in
rejection text. Optional fields, explicit nulls, and nested JSON extensions
survive the boundary. The service's forced deletion path remains a sequence of
durable child deletions followed by project deletion, so a native project-plan
failure preserves partial progress for retry. Claiming one transaction for the
whole cascade would change upstream behavior and hide lost cleanup work.

Source basis: [Lust's implementation contract](https://github.com/t3dotgg/lust-lang/blob/240e08d91d4aed0299639f6f11fd522912b248f9/README.md),
[release target allowlist](https://github.com/t3dotgg/lust-lang/blob/240e08d91d4aed0299639f6f11fd522912b248f9/src/resolve/cfg.rs),
and [C/zig release driver](https://github.com/t3dotgg/lust-lang/blob/240e08d91d4aed0299639f6f11fd522912b248f9/src/cc.rs).

## Database decision

The native authoritative database target is **SurrealDB 4.0.0**, using
**SurrealKV** on a single node now and the supported **SurrealDS** distributed
backend for a future active-active deployment. The database service and local
storage engine are Rust. The application uses the database's authenticated
network API so independent application processes share a service rather than
opening the same local files. Local tests use `sync=every` and explicitly verify
recovery after killing both the database process and its clients.

SurrealKV itself is not replicated HA. SurrealDS is available through SurrealDB
Enterprise and Cloud Scale, with paid licensing required for that path. This
choice accepts that future commercial dependency; it does not implement a
cluster or claim that the free single-node engine provides active-active HA.
The engine version, official Linux archive, and checksum are pinned in
`native/t3-code-ls/database.lock.json`.

The choice preserves one query/transaction API when the storage deployment
changes. SurrealDB's single-node SurrealKV configuration uses snapshot isolation,
not general serializability. The application therefore compares and updates a
global revision record inside every write transaction. Independent commands
that read the same revision cannot both commit, even when they modify different
projects. This prevents write skew from decisions such as "no other project owns
this workspace." It also serializes successful commits across the entire store.
Removing that bottleneck requires a separately tested partition/constraint
design; concurrent clients alone are not evidence of higher throughput.
SurrealDS accepts transactions at any node but routes commits through one
sequencer with a durable acknowledgement from another zone. Adding nodes
increases read/query capacity and failover options, not write capacity. This
supports active application nodes; it does not promise horizontal write scaling.

Turso's Rust engine remains a plausible SQLite-compatible option for local
applications. Its documented offline synchronization conflict resolution uses
rollback/replay and last-push-wins behavior, which is not sufficient for this
application's authoritative ownership and event-ordering contracts. CRDT stores
or Iroh docs likewise do not replace a transactional event log. SurrealDB's
PostgreSQL/MySQL compatibility layer does not make it a SQLite driver replacement.
FTS5, SQLite JSON expressions, `INDEXED BY`, AUTOINCREMENT, migrations, savepoints,
and SQLite-specific schemas all require repository work.

### Implemented native transaction boundary

`t3-code-ls-storage` commits an immutable command receipt, ordered immutable
events, projection mutations, pending outbox entries, and revision/sequence
counters in one database transaction. A failure after an earlier write rolls
the entire transaction back. Commands carry a digest of their validated intent;
replay returns the original receipt, while reuse of the ID with another digest
fails. Event and outbox IDs use create-only writes. The client distinguishes
an explicit conflict from an unknown commit outcome and performs no blind
automatic retry. Callers resolve uncertainty by the original receipt ID.

The bounded JSONL client supports local authenticated loopback connections and
TLS remote connections. Initialization is explicit, versioned, and limited to
the native namespace/database. Read operations validate stored sizes before
fetching payloads and reject oversized snapshots rather than truncating them.
Project planning reads a revision-consistent snapshot of the indexed
`project/` key range, so unrelated domains do not consume its allowance. This
initial project snapshot is bounded to 16 MiB and 65,536 rows; it needs direct
project/workspace queries before it can replace larger production stores.
The outbox has durable pending records and read operations, but no distributed
claim/lease/ack worker yet.

The standalone native project service accepts creation and metadata updates.
It reads projects and workspace ownership from the actual database snapshot,
runs the new Lust-built `t3-code-ls-project-store` executable, and commits its
event and projected row with the receipt. The executable ports both the nightly
planner and the `ProjectStoreV2.apply` row behavior, including omission versus
null and the persistence schema's handling of nested metadata. A separate oracle
executes immutable upstream code with its real contracts, Effect runtime, and
SQLite project store, then compares complete events and rows.
Callers must normalize workspace paths using the production service's path
rules before invoking this low-level API. Its ownership comparisons use exact
stored strings; filesystem resolution and repository enrichment are not ported.

The Cargo host validates input contracts and computes Unicode grapheme counts.
For this new store it also evaluates the portable planner/projector to validate
the complete selected executable response before writing. This duplicates
decision evaluation in Rust; it is response validation, not a fallback when
Lust fails, and is not an all-Lust database host. The original production
TypeScript adapter retains its existing validation approach. Native project
deletion is rejected because its child-thread lifecycle, cleanup and partial
progress/retry semantics have not yet been migrated. The production project's
previously ported deletion path is unchanged.

### Data migration and remaining cutover

The SQLite exporter opens a consistent read transaction and emits a versioned,
bounded, checksummed NDJSON representation. It preserves schema and migration
metadata, sequences, exact integer/real values, text including invalid UTF-8,
blobs, and nulls. Only the known T3 contentless message-search index and its
SQLite-reported shadow tables are excluded after checking the exact schema and
backing message table. Other virtual tables, including FTS that may contain
authoritative text, fail rather than silently lose data.
The importer validates the complete format, stores immutable content-addressed
chunks, verifies a second pass over the same file and database readback, and
only then creates a completed archive record. Recovery verifies all stored
chunks and the full export before publishing a new NDJSON file exclusively.

These archives are a data-preservation prerequisite. They do not translate the
upstream SQL repositories into native projections or activate a production
database. Existing application data must not be split across independently
committing stores. In particular, `EventSink.ts` atomically combines events,
project/thread projections, command receipts, and outbox work; those participants
must migrate together.

| Database surface                                      | Current state                                           | Required work before replacement                                                                                 |
| ----------------------------------------------------- | ------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| Native event/projection/receipt/outbox store          | SurrealDB/SurrealKV with atomic revision-guarded writes | Domain repositories, indexed reads, outbox execution and application integration                                 |
| Production server `statev2.sqlite`                    | Existing TypeScript/SQLite remains authoritative        | Migrate all transaction participants, typed row transforms, read parity, verified backup/restore and cutover     |
| Mobile SQLite caches and preferences                  | Existing Expo/platform adapters                         | Cache schema and lifecycle port; local offline storage is a different role from the server's authoritative store |
| Relay PostgreSQL and Cloudflare Durable Object SQLite | Existing hosted infrastructure                          | Rewrite the relay's account/session/device/delivery repositories and deploy its replacement                      |
| Browser IndexedDB and nonexportable key storage       | Existing browser adapters                               | Preserve browser lifecycle and key protection; a server database cannot replace platform key storage             |
| External SQLite formats read by integrations          | Remain readers of their owners' formats                 | Keep SQLite import/read adapters where interoperability requires them                                            |

Future active-active application work also includes durable cross-node event
notification, outbox leases/fencing, provider-session ownership, workspace
side-effect coordination, failover and backup testing. Upstream in-process
PubSub and startup-wide outbox resetting are not safe substitutes for those
mechanisms. Database multiwriter support does not make the current application
an active-active system. No performance gain, HA result, complete database
replacement, or full application port is claimed by this component.

Sources: [SurrealDB architecture](https://surrealdb.com/docs/learn/data-models/architecture),
[SurrealDS commit and scaling model](https://surrealdb.com/docs/manage/enterprise/product/surrealds),
[deployment models](https://surrealdb.com/docs/manage/self-hosted/deployment-models),
[Enterprise licensing](https://surrealdb.com/docs/manage/enterprise/product/licensing),
[transactions](https://surrealdb.com/docs/learn/querying/concepts-and-guides/transactions),
[SQL compatibility](https://surrealdb.com/docs/reference/rest-api/sql-compatibility),
and [Turso sync conflict resolution](https://docs.turso.tech/sync/conflict-resolution).

## Admission and application authority

The Iroh handshake authenticates an endpoint key. A local allowlist decides
whether that key may reach the one configured T3 service. Neither an endpoint
address nor an Iroh ticket grants application authority. The bridge preserves
HTTP headers, request bodies, and WebSocket bytes so the upstream application
still evaluates pairing, sessions, and permissions. Each affected contract needs
an application-level test; byte preservation alone is not policy equivalence.

The backend socket's source is always loopback. Upstream's
`deriveAuthClientMetadata` therefore classifies the connection as local. The
managed remote-client policy in `EnvironmentAuth.ts` exempts some
administrative-bootstrap sessions recorded as loopback from remote-client
revocation. A remote holder of such a credential can retain that exemption
through this bridge. This still requires a valid T3 credential, but it prevents
qualification of the existing managed-policy and audit contracts. Ordinary
scope-limited pairing is the tested path. Completing the integration requires
trusted, authenticated Iroh endpoint metadata at the application boundary;
accepting arbitrary forwarded headers would not establish that trust.

The standalone Iroh host can now read a bounded, owner-controlled endpoint
allowlist using `serve --allow-file PATH`. On Unix, SIGHUP reloads the file
without restarting the QUIC endpoint. A reload parses and validates the complete
file before publishing one atomic policy replacement; syntax, duplicate, size,
permission, and missing-file errors leave the old allowlist in force and are
reported on stderr. Operators must resolve a failed revocation immediately;
failed reloads deliberately do not produce partial authorization changes.
Existing peers retain their generation when they remain authorized. The
actual peer reload control plane is implemented by the native Rust Iroh adapter,
not yet compiled by Lust, and it is not the upstream Connect account UI.

The allowlist denies by default. Each new grant has a unique generation. An
existing connection binds that generation, so revoking and immediately
regranting an endpoint cannot preserve its old connection. Revocation must close
active connections and their backend streams, not only reject the next handshake.

Both the backend target and the local client listener are explicit loopback
addresses. A remote client cannot choose another target, and a grant does not
turn the host into an arbitrary TCP proxy. Connection and stream limits,
handshake deadlines, read/write deadlines, and fixed-size copy buffers bound the
work a peer can create. Stream cancellation and process shutdown must release
backend sockets and connection permits.

The client permission port preserves the distinction between absent permissions
and explicit empty permissions. Only an old client view may expand legacy scope
names. Server grants always use exact permissions. This distinction is checked
against the actual upstream TypeScript functions, rather than a second
handwritten interpretation of their intended behavior.

## Connect includes more than transport

Upstream Connect also manages environment linking and discovery, account and
session flows, mobile device registration and notifications, and webhook
delivery. Its implementation lives in `infra/relay` and the server/client relay
modules. The Iroh byte bridge does not replace those functions. The native client
route must be integrated with the shared connection supervisor, then used by
web, desktop, and mobile entry points. Provider sessions, shell streams, history
pagination, attachments, and permission prompts need application-level coverage
through that route.

The iOS and Android app must own the endpoint lifecycle, secure identity storage,
network-change notifications, reconnect behavior, and foreground/background
transitions. An always-running background socket is not a substitute for the
platform's push delivery mechanism. A browser needs a separately verified Iroh
browser transport or a local companion. A native loopback bridge is not evidence
that the hosted browser app can speak QUIC directly.

## Iroh protocol choices

Use protocols for specific application behavior, with authority scoped to that
behavior. Mounting every available handler on one endpoint would expose data
without proving the caller's T3 permissions.

| Component                     | Intended T3 use                                               | Required boundary                                                                                             |
| ----------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| Iroh endpoint, QUIC, relays   | HTTP/WebSocket transport, direct paths with relay fallback    | Pin peer identity; explicit admission; preserve T3 application authentication                                 |
| Address discovery and tickets | Find and pair an endpoint                                     | Discovery is opt-in; addresses and tickets are not authorization                                              |
| Iroh blobs                    | Attachments, screenshots, large diffs, transferable artifacts | Per-environment storage and grants, validated content, quotas, cancellation, retention and garbage collection |
| Iroh gossip                   | Short-lived presence and invalidation hints                   | Authorized peers, bounded messages, private content protection; never command ordering or durable delivery    |
| Iroh docs                     | Explicitly shared small metadata such as a device catalog     | Separate read/write capabilities, bounded synchronization, revocation and recovery rules                      |
| Browser and mobile bindings   | Embed the transport in each client                            | Real platform builds and lifecycle tests; no inferred support from the Rust core compiling                    |

Blobs, docs, and gossip do not replace T3's authoritative event log, transactional
outbox, provider adapters, or permission checks. Adding an entry to Cargo.toml is
not integration. Each protocol remains incomplete until a T3 action reaches it,
receives the result, and passes the relevant authorization and recovery tests.

Global content announcement and a public DHT are not defaults for source code,
prompts, terminal output, or private artifacts. Media streaming, trackers, and
other optional Iroh protocols need an actual T3 use case before entering the
runtime. None is required merely to use Iroh as Connect transport.

The implemented transport pins Iroh `1.4.1`. The inspected suite versions are
blobs `0.104.1`, gossip `0.102.0`, and docs `0.101.0`. Docs currently requires
blobs `^0.103` and gossip `^0.101`; independently selecting each newest crate
would create incompatible store and protocol types. Choose and test a coherent
version set when the first application storage flow is implemented. Current
`iroh-ffi` exposes core bindings for Swift, Kotlin, Python, and Node, and explicitly
excludes blobs, docs, and gossip. It cannot supply the complete mobile suite by
itself. These protocols are design decisions here, not implemented functionality.

Source basis: [docs dependencies](https://github.com/n0-computer/iroh-docs/blob/8cfeacb087b4b195b1930683aa4448e990da0659/Cargo.toml)
and [FFI scope](https://github.com/n0-computer/iroh-ffi/blob/66a8d619a4d791d458d582f312df6e0ab42bc541/README.md).

## Upstream maintenance

The tracker records source inventories separately from qualification. A new
published release produces a complete changed-file report, including files
outside the named application areas. Review all changes, update affected ports,
and re-run their compatibility tests. Do not advance a qualified revision while
only updating its source pin.

The fork's integration branch is `main`. Maintenance checks run every two hours
at minute 08, aligned with one of upstream's check times.
The upstream release workflow checks twice an hour, requires at least six hours
since the last published nightly, and skips releases without new commits. A
two-hour poll follows the requested cadence, but is not an exact release event
trigger. Only a published nightly advances the source baseline.

For nightly `v0.1.8-nightly.20261108.4380` at immutable commit
`ade473018d32003658b7d38693e083ee2e314bd8`, source integration included
[merged upstream #21524](https://github.com/pingdotgg/t3code/pull/21524):
`mise`-installed provider upgrades must use the verified owning `mise` tool;
pinned versions, fixed installations and ambiguous wrappers must remain manual.
This provider-maintenance resolver and its tests are upstream TypeScript, not
Lust-compiled application code. Next implement the same ownership and upgrade
policy as a pinned-Lust executable with comparisons against the immutable
upstream function and an injected production maintenance service boundary.
Full server, all provider sessions, Connect authorization and control plane,
trusted Iroh remote-client metadata, Iroh blobs/docs/gossip, UI, desktop,
iOS and Android remain unqualified until their actual app/platform gates run.

PRs and issues provide advance notice and regression evidence. Track their
updates with an overlapping cursor window and complete pagination. Do not
advance after a failed or truncated scan. Deferred items must retain their own
earlier event boundaries while other items progress. An unmerged PR, an issue report, or a
passing PR check is not a released compatibility contract. Review relevant code,
tests, discussions, and eventual release inclusion before incorporating changes.
Prepare updates on candidate branches and merge into `main` only after the
applicable checks pass, preserving independent fork fixes and branch protections.

The transport, permission oracle, Lust checks, and portable-core builds are
necessary gates for this native implementation. Full nightly parity also needs
the upstream application tests, real provider protocol fixtures, real client
flows, mobile lifecycle checks, installation/update/rollback tests, and signed
packages under the fork's identity. Their absence must remain visible in the
qualification state.
