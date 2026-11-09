# t3 code ls

glopyglerky's T3 Code fork with native Rust components, selected Lust-built
application components, and a Cargo-built Iroh transport. Start with the
[fork README](../../README.md) for application entry points and the source map.
Full conversion and 100% memory-safe Rust using Lustr/BlueFe are future goals;
working application builds take priority.
This is an implementation in progress. It is not a complete replacement for the T3
desktop, web, iOS, or Android applications.

The source baseline is the published nightly recorded in
[upstream.lock.json](./upstream.lock.json). Its `qualification` field stays
unqualified until the full application passes its compatibility gates. Tracking
a newer source revision does not establish feature parity.

## Current code

The portable core ports the nightly's permission vocabulary, explicit grant
precedence, legacy client compatibility, permission error projection, and the
connection retry decision policy. The comparison tools execute the original
TypeScript functions from the immutable nightly Git commit. They compare 28,560
permission decisions, 50,025 retry delay cases, 2,001 first-session-loss delays,
3,360 retry transitions, and 30 wakeup decisions with the Lust-compiled port.
Client compatibility rules and exact server grants have separate APIs so an
old-client fallback cannot broaden server authority.

Provider capability decisions also run in a standalone Lust executable. They
cover queued turns, steering, interrupts, native forks, rollback, and context
handoff. The frame-bounded `t3-code-ls-policy` executable has a fail-stop
subprocess protocol; its oracle compares all 688,128 capability and command
combinations against the immutable nightly implementation. The server's actual
`CommandPolicyV2` service selects this executable through
`T3_CODE_LS_POLICY_EXECUTABLE`. A temporary TypeScript adapter translates the
existing service contract while the policy decisions execute in Lust.

The same executable owns message-delivery precedence (queue, steer, restart,
and immediate start) and goal-command stop decisions. The orchestrator awaits
these operations through `CommandPolicyV2`, including both goal-stop paths.
The temporary adapter still joins projection records and classifies maintenance
messages; it sends only bounded capability/status facts, not prompts or IDs.
The Lust resolver preserves absent versus explicit delivery intent, the requested
versus latest-active target, preparation gaps, missing sessions, and maintenance
commands. The dispatch oracle compares 46,430 cases with the pinned upstream
implementation through the production fact adapter. This is not a port of the
complete orchestrator, provider lifecycle, persistence, or UI.

The guest-share decision port covers opening, redemption, expiry, submission,
owner sending, dismissal, cancellation, attachment release, and pending-message
and attachment limits. `T3_CODE_LS_GUEST_SHARE_EXECUTABLE` selects these decisions
in the actual orchestrator, including attachment cleanup after archiving and
removal of forged guest attribution. The native response distinguishes a real
change from an unchanged result so the server preserves event and receipt
semantics. Expiration-string parsing remains the date adapter's responsibility;
the port preserves its parsed result and the original string. Guest HTTP
authorization, signing, uploads, streaming, and Connect hosting remain upstream
implementations.

The Unix provider process runtime runs JSON-RPC over a child process's standard
streams. It is compiled and executed by Lust, including its bounded JSON codec,
request tracking, notifications, approval requests, and owned-child cleanup.
Controlled subprocess fixtures exercise its behavior without paid provider turns
or personal credentials. `T3_CODE_LS_PROVIDER_EXECUTABLE` selects it for the actual
Codex session factory and the provider, account, and skills probes. Generated
Codex payload schemas, provider-to-event translation, orchestration persistence,
and application UI retain their upstream implementations. Codex's separate
`exec` text-generation path has its own selection described below. Other
provider adapters are not ported by this process-runtime selection.

Codex structured generation runs through `t3-code-ls-generate` when
`T3_CODE_LS_GENERATION_EXECUTABLE` is set. This covers the actual title, branch,
commit, PR, transcription-cleanup, and review-guide operations. Lust owns model
fallback, launch arguments, MCP discovery and disabling, provider execution,
private schema/output files, and cleanup. The host retains runtime and credential
selection, attachment authorization, prompt construction, home-path expansion,
and final contract decoding.

Provider installation ownership and update-command construction run through
`t3-code-ls-maintenance` when `T3_CODE_LS_MAINTENANCE_EXECUTABLE` is set. The
shared package-managed, mise, and Homebrew resolvers preserve installer priority,
manual-only installations, verified prefixes, active mise ownership, and lazy
version lookups. The host retains provider-specific predicates, filesystem and
process access, semver facts, version parsing, caching, and the update runner.
The integration gate executes that real runner with controlled update processes.

The recovery planner ports orphaned native subagent and runless root-turn
completion. Its oracle compares complete event batches, allocator failures, and
replayed recovery cases against the immutable upstream functions using the real
Effect runtime. `T3_CODE_LS_RECOVERY_EXECUTABLE` selects the Lust executable for
startup and shutdown recovery and the live orchestrator's subagent completion
and stop flows. The existing event sink still owns the SQLite transaction,
projection updates, command receipt, and outbox. The integration checks cover
durable replay, repeated recovery, failure rollback, and child cleanup.

Project creation, metadata updates, and deletion planning run in the Lust
`t3-code-ls-project` executable when selected through
`T3_CODE_LS_PROJECT_EXECUTABLE`. The port preserves workspace conflicts, exact
domain rejections, legacy script IDs, optional and null metadata, and pin/archive
timestamps. Its oracle executes the immutable nightly planner and its real
contract validators. The production project service still owns locks, enrichment,
SQLite events, projections, and command receipts. Unicode grapheme segmentation
remains an explicit adapter input for the monogram-length rule.

The native persistence component embeds redb 4.5.0, a Rust database licensed
under MIT or Apache-2.0. Independent local application processes share one
file through redb's MultiWriter mode. A revision check commits events, projections, command
receipts, and outbox entries atomically, and rejects stale plans. The new
`t3-code-ls-project-store` executable plans project commands and projects their
rows through Lust. The Cargo-built `t3-code-ls-db` host validates the complete
response and persists native project creation and metadata updates. This is a
standalone native storage path; the existing production server still uses SQLite.
Its thread lifecycle and transaction participants must migrate together before
cutover. Native project deletion is explicitly unavailable until that work lands.
The native outbox now has indexed claims, owner-checked completion/retry,
cancellation, process-loss reconciliation, and retention. Its portable policy
is qualified through Lustr and called by the Cargo database host. The production
TypeScript effect worker remains connected to SQLite.

The native transport carries a fixed loopback service's HTTP, WebSocket, and
binary traffic over authenticated Iroh connections. Each host explicitly admits
client endpoint identities. Admitting a peer does not issue a T3 application
session, change a user's permissions, or replace T3 pairing. The transport uses
the core's peer policy and closes admitted connections when their grant is
revoked, including a revoke immediately followed by a new grant.

The rest of the T3 server and clients remain upstream implementations. The
Connect account flow, client route selector, mobile lifecycle, notifications,
shared storage, and native UI are not completed by forwarding network traffic.

## Build the Iroh bridge

Use Rust 1.99.1. The Iroh adapter uses async Rust, Tokio, and dependencies that
current Lust cannot compile. Cargo and rustc build this bridge.

```sh
cargo build --locked --release --manifest-path native/t3-code-ls/Cargo.toml
native/t3-code-ls/target/release/t3-code-ls --help
```

The CLI stores a raw 32-byte endpoint identity at the path you supply. On Unix it
creates owner-only files and rejects existing symlinks, multiple hard links, and
group/world permissions. Keep keys in a private directory. The CLI file storage
is not an iOS Keychain, Android Keystore, or Windows credential-store integration.
Applications embedding the library must provide an identity from their platform
key store.

## Use the Lust provider policy in the server

On Linux x86-64, use `target/lust-bin/t3-code-ls-policy` produced by the
[independent Lust gate](#verification). After installing the existing
application's dependencies, check the actual service integration from
`native/t3-code-ls` without starting a provider or accessing personal T3 state:

```sh
node tools/check-policy-adapter.mjs "$PWD/target/lust-bin/t3-code-ls-policy"
node tools/check-dispatch.mjs "$PWD/target/lust-bin/t3-code-ls-policy" --service
```

To select the executable in a development server, run from the repository root:

```sh
T3CODE_DEV_SEED=0 \
T3_CODE_LS_POLICY_EXECUTABLE="$PWD/native/t3-code-ls/target/lust-bin/t3-code-ls-policy" \
vp run dev
```

Only an unset variable selects upstream's original policy. An empty, relative,
missing, failing, or malformed native executable never silently selects the
original policy. The adapter uses an absolute executable path without a shell,
passes only bounded capability and status facts, does not inherit the server's
environment, and bounds execution time and output. Existing application
authentication and permission checks are unchanged. Each policy call uses a
separate process; this boundary is not a qualified performance improvement or a
full server port. The CI executable artifact is Linux x86-64 only and is not a
desktop or mobile application package.

## Use the Lust process runtime for Codex sessions

After building the verified Linux components, select the provider executable in
the server's environment. From the repository root:

```sh
T3CODE_DEV_SEED=0 \
T3_CODE_LS_PROVIDER_EXECUTABLE="$PWD/native/t3-code-ls/target/lust-bin/t3-code-ls-provider" \
vp run dev
```

This selection applies to both live Codex sessions and discovery probes. It is
independent of `T3_CODE_LS_POLICY_EXECUTABLE`; set both to use both components.
Only an unset selector preserves the original launcher. Invalid configuration
or a failing selected executable fails the operation, including the diagnostic
version probe. Provider-specific environment settings cannot select a different
runtime executable.

The adapter preserves the selected provider environment, working directory,
launch arguments, and separate `HOME` and `CODEX_HOME` values. Credentials remain
in the environment rather than command arguments. Existing agent process scopes
wrap the Lust launcher and its child. T3's Effect client owns request correlation,
request cancellation, and deadlines; parent-protocol mode forwards provider frames
without retaining a second pending-request table. Provider exit codes and stderr
continue through T3's existing process-error handling. Invalid native frames and
exceeded native queue or frame limits fail the connection.

The integration checks run the actual session factory, probe, generated codecs,
and Node process service with controlled provider peers. They cover request and
approval exchanges, notifications, cancelled requests on a continuing session,
terminal errors, and scope cleanup. They do not establish a complete server,
provider conversation, or client application port.

## Use the Lust Codex generation runner

Build the verified Linux components and set the server's selector:

```sh
T3CODE_DEV_SEED=0 \
T3_CODE_LS_GENERATION_EXECUTABLE="$PWD/native/t3-code-ls/target/lust-bin/t3-code-ls-generate" \
vp run dev
```

The selector comes from the server environment before provider-specific or
managed-account overlays. The selected process receives those effective provider
credentials through its environment, while the bounded request travels over
standard input. Invalid selection or native failure fails the operation. Only
an unset selector uses the original TypeScript runner.

The native runner uses a private directory with mode `0700` and schema/output
files with mode `0600`. It removes them after success, error, timeout, or handled
interruption. The output must be a regular file; symbolic links and FIFOs are
rejected. The runner closes its owned socket endpoints and kills and reaps its
child on interruption, including when a descendant holds a descriptor open.

Limits are 8 MiB for the complete JSONL request, 4 MiB for the prompt and output
file, 1 MiB for the schema and MCP listing, 4 MiB of execution stdout, and 1 MiB
of stderr. JSON has a maximum depth of 64 and 262,144 values in the request;
model-catalog, selected-option, and image arrays contain at most 4,096 entries each. The default
operation deadline is 180 seconds, or 15 minutes for a review guide. An MCP probe
gets at most 10 seconds within that deadline. The native request and MCP JSON
require Unicode scalar strings. Invalid UTF-8 in those inputs or the output file,
and resource-limit violations, fail closed. The host decodes generated JSON with
the existing Effect schema and JavaScript string semantics; exit diagnostics use
upstream's replacement decoding. Ordinary MCP probe failure retains upstream's
behavior of proceeding without named-MCP overrides.

This path requires Unix process support. Linux x86-64 is executed through the
pinned Lustr compiler; Windows and mobile native generation are not qualified.
The integration checks use controlled CLI processes and do not spend provider
credits or perform a real generation.

## Use the Lust provider-maintenance resolver

Build the verified Linux components, then select the executable in the server's
environment. From the repository root:

```sh
T3CODE_DEV_SEED=0 \
T3_CODE_LS_MAINTENANCE_EXECUTABLE="$PWD/native/t3-code-ls/target/lust-bin/t3-code-ls-maintenance" \
vp run dev
```

Only an unset selector retains the original shared resolver. Invalid selection,
process failure, or an invalid action fails maintenance resolution. Each
resolution uses one scoped process with an empty environment and no shell.
The native process requests typed observations; the host runs the corresponding
bounded probes and returns their results. It never sends arbitrary provider
environment values to the policy process. The existing runner still refreshes
ownership before updating and refuses a changed installation.

Frames, including the terminating LF, are limited to 1 MiB and a resolution's
input to 16 MiB. Protocol JSON depth is limited to 32, values to 131,072, and observations
to 4,096. Resolution has a 60-second deadline. Wrapper inspection retains the
16 KiB limit and package-manager probes retain the 256 KiB output limit. Native
resource-limit failures reject the operation instead of treating incomplete
evidence as permission to use another updater. Strings require Unicode scalar
values; unsupported Unicode in an install listing also fails resolution.

This selection reaches the common resolvers used by Codex, Claude, OpenCode,
Pi, Grok, and Muse. It does not port their complete drivers, version advisories,
or installation execution. The compiled artifact is qualified on Linux x86-64;
Windows decision fixtures do not establish a Windows native runtime build.

## Use the Lust recovery planner

Build the verified Linux components, then select the recovery executable in the
server's environment. From the repository root:

```sh
T3CODE_DEV_SEED=0 \
T3_CODE_LS_RECOVERY_EXECUTABLE="$PWD/native/t3-code-ls/target/lust-bin/t3-code-ls-recovery" \
vp run dev
```

The selector is independent of the policy and provider selectors. Only an unset
value retains upstream recovery planning. An empty or relative path, a failed
process, or an invalid response fails the operation. The planner receives its
projection on standard input with an empty environment and no shell. It returns
event drafts without IDs. The adapter validates every draft with the real event
schema and its source scope before allocating IDs, then the caller commits the
batch through the existing event sink. Recovery uses one native process per
thread; shutdown retains the existing grouped transaction and rolls back the
group when a planner fails.

Each request and response is limited to 128 MiB, each response to 65,536 events,
and execution to 30 seconds. Native parsing also bounds nesting and value count,
rejects duplicate object keys, and requires Unicode scalar strings. Interruption
closes and reaps the owned process before returning. The verified artifact is
Linux x86-64; other hosts need their own compilation and runtime qualification.
This ports recovery decisions while the persistence engine remains TypeScript.

## Use the Lust guest-share decisions

After building the verified Linux components, select the guest-share executable
in the server's environment. From the repository root:

```sh
T3CODE_DEV_SEED=0 \
T3_CODE_LS_GUEST_SHARE_EXECUTABLE="$PWD/native/t3-code-ls/target/lust-bin/t3-code-ls-guest-share" \
vp run dev
```

The build also includes `t3-code-ls-guest` as a byte-identical compatibility
filename for `t3-code-ls-guest-share`. Deploy the server and verified components
together so their wire formats match.

Only an unset selector retains the upstream decision functions. A failed selected
executable rejects the operation. The adapter validates the complete result
before the caller commits domain events or attachment-cleanup requests. Failed
commands retain the existing rejected-receipt behavior. Accepted changes and
cleanup requests commit through the existing event sink, and the effect worker
deletes released files after that commit.

The process receives only the data needed by the selected decision, with an
empty environment and no shell. Requests and responses are limited to 128 MiB
each and execution to 30 seconds. The native JSON parser limits nesting and value
count, rejects duplicate keys, and requires Unicode scalar strings. This
component preserves the existing guest link and provider behavior; links still
use the existing Connect host and guest service. The verified executable is
Linux x86-64, and is independent of the other native selectors.

## Use the Lust project planner

Build the verified Linux components, then select the project executable in the
server's environment. From the repository root:

```sh
T3CODE_DEV_SEED=0 \
T3_CODE_LS_PROJECT_EXECUTABLE="$PWD/native/t3-code-ls/target/lust-bin/t3-code-ls-project" \
vp run dev
```

Only an unset selector keeps the upstream planner. A failed selected executable
fails the operation. The adapter validates the response schema, command and
project identity, event payload, timestamps, and rejection association before
the existing service commits it. Accepted and rejected receipts keep the
upstream retry behavior. The process receives bounded JSON on standard input,
an empty environment, and no shell; each request and response is limited to
128 MiB and execution to 30 seconds.

Forced project deletion preserves upstream's durable sequence: child thread
deletions commit before the project deletion is planned. A native planning
failure can therefore leave already-deleted children while the project remains.
Retry continues from that state without repeating their events or cleanup
requests. It does not roll back the entire deletion cascade.

The service integration gate exercises the real production layer, SQLite event
sink, project store, command receipts, and thread deletion path. Storage,
workspace normalization, repository enrichment, and client UI remain upstream
implementations. The verified executable is Linux x86-64.

## Run the native database component

The engine version, source identity, license, and crate checksum are pinned in
[database.lock.json](./database.lock.json) and `Cargo.lock`. redb is embedded in
the host executable; it needs no database daemon, network endpoint, or paid
feature. Multiple local processes can open the same file using the pinned
`MultiWriter` mode. redb serializes write transactions. This is local process
concurrency, not replicated or distributed active-active HA.
See the [database decision](../../docs/internals/t3-code-ls.md#database-decision).

From `native/t3-code-ls`, build the host and use the Lust-built projector
from the verification gate:

```sh
cargo build --locked --release -p t3-code-ls-storage --bin t3-code-ls-db
export T3_CODE_LS_PROJECT_STORE_EXECUTABLE="$PWD/target/lust-bin/t3-code-ls-project-store"
printf '%s\n' '{"operation":"snapshot"}' | \
  target/release/t3-code-ls-db initialize-local /absolute/path/to/new-state.redb
```

Use a dedicated new file on a local filesystem. Subsequent clients open it with
`local /absolute/path/to/new-state.redb`. Initialization is explicit; ordinary
open does not create or reset a store. Existing SQLite files and the prior
native engine's data are not automatically converted. All cooperating processes
must use the pinned redb version: its multiprocess locking is incompatible with
the experimental modes in redb 4.3 and 4.4. Do not put the file on a network
filesystem or copy a live file as a backup.

Every open verifies the committed tree checksums before exposing application
data. That scans the database and holds the writer lock during verification;
existing readers retain their snapshots. Commits explicitly use redb's
`Immediate` durability. The gate kills every
process holding the database and verifies acknowledged state after reopening.
That verifies process-crash recovery; it does not simulate power loss or qualify
the storage device's flush behavior.

The CLI consumes one JSON request per line and emits one JSON response per
line. A native project command looks like this:

```json
{
  "operation": "project",
  "request": {
    "command": {
      "type": "project.create",
      "commandId": "create-1",
      "projectId": "project-1",
      "title": "Example",
      "workspaceRoot": "/workspace/example"
    },
    "event_id": "event-1",
    "now": 1794139200123
  }
}
```

`project.meta.update` uses the same decoded command fields as the upstream
planner. The caller supplies the timestamp and event ID. The service computes
Unicode grapheme counts, validates the selected executable's complete event and
projection, and stores both accepted and rejected command outcomes. Reusing a
command ID with different intent fails. Retrying the same intent reads its
original receipt before looking at a new snapshot; timestamps and event IDs
allocated for the retry do not replace the committed result.

The caller must first apply the application's workspace-path normalization.
This low-level service trims contract strings and compares workspace paths
exactly; it does not resolve relative paths, `..`, aliases, or symlinks, or
perform the production service's repository enrichment.

The `snapshot`, `receipt`, `events`, and `pending_effects` operations expose
bounded reads. `commit` is a trusted application API for an atomic batch at an
expected revision, not an authorization boundary for end users. Every writer
must use the transaction protocol. Conflicts require a fresh snapshot and
replanning. An unknown commit outcome requires looking up the original receipt
before retrying; the client never blindly repeats an uncertain commit.

The initial revision guard serializes successful commits across the store. It
protects workspace ownership and other decisions based on absence as well as
existing rows. Snapshot size is limited to 16 MiB and 65,536 projections, batch
size to 16 MiB, and event/effect pages to 4,096 records and 16 MiB. Project planning
reads only the indexed project key range, so unrelated projections do not consume
its snapshot allowance. The project
planner has a 30-second deadline. Performance improvements require equivalent
workload measurements; this component does not yet execute provider effects.

### Durable orchestration outbox

Typed worker effects are separate from generic immutable `Record` effects.
They preserve all five statuses, attempt counts, timestamps, error details, and
worker ownership. The obsolete SQLite `lease_expires_at` field remains null;
these are owner checks, not expiring distributed leases.

| JSONL operation                                                      | Result                                                                   |
| -------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `outbox_enqueue` (`effects`, `now`)                                  | Insert-if-absent count; duplicate IDs retain the existing row            |
| `outbox_get` (`effect_id`) / `outbox_list` (`command_id`)            | Complete row or command's ordered rows                                   |
| `outbox_claim` (`worker_id`, `now`, `exclude_restart_continuations`) | Claimed row or null                                                      |
| `outbox_claim_once` (`stream_id`, `sequence`, plus the claim fields) | Recoverable claimed, empty, or retired result                            |
| `outbox_next_claimable_at`                                           | Earliest eligible timestamp or null                                      |
| `outbox_succeed` / `outbox_fail` / `outbox_retry`                    | Boolean indicating an owner-checked transition                           |
| `outbox_cancel` (`thread_id`, `effect_types`, `reason`, `now`)       | Cancelled pending/running IDs in insertion order                         |
| `outbox_reconcile` (`now`)                                           | Requeued/cancelled counts after process loss                             |
| `outbox_prune` (`before`, `limit`)                                   | Number of succeeded/cancelled rows removed                               |
| `commit_outbox` (`batch`, `outbox`)                                  | Atomic event/projection/receipt commit plus enqueue/cancellation results |
| `outbox_receipt` (`command_id`)                                      | Original commit and worker-mutation results, or null                     |

`outbox_succeed` takes `effect_id`, `worker_id`, and `now`; failure also takes
`error`, and retry takes `error` and `available_at`. The host supplies canonical
UTC strings with milliseconds (years 0000–9999), including a retry deadline at
or after `now` and the seven-day retention cutoff used by production. Reads and
mutations reject work exceeding 16 MiB or 65,536 records without partial writes.
The transaction budget includes index keys and accumulated observations across
all stages, and bounds expanded stored values as well as the input request.
The 65,536 observation limit counts internal index reads too, so an operation
may reach it with fewer logical effects.
Pruning accepts a limit from 1 to 500; repeat batches to drain eligible rows.

`commit_outbox` accepts the existing `CommitBatch` as `batch`, and
`{ "enqueue": [], "enqueue_at": "2026-11-08T12:00:00.000Z", "cancel": [] }`
as `outbox`. Each enqueue supplies `id`, `command_id`, `thread_id`, `request`,
and optional `available_at`; cancellations use the `outbox_cancel` fields.
It enqueues before cancelling, matching EventSink. Retries reuse the original
outbox envelope and retrieve its stored results even after workers have advanced
those effects. Use `outbox_receipt` to resolve an uncertain combined commit.
Generic `commit` and `commit_outbox` cannot share a receipt ID.

Workers use `outbox_claim_once`. Each worker incarnation creates a unique
`stream_id` and starts `sequence` at 1. Keep the exact request, including `now`,
until its outcome is resolved. The same sequence and request recovers the
original result; only the next sequence can claim new work. Changed inputs,
skipped sequences, older sequences, and changed worker ownership fail. One
compact cursor per stream records the outcome without duplicating its payload
or accumulating receipts on every idle poll. Cursors survive effect pruning.

A recovered `claimed` result is executable only while its insertion ordinal,
attempt, and owner still match a running effect. Otherwise the response is
`retired`, with no effect to execute. An empty result stays empty even if work
arrives later. Keep the same stream when reconnecting the database subprocess;
use a new stream only for a new worker incarnation. The legacy `outbox_claim`
operation has no recovery receipt and must not be blindly retried.

The injectable `NativeEffectOutbox` server layer drives the existing effect
worker through a persistent, bounded `NativeDatabase` subprocess. It retains an
unresolved request across transport failures and serializes claims while effect
execution remains concurrent. It refuses claim-mode changes until that worker's
pending request is resolved. Unknown settlement responses trigger a durable
state read to wake applicable waiters, without repeating the effect execution.
The integration gate uses controlled effect executors and isolated redb files.
There is no production database selector yet; the server still uses SQLite.

Each thread has a title-generation lane and an ordinary-work lane. An earlier
pending effect blocks later work in its lane even while waiting for retry;
running work also blocks that lane. Claims order eligible rows by availability,
creation time, and ID. Reconciliation cancels process-bound provider operations
and requeues running replay-safe work. Run it only after the previous server's
workers have stopped; calling it while another server is active can duplicate
side effects. Future active-active integration needs worker fencing and durable
cross-node notifications. Production activation must move the shared event,
projection, receipt, outbox, and consistent-read transaction domain together.

### Preserve existing SQLite data

The exporter reads a consistent SQLite snapshot without modifying the source:

```sh
node --disable-warning=ExperimentalWarning tools/export-sqlite-state.mjs \
  /absolute/path/to/source.sqlite /absolute/path/to/fresh-export.ndjson
```

It preserves schema SQL, migration records, AUTOINCREMENT high-water marks,
signed 64-bit integers, binary floating-point representations, blobs, text,
and explicit nulls. Only the known T3 contentless message-search index and its
SQLite-identified shadow tables are excluded, after validating the migration's
schema and backing message table. Other virtual tables, including FTS tables
that might hold the only copy of their text, fail explicitly. The default
export/import ceiling is 512 MiB; the library supports
an explicit ceiling up to 4 GiB. The exporter never overwrites its destination.

Send `{"operation":"archive_sqlite","path":"/absolute/path/to/fresh-export.ndjson"}`
to `t3-code-ls-db`. The importer verifies every record and checksum, stores
immutable chunks, and publishes the archive only after a second complete read
and stored-chunk verification. Interrupted imports are retryable. Send
`{"operation":"restore_sqlite_export","archive_id":"RETURNED_ID","destination":"/absolute/path/to/fresh-recovered.ndjson"}`
to recover the exact verified export bytes without overwriting a file.

This recovery archive does not activate imported rows as native projections,
rebuild a live SQLite database, or change the application's database selector.
The original SQLite data remains the application's authority until a complete
repository migration, read comparison, and cutover/recovery procedure are
implemented. The exporter is a Node SQLite adapter and the database host is
Cargo-built Rust; neither is qualified as Lust code.

## Connect two machines

The bridge currently makes every backend connection from loopback. Upstream
therefore records remote devices as local TCP clients. Its IP-based managed
policy treats some administrative-bootstrap sessions differently for loopback
clients; this bridge has not preserved that policy boundary. Use ordinary,
scope-limited pairing for experiments, never distribute the startup
administrative-bootstrap credential to remote peers, and do not rely on the
upstream remote-client restriction through this adapter. A trusted endpoint
metadata integration is required before this route can be qualified for use.

Keep the existing T3 server running with authentication enabled. Use its actual
loopback port in place of `3773` below. A peer grant exposes the configured local
service to that peer, so only target an authenticated T3 server.

Create an identity on each machine. The command prints the public endpoint ID.

```sh
t3-code-ls identity --identity ./client.key
t3-code-ls identity --identity ./host.key
```

On the T3 host, grant the client's public ID and expose the one configured service:

```sh
t3-code-ls serve --identity ./host.key --target 127.0.0.1:3773 \
  --allow CLIENT_PUBLIC_ID --public-discovery
```

On the client, pin the host's public ID and open a loopback forwarder:

```sh
t3-code-ls forward --identity ./client.key --remote HOST_PUBLIC_ID \
  --listen 127.0.0.1:3774 --public-discovery
```

Open `http://127.0.0.1:3774` on that client. Complete normal T3 pairing using the
forwarded origin. A pairing URL's credential must remain secret; the bridge does
not generate one or print one. The public IDs above are placeholders, never
private keys.

`--public-discovery` explicitly opts into Number 0's public relay and discovery
infrastructure. To use your own relay instead, supply `--relay https://YOUR_RELAY`
on each endpoint and `--remote-relay https://HOST_RELAY` to the forwarder. Without
either routing option, the bridge uses direct addresses only. Pass the host's
reported Iroh UDP address with `--address IP:PORT` when using direct mode.
Neither mode configures a router or opens a firewall rule.

For isolated validation on one machine, pass `--loopback` to both commands and
the host's reported address to `forward --address`. This binds Iroh itself to
loopback and does not contact a relay or publish endpoint addresses.

The forwarder's local port is a transport path, not an authentication boundary.
Other processes on that client can reach it and still need a valid T3 session.
A phone needs the embedded native bridge on the phone itself. A loopback address
on another computer cannot make the unmodified store app use Iroh.

## Verification

```sh
cd native/t3-code-ls
node tools/install-server-tests.mjs
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
node --disable-warning=ExperimentalWarning tools/check-permissions.mjs
node --disable-warning=ExperimentalWarning tools/check-connection.mjs
node --disable-warning=ExperimentalWarning tools/check-policy.mjs
node --disable-warning=ExperimentalWarning tools/check-dispatch.mjs
node --disable-warning=ExperimentalWarning tools/check-guest-share.mjs
cargo build --locked -p t3-code-ls-core --bin t3-code-ls-guest-share
node --disable-warning=ExperimentalWarning tools/check-guest-share-runtime.mjs target/debug/t3-code-ls-guest-share
cargo build --locked -p t3-code-ls-core --example project_commands_matrix
node --disable-warning=ExperimentalWarning tools/check-project-commands.mjs target/debug/examples/project_commands_matrix
cargo build --locked -p t3-code-ls-core --bin t3-code-ls-project
node --disable-warning=ExperimentalWarning tools/check-project-runtime.mjs target/debug/t3-code-ls-project
cargo build --locked -p t3-code-ls-core --example project_persistence_matrix --bin t3-code-ls-project-store
node --disable-warning=ExperimentalWarning tools/check-project-persistence.mjs target/debug/examples/project_persistence_matrix target/debug/t3-code-ls-project-store
cargo build --locked -p t3-code-ls-core --example provider_recovery_matrix
node --disable-warning=ExperimentalWarning tools/check-provider-recovery.mjs target/debug/examples/provider_recovery_matrix
cargo build --locked -p t3-code-ls-core --bin t3-code-ls-recovery
node --disable-warning=ExperimentalWarning tools/check-recovery-runtime.mjs target/debug/t3-code-ls-recovery
node tools/check-json.mjs
cargo build --locked -p t3-code-ls-core --bin t3-code-ls-provider
node tools/check-provider.mjs target/debug/t3-code-ls-provider
python3 -B -m unittest discover -s tools -p 'test_*.py'
```

For the independent Lust gate, check out `glopyglerky/lustr` at the exact revision
in [lust-toolchain.json](./lust-toolchain.json), then run:

```sh
python3 tools/check-lust.py --source /absolute/path/to/lustr --artifacts target/lust-bin
tools/server-test-runtime/node_modules/.bin/effect-tsgo tsrs --noEmit --project tools/provider-tsconfig.json
node tools/check-provider-adapter.mjs "$PWD/target/lust-bin/t3-code-ls-provider"
tools/server-test-runtime/node_modules/.bin/effect-tsgo tsrs --noEmit --project tools/recovery-tsconfig.json
node tools/check-recovery-adapter.mjs "$PWD/target/lust-bin/t3-code-ls-recovery"
tools/server-test-runtime/node_modules/.bin/effect-tsgo tsrs --noEmit --project tools/guest-share-tsconfig.json
node tools/check-guest-share-adapter.mjs "$PWD/target/lust-bin/t3-code-ls-guest-share"
tools/server-test-runtime/node_modules/.bin/effect-tsgo tsrs --noEmit --project tools/project-tsconfig.json
node tools/check-project-adapter.mjs "$PWD/target/lust-bin/t3-code-ls-project"
tools/server-test-runtime/node_modules/.bin/effect-tsgo tsrs --noEmit --project tools/maintenance-tsconfig.json
node tools/check-maintenance-adapter.mjs "$PWD/target/lust-bin/t3-code-ls-maintenance"
tools/server-test-runtime/node_modules/.bin/effect-tsgo tsrs --noEmit --project tools/generation-tsconfig.json
node tools/check-generation-adapter.mjs "$PWD/target/lust-bin/t3-code-ls-generate"
```

The gate builds the pinned compiler and TinyCC, checks the unchanged core, runs
its unit tests through Lust, and compiles and executes the application oracles
and provider and recovery subprocess fixtures. It rejects a dirty or wrong compiler checkout,
ignored tests, source changes during verification, and mismatched behavior. The
receipt records hashes of the exact Git blobs, binaries, and command results at
`target/lust-proof.json`. There is no Cargo fallback for these Lust checks.

The full Rust database host and Iroh adapter use the pinned Rust toolchain.
A [blueFe](https://github.com/glopyglerky/blueFe) compiler must be a produced
stage1 sysroot with its source identity, standard libraries, test harness, and
rustdoc. Select its `RUSTC` and `RUSTDOC` explicitly through the Lustr coordinator
and rerun the same crate and process gates before adopting it. The downloaded
stage0 compiler and a successful compile alone do not qualify that route.
No build-speed improvement is claimed for an unmeasured compiler selection.

The dedicated CI additionally typechecks the server-policy integration and
executes the actual `CommandPolicyV2` layer with the real Effect runtime. Those
checks cover native selection, success and refusal mapping, concurrent calls,
invalid configuration, malformed responses, failed subprocesses, environment
isolation, and pre-execution cancellation. Dispatch checks also verify the
orchestrator's three service call sites and parse its TypeScript syntax. These
are service and call-site integration checks, not a complete orchestrator,
application, provider-session, browser, or mobile qualification.

After a successful run, `target/lust-bin` contains the standalone Linux x86-64
`t3-code-ls-policy`, `t3-code-ls-provider`, `t3-code-ls-recovery`,
`t3-code-ls-guest-share`, `t3-code-ls-project`, `t3-code-ls-project-store`,
`t3-code-ls-outbox`, `t3-code-ls-maintenance`, and `t3-code-ls-generate` executables.
CI retains these verified components and their proof receipts as an artifact,
including dispatch, SQLite recovery, guest-share, maintenance, and generation integration receipts.
They are development components, not desktop or mobile installers.

The database gate uses real redb files and independent host processes:

```sh
cargo clippy --locked -p t3-code-ls-storage --all-targets -- -D warnings
cargo test --locked -p t3-code-ls-storage --all-targets -- --test-threads=2
cargo build --locked -p t3-code-ls-storage --bin t3-code-ls-db
node --disable-warning=ExperimentalWarning --test tools/export-sqlite-state.test.mjs
node --disable-warning=ExperimentalWarning tools/check-database.mjs \
  "$PWD/target/debug/t3-code-ls-db" "$PWD/target/lust-bin/t3-code-ls-project-store" \
  "$PWD/target/database-proof.json"
node --disable-warning=ExperimentalWarning tools/check-outbox.mjs \
  "$PWD/target/debug/t3-code-ls-db" "$PWD/target/lust-bin/t3-code-ls-outbox" \
  "$PWD/target/outbox-proof.json"
```

The dedicated CI database job consumes the exact Lust artifact from the same
workflow run and retains the native database client plus its execution receipt.
It checks independent writer processes, rollback, command replay, ownership
conflicts, malformed selected planners, process-crash recovery, and verified
SQLite archive recovery. The projection oracle compares only rows accepted by
the real upstream persistence schemas and reports planner-only fixtures outside
those schemas separately; the original complete planner comparison remains.
The outbox oracle executes immutable upstream `EffectOutbox.ts` with actual
SQLite migrations and a controlled Effect clock, then compares full rows and
operation results with redb and the Lust-built lifecycle policy. It covers all
effect types and statuses, FIFO/backoff, separate title work, ownership,
cancellation, recovery, and retention. Independent-process checks cover claim
contention and restart after acknowledged work survives `SIGKILL`.

The provider component accepts Codex app-server JSONL on standard input and
returns responses, notifications, and approval requests on standard output:

```sh
target/lust-bin/t3-code-ls-provider \
  --program /absolute/path/to/codex \
  --cwd /absolute/path/to/project \
  --home /absolute/path/to/provider-home \
  --env PATH=/usr/local/bin:/usr/bin:/bin -- app-server
```

The project and provider-home directories must already exist. The child receives
the selected home and explicit environment only. Supply additional environment
entries deliberately when a provider needs them. The caller sends `initialize`
and `initialized`, and must handle approval requests; this executable does not
automatically approve tools or issue paid turns. `--help` lists frame, queue,
pending-request, and request-timeout limits.

The server integration explicitly uses `--inherit-env --inherit-stderr
--parent-protocol --request-timeout-ms 0`. These options preserve the environment
already selected by the server, forward stderr to its existing diagnostic
consumer, and delegate request bookkeeping and deadlines to the parent client.
The standalone CLI keeps isolated environment and local request tracking by
default. `--home` is optional in inherited-environment mode and overrides only
`HOME` when supplied.

The portable core can also be checked with Cargo for `aarch64-apple-ios`,
`aarch64-apple-ios-sim`, `aarch64-linux-android`, `x86_64-pc-windows-gnu`, and
`wasm32-unknown-unknown`. These are core compilation checks. They do not prove a
signed mobile app, native rendering, transport behavior on a phone, or a browser
transport implementation.

The [dedicated CI workflow](../../.github/workflows/t3-code-ls.yml) checks this
workspace without requiring upstream's private CI infrastructure. The original
T3 application's full test suite remains a separate gate.

The official-runtime check uses the checksum-pinned Linux nightly archive and
fresh, disposable application state. With the debug CLI built, run from the
repository root:

```sh
python3 native/t3-code-ls/tools/verify-upstream.py \
  --archive /absolute/path/to/t3-0.1.8-nightly.20261108.4380-linux-x64.tar.gz \
  --output /tmp/t3-code-ls-upstream-verification.json
```

It checks the real application's HTML, limited pairing, cookie authentication,
rejected credentials, denied administrative permissions, WebSocket origin
checks, and `server.probe` over the Iroh bridge. It uses neither a provider turn
nor personal T3 state. The fixture must be reviewed when the nightly pin changes.
The test exercises both the upstream address and the forwarded address as the
browser Host and Origin, without forwarded-header overrides. It does not qualify
browser rendering, remote-client metadata, relays, or mobile lifecycle behavior.

## Track published nightlies

Run from the repository root:

```sh
python3 native/t3-code-ls/tools/nightly.py check \
  --repo-root . \
  --lock native/t3-code-ls/upstream.lock.json \
  --output /tmp/t3-code-ls-nightly.json --fetch
```

The tracker reads all release pages, resolves the release tag to an immutable
commit, and compares complete Git trees. It records affected application areas,
including mobile, providers, and Connect. It refuses moved pinned tags, incomplete
release scans, and source inventories that disagree with their recorded commits.
The command writes a candidate report. It does not merge source, modify branches,
advance the baseline, or claim compatibility.

Track PR and issue updates separately:

```sh
python3 native/t3-code-ls/tools/activity.py \
  --state native/t3-code-ls/target/upstream-activity-state.json \
  --output /tmp/t3-code-ls-activity.json \
  --lookback-hours 24 --max-items 1000 --overlap-minutes 60 --details none
```

The [upstream watch](../../.github/workflows/t3-code-ls-upstream.yml) runs at
`00:08`, `02:08`, and every two hours thereafter in UTC, subject to GitHub's
scheduler. It inventories changed PRs and issues and reports the newest published
nightly. Relevant candidates need a follow-up review of their code, discussions,
reviews, and CI; this broad scan does not download every discussion or patch.
Omit `--details none` for PR file, review, and head-check enrichment.

State is saved only after the scan's required requests and report write succeed.
An issue that changes during the scan retains its earlier pending event window;
the next scan retries it even if the updated listing omits it. Reports remain
explicitly incomplete while any pending windows remain. The workflow archives
reports and state before caching the next cursor, so a failed upload cannot
silently discard a scan. Candidate-branch pushes also exercise this watch.
With no state, the report explicitly covers only the initial 24-hour window;
restore the archived state after cache loss to retain older coverage. The watch
has read-only repository permissions and does not merge proposals. Authorized
maintenance prepares adaptations on candidate branches, runs the applicable
gates, and integrates passing changes into `main`. Failed checks leave the
candidate separate and the qualification status unchanged.

## Language and transport boundaries

Current Lust is an experimental Rust-subset compiler. Its supported development
host is Linux x86-64. It rejects async, external FFI, custom procedural macros,
and crates without Lust adapters. The native adapter and platform application
work therefore cannot honestly be described as all-Lust today. See the
[architecture decision](../../docs/internals/t3-code-ls.md) for the boundaries
and Iroh protocol choices.

This is an attributed port, not a clean-room implementation. Upstream source and
tests were inspected. The repository's MIT license and T3 Tools copyright notice
remain in place. New code uses the same license. Store packages, bundle IDs,
signing credentials, release endpoints, and branding must be separately
configured for this fork before distributing applications.
