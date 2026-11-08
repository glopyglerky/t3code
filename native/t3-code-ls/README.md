# t3 code ls

An unofficial T3 Code port with Lust-built application components and a native Iroh transport.
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
`exec` text-generation path and the other provider adapters are not ported by
this selection.

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
node --disable-warning=ExperimentalWarning tools/check-project-commands.mjs
cargo build --locked -p t3-code-ls-core --bin t3-code-ls-project
node --disable-warning=ExperimentalWarning tools/check-project-runtime.mjs target/debug/t3-code-ls-project
cargo build --locked -p t3-code-ls-core --example provider_recovery_matrix
node --disable-warning=ExperimentalWarning tools/check-provider-recovery.mjs target/debug/examples/provider_recovery_matrix
cargo build --locked -p t3-code-ls-core --bin t3-code-ls-recovery
node --disable-warning=ExperimentalWarning tools/check-recovery-runtime.mjs target/debug/t3-code-ls-recovery
node tools/check-json.mjs
cargo build --locked -p t3-code-ls-core --bin t3-code-ls-provider
node tools/check-provider.mjs target/debug/t3-code-ls-provider
python3 -B -m unittest discover -s tools -p 'test_*.py'
```

For the independent Lust gate, check out `t3dotgg/lust-lang` at the exact revision
in [lust-toolchain.json](./lust-toolchain.json), then run:

```sh
python3 tools/check-lust.py --source /absolute/path/to/lust-lang --artifacts target/lust-bin
tools/server-test-runtime/node_modules/.bin/effect-tsgo tsrs --noEmit --project tools/provider-tsconfig.json
node tools/check-provider-adapter.mjs "$PWD/target/lust-bin/t3-code-ls-provider"
tools/server-test-runtime/node_modules/.bin/effect-tsgo tsrs --noEmit --project tools/recovery-tsconfig.json
node tools/check-recovery-adapter.mjs "$PWD/target/lust-bin/t3-code-ls-recovery"
tools/server-test-runtime/node_modules/.bin/effect-tsgo tsrs --noEmit --project tools/guest-share-tsconfig.json
node tools/check-guest-share-adapter.mjs "$PWD/target/lust-bin/t3-code-ls-guest-share"
tools/server-test-runtime/node_modules/.bin/effect-tsgo tsrs --noEmit --project tools/project-tsconfig.json
node tools/check-project-adapter.mjs "$PWD/target/lust-bin/t3-code-ls-project"
```

The gate builds the pinned compiler and TinyCC, checks the unchanged core, runs
its unit tests through Lust, and compiles and executes the application oracles
and provider and recovery subprocess fixtures. It rejects a dirty or wrong compiler checkout,
ignored tests, source changes during verification, and mismatched behavior. The
receipt records hashes of the exact Git blobs, binaries, and command results at
`target/lust-proof.json`. There is no Cargo fallback for these Lust checks.

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
`t3-code-ls-guest-share`, and `t3-code-ls-project` executables.
CI retains these verified components and their proof receipts as an artifact,
including the dispatch, SQLite recovery, and guest-share integration receipts.
They are development components, not desktop or mobile installers.

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
