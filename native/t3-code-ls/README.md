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
and attachment limits. Expiration-string parsing remains the date adapter's
responsibility; the port preserves its parsed result and the original string.

The Unix provider process runtime runs JSON-RPC over a child process's standard
streams. It is compiled and executed by Lust, including its bounded JSON codec,
request tracking, notifications, approval requests, and owned-child cleanup.
Controlled subprocess fixtures exercise its behavior without paid provider turns
or personal credentials. This is the raw provider protocol layer; the generated
Codex payload schemas, other provider adapters, orchestration persistence, and
application UI still need integration.

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
cargo test --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
node --disable-warning=ExperimentalWarning tools/check-permissions.mjs
node --disable-warning=ExperimentalWarning tools/check-connection.mjs
node --disable-warning=ExperimentalWarning tools/check-policy.mjs
node --disable-warning=ExperimentalWarning tools/check-dispatch.mjs
node --disable-warning=ExperimentalWarning tools/check-guest-share.mjs
node tools/check-json.mjs
cargo build --locked -p t3-code-ls-core --bin t3-code-ls-provider
node tools/check-provider.mjs target/debug/t3-code-ls-provider
python3 -B -m unittest discover -s tools -p 'test_*.py'
```

For the independent Lust gate, check out `t3dotgg/lust-lang` at the exact revision
in [lust-toolchain.json](./lust-toolchain.json), then run:

```sh
python3 tools/check-lust.py --source /absolute/path/to/lust-lang --artifacts target/lust-bin
```

The gate builds the pinned compiler and TinyCC, checks the unchanged core, runs
its unit tests through Lust, and compiles and executes the application oracles
and provider subprocess fixtures. It rejects a dirty or wrong compiler checkout,
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
`t3-code-ls-policy` and `t3-code-ls-provider` executables. CI retains these verified
components and their proof receipts as an artifact, including the dispatch-service
receipt. They are development components, not desktop or mobile installers.

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
  --archive /absolute/path/to/t3-0.1.8-nightly.20261108.4367-linux-x64.tar.gz \
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
