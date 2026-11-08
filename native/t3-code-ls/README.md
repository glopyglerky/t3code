# t3 code ls

An unofficial T3 Code port with a Lust-compatible core and a native Iroh transport.
This is an implementation in progress. It is not a complete replacement for the T3
desktop, web, iOS, or Android applications.

The source baseline is the published nightly recorded in
[upstream.lock.json](./upstream.lock.json). Its `qualification` field stays
unqualified until the full application passes its compatibility gates. Tracking
a newer source revision does not establish feature parity.

## Current code

The portable core ports the nightly's permission vocabulary, explicit grant
precedence, legacy client compatibility, and permission error projection. The
comparison tool executes the original TypeScript functions from the checkout and
compares 28,560 decisions with the port. Client compatibility rules and exact
server grants have separate APIs so an old-client fallback cannot broaden server
authority.

The native transport carries a fixed loopback service's HTTP, WebSocket, and
binary traffic over authenticated Iroh connections. Each host explicitly admits
client endpoint identities. Admitting a peer does not issue a T3 application
session, change a user's permissions, or replace T3 pairing. The transport uses
the core's peer policy and closes admitted connections when their grant is
revoked, including a revoke immediately followed by a new grant.

The existing T3 server and clients remain upstream code. The bridge is an
executable migration step. The Connect account flow, client route selector,
mobile lifecycle, notifications, shared storage, and native UI are not completed
by forwarding their network traffic.

## Build

Use Rust 1.99.1. Cargo and rustc build release artifacts from the same core source
that Lust checks. The Iroh adapter uses async Rust, Tokio, and dependencies that
current Lust cannot compile.

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
python3 -B -m unittest discover -s tools -p 'test_*.py'
```

For the independent Lust gate, check out `t3dotgg/lust-lang` at the exact revision
in [lust-toolchain.json](./lust-toolchain.json), then run:

```sh
python3 tools/check-lust.py --source /absolute/path/to/lust-lang
```

The gate builds the pinned compiler and TinyCC, checks the unchanged core, runs
its five unit tests through Lust, and compiles and executes the same permission
matrix through Lust. It compares that output to upstream's actual TypeScript
functions. It rejects a dirty or wrong compiler checkout, ignored tests, source
changes during verification, and mismatched behavior. The receipt records hashes
and command results at `target/lust-proof.json`. There is no Cargo fallback for
these Lust checks.

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
host is Linux x86-64. It rejects async, external FFI, custom procedural macros, and crates
without Lust adapters. The native adapter and platform application work therefore
cannot honestly be described as all-Lust today. See the
[architecture decision](../../docs/internals/t3-code-ls.md) for the boundaries
and Iroh protocol choices.

This is an attributed port, not a clean-room implementation. Upstream source and
tests were inspected. The repository's MIT license and T3 Tools copyright notice
remain in place. New code uses the same license. Store packages, bundle IDs,
signing credentials, release endpoints, and branding must be separately
configured for this fork before distributing applications.
