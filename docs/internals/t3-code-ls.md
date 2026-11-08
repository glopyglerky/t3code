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
failures while retaining payload fields outside its model. The caller still
owns authoritative event storage and transactional commit. A matching planned
batch does not establish durable recovery or wire this planner into the current
recovery service.

Source basis: [Lust's implementation contract](https://github.com/t3dotgg/lust-lang/blob/240e08d91d4aed0299639f6f11fd522912b248f9/README.md),
[release target allowlist](https://github.com/t3dotgg/lust-lang/blob/240e08d91d4aed0299639f6f11fd522912b248f9/src/resolve/cfg.rs),
and [C/zig release driver](https://github.com/t3dotgg/lust-lang/blob/240e08d91d4aed0299639f6f11fd522912b248f9/src/cc.rs).

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
