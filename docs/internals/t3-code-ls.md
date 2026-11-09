# t3code-ls design and divergence

The default tree contains native Rust components and an application API/CLI.
The former application clients, server adapters and foreign-language tooling
are retired; the native application's implemented subset does not replace their
full behavior. Their source is available
in Git history and upstream at the immutable `upstream.lock.json` revision.
No vendored reference checkout belongs in this product checkout.

Core protocol/decision code, redb storage and Iroh transport retain their existing
contracts. Standalone redb does not substitute for an installed application's
shared transaction boundary. Iroh carries authenticated bytes; it does not
supply Connect identities, account pairing, push or mobile lifecycle.

The native application shares one redb event/projection/receipt transaction
boundary for its project and thread service operations. Its provider runtime
records intent before submitting external work and never automatically replays
an uncertain action. Raw provider events and native session projections have
their own namespace; they are not upstream orchestration events. A process-wide
server lock prevents another native server from declaring live sessions crashed.
Database multiwriter support alone does not establish provider process ownership.

After a normal stop, a native session records confirmed process cleanup. After
a hard restart, process ownership is unknown and cleanup-dependent lifecycle
operations remain blocked. A workspace change cannot silently reuse a provider
thread created with different execution settings. Project/thread deletion and
the general outbox executor remain outside the implemented service subset.

KDE capture depended on executable JavaScript/QML, including scripts embedded
through Rust strings. That helper was retired as a unit. The remaining optional
Hyprland and resource-monitor crates contain Rust implementation with upstream
platform dependencies. They are excluded from the root workspace and have no
qualified desktop delivery path.

Native Lust subset acceptance and full Rust via the Lustr/BlueFe coordinator
remain different checks. Compiler and database pins remain explicit. Whole-stack
memory safety is unverified: upstream libraries, cryptographic native code,
OS interfaces, compiler payloads and provider processes are separate dependencies.

The hosted source bootstrap produces a relocatable compiler payload with a full
file manifest. A cache hit is not qualification: restored source identities,
hashes, native execution, procedural macros, tests, Wasm linkage and native Lust
execution must pass before the application is compiled. The old root-layout
qualification record remains historical evidence for its own source revision;
it does not qualify the current application or removed adapters.

Historical reference source/tree pins live in `reference-history.json`. Read a
historical file using `git show <historical_snapshot_commit>:<snapshot_prefix>/<path>`.
The pinned upstream reference trees contain some files omitted by the original
snapshot, and Alchemy's historical gitlink differs; both identities are recorded.
Source/licensing history is preserved without restoring reference caches.
