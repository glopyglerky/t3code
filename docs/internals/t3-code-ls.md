# t3code-ls design and divergence

The default tree is a Rust component project. The former application clients,
server adapters, foreign-language tooling and release workflows are retired,
not replaced by equivalent Rust application behavior. Their source is available
in Git history and upstream at the immutable `upstream.lock.json` revision.
No vendored reference checkout belongs in this product checkout.

Core protocol/decision code, redb storage and Iroh transport retain their existing
contracts. Standalone redb does not substitute for an installed application's
shared transaction boundary. Iroh carries authenticated bytes; it does not
supply Connect identities, account pairing, push or mobile lifecycle.

KDE capture depended on executable JavaScript/QML, including scripts embedded
through Rust strings. That helper was retired as a unit. The remaining optional
Hyprland and resource-monitor crates contain Rust implementation with upstream
platform dependencies. They are excluded from the root workspace and have no
qualified desktop delivery path.

Native Lust subset acceptance and full Rust via the Lustr/BlueFe coordinator
remain different checks. Compiler and database pins remain explicit. Whole-stack
memory safety is unverified: upstream libraries, cryptographic native code,
OS interfaces, compiler payloads and provider processes are separate dependencies.

Historical reference source/tree pins live in `reference-history.json`. Read a
historical file using `git show <historical_snapshot_commit>:<snapshot_prefix>/<path>`.
The pinned upstream reference trees contain some files omitted by the original
snapshot, and Alchemy's historical gitlink differs; both identities are recorded.
Source/licensing history is preserved without restoring reference caches.
