# t3code-ls

## Lust Port Info

This is an incremental Rust port of [T3 Code](https://github.com/pingdotgg/t3code), using [Lust](https://github.com/pingdotgg/lust), [Lustr](https://github.com/glopyglerky/lustr) and [blueFe](https://github.com/glopyglerky/blueFe). The goal is one complete T3 Code application whose implementation becomes Rust without losing upstream functionality or changing the clients' public contracts.

The original server, React web client, Electron desktop client and React Native mobile client are retained at their upstream paths. Rust replaces implementation behind their existing interfaces. Unported code stays until its replacement works in the actual application and passes the affected platform gates.

## Build and run the hybrid application

Use Node `26.12.1` and pnpm `12.14.0`, matching the inherited workspace. Install the pinned Linux GNU x86_64 compiler package into a new directory outside the checkout:

```sh
T3_CODE_LS_TOOLCHAIN="$(node tools/install-toolchain.mjs --destination /absolute/path/t3code-ls-toolchain)"
export T3_CODE_LS_TOOLCHAIN
export CARGO_TARGET_DIR=/absolute/path/t3code-ls-build
export CARGO="$T3_CODE_LS_TOOLCHAIN/bin/cargo"
pnpm install --frozen-lockfile
node tools/build-native.mjs
T3CODE_DEV_SEED=0 pnpm dev
```

The compiler installer verifies the pinned archive, component identities and relocated toolchain. If the producer's ELF loader path is absent, it creates recorded host launchers outside the unchanged package; use the path printed by the installer. Application builds use the produced BlueFe compiler through Lustr's compatibility coordinator. This package does not contain native Lust; [lust-toolchain.json](lust-toolchain.json) separately retains the direct-Lust subset compiler identity. Full Rust compiled by BlueFe does not establish full native Lustr coverage.

Development and server packaging stage the Rust executables in `bin/` or `dist/native-bin/`. To use previously produced components, set `T3_CODE_LS_NATIVE_DIR` to a matching directory containing `manifest.json`. Staging verifies the target and executable hashes. Missing components fail visibly. Backend selection happens before execution; a failed or uncertain operation is not retried through another implementation.

The default native route is restricted to Linux GNU x86_64 with GLIBC 2.39 or newer. Other hosts keep the original implementation, selected before execution. Their ordinary Node application build does not need a native compiler. An explicit native bundle or component override still requires matching artifacts and fails visibly when invalid. Platform-specific release and lifecycle qualification remains separate from retaining the working source.

Set `T3_CODE_LS_IMPLEMENTATION=upstream` before starting the application to choose the original backend for controlled comparisons. Explicit component overrides select the requested native implementation. No operation retries through a different backend after failure.

Use isolated development state and controlled replay-provider fixtures. Do not point tests at an installed T3 database or use real provider turns for routine qualification. [Application development](docs/operations/development.md) and the [user documentation](docs/README.md) describe the inherited application.

## Source organization

| Path | Responsibility |
| --- | --- |
| `apps/server` | Existing WebSocket/HTTP contracts, server services, provider orchestration and native adapters |
| `apps/web`, `apps/desktop`, `apps/mobile`, `apps/marketing` | Original application clients and site |
| `packages` | Shared contracts, client state, provider and source-control packages |
| `native`, `infra` | Platform helpers and Connect infrastructure |
| `Cargo.toml`, `Cargo.lock` | Root Rust workspace beside the temporary pnpm workspace |
| `crates/core`, `crates/storage`, `crates/transport`, `crates/app` | Preserved Rust work, with domain ownership and migration status in the port map |
| `tools`, `docs/porting` | Build, differential tests and migration accounting |

Source recovery, Rust source moves and behavior substitutions are separate changes. Unported implementation remains at its upstream location. No second vendored T3 application or `legacy/` tree is required.

## Current integration boundaries

Project planning, command/dispatch policy, guest sharing, recovery and maintenance have Rust implementations connected through the original server services. Codex process and structured-generation paths use native executables on the qualified Linux host route. Other hosts retain their original implementations until their native replacements are accepted. [port-map.json](port-map.json) records the actual consumers, compiler route, upstream source identities and acceptance state.

Production persistence still uses the inherited SQLite transaction boundary. The packaged redb helper now provides shared application-row transactions and validated SQLite row hydration with atomic publication. The [redb cutover record](docs/porting/redb-cutover.md) describes these tested components and the repository work still required. Authoritative cutover must cover events, projections, command and rejection receipts, outbox, replay, crash recovery, migration and backup/restore together. SurrealDB is not required. Switching back to an old database after redb has accepted new writes requires a defined data-transfer boundary.

Iroh provides authenticated byte forwarding. Connect identities, pairing, permissions, browser access, mobile reconnect and session lifecycle remain in the functioning application until their replacements are accepted.

The experimental `t3-code-ls-app` API/CLI is preserved as component work. Its `POST /rpc` protocol is not the existing clients' application protocol and is not the default T3 server. See [the native API documentation](docs/native-api.md) for its limited scope.

## Qualification and synchronization

A candidate has component evidence. An integrated port has a real application consumer. Acceptance requires normal execution through Rust and behavior, failure-path and platform evidence. Only then may the accepted original implementation be retired. A synced port has also incorporated changes through its recorded upstream revision. The [migration gate](docs/porting/port-map.md) checks source retention and accounting; it does not manufacture behavioral acceptance.

[upstream.lock.json](upstream.lock.json) records the immutable integrated upstream revision and keeps detection separate from qualification. The source may come from the trusted default branch or a published nightly; a default-branch snapshot does not claim a nightly release identity. The recovered source is based on `d29a94f`, overlaid on `0be60f8` while retaining all later Rust work. Newer upstream revisions are separate integration work. A changed lockfile or a successful component suite is not proof of full application parity.

[Fork CI](.github/workflows/hybrid.yml) selects affected Rust packages, adapters and client integration from a verified qualified ancestor. Upstream's default branch is trusted; its pristine test suites and original-only client flow are not repeated. Exact-source qualification receipts avoid duplicate builds when a tested commit reaches `main`. Each run also generates an immutable upstream comparison and prioritized porting queue. [CI and porting](docs/ci-and-porting.md) explains selection, evidence reuse and local commands.

[Recovery and acceptance](docs/porting/recovery.md) records the exact tested scope and remaining platform checks. Ordinary application CI downloads a pinned compiler artifact; compiler bootstrap belongs in the compiler repositories.

## Upstream and licensing

[upstream-README.md](upstream-README.md) preserves the immutable upstream README body. Its download and installation links describe upstream releases. Source and replacement mappings remain after retirement so future upstream edits cannot silently bypass translated code.

The [MIT license](LICENSE), upstream copyright, dependency notices, repository identity and fork relationship remain unchanged. Historical source and reference pins remain in Git history. Disposable reference checkouts belong outside the product tree.
