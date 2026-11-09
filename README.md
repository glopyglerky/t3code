# t3 code ls

t3 code ls is glopyglerky's independently maintained fork of [T3 Code](https://github.com/pingdotgg/t3code), with a Rust port in progress. It keeps the existing application usable while native components replace specific responsibilities. Working builds come first.

The full application still uses the TypeScript server, React web UI, Electron desktop shell, and React Native mobile clients. Native Rust code exists for policy and recovery decisions, provider process handling, project planning, redb storage, and an Iroh transport bridge. These components do not yet replace the complete application.

## Start here

- [Native component guide](native/t3-code-ls/README.md) explains builds, executable selectors, comparison tools, and limits.
- [Design and divergence record](docs/internals/t3-code-ls.md) explains what this fork owns, why inherited paths remain, and what must pass before replacement.
- [Application development](docs/operations/development.md) covers the existing server and clients.
- [User documentation](docs/README.md) describes the inherited application's behavior. Upstream download links and package names refer to upstream releases.

For application development, use the Node and package-manager versions in [package.json](package.json), install Vite+ as described in [CONTRIBUTING.md](CONTRIBUTING.md), then run from the repository root:

```sh
vp i
T3CODE_DEV_SEED=0 vp run dev
```

This starts the existing server and web development path with automatic state seeding disabled. Authenticate a supported provider separately when needed. It does not select the native components or install this fork over an existing T3 installation. Read [AGENTS.md](AGENTS.md) before changing code.

## Native entry points and source map

| Path | Responsibility today |
| --- | --- |
| [native/t3-code-ls/crates/core](native/t3-code-ls/crates/core) | Portable decisions, JSON protocols, Unix provider process runtime, and component executables |
| [native/t3-code-ls/crates/storage](native/t3-code-ls/crates/storage) | Standalone redb event, projection, receipt, outbox, and import work; production storage cutover remains incomplete |
| [native/t3-code-ls/crates/transport](native/t3-code-ls/crates/transport) | Cargo-built Iroh HTTP/WebSocket bridge, exposed as `t3-code-ls` |
| [native/t3-code-ls/tools](native/t3-code-ls/tools) | Pinned-source comparisons, compiler gates, adapter checks, and upstream tracking |
| [apps/server](apps/server) | Existing application server and adapters that select native executables |
| [apps/web](apps/web), [apps/desktop](apps/desktop), [apps/mobile](apps/mobile) | Existing web, desktop, iOS, and Android clients |
| [packages](packages) | Shared contracts, client runtime, provider and source-control integrations |
| [infra/relay](infra/relay) | Inherited Connect control plane, still required independently of the Iroh bridge |
| [scripts](scripts), [patches](patches), [pnpm-workspace.yaml](pnpm-workspace.yaml) | Existing application build and dependency contracts |
| [.repos](.repos) | Read-only dependency references, not fork runtime entry points |

The native Cargo workspace is [native/t3-code-ls/Cargo.toml](native/t3-code-ls/Cargo.toml). Its default members are core and transport; storage is a separate workspace member. The [native guide](native/t3-code-ls/README.md) contains the component build commands and selectors such as `T3_CODE_LS_POLICY_EXECUTABLE`. An unset selector retains the existing implementation; selected native failures must not silently fall back.

## Port status and direction

[upstream.lock.json](native/t3-code-ls/upstream.lock.json) records the immutable nightly source baseline. Its qualification status is currently `unqualified`, with no last qualified application commit. Source tracking, component tests, and a working upstream application do not establish full fork parity.

The native core and workspace forbid unsafe code in their own crates. That lint does not establish memory safety across dependencies, compiler output, foreign libraries, providers, or the remaining application. Full native Rust conversion and 100% memory-safe Rust using Lustr/BlueFe are future goals. BlueFe application/toolchain qualification and complete native web, desktop, iOS, and Android replacements are not verified support today. Existing TypeScript, UI, build code, and interfaces remain until replacements pass meaningful checks.

The production application still uses SQLite. The native redb store is a separate implementation, and Iroh transport does not replace Connect accounts, permissions, mobile lifecycle, or notifications. See the [design record](docs/internals/t3-code-ls.md) for the cutover and authorization boundaries.

## Upstream and licensing

GitHub identifies [pingdotgg/t3code](https://github.com/pingdotgg/t3code) as this fork's direct parent and source repository. The [preserved upstream README](native/t3-code-ls/upstream-README.md) retains the pinned upstream text for reference; its installation commands distribute upstream T3 Code.

The [MIT license](LICENSE), upstream copyright notices, and dependency license material remain intact. Existing `t3`, `@t3tools/*`, package identifiers, and source paths remain compatibility contracts while the port progresses. They do not imply that upstream publishes or supports this fork.
