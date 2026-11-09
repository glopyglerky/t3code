# t3code-ls

t3code-ls is glopyglerky's independently maintained [T3 Code](https://github.com/pingdotgg/t3code) fork. The Rust port lives at repository root. Working behavior comes first; complete Rust UI/application replacement, whole-stack memory safety, and an independent Lustr compiler remain future goals.

## Source and runtime

| Path                                      | Responsibility                                                                                        |
| ----------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| `Cargo.toml`, `Cargo.lock`, `crates/core` | Root Rust workspace, application decisions, JSON protocols, Unix provider and generation processes    |
| `crates/storage`                          | Standalone redb event/projection/receipt/outbox storage and SQLite import                             |
| `crates/transport`                        | Iroh HTTP/WebSocket bridge                                                                            |
| `tools`                                   | Compiler gates, immutable upstream comparisons, adapter/runtime checks and source synchronization     |
| `apps/server`, `packages`                 | Required wire schemas, Effect adapters, side effects, provider drivers and shared SQLite transactions |
| `apps/web`, `apps/desktop`, `apps/mobile` | Working React, Electron and React Native clients awaiting qualified replacements                      |
| `infra/relay`                             | Connect account, authorization and client lifecycle integration not replaced by byte forwarding       |
| `native/resource-monitor`                 | Separate Rust workspace with its existing platform build contract                                     |

Policy, project planning, guest-share, recovery and maintenance decisions use Rust executables by default. Unix Codex provider and structured generation also use Rust by default. `T3_CODE_LS_{POLICY,PROJECT,GUEST_SHARE,RECOVERY,MAINTENANCE,PROVIDER,GENERATION}_EXECUTABLE` can explicitly override a component. Invalid selections and missing executables fail visibly. There is no TypeScript decision fallback. Windows retains its Codex process and generation implementation because the native process runtime requires Unix.

The redb host does **not** replace production SQLite: the existing server commits events, projections, receipts and effects together. Switching one participant alone would break atomicity. Iroh forwarding does not replace Connect identity, pairing, permissions, push or mobile lifecycle.

## Build and develop

Build on the NAS with a qualified toolchain. Set `T3_CODE_LS_TOOLCHAIN` to its directory and `CARGO_TARGET_DIR` to a generated-output directory on the build filesystem. The toolchain must contain produced `lustr`, `rustc`, `rustdoc` and a matching sysroot; its Cargo may be bootstrap Cargo. The build helper explicitly selects those drivers and sysroot.

```sh
node tools/build-native.mjs
```

This builds the core executables into root `bin/`. Application dev scripts build these first. Server package builds put them beside the bundled server in `dist/native-bin/`. `node apps/server/scripts/cli.ts build-exe` stages a separate `dist-exe/native-bin/` for the SEA executable; `scripts/build-cli-archive.ts` includes it in the runtime archive. Desktop packaging unpacks these components beside ASAR files so they can be spawned.

For cross builds, set `T3_CODE_LS_NATIVE_TARGET` to the Node platform/architecture (for example `win32-x64` or `darwin-arm64`) and `T3_CODE_LS_SYSROOT` to a produced sysroot containing that Rust target. The helper passes the corresponding explicit Cargo target and preserves caller Rust flags. A matching linker is also required. Alternatively, set `T3_CODE_LS_NATIVE_DIR` to a previously produced component directory with its `manifest.json`; staging verifies target and file hashes. CLI `build-exe --target` selects the component target automatically. Desktop packaging verifies the server's components against its requested platform and architecture. Missing target sysroots or mismatched artifacts fail the build; host binaries are never a cross-build substitute. Windows WSL uses a Linux runtime archive and Linux components, while the Windows server sidecar needs Windows components. Cross-target staging does not qualify execution on Windows, macOS, or WSL. Explicit component overrides remain useful for independently qualified Lust executables. Cross-platform source and decision fixtures do not establish native execution on another platform.

The existing application still needs Node and Vite+ as described in [CONTRIBUTING.md](CONTRIBUTING.md):

```sh
vp i
T3CODE_DEV_SEED=0 vp run dev
```

Do not point development at installed user state. These commands do not install this fork over an existing T3 installation.

For full Rust workspace qualification, use the produced toolchain's Lustr coordinator with `--bluefe-root`, matching `RUSTC`/`RUSTDOC`, encoded `--sysroot`, and Cargo's `test --locked --workspace --all-targets`. Coordinator forwarding to BlueFe is distinct from native subset compilation.

For native subset qualification:

```sh
python3 tools/check-lust.py --toolchain "$T3_CODE_LS_TOOLCHAIN" \
  --receipt "$CARGO_TARGET_DIR/lust-proof.json" \
  --artifacts "$CARGO_TARGET_DIR/lust-bin"
```

This validates packaged compiler hashes from [lust-toolchain.json](lust-toolchain.json), builds unchanged core source, executes tests, and compares decisions against immutable upstream objects. `--source` instead builds the pinned native compiler from source. The `tools/check-*-adapter.mjs` checks execute real services with controlled provider subprocesses and ephemeral databases. They require the bounded dependencies installed by `node tools/install-server-tests.mjs`. They never require paid provider sessions.

[Design and divergence](docs/internals/t3-code-ls.md) records the remaining boundaries and replacement decisions. [Application development](docs/operations/development.md) and [user documentation](docs/README.md) describe the inherited application. Upstream download links refer to upstream releases, not qualified fork distributions.

## Upstream and licensing

Optional Effect and Alchemy development references are restored to ignored `.repos/` caches with `vp run sync:repos`; exact upstream and historical snapshot pins remain in [the reference manifest](scripts/lib/reference-repos.ts). They are not build inputs. Retiring their tracked snapshots does not represent additional Rust implementation.

The immutable nightly source baseline is [upstream.lock.json](upstream.lock.json); source currency is separate from application qualification. [upstream-README.md](upstream-README.md) is necessary historical reference material, preserved byte-for-byte by `tools/sync-readme.py`, not this fork's product guide. The two-hour read-only upstream workflow remains enabled in source.

The [MIT license](LICENSE), upstream copyright and dependency notices remain intact. GitHub's fork relationship and compatibility package identifiers remain unchanged. No language-percentage overrides are used.
