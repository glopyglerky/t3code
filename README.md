# t3code-ls

t3code-ls is glopyglerky's Rust component fork of [T3 Code](https://github.com/pingdotgg/t3code). The default tree contains Rust implementation, Cargo metadata, licenses, documentation and passive assets/data. It is **not a working replacement for the original application**.

## Usable components

- `t3-code-ls`: authenticated Iroh transport with `host` and `forward` commands; run `--help` for routing, identity and admission options. This is byte transport, not the original Connect account/pairing service.
- `t3-code-ls-policy`, `project`, `guest-share`, `recovery`, `maintenance`, `outbox`, and `project-store` (each with the `t3-code-ls-` prefix): decision and JSON protocol components. They do not run an application server or user interface.
- `t3-code-ls-provider` and `t3-code-ls-generate`: Unix process/protocol runtimes. They require an external provider executable. Windows process support is unimplemented.
- `t3-code-ls-db`: standalone redb events, projections, receipts, outbox and SQLite-export import. It does not automatically migrate an installed T3 database or provide distributed active-active HA.
- `t3code-ls-tools`: Rust build staging and source/upstream README checks.

The React web app, Node/Effect server and adapters, Electron/SEA/npm distributions, iOS/Android clients, Windows fallback, non-Rust build/test tools and old CI/release workflows have been removed. There is no Rust UI/server replacement yet. The KDE capture helper was retired because it depended on embedded JavaScript/QML. Rust-only resource-monitor and Hyprland helper sources remain separate optional crates; their desktop integration and platform execution are unqualified.

## Build and check

Use a qualified Lustr/BlueFe payload matching [lust-toolchain.json](lust-toolchain.json). Set `T3_CODE_LS_TOOLCHAIN` to its absolute directory, `RUSTC` and `RUSTDOC` to its produced drivers, `CARGO_TARGET_DIR` to an absolute generated-output directory outside this checkout, and `CARGO_ENCODED_RUSTFLAGS` to `--sysroot` followed by the unit-separator byte and the produced sysroot directory. Then invoke the toolchain's `bin/lustr --bluefe-root <toolchain>` with Cargo arguments:

```text
test --locked --workspace --all-targets
run --locked -p t3code-ls-tools -- check-source
run --locked -p t3code-ls-tools -- check-upstream-readme
```

For executable staging, unset the manually supplied sysroot flag and run `t3code-ls-tools build`. It selects the produced drivers, appends the selected sysroot while preserving caller flags, passes an explicit Rust target, and stages workspace executables plus a manifest under `<CARGO_TARGET_DIR>/deliverable/<target>/`. `T3_CODE_LS_SYSROOT` and `T3_CODE_LS_RUST_TARGET` select a matching cross-target payload. Missing sysroots fail before building; a target linker and native dependencies are also required. This is component staging, not an installer or a release archive.

The toolchain's Cargo is unmodified bootstrap Cargo. Full Rust through Lustr's BlueFe coordinator is distinct from executing unchanged source through the native Lust compiler. No matching Clippy driver is presently qualified. Component tests and Linux builds do not establish macOS, Windows, mobile, UI or release acceptance.

## Boundaries and provenance

Workspace-owned Rust forbids unsafe code, but dependencies and the OS remain outside that guarantee. Iroh's `ring` and `blake3` dependencies use upstream C/assembly and a native build toolchain; libc, networking, process and filesystem interfaces are platform dependencies. Optional helpers depend on OS/Wayland APIs. Provider commands are external programs, not Rust implementations owned by this fork. No whole-stack memory-safety or all-Rust dependency claim is made.

Original behavior/source is pinned in [upstream.lock.json](upstream.lock.json). Read it with `git show <pinned-commit>:<path>` after fetching that exact commit, or at the pinned upstream GitHub revision. Do not copy upstream implementation/reference caches into this product checkout. Retired fork implementation and exact reference snapshots remain in Git history, with [reference pin metadata](reference-history.json). Removing them adds no Rust implementation.

[upstream-README.md](upstream-README.md) preserves the immutable upstream README body and its installation links describe upstream software. [Design and divergence](docs/internals/t3-code-ls.md) records the current scope. The [MIT license](LICENSE), donor licenses and provenance remain. Repository identity and fork relationship are unchanged; there are no language-classification overrides added by this change.
