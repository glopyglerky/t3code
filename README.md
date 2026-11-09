# t3code-ls

t3code-ls is glopyglerky's native Rust port of [T3 Code](https://github.com/pingdotgg/t3code), in progress. It includes a native application API and command-line client, durable redb storage, Codex sessions and Iroh forwarding. It is **not a complete replacement for the original application**. Web, desktop and mobile user interfaces remain unfinished.

## Usable components

- `t3-code-ls-app`: authenticated native application server and command-line client. It connects project and thread commands to redb, provides durable event cursors, and runs explicitly configured Codex sessions on Unix.
- `t3-code-ls`: authenticated Iroh transport with `serve` and `forward` commands; run `--help` for routing, identity and admission options. This is byte transport, not the original Connect account/pairing service.
- `t3-code-ls-policy`, `project`, `guest-share`, `recovery`, `maintenance`, `outbox`, and `project-store` (each with the `t3-code-ls-` prefix): decision and JSON protocol components. They do not run an application server or user interface.
- `t3-code-ls-provider` and `t3-code-ls-generate`: Unix process/protocol runtimes. They require an external provider executable. Windows process support is unimplemented.
- `t3-code-ls-db`: standalone redb events, projections, receipts, outbox and SQLite-export import. It does not automatically migrate an installed T3 database or provide distributed active-active HA.
- `t3code-ls-tools`: Rust build staging and source/upstream README checks.

The React web app, Node/Effect server and adapters, Electron/SEA/npm distributions, iOS/Android clients, Windows fallback and non-Rust build/test tools were removed. The native API below implements a subset of their behavior with a new protocol. It is not compatible with the retired clients. The KDE capture helper was retired because it depended on embedded JavaScript/QML. Rust-only resource-monitor and Hyprland helper sources remain separate optional crates; their desktop integration and platform execution are unqualified.

## Run the native application

Use the `t3-code-ls-app` executable from the verified build. Create a new, isolated state directory and start its server:

```sh
t3-code-ls-app initialize --state /tmp/t3code-ls-demo
t3-code-ls-app serve --state /tmp/t3code-ls-demo --listen 127.0.0.1:3773
```

Initialization creates `state.redb` and an owner-only `api-token`. It does not import an installed application's database. The server admits one process per state directory. Every request needs the token. It binds only to loopback; use the existing Iroh `serve` and `forward` commands for an authenticated remote byte connection. Iroh admission and the application token are separate checks.

Save a request as JSON and pass it to the native client:

```json
{"jsonrpc":"2.0","id":"capabilities","method":"application.capabilities","params":{}}
```

```sh
t3-code-ls-app request --address 127.0.0.1:3773 --token-file /tmp/t3code-ls-demo/api-token < request.json
```

The HTTP endpoint is `POST /rpc`, with `Authorization: Bearer <token>` and `Content-Type: application/json`. Browser origins are not enabled. Requests have bounded size and execution time. JSON-RPC notifications and batches are not accepted; a caller must receive or recover the result of each operation.

| Method | Parameters and behavior |
| --- | --- |
| `application.capabilities` | `{}` reports supported commands and whether Codex execution is configured. |
| `command.execute` | `{ "command": ... }` accepts the supported upstream project/thread command shape. Keep its `commandId` unchanged when retrying a lost reply. |
| `state.snapshot` | `{}` returns the redb revision, durable event cursor and current projections. |
| `events.list` | `{ "afterSequence": 0, "limit": 256 }` reads ordered durable events; save the last returned sequence for reconnection. |
| `receipt.get` | `{ "commandId": "..." }` retrieves a committed result. IDs beginning with `native:` are reserved for application-generated operations. |
| `outbox.get` | `{ "effectId": "..." }` reads a background effect's current state. |
| `session.open` / `session.close` | `{ "threadId": "..." }` opens or stops the configured Codex process for an existing native thread. |
| `session.request` | `{ "threadId": "...", "commandId": "...", "method": "...", "params": {} }` accepts the Codex methods described below. |
| `session.respond` | `{ "threadId": "...", "commandId": "...", "requestId": 123, "result": ... }` answers an observed pending provider request. |
| `session.request.wait` | `{ "commandId": "..." }` waits for a persisted provider result or uncertain outcome. |
| `session.approval.wait` | `{ "threadId": "..." }` waits for an observed pending provider request. |
| `session.idle.wait` | `{ "threadId": "..." }` waits until the durable session has no active turn or unresolved turn start. Wait methods return `pending: true` after 25 seconds when no result is available. |

Supported storage commands are project creation and metadata updates; thread creation and metadata updates; pinning, unpinning and ordering; archive/unarchive; read visits; and the auto-settle preference. Thread creation records the authenticated application's creation provenance. Events, projections and receipts commit together. Archive also commits its terminal-cleanup obligation; the native application does not yet execute that cleanup worker. Active or uncertain provider work blocks lifecycle changes requiring cleanup. A closed session with confirmed process cleanup can be archived.

Provider execution is disabled until `serve` receives `--provider` with an absolute Codex executable path. Its default launch argument is `app-server`; repeated `--provider-arg` values supply a different trusted launch configuration. The external executable retains its own authentication. Only a thread selecting the configured `codex` instance can use this native path.

Open a session, request `thread/start` with `{}`, then request `turn/start` with `{"input":[{"type":"text","text":"..."}]}`. Use `thread/resume` after reopening a previously bound session, and `turn/interrupt` for an active turn. The host supplies the stored workspace, model and permission settings. Clients cannot override the native thread identity, executable, working directory or approval policy. Text input is supported; attachments require the unfinished attachment authorization port.

Provider intents commit before transmission. Reusing a command ID reads its prior receipt instead of sending another turn or approval response. Provider replies, notifications and pending approval requests enter the same durable event log before clients can read them. A request receipt means the action was accepted, not that the provider finished it. Its terminal receipt is available under `native:result:<commandId>`. Restart recovery marks interrupted actions uncertain and never replays them automatically. It does not claim to have stopped a provider process after a hard crash. Explicit shutdown closes and reaps owned processes.

The native provider events preserve Codex protocol data. Full upstream orchestration, provider-event translation, checkpointing, queued/steered turns, other providers, project/thread deletion, attachments, guest hosting, scheduled tasks and terminals remain unfinished. These APIs do not establish UI or feature parity.

## Build and check

[The native workflow](.github/workflows/native.yml) builds the pinned compiler sources from `glopyglerky/lustr` and `glopyglerky/blueFe` on a disposable GitHub-hosted runner. The Rust bootstrap helper verifies the upstream LLVM download and compiler identities, produces Rust and native Lust drivers, and checks their relocated sysroots. Cached payloads must pass exact source, file-hash and execution checks before application compilation. The bootstrap compiler is used to build the toolchain; the application is tested with produced BlueFe through Lustr.

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
