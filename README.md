# t3 code ls: T3 Code being ported to Lust

<!-- t3-code-ls:upstream-readme:start -->

# T3 Code

T3 Code is an "agent harness control surface". It enables control of the agents on your machine with a best-in-class mobile app ([iOS](https://apps.apple.com/us/app/t3-code-remote-claude-more/id6787819824), [Android](https://play.google.com/store/apps/details?id=com.t3tools.t3code)), [web app](https://app.t3.codes) and [Electron-based desktop app](https://t3.codes).

Works with your subscriptions on Claude Code, Codex, Cursor, Grok Build, OpenCode, Google Antigravity, and fx. If they're set up on your computer, T3 Code can control them.

## "Wait, what are you selling me?"

Nothing. We built T3 Code because we wanted the best possible development experience with agents. We were inspired by existing solutions like the Codex desktop app, Conductor, Claude Desktop and Cursor Glass, but none met our bar.

We wanted something performant, remote-ready, and truly open. If we ever go the wrong direction, we want you to have everything you need to fork and build the editor that you want.

## Installation

> [!WARNING]
> T3 Code currently supports Codex, Claude, Cursor, Grok Build, OpenCode, Antigravity, and fx. Install and authenticate at least one provider before use:
>
> - Codex: install [Codex CLI](https://developers.openai.com/codex/cli) and run `codex login`
> - Claude: install [Claude Code](https://claude.com/product/claude-code) and run `claude auth login`
> - Cursor: install [Cursor CLI](https://cursor.com/cli) and run `agent login`
> - Grok Build: install [Grok Build CLI](https://x.ai/cli) and run `grok login`
> - OpenCode: install [OpenCode](https://opencode.ai) and run `opencode auth login`
> - Antigravity: enable it in Settings, then use **Install Antigravity** and **Sign in with Google**. No CLI is required.
> - fx: install [fx](https://fx.sh) and run `fx login` on the machine running the T3 Code server ([setup guide](./docs/user/providers-fx.md))

### Command line

```bash
curl -fsSL https://t3.codes/install.sh | sh
```

On Windows, in PowerShell:

```powershell
irm https://t3.codes/install.ps1 | iex
```

Then run `t3` to start the server and open the local web app. `t3 service install` keeps it running in the background, `t3 update` moves to a newer release, and `t3 --help` has the full reference.

To try it once without installing, run `npx t3@latest` instead.

### Desktop app

Install the latest version of the desktop app from [GitHub Releases](https://github.com/pingdotgg/t3code/releases), or from your favorite package registry:

#### Windows (`winget`)

```bash
winget install T3Tools.T3Code
```

#### Windows (`scoop`)

```bash
scoop install extras/t3code
```

#### macOS (Homebrew)

```bash
brew install --cask t3-code
```

#### Debian, Ubuntu (`.deb`)

Download the `.deb` from [GitHub Releases](https://github.com/pingdotgg/t3code/releases), then:

```bash
sudo apt install ./T3-Code-*.deb
```

#### Arch Linux (AUR)

Stable:

```bash
yay -S t3code-bin
```

Nightly:

```bash
yay -S t3code-nightly-bin
```

The AUR packaging is maintained in this repository under [`packaging/aur`](./packaging/aur).

#### Nix (nixpkgs)

```bash
nix profile add github:NixOS/nixpkgs/nixos-unstable#t3code
```

The package is maintained in [nixpkgs](https://github.com/NixOS/nixpkgs/tree/nixos-unstable/pkgs/by-name/t3/t3code)
and includes the desktop app (`t3code-desktop`) and the `t3` command.
See [Nix installation](./docs/user/install.md#nix-nixpkgs) for NixOS configuration and updates.

## Some notes

We are very very early in this project. Expect bugs.

We are (mostly) not accepting contributions yet. Small fixes may be considered. Big features will not be.

## Documentation

Full docs live in [docs/](./docs). There's no docs site yet.

- [FAQ](./docs/user/faq.md)
- [Install and first run](./docs/user/install.md)
- [Permission modes](./docs/user/permission-modes.md)
- [Keyboard shortcuts](./docs/user/keybindings.md)
- [Project settings](./docs/user/project-settings.md)
- [Appearance preferences](./docs/user/appearance.md)
- [Remote access from a phone or another machine](./docs/user/remote-access.md)
- [Connect Claude Code, Codex, ChatGPT and other agents over MCP](./docs/user/outside-agents.md)
- [Keeping app and server in sync](./docs/user/updating.md)
- [Source control integrations](./docs/user/source-control.md)
- Multiple accounts: [Codex](./docs/user/providers-codex.md) · [Claude](./docs/user/providers-claude.md)
- [Run T3 Code as a background service](./docs/user/background-service.md)

Building from source? Start at [docs/internals/overview.md](./docs/internals/overview.md).

## If you REALLY want to contribute still.... read this first

### Install `vp`

T3 Code uses Vite+ so you'll need to install the global `vp` command-line tool.

#### macOS / Linux

```bash
curl -fsSL https://vite.plus | bash
```

#### Windows

```bash
irm https://vite.plus/ps1 | iex
```

Checkout their getting started guide for more information: https://viteplus.dev/guide/

### Install dependencies

```bash
vp i
```

Read [CONTRIBUTING.md](./CONTRIBUTING.md) before reporting a bug or opening a PR.

Have a feature request? Start an [Ideas discussion](https://github.com/pingdotgg/t3code/discussions/categories/ideas).

Need support? Join the [Discord](https://discord.gg/jn4EGJjrvv).

<!-- t3-code-ls:upstream-readme:end -->

## t3 code ls customizations

t3 code luxury sport addition

This is glopyglerky's fork of T3 Code. The goal is to recreate the server,
web and desktop clients, and iOS and Android apps in
[Theo Browne's Lust](https://github.com/t3dotgg/lust-lang), with the same behavior
and UI as the current published T3 Code nightly. **The recreation is incomplete.**
The working user interfaces still use upstream React and React Native code, and
the server still uses TypeScript and Node.

The upstream README above is preserved byte for byte from the published nightly
commit recorded in [upstream.lock.json](./native/t3-code-ls/upstream.lock.json).
Its installation commands, package names, store links, support links, and release
links refer to upstream T3 Code. They do not install a completed Lust edition.
Build and verification instructions for this fork's native code are in
[native/t3-code-ls](./native/t3-code-ls/README.md).

### Customizations and divergence decisions

| Area                        | Fork decision and current boundary                                                                                                                                                                                                                                                                                                     |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Language                    | Port application logic into dependency-free source accepted by the pinned Lust compiler. Execute it with Lust and compare its behavior with the original TypeScript. Cargo compilation alone does not count as a Lust port.                                                                                                            |
| Native builds               | Cargo and rustc currently build the native release artifacts. The Iroh adapter requires async Rust, Tokio, and dependencies beyond current Lust support. These boundaries remain explicit until the compiler and target builds support their replacement.                                                                              |
| Connect transport           | Carry the configured T3 service's HTTP and WebSocket traffic over authenticated Iroh connections. Peer admission and revocation apply in addition to existing T3 pairing, sessions, and permissions. The bridge is not a completed replacement for Connect's discovery, account, session, notification, and mobile lifecycle behavior. |
| Iroh protocols              | Use blobs for authorized artifacts, gossip for bounded presence hints, and docs only for explicitly shared metadata where they fit T3 behavior. These integrations remain incomplete. Neither an address nor a content hash grants access.                                                                                             |
| UI and application behavior | Preserve the upstream application as the reference while replacing implementations. A retained TypeScript screen does not count as a Lust implementation of that screen. Platform builds, accessibility, reconnects, and signed packages need their own evidence.                                                                      |
| Upstream updates            | Check published nightlies and upstream activity every two hours. Review complete source changes and port adaptations before advancing qualification. Unmerged PRs and a newer source pin do not establish compatibility.                                                                                                               |

[Architecture decisions](./docs/internals/t3-code-ls.md) explain the compiler,
transport, authority, mobile, and storage boundaries. The native README records
current usage limits and verification commands.

### Methods used for the port

This is an attributed port. We inspect upstream source, tests, and published
artifacts, so this is not a clean-room implementation. The upstream MIT license
and copyright notice remain in [LICENSE](./LICENSE).

1. Pin the published nightly to its immutable commit and Git tree. Keep a complete
   inventory of server, contracts, providers, shared client logic, Connect, web,
   desktop, and mobile sources.
2. Port behavior at defined interfaces. Run the actual upstream implementation
   and the Lust-compiled implementation on the same cases, comparing outputs and
   errors. Permission tests include the distinction between absent and explicitly
   empty grants, legacy client compatibility, and exact server grants.
3. Check the compiler itself against the immutable revision in
   [lust-toolchain.json](./native/t3-code-ls/lust-toolchain.json). The Lust gate
   rejects dirty or mismatched compiler sources, failed or ignored tests, source
   changes during verification, and behavior mismatches. It does not fall back
   to Cargo when Lust fails.
4. Exercise actual application paths. The native transport tests cover endpoint
   admission and revocation; the official-nightly verifier checks application
   authentication and WebSocket requests through the bridge with disposable
   state. These checks do not establish full application or UI parity.
5. Require parity evidence before declaring completion. Compare command results,
   persisted state, ordered events, permissions, errors, cancellation, and recovery
   across supported providers and connection modes. Compare all corresponding
   screens and interactions on web, desktop, iOS, and Android using fixed viewports,
   fonts, themes, and device settings, with screenshot differences and accessibility
   checks. Qualify installation, updates, rollback, and platform lifecycle behavior
   on their actual targets. The lock's qualification remains unqualified until
   the full application satisfies these checks.

To verify that the upstream README section still matches the pinned source, run
from the repository root:

```sh
python3 native/t3-code-ls/tools/sync-readme.py
```

After an intentional upstream pin change, fetch that exact commit and run the
same command with `--write`. It replaces only the marked upstream section and
preserves this fork's heading and additions. It rejects missing or ambiguous
markers, an unavailable source commit, and a mismatched root tree.
