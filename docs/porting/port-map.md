# Port map and source retention

[`port-map.json`](../../port-map.json) maps upstream behavior to the Rust files
and remaining production adapters that implement it. It also records the source
revision, exact upstream blobs, current compiler route, target status and tests.
`rust_crates` lists each owning crate and its local crate dependencies. Acceptance
hashes every tracked file under those crate roots, including shared decoders,
manifests and fixtures, so a change outside the named feature module expires its
receipt too.
`domain_owner` names the eventual application or package domain. Recovery keeps
the current crates in place so source movement does not obscure behavior changes.

The recovered decision and process ports are marked `integrated`. This means the
restored application contains their production calls. It does not certify that
the recovered application or any target build passed its new acceptance gates.
The standalone native API, redb backend and Iroh transport remain `candidate`.
No entry in the initial map authorizes source retirement.

## States

| State | Required meaning |
| --- | --- |
| `candidate` | A Rust component exists. Required original application code stays. |
| `integrated` | A production application path calls the component. Acceptance is still pending. |
| `accepted` | The stated target has current application, contract, failure and build evidence. |
| `retired` | Required consumers have qualified replacements and the listed original source can be removed. |
| `synced` | The replacement and its acceptance evidence include the recorded integrated upstream revision. |

Platform routes describe selection before execution. An uncertain native provider
request or mutation must not be retried through another implementation.
Windows generation and provider execution retain their existing implementation
until Windows replacements qualify. A browser or mobile client still needs its
original UI and lifecycle even when its server uses Rust.

## `check-source`

Run `t3code-ls-tools check-source` through the pinned application toolchain. The
checkout must include Git objects for the immutable recovery commit
`d29a94f3a175eac1f26c548023fedd8695cedfaf` and the integrated upstream commit from
`upstream.lock.json`. CI fetches these exact objects; it does not resolve a moving
branch. New files must be staged before a local check because the check verifies
tracked product source.

The check requires the application and workspace manifests, validates mapped
paths and upstream blob identities, and rejects in-checkout `.repos` caches. It
compares `apps`, `packages`, `native`, `infra`, `scripts`, `patches` and the lint
plugin with the recovery tree. Removing any protected file needs an explicit
retirement record. TypeScript, JavaScript, platform bindings, HTML and CSS are
valid application dependencies throughout migration.

The initial deletion guard covers the recovered tree. Reviewers must also map
new functionality introduced by later upstream revisions. The gate cannot infer
that a retained file still has correct behavior, or that a valid receipt describes
a real passing run. Application tests and review of the linked run remain required.

## Acceptance receipts

For a target to enter `accepted`, `retired` or `synced`, its platform route must
name a tracked JSON acceptance receipt in `acceptance_evidence`. The key is the
exact product target, such as `linux-x64`; the value is the repository-relative
receipt path. The active implementation must be `rust` or `rust-backed-client`.
Passing tests against the original implementation does not accept its Rust
replacement. A receipt has these fields:

```json
{
  "schema_version": 1,
  "port": "project-commands",
  "target": "linux-x64",
  "result": "passed",
  "upstream_commit": "<40-character integrated upstream commit>",
  "compiler_route": "lustr-bluefe-compatibility",
  "run_url": "https://github.com/glopyglerky/t3code-ls/actions/runs/<numeric-run-id>",
  "checks": [
    "normal-client-flow",
    "contract-parity",
    "failure-recovery",
    "target-build"
  ],
  "source_hashes": {
    "<each mapped input and every tracked file in rust_crates>": "<Git blob hash>",
    "Cargo.toml": "<Git blob hash>",
    "Cargo.lock": "<Git blob hash>",
    "pnpm-lock.yaml": "<Git blob hash>",
    "lust-toolchain.json": "<Git blob hash>",
    "toolchain.lock.json": "<Git blob hash>"
  }
}
```

The placeholder example is not evidence. Record actual commands, results and
compiler identities alongside the receipt, then verify the exact Actions run.
The gate checks that the receipt names the correct port, target, source revision
and compiler route. It checks every mapped input hash against the working tree.
Changes to the implementation, adapters, tests or dependency/compiler locks make
the receipt stale.

To retire a source file, add its exact path and recovery blob hash to the port's
`retired_paths`. The path must already be an upstream source or adapter mapped to
that port. The port must be `retired` or `synced`. The conservative retirement gate
requires acceptance receipts across Linux x64/arm64, macOS x64/arm64, Windows
x64/arm64, browser, iOS and Android. For clients that call a server component,
qualify the normal client/server contract on that client platform. A Linux-only
component result cannot delete code needed by Windows or mobile consumers.

Keep the mapping after retirement. Upstream may change a removed TypeScript
module, and Git cannot identify the corresponding Rust behavior by itself.

## Nightly impact report

The existing read-only checker loads `port-map.json` from the checkout:

```sh
python3 tools/nightly.py check --repo-root "$PWD" --lock upstream.lock.json \
  --output /absolute/path/outside-checkout/nightly-impact.json --fetch
```

The report lists detected, integrated and qualified revisions separately.
`port_impact.affected_ports` joins changed upstream paths to their Rust modules,
adapters, platform routes and acceptance tests, including mappings whose original
files have been retired from the fork. Shared contract changes conservatively
require review of every mapped port and the original server, provider and client
consumers. This is an impact warning; it does not claim every consumer needs a
code change.

`unmapped_changes` retains additions and other changes needing original upstream
implementation or explicit removal review. A missing map is reported as
unavailable; a map or port review naming a different integrated revision fails.
The command leaves the checkout, integration lock and qualification untouched.
`--fetch` only obtains missing immutable Git objects for the comparison.
