# CI and the Rust porting queue

Upstream `pingdotgg/t3code`'s default branch is trusted as working. CI qualifies our Rust replacements, their production adapters, and their integration with the retained application. It does not rerun the inherited application test suite or the client flow using only the original services.

## What each run checks

The qualification planner compares the current commit with a verified successful ancestor. It reads Cargo dependencies and [the port map](../port-map.json) to select work.

| Change | Qualification |
| --- | --- |
| Rust implementation | Changed crates and their reverse dependency consumers, plus affected production adapters and the native client flow when execution changes |
| Mapped adapter or shared contract | Affected Rust ports, adapter typechecks and differential checks, and client/server integration |
| Port tests only | Relevant Rust, mapped component acceptance, or adapter tests without rebuilding the client solely for a test change |
| Unmodified upstream implementation imported at its exact Git identity | No independent upstream test run; mapped contracts and fork integration still select their checks |
| Documentation only | Planning and lightweight tooling checks |
| Shared locks, compiler pins, CI policy, or an unclassified fork integration change | Full fork qualification |

Full fork qualification covers the Rust workspace, production adapters, and the retained client with Rust services. Adapter checks use controlled fixtures and the trusted upstream implementation as a parity reference. The client flow checks onboarding, a provider approval and completed response, reload, and recovery after server restart. It uses no paid provider turns.

Mapped component acceptance scripts have explicit executable requirements. CI builds those targets with the pinned compiler and runs changed scripts; a full qualification runs all mapped component checks. Unknown test mappings fail planning. Builds retain the verified compiler package and locked Cargo and pnpm dependency gates. Expensive jobs run only when selected.

## Reusing qualification

CI only accepts a qualification receipt from a completed successful run of this repository's qualification workflow. The receipt must match the source commit, Git tree, and current CI policy digest, and every selected gate must have succeeded.

A matching receipt for the exact current commit lets a repeated run reuse qualification. A receipt for an ancestor anchors the changed-file comparison. A previous push SHA alone is insufficient: a canceled or failed intervening push must not hide its changes.

Missing, expired, malformed, inaccessible, or incompatible receipts select full fork qualification. Changed CI policy also prevents reuse. This fallback costs a full run rather than silently skipping checks. Restoring a fork modification to pristine upstream still qualifies the changed integration. Edits or deletions of recorded platform receipts always run the source guard, including receipts stored under `docs/`.

## Reading the porting queue

Each qualification run generates a JSON porting plan and a job summary. This uses the existing CI triggers; it adds no schedule. The planner resolves the advertised upstream default branch once, records its immutable commit, and fetches Git tree metadata. It compares three committed snapshots: the integrated upstream baseline, the detected upstream commit, and the fork's `HEAD`.

The queue orders work as follows:

1. Adapt mapped Rust ports to upstream changes, including shared contracts, adapters, and originals already recorded as retired.
2. Resolve fork conflicts, preserve fork policy, account for upstream removals, and review build or dependency boundaries.
3. Import clean upstream changes whose fork copies still match the integrated baseline.
4. Complete acceptance for integrated ports, then integrate candidate ports.

Each port item names its owner, Rust paths and crates, tests, compiler routes, and missing platform evidence. File changes include exact modes and object IDs. Rename suggestions require identical Git objects; they do not infer equivalent behavior.

The queue is a work plan. It does not generate Rust implementations, merge changes, update pins, accept a port, or delete originals. Mechanical import candidates still belong in a complete integration change with its affected Rust work.

Detected, integrated, and qualified revisions remain separate. Discovering a new default-branch commit changes neither the integrated pin nor qualification. CI receipts for this fork also do not replace the per-platform acceptance receipts required to retire original code. The source guard validates those receipts against their current inputs.

## Generate plans locally

Run from the repository root. Reports must stay outside the source checkout.

```sh
t3code_report_dir="$(mktemp -d /tmp/t3code-ci.XXXXXX)"
python3 tools/plan-porting.py --repo-root "$PWD" --fetch-default \
  --output "$t3code_report_dir/porting-plan.json" \
  --summary "$t3code_report_dir/porting-plan.md"
python3 tools/ci_plan.py --repo-root "$PWD" --offline \
  --output "$t3code_report_dir/ci-plan.json"
```

For an already fetched upstream snapshot, replace `--fetch-default` with `--upstream-commit <full-commit-SHA>`. The two options are mutually exclusive. The porting planner reads committed source and requires committed, unchanged lock and map files.

`ci_plan.py --offline` skips remote receipt lookup and plans full fork qualification. `--full` explicitly forces that scope even when reusable evidence exists. These commands write plans; the workflow executes the selected checks.

The current integrated upstream pin is recorded in [upstream.lock.json](../upstream.lock.json). The [8226715 integration](porting/nightly-8226715.md) preserves this CI policy while importing the next pinned nightly. Application Rust still uses the pinned rustc-derived BlueFe package through Lustr's compatibility coordinator. Native Lust and platform retirement acceptance remain separately recorded in the [toolchain lock](../toolchain.lock.json) and [port map](../port-map.json).
