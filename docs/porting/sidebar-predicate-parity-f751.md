# Sidebar predicate parity at f751c848

The source audit following the persistence profile found two native shell
predicates that differed from immutable upstream. The new differential fixture
reproduced both against the preserved native helper before the Rust change. The
corrected helper then passed the complete shell acceptance gate. This record is
local correctness evidence; it makes no performance, full-application or main
promotion claim.

## Source and scope

The working base is fork commit
`192007cc03f418763b1f89eac26211964fc3c1c1`, tree
`0a866653fbbb1e1ae4df1ace6543151c756bd0f8`. Its immutable upstream cutoff remains
[`f751c8488f7ca1e659e44ef12b05861c1bcf9bfb`](https://github.com/pingdotgg/t3code/commit/f751c8488f7ca1e659e44ef12b05861c1bcf9bfb).
The upstream `ProjectionStore.ts` oracle SHA-256 is
`152a0e8a0df7556056e9986005e0ff754f4251300a312840493d1e4fdee1c11d`.

Only the native shell implementation and its existing production-boundary
differential fixture change. The patch adds no schema, ownership marker,
persistent index, compact metadata field, fallback, or backend choice. Both files
already belong to the mapped persistence boundary and its shell acceptance gate.
The retained SQLite layer and immutable upstream source remain the oracle.

## Why the predicates change

### Authored-message suppression is global by run ID

[Upstream's background-wake SQL](https://github.com/pingdotgg/t3code/blob/f751c8488f7ca1e659e44ef12b05861c1bcf9bfb/apps/server/src/orchestration-v2/ProjectionStore.ts#L7631-L7646)
classifies the prompt, then excludes a run if any message with that run ID has
`createdBy = 'user'`. The second condition has no thread filter. Native code had
searched only the messages loaded for the shell's thread, so a user-authored
message recorded under another thread did not suppress the wake.

The correction preserves the existing prompt classification, then traverses the
existing global `MESSAGES/run_id` relationship. The existing bounded iterator
validates each visited relationship and canonical row. It reads `createdBy` from
the canonical JSON with the existing first-member parser, rather than using the
compact copy that has lost duplicate-member order. It stops at the first matching
authored message, as an existence query permits. The obsolete thread-data argument
is removed from the private helper.

The fixture covers a foreign user author, a foreign agent author, a user author
attached to another run, a local user author, and both orders of a duplicate
`createdBy` member. The immutable SQL and retained layer establish the expected
values before native results are compared. All raw duplicate-member edits happen
in the source SQLite fixture before native import and preparation.

### Remote presence differs from open-task recovery

[Upstream's active-shell index](https://github.com/pingdotgg/t3code/blob/f751c8488f7ca1e659e44ef12b05861c1bcf9bfb/apps/server/src/orchestration-v2/ProjectionStore.ts#L8803-L8810)
sets remote presence when an app-owned subagent has no local child thread and its
first `remoteChild` JSON member is an object. A completed task can still satisfy
that condition. Native code had reused the more restrictive open-task flag, which
also considers the result, cancellation and remote stop state.

The correction keeps the existing candidate traversal and its validation, then
reads the canonical payload for app-owned candidates with a null local child. The
existing raw-field parser checks the first `remoteChild` member and distinguishes
an object from an array, scalar, or null. This works for already-prepared stores;
no new compact flag or backfill is needed.

The separate
[open-remote-task query](https://github.com/pingdotgg/t3code/blob/f751c8488f7ca1e659e44ef12b05861c1bcf9bfb/apps/server/src/orchestration-v2/ProjectionStore.ts#L8217-L8244)
keeps its existing native predicate. Twelve source-derived cases cover completed
and cancelled tasks whose remote work is stopped, interrupted and completed tasks
that still owe a stop, a completed task with no stop field, null/array/scalar
children, both duplicate-member orders, provider-native ownership, and a non-null
local child. The test compares both public APIs so presence cannot accidentally
become another name for openness.

## Retained red and green outcomes

The red fixture was added first, with production Rust unchanged. Its staged tree
was `75b5c8ad743b09fc0e5bb606ee7e48d8001ec9b1`. It used the preserved helper SHA-256
`17b743ce8bca3b9dc0bd9accc0055b843017c60f06faf44b9bbba44bab25e6a1`, whose earlier
actual Lustr build receipt is bound by the red receipt. The run matched 148
recorded Rust/Cargo/toolchain inputs and four embedded schema files. The changed
upstream lock and f751 oracle were recorded separately; the entire earlier build
manifest was not claimed unchanged.

The red gate failed in 4.026 seconds with an assertion after both SQLite lanes
passed. Native reported `true` instead of `false` for the foreign-user and
first-user duplicate wake cases. It reported remote presence as `false` instead
of `true` for completed-stopped, cancelled-stopped, completed-without-stop-field,
and first-object duplicate cases. The complete expected and actual maps remain
in `red/stderr.log`; the process did not time out and its owned fixture directory
was empty after cleanup.

The production fix was then reviewed and built once through the pinned packaged
Lustr coordinator and produced BlueFe driver. The build command was:

```text
taskset -c 0 <host-tools>/bin/lustr --bluefe-root <host-tools> build --locked -p t3-code-ls-storage --bin t3-code-ls-db
```

The build used one Cargo job, one CPU, the existing debug settings, no incremental
compilation, and no extra compiler flags. The receipt records the actual command,
selected build environment and driver hashes. This was a normal helper build,
not a retry of a full library-test compilation and not execution of Rust unit
tests.

| Check                           | Result                     |  Elapsed | Exact scope                                                                                                  |
| ------------------------------- | -------------------------- | -------: | ------------------------------------------------------------------------------------------------------------ |
| Preserved-helper red gate       | Expected assertion failure |  4.026 s | Six wake and twelve remote cases; full expected/actual maps retained                                         |
| Actual Lustr debug helper build | Passed                     | 40.781 s | Core and storage compiled; maximum child RSS 1,057,204 KiB                                                   |
| Unchanged expanded shell gate   | Passed                     | 11.193 s | All 14 check groups, including the 18 new cases and existing filtering, count, rollback and restart coverage |

The successful build and gate used staged source tree
`9e54aaab2f7d5a633627499448cbcf0d7c71bae9` and helper SHA-256
`493981dc8aaa1596b5695056a8ac37388160b5e16784d57a0493a8400138e492`.
The compiled-input digest is
`1ba84c863fccfdb72a2ddba1401949426c4e7c0291537516003462d11d453045`.

| Source or receipt                                      | SHA-256                                                            |
| ------------------------------------------------------ | ------------------------------------------------------------------ |
| `redb_projection_shell.rs`                             | `6c140918578439dae756d459628d234fa0f47f9a5b5235361ba052df256fc336` |
| Unchanged red/green `check-redb-projection-shells.mjs` | `1dde2bc4d062adb80a7a2b719b3b296992deee6a9f130435fd1b7700b731a3eb` |
| Red receipt                                            | `d109826ada30d17f50d2ef0bb6b9adfdd1459a69f026da54c1cfa3bb4c558e32` |
| Build receipt                                          | `6c240bcc1d87f479f53b34de17a9a50d610c0267a004e6afbd8d74924c807bdd` |
| Green receipt                                          | `e8063cd3a350c7d97c2c4f13364e35236225815b8a426ac367a77ab6c7056c76` |
| Green proof                                            | `06b14646d389f497b84f5223a3bc2e261b6c7e1cf2b7eb2954579c83f079ec49` |

Source, test and helper hashes remained unchanged during each recorded run. The
successful gate's stderr contains only Node's normal experimental
`stripTypeScriptTypes` warning. Both fixture roots were empty at completion.

The [raw evidence archive](evidence/2026-11-14-sidebar-predicates.raw.tar.gz) contains
12 entries: all three receipts, complete stdout/stderr/build logs, the green
proof, exact fixture and production patches, and a per-file byte/hash manifest.
Archive SHA-256:
`9d9670f73e7379dd932348e4565c2ae4d6b2a644cec2e477848b1d2bdc28ad3c`.
Every archived entry was read back and compared with its retained original.

## Decisions and remaining qualification

- Adopt the two source-proven correctness changes now. A performance audit should
  not preserve a wrong predicate merely because it is cheaper.
- Keep the existing background-prompt classifier and open-task recovery predicate.
  The fix does not rewrite unrelated compact metadata or claim to exhaust all
  historical malformed-JSON cases.
- Defer the separate sidebar overfetch reductions. They need their own selection,
  corruption, snapshot and observation review and an unchanged fixed comparison.
- Keep the strict performance contract and main promotion gates unchanged. No
  performance comparison was run for this fix, and a debug acceptance run is not
  speed evidence. The canonical-row lookups may add work for background-wake and
  remote-child cases; their latency effect has not been measured.
- Require hosted compilation/tests, the actual normal commit hook, final typecheck,
  full application/recovery checks and performance qualification on the eventual
  exact candidate. This local record alone does not qualify it or advance main.
