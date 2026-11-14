# Upstream integration 5fe2f7e4

The next immutable upstream cutoff is
[`5fe2f7e4567d9e530ded0dabc9d706db27d77b84`](https://github.com/pingdotgg/t3code/commit/5fe2f7e4567d9e530ded0dabc9d706db27d77b84),
tree `5d1b0d638e32a15f4ea85924c1e751b7da12a92a`. It contains 63 commits
and changes 162 paths after the [913d7fc4 integration](default-913d7fc4.md).
The merge retains the upstream history and the fork history through
`d661dbb2bf693b88c8969fcda9a0f5b98e3a2cca`. The source inventory and mapped
oracle blobs identify this cutoff; later upstream movement remains a separate
detection and integration decision.

During candidate preparation, upstream revision
[`f751c8488f7ca1e659e44ef12b05861c1bcf9bfb`](https://github.com/pingdotgg/t3code/commit/f751c8488f7ca1e659e44ef12b05861c1bcf9bfb),
tree `a8157c768c81bdb0564612e2adca54dde1411a22`, was detected. It is a
separate successor for review, not part of this integration. The candidate's
CI, profiling and stock comparison keep 5fe2f7e4 fixed throughout the attempt.

This source advance is not a qualification receipt. Existing main remains at
`3577878a5e008c8af87a8fbdae13d26dea1c2f50` while the candidate completes the
current native, adapter, application and stock-comparison gates. This record
does not claim that existing main has passed the expanded performance policy.
Every existing performance attempt through the write-pruning comparison used
upstream 913d7fc4. Those measurements remain evidence about that source and
cannot qualify this cutoff.

The local normal commit hook passed formatting and lint, then stopped during
the complete Knip task without a finding or explanatory diagnostic. Two owned
diagnostic invocations of the unchanged files/dependencies scan received
SIGKILL. The first used a 768 MiB V8 heap and recorded 699,040 KiB maximum child
RSS over 35.285 seconds. The second used a 384 MiB heap and Knip's supported
normal-AST transport switch, recording 669,260 KiB over 28.088 seconds. Both
used one CPU. These observations establish process termination, not its cause
and not a successful unused-code check. No third local scanner retry was made.
The [original hook and diagnostic evidence](evidence/2026-11-14-hook-diagnostics.raw.tar.gz)
is retained unchanged.

The reviewed staged tree is therefore published only as an unqualified CI
candidate through Git Data. Its required application job runs the actual
normal hook in a clean disposable GitHub-hosted checkout and rejects any
source change, failure, timeout or incomplete process cleanup. This candidate's
TypeScript and MJS changes select both existing Knip scans. The gate preserves
their normal scope and does not repeat the task after a successful hook.
Hosted hook success, the full Rust suite, application execution and the strict
new-stock comparison are still required before promotion; local publication
does not claim any of those results.

## Retained upstream changes

The imported server changes make a new thread and its initial run commit
together, preserve later unsent steering prompts during manual continuation,
refresh VCS state after checkout, and distinguish a branchless cleanup
candidate from a thread whose run has not stopped. The merge also retains the
upstream provider, web, desktop, mobile and shared-library changes at their
original paths. This includes attachment and draft recovery, connection cleanup,
scheduled-task editing, document previews, citation escaping, shell executable
resolution and Markdown link handling.

The new server-only `thread.create.initialMessage` contract is handled by the
shared orchestrator and launch service. Their resulting events, effects and
receipt still use the selected native transaction owner. The experimental
standalone Rust API remains a separate component; it is not the clients'
production server or a replacement for the shared orchestrator.

No application migration was added: the canonical schema remains 91. The
existing schema-80, schema-88 and schema-89 upgrade paths and activated-home
fence remain required. The queued provider-thread placeholder predicate moved
to the shared contracts package without changing its conditions. The native
predicate therefore keeps the same status and null checks.

## Shell count parity and writer compatibility

The stock shell query subtracts queued user inputs by run ID across item
threads. The previous native query restricted that subtraction to the item's
thread/run group and represented the result as an unsigned count. A
source-derived fixture exposed the difference before the repair: the immutable
913d7fc4 stock query returned `-2` where the preserved b5250fe8 native helper
returned `1`. These stock count declarations are unchanged between 913d7fc4
and 5fe2f7e4, with declaration SHA256
`8a5ba7bdcaf1a8a84701d3f0cdcf630abb6acfc458adc473d0e8386cb41d2217`.
The combined candidate must execute the final differential at 5fe2f7e4.

The count repair uses migration 85's canonical count rows and a bounded index
of their thread/run groups. The index retains group identity and per-thread
cardinality witnesses; it does not duplicate the count total or silently
recount imported data. Signed results, globally queued inputs, cancelled and
rolled-back runs, missing groups and empty run IDs follow the source query.
Canonical mutations remain in the existing projection transaction.

The imported SQL table permits zero and negative count values. Its triggers
also distinguish a NULL run ID from an empty string before coalescing their
storage key. The repair preserves those cases. During a schema-80 upgrade,
source groups containing both NULL and empty run IDs for the same thread must
fail the same unique-key boundary as migration 85; a bounded staging witness
detects that collision before activation and leaves the source generation
intact.

An older schema-91 executable cannot maintain the new group index. The
candidate therefore needs an owner-version fence before preparing or using
that index. The turn-item table changes from `projection-commit-v1` to
`projection-commit-v2` within reserved preparation. Older writers observe this
marker before mutations: a newly started old writer rejects it, and an
optimistic writer that already observed v1 must conflict after the transition.
The other table ownership markers retain their existing versions. Staged
upgrades establish the same fence before publishing the active generation.

This compatibility boundary is part of the repair, not an optional performance
feature. Ownership, interruption, conflict and real old-helper checks remain
required before main promotion. Candidate publication records the local checks
actually completed and still requires the full native CI gate. Existing v1 stores with an unexpected count-index
namespace, unknown owners or invalid upgrade lineage are rejected; their
witnesses are not adopted as valid preparation state.

### Verification boundaries

The count fixture executes the immutable projection declarations and migration
85 itself. Its three SQL migration cases passed, including rejection and
transaction rollback for distinct NULL/empty groups that coalesce to one key.
The earlier red fixture remains evidence of the original native discrepancy.
Both fixtures are retained through the count helper, and the source map binds
the migration blob. Helper-only edits select the shell, startup and migration
acceptance gates in the CI planner.

The initial local Rust test compilation ended with SIGKILL before running any
tests. A second invocation could not start because its timing executable was
unavailable. A subsequent one-job compilation also ended with SIGKILL before
tests, after 65.847 seconds; its wrapper recorded maximum child RSS of
1,053,764 KiB and exit status 101. SIGKILL alone does not establish the cause.
These incomplete attempts are not test passes, and the full pinned Rust suite
remains required on the exact combined candidate.

The [unchanged diagnostics, proofs and manifest](evidence/2026-11-14-count-verification.raw.tar.gz)
retain the original mismatch, SQL oracle, all compilation invocation outcomes,
failed shell attempt, corrected component checks and old-helper compatibility
proof. The archive contains 33 entries and has SHA256
`b5118072cded904e0d398c956ef84986245b425c71b41926d153eb6f6caf1036`.
The failed lib-test build logs describe uncommitted development before the final
owner fence; they do not establish a completed candidate build.

| Retained diagnostic            | SHA256                                                             |
| ------------------------------ | ------------------------------------------------------------------ |
| Original count mismatch        | `3dba436172793413ac844470ae88bdbaf7d4ab5c976f2b76bb355c5ed1a0af4d` |
| Three migration85 oracle cases | `a09be891cf17f45b601bda936d53dc287187199118a939c280f21d1150688f88` |
| First compile termination      | `20e43f81ef5039d8c012fa6a45e80473a59d0f61e0ea4da0572364ffc10f2d2e` |
| Missing timing executable      | `3e737f6a9117cec194b02103b357f8668fae9adb86a8a8cd0bf25efab8948548` |
| One-job compile termination    | `1b6cf3993fafbb9bf37746656f5bf7450b67ccb1d980dbbefc35ef795c42520c` |

`check-redb-count-writer-compat.mjs` is a separate compatibility proof with two
explicit executable arguments. It pins the preserved old executable and
records both executable identities. Normal CI runs the source-derived count
and migration differential plus Rust ownership/conflict tests; it does not
pretend an unavailable old binary was tested or silently skip that proof.

The first successful normal debug helper build used the packaged Cargo and
produced BlueFe driver directly, without the required Lustr coordinator. That
build is retained as a diagnostic only. A supported cleanup of this task's
storage development artifacts was followed by a fresh one-CPU, one-job build
through the packaged Lustr coordinator and produced BlueFe driver. It completed
in 34.572 seconds with unchanged Rust/build inputs. Its receipt is SHA256
`918e85d623f5dc7332a94c64f9e7b9ea062f732828892e30dadcbd97156c1008`,
and its log is `5b95dd88d2808ce68733d49b3d612ffbfed3052304b43ad8d55219cbc3c71b10`.
The resulting helper SHA256 is
`17b743ce8bca3b9dc0bd9accc0055b843017c60f06faf44b9bbba44bab25e6a1`.
The two builds produced identical bytes; that identity alone does not prove a
compilation route. The separate invocation and tool hashes establish the
corrected route. Neither normal helper build executed the Rust unit tests.

The real old-helper proof then passed 54 invocations and seven exact state
snapshots. It checked refusal both before and after count-index preparation,
unchanged canonical counts and group results after rejected old inserts and
moves, and new-helper writes surviving fresh reopens. It used the preserved
d661dbb2 executable with explicitly unverified old compiler provenance. The
proof SHA256 is
`a71cebed4ec68ecd3c47a818024a2e052cf088ddb8824aa1fe02f3035f9b04b0`.

The final formatted fixture passed the following local checks at the immutable
5fe2f7e4 oracle. Every recorded source/helper stability check passed.

| Local check                                                          | Elapsed time | Proof SHA256                                                       |
| -------------------------------------------------------------------- | -----------: | ------------------------------------------------------------------ |
| Shell/count differential, including actual migration85 cases         |      8.456 s | `4106d152bcde0427abdc7d93117378161d702a6628bb2491caf5291d96253d87` |
| Selected production persistence graph and schema80 migration/restart |      8.241 s | `4d107aeea1da65d3fdb3c4a51d6d18981f53320d55528aed4d088ad93c716314` |
| Native startup, import and schema88/89 upgrade paths                 |     10.620 s | `0253fcfcc43c7e165f8ff70dad4f263ed6bdedff253025767b3f837bce48b542` |

The aggregate local receipt is
`1a96fa8665380efe5b47c8c058b05fe9e7527cfa0f693486946a9d0f883da982`.
These are component acceptance checks against a debug helper. They do not
establish full client application, release performance or platform acceptance.
The full exact-source Rust suite and all remaining hosted gates stay mandatory.

### Deliberate corruption-handling divergence

The first combined shell differential failed after its fixture corrupted the
stored payload of the same turn item that a later scenario moved. Stock's SQL
update completed that sequence. The native writer rejected the malformed
prior payload with `incompatible_store` while validating the existing
projection. The fork retains this existing integrity guard rather than
overwriting data it cannot validate. This is an explicit corruption-handling
difference, not a claim of parity for that malformed mutation.

The corrected fixture keeps a distinct, never-mutated malformed reasoning row
on the same queued run. Count reads must still match the stock query without
parsing a reasoning payload that the query does not inspect. Movement,
trigger, rollback and committed-reopen cases operate on separate valid rows.
This preserves the count-query coverage and ordinary valid-row mutation
parity without weakening the native writer's corruption rejection. The
original failed attempt and its source identities remain retained.

## Manual-continuation execution boundary

`manualContinuation.ts` remains the single shared prompt-selection algorithm.
It compares the greatest accepted attempt ordinal with the greatest
`steering_restart` ordinal. A later restart leaves the source prompt unsent;
an accepted attempt at or after that restart does not. Its existing chain and
cycle handling remain in the same helper.

Both projection implementations call this helper through
`makeProjectionContexts`. That shared factory selects the current run's user
message and any unsent source message, then passes their explicit IDs to its
row service. On the native route, `NativeProjectionContextRows` sends those IDs
to the Rust `turn_messages` query. Rust owns row selection, transaction
consistency and the existing compact/goal message filters. It does not contain
a second copy of the manual-continuation algorithm.

The source map now includes this helper explicitly, along with the shared
thread-find and Markdown normalization dependencies. Their source changes must
remain visible during future port reviews. Native thread-find row services
return raw message and plan text to `makeThreadFindWithRows`; its shared
searchable-segment functions perform the Markdown interpretation. Rust's
message-search normalization is a separate port of the unchanged
`messageSearchIndex` and assistant-citation behavior. The PR-link event
normalization in `NativeProjectionCommitStore` is separate again. Keeping
these boundaries explicit avoids divergent unused copies of the find parser.

## Fresh comparison required

### Focused source-boundary checks

The context differential passed against the immutable 5fe2f7e4 projection and
manual-continuation sources. Four accepted-attempt/restart stages exercise the
real native context factory, exact source prompt and attachment retention,
ancestor exclusion, and deliberate rollback with the exact expected sentinel.

The thread-find differential also passed six new real native write/read cases:
bare and escaped parenthesis paths, entity-decoded user links, angle-wrapped
paths, Windows plan paths and literal ampersands in Codex citations. It asserts
nonempty matches and excluded-text counts, and byte-attests the shared
thread-find, Markdown and citation parser sources to the new pin. These checks
found no additional Rust algorithm gap at those shared boundaries.

Both focused runs used the preserved d661dbb2 release helper, SHA256
`1d04aae57b316d02eba6452c04b81bc616ab6d554f9686c76e973aa58068b60c`.
They establish the new shared-source behavior at the unchanged context/find
interfaces. The combined count/backend candidate still needs its rebuilt
helper and exact-source qualification; these focused runs do not substitute
for that result.

### Changed stock runtime

The stock SQLite client changes its reader admission and cancellation handling.
It now limits acquisition through an admission semaphore, claims an available
reader and registers release ownership within the acquisition mask. This is a
material change to the actual stock service path used by the comparison.
The frozen workspace lock also changes with the upstream scripts dependency.
The next comparison must therefore export 5fe2f7e4, install that frozen graph,
verify the installed runtime and run the complete fixed workload again.

The [write-pruning measurement](../performance/measurements/2026-11-14-write-pruning.md)
retains the completed d661dbb2/913d7fc4 attempt. Correctness, rollback and reopen
checks passed, but all eleven latency rows regressed. Visit median latency was
1.397 ms for stock and 5.780 ms for native, with paired ratio 4.093 and 95%
interval [3.814, 4.380]. No write speedup was established. The
[hosted table-handle record](../performance/measurements/2026-11-14-hosted-table-handle.md)
also preserves its original source and failed speed verdict.

Measured persistence API slowdowns remain the next porting priority. New
transport or platform cutovers wait behind correcting and qualifying the
existing native path. The fixed workload, individual row gates and Immediate
native commit acknowledgement remain unchanged. All failed and inconclusive
attempts stay available; no earlier passing application artifact substitutes
for the full qualification of this new source.

## Source retirement and qualification scope

### Removing experiments without demonstrated benefit

The complete table-handle and write-pruning attempts did not establish a
performance benefit against stock. The table cache added a retained engine
handle/root page to each immutable read transaction; pruning added a map
lookup and mutation to point-observation validation. Their temporary retention
kept the individual experiments isolated. That purpose is now complete, and
the candidate removes both production changes instead of accumulating
unproven state and branches.

The production backend returns to the dispatcher-only c3f8245 behavior:
each existing read operation opens its snapshot's table, and every validated
staged mutation follows the ordinary engine write path. Snapshot-scoped
readiness validation and the joined dispatcher remain. Immediate commit
acknowledgement, reservation, cancellation and uncertain-outcome handling
remain unchanged.

General tests remain for old/new snapshot and catalog visibility, transaction
guard release, cancelled reads, byte limits, writer savepoints, point/range/
fingerprint conflicts, and real flush failure for mixed and entirely identical
writes in both writer modes. Cache-initialization assertions and the removed
cache type's trait assertion no longer describe an implementation and are
removed. The combined candidate must pass those retained tests again.

All original measurements remain attached to their measured source. The
[hosted write-pruning report](../performance/measurements/2026-11-14-hosted-write-pruning.md)
also records a severe visit latency tail, without attributing it causally to
pruning across different hosts. The removal is a complexity decision based on
the absence of an established benefit, not a claim that the next candidate is
faster. Fresh paired evidence is still required.

### Preserved application consumers

The merge preserves the existing native entrypoints and source-retirement
records. It does not restore a retired business implementation into a native
entrypoint. Shared orchestration, normalization, codecs and platform code still
serve real application consumers and remain required. No new whole-platform
retirement follows from this source merge.

SQLite remains the immutable stock oracle, migration source and implementation
for unqualified host routes. An activated native home cannot fall back to it.
Linux browser evidence does not qualify desktop, mobile, macOS, Windows, Iroh
or native Lust. The current full Rust compiler route remains the pinned
Lustr coordinator with the produced rustc-derived BlueFe toolchain.

The fork workflow records exact-candidate qualification independently of this
document. Source guards and updated oracle identities establish accounting;
native API differentials, real transaction failure paths, the original client
flow and the paired comparison establish their respective behavior scopes.
