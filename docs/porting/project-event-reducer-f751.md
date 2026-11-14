# Native project-event reducer at f751

This partial source candidate moves representable project event reductions into
Rust through the existing application database helper. Inputs outside that
bounded representation select the one retained TypeScript reducer before any
native reduction request, keeping the same native rows and transaction. It is based on
`4b5719f34a8b76a23f3c77982940cea67234186c`, with immutable upstream
`f751c8488f7ca1e659e44ef12b05861c1bcf9bfb`. The exact local functional run below
passed both production typechecks, 37 targeted Rust tests and four real-helper
gates. It remains a partial API candidate: the added helper exchange has no
accepted latency measurement, the original reducer remains necessary, and this
f751 result does not qualify a newer upstream candidate or main.

## Boundary and retained ownership

The immutable [ProjectStore implementation](https://github.com/pingdotgg/t3code/blob/f751c8488f7ca1e659e44ef12b05861c1bcf9bfb/apps/server/src/orchestration-v2/ProjectStore.ts)
is the complete apply/upsert oracle. The existing fork already uses native
project command planning and native project rows/indexes; its event-to-row
reduction remained in TypeScript. This change connects that reducer to both
`SelectedPersistence.projects` and `NativeEventSink.projects`.

| Boundary                                                                              | Implementation after this change                                                                        |
| ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| Public project commands, WebSocket/HTTP authorization and client contracts            | Existing application interfaces                                                                         |
| Project/workspace locks, clone/delete checks, filesystem actions and command planning | Existing ProjectService and native command planner                                                      |
| Event-to-row field reduction                                                          | Existing Rust project module when representable; retained reducer selected before the request otherwise |
| Shared project codecs, get/list/workspace reads and shell mapping                     | Existing ProjectStore shell                                                                             |
| Canonical row upsert, custom-title preservation and ordered indexes                   | Existing native project row repository                                                                  |
| Receipt reservation, event append, commit, rollback and publication                   | Existing EventSink and RedbClient transaction owner                                                     |
| Upstream/platform route                                                               | The same retained TypeScript reducer and SQLite row repository                                          |

The standalone native listener remains a separate candidate. Its own Store,
transport, command receipts and authentication do not replace the application's
public API. No listener, second database owner, row format, schema version,
managed marker or migration is introduced here. All clients remain in place.

## Decisions that preserve the complete boundary

**Creation does not decode an old row.** The shared shell passes null to the
reducer and obtains a fresh row with creation defaults. The unchanged upsert
reads the prior encoded record and preserves its custom title and flag. This
allows creation to replace old malformed model, icon or script JSON and an
unsupported environment string, as the stock upsert does. Required cell, bit and
index-witness checks in the native row owner remain in force.

**Updates and deletes retain their original get.** They call
`get(projectId, { includeDeleted: true })` first. Missing rows return successfully
without validating unused event fields or issuing a reduction request. A failure
decoding an existing row retains `ProjectStoreV2Error.operation = "get"`; it is
not wrapped into a new apply error. Reduction, encoding and upsert failures use
the existing apply boundary.

**Rust field reduction and normalization are separate.** The reusable core
function previously combined both for its standalone planner. The shared field
reducer is now separately callable; the standalone `project_event` still runs
its existing normalization afterward. The production adapter receives an
unnormalized row and keeps the existing `ProjectDbRow` encoder before upsert.
This matters for direct events: trimming an icon name before its pattern check,
or decoding a legacy icon before the original encoder, would change acceptance.

The wire input schema selects only fields consumed by apply, reusing the
`ProjectRow` field schemas through `Schema.toType`. It removes ignored envelope,
payload and nested extension fields before JSON serialization, without running
the normalization transforms. It deliberately does not substitute the stricter
public mutation schema: direct apply accepts plain-string titles/workspace roots
and ignores its event envelope. At this pin `IsoDateTime` is `Schema.String`;
arbitrary date text remains valid. No calendar validation is added.

**A domain rejection does not poison the database transaction.** The pure
operation returns a normal `reduced` or `invalid_event` result. The adapter turns
the latter into an application error after the successful request. Returned-row
type validation and the existing row encoder also remain outside the request's
database-error handling. A caller that catches a domain/codec failure can still
read or write another project and commit, as with the stock row encoder.
Malformed envelopes, absent rows and wrong-project response identities follow
the existing protocol failure path. Session, storage, cancellation and uncertain
write failures retain their existing handling; no fallback or replay is added.

**The helper operation is pure and session-bound.** It takes and restores an
active application session, performs no row access, and owns no commit. A read
session can reduce a supplied event but cannot use that result to bypass the
unchanged write-only upsert. Creation reduction, an update's preceding get,
encoded upsert, events and receipts all join the current EventSink transaction.

## Representation bounds and compatibility selection

The outer database protocol retains its 16 MiB frame limit. The pure reducer
also checks the combined UTF-8 length of `existing_json` and `event_json` against
`MAX_BATCH_BYTES`, and parses each argument with depth 64 and 262,144 values.
Oversized or malformed pure inputs produce the domain rejection envelope; an
invalid outer frame/session retains the existing protocol handling.

The [initial source freeze](project-event-reducer-f751.rejected-source.raw.tar.gz)
was rejected before execution because documenting a narrower input limit does
not preserve the application contract. A nine-MiB old title and a nine-MiB new
title each fit the former separate get/upsert requests, while the combined pure
request cannot fit. Escaping the inner JSON strings can also exceed the outer
frame when their combined inner JSON still fits. A legal large script array is
stored as one encoded JSON text cell by the row repository but can exceed the
pure reducer's parsed-value count. Finally, JSON text cells can retain escaped
lone UTF-16 surrogates that Rust strings cannot represent after decoding.

The revised adapter checks representation **before calling `client.request`**.
It first uses the selected-field type codecs, which remove ignored extensions
without normalizing the consumed fields. It checks both combined UTF-8 input
bytes and the actual escaped JSON payload against 16 MiB minus 1,024 bytes.
That reserve exceeds the fixed operation/envelope/newline plus the longest
generated session ID, `application:9007199254740991`, and also covers creation's
added defaults and the success envelope. The reduced row contains no new
unbounded data: its strings/arrays come from those two inputs.

A depth-bounded iterator counts containers and scalar values as the Rust parser
does, excluding object keys. Its conservative combined count reserves 32 values
from the 262,144 per-input limit, and it leaves one container level below depth 64. Strings and object keys must be well-formed UTF-16. The JSON inspection is
over codec-selected values, so a deeply nested ignored extension does not alter
selection. There is no new public rejection caused by these eligibility checks.

If any check is false, the adapter uses the one retained pure reducer and then
the unchanged row encoder/native upsert. This preserves encoded JSON text cells
and the former native size boundary; it does not select SQLite, acquire a new
owner or commit separately. No native response, domain rejection, timeout,
session error, protocol failure or unknown outcome may trigger that route after
a request. The normal error and no-replay paths remain in place. This explicit
compatibility need is why the TypeScript reducer cannot yet be retired.

## Verification boundary

The existing project differential now constructs the selected native service,
executes immutable stock apply plus its real upsert, and compares the complete
result and stored state. It includes ordinary and direct-event cases, explicit
null/undefined, normalization, imported legacy icons, automatic recreation,
sticky custom titles, deleted rows, missing updates/deletes, and malformed prior
JSON replaced by creation. Native request observations require creation to call
reduction plus upsert without a get, and missing updates/deletes to stop after
their get.

Additional cases catch invalid selected fields inside root and nested
transactions, continue with unrelated writes, commit, and reopen. Forged reducer
envelopes and identities must reject before an upsert and retain terminal
protocol handling even if the apply error is caught. The event-sink gate keeps
the existing shared project/thread receipt, replay, writer contention, savepoint,
rollback and publication checks. Its new fault cases inject a typed reducer
rejection after the real pure request, interrupt after reduction and before
upsert, and lose real reducer/upsert replies. They check complete row/event/receipt
rollback and absence of publication or fallback.

Representation cases compare immutable stock with both the former native
rows/shared-reducer composition and the revised native service. They exercise
nine-MiB old/replacement titles, an escaping-only frame overflow, 45,000 valid
scripts, lone high and low surrogates inside model/script JSON cells, and valid
paired-Unicode/literal-backslash controls. Request observations distinguish
pre-request compatibility selection from actual Rust execution. Each complete
persisted row is represented in evidence by its length/count and JSON hash,
then checked again after reopen; no megabyte payload is dumped into a receipt.
Caught representation-input codec failures must also permit unrelated
root/savepoint writes to commit. The blank-script case may fail the consumed-field
type codec before eligibility selection; it is not evidence of a later encoder
failure after the compatibility route has already been chosen.

Rust cases cover pure reduction in read sessions, ended/missing sessions, no
persisted mutation, domain rejection without writer/savepoint poisoning, bounded
input rejection, and the separation of raw reduction from standalone
normalization. Existing sticky-upsert/index/corruption and transaction tests
remain required. The mapped component gates, normal hook, final production
typechecks, full selected Rust suites, application/browser restart flow and
strict performance qualification must run on the exact final source.

The first local execution attempt stopped in its runtime preflight before any
typecheck or test: inherited root aliases included application packages outside
the frozen pnpm store. The isolated checkout now uses the exact existing locked
production targets for 22 aliases, and removes two undeclared root SDK aliases;
the donor runtime and production peer dependencies were not changed. The next
preflight passed, then the first project typecheck failed on three new-code
Effect diagnostics: the reducer interface exposed an `unknown` error channel,
and its impossible-null guard used an untagged `Error`. No Rust build or
application test ran in either attempt.

The correction keeps the shared reducer factory generic in its precise error
channel, with `never` for the retained pure reducer, and tags the impossible-null
failure while keeping its message and outer apply-error boundary. It does not
change requests, codecs, transaction ownership or compatibility selection. Both
failed attempts and the earlier source freezes remain retained. Later attempts
and the final successful ordered sequence are accounted for below.

The third attempt's strict runtime preflight also passed. Its first typecheck
confirmed that the unknown-error diagnostics were fixed, but rejected inference
of the tagged-null and upsert branches inside a conditional `Effect.flatMap`
callback. The resulting unknown-requirements diagnostics were retained with the
complete log; no later validation stage ran. The revised helper uses
`Effect.fnUntraced` generator inference and the same `mapError("apply")`, preserving
both failures without a cast, suppression or additional tracing span. The outer
`ProjectStoreV2.apply` remains the traced operation.

## Executed local validation

Attempt 05 passed the complete ordered local plan against reviewed source tree
`f711b36da9f0c1d6e69fafceb5df41ef1c53cf38`. All 180 execution inputs, the 160
Rust/Cargo/schema build inputs, isolated runtime links and toolchain files stayed
unchanged during execution. The source manifest, exact commands, selected build
environment, resource bounds, full logs and receipts are in the
[validation evidence archive](evidence/2026-11-14-project-event-validation.raw.tar.gz).
This report and archive were added afterward; the production and fixture bytes
remain those of the tested source freeze.

| Check                                       | Observed result                                                                                   |
| ------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| Production project and EventSink typechecks | Both passed                                                                                       |
| Core project reduction                      | Four exact tests passed, including raw reduction versus codec normalization                       |
| Native project rows/reduction               | Nine exact tests passed, including the three new session, domain-error and bounded-input cases    |
| Native application transactions             | All 24 exact tests passed; zero failed or ignored                                                 |
| Standalone project persistence              | 98 comparisons, 57 projected rows, 14 required projection scenarios and eight frame checks passed |
| Complete project-store differential         | 18 functional checks plus nine compact representation witnesses passed                            |
| EventSink differential                      | 18 checks passed, including caught domain failure, cancellation and lost-response cases           |
| Real RedbClient transactions                | 34 checks passed                                                                                  |

Both helpers were built through the pinned Lustr coordinator and BlueFe driver,
with one CPU and one Cargo job. The preserved project helper has SHA-256
`92a4736a77336e7b9a4063f48f73714667a3807da265de8b2ea0897c3c323318`
and the database helper has SHA-256
`393f0c44359499a00d82592b5a9eb48762ede3a3fcad36a4a78f712fdf85e6bb`.
The aggregate receipt is
`acf28012349c270ce7c1ccdb07a2219352edc69252ab1a34f8e62726c29d607d`.
Each gate binds its actual helper and source hashes; both differential oracles
bind the immutable f751 source. The fixture directory was empty after completion,
and each owned process group had exited before the next stage.

The representation proof retains nine row hashes with only project IDs, title
lengths and script counts. It includes the 9,437,184-code-unit title and 45,000
scripts without embedding their payloads. The largest helper-gate log is 6,478
bytes. Normal Node experimental-type-stripping warnings remain in the complete
logs; they are not removed or described as an error-free console.

### Failed and incomplete attempts remain evidence

| Attempt | Outcome and retained limit                                                                                                                                                                                       |
| ------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 01      | Runtime preflight failed before execution because inherited aliases did not all use the frozen application store.                                                                                                |
| 02      | Corrected runtime preflight passed; the first typecheck failed on the new reducer's error-channel and untagged-null diagnostics. No later stage ran.                                                             |
| 03      | Preflight passed; the first typecheck failed on conditional error-branch inference. No later stage ran.                                                                                                          |
| 04      | Both typechecks and four core tests passed. The storage command exited zero but ran only six old cases, omitting all three new tests; its explicit review marks source execution incomplete. No later stage ran. |
| 05      | After the bounded cache cleanup, both typechecks, all 37 exact targeted Rust cases, both helper builds and all four gates passed.                                                                                |

The attempt-04 storage executable had an 18:00 modification time, while the
reviewed project sources retained 17:53 modification times. The command working
directory and dependency paths were correct. This supports stale reuse across
checkouts sharing a target directory; it does not establish a specific
Cargo, Lustr or compiler implementation bug. The old executable, dependency
records and core metadata were copied and hashed before cleanup.

The supported Cargo cleanup selected only the core and storage development
packages. Its preview printed 922 lines representing 461 distinct paths. The
actual verbose output described the two package build roots and 36 additional
package-owned binary/example aliases, so it was not a strict subset of that
preview. Every observed removal belonged to those two packages' development
outputs; no observed removal was a release or dependency-package path.

A separate evidence-preparation error also remains explicit: the full inventory
script checked the preserved count-helper directory as though it were a file,
failed before saving the inventory, and the orchestration still invoked the
separately authorized cleanup because it had not checked that exit code. There
is no complete before/after target inventory to claim. Subsequent checks proved
that the expected package roots were absent and all seven specifically preserved
artifacts still matched their earlier hashes. Existing dependency files alone
cannot prove equality to the missing baseline.

Attempt 05 checks dependent subprocess outcomes and evidence preconditions
before proceeding. Its Rust acceptance requires each exact expected name once,
matching nonzero counts and a successful summary with zero failed, ignored or
measured tests. A zero-exit command without those witnesses is a failure of
acceptance. The retained attempt-04 log was verified to fail this new predicate;
the final fresh run passed it with all four, nine and 24 expected names.

The archive contains all five attempts, source revisions, plans, supervisors,
static results, runtime correction, cache identity records and cleanup limits.
Its 136 evidence payloads match the retained originals; the manifest and all
archive entries were read back and verified. Generated helpers and stale binary
payloads are identified by hash and kept as
local immutable copies, not committed to Git. Archive SHA-256:
`84425411856456ee839c5fdd8ac4e89b0ef980e20f44a720c32d83bddf69b335`.

These are targeted local functional results. The normal hosted hook, complete
selected Rust suites, current application/browser/restart gates and performance
qualification remain required on any final integrated candidate. No original
source is retired on the strength of this run.

## Cost and a separate project API comparison

Each Rust-selected event adds one helper exchange, local eligibility inspection
and bounded JSON encoding, parsing and validation. Updates/deletes also transmit
the already-decoded old row. The compatibility route avoids that exchange but
still performs selection and the retained reduction. Both costs are unmeasured;
no project API speedup or zero-overhead claim is made. Missing updates/deletes
retain their existing no-op path. A later compound
native apply could remove the exchange only after separately porting the codec
and error semantics.

The fixed eleven persistence workloads and their 1.0 upper-bound requirement are
unchanged. They do not isolate this reducer's cost. Before claiming project API
speed acceptance, add a separately named comparison of the complete
`EventSink.commitProjectCommand` boundary, including receipt, append, reduction,
upsert and durable acknowledgement. Its stock route must use the immutable full
upstream tree and production SQLite settings; native keeps Immediate durability.

Freeze separate rows for create, title change, metadata-only update, delete and
automatic recreation of a custom-titled project. Use distinct event/command IDs
and changed values per measured mutation, small and declared script/model payload
sizes, fresh isolated files, identical initial state and nonempty full-row/event/
receipt witnesses. Missing and replay cases belong in explicit functional or
separately labelled no-op rows. Include catch-and-continue, rollback and reopen
before timing. Keep setup/index preparation outside warm measurements, and
report service acquisition separately.

Use balanced process order and the established eight-pair, three-warmup,
twelve-cycle protocol; retain every raw observation, median, p95, worst case and
whole-pair confidence interval for each project workload. Bind exact source,
compiler, helper, dependency/runtime and host identity. No aggregate may hide a
slow operation. A comparison with the pre-port native helper requires a separate
same-host controlled attempt; results from unrelated hosted jobs cannot establish
the exchange's causal cost. This is a measurement plan, not a new result or a
replacement for the current failed qualification gate.

## Source retention

The existing TypeScript reducer remains exactly once for the upstream/platform
route and the explicit native representation compatibility cases. Shared codecs
and application interfaces are still used by both routes.
No source file is retired and no Windows, mobile, distribution, provider, UI or
whole-stack Rust qualification follows from this source integration. The
`port-map.json` ownership and required component gates include the new adapter.
