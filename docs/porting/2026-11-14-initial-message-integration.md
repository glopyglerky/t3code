# Initial-message integration repair

The first hosted qualification of the 5fe2f7e4 integration stopped on a
TypeScript error in the new `thread.create.initialMessage` path. The repair
routes that call through the existing native guest decider and its command-error
translation. The immutable upstream cutoff remains
`5fe2f7e4567d9e530ded0dabc9d706db27d77b84`; this change does not advance the
comparison oracle or change the performance contract.

## Exact failed candidate

[Run 43612612179, attempt 1](https://github.com/glopyglerky/t3code-ls/actions/runs/43612612179)
tested commit `a2fb16d84f089e2a7ca681e431e6acff083695bf`, tree
`9aca39040e56c362ed1edba9e69d49602b5feee8`. Its ordered parents are the fork's
`d661dbb2bf693b88c8969fcda9a0f5b98e3a2cca` and upstream
`5fe2f7e4567d9e530ded0dabc9d706db27d77b84`.

| Gate                                               | Observed outcome                                                                                                                                                                                  |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Generated CI plan and policy/helper tests          | Passed.                                                                                                                                                                                           |
| Pinned Rust tests                                  | 419 passed, zero failed and zero ignored, across 37 result groups; 18 groups contained tests. This includes 202 storage unit tests and the new count, backfill, collision and writer-fence cases. |
| Source inventory and upstream README guards        | Passed.                                                                                                                                                                                           |
| Mapped component acceptance                        | Failed during `tools/omdb-production-tsconfig.json`; no component script executed in this attempt.                                                                                                |
| Production adapters                                | Skipped because the native job failed.                                                                                                                                                            |
| Application, including the hosted normal-hook gate | Skipped because the native job failed.                                                                                                                                                            |
| Paired stock/native performance and reliability    | Skipped because the native job failed.                                                                                                                                                            |
| Final qualification                                | Rejected the failed required native job.                                                                                                                                                          |

Native job `132500944347` reported one TypeScript error:

```text
apps/server/src/orchestration-v2/Orchestrator.ts(14802,13):
TS2304: Cannot find name 'withoutGuestAttribution'.
```

The failed component artifact is `14121296091`, named
`component-acceptance-proof-1`. Its ZIP has SHA256
`83e41acdca5c5acfe90415ab692cf06a09e73e6a8abfdc728c298e6f77bd77a3`.
It records successful helper builds and the migration-head typecheck, followed
by the failed production typecheck; its component-check array is empty. The
retained decoded native job log is 238,795 bytes with SHA256
`bb544679083a66bf6de0453fb24e04a23dbd89eb6b6f58b0c246916b764abfcd`.
These Rust results resolve the earlier local test-compilation uncertainty for
this exact candidate. They do not establish application or speed acceptance.

## Repair and execution boundary

The fork already routes three attribution-removal call sites through
`guestShareDecider.withoutGuestAttribution`. The upstream sync introduced a
fourth call while combining thread creation and its initial message into one
transaction. That new call still used the former bare helper name, whose
import had been replaced by the native adapter.

The repair awaits the same decider and applies `mapDispatchError(command)`
before passing its result to `dispatchMessage`. On the native selection,
attribution removal therefore crosses the existing Rust guest-share boundary.
The adapter retains its selected upstream implementation on supported fallback
targets, validates native responses and preserves generic command fields.
There is no new implementation of the attribution algorithm. Event creation,
effects, receipt ownership and the enclosing transaction remain unchanged.

## Focused local verification

The repair was checked in an isolated checkout based on the failed commit.
The profiling checkout and its dependency links stayed unchanged. The checks
used Node 26.12.1 and the already frozen application dependencies, without
installing or resolving a new lockfile.

| Check                                                                               | Result                                                      |
| ----------------------------------------------------------------------------------- | ----------------------------------------------------------- |
| Full production entry graph through `tools/omdb-production-tsconfig.json`           | Passed with exit status 0 in 22.114 seconds.                |
| Existing launch test: first-message acceptance and durable title generation         | Passed.                                                     |
| Existing launch test: rejected initial message leaves no title change or new thread | Passed.                                                     |
| Existing launch test: durable attachment bytes survive a lost launch result         | Passed.                                                     |
| Formatter and linter on the changed production file                                 | Passed; formatted source bytes still match the tested file. |

The three selected launch tests completed together in 14.412 seconds. Their
existing SQLite service harness was retained, while the guest decider used an
explicit real native executable. An owned forwarding wrapper recorded 13
invocations of that executable. The helper SHA256 is
`9313023a4e5dd7cf53e41cc8460dc5ae4c2027d22a792e1f01fb3b8197b99b39`;
this focused invocation does not independently attest its compiler origin.
The other 58 tests in that file were outside the focused name selection and
are not reported as passes. These execution times are test diagnostics, not
application performance measurements.

The tested `Orchestrator.ts` has SHA256
`ce31a728810c02e9f1cd9f17b82ff9089a8398d72942412add3000b887e3a983`.
The local receipt has SHA256
`b4e3965f0b22099587a5376ebd7fbd5938b609dda05cdc1fa881a16a336e7002`.

## Retained evidence and remaining gate

The [evidence archive](evidence/2026-11-14-initial-message-integration.raw.tar.gz)
contains the exact failed component ZIP, decoded native log, job outcomes,
repair diff, local logs, native-call witness and an index of file hashes. It
contains 14 regular entries and has SHA256
`12aece47644c186ae7a702c8894198d090f0b65de63eccdf0177a6e408bba8d5`.
The failed run was not rerun or replaced.

The repaired source may be published only as another unqualified CI candidate.
The actual normal commit hook remains mandatory in the clean disposable
hosted checkout; its known local process-termination diagnostics are not
relabeled as a pass. Full exact-source native, adapter, application and strict
stock-comparison gates must succeed before promotion. Existing main remains
`3577878a5e008c8af87a8fbdae13d26dea1c2f50`.
