# ee178 Linux functional acceptance

[Run 43614308563, attempt 1](https://github.com/glopyglerky/t3code-ls/actions/runs/43614308563)
passed the exact repaired candidate's Rust, component, adapter, normal commit
hook and original Linux client checks. Its eleven stock/native latency rows
regressed, so final qualification failed and main was not promoted. The full
[performance report](../performance/measurements/2026-11-14-hosted-5fe.md) retains
every result and the unchanged raw performance artifact.

This report records functional evidence for commit
`ee178c237d5fad5842f950871003dc898ab26990`, tree
`68ff57f62a644665e401c6d9a046b4c5129eca7a`, whose sole parent is
`a2fb16d84f089e2a7ca681e431e6acff083695bf`. Its upstream cutoff is
`5fe2f7e4567d9e530ded0dabc9d706db27d77b84`. The
[initial-message report](2026-11-14-initial-message-integration.md) preserves the
preceding failure and the native guest-decider integration repair.

## Observed gates

| Gate                                    | Exact result                                                                                                                           |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| Plan and source accounting              | Passed.                                                                                                                                |
| Native Rust tests, job `132506130180`   | 419 passed, zero failed and zero ignored, across 37 result groups; 18 groups contained tests. This includes 202 storage library tests. |
| Mapped component acceptance             | All 29 selected typechecks and 49 component scripts passed. The retained receipt binds their source and executable hashes.             |
| Production adapters, job `132509815163` | Passed.                                                                                                                                |
| Application, job `132509815105`         | Passed the actual normal commit hook, native selection contract, server/client bundles and the controlled original Linux browser flow. |
| Paired stock/native workload            | Correctness and runtime provenance passed; all eleven latency rows regressed.                                                          |
| Final qualification                     | Rejected the failed required performance job.                                                                                          |

The complete decoded native job log has SHA256
`0178b03cdc9ce9d8eaf27a968caa2da1c8b67fa0d33824e2252c29355e1b33ce`.
The archive manifest is authoritative for its bytes and hash; the exact log is
retained rather than replacing the failed previous run's evidence.

## Actual normal commit hook

The disposable hosted checkout ran `git hook run pre-commit` once against the
exact candidate-versus-first-parent staged change. The helper recorded the
committed hook and task configuration, rejected skip controls and required a
clean source tree. The hook completed in 39.598 seconds. Its retained stderr
explicitly reports success for:

```text
vp fmt --no-error-on-unmatched-pattern
vp lint --no-error-on-unmatched-pattern
vp run knip:check
```

The committed `knip:check` task retains both configured scans. The hook proof
reports exact restoration of the candidate HEAD, index tree and worktree, with
no unstaged or new source files. Its SHA256 is
`9d392e3c3438b8b9c0cee039fbf300221140352effa28593eea23ce06ab3f6b3`.
The stderr SHA256 is
`34a5da7cb8404c8ada04c36f5f34a238d44e572ba3efad4ee479792094f764c4`.

This hosted result resolves the normal-hook gate for ee178. Earlier local
process terminations remain recorded failures; they are not relabeled as
successful local executions. Later source requires its own planned checks.

## Original client and durable state

The application evidence identifies clean ee178 source, Linux x64, the pinned
stock revision and the selected release database helper. The native-selection
contract and both application bundles passed. The browser exercised the normal
web client with synthetic state and a free controlled ACP provider fixture:

1. Completed normal setup without account sign-in or state import.
2. Created a local project through the user interface.
3. Presented and accepted the controlled approval.
4. Observed the completed turn.
5. Reloaded the client and observed the same completed thread.
6. Restarted the server against the same state directory, opened a fresh client
   and observed the same durable thread and session.
7. Inspected canonical omdb rows and event heads through a read-only reopen,
   with no application SQLite files.

The initial completion, client reload and server restart snapshots preserve the
same recorded turn identity. The browser proof has SHA256
`6dc98d8ba45b2d26ae86e91d82471f709c39eed50143ba70a4800dbd149d7021`.
The client summary and build-level application proof also report success.

This is evidence for the recorded Linux original-client path and controlled
provider scenario. It does not retire other clients, establish Windows or mobile
acceptance, cover real paid provider sessions, or establish speed parity.

## Durable evidence

The [functional archive](evidence/2026-11-14-ee178-functional-acceptance.raw.tar.gz)
contains the two unchanged downloaded artifacts, the complete native job log and
an indexed manifest. Its four regular entries have SHA256
`0f317f89540b423e10233816e24bca5b74987c7ba203f64785a9fd038f792175`.

| Original artifact              | ID            | ZIP SHA256                                                         |
| ------------------------------ | ------------- | ------------------------------------------------------------------ |
| `component-acceptance-proof-1` | `14121319971` | `67ff2c89ec9440cd6e14b2e90a56158516d54a2cbd376cfc521f3ac02b9c7d34` |
| `hybrid-application-proof-1`   | `14121997612` | `5496bb5f50778985279639db02ccd36391c8b3cc50a3997caa795ee0eca1c953` |

The component ZIP contains 39 entries, including its aggregate receipt. The
application ZIP contains 12 entries, including the actual hook logs, application
proof, browser snapshots, screenshots and startup/restart logs. The files are
controlled synthetic application evidence. No database or executable bytes are
included. Original download hashes were verified before archival, and both ZIP
entry sets were checked for unsafe paths or symlinks.

These artifacts remain bound to their original producing attempt. They do not
replace a qualification receipt or authorize main promotion while performance
fails. The node-root experiment and later upstream integration must retain their
own exact-source outcomes.
