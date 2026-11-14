# Hosted functional evidence for f751c848

The published merge
[`192007cc03f418763b1f89eac26211964fc3c1c1`](https://github.com/glopyglerky/t3code-ls/commit/192007cc03f418763b1f89eac26211964fc3c1c1),
tree `0a866653fbbb1e1ae4df1ace6543151c756bd0f8`, completed its existing hosted
functional checks against upstream
`f751c8488f7ca1e659e44ef12b05861c1bcf9bfb`. The strict performance job failed all
eleven rows, so the qualification job failed and this run did not qualify the
candidate for main.

Passing the existing checks did not establish complete SQL parity. A later
[18-case sidebar differential](sidebar-predicate-parity-f751.md) exposed two
uncovered native predicates: global authored-message suppression and remote-child
presence. That follow-up preserves its red result and separately records the
corrected helper's local acceptance. The hosted results below belong to the
earlier, uncorrected `192007cc` tree.

## Exact run and gate outcomes

[Run 43617033806, attempt 1](https://github.com/glopyglerky/t3code-ls/actions/runs/43617033806)
ran from 16:42:07 to 16:58:12 UTC on 2026-11-14. Its immutable source and ordered
parents are recorded in the commit. Fresh GitHub run/job metadata and the raw
artifacts are retained in the archive linked below.

| Gate                                     | Outcome and observed scope                                                                                                                 | Primary evidence                                                                                                                                 |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Affected Rust packages and source guards | 419 tests passed, zero failed and zero ignored, across 37 result groups, 18 nonempty                                                       | [Native job 132514446861](https://github.com/glopyglerky/t3code-ls/actions/runs/43617033806/job/132514446861), complete log retained             |
| Mapped component acceptance              | 29 typecheck invocations and 49 selected scripts passed, including production/startup typechecks and the then-current shell fixture        | Component artifact 14123100663, `acceptance.json` and individual proofs                                                                          |
| Production adapters                      | The selected production-adapter parity and failure-path job passed                                                                         | [Adapter job 132517662500](https://github.com/glopyglerky/t3code-ls/actions/runs/43617033806/job/132517662500), job/step metadata retained       |
| Normal commit hook                       | The committed pre-commit hook passed in 27.207 seconds, with formatting, lint and configured Knip tasks; exact checkout restoration passed | [Application job 132517662579](https://github.com/glopyglerky/t3code-ls/actions/runs/43617033806/job/132517662579), hook proof and stdout/stderr |
| Application builds                       | Native-executable selection tests, original web-client build and server bundle with native adapters passed                                 | `hybrid-proof/application.json`                                                                                                                  |
| Controlled Linux browser/recovery flow   | Seven recorded steps completed; the same completed turn survived client reload and server restart                                          | Browser proof, screenshots, accessibility snapshots and server logs                                                                              |
| Stock/native performance                 | All eleven latency rows failed the unchanged contract                                                                                      | [Complete f751 measurement report](../performance/measurements/2026-11-14-hosted-f751.md) and unchanged raw performance ZIP                      |
| Qualification                            | Failed; no verified promotion receipt from this run                                                                                        | [Qualification job 132518586926](https://github.com/glopyglerky/t3code-ls/actions/runs/43617033806/job/132518586926)                             |

The component aggregate's exact source commit and every recorded typecheck/script
status and exit code were checked. Its old shell script SHA-256 was
`0a1ef3d5562fb87851f5ec9835320530f70eef365d13c63cb31bebb39be07d61`.
It did not contain the later six wake and twelve remote-presence cases.

The retained native log independently yields the 419/0/0 result above. The
component receipt records the packaged Lustr coordinator and produced BlueFe
route. The application manifest binds the same source/tree to Linux x64 release
executables, run 43617033806 and attempt 1. Its database helper SHA-256 is
`e7fc6069f5ef3b4344b6b2ebb5a5d77309204b7f660a0811f6fe911a826a59a0`.
This is the full Rust compatibility route; it does not establish compilation of
the full application by the separate native Lust compiler.

## Actual hook execution

The hosted gate ran `git hook run pre-commit` exactly once in its disposable
checkout. It temporarily set the first parent to
`ee178c237d5fad5842f950871003dc898ab26990` while staging the exact candidate tree,
so the normal hook saw the real candidate diff. Its proof records the hook and
task-configuration identities, configured Knip commands, complete output,
zero exit status and no timeout.

After the hook, the observed index still matched
`0a866653fbbb1e1ae4df1ace6543151c756bd0f8`, with no unstaged changes or newly created
source files. Restoration returned the detached, clean checkout to `192007cc`.
This supplies the hosted hook evidence that was still missing in the
[earlier local integration record](default-f751c848.md). The local killed
typecheck and earlier environment-error attempt remain recorded unchanged.

## Browser scope and retained limits

The controlled replay used the original Linux web client. It completed initial
setup, created a local project, approved the fixture provider request, received a
completed turn, reloaded the client, restarted the server with the same state,
and inspected canonical omdb rows and event heads read-only. It did not require a
real provider account or inspect personal state.

The initial, client-reload and server-restart snapshots retain the same project,
thread, run, attempt, provider turn and assistant message identities. Their
completed states and completion timestamps agree. Event sequences legitimately
advance from 79 to 80 to 82; they are not claimed identical. The closed-state
inspection between reload and restart records sequence 81 and unchanged closed
database bytes during that read-only inspection. No application SQLite files were
reported in this controlled native flow.

The successful browser proof also retains two connection-refused messages and two
HTTP 502 messages. Its terminal `error` is null and the seven required steps
completed. The proof does not timestamp those browser messages individually, so
this record does not assign them a more specific cause or describe an error-free
session.

This evidence covers the selected Linux replay and recovery path. It does not
establish every provider, native desktop/mobile distribution, macOS/Windows
execution, Iroh cutover, power-loss durability, or all historical SQL inputs.
The two subsequent sidebar findings are concrete examples of the remaining
coverage boundary, and require a new exact-source candidate after correction.

## Retained evidence

The [functional evidence archive](evidence/2026-11-14-hosted-f751-functional.raw.tar.gz)
has seven entries: the unchanged application and component ZIPs, complete native
job log, original and extended retrieval summaries, fresh run/job metadata, and a
byte/hash manifest. Every archive entry was read back and compared with its
original. Archive SHA-256:
`7d8a59c01790e379a866d1b5c482ce1c9cdb0d1867a32dfee93342b85b2bd5b7`.

| Evidence                              | SHA-256                                                            |
| ------------------------------------- | ------------------------------------------------------------------ |
| Application ZIP, artifact 14123430293 | `0ea90d61bf842d3597f9ac2beb105adc0a5d1f293ed9a9f253d8f5bea333fd4e` |
| Component ZIP, artifact 14123100663   | `0e96e5f956d63da278673ade2734178f4f4dd1f13fd613a7ea871b742e65ea65` |
| Component aggregate receipt           | `5ed6df152dbc4078f65eb7e84aff32f19f8fc1492df0fc52a7fd9b7f476ab7fb` |
| Native job log                        | `c458c63ab3d553236faf08685ca652a1c0180239bf0f55cd5ddbc0085aafab31` |
| Normal hook proof                     | `50a112454ea8a0c364918686680389f85f0776cbebac0d766ecb1d6cd3d1a64d` |
| Application-build proof               | `b30fac67080420ce6e445f12db257107a357db479112c267a7f17daab6690d1d` |
| Browser proof                         | `58be77469611b0f985ccc3d3513c2b05702c5efb319e9bca6844b4f684397ee6` |
| Extended retrieval summary            | `5a13e471b16c44047d05f3038fd925f0b1783eb2986a62463aae4b191d812b41` |
| Fresh run/job metadata                | `c089ac077c164550a8f87c58e2d8ac6dec1fc6422c4aa6f5ee8ca8ff56a1f16c` |

The separate measurement report retains every failed speed row, confidence
interval and tail. No result here relaxes a threshold, retires additional source,
or converts this failed qualification into permission to advance main.
