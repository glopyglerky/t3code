# Controlled browser failure context

The controlled Linux browser proof previously retained console errors as strings without their time, step or URL. The completed search experiment recorded two connection-refused errors and two HTTP 502 errors while its seven application-flow assertions passed. Those strings do not establish which request failed or whether the intentional server restart caused it. See the exact [search workflow run](https://github.com/glopyglerky/t3code-ls/actions/runs/43618640861).

The proof now adds bounded diagnostic context. Console and page errors still append the same redacted strings to `browserErrors`; the new records refer to those array indices. Request failures and HTTP responses with status at least 400 record their time, current step, URL source and limited request metadata. The recorder does not read headers or bodies, duplicate console text, or change any assertion, error policy, performance threshold or required job.

URLs retain only parsed HTTP(S) or WS(S) origin and pathname. Userinfo, query and fragment are omitted. Missing, invalid, unsupported and overlong inputs have explicit status values; raw inputs are not retained. Controlled fixture pathnames are visible, so this is not a general promise to remove secrets embedded in arbitrary paths. Console locations may identify the emitting script rather than the failed resource; each record identifies its URL source. Several event kinds can describe one failure, so event counts are not unique-failure counts.

At most 128 events and 32 phase changes are retained. Step, URL input, origin and pathname lengths are bounded, and dropped/truncated counters remain visible. A phase overflow still updates the bounded current step for later events. The owned server restart records stopping, stopped, starting and ready phases separately. UTC timestamps and monotonic elapsed milliseconds connect these phases to the observations.

The existing proof-save and cleanup lifecycle is preserved. **Terminal cleanup after the final save is outside the persisted diagnostic window.** No new final save is added, and no error is silently treated as harmless. Future exact-source CI evidence can identify an error's origin and phase; this patch does not fix a cause or establish error-free operation.

Five pure tests cover URL privacy, invalid/unsupported inputs, record and length limits, unknown-field omission, and restart/overflow phase context. They are included in the existing client-helper test command and client-persistence typecheck. The helper, tests and this note are tracked by the existing redb boundary. At source review, only formatting, parser and whitespace checks have run; application behavior remains pending the required exact-source CI gates.

## Local result — 2026-11-14, 18:08 UTC

After that source-review checkpoint, all five pure tests passed with pinned
Node v26.12.1 on one CPU. The supervised command exited 0 in 0.385 seconds;
there were no failures, skipped tests or stderr output. Source revision02
remained hash-identical before and after. The tests cover URL credential/query
removal, invalid and over-limit input handling, event/length bounds, unknown-field
omission, restart phases and phase-overflow context.

The [indexed receipt and log archive](evidence/2026-11-14-browser-diagnostics-pure.raw.tar.gz)
preserves the exact source manifest, test receipt, stdout and empty stderr. It is
2,267 bytes with five safe regular entries, SHA-256
`ebfec4cb3f977c12cac6dcfa028dec4eb20361681ed91958ce424eb8adb0d6ce`.
The receipt SHA-256 is
`be7258494c8de96003b71ae84e14bd28a7eed1d3e210cc31d070732f21670a84`.

This local result exercised only the pure recorder. It did not build or launch
the application, change dependencies, or establish hosted qualification. The
exact-source browser flow, typecheck and normal hook remain required after this
diagnostic coverage is combined with the independent digest experiment.
