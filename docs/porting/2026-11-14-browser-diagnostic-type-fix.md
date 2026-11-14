# Browser diagnostic optional input correction — 2026-11-14

The browser diagnostic successor changes `EventInput.failureCode` from an optional string to an optional `string | undefined`. Playwright's `request.failure()?.errorText` can produce explicit `undefined`, and this repository enables `exactOptionalPropertyTypes`. The original declaration did not describe that call correctly. The recorder already filters undefined values to null; the correction changes only the TypeScript input contract, with no emitted runtime behavior change.

The original diagnostic package passed five pure Node 26.12.1 privacy/integrity tests. Those tests erase TypeScript types and do not establish typecheck success. Source review found this mismatch after publication of `a0eee4f2a0e47bf080b0d1514dfb3cbcb51fefce`; its [workflow run](https://github.com/glopyglerky/t3code-ls/actions/runs/43625465013) and source remain intact. This note does not claim that an observed CI failure was caused by the mismatch.

The reviewed correction patch has SHA256 `253736f66958bdb8670e6c0b235d546d46b54910b9941f156b05c7693c784bd1`; the corrected helper has SHA256 `0d314621572ae44c15f0799ea62b90794ac3d68653b10d7a4d71057917efadd6`. The original five-test receipt has SHA256 `be7258494c8de96003b71ae84e14bd28a7eed1d3e210cc31d070732f21670a84` and binds the earlier source. No successor build, application run or performance measurement has been executed at this source-only checkpoint.

The existing required typecheck, pure helper tests, normal hook, controlled application/recovery proof and strict performance gate remain required for any published successor. The original error array, diagnostic limits, URL filtering, assertions, and save-before-cleanup lifecycle are unchanged. The digest experiment inherited from this branch is still unqualified and is kept separate from the upstream synchronization branch.

## Subsequent observed result — 2026-11-14

The original a0 run subsequently stopped in its normal hook with eight lint findings, before the client typecheck or browser flow. The [inherited-hook coverage record](2026-11-14-inherited-hook-coverage.md) preserves that actual failure and the separate lint/policy correction. This does not retroactively turn the source-review type finding into an observed typecheck result.

The [completed b240 successor](../performance/measurements/2026-11-14-browser-type.md) passed its scoped Linux application flow and all eleven performance rows regressed. Its hook selected only this helper and note, so it did not establish lint cleanliness for the inherited test and acceptance runner. The unchanged helper binary and different host also prevent any speed claim from this type-only correction.
