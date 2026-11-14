# Inherited hook coverage and browser lint correction — 2026-11-14

The digest candidate
[`a0eee4f2a0e47bf080b0d1514dfb3cbcb51fefce`](https://github.com/glopyglerky/t3code-ls/commit/a0eee4f2a0e47bf080b0d1514dfb3cbcb51fefce)
failed the actual normal pre-commit hook in
[run 43625465013](https://github.com/glopyglerky/t3code-ls/actions/runs/43625465013),
application job `132544147654`. The hook reported eight lint errors in the new
browser diagnostic test and the touched application acceptance runner. Formatting
passed. Lint failed; the Knip task was started but no completed Knip success is
claimed. Client typechecking, application bundle construction and the browser
flow did not run in that job.

The exact application artifact is retained in the evidence archive linked below.
Its ZIP SHA256 is
`96f828be019c3afa5f52e3fd1bd9547cd8106d5c28413fcc1f4986c410f64442`.
The hook's stderr has SHA256
`b9f0e2553a991730b04da3686f4d7c4787c2b6b2fef2b9cd90b1766e2bb545cb`.
It records exit 1 after 9.672 seconds, without timeout. Its final restoration
record confirms the exact candidate HEAD/tree, detached state and clean source.

## Concrete lint correction

Five findings require the repository's standard namespace imports for Node's
assert, test, child-process and utility modules. The correction renames those
imports and references without changing assertions or process ownership. Three
findings reject direct platform/architecture globals in the touched runner. The
runner now reads the existing `HostProcess.Platform` and
`HostProcess.Architecture` references through `Effect.runSync`, then uses those
same values for its Linux guard and proof metadata. There is no new runtime
dependency, changed acceptance workload, weakened assertion or diagnostic
privacy exception.

The separate
[optional-property correction](2026-11-14-browser-diagnostic-type-fix.md)
was identified by source review. It remains valid, but the observed a0 failure
occurred earlier at lint, so it is not attributed to that TypeScript mismatch.
The five earlier pure diagnostic tests did not establish lint or typecheck
success. The lint correction starts from the published type-only successor
`b240bdc11b11237dba2c0134401eac5e0e4a096f`; both earlier runs remain intact.

## Why first-parent coverage was insufficient

The hosted helper previously staged only the candidate's first-parent diff. A
follow-up such as b240 changes only the optional property and its note. Its hook
could therefore omit the inherited test and runner that failed at a0, even
though a0 never completed qualification. The full application remained blocked
by performance, but that independent failure does not excuse the hook's coverage
gap.

The completed b240 run made the coverage limit concrete: its normal hook
passed fmt, lint and Knip, but its recorded selected paths were only the
diagnostic helper and type-fix note. The inherited failing test and acceptance
runner were absent from that list. Its scoped Linux application flow passed,
while all eleven speed rows still failed. The
[complete b240 report](../performance/measurements/2026-11-14-browser-type.md)
retains the exact hook scope, browser diagnostics, samples and failed
qualification; a narrow green hook does not erase a0's findings.

The corrected helper uses the exact-run qualification plan. It checks the plan's
repository, run and producing attempt; candidate commit and tree; current policy
digest; and immutable trusted upstream commit/tree. Reusing a plan produced in
an earlier attempt of the same run is allowed, while a future or different-run
plan is rejected. These checks do not treat a locally supplied claim of a
qualified ancestor as proof.

When the plan names a qualified ancestor, the existing bounded qualification
lookup revalidates that exact successful same-repository workflow run and its
same-policy receipt. It does not silently replace the planned baseline with a
newer success. Missing, failed, expired or inconsistent evidence rejects that
baseline. When no qualified ancestor exists, the baseline is the candidate's
immutable `pingdotgg/t3code` source pin, covering every surviving fork difference
while excluding pristine upstream blobs. The chosen baseline must be an
ancestor, and a missing object may be fetched only by a validated full commit
SHA from its expected repository.

The normal committed hook still runs once with its existing task selection,
rules, ten-minute timeout and owned-process cleanup. The helper stages the exact
candidate tree against the selected baseline. It records the first parent
separately, the hook baseline/tree and selection reason, plan hash and producing
attempt, selected paths and their NUL-delimited SHA256 witness. The plan is
checked again after execution. The GitHub history token is removed before the
hook's subprocess starts. No source change or incomplete process is accepted as
a successful hook, and the same unconditional restoration path remains required
before later application work.

The application checkout now includes history with deferred blobs so ancestry
can be proved without relying on a shallow first-parent boundary. Its required
plan artifact and exact-base provenance verification precede the hook. The
helper, tests, planner, history logic and workflow remain covered by the CI policy
digest; this correction invalidates older policy receipts rather than treating
their narrower hook scope as current evidence.

## Initial source-review checkpoint

This is a source-preparation checkpoint. New isolated Git fixtures exercise an
inherited bad source file behind an unrelated follow-up, exclusion of an
unchanged upstream file, an explicitly revalidated ancestor, stale/forged and
nonancestor plans, and plan mutation during the hook. Existing process timeout,
cancellation, formatter mutation, cleanup failure and exact restoration tests
remain. Exact-run history fixtures verify that an unrelated or failed run is
not reused or fetched. These additions have not yet executed at this checkpoint.

## Subsequent local policy checks — 2026-11-14

The first bounded run passed 39 tests, then stopped on an existing workflow
fixture that expected two plan-artifact consumers. The new required application
download makes three consumers and seven total downloads. The correction
updates only those two expected counts; it does not alter workflow behavior.
That failed attempt, its unchanged source hashes and full logs remain retained.

The fresh corrected run passed all 113 tests across `test_ci_commit_hooks`,
`test_ci_history`, `test_ci_workflow`, `test_ci_plan`, `test_ci_receipt` and
`test_ci_promotion` in 14.180 seconds. It used one CPU, a 180-second bound and
a 16 MiB output bound, and recorded 41,156 KiB peak child RSS. All nineteen
reviewed file hashes remained unchanged during the run. Its receipt SHA-256 is
`faadac2a47261d4ea37146d2b7db9134873e155c1219d9246f4e8dcc1e5ca92f`;
the complete successful stderr is
`005498dd1444287a92f3b0b4ce2fca5cf91f4e8dee7c1fbf890f553330a63eed`.

The [indexed local policy evidence](evidence/2026-11-14-inherited-hook-policy-tests.raw.tar.gz)
preserves both source manifests, patches, owned supervisors, receipts and logs.
The fixtures execute real temporary Git hook boundaries with controlled task
stubs; they do not execute the repository's full Vite/Knip hook or establish
application, browser or performance acceptance. Those exact-source hosted
requirements remain in force.

The subsequent static check first stopped on four formatting findings: the two
lint-corrected TypeScript files and the two new measurement tables. That failed
check left source unchanged. The reviewed correction changes only formatter
wrapping, trailing commas and table alignment in those four files.

A fresh static block then passed formatter CHECK on all ten selected paths,
lint on the diagnostic helper, its test and the acceptance runner, and all five
pure diagnostic tests. No test was skipped or cancelled. The three stages took
0.404, 0.906 and 0.301 seconds; source, Node, tool and dependency-alias identities
remained unchanged. Receipt SHA-256:
`9c0a910fd74de936b6500c967f9b2541947a87b6647d8b3a2b5ebe713f1d9128`.
The [indexed static evidence](evidence/2026-11-14-browser-lint-static.raw.tar.gz)
retains the eleven private alias bindings, both attempts, formatting correction,
source manifests and complete logs. No dependency install or shared-link/source
alias mutation occurred. This verifies the eight concrete lint repairs and the
existing bounded diagnostic logic; it is not a client typecheck, normal full
hook or browser execution. The complete exact-source CI gates remain required.

The original a0 native/source/component job passed 421 Rust tests across 37
test-result groups, including 203 storage unit tests, with zero failed or ignored
tests. Its production adapter job passed. Its fixed stock/native comparison
completed and failed all eleven latency rows; the complete
[digest outcome report](../performance/measurements/2026-11-14-digest-hex.md)
retains the values, tails and limits. Those results do not qualify the browser
lint or hook-policy correction and do not establish a causal digest speedup.

The [original hook evidence and job logs](evidence/2026-11-14-digest-hook-failure.raw.tar.gz)
preserve the failed attempt. Main remains unchanged. The corrected source still
requires its normal hook, exact-source functional gates and the unchanged strict
performance policy before any promotion.
