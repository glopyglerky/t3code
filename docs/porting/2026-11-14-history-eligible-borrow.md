# Borrow eligible history rows

This source-only experiment starts from node-root candidate
`02296614b7d5989475fed25e86ebb5116766645f` and keeps its immutable upstream
comparison at `5fe2f7e4567d9e530ded0dabc9d706db27d77b84`. It removes one deep
clone of each eligible compact history row. Latency is unmeasured. This change
has not been compiled, tested or published at this preparation checkpoint.

The subsequent [hosted node-root result](../performance/measurements/2026-11-14-history-node-reuse.md)
passed 425 Rust tests, mapped components, adapters and the controlled Linux
application flow, but all eleven latency rows regressed and qualification
failed. Its original artifacts and complete distributions are retained here.
Those predecessor results do not compile, test or qualify this borrowing change;
the source-only preparation boundary above still applies.

## Decision

`retained()` already owns the `items` vector for the entire eligibility and
selection calculation. `Eligible` previously cloned each qualifying
`IndexedRow`, even when that row would later be hidden in a deferred work group.
The change makes `Eligible` borrow that original row. Its run-status and
attempt strings remain owned and unchanged.

The later selection still creates owned rows through the existing `cloned()`
calls. The final latest-row handling still consumes the original `items`
vector, and `Retained` still returns owned output. The borrow is confined to
eligibility and grouping; it does not introduce a reference into a returned
response or another transaction. Rust compilation must verify that lifetime
boundary before the experiment advances.

The [retained attribution](../performance/measurements/2026-11-14-attribution-a2fb16d.md)
showed 135.347 ms mean history API time, 131.017 ms helper evaluation, 6,059 point
reads and 61 scans. Those inclusive observations do not identify the cost of
this particular clone. The fixed fixture contains 1,500 items, all eligible in
its unanchored history request. Source therefore predicts 1,500 fewer deep
`Map`/`Value` clones per history calculation. It does not establish a latency
gain or a measured reduction in whole-process memory.

This ownership change is narrower than a decoded-metadata cache: it needs no
cache budget, invalidation, snapshot lookup rule or storage format. It also
keeps all repeated point reads, scans and decodes, so the expected I/O-wrapper
call-count change is zero. Any further clone removal or metadata reuse requires
a separate decision and measurement.

## Preserved behavior

- Eligibility, parsing, corruption checks and their error order remain in the
  same statements and iteration order.
- All three global run-item consumers and their distinct thread scopes remain
  intact. The presented-run metadata pass also remains.
- Writer staged reads, conflict observations, range budgets, savepoints and
  mutation/commit behavior use the same paths. The change adds no session-mode
  condition.
- Existing sort order, duplicate-ordinal handling, missing-node fallback,
  deferred groups, inherited history and owned response assembly remain.
- The preceding node-root experiment is unchanged. This patch removes only
  the first `Eligible` row clone, not any later selection or output clone.

## Verification boundary

Preparation uses the pinned formatter to parse and compare the changed source,
plus whitespace and exact-diff checks. Those checks do not establish Rust
borrow-checker acceptance or runtime correctness. No local build or benchmark
is part of this preparation, and the published node candidate remains fixed.

The existing records/window differential gates cover long deferred turns,
forks, duplicate ordinals, selected payloads and inherited counts. The existing
node-root regressions additionally cover eligibility before corruption,
missing/foreign roots, public transaction closure, snapshot visibility and
writer/savepoint/observation limits. No concrete missing behavior case has been
identified for this ownership-only change, so there is no new clone-count test
or duplicate fixture suite.

After review, exact-source Rust and affected component checks remain required,
followed by the normal hosted hook, adapters, original client flow and unchanged
fixed stock/native performance policy. A previous candidate's passes do not
qualify this source. The experiment must record its own measured gains, losses
or inconclusive results, and main must remain unchanged until the full policy
passes.
