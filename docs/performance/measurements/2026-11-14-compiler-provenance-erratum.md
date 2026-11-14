# Compiler provenance correction, 2026-11-14

The earlier performance runner overstated compiler provenance. It initialized
`compiler_route` to `bluefe-rustc-via-lustr` and copied `compiler_identity` from
`toolchain.lock.json` before checking whether a build manifest was supplied.
Those values described the required compiler configuration. In a diagnostic run
without a manifest, they did not establish how the measured executable was built.
Even when a release manifest was supplied, its matching toolchain-lock hash did
not make the lock's driver identities independently observed facts.

Every retained timing attempt still reports a regression in all eleven rows.
This correction changes the interpretation of compiler metadata, not any raw
sample, executable hash, fixture, comparison, confidence interval or verdict.
Existing main remains unchanged; these attempts did not qualify a candidate.

## Historical evidence and its limits

The following local reports have `native_manifest_sha256: null` in their
original JSON. Their compiler route and identity must be read as expectations,
not verified build provenance. A hash identifies the exact executable that ran;
it does not identify the compiler that produced it. Compilation logs that only
show Cargo's package/profile progress also do not prove a coordinator invocation.

| Diagnostic attempt                                                    | Retained result                                       | Compiler evidence from this report         |
| --------------------------------------------------------------------- | ----------------------------------------------------- | ------------------------------------------ |
| [First baseline](2026-11-14-baseline.md)                              | All eleven latency rows regressed                     | Expected pin only; no bound build manifest |
| [Readiness cache](2026-11-14-readiness.md)                            | All eleven latency rows regressed                     | Expected pin only; no bound build manifest |
| [Frozen-runtime validation](2026-11-14-runtime-validation.raw.tar.gz) | Correctness/runtime validation only; no speed verdict | Expected pin only; no bound build manifest |
| [Owned dispatcher](2026-11-14-dispatcher.md)                          | All eleven latency rows regressed                     | Expected pin only; no bound build manifest |
| [Immutable table handles](2026-11-14-table-handle.md)                 | All eleven latency rows regressed                     | Expected pin only; no bound build manifest |
| [Observed-value write pruning](2026-11-14-write-pruning.md)           | All eleven latency rows regressed                     | Expected pin only; no bound build manifest |

The hosted attempts ran in qualification mode and supplied exact-source,
same-run release manifests. The runner checked their executable hashes,
route, release profile, source commit/tree, clean build state and toolchain-lock
hash before timing. That supports the narrower claim that the build manifest
reported the required route and was bound to the measured release artifact.
The original `compiler_identity` field was still copied from the expected lock;
the manifest did not explicitly report that identity.

| Hosted attempt                                      | Retained native manifest SHA-256                                   | Result                                                                           |
| --------------------------------------------------- | ------------------------------------------------------------------ | -------------------------------------------------------------------------------- |
| [Readiness](2026-11-14-hosted-readiness.md)         | `f041e295b2cae9aa200df7239604e83b603646c69a56aa55d6998c4d46502d86` | All eleven rows regressed; earlier dependency-provenance limitation also remains |
| [Dispatcher](2026-11-14-hosted-dispatcher.md)       | `05843ec6bb1b9800b9407809400733a5889aadea6aad6ad28bbe243cfa9ce0ff` | All eleven rows regressed                                                        |
| [Table handles](2026-11-14-hosted-table-handle.md)  | `24c273c63523e68c463bee72db952d475fa67837e3a9b86f6a933991d8ff28e8` | All eleven rows regressed                                                        |
| [Write pruning](2026-11-14-hosted-write-pruning.md) | `0da1f24735bfc2351cc1bc4283ad20b08f7620ccea548a49203d7cca73f695da` | All eleven rows regressed; the complete write-tail observations remain retained  |

All original raw archives, embedded JSON/Markdown, helper hashes, worker samples,
failed setup attempts and their recorded checksums remain unchanged. The readable
reports now link this correction. No historical report is silently relabeled
with the new provenance status, and the older report format cannot satisfy the
updated qualification validator.

## Corrected reporting contract

The [compiler metadata resolver](../../../tools/performance-compiler.mjs) keeps
configuration, manifest claims and qualification checks separate.

| Field or status              | Meaning                                                                                                                                                     |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `expected_compiler_route`    | The coordinator route required by policy                                                                                                                    |
| `expected_compiler_identity` | The compiler/coordinator pin in the current toolchain lock                                                                                                  |
| `compiler_route`             | A route explicitly reported by an executable-bound manifest, or `null`                                                                                      |
| `compiler_identity`          | An identity explicitly present in that manifest, or `null`; never synthesized from a matching lock hash                                                     |
| `unverified`                 | No executable-bound build manifest was supplied                                                                                                             |
| `manifest-reported`          | A diagnostic manifest matches the executable hash; its other metadata remains reported claims                                                               |
| `verified-ci-manifest`       | Qualification checked the manifest's exact source/tree, clean state, workflow run, toolchain-lock hash, executable hash, required route and release profile |

The current native manifest reports a route and a toolchain-lock hash, but no
`compiler_identity` field. Correctly qualified reports therefore keep observed
`compiler_identity: null` while retaining the expected pin separately. If a
future manifest supplies an identity, diagnostic mode labels it as a manifest
claim, and qualification also requires it to match the pinned expectation.
`verified-ci-manifest` is a statement about the manifest binding; it is not
binary-derived compiler identification or native Lust language coverage.

Build and measurement attempts may still differ within the same exact workflow
run when a failed-job retry reuses a successful dependency. Both attempts and
the workflow run must be positive decimal IDs; the build attempt cannot be newer
than the measurement attempt. A newer measurement does not rewrite the original
build attempt. The executable, source, toolchain and workflow-run bindings remain
mandatory.

The generated Markdown now prints expected and manifest-reported values with
their scope. The resolver is included in the report's source hashes. The
[focused metadata tests](../../../tools/performance-compiler.test.mjs) cover
absent manifests, wrong executable hashes, diagnostic claims, every required
qualification binding, same-run retry reuse, missing or future build attempts,
missing driver identity and an explicit identity mismatch. The CI validator additionally requires
`verified-ci-manifest` before accepting performance evidence.

The fixed workloads, eight balanced pairs, three warmups, twelve measured
cycles, whole-pair bootstrap, exact `1.0` comparison and rejection of inconclusive
evidence are unchanged. A future claim about a particular compiler route needs
the corresponding retained build evidence; an expected pin cannot supply it.
