# t3code-ls

This is one working T3 Code fork whose implementation becomes Rust incrementally.
Keep upstream functionality, source organization and public contracts. Replace
implementation behind the existing application interfaces. Keep required upstream
code, including TypeScript, JavaScript, HTML, CSS and platform code, until its
replacement runs in the real application and passes the affected platform gates.
Never delete a capability to increase the Rust percentage.

The production application lives in apps/, packages/, native/ and infra/ alongside
the root Cargo workspace and temporary pnpm workspace. Keep unported files at their
upstream paths. Place new Rust in its owning application or package. Existing crates/
are preserved migration inputs with ownership recorded in port-map.json; move them
in separate changes from behavior substitutions. Do not create a second reduced
product, a giant legacy/ tree or a vendored copy of the application.

Port states are candidate, integrated, accepted, retired and synced. Component
tests establish candidate behavior. Integrated means the real application can use
the port. Acceptance requires normal execution through Rust, failure-path tests
and evidence for each affected platform. Retire original code only within that
accepted scope, then keep its upstream mapping for future changes. Linux evidence
does not retire Windows or mobile behavior. Never infer UI, platform, feature or
whole-stack memory-safety qualification from API or component tests.

Thin adapters may translate arguments, cancellation and errors, but must not copy
the business algorithm. Select the backend before execution; never retry an
uncertain provider action or database mutation through a fallback implementation.
Preserve authorization, omitted/null distinctions, event ordering, receipts and
transaction ownership. The qualified native-selected application uses omdb as its
authoritative production database; SQLite is retained for source migration,
immutable behavior comparisons and unqualified upstream-selected targets. Never
fall back to a preserved SQLite source after native activation. Iroh byte
forwarding does not replace
Connect account, pairing or client lifecycle behavior. Keep all working clients.

Build full Rust with the verified packaged Lustr coordinator and produced BlueFe
drivers recorded in toolchain.lock.json. Cargo is bootstrap Cargo. Native Lust's
separate subset pin remains in lust-toolchain.json. Record the actual compilation
route; coordinator forwarding to rustc-derived BlueFe is not native Lust coverage.
Use pinned compiler artifacts in application CI; compiler bootstrap and release
qualification belong in the compiler repositories. Preserve caller flags and
select the matching produced target sysroot. Keep Cargo --locked and pnpm's frozen
lockfile gate. Do not substitute unmatched Clippy or compiler drivers.
Keep generated output outside canonical source and clean owned disposable output.

Use the generated CI plan to run affected Rust packages and reverse consumers,
`t3code-ls-tools check-source`, `check-upstream-readme` and affected production
adapters. Run the native client flow and pinned stock/native performance and
reliability comparisons when our production execution boundary changes.
Compiler, global dependency and CI-policy changes require full fork qualification.
Select changes from a verified qualified ancestor, never an unqualified previous
push. Reuse an identical commit only with a successful same-policy qualification
receipt. Missing evidence widens qualification. check-source enforces
migration accounting and source retention, not a language ban. Test real services
and controlled provider fixtures. Do not recreate in-checkout reference caches.

Upstream `pingdotgg/t3code`'s default branch is trusted as working. Do not rerun
its pristine test suites or separately qualify its original client execution.
Use its immutable source as the behavior oracle for Rust differential tests.
The required paired performance/reliability workloads are an explicit exception:
execute the same fixed inputs through immutable stock services and our selected
production services, including their normal durability settings. Preserve raw
samples, every failed or inconclusive attempt and exact source/compiler identity.
Document every measured speedup, slowdown, inconclusive result and deliberate
behavior or performance divergence with its reason. Never hide a slow workload
in an aggregate, weaken a threshold to obtain a pass, or claim broader speed or
reliability than the measurements establish.
Our translations, adapters, fork modifications and integration still need their
affected checks. A trusted upstream revision does not qualify our Rust replacement.

Original behavior is available from `pingdotgg/t3code` at the immutable commit
and tree in `upstream.lock.json`. The current integrated pin remains explicit;
the porting planner follows the trusted default branch at an immutable commit.
Nightly release tracking remains available for release comparisons. Record
detected, integrated and qualified revisions separately. Review
changes against port-map.json, including translated files that Git cannot match.
New upstream functionality may retain its original implementation. A changed pin
alone does not establish synchronization or application parity. Use disposable
reference checkouts outside this tree for differential tests. The unchanged
upstream README body is wrapped in `upstream-README.md`; its wrapper is fork prose.
Preserve licenses, provenance, fixtures, assets and Git history.

Keep repository identity 1352693431, fork relationship, license and separate
archived-thread/mobile-backup branches. Publish main only after green exact-source
qualification under the current policy, through an authorized fresh-head,
non-forced fast-forward update. Revalidate inherited performance evidence against
every intervening runtime, oracle and policy change. Do not claim repository
branch protection is enabled without checking the actual settings. No PR is implied. External
GitHub submissions require verified model/harness attribution under the user's
session rule. Never modify installed T3 state, services or user data for tests.

## Mandatory external GitHub attribution

Before publishing agent-authored text or code on GitHub outside verified user
namespaces `glopyglerky` and `omnidia-dev`, append the visible footer below at the
very end of the submission body (or commit message). Classify a PR by its base
repository. Verify any additional claimed ownership. This applies to PRs, issues,
comments, reviews, discussions and release text; it does not authorize publication.

Use runtime evidence for exact author models, roles and actual harness chain;
distinguish reviewers. Never infer historical authorship from the current editor,
configured defaults or commit names. If provenance cannot be verified, keep a
private draft and report the missing evidence. Exclude private paths, thread IDs,
transcripts and credentials from public attribution.

For retrospective corrections, amend only the user's existing open PR or issue
with verifiable original provenance. Recheck open state immediately before editing,
preserve its body and append the footer. Never reopen or modify closed submissions
or add speculative attribution comments. Carry this rule into worker handoffs.

```md
***

**AI attribution**

- Models: <verified exact models and roles>
- Harness: <verified actual harness chain>
```

## Runtime and storage safety

- Use GitHub repositories and the hosted workspace for implementation and qualification.
- Use controlled provider fixtures for qualification, without real provider turns or personal credentials.
- Use the session's verified model/settings; reasoning above xhigh and non-Standard speed are prohibited.
- Run authorized workers through T3's supported lifecycle APIs. Do not spawn workers for a bounded task assigned to work alone.
- Never kill processes by name/path pattern. Stop only owned processes identified when spawned.
- Never run tests against an installed application's state or remove user data.
- Consume pinned and verified Lustr/BlueFe artifacts in isolated hosted directories. Application qualification must use the produced drivers and matching sysroot. Keep native Lust subset qualification distinct.
- Keep generated output outside the source checkout and bound build concurrency to the hosted runner's capacity. Do not touch another owner's source or builds.
- Own generated output through completion/failure/cancellation. Retain source/history, a current deliverable, one useful rollback and concise hashes/logs. Remove disposable targets, dependencies and staging after checking actual processes, descriptors, mounts and Cargo locks; record available-space change. Do not clean another owner's scope.
- Do not resume unrelated work, schedules or agents to perform housekeeping. Preserve source revisions, first failures, receipts and rollback boundaries.
- Do not publish, deploy, rename repositories, rewrite history or change visibility/license unless the user has authorized that exact scope. Do not send messages to others without authorization.
