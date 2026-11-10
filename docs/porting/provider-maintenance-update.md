# Provider update selection and verification

The production maintenance runner calls the existing Rust maintenance helper to
select targeted and latest update actions and verify their results on qualified Linux x64 hosts. Other
hosts use the retained upstream functions, selected before execution. A native
failure stops the update; it never retries through the retained implementation.

The host still owns fresh installation checks, compatibility decisions, command
locks, the updater executable and environment, process execution, version probes
and cache invalidation. For action selection the helper receives only the package name, existing
display command, arguments, lock key and requested version. It returns a display
command and argument list, or no action. It cannot replace the executable,
environment or lock key.

Targeted updates preserve the upstream rules: an exact three-component numeric
version, a supported installer lock and a matching package argument are required.
The first matching argument and last matching display-command occurrence are
replaced. Latest pnpm updates use the advertised version when a targeted action
can be formed; otherwise the original action is retained. Other installers keep
their original latest action.

Before an unpinned update, Rust evaluates the refreshed provider observations and
can return the existing "Already up to date." result. It requires successful
advisory enrichment, a prior provider, a nonempty refresh with changed timestamps,
and installed providers with current advisories and no error status. After a
successful installer command, Rust checks installation, authentication/error
state, targeted version equality and outdated advisories. The host sends only
the fields used by these decisions.

An omitted installed version retains the upstream fallback to the reported
version; explicit null stays unknown. Targeted verification removes exactly one
lowercase `v`. An unauthenticated provider error can still verify when its reported
version exists. Unpinned updates can succeed without version data when no other
failure condition applies. Inability to verify takes precedence over an outdated
advisory. The original status and message pairs are preserved.

A helper failure records the existing failed update state. It never reruns a
completed installer or switches backends after execution. The bounded protocol
requires the requested phase and an exact status/message pair, and rejects extra
output, observation requests and nonzero child exits.

## Evidence and maintenance

`tools/check-maintenance-adapter.mjs` extracts the two selection functions from
the immutable integrated upstream source as its behavior oracle. It also extracts
the original runner's verification expressions and helpers. Its existing
gate covers version, installer and argument cases, pre/post-update decisions, the actual production runner,
host metadata preservation, process failures and cancellation. All updates use
controlled fixtures. Rust unit tests cover the selection rules and bounded JSONL
protocol; the existing adapter typecheck uses `effect-tsgo tsrs --noEmit`.

`port-map.json` includes the production runner in the upstream sources and adapter paths,
so its changes participate in port impact and acceptance hashing. The port stays
`integrated`; this slice does not retire the original functions or establish
acceptance on other platforms. The verification module lives beside the owning
application domain. The [9e12eca1 integration](default-9e12eca1.md) records the
combined upstream update and qualification scope.

No CI job, compiler check, dependency or compiler pin is added for this slice.
