# Provider update-action selection

The production maintenance runner calls the existing Rust maintenance helper to
select targeted and latest update actions on qualified Linux x64 hosts. Other
hosts use the retained upstream functions, selected before execution. A native
failure stops the update; it never retries through the retained implementation.

The host still owns fresh installation checks, compatibility decisions, command
locks, the updater executable and environment, process execution, verification
and cache invalidation. The helper receives only the package name, existing
display command, arguments, lock key and requested version. It returns a display
command and argument list, or no action. It cannot replace the executable,
environment or lock key.

Targeted updates preserve the upstream rules: an exact three-component numeric
version, a supported installer lock and a matching package argument are required.
The first matching argument and last matching display-command occurrence are
replaced. Latest pnpm updates use the advertised version when a targeted action
can be formed; otherwise the original action is retained. Other installers keep
their original latest action.

## Evidence and maintenance

`tools/check-maintenance-adapter.mjs` extracts the two selection functions from
the immutable integrated upstream source as its behavior oracle. Its existing
gate covers version, installer and argument cases, the actual production runner,
host metadata preservation, process failures and cancellation. All updates use
controlled fixtures. Rust unit tests cover the selection rules and bounded JSONL
protocol; the existing adapter typecheck uses `effect-tsgo tsrs --noEmit`.

`port-map.json` includes the production runner in the maintenance adapter paths,
so its changes participate in port impact and acceptance hashing. The port stays
`integrated`; this slice does not retire the original functions or establish
acceptance on other platforms. The integrated upstream revision is unchanged.

No CI job, compiler check, dependency or compiler pin is added for this slice.
