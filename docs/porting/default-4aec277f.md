# Upstream integration 4aec277f

The trusted upstream source advances from
`ed7e5010baafd73d0164ee1d5adbd21e26f8f9cb` to
`4aec277f79b2d4dabd88f13cfab264d9f80cbcef`. The merge preserves the fork's
history and native execution paths. `upstream.lock.json` records the immutable
source inventories, and `port-map.json` records the updated source identities.

## Changes and integration boundaries

Five upstream commits modify 17 paths, with no migrations, dependency changes or
workflow changes. Mobile keeps final answers visible when work continues in the
same run. The web update flow reconnects when a restarting server is ready.
Copilot ACP distinguishes completed foreground work from background shell work,
while stale idle notifications cannot finish a later turn. These implementations
and their upstream tests remain at their original paths.

Peer resolution rereads a link after a missing credential. A concurrent unlink
therefore returns the ordinary not-linked error, while a remaining link still
reports its missing credential. The shared service continues to use selected
`PeerLinkRows`; the extra read uses the native repository on the native route.
The fork's peer acceptance fixture compares this race and the retained-link case
against the immutable upstream service.

The MCP queue API requests queued runs and their messages instead of loading the
whole thread transcript. Handoff analytics requests the specific run and message.
The new `getMessagesById` service method preserves transcript hydration and error
translation through ThreadManagementService and Orchestrator. Its native lookup
already reads requested message keys directly and enforces thread ownership.
The shared orchestration service keeps its native policy and persistence wiring.

Native run selection now uses existing indexes for the new MCP read patterns.
An explicit run-ID filter reads those compact keys directly. A queue-only status
filter reads the existing global queued-run index and checks thread ownership,
avoiding completed run history. Other status filters retain their existing path.
`includeLatestRun` keeps the full thread candidate set before applying the original
filters and ordering. No schema or index format changes are needed.

The native regression places a missing-metadata tripwire in a history containing
1,024 completed runs. Selected queued and explicit-ID reads must avoid that
unrelated history, while a full-history read must detect the missing metadata.
Differential cases compare empty, duplicate, missing and cross-thread IDs, combined
filters, latest-run inclusion and requested message lookup against immutable
upstream. Queued membership also follows status changes through nested rollback
and commit in the existing native transaction.

## Qualification

The previous candidate, `c2b0096246dbf153e74e984b223ef6d8d1a80e58`, did not
complete qualification: its native component job stopped in the production
TypeScript contract check before adapter and application jobs could run.
That run is not a qualified ancestor or a reusable full-application receipt.

The standalone acceptance runtime now includes the production `jsonc-parser`
dependency and applies the existing workspace patches for `@cursor/sdk`,
`@ff-labs/fff-node` and `node-pty`. This closes missing-module and missing-symbol
errors found in a fresh installation of the earlier candidate. Package versions
match the integrated workspace, and the typecheck diagnostics remain enabled.

The updated source therefore requires fresh fork qualification from the last
verified ancestor, including native storage, production adapters and the normal
application flow. The trusted upstream source does not qualify changed native
behavior. Source integration, local differential checks and full application
acceptance remain distinct. No additional platform implementation is retired by
this update, and the fork's qualification workflows remain authoritative.
