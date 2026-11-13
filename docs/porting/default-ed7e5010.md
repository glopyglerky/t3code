# Upstream integration ed7e5010

The upstream source advances from `7171fdd7716b05dc1b4be40c26ac78e8bb35f80c`
to `ed7e5010baafd73d0164ee1d5adbd21e26f8f9cb` from `pingdotgg/t3code`'s
trusted default branch. The merge preserves both histories and the fork's existing
Rust execution paths. `upstream.lock.json` records the complete source inventories,
and `port-map.json` records source identities and reviewed upstream removals.

The update contains 1,054 upstream commits and changes 2,263 paths. It includes
278 additions and 15 upstream removals. The first integration target was
`10fbff5ab6d31c494b5c6cfd759d78c12aa731f6`. Two subsequent upstream commits fix
cross-host pull-request suggestions and terminal selection added to an edited
queued message. Those changes touch only shared, web and mobile source and tests.

The server, web, desktop and mobile source includes the upstream changes. The fork's
two qualification workflows remain authoritative. Upstream deployment, publishing
and repository-management automations remain excluded.

## Native storage and orchestration

The native database upgrades an activated schema-80 store to schema 88 within its
existing transaction and ownership boundary. The upgrade preserves native writes
made since cutover. It backfills citation-decoded message search text, derived turn
item counts and the custom-project-title flag. A malformed catalog or migration
history remains an error. Startup never reimports the retained SQLite copy to
repair an activated native store.

The selected server and CLI share the native database owner. Their migration and
writer lifecycle follows the upstream startup changes without adding a hidden
SQLite dependency. The production type guards continue to require a complete
runtime with no unresolved `SqlClient` service.

Native projection APIs now carry the new upstream fields and queries. These include
run lookup, transcript byte limits, delegated completion, background wakes, remote
tasks, rollback state and preserved checkpoint ordinals. Record filters preserve
compaction selections and settled-subagent visibility. Fork history and attachment
cleanup retain source data still visible in a live fork. Timeline pages preserve
cursor, position and earlier-page behavior through the shared projection helpers.

Event publication checks turn-item identity and interrupt state in the same native
transaction as receipts, events and projection changes. Outbox claims, provider
commit positions and scheduled-task compare-and-set updates follow the new upstream
contracts. Follow-up owner selection now streams through the Rust policy service
from both Orchestrator callsites.

The new KeepAwake query also runs through selected persistence. Its native API
checks the runtime index for preparing, starting or running work and stops at the
first match. The existing inhibitor, settings and retry lifecycle stays in the
upstream service.

Provider hydration also reads legacy subagent thread IDs through native persistence.
The query scans bounded index pages and matches SQLite's ASCII-insensitive prefix
rule, including archived and deleted threads. The identifier allocator preserves
their stored IDs while new subagents receive short IDs. Read failures retain the
upstream warning and empty-set behavior without opening SQLite.

## Provider integration

Codex structured generation uses separate exec and MCP launch-argument filters,
including the new exec-only options. The original implementation remains available
for targets without native qualification. Text generation and provider sessions use
the upstream installation gate.

The Rust maintenance policy handles read-only Nix installations, the current Vite+
and Bun latest-version probes, and OpenCode server restart guidance after an update.
The shared filesystem boundary follows Vite+ system-first shims before policy
selection. Native verification errors do not retry a provider update through another
implementation.

The workspace lockfile and the external server-test runtime use the integrated
dependency versions. Their immutable install gates remain enabled.

## Source retention and qualification

The obsolete fork-only `SqliteProjectionRecordRows.ts` is removed. Its selected
record API now uses the current canonical upstream SQL reader for SQLite routes,
while the native route uses Rust. Keeping that separate duplicate would leave two
SQL implementations with different filter semantics. This removes no platform
capability and does not retire the canonical upstream implementation.

The unused pre-cutover copy `tools/oracles/CommandPolicy.ts` is also removed.
Its last fork revision is `17ec4bae52471ba66f24f6abadbf29e4498106e7`, blob
`c12e10eef2f1f57c9e0311c37e86224112eb1bfb`. The policy and dispatch checks now
load the integrated immutable upstream source, while the adapter check exercises
the selected production service. A complete tracked-source reference check found
no remaining consumer of the old copy. The current TypeScript policy service and
its upstream-selected behavior remain present.

Internal persistence factories keep their implementations with module-local
bindings where no external consumer exists. The explicit SQLite keep-awake
composition, standalone native-client layer and maintenance policy export retain
documented public contracts. Fork acceptance scripts and compile-only contract
checks are explicit Knip entries rather than unused-export exclusions.

The 15 original upstream source removals are accounted for by immutable source
identities, replacement paths where applicable and the fork's disposition of each
file. Original implementations required by unqualified platforms remain present.

Updated differential fixtures cover schema upgrades, search, projection filters,
fork visibility, atomic event guards, outbox behavior, owner selection, all active-run
statuses and provider maintenance. They compare against immutable source from the
integrated commit. Normal application acceptance still needs the built server,
client and controlled provider flows, including restart and storage-failure paths.

The source revision is separate from behavioral qualification. Acceptance records
must identify the exact tested fork commit, compiler package and affected platform.
Component success does not establish full application or cross-platform acceptance.
This source report does not replace those receipts.
