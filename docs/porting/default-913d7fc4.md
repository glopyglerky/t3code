# Upstream integration 913d7fc4

The source advances from `c7a3f02e3851923ced8ebd000db79f641e11b038` to
`913d7fc45ec5814fff25c819cfe4de193ea42d63`, the upstream default branch cutoff
fetched on 2026-11-14. This integrates 293 upstream commits and changes across
887 upstream paths. The merge keeps both histories and the existing fork
ancestor `3577878a5e008c8af87a8fbdae13d26dea1c2f50`.

The earlier scan at `121f0cf014dcd6e94327588bf3f1f200a5f07ad7` covered the
main integration. Five subsequent commits add the boot-service SSH wait,
custom Claude thinking choices, external file reload recovery, pull-request
reference links around embedded media, and PageUp/PageDown labels. The tail
retains its upstream implementation. Its client atom lifecycle changes do not
replace a mapped Rust algorithm. Source inventories and all mapped oracle
blobs refer to the final cutoff.

## Native persistence and API coverage

The production-selected omdb graph now accepts migrations 90 and 91, including
Connect device registrations and denied-request records. The retained schema
80, 88 and 89 artifacts exercise upgrade to schema 91. Upgrade constructs a
new generation, rebuilds derived projection metadata, initializes the current
auth index version and publishes the catalog atomically. Interrupted staging
keeps the previous generation readable; a completed native activation never
returns to its preserved SQLite source.

The Connect repositories now support device listing, insertion, lookup and
revocation; denied-request persistence and expiration; expanded pairing-link
lookup and deletion; and session lookup, expiration and device revocation.
All 24 public auth methods are exercised against immutable upstream SQLite
behavior. Runtime wiring selects these repositories for the real server and
CLI. Durable SQL codecs stay at the shared contract boundary, while the
selected repository performs native queries and mutations.

Additional native API work follows the upstream contracts:

| Boundary            | Integrated behavior                                                                                                                                                            |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| EventSink           | Exact serialized subagent payload guards share the reserved writer with event append and projection mutation. Missing and stale observations produce no events or publication. |
| Thread visits       | Partial visit events update read state, preserve omitted open statistics, honor explicit null, and leave thread activity timestamps unchanged.                                 |
| Projection listings | `listActiveThreads` returns every nondeleted, nonarchived thread, including subagents and side chats, with optional project filtering. Pull-request rows include `updatedAt`.  |
| Fork views          | A live fork can inherit from a deleted source while directly requested deleted threads remain hidden.                                                                          |
| Preparation context | Unsent manual continuations include their original source prompt; both readers receive the same selected message IDs.                                                          |
| Tool work           | Inline work includes delegated tasks, sent messages and `send_to_user`; schema upgrade rebuilds existing summaries.                                                            |
| Search              | Native message search follows the new encoded substring inputs and SQLite GLOB semantics, including escaped punctuation and Unicode cases.                                     |
| Cleanup             | Enabled scheduled-task thread IDs come from the selected persistence service, keeping scratch cleanup independent of SQL on the native graph.                                  |
| EffectWorker        | Native writer contention uses a narrow retry classification; unrelated storage errors still propagate.                                                                         |

The server's selected runtime layer also exposes the selected provider runtime
repository to per-connection services. This keeps the CLI and server layer
contracts complete after upstream's service-graph changes.

## Provider maintenance and generation

Native maintenance follows the new ownership rules. MacPorts-owned binaries
produce manual upgrade instructions only after ownership validation. Mise uses
the configured backend key from its installed-tool listing, while its lazy
latest-version lookup keeps the upstream short tool name. Npm global installs
use the upstream minimum-release-age-aware latest-version lookup and a cache
key scoped to installation and environment. Failed or untrusted ownership
observations do not select an automatic updater.

Structured Codex generation places the disabled MCP table before user launch
arguments, repeats it only when those arguments replace the entire table, and
appends safe per-server disable overrides. Server names containing dots or
equals signs remain covered by the table instead of ambiguous dotted keys.
The retained platform implementation receives the same upstream change.

## Replacement boundaries and source retention

The merge keeps thin native entrypoints for already ported policy, persistence,
maintenance and generation behavior. It does not restore full upstream business
implementations into those entrypoints. Shared application orchestration,
codecs, browser state and platform integrations remain at their upstream paths.

No additional whole-platform implementation can be retired from this source
integration alone. Linux native application acceptance is separate from Windows,
macOS, iOS and Android lifecycle qualification. Their required selected-host
implementations remain available. This is also why SQLite's migration exporter,
immutable differential oracle and unqualified-host implementation stay in the
repository while the native production path remains authoritative.

Upstream removes an obsolete Grok transcript fixture and replaces its pinned
mobile Markdown archive; `port-map.json` records both source transitions.
Previously retired upstream release workflows remain absent. Licenses, original
sources and both Git histories remain recoverable.

## First hosted qualification failure and connection repair

The first candidate's [hosted qualification run](https://github.com/glopyglerky/t3code-ls/actions/runs/43592588234)
failed the connection differential check. Its immutable-source harness omitted
upstream's new `backoffFailures` state. Reviewing the source revealed a matching
Rust gap: approval waits, connection attempts and failure backoff now have
separate counters, and focus wakeups have distinct handling. The Rust connection
reducer, source extraction and matrix were repaired together. The focused gate
then passed 61,304 source-derived cases covering delays, connection loss,
transitions and wakeups. The transport remains a candidate; this repair does
not route the production browser through Iroh.

The failed run is retained as evidence. Its Rust and source guards passed, while
application jobs were skipped after the failed dependency. It therefore never
qualified a main update. The subsequent policy adds required paired performance
and reliability measurements against immutable stock source, as described in
[the CI and measurement policy](../ci-and-porting.md).

## Verified Linux application flow at bc0d469

The next [hosted run](https://github.com/glopyglerky/t3code-ls/actions/runs/43597075132)
produced passing `application.json` and `client/native/proof.json` records for
clean source `bc0d46941ccac37a97062fb135544c62381cf453`, tree
`ae264726ef695ccb96006236c4745f8e3c915360`. The native selection checks and
production client/server bundle builds passed. The browser proof uses the
retained web client on Linux x64 with the same run's release helpers, compiled
through the pinned Lustr/BlueFe route.

The flow completed normal initial setup and local project creation, approved a
controlled ACP replay-provider request, received a completed turn on the
original thread route, and recovered the same completed thread after client
reload. It then restarted the server over the same state and verified durable
recovery in a fresh client with the existing session. Read-only native
inspection confirmed canonical omdb rows and event heads, with no application
SQLite files. The fixture used no real provider turn or installed user state.

The [application artifact](https://github.com/glopyglerky/t3code-ls/actions/runs/43597075132/artifacts/14116230036)
was downloaded and verified against SHA256
`17a594b69f65a47ad6e0fd14ddf33604edb75af59ab1c21f4c3e7cbb22828042`.
It belongs to application job `132458641004`, run attempt 1. The accompanying
native/source/component and production-adapter jobs also passed. This evidence
covers that exact Linux source and controlled flow; it does not establish
desktop, mobile, macOS, Windows, Iroh or native Lust acceptance.

The recorded [stock comparisons](../performance/persistence-api.md) still show
regressions, so this application evidence alone cannot qualify a main update.
The subsequent dispatcher and runtime-provenance changes require their own
exact-source application and performance gates. Earlier passing application
artifacts cannot qualify those later runtime changes.

## Verified dispatcher application flow at c3f8245

The [dispatcher candidate's hosted run](https://github.com/glopyglerky/t3code-ls/actions/runs/43600673237)
also passed native/source/component checks, production adapters and the full
Linux application job. Its `application.json`, `client/native/proof.json` and
`client/summary.json` records pass for clean source
`c3f8245ed7daf3c16dcfcfbc33682811728bc5bf`, tree
`04386131068654c883f2cca8689b5f5853c74eb3`. The same normal web-client flow
covered project creation, controlled ACP approval, completed turn, reload,
fresh-client recovery after a same-state server restart, and read-only native
row/event-head inspection without application SQLite files.

The [original application artifact](https://github.com/glopyglerky/t3code-ls/actions/runs/43600673237/artifacts/14116622101)
has verified ZIP SHA256
`e4e50ddc5efaa221c0be35ff9eeb559e0eb2b2ff4915f16378316b581d7db474`.
It belongs to job `132467286580`, run attempt 1, and uses that run's release
helpers through the pinned Lustr/BlueFe route. This verifies the dispatcher
candidate within the stated Linux flow; desktop, mobile, other operating
systems and native Lust remain outside that evidence.

Its [hosted stock comparison](../performance/measurements/2026-11-14-hosted-dispatcher.md)
still regressed in all eleven latency rows. Performance and final qualification
therefore failed despite the successful application flow. Existing main remains
unchanged, and subsequent read-table and write changes need
their own exact-source application and performance evidence.

## Verified table-handle application flow at b5250fe8

The [table-handle candidate's hosted run](https://github.com/glopyglerky/t3code-ls/actions/runs/43602183576)
passed native/source/component checks, production adapters and the original
Linux browser/restart flow for clean source
`b5250fe811e64ef06dd6b79963191ee48a91fe8f`, tree
`85230b3187cd32bbeafb88b32d14b91f58363b27`. Its scope matches the controlled
flow described above: no paid provider turn, installed user state or additional
platform acceptance is implied.

The [application artifact](https://github.com/glopyglerky/t3code-ls/actions/runs/43602183576/artifacts/14117400950)
has verified ZIP SHA256
`721cb125ab463e2c130635dbf09688e7c23ce391207a9526c4bd6047975c606e`.
It belongs to job `132471888947`, run attempt 1. The
[hosted comparison](../performance/measurements/2026-11-14-hosted-table-handle.md)
regressed in all eleven speed rows, so final qualification rejected the failed
performance job. That rejection also verifies the improved failure diagnostic:
the receipt names the failed required gate instead of attempting an empty
artifact download. Main did not advance.

Subsequent write changes and the [5fe2f7e4 source advance](default-5fe2f7e4.md)
require their own exact-source gates. This historical application evidence
remains tied to b5250fe8 and upstream 913d7fc4.

## Verified write-pruning application flow at d661dbb2

The [write-pruning candidate's hosted run](https://github.com/glopyglerky/t3code-ls/actions/runs/43603651670)
also passed native/source/component checks, production adapters and the full
controlled Linux application flow for clean source
`d661dbb2bf693b88c8969fcda9a0f5b98e3a2cca`, tree
`549bff502c033739693b562a854437c0292a5019`. Its application build and
`client/native/proof.json` pass for the same setup, project creation, controlled
ACP approval, completed turn, reload and fresh-client durable restart scope.

The [application artifact](https://github.com/glopyglerky/t3code-ls/actions/runs/43603651670/artifacts/14117637443)
has independently verified ZIP SHA256
`65924edff9a2407e42f22519eefaa902d96f99ec853a9b77ceb0ee6622c9b913`.
It belongs to job `132477074663`, run attempt 1. This establishes that exact
Linux application flow, without extending it to other clients, platforms,
native Lust or real provider turns.

The [hosted performance report](../performance/measurements/2026-11-14-hosted-write-pruning.md)
retains all eleven failing rows, including the much longer observed visit
tail. Its host differs from the local diagnostic, so those observations do
not establish that write pruning caused the tail. Performance and final
qualification failed; main remained unchanged. The subsequent 5fe2f7e4/count
candidate removes the unproven table-handle and write-pruning experiments
while preserving their measurements and useful general regression tests.

## Qualification

Source integration is complete; exact-candidate qualification is recorded by the
fork workflow and its artifacts, independently of this document. Local focused
checks cover the verified packaged Lustr/BlueFe route, auth repositories,
schema startup and upgrade, search, maintenance, the selected production graph,
and TypeScript layer contracts. Expanded differential cases cover the new
guards, visits, shell reads and MCP ordering.

Full application and browser acceptance must run against the final candidate
commit before main advances. The hosted workspace cannot launch Chromium due
to its kernel socket restriction, so local component evidence does not claim
browser or desktop acceptance. No real provider turn or installed application
state is used for qualification.
