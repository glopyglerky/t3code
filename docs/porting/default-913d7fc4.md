# Upstream integration 913d7fc4

The source advances from `c7a3f02e3851923ced8ebd000db79f641e11b038` to
`913d7fc45ec5814fff25c819cfe4de193ea42d63`, the upstream default branch cutoff
fetched on 2026-11-14. This integrates 293 upstream commits and changes across
887 upstream paths. The merge keeps both histories and the qualified fork
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
