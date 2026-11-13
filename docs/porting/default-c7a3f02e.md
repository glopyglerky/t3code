# Upstream integration c7a3f02e

The source advances from `2d8a843e6be9ca170f3bce98cc92ad76617b00c4` to
`c7a3f02e3851923ced8ebd000db79f641e11b038`, the final upstream cutoff for this
qualification. The merge preserves both histories and the preceding native
context-ordering repair. The schema-89 upgrade, shell reads and retained platform
boundaries are described in [the preceding integration report](default-2d8a843e.md)
and [the native cutover record](redb-cutover.md).

## Desktop settings

One upstream commit changes `DesktopAppSettings.ts` and its existing test suite.
Both files retain their exact upstream contents. Each optional setting now handles
its own decoding failure, so an unreadable value drops only that key. Other valid
preferences survive loading and the next write. The regression case combines
`localEnvironmentEnabled: false` with an obsolete server-exposure value, then
updates the window bounds and checks that the local environment remains disabled.

The test uses scoped temporary settings directories and a controlled desktop
environment. The existing settings suite and desktop typecheck cover this
integration without accessing an installed application's state.

## Native source and provenance

The two desktop files are the complete upstream delta. Rust, server, contracts,
shared packages, providers, dependencies and migration sources are unchanged.
All 80 mapped upstream oracle entries retain their previous Git blob identities.
Schema 89 and the preserved schema-80/schema-88 fixtures therefore remain intact.
The context rollback fix retains the source run and attempt ordering observed
after migration 89, with immutable-upstream, retained-SQLite and native comparisons
before and after restart.

The source pin, inventories and reviewed port revisions now refer to `c7a3f02e`.
Only the desktop inventory changes. The event-store oracle names this same commit
and continues to assert its independent, unchanged Git blob identity. The wrapped
upstream README body is unchanged.

This integration retires no additional implementation. Source integration and
the focused desktop checks do not establish full application qualification. The
final candidate still requires its exact native, adapter, application and browser
acceptance run before main can advance.
