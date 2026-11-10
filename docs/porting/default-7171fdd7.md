# Default-branch 7171fdd7 integration

The trusted upstream source advances from `9e12eca12526c842f97cfcbc6f3cd2149259c776` to `7171fdd7716b05dc1b4be40c26ac78e8bb35f80c` (tree `0afcff3f07e7d56829c8339a9e0d7fa1e9de95f6`). The integrated nightly reference `8226715` remains an ancestor. This is a source integration, not a new assertion of full Rust or cross-platform acceptance.

Upstream changed 49 paths without adding or removing files. Forty-seven have no fork-specific changes and were imported directly from their upstream blobs; one upstream test already matched the fork. The remaining two files were merged three-way against the previous upstream revision without conflicts:

- `apps/server/src/orchestration-v2/Orchestrator.ts` retains the Rust guest-share, dispatch-policy, and recovery integrations while gaining upstream delegated completion acknowledgment after steer replacement.
- `apps/server/src/provider/CodexProvider.ts` retains native executable selection and no-fallback failure behavior while adopting upstream's offline status-probe environment.

The upstream changes also cover desktop window lifecycle, self-update, Git and source-control handling, browser evaluation, provider status probes and OpenCode behavior, Markdown paths, Forgejo, and SSH tunnels. Their upstream tests accompany the production files. The compiler and dependency locks, application public contracts, retained implementations, and CI job definitions have not been changed.

The source lock inventories the exact new upstream tree. The source history appends the new commit and retains all earlier revisions and reviewed upstream-removal records. The Codex process port's upstream source blob is refreshed to the new content; the existing Rust code and platform routes remain in place.

Qualification must run the complete selected fork gates on the candidate's exact commit before `main` advances. Production SQLite and Connect remain authoritative. No original implementation is retired, and no new platform acceptance is claimed.
