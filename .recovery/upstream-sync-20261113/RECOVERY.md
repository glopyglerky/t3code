# Incomplete upstream-sync recovery archive

This branch preserves recoverable work after the hosted execution environment failed during publication. **It is incomplete and must not be merged or treated as a working candidate.**

Production source remains byte-for-byte at `17ec4bae52471ba66f24f6abadbf29e4498106e7`. Every change in this recovery commit is under `.recovery/upstream-sync-20261113/`. Main is unchanged. The recovery branch is outside the automatic qualification workflow's push filters.

## What is preserved

The attempted integration targeted upstream `ed7e5010baafd73d0164ee1d5adbd21e26f8f9cb`, advancing 1,054 commits from `7171fdd7716b05dc1b4be40c26ac78e8bb35f80c`. Work included native schema80-to88 upgrades, current projection and orchestration contracts, Codex generation and maintenance parity, KeepAwake reads, and legacy subagent ID retention.

The archive contains **64 verified Git blobs, 5,448,463 bytes**. Another **94 expected blobs, 1,045,324 bytes**, could not be recovered before the executor became unavailable. The object files retain their original Git blob identities. The manifest maps every expected object to its canonical source path and marks missing data explicitly.

Expected local merge commit: `2f30c6c17b63d17ae0a1052a9f304117bcaa2c27`, tree `fd8f5f4a9544ed7a57fa46cfac2ecfd9818ab9ac`, parents `17ec4bae52471ba66f24f6abadbf29e4498106e7` and `ed7e5010baafd73d0164ee1d5adbd21e26f8f9cb`.

Expected local follow-up: `ee279fead1c152d286ba81fd561d55869b956f38`, tree `707b3bfe125b7c3c3bbdbf2aa3effc92b4d92ea1`. Neither intended commit was published. Their hashes identify the lost local snapshots and do not imply that GitHub holds those complete commits.

## Checks completed during implementation

- Frozen pnpm workspace installation passed.
- The full five-package Rust run reported 368 passed and 0 failed across 37 test binaries. This preceded the final receipt-lineage, citation and legacy-ID changes.
- Seven production adapter typecheck projects passed with `--singleThreaded`. Six mapped component projects passed before the large CLI checker was killed under host memory pressure.
- Source retention and exact upstream README checks passed for the integrated upstream pin.
- Runtime migration-head, startup, CLI, projection-record, receipts/positions, thread-search and project gates passed during the session.

These are partial observations from working revisions. They do not qualify the complete intended snapshot. The existing main baseline's successful run is [43089751300](https://github.com/glopyglerky/t3code-ls/actions/runs/43089751300).

## Remaining repairs

- Restore the shared shell response's `latestRunBackgroundWake` field.
- Make the simulated schema80 fixture export through the same private-backup path used by real startup. Its current direct export produces a different schemaVersion header.
- Correct the live-fork attachment fixture's guest-share expectation after metadata replacement, using immutable SQLite behavior as the oracle.
- Correct the windows fixture's turn-item status to satisfy the current event schema.
- Recover the outbox oracle follow-up that adds `ModelSelection` and `NonNegativeInt` imports.

Final storage tests, the native production guard after the last API port, the full bundle, complete exact-candidate CI, provider restart/failure paths and browser acceptance remain outstanding.

## Recovery

Read [manifest.json](manifest.json) first. Each available object lives at `objects/<Git-blob-SHA>`. Recover the complete source tree and missing fixes in an isolated checkout, then verify the intended tree or review any replacement implementation. Run all required qualification gates on the resulting exact commit before advancing main.

Do not copy this partial archive over an installed application or use it as a database migration release.
