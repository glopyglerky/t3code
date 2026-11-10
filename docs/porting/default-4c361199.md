# Default-branch 4c361199 and guest upload classification

This change advances the integrated upstream source from
`934c7f8d8e7a012aa55be9c0b17498979040fe02` to
`4c361199f47f6ea32a2c14835df392640d0f7c63`, observed on the trusted default
branch of `pingdotgg/t3code`. The fork base is
`bfcfa46b7c3d7d1fc7bbccfae89167be6f0e869b`.

## Upstream integration

Both changed upstream files are imported at their exact blob identities:

- `apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.ts`
- `apps/server/src/orchestration-v2/Adapters/ClaudeAdapterV2.test.ts`

Claude MCP approval cards can offer the existing `acceptForSession` decision.
An approved server name and exact prompt message are remembered for that live
adapter session. Different servers or messages still ask. The existing web and
mobile approval controls already support the decision; no contract or client
changes are needed. Upstream's tests remain the trusted source tests.

The source history records four integrated revisions. Existing mapped source
blobs, the upstream README, dependency locks and compiler pins are unchanged.
The 51 reviewed upstream removal records remain intact. The `8226715` nightly
identity stays separate from this default-branch snapshot.

## Rust upload classification

`GuestShareService.storeUploads` now calls the existing guest-share helper after
reading file sizes and at most 12 header bytes, before copying any attachment.
Text-only submissions skip upload work. The new Rust module lives in its owning
domain at `apps/server/src/guest/guest_uploads.rs`, compiled by the existing core
crate and `t3-code-ls-guest-share` executable.

Rust preserves the original classifier's evaluation order and results:

1. Reject more than four files before evaluating size or type.
2. Reject a total above 10 MiB before inspecting individual types.
3. Preserve input order, reject empty files, and check PNG, JPEG, GIF and WebP
   signatures for supported image types.
4. Select the MIME text before its first semicolon, apply JavaScript whitespace
   trimming, and retain the original default and 100-unit length rules.
5. Replace control characters, path separators and specified direction controls
   in names, trim, truncate to 255 UTF-16 units, and apply the original fallback.

The private `classify-uploads` request contains names, MIME observations, sizes
and header bytes. It contains no file paths. Names and MIME values cross the
protocol as UTF-16 code units, preserving JavaScript's truncation behavior even
when a boundary leaves a lone surrogate. The host supplies JavaScript Unicode
lowercasing as a primitive fact; Rust owns the classification and filename
transformations. This avoids depending on the compiler's Unicode case tables.
Attachment names and MIME values also use this private encoding in later guest
decision and release requests. The adapter restores their strings before contract
validation and retained-field checks. This fixes an existing boundary where a
correctly truncated name could be copied and then rejected by the strict JSON
parser in the next helper call. Rust's global JSON parser remains strict.

The adapter checks the response schema, code-unit bounds, attachment count and
original byte counts. Native failures become the existing public `rejected`
error before attachment copying, without retrying the original classifier.
Existing authorization, IDs, storage paths, SQLite transactions and cleanup stay
in the application. Other supported hosts and explicit upstream mode retain the
original implementation at its original path.

The existing guest-share runtime and production adapter gates cover immutable
upstream parity, filename and MIME boundaries, malformed requests and responses,
actual service storage, failure before copying, and interruption. No new binary,
dependency, CI job or compiler check is introduced.

## CI selection

A port-map update that changes only the integrated commit, source history and
per-port reviewed commit now selects the source guard. Every other mapping value
must stay identical, including source blobs, ownership, Rust paths, adapters,
routes, tests, removal records and acceptance evidence. Runtime and contract
changes still select their affected checks alongside a provenance-only update.
Malformed metadata and any other mapping change keep full qualification.
The existing Cargo cache key includes Rust files in application and package
directories, so edits to domain-owned modules get their own cached outputs.

This change adds a mapped Rust implementation and edits the CI selector, so its
candidate must pass the existing full fork qualification before `main` advances.
Exact-commit receipt reuse remains available afterward. CI qualification does
not grant cross-platform retirement acceptance. No original implementation is
retired here; production SQLite and Connect remain authoritative.
