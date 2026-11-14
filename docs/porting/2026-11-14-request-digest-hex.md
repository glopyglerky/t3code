# Request digest hex encoding experiment

This candidate starts from `4b5719f34a8b76a23f3c77982940cea67234186c`, whose
upstream cutoff is `f751c8488f7ca1e659e44ef12b05861c1bcf9bfb`. It does not carry
the isolated node-root, history-borrowing or search-seek experiments. Main is
unchanged. This document records the reviewed source and subsequent local
compatibility checks. No release benchmark, full application or hosted
qualification result is claimed for this change.

The final package also includes the separately reviewed
[controlled browser diagnostics](controlled-browser-diagnostics.md), their five
passing pure tests and required CI/typecheck/source ownership wiring. Those
changes add bounded failure context to the existing browser proof; they do not
change application assertions, runtime Rust, performance thresholds or the
digest compatibility inputs that were tested below.

## Why this operation is being examined

The [finer fixed-workload attribution](../performance/measurements/2026-11-14-finer-attribution-a2fb16d.md)
measured substantial inclusive time in the
existing `digest_hex` span. Across twelve measured calls per operation, the
reported means were 30.601 ms and 20,587 calls for sidebar, 22.034 ms and 15,169
calls for history, 5.113 ms and 3,472 calls for message search, and 6.900 ms and
3,966 calls for accent search. The span includes SHA finalization and the old
formatted-string conversion. Observer overhead and nested inclusive time prevent
treating these numbers as removable wall time or as a forecast of application
speedup. They identify a narrow operation worth testing.

That diagnostic measured isolated a2fb source with an observer; this candidate
starts from f751-based 4b and has no observer. The report and its raw archive are
included byte-for-byte, without relabeling their measured source or treating the
fixed-order probes-off/on workers as a controlled speed comparison.

The source currently formats each of 32 digest bytes into a two-character
String and collects the fragments. The experiment replaces only that final
representation step with one String reserved for 64 bytes and two lowercase
hex characters appended per byte. It introduces no unsafe code, dependency,
cache, protocol field or new error path. The source no longer constructs those
32 formatted fragments; no allocation count or latency improvement has been
measured for this candidate.

## Persisted identity is the compatibility boundary

`request_digest` is used in physical application row keys, projection/index
witnesses, generation/import identities, auth/event locations and command/outbox
replay. These are persisted names and equality checks, not presentation text.
A difference in even one byte could hide existing data or turn a valid retry
into a conflict. There is no migration in this experiment because byte identity
is a required invariant, not a best-effort conversion.

The SHA-256 input and `hash_json` function remain byte-for-byte unchanged:
object keys are sorted, array order is preserved, keys/scalars use the same
serde_json encoding, and separators/nulls/Unicode/number spelling are unchanged.
The result remains exactly 64 lowercase ASCII hex bytes, including leading
zeroes. Table-name encoding, other digest implementations, database schema,
indexes, read/write budgets, transaction observation, cancellation and Immediate
durability are unchanged. The private import/outbox hex routines are not moved
or exported; this keeps the experiment restricted to one hot function.

## Compatibility fixtures prepared

The existing same-version round trips derive both the seeded and incoming key
using the current digest function. These additional fixtures break that symmetry:

1. A public-function table compares literal historical SHA-256 vectors for
   null, empty string/array/object, nested Unicode plus newline and boolean,
   reordered object keys, reversed arrays, integer 286 with a leading `00`,
   and a typed signed-64-bit/blob compound key. Expected strings were computed
   independently over explicit canonical UTF-8 JSON during source review.
2. A valid typed compound row is seeded directly at its complete literal
   historical physical key, including the unusual table name's UTF-8 hex
   prefix. After close/reopen, the public application transaction must read the
   full row, and read-only inspection must find the identical bytes at that
   original key. The seed does not use the changed digest or row-key helper.
   This is a frozen old-key fixture, not a claim that an old executable ran.
3. The existing legacy project receipt test now seeds a literal old command
   digest. The normal native command path recomputes the incoming digest after
   reopen and must replay the original receipt/result without changing the
   snapshot. A changed command with that ID must still conflict without writes.

Existing atomic-batch replay, outbox replay/reopen, typed cell, corruption,
import/schema upgrade and application transaction suites remain required.
No allocation-count test, timing assertion, weakened corruption check or special
case in production was added to make these fixtures pass.

## Validation and measurement decision

Source review and pinned formatting passed. The serial local checks then ran
through the actual pinned Lustr coordinator and produced BlueFe drivers, with
one CPU/job, a 900-second limit and a 16 MiB log cap per command. The complete
160-file build-input manifest includes Rust/Cargo/toolchain files and embedded
JSON/schema inputs. All source, tool and working-tree checks matched before and
after each stage.

| Local stage                                                           | Result                                            | Elapsed seconds |
| --------------------------------------------------------------------- | ------------------------------------------------- | --------------: |
| Complete existing storage integration target, including fixed vectors | 11 passed, 0 failed, 0 ignored                    |          45.293 |
| Literal old compound key read after reopen                            | 1 passed; 202 library tests filtered              |          37.001 |
| Frozen legacy receipt replay and conflicting-command rejection        | 1 passed; 202 library tests filtered              |           0.112 |
| Explicit debug helper build                                           | Passed, reused the fresh integration-target build |           0.078 |

The first stage recompiled core/storage and built the helper through Lustr; the
library compatibility stage also compiled the changed test target. The final
explicit helper build reused those outputs and is not described as another
fresh compilation. This is thirteen executed tests, not a full local Rust suite.
The preserved debug helper SHA-256 is
`6afcc346a330c64555c292f0e25ab00f04f64f82523b2f7d2801eab9a3bbd231`.

The [local evidence archive](evidence/2026-11-14-request-digest-hex-local.raw.tar.gz)
contains the source patch, both source manifests, exact commands/supervisor,
four stage receipts/logs and the aggregate helper receipt. It is 28,976 bytes,
14 safe regular entries, SHA-256
`18b69dbc20510cac9783fb569ca786e78a6c8de079e21cf4ab45ed473102e0d1`.
Every one of its thirteen payloads is size/hash-bound by its manifest. No
executable or database bytes are included. The aggregate receipt SHA-256 is
`eb78afa5079a962ed8d6a6b289c455d15da66b17bb477e19e17b889286a3db3c`.

Before any carry decision, the candidate still requires the normal exact-source
hosted policy, including production adapters, the original Linux browser/recovery
flow and release performance comparison. Local diagnostic evidence does not
qualify the application or permit main promotion.

The unchanged stock/native gate remains eight balanced pairs, three warmups,
twelve samples and the same eleven individual latency rows. A separate controlled
same-host before/after comparison may establish attribution, but cannot replace
that gate or offset a slow row with a gain elsewhere. All failed, inconclusive
and successful observations must be retained. Neither the profiler's inclusive
time nor a lower source-level allocation count qualifies this candidate.

The [borrowing experiment's complete hosted result](../performance/measurements/2026-11-14-history-eligible-borrow.md)
is retained separately in this package as historical evidence. Its frozen 5fe oracle and source identity
must not be relabeled as this f751 candidate or used as its performance result.
