import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";

import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import { hash, nativeRequest, normalized, withSqlite } from "./redb-domain-fixture.mjs";
import { fixedTime, jsonPayloads, makeEvent } from "./redb-event-sink-fixtures.mjs";

const threads = "orchestration_v2_projection_threads";
const turns = "orchestration_v2_projection_provider_turns";
const threadCreatedAt = "2026-11-10T08:30:00+02:30";
const threadArchivedAt = "2026-11-09T21:15:42.120-03:30";
const tokenUsageUpdatedAt = "2026-11-10T04:05:06-04:00";

function row(snapshot, table, key, value) {
  const rows = snapshot[table].filter((candidate) => candidate[key] === value);
  Assert.equal(rows.length, 1, `Expected one ${table} row for ${value}`);
  return rows[0];
}

const providerThreadEvent = (id) => makeEvent("provider-thread.updated", {
  ...jsonPayloads.providerThread,
  updatedAt: "2026-11-10T11:59:00.000Z",
}, { id });

const providerTurnEvent = (id, extra = {}) => makeEvent("provider-turn.updated", {
  ...jsonPayloads.providerTurn, model: "codec-fixture-model", ...extra,
}, { id });

/** Run codec compatibility cases through the complete EventSink compositions. */
export async function checkProjectionCodecs({ executable, seedSource, withServices }) {
  const sourceHash = hash(await FS.readFile(seedSource));

  async function compare(name, mutate, exercise) {
    const fixture = await createFixture();
    try {
      const source = Path.join(fixture.root, "source.sqlite");
      const upstream = Path.join(fixture.root, "upstream.sqlite");
      const retained = Path.join(fixture.root, "retained.sqlite");
      const exported = Path.join(fixture.root, "source.ndjson");
      await FS.copyFile(seedSource, source);
      await withSqlite(source, (sql) => Effect.gen(function* () {
        yield* mutate(sql);
        yield* sql`PRAGMA wal_checkpoint(TRUNCATE)`;
      }));
      await FS.copyFile(source, upstream);
      await FS.copyFile(source, retained);
      exportSqliteState({ source, destination: exported });
      nativeRequest(executable, fixture, "initialize", {
        operation: "import_sqlite_application", path: exported,
      });
      const expected = await withServices("upstream", upstream, exercise, fixture);
      Assert.deepEqual(await withServices("sqlite", retained, exercise, fixture), expected,
        `${name}: retained SQLite must agree with immutable upstream`);
      Assert.deepEqual(await withServices("redb", source, exercise, fixture), expected,
        `${name}: native EventSink must preserve complete rows and publication behavior`);
    } finally {
      await fixture.close();
    }
  }

  await compare("legacy projection codec round trips", (sql) => Effect.gen(function* () {
    const threadRows = yield* sql`SELECT payload_json FROM orchestration_v2_projection_threads
      WHERE thread_id = ${"thread:main"}`;
    Assert.equal(threadRows.length, 1);
    const thread = {
      unknownLegacyField: { shouldDisappear: true },
      ...JSON.parse(threadRows[0].payload_json),
      createdAt: threadCreatedAt,
      archivedAt: threadArchivedAt,
    };
    delete thread.settledOverride;
    delete thread.settledAt;
    delete thread.lastVisitedAt;
    thread.lineage.unknownLegacyField = "discard nested unknown fields too";
    yield* sql`UPDATE orchestration_v2_projection_threads
      SET payload_json = ${JSON.stringify(thread, null, 2)},
        created_at = ${threadCreatedAt}, archived_at = ${threadArchivedAt}
      WHERE thread_id = ${"thread:main"}`;
    const turnRows = yield* sql`SELECT payload_json FROM orchestration_v2_projection_provider_turns
      WHERE provider_turn_id = ${"turn:main"}`;
    Assert.equal(turnRows.length, 1);
    const turn = {
      ...JSON.parse(turnRows[0].payload_json),
      unknownLegacyField: "discarded by the stored-turn decoder",
      startedAt: "2026-11-10T14:00:00+02:00",
      tokenUsage: {
        unknownLegacyField: "discarded before sparse preservation",
        usedTokens: 17, maxTokens: null, updatedAt: tokenUsageUpdatedAt,
      },
    };
    yield* sql`UPDATE orchestration_v2_projection_provider_turns
      SET payload_json = ${JSON.stringify(turn, null, 2)}
      WHERE provider_turn_id = ${"turn:main"}`;
  }), ({ sink, snapshot, publications }) => Effect.gen(function* () {
    const before = yield* snapshot;
    const oldThread = JSON.parse(row(before, threads, "thread_id", "thread:main").payload_json);
    Assert.equal(Object.hasOwn(oldThread, "settledAt"), false);
    Assert.equal(Object.hasOwn(oldThread, "lastVisitedAt"), false);
    const stored = yield* sink.withProviderBatch(sink.write({ events: [
      providerThreadEvent("codec:legacy:thread"), providerTurnEvent("codec:legacy:turn"),
    ] }));
    const after = yield* snapshot;
    const threadRow = row(after, threads, "thread_id", "thread:main");
    const thread = JSON.parse(threadRow.payload_json);
    Assert.equal(thread.settledOverride, null);
    Assert.equal(thread.settledAt, null);
    Assert.equal(thread.lastVisitedAt, null);
    Assert.equal(thread.createdAt, "2026-11-10T06:00:00.000Z");
    Assert.equal(thread.archivedAt, "2026-11-10T00:45:42.120Z");
    Assert.equal(thread.updatedAt, fixedTime, "The common event timestamp follows provider activation");
    Assert.equal(Object.hasOwn(thread, "unknownLegacyField"), false);
    Assert.equal(Object.hasOwn(thread.lineage, "unknownLegacyField"), false);
    Assert.equal(threadRow.created_at, threadCreatedAt, "Unmodified SQL columns retain their original bytes");
    Assert.equal(threadRow.archived_at, threadArchivedAt);
    const turnRow = row(after, turns, "provider_turn_id", "turn:main");
    const turn = JSON.parse(turnRow.payload_json);
    Assert.deepEqual(turn.tokenUsage, { usedTokens: 17, maxTokens: null, updatedAt: tokenUsageUpdatedAt });
    Assert.equal(Object.hasOwn(turn, "unknownLegacyField"), false);
    Assert.equal(turn.model, "codec-fixture-model");
    Assert.ok(turnRow.payload_json.indexOf('"tokenUsage"') < turnRow.payload_json.indexOf('"model"'),
      "Preserved token usage retains the original encoder's property order");
    Assert.deepEqual(publications.map((record) => record.id), ["codec:legacy:thread", "codec:legacy:turn"]);
    return normalized({ stored, snapshot: after, publications });
  }));

  function expectAtomicDecodeFailure(services, id, failingWrite) {
    const { sink, snapshot, publications } = services;
    return Effect.gen(function* () {
      const before = yield* snapshot;
      const publishedBefore = publications.length;
      let staged = false;
      const exit = yield* sink.withProviderBatch(Effect.gen(function* () {
        yield* sink.writeWithEffects({
          events: [makeEvent("provider-session.detached", {
            providerSessionId: `session:${id}`, detachedAt: fixedTime,
          }, { id: `codec:${id}:staged` })],
          effects: [{ id: `codec:${id}:effect`, commandId: `command:codec:${id}`,
            threadId: "thread:main", request: { type: "terminal.cleanup" } }],
        });
        staged = true;
        const pending = yield* snapshot;
        Assert.ok(pending.orchestration_events.some((record) => record.event_id === `codec:${id}:staged`));
        yield* failingWrite;
      })).pipe(Effect.exit);
      Assert.equal(staged, true, "The failure must follow an actual staged event and outbox write");
      Assert.ok(Exit.isFailure(exit), `${id}: malformed stored payload must fail`);
      Assert.deepEqual(yield* snapshot, before, `${id}: every row must roll back byte for byte`);
      Assert.equal(publications.length, publishedBefore, `${id}: failed decoding must publish nothing`);
    });
  }

  await compare("malformed owning thread", (sql) =>
    sql`UPDATE orchestration_v2_projection_threads SET payload_json = ${"{}"}
      WHERE thread_id = ${"thread:main"}`,
  (services) => Effect.gen(function* () {
    const { sink, snapshot, publications } = services;
    yield* expectAtomicDecodeFailure(services, "invalid-thread-activation",
      sink.write({ events: [providerThreadEvent("codec:invalid-thread:activation")] }));
    for (const runId of ["run:main", "run:absent"]) {
      yield* expectAtomicDecodeFailure(services, `invalid-thread:${runId}`,
        sink.writeWithEffects({ updateDelegatedCompletionOnly: true, effects: [], events: [
          makeEvent("run.updated", { ...jsonPayloads.run, id: runId, ordinal: runId === "run:main" ? 1 : 100,
            delegatedCompletion: { disposition: "open", nextGeneration: 3, delivery: null } },
          { id: `codec:invalid-thread:${runId}` }),
        ] }));
    }
    return { snapshot: yield* snapshot, publications };
  }));

  await compare("malformed old turn without token usage", (sql) => Effect.gen(function* () {
    const rows = yield* sql`SELECT payload_json FROM orchestration_v2_projection_provider_turns
      WHERE provider_turn_id = ${"turn:main"}`;
    Assert.equal(rows.length, 1);
    const turn = { ...JSON.parse(rows[0].payload_json), status: "invalid-stored-status" };
    delete turn.tokenUsage;
    yield* sql`UPDATE orchestration_v2_projection_provider_turns
      SET payload_json = ${JSON.stringify(turn)} WHERE provider_turn_id = ${"turn:main"}`;
  }), (services) => Effect.gen(function* () {
    const { sink, snapshot, publications } = services;
    yield* expectAtomicDecodeFailure(services, "invalid-old-turn",
      sink.write({ events: [providerTurnEvent("codec:invalid-turn:omitted-usage")] }));
    // Explicit token usage bypasses the old-row decoder in the original writer.
    const repaired = yield* sink.write({ events: [providerTurnEvent("codec:invalid-turn:explicit-usage", {
      tokenUsage: { usedTokens: 23, updatedAt: fixedTime },
    })] });
    const after = yield* snapshot;
    Assert.equal(JSON.parse(row(after, turns, "provider_turn_id", "turn:main").payload_json).tokenUsage.usedTokens, 23);
    Assert.deepEqual(publications.map((record) => record.id), ["codec:invalid-turn:explicit-usage"]);
    return normalized({ repaired, snapshot: after, publications });
  }));

  Assert.equal(hash(await FS.readFile(seedSource)), sourceHash, "Codec fixtures must not mutate their seed database");
  return [
    "Provider-thread activation round-trips legacy defaults, alternate date encodings and unknown fields through the original codecs while preserving untouched columns",
    "Sparse provider-turn updates preserve decoded token usage and encoded property order, including nested unknown-field removal and literal token-usage timestamps",
    "Malformed owning threads reject activation and delegated-only updates for both present and absent runs, rolling back staged events, outbox rows and publications",
    "Malformed old turns fail even when no token usage would be retained; explicit token usage follows the original decoder bypass and repairs the row",
  ];
}
