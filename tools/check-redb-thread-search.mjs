#!/usr/bin/env node
import * as NodeAssert from "node:assert/strict";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Schema from "effect/Schema";
import { ATTACHMENT_ONLY_BOOTSTRAP_PROMPT } from "@t3tools/shared/composerContextReferences";

import * as ThreadSearch from "../apps/server/src/orchestration-v2/ThreadSearch.ts";
import * as NativeThreadSearch from "../apps/server/src/orchestration-v2/NativeThreadSearch.ts";
import * as NativeProjectionCommit from "../apps/server/src/persistence/NativeProjectionCommitStore.ts";
import * as ProjectionStore from "../apps/server/src/orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../apps/server/src/orchestration-v2/ProjectStore.ts";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import { fixedTime, jsonPayloads, makeEvent } from "./redb-event-sink-fixtures.mjs";
import {
  createMigratedSqlite,
  finishProof,
  hash,
  loadUpstreamModule,
  nativeRequest,
  normalized,
  withNative,
  withSqlite,
} from "./redb-domain-fixture.mjs";

const encodeSearchRequest = Schema.encodeEffect(ThreadSearch.SearchRequest);

const [executable, proofPath] = process.argv.slice(2);
NodeAssert.ok(
  executable && NodePath.isAbsolute(executable),
  "Pass an absolute native database executable",
);
await NodeFSP.access(executable, NodeFSP.constants.X_OK);
const oracle = await loadUpstreamModule("apps/server/src/orchestration-v2/ThreadSearch.ts");
const projectionOracle = await loadUpstreamModule(
  "apps/server/src/orchestration-v2/ProjectionStore.ts",
);
const citationOracle = await loadUpstreamModule("packages/shared/src/assistantCitations.ts");
const citationFixtures = JSON.parse(
  await NodeFSP.readFile(
    new URL("../apps/server/src/persistence/fixtures/message-search-text.json", import.meta.url),
    "utf8",
  ),
).cases;
for (const fixture of citationFixtures) {
  const readable = citationOracle.module.assistantCitationsToPlainText(fixture.input);
  NodeAssert.equal(readable === fixture.input ? null : readable, fixture.expected, fixture.name);
}
const checks = [];
const projectId = "project:search";
const otherProjectId = "project:elsewhere";
const main = "thread:search";
const assistant = "thread:assistant";
const mutationThread = "thread:mutations";
const before = "2026-11-10T13:00:00.000Z";
const at = (minute) => new Date(Date.parse(fixedTime) + minute * 60000).toISOString();
const event = (type, payload, owner = main) => makeEvent(type, payload, { threadId: owner });
const thread = (id, extra = {}) =>
  event(
    "thread.created",
    {
      ...jsonPayloads.thread,
      id,
      projectId,
      title: id,
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: id },
      ...extra,
    },
    id,
  );
const message = (id, text, extra = {}, owner = main) =>
  event(
    "message.updated",
    {
      ...jsonPayloads.message,
      id,
      threadId: owner,
      runId: null,
      nodeId: null,
      role: "user",
      createdBy: "user",
      creationSource: "web",
      streaming: false,
      createdAt: at(1),
      updatedAt: at(1),
      text,
      ...extra,
    },
    owner,
  );
const projectEvent = (id, title) => ({
  sequence: 0,
  eventId: `event:project:${id}`,
  aggregateKind: "project",
  aggregateId: id,
  type: "project.created",
  occurredAt: fixedTime,
  commandId: null,
  causationEventId: null,
  correlationId: null,
  metadata: {},
  payload: {
    projectId: id,
    title,
    workspaceRoot: `/fixture/${id}`,
    defaultModelSelection: null,
    scripts: [],
    createdAt: fixedTime,
    updatedAt: fixedTime,
  },
});
const link = (number) => ({
  projectId,
  repository: "acme/search",
  number,
  url: `https://github.com/acme/search/pull/${number}`,
});
const seedEvents = [
  thread(main, { title: "SearchService implementation", branch: "fix/rust-search" }),
  thread(assistant, { title: "Trust the build" }),
  thread(mutationThread),
  thread("thread:other", { projectId: otherProjectId, title: "Elsewhere Rust" }),
  thread("thread:archived", { archivedAt: fixedTime }),
  thread("thread:deleted", { deletedAt: fixedTime }),
  thread("thread:subagent", {
    lineage: { parentThreadId: main, rootThreadId: main, relationshipToParent: "subagent" },
  }),
  thread("thread:side", {
    lineage: {
      parentThreadId: main,
      rootThreadId: main,
      relationshipToParent: "fork",
      sideChat: true,
    },
  }),
  thread("thread:old-title", {
    title: "Kestrel nest",
    settledOverride: "settled",
    settledAt: at(10),
    linkedPullRequest: link(4202),
  }),
  thread("thread:old-note", {
    title: "Unrelated",
    settledOverride: "settled",
    settledAt: at(20),
    branchPullRequest: link(4101),
  }),
  thread("thread:recent", { title: "Kestrel nest", settledOverride: "settled", settledAt: at(90) }),
  message("message:old", "Older needle question", { createdAt: at(1), updatedAt: at(1) }),
  message("message:new", "Newer needle question", { createdAt: at(2), updatedAt: at(2) }),
  message("message:answer", "Newest needle answer", {
    role: "assistant",
    createdBy: "agent",
    createdAt: at(3),
    updatedAt: at(3),
  }),
  message("message:rust", "Rust rewrite notes, not a trust exercise"),
  message(
    "message:assistant",
    "We trust the build needle",
    { role: "assistant", createdBy: "agent" },
    assistant,
  ),
  message("message:other", "Rust needle elsewhere", {}, "thread:other"),
  ...["archived", "deleted", "subagent", "side"].map((name) =>
    message(`message:${name}`, "Hidden needle Rust", {}, `thread:${name}`),
  ),
  message("message:streaming", "needle unfinished", { streaming: true }),
  message("message:generated", "needle generated notice", {
    createdBy: "agent",
    creationSource: "provider",
  }),
  message("message:system", "needle system prompt", {
    role: "system",
    createdBy: "agent",
    creationSource: "provider",
  }),
  message("async-answer:hidden", "needle asynchronous answer"),
  message("message:placeholder", ATTACHMENT_ONLY_BOOTSTRAP_PROMPT),
  message("message:ultrathink", `uLtRaThInK:\r\n\t\u00a0${ATTACHMENT_ONLY_BOOTSTRAP_PROMPT}`),
  message("message:ordinary", "Review the attached files in context"),
  message(
    "message:unicode",
    "École fermée à Lyon; Résumé ÀḈộ; A\u0301BC; İstanbul STRAẞE Σίσυφος; 𐐀𐐁𐐂; 東京 🦊",
  ),
  message("message:russian", "В логах ошибка"),
  message("message:latin-short", "Météo école; Școala închisă; Hồ Chí Minh; literal [*?]"),
  message("message:glob-nul", "École\0hidden suffix"),
  message("message:korean", "검색 기능을 고쳐 주세요"),
  message("message:japanese", "がんばって検索します"),
  message("message:chinese", "明天部署到生产环境"),
  message(
    "message:identifiers",
    "Rename ThreadSearchService before shipping; enableFTS today; don't stop and don’t forget",
  ),
  message("message:quotes", 'A"BC quoted; ab abc def; ABC\u0000DEF'),
  message(
    "message:markdown",
    "[Needle docs](https://hidden-address.example/needle-secret) and **needle**",
  ),
  message("message:positions", "positional ".repeat(180) + "tail-end"),
  message("message:negative", "negativeonly needle"),
];
const queries = [
  "needle",
  "NEEDLE",
  "needle",
  "rust",
  "trust",
  "search servi",
  "renam be",
  "renam ab",
  "ab",
  "ne%le",
  "conversation context",
  "ultrathink",
  "attached files",
  "context",
  "fermee éc",
  "fermée éc",
  "fermée ec",
  "ecole fermée",
  "école me",
  "închisă sc",
  "chí ho",
  "fermée ce",
  "resume",
  "aco",
  "abc",
  "ошибка в",
  "ошибка я",
  "검색 기능",
  "がんばって",
  "部署",
  "fts",
  "don't",
  "don’t",
  "東京",
  "🦊",
  'a"bc',
  "negativeonly",
  "hidden-address",
  "needle-secret",
  "positional tail-end",
];
const substring = (
  token,
  accentInsensitive = `*${token.replaceAll("[", "[[]").replaceAll("*", "[*]").replaceAll("?", "[?]")}*`,
) => ({ token, accentInsensitive });
const rawRequests = [
  { match: '"ab" "abc"' },
  { match: '"a""bc"' },
  { match: '"abc" "def"' },
  { match: '"aco"' },
  { match: '"A\u0301BC"' },
  { match: '"𐐨𐐩𐐪"' },
  { match: '"strasse"' },
  { match: '"straße"' },
  { match: '"positional" "tail-end"' },
  { match: '"negativeonly"' },
  { match: null, substrings: [substring("negativeonly")] },
  ...[
    "*",
    "*[ÉE]cole*",
    "*[A-Z]*",
    "*[^a-z]*",
    "*[*][?][]]*",
    "*[*][?]*",
    "*[[]*",
    "*hidden*",
    "*École\0unreachable",
    "*[",
    "*[^]",
    "*[]]*",
    "*?cole*",
    "*[z-a]*",
  ].map((pattern) => ({ match: null, substrings: [substring("__absent_glob_probe__", pattern)] })),
  { match: '"needle"', excludedThreadIds: [main] },
  { match: '"needle"', projectId: otherProjectId },
].map((request) => ({
  match: null,
  substrings: [],
  shortSpellings: [],
  projectId: null,
  excludedThreadIds: [],
  ...request,
}));

async function withServices(kind, filename, fixture, use) {
  if (kind === "native")
    return withNative(executable, fixture, (client) =>
      Effect.gen(function* () {
        const rows = yield* NativeThreadSearch.makeRows;
        const search = yield* ThreadSearch.makeWithRows(rows);
        const writer = yield* NativeProjectionCommit.make;
        return yield* use({
          search,
          rows,
          apply: writer.apply,
          transaction: client.withTransaction,
        });
      }),
    );
  return withSqlite(filename, (sql) =>
    Effect.gen(function* () {
      const module = kind === "upstream" ? oracle.module : ThreadSearch;
      const search = yield* module.ThreadSearch.pipe(Effect.provide(module.layer));
      const projections = yield* (
        kind === "upstream" ? projectionOracle.module : ProjectionStore
      ).ProjectionStoreV2.pipe(
        Effect.provide((kind === "upstream" ? projectionOracle.module : ProjectionStore).layer),
      );
      const rows = {
        hasIndex:
          sql`SELECT 1 FROM sqlite_master WHERE name='orchestration_v2_message_search'`.pipe(
            Effect.map((rows) => rows.length > 0),
          ),
        searchRows: (request) =>
          encodeSearchRequest(request).pipe(
            Effect.flatMap((encoded) => oracle.module.messageHitsQuery(sql, encoded)),
          ),
      };
      return yield* use({
        search,
        rows,
        apply: projections.apply,
        transaction: (effect) => sql.withTransaction(effect),
      });
    }),
  );
}

const exercise = Effect.fn("ThreadSearchFixture.exercise")(function* ({
  search,
  rows,
  apply,
  transaction,
}) {
  const output = [];
  for (const [index, query] of queries.entries()) {
    output.push(normalized(yield* search.search({ query, ...(index === 2 ? { limit: 1 } : {}) })));
  }
  for (const query of [
    "rust",
    "SearchService",
    "acme/search#4202",
    "fix/rust-search",
    "Elsewhere",
  ]) {
    output.push(normalized(yield* search.searchThreads({ query, limit: 5 })));
    output.push(normalized(yield* search.searchThreads({ query, projectId, limit: 5 })));
  }
  for (const query of ["kestrel", "#4101", "acme/search#4202", "fixture unrelated", ""]) {
    for (const projectIds of [undefined, [projectId], [otherProjectId]]) {
      output.push(
        normalized(
          yield* search.searchOlderSettled({
            query,
            before,
            projectIds,
            environmentLabel: "Fixture",
            messageThreadIds: [
              "thread:recent",
              "thread:old-note",
              "thread:old-title",
              "thread:old-note",
              "missing",
            ],
          }),
        ),
      );
    }
  }
  const indexed = yield* rows.hasIndex;
  for (const request of rawRequests) {
    if (!indexed && request.match !== null) continue;
    output.push(normalized(yield* rows.searchRows(request)));
  }
  const update = (text, extra = {}) =>
    apply(message("message:mutable", text, extra, mutationThread));
  for (const fixture of citationFixtures) {
    yield* update(fixture.input);
    const raw = yield* rows.searchRows({
      match: null,
      substrings: [],
      shortSpellings: [],
      projectId: null,
      excludedThreadIds: [],
    });
    const actual = raw.find((row) => row.messageId === "message:mutable");
    NodeAssert.equal(
      actual?.text,
      fixture.expected ?? fixture.input,
      `live citation search text: ${fixture.name}`,
    );
    output.push(normalized(yield* search.search({ query: "quoted words" })));
    output.push(normalized(yield* search.search({ query: "review comment" })));
  }
  yield* update("unique draft", { streaming: true });
  NodeAssert.equal((yield* search.search({ query: "unique" })).matches.length, 0);
  yield* update("unique finished Café 𐐀𐐁𐐂");
  NodeAssert.equal((yield* search.search({ query: "unique" })).matches.length, 1);
  for (const query of ["unique", "café", "𐐨𐐩𐐪"])
    output.push(normalized(yield* search.search({ query })));
  yield* update("unique edited don’t stop");
  NodeAssert.equal((yield* search.search({ query: "finished" })).matches.length, 0);
  output.push(normalized(yield* search.search({ query: "edited don't" })));
  yield* update("unique edited don’t stop", { updatedAt: at(8) });
  output.push(normalized(yield* search.search({ query: "edited" })));
  const failed = yield* Effect.exit(
    transaction(
      Effect.gen(function* () {
        yield* update("rolledback unique");
        NodeAssert.equal((yield* search.search({ query: "rolledback" })).matches.length, 1);
        return yield* Effect.fail("roll back lexical postings");
      }),
    ),
  );
  NodeAssert.ok(Exit.isFailure(failed));
  NodeAssert.equal((yield* search.search({ query: "rolledback" })).matches.length, 0);
  output.push(normalized(yield* search.search({ query: "edited" })));
  yield* update(ATTACHMENT_ONLY_BOOTSTRAP_PROMPT);
  NodeAssert.equal((yield* search.search({ query: "unique" })).matches.length, 0);
  yield* apply(message("message:positions", "replacement only"));
  NodeAssert.equal(
    (yield* search.search({ query: "positional" })).matches.length,
    0,
    "Editing an imported row atomically shadows all its old positions",
  );
  for (const length of [40000, 400000]) {
    yield* update("a".repeat(length));
    const repeated = yield* search.search({ query: "aaa" });
    NodeAssert.equal(
      repeated.matches.length,
      1,
      "Long repeated model output remains writable and searchable",
    );
    output.push(normalized(repeated));
  }
  // More than the native transaction's 65,536 record cap in distinct terms.
  // Ordinary per-term indexes still fail this case; bounded bucket fan-out must not.
  const unicodeText = Array.from({ length: 70000 }, (_, index) =>
    String.fromCodePoint(0x20000 + index),
  ).join("");
  const unicodeNeedle = Array.from({ length: 3 }, (_, index) =>
    String.fromCodePoint(0x20000 + 35000 + index),
  ).join("");
  yield* update(unicodeText);
  const unicodeRequest = {
    match: indexed ? `"${unicodeNeedle}"` : null,
    substrings: indexed ? [] : [substring(unicodeNeedle)],
    shortSpellings: [],
    projectId: null,
    excludedThreadIds: [],
  };
  const unicodeHits = yield* rows.searchRows(unicodeRequest);
  NodeAssert.deepEqual(
    unicodeHits.map((row) => row.messageId),
    ["message:mutable"],
  );
  output.push(normalized(unicodeHits));
  const failedLarge = yield* Effect.exit(
    transaction(
      Effect.gen(function* () {
        yield* update("rolledback high cardinality");
        return yield* Effect.fail("roll back bucket replacement");
      }),
    ),
  );
  NodeAssert.ok(Exit.isFailure(failedLarge));
  NodeAssert.deepEqual(normalized(yield* rows.searchRows(unicodeRequest)), normalized(unicodeHits));
  const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789";
  let random = 0x12345678;
  const dense = Array.from({ length: 1024 * 1024 }, () => {
    random = (Math.imul(random, 1664525) + 1013904223) >>> 0;
    return alphabet[(random >>> 16) % alphabet.length];
  }).join("");
  yield* update(`dense-entropy ${dense}`);
  const denseHits = yield* search.search({ query: "dense-entropy" });
  NodeAssert.equal(
    denseHits.matches.length,
    1,
    "One MiB of nonconsecutive dense positions stays within the atomic mutation budget",
  );
  output.push(normalized(denseHits));
  yield* update("final small message");
  NodeAssert.deepEqual(
    yield* rows.searchRows(unicodeRequest),
    [],
    "Bucket-to-direct replacement deletes the previous live index",
  );
  output.push(normalized(yield* search.search({ query: "context" })));
  return output;
});

const fixtures = [];
let sourceHash;
try {
  for (const indexed of [true, false]) {
    const fixture = await createFixture();
    fixtures.push(fixture);
    const source = NodePath.join(fixture.root, "source.sqlite");
    await createMigratedSqlite(source, (sql) =>
      Effect.gen(function* () {
        const projects = yield* ProjectStore.ProjectStoreV2.pipe(
          Effect.provide(ProjectStore.layer),
        );
        const projections = yield* projectionOracle.module.ProjectionStoreV2.pipe(
          Effect.provide(projectionOracle.module.layer),
        );
        yield* projects.apply(projectEvent(projectId, "Search Project"));
        yield* projects.apply(projectEvent(otherProjectId, "Elsewhere"));
        for (const next of seedEvents) yield* projections.apply(next);
        const [negative] =
          yield* sql`SELECT rowid FROM orchestration_v2_projection_messages WHERE message_id='message:negative'`;
        yield* sql`DELETE FROM orchestration_v2_message_search WHERE rowid=${negative.rowid}`;
        yield* sql`UPDATE orchestration_v2_projection_messages SET rowid=-7 WHERE message_id='message:negative'`;
        yield* sql`INSERT INTO orchestration_v2_message_search(rowid,text) VALUES(-7,'negativeonly needle')`;
        // A legacy index can contain placeholders and orphan rows. The import must preserve it,
        // not silently rebuild the index using the current trigger predicate.
        const [placeholder] =
          yield* sql`SELECT rowid FROM orchestration_v2_projection_messages WHERE message_id='message:placeholder'`;
        yield* sql`INSERT INTO orchestration_v2_message_search(rowid,text) VALUES(${placeholder.rowid},${ATTACHMENT_ONLY_BOOTSTRAP_PROMPT})`;
        yield* sql`INSERT INTO orchestration_v2_message_search(rowid,text) VALUES(100000,'orphan lexical needle')`;
        if (!indexed) {
          yield* sql`DROP TRIGGER orchestration_v2_message_search_insert`;
          yield* sql`DROP TRIGGER orchestration_v2_message_search_update`;
          yield* sql`DROP TRIGGER orchestration_v2_message_search_delete`;
          yield* sql`DROP TABLE orchestration_v2_message_search`;
        }
      }),
    );
    const upstream = NodePath.join(fixture.root, "upstream.sqlite");
    const retained = NodePath.join(fixture.root, "retained.sqlite");
    await NodeFSP.copyFile(source, upstream);
    await NodeFSP.copyFile(source, retained);
    sourceHash = hash(await NodeFSP.readFile(source));
    const exported = NodePath.join(fixture.root, "source.ndjson");
    exportSqliteState({ source, destination: exported });
    nativeRequest(executable, fixture, "initialize", {
      operation: "import_sqlite_application",
      path: exported,
    });
    const expected = await withServices("upstream", upstream, fixture, exercise);
    NodeAssert.deepEqual(
      await withServices("sqlite", retained, fixture, exercise),
      expected,
      `Retained search matches immutable source (indexed=${indexed})`,
    );
    NodeAssert.deepEqual(
      await withServices("native", source, fixture, exercise),
      expected,
      `Native search matches immutable source (indexed=${indexed})`,
    );
    await withServices("native", source, fixture, ({ search }) =>
      Effect.gen(function* () {
        NodeAssert.deepEqual(
          normalized(yield* search.search({ query: "context" })),
          expected.at(-1),
        );
      }),
    );
    NodeAssert.equal(
      hash(await NodeFSP.readFile(source)),
      sourceHash,
      "Original SQLite source stays untouched",
    );
    checks.push(
      `Immutable upstream parity for message, title, project, PR and older-settled ranking (index=${indexed})`,
    );
  }
  {
    // Contentless FTS can contain older terms after an insert without a delete.
    // A later DELETE removes the latest insertion's terms, not those older
    // surviving terms. Exercise this through the actual production writer.
    const fixture = await createFixture();
    fixtures.push(fixture);
    const source = NodePath.join(fixture.root, "stale-transition-source.sqlite");
    const upstream = NodePath.join(fixture.root, "stale-transition-upstream.sqlite");
    const retained = NodePath.join(fixture.root, "stale-transition-retained.sqlite");
    await createMigratedSqlite(source, (sql) =>
      Effect.gen(function* () {
        const projects = yield* ProjectStore.ProjectStoreV2.pipe(
          Effect.provide(ProjectStore.layer),
        );
        const projections = yield* projectionOracle.module.ProjectionStoreV2.pipe(
          Effect.provide(projectionOracle.module.layer),
        );
        yield* projects.apply(projectEvent(projectId, "Stale transitions"));
        yield* projections.apply(thread(main));
        yield* projections.apply(message("message:stale", "unindexed draft", { streaming: true }));
        const [stale] =
          yield* sql`SELECT rowid FROM orchestration_v2_projection_messages WHERE message_id='message:stale'`;
        NodeAssert.equal(stale.rowid, 1);
        yield* sql`INSERT INTO orchestration_v2_message_search(rowid,text) VALUES(1,'oldtoken overlap')`;
        yield* sql`INSERT INTO orchestration_v2_message_search(rowid,text) VALUES(2,'orphanword overlap')`;
      }),
    );
    await NodeFSP.copyFile(source, upstream);
    await NodeFSP.copyFile(source, retained);
    const sourceHashBefore = hash(await NodeFSP.readFile(source));
    const exported = NodePath.join(fixture.root, "stale-transition-source.ndjson");
    exportSqliteState({ source, destination: exported });
    nativeRequest(executable, fixture, "initialize", {
      operation: "import_sqlite_application",
      path: exported,
    });
    const requests = [
      "oldtoken",
      "completed",
      "overlap",
      "oldtoken overlap",
      "completed overlap",
      "orphanword",
      "newborn",
      "orphanword overlap",
      "newborn overlap",
    ].map((text) => ({
      match: `"${text}"`,
      substrings: [],
      shortSpellings: [],
      projectId: null,
      excludedThreadIds: [],
    }));
    const transitions = Effect.fn("ThreadSearchFixture.staleTransitions")(function* ({
      rows,
      apply,
    }) {
      const output = [];
      const read = Effect.fn("ThreadSearchFixture.staleTransitions.read")(function* () {
        const result = [];
        for (const request of requests) result.push(normalized(yield* rows.searchRows(request)));
        return result;
      });
      output.push(yield* read());
      yield* apply(message("message:stale", "completed overlap"));
      output.push(yield* read());
      yield* apply(message("message:reuse", "newborn overlap"));
      output.push(yield* read());
      yield* apply(message("message:stale", "draft again", { streaming: true }));
      output.push(yield* read());
      yield* apply(message("message:stale", "finished final"));
      yield* apply(message("message:reuse", "final replacement"));
      output.push(yield* read());
      return output;
    });
    const expected = await withServices("upstream", upstream, fixture, transitions);
    NodeAssert.equal(
      expected[1][0][0].messageId,
      "message:stale",
      "Insert without delete retains missing old terms",
    );
    NodeAssert.deepEqual(
      expected[1][3],
      [],
      "Overlapping term positions come from the latest insertion",
    );
    NodeAssert.equal(
      expected[2][5][0].messageId,
      "message:reuse",
      "New messages can reuse an orphan FTS rowid",
    );
    NodeAssert.equal(
      expected[3][0][0].messageId,
      "message:stale",
      "Delete retains terms absent from the latest insertion",
    );
    NodeAssert.deepEqual(expected[3][1], [], "Delete removes latest insertion terms");
    NodeAssert.equal(expected[4][0][0].messageId, "message:stale");
    NodeAssert.equal(expected[4][5][0].messageId, "message:reuse");
    NodeAssert.deepEqual(await withServices("sqlite", retained, fixture, transitions), expected);
    NodeAssert.deepEqual(
      await withServices("native", source, fixture, transitions),
      expected,
      "All 45 stale insertion, subsequent delete and orphan-rowid reuse queries match immutable SQLite",
    );
    NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceHashBefore);
    checks.push(
      "Insert-without-delete, overlapping positions, subsequent delete and orphan-rowid reuse preserve surviving historical terms across 45 real queries",
    );
  }
  {
    // Instance vocabulary alone cannot identify which imported insertion owns
    // the next contentless DELETE. Include an already-duplicated source, a
    // latest empty-token generation, and an orphan with no current generation.
    const fixture = await createFixture();
    fixtures.push(fixture);
    const source = NodePath.join(fixture.root, "delete-witness-source.sqlite");
    const upstream = NodePath.join(fixture.root, "delete-witness-upstream.sqlite");
    const retained = NodePath.join(fixture.root, "delete-witness-retained.sqlite");
    await createMigratedSqlite(source, (sql) =>
      Effect.gen(function* () {
        const projects = yield* ProjectStore.ProjectStoreV2.pipe(
          Effect.provide(ProjectStore.layer),
        );
        const projections = yield* projectionOracle.module.ProjectionStoreV2.pipe(
          Effect.provide(projectionOracle.module.layer),
        );
        yield* projects.apply(projectEvent(projectId, "Source delete witnesses"));
        yield* projections.apply(thread(main));
        for (const [index, text] of ["completed overlap", "xy", "completed overlap"].entries()) {
          yield* projections.apply(message(`message:witness:${index}`, text));
          const rowid = index + 1;
          yield* sql`DELETE FROM orchestration_v2_message_search WHERE rowid=${rowid}`;
          yield* sql`INSERT INTO orchestration_v2_message_search(rowid,text) VALUES(${rowid},'oldtoken overlap')`;
          yield* sql`INSERT INTO orchestration_v2_message_search(rowid,text) VALUES(${rowid},${text})`;
          if (index === 2)
            yield* sql`DELETE FROM orchestration_v2_message_search WHERE rowid=${rowid}`;
        }
      }),
    );
    await NodeFSP.copyFile(source, upstream);
    await NodeFSP.copyFile(source, retained);
    const beforeHash = hash(await NodeFSP.readFile(source));
    const exported = NodePath.join(fixture.root, "delete-witness-source.ndjson");
    exportSqliteState({ source, destination: exported });
    nativeRequest(executable, fixture, "initialize", {
      operation: "import_sqlite_application",
      path: exported,
    });
    const transitions = Effect.fn("ThreadSearchFixture.deleteWitnessTransitions")(function* ({
      rows,
      apply,
    }) {
      const output = [];
      const read = Effect.fn("ThreadSearchFixture.deleteWitnessTransitions.read")(function* () {
        const results = [];
        for (const text of [
          "oldtoken",
          "completed",
          "overlap",
          "oldtoken overlap",
          "replacement",
        ]) {
          results.push(
            normalized(
              yield* rows.searchRows({
                match: `"${text}"`,
                substrings: [],
                shortSpellings: [],
                projectId: null,
                excludedThreadIds: [],
              }),
            ),
          );
        }
        return results;
      });
      output.push(yield* read());
      for (let index = 0; index < 3; index++) {
        yield* apply(message(`message:witness:${index}`, "draft", { streaming: true }));
      }
      output.push(yield* read());
      for (let index = 0; index < 3; index++) {
        yield* apply(message(`message:witness:${index}`, "replacement"));
      }
      output.push(yield* read());
      for (let index = 0; index < 3; index++) {
        yield* apply(message(`message:witness:${index}`, "another draft", { streaming: true }));
      }
      output.push(yield* read());
      return output;
    });
    const expected = await withServices("upstream", upstream, fixture, transitions);
    NodeAssert.equal(
      expected[1][0].length,
      3,
      "First DELETE retains all old-only source generations",
    );
    NodeAssert.equal(
      expected[1][1].length,
      0,
      "First DELETE removes the current nonempty source generation",
    );
    NodeAssert.equal(
      expected[1][2].length,
      1,
      "An empty latest source generation leaves prior overlap positions intact",
    );
    NodeAssert.equal(
      expected[3][0].length,
      3,
      "Later native DELETE preserves witnessed historical source terms",
    );
    NodeAssert.equal(expected[3][4].length, 0, "Later native DELETE removes its latest insertion");
    NodeAssert.deepEqual(await withServices("sqlite", retained, fixture, transitions), expected);
    NodeAssert.deepEqual(await withServices("native", source, fixture, transitions), expected);
    NodeAssert.equal(
      hash(await NodeFSP.readFile(source)),
      beforeHash,
      "Obtaining delete witnesses leaves original SQLite bytes unchanged",
    );
    checks.push(
      "Already-duplicated source, empty-token current generation and orphan first DELETE match immutable SQLite across 20 queries",
    );
  }
  {
    const fixture = await createFixture();
    fixtures.push(fixture);
    const source = NodePath.join(fixture.root, "bounded-source.sqlite");
    const unicodeSource = await NodeFSP.readFile(
      new URL("../apps/server/src/persistence/redb_search_unicode.rs", import.meta.url),
      "utf8",
    );
    const foldRules = unicodeSource.match(/const FOLD_RULES[^=]*= &\[([\s\S]*?)\];/)[1];
    const diacriticRules = unicodeSource.match(/const DIACRITIC_RANGES[^=]*= &\[([\s\S]*?)\];/)[1];
    // These choose coverage, not expected values: only real SQLite's positional
    // index decides the oracle. Both ends and immediate neighbors of every
    // translated rule are exercised, with unique markers preventing collisions.
    const points = new Set([128, 65534, 65535, 66559, 66560, 66599, 66600, 0x1f98a, 0x10ffff]);
    for (const [, begin, , count] of foldRules.matchAll(/\((\d+), (\d+), (\d+)\)/g)) {
      for (const point of [+begin - 1, +begin, +begin + 1, +begin + +count - 1, +begin + +count])
        points.add(point);
    }
    for (const rule of diacriticRules.match(/\d+/g).map(Number)) {
      const first = rule >> 3;
      for (const point of [first - 1, first, first + (rule & 7), first + (rule & 7) + 1])
        points.add(point);
    }
    const words = [...points]
      .filter((point) => point >= 32 && !(point >= 0xd800 && point <= 0xdfff))
      .sort((a, b) => a - b)
      .map(
        (point) => `u${point.toString(16)}v${String.fromCodePoint(point)}w${point.toString(16)}z`,
      );
    const groups = [];
    for (let offset = 0; offset < words.length; offset += 32)
      groups.push(words.slice(offset, offset + 32));
    await createMigratedSqlite(source, (sql) =>
      Effect.gen(function* () {
        const projects = yield* ProjectStore.ProjectStoreV2.pipe(
          Effect.provide(ProjectStore.layer),
        );
        const projections = yield* projectionOracle.module.ProjectionStoreV2.pipe(
          Effect.provide(projectionOracle.module.layer),
        );
        yield* projects.apply(projectEvent(projectId, "Bounds"));
        yield* projections.apply(thread(main));
        yield* projections.apply(thread(assistant));
        const insert = (rowid, id, text, owner = main) => sql`
        INSERT INTO orchestration_v2_projection_messages
          (rowid,message_id,thread_id,role,streaming,created_at,updated_at,payload_json)
        VALUES (${rowid},${id},${owner},'user',0,${fixedTime},${fixedTime},${JSON.stringify({ text })})
      `;
        yield* insert(10, "boundary:excluded", "qqq");
        yield* insert(11, "boundary:included", "www");
        yield* insert(12, "cap:eligible", "hhh", assistant);
        yield* sql`
        WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<10000)
        INSERT INTO orchestration_v2_message_search(rowid,text)
        SELECT 1000000+i, 'qqq' FROM n
      `;
        yield* sql`
        WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<9999)
        INSERT INTO orchestration_v2_message_search(rowid,text)
        SELECT 2000000+i, 'www' FROM n
      `;
        yield* sql`
        WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<5000)
        INSERT INTO orchestration_v2_message_search(rowid,text)
        SELECT 3000000+i, 'needle' FROM n
      `;
        yield* sql`
        WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<2005)
        INSERT INTO orchestration_v2_projection_messages
          (rowid,message_id,thread_id,role,streaming,created_at,updated_at,payload_json)
        SELECT 201000+i,'cap:'||i,${main},'user',0,${fixedTime},${fixedTime},' {"text":"hhh"}' FROM n
      `;
        yield* insert(220000, "scan:excluded", "scanwindow");
        yield* insert(220001, "scan:included", "scanwindow");
        for (const [index, group] of groups.entries()) {
          yield* insert(230000 + index, `unicode:${index}`, group.join(" "));
        }
        yield* insert(240000, "scan:newest", "newest sentinel");
      }),
    );
    const sourceHashBefore = hash(await NodeFSP.readFile(source));
    const exported = NodePath.join(fixture.root, "bounded-source.ndjson");
    exportSqliteState({ source, destination: exported });
    nativeRequest(executable, fixture, "initialize", {
      operation: "import_sqlite_application",
      path: exported,
    });
    const query = (match, extra = {}) => ({
      match,
      substrings: [],
      shortSpellings: [],
      projectId: null,
      excludedThreadIds: [],
      ...extra,
    });
    const boundedRequests = [
      query('"qqq"'),
      query('"www"'),
      query('"hhh"'),
      query('"hhh"', { excludedThreadIds: [main] }),
      query(null, { substrings: [substring("scanwindow")] }),
      query('"needle"'),
      ...groups.map((group) =>
        query(group.map((word) => `"${word.replaceAll('"', '""')}"`).join(" ")),
      ),
    ];
    const evaluate = Effect.fn("ThreadSearchFixture.evaluateBounds")(function* ({ rows }) {
      const output = [];
      for (const request of boundedRequests)
        output.push(normalized(yield* rows.searchRows(request)));
      return output;
    });
    const expected = await withServices("upstream", source, fixture, evaluate);
    NodeAssert.deepEqual(
      expected[0],
      [],
      "10,000 orphan candidates consume the original index budget",
    );
    NodeAssert.deepEqual(
      expected[1].map((row) => row.messageId),
      ["boundary:included"],
      "The 10,000th candidate itself is included",
    );
    NodeAssert.equal(expected[2].length, 2000, "Raw hit limit is exactly 2,000");
    NodeAssert.deepEqual(
      expected[3].map((row) => row.messageId),
      ["cap:eligible"],
      "Thread exclusions precede the hit budget",
    );
    NodeAssert.deepEqual(
      expected[4].map((row) => row.messageId),
      ["scan:included"],
      "Fallback scans use rowid distance, not row count",
    );
    NodeAssert.deepEqual(
      expected[5],
      [],
      "A multi-trigram query can consume thousands of orphan matches",
    );
    for (const [index, result] of expected.slice(6).entries()) {
      NodeAssert.deepEqual(
        result.map((row) => row.messageId),
        [`unicode:${index}`],
      );
    }
    NodeAssert.deepEqual(
      await withServices("native", source, fixture, evaluate),
      expected,
      "Native bounded candidates and every Unicode fold-rule boundary match SQLite",
    );
    NodeAssert.equal(hash(await NodeFSP.readFile(source)), sourceHashBefore);
    checks.push(
      "Exact 10,000 candidate, 2,000 filtered-hit and 20,000 source-rowid-distance bounds, including orphan postings and numeric gaps",
    );
    checks.push(
      `Positional phrase parity at ${words.length} Unicode fold/diacritic-rule boundaries and neighboring codepoints`,
    );
  }
  checks.push(
    "Citation URLs, dot-segment normalization, encoded IDs, invalid UTF-8, fragments, offsets and optional comments match current upstream before and after live indexing",
    "Exact trigram phrases, quote escaping, Unicode legacy case/diacritic folding, CJK scans and short non-ASCII spellings",
    "Current substring token/GLOB pairs preserve Latin accent narrowing, escaped wildcards, bracket classes, ranges, negation and embedded NUL behavior against SQLite",
    "Signed source rowids, negative cutoff, stale placeholder and orphan index entries preserve candidate membership",
    "Streaming completion, edited text, unchanged-text updates, repeated token positions and attachment placeholders preserve trigger semantics",
    "40k/400k repeated output, 70k distinct Unicode trigrams and 1MiB dense nonconsecutive positions remain atomic through edits and compact-layout changes",
    "A four-trigram query over 5,000 orphan documents preserves the immutable read snapshot without accumulating writable conflict observations",
    "Transaction rollback and helper reopen preserve native lexical postings without querying SQLite",
  );
  await finishProof({
    executable,
    fixture: fixtures[0],
    proofPath,
    checks,
    oracles: [oracle.source, projectionOracle.source, citationOracle.source],
    sourceHash,
    sourcePaths: [
      "apps/server/src/orchestration-v2/ThreadSearch.ts",
      "apps/server/src/orchestration-v2/ThreadSearchRows.ts",
      "apps/server/src/orchestration-v2/NativeThreadSearch.ts",
      "apps/server/src/persistence/redb_thread_search.rs",
      "apps/server/src/persistence/redb_message_search_text.rs",
      "apps/server/src/persistence/fixtures/message-search-text.json",
      "apps/server/src/persistence/redb_search_unicode.rs",
      "apps/server/src/persistence/redb_search_positions.rs",
      "apps/server/src/persistence/redb_projection_commit.rs",
      "tools/check-redb-thread-search.mjs",
    ],
  });
} finally {
  for (const fixture of fixtures) await fixture.close();
}
