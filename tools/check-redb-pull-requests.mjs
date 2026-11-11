#!/usr/bin/env node
import * as Assert from "node:assert/strict";
import * as FS from "node:fs/promises";
import * as Path from "node:path";
import * as Sqlite from "node:sqlite";
import * as Contracts from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlSchema from "effect/sql/SqlSchema";
import * as Errors from "../apps/server/src/persistence/Errors.ts";
import * as Guides from "../apps/server/src/persistence/PullRequestReviewGuides.ts";
import * as Viewed from "../apps/server/src/persistence/PullRequestFilesViewed.ts";
import * as Native from "../apps/server/src/persistence/NativePullRequestRows.ts";
import { createFixture } from "./database-fixture.mjs";
import { exportSqliteState } from "./export-sqlite-state.mjs";
import {
  createMigratedSqlite,
  finishProof,
  hash,
  loadUpstream,
  nativeRequest,
  normalized,
  withNative,
  withSqlite,
} from "./redb-domain-fixture.mjs";

const [executable, proofPath] = process.argv.slice(2);
Assert.ok(executable && Path.isAbsolute(executable));
const imports = {
  ...Contracts,
  ...Errors,
  Context,
  Effect,
  Layer,
  Option,
  Schema,
  SqlClient,
  SqlSchema,
  NodeSqliteClient,
};
const guideOracle = loadUpstream(
  "apps/server/src/persistence/PullRequestReviewGuides.ts",
  imports,
  ["make"],
);
const viewedOracle = loadUpstream(
  "apps/server/src/persistence/PullRequestFilesViewed.ts",
  imports,
  ["make"],
);
const fixture = await createFixture();
const checks = [];
const at = "2026-11-10T00:00:00.000Z";
const key = { host: "github.com", repository: "example/project", number: 17 };
const scope = { ...key, provider: "github", viewer: "alice" };
const guide = {
  headSha: "fixture-head",
  generatedAt: at,
  modelSelection: { instanceId: "codex", model: "fixture-model" },
  stops: [],
};

const failureTag = (effect) =>
  effect.pipe(
    Effect.map(() => "unexpected-success"),
    Effect.catch((error) => Effect.succeed(error._tag)),
  );

async function withRepositories(kind, filename, use) {
  if (kind === "native")
    return withNative(executable, fixture, (client) =>
      Effect.gen(function* () {
        const guides = yield* Native.makeGuide;
        const viewed = yield* Native.makeViewedFiles;
        return yield* use({ guides, viewed, transaction: client.withTransaction });
      }),
    );
  return withSqlite(filename, (sql) =>
    Effect.gen(function* () {
      const guides =
        kind === "upstream"
          ? yield* guideOracle.module.make
          : yield* Guides.PullRequestReviewGuideRepository.pipe(Effect.provide(Guides.layer));
      const viewed =
        kind === "upstream"
          ? yield* viewedOracle.module.make
          : yield* Viewed.PullRequestFilesViewedRepository.pipe(Effect.provide(Viewed.layer));
      return yield* use({ guides, viewed, transaction: sql.withTransaction });
    }),
  );
}

function exercise({ guides, viewed, transaction }) {
  return Effect.gen(function* () {
    const result = {};
    result.imported = yield* guides.get(key);
    Assert.equal(result.imported.value.headSha, "imported");
    result.initialMarks = yield* viewed.list(scope);
    Assert.deepEqual(result.initialMarks, {
      files: [{ path: "imported.rs", revision: null }],
      truncated: false,
    });
    yield* transaction(
      Effect.gen(function* () {
        yield* guides.put(key, guide);
        yield* viewed.set({
          ...scope,
          viewedAt: at,
          files: [
            { path: "z.rs", revision: "rev-z", viewed: true },
            { path: "a.rs", revision: "", viewed: true },
            { path: "🦊.rs", revision: null, viewed: true },
            { path: "z.rs", revision: "replacement", viewed: true },
            { path: "absent", revision: null, viewed: false },
          ],
        });
        yield* transaction(
          Effect.gen(function* () {
            yield* guides.put(key, { ...guide, headSha: "discarded" });
            yield* viewed.set({
              ...scope,
              viewedAt: at,
              files: [{ path: "a.rs", revision: null, viewed: false }],
            });
            return yield* Effect.fail("rollback");
          }),
        ).pipe(Effect.catch(() => Effect.void));
        Assert.equal((yield* guides.get(key)).value.headSha, guide.headSha);
        result.nested = yield* viewed.list(scope);
        Assert.deepEqual(
          result.nested.files.map((file) => file.path),
          ["a.rs", "imported.rs", "z.rs", "🦊.rs"],
        );
      }),
    );
    yield* transaction(
      Effect.gen(function* () {
        yield* guides.put({ ...key, number: 18 }, guide);
        yield* viewed.set({
          ...scope,
          viewer: "bob",
          viewedAt: at,
          files: [{ path: "private", revision: null, viewed: true }],
        });
        return yield* Effect.fail("rollback");
      }),
    ).pipe(Effect.catch(() => Effect.void));
    Assert.ok(Option.isNone(yield* guides.get({ ...key, number: 18 })));
    result.otherViewer = yield* viewed.list({ ...scope, viewer: "bob" });
    Assert.deepEqual(result.otherViewer.files, []);
    yield* viewed.set({
      ...scope,
      viewedAt: at,
      files: [{ path: "imported.rs", revision: null, viewed: false }],
    });
    result.afterDelete = yield* viewed.list(scope);
    const many = { ...scope, number: 500 };
    yield* viewed.set({
      ...many,
      viewedAt: at,
      files: Array.from({ length: 501 }, (_, i) => ({
        path: `file-${String(i).padStart(4, "0")}`,
        revision: null,
        viewed: true,
      })),
    });
    result.many = yield* viewed.list(many);
    Assert.equal(result.many.files.length, 500);
    Assert.equal(result.many.truncated, true);
    result.invalidGuide = yield* failureTag(guides.get({ ...key, number: 1.5 }));
    result.invalidScope = yield* failureTag(viewed.list({ ...scope, number: 1.5 }));
    Assert.equal(result.invalidGuide, "PersistenceDecodeError");
    Assert.equal(result.invalidScope, "PersistenceDecodeError");
    result.finalGuide = yield* guides.get(key);
    return normalized(result);
  });
}

try {
  const source = Path.join(fixture.root, "source.sqlite");
  await createMigratedSqlite(source);
  const database = new Sqlite.DatabaseSync(source);
  try {
    database
      .prepare("INSERT INTO pull_request_review_guides VALUES (?, ?, ?, ?)")
      .run(key.host, key.repository, key.number, JSON.stringify({ ...guide, headSha: "imported" }));
    database
      .prepare("INSERT INTO pull_request_files_viewed VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
      .run(
        scope.provider,
        scope.host,
        scope.repository,
        scope.number,
        scope.viewer,
        "imported.rs",
        null,
        at,
      );
  } finally {
    database.close();
  }
  const sourceHash = hash(await FS.readFile(source));
  const upstreamPath = Path.join(fixture.root, "upstream.sqlite");
  const retainedPath = Path.join(fixture.root, "retained.sqlite");
  await FS.copyFile(source, upstreamPath);
  await FS.copyFile(source, retainedPath);
  const exported = Path.join(fixture.root, "source.ndjson");
  exportSqliteState({ source, destination: exported });
  nativeRequest(executable, fixture, "initialize", {
    operation: "import_sqlite_application",
    path: exported,
  });
  const expected = await withRepositories("upstream", upstreamPath, exercise);
  Assert.deepEqual(await withRepositories("sqlite", retainedPath, exercise), expected);
  Assert.deepEqual(await withRepositories("native", source, exercise), expected);
  await withRepositories("native", source, ({ guides, viewed }) =>
    Effect.gen(function* () {
      Assert.deepEqual(normalized(yield* guides.get(key)), expected.finalGuide);
      Assert.deepEqual(normalized(yield* viewed.list(scope)), expected.afterDelete);
    }),
  );
  Assert.equal(hash(await FS.readFile(source)), sourceHash);
  checks.push(
    "All four repository methods match immutable upstream and retained SQLite",
    "Imported rows, exact empty/null revisions, sorted paths, duplicate updates and 500-row truncation survive native preparation",
    "Guide and viewed-file mutations share root and nested rollback, isolate viewer scopes and survive reopen",
    "Request schema rejection retains PersistenceDecodeError and migration never modifies its source",
  );
  await finishProof({
    executable,
    fixture,
    proofPath,
    checks,
    sourceHash,
    oracles: [guideOracle.source, viewedOracle.source],
    sourcePaths: [
      "apps/server/src/persistence/NativePullRequestRows.ts",
      "apps/server/src/pullRequest/redb_pull_request_rows.rs",
    ],
  });
} finally {
  await fixture.close();
}
