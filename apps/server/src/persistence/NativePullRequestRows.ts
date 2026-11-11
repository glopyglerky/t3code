/** Native omdb counterparts for the existing pull-request persistence contracts. */
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { PullRequestReviewGuide } from "@t3tools/contracts";

import { PersistenceDecodeError, PersistenceSqlError } from "./Errors.ts";
import {
  PullRequestFileViewedMark,
  PullRequestFilesViewedRepository,
} from "./PullRequestFilesViewed.ts";
import { PullRequestReviewGuideRepository } from "./PullRequestReviewGuides.ts";
import { RedbClient } from "./RedbClient.ts";

const Written = Schema.Struct({ written: Schema.Literal(true) });
const ViewedPage = Schema.Struct({
  files: Schema.Array(PullRequestFileViewedMark),
  truncated: Schema.Boolean,
});
const GuideJson = Schema.fromJsonString(PullRequestReviewGuide);

export const makeGuide = Effect.gen(function* () {
  const client = yield* RedbClient;
  return PullRequestReviewGuideRepository.of({
    get: Effect.fn("NativePullRequestReviewGuide.get")((key) =>
      client.request("application_guide_get", { key }, Schema.NullOr(Schema.String)).pipe(
        Effect.flatMap((json) =>
          json === null
            ? Effect.succeed(Option.none<PullRequestReviewGuide>())
            : Schema.decodeUnknownEffect(GuideJson)(json).pipe(Effect.map(Option.some)),
        ),
        Effect.mapError((cause) => Schema.isSchemaError(cause)
          ? PersistenceDecodeError.fromSchemaError("PullRequestReviewGuide", cause)
          : new PersistenceSqlError({ operation: "getPullRequestReviewGuide", cause })),
      ),
    ),
    put: Effect.fn("NativePullRequestReviewGuide.put")((key, guide) =>
      Schema.encodeEffect(GuideJson)(guide).pipe(
        Effect.mapError((cause) => PersistenceDecodeError.fromSchemaError("PullRequestReviewGuide", cause)),
        Effect.flatMap((guide_json) => client.request("application_guide_put", { key, guide_json }, Written).pipe(
          Effect.asVoid,
          Effect.mapError((cause) => new PersistenceSqlError({ operation: "putPullRequestReviewGuide", cause })),
        )),
      ),
    ),
  });
});

export const layerGuide = Layer.effect(PullRequestReviewGuideRepository, makeGuide);

export const makeViewedFiles = Effect.gen(function* () {
  const client = yield* RedbClient;
  while (!(yield* client.prepareViewedFiles).prepared) yield* Effect.yieldNow;
  return PullRequestFilesViewedRepository.of({
    list: Effect.fn("NativePullRequestFilesViewed.list")((scope) =>
      client.request("application_viewed_files_list", { scope }, ViewedPage).pipe(
        Effect.mapError((cause) => Schema.isSchemaError(cause)
          ? PersistenceDecodeError.fromSchemaError("PullRequestFileViewed", cause)
          : new PersistenceSqlError({ operation: "listPullRequestFilesViewed", cause })),
      ),
    ),
    set: Effect.fn("NativePullRequestFilesViewed.set")(({ files, viewedAt, ...scope }) =>
      client.request("application_viewed_files_set", {
        scope, files, viewed_at: viewedAt,
      }, Written).pipe(
        Effect.asVoid,
        Effect.mapError((cause) => new PersistenceSqlError({ operation: "setPullRequestFilesViewed", cause })),
      ),
    ),
  });
});

export const layerViewedFiles = Layer.effect(PullRequestFilesViewedRepository, makeViewedFiles);
