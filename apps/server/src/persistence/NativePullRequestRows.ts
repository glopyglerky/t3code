/** Native omdb counterparts for the existing pull-request persistence contracts. */
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { PullRequestReviewGuide, ThreadPullRequestKey } from "@t3tools/contracts";

import { PersistenceDecodeError, PersistenceSqlError } from "./Errors.ts";
import {
  PullRequestFileViewedMark,
  PullRequestFilesViewedRepository,
  PullRequestFilesViewedScope,
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
      Schema.encodeEffect(ThreadPullRequestKey)(key).pipe(
        Effect.flatMap((key) =>
          client.request("application_guide_get", { key }, Schema.NullOr(Schema.String)),
        ),
        Effect.flatMap((json) =>
          json === null
            ? Effect.succeed(Option.none<PullRequestReviewGuide>())
            : Schema.decodeEffect(GuideJson)(json).pipe(Effect.asSome),
        ),
        Effect.mapError((cause) =>
          Schema.isSchemaError(cause)
            ? PersistenceDecodeError.fromSchemaError("PullRequestReviewGuide", cause)
            : new PersistenceSqlError({ operation: "getPullRequestReviewGuide", cause }),
        ),
      ),
    ),
    put: Effect.fn("NativePullRequestReviewGuide.put")((key, guide) =>
      Schema.encodeEffect(GuideJson)(guide).pipe(
        Effect.mapError((cause) =>
          PersistenceDecodeError.fromSchemaError("PullRequestReviewGuide", cause),
        ),
        Effect.flatMap((guide_json) =>
          client
            .request(
              "application_guide_put",
              {
                key: { host: key.host, repository: key.repository, number: key.number },
                guide_json,
              },
              Written,
            )
            .pipe(
              Effect.asVoid,
              Effect.mapError(
                (cause) =>
                  new PersistenceSqlError({ operation: "putPullRequestReviewGuide", cause }),
              ),
            ),
        ),
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
      Schema.encodeEffect(PullRequestFilesViewedScope)(scope).pipe(
        Effect.flatMap((scope) =>
          client.request("application_viewed_files_list", { scope }, ViewedPage),
        ),
        Effect.mapError((cause) =>
          Schema.isSchemaError(cause)
            ? PersistenceDecodeError.fromSchemaError("PullRequestFileViewed", cause)
            : new PersistenceSqlError({ operation: "listPullRequestFilesViewed", cause }),
        ),
      ),
    ),
    set: Effect.fn("NativePullRequestFilesViewed.set")(
      ({ files, viewedAt, provider, host, repository, number, viewer }) =>
        client
          .request(
            "application_viewed_files_set",
            {
              scope: { provider, host, repository, number, viewer },
              files: files.map(({ path, revision, viewed }) => ({ path, revision, viewed })),
              viewed_at: viewedAt,
            },
            Written,
          )
          .pipe(
            Effect.asVoid,
            Effect.mapError(
              (cause) => new PersistenceSqlError({ operation: "setPullRequestFilesViewed", cause }),
            ),
          ),
    ),
  });
});

export const layerViewedFiles = Layer.effect(PullRequestFilesViewedRepository, makeViewedFiles);
