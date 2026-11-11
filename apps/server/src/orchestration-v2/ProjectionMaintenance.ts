import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import {
  EVENT_LOG_PRUNE_MARK,
  EVENT_LOG_PRUNED_THROUGH,
} from "../persistence/OrchestrationEventStore.ts";
import { PersistenceTransactions, sqliteLayer } from "../persistence/PersistenceTransactions.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as Rows from "./MaintenanceRows.ts";
import * as SqliteMaintenanceRows from "./SqliteMaintenanceRows.ts";

export class ProjectionMaintenanceError extends Schema.TaggedError<ProjectionMaintenanceError>()(
  "ProjectionMaintenanceError",
  { operation: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {}

export interface ProjectionMaintenanceV2Shape {
  /** Deletes events the projections already cover once they are older than the retained history. */
  readonly pruneEventStore: (betweenPages?: Effect.Effect<void>) => Effect.Effect<
    {
      readonly deletedEventCount: number;
      readonly deletedReceiptCount: number;
      readonly reclaimableBytes: number;
    },
    ProjectionMaintenanceError
  >;
}
export class ProjectionMaintenanceV2 extends Context.Service<
  ProjectionMaintenanceV2,
  ProjectionMaintenanceV2Shape
>()("t3/orchestration-v2/ProjectionMaintenance/ProjectionMaintenanceV2") {}

// Reconnect replay and command-id retries read this tail; older cursors resync from a snapshot.
const RETAINED_EVENT_HISTORY = Duration.days(1);
const PAGE_SIZE = 50;
const REFERENCE_SCAN_WINDOW = 1000;

/** Shared retention policy; row access and transaction ownership are selected before execution. */
export const layerWithRows = Layer.effect(
  ProjectionMaintenanceV2,
  Effect.gen(function* () {
    const rows = yield* Rows.MaintenanceRows;
    const transactions = yield* PersistenceTransactions;
    const writeMetadata = (name: string, sequence: number, at: DateTime.Utc) =>
      rows.write(
        {
          kind: "metadata",
          name,
          sequence,
          schema_version: ProjectionStore.ORCHESTRATION_V2_PROJECTION_SCHEMA_VERSION,
          updated_at: DateTime.formatIso(at),
        },
        Rows.EmptyRows,
      );

    const readPromptReferences = Effect.fnUntraced(function* (
      afterSequence: number,
      throughSequence: number,
      betweenPages: Effect.Effect<void>,
    ) {
      const referenced = new Set<number>();
      for (let cursor = afterSequence; cursor < throughSequence;) {
        const windowEnd = Math.min(cursor + REFERENCE_SCAN_WINDOW, throughSequence);
        const references = yield* rows.read(
          { kind: "prompt_references", after_sequence: cursor, through_sequence: windowEnd },
          Rows.ReferenceRows,
        );
        for (const row of references) referenced.add(row.prompt_sequence);
        cursor = windowEnd;
        yield* betweenPages;
      }
      return referenced;
    });

    const pruneEvents = Effect.fnUntraced(function* (
      throughSequence: number,
      head: number,
      betweenPages: Effect.Effect<void>,
    ) {
      const referenced = yield* readPromptReferences(throughSequence, head, betweenPages);
      // Retained stream heads can also borrow anchors from before the boundary.
      for (const row of yield* rows.read(
        { kind: "head_references", through_sequence: throughSequence },
        Rows.ReferenceRows,
      ))
        referenced.add(row.prompt_sequence);
      yield* betweenPages;
      let referencesThrough = head;
      let deleted = 0;
      let cursor = 0;
      while (cursor < throughSequence) {
        const candidatesPage = yield* rows.read(
          {
            kind: "candidates",
            after_sequence: cursor,
            through_sequence: throughSequence,
            limit: PAGE_SIZE,
          },
          Rows.CandidateRows,
        );
        const candidates = candidatesPage
          .filter((row) => row.prunable === 1)
          .map((row) => row.sequence);
        // Never hold a transaction or reader lock across a yield or rest.
        yield* Effect.yieldNow;
        if (candidates.length > 0) {
          deleted += yield* transactions.withTransaction(
            Effect.gen(function* () {
              const appended = yield* rows.read(
                { kind: "appended", after_sequence: referencesThrough },
                Rows.AppendedRows,
              );
              for (const row of appended) {
                if (row.prompt_sequence !== null) referenced.add(row.prompt_sequence);
                referencesThrough = Math.max(referencesThrough, row.sequence);
              }
              const obsolete = candidates.filter((sequence) => !referenced.has(sequence));
              if (obsolete.length === 0) return 0;
              yield* rows.write({ kind: "delete_events", sequences: obsolete }, Rows.EmptyRows);
              return obsolete.length;
            }),
          );
          yield* Effect.yieldNow;
        }
        yield* betweenPages;
        if (candidatesPage.length < PAGE_SIZE) break;
        cursor = candidatesPage.at(-1)?.sequence ?? throughSequence;
      }
      return deleted;
    });

    const pruneImportedLegacyHistory = Effect.fnUntraced(function* (
      throughSequence: number,
      throughReceiptRowId: number,
      betweenPages: Effect.Effect<void>,
    ) {
      let deletedEventCount = 0;
      let deletedReceiptCount = 0;
      for (let cursor = 0; ;) {
        const page = yield* rows.read(
          {
            kind: "legacy_events",
            after_sequence: cursor,
            through_sequence: throughSequence,
            limit: PAGE_SIZE,
          },
          Rows.LegacyEventRows,
        );
        const obsolete = page.filter((row) => row.imported === 1).map((row) => row.sequence);
        yield* Effect.yieldNow;
        if (obsolete.length > 0) {
          yield* rows.write({ kind: "delete_events", sequences: obsolete }, Rows.EmptyRows);
          deletedEventCount += obsolete.length;
          yield* Effect.yieldNow;
        }
        yield* betweenPages;
        if (page.length < PAGE_SIZE) break;
        cursor = page.at(-1)?.sequence ?? cursor;
      }
      for (let cursor = 0; ;) {
        const page = yield* rows.read(
          {
            kind: "legacy_receipts",
            after_row_id: cursor,
            through_row_id: throughReceiptRowId,
            limit: PAGE_SIZE,
          },
          Rows.LegacyReceiptRows,
        );
        const obsolete = page.filter((row) => row.imported === 1).map((row) => row.command_id);
        yield* Effect.yieldNow;
        if (obsolete.length > 0) {
          yield* rows.write({ kind: "delete_receipts", command_ids: obsolete }, Rows.EmptyRows);
          deletedReceiptCount += obsolete.length;
          yield* Effect.yieldNow;
        }
        yield* betweenPages;
        if (page.length < PAGE_SIZE) break;
        cursor = page.at(-1)?.row_id ?? cursor;
      }
      return { deletedEventCount, deletedReceiptCount };
    });

    const pruneEventStore = Effect.fn("ProjectionMaintenance.pruneEventStore")(
      function* (betweenPages: Effect.Effect<void> = Effect.void) {
        const now = yield* DateTime.now;
        const metadata = yield* rows.read({ kind: "metadata" }, Rows.MetadataRows);
        const read = (name: string) => metadata.find((row) => row.projection_name === name);
        const projected = read("thread-projections")?.last_sequence ?? 0;
        const mark = read(EVENT_LOG_PRUNE_MARK);
        const prunedThrough = read(EVENT_LOG_PRUNED_THROUGH)?.last_sequence ?? 0;
        // Fixed bounds: foreground commits during the pass wait for the next one.
        const bounds = yield* rows.read({ kind: "bounds" }, Rows.BoundRows);
        const head = bounds[0]?.sequence ?? 0;
        let deletedEventCount = 0;
        const markIsDue =
          mark !== undefined &&
          DateTime.toEpochMillis(now) - Date.parse(mark.updated_at) >=
            Duration.toMillis(RETAINED_EVENT_HISTORY);
        if (mark !== undefined && markIsDue) {
          const throughSequence = Math.max(Math.min(mark.last_sequence, projected), prunedThrough);
          // Publish the boundary before deleting so reconnects take a complete snapshot.
          if (throughSequence > prunedThrough)
            yield* writeMetadata(EVENT_LOG_PRUNED_THROUGH, throughSequence, now);
          deletedEventCount += yield* pruneEvents(throughSequence, head, betweenPages);
        }
        if (mark === undefined || markIsDue) yield* writeMetadata(EVENT_LOG_PRUNE_MARK, head, now);
        const legacy = yield* pruneImportedLegacyHistory(
          head,
          bounds[0]?.receipt_row_id ?? 0,
          betweenPages,
        );
        deletedEventCount += legacy.deletedEventCount;
        const reclaimable = yield* rows.read({ kind: "reclaimable" }, Rows.ReclaimableRows);
        return {
          deletedEventCount,
          deletedReceiptCount: legacy.deletedReceiptCount,
          reclaimableBytes: reclaimable[0]?.reclaimable_bytes ?? 0,
        };
      },
      Effect.mapError(
        (cause) => new ProjectionMaintenanceError({ operation: "prune event store", cause }),
      ),
    );
    return ProjectionMaintenanceV2.of({ pruneEventStore });
  }),
);

export const layer = layerWithRows.pipe(
  Layer.provide(SqliteMaintenanceRows.layer),
  Layer.provide(sqliteLayer),
);
