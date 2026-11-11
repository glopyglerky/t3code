import type { ThreadId, TurnItemId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

export const MCP_APP_MODEL_CONTEXT_MAX_BYTES = 16 * 1024;

export class McpAppModelContextError extends Schema.TaggedError<McpAppModelContextError>()(
  "McpAppModelContextError",
  { cause: Schema.optional(Schema.Defect()) },
) {}

export interface McpAppModelContextEntry {
  readonly itemId: string;
  readonly server: string;
  readonly tool: string;
  readonly text: string;
}

export class McpAppModelContext extends Context.Service<
  McpAppModelContext,
  {
    /** Empty text removes the app's context. */
    readonly set: (input: {
      readonly threadId: ThreadId;
      readonly itemId: TurnItemId;
      readonly server: string;
      readonly tool: string;
      readonly text: string;
    }) => Effect.Effect<void, McpAppModelContextError>;
    readonly forThread: (
      threadId: ThreadId,
    ) => Effect.Effect<ReadonlyArray<McpAppModelContextEntry>, McpAppModelContextError>;
  }
>()("t3/mcpApps/McpAppModelContext") {}

export type SetMcpAppModelContextInput = Parameters<McpAppModelContext["Service"]["set"]>[0];

export interface McpAppModelContextRows<E> {
  readonly remove: (input: SetMcpAppModelContextInput) => Effect.Effect<void, E>;
  readonly upsert: (
    input: SetMcpAppModelContextInput & { readonly updatedAt: string },
  ) => Effect.Effect<void, E>;
  readonly forThread: (
    threadId: ThreadId,
  ) => Effect.Effect<ReadonlyArray<McpAppModelContextEntry>, E>;
}

/** Shared input normalization and clock behavior, independent of the selected store. */
export function modelContextFromRows<E>(
  rows: McpAppModelContextRows<E>,
): McpAppModelContext["Service"] {
  const fail = (cause: unknown) => new McpAppModelContextError({ cause });
  return McpAppModelContext.of({
    set: Effect.fn("McpAppModelContext.set")(function* (input) {
      if (input.text.trim() === "") return yield* rows.remove(input).pipe(Effect.mapError(fail));
      const updatedAt = DateTime.formatIso(yield* DateTime.now);
      yield* rows.upsert({ ...input, updatedAt }).pipe(Effect.mapError(fail));
    }),
    forThread: Effect.fn("McpAppModelContext.forThread")((threadId) =>
      rows.forThread(threadId).pipe(Effect.mapError(fail)),
    ),
  });
}

export const layer = Layer.effect(
  McpAppModelContext,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return modelContextFromRows({
      remove: (input) =>
        sql`
          DELETE FROM mcp_app_model_context
          WHERE thread_id = ${input.threadId} AND item_id = ${input.itemId}
        `.pipe(Effect.asVoid),
      upsert: (input) =>
        sql`
        INSERT INTO mcp_app_model_context (thread_id, item_id, server, tool, text, updated_at)
        VALUES (${input.threadId}, ${input.itemId}, ${input.server}, ${input.tool}, ${input.text}, ${input.updatedAt})
        ON CONFLICT(thread_id, item_id) DO UPDATE SET text = excluded.text, updated_at = excluded.updated_at
      `.pipe(Effect.asVoid),
      forThread: (threadId) => {
        // Match by id across fork lineage; later source rollbacks leave the fork's copy intact.
        return sql<McpAppModelContextEntry>`
        SELECT context.item_id AS "itemId", context.server, context.tool, context.text
        FROM mcp_app_model_context AS context
        JOIN orchestration_v2_projection_turn_items AS item
          ON item.turn_item_id = context.item_id
        LEFT JOIN orchestration_v2_projection_runs AS run
          ON run.run_id = item.run_id
        WHERE context.thread_id = ${threadId}
          AND (
            run.status IS NULL
            OR run.status <> 'rolled_back'
            OR run.thread_id <> context.thread_id
          )
        ORDER BY context.updated_at, context.item_id
      `.pipe(NodeSqliteClient.readOnly);
      },
    });
  }),
);

export const layerEmpty = Layer.succeed(
  McpAppModelContext,
  McpAppModelContext.of({
    set: Effect.fn("McpAppModelContext.set")(function () {
      return Effect.void;
    }),
    forThread: Effect.fn("McpAppModelContext.forThread")(function () {
      return Effect.succeed([]);
    }),
  }),
);
