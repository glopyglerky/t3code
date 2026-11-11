import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import {
  ScheduledRows,
  type Row,
  type ScheduledRead,
  type ScheduledWrite,
  toRowsError,
} from "./ScheduledRows.ts";

/** Retains the original SQL row operations as the independent native-port oracle. */
export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const conditions = (values: Row) =>
    Object.entries(values).map(([name, value]) => sql`${sql(name)} IS ${value}`);
  const assignments = (values: Row) =>
    Object.entries(values).map(([name, value]) => sql`${sql(name)} = ${value}`);
  const taskFilter = (input: { readonly task_id?: string; readonly thread_id?: string }) =>
    sql.and([
      ...(input.task_id === undefined ? [] : [sql`task_id = ${input.task_id}`]),
      ...(input.thread_id === undefined ? [] : [sql`thread_id = ${input.thread_id}`]),
    ]);
  const read = (query: ScheduledRead) => {
    switch (query.kind) {
      case "tasks":
        return sql`SELECT * FROM scheduled_tasks WHERE ${sql.and([
          taskFilter(query),
          ...(query.due_before === undefined
            ? []
            : [
                sql`enabled = 1 AND next_run_at IS NOT NULL AND next_run_at <= ${query.due_before} AND last_run_status <> 'running'`,
              ]),
        ])} ORDER BY ${query.due_before === undefined ? sql.literal("created_at DESC, task_id ASC") : sql.literal("next_run_at ASC, task_id ASC")}`;
      case "next_due":
        return sql`SELECT MIN(next_run_at) AS next_run_at FROM scheduled_tasks WHERE enabled = 1 AND next_run_at IS NOT NULL AND last_run_status <> 'running'`;
      case "deliveries":
        return sql`SELECT d.*, EXISTS (SELECT 1 FROM scheduled_task_webhook_pending p WHERE p.delivery_id = d.delivery_id AND p.state IN ('pending', 'dispatching')) AS pending_window
        FROM scheduled_task_webhook_deliveries d WHERE ${sql.and([sql`task_id = ${query.task_id}`, ...(query.delivery_id === undefined ? [] : [sql`delivery_id = ${query.delivery_id}`])])}
        ORDER BY received_at DESC, rowid DESC`;
      case "pending":
        return sql`SELECT * FROM scheduled_task_webhook_pending WHERE ${sql.and([
          ...(query.delivery_id === undefined ? [] : [sql`delivery_id = ${query.delivery_id}`]),
          ...(query.task_id === undefined ? [] : [sql`task_id = ${query.task_id}`]),
          ...(query.states === undefined ? [] : [sql.in("state", query.states)]),
          ...(query.coalesce_key === undefined ? [] : [sql`coalesce_key = ${query.coalesce_key}`]),
          ...(query.due_after === undefined ? [] : [sql`due_at > ${query.due_after}`]),
        ])} ORDER BY due_at, rowid ${query.first ? sql.literal("LIMIT 1") : sql.literal("")}`;
      case "pending_count":
        return sql`SELECT COUNT(*) AS count FROM scheduled_task_webhook_pending WHERE task_id = ${query.task_id} AND state IN ('pending', 'dispatching', 'running')`;
      case "next_pending":
        return sql`SELECT MIN(due_at) AS due_at FROM scheduled_task_webhook_pending WHERE state = 'pending'`;
      case "active_pending_tasks":
        return sql`SELECT DISTINCT task_id FROM scheduled_task_webhook_pending WHERE state IN ('dispatching', 'running', 'completed', 'failed')`;
      case "has_replay":
        return sql`SELECT 1 AS present FROM scheduled_task_webhook_replay_keys WHERE task_id = ${query.task_id} AND replay_key = ${query.replay_key} AND expires_at > ${query.now}`;
    }
  };
  const write = (mutation: ScheduledWrite) => {
    switch (mutation.kind) {
      case "save_task": {
        const row = mutation.row;
        return sql`INSERT INTO scheduled_tasks (task_id,title,prompt,enabled,schedule_json,project_id,thread_id,thread_mode,workspace_strategy_json,model_selection_json,
          runtime_mode,interaction_mode,created_by,creation_source,created_at,updated_at,next_run_at,last_run_at,last_run_status,last_run_error,run_count,webhook_token,webhook_secret)
          SELECT ${row.task_id},${row.title},${row.prompt},${row.enabled},${row.schedule_json},${row.project_id},${row.thread_id},${row.thread_mode},${row.workspace_strategy_json},${row.model_selection_json},
            ${row.runtime_mode},${row.interaction_mode},${row.created_by},${row.creation_source},${row.created_at},${row.updated_at},${row.next_run_at},${row.last_run_at},${row.last_run_status},${row.last_run_error},${row.run_count},${row.webhook_token},${row.webhook_secret}
          WHERE ${mutation.require_existing ? 0 : 1} = 1 OR EXISTS (SELECT 1 FROM scheduled_tasks WHERE task_id = ${row.task_id})
          ON CONFLICT (task_id) DO UPDATE SET title=excluded.title,prompt=excluded.prompt,enabled=excluded.enabled,schedule_json=excluded.schedule_json,project_id=excluded.project_id,
            thread_id=CASE WHEN excluded.thread_mode='new_thread' THEN NULL WHEN excluded.project_id!=scheduled_tasks.project_id THEN excluded.thread_id ELSE COALESCE(excluded.thread_id,scheduled_tasks.thread_id) END,
            thread_mode=excluded.thread_mode,workspace_strategy_json=excluded.workspace_strategy_json,model_selection_json=excluded.model_selection_json,runtime_mode=excluded.runtime_mode,interaction_mode=excluded.interaction_mode,
            creation_source=excluded.creation_source,updated_at=excluded.updated_at,
            next_run_at=CASE WHEN ${mutation.kept_schedule_json === null ? 0 : 1}=1 AND scheduled_tasks.schedule_json IN (excluded.schedule_json,${mutation.kept_schedule_json}) AND scheduled_tasks.enabled=excluded.enabled
              THEN scheduled_tasks.next_run_at ELSE excluded.next_run_at END,
            webhook_token=CASE WHEN excluded.webhook_token IS NULL THEN NULL ELSE COALESCE(scheduled_tasks.webhook_token,excluded.webhook_token) END,
            webhook_secret=CASE WHEN ${mutation.secret_changed ? 1 : 0}=1 THEN excluded.webhook_secret ELSE scheduled_tasks.webhook_secret END
          RETURNING task_id`;
      }
      case "patch_task":
        return sql`UPDATE scheduled_tasks SET ${sql.csv([
          ...assignments(mutation.patch),
          ...(mutation.increment_run_count ? [sql.literal("run_count = run_count + 1")] : []),
        ])} WHERE ${sql.and([
          ...(mutation.task_id === undefined ? [] : [sql`task_id = ${mutation.task_id}`]),
          ...(mutation.thread_ids === undefined ? [] : [sql.in("thread_id", mutation.thread_ids)]),
          ...conditions(mutation.expected),
          ...Object.entries(mutation.different).map(
            ([name, value]) => sql`${sql(name)} IS NOT ${value}`,
          ),
        ])} RETURNING task_id`;
      case "delete_tasks":
        return sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`DELETE FROM scheduled_task_webhook_deliveries WHERE task_id IN (SELECT task_id FROM scheduled_tasks WHERE ${taskFilter(mutation)})`;
            return yield* sql`DELETE FROM scheduled_tasks WHERE ${taskFilter(mutation)} RETURNING task_id`;
          }),
        );
      case "recover_task":
        return sql`UPDATE scheduled_tasks SET ${sql.csv([
          ...assignments(mutation.patch),
          sql`run_count = run_count + CASE WHEN EXISTS (SELECT 1 FROM scheduled_task_webhook_pending WHERE state = 'running' AND ${
            mutation.match_pending_task
              ? sql`task_id IS ${mutation.task_id} AND delivery_id = scheduled_tasks.last_run_webhook_delivery_id`
              : sql`delivery_id = ${mutation.pending_delivery_id ?? null}`
          }) THEN 0 ELSE 1 END`,
        ])} WHERE task_id IS ${mutation.task_id} AND last_run_status = 'running'`;
      case "record_delivery": {
        const row = mutation.row;
        return sql`INSERT INTO scheduled_task_webhook_deliveries (delivery_id,task_id,received_at,method,query,headers_json,body,body_bytes,body_truncated,outcome,signature_verified,missing_fields_json,rendered_prompt,error)
          SELECT ${row.delivery_id},${row.task_id},${row.received_at},${row.method},${row.query},${row.headers_json},${row.body},${row.body_bytes},${row.body_truncated},${row.outcome},${row.signature_verified},${row.missing_fields_json},${row.rendered_prompt},${row.error}
          WHERE EXISTS (SELECT 1 FROM scheduled_tasks WHERE task_id = ${row.task_id})
          ON CONFLICT (delivery_id) DO UPDATE SET outcome=excluded.outcome,signature_verified=excluded.signature_verified,missing_fields_json=excluded.missing_fields_json,rendered_prompt=excluded.rendered_prompt,error=excluded.error`;
      }
      case "patch_delivery":
        return sql`UPDATE scheduled_task_webhook_deliveries SET ${sql.csv(assignments(mutation.patch))} WHERE delivery_id=${mutation.delivery_id} RETURNING task_id`;
      case "prune_deliveries": {
        const bucket = sql.literal(
          "CASE WHEN outcome = 'filtered' THEN 'filtered' WHEN outcome IN ('duplicate','coalesced','rate_limited') OR error = 'The filled-in prompt is too long.' THEN 'dedupe' ELSE 'main' END",
        );
        return sql`DELETE FROM scheduled_task_webhook_deliveries WHERE task_id=${mutation.task_id} AND ${bucket}=${mutation.bucket} AND delivery_id NOT IN (
          SELECT delivery_id FROM scheduled_task_webhook_deliveries WHERE task_id=${mutation.task_id} AND ${bucket}=${mutation.bucket} ORDER BY received_at DESC,rowid DESC LIMIT ${mutation.limit})`;
      }
      case "claim_relay":
        return sql`INSERT INTO scheduled_task_webhook_relay_deliveries (relay_delivery_id,task_id,seen_at) VALUES (${mutation.relay_delivery_id},${mutation.task_id},${mutation.seen_at}) ON CONFLICT (relay_delivery_id) DO NOTHING RETURNING relay_delivery_id`;
      case "delete_relay":
        return sql`DELETE FROM scheduled_task_webhook_relay_deliveries WHERE relay_delivery_id=${mutation.relay_delivery_id}`;
      case "prune_relays":
        return sql`DELETE FROM scheduled_task_webhook_relay_deliveries WHERE seen_at<${mutation.before}`;
      case "claim_replay":
        return Effect.gen(function* () {
          yield* sql`DELETE FROM scheduled_task_webhook_replay_keys WHERE expires_at<=${mutation.now}`;
          return yield* sql`INSERT INTO scheduled_task_webhook_replay_keys (task_id,replay_key,delivery_id,expires_at) VALUES (${mutation.task_id},${mutation.replay_key},${mutation.delivery_id},${mutation.expires_at})`;
        });
      case "release_replay":
        return sql`DELETE FROM scheduled_task_webhook_replay_keys WHERE delivery_id=${mutation.delivery_id}`;
      case "insert_pending":
        return sql`INSERT INTO scheduled_task_webhook_pending ${sql.insert(mutation.row)}`;
      case "patch_pending":
        return sql`UPDATE scheduled_task_webhook_pending SET ${sql.csv([
          ...assignments(mutation.patch),
          ...(mutation.append_delivery_id === undefined
            ? []
            : [
                sql`delivery_ids_json = json_insert(delivery_ids_json,'$[#]',${mutation.append_delivery_id})`,
              ]),
        ])} WHERE ${sql.and([
          sql`delivery_id=${mutation.delivery_id}`,
          ...(mutation.states === undefined ? [] : [sql.in("state", mutation.states)]),
          ...(mutation.not_states === undefined
            ? []
            : [sql`NOT ${sql.in("state", mutation.not_states)}`]),
        ])} RETURNING delivery_id`;
      case "delete_pending":
        return sql`DELETE FROM scheduled_task_webhook_pending WHERE delivery_id=${mutation.delivery_id}`;
      case "expire_pending":
        return sql`UPDATE scheduled_task_webhook_pending SET state='dispatching' WHERE ${sql.and([
          sql`state='pending' AND due_at<=${mutation.now}`,
          ...(mutation.task_id === undefined ? [] : [sql`task_id=${mutation.task_id}`]),
          ...(mutation.coalesce_key === undefined
            ? []
            : [sql`coalesce_key=${mutation.coalesce_key}`]),
        ])}`;
      case "release_failed_relays":
        return Effect.gen(function* () {
          yield* sql`DELETE FROM scheduled_task_webhook_relay_deliveries WHERE relay_delivery_id IN (SELECT substr(value,${"delivery:relay:".length + 1}) FROM scheduled_task_webhook_pending,json_each(delivery_ids_json) WHERE delivery_id=${mutation.delivery_id} AND value LIKE 'delivery:relay:%')`;
          if (mutation.delivery_id.startsWith("delivery:relay:"))
            yield* sql`DELETE FROM scheduled_task_webhook_relay_deliveries WHERE relay_delivery_id=${mutation.delivery_id.slice("delivery:relay:".length)}`;
          return [];
        });
    }
  };
  return ScheduledRows.of({
    read: Effect.fn("ScheduledRows.read")(<A>(query: ScheduledRead, response: Schema.Decoder<A>) =>
      read(query).pipe(
        NodeSqliteClient.readOnly,
        Effect.flatMap(Schema.decodeUnknownEffect(response)),
        Effect.mapError(toRowsError("read")),
      ),
    ),
    write: Effect.fn("ScheduledRows.write")(
      <A>(mutation: ScheduledWrite, response: Schema.Decoder<A>) =>
        write(mutation).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(response)),
          Effect.mapError(toRowsError("write")),
        ),
    ),
  });
});
export const layer = Layer.effect(ScheduledRows, make);
