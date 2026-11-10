# Native qualification API walkthrough

This is the temporary native qualification host's JSON-RPC protocol, not the
original T3 Code application protocol. It is not compatible with the original
web, desktop or mobile clients; porting those clients and their contracts remains
required. This walkthrough documents the existing host, not a replacement
application design. See [the source-to-port map](port-map.md) for the direct ports.

The sequence below follows the implemented service and controlled provider tests;
it is not evidence of qualification with a real Codex installation or model.

## Start a server

Use a qualified `t3-code-ls-app` build on Unix. Replace `/absolute/path/to/codex` with a trusted Codex executable, and replace `MODEL_ID` in the thread request below with a model available through that executable's configuration and authentication. The native server does not provision provider credentials.

```sh
mkdir -p /tmp/t3code-ls-example/workspace
t3-code-ls-app initialize --state /tmp/t3code-ls-example/state
t3-code-ls-app serve --state /tmp/t3code-ls-example/state --listen 127.0.0.1:3773 --provider /absolute/path/to/codex
```

The workspace must exist before opening a provider session. The default provider launch argument is `app-server`. Omitting `--provider` leaves storage operations available and disables provider sessions.

In another terminal, save each request below as `request.json` and send it with:

```sh
t3-code-ls-app request --address 127.0.0.1:3773 --token-file /tmp/t3code-ls-example/state/api-token < request.json
```

The client reads the private token file and supplies authentication. Keep the token in that file; do not print it, paste it into request JSON, or put its contents in command arguments. Direct HTTP clients use `POST /rpc` with `Content-Type: application/json` and exactly one `Authorization: Bearer <token>` header. Requests containing an `Origin` header are rejected.

Send one JSON object per request; batches and notifications are unsupported. `id` correlates a JSON-RPC reply. `commandId` identifies a durable operation: keep it unchanged when retrying the same operation, and choose a new one for a new operation. Command IDs share a database-wide namespace; `native:` is reserved.

Check configuration first:

```json
{"jsonrpc":"2.0","id":"capabilities","method":"application.capabilities","params":{}}
```

Provider steps require `result.codexSessions` to be `true`. For every reply, inspect a top-level `error` before continuing. For `command.execute`, also inspect `result.receipt.result`: a durable business rejection can appear there as `error` even when the JSON-RPC envelope has a `result`.

## 1. Create a project and thread

```json
{
  "jsonrpc": "2.0",
  "id": "create-project",
  "method": "command.execute",
  "params": {
    "command": {
      "type": "project.create",
      "commandId": "project-create-1",
      "projectId": "project-1",
      "title": "Example",
      "workspaceRoot": "/tmp/t3code-ls-example/workspace"
    }
  }
}
```

```json
{
  "jsonrpc": "2.0",
  "id": "create-thread",
  "method": "command.execute",
  "params": {
    "command": {
      "type": "thread.create",
      "commandId": "thread-create-1",
      "threadId": "thread-1",
      "projectId": "project-1",
      "title": "Example conversation",
      "modelSelection": {"instanceId": "codex", "model": "MODEL_ID"},
      "runtimeMode": "approval-required",
      "interactionMode": "default",
      "branch": null,
      "worktreePath": null
    }
  }
}
```

`instanceId` must be `codex` for native provider execution. This example uses the implemented approval-required, read-only provider policy. The server supplies creation provenance and derives provider workspace, model and permissions from stored state; do not add overrides to provider requests.

## 2. Open a session and start its provider thread

```json
{"jsonrpc":"2.0","id":"open-session","method":"session.open","params":{"threadId":"thread-1"}}
```

A successful open returns the session projection with `status: "open"`. It starts and initializes the process; it does not yet start the provider thread.

```json
{"jsonrpc":"2.0","id":"start-provider-thread","method":"session.request","params":{"threadId":"thread-1","commandId":"provider-start-1","method":"thread/start","params":{}}}
```

```json
{"jsonrpc":"2.0","id":"wait-provider-thread","method":"session.request.wait","params":{"commandId":"provider-start-1"}}
```

Wait for `result.completion.result.state` to be `"responded"` before starting a turn. An acceptance receipt alone does not establish success. If a wait returns `result.pending: true`, repeat that wait. The provider response itself is stored in the event identified by `result.completion.result.eventId`.

## 3. Start a text turn and handle an approval if requested

```json
{"jsonrpc":"2.0","id":"start-turn","method":"session.request","params":{"threadId":"thread-1","commandId":"turn-1","method":"turn/start","params":{"input":[{"type":"text","text":"List the files in this workspace and explain what you find."}]}}}
```

```json
{"jsonrpc":"2.0","id":"wait-turn-request","method":"session.request.wait","params":{"commandId":"turn-1"}}
```

Here, `"responded"` means the provider answered the turn-start RPC. The turn may still be running, producing output or waiting for approval. Text input accepts 1–32 nonempty entries; this path does not accept attachments.

```json
{"jsonrpc":"2.0","id":"wait-approval","method":"session.approval.wait","params":{"threadId":"thread-1"}}
```

An approval is conditional on provider behavior. If the turn becomes idle without one, skip the response step. A successful approval wait returns correlation fields such as `sessionId`, `requestId`, `method`, `nativeThreadId` and `nativeTurnId`. Inspect the requested action before answering: find an event with `event.payload.type: "native.provider.event"`, `event.payload.payload.kind: "server_request"`, and matching `event.payload.sessionId` and `event.payload.payload.id`. Its `event.payload.payload.params` contains the provider's request details.

```json
{"jsonrpc":"2.0","id":"read-provider-events","method":"events.list","params":{"afterSequence":0,"limit":256}}
```

Page forward using each page's last `sequence` until the matching request is found. For an observed `item/commandExecution/requestApproval` that you have decided to accept, submit:

```json
{"jsonrpc":"2.0","id":"answer-approval","method":"session.respond","params":{"threadId":"thread-1","commandId":"approval-1","requestId":100,"result":{"decision":"accept"}}}
```

`100` is an example, not a fixed protocol ID. Replace it with the exact observed `requestId`, preserving its JSON type. Do not reuse this answer shape for a different provider request method. Unknown, resolved and other-thread request IDs are rejected. A successful answer's completion state is `"submitted"`; that records submission of the answer, not completion of the turn.

## 4. Wait for idle, close, then archive

```json
{"jsonrpc":"2.0","id":"wait-idle","method":"session.idle.wait","params":{"threadId":"thread-1"}}
```

Repeat while `result.pending` is `true`, handling any further approvals as above. A successful idle result has `activeTurnId: null` and no unresolved thread/turn start. Read events for the provider's output and turn status; idle does not by itself mean the requested task succeeded.

```json
{"jsonrpc":"2.0","id":"close-session","method":"session.close","params":{"threadId":"thread-1"}}
```

The normal close path stops and reaps the owned process. Confirm `status: "closed"`, `processState: "stopped"`, `loaded: false` and `activeTurnId: null` before archiving:

```json
{"jsonrpc":"2.0","id":"archive-thread","method":"command.execute","params":{"command":{"type":"thread.archive","commandId":"archive-1","threadId":"thread-1"}}}
```

Archive commits the thread event, projection, receipt and required `terminal.cleanup` effect together. The general cleanup worker is unfinished, so the effect remains pending:

```json
{"jsonrpc":"2.0","id":"read-cleanup","method":"outbox.get","params":{"effectId":"effect:archive-1:terminal.cleanup"}}
```

To continue an unarchived, previously bound thread after a normal close, reopen its session and submit `thread/resume` with `{}` under a new command ID instead of `thread/start`. Changed workspace/model/runtime context cannot silently reuse the previous provider history. `turn/interrupt` also accepts `{}` and targets the currently active turn; wait for its receipt and then for idle.

## Recover a lost reply

The server and client impose a 45-second HTTP request deadline. The three wait methods return `pending: true` after 25 seconds without a result; that is a polling boundary, not cancellation of the provider action. HTTP request bodies are limited to 8 MiB, with a 10-second body-read deadline; provider request parameters and answers are limited to 2 MiB. A disconnect or timeout does not prove that nothing committed or executed.

Read the original operation's receipt before deciding what to do next:

```json
{"jsonrpc":"2.0","id":"recover-turn-intent","method":"receipt.get","params":{"commandId":"turn-1"}}
```

For provider requests and answers, the original receipt records acceptance. The terminal receipt has a separate application-generated ID:

```json
{"jsonrpc":"2.0","id":"recover-turn-result","method":"receipt.get","params":{"commandId":"native:result:turn-1"}}
```

`receipt.get` returns `null` when that receipt does not exist. Repeat the original request with the same `commandId` and payload to recover its existing receipt without resending an accepted turn or answer. Reusing an ID with different content fails. A new ID represents a new action and may duplicate work if used merely to bypass a lost reply. `session.request.wait` returns the acceptance receipt, terminal receipt and any unresolved intent together.

| Terminal state | Meaning |
| --- | --- |
| `responded` | The provider RPC returned a response; inspect its event. A turn may still be active. |
| `submitted` | An answer was submitted to the provider. |
| `rejected` | The provider returned an RPC error; inspect its event. |
| `not_sent` | The runtime recorded a submission failure; inspect the receipt's error. |
| `uncertain` | The runtime cannot establish the external outcome. Do not infer success or failure. |

Uncertainty can also appear as `unresolved.state: "uncertain"` without a terminal receipt after recovery. Interrupted accepted work is never replayed automatically. Inspect receipts, events, session state and provider history before issuing another action. The native API has no force-clear or automatic retry operation for uncertain work; such work blocks reopening or lifecycle changes requiring cleanup. Recovery may also record `processState: "unknown"` when process exit cannot be confirmed. Restarting the server alone does not prove that a provider process stopped.

For reconnection, obtain the current state and its cursor:

```json
{"jsonrpc":"2.0","id":"reconnect-snapshot","method":"state.snapshot","params":{}}
```

Use `result.last_sequence` as the next `events.list` request's `afterSequence`. A client retaining older state instead resumes from its last processed event's `sequence`. Events are ordered and strictly after the supplied cursor; advance the cursor only after processing a page. The cursor reflects committed storage, not provider completion.

## Implementation references

The exact request boundary is in [service.rs](../crates/app/src/service.rs), HTTP limits are in [http.rs](../crates/app/src/http.rs), and provider state transitions are in [runtime.rs](../crates/app/src/runtime.rs). Controlled examples and recovery assertions are in [provider_runtime.rs](../crates/app/tests/provider_runtime.rs); [application.rs](../crates/app/tests/application.rs) covers durable commands and receipts. These source/tests define the implemented subset; they do not establish real-provider or original-client parity.
