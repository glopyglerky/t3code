use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use t3_code_ls_core::{json as core_json, project_persistence_wire};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

use crate::project_validation::{monogram_graphemes, validate_command};
use crate::{CommitBatch, CommitOutcome, Error, ProjectionMutation, Record, Store, request_digest};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectRequest {
    pub command: Value,
    pub event_id: String,
    pub now: i64,
}

fn invalid(message: &str) -> Error {
    Error::Invalid(message.to_owned())
}

async fn read_bounded(reader: impl AsyncRead + Unpin, maximum: usize) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    reader
        .take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| invalid("planner output failed"))?;
    if bytes.len() > maximum {
        return Err(invalid("planner output exceeded its limit"));
    }
    Ok(bytes)
}

async fn run_planner(executable: &Path, request: &Value) -> Result<Value, Error> {
    if !executable.is_absolute() || !executable.is_file() {
        return Err(invalid("planner must be an absolute executable file"));
    }
    let mut input = serde_json::to_vec(request).map_err(|_| invalid("invalid planner input"))?;
    input.push(b'\n');
    if input.len() > project_persistence_wire::MAX_FRAME_BYTES {
        return Err(invalid("planner request exceeded 16 MiB"));
    }
    let expected = core_json::parse(
        std::str::from_utf8(&input).map_err(|_| invalid("invalid input encoding"))?,
    )
    .map_err(|_| invalid("invalid planner request"))?;
    let expected = project_persistence_wire::evaluate(&expected)
        .map_err(|_| invalid("invalid project command or stored projection"))?;

    let mut child = Command::new(executable)
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|_| invalid("failed to start the selected Lust planner"))?;
    let mut stdin = child
        .stdin
        .take()
        .ok_or_else(|| invalid("planner stdin unavailable"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| invalid("planner stdout unavailable"))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| invalid("planner stderr unavailable"))?;
    let operation = tokio::time::timeout(Duration::from_secs(30), async {
        let writer = async {
            stdin
                .write_all(&input)
                .await
                .map_err(|_| invalid("planner input failed"))?;
            stdin
                .shutdown()
                .await
                .map_err(|_| invalid("planner input close failed"))?;
            drop(stdin);
            Ok::<(), Error>(())
        };
        let ((), output, errors) = tokio::try_join!(
            writer,
            read_bounded(stdout, project_persistence_wire::MAX_FRAME_BYTES),
            read_bounded(stderr, 16 * 1024),
        )?;
        let status = child
            .wait()
            .await
            .map_err(|_| invalid("planner wait failed"))?;
        if !status.success() || !errors.is_empty() {
            return Err(invalid("selected Lust planner failed"));
        }
        Ok(output)
    })
    .await;
    let bytes = match operation {
        Ok(Ok(bytes)) => bytes,
        failure => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            return Err(match failure {
                Ok(Err(error)) => error,
                _ => invalid("selected Lust planner exceeded 30 seconds"),
            });
        }
    };
    let text =
        std::str::from_utf8(&bytes).map_err(|_| invalid("invalid planner response encoding"))?;
    let body = text
        .strip_suffix('\n')
        .ok_or_else(|| invalid("incomplete planner response"))?;
    if body.contains('\n') || body.contains('\r') {
        return Err(invalid("planner returned more than one frame"));
    }
    let actual = core_json::parse_with_limits(
        body,
        core_json::Limits {
            max_bytes: project_persistence_wire::MAX_FRAME_BYTES,
            max_depth: 64,
            max_values: 262_144,
        },
    )
    .map_err(|_| invalid("invalid planner response"))?;
    // A selected executable cannot substitute another command's event or projection.
    if actual != expected {
        return Err(invalid(
            "planner response does not match its validated request",
        ));
    }
    serde_json::from_str(body).map_err(|_| invalid("invalid planner JSON response"))
}

/// Execute the same planner in the full Rust application, without a subprocess boundary.
pub async fn execute_project_native(
    store: &Store,
    request: ProjectRequest,
) -> Result<CommitOutcome, Error> {
    execute_project_with(store, None, request).await
}

/// Qualify an independently compiled native Lust planner against the same transaction contract.
pub async fn execute_project(
    store: &Store,
    planner: &Path,
    request: ProjectRequest,
) -> Result<CommitOutcome, Error> {
    execute_project_with(store, Some(planner), request).await
}

async fn execute_project_with(
    store: &Store,
    planner: Option<&Path>,
    request: ProjectRequest,
) -> Result<CommitOutcome, Error> {
    let command = validate_command(&request.command)?;
    crate::validate_id(&request.event_id)?;
    let command_id = command
        .get("commandId")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("commandId is required"))?
        .to_owned();
    let project_id = command
        .get("projectId")
        .and_then(Value::as_str)
        .ok_or_else(|| invalid("projectId is required"))?
        .to_owned();
    crate::validate_id(&command_id)?;
    crate::validate_id(&project_id)?;
    let digest =
        request_digest(&json!({"protocol": "native-project-command-v1", "command": command}));
    if let Some(receipt) = store.read_receipt(&command_id).await? {
        if receipt.request_digest != digest {
            return Err(Error::CommandConflict);
        }
        return Ok(CommitOutcome {
            receipt,
            replayed: true,
        });
    }

    let snapshot = store.project_snapshot().await?;
    let key = format!("project/{project_id}");
    let mut project = Value::Null;
    let mut workspace_owner = Value::Null;
    let workspace_root = command.get("workspaceRoot").and_then(Value::as_str);
    for row in &snapshot.projections {
        if !row.key.starts_with("project/") {
            continue;
        }
        let encoded_row = serde_json::to_string(&row.payload)
            .map_err(|_| invalid("invalid stored project encoding"))?;
        let stored_row = core_json::parse(&encoded_row)
            .map_err(|_| invalid("invalid stored project encoding"))?;
        project_persistence_wire::validate_projection_shape(&stored_row)
            .map_err(|_| invalid("invalid stored project projection"))?;
        let row_id = row
            .payload
            .get("projectId")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("stored project has no identity"))?;
        if row.key != format!("project/{row_id}") {
            return Err(invalid("stored project identity does not match its key"));
        }
        if row.key == key {
            project = row.payload.clone();
        }
        if workspace_root.is_some()
            && row.payload.get("workspaceRoot").and_then(Value::as_str) == workspace_root
            && row.payload.get("deletedAt") == Some(&Value::Null)
        {
            if !workspace_owner.is_null() {
                return Err(invalid("more than one active project owns this workspace"));
            }
            workspace_owner = row.payload.clone();
        }
    }
    let planner_input = json!({
        "version": 1,
        "operation": "plan-and-project",
        "command": command,
        "state": { "project": project, "workspaceOwner": workspace_owner },
        "eventId": request.event_id,
        "now": request.now,
        "monogramGraphemeCount": monogram_graphemes(&command),
    });
    let result = match planner {
        Some(planner) => run_planner(planner, &planner_input).await?,
        None => {
            let encoded = serde_json::to_string(&planner_input)
                .map_err(|_| invalid("invalid project input encoding"))?;
            let input = core_json::parse_with_limits(
                &encoded,
                core_json::Limits {
                    max_bytes: project_persistence_wire::MAX_FRAME_BYTES,
                    max_depth: 64,
                    max_values: 262_144,
                },
            )
            .map_err(|_| invalid("invalid project planner input"))?;
            let result = project_persistence_wire::evaluate(&input)
                .map_err(|_| invalid("invalid project command or stored projection"))?;
            serde_json::from_str(&core_json::stringify(&result))
                .map_err(|_| invalid("invalid project planner result"))?
        }
    };
    let mut events = Vec::new();
    let mut projections = Vec::new();
    if let Some(accepted) = result.get("ok") {
        let event = accepted
            .get("event")
            .ok_or_else(|| invalid("planner omitted its event"))?;
        let projected = accepted
            .get("project")
            .ok_or_else(|| invalid("planner omitted its projection"))?;
        events.push(Record {
            id: request.event_id,
            payload: event.clone(),
        });
        projections.push(ProjectionMutation::Put {
            key,
            payload: projected.clone(),
        });
    }
    store
        .commit(&CommitBatch {
            command_id,
            request_digest: digest,
            expected_revision: snapshot.revision,
            events,
            projections,
            effects: Vec::new(),
            result,
        })
        .await
}
