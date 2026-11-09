use std::sync::Arc;

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::{Value, json};
use t3_code_ls_core::codex_generation::js_trim;
use t3_code_ls_storage::project_service::{ProjectRequest, execute_project_native};
use t3_code_ls_storage::thread_service::{ThreadRequest, execute_thread};
use t3_code_ls_storage::{Error, Store};

#[cfg(unix)]
use crate::runtime::ProviderHub;

pub struct Application {
    store: Arc<Store>,
    #[cfg(unix)]
    providers: Option<ProviderHub>,
}

#[derive(Debug)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    pub data: Value,
}

impl RpcError {
    fn invalid(message: impl Into<String>) -> Self {
        Self {
            code: -32602,
            message: message.into(),
            data: Value::Null,
        }
    }
    fn unsupported() -> Self {
        Self {
            code: -32601,
            message: "Operation is not implemented by the native application".into(),
            data: Value::Null,
        }
    }
    #[cfg(unix)]
    fn provider(error: impl std::fmt::Display) -> Self {
        Self {
            code: -32002,
            message: error.to_string(),
            data: json!({"kind":"provider_error"}),
        }
    }
}

impl From<Error> for RpcError {
    fn from(error: Error) -> Self {
        let kind = match error {
            Error::Invalid(_) => "invalid_request",
            Error::IncompatibleStore => "incompatible_store",
            Error::StaleRevision { .. } => "stale_revision",
            Error::CommandConflict => "command_conflict",
            Error::DuplicateRecord => "duplicate_record",
            Error::TransactionConflict => "transaction_conflict",
            Error::UnknownCommit => "unknown_commit",
            Error::Database(_) => "database_error",
        };
        Self {
            code: -32001,
            message: error.to_string(),
            data: json!({"kind":kind}),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    jsonrpc: String,
    id: Value,
    method: String,
    #[serde(default = "empty_params")]
    params: Value,
}

fn empty_params() -> Value {
    json!({})
}

fn params<T: DeserializeOwned>(value: Value) -> Result<T, RpcError> {
    serde_json::from_value(value).map_err(|error| RpcError::invalid(error.to_string()))
}

fn encoded(value: impl serde::Serialize) -> Result<Value, RpcError> {
    serde_json::to_value(value).map_err(|_| RpcError::invalid("response cannot be encoded"))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Empty {}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EventPage {
    #[serde(default)]
    after_sequence: u64,
    #[serde(default = "page_limit")]
    limit: u32,
}
fn page_limit() -> u32 {
    256
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Command {
    command: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CommandId {
    command_id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EffectId {
    effect_id: String,
}

#[cfg(unix)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ThreadId {
    thread_id: String,
}

#[cfg(unix)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProviderRequest {
    thread_id: String,
    command_id: String,
    method: String,
    params: Value,
}

#[cfg(unix)]
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProviderResponse {
    thread_id: String,
    command_id: String,
    request_id: Value,
    result: Value,
}

impl Application {
    pub fn new(store: Arc<Store>) -> Self {
        Self {
            store,
            #[cfg(unix)]
            providers: None,
        }
    }

    #[cfg(unix)]
    pub fn with_providers(mut self, providers: ProviderHub) -> Self {
        self.providers = Some(providers);
        self
    }

    pub async fn shutdown(&self) -> Result<(), RpcError> {
        #[cfg(unix)]
        if let Some(providers) = &self.providers {
            providers.shutdown().await.map_err(RpcError::provider)?;
        }
        Ok(())
    }

    /// Every transport, including the command-line client and Iroh forwarding, uses this service.
    pub async fn handle(&self, value: Value) -> Value {
        let request: Request = match serde_json::from_value(value) {
            Ok(request) => request,
            Err(_) => return failure(Value::Null, -32600, "Invalid JSON-RPC request", Value::Null),
        };
        if request.jsonrpc != "2.0"
            || !(request.id.is_string() || request.id.is_u64())
            || request.id.as_str().is_some_and(|id| id.len() > 1024)
            || !request.params.is_object()
        {
            return failure(Value::Null, -32600, "Invalid JSON-RPC request", Value::Null);
        }
        match self.execute(&request.method, request.params).await {
            Ok(result) => json!({"jsonrpc":"2.0", "id":request.id, "result":result}),
            Err(error) => failure(request.id, error.code, &error.message, error.data),
        }
    }

    async fn execute(&self, method: &str, input: Value) -> Result<Value, RpcError> {
        match method {
            "application.capabilities" => {
                let _: Empty = params(input)?;
                #[cfg(unix)]
                let codex = self.providers.is_some();
                #[cfg(not(unix))]
                let codex = false;
                Ok(json!({
                    "protocolVersion": 1,
                    "storage": "redb",
                    "codexSessions": codex,
                    "commands": ["project.create", "project.meta.update", "thread.create",
                        "thread.metadata.update", "thread.pin", "thread.unpin", "thread.pin.reorder",
                        "thread.active.reorder", "thread.archive", "thread.unarchive", "thread.visit",
                        "thread.auto-settle.set"],
                    "events": "durable-cursor",
                    "upstreamClientCompatible": false
                }))
            }
            "state.snapshot" => {
                let _: Empty = params(input)?;
                encoded(self.store.snapshot().await?)
            }
            "events.list" => {
                let page: EventPage = params(input)?;
                encoded(self.store.events(page.after_sequence, page.limit).await?)
            }
            "receipt.get" => {
                let request: CommandId = params(input)?;
                encoded(self.store.read_receipt(&request.command_id).await?)
            }
            "outbox.get" => {
                let request: EffectId = params(input)?;
                encoded(self.store.outbox_get(&request.effect_id).await?)
            }
            "command.execute" => {
                let mut request: Command = params(input)?;
                if request
                    .command
                    .get("commandId")
                    .and_then(Value::as_str)
                    .is_some_and(|id| js_trim(id).starts_with("native:"))
                {
                    return Err(RpcError::invalid(
                        "native: command IDs are reserved for application operations",
                    ));
                }
                let kind = request
                    .command
                    .get("type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| RpcError::invalid("command.type is required"))?
                    .to_owned();
                if kind == "thread.create" {
                    let object = request
                        .command
                        .as_object_mut()
                        .ok_or_else(|| RpcError::invalid("command must be an object"))?;
                    object.insert("createdBy".into(), json!("user"));
                    object.insert("creationSource".into(), json!("server"));
                }
                let event_id = crate::new_id()
                    .map_err(|_| RpcError::invalid("event identity is unavailable"))?;
                let now = Utc::now();
                for attempt in 0..8 {
                    let result = if kind.starts_with("project.") {
                        execute_project_native(
                            &self.store,
                            ProjectRequest {
                                command: request.command.clone(),
                                event_id: event_id.clone(),
                                now: now.timestamp_millis(),
                            },
                        )
                        .await
                    } else if kind.starts_with("thread.") {
                        execute_thread(
                            &self.store,
                            ThreadRequest {
                                command: request.command.clone(),
                                event_id: event_id.clone(),
                                now: now.to_rfc3339_opts(SecondsFormat::Millis, true),
                            },
                        )
                        .await
                    } else {
                        return Err(RpcError::unsupported());
                    };
                    match result {
                        Err(Error::TransactionConflict | Error::StaleRevision { .. })
                            if attempt < 7 =>
                        {
                            continue;
                        }
                        result => return encoded(result?),
                    }
                }
                unreachable!("bounded retry returns on its last attempt")
            }
            #[cfg(unix)]
            "session.open" => {
                let request: ThreadId = params(input)?;
                self.providers
                    .as_ref()
                    .ok_or_else(RpcError::unsupported)?
                    .open(&request.thread_id)
                    .await
                    .map_err(RpcError::provider)
            }
            #[cfg(unix)]
            "session.request" => {
                let request: ProviderRequest = params(input)?;
                self.providers
                    .as_ref()
                    .ok_or_else(RpcError::unsupported)?
                    .request(
                        &request.thread_id,
                        &request.command_id,
                        &request.method,
                        request.params,
                    )
                    .await
                    .map_err(RpcError::provider)
            }
            #[cfg(unix)]
            "session.respond" => {
                let request: ProviderResponse = params(input)?;
                self.providers
                    .as_ref()
                    .ok_or_else(RpcError::unsupported)?
                    .respond(
                        &request.thread_id,
                        &request.command_id,
                        request.request_id,
                        request.result,
                    )
                    .await
                    .map_err(RpcError::provider)
            }
            #[cfg(unix)]
            "session.close" => {
                let request: ThreadId = params(input)?;
                self.providers
                    .as_ref()
                    .ok_or_else(RpcError::unsupported)?
                    .close(&request.thread_id)
                    .await
                    .map_err(RpcError::provider)
            }
            #[cfg(unix)]
            "session.request.wait" => {
                let request: CommandId = params(input)?;
                let providers = self.providers.as_ref().ok_or_else(RpcError::unsupported)?;
                match tokio::time::timeout(
                    std::time::Duration::from_secs(25),
                    providers.wait_for_request(&request.command_id),
                )
                .await
                {
                    Ok(result) => result.map_err(RpcError::provider),
                    Err(_) => Ok(json!({"pending":true,"commandId":request.command_id})),
                }
            }
            #[cfg(unix)]
            "session.approval.wait" => {
                let request: ThreadId = params(input)?;
                let providers = self.providers.as_ref().ok_or_else(RpcError::unsupported)?;
                match tokio::time::timeout(
                    std::time::Duration::from_secs(25),
                    providers.wait_for_approval(&request.thread_id),
                )
                .await
                {
                    Ok(result) => result.map_err(RpcError::provider),
                    Err(_) => Ok(json!({"pending":true,"threadId":request.thread_id})),
                }
            }
            #[cfg(unix)]
            "session.idle.wait" => {
                let request: ThreadId = params(input)?;
                let providers = self.providers.as_ref().ok_or_else(RpcError::unsupported)?;
                match tokio::time::timeout(
                    std::time::Duration::from_secs(25),
                    providers.wait_for_idle(&request.thread_id),
                )
                .await
                {
                    Ok(result) => result.map_err(RpcError::provider),
                    Err(_) => Ok(json!({"pending":true,"threadId":request.thread_id})),
                }
            }
            _ => Err(RpcError::unsupported()),
        }
    }
}

pub fn failure(id: Value, code: i64, message: &str, data: Value) -> Value {
    json!({"jsonrpc":"2.0", "id":id, "error":{"code":code, "message":message, "data":data}})
}
