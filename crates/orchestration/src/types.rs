//! Rust ownership for existing OrchestrationV2 JSON records. Arc identity is
//! significant: upstream compaction only removes an item shared by identity.
//! Unmapped contract fields remain intact; this is not the full schema validator.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub type Item = Arc<Value>;
pub type Projection = Arc<ThreadProjection>;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectedTurnItem {
    pub source_thread_id: String,
    pub visibility: String,
    pub item: Item,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ThreadProjection {
    pub thread: Value,
    pub messages: Vec<Value>,
    pub checkpoints: Vec<Value>,
    pub subagents: Vec<Value>,
    pub context_handoffs: Vec<Value>,
    pub turn_items: Vec<Item>,
    pub visible_turn_items: Vec<Arc<ProjectedTurnItem>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
