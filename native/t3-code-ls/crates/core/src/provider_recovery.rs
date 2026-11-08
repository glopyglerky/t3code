//! Recovery event planning from provider-core/server/subagentProjection.ts.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.
//!
//! These functions plan events; the caller still owns their durable commit. Time
//! values are opaque because upstream only copies them. Payload retains fields
//! outside this planner's model, without dropping provider-specific data.

use std::collections::HashSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkStatus {
    Idle,
    Pending,
    Running,
    Waiting,
    Completed,
    Interrupted,
    Failed,
    Cancelled,
    RolledBack,
}

impl WorkStatus {
    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "idle" => Some(Self::Idle),
            "pending" => Some(Self::Pending),
            "running" => Some(Self::Running),
            "waiting" => Some(Self::Waiting),
            "completed" => Some(Self::Completed),
            "interrupted" => Some(Self::Interrupted),
            "failed" => Some(Self::Failed),
            "cancelled" => Some(Self::Cancelled),
            "rolled_back" => Some(Self::RolledBack),
            _ => None,
        }
    }

    pub fn as_wire(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Pending => "pending",
            Self::Running => "running",
            Self::Waiting => "waiting",
            Self::Completed => "completed",
            Self::Interrupted => "interrupted",
            Self::Failed => "failed",
            Self::Cancelled => "cancelled",
            Self::RolledBack => "rolled_back",
        }
    }

    pub fn is_active(self) -> bool {
        matches!(self, Self::Pending | Self::Running | Self::Waiting)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalStatus {
    Completed,
    Failed,
    Interrupted,
    Cancelled,
}

impl TerminalStatus {
    pub fn work_status(self) -> WorkStatus {
        match self {
            Self::Completed => WorkStatus::Completed,
            Self::Failed => WorkStatus::Failed,
            Self::Interrupted => WorkStatus::Interrupted,
            Self::Cancelled => WorkStatus::Cancelled,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubagentOrigin {
    ProviderNative,
    AppOwned,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeKind {
    RootTurn,
    Other(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TurnItemKind {
    Subagent {
        subagent_id: String,
        result: Option<String>,
    },
    Reasoning {
        streaming: bool,
    },
    AssistantMessage {
        streaming: bool,
    },
    CommandExecution,
    DynamicTool,
    Other(String),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Subagent<Payload, Time> {
    pub id: String,
    pub thread_id: String,
    pub run_id: Option<String>,
    pub origin: SubagentOrigin,
    pub driver: String,
    pub provider_instance_id: String,
    pub status: WorkStatus,
    pub completed_at: Option<Time>,
    pub updated_at: Time,
    pub result: Option<String>,
    pub payload: Payload,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Node<Payload, Time> {
    pub id: String,
    pub kind: NodeKind,
    pub run_id: Option<String>,
    pub status: WorkStatus,
    pub completed_at: Option<Time>,
    pub payload: Payload,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TurnItem<Payload, Time> {
    pub id: String,
    pub node_id: Option<String>,
    pub run_id: Option<String>,
    pub kind: TurnItemKind,
    pub status: WorkStatus,
    pub completed_at: Option<Time>,
    pub updated_at: Time,
    pub payload: Payload,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ProviderTurn<Payload, Time> {
    pub id: String,
    pub node_id: String,
    pub run_attempt_id: Option<String>,
    pub status: WorkStatus,
    pub completed_at: Option<Time>,
    pub payload: Payload,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Projection<Payload, Time> {
    pub subagents: Vec<Subagent<Payload, Time>>,
    pub nodes: Vec<Node<Payload, Time>>,
    pub turn_items: Vec<TurnItem<Payload, Time>>,
    pub provider_turns: Vec<ProviderTurn<Payload, Time>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UpdateKind {
    Subagent,
    Node,
    TurnItem,
    ProviderTurn,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmittedUpdate {
    pub kind: UpdateKind,
    pub id: String,
}

#[derive(Clone, Debug, PartialEq)]
pub enum EventPayload<Payload, Time> {
    Subagent(Subagent<Payload, Time>),
    Node(Node<Payload, Time>),
    TurnItem(TurnItem<Payload, Time>),
    ProviderTurn(ProviderTurn<Payload, Time>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Event<Payload, Time> {
    pub id: String,
    pub thread_id: String,
    pub run_id: Option<String>,
    pub node_id: String,
    pub provider_instance_id: String,
    pub occurred_at: Time,
    pub payload: EventPayload<Payload, Time>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Completion<Time> {
    pub status: TerminalStatus,
    /// None means upstream's omitted completedAt, which defaults to now.
    pub completed_at: Option<Time>,
    pub now: Time,
}

impl<Time: Clone> Completion<Time> {
    fn completed_at(&self) -> Time {
        self.completed_at.as_ref().unwrap_or(&self.now).clone()
    }
}

pub struct NativeSubagentEnd<'a, Time> {
    pub subagent_id: &'a str,
    pub completion: Completion<Time>,
    /// None preserves the old result; Some(None) explicitly clears it.
    pub result: Option<Option<String>>,
}

pub struct RootTurnsEnd<Time> {
    pub thread_id: String,
    pub provider_instance_id: String,
    pub completion: Completion<Time>,
}

fn written_ids(emitted: &[EmittedUpdate], kind: UpdateKind) -> HashSet<String> {
    emitted
        .iter()
        .filter(|event| event.kind == kind)
        .map(|event| event.id.clone())
        .collect()
}

/// End each still-active record independently, including when a related record
/// was already settled earlier in this recovery batch. Allocation failures
/// propagate immediately; the caller must not commit a partial event batch.
pub fn end_orphaned_native_subagent<Payload: Clone, Time: Clone, Error>(
    projection: &Projection<Payload, Time>,
    input: NativeSubagentEnd<'_, Time>,
    emitted: &[EmittedUpdate],
    allocate_event_id: &mut impl FnMut() -> Result<String, Error>,
) -> Result<Vec<Event<Payload, Time>>, Error> {
    let Some(subagent) = projection
        .subagents
        .iter()
        .find(|entry| entry.id == input.subagent_id)
    else {
        return Ok(Vec::new());
    };
    if subagent.origin != SubagentOrigin::ProviderNative {
        return Ok(Vec::new());
    }
    let node = projection
        .nodes
        .iter()
        .find(|entry| entry.id == input.subagent_id);
    let item = projection.turn_items.iter().find(|entry| {
        matches!(&entry.kind, TurnItemKind::Subagent { subagent_id, .. }
            if subagent_id == input.subagent_id)
    });
    let completion = &input.completion;
    let scope = |id, payload| Event {
        id,
        thread_id: subagent.thread_id.clone(),
        run_id: subagent.run_id.clone(),
        node_id: input.subagent_id.to_string(),
        provider_instance_id: subagent.provider_instance_id.clone(),
        occurred_at: completion.now.clone(),
        payload,
    };
    let mut events = Vec::new();
    if subagent.status.is_active()
        && !written_ids(emitted, UpdateKind::Subagent).contains(input.subagent_id)
    {
        let id = allocate_event_id()?;
        let mut ended = subagent.clone();
        ended.status = completion.status.work_status();
        ended.completed_at = Some(completion.completed_at());
        ended.updated_at = completion.now.clone();
        if let Some(result) = &input.result {
            ended.result = result.clone();
        }
        events.push(scope(id, EventPayload::Subagent(ended)));
    }
    if let Some(node) = node
        && node.status.is_active()
        && !written_ids(emitted, UpdateKind::Node).contains(input.subagent_id)
    {
        let id = allocate_event_id()?;
        let mut ended = node.clone();
        ended.status = completion.status.work_status();
        ended.completed_at = Some(completion.completed_at());
        events.push(scope(id, EventPayload::Node(ended)));
    }
    if let Some(item) = item
        && item.status.is_active()
        && !written_ids(emitted, UpdateKind::TurnItem).contains(&item.id)
    {
        let id = allocate_event_id()?;
        let mut ended = item.clone();
        ended.status = completion.status.work_status();
        ended.completed_at = Some(completion.completed_at());
        ended.updated_at = completion.now.clone();
        if let TurnItemKind::Subagent { result, .. } = &mut ended.kind
            && let Some(next_result) = &input.result
        {
            *result = next_result.clone();
        }
        events.push(scope(id, EventPayload::TurnItem(ended)));
    }
    Ok(events)
}

/// End active runless roots and their unresolved work in projection order.
/// Only events present on entry suppress writes: newly planned events do not
/// change those exclusion sets, matching upstream's replay behavior.
pub fn end_runless_root_turns<Payload: Clone, Time: Clone, Error>(
    projection: &Projection<Payload, Time>,
    input: RootTurnsEnd<Time>,
    emitted: &[EmittedUpdate],
    allocate_event_id: &mut impl FnMut() -> Result<String, Error>,
) -> Result<Vec<Event<Payload, Time>>, Error> {
    let ended_nodes = written_ids(emitted, UpdateKind::Node);
    let ended_items = written_ids(emitted, UpdateKind::TurnItem);
    let ended_turns = written_ids(emitted, UpdateKind::ProviderTurn);
    let completion = &input.completion;
    let scope = |id, node_id: &str, payload| Event {
        id,
        thread_id: input.thread_id.clone(),
        run_id: None,
        node_id: node_id.to_string(),
        provider_instance_id: input.provider_instance_id.clone(),
        occurred_at: completion.now.clone(),
        payload,
    };
    let mut events = Vec::new();
    for node in &projection.nodes {
        if node.kind != NodeKind::RootTurn || node.run_id.is_some() || !node.status.is_active() {
            continue;
        }
        if !ended_nodes.contains(&node.id) {
            let id = allocate_event_id()?;
            let mut ended = node.clone();
            ended.status = completion.status.work_status();
            ended.completed_at = Some(completion.completed_at());
            events.push(scope(id, &node.id, EventPayload::Node(ended)));
        }
        for item in &projection.turn_items {
            if item.node_id.as_deref() != Some(node.id.as_str())
                || item.run_id.is_some()
                || !item.status.is_active()
                || ended_items.contains(&item.id)
            {
                continue;
            }
            let id = allocate_event_id()?;
            let mut ended = item.clone();
            ended.status = if completion.status == TerminalStatus::Completed
                && matches!(
                    ended.kind,
                    TurnItemKind::CommandExecution | TurnItemKind::DynamicTool
                ) {
                WorkStatus::Failed
            } else {
                completion.status.work_status()
            };
            ended.completed_at = Some(completion.completed_at());
            ended.updated_at = completion.now.clone();
            match &mut ended.kind {
                TurnItemKind::Reasoning { streaming }
                | TurnItemKind::AssistantMessage { streaming } => *streaming = false,
                _ => {}
            }
            events.push(scope(id, &node.id, EventPayload::TurnItem(ended)));
        }
        for provider_turn in &projection.provider_turns {
            if provider_turn.node_id != node.id
                || provider_turn.run_attempt_id.is_some()
                || !provider_turn.status.is_active()
                || ended_turns.contains(&provider_turn.id)
            {
                continue;
            }
            let id = allocate_event_id()?;
            let mut ended = provider_turn.clone();
            ended.status = completion.status.work_status();
            ended.completed_at = Some(completion.completed_at());
            events.push(scope(id, &node.id, EventPayload::ProviderTurn(ended)));
        }
    }
    Ok(events)
}
