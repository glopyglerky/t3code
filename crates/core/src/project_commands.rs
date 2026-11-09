//! Project command decisions from apps/server/src/orchestration-v2/ProjectCommands.ts.
//! Copyright (c) 2026 T3 Tools Inc. Distributed under the repository's MIT license.

use std::collections::HashSet;

pub const MAX_SCRIPT_ID_LENGTH: usize = 24;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Script<Payload> {
    pub id: String,
    pub payload: Payload,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Icon<Payload> {
    /// The host's Intl.Segmenter count, present only for a monogram.
    pub monogram_graphemes: Option<u64>,
    pub payload: Payload,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MetadataChanges<Payload> {
    pub title: Option<String>,
    pub workspace_root: Option<String>,
    pub default_model_selection: Option<Option<Payload>>,
    pub default_thread_env_mode: Option<Option<String>>,
    pub auto_pull: Option<bool>,
    pub favicon_path: Option<Option<String>>,
    pub project_icon: Option<Option<Icon<Payload>>>,
    pub scripts: Option<Vec<Script<Payload>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CommandKind<Payload> {
    Create {
        title: String,
        workspace_root: String,
        scripts: Option<Vec<Script<Payload>>>,
    },
    Update {
        changes: MetadataChanges<Payload>,
        pinned: Option<bool>,
        archived: Option<bool>,
    },
    Delete,
}

impl<Payload> CommandKind<Payload> {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Create { .. } => "project.create",
            Self::Update { .. } => "project.meta.update",
            Self::Delete => "project.delete",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Command<Payload> {
    pub command_id: String,
    pub project_id: String,
    pub kind: CommandKind<Payload>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Project {
    pub deleted_at: Option<String>,
    pub script_ids: Vec<String>,
    pub pinned_at: Option<String>,
    pub archived_at: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct State {
    pub project: Option<Project>,
    pub workspace_owner_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventKind<Payload> {
    Created {
        title: String,
        workspace_root: String,
        scripts: Vec<Script<Payload>>,
    },
    Updated {
        changes: MetadataChanges<Payload>,
        pinned_at: Option<Option<String>>,
        archived_at: Option<Option<String>>,
    },
    Deleted,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event<Payload> {
    pub event_id: String,
    pub project_id: String,
    pub command_id: String,
    pub occurred_at: String,
    pub kind: EventKind<Payload>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rejection {
    Invariant {
        command_type: &'static str,
        detail: String,
    },
    MissingProject {
        command_type: &'static str,
        project_id: String,
    },
    WorkspaceConflict {
        workspace_root: String,
        conflicting_project_id: String,
    },
}

fn workspace_available(
    project_id: &str,
    state: &State,
    workspace_root: &str,
) -> Result<(), Rejection> {
    if let Some(owner) = &state.workspace_owner_id
        && owner != project_id
    {
        return Err(Rejection::WorkspaceConflict {
            workspace_root: workspace_root.to_string(),
            conflicting_project_id: owner.clone(),
        });
    }
    Ok(())
}

fn script_id_allowed(id: &str) -> bool {
    let bytes = id.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_SCRIPT_ID_LENGTH {
        return false;
    }
    let first = bytes[0];
    (first.is_ascii_lowercase() || first.is_ascii_digit())
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

/// Plans one event without mutating the supplied state or allocating any identifiers.
/// `occurred_at` is the wire adapter's DateTime.formatIso-compatible UTC timestamp.
pub fn plan<Payload: Clone>(
    command: &Command<Payload>,
    state: &State,
    event_id: &str,
    occurred_at: &str,
) -> Result<Event<Payload>, Rejection> {
    let invariant = |detail| Rejection::Invariant {
        command_type: command.kind.name(),
        detail,
    };
    let active = state
        .project
        .as_ref()
        .filter(|project| project.deleted_at.is_none());
    let missing = || Rejection::MissingProject {
        command_type: command.kind.name(),
        project_id: command.project_id.clone(),
    };

    let kind = match &command.kind {
        CommandKind::Create {
            title,
            workspace_root,
            scripts,
        } => {
            if state.project.is_some() {
                return Err(invariant(format!(
                    "Project '{}' already exists and cannot be created twice.",
                    command.project_id
                )));
            }
            workspace_available(&command.project_id, state, workspace_root)?;
            EventKind::Created {
                title: title.clone(),
                workspace_root: workspace_root.clone(),
                // Upstream validates newly introduced script IDs only on metadata updates.
                scripts: scripts.clone().unwrap_or_default(),
            }
        }
        CommandKind::Update {
            changes,
            pinned,
            archived,
        } => {
            let project = active.ok_or_else(missing)?;
            if changes
                .project_icon
                .as_ref()
                .and_then(Option::as_ref)
                .and_then(|icon| icon.monogram_graphemes)
                .is_some_and(|count| count > 2)
            {
                return Err(invariant(
                    "Project monograms must contain at most two characters.".to_string(),
                ));
            }
            if let Some(scripts) = &changes.scripts {
                let existing_ids: HashSet<&str> =
                    project.script_ids.iter().map(String::as_str).collect();
                for script in scripts {
                    if !existing_ids.contains(script.id.as_str()) && !script_id_allowed(&script.id)
                    {
                        let length: usize = script.id.chars().map(char::len_utf16).sum();
                        return Err(invariant(format!(
                            "Script IDs must be 1-{MAX_SCRIPT_ID_LENGTH} lowercase letters, digits or hyphens, starting with a letter or digit (got {length} characters)."
                        )));
                    }
                }
            }
            if let Some(workspace_root) = &changes.workspace_root {
                workspace_available(&command.project_id, state, workspace_root)?;
            }
            let timestamp = |enabled: bool, before: &Option<String>| {
                if enabled {
                    Some(before.clone().unwrap_or_else(|| occurred_at.to_string()))
                } else {
                    None
                }
            };
            EventKind::Updated {
                changes: changes.clone(),
                pinned_at: pinned.map(|enabled| timestamp(enabled, &project.pinned_at)),
                archived_at: archived.map(|enabled| timestamp(enabled, &project.archived_at)),
            }
        }
        CommandKind::Delete => {
            active.ok_or_else(missing)?;
            EventKind::Deleted
        }
    };
    Ok(Event {
        event_id: event_id.to_string(),
        project_id: command.project_id.clone(),
        command_id: command.command_id.clone(),
        occurred_at: occurred_at.to_string(),
        kind,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project() -> Project {
        Project {
            deleted_at: None,
            script_ids: vec!["legacy.invalid".to_string()],
            pinned_at: Some("first-pin".to_string()),
            archived_at: None,
        }
    }

    fn command(kind: CommandKind<()>) -> Command<()> {
        Command {
            command_id: "command".to_string(),
            project_id: "project".to_string(),
            kind,
        }
    }

    #[test]
    fn updates_keep_legacy_ids_and_original_pin_time() {
        let input = command(CommandKind::Update {
            changes: MetadataChanges {
                scripts: Some(vec![Script {
                    id: "legacy.invalid".to_string(),
                    payload: (),
                }]),
                ..MetadataChanges::default()
            },
            pinned: Some(true),
            archived: Some(true),
        });
        let state = State {
            project: Some(project()),
            workspace_owner_id: None,
        };
        let event = plan(&input, &state, "event", "now").unwrap();
        let EventKind::Updated {
            pinned_at,
            archived_at,
            ..
        } = event.kind
        else {
            panic!("metadata event")
        };
        assert_eq!(pinned_at, Some(Some("first-pin".to_string())));
        assert_eq!(archived_at, Some(Some("now".to_string())));
        assert_eq!(state.project, Some(project()));
    }

    #[test]
    fn first_new_invalid_script_reports_utf16_length() {
        let input = command(CommandKind::Update {
            changes: MetadataChanges {
                scripts: Some(vec![
                    Script {
                        id: "🦀a".to_string(),
                        payload: (),
                    },
                    Script {
                        id: "second.invalid".to_string(),
                        payload: (),
                    },
                ]),
                workspace_root: Some("/taken".to_string()),
                ..MetadataChanges::default()
            },
            pinned: None,
            archived: None,
        });
        let rejection = plan(
            &input,
            &State {
                project: Some(project()),
                workspace_owner_id: Some("other".to_string()),
            },
            "event",
            "now",
        )
        .unwrap_err();
        assert_eq!(
            rejection,
            Rejection::Invariant {
                command_type: "project.meta.update",
                detail: "Script IDs must be 1-24 lowercase letters, digits or hyphens, starting with a letter or digit (got 3 characters).".to_string(),
            }
        );
    }

    #[test]
    fn missing_project_precedes_monogram_and_workspace_checks() {
        let input = command(CommandKind::Update {
            changes: MetadataChanges {
                project_icon: Some(Some(Icon {
                    monogram_graphemes: Some(3),
                    payload: (),
                })),
                workspace_root: Some("/taken".to_string()),
                ..MetadataChanges::default()
            },
            pinned: None,
            archived: None,
        });
        assert_eq!(
            plan(&input, &State::default(), "event", "now"),
            Err(Rejection::MissingProject {
                command_type: "project.meta.update",
                project_id: "project".to_string(),
            })
        );
    }

    #[test]
    fn create_cannot_reuse_a_deleted_project_id() {
        let input = command(CommandKind::Create {
            title: "Restored".to_string(),
            workspace_root: "/taken".to_string(),
            scripts: None,
        });
        let mut deleted = project();
        deleted.deleted_at = Some("deleted".to_string());
        assert_eq!(
            plan(
                &input,
                &State {
                    project: Some(deleted),
                    workspace_owner_id: Some("other".to_string()),
                },
                "event",
                "now",
            ),
            Err(Rejection::Invariant {
                command_type: "project.create",
                detail: "Project 'project' already exists and cannot be created twice.".to_string(),
            })
        );
    }
}
