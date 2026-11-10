//! Direct Rust port of `packages/shared/src/orchestrationV2DedupedBodies.ts`.
//! Source: pingdotgg/t3code at 521ac7c218b3faa74281fb72fc36ce74a0951fb3.
//! Copyright (c) 2026 T3 Tools Inc., MIT license.

use std::cell::OnceCell;
use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Value, json};

use crate::types::{Item, Projection, ThreadProjection};

/// Records whose bodies can be shared with their timeline items.
pub trait BodySource {
    fn message_text(&self, id: &str) -> Option<&str>;
    fn checkpoint_files(&self, id: &str) -> Option<&Value>;
    fn subagent(&self, id: &str) -> Option<&Value>;
}

fn marked(value: &Value, field: &str) -> bool {
    value.get(field) == Some(&Value::Bool(true))
}

/// Also used for a repeated `subagent.updated`, whose previous record was sent earlier.
pub fn dedupe_subagent_bodies(value: Item, previous: Option<&Value>) -> Item {
    let Some(previous) = previous else {
        return value;
    };
    let prompt = value.get("prompt").and_then(Value::as_str).is_some_and(|text| {
        !text.is_empty() && previous.get("prompt").and_then(Value::as_str) == Some(text)
    });
    let result = value.get("result").is_some_and(|result| {
        !result.is_null() && previous.get("result") == Some(result)
    });
    if !prompt && !result {
        return value;
    }
    let Some(mut next) = value.as_object().cloned() else {
        return value;
    };
    if prompt {
        next.insert("prompt".into(), json!(""));
        next.insert("promptOmitted".into(), json!(true));
    }
    if result {
        next.insert("result".into(), Value::Null);
        next.insert("resultOmitted".into(), json!(true));
    }
    Arc::new(Value::Object(next))
}

/// A missing or still-marked source field cannot restore a marked body.
pub fn restore_subagent_bodies(value: Item, previous: Option<&Value>) -> Item {
    let Some(previous) = previous else {
        return value;
    };
    let prompt = marked(&value, "promptOmitted") && !marked(previous, "promptOmitted");
    let result = marked(&value, "resultOmitted") && !marked(previous, "resultOmitted");
    if !prompt && !result {
        return value;
    }
    let Some(mut next) = value.as_object().cloned() else {
        return value;
    };
    next.remove("promptOmitted");
    next.remove("resultOmitted");
    for (restore, field, marker) in [
        (prompt, "prompt", "promptOmitted"),
        (result, "result", "resultOmitted"),
    ] {
        if restore {
            if let Some(body) = previous.get(field) {
                next.insert(field.into(), body.clone());
            } else {
                // JavaScript's object spread sets an absent source field to undefined.
                // Its JSON representation omits that property.
                next.remove(field);
            }
        } else if marked(&value, marker) {
            next.insert(marker.into(), json!(true));
        }
    }
    Arc::new(Value::Object(next))
}

// Schema.toEquivalence(Array(OrchestrationV2CheckpointFileSummary)) compares
// these four schema fields, not arbitrary extra properties on a file record.
// JSON numbers are compared as JavaScript numbers, including 1 versus 1.0.
fn same_checkpoint_files(left: &Value, right: &Value) -> bool {
    let (Some(left), Some(right)) = (left.as_array(), right.as_array()) else {
        return false;
    };
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            ["path", "kind"].iter().all(|field| {
                left.get(field).and_then(Value::as_str).is_some_and(|value| {
                    right.get(field).and_then(Value::as_str) == Some(value)
                })
            }) && ["additions", "deletions"].iter().all(|field| {
                left.get(field).and_then(Value::as_f64).is_some_and(|value| {
                    right.get(field).and_then(Value::as_f64) == Some(value)
                })
            })
        })
}

pub fn dedupe_turn_item_bodies(item: Item, source: &dyn BodySource) -> Item {
    let mut next = None;
    match item.get("type").and_then(Value::as_str) {
        Some("user_message" | "assistant_message") => {
            if let (Some(text), Some(id)) = (
                item.get("text").and_then(Value::as_str),
                item.get("messageId").and_then(Value::as_str),
            ) {
                if !text.is_empty() && source.message_text(id) == Some(text) {
                    next = Some(("text", json!(""), "textOmitted"));
                }
            }
        }
        Some("checkpoint") => {
            if let (Some(files), Some(id)) = (
                item.get("files"),
                item.get("checkpointId").and_then(Value::as_str),
            ) {
                let record = source.checkpoint_files(id);
                if files.as_array().is_some_and(|files| !files.is_empty())
                    && record.is_some_and(|record| same_checkpoint_files(record, files))
                {
                    next = Some(("files", json!([]), "filesOmitted"));
                }
            }
        }
        Some("subagent") => {
            let previous = item
                .get("subagentId")
                .and_then(Value::as_str)
                .and_then(|id| source.subagent(id));
            return dedupe_subagent_bodies(item, previous);
        }
        _ => {}
    }
    let Some((field, value, marker)) = next else {
        return item;
    };
    let mut next = item.as_object().expect("typed turn item").clone();
    next.insert(field.into(), value);
    next.insert(marker.into(), json!(true));
    Arc::new(Value::Object(next))
}

pub fn restore_turn_item_bodies(item: Item, source: &dyn BodySource) -> Item {
    let next = match item.get("type").and_then(Value::as_str) {
        Some("user_message" | "assistant_message") if marked(&item, "textOmitted") => item
            .get("messageId")
            .and_then(Value::as_str)
            .and_then(|id| source.message_text(id))
            .map(|text| ("text", json!(text), "textOmitted")),
        Some("checkpoint") if marked(&item, "filesOmitted") => item
            .get("checkpointId")
            .and_then(Value::as_str)
            .and_then(|id| source.checkpoint_files(id))
            .map(|files| ("files", files.clone(), "filesOmitted")),
        Some("subagent") if marked(&item, "promptOmitted") || marked(&item, "resultOmitted") => {
            let previous = item
                .get("subagentId")
                .and_then(Value::as_str)
                .and_then(|id| source.subagent(id));
            return restore_subagent_bodies(item, previous);
        }
        _ => None,
    };
    let Some((field, value, marker)) = next else {
        return item;
    };
    let mut next = item.as_object().expect("typed turn item").clone();
    next.remove(marker);
    next.insert(field.into(), value);
    Arc::new(Value::Object(next))
}

/// The source builds each index only on its first lookup.
struct SnapshotBodySource<'a> {
    projection: &'a ThreadProjection,
    messages: OnceCell<HashMap<&'a str, &'a str>>,
    checkpoints: OnceCell<HashMap<&'a str, &'a Value>>,
    subagents: OnceCell<HashMap<&'a str, &'a Value>>,
}

impl<'a> SnapshotBodySource<'a> {
    fn new(projection: &'a ThreadProjection) -> Self {
        Self {
            projection,
            messages: OnceCell::new(),
            checkpoints: OnceCell::new(),
            subagents: OnceCell::new(),
        }
    }
}

impl BodySource for SnapshotBodySource<'_> {
    fn message_text(&self, id: &str) -> Option<&str> {
        self.messages
            .get_or_init(|| {
                self.projection
                    .messages
                    .iter()
                    .filter_map(|message| Some((message.get("id")?.as_str()?, message.get("text")?.as_str()?)))
                    .collect()
            })
            .get(id)
            .copied()
    }

    fn checkpoint_files(&self, id: &str) -> Option<&Value> {
        self.checkpoints
            .get_or_init(|| {
                self.projection
                    .checkpoints
                    .iter()
                    .filter_map(|checkpoint| Some((checkpoint.get("id")?.as_str()?, checkpoint.get("files")?)))
                    .collect()
            })
            .get(id)
            .copied()
    }

    fn subagent(&self, id: &str) -> Option<&Value> {
        self.subagents
            .get_or_init(|| {
                self.projection
                    .subagents
                    .iter()
                    .filter_map(|subagent| Some((subagent.get("id")?.as_str()?, subagent)))
                    .collect()
            })
            .get(id)
            .copied()
    }
}

/// Map each local item once, retaining sharing between both timeline arrays.
fn map_local_turn_items(projection: Projection, map: impl Fn(Item) -> Item) -> Projection {
    let mut mapped = HashMap::<*const Value, Item>::new();
    let mut changed = false;
    let mut once = |item: &Item| {
        mapped
            .entry(Arc::as_ptr(item))
            .or_insert_with(|| {
                let next = map(item.clone());
                changed |= !Arc::ptr_eq(&next, item);
                next
            })
            .clone()
    };
    let visible_turn_items = projection
        .visible_turn_items
        .iter()
        .map(|row| {
            if row.visibility != "local"
                && Some(row.source_thread_id.as_str()) != projection.thread.get("id").and_then(Value::as_str)
            {
                return row.clone();
            }
            let item = once(&row.item);
            if Arc::ptr_eq(&item, &row.item) {
                row.clone()
            } else {
                let mut next = row.as_ref().clone();
                next.item = item;
                Arc::new(next)
            }
        })
        .collect();
    let turn_items = projection.turn_items.iter().map(&mut once).collect();
    if !changed {
        return projection;
    }
    let mut next = projection.as_ref().clone();
    next.visible_turn_items = visible_turn_items;
    next.turn_items = turn_items;
    Arc::new(next)
}

pub fn dedupe_snapshot_bodies(projection: Projection) -> Projection {
    let source = SnapshotBodySource::new(&projection);
    map_local_turn_items(projection.clone(), |item| dedupe_turn_item_bodies(item, &source))
}

/// Restore each decoded snapshot before reducers or caches see it.
pub fn restore_snapshot_bodies(projection: Projection) -> Projection {
    let source = SnapshotBodySource::new(&projection);
    map_local_turn_items(projection.clone(), |item| restore_turn_item_bodies(item, &source))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bounded_snapshot::{bounded_snapshot_projection, omit_local_visible_turn_items};
    use crate::types::ProjectedTurnItem;
    use serde_json::Map;

    fn item(id: &str, fields: Value) -> Item {
        let mut value = json!({
            "id":id,"threadId":"thread:bodies","runId":null,"nodeId":null,
            "providerThreadId":null,"providerTurnId":null,"nativeItemRef":null,
            "parentItemId":null,"ordinal":1,"status":"completed","title":null,
            "startedAt":"2026-10-01T00:00:00.000Z","completedAt":"2026-10-01T00:00:00.000Z",
            "updatedAt":"2026-10-01T00:00:00.000Z"
        });
        value.as_object_mut().unwrap().extend(fields.as_object().unwrap().clone());
        Arc::new(value)
    }

    fn row(visibility: &str, source_thread_id: &str, item: Item, position: usize) -> Arc<ProjectedTurnItem> {
        let mut extra = Map::new();
        extra.insert("position".into(), json!(position));
        extra.insert("sourceItemId".into(), item["id"].clone());
        Arc::new(ProjectedTurnItem { visibility:visibility.into(), source_thread_id:source_thread_id.into(), item, extra })
    }

    fn fixture() -> Projection {
        let files = json!([{"path":"src/a.ts","kind":"modified","additions":3,"deletions":1}]);
        let local = vec![
            item("user",json!({"type":"user_message","messageId":"m-user","text":"hello","attachments":[],"inputIntent":"turn_start","createdBy":"user","creationSource":"web"})),
            item("assistant",json!({"type":"assistant_message","messageId":"m-assistant","text":"hi there","streaming":false})),
            item("stale",json!({"type":"assistant_message","messageId":"m-stale","text":"older text","streaming":false})),
            item("empty",json!({"type":"user_message","messageId":"m-empty","text":""})),
            item("checkpoint",json!({"type":"checkpoint","checkpointId":"c-1","scopeId":"scope","files":files})),
            item("orphan-checkpoint",json!({"type":"checkpoint","checkpointId":"c-missing","scopeId":"scope","files":files})),
            item("no-files",json!({"type":"checkpoint","checkpointId":"c-empty","scopeId":"scope","files":[]})),
            item("task",json!({"type":"subagent","subagentId":"node:task","origin":"app_owned","driver":"codex","providerInstanceId":"codex","childThreadId":null,"prompt":"do the task","result":"partial result"})),
        ];
        let inherited = item("inherited",json!({"threadId":"thread:bodies-parent","type":"assistant_message","messageId":"m-assistant","text":"hi there","streaming":false}));
        let mut visible_turn_items = vec![row("inherited","thread:bodies-parent",inherited,0)];
        visible_turn_items.extend(local.iter().enumerate().map(|(i,item)|row("local","thread:bodies",item.clone(),i+1)));
        Arc::new(ThreadProjection {
            thread:json!({"id":"thread:bodies"}),
            messages:vec![
                json!({"id":"m-user","text":"hello"}),
                json!({"id":"m-assistant","text":"hi there"}),
                json!({"id":"m-stale","text":"newer text"}),
                json!({"id":"m-empty","text":""}),
            ],
            checkpoints:vec![json!({"id":"c-1","files":files}),json!({"id":"c-empty","files":[]})],
            subagents:vec![json!({"id":"node:task","prompt":"do the task","result":"final result"})],
            context_handoffs:vec![], turn_items:local, visible_turn_items, extra:Map::new(),
        })
    }

    fn wire(projection: &Projection) -> Value {
        serde_json::to_value(projection.as_ref()).unwrap()
    }

    #[test]
    fn upstream_blanks_only_bodies_equal_to_records_and_restores_exactly() {
        let projection = fixture();
        let deduped = dedupe_snapshot_bodies(projection.clone());
        let markers: Map<String, Value> = deduped.visible_turn_items.iter().filter_map(|row| {
            let markers:Vec<_> = ["textOmitted","filesOmitted","promptOmitted","resultOmitted"].into_iter().filter(|marker|row.item.get(marker).is_some()).collect();
            (!markers.is_empty()).then(||(row.item["id"].as_str().unwrap().to_owned(),json!(markers)))
        }).collect();
        assert_eq!(Value::Object(markers),json!({"user":["textOmitted"],"assistant":["textOmitted"],"checkpoint":["filesOmitted"],"task":["promptOmitted"]}));
        assert_eq!(wire(&restore_snapshot_bodies(deduped.clone())), wire(&projection));
        assert!(Arc::ptr_eq(&restore_snapshot_bodies(projection.clone()),&projection));
        assert!(Arc::ptr_eq(&deduped.visible_turn_items[0],&projection.visible_turn_items[0]));
        for index in [2,3,5,6] {
            assert!(Arc::ptr_eq(&deduped.turn_items[index],&projection.turn_items[index]));
        }
    }

    #[test]
    fn upstream_shared_items_remain_shared_so_both_compactions_round_trip() {
        let projection = fixture();
        let deduped = dedupe_snapshot_bodies(projection.clone());
        for (index,item) in deduped.turn_items.iter().enumerate() {
            assert!(Arc::ptr_eq(item,&deduped.visible_turn_items[index+1].item));
        }
        let compact = omit_local_visible_turn_items(deduped).unwrap();
        assert!(compact.turn_items.is_empty());
        assert_eq!(wire(&bounded_snapshot_projection(compact,true)),wire(&projection));
    }

    #[test]
    fn upstream_subagent_restore_leaves_a_still_marked_source_field_alone() {
        let marked = Arc::new(json!({"prompt":"","result":null,"promptOmitted":true,"resultOmitted":true}));
        let previous = json!({"prompt":"","result":"done","promptOmitted":true});
        assert_eq!(*restore_subagent_bodies(marked,Some(&previous)),json!({"prompt":"","result":"done","promptOmitted":true}));
    }

    #[test]
    fn missing_sources_and_non_boolean_markers_preserve_identity() {
        let projection = fixture();
        let source = SnapshotBodySource::new(&projection);
        for marker in [Value::Null,json!(false),json!(1),json!("true"),json!({})] {
            let message = item("marked",json!({"type":"assistant_message","messageId":"m-assistant","text":"","textOmitted":marker}));
            assert!(Arc::ptr_eq(&restore_turn_item_bodies(message.clone(),&source),&message));
        }
        for fields in [
            json!({"type":"assistant_message","messageId":"missing","text":"","textOmitted":true}),
            json!({"type":"checkpoint","checkpointId":"missing","files":[],"filesOmitted":true}),
            json!({"type":"subagent","subagentId":"missing","prompt":"","result":null,"promptOmitted":true,"resultOmitted":true}),
            json!({"type":"future_item","text":"","textOmitted":true}),
        ] {
            let value = item("orphan",fields);
            assert!(Arc::ptr_eq(&restore_turn_item_bodies(value.clone(),&source),&value));
        }
    }

    #[test]
    fn checkpoint_equivalence_uses_exact_schema_fields_and_array_order() {
        let projection = fixture();
        let source = SnapshotBodySource::new(&projection);
        let matching = item("c",json!({"type":"checkpoint","checkpointId":"c-1","files":[{"path":"src/a.ts","kind":"modified","additions":3.0,"deletions":1,"future":"not in schema"}],"future":{"keep":true}}));
        let omitted = dedupe_turn_item_bodies(matching,&source);
        assert_eq!(omitted["filesOmitted"],json!(true));
        assert_eq!(omitted["future"],json!({"keep":true}));
        for files in [
            json!([{"path":"src/b.ts","kind":"modified","additions":3,"deletions":1}]),
            json!([{"path":"src/a.ts","kind":"added","additions":3,"deletions":1}]),
            json!([{"path":"src/a.ts","kind":"modified","additions":4,"deletions":1}]),
            json!([{"path":"src/a.ts","kind":"modified","additions":3,"deletions":2}]),
            json!([]),
        ] {
            let value = item("c",json!({"type":"checkpoint","checkpointId":"c-1","files":files}));
            assert!(Arc::ptr_eq(&dedupe_turn_item_bodies(value.clone(),&source),&value));
        }
        let mut pairs = projection.as_ref().clone();
        pairs.checkpoints.push(json!({"id":"pair","files":[{"path":"a","kind":"added","additions":1,"deletions":0},{"path":"b","kind":"modified","additions":0,"deletions":2}]}));
        let source = SnapshotBodySource::new(&pairs);
        let reversed = item("c",json!({"type":"checkpoint","checkpointId":"pair","files":[{"path":"b","kind":"modified","additions":0,"deletions":2},{"path":"a","kind":"added","additions":1,"deletions":0}]}));
        assert!(Arc::ptr_eq(&dedupe_turn_item_bodies(reversed.clone(),&source),&reversed));
    }

    #[test]
    fn subagent_bodies_dedupe_independently_and_keep_unknown_fields() {
        let previous = json!({"prompt":"same","result":"done"});
        let value = Arc::new(json!({"prompt":"same","result":"done","future":[1,2]}));
        let omitted = dedupe_subagent_bodies(value.clone(),Some(&previous));
        assert_eq!(*omitted,json!({"prompt":"","result":null,"promptOmitted":true,"resultOmitted":true,"future":[1,2]}));
        assert_eq!(*restore_subagent_bodies(omitted,Some(&previous)),*value);
        let empty = Arc::new(json!({"prompt":"","result":null}));
        assert!(Arc::ptr_eq(&dedupe_subagent_bodies(empty.clone(),Some(&json!({"prompt":"","result":null}))),&empty));
        assert!(Arc::ptr_eq(&restore_subagent_bodies(value.clone(),None),&value));
    }

    #[test]
    fn marked_empty_bodies_restore_and_duplicate_source_ids_use_last_record() {
        let mut projection = fixture().as_ref().clone();
        projection.messages.push(json!({"id":"m-assistant","text":"last"}));
        let source = SnapshotBodySource::new(&projection);
        let message = item("marked",json!({"type":"assistant_message","messageId":"m-assistant","text":"","textOmitted":true,"future":{"keep":true}}));
        let restored = restore_turn_item_bodies(message,&source);
        assert_eq!(restored["text"],json!("last"));
        assert_eq!(restored["future"],json!({"keep":true}));
        assert!(restored.get("textOmitted").is_none());
        let empty_message = item("empty",json!({"type":"user_message","messageId":"m-empty","text":"","textOmitted":true}));
        let restored = restore_turn_item_bodies(empty_message,&source);
        assert_eq!(restored["text"],json!(""));
        assert!(restored.get("textOmitted").is_none());
        let checkpoint = item("empty",json!({"type":"checkpoint","checkpointId":"c-empty","files":[],"filesOmitted":true}));
        assert!(restore_turn_item_bodies(checkpoint,&source).get("filesOmitted").is_none());
    }

    #[test]
    fn own_thread_rows_are_local_and_equal_but_distinct_items_do_not_gain_identity() {
        let mut projection = fixture().as_ref().clone();
        let own = projection.turn_items[0].clone();
        let separate = Arc::new(own.as_ref().clone());
        projection.visible_turn_items = vec![row("inherited","thread:bodies",separate,0)];
        projection.turn_items = vec![own];
        let omitted = dedupe_snapshot_bodies(Arc::new(projection));
        assert_eq!(omitted.visible_turn_items[0].item["textOmitted"],json!(true));
        assert!(!Arc::ptr_eq(&omitted.visible_turn_items[0].item,&omitted.turn_items[0]));
        assert!(omit_local_visible_turn_items(omitted).is_none());
    }
}
