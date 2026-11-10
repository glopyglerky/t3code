//! Direct Rust port of `packages/shared/src/orchestrationV2BoundedSnapshot.ts`.
//! Source: pingdotgg/t3code at 521ac7c218b3faa74281fb72fc36ce74a0951fb3.
//! Copyright (c) 2026 T3 Tools Inc., MIT license.

use std::sync::Arc;

use serde_json::Value;

use crate::deduped_bodies::restore_snapshot_bodies;
use crate::types::{ProjectedTurnItem, Projection, ThreadProjection};

fn is_local_timeline_row(projection: &ThreadProjection, row: &ProjectedTurnItem) -> bool {
    row.visibility == "local"
        || Some(row.source_thread_id.as_str()) == projection.thread.get("id").and_then(Value::as_str)
}

pub fn omit_local_visible_turn_items(projection: Projection) -> Option<Projection> {
    let mut omitted = 0;
    for row in &projection.visible_turn_items {
        if !is_local_timeline_row(&projection, row) {
            continue;
        }
        // Identity, not structural equality, makes the restored entry exact.
        if projection
            .turn_items
            .get(omitted)
            .is_none_or(|item| !Arc::ptr_eq(item, &row.item))
        {
            return None;
        }
        omitted += 1;
    }
    if omitted == 0 {
        return None;
    }
    let mut next = projection.as_ref().clone();
    next.turn_items = projection.turn_items[omitted..].to_vec();
    Some(Arc::new(next))
}

/// Call only when the wire's turnItemsOmitLocalVisible flag is true.
pub fn restore_local_visible_turn_items(projection: Projection) -> Projection {
    let local: Vec<_> = projection
        .visible_turn_items
        .iter()
        .filter(|row| is_local_timeline_row(&projection, row))
        .map(|row| row.item.clone())
        .collect();
    if local.is_empty() {
        return projection;
    }
    let mut next = projection.as_ref().clone();
    next.turn_items = local;
    next.turn_items.extend(projection.turn_items.iter().cloned());
    Arc::new(next)
}

/// Undo both upstream opt-in compactions. Body fields carry their own markers.
pub fn bounded_snapshot_projection(
    projection: Projection,
    turn_items_omit_local_visible: bool,
) -> Projection {
    restore_snapshot_bodies(if turn_items_omit_local_visible {
        restore_local_visible_turn_items(projection)
    } else {
        projection
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Item;
    use serde_json::{Map, json};

    fn item(thread_id: &str, id: &str, ordinal: u64, kind: &str) -> Item {
        Arc::new(json!({
            "id":id,"type":kind,"threadId":thread_id,
            "runId":if kind == "command_execution" {Value::Null} else {json!("run:compact")},
            "nodeId":null,"providerThreadId":null,"providerTurnId":null,
            "nativeItemRef":null,"parentItemId":null,"ordinal":ordinal,
            "status":"completed","title":id,"input":id,"exitCode":0,
            "startedAt":null,"completedAt":null,"updatedAt":"2026-10-01T00:00:00.000Z"
        }))
    }

    fn row(visibility: &str, source_thread_id: &str, item: Item) -> Arc<ProjectedTurnItem> {
        let mut extra = Map::new();
        extra.insert("sourceItemId".into(),item["id"].clone());
        Arc::new(ProjectedTurnItem { visibility:visibility.into(),source_thread_id:source_thread_id.into(),item,extra })
    }

    fn projection(visible:Vec<Arc<ProjectedTurnItem>>, turn_items:Vec<Item>) -> Projection {
        let visible_turn_items = visible.into_iter().enumerate().map(|(position,row)|{
            let mut row = row.as_ref().clone();
            row.extra.insert("position".into(),json!(position));
            Arc::new(row)
        }).collect();
        Arc::new(ThreadProjection {thread:json!({"id":"thread:compact-child"}),messages:vec![],checkpoints:vec![],subagents:vec![],context_handoffs:vec![],turn_items,visible_turn_items,extra:Map::new()})
    }

    fn fork_window() -> Projection {
        let mut visible = vec![
            row("inherited","thread:compact-parent",item("thread:compact-parent","parent-1",1,"command_execution")),
            row("inherited","thread:compact-parent",item("thread:compact-parent","parent-2",2,"command_execution")),
            row("synthetic","thread:compact-parent",item("thread:compact-parent","fork-marker",3,"fork")),
        ];
        let local = vec![
            item("thread:compact-child","child-1",1,"command_execution"),
            item("thread:compact-child","child-2",2,"command_execution"),
            item("thread:compact-child","child-3",3,"command_execution"),
        ];
        visible.extend(local.iter().cloned().map(|item|row("local","thread:compact-child",item)));
        let mut retained = local;
        retained.push(item("thread:compact-child","interrupt-request",0,"run_interrupt_request"));
        projection(visible,retained)
    }

    fn wire(projection:&Projection)->Value {
        serde_json::to_value(projection.as_ref()).unwrap()
    }

    #[test]
    fn upstream_compaction_round_trips_through_separately_decoded_json() {
        let full = fork_window();
        let compact = omit_local_visible_turn_items(full.clone()).unwrap();
        assert_eq!(compact.turn_items.len(),1);
        assert_eq!(compact.turn_items[0]["id"],json!("interrupt-request"));
        let decoded:ThreadProjection = serde_json::from_value(wire(&compact)).unwrap();
        let restored = bounded_snapshot_projection(Arc::new(decoded),true);
        assert_eq!(wire(&restored),wire(&full));
        for index in 0..3 {
            assert!(Arc::ptr_eq(&restored.turn_items[index],&restored.visible_turn_items[index+3].item));
        }
    }

    #[test]
    fn upstream_treats_rows_sourced_from_the_same_thread_as_local() {
        let own = item("thread:compact-child","own",1,"command_execution");
        let full = projection(vec![row("inherited","thread:compact-child",own.clone())],vec![own]);
        let compact = omit_local_visible_turn_items(full.clone()).unwrap();
        assert!(compact.turn_items.is_empty());
        assert_eq!(wire(&restore_local_visible_turn_items(compact)),wire(&full));
    }

    #[test]
    fn upstream_refuses_reordered_structurally_equal_or_short_prefixes() {
        let first = item("thread:compact-child","a",1,"command_execution");
        let second = item("thread:compact-child","b",2,"command_execution");
        let visible = vec![row("local","thread:compact-child",first.clone()),row("local","thread:compact-child",second.clone())];
        for items in [
            vec![second.clone(),first.clone()],
            vec![Arc::new(first.as_ref().clone()),second.clone()],
            vec![first.clone()],
        ] {
            assert!(omit_local_visible_turn_items(projection(visible.clone(),items)).is_none());
        }
    }

    #[test]
    fn empty_or_inherited_only_windows_remain_unchanged() {
        let inherited = item("thread:compact-parent","parent",1,"command_execution");
        for full in [
            projection(vec![],vec![]),
            projection(vec![row("inherited","thread:compact-parent",inherited.clone())],vec![inherited]),
        ] {
            assert!(omit_local_visible_turn_items(full.clone()).is_none());
            assert!(Arc::ptr_eq(&restore_local_visible_turn_items(full.clone()),&full));
            assert!(Arc::ptr_eq(&bounded_snapshot_projection(full.clone(),false),&full));
        }
    }

    #[test]
    fn unmarked_snapshot_does_not_prepend_visible_items_and_keeps_unknown_fields() {
        let mut full = fork_window().as_ref().clone();
        full.extra.insert("future".into(),json!({"nested":[1,2]}));
        let full = Arc::new(full);
        assert!(Arc::ptr_eq(&bounded_snapshot_projection(full.clone(),false),&full));
        let compact = omit_local_visible_turn_items(full.clone()).unwrap();
        assert_eq!(compact.extra["future"],json!({"nested":[1,2]}));
        assert_eq!(wire(&bounded_snapshot_projection(compact,true)),wire(&full));
    }
}
