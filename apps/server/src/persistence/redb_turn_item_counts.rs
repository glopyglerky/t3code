//! Migration 85's per-thread/run counts share the projection transaction.
use crate::application::{ApplicationCatalog, decode_row, row_prefix, table};
use crate::application_rows::{
    delete_row, int_cell, integer, managed_marker, nullable, read_row, text, text_cell,
    validate_table, write_row,
};
use crate::backend::Transaction;
use crate::{Error, Result, request_digest, store_key};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

pub(crate) const TABLE: &str = "orchestration_v2_projection_turn_item_counts";
pub(crate) const INDEX_NAMESPACE: &[u8] = b"projection-shell-counts/v1/";
const OWNER: &str = "projection-commit-v1";
const PAGE_ROWS: usize = 128;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct GroupWitness {
    thread_id: String,
    run_key: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ThreadGroups {
    thread_id: String,
    count: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Preparation {
    version: u32,
    generation: String,
    after: Option<String>,
    indexed_rows: u64,
    ready: bool,
}

pub(crate) struct GroupCount {
    pub thread_id: String,
    pub run_key: String,
    pub item_count: i64,
}

pub(crate) struct GroupPage {
    pub rows: Vec<GroupCount>,
    pub after: Option<Vec<u8>>,
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(crate::json_error)
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|_| Error::IncompatibleStore)
}

fn location(catalog: &ApplicationCatalog, suffix: &str) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/projection-shell-counts/v1/{suffix}",
        catalog.generation
    ))
}

fn group_prefix(catalog: &ApplicationCatalog, thread: &str) -> Vec<u8> {
    location(
        catalog,
        &format!("groups/{}/", request_digest(&json!(thread))),
    )
}

fn group_key(catalog: &ApplicationCatalog, witness: &GroupWitness) -> Vec<u8> {
    let mut key = group_prefix(catalog, &witness.thread_id);
    key.extend_from_slice(request_digest(&json!([witness.thread_id, witness.run_key])).as_bytes());
    key
}

fn thread_groups_key(catalog: &ApplicationCatalog, thread: &str) -> Vec<u8> {
    location(
        catalog,
        &format!("threads/{}", request_digest(&json!(thread))),
    )
}

pub(crate) async fn expected_groups(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    thread: &str,
) -> Result<u64> {
    let Some(bytes) = tx.get(&thread_groups_key(catalog, thread)).await? else {
        return Ok(0);
    };
    let witness: ThreadGroups = decode(&bytes)?;
    if witness.thread_id != thread || witness.count == 0 {
        return Err(Error::IncompatibleStore);
    }
    Ok(witness.count)
}

async fn adjust_groups(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    thread: &str,
    increment: bool,
) -> Result<()> {
    let count = expected_groups(tx, catalog, thread).await?;
    let next = if increment {
        count.checked_add(1)
    } else {
        count.checked_sub(1)
    }
    .ok_or(Error::IncompatibleStore)?;
    let key = thread_groups_key(catalog, thread);
    if next == 0 {
        tx.delete(key).await?;
    } else {
        tx.put(
            key,
            encode(&ThreadGroups {
                thread_id: thread.into(),
                count: next,
            })?,
        )
        .await?;
    }
    Ok(())
}

fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    end.push(255);
    end
}

pub(crate) async fn require_unprepared_namespace(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
) -> Result<()> {
    let prefix = location(catalog, "");
    if !tx.scan(&prefix, &prefix_end(&prefix), 1).await?.is_empty() {
        // A v1 writer never maintained this namespace. Its existing witnesses
        // cannot safely be adopted during the owner-version transition.
        return Err(Error::IncompatibleStore);
    }
    Ok(())
}

fn digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

async fn preparation(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
) -> Result<Option<Preparation>> {
    let state = tx
        .get(&location(catalog, "preparation"))
        .await?
        .as_deref()
        .map(decode::<Preparation>)
        .transpose()?;
    if state.as_ref().is_some_and(|state| {
        state.version != 1
            || state.generation != catalog.generation
            || (state.ready && state.after.is_some())
            || state.after.as_deref().is_some_and(|after| !digest(after))
    }) {
        return Err(Error::IncompatibleStore);
    }
    Ok(state)
}

async fn validate_owner(tx: &mut Transaction, catalog: &ApplicationCatalog) -> Result<()> {
    if tx
        .get(&managed_marker(catalog, TABLE))
        .await?
        .as_deref()
        .map(decode::<String>)
        .transpose()?
        .as_deref()
        != Some(OWNER)
    {
        return Err(Error::IncompatibleStore);
    }
    crate::application_projections::validate_item_owner(tx, catalog).await?;
    Ok(())
}

/// Called inside the existing bounded read-index preparation transaction. An
/// imported count row is authoritative: enumerate it rather than counting its
/// items again, which would silently change the imported SQLite result.
pub(crate) async fn prepare_batch(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    limit: usize,
) -> Result<(crate::application_projections::ProjectionPreparation, bool)> {
    validate(catalog)?;
    validate_owner(tx, catalog).await?;
    let mut state = preparation(tx, catalog).await?.unwrap_or(Preparation {
        version: 1,
        generation: catalog.generation.clone(),
        after: None,
        indexed_rows: 0,
        ready: false,
    });
    if state.ready {
        return Ok((
            crate::application_projections::ProjectionPreparation {
                prepared: true,
                indexed_rows: state.indexed_rows,
            },
            false,
        ));
    }
    let prefix = row_prefix(&catalog.generation, TABLE)?;
    let mut cursor = prefix.clone();
    if let Some(after) = &state.after {
        cursor.extend_from_slice(after.as_bytes());
        cursor.push(0);
    }
    let page = tx.scan(&cursor, &prefix_end(&prefix), limit).await?;
    let complete = page.len() < limit;
    for (key, bytes) in page {
        let source = decode_row(catalog, table(catalog, TABLE)?, &key, &bytes)?;
        integer(&source.row, "item_count")?;
        let witness = GroupWitness {
            thread_id: text(&source.row, "thread_id")?,
            run_key: text(&source.row, "run_key")?,
        };
        let index = group_key(catalog, &witness);
        if let Some(previous) = tx.get(&index).await? {
            if decode::<GroupWitness>(&previous)? != witness
                || expected_groups(tx, catalog, &witness.thread_id).await? == 0
            {
                return Err(Error::IncompatibleStore);
            }
        } else {
            adjust_groups(tx, catalog, &witness.thread_id, true).await?;
            tx.put(index, encode(&witness)?).await?;
        }
        state.after = Some(
            std::str::from_utf8(
                key.strip_prefix(prefix.as_slice())
                    .ok_or(Error::IncompatibleStore)?,
            )
            .map_err(|_| Error::IncompatibleStore)?
            .into(),
        );
        state.indexed_rows = state
            .indexed_rows
            .checked_add(1)
            .ok_or(Error::IncompatibleStore)?;
    }
    if complete {
        state.ready = true;
        state.after = None;
    }
    tx.put(location(catalog, "preparation"), encode(&state)?)
        .await?;
    Ok((
        crate::application_projections::ProjectionPreparation {
            prepared: state.ready,
            indexed_rows: state.indexed_rows,
        },
        true,
    ))
}

pub(crate) async fn ensure_ready(tx: &mut Transaction, catalog: &ApplicationCatalog) -> Result<()> {
    validate(catalog)?;
    validate_owner(tx, catalog).await?;
    if !preparation(tx, catalog)
        .await?
        .is_some_and(|state| state.ready)
    {
        return Err(Error::Invalid(
            "projection count group indexes are not prepared".into(),
        ));
    }
    Ok(())
}

/// Page one selected thread only; no completed item payloads or unrelated
/// thread groups are needed to execute the upstream aggregate count query.
pub(crate) async fn read_page(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    thread: &str,
    after: Option<&[u8]>,
) -> Result<GroupPage> {
    let prefix = group_prefix(catalog, thread);
    let mut cursor = prefix.clone();
    if let Some(after) = after {
        let suffix = after
            .strip_prefix(prefix.as_slice())
            .ok_or(Error::IncompatibleStore)?;
        if !std::str::from_utf8(suffix).ok().is_some_and(digest) {
            return Err(Error::IncompatibleStore);
        }
        cursor = after.to_vec();
        cursor.push(0);
    }
    let page = tx.scan(&cursor, &prefix_end(&prefix), PAGE_ROWS).await?;
    let full = page.len() == PAGE_ROWS;
    let mut rows = Vec::with_capacity(page.len());
    let mut last = None;
    for (key, bytes) in page {
        let witness: GroupWitness = decode(&bytes)?;
        if witness.thread_id != thread || group_key(catalog, &witness) != key {
            return Err(Error::IncompatibleStore);
        }
        let source = read_row(
            tx,
            catalog,
            TABLE,
            &[text_cell(&witness.thread_id), text_cell(&witness.run_key)],
        )
        .await?
        .ok_or(Error::IncompatibleStore)?;
        rows.push(GroupCount {
            thread_id: witness.thread_id,
            run_key: witness.run_key,
            item_count: integer(&source.row, "item_count")?,
        });
        last = Some(key);
    }
    Ok(GroupPage {
        rows,
        after: if full { last } else { None },
    })
}

pub(crate) fn validate(catalog: &ApplicationCatalog) -> Result<()> {
    validate_table(
        catalog,
        TABLE,
        &["thread_id", "run_key", "item_count"],
        &["thread_id", "run_key"],
    )
}
fn group(row: &Map<String, Value>) -> Result<(String, Option<String>)> {
    Ok((text(row, "thread_id")?, nullable(row, "run_id")?))
}
async fn adjust(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    group: &(String, String),
    delta: i64,
) -> Result<()> {
    let key = [text_cell(&group.0), text_cell(&group.1)];
    let previous = read_row(tx, catalog, TABLE, &key).await?;
    let count = previous
        .as_ref()
        .map(|row| integer(&row.row, "item_count"))
        .transpose()?
        .unwrap_or(0);
    // Migration 85 has no positive-count CHECK. INSERT preserves zero and
    // negative imported values while incrementing; DELETE/old-group UPDATE
    // removes nonpositive results and leaves a missing count row absent.
    let next = match delta {
        1 => Some(count.checked_add(1).ok_or(Error::IncompatibleStore)?),
        -1 => previous
            .as_ref()
            .and_then(|_| (count > 1).then(|| count - 1)),
        _ => return Err(Error::IncompatibleStore),
    };
    let witness = GroupWitness {
        thread_id: group.0.clone(),
        run_key: group.1.clone(),
    };
    let index = group_key(catalog, &witness);
    let strict = preparation(tx, catalog)
        .await?
        .is_some_and(|state| state.ready);
    let indexed = match tx.get(&index).await? {
        Some(bytes) if previous.is_some() && decode::<GroupWitness>(&bytes)? == witness => {
            if expected_groups(tx, catalog, &group.0).await? == 0 {
                return Err(Error::IncompatibleStore);
            }
            true
        }
        None if previous.is_none() || !strict => false,
        _ => return Err(Error::IncompatibleStore),
    };
    if let Some(next) = next {
        write_row(
            tx,
            catalog,
            TABLE,
            Map::from_iter([
                ("thread_id".into(), key[0].clone()),
                ("run_key".into(), key[1].clone()),
                ("item_count".into(), int_cell(next)),
            ]),
        )
        .await?;
        if !indexed {
            adjust_groups(tx, catalog, &group.0, true).await?;
            tx.put(index, encode(&witness)?).await?;
        }
    } else if previous.is_some() {
        delete_row(tx, catalog, TABLE, &key).await?;
        if indexed {
            adjust_groups(tx, catalog, &group.0, false).await?;
            tx.delete(index).await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Store;
    use crate::application::{decode_catalog, schema_key};
    use crate::test_support::TestFixture;
    use std::collections::BTreeMap;

    fn row(thread: &str, run: &str, count: i64) -> Map<String, Value> {
        Map::from_iter([
            ("thread_id".into(), text_cell(thread)),
            ("run_key".into(), text_cell(run)),
            ("item_count".into(), int_cell(count)),
        ])
    }

    async fn fixture(rows: Vec<(String, String, i64)>) -> (TestFixture, Store, ApplicationCatalog) {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        store.bootstrap_application().await.unwrap();
        let mut tx = store.db.begin().await.unwrap();
        let catalog = decode_catalog(&tx.get(&schema_key()).await.unwrap().unwrap()).unwrap();
        for (thread, run, count) in rows {
            write_row(&mut tx, &catalog, TABLE, row(&thread, &run, count))
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();
        (fixture, store, catalog)
    }

    async fn finish_preparation(store: &Store) {
        while !store
            .prepare_application_projection_reads()
            .await
            .unwrap()
            .prepared
        {}
    }

    async fn groups(
        tx: &mut Transaction,
        catalog: &ApplicationCatalog,
        thread: &str,
    ) -> Result<BTreeMap<String, i64>> {
        ensure_ready(tx, catalog).await?;
        let expected = expected_groups(tx, catalog, thread).await?;
        let mut after: Option<Vec<u8>> = None;
        let mut result = BTreeMap::new();
        loop {
            let page = read_page(tx, catalog, thread, after.as_deref()).await?;
            for row in page.rows {
                assert_eq!(row.thread_id, thread);
                assert!(result.insert(row.run_key, row.item_count).is_none());
            }
            match page.after {
                Some(cursor) => after = Some(cursor),
                None => {
                    if u64::try_from(result.len()).ok() != Some(expected) {
                        return Err(Error::IncompatibleStore);
                    }
                    return Ok(result);
                }
            }
        }
    }

    #[tokio::test]
    async fn count_groups_resume_bounded_preparation_without_recounting_imports() {
        let mut rows: Vec<_> = (0..132)
            .map(|index| ("thread".into(), format!("run:{index:03}"), 1))
            .collect();
        rows.push(("thread".into(), "zero".into(), 0));
        rows.push(("other".into(), "run:foreign".into(), 7));
        let (fixture, store, catalog) = fixture(rows).await;
        // Stop after a real public preparation batch has staged only part of
        // the new index. An old relationship-ready marker is not sufficient.
        loop {
            let progress = store.prepare_application_projection_reads().await.unwrap();
            let mut tx = store.db.begin_read_only().await.unwrap();
            let state = preparation(&mut tx, &catalog).await.unwrap();
            tx.rollback().await.unwrap();
            if state.is_some_and(|state| !state.ready && state.after.is_some()) {
                assert!(!progress.prepared);
                break;
            }
            assert!(!progress.prepared);
        }
        let mut partial = store.db.begin_read_only().await.unwrap();
        assert!(matches!(
            ensure_ready(&mut partial, &catalog).await,
            Err(Error::Invalid(_))
        ));
        // The source can change between bounded batches. Every live mutation
        // maintains its witness, including keys on either side of the cursor.
        let mut writer = store.db.begin().await.unwrap();
        adjust(
            &mut writer,
            &catalog,
            &("thread".into(), "run:000".into()),
            1,
        )
        .await
        .unwrap();
        adjust(
            &mut writer,
            &catalog,
            &("thread".into(), "run:001".into()),
            -1,
        )
        .await
        .unwrap();
        adjust(
            &mut writer,
            &catalog,
            &("thread".into(), "new-during-preparation".into()),
            1,
        )
        .await
        .unwrap();
        writer.commit().await.unwrap();
        partial.rollback().await.unwrap();
        store.shutdown().await.unwrap();

        let reopened = fixture.connect().await;
        let mut partial = reopened.db.begin_read_only().await.unwrap();
        assert!(matches!(
            ensure_ready(&mut partial, &catalog).await,
            Err(Error::Invalid(_))
        ));
        finish_preparation(&reopened).await;
        assert!(matches!(
            ensure_ready(&mut partial, &catalog).await,
            Err(Error::Invalid(_))
        ));
        partial.rollback().await.unwrap();
        finish_preparation(&reopened).await;
        let mut reader = reopened.db.begin_read_only().await.unwrap();
        let values = groups(&mut reader, &catalog, "thread").await.unwrap();
        assert_eq!(values.len(), 133);
        assert_eq!(values["run:000"], 2);
        assert!(!values.contains_key("run:001"));
        assert_eq!(values["new-during-preparation"], 1);
        assert_eq!(values["zero"], 0);
        assert_eq!(
            groups(&mut reader, &catalog, "other").await.unwrap(),
            BTreeMap::from([("run:foreign".into(), 7)])
        );
        assert!(
            groups(&mut reader, &catalog, "missing")
                .await
                .unwrap()
                .is_empty()
        );
        // The page boundary returns every canonical group exactly once, even
        // when the source count has no corresponding imported turn-item rows.
        let first = read_page(&mut reader, &catalog, "thread", None)
            .await
            .unwrap();
        assert_eq!(first.rows.len(), PAGE_ROWS);
        let second = read_page(&mut reader, &catalog, "thread", first.after.as_deref())
            .await
            .unwrap();
        assert_eq!(second.rows.len(), 5);
        assert!(second.after.is_none());
        reader.rollback().await.unwrap();
        reopened.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn count_groups_keep_snapshot_values_and_reject_fresh_corrupt_witnesses() {
        let (_fixture, store, catalog) = fixture(vec![("thread".into(), "run".into(), 9)]).await;
        finish_preparation(&store).await;
        let mut old = store.db.begin_read_only().await.unwrap();
        assert_eq!(
            groups(&mut old, &catalog, "thread").await.unwrap()["run"],
            9
        );
        let mut writer = store.db.begin().await.unwrap();
        for _ in 0..4 {
            adjust(&mut writer, &catalog, &("thread".into(), "run".into()), 1)
                .await
                .unwrap();
        }
        writer.commit().await.unwrap();
        let mut fresh = store.db.begin_read_only().await.unwrap();
        assert_eq!(
            groups(&mut fresh, &catalog, "thread").await.unwrap()["run"],
            13
        );
        fresh.rollback().await.unwrap();
        assert_eq!(
            groups(&mut old, &catalog, "thread").await.unwrap()["run"],
            9
        );

        let witness = GroupWitness {
            thread_id: "thread".into(),
            run_key: "run".into(),
        };
        let index = group_key(&catalog, &witness);
        let mut writer = store.db.begin().await.unwrap();
        writer
            .put(
                index.clone(),
                encode(&GroupWitness {
                    thread_id: "another".into(),
                    ..witness.clone()
                })
                .unwrap(),
            )
            .await
            .unwrap();
        writer.commit().await.unwrap();
        let mut fresh = store.db.begin_read_only().await.unwrap();
        assert!(matches!(
            groups(&mut fresh, &catalog, "thread").await,
            Err(Error::IncompatibleStore)
        ));
        assert!(
            groups(&mut fresh, &catalog, "unrelated")
                .await
                .unwrap()
                .is_empty()
        );
        fresh.rollback().await.unwrap();
        assert_eq!(
            groups(&mut old, &catalog, "thread").await.unwrap()["run"],
            9
        );

        let mut writer = store.db.begin().await.unwrap();
        writer
            .put(index.clone(), encode(&witness).unwrap())
            .await
            .unwrap();
        writer
            .delete(thread_groups_key(&catalog, "thread"))
            .await
            .unwrap();
        writer.commit().await.unwrap();
        let mut fresh = store.db.begin_read_only().await.unwrap();
        assert!(matches!(
            groups(&mut fresh, &catalog, "thread").await,
            Err(Error::IncompatibleStore)
        ));
        fresh.rollback().await.unwrap();

        let mut writer = store.db.begin().await.unwrap();
        writer
            .put(
                thread_groups_key(&catalog, "thread"),
                encode(&ThreadGroups {
                    thread_id: "thread".into(),
                    count: 1,
                })
                .unwrap(),
            )
            .await
            .unwrap();
        delete_row(
            &mut writer,
            &catalog,
            TABLE,
            &[text_cell("thread"), text_cell("run")],
        )
        .await
        .unwrap();
        writer.commit().await.unwrap();
        let mut fresh = store.db.begin_read_only().await.unwrap();
        assert!(matches!(
            groups(&mut fresh, &catalog, "thread").await,
            Err(Error::IncompatibleStore)
        ));
        fresh.rollback().await.unwrap();

        let mut writer = store.db.begin().await.unwrap();
        write_row(&mut writer, &catalog, TABLE, row("thread", "run", 13))
            .await
            .unwrap();
        writer.delete(index).await.unwrap();
        writer.commit().await.unwrap();
        let mut fresh = store.db.begin_read_only().await.unwrap();
        assert!(matches!(
            groups(&mut fresh, &catalog, "thread").await,
            Err(Error::IncompatibleStore)
        ));
        fresh.rollback().await.unwrap();
        let mut writer = store.db.begin().await.unwrap();
        assert!(matches!(
            adjust(&mut writer, &catalog, &("thread".into(), "run".into()), 1).await,
            Err(Error::IncompatibleStore)
        ));
        assert_eq!(
            integer(
                &read_row(
                    &mut writer,
                    &catalog,
                    TABLE,
                    &[text_cell("thread"), text_cell("run")]
                )
                .await
                .unwrap()
                .unwrap()
                .row,
                "item_count"
            )
            .unwrap(),
            13
        );
        writer.rollback().await.unwrap();

        let mut writer = store.db.begin().await.unwrap();
        let mut state = preparation(&mut writer, &catalog).await.unwrap().unwrap();
        state.generation = "foreign-generation".into();
        writer
            .put(location(&catalog, "preparation"), encode(&state).unwrap())
            .await
            .unwrap();
        writer.commit().await.unwrap();
        let mut fresh = store.db.begin_read_only().await.unwrap();
        assert!(matches!(
            ensure_ready(&mut fresh, &catalog).await,
            Err(Error::IncompatibleStore)
        ));
        fresh.rollback().await.unwrap();
        let mut writer = store.db.begin().await.unwrap();
        state.generation = catalog.generation.clone();
        writer
            .put(location(&catalog, "preparation"), encode(&state).unwrap())
            .await
            .unwrap();
        writer
            .put(
                managed_marker(&catalog, TABLE),
                encode(&"foreign-owner").unwrap(),
            )
            .await
            .unwrap();
        writer.commit().await.unwrap();
        let mut fresh = store.db.begin_read_only().await.unwrap();
        assert!(matches!(
            ensure_ready(&mut fresh, &catalog).await,
            Err(Error::IncompatibleStore)
        ));
        fresh.rollback().await.unwrap();
        assert_eq!(
            groups(&mut old, &catalog, "thread").await.unwrap()["run"],
            9
        );
        old.rollback().await.unwrap();
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn corrupt_count_backfill_never_reports_ready_or_repairs_source_counts() {
        let (_fixture, store, catalog) = fixture(vec![("thread".into(), "run".into(), 17)]).await;
        while !store.prepare_application_projections().await.unwrap().prepared {}
        let witness = GroupWitness {
            thread_id: "thread".into(),
            run_key: "run".into(),
        };
        let mut writer = store.db.begin().await.unwrap();
        writer
            .put(group_key(&catalog, &witness), b"{corrupt".to_vec())
            .await
            .unwrap();
        writer.commit().await.unwrap();
        loop {
            match store.prepare_application_projection_reads().await {
                Ok(progress) => assert!(!progress.prepared),
                Err(Error::IncompatibleStore) => break,
                Err(error) => panic!("unexpected preparation error: {error:?}"),
            }
        }
        let mut reader = store.db.begin_read_only().await.unwrap();
        assert!(
            !preparation(&mut reader, &catalog)
                .await
                .unwrap()
                .is_some_and(|state| state.ready)
        );
        assert_eq!(
            integer(
                &read_row(
                    &mut reader,
                    &catalog,
                    TABLE,
                    &[text_cell("thread"), text_cell("run")]
                )
                .await
                .unwrap()
                .unwrap()
                .row,
                "item_count"
            )
            .unwrap(),
            17
        );
        assert!(matches!(
            ensure_ready(&mut reader, &catalog).await,
            Err(Error::Invalid(_))
        ));
        reader.rollback().await.unwrap();
        store.shutdown().await.unwrap();
    }
}
pub(crate) async fn replace(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    old: Option<&Map<String, Value>>,
    new: Option<&Map<String, Value>>,
) -> Result<()> {
    let old = old.map(group).transpose()?;
    let new = new.map(group).transpose()?;
    // The trigger's WHEN compares nullable source cells before COALESCE. NULL
    // to empty (or the reverse) still removes and reinserts the same group.
    if old == new {
        return Ok(());
    }
    if let Some(old) = &old {
        adjust(
            tx,
            catalog,
            &(old.0.clone(), old.1.clone().unwrap_or_default()),
            -1,
        )
        .await?;
    }
    if let Some(new) = &new {
        adjust(
            tx,
            catalog,
            &(new.0.clone(), new.1.clone().unwrap_or_default()),
            1,
        )
        .await?;
    }
    Ok(())
}
