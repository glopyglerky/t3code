//! Secondary relationship indexes shared by native projection read models.
//! Import order and live insertion order stay distinct, so an update never moves
//! a historical row and a delete/reinsert receives a new position.

use std::collections::BTreeSet;

use super::*;

#[path = "redb_projection_derived_indexes.rs"]
pub(super) mod derived;

#[path = "redb_projection_window_index.rs"]
pub(crate) mod window;

const RELATIONSHIPS: &[&str] = &[
    "thread_id",
    "run_id",
    "node_id",
    "owner_node_id",
    "provider_thread_id",
    "provider_turn_id",
    "provider_session_id",
    "source_thread_id",
    "target_thread_id",
    "child_thread_id",
];

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
pub(crate) struct RowOrder {
    pub epoch: u8,
    pub ordinal: u64,
}

pub(crate) struct IndexedRow {
    pub row: Map<String, Value>,
    pub order: RowOrder,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct IndexRow {
    table: String,
    column: String,
    value: Value,
    primary: Vec<Value>,
    order: RowOrder,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct WorkspaceRow {
    thread_id: String,
    project_id: String,
    worktree_path: String,
    deleted: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct IndexCount {
    table: String,
    column: String,
    value: Value,
    count: u64,
}

#[derive(Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
struct ItemOrdinal {
    thread_id: String,
    item_id: String,
    ordinal: i64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ReadPreparation {
    version: u32,
    table_index: usize,
    after: Option<String>,
    indexed_rows: u64,
    ready: bool,
}

fn location(catalog: &ApplicationCatalog, suffix: &str) -> Vec<u8> {
    store_key(&format!(
        "application/generation/{}/projection-reads/v1/{suffix}",
        catalog.generation
    ))
}

fn primary(name: &str, row: &Map<String, Value>) -> Result<Vec<Value>> {
    spec(name)
        .primary
        .iter()
        .map(|column| row.get(*column).cloned().ok_or(Error::IncompatibleStore))
        .collect()
}

fn order_key(catalog: &ApplicationCatalog, name: &str, primary: &[Value]) -> Vec<u8> {
    location(
        catalog,
        &format!("order/{name}/{}", request_digest(&json!(primary))),
    )
}

fn index_prefix(catalog: &ApplicationCatalog, name: &str, column: &str, value: &Value) -> Vec<u8> {
    location(
        catalog,
        &format!("rows/{name}/{column}/{}/", request_digest(value)),
    )
}

fn index_key(catalog: &ApplicationCatalog, witness: &IndexRow) -> Vec<u8> {
    let mut key = index_prefix(catalog, &witness.table, &witness.column, &witness.value);
    key.extend_from_slice(request_digest(&json!(witness.primary)).as_bytes());
    key
}

fn count_key(catalog: &ApplicationCatalog, name: &str, column: &str, value: &Value) -> Vec<u8> {
    location(
        catalog,
        &format!("counts/{name}/{column}/{}", request_digest(value)),
    )
}

async fn adjust_count(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    row: &IndexRow,
    increment: bool,
) -> Result<()> {
    let key = count_key(catalog, &row.table, &row.column, &row.value);
    let mut count = match tx.get(&key).await? {
        Some(bytes) => decode::<IndexCount>(&bytes)?,
        None => IndexCount {
            table: row.table.clone(),
            column: row.column.clone(),
            value: row.value.clone(),
            count: 0,
        },
    };
    if count.table != row.table || count.column != row.column || count.value != row.value {
        return Err(Error::IncompatibleStore);
    }
    count.count = if increment {
        count.count.checked_add(1)
    } else {
        count.count.checked_sub(1)
    }
    .ok_or(Error::IncompatibleStore)?;
    tx.put(key, encode(&count)?).await
}

fn item_ordinal(row: &Map<String, Value>) -> Result<ItemOrdinal> {
    Ok(ItemOrdinal {
        thread_id: text(row, "thread_id")?,
        item_id: text(row, "turn_item_id")?,
        ordinal: integer(row, "ordinal")?,
    })
}

fn ordinal_prefix(catalog: &ApplicationCatalog, thread_id: &str) -> Vec<u8> {
    location(
        catalog,
        &format!("item-ordinal/{}/", request_digest(&json!(thread_id))),
    )
}

fn ordinal_key(catalog: &ApplicationCatalog, row: &ItemOrdinal) -> Vec<u8> {
    let mut key = ordinal_prefix(catalog, &row.thread_id);
    let descending = !((row.ordinal as u64) ^ (1_u64 << 63));
    key.extend_from_slice(
        format!("{descending:016x}/{}", request_digest(&json!(row.item_id))).as_bytes(),
    );
    key
}

fn entries(name: &str, row: &Map<String, Value>, order: RowOrder) -> Result<Vec<IndexRow>> {
    let primary = primary(name, row)?;
    let mut result = vec![IndexRow {
        table: name.into(),
        column: "all".into(),
        value: Value::Null,
        primary: primary.clone(),
        order,
    }];
    for column in RELATIONSHIPS {
        if let Some(value) = row.get(*column) {
            result.push(IndexRow {
                table: name.into(),
                column: (*column).into(),
                value: value.clone(),
                primary: primary.clone(),
                order,
            });
        }
    }
    for (column, value) in derived::values(name, row)? {
        result.push(IndexRow {
            table: name.into(),
            column: column.into(),
            value,
            primary: primary.clone(),
            order,
        });
    }
    Ok(result)
}

fn workspace(row: &Map<String, Value>) -> Result<Option<WorkspaceRow>> {
    let deleted = nullable(row, "deleted_at")?.is_some();
    let Ok(payload) = serde_json::from_str::<Value>(&text(row, "payload_json")?) else {
        return Ok(None);
    };
    match payload.get("worktreePath") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(path)) => Ok(Some(WorkspaceRow {
            thread_id: text(row, "thread_id")?,
            project_id: text(row, "project_id")?,
            worktree_path: path.clone(),
            deleted,
        })),
        _ => Err(Error::IncompatibleStore),
    }
}

fn workspace_key(catalog: &ApplicationCatalog, row: &WorkspaceRow) -> Vec<u8> {
    location(
        catalog,
        &format!(
            "workspaces/{}",
            request_digest(&json!([row.project_id, row.worktree_path, row.thread_id]))
        ),
    )
}

async fn imported_order(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    primary: &[Value],
) -> Result<Option<RowOrder>> {
    let key = store_key(&format!(
        "application/generation/{}/metadata/source/{}",
        catalog.generation,
        request_digest(&json!([name, primary]))
    ));
    let Some(bytes) = tx.get(&key).await? else {
        return Ok(None);
    };
    let witness: Value = decode(&bytes)?;
    if witness.get("table").and_then(Value::as_str) != Some(name)
        || witness.get("key") != Some(&json!(primary))
    {
        return Err(Error::IncompatibleStore);
    }
    Ok(Some(RowOrder {
        epoch: 0,
        ordinal: witness
            .get("ordinal")
            .and_then(Value::as_u64)
            .ok_or(Error::IncompatibleStore)?,
    }))
}

async fn row_order(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    primary: &[Value],
    can_import: bool,
) -> Result<RowOrder> {
    let key = order_key(catalog, name, primary);
    if let Some(bytes) = tx.get(&key).await? {
        let order: RowOrder = decode(&bytes)?;
        if order.epoch > 1 {
            return Err(Error::IncompatibleStore);
        }
        return Ok(order);
    }
    let order = if can_import {
        imported_order(tx, catalog, name, primary).await?
    } else {
        None
    };
    if can_import && order.is_none() && matches!(name, HANDOFFS | TRANSFERS) {
        return Err(invalid(
            "projection insertion order is unavailable; reimport the original SQLite source",
        ));
    }
    let order = match order {
        Some(order) => order,
        None => {
            let counter_key = location(catalog, &format!("next-order/{name}"));
            let previous: u64 = tx
                .get(&counter_key)
                .await?
                .as_deref()
                .map(decode)
                .transpose()?
                .unwrap_or(0);
            let ordinal = previous.checked_add(1).ok_or(Error::IncompatibleStore)?;
            tx.put(counter_key, encode(&ordinal)?).await?;
            RowOrder { epoch: 1, ordinal }
        }
    };
    tx.put(key, encode(&order)?).await?;
    Ok(order)
}

async fn insert_entries(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    row: &Map<String, Value>,
    order: RowOrder,
) -> Result<()> {
    for witness in entries(name, row, order)? {
        let key = index_key(catalog, &witness);
        if let Some(previous) = tx.get(&key).await? {
            if decode::<IndexRow>(&previous)? != witness {
                return Err(Error::IncompatibleStore);
            }
        } else {
            adjust_count(tx, catalog, &witness, true).await?;
        }
        tx.put(key, encode(&witness)?).await?;
    }
    if name == ITEMS {
        let witness = item_ordinal(row)?;
        let key = ordinal_key(catalog, &witness);
        if let Some(previous) = tx.get(&key).await? {
            if decode::<ItemOrdinal>(&previous)? != witness {
                return Err(Error::IncompatibleStore);
            }
        }
        tx.put(key, encode(&witness)?).await?;
    }
    if name == THREADS {
        if let Some(witness) = workspace(row)? {
            let key = workspace_key(catalog, &witness);
            if let Some(previous) = tx.get(&key).await? {
                if decode::<WorkspaceRow>(&previous)? != witness {
                    return Err(Error::IncompatibleStore);
                }
            }
            tx.put(key, encode(&witness)?).await?;
        }
    }
    window::insert(tx, catalog, name, row, order).await?;
    Ok(())
}

/// Every projection write/delete calls this while holding the same transaction.
/// Existing candidate stores may not yet have backfilled indexes, so a missing
/// old entry is accepted until preparation is complete, never after that point.
pub(super) async fn replace(
    tx: &mut Transaction,
    catalog: &ApplicationCatalog,
    name: &str,
    old: Option<&Map<String, Value>>,
    row: Option<&Map<String, Value>>,
) -> Result<()> {
    let state: Option<ReadPreparation> = tx
        .get(&location(catalog, "preparation"))
        .await?
        .as_deref()
        .map(decode)
        .transpose()?;
    let strict = state.as_ref().is_some_and(|state| state.ready);
    let old_order = if let Some(old) = old {
        let order = row_order(tx, catalog, name, &primary(name, old)?, true).await?;
        window::remove(tx, catalog, name, old, order, strict).await?;
        for witness in entries(name, old, order)? {
            let key = index_key(catalog, &witness);
            match tx.get(&key).await? {
                Some(previous) if decode::<IndexRow>(&previous)? == witness => {
                    tx.delete(key).await?;
                    adjust_count(tx, catalog, &witness, false).await?;
                }
                None if !strict => (),
                _ => return Err(Error::IncompatibleStore),
            }
        }
        if name == ITEMS {
            let witness = item_ordinal(old)?;
            let key = ordinal_key(catalog, &witness);
            match tx.get(&key).await? {
                Some(previous) if decode::<ItemOrdinal>(&previous)? == witness => {
                    tx.delete(key).await?;
                }
                None if !strict => (),
                _ => return Err(Error::IncompatibleStore),
            }
        }
        if name == THREADS {
            if let Some(witness) = workspace(old)? {
                let key = workspace_key(catalog, &witness);
                match tx.get(&key).await? {
                    Some(previous) if decode::<WorkspaceRow>(&previous)? == witness => {
                        tx.delete(key).await?;
                    }
                    None if !strict => (),
                    _ => return Err(Error::IncompatibleStore),
                }
            }
        }
        Some(order)
    } else {
        None
    };
    match row {
        Some(row) => {
            let order = match old_order {
                Some(order) => order,
                None => row_order(tx, catalog, name, &primary(name, row)?, false).await?,
            };
            insert_entries(tx, catalog, name, row, order).await?;
        }
        None => {
            if let Some(old) = old {
                tx.delete(order_key(catalog, name, &primary(name, old)?))
                    .await?;
            }
        }
    }
    Ok(())
}

pub(crate) async fn is_prepared(session: &mut Session) -> Result<bool> {
    session.transaction.ensure_active()?;
    if session
        .read_snapshot_validation
        .as_ref()
        .is_some_and(|validation| validation.projection_reads)
    {
        return Ok(true);
    }
    let state: Option<ReadPreparation> = session
        .transaction
        .get(&location(&session.catalog, "preparation"))
        .await?
        .as_deref()
        .map(decode)
        .transpose()?;
    let Some(state) = state else {
        return Ok(false);
    };
    if state.version != 1
        || state.table_index > SPECS.len()
        || state.ready != (state.table_index == SPECS.len())
    {
        return Err(Error::IncompatibleStore);
    }
    if state.ready {
        ready(session).await?;
        if let Some(validation) = &mut session.read_snapshot_validation {
            validation.projection_reads = true;
        }
    }
    Ok(state.ready)
}

pub(crate) async fn ensure_ready(session: &mut Session) -> Result<()> {
    ready(session).await?;
    if !is_prepared(session).await? {
        return Err(invalid("projection read indexes are not prepared"));
    }
    Ok(())
}

async fn visit_equal_rows(
    session: &mut Session,
    name: &str,
    column: &str,
    value: &Value,
    page_rows: usize,
    mut visit: impl FnMut(IndexedRow) -> Result<bool>,
) -> Result<()> {
    ensure_ready(session).await?;
    if !SPECS.iter().any(|spec| spec.name == name)
        || !(column == "all"
            || RELATIONSHIPS.contains(&column)
            || derived::COLUMNS.contains(&column))
    {
        return Err(invalid("unsupported projection relationship index"));
    }
    let prefix = index_prefix(&session.catalog, name, column, value);
    let end = prefix_end(&prefix);
    let mut cursor = prefix.clone();
    loop {
        let page = session.transaction.scan(&cursor, &end, page_rows).await?;
        let count = page.len();
        for (key, bytes) in page {
            let witness: IndexRow = decode(&bytes)?;
            if witness.table != name
                || witness.column != column
                || &witness.value != value
                || index_key(&session.catalog, &witness) != key
            {
                return Err(Error::IncompatibleStore);
            }
            let source = read_row(
                &mut session.transaction,
                &session.catalog,
                name,
                &witness.primary,
            )
            .await?
            .ok_or(Error::IncompatibleStore)?;
            if column != "all" {
                let matches = if derived::COLUMNS.contains(&column) {
                    derived::values(name, &source.row)?
                        .iter()
                        .any(|(name, found)| *name == column && found == value)
                } else {
                    source.row.get(column) == Some(value)
                };
                if !matches {
                    return Err(Error::IncompatibleStore);
                }
            }
            if !visit(IndexedRow {
                row: source.row,
                order: witness.order,
            })? {
                return Ok(());
            }
            cursor = key;
            cursor.push(0);
        }
        if count < page_rows {
            break;
        }
    }
    Ok(())
}

pub(crate) async fn equal_rows(
    session: &mut Session,
    name: &str,
    column: &str,
    value: &Value,
) -> Result<Vec<IndexedRow>> {
    let mut result = Vec::new();
    visit_equal_rows(session, name, column, value, 128, |row| {
        result.push(row);
        Ok(true)
    })
    .await?;
    Ok(result)
}

/// Retain only selected values while scanning and validating one bounded page at a time.
pub(crate) async fn filter_map_equal<T>(
    session: &mut Session,
    name: &str,
    column: &str,
    value: &Value,
    mut project: impl FnMut(Map<String, Value>) -> Result<Option<T>>,
) -> Result<Vec<T>> {
    let mut result = Vec::new();
    visit_equal_rows(session, name, column, value, 128, |row| {
        if let Some(value) = project(row.row)? {
            result.push(value);
        }
        Ok(true)
    })
    .await?;
    Ok(result)
}

/// Validate rows incrementally, stopping at the first match without materializing the cohort.
pub(crate) async fn any_equal(
    session: &mut Session,
    name: &str,
    column: &str,
    value: &Value,
    mut predicate: impl FnMut(&Map<String, Value>) -> Result<bool>,
) -> Result<bool> {
    let mut found = false;
    visit_equal_rows(session, name, column, value, 32, |row| {
        found = predicate(&row.row)?;
        Ok(!found)
    })
    .await?;
    Ok(found)
}

/// Count a selected relationship without retaining source rows or their payloads.
pub(crate) async fn count_matching(
    session: &mut Session,
    name: &str,
    column: &str,
    value: &Value,
    mut predicate: impl FnMut(&Map<String, Value>) -> Result<bool>,
) -> Result<i64> {
    let mut count = 0_i64;
    visit_equal_rows(session, name, column, value, 128, |row| {
        if predicate(&row.row)? {
            count = count.checked_add(1).ok_or(Error::IncompatibleStore)?;
        }
        Ok(true)
    })
    .await?;
    Ok(count)
}

pub(crate) async fn all_rows(session: &mut Session, name: &str) -> Result<Vec<IndexedRow>> {
    equal_rows(session, name, "all", &Value::Null).await
}

pub(crate) async fn count_equal(
    session: &mut Session,
    name: &str,
    column: &str,
    value: &Value,
) -> Result<u64> {
    ensure_ready(session).await?;
    let key = count_key(&session.catalog, name, column, value);
    let Some(bytes) = session.transaction.get(&key).await? else {
        return Ok(0);
    };
    let count: IndexCount = decode(&bytes)?;
    if count.table != name || count.column != column || &count.value != value {
        return Err(Error::IncompatibleStore);
    }
    Ok(count.count)
}

pub(crate) async fn next_item_ordinal(session: &mut Session, thread_id: &str) -> Result<i64> {
    ensure_ready(session).await?;
    let prefix = ordinal_prefix(&session.catalog, thread_id);
    let page = session
        .transaction
        .scan(&prefix, &prefix_end(&prefix), 1)
        .await?;
    let Some((key, bytes)) = page.first() else {
        return Ok(1);
    };
    let witness: ItemOrdinal = decode(bytes)?;
    if witness.thread_id != thread_id || ordinal_key(&session.catalog, &witness) != *key {
        return Err(Error::IncompatibleStore);
    }
    let row = window::read_metadata(session, ITEMS, &witness.item_id)
        .await?
        .ok_or(Error::IncompatibleStore)?;
    if item_ordinal(&row.row)? != witness {
        return Err(Error::IncompatibleStore);
    }
    witness
        .ordinal
        .checked_add(1)
        .filter(|value| value.unsigned_abs() <= MAX_SAFE_INTEGER)
        .ok_or_else(|| invalid("turn item ordinal exceeds the JavaScript integer range"))
}

pub(super) async fn workspace_paths(session: &mut Session) -> Result<Vec<Value>> {
    read_workspaces(session, false).await
}

pub(crate) async fn all_workspace_paths(session: &mut Session) -> Result<Vec<Value>> {
    read_workspaces(session, true).await
}

async fn read_workspaces(session: &mut Session, include_deleted: bool) -> Result<Vec<Value>> {
    ensure_ready(session).await?;
    for invalid in equal_rows(session, THREADS, "_unreadable_payload", &int_cell(1)).await? {
        if include_deleted || nullable(&invalid.row, "deleted_at")?.is_none() {
            return Err(Error::IncompatibleStore);
        }
    }
    let prefix = location(&session.catalog, "workspaces/");
    let end = prefix_end(&prefix);
    let mut cursor = prefix;
    let mut pairs = BTreeSet::new();
    loop {
        let page = session.transaction.scan(&cursor, &end, 128).await?;
        let count = page.len();
        for (key, bytes) in page {
            let witness: WorkspaceRow = decode(&bytes)?;
            if workspace_key(&session.catalog, &witness) != key {
                return Err(Error::IncompatibleStore);
            }
            if include_deleted || !witness.deleted {
                pairs.insert((witness.project_id, witness.worktree_path));
            }
            cursor = key;
            cursor.push(0);
        }
        if count < 128 {
            break;
        }
    }
    Ok(pairs.into_iter().map(|(project_id, worktree_path)| json!({"project_id":project_id,"worktree_path":worktree_path})).collect())
}

impl Store {
    pub async fn prepare_application_projection_reads(&self) -> Result<ProjectionPreparation> {
        let base = self.prepare_application_projections().await?;
        if !base.prepared {
            return Ok(ProjectionPreparation {
                prepared: false,
                indexed_rows: 0,
            });
        }
        let mut limit = PREPARE_ROWS;
        loop {
            match self.prepare_projection_reads_batch(limit, None).await {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }

    pub(crate) async fn prepare_projection_reads_for_catalog(
        &self,
        catalog: &ApplicationCatalog,
    ) -> Result<ProjectionPreparation> {
        self.db.require_exclusive()?;
        let base = self.prepare_projections_for_catalog(catalog).await?;
        if !base.prepared {
            return Ok(ProjectionPreparation {
                prepared: false,
                indexed_rows: 0,
            });
        }
        let mut limit = PREPARE_ROWS;
        loop {
            match self
                .prepare_projection_reads_batch(limit, Some(catalog))
                .await
            {
                Err(Error::ResourceLimit(_)) if limit > 1 => limit = (limit / 2).max(1),
                result => return result,
            }
        }
    }

    async fn prepare_projection_reads_batch(
        &self,
        limit: usize,
        target: Option<&ApplicationCatalog>,
    ) -> Result<ProjectionPreparation> {
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        let catalog = match target {
            Some(catalog) => catalog.clone(),
            None => decode_catalog(
                &tx.get(&schema_key())
                    .await?
                    .ok_or(Error::IncompatibleStore)?,
            )?,
        };
        let state_key = location(&catalog, "preparation");
        let mut state = match tx.get(&state_key).await? {
            Some(bytes) => decode::<ReadPreparation>(&bytes)?,
            None => ReadPreparation {
                version: 1,
                table_index: 0,
                after: None,
                indexed_rows: 0,
                ready: false,
            },
        };
        if state.version != 1
            || state.table_index > SPECS.len()
            || state.ready != (state.table_index == SPECS.len())
        {
            return Err(Error::IncompatibleStore);
        }
        if state.ready {
            let (counts, changed) = crate::application_turn_item_counts::prepare_batch(
                &mut tx,
                &catalog,
                limit,
            )
            .await?;
            let indexed_rows = state
                .indexed_rows
                .checked_add(counts.indexed_rows)
                .ok_or(Error::IncompatibleStore)?;
            if changed {
                tx.commit().await?;
            } else {
                tx.rollback().await?;
            }
            return Ok(ProjectionPreparation {
                prepared: counts.prepared,
                indexed_rows,
            });
        }
        let spec = &SPECS[state.table_index];
        let prefix = row_prefix(&catalog.generation, spec.name)?;
        let mut cursor = prefix.clone();
        if let Some(after) = &state.after {
            if after.len() != 64
                || !after
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            {
                return Err(Error::IncompatibleStore);
            }
            cursor.extend_from_slice(after.as_bytes());
            cursor.push(0);
        }
        let page = tx.scan(&cursor, &prefix_end(&prefix), limit).await?;
        let count = page.len();
        for (key, bytes) in page {
            let source = decode_row(&catalog, table(&catalog, spec.name)?, &key, &bytes)?;
            let order = row_order(&mut tx, &catalog, spec.name, &source.key, true).await?;
            insert_entries(&mut tx, &catalog, spec.name, &source.row, order).await?;
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
        if count < limit {
            state.table_index += 1;
            state.after = None;
            state.ready = state.table_index == SPECS.len();
        }
        tx.put(state_key, encode(&state)?).await?;
        tx.commit().await?;
        Ok(ProjectionPreparation {
            // The next bounded batch also prepares the count-group index. An
            // already-ready v1 relationship marker alone cannot activate it.
            prepared: false,
            indexed_rows: state.indexed_rows,
        })
    }
}

#[cfg(test)]
mod readiness_tests {
    use super::*;
    use crate::application::ApplicationTransactionMode;
    use crate::test_support::TestFixture;

    async fn fixture() -> (TestFixture, Store, ApplicationTransactions) {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        store.bootstrap_application().await.unwrap();
        while !store
            .prepare_application_projection_reads()
            .await
            .unwrap()
            .prepared
        {}
        let application = ApplicationTransactions::new(store.clone());
        (fixture, store, application)
    }

    async fn begin_read(application: &mut ApplicationTransactions, id: &str) {
        application
            .begin_mode(id, ApplicationTransactionMode::Read, None)
            .await
            .unwrap();
    }

    async fn count_threads(application: &mut ApplicationTransactions, id: &str) -> Result<Value> {
        application
            .projections_shell(id, ProjectionShellQuery::CountThreads {})
            .await
    }

    async fn replace_marker(store: &Store, marker: &[u8], bytes: Vec<u8>) {
        let mut transaction = store.db.begin().await.unwrap();
        transaction.put(marker.to_vec(), bytes).await.unwrap();
        transaction.commit().await.unwrap();
    }

    #[tokio::test]
    async fn validated_read_snapshot_keeps_its_markers_while_new_snapshot_rejects_corruption() {
        let (_fixture, store, mut application) = fixture().await;
        let catalog =
            decode_catalog(&store.db.read(&schema_key()).await.unwrap().unwrap()).unwrap();
        let markers = [
            managed_marker(&catalog, SPECS.last().unwrap().name),
            super::super::key(&catalog, "preparation"),
            location(&catalog, "preparation"),
        ];
        for (index, marker) in markers.into_iter().enumerate() {
            let original = store.db.read(&marker).await.unwrap().unwrap();
            begin_read(&mut application, "old").await;
            assert_eq!(
                count_threads(&mut application, "old").await.unwrap(),
                json!([{"count":0}])
            );
            let invalid = if index == 0 {
                encode(&"another-owner").unwrap()
            } else {
                let mut state: Value = decode(&original).unwrap();
                state["version"] = json!(2);
                encode(&state).unwrap()
            };
            replace_marker(&store, &marker, invalid).await;

            // The existing reader still owns the original engine snapshot,
            // including through a read savepoint; a later reader must revalidate.
            application.savepoint("old", "nested").await.unwrap();
            assert_eq!(
                count_threads(&mut application, "old").await.unwrap(),
                json!([{"count":0}])
            );
            application.rollback_to("old", "nested").await.unwrap();
            application.release("old", "nested").await.unwrap();
            begin_read(&mut application, "fresh").await;
            assert!(matches!(
                count_threads(&mut application, "fresh").await,
                Err(Error::IncompatibleStore)
            ));
            assert!(matches!(
                application.rollback("fresh").await,
                Err(Error::Invalid(_))
            ));

            replace_marker(&store, &marker, original).await;
            application.rollback("old").await.unwrap();
            begin_read(&mut application, "repaired").await;
            assert_eq!(
                count_threads(&mut application, "repaired").await.unwrap(),
                json!([{"count":0}])
            );
            application.rollback("repaired").await.unwrap();
        }
    }

    #[tokio::test]
    async fn writer_revalidates_staged_markers_and_savepoint_restore() {
        let (_fixture, store, mut application) = fixture().await;
        let catalog =
            decode_catalog(&store.db.read(&schema_key()).await.unwrap().unwrap()).unwrap();
        let marker = managed_marker(&catalog, THREADS);
        let original = store.db.read(&marker).await.unwrap().unwrap();
        application.begin("writer").await.unwrap();
        count_threads(&mut application, "writer").await.unwrap();
        application.savepoint("writer", "nested").await.unwrap();
        let mut session = application.take("writer").unwrap();
        session
            .transaction
            .put(marker.clone(), encode(&"another-owner").unwrap())
            .await
            .unwrap();
        assert!(matches!(
            ensure_ready(&mut session).await,
            Err(Error::IncompatibleStore)
        ));
        // Exercise restoration of the staged overlay before sending another
        // public query; writers must never reuse the earlier readiness proof.
        application.finish("writer", session, Ok(())).await.unwrap();
        application.rollback_to("writer", "nested").await.unwrap();
        application.release("writer", "nested").await.unwrap();
        count_threads(&mut application, "writer").await.unwrap();

        let mut session = application.take("writer").unwrap();
        session
            .transaction
            .put(marker.clone(), encode(&"another-owner").unwrap())
            .await
            .unwrap();
        application.finish("writer", session, Ok(())).await.unwrap();
        assert!(matches!(
            count_threads(&mut application, "writer").await,
            Err(Error::IncompatibleStore)
        ));
        assert!(!application.has_active_writer());
        assert_eq!(store.db.read(&marker).await.unwrap().unwrap(), original);
        begin_read(&mut application, "after-abort").await;
        count_threads(&mut application, "after-abort")
            .await
            .unwrap();
        application.rollback("after-abort").await.unwrap();
    }

    #[tokio::test]
    async fn oversized_corrupt_marker_batch_retains_terminal_rejection() {
        let (_fixture, store, mut application) = fixture().await;
        let catalog =
            decode_catalog(&store.db.read(&schema_key()).await.unwrap().unwrap()).unwrap();
        for spec in &SPECS[..2] {
            replace_marker(
                &store,
                &managed_marker(&catalog, spec.name),
                vec![b'x'; crate::MAX_BATCH_BYTES / 2 + 1],
            )
            .await;
        }
        begin_read(&mut application, "corrupt").await;
        application.savepoint("corrupt", "nested").await.unwrap();
        assert!(matches!(
            count_threads(&mut application, "corrupt").await,
            Err(Error::IncompatibleStore)
        ));
        // A recoverable batch-size error must not disguise corrupt ownership as
        // a failed inner query that could continue using this transaction.
        assert!(matches!(
            application.rollback_to("corrupt", "nested").await,
            Err(Error::Invalid(_))
        ));
    }
}
