use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use redb::{
    ConcurrencyMode, Database, Durability, ReadTransaction, ReadableDatabase, ReadableTable,
    TableDefinition, TableHandle,
};
use sha2::{Digest, Sha256};

use crate::{Error, MAX_BATCH_BYTES, MAX_BATCH_RECORDS, Result};

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("t3_code_ls_v2");
type Pair = (Vec<u8>, Vec<u8>);

fn storage_error(_: impl std::fmt::Display) -> Error {
    // Database errors may contain file names or stored keys. Keep them out of IPC/log output.
    Error::Database("redb storage operation failed".into())
}

fn worker_error(_: tokio::task::JoinError) -> Error {
    Error::Database("database worker failed".into())
}

#[derive(Clone)]
pub(crate) struct Backend {
    database: Arc<Database>,
}

impl Backend {
    pub(crate) async fn open(path: impl AsRef<Path>, initialize: bool) -> Result<Self> {
        let path = path.as_ref().to_owned();
        if path.as_os_str().is_empty() {
            return Err(Error::Invalid("database file path is empty".into()));
        }
        let database = tokio::task::spawn_blocking(move || {
            let mut builder = Database::builder();
            builder
                .set_concurrency_mode(ConcurrencyMode::MultiWriter)
                .set_check_integrity_on_open(true)
                .set_cache_size(32 * 1024 * 1024);
            let database = if initialize {
                let mut options = OpenOptions::new();
                options.read(true).write(true).create(true).truncate(false);
                #[cfg(unix)]
                options.mode(0o600);
                let file = options.open(&path).map_err(storage_error)?;
                let file = redb::backends::FileBackend::new(file).map_err(storage_error)?;
                builder.create_with_backend(file)
            } else {
                builder.open(&path)
            }
            .map_err(storage_error)?;
            if initialize {
                let mut transaction = database.begin_write().map_err(storage_error)?;
                transaction
                    .set_durability(Durability::Immediate)
                    .map_err(storage_error)?;
                let names = transaction
                    .list_tables()
                    .map_err(storage_error)?
                    .map(|table| table.name().to_owned())
                    .collect::<Vec<_>>();
                if transaction
                    .list_multimap_tables()
                    .map_err(storage_error)?
                    .next()
                    .is_some()
                    || names.iter().any(|name| name != TABLE.name())
                {
                    return Err(Error::IncompatibleStore);
                }
                {
                    let table = transaction.open_table(TABLE).map_err(storage_error)?;
                    if table
                        .iter()
                        .map_err(storage_error)?
                        .next()
                        .transpose()
                        .map_err(storage_error)?
                        .is_some()
                        && table
                            .get(crate::store_key("meta/state").as_slice())
                            .map_err(storage_error)?
                            .is_none()
                    {
                        return Err(Error::IncompatibleStore);
                    }
                }
                transaction.commit().map_err(commit_error)?;
                // A durable new database also needs its directory entry persisted.
                #[cfg(unix)]
                {
                    let parent = path
                        .parent()
                        .filter(|value| !value.as_os_str().is_empty())
                        .unwrap_or_else(|| Path::new("."));
                    std::fs::File::open(parent)
                        .and_then(|directory| directory.sync_all())
                        .map_err(storage_error)?;
                }
            } else {
                let transaction = database.begin_read().map_err(storage_error)?;
                let names = transaction
                    .list_tables()
                    .map_err(storage_error)?
                    .map(|table| table.name().to_owned())
                    .collect::<Vec<_>>();
                if names != [TABLE.name()]
                    || transaction
                        .list_multimap_tables()
                        .map_err(storage_error)?
                        .next()
                        .is_some()
                {
                    return Err(Error::IncompatibleStore);
                }
                transaction
                    .open_table(TABLE)
                    .map_err(|_| Error::IncompatibleStore)?;
            }
            Ok::<_, Error>(Arc::new(database))
        })
        .await
        .map_err(worker_error)??;
        Ok(Self { database })
    }

    pub(crate) async fn begin(&self) -> Result<Transaction> {
        let database = self.database.clone();
        let snapshot = tokio::task::spawn_blocking(move || database.begin_read())
            .await
            .map_err(worker_error)?
            .map_err(storage_error)?;
        Ok(Transaction {
            backend: self.clone(),
            snapshot: Arc::new(snapshot),
            reads: BTreeMap::new(),
            mutation_reads: BTreeMap::new(),
            writes: BTreeMap::new(),
            staged_bytes: 0,
            observed_bytes: 0,
            observed_records: 0,
            ranges: Vec::new(),
        })
    }

    pub(crate) async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut transaction = self.begin().await?;
        let result = transaction.get(key).await;
        transaction.rollback().await?;
        result
    }

    pub(crate) async fn shutdown(self) -> Result<()> {
        tokio::task::spawn_blocking(move || match Arc::try_unwrap(self.database) {
            Ok(database) => database.close().map_err(storage_error),
            Err(database) => {
                drop(database);
                Ok(())
            }
        })
        .await
        .map_err(worker_error)?
    }
}

struct ObservedRange {
    start: Vec<u8>,
    end: Vec<u8>,
    limit: usize,
    rows: Vec<Pair>,
}

pub(crate) struct Transaction {
    backend: Backend,
    snapshot: Arc<ReadTransaction>,
    reads: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    mutation_reads: BTreeMap<Vec<u8>, Option<(usize, [u8; 32])>>,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    staged_bytes: usize,
    observed_bytes: usize,
    observed_records: usize,
    ranges: Vec<ObservedRange>,
}

impl Transaction {
    pub(crate) async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(value) = self.writes.get(key).or_else(|| self.reads.get(key)) {
            return Ok(value.clone());
        }
        let owned_key = key.to_vec();
        let snapshot = self.snapshot.clone();
        let value = tokio::task::spawn_blocking(move || {
            let table = snapshot.open_table(TABLE).map_err(storage_error)?;
            let value = table.get(owned_key.as_slice()).map_err(storage_error)?;
            value.map(|value| bounded_copy(value.value())).transpose()
        })
        .await
        .map_err(worker_error)??;
        self.observe_size(
            key.len().saturating_add(value.as_ref().map_or(0, Vec::len)),
            1,
        )?;
        self.reads.insert(key.to_vec(), value.clone());
        Ok(value)
    }

    pub(crate) async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if value.len() > MAX_BATCH_BYTES {
            return Err(Error::Invalid("stored value exceeds 16 MiB".into()));
        }
        let staged_bytes = self.staged_size(&key, value.len())?;
        self.observe_mutation(&key).await?;
        self.writes.insert(key, Some(value));
        self.staged_bytes = staged_bytes;
        Ok(())
    }

    pub(crate) async fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        if self.get(&key).await?.is_some() {
            return Err(Error::DuplicateRecord);
        }
        self.put(key, value).await
    }

    pub(crate) async fn delete(&mut self, key: Vec<u8>) -> Result<()> {
        let staged_bytes = self.staged_size(&key, 0)?;
        self.observe_mutation(&key).await?;
        self.writes.insert(key, None);
        self.staged_bytes = staged_bytes;
        Ok(())
    }

    fn staged_size(&self, key: &[u8], value_bytes: usize) -> Result<usize> {
        let previous = self
            .writes
            .get(key)
            .map_or(0, |value| key.len() + value.as_ref().map_or(0, Vec::len));
        let total = self
            .staged_bytes
            .saturating_sub(previous)
            .saturating_add(key.len())
            .saturating_add(value_bytes);
        if total > MAX_BATCH_BYTES {
            return Err(Error::Invalid("transaction mutations exceed 16 MiB".into()));
        }
        Ok(total)
    }

    fn observe_size(&mut self, bytes: usize, records: usize) -> Result<()> {
        let total_bytes = self.observed_bytes.saturating_add(bytes);
        let total_records = self.observed_records.saturating_add(records);
        if total_bytes > MAX_BATCH_BYTES || total_records > MAX_BATCH_RECORDS {
            return Err(Error::Invalid(
                "transaction observations exceed read limits".into(),
            ));
        }
        self.observed_bytes = total_bytes;
        self.observed_records = total_records;
        Ok(())
    }

    async fn observe_mutation(&mut self, key: &[u8]) -> Result<()> {
        if self.reads.contains_key(key) || self.mutation_reads.contains_key(key) {
            return Ok(());
        }
        let snapshot = self.snapshot.clone();
        let owned_key = key.to_vec();
        let observed = tokio::task::spawn_blocking(move || {
            let table = snapshot.open_table(TABLE).map_err(storage_error)?;
            let value = table.get(owned_key.as_slice()).map_err(storage_error)?;
            Ok::<_, Error>(value.map(|value| fingerprint(value.value())))
        })
        .await
        .map_err(worker_error)??;
        self.observe_size(
            key.len()
                .saturating_add(observed.as_ref().map_or(0, |_| 40)),
            1,
        )?;
        self.mutation_reads.insert(key.to_vec(), observed);
        Ok(())
    }

    /// Fetch a previously size-checked set of values in one blocking operation.
    pub(crate) async fn get_many(&mut self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        let missing: std::collections::BTreeSet<_> = keys
            .iter()
            .filter(|key| !self.writes.contains_key(*key) && !self.reads.contains_key(*key))
            .cloned()
            .collect();
        if !missing.is_empty() {
            let snapshot = self.snapshot.clone();
            let loaded = tokio::task::spawn_blocking(move || {
                let table = snapshot.open_table(TABLE).map_err(storage_error)?;
                let mut total = 0usize;
                let mut loaded = Vec::with_capacity(missing.len());
                for key in missing {
                    let value = table.get(key.as_slice()).map_err(storage_error)?;
                    if let Some(value) = &value {
                        total = total.saturating_add(value.value().len());
                        if total > MAX_BATCH_BYTES {
                            return Err(Error::Invalid("database read exceeds 16 MiB".into()));
                        }
                    }
                    loaded.push((key, value.map(|value| value.value().to_vec())));
                }
                Ok(loaded)
            })
            .await
            .map_err(worker_error)??;
            let bytes = loaded.iter().fold(0usize, |total, (key, value)| {
                total
                    .saturating_add(key.len())
                    .saturating_add(value.as_ref().map_or(0, Vec::len))
            });
            self.observe_size(bytes, loaded.len())?;
            self.reads.extend(loaded);
        }
        let mut total = 0usize;
        let mut result = Vec::with_capacity(keys.len());
        for key in keys {
            let value = self
                .writes
                .get(key)
                .or_else(|| self.reads.get(key))
                .ok_or(Error::IncompatibleStore)?;
            total = total.saturating_add(value.as_ref().map_or(0, Vec::len));
            if total > MAX_BATCH_BYTES {
                return Err(Error::Invalid("database read exceeds 16 MiB".into()));
            }
            result.push(value.clone());
        }
        Ok(result)
    }

    pub(crate) async fn scan(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<Pair>> {
        if start >= end || limit == 0 {
            return Err(Error::Invalid("invalid database scan".into()));
        }
        let pending = self.writes.range(start.to_vec()..end.to_vec()).count();
        let fetch_limit = limit
            .checked_add(pending)
            .ok_or_else(|| Error::Invalid("database scan limit overflow".into()))?;
        let owned_start = start.to_vec();
        let owned_end = end.to_vec();
        let snapshot = self.snapshot.clone();
        let rows = tokio::task::spawn_blocking(move || {
            let table = snapshot.open_table(TABLE).map_err(storage_error)?;
            scan_table(&table, &owned_start, &owned_end, fetch_limit)
        })
        .await
        .map_err(worker_error)??;
        let observed_bytes = rows.iter().fold(
            start.len().saturating_add(end.len()),
            |total, (key, value)| total.saturating_add(key.len()).saturating_add(value.len()),
        );
        self.observe_size(observed_bytes, rows.len().saturating_add(1))?;
        self.ranges.push(ObservedRange {
            start: start.to_vec(),
            end: end.to_vec(),
            limit: fetch_limit,
            rows: rows.clone(),
        });
        let mut merged: BTreeMap<_, _> = rows.into_iter().collect();
        for (key, value) in self.writes.range(start.to_vec()..end.to_vec()) {
            match value {
                Some(value) => {
                    merged.insert(key.clone(), value.clone());
                }
                None => {
                    merged.remove(key);
                }
            }
        }
        Ok(merged.into_iter().take(limit).collect())
    }

    pub(crate) async fn commit(self) -> Result<()> {
        if self.writes.is_empty() {
            return self.rollback().await;
        }
        // The complete writer critical section stays on one blocking worker. No writer lock
        // is retained across an async suspension, even while another process owns the file.
        tokio::task::spawn_blocking(move || {
            let Self {
                backend,
                snapshot,
                reads,
                mutation_reads,
                writes,
                staged_bytes: _,
                observed_bytes: _,
                observed_records: _,
                ranges,
            } = self;
            drop(snapshot);
            let mut transaction = backend.database.begin_write().map_err(storage_error)?;
            transaction
                .set_durability(Durability::Immediate)
                .map_err(storage_error)?;
            {
                let mut table = transaction.open_table(TABLE).map_err(storage_error)?;
                for (key, observed) in reads {
                    let current = table.get(key.as_slice()).map_err(storage_error)?;
                    if current.as_ref().map(|value| value.value()) != observed.as_deref() {
                        return Err(Error::TransactionConflict);
                    }
                }
                for (key, observed) in mutation_reads {
                    let current = table.get(key.as_slice()).map_err(storage_error)?;
                    if current.as_ref().map(|value| fingerprint(value.value())) != observed {
                        return Err(Error::TransactionConflict);
                    }
                }
                for range in ranges {
                    if scan_table(&table, &range.start, &range.end, range.limit)? != range.rows {
                        return Err(Error::TransactionConflict);
                    }
                }
                for (key, value) in writes {
                    match value {
                        Some(value) => {
                            table
                                .insert(key.as_slice(), value.as_slice())
                                .map_err(storage_error)?;
                        }
                        None => {
                            table.remove(key.as_slice()).map_err(storage_error)?;
                        }
                    }
                }
            }
            transaction.commit().map_err(commit_error)
        })
        .await
        // A worker panic can happen after durable commit but before its result is returned.
        .map_err(|_| Error::UnknownCommit)?
    }

    pub(crate) async fn rollback(self) -> Result<()> {
        tokio::task::spawn_blocking(move || drop(self))
            .await
            .map_err(worker_error)
    }
}

fn bounded_copy(value: &[u8]) -> Result<Vec<u8>> {
    if value.len() > MAX_BATCH_BYTES {
        return Err(Error::IncompatibleStore);
    }
    Ok(value.to_vec())
}

fn fingerprint(value: &[u8]) -> (usize, [u8; 32]) {
    let mut digest = [0; 32];
    digest.copy_from_slice(&Sha256::digest(value));
    (value.len(), digest)
}

fn scan_table(
    table: &impl ReadableTable<&'static [u8], &'static [u8]>,
    start: &[u8],
    end: &[u8],
    limit: usize,
) -> Result<Vec<Pair>> {
    let mut bytes = 0usize;
    let mut rows = Vec::new();
    for row in table.range(start..end).map_err(storage_error)?.take(limit) {
        let (key, value) = row.map_err(storage_error)?;
        bytes = bytes
            .saturating_add(key.value().len())
            .saturating_add(value.value().len());
        if bytes > MAX_BATCH_BYTES {
            return Err(Error::Invalid("database scan exceeds 16 MiB".into()));
        }
        rows.push((key.value().to_vec(), value.value().to_vec()));
    }
    Ok(rows)
}

fn commit_error(error: redb::CommitError) -> Error {
    match error {
        redb::CommitError::TransactionPoisoned => {
            Error::Database("redb transaction was poisoned".into())
        }
        _ => Error::UnknownCommit,
    }
}

#[cfg(test)]
mod tests {
    use std::io;
    use std::sync::atomic::{AtomicBool, Ordering};

    use redb::StorageBackend;

    use super::*;
    use crate::{
        CommitBatch, ProjectionMutation, Store, request_digest, store_key,
        test_support::TestFixture,
    };

    #[tokio::test]
    async fn stale_key_and_range_observations_cannot_commit() {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        let peer = fixture.connect().await;
        let first = store_key("test/range/a");
        let second = store_key("test/range/b");
        let outcome = store_key("test/outcome");
        let mut seed = store.db.begin().await.unwrap();
        seed.put(first.clone(), b"old".to_vec()).await.unwrap();
        seed.commit().await.unwrap();

        let mut stale = store.db.begin().await.unwrap();
        assert_eq!(stale.get(&first).await.unwrap(), Some(b"old".to_vec()));
        let mut fresh = peer.db.begin().await.unwrap();
        fresh.put(first.clone(), b"new".to_vec()).await.unwrap();
        fresh.commit().await.unwrap();
        stale
            .put(outcome.clone(), b"stale decision".to_vec())
            .await
            .unwrap();
        assert!(matches!(
            stale.commit().await,
            Err(Error::TransactionConflict)
        ));
        assert!(store.db.read(&outcome).await.unwrap().is_none());

        let mut stale = store.db.begin().await.unwrap();
        let start = store_key("test/range/");
        let end = store_key("test/range0");
        assert_eq!(
            stale.scan(&start, &end, 2).await.unwrap(),
            [(first.clone(), b"new".to_vec())]
        );
        let mut fresh = peer.db.begin().await.unwrap();
        fresh
            .insert(second.clone(), b"phantom".to_vec())
            .await
            .unwrap();
        fresh.commit().await.unwrap();
        stale
            .put(outcome.clone(), b"stale range decision".to_vec())
            .await
            .unwrap();
        assert!(matches!(
            stale.commit().await,
            Err(Error::TransactionConflict)
        ));
        assert!(store.db.read(&outcome).await.unwrap().is_none());

        let mut buffered = store.db.begin().await.unwrap();
        buffered.delete(first).await.unwrap();
        buffered
            .put(second.clone(), b"buffered".to_vec())
            .await
            .unwrap();
        assert_eq!(
            buffered.scan(&start, &end, 1).await.unwrap(),
            [(second, b"buffered".to_vec())]
        );
        buffered.rollback().await.unwrap();
    }

    #[tokio::test]
    async fn opening_and_initializing_preserve_missing_or_incompatible_files() {
        let fixture = TestFixture::start().await;
        assert!(Store::open_local(fixture.path()).await.is_err());
        assert!(!fixture.path().exists());

        let original = b"an existing unrelated file";
        std::fs::write(fixture.path(), original).unwrap();
        assert!(Store::initialize_local(fixture.path()).await.is_err());
        assert_eq!(std::fs::read(fixture.path()).unwrap(), original);
        std::fs::remove_file(fixture.path()).unwrap();

        let store = fixture.initialize().await;
        let again = Store::initialize_local(fixture.path()).await.unwrap();
        assert_eq!(
            store.snapshot().await.unwrap(),
            again.snapshot().await.unwrap()
        );
        store.shutdown().await.unwrap();
        again.shutdown().await.unwrap();

        let foreign = fixture.path().with_file_name("foreign.redb");
        let path = foreign.clone();
        tokio::task::spawn_blocking(move || {
            let database = Database::create(path).unwrap();
            let transaction = database.begin_write().unwrap();
            {
                let mut table = transaction
                    .open_table(TableDefinition::<u64, u64>::new("foreign"))
                    .unwrap();
                table.insert(1, 77).unwrap();
            }
            transaction.commit().unwrap();
        })
        .await
        .unwrap();
        assert!(matches!(
            Store::initialize_local(&foreign).await,
            Err(Error::IncompatibleStore)
        ));
        tokio::task::spawn_blocking(move || {
            let database = Database::open(foreign).unwrap();
            let transaction = database.begin_read().unwrap();
            let table = transaction
                .open_table(TableDefinition::<u64, u64>::new("foreign"))
                .unwrap();
            assert_eq!(table.get(1).unwrap().unwrap().value(), 77);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn initialization_rejects_data_without_a_store_identity() {
        let fixture = TestFixture::start().await;
        let backend = Backend::open(fixture.path(), true).await.unwrap();
        let mut transaction = backend.begin().await.unwrap();
        transaction
            .put(b"unrelated-key".to_vec(), b"preserve this".to_vec())
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert!(matches!(
            Store::initialize_local(fixture.path()).await,
            Err(Error::IncompatibleStore)
        ));
        assert_eq!(
            backend.read(b"unrelated-key").await.unwrap(),
            Some(b"preserve this".to_vec())
        );
        assert!(
            backend
                .read(&store_key("meta/state"))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn clean_file_payload_corruption_is_rejected_on_open_and_initialize() {
        const ORIGINAL: &[u8] = b"UNIQUE_PROJECTION_PAYLOAD_ORIGINAL";
        const CORRUPTED: &[u8] = b"UNIQUE_PROJECTION_PAYLOAD_CORRUPTD";
        assert_eq!(ORIGINAL.len(), CORRUPTED.len());
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        let payload = serde_json::json!({"marker": std::str::from_utf8(ORIGINAL).unwrap()});
        let command = CommitBatch {
            command_id: "corruption-command".into(),
            request_digest: request_digest(&payload),
            expected_revision: 0,
            events: Vec::new(),
            projections: vec![ProjectionMutation::Put {
                key: "project/corruption".into(),
                payload,
            }],
            effects: Vec::new(),
            result: serde_json::Value::Null,
        };
        store.commit(&command).await.unwrap();
        store.shutdown().await.unwrap();

        // Alter valid JSON without changing its length, after the clean-close marker was saved.
        // Application-level JSON and payload-size validation alone cannot detect this damage.
        let mut bytes = std::fs::read(fixture.path()).unwrap();
        let positions = bytes
            .windows(ORIGINAL.len())
            .enumerate()
            .filter_map(|(position, value)| (value == ORIGINAL).then_some(position))
            .collect::<Vec<_>>();
        assert_eq!(
            positions.len(),
            1,
            "fixture must contain one payload marker"
        );
        bytes[positions[0]..positions[0] + CORRUPTED.len()].copy_from_slice(CORRUPTED);
        for initialize in [false, true] {
            let path = fixture.path().with_file_name(if initialize {
                "corrupt-initialize.redb"
            } else {
                "corrupt-open.redb"
            });
            std::fs::write(&path, &bytes).unwrap();
            let opened = if initialize {
                Store::initialize_local(&path).await
            } else {
                Store::open_local(&path).await
            };
            if let Ok(store) = opened {
                let observed = store.snapshot().await.unwrap();
                store.shutdown().await.unwrap();
                panic!(
                    "checksum-corrupt database was accepted (initialize={initialize}), returning {:?}",
                    observed.projections
                );
            }
        }
    }

    #[derive(Debug)]
    struct FailingSync {
        inner: redb::backends::InMemoryBackend,
        fail: Arc<AtomicBool>,
    }

    impl StorageBackend for FailingSync {
        fn len(&self) -> io::Result<u64> {
            self.inner.len()
        }
        fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
            StorageBackend::read(&self.inner, offset, out)
        }
        fn set_len(&self, len: u64) -> io::Result<()> {
            self.inner.set_len(len)
        }
        fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
            StorageBackend::write(&self.inner, offset, data)
        }
        fn sync_data(&self) -> io::Result<()> {
            if self.fail.load(Ordering::SeqCst) {
                Err(io::Error::other("injected durable flush failure"))
            } else {
                self.inner.sync_data()
            }
        }
    }

    #[tokio::test]
    async fn real_engine_flush_failure_has_an_unknown_commit_outcome() {
        let fail = Arc::new(AtomicBool::new(false));
        let control = fail.clone();
        let database = tokio::task::spawn_blocking(move || {
            let database = Database::builder()
                .create_with_backend(FailingSync {
                    inner: redb::backends::InMemoryBackend::new(),
                    fail: control,
                })
                .unwrap();
            let transaction = database.begin_write().unwrap();
            transaction.open_table(TABLE).unwrap();
            transaction.commit().unwrap();
            database
        })
        .await
        .unwrap();
        let backend = Backend {
            database: Arc::new(database),
        };
        let mut transaction = backend.begin().await.unwrap();
        transaction
            .put(b"receipt".to_vec(), b"possibly committed".to_vec())
            .await
            .unwrap();
        fail.store(true, Ordering::SeqCst);
        assert!(matches!(
            transaction.commit().await,
            Err(Error::UnknownCommit)
        ));
        fail.store(false, Ordering::SeqCst);
        // redb retains the earlier I/O failure; explicit close must report it too.
        assert!(matches!(backend.shutdown().await, Err(Error::Database(_))));
    }
}
