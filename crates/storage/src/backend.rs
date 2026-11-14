use std::cell::Cell;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock, TryLockError, mpsc};
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

use redb::{
    BackendError, ConcurrencyMode, Database, Durability, ReadOnlyDatabase, ReadOnlyTable,
    ReadTransaction, ReadableDatabase, ReadableTable, StorageBackend, TableDefinition, TableHandle,
    WriteTransaction,
};
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use crate::{Error, MAX_BATCH_BYTES, MAX_BATCH_RECORDS, ResourceLimit, Result};

const TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("t3_code_ls_v2");
type Pair = (Vec<u8>, Vec<u8>);
type SnapshotTable = ReadOnlyTable<&'static [u8], &'static [u8]>;
type SnapshotTableCache = Arc<OnceLock<SnapshotTable>>;

fn with_snapshot_table<T>(
    snapshot: &ReadTransaction,
    cache: Option<&OnceLock<SnapshotTable>>,
    read: impl FnOnce(&SnapshotTable) -> Result<T>,
) -> Result<T> {
    if let Some(cache) = cache {
        if cache.get().is_none() {
            // Initialization stays at the first actual read, and a failed open
            // is never retained. An in-flight cancelled read may finish opening
            // the same immutable snapshot; either successful handle is valid.
            let table = snapshot.open_table(TABLE).map_err(storage_error)?;
            let _ = cache.set(table);
        }
        return read(
            cache
                .get()
                .expect("successful snapshot table initialization"),
        );
    }
    let table = snapshot.open_table(TABLE).map_err(storage_error)?;
    read(&table)
}

fn storage_error(_: impl std::fmt::Display) -> Error {
    // Database errors may contain file names or stored keys. Keep them out of IPC/log output.
    Error::Database("redb storage operation failed".into())
}

fn worker_error(_: tokio::task::JoinError) -> Error {
    read_worker_error()
}

fn read_worker_error() -> Error {
    Error::Database("database worker failed".into())
}

thread_local! {
    static INLINE_READS: Cell<bool> = const { Cell::new(false) };
}

struct InlineReadScope(bool);

impl InlineReadScope {
    fn enter() -> Self {
        Self(INLINE_READS.replace(true))
    }
}

impl Drop for InlineReadScope {
    fn drop(&mut self) {
        INLINE_READS.set(self.0);
    }
}

/// Run the serial database dispatcher on a joined, owned OS thread. Only its
/// direct calls may execute bounded engine reads inline; Tokio tasks and normal
/// library callers continue offloading those reads. Writer reservation, commit,
/// and lifecycle operations always retain their separate workers.
///
/// This blocks the caller until `run` returns and is intended for synchronous
/// process entrypoints. The owned thread has an explicit 8 MiB stack for the
/// helper's bounded request parser, rather than the smaller spawned default.
pub fn run_on_database_thread<F, T>(run: F) -> io::Result<T>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let worker = std::thread::Builder::new()
        .name("t3-db-dispatch".into())
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            let _scope = InlineReadScope::enter();
            run()
        })?;
    match worker.join() {
        Ok(output) => Ok(output),
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

async fn execute_read<F, T>(read: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    // A spawned task can be polled on an otherwise permitted thread (for
    // example by a LocalSet). Never let that task inherit the caller's scope.
    if INLINE_READS.get() && tokio::task::try_id().is_none() {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(read))
            .map_err(|_| read_worker_error())?
    } else {
        tokio::task::spawn_blocking(read)
            .await
            .map_err(worker_error)?
    }
}

#[derive(Clone)]
pub(crate) struct Backend {
    handles: Arc<DatabaseHandles>,
}

struct DatabaseHandles {
    path: PathBuf,
    exclusive: bool,
    read_only: bool,
    identity: Option<same_file::Handle>,
    reader: Option<ReadOnlyDatabase>,
    writer: Mutex<Option<Arc<Database>>>,
    opening: Mutex<()>,
}

impl DatabaseHandles {
    fn verify_identity(&self) -> Result<()> {
        if let Some(expected) = &self.identity {
            let actual = same_file::Handle::from_path(&self.path).map_err(storage_error)?;
            if actual != *expected {
                return Err(Error::IncompatibleStore);
            }
        }
        Ok(())
    }

    fn begin_read(&self) -> Result<ReadTransaction> {
        self.verify_identity()?;
        if let Some(writer) = self.writer.lock().map_err(storage_error)?.clone() {
            return writer.begin_read().map_err(storage_error);
        }
        self.reader
            .as_ref()
            .ok_or(Error::IncompatibleStore)?
            .begin_read()
            .map_err(storage_error)
    }

    fn try_writer(&self) -> Result<Arc<Database>> {
        if self.read_only {
            return Err(Error::Invalid(
                "read-only database handle cannot acquire a writer".into(),
            ));
        }
        self.verify_identity()?;
        if let Some(writer) = self.writer.lock().map_err(storage_error)?.clone() {
            return Ok(writer);
        }
        let _opening = match self.opening.try_lock() {
            Ok(guard) => guard,
            Err(TryLockError::WouldBlock) => return Err(Error::WriterBusy),
            Err(error) => return Err(storage_error(error)),
        };
        if let Some(writer) = self.writer.lock().map_err(storage_error)?.clone() {
            return Ok(writer);
        }
        let expected = self.identity.as_ref().ok_or(Error::IncompatibleStore)?;
        let writer = Arc::new(checked_open(&self.path, expected)?);
        self.verify_identity()?;
        validate_tables(&writer.begin_read().map_err(storage_error)?)?;
        *self.writer.lock().map_err(storage_error)? = Some(writer.clone());
        Ok(writer)
    }

    fn wait_for_writer(&self) -> Result<Arc<Database>> {
        loop {
            match self.try_writer() {
                Err(Error::WriterBusy) => std::thread::sleep(Duration::from_millis(5)),
                result => return result,
            }
        }
    }
}

fn builder() -> redb::Builder {
    let mut builder = Database::builder();
    builder
        .set_concurrency_mode(ConcurrencyMode::MultiWriter)
        .set_check_integrity_on_open(true)
        .set_cache_size(32 * 1024 * 1024);
    builder
}

fn validate_tables(transaction: &ReadTransaction) -> Result<()> {
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
    Ok(())
}

#[derive(Debug)]
struct OpenLocks {
    opening: AtomicBool,
    writer_acquired: AtomicBool,
    busy: AtomicBool,
    mutated: AtomicBool,
}

/// redb takes the writer lock while opening a writable multi-process handle.
/// Only the initial writer acquisition is nonblocking. Once owned, redb's
/// header, recovery and consistency locks retain their normal wait behavior.
#[derive(Debug)]
struct NonblockingOpen {
    file: redb::backends::FileBackend,
    locks: Arc<OpenLocks>,
}

impl NonblockingOpen {
    fn lock(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
        shared: bool,
    ) -> std::result::Result<(), BackendError> {
        if !self.locks.opening.load(Ordering::Acquire)
            || self.locks.writer_acquired.load(Ordering::Acquire)
        {
            return if shared {
                self.file.lock_shared_range(start, end)
            } else {
                self.file.lock_range(start, end)
            };
        }
        let acquired = if shared {
            self.file.try_lock_shared_range(start, end)?
        } else {
            self.file.try_lock_range(start, end)?
        };
        if acquired {
            // In pinned redb 4.5 MultiWriter open, mode locks use try_lock_*,
            // then the first blocking exclusive acquisition is the writer lock.
            // Keep this phase separate from `opening`, which also prevents the
            // custom-backend create entry point from initializing an empty file.
            if !shared {
                self.locks.writer_acquired.store(true, Ordering::Release);
            }
            Ok(())
        } else {
            self.locks.busy.store(true, Ordering::Release);
            Err(BackendError::Io(io::Error::new(
                io::ErrorKind::WouldBlock,
                "database open lock is busy",
            )))
        }
    }
}

impl StorageBackend for NonblockingOpen {
    fn len(&self) -> io::Result<u64> {
        let length = self.file.len()?;
        // create_with_backend is redb's only writable custom-backend entry point.
        // Refuse empty files under its own open lock so this remains an open,
        // never implicit initialization of a missing or empty store.
        if self.locks.opening.load(Ordering::Acquire) && length == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "database file is empty",
            ));
        }
        Ok(length)
    }
    fn read(&self, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.file.read(offset, out)
    }
    fn set_len(&self, length: u64) -> io::Result<()> {
        self.locks.mutated.store(true, Ordering::Release);
        self.file.set_len(length)
    }
    fn sync_data(&self) -> io::Result<()> {
        self.file.sync_data()
    }
    fn write(&self, offset: u64, data: &[u8]) -> io::Result<()> {
        self.locks.mutated.store(true, Ordering::Release);
        self.file.write(offset, data)
    }
    fn close(&self) -> io::Result<()> {
        self.file.close()
    }
    fn try_lock_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> std::result::Result<bool, BackendError> {
        self.file.try_lock_range(start, end)
    }
    fn try_lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> std::result::Result<bool, BackendError> {
        self.file.try_lock_shared_range(start, end)
    }
    fn lock_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> std::result::Result<(), BackendError> {
        self.lock(start, end, false)
    }
    fn lock_shared_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> std::result::Result<(), BackendError> {
        self.lock(start, end, true)
    }
    fn unlock_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> std::result::Result<(), BackendError> {
        self.file.unlock_range(start, end)
    }
    fn query_lock_range(
        &self,
        start: Bound<u64>,
        end: Bound<u64>,
    ) -> std::result::Result<bool, BackendError> {
        self.file.query_lock_range(start, end)
    }
}

fn file_identity(file: &std::fs::File) -> Result<same_file::Handle> {
    same_file::Handle::from_file(file.try_clone().map_err(storage_error)?).map_err(storage_error)
}

fn checked_open(path: &Path, identity: &same_file::Handle) -> Result<Database> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .map_err(storage_error)?;
    if file_identity(&file)? != *identity {
        return Err(Error::IncompatibleStore);
    }
    let file = redb::backends::FileBackend::new(file).map_err(storage_error)?;
    let locks = Arc::new(OpenLocks {
        opening: AtomicBool::new(true),
        writer_acquired: AtomicBool::new(false),
        busy: AtomicBool::new(false),
        mutated: AtomicBool::new(false),
    });
    let result = builder().create_with_backend(NonblockingOpen {
        file,
        locks: locks.clone(),
    });
    locks.opening.store(false, Ordering::Release);
    result.map_err(|error| {
        if locks.busy.load(Ordering::Acquire) && !locks.mutated.load(Ordering::Acquire) {
            Error::WriterBusy
        } else {
            storage_error(error)
        }
    })
}

impl Backend {
    pub(crate) async fn open(path: impl AsRef<Path>, initialize: bool) -> Result<Self> {
        let path = path.as_ref().to_owned();
        if path.as_os_str().is_empty() {
            return Err(Error::Invalid("database file path is empty".into()));
        }
        let path = if path.is_absolute() {
            path
        } else {
            std::env::current_dir().map_err(storage_error)?.join(path)
        };
        let handles = tokio::task::spawn_blocking(move || {
            let identity;
            let database = if initialize {
                let mut options = OpenOptions::new();
                options.read(true).write(true).create(true).truncate(false);
                #[cfg(unix)]
                options.mode(0o600);
                let file = options.open(&path).map_err(storage_error)?;
                identity = file_identity(&file)?;
                let file = redb::backends::FileBackend::new(file).map_err(storage_error)?;
                builder().create_with_backend(file).map_err(storage_error)?
            } else {
                identity = same_file::Handle::from_path(&path).map_err(storage_error)?;
                let started = Instant::now();
                loop {
                    match checked_open(&path, &identity) {
                        Ok(database) => break database,
                        Err(Error::WriterBusy) => {
                            // A live writer prevents a checked writable open. Readers
                            // still observe the last committed snapshot. A later writer
                            // promotion must finish the integrity check before begin.
                            let file = OpenOptions::new()
                                .read(true)
                                .open(&path)
                                .map_err(storage_error)?;
                            if file_identity(&file)? != identity {
                                return Err(Error::IncompatibleStore);
                            }
                            let file =
                                redb::backends::FileBackend::new(file).map_err(storage_error)?;
                            let reader = match builder().open_read_only_with_backend(file) {
                                Ok(reader) => reader,
                                // A peer can own the writer while recovering a crashed
                                // file, before redb publishes its consistency marker.
                                // This read-only attempt wrote nothing. Rejoin checked
                                // open instead of treating unready recovery as corruption.
                                Err(redb::DatabaseError::RepairAborted) => {
                                    if started.elapsed() >= Duration::from_secs(5) {
                                        return Err(Error::WriterBusy);
                                    }
                                    std::thread::sleep(Duration::from_millis(5));
                                    continue;
                                }
                                Err(error) => return Err(storage_error(error)),
                            };
                            validate_tables(&reader.begin_read().map_err(storage_error)?)?;
                            return Ok(Arc::new(DatabaseHandles {
                                path,
                                exclusive: false,
                                read_only: false,
                                identity: Some(identity),
                                reader: Some(reader),
                                writer: Mutex::new(None),
                                opening: Mutex::new(()),
                            }));
                        }
                        Err(error) => return Err(error),
                    }
                }
            };
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
                validate_tables(&database.begin_read().map_err(storage_error)?)?;
            }
            Ok::<_, Error>(Arc::new(DatabaseHandles {
                path,
                exclusive: false,
                read_only: false,
                identity: Some(identity),
                reader: None,
                writer: Mutex::new(Some(Arc::new(database))),
                opening: Mutex::new(()),
            }))
        })
        .await
        .map_err(worker_error)??;
        Ok(Self { handles })
    }

    /// Inspect an existing store without a writable descriptor, integrity repair,
    /// schema initialization, or later promotion to a writer.
    pub(crate) async fn open_read_only(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        if !path.is_absolute() {
            return Err(Error::Invalid("database path must be absolute".into()));
        }
        let handles = tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new()
                .read(true)
                .open(&path)
                .map_err(storage_error)?;
            let identity = file_identity(&file)?;
            let file = redb::backends::FileBackend::new(file).map_err(storage_error)?;
            let reader = builder()
                .open_read_only_with_backend(file)
                .map_err(storage_error)?;
            if same_file::Handle::from_path(&path).map_err(storage_error)? != identity {
                return Err(Error::IncompatibleStore);
            }
            validate_tables(&reader.begin_read().map_err(storage_error)?)?;
            Ok::<_, Error>(Arc::new(DatabaseHandles {
                path,
                exclusive: false,
                read_only: true,
                identity: Some(identity),
                reader: Some(reader),
                writer: Mutex::new(None),
                opening: Mutex::new(()),
            }))
        })
        .await
        .map_err(worker_error)??;
        Ok(Self { handles })
    }

    /// An offline owner excludes every open omdb handle, including idle readers.
    pub(crate) async fn open_exclusive(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_owned();
        if !path.is_absolute() {
            return Err(Error::Invalid("database path must be absolute".into()));
        }
        let handles = tokio::task::spawn_blocking(move || {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .map_err(storage_error)?;
            if file.metadata().map_err(storage_error)?.len() == 0 {
                return Err(Error::IncompatibleStore);
            }
            let identity = file_identity(&file)?;
            let file = redb::backends::FileBackend::new(file).map_err(storage_error)?;
            let mut options = builder();
            options.set_concurrency_mode(ConcurrencyMode::ExclusiveWriter);
            let database = options
                .create_with_backend(file)
                .map_err(|error| match error {
                    redb::DatabaseError::DatabaseAlreadyOpen => Error::WriterBusy,
                    other => storage_error(other),
                })?;
            if same_file::Handle::from_path(&path).map_err(storage_error)? != identity {
                return Err(Error::IncompatibleStore);
            }
            validate_tables(&database.begin_read().map_err(storage_error)?)?;
            Ok::<_, Error>(Arc::new(DatabaseHandles {
                path,
                exclusive: true,
                read_only: false,
                identity: Some(identity),
                reader: None,
                writer: Mutex::new(Some(Arc::new(database))),
                opening: Mutex::new(()),
            }))
        })
        .await
        .map_err(worker_error)??;
        Ok(Self { handles })
    }

    pub(crate) async fn storage_statistics(
        &self,
    ) -> Result<crate::application_storage::StorageStatistics> {
        let handles = self.handles.clone();
        tokio::task::spawn_blocking(move || {
            let writer = handles.try_writer()?;
            let transaction = writer.try_begin_write().map_err(|error| match error {
                redb::TransactionError::WouldBlock => Error::WriterBusy,
                other => storage_error(other),
            })?;
            let stats = transaction.stats().map_err(storage_error)?;
            let database_bytes = std::fs::metadata(&handles.path)
                .map_err(storage_error)?
                .len();
            let allocated_bytes = stats
                .allocated_pages()
                .checked_mul(stats.page_size() as u64)
                .ok_or(Error::IncompatibleStore)?;
            let accounted_bytes = stats
                .stored_bytes()
                .checked_add(stats.metadata_bytes())
                .ok_or(Error::IncompatibleStore)?;
            // The engine includes both free pages and within-page slack in fragmentation.
            // Subtract all unaccounted allocated capacity, a conservative upper bound on
            // within-page slack. This is reusable capacity, not removable file bytes:
            // the allocator may retain empty regions even after offline compaction.
            let reusable_bytes = stats
                .fragmented_bytes()
                .saturating_sub(allocated_bytes.saturating_sub(accounted_bytes))
                .min(database_bytes);
            let result = crate::application_storage::StorageStatistics {
                database_bytes,
                // Pinned omdb has no guaranteed physical-shrink prediction API.
                reclaimable_bytes: 0,
                reusable_bytes,
                fragmented_bytes: stats.fragmented_bytes(),
                allocated_bytes,
            };
            handles.verify_identity()?;
            transaction.abort().map_err(storage_error)?;
            Ok(result)
        })
        .await
        .map_err(worker_error)?
    }

    /// A verified logical snapshot, never a copy of a live, mutating file.
    pub(crate) async fn storage_backup(
        &self,
        destination: PathBuf,
    ) -> Result<crate::application_storage::StorageBackup> {
        if !destination.is_absolute() {
            return Err(Error::Invalid("backup path must be absolute".into()));
        }
        let handles = self.handles.clone();
        tokio::task::spawn_blocking(move || {
            let source = handles.begin_read()?;
            validate_tables(&source)?;
            let mut options = OpenOptions::new();
            options.read(true).write(true).create_new(true);
            #[cfg(unix)]
            options.mode(0o600);
            let file = options.open(&destination).map_err(storage_error)?;
            let backup_identity = file_identity(&file)?;
            let file = redb::backends::FileBackend::new(file).map_err(storage_error)?;
            let mut builder = redb::Database::builder();
            builder
                .set_cache_size(32 * 1024 * 1024)
                .set_check_integrity_on_open(true);
            let backup = builder.create_with_backend(file).map_err(storage_error)?;
            let mut write = backup.begin_write().map_err(storage_error)?;
            write
                .set_durability(Durability::Immediate)
                .map_err(storage_error)?;
            let mut count = 0u64;
            let mut digest = Sha256::new();
            {
                let input = source.open_table(TABLE).map_err(storage_error)?;
                let mut output = write.open_table(TABLE).map_err(storage_error)?;
                for entry in input.iter().map_err(storage_error)? {
                    let (key, value) = entry.map_err(storage_error)?;
                    output
                        .insert(key.value(), value.value())
                        .map_err(storage_error)?;
                    digest.update((key.value().len() as u64).to_le_bytes());
                    digest.update(key.value());
                    digest.update((value.value().len() as u64).to_le_bytes());
                    digest.update(value.value());
                    count = count.checked_add(1).ok_or(Error::IncompatibleStore)?;
                }
            }
            write.commit().map_err(commit_error)?;
            drop(backup);
            let verified_file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&destination)
                .map_err(storage_error)?;
            if file_identity(&verified_file)? != backup_identity {
                return Err(Error::IncompatibleStore);
            }
            let backup = builder
                .create_with_backend(
                    redb::backends::FileBackend::new(verified_file).map_err(storage_error)?,
                )
                .map_err(storage_error)?;
            let verify = backup.begin_read().map_err(storage_error)?;
            validate_tables(&verify)?;
            let mut verified_count = 0u64;
            let mut verified_digest = Sha256::new();
            for entry in verify
                .open_table(TABLE)
                .map_err(storage_error)?
                .iter()
                .map_err(storage_error)?
            {
                let (key, value) = entry.map_err(storage_error)?;
                verified_digest.update((key.value().len() as u64).to_le_bytes());
                verified_digest.update(key.value());
                verified_digest.update((value.value().len() as u64).to_le_bytes());
                verified_digest.update(value.value());
                verified_count = verified_count
                    .checked_add(1)
                    .ok_or(Error::IncompatibleStore)?;
            }
            let digest = digest.finalize();
            if count != verified_count || digest != verified_digest.finalize() {
                return Err(Error::IncompatibleStore);
            }
            drop(verify);
            drop(backup);
            handles.verify_identity()?;
            if same_file::Handle::from_path(&destination).map_err(storage_error)? != backup_identity
            {
                return Err(Error::IncompatibleStore);
            }
            #[cfg(unix)]
            {
                std::fs::File::open(destination.parent().ok_or(Error::IncompatibleStore)?)
                    .and_then(|directory| directory.sync_all())
                    .map_err(storage_error)?;
            }
            Ok(crate::application_storage::StorageBackup {
                rows: count,
                bytes: std::fs::metadata(&destination)
                    .map_err(storage_error)?
                    .len(),
                checksum: digest.iter().map(|byte| format!("{byte:02x}")).collect(),
            })
        })
        .await
        .map_err(worker_error)?
    }

    pub(crate) async fn storage_compact(
        &self,
    ) -> Result<crate::application_storage::StorageCompaction> {
        let handles = self.handles.clone();
        tokio::task::spawn_blocking(move || {
            handles.verify_identity()?;
            if !handles.exclusive {
                return Err(Error::Invalid(
                    "physical compaction requires an exclusive offline handle".into(),
                ));
            }
            let mut writer = handles.writer.lock().map_err(storage_error)?;
            let database = Arc::get_mut(writer.as_mut().ok_or(Error::IncompatibleStore)?)
                .ok_or(Error::WriterBusy)?;
            let before_bytes = std::fs::metadata(&handles.path)
                .map_err(storage_error)?
                .len();
            database.compact().map_err(storage_error)?;
            handles.verify_identity()?;
            Ok(crate::application_storage::StorageCompaction {
                before_bytes,
                after_bytes: std::fs::metadata(&handles.path)
                    .map_err(storage_error)?
                    .len(),
            })
        })
        .await
        .map_err(worker_error)?
    }

    pub(crate) fn require_exclusive(&self) -> Result<()> {
        if !self.handles.exclusive || self.handles.read_only {
            return Err(Error::Invalid(
                "native schema upgrade requires an exclusive offline handle".into(),
            ));
        }
        self.handles.verify_identity()
    }

    pub(crate) async fn begin(&self) -> Result<Transaction> {
        let handles = self.handles.clone();
        let snapshot = tokio::task::spawn_blocking(move || handles.begin_read())
            .await
            .map_err(worker_error)??;
        Ok(Transaction {
            backend: self.clone(),
            snapshot: Arc::new(snapshot),
            read_table: self.handles.read_only.then(|| Arc::new(OnceLock::new())),
            reads: BTreeMap::new(),
            mutation_reads: BTreeMap::new(),
            writes: BTreeMap::new(),
            staged_bytes: 0,
            observed_bytes: 0,
            observed_records: 0,
            ranges: Vec::new(),
            reserved: None,
            read_only: self.handles.read_only,
        })
    }

    pub(crate) async fn begin_read_only(&self) -> Result<Transaction> {
        let mut transaction = self.begin().await?;
        transaction.read_only = true;
        transaction
            .read_table
            .get_or_insert_with(|| Arc::new(OnceLock::new()));
        Ok(transaction)
    }

    /// Acquire the database's cross-process writer before the caller starts work.
    /// A blocking worker owns the redb writer until commit, cancellation, or expiry.
    pub(crate) async fn begin_reserved(
        &self,
        lock_timeout: Duration,
        idle_timeout: Duration,
    ) -> Result<Transaction> {
        let (commands, receive) = mpsc::sync_channel(1);
        let (ready, snapshot) = oneshot::channel();
        let state = Arc::new(Mutex::new(ReservationState {
            touched: Instant::now(),
            closed: false,
            idle_timeout,
        }));
        let worker_state = state.clone();
        let handles = self.handles.clone();
        let worker = tokio::task::spawn_blocking(move || {
            reserved_worker(handles, worker_state, receive, ready, lock_timeout);
        });
        let reserved = ReservedWriter {
            commands: Some(commands),
            worker: Some(worker),
            state,
        };
        let snapshot = snapshot.await.map_err(worker_error_from_channel)??;
        Ok(Transaction {
            backend: self.clone(),
            snapshot,
            read_table: None,
            reads: BTreeMap::new(),
            mutation_reads: BTreeMap::new(),
            writes: BTreeMap::new(),
            staged_bytes: 0,
            observed_bytes: 0,
            observed_records: 0,
            ranges: Vec::new(),
            reserved: Some(reserved),
            read_only: false,
        })
    }

    pub(crate) async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let mut transaction = self.begin().await?;
        let result = transaction.get(key).await;
        transaction.rollback().await?;
        result
    }

    pub(crate) async fn shutdown(self) -> Result<()> {
        tokio::task::spawn_blocking(move || match Arc::try_unwrap(self.handles) {
            Ok(handles) => {
                if let Some(writer) = handles.writer.into_inner().map_err(storage_error)? {
                    match Arc::try_unwrap(writer) {
                        Ok(writer) => writer.close().map_err(storage_error)?,
                        Err(writer) => drop(writer),
                    }
                }
                if let Some(reader) = handles.reader {
                    reader.close().map_err(storage_error)?;
                }
                Ok(())
            }
            Err(handles) => {
                drop(handles);
                Ok(())
            }
        })
        .await
        .map_err(worker_error)?
    }
}

#[derive(Clone)]
struct ObservedRange {
    start: Vec<u8>,
    end: Vec<u8>,
    limit: usize,
    rows: Vec<Pair>,
}

pub(crate) struct Transaction {
    backend: Backend,
    snapshot: Arc<ReadTransaction>,
    // Only immutable read transactions retain a table and its root page. This
    // cache belongs to exactly `snapshot`, stores no rows, and is dropped with
    // the transaction. Writers keep opening their table through the old path.
    read_table: Option<SnapshotTableCache>,
    reads: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    mutation_reads: BTreeMap<Vec<u8>, Option<(usize, [u8; 32])>>,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    staged_bytes: usize,
    observed_bytes: usize,
    observed_records: usize,
    ranges: Vec<ObservedRange>,
    reserved: Option<ReservedWriter>,
    read_only: bool,
}

/// Only staged state changes at savepoints. The reserved writer has not applied
/// any mutation yet, so restoring this state cannot publish or undo a commit.
#[derive(Clone)]
pub(crate) struct TransactionCheckpoint {
    reads: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    mutation_reads: BTreeMap<Vec<u8>, Option<(usize, [u8; 32])>>,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    staged_bytes: usize,
    observed_bytes: usize,
    observed_records: usize,
    ranges: Vec<ObservedRange>,
}

impl Transaction {
    pub(crate) fn is_active(&self) -> bool {
        self.reserved.as_ref().is_none_or(|reserved| {
            !reserved
                .worker
                .as_ref()
                .is_some_and(|worker| worker.is_finished())
                && reserved.state.lock().is_ok_and(|state| {
                    !state.closed && state.touched.elapsed() < state.idle_timeout
                })
        })
    }

    pub(crate) fn owns_writer(&self) -> bool {
        self.reserved.is_some()
    }

    pub(crate) fn ensure_active(&self) -> Result<()> {
        if let Some(reserved) = &self.reserved {
            reserved.touch()?;
        }
        Ok(())
    }

    pub(crate) fn checkpoint_bytes(&self) -> usize {
        self.staged_bytes.saturating_add(self.observed_bytes)
    }

    pub(crate) fn checkpoint(&self) -> TransactionCheckpoint {
        TransactionCheckpoint {
            reads: self.reads.clone(),
            mutation_reads: self.mutation_reads.clone(),
            writes: self.writes.clone(),
            staged_bytes: self.staged_bytes,
            observed_bytes: self.observed_bytes,
            observed_records: self.observed_records,
            ranges: self.ranges.clone(),
        }
    }

    pub(crate) fn restore(&mut self, checkpoint: &TransactionCheckpoint) {
        self.reads.clone_from(&checkpoint.reads);
        self.mutation_reads.clone_from(&checkpoint.mutation_reads);
        self.writes.clone_from(&checkpoint.writes);
        self.staged_bytes = checkpoint.staged_bytes;
        self.observed_bytes = checkpoint.observed_bytes;
        self.observed_records = checkpoint.observed_records;
        self.ranges.clone_from(&checkpoint.ranges);
    }

    pub(crate) fn require_write(&self) -> Result<()> {
        self.ensure_active()?;
        if self.read_only {
            return Err(Error::Invalid(
                "application transaction is read-only".into(),
            ));
        }
        Ok(())
    }

    pub(crate) async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.ensure_active()?;
        if let Some(value) = self.writes.get(key).or_else(|| self.reads.get(key)) {
            return Ok(value.clone());
        }
        let owned_key = key.to_vec();
        let snapshot = self.snapshot.clone();
        let read_table = self.read_table.clone();
        let value = execute_read(move || {
            with_snapshot_table(&snapshot, read_table.as_deref(), |table| {
                let value = table.get(owned_key.as_slice()).map_err(storage_error)?;
                value.map(|value| bounded_copy(value.value())).transpose()
            })
        })
        .await?;
        // A read-only transaction owns one immutable engine snapshot and cannot
        // commit mutations. Retaining every read as conflict evidence only
        // duplicates that snapshot and makes bounded paged queries exhaust a
        // write-transaction observation budget. Each returned value stays capped.
        if self.read_only {
            return Ok(value);
        }
        self.observe_size(
            key.len().saturating_add(value.as_ref().map_or(0, Vec::len)),
            1,
        )?;
        self.reads.insert(key.to_vec(), value.clone());
        Ok(value)
    }

    pub(crate) async fn put(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.require_write()?;
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
        self.require_write()?;
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
            return Err(Error::ResourceLimit(ResourceLimit::MutationBytes));
        }
        Ok(total)
    }

    fn observe_size(&mut self, bytes: usize, records: usize) -> Result<()> {
        let total_bytes = self.observed_bytes.saturating_add(bytes);
        let total_records = self.observed_records.saturating_add(records);
        if total_bytes > MAX_BATCH_BYTES || total_records > MAX_BATCH_RECORDS {
            return Err(Error::ResourceLimit(ResourceLimit::ObservationBudget));
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
        let observed = execute_read(move || {
            let table = snapshot.open_table(TABLE).map_err(storage_error)?;
            let value = table.get(owned_key.as_slice()).map_err(storage_error)?;
            Ok::<_, Error>(value.map(|value| fingerprint(value.value())))
        })
        .await?;
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
        self.ensure_active()?;
        if self.read_only {
            if keys.len() > MAX_BATCH_RECORDS {
                return Err(Error::ResourceLimit(ResourceLimit::ReadBytes));
            }
            let snapshot = self.snapshot.clone();
            let read_table = self.read_table.clone();
            let keys = keys.to_vec();
            return execute_read(move || {
                with_snapshot_table(&snapshot, read_table.as_deref(), |table| {
                    let mut total = 0usize;
                    let mut result = Vec::with_capacity(keys.len());
                    for key in keys {
                        let value = table.get(key.as_slice()).map_err(storage_error)?;
                        if let Some(value) = &value {
                            total = total.saturating_add(value.value().len());
                            if total > MAX_BATCH_BYTES {
                                return Err(Error::ResourceLimit(ResourceLimit::ReadBytes));
                            }
                        }
                        result.push(value.map(|value| value.value().to_vec()));
                    }
                    Ok(result)
                })
            })
            .await;
        }
        let missing: std::collections::BTreeSet<_> = keys
            .iter()
            .filter(|key| !self.writes.contains_key(*key) && !self.reads.contains_key(*key))
            .cloned()
            .collect();
        if !missing.is_empty() {
            let snapshot = self.snapshot.clone();
            let loaded = execute_read(move || {
                let table = snapshot.open_table(TABLE).map_err(storage_error)?;
                let mut total = 0usize;
                let mut loaded = Vec::with_capacity(missing.len());
                for key in missing {
                    let value = table.get(key.as_slice()).map_err(storage_error)?;
                    if let Some(value) = &value {
                        total = total.saturating_add(value.value().len());
                        if total > MAX_BATCH_BYTES {
                            return Err(Error::ResourceLimit(ResourceLimit::ReadBytes));
                        }
                    }
                    loaded.push((key, value.map(|value| value.value().to_vec())));
                }
                Ok(loaded)
            })
            .await?;
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
                return Err(Error::ResourceLimit(ResourceLimit::ReadBytes));
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
        self.ensure_active()?;
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
        let read_table = self.read_table.clone();
        let rows = execute_read(move || {
            with_snapshot_table(&snapshot, read_table.as_deref(), |table| {
                scan_table(table, &owned_start, &owned_end, fetch_limit)
            })
        })
        .await?;
        if self.read_only {
            // No overlay is possible: require_write rejects every mutation.
            // scan_table still bounds this page's bytes and requested row count.
            debug_assert!(self.writes.is_empty());
            return Ok(rows);
        }
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
        self.ensure_active()?;
        if self.writes.is_empty() {
            return self.rollback().await;
        }
        let Self {
            backend,
            snapshot,
            reads,
            mutation_reads,
            writes,
            ranges,
            reserved,
            ..
        } = self;
        let staged = StagedCommit {
            reads,
            mutation_reads,
            writes,
            ranges,
        };
        if let Some(reserved) = reserved {
            drop(snapshot);
            return reserved.commit(staged).await;
        }
        // Component transactions retain their optimistic commit behavior. Application
        // sessions use the writer acquired before their callback, on its owning worker.
        tokio::task::spawn_blocking(move || {
            drop(snapshot);
            let database = backend.handles.wait_for_writer()?;
            let mut transaction = database.begin_write().map_err(storage_error)?;
            backend.handles.verify_identity()?;
            transaction
                .set_durability(Durability::Immediate)
                .map_err(storage_error)?;
            commit_staged(transaction, staged)
        })
        .await
        // A worker panic can happen after durable commit but before its result is returned.
        .map_err(|_| Error::UnknownCommit)?
    }

    pub(crate) async fn rollback(mut self) -> Result<()> {
        let reserved = self.reserved.take();
        tokio::task::spawn_blocking(move || drop(self))
            .await
            .map_err(worker_error)?;
        if let Some(reserved) = reserved {
            reserved.abort().await?;
        }
        Ok(())
    }
}

struct StagedCommit {
    reads: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    mutation_reads: BTreeMap<Vec<u8>, Option<(usize, [u8; 32])>>,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    ranges: Vec<ObservedRange>,
}

fn commit_staged(transaction: WriteTransaction, staged: StagedCommit) -> Result<()> {
    {
        let mut table = transaction.open_table(TABLE).map_err(storage_error)?;
        for (key, observed) in staged.reads {
            let current = table.get(key.as_slice()).map_err(storage_error)?;
            if current.as_ref().map(|value| value.value()) != observed.as_deref() {
                return Err(Error::TransactionConflict);
            }
        }
        for (key, observed) in staged.mutation_reads {
            let current = table.get(key.as_slice()).map_err(storage_error)?;
            if current.as_ref().map(|value| fingerprint(value.value())) != observed {
                return Err(Error::TransactionConflict);
            }
        }
        for range in staged.ranges {
            if scan_table(&table, &range.start, &range.end, range.limit)? != range.rows {
                return Err(Error::TransactionConflict);
            }
        }
        for (key, value) in staged.writes {
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
}

struct ReservationState {
    touched: Instant,
    closed: bool,
    idle_timeout: Duration,
}

enum ReservationCommand {
    Commit(StagedCommit, oneshot::Sender<Result<()>>),
    Abort(oneshot::Sender<Result<()>>),
}

struct ReservedWriter {
    commands: Option<mpsc::SyncSender<ReservationCommand>>,
    worker: Option<tokio::task::JoinHandle<()>>,
    state: Arc<Mutex<ReservationState>>,
}

fn expired_writer() -> Error {
    Error::Invalid("application writer reservation is no longer active".into())
}

fn worker_error_from_channel(_: oneshot::error::RecvError) -> Error {
    Error::Database("database reservation worker failed".into())
}

impl ReservedWriter {
    fn touch(&self) -> Result<()> {
        let mut state = self.state.lock().map_err(storage_error)?;
        if state.closed || state.touched.elapsed() >= state.idle_timeout {
            state.closed = true;
            return Err(expired_writer());
        }
        if self
            .worker
            .as_ref()
            .is_some_and(|worker| worker.is_finished())
        {
            state.closed = true;
            return Err(Error::Database("database reservation worker failed".into()));
        }
        state.touched = Instant::now();
        Ok(())
    }

    async fn commit(mut self, staged: StagedCommit) -> Result<()> {
        self.touch()?;
        let (reply, receive) = oneshot::channel();
        self.commands
            .take()
            .ok_or_else(expired_writer)?
            .send(ReservationCommand::Commit(staged, reply))
            .map_err(|_| expired_writer())?;
        let result = receive.await.map_err(|_| Error::UnknownCommit)?;
        if let Some(worker) = self.worker.take() {
            worker.await.map_err(|_| Error::UnknownCommit)?;
        }
        result
    }

    async fn abort(mut self) -> Result<()> {
        let (reply, receive) = oneshot::channel();
        let sent = self
            .commands
            .take()
            .is_some_and(|commands| commands.send(ReservationCommand::Abort(reply)).is_ok());
        let result = if sent {
            receive.await.map_err(worker_error_from_channel)?
        } else {
            Ok(())
        };
        if let Some(worker) = self.worker.take() {
            worker.await.map_err(worker_error)?;
        }
        result
    }
}

impl Drop for ReservedWriter {
    fn drop(&mut self) {
        // The worker observes disconnection and aborts. It never needs an async
        // task to be scheduled to release a cancelled session's writer lock.
        self.commands.take();
    }
}

fn close_reservation(state: &Mutex<ReservationState>) {
    if let Ok(mut state) = state.lock() {
        state.closed = true;
    }
}

fn reserved_worker(
    handles: Arc<DatabaseHandles>,
    state: Arc<Mutex<ReservationState>>,
    commands: mpsc::Receiver<ReservationCommand>,
    ready: oneshot::Sender<Result<Arc<ReadTransaction>>>,
    lock_timeout: Duration,
) {
    let started = Instant::now();
    let acquired = loop {
        if ready.is_closed() {
            close_reservation(&state);
            return;
        }
        let acquired = handles.try_writer().and_then(|database| {
            if ready.is_closed() {
                return Err(expired_writer());
            }
            database
                .try_begin_write()
                .map(|transaction| (database, transaction))
                .map_err(|error| match error {
                    redb::TransactionError::WouldBlock => Error::WriterBusy,
                    error => storage_error(error),
                })
        });
        match acquired {
            Ok(acquired) => break Ok(acquired),
            Err(Error::WriterBusy) if started.elapsed() < lock_timeout => {
                std::thread::sleep(
                    Duration::from_millis(5)
                        .min(lock_timeout - started.elapsed().min(lock_timeout)),
                );
            }
            Err(error) => break Err(error),
        }
    };
    let prepared = acquired.and_then(|(database, mut transaction)| {
        transaction
            .set_durability(Durability::Immediate)
            .map_err(storage_error)?;
        // The snapshot is taken after acquiring the writer, never before a peer's commit.
        let snapshot = Arc::new(database.begin_read().map_err(storage_error)?);
        Ok((transaction, snapshot))
    });
    let (transaction, snapshot) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            close_reservation(&state);
            let _ = ready.send(Err(error));
            return;
        }
    };
    if let Ok(mut state) = state.lock() {
        state.touched = Instant::now();
    }
    if ready.send(Ok(snapshot)).is_err() {
        close_reservation(&state);
        let _ = transaction.abort();
        return;
    }
    loop {
        let remaining = match state.lock() {
            Ok(state) if !state.closed => {
                state.idle_timeout.saturating_sub(state.touched.elapsed())
            }
            _ => Duration::ZERO,
        };
        if remaining.is_zero() {
            close_reservation(&state);
            let _ = transaction.abort();
            return;
        }
        match commands.recv_timeout(remaining) {
            Ok(ReservationCommand::Commit(staged, reply)) => {
                let live = match state.lock() {
                    Ok(mut state) => {
                        let live = !state.closed && state.touched.elapsed() < state.idle_timeout;
                        state.closed = true;
                        live
                    }
                    Err(_) => false,
                };
                let result = if live {
                    handles
                        .verify_identity()
                        .and_then(|()| commit_staged(transaction, staged))
                } else {
                    transaction
                        .abort()
                        .map_err(storage_error)
                        .and(Err(expired_writer()))
                };
                let _ = reply.send(result);
                return;
            }
            Ok(ReservationCommand::Abort(reply)) => {
                close_reservation(&state);
                let _ = reply.send(transaction.abort().map_err(storage_error));
                return;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                close_reservation(&state);
                let _ = transaction.abort();
                return;
            }
        }
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
            return Err(Error::ResourceLimit(ResourceLimit::ScanBytes));
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
mod read_table_tests {
    use super::*;
    use crate::test_support::TestFixture;

    fn engine(backend: &Backend) -> Arc<Database> {
        backend
            .handles
            .writer
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .clone()
    }

    #[test]
    fn immutable_table_handle_has_the_required_worker_traits() {
        fn require_worker_traits<T: Send + Sync + 'static>() {}
        require_worker_traits::<SnapshotTable>();
        require_worker_traits::<SnapshotTableCache>();
    }

    #[tokio::test]
    async fn cached_table_stays_with_its_snapshot_and_failed_opens_are_not_retained() {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        let key = crate::store_key("read-table/value");
        let mut end = key.clone();
        end.push(0);
        let mut seed = store.db.begin().await.unwrap();
        seed.put(key.clone(), b"before".to_vec()).await.unwrap();
        seed.commit().await.unwrap();

        let mut old = store.db.begin_read_only().await.unwrap();
        assert!(old.read_table.as_ref().unwrap().get().is_none());
        assert_eq!(old.get(&key).await.unwrap(), Some(b"before".to_vec()));
        let mut writer = store.db.begin().await.unwrap();
        writer.put(key.clone(), b"after".to_vec()).await.unwrap();
        writer.commit().await.unwrap();
        assert_eq!(
            old.get_many(std::slice::from_ref(&key)).await.unwrap(),
            vec![Some(b"before".to_vec())]
        );
        assert_eq!(
            old.scan(&key, &end, 1).await.unwrap(),
            vec![(key.clone(), b"before".to_vec())]
        );
        let mut current = store.db.begin_read_only().await.unwrap();
        assert_eq!(current.get(&key).await.unwrap(), Some(b"after".to_vec()));
        current.rollback().await.unwrap();

        // Change the real engine table's types, which must invalidate a fresh
        // application snapshot without changing a previously captured root.
        let database = engine(&store.db);
        let incompatible: TableDefinition<u64, u64> = TableDefinition::new(TABLE.name());
        let writer = database.begin_write().unwrap();
        writer.delete_table(TABLE).unwrap();
        writer
            .open_table(incompatible)
            .unwrap()
            .insert(1, 2)
            .unwrap();
        writer.commit().unwrap();
        assert_eq!(old.get(&key).await.unwrap(), Some(b"before".to_vec()));

        let mut rejected = store.db.begin_read_only().await.unwrap();
        assert!(matches!(
            rejected.scan(&key, &key, 1).await,
            Err(Error::Invalid(_))
        ));
        assert!(rejected.read_table.as_ref().unwrap().get().is_none());
        assert!(matches!(rejected.get(&key).await, Err(Error::Database(_))));
        assert!(rejected.read_table.as_ref().unwrap().get().is_none());
        assert!(matches!(
            rejected.get_many(std::slice::from_ref(&key)).await,
            Err(Error::Database(_))
        ));
        assert!(rejected.read_table.as_ref().unwrap().get().is_none());

        let writer = database.begin_write().unwrap();
        writer.delete_table(incompatible).unwrap();
        writer
            .open_table(TABLE)
            .unwrap()
            .insert(key.as_slice(), b"repaired".as_slice())
            .unwrap();
        writer.commit().unwrap();
        // Retrying an open cannot silently move an existing transaction to the
        // repaired table. Only a new transaction sees the new catalog.
        assert!(matches!(rejected.get(&key).await, Err(Error::Database(_))));
        assert!(rejected.read_table.as_ref().unwrap().get().is_none());
        let mut repaired = store.db.begin_read_only().await.unwrap();
        assert_eq!(
            repaired.get(&key).await.unwrap(),
            Some(b"repaired".to_vec())
        );
        repaired.rollback().await.unwrap();
        rejected.rollback().await.unwrap();
        old.rollback().await.unwrap();
        drop(database);
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn cached_reads_preserve_partial_batch_limits_order_and_writer_savepoints() {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        let first = crate::store_key("read-table/budget/a");
        let second = crate::store_key("read-table/budget/b");
        let small = crate::store_key("read-table/budget/c");
        let missing = crate::store_key("read-table/budget/missing");
        let end = crate::store_key("read-table/budget0");
        let large = vec![7u8; MAX_BATCH_BYTES / 2 + 1];
        let database = engine(&store.db);
        let writer = database.begin_write().unwrap();
        {
            let mut table = writer.open_table(TABLE).unwrap();
            table.insert(first.as_slice(), large.as_slice()).unwrap();
            table.insert(second.as_slice(), large.as_slice()).unwrap();
            table.insert(small.as_slice(), b"small".as_slice()).unwrap();
        }
        writer.commit().unwrap();
        drop(database);

        let mut reader = store.db.begin_read_only().await.unwrap();
        let too_many = vec![Vec::new(); MAX_BATCH_RECORDS + 1];
        assert!(matches!(
            reader.get_many(&too_many).await,
            Err(Error::ResourceLimit(ResourceLimit::ReadBytes))
        ));
        assert!(reader.read_table.as_ref().unwrap().get().is_none());
        assert!(matches!(
            reader.get_many(&[first.clone(), second.clone()]).await,
            Err(Error::ResourceLimit(ResourceLimit::ReadBytes))
        ));
        assert!(reader.read_table.as_ref().unwrap().get().is_some());
        assert_eq!(reader.get(&first).await.unwrap(), Some(large.clone()));
        assert_eq!(
            reader
                .get_many(&[small.clone(), missing.clone(), first.clone()])
                .await
                .unwrap(),
            vec![Some(b"small".to_vec()), None, Some(large.clone())]
        );
        assert_eq!(
            reader.scan(&first, &end, 1).await.unwrap(),
            vec![(first.clone(), large)]
        );
        assert!(matches!(
            reader.scan(&first, &end, 2).await,
            Err(Error::ResourceLimit(ResourceLimit::ScanBytes))
        ));
        assert_eq!(
            reader.scan(&small, &end, 4).await.unwrap(),
            vec![(small.clone(), b"small".to_vec())]
        );

        let mut writer = store.db.begin().await.unwrap();
        assert!(writer.read_table.is_none());
        writer.put(small.clone(), b"staged".to_vec()).await.unwrap();
        let checkpoint = writer.checkpoint();
        writer.delete(small.clone()).await.unwrap();
        writer.restore(&checkpoint);
        assert_eq!(
            writer.get_many(&[missing, small.clone()]).await.unwrap(),
            vec![None, Some(b"staged".to_vec())]
        );
        assert_eq!(
            writer.scan(&small, &end, 1).await.unwrap(),
            vec![(small.clone(), b"staged".to_vec())]
        );
        assert!(writer.read_table.is_none());
        writer.commit().await.unwrap();
        assert_eq!(reader.get(&small).await.unwrap(), Some(b"small".to_vec()));
        reader.rollback().await.unwrap();
        assert_eq!(
            store.db.read(&small).await.unwrap(),
            Some(b"staged".to_vec())
        );
        store.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn read_table_guards_are_released_by_every_read_transaction_end() {
        for ending in ["rollback", "commit", "drop"] {
            let fixture = TestFixture::start().await;
            let store = fixture.initialize().await;
            let mut reader = store.db.begin_read_only().await.unwrap();
            reader
                .get(&crate::store_key("meta/state"))
                .await
                .unwrap()
                .unwrap();
            let retained = Arc::downgrade(reader.read_table.as_ref().unwrap());
            store.shutdown().await.unwrap();
            match ending {
                "rollback" => reader.rollback().await.unwrap(),
                "commit" => reader.commit().await.unwrap(),
                _ => drop(reader),
            }
            assert!(retained.upgrade().is_none());
            // An actual exclusive reopen verifies that no table-owned engine
            // transaction or file lock survived the application transaction.
            Backend::open_exclusive(fixture.path())
                .await
                .unwrap()
                .shutdown()
                .await
                .unwrap();
            let backend = Backend::open_read_only(fixture.path()).await.unwrap();
            let mut strict = backend.begin().await.unwrap();
            assert!(strict.read_table.as_ref().unwrap().get().is_none());
            strict
                .get(&crate::store_key("meta/state"))
                .await
                .unwrap()
                .unwrap();
            strict.rollback().await.unwrap();
            backend.shutdown().await.unwrap();
        }
    }

    #[tokio::test]
    async fn cancelled_offloaded_initialization_cannot_populate_another_snapshot() {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        let key = crate::store_key("read-table/cancelled");
        let mut writer = store.db.begin().await.unwrap();
        writer.put(key.clone(), b"before".to_vec()).await.unwrap();
        writer.commit().await.unwrap();
        let reader = store.db.begin_read_only().await.unwrap();
        let snapshot = reader.snapshot.clone();
        let cache = reader.read_table.as_ref().unwrap().clone();
        let retained = Arc::downgrade(&cache);
        let worker_key = key.clone();
        let (entered, entering) = oneshot::channel();
        let (release, released) = mpsc::sync_channel(1);
        let (complete, completed) = oneshot::channel();
        let task = tokio::spawn(async move {
            execute_read(move || {
                entered.send(()).unwrap();
                released.recv().unwrap();
                let result = with_snapshot_table(&snapshot, Some(&cache), |table| {
                    table
                        .get(worker_key.as_slice())
                        .map_err(storage_error)?
                        .map(|value| bounded_copy(value.value()))
                        .transpose()
                });
                drop(cache);
                drop(snapshot);
                complete.send(result).unwrap();
                Ok(())
            })
            .await
        });
        tokio::time::timeout(Duration::from_secs(2), entering)
            .await
            .unwrap()
            .unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        drop(reader);

        let mut writer = store.db.begin().await.unwrap();
        writer.put(key.clone(), b"after".to_vec()).await.unwrap();
        writer.commit().await.unwrap();
        let mut current = store.db.begin_read_only().await.unwrap();
        assert_eq!(current.get(&key).await.unwrap(), Some(b"after".to_vec()));
        release.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), completed)
                .await
                .unwrap()
                .unwrap()
                .unwrap(),
            Some(b"before".to_vec())
        );
        assert!(retained.upgrade().is_none());
        assert_eq!(current.get(&key).await.unwrap(), Some(b"after".to_vec()));
        current.rollback().await.unwrap();
        store.shutdown().await.unwrap();
    }
}

#[cfg(test)]
mod execution_scope_tests {
    use super::*;
    use crate::test_support::TestFixture;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn normal_library_calls_keep_reads_off_the_calling_thread() {
        let caller = std::thread::current().id();
        runtime().block_on(async {
            let reader = execute_read(|| Ok(std::thread::current().id()))
                .await
                .unwrap();
            assert_ne!(reader, caller);
        });
    }

    #[test]
    fn owned_dispatcher_scope_does_not_leak_to_spawned_or_local_tasks() {
        let caller = std::thread::current().id();
        run_on_database_thread(move || {
            let owner = std::thread::current().id();
            assert_ne!(owner, caller);
            runtime().block_on(async {
                assert_eq!(
                    execute_read(|| Ok(std::thread::current().id()))
                        .await
                        .unwrap(),
                    owner
                );
                tokio::spawn(async {
                    let task = std::thread::current().id();
                    assert!(tokio::task::try_id().is_some());
                    assert_ne!(
                        execute_read(|| Ok(std::thread::current().id()))
                            .await
                            .unwrap(),
                        task
                    );
                })
                .await
                .unwrap();
                tokio::task::LocalSet::new()
                    .run_until(async {
                        tokio::task::spawn_local(async move {
                            assert_eq!(std::thread::current().id(), owner);
                            assert!(tokio::task::try_id().is_some());
                            assert_ne!(
                                execute_read(|| Ok(std::thread::current().id()))
                                    .await
                                    .unwrap(),
                                owner
                            );
                        })
                        .await
                        .unwrap();
                    })
                    .await;
                assert_eq!(
                    execute_read(|| Ok(std::thread::current().id()))
                        .await
                        .unwrap(),
                    owner
                );
            });
        })
        .unwrap();
        runtime().block_on(async {
            assert_ne!(
                execute_read(|| Ok(std::thread::current().id()))
                    .await
                    .unwrap(),
                caller
            );
        });
    }

    fn check_read_failures() {
        runtime().block_on(async {
            assert!(matches!(
                execute_read(|| Err::<(), _>(Error::IncompatibleStore)).await,
                Err(Error::IncompatibleStore)
            ));
            assert!(matches!(
                execute_read(|| Err::<(), _>(Error::ResourceLimit(ResourceLimit::ReadBytes))).await,
                Err(Error::ResourceLimit(ResourceLimit::ReadBytes))
            ));
            let panic = execute_read::<_, ()>(|| panic!("controlled read failure")).await;
            assert!(matches!(
                panic,
                Err(Error::Database(message)) if message == "database worker failed"
            ));
            assert_eq!(execute_read(|| Ok(7)).await.unwrap(), 7);
        });
    }

    #[test]
    fn inline_and_offloaded_reads_preserve_errors_and_panic_classification() {
        check_read_failures();
        run_on_database_thread(check_read_failures).unwrap();
    }

    #[test]
    fn owned_dispatcher_preserves_snapshots_writer_rollback_and_reservation_release() {
        run_on_database_thread(|| {
            runtime().block_on(async {
                let fixture = TestFixture::start().await;
                let store = fixture.initialize().await;
                let key = crate::store_key("execution-scope/value");
                let mut end = key.clone();
                end.push(0);
                let mut seed = store.db.begin().await.unwrap();
                seed.put(key.clone(), b"before".to_vec()).await.unwrap();
                seed.commit().await.unwrap();
                let mut snapshot = store.db.begin_read_only().await.unwrap();
                assert_eq!(snapshot.get(&key).await.unwrap(), Some(b"before".to_vec()));

                let acquire = || {
                    store
                        .db
                        .begin_reserved(Duration::from_millis(100), Duration::from_secs(2))
                };
                let mut writer = acquire().await.unwrap();
                writer.put(key.clone(), b"staged".to_vec()).await.unwrap();
                let checkpoint = writer.checkpoint();
                writer
                    .put(key.clone(), b"rolled-back".to_vec())
                    .await
                    .unwrap();
                writer.restore(&checkpoint);
                assert_eq!(
                    writer.get_many(std::slice::from_ref(&key)).await.unwrap(),
                    vec![Some(b"staged".to_vec())]
                );
                assert_eq!(
                    writer.scan(&key, &end, 1).await.unwrap(),
                    vec![(key.clone(), b"staged".to_vec())]
                );
                assert!(matches!(
                    store
                        .db
                        .begin_reserved(Duration::from_millis(5), Duration::from_secs(2))
                        .await,
                    Err(Error::WriterBusy)
                ));
                // Dropping a cancelled owner must still disconnect the separate
                // reservation worker and abort every staged mutation.
                drop(writer);
                let mut replacement = acquire().await.unwrap();
                assert_eq!(
                    replacement.get(&key).await.unwrap(),
                    Some(b"before".to_vec())
                );
                replacement
                    .put(key.clone(), b"after".to_vec())
                    .await
                    .unwrap();
                replacement.commit().await.unwrap();
                assert_eq!(
                    snapshot.get_many(std::slice::from_ref(&key)).await.unwrap(),
                    vec![Some(b"before".to_vec())]
                );
                assert_eq!(
                    snapshot.scan(&key, &end, 1).await.unwrap(),
                    vec![(key.clone(), b"before".to_vec())]
                );
                snapshot.rollback().await.unwrap();
                let mut current = store.db.begin_read_only().await.unwrap();
                assert_eq!(current.get(&key).await.unwrap(), Some(b"after".to_vec()));
                current.rollback().await.unwrap();
            });
        })
        .unwrap();
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

    #[test]
    fn checked_open_preserves_internal_lock_waits_after_writer_acquisition() {
        let temporary = tempfile::NamedTempFile::new().unwrap();
        let open_file = || {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(temporary.path())
                .unwrap()
        };
        let backend = NonblockingOpen {
            file: redb::backends::FileBackend::new(open_file()).unwrap(),
            locks: Arc::new(OpenLocks {
                opening: AtomicBool::new(true),
                writer_acquired: AtomicBool::new(false),
                busy: AtomicBool::new(false),
                mutated: AtomicBool::new(false),
            }),
        };
        let peer = redb::backends::FileBackend::new(open_file()).unwrap();
        let writer = (Bound::Included(2048), Bound::Included(2048));
        let header = (Bound::Included(4096), Bound::Included(4096));
        backend.lock_range(writer.0, writer.1).unwrap();
        peer.lock_shared_range(header.0, header.1).unwrap();
        let (send, receive) = mpsc::channel();
        let worker = std::thread::spawn(move || {
            let result = backend.lock_range(header.0, header.1);
            let acquired = result.is_ok();
            send.send(result).unwrap();
            if acquired {
                backend.unlock_range(header.0, header.1).unwrap();
            }
            backend.unlock_range(writer.0, writer.1).unwrap();
        });
        let early = receive.recv_timeout(Duration::from_millis(20));
        let waited = matches!(early, Err(mpsc::RecvTimeoutError::Timeout));
        peer.unlock_range(header.0, header.1).unwrap();
        let outcome = match early {
            Ok(result) => result,
            Err(mpsc::RecvTimeoutError::Timeout) => {
                receive.recv_timeout(Duration::from_secs(1)).unwrap()
            }
            Err(error) => panic!("internal lock worker disconnected: {error}"),
        };
        worker.join().unwrap();
        assert!(
            waited,
            "an internal header wait was rejected after writer acquisition: {outcome:?}"
        );
        outcome.unwrap();
    }

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
    async fn immutable_read_snapshots_page_without_retaining_write_observations() {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        let peer = fixture.connect().await;
        let start = store_key("test/readonly/");
        let end = store_key("test/readonly0");
        let keys = (0..1024)
            .map(|index| store_key(&format!("test/readonly/{index:04}")))
            .collect::<Vec<_>>();
        let mut seed = store.db.begin().await.unwrap();
        for key in &keys {
            seed.put(key.clone(), b"original".to_vec()).await.unwrap();
        }
        seed.commit().await.unwrap();

        let mut snapshot = store.db.begin_read_only().await.unwrap();
        assert_eq!(
            snapshot.get(&keys[0]).await.unwrap(),
            Some(b"original".to_vec())
        );
        let mut fresh = peer.db.begin().await.unwrap();
        fresh
            .put(keys[0].clone(), b"updated".to_vec())
            .await
            .unwrap();
        fresh.commit().await.unwrap();
        // Repeated pages cross the writable observation limit without retaining
        // their keys or bodies, while the actual engine snapshot stays stable.
        for _ in 0..70 {
            let page = snapshot.scan(&start, &end, 1024).await.unwrap();
            assert_eq!(page.len(), 1024);
            assert_eq!(page[0].1, b"original");
        }
        assert_eq!(
            snapshot.get_many(&keys).await.unwrap(),
            vec![Some(b"original".to_vec()); 1024]
        );
        assert!(snapshot.reads.is_empty());
        assert!(snapshot.mutation_reads.is_empty());
        assert!(snapshot.ranges.is_empty());
        assert_eq!(snapshot.observed_bytes, 0);
        assert_eq!(snapshot.observed_records, 0);
        assert!(matches!(
            snapshot.put(keys[0].clone(), b"forbidden".to_vec()).await,
            Err(Error::Invalid(_))
        ));
        snapshot.rollback().await.unwrap();

        // Writable snapshots retain the original range/conflict resource caps.
        let mut writable = store.db.begin().await.unwrap();
        let mut bounded = false;
        for _ in 0..70 {
            match writable.scan(&start, &end, 1024).await {
                Ok(_) => {}
                Err(Error::ResourceLimit(ResourceLimit::ObservationBudget)) => {
                    bounded = true;
                    break;
                }
                Err(error) => panic!("unexpected writable scan error: {error:?}"),
            }
        }
        assert!(bounded);
        writable.rollback().await.unwrap();
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
            handles: Arc::new(DatabaseHandles {
                path: PathBuf::new(),
                exclusive: false,
                read_only: false,
                identity: None,
                reader: None,
                writer: Mutex::new(Some(Arc::new(database))),
                opening: Mutex::new(()),
            }),
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
