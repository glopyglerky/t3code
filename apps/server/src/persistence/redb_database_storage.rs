//! Native file accounting, snapshot backups and offline compaction.

use crate::{Result, Store};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize, Serialize)]
pub struct StorageStatistics {
    pub database_bytes: u64,
    /// Guaranteed removable file bytes. Zero until the engine exposes a shrink prediction.
    pub reclaimable_bytes: u64,
    /// Conservative reusable free-page capacity, not a promise of filesystem shrink.
    pub reusable_bytes: u64,
    pub fragmented_bytes: u64,
    pub allocated_bytes: u64,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StorageBackup {
    pub rows: u64,
    pub bytes: u64,
    pub checksum: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct StorageCompaction {
    pub before_bytes: u64,
    pub after_bytes: u64,
}

impl Store {
    pub async fn open_exclusive(path: impl AsRef<Path>) -> Result<Self> {
        let store = Self {
            db: crate::backend::Backend::open_exclusive(path).await?,
        };
        store.metadata().await?;
        Ok(store)
    }
    pub async fn storage_statistics(&self) -> Result<StorageStatistics> {
        self.db.storage_statistics().await
    }
    pub async fn storage_backup(&self, destination: PathBuf) -> Result<StorageBackup> {
        self.db.storage_backup(destination).await
    }
    pub async fn storage_compact(&self) -> Result<StorageCompaction> {
        self.db.storage_compact().await
    }
}
