use std::path::{Path, PathBuf};

use tempfile::TempDir;

use crate::Store;

/// Owns a private directory and one real redb file for a test.
pub struct TestFixture {
    directory: TempDir,
    database_path: PathBuf,
}

impl TestFixture {
    pub async fn start() -> Self {
        let temp_root = std::env::var_os("T3_CODE_LS_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let directory = tempfile::Builder::new()
            .prefix("t3-redb-test-")
            .tempdir_in(temp_root)
            .expect("create private database test directory");
        let database_path = directory.path().join("state.redb");
        Self {
            directory,
            database_path,
        }
    }

    pub fn path(&self) -> &Path {
        &self.database_path
    }

    pub async fn initialize(&self) -> Store {
        Store::initialize_local(self.path())
            .await
            .expect("initialize redb test store")
    }

    pub async fn connect(&self) -> Store {
        Store::open_local(self.path())
            .await
            .expect("open independent redb test handle")
    }

    /// Verify a fresh open after the test has dropped its earlier handles.
    /// Process-crash recovery is exercised by the separate CLI SIGKILL tests.
    pub async fn restart(&mut self) {
        assert!(self.directory.path().is_dir());
        self.connect()
            .await
            .shutdown()
            .await
            .expect("close reopened redb test handle");
    }
}
