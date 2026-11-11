//! Compile-time current schema: fresh application startup has no SQL dependency.
use crate::application::{
    ApplicationCatalog, ApplicationSchema, decode_catalog, decode_row, encode_catalog, row_prefix,
    schema_key, table, validate_schema,
};
use crate::application_rows::{int_cell, integer, text, text_cell, write_row};
use crate::{Error, Result, Store};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;

#[derive(Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Migration {
    pub id: String,
    pub name: String,
}

#[derive(Deserialize)]
struct Definition {
    format: u32,
    migrations: Vec<Migration>,
    schema: ApplicationSchema,
}

#[derive(Serialize)]
pub struct BootstrapResult {
    pub initialized: bool,
    pub migrations: Vec<Migration>,
    pub schema_version: u32,
}

fn definition() -> Result<Definition> {
    let definition: Definition = serde_json::from_str(include_str!("omdb-schema.json"))
        .map_err(|_| Error::IncompatibleStore)?;
    if definition.format != 1 {
        return Err(Error::IncompatibleStore);
    }
    validate_schema(&definition.schema)?;
    Ok(definition)
}

impl Store {
    pub async fn application_migrations(&self) -> Result<Vec<Migration>> {
        let mut tx = self.db.begin_read_only().await?;
        let Some(bytes) = tx.get(&schema_key()).await? else {
            tx.rollback().await?;
            return Ok(Vec::new());
        };
        let catalog = decode_catalog(&bytes)?;
        let prefix = row_prefix(&catalog.generation, "effect_sql_migrations")?;
        let mut end = prefix.clone();
        *end.last_mut().ok_or(Error::IncompatibleStore)? += 1;
        let mut cursor = prefix.clone();
        let mut result = Vec::new();
        loop {
            let page = tx.scan(&cursor, &end, 128).await?;
            let count = page.len();
            for (key, bytes) in page {
                let row = decode_row(
                    &catalog,
                    table(&catalog, "effect_sql_migrations")?,
                    &key,
                    &bytes,
                )?;
                result.push(Migration {
                    id: integer(&row.row, "migration_id")?.to_string(),
                    name: text(&row.row, "name")?,
                });
                cursor = key;
                cursor.push(0);
            }
            if count < 128 {
                break;
            }
            if result.len() > 4096 {
                return Err(Error::IncompatibleStore);
            }
        }
        result.sort_by_key(|entry| entry.id.parse::<i64>().unwrap_or(i64::MAX));
        tx.rollback().await?;
        Ok(result)
    }

    /// New files are initialized atomically; an imported or existing file is
    /// verified, never overwritten or silently replaced by an empty database.
    pub async fn bootstrap_application(&self) -> Result<BootstrapResult> {
        let definition = definition()?;
        let mut tx = self
            .db
            .begin_reserved(Duration::from_secs(5), Duration::from_secs(300))
            .await?;
        if let Some(bytes) = tx.get(&schema_key()).await? {
            let catalog = decode_catalog(&bytes)?;
            if catalog.schema != definition.schema {
                return Err(Error::Invalid("Native application schema does not match this build; migrate the source with its matching build before importing".into()));
            }
            tx.rollback().await?;
            if self.application_migrations().await? != definition.migrations {
                return Err(Error::Invalid(
                    "Native migration history does not match this build".into(),
                ));
            }
            return Ok(BootstrapResult {
                initialized: false,
                migrations: definition.migrations,
                schema_version: 1,
            });
        }
        let catalog = ApplicationCatalog {
            schema: definition.schema,
            generation: "native".into(),
        };
        tx.put(schema_key(), encode_catalog(&catalog)?).await?;
        for migration in &definition.migrations {
            let id = migration
                .id
                .parse::<i64>()
                .map_err(|_| Error::IncompatibleStore)?;
            write_row(
                &mut tx,
                &catalog,
                "effect_sql_migrations",
                serde_json::Map::from_iter([
                    ("migration_id".into(), int_cell(id)),
                    ("name".into(), text_cell(&migration.name)),
                    ("created_at".into(), text_cell("1970-01-01 00:00:00")),
                ]),
            )
            .await?;
        }
        // Fresh stores have a complete empty source vocabulary and deletion
        // witness, so search can qualify before their first message.
        tx.put(crate::store_key("application/generation/native/metadata/header"),serde_json::to_vec(&json!({
            "version":2,"searchIndex":true,"searchDeleteWitness":true,"excludedDerivedTables":["orchestration_v2_message_search"],"sequences":[],"migrations":definition.migrations
        })).map_err(|_|Error::IncompatibleStore)?).await?;
        tx.commit().await?;
        Ok(BootstrapResult {
            initialized: true,
            migrations: definition.migrations,
            schema_version: 1,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestFixture;
    #[tokio::test]
    async fn migration_inspection_is_read_only_and_cannot_initialize_or_promote() {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        store.bootstrap_application().await.unwrap();
        store.shutdown().await.unwrap();
        let before = std::fs::read(fixture.path()).unwrap();
        let read_only = Store::open_read_only(fixture.path()).await.unwrap();
        assert_eq!(read_only.application_migrations().await.unwrap().len(), 80);
        assert!(read_only.bootstrap_application().await.is_err());
        read_only.shutdown().await.unwrap();
        assert_eq!(std::fs::read(fixture.path()).unwrap(), before);
        let missing = fixture
            .path()
            .with_file_name("absent-migration-preflight.omdb");
        assert!(Store::open_read_only(&missing).await.is_err());
        assert!(!missing.exists());
    }
    #[tokio::test]
    async fn fresh_schema_boots_once_without_sqlite_and_retains_migrations() {
        let fixture = TestFixture::start().await;
        let store = fixture.initialize().await;
        let first = store.bootstrap_application().await.unwrap();
        assert!(first.initialized);
        assert_eq!(first.migrations.len(), 80);
        assert!(store.prepare_application_search().await.unwrap().prepared);
        assert!(!store.bootstrap_application().await.unwrap().initialized);
        assert_eq!(store.application_migrations().await.unwrap().len(), 80);
        while !store
            .prepare_application_message_rowids()
            .await
            .unwrap()
            .prepared
        {}
        store.shutdown().await.unwrap();
        let reopened = fixture.connect().await;
        assert!(!reopened.bootstrap_application().await.unwrap().initialized);
        assert!(
            reopened
                .prepare_application_search()
                .await
                .unwrap()
                .prepared
        );
        reopened.shutdown().await.unwrap();
    }
}
