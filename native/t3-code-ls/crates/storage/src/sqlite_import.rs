//! Validated SQLite exports are archived separately from the native command store.
use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::{self, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};

use crate::{Error, Result, Store, store_key};

const FORMAT: &str = "t3-code-ls/sqlite-export";
const CHUNK_BYTES: usize = 1024 * 1024;
const MAX_TRANSACTION_BYTES: usize = 4 * 1024 * 1024;
static RESTORE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug)]
pub struct ImportLimits {
    pub max_bytes: u64,
    pub max_line_bytes: usize,
    pub max_rows: u64,
    pub max_tables: usize,
    pub max_columns: usize,
}

impl Default for ImportLimits {
    fn default() -> Self {
        Self {
            max_bytes: 512 * 1024 * 1024,
            max_line_bytes: 16 * 1024 * 1024,
            max_rows: 10_000_000,
            max_tables: 4096,
            max_columns: 2048,
        }
    }
}

impl ImportLimits {
    fn validate(self) -> Result<Self> {
        if self.max_bytes == 0
            || self.max_bytes > 4 * 1024 * 1024 * 1024
            || self.max_line_bytes == 0
            || self.max_line_bytes > 16 * 1024 * 1024
            || self.max_rows == 0
            || self.max_rows > 10_000_000
            || self.max_tables == 0
            || self.max_tables > 4096
            || self.max_columns == 0
            || self.max_columns > 2048
        {
            return invalid("archive limits exceed supported bounds");
        }
        Ok(self)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExportSummary {
    pub export_sha256: String,
    pub file_sha256: String,
    pub byte_count: u64,
    pub table_count: u64,
    pub row_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveReceipt {
    pub archive_id: String,
    pub summary: ExportSummary,
    pub chunk_count: u64,
    pub replayed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChunkRef {
    sha256: String,
    byte_count: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ArchiveRecord {
    format_version: u64,
    ready: bool,
    summary: ExportSummary,
    chunks: Vec<ChunkRef>,
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(Error::Invalid(format!("SQLite export: {message}")))
}

fn io_error(_: std::io::Error) -> Error {
    Error::Invalid("SQLite export file could not be read".into())
}

fn hash(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[(byte >> 4) as usize] as char);
        output.push(DIGITS[(byte & 15) as usize] as char);
    }
    output
}

fn is_hash(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

struct UniqueValue(Value);

impl<'de> Deserialize<'de> for UniqueValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct UniqueVisitor;
        impl<'de> Visitor<'de> for UniqueVisitor {
            type Value = UniqueValue;
            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("JSON without duplicate object keys")
            }
            fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(Value::Null))
            }
            fn visit_bool<E: de::Error>(self, value: bool) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(Value::Bool(value)))
            }
            fn visit_i64<E: de::Error>(self, value: i64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_f64<E: de::Error>(self, value: f64) -> std::result::Result<Self::Value, E> {
                serde_json::Number::from_f64(value)
                    .map(|number| UniqueValue(Value::Number(number)))
                    .ok_or_else(|| E::custom("non-finite JSON number"))
            }
            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_string<E: de::Error>(
                self,
                value: String,
            ) -> std::result::Result<Self::Value, E> {
                Ok(UniqueValue(value.into()))
            }
            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Vec::new();
                while let Some(value) = sequence.next_element::<UniqueValue>()? {
                    values.push(value.0);
                }
                Ok(UniqueValue(Value::Array(values)))
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut values = Map::new();
                while let Some(key) = map.next_key::<String>()? {
                    if values.contains_key(&key) {
                        return Err(de::Error::custom("duplicate JSON key"));
                    }
                    values.insert(key, map.next_value::<UniqueValue>()?.0);
                }
                Ok(UniqueValue(Value::Object(values)))
            }
        }
        deserializer.deserialize_any(UniqueVisitor)
    }
}

fn canonical(value: &Value, output: &mut Vec<u8>) {
    match value {
        Value::Object(object) => {
            output.push(b'{');
            let mut keys: Vec<_> = object.keys().collect();
            keys.sort();
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                output.extend(serde_json::to_vec(key).expect("JSON key serializes"));
                output.push(b':');
                canonical(&object[*key], output);
            }
            output.push(b'}');
        }
        Value::Array(values) => {
            output.push(b'[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(b',');
                }
                canonical(value, output);
            }
            output.push(b']');
        }
        _ => output.extend(serde_json::to_vec(value).expect("JSON value serializes")),
    }
}

fn fields(value: &Value, required: &[&str], optional: &[&str]) -> Result<()> {
    let Some(object) = value.as_object() else {
        return invalid("expected an object");
    };
    if required.iter().any(|key| !object.contains_key(*key))
        || object
            .keys()
            .any(|key| !required.contains(&key.as_str()) && !optional.contains(&key.as_str()))
    {
        return invalid("missing or unknown record field");
    }
    Ok(())
}

fn string<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Invalid("SQLite export: expected a string".into()))
}

fn number(value: &Value, key: &str) -> Result<u64> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| Error::Invalid("SQLite export: expected an unsigned integer".into()))
}

fn array<'a>(value: &'a Value, key: &str) -> Result<&'a Vec<Value>> {
    value
        .get(key)
        .and_then(Value::as_array)
        .ok_or_else(|| Error::Invalid("SQLite export: expected an array".into()))
}

fn decimal(value: &str) -> Result<i64> {
    match value.parse::<i64>() {
        Ok(number) if number.to_string() == value => Ok(number),
        _ => invalid("invalid canonical signed 64-bit integer"),
    }
}

fn valid_base64(value: &str) -> bool {
    let bytes = value.as_bytes();
    if !bytes.len().is_multiple_of(4) {
        return false;
    }
    if bytes.is_empty() {
        return true;
    }
    let padding = if bytes.ends_with(b"==") {
        2
    } else if bytes.ends_with(b"=") {
        1
    } else {
        0
    };
    let digit = |byte| match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    };
    if bytes[..bytes.len() - padding]
        .iter()
        .any(|byte| digit(*byte).is_none())
    {
        return false;
    }
    match padding {
        2 => digit(bytes[bytes.len() - 3]).is_some_and(|last| last & 15 == 0),
        1 => digit(bytes[bytes.len() - 2]).is_some_and(|last| last & 3 == 0),
        _ => true,
    }
}

fn validate_cell(value: &Value) -> Result<()> {
    match string(value, "type")? {
        "null" => fields(value, &["type"], &[]),
        "integer" => {
            fields(value, &["type", "value"], &[])?;
            decimal(string(value, "value")?)?;
            Ok(())
        }
        "real" => {
            fields(value, &["type", "value"], &[])?;
            let bits = string(value, "value")?;
            if bits.len() != 16
                || !bits
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
                || u64::from_str_radix(bits, 16)
                    .ok()
                    .is_none_or(|bits| f64::from_bits(bits).is_nan())
            {
                return invalid("invalid SQLite real bits");
            }
            Ok(())
        }
        "text" => {
            fields(value, &["type", "value"], &["encoding"])?;
            let text = string(value, "value")?;
            if value.get("encoding").is_some()
                && (string(value, "encoding")? != "base64" || !valid_base64(text))
            {
                return invalid("invalid text byte encoding");
            }
            Ok(())
        }
        "blob" => {
            fields(value, &["type", "value"], &[])?;
            if !valid_base64(string(value, "value")?) {
                return invalid("invalid canonical base64 blob");
            }
            Ok(())
        }
        _ => invalid("unknown SQL storage class"),
    }
}

struct Header {
    table_schema: BTreeMap<String, String>,
    migrations: Vec<(String, String)>,
}

fn validate_header(value: &Value, limits: ImportLimits) -> Result<Header> {
    fields(
        value,
        &[
            "type",
            "format",
            "version",
            "sqliteVersion",
            "userVersion",
            "schemaVersion",
            "applicationId",
            "tableCount",
            "excludedDerivedTables",
            "sequences",
            "migrations",
            "schema",
        ],
        &[],
    )?;
    if string(value, "format")? != FORMAT
        || number(value, "version")? != 1
        || string(value, "sqliteVersion")?.is_empty()
    {
        return invalid("unsupported export format");
    }
    for key in ["userVersion", "schemaVersion", "applicationId"] {
        if value
            .get(key)
            .and_then(Value::as_i64)
            .and_then(|number| i32::try_from(number).ok())
            .is_none()
        {
            return invalid("invalid SQLite schema version metadata");
        }
    }
    let mut excluded = HashSet::new();
    let mut previous = None;
    for entry in array(value, "excludedDerivedTables")? {
        let Some(name) = entry.as_str() else {
            return invalid("invalid excluded table name");
        };
        if previous
            .is_some_and(|previous: &str| previous.encode_utf16().cmp(name.encode_utf16()).is_ge())
            || !excluded.insert(name.to_owned())
        {
            return invalid("excluded table names are not unique and ordered");
        }
        previous = Some(name);
    }
    let mut schema_names = HashSet::new();
    let mut table_schema = BTreeMap::new();
    for entry in array(value, "schema")? {
        fields(entry, &["type", "name", "tableName", "sql"], &[])?;
        let kind = string(entry, "type")?;
        let name = string(entry, "name")?;
        let table = string(entry, "tableName")?;
        if !["table", "index", "view", "trigger"].contains(&kind)
            || name.starts_with("sqlite_")
            || excluded.contains(name)
            || excluded.contains(table)
            || !schema_names.insert((kind, name))
            || !(entry["sql"].is_null() || entry["sql"].is_string())
        {
            return invalid("invalid or duplicate schema object");
        }
        if kind == "table" {
            if name != table {
                return invalid("table schema owner mismatch");
            }
            table_schema.insert(name.to_owned(), string(entry, "sql")?.to_owned());
        }
    }
    if table_schema.len() > limits.max_tables
        || number(value, "tableCount")? != table_schema.len() as u64
    {
        return invalid("header table count mismatch or limit exceeded");
    }
    let mut sequence_names = HashSet::new();
    for entry in array(value, "sequences")? {
        fields(entry, &["name", "sequence"], &[])?;
        let name = string(entry, "name")?;
        if !table_schema.contains_key(name) || !sequence_names.insert(name) {
            return invalid("invalid sequence owner");
        }
        decimal(string(entry, "sequence")?)?;
    }
    let mut migrations = Vec::new();
    let mut previous = None;
    for entry in array(value, "migrations")? {
        fields(entry, &["id", "name"], &[])?;
        let id = decimal(string(entry, "id")?)?;
        if previous.is_some_and(|previous| previous >= id) {
            return invalid("migration IDs are not unique and ordered");
        }
        previous = Some(id);
        migrations.push((id.to_string(), string(entry, "name")?.to_owned()));
    }
    if !migrations.is_empty() && !table_schema.contains_key("effect_sql_migrations") {
        return invalid("migration metadata has no source table");
    }
    Ok(Header {
        table_schema,
        migrations,
    })
}

struct TableState {
    name: String,
    columns: Vec<String>,
    expected_rows: u64,
    observed_rows: u64,
    hash: Sha256,
}

fn start_table(value: &Value, header: &Header, limits: ImportLimits) -> Result<TableState> {
    fields(value, &["type", "name", "sql", "columns", "rowCount"], &[])?;
    let name = string(value, "name")?;
    if header.table_schema.get(name).map(String::as_str) != Some(string(value, "sql")?) {
        return invalid("table does not match header schema");
    }
    let columns = array(value, "columns")?;
    if columns.is_empty() || columns.len() > limits.max_columns {
        return invalid("column limit exceeded or table has no columns");
    }
    let mut names = Vec::new();
    let mut unique_names = HashSet::new();
    let mut primary_keys = Vec::new();
    for column in columns {
        fields(
            column,
            &[
                "name",
                "declaredType",
                "notNull",
                "defaultSql",
                "primaryKeyOrder",
                "hidden",
            ],
            &[],
        )?;
        let name = string(column, "name")?;
        string(column, "declaredType")?;
        if !unique_names.insert(name.to_ascii_lowercase())
            || !column["notNull"].is_boolean()
            || !(column["defaultSql"].is_null() || column["defaultSql"].is_string())
            || number(column, "hidden")? > 3
        {
            return invalid("invalid column metadata");
        }
        let primary = number(column, "primaryKeyOrder")?;
        if primary > 0 {
            primary_keys.push(primary);
        }
        names.push(name.to_owned());
    }
    primary_keys.sort_unstable();
    if primary_keys
        .iter()
        .enumerate()
        .any(|(index, order)| *order != index as u64 + 1)
    {
        return invalid("invalid primary key ordering");
    }
    let expected_rows = number(value, "rowCount")?;
    if expected_rows > limits.max_rows {
        return invalid("row limit exceeded");
    }
    if name == "effect_sql_migrations" && expected_rows != header.migrations.len() as u64 {
        return invalid("migration count does not match table");
    }
    if name == "effect_sql_migrations"
        && (!names.iter().any(|name| name == "migration_id")
            || !names.iter().any(|name| name == "name"))
    {
        return invalid("migration table columns are missing");
    }
    Ok(TableState {
        name: name.to_owned(),
        columns: names,
        expected_rows,
        observed_rows: 0,
        hash: Sha256::new(),
    })
}

fn finish_table(current: &mut Option<TableState>, tables: &mut Vec<Value>) -> Result<()> {
    if let Some(table) = current.take() {
        if table.observed_rows != table.expected_rows {
            return invalid("table row count mismatch");
        }
        tables.push(json!({"name": table.name, "rowCount": table.observed_rows, "sha256": hex(&table.hash.finalize())}));
    }
    Ok(())
}

fn validate_reader<R: BufRead>(
    mut reader: R,
    limits: ImportLimits,
    mut on_line: impl FnMut(&[u8]) -> Result<()>,
) -> Result<ExportSummary> {
    let limits = limits.validate()?;
    let mut export_hash = Sha256::new();
    let mut file_hash = Sha256::new();
    let mut header = None;
    let mut current: Option<TableState> = None;
    let mut tables = Vec::new();
    let mut bytes = 0u64;
    let mut rows = 0u64;
    let mut manifest = None;
    loop {
        let mut line = Vec::new();
        let count = reader
            .by_ref()
            .take((limits.max_line_bytes + 1) as u64)
            .read_until(b'\n', &mut line)
            .map_err(io_error)?;
        if count == 0 {
            break;
        }
        bytes = bytes
            .checked_add(count as u64)
            .ok_or_else(|| Error::Invalid("SQLite export byte count overflow".into()))?;
        if count > limits.max_line_bytes || bytes > limits.max_bytes || line.last() != Some(&b'\n')
        {
            return invalid("byte limit exceeded or truncated line");
        }
        if manifest.is_some() {
            return invalid("data follows the final manifest");
        }
        let unique: UniqueValue = serde_json::from_slice(&line).map_err(|_| {
            Error::Invalid("SQLite export: malformed JSON or duplicate object key".into())
        })?;
        let value = unique.0;
        let mut encoded = Vec::with_capacity(line.len());
        canonical(&value, &mut encoded);
        encoded.push(b'\n');
        if encoded != line {
            return invalid("noncanonical JSON encoding");
        }
        file_hash.update(&line);
        let kind = string(&value, "type")?;
        if let Some(header) = &header {
            match kind {
                "table" => {
                    finish_table(&mut current, &mut tables)?;
                    let table = start_table(&value, header, limits)?;
                    if tables.last().is_some_and(|previous| {
                        previous["name"]
                            .as_str()
                            .is_some_and(|name| name >= table.name.as_str())
                    }) {
                        return invalid("table records are not unique and ordered");
                    }
                    if tables.len() >= limits.max_tables {
                        return invalid("table limit exceeded");
                    }
                    current = Some(table);
                }
                "row" => {
                    fields(&value, &["type", "table", "ordinal", "values"], &[])?;
                    let Some(table) = current.as_mut() else {
                        return invalid("row has no table");
                    };
                    if string(&value, "table")? != table.name
                        || number(&value, "ordinal")? != table.observed_rows
                        || table.observed_rows >= table.expected_rows
                    {
                        return invalid("row table or ordinal mismatch");
                    }
                    let cells = array(&value, "values")?;
                    if cells.len() != table.columns.len() {
                        return invalid("row column count mismatch");
                    }
                    for cell in cells {
                        validate_cell(cell)?;
                    }
                    if table.name == "effect_sql_migrations" {
                        let id = table
                            .columns
                            .iter()
                            .position(|name| name == "migration_id")
                            .ok_or_else(|| {
                                Error::Invalid("SQLite migration ID column missing".into())
                            })?;
                        let name = table
                            .columns
                            .iter()
                            .position(|name| name == "name")
                            .ok_or_else(|| {
                                Error::Invalid("SQLite migration name column missing".into())
                            })?;
                        let expected = &header.migrations[table.observed_rows as usize];
                        if cells[id] != json!({"type":"integer", "value":expected.0})
                            || cells[name] != json!({"type":"text", "value":expected.1})
                        {
                            return invalid("migration row disagrees with metadata");
                        }
                    }
                    table.observed_rows += 1;
                    rows += 1;
                    if rows > limits.max_rows {
                        return invalid("row limit exceeded");
                    }
                    table.hash.update(&line);
                }
                "manifest" => {
                    finish_table(&mut current, &mut tables)?;
                    fields(
                        &value,
                        &["type", "tableCount", "rowCount", "tables", "sha256"],
                        &[],
                    )?;
                    let digest = hex(&export_hash.clone().finalize());
                    let expected = json!({"type":"manifest", "tableCount":tables.len(), "rowCount":rows, "tables":tables, "sha256":digest});
                    if value != expected || tables.len() != header.table_schema.len() {
                        return invalid("manifest counts or SHA-256 checksums do not match");
                    }
                    manifest = Some(digest);
                }
                _ => return invalid("unexpected export record"),
            }
        } else {
            if kind != "header" {
                return invalid("first record must be the header");
            }
            header = Some(validate_header(&value, limits)?);
        }
        if kind != "manifest" {
            export_hash.update(&line);
        }
        on_line(&line)?;
    }
    let Some(export_sha256) = manifest else {
        return invalid("final manifest is missing");
    };
    Ok(ExportSummary {
        export_sha256,
        file_sha256: hex(&file_hash.finalize()),
        byte_count: bytes,
        table_count: tables.len() as u64,
        row_count: rows,
    })
}

fn open_export(path: &Path, limits: ImportLimits) -> Result<File> {
    let file = File::open(path).map_err(io_error)?;
    let metadata = file.metadata().map_err(io_error)?;
    if !metadata.is_file() || metadata.len() > limits.max_bytes {
        return invalid("source is not a bounded regular file");
    }
    Ok(file)
}

pub fn validate_sqlite_export(
    path: impl AsRef<Path>,
    limits: ImportLimits,
) -> Result<ExportSummary> {
    let limits = limits.validate()?;
    validate_reader(
        BufReader::new(open_export(path.as_ref(), limits)?),
        limits,
        |_| Ok(()),
    )
}

struct RestoredFile {
    file: File,
    temporary: PathBuf,
    destination: PathBuf,
}

impl RestoredFile {
    fn create(destination: &Path) -> Result<Self> {
        match std::fs::symlink_metadata(destination) {
            Ok(_) => return invalid("restore destination already exists"),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error(error)),
        }
        let directory = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Invalid("restore clock is invalid".into()))?
            .as_nanos();
        let temporary = directory.join(format!(
            ".t3-sqlite-restore-{}-{nonce}-{}.partial",
            std::process::id(),
            RESTORE_SEQUENCE.fetch_add(1, Ordering::Relaxed)
        ));
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let file = options.open(&temporary).map_err(io_error)?;
        Ok(Self {
            file,
            temporary,
            destination: destination.to_owned(),
        })
    }

    fn remove_owned(&self, path: &Path) {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let Ok(owned) = self.file.metadata() else {
                return;
            };
            let Ok(found) = std::fs::symlink_metadata(path) else {
                return;
            };
            if owned.dev() != found.dev() || owned.ino() != found.ino() {
                return;
            }
        }
        let _ = std::fs::remove_file(path);
    }

    fn publish(&self) -> Result<()> {
        self.file.sync_all().map_err(io_error)?;
        std::fs::hard_link(&self.temporary, &self.destination).map_err(io_error)?;
        #[cfg(unix)]
        {
            let directory = self
                .destination
                .parent()
                .filter(|path| !path.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            if let Err(error) = File::open(directory).and_then(|directory| directory.sync_all()) {
                self.remove_owned(&self.destination);
                return Err(io_error(error));
            }
        }
        Ok(())
    }
}

impl Drop for RestoredFile {
    fn drop(&mut self) {
        self.remove_owned(&self.temporary);
    }
}

impl Store {
    /// Reconstruct the original NDJSON export. This does not activate imported application state.
    pub async fn restore_sqlite_export(
        &self,
        archive_id: &str,
        destination: impl AsRef<Path>,
    ) -> Result<ArchiveReceipt> {
        let record = self
            .archive_record(archive_id)
            .await?
            .ok_or_else(|| Error::Invalid("SQLite archive does not exist".into()))?;
        let mut restored = RestoredFile::create(destination.as_ref())?;
        let mut file_hash = Sha256::new();
        for chunk in &record.chunks {
            let payload = self.read_archive_chunk(chunk).await?;
            restored.file.write_all(&payload).map_err(io_error)?;
            file_hash.update(&payload);
        }
        if hex(&file_hash.finalize()) != record.summary.file_sha256 {
            return Err(Error::IncompatibleStore);
        }
        restored.file.seek(SeekFrom::Start(0)).map_err(io_error)?;
        let summary = validate_reader(
            BufReader::new(&mut restored.file),
            ImportLimits {
                max_bytes: 4 * 1024 * 1024 * 1024,
                ..ImportLimits::default()
            },
            |_| Ok(()),
        )?;
        if summary != record.summary {
            return Err(Error::IncompatibleStore);
        }
        restored.publish()?;
        Ok(ArchiveReceipt {
            archive_id: archive_id.to_owned(),
            summary,
            chunk_count: record.chunks.len() as u64,
            replayed: true,
        })
    }

    pub async fn sqlite_archive(&self, archive_id: &str) -> Result<Option<ArchiveReceipt>> {
        Ok(self
            .archive_record(archive_id)
            .await?
            .map(|record| ArchiveReceipt {
                archive_id: archive_id.to_owned(),
                chunk_count: record.chunks.len() as u64,
                summary: record.summary,
                replayed: true,
            }))
    }

    async fn archive_record(&self, archive_id: &str) -> Result<Option<ArchiveRecord>> {
        if !is_hash(archive_id) {
            return invalid("invalid archive ID");
        }
        let key = store_key(&format!("archive/ready/{archive_id}"));
        let Some(encoded) = self.db.read(&key).await? else {
            return Ok(None);
        };
        if encoded.len() > MAX_TRANSACTION_BYTES {
            return Err(Error::IncompatibleStore);
        }
        let record: ArchiveRecord =
            serde_json::from_slice(&encoded).map_err(|_| Error::IncompatibleStore)?;
        if record.format_version != 1
            || !record.ready
            || record.summary.export_sha256 != archive_id
            || !is_hash(&record.summary.file_sha256)
            || record.summary.byte_count == 0
            || record.summary.byte_count > 4 * 1024 * 1024 * 1024
            || record.summary.table_count > 4096
            || record.summary.row_count > 10_000_000
            || record.chunks.is_empty()
            || record.chunks.len() > 8192
            || record.chunks.iter().any(|chunk| {
                !is_hash(&chunk.sha256)
                    || chunk.byte_count == 0
                    || chunk.byte_count > CHUNK_BYTES as u64
            })
            || record
                .chunks
                .iter()
                .try_fold(0u64, |total, chunk| total.checked_add(chunk.byte_count))
                != Some(record.summary.byte_count)
        {
            return Err(Error::IncompatibleStore);
        }
        Ok(Some(record))
    }

    pub async fn archive_sqlite_export(
        &self,
        path: impl AsRef<Path>,
        limits: ImportLimits,
    ) -> Result<ArchiveReceipt> {
        let limits = limits.validate()?;
        let path = path.as_ref().to_owned();
        let (file, expected) = tokio::task::spawn_blocking(move || {
            let mut file = open_export(&path, limits)?;
            let summary = validate_reader(BufReader::new(&mut file), limits, |_| Ok(()))?;
            file.seek(SeekFrom::Start(0)).map_err(io_error)?;
            Ok::<_, Error>((file, summary))
        })
        .await
        .map_err(|_| Error::Database("archive validation worker failed".into()))??;
        self.archive_validated_file(file, expected, limits).await
    }

    async fn archive_validated_file(
        &self,
        mut file: File,
        expected: ExportSummary,
        limits: ImportLimits,
    ) -> Result<ArchiveReceipt> {
        if let Some(record) = self.archive_record(&expected.export_sha256).await? {
            if record.summary != expected {
                return Err(Error::IncompatibleStore);
            }
            return Ok(ArchiveReceipt {
                archive_id: expected.export_sha256.clone(),
                summary: expected,
                chunk_count: record.chunks.len() as u64,
                replayed: true,
            });
        }
        let (send, mut receive) = tokio::sync::mpsc::channel::<String>(2);
        let producer = tokio::task::spawn_blocking(move || {
            let mut pending = String::new();
            let summary = validate_reader(BufReader::new(&mut file), limits, |line| {
                let mut text = std::str::from_utf8(line)
                    .map_err(|_| Error::Invalid("SQLite export: invalid UTF-8 framing".into()))?;
                while !text.is_empty() {
                    let mut take = (CHUNK_BYTES - pending.len()).min(text.len());
                    while !text.is_char_boundary(take) {
                        take -= 1;
                    }
                    if take == 0 {
                        send.blocking_send(std::mem::take(&mut pending))
                            .map_err(|_| Error::Database("archive staging stopped".into()))?;
                        continue;
                    }
                    pending.push_str(&text[..take]);
                    text = &text[take..];
                    if pending.len() == CHUNK_BYTES {
                        send.blocking_send(std::mem::take(&mut pending))
                            .map_err(|_| Error::Database("archive staging stopped".into()))?;
                    }
                }
                Ok(())
            })?;
            if !pending.is_empty() {
                send.blocking_send(pending)
                    .map_err(|_| Error::Database("archive staging stopped".into()))?;
            }
            Ok::<_, Error>(summary)
        });
        let mut chunks = Vec::new();
        while let Some(payload) = receive.recv().await {
            match self.archive_chunk(&payload).await {
                Ok(chunk) => chunks.push(chunk),
                Err(error) => {
                    drop(receive);
                    let _ = producer.await;
                    return Err(error);
                }
            }
        }
        let observed = producer
            .await
            .map_err(|_| Error::Database("archive validation worker failed".into()))??;
        if observed != expected {
            return invalid("source changed between validation and staging");
        }
        let record = ArchiveRecord {
            format_version: 1,
            ready: true,
            summary: expected.clone(),
            chunks,
        };
        let replayed = self.publish_archive(&record).await?;
        Ok(ArchiveReceipt {
            archive_id: expected.export_sha256.clone(),
            summary: expected,
            chunk_count: record.chunks.len() as u64,
            replayed,
        })
    }

    async fn archive_chunk(&self, payload: &str) -> Result<ChunkRef> {
        let chunk = ChunkRef {
            sha256: hash(payload.as_bytes()),
            byte_count: payload.len() as u64,
        };
        if serde_json::to_vec(payload)
            .map_err(|_| Error::Invalid("archive chunk serialization failed".into()))?
            .len()
            + 4096
            > MAX_TRANSACTION_BYTES
        {
            return invalid("archive chunk exceeds transaction budget");
        }
        for attempt in 0..3 {
            let result = self.stage_archive_chunk(&chunk, payload).await;
            match result {
                Err(Error::TransactionConflict | Error::DuplicateRecord) if attempt < 2 => continue,
                Err(error) => return Err(error),
                Ok(()) => break,
            }
        }
        self.read_archive_chunk(&chunk).await?;
        Ok(chunk)
    }

    async fn read_archive_chunk(&self, chunk: &ChunkRef) -> Result<Vec<u8>> {
        let witness_key = store_key(&format!("archive/chunk-meta/{}", chunk.sha256));
        let witness = self.db.read(&witness_key).await?;
        if witness.as_deref() != Some(chunk.byte_count.to_be_bytes().as_slice()) {
            return Err(Error::IncompatibleStore);
        }
        let payload_key = store_key(&format!("archive/chunk/{}", chunk.sha256));
        let payload = self
            .db
            .read(&payload_key)
            .await?
            .ok_or(Error::IncompatibleStore)?;
        if payload.len() as u64 != chunk.byte_count || hash(&payload) != chunk.sha256 {
            return Err(Error::IncompatibleStore);
        }
        Ok(payload)
    }

    async fn stage_archive_chunk(&self, chunk: &ChunkRef, payload: &str) -> Result<()> {
        let payload_key = store_key(&format!("archive/chunk/{}", chunk.sha256));
        let witness_key = store_key(&format!("archive/chunk-meta/{}", chunk.sha256));
        let witness = chunk.byte_count.to_be_bytes().to_vec();
        let mut tx = self.db.begin().await?;
        let result = async {
            let stored_payload = tx.get(&payload_key).await?;
            let stored_witness = tx.get(&witness_key).await?;
            if let Some(stored_payload) = stored_payload {
                if stored_payload != payload.as_bytes()
                    || stored_witness.as_deref() != Some(witness.as_slice())
                {
                    return Err(Error::IncompatibleStore);
                }
                return Ok(false);
            }
            if stored_witness.is_some() {
                return Err(Error::IncompatibleStore);
            }
            tx.insert(payload_key, payload.as_bytes().to_vec()).await?;
            tx.insert(witness_key, witness).await?;
            Ok(true)
        }
        .await;
        match result {
            Ok(true) => tx.commit().await,
            Ok(false) => tx.rollback().await,
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }

    async fn publish_archive(&self, record: &ArchiveRecord) -> Result<bool> {
        let record_json = serde_json::to_string(record)
            .map_err(|_| Error::Invalid("archive manifest serialization failed".into()))?;
        if record_json.len()
            + serde_json::to_vec(&record.chunks)
                .map_err(|_| Error::Invalid("archive manifest serialization failed".into()))?
                .len()
            + 4096
            > MAX_TRANSACTION_BYTES
        {
            return invalid("archive manifest exceeds transaction budget");
        }
        let ready_key = store_key(&format!("archive/ready/{}", record.summary.export_sha256));
        let mut tx = self.db.begin().await?;
        let result = async {
            if let Some(stored) = tx.get(&ready_key).await? {
                if stored != record_json.as_bytes() {
                    return Err(Error::IncompatibleStore);
                }
                return Ok(true);
            }
            // Immutable witnesses are committed with their payloads; publication need not reread the full archive.
            for chunk in &record.chunks {
                let witness_key = store_key(&format!("archive/chunk-meta/{}", chunk.sha256));
                if tx.get(&witness_key).await?.as_deref()
                    != Some(chunk.byte_count.to_be_bytes().as_slice())
                {
                    return Err(Error::IncompatibleStore);
                }
            }
            tx.insert(ready_key, record_json.into_bytes()).await?;
            Ok(false)
        }
        .await;
        match result {
            Ok(true) => {
                tx.rollback().await?;
                Ok(true)
            }
            Ok(false) => {
                tx.commit().await?;
                Ok(false)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn directory() -> tempfile::TempDir {
        let root = std::env::var_os("T3_CODE_LS_TEST_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        tempfile::Builder::new()
            .prefix("t3-sqlite-archive-")
            .tempdir_in(root)
            .unwrap()
    }

    fn line(value: &Value) -> Vec<u8> {
        let mut result = Vec::new();
        canonical(value, &mut result);
        result.push(b'\n');
        result
    }

    fn fixture(cells: Vec<Value>) -> Vec<u8> {
        let sql = "CREATE TABLE records (value)";
        let header = json!({"type":"header","format":FORMAT,"version":1,"sqliteVersion":"3.53.0","userVersion":0,"schemaVersion":1,"applicationId":0,"tableCount":1,"excludedDerivedTables":[],"sequences":[],"migrations":[],"schema":[{"type":"table","name":"records","tableName":"records","sql":sql}]});
        let table = json!({"type":"table","name":"records","sql":sql,"columns":[{"name":"value","declaredType":"","notNull":false,"defaultSql":null,"primaryKeyOrder":0,"hidden":0}],"rowCount":cells.len()});
        let mut bytes = line(&header);
        bytes.extend(line(&table));
        let mut rows = Vec::new();
        for (ordinal, cell) in cells.iter().enumerate() {
            rows.extend(line(
                &json!({"type":"row","table":"records","ordinal":ordinal,"values":[cell]}),
            ));
        }
        bytes.extend(&rows);
        let manifest = json!({"type":"manifest","tableCount":1,"rowCount":cells.len(),"tables":[{"name":"records","rowCount":cells.len(),"sha256":hash(&rows)}],"sha256":hash(&bytes)});
        bytes.extend(line(&manifest));
        bytes
    }

    #[test]
    fn validates_exact_sql_types_and_checksums() {
        let bytes = fixture(vec![
            json!({"type":"integer","value":"-9223372036854775808"}),
            json!({"type":"integer","value":"9223372036854775807"}),
            json!({"type":"real","value":"7ff0000000000000"}),
            json!({"type":"blob","value":"AID/"}),
            json!({"type":"text","encoding":"base64","value":"gP8="}),
            json!({"type":"text","value":"\u{feff}hello\0 💬"}),
            json!({"type":"null"}),
        ]);
        let summary =
            validate_reader(Cursor::new(&bytes), ImportLimits::default(), |_| Ok(())).unwrap();
        assert_eq!(summary.row_count, 7);
        assert_eq!(summary.byte_count, bytes.len() as u64);
        assert_eq!(summary.file_sha256, hash(&bytes));
    }

    #[test]
    fn rejects_missing_corrupt_trailing_and_duplicate_key_records() {
        let bytes = fixture(vec![json!({"type":"text","value":"original"})]);
        let text = String::from_utf8(bytes.clone()).unwrap();
        let last_start = text[..text.len() - 1].rfind('\n').unwrap() + 1;
        let cases = [
            text[..last_start].to_owned(),
            text.replace("original", "modified"),
            format!("{text}\n"),
            text.replace("\"ordinal\":0", "\"ordinal\":0,\"ordinal\":0"),
            text.replace("\"rowCount\":1", "\"rowCount\":2"),
            text.replace("\"ordinal\":0", "\"ordinal\":1"),
            text[..text.len() - 1].to_owned(),
        ];
        for case in cases {
            assert!(
                validate_reader(Cursor::new(case), ImportLimits::default(), |_| Ok(())).is_err()
            );
        }
    }

    #[test]
    fn rejects_invalid_sql_cells_even_with_valid_recomputed_hashes() {
        let cells = [
            json!({"type":"integer","value":"9223372036854775808"}),
            json!({"type":"integer","value":"01"}),
            json!({"type":"integer","value":"-0"}),
            json!({"type":"blob","value":"AB=="}),
            json!({"type":"blob","value":"A==="}),
            json!({"type":"blob","value":"AA"}),
            json!({"type":"real","value":"7ff8000000000000"}),
            json!({"type":"null","value":null}),
            json!({"type":"text","encoding":"hex","value":"ff"}),
        ];
        for cell in cells {
            assert!(
                validate_reader(
                    Cursor::new(fixture(vec![cell])),
                    ImportLimits::default(),
                    |_| Ok(())
                )
                .is_err()
            );
        }
    }

    #[test]
    fn enforces_streaming_limits_and_stops_before_extra_input() {
        let bytes = fixture(vec![json!({"type":"null"}), json!({"type":"null"})]);
        for limits in [
            ImportLimits {
                max_bytes: bytes.len() as u64 - 1,
                ..ImportLimits::default()
            },
            ImportLimits {
                max_line_bytes: 32,
                ..ImportLimits::default()
            },
            ImportLimits {
                max_rows: 1,
                ..ImportLimits::default()
            },
        ] {
            assert!(validate_reader(Cursor::new(&bytes), limits, |_| Ok(())).is_err());
        }
    }

    #[tokio::test]
    async fn archives_idempotently_without_changing_the_native_command_store() {
        let directory = directory();
        let path = directory.path().join("snapshot.ndjson");
        let bytes = fixture(vec![json!({"type":"text","value":"preserved"})]);
        std::fs::write(&path, &bytes).unwrap();
        let mut database = crate::test_support::TestFixture::start().await;
        let store = database.initialize().await;
        let receipt = store
            .archive_sqlite_export(&path, ImportLimits::default())
            .await
            .unwrap();
        assert!(!receipt.replayed);
        assert_eq!(receipt.summary.row_count, 1);
        assert_eq!(store.snapshot().await.unwrap().revision, 0);
        assert!(store.snapshot().await.unwrap().projections.is_empty());
        let replay = store
            .archive_sqlite_export(&path, ImportLimits::default())
            .await
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(receipt.archive_id, replay.archive_id);
        assert_eq!(
            store.sqlite_archive(&receipt.archive_id).await.unwrap(),
            Some(replay)
        );
        let restored = directory.path().join("restored.ndjson");
        let restored_receipt = store
            .restore_sqlite_export(&receipt.archive_id, &restored)
            .await
            .unwrap();
        assert_eq!(restored_receipt.summary, receipt.summary);
        assert_eq!(std::fs::read(&restored).unwrap(), bytes);
        assert!(
            store
                .restore_sqlite_export(&receipt.archive_id, &restored)
                .await
                .is_err()
        );
        assert_eq!(std::fs::read(&restored).unwrap(), bytes);
        store.shutdown().await.unwrap();
        database.restart().await;
        let store = database.connect().await;
        let recovered = store
            .sqlite_archive(&receipt.archive_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(recovered.summary, receipt.summary);
        let restarted_export = directory.path().join("after-restart.ndjson");
        store
            .restore_sqlite_export(&receipt.archive_id, &restarted_export)
            .await
            .unwrap();
        assert_eq!(std::fs::read(restarted_export).unwrap(), bytes);
    }

    #[tokio::test]
    async fn rejects_corrupt_archives_without_publishing_a_ready_record() {
        let directory = directory();
        let path = directory.path().join("snapshot.ndjson");
        let bytes = fixture(vec![json!({"type":"text","value":"original"})]);
        let summary =
            validate_reader(Cursor::new(&bytes), ImportLimits::default(), |_| Ok(())).unwrap();
        std::fs::write(
            &path,
            String::from_utf8(bytes.clone())
                .unwrap()
                .replace("original", "modified"),
        )
        .unwrap();
        let database = crate::test_support::TestFixture::start().await;
        let store = database.initialize().await;
        assert!(
            store
                .archive_sqlite_export(&path, ImportLimits::default())
                .await
                .is_err()
        );
        assert!(
            store
                .sqlite_archive(&summary.export_sha256)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(store.snapshot().await.unwrap().revision, 0);
        std::fs::write(&path, bytes).unwrap();
        let receipt = store
            .archive_sqlite_export(&path, ImportLimits::default())
            .await
            .unwrap();
        let record = store
            .archive_record(&receipt.archive_id)
            .await
            .unwrap()
            .unwrap();
        let witness_key = store_key(&format!("archive/chunk-meta/{}", record.chunks[0].sha256));
        let mut missing_witness = store.db.begin().await.unwrap();
        missing_witness.delete(witness_key.clone()).await.unwrap();
        missing_witness
            .delete(store_key(&format!("archive/ready/{}", receipt.archive_id)))
            .await
            .unwrap();
        missing_witness.commit().await.unwrap();
        assert!(store.publish_archive(&record).await.is_err());
        assert!(
            store
                .sqlite_archive(&receipt.archive_id)
                .await
                .unwrap()
                .is_none()
        );
        let mut repaired_witness = store.db.begin().await.unwrap();
        repaired_witness
            .insert(
                witness_key,
                record.chunks[0].byte_count.to_be_bytes().to_vec(),
            )
            .await
            .unwrap();
        repaired_witness.commit().await.unwrap();
        assert!(!store.publish_archive(&record).await.unwrap());
        let mut corruption = store.db.begin().await.unwrap();
        corruption
            .put(
                store_key(&format!("archive/chunk/{}", record.chunks[0].sha256)),
                b"tampered".to_vec(),
            )
            .await
            .unwrap();
        corruption.commit().await.unwrap();
        let restored = directory.path().join("restored.ndjson");
        assert!(
            store
                .restore_sqlite_export(&receipt.archive_id, &restored)
                .await
                .is_err()
        );
        assert!(!restored.exists());
        assert!(!std::fs::read_dir(directory.path()).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".partial")
        }));
    }

    #[tokio::test]
    async fn changed_source_leaves_no_ready_marker_and_does_not_poison_retry() {
        let directory = directory();
        let path = directory.path().join("snapshot.ndjson");
        let original = fixture(vec![json!({"type":"text","value":"💬".repeat(600_000)})]);
        let changed = fixture(vec![json!({"type":"text","value":"🧪".repeat(600_000)})]);
        std::fs::write(&path, &original).unwrap();
        let limits = ImportLimits::default();
        let mut file = open_export(&path, limits).unwrap();
        let expected = validate_reader(BufReader::new(&mut file), limits, |_| Ok(())).unwrap();
        file.seek(SeekFrom::Start(0)).unwrap();
        std::fs::write(&path, changed).unwrap();
        let database = crate::test_support::TestFixture::start().await;
        let store = database.initialize().await;
        assert!(
            store
                .archive_validated_file(file, expected.clone(), limits)
                .await
                .is_err()
        );
        assert!(
            store
                .sqlite_archive(&expected.export_sha256)
                .await
                .unwrap()
                .is_none()
        );
        std::fs::write(&path, &original).unwrap();
        let receipt = store.archive_sqlite_export(&path, limits).await.unwrap();
        assert!(receipt.chunk_count > 1);
        assert_eq!(receipt.summary, expected);
        let restored = directory.path().join("restored.ndjson");
        store
            .restore_sqlite_export(&receipt.archive_id, &restored)
            .await
            .unwrap();
        assert_eq!(std::fs::read(restored).unwrap(), original);
    }
}
