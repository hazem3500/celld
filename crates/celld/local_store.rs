// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The persistent object store for `celld dev`.
//!
//! The development supervisor and its node are separate processes, and both
//! write the fleet store during a reload. SQLite supplies the cross-process
//! transaction that a directory of files cannot: a conditional update checks
//! its ETag and installs the new object in one commit. This backend is local to
//! one development machine. It is not a shared-filesystem production mode.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::BoxFuture;
use futures_util::stream::{self, BoxStream};
use futures_util::FutureExt as _;
use futures_util::StreamExt as _;
use object_store::list::{PaginatedListOptions, PaginatedListResult, PaginatedListStore};
use object_store::path::Path;
use object_store::{
    Attribute, AttributeValue, Attributes, Error, GetOptions, GetResult, GetResultPayload,
    ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions,
    PutPayload, PutResult,
};
use rusqlite::{params, Connection, OptionalExtension as _, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;
use std::path::{Path as FsPath, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

const STORE: &str = "celld development store";

#[derive(Clone, Debug)]
pub(crate) struct LocalStore {
    database: PathBuf,
}

#[derive(Debug)]
struct StoredObject {
    key: String,
    body: Vec<u8>,
    size: u64,
    etag: i64,
    modified_ms: i64,
    attributes: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct StoredAttribute {
    kind: String,
    name: Option<String>,
    value: String,
}

impl LocalStore {
    pub(crate) fn open(database: impl AsRef<FsPath>) -> object_store::Result<Self> {
        let database = database.as_ref().to_path_buf();
        let store = Self { database };
        let connection = store.connect()?;
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .map_err(db_error)?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS objects (
                   key TEXT PRIMARY KEY,
                   body BLOB NOT NULL,
                   etag INTEGER NOT NULL,
                   modified_ms INTEGER NOT NULL,
                   attributes TEXT NOT NULL,
                   body_len INTEGER NOT NULL DEFAULT 0
                 );
                 CREATE TABLE IF NOT EXISTS store_sequence (
                   singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                   next_etag INTEGER NOT NULL
                 );
                 INSERT OR IGNORE INTO store_sequence(singleton, next_etag) VALUES (1, 1);",
            )
            .map_err(db_error)?;
        migrate_objects_schema(&connection)?;
        Ok(store)
    }

    fn connect(&self) -> object_store::Result<Connection> {
        let connection = Connection::open(&self.database).map_err(db_error)?;
        configure_connection(&connection)?;
        Ok(connection)
    }

    fn read(&self, key: &str) -> object_store::Result<StoredObject> {
        self.connect()?
            .query_row(
                "SELECT key, body, etag, modified_ms, attributes, body_len
                 FROM objects WHERE key = ?1",
                [key],
                |row| {
                    Ok(StoredObject {
                        key: row.get(0)?,
                        body: row.get(1)?,
                        size: row.get::<_, i64>(5)? as u64,
                        etag: row.get(2)?,
                        modified_ms: row.get(3)?,
                        attributes: row.get(4)?,
                    })
                },
            )
            .optional()
            .map_err(db_error)?
            .ok_or_else(|| not_found(key))
    }

    /// Metadata-only list. Optional `prefix` uses a key-range predicate so SQLite
    /// can range-scan the covering `objects_list` index instead of walking every
    /// row and computing `length(body)`.
    fn metadata(&self, prefix: Option<&str>) -> object_store::Result<Vec<StoredObject>> {
        let connection = self.connect()?;
        let prefix = prefix.unwrap_or("");
        let (sql, bind): (&str, Vec<String>) = match key_prefix_upper(prefix) {
            Some(upper) => (
                "SELECT key, etag, modified_ms, attributes, body_len
                 FROM objects
                 WHERE key >= ?1 AND key < ?2
                 ORDER BY key",
                vec![prefix.to_string(), upper],
            ),
            None if prefix.is_empty() => (
                "SELECT key, etag, modified_ms, attributes, body_len
                 FROM objects
                 ORDER BY key",
                Vec::new(),
            ),
            None => (
                "SELECT key, etag, modified_ms, attributes, body_len
                 FROM objects
                 WHERE key >= ?1
                 ORDER BY key",
                vec![prefix.to_string()],
            ),
        };
        let mut statement = connection.prepare(sql).map_err(db_error)?;
        let map_row = |row: &rusqlite::Row<'_>| -> rusqlite::Result<StoredObject> {
            Ok(StoredObject {
                key: row.get(0)?,
                body: Vec::new(),
                size: row.get::<_, i64>(4)? as u64,
                etag: row.get(1)?,
                modified_ms: row.get(2)?,
                attributes: row.get(3)?,
            })
        };
        if bind.is_empty() {
            statement
                .query_map([], map_row)
                .map_err(db_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(db_error)
        } else if bind.len() == 1 {
            statement
                .query_map(params![bind[0]], map_row)
                .map_err(db_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(db_error)
        } else {
            statement
                .query_map(params![bind[0], bind[1]], map_row)
                .map_err(db_error)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(db_error)
        }
    }

    #[cfg(test)]
    fn list_plan(&self, prefix: &str) -> object_store::Result<String> {
        let connection = self.connect()?;
        let format_row = |row: &rusqlite::Row<'_>| -> rusqlite::Result<String> {
            Ok(format!(
                "{}|{}|{}|{}",
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, String>(3)?
            ))
        };
        let lines = match key_prefix_upper(prefix) {
            Some(upper) => {
                let mut statement = connection
                    .prepare(
                        "EXPLAIN QUERY PLAN
                         SELECT key, etag, modified_ms, attributes, body_len
                         FROM objects
                         WHERE key >= ?1 AND key < ?2
                         ORDER BY key",
                    )
                    .map_err(db_error)?;
                let lines = statement
                    .query_map(params![prefix, upper], format_row)
                    .map_err(db_error)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(db_error)?;
                lines
            }
            None if prefix.is_empty() => {
                let mut statement = connection
                    .prepare(
                        "EXPLAIN QUERY PLAN
                         SELECT key, etag, modified_ms, attributes, body_len
                         FROM objects
                         ORDER BY key",
                    )
                    .map_err(db_error)?;
                let lines = statement
                    .query_map([], format_row)
                    .map_err(db_error)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(db_error)?;
                lines
            }
            None => {
                let mut statement = connection
                    .prepare(
                        "EXPLAIN QUERY PLAN
                         SELECT key, etag, modified_ms, attributes, body_len
                         FROM objects
                         WHERE key >= ?1
                         ORDER BY key",
                    )
                    .map_err(db_error)?;
                let lines = statement
                    .query_map(params![prefix], format_row)
                    .map_err(db_error)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(db_error)?;
                lines
            }
        };
        Ok(lines.join("\n"))
    }

    fn put_sync(
        &self,
        key: String,
        body: Bytes,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let attributes = encode_attributes(&options.attributes)?;
        let modified_ms = crate::asyncrt::wall_ms();
        let mut connection = self.connect()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let current = transaction
            .query_row("SELECT etag FROM objects WHERE key = ?1", [&key], |row| {
                row.get::<_, i64>(0)
            })
            .optional()
            .map_err(db_error)?;
        match options.mode {
            PutMode::Overwrite => {}
            PutMode::Create if current.is_some() => return Err(already_exists(&key)),
            PutMode::Create => {}
            PutMode::Update(version) => {
                let current = current.map(|etag| etag.to_string());
                if current.as_deref().is_none() || version.e_tag.as_deref() != current.as_deref() {
                    return Err(precondition(&key));
                }
            }
        }
        let etag = transaction
            .query_row(
                "UPDATE store_sequence SET next_etag = next_etag + 1
                 WHERE singleton = 1 RETURNING next_etag - 1",
                [],
                |row| row.get::<_, i64>(0),
            )
            .map_err(db_error)?;
        let body_len = body.len() as i64;
        transaction
            .execute(
                "INSERT INTO objects(key, body, etag, modified_ms, attributes, body_len)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                 ON CONFLICT(key) DO UPDATE SET
                   body = excluded.body,
                   etag = excluded.etag,
                   modified_ms = excluded.modified_ms,
                   attributes = excluded.attributes,
                   body_len = excluded.body_len",
                params![key, body.as_ref(), etag, modified_ms, attributes, body_len],
            )
            .map_err(db_error)?;
        transaction.commit().map_err(db_error)?;
        Ok(PutResult {
            e_tag: Some(etag.to_string()),
            version: None,
        })
    }

    fn copy_sync(&self, from: &str, to: &str, create: bool) -> object_store::Result<()> {
        let source = self.read(from)?;
        let attributes = decode_attributes(&source.attributes)?;
        self.put_sync(
            to.to_string(),
            source.body.into(),
            PutOptions {
                mode: if create {
                    PutMode::Create
                } else {
                    PutMode::Overwrite
                },
                attributes,
                ..PutOptions::default()
            },
        )?;
        Ok(())
    }
}

fn configure_connection(connection: &Connection) -> object_store::Result<()> {
    // `synchronous` is connection-local. Setting it only while the schema is
    // created leaves later writes at the bundled SQLite default, so a build
    // configuration can silently weaken the development store's durability.
    connection
        .pragma_update(None, "synchronous", "FULL")
        .map_err(db_error)?;
    connection
        .busy_timeout(Duration::from_secs(30))
        .map_err(db_error)
}

fn migrate_objects_schema(connection: &Connection) -> object_store::Result<()> {
    let has_body_len: bool = connection
        .prepare("PRAGMA table_info(objects)")
        .map_err(db_error)?
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(db_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(db_error)?
        .iter()
        .any(|name| name == "body_len");
    if !has_body_len {
        connection
            .execute_batch(
                "ALTER TABLE objects ADD COLUMN body_len INTEGER NOT NULL DEFAULT 0;
                 UPDATE objects SET body_len = length(body);",
            )
            .map_err(db_error)?;
    } else {
        connection
            .execute(
                "UPDATE objects SET body_len = length(body) WHERE body_len = 0 AND length(body) > 0",
                [],
            )
            .map_err(db_error)?;
    }
    connection
        .execute_batch(
            "CREATE INDEX IF NOT EXISTS objects_list
             ON objects(key, etag, modified_ms, attributes, body_len);",
        )
        .map_err(db_error)?;
    Ok(())
}

/// Exclusive upper bound for a lexicographic key-prefix scan, or `None` when
/// the prefix has no successor (every byte is 0xff) or is empty.
fn key_prefix_upper(prefix: &str) -> Option<String> {
    if prefix.is_empty() {
        return None;
    }
    let mut bytes = prefix.as_bytes().to_vec();
    while let Some(last) = bytes.last_mut() {
        if *last != 0xff {
            *last += 1;
            return Some(String::from_utf8_lossy(&bytes).into_owned());
        }
        bytes.pop();
    }
    None
}

impl fmt::Display for LocalStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "LocalStore({})", self.database.display())
    }
}

#[async_trait]
impl ObjectStore for LocalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        let store = self.clone();
        let key = location.to_string();
        let body: Bytes = payload.into();
        crate::asyncrt::blocking(move || store.put_sync(key, body, options))
            .await
            .map_err(db_error)?
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(LocalUpload {
            store: self.clone(),
            location: location.clone(),
            attributes: options.attributes,
            parts: Arc::new(Mutex::new(Vec::new())),
        }))
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let store = self.clone();
        let key = location.to_string();
        let object = crate::asyncrt::blocking(move || store.read(&key))
            .await
            .map_err(db_error)??;
        let meta = object_meta(&object)?;
        options.check_preconditions(&meta)?;
        let full = 0..object.body.len() as u64;
        let range = match options.range {
            Some(range) => range.as_range(full.end).map_err(db_error)?,
            None => full,
        };
        let body = Bytes::from(object.body).slice(range.start as usize..range.end as usize);
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream::once(async move { Ok(body) }).boxed()),
            meta,
            range,
            attributes: decode_attributes(&object.attributes)?,
        })
    }

    async fn delete(&self, location: &Path) -> object_store::Result<()> {
        let store = self.clone();
        let key = location.to_string();
        crate::asyncrt::blocking(move || {
            store
                .connect()?
                .execute("DELETE FROM objects WHERE key = ?1", [key])
                .map_err(db_error)?;
            Ok(())
        })
        .await
        .map_err(db_error)?
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let key_prefix = prefix.as_ref().map(|path| path.to_string());
        let result = self
            .metadata(key_prefix.as_deref())
            .and_then(|objects| {
                objects
                    .into_iter()
                    .map(|object| object_meta(&object))
                    .filter(|meta| {
                        meta.as_ref().map_or(true, |meta| {
                            prefix.as_ref().is_none_or(|prefix| {
                                meta.location
                                    .prefix_match(prefix)
                                    .is_some_and(|mut remainder| remainder.next().is_some())
                            })
                        })
                    })
                    .collect::<object_store::Result<Vec<_>>>()
            });
        match result {
            Ok(objects) => stream::iter(objects.into_iter().map(Ok)).boxed(),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        }
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let key_prefix = prefix.to_string();
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();
        for object in self.metadata(Some(&key_prefix))? {
            let location = stored_location(&object.key)?;
            let Some(mut remainder) = location.prefix_match(&prefix) else {
                continue;
            };
            let Some(child) = remainder.next() else {
                continue;
            };
            if remainder.next().is_some() {
                common_prefixes.insert(prefix.child(child));
            } else {
                objects.push(object_meta(&object)?);
            }
        }
        Ok(ListResult {
            common_prefixes: common_prefixes.into_iter().collect(),
            objects,
        })
    }

    async fn copy(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let store = self.clone();
        let from = from.to_string();
        let to = to.to_string();
        crate::asyncrt::blocking(move || store.copy_sync(&from, &to, false))
            .await
            .map_err(db_error)?
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> object_store::Result<()> {
        let store = self.clone();
        let from = from.to_string();
        let to = to.to_string();
        crate::asyncrt::blocking(move || store.copy_sync(&from, &to, true))
            .await
            .map_err(db_error)?
    }
}

#[async_trait]
impl PaginatedListStore for LocalStore {
    async fn list_paginated(
        &self,
        prefix: Option<&str>,
        options: PaginatedListOptions,
    ) -> object_store::Result<PaginatedListResult> {
        let prefix = prefix.unwrap_or_default();
        let after = options.page_token.as_deref().or(options.offset.as_deref());
        let delimiter = options.delimiter.as_deref();
        let limit = options.max_keys.unwrap_or(usize::MAX);
        let mut entries: Vec<(String, Option<Path>, Option<ObjectMeta>)> = Vec::new();
        for object in self.metadata(Some(prefix))? {
            let Some(remainder) = object.key.strip_prefix(prefix) else {
                continue;
            };
            if after.is_some_and(|after| object.key.as_str() <= after) {
                continue;
            }
            let common = delimiter.and_then(|delimiter| {
                remainder.find(delimiter).map(|index| {
                    stored_location(&format!("{prefix}{}{}", &remainder[..index], delimiter))
                })
            });
            let common = common.transpose()?;
            if let Some(common) = common {
                if let Some((last, Some(previous), _)) = entries.last_mut() {
                    if *previous == common {
                        *last = object.key;
                        continue;
                    }
                }
                entries.push((object.key, Some(common), None));
            } else {
                entries.push((object.key.clone(), None, Some(object_meta(&object)?)));
            }
        }
        let truncated = entries.len() > limit;
        entries.truncate(limit);
        let page_token = truncated
            .then(|| entries.last().map(|(last, _, _)| last.clone()))
            .flatten();
        let mut result = ListResult {
            common_prefixes: Vec::new(),
            objects: Vec::new(),
        };
        for (_, common, object) in entries {
            if let Some(common) = common {
                result.common_prefixes.push(common);
            }
            if let Some(object) = object {
                result.objects.push(object);
            }
        }
        Ok(PaginatedListResult { result, page_token })
    }
}

#[derive(Debug)]
struct LocalUpload {
    store: LocalStore,
    location: Path,
    attributes: Attributes,
    parts: Arc<Mutex<Vec<Bytes>>>,
}

#[async_trait]
impl MultipartUpload for LocalUpload {
    fn put_part(&mut self, data: PutPayload) -> BoxFuture<'static, object_store::Result<()>> {
        self.parts.lock().unwrap().push(data.into());
        async { Ok(()) }.boxed()
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let parts = std::mem::take(&mut *self.parts.lock().unwrap());
        let bytes = parts.iter().map(Bytes::len).sum();
        let mut body = Vec::with_capacity(bytes);
        for part in parts {
            body.extend_from_slice(&part);
        }
        self.store
            .put_opts(
                &self.location,
                body.into(),
                PutOptions {
                    attributes: self.attributes.clone(),
                    ..PutOptions::default()
                },
            )
            .await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.parts.lock().unwrap().clear();
        Ok(())
    }
}

fn object_meta(object: &StoredObject) -> object_store::Result<ObjectMeta> {
    let modified = SystemTime::UNIX_EPOCH
        .checked_add(Duration::from_millis(object.modified_ms.max(0) as u64))
        .ok_or_else(|| message_error("the object timestamp is outside the system clock range"))?;
    Ok(ObjectMeta {
        location: stored_location(&object.key)?,
        last_modified: modified.into(),
        size: object.size,
        e_tag: Some(object.etag.to_string()),
        version: None,
    })
}

fn encode_attributes(attributes: &Attributes) -> object_store::Result<String> {
    let mut stored = Vec::with_capacity(attributes.len());
    for (attribute, value) in attributes {
        let (kind, name) = match attribute {
            Attribute::ContentDisposition => ("content-disposition", None),
            Attribute::ContentEncoding => ("content-encoding", None),
            Attribute::ContentLanguage => ("content-language", None),
            Attribute::ContentType => ("content-type", None),
            Attribute::CacheControl => ("cache-control", None),
            Attribute::StorageClass => ("storage-class", None),
            Attribute::Metadata(name) => ("metadata", Some(name.as_ref().to_string())),
            _ => {
                return Err(message_error(
                    "the development store does not support this attribute",
                ))
            }
        };
        stored.push(StoredAttribute {
            kind: kind.to_string(),
            name,
            value: value.as_ref().to_string(),
        });
    }
    serde_json::to_string(&stored).map_err(db_error)
}

fn decode_attributes(encoded: &str) -> object_store::Result<Attributes> {
    let stored: Vec<StoredAttribute> = serde_json::from_str(encoded).map_err(db_error)?;
    let mut attributes = Attributes::with_capacity(stored.len());
    for stored in stored {
        let attribute = match stored.kind.as_str() {
            "content-disposition" => Attribute::ContentDisposition,
            "content-encoding" => Attribute::ContentEncoding,
            "content-language" => Attribute::ContentLanguage,
            "content-type" => Attribute::ContentType,
            "cache-control" => Attribute::CacheControl,
            "storage-class" => Attribute::StorageClass,
            "metadata" => Attribute::Metadata(stored.name.unwrap_or_default().into()),
            kind => return Err(message_error(format!("unknown stored attribute {kind:?}"))),
        };
        attributes.insert(attribute, AttributeValue::from(stored.value));
    }
    Ok(attributes)
}

#[cfg(all(test, celld_internal_tests))]
mod internal_tests {
    include!(env!("CELLD_INTERNAL_LOCAL_STORE_TESTS"));
}

#[cfg(test)]
mod list_index_tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn list_uses_covering_index_range_scan() {
        let dir = TempDir::new().unwrap();
        let store = LocalStore::open(dir.path().join("objects.sqlite3")).unwrap();
        for i in 0..200 {
            store
                .put_sync(
                    format!("cells/user/{i:04}.ltx"),
                    Bytes::from(vec![b'x'; 64]),
                    PutOptions::default(),
                )
                .unwrap();
            store
                .put_sync(
                    format!("other/{i:04}"),
                    Bytes::from(vec![b'y'; 8]),
                    PutOptions::default(),
                )
                .unwrap();
        }
        let listed = store.metadata(Some("cells/")).unwrap();
        assert_eq!(listed.len(), 200);
        assert!(listed.iter().all(|object| object.key.starts_with("cells/")));

        let plan = store.list_plan("cells/").unwrap().to_ascii_lowercase();
        assert!(
            plan.contains("objects_list"),
            "expected objects_list index in plan, got:\n{plan}"
        );
        assert!(
            plan.contains("covering") || plan.contains("using index"),
            "expected covering/index range scan, got:\n{plan}"
        );
        assert!(
            !plan.contains("scan table objects"),
            "must not full-scan objects table, got:\n{plan}"
        );
    }

    #[test]
    fn key_prefix_upper_increments_last_byte() {
        assert_eq!(key_prefix_upper("cells/").as_deref(), Some("cells0"));
        assert_eq!(key_prefix_upper("").as_deref(), None);
    }
}

/// The [`Path`] a stored key came from. Every key is stored as
/// `location.to_string()`, which is already percent-encoded, so this parses
/// that form back. `Path::from` would encode it a second time, and a listed
/// `přehled.html` would come back as `p%25C5%2599ehled.html`, a key that
/// names no object (denoland/celld#232).
fn stored_location(key: &str) -> object_store::Result<Path> {
    Ok(Path::parse(key)?)
}

fn db_error(error: impl fmt::Display) -> Error {
    message_error(error.to_string())
}

fn message_error(message: impl Into<String>) -> Error {
    Error::Generic {
        store: STORE,
        source: Box::new(std::io::Error::other(message.into())),
    }
}

fn not_found(path: &str) -> Error {
    Error::NotFound {
        path: path.to_string(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "the object does not exist",
        )),
    }
}

fn already_exists(path: &str) -> Error {
    Error::AlreadyExists {
        path: path.to_string(),
        source: Box::new(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "the object already exists",
        )),
    }
}

fn precondition(path: &str) -> Error {
    Error::Precondition {
        path: path.to_string(),
        source: Box::new(std::io::Error::other("the ETag does not match")),
    }
}
