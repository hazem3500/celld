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
        let mut connection = store.connect()?;
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
        migrate_objects_schema(&mut connection)?;
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
                "SELECT key, body, etag, modified_ms, attributes
                 FROM objects WHERE key = ?1",
                [key],
                |row| {
                    let body: Vec<u8> = row.get(1)?;
                    Ok(StoredObject {
                        key: row.get(0)?,
                        size: body.len() as u64,
                        body,
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

    /// Metadata-only list over a key range. The query reads only the
    /// covering `objects_list` index: it bounds the key with `prefix` (and
    /// `after`) so SQLite seeks to the range instead of walking every key in
    /// the store, and it selects the stored `body_len` rather than
    /// `length(body)`, which that index does not hold.
    fn metadata(&self, query: ListQuery<'_>) -> object_store::Result<Vec<StoredObject>> {
        let connection = self.connect()?;
        let (sql, binds) = query.sql();
        let mut statement = connection.prepare(&sql).map_err(db_error)?;
        let rows = statement
            .query_map(rusqlite::params_from_iter(binds.iter()), |row| {
                Ok(StoredObject {
                    key: row.get(0)?,
                    body: Vec::new(),
                    size: row.get::<_, i64>(4)?.max(0) as u64,
                    etag: row.get(1)?,
                    modified_ms: row.get(2)?,
                    attributes: row.get(3)?,
                })
            })
            .map_err(db_error)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(db_error)
    }

    /// `metadata` off the async worker. A list on a large store reads index
    /// pages for as long as the range is wide; running that inline blocked
    /// the runtime thread that polled it.
    async fn metadata_blocking(
        &self,
        query: OwnedListQuery,
    ) -> object_store::Result<Vec<StoredObject>> {
        let store = self.clone();
        crate::asyncrt::blocking(move || store.metadata(query.borrow()))
            .await
            .map_err(db_error)?
    }

    #[cfg(test)]
    fn list_plan(&self, query: ListQuery<'_>) -> object_store::Result<String> {
        let connection = self.connect()?;
        let (sql, binds) = query.sql();
        let mut statement = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .map_err(db_error)?;
        let lines = statement
            .query_map(rusqlite::params_from_iter(binds.iter()), |row| {
                row.get::<_, String>(3)
            })
            .map_err(db_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(db_error)?;
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

/// The `objects` schema version this build writes, in `PRAGMA user_version`.
/// 1: `body_len` exists, is set on every write and was backfilled once, and
/// `objects_list` covers a metadata list.
const OBJECTS_SCHEMA_VERSION: i64 = 1;

/// Rows per backfill transaction. Each batch rewrites only rows whose
/// `body_len` disagrees with their body, and commits so the WAL can
/// checkpoint between batches instead of growing by the whole store.
const BODY_LEN_BACKFILL_BATCH: i64 = 2048;

/// Brings a store to [`OBJECTS_SCHEMA_VERSION`]. The backfill runs once: a
/// store at the current version only re-asserts the index. Both the dev
/// supervisor and its node open the store, so every step is idempotent and
/// each batch is its own immediate transaction.
fn migrate_objects_schema(connection: &mut Connection) -> object_store::Result<()> {
    let version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .map_err(db_error)?;
    if version < OBJECTS_SCHEMA_VERSION {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let has_body_len = transaction
            .prepare("PRAGMA table_info(objects)")
            .map_err(db_error)?
            .query_map([], |row| row.get::<_, String>(1))
            .map_err(db_error)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(db_error)?
            .iter()
            .any(|name| name == "body_len");
        if !has_body_len {
            transaction
                .execute_batch(
                    "ALTER TABLE objects ADD COLUMN body_len INTEGER NOT NULL DEFAULT 0;",
                )
                .map_err(db_error)?;
        }
        transaction.commit().map_err(db_error)?;
        backfill_body_len(connection)?;
    }
    connection
        .execute_batch(
            "CREATE INDEX IF NOT EXISTS objects_list
             ON objects(key, etag, modified_ms, attributes, body_len);",
        )
        .map_err(db_error)?;
    if version < OBJECTS_SCHEMA_VERSION {
        connection
            .pragma_update(None, "user_version", OBJECTS_SCHEMA_VERSION)
            .map_err(db_error)?;
    }
    Ok(())
}

/// Sets `body_len` on every row written before it was maintained, in rowid
/// batches. `length(body)` reads a blob's size from the record header, not
/// its overflow pages, so only the rows that are actually wrong are
/// rewritten. Returns the number of rows it fixed.
fn backfill_body_len(connection: &mut Connection) -> object_store::Result<u64> {
    let mut after: i64 = i64::MIN;
    let mut fixed = 0_u64;
    loop {
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(db_error)?;
        let last: Option<i64> = transaction
            .query_row(
                "SELECT max(rowid) FROM (
                   SELECT rowid FROM objects WHERE rowid > ?1 ORDER BY rowid LIMIT ?2
                 )",
                params![after, BODY_LEN_BACKFILL_BATCH],
                |row| row.get(0),
            )
            .map_err(db_error)?;
        let Some(last) = last else {
            transaction.commit().map_err(db_error)?;
            return Ok(fixed);
        };
        fixed += transaction
            .execute(
                "UPDATE objects SET body_len = length(body)
                 WHERE rowid > ?1 AND rowid <= ?2 AND body_len <> length(body)",
                params![after, last],
            )
            .map_err(db_error)? as u64;
        transaction.commit().map_err(db_error)?;
        after = last;
    }
}

/// One metadata list: keys in `[prefix, successor(prefix))`, strictly after
/// `after`, in key order, at most `limit` rows.
#[derive(Clone, Copy, Debug, Default)]
struct ListQuery<'a> {
    prefix: &'a str,
    after: Option<&'a str>,
    limit: Option<usize>,
}

/// A [`ListQuery`] that can move to a blocking thread.
#[derive(Clone, Debug, Default)]
struct OwnedListQuery {
    prefix: String,
    after: Option<String>,
    limit: Option<usize>,
}

impl OwnedListQuery {
    fn borrow(&self) -> ListQuery<'_> {
        ListQuery {
            prefix: &self.prefix,
            after: self.after.as_deref(),
            limit: self.limit,
        }
    }
}

impl ListQuery<'_> {
    fn sql(&self) -> (String, Vec<String>) {
        let mut clauses = Vec::new();
        let mut binds = Vec::new();
        if !self.prefix.is_empty() {
            binds.push(self.prefix.to_string());
            clauses.push(format!("key >= ?{}", binds.len()));
            if let Some(upper) = key_prefix_upper(self.prefix) {
                binds.push(upper);
                clauses.push(format!("key < ?{}", binds.len()));
            }
        }
        if let Some(after) = self.after {
            binds.push(after.to_string());
            clauses.push(format!("key > ?{}", binds.len()));
        }
        let mut sql =
            String::from("SELECT key, etag, modified_ms, attributes, body_len FROM objects");
        if !clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY key");
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {}", limit.min(i64::MAX as usize)));
        }
        (sql, binds)
    }
}

/// The SQL prefix for an `ObjectStore` path prefix. A path prefix matches
/// whole segments (`a/b` lists `a/b/c`, not `a/bc`), so the range is
/// `a/b/`; the segment filter after it stays as the definition.
fn path_key_prefix(prefix: Option<&Path>) -> String {
    match prefix.map(Path::to_string) {
        Some(prefix) if !prefix.is_empty() => format!("{prefix}/"),
        _ => String::new(),
    }
}

/// The least string greater than every string that starts with `prefix`, or
/// `None` when there is none (empty, or only `char::MAX`). SQLite compares
/// TEXT by its UTF-8 bytes, and UTF-8 byte order is code point order, so
/// stepping the last code point gives an exact exclusive bound that is still
/// valid UTF-8; incrementing the last byte could produce invalid UTF-8.
fn key_prefix_upper(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let next = match last as u32 {
            0xD7FF => Some('\u{E000}'),
            code => char::from_u32(code + 1),
        };
        if let Some(next) = next {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
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
        let store = self.clone();
        let prefix = prefix.cloned();
        let query = OwnedListQuery {
            prefix: path_key_prefix(prefix.as_ref()),
            ..OwnedListQuery::default()
        };
        stream::once(async move {
            let objects = store.metadata_blocking(query).await?;
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
        })
        .flat_map(|result| match result {
            Ok(objects) => stream::iter(objects.into_iter().map(Ok)).boxed(),
            Err(error) => stream::once(async move { Err(error) }).boxed(),
        })
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        let query = OwnedListQuery {
            prefix: path_key_prefix(prefix),
            ..OwnedListQuery::default()
        };
        let prefix = prefix.cloned().unwrap_or_default();
        let mut common_prefixes = BTreeSet::new();
        let mut objects = Vec::new();
        for object in self.metadata_blocking(query).await? {
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
        // Without a delimiter every row is one entry, so one row past the
        // page proves truncation. With one, rows fold into common prefixes
        // and the page needs the whole range after `after`.
        let query = OwnedListQuery {
            prefix: prefix.to_string(),
            after: after.map(str::to_string),
            limit: delimiter.is_none().then(|| limit.saturating_add(1)),
        };
        let mut entries: Vec<(String, Option<Path>, Option<ObjectMeta>)> = Vec::new();
        for object in self.metadata_blocking(query).await? {
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

#[cfg(test)]
mod list_index_tests {
    use super::*;
    use object_store::list::PaginatedListOptions;
    use std::sync::OnceLock;
    use tempfile::TempDir;

    /// `metadata_blocking` runs on the process async domain, which a unit
    /// test binary has to install itself, once.
    fn runtime() -> &'static tokio::runtime::Runtime {
        static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
        RUNTIME.get_or_init(|| {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            crate::asyncrt::set_host_handle(runtime.handle().clone());
            runtime
        })
    }

    fn put(store: &LocalStore, key: &str, len: usize) {
        store
            .put_sync(
                key.to_string(),
                Bytes::from(vec![b'x'; len]),
                PutOptions::default(),
            )
            .unwrap();
    }

    fn seeded() -> (TempDir, LocalStore) {
        let dir = TempDir::new().unwrap();
        let store = LocalStore::open(dir.path().join("objects.sqlite3")).unwrap();
        for i in 0..200 {
            put(&store, &format!("cells/user/ltx/e1/0/{i:04}.ltx"), 64 + i);
            put(&store, &format!("other/{i:04}"), 8);
        }
        put(&store, "nodes/dev-1.json", 3);
        put(&store, "nodes-archive/old.json", 5);
        (dir, store)
    }

    fn plan(store: &LocalStore, query: ListQuery<'_>) -> String {
        store.list_plan(query).unwrap().to_ascii_lowercase()
    }

    #[test]
    fn prefix_list_is_a_covering_index_range_search() {
        let (_dir, store) = seeded();
        let plan = plan(
            &store,
            ListQuery {
                prefix: "nodes/",
                ..ListQuery::default()
            },
        );
        assert!(
            plan.contains("search objects using covering index objects_list (key>? and key<?)"),
            "expected a covering range search, got:\n{plan}"
        );
    }

    #[test]
    fn whole_store_list_still_reads_only_the_covering_index() {
        let (_dir, store) = seeded();
        let plan = plan(&store, ListQuery::default());
        assert!(
            plan.contains("using covering index objects_list"),
            "a full list must not visit the table rows, got:\n{plan}"
        );
    }

    #[test]
    fn upstream_list_shape_was_not_covering() {
        // The statement the released binary runs. Kept as the measured
        // "before": SQLite cannot cover length(body) from objects_list.
        let (_dir, store) = seeded();
        let connection = store.connect().unwrap();
        let plan: Vec<String> = connection
            .prepare(
                "EXPLAIN QUERY PLAN SELECT key, CAST(X'' AS BLOB), etag, modified_ms, attributes, length(body)
                 FROM objects ORDER BY key",
            )
            .unwrap()
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        let plan = plan.join("\n").to_ascii_lowercase();
        assert!(!plan.contains("covering"), "{plan}");
    }

    #[test]
    fn list_returns_only_whole_segment_matches_with_sizes() {
        let (_dir, store) = seeded();
        let listed: Vec<ObjectMeta> = runtime().block_on(async {
            use futures_util::TryStreamExt as _;
            store
                .list(Some(&Path::from("nodes")))
                .try_collect()
                .await
                .unwrap()
        });
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].location.as_ref(), "nodes/dev-1.json");
        assert_eq!(listed[0].size, 3);

        let cells: Vec<ObjectMeta> = runtime().block_on(async {
            use futures_util::TryStreamExt as _;
            store
                .list(Some(&Path::from("cells/user")))
                .try_collect()
                .await
                .unwrap()
        });
        assert_eq!(cells.len(), 200);
        assert!(cells
            .iter()
            .enumerate()
            .all(|(i, meta)| meta.size == 64 + i as u64));

        let all: Vec<ObjectMeta> = runtime().block_on(async {
            use futures_util::TryStreamExt as _;
            store.list(None).try_collect().await.unwrap()
        });
        assert_eq!(all.len(), 402);
    }

    #[test]
    fn list_with_delimiter_uses_the_segment_range() {
        let (_dir, store) = seeded();
        let result = runtime()
            .block_on(store.list_with_delimiter(Some(&Path::from("cells/user/ltx"))))
            .unwrap();
        assert!(result.objects.is_empty());
        assert_eq!(
            result.common_prefixes,
            vec![Path::from("cells/user/ltx/e1")]
        );
    }

    #[test]
    fn paginated_list_pushes_offset_and_limit_into_sql() {
        let (_dir, store) = seeded();
        let first = runtime()
            .block_on(store.list_paginated(
                Some("other/"),
                PaginatedListOptions {
                    max_keys: Some(10),
                    ..PaginatedListOptions::default()
                },
            ))
            .unwrap();
        assert_eq!(first.result.objects.len(), 10);
        assert_eq!(first.page_token.as_deref(), Some("other/0009"));
        let second = runtime()
            .block_on(store.list_paginated(
                Some("other/"),
                PaginatedListOptions {
                    max_keys: Some(10),
                    page_token: first.page_token.clone(),
                    ..PaginatedListOptions::default()
                },
            ))
            .unwrap();
        assert_eq!(second.result.objects[0].location.as_ref(), "other/0010");
        let last = runtime()
            .block_on(store.list_paginated(
                Some("other/"),
                PaginatedListOptions {
                    max_keys: Some(10),
                    page_token: Some("other/0194".into()),
                    ..PaginatedListOptions::default()
                },
            ))
            .unwrap();
        assert_eq!(last.result.objects.len(), 5);
        assert_eq!(last.page_token, None);

        let plan = plan(
            &store,
            ListQuery {
                prefix: "other/",
                after: Some("other/0009"),
                limit: Some(11),
            },
        );
        assert!(plan.contains("covering index objects_list"), "{plan}");
    }

    #[test]
    fn writes_keep_body_len_in_step_with_the_body() {
        let dir = TempDir::new().unwrap();
        let store = LocalStore::open(dir.path().join("objects.sqlite3")).unwrap();
        put(&store, "a", 10);
        put(&store, "a", 3);
        store.copy_sync("a", "b", true).unwrap();
        let connection = store.connect().unwrap();
        let rows: Vec<(String, i64, i64)> = connection
            .prepare("SELECT key, body_len, length(body) FROM objects ORDER BY key")
            .unwrap()
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(rows, vec![("a".into(), 3, 3), ("b".into(), 3, 3)]);
    }

    /// The table as the released binary creates it: no `body_len`.
    fn legacy_store(path: &FsPath, rows: usize) {
        let connection = Connection::open(path).unwrap();
        connection
            .execute_batch(
                "PRAGMA journal_mode = WAL;
                 CREATE TABLE objects (
                   key TEXT PRIMARY KEY,
                   body BLOB NOT NULL,
                   etag INTEGER NOT NULL,
                   modified_ms INTEGER NOT NULL,
                   attributes TEXT NOT NULL
                 );
                 CREATE TABLE store_sequence (
                   singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                   next_etag INTEGER NOT NULL
                 );
                 INSERT INTO store_sequence VALUES (1, 1);",
            )
            .unwrap();
        for i in 0..rows {
            connection
                .execute(
                    "INSERT INTO objects VALUES (?1, ?2, ?3, 0, '[]')",
                    params![format!("k/{i:05}"), vec![7_u8; i % 9000], i as i64],
                )
                .unwrap();
        }
    }

    fn user_version(store: &LocalStore) -> i64 {
        store
            .connect()
            .unwrap()
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap()
    }

    fn wrong_sizes(store: &LocalStore) -> i64 {
        store
            .connect()
            .unwrap()
            .query_row(
                "SELECT count(*) FROM objects WHERE body_len <> length(body)",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }

    #[test]
    fn opening_a_legacy_store_adds_and_backfills_body_len_once() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("objects.sqlite3");
        // More rows than one backfill batch, so batching is exercised.
        legacy_store(&path, BODY_LEN_BACKFILL_BATCH as usize * 2 + 17);
        let store = LocalStore::open(&path).unwrap();
        assert_eq!(wrong_sizes(&store), 0);
        assert_eq!(user_version(&store), OBJECTS_SCHEMA_VERSION);

        // A later open does not rescan: a row made wrong behind the
        // store's back stays wrong, which proves the backfill ran once.
        store
            .connect()
            .unwrap()
            .execute("UPDATE objects SET body_len = 0 WHERE key = 'k/00005'", [])
            .unwrap();
        let store = LocalStore::open(&path).unwrap();
        assert_eq!(wrong_sizes(&store), 1);
    }

    #[test]
    fn a_store_with_body_len_but_unset_rows_is_backfilled() {
        // The live dev store: the column and index exist, but every row
        // written by a binary that did not set them has body_len = 0.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("objects.sqlite3");
        legacy_store(&path, 500);
        Connection::open(&path)
            .unwrap()
            .execute_batch(
                "ALTER TABLE objects ADD COLUMN body_len INTEGER NOT NULL DEFAULT 0;
                 CREATE INDEX objects_list ON objects(key, etag, modified_ms, attributes, body_len);
                 UPDATE objects SET body_len = length(body) WHERE rowid <= 100;",
            )
            .unwrap();
        let store = LocalStore::open(&path).unwrap();
        assert_eq!(wrong_sizes(&store), 0);
        assert_eq!(user_version(&store), OBJECTS_SCHEMA_VERSION);
        let listed = store
            .metadata(ListQuery {
                prefix: "k/",
                ..ListQuery::default()
            })
            .unwrap();
        assert!(listed.iter().all(|object| object.size == {
            let i: usize = object.key[2..].parse().unwrap();
            (i % 9000) as u64
        }));
    }

    #[test]
    fn key_prefix_upper_steps_the_last_code_point() {
        assert_eq!(key_prefix_upper("cells/").as_deref(), Some("cells0"));
        assert_eq!(key_prefix_upper("").as_deref(), None);
        // A multi-byte last character: the bound must stay valid UTF-8 and
        // still sort after every key with the prefix.
        let upper = key_prefix_upper("p\u{159}").unwrap();
        assert_eq!(upper, "p\u{15a}");
        assert!("p\u{159}\u{10FFFF}zzz" < upper.as_str());
        assert_eq!(key_prefix_upper("a\u{D7FF}").as_deref(), Some("a\u{E000}"));
        assert_eq!(key_prefix_upper("a\u{10FFFF}").as_deref(), Some("b"));
        assert_eq!(key_prefix_upper("\u{10FFFF}"), None);
    }

    #[test]
    fn non_ascii_prefixes_list_exactly_their_keys() {
        let dir = TempDir::new().unwrap();
        let store = LocalStore::open(dir.path().join("objects.sqlite3")).unwrap();
        for key in ["p\u{159}/a", "p\u{159}/b", "p\u{15a}/c", "p\u{158}/d"] {
            put(&store, key, 1);
        }
        let listed = store
            .metadata(ListQuery {
                prefix: "p\u{159}/",
                ..ListQuery::default()
            })
            .unwrap();
        let keys: Vec<_> = listed.iter().map(|object| object.key.as_str()).collect();
        assert_eq!(keys, vec!["p\u{159}/a", "p\u{159}/b"]);
    }
}
