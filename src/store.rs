//! The store: corestore-level rows, and per-core read/write transactions.
//!
//! The structure follows npm `hypercore-storage`'s `CorestoreStorage`,
//! `HypercoreStorage`, `CoreTX` and `CoreRX`; the naming is snake_case.

use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, RwLockReadGuard};

use bytes::Bytes;
use compact_encoding::hypercore::Node;
use rocksdb::{
    BottommostLevelCompaction, ColumnFamily, CompactOptions, DB, IteratorMode, ReadOptions,
    WriteBatch,
};

use crate::db::{self, COLUMN_FAMILY};
use crate::keys;
use crate::rows::{
    Alias, Auth, Core, CoreGroup, CoreHead, Dependency, GroupRow, Hints, Info, Session, StoreHead,
};
use crate::{Error, Result};

/// Version this crate writes in `@corestore/core` and `@corestore/head`.
pub const STORE_VERSION: u64 = 2;

/// The deepest dependency chain `resume_core` follows before giving up.
const MAX_DEPENDENCIES: usize = 64;

const TREE_LOCKS: usize = 32;

fn tree_lock(pointer: u64) -> usize {
    (pointer % TREE_LOCKS as u64) as usize
}

/// A directory of cores in one RocksDB, in hypercore-storage's layout.
#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
}

struct Inner {
    /// An `Arc` so a compaction can drop the lock while it rewrites files, instead of
    /// blocking every read that needs the same handle.
    db: Mutex<Option<Arc<DB>>>,
    read_only: bool,
    closed: AtomicBool,
    closed_rx: tokio::sync::watch::Sender<bool>,
    /// One writer at a time: creating a core reads and bumps the store head.
    write: Mutex<()>,
    compact: Mutex<()>,
    tree_cache: Mutex<crate::tree_cache::TreeCache>,
    tree_io: [RwLock<()>; TREE_LOCKS],
}

impl Store {
    /// Opens (or creates) the store in `<dir>/db`, as JS keeps it.
    pub fn open(dir: &Path) -> Result<Store> {
        std::fs::create_dir_all(dir)?;
        Self::from_db(db::open(&dir.join("db"), false)?, false)
    }

    pub fn open_read_only(dir: &Path) -> Result<Store> {
        Self::from_db(db::open(&dir.join("db"), true)?, true)
    }

    fn from_db(db: DB, read_only: bool) -> Result<Store> {
        Ok(Store {
            inner: Arc::new(Inner {
                db: Mutex::new(Some(Arc::new(db))),
                read_only,
                closed: AtomicBool::new(false),
                closed_rx: tokio::sync::watch::channel(false).0,
                write: Mutex::new(()),
                compact: Mutex::new(()),
                tree_cache: Mutex::new(Default::default()),
                tree_io: std::array::from_fn(|_| RwLock::new(())),
            }),
        })
    }

    pub fn read_only(&self) -> bool {
        self.inner.read_only
    }

    /// Whether [`Store::close`] has run.
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire) || self.lock_db().is_none()
    }

    pub fn close(&self) {
        self.inner.closed.store(true, Ordering::Release);
        self.lock_db().take();
        let _ = self.inner.closed_rx.send(true);
    }

    /// A receiver that flips to `true` once [`Store::close`] has run.
    pub fn closed(&self) -> tokio::sync::watch::Receiver<bool> {
        self.inner.closed_rx.subscribe()
    }

    /// Flushes the memtable, as JS `storage.flush()` does.
    pub fn flush(&self) -> Result<()> {
        self.with_db(|db, cf| db.flush_cf(cf).map_err(|e| Error::Rocks(e.to_string())))
    }

    /// Rewrites the SSTs and `.blob` files now, as JS `coreStorage.compact(options)` does, so
    /// rows tombstoned by `delete_block_range` leave the disk instead of waiting for a
    /// size-triggered compaction.
    ///
    /// `relocate_blobs` matches JS's exclusive full-wipe compaction with forced blob GC.
    /// Streaming/ceiling compaction is non-exclusive and never relocates live blobs.
    pub fn compact(&self, relocate_blobs: bool) -> Result<()> {
        if self.inner.read_only {
            return Err(Error::ReadOnly);
        }
        let _compact = self.inner.compact.lock().unwrap_or_else(|e| e.into_inner());
        let db = self.handle()?;
        let cf = db
            .cf_handle(COLUMN_FAMILY)
            .ok_or_else(|| Error::Corrupt("missing corestore column family".into()))?;
        // shortcut: CF-wide GC override until the Rust binding exposes per-compaction GC options.
        db.set_options_cf(
            &cf,
            &[
                (
                    "enable_blob_garbage_collection",
                    if relocate_blobs { "true" } else { "false" },
                ),
                (
                    "blob_garbage_collection_age_cutoff",
                    if relocate_blobs { "1.0" } else { "0.25" },
                ),
            ],
        )
        .map_err(|e| Error::Rocks(e.to_string()))?;
        let mut options = CompactOptions::default();
        options.set_exclusive_manual_compaction(relocate_blobs);
        options.set_bottommost_level_compaction(if relocate_blobs {
            BottommostLevelCompaction::Force
        } else {
            BottommostLevelCompaction::Skip
        });
        db.compact_range_cf_opt(&cf, None::<&[u8]>, None::<&[u8]>, &options);
        db.set_options_cf(
            &cf,
            &[
                ("enable_blob_garbage_collection", "true"),
                ("blob_garbage_collection_age_cutoff", "0.25"),
            ],
        )
        .map_err(|e| Error::Rocks(e.to_string()))?;
        Ok(())
    }

    fn lock_db(&self) -> MutexGuard<'_, Option<Arc<DB>>> {
        self.inner
            .db
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn handle(&self) -> Result<Arc<DB>> {
        self.lock_db().as_ref().cloned().ok_or(Error::Closed)
    }

    fn tree_cache(&self) -> MutexGuard<'_, crate::tree_cache::TreeCache> {
        self.inner
            .tree_cache
            .lock()
            .unwrap_or_else(|poison| poison.into_inner())
    }

    fn with_db<T>(&self, f: impl FnOnce(&DB, &ColumnFamily) -> Result<T>) -> Result<T> {
        let db = self.handle()?;
        let cf = db
            .cf_handle(COLUMN_FAMILY)
            .ok_or_else(|| Error::Corrupt("missing corestore column family".into()))?;
        f(&db, cf)
    }

    /// Reads one row.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.with_db(|db, cf| {
            db.get_cf(cf, key)
                .map(|value| value.map(Bytes::from))
                .map_err(|e| Error::Rocks(e.to_string()))
        })
    }

    /// Reads a `[start, end)` key range in ascending order.
    pub fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.with_db(|db, cf| {
            let mut options = ReadOptions::default();
            options.set_iterate_lower_bound(start.to_vec());
            options.set_iterate_upper_bound(end.to_vec());
            let mut out = Vec::new();
            for item in db.iterator_cf_opt(cf, options, IteratorMode::Start) {
                let (key, value) = item.map_err(|e| Error::Rocks(e.to_string()))?;
                out.push((key.to_vec(), Bytes::from(value.to_vec())));
            }
            Ok(out)
        })
    }

    /// Reads a whole tag/pointer range in descending order (group updates).
    pub fn range_reverse(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Bytes)>> {
        self.with_db(|db, cf| {
            let mut options = ReadOptions::default();
            options.set_iterate_lower_bound(start.to_vec());
            options.set_iterate_upper_bound(end.to_vec());
            let mut out = Vec::new();
            for item in db.iterator_cf_opt(cf, options, IteratorMode::End) {
                let (key, value) = item.map_err(|e| Error::Rocks(e.to_string()))?;
                out.push((key.to_vec(), Bytes::from(value.to_vec())));
            }
            Ok(out)
        })
    }

    /// Applies a plan of row operations in one atomic batch.
    fn apply_ops(&self, ops: Vec<Op>) -> Result<()> {
        if ops.is_empty() {
            return Ok(());
        }
        if self.inner.read_only {
            return Err(Error::ReadOnly);
        }
        self.with_db(|db, cf| {
            let mut batch = WriteBatch::default();
            for op in &ops {
                match op {
                    Op::Put(key, value) => batch.put_cf(cf, key, value),
                    Op::PutTree(pointer, node) => batch.put_cf(
                        cf,
                        keys::data_tree(*pointer, node.index),
                        crate::rows::encode_tree_node(node),
                    ),
                    Op::Delete(key) => batch.delete_cf(cf, key),
                    Op::DeleteRange(start, end) => batch.delete_range_cf(cf, start, end),
                }
            }
            // Different data pointers may commit concurrently; cache fills must follow their writes.
            let mask = ops.iter().fold(0u32, |mask, op| match op {
                Op::PutTree(pointer, _) => mask | (1 << tree_lock(*pointer)),
                Op::Delete(_) | Op::DeleteRange(_, _) => u32::MAX,
                Op::Put(_, _) => mask,
            });
            let _writes: [_; TREE_LOCKS] = std::array::from_fn(|i| {
                (mask & (1 << i) != 0).then(|| {
                    self.inner.tree_io[i]
                        .write()
                        .unwrap_or_else(|e| e.into_inner())
                })
            });
            // JS writes with default options: atomic, WAL on, no per-batch fsync.
            db.write(batch).map_err(|e| Error::Rocks(e.to_string()))?;
            let mut cache = self.tree_cache();
            for op in ops {
                match op {
                    Op::PutTree(pointer, node) => cache.put(pointer, node),
                    Op::Put(_, _) => {}
                    Op::Delete(_) | Op::DeleteRange(_, _) => cache.clear(),
                }
            }
            Ok(())
        })
    }

    /// Runs a read-modify-write of store-level rows under the write lock.
    ///
    /// The closure may read (each read takes the database lock briefly) and
    /// returns the rows to write; the writes land as one batch afterwards.
    fn write_with<T>(&self, f: impl FnOnce(&Store) -> Result<(Vec<Op>, T)>) -> Result<T> {
        if self.inner.read_only {
            return Err(Error::ReadOnly);
        }
        let _writer = self
            .inner
            .write
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let (ops, out) = f(self)?;
        self.apply_ops(ops)?;
        Ok(out)
    }

    // ---- store head, seed, default discovery key -------------------------

    /// The store head, with zeroed counters when the store is new.
    pub fn head(&self) -> Result<StoreHead> {
        match self.get(&keys::store_head())? {
            Some(bytes) => StoreHead::decode(bytes),
            None => Ok(StoreHead::default()),
        }
    }

    /// Reads or creates the corestore primary key (`head.seed`), like
    /// `Corestore._getOrSetSeed`.
    pub fn seed(&self) -> Result<Option<[u8; 32]>> {
        Ok(self.head()?.seed)
    }

    /// Stores the primary key unless one is already there; returns the stored key.
    pub fn set_seed(&self, seed: [u8; 32]) -> Result<[u8; 32]> {
        self.write_with(|store| {
            let mut head = store.head()?;
            if head.seed.is_some() {
                return Ok((Vec::new(), head.seed.unwrap_or(seed)));
            }
            head.seed = Some(seed);
            Ok((vec![Op::Put(keys::store_head(), head.encode())], seed))
        })
    }

    pub fn default_discovery_key(&self) -> Result<Option<[u8; 32]>> {
        Ok(self.head()?.default_discovery_key)
    }

    pub fn set_default_discovery_key(&self, discovery_key: [u8; 32]) -> Result<[u8; 32]> {
        self.write_with(|store| {
            let mut head = store.head()?;
            if head.default_discovery_key.is_some() {
                return Ok((Vec::new(), discovery_key));
            }
            head.default_discovery_key = Some(discovery_key);
            Ok((
                vec![Op::Put(keys::store_head(), head.encode())],
                discovery_key,
            ))
        })
    }

    // ---- cores ------------------------------------------------------------

    /// A core row by discovery key.
    pub fn get_core(&self, discovery_key: &[u8; 32]) -> Result<Option<Core>> {
        match self.get(&keys::core_by_discovery_key(discovery_key))? {
            Some(bytes) => Ok(Some(Core::decode(bytes)?)),
            None => Ok(None),
        }
    }

    pub fn has_core(&self, discovery_key: &[u8; 32]) -> Result<bool> {
        Ok(self.get_core(discovery_key)?.is_some())
    }

    /// Every discovery key with a core row.
    pub fn cores(&self) -> Result<Vec<[u8; 32]>> {
        let start = keys::core_by_discovery_key_start();
        let end = keys::core_by_discovery_key_end();
        let mut out = Vec::new();
        for (key, _) in self.range(&start, &end)? {
            if let Some(discovery_key) = keys::read_alias_discovery_key(&key) {
                out.push(discovery_key);
            }
        }
        Ok(out)
    }

    pub fn get_auth(&self, discovery_key: &[u8; 32]) -> Result<Option<Auth>> {
        let Some(core) = self.get_core(discovery_key)? else {
            return Ok(None);
        };
        self.auth_at(core.core_pointer)
    }

    fn auth_at(&self, core_pointer: u64) -> Result<Option<Auth>> {
        match self.get(&keys::core_auth(core_pointer))? {
            Some(bytes) => Ok(Some(Auth::decode(bytes)?)),
            None => Ok(None),
        }
    }

    /// Creates a core (or resumes the existing one) and returns its handle.
    pub fn create_core(&self, options: CreateCore) -> Result<CoreStorage> {
        if let Some(core) = self.get_core(&options.discovery_key)? {
            return self.core_from(core);
        }
        let core = self.write_with(|store| {
            let mut head = store.head()?;
            if head.default_discovery_key.is_none() {
                head.default_discovery_key = Some(options.discovery_key);
            }
            let (core_pointer, data_pointer) = match options.pointers {
                Some(pointers) => pointers,
                None => {
                    let pointers = (head.cores, head.datas);
                    head.cores += 1;
                    head.datas += 1;
                    pointers
                }
            };
            let core = Core {
                version: STORE_VERSION,
                core_pointer,
                data_pointer,
                alias: options.alias.clone(),
            };
            let auth = Auth {
                key: options.key,
                discovery_key: options.discovery_key,
                manifest: options.manifest.clone(),
                key_pair: options.key_pair.clone(),
                encryption_key: options.encryption_key.clone(),
            };
            let mut ops = vec![
                Op::Put(keys::store_head(), head.encode()),
                Op::Put(
                    keys::core_by_discovery_key(&options.discovery_key),
                    core.encode(),
                ),
                Op::Put(keys::core_auth(core_pointer), auth.encode()),
            ];
            if let Some(alias) = &core.alias {
                ops.push(Op::Put(
                    keys::core_by_alias(&alias.namespace, &alias.name),
                    options.discovery_key.to_vec().into(),
                ));
            }
            for (name, value) in &options.user_data {
                ops.push(Op::Put(
                    keys::data_user_data(data_pointer, name),
                    value.clone(),
                ));
            }
            Ok((ops, core))
        })?;
        self.core_from(core)
    }

    /// A handle for an existing core, following its dependency chain.
    pub fn resume_core(&self, discovery_key: &[u8; 32]) -> Result<Option<CoreStorage>> {
        match self.get_core(discovery_key)? {
            Some(core) => Ok(Some(self.core_from(core)?)),
            None => Ok(None),
        }
    }

    fn core_from(&self, core: Core) -> Result<CoreStorage> {
        let mut dependencies = Vec::new();
        let mut data_pointer = core.data_pointer;
        for _ in 0..MAX_DEPENDENCIES {
            let Some(bytes) = self.get(&keys::data_dependency(data_pointer))? else {
                break;
            };
            let dependency = Dependency::decode(bytes)?;
            dependencies.push(dependency);
            data_pointer = dependency.data_pointer;
        }
        Ok(CoreStorage {
            store: self.clone(),
            core_pointer: core.core_pointer,
            data_pointer: core.data_pointer,
            dependencies,
        })
    }

    /// Removes a core: its row, alias, all `TL_CORE` rows and every data pointer.
    pub fn delete_core(&self, discovery_key: &[u8; 32]) -> Result<()> {
        if self.get_core(discovery_key)?.is_none() {
            return Ok(());
        }
        self.write_with(|store| {
            let Some(core) = store.get_core(discovery_key)? else {
                return Ok((Vec::new(), ()));
            };
            let sessions = store.sessions_at(core.core_pointer)?;
            let aliased = match &core.alias {
                Some(alias) => store
                    .get_alias(alias)?
                    .is_some_and(|current| current == *discovery_key),
                None => false,
            };
            let mut ops = vec![Op::Delete(keys::core_by_discovery_key(discovery_key))];
            if aliased && let Some(alias) = &core.alias {
                ops.push(Op::Delete(keys::core_by_alias(
                    &alias.namespace,
                    &alias.name,
                )));
            }
            ops.push(Op::DeleteRange(
                keys::core_range_start(core.core_pointer),
                keys::core_range_end(core.core_pointer),
            ));
            ops.push(Op::DeleteRange(
                keys::data_range_start(core.data_pointer),
                keys::data_range_end(core.data_pointer),
            ));
            for session in sessions {
                ops.push(Op::DeleteRange(
                    keys::data_range_start(session.data_pointer),
                    keys::data_range_end(session.data_pointer),
                ));
            }
            Ok((ops, ()))
        })
    }

    /// Auth, head and hints for many discovery keys (`getInfos`).
    pub fn get_infos(
        &self,
        discovery_keys: &[[u8; 32]],
        options: InfoOptions,
    ) -> Result<Vec<Option<Info>>> {
        let mut out = Vec::with_capacity(discovery_keys.len());
        for discovery_key in discovery_keys {
            let Some(core) = self.get_core(discovery_key)? else {
                out.push(None);
                continue;
            };
            let auth = if options.auth {
                self.auth_at(core.core_pointer)?
            } else {
                None
            };
            let head = if options.head {
                self.get(&keys::data_head(core.data_pointer))?
                    .map(CoreHead::decode)
                    .transpose()?
            } else {
                None
            };
            let hints = if options.hints {
                self.get(&keys::data_hints(core.data_pointer))?
                    .map(Hints::decode)
                    .transpose()?
            } else {
                None
            };
            out.push(Some(Info {
                discovery_key: *discovery_key,
                core,
                auth,
                head,
                hints,
            }));
        }
        Ok(out)
    }

    fn sessions_at(&self, core_pointer: u64) -> Result<Vec<Session>> {
        match self.get(&keys::core_sessions(core_pointer))? {
            Some(bytes) => Session::decode_list(bytes),
            None => Ok(Vec::new()),
        }
    }

    // ---- aliases ----------------------------------------------------------

    /// `store.getAlias(alias)`; the value is a discovery key.
    pub fn get_alias(&self, alias: &Alias) -> Result<Option<[u8; 32]>> {
        let value = self.get(&keys::core_by_alias(&alias.namespace, &alias.name))?;
        match value {
            Some(value) => {
                Ok(Some(value[..].try_into().map_err(|_| {
                    Error::Corrupt("alias row is not a discovery key".into())
                })?))
            }
            None => Ok(None),
        }
    }

    pub fn set_alias(&self, alias: &Alias, discovery_key: &[u8; 32]) -> Result<()> {
        if self.inner.read_only {
            return Err(Error::ReadOnly);
        }
        self.apply_ops(vec![Op::Put(
            keys::core_by_alias(&alias.namespace, &alias.name),
            discovery_key.to_vec().into(),
        )])
    }

    /// Deletes the alias only when it still points at `discovery_key`.
    pub fn delete_alias(&self, alias: &Alias, discovery_key: &[u8; 32]) -> Result<bool> {
        if self.get_alias(alias)? != Some(*discovery_key) {
            return Ok(false);
        }
        if self.inner.read_only {
            return Err(Error::ReadOnly);
        }
        self.apply_ops(vec![Op::Delete(keys::core_by_alias(
            &alias.namespace,
            &alias.name,
        ))])?;
        Ok(true)
    }

    /// Every alias row, ordered by key.
    pub fn aliases(&self) -> Result<Vec<(Alias, [u8; 32])>> {
        let start = keys::core_by_alias_start();
        let end = keys::core_by_alias_end();
        let mut out = Vec::new();
        for (key, value) in self.range(&start, &end)? {
            let Some((namespace, name)) = keys::read_alias(&key) else {
                continue;
            };
            let discovery_key: [u8; 32] = value[..]
                .try_into()
                .map_err(|_| Error::Corrupt("alias row is not a discovery key".into()))?;
            out.push((Alias { namespace, name }, discovery_key));
        }
        Ok(out)
    }

    // ---- groups -----------------------------------------------------------

    /// `store.createGroup(topic)`: allocates the group on first use.
    pub fn create_group(&self, topic: &[u8; 32]) -> Result<u64> {
        if let Some(group) = self.get_group(topic)? {
            return Ok(group);
        }
        self.write_with(|store| {
            let mut head = store.head()?;
            let group = head.groups;
            head.groups += 1;
            let mut value = Vec::new();
            keys::uint(&mut value, group);
            Ok((
                vec![
                    Op::Put(keys::store_head(), head.encode()),
                    Op::Put(keys::group_topic(topic), value.into()),
                ],
                group,
            ))
        })
    }

    pub fn get_group(&self, topic: &[u8; 32]) -> Result<Option<u64>> {
        match self.get(&keys::group_topic(topic))? {
            Some(value) => {
                let mut pos = 0;
                Ok(keys::read_uint(&value, &mut pos))
            }
            None => Ok(None),
        }
    }

    /// Group updates at or after `since`, descending (JS's stream default).
    pub fn group_updates(&self, group: u64, since: u64) -> Result<Vec<GroupRow>> {
        let start = keys::group_entry(group, since, 0);
        let end = keys::group_entry_end(group);
        let mut out = Vec::new();
        for (row, value) in self.range_reverse(&start, &end)? {
            let Some((timestamp, core_pointer)) = keys::read_group_entry(&row) else {
                continue;
            };
            if let Ok(key) = <[u8; 32]>::try_from(value.as_ref()) {
                out.push(GroupRow {
                    timestamp,
                    core_pointer,
                    key,
                });
            }
        }
        Ok(out)
    }

    /// Records a group update (`putGroupUpdate`).
    pub fn put_group_update(
        &self,
        group: u64,
        timestamp: u64,
        core_pointer: u64,
        key: &[u8; 32],
    ) -> Result<()> {
        if self.inner.read_only {
            return Err(Error::ReadOnly);
        }
        self.apply_ops(vec![Op::Put(
            keys::group_entry(group, timestamp, core_pointer),
            Bytes::copy_from_slice(&key[..]),
        )])
    }

    /// Deletes one group update (`deleteGroupUpdate`).
    pub fn delete_group_update(&self, group: u64, timestamp: u64, core_pointer: u64) -> Result<()> {
        if self.inner.read_only {
            return Err(Error::ReadOnly);
        }
        self.apply_ops(vec![Op::Delete(keys::group_entry(
            group,
            timestamp,
            core_pointer,
        ))])
    }

    /// Removes every row (`store.clear()`).
    pub fn clear(&self) -> Result<()> {
        self.write_with(|_| {
            Ok((
                vec![Op::DeleteRange(
                    keys::uint_key(0),
                    keys::uint_key(keys::TL_END),
                )],
                (),
            ))
        })
    }
}

/// What to store when creating a core.
#[derive(Clone, Default)]
pub struct CreateCore {
    pub key: [u8; 32],
    pub discovery_key: [u8; 32],
    pub manifest: Option<compact_encoding::hypercore::Manifest>,
    pub key_pair: Option<crate::rows::KeyPair>,
    pub encryption_key: Option<Bytes>,
    pub alias: Option<Alias>,
    pub user_data: Vec<(String, Bytes)>,
    /// Explicit `(corePointer, dataPointer)` (JS `createCore({ core })`).
    pub pointers: Option<(u64, u64)>,
}

/// Which parts of a core `get_infos` should load.
#[derive(Debug, Clone, Copy)]
pub struct InfoOptions {
    pub auth: bool,
    pub head: bool,
    pub hints: bool,
}

impl Default for InfoOptions {
    fn default() -> Self {
        Self {
            auth: true,
            head: true,
            hints: true,
        }
    }
}

/// One core plus its current data pointer and dependency chain.
#[derive(Clone)]
pub struct CoreStorage {
    store: Store,
    core_pointer: u64,
    data_pointer: u64,
    dependencies: Vec<Dependency>,
}

impl CoreStorage {
    pub fn store(&self) -> &Store {
        &self.store
    }

    pub fn core_pointer(&self) -> u64 {
        self.core_pointer
    }

    pub fn data_pointer(&self) -> u64 {
        self.data_pointer
    }

    pub fn dependencies(&self) -> &[Dependency] {
        &self.dependencies
    }

    // ---- reads ------------------------------------------------------------

    pub fn get_auth(&self) -> Result<Option<Auth>> {
        self.store.auth_at(self.core_pointer)
    }

    pub fn get_sessions(&self) -> Result<Vec<Session>> {
        self.store.sessions_at(self.core_pointer)
    }

    pub fn get_group(&self) -> Result<Option<CoreGroup>> {
        match self.store.get(&keys::core_group(self.core_pointer))? {
            Some(bytes) => Ok(Some(CoreGroup::decode(bytes)?)),
            None => Ok(None),
        }
    }

    pub fn get_head(&self) -> Result<Option<CoreHead>> {
        match self.store.get(&keys::data_head(self.data_pointer))? {
            Some(bytes) => Ok(Some(CoreHead::decode(bytes)?)),
            None => Ok(None),
        }
    }

    pub fn get_dependency(&self) -> Result<Option<Dependency>> {
        match self.store.get(&keys::data_dependency(self.data_pointer))? {
            Some(bytes) => Ok(Some(Dependency::decode(bytes)?)),
            None => Ok(None),
        }
    }

    pub fn get_hints(&self) -> Result<Option<Hints>> {
        match self.store.get(&keys::data_hints(self.data_pointer))? {
            Some(bytes) => Ok(Some(Hints::decode(bytes)?)),
            None => Ok(None),
        }
    }

    /// Block `index`, looking in the oldest dependency that covers it first.
    pub fn get_block(&self, index: u64) -> Result<Option<Bytes>> {
        let pointer = self
            .dependencies
            .iter()
            .find(|dependency| index < dependency.length)
            .map_or(self.data_pointer, |dependency| dependency.data_pointer);
        self.store.get(&keys::data_block(pointer, index))
    }

    pub fn get_bitfield_page(&self, index: u64) -> Result<Option<Bytes>> {
        self.store
            .get(&keys::data_bitfield(self.data_pointer, index, 0))
    }

    /// Tree node `index`, looking in the oldest dependency that covers it first.
    pub fn get_tree_node(&self, index: u64) -> Result<Option<Node>> {
        self.store.with_db(|db, cf| {
            let pointer = self.tree_pointer(index);
            let _read = self.store.inner.tree_io[tree_lock(pointer)]
                .read()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(node) = self.store.tree_cache().get((pointer, index)) {
                return Ok(Some(node));
            }
            let node = db
                .get_cf(cf, keys::data_tree(pointer, index))
                .map_err(|e| Error::Rocks(e.to_string()))?
                .map(|bytes| crate::rows::decode_tree_node(Bytes::from(bytes)))
                .transpose()?;
            if let Some(node) = node {
                self.store.tree_cache().put(pointer, node);
            }
            Ok(node)
        })
    }

    fn tree_pointer(&self, index: u64) -> u64 {
        self.dependencies
            .iter()
            .find(|dependency| {
                crate::flat::right_span(index) <= dependency.length.saturating_sub(1) * 2
            })
            .map_or(self.data_pointer, |dependency| dependency.data_pointer)
    }

    fn tree_reads(&self) -> [Option<RwLockReadGuard<'_, ()>>; TREE_LOCKS] {
        let mut mask = 1u32 << tree_lock(self.data_pointer);
        for dependency in &self.dependencies {
            mask |= 1 << tree_lock(dependency.data_pointer);
        }
        std::array::from_fn(|i| {
            (mask & (1 << i) != 0).then(|| {
                self.store.inner.tree_io[i]
                    .read()
                    .unwrap_or_else(|e| e.into_inner())
            })
        })
    }

    /// Ordered, dependency-aware tree reads in one RocksDB multi-get; cached nodes need no I/O.
    pub fn get_tree_nodes(&self, indices: &[u64]) -> Result<Vec<Option<Node>>> {
        self.store.with_db(|db, cf| {
            let _reads = self.tree_reads();
            let mut cache = self.store.tree_cache();
            let mut out = Vec::with_capacity(indices.len());
            let mut missing = Vec::new();
            let mut row_keys = Vec::new();
            for (slot, &index) in indices.iter().enumerate() {
                let pointer = self.tree_pointer(index);
                let node = cache.get((pointer, index));
                out.push(node);
                if node.is_none() {
                    missing.push((slot, pointer));
                    row_keys.push(keys::data_tree(pointer, index));
                }
            }
            drop(cache);
            if !missing.is_empty() {
                let values = if row_keys.len() == 1 {
                    vec![db.get_cf(cf, &row_keys[0])]
                } else {
                    db.multi_get_cf(row_keys.iter().map(|key| (cf, key)))
                };
                let mut cache = self.store.tree_cache();
                for ((slot, pointer), value) in missing.into_iter().zip(values) {
                    if let Some(bytes) = value.map_err(|e| Error::Rocks(e.to_string()))? {
                        let node = crate::rows::decode_tree_node(Bytes::from(bytes))?;
                        cache.put(pointer, node);
                        out[slot] = Some(node);
                    }
                }
            }
            Ok(out)
        })
    }

    /// Missing prefix of an ancestor path, stopping at the first cached or persisted node.
    pub fn missing_tree_nodes(&self, indices: impl IntoIterator<Item = u64>) -> Result<u64> {
        self.store.with_db(|db, cf| {
            let _reads = self.tree_reads();
            let mut cache = self.store.tree_cache();
            let mut row_keys = Vec::new();
            for index in indices {
                if cache.get((self.tree_pointer(index), index)).is_some() {
                    break;
                }
                row_keys.push(keys::data_tree(self.tree_pointer(index), index));
            }
            drop(cache);
            if row_keys.is_empty() {
                return Ok(0);
            }
            let values = if row_keys.len() == 1 {
                vec![db.get_cf(cf, &row_keys[0])]
            } else {
                db.multi_get_cf(row_keys.iter().map(|key| (cf, key)))
            };
            for (slot, value) in values.into_iter().enumerate() {
                if let Some(bytes) = value.map_err(|e| Error::Rocks(e.to_string()))? {
                    let node = crate::rows::decode_tree_node(Bytes::from(bytes))?;
                    self.store
                        .tree_cache()
                        .put(self.tree_pointer(node.index), node);
                    return Ok(slot as u64);
                }
            }
            Ok(row_keys.len() as u64)
        })
    }

    pub fn get_user_data(&self, key: &str) -> Result<Option<Bytes>> {
        self.store
            .get(&keys::data_user_data(self.data_pointer, key))
    }

    pub fn get_local(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.store.get(&keys::data_local(self.data_pointer, key))
    }

    pub fn get_mark(&self, index: u64) -> Result<Option<Bytes>> {
        self.store.get(&keys::data_mark(self.data_pointer, index))
    }

    /// Every stored block, ascending.
    pub fn blocks(&self) -> Result<Vec<(u64, Bytes)>> {
        let start = keys::data_block_start(self.data_pointer);
        let end = keys::data_tree_start(self.data_pointer);
        let mut out = Vec::new();
        for (key, value) in self.store.range(&start, &end)? {
            if let Some(index) = keys::read_block_index(&key) {
                out.push((index, value));
            }
        }
        Ok(out)
    }

    /// Every tree node, ascending.
    pub fn tree_nodes(&self) -> Result<Vec<(u64, Node)>> {
        let start = keys::data_tree_start(self.data_pointer);
        let end = keys::data_bitfield_start(self.data_pointer);
        let mut out = Vec::new();
        for (key, value) in self.store.range(&start, &end)? {
            if let Some(index) = keys::read_block_index(&key) {
                out.push((index, crate::rows::decode_tree_node(value)?));
            }
        }
        Ok(out)
    }

    /// Every bitfield page of kind 0, ascending.
    pub fn bitfield_pages(&self) -> Result<Vec<(u64, Bytes)>> {
        let start = keys::data_bitfield_start(self.data_pointer);
        let end = keys::data_user_data_start(self.data_pointer);
        let mut out = Vec::new();
        for (key, value) in self.store.range(&start, &end)? {
            if let Some((index, 0)) = keys::read_bitfield_index(&key) {
                out.push((index, value));
            }
        }
        Ok(out)
    }

    /// Every user-data value, by key.
    pub fn user_data_all(&self) -> Result<Vec<(String, Bytes)>> {
        let start = keys::data_user_data_start(self.data_pointer);
        let end = keys::data_user_data_end(self.data_pointer);
        let mut out = Vec::new();
        for (key, value) in self.store.range(&start, &end)? {
            if let Some(name) = keys::read_user_data_key(&key) {
                out.push((name, value));
            }
        }
        Ok(out)
    }

    // ---- writes -----------------------------------------------------------

    /// A write transaction for this core's current data pointer.
    pub fn write(&self) -> CoreTx {
        CoreTx {
            core_pointer: self.core_pointer,
            data_pointer: self.data_pointer,
            ops: Vec::new(),
        }
    }

    /// Applies a transaction atomically.
    pub fn flush(&self, tx: CoreTx) -> Result<()> {
        if tx.core_pointer != self.core_pointer || tx.data_pointer != self.data_pointer {
            return Err(Error::BadArgument(
                "transaction belongs to another core".into(),
            ));
        }
        self.store.apply_ops(tx.ops)
    }

    // ---- sessions ---------------------------------------------------------

    /// JS `createSession(name, head)`: a named data pointer whose dependency
    /// covers `head.length` blocks of the current data pointer.
    pub fn create_session(&self, name: &str, head: Option<&CoreHead>) -> Result<CoreStorage> {
        let current_head = self.get_head()?.unwrap_or_default();
        let head = head.cloned().unwrap_or_else(|| current_head.clone());
        if head.length > current_head.length {
            return Err(Error::BadArgument(
                "Invalid head passed, ahead of core".into(),
            ));
        }
        let mut sessions = self.get_sessions()?;
        let data_pointer = match sessions.iter().find(|session| session.name == name) {
            Some(existing) => existing.data_pointer,
            None => {
                let pointer = self.store.alloc_data_pointer()?;
                sessions.push(Session {
                    name: name.to_string(),
                    data_pointer: pointer,
                });
                pointer
            }
        };
        // The sessions row lives under the core pointer; the head and dependency
        // belong to the session's own data pointer. One transaction writes both,
        // like JS's shared view flush.
        let session = CoreStorage {
            store: self.store.clone(),
            core_pointer: self.core_pointer,
            data_pointer,
            dependencies: vec![Dependency {
                data_pointer: self.data_pointer,
                length: head.length,
            }],
        };
        let mut tx = session.write();
        tx.set_sessions(&sessions);
        tx.set_dependency(session.dependencies[0]);
        if head.length > 0 {
            tx.set_head(&head);
        }
        session.flush(tx)?;
        Ok(session)
    }

    /// JS `resumeSession(name)`.
    pub fn resume_session(&self, name: &str) -> Result<Option<CoreStorage>> {
        let Some(session) = self
            .get_sessions()?
            .into_iter()
            .find(|session| session.name == name)
        else {
            return Ok(None);
        };
        let mut storage = CoreStorage {
            store: self.store.clone(),
            core_pointer: self.core_pointer,
            data_pointer: session.data_pointer,
            dependencies: Vec::new(),
        };
        if let Some(dependency) = storage.get_dependency()? {
            storage.dependencies.push(dependency);
        }
        Ok(Some(storage))
    }

    /// JS `deleteSessions`.
    pub fn delete_sessions(&self) -> Result<()> {
        let sessions = self.get_sessions()?;
        let mut tx = self.write();
        tx.set_sessions(&[]);
        self.flush(tx)?;
        let ops = sessions
            .into_iter()
            .map(|session| {
                Op::DeleteRange(
                    keys::data_range_start(session.data_pointer),
                    keys::data_range_end(session.data_pointer),
                )
            })
            .collect();
        self.store.apply_ops(ops)
    }
}

impl Store {
    /// Bumps the data pointer counter (`_allocData`).
    fn alloc_data_pointer(&self) -> Result<u64> {
        self.write_with(|store| {
            let mut head = store.head()?;
            let pointer = head.datas;
            head.datas += 1;
            Ok((vec![Op::Put(keys::store_head(), head.encode())], pointer))
        })
    }
}

enum Op {
    Put(Vec<u8>, Bytes),
    PutTree(u64, Node),
    Delete(Vec<u8>),
    DeleteRange(Vec<u8>, Vec<u8>),
}

/// Pending writes for one core, in JS `CoreTX`'s shape.
pub struct CoreTx {
    core_pointer: u64,
    data_pointer: u64,
    ops: Vec<Op>,
}

impl CoreTx {
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    fn put(&mut self, key: Vec<u8>, value: Bytes) {
        self.ops.push(Op::Put(key, value));
    }

    fn delete(&mut self, key: Vec<u8>) {
        self.ops.push(Op::Delete(key));
    }

    fn delete_range(&mut self, start: Vec<u8>, end: Vec<u8>) {
        self.ops.push(Op::DeleteRange(start, end));
    }

    pub fn set_auth(&mut self, auth: &Auth) {
        self.put(keys::core_auth(self.core_pointer), auth.encode());
    }

    pub fn set_sessions(&mut self, sessions: &[Session]) {
        self.put(
            keys::core_sessions(self.core_pointer),
            Session::encode_list(sessions),
        );
    }

    pub fn set_group(&mut self, group: Option<CoreGroup>) {
        match group {
            Some(group) => self.put(keys::core_group(self.core_pointer), group.encode()),
            None => self.delete(keys::core_group(self.core_pointer)),
        }
    }

    pub fn set_head(&mut self, head: &CoreHead) {
        self.put(keys::data_head(self.data_pointer), head.encode());
    }

    /// `putGroupUpdate(groupPtr, timestamp, key)`: this core's entry in the
    /// group's update log, keyed by the head timestamp it stands for.
    pub fn put_group_update(&mut self, group: u64, timestamp: u64, key: &[u8; 32]) {
        self.put(
            keys::group_entry(group, timestamp, self.core_pointer),
            Bytes::copy_from_slice(key),
        );
    }

    pub fn delete_group_update(&mut self, group: u64, timestamp: u64) {
        self.delete(keys::group_entry(group, timestamp, self.core_pointer));
    }

    pub fn delete_head(&mut self) {
        self.delete(keys::data_head(self.data_pointer));
    }

    pub fn set_dependency(&mut self, dependency: Dependency) {
        self.put(
            keys::data_dependency(self.data_pointer),
            dependency.encode(),
        );
    }

    pub fn set_hints(&mut self, hints: &Hints) {
        self.put(keys::data_hints(self.data_pointer), hints.encode());
    }

    pub fn put_block(&mut self, index: u64, data: Bytes) {
        self.put(keys::data_block(self.data_pointer, index), data);
    }

    pub fn delete_block(&mut self, index: u64) {
        self.delete(keys::data_block(self.data_pointer, index));
    }

    pub fn delete_block_range(&mut self, start: u64, end: u64) {
        self.delete_range(
            keys::data_block(self.data_pointer, start),
            keys::data_block(self.data_pointer, end),
        );
    }

    pub fn put_bitfield_page(&mut self, index: u64, data: Bytes) {
        self.put(keys::data_bitfield(self.data_pointer, index, 0), data);
    }

    pub fn delete_bitfield_page(&mut self, index: u64) {
        self.delete(keys::data_bitfield(self.data_pointer, index, 0));
    }

    pub fn delete_bitfield_page_range(&mut self, start: u64, end: u64) {
        self.delete_range(
            keys::data_bitfield(self.data_pointer, start, 0),
            keys::data_bitfield(self.data_pointer, end, 0),
        );
    }

    pub fn put_tree_node(&mut self, node: Node) {
        self.ops.push(Op::PutTree(self.data_pointer, node));
    }

    pub fn delete_tree_node(&mut self, index: u64) {
        self.delete(keys::data_tree(self.data_pointer, index));
    }

    pub fn delete_tree_range(&mut self, start: u64, end: u64) {
        self.delete_range(
            keys::data_tree(self.data_pointer, start),
            keys::data_tree(self.data_pointer, end),
        );
    }

    pub fn put_user_data(&mut self, key: &str, value: Option<Bytes>) {
        let row = keys::data_user_data(self.data_pointer, key);
        match value {
            Some(value) => self.put(row, value),
            None => self.delete(row),
        }
    }

    pub fn put_mark(&mut self, index: u64, data: Bytes) {
        self.put(keys::data_mark(self.data_pointer, index), data);
    }

    pub fn delete_mark(&mut self, index: u64) {
        self.delete(keys::data_mark(self.data_pointer, index));
    }

    pub fn delete_mark_range(&mut self, start: u64, end: u64) {
        self.delete_range(
            keys::data_mark(self.data_pointer, start),
            keys::data_mark(self.data_pointer, end),
        );
    }

    pub fn put_local(&mut self, key: &[u8], value: Bytes) {
        self.put(keys::data_local(self.data_pointer, key), value);
    }

    pub fn delete_local(&mut self, key: &[u8]) {
        self.delete(keys::data_local(self.data_pointer, key));
    }

    pub fn delete_local_range(&mut self, start: &[u8], end: Option<&[u8]>) {
        self.delete_range(
            keys::data_local(self.data_pointer, start),
            match end {
                Some(end) => keys::data_local(self.data_pointer, end),
                None => keys::data_local_end(self.data_pointer),
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tree_io_for_one_data_pointer_does_not_lock_another() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        let cores: Vec<_> = (1..=2)
            .map(|key| {
                store
                    .create_core(CreateCore {
                        key: [key; 32],
                        discovery_key: [key; 32],
                        ..Default::default()
                    })
                    .unwrap()
            })
            .collect();
        let node = Node {
            index: 0,
            size: 1,
            hash: [1; 32],
        };
        let first = tree_lock(cores[0].data_pointer());
        let _writing = store.inner.tree_io[first].write().unwrap();
        let mut tx = cores[1].write();
        tx.put_tree_node(node);
        cores[1].flush(tx).unwrap();
        assert_eq!(cores[1].get_tree_node(0).unwrap(), Some(node));
        assert_eq!(
            cores[1].get_tree_nodes(&[0, 1]).unwrap(),
            vec![Some(node), None]
        );
        assert_eq!(cores[1].missing_tree_nodes([1, 0]).unwrap(), 1);
    }

    #[test]
    fn streaming_compaction_does_not_relocate_live_blobs() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path()).unwrap();
        store
            .with_db(|db, cf| {
                db.set_options_cf(
                    cf,
                    &[
                        ("blob_file_size", "1048576"),
                        ("disable_auto_compactions", "true"),
                    ],
                )
                .map_err(|e| Error::Rocks(e.to_string()))
            })
            .unwrap();
        let storage = store
            .create_core(CreateCore {
                key: [1; 32],
                discovery_key: [2; 32],
                ..Default::default()
            })
            .unwrap();
        for index in 0..16 {
            let mut tx = storage.write();
            tx.put_block(index, Bytes::from(vec![index as u8; 1024 * 1024]));
            storage.flush(tx).unwrap();
            store.flush().unwrap();
        }
        store.compact(true).unwrap();
        let blobs = || {
            let mut files: Vec<_> = std::fs::read_dir(dir.path().join("db"))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "blob"))
                .collect();
            files.sort();
            files
        };
        let before = blobs();
        assert!(
            before.len() >= 4,
            "exercise the oldest quarter of multiple blob files"
        );
        store.compact(false).unwrap();
        assert_eq!(
            blobs(),
            before,
            "streaming GC must never relocate live blob files"
        );
        for index in 0..16 {
            assert_eq!(
                storage.get_block(index).unwrap().unwrap(),
                Bytes::from(vec![index as u8; 1024 * 1024])
            );
        }
    }
}
