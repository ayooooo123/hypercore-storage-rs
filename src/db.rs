//! Storage backend abstraction: RocksDB (`rocksdb-backend`, default) or pure-Rust
//! `tinyrocks` (`tinyrocks-backend`), both reading and writing JS `hypercore-storage` 3.4.0's
//! exact on-disk format.

use std::path::Path;

use crate::Result;

/// The single non-default column family (`index.js:13`).
pub const COLUMN_FAMILY: &str = "corestore";

/// Backend-agnostic write batch operations for the `corestore` column family.
#[derive(Default)]
pub(crate) struct Batch {
    ops: Vec<BatchOp>,
}

enum BatchOp {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
    DeleteRange(Vec<u8>, Vec<u8>),
}

impl Batch {
    pub(crate) fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        self.ops.push(BatchOp::Put(key.into(), value.into()));
    }

    pub(crate) fn delete(&mut self, key: impl Into<Vec<u8>>) {
        self.ops.push(BatchOp::Delete(key.into()));
    }

    pub(crate) fn delete_range(&mut self, start: impl Into<Vec<u8>>, end: impl Into<Vec<u8>>) {
        self.ops
            .push(BatchOp::DeleteRange(start.into(), end.into()));
    }
}

#[cfg(all(feature = "rocksdb-backend", feature = "tinyrocks-backend"))]
compile_error!("enable only one of `rocksdb-backend` or `tinyrocks-backend`");

#[cfg(not(any(feature = "rocksdb-backend", feature = "tinyrocks-backend")))]
compile_error!("enable one of `rocksdb-backend` or `tinyrocks-backend`");

#[cfg(feature = "rocksdb-backend")]
mod imp {
    use std::path::Path;
    use std::sync::LazyLock;

    use rocksdb::{
        BlockBasedOptions, BottommostLevelCompaction, Cache, CompactOptions, DB, DBCompressionType,
        IteratorMode, Options, ReadOptions, WriteBatch, WriteBufferManager, WriteOptions,
    };

    use super::{Batch, BatchOp, COLUMN_FAMILY};
    use crate::{Error, Result};

    const MIB: usize = 1024 * 1024;
    const MIN_CACHE_MIB: usize = 8;
    const MAX_CACHE_MIB: usize = 4096;

    pub(super) fn parse_cache_mb_override(value: Option<&str>) -> Option<usize> {
        let mb = value?.trim().parse::<usize>().ok()?;
        Some(mb.clamp(MIN_CACHE_MIB, MAX_CACHE_MIB) * MIB)
    }

    pub(super) fn default_block_cache_bytes() -> usize {
        if cfg!(any(target_os = "android", target_os = "ios")) {
            8 * MIB
        } else if cfg!(target_os = "macos") {
            256 * MIB
        } else {
            512 * MIB
        }
    }

    fn block_cache_bytes() -> usize {
        parse_cache_mb_override(std::env::var("PEARTUBE_ROCKS_CACHE_MB").ok().as_deref())
            .unwrap_or_else(default_block_cache_bytes)
    }

    pub(super) fn max_open_files() -> i32 {
        if cfg!(any(target_os = "android", target_os = "ios")) {
            48
        } else {
            512
        }
    }

    // Large multi-TB relays carry 50–150+ L6 SST files whose monolithic Bloom
    // filter + index blocks are 2–4 MiB each. Keep mobile tight at 8 MiB while
    // giving desktop and Linux relays enough cache and file descriptors to hold
    // active filters in RAM instead of thrashing page-cache pread64s per lookup.
    static BLOCK_CACHE: LazyLock<Cache> =
        LazyLock::new(|| Cache::new_lru_cache(block_cache_bytes()));
    // A large atomic batch can leave an idle DB over budget; stalling another DB deadlocks it.
    static WRITE_BUFFERS: LazyLock<WriteBufferManager> =
        LazyLock::new(|| WriteBufferManager::new_write_buffer_manager(32 * 1024 * 1024, false));

    pub(crate) struct Backend {
        db: DB,
        write_options: WriteOptions,
    }

    impl Backend {
        pub(crate) fn open(path: &Path, read_only: bool, memory: bool, wal: bool) -> Result<Self> {
            let mut options = Options::default();
            if memory {
                let env = rocksdb::Env::mem_env().map_err(|e| Error::Rocks(e.to_string()))?;
                options.set_env(&env);
            }
            options.create_if_missing(!read_only);
            options.set_max_open_files(max_open_files());
            options.set_bytes_per_sync(1024 * 1024);
            options.set_max_background_jobs(if cfg!(target_os = "macos") { 4 } else { 3 });
            options.set_max_file_opening_threads(2);
            options.set_write_buffer_manager(&WRITE_BUFFERS);
            let db = if read_only {
                DB::open_cf_with_opts_for_read_only(
                    &options,
                    path,
                    [(COLUMN_FAMILY, column_family())],
                    false,
                )
            } else {
                options.create_missing_column_families(true);
                DB::open_cf_with_opts(&options, path, [(COLUMN_FAMILY, column_family())])
            }
            .map_err(|e| Error::Rocks(e.to_string()))?;
            let mut write_options = WriteOptions::default();
            write_options.disable_wal(!wal);
            Ok(Self { db, write_options })
        }

        fn cf(&self) -> Result<&rocksdb::ColumnFamily> {
            self.db
                .cf_handle(COLUMN_FAMILY)
                .ok_or_else(|| Error::Corrupt("missing corestore column family".into()))
        }

        pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            let cf = self.cf()?;
            self.db
                .get_cf(cf, key)
                .map_err(|e| Error::Rocks(e.to_string()))
        }

        pub(crate) fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
            let cf = self.cf()?;
            let results = if keys.len() == 1 {
                vec![self.db.get_cf(cf, &keys[0])]
            } else {
                self.db.multi_get_cf(keys.iter().map(|key| (cf, key)))
            };
            results
                .into_iter()
                .map(|r| r.map_err(|e| Error::Rocks(e.to_string())))
                .collect()
        }

        pub(crate) fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            let cf = self.cf()?;
            let mut options = ReadOptions::default();
            options.set_iterate_lower_bound(start.to_vec());
            options.set_iterate_upper_bound(end.to_vec());
            let mut out = Vec::new();
            for item in self.db.iterator_cf_opt(cf, options, IteratorMode::Start) {
                let (k, v) = item.map_err(|e| Error::Rocks(e.to_string()))?;
                out.push((k.to_vec(), v.to_vec()));
            }
            Ok(out)
        }

        pub(crate) fn range_reverse(
            &self,
            start: &[u8],
            end: &[u8],
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            let cf = self.cf()?;
            let mut options = ReadOptions::default();
            options.set_iterate_lower_bound(start.to_vec());
            options.set_iterate_upper_bound(end.to_vec());
            let mut out = Vec::new();
            for item in self.db.iterator_cf_opt(cf, options, IteratorMode::End) {
                let (k, v) = item.map_err(|e| Error::Rocks(e.to_string()))?;
                out.push((k.to_vec(), v.to_vec()));
            }
            Ok(out)
        }

        pub(crate) fn write_batch(&self, batch: Batch) -> Result<()> {
            let cf = self.cf()?;
            let mut rb = WriteBatch::default();
            for op in batch.ops {
                match op {
                    BatchOp::Put(k, v) => rb.put_cf(cf, k, v),
                    BatchOp::Delete(k) => rb.delete_cf(cf, k),
                    BatchOp::DeleteRange(s, e) => rb.delete_range_cf(cf, s, e),
                }
            }
            self.db
                .write_opt(rb, &self.write_options)
                .map_err(|e| Error::Rocks(e.to_string()))
        }

        pub(crate) fn flush_wal(&self, sync: bool) -> Result<()> {
            self.db
                .flush_wal(sync)
                .map_err(|e| Error::Rocks(e.to_string()))
        }

        pub(crate) fn flush(&self) -> Result<()> {
            let cf = self.cf()?;
            self.db
                .flush_cf(cf)
                .map_err(|e| Error::Rocks(e.to_string()))
        }

        pub(crate) fn compact(&self, relocate_blobs: bool) -> Result<()> {
            let cf = self.cf()?;
            self.db
                .set_options_cf(
                    cf,
                    &[
                        ("enable_blob_garbage_collection", "true"),
                        (
                            "blob_garbage_collection_age_cutoff",
                            if relocate_blobs { "1.0" } else { "0.0" },
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
            self.db
                .compact_range_cf_opt(cf, None::<&[u8]>, None::<&[u8]>, &options);
            if !relocate_blobs {
                let size = self
                    .db
                    .property_int_value_cf(cf, "rocksdb.live-blob-file-size")
                    .map_err(|e| Error::Rocks(e.to_string()))?
                    .unwrap_or(0);
                let garbage = self
                    .db
                    .property_int_value_cf(cf, "rocksdb.live-blob-file-garbage-size")
                    .map_err(|e| Error::Rocks(e.to_string()))?
                    .unwrap_or(0);
                if garbage > 0 && garbage >= size.div_ceil(4) {
                    self.db
                        .set_options_cf(cf, &[("blob_garbage_collection_age_cutoff", "1.0")])
                        .map_err(|e| Error::Rocks(e.to_string()))?;
                    options.set_bottommost_level_compaction(BottommostLevelCompaction::Force);
                    self.db
                        .compact_range_cf_opt(cf, None::<&[u8]>, None::<&[u8]>, &options);
                }
            }
            self.db
                .set_options_cf(
                    cf,
                    &[
                        ("enable_blob_garbage_collection", "true"),
                        ("blob_garbage_collection_age_cutoff", "0.0"),
                    ],
                )
                .map_err(|e| Error::Rocks(e.to_string()))?;
            Ok(())
        }

        #[cfg(test)]
        pub(crate) fn open_with_stats(path: &Path) -> Result<(DB, Options)> {
            let mut options = Options::default();
            options.create_if_missing(false);
            options.set_max_open_files(max_open_files());
            options.set_bytes_per_sync(1024 * 1024);
            options.set_max_background_jobs(if cfg!(target_os = "macos") { 4 } else { 3 });
            options.set_max_file_opening_threads(2);
            options.set_write_buffer_manager(&WRITE_BUFFERS);
            options.enable_statistics();
            let db = DB::open_cf_with_opts(&options, path, [(COLUMN_FAMILY, column_family())])
                .map_err(|e| Error::Rocks(e.to_string()))?;
            Ok((db, options))
        }

        #[cfg(test)]
        pub(crate) fn live_blob_file_size(&self) -> Result<u64> {
            let cf = self.cf()?;
            self.db
                .property_int_value_cf(cf, "rocksdb.live-blob-file-size")
                .map(|bytes| bytes.unwrap_or(0))
                .map_err(|e| Error::Rocks(e.to_string()))
        }

        #[cfg(test)]
        pub(crate) fn set_test_blob_file_size(&self, bytes: u64) -> Result<()> {
            let cf = self.cf()?;
            let s = bytes.to_string();
            self.db
                .set_options_cf(
                    cf,
                    &[("blob_file_size", &s), ("disable_auto_compactions", "true")],
                )
                .map_err(|e| Error::Rocks(e.to_string()))
        }
    }

    fn column_family() -> Options {
        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&BLOCK_CACHE);
        table.set_block_size(8192);
        table.set_format_version(6);
        table.set_cache_index_and_filter_blocks(true);
        table.set_pin_l0_filter_and_index_blocks_in_cache(true);
        table.set_bloom_filter(10.0, true);
        table.set_checksum_type(rocksdb::ChecksumType::XXH3);
        table.set_optimize_filters_for_memory(false);

        let mut options = Options::default();
        options.set_block_based_table_factory(&table);
        options.set_num_levels(7);
        options.set_max_write_buffer_number(2);
        options.set_write_buffer_size(if cfg!(target_os = "macos") {
            32 * 1024 * 1024
        } else {
            12 * 1024 * 1024
        });
        options.set_compression_type(DBCompressionType::None);
        options.set_enable_blob_files(true);
        options.set_min_blob_size(4096);
        options.set_blob_file_size(256 * 1024 * 1024);
        options.set_enable_blob_gc(true);
        options.set_blob_gc_age_cutoff(0.0);
        options.set_blob_gc_force_threshold(1.0);
        options.set_blob_compression_type(DBCompressionType::None);
        options
    }
}

#[cfg(feature = "tinyrocks-backend")]
mod imp {
    use std::path::Path;
    use std::sync::RwLock;

    use tinyrocks::{Db, ScanDirection, WriteBatch};

    use super::{Batch, BatchOp, COLUMN_FAMILY};
    use crate::{Error, Result};

    pub(crate) struct Backend {
        db: RwLock<Db>,
        cf_id: u32,
        disable_wal: bool,
    }

    impl Backend {
        pub(crate) fn open(path: &Path, read_only: bool, memory: bool, wal: bool) -> Result<Self> {
            let mut db = if memory {
                // Match rust-rocksdb MemEnv behavior (creates empty directory, writes no files).
                std::fs::create_dir_all(path)?;
                Db::open_memory(&[COLUMN_FAMILY]).map_err(|e| Error::Rocks(e.to_string()))?
            } else if read_only {
                Db::open_read_only(path).map_err(|e| Error::Rocks(e.to_string()))?
            } else {
                Db::open_rw(path, &[COLUMN_FAMILY]).map_err(|e| Error::Rocks(e.to_string()))?
            };
            db.set_write_buffer_size(if cfg!(target_os = "macos") {
                32 * 1024 * 1024
            } else {
                12 * 1024 * 1024
            });
            let cf_id = db
                .cf_id(COLUMN_FAMILY)
                .map_err(|e| Error::Rocks(e.to_string()))?;
            Ok(Self {
                db: RwLock::new(db),
                cf_id,
                disable_wal: !wal,
            })
        }

        pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.db
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .get_cf(COLUMN_FAMILY, key)
                .map_err(|e| Error::Rocks(e.to_string()))
        }

        pub(crate) fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
            let guard = self.db.read().unwrap_or_else(|e| e.into_inner());
            let mut out = Vec::with_capacity(keys.len());
            for key in keys {
                out.push(
                    guard
                        .get_cf(COLUMN_FAMILY, key)
                        .map_err(|e| Error::Rocks(e.to_string()))?,
                );
            }
            Ok(out)
        }

        pub(crate) fn range(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.db
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .scan_cf(
                    COLUMN_FAMILY,
                    Some(start),
                    Some(end),
                    ScanDirection::Forward,
                )
                .map_err(|e| Error::Rocks(e.to_string()))
        }

        pub(crate) fn range_reverse(
            &self,
            start: &[u8],
            end: &[u8],
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.db
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .scan_cf(
                    COLUMN_FAMILY,
                    Some(start),
                    Some(end),
                    ScanDirection::Reverse,
                )
                .map_err(|e| Error::Rocks(e.to_string()))
        }

        pub(crate) fn write_batch(&self, batch: Batch) -> Result<()> {
            let mut tb = WriteBatch::default();
            for op in batch.ops {
                match op {
                    BatchOp::Put(k, v) => tb.put_cf(self.cf_id, &k, &v),
                    BatchOp::Delete(k) => tb.delete_cf(self.cf_id, &k),
                    BatchOp::DeleteRange(s, e) => tb.delete_range_cf(self.cf_id, &s, &e),
                }
            }
            self.db
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .write_batch(&tb, self.disable_wal)
                .map_err(|e| Error::Rocks(e.to_string()))
        }

        pub(crate) fn flush_wal(&self, sync: bool) -> Result<()> {
            self.db
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .flush_wal(sync)
                .map_err(|e| Error::Rocks(e.to_string()))
        }

        pub(crate) fn flush(&self) -> Result<()> {
            self.db
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .flush_cf(COLUMN_FAMILY)
                .map_err(|e| Error::Rocks(e.to_string()))
        }

        pub(crate) fn compact(&self, relocate_blobs: bool) -> Result<()> {
            self.db
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .compact_cf(COLUMN_FAMILY, relocate_blobs)
                .map_err(|e| Error::Rocks(e.to_string()))
        }

        #[cfg(test)]
        pub(crate) fn live_blob_file_size(&self) -> Result<u64> {
            self.db
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .live_blob_file_size(COLUMN_FAMILY)
                .map_err(|e| Error::Rocks(e.to_string()))
        }

        #[cfg(test)]
        pub(crate) fn set_test_blob_file_size(&self, bytes: u64) -> Result<()> {
            self.db
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .set_blob_file_size(bytes);
            Ok(())
        }
    }
}

pub(crate) use imp::Backend;

pub(crate) fn open(path: &Path, read_only: bool, memory: bool, wal: bool) -> Result<Backend> {
    Backend::open(path, read_only, memory, wal)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_compaction_does_not_rewrite_live_blobs() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path(), false, false, true).unwrap();
        let value = vec![0x5a; 512 * 1024];
        for batch_idx in 0u64..8 {
            let mut b = Batch::default();
            for i in batch_idx * 8..(batch_idx + 1) * 8 {
                b.put(i.to_be_bytes(), value.clone());
            }
            b.put(b"head", batch_idx.to_be_bytes());
            db.write_batch(b).unwrap();
            db.flush().unwrap();
        }
        let blobs = || {
            let mut names = std::fs::read_dir(dir.path())
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| path.extension().is_some_and(|ext| ext == "blob"))
                .collect::<Vec<_>>();
            names.sort();
            names
        };
        let before = blobs();
        assert!(!before.is_empty());
        db.compact(false).unwrap();
        assert_eq!(blobs(), before, "compaction relocated live video data");
        for i in 0u64..64 {
            assert_eq!(db.get(&i.to_be_bytes()).unwrap().unwrap(), value);
        }
    }

    #[test]
    fn bounded_memtables_flush_and_reopen_large_blocks() {
        let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
        let value = vec![0x5a; 512 * 1024];
        for dir in &dirs {
            let db = open(dir.path(), false, false, true).unwrap();
            for i in 0u64..48 {
                let mut b = Batch::default();
                b.put(i.to_be_bytes(), value.clone());
                db.write_batch(b).unwrap();
            }
            db.flush().unwrap();
            drop(db);
            let db = open(dir.path(), true, false, true).unwrap();
            for i in 0u64..48 {
                assert_eq!(db.get(&i.to_be_bytes()).unwrap().unwrap(), value);
            }
        }
    }

    #[test]
    fn oversized_atomic_batch_does_not_stall_another_store() {
        let writer_dir = tempfile::tempdir().unwrap();
        let reader_dir = tempfile::tempdir().unwrap();
        let writer = open(writer_dir.path(), false, false, true).unwrap();
        let reader = open(reader_dir.path(), false, false, true).unwrap();
        let value = vec![0x5a; 512 * 1024];
        let mut batch = Batch::default();
        for i in 0u64..96 {
            batch.put(i.to_be_bytes(), value.clone());
        }
        writer.write_batch(batch).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let write = std::thread::spawn(move || {
            let mut b = Batch::default();
            b.put(b"metadata", b"value");
            tx.send(reader.write_batch(b)).unwrap();
        });
        rx.recv_timeout(std::time::Duration::from_secs(5))
            .expect("an idle writer must not stall another DB")
            .unwrap();
        write.join().unwrap();
    }

    #[cfg(feature = "rocksdb-backend")]
    #[test]
    fn rocksdb_cache_and_fd_budget_scales_for_non_mobile() {
        if cfg!(any(target_os = "android", target_os = "ios")) {
            assert_eq!(imp::default_block_cache_bytes(), 8 * 1024 * 1024);
            assert_eq!(imp::max_open_files(), 48);
        } else {
            assert!(imp::default_block_cache_bytes() >= 256 * 1024 * 1024);
            assert!(imp::max_open_files() >= 512);
        }
        assert_eq!(imp::parse_cache_mb_override(None), None);
        assert_eq!(imp::parse_cache_mb_override(Some("")), None);
        assert_eq!(imp::parse_cache_mb_override(Some("abc")), None);
        assert_eq!(
            imp::parse_cache_mb_override(Some("1")),
            Some(8 * 1024 * 1024)
        );
        assert_eq!(
            imp::parse_cache_mb_override(Some("512")),
            Some(512 * 1024 * 1024)
        );
        assert_eq!(
            imp::parse_cache_mb_override(Some("99999")),
            Some(4096 * 1024 * 1024)
        );
        // Verify 60 distinct flushed SSTs (> old 48 max_open_files cap) stay
        // resident across forward + reverse lookup passes with zero filter or
        // index cache misses on the second pass.
        let dir = tempfile::tempdir().unwrap();
        let sst_count = 60u64;
        {
            let db = open(dir.path(), false, false, true).unwrap();
            db.set_test_blob_file_size(256 * 1024 * 1024).unwrap();
            for sst in 0..sst_count {
                let mut b = Batch::default();
                for i in 0u64..128 {
                    let key = format!("sst-{sst:03}-key-{i:04}");
                    b.put(key.into_bytes(), vec![sst as u8; 128]);
                }
                db.write_batch(b).unwrap();
                db.flush().unwrap();
            }
        }
        let (db, opts) = imp::Backend::open_with_stats(dir.path()).unwrap();
        let cf = db.cf_handle(COLUMN_FAMILY).unwrap();
        // Warm all 60 table readers and their filter/index blocks.
        for sst in 0..sst_count {
            let key = format!("sst-{sst:03}-key-0001");
            assert_eq!(
                db.get_cf(cf, key.as_bytes()).unwrap().unwrap(),
                vec![sst as u8; 128]
            );
        }
        let filter_miss_before =
            opts.get_ticker_count(rocksdb::statistics::Ticker::BlockCacheFilterMiss);
        let index_miss_before =
            opts.get_ticker_count(rocksdb::statistics::Ticker::BlockCacheIndexMiss);
        let filter_hit_before =
            opts.get_ticker_count(rocksdb::statistics::Ticker::BlockCacheFilterHit);
        // Reverse pass over a different key in every SST: an LRU table/block
        // cache smaller than 60 SSTs evicts entries and causes misses here.
        for sst in (0..sst_count).rev() {
            let key = format!("sst-{sst:03}-key-0100");
            assert_eq!(
                db.get_cf(cf, key.as_bytes()).unwrap().unwrap(),
                vec![sst as u8; 128]
            );
        }
        let filter_miss_after =
            opts.get_ticker_count(rocksdb::statistics::Ticker::BlockCacheFilterMiss);
        let index_miss_after =
            opts.get_ticker_count(rocksdb::statistics::Ticker::BlockCacheIndexMiss);
        let filter_hit_after =
            opts.get_ticker_count(rocksdb::statistics::Ticker::BlockCacheFilterHit);
        if !cfg!(any(target_os = "android", target_os = "ios")) {
            assert_eq!(
                filter_miss_after, filter_miss_before,
                "60-SST reverse lookup pass thrashed Bloom filter blocks out of cache"
            );
            assert_eq!(
                index_miss_after, index_miss_before,
                "60-SST reverse lookup pass thrashed index blocks out of cache"
            );
            assert!(
                filter_hit_after > filter_hit_before,
                "expected filter cache hits on second pass"
            );
        }
    }

    #[test]
    fn multi_sst_forward_and_reverse_lookups_return_exact_values() {
        let dir = tempfile::tempdir().unwrap();
        let sst_count = 60u64;
        {
            let db = open(dir.path(), false, false, true).unwrap();
            for sst in 0..sst_count {
                let mut b = Batch::default();
                for i in 0u64..16 {
                    let key = format!("multi-{sst:03}-key-{i:03}");
                    b.put(key.into_bytes(), vec![sst as u8; 64]);
                }
                db.write_batch(b).unwrap();
                db.flush().unwrap();
            }
        }
        let db = open(dir.path(), true, false, true).unwrap();
        for sst in 0..sst_count {
            let key = format!("multi-{sst:03}-key-001");
            assert_eq!(
                db.get(key.as_bytes()).unwrap().unwrap(),
                vec![sst as u8; 64]
            );
        }
        for sst in (0..sst_count).rev() {
            let key = format!("multi-{sst:03}-key-015");
            assert_eq!(
                db.get(key.as_bytes()).unwrap().unwrap(),
                vec![sst as u8; 64]
            );
        }
    }
}
