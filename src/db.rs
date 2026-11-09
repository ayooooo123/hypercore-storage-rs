//! RocksDB's JS-compatible table format with process-wide memory budgets.
//!
//! Cache, memtable and file-handle limits are runtime-only: they change when
//! tables flush, not the row encoding, table format, compression or checksums.

use std::path::Path;
use std::sync::LazyLock;

use rocksdb::{BlockBasedOptions, Cache, DB, DBCompressionType, Options, WriteBufferManager};

use crate::{Error, Result};

/// The single non-default column family (`index.js:13`).
pub const COLUMN_FAMILY: &str = "corestore";

static BLOCK_CACHE: LazyLock<Cache> = LazyLock::new(|| Cache::new_lru_cache(8 * 1024 * 1024));
// A large atomic batch can leave an idle DB over budget; stalling another DB deadlocks it.
static WRITE_BUFFERS: LazyLock<WriteBufferManager> =
    LazyLock::new(|| WriteBufferManager::new_write_buffer_manager(32 * 1024 * 1024, false));

pub(crate) fn open(path: &Path, read_only: bool) -> Result<DB> {
    let mut options = Options::default();
    options.create_if_missing(!read_only);
    options.set_max_open_files(48);
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
    };
    db.map_err(|e| Error::Rocks(e.to_string()))
}

fn column_family() -> Options {
    let mut table = BlockBasedOptions::default();
    table.set_block_cache(&BLOCK_CACHE);
    table.set_block_size(8192);
    table.set_format_version(6);
    table.set_cache_index_and_filter_blocks(true);
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
    // JS writes with kNoCompression and XXH3 checksums; match both so the two
    // libraries produce the same tables (see docs/format.md).
    options.set_compression_type(DBCompressionType::None);
    options.set_enable_blob_files(true);
    options.set_min_blob_size(4096);
    options.set_blob_file_size(256 * 1024 * 1024);
    options.set_enable_blob_gc(true);
    // Reclaim on explicit trims, not on every metadata compaction of a live video.
    options.set_blob_gc_age_cutoff(0.0);
    options.set_blob_gc_force_threshold(1.0);
    options.set_blob_compression_type(DBCompressionType::None);
    options
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_compaction_does_not_rewrite_live_blobs() {
        let dir = tempfile::tempdir().unwrap();
        let db = open(dir.path(), false).unwrap();
        let cf = db.cf_handle(COLUMN_FAMILY).unwrap();
        db.set_options_cf(cf, &[("disable_auto_compactions", "true")])
            .unwrap();
        let value = vec![0x5a; 512 * 1024];
        for batch in 0u64..8 {
            for i in batch * 8..(batch + 1) * 8 {
                db.put_cf(cf, i.to_be_bytes(), &value).unwrap();
            }
            db.put_cf(cf, b"head", batch.to_be_bytes()).unwrap();
            db.flush_cf(cf).unwrap();
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
        // Inherit the CF policy, as an automatic compaction does (no GC override).
        db.compact_range_cf(cf, None::<&[u8]>, None::<&[u8]>);
        assert_eq!(blobs(), before, "compaction relocated live video data");
        for i in 0u64..64 {
            assert_eq!(db.get_cf(cf, i.to_be_bytes()).unwrap().unwrap(), value);
        }
    }

    #[test]
    fn bounded_memtables_flush_and_reopen_large_blocks() {
        let dirs = [tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap()];
        let value = vec![0x5a; 512 * 1024];
        for dir in &dirs {
            let db = open(dir.path(), false).unwrap();
            let cf = db.cf_handle(COLUMN_FAMILY).unwrap();
            for i in 0u64..48 {
                db.put_cf(cf, i.to_be_bytes(), &value).unwrap();
            }
            db.flush_cf(cf).unwrap();
            drop(db);
            let db = open(dir.path(), true).unwrap();
            let cf = db.cf_handle(COLUMN_FAMILY).unwrap();
            for i in 0u64..48 {
                assert_eq!(db.get_cf(cf, i.to_be_bytes()).unwrap().unwrap(), value);
            }
        }
    }

    #[test]
    fn oversized_atomic_batch_does_not_stall_another_store() {
        let writer_dir = tempfile::tempdir().unwrap();
        let reader_dir = tempfile::tempdir().unwrap();
        let writer = open(writer_dir.path(), false).unwrap();
        let reader = open(reader_dir.path(), false).unwrap();
        let cf = writer.cf_handle(COLUMN_FAMILY).unwrap();
        let value = vec![0x5a; 512 * 1024];
        let mut batch = rocksdb::WriteBatch::default();
        for i in 0u64..96 {
            batch.put_cf(cf, i.to_be_bytes(), &value);
        }
        writer.write(batch).unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let write = std::thread::spawn(move || {
            let cf = reader.cf_handle(COLUMN_FAMILY).unwrap();
            tx.send(reader.put_cf(cf, b"metadata", b"value")).unwrap();
        });
        rx.recv_timeout(std::time::Duration::from_secs(5))
            .expect("an idle writer must not stall another DB")
            .unwrap();
        write.join().unwrap();
    }
}
