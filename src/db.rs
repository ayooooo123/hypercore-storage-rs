//! RocksDB open options that match npm `rocksdb-native`'s column family.
//!
//! `hypercore-storage/index.js:1308-1330` asks for blob files from 4 KiB, a
//! 256 MiB blob file, blob GC, 8 KiB table blocks, table format 6, a 10
//! bits/key bloom filter, 7 levels, 2 write buffers of 64 MiB. The same options
//! are set here so a Rust writer produces SSTs the JS reader expects and the
//! reverse.

use std::path::Path;

use rocksdb::{BlockBasedOptions, DB, DBCompressionType, Options};

use crate::{Error, Result};

/// The single non-default column family (`index.js:13`).
pub const COLUMN_FAMILY: &str = "corestore";

pub(crate) fn open(path: &Path, read_only: bool) -> Result<DB> {
    let mut options = Options::default();
    options.create_if_missing(!read_only);
    let db = if read_only {
        DB::open_cf_for_read_only(&options, path, [COLUMN_FAMILY], false)
    } else {
        options.create_missing_column_families(true);
        DB::open_cf_with_opts(&options, path, [(COLUMN_FAMILY, column_family())])
    };
    db.map_err(|e| Error::Rocks(e.to_string()))
}

fn column_family() -> Options {
    let mut table = BlockBasedOptions::default();
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
    options.set_write_buffer_size(64 * 1024 * 1024);
    // JS writes with kNoCompression and XXH3 checksums; match both so the two
    // libraries produce the same tables (see docs/format.md).
    options.set_compression_type(DBCompressionType::None);
    options.set_enable_blob_files(true);
    options.set_min_blob_size(4096);
    options.set_blob_file_size(256 * 1024 * 1024);
    options.set_enable_blob_gc(true);
    options.set_blob_gc_age_cutoff(0.25);
    options.set_blob_gc_force_threshold(1.0);
    options.set_blob_compression_type(DBCompressionType::None);
    options
}
