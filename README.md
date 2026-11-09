# hypercore-storage-rs

A Rust port of npm [`hypercore-storage`](https://github.com/holepunchto/hypercore-storage)
3.4.0 (the storage behind `corestore` 7.16.1 and `hypercore` 11.40.3), on the
`rocksdb` crate.

A directory written by the JS stack opens here, and the reverse: both stacks can
take turns on one directory, which is what this crate exists for. The row layout
is in [`docs/format.md`](docs/format.md).

## Usage

```rust
use hypercore_storage::{CreateCore, Store};

let store = Store::open(std::path::Path::new("./db"))?;
let core = store.create_core(CreateCore {
    key,
    discovery_key,
    ..Default::default()
})?;

let mut tx = core.write();
tx.put_block(0, bytes::Bytes::from_static(b"hello"));
tx.set_head(&hypercore_storage::CoreHead { length: 1, ..Default::default() });
core.flush(tx)?;

let block = core.get_block(0)?.unwrap();
assert_eq!(&block[..], b"hello");
# Ok::<(), hypercore_storage::Error>(())
```

* `Store` — the corestore level: store head (counters, primary key as `seed`,
  `defaultDiscoveryKey`), the discovery-key index, aliases, groups and their
  update log, `get_infos`, `delete_core`, `clear`, `read_only`, `flush`, `close`.
* `CoreStorage` — one core and its current data pointer: auth, sessions, group,
  head, hints, dependency, blocks, tree nodes, bitfield pages, user data, local
  entries and marks, each read from its own row.
* `CoreTx` — JS `CoreTX` in snake_case: `set_auth`, `set_head`, `set_hints`,
  `put_block`, `delete_block_range`, `put_tree_node`, `put_bitfield_page`,
  `put_user_data`, `put_local`, `put_mark`, sessions and dependencies. One
  transaction is one atomic `WriteBatch`.

Rows this crate does not interpret (named-session dependencies, marks, local
entries, extra bitfield kinds, group rows) are never rewritten or dropped.

`Store::open_with_options(path, StoreOptions { wal: false, ..Default::default() })` is for a re-fetchable
cache only. It skips WAL writes; a crash can discard unflushed whole batches
(normal shutdown still flushes). Blocks, tree nodes and bitfield rows still
commit atomically in one column family. `Store::open` keeps the WAL enabled for
owned data, as before. No on-disk format changes.

`StoreOptions { memory: true, ..Default::default() }` uses a fresh RocksDB MemEnv,
including blob files, WAL-free. Reopening starts empty; flush, eviction and blob
GC retain their normal paths and release MemEnv file storage. rust-rocksdb creates
an empty directory on disk, but no database or media files. The caller must bound
live data and allow headroom for memtables and transient GC copies.

Runtime memory budgets are shared across stores in a process: an 8 MiB block
cache (including index/filter blocks) and a 32 MiB write-buffer flush target, with
two 12 MiB memtables (32 MiB on macOS) per corestore column family and at most 48 open files per DB.
Each Corestore directory is one DB, not one DB per core or followed channel.
These limits also apply to read-only opens; table/row formats are unchanged.
The write-buffer target is soft: an atomic batch may exceed it. Global writer
stalling is disabled because an oversized batch in an idle DB can otherwise
block writes to another DB forever, including its first core's metadata.
Incremental sync every 1 MiB and three background jobs (four on macOS), with two
file-opening threads, match the JS worklet's I/O scheduling. The shared 32 MiB
flush target stays soft; a slow disk can retain an active and an immutable memtable.

Tree reads also share a per-store 32768-node LRU (about 4 MiB). It caches only
present nodes, keyed by resolved data pointer and node index, and follows atomic
overwrites and deletions, including named-session dependencies. `get_tree_nodes`
preserves input order and uses one RocksDB multi-get for multiple cache misses
(a single miss uses one get without multi-get setup).
`missing_tree_nodes` consumes an ancestor iterator only up to a cached node,
then batches the unresolved prefix; a cache hit allocates nothing. Committed
tree writes populate the cache from their decoded nodes without decoding them again.
The cache grows on demand rather than reserving its full capacity at store open.
Tree I/O is synchronized by data pointer (32 fixed lock stripes), while the one
shared LRU mutex protects only cache operations, never RocksDB reads or writes.
Multi-get and named-session reads lock their dependency pointers in stripe order;
deletions lock all stripes before committing and invalidating the cache.

This changes no row, table, or wire encoding.

Automatic metadata compactions do not relocate live video blobs (age cutoff 0).
`Store::compact(false)` is non-exclusive and first applies tombstones without
relocation. If at least 25% of the remaining blob-file bytes are garbage, a second
pass reclaims them; below that gate a mostly-live cache stays put. Retained blob
space is therefore about 4/3 of live bytes between trims (excluding WAL/SST and
temporary compaction output). Cast read-ahead reserves this overhead from physical
free space; logical cache contribution settings are unchanged.
`compact(true)` still forces relocation for an explicit full wipe. Manual passes
are serialized because the Rust binding requires a temporary CF-wide GC override;
the default cutoff 0 is restored afterward. These blocking calls belong on the
host's blocking executor, not its HTTP/replication event loop.

## Tests

`cargo test` runs row-level round trips against the raw layout, including a
reopen and a session. It needs `compact-encoding-rs` and `hyperschema-rs`
checked out next to this repo (`../compact-encoding-rs`, `../hyperschema-rs`),
as CI does. The JS interop tests live in `hypercore-rs`
(`tests/storage_switch.rs`, `tests/storage_crash.rs`).

## Throughput

Append and read of 1 GiB of 512 KiB blocks through `hypercore::Core`, on an
aarch64 Mac (`cargo run --release --example storage_bench -- <dir> 1024`, from
`../hypercore-rs`):

* append: 1.6–2.3 s for 1024 MiB → **440–630 MiB/s** (including the blake2b
  hashing and signing every batch)
* read back: 180–490 ms → **2.1–5.6 GiB/s** from the page cache

Six runs, no `sync` in between; the spread is other builds on the same
machine. The first, cold run wrote at 283 MiB/s.

For a paced 256 MiB ingest (512 KiB values, 20 ms between eight-block bursts),
three local arm64 Mac runs reduced peak memtables from 76 MiB to 12 MiB
and maximum RSS from 102–126 MiB to 43–53 MiB. The measured RocksDB default
cache capacity was 32 MiB per DB; the shared cache is 8 MiB total. Smaller
flushes produced more table readers (7.6 KiB → 37.9 KiB in that workload).
