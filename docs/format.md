# On-disk format

This crate is a read/write port of npm `hypercore-storage` 3.4.0 as used by
`corestore` 7.16.1 and `hypercore` 11.40.3. A directory written by either stack
opens in the other.

## Directory

| Path | Holds |
|---|---|
| `<dir>/db/` | RocksDB, one non-default column family named `corestore` (`hypercore-storage/index.js:13`, `526-537`, `1308-1330`) |
| `<dir>/CORESTORE` | `device-file` lock plus `{"id": ...}`; JS only, never read or written here |
| `<dir>/primary-key` | JS file from before `CORESTORE`; never written by 7.16.x |

The column family options match JS: blob files from 4 KiB, 256 MiB blob files,
blob GC, 8 KiB table blocks, table format **6**, a 10 bits/key bloom filter,
7 levels, 2 write buffers of 64 MiB, `compression=kNoCompression`,
`blob_compression_type=kNoCompression` and `checksum=kXXH3` (the last three are
RocksDB defaults for the JS build; they are set explicitly here so both sides
write the same tables). There is no custom comparator, so the default bytewise
order is the key order on both sides.

### RocksDB versions

| Side | Library | RocksDB |
|---|---|---|
| JS | `rocksdb-native` 3.19.x, built from `holepunchto/librocksdb#7b5352d` | **10.5.1** (as `rocksdb_version` in its OPTIONS file) |
| Rust | `rocksdb` crate 0.24 → `librocksdb-sys 0.17.3+10.4.2` | **10.4.2** |

Rust is one patch older inside the same major version, so the store each side
writes stays readable by the other; the four-switch test in `hypercore-rs`
(`tests/storage_switch.rs`) checks exactly that, and the OPTIONS file records
`format_version=6` on both sides.

Do not bump the `rocksdb` crate to 0.25 (`librocksdb-sys 0.19.0+11.8.1`) without
re-running that test: RocksDB does not promise that an older library opens a
newer MANIFEST, and JS 10.5.1 would then be the older side. The alternative is
to build against the same `holepunchto/librocksdb` fork JS uses.

## Keys

`index-encoder` values, not compact-encoding:

* `UINT`: `n <= 0xfb` is one byte; else `0xfc hi lo`, `0xfd` (four bytes BE),
  `0xfe` (eight bytes BE), `0xff` for `Infinity`.
* `STRING`/`BUFFER`: `0x00`, bytes, `0x00 0x01`, where a zero byte inside the
  payload is written `0x00 0x02`.

Every key starts with a tag (`lib/keys.js:5-11`):

| Tag | Row | Rest of key | Value |
|---|---|---|---|
| 0 | store head | — | `@corestore/head` |
| 1 | core by discovery key | discovery key (32 bytes) | `@corestore/core` |
| 2 | core by alias | namespace (32), name (`STRING`) | discovery key |
| 3 | core | core pointer, kind | see below |
| 4 | data | data pointer, kind | see below |
| 5 | group by topic | topic (32) | group pointer (`UINT`) |
| 6 | group update | group (`UINT`), timestamp (`uint64be`), core pointer | 32-byte core key |

Core kinds: `0` auth, `1` sessions, `2` group.
Data kinds: `0` head, `1` dependency, `2` hints, `3` block (then a block index),
`4` tree node (then a flat-tree index), `5` bitfield page (then a page index and
a bitfield kind; kind `0` is the block bitfield), `6` user data (then a
`STRING` key), `7` local (then raw key bytes), `8` mark (then an index).

## Values

Bare hyperschema encodings from hypercore-storage's `schema.json`, no prefix
except the version byte inside `@corestore/head`:

* `@corestore/head` — version 2: `cores`, `datas`, `groups`, `seed`,
  `defaultDiscoveryKey`. `seed` **is the corestore primary key**. Version 1 is
  read through the same `headLegacyMap` JS applies.
* `@corestore/core` — `version` (2), `corePointer`, `dataPointer`, alias.
* `@core/auth` — key, discovery key, flags; the manifest is *framed* and uses
  hypercore's manifest codec (which has version-0 compatibility cases), so this
  row is encoded by hand rather than with the generated struct codec.
* `@core/head` — `fork`, `length`, `rootHash`, `signature` (optional buffer),
  flags, `timestamp` (fixed eight bytes, epoch milliseconds).
* `@core/hints` — `contiguousLength`, `remoteContiguousLength`, `recovering`.
* `@core/tree-node` — `{ index, size, hash }`, byte-identical to
  `compact_encoding::hypercore::Node`.
* `@core/dependency` — `{ dataPointer, length }` for named sessions.
* bitfield pages — 4096 bytes, bit `i` is bit `i % 8` of byte `i / 8`.
* blocks — the stored bytes; ciphertext with an 8-byte padding prefix when the
  core is encrypted.

`hypercore-rs`'s old per-core header blob is gone: its fields are split across
the auth row (key, manifest, key pair, group, encryption key), the head row
(fork, length, signature, root hash, timestamp) and the hints row (contiguous
lengths, and `recovering` carries hypercore-rs's `frozen` flag).

## What is preserved

Rows this crate does not interpret — named-session dependencies, marks, local
entries, hints, extra bitfield kinds, group rows — are never rewritten or
dropped. Writing a core touches only the rows the operation names, and
`delete_core` removes exactly one core's ranges.

## Writes

Each transaction is one `WriteBatch` applied with RocksDB's default write
options, i.e. atomic with the WAL on and no per-batch fsync, matching JS's
`rocksdb_write(..., nullptr, ...)`. A process kill can lose the last commits and
leaves a readable store; `Store::flush` flushes the memtable.
