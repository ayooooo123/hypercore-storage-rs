//! Row keys, byte-for-byte as npm `hypercore-storage`'s `lib/keys.js` builds them.
//!
//! Keys are `index-encoder` values, not compact-encoding: `UINT` is
//! `0xfc hi lo` / `0xfd` / `0xfe` (big-endian) with `0xff` as `Infinity`, and
//! `STRING`/`BUFFER` is `0x00`, bytes, `0x00 0x01`, where a zero byte inside the
//! payload is written as `0x00 0x02`.

/// Row tags (`TL_*` in `lib/keys.js`).
pub const TL_HEAD: u64 = 0;
pub const TL_CORE_BY_DKEY: u64 = 1;
pub const TL_CORE_BY_ALIAS: u64 = 2;
pub const TL_CORE: u64 = 3;
pub const TL_DATA: u64 = 4;
pub const TL_GROUP_BY_TOPIC: u64 = 5;
pub const TL_GROUP: u64 = 6;
/// One past the last tag (`store.clear` deletes up to here).
pub const TL_END: u64 = 7;

/// Kinds under a `TL_CORE` row.
pub const CORE_AUTH: u64 = 0;
pub const CORE_SESSIONS: u64 = 1;
pub const CORE_GROUP: u64 = 2;

/// Kinds under a `TL_DATA` row.
pub const DATA_HEAD: u64 = 0;
pub const DATA_DEPENDENCY: u64 = 1;
pub const DATA_HINTS: u64 = 2;
pub const DATA_BLOCK: u64 = 3;
pub const DATA_TREE: u64 = 4;
pub const DATA_BITFIELD: u64 = 5;
pub const DATA_USER_DATA: u64 = 6;
pub const DATA_LOCAL: u64 = 7;
pub const DATA_MARK: u64 = 8;
pub const DATA_END: u64 = 9;

/// Appends an `index-encoder` `UINT`.
pub fn uint(out: &mut Vec<u8>, n: u64) {
    if n <= 0xfb {
        out.push(n as u8);
    } else if n <= 0xffff {
        out.push(0xfc);
        out.push((n >> 8) as u8);
        out.push(n as u8);
    } else if n <= 0xffff_ffff {
        out.push(0xfd);
        out.extend_from_slice(&(n as u32).to_be_bytes());
    } else {
        out.push(0xfe);
        out.extend_from_slice(&n.to_be_bytes());
    }
}

/// Appends an `index-encoder` `FIXED32`/`BUFFER` (same escaping as `STRING`).
pub fn buffer(out: &mut Vec<u8>, bytes: &[u8]) {
    out.push(0);
    for &byte in bytes {
        if byte == 0 {
            out.push(0);
            out.push(2);
        } else {
            out.push(byte);
        }
    }
    out.extend_from_slice(&[0, 1]);
}

/// Appends an `index-encoder` `STRING`.
pub fn string(out: &mut Vec<u8>, s: &str) {
    buffer(out, s.as_bytes());
}

/// Decodes an `index-encoder` `UINT`; `None` is the encoder's `Infinity`.
pub fn read_uint(buf: &[u8], pos: &mut usize) -> Option<u64> {
    let a = *buf.get(*pos)?;
    *pos += 1;
    if a <= 0xfb {
        return Some(a as u64);
    }
    let bytes = match a {
        0xfc => 2,
        0xfd => 4,
        0xfe => 8,
        _ => return None,
    };
    if buf.len() - *pos < bytes {
        return None;
    }
    let mut n = 0u64;
    for _ in 0..bytes {
        n = (n << 8) | buf[*pos] as u64;
        *pos += 1;
    }
    Some(n)
}

pub fn read_fixed32(buf: &[u8], pos: &mut usize) -> Option<[u8; 32]> {
    let end = *pos + 32;
    if buf.len() < end {
        return None;
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&buf[*pos..end]);
    *pos = end;
    Some(out)
}

/// Decodes an `index-encoder` `STRING`/`BUFFER` payload.
pub fn read_buffer(buf: &[u8], pos: &mut usize) -> Option<Vec<u8>> {
    if *buf.get(*pos)? != 0 {
        return None;
    }
    *pos += 1;
    let mut out = Vec::new();
    loop {
        let i = *pos + buf[*pos..].iter().position(|&b| b == 0)?;
        out.extend_from_slice(&buf[*pos..i]);
        match buf.get(i + 1).copied().unwrap_or(0) {
            1 => {
                *pos = i + 2;
                return Some(out);
            }
            2 => {
                out.push(0);
                *pos = i + 2;
            }
            _ => return None,
        }
    }
}

pub fn read_string(buf: &[u8], pos: &mut usize) -> Option<String> {
    String::from_utf8(read_buffer(buf, pos)?).ok()
}

/// `store.head()`.
pub fn store_head() -> Vec<u8> {
    let mut key = Vec::with_capacity(1);
    uint(&mut key, TL_HEAD);
    key
}

/// `store.core(discoveryKey)`.
pub fn core_by_discovery_key(discovery_key: &[u8; 32]) -> Vec<u8> {
    let mut key = Vec::with_capacity(33);
    uint(&mut key, TL_CORE_BY_DKEY);
    key.extend_from_slice(discovery_key);
    key
}

/// The start of the `store.coreStart()..store.coreEnd()` range.
pub fn core_by_discovery_key_start() -> Vec<u8> {
    let mut key = Vec::with_capacity(1);
    uint(&mut key, TL_CORE_BY_DKEY);
    key
}

pub fn core_by_discovery_key_end() -> Vec<u8> {
    let mut key = Vec::with_capacity(1);
    uint(&mut key, TL_CORE_BY_DKEY + 1);
    key
}

/// `store.coreByAlias({ namespace, name })`.
pub fn core_by_alias(namespace: &[u8; 32], name: &str) -> Vec<u8> {
    let mut key = Vec::new();
    uint(&mut key, TL_CORE_BY_ALIAS);
    key.extend_from_slice(namespace);
    string(&mut key, name);
    key
}

pub fn core_by_alias_start() -> Vec<u8> {
    let mut key = Vec::new();
    uint(&mut key, TL_CORE_BY_ALIAS);
    key
}

pub fn core_by_alias_end() -> Vec<u8> {
    let mut key = Vec::new();
    uint(&mut key, TL_CORE_BY_ALIAS + 1);
    key
}

/// `store.alias(buffer)`.
pub fn read_alias(key: &[u8]) -> Option<([u8; 32], String)> {
    let mut pos = 0;
    read_uint(key, &mut pos)?;
    let namespace = read_fixed32(key, &mut pos)?;
    let name = read_string(key, &mut pos)?;
    Some((namespace, name))
}

/// `store.discoveryKey(buffer)`.
pub fn read_alias_discovery_key(key: &[u8]) -> Option<[u8; 32]> {
    let mut pos = 0;
    read_uint(key, &mut pos)?;
    read_fixed32(key, &mut pos)
}

fn core_key(pointer: u64, kind: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(11);
    uint(&mut key, TL_CORE);
    uint(&mut key, pointer);
    uint(&mut key, kind);
    key
}

pub fn core_auth(pointer: u64) -> Vec<u8> {
    core_key(pointer, CORE_AUTH)
}

pub fn core_sessions(pointer: u64) -> Vec<u8> {
    core_key(pointer, CORE_SESSIONS)
}

pub fn core_group(pointer: u64) -> Vec<u8> {
    core_key(pointer, CORE_GROUP)
}

/// Start of the whole `TL_CORE pointer .. TL_CORE pointer+1` range.
pub fn core_range_start(pointer: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(11);
    uint(&mut key, TL_CORE);
    uint(&mut key, pointer);
    key
}

pub fn core_range_end(pointer: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(11);
    uint(&mut key, TL_CORE);
    uint(&mut key, pointer + 1);
    key
}

fn data_key(pointer: u64, kind: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(11);
    uint(&mut key, TL_DATA);
    uint(&mut key, pointer);
    uint(&mut key, kind);
    key
}

pub fn data_head(pointer: u64) -> Vec<u8> {
    data_key(pointer, DATA_HEAD)
}

pub fn data_dependency(pointer: u64) -> Vec<u8> {
    data_key(pointer, DATA_DEPENDENCY)
}

pub fn data_hints(pointer: u64) -> Vec<u8> {
    data_key(pointer, DATA_HINTS)
}

pub fn data_block(pointer: u64, index: u64) -> Vec<u8> {
    let mut key = data_key(pointer, DATA_BLOCK);
    uint(&mut key, index);
    key
}

/// The `block(index)` key without the index, for range scans.
pub fn data_block_start(pointer: u64) -> Vec<u8> {
    data_key(pointer, DATA_BLOCK)
}

pub fn data_tree(pointer: u64, index: u64) -> Vec<u8> {
    let mut key = data_key(pointer, DATA_TREE);
    uint(&mut key, index);
    key
}

pub fn data_tree_start(pointer: u64) -> Vec<u8> {
    data_key(pointer, DATA_TREE)
}

pub fn data_bitfield(pointer: u64, index: u64, kind: u64) -> Vec<u8> {
    let mut key = data_key(pointer, DATA_BITFIELD);
    uint(&mut key, index);
    uint(&mut key, kind);
    key
}

/// Start of the block-bitfield (`kind = 0`) range.
pub fn data_bitfield_start(pointer: u64) -> Vec<u8> {
    data_key(pointer, DATA_BITFIELD)
}

pub fn data_user_data(pointer: u64, key: &str) -> Vec<u8> {
    let mut row = data_key(pointer, DATA_USER_DATA);
    string(&mut row, key);
    row
}

pub fn data_user_data_start(pointer: u64) -> Vec<u8> {
    data_key(pointer, DATA_USER_DATA)
}

pub fn data_user_data_end(pointer: u64) -> Vec<u8> {
    data_key(pointer, DATA_USER_DATA + 1)
}

pub fn data_local(pointer: u64, key: &[u8]) -> Vec<u8> {
    let mut row = data_key(pointer, DATA_LOCAL);
    row.extend_from_slice(key);
    row
}

pub fn data_local_start(pointer: u64) -> Vec<u8> {
    data_key(pointer, DATA_LOCAL)
}

pub fn data_local_end(pointer: u64) -> Vec<u8> {
    data_key(pointer, DATA_LOCAL + 1)
}

pub fn data_mark(pointer: u64, index: u64) -> Vec<u8> {
    let mut key = data_key(pointer, DATA_MARK);
    uint(&mut key, index);
    key
}

pub fn data_mark_start(pointer: u64) -> Vec<u8> {
    data_key(pointer, DATA_MARK)
}

/// Start of a data pointer's whole range (`TL_DATA pointer .. TL_DATA pointer+1`).
pub fn data_range_start(pointer: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(11);
    uint(&mut key, TL_DATA);
    uint(&mut key, pointer);
    key
}

pub fn data_range_end(pointer: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(11);
    uint(&mut key, TL_DATA);
    uint(&mut key, pointer + 1);
    key
}

/// `group.topic(topic)`.
pub fn group_topic(topic: &[u8; 32]) -> Vec<u8> {
    let mut key = Vec::with_capacity(33);
    uint(&mut key, TL_GROUP_BY_TOPIC);
    key.extend_from_slice(topic);
    key
}

/// `group.entry(group, timestamp, ptr)`.
pub fn group_entry(group: u64, timestamp: u64, pointer: u64) -> Vec<u8> {
    let mut key = Vec::new();
    uint(&mut key, TL_GROUP);
    uint(&mut key, group);
    key.extend_from_slice(&timestamp.to_be_bytes());
    uint(&mut key, pointer);
    key
}

/// Start of a group's update range.
pub fn group_entry_start(group: u64) -> Vec<u8> {
    let mut key = Vec::new();
    uint(&mut key, TL_GROUP);
    uint(&mut key, group);
    key
}

pub fn group_entry_end(group: u64) -> Vec<u8> {
    let mut key = Vec::new();
    uint(&mut key, TL_GROUP);
    uint(&mut key, group + 1);
    key
}

/// `core.blockIndex(buffer)` / `markKey` / `userDataKey` / `bitfieldIndexAndType`.
pub fn read_block_index(key: &[u8]) -> Option<u64> {
    let mut pos = 0;
    read_uint(key, &mut pos)?;
    read_uint(key, &mut pos)?;
    read_uint(key, &mut pos)?;
    read_uint(key, &mut pos)
}

pub fn read_bitfield_index(key: &[u8]) -> Option<(u64, u64)> {
    let mut pos = 0;
    read_uint(key, &mut pos)?;
    read_uint(key, &mut pos)?;
    read_uint(key, &mut pos)?;
    let index = read_uint(key, &mut pos)?;
    Some((index, read_uint(key, &mut pos)?))
}

pub fn read_user_data_key(key: &[u8]) -> Option<String> {
    let mut pos = 0;
    read_uint(key, &mut pos)?;
    read_uint(key, &mut pos)?;
    read_uint(key, &mut pos)?;
    read_string(key, &mut pos)
}

pub fn read_local_key(key: &[u8]) -> Option<Vec<u8>> {
    let mut pos = 0;
    read_uint(key, &mut pos)?;
    read_uint(key, &mut pos)?;
    read_uint(key, &mut pos)?;
    Some(key[pos..].to_vec())
}

/// `group.entry` decode: timestamp and the core pointer it names.
pub fn read_group_entry(key: &[u8]) -> Option<(u64, u64)> {
    let mut pos = 0;
    read_uint(key, &mut pos)?;
    read_uint(key, &mut pos)?;
    let end = pos + 8;
    let timestamp = u64::from_be_bytes(key.get(pos..end)?.try_into().ok()?);
    pos = end;
    Some((timestamp, read_uint(key, &mut pos)?))
}

/// Puts an `index-encoder` uint into a fresh key, for range bounds.
pub fn uint_key(n: u64) -> Vec<u8> {
    let mut key = Vec::new();
    uint(&mut key, n);
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uint_round_trips_at_boundaries() {
        for n in [
            0u64,
            1,
            0xfa,
            0xfb,
            0x100,
            0xffff,
            0x1_0000,
            u32::MAX as u64,
            u64::MAX,
        ] {
            let mut key = Vec::new();
            uint(&mut key, n);
            let mut pos = 0;
            assert_eq!(read_uint(&key, &mut pos), Some(n), "value {n}");
            assert_eq!(pos, key.len());
        }
        assert_eq!(read_uint(&[0xff], &mut 0), None);
    }

    #[test]
    fn strings_escape_zero_bytes() {
        let mut key = Vec::new();
        string(&mut key, "a\0b");
        let mut pos = 0;
        assert_eq!(read_string(&key, &mut pos).as_deref(), Some("a\0b"));
        assert_eq!(pos, key.len());
    }

    #[test]
    fn alias_key_matches_the_js_layout() {
        // keys.js:75 builds tag, namespace, name; the name is an index string.
        let mut key = core_by_alias(&[7; 32], "abc");
        assert_eq!(key[0], TL_CORE_BY_ALIAS as u8);
        assert_eq!(key.len(), 1 + 32 + 1 + 3 + 2);
        let (namespace, name) = read_alias(&key).unwrap();
        assert_eq!(namespace, [7; 32]);
        assert_eq!(name, "abc");
        key.truncate(33);
        assert_eq!(read_alias_discovery_key(&key), Some([7; 32]));
    }

    #[test]
    fn user_data_key_round_trips() {
        let key = data_user_data(3, "title");
        assert_eq!(read_user_data_key(&key).as_deref(), Some("title"));
        let key = data_user_data(300, "x");
        assert_eq!(read_user_data_key(&key).as_deref(), Some("x"));
    }

    #[test]
    fn group_entry_round_trips() {
        let key = group_entry(2, 1234, 5);
        assert_eq!(read_group_entry(&key), Some((1234, 5)));
    }
}
