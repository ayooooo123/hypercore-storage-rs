//! Row-level round trips, and the raw layout the JS side must find.

use bytes::Bytes;
use hypercore_storage::{
    Alias, Auth, Core, CoreHead, CreateCore, Hints, InfoOptions, KeyPair, Node, Store, keys,
};
use tempfile::tempdir;

fn discovery_key(n: u8) -> [u8; 32] {
    [n; 32]
}

fn core(key: u8, namespace: [u8; 32], name: &str) -> (CreateCore, Alias) {
    let alias = Alias {
        namespace,
        name: name.to_string(),
    };
    (
        CreateCore {
            key: [key; 32],
            discovery_key: discovery_key(key),
            alias: Some(alias.clone()),
            ..Default::default()
        },
        alias,
    )
}

#[test]
fn rows_are_where_js_puts_them() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let seed = [9u8; 32];
    assert_eq!(store.set_seed(seed).unwrap(), seed);

    let (create, alias) = core(1, [0; 32], "alpha");
    let storage = store.create_core(create).unwrap();
    assert_eq!(storage.core_pointer(), 0);
    assert_eq!(storage.data_pointer(), 0);

    let mut tx = storage.write();
    tx.put_block(0, Bytes::from_static(b"one"));
    tx.put_block(1, Bytes::from_static(b"two"));
    tx.put_tree_node(Node {
        index: 0,
        size: 1,
        hash: [1; 32],
    });
    tx.set_head(&CoreHead {
        fork: 0,
        length: 2,
        root_hash: [2; 32],
        signature: Some(Bytes::from_static(&[3; 64])),
        timestamp: 42,
    });
    tx.set_hints(&Hints {
        contiguous_length: 2,
        remote_contiguous_length: 2,
        recovering: 0,
    });
    tx.put_user_data("title", Some(Bytes::from_static(b"alpha core")));
    storage.flush(tx).unwrap();

    // Keys are index-encoder values: tag 0 head, tag 1 dk index, tag 2 alias,
    // tag 3 auth, tag 4 data.
    assert_eq!(keys::store_head(), vec![0]);
    let core_row = store
        .get(&keys::core_by_discovery_key(&discovery_key(1)))
        .unwrap()
        .unwrap();
    let decoded = Core::decode(core_row).unwrap();
    assert_eq!(decoded.version, 2);
    assert_eq!(decoded.alias.as_ref(), Some(&alias));

    assert_eq!(
        store.get(&keys::core_by_alias(&[0; 32], "alpha")).unwrap(),
        Some(Bytes::copy_from_slice(&discovery_key(1)))
    );
    assert_eq!(
        store
            .get(&keys::data_block(0, 1))
            .unwrap()
            .unwrap()
            .as_ref(),
        b"two"
    );

    let auth = Auth::decode(store.get(&keys::core_auth(0)).unwrap().expect("auth row")).unwrap();
    assert_eq!(auth.key, [1; 32]);
    assert_eq!(auth.discovery_key, discovery_key(1));

    // Reopen: everything is in the rows, not in memory.
    drop(store);
    let store = Store::open_read_only(dir.path()).unwrap();
    assert_eq!(store.seed().unwrap(), Some(seed));
    assert_eq!(store.cores().unwrap(), vec![discovery_key(1)]);
    let storage = store.resume_core(&discovery_key(1)).unwrap().unwrap();
    assert_eq!(storage.get_block(0).unwrap().as_deref(), Some(&b"one"[..]));
    assert_eq!(storage.get_head().unwrap().unwrap().length, 2);
    assert_eq!(storage.get_head().unwrap().unwrap().timestamp, 42);
    assert_eq!(storage.get_hints().unwrap().unwrap().contiguous_length, 2);
    assert_eq!(
        storage.get_user_data("title").unwrap().as_deref(),
        Some(&b"alpha core"[..])
    );
    assert_eq!(storage.tree_nodes().unwrap().len(), 1);

    let infos = store
        .get_infos(
            &[discovery_key(1), discovery_key(2)],
            InfoOptions::default(),
        )
        .unwrap();
    assert!(infos[0].is_some());
    assert!(infos[1].is_none());
    assert_eq!(
        infos[0].as_ref().unwrap().auth.as_ref().unwrap().key,
        [1; 32]
    );

    assert_eq!(store.get_alias(&alias).unwrap(), Some(discovery_key(1)));
    assert!(store.has_core(&discovery_key(1)).unwrap());
}

#[test]
fn counters_and_groups_follow_the_head() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let (a, _) = core(1, [0; 32], "a");
    let (b, _) = core(2, [0; 32], "b");
    store.create_core(a).unwrap();
    store.create_core(b).unwrap();

    let head = store.head().unwrap();
    assert_eq!((head.cores, head.datas), (2, 2));

    let topic = [5u8; 32];
    let group = store.create_group(&topic).unwrap();
    assert_eq!(group, 0);
    assert_eq!(store.create_group(&topic).unwrap(), 0, "allocated once");
    assert_eq!(store.head().unwrap().groups, 1);

    store.put_group_update(group, 100, 0, &[1; 32]).unwrap();
    store.put_group_update(group, 200, 1, &[2; 32]).unwrap();
    let updates = store.group_updates(group, 100).unwrap();
    assert_eq!(
        updates
            .iter()
            .map(|row| (row.timestamp, row.key))
            .collect::<Vec<_>>(),
        vec![(200, [2u8; 32]), (100, [1u8; 32])],
        "newest first, at or after since"
    );
    assert_eq!(
        updates
            .iter()
            .map(|row| row.core_pointer)
            .collect::<Vec<_>>(),
        vec![1, 0],
        "each row keeps the core pointer it was written with"
    );
    assert_eq!(store.group_updates(group, 150).unwrap().len(), 1);
}

#[test]
fn sessions_share_the_core_and_keep_their_own_pointer() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let (create, _) = core(1, [0; 32], "a");
    let parent = store.create_core(create).unwrap();

    let mut tx = parent.write();
    tx.put_block(0, Bytes::from_static(b"genesis"));
    tx.set_head(&CoreHead {
        length: 1,
        ..Default::default()
    });
    parent.flush(tx).unwrap();

    let session = parent.create_session("copy", None).unwrap();
    assert_eq!(session.core_pointer(), parent.core_pointer());
    assert_ne!(session.data_pointer(), parent.data_pointer());
    assert_eq!(session.dependencies().len(), 1);
    assert_eq!(
        session.get_block(0).unwrap().as_deref(),
        Some(&b"genesis"[..]),
        "block comes from the parent pointer"
    );
    assert_eq!(session.get_head().unwrap().unwrap().length, 1);

    let mut tx = session.write();
    tx.put_block(1, Bytes::from_static(b"session"));
    session.flush(tx).unwrap();
    assert_eq!(
        session.get_block(1).unwrap().as_deref(),
        Some(&b"session"[..])
    );

    // A fresh handle resumes the dependency chain from the rows.
    drop(parent);
    drop(session);
    store.close();
    let store = Store::open(dir.path()).unwrap();
    let parent = store.resume_core(&discovery_key(1)).unwrap().unwrap();
    let sessions = parent.get_sessions().unwrap();
    assert_eq!(sessions.len(), 1);
    let resumed = store
        .resume_core(&discovery_key(1))
        .unwrap()
        .unwrap()
        .resume_session("copy")
        .unwrap()
        .unwrap();
    assert_eq!(resumed.dependencies().len(), 1);
    assert_eq!(
        resumed.get_block(0).unwrap().as_deref(),
        Some(&b"genesis"[..])
    );
}

#[test]
fn read_only_refuses_writes_and_delete_core_clears_rows() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let (create, alias) = core(1, [0; 32], "a");
    let storage = store.create_core(create).unwrap();
    let mut tx = storage.write();
    tx.put_block(0, Bytes::from_static(b"x"));
    storage.flush(tx).unwrap();
    store.close();

    let read_only = Store::open_read_only(dir.path()).unwrap();
    let mut tx = read_only
        .resume_core(&discovery_key(1))
        .unwrap()
        .unwrap()
        .write();
    tx.put_block(1, Bytes::from_static(b"y"));
    assert!(
        read_only
            .resume_core(&discovery_key(1))
            .unwrap()
            .unwrap()
            .flush(tx)
            .is_err()
    );
    drop(read_only);

    let store = Store::open(dir.path()).unwrap();
    store.delete_core(&discovery_key(1)).unwrap();
    assert!(store.cores().unwrap().is_empty());
    assert_eq!(store.get_alias(&alias).unwrap(), None);
    assert!(store.get(&keys::core_auth(0)).unwrap().is_none());
    assert!(store.get(&keys::data_block(0, 0)).unwrap().is_none());
}

#[test]
fn a_key_pair_survives_and_rebuilds_the_public_half() {
    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let mut secret = [0u8; 64];
    secret[..32].fill(7);
    secret[32..].fill(8);
    let (create, _) = core(1, [0; 32], "a");
    store
        .create_core(CreateCore {
            key_pair: Some(KeyPair {
                public_key: [8; 32],
                secret_key: Some(secret),
            }),
            ..create
        })
        .unwrap();
    let auth = store.get_auth(&discovery_key(1)).unwrap().unwrap();
    let pair = auth.key_pair.unwrap();
    assert_eq!(pair.secret_key, Some(secret));
    assert_eq!(pair.public_key, [8; 32]);
}

/// `gcStorage`'s compaction: a cache wipe must give the bytes back now, not after the next
/// size-triggered compaction. Blob rows live in `.blob` files that only a compaction unlinks.
#[test]
fn compaction_gives_back_the_bytes_a_clear_tombstoned() {
    fn dir_bytes(dir: &std::path::Path) -> u64 {
        std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|entry| {
                let path = entry.path();
                if path.is_dir() {
                    dir_bytes(&path)
                } else {
                    std::fs::metadata(&path).unwrap().len()
                }
            })
            .sum()
    }

    let dir = tempdir().unwrap();
    let store = Store::open(dir.path()).unwrap();
    let (create, _) = core(1, [0; 32], "a");
    let storage = store.create_core(create).unwrap();
    let block = Bytes::from(vec![7u8; 512 * 1024]);
    let blocks = 32u64;
    let mut tx = storage.write();
    for index in 0..blocks {
        tx.put_block(index, block.clone());
    }
    tx.set_head(&CoreHead {
        fork: 0,
        length: blocks,
        root_hash: [2; 32],
        signature: None,
        timestamp: 1,
    });
    storage.flush(tx).unwrap();
    store.flush().unwrap();
    let written = dir_bytes(dir.path());
    assert!(
        written >= 16 * 1024 * 1024,
        "16 MiB of blocks must be on disk before the clear, got {written}"
    );

    let mut tx = storage.write();
    tx.delete_block_range(0, blocks);
    storage.flush(tx).unwrap();
    store.flush().unwrap();
    store.compact(false).unwrap();
    let after = dir_bytes(dir.path());
    assert!(
        written.saturating_sub(after) >= 12 * 1024 * 1024,
        "compaction reclaimed {} of {written} bytes",
        written.saturating_sub(after)
    );
}
