//! Row values: one struct per message in hypercore-storage's hyperschema.
//!
//! Values are the bare schema encoding, with no length or version prefix except
//! the store head, whose versioned type carries its own version byte.

use bytes::Bytes;
use compact_encoding::hypercore::{Manifest, Node};
use compact_encoding::{Codec, Reader, Writer};

use crate::encoding::{core as core_schema, corestore as store_schema};
use crate::{Error, Result};

/// The store-level head (`@corestore/head`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StoreHead {
    pub cores: u64,
    pub datas: u64,
    pub groups: u64,
    /// The corestore primary key, stored as the head's `seed`.
    pub seed: Option<[u8; 32]>,
    pub default_discovery_key: Option<[u8; 32]>,
}

impl StoreHead {
    pub fn encode(&self) -> Bytes {
        let head = store_schema::HeadV2 {
            cores: self.cores,
            datas: self.datas,
            groups: self.groups,
            seed: self.seed,
            default_discovery_key: self.default_discovery_key,
        };
        compact_encoding::encode(&store_schema::Head::V2(head))
    }

    pub fn decode(bytes: Bytes) -> Result<Self> {
        match store_schema::Head::decode(&mut Reader::new(bytes))? {
            store_schema::Head::V2(h) => Ok(Self {
                cores: h.cores,
                datas: h.datas,
                groups: h.groups,
                seed: h.seed,
                default_discovery_key: h.default_discovery_key,
            }),
            // `headLegacyMap` (encoding/external.js): the counters moved out of
            // `allocated` and `groups` is new.
            store_schema::Head::V1(h) => Ok(Self {
                cores: h.allocated.as_ref().map_or(0, |a| a.cores),
                datas: h.allocated.as_ref().map_or(0, |a| a.datas),
                groups: 0,
                seed: h.seed,
                default_discovery_key: h.default_discovery_key,
            }),
        }
    }
}

/// A core's signed head (`@core/head`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CoreHead {
    pub fork: u64,
    pub length: u64,
    pub root_hash: [u8; 32],
    pub signature: Option<Bytes>,
    /// Epoch milliseconds, as JS writes `Date.now()`; 0 when absent.
    pub timestamp: u64,
}

impl CoreHead {
    pub fn encode(&self) -> Bytes {
        compact_encoding::encode(&core_schema::Head {
            fork: self.fork,
            length: self.length,
            root_hash: self.root_hash,
            signature: self.signature.clone(),
            timestamp: Some(self.timestamp),
        })
    }

    pub fn decode(bytes: Bytes) -> Result<Self> {
        let head = core_schema::Head::decode(&mut Reader::new(bytes))?;
        Ok(Self {
            fork: head.fork,
            length: head.length,
            root_hash: head.root_hash,
            signature: head.signature,
            timestamp: head.timestamp.unwrap_or(0),
        })
    }
}

/// Storage hints (`@core/hints`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Hints {
    pub contiguous_length: u64,
    pub remote_contiguous_length: u64,
    /// The JS store's `recovering` stamp (hypercore-rs uses it as its `frozen` flag).
    pub recovering: u64,
}

impl Hints {
    pub fn encode(&self) -> Bytes {
        compact_encoding::encode(&core_schema::Hints {
            contiguous_length: Some(self.contiguous_length),
            remote_contiguous_length: Some(self.remote_contiguous_length),
            recovering: Some(self.recovering),
        })
    }

    pub fn decode(bytes: Bytes) -> Result<Self> {
        let hints = core_schema::Hints::decode(&mut Reader::new(bytes))?;
        Ok(Self {
            contiguous_length: hints.contiguous_length.unwrap_or(0),
            remote_contiguous_length: hints.remote_contiguous_length.unwrap_or(0),
            recovering: hints.recovering.unwrap_or(0),
        })
    }
}

/// The ed25519 key pair in a core's auth row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyPair {
    pub public_key: [u8; 32],
    pub secret_key: Option<[u8; 64]>,
}

/// A core's auth row (`@core/auth`).
///
/// Written by hand: the manifest is framed and uses hypercore's manifest codec
/// (with its version-0 compatibility cases), not the plain schema struct.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Auth {
    pub key: [u8; 32],
    pub discovery_key: [u8; 32],
    pub manifest: Option<Manifest>,
    pub key_pair: Option<KeyPair>,
    pub encryption_key: Option<Bytes>,
}

impl Auth {
    pub fn encode(&self) -> Bytes {
        let mut w = Writer::new();
        w.raw(&self.key);
        w.raw(&self.discovery_key);
        let flags = self.manifest.is_some() as u64
            | (self.key_pair.is_some() as u64) << 1
            | (self.encryption_key.is_some() as u64) << 2;
        w.uint(flags);
        if let Some(manifest) = &self.manifest {
            w.frame(manifest);
        }
        if let Some(pair) = &self.key_pair {
            w.buffer(&pair.public_key);
            w.optional_buffer(pair.secret_key.as_ref().map(|k| &k[..]));
        }
        if let Some(key) = &self.encryption_key {
            w.buffer(key);
        }
        w.finish()
    }

    pub fn decode(bytes: Bytes) -> Result<Self> {
        let mut r = Reader::new(bytes);
        let key = r.fixed()?;
        let discovery_key = r.fixed()?;
        let flags = r.uint()?;
        let manifest = if flags & 1 != 0 {
            Some(r.frame::<Manifest>()?)
        } else {
            None
        };
        let key_pair = if flags & 2 != 0 {
            let public_key: [u8; 32] = r.buffer()?[..]
                .try_into()
                .map_err(|_| Error::Corrupt("core key pair public key is not 32 bytes".into()))?;
            let secret_key = match r.optional_buffer()? {
                Some(secret) => Some(secret[..].try_into().map_err(|_| {
                    Error::Corrupt("core key pair secret key is not 64 bytes".into())
                })?),
                None => None,
            };
            Some(KeyPair {
                public_key,
                secret_key,
            })
        } else {
            None
        };
        let encryption_key = if flags & 4 != 0 {
            Some(r.buffer()?)
        } else {
            None
        };
        Ok(Self {
            key,
            discovery_key,
            manifest,
            key_pair,
            encryption_key,
        })
    }
}

/// A `(namespace, name)` alias.
#[derive(Debug, Clone, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct Alias {
    pub namespace: [u8; 32],
    pub name: String,
}

/// A corestore core row (`@corestore/core`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Core {
    pub version: u64,
    pub core_pointer: u64,
    pub data_pointer: u64,
    pub alias: Option<Alias>,
}

impl Core {
    pub fn encode(&self) -> Bytes {
        compact_encoding::encode(&store_schema::Core {
            version: self.version,
            core_pointer: self.core_pointer,
            data_pointer: self.data_pointer,
            alias: self.alias.as_ref().map(|a| store_schema::Alias {
                name: a.name.clone(),
                namespace: a.namespace,
            }),
        })
    }

    pub fn decode(bytes: Bytes) -> Result<Self> {
        let core = store_schema::Core::decode(&mut Reader::new(bytes))?;
        Ok(Self {
            version: core.version,
            core_pointer: core.core_pointer,
            data_pointer: core.data_pointer,
            alias: core.alias.map(|a| Alias {
                namespace: a.namespace,
                name: a.name,
            }),
        })
    }
}

/// A session dependency (`@core/dependency`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Dependency {
    pub data_pointer: u64,
    pub length: u64,
}

impl Dependency {
    pub fn encode(&self) -> Bytes {
        compact_encoding::encode(&core_schema::Dependency {
            data_pointer: self.data_pointer,
            length: self.length,
        })
    }

    pub fn decode(bytes: Bytes) -> Result<Self> {
        let dependency = core_schema::Dependency::decode(&mut Reader::new(bytes))?;
        Ok(Self {
            data_pointer: dependency.data_pointer,
            length: dependency.length,
        })
    }
}

/// A named session (`@core/session`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    pub name: String,
    pub data_pointer: u64,
}

impl Session {
    pub fn encode_list(sessions: &[Session]) -> Bytes {
        let list: Vec<core_schema::Session> = sessions
            .iter()
            .map(|s| core_schema::Session {
                name: s.name.clone(),
                data_pointer: s.data_pointer,
            })
            .collect();
        let mut w = Writer::new();
        w.array(&list);
        w.finish()
    }

    pub fn decode_list(bytes: Bytes) -> Result<Vec<Session>> {
        let list: Vec<core_schema::Session> = Reader::new(bytes).array()?;
        Ok(list
            .into_iter()
            .map(|s| Session {
                name: s.name,
                data_pointer: s.data_pointer,
            })
            .collect())
    }
}

/// The group a core belongs to (`@core/group`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CoreGroup {
    pub key: Option<[u8; 32]>,
}

impl CoreGroup {
    pub fn encode(&self) -> Bytes {
        compact_encoding::encode(&core_schema::Group {
            key: self.key,
            pointer: None,
        })
    }

    pub fn decode(bytes: Bytes) -> Result<Self> {
        Ok(Self {
            key: core_schema::Group::decode(&mut Reader::new(bytes))?.key,
        })
    }
}

/// A group update (`TL_GROUP` row value: the core key).
pub type GroupKey = [u8; 32];

/// Everything `getInfos` returns for one discovery key.
#[derive(Debug, Clone)]
pub struct Info {
    pub discovery_key: [u8; 32],
    pub core: Core,
    pub auth: Option<Auth>,
    pub head: Option<CoreHead>,
    pub hints: Option<Hints>,
}

/// Encodes a tree node exactly as `@core/tree-node` does.
pub fn encode_tree_node(node: &Node) -> Bytes {
    compact_encoding::encode(node)
}

pub fn decode_tree_node(bytes: Bytes) -> Result<Node> {
    Ok(Node::decode(&mut Reader::new(bytes))?)
}
