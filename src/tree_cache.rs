use std::collections::HashMap;

use compact_encoding::hypercore::Node;

const CAPACITY: usize = 32768;
const NONE: usize = usize::MAX;
type Key = (u64, u64);

struct Entry {
    key: Key,
    node: Node,
    previous: usize,
    next: usize,
}

/// About 4 MiB per store; hits and full-cache replacements do not allocate.
pub(crate) struct TreeCache {
    indices: HashMap<Key, usize>,
    entries: Vec<Entry>,
    newest: usize,
    oldest: usize,
}

impl Default for TreeCache {
    fn default() -> Self {
        Self {
            indices: HashMap::with_capacity(CAPACITY),
            entries: Vec::with_capacity(CAPACITY),
            newest: NONE,
            oldest: NONE,
        }
    }
}

impl TreeCache {
    pub(crate) fn clear(&mut self) {
        self.indices.clear();
        self.entries.clear();
        self.newest = NONE;
        self.oldest = NONE;
    }

    fn touch(&mut self, slot: usize) {
        if self.newest == slot {
            return;
        }
        let Entry { previous, next, .. } = self.entries[slot];
        if previous != NONE {
            self.entries[previous].next = next;
        } else {
            self.oldest = next;
        }
        if next != NONE {
            self.entries[next].previous = previous;
        }
        if self.newest != NONE {
            self.entries[self.newest].next = slot;
        }
        self.entries[slot].previous = self.newest;
        self.entries[slot].next = NONE;
        self.newest = slot;
        if self.oldest == NONE {
            self.oldest = slot;
        }
    }

    pub(crate) fn get(&mut self, key: Key) -> Option<Node> {
        let slot = *self.indices.get(&key)?;
        self.touch(slot);
        Some(self.entries[slot].node)
    }

    pub(crate) fn put(&mut self, pointer: u64, node: Node) {
        let key = (pointer, node.index);
        if let Some(&slot) = self.indices.get(&key) {
            self.entries[slot].node = node;
            self.touch(slot);
            return;
        }
        if self.entries.len() == CAPACITY {
            let slot = self.oldest;
            self.indices.remove(&self.entries[slot].key);
            self.touch(slot);
            self.entries[slot].key = key;
            self.entries[slot].node = node;
            self.indices.insert(key, slot);
        } else {
            let slot = self.entries.len();
            self.entries.push(Entry {
                key,
                node,
                previous: self.newest,
                next: NONE,
            });
            if self.newest != NONE {
                self.entries[self.newest].next = slot;
            } else {
                self.oldest = slot;
            }
            self.newest = slot;
            self.indices.insert(key, slot);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(index: u64) -> Node {
        Node {
            index,
            size: index,
            hash: [0; 32],
        }
    }

    #[test]
    fn bounds_and_evicts_least_recently_used() {
        let mut cache = TreeCache::default();
        for i in 0..CAPACITY as u64 {
            cache.put(1, node(i));
        }
        assert_eq!(cache.get((1, 0)), Some(node(0)));
        cache.put(1, node(CAPACITY as u64));
        assert_eq!(cache.get((1, 1)), None);
        assert_eq!(cache.get((1, 0)), Some(node(0)));
        assert_eq!(cache.indices.len(), CAPACITY);
        assert_eq!(cache.entries.len(), CAPACITY);
        cache.put(2, node(0));
        assert_eq!(cache.get((2, 0)), Some(node(0)));
        let replacement = Node {
            hash: [1; 32],
            ..node(0)
        };
        cache.put(1, replacement);
        assert_eq!(cache.get((1, 0)), Some(replacement));
        cache.clear();
        assert_eq!(cache.get((1, 0)), None);
        cache.put(3, node(1));
        assert_eq!(cache.get((3, 1)), Some(node(1)));
    }
}
