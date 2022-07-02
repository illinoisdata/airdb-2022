use std::{collections::HashMap};

use crate::{db::rw_db::Key, storage::segment::Entry};

pub struct EntryCache {
    is_full: bool,
    upper_bound: u64,
    entries: HashMap<Key, Entry>,
}

impl EntryCache {
    pub fn new() -> Self {
        Self {
            is_full: false,
            upper_bound: 0,
            entries: HashMap::new(),
        }
    }

    pub fn extend(&mut self, map: HashMap<Key, Entry>) {
        self.entries.extend(map)

    }

    pub fn get_upper_bound(&self) -> u64 {
        self.upper_bound
    }

    pub fn is_empty(&self) -> bool {
        self.upper_bound == 0
    }

    pub fn is_full(&self) -> bool {
        self.is_full
    }

    pub fn set_full(&mut self) {
        self.is_full = true;
    }

    pub fn set_upper_bound(&mut self, upper_bound_new: u64) {
        self.upper_bound = upper_bound_new;
    }

    pub fn contains(&self, key: &Key) -> bool {
        self.entries.contains_key(key)
    }

    pub fn insert(&mut self, k: Key, v: Entry) -> Option<Entry> {
        self.entries.insert(k, v)
    }

    pub fn get(&self, search_key: &[u8]) -> Option<Entry> {
        self.entries.get(search_key).cloned()
    }

    // pub fn update_cache
}

impl Default for EntryCache {
    fn default() -> Self {
        Self::new()
    }
}
