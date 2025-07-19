use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub struct ShardManager {
    shards: Vec<String>,
}

impl ShardManager {
    pub fn new(shards: Vec<String>) -> Self {
        Self { shards }
    }

    pub fn get_shard<K: Hash>(&self, key: &K) -> &String {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        &self.shards[(hash % self.shards.len() as u64) as usize]
    }
}
