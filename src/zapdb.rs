use std::collections::HashMap;
use std::fs::File;
use std::io::{self, Write};
use tokio;
pub struct ZapDB {
    data: HashMap<String, String>,
}

impl ZapDB {
    pub fn new() -> Self {
        ZapDB {
            data: HashMap::with_capacity(100),
        }
    }

    pub fn insert(&mut self, key: String, value: String) {
        self.data.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.data.get(key)
    }

    pub fn delete(&mut self, key: &str) {
        self.data.remove(key);
    }

    pub async fn save(&self, path: &str) -> io::Result<()> {
        let encoded: Vec<u8> = bincode::serialize(&self.data).unwrap();
        let mut file = File::create(path)?;
        file.write_all(&encoded)?;
        Ok(())
    }

    pub async fn load(&mut self, path: &str) -> io::Result<()> {
        let file = File::open(path)?;
        let decoded: HashMap<String, String> = bincode::deserialize_from(file).unwrap();
        self.data = decoded;
        Ok(())
    }
}