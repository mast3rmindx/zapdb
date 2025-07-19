use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Instant, Duration};
use std::fs::File;
use std::io::{self, Write, Read};
use serde::{Serialize, Deserialize};
use flate2::write::GzEncoder;
use flate2::read::GzDecoder;
use flate2::Compression;
use tokio::sync::RwLock;
use std::sync::Arc;
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use rand::{rngs::OsRng, RngCore};
use rs_merkle::{MerkleTree, Hasher as MerkleHasher};

#[derive(Clone)]
struct Blake3Hasher;

impl MerkleHasher for Blake3Hasher {
    type Hash = [u8; 32];

    fn hash(data: &[u8]) -> [u8; 32] {
        blake3::hash(data).into()
    }
}


#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

impl Column {
    pub fn new(name: String, data_type: DataType) -> Self {
        Column { name, data_type }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DataType {
    Integer,
    String,
    Float,
    Boolean,
}

use std::collections::BTreeMap;

#[derive(Clone, Serialize, Deserialize)]
pub struct Table {
    name: String,
    columns: Vec<Column>,
    data: Vec<HashMap<String, Value>>,
    indexes: HashMap<String, BTreeMap<Value, Vec<usize>>>,
    #[serde(skip)]
    merkle_tree: Option<MerkleTree<Blake3Hasher>>,
}

impl Table {
    fn build_merkle_tree(&mut self) {
        let mut leaves = Vec::new();
        for row in &self.data {
            let encoded_row = bincode::serialize(&row).unwrap();
            leaves.push(Blake3Hasher::hash(&encoded_row));
        }
        self.merkle_tree = Some(MerkleTree::<Blake3Hasher>::from_leaves(&leaves));
    }

    pub fn verify_integrity(&self) -> bool {
        if let Some(tree) = &self.merkle_tree {
            let mut leaves = Vec::new();
            for row in &self.data {
                let encoded_row = bincode::serialize(&row).unwrap();
                leaves.push(Blake3Hasher::hash(&encoded_row));
            }
            let new_tree = MerkleTree::<Blake3Hasher>::from_leaves(&leaves);
            tree.root() == new_tree.root()
        } else {
            true
        }
    }
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Value {
    Integer(i64),
    String(String),
    Float(f64),
    Boolean(bool),
    Null,
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => (a - b).abs() < f64::EPSILON,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}


#[derive(Clone, Debug)]
pub enum Operator {
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Clone, Debug)]
pub struct Condition {
    pub column: String,
    pub operator: Operator,
    pub value: Value,
}

#[derive(Clone, Debug)]
pub enum Query {
    MatchAll,
    Condition(Condition),
    And(Vec<Query>),
    Or(Vec<Query>),
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Value::Integer(i) => i.hash(state),
            Value::String(s) => s.hash(state),
            Value::Float(f) => {
                let bits = f.to_bits();
                bits.hash(state);
            }
            Value::Boolean(b) => b.hash(state),
            Value::Null => 0.hash(state),
        }
    }
}

pub struct Database {
    pub tables: Arc<RwLock<HashMap<String, Table>>>,
    key: [u8; 32],
}

impl Database {
    pub fn new(key: [u8; 32]) -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            key,
        }
    }
    pub async fn save(&self, path: &str) -> io::Result<()> {
        let start = Instant::now();
        let tables = self.tables.read().await;
        let encoded: Vec<u8> =
            bincode::serialize(&*tables).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&encoded)?;
        let compressed_data = encoder.finish()?;

        let cipher = Aes256Gcm::new((&self.key).into());
        let mut nonce_bytes = [0u8; 12];
        OsRng.fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = cipher
            .encrypt(nonce, compressed_data.as_slice())
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;

        let mut file = File::create(path)?;
        file.write_all(&nonce)?;
        file.write_all(&ciphertext)?;
        println!("Database saved in {:?}", start.elapsed());
        Ok(())
    }

    pub async fn load(&mut self, path: &str) -> io::Result<()> {
        let start = Instant::now();
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let cipher = Aes256Gcm::new((&self.key).into());
        let nonce = Nonce::from_slice(&buffer[..12]);
        let ciphertext = &buffer[12..];

        let decrypted_data = cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;

        let mut decoder = GzDecoder::new(&decrypted_data[..]);
        let mut decompressed_data = Vec::new();
        decoder.read_to_end(&mut decompressed_data)?;

        let tables: HashMap<String, Table> = bincode::deserialize(&decompressed_data)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut self_tables = self.tables.write().await;
        *self_tables = tables;
        for table in self_tables.values_mut() {
            table.build_merkle_tree();
        }
        println!("Database loaded in {:?}", start.elapsed());
        Ok(())
    }
    pub async fn create_table(
        &mut self,
        name: String,
        columns: Vec<Column>,
    ) -> Result<Duration, String> {
        let start = Instant::now();
        let mut tables = self.tables.write().await;
        if tables.contains_key(&name) {
            return Err(format!("Table {} already exists", name));
        }
        tables.insert(
            name.clone(),
            Table {
                name,
                columns,
                data: Vec::new(),
                indexes: HashMap::new(),
                merkle_tree: None,
            },
        );
        Ok(start.elapsed())
    }

    pub async fn create_index(&mut self, table_name: &str, column_name: &str) -> Result<(), String> {
        let mut tables = self.tables.write().await;
        let table = tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table {} not found", table_name))?;

        if !table.columns.iter().any(|c| c.name == column_name) {
            return Err(format!("Column {} not found", column_name));
        }

        let mut index = BTreeMap::new();
        for (i, row) in table.data.iter().enumerate() {
            if let Some(value) = row.get(column_name) {
                index.entry(value.clone()).or_insert_with(Vec::new).push(i);
            }
        }

        table.indexes.insert(column_name.to_string(), index);
        Ok(())
    }

    pub async fn insert(
        &mut self,
        table_name: &str,
        row: HashMap<String, Value>,
    ) -> Result<Duration, String> {
        let start = Instant::now();
        let mut tables = self.tables.write().await;
        let table = tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table {} not found", table_name))?;

        for col in &table.columns {
            if !row.contains_key(&col.name) {
                return Err(format!("Missing column: {}", col.name));
            }
        }

        let new_index = table.data.len();
        for (col_name, index) in &mut table.indexes {
            if let Some(value) = row.get(col_name) {
                index.entry(value.clone()).or_insert_with(Vec::new).push(new_index);
            }
        }

        table.data.push(row);
        table.build_merkle_tree();
        Ok(start.elapsed())
    }

    pub async fn select(
        &self,
        table_name: &str,
        query: &Query,
    ) -> Result<(Vec<HashMap<String, Value>>, Duration), String> {
        let start = Instant::now();
        let tables = self.tables.read().await;
        let table = tables
            .get(table_name)
            .ok_or_else(|| format!("Table {} not found", table_name))?;

        let results = self.execute_query(table, query);

        let rows = results.into_iter().map(|i| table.data[i].clone()).collect();

        Ok((rows, start.elapsed()))
    }

    fn execute_query(&self, table: &Table, query: &Query) -> Vec<usize> {
        match query {
            Query::MatchAll => (0..table.data.len()).collect(),
            Query::Condition(condition) => {
                if let Some(index) = table.indexes.get(&condition.column) {
                    let mut results = Vec::new();
                    match condition.operator {
                        Operator::Eq => {
                            if let Some(indices) = index.get(&condition.value) {
                                results.extend(indices);
                            }
                        }
                        Operator::NotEq => {
                            for (key, indices) in index.iter() {
                                if *key != condition.value {
                                    results.extend(indices);
                                }
                            }
                        }
                        Operator::Gt => {
                            for (_key, indices) in index.range(condition.value.clone()..) {
                                if *_key > condition.value {
                                    results.extend(indices);
                                }
                            }
                        }
                        Operator::Gte => {
                            for (_key, indices) in index.range(condition.value.clone()..) {
                                results.extend(indices);
                            }
                        }
                        Operator::Lt => {
                            for (key, indices) in index.range(..condition.value.clone()) {
                                if *key < condition.value {
                                    results.extend(indices);
                                }
                            }
                        }
                        Operator::Lte => {
                            for (_key, indices) in index.range(..=condition.value.clone()) {
                                results.extend(indices);
                            }
                        }
                    }
                    results
                } else {
                    (0..table.data.len())
                        .filter(|i| self.evaluate_condition(&table.data[*i], condition))
                        .collect()
                }
            }
            Query::And(queries) => {
                if queries.is_empty() {
                    return (0..table.data.len()).collect();
                }
                let mut result_sets: Vec<Vec<usize>> = queries
                    .iter()
                    .map(|q| self.execute_query(table, q))
                    .collect();

                result_sets.sort_by_key(|a| a.len());

                let mut final_result = result_sets[0].clone();
                for i in 1..result_sets.len() {
                    let other_set: std::collections::HashSet<usize> = result_sets[i].iter().cloned().collect();
                    final_result.retain(|item| other_set.contains(item));
                }
                final_result
            }
            Query::Or(queries) => {
                let mut final_result = std::collections::HashSet::new();
                for q in queries {
                    let result_set = self.execute_query(table, q);
                    final_result.extend(result_set);
                }
                final_result.into_iter().collect()
            }
        }
    }

    fn evaluate_condition(&self, row: &HashMap<String, Value>, condition: &Condition) -> bool {
        if let Some(value) = row.get(&condition.column) {
            match condition.operator {
                Operator::Eq => value == &condition.value,
                Operator::NotEq => value != &condition.value,
                Operator::Gt => value > &condition.value,
                Operator::Gte => value >= &condition.value,
                Operator::Lt => value < &condition.value,
                Operator::Lte => value <= &condition.value,
            }
        } else {
            false
        }
    }

    pub async fn update<U>(
        &mut self,
        table_name: &str,
        query: &Query,
        update_fn: U,
    ) -> Result<usize, String>
    where
        U: Fn(&mut HashMap<String, Value>),
    {
        let mut tables = self.tables.write().await;
        let table = tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table {} not found", table_name))?;

        let indices_to_update = self.execute_query(table, query);
        let updated_count = indices_to_update.len();

        for index in indices_to_update {
            update_fn(&mut table.data[index]);
        }

        if updated_count > 0 {
            for (col_name, index) in &mut table.indexes {
                let mut new_index = BTreeMap::new();
                for (i, row) in table.data.iter().enumerate() {
                    if let Some(value) = row.get(col_name) {
                        new_index.entry(value.clone()).or_insert_with(Vec::new).push(i);
                    }
                }
                *index = new_index;
            }
            table.build_merkle_tree();
        }

        Ok(updated_count)
    }

    pub async fn delete(&mut self, table_name: &str, query: &Query) -> Result<usize, String> {
        let mut tables = self.tables.write().await;
        let table = tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table {} not found", table_name))?;

        let indices_to_delete = self.execute_query(table, query);
        let deleted_count = indices_to_delete.len();

        let indices_to_delete_set: std::collections::HashSet<usize> =
            indices_to_delete.into_iter().collect();

        let mut new_data = Vec::new();
        for (i, row) in table.data.iter().enumerate() {
            if !indices_to_delete_set.contains(&i) {
                new_data.push(row.clone());
            }
        }
        table.data = new_data;

        if deleted_count > 0 {
            for (col_name, index) in &mut table.indexes {
                let mut new_index = BTreeMap::new();
                for (i, row) in table.data.iter().enumerate() {
                    if let Some(value) = row.get(col_name) {
                        new_index.entry(value.clone()).or_insert_with(Vec::new).push(i);
                    }
                }
                *index = new_index;
            }
            table.build_merkle_tree();
        }

        Ok(deleted_count)
    }

    pub async fn verify_integrity(&self) -> bool {
        let tables = self.tables.read().await;
        for table in tables.values() {
            if !table.verify_integrity() {
                return false;
            }
        }
        true
    }
}
