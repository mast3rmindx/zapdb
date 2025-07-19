use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::{Instant, Duration};
use std::fs::File;
use std::io::{self, Write, Read, BufWriter};
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
use chrono::{DateTime, Utc};
use uuid::Uuid;
use serde_json;

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
    DateTime,
    Uuid,
    Json,
}

use std::collections::BTreeMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Operation {
    Insert {
        table_name: String,
        row: HashMap<String, Value>,
    },
    Update {
        table_name: String,
        query: Query,
        // update_fn is not serializable, so we'll handle it differently
    },
    Delete {
        table_name: String,
        query: Query,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum WalEntry {
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    Insert {
        table_name: String,
        row: HashMap<String, Value>,
    },
    Update {
        table_name: String,
        query: Query,
        // update_fn is not serializable, so we'll handle it differently
    },
    Delete {
        table_name: String,
        query: Query,
    },
}

#[derive(Clone)]
pub struct Transaction {
    operations: Vec<(Operation, Option<fn(&mut HashMap<String, Value>)>)>,
}

impl Transaction {
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    pub fn insert(&mut self, table_name: String, row: HashMap<String, Value>) {
        self.operations
            .push((Operation::Insert { table_name, row }, None));
    }

    pub fn update(
        &mut self,
        table_name: String,
        query: Query,
        update_fn: fn(&mut HashMap<String, Value>),
    ) {
        self.operations.push((
            Operation::Update {
                table_name,
                query,
            },
            Some(update_fn),
        ));
    }

    pub fn delete(&mut self, table_name: String, query: Query) {
        self.operations
            .push((Operation::Delete { table_name, query }, None));
    }
}

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
    DateTime(DateTime<Utc>),
    Uuid(Uuid),
    Json(serde_json::Value),
    Null,
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => (a - b).abs() < f64::EPSILON,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::DateTime(a), Value::DateTime(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b)) => a == b,
            (Value::Json(a), Value::Json(b)) => a == b,
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
            (Value::DateTime(a), Value::DateTime(b)) => a.partial_cmp(b),
            (Value::Uuid(a), Value::Uuid(b)) => a.partial_cmp(b),
            _ => None,
        }
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap_or(std::cmp::Ordering::Equal)
    }
}


#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Operator {
    Eq,
    NotEq,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Condition {
    pub column: String,
    pub operator: Operator,
    pub value: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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
            Value::DateTime(dt) => dt.hash(state),
            Value::Uuid(u) => u.hash(state),
            Value::Json(j) => {
                let s = serde_json::to_string(j).unwrap_or_default();
                s.hash(state);
            }
            Value::Null => 0.hash(state),
        }
    }
}

pub struct WalWriter {
    writer: BufWriter<File>,
}

impl WalWriter {
    pub fn new(path: &str) -> io::Result<Self> {
        let file = File::options().append(true).create(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    pub fn log(&mut self, entry: &WalEntry) -> io::Result<()> {
        let encoded: Vec<u8> = bincode::serialize(entry).unwrap();
        self.writer.write_all(&encoded)?;
        self.writer.flush()?;
        Ok(())
    }
}

pub struct Database {
    pub tables: Arc<RwLock<HashMap<String, Table>>>,
    key: [u8; 32],
    wal_writer: Arc<RwLock<WalWriter>>,
    wal_path: String,
}

impl Database {
    pub fn new(key: [u8; 32], wal_path: &str) -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashMap::new())),
            key,
            wal_writer: Arc::new(RwLock::new(WalWriter::new(wal_path).unwrap())),
            wal_path: wal_path.to_string(),
        }
    }

    pub fn begin_transaction(&self) -> Transaction {
        Transaction::new()
    }

    pub async fn commit(&mut self, transaction: Transaction) -> Result<(), String> {
        let mut wal_writer = self.wal_writer.write().await;
        for (op, _) in &transaction.operations {
            let wal_entry = match op {
                Operation::Insert { table_name, row } => WalEntry::Insert {
                    table_name: table_name.clone(),
                    row: row.clone(),
                },
                Operation::Update { table_name, query } => WalEntry::Update {
                    table_name: table_name.clone(),
                    query: query.clone(),
                },
                Operation::Delete { table_name, query } => WalEntry::Delete {
                    table_name: table_name.clone(),
                    query: query.clone(),
                },
            };
            wal_writer.log(&wal_entry).map_err(|e| e.to_string())?;
        }

        let mut tables = self.tables.write().await;
        let original_tables = tables.clone();

        for (op, update_fn) in transaction.operations {
            let result = match op {
                Operation::Insert { table_name, row } => {
                    self.insert_internal(&mut tables, &table_name, row)
                }
                Operation::Update { table_name, query } => self
                    .update_internal(&mut tables, &table_name, &query, update_fn.unwrap())
                    .map(|_| ()),
                Operation::Delete { table_name, query } => self
                    .delete_internal(&mut tables, &table_name, &query)
                    .map(|_| ()),
            };
            if result.is_err() {
                *tables = original_tables;
                return Err(result.unwrap_err());
            }
        }
        Ok(())
    }

    pub fn rollback(&self, transaction: Transaction) {
        // No-op for now, as commit will handle rollback on failure.
        // This can be expanded later if needed.
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

        // Truncate the WAL file
        File::create(&self.wal_path)?;

        println!("Database saved in {:?}", start.elapsed());
        Ok(())
    }

    pub async fn load(&mut self, path: &str) -> io::Result<()> {
        let start = Instant::now();
        if let Ok(mut file) = File::open(path) {
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
        }

        self.replay_wal().await?;

        println!("Database loaded in {:?}", start.elapsed());
        Ok(())
    }

    async fn replay_wal(&mut self) -> io::Result<()> {
        let mut file = match File::open(&self.wal_path) {
            Ok(f) => f,
            Err(_) => return Ok(()),
        };
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let mut cursor = io::Cursor::new(buffer);
        while cursor.position() < cursor.get_ref().len() as u64 {
            let entry: WalEntry = bincode::deserialize_from(&mut cursor)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            self.apply_wal_entry(entry).await;
        }

        Ok(())
    }

    async fn apply_wal_entry(&mut self, entry: WalEntry) {
        match entry {
            WalEntry::CreateTable { name, columns } => {
                let _ = self.create_table(name, columns).await;
            }
            WalEntry::Insert { table_name, row } => {
                let _ = self.insert(&table_name, row).await;
            }
            WalEntry::Update { .. } => {
                // Not implemented due to non-serializable update_fn
            }
            WalEntry::Delete { table_name, query } => {
                let _ = self.delete(&table_name, &query).await;
            }
        }
    }
    pub async fn create_table(
        &mut self,
        name: String,
        columns: Vec<Column>,
    ) -> Result<Duration, String> {
        let start = Instant::now();
        let wal_entry = WalEntry::CreateTable {
            name: name.clone(),
            columns: columns.clone(),
        };
        self.wal_writer
            .write()
            .await
            .log(&wal_entry)
            .map_err(|e| e.to_string())?;

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

    fn insert_internal(
        &self,
        tables: &mut HashMap<String, Table>,
        table_name: &str,
        row: HashMap<String, Value>,
    ) -> Result<(), String> {
        let table = tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table {} not found", table_name))?;

        for col in &table.columns {
            if let Some(value) = row.get(&col.name) {
                let type_matches = match (&col.data_type, value) {
                    (DataType::Integer, Value::Integer(_)) => true,
                    (DataType::String, Value::String(_)) => true,
                    (DataType::Float, Value::Float(_)) => true,
                    (DataType::Boolean, Value::Boolean(_)) => true,
                    (DataType::DateTime, Value::DateTime(_)) => true,
                    (DataType::Uuid, Value::Uuid(_)) => true,
                    (DataType::Json, Value::Json(_)) => true,
                    (_, Value::Null) => true, // Assuming null is allowed for any type
                    _ => false,
                };
                if !type_matches {
                    return Err(format!(
                        "Invalid data type for column {}: expected {:?}, got {:?}",
                        col.name, col.data_type, value
                    ));
                }
            } else {
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
        Ok(())
    }

    pub async fn insert(
        &mut self,
        table_name: &str,
        row: HashMap<String, Value>,
    ) -> Result<Duration, String> {
        let start = Instant::now();
        let wal_entry = WalEntry::Insert {
            table_name: table_name.to_string(),
            row: row.clone(),
        };
        self.wal_writer
            .write()
            .await
            .log(&wal_entry)
            .map_err(|e| e.to_string())?;

        let mut tables = self.tables.write().await;
        self.insert_internal(&mut tables, table_name, row)?;
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

    fn update_internal(
        &self,
        tables: &mut HashMap<String, Table>,
        table_name: &str,
        query: &Query,
        update_fn: fn(&mut HashMap<String, Value>),
    ) -> Result<usize, String> {
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

    pub async fn update(
        &mut self,
        table_name: &str,
        query: &Query,
        update_fn: fn(&mut HashMap<String, Value>),
    ) -> Result<usize, String> {
        let wal_entry = WalEntry::Update {
            table_name: table_name.to_string(),
            query: query.clone(),
        };
        self.wal_writer
            .write()
            .await
            .log(&wal_entry)
            .map_err(|e| e.to_string())?;

        let mut tables = self.tables.write().await;
        self.update_internal(&mut tables, table_name, query, update_fn)
    }

    fn delete_internal(
        &self,
        tables: &mut HashMap<String, Table>,
        table_name: &str,
        query: &Query,
    ) -> Result<usize, String> {
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

    pub async fn delete(&mut self, table_name: &str, query: &Query) -> Result<usize, String> {
        let wal_entry = WalEntry::Delete {
            table_name: table_name.to_string(),
            query: query.clone(),
        };
        self.wal_writer
            .write()
            .await
            .log(&wal_entry)
            .map_err(|e| e.to_string())?;

        let mut tables = self.tables.write().await;
        self.delete_internal(&mut tables, table_name, query)
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
