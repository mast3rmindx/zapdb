use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Instant;
use std::fs::File;
use std::io::{self, Write, Read};
use serde::{Serialize, Deserialize};


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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Table {
    name: String,
    columns: Vec<Column>,
    data: Vec<HashMap<String, Value>>,
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
    tables: HashMap<String, Table>,
}

impl Default for Database {
    fn default() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }
}

impl Database {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn save(&self, path: &str) -> io::Result<()> {
        let start = Instant::now();
        let encoded: Vec<u8> =
            bincode::serialize(&self.tables).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let mut file = File::create(path)?;
        file.write_all(&encoded)?;
        println!("Database saved in {:?}", start.elapsed());
        Ok(())
    }

    pub fn load(&mut self, path: &str) -> io::Result<()> {
        let start = Instant::now();
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let tables: HashMap<String, Table> = bincode::deserialize(&buffer)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.tables = tables;
        println!("Database loaded in {:?}", start.elapsed());
        Ok(())
    }
    pub fn create_table(
        &mut self,
        name: String,
        columns: Vec<Column>,
    ) -> Result<Duration, String> {
        let start = Instant::now();
        if self.tables.contains_key(&name) {
            return Err(format!("Table {} already exists", name));
        }
        self.tables.insert(
            name.clone(),
            Table {
                name,
                columns,
                data: Vec::new(),
            },
        );
        Ok(start.elapsed())
    }

    pub fn insert(
        &mut self,
        table_name: &str,
        row: HashMap<String, Value>,
    ) -> Result<Duration, String> {
        let start = Instant::now();
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table {} not found", table_name))?;

        for col in &table.columns {
            if !row.contains_key(&col.name) {
                return Err(format!("Missing column: {}", col.name));
            }
        }

        table.data.push(row);
        Ok(start.elapsed())
    }

    pub fn select<F>(
        &self,
        table_name: &str,
        conditions: Option<F>,
    ) -> Result<(Vec<&HashMap<String, Value>>, Duration), String>
    where
        F: Fn(&HashMap<String, Value>) -> bool,
    {
        let start = Instant::now();
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| format!("Table {} not found", table_name))?;

        let results = match conditions {
            Some(filter) => table.data.iter().filter(|row| filter(row)).collect(),
            None => table.data.iter().collect(),
        };

        Ok((results, start.elapsed()))
    }

    pub fn update<C, U>(&mut self, table_name: &str, conditions: C, update_fn: U) -> Result<usize, String>
    where
        C: Fn(&HashMap<String, Value>) -> bool,
        U: Fn(&mut HashMap<String, Value>),
    {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table {} not found", table_name))?;

        let mut updated_count = 0;
        for row in &mut table.data {
            if conditions(row) {
                update_fn(row);
                updated_count += 1;
            }
        }

        Ok(updated_count)
    }

    pub fn delete<F>(&mut self, table_name: &str, conditions: F) -> Result<usize, String>
    where
        F: Fn(&HashMap<String, Value>) -> bool,
    {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table {} not found", table_name))?;

        let initial_len = table.data.len();
        table.data.retain(|row| !conditions(row));

        Ok(initial_len - table.data.len())
    }
}
