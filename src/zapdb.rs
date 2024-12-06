use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::time::Instant;
use std::fs::File;
use std::io::{self, Write};
use serde::{Serialize, Deserialize};


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: DataType,
}

impl Column {
    pub fn new(name: String, data_type: DataType) -> Self {
        Column { name, data_type }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
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

pub struct Database {
    tables: HashMap<String, Table>,
}

impl Database {
    pub fn new() -> Self {
        Database {
            tables: HashMap::new(),
        }
    }
    pub fn save(&self, path: &str) -> io::Result<()> {
        let start = Instant::now();
        let encoded: Vec<u8> = bincode::serialize(&self.tables).unwrap();
        let mut file = File::create(path)?;
        file.write_all(&encoded)?;

        println!("Database saved in {:?}", start.elapsed());
        Ok(())
    }
    pub fn create_table(&mut self, name: String, columns: Vec<Column>) -> Result<(), String> {
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

        println!("Table created in {:?}", start.elapsed());
        Ok(())
    }

    pub fn insert(&mut self, table_name: &str, row: HashMap<String, Value>) -> Result<(), String> {
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

        println!("Row inserted in {:?}", start.elapsed());
        Ok(())
    }

    pub fn select(
        &self,
        table_name: &str,
        conditions: Option<fn(&HashMap<String, Value>) -> bool>,
    ) -> Result<Vec<&HashMap<String, Value>>, String> {
        let start = Instant::now();
        let table = self
            .tables
            .get(table_name)
            .ok_or_else(|| format!("Table {} not found", table_name))?;

        let results = match conditions {
            Some(filter) => table.data.iter().filter(|row| filter(row)).collect(),
            None => table.data.iter().collect(),
        };

        println!("Rows selected in {:?}", start.elapsed());
        Ok(results)
    }

    pub fn update(
        &mut self,
        table_name: &str,
        conditions: fn(&HashMap<String, Value>) -> bool,
        update_fn: fn(&mut HashMap<String, Value>),
    ) -> Result<usize, String> {
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

    pub fn delete(
        &mut self,
        table_name: &str,
        conditions: fn(&HashMap<String, Value>) -> bool,
    ) -> Result<usize, String> {
        let table = self
            .tables
            .get_mut(table_name)
            .ok_or_else(|| format!("Table {} not found", table_name))?;

        let initial_len = table.data.len();
        table.data.retain(|row| !conditions(row));

        Ok(initial_len - table.data.len())
    }
}
