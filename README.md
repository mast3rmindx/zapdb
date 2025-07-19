# zapdb

zapdb is a lightweight, in-memory, SQL-like database written in Rust. It's designed for simplicity and performance, making it a great choice for applications that need a fast, embedded database.

## Features

- **In-Memory:** Data is stored in memory for fast access.
- **SQL-like:** Provides a simple, SQL-like interface for interacting with the database.
- **Concurrency:** Uses `tokio` for asynchronous operations and `RwLock` for concurrent access to data.
- **Encryption:** Secures your data at rest with AES-256-GCM encryption.
- **Compression:** Reduces the on-disk footprint of your database with Gzip compression.
- **Data Integrity:** Ensures data integrity with a Blake3-based Merkle tree.
- **Indexing:** Speeds up queries with B-Tree indexes.

## Getting Started

### Installation

Add zapdb to your `Cargo.toml`:

```toml
[dependencies]
zapdb = "1.0.0"
tokio = { version = "1", features = ["full"] }
```

### Usage

Here's a quick example of how to use zapdb:

```rust
use zapdb::{Column, DataType, Database, Value, Query, Condition, Operator};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    // Create a new database with a 32-byte key for encryption.
    let key = [0u8; 32];
    let mut db = Database::new(key);

    // Create a table.
    db.create_table(
        "users".to_string(),
        vec![
            Column::new("id".to_string(), DataType::Integer),
            Column::new("name".to_string(), DataType::String),
        ],
    )
    .await
    .unwrap();

    // Insert some data.
    let mut user = HashMap::new();
    user.insert("id".to_string(), Value::Integer(1));
    user.insert("name".to_string(), Value::String("Alice".to_string()));
    db.insert("users", user).await.unwrap();

    // Query the data.
    let (users, _) = db.select("users", &Query::MatchAll).await.unwrap();
    println!("{:?}", users);

    // Save the database to a file.
    db.save("my_database.zap").await.unwrap();

    // Load the database from a file.
    let mut new_db = Database::new(key);
    new_db.load("my_database.zap").await.unwrap();

    // Verify the integrity of the database.
    assert!(new_db.verify_integrity().await);
}
```

## How It Works

### Encryption

zapdb uses AES-256-GCM to encrypt the database when it's saved to disk. A 32-byte key is required to create a new database. This key is used to encrypt and decrypt the data.

### Compression

Before being encrypted, the database is compressed using Gzip to reduce its size. This can significantly reduce the amount of disk space required to store the database, especially for large datasets.

### Data Integrity

To ensure that the data is not corrupted, zapdb uses a Merkle tree. The leaves of the tree are the Blake3 hashes of each row in a table. The root of the tree is a single hash that represents the entire table. When the database is loaded, the Merkle tree is rebuilt and the root hash is compared to the stored hash to verify the integrity of the data.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.

## License

zapdb is licensed under the MIT License. See the [LICENSE](LICENSE) file for more information.
