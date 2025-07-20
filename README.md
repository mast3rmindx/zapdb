# zapdb

zapdb is a lightweight, in-memory, SQL-like database written in Rust. It's designed for simplicity and performance, making it a great choice for applications that need a fast, embedded database.

## Features

- **In-Memory:** Data is stored in memory for fast access.
- **SQL-like:** Provides a simple, SQL-like interface for interacting with the database.
- **Concurrency:** Uses `tokio` for asynchronous operations and `RwLock` for concurrent access to data.
- **Encryption:** Secures your data at rest with AES-256-GCM encryption.
- **Compression:** Reduces the on-disk footprint of your database with Gzip compression.
- **Data Integrity:** Ensures data integrity with a Blake3-based Merkle tree.
- **Indexing:** Speeds up queries with concurrent hash map indexes.
- **Constraints:** Supports `NOT NULL`, `UNIQUE`, and `FOREIGN KEY` constraints.
- **Transactions:** Provides ACID transactions to ensure data consistency.
- **Joins:** Supports `INNER`, `LEFT`, and `RIGHT` joins to query data from multiple tables.
- **Query Optimizer:** Improves the performance of complex queries by reordering `And` queries to prioritize indexed columns.

## Getting Started

To get started with zapdb, add the following to your `Cargo.toml`:

```toml
[dependencies]
zapdb = "1.0.0"
tokio = { version = "1", features = ["full"] }
```

### Usage

Here's a quick example of how to use zapdb:

```rust
use zapdb::{create_pool, Column, DataType, Value, Query, Condition, Operator, Constraint};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    // Create a new connection pool with a 32-byte key for encryption.
    let key = [0u8; 32];
    let pool = create_pool(key, "database.wal").unwrap();
    let db = pool.get().unwrap();

    // Create a table with constraints.
    db.create_table(
        "users".to_string(),
        vec![
            Column::new("id".to_string(), DataType::Integer, vec![Constraint::NotNull, Constraint::Unique]),
            Column::new("name".to_string(), DataType::String, vec![Constraint::NotNull]),
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
    let new_pool = create_pool(key, "database.wal").unwrap();
    let new_db = new_pool.get().unwrap();
    new_db.load("my_database.zap").await.unwrap();

    // Verify the integrity of the database.
    assert!(new_db.verify_integrity().await);
}
```

### Transactions

zapdb supports ACID transactions. Here's an example of how to use a transaction:

```rust
use zapdb::{create_pool, begin_transaction, Value};
use std::collections::HashMap;

async fn transaction_example() {
    let pool = create_pool([0; 32], "transaction_example.wal").unwrap();
    let db = pool.get().unwrap();
    // ... (create tables)

    let mut transaction = begin_transaction();

    let mut row1 = HashMap::new();
    row1.insert("id".to_string(), Value::Integer(1));
    row1.insert("name".to_string(), Value::String("Alice".to_string()));
    transaction.insert("users".to_string(), row1);

    let mut row2 = HashMap::new();
    row2.insert("id".to_string(), Value::Integer(2));
    row2.insert("name".to_string(), Value::String("Bob".to_string()));
    transaction.insert("users".to_string(), row2);

    db.commit(transaction).await.unwrap();
}
```

### Joins

zapdb supports `INNER`, `LEFT`, and `RIGHT` joins. Here's an example of how to perform a `LEFT JOIN`:

```rust
use zapdb::{create_pool, Query, Join, JoinType};

async fn join_example() {
    let pool = create_pool([0; 32], "join_example.wal").unwrap();
    let db = pool.get().unwrap();
    // ... (create tables and insert data)

    let join = Join {
        join_type: JoinType::Left,
        target_table: "posts".to_string(),
        on_condition: ("id".to_string(), "user_id".to_string()),
    };

    let (results, _) = db.select("users", &Query::Join(join)).await.unwrap();
    println!("{:?}", results);
}
```

### Aggregation

zapdb supports the following aggregate functions:

- `COUNT`: Counts the number of rows.
- `SUM`: Calculates the sum of a numeric column.
- `AVG`: Calculates the average of a numeric column.
- `MIN`: Finds the minimum value in a column.
- `MAX`: Finds the maximum value in a column.

Here's an example of how to use the `COUNT` function:

```rust
use zapdb::{create_pool, Query, AggregateQuery, AggregateFunction};

async fn aggregate_example() {
    let pool = create_pool([0; 32], "aggregate_example.wal").unwrap();
    let db = pool.get().unwrap();
    // ... (create tables and insert data)

    let query = Query::Aggregate(AggregateQuery {
        function: AggregateFunction::Count,
        column: "id".to_string(),
        filter: None,
    });

    let (results, _) = db.select("users", &query).await.unwrap();
    println!("{:?}", results);
}
```

### Sharding

zapdb supports sharding to distribute data across multiple nodes. The communication between nodes is encrypted using AES-256-GCM to ensure that your data is secure.

To enable sharding, you need to provide a list of shard addresses and a 32-byte key for encryption. Here's an example:

```rust
use zapdb::{create_pool, Database};
use rand::rngs::OsRng;
use rand::RngCore;
use std::sync::Arc;
use tokio::sync::RwLock;

async fn sharding_example() {
    // Create a new database instance
    let db = Arc::new(RwLock::new(Database::new([0; 32], "test_sharding.wal")));

    // Generate a key for encryption
    let mut key = [0u8; 32];
    OsRng.fill_bytes(&mut key);

    // Enable sharding
    db.write()
        .await
        .enable_sharding(vec!["127.0.0.1:8080".to_string()], key)
        .await
        .unwrap();

    // Start the network in the background
    let mut db_clone = db.clone();
    tokio::spawn(async move {
        db_clone.write().await.start_network().await;
    });

    // ... (use the database as usual)
}
```

## How It Works

### Encryption

zapdb uses AES-256-GCM to encrypt the database when it's saved to disk. A 32-byte key is required to create a new database. This key is used to encrypt and decrypt the data.

### Compression

Before being encrypted, the database is compressed using Gzip to reduce its size. This can significantly reduce the amount of disk space required to store the database, especially for large datasets.

### Data Integrity

To ensure that the data is not corrupted, zapdb uses a Merkle tree. The leaves of the tree are the Blake3 hashes of each row in a table. The root of the tree is a single hash that represents the entire table. When the database is loaded, the Merkle tree is rebuilt and the root hash is compared to the stored hash to verify the integrity of the data.

### Query Optimizer

zapdb includes a simple query optimizer that improves the performance of complex queries. When you execute a query with multiple `And` conditions, the optimizer reorders the conditions to prioritize those that use an index. This can significantly reduce the number of rows that need to be scanned, resulting in faster query execution times.

## Contributing

Contributions are welcome! Please feel free to open an issue or submit a pull request.

## License

zapdb is licensed under the MIT License. See the [LICENSE](LICENSE) file for more information.
