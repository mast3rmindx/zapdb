### zapdb

zapdb is a lightweight database written in Rust. It offers basic database functionalities such as creating tables, inserting, updating, selecting, and deleting records.

#### Features

- Create tables with specified columns and data types
- Insert, update, and delete records
- Query records using custom filters
- Load and save the database to a file
- Asynchronous stuff
- Encryption support

### Encryption

zapdb supports encryption out of the box. To use it, you need to provide a 32-byte key to the `Database::new` function. The database will then be encrypted when saved to a file and decrypted when loaded.

#### Installation

To use zapdb in your Rust project, add the following dependencies to your `Cargo.toml` file:

```toml
[dependencies]
tokio = "1.42.0"
zapdb = "0.1.1"
```
Or use the `cargo add` command:

```bash
cargo add zapdb tokio
```


#### Usage

Here is a simple example demonstrating how to use zapdb:

```rust

use zapdb::{Column, DataType, Database, Value, Query, Condition, Operator};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let key = [0u8; 32];
    let mut db = Database::new(key);

    // Loading the database from a file
    match db.load("database.zap").await {
        Ok(_) => println!("Database loaded successfully."),
        Err(e) => println!("Failed to load database: {:?}", e),
    };
    // Creating a table
    db.create_table(
        "users".to_string(),
        vec![
            Column::new("id".to_string(), DataType::Integer),
            Column::new("name".to_string(), DataType::String),
            Column::new("age".to_string(), DataType::Integer),
        ],
    )
    .await
    .unwrap();

    // Create an index on the 'age' column
    db.create_index("users", "age").await.unwrap();

    // Inserting records
    let user1: HashMap<String, Value> = HashMap::from([
        ("id".to_string(), Value::Integer(1)),
        ("name".to_string(), Value::String("Alice".to_string())),
        ("age".to_string(), Value::Integer(30)),
    ]);

    let user2: HashMap<String, Value> = HashMap::from([
        ("id".to_string(), Value::Integer(2)),
        ("name".to_string(), Value::String("Bob".to_string())),
        ("age".to_string(), Value::Integer(25)),
    ]);

    db.insert("users", user1).await.unwrap();
    db.insert("users", user2).await.unwrap();

    // Selecting records with a query
    let query = Query::And(vec![
        Query::Condition(Condition {
            column: "age".to_string(),
            operator: Operator::Gte,
            value: Value::Integer(25),
        }),
        Query::Condition(Condition {
            column: "name".to_string(),
            operator: Operator::Eq,
            value: Value::String("Alice".to_string()),
        }),
    ]);

    let (users, _) = db.select("users", &query).await.unwrap();

    for user in users {
        println!("Spotted user: {:?}", user);
    }

    // Updating records
    let update_query = Query::Condition(Condition {
        column: "id".to_string(),
        operator: Operator::Eq,
        value: Value::Integer(1),
    });
    db.update("users", &update_query, |user| {
        user.insert("age".to_string(), Value::Integer(31));
    })
    .await
    .unwrap();

    // Saving the database
    db.save("database.zap").await.unwrap();

    // Deleting records
    let delete_query = Query::Condition(Condition {
        column: "name".to_string(),
        operator: Operator::Eq,
        value: Value::String("Alice".to_string()),
    });
    let deleted = db.delete("users", &delete_query).await.unwrap();

    println!("Deleted {} users", deleted);
}

```

#### Contribution

Contributions are welcome! Feel free to open an issue or submit a pull request on GitHub.

#### License

This project is licensed under the GNU General Public License v3.0.  [LICENSE](https://github.com/Smartlinuxcoder/zapdb/blob/main/LICENSE)

