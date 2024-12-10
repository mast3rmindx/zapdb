### zapdb

zapdb is a lightweight database written in Rust that provides an easy-to-use interface for managing and querying data. It offers basic database functionalities such as creating tables, inserting, updating, selecting, and deleting records. The project is designed to be simple yet powerful for small to medium-sized applications.

#### Features

- Create tables with specified columns and data types
- Insert, update, and delete records
- Query records using custom filters
- Load and save the database to a file
- Asynchronous operations using Tokio

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

use zapdb::{Column, DataType, Database, Value};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let mut db = Database::new();

    // Loading the database from a file
    match db.load("database.zap").await {
        Ok(_) => println!("Database loaded successfully."),
        Err(e) => println!("Failed to load database: {:?}", e),
    };
    // Creating a table
    db.create_table(
        "users".to_string(),
        vec![
            Column {
                name: "id".to_string(),
                data_type: DataType::Integer,
            },
            Column {
                name: "name".to_string(),
                data_type: DataType::String,
            },
            Column {
                name: "age".to_string(),
                data_type: DataType::Integer,
            },
        ],
    )
    .unwrap();

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

    db.insert("users", user1).unwrap();
    db.insert("users", user2).unwrap();

    // Selecting records
    let users = db
        .select(
            "users",
            Some(|user| match (user.get("age"), user.get("name")) {
                (Some(Value::Integer(age)), Some(Value::String(name))) => {
                    *age >= 25 && name.starts_with('A')
                }
                _ => false,
            }),
        )
        .unwrap(); 

    for user in users {
        println!("Spotted user: {:?}", user);
    }

    // Updating records
    db.update(
        "users",
        |user| match user.get("id") {
            Some(Value::Integer(id)) => *id == 1,
            _ => false,
        },
        |user| {
            user.insert("age".to_string(), Value::Integer(31));
        },
    )
    .unwrap();

    // Saving the database
    db.save("database.zap").await.unwrap();

    // Deleting records
    let deleted = db
        .delete("users", |user| match user.get("name") {
            Some(Value::String(name)) => name == "Alice",
            _ => false,
        })
        .unwrap();

    println!("Deleted {} users", deleted);
}

```

#### Contribution

Contributions are welcome! Feel free to open an issue or submit a pull request on GitHub.

#### License

This project is licensed under the GNU General Public License v3.0. See the [LICENSE](https://github.com/Smartlinuxcoder/zapdb/blob/main/LICENSE) file for details.

