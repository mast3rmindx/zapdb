use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time;
use zapdb::{Database, Value};
use rand::rngs::OsRng;
use rand::RngCore;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a new database instance
    let db = Arc::new(RwLock::new(Database::new([0; 32], "test_sharding.wal")));

    // Generate a key for encryption
    let mut key = [0u8; 32];
    OsRng.fill_bytes(&mut key);

    // Enable sharding
    db.write()
        .await
        .enable_sharding(vec!["127.0.0.1:8080".to_string()], key)
        .await?;

    // Start the network in the background
    let mut db_clone = db.clone();
    let network_handle = tokio::spawn(async move {
        db_clone.write().await.start_network().await;
    });

    // Create a table
    db.read()
        .await
        .create_table(
            "users".to_string(),
            vec![
                zapdb::Column::new("id".to_string(), zapdb::DataType::Integer, vec![]),
                zapdb::Column::new("name".to_string(), zapdb::DataType::String, vec![]),
            ],
        )
        .await?;

    // Insert some data
    let mut row1 = HashMap::new();
    row1.insert("id".to_string(), Value::Integer(1));
    row1.insert("name".to_string(), Value::String("Alice".to_string()));
    db.read()
        .await
        .insert("users", row1)
        .await?;

    let mut row2 = HashMap::new();
    row2.insert("id".to_string(), Value::Integer(2));
    row2.insert("name".to_string(), Value::String("Bob".to_string()));
    db.read()
        .await
        .insert("users", row2)
        .await?;

    // Select data
    let (results, _) = db
        .read()
        .await
        .select(
            "users",
            &zapdb::Query::Condition(zapdb::Condition {
                column: "id".to_string(),
                operator: zapdb::Operator::Eq,
                value: Value::Integer(1),
            }),
        )
        .await?;

    println!("Results: {:?}", results);

    // Give the some time to run
    time::sleep(Duration::from_secs(2)).await;

    // Stop the network
    network_handle.abort();

    Ok(())
}
