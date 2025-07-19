use zapdb::{Column, DataType, Value, Query, Condition, Operator, create_pool};
use std::collections::HashMap;

#[tokio::main]
async fn main() {
    let key = [0u8; 32];
    let pool = create_pool(key, "database.wal").unwrap();
    let db = pool.get().unwrap();

    // Loading the database from a file
    match db.load("database.zap").await {
        Ok(_) => println!("Database loaded successfully."),
        Err(e) => println!("Failed to load database: {:?}", e),
    };
    // Creating a table
    db.create_table(
        "users".to_string(),
        vec![
            Column::new("id".to_string(), DataType::Integer, vec![]),
            Column::new("name".to_string(), DataType::String, vec![]),
            Column::new("age".to_string(), DataType::Integer, vec![]),
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

    // Verify integrity
    assert!(db.verify_integrity().await);
    println!("Database integrity verified.");

    // Deleting records
    let delete_query = Query::Condition(Condition {
        column: "name".to_string(),
        operator: Operator::Eq,
        value: Value::String("Alice".to_string()),
    });
    let deleted = db.delete("users", &delete_query).await.unwrap();

    println!("Deleted {} users", deleted);
}
