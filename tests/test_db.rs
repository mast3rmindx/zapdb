#[cfg(test)]
mod tests {
    use zapdb::{create_pool, Column, DataType, Value, Query, Constraint};
    use std::collections::HashMap;
    use std::fs;

    #[tokio::test]
    async fn test_not_null_constraint() {
        let pool = create_pool([0; 32], "test_not_null_constraint.wal").unwrap();
        let db = pool.get().unwrap();
        let columns = vec![
            Column::new("id".to_string(), DataType::Integer, vec![Constraint::NotNull]),
            Column::new("name".to_string(), DataType::String, vec![]),
        ];
        db.create_table("users".to_string(), columns).await.unwrap();

        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Integer(1));
        row.insert("name".to_string(), Value::String("Alice".to_string()));
        assert!(db.insert("users", row, None).await.is_ok());

        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Null);
        row.insert("name".to_string(), Value::String("Bob".to_string()));
        assert!(db.insert("users", row, None).await.is_err());
    }

    #[tokio::test]
    async fn test_unique_constraint() {
        let pool = create_pool([0; 32], "test_unique_constraint.wal").unwrap();
        let db = pool.get().unwrap();
        let columns = vec![
            Column::new("id".to_string(), DataType::Integer, vec![Constraint::Unique]),
            Column::new("name".to_string(), DataType::String, vec![]),
        ];
        db.create_table("users".to_string(), columns).await.unwrap();

        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Integer(1));
        row.insert("name".to_string(), Value::String("Alice".to_string()));
        assert!(db.insert("users", row).await.is_ok());

        let mut row = HashMap::new();
        row.insert("id".to_string(), Value::Integer(1));
        row.insert("name".to_string(), Value::String("Bob".to_string()));
        assert!(db.insert("users", row).await.is_err());
    }

    #[tokio::test]
    async fn test_foreign_key_constraint() {
        let pool = create_pool([0; 32], "test_foreign_key_constraint.wal").unwrap();
        let db = pool.get().unwrap();

        let users_columns = vec![
            Column::new("id".to_string(), DataType::Integer, vec![Constraint::Unique]),
        ];
        db.create_table("users".to_string(), users_columns).await.unwrap();

        let posts_columns = vec![
            Column::new("id".to_string(), DataType::Integer, vec![]),
            Column::new("user_id".to_string(), DataType::Integer, vec![Constraint::ForeignKey { table: "users".to_string(), column: "id".to_string() }]),
        ];
        db.create_table("posts".to_string(), posts_columns).await.unwrap();

        let mut user_row = HashMap::new();
        user_row.insert("id".to_string(), Value::Integer(1));
        assert!(db.insert("users", user_row).await.is_ok());

        let mut post_row = HashMap::new();
        post_row.insert("id".to_string(), Value::Integer(1));
        post_row.insert("user_id".to_string(), Value::Integer(1));
        assert!(db.insert("posts", post_row).await.is_ok());

        let mut post_row = HashMap::new();
        post_row.insert("id".to_string(), Value::Integer(2));
        post_row.insert("user_id".to_string(), Value::Integer(2));
        assert!(db.insert("posts", post_row).await.is_err());
    }

    #[tokio::test]
    async fn test_save_load_with_compression_and_integrity_check() {
        let key = [0u8; 32];
        let db_path = "test_db.zap";
        let wal_path = "test_db.wal";
        let pool = create_pool(key, wal_path).unwrap();
        let db = pool.get().unwrap();

        // Create a table and insert some data
        db.create_table(
            "users".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer, vec![]),
                Column::new("name".to_string(), DataType::String, vec![]),
            ],
        )
        .await
        .unwrap();

        for i in 0..100 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), Value::Integer(i));
            row.insert("name".to_string(), Value::String(format!("user{}", i)));
            db.insert("users", row).await.unwrap();
        }

        // Save the database
        db.save(db_path).await.unwrap();

        // Check that the file is smaller than the uncompressed data
        let metadata = fs::metadata(db_path).unwrap();
        let tables = db.tables.read().await;
        let encoded: Vec<u8> = bincode::serialize(&*tables).unwrap();
        assert!(metadata.len() < encoded.len() as u64);

        // Load the database
        let new_pool = create_pool(key, wal_path).unwrap();
        let new_db = new_pool.get().unwrap();
        new_db.load(db_path).await.unwrap();

        // Verify integrity
        assert!(new_db.verify_integrity().await);

        // Verify data
        let (users, _) = new_db.select("users", &Query::MatchAll).await.unwrap();
        assert_eq!(users.len(), 100);

        // Clean up
        fs::remove_file(db_path).unwrap();
    }

    #[tokio::test]
    async fn test_wal_recovery() {
        let key = [0u8; 32];
        let db_path = "test_wal.zap";
        let wal_path = "test_wal.wal";

        // Create a database and insert some data
        let pool = create_pool(key, wal_path).unwrap();
        let db = pool.get().unwrap();
        db.create_table(
            "users".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer, vec![]),
                Column::new("name".to_string(), DataType::String, vec![]),
            ],
        )
        .await
        .unwrap();

        for i in 0..10 {
            let mut row = HashMap::new();
            row.insert("id".to_string(), Value::Integer(i));
            row.insert("name".to_string(), Value::String(format!("user{}", i)));
            db.insert("users", row).await.unwrap();
        }

        // Simulate a crash (don't call save)

        // Load the database
        let new_pool = create_pool(key, wal_path).unwrap();
        let new_db = new_pool.get().unwrap();
        new_db.load(db_path).await.unwrap();

        // Verify data
        let (users, _) = new_db.select("users", &Query::MatchAll).await.unwrap();
        assert_eq!(users.len(), 10);

        // Clean up
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);
    }
}
