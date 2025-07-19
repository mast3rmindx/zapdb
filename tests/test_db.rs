#[cfg(test)]
mod tests {
    use zapdb::{Database, Column, DataType, Value, Query};
    use std::collections::HashMap;
    use std::fs;

    #[tokio::test]
    async fn test_save_load_with_compression_and_integrity_check() {
        let key = [0u8; 32];
        let db_path = "test_db.zap";
        let wal_path = "test_db.wal";
        let mut db = Database::new(key, wal_path);

        // Create a table and insert some data
        db.create_table(
            "users".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer),
                Column::new("name".to_string(), DataType::String),
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
        let mut new_db = Database::new(key, wal_path);
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
        let mut db = Database::new(key, wal_path);
        db.create_table(
            "users".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer),
                Column::new("name".to_string(), DataType::String),
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
        let mut new_db = Database::new(key, wal_path);
        new_db.load(db_path).await.unwrap();

        // Verify data
        let (users, _) = new_db.select("users", &Query::MatchAll).await.unwrap();
        assert_eq!(users.len(), 10);

        // Clean up
        let _ = fs::remove_file(db_path);
        let _ = fs::remove_file(wal_path);
    }
}
