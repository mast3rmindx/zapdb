#[cfg(test)]
mod tests {
    use zapdb::{Database, Column, DataType, Value, Query};
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_transaction_commit() {
        let key = [0u8; 32];
        let mut db = Database::new(key, "test_transactions.wal");

        db.create_table(
            "users".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer),
                Column::new("name".to_string(), DataType::String),
            ],
        )
        .await
        .unwrap();

        let mut transaction = db.begin_transaction();

        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), Value::Integer(1));
        row1.insert("name".to_string(), Value::String("Alice".to_string()));
        transaction.insert("users".to_string(), row1);

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), Value::Integer(2));
        row2.insert("name".to_string(), Value::String("Bob".to_string()));
        transaction.insert("users".to_string(), row2);

        db.commit(transaction).await.unwrap();

        let (users, _) = db.select("users", &Query::MatchAll).await.unwrap();
        assert_eq!(users.len(), 2);
    }

    #[tokio::test]
    async fn test_transaction_rollback() {
        let key = [0u8; 32];
        let mut db = Database::new(key, "test_transactions_rollback.wal");

        db.create_table(
            "users".to_string(),
            vec![
                Column::new("id".to_string(), DataType::Integer),
                Column::new("name".to_string(), DataType::String),
            ],
        )
        .await
        .unwrap();

        let mut transaction = db.begin_transaction();

        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), Value::Integer(1));
        row1.insert("name".to_string(), Value::String("Alice".to_string()));
        transaction.insert("users".to_string(), row1);

        // This insert will fail because of a missing column
        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), Value::Integer(2));
        transaction.insert("users".to_string(), row2);

        let result = db.commit(transaction).await;
        assert!(result.is_err());

        let (users, _) = db.select("users", &Query::MatchAll).await.unwrap();
        assert_eq!(users.len(), 0);
    }
}
