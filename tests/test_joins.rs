#[cfg(test)]
mod tests {
    use zapdb::{create_pool, PooledConnection, Column, DataType, Value, Query, Join, JoinType};
    use std::collections::HashMap;

    async fn setup_db() -> PooledConnection {
        let pool = create_pool([0; 32], "test_joins.wal").unwrap();
        let db = pool.get().unwrap();

        // Create users table
        let users_columns = vec![
            Column::new("id".to_string(), DataType::Integer, vec![]),
            Column::new("name".to_string(), DataType::String, vec![]),
        ];
        db.create_table("users".to_string(), users_columns).await.unwrap();

        // Create posts table
        let posts_columns = vec![
            Column::new("id".to_string(), DataType::Integer, vec![]),
            Column::new("user_id".to_string(), DataType::Integer, vec![]),
            Column::new("title".to_string(), DataType::String, vec![]),
        ];
        db.create_table("posts".to_string(), posts_columns).await.unwrap();

        // Insert users
        let mut user1 = HashMap::new();
        user1.insert("id".to_string(), Value::Integer(1));
        user1.insert("name".to_string(), Value::String("Alice".to_string()));
        db.insert("users", user1).await.unwrap();

        let mut user2 = HashMap::new();
        user2.insert("id".to_string(), Value::Integer(2));
        user2.insert("name".to_string(), Value::String("Bob".to_string()));
        db.insert("users", user2).await.unwrap();

        let mut user3 = HashMap::new();
        user3.insert("id".to_string(), Value::Integer(3));
        user3.insert("name".to_string(), Value::String("Charlie".to_string()));
        db.insert("users", user3).await.unwrap();

        // Insert posts
        let mut post1 = HashMap::new();
        post1.insert("id".to_string(), Value::Integer(101));
        post1.insert("user_id".to_string(), Value::Integer(1));
        post1.insert("title".to_string(), Value::String("Post 1".to_string()));
        db.insert("posts", post1).await.unwrap();

        let mut post2 = HashMap::new();
        post2.insert("id".to_string(), Value::Integer(102));
        post2.insert("user_id".to_string(), Value::Integer(2));
        post2.insert("title".to_string(), Value::String("Post 2".to_string()));
        db.insert("posts", post2).await.unwrap();

        let mut post3 = HashMap::new();
        post3.insert("id".to_string(), Value::Integer(103));
        post3.insert("user_id".to_string(), Value::Integer(1));
        post3.insert("title".to_string(), Value::String("Post 3".to_string()));
        db.insert("posts", post3).await.unwrap();

        db
    }

    #[tokio::test]
    async fn test_inner_join() {
        let db = setup_db().await;

        let join = Join {
            join_type: JoinType::Inner,
            target_table: "posts".to_string(),
            on_condition: ("id".to_string(), "user_id".to_string()),
        };

        let (results, _) = db.select("users", &Query::Join(join)).await.unwrap();
        assert_eq!(results.len(), 3);
    }

    #[tokio::test]
    async fn test_left_join() {
        let db = setup_db().await;

        let join = Join {
            join_type: JoinType::Left,
            target_table: "posts".to_string(),
            on_condition: ("id".to_string(), "user_id".to_string()),
        };

        let (results, _) = db.select("users", &Query::Join(join)).await.unwrap();
        assert_eq!(results.len(), 4);
    }

    #[tokio::test]
    async fn test_right_join() {
        let db = setup_db().await;

        let mut post4 = HashMap::new();
        post4.insert("id".to_string(), Value::Integer(104));
        post4.insert("user_id".to_string(), Value::Integer(4));
        post4.insert("title".to_string(), Value::String("Post 4".to_string()));
        db.insert("posts", post4).await.unwrap();

        let join = Join {
            join_type: JoinType::Right,
            target_table: "posts".to_string(),
            on_condition: ("id".to_string(), "user_id".to_string()),
        };

        let (results, _) = db.select("users", &Query::Join(join)).await.unwrap();
        assert_eq!(results.len(), 4);
    }
}
