#[cfg(test)]
mod tests {
    use zapdb::{Database, Column, DataType, Value, Query, AggregateQuery, AggregateFunction};
    use std::collections::HashMap;
    use std::fs;

    async fn setup_db() -> Database {
        let mut db = Database::new([0; 32], "test_aggregation.wal");
        let columns = vec![
            Column::new("id".to_string(), DataType::Integer, vec![]),
            Column::new("name".to_string(), DataType::String, vec![]),
            Column::new("age".to_string(), DataType::Integer, vec![]),
            Column::new("salary".to_string(), DataType::Float, vec![]),
        ];
        db.create_table("employees".to_string(), columns).await.unwrap();

        let mut row1 = HashMap::new();
        row1.insert("id".to_string(), Value::Integer(1));
        row1.insert("name".to_string(), Value::String("Alice".to_string()));
        row1.insert("age".to_string(), Value::Integer(30));
        row1.insert("salary".to_string(), Value::Float(50000.0));
        db.insert("employees", row1).await.unwrap();

        let mut row2 = HashMap::new();
        row2.insert("id".to_string(), Value::Integer(2));
        row2.insert("name".to_string(), Value::String("Bob".to_string()));
        row2.insert("age".to_string(), Value::Integer(40));
        row2.insert("salary".to_string(), Value::Float(60000.0));
        db.insert("employees", row2).await.unwrap();

        let mut row3 = HashMap::new();
        row3.insert("id".to_string(), Value::Integer(3));
        row3.insert("name".to_string(), Value::String("Charlie".to_string()));
        row3.insert("age".to_string(), Value::Integer(30));
        row3.insert("salary".to_string(), Value::Float(70000.0));
        db.insert("employees", row3).await.unwrap();

        db
    }

    #[tokio::test]
    async fn test_count() {
        let db = setup_db().await;
        let query = Query::Aggregate(AggregateQuery {
            function: AggregateFunction::Count,
            column: "id".to_string(),
            filter: None,
        });
        let (result, _) = db.select("employees", &query).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("result"), Some(&Value::Integer(3)));
    }

    #[tokio::test]
    async fn test_sum() {
        let db = setup_db().await;
        let query = Query::Aggregate(AggregateQuery {
            function: AggregateFunction::Sum,
            column: "salary".to_string(),
            filter: None,
        });
        let (result, _) = db.select("employees", &query).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("result"), Some(&Value::Float(180000.0)));
    }

    #[tokio::test]
    async fn test_avg() {
        let db = setup_db().await;
        let query = Query::Aggregate(AggregateQuery {
            function: AggregateFunction::Avg,
            column: "salary".to_string(),
            filter: None,
        });
        let (result, _) = db.select("employees", &query).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("result"), Some(&Value::Float(60000.0)));
    }

    #[tokio::test]
    async fn test_min() {
        let db = setup_db().await;
        let query = Query::Aggregate(AggregateQuery {
            function: AggregateFunction::Min,
            column: "age".to_string(),
            filter: None,
        });
        let (result, _) = db.select("employees", &query).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("result"), Some(&Value::Integer(30)));
    }

    #[tokio::test]
    async fn test_max() {
        let db = setup_db().await;
        let query = Query::Aggregate(AggregateQuery {
            function: AggregateFunction::Max,
            column: "age".to_string(),
            filter: None,
        });
        let (result, _) = db.select("employees", &query).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("result"), Some(&Value::Integer(40)));
    }

    #[tokio::test]
    async fn test_aggregation_with_filter() {
        let db = setup_db().await;
        let filter = Query::Condition(zapdb::Condition {
            column: "age".to_string(),
            operator: zapdb::Operator::Eq,
            value: Value::Integer(30),
        });
        let query = Query::Aggregate(AggregateQuery {
            function: AggregateFunction::Count,
            column: "id".to_string(),
            filter: Some(Box::new(filter)),
        });
        let (result, _) = db.select("employees", &query).await.unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get("result"), Some(&Value::Integer(2)));
    }
}
