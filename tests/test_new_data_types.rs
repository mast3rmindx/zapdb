use zapdb::{
    Database,
    Value,
    Column,
    DataType,
    Query,
    Condition,
    Operator,
};
use std::collections::HashMap;
use chrono::{Utc, TimeZone};
use uuid::Uuid;
use serde_json::json;

#[tokio::test]
async fn test_new_data_types() {
    let mut db = Database::new([0; 32], "test_new_data_types.wal");
    let columns = vec![
        Column::new("id".to_string(), DataType::Integer),
        Column::new("created_at".to_string(), DataType::DateTime),
        Column::new("uuid".to_string(), DataType::Uuid),
        Column::new("data".to_string(), DataType::Json),
    ];
    db.create_table("test".to_string(), columns).await.unwrap();

    let now = Utc::now();
    let uuid = Uuid::new_v4();
    let json_data = json!({ "a": 1, "b": "hello" });

    let mut row = HashMap::new();
    row.insert("id".to_string(), Value::Integer(1));
    row.insert("created_at".to_string(), Value::DateTime(now));
    row.insert("uuid".to_string(), Value::Uuid(uuid));
    row.insert("data".to_string(), Value::Json(json_data.clone()));

    db.insert("test", row).await.unwrap();

    let (results, _) = db.select("test", &Query::MatchAll).await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].get("id"), Some(&Value::Integer(1)));
    assert_eq!(results[0].get("created_at"), Some(&Value::DateTime(now)));
    assert_eq!(results[0].get("uuid"), Some(&Value::Uuid(uuid)));
    assert_eq!(results[0].get("data"), Some(&Value::Json(json_data)));
}
