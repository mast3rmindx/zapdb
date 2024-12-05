mod zapdb;
use std::collections::HashMap;
use crate::zapdb::{Database, Column, DataType, Value};

fn main() {
    let mut db = Database::new();

    db.create_table(
        "users".to_string(), 
        vec![
            Column { name: "id".to_string(), data_type: DataType::Integer },
            Column { name: "name".to_string(), data_type: DataType::String },
            Column { name: "age".to_string(), data_type: DataType::Integer }
        ]
    ).unwrap();

    let user1: HashMap<String, Value> = HashMap::from([
        ("id".to_string(), Value::Integer(1)),
        ("name".to_string(), Value::String("Alice".to_string())),
        ("age".to_string(), Value::Integer(30))
    ]);

    let user2: HashMap<String, Value> = HashMap::from([
        ("id".to_string(), Value::Integer(2)),
        ("name".to_string(), Value::String("Bob".to_string())),
        ("age".to_string(), Value::Integer(25))
    ]);

    db.insert("users", user1).unwrap();
    db.insert("users", user2).unwrap();

    let users = db.select("users", Some(|user| {
        match (user.get("age"), user.get("name")) {
            (Some(Value::Integer(age)), Some(Value::String(name))) => 
                *age >= 25 && name.starts_with('A'),
            _ => false
        }
    })).unwrap();

    for user in users {
        println!("Spotted user: {:?}", user);
    }

    db.update("users", 
        |user| match user.get("id") { 
            Some(Value::Integer(id)) => *id == 1, 
            _ => false 
        },
        |user| {
            user.insert("age".to_string(), Value::Integer(31));
        }
    ).unwrap();

    let deleted = db.delete("users", 
        |user| match user.get("name") { 
            Some(Value::String(name)) => name == "Alice", 
            _ => false 
        }
    ).unwrap();

    println!("Deleted {} users", deleted);
}