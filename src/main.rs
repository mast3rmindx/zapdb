use zapdb::ZapDB;

mod zapdb;

#[tokio::main] 
async fn main() {
    let mut db = ZapDB::new();
    //db.load("database.zap").unwrap();
    db.insert("test".to_string(), "sigma".to_string());
    println!("Value: {}", db.get("test").unwrap());
    db.save("database.zap").await.unwrap();
}