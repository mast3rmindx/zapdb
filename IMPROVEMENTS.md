# Recommended Improvements and Features

This document lists recommended improvements and features for zapdb.

## Core Database

- **Transactions:** Add support for ACID transactions to ensure data consistency.
- **Durability:** Implement a write-ahead log (WAL) to ensure that data is not lost in the event of a crash.
- **More Data Types:** Add support for more data types, such as `DateTime`, `UUID`, and `JSON`.
- **Constraints:** Add support for constraints, such as `NOT NULL`, `UNIQUE`, and `FOREIGN KEY`.
- **Joins:** Implement support for `JOIN` operations to query data from multiple tables.
- **Aggregation:** Add support for aggregate functions, such as `COUNT`, `SUM`, `AVG`, `MIN`, and `MAX`.

## Performance

- **Concurrent Indexes:** Use concurrent data structures for indexes to improve performance for write-heavy workloads. (DONE)
- **Query Optimizer:** Implement a query optimizer to improve the performance of complex queries.
- **Connection Pooling:** Implement a connection pool to reduce the overhead of creating new connections to the database.

## Tooling

- **CLI:** Create a command-line interface (CLI) for interacting with the database.
- **GUI:** Create a graphical user interface (GUI) for managing the database.
- **Backup and Restore:** Add support for backing up and restoring the database.

## Other

- **Replication:** Add support for replicating the database to other nodes for high availability and scalability.
- **Sharding:** Add support for sharding the database to distribute data across multiple nodes.
- **Pluggable Storage Engine:** Allow users to choose different storage engines, such as RocksDB or a custom on-disk format.
