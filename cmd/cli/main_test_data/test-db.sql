CREATE DATABASE IF NOT EXISTS test_db;

CREATE TABLE IF NOT EXISTS test_db.input_table
(
    id        Int64,
    parent_id Nullable(Int64),
    name      String
) ENGINE = Null;

CREATE MATERIALIZED VIEW IF NOT EXISTS test_db.target_table_mv TO test_db.target_table AS
SELECT input_table.id   AS id,
       input_table.name AS path
FROM test_db.input_table;


CREATE TABLE IF NOT EXISTS test_db.target_table
(
    id   Int64,
    path String
) ENGINE = ReplacingMergeTree()
      ORDER BY (id);
