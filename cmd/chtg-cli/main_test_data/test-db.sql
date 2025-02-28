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

-- tables to test joins
CREATE TABLE IF NOT EXISTS test_db.base_1
(
    id   Int64,
    data String
) ENGINE = MergeTree()
      ORDER BY (id);

CREATE TABLE IF NOT EXISTS test_db.base_2
(
    id          Int64,
    description String
) ENGINE = MergeTree()
      ORDER BY (id);

CREATE TABLE IF NOT EXISTS test_db.join_target
(
    id          Int64,
    name        String,
    data        String,
    description String
) ENGINE = MergeTree()
      ORDER BY (id);

CREATE MATERIALIZED VIEW IF NOT EXISTS test_db.join_target_mv TO test_db.join_target AS
SELECT input_table.id     AS id,
       input_table.name   AS name,
       base_1.data        AS data,
       base_2.description AS description
FROM test_db.input_table
         JOIN test_db.base_1 ON input_table.id = base_1.id
         JOIN test_db.base_2 ON input_table.id = base_2.id;
