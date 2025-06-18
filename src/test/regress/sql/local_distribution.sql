-- Creates db on coordinator shard without distributing

CREATE TABLE local_table (asd Int) DISTRIBUTED LOCAL;

EXPLAIN SELECT * FROM local_table;
