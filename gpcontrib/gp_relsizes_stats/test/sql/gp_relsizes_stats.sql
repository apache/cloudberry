CREATE EXTENSION gp_relsizes_stats;

-- start_ignore
DROP TABLE IF EXISTS employees;
-- end_ignore
CREATE TABLE employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    department_id INT,
    date_of_birth DATE
);

INSERT INTO employees (first_name, last_name, department_id, date_of_birth) VALUES
('John', 'Doe', 1, '1988-06-15'),
('Jane', 'Smith', 2, '1990-07-20'),
('Emily', 'Jones', 1, '1985-08-30');

SELECT relsizes_stats_schema.relsizes_collect_stats_once();

SELECT size FROM relsizes_stats_schema.table_sizes_history WHERE relname = 'employees';

-- Fill table with a lot of different rows
insert into employees (first_name, last_name, department_id, date_of_birth) 
select 'First' || i, 'Last' || i, (i % 10) + 1, DATE '1980-01-01' + (i % 365 * 365 / 30) 
from generate_series(1, 10001)i;

SELECT relsizes_stats_schema.relsizes_collect_stats_once();

-- Check that collected stats are correct
SELECT size FROM relsizes_stats_schema.table_sizes_history WHERE relname = 'employees';

SELECT relsizes_stats_schema.relsizes_collect_stats_once();

-- Validate that after rerun stats collection size of table has not change
SELECT size FROM relsizes_stats_schema.table_sizes_history WHERE relname = 'employees';

-- Cleanup
DROP TABLE employees;


--
-- relsizes_collect_stats_once should collect files sizes without pauses
-- The naptime value is 1ms, so the pauses take at least 10s to process 10k files.
-- Check that relsizes_collect_stats_once completes in significantly less time.

-- start_ignore
DROP TABLE IF EXISTS t;
CREATE TABLE t (i int)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE (i) (PARTITION a START (0) END (10000) EVERY (1));
-- end_ignore

SELECT EXTRACT(EPOCH FROM LOCALTIMESTAMP(0)) t1 \gset

SELECT relsizes_stats_schema.relsizes_collect_stats_once();

SELECT (EXTRACT(EPOCH FROM LOCALTIMESTAMP(0)) - :t1) < 5;

-- Cleanup
DROP TABLE t;


--
-- Check that schema size is calculated correctly when the schema
-- contains partitioned tables and ordinary ones.

-- start_ignore
DROP SCHEMA IF EXISTS test CASCADE;
-- end_ignore
CREATE SCHEMA test;

CREATE TABLE test.t1 (i INT, j INT)
DISTRIBUTED BY (i)
PARTITION BY RANGE (i)
  SUBPARTITION BY RANGE (j)
  SUBPARTITION TEMPLATE (SUBPARTITION sp START (0) END (2) EVERY(1))
(PARTITION p START (0) END (3) EVERY(1));

INSERT INTO test.t1 (i, j)
SELECT a % 3, a % 2 FROM generate_series(0, 2 * 3 - 1) a;

CREATE TABLE test.t2 AS SELECT 1 i DISTRIBUTED BY(i);

SELECT relsizes_stats_schema.relsizes_collect_stats_once();

SELECT size = 32768 * 2 * 3 /* t1 */ + 32768 /* t2 */
  FROM relsizes_stats_schema.namespace_sizes
 WHERE nspname = 'test';

SELECT relname, segment, own_file, size
  FROM relsizes_stats_schema.table_files
 WHERE nspname = 'test'
ORDER BY relname, segment, own_file;

DROP SCHEMA test CASCADE;
