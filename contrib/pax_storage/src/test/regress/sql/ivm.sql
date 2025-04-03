
DROP TABLE IF EXISTS t0 CASCADE;
DROP TABLE IF EXISTS t1 CASCADE;

-- multi insert in udf function
CREATE TABLE t0 (a int, b int) DISTRIBUTED BY (a);
CREATE TABLE t1 (a int, b int) DISTRIBUTED BY (a);
CREATE INCREMENTAL MATERIALIZED VIEW m AS SELECT t0.a, t0.b FROM t0, t1 WHERE t0.a = t1.b DISTRIBUTED BY (a);
CREATE OR REPLACE FUNCTION insert_t0_and_t1(val integer, v2 integer) RETURNS void AS $$
BEGIN
    INSERT INTO t0 VALUES (val, v2);
    INSERT INTO t1 VALUES (val, v2);
END;
$$ LANGUAGE plpgsql;
select insert_t0_and_t1(1,1);
select insert_t0_and_t1(2,2);
select insert_t0_and_t1(3,3);
select * from m order by 1;

-- use rule to trigger insert
DROP TABLE t0 CASCADE;
DROP TABLE t1 CASCADE;
CREATE TABLE t0 (a int, b int) DISTRIBUTED BY (a);
CREATE TABLE t1 (a int, b int) DISTRIBUTED BY (a);
CREATE INCREMENTAL MATERIALIZED VIEW m AS SELECT t0.a FROM t0, t1 WHERE t0.a = t1.a DISTRIBUTED BY (a);
CREATE RULE insert_t0_and_t1 AS
    ON INSERT TO t0 DO ALSO
    INSERT INTO t1 (a, b) VALUES (NEW.a, NEW.b);
INSERT INTO t0 VALUES (1,1);
INSERT INTO t0 VALUES (2,2);
INSERT INTO t0 VALUES (3,3);
select * from m order by 1;


-- one base table and multi incremental view
DROP TABLE t0 CASCADE;
DROP TABLE t1 CASCADE;

CREATE TABLE t0 (a int) DISTRIBUTED BY (a);
CREATE INCREMENTAL MATERIALIZED VIEW m1 AS SELECT sum(a) FROM t0;
CREATE INCREMENTAL MATERIALIZED VIEW m2 AS SELECT sum(a) FROM t0;
BEGIN;
INSERT INTO t0 VALUES (1), (2);
INSERT INTO t0 VALUES (3);
COMMIT;
SELECT * FROM m1 order by 1;
SELECT * FROM m2 order by 1;
-- abort transaction
BEGIN;
INSERT INTO t0 VALUES (4);
ABORT;
SELECT * FROM m1;
-- subtransaction
BEGIN;
INSERT INTO t0 VALUES (5);
SAVEPOINT p1;
INSERT INTO t0 VALUES (6);
ROLLBACK TO SAVEPOINT p1;
INSERT INTO t0 VALUES (6);
SELECT * FROM m1;
COMMIT;
-- test copy
COPY t0 (a) from stdin;
7
8
9
10
\.
-- test insert batch
INSERT INTO t0 SELECT * FROM generate_series(11, 100000);
SELECT * FROM m1;
SELECT * FROM m2;

-- cleanup
DROP TABLE t0 CASCADE;
