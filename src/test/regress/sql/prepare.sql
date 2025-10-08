-- start_matchsubs
-- m/Execution time: \d+\.\d+ ms/
-- s/Execution time: \d+\.\d+ ms/Execution time: ##.### ms/
-- s/Executor memory: (\d+)\w bytes \(seg\d+\)\./Executor memory: (#####)K bytes (seg#)./
-- s/Executor memory: (\d+)\w bytes avg x \d+ workers, \d+\w bytes max \(seg\d+\)\./Executor memory: ####K bytes avg x #### workers, ####K bytes max (seg#)./
-- m/Memory used:  \d+\w?B/
-- s/Memory used:  \d+\w?B/Memory used: ###B/
-- end_matchsubs

-- Regression tests for prepareable statements. We query the content
-- of the pg_prepared_statements view as prepared statements are
-- created and removed.

SELECT name, statement, parameter_types FROM pg_prepared_statements;

PREPARE q1 AS SELECT 1 AS a;
EXECUTE q1;

SELECT name, statement, parameter_types FROM pg_prepared_statements;

-- should fail
PREPARE q1 AS SELECT 2;

-- should succeed
DEALLOCATE q1;
PREPARE q1 AS SELECT 2;
EXECUTE q1;

PREPARE q2 AS SELECT 2 AS b;
SELECT name, statement, parameter_types FROM pg_prepared_statements;

-- sql92 syntax
DEALLOCATE PREPARE q1;

SELECT name, statement, parameter_types FROM pg_prepared_statements;

DEALLOCATE PREPARE q2;
-- the view should return the empty set again
SELECT name, statement, parameter_types FROM pg_prepared_statements;

-- parameterized queries
PREPARE q2(text) AS
	SELECT datname, datistemplate, datallowconn
	FROM pg_database WHERE datname = $1;

EXECUTE q2('postgres');

PREPARE q3(text, int, float, boolean, oid, smallint) AS
	SELECT * FROM tenk1 WHERE string4 = $1 AND (four = $2 OR
	ten = $3::bigint OR true = $4 OR oid = $5 OR odd = $6::int)
	ORDER BY unique1;

EXECUTE q3('AAAAxx', 5::smallint, 10.5::float, false, 500::oid, 4::bigint);

-- too few params
EXECUTE q3('bool');

-- too many params
EXECUTE q3('bytea', 5::smallint, 10.5::float, false, 500::oid, 4::bigint, true);

-- wrong param types
EXECUTE q3(5::smallint, 10.5::float, false, 500::oid, 4::bigint, 'bytea');

-- invalid type
PREPARE q4(nonexistenttype) AS SELECT $1;

-- create table as execute
PREPARE q5(int, text) AS
	SELECT * FROM tenk1 WHERE unique1 = $1 OR stringu1 = $2
	ORDER BY unique1;
CREATE TEMPORARY TABLE q5_prep_results AS EXECUTE q5(200, 'DTAAAA');
SELECT * FROM q5_prep_results;

-- ensure EXPLAIN ANALYZE CTAS from prepared statement creates table
PREPARE p AS SELECT 1 i;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF)
CREATE TEMPORARY TABLE t AS EXECUTE p;
SELECT * FROM t;
DEALLOCATE p;

-- unknown or unspecified parameter types: should succeed
PREPARE q6 AS
    SELECT * FROM tenk1 WHERE unique1 = $1 AND stringu1 = $2;
PREPARE q7(unknown) AS
    SELECT * FROM road WHERE thepath = $1;

SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;

-- make sure the plan is correct after CTAS
DROP TABLE t;
PREPARE p AS SELECT * FROM generate_series(1, 10) i;
CREATE TEMPORARY TABLE t AS EXECUTE p;
EXPLAIN (COSTS OFF) EXECUTE p;

-- test DEALLOCATE ALL;
DEALLOCATE ALL;
SELECT name, statement, parameter_types FROM pg_prepared_statements
    ORDER BY name;
