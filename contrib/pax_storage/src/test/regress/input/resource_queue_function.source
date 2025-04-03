
CREATE FUNCTION checkResourceQueueMemoryLimits(cstring) RETURNS boolean
AS '@abs_builddir@/regress@DLSUFFIX@', 'checkResourceQueueMemoryLimits' LANGUAGE C READS SQL DATA;

CREATE TABLE test_table(c1 int, c2 int); 
INSERT INTO test_table values(1, 2);

-- MPP-17240
CREATE RESOURCE QUEUE test_q WITH (ACTIVE_STATEMENTS = 1, MEMORY_LIMIT='10MB');
CREATE ROLE test_role WITH RESOURCE QUEUE test_q;
GRANT SELECT ON test_table TO test_role;

SET STATEMENT_MEM = '125MB';

-- should return true
select checkResourceQueueMemoryLimits('test_q'); 

SET ROLE test_role; 

-- should deadlock as there aren't enough resources to run the query.
-- query should be assigned 125MB, but queue only allows 10MB 
SELECT count(*) FROM test_table; 

RESET ROLE;

-- should return true
select checkResourceQueueMemoryLimits('test_q'); 

RESET STATEMENT_MEM;

REVOKE SELECT ON test_table FROM test_role;
DROP ROLE test_role; 
DROP RESOURCE QUEUE test_q;

-- MPP-15992
CREATE RESOURCE QUEUE test_q WITH (ACTIVE_STATEMENTS = 1, MEMORY_LIMIT='8GB');
CREATE ROLE test_role WITH RESOURCE QUEUE test_q;
GRANT SELECT ON test_table TO test_role;

SET ROLE test_role; 

-- should return result 
SELECT count(*) FROM test_table; 

RESET ROLE;

ALTER RESOURCE QUEUE test_q WITH (ACTIVE_STATEMENTS = 1, MEMORY_LIMIT='10GB');

SET ROLE test_role; 

-- should return result 
SELECT count(*) FROM test_table; 

RESET ROLE;
-- should return true
select checkResourceQueueMemoryLimits('test_q'); 

ALTER RESOURCE QUEUE test_q WITH (ACTIVE_STATEMENTS = 1, MEMORY_LIMIT='7GB');

SET ROLE test_role; 

-- should return result 
SELECT count(*) FROM test_table; 

RESET ROLE;

-- should return true
select checkResourceQueueMemoryLimits('test_q'); 

REVOKE SELECT ON test_table FROM test_role;
DROP TABLE test_table;
DROP ROLE test_role; 
DROP RESOURCE QUEUE test_q;
DROP FUNCTION checkResourceQueueMemoryLimits(cstring);
