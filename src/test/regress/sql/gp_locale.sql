-- ORCA uses functions (e.g. vswprintf) to translation to wide character
-- format. But those libraries may fail if the current locale cannot handle the
-- character set. This test checks that even when those libraries fail, ORCA is
-- still able to generate plans.

--
-- Create a database that sets the minimum locale
--
DROP DATABASE IF EXISTS test_locale;
CREATE DATABASE test_locale WITH LC_COLLATE='C' LC_CTYPE='C' TEMPLATE=template0;
\c test_locale

--
-- drop/add/remove columns
--
CREATE TABLE hi_안녕세계 (a int, 안녕세계1 text, 안녕세계2 text, 안녕세계3 text) DISTRIBUTED BY (a);
ALTER TABLE hi_안녕세계 DROP COLUMN 안녕세계2;
ALTER TABLE hi_안녕세계 ADD COLUMN 안녕세계2_ADD_COLUMN text;
ALTER TABLE hi_안녕세계 RENAME COLUMN 안녕세계3 TO こんにちわ3;

INSERT INTO hi_안녕세계 VALUES(1, '안녕세계1 first', '안녕세2 first', '안녕세계3 first');
INSERT INTO hi_안녕세계 VALUES(42, '안녕세계1 second', '안녕세2 second', '안녕세계3 second');

--
-- Try various queries containing multibyte character set and check the column
-- name output
--
SET optimizer_trace_fallback=on;

-- DELETE
DELETE FROM hi_안녕세계 WHERE a=42;

-- UPDATE
UPDATE hi_안녕세계 SET 안녕세계1='안녕세계1 first UPDATE' WHERE 안녕세계1='안녕세계1 first';

-- SELECT
SELECT * FROM hi_안녕세계;

SELECT 안녕세계1 || こんにちわ3 FROM hi_안녕세계;

-- SELECT ALIAS
SELECT 안녕세계1 AS 안녕세계1_Alias FROM hi_안녕세계;

-- SUBQUERY
SELECT * FROM (SELECT 안녕세계1 FROM hi_안녕세계) t;

SELECT (SELECT こんにちわ3 FROM hi_안녕세계) FROM (SELECT 1) AS q;

SELECT (SELECT (SELECT こんにちわ3 FROM hi_안녕세계) FROM  hi_안녕세계) FROM (SELECT 1) AS q;

-- CTE
WITH cte AS
(SELECT 안녕세계1, こんにちわ3 FROM hi_안녕세계) SELECT * FROM cte WHERE 안녕세계1 LIKE '안녕세계1%';

WITH cte(안녕세계x, こんにちわx) AS
(SELECT 안녕세계1, こんにちわ3 FROM hi_안녕세계) SELECT * FROM cte WHERE 안녕세계x LIKE '안녕세계1%';

-- JOIN
SELECT * FROM hi_안녕세계 hi_안녕세계1, hi_안녕세계 hi_안녕세계2 WHERE hi_안녕세계1.안녕세계1 LIKE '%UPDATE';

-- ALIAS ON CONSTANTS, AGGREGATES AND SET OPERATIONS
-- These project elements are not Vars, so restoring the alias after a failed
-- wide character conversion must not depend on the Var-origin lookup.
SELECT '한글' AS "한글";

SELECT 1+1 AS 안녕세계표현식;

SELECT count(*) AS 안녕세계카운트 FROM hi_안녕세계;

SELECT sum(a) AS 안녕세계합계, max(a) AS 안녕세계최대 FROM hi_안녕세계;

SELECT '안녕' AS 안녕세계유니온 UNION ALL SELECT 'x';

-- SET RETURNING FUNCTION (ProjectSet can be the topmost plan node)
SELECT generate_series(1,2) AS 안녕세계SRF;

-- The restore must take the name from the top-level target list entry at the
-- same position, never from a same-position entry inside a subquery.
SELECT EXISTS(SELECT a, 안녕세계1 FROM hi_안녕세계) AS c, 안녕세계1 AS 안녕세계별칭 FROM hi_안녕세계;

-- A legitimate alias named "UNKNOWN" (no conversion failure) must survive.
SELECT EXISTS(SELECT a, 안녕세계1 FROM hi_안녕세계) AS c, a AS "UNKNOWN" FROM hi_안녕세계;

RESET optimizer_trace_fallback;
