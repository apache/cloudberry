--
-- Create new table t
--
CREATE TEMPORARY TABLE t(pid INT, wid INT, state CHARACTER VARYING(30));

--
-- Insert a row and keep state as empty
--
INSERT INTO t VALUES(1, 1);

--
-- use nested decode
--
SELECT DECODE(DECODE(state, '', NULL, state), '-', NULL, state) AS state FROM t;
