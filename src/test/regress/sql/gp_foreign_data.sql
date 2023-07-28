--
-- Test foreign-data wrapper and server management. Cloudberry MPP specific
--

-- start_ignore
DROP SERVER s0 CASCADE;
DROP SERVER s1 CASCADE;
DROP FOREIGN DATA WRAPPER dummy CASCADE;
-- end_ignore

CREATE FOREIGN DATA WRAPPER dummy OPTIONS (mpp_execute 'coordinator');;
COMMENT ON FOREIGN DATA WRAPPER dummy IS 'useless';
ALTER FOREIGN DATA WRAPPER dummy OPTIONS (SET mpp_execute 'all segments');

-- CREATE FOREIGN TABLE
CREATE SERVER s0 FOREIGN DATA WRAPPER dummy;
CREATE FOREIGN TABLE ft2 (
	c1 int
) SERVER s0 OPTIONS (delimiter ',', mpp_execute 'a');           -- ERROR
CREATE FOREIGN TABLE ft2 (
	c1 int
) SERVER s0 OPTIONS (delimiter ',', mpp_execute 'any');
\d+ ft2
CREATE FOREIGN TABLE ft3 (
	c1 int
) SERVER s0 OPTIONS (delimiter ',', mpp_execute 'master');
CREATE FOREIGN TABLE ft4 (
	c1 int
) SERVER s0 OPTIONS (delimiter ',', mpp_execute 'all segments');

-- Test num_segments option
CREATE SERVER s1 FOREIGN DATA WRAPPER dummy OPTIONS (num_segments '3');
CREATE FOREIGN TABLE ft5 (
       c1 int
) SERVER s1 OPTIONS (delimiter ',', mpp_execute 'all segments', num_segments '5');
\d+ ft5

-- CREATE FOREIGN TABLE LIKE
CREATE TABLE ft_source_table(a INT, b INT, c INT) DISTRIBUTED BY (b);
CREATE FOREIGN TABLE ft_like (LIKE ft_source_table) SERVER s0; 
\d+ ft_like
-- shoule be null
SELECT * FROM gp_distribution_policy WHERE localoid = 'ft_like'::regclass::oid;
CREATE FOREIGN TABLE ft_like1 (LIKE ft_source_table) SERVER s0
OPTIONS (delimiter ',', mpp_execute 'all segments', num_segments '3');
\d+ ft_like1
SELECT * FROM gp_distribution_policy WHERE localoid = 'ft_like1'::regclass::oid;

DROP TABLE ft_source_table;
DROP FOREIGN TABLE ft_like;
DROP FOREIGN TABLE ft_like1;

--start_ignore
DROP FOREIGN DATA WRAPPER dummy CASCADE;
--end_ignore
