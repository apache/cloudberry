--
-- Test lake table DDL: FOREIGN CATALOG, FOREIGN VOLUME, ICEBERG TABLE
--

-- Display the lake table catalogs
\d+ pg_foreign_catalog
\d+ pg_foreign_volume
\d+ pg_lake_table

-- Setup: foreign servers for the catalogs and volumes to hang off
CREATE FOREIGN DATA WRAPPER lake_test_fdw;
CREATE SERVER lake_test_srv FOREIGN DATA WRAPPER lake_test_fdw;
CREATE SERVER lake_test_srv2 FOREIGN DATA WRAPPER lake_test_fdw;

-- CREATE FOREIGN CATALOG
CREATE FOREIGN CATALOG lake_test_cat SERVER lake_test_srv OPTIONS (type 'hive', uri 'thrift://localhost:9083');
CREATE FOREIGN CATALOG lake_test_cat SERVER lake_test_srv;			-- fail, duplicate
CREATE FOREIGN CATALOG IF NOT EXISTS lake_test_cat SERVER lake_test_srv;	-- skip with notice
-- catalog names are global: the same name on another server is still a duplicate
CREATE FOREIGN CATALOG lake_test_cat SERVER lake_test_srv2;			-- fail, duplicate
CREATE FOREIGN CATALOG IF NOT EXISTS lake_test_cat SERVER lake_test_srv2;	-- skip with notice
CREATE FOREIGN CATALOG lake_test_bad SERVER no_such_server;		-- fail, no server
SELECT fcname, fcoptions FROM pg_foreign_catalog WHERE fcname LIKE 'lake\_test%' ORDER BY 1;

-- CREATE FOREIGN VOLUME
CREATE FOREIGN VOLUME lake_test_vol SERVER lake_test_srv OPTIONS (path 's3://bucket/prefix');
CREATE FOREIGN VOLUME lake_test_vol SERVER lake_test_srv;			-- fail, duplicate
CREATE FOREIGN VOLUME IF NOT EXISTS lake_test_vol SERVER lake_test_srv;	-- skip with notice
-- volume names are global: the same name on another server is still a duplicate
CREATE FOREIGN VOLUME lake_test_vol SERVER lake_test_srv2;			-- fail, duplicate
CREATE FOREIGN VOLUME IF NOT EXISTS lake_test_vol SERVER lake_test_srv2;	-- skip with notice
CREATE FOREIGN VOLUME lake_test_bad SERVER no_such_server;			-- fail, no server
SELECT fvname, fvoptions FROM pg_foreign_volume WHERE fvname LIKE 'lake\_test%' ORDER BY 1;

-- Object descriptions
SELECT pg_catalog.pg_describe_object('pg_foreign_catalog'::regclass, oid, 0)
  FROM pg_foreign_catalog WHERE fcname = 'lake_test_cat';
SELECT pg_catalog.pg_describe_object('pg_foreign_volume'::regclass, oid, 0)
  FROM pg_foreign_volume WHERE fvname = 'lake_test_vol';

-- Catalog and volume rows are dispatched to all segments
SELECT count(DISTINCT gp_segment_id) > 1 AS on_all_segments
  FROM gp_dist_random('pg_foreign_catalog') WHERE fcname = 'lake_test_cat';
SELECT count(DISTINCT gp_segment_id) > 1 AS on_all_segments
  FROM gp_dist_random('pg_foreign_volume') WHERE fvname = 'lake_test_vol';

-- Without a provider extension there is no iceberg table AM
CREATE ICEBERG TABLE lake_test_t0 (a int) CATALOG lake_test_cat VOLUME lake_test_vol;	-- fail with hint

-- The default catalog/volume GUCs verify that the object exists
SET iceberg_default_catalog = 'no_such_catalog';	-- fail
SET iceberg_default_volume = 'no_such_volume';		-- fail

-- Simulate a datalake provider with a heap-backed iceberg AM
CREATE ACCESS METHOD iceberg TYPE TABLE HANDLER heap_tableam_handler;

-- CREATE ICEBERG TABLE with explicit catalog and volume
CREATE ICEBERG TABLE lake_test_t1 (a int, b text) CATALOG lake_test_cat VOLUME lake_test_vol OPTIONS (fileformat 'parquet');
SELECT c.relname, lt.lttable_type, lt.ltoptions, fc.fcname, fv.fvname
  FROM pg_lake_table lt
  JOIN pg_class c ON c.oid = lt.ltrelid
  JOIN pg_foreign_catalog fc ON fc.oid = lt.ltforeign_catalog
  JOIN pg_foreign_volume fv ON fv.oid = lt.ltforeign_volume
 ORDER BY 1;
SELECT a.amname FROM pg_am a JOIN pg_class c ON c.relam = a.oid WHERE c.relname = 'lake_test_t1';
-- Lake tables are always DISTRIBUTED RANDOMLY (policytype 'p', no distkey)
SELECT policytype, distkey FROM gp_distribution_policy WHERE localoid = 'lake_test_t1'::regclass;

-- The (heap-backed) table is usable
INSERT INTO lake_test_t1 VALUES (1, 'x'), (2, 'y');
SELECT count(*) FROM lake_test_t1;

-- Lake tables get a TOAST table like plain tables, so wide values work
SELECT reltoastrelid <> 0 AS has_toast FROM pg_class WHERE relname = 'lake_test_t1';
INSERT INTO lake_test_t1 VALUES (3, repeat('x', 500000));
SELECT a, length(b) FROM lake_test_t1 WHERE a = 3;

-- Catalog and volume are both required
CREATE ICEBERG TABLE lake_test_t2 (a int) VOLUME lake_test_vol;		-- fail, no catalog
CREATE ICEBERG TABLE lake_test_t2 (a int) CATALOG lake_test_cat;	-- fail, no volume

-- ... unless the GUCs provide defaults
SET iceberg_default_catalog = 'lake_test_cat';
SET iceberg_default_volume = 'lake_test_vol';
CREATE ICEBERG TABLE lake_test_t2 (a int);
RESET iceberg_default_catalog;
RESET iceberg_default_volume;

-- A DISTRIBUTED clause is ignored with a warning
CREATE ICEBERG TABLE lake_test_t3 (a int) CATALOG lake_test_cat VOLUME lake_test_vol DISTRIBUTED BY (a);
SELECT policytype, distkey FROM gp_distribution_policy WHERE localoid = 'lake_test_t3'::regclass;

-- CREATE ICEBERG TABLE only accepts the iceberg access method
CREATE ICEBERG TABLE lake_test_bad0 (a int) CATALOG lake_test_cat VOLUME lake_test_vol USING heap;	-- fail
CREATE ICEBERG TABLE lake_test_t4 (a int) CATALOG lake_test_cat VOLUME lake_test_vol USING iceberg;	-- explicit iceberg is fine

-- The iceberg AM is rejected for every path other than CREATE ICEBERG TABLE
CREATE TABLE lake_test_bad1 (a int) USING iceberg DISTRIBUTED RANDOMLY;			-- fail
CREATE TABLE lake_test_bad2 USING iceberg AS SELECT 1 AS a DISTRIBUTED RANDOMLY;	-- fail
SET default_table_access_method = iceberg;
CREATE TABLE lake_test_bad3 (a int) DISTRIBUTED RANDOMLY;				-- fail
RESET default_table_access_method;
CREATE TABLE lake_test_heap (a int) DISTRIBUTED RANDOMLY;
ALTER TABLE lake_test_heap SET ACCESS METHOD iceberg;					-- fail
ALTER TABLE lake_test_t1 SET ACCESS METHOD heap;					-- fail
ALTER TABLE lake_test_t1 SET DISTRIBUTED BY (a);					-- fail

-- Only the owner can drop a catalog or volume
CREATE ROLE regress_lake_user;
SET ROLE regress_lake_user;
DROP FOREIGN CATALOG lake_test_cat;	-- fail, not owner
DROP FOREIGN VOLUME lake_test_vol;	-- fail, not owner
RESET ROLE;

-- Dependencies: the server holds the catalog/volume, which hold the tables
DROP SERVER lake_test_srv;		-- fail, catalog and volume depend on it
DROP FOREIGN CATALOG lake_test_cat;	-- fail, tables depend on it

-- Dropping a lake table removes its pg_lake_table entry
DROP ICEBERG TABLE lake_test_t1;
SELECT count(*) FROM pg_lake_table lt JOIN pg_class c ON c.oid = lt.ltrelid
 WHERE c.relname = 'lake_test_t1';

-- DROP ICEBERG TABLE rejects a non-iceberg table ...
DROP ICEBERG TABLE lake_test_heap;	-- fail, not an iceberg table
-- ... while DROP TABLE still removes an iceberg table (no reverse restriction)
DROP TABLE lake_test_t2;
SELECT count(*) FROM pg_lake_table lt JOIN pg_class c ON c.oid = lt.ltrelid
 WHERE c.relname = 'lake_test_t2';

-- DROP FOREIGN CATALOG ... CASCADE takes the remaining tables with it
DROP FOREIGN CATALOG lake_test_cat CASCADE;
SELECT count(*) FROM pg_lake_table lt JOIN pg_class c ON c.oid = lt.ltrelid
 WHERE c.relname LIKE 'lake\_test%';

-- DROP variants
DROP FOREIGN CATALOG lake_test_cat;		-- fail, already gone
DROP FOREIGN CATALOG IF EXISTS lake_test_cat;	-- skip with notice
DROP FOREIGN VOLUME lake_test_vol;
DROP FOREIGN VOLUME lake_test_vol;		-- fail, already gone
DROP FOREIGN VOLUME IF EXISTS lake_test_vol;	-- skip with notice

-- Cleanup
DROP TABLE lake_test_heap;
DROP ROLE regress_lake_user;
DROP SERVER lake_test_srv;
DROP SERVER lake_test_srv2;
DROP FOREIGN DATA WRAPPER lake_test_fdw;
DROP ACCESS METHOD iceberg;
