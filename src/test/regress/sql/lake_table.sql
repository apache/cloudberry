--
-- Test lake table DDL: FOREIGN CATALOG, FOREIGN VOLUME, LAKE TABLE
--

-- Display the lake table catalogs
\d+ pg_foreign_catalog
\d+ pg_foreign_volume
\d+ pg_lake_table

-- Setup: foreign servers for the catalogs and volumes to hang off
CREATE FOREIGN DATA WRAPPER lake_test_fdw;
CREATE SERVER lake_test_srv FOREIGN DATA WRAPPER lake_test_fdw;
CREATE SERVER lake_test_srv2 FOREIGN DATA WRAPPER lake_test_fdw;

-- CREATE FOREIGN CATALOG: TYPE is a required first-class property
CREATE FOREIGN CATALOG lake_test_cat SERVER lake_test_srv TYPE 'hive' OPTIONS (uri 'thrift://localhost:9083');
CREATE FOREIGN CATALOG lake_test_notype SERVER lake_test_srv;		-- fail, TYPE is required
CREATE FOREIGN CATALOG lake_test_cat SERVER lake_test_srv TYPE 'hive';			-- fail, duplicate
CREATE FOREIGN CATALOG IF NOT EXISTS lake_test_cat SERVER lake_test_srv TYPE 'hive';	-- skip with notice
-- catalog names are global: the same name on another server is still a duplicate
CREATE FOREIGN CATALOG lake_test_cat SERVER lake_test_srv2 TYPE 'hive';			-- fail, duplicate
CREATE FOREIGN CATALOG IF NOT EXISTS lake_test_cat SERVER lake_test_srv2 TYPE 'hive';	-- skip with notice
CREATE FOREIGN CATALOG lake_test_bad SERVER no_such_server TYPE 'hive';		-- fail, no server
SELECT fcname, fctype, fcoptions FROM pg_foreign_catalog WHERE fcname LIKE 'lake\_test%';

-- CREATE FOREIGN VOLUME
CREATE FOREIGN VOLUME lake_test_vol SERVER lake_test_srv OPTIONS (base_path 's3://bucket/prefix');
CREATE FOREIGN VOLUME lake_test_vol SERVER lake_test_srv;			-- fail, duplicate
CREATE FOREIGN VOLUME IF NOT EXISTS lake_test_vol SERVER lake_test_srv;	-- skip with notice
-- volume names are global: the same name on another server is still a duplicate
CREATE FOREIGN VOLUME lake_test_vol SERVER lake_test_srv2;			-- fail, duplicate
CREATE FOREIGN VOLUME IF NOT EXISTS lake_test_vol SERVER lake_test_srv2;	-- skip with notice
CREATE FOREIGN VOLUME lake_test_bad SERVER no_such_server;			-- fail, no server
SELECT fvname, fvoptions FROM pg_foreign_volume WHERE fvname LIKE 'lake\_test%';

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
CREATE LAKE TABLE lake_test_t0 (a int) USING ICEBERG CATALOG lake_test_cat VOLUME lake_test_vol;	-- fail with hint

-- The default catalog/volume GUCs verify that the object exists
SET iceberg_default_catalog = 'no_such_catalog';	-- fail
SET iceberg_default_volume = 'no_such_volume';		-- fail

-- Simulate a datalake provider with a heap-backed iceberg AM
CREATE ACCESS METHOD iceberg TYPE TABLE HANDLER heap_tableam_handler;

-- CREATE LAKE TABLE with explicit catalog and volume.  The format is the
-- table's access method (pg_class.relam) and its options are the relation's
-- reloptions; pg_lake_table records only the catalog/volume binding.
CREATE LAKE TABLE lake_test_t1 (a int, b text) USING ICEBERG CATALOG lake_test_cat VOLUME lake_test_vol;
SELECT c.relname, am.amname, c.reloptions, fc.fcname, fv.fvname
  FROM pg_lake_table lt
  JOIN pg_class c ON c.oid = lt.ltrelid
  JOIN pg_am am ON am.oid = c.relam
  JOIN pg_foreign_catalog fc ON fc.oid = lt.ltforeign_catalog
  JOIN pg_foreign_volume fv ON fv.oid = lt.ltforeign_volume;
-- Lake tables are always DISTRIBUTED RANDOMLY (policytype 'p', no distkey)
SELECT policytype, distkey FROM gp_distribution_policy WHERE localoid = 'lake_test_t1'::regclass;

-- The (heap-backed) table is usable
INSERT INTO lake_test_t1 VALUES (1, 'x'), (2, 'y');
SELECT count(*) FROM lake_test_t1;

-- OPTIONS become the relation's reloptions and are validated by the access
-- method: a value the AM accepts is stored, an unknown one is rejected.
CREATE LAKE TABLE lake_test_opt (a int) USING ICEBERG CATALOG lake_test_cat VOLUME lake_test_vol OPTIONS (fillfactor '70');
SELECT reloptions FROM pg_class WHERE relname = 'lake_test_opt';
CREATE LAKE TABLE lake_test_optbad (a int) USING ICEBERG CATALOG lake_test_cat VOLUME lake_test_vol OPTIONS (bogus_opt 'x');	-- fail, AM rejects unknown option
DROP LAKE TABLE lake_test_opt;

-- Catalog and volume are both required
CREATE LAKE TABLE lake_test_t2 (a int) USING ICEBERG VOLUME lake_test_vol;	-- fail, no catalog
CREATE LAKE TABLE lake_test_t2 (a int) USING ICEBERG CATALOG lake_test_cat;	-- fail, no volume

-- ... unless the GUCs provide defaults
SET iceberg_default_catalog = 'lake_test_cat';
SET iceberg_default_volume = 'lake_test_vol';
CREATE LAKE TABLE lake_test_t2 (a int) USING ICEBERG;
RESET iceberg_default_catalog;
RESET iceberg_default_volume;

-- A DISTRIBUTED clause is rejected (lake tables are always distributed randomly)
CREATE LAKE TABLE lake_test_t3 (a int) USING ICEBERG CATALOG lake_test_cat VOLUME lake_test_vol DISTRIBUTED BY (a);	-- fail

-- The USING clause names the table format; only ICEBERG is supported (any case)
CREATE LAKE TABLE lake_test_bad0 (a int) USING heap CATALOG lake_test_cat VOLUME lake_test_vol;		-- fail, unsupported format
CREATE LAKE TABLE lake_test_t4 (a int) USING "IceBerg" CATALOG lake_test_cat VOLUME lake_test_vol;	-- quoted mixed-case format resolves the iceberg AM

-- The iceberg AM is rejected for every path other than CREATE LAKE TABLE
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

-- Dropping a lake table removes its pg_lake_table entry: remember the
-- table's OID so the check still finds an orphaned row after the drop
SELECT oid AS t1_oid FROM pg_class WHERE relname = 'lake_test_t1' \gset
DROP LAKE TABLE lake_test_t1;
SELECT count(*) FROM pg_lake_table WHERE ltrelid = :t1_oid;

-- DROP LAKE TABLE rejects a non-lake table ...
DROP LAKE TABLE lake_test_heap;	-- fail, not a lake table
-- plain DROP TABLE must reject a lake table (mirrors foreign-table behavior)
DROP TABLE lake_test_t2;
DROP TABLE IF EXISTS lake_test_t2;	-- IF EXISTS does not suppress wrong-type errors
-- the rejected drops must have left the table and its lake metadata intact
SELECT count(*) FROM pg_lake_table WHERE ltrelid = 'lake_test_t2'::regclass;
-- the correct command still works
SELECT oid AS t2_oid FROM pg_class WHERE relname = 'lake_test_t2' \gset
DROP LAKE TABLE lake_test_t2;
SELECT count(*) FROM pg_lake_table WHERE ltrelid = :t2_oid;

-- DROP FOREIGN CATALOG ... CASCADE takes the remaining tables with it
SELECT oid AS t4_oid FROM pg_class WHERE relname = 'lake_test_t4' \gset
DROP FOREIGN CATALOG lake_test_cat CASCADE;
SELECT count(*) FROM pg_lake_table WHERE ltrelid = :t4_oid;

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
