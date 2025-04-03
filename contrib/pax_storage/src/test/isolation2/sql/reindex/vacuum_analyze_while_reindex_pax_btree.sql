DROP TABLE IF EXISTS reindex_pax;

CREATE TABLE reindex_pax (a INT);
insert into reindex_pax select generate_series(1,1000);
analyze reindex_pax;
select 1 as reltuples_same_as_count from pg_class where relname = 'reindex_pax'  and reltuples = (select count(*) from reindex_pax);
insert into reindex_pax select generate_series(1,1000);
select 1 as reltuples_same_as_count from pg_class where relname = 'reindex_pax'  and reltuples = (select count(*) from reindex_pax);
create index idx_btree_reindex_vacuum_analyze_pax on reindex_pax(a);
-- @Description Ensures that a vacuum during reindex operations is ok
-- 

DELETE FROM reindex_pax WHERE a < 128;
1: BEGIN;
-- Remember index relfilenodes from master and segments before
-- reindex.
1: create temp table old_relfilenodes as
   (select gp_segment_id as dbid, relfilenode, oid, relname from gp_dist_random('pg_class')
    where relname = 'idx_btree_reindex_vacuum_analyze_pax'
    union all
    select gp_segment_id as dbid, relfilenode, oid, relname from pg_class
    where relname = 'idx_btree_reindex_vacuum_analyze_pax');
1: REINDEX index idx_btree_reindex_vacuum_analyze_pax;
2&: VACUUM ANALYZE reindex_pax;
1: COMMIT;
2<:
-- Validate that reindex changed all index relfilenodes on master as well as
-- segments.  The following query should return 0 tuples.
1: select oldrels.* from old_relfilenodes oldrels join
   (select gp_segment_id as dbid, relfilenode, relname from gp_dist_random('pg_class')
    where relname = 'idx_btree_reindex_vacuum_analyze_pax'
    union all
    select gp_segment_id as dbid, relfilenode, relname from pg_class
    where relname = 'idx_btree_reindex_vacuum_analyze_pax') newrels
    on oldrels.relfilenode = newrels.relfilenode
    and oldrels.dbid = newrels.dbid
    and oldrels.relname = newrels.relname;
2: COMMIT;
3: SELECT COUNT(*) FROM reindex_pax WHERE a = 1500;

3: SELECT 1 as reltuples_same_as_count from pg_class where relname = 'reindex_pax'  and reltuples = (SELECT count(*) from reindex_pax);
3: INSERT INTO reindex_pax VALUES (0);
