DROP TABLE IF EXISTS reindex_abort_pax;

CREATE TABLE reindex_abort_pax (a INT);
insert into reindex_abort_pax select generate_series(1,1000);
create index idx_btree_reindex_abort_pax on reindex_abort_pax(a);

-- start_ignore
drop table if exists reindex_abort_pax_old;
create table reindex_abort_pax_old as
  (select oid as c_oid, gp_segment_id as c_gp_segment_id, relfilenode as c_relfilenode from pg_class where relname = 'idx_btree_reindex_abort_pax' union all
   select oid as c_oid, gp_segment_id as c_gp_segment_id, relfilenode as c_relfilenode from gp_dist_random('pg_class') where relname = 'idx_btree_reindex_abort_pax');
-- end_ignore

select 1 as have_same_number_of_rows from reindex_abort_pax_old where c_gp_segment_id > -1 group by c_oid having count(*) = (select count(*) from gp_segment_configuration where role = 'p' and content > -1);
-- @Description Ensures that relnode for an index does not change if reindex command is aborted
-- 

1: BEGIN;
1: REINDEX index idx_btree_reindex_abort_pax;
1: ROLLBACK;
3: SELECT 1 AS oid_same_on_all_segs from gp_dist_random('pg_class') WHERE relname = 'idx_btree_reindex_abort_pax' GROUP BY oid having count(*) = (SELECT count(*) FROM gp_segment_configuration WHERE role='p' AND content > -1);
3: SELECT 1 as relfilenode_didnot_change from pg_class pc, reindex_abort_pax_old re where pc.relname = 'idx_btree_reindex_abort_pax' and pc.relfilenode = re.c_relfilenode and pc.gp_segment_id = re.c_gp_segment_id;
3: SELECT * from reindex_abort_pax_old o where o.c_gp_segment_id > -1 and exists (select oid, gp_segment_id, relfilenode from gp_dist_random('pg_class') g where relname = 'idx_btree_reindex_abort_pax' and o.c_oid = g.oid and o.c_gp_segment_id = g.gp_segment_id and o.c_relfilenode != g.relfilenode);
