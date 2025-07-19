-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

DROP TABLE IF EXISTS reindex_crtab_pax_bitmap;

CREATE TABLE reindex_crtab_pax_bitmap (a INT);
insert into reindex_crtab_pax_bitmap select generate_series(1,1000);
insert into reindex_crtab_pax_bitmap select generate_series(1,1000);
create index idx_reindex_crtab_pax_bitmap on reindex_crtab_pax_bitmap USING BITMAP(a);
-- @Description Ensures that a reindex table during reindex index operations is ok
-- 

DELETE FROM reindex_crtab_pax_bitmap WHERE a < 128;
1: BEGIN;
1: create temp table old_relfilenodes as
   (select gp_segment_id as dbid, relfilenode, oid, relname from gp_dist_random('pg_class')
    where relname = 'idx_reindex_crtab_pax_bitmap'
    union all
    select gp_segment_id as dbid, relfilenode, oid, relname from pg_class
    where relname = 'idx_reindex_crtab_pax_bitmap');
2: BEGIN;
1: REINDEX index idx_reindex_crtab_pax_bitmap;
2&: REINDEX TABLE  reindex_crtab_pax_bitmap;
1: COMMIT;
2<:
-- Session 2 has not committed yet.  Session 1 should see effects of
-- its own reindex command above in pg_class.  The following query
-- validates that reindex command in session 1 indeed generates new
-- relfilenode for the index.
1: insert into old_relfilenodes
   (select gp_segment_id as dbid, relfilenode, oid, relname from gp_dist_random('pg_class')
    where relname = 'idx_reindex_crtab_pax_bitmap'
    union all
    select gp_segment_id as dbid, relfilenode, oid, relname from pg_class
    where relname = 'idx_reindex_crtab_pax_bitmap');
-- Expect two distinct relfilenodes per segment in old_relfilenodes table.
1: select distinct count(distinct relfilenode), relname from old_relfilenodes group by dbid, relname;
2: COMMIT;
-- After session 2 commits, the relfilenode it assigned to the index
-- is visible to session 1.
1: insert into old_relfilenodes
   (select gp_segment_id as dbid, relfilenode, oid, relname from gp_dist_random('pg_class')
    where relname = 'idx_reindex_crtab_pax_bitmap'
    union all
    select gp_segment_id as dbid, relfilenode, oid, relname from pg_class
    where relname = 'idx_reindex_crtab_pax_bitmap');
-- Expect three distinct relfilenodes per segment in old_relfilenodes table.
1: select distinct count(distinct relfilenode), relname from old_relfilenodes group by dbid, relname;

3: select count(*) from reindex_crtab_pax_bitmap where a = 1000;
3: set enable_seqscan=false;
3: set enable_indexscan=true;
3: select count(*) from reindex_crtab_pax_bitmap where a = 1000;
