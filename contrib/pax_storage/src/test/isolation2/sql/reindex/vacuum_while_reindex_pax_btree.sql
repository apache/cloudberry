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

DROP TABLE IF EXISTS reindex_pax;

CREATE TABLE reindex_pax (a INT); 
insert into reindex_pax select generate_series(1,1000);
insert into reindex_pax select generate_series(1,1000);
create index idx_btree_reindex_pax on reindex_pax(a);
-- @Description Ensures that a vacuum during reindex operations is ok
-- 

DELETE FROM reindex_pax WHERE a < 128;
1: BEGIN;
-- Remember index relfilenodes from master and segments before
-- reindex.
1: create temp table old_relfilenodes as
   (select gp_segment_id as dbid, relfilenode, oid, relname from gp_dist_random('pg_class')
    where relname = 'idx_btree_reindex_pax'
    union all
    select gp_segment_id as dbid, relfilenode, oid, relname from pg_class
    where relname = 'idx_btree_reindex_pax');
1: REINDEX index idx_btree_reindex_pax;
2&: VACUUM reindex_pax;
1: COMMIT;
2<:
-- Validate that reindex changed all index relfilenodes on master as well as
-- segments.  The following query should return 0 tuples.
1: select oldrels.* from old_relfilenodes oldrels join
   (select gp_segment_id as dbid, relfilenode, relname from gp_dist_random('pg_class')
    where relname = 'idx_btree_reindex_pax'
    union all
    select gp_segment_id as dbid, relfilenode, relname from pg_class
    where relname = 'idx_btree_reindex_pax') newrels
    on oldrels.relfilenode = newrels.relfilenode
    and oldrels.dbid = newrels.dbid
    and oldrels.relname = newrels.relname;
2: COMMIT;
3: SELECT COUNT(*) FROM reindex_pax WHERE a = 1500;
3: INSERT INTO reindex_pax VALUES (0);
