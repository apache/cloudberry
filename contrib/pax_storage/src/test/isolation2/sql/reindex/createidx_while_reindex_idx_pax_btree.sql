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

DROP TABLE IF EXISTS reindex_crtab_pax_btree;

CREATE TABLE reindex_crtab_pax_btree (a INT);
insert into reindex_crtab_pax_btree select generate_series(1,1000);
insert into reindex_crtab_pax_btree select generate_series(1,1000);
create index idx_reindex_crtab_pax_btree on reindex_crtab_pax_btree(a);
select 1 as oid_same_on_all_segs from gp_dist_random('pg_class')   where relname = 'idx_reindex_crtab_pax_btree' group by oid having count(*) = (select count(*) from gp_segment_configuration where role='p' and content > -1);

-- @Description Ensures that a create index during reindex operations is ok
-- 

DELETE FROM reindex_crtab_pax_btree WHERE a < 128;
1: BEGIN;
2: BEGIN;
1: REINDEX index idx_reindex_crtab_pax_btree;
2: create index idx_reindex_crtab_pax_btree2 on reindex_crtab_pax_btree(a);
1: COMMIT;
2: COMMIT;
3: SELECT 1 AS oid_same_on_all_segs from gp_dist_random('pg_class')   WHERE relname = 'idx_reindex_crtab_pax_btree' GROUP BY oid having count(*) = (SELECT count(*) FROM gp_segment_configuration WHERE role='p' AND content > -1);
3: SELECT 1 AS oid_same_on_all_segs from gp_dist_random('pg_class')   WHERE relname = 'idx_reindex_crtab_pax_btree2' GROUP BY oid having count(*) = (SELECT count(*) FROM gp_segment_configuration WHERE role='p' AND content > -1);
