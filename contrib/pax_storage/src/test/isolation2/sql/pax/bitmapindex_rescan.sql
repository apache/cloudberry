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
--
-- start_ignore
drop table if exists bir;
drop table if exists yolo cascade;
-- end_ignore
create table bir (a int, b int) distributed by (a);
insert into bir select i, i from generate_series(1, 5) i;

create table yolo (a int, b int) distributed by (a);
create index yolo_idx on yolo using btree (a);

1: begin;
2: begin;
1: insert into yolo select i, i from generate_series(1, 10000) i;
2: insert into yolo select i, i from generate_series(1, 2) i;
1: commit;
2: abort;

analyze yolo;

-- repro needs a plan with bitmap index join with bir on the outer side
set optimizer_enable_hashjoin = off;
set enable_nestloop = on;
set enable_hashjoin = off;
set enable_seqscan = off;

select *
from bir left join yolo
on (bir.a = yolo.a);

