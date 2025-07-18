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
-- Tests on the table and index size variants.
--
CREATE TABLE paxsizetest (a int) with (compresstype=none);

-- First test with an empty table and no indexes. Should be all zeros.
select pg_relation_size('paxsizetest');
select pg_table_size('paxsizetest');
select pg_indexes_size('paxsizetest');
select pg_total_relation_size('paxsizetest');

-- Now test with a non-empty table (still no indexes, though).
insert into paxsizetest select generate_series(1, 100000);
vacuum paxsizetest;

-- Check that the values are in an expected size. 
-- if pax proto buffer changed, the size will be changed.
select pg_relation_size('paxsizetest');
select pg_table_size('paxsizetest');
select pg_indexes_size('paxsizetest');
select pg_total_relation_size('paxsizetest');

-- Check that the values are in an expected ranges.
select pg_relation_size('paxsizetest') between 400000 and 450000; -- 400351
select pg_table_size('paxsizetest') between 400000 and 450000; -- 400351
select pg_table_size('paxsizetest') >= pg_relation_size('paxsizetest');
select pg_indexes_size('paxsizetest'); -- 0
select pg_total_relation_size('paxsizetest') between 400000 and 450000; -- 400351
select pg_total_relation_size('paxsizetest') = pg_table_size('paxsizetest'); -- true

-- Now also indexes
create index on paxsizetest(a);

select pg_relation_size('paxsizetest') between 400000 and 450000; -- 400351
select pg_table_size('paxsizetest') between 400000 and 450000; -- 400351
select pg_indexes_size('paxsizetest') between 2000000 and 3000000; -- 2490368
select pg_total_relation_size('paxsizetest') between 2000000 and 3000000; -- 2890719
select pg_total_relation_size('paxsizetest') = pg_table_size('paxsizetest') + pg_indexes_size('paxsizetest');