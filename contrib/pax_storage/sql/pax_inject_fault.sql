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

create table t_insert(a int);
select gp_inject_fault_infinite('fts_probe','skip',dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
select gp_request_fts_probe_scan();
select gp_inject_fault('orc_writer_write_tuple','panic',dbid) FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
-- failed because of fault injection
insert into t_insert select generate_series(1,10);

-- start_ignore
-- clear the fault inject, so the next insert will success.
-- put the reset operation in ignore range
select gp_inject_fault('orc_writer_write_tuple','reset',dbid) FROM gp_segment_configuration WHERE role = 'p' AND content > -1;
select gp_inject_fault('fts_probe','reset',dbid) FROM gp_segment_configuration WHERE role = 'p' AND content = -1;
-- end_ignore

-- success 
insert into t_insert select generate_series(1,10);
drop table t_insert;
