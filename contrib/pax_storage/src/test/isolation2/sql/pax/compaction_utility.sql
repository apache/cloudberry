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
-- @Description Tests the basic behavior of (lazy) vacuum when called from utility mode
-- 
DROP TABLE IF EXISTS foo;
CREATE TABLE foo (a INT, b INT, c CHAR(128));
CREATE INDEX foo_index ON foo(b);
INSERT INTO foo SELECT i as a, 1 as b, 'hello world' as c FROM generate_series(1, 100) AS i;

DELETE FROM foo WHERE a < 20;
SELECT COUNT(*) FROM foo;
0U: SELECT segment_id, case when pttupcount = 0 then 'zero' when pttupcount <= 5 then 'few' else 'many' end FROM get_pax_aux_table_all('foo');
0U: VACUUM foo;
SELECT COUNT(*) FROM foo;
0U: SELECT segment_id, case when pttupcount = 0 then 'zero' when pttupcount <= 5 then 'few' else 'many' end FROM get_pax_aux_table_all('foo');
