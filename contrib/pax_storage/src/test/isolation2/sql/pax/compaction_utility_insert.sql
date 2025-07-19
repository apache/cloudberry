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
-- @Description Tests the compaction of data inserted in utility mode
-- 
DROP TABLE IF EXISTS foo;
CREATE TABLE foo (a INT, b INT, c CHAR(128)) distributed randomly;
CREATE INDEX foo_index ON foo(b);

0U: INSERT INTO foo VALUES (2, 2, 'c');
0U: INSERT INTO foo VALUES (3, 3, 'c');
SELECT segment_id, pttupcount FROM get_pax_aux_table_all('foo');
DELETE FROM foo WHERE a = 2;
UPDATE foo SET b = -1 WHERE a = 3;
VACUUM foo;
SELECT segment_id, pttupcount FROM get_pax_aux_table_all('foo') where pttupcount > 0;
