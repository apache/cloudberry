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
-- @Description Tests the behavior of full vacuum w.r.t. the pg_class statistics
-- 
DROP TABLE IF EXISTS foo;

CREATE TABLE foo (a INT, b INT, c CHAR(128)) DISTRIBUTED BY (a);
CREATE INDEX foo_index ON foo(b);
INSERT INTO foo SELECT i as a, i as b, 'hello world' as c FROM generate_series(1, 50) AS i;
INSERT INTO foo SELECT i as a, i as b, 'hello world' as c FROM generate_series(51, 100) AS i;
ANALYZE foo;

-- ensure that the scan go through the index
SET enable_seqscan=false;
SELECT relname, reltuples FROM pg_class WHERE relname = 'foo';
SELECT relname, reltuples FROM pg_class WHERE relname = 'foo_index';
DELETE FROM foo WHERE a < 16;
VACUUM FULL foo;
SELECT relname, reltuples FROM pg_class WHERE relname = 'foo';
SELECT relname, reltuples FROM pg_class WHERE relname = 'foo_index';
-- in case there's autovacuum worker running in the backend, the aoseg will not be dropped which has state = 2
SELECT ptblockname, pttupcount FROM get_pax_aux_table_all('foo') where pttupcount > 0;
