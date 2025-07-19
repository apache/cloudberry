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
-- @Description Tests that a update operation in progress will block all other updates
-- until the transaction is committed.
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT, b INT);
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,10) AS i;

-- The actual test begins
1: BEGIN;
2: BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
1: UPDATE pax_tbl SET b = 42 WHERE b = 1;
2&: UPDATE pax_tbl SET b = -1 WHERE b = 1;
1: COMMIT;
2<:
2: COMMIT;
SELECT * FROM pax_tbl WHERE b < 2;
