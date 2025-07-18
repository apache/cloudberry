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
-- @Description Tests that a delete operation in progress will block all other deletes
-- until the transaction is committed.
--
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT) DISTRIBUTED BY (a);
insert into pax_tbl select generate_series(1,100);

-- The actual test begins
1: BEGIN;
2: BEGIN;
2: DELETE FROM pax_tbl WHERE a = 2;
-- The case here should delete a tuple at the same seg with(2).
-- Under jump hash, (2) and (3) are on the same seg(seg0).
1&: DELETE FROM pax_tbl WHERE a = 3;
2: COMMIT;
1<:
1: COMMIT;
3: BEGIN;
3: SELECT * FROM pax_tbl WHERE a < 5 ORDER BY a;
3: COMMIT;
