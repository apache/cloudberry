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
-- @Description Tests the visibility when a cursor has been created before the delete.
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT);
insert into pax_tbl select generate_series(1, 10);

1: BEGIN;
1: DECLARE cur CURSOR FOR SELECT a FROM pax_tbl ORDER BY a;
1: FETCH NEXT IN cur;
1: FETCH NEXT IN cur;
2: BEGIN;
2: DELETE FROM pax_tbl WHERE a < 5;
2: COMMIT;
2&: VACUUM FULL pax_tbl;
1: FETCH NEXT IN cur;
1: FETCH NEXT IN cur;
1: FETCH NEXT IN cur;
1: CLOSE cur;
1: COMMIT;
2<:
3: BEGIN;
3: DECLARE cur CURSOR FOR SELECT a FROM pax_tbl ORDER BY a;
3: FETCH NEXT IN cur;
3: CLOSE cur;
3: COMMIT;
