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
-- @Description Ensures that an update during a vacuum operation is ok
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT, b INT);
INSERT INTO pax_tbl SELECT i as a, i as b FROM generate_series(1,10) AS i;

DELETE FROM pax_tbl WHERE a < 2;
4: BEGIN;
4: SELECT COUNT(*) FROM pax_tbl;
4: INSERT INTO pax_tbl VALUES (1, 1);
4>: UPDATE pax_tbl SET b=1 WHERE a > 5;UPDATE pax_tbl SET b=1 WHERE a > 6;COMMIT;
2: VACUUM pax_tbl;
4<:
3: SELECT COUNT(*) FROM pax_tbl WHERE b = 1;
3: INSERT INTO pax_tbl VALUES (0);
