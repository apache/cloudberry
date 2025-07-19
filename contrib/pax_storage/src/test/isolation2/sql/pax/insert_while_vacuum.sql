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
-- @Description Ensures that an insert during a vacuum operation is ok
-- 
DROP TABLE IF EXISTS pax_tbl;
CREATE TABLE pax_tbl (a INT);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);
insert into pax_tbl select generate_series(1,1000);

DELETE FROM pax_tbl WHERE a < 128;
2>: VACUUM pax_tbl;
4: BEGIN;
4: insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);insert into pax_tbl select generate_series(1001,2000);COMMIT;
2<:
3: SELECT COUNT(*) FROM pax_tbl WHERE a = 1500;
4: INSERT INTO pax_tbl VALUES (0);
