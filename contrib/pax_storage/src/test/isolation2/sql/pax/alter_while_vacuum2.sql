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
-- @Description Ensures that an alter table while a vacuum operation is ok
-- 
CREATE TABLE alter_while_vacuum2_pax_column (a INT, b INT);
INSERT INTO alter_while_vacuum2_pax_column SELECT i as a, i as b FROM generate_series(1, 100000) AS i;
INSERT INTO alter_while_vacuum2_pax_column SELECT i as a, i as b FROM generate_series(1, 100000) AS i;
INSERT INTO alter_while_vacuum2_pax_column SELECT i as a, i as b FROM generate_series(1, 100000) AS i;
INSERT INTO alter_while_vacuum2_pax_column SELECT i as a, i as b FROM generate_series(1, 100000) AS i;
INSERT INTO alter_while_vacuum2_pax_column SELECT i as a, i as b FROM generate_series(1, 100000) AS i;
INSERT INTO alter_while_vacuum2_pax_column SELECT i as a, i as b FROM generate_series(1, 100000) AS i;

DELETE FROM alter_while_vacuum2_pax_column WHERE a < 12000;
1: SELECT COUNT(*) FROM alter_while_vacuum2_pax_column;
2>: VACUUM alter_while_vacuum2_pax_column;
1: Alter table alter_while_vacuum2_pax_column set distributed randomly;
2<:
1: SELECT COUNT(*) FROM alter_while_vacuum2_pax_column WHERE a < 12010;
