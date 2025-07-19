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
-- start_ignore
create schema pax_test;
CREATE EXTENSION gp_inject_fault;
-- end_ignore

CREATE OR REPLACE FUNCTION pax_get_catalog_rows(rel regclass,
  segment_id out int,
  ptblockname out int,
  pttupcount out int,
  ptblocksize out int,
  ptstatistics out pg_ext_aux.paxauxstats,
  ptvisimapname out name,
  pthastoast out boolean,
  ptisclustered out boolean
)
returns setof record
EXECUTE ON  ALL SEGMENTS
AS '$libdir/pax', 'pax_get_catalog_rows' LANGUAGE C ;

CREATE OR REPLACE FUNCTION get_pax_aux_table(rel regclass)
RETURNS TABLE(
  ptblockname integer,
  pttupcount integer,
  ptstatistics pg_ext_aux.paxauxstats,
  ptexistvisimap bool,
  ptexistexttoast bool,
  ptisclustered bool
) AS $$
  SELECT ptblockname, pttupcount, ptstatistics, ptvisimapname IS NOT NULL, pthastoast, ptisclustered
  FROM pax_get_catalog_rows(rel);
$$ LANGUAGE sql;


CREATE OR REPLACE FUNCTION get_pax_table_stats(rel regclass)
  RETURNS TABLE("ptstatistics" pg_ext_aux.paxauxstats) AS $$
  SELECT ptstatistics FROM pax_get_catalog_rows(rel);
$$ LANGUAGE sql;
