/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * sql/lance_fdw--1.0.sql
 */

-- complain if the script is sourced by psql instead of CREATE EXTENSION
\echo Use "CREATE EXTENSION lance_fdw" to load this file. \quit

CREATE FUNCTION lance_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME', 'lance_fdw_handler'
LANGUAGE C STRICT;

CREATE FUNCTION lance_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME', 'lance_fdw_validator'
LANGUAGE C STRICT;

/*
 * Arrow batches are allocated by lance outside palloc.  This wrapper charges
 * them to Cloudberry's memory accounting so that the resource group and
 * gp_vmem_protect_limit can see them; these two report what it is holding and
 * how the per-backend lance caches are doing.  Both are backend-local, so on a
 * segment they describe that segment's process.
 */
CREATE FUNCTION lance_fdw_memory(
  OUT current_bytes bigint,
  OUT peak_bytes bigint)
RETURNS record
AS 'MODULE_PATHNAME', 'lance_fdw_memory'
LANGUAGE C STRICT VOLATILE PARALLEL RESTRICTED;

COMMENT ON FUNCTION lance_fdw_memory() IS
  'Arrow bytes on the vmem ledger for this backend: now, and at its highest';

CREATE FUNCTION lance_fdw_cache_stats(
  OUT index_cache_hits bigint,
  OUT index_cache_misses bigint,
  OUT index_cache_entries bigint,
  OUT index_cache_size_bytes bigint,
  OUT metadata_cache_hits bigint,
  OUT metadata_cache_misses bigint,
  OUT metadata_cache_entries bigint,
  OUT metadata_cache_size_bytes bigint)
RETURNS record
AS 'MODULE_PATHNAME', 'lance_fdw_cache_stats'
LANGUAGE C STRICT VOLATILE PARALLEL RESTRICTED;

COMMENT ON FUNCTION lance_fdw_cache_stats() IS
  'hit and miss counters for this backend''s lance index and metadata caches';

/*
 * mpp_execute defaults to 'all segments' so that the planner puts the
 * ForeignScan on every segment and gives it a Strewn locus.  A
 * server or a table may still override it with 'coordinator' or 'any', in
 * which case the executing process reads every fragment by itself.
 */
CREATE FOREIGN DATA WRAPPER lance_fdw
  HANDLER lance_fdw_handler
  VALIDATOR lance_fdw_validator
  OPTIONS (mpp_execute 'all segments');

COMMENT ON FOREIGN DATA WRAPPER lance_fdw IS 'reads Lance datasets in parallel on all segments';
