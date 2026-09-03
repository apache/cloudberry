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
 * contrib/datalake_fdw/datalake_fdw_test--1.0.sql
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION datalake_fdw_test" to load this file. \quit

/*
 * Both functions name a path on the server's file system and run as the
 * operating system user the server does, so they are as privileged as
 * pg_read_server_files and are granted the same way: to nobody, until someone
 * decides otherwise.
 *
 * The reader is pinned to the coordinator.  Without that the planner may put a
 * function scan on the segments, where each of them would read the whole file
 * and the rows would come back as many times as there are segments.  The writer
 * cannot say the same -- EXECUTE ON is only accepted for a set-returning
 * function -- but it does not need to: it is called in a target list with no
 * FROM clause, which is evaluated on the coordinator, and the query it runs is
 * dispatched from there like any other.
 */
CREATE FUNCTION datalake_parquet_write(path text,
									   query text,
									   row_group_size int DEFAULT 0)
RETURNS bigint AS 'MODULE_PATHNAME' LANGUAGE C STRICT VOLATILE;

REVOKE EXECUTE ON FUNCTION datalake_parquet_write(text, text, int) FROM PUBLIC;

CREATE FUNCTION datalake_parquet_read(path text,
									  first_row_group int DEFAULT 0,
									  n_row_groups int DEFAULT 0)
RETURNS SETOF record AS 'MODULE_PATHNAME' LANGUAGE C STRICT EXECUTE ON COORDINATOR;

REVOKE EXECUTE ON FUNCTION datalake_parquet_read(text, int, int) FROM PUBLIC;
