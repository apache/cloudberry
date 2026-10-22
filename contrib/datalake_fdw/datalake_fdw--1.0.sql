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
 * contrib/datalake_fdw/datalake_fdw--1.0.sql
 */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION datalake_fdw" to load this file. \quit

CREATE FUNCTION iceberg_am_handler(internal)
RETURNS table_am_handler AS 'MODULE_PATHNAME' LANGUAGE C;

CREATE ACCESS METHOD iceberg TYPE TABLE HANDLER iceberg_am_handler;

CREATE FUNCTION iceberg_catalog_fdw_validator(text[], oid)
RETURNS void AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER iceberg_catalog_fdw
  VALIDATOR iceberg_catalog_fdw_validator;

CREATE FUNCTION iceberg_volume_fdw_validator(text[], oid)
RETURNS void AS 'MODULE_PATHNAME' LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER iceberg_volume_fdw
  VALIDATOR iceberg_volume_fdw_validator;
