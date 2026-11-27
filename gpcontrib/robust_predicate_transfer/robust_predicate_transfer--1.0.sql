---------------------------------------------------------------------------
--
-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--  http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.
--

/* gpcontrib/robust_predicate_transfer/robust_predicate_transfer--1.0.sql */

-- Plan a query and dump its RPT+ transfer plan (regions, join graph,
-- equivalence classes, forward chain, backward broadcast, consumer
-- eligibility).  Analysis only: nothing is injected or executed.
CREATE FUNCTION rpt_transfer_plan(query text)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- Planning exposes estimates without executing the query permission checks.
REVOKE ALL ON FUNCTION rpt_transfer_plan(text) FROM PUBLIC;
