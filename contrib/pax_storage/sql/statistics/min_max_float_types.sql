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
-- start_matchignore
-- m/LOG:  statement:/
-- m/no filter/
-- m/scan key build success/
-- end_matchignore

set default_table_access_method to pax;
set pax_enable_debug to on;
set pax_enable_sparse_filter to on;
set pax_max_tuples_per_group to 5;

-- 
-- Test the float4 min/max types support 
-- 
create table t_float4(i int, v float4) with(minmax_columns='v');
insert into t_float4(i, v) values(1, generate_series(1, 10));
insert into t_float4(i, v) values(1, generate_series(11, 20));

set client_min_messages to log;
select count(*) from t_float4 where v > 0::float4;
select count(*) from t_float4 where v > 5::float4;
select count(*) from t_float4 where v > 10::float4;
select count(*) from t_float4 where v > 15::float4;
select count(*) from t_float4 where v > 20::float4;

select count(*) from t_float4 where v < 0::float4;
select count(*) from t_float4 where v < 6::float4;
select count(*) from t_float4 where v < 11::float4;
select count(*) from t_float4 where v < 16::float4;
select count(*) from t_float4 where v < 21::float4;

select count(*) from t_float4 where v = -1::float4;
select count(*) from t_float4 where v = 5::float4;
select count(*) from t_float4 where v = 10::float4;
select count(*) from t_float4 where v = 11::float4;
select count(*) from t_float4 where v = 20::float4;
select count(*) from t_float4 where v = 21::float4;

select count(*) from t_float4 where v >= 0::float4;
select count(*) from t_float4 where v >= 6::float4;
select count(*) from t_float4 where v >= 11::float4;
select count(*) from t_float4 where v >= 16::float4;
select count(*) from t_float4 where v >= 21::float4;

select count(*) from t_float4 where v <= -1::float4;
select count(*) from t_float4 where v <= 5::float4;
select count(*) from t_float4 where v <= 10::float4;
select count(*) from t_float4 where v <= 15::float4;
select count(*) from t_float4 where v <= 20::float4;

-- oper(float4, float8)
select count(*) from t_float4 where v > 0::float8;
select count(*) from t_float4 where v > 5::float8;
select count(*) from t_float4 where v > 10::float8;
select count(*) from t_float4 where v > 15::float8;
select count(*) from t_float4 where v > 20::float8;

select count(*) from t_float4 where v < 0::float8;
select count(*) from t_float4 where v < 6::float8;
select count(*) from t_float4 where v < 11::float8;
select count(*) from t_float4 where v < 16::float8;
select count(*) from t_float4 where v < 21::float8;

select count(*) from t_float4 where v = -1::float8;
select count(*) from t_float4 where v = 5::float8;
select count(*) from t_float4 where v = 10::float8;
select count(*) from t_float4 where v = 11::float8;
select count(*) from t_float4 where v = 20::float8;
select count(*) from t_float4 where v = 21::float8;

select count(*) from t_float4 where v >= 0::float8;
select count(*) from t_float4 where v >= 6::float8;
select count(*) from t_float4 where v >= 11::float8;
select count(*) from t_float4 where v >= 16::float8;
select count(*) from t_float4 where v >= 21::float8;

select count(*) from t_float4 where v <= -1::float8;
select count(*) from t_float4 where v <= 5::float8;
select count(*) from t_float4 where v <= 10::float8;
select count(*) from t_float4 where v <= 15::float8;
select count(*) from t_float4 where v <= 20::float8;
reset client_min_messages;
drop table t_float4;

-- 
-- Test the float8 min/max types support 
-- 
create table t_float8(i int, v float8) with(minmax_columns='v');
insert into t_float8(i, v) values(1, generate_series(1, 10));
insert into t_float8(i, v) values(1, generate_series(11, 20));

set client_min_messages to log;

select count(*) from t_float8 where v > 0::float8;
select count(*) from t_float8 where v > 5::float8;
select count(*) from t_float8 where v > 10::float8;
select count(*) from t_float8 where v > 15::float8;
select count(*) from t_float8 where v > 20::float8;

select count(*) from t_float8 where v < 0::float8;
select count(*) from t_float8 where v < 6::float8;
select count(*) from t_float8 where v < 11::float8;
select count(*) from t_float8 where v < 16::float8;
select count(*) from t_float8 where v < 21::float8;

select count(*) from t_float8 where v = -1::float8;
select count(*) from t_float8 where v = 5::float8;
select count(*) from t_float8 where v = 10::float8;
select count(*) from t_float8 where v = 11::float8;
select count(*) from t_float8 where v = 20::float8;
select count(*) from t_float8 where v = 21::float8;

select count(*) from t_float8 where v >= 0::float8;
select count(*) from t_float8 where v >= 6::float8;
select count(*) from t_float8 where v >= 11::float8;
select count(*) from t_float8 where v >= 16::float8;
select count(*) from t_float8 where v >= 21::float8;

select count(*) from t_float8 where v <= -1::float8;
select count(*) from t_float8 where v <= 5::float8;
select count(*) from t_float8 where v <= 10::float8;
select count(*) from t_float8 where v <= 15::float8;
select count(*) from t_float8 where v <= 20::float8;

-- oper(float8, float4)
select count(*) from t_float8 where v > 0::float4;
select count(*) from t_float8 where v > 5::float4;
select count(*) from t_float8 where v > 10::float4;
select count(*) from t_float8 where v > 15::float4;
select count(*) from t_float8 where v > 20::float4;

select count(*) from t_float8 where v < 0::float4;
select count(*) from t_float8 where v < 6::float4;
select count(*) from t_float8 where v < 11::float4;
select count(*) from t_float8 where v < 16::float4;
select count(*) from t_float8 where v < 21::float4;

select count(*) from t_float8 where v = -1::float4;
select count(*) from t_float8 where v = 5::float4;
select count(*) from t_float8 where v = 10::float4;
select count(*) from t_float8 where v = 11::float4;
select count(*) from t_float8 where v = 20::float4;
select count(*) from t_float8 where v = 21::float4;

select count(*) from t_float8 where v >= 0::float4;
select count(*) from t_float8 where v >= 6::float4;
select count(*) from t_float8 where v >= 11::float4;
select count(*) from t_float8 where v >= 16::float4;
select count(*) from t_float8 where v >= 21::float4;

select count(*) from t_float8 where v <= -1::float4;
select count(*) from t_float8 where v <= 5::float4;
select count(*) from t_float8 where v <= 10::float4;
select count(*) from t_float8 where v <= 15::float4;
select count(*) from t_float8 where v <= 20::float4;
reset client_min_messages;
drop table t_float8;

reset pax_enable_debug;
reset pax_enable_sparse_filter;
reset pax_max_tuples_per_group;
