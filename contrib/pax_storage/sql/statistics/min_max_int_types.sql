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
-- Test the int2 min/max types support 
-- 
create table t_int2(i int, v int2) with(minmax_columns='v');
insert into t_int2(i, v) values(1, generate_series(1, 10));
insert into t_int2(i, v) values(1, generate_series(11, 20));

set client_min_messages to log;
select count(*) from t_int2 where v > 0::int2;
select count(*) from t_int2 where v > 5::int2;
select count(*) from t_int2 where v > 10::int2;
select count(*) from t_int2 where v > 15::int2;
select count(*) from t_int2 where v > 20::int2;

select count(*) from t_int2 where v < 0::int2;
select count(*) from t_int2 where v < 6::int2;
select count(*) from t_int2 where v < 11::int2;
select count(*) from t_int2 where v < 16::int2;
select count(*) from t_int2 where v < 21::int2;

select count(*) from t_int2 where v = -1::int2;
select count(*) from t_int2 where v = 5::int2;
select count(*) from t_int2 where v = 10::int2;
select count(*) from t_int2 where v = 11::int2;
select count(*) from t_int2 where v = 20::int2;
select count(*) from t_int2 where v = 21::int2;

select count(*) from t_int2 where v >= 0::int2;
select count(*) from t_int2 where v >= 6::int2;
select count(*) from t_int2 where v >= 11::int2;
select count(*) from t_int2 where v >= 16::int2;
select count(*) from t_int2 where v >= 21::int2;

select count(*) from t_int2 where v <= -1::int2;
select count(*) from t_int2 where v <= 5::int2;
select count(*) from t_int2 where v <= 10::int2;
select count(*) from t_int2 where v <= 15::int2;
select count(*) from t_int2 where v <= 20::int2;

-- oper(int2, int4)
select count(*) from t_int2 where v > 0::int4;
select count(*) from t_int2 where v > 5::int4;
select count(*) from t_int2 where v > 10::int4;
select count(*) from t_int2 where v > 15::int4;
select count(*) from t_int2 where v > 20::int4;

select count(*) from t_int2 where v < 0::int4;
select count(*) from t_int2 where v < 6::int4;
select count(*) from t_int2 where v < 11::int4;
select count(*) from t_int2 where v < 16::int4;
select count(*) from t_int2 where v < 21::int4;

select count(*) from t_int2 where v = -1::int4;
select count(*) from t_int2 where v = 5::int4;
select count(*) from t_int2 where v = 10::int4;
select count(*) from t_int2 where v = 11::int4;
select count(*) from t_int2 where v = 20::int4;
select count(*) from t_int2 where v = 21::int4;

select count(*) from t_int2 where v >= 0::int4;
select count(*) from t_int2 where v >= 6::int4;
select count(*) from t_int2 where v >= 11::int4;
select count(*) from t_int2 where v >= 16::int4;
select count(*) from t_int2 where v >= 21::int4;

select count(*) from t_int2 where v <= -1::int4;
select count(*) from t_int2 where v <= 5::int4;
select count(*) from t_int2 where v <= 10::int4;
select count(*) from t_int2 where v <= 15::int4;
select count(*) from t_int2 where v <= 20::int4;

-- oper(int2, int8)
select count(*) from t_int2 where v > 0::int8;
select count(*) from t_int2 where v > 5::int8;
select count(*) from t_int2 where v > 10::int8;
select count(*) from t_int2 where v > 15::int8;
select count(*) from t_int2 where v > 20::int8;

select count(*) from t_int2 where v < 0::int8;
select count(*) from t_int2 where v < 6::int8;
select count(*) from t_int2 where v < 11::int8;
select count(*) from t_int2 where v < 16::int8;
select count(*) from t_int2 where v < 21::int8;

select count(*) from t_int2 where v = -1::int8;
select count(*) from t_int2 where v = 5::int8;
select count(*) from t_int2 where v = 10::int8;
select count(*) from t_int2 where v = 11::int8;
select count(*) from t_int2 where v = 20::int8;
select count(*) from t_int2 where v = 21::int8;

select count(*) from t_int2 where v >= 0::int8;
select count(*) from t_int2 where v >= 6::int8;
select count(*) from t_int2 where v >= 11::int8;
select count(*) from t_int2 where v >= 16::int8;
select count(*) from t_int2 where v >= 21::int8;

select count(*) from t_int2 where v <= -1::int8;
select count(*) from t_int2 where v <= 5::int8;
select count(*) from t_int2 where v <= 10::int8;
select count(*) from t_int2 where v <= 15::int8;
select count(*) from t_int2 where v <= 20::int8;

reset client_min_messages;
drop table t_int2;

-- 
-- Test the int4 min/max types support 
-- 
create table t_int4(i int, v int4) with(minmax_columns='v');
insert into t_int4(i, v) values(1, generate_series(1, 10));
insert into t_int4(i, v) values(1, generate_series(10, 20));

set client_min_messages to log;
select count(*) from t_int4 where v > 0::int4;
select count(*) from t_int4 where v > 5::int4;
select count(*) from t_int4 where v > 10::int4;
select count(*) from t_int4 where v > 15::int4;
select count(*) from t_int4 where v > 20::int4;

select count(*) from t_int4 where v < 0::int4;
select count(*) from t_int4 where v < 6::int4;
select count(*) from t_int4 where v < 11::int4;
select count(*) from t_int4 where v < 16::int4;
select count(*) from t_int4 where v < 21::int4;

select count(*) from t_int4 where v = -1::int4;
select count(*) from t_int4 where v = 5::int4;
select count(*) from t_int4 where v = 10::int4;
select count(*) from t_int4 where v = 11::int4;
select count(*) from t_int4 where v = 20::int4;
select count(*) from t_int4 where v = 21::int4;

select count(*) from t_int4 where v >= 0::int4;
select count(*) from t_int4 where v >= 6::int4;
select count(*) from t_int4 where v >= 11::int4;
select count(*) from t_int4 where v >= 16::int4;
select count(*) from t_int4 where v >= 21::int4;

select count(*) from t_int4 where v <= -1::int4;
select count(*) from t_int4 where v <= 5::int4;
select count(*) from t_int4 where v <= 10::int4;
select count(*) from t_int4 where v <= 15::int4;
select count(*) from t_int4 where v <= 20::int4;

-- oper(int4, int2)
select count(*) from t_int4 where v > 0::int2;
select count(*) from t_int4 where v > 5::int2;
select count(*) from t_int4 where v > 10::int2;
select count(*) from t_int4 where v > 15::int2;
select count(*) from t_int4 where v > 20::int2;

select count(*) from t_int4 where v < 0::int2;
select count(*) from t_int4 where v < 6::int2;
select count(*) from t_int4 where v < 11::int2;
select count(*) from t_int4 where v < 16::int2;
select count(*) from t_int4 where v < 21::int2;

select count(*) from t_int4 where v = -1::int2;
select count(*) from t_int4 where v = 5::int2;
select count(*) from t_int4 where v = 10::int2;
select count(*) from t_int4 where v = 11::int2;
select count(*) from t_int4 where v = 20::int2;
select count(*) from t_int4 where v = 21::int2;

select count(*) from t_int4 where v >= 0::int2;
select count(*) from t_int4 where v >= 6::int2;
select count(*) from t_int4 where v >= 11::int2;
select count(*) from t_int4 where v >= 16::int2;
select count(*) from t_int4 where v >= 21::int2;

select count(*) from t_int4 where v <= -1::int2;
select count(*) from t_int4 where v <= 5::int2;
select count(*) from t_int4 where v <= 10::int2;
select count(*) from t_int4 where v <= 15::int2;
select count(*) from t_int4 where v <= 20::int2;

-- oper(int4, int8)
select count(*) from t_int4 where v > 0::int8;
select count(*) from t_int4 where v > 5::int8;
select count(*) from t_int4 where v > 10::int8;
select count(*) from t_int4 where v > 15::int8;
select count(*) from t_int4 where v > 20::int8;

select count(*) from t_int4 where v < 0::int8;
select count(*) from t_int4 where v < 6::int8;
select count(*) from t_int4 where v < 11::int8;
select count(*) from t_int4 where v < 16::int8;
select count(*) from t_int4 where v < 21::int8;

select count(*) from t_int4 where v = -1::int8;
select count(*) from t_int4 where v = 5::int8;
select count(*) from t_int4 where v = 10::int8;
select count(*) from t_int4 where v = 11::int8;
select count(*) from t_int4 where v = 20::int8;
select count(*) from t_int4 where v = 21::int8;

select count(*) from t_int4 where v >= 0::int8;
select count(*) from t_int4 where v >= 6::int8;
select count(*) from t_int4 where v >= 11::int8;
select count(*) from t_int4 where v >= 16::int8;
select count(*) from t_int4 where v >= 21::int8;

select count(*) from t_int4 where v <= -1::int8;
select count(*) from t_int4 where v <= 5::int8;
select count(*) from t_int4 where v <= 10::int8;
select count(*) from t_int4 where v <= 15::int8;
select count(*) from t_int4 where v <= 20::int8;
reset client_min_messages;
drop table t_int4;

-- 
-- Test the int8 min/max types support 
-- 
create table t_int8(i int,v int8) with(minmax_columns='v');
insert into t_int8(i, v) values(1, generate_series(1, 10));
insert into t_int8(i, v) values(1, generate_series(10, 20));

set client_min_messages to log;
select count(*) from t_int8 where v > 0::int8;
select count(*) from t_int8 where v > 5::int8;
select count(*) from t_int8 where v > 10::int8;
select count(*) from t_int8 where v > 15::int8;
select count(*) from t_int8 where v > 20::int8;

select count(*) from t_int8 where v < 0::int8;
select count(*) from t_int8 where v < 6::int8;
select count(*) from t_int8 where v < 11::int8;
select count(*) from t_int8 where v < 16::int8;
select count(*) from t_int8 where v < 21::int8;

select count(*) from t_int8 where v = -1::int8;
select count(*) from t_int8 where v = 5::int8;
select count(*) from t_int8 where v = 10::int8;
select count(*) from t_int8 where v = 11::int8;
select count(*) from t_int8 where v = 20::int8;
select count(*) from t_int8 where v = 21::int8;

select count(*) from t_int8 where v >= 0::int8;
select count(*) from t_int8 where v >= 6::int8;
select count(*) from t_int8 where v >= 11::int8;
select count(*) from t_int8 where v >= 16::int8;
select count(*) from t_int8 where v >= 21::int8;

select count(*) from t_int8 where v <= -1::int8;
select count(*) from t_int8 where v <= 5::int8;
select count(*) from t_int8 where v <= 10::int8;
select count(*) from t_int8 where v <= 15::int8;
select count(*) from t_int8 where v <= 20::int8;

-- oper(int8, int2)
select count(*) from t_int8 where v > 0::int2;
select count(*) from t_int8 where v > 5::int2;
select count(*) from t_int8 where v > 10::int2;
select count(*) from t_int8 where v > 15::int2;
select count(*) from t_int8 where v > 20::int2;

select count(*) from t_int8 where v < 0::int2;
select count(*) from t_int8 where v < 6::int2;
select count(*) from t_int8 where v < 11::int2;
select count(*) from t_int8 where v < 16::int2;
select count(*) from t_int8 where v < 21::int2;

select count(*) from t_int8 where v = -1::int2;
select count(*) from t_int8 where v = 5::int2;
select count(*) from t_int8 where v = 10::int2;
select count(*) from t_int8 where v = 11::int2;
select count(*) from t_int8 where v = 20::int2;
select count(*) from t_int8 where v = 21::int2;

select count(*) from t_int8 where v >= 0::int2;
select count(*) from t_int8 where v >= 6::int2;
select count(*) from t_int8 where v >= 11::int2;
select count(*) from t_int8 where v >= 16::int2;
select count(*) from t_int8 where v >= 21::int2;

select count(*) from t_int8 where v <= -1::int2;
select count(*) from t_int8 where v <= 5::int2;
select count(*) from t_int8 where v <= 10::int2;
select count(*) from t_int8 where v <= 15::int2;
select count(*) from t_int8 where v <= 20::int2;

-- oper(int8, int4)
select count(*) from t_int8 where v > 0::int4;
select count(*) from t_int8 where v > 5::int4;
select count(*) from t_int8 where v > 10::int4;
select count(*) from t_int8 where v > 15::int4;
select count(*) from t_int8 where v > 20::int4;

select count(*) from t_int8 where v < 0::int4;
select count(*) from t_int8 where v < 6::int4;
select count(*) from t_int8 where v < 11::int4;
select count(*) from t_int8 where v < 16::int4;
select count(*) from t_int8 where v < 21::int4;

select count(*) from t_int8 where v = -1::int4;
select count(*) from t_int8 where v = 5::int4;
select count(*) from t_int8 where v = 10::int4;
select count(*) from t_int8 where v = 11::int4;
select count(*) from t_int8 where v = 20::int4;
select count(*) from t_int8 where v = 21::int4;

select count(*) from t_int8 where v >= 0::int4;
select count(*) from t_int8 where v >= 6::int4;
select count(*) from t_int8 where v >= 11::int4;
select count(*) from t_int8 where v >= 16::int4;
select count(*) from t_int8 where v >= 21::int4;

select count(*) from t_int8 where v <= -1::int4;
select count(*) from t_int8 where v <= 5::int4;
select count(*) from t_int8 where v <= 10::int4;
select count(*) from t_int8 where v <= 15::int4;
select count(*) from t_int8 where v <= 20::int4;
reset client_min_messages;
drop table t_int8;

--
-- Test the numeric min/max types support 
--
create table t_numeric(i int, v numeric) with(minmax_columns='v');
insert into t_numeric(i, v) values(1, generate_series(1, 10));
insert into t_numeric(i, v) values(1, generate_series(10, 20));

set client_min_messages to log;
select count(*) from t_numeric where v > 0::numeric;
select count(*) from t_numeric where v > 5::numeric;
select count(*) from t_numeric where v > 10::numeric;
select count(*) from t_numeric where v > 15::numeric;
select count(*) from t_numeric where v > 20::numeric;

select count(*) from t_numeric where v < 0::numeric;
select count(*) from t_numeric where v < 6::numeric;
select count(*) from t_numeric where v < 11::numeric;
select count(*) from t_numeric where v < 16::numeric;
select count(*) from t_numeric where v < 21::numeric;

select count(*) from t_numeric where v = -1::numeric;
select count(*) from t_numeric where v = 5::numeric;
select count(*) from t_numeric where v = 10::numeric;
select count(*) from t_numeric where v = 11::numeric;
select count(*) from t_numeric where v = 20::numeric;
select count(*) from t_numeric where v = 21::numeric;

select count(*) from t_numeric where v >= 0::numeric;
select count(*) from t_numeric where v >= 6::numeric;
select count(*) from t_numeric where v >= 11::numeric;
select count(*) from t_numeric where v >= 16::numeric;
select count(*) from t_numeric where v >= 21::numeric;

select count(*) from t_numeric where v <= -1::numeric;
select count(*) from t_numeric where v <= 5::numeric;
select count(*) from t_numeric where v <= 10::numeric;
select count(*) from t_numeric where v <= 15::numeric;
select count(*) from t_numeric where v <= 20::numeric;
reset client_min_messages;
drop table t_numeric;

reset pax_enable_debug;
reset pax_enable_sparse_filter;
reset pax_max_tuples_per_group;
