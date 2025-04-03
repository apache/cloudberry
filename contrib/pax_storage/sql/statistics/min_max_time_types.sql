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
-- Test the date min/max types support 
-- 
create table t_date(i int, v date) with(minmax_columns='v');

insert into t_date(i, v) values (1, '2000-01-01'),(1, '2000-01-02'),(1, '2000-01-03'),(1, '2000-01-04'),(1, '2000-01-05'),
    (1, '2000-01-06'),(1, '2000-01-07'),(1, '2000-01-08'),(1, '2000-01-09'),(1, '2000-01-10');
insert into t_date(i, v) values (1, '2000-01-11'),(1, '2000-01-12'),(1, '2000-01-13'),(1, '2000-01-14'),(1, '2000-01-15'),
    (1, '2000-01-16'),(1, '2000-01-17'),(1, '2000-01-18'),(1, '2000-01-19'),(1, '2000-01-20');

set client_min_messages to log;
select count(*) from t_date where v > '1999-12-01'::date;
select count(*) from t_date where v > '2000-01-05'::date;
select count(*) from t_date where v > '2000-01-10'::date;
select count(*) from t_date where v > '2000-01-15'::date;
select count(*) from t_date where v > '2000-01-20'::date;

select count(*) from t_date where v < '1999-12-01'::date;
select count(*) from t_date where v < '2000-01-06'::date;
select count(*) from t_date where v < '2000-01-11'::date;
select count(*) from t_date where v < '2000-01-16'::date;
select count(*) from t_date where v < '2000-01-21'::date;

select count(*) from t_date where v = '1999-12-01'::date;
select count(*) from t_date where v = '2000-01-05'::date;
select count(*) from t_date where v = '2000-01-10'::date;
select count(*) from t_date where v = '2000-01-11'::date;
select count(*) from t_date where v = '2000-01-20'::date;
select count(*) from t_date where v = '2000-01-21'::date;

select count(*) from t_date where v >= '1999-12-01'::date;
select count(*) from t_date where v >= '2000-01-06'::date;
select count(*) from t_date where v >= '2000-01-11'::date;
select count(*) from t_date where v >= '2000-01-16'::date;
select count(*) from t_date where v >= '2000-01-21'::date;

select count(*) from t_date where v <= '1999-12-01'::date;
select count(*) from t_date where v <= '2000-01-05'::date;
select count(*) from t_date where v <= '2000-01-10'::date;
select count(*) from t_date where v <= '2000-01-15'::date;
select count(*) from t_date where v <= '2000-01-20'::date;

-- oper(date, timestamp)
select count(*) from t_date where v > '1999-12-01 00:00:00'::timestamp;
select count(*) from t_date where v > '2000-01-05 00:00:00'::timestamp;
select count(*) from t_date where v > '2000-01-10 00:00:00'::timestamp;
select count(*) from t_date where v > '2000-01-15 00:00:00'::timestamp;
select count(*) from t_date where v > '2000-01-20 00:00:00'::timestamp;

select count(*) from t_date where v < '1999-12-01 00:00:00'::timestamp;
select count(*) from t_date where v < '2000-01-06 00:00:00'::timestamp;
select count(*) from t_date where v < '2000-01-11 00:00:00'::timestamp;
select count(*) from t_date where v < '2000-01-16 00:00:00'::timestamp;
select count(*) from t_date where v < '2000-01-21 00:00:00'::timestamp;

select count(*) from t_date where v = '1999-12-01 00:00:00'::timestamp;
select count(*) from t_date where v = '2000-01-05 00:00:00'::timestamp;
select count(*) from t_date where v = '2000-01-10 00:00:00'::timestamp;
select count(*) from t_date where v = '2000-01-11 00:00:00'::timestamp;
select count(*) from t_date where v = '2000-01-20 00:00:00'::timestamp;
select count(*) from t_date where v = '2000-01-21 00:00:00'::timestamp;

select count(*) from t_date where v >= '1999-12-01 00:00:00'::timestamp;
select count(*) from t_date where v >= '2000-01-06 00:00:00'::timestamp;
select count(*) from t_date where v >= '2000-01-11 00:00:00'::timestamp;
select count(*) from t_date where v >= '2000-01-16 00:00:00'::timestamp;
select count(*) from t_date where v >= '2000-01-21 00:00:00'::timestamp;

select count(*) from t_date where v <= '1999-12-01 00:00:00'::timestamp;
select count(*) from t_date where v <= '2000-01-05 00:00:00'::timestamp;
select count(*) from t_date where v <= '2000-01-10 00:00:00'::timestamp;
select count(*) from t_date where v <= '2000-01-15 00:00:00'::timestamp;
select count(*) from t_date where v <= '2000-01-20 00:00:00'::timestamp;

-- oper(date, timestamptz)
select count(*) from t_date where v > '1999-12-01 00:00:00+00'::timestamptz;
select count(*) from t_date where v > '2000-01-05 00:00:00+00'::timestamptz;
select count(*) from t_date where v > '2000-01-10 00:00:00+00'::timestamptz;
select count(*) from t_date where v > '2000-01-15 00:00:00+00'::timestamptz;
select count(*) from t_date where v > '2000-01-20 00:00:00+00'::timestamptz;

select count(*) from t_date where v < '1999-12-01 00:00:00+00'::timestamptz;
select count(*) from t_date where v < '2000-01-06 00:00:00+00'::timestamptz;
select count(*) from t_date where v < '2000-01-11 00:00:00+00'::timestamptz;
select count(*) from t_date where v < '2000-01-16 00:00:00+00'::timestamptz;
select count(*) from t_date where v < '2000-01-21 00:00:00+00'::timestamptz;

select count(*) from t_date where v = '1999-12-01 00:00:00+00'::timestamptz;
select count(*) from t_date where v = '2000-01-05 00:00:00+00'::timestamptz;
select count(*) from t_date where v = '2000-01-10 00:00:00+00'::timestamptz;
select count(*) from t_date where v = '2000-01-11 00:00:00+00'::timestamptz;
select count(*) from t_date where v = '2000-01-20 00:00:00+00'::timestamptz;
select count(*) from t_date where v = '2000-01-21 00:00:00+00'::timestamptz;

select count(*) from t_date where v >= '1999-12-01 00:00:00+00'::timestamptz;
select count(*) from t_date where v >= '2000-01-06 00:00:00+00'::timestamptz;
select count(*) from t_date where v >= '2000-01-11 00:00:00+00'::timestamptz;
select count(*) from t_date where v >= '2000-01-16 00:00:00+00'::timestamptz;
select count(*) from t_date where v >= '2000-01-21 00:00:00+00'::timestamptz;

select count(*) from t_date where v <= '1999-12-01 00:00:00+00'::timestamptz;
select count(*) from t_date where v <= '2000-01-05 00:00:00+00'::timestamptz;
select count(*) from t_date where v <= '2000-01-10 00:00:00+00'::timestamptz;
select count(*) from t_date where v <= '2000-01-15 00:00:00+00'::timestamptz;
select count(*) from t_date where v <= '2000-01-20 00:00:00+00'::timestamptz;
reset client_min_messages;
drop table t_date;

-- 
-- Test the timestamp min/max types support 
-- 
create table t_timestamp(i int, v timestamp) with(minmax_columns='v');

insert into t_timestamp(i, v) values (1, '2000-01-01 00:00:01'),(1, '2000-01-01 00:00:02'),(1, '2000-01-01 00:00:03'),
(1, '2000-01-01 00:00:04'),(1, '2000-01-01 00:00:05'),(1, '2000-01-01 00:00:06'),
(1, '2000-01-01 00:00:07'),(1, '2000-01-01 00:00:08'),(1, '2000-01-01 00:00:09'),
(1, '2000-01-01 00:00:10');

insert into t_timestamp(i, v) values (1, '2000-01-01 00:00:11'),(1, '2000-01-01 00:00:12'),(1, '2000-01-01 00:00:13'),
(1, '2000-01-01 00:00:14'),(1, '2000-01-01 00:00:15'),(1, '2000-01-01 00:00:16'),
(1, '2000-01-01 00:00:17'),(1, '2000-01-01 00:00:18'),(1, '2000-01-01 00:00:19'),
(1, '2000-01-01 00:00:20');

set client_min_messages to log;
select count(*) from t_timestamp where v > '1999-01-01 00:00:01'::timestamp;
select count(*) from t_timestamp where v > '2000-01-01 00:00:00'::timestamp;
select count(*) from t_timestamp where v > '2000-01-01 00:00:05'::timestamp;
select count(*) from t_timestamp where v > '2000-01-01 00:00:10'::timestamp;
select count(*) from t_timestamp where v > '2000-01-01 00:00:15'::timestamp;
select count(*) from t_timestamp where v > '2000-01-01 00:00:20'::timestamp;

select count(*) from t_timestamp where v < '1999-12-01 00:00:00'::timestamp;
select count(*) from t_timestamp where v < '2000-01-01 00:00:06'::timestamp;
select count(*) from t_timestamp where v < '2000-01-01 00:00:11'::timestamp;
select count(*) from t_timestamp where v < '2000-01-01 00:00:16'::timestamp;
select count(*) from t_timestamp where v < '2000-01-01 00:00:21'::timestamp;

select count(*) from t_timestamp where v = '1999-12-01 00:00:00'::timestamp;
select count(*) from t_timestamp where v = '2000-01-01 00:00:05'::timestamp;
select count(*) from t_timestamp where v = '2000-01-01 00:00:10'::timestamp;
select count(*) from t_timestamp where v = '2000-01-01 00:00:11'::timestamp;
select count(*) from t_timestamp where v = '2000-01-01 00:00:20'::timestamp;
select count(*) from t_timestamp where v = '2000-01-01 00:00:21'::timestamp;

select count(*) from t_timestamp where v >= '1999-12-01 00:00:00'::timestamp;
select count(*) from t_timestamp where v >= '2000-01-01 00:00:06'::timestamp;
select count(*) from t_timestamp where v >= '2000-01-01 00:00:11'::timestamp;
select count(*) from t_timestamp where v >= '2000-01-01 00:00:16'::timestamp;
select count(*) from t_timestamp where v >= '2000-01-01 00:00:21'::timestamp;

select count(*) from t_timestamp where v <= '1999-12-01 00:00:00'::timestamp;
select count(*) from t_timestamp where v <= '2000-01-01 00:00:05'::timestamp;
select count(*) from t_timestamp where v <= '2000-01-01 00:00:10'::timestamp;
select count(*) from t_timestamp where v <= '2000-01-01 00:00:15'::timestamp;
select count(*) from t_timestamp where v <= '2000-01-01 00:00:20'::timestamp;

-- oper(timestamp, date)
select count(*) from t_timestamp where v > '2000-01-01'::date;
select count(*) from t_timestamp where v < '2000-01-01'::date;
select count(*) from t_timestamp where v = '2000-01-01'::date;
select count(*) from t_timestamp where v >= '2000-01-01'::date;
select count(*) from t_timestamp where v <= '2000-01-01'::date;

-- oper(timestamp, timestamptz)
select count(*) from t_timestamp where v > '1999-01-01 00:00:01+00'::timestamptz;
select count(*) from t_timestamp where v > '2000-01-01 00:00:00+00'::timestamptz;
select count(*) from t_timestamp where v > '2000-01-01 00:00:05+00'::timestamptz;
select count(*) from t_timestamp where v > '2000-01-01 00:00:10+00'::timestamptz;
select count(*) from t_timestamp where v > '2000-01-01 00:00:15+00'::timestamptz;
select count(*) from t_timestamp where v > '2000-01-01 00:00:20+00'::timestamptz;

select count(*) from t_timestamp where v < '1999-12-01 00:00:00+00'::timestamptz;
select count(*) from t_timestamp where v < '2000-01-01 00:00:06+00'::timestamptz;
select count(*) from t_timestamp where v < '2000-01-01 00:00:11+00'::timestamptz;
select count(*) from t_timestamp where v < '2000-01-01 00:00:16+00'::timestamptz;
select count(*) from t_timestamp where v < '2000-01-01 00:00:21+00'::timestamptz;

select count(*) from t_timestamp where v = '1999-12-01 00:00:00+00'::timestamptz;
select count(*) from t_timestamp where v = '2000-01-01 00:00:05+00'::timestamptz;
select count(*) from t_timestamp where v = '2000-01-01 00:00:10+00'::timestamptz;
select count(*) from t_timestamp where v = '2000-01-01 00:00:11+00'::timestamptz;
select count(*) from t_timestamp where v = '2000-01-01 00:00:20+00'::timestamptz;
select count(*) from t_timestamp where v = '2000-01-01 00:00:21+00'::timestamptz;

select count(*) from t_timestamp where v >= '1999-12-01 00:00:00+00'::timestamptz;
select count(*) from t_timestamp where v >= '2000-01-01 00:00:06+00'::timestamptz;
select count(*) from t_timestamp where v >= '2000-01-01 00:00:11+00'::timestamptz;
select count(*) from t_timestamp where v >= '2000-01-01 00:00:16+00'::timestamptz;
select count(*) from t_timestamp where v >= '2000-01-01 00:00:21+00'::timestamptz;

select count(*) from t_timestamp where v <= '1999-12-01 00:00:00+00'::timestamptz;
select count(*) from t_timestamp where v <= '2000-01-01 00:00:05+00'::timestamptz;
select count(*) from t_timestamp where v <= '2000-01-01 00:00:10+00'::timestamptz;
select count(*) from t_timestamp where v <= '2000-01-01 00:00:15+00'::timestamptz;
select count(*) from t_timestamp where v <= '2000-01-01 00:00:20+00'::timestamptz;

reset client_min_messages;
drop table t_timestamp;

-- 
-- Test the timestamptz min/max types support 
-- 
create table t_timestamptz(i int, v timestamptz) with(minmax_columns='v');

insert into t_timestamptz(i, v) values (1, '2000-01-01 00:00:01'),(1, '2000-01-01 00:00:02'),(1, '2000-01-01 00:00:03'),
(1, '2000-01-01 00:00:04'),(1, '2000-01-01 00:00:05'),(1, '2000-01-01 00:00:06'),
(1, '2000-01-01 00:00:07'),(1, '2000-01-01 00:00:08'),(1, '2000-01-01 00:00:09'),
(1, '2000-01-01 00:00:10');

insert into t_timestamptz(i, v) values (1, '2000-01-01 00:00:11'),(1, '2000-01-01 00:00:12'),(1, '2000-01-01 00:00:13'),
(1, '2000-01-01 00:00:14'),(1, '2000-01-01 00:00:15'),(1, '2000-01-01 00:00:16'),
(1, '2000-01-01 00:00:17'),(1, '2000-01-01 00:00:18'),(1, '2000-01-01 00:00:19'),
(1, '2000-01-01 00:00:20');

set client_min_messages to log;
select count(*) from t_timestamptz where v > '1999-01-01 00:00:01+00'::timestamptz;
select count(*) from t_timestamptz where v > '2000-01-01 00:00:00+00'::timestamptz;
select count(*) from t_timestamptz where v > '2000-01-01 00:00:05+00'::timestamptz;
select count(*) from t_timestamptz where v > '2000-01-01 00:00:10+00'::timestamptz;
select count(*) from t_timestamptz where v > '2000-01-01 00:00:15+00'::timestamptz;
select count(*) from t_timestamptz where v > '2000-01-01 00:00:20+00'::timestamptz;

select count(*) from t_timestamptz where v < '1999-12-01 00:00:00+00'::timestamptz;
select count(*) from t_timestamptz where v < '2000-01-01 00:00:06+00'::timestamptz;
select count(*) from t_timestamptz where v < '2000-01-01 00:00:11+00'::timestamptz;
select count(*) from t_timestamptz where v < '2000-01-01 00:00:16+00'::timestamptz;
select count(*) from t_timestamptz where v < '2000-01-01 00:00:21+00'::timestamptz;

select count(*) from t_timestamptz where v = '1999-12-01 00:00:00+00'::timestamptz;
select count(*) from t_timestamptz where v = '2000-01-01 00:00:05+00'::timestamptz;
select count(*) from t_timestamptz where v = '2000-01-01 00:00:10+00'::timestamptz;
select count(*) from t_timestamptz where v = '2000-01-01 00:00:11+00'::timestamptz;
select count(*) from t_timestamptz where v = '2000-01-01 00:00:20+00'::timestamptz;
select count(*) from t_timestamptz where v = '2000-01-01 00:00:21+00'::timestamptz;

select count(*) from t_timestamptz where v >= '1999-12-01 00:00:00+00'::timestamptz;
select count(*) from t_timestamptz where v >= '2000-01-01 00:00:06+00'::timestamptz;
select count(*) from t_timestamptz where v >= '2000-01-01 00:00:11+00'::timestamptz;
select count(*) from t_timestamptz where v >= '2000-01-01 00:00:16+00'::timestamptz;
select count(*) from t_timestamptz where v >= '2000-01-01 00:00:21+00'::timestamptz;

select count(*) from t_timestamptz where v <= '1999-12-01 00:00:00+00'::timestamptz;
select count(*) from t_timestamptz where v <= '2000-01-01 00:00:05+00'::timestamptz;
select count(*) from t_timestamptz where v <= '2000-01-01 00:00:10+00'::timestamptz;
select count(*) from t_timestamptz where v <= '2000-01-01 00:00:15+00'::timestamptz;
select count(*) from t_timestamptz where v <= '2000-01-01 00:00:20+00'::timestamptz;

-- oper(timestamp, date)
select count(*) from t_timestamptz where v > '2000-01-01'::date;
select count(*) from t_timestamptz where v < '2000-01-01'::date;
select count(*) from t_timestamptz where v = '2000-01-01'::date;
select count(*) from t_timestamptz where v >= '2000-01-01'::date;
select count(*) from t_timestamptz where v <= '2000-01-01'::date;

-- oper(timestamptz, timestamp)
select count(*) from t_timestamptz where v > '1999-01-01 00:00:01'::timestamp;
select count(*) from t_timestamptz where v > '2000-01-01 00:00:00'::timestamp;
select count(*) from t_timestamptz where v > '2000-01-01 00:00:05'::timestamp;
select count(*) from t_timestamptz where v > '2000-01-01 00:00:10'::timestamp;
select count(*) from t_timestamptz where v > '2000-01-01 00:00:15'::timestamp;
select count(*) from t_timestamptz where v > '2000-01-01 00:00:20'::timestamp;

select count(*) from t_timestamptz where v < '1999-12-01 00:00:00'::timestamp;
select count(*) from t_timestamptz where v < '2000-01-01 00:00:06'::timestamp;
select count(*) from t_timestamptz where v < '2000-01-01 00:00:11'::timestamp;
select count(*) from t_timestamptz where v < '2000-01-01 00:00:16'::timestamp;
select count(*) from t_timestamptz where v < '2000-01-01 00:00:21'::timestamp;

select count(*) from t_timestamptz where v = '1999-12-01 00:00:00'::timestamp;
select count(*) from t_timestamptz where v = '2000-01-01 00:00:05'::timestamp;
select count(*) from t_timestamptz where v = '2000-01-01 00:00:10'::timestamp;
select count(*) from t_timestamptz where v = '2000-01-01 00:00:11'::timestamp;
select count(*) from t_timestamptz where v = '2000-01-01 00:00:20'::timestamp;
select count(*) from t_timestamptz where v = '2000-01-01 00:00:21'::timestamp;

select count(*) from t_timestamptz where v >= '1999-12-01 00:00:00'::timestamp;
select count(*) from t_timestamptz where v >= '2000-01-01 00:00:06'::timestamp;
select count(*) from t_timestamptz where v >= '2000-01-01 00:00:11'::timestamp;
select count(*) from t_timestamptz where v >= '2000-01-01 00:00:16'::timestamp;
select count(*) from t_timestamptz where v >= '2000-01-01 00:00:21'::timestamp;

select count(*) from t_timestamptz where v <= '1999-12-01 00:00:00'::timestamp;
select count(*) from t_timestamptz where v <= '2000-01-01 00:00:05'::timestamp;
select count(*) from t_timestamptz where v <= '2000-01-01 00:00:10'::timestamp;
select count(*) from t_timestamptz where v <= '2000-01-01 00:00:15'::timestamp;
select count(*) from t_timestamptz where v <= '2000-01-01 00:00:20'::timestamp;

reset client_min_messages;
drop table t_timestamptz;


-- 
-- Test the time min/max types support 
-- 
create table t_time(i int, v time) with(minmax_columns='v');

insert into t_time(i, v) values (1, '00:00:01'),(1, '00:00:02'),(1, '00:00:03'),(1, '00:00:04'),(1, '00:00:05'),
(1, '00:00:06'),(1, '00:00:07'),(1, '00:00:08'),(1, '00:00:09'),(1, '00:00:10');
insert into t_time(i, v) values (1, '00:00:11'),(1, '00:00:12'),(1, '00:00:13'),(1, '00:00:14'),(1, '00:00:15'),
(1, '00:00:16'),(1, '00:00:17'),(1, '00:00:18'),(1, '00:00:19'),(1, '00:00:20');

set client_min_messages to log;
select count(*) from t_time where v > '00:00:00'::time;
select count(*) from t_time where v > '00:00:05'::time;
select count(*) from t_time where v > '00:00:10'::time;
select count(*) from t_time where v > '00:00:15'::time;
select count(*) from t_time where v > '00:00:20'::time;

select count(*) from t_time where v < '00:00:00'::time;
select count(*) from t_time where v < '00:00:06'::time;
select count(*) from t_time where v < '00:00:11'::time;
select count(*) from t_time where v < '00:00:16'::time;
select count(*) from t_time where v < '00:00:21'::time;

select count(*) from t_time where v = '00:00:00'::time;
select count(*) from t_time where v = '00:00:05'::time;
select count(*) from t_time where v = '00:00:10'::time;
select count(*) from t_time where v = '00:00:11'::time;
select count(*) from t_time where v = '00:00:20'::time;
select count(*) from t_time where v = '00:00:21'::time;

select count(*) from t_time where v >= '00:00:00'::time;
select count(*) from t_time where v >= '00:00:06'::time;
select count(*) from t_time where v >= '00:00:11'::time;
select count(*) from t_time where v >= '00:00:16'::time;
select count(*) from t_time where v >= '00:00:21'::time;

select count(*) from t_time where v <= '00:00:00'::time;
select count(*) from t_time where v <= '00:00:05'::time;
select count(*) from t_time where v <= '00:00:10'::time;
select count(*) from t_time where v <= '00:00:15'::time;
select count(*) from t_time where v <= '00:00:20'::time;

-- oper(time, timetz)
select count(*) from t_time where v > '00:00:00+00'::timetz;
select count(*) from t_time where v > '00:00:05+00'::timetz;
select count(*) from t_time where v > '00:00:10+00'::timetz;
select count(*) from t_time where v > '00:00:15+00'::timetz;
select count(*) from t_time where v > '00:00:20+00'::timetz;

select count(*) from t_time where v < '00:00:00+00'::timetz;
select count(*) from t_time where v < '00:00:06+00'::timetz;
select count(*) from t_time where v < '00:00:11+00'::timetz;
select count(*) from t_time where v < '00:00:16+00'::timetz;
select count(*) from t_time where v < '00:00:21+00'::timetz;

select count(*) from t_time where v = '00:00:00+00'::timetz;
select count(*) from t_time where v = '00:00:05+00'::timetz;
select count(*) from t_time where v = '00:00:10+00'::timetz;
select count(*) from t_time where v = '00:00:11+00'::timetz;
select count(*) from t_time where v = '00:00:20+00'::timetz;
select count(*) from t_time where v = '00:00:21+00'::timetz;

select count(*) from t_time where v >= '00:00:00+00'::timetz;
select count(*) from t_time where v >= '00:00:06+00'::timetz;
select count(*) from t_time where v >= '00:00:11+00'::timetz;
select count(*) from t_time where v >= '00:00:16+00'::timetz;
select count(*) from t_time where v >= '00:00:21+00'::timetz;

select count(*) from t_time where v <= '00:00:00+00'::timetz;
select count(*) from t_time where v <= '00:00:05+00'::timetz;
select count(*) from t_time where v <= '00:00:10+00'::timetz;
select count(*) from t_time where v <= '00:00:15+00'::timetz;
select count(*) from t_time where v <= '00:00:20+00'::timetz;

reset client_min_messages;
drop table t_time;


-- 
-- Test the timetz min/max types support 
-- 
create table t_timetz(i int, v timetz) with(minmax_columns='v');

insert into t_timetz(i, v) values (1, '00:00:01'),(1, '00:00:02'),(1, '00:00:03'),(1, '00:00:04'),(1, '00:00:05'),
(1, '00:00:06'),(1, '00:00:07'),(1, '00:00:08'),(1, '00:00:09'),(1, '00:00:10');
insert into t_timetz(i, v) values (1, '00:00:11'),(1, '00:00:12'),(1, '00:00:13'),(1, '00:00:14'),(1, '00:00:15'),
(1, '00:00:16'),(1, '00:00:17'),(1, '00:00:18'),(1, '00:00:19'),(1, '00:00:20');

set client_min_messages to log;
select count(*) from t_timetz where v > '00:00:00+00'::timetz;
select count(*) from t_timetz where v > '00:00:05+00'::timetz;
select count(*) from t_timetz where v > '00:00:10+00'::timetz;
select count(*) from t_timetz where v > '00:00:15+00'::timetz;
select count(*) from t_timetz where v > '00:00:20+00'::timetz;

select count(*) from t_timetz where v < '00:00:00+00'::timetz;
select count(*) from t_timetz where v < '00:00:06+00'::timetz;
select count(*) from t_timetz where v < '00:00:11+00'::timetz;
select count(*) from t_timetz where v < '00:00:16+00'::timetz;
select count(*) from t_timetz where v < '00:00:21+00'::timetz;

select count(*) from t_timetz where v = '00:00:00+00'::timetz;
select count(*) from t_timetz where v = '00:00:05+00'::timetz;
select count(*) from t_timetz where v = '00:00:10+00'::timetz;
select count(*) from t_timetz where v = '00:00:11+00'::timetz;
select count(*) from t_timetz where v = '00:00:20+00'::timetz;
select count(*) from t_timetz where v = '00:00:21+00'::timetz;

select count(*) from t_timetz where v >= '00:00:00+00'::timetz;
select count(*) from t_timetz where v >= '00:00:06+00'::timetz;
select count(*) from t_timetz where v >= '00:00:11+00'::timetz;
select count(*) from t_timetz where v >= '00:00:16+00'::timetz;
select count(*) from t_timetz where v >= '00:00:21+00'::timetz;

select count(*) from t_timetz where v <= '00:00:00+00'::timetz;
select count(*) from t_timetz where v <= '00:00:05+00'::timetz;
select count(*) from t_timetz where v <= '00:00:10+00'::timetz;
select count(*) from t_timetz where v <= '00:00:15+00'::timetz;
select count(*) from t_timetz where v <= '00:00:20+00'::timetz;

-- oper(timetz, time)
select count(*) from t_timetz where v > '00:00:00'::time;
select count(*) from t_timetz where v > '00:00:05'::time;
select count(*) from t_timetz where v > '00:00:10'::time;
select count(*) from t_timetz where v > '00:00:15'::time;
select count(*) from t_timetz where v > '00:00:20'::time;

select count(*) from t_timetz where v < '00:00:00'::time;
select count(*) from t_timetz where v < '00:00:06'::time;
select count(*) from t_timetz where v < '00:00:11'::time;
select count(*) from t_timetz where v < '00:00:16'::time;
select count(*) from t_timetz where v < '00:00:21'::time;

select count(*) from t_timetz where v = '00:00:00'::time;
select count(*) from t_timetz where v = '00:00:05'::time;
select count(*) from t_timetz where v = '00:00:10'::time;
select count(*) from t_timetz where v = '00:00:11'::time;
select count(*) from t_timetz where v = '00:00:20'::time;
select count(*) from t_timetz where v = '00:00:21'::time;

select count(*) from t_timetz where v >= '00:00:00'::time;
select count(*) from t_timetz where v >= '00:00:06'::time;
select count(*) from t_timetz where v >= '00:00:11'::time;
select count(*) from t_timetz where v >= '00:00:16'::time;
select count(*) from t_timetz where v >= '00:00:21'::time;

select count(*) from t_timetz where v <= '00:00:00'::time;
select count(*) from t_timetz where v <= '00:00:05'::time;
select count(*) from t_timetz where v <= '00:00:10'::time;
select count(*) from t_timetz where v <= '00:00:15'::time;
select count(*) from t_timetz where v <= '00:00:20'::time;

reset client_min_messages;
drop table t_timetz;


-- 
-- Test the interval min/max types support 
-- 
create table t_interval(i int, v interval) with(minmax_columns='v');

insert into t_interval(i, v) values (1, '1 second'),(1, '2 second'),(1, '3 second'),(1, '4 second'),(1, '5 second'),
(1, '6 second'),(1, '7 second'),(1, '8 second'),(1, '9 second'),(1, '10 second');
insert into t_interval(i, v) values (1, '11 second'),(1, '12 second'),(1, '13 second'),(1, '14 second'),(1, '15 second'),
(1, '16 second'),(1, '17 second'),(1, '18 second'),(1, '19 second'),(1, '20 second');

set client_min_messages to log;
select count(*) from t_interval where v > '0 second'::interval;
select count(*) from t_interval where v > '5 second'::interval;
select count(*) from t_interval where v > '10 second'::interval;
select count(*) from t_interval where v > '15 second'::interval;
select count(*) from t_interval where v > '20 second'::interval;

select count(*) from t_interval where v < '0 second'::interval;
select count(*) from t_interval where v < '6 second'::interval;
select count(*) from t_interval where v < '11 second'::interval;
select count(*) from t_interval where v < '16 second'::interval;
select count(*) from t_interval where v < '21 second'::interval;

select count(*) from t_interval where v = '0 second'::interval;
select count(*) from t_interval where v = '5 second'::interval;
select count(*) from t_interval where v = '10 second'::interval;
select count(*) from t_interval where v = '11 second'::interval;
select count(*) from t_interval where v = '20 second'::interval;
select count(*) from t_interval where v = '21 second'::interval;

select count(*) from t_interval where v >= '0 second'::interval;
select count(*) from t_interval where v >= '6 second'::interval;
select count(*) from t_interval where v >= '11 second'::interval;
select count(*) from t_interval where v >= '16 second'::interval;
select count(*) from t_interval where v >= '21 second'::interval;

select count(*) from t_interval where v <= '0 second'::interval;
select count(*) from t_interval where v <= '5 second'::interval;
select count(*) from t_interval where v <= '10 second'::interval;
select count(*) from t_interval where v <= '15 second'::interval;
select count(*) from t_interval where v <= '20 second'::interval;

reset client_min_messages;
drop table t_interval;

reset pax_enable_debug;
reset pax_enable_sparse_filter;
reset pax_max_tuples_per_group;
