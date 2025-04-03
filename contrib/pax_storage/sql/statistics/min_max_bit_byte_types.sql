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
-- Test the bool min/max types support 
-- 
create table t_bool(i int, v bool) with(minmax_columns='v');
insert into t_bool(i, v) SELECT 1, true FROM generate_series(1, 10);
insert into t_bool(i, v) SELECT 1, false FROM generate_series(1, 10);
insert into t_bool(i, v) values(1, true), (1, true), (1, true), (1, true), (1, true), 
(1, false), (1, false), (1, false), (1, false), (1, false);

set client_min_messages to log;
select count(*) from t_bool where v > false;
select count(*) from t_bool where v > true;
select count(*) from t_bool where v >= true;
select count(*) from t_bool where v >= false;
select count(*) from t_bool where v < false;
select count(*) from t_bool where v < true;
select count(*) from t_bool where v <= true;
select count(*) from t_bool where v <= false;
select count(*) from t_bool where v = false;
select count(*) from t_bool where v = true;
reset client_min_messages;
drop table t_bool;

-- 
-- Test the char min/max types support 
-- 
create table t_char(i int, v char) with(minmax_columns='v');
insert into t_char(i, v) SELECT 1, '1' FROM generate_series(1, 10);
insert into t_char(i, v) SELECT 1, '2' FROM generate_series(1, 10);
insert into t_char(i, v) values(1, '1'), (1, '1'), (1, '1'), (1, '1'), (1, '1'), 
(1, '2'), (1, '2'), (1, '2'), (1, '2'), (1, '2');

set client_min_messages to log;
select count(*) from t_char where v > '1';
select count(*) from t_char where v > '2';
select count(*) from t_char where v >= '1';
select count(*) from t_char where v >= '2';
select count(*) from t_char where v < '2';
select count(*) from t_char where v < '1';
select count(*) from t_char where v <= '1';
select count(*) from t_char where v <= '2';
select count(*) from t_char where v = '2';
select count(*) from t_char where v = '1';
reset client_min_messages;
drop table t_char;


-- 
-- Test the bytea min/max types support 
-- 
create table t_bytea(i int, v bytea) with(minmax_columns='v');
insert into t_bytea(i, v) SELECT 1, '1' FROM generate_series(1, 10);
insert into t_bytea(i, v) SELECT 1, '2' FROM generate_series(1, 10);
insert into t_bytea(i, v) values(1, '1'), (1, '1'), (1, '1'), (1, '1'), (1, '1'), 
(1, '2'), (1, '2'), (1, '2'), (1, '2'), (1, '2');

set client_min_messages to log;
select count(*) from t_bytea where v > '1';
select count(*) from t_bytea where v > '2';
select count(*) from t_bytea where v >= '1';
select count(*) from t_bytea where v >= '2';
select count(*) from t_bytea where v < '2';
select count(*) from t_bytea where v < '1';
select count(*) from t_bytea where v <= '1';
select count(*) from t_bytea where v <= '2';
select count(*) from t_bytea where v = '2';
select count(*) from t_bytea where v = '1';
reset client_min_messages;
drop table t_bytea;

-- 
-- Test the bit min/max types support 
-- 
create table t_bit(i int, v bit) with(minmax_columns='v');
insert into t_bit(i, v) SELECT 1, '0' FROM generate_series(1, 10);
insert into t_bit(i, v) SELECT 1, '1' FROM generate_series(1, 10);

insert into t_bit(i, v) values(1, '0'), (1, '0'), (1, '0'), (1, '0'), (1, '0'),
(1, '1'), (1, '1'), (1, '1'), (1, '1'), (1, '1');

set client_min_messages to log;
select count(*) from t_bit where v > '1'::bit;
select count(*) from t_bit where v > '0'::bit;
select count(*) from t_bit where v >= '1'::bit;
select count(*) from t_bit where v >= '0'::bit;
select count(*) from t_bit where v < '0'::bit;
select count(*) from t_bit where v < '1'::bit;
select count(*) from t_bit where v <= '1'::bit;
select count(*) from t_bit where v <= '0'::bit;
select count(*) from t_bit where v = '0'::bit;
select count(*) from t_bit where v = '1'::bit;

-- oper(bit, varbit)
select count(*) from t_bit where v > '1'::varbit;
select count(*) from t_bit where v > '0'::varbit;
select count(*) from t_bit where v >= '1'::varbit;
select count(*) from t_bit where v >= '0'::varbit;
select count(*) from t_bit where v < '0'::varbit;
select count(*) from t_bit where v < '1'::varbit;
select count(*) from t_bit where v <= '1'::varbit;
select count(*) from t_bit where v <= '0'::varbit;
select count(*) from t_bit where v = '0'::varbit;
select count(*) from t_bit where v = '1'::varbit;
reset client_min_messages;
drop table t_bit;

-- 
-- Test the varbit min/max types support 
-- 
create table t_varbit(i int, v varbit) with(minmax_columns='v');
insert into t_varbit(i, v) SELECT 1, '0' FROM generate_series(1, 10);
insert into t_varbit(i, v) SELECT 1, '1' FROM generate_series(1, 10);
insert into t_varbit(i, v) values (1, '0'), (1, '0'), (1, '0'), (1, '0'), (1, '0'),
(1, '1'), (1, '1'), (1, '1'), (1, '1'), (1, '1');

set client_min_messages to log;
select count(*) from t_varbit where v > '1'::varbit;
select count(*) from t_varbit where v > '0'::varbit;
select count(*) from t_varbit where v >= '1'::varbit;
select count(*) from t_varbit where v >= '0'::varbit;
select count(*) from t_varbit where v < '0'::varbit;
select count(*) from t_varbit where v < '1'::varbit;
select count(*) from t_varbit where v <= '1'::varbit;
select count(*) from t_varbit where v <= '0'::varbit;
select count(*) from t_varbit where v = '0'::varbit;
select count(*) from t_varbit where v = '1'::varbit;

-- oper(varbit, bit)
select count(*) from t_varbit where v > '1'::bit;
select count(*) from t_varbit where v > '0'::bit;
select count(*) from t_varbit where v >= '1'::bit;
select count(*) from t_varbit where v >= '0'::bit;
select count(*) from t_varbit where v < '0'::bit;
select count(*) from t_varbit where v < '1'::bit;
select count(*) from t_varbit where v <= '1'::bit;
select count(*) from t_varbit where v <= '0'::bit;
select count(*) from t_varbit where v = '0'::bit;
select count(*) from t_varbit where v = '1'::bit;

reset client_min_messages;
drop table t_varbit;

reset pax_enable_debug;
reset pax_enable_sparse_filter;
reset pax_max_tuples_per_group;


