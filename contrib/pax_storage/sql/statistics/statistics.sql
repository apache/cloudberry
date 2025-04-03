set default_table_access_method = pax;


-- 
-- Test with small group
-- 
set pax_max_tuples_per_group = 10;

-- test min/max type support
create table t1(v1 int, v2 text, v3 float8, v4 bool) with (minmax_columns='v1,v2,v3,v4');
insert into t1 select i,i::text,i::float8, i % 2 > 0 from generate_series(1, 1000)i;
select * from get_pax_aux_table('t1');
drop table t1;

create table t2(v1 bpchar, v2 bpchar(20), v3 varchar(20), v4 varchar(20)) with (minmax_columns='v1,v2,v3,v4');
insert into t2 select i::bpchar,i::bpchar,i::varchar, i::varchar from generate_series(1, 1000)i;
select * from get_pax_aux_table('t2');
drop table t2;

-- stats reloption without order
create table t1(v1 int, v2 text, v3 float8, v4 bool) with (minmax_columns='v2,v4,v1,v3', bloomfilter_columns='v4,v2,v3');
insert into t1 select i,i::text,i::float8, i % 2 > 0 from generate_series(1, 1000)i;
select * from get_pax_aux_table('t1');
drop table t1;

-- test `hasnull/allnull`/`count`, not need setting the minmax_columns
create table t1(v1 int, v2 float8);

insert into t1 values(generate_series(1, 1000), NULL);
select * from get_pax_aux_table('t1');
truncate t1;

insert into t1 select NULL, NULL from generate_series(1, 200)i;
select * from get_pax_aux_table('t1');
truncate t1;

insert into t1 SELECT (CASE WHEN i % 2 > 0 THEN NULL ELSE i END), (CASE WHEN i % 2 < 1 THEN NULL ELSE i END) FROM generate_series(1, 1000)i;
select * from get_pax_aux_table('t1');
truncate t1;

drop table t1;

-- test `sum`

create table t1(v1 int2, v2 int4, v3 int8, v4 float4, v5 float8, v6 money, v7 interval) with (minmax_columns='v1,v2,v3,v4,v5,v6,v7');
insert into t1 values(generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), 
  generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), (interval '3 seconds'));
select * from get_pax_aux_table('t1');
drop table t1;

create table t_int2(v1 int2) with (minmax_columns='v1');
insert into t_int2 values(generate_series(1, 1000));
select * from get_pax_aux_table('t_int2');
drop table t_int2;

create table t_int4(v1 int4) with (minmax_columns='v1');
insert into t_int4 values(generate_series(1001, 2000));
select * from get_pax_aux_table('t_int4');
drop table t_int4;

create table t_int8(v1 int8) with (minmax_columns='v1');
insert into t_int8 values(generate_series(2001, 3000));
select * from get_pax_aux_table('t_int8');
drop table t_int8;

create table t_float4(v1 float4) with (minmax_columns='v1');
insert into t_float4 values(generate_series(3001, 4000));
select * from get_pax_aux_table('t_float4');
drop table t_float4;

create table t_float8(v1 float8) with (minmax_columns='v1');
insert into t_float8 values(generate_series(4001, 5000));
select * from get_pax_aux_table('t_float8');
drop table t_float8;

create table t_money(v1 int4, v2 money) with (minmax_columns='v1,v2');
insert into t_money values(generate_series(5001, 6000), generate_series(5001, 6000));
select * from get_pax_aux_table('t_money');
drop table t_money;

create table t_interval(v1 interval) with (minmax_columns='v1');

insert into t_interval values 
  (interval '1 seconds'), (interval '2 seconds'), (interval '3 seconds'), (interval '4 seconds'), (interval '5 seconds'), (interval '50 seconds'), 
  (interval '6 minutes'), (interval '7 minutes'), (interval '8 minutes'), (interval '9 minutes'), (interval '10 minutes'), (interval '60 minutes'), 
  (interval '7 hours') , (interval '8 hours') , (interval '9 hours') , (interval '10 hours') , (interval '11 hours') , (interval '400 hours');
select * from get_pax_aux_table('t_interval');
drop table t_interval;

reset pax_max_tuples_per_group;

-- 
-- Test with small group
-- 

create table t1(v1 int, v2 text, v3 float8, v4 bool) with (minmax_columns='v1,v2,v3,v4');
insert into t1 select i,i::text,i::float8, i % 2 > 0 from generate_series(1, 1000)i;
select * from get_pax_aux_table('t1');
drop table t1;

create table t1(v1 int, v2 float8);
insert into t1 values(generate_series(1, 1000), NULL);
select * from get_pax_aux_table('t1');
truncate t1;

insert into t1 select NULL, NULL from generate_series(1, 200)i;
select * from get_pax_aux_table('t1');
truncate t1;

insert into t1 SELECT (CASE WHEN i % 2 > 0 THEN NULL ELSE i END), (CASE WHEN i % 2 < 1 THEN NULL ELSE i END) FROM generate_series(1, 1000)i;
select * from get_pax_aux_table('t1');
truncate t1;

drop table t1;

create table t1(v1 int2, v2 int4, v3 int8, v4 float4, v5 float8, v6 money, v7 interval) with (minmax_columns='v1,v2,v3,v4,v5,v6,v7');
insert into t1 values(generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), 
  generate_series(1, 1000), generate_series(1, 1000), generate_series(1, 1000), (interval '3 seconds'));
select * from get_pax_aux_table('t1');
drop table t1;

create table t_int2(v1 int2) with (minmax_columns='v1');
insert into t_int2 values(generate_series(1, 1000));
select * from get_pax_aux_table('t_int2');
drop table t_int2;

create table t_int4(v1 int4) with (minmax_columns='v1');
insert into t_int4 values(generate_series(1001, 2000));
select * from get_pax_aux_table('t_int4');
drop table t_int4;

create table t_int8(v1 int8) with (minmax_columns='v1');
insert into t_int8 values(generate_series(2001, 3000));
select * from get_pax_aux_table('t_int8');
drop table t_int8;

create table t_float4(v1 float4) with (minmax_columns='v1');
insert into t_float4 values(generate_series(3001, 4000));
select * from get_pax_aux_table('t_float4');
drop table t_float4;

create table t_float8(v1 float8) with (minmax_columns='v1');
insert into t_float8 values(generate_series(4001, 5000));
select * from get_pax_aux_table('t_float8');
drop table t_float8;

create table t_money(v1 int4, v2 money) with (minmax_columns='v1,v2');
insert into t_money values(generate_series(5001, 6000), generate_series(5001, 6000));
select * from get_pax_aux_table('t_money');
drop table t_money;

create table t_interval(v1 interval) with (minmax_columns='v1');

-- 15sec + 50sec + 40mins + 60 mins + 45hrs + 400hrs
insert into t_interval values 
  (interval '1 seconds'), (interval '2 seconds'), (interval '3 seconds'), (interval '4 seconds'), (interval '5 seconds'), (interval '50 seconds'), 
  (interval '6 minutes'), (interval '7 minutes'), (interval '8 minutes'), (interval '9 minutes'), (interval '10 minutes'), (interval '60 minutes'), 
  (interval '7 hours') , (interval '8 hours') , (interval '9 hours') , (interval '10 hours') , (interval '11 hours') , (interval '400 hours');
select * from get_pax_aux_table('t_interval');
drop table t_interval;

-- 
-- Test the update/delete DML, stats should be updated
-- 
set pax_max_tuples_per_group = 250;

-- delete part of data in the first group
create table t1_update_stats(v1 int, v2 int, v3 int) with (minmax_columns='v1,v2');
insert into t1_update_stats values(generate_series(1, 1000), generate_series(1001, 2000), 1);
select * from get_pax_aux_table('t1_update_stats');
-- v1: ((1 + 1000) * 1000 / 2) - ((1 + 20) * 20 / 2) = 500290
-- v2: ((1001 + 2000) * 1000 / 2) - ((1001 + 1020) * 20 / 2) = 1480290
delete from t1_update_stats where v1 <= 20;
select * from get_pax_aux_table('t1_update_stats');
drop table t1_update_stats;

-- delete part of data in the second group
create table t2_update_stats(v1 int, v2 int, v3 int) with (minmax_columns='v1,v2');
insert into t2_update_stats values(generate_series(1, 1000), generate_series(1001, 2000), 1);
select * from get_pax_aux_table('t2_update_stats');
-- v1: ((1 + 1000) * 1000 / 2) - ((301 + 400) * 100 / 2) = 465450
-- v2: ((1001 + 2000) * 1000 / 2) - ((1301 + 1400) * 100 / 2) = 1365450
delete from t2_update_stats where v1 <= 400 and v1 > 300;
select * from get_pax_aux_table('t2_update_stats');
drop table t2_update_stats;

-- delete part of data in all group
create table t3_update_stats(v1 int, v2 int, v3 int) with (minmax_columns='v1,v2');
insert into t3_update_stats values(generate_series(1, 1000), generate_series(1001, 2000), 1);
select * from get_pax_aux_table('t3_update_stats');
-- v1: ((1 + 1000) * 1000 / 2) - ((1 + 900) * 900 / 2) = 95050
-- v2: ((1001 + 2000) * 1000 / 2) - ((1001 + 1900) * 900 / 2) = 195050
delete from t3_update_stats where v2 <= 1900;
select * from get_pax_aux_table('t3_update_stats');
drop table t3_update_stats;

-- delete all 
create table t4_update_stats(v1 int, v2 int, v3 int) with (minmax_columns='v1,v2');
insert into t4_update_stats values(generate_series(1, 1000), generate_series(1001, 2000), 1);
select * from get_pax_aux_table('t4_update_stats');
-- v1: 0
-- v2: 0
delete from t4_update_stats;
select * from get_pax_aux_table('t4_update_stats');
drop table t4_update_stats;

-- update part of data in the first group
create table t5_update_stats(v1 int, v2 int, v3 int) with (minmax_columns='v1,v2');
insert into t5_update_stats values(generate_series(1, 1000), generate_series(1001, 2000), 1);
select * from get_pax_aux_table('t5_update_stats');
-- v1(block 0): ((1 + 1000) * 1000 / 2) - ((1 + 20) * 20 / 2) = 500290
-- v2(block 0): ((1001 + 2000) * 1000 / 2) - ((1001 + 1020) * 20 / 2) = 1480290
-- v1(block 1): (1 + 20) * 20 / 2 + 20 = 230
-- v2(block 1): (1001 + 1020) * 20 / 2 + 20 = 20230
update t5_update_stats set v1 = v1 + 1, v2 = v2 + 1 where v1 <= 20;
select * from get_pax_aux_table('t5_update_stats');
drop table t5_update_stats;

-- update part of data in the second group
create table t6_update_stats(v1 int, v2 int, v3 int) with (minmax_columns='v1,v2');
insert into t6_update_stats values(generate_series(1, 1000), generate_series(1001, 2000), 1);
select * from get_pax_aux_table('t6_update_stats');
-- v1(block 0): ((1 + 1000) * 1000 / 2) - ((301 + 400) * 100 / 2) = 465450
-- v2(block 0): ((1001 + 2000) * 1000 / 2) - ((1301 + 1400) * 100 / 2) = 1365450
-- v1(block 1): (301 + 400) * 100 / 2 + 100 = 35150
-- v2(block 1): (1301 + 1400) * 100 / 2 + 100 = 135150
update t6_update_stats set v1 = v1 + 1, v2 = v2 + 1 where v1 <= 400 and v1 > 300;
select * from get_pax_aux_table('t6_update_stats');
drop table t6_update_stats;

-- update part of data in all group
create table t7_update_stats(v1 int, v2 int, v3 int) with (minmax_columns='v1,v2');
insert into t7_update_stats values(generate_series(1, 1000), generate_series(1001, 2000), 1);
select * from get_pax_aux_table('t7_update_stats');
-- v1(block 0): ((1 + 1000) * 1000 / 2) - ((1 + 900) * 900 / 2) = 95050
-- v2(block 0): ((1001 + 2000) * 1000 / 2) - ((1001 + 1900) * 900 / 2) = 195050
-- v1(block 1): (1 + 900) * 900 / 2 + 900 = 406350
-- v2(block 1): (1001 + 1900) * 900 / 2 + 900 = 1306350
update t7_update_stats set v1 = v1 + 1, v2 = v2 + 1 where v2 <= 1900;
select * from get_pax_aux_table('t7_update_stats');
drop table t7_update_stats;

-- update all 
create table t8_update_stats(v1 int, v2 int, v3 int) with (minmax_columns='v1,v2');
insert into t8_update_stats values(generate_series(1, 1000), generate_series(1001, 2000), 1);
select * from get_pax_aux_table('t8_update_stats');
-- v1(block 0): 0
-- v2(block 0): 0
-- v1(block 1): (2 + 1001) * 1000 / 2 = 501500
-- v2(block 1): (1002 + 2001) * 1000 / 2 = 1501500
update t8_update_stats set v1 = v1 + 1, v2 = v2 + 1;
select * from get_pax_aux_table('t8_update_stats');
drop table t8_update_stats;

-- delete cross multi files
create table t9_update_stats(v1 int, v2 int, v3 int) with (minmax_columns='v1,v2');
insert into t9_update_stats values(generate_series(1, 1000), generate_series(1001, 2000), 1);
insert into t9_update_stats values(generate_series(1, 1000), generate_series(1001, 2000), 1);
insert into t9_update_stats values(generate_series(1, 100), generate_series(101, 200), 1);
insert into t9_update_stats values(generate_series(100, 1000), generate_series(1101, 2000), 1);
select * from get_pax_aux_table('t9_update_stats');
delete from t9_update_stats where v1 <= 20;
select * from get_pax_aux_table('t9_update_stats');
drop table t9_update_stats;

-- update cross multi files
create table t10_update_stats(v1 int, v2 int, v3 int) with (minmax_columns='v1,v2');
insert into t10_update_stats values(generate_series(1, 1000), generate_series(1001, 2000), 1);
insert into t10_update_stats values(generate_series(1, 1000), generate_series(1001, 2000), 1);
insert into t10_update_stats values(generate_series(1, 100), generate_series(101, 200), 1);
insert into t10_update_stats values(generate_series(100, 1000), generate_series(1101, 2000), 1);
select * from get_pax_aux_table('t10_update_stats');
update t10_update_stats set v1 = v1 + 1, v2 = v2 + 1 where v1 <= 20;
select * from get_pax_aux_table('t10_update_stats');
drop table t10_update_stats;


-- delete twice
set pax_max_tuples_per_group = 25;
create table t_delete_twice_stats(v1 int, v2 int, v3 int) with (minmax_columns='v2,v3');
insert into t_delete_twice_stats values(1, generate_series(1, 100), generate_series(101, 200));
select * from get_pax_aux_table('t_delete_twice_stats');
-- delete from group 1
delete from t_delete_twice_stats where v2 <= 10;
select sum(v2), sum(v3) from t_delete_twice_stats;
select * from get_pax_aux_table('t_delete_twice_stats');
-- delete from group 2
delete from t_delete_twice_stats where v2 > 30 and v2 <= 40;
select sum(v2), sum(v3) from t_delete_twice_stats;
select * from get_pax_aux_table('t_delete_twice_stats');
drop table t_delete_twice_stats;

-- update twice
create table t_update_twice_stats(v1 int, v2 int, v3 int) with (minmax_columns='v2,v3');
insert into t_update_twice_stats values(1, generate_series(1, 100), generate_series(101, 200));
select * from get_pax_aux_table('t_update_twice_stats');
-- update from group 1
update t_update_twice_stats set v2 = v2 + 1, v3 = v3 + 1 where v2 <= 10;
select sum(v2), sum(v3) from t_update_twice_stats;
select * from get_pax_aux_table('t_update_twice_stats');
-- delete from group 2
update t_update_twice_stats set v2 = v2 + 1, v3 = v3 + 1 where v2 > 30 and v2 <= 40;
select sum(v2), sum(v3) from t_update_twice_stats;
select * from get_pax_aux_table('t_update_twice_stats');
drop table t_update_twice_stats;

reset pax_max_tuples_per_group;
