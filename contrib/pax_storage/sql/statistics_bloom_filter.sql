-- start_matchignore
-- m/LOG:  statement:/
-- m/no filter/
-- m/scan key build success/
-- m/Build a readable bloom filter/
-- m/^LOG:  Missing statistics for column: .*/
-- end_matchignore
set default_table_access_method = pax;

-- 
-- Test with small group
-- 
set pax_max_tuples_per_group = 10;
set pax_bloom_filter_work_memory_bytes = 102400; -- 100kb

-- create pax table with bloom filter reloptions 
create table t1(v1 int, v2 text, v3 varchar, v4 varchar(100), v5 bit, v6 float, v7 numeric, v8 numeric(20,10)) 
    with (bloomfilter_columns='v1,v2,v3,v4,v5,v6,v7,v8');
create table t2(v1 int, v2 text, v3 varchar, v4 varchar(100), v5 bit, v6 float, v7 numeric, v8 numeric(20,10)) 
    with (minmax_columns='v1,v2,v3,v4,v5,v6,v7,v8', bloomfilter_columns='v1,v2,v3,v4,v5,v6,v7,v8');

drop table t1;
drop table t2;

-- test bloom filter(only work on IN case)
set pax_enable_sparse_filter to on;
-- the fixed length and type by value type
create table t1(single_seg int, v1 int, v2 int) with (bloomfilter_columns='v1,v2');

insert into t1 values(1, generate_series(1, 100), generate_series(101, 200));
insert into t1 values(1, generate_series(1, 100), generate_series(101, 200));

set client_min_messages to log;

select * from t1 where v1 in (1, 2, 3);
select * from t1 where v1 in (1, 2, 3) and v2 > 100;
select * from t1 where v1 in (1, 2, 301);
select * from t1 where v1 in (1, 2, 301) and v2 > 100;
select * from t1 where v1 in (1, 2, 301) and v2 < 100;

select * from t1 where v1 in (1, 2, 3) and v2 in (4, 5, 6);
select * from t1 where v1 in (1, 2, 3) and v2 in (104, 105, 106);
select * from t1 where v1 in (1, 2, 3) and v2 in (101, 102, 103);

select * from t1 where v1 in (104, 105, 106) and v2 > 100;
select * from t1 where v1 in (104, 105, 106, NULL) and v2 > 100;

select * from t1 where v1 in (1, 2, NULL) and v2 in (4, 5, 6); -- filter all by (4, 5, 6)
select * from t1 where v1 in (1, 2, 3) and v2 in (4, 5, NULL); -- support filter NULL
select * from t1 where v1 in (1, 2, NULL) and v2 in (4, 5, NULL); -- support filter NULL

set client_min_messages to log;
select * from t1 where v1 in (12, 55, 77); -- different group
select * from t1 where v1 in (12, 55, 77) and v2 > 100;
reset client_min_messages;

drop table t1;

-- the non-fixed type
create table t2(single_seg int, v1 varchar(100), v2 varchar(100)) with (bloomfilter_columns='v1,v2');

insert into t2 values(1, generate_series(1, 100), generate_series(101, 200));
insert into t2 values(1, generate_series(1, 100), generate_series(101, 200));

set client_min_messages to log;
select * from t2 where v1 in ('1', '2', '3');
select * from t2 where v1 in ('1', '2', '3') and v2 > '100';

select * from t2 where v1 in ('1', '2', '3') and v2 in ('4', '5', '6');
select * from t2 where v1 in ('1', '2', '3') and v2 in ('104', '105', '106');
select * from t2 where v1 in ('1', '2', '3') and v2 in ('101', '102', '103');

select * from t2 where v1 in ('1', '2', NULL) and v2 in ('4', '5', '6'); -- filter all by ('4', '5', '6')
select * from t2 where v1 in ('1', '2', '3') and v2 in ('4', '5', NULL); -- support filter NULL
select * from t2 where v1 in ('1', '2', NULL) and v2 in ('4', '5', NULL); -- support filter NULL

select * from t2 where v1 in ('104', '105', '106') and v2 > '100';

set client_min_messages to log;
select * from t2 where v1 in ('12', '55', '77'); -- different group
select * from t2 where v1 in ('12', '55', '77') and v2 > '100';
reset client_min_messages;

drop table t2;

-- the fixed length but not type by value type
create table t3(single_seg int, v1 uuid, v2 int) with (bloomfilter_columns='v1,v2');

insert into t3 values 
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 31),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', 32),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13', 33),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14', 34),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a15', 35),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a16', 36),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a17', 37),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a18', 38),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a19', 39),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a20', 40),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a21', 41),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22', 42),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a23', 43),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a24', 44),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a25', 45),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a26', 46),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a27', 47),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a28', 48),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a29', 49),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a30', 50);

insert into t3 values 
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 31),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', 32),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13', 33),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14', 34),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a15', 35),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a16', 36),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a17', 37),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a18', 38),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a19', 39),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a20', 40),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a21', 41),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22', 42),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a23', 43),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a24', 44),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a25', 45),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a26', 46),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a27', 47),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a28', 48),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a29', 49),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a30', 50);

set client_min_messages to log;
select * from t3 where v1 in ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13');
select * from t3 where v1 in ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13') and v2 > 30;

select * from t3 where v1 in ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13') and v2 in (4, 5, 6);
select * from t3 where v1 in ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13') and v2 in (34, 35, 36);
select * from t3 where v1 in ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13') and v2 in (31, 32, 33);

select * from t3 where v1 in ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380aaa', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380aab', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380aac') and v2 > 30;
select * from t3 where v1 in ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380aaa', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380aab', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380aac') and v2 > 300;
select * from t3 where v2 > 300 and v1 in ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380aaa', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380aab');

set client_min_messages to log;
select * from t3 where v1 in ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a29'); -- different group
select * from t3 where v1 in ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14');
select * from t3 where v1 in ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a23', 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a28');
reset client_min_messages;
drop table t3;

-- test the big bloom filter
set pax_max_tuples_per_group to 16384;
set pax_max_tuples_per_file to 131072;

create table t4(single_seg int, v1 varchar, v2 varchar) with (bloomfilter_columns='v1,v2');
insert into t4 values(1, generate_series(1, 1000000), generate_series(1000001, 2000000));

set client_min_messages to log;
select * from t4 where v1 in ('1000008', '1000009') and v2 > '1';
select * from t4 where v1 in ('100008', '1000009') and v2 > '1';
select * from t4 where v1 in ('666', '1000009') and v2 > '1';

select * from t4 where v1 > '1' and v2 in ('8', '9');
select * from t4 where v1 > '1' and v2 in ('8', '1000009');
select * from t4 where v1 in ('8', '1000009') and v2 in ('8', '1000009');
reset client_min_messages;

reset pax_bloom_filter_work_memory_bytes;
reset pax_max_tuples_per_group;
reset pax_max_tuples_per_file;
reset pax_enable_sparse_filter;
