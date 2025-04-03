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
-- Test the oid min/max types support 
-- 
create table t_oid(i int, v oid) with(minmax_columns='v');

insert into t_oid(i, v) values(1, generate_series(1, 10));
insert into t_oid(i, v) values(1, generate_series(10, 20));

set client_min_messages to log;
select count(*) from t_oid where v > 0::oid;
select count(*) from t_oid where v > 5::oid;
select count(*) from t_oid where v > 10::oid;
select count(*) from t_oid where v > 15::oid;
select count(*) from t_oid where v > 20::oid;

select count(*) from t_oid where v < 0::oid;
select count(*) from t_oid where v < 6::oid;
select count(*) from t_oid where v < 11::oid;
select count(*) from t_oid where v < 16::oid;
select count(*) from t_oid where v < 21::oid;

select count(*) from t_oid where v = 0::oid;
select count(*) from t_oid where v = 5::oid;
select count(*) from t_oid where v = 10::oid;
select count(*) from t_oid where v = 11::oid;
select count(*) from t_oid where v = 20::oid;
select count(*) from t_oid where v = 21::oid;

select count(*) from t_oid where v >= 0::oid;
select count(*) from t_oid where v >= 6::oid;
select count(*) from t_oid where v >= 11::oid;
select count(*) from t_oid where v >= 16::oid;
select count(*) from t_oid where v >= 21::oid;

select count(*) from t_oid where v <= 0::oid;
select count(*) from t_oid where v <= 5::oid;
select count(*) from t_oid where v <= 10::oid;
select count(*) from t_oid where v <= 15::oid;
select count(*) from t_oid where v <= 20::oid;
reset client_min_messages;

drop table t_oid;

-- 
-- Test the money min/max types support 
-- 
create table t_cash(i int, v money) with(minmax_columns='v');

insert into t_cash(i, v) values(1, generate_series(1, 10));
insert into t_cash(i, v) values(1, generate_series(10, 20));

set client_min_messages to log;
-- current type can't compare with int2/int4/int8
select count(*) from t_cash where v > 0::money;
select count(*) from t_cash where v > 5::money;
select count(*) from t_cash where v > 10::money;
select count(*) from t_cash where v > 15::money;
select count(*) from t_cash where v > 20::money;

select count(*) from t_cash where v < 0::money;
select count(*) from t_cash where v < 6::money;
select count(*) from t_cash where v < 11::money;
select count(*) from t_cash where v < 16::money;
select count(*) from t_cash where v < 21::money;

select count(*) from t_cash where v = 0::money;
select count(*) from t_cash where v = 5::money;
select count(*) from t_cash where v = 10::money;
select count(*) from t_cash where v = 11::money;
select count(*) from t_cash where v = 20::money;
select count(*) from t_cash where v = 21::money;

select count(*) from t_cash where v >= 0::money;
select count(*) from t_cash where v >= 6::money;
select count(*) from t_cash where v >= 11::money;
select count(*) from t_cash where v >= 16::money;
select count(*) from t_cash where v >= 21::money;

select count(*) from t_cash where v <= 0::money;
select count(*) from t_cash where v <= 5::money;
select count(*) from t_cash where v <= 10::money;
select count(*) from t_cash where v <= 15::money;
select count(*) from t_cash where v <= 20::money;
reset client_min_messages;
drop table t_cash;

-- 
-- Test the uuid min/max types support 
-- 
create table t_uuid(i int, v uuid) with(minmax_columns='v');

insert into t_uuid(i, v) values (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12'),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13'),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14'),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a15'),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a16'),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a17'),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a18'),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a19'),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a20');

insert into t_uuid(i, v) values (1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a21'),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a22'),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a23'),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a24'),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a25'),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a26'),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a27'),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a28'),(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a29'),
(1, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a30');

set client_min_messages to log;
-- current type can't compare with int2/int4/int8
select count(*) from t_uuid where v > 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a10'::uuid;
select count(*) from t_uuid where v > 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a15'::uuid;
select count(*) from t_uuid where v > 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a20'::uuid;
select count(*) from t_uuid where v > 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a25'::uuid;
select count(*) from t_uuid where v > 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a30'::uuid;

select count(*) from t_uuid where v < 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid;
select count(*) from t_uuid where v < 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a16'::uuid;
select count(*) from t_uuid where v < 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a21'::uuid;
select count(*) from t_uuid where v < 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a26'::uuid;
select count(*) from t_uuid where v < 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a31'::uuid;

select count(*) from t_uuid where v = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a00'::uuid;
select count(*) from t_uuid where v = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a15'::uuid;
select count(*) from t_uuid where v = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a20'::uuid;
select count(*) from t_uuid where v = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a21'::uuid;
select count(*) from t_uuid where v = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a30'::uuid;
select count(*) from t_uuid where v = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a31'::uuid;

select count(*) from t_uuid where v >= 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::uuid;
select count(*) from t_uuid where v >= 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a16'::uuid;
select count(*) from t_uuid where v >= 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a21'::uuid;
select count(*) from t_uuid where v >= 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a26'::uuid;
select count(*) from t_uuid where v >= 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a31'::uuid;

select count(*) from t_uuid where v <= 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a00'::uuid;
select count(*) from t_uuid where v <= 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a15'::uuid;
select count(*) from t_uuid where v <= 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a20'::uuid;
select count(*) from t_uuid where v <= 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a25'::uuid;
select count(*) from t_uuid where v <= 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a30'::uuid;
reset client_min_messages;
drop table t_uuid;

reset pax_enable_debug;
reset pax_enable_sparse_filter;
reset pax_max_tuples_per_group;
