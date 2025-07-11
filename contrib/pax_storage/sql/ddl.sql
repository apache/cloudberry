set default_table_access_method = pax;
create table pax_test.t1(
    id int,
    name text not null,
    height float not null,
    decimal_col decimal(10, 2) not null,
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null
)  using pax distributed BY (id);
\d+ pax_test.t1

create table pax_test.t2(
    id int,
    name text not null,
    height float not null,
    decimal_col decimal(10, 2) not null,
    created_at timestamp with time zone not null,
    updated_at timestamp with time zone not null
);
\d+ pax_test.t2

insert into pax_test.t1 (id, name, height, decimal_col, created_at, updated_at) values
    (1, 'Alice', 1.65, 1.23, '2023-05-17 17:56:49.633664+08', '2023-05-17 17:56:49.633664+08'),
    (2, 'Bob', 1.75, 2.34, '2023-05-17 17:56:49.633664+08', '2023-05-17 17:56:49.633664+08'),
    (3, 'Carol', 1.85, 3.45, '2023-05-17 17:56:49.633664+08', '2023-05-17 17:56:49.633664+08');

alter table pax_test.t1 add column new_col1 int;
alter table pax_test.t1 add column new_col2 int default null;
alter table pax_test.t1 add column new_col3 int default 0;
alter table pax_test.t1 add column new_col4 int default 12;

select * from pax_test.t1;

alter table pax_test.t1 drop column new_col2;
alter table pax_test.t1 drop column new_col3;

vacuum pax_test.t1;
vacuum full pax_test.t1;

drop table pax_test.t1;
drop table pax_test.t2;

-- alter column with options 
create table pax_test.t3 (v1 numeric(100,1)) with(compresstype=zstd, compresslevel=1);
alter table pax_test.t3 alter column v1 type numeric;
drop table pax_test.t3;
-- add column with options
create table pax_test.t4 (v1 text) with(compresstype=zstd, compresslevel=1);
alter table pax_test.t4 add column v2 text;
drop table pax_test.t4;

-- test pg_relation_size
CREATE TABLE pt_pax (id int, date date) using pax DISTRIBUTED BY (id)
PARTITION BY RANGE (date)
( PARTITION subpt1 START (date '2016-01-01') INCLUSIVE ,
  PARTITION subpt2 START (date '2016-02-01') INCLUSIVE);

SELECT pg_catalog.pg_relation_size('pt_pax'::regclass);
select 1 from (select count(*) from gp_toolkit.gp_size_of_schema_disk) empty_out;
drop table pt_pax;

-- expect create ok 
create table pax_test.partition_cow(c1 int, c2 bigint, c3 varchar, c4 text, c5 date, c6 float4, c7 float8, c8 numeric, c9 interval)
partition by range(c1) (start(1) end(15000) every(5000))
with (minmax_columns='c1,c2,c3,c4,c5,c6,c7,c8,c9');
-- expect create ok 
create table pax_test.partition_cow_1(c1 int, c2 bigint, c3 varchar, c4 text, c5 date, c6 float4, c7 float8, c8 numeric, c9 interval)
partition by range(c1) (start(1) end(15000) every(5000))
using pax with (minmax_columns='c1,c2,c3,c4,c5,c6,c7,c8,c9');

set default_table_access_method = heap;
-- expect create fail
create table pax_test.partition_cow_2(c1 int, c2 bigint, c3 varchar, c4 text, c5 date, c6 float4, c7 float8, c8 numeric, c9 interval)
partition by range(c1) (start(1) end(15000) every(5000))
with (minmax_columns='c1,c2,c3,c4,c5,c6,c7,c8,c9');
set default_table_access_method = pax;
drop table pax_test.partition_cow;
drop table pax_test.partition_cow_1;