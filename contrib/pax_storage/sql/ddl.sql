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
