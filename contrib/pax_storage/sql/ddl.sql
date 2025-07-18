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
