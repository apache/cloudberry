-- test legacy/gp-style partition tables

create schema gppt_ao_column;
set search_path="$user",gppt_ao_column,public;

-- set default table access method to non-ao/co table
SET default_table_access_method=heap;

CREATE TABLE gppt_ao_column.pt_ao_column (
a date NOT NULL,
b integer NOT NULL,
c numeric(20,10),
d integer,
bi bigint NOT NULL,
ts timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
)
using ao_column
WITH (compresstype=zlib, compresslevel=6)
DISTRIBUTED RANDOMLY
PARTITION BY RANGE (a) (
START ('2020-01-01'::date) INCLUSIVE
END ('2024-01-01'::date) EXCLUSIVE
EVERY (interval '1 Year')
)
;

select relname, relkind, amname, reloptions from pg_class c left join pg_am am on c.relam=am.oid where relname='pt_ao_column';
\d+ gppt_ao_column.pt_ao_column
\d+ gppt_ao_column.pt_ao_column_1_prt_2

ALTER TABLE gppt_ao_column.pt_ao_column ADD PARTITION START ('2027-01-01') INCLUSIVE END ('2028-01-01') EXCLUSIVE;
\d+ gppt_ao_column.pt_ao_column_1_prt_11
select relname, relkind, amname, reloptions from pg_class c left join pg_am am on c.relam=am.oid where relname='pt_ao_column_1_prt_11';

CREATE TABLE gppt_ao_column.pt2_ao_column (
a date NOT NULL,
b integer NOT NULL,
c numeric(20,10),
d integer,
bi bigint NOT NULL,
ts timestamp DEFAULT CURRENT_TIMESTAMP NOT NULL
)
using ao_column
DISTRIBUTED RANDOMLY
PARTITION BY RANGE (a) (
START ('2020-01-01'::date) INCLUSIVE
END ('2024-01-01'::date) EXCLUSIVE
EVERY (interval '1 Year')
)
;

select relname, relkind, amname, reloptions from pg_class c left join pg_am am on c.relam=am.oid where relname='pt2_ao_column';
\d+ gppt_ao_column.pt2_ao_column
\d+ gppt_ao_column.pt2_ao_column_1_prt_2

ALTER TABLE gppt_ao_column.pt2_ao_column ADD PARTITION START ('2027-01-01') INCLUSIVE END ('2028-01-01') EXCLUSIVE;
\d+ gppt_ao_column.pt2_ao_column
\d+ gppt_ao_column.pt2_ao_column_1_prt_11
select relname, relkind, amname, reloptions from pg_class c left join pg_am am on c.relam=am.oid where relname='pt2_ao_column_1_prt_11';

create table gppt_ao_column.alter_part_tab1 (id SERIAL, a1 int, a2 char(5), a3 text)
  WITH (appendonly=true, orientation=column, compresstype=zlib)
  partition by list(a2) subpartition by range(a1)
  subpartition template (
    default subpartition subothers,
    subpartition sp1 start(1) end(9) with(appendonly=true,orientation=column,compresstype=rle_type),
    subpartition sp2 start(11) end(20) with(appendonly=true,orientation=column,compresstype=zstd))
   (partition p1 values('val1') , partition p2 values('val2'));

create index on gppt_ao_column.alter_part_tab1(a1);
create index on gppt_ao_column.alter_part_tab1 using bitmap(a3);
alter table gppt_ao_column.alter_part_tab1 add column a4 numeric default 5.5;
update gppt_ao_column.alter_part_tab1 set a4 = a1 % 2;
ALTER TABLE gppt_ao_column.alter_part_tab1 ADD partition p31 values(1);
\d+ gppt_ao_column.alter_part_tab1
\d+ gppt_ao_column.alter_part_tab1_1_prt_p31


reset default_table_access_method;
reset search_path;
