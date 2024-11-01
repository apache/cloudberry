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

reset default_table_access_method;
reset search_path;
