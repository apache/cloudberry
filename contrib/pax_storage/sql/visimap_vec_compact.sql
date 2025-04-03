-- set the number of tuples in a group to 5,
-- so we can test multiple group with visimap,
-- at the beginning, in the middle, or at the end
-- of a group.
set pax_max_tuples_per_group = 5;

-- column types contain:
-- 1. normal fix-length column, like int
-- 2. normal variable-length column, like text
-- 3. numeric type
-- 4. bool type, uses one bit instead of one byte
-- 5. char(n), bpchar type

create table pt1(a int, i int, t text, n numeric(15,2), b bool, bp char(16))
using pax
with(storage_format=porc);
explain (costs off) select ctid, * from pt1;
select ctid, * from pt1;

insert into pt1 select 1,
--case when i % 3 = 0 and i % 2 = 0 then null else i end case,
i,
case when i % 2 = 0 or i % 5 = 0 then null else 'text_' || i end case,
case when i % 3 = 0 or i % 2 = 0 then null else 10000 + i end case,
case when i % 3 = 0 and i % 2 = 0 then 't'::bool when i % 3 != 0 then 'f'::bool else null end case,
case when i % 2 = 0 or i % 5 = 0 then null else 'bpchar_' || i end case
from generate_series(1,15)i;

select ctid, * from pt1;

begin;
delete from pt1 where i < 2;
select ctid, * from pt1;
rollback;

begin;
delete from pt1 where i = 5;
select ctid, * from pt1;
rollback;

begin;
delete from pt1 where i >= 6 and i <= 7;
select ctid, * from pt1;
rollback;

begin;
delete from pt1 where i = 8;
select ctid, * from pt1;
rollback;

begin;
delete from pt1 where i > 7 and i <= 10;
select ctid, * from pt1;
rollback;

-- Test: all values are invisible
begin;
delete from pt1;
select ctid, * from pt1;
rollback;

-- Test: all columns are nulls
begin;
update pt1 set a = null, i = null, t = null, n = null, b = null, bp = null;
select ctid, * from pt1;
rollback;

-- Test if visimap works well with btree index
set optimizer to on;
create unique index on pt1(a, i);
begin;
delete from pt1 where i >= 4 and i < 8;
select ctid, * from pt1;
explain (costs off) select * from pt1 where a = 1 and i >= 2 and i < 10;
select * from pt1 where a = 1 and i >= 2 and i < 10;
rollback;
reset optimizer;

drop table pt1;

reset pax_max_tuples_per_group;
