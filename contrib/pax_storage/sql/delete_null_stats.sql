-- Regression test for issue #2091:
-- After a DELETE on a PAX micro-partition whose column is entirely NULL,
-- "WHERE col IS NULL" must still return the remaining rows while the sparse
-- filter is enabled.
--
-- Root cause: DeleteWithVisibilityMap recomputed micro-partition stats using a
-- column projection of only the minmax / bloomfilter columns. When the table
-- declares none of those (or the all-NULL column is not among them), the
-- projected tuple left every tts_isnull flag false, so the refreshed stats
-- recorded "no nulls" (hasnull = false). The sparse filter then skipped the
-- whole block for an IS NULL qual and returned too few rows.
--
-- The fix projects every attribute when refreshing stats after DELETE, so the
-- per-column null counts (hasnull / allnull / nonnullrows) stay correct.

set pax.enable_sparse_filter = on;

-- Case 1: minimal reproducer from the issue. No minmax / bloomfilter columns,
-- c0 is entirely NULL. Pre-fix this returned 0 instead of 2.
create table pax_delete_null_2091 (c0 int, c1 boolean)
using pax
distributed by (c0);

insert into pax_delete_null_2091 (c1) values (true), (false), (false);
delete from pax_delete_null_2091 where c1;

select count(*) as isnull_rows from pax_delete_null_2091 where c0 is null;

select count(*) as total_rows from pax_delete_null_2091;

drop table pax_delete_null_2091;

-- Case 2: larger all-NULL columns of different types, partial DELETE.
create table pax_delete_null_2091_big (id int, payload int, tag text)
using pax
distributed by (id);

insert into pax_delete_null_2091_big
select i, null, null from generate_series(1, 2000) i;
delete from pax_delete_null_2091_big where id between 1 and 500;

select count(*) as payload_isnull from pax_delete_null_2091_big where payload is null;

select count(*) as tag_isnull from pax_delete_null_2091_big where tag is null;
select count(*) as payload_notnull from pax_delete_null_2091_big where payload is not null;

drop table pax_delete_null_2091_big;

-- Case 3: mixed NULL / non-NULL column. Checks both hasnull and nonnullrows
-- after DELETE (the column is neither all-NULL nor all-non-NULL).
create table pax_delete_null_2091_mixed (id int, v int)
using pax
distributed by (id);

insert into pax_delete_null_2091_mixed
select i, case when i % 2 = 0 then null else i end
from generate_series(1, 1000) i;
delete from pax_delete_null_2091_mixed where id between 1 and 200;

select count(*) as v_isnull from pax_delete_null_2091_mixed where v is null;

select count(*) as v_notnull from pax_delete_null_2091_mixed where v is not null;

drop table pax_delete_null_2091_mixed;

-- Case 4: minmax_columns declared on a different column, so the projection is
-- non-empty but still omits the all-NULL column. Pre-fix this also returned 0.
create table pax_delete_null_2091_mm (id int, nullcol int, key int)
using pax
with (minmax_columns = 'key')
distributed by (id);

insert into pax_delete_null_2091_mm
select i, null, i from generate_series(1, 1000) i;
delete from pax_delete_null_2091_mm where id between 1 and 100;

select count(*) as nullcol_isnull from pax_delete_null_2091_mm where nullcol is null;

select count(*) as total_rows from pax_delete_null_2091_mm;

drop table pax_delete_null_2091_mm;

reset pax.enable_sparse_filter;
