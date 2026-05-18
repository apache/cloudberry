-- Regression test for issue #1749:
-- PAX DELETE crashes with SIGSEGV when bloomfilter_columns are not a
-- subset of minmax_columns. The stats refresh inside
-- DeleteWithVisibilityMap must project every column it reads.

-- Case 1: bloomfilter column (payload) is NOT in minmax_columns.
-- Pre-fix: segment crashed on DELETE.
drop table if exists pax_delete_bloom_crash;
create table pax_delete_bloom_crash (id int, k int, payload text)
using pax
with (minmax_columns = 'id', bloomfilter_columns = 'payload');

insert into pax_delete_bloom_crash
select i, i % 10, 'payload-' || i::text
from generate_series(1, 10000) as i;

delete from pax_delete_bloom_crash where id between 1 and 100;
select count(*) from pax_delete_bloom_crash;

drop table pax_delete_bloom_crash;

-- Case 2: bloomfilter only, no minmax columns.
drop table if exists pax_delete_bf_only;
create table pax_delete_bf_only (id int, payload text)
using pax
with (bloomfilter_columns = 'payload');

insert into pax_delete_bf_only
select i, 'payload-' || i::text from generate_series(1, 5000) as i;

delete from pax_delete_bf_only where id between 1 and 50;
select count(*) from pax_delete_bf_only;

drop table pax_delete_bf_only;

-- Case 3: minmax and bloomfilter columns overlap but neither is a subset.
drop table if exists pax_delete_mm_bf_mixed;
create table pax_delete_mm_bf_mixed (id int, k int, payload text)
using pax
with (minmax_columns = 'id,payload', bloomfilter_columns = 'k,payload');

insert into pax_delete_mm_bf_mixed
select i, i % 7, 'p-' || i::text from generate_series(1, 5000) as i;

delete from pax_delete_mm_bf_mixed where id between 1 and 50;
select count(*) from pax_delete_mm_bf_mixed;

drop table pax_delete_mm_bf_mixed;
