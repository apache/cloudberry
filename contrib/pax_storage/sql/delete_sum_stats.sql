-- Verify that DELETE statistics refresh uses the SUM result type when
-- deserializing and merging stats. In particular, sum(bigint) returns numeric.
set pax.max_tuples_per_group = 25;

create table pax_delete_sum_stats (
  dist_key int,
  id bigint,
  int_amount int,
  bigint_amount bigint,
  numeric_amount numeric
)
using pax
with (minmax_columns = 'int_amount,bigint_amount,numeric_amount')
distributed by (dist_key);

insert into pax_delete_sum_stats
select 1, i, i, i, i::numeric * 2
from generate_series(1, 1000) i;

select count(*) as rows,
       sum(int_amount) as int_sum,
       sum(bigint_amount) as bigint_sum,
       sum(numeric_amount) as numeric_sum
from pax_delete_sum_stats;

delete from pax_delete_sum_stats where id between 1 and 100;

select count(*) as rows,
       sum(int_amount) as int_sum,
       sum(bigint_amount) as bigint_sum,
       sum(numeric_amount) as numeric_sum
from pax_delete_sum_stats;

insert into pax_delete_sum_stats
select 1, i, i, i, i::numeric * 2
from generate_series(1001, 1100) i;

delete from pax_delete_sum_stats where id between 301 and 400;

select count(*) as rows,
       sum(int_amount) as int_sum,
       sum(bigint_amount) as bigint_sum,
       sum(numeric_amount) as numeric_sum
from pax_delete_sum_stats;

drop table pax_delete_sum_stats;
reset pax.max_tuples_per_group;
