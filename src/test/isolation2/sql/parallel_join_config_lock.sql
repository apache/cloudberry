-- Planning a parallel DEDUP_SEMI query must not block catalog readers, even
-- if the transaction stays open after planning.
drop table if exists parallel_join_lock_foo;
drop table if exists parallel_join_lock_bar;

create table parallel_join_lock_foo(a int) with (parallel_workers=2) distributed randomly;
create table parallel_join_lock_bar(b int) with (parallel_workers=2) distributed randomly;
insert into parallel_join_lock_foo select i from generate_series(1, 1000) i;
insert into parallel_join_lock_bar select i from generate_series(1, 20000) i;
analyze parallel_join_lock_foo;
analyze parallel_join_lock_bar;

1: begin;
1: set local optimizer = off;
1: set local enable_parallel = on;
1: set local enable_parallel_semi_join = off;
1: set local enable_parallel_dedup_semi_reverse_join = off;
1: set local enable_parallel_dedup_semi_join = on;
1: select sum(foo.a) from parallel_join_lock_foo foo where exists (select 1 from parallel_join_lock_bar bar where foo.a = bar.b);

2: set lock_timeout = '1s';
2: select 1 from pg_catalog.gp_segment_configuration limit 1;

1: rollback;
drop table parallel_join_lock_foo;
drop table parallel_join_lock_bar;
