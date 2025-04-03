set pax_enable_sparse_filter = on;
create table pax_test.null_test_t(a int, b int, c text) using pax;

insert into pax_test.null_test_t(a) select null from generate_series(1,2)i;
insert into pax_test.null_test_t select 1, i, 'cc_' || i from generate_series(1,2)i;

insert into pax_test.null_test_t select 1, i, null from generate_series(1,2)i;

select * from pax_test.null_test_t t where t.* is null;
select * from pax_test.null_test_t t where t.* is not null;

drop table pax_test.null_test_t;

-- start_matchignore
-- m/LOG:  statement:/
-- m/no filter/
-- m/No sparse filter/
-- end_matchignore

-- test cases for different column types: int, int8, bool, float, numeric, text, varchar, bpchar
create table pax_test.in_test_t(a int, b int8, c bool, d float, e numeric(20, 10), f text, g varchar(32), h char(32))
  using pax with(bloomfilter_columns='b,c,d,e,f,g,h');

insert into pax_test.in_test_t select 1,
case when i=1 then null else i end,
    't',
    i::float4,
    i::numeric(20, 10),
    't_' || i,
    'v_' || i,
    'bp_' || i
  from generate_series(1,10)i;

insert into pax_test.in_test_t select 1,
case when i=1 then null else i end,
    'f',
    i::float4,
    i::numeric(20, 10),
    't_' || i,
    'v_' || i,
    'bp_' || i
  from generate_series(20,30)i;

set client_min_messages = log;
explain select * from pax_test.in_test_t where b in (1, null);
select * from pax_test.in_test_t where b in (1, null);

explain select * from pax_test.in_test_t where b in (2, 3);
select * from pax_test.in_test_t where b in (2, 3);

explain select * from pax_test.in_test_t where c in ('t');
explain select * from pax_test.in_test_t where c in ('f');
select * from pax_test.in_test_t where c in ('t');
select * from pax_test.in_test_t where c in ('f');


explain select * from pax_test.in_test_t where d in (2.0, 4.0);
select * from pax_test.in_test_t where d in (2.0, 4.0);

explain select * from pax_test.in_test_t where e in (2.0, 4.0);
select * from pax_test.in_test_t where e in (2.0, 4.0);

explain select * from pax_test.in_test_t where f in ('t_2', 't_4');
select * from pax_test.in_test_t where f in ('t_2', 't_4');

explain select * from pax_test.in_test_t where g in ('v_2', 'v_4');
select * from pax_test.in_test_t where g in ('v_2', 'v_4');

explain select * from pax_test.in_test_t where h in ('bp_2', 'bp_4');
select * from pax_test.in_test_t where h in ('bp_2', 'bp_4');

reset client_min_messages;
drop table pax_test.in_test_t;

reset pax_enable_sparse_filter;
