do $$
declare
  c int;
  strval varchar;
  intval int;
  nrows int default 30;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'select ''ahoj'' || i, i from generate_series(1, :nrows) g(i)');
  call dbms_sql.bind_variable(c, 'nrows', nrows);
  call dbms_sql.define_column(c, 1, strval);
  call dbms_sql.define_column(c, 2, intval);
  perform dbms_sql.execute(c);
  while dbms_sql.fetch_rows(c) > 0
  loop
    call dbms_sql.column_value(c, 1, strval);
    call dbms_sql.column_value(c, 2, intval);
    raise notice 'c1: %, c2: %', strval, intval;
  end loop;
  call dbms_sql.close_cursor(c);
end;
$$;

do $$
declare
  c int;
  strval varchar;
  intval int;
  nrows int default 30;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'select ''ahoj'' || i, i from generate_series(1, :nrows) g(i)');
  call dbms_sql.bind_variable(c, 'nrows', nrows);
  call dbms_sql.define_column(c, 1, strval);
  call dbms_sql.define_column(c, 2, intval);
  perform dbms_sql.execute(c);
  while dbms_sql.fetch_rows(c) > 0
  loop
    strval := dbms_sql.column_value_f(c, 1, strval);
    intval := dbms_sql.column_value_f(c, 2, intval);
    raise notice 'c1: %, c2: %', strval, intval;
  end loop;
  call dbms_sql.close_cursor(c);
end;
$$;

drop table if exists foo;

create table foo(a int, b varchar, c numeric);

do $$
declare c int;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'insert into foo values(:a, :b, :c)');
  for i in 1..100
  loop
    call dbms_sql.bind_variable(c, 'a', i);
    call dbms_sql.bind_variable(c, 'b', 'Ahoj ' || i);
    call dbms_sql.bind_variable(c, 'c', i + 0.033);
    perform dbms_sql.execute(c);
  end loop;
end;
$$;

select * from foo;
truncate foo;

do $$
declare c int;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'insert into foo values(:a, :b, :c)');
  for i in 1..100
  loop
    perform dbms_sql.bind_variable_f(c, 'a', i);
    perform dbms_sql.bind_variable_f(c, 'b', 'Ahoj ' || i);
    perform dbms_sql.bind_variable_f(c, 'c', i + 0.033);
    perform dbms_sql.execute(c);
  end loop;
end;
$$;

select * from foo;
truncate foo;

do $$
declare
  c int;
  a int[];
  b varchar[];
  ca numeric[];
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'insert into foo values(:a, :b, :c)');
  a := ARRAY[1, 2, 3, 4, 5];
  b := ARRAY['Ahoj', 'Nazdar', 'Bazar'];
  ca := ARRAY[3.14, 2.22, 3.8, 4];

  call dbms_sql.bind_array(c, 'a', a);
  call dbms_sql.bind_array(c, 'b', b);
  call dbms_sql.bind_array(c, 'c', ca);
  raise notice 'inserted rows %d', dbms_sql.execute(c);
end;
$$;

select * from foo;
truncate foo;

-- should not to crash, when bound array is null
do $$
declare
  c int;
  ca numeric[];
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'insert into foo values(:a, 10, 20)');

  call dbms_sql.bind_array(c, 'a', ca);
  raise notice 'inserted rows %d', dbms_sql.execute(c);
end;
$$;

-- should not to crash, when we try to touch result without execute
do $$
declare
  c int;
  a int[];
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'select i from generate_series(1, 2) g(i)');
  call dbms_sql.define_array(c, 1, a, 10, 1);
  call dbms_sql.column_value(c, 1, a);
  call dbms_sql.close_cursor(c);
end;
$$;

-- should not to crash, when the variable is overwritten
DO $$
declare
  c integer;
  n integer;
  c2 numeric;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'INSERT INTO foo(a) VALUES (:bnd2)');
  call dbms_sql.bind_variable(c, 'bnd2', c2);
  call dbms_sql.bind_variable(c, 'bnd2', c2);
  n := dbms_sql.execute(c);
end
$$;

-- should not to crash, when we try to read column without data
do $$
declare
  c int;
  strval varchar;
  intval int;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'select ''foo'', 1');
  call dbms_sql.define_column(c, 1, strval);
  call dbms_sql.define_column(c, 2, intval);
  perform dbms_sql.execute(c);
  while dbms_sql.fetch_rows(c) > -1
  loop
    call dbms_sql.column_value(c, 1, strval);
  end loop;
  call dbms_sql.close_cursor(c);
end;
$$;

select * from foo;
truncate foo;
do $$
declare
  c int;
  a int[];
  b varchar[];
  ca numeric[];
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'insert into foo values(:a, :b, :c)');
  a := ARRAY[1, 2, 3, 4, 5];
  b := ARRAY['Ahoj', 'Nazdar', 'Bazar'];
  ca := ARRAY[3.14, 2.22, 3.8, 4];

  call dbms_sql.bind_array(c, 'a', a, 2, 3);
  call dbms_sql.bind_array(c, 'b', b, 3, 4);
  call dbms_sql.bind_array(c, 'c', ca);
  raise notice 'inserted rows %d', dbms_sql.execute(c);
end;
$$;

select * from foo;
truncate foo;

do $$
declare
  c int;
  a int[];
  b varchar[];
  ca numeric[];
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'select i, ''Ahoj'' || i, i + 0.003 from generate_series(1, 35) g(i)');
  call dbms_sql.define_array(c, 1, a, 10, 1);
  call dbms_sql.define_array(c, 2, b, 10, 1);
  call dbms_sql.define_array(c, 3, ca, 10, 1);

  perform dbms_sql.execute(c);
  while dbms_sql.fetch_rows(c) > 0
  loop
    call dbms_sql.column_value(c, 1, a);
    call dbms_sql.column_value(c, 2, b);
    call dbms_sql.column_value(c, 3, ca);
    raise notice 'a = %', a;
    raise notice 'b = %', b;
    raise notice 'c = %', ca;
  end loop;
  call dbms_sql.close_cursor(c);
end;
$$;

drop table foo;

create table tab1(c1 integer,  c2 numeric);

create or replace procedure single_Row_insert(c1 integer, c2 numeric)
as $$
declare
  c integer;
  n integer;
begin
  c := dbms_sql.open_cursor();

  call dbms_sql.parse(c, 'INSERT INTO tab1 VALUES (:bnd1, :bnd2)');

  call dbms_sql.bind_variable(c, 'bnd1', c1);
  call dbms_sql.bind_variable(c, 'bnd2', c2);

  n := dbms_sql.execute(c);

  call dbms_sql.debug_cursor(c);
  call dbms_sql.close_cursor(c);
end
$$language plpgsql;

do $$
declare a numeric(7,2);
begin
  call single_Row_insert(2,a);
end
$$;

select * from tab1;

do $$
declare a numeric(7,2) default 1.23;
begin
  call single_Row_insert(2,a);
end
$$;

select * from tab1;
select * from tab1 where c2 is null;


do $$
declare a numeric(7,2);
begin
  call single_Row_insert(0,a);   -- single_Row_insert(0, null)
end
$$;

select * from tab1;

do $$
declare a numeric(7,2) default 1.23;
begin
  call single_Row_insert(0,a);  -- single_Row_insert(0, 1.23)
end
$$;

select * from tab1;
drop procedure single_Row_insert;
drop table tab1;

create table test(id text);
insert into test(id) values ('1'), (null);

-- should not to crash
do $$
declare
  cursor int;
  id text;
  row_counter int := 0;
begin
  cursor := dbms_sql.open_cursor();
  call dbms_sql.parse(cursor, 'select id from test');
  call dbms_sql.define_column(cursor, 1, 'id');
  perform dbms_sql.execute(cursor);
  while dbms_sql.fetch_rows(cursor) > 0 loop
    row_counter = row_counter + 1;
    raise notice 'process row #%', row_counter;
    call dbms_sql.column_value(cursor, 1, id);
    raise notice 'row id: `%`', id;
  end loop;
  call dbms_sql.close_cursor(cursor);
end;
$$;

-- should not to crash
do $$
declare
  c int;
  n int;
  d dbms_sql.desc_rec[];
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'select * from test');
  call dbms_sql.describe_columns(c, n, d);
  raise notice '% %', n, d;
  call dbms_sql.close_cursor(c);
  -- should not crash
  call dbms_sql.parse(c, 'create temp table t(a int)');
  call dbms_sql.describe_columns(c, n, d);
  call dbms_sql.close_cursor(c);
end;
$$;

drop table test;

do $$
declare
  c int;
  v text;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'select null::text');
  call dbms_sql.define_column(c, 1, null::text);
  perform dbms_sql.execute_and_fetch(c, false);

  -- crashes here before the fix
  v := dbms_sql.column_value_f(c, 1, null::text);

  raise notice 'v is null = %', v is null;
  call dbms_sql.close_cursor(c);
end
$$;

-- the same defect is reachable through the procedure form of column_value().
do $$
declare
  c int;
  v text;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'select null::text');
  call dbms_sql.define_column(c, 1, null::text);
  perform dbms_sql.execute_and_fetch(c, false);

  call dbms_sql.column_value(c, 1, v);

  raise notice 'v is null = %', v is null;
  call dbms_sql.close_cursor(c);
end
$$;

-- a cursor has to be executable repeatedly.  the portal opened by the
-- previous execution was never closed, so the second execution failed with
-- 'cursor "__orafce_dbms_sql_cursor_0" already exists'.
do $$
declare c int; v int := 0;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'select 42');
  call dbms_sql.define_column(c, 1, v);
  perform dbms_sql.execute_and_fetch(c);
  perform dbms_sql.execute_and_fetch(c);
  call dbms_sql.column_value(c, 1, v);
  call dbms_sql.close_cursor(c);
  raise notice 'v = %', v; -- expected 42
end
$$;

-- the cast cache checked the requested array type only while the entry was
-- built, so a second column_value() call for the same position handed back the
-- value of the first type labelled with the second one.
do $$
declare c int; ia int[]; ta text[]; mismatch boolean := false;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'select i from generate_series(1,3) g(i)');
  call dbms_sql.define_array(c, 1, ia, 3, 1);
  perform dbms_sql.execute(c);
  perform dbms_sql.fetch_rows(c);
  call dbms_sql.column_value(c, 1, ia);
  begin
    call dbms_sql.column_value(c, 1, ta);
  exception when datatype_mismatch then
    mismatch := true;
  end;
  call dbms_sql.close_cursor(c);
  raise notice 'ia = %', ia; -- expected [1,2,3]
  raise notice 'mismatch = %', mismatch; -- expected true
end
$$;


-- execute() registered the reset callback of the transaction memory
-- context only when the context was created, but such callbacks fire at most
-- once.  a cursor executed twice inside one transaction therefore kept a
-- pointer to a context that the end of the transaction had already freed, and
-- the next execution reset that freed memory.
create temp table c31_rows(a int);

do $$
declare c int;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'insert into c31_rows values(10)');
  raise notice '%', dbms_sql.execute(c);
  raise notice '%', dbms_sql.execute(c);
  raise notice '%', dbms_sql.execute(c);
  commit;
  raise notice '%', dbms_sql.execute(c);
end;
$$;


select dbms_sql.open_cursor() as cur \gset c31_

call dbms_sql.parse(:c31_cur, 'insert into c31_rows values(1)');
begin;
select dbms_sql.execute(:c31_cur) as r \gset c31_first_
select dbms_sql.execute(:c31_cur) as r \gset c31_second_
commit;
select dbms_sql.execute(:c31_cur) as r \gset c31_third_
call dbms_sql.close_cursor(:c31_cur);
do $$
begin
  raise notice 'result = %', (select count(*) = 3 from c31_rows); -- true
end
$$;

drop table c31_rows;

create temp table test_cursor(a int);

-- any cursor can be reused
do $$
declare c int;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'insert into test_cursor values(10)');
  raise notice '%', dbms_sql.execute(c);
  call dbms_sql.parse(c, 'update test_cursor set a = 30');
  raise notice '%', dbms_sql.execute(c);
  call dbms_sql.close_cursor(c);
end;
$$;

select * from test_cursor;

drop table test_cursor;

create function public.close_during_convert()
returns boolean
language plpgsql
as $$
begin
  if current_setting('audit.arm', true) = 'on' then
    perform set_config('audit.arm', 'off', false);
    call dbms_sql.close_cursor(current_setting('audit.cursor')::int);
  end if;
  return true;
end $$;

create domain public.d_big as text check (public.close_during_convert());

do $$
declare c integer; big public.d_big; result text;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'select repeat(''x'', 300000)::text');
  call dbms_sql.define_column(c, 1, big);
  perform dbms_sql.execute(c);
  perform dbms_sql.fetch_rows(c);
  perform set_config('audit.cursor', c::text, false);
  perform set_config('audit.arm', 'on', false);
  result := dbms_sql.column_value_f(c, 1, big); -- closes cursor mid-call, then faults
end $$;

drop function public.close_during_convert cascade;

create temp table t(v int);

do $$
declare c int;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'insert into t(v) values (:value)');
  call dbms_sql.bind_array(c, 'value', array[1,2], 1, 2); -- is_array = true
  perform dbms_sql.execute(c);
  call dbms_sql.bind_variable(c, 'value', 42); -- scalar rebind; is_array stays true
  perform dbms_sql.execute(c);-- datumgetarraytypep(42) -> deref
end $$;

drop table t;

begin;

create temp table audit_row(a integer, b text);

do $$
  declare c integer; a text; b text;
begin
  c := dbms_sql.open_cursor();
  call dbms_sql.parse(c, 'select row(1, ''marker'')::pg_temp.audit_row');
  call dbms_sql.define_column(c, 1, null::text);
  perform dbms_sql.execute(c);
  perform dbms_sql.fetch_rows(c);
  a := dbms_sql.column_value_f(c, 1, null::text); -- builds the cast cache
  b := dbms_sql.column_value_f(c, 1, null::text); -- reuses it after spi_finish freed fn_mcxt
  call dbms_sql.close_cursor(c);
end $$;

rollback;
