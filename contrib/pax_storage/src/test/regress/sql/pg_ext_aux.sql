-- Test use of namespace pg_ext_aux

-- Test 1: Create database object in pg_ext_aux should fail normally
create table pg_ext_aux.extaux_t(a int, b int);
create view pg_ext_aux.extaux_v as select oid, relname from pg_class;
create materialized view pg_ext_aux.extaux_mv as select oid, relname from pg_class;

-- shell type is created successfully
create type pg_ext_aux.extaux_shell_type;
-- concreate type will fail
create type pg_ext_aux.extaux_concreat_type AS (sum int, count int);

-- create function will success
create function pg_ext_aux.extaux_add1(i integer) returns integer AS $$
begin
  return i + 1;
end;
$$ language plpgsql;


-- Test 2: Create database object in pg_ext_aux should success
-- if allow_system_table_mods is on
set allow_system_table_mods = on;
create table pg_ext_aux.extaux_t(a int, b text);
create view pg_ext_aux.extaux_v as select * from pg_ext_aux.extaux_t;
create materialized view pg_ext_aux.extaux_mv as select * from pg_ext_aux.extaux_t;
create type pg_ext_aux.extaux_concreat_type AS (sum int, count int);
set allow_system_table_mods = off;

-- Test 3: Check that the array type is not created
-- these relations have its reltype, but have no array type
select typname, typarray from pg_type where oid = (select reltype from pg_class where relname='extaux_t');
select typname, typarray from pg_type where oid = (select reltype from pg_class where relname='extaux_v');
select typname, typarray from pg_type where oid = (select reltype from pg_class where relname='extaux_mv');

-- Test 4: check function works
select pg_ext_aux.extaux_add1(7);

-- Test 5: Check that these relations are protected
-- fail: should not allowed to insert by user
insert into pg_ext_aux.extaux_t values(1,'hello');
--- success: The pax table does not create toast table, different from the heap table.
refresh materialized view pg_ext_aux.extaux_mv;

-- fail: should not allow to be dropped by user
drop view pg_ext_aux.extaux_v;
drop materialized view pg_ext_aux.extaux_mv;
drop table pg_ext_aux.extaux_t;

-- Test 6: type and functions may be dropped normally
drop type pg_ext_aux.extaux_shell_type;
drop type pg_ext_aux.extaux_concreat_type;
drop function pg_ext_aux.extaux_add1(i integer);


-- Clean up
set allow_system_table_mods = on;
drop view pg_ext_aux.extaux_v;
drop materialized view pg_ext_aux.extaux_mv;
drop table pg_ext_aux.extaux_t;

reset allow_system_table_mods;
