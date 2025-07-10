-- Test cases when a parallel transaction drops a dependency object
-- while current transaction is yet not committed.

-- Case 1. Function dependency on the schema.
create schema test_1_schema;

1: begin;
1: create function test_1_schema.test_1_function() returns text as $$
    select 'test'::text; /**/
$$ language sql;

-- Check that we didn't add extra locks. Here we lock only namespace, so the count is 1.
-- Do this check only for the first couple of tests in order not to overcomplicate the remaining.
1:  select count(1), l.gp_segment_id
from pg_locks l
join pg_locks r using (locktype, classid, objid)
where r.pid = pg_backend_pid() and r.locktype = 'object'
group by 2 order by 2;

2&: drop schema test_1_schema;

1: commit;

2<:

1: select test_1_schema.test_1_function();

drop schema test_1_schema cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create schema test_1_schema;
1: begin;
2: begin;
2: drop schema test_1_schema;
1&: create function test_1_schema.test_1_function() returns text as $$
    select 'test'::text; /**/
$$ language sql;

2: commit;
1<:
1: end;

-- Case 2. Function dependency on the return type.
create type test_2_type as (a int);

1: begin;
1: create function test_2_function() returns setof test_2_type as $$
    select i from generate_series(1,5)i; /**/
$$ language sql;

-- Check that we didn't add extra locks. Here we lock namespace ('public') and type, so the count is 2.
-- Do this check only for the first couple of tests in order not to overcomplicate the remaining.
1:  select count(1), l.gp_segment_id
from pg_locks l
join pg_locks r using (locktype, classid, objid)
where r.pid = pg_backend_pid() and r.locktype = 'object'
group by 2 order by 2;

2&: drop type test_2_type;

1: commit;

2<:

1: select test_2_function();

drop type test_2_type cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create type test_2_type as (a int);
1: begin;
2: begin;
2: drop type test_2_type;
1&: create function test_2_function() returns setof test_2_type as $$
    select i from generate_series(1,5)i; /**/
$$ language sql;

2: commit;
1<:
1: end;

-- Case 3. Function dependency on the parameter type.
create type test_3_type as enum ('one', 'two');

1: begin;
1: create function test_3_function(a test_3_type) returns text as $$
    select 'Return ' || a; /**/
$$ language sql;

2&: drop type test_3_type;

1: commit;

2<:

1: select test_3_function('one');

drop type test_3_type cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create type test_3_type as enum ('one', 'two');
1: begin;
2: begin;
2: drop type test_3_type;
1&: create function test_3_function(a test_3_type) returns text as $$
    select 'Return ' || a; /**/
$$ language sql;

2: commit;
1<:
1: end;

-- Case 4. Function dependency on the language.
-- start_ignore
drop language if exists plpythonu cascade;
-- end_ignore
create language plpythonu;

1: begin;
1: create function test_4_function() returns text as $$
	return "test"
$$ language plpythonu;

2&: drop language plpythonu;

1: commit;

2<:

1: select test_4_function();

drop language plpythonu cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create language plpythonu;

1: begin;
2: begin;
2: drop language plpythonu;
1&: create function test_4_function() returns text as $$
	return "test"
$$ language plpythonu;

2: commit;
1<:
1: end;

-- Case 5. Function dependency on the parameter default expression.
create function test5_default_value_function() returns text as $$
    select 'test'::text; /**/
$$ language sql;

1: begin;
1: create function test_5_function(a text default test5_default_value_function()) returns text as
$$
begin
	return a; /**/
end
$$ language plpgsql;

2&: drop function test5_default_value_function();

1: commit;

2<:

1: select test_5_function();

drop function test5_default_value_function() cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create function test5_default_value_function() returns text as $$
    select 'test'::text; /**/
$$ language sql;

1: begin;
2: begin;
2: drop function test5_default_value_function();
1&: create function test_5_function(a text default test5_default_value_function()) returns text as
$$
begin
	return a; /**/
end
$$ language plpgsql;

2: commit;
1<:
1: end;

-- Case 6. Table dependency on the column default expression.
create function test_6_default_value_function() returns text as $$
    select 'test'::text; /**/
$$ language sql;

1: begin;
1: create table test_6_table(a text default test_6_default_value_function());

2&: drop function test_6_default_value_function();

1: commit;

2<:

1: insert into test_6_table default values;
1: select * from test_6_table;

drop function test_6_default_value_function() cascade;
drop table test_6_table;

-- Check if dependency is dropped before the creation of the dependent object.
create function test_6_default_value_function() returns text as $$
    select 'test'::text; /**/
$$ language sql;

1: begin;
2: begin;
2: drop function test_6_default_value_function();
1&: create table test_6_table(a text default test_6_default_value_function());

2: commit;
1<:
1: end;

-- Case 7. Table dependency on the column type.
create type test_7_type as enum ('one', 'two');

1: begin;
1: create table test_7_table(a test_7_type);

2&: drop type test_7_type;

1: commit;

2<:

1: select * from test_7_table;

drop type test_7_type cascade;
drop table test_7_table;

-- Check if dependency is dropped before the creation of the dependent object.
create type test_7_type as enum ('one', 'two');

1: begin;
2: begin;
2: drop type test_7_type;
1&: create table test_7_table(a test_7_type);

2: commit;
1<:
1: end;

-- Case 8. Table dependency on the collation.
create collation test_8_collation (locale="en_US.utf8");

1: begin;
1: create table test_8_table(a text collate test_8_collation);
1: insert into test_8_table values('data');

2&: drop collation test_8_collation;

1: commit;

2<:

1: select * from test_8_table where a < 'test';

drop collation test_8_collation cascade;
drop table test_8_table;

-- Check if dependency is dropped before the creation of the dependent object.
create collation test_8_collation (locale="en_US.utf8");

1: begin;
2: begin;
2: drop collation test_8_collation;
1&: create table test_8_table(a text collate test_8_collation);

2: commit;
1<:
1: end;

-- Case 9. Text search configuration dependency on the parser.
create text search parser test_9_parser(start = prsd_start, gettoken = prsd_nexttoken, end = prsd_end, lextypes = prsd_lextype);

1: begin;
1: create text search configuration test_9_configuration(parser = test_9_parser);

2&: drop text search parser test_9_parser;

1: commit;

2<:

1: select count(1) from ts_debug('public.test_9_configuration', 'test');

drop text search parser test_9_parser cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create text search parser test_9_parser(start = prsd_start, gettoken = prsd_nexttoken, end = prsd_end, lextypes = prsd_lextype);

1: begin;
2: begin;
2: drop text search parser test_9_parser;
1&: create text search configuration test_9_configuration(parser = test_9_parser);

2: commit;
1<:
1: end;

-- Case 10. Text search dictionary dependency on the template.
create text search template test_10_template(init = dsimple_init, lexize = dsimple_lexize);

1: begin;
1: create text search dictionary test_10_dictionary(template = test_10_template);

2&: drop text search template test_10_template;

1: commit;

2<:

1: select ts_lexize('public.test_10_dictionary', 'test');

drop text search template test_10_template cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create text search template test_10_template(init = dsimple_init, lexize = dsimple_lexize);

1: begin;
2: begin;
2: drop text search template test_10_template;
1&: create text search dictionary test_10_dictionary(template = test_10_template);

2: commit;
1<:
1: end;

-- Case 11. Server dependency on the foreign data wrapper.
create foreign data wrapper test_11_fdw;

1: begin;
1: create server test_11_server foreign data wrapper test_11_fdw;

2&: drop foreign data wrapper test_11_fdw;

1: commit;

2<:

1: alter server test_11_server options (servername 'test_server');

drop foreign data wrapper test_11_fdw cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create foreign data wrapper test_11_fdw;

1: begin;
2: begin;
2: drop foreign data wrapper test_11_fdw;
1&: create server test_11_server foreign data wrapper test_11_fdw;

2: commit;
1<:
1: end;

-- Case 12. User mapping dependency on the server.
create foreign data wrapper test_12_fdw;
create server test_12_server foreign data wrapper test_12_fdw;

1: begin;
1: create user mapping for public server test_12_server;

2&: drop server test_12_server;

1: commit;

2<:

SELECT srvname, usename FROM pg_user_mappings ORDER BY 1, 2;

drop server test_12_server cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create server test_12_server foreign data wrapper test_12_fdw;

1: begin;
2: begin;
2: drop server test_12_server;
1&: create user mapping for public server test_12_server;

2: commit;
1<:
1: end;

drop foreign data wrapper test_12_fdw;

-- Case 13. External table dependency on the protocol.
create or replace function write_to_file() returns integer as '$libdir/gpextprotocol.so', 'demoprot_export' language c stable no sql;
create or replace function read_from_file() returns integer as '$libdir/gpextprotocol.so', 'demoprot_import' language c stable no sql;

create protocol demoprot (readfunc = 'read_from_file', writefunc = 'write_to_file');
! echo 1 > /tmp/test_13.txt;

1: begin;
1: create readable external table test_13_ext_table(a int) location('demoprot:///tmp/test_13.txt') format 'text';

2&: drop protocol demoprot;

1: commit;

2<:

1: select * from test_13_ext_table;

! rm /tmp/test_13.txt;

drop protocol demoprot cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create protocol demoprot (readfunc = 'read_from_file', writefunc = 'write_to_file');

1: begin;
2: begin;
2: drop protocol demoprot;
1&: create readable external table test_13_ext_table(a int) location('demoprot://test.txt') format 'text';

2: commit;
1<:
1: end;

drop function write_to_file();
drop function read_from_file();

-- Case 14. Text search configuration dependency on the text search dictionary
create text search dictionary test_14_dict ( template = simple );

1: begin;
1: create text search configuration test_14_config (parser = default);
1: alter text search configuration test_14_config alter mapping for asciiword with test_14_dict;

2&: drop text search dictionary test_14_dict;

1: commit;

2<:

1: select count(1) from ts_debug('public.test_14_config', 'test');

drop text search dictionary test_14_dict cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create text search dictionary test_14_dict ( template = simple );

1: begin;
1: create text search configuration test_14_config (parser = default);
2: begin;
2: drop text search dictionary test_14_dict;
1&: alter text search configuration test_14_config alter mapping for asciiword with test_14_dict;

2: commit;
1<:
1: end;

-- Case 15. Table dependency on the operator.
create function test_15_function(text, text) returns text as $$
begin
	return $1 || $2;  /**/
end;  /**/
$$ language plpgsql;

create operator ~*~ (
    procedure = test_15_function,
    leftarg = text,
    rightarg = text,
    commutator = ~*~
);

1: begin;
1: create table test_15_table(a text default 'a' ~*~ 'b');

2&: drop operator ~*~ (text, text);

1: commit;

2<:

1: insert into test_15_table values (default);

drop operator ~*~ (text, text) cascade;
drop table test_15_table;

-- Check if dependency is dropped before the creation of the dependent object.
create operator ~*~ (
    procedure = test_15_function,
    leftarg = text,
    rightarg = text,
    commutator = ~*~
);

1: begin;
2: begin;
2: drop operator ~*~ (text, text);
1&: create table test_15_table(a text default 'a' ~*~ 'b');

2: commit;
1<:
1: end;

drop function test_15_function(text, text);

-- Case 16. Index dependency on the operator class.
create function test_16_idx_func(int, int) returns int as $$
begin
    return $1 - $2;  /**/
end;  /**/
$$ language plpgsql immutable;

create operator class test_16_op_class for type int using btree as
    operator 1 =(int, int),
    function 1 test_16_idx_func(int, int);

create table test_16_table(a int);

1: begin;
1: create index idx_test_16_table on test_16_table using btree (a test_16_op_class);

2&: drop operator class test_16_op_class using btree;

1: commit;

2<:

1: select * from test_16_table where a = 0;

drop operator class test_16_op_class using btree cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create operator class test_16_op_class for type int using btree as
    operator 1 =(int, int),
    function 1 test_16_idx_func(int, int);

1: begin;
2: begin;
2: drop operator class test_16_op_class using btree;
1&: create index idx_test_16_table on test_16_table using btree (a test_16_op_class);

2: commit;
1<:
1: end;

drop function test_16_idx_func(int, int);
drop table test_16_table;

-- Case 17. Operator class dependency on the operator family.
create function test_17_eq(int, int) returns int as $$
begin
    return 1;  /**/
end;  /**/
$$ language plpgsql immutable;

create operator family test_17_op_family using btree;

1: begin;
1: create operator class test_17_op_class for type int using btree family test_17_op_family as
    operator 1 =(int, int),
    function 1 test_17_eq(int, int);

2&: drop operator family test_17_op_family using btree;

1: commit;

2<:

-- Count below should be 0, as 'drop operator family' automatically drops
-- all its operator classes.
1: select count(*) from pg_opclass where opcname = 'test_17_op_class';

-- Check if dependency is dropped before the creation of the dependent object.
create operator family test_17_op_family using btree;

1: begin;
2: begin;
2: drop operator family test_17_op_family using btree;
1&: create operator class test_17_op_class for type int using btree family test_17_op_family as
    operator 1 =(int, int),
    function 1 test_17_eq(int, int);

2: commit;
1<:
1: end;

drop function test_17_eq(int, int);

-- Case 18. View dependency on the function.
create function test_18_function() returns text as $$
    select 'test'::text;  /**/
$$ language sql;

1: begin;
1: create view test_18_view as select test_18_function();

2&: drop function test_18_function();

1: commit;

2<:

1: select * from test_18_view;

drop function test_18_function() cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create function test_18_function() returns text as $$
    select 'test'::text;  /**/
$$ language sql;

1: begin;
2: begin;
2: drop function test_18_function();
1&: create view test_18_view as select test_18_function();

2: commit;
1<:
1: end;

-- Case 19. Materialized view dependency on the function.
create function test_19_function() returns text as $$
    select 'test'::text;  /**/
$$ language sql;

1: begin;
1: create materialized view test_19_view as select test_19_function();

2&: drop function test_19_function();

1: commit;

2<:

1: select * from test_19_view;

drop function test_19_function() cascade;

-- Check if dependency is dropped before the creation of the dependent object.
create function test_19_function() returns text as $$
    select 'test'::text;  /**/
$$ language sql;

1: begin;
2: begin;
2: drop function test_19_function();
1&: create materialized view test_19_view as select test_19_function();

2: commit;
1<:
1: end;

-- Case 20. Table dependency on the column type (with ALTER COLUMN).
create type test_20_type as enum ('one', 'two');
create table test_20_table(a text);

1: begin;
1: alter table test_20_table alter column a set data type test_20_type using a::test_20_type;

2&: drop type test_20_type;

1: commit;

2<:

1: select * from test_20_table;

drop type test_20_type cascade;
drop table test_20_table;

-- Check if dependency is dropped before the creation of the dependent object.
create type test_20_type as enum ('one', 'two');
create table test_20_table(a text);

1: begin;
2: begin;
2: drop type test_20_type;
1&: alter table test_20_table alter column a set data type test_20_type using a::test_20_type;

2: commit;
1<:
1: end;

-- Case 21. Rule dependency on the table.
create table test_21_table_1(a int);
create table test_21_table_2(a int);

1: begin;
1: create rule test_21_rule as on insert to test_21_table_1 do instead insert into test_21_table_2 values (1);

2&: drop table test_21_table_2;

1: commit;

2<:

1: insert into test_21_table_1 values (0);
1: select * from test_21_table_2;

drop rule test_21_rule on test_21_table_1;

-- Check if dependency is dropped before the creation of the dependent object.
1: begin;
2: begin;
2: drop table test_21_table_2;
1&: create rule test_21_rule as on insert to test_21_table_1 do instead insert into test_21_table_2 values (1);

2: commit;
1<:
1: end;

drop table test_21_table_1;

-- Case 22. Table dependency on the sequence.
create sequence test_22_seq;

1: begin;
1: create table test_22_table(id int default nextval('test_22_seq'));

2&: drop sequence test_22_seq;

1: commit;

2<:

1: insert into test_22_table default values;

drop sequence test_22_seq cascade;
drop table test_22_table;

-- Check if dependency is dropped before the creation of the dependent object.
create sequence test_22_seq;

1: begin;
2: begin;
2: drop sequence test_22_seq;
1&: create table test_22_table(id int default nextval('test_22_seq'));

2: commit;
1<:
1: end;

-- Case 23. Table dependency on the column type with REPEATABLE READ isolation level.
create type test_23_type as enum ('one', 'two');

1: begin;
1: set transaction isolation level repeatable read;
1: create table test_23_table(a test_23_type);

2&: drop type test_23_type;

1: commit;

2<:

1: select * from test_23_table;

drop type test_23_type cascade;
drop table test_23_table;

-- Check if dependency is dropped before the creation of the dependent object.
create type test_23_type as enum ('one', 'two');

1: begin;
1: set transaction isolation level repeatable read;
2: begin;
2: set transaction isolation level repeatable read;
2: drop type test_23_type;
1&: create table test_23_table(a test_23_type);

2: commit;
1<:
1: end;

-- Test deadlock scenario. It should be resolved by the deadlock detection algorithm.
create schema test_schema;
create type test_type as enum ('one', 'two');

1: begin;
2: begin;

2: drop type test_type;
1&: create function test_schema.test_function(a test_type) returns text as $$
    select 'Return ' || a;  /**/
$$ language sql;
2&: drop schema test_schema;

-- start_ignore
1<:
2<:
-- end_ignore

1: rollback;
2: rollback;

drop schema test_schema;
drop type test_type;
