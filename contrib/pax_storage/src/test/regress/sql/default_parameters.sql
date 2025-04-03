-- -----------------------------------------------------------------
-- Test function default parameters
--
-- PostgreSQL's 'polymorphism' test mostly covers default
-- parameters. This file contains a few extra tests.
-- -----------------------------------------------------------------

-- inout argmode test:
create function dfunc(a inout int default 10, b int = 2) as $$ select $1 + $2; $$ language sql;
select dfunc();
select dfunc(4);
select dfunc(4,5);

\df dfunc
drop function dfunc(int, int);

-- ambiguity should be reported only if there's not a better match available
create function dfunc(text) returns text as $$ select $1; $$ language sql;
create function dfunc(int = 1, int = 2) returns int as $$ select 2; $$ language sql;
create function dfunc(int = 1, int = 2, int = 3, int = 4) returns int as $$ select 4; $$ language sql;

-- now, dfunc(nargs = 2) and dfunc(nargs = 4) are ambiguous when called
-- with 0 to 2 arguments with input type int
select dfunc('Hi');	-- ok, input type text
select dfunc();  -- fail
select dfunc(1);  -- fail
select dfunc(1, 2);  -- fail
select dfunc(1, 2, 3);  -- ok
select dfunc(1, 2, 3, 4);  -- ok
drop function dfunc(text);
drop function dfunc(int, int);
drop function dfunc(int, int, int, int);

-- check defaults for variadics
create function dfunc(a variadic int[]) returns int as
$$ select array_upper($1, 1) $$ language sql;
select dfunc();  -- fail
select dfunc(10);
select dfunc(10,20);

create or replace function dfunc(a variadic int[] default '{44,88,99}'::int[]) returns int as $$ select array_upper($1, 1) $$ language sql;
select dfunc();  -- now ok
select dfunc(10);
select dfunc(10,20);

-- can change default value
create or replace function dfunc(a variadic int[] default '{44,55,88,99}'::int[]) returns int as $$ select array_upper($1, 1) $$ language sql;
select dfunc();

drop function dfunc(a variadic int[]);

-- test expression
create function dfunc(a int, b double precision default random()*10) returns double precision as $$ select $1+$2; $$ language sql;
\df dfunc

create view dview as select random()*10, dfunc(4);
select pg_get_viewdef('dview',true);

-- need drop cascade
drop function dfunc(int, double precision);
drop function dfunc(int, double precision) cascade;
