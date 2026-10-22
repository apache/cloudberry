--
-- CREATE_FUNCTION_1
--

-- Create C functions needed by create_type.sql

CREATE FUNCTION widget_in(cstring)
   RETURNS widget
   AS '/home/gpadmin/gpdb/lib/postgresql/regress.so'
   LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION widget_out(widget)
   RETURNS cstring
   AS '/home/gpadmin/gpdb/lib/postgresql/regress.so'
   LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION int44in(cstring)
   RETURNS city_budget
   AS '/home/gpadmin/gpdb/lib/postgresql/regress.so'
   LANGUAGE C STRICT IMMUTABLE;

CREATE FUNCTION int44out(city_budget)
   RETURNS cstring
   AS '/home/gpadmin/gpdb/lib/postgresql/regress.so'
   LANGUAGE C STRICT IMMUTABLE;
