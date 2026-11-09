-- Create a function that will be added as an operator
CREATE FUNCTION my_pk_schema.is_zero(int) RETURNS TABLE(f1 boolean)
AS $$ select $1 = 0 $$  LANGUAGE SQL;

-- A prefix operator: PostgreSQL 14 removed postfix operators, so the LEFTARG
-- form this used to have now fails with "operator right argument type must be
-- specified ... Postfix operators are not supported" and leaves pg_operator
-- without the row the scenario goes on to delete.
CREATE OPERATOR my_pk_schema.!# (PROCEDURE = my_pk_schema.is_zero,RIGHTARG = integer);
