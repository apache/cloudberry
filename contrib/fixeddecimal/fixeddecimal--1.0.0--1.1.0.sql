CREATE FUNCTION fixeddecimal_numeric_cmp(FIXEDDECIMAL, NUMERIC)
RETURNS INT4
AS 'fixeddecimal', 'fixeddecimal_numeric_cmp'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION numeric_fixeddecimal_cmp(NUMERIC, FIXEDDECIMAL)
RETURNS INT4
AS 'fixeddecimal', 'numeric_fixeddecimal_cmp'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_numeric_eq(FIXEDDECIMAL, NUMERIC)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_numeric_eq'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_numeric_ne(FIXEDDECIMAL, NUMERIC)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_numeric_ne'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_numeric_lt(FIXEDDECIMAL, NUMERIC)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_numeric_lt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_numeric_le(FIXEDDECIMAL, NUMERIC)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_numeric_le'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_numeric_gt(FIXEDDECIMAL, NUMERIC)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_numeric_gt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_numeric_ge(FIXEDDECIMAL, NUMERIC)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_numeric_ge'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR = (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = NUMERIC,
    COMMUTATOR = =,
    NEGATOR    = <>,
    PROCEDURE  = fixeddecimal_numeric_eq,
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel,
    MERGES
);

CREATE OPERATOR <> (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = NUMERIC,
    NEGATOR    = =,
    COMMUTATOR = <>,
    PROCEDURE  = fixeddecimal_numeric_ne,
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR < (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = NUMERIC,
    NEGATOR    = >=,
    COMMUTATOR = >,
    PROCEDURE  = fixeddecimal_numeric_lt,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = NUMERIC,
    NEGATOR    = >,
    COMMUTATOR = >=,
    PROCEDURE  = fixeddecimal_numeric_le,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = NUMERIC,
    NEGATOR    = <,
    COMMUTATOR = <=,
    PROCEDURE  = fixeddecimal_numeric_ge,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = NUMERIC,
    NEGATOR    = <=,
    COMMUTATOR = <,
    PROCEDURE  = fixeddecimal_numeric_gt,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR CLASS fixeddecimal_numeric_ops
FOR TYPE FIXEDDECIMAL USING btree AS
    OPERATOR    1   <  (FIXEDDECIMAL, NUMERIC),
    OPERATOR    2   <= (FIXEDDECIMAL, NUMERIC),
    OPERATOR    3   =  (FIXEDDECIMAL, NUMERIC),
    OPERATOR    4   >= (FIXEDDECIMAL, NUMERIC),
    OPERATOR    5   >  (FIXEDDECIMAL, NUMERIC),
    FUNCTION    1   fixeddecimal_numeric_cmp(FIXEDDECIMAL, NUMERIC);

CREATE OPERATOR CLASS fixeddecimal_numeric_ops
FOR TYPE FIXEDDECIMAL USING hash AS
    OPERATOR    1   =  (FIXEDDECIMAL, NUMERIC),
    FUNCTION    1   fixeddecimal_hash(FIXEDDECIMAL);

-- NUMERIC, FIXEDDECIMAL
CREATE FUNCTION numeric_fixeddecimal_eq(NUMERIC, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'numeric_fixeddecimal_eq'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION numeric_fixeddecimal_ne(NUMERIC, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'numeric_fixeddecimal_ne'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION numeric_fixeddecimal_lt(NUMERIC, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'numeric_fixeddecimal_lt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION numeric_fixeddecimal_le(NUMERIC, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'numeric_fixeddecimal_le'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION numeric_fixeddecimal_gt(NUMERIC, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'numeric_fixeddecimal_gt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION numeric_fixeddecimal_ge(NUMERIC, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'numeric_fixeddecimal_ge'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR = (
    LEFTARG    = NUMERIC,
    RIGHTARG   = FIXEDDECIMAL,
    COMMUTATOR = =,
    NEGATOR    = <>,
    PROCEDURE  = numeric_fixeddecimal_eq,
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel,
    MERGES
);

CREATE OPERATOR <> (
    LEFTARG    = NUMERIC,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = =,
    COMMUTATOR = <>,
    PROCEDURE  = numeric_fixeddecimal_ne,
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR < (
    LEFTARG    = NUMERIC,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = >=,
    COMMUTATOR = >,
    PROCEDURE  = numeric_fixeddecimal_lt,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = NUMERIC,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = >,
    COMMUTATOR = >=,
    PROCEDURE  = numeric_fixeddecimal_le,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = NUMERIC,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = <,
    COMMUTATOR = <=,
    PROCEDURE  = numeric_fixeddecimal_ge,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = NUMERIC,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = <=,
    COMMUTATOR = <,
    PROCEDURE  = numeric_fixeddecimal_gt,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR CLASS numeric_fixeddecimal_ops
FOR TYPE FIXEDDECIMAL USING btree AS
    OPERATOR    1   <  (NUMERIC, FIXEDDECIMAL) FOR SEARCH,
    OPERATOR    2   <= (NUMERIC, FIXEDDECIMAL) FOR SEARCH,
    OPERATOR    3   =  (NUMERIC, FIXEDDECIMAL) FOR SEARCH,
    OPERATOR    4   >= (NUMERIC, FIXEDDECIMAL) FOR SEARCH,
    OPERATOR    5   >  (NUMERIC, FIXEDDECIMAL) FOR SEARCH,
    FUNCTION    1   numeric_fixeddecimal_cmp(NUMERIC, FIXEDDECIMAL);

CREATE OPERATOR CLASS numeric_fixeddecimal_ops
FOR TYPE FIXEDDECIMAL USING hash AS
    OPERATOR    1   =  (NUMERIC, FIXEDDECIMAL),
    FUNCTION    1   fixeddecimal_hash(FIXEDDECIMAL);

-- FIXEDDECIMAL, INT4
CREATE FUNCTION fixeddecimal_int4_cmp(FIXEDDECIMAL, INT4)
RETURNS INT4
AS 'fixeddecimal', 'fixeddecimal_int4_cmp'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_int4_eq(FIXEDDECIMAL, INT4)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_int4_eq'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_int4_ne(FIXEDDECIMAL, INT4)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_int4_ne'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_int4_lt(FIXEDDECIMAL, INT4)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_int4_lt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_int4_le(FIXEDDECIMAL, INT4)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_int4_le'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_int4_gt(FIXEDDECIMAL, INT4)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_int4_gt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_int4_ge(FIXEDDECIMAL, INT4)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_int4_ge'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR = (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT4,
    COMMUTATOR = =,
    NEGATOR    = <>,
    PROCEDURE  = fixeddecimal_int4_eq,
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel,
    MERGES
);

CREATE OPERATOR <> (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT4,
    NEGATOR    = =,
    COMMUTATOR = <>,
    PROCEDURE  = fixeddecimal_int4_ne,
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR < (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT4,
    NEGATOR    = >=,
    COMMUTATOR = >,
    PROCEDURE  = fixeddecimal_int4_lt,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT4,
    NEGATOR    = >,
    COMMUTATOR = >=,
    PROCEDURE  = fixeddecimal_int4_le,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT4,
    NEGATOR    = <,
    COMMUTATOR = <=,
    PROCEDURE  = fixeddecimal_int4_ge,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT4,
    NEGATOR    = <=,
    COMMUTATOR = <,
    PROCEDURE  = fixeddecimal_int4_gt,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR CLASS fixeddecimal_int4_ops
FOR TYPE FIXEDDECIMAL USING btree AS
    OPERATOR    1   <  (FIXEDDECIMAL, INT4),
    OPERATOR    2   <= (FIXEDDECIMAL, INT4),
    OPERATOR    3   =  (FIXEDDECIMAL, INT4),
    OPERATOR    4   >= (FIXEDDECIMAL, INT4),
    OPERATOR    5   >  (FIXEDDECIMAL, INT4),
    FUNCTION    1   fixeddecimal_int4_cmp(FIXEDDECIMAL, INT4);

CREATE OPERATOR CLASS fixeddecimal_int4_ops
FOR TYPE FIXEDDECIMAL USING hash AS
    OPERATOR    1   =  (FIXEDDECIMAL, INT4),
    FUNCTION    1   fixeddecimal_hash(FIXEDDECIMAL);

-- INT4, FIXEDDECIMAL
CREATE FUNCTION int4_fixeddecimal_cmp(INT4, FIXEDDECIMAL)
RETURNS INT4
AS 'fixeddecimal', 'int4_fixeddecimal_cmp'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4_fixeddecimal_eq(INT4, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'int4_fixeddecimal_eq'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4_fixeddecimal_ne(INT4, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'int4_fixeddecimal_ne'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4_fixeddecimal_lt(INT4, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'int4_fixeddecimal_lt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4_fixeddecimal_le(INT4, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'int4_fixeddecimal_le'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4_fixeddecimal_gt(INT4, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'int4_fixeddecimal_gt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4_fixeddecimal_ge(INT4, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'int4_fixeddecimal_ge'
LANGUAGE C IMMUTABLE STRICT;
CREATE OPERATOR = (
    LEFTARG    = INT4,
    RIGHTARG   = FIXEDDECIMAL,
    COMMUTATOR = =,
    NEGATOR    = <>,
    PROCEDURE  = int4_fixeddecimal_eq,
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel,
    MERGES
);

CREATE OPERATOR <> (
    LEFTARG    = INT4,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = =,
    COMMUTATOR = <>,
    PROCEDURE  = int4_fixeddecimal_ne,
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR < (
    LEFTARG    = INT4,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = >=,
    COMMUTATOR = >,
    PROCEDURE  = int4_fixeddecimal_lt,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = INT4,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = >,
    COMMUTATOR = >=,
    PROCEDURE  = int4_fixeddecimal_le,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = INT4,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = <,
    COMMUTATOR = <=,
    PROCEDURE  = int4_fixeddecimal_ge,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = INT4,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = <=,
    COMMUTATOR = <,
    PROCEDURE  = int4_fixeddecimal_gt,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR CLASS int4_fixeddecimal_ops
FOR TYPE FIXEDDECIMAL USING btree AS
    OPERATOR    1   <  (INT4, FIXEDDECIMAL),
    OPERATOR    2   <= (INT4, FIXEDDECIMAL),
    OPERATOR    3   =  (INT4, FIXEDDECIMAL),
    OPERATOR    4   >= (INT4, FIXEDDECIMAL),
    OPERATOR    5   >  (INT4, FIXEDDECIMAL),
    FUNCTION    1   int4_fixeddecimal_cmp(INT4, FIXEDDECIMAL);

CREATE OPERATOR CLASS int4_fixeddecimal_ops
FOR TYPE FIXEDDECIMAL USING hash AS
    OPERATOR    1   =  (INT4, FIXEDDECIMAL),
    FUNCTION    1   fixeddecimal_hash(FIXEDDECIMAL);

-- FIXEDDECIMAL, INT2
CREATE FUNCTION fixeddecimal_int2_cmp(FIXEDDECIMAL, INT2)
RETURNS INT4
AS 'fixeddecimal', 'fixeddecimal_int2_cmp'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_int2_eq(FIXEDDECIMAL, INT2)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_int2_eq'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_int2_ne(FIXEDDECIMAL, INT2)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_int2_ne'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_int2_lt(FIXEDDECIMAL, INT2)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_int2_lt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_int2_le(FIXEDDECIMAL, INT2)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_int2_le'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_int2_gt(FIXEDDECIMAL, INT2)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_int2_gt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_int2_ge(FIXEDDECIMAL, INT2)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimal_int2_ge'
LANGUAGE C IMMUTABLE STRICT;
CREATE OPERATOR = (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT2,
    COMMUTATOR = =,
    NEGATOR    = <>,
    PROCEDURE  = fixeddecimal_int2_eq,
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel,
    MERGES
);

CREATE OPERATOR <> (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT2,
    NEGATOR    = =,
    COMMUTATOR = <>,
    PROCEDURE  = fixeddecimal_int2_ne,
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR < (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT2,
    NEGATOR    = >=,
    COMMUTATOR = >,
    PROCEDURE  = fixeddecimal_int2_lt,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT2,
    NEGATOR    = >,
    COMMUTATOR = >=,
    PROCEDURE  = fixeddecimal_int2_le,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT2,
    NEGATOR    = <,
    COMMUTATOR = <=,
    PROCEDURE  = fixeddecimal_int2_ge,
    RESTRICT   = scalargtsel,
   JOIN       = scalargtjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT2,
    NEGATOR    = <=,
    COMMUTATOR = <,
    PROCEDURE  = fixeddecimal_int2_gt,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR CLASS fixeddecimal_int2_ops
FOR TYPE FIXEDDECIMAL USING btree AS
    OPERATOR    1   <  (FIXEDDECIMAL, INT2),
    OPERATOR    2   <= (FIXEDDECIMAL, INT2),
    OPERATOR    3   =  (FIXEDDECIMAL, INT2),
    OPERATOR    4   >= (FIXEDDECIMAL, INT2),
    OPERATOR    5   >  (FIXEDDECIMAL, INT2),
    FUNCTION    1   fixeddecimal_int2_cmp(FIXEDDECIMAL, INT2);

CREATE OPERATOR CLASS fixeddecimal_int2_ops
FOR TYPE FIXEDDECIMAL USING hash AS
    OPERATOR    1   =  (FIXEDDECIMAL, INT2),
    FUNCTION    1   fixeddecimal_hash(FIXEDDECIMAL);

-- INT2, FIXEDDECIMAL
CREATE FUNCTION int2_fixeddecimal_cmp(INT2, FIXEDDECIMAL)
RETURNS INT4
AS 'fixeddecimal', 'int2_fixeddecimal_cmp'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2_fixeddecimal_eq(INT2, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'int2_fixeddecimal_eq'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2_fixeddecimal_ne(INT2, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'int2_fixeddecimal_ne'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2_fixeddecimal_lt(INT2, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'int2_fixeddecimal_lt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2_fixeddecimal_le(INT2, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'int2_fixeddecimal_le'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2_fixeddecimal_gt(INT2, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'int2_fixeddecimal_gt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2_fixeddecimal_ge(INT2, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'int2_fixeddecimal_ge'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR = (
    LEFTARG    = INT2,
    RIGHTARG   = FIXEDDECIMAL,
    COMMUTATOR = =,
    NEGATOR    = <>,
    PROCEDURE  = int2_fixeddecimal_eq,
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel,
    MERGES
);

CREATE OPERATOR <> (
    LEFTARG    = INT2,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = =,
    COMMUTATOR = <>,
    PROCEDURE  = int2_fixeddecimal_ne,
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR < (
    LEFTARG    = INT2,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = >=,
    COMMUTATOR = >,
    PROCEDURE  = int2_fixeddecimal_lt,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = INT2,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = >,
    COMMUTATOR = >=,
    PROCEDURE  = int2_fixeddecimal_le,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = INT2,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = <,
    COMMUTATOR = <=,
    PROCEDURE  = int2_fixeddecimal_ge,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = INT2,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = <=,
    COMMUTATOR = <,
    PROCEDURE  = int2_fixeddecimal_gt,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR CLASS int2_fixeddecimal_ops
FOR TYPE FIXEDDECIMAL USING btree AS
    OPERATOR    1   <  (INT2, FIXEDDECIMAL),
    OPERATOR    2   <= (INT2, FIXEDDECIMAL),
    OPERATOR    3   =  (INT2, FIXEDDECIMAL),
    OPERATOR    4   >= (INT2, FIXEDDECIMAL),
    OPERATOR    5   >  (INT2, FIXEDDECIMAL),
    FUNCTION    1   int2_fixeddecimal_cmp(INT2, FIXEDDECIMAL);

CREATE OPERATOR CLASS int2_fixeddecimal_ops
FOR TYPE FIXEDDECIMAL USING hash AS
    OPERATOR    1   =  (INT2, FIXEDDECIMAL),
    FUNCTION    1   fixeddecimal_hash(FIXEDDECIMAL);


-- 9.6+ Parallel function changes.
ALTER FUNCTION fixeddecimalin(cstring, oid, int4) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalout(fixeddecimal) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalrecv(internal) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalsend(FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimaltypmodin(_cstring) PARALLEL SAFE;
ALTER FUNCTION fixeddecimaltypmodout(INT4) PARALLEL SAFE;
ALTER FUNCTION fixeddecimaleq(FIXEDDECIMAL, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalne(FIXEDDECIMAL, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimallt(FIXEDDECIMAL, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalle(FIXEDDECIMAL, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalgt(FIXEDDECIMAL, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalge(FIXEDDECIMAL, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalum(FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalpl(FIXEDDECIMAL, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalmi(FIXEDDECIMAL, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalmul(FIXEDDECIMAL, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimaldiv(FIXEDDECIMAL, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION abs(FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimallarger(FIXEDDECIMAL, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalsmaller(FIXEDDECIMAL, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_cmp(FIXEDDECIMAL, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_hash(FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_numeric_cmp(FIXEDDECIMAL, NUMERIC) PARALLEL SAFE;
ALTER FUNCTION numeric_fixeddecimal_cmp(NUMERIC, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_numeric_eq(FIXEDDECIMAL, NUMERIC) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_numeric_ne(FIXEDDECIMAL, NUMERIC) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_numeric_lt(FIXEDDECIMAL, NUMERIC) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_numeric_le(FIXEDDECIMAL, NUMERIC) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_numeric_gt(FIXEDDECIMAL, NUMERIC) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_numeric_ge(FIXEDDECIMAL, NUMERIC) PARALLEL SAFE;
ALTER FUNCTION numeric_fixeddecimal_eq(NUMERIC, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION numeric_fixeddecimal_ne(NUMERIC, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION numeric_fixeddecimal_lt(NUMERIC, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION numeric_fixeddecimal_le(NUMERIC, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION numeric_fixeddecimal_gt(NUMERIC, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION numeric_fixeddecimal_ge(NUMERIC, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_int4_cmp(FIXEDDECIMAL, INT4) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_int4_eq(FIXEDDECIMAL, INT4) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_int4_ne(FIXEDDECIMAL, INT4) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_int4_lt(FIXEDDECIMAL, INT4) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_int4_le(FIXEDDECIMAL, INT4) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_int4_gt(FIXEDDECIMAL, INT4) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_int4_ge(FIXEDDECIMAL, INT4) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalint4pl(FIXEDDECIMAL, INT4) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalint4mi(FIXEDDECIMAL, INT4) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalint4mul(FIXEDDECIMAL, INT4) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalint4div(FIXEDDECIMAL, INT4) PARALLEL SAFE;
ALTER FUNCTION int4_fixeddecimal_cmp(INT4, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int4_fixeddecimal_eq(INT4, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int4_fixeddecimal_ne(INT4, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int4_fixeddecimal_lt(INT4, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int4_fixeddecimal_le(INT4, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int4_fixeddecimal_gt(INT4, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int4_fixeddecimal_ge(INT4, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int4fixeddecimalpl(INT4, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int4fixeddecimalmi(INT4, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int4fixeddecimalmul(INT4, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int4fixeddecimaldiv(INT4, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_int2_cmp(FIXEDDECIMAL, INT2) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_int2_eq(FIXEDDECIMAL, INT2) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_int2_ne(FIXEDDECIMAL, INT2) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_int2_lt(FIXEDDECIMAL, INT2) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_int2_le(FIXEDDECIMAL, INT2) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_int2_gt(FIXEDDECIMAL, INT2) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_int2_ge(FIXEDDECIMAL, INT2) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalint2pl(FIXEDDECIMAL, INT2) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalint2mi(FIXEDDECIMAL, INT2) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalint2mul(FIXEDDECIMAL, INT2) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalint2div(FIXEDDECIMAL, INT2) PARALLEL SAFE;
ALTER FUNCTION int2_fixeddecimal_cmp(INT2, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int2_fixeddecimal_eq(INT2, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int2_fixeddecimal_ne(INT2, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int2_fixeddecimal_lt(INT2, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int2_fixeddecimal_le(INT2, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int2_fixeddecimal_gt(INT2, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int2_fixeddecimal_ge(INT2, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int2fixeddecimalpl(INT2, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int2fixeddecimalmi(INT2, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int2fixeddecimalmul(INT2, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int2fixeddecimaldiv(INT2, FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal(FIXEDDECIMAL, INT4) PARALLEL SAFE;
ALTER FUNCTION int4fixeddecimal(INT4) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalint4(FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION int2fixeddecimal(INT2) PARALLEL SAFE;
ALTER FUNCTION fixeddecimalint2(FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimaltod(FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION dtofixeddecimal(DOUBLE PRECISION) PARALLEL SAFE;
ALTER FUNCTION fixeddecimaltof(FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION ftofixeddecimal(REAL) PARALLEL SAFE;
ALTER FUNCTION fixeddecimal_numeric(FIXEDDECIMAL) PARALLEL SAFE;
ALTER FUNCTION numeric_fixeddecimal(NUMERIC) PARALLEL SAFE;

CREATE FUNCTION fixeddecimalaggstatecombine(INTERNAL, INTERNAL)
RETURNS INTERNAL
AS 'fixeddecimal', 'fixeddecimalaggstatecombine'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION fixeddecimalaggstateserialize(INTERNAL)
RETURNS BYTEA
AS 'fixeddecimal', 'fixeddecimalaggstateserialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION fixeddecimalaggstatedeserialize(BYTEA, INTERNAL)
RETURNS INTERNAL
AS 'fixeddecimal', 'fixeddecimalaggstatedeserialize'
LANGUAGE C IMMUTABLE PARALLEL SAFE;


UPDATE pg_proc SET proparallel = 's'
WHERE oid = 'min(FIXEDDECIMAL)'::pg_catalog.regprocedure;

UPDATE pg_proc SET proparallel = 's'
WHERE oid = 'max(FIXEDDECIMAL)'::pg_catalog.regprocedure;

UPDATE pg_proc SET proparallel = 's'
WHERE oid = 'sum(FIXEDDECIMAL)'::pg_catalog.regprocedure;

UPDATE pg_proc SET proparallel = 's'
WHERE oid = 'avg(FIXEDDECIMAL)'::pg_catalog.regprocedure;

UPDATE pg_aggregate SET aggcombinefn = 'fixeddecimalsmaller'
WHERE aggfnoid = 'min(FIXEDDECIMAL)'::pg_catalog.regprocedure;

UPDATE pg_aggregate SET aggcombinefn = 'fixeddecimallarger'
WHERE aggfnoid = 'max(FIXEDDECIMAL)'::pg_catalog.regprocedure;

UPDATE pg_aggregate SET aggcombinefn = 'fixeddecimalaggstatecombine',
						aggserialfn = 'fixeddecimalaggstateserialize',
						aggdeserialfn = 'fixeddecimalaggstatedeserialize'
WHERE aggfnoid = 'sum(FIXEDDECIMAL)'::pg_catalog.regprocedure;

UPDATE pg_aggregate SET aggcombinefn = 'fixeddecimalaggstatecombine',
						aggserialfn = 'fixeddecimalaggstateserialize',
						aggdeserialfn = 'fixeddecimalaggstatedeserialize'
WHERE aggfnoid = 'avg(FIXEDDECIMAL)'::pg_catalog.regprocedure;
