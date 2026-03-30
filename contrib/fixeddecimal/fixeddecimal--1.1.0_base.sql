
------------------
-- FIXEDDECIMAL --
------------------

CREATE TYPE FIXEDDECIMAL;

CREATE FUNCTION fixeddecimalin(cstring, oid, int4)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalin'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalout(fixeddecimal)
RETURNS cstring
AS 'fixeddecimal', 'fixeddecimalout'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalrecv(internal)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalrecv'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalsend(FIXEDDECIMAL)
RETURNS bytea
AS 'fixeddecimal', 'fixeddecimalsend'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimaltypmodin(_cstring)
RETURNS INT4
AS 'fixeddecimal', 'fixeddecimaltypmodin'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimaltypmodout(INT4)
RETURNS cstring
AS 'fixeddecimal', 'fixeddecimaltypmodout'
LANGUAGE C IMMUTABLE STRICT;


CREATE TYPE FIXEDDECIMAL (
    INPUT          = fixeddecimalin,
    OUTPUT         = fixeddecimalout,
    RECEIVE        = fixeddecimalrecv,
    SEND           = fixeddecimalsend,
	TYPMOD_IN      = fixeddecimaltypmodin,
	TYPMOD_OUT     = fixeddecimaltypmodout,
    INTERNALLENGTH = 8,
	ALIGNMENT      = 'double',
    STORAGE        = plain,
    CATEGORY       = 'N',
    PREFERRED      = false,
    COLLATABLE     = false,
	PASSEDBYVALUE -- But not always.. XXX fix that.
);

-- FIXEDDECIMAL, NUMERIC
CREATE FUNCTION fixeddecimaleq(FIXEDDECIMAL, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimaleq'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalne(FIXEDDECIMAL, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimalne'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimallt(FIXEDDECIMAL, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimallt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalle(FIXEDDECIMAL, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimalle'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalgt(FIXEDDECIMAL, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimalgt'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalge(FIXEDDECIMAL, FIXEDDECIMAL)
RETURNS bool
AS 'fixeddecimal', 'fixeddecimalge'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalum(FIXEDDECIMAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalum'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalpl(FIXEDDECIMAL, FIXEDDECIMAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalpl'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalmi(FIXEDDECIMAL, FIXEDDECIMAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalmi'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalmul(FIXEDDECIMAL, FIXEDDECIMAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalmul'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimaldiv(FIXEDDECIMAL, FIXEDDECIMAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimaldiv'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION abs(FIXEDDECIMAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalabs'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimallarger(FIXEDDECIMAL, FIXEDDECIMAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimallarger'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalsmaller(FIXEDDECIMAL, FIXEDDECIMAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalsmaller'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_cmp(FIXEDDECIMAL, FIXEDDECIMAL)
RETURNS INT4
AS 'fixeddecimal', 'fixeddecimal_cmp'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_hash(FIXEDDECIMAL)
RETURNS INT4
AS 'fixeddecimal', 'fixeddecimal_hash'
LANGUAGE C IMMUTABLE STRICT;

--
-- Operators.
--

-- FIXEDDECIMAL op FIXEDDECIMAL
CREATE OPERATOR = (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = FIXEDDECIMAL,
    COMMUTATOR = =,
    NEGATOR    = <>,
    PROCEDURE  = fixeddecimaleq,
    RESTRICT   = eqsel,
    JOIN       = eqjoinsel,
    MERGES
);

CREATE OPERATOR <> (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = =,
    COMMUTATOR = <>,
    PROCEDURE  = fixeddecimalne,
    RESTRICT   = neqsel,
    JOIN       = neqjoinsel
);

CREATE OPERATOR < (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = >=,
    COMMUTATOR = >,
    PROCEDURE  = fixeddecimallt,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR <= (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = >,
    COMMUTATOR = >=,
    PROCEDURE  = fixeddecimalle,
    RESTRICT   = scalarltsel,
    JOIN       = scalarltjoinsel
);

CREATE OPERATOR >= (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = <,
    COMMUTATOR = <=,
    PROCEDURE  = fixeddecimalge,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR > (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = FIXEDDECIMAL,
    NEGATOR    = <=,
    COMMUTATOR = <,
    PROCEDURE  = fixeddecimalgt,
    RESTRICT   = scalargtsel,
    JOIN       = scalargtjoinsel
);

CREATE OPERATOR + (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = FIXEDDECIMAL,
    COMMUTATOR = +,
    PROCEDURE  = fixeddecimalpl
);

CREATE OPERATOR - (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = FIXEDDECIMAL,
    PROCEDURE  = fixeddecimalmi
);

CREATE OPERATOR - (
    RIGHTARG   = FIXEDDECIMAL,
    PROCEDURE  = fixeddecimalum
);

CREATE OPERATOR * (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = FIXEDDECIMAL,
    COMMUTATOR = *,
    PROCEDURE  = fixeddecimalmul
);

CREATE OPERATOR / (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = FIXEDDECIMAL,
    PROCEDURE  = fixeddecimaldiv
);

CREATE OPERATOR CLASS fixeddecimal_ops
DEFAULT FOR TYPE FIXEDDECIMAL USING btree AS
    OPERATOR    1   <  (FIXEDDECIMAL, FIXEDDECIMAL),
    OPERATOR    2   <= (FIXEDDECIMAL, FIXEDDECIMAL),
    OPERATOR    3   =  (FIXEDDECIMAL, FIXEDDECIMAL),
    OPERATOR    4   >= (FIXEDDECIMAL, FIXEDDECIMAL),
    OPERATOR    5   >  (FIXEDDECIMAL, FIXEDDECIMAL),
    FUNCTION    1   fixeddecimal_cmp(FIXEDDECIMAL, FIXEDDECIMAL);

CREATE OPERATOR CLASS fixeddecimal_ops
DEFAULT FOR TYPE FIXEDDECIMAL USING hash AS
    OPERATOR    1   =  (FIXEDDECIMAL, FIXEDDECIMAL),
    FUNCTION    1   fixeddecimal_hash(FIXEDDECIMAL);

-- FIXEDDECIMAL, NUMERIC
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

--
-- Cross type operators with int4
--

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

CREATE FUNCTION fixeddecimalint4pl(FIXEDDECIMAL, INT4)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalint4pl'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalint4mi(FIXEDDECIMAL, INT4)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalint4mi'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalint4mul(FIXEDDECIMAL, INT4)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalint4mul'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalint4div(FIXEDDECIMAL, INT4)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalint4div'
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

CREATE OPERATOR + (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT4,
    COMMUTATOR = +,
    PROCEDURE  = fixeddecimalint4pl
);

CREATE OPERATOR - (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT4,
    PROCEDURE  = fixeddecimalint4mi
);

CREATE OPERATOR * (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT4,
    COMMUTATOR = *,
    PROCEDURE  = fixeddecimalint4mul
);

CREATE OPERATOR / (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT4,
    PROCEDURE  = fixeddecimalint4div
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

CREATE FUNCTION int4fixeddecimalpl(INT4, FIXEDDECIMAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'int4fixeddecimalpl'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4fixeddecimalmi(INT4, FIXEDDECIMAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'int4fixeddecimalmi'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4fixeddecimalmul(INT4, FIXEDDECIMAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'int4fixeddecimalmul'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4fixeddecimaldiv(INT4, FIXEDDECIMAL)
RETURNS DOUBLE PRECISION
AS 'fixeddecimal', 'int4fixeddecimaldiv'
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

CREATE OPERATOR + (
    LEFTARG    = INT4,
    RIGHTARG   = FIXEDDECIMAL,
    COMMUTATOR = +,
    PROCEDURE  = int4fixeddecimalpl
);

CREATE OPERATOR - (
    LEFTARG    = INT4,
    RIGHTARG   = FIXEDDECIMAL,
    PROCEDURE  = int4fixeddecimalmi
);

CREATE OPERATOR * (
    LEFTARG    = INT4,
    RIGHTARG   = FIXEDDECIMAL,
    COMMUTATOR = *,
    PROCEDURE  = int4fixeddecimalmul
);

CREATE OPERATOR / (
    LEFTARG    = INT4,
    RIGHTARG   = FIXEDDECIMAL,
    PROCEDURE  = int4fixeddecimaldiv
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

--
-- Cross type operators with int2
--
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

CREATE FUNCTION fixeddecimalint2pl(FIXEDDECIMAL, INT2)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalint2pl'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalint2mi(FIXEDDECIMAL, INT2)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalint2mi'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalint2mul(FIXEDDECIMAL, INT2)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalint2mul'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalint2div(FIXEDDECIMAL, INT2)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimalint2div'
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

CREATE OPERATOR + (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT2,
    COMMUTATOR = +,
    PROCEDURE  = fixeddecimalint2pl
);

CREATE OPERATOR - (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT2,
    PROCEDURE  = fixeddecimalint2mi
);

CREATE OPERATOR * (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT2,
    COMMUTATOR = *,
    PROCEDURE  = fixeddecimalint2mul
);

CREATE OPERATOR / (
    LEFTARG    = FIXEDDECIMAL,
    RIGHTARG   = INT2,
    PROCEDURE  = fixeddecimalint2div
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

CREATE FUNCTION int2fixeddecimalpl(INT2, FIXEDDECIMAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'int2fixeddecimalpl'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2fixeddecimalmi(INT2, FIXEDDECIMAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'int2fixeddecimalmi'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2fixeddecimalmul(INT2, FIXEDDECIMAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'int2fixeddecimalmul'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2fixeddecimaldiv(INT2, FIXEDDECIMAL)
RETURNS DOUBLE PRECISION
AS 'fixeddecimal', 'int2fixeddecimaldiv'
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

CREATE OPERATOR + (
    LEFTARG    = INT2,
    RIGHTARG   = FIXEDDECIMAL,
    COMMUTATOR = +,
    PROCEDURE  = int2fixeddecimalpl
);

CREATE OPERATOR - (
    LEFTARG    = INT2,
    RIGHTARG   = FIXEDDECIMAL,
    PROCEDURE  = int2fixeddecimalmi
);

CREATE OPERATOR * (
    LEFTARG    = INT2,
    RIGHTARG   = FIXEDDECIMAL,
    COMMUTATOR = *,
    PROCEDURE  = int2fixeddecimalmul
);

CREATE OPERATOR / (
    LEFTARG    = INT2,
    RIGHTARG   = FIXEDDECIMAL,
    PROCEDURE  = int2fixeddecimaldiv
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

--
-- Casts
--

CREATE FUNCTION fixeddecimal(FIXEDDECIMAL, INT4)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimal'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int4fixeddecimal(INT4)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'int4fixeddecimal'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalint4(FIXEDDECIMAL)
RETURNS INT4
AS 'fixeddecimal', 'fixeddecimalint4'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION int2fixeddecimal(INT2)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'int2fixeddecimal'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimalint2(FIXEDDECIMAL)
RETURNS INT2
AS 'fixeddecimal', 'fixeddecimalint2'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimaltod(FIXEDDECIMAL)
RETURNS DOUBLE PRECISION
AS 'fixeddecimal', 'fixeddecimaltod'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION dtofixeddecimal(DOUBLE PRECISION)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'dtofixeddecimal'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimaltof(FIXEDDECIMAL)
RETURNS REAL
AS 'fixeddecimal', 'fixeddecimaltof'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION ftofixeddecimal(REAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'ftofixeddecimal'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_numeric(FIXEDDECIMAL)
RETURNS NUMERIC
AS 'fixeddecimal', 'fixeddecimal_numeric'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION numeric_fixeddecimal(NUMERIC)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'numeric_fixeddecimal'
LANGUAGE C IMMUTABLE STRICT;

CREATE CAST (FIXEDDECIMAL AS FIXEDDECIMAL)
	WITH FUNCTION fixeddecimal (FIXEDDECIMAL, INT4) AS ASSIGNMENT;

CREATE CAST (INT4 AS FIXEDDECIMAL)
	WITH FUNCTION int4fixeddecimal (INT4) AS IMPLICIT;

CREATE CAST (FIXEDDECIMAL AS INT4)
	WITH FUNCTION fixeddecimalint4 (FIXEDDECIMAL) AS ASSIGNMENT;

CREATE CAST (INT2 AS FIXEDDECIMAL)
	WITH FUNCTION int2fixeddecimal (INT2) AS IMPLICIT;

CREATE CAST (FIXEDDECIMAL AS INT2)
	WITH FUNCTION fixeddecimalint2 (FIXEDDECIMAL) AS ASSIGNMENT;

CREATE CAST (FIXEDDECIMAL AS DOUBLE PRECISION)
	WITH FUNCTION fixeddecimaltod (FIXEDDECIMAL) AS IMPLICIT;

CREATE CAST (DOUBLE PRECISION AS FIXEDDECIMAL)
	WITH FUNCTION dtofixeddecimal (DOUBLE PRECISION) AS ASSIGNMENT; -- XXX? or Implicit?

CREATE CAST (FIXEDDECIMAL AS REAL)
	WITH FUNCTION fixeddecimaltof (FIXEDDECIMAL) AS IMPLICIT;

CREATE CAST (REAL AS FIXEDDECIMAL)
	WITH FUNCTION ftofixeddecimal (REAL) AS ASSIGNMENT; -- XXX or Implicit?

CREATE CAST (FIXEDDECIMAL AS NUMERIC)
	WITH FUNCTION fixeddecimal_numeric (FIXEDDECIMAL) AS IMPLICIT;

CREATE CAST (NUMERIC AS FIXEDDECIMAL)
	WITH FUNCTION numeric_fixeddecimal (NUMERIC) AS ASSIGNMENT;
