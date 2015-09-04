
------------------
-- FIXEDDECIMAL --
------------------

CREATE TYPE FIXEDDECIMAL;

CREATE FUNCTION fixeddecimalin(cstring)
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
LANGUAGE C STABLE STRICT;

CREATE FUNCTION fixeddecimalsend(FIXEDDECIMAL)
RETURNS bytea
AS 'fixeddecimal', 'fixeddecimalsend'
LANGUAGE C STABLE STRICT;

CREATE TYPE FIXEDDECIMAL (
    INPUT          = fixeddecimalin,
    OUTPUT         = fixeddecimalout,
    RECEIVE        = fixeddecimalrecv,
    SEND           = fixeddecimalsend,
    INTERNALLENGTH = 8,
	ALIGNMENT      = 'double',
    STORAGE        = plain,
    CATEGORY       = 'N',
    PREFERRED      = false,
    COLLATABLE     = false,
	PASSEDBYVALUE -- But not always.. XXX fix that.
);

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

--
-- Operators.
--

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

--
-- Cross type operators with int4
--

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

--
-- Cross type operators with int2
--

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

--
-- Casts
--

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

-- Aggregate Support


CREATE FUNCTION fixeddecimal_avg_accum(INTERNAL, FIXEDDECIMAL)
RETURNS INTERNAL
AS 'fixeddecimal', 'fixeddecimal_avg_accum'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION fixeddecimal_sum(INTERNAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimal_sum'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION fixeddecimal_avg(INTERNAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimal_avg'
LANGUAGE C IMMUTABLE STRICT;

CREATE AGGREGATE min(FIXEDDECIMAL) (
    SFUNC = fixeddecimalsmaller,
    STYPE = FIXEDDECIMAL,
    SORTOP = <
);

CREATE AGGREGATE max(FIXEDDECIMAL) (
    SFUNC = fixeddecimallarger,
    STYPE = FIXEDDECIMAL,
    SORTOP = >
);

CREATE AGGREGATE sum(FIXEDDECIMAL) (
    SFUNC = fixeddecimal_avg_accum,
	FINALFUNC = fixeddecimal_sum,
    STYPE = INTERNAL
);

CREATE AGGREGATE avg(FIXEDDECIMAL) (
    SFUNC = fixeddecimal_avg_accum,
	FINALFUNC = fixeddecimal_avg,
    STYPE = INTERNAL
);