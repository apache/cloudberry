
-- Aggregate Support


CREATE FUNCTION fixeddecimalaggstatecombine(FIXEDDECIMALAGGSTATE, INTERNAL)
RETURNS FIXEDDECIMALAGGSTATE
AS 'fixeddecimal', 'fixeddecimalaggstatecombine'
LANGUAGE C IMMUTABLE;

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
    CFUNC = fixeddecimalsmaller,
    CTYPE = FIXEDDECIMAL,
    STYPE = FIXEDDECIMAL,
    SORTOP = <
);

CREATE AGGREGATE max(FIXEDDECIMAL) (
    SFUNC = fixeddecimallarger,
    CFUNC = fixeddecimallarger,
    CTYPE = FIXEDDECIMAL,
    STYPE = FIXEDDECIMAL,
    SORTOP = >
);

CREATE AGGREGATE sum(FIXEDDECIMAL) (
    SFUNC = fixeddecimal_avg_accum,
    CFUNC = fixeddecimalaggstatecombine,
    CTYPE = FIXEDDECIMALAGGSTATE,
    FINALFUNC = fixeddecimal_sum,
    STYPE = INTERNAL
);

CREATE AGGREGATE avg(FIXEDDECIMAL) (
    SFUNC = fixeddecimal_avg_accum,
    CFUNC = fixeddecimalaggstatecombine,
    CTYPE = FIXEDDECIMALAGGSTATE,
    FINALFUNC = fixeddecimal_avg,
    STYPE = INTERNAL
);


