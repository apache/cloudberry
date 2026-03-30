
-- Aggregate Support


CREATE FUNCTION fixeddecimalaggstatecombine(FIXEDDECIMALAGGSTATE, FIXEDDECIMALAGGSTATE)
RETURNS FIXEDDECIMALAGGSTATE
AS 'fixeddecimal', 'fixeddecimalaggstatecombine'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION fixeddecimal_avg_accum(FIXEDDECIMALAGGSTATE, FIXEDDECIMAL)
RETURNS FIXEDDECIMALAGGSTATE
AS 'fixeddecimal', 'fixeddecimal_avg_accum'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION fixeddecimal_sum(FIXEDDECIMALAGGSTATE)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimal_sum'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION fixeddecimal_avg(FIXEDDECIMALAGGSTATE)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimal_avg'
LANGUAGE C IMMUTABLE;

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
    STYPE = FIXEDDECIMALAGGSTATE
);

CREATE AGGREGATE avg(FIXEDDECIMAL) (
    SFUNC = fixeddecimal_avg_accum,
    CFUNC = fixeddecimalaggstatecombine,
    CTYPE = FIXEDDECIMALAGGSTATE,
    FINALFUNC = fixeddecimal_avg,
    STYPE = FIXEDDECIMALAGGSTATE
);


