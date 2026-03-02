
-- Aggregate Support


CREATE FUNCTION fixeddecimal_avg_accum(INTERNAL, FIXEDDECIMAL)
RETURNS INTERNAL
AS 'fixeddecimal', 'fixeddecimal_avg_accum'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION fixeddecimal_sum(INTERNAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimal_sum'
LANGUAGE C IMMUTABLE;

CREATE FUNCTION fixeddecimal_avg(INTERNAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimal_avg'
LANGUAGE C IMMUTABLE;

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


