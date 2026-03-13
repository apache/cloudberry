
-- Aggregate Support

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

CREATE FUNCTION fixeddecimal_avg_accum(INTERNAL, FIXEDDECIMAL)
RETURNS INTERNAL
AS 'fixeddecimal', 'fixeddecimal_avg_accum'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION fixeddecimal_sum(INTERNAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimal_sum'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE FUNCTION fixeddecimal_avg(INTERNAL)
RETURNS FIXEDDECIMAL
AS 'fixeddecimal', 'fixeddecimal_avg'
LANGUAGE C IMMUTABLE PARALLEL SAFE;

CREATE AGGREGATE min(FIXEDDECIMAL) (
    SFUNC = fixeddecimalsmaller,
    STYPE = FIXEDDECIMAL,
    SORTOP = <,
    COMBINEFUNC = fixeddecimalsmaller,
    PARALLEL = SAFE
);

CREATE AGGREGATE max(FIXEDDECIMAL) (
    SFUNC = fixeddecimallarger,
    STYPE = FIXEDDECIMAL,
    SORTOP = >,
    COMBINEFUNC = fixeddecimallarger,
    PARALLEL = SAFE
);

CREATE AGGREGATE sum(FIXEDDECIMAL) (
    SFUNC = fixeddecimal_avg_accum,
    FINALFUNC = fixeddecimal_sum,
    STYPE = INTERNAL,
    COMBINEFUNC = fixeddecimalaggstatecombine,
    SERIALFUNC = fixeddecimalaggstateserialize,
    DESERIALFUNC = fixeddecimalaggstatedeserialize,
    PARALLEL = SAFE
);

CREATE AGGREGATE avg(FIXEDDECIMAL) (
    SFUNC = fixeddecimal_avg_accum,
    FINALFUNC = fixeddecimal_avg,
    STYPE = INTERNAL,
    COMBINEFUNC = fixeddecimalaggstatecombine,
    SERIALFUNC = fixeddecimalaggstateserialize,
    DESERIALFUNC = fixeddecimalaggstatedeserialize,
    PARALLEL = SAFE
);


