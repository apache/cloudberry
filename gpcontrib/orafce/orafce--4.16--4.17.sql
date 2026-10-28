CREATE FUNCTION oracle.hextoraw(text)
RETURNS bytea
AS $$ SELECT pg_catalog.decode($1, 'hex') $$
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION oracle.hextoraw(text) IS 'Converts a string of hexadecimal digits to a raw (bytea) value';

CREATE FUNCTION oracle.rawtohex(bytea)
RETURNS text
AS $$ SELECT pg_catalog.upper(encode($1, 'hex')) $$
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION oracle.rawtohex(bytea) IS 'Converts a raw (bytea) value to a string of hexadecimal digits';

CREATE FUNCTION oracle.empty_clob()
RETURNS text
AS $$ SELECT ''::text $$
LANGUAGE sql IMMUTABLE PARALLEL SAFE;
COMMENT ON FUNCTION oracle.empty_clob() IS 'Returns an empty character LOB';

CREATE FUNCTION oracle.empty_blob()
RETURNS bytea
AS $$ SELECT ''::bytea $$
LANGUAGE sql IMMUTABLE PARALLEL SAFE;
COMMENT ON FUNCTION oracle.empty_blob() IS 'Returns an empty binary LOB';

CREATE FUNCTION oracle.from_tz(timestamp, text)
RETURNS timestamp with time zone
AS $$
    -- A numeric offset ('+02:00') is applied as an interval so it follows the
    -- ISO sign convention (east of UTC is positive), as Oracle does; passing it
    -- straight to AT TIME ZONE as text would use PostgreSQL's inverted POSIX
    -- sign. A region name ('Europe/Prague') is looked up in the zone database.
    SELECT CASE
        WHEN $2 ~ '^[+-]?[0-9]{1,2}:[0-9]{2}$' THEN $1 AT TIME ZONE ($2)::interval
        ELSE $1 AT TIME ZONE $2
    END
$$
LANGUAGE sql STABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION oracle.from_tz(timestamp, text) IS 'Interprets a timestamp as being in the given time zone and returns a timestamp with time zone';

-- UTL_RAW: Oracle's RAW/bytea manipulation package. CAST_TO_RAW / CAST_TO_VARCHAR2
-- use the database character set (like Oracle, which uses the database charset for
-- the VARCHAR2<->RAW casts). The bitwise operators follow Oracle's rule that, when
-- the operands differ in length, the operation stops at the end of the shorter one
-- and the unprocessed tail of the longer is appended to the result.
CREATE SCHEMA utl_raw;

CREATE FUNCTION utl_raw.cast_to_raw(text)
RETURNS bytea
AS $$ SELECT pg_catalog.convert_to($1, pg_catalog.current_setting('server_encoding')) $$
LANGUAGE sql STABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION utl_raw.cast_to_raw(text) IS 'Converts a value to a raw (bytea) value using the database character set';

CREATE FUNCTION utl_raw.cast_to_varchar2(bytea)
RETURNS text
AS $$ SELECT pg_catalog.convert_from($1, pg_catalog.current_setting('server_encoding')) $$
LANGUAGE sql STABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION utl_raw.cast_to_varchar2(bytea) IS 'Converts a raw (bytea) value to a value using the database character set';

CREATE FUNCTION utl_raw.length(bytea)
RETURNS integer
AS $$ SELECT pg_catalog.length($1) $$
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION utl_raw.length(bytea) IS 'Returns the number of bytes in a raw (bytea) value';

CREATE FUNCTION utl_raw.substr(bytea, integer, integer DEFAULT NULL)
RETURNS bytea
AS $$
DECLARE
    total integer;
    pos integer := coalesce($2, 0);
    len integer;
BEGIN
    -- Oracle UTL_RAW.SUBSTR is 1-based; a negative position counts from the end,
    -- a position of 0 or NULL is taken as 1, and an omitted or NULL length runs
    -- to the end of the value. Oracle raises VALUE_ERROR (ORA-06502) for a NULL
    -- raw, a length below 1, and a range that is not wholly inside the value.
    IF $1 IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = 'null_value_not_allowed',
            MESSAGE = 'argument ''r'' must not be NULL';
    END IF;
    total := pg_catalog.length($1);
    IF pos = 0 THEN
        pos := 1;
    ELSIF pos < 0 THEN
        pos := total + pos + 1;
    END IF;
    IF pos < 1 OR pos > total THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'argument ''pos'' is outside the value';
    END IF;
    len := coalesce($3, total - pos + 1);
    IF len < 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'argument ''len'' must be a number greater than 0';
    END IF;
    IF pos + len - 1 > total THEN
        RAISE EXCEPTION USING
            ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'argument ''len'' runs past the end of the value';
    END IF;
    RETURN substring($1 FROM pos FOR len);
END;
$$
LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;
COMMENT ON FUNCTION utl_raw.substr(bytea, integer, integer) IS 'Returns a portion of a raw (bytea) value';

CREATE FUNCTION utl_raw.concat(VARIADIC bytea[])
RETURNS bytea
AS $$ SELECT coalesce(pg_catalog.string_agg(part, ''::bytea), ''::bytea) FROM unnest($1) AS part $$
LANGUAGE sql IMMUTABLE PARALLEL SAFE;
COMMENT ON FUNCTION utl_raw.concat(VARIADIC bytea[]) IS 'Concatenates raw (bytea) values';

CREATE FUNCTION utl_raw.bit_and(bytea, bytea)
RETURNS bytea
AS 'MODULE_PATHNAME','orafce_utl_raw_bit_and'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION utl_raw.bit_and(bytea, bytea) IS 'Bitwise AND of two raw (bytea) values';

CREATE FUNCTION utl_raw.bit_or(bytea, bytea)
RETURNS bytea
AS 'MODULE_PATHNAME','orafce_utl_raw_bit_or'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION utl_raw.bit_or(bytea, bytea) IS 'Bitwise OR of two raw (bytea) values';

CREATE FUNCTION utl_raw.bit_xor(bytea, bytea)
RETURNS bytea
AS 'MODULE_PATHNAME','orafce_utl_raw_bit_xor'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;
COMMENT ON FUNCTION utl_raw.bit_xor(bytea, bytea) IS 'Bitwise XOR of two raw (bytea) values';

GRANT USAGE ON SCHEMA utl_raw TO PUBLIC;

-- The USER_* views describe the objects the current user owns, so they are
-- restricted to the current schema. Without that restriction they described
-- every table in the database, PostgreSQL's own catalogs included.
CREATE OR REPLACE VIEW oracle.user_tab_columns AS
    select table_name,
           column_name,
           data_type,
           coalesce(character_maximum_length, numeric_precision) AS data_length,
           numeric_precision AS data_precision,
           numeric_scale AS data_scale,
           is_nullable AS nullable,
           ordinal_position AS column_id,
           is_updatable AS data_upgraded,
           table_schema
    from information_schema.columns
   where table_schema = current_schema();

CREATE OR REPLACE VIEW oracle.user_tables AS
    select table_name
      from information_schema.tables
     where table_type = 'BASE TABLE'
       and table_schema = current_schema();

/*
 * orafce--4.7--4.8.sql declared the string functions parallel safe, but it
 * missed these five, so an installation created before 4.8 still excludes
 * every query that uses them from parallel execution.
 */
ALTER FUNCTION plvstr.instr(text, text) PARALLEL SAFE;
ALTER FUNCTION plvstr.instr(text, text, int) PARALLEL SAFE;
ALTER FUNCTION plvstr.instr(text, text, int, int) PARALLEL SAFE;
ALTER FUNCTION plvstr.substr(text, int) PARALLEL SAFE;
ALTER FUNCTION plvstr.substr(text, int, int) PARALLEL SAFE;

/*
 * orafce--3.21--3.22.sql qualified these references with pg_catalog, but the
 * install script kept the unqualified bodies, so an installation that was
 * created from an install script still resolves them through search_path.
 */
CREATE OR REPLACE FUNCTION oracle.sinh(float8)
RETURNS float8 AS
$$ SELECT (pg_catalog.exp($1) - pg_catalog.exp(-$1)) / 2; $$
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION oracle.cosh(float8)
RETURNS float8 AS
$$ SELECT (pg_catalog.exp($1) + pg_catalog.exp(-$1)) / 2; $$
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION oracle.last_day(TIMESTAMPTZ)
RETURNS TIMESTAMP
AS $$ SELECT (pg_catalog.date_trunc('MONTH', $1) + INTERVAL '1 MONTH - 1 day' + $1::time)::oracle.date; $$
LANGUAGE SQL IMMUTABLE STRICT PARALLEL SAFE;

/*
 * These functions reduce their timestamptz argument to a local date or a
 * local time, so the result depends on the TimeZone setting and they must
 * not be marked IMMUTABLE.  PostgreSQL marks date(timestamptz) and
 * date_trunc(text, timestamptz) STABLE for the same reason.
 */
ALTER FUNCTION oracle.add_months(timestamptz, int) STABLE;
ALTER FUNCTION oracle.last_day(timestamptz) STABLE;
ALTER FUNCTION oracle.months_between(timestamptz, timestamptz) STABLE;
ALTER FUNCTION oracle.next_day(timestamptz, int) STABLE;
ALTER FUNCTION oracle.next_day(timestamptz, text) STABLE;
ALTER FUNCTION oracle.round(timestamptz) STABLE;
ALTER FUNCTION oracle.round(timestamptz, text) STABLE;
ALTER FUNCTION oracle.trunc(timestamptz) STABLE;
ALTER FUNCTION oracle.trunc(timestamptz, text) STABLE;

/*
 * These overloads take the substitution keyword from the session state set
 * by plvsubst.setsubst() whenever the subst argument is absent or NULL, so
 * they are not immutable either.
 */
ALTER FUNCTION plvsubst.string(text, text) STABLE;
ALTER FUNCTION plvsubst.string(text, text, text) STABLE;
ALTER FUNCTION plvsubst.string(text, text, text, text) STABLE;
ALTER FUNCTION plvsubst.string(text, text[]) STABLE;
ALTER FUNCTION plvsubst.string(text, text[], text) STABLE;

/*
 * These function was marked as STABLE, should be IMMUTABLE.
 *
 * For consistency with orafce_concat2(varchar2, varchar2).
 * The function is not really IMMUTABLE because it depends
 * on orafce.varchar2_null_safe_concat. But without overwritting
 * volatility, this functionality was useable. Overwriting volatility
 * is known workaround with known risks.
 */
ALTER FUNCTION oracle.orafce_concat2(oracle.nvarchar2, oracle.nvarchar2) IMMUTABLE;

/*
 * These bodies resolved upper(), encode(), trunc(), date_part(),
 * translate(), convert_to(), convert_from(), current_setting(),
 * string_agg() and unnest() through the caller's search_path, so anybody
 * who could create objects in a schema the caller had in its search_path
 * could change what they return.  Qualify the calls with pg_catalog.
 */
CREATE OR REPLACE FUNCTION oracle.substr(numeric,numeric)
RETURNS text AS $$
SELECT oracle.substr($1::text,pg_catalog.trunc($2)::int);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION oracle.substr(numeric,numeric,numeric)
RETURNS text AS $$
SELECT oracle.substr($1::text,pg_catalog.trunc($2)::int,pg_catalog.trunc($3)::int);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION oracle.substr(varchar,numeric)
RETURNS text AS $$
SELECT oracle.substr($1,pg_catalog.trunc($2)::int);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION oracle.substr(varchar,numeric,numeric)
RETURNS text AS $$
SELECT oracle.substr($1,pg_catalog.trunc($2)::int,pg_catalog.trunc($3)::int);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION oracle.subtract(oracle.date,oracle.date)
RETURNS double precision AS $$
SELECT pg_catalog.date_part('epoch', ($1::timestamp - $2::timestamp)/3600/24);
$$ LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION oracle.rawtohex(bytea)
RETURNS text
AS $$ SELECT pg_catalog.upper(pg_catalog.encode($1, 'hex')) $$
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE;

CREATE OR REPLACE FUNCTION plvchr.stripped(str text, char_in text)
RETURNS varchar
AS $$ SELECT pg_catalog.translate($1, 'A'||$2, 'A'); $$
LANGUAGE SQL IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION utl_file.put_line(file utl_file.file_type, buffer anyelement, autoflush bool)
RETURNS bool
AS $$SELECT utl_file.put_line($1, $2::text, $3); $$
LANGUAGE SQL VOLATILE;

ALTER FUNCTION plvdate.add_bizdays(date, integer) VOLATILE;
ALTER FUNCTION plvdate.nearest_bizday(date) VOLATILE;
ALTER FUNCTION plvdate.next_bizday(date) VOLATILE;
ALTER FUNCTION plvdate.bizdays_between(date, date) VOLATILE;
ALTER FUNCTION plvdate.prev_bizday(date) VOLATILE;
ALTER FUNCTION plvdate.isbizday(date) VOLATILE;

ALTER FUNCTION dbms_random.initialize(integer) VOLATILE;
ALTER FUNCTION dbms_random.seed(integer) VOLATILE;
ALTER FUNCTION dbms_random.seed(text) VOLATILE;
