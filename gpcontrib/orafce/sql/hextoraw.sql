-- Test for hextoraw()/rawtohex(), empty_clob()/empty_blob() and from_tz()
\set ECHO none
SET client_min_messages = warning;
SET client_encoding = utf8;
\set VERBOSITY terse
\set ECHO all

SET search_path TO oracle, "$user", public, pg_catalog;

----
-- hextoraw() / rawtohex() -- inverse hex <-> raw (bytea) pair
----

-- ORACLE> SELECT hextoraw('4142') FROM dual; -> 4142  (the raw bytes 0x41 0x42)
SELECT hextoraw('4142');
-- input hex is case-insensitive
SELECT hextoraw('deadbeef');
SELECT hextoraw('DEADBEEF');
SELECT hextoraw('deadbeef') = hextoraw('DEADBEEF');
-- empty input yields an empty raw
SELECT hextoraw('');

-- ORACLE> SELECT rawtohex(utl_raw.cast_to_raw('AB')) FROM dual; -> 4142
-- rawtohex yields UPPER-case hex, as Oracle does
SELECT rawtohex('\xdeadbeef'::bytea);
SELECT rawtohex('\x4142'::bytea);
SELECT rawtohex(''::bytea);

-- the two are inverses (round-trips, both directions)
SELECT rawtohex(hextoraw('DEADBEEF'));
SELECT hextoraw(rawtohex('\x0123456789abcdef'::bytea));

----
-- empty_clob() / empty_blob() -- empty character / binary LOBs
----

-- ORACLE> SELECT empty_clob() FROM dual; -> (empty)
SELECT empty_clob();
SELECT empty_clob() IS NULL;
SELECT length(empty_clob());
SELECT empty_clob() = '';

-- ORACLE> SELECT empty_blob() FROM dual; -> (empty)
SELECT empty_blob();
SELECT empty_blob() IS NULL;
SELECT length(empty_blob());
SELECT empty_blob() = ''::bytea;

----
-- from_tz() -- read a naive timestamp as being in the given zone
----

-- Display the result in a fixed session zone so the point-in-time is stable.
SET TimeZone TO 'UTC';

-- A numeric offset follows the ISO sign convention (east of UTC is positive),
-- as Oracle does -- NOT PostgreSQL's inverted POSIX sign.
-- ORACLE> SELECT from_tz(TIMESTAMP '2005-04-12 13:00:00', '+02:00') FROM dual;
--         -> 2005-04-12 13:00:00.000000 +02:00  (i.e. 11:00 UTC)
SELECT from_tz(TIMESTAMP '2005-04-12 13:00:00', '+02:00');
-- ORACLE> SELECT from_tz(TIMESTAMP '2005-04-12 13:00:00', '-05:00') FROM dual;
--         -> 2005-04-12 13:00:00.000000 -05:00  (i.e. 18:00 UTC)
SELECT from_tz(TIMESTAMP '2005-04-12 13:00:00', '-05:00');
-- a fractional-hour offset
SELECT from_tz(TIMESTAMP '2005-04-12 13:00:00', '+05:30');
-- a named region resolves against the live zone database
SELECT from_tz(TIMESTAMP '2005-01-15 12:00:00', 'Europe/Prague');
SELECT from_tz(TIMESTAMP '2005-07-15 12:00:00', 'Europe/Prague');
-- UTC in, UTC out is a no-op
SELECT from_tz(TIMESTAMP '2005-04-12 13:00:00', 'UTC');
-- from_tz is the inverse of the existing sys_extract_utc()
SELECT sys_extract_utc(from_tz(TIMESTAMP '2005-04-12 13:00:00', '+02:00'));

RESET TimeZone;
