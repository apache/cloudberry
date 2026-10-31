-- Test for the UTL_RAW package
\set ECHO none
SET client_min_messages = warning;
SET client_encoding = utf8;
\set VERBOSITY terse
\set ECHO all

SET search_path TO utl_raw, oracle, "$user", public, pg_catalog;

----
-- CAST_TO_RAW() / CAST_TO_VARCHAR2() -- the database charset byte view of a string
----

-- ORACLE> SELECT UTL_RAW.CAST_TO_RAW('ABC') FROM dual; -> 414243
SELECT rawtohex(utl_raw.cast_to_raw('ABC'));
-- ORACLE> SELECT UTL_RAW.CAST_TO_VARCHAR2(HEXTORAW('414243')) FROM dual; -> ABC
SELECT utl_raw.cast_to_varchar2(hextoraw('414243'));
-- inverse of each other, including a multibyte string
SELECT utl_raw.cast_to_varchar2(utl_raw.cast_to_raw('Ångström'));

----
-- LENGTH() -- the number of bytes
----

-- ORACLE> SELECT UTL_RAW.LENGTH(HEXTORAW('DEADBEEF')) FROM dual; -> 4
SELECT utl_raw.length(hextoraw('DEADBEEF'));
SELECT utl_raw.length(hextoraw(''));

----
-- SUBSTR() -- 1-based, a negative position counts from the end, length optional
----

-- ORACLE> SELECT UTL_RAW.SUBSTR(HEXTORAW('DEADBEEF'), 2, 2) FROM dual; -> ADBE
SELECT rawtohex(utl_raw.substr(hextoraw('DEADBEEF'), 2, 2));
-- omitted length runs to the end
SELECT rawtohex(utl_raw.substr(hextoraw('DEADBEEF'), 3));
-- a negative position counts from the end
SELECT rawtohex(utl_raw.substr(hextoraw('DEADBEEF'), -1));
-- ORACLE> SELECT UTL_RAW.SUBSTR(HEXTORAW('DEADBEEF'), -4, 2) FROM dual; -> DEAD
SELECT rawtohex(utl_raw.substr(hextoraw('DEADBEEF'), -4, 2));
-- a range ending on the last byte is inside the value
SELECT rawtohex(utl_raw.substr(hextoraw('DEADBEEF'), 4, 1));
-- a position of 0 or NULL is taken as 1, and a NULL length runs to the end
-- ORACLE> SELECT UTL_RAW.SUBSTR(HEXTORAW('DEADBEEF'), 0) FROM dual; -> DEADBEEF
SELECT rawtohex(utl_raw.substr(hextoraw('DEADBEEF'), 0));
SELECT rawtohex(utl_raw.substr(hextoraw('DEADBEEF'), 0, 2));
SELECT rawtohex(utl_raw.substr(hextoraw('DEADBEEF'), NULL));
SELECT rawtohex(utl_raw.substr(hextoraw('DEADBEEF'), 2, NULL));
-- Oracle raises VALUE_ERROR (ORA-06502) for each of these
SELECT utl_raw.substr(hextoraw('DEADBEEF'), 1, 0);
SELECT utl_raw.substr(hextoraw('DEADBEEF'), 1, -1);
SELECT utl_raw.substr(hextoraw('DEADBEEF'), 5);
SELECT utl_raw.substr(hextoraw('DEADBEEF'), -5);
SELECT utl_raw.substr(hextoraw('DEADBEEF'), 4, 2);
SELECT utl_raw.substr(hextoraw('DEADBEEF'), 1, 5);
SELECT utl_raw.substr(hextoraw('DEADBEEF'), -1, 2);
SELECT utl_raw.substr(NULL, 1, 1);

----
-- CONCAT() -- concatenate raw values
----

-- ORACLE> SELECT UTL_RAW.CONCAT(HEXTORAW('DEAD'), HEXTORAW('BEEF')) FROM dual; -> DEADBEEF
SELECT rawtohex(utl_raw.concat(hextoraw('DEAD'), hextoraw('BEEF')));

----
-- BIT_AND() / BIT_OR() / BIT_XOR() -- byte-wise; the tail of the longer operand
-- is appended once the shorter one runs out, as in Oracle
----

-- ORACLE> SELECT UTL_RAW.BIT_AND(HEXTORAW('F0F0'), HEXTORAW('FF00')) FROM dual; -> F000
SELECT rawtohex(utl_raw.bit_and(hextoraw('F0F0'), hextoraw('FF00')));
-- ORACLE> SELECT UTL_RAW.BIT_OR(HEXTORAW('F000'), HEXTORAW('0F0F')) FROM dual; -> FF0F
SELECT rawtohex(utl_raw.bit_or(hextoraw('F000'), hextoraw('0F0F')));
-- ORACLE> SELECT UTL_RAW.BIT_XOR(HEXTORAW('FF'), HEXTORAW('0F')) FROM dual; -> F0
SELECT rawtohex(utl_raw.bit_xor(hextoraw('FF'), hextoraw('0F')));
-- unequal lengths: the unprocessed tail of the longer operand is appended
SELECT rawtohex(utl_raw.bit_and(hextoraw('FFFF'), hextoraw('F0')));
