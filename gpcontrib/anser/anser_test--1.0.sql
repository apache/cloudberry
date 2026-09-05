/* gpcontrib/anser/anser_test--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION anser_test" to load this file. \quit

CREATE FUNCTION anser_test_bloom_roundtrip(
    condition_key text,
    value int4)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION anser_test_bloom_fold_inplace()
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION anser_test_bloom_rejects_mismatch()
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION anser_test_node_roundtrip(value int4)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
