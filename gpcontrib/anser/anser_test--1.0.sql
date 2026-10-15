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

-- The planner's give-up decision: NULL means no filter is injected.
CREATE FUNCTION anser_test_rf_size(
    est_rows float8,
    OUT injected bool,
    OUT total_elems bigint,
    OUT max_payload bigint,
    OUT planned_bytes bigint)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- What AnserBloomCreate builds, or NULL when it declines to build anything.
CREATE FUNCTION anser_test_bloom_shape(
    total_elems bigint,
    cap bigint,
    OUT built bool,
    OUT bits bigint,
    OUT serialized bigint,
    OUT bits_per_key float8)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- The coordinator's decision about a merged payload, on a synthetic part.
CREATE FUNCTION anser_test_worth_delivering(
    bitset_bytes int4,
    bytes_set int4,
    damage text)
RETURNS bool
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- Producer end to end: "<built|no-filter>:<delivered|cancelled|missing>".
CREATE FUNCTION anser_test_producer_decision(
    total_elems bigint,
    cap bigint,
    n_keys int4)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- The formatted QE -> QD message, verbatim.
CREATE FUNCTION anser_test_wire_format(
    kind "char",
    payload_type "char",
    flags int4,
    condition_key text,
    body bytea)
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- Format, alter one byte, parse: reports what the reader made of it.
CREATE FUNCTION anser_test_wire_roundtrip(
    condition_key text,
    body bytea,
    tamper text,
    payload_type "char")
RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- The QD -> QE checksum, so a test can compare two of them.
CREATE FUNCTION anser_test_push_crc(
    payload_type "char",
    condition_id int4,
    flags int4,
    condition_key text,
    body bytea)
RETURNS int8
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
