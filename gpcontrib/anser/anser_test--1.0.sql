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

-- The runtime-filter pushdown decision, over a synthetic scan.  `tlist`
-- describes the scan's output targetlist, one element per entry: a positive
-- value is a plain Var on that table attno, and the rest are the shapes that
-- must be refused -- 0 a whole-row Var, -1 a system column, -2 a computed
-- column, -3 a Var of another relation, -4 a cast wrapping a Var.
--
-- sk_attno is the attribute number the scan key would carry, and is NULL when
-- the shape is refused.
CREATE FUNCTION anser_test_pushdown_accepts(
    tlist int4[],
    attno int4,
    filter_in_seqscan bool,
    as_seqscan bool,
    OUT accepts bool,
    OUT sk_attno int4)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

-- The same scan put through AnserPushdownTarget, which must agree and name the
-- node itself when there is nothing to descend through.
CREATE FUNCTION anser_test_pushdown_target(
    tlist int4[],
    attno int4,
    filter_in_seqscan bool,
    as_seqscan bool,
    OUT found_self bool,
    OUT sk_attno int4)
RETURNS record
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;
