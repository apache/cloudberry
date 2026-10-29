/* src/test/modules/test_mcxt_peak_tracker/test_mcxt_peak_tracker--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION test_mcxt_peak_tracker" to load this file. \quit

CREATE FUNCTION check_peak_usage_propagation()
RETURNS BOOLEAN
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION check_alloc_and_free_for_context_types()
RETURNS BOOLEAN
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION check_reparent()
RETURNS BOOLEAN
AS 'MODULE_PATHNAME'
LANGUAGE C;

