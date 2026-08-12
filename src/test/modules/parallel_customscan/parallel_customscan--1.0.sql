/* src/test/modules/parallel_customscan/parallel_customscan--1.0.sql */

-- Complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION parallel_customscan" to load this file. \quit

CREATE FUNCTION pcs_get_hook_calls(
    OUT estimate_calls    bigint,
    OUT init_dsm_calls    bigint,
    OUT reinit_dsm_calls  bigint,
    OUT init_worker_calls bigint,
    OUT shutdown_calls    bigint
)
RETURNS record
AS 'MODULE_PATHNAME', 'pcs_get_hook_calls'
LANGUAGE C STRICT;
