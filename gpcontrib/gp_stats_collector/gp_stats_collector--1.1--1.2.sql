/* gp_stats_collector--1.1--1.2.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION gp_stats_collector UPDATE TO '1.2'" to load this file. \quit

-- Compact (segid, pid) identifier for a QE backend running on a segment.
-- Matches the C gp_segment_pid struct used by the pg_query_state signal layer.
CREATE TYPE gpsc.gp_segment_pid AS (
  segid int,
  pid   int
);

-- pg_query_state(pid): trigger runtime per-node collection for the query
-- running on backend `pid`.  Fans QueryStatePollReason out to every QE via
-- cbdb_mpp_query_state; each matching QE walks its plan tree and pushes a
-- per-node batch to its local yagpcc over UDS.  Fire-and-forget: returns void.
CREATE FUNCTION gpsc.pg_query_state(pid int, trace_id bytea)
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'pg_query_state'
LANGUAGE C VOLATILE EXECUTE ON COORDINATOR;

-- cbdb_mpp_query_state(gp_segment_pid[], trace_id): dispatched verbatim to
-- every segment by pg_query_state() via CdbDispatchCommand; runs locally on
-- each QE, so no EXECUTE ON marker. Signals the matching local backends.  The
-- trace_id is stamped into every per-node batch so all backends' pushes land
-- under the one key this collection owns.
CREATE FUNCTION gpsc.cbdb_mpp_query_state(gpsc.gp_segment_pid[], trace_id bytea)
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'cbdb_mpp_query_state'
LANGUAGE C VOLATILE;

-- pg_query_state_backends(pid): list the QE backends participating in the
-- query running on backend `pid`, as (segid, pid) rows.  yagpcc uses the row
-- count as the "expected batches" barrier: per-node collection is complete
-- once a batch has arrived from every listed backend.
CREATE FUNCTION gpsc.pg_query_state_backends(pid int)
RETURNS TABLE(segid int, pid int)
AS 'MODULE_PATHNAME', 'pg_query_state_backends'
LANGUAGE C VOLATILE EXECUTE ON COORDINATOR;

-- The runtime query-state API is callable by any role; the per-backend
-- permission gate in C (superuser or the query's owner) enforces access, so
-- these can be granted broadly.  This lets monitoring agents (e.g. yagpcc) run
-- under a non-superuser role.  cbdb_mpp_query_state is dispatched to the QEs
-- under the caller's role, so it needs EXECUTE too.
GRANT USAGE ON SCHEMA gpsc TO PUBLIC;
GRANT EXECUTE ON FUNCTION gpsc.pg_query_state(int, bytea) TO PUBLIC;
GRANT EXECUTE ON FUNCTION gpsc.pg_query_state_backends(int) TO PUBLIC;
GRANT EXECUTE ON FUNCTION gpsc.cbdb_mpp_query_state(gpsc.gp_segment_pid[], bytea) TO PUBLIC;