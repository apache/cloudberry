/* gp_stats_collector--1.2.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_stats_collector" to load this file. \quit

CREATE SCHEMA gpsc;

CREATE FUNCTION gpsc.__stat_messages_reset_f_on_master()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'gpsc_stat_messages_reset'
LANGUAGE C EXECUTE ON COORDINATOR;

CREATE FUNCTION gpsc.__stat_messages_reset_f_on_segments()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'gpsc_stat_messages_reset'
LANGUAGE C EXECUTE ON ALL SEGMENTS;

CREATE FUNCTION gpsc.stat_messages_reset()
RETURNS SETOF void
AS
$$
  SELECT gpsc.__stat_messages_reset_f_on_master();
  SELECT gpsc.__stat_messages_reset_f_on_segments();
$$
LANGUAGE SQL EXECUTE ON COORDINATOR;

CREATE FUNCTION gpsc.__stat_messages_f_on_master()
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'gpsc_stat_messages'
LANGUAGE C STRICT VOLATILE EXECUTE ON COORDINATOR;

CREATE FUNCTION gpsc.__stat_messages_f_on_segments()
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'gpsc_stat_messages'
LANGUAGE C STRICT VOLATILE EXECUTE ON ALL SEGMENTS;

CREATE VIEW gpsc.stat_messages AS
  SELECT C.*
	FROM gpsc.__stat_messages_f_on_master() as C (
    segid int,
    total_messages bigint,
    send_failures bigint,
    connection_failures bigint,
    other_errors bigint,
    max_message_size int
	)
  UNION ALL
  SELECT C.*
	FROM gpsc.__stat_messages_f_on_segments() as C (
    segid int,
    total_messages bigint,
    send_failures bigint,
    connection_failures bigint,
    other_errors bigint,
    max_message_size int
	)
ORDER BY segid;

CREATE FUNCTION gpsc.__init_log_on_master()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'gpsc_init_log'
LANGUAGE C STRICT VOLATILE EXECUTE ON COORDINATOR;

CREATE FUNCTION gpsc.__init_log_on_segments()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'gpsc_init_log'
LANGUAGE C STRICT VOLATILE EXECUTE ON ALL SEGMENTS;

-- Creates log table inside gpsc schema.
SELECT gpsc.__init_log_on_master();
SELECT gpsc.__init_log_on_segments();

CREATE VIEW gpsc.log AS
  SELECT * FROM gpsc.__log -- master
  UNION ALL
  SELECT * FROM gp_dist_random('gpsc.__log') -- segments
ORDER BY tmid, ssid, ccnt;

CREATE FUNCTION gpsc.__truncate_log_on_master()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'gpsc_truncate_log'
LANGUAGE C STRICT VOLATILE EXECUTE ON COORDINATOR;

CREATE FUNCTION gpsc.__truncate_log_on_segments()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'gpsc_truncate_log'
LANGUAGE C STRICT VOLATILE EXECUTE ON ALL SEGMENTS;

CREATE FUNCTION gpsc.truncate_log()
RETURNS SETOF void AS $$
BEGIN
    PERFORM gpsc.__truncate_log_on_master();
    PERFORM gpsc.__truncate_log_on_segments();
END;
$$ LANGUAGE plpgsql VOLATILE;

CREATE FUNCTION gpsc.__test_uds_start_server(path text)
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'gpsc_test_uds_start_server'
LANGUAGE C STRICT EXECUTE ON COORDINATOR;

CREATE FUNCTION gpsc.__test_uds_receive(timeout_ms int DEFAULT 2000)
RETURNS SETOF bigint
AS 'MODULE_PATHNAME', 'gpsc_test_uds_receive'
LANGUAGE C STRICT EXECUTE ON COORDINATOR;

CREATE FUNCTION gpsc.__test_uds_stop_server()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'gpsc_test_uds_stop_server'
LANGUAGE C EXECUTE ON COORDINATOR;

-- ---------------------------------------------------------------------------
-- 1.2: pg_query_state per-node runtime collection (push to yagpcc via UDS)
-- ---------------------------------------------------------------------------

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