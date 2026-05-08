/* gp_stats_collector--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_stats_collector" to load this file. \quit

CREATE FUNCTION __gpsc_stat_messages_reset_f_on_master()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'gpsc_stat_messages_reset'
LANGUAGE C EXECUTE ON COORDINATOR;

CREATE FUNCTION __gpsc_stat_messages_reset_f_on_segments()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'gpsc_stat_messages_reset'
LANGUAGE C EXECUTE ON ALL SEGMENTS;

CREATE FUNCTION gpsc_stat_messages_reset()
RETURNS SETOF void
AS
$$
  SELECT __gpsc_stat_messages_reset_f_on_master();
  SELECT __gpsc_stat_messages_reset_f_on_segments();
$$
LANGUAGE SQL EXECUTE ON COORDINATOR;

CREATE FUNCTION __gpsc_stat_messages_f_on_master()
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'gpsc_stat_messages'
LANGUAGE C STRICT VOLATILE EXECUTE ON COORDINATOR;

CREATE FUNCTION __gpsc_stat_messages_f_on_segments()
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'gpsc_stat_messages'
LANGUAGE C STRICT VOLATILE EXECUTE ON ALL SEGMENTS;

CREATE VIEW gpsc_stat_messages AS
  SELECT C.*
	FROM __gpsc_stat_messages_f_on_master() as C (
    segid int,
    total_messages bigint,
    send_failures bigint,
    connection_failures bigint,
    other_errors bigint,
    max_message_size int
	)
  UNION ALL
  SELECT C.*
	FROM __gpsc_stat_messages_f_on_segments() as C (
    segid int,
    total_messages bigint,
    send_failures bigint,
    connection_failures bigint,
    other_errors bigint,
    max_message_size int
	)
ORDER BY segid;
