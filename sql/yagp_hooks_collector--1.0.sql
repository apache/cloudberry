/* yagp_hooks_collector--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION yagp_hooks_collector" to load this file. \quit

CREATE FUNCTION __yagp_stat_messages_reset_f_on_master()
RETURNS void
AS 'MODULE_PATHNAME', 'yagp_stat_messages_reset'
LANGUAGE C EXECUTE ON MASTER;

CREATE FUNCTION __yagp_stat_messages_reset_f_on_segments()
RETURNS void
AS 'MODULE_PATHNAME', 'yagp_stat_messages_reset'
LANGUAGE C EXECUTE ON ALL SEGMENTS;

CREATE FUNCTION yagp_stat_messages_reset()
RETURNS void
AS 
$$
  SELECT __yagp_stat_messages_reset_f_on_master();
  SELECT __yagp_stat_messages_reset_f_on_segments();
$$
LANGUAGE SQL EXECUTE ON MASTER;

CREATE FUNCTION __yagp_stat_messages_f_on_master()
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'yagp_stat_messages'
LANGUAGE C STRICT VOLATILE EXECUTE ON MASTER;

CREATE FUNCTION __yagp_stat_messages_f_on_segments()
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'yagp_stat_messages'
LANGUAGE C STRICT VOLATILE EXECUTE ON ALL SEGMENTS;

CREATE VIEW yagp_stat_messages AS
  SELECT C.*
	FROM __yagp_stat_messages_f_on_master() as C (
    segid int,
    total_messages bigint,
    send_failures bigint,
    connection_failures bigint,
    other_errors bigint,
    max_message_size int
	)
  UNION ALL
  SELECT C.*
	FROM __yagp_stat_messages_f_on_segments() as C (
    segid int,
    total_messages bigint,
    send_failures bigint,
    connection_failures bigint,
    other_errors bigint,
    max_message_size int
	)
ORDER BY segid;
