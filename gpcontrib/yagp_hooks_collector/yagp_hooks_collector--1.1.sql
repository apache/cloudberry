/* yagp_hooks_collector--1.1.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION yagp_hooks_collector" to load this file. \quit

CREATE SCHEMA yagpcc;

CREATE FUNCTION yagpcc.__stat_messages_reset_f_on_master()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'yagp_stat_messages_reset'
LANGUAGE C EXECUTE ON MASTER;

CREATE FUNCTION yagpcc.__stat_messages_reset_f_on_segments()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'yagp_stat_messages_reset'
LANGUAGE C EXECUTE ON ALL SEGMENTS;

CREATE FUNCTION yagpcc.stat_messages_reset()
RETURNS SETOF void
AS
$$
  SELECT yagpcc.__stat_messages_reset_f_on_master();
  SELECT yagpcc.__stat_messages_reset_f_on_segments();
$$
LANGUAGE SQL EXECUTE ON MASTER;

CREATE FUNCTION yagpcc.__stat_messages_f_on_master()
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'yagp_stat_messages'
LANGUAGE C STRICT VOLATILE EXECUTE ON MASTER;

CREATE FUNCTION yagpcc.__stat_messages_f_on_segments()
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'yagp_stat_messages'
LANGUAGE C STRICT VOLATILE EXECUTE ON ALL SEGMENTS;

CREATE VIEW yagpcc.stat_messages AS
  SELECT C.*
	FROM yagpcc.__stat_messages_f_on_master() as C (
    segid int,
    total_messages bigint,
    send_failures bigint,
    connection_failures bigint,
    other_errors bigint,
    max_message_size int
	)
  UNION ALL
  SELECT C.*
	FROM yagpcc.__stat_messages_f_on_segments() as C (
    segid int,
    total_messages bigint,
    send_failures bigint,
    connection_failures bigint,
    other_errors bigint,
    max_message_size int
	)
ORDER BY segid;

CREATE FUNCTION yagpcc.__init_log_on_master()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'yagp_init_log'
LANGUAGE C STRICT VOLATILE EXECUTE ON MASTER;

CREATE FUNCTION yagpcc.__init_log_on_segments()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'yagp_init_log'
LANGUAGE C STRICT VOLATILE EXECUTE ON ALL SEGMENTS;

-- Creates log table inside yagpcc schema.
SELECT yagpcc.__init_log_on_master();
SELECT yagpcc.__init_log_on_segments();

CREATE VIEW yagpcc.log AS
  SELECT * FROM yagpcc.__log -- master
  UNION ALL
  SELECT * FROM gp_dist_random('yagpcc.__log') -- segments
ORDER BY tmid, ssid, ccnt;

CREATE FUNCTION yagpcc.__truncate_log_on_master()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'yagp_truncate_log'
LANGUAGE C STRICT VOLATILE EXECUTE ON MASTER;

CREATE FUNCTION yagpcc.__truncate_log_on_segments()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'yagp_truncate_log'
LANGUAGE C STRICT VOLATILE EXECUTE ON ALL SEGMENTS;

CREATE FUNCTION yagpcc.truncate_log()
RETURNS SETOF void AS $$
BEGIN
    PERFORM yagpcc.__truncate_log_on_master();
    PERFORM yagpcc.__truncate_log_on_segments();
END;
$$ LANGUAGE plpgsql VOLATILE;
