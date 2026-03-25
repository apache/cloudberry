/* gp_stats_collector--1.0--1.1.sql */

-- complain if script is sourced in psql, rather than via ALTER EXTENSION
\echo Use "ALTER EXTENSION gp_stats_collector UPDATE TO '1.1'" to load this file. \quit

CREATE SCHEMA gpsc;

-- Unlink existing objects from extension.
ALTER EXTENSION gp_stats_collector DROP VIEW gpsc_stat_messages;
ALTER EXTENSION gp_stats_collector DROP FUNCTION gpsc_stat_messages_reset();
ALTER EXTENSION gp_stats_collector DROP FUNCTION __gpsc_stat_messages_f_on_segments();
ALTER EXTENSION gp_stats_collector DROP FUNCTION __gpsc_stat_messages_f_on_master();
ALTER EXTENSION gp_stats_collector DROP FUNCTION __gpsc_stat_messages_reset_f_on_segments();
ALTER EXTENSION gp_stats_collector DROP FUNCTION __gpsc_stat_messages_reset_f_on_master();

-- Now drop the objects.
DROP VIEW gpsc_stat_messages;
DROP FUNCTION gpsc_stat_messages_reset();
DROP FUNCTION __gpsc_stat_messages_f_on_segments();
DROP FUNCTION __gpsc_stat_messages_f_on_master();
DROP FUNCTION __gpsc_stat_messages_reset_f_on_segments();
DROP FUNCTION __gpsc_stat_messages_reset_f_on_master();

-- Recreate functions and view in new schema.
CREATE FUNCTION gpsc.__stat_messages_reset_f_on_master()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'gpsc_stat_messages_reset'
LANGUAGE C EXECUTE ON MASTER;

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
LANGUAGE SQL EXECUTE ON MASTER;

CREATE FUNCTION gpsc.__stat_messages_f_on_master()
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'gpsc_stat_messages'
LANGUAGE C STRICT VOLATILE EXECUTE ON MASTER;

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

-- Create new objects.
CREATE FUNCTION gpsc.__init_log_on_master()
RETURNS SETOF void
AS 'MODULE_PATHNAME', 'gpsc_init_log'
LANGUAGE C STRICT VOLATILE EXECUTE ON MASTER;

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
LANGUAGE C STRICT VOLATILE EXECUTE ON MASTER;

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
