/* gpcontrib/gp_interconnect_stats/gp_interconnect_stats--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION gp_interconnect_stats" to load this file. \quit

CREATE FUNCTION __gp_interconnect_get_stats_f_on_master(
	OUT gp_segment_id smallint,
	OUT total_recv_queue_size bigint,
	OUT recv_queue_conting_time bigint,
	OUT total_capacity bigint,
	OUT capacity_counting_time bigint,
	OUT total_buffers bigint,
	OUT buffer_counting_time bigint,
	OUT retransmits bigint,
	OUT startup_cached_pkts bigint,
	OUT mismatches bigint,
	OUT crs_errors bigint,
	OUT snd_pkt_num bigint,
	OUT recv_pkt_num bigint,
	OUT disordered_pkt_num bigint,
	OUT duplicate_pkt_num bigint,
	OUT recv_ack_num bigint,
	OUT status_query_msg_num bigint
)
RETURNS SETOF record
LANGUAGE C VOLATILE EXECUTE ON MASTER
AS '$libdir/gp_interconnect_stats', 'gp_interconnect_get_stats';

CREATE FUNCTION __gp_interconnect_get_stats_f_on_segments(
	OUT gp_segment_id smallint,
	OUT total_recv_queue_size bigint,
	OUT recv_queue_conting_time bigint,
	OUT total_capacity bigint,
	OUT capacity_counting_time bigint,
	OUT total_buffers bigint,
	OUT buffer_counting_time bigint,
	OUT retransmits bigint,
	OUT startup_cached_pkts bigint,
	OUT mismatches bigint,
	OUT crs_errors bigint,
	OUT snd_pkt_num bigint,
	OUT recv_pkt_num bigint,
	OUT disordered_pkt_num bigint,
	OUT duplicate_pkt_num bigint,
	OUT recv_ack_num bigint,
	OUT status_query_msg_num bigint
)
RETURNS SETOF record LANGUAGE C VOLATILE EXECUTE ON ALL SEGMENTS
AS '$libdir/gp_interconnect_stats', 'gp_interconnect_get_stats';


--------------------------------------------------------------------------------
-- @view:
--              gp_interconnect_stats_per_segment
--
-- @doc:
--              Cummulative interconnect statistics per segment
--
--------------------------------------------------------------------------------
CREATE VIEW gp_interconnect_stats_per_segment AS
    SELECT c.hostname, s.* FROM (
        SELECT * FROM __gp_interconnect_get_stats_f_on_master()
        UNION ALL
        SELECT * FROM __gp_interconnect_get_stats_f_on_segments()
    ) s
    INNER JOIN pg_catalog.gp_segment_configuration AS c
        ON s.gp_segment_id = c.content
        AND c.role = 'p'
    ;

GRANT SELECT ON gp_interconnect_stats_per_segment TO public;

--------------------------------------------------------------------------------
-- @view:
--              gp_interconnect_stats
--
-- @doc:
--              Cummulative interconnect statistics
--
--------------------------------------------------------------------------------
CREATE VIEW gp_interconnect_stats AS
    SELECT
        sum(total_recv_queue_size) as total_recv_queue_size
	  , sum(recv_queue_conting_time) as recv_queue_conting_time
	  , sum(total_capacity) as total_capacity
	  , sum(capacity_counting_time) as capacity_counting_time
	  , sum(total_buffers) as total_buffers
	  , sum(buffer_counting_time) as buffer_counting_time
	  , sum(retransmits) as retransmits
	  , sum(startup_cached_pkts) as startup_cached_pkts
	  , sum(mismatches) as mismatches
	  , sum(crs_errors) as crs_errors
	  , sum(snd_pkt_num) as snd_pkt_num
	  , sum(recv_pkt_num) as recv_pkt_num
	  , sum(disordered_pkt_num) as disordered_pkt_num
	  , sum(duplicate_pkt_num) as duplicate_pkt_num
	  , sum(recv_ack_num) as recv_ack_num
	  , sum(status_query_msg_num) as status_query_msg_num
    FROM gp_interconnect_stats_per_segment
    ;

GRANT SELECT ON gp_interconnect_stats TO public;

--------------------------------------------------------------------------------
-- @view:
--              gp_interconnect_stats_per_host
--
-- @doc:
--              Cummulative interconnect statistics grouped by host
--
--------------------------------------------------------------------------------

CREATE VIEW gp_interconnect_stats_per_host AS
    SELECT
        hostname
      , sum(total_recv_queue_size) as total_recv_queue_size
	  , sum(recv_queue_conting_time) as recv_queue_conting_time
	  , sum(total_capacity) as total_capacity
	  , sum(capacity_counting_time) as capacity_counting_time
	  , sum(total_buffers) as total_buffers
	  , sum(buffer_counting_time) as buffer_counting_time
	  , sum(retransmits) as retransmits
	  , sum(startup_cached_pkts) as startup_cached_pkts
	  , sum(mismatches) as mismatches
	  , sum(crs_errors) as crs_errors
	  , sum(snd_pkt_num) as snd_pkt_num
	  , sum(recv_pkt_num) as recv_pkt_num
	  , sum(disordered_pkt_num) as disordered_pkt_num
	  , sum(duplicate_pkt_num) as duplicate_pkt_num
	  , sum(recv_ack_num) as recv_ack_num
	  , sum(status_query_msg_num) as status_query_msg_num
    FROM gp_interconnect_stats_per_segment
    GROUP BY hostname;

GRANT SELECT ON gp_interconnect_stats_per_host TO public;
