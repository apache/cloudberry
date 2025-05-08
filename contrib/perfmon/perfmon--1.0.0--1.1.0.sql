ALTER TABLE queries_history
ADD COLUMN mem_peak BIGINT NOT NULL,
ADD COLUMN spill_file_size BIGINT NOT NULL,
ADD COLUMN disk_read BIGINT NOT NULL,
ADD COLUMN disk_write BIGINT NOT NULL;

ALTER FOREIGN TABLE queries_now
ADD COLUMN mem_peak BIGINT NOT NULL,
ADD COLUMN spill_file_size BIGINT NOT NULL,
ADD COLUMN disk_read BIGINT NOT NULL,
ADD COLUMN disk_write BIGINT NOT NULL;

ALTER FOREIGN TABLE queries_tail
ADD COLUMN mem_peak BIGINT NOT NULL,
ADD COLUMN spill_file_size BIGINT NOT NULL,
ADD COLUMN disk_read BIGINT NOT NULL,
ADD COLUMN disk_write BIGINT NOT NULL;

ALTER FOREIGN TABLE _queries_tail
ADD COLUMN mem_peak BIGINT NOT NULL,
ADD COLUMN spill_file_size BIGINT NOT NULL,
ADD COLUMN disk_read BIGINT NOT NULL,
ADD COLUMN disk_write BIGINT NOT NULL;
CREATE FUNCTION pg_query_state(pid 		integer
			, verbose	boolean = FALSE
			, costs 	boolean = FALSE
			, timing 	boolean = FALSE
			, buffers 	boolean = FALSE
			, triggers	boolean = FALSE
			, format	text = 'text')
	RETURNS TABLE (pid integer
			, frame_number integer
			, query_text text
			, plan text
			, leader_pid integer)
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION cbdb_mpp_query_state(gp_segment_pid[])
	RETURNS void
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION query_state_pause()
	RETURNS void
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION query_state_resume()
	RETURNS void
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION query_state_pause_command()
	RETURNS void
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT VOLATILE;

CREATE FUNCTION query_state_resume_command()
	RETURNS void
	AS 'MODULE_PATHNAME'
	LANGUAGE C STRICT VOLATILE;

