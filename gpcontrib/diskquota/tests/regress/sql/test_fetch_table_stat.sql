--
-- 1. Test that when an error occurs in diskquota_fetch_table_stat
--    the error message is preserved for us to debug.
--

CREATE TABLE t_error_handling (i int) DISTRIBUTED BY (i);
-- Inject an error to a segment server, since this UDF is only called on segments.
SELECT gp_inject_fault_infinite('diskquota_fetch_table_stat', 'error', dbid)
    FROM gp_segment_configuration WHERE role='p' AND content=0;

-- Dispatch diskquota_fetch_table_stat to segments.
-- There should be a warning message from segment server saying:
-- fault triggered, fault name:'diskquota_fetch_table_stat' fault type:'error'
-- We're not interested in the oid here, we aggregate the result by COUNT(*).
SELECT COUNT(*)
    FROM (SELECT diskquota.diskquota_fetch_table_stat(1, array[(SELECT oid FROM pg_class WHERE relname='t_error_handling')])
          FROM gp_dist_random('gp_id') WHERE gp_segment_id=0) AS count;

-- Reset the fault injector to prevent future failure.
SELECT gp_inject_fault_infinite('diskquota_fetch_table_stat', 'reset', dbid)
    FROM gp_segment_configuration WHERE role='p' AND content=0;

-- Do some clean-ups.
DROP TABLE t_error_handling;
