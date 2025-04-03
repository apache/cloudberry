
select count(*) = 2 as in_sync from gp_segment_configuration
where content = 0 and mode = 's';

-- fts_update_config have been removed

-- Verify that FTS didn't leak any locks
select locktype, mode, relation, pid, granted from pg_locks where
relation = 'gp_segment_configuration'::regclass or
relation = 'gp_configuration_history'::regclass;

select count(*) = 2 as in_sync from gp_segment_configuration
where content = 0 and mode = 's';

set optimizer = off;
-- start_ignore
\! gpconfig -c client_min_messages -v DEBUG1
\! gpstop -u

select gp_request_fts_probe_scan();
-- end_ignore

select count(*) from gp_segment_configuration where status = 'd';

-- start_ignore
\! gpconfig -r client_min_messages
\! gpstop -u
-- end_ignore

reset optimizer;
