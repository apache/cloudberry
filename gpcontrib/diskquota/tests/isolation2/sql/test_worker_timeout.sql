!\retcode gpconfig -c diskquota.worker_timeout -v 1;
!\retcode gpstop -u;

SELECT gp_inject_fault_infinite('diskquota_worker_main', 'suspend', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=-1;

1&: SELECT diskquota.wait_for_worker_new_epoch();

SELECT pg_sleep(2 * current_setting('diskquota.worker_timeout')::int);

SELECT pg_cancel_backend(pid) FROM pg_stat_activity
WHERE query = 'SELECT diskquota.wait_for_worker_new_epoch();';

SELECT gp_inject_fault_infinite('diskquota_worker_main', 'resume', dbid)
  FROM gp_segment_configuration WHERE role='p' AND content=-1;

1<:

!\retcode gpconfig -r diskquota.worker_timeout;
!\retcode gpstop -u;
