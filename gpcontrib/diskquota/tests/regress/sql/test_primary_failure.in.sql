CREATE SCHEMA ftsr;
SELECT diskquota.set_schema_quota('ftsr', '1 MB');
SET search_path TO ftsr;
create or replace language @PLPYTHON_LANG_STR@;
--
-- pg_ctl:
--   datadir: data directory of process to target with `pg_ctl`
--   command: commands valid for `pg_ctl`
--   command_mode: modes valid for `pg_ctl -m`  
--
create or replace function pg_ctl(datadir text, command text, command_mode text default 'immediate')
returns text as $$
    import subprocess
    if command not in ('stop', 'restart'):
        plpy.error('Invalid command input')

    cmd = ['pg_ctl', '-l', 'postmaster.log', '-D', datadir,
           '-w', '-t', '120', '-m', command_mode, command]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = process.communicate()[0]
    if process.returncode != 0:
        plpy.error('pg_ctl failed: %s' % output)
    return 'OK'

$$ language @PLPYTHON_LANG_STR@;

create or replace function pg_recoverseg(datadir text, command text)
returns integer as $$
    import subprocess
    if command not in ('a', 'ar'):
        plpy.error('Invalid recovery command')
    process = subprocess.Popen(['gprecoverseg', '-' + command, '-d', datadir],
                               stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    output = process.communicate()[0]
    if process.returncode != 0:
        plpy.error('gprecoverseg -%s failed: %s' % (command, output))
    return process.returncode
$$ language @PLPYTHON_LANG_STR@;

CREATE FUNCTION wait_for_content0_state(expected text, timeout_seconds integer)
RETURNS boolean AS $$
DECLARE
    deadline timestamptz := clock_timestamp() + make_interval(secs => timeout_seconds);
    reached boolean;
BEGIN
    LOOP
        PERFORM gp_request_fts_probe_scan();
        SELECT count(*) = 2
               AND (SELECT count(*) FROM gp_segment_configuration WHERE content = 0) = 2
               AND count(DISTINCT c.dbid) = 2
               AND count(DISTINCT c.preferred_role) = 2
               AND bool_and(CASE expected
                   WHEN 'failed_over' THEN
                       (c.preferred_role = 'p' AND c.role = 'm' AND c.status = 'd' AND c.mode = 'n') OR
                       (c.preferred_role = 'm' AND c.role = 'p' AND c.status = 'u' AND c.mode = 'n')
                   WHEN 'recovered' THEN
                       c.role <> c.preferred_role AND c.status = 'u' AND c.mode = 's'
                   WHEN 'balanced' THEN
                       c.role = c.preferred_role AND c.status = 'u' AND c.mode = 's'
                   ELSE false END)
          INTO reached
          FROM gp_segment_configuration c
         WHERE c.content = 0 AND c.preferred_role IN ('p', 'm');
        IF reached THEN
            RETURN true;
        END IF;
        IF clock_timestamp() >= deadline THEN
            RETURN false;
        END IF;
        PERFORM pg_sleep(0.5);
    END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE FUNCTION wait_for_distributed_sql(timeout_seconds integer)
RETURNS boolean AS $$
    import subprocess
    import time

    port = plpy.execute("SELECT current_setting('port') AS value")[0]['value']
    socket = plpy.execute("SELECT current_setting('unix_socket_directories') AS value")[0]['value'].split(',')[0].strip()
    database = plpy.execute("SELECT current_database() AS value")[0]['value']
    user = plpy.execute("SELECT current_user AS value")[0]['value']
    contents = plpy.execute("SELECT content FROM gp_segment_configuration WHERE content >= 0 AND role = 'p' ORDER BY content")
    expected = '{%s}' % ','.join(str(row['content']) for row in contents)
    query = "SELECT array_agg(gp_segment_id ORDER BY gp_segment_id)::text FROM gp_dist_random('gp_id')"
    cmd = ['psql', '-X', '-A', '-t', '-v', 'ON_ERROR_STOP=1', '-h', socket,
           '-p', port, '-U', user, '-d', database, '-c', query]
    deadline = time.time() + timeout_seconds
    last_error = 'no attempt'
    while time.time() < deadline:
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        attempt_deadline = min(deadline, time.time() + 15)
        try:
            while process.poll() is None and time.time() < attempt_deadline:
                time.sleep(0.1)
            timed_out = process.poll() is None
        finally:
            if process.poll() is None:
                process.kill()
            output = process.communicate()[0].decode('utf8', 'replace').strip()
        if timed_out:
            last_error = 'distributed SQL attempt timed out: %s' % output
        elif process.returncode == 0:
            if output == expected:
                return True
            last_error = 'expected contents %s, got %s' % (expected, output)
        else:
            last_error = output
            transient = ('could not connect', 'connection refused', 'server closed the connection',
                         'failed to acquire resources', 'segment is down', 'gang', 'FTS')
            if not any(error.lower() in output.lower() for error in transient):
                plpy.error('distributed SQL failed: %s' % output)
        time.sleep(0.5)
    state = plpy.execute("SELECT dbid, role, status, mode FROM gp_segment_configuration WHERE content = 0 ORDER BY dbid")
    plpy.error('distributed SQL timed out: %s; content 0: %s' % (last_error, list(state)))
$$ LANGUAGE @PLPYTHON_LANG_STR@;

CREATE TABLE a(i int, j int) DISTRIBUTED BY (i);
-- the entries will be inserted into seg0
INSERT INTO a SELECT 2, generate_series(1,100);
INSERT INTO a SELECT 2, generate_series(1,100000);
SELECT diskquota.wait_for_worker_new_epoch();

SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'a'::regclass ORDER BY segid;

-- expect insert fail
INSERT INTO a SELECT 2, generate_series(1,100);

SELECT count(*) = 2
       AND (SELECT count(*) FROM gp_segment_configuration WHERE content = 0) = 2
       AND count(DISTINCT c.dbid) = 2
       AND count(DISTINCT c.preferred_role) = 2
       AND bool_and(c.role = c.preferred_role AND c.status = 'u' AND c.mode = 's')
       AS content0_initially_synchronized
FROM gp_segment_configuration c
WHERE c.content = 0 AND c.preferred_role IN ('p', 'm');

-- now one of primary is down
SET statement_timeout = '180s';
select pg_ctl((select datadir from gp_segment_configuration c where c.role='p' and c.content=0), 'stop');

-- switch mirror to primary
SELECT wait_for_content0_state('failed_over', 120);
RESET statement_timeout;

-- check GPDB status
select content, preferred_role, role, status, mode from gp_segment_configuration where content = 0 order by preferred_role desc;

SET statement_timeout = '180s';
SELECT wait_for_distributed_sql(120);
RESET statement_timeout;

-- expect insert fail
INSERT INTO a SELECT 2, generate_series(1,100);

-- increase quota
SELECT diskquota.set_schema_quota('ftsr', '200 MB');

SELECT diskquota.wait_for_worker_new_epoch();

-- expect insert success
INSERT INTO a SELECT 2, generate_series(1,10000);

SELECT diskquota.wait_for_worker_new_epoch();

-- check whether monitored_dbid_cache is refreshed in mirror
-- diskquota.table_size should be updated
SELECT tableid::regclass, size, segid FROM diskquota.table_size WHERE tableid = 'a'::regclass ORDER BY segid;

-- pull up failed primary
SET statement_timeout = '420s';
select pg_recoverseg((select datadir from gp_segment_configuration c where c.role='p' and c.content=-1), 'a');
SELECT wait_for_content0_state('recovered', 300);
RESET statement_timeout;
SET statement_timeout = '420s';
select pg_recoverseg((select datadir from gp_segment_configuration c where c.role='p' and c.content=-1), 'ar');
SELECT wait_for_content0_state('balanced', 300);
RESET statement_timeout;
-- check GPDB status
select content, preferred_role, role, status, mode from gp_segment_configuration where content = 0 order by preferred_role desc;

SET statement_timeout = '180s';
SELECT wait_for_distributed_sql(120);
RESET statement_timeout;

SELECT diskquota.wait_for_worker_new_epoch();
SELECT quota_in_mb, nspsize_in_bytes from diskquota.show_fast_schema_quota_view where schema_name='ftsr';
INSERT INTO a SELECT 2, generate_series(1,100);

DROP TABLE a;
DROP SCHEMA ftsr CASCADE;
