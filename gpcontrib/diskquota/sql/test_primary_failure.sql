CREATE SCHEMA ftsr;
SELECT diskquota.set_schema_quota('ftsr', '1 MB');
SET search_path TO ftsr;
create or replace language plpythonu;
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
        return 'Invalid command input'

    cmd = 'pg_ctl -l postmaster.log -D %s ' % datadir
    cmd = cmd + '-W -m %s %s' % (command_mode, command)

    return subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).replace('.', '')
$$ language plpythonu;

create or replace function pg_recoverseg(datadir text, command text)
returns text as $$
    import subprocess
    cmd = 'gprecoverseg -%s -d %s; exit 0; ' % (command, datadir)
    return subprocess.check_output(cmd, stderr=subprocess.STDOUT, shell=True).replace('.', '')
$$ language plpythonu;

CREATE TABLE a(i int);
INSERT INTO a SELECT generate_series(1,100);
INSERT INTO a SELECT generate_series(1,100000);
SELECT pg_sleep(5);
-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);

-- now one of primary is down
select pg_ctl((select datadir from gp_segment_configuration c where c.role='p' and c.content=0), 'stop');

-- switch mirror to primary
select gp_request_fts_probe_scan();

-- check GPDB status
select content, preferred_role, role, status, mode from gp_segment_configuration where content = 0;

-- expect insert fail
INSERT INTO a SELECT generate_series(1,100);

-- increase quota
SELECT diskquota.set_schema_quota('ftsr', '200 MB');

-- pull up failed primary
-- start_ignore
select pg_recoverseg((select datadir from gp_segment_configuration c where c.role='p' and c.content=-1), 'a');
select pg_sleep(10);
select pg_recoverseg((select datadir from gp_segment_configuration c where c.role='p' and c.content=-1), 'ar');
select pg_sleep(15);
select pg_recoverseg((select datadir from gp_segment_configuration c where c.role='p' and c.content=-1), 'a');
select pg_sleep(10);
select pg_recoverseg((select datadir from gp_segment_configuration c where c.role='p' and c.content=-1), 'ar');
-- end_ignore

-- check GPDB status
select content, preferred_role, role, status, mode from gp_segment_configuration where content = 0;
-- no sleep, it will take effect immediately
SELECT pg_sleep(10);
SELECT quota_in_mb, nspsize_in_bytes from diskquota.show_fast_schema_quota_view where schema_name='ftsr';
INSERT INTO a SELECT generate_series(1,100);

DROP TABLE a;
DROP SCHEMA ftsr CASCADE;
