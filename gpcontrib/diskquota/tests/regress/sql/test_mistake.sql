-- to make sure that the schema 'notfoundns' is really not found
select nspname from pg_namespace where nspname = 'notfoundns';
select diskquota.set_schema_quota('notfoundns', '1 MB');

DROP SCHEMA IF EXISTS nmistake;
CREATE SCHEMA nmistake;
select diskquota.set_schema_quota('nmistake', '0 MB');

DROP ROLE IF EXISTS rmistake;
CREATE ROLE rmistake;
select diskquota.set_role_quota('rmistake', '0 MB');

-- start_ignore
\! mkdir -p /tmp/spcmistake
-- end_ignore
DROP TABLESPACE  IF EXISTS spcmistake;
CREATE TABLESPACE spcmistake LOCATION '/tmp/spcmistake';
SELECT diskquota.set_schema_tablespace_quota('nmistake', 'spcmistake','0 MB');
SELECT diskquota.set_role_tablespace_quota('rmistake', 'spcmistake','0 MB');
SELECT diskquota.set_per_segment_quota('spcmistake', 0);

DROP SCHEMA nmistake;
DROP ROLE rmistake;
DROP TABLESPACE spcmistake;
