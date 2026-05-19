-- Check that user who has created gp_relsizes_stats have privileges to use all
-- tables, views and functions from the extension.
-- Check that this user can grant this privileges to others.

-- start_ignore
DROP DATABASE IF EXISTS db1;
DROP ROLE IF EXISTS user1, user2;
-- end_ignore

SELECT '\! cp "' || setting || '/pg_hba.conf" "'  || setting || '/pg_hba.conf.backup"' as cp_backup
FROM pg_settings
WHERE name = 'data_directory' \gset

:cp_backup

SELECT '\! echo "local all user1,user2 trust" >> ' || setting || '/pg_hba.conf' as add_users
FROM pg_settings
WHERE name = 'data_directory' \gset

:add_users

-- start_ignore
\! gpstop -u
-- end_ignore

CREATE ROLE user1 LOGIN RESOURCE QUEUE pg_default;
CREATE ROLE user2 LOGIN RESOURCE QUEUE pg_default;
CREATE DATABASE db1 OWNER user1;

\set initial_user :USER
\set initial_db :DBNAME

\c db1 user1

CREATE EXTENSION gp_relsizes_stats;

-- Check that user who has created gp_relsizes_stats can use the extension
SELECT FROM relsizes_stats_schema.segment_file_map LIMIT 0;
SELECT FROM relsizes_stats_schema.segment_file_sizes LIMIT 0;
SELECT FROM relsizes_stats_schema.table_sizes_history LIMIT 0;
SELECT FROM relsizes_stats_schema.table_files LIMIT 0;
SELECT FROM relsizes_stats_schema.table_sizes LIMIT 0;
SELECT FROM relsizes_stats_schema.namespace_sizes LIMIT 0;
SELECT relsizes_stats_schema.relsizes_collect_stats_once();

SELECT FROM relsizes_stats_schema.get_stats_for_database(
    (SELECT oid FROM pg_database WHERE datname = current_database()), true)
LIMIT 0;

-- Check that user who has created gp_relsizes_stats can grant privileges to others
GRANT USAGE ON SCHEMA relsizes_stats_schema TO user2;
GRANT SELECT ON ALL TABLES IN SCHEMA relsizes_stats_schema TO user2;

-- Check that user2 has got required privileges
\c - user2
SELECT FROM relsizes_stats_schema.segment_file_map LIMIT 0;
SELECT FROM relsizes_stats_schema.segment_file_sizes LIMIT 0;
SELECT FROM relsizes_stats_schema.table_sizes_history LIMIT 0;
SELECT FROM relsizes_stats_schema.table_files LIMIT 0;
SELECT FROM relsizes_stats_schema.table_sizes LIMIT 0;
SELECT FROM relsizes_stats_schema.namespace_sizes LIMIT 0;
SELECT relsizes_stats_schema.relsizes_collect_stats_once();

SELECT FROM relsizes_stats_schema.get_stats_for_database(
    (SELECT oid FROM pg_database WHERE datname = current_database()), true)
LIMIT 0;

-- Cleanup
\c :"initial_db" :"initial_user"

DROP DATABASE db1;
DROP ROLE user1, user2;

SELECT '\! mv "' || setting || '/pg_hba.conf.backup" "' || setting || '/pg_hba.conf"' as cp_restore
FROM pg_settings
WHERE name = 'data_directory' \gset

:cp_restore

-- start_ignore
\! gpstop -u
-- end_ignore
