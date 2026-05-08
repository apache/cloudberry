-- start_ignore
DROP DATABASE IF EXISTS partition_append;
-- end_ignore

create database partition_append;
\c partition_append
GRANT ALL ON SCHEMA public TO public;
