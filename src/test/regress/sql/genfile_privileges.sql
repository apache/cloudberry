--
-- pg_file_write()/pg_file_rename()/pg_file_unlink() must only be usable by
-- superusers and members of pg_write_server_files; pg_logdir_ls() is the
-- read-side equivalent, gated on pg_read_server_files.
--
CREATE ROLE regress_genfile_plain;
CREATE ROLE regress_genfile_writer IN ROLE pg_write_server_files;
CREATE ROLE regress_genfile_reader IN ROLE pg_read_server_files;

-- A plain role is denied at the ACL layer by the REVOKE in
-- system_functions.sql.
SET SESSION AUTHORIZATION regress_genfile_plain;
SELECT pg_file_write('regress_genfile.txt', 'hello', false);
SELECT pg_file_rename('regress_genfile.txt', 'regress_genfile2.txt', NULL);
SELECT pg_file_unlink('regress_genfile.txt');
SELECT count(*) >= 0 AS ok FROM pg_logdir_ls() AS t(starttime timestamp, filename text);
RESET SESSION AUTHORIZATION;

-- On a cluster upgraded in place proacl stays NULL, so the checks in
-- genfile.c are the only defense.  Simulate that by granting EXECUTE.
GRANT EXECUTE ON FUNCTION pg_file_write(text,text,boolean),
                          pg_file_rename(text,text,text),
                          pg_file_unlink(text),
                          pg_logdir_ls() TO regress_genfile_plain;
SET SESSION AUTHORIZATION regress_genfile_plain;
SELECT pg_file_write('regress_genfile.txt', 'hello', false);
SELECT pg_file_rename('regress_genfile.txt', 'regress_genfile2.txt', NULL);
SELECT pg_file_unlink('regress_genfile.txt');
SELECT count(*) >= 0 AS ok FROM pg_logdir_ls() AS t(starttime timestamp, filename text);
RESET SESSION AUTHORIZATION;

-- A pg_write_server_files member is allowed; the superuser cleans up after
-- it, which covers the superuser path too.
SET SESSION AUTHORIZATION regress_genfile_writer;
SELECT pg_file_write('regress_genfile.txt', 'hello', false);
SELECT pg_file_rename('regress_genfile.txt', 'regress_genfile2.txt', NULL);
RESET SESSION AUTHORIZATION;
SELECT pg_file_unlink('regress_genfile2.txt');

-- Likewise for pg_logdir_ls().  Which log files exist is not deterministic,
-- so only assert that the call succeeds.
SET SESSION AUTHORIZATION regress_genfile_reader;
SELECT count(*) >= 0 AS ok FROM pg_logdir_ls() AS t(starttime timestamp, filename text);
RESET SESSION AUTHORIZATION;

REVOKE ALL ON FUNCTION pg_file_write(text,text,boolean),
                       pg_file_rename(text,text,text),
                       pg_file_unlink(text),
                       pg_logdir_ls() FROM regress_genfile_plain;
DROP ROLE regress_genfile_plain, regress_genfile_writer, regress_genfile_reader;
