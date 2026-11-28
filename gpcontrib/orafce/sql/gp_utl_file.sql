-- The utl_file_dir checks must not depend on the caller's search_path: a
-- user's own "=" operator must not let utl_file open files outside the
-- allowed directories.
INSERT INTO utl_file.utl_file_dir(dir, dirname) VALUES(utl_file.tmpdir(), 'GP_TMP');
CREATE ROLE regress_gp_utl_file;
CREATE SCHEMA regress_gp_utl_file AUTHORIZATION regress_gp_utl_file;
SET ROLE regress_gp_utl_file;
CREATE FUNCTION regress_gp_utl_file.always(text, text) RETURNS bool
  LANGUAGE sql IMMUTABLE AS 'SELECT true';
CREATE OPERATOR regress_gp_utl_file.= (LEFTARG = text, RIGHTARG = text,
                                       FUNCTION = regress_gp_utl_file.always);
SET search_path = regress_gp_utl_file, pg_catalog;
SELECT 'a'::text = 'b'::text AS own_operator_used;
SELECT utl_file.fopen('/etc', 'passwd', 'r');
SELECT utl_file.fopen('GP_TMP', 'passwd', 'r');
RESET search_path;
RESET ROLE;
DROP SCHEMA regress_gp_utl_file CASCADE;
DROP ROLE regress_gp_utl_file;
DELETE FROM utl_file.utl_file_dir WHERE dirname = 'GP_TMP';
