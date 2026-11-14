-- orafce.umask needs superuser or the orafce_set_umask role, also when it
-- is set before the orafce library is loaded.
CREATE ROLE regress_gp_umask;
\c
SET ROLE regress_gp_umask;
-- the library is not loaded yet, so this is only a placeholder
SET orafce.umask = '0000';
-- loading the library checks the placeholder value
SELECT oracle.add_months('2020-01-31'::date, 1);
SHOW orafce.umask;
SET orafce.umask = '0000';
RESET ROLE;
GRANT orafce_set_umask TO regress_gp_umask;
SET ROLE regress_gp_umask;
SET orafce.umask = '0027';
SHOW orafce.umask;
SET orafce.umask = '';
RESET orafce.umask;
RESET ROLE;
DROP ROLE regress_gp_umask;
