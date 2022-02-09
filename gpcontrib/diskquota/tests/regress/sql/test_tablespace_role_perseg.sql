-- Test role quota
-- start_ignore
\! mkdir -p /tmp/rolespc_perseg
-- end_ignore
DROP TABLESPACE  IF EXISTS rolespc_perseg;
CREATE TABLESPACE rolespc_perseg LOCATION '/tmp/rolespc_perseg';
CREATE SCHEMA rolespc_persegrole;
SET search_path TO rolespc_persegrole;

DROP ROLE IF EXISTS rolespc_persegu1;
DROP ROLE IF EXISTS rolespc_persegu2;
CREATE ROLE rolespc_persegu1 NOLOGIN;
CREATE ROLE rolespc_persegu2 NOLOGIN;
CREATE TABLE b (t TEXT) TABLESPACE rolespc_perseg;
ALTER TABLE b OWNER TO rolespc_persegu1;

SELECT diskquota.set_role_tablespace_quota('rolespc_persegu1', 'rolespc_perseg', '1 MB');

INSERT INTO b SELECT generate_series(1,100);
-- expect insert success
INSERT INTO b SELECT generate_series(1,100000);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO b SELECT generate_series(1,100);
-- change tablespace role quota
SELECT diskquota.set_role_tablespace_quota('rolespc_persegu1', 'rolespc_perseg', '10 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert success
INSERT INTO b SELECT generate_series(1,100);

-- Test show_fast_schema_tablespace_quota_view
SELECT role_name, tablespace_name, quota_in_mb, rolsize_tablespace_in_bytes FROM diskquota.show_fast_role_tablespace_quota_view WHERE role_name = 'rolespc_persegu1' and tablespace_name = 'rolespc_perseg';

SELECT diskquota.set_per_segment_quota('rolespc_perseg', '0.1');
SELECT diskquota.wait_for_worker_new_epoch();
---- expect insert fail by tablespace schema perseg quota
INSERT INTO b SELECT generate_series(1,100);
-- Test alter owner
ALTER TABLE b OWNER TO rolespc_persegu2;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert succeed
INSERT INTO b SELECT generate_series(1,100);
ALTER TABLE b OWNER TO rolespc_persegu1;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO b SELECT generate_series(1,100);

-- Test alter tablespace
-- start_ignore
\! mkdir -p /tmp/rolespc_perseg2
-- end_ignore
DROP TABLESPACE  IF EXISTS rolespc_perseg2;
CREATE TABLESPACE rolespc_perseg2 LOCATION '/tmp/rolespc_perseg2';
ALTER TABLE b SET TABLESPACE rolespc_perseg2;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert succeed
INSERT INTO b SELECT generate_series(1,100);
-- alter table b back to tablespace rolespc_perseg
ALTER TABLE b SET TABLESPACE rolespc_perseg;
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO b SELECT generate_series(1,100);

-- Test update per segment ratio
SELECT diskquota.set_per_segment_quota('rolespc_perseg', 3.1);
SELECT diskquota.wait_for_worker_new_epoch();
SELECT role_name, tablespace_name, quota_in_mb, rolsize_tablespace_in_bytes FROM diskquota.show_fast_role_tablespace_quota_view WHERE role_name = 'rolespc_persegu1' and tablespace_name = 'rolespc_perseg';

-- expect insert success
INSERT INTO b SELECT generate_series(1,100);
SELECT diskquota.set_per_segment_quota('rolespc_perseg', 0.11);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO b SELECT generate_series(1,100);

-- Test delete per segment ratio
SELECT diskquota.set_per_segment_quota('rolespc_perseg', -1);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert success
INSERT INTO b SELECT generate_series(1,100);
SELECT diskquota.set_per_segment_quota('rolespc_perseg', 0.11);
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert fail
INSERT INTO b SELECT generate_series(1,100);

-- Test delete quota config
SELECT diskquota.set_role_tablespace_quota('rolespc_persegu1', 'rolespc_perseg', '-1 MB');
SELECT diskquota.wait_for_worker_new_epoch();
-- expect insert success
INSERT INTO b SELECT generate_series(1,100);

DROP table b;
DROP ROLE rolespc_persegu1, rolespc_persegu2;
RESET search_path;
DROP SCHEMA rolespc_persegrole;
DROP TABLESPACE rolespc_perseg;
DROP TABLESPACE rolespc_perseg2;
